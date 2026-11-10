"""Tests for src.analysis.expense_trends."""

import numpy as np
import pandas as pd
import pytest
import statsmodels.api as sm
from statsmodels.stats.multitest import multipletests

from src.analysis.expense_trends import (
    check_seasonality,
    compute_expense_trends,
    fit_category_trend,
    month_of_year_kruskal,
)
from src.analysis.stats_utils import FDR_Q, bh_adjust, fit_linear_trend, newey_west_maxlags


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rising_series(n: int = 12, slope: float = 100.0) -> pd.Series:
    """Strictly rising series: 100, 200, 300, … so OLS will be clearly rising."""
    return pd.Series([slope * (i + 1) for i in range(n)])


def _flat_series(n: int = 12, value: float = 500.0) -> pd.Series:
    return pd.Series([value] * n)


def _falling_series(n: int = 12, slope: float = 100.0) -> pd.Series:
    return pd.Series([slope * (n - i) for i in range(n)])


def _make_monthly_df(n_months: int = 12) -> pd.DataFrame:
    """
    Legacy signed monthly aggregates: Food (constant -400) and Income (+3000).

    compute_expense_trends() must keep only the negative expense flow and
    analyse it as positive spend, dropping the positive income flow.
    """
    import pandas as pd

    rows = []
    for i in range(n_months):
        year_month = f"{2024 + (i // 12)}-{(i % 12) + 1:02d}"
        rows.append(
            {
                "year_month": year_month,
                "category": "Food",
                "total_amount": -400.0,
                "month_ordinal": i,
            }
        )
        rows.append(
            {
                "year_month": year_month,
                "category": "Income",
                "total_amount": 3000.0,
                "month_ordinal": i,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# fit_category_trend
# ---------------------------------------------------------------------------


class TestFitCategoryTrend:
    def test_rising_trend_detected(self):
        result = fit_category_trend(_rising_series(12))
        assert result["trend_class"] == "rising"

    def test_falling_trend_detected(self):
        result = fit_category_trend(_falling_series(12))
        assert result["trend_class"] == "falling"

    def test_flat_series_classified_flat(self):
        result = fit_category_trend(_flat_series(12))
        assert result["trend_class"] == "flat"

    def test_insufficient_data_below_6_months(self):
        result = fit_category_trend(_rising_series(5))
        assert result["trend_class"] == "insufficient_data"
        assert result["n_months"] == 5

    def test_exactly_6_months_is_not_insufficient(self):
        result = fit_category_trend(_rising_series(6))
        assert result["trend_class"] != "insufficient_data"

    def test_returns_all_required_keys_for_sufficient_data(self):
        result = fit_category_trend(_rising_series(12))
        for key in (
            "slope",
            "intercept",
            "r_squared",
            "p_value",
            "n_months",
            "trend_class",
            "ci_slope_lower",
            "ci_slope_upper",
        ):
            assert key in result, f"Missing key: {key}"

    def test_n_months_matches_input_length(self):
        result = fit_category_trend(_rising_series(15))
        assert result["n_months"] == 15

    def test_slope_positive_for_rising_series(self):
        result = fit_category_trend(_rising_series(12))
        assert result["slope"] > 0

    def test_slope_negative_for_falling_series(self):
        result = fit_category_trend(_falling_series(12))
        assert result["slope"] < 0

    def test_r_squared_high_for_perfect_linear(self):
        result = fit_category_trend(_rising_series(12))
        assert result["r_squared"] > 0.99

    def test_ci_lower_less_than_upper(self):
        result = fit_category_trend(_rising_series(12))
        assert result["ci_slope_lower"] < result["ci_slope_upper"]

    def test_p_value_in_0_1_range(self):
        result = fit_category_trend(_rising_series(12))
        assert 0.0 <= result["p_value"] <= 1.0

    def test_zero_series_does_not_raise(self):
        # all zeros → slope == 0; p_value will be NaN or 1; should return 'flat'
        result = fit_category_trend(pd.Series([0.0] * 12))
        assert "trend_class" in result


# ---------------------------------------------------------------------------
# check_seasonality
# ---------------------------------------------------------------------------


class TestCheckSeasonality:
    def _make_seasonal_df(self) -> pd.DataFrame:
        """12 months, extreme spike in December."""
        months = pd.date_range("2024-01-01", periods=12, freq="MS")
        values = [100.0] * 11 + [2000.0]  # huge Dec spike → high CV
        return pd.DataFrame({"year_month": months, "amount": values})

    def _make_uniform_df(self) -> pd.DataFrame:
        """24 months of perfectly uniform monthly spend."""
        months = pd.date_range("2023-01-01", periods=24, freq="MS")
        values = [500.0] * 24
        return pd.DataFrame({"year_month": months, "amount": values})

    def test_high_cv_reported_descriptively(self):
        df = self._make_seasonal_df()
        result = check_seasonality(df, date_col="year_month", value_col="amount")
        assert result["cv"] > 0.4
        assert "is_seasonal" not in result

    def test_returns_cv_key(self):
        df = self._make_uniform_df()
        result = check_seasonality(df, date_col="year_month", value_col="amount")
        assert "cv" in result

    def test_returns_month_means_dict(self):
        df = self._make_uniform_df()
        result = check_seasonality(df, date_col="year_month", value_col="amount")
        assert isinstance(result["month_means"], dict)
        assert len(result["month_means"]) > 0

    def test_cv_non_negative(self):
        df = self._make_uniform_df()
        result = check_seasonality(df, date_col="year_month", value_col="amount")
        assert result["cv"] >= 0

    # ── Fix 1: CV sign bug ────────────────────────────────────────────────

    def _make_negative_seasonal_df(self) -> pd.DataFrame:
        """12 months of negative amounts (expense convention) with a Dec spike."""
        months = pd.date_range("2024-01-01", periods=12, freq="MS")
        values = [-100.0] * 11 + [-2000.0]  # identical shape to positive test but negated
        return pd.DataFrame({"year_month": months, "amount": values})

    def _make_negative_uniform_df(self) -> pd.DataFrame:
        """24 months of constant negative spend."""
        months = pd.date_range("2023-01-01", periods=24, freq="MS")
        values = [-500.0] * 24
        return pd.DataFrame({"year_month": months, "amount": values})

    def test_cv_is_always_non_negative_for_negative_amounts(self):
        """CV should be a magnitude ratio — never negative regardless of amount sign."""
        df = self._make_negative_uniform_df()
        result = check_seasonality(df, date_col="year_month", value_col="amount")
        assert result["cv"] >= 0


class TestMonthOfYearKruskal:
    def test_strong_month_pattern_is_significant(self):
        idx = pd.date_range("2022-01-01", periods=36, freq="MS")
        values = np.where(idx.month == 12, 500.0, 100.0)
        residuals = pd.Series(values, index=idx)
        assert month_of_year_kruskal(residuals) < 0.01

    def test_flat_noise_is_not_significant(self):
        rng = np.random.default_rng(5)
        idx = pd.date_range("2022-01-01", periods=72, freq="MS")
        residuals = pd.Series(rng.normal(0.0, 1.0, len(idx)), index=idx)
        assert month_of_year_kruskal(residuals) > 0.01

    def test_single_month_returns_nan(self):
        idx = pd.date_range("2024-01-01", periods=1, freq="MS")
        assert np.isnan(month_of_year_kruskal(pd.Series([1.0], index=idx)))


# ---------------------------------------------------------------------------
# compute_expense_trends
# ---------------------------------------------------------------------------


class TestComputeExpenseTrends:
    def test_returns_dataframe(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert isinstance(result, pd.DataFrame)

    def test_has_all_required_trend_columns(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        for col in (
            "trend_slope",
            "trend_intercept",
            "trend_r2",
            "trend_pvalue",
            "trend_n_months",
            "trend_class",
            "trend_ci_lower",
            "trend_ci_upper",
            "seasonality_cv",
            "seasonality_pvalue",
            "seasonality_pvalue_adj",
            "is_seasonal",
            "share_pct",
        ):
            assert col in result.columns, f"Missing column: {col}"

    def test_row_count_matches_expense_input(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        # Only the 12 Food expense rows survive; Income is excluded.
        assert len(result) == 12

    def test_expense_categories_preserved(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert set(result["category"].unique()) == {"Food"}

    def test_income_is_excluded(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert "Income" not in result["category"].unique()

    def test_rising_expense_with_legacy_sign_is_classified_rising(self):
        """Growing spend is negative in legacy aggregates but must read as rising."""
        df = pd.DataFrame(
            {
                "year_month": [f"2024-{m:02d}" for m in range(1, 7)],
                "category": ["Food"] * 6,
                "total_amount": [-400.0, -500.0, -600.0, -700.0, -800.0, -900.0],
                "month_ordinal": list(range(6)),
            }
        )
        result = compute_expense_trends(df)
        assert result["trend_class"].iloc[0] == "rising"
        assert result["trend_slope"].iloc[0] > 0

    def test_flow_type_frame_uses_only_expense_rows(self):
        df = pd.DataFrame(
            {
                "year_month": ["2024-01", "2024-01"],
                "category": ["Food", "Income"],
                "flow_type": ["expense", "income"],
                "total_amount": [400.0, 3000.0],
                "month_ordinal": [0, 0],
            }
        )
        result = compute_expense_trends(df)
        assert result["category"].tolist() == ["Food"]
        assert result["total_amount"].tolist() == [400.0]

    def test_share_pct_sums_to_100(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        total_share = result.drop_duplicates("category")["share_pct"].sum()
        assert abs(total_share - 100.0) < 1e-6

    def test_insufficient_data_for_short_series(self):
        # Only 4 months — below 6-month threshold
        df = _make_monthly_df(4)
        result = compute_expense_trends(df)
        assert (result["trend_class"] == "insufficient_data").all()

    def test_exactly_6_months_not_insufficient(self):
        df = _make_monthly_df(6)
        result = compute_expense_trends(df)
        assert not (result["trend_class"] == "insufficient_data").all()

    def test_flat_category_gets_flat_or_insufficient_trend(self):
        df = _make_monthly_df(12)  # Food is constant -400
        result = compute_expense_trends(df)
        food_rows = result[result["category"] == "Food"]
        trend = food_rows["trend_class"].iloc[0]
        assert trend in ("flat", "rising", "falling", "insufficient_data")

    def test_share_pct_non_negative(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert (result["share_pct"] >= 0).all()

    def test_does_not_mutate_input(self):
        df = _make_monthly_df(12)
        original_cols = list(df.columns)
        compute_expense_trends(df)
        assert list(df.columns) == original_cols

    def test_partial_month_rows_are_dropped(self):
        df = _make_monthly_df(12)
        df["is_partial_month"] = False
        df.loc[df["year_month"] == "2024-12", "is_partial_month"] = True
        result = compute_expense_trends(df)
        assert "2024-12" not in set(result["year_month"])
        assert len(result) == 11


# ---------------------------------------------------------------------------
# stats_utils — HAC errors and BH adjustment (docs/12 Phase 2)
# ---------------------------------------------------------------------------


def test_newey_west_maxlags_matches_formula():
    for n in (6, 12, 110):
        expected = max(1, int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0))))
        assert newey_west_maxlags(n) == expected
    assert newey_west_maxlags(0) == 1


def test_bh_adjust_matches_multipletests_and_keeps_nan():
    p = pd.Series([0.001, 0.02, 0.2, np.nan, 0.5], index=list("abcde"))
    adjusted = bh_adjust(p)
    expected = multipletests([0.001, 0.02, 0.2, 0.5], method="fdr_bh")[1]
    assert adjusted.index.tolist() == list("abcde")
    assert np.isnan(adjusted["d"])
    assert adjusted[["a", "b", "c", "e"]].to_numpy() == pytest.approx(expected)


def test_hac_standard_error_larger_for_ar1_series():
    rng = np.random.default_rng(7)
    n = 120
    eps = rng.normal(0.0, 1.0, n)
    y = np.zeros(n)
    for i in range(1, n):
        y[i] = 0.7 * y[i - 1] + eps[i]
    x = np.arange(n, dtype=float)

    hac = fit_linear_trend(y, x)
    naive = sm.OLS(y, sm.add_constant(x)).fit()

    hac_se = (hac["ci_upper"] - hac["slope"]) / 1.96
    assert hac_se > naive.bse[1]


# ---------------------------------------------------------------------------
# TestComputeExpenseTrends — zero-filled grid and BH labels
# ---------------------------------------------------------------------------


def _make_monthly_df_many_categories(
    n_cats: int = 20,
    n_months: int = 12,
) -> pd.DataFrame:
    """Produce n_cats flat categories × n_months months for multiple-comparisons tests."""
    rows = []
    for c in range(n_cats):
        cat_name = f"Cat{c:02d}"
        for i in range(n_months):
            year_month = f"{2024 + (i // 12)}-{(i % 12) + 1:02d}"
            rows.append(
                {
                    "year_month": year_month,
                    "category": cat_name,
                    "total_amount": -(300.0 + c * 10),  # constant negatives
                    "month_ordinal": i,
                }
            )
    return pd.DataFrame(rows)


class TestComputeExpenseTrendsBH:
    def test_trend_pvalue_adj_column_present(self):
        df = _make_monthly_df_many_categories(n_cats=20)
        result = compute_expense_trends(df)
        assert "trend_pvalue_adj" in result.columns

    def test_trend_pvalue_adj_is_bh_adjusted(self):
        df = _make_monthly_df_many_categories(n_cats=5)
        result = compute_expense_trends(df)
        per_cat = result.drop_duplicates("category").set_index("category")["trend_pvalue"]
        expected = multipletests(per_cat.to_numpy(), method="fdr_bh")[1]
        actual = result.drop_duplicates("category").set_index("category")["trend_pvalue_adj"]
        assert actual.to_numpy() == pytest.approx(expected)

    def test_only_true_trend_survives_among_noise(self):
        rng = np.random.default_rng(11)
        rows = []
        for c in range(20):
            for i in range(36):
                rows.append(
                    {
                        "year_month": f"{2021 + i // 12}-{i % 12 + 1:02d}",
                        "category": f"Noise{c:02d}",
                        "flow_type": "expense",
                        "total_amount": float(rng.lognormal(5.0, 0.6)),
                        "month_ordinal": i,
                    }
                )
        for i in range(36):
            rows.append(
                {
                    "year_month": f"{2021 + i // 12}-{i % 12 + 1:02d}",
                    "category": "RealTrend",
                    "flow_type": "expense",
                    "total_amount": 500.0 + 100.0 * i,
                    "month_ordinal": i,
                }
            )
        result = compute_expense_trends(pd.DataFrame(rows))
        rising = set(result.loc[result["trend_class"] == "rising", "category"])
        assert "RealTrend" in rising
        assert rising == {"RealTrend"}

    def test_short_grid_stays_insufficient(self):
        df = _make_monthly_df_many_categories(n_cats=3, n_months=4)
        result = compute_expense_trends(df)
        assert (result["trend_class"] == "insufficient_data").all()


class TestZeroFilledGrid:
    def test_gap_produces_continuous_rows(self):
        rows = []
        for i in list(range(6)) + list(range(30, 36)):
            rows.append(
                {
                    "year_month": f"{2020 + i // 12}-{i % 12 + 1:02d}",
                    "category": "Food",
                    "flow_type": "expense",
                    "total_amount": 400.0 + i,
                    "month_ordinal": i,
                }
            )
        result = compute_expense_trends(pd.DataFrame(rows))

        assert len(result) == 36
        assert int((result["total_amount"] == 0.0).sum()) == 24
        assert result["trend_n_months"].iloc[0] == 36

    def test_trend_line_matches_ols_fit(self):
        rows = []
        for i in range(18):
            rows.append(
                {
                    "year_month": f"{2022 + i // 12}-{i % 12 + 1:02d}",
                    "category": "Food",
                    "flow_type": "expense",
                    "total_amount": 300.0 + 25.0 * i + (7.0 if i % 4 == 0 else 0.0),
                    "month_ordinal": i,
                }
            )
        result = compute_expense_trends(pd.DataFrame(rows))
        cat = result.sort_values("year_month")
        fitted = cat["trend_intercept"] + cat["trend_slope"] * cat["month_ordinal"]
        ols = sm.OLS(cat["total_amount"].to_numpy(), sm.add_constant(cat["month_ordinal"])).fit()
        assert fitted.to_numpy() == pytest.approx(ols.fittedvalues, rel=1e-6)

    def test_trend_line_matches_fit_when_category_starts_late(self):
        # "Food" spans ordinals 0-29; "Wedding" starts at ordinal 20, so its
        # grid positions (0..9) differ from its month_ordinals (20..29).
        rows = []
        for i in range(30):
            rows.append(
                {
                    "year_month": f"{2022 + i // 12}-{i % 12 + 1:02d}",
                    "category": "Food",
                    "flow_type": "expense",
                    "total_amount": 400.0 + (5.0 if i % 3 == 0 else 0.0),
                    "month_ordinal": i,
                }
            )
            if i >= 20:
                rows.append(
                    {
                        "year_month": f"{2022 + i // 12}-{i % 12 + 1:02d}",
                        "category": "Wedding",
                        "flow_type": "expense",
                        "total_amount": 5000.0 - 400.0 * (i - 20) + (90.0 if i % 2 else 0.0),
                        "month_ordinal": i,
                    }
                )
        result = compute_expense_trends(pd.DataFrame(rows))
        cat = result[result["category"] == "Wedding"].sort_values("year_month")
        assert cat["month_ordinal"].iloc[0] == 20
        fitted = cat["trend_intercept"] + cat["trend_slope"] * cat["month_ordinal"]
        ols = sm.OLS(cat["total_amount"].to_numpy(), sm.add_constant(cat["month_ordinal"])).fit()
        assert fitted.to_numpy() == pytest.approx(ols.fittedvalues.to_numpy(), rel=1e-6)


class TestFDRConstant:
    def test_fdr_q_is_point_ten(self):
        assert FDR_Q == 0.10


# ---------------------------------------------------------------------------
# Phase 3a — calibrated seasonality label
# ---------------------------------------------------------------------------


class TestSeasonalityLabel:
    def _grid(self, values_by_month: np.ndarray, start: str = "2022-01") -> pd.DataFrame:
        idx = pd.date_range(start, periods=len(values_by_month), freq="MS")
        return pd.DataFrame(
            {
                "year_month": idx.strftime("%Y-%m"),
                "category": "Food",
                "flow_type": "expense",
                "total_amount": values_by_month,
                "month_ordinal": np.arange(len(values_by_month)),
            }
        )

    def test_strong_month_pattern_is_seasonal(self):
        rng = np.random.default_rng(21)
        n = 96
        idx = pd.date_range("2015-01-01", periods=n, freq="MS")
        base = 500.0 + 3.0 * np.arange(n) + rng.normal(0, 30.0, n)
        values = base + np.where(idx.month == 12, 400.0, 0.0)
        result = compute_expense_trends(self._grid(values, start="2015-01"))
        assert bool(result["is_seasonal"].iloc[0]) is True

    def test_flat_noise_is_not_seasonal(self):
        rng = np.random.default_rng(22)
        n = 48
        values = rng.lognormal(5.0, 0.3, n)
        result = compute_expense_trends(self._grid(values))
        assert bool(result["is_seasonal"].iloc[0]) is False

    def test_short_grid_has_nan_seasonality(self):
        values = np.full(12, 400.0)
        result = compute_expense_trends(self._grid(values))
        assert pd.isna(result["seasonality_pvalue"].iloc[0])
        assert bool(result["is_seasonal"].iloc[0]) is False

    def test_seasonality_pvalue_adj_is_bh(self):
        rows = []
        for c in range(4):
            for i in range(36):
                rows.append(
                    {
                        "year_month": f"{2021 + i // 12}-{i % 12 + 1:02d}",
                        "category": f"Cat{c}",
                        "flow_type": "expense",
                        "total_amount": 300.0 + 10.0 * c + (200.0 if (i % 12) == 6 else 0.0),
                        "month_ordinal": i,
                    }
                )
        result = compute_expense_trends(pd.DataFrame(rows))
        per_cat = result.drop_duplicates("category").set_index("category")["seasonality_pvalue"]
        expected = multipletests(per_cat.to_numpy(), method="fdr_bh")[1]
        actual = result.drop_duplicates("category").set_index("category")[
            "seasonality_pvalue_adj"
        ]
        assert actual.to_numpy() == pytest.approx(expected)


def test_is_seasonal_noise_rate_is_calibrated():
    """200 i.i.d. lognormal series (n=60, CV≈0.6): at most 15% flagged."""
    rng = np.random.default_rng(123)
    n_cats = 200
    n_months = 60
    sigma = np.sqrt(np.log(1 + 0.6**2))
    rows = []
    for c in range(n_cats):
        values = rng.lognormal(np.log(500.0) - sigma**2 / 2, sigma, n_months)
        for i in range(n_months):
            rows.append(
                {
                    "year_month": f"{2018 + i // 12}-{i % 12 + 1:02d}",
                    "category": f"Noise{c:03d}",
                    "flow_type": "expense",
                    "total_amount": float(values[i]),
                    "month_ordinal": i,
                }
            )
    result = compute_expense_trends(pd.DataFrame(rows))
    per_cat = result.drop_duplicates("category")
    share = float(per_cat["is_seasonal"].astype(bool).mean())
    assert share <= 0.15, f"is_seasonal fired on {share:.0%} of noise series"
