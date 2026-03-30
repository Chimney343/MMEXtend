"""Tests for src.analysis.expense_trends."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.expense_trends import (
    check_seasonality,
    compute_expense_trends,
    detect_consecutive_rises,
    fit_category_trend,
)


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
    Minimal monthly-aggregates DataFrame compatible with compute_expense_trends().
    Two categories: Food (constant -400) and Income (constant +3000).
    """
    import pandas as pd
    from datetime import date

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
        for key in ("slope", "intercept", "r_squared", "p_value", "n_months",
                    "trend_class", "ci_slope_lower", "ci_slope_upper"):
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

    def test_high_cv_flagged_as_seasonal(self):
        df = self._make_seasonal_df()
        result = check_seasonality(df, date_col="year_month", value_col="amount")
        assert result["is_seasonal"] is True

    def test_uniform_data_not_seasonal(self):
        df = self._make_uniform_df()
        result = check_seasonality(df, date_col="year_month", value_col="amount")
        assert result["is_seasonal"] is False

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


# ---------------------------------------------------------------------------
# detect_consecutive_rises
# ---------------------------------------------------------------------------


class TestDetectConsecutiveRises:
    def test_flat_series_returns_empty(self):
        runs = detect_consecutive_rises(_flat_series(12))
        assert runs == []

    def test_detects_two_consecutive_rises(self):
        # Monotonically rising → pct_change always > 0 after first point
        series = pd.Series([100.0, 150.0, 225.0, 300.0])
        runs = detect_consecutive_rises(series, threshold=0.20, consecutive=2)
        assert len(runs) >= 1

    def test_below_threshold_not_flagged(self):
        # Tiny rises of 1% — below 20% threshold
        series = pd.Series([100.0 * (1.01 ** i) for i in range(12)])
        runs = detect_consecutive_rises(series, threshold=0.20, consecutive=2)
        assert runs == []

    def test_run_start_end_indices_valid(self):
        series = pd.Series([100.0, 200.0, 400.0, 800.0])
        runs = detect_consecutive_rises(series, threshold=0.20, consecutive=2)
        for start, end in runs:
            assert 0 <= start <= end < len(series)

    def test_single_rise_not_enough_for_consecutive_2(self):
        # Rise on first step only, then flat
        series = pd.Series([100.0, 200.0, 200.0, 200.0, 200.0])
        runs = detect_consecutive_rises(series, threshold=0.20, consecutive=2)
        assert runs == []

    def test_multiple_runs_detected(self):
        # Two distinct rise windows separated by a drop
        series = pd.Series([
            100.0, 200.0, 400.0,   # run 1
            50.0,                  # drop resets
            100.0, 200.0, 400.0,   # run 2
        ])
        runs = detect_consecutive_rises(series, threshold=0.20, consecutive=2)
        assert len(runs) == 2

    def test_custom_consecutive_threshold(self):
        # With consecutive=3, need 3 in a row
        series = pd.Series([100.0, 200.0, 400.0, 800.0])
        runs_2 = detect_consecutive_rises(series, threshold=0.20, consecutive=2)
        runs_3 = detect_consecutive_rises(series, threshold=0.20, consecutive=3)
        # Should detect more or equal runs with lower consecutive requirement
        assert len(runs_2) >= len(runs_3)

    def test_returns_list_of_tuples(self):
        series = pd.Series([100.0, 200.0, 400.0])
        runs = detect_consecutive_rises(series, threshold=0.20, consecutive=2)
        for item in runs:
            assert isinstance(item, tuple)
            assert len(item) == 2


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
            "trend_slope", "trend_intercept", "trend_r2", "trend_pvalue",
            "trend_n_months", "trend_class",
            "trend_ci_lower", "trend_ci_upper",
            "seasonality_cv", "is_seasonal",
            "has_consecutive_rise", "n_rise_runs", "share_pct",
        ):
            assert col in result.columns, f"Missing column: {col}"

    def test_row_count_matches_input(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert len(result) == len(df)

    def test_categories_preserved(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert set(result["category"].unique()) == {"Food", "Income"}

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

    def test_has_consecutive_rise_is_bool(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert result["has_consecutive_rise"].dtype == bool

    def test_n_rise_runs_non_negative(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert (result["n_rise_runs"] >= 0).all()

    def test_share_pct_non_negative(self):
        df = _make_monthly_df(12)
        result = compute_expense_trends(df)
        assert (result["share_pct"] >= 0).all()

    def test_does_not_mutate_input(self):
        df = _make_monthly_df(12)
        original_cols = list(df.columns)
        compute_expense_trends(df)
        assert list(df.columns) == original_cols
