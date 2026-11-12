"""Tests for src.analysis.category_prophet."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from src.analysis import category_prophet
from src.analysis.category_prophet import (
    build_category_monthly_series,
    compute_category_forecasts,
    compute_category_trends,
    compute_trend_metrics,
    forecast_series,
)
from src.analysis.feature_engineering import engineer_features
from src.analysis.stats_utils import bh_adjust


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_FORECAST_COLS = (
    "month",
    "point_estimate",
    "ci80_lower",
    "ci80_upper",
    "ci95_lower",
    "ci95_upper",
    "method",
)


def _monthly_series(n: int, start: str = "2024-01-01", value: float = 100.0) -> pd.Series:
    idx = pd.date_range(start, periods=n, freq="MS")
    return pd.Series([value] * n, index=idx)


def _make_forecast_frame(n: int = 36) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "month": pd.date_range("2026-01-01", periods=n, freq="MS"),
            "point_estimate": [500.0] * n,
            "ci80_lower": [400.0] * n,
            "ci80_upper": [600.0] * n,
            "ci95_lower": [300.0] * n,
            "ci95_upper": [700.0] * n,
            "method": ["Prophet"] * n,
        }
    )


def _transfer_row() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-15"),
                "account": "Checking",
                "to_account": "Savings",
                "payee": None,
                "category": "Transfer",
                "subcategory": None,
                "amount": -500.0,
                "type": "transfer",
                "currency": "USD",
                "notes": None,
                "transaction_id": 999,
            }
        ]
    )


# ---------------------------------------------------------------------------
# build_category_monthly_series
# ---------------------------------------------------------------------------


class TestBuildCategoryMonthlySeries:
    def test_has_expected_columns(self, sample_transactions):
        monthly = build_category_monthly_series(engineer_features(sample_transactions))
        assert list(monthly.columns) == [
            "flow_type",
            "level",
            "category",
            "subcategory",
            "month",
            "y",
        ]

    def test_flow_types_are_expense_and_income(self, sample_transactions):
        monthly = build_category_monthly_series(engineer_features(sample_transactions))
        assert set(monthly["flow_type"].unique()) == {"expense", "income"}

    def test_levels_are_category_and_subcategory(self, sample_transactions):
        monthly = build_category_monthly_series(engineer_features(sample_transactions))
        assert set(monthly["level"].unique()) == {"category", "subcategory"}

    def test_categories_preserved(self, sample_transactions):
        monthly = build_category_monthly_series(engineer_features(sample_transactions))
        assert set(monthly["category"].unique()) == {"Food", "Income"}

    def test_subcategory_level_rows_present(self, sample_transactions):
        monthly = build_category_monthly_series(engineer_features(sample_transactions))
        subcats = monthly[monthly["level"] == "subcategory"]["subcategory"].dropna().unique()
        assert set(subcats) == {"Groceries", "Salary"}

    def test_category_level_rows_have_none_subcategory(self, sample_transactions):
        monthly = build_category_monthly_series(engineer_features(sample_transactions))
        cat_rows = monthly[monthly["level"] == "category"]
        assert cat_rows["subcategory"].isna().all()

    def test_category_level_amounts_are_abs(self, sample_transactions):
        monthly = build_category_monthly_series(engineer_features(sample_transactions))
        food = monthly[
            (monthly["flow_type"] == "expense")
            & (monthly["level"] == "category")
            & (monthly["category"] == "Food")
        ]
        assert (food["y"] == 400.0).all()

    def test_excludes_transfers(self, sample_transactions):
        combined = pd.concat([sample_transactions, _transfer_row()], ignore_index=True)
        monthly = build_category_monthly_series(engineer_features(combined))
        assert "Transfer" not in monthly["category"].unique()

    def test_excludes_future_dated_rows(self, sample_transactions):
        future = sample_transactions.iloc[[0]].copy()
        future["date"] = pd.Timestamp("2030-01-01")
        future["category"] = "FutureOnly"
        combined = pd.concat([sample_transactions, future], ignore_index=True)

        featured = engineer_features(combined, as_of=date(2025, 3, 1))
        monthly = build_category_monthly_series(featured)

        assert "FutureOnly" not in monthly["category"].unique()

    def test_drops_partial_month(self, sample_transactions):
        # sample_transactions covers 2024-01..2025-02; treat 2025-02 as partial.
        featured = engineer_features(sample_transactions, as_of=date(2025, 2, 15))
        monthly = build_category_monthly_series(featured, as_of=date(2025, 2, 15))

        months = pd.to_datetime(monthly["month"])
        assert months.max() == pd.Timestamp("2025-01-01")


# ---------------------------------------------------------------------------
# compute_trend_metrics
# ---------------------------------------------------------------------------


class TestComputeTrendMetrics:
    def test_short_series_returns_insufficient_data(self):
        result = compute_trend_metrics(_monthly_series(5))
        assert result["direction"] == "insufficient_data"
        assert result["n_months"] == 5
        assert pd.isna(result["trend_change_pln"])
        assert pd.isna(result["growth_pct"])

    def test_insufficient_metrics_have_nan_trend(self):
        result = compute_trend_metrics(_monthly_series(3))
        assert pd.isna(result["trend_start"])
        assert pd.isna(result["trend_end"])
        assert pd.isna(result["avg_monthly_change_pln"])

    def test_rising_series_returns_positive_change_and_rising(self, monkeypatch):
        series = pd.Series(
            [100.0 + 50.0 * i for i in range(12)],
            index=pd.date_range("2024-01-01", periods=12, freq="MS"),
        )
        forecast = pd.DataFrame({"trend": [100.0 + 60.0 * i for i in range(12)]})
        monkeypatch.setattr(
            category_prophet,
            "_fit_prophet",
            lambda series, yearly_seasonality=False: (None, forecast),
        )

        result = compute_trend_metrics(series)

        assert result["direction"] == "rising"
        assert result["trend_change_pln"] > 0
        assert result["growth_pct"] > 10.0
        assert result["n_months"] == 12

    def test_falling_trend_detected(self, monkeypatch):
        series = pd.Series(
            [1000.0 - 50.0 * i for i in range(12)],
            index=pd.date_range("2024-01-01", periods=12, freq="MS"),
        )
        forecast = pd.DataFrame({"trend": [1000.0 - 60.0 * i for i in range(12)]})
        monkeypatch.setattr(
            category_prophet,
            "_fit_prophet",
            lambda series, yearly_seasonality=False: (None, forecast),
        )

        result = compute_trend_metrics(series)

        assert result["direction"] == "falling"
        assert result["trend_change_pln"] < 0

    def test_prophet_failure_still_computes_direction_from_ols(self, monkeypatch):
        def _boom(series, yearly_seasonality=False):
            raise RuntimeError("prophet unavailable")

        monkeypatch.setattr(category_prophet, "_fit_prophet", _boom)

        result = compute_trend_metrics(_monthly_series(12))

        # Decision D6: direction comes from the HAC OLS slope, so it survives a
        # failed Prophet fit; Prophet-derived metrics stay NaN.
        assert result["direction"] == "flat"
        assert pd.isna(result["trend_change_pln"])
        assert pd.notna(result["trend_pvalue"])


# ---------------------------------------------------------------------------
# forecast_series
# ---------------------------------------------------------------------------


class TestForecastSeries:
    def test_short_series_returns_none(self):
        assert forecast_series(_monthly_series(11), periods=36, min_months=12) is None

    def test_exactly_min_months_is_not_none(self, monkeypatch):
        mock = _make_forecast_frame(36)
        monkeypatch.setattr(
            category_prophet,
            "forecast_prophet",
            lambda hist, periods=36, yearly_seasonality=False: mock,
        )
        assert forecast_series(_monthly_series(12), periods=36, min_months=12) is not None

    def test_returns_36_rows_with_required_columns(self, monkeypatch):
        mock = _make_forecast_frame(36)
        monkeypatch.setattr(
            category_prophet,
            "forecast_prophet",
            lambda hist, periods=36, yearly_seasonality=False: mock,
        )

        result = forecast_series(_monthly_series(24), periods=36)

        assert result is not None
        assert len(result) == 36
        for col in REQUIRED_FORECAST_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_prophet_failure_returns_none(self, monkeypatch):
        def _boom(hist, periods=36, yearly_seasonality=False):
            raise RuntimeError("prophet unavailable")

        monkeypatch.setattr(category_prophet, "forecast_prophet", _boom)

        assert forecast_series(_monthly_series(24), periods=36) is None


# ---------------------------------------------------------------------------
# compute_category_trends / compute_category_forecasts
# ---------------------------------------------------------------------------

TREND_SUMMARY_COLS = (
    "flow_type",
    "level",
    "category",
    "subcategory",
    "n_months",
    "first_month",
    "last_month",
    "mean_monthly",
    "trend_start",
    "trend_end",
    "trend_change_pln",
    "avg_monthly_change_pln",
    "growth_pct",
    "direction",
    "trend_pvalue",
    "direction_pvalue_adj",
)

TREND_SERIES_COLS = ("flow_type", "level", "category", "subcategory", "month", "y", "trend")

FORECAST_COLS = (
    "flow_type",
    "level",
    "category",
    "subcategory",
    "months_ahead",
    "month",
    "point_estimate",
    "ci80_lower",
    "ci80_upper",
    "ci95_lower",
    "ci95_upper",
    "method",
)


def _fake_fit_prophet(series, yearly_seasonality=False):
    n = len(series)
    return None, pd.DataFrame({"trend": [float(i) for i in range(n)]})


class TestComputeCategoryTrends:
    def test_returns_dataframes_with_documented_columns(self, sample_transactions, monkeypatch):
        monkeypatch.setattr(category_prophet, "_fit_prophet", _fake_fit_prophet)

        summary, series_df = compute_category_trends(engineer_features(sample_transactions))

        assert isinstance(summary, pd.DataFrame)
        assert isinstance(series_df, pd.DataFrame)
        for col in TREND_SUMMARY_COLS:
            assert col in summary.columns, f"Missing column: {col}"
        for col in TREND_SERIES_COLS:
            assert col in series_df.columns, f"Missing column: {col}"

    def test_summary_has_both_levels(self, sample_transactions, monkeypatch):
        monkeypatch.setattr(category_prophet, "_fit_prophet", _fake_fit_prophet)

        summary, _ = compute_category_trends(engineer_features(sample_transactions))

        assert set(summary["level"].unique()) == {"category", "subcategory"}

    def test_series_trend_nan_when_insufficient(self, monkeypatch):
        # 5 months → below MIN_MONTHS_TREND → trend column all NaN, no Prophet fit
        short = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp(f"2024-{m:02d}-01"),
                    "account": "Checking",
                    "type": "withdrawal",
                    "category": "Food",
                    "subcategory": None,
                    "amount": -100.0,
                }
                for m in range(1, 6)
            ]
        )
        monkeypatch.setattr(category_prophet, "_fit_prophet", _fake_fit_prophet)

        summary, series_df = compute_category_trends(engineer_features(short))

        assert (summary["direction"] == "insufficient_data").all()
        assert series_df["trend"].isna().all()


class TestComputeCategoryForecasts:
    def test_returns_dataframe_with_documented_columns(self, sample_transactions):
        result = compute_category_forecasts(engineer_features(sample_transactions))

        assert isinstance(result, pd.DataFrame)
        for col in FORECAST_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_horizon_is_24_months(self, sample_transactions):
        result = compute_category_forecasts(engineer_features(sample_transactions))

        assert not result.empty
        assert result["months_ahead"].max() == 24
        assert result["months_ahead"].min() == 1

    def test_short_series_emit_no_forecast_rows(self):
        # 5 months -> below MIN_MONTHS_FORECAST -> no rows
        short = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp(f"2024-{m:02d}-01"),
                    "account": "Checking",
                    "type": "withdrawal",
                    "category": "Food",
                    "subcategory": None,
                    "amount": -100.0,
                }
                for m in range(1, 6)
            ]
        )

        result = compute_category_forecasts(engineer_features(short))

        assert result.empty


# ---------------------------------------------------------------------------
# Phase 3c — Prophet direction from a HAC OLS slope, BH within family
# ---------------------------------------------------------------------------


def _featured_from_series(series_by_category: dict[str, np.ndarray], start: str = "2018-01-01"):
    idx = pd.date_range(start, periods=len(next(iter(series_by_category.values()))), freq="MS")
    rows = []
    for category, values in series_by_category.items():
        for month, value in zip(idx, values):
            rows.append(
                {
                    "date": month,
                    "account": "Checking",
                    "type": "withdrawal",
                    "category": category,
                    "subcategory": None,
                    "amount": -float(value),
                    "year_month": month.strftime("%Y-%m"),
                }
            )
    return engineer_features(pd.DataFrame(rows), as_of=idx[-1] + pd.Timedelta(days=40))


def _flat_trend_fit(series, yearly_seasonality=False):
    n = len(series)
    return None, pd.DataFrame({"trend": np.full(n, float(series.iloc[0]))})


class TestDirectionCalibration:
    def test_noise_direction_rate_is_calibrated(self, monkeypatch):
        """200 i.i.d. lognormal series (n=60, CV≈0.6): ≤15% non-flat."""
        rng = np.random.default_rng(321)
        sigma = np.sqrt(np.log(1 + 0.6**2))
        series = {
            f"Noise{c:03d}": rng.lognormal(np.log(500.0) - sigma**2 / 2, sigma, 60)
            for c in range(200)
        }
        monkeypatch.setattr(category_prophet, "_fit_prophet", _flat_trend_fit)

        summary, _ = compute_category_trends(_featured_from_series(series))

        cat_rows = summary[(summary["flow_type"] == "expense") & (summary["level"] == "category")]
        share = float((cat_rows["direction"] != "flat").mean())
        assert share <= 0.15, f"direction non-flat on {share:.0%} of noise series"

    def test_strong_trend_is_detected(self, monkeypatch):
        series = {"Trend": 200.0 + 20.0 * np.arange(48)}
        monkeypatch.setattr(category_prophet, "_fit_prophet", _flat_trend_fit)

        summary, _ = compute_category_trends(_featured_from_series(series))

        row = summary[
            (summary["flow_type"] == "expense")
            & (summary["level"] == "category")
            & (summary["category"] == "Trend")
        ].iloc[0]
        assert row["direction"] == "rising"
        assert row["direction_pvalue_adj"] < 0.10
        assert row["trend_pvalue"] < 0.10

    def test_opposite_prophet_trend_is_mixed(self, monkeypatch):
        series = pd.Series(
            200.0 + 20.0 * np.arange(48),
            index=pd.date_range("2020-01-01", periods=48, freq="MS"),
        )
        descending = pd.DataFrame({"trend": 2000.0 - 20.0 * np.arange(48)})
        monkeypatch.setattr(
            category_prophet,
            "_fit_prophet",
            lambda s, yearly_seasonality=False: (None, descending),
        )

        summary, _ = compute_category_trends(
            _featured_from_series({"Trend": series.to_numpy()})
        )
        row = summary[(summary["level"] == "category") & (summary["category"] == "Trend")].iloc[0]

        assert row["direction"] == "mixed"

    def test_direction_pvalue_adj_is_bh_within_family(self, monkeypatch):
        rng = np.random.default_rng(9)
        series = {f"Cat{c:02d}": rng.lognormal(5.0, 0.4, 60) for c in range(8)}
        series["RealTrend"] = 200.0 + 10.0 * np.arange(60)
        monkeypatch.setattr(category_prophet, "_fit_prophet", _flat_trend_fit)

        summary, _ = compute_category_trends(_featured_from_series(series))

        family = summary[(summary["flow_type"] == "expense") & (summary["level"] == "category")]
        raw = family.set_index("category")["trend_pvalue"]
        expected = bh_adjust(raw)
        actual = family.set_index("category")["direction_pvalue_adj"]
        assert actual.to_numpy() == pytest.approx(expected.to_numpy())
