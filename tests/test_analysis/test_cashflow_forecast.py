"""Tests for src.analysis.cashflow_forecast."""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.analysis.cashflow_forecast import (
    compute_runway,
    forecast_holtwinters,
    forecast_rolling_mean,
    select_and_run_forecast,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CI_COLS = ("ci80_lower", "ci80_upper", "ci95_lower", "ci95_upper")
REQUIRED_COLS = ("month", "point_estimate") + CI_COLS + ("method",)


def _monthly_series(n: int, start: str = "2022-01-01", value: float = 1000.0) -> pd.Series:
    """Flat monthly Series with DatetimeIndex."""
    idx = pd.date_range(start, periods=n, freq="MS")
    return pd.Series([value] * n, index=idx)


def _rising_series(n: int, start: str = "2022-01-01") -> pd.Series:
    idx = pd.date_range(start, periods=n, freq="MS")
    return pd.Series([500.0 + 50.0 * i for i in range(n)], index=idx)


# ---------------------------------------------------------------------------
# forecast_rolling_mean
# ---------------------------------------------------------------------------


class TestForecastRollingMean:
    def test_returns_dataframe(self):
        result = forecast_rolling_mean(_monthly_series(6))
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self):
        result = forecast_rolling_mean(_monthly_series(6))
        for col in REQUIRED_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_note_column_present(self):
        result = forecast_rolling_mean(_monthly_series(6))
        assert "note" in result.columns

    def test_note_column_non_empty(self):
        result = forecast_rolling_mean(_monthly_series(6))
        assert result["note"].notna().all()

    def test_method_is_rolling_mean(self):
        result = forecast_rolling_mean(_monthly_series(6))
        assert (result["method"] == "rolling_mean").all()

    def test_returns_n_periods_rows(self):
        result = forecast_rolling_mean(_monthly_series(6), periods=12)
        assert len(result) == 12

    def test_ci_columns_are_nan(self):
        result = forecast_rolling_mean(_monthly_series(6))
        for col in CI_COLS:
            assert result[col].isna().all(), f"Expected NaN in {col}"

    def test_point_estimate_equals_rolling_mean(self):
        # Flat series of 500 with window=3 → rolling mean = 500
        result = forecast_rolling_mean(_monthly_series(10, value=500.0), window=3)
        assert (result["point_estimate"] == 500.0).all()

    def test_future_months_after_last_known_date(self):
        series = _monthly_series(6, start="2024-01-01")
        result = forecast_rolling_mean(series, periods=3)
        assert result["month"].min() > series.index[-1]

    def test_future_months_are_month_start(self):
        result = forecast_rolling_mean(_monthly_series(6), periods=3)
        for ts in result["month"]:
            assert ts.day == 1

    def test_custom_periods(self):
        for n in (1, 6, 24):
            result = forecast_rolling_mean(_monthly_series(8), periods=n)
            assert len(result) == n


# ---------------------------------------------------------------------------
# forecast_holtwinters
# ---------------------------------------------------------------------------


class TestForecastHoltWinters:
    def test_returns_dataframe(self):
        result = forecast_holtwinters(_monthly_series(18))
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self):
        result = forecast_holtwinters(_monthly_series(18))
        for col in REQUIRED_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_method_is_holt_winters(self):
        result = forecast_holtwinters(_monthly_series(18))
        assert (result["method"] == "Holt-Winters").all()

    def test_returns_n_periods_rows(self):
        result = forecast_holtwinters(_monthly_series(18), periods=6)
        assert len(result) == 6

    def test_ci_columns_are_finite(self):
        result = forecast_holtwinters(_monthly_series(18))
        for col in CI_COLS:
            assert result[col].notna().all(), f"NaN in {col}"

    def test_ci80_lower_le_point_estimate_on_average(self):
        # Bootstrapped CIs — lower bound should typically be below point estimate
        result = forecast_holtwinters(_monthly_series(18), n_bootstrap=200)
        assert result["ci80_lower"].mean() <= result["point_estimate"].mean()

    def test_ci80_upper_ge_point_estimate_on_average(self):
        result = forecast_holtwinters(_monthly_series(18), n_bootstrap=200)
        assert result["ci80_upper"].mean() >= result["point_estimate"].mean()

    def test_future_months_after_last_known_date(self):
        series = _monthly_series(18, start="2023-01-01")
        result = forecast_holtwinters(series, periods=6)
        assert result["month"].min() > series.index[-1]

    def test_custom_periods(self):
        for n in (1, 6, 12):
            result = forecast_holtwinters(_monthly_series(18), periods=n)
            assert len(result) == n

    def test_seasonal_model_used_for_24_months(self):
        # 24 months triggers seasonal component — should not raise
        result = forecast_holtwinters(_monthly_series(24), periods=3)
        assert len(result) == 3

    def test_rising_series_point_estimate_positive(self):
        result = forecast_holtwinters(_rising_series(18), periods=3)
        assert result["point_estimate"].mean() > 0


# ---------------------------------------------------------------------------
# select_and_run_forecast — method routing
# ---------------------------------------------------------------------------


class TestSelectAndRunForecast:
    def test_less_than_12_uses_rolling_mean(self):
        result = select_and_run_forecast(_monthly_series(8), periods=3)
        assert (result["method"] == "rolling_mean").all()

    def test_exactly_11_uses_rolling_mean(self):
        result = select_and_run_forecast(_monthly_series(11), periods=3)
        assert (result["method"] == "rolling_mean").all()

    def test_12_months_uses_holt_winters(self):
        result = select_and_run_forecast(_monthly_series(12), periods=3)
        assert (result["method"] == "Holt-Winters").all()

    def test_23_months_uses_holt_winters(self):
        result = select_and_run_forecast(_monthly_series(23), periods=3)
        assert (result["method"] == "Holt-Winters").all()

    def test_24_months_uses_prophet(self):
        """Prophet is called for >=24 months — mock to avoid heavy dependency."""
        mock_result = pd.DataFrame({
            "month": pd.date_range("2026-01-01", periods=3, freq="MS"),
            "point_estimate": [1000.0, 1100.0, 1200.0],
            "ci80_lower": [900.0, 950.0, 1000.0],
            "ci80_upper": [1100.0, 1200.0, 1350.0],
            "ci95_lower": [800.0, 850.0, 900.0],
            "ci95_upper": [1200.0, 1300.0, 1450.0],
            "method": ["Prophet"] * 3,
        })
        with patch(
            "src.analysis.cashflow_forecast.forecast_prophet",
            return_value=mock_result,
        ) as mock_prophet:
            result = select_and_run_forecast(_monthly_series(24), periods=3)
            mock_prophet.assert_called_once()

        assert (result["method"] == "Prophet").all()

    def test_returns_dataframe_with_required_columns(self):
        result = select_and_run_forecast(_monthly_series(15), periods=3)
        for col in REQUIRED_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_returns_correct_number_of_periods(self):
        for n_periods in (3, 6, 12):
            result = select_and_run_forecast(_monthly_series(15), periods=n_periods)
            assert len(result) == n_periods


# ---------------------------------------------------------------------------
# compute_runway
# ---------------------------------------------------------------------------


class TestComputeRunway:
    def _forecast_df(self, monthly_cf: float = 500.0, periods: int = 12) -> pd.DataFrame:
        """Forecast DataFrame with constant CI columns."""
        return pd.DataFrame({
            "month": pd.date_range("2026-01-01", periods=periods, freq="MS"),
            "point_estimate": [monthly_cf] * periods,
            "ci80_lower": [monthly_cf * 0.8] * periods,
            "ci80_upper": [monthly_cf * 1.2] * periods,
            "ci95_lower": [monthly_cf * 0.7] * periods,
            "ci95_upper": [monthly_cf * 1.3] * periods,
            "method": ["Holt-Winters"] * periods,
        })

    def _forecast_df_nan_ci(self, monthly_cf: float = 500.0, periods: int = 12) -> pd.DataFrame:
        """Rolling mean forecast — CI columns are NaN."""
        return pd.DataFrame({
            "month": pd.date_range("2026-01-01", periods=periods, freq="MS"),
            "point_estimate": [monthly_cf] * periods,
            "ci80_lower": [float("nan")] * periods,
            "ci80_upper": [float("nan")] * periods,
            "ci95_lower": [float("nan")] * periods,
            "ci95_upper": [float("nan")] * periods,
            "method": ["rolling_mean"] * periods,
        })

    def test_returns_dict(self):
        result = compute_runway(10_000.0, self._forecast_df())
        assert isinstance(result, dict)

    def test_has_three_scenario_keys(self):
        result = compute_runway(10_000.0, self._forecast_df())
        assert "pessimistic_months" in result
        assert "median_months" in result
        assert "optimistic_months" in result

    def test_positive_balance_full_runway(self):
        # Plenty of money, strongly positive cash flow → full period
        result = compute_runway(1_000_000.0, self._forecast_df(monthly_cf=1000.0))
        assert result["median_months"] == 12

    def test_balance_drains_returns_correct_months(self):
        # Balance 1500, monthly CF -500 → cumsum: [-500,-1000,-1500,...]
        # running balance: [1000, 500, 0, -500, ...] → positive for 2 months
        df = self._forecast_df(monthly_cf=-500.0, periods=12)
        result = compute_runway(1500.0, df)
        assert result["median_months"] == 2

    def test_zero_balance_no_runway(self):
        df = self._forecast_df(monthly_cf=-100.0, periods=12)
        result = compute_runway(0.0, df)
        assert result["median_months"] == 0

    def test_pessimistic_le_median_le_optimistic(self):
        # With positive CF: optimistic (higher CF) should give >= median >= pessimistic
        result = compute_runway(5_000.0, self._forecast_df(monthly_cf=200.0))
        assert result["pessimistic_months"] <= result["median_months"]
        assert result["median_months"] <= result["optimistic_months"]

    def test_nan_ci_uses_point_estimate_for_all_scenarios(self):
        # When CI is NaN all three scenarios should collapse to the same value
        result = compute_runway(3_000.0, self._forecast_df_nan_ci(monthly_cf=500.0))
        assert result["pessimistic_months"] == result["median_months"]
        assert result["median_months"] == result["optimistic_months"]

    def test_runway_capped_at_forecast_window(self):
        result = compute_runway(1_000_000.0, self._forecast_df(monthly_cf=1.0, periods=6))
        assert result["median_months"] <= 6

    def test_months_values_are_integers(self):
        result = compute_runway(10_000.0, self._forecast_df())
        for key in ("pessimistic_months", "median_months", "optimistic_months"):
            assert isinstance(result[key], int)
