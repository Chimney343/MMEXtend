"""Tests for src.analysis.cashflow_forecast."""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

from src.analysis.cashflow_forecast import (
    build_prophet_model,
    compute_runway,
    forecast_holtwinters,
    forecast_prophet,
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

    def test_cumulative_bands_present_and_path_based(self):
        result = forecast_holtwinters(_monthly_series(18), periods=6, n_bootstrap=200)
        for col in ("cum_point", "cum_ci80_lower", "cum_ci80_upper", "interval_method"):
            assert col in result.columns
        assert result["cum_point"].tolist() == pytest.approx(
            result["point_estimate"].cumsum().tolist()
        )
        assert (result["cum_ci80_lower"] <= result["cum_ci80_upper"]).all()
        assert (result["interval_method"] == "model").all()

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
        mock_result = pd.DataFrame(
            {
                "month": pd.date_range("2026-01-01", periods=3, freq="MS"),
                "point_estimate": [1000.0, 1100.0, 1200.0],
                "ci80_lower": [900.0, 950.0, 1000.0],
                "ci80_upper": [1100.0, 1200.0, 1350.0],
                "ci95_lower": [800.0, 850.0, 900.0],
                "ci95_upper": [1200.0, 1300.0, 1450.0],
                "method": ["Prophet"] * 3,
            }
        )
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
# Prophet (Phase 4) — no holidays, per-path cumulative quantiles
# ---------------------------------------------------------------------------


class _FakeProphet:
    """Minimal Prophet stand-in: controlled samples and a linear trend."""

    def __init__(self, samples):
        self._samples = samples
        self.holidays = None
        self.fitted_hist = None

    def fit(self, hist):
        self.fitted_hist = hist
        return self

    def make_future_dataframe(self, periods, freq="MS"):
        dates = pd.date_range(
            start=self.fitted_hist["ds"].iloc[0],
            periods=len(self.fitted_hist) + periods,
            freq=freq,
        )
        return pd.DataFrame({"ds": dates})

    def predict(self, future):
        n = len(future)
        return pd.DataFrame({"ds": future["ds"].values, "yhat": np.arange(n, dtype=float)})

    def predictive_samples(self, future):
        return {"yhat": self._samples}


def test_prophet_cumulative_quantiles_equal_path_cumsum_percentiles(monkeypatch):
    pytest.importorskip("prophet")
    n_hist, periods, n_samples = 24, 6, 50
    rng = np.random.default_rng(4)
    samples = rng.normal(0.0, 1.0, (n_hist + periods, n_samples))

    monkeypatch.setattr(
        "src.analysis.cashflow_forecast.build_prophet_model",
        lambda yearly_seasonality=False: _FakeProphet(samples),
    )

    hist = pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=n_hist, freq="MS"),
            "y": np.arange(n_hist, dtype=float),
        }
    )
    result = forecast_prophet(hist, periods=periods)

    future_samples = samples[n_hist:, :]
    cumulative = np.cumsum(future_samples, axis=0)
    assert result["cum_ci80_lower"].to_numpy() == pytest.approx(
        np.percentile(cumulative, 10, axis=1)
    )
    assert result["cum_ci80_upper"].to_numpy() == pytest.approx(
        np.percentile(cumulative, 90, axis=1)
    )
    assert result["cum_point"].to_numpy() == pytest.approx(
        np.cumsum(np.arange(n_hist, n_hist + periods, dtype=float))
    )
    assert (result["interval_method"] == "model").all()
    # Summed monthly quantiles are not the same as quantiles of the cumsum.
    assert result["cum_ci80_upper"].iloc[-1] != pytest.approx(
        result["ci80_upper"].sum()
    )


def test_prophet_has_no_country_holidays(monkeypatch):
    pytest.importorskip("prophet")
    model = build_prophet_model()
    assert model.holidays is None
    assert not getattr(model, "country_holidays", None)


def test_category_prophet_fit_has_no_holidays():
    pytest.importorskip("prophet")
    from src.analysis.category_prophet import _fit_prophet

    series = pd.Series(
        np.arange(24, dtype=float),
        index=pd.date_range("2024-01-01", periods=24, freq="MS"),
    )
    model, _ = _fit_prophet(series)
    assert model.holidays is None
    assert not getattr(model, "country_holidays", None)


# ---------------------------------------------------------------------------
# compute_runway
# ---------------------------------------------------------------------------


class TestComputeRunway:
    def _forecast_df(self, monthly_cf: float = 500.0, periods: int = 12) -> pd.DataFrame:
        """Forecast DataFrame with constant CI columns."""
        return pd.DataFrame(
            {
                "month": pd.date_range("2026-01-01", periods=periods, freq="MS"),
                "point_estimate": [monthly_cf] * periods,
                "ci80_lower": [monthly_cf * 0.8] * periods,
                "ci80_upper": [monthly_cf * 1.2] * periods,
                "ci95_lower": [monthly_cf * 0.7] * periods,
                "ci95_upper": [monthly_cf * 1.3] * periods,
                "method": ["Holt-Winters"] * periods,
            }
        )

    def _forecast_df_nan_ci(self, monthly_cf: float = 500.0, periods: int = 12) -> pd.DataFrame:
        """Rolling mean forecast — CI columns are NaN."""
        return pd.DataFrame(
            {
                "month": pd.date_range("2026-01-01", periods=periods, freq="MS"),
                "point_estimate": [monthly_cf] * periods,
                "ci80_lower": [float("nan")] * periods,
                "ci80_upper": [float("nan")] * periods,
                "ci95_lower": [float("nan")] * periods,
                "ci95_upper": [float("nan")] * periods,
                "method": ["rolling_mean"] * periods,
            }
        )

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

    # ── Fix 4: negative-balance note ──────────────────────────────────────

    def test_negative_starting_balance_emits_note(self):
        """When starting balance is negative, a 'note' key must be present
        and its value must describe the negative-balance condition."""
        result = compute_runway(-1_000.0, self._forecast_df(monthly_cf=500.0))
        assert "note" in result
        assert "negative" in result["note"].lower()

    def test_zero_starting_balance_emits_note(self):
        """Zero balance is also an edge case that should emit a note."""
        result = compute_runway(0.0, self._forecast_df(monthly_cf=500.0))
        assert "note" in result

    def test_positive_starting_balance_has_no_note(self):
        """Positive starting balance must NOT add a note key to the result."""
        result = compute_runway(5_000.0, self._forecast_df())
        assert "note" not in result

    def test_uses_cumulative_band_when_present(self):
        """Cumulative quantiles are used directly, never summed monthly bands."""
        periods = 4
        df = pd.DataFrame(
            {
                "month": pd.date_range("2026-01-01", periods=periods, freq="MS"),
                "point_estimate": [100.0] * periods,
                "ci80_lower": [float("nan")] * periods,
                "ci80_upper": [float("nan")] * periods,
                "cum_point": [100.0, 200.0, 300.0, 400.0],
                "cum_ci80_lower": [-50.0, 20.0, 90.0, 160.0],
                "cum_ci80_upper": [150.0, 260.0, 370.0, 480.0],
            }
        )
        result = compute_runway(100.0, df)
        # Pessimistic balance: [50, 120, 190, 260] → all positive → 4.
        assert result["pessimistic_months"] == 4
        # Summed monthly ci80 would be NaN here; the point path would give 4 too.
        assert result["median_months"] == 4

    def test_cumulative_band_can_shorten_pessimistic_runway(self):
        periods = 3
        df = pd.DataFrame(
            {
                "month": pd.date_range("2026-01-01", periods=periods, freq="MS"),
                "point_estimate": [100.0] * periods,
                "cum_point": [100.0, 200.0, 300.0],
                "cum_ci80_lower": [-500.0, -400.0, -300.0],
                "cum_ci80_upper": [700.0, 800.0, 900.0],
            }
        )
        result = compute_runway(0.0, df)
        assert result["pessimistic_months"] == 0
        assert result["optimistic_months"] == 3
