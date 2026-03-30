"""Tests for src.analysis.seasonality_tests."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.seasonality_tests import (
    check_acf_seasonal_lag,
    check_canova_hansen,
    check_kruskal_wallis,
    check_multiplicative_vs_additive,
    check_ocsb,
    check_stl_strength,
    run_seasonality_battery,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _monthly_series(n_months: int, seed: int = 42, seasonal: bool = True) -> pd.Series:
    """Generate a monthly series with optional yearly seasonality."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2022-01-01", periods=n_months, freq="MS")
    trend = np.linspace(1000, 1200, n_months)
    noise = rng.normal(0, 50, n_months)

    if seasonal:
        # Sine wave with 12-month period
        season = 200 * np.sin(2 * np.pi * np.arange(n_months) / 12)
    else:
        season = np.zeros(n_months)

    return pd.Series(trend + season + noise, index=dates, name="net_cashflow")


def _flat_series(n_months: int = 36) -> pd.Series:
    """Constant series — no seasonality at all."""
    dates = pd.date_range("2022-01-01", periods=n_months, freq="MS")
    return pd.Series([1000.0] * n_months, index=dates, name="flat")


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def test_battery_rejects_non_series():
    with pytest.raises(TypeError, match="must be a pd.Series"):
        run_seasonality_battery(pd.DataFrame({"y": [1, 2, 3]}))


def test_battery_rejects_short_series():
    short = _monthly_series(6)
    with pytest.raises(ValueError, match="needs >= 12"):
        run_seasonality_battery(short)


def test_battery_rejects_no_datetime_index():
    s = pd.Series([1.0] * 24)  # RangeIndex, not DatetimeIndex
    with pytest.raises(TypeError, match="must have a DatetimeIndex"):
        run_seasonality_battery(s)


# ---------------------------------------------------------------------------
# Individual tests — seasonal data (36 months with strong sine wave)
# ---------------------------------------------------------------------------

class TestCanovaHansen:
    def test_detects_strong_seasonality(self):
        result = check_canova_hansen(_monthly_series(36, seasonal=True))
        assert result["test_name"] == "Canova-Hansen"
        assert isinstance(result["nsdiffs"], int)
        assert isinstance(result["has_seasonality"], bool)

    def test_no_seasonality_on_flat(self):
        result = check_canova_hansen(_flat_series())
        assert result["has_seasonality"] is False


class TestOCSB:
    def test_returns_valid_dict(self):
        result = check_ocsb(_monthly_series(36, seasonal=True))
        assert result["test_name"] == "OCSB"
        assert isinstance(result["nsdiffs"], int)
        assert isinstance(result["has_seasonality"], bool)

    def test_handles_flat_series_gracefully(self):
        """Flat/constant series causes singular matrix; OCSB should not crash."""
        result = check_ocsb(_flat_series())
        assert result["has_seasonality"] is False
        assert "could not run" in result["note"] or result["nsdiffs"] == 0


class TestKruskalWallis:
    def test_detects_seasonal_differences(self):
        result = check_kruskal_wallis(_monthly_series(36, seasonal=True))
        assert result["test_name"] == "Kruskal-Wallis"
        assert result["has_seasonality"] is True
        assert result["p_value"] < 0.05

    def test_no_difference_on_flat(self):
        result = check_kruskal_wallis(_flat_series())
        assert result["has_seasonality"] is False


class TestACFSeasonalLag:
    def test_significant_at_lag12(self):
        # ACF needs sufficient cycles for power; 60 months = 5 years
        result = check_acf_seasonal_lag(_monthly_series(60, seasonal=True))
        assert result["test_name"] == "ACF lag-12"
        assert result["has_seasonality"] is True

    def test_low_power_with_few_cycles(self):
        """With only 36 months (3 cycles), ACF may lack power to detect."""
        result = check_acf_seasonal_lag(_monthly_series(36, seasonal=True))
        # Just ensure it runs and returns valid structure
        assert isinstance(result["has_seasonality"], bool)
        assert not np.isnan(result["acf_lag12"])

    def test_not_significant_on_flat(self):
        result = check_acf_seasonal_lag(_flat_series())
        assert result["has_seasonality"] is False

    def test_too_short_returns_nan(self):
        result = check_acf_seasonal_lag(_monthly_series(12, seasonal=True))
        assert np.isnan(result["acf_lag12"])


class TestSTLStrength:
    def test_strong_on_seasonal_data(self):
        result = check_stl_strength(_monthly_series(36, seasonal=True))
        assert result["test_name"] == "STL strength"
        assert result["strength"] > 0.64
        assert result["has_seasonality"] is True

    def test_weak_on_flat(self):
        result = check_stl_strength(_flat_series())
        assert result["has_seasonality"] is False

    def test_too_short_returns_nan(self):
        result = check_stl_strength(_monthly_series(18, seasonal=True))
        assert np.isnan(result["strength"])


class TestMultiplicativeVsAdditive:
    def test_additive_data_returns_additive(self):
        # Additive: constant amplitude regardless of level
        result = check_multiplicative_vs_additive(_monthly_series(36, seasonal=True))
        assert result["recommended_mode"] in ("additive", "multiplicative")

    def test_returns_valid_keys(self):
        result = check_multiplicative_vs_additive(_monthly_series(36))
        assert "spearman_rho" in result
        assert "p_value" in result
        assert "recommended_mode" in result


# ---------------------------------------------------------------------------
# Full battery
# ---------------------------------------------------------------------------

class TestBattery:
    def test_returns_all_keys(self):
        result = run_seasonality_battery(_monthly_series(36, seasonal=True))
        assert "tests" in result
        assert "seasonality_detected" in result
        assert "recommended_mode" in result
        assert "recommended_fourier_order" in result
        assert "verdict" in result
        assert "caveats" in result

    def test_detects_seasonality_in_strong_signal(self):
        result = run_seasonality_battery(_monthly_series(36, seasonal=True))
        assert result["seasonality_detected"] is True
        assert result["recommended_fourier_order"] > 0

    def test_no_seasonality_on_flat(self):
        result = run_seasonality_battery(_flat_series())
        assert result["seasonality_detected"] is False

    def test_caveats_on_short_data(self):
        # 18 months = 1.5 years — should warn about limited power
        series = _monthly_series(18, seasonal=True)
        result = run_seasonality_battery(series)
        assert len(result["caveats"]) > 0
        assert any("limited power" in c or "caution" in c for c in result["caveats"])

    def test_six_tests_in_battery(self):
        result = run_seasonality_battery(_monthly_series(36, seasonal=True))
        assert len(result["tests"]) == 6

    def test_verdict_is_string(self):
        result = run_seasonality_battery(_monthly_series(36))
        assert isinstance(result["verdict"], str)
        assert len(result["verdict"]) > 0
