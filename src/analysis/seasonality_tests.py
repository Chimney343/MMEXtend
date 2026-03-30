"""
Statistical tests for seasonality detection in monthly time series.

Runs a battery of complementary tests and returns a consolidated verdict
that can drive Prophet parameter selection (additive vs multiplicative,
Fourier order, whether to enable yearly_seasonality at all).

Usage:
    from src.analysis.seasonality_tests import run_seasonality_battery
    results = run_seasonality_battery(monthly_net_cashflow)
    print(results["verdict"])

Tests included:
    1. Canova-Hansen (CH) — seasonal unit-root test (pmdarima)
    2. OCSB — Osborn-Chui-Smith-Birchenhall test (pmdarima)
    3. Kruskal-Wallis H — non-parametric month-group comparison (scipy)
    4. ACF at lag 12 — autocorrelation at the yearly period (statsmodels)
    5. STL strength of seasonality — variance ratio Fₛ (statsmodels)
    6. Multiplicative vs additive — coefficient of variation by month
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy import stats as sp_stats
from statsmodels.tsa.seasonal import STL
from statsmodels.tsa.stattools import acf

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

MIN_YEARS_FOR_RELIABLE_TESTS = 2
MONTHS_PER_YEAR = 12


def _require_monthly_series(series: pd.Series, name: str = "series") -> None:
    """Validate that the input is a monthly-frequency pd.Series."""
    if not isinstance(series, pd.Series):
        raise TypeError(f"{name} must be a pd.Series, got {type(series).__name__}")
    if not isinstance(series.index, pd.DatetimeIndex):
        raise TypeError(f"{name} must have a DatetimeIndex")
    if len(series) < MONTHS_PER_YEAR:
        raise ValueError(
            f"{name} needs >= {MONTHS_PER_YEAR} observations for seasonality "
            f"tests, got {len(series)}"
        )


# ── Individual tests ──────────────────────────────────────────────────────────


def check_canova_hansen(series: pd.Series) -> dict[str, Any]:
    """
    Canova-Hansen test for seasonal stability.

    H0: seasonal pattern is stable (no seasonal unit root).
    If nsdiffs > 0, the series needs seasonal differencing → strong seasonality.

    Parameters
    ----------
    series : pd.Series
        Monthly time series with DatetimeIndex.

    Returns
    -------
    dict with keys: test_name, nsdiffs, has_seasonality, note.
    """
    from pmdarima.arima import CHTest

    ch = CHTest(m=MONTHS_PER_YEAR)
    nsdiffs = ch.estimate_seasonal_differencing_term(series.values)
    has_seasonality = bool(nsdiffs > 0)

    return {
        "test_name": "Canova-Hansen",
        "nsdiffs": int(nsdiffs),
        "has_seasonality": has_seasonality,
        "note": (
            f"Seasonal differences needed: {nsdiffs}. "
            f"{'Significant' if has_seasonality else 'No significant'} "
            "seasonal pattern detected."
        ),
    }


def check_ocsb(series: pd.Series) -> dict[str, Any]:
    """
    OCSB test for seasonal differencing.

    Osborn, Chui, Smith & Birchenhall (1988). Tests whether the series
    requires seasonal differencing. If nsdiffs > 0 → seasonality present.

    Parameters
    ----------
    series : pd.Series
        Monthly time series with DatetimeIndex.

    Returns
    -------
    dict with keys: test_name, nsdiffs, has_seasonality, note.
    """
    from pmdarima.arima import OCSBTest

    try:
        ocsb = OCSBTest(m=MONTHS_PER_YEAR)
        nsdiffs = ocsb.estimate_seasonal_differencing_term(series.values)
    except ValueError as exc:
        logger.warning(f"OCSB test failed: {exc}")
        return {
            "test_name": "OCSB",
            "nsdiffs": 0,
            "has_seasonality": False,
            "note": f"OCSB test could not run: {exc}",
        }

    has_seasonality = bool(nsdiffs > 0)

    return {
        "test_name": "OCSB",
        "nsdiffs": int(nsdiffs),
        "has_seasonality": has_seasonality,
        "note": (
            f"Seasonal differences needed: {nsdiffs}. "
            f"{'Significant' if has_seasonality else 'No significant'} "
            "seasonal pattern detected."
        ),
    }


def check_kruskal_wallis(series: pd.Series) -> dict[str, Any]:
    """
    Kruskal-Wallis H-test comparing distributions across months.

    Non-parametric test: are the monthly distributions significantly different?
    H0: all month groups have the same distribution.

    Parameters
    ----------
    series : pd.Series
        Monthly time series with DatetimeIndex.

    Returns
    -------
    dict with keys: test_name, statistic, p_value, has_seasonality, note.
    """
    month_groups = [group.values for _, group in series.groupby(series.index.month)]
    # Need at least 2 groups with data
    month_groups = [g for g in month_groups if len(g) > 0]

    if len(month_groups) < 2:
        return {
            "test_name": "Kruskal-Wallis",
            "statistic": float("nan"),
            "p_value": float("nan"),
            "has_seasonality": False,
            "note": "Insufficient month groups for comparison.",
        }

    stat, p_value = sp_stats.kruskal(*month_groups)
    has_seasonality = bool(p_value < 0.05)

    return {
        "test_name": "Kruskal-Wallis",
        "statistic": round(float(stat), 4),
        "p_value": round(float(p_value), 4),
        "has_seasonality": has_seasonality,
        "note": (
            f"H={stat:.4f}, p={p_value:.4f}. "
            f"{'Significant' if has_seasonality else 'No significant'} "
            "difference between monthly distributions (alpha=0.05)."
        ),
    }


def check_acf_seasonal_lag(series: pd.Series) -> dict[str, Any]:
    """
    Check autocorrelation at lag 12 (yearly cycle for monthly data).

    Uses Bartlett's formula for the confidence band. ACF at lag 12 is
    significant if its confidence interval does not include zero.

    Parameters
    ----------
    series : pd.Series
        Monthly time series with DatetimeIndex.

    Returns
    -------
    dict with keys: test_name, acf_lag12, conf_bound, has_seasonality, note.
    """
    n = len(series)
    if n <= MONTHS_PER_YEAR:
        return {
            "test_name": "ACF lag-12",
            "acf_lag12": float("nan"),
            "conf_bound": float("nan"),
            "has_seasonality": False,
            "note": f"Need > {MONTHS_PER_YEAR} observations, got {n}.",
        }

    nlags = min(MONTHS_PER_YEAR, n - 1)
    acf_values, confint = acf(series.values, nlags=nlags, alpha=0.05)

    acf_12 = float(acf_values[MONTHS_PER_YEAR])
    # confint rows are [lower, upper] for each lag's ACF.
    # Significant if the confidence interval excludes zero.
    ci_lower = float(confint[MONTHS_PER_YEAR, 0])
    ci_upper = float(confint[MONTHS_PER_YEAR, 1])
    has_seasonality = bool(ci_lower > 0 or ci_upper < 0)

    # Report the approximate half-width as the bound for display
    conf_bound = (ci_upper - ci_lower) / 2

    return {
        "test_name": "ACF lag-12",
        "acf_lag12": round(acf_12, 4),
        "conf_bound": round(conf_bound, 4),
        "has_seasonality": has_seasonality,
        "note": (
            f"ACF(12)={acf_12:.4f}, 95% CI=[{ci_lower:.4f}, {ci_upper:.4f}]. "
            f"{'Significant' if has_seasonality else 'No significant'} "
            "yearly autocorrelation."
        ),
    }


def check_stl_strength(series: pd.Series) -> dict[str, Any]:
    """
    STL decomposition strength-of-seasonality metric.

    Wang, Smith & Hyndman (2006): Fₛ = 1 - Var(R) / Var(S + R)
    where S = seasonal component, R = remainder from STL.
    Fₛ > 0.64 is conventionally considered moderate-to-strong seasonality.

    Parameters
    ----------
    series : pd.Series
        Monthly time series with DatetimeIndex.

    Returns
    -------
    dict with keys: test_name, strength, has_seasonality, note.
    """
    n = len(series)
    if n < 2 * MONTHS_PER_YEAR:
        return {
            "test_name": "STL strength",
            "strength": float("nan"),
            "has_seasonality": False,
            "note": (
                f"STL needs >= {2 * MONTHS_PER_YEAR} observations "
                f"for reliable decomposition, got {n}."
            ),
        }

    stl = STL(series, period=MONTHS_PER_YEAR, robust=True)
    result = stl.fit()

    seasonal = result.seasonal
    remainder = result.resid
    sr = seasonal + remainder

    var_r = np.var(remainder)
    var_sr = np.var(sr)

    strength = 1.0 - (var_r / var_sr) if var_sr > 0 else 0.0
    strength = max(0.0, strength)  # clip at 0

    # Hyndman & Athanasopoulos threshold
    has_seasonality = bool(strength > 0.64)

    label = (
        "strong" if strength > 0.80
        else "moderate" if strength > 0.64
        else "weak" if strength > 0.40
        else "negligible"
    )

    return {
        "test_name": "STL strength",
        "strength": round(float(strength), 4),
        "has_seasonality": has_seasonality,
        "note": f"Fₛ={strength:.4f} ({label}). Threshold for significance: 0.64.",
    }


def check_multiplicative_vs_additive(series: pd.Series) -> dict[str, str]:
    """
    Heuristic: if the coefficient of variation of monthly std-devs scales
    with the level, multiplicative seasonality is more appropriate.

    Compares the correlation between monthly means and monthly std-devs.
    If Spearman rho > 0.5 and p < 0.10, seasonal amplitude scales with
    level → multiplicative mode is preferred.

    Parameters
    ----------
    series : pd.Series
        Monthly time series with DatetimeIndex.

    Returns
    -------
    dict with keys: test_name, spearman_rho, p_value, recommended_mode, note.
    """
    monthly = series.groupby(series.index.month)
    means = monthly.mean()
    stds = monthly.std()

    # Need enough months with variation
    valid = stds.notna() & means.notna() & (stds > 0)
    if valid.sum() < 4:
        return {
            "test_name": "Additive vs Multiplicative",
            "spearman_rho": float("nan"),
            "p_value": float("nan"),
            "recommended_mode": "additive",
            "note": "Too few month groups with variation to assess. Defaulting to additive.",
        }

    rho, p_value = sp_stats.spearmanr(means[valid], stds[valid])

    if rho > 0.5 and p_value < 0.10:
        mode = "multiplicative"
    else:
        mode = "additive"

    return {
        "test_name": "Additive vs Multiplicative",
        "spearman_rho": round(float(rho), 4),
        "p_value": round(float(p_value), 4),
        "recommended_mode": mode,
        "note": (
            f"Spearman ρ={rho:.4f} (p={p_value:.4f}) between monthly means and "
            f"std-devs. {'Amplitude scales with level → multiplicative.' if mode == 'multiplicative' else 'No clear scaling → additive.'}"
        ),
    }


# ── Battery runner ────────────────────────────────────────────────────────────


def run_seasonality_battery(
    monthly_series: pd.Series,
    alpha: float = 0.05,
) -> dict[str, Any]:
    """
    Run a full battery of seasonality tests and return a consolidated verdict.

    Parameters
    ----------
    monthly_series : pd.Series
        Monthly time series with DatetimeIndex (e.g. net cashflow).
    alpha : float
        Significance level for individual tests.

    Returns
    -------
    dict with keys:
        - tests: list[dict]      — individual test results
        - seasonality_detected: bool
        - recommended_mode: str  — "additive" or "multiplicative"
        - recommended_fourier_order: int
        - verdict: str           — human-readable summary
        - caveats: list[str]     — statistical warnings

    Notes
    -----
    With < 3 full years of monthly data, power is limited for all tests.
    This is stated explicitly per the statistical analysis skill guidelines.
    """
    _require_monthly_series(monthly_series, "monthly_series")
    n = len(monthly_series)
    n_years = n / MONTHS_PER_YEAR

    logger.info(
        f"Running seasonality test battery on {n} monthly observations "
        f"({n_years:.1f} years)"
    )

    # Run all tests
    test_results = []
    test_results.append(check_canova_hansen(monthly_series))
    test_results.append(check_ocsb(monthly_series))
    test_results.append(check_kruskal_wallis(monthly_series))
    test_results.append(check_acf_seasonal_lag(monthly_series))
    test_results.append(check_stl_strength(monthly_series))

    mode_result = check_multiplicative_vs_additive(monthly_series)
    test_results.append(mode_result)

    # Tally votes
    seasonality_votes = sum(
        1 for t in test_results
        if t.get("has_seasonality") is True
    )
    # mode_result doesn't vote on has_seasonality, so count from first 5
    total_voting_tests = sum(
        1 for t in test_results
        if "has_seasonality" in t and t["has_seasonality"] is not None
    )

    seasonality_detected = seasonality_votes >= (total_voting_tests / 2)

    # Fourier order recommendation based on STL strength & data length
    stl_result = next(
        (t for t in test_results if t["test_name"] == "STL strength"), None
    )
    stl_strength = stl_result["strength"] if stl_result else 0.0

    if not seasonality_detected or np.isnan(stl_strength):
        recommended_fourier = 0  # disable yearly seasonality
    elif stl_strength > 0.80:
        recommended_fourier = min(6, max(2, int(n_years)))
    elif stl_strength > 0.64:
        recommended_fourier = min(4, max(2, int(n_years)))
    else:
        recommended_fourier = 2  # minimal

    recommended_mode = mode_result.get("recommended_mode", "additive")

    # Caveats
    caveats: list[str] = []
    if n_years < MIN_YEARS_FOR_RELIABLE_TESTS:
        caveats.append(
            f"Only {n_years:.1f} years of data. Tests have limited power; "
            "results may not be reliable."
        )
    if n_years < 3:
        caveats.append(
            f"With {n:.0f} monthly observations ({n_years:.1f} years), "
            "Kruskal-Wallis has very few observations per month group. "
            "Interpret with caution."
        )

    verdict_parts = [
        f"Seasonality {'DETECTED' if seasonality_detected else 'NOT detected'} "
        f"({seasonality_votes}/{total_voting_tests} tests positive).",
    ]
    if seasonality_detected:
        verdict_parts.append(
            f"Recommended: yearly_seasonality={recommended_fourier} (Fourier order), "
            f"seasonality_mode='{recommended_mode}'."
        )
    else:
        verdict_parts.append(
            "Recommended: yearly_seasonality=False for Prophet, or Fourier order=0."
        )
    if caveats:
        verdict_parts.append("⚠ " + " ".join(caveats))

    verdict = " ".join(verdict_parts)
    logger.info(f"Seasonality verdict: {verdict}")

    return {
        "tests": test_results,
        "seasonality_detected": seasonality_detected,
        "recommended_mode": recommended_mode,
        "recommended_fourier_order": recommended_fourier,
        "verdict": verdict,
        "caveats": caveats,
    }
