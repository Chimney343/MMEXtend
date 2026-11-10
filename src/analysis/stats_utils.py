"""
Shared statistical primitives for the analysis layer.

The two recurring problems these solve:

- Monthly observations are serially correlated, so OLS standard errors are too
  small. ``fit_linear_trend`` uses Newey-West HAC errors.
- Many series are screened at once, so a single test's p-value overstates
  significance. Labels use Benjamini-Hochberg adjusted q-values.

Usage:
    from src.analysis.stats_utils import FDR_Q, bh_adjust, fit_linear_trend
    fit = fit_linear_trend(y, x)
    q = bh_adjust(p_values)
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.multitest import multipletests

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

# Benjamini-Hochberg false-discovery rate for screening labels.
FDR_Q = 0.10


def newey_west_maxlags(n: int) -> int:
    """
    Newey-West bandwidth for ``n`` monthly observations.

    ``max(1, floor(4 * (n / 100) ** (2 / 9)))`` gives one lag for short
    series and grows slowly with the sample size.

    Parameters
    ----------
    n : int
        Number of observations.

    Returns
    -------
    int
        Lag truncation parameter, at least 1.
    """
    return max(1, int(np.floor(4.0 * (max(int(n), 1) / 100.0) ** (2.0 / 9.0))))


def fit_linear_trend(y: np.ndarray, x: np.ndarray) -> dict:
    """
    Fit a linear trend with Newey-West (HAC) standard errors.

    The point estimate is plain OLS; the p-value and 95% CI come from the HAC
    covariance, so autocorrelated residuals do not shrink the standard error.

    Parameters
    ----------
    y : np.ndarray
        Response values.
    x : np.ndarray
        Time axis (month ordinals, not positions).

    Returns
    -------
    dict
        Keys: slope, intercept, r_squared, p_value, ci_lower, ci_upper, n.
        When the HAC covariance cannot be computed (degenerate design), the
        p-value and CI are NaN. ``bh_adjust`` leaves NaN untested, and the
        callers' ``q < FDR_Q`` comparison is False, so the label is "flat".
    """
    y_arr = np.asarray(y, dtype=float)
    x_arr = np.asarray(x, dtype=float)
    n = int(len(y_arr))
    if n == 0:
        return {
            "slope": float("nan"),
            "intercept": float("nan"),
            "r_squared": float("nan"),
            "p_value": float("nan"),
            "ci_lower": float("nan"),
            "ci_upper": float("nan"),
            "n": 0,
        }

    design = sm.add_constant(x_arr)
    ols = sm.OLS(y_arr, design).fit(disp=0)
    maxlags = newey_west_maxlags(n)
    try:
        hac = sm.OLS(y_arr, design).fit(
            cov_type="HAC", cov_kwds={"maxlags": maxlags}, disp=0
        )
        p_value = float(hac.pvalues[1])
        ci = hac.conf_int(alpha=0.05)
        ci_lower, ci_upper = float(ci[1][0]), float(ci[1][1])
    except Exception as exc:  # noqa: BLE001 — degenerate designs (all-zero y)
        logger.debug("HAC covariance failed (%d points): %s", n, exc)
        p_value = float("nan")
        ci_lower = ci_upper = float("nan")

    return {
        "slope": float(ols.params[1]),
        "intercept": float(ols.params[0]),
        "r_squared": float(ols.rsquared),
        "p_value": p_value,
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
        "n": n,
    }


def bh_adjust(p: pd.Series) -> pd.Series:
    """
    Benjamini-Hochberg adjusted p-values (q-values), NaN-safe.

    Only finite values are adjusted; NaN entries are left as NaN and the input
    index is preserved.

    Parameters
    ----------
    p : pd.Series
        Raw p-values.

    Returns
    -------
    pd.Series
        Adjusted p-values, same index and length as ``p``.
    """
    result = pd.Series(float("nan"), index=p.index, dtype=float)
    finite = p.notna() & np.isfinite(p.astype(float))
    if finite.any():
        values = p.loc[finite].astype(float).to_numpy()
        result.loc[finite] = multipletests(values, method="fdr_bh")[1]
    return result
