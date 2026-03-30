"""
Expense trend analysis for monthly category spend.

Fits OLS linear trends, detects seasonality via CV, and flags consecutive
month-over-month rises per category.

Usage:
    from src.analysis.expense_trends import compute_expense_trends
    df_trends = compute_expense_trends(monthly_aggregates_df)
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


# ── Low-level statistical primitives ─────────────────────────────────────────


def fit_category_trend(monthly_spend: pd.Series) -> dict:
    """
    Fit OLS linear trend to monthly category spend.

    Parameters
    ----------
    monthly_spend : pd.Series
        Monthly spend values ordered chronologically (index ignored).

    Returns
    -------
    dict with keys: slope, intercept, r_squared, p_value, n_months,
                    trend_class, ci_slope_lower, ci_slope_upper.
        trend_class is one of: "rising", "falling", "flat",
        "insufficient_data".
    """
    n = len(monthly_spend)
    if n < 6:
        return {"trend_class": "insufficient_data", "n_months": n}

    X = sm.add_constant(np.arange(n))
    model = sm.OLS(monthly_spend.values, X).fit()

    slope = model.params[1]
    p_value = model.pvalues[1]
    ci = model.conf_int(alpha=0.05)

    if p_value < 0.05:
        trend_class = "rising" if slope > 0 else "falling"
    else:
        trend_class = "flat"

    return {
        "slope": slope,
        "intercept": model.params[0],
        "r_squared": model.rsquared,
        "p_value": p_value,
        "n_months": n,
        "trend_class": trend_class,
        "ci_slope_lower": ci[1][0],
        "ci_slope_upper": ci[1][1],
    }


def check_seasonality(
    monthly_spend: pd.DataFrame,
    date_col: str,
    value_col: str,
) -> dict:
    """
    Flag categories where spend varies dramatically by month-of-year.

    Metric: CV = σ / μ across month-of-year group means.
    Threshold: CV > 0.4 → "highly seasonal".

    Parameters
    ----------
    monthly_spend : pd.DataFrame
        Must contain date_col (parseable as datetime) and value_col.
    date_col : str
    value_col : str

    Returns
    -------
    dict with keys: cv, is_seasonal, month_means.
    """
    df = monthly_spend.copy()
    df["_month_of_year"] = pd.to_datetime(df[date_col]).dt.month
    group_means = df.groupby("_month_of_year")[value_col].mean()
    mean_val = group_means.mean()
    cv = group_means.std() / mean_val if mean_val != 0 else float("inf")
    return {
        "cv": cv,
        "is_seasonal": bool(cv > 0.4),
        "month_means": group_means.to_dict(),
    }


def detect_consecutive_rises(
    monthly_series: pd.Series,
    threshold: float = 0.20,
    consecutive: int = 2,
) -> list[tuple[int, int]]:
    """
    Find periods where MoM % increase exceeds threshold for N consecutive months.

    Parameters
    ----------
    monthly_series : pd.Series
        Monthly spend values in chronological order.
    threshold : float
        MoM fractional increase threshold (default 0.20 = 20%).
    consecutive : int
        Minimum run length to flag (default 2).

    Returns
    -------
    List of (start_idx, end_idx) tuples (0-based, inclusive).
    """
    pct_change = monthly_series.pct_change()
    rising = (pct_change > threshold).astype(int)

    runs: list[tuple[int, int]] = []
    run_start = None
    run_length = 0

    for i, val in enumerate(rising):
        if val == 1:
            if run_start is None:
                run_start = i
            run_length += 1
        else:
            if run_length >= consecutive:
                runs.append((run_start, i - 1))
            run_start = None
            run_length = 0

    if run_length >= consecutive:
        runs.append((run_start, len(rising) - 1))

    return runs


# ── Top-level pipeline function ───────────────────────────────────────────────


def compute_expense_trends(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Run trend analysis on monthly category aggregates.

    For each category:
    - Fit OLS trend (slope, R², p-value, trend_class)
    - Detect seasonality (CV, is_seasonal)
    - Detect consecutive MoM rises (has_consecutive_rise, n_rise_runs)
    - Compute share of total absolute spend (share_pct)

    Parameters
    ----------
    monthly_df : pd.DataFrame
        Output of build_monthly_aggregates(). Must have columns:
        year_month, category, total_amount, month_ordinal.

    Returns
    -------
    pd.DataFrame
        One row per (year_month, category) with original columns plus:
        trend_slope, trend_intercept, trend_r2, trend_pvalue, trend_n_months,
        trend_class, trend_ci_lower, trend_ci_upper,
        seasonality_cv, is_seasonal,
        has_consecutive_rise, n_rise_runs, share_pct.

    Notes
    -----
    Assumptions:
    1. Spend is approximately linear over time (no structural breaks)
    2. Residuals are approximately normally distributed
    3. No extreme outliers dominating the fit
    4. Monthly observations are approximately independent
    """
    df = monthly_df.copy().sort_values(["category", "year_month"]).reset_index(drop=True)

    total_abs_spend = df["total_amount"].abs().sum()

    categories = df["category"].unique()
    trend_records: list[dict] = []

    for cat in categories:
        cat_df = df[df["category"] == cat].sort_values("month_ordinal")
        series = cat_df["total_amount"].reset_index(drop=True)

        # OLS trend
        trend = fit_category_trend(series)

        # Seasonality (needs year_month as date proxy)
        seasonality = check_seasonality(
            cat_df[["year_month", "total_amount"]],
            date_col="year_month",
            value_col="total_amount",
        )

        # Consecutive MoM rises
        rise_runs = detect_consecutive_rises(series)

        # Share of total absolute spend
        cat_abs = cat_df["total_amount"].abs().sum()
        share_pct = (cat_abs / total_abs_spend * 100) if total_abs_spend > 0 else 0.0

        for _, row in cat_df.iterrows():
            trend_records.append({
                **row.to_dict(),
                "trend_slope": trend.get("slope", float("nan")),
                "trend_intercept": trend.get("intercept", float("nan")),
                "trend_r2": trend.get("r_squared", float("nan")),
                "trend_pvalue": trend.get("p_value", float("nan")),
                "trend_n_months": trend.get("n_months", len(series)),
                "trend_class": trend.get("trend_class", "insufficient_data"),
                "trend_ci_lower": trend.get("ci_slope_lower", float("nan")),
                "trend_ci_upper": trend.get("ci_slope_upper", float("nan")),
                "seasonality_cv": seasonality["cv"],
                "is_seasonal": seasonality["is_seasonal"],
                "has_consecutive_rise": len(rise_runs) > 0,
                "n_rise_runs": len(rise_runs),
                "share_pct": share_pct,
            })

    result = pd.DataFrame(trend_records)

    n_rising = (result.drop_duplicates("category")["trend_class"] == "rising").sum()
    n_seasonal = result.drop_duplicates("category")["is_seasonal"].sum()
    logger.info(
        f"Expense trends: {len(categories)} categories | "
        f"{n_rising} rising | {n_seasonal} seasonal"
    )

    return result
