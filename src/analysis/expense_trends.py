"""
Expense trend analysis for monthly category spend.

Fits HAC (Newey-West) linear trends on a zero-filled continuous month grid
and tests seasonality with a Kruskal-Wallis test on trend residuals by
month-of-year. Screening labels are Benjamini-Hochberg adjusted.

Usage:
    from src.analysis.expense_trends import compute_expense_trends
    df_trends = compute_expense_trends(monthly_aggregates_df)
"""

import numpy as np
import pandas as pd

from src.analysis.stats_utils import FDR_Q, bh_adjust, fit_linear_trend
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


# ── Low-level statistical primitives ─────────────────────────────────────────


def fit_category_trend(monthly_spend: pd.Series, x: np.ndarray | None = None) -> dict:
    """
    Fit a linear trend to monthly category spend.

    Thin wrapper over ``stats_utils.fit_linear_trend``: OLS point fit with
    Newey-West (HAC) standard errors. The returned ``trend_class`` is a
    single-series, *unadjusted* label (HAC p < 0.05);
    ``compute_expense_trends`` overwrites it with the BH label.

    Parameters
    ----------
    monthly_spend : pd.Series
        Monthly spend values ordered chronologically (index ignored).
    x : np.ndarray | None
        Time axis, e.g. ``month_ordinal``. Defaults to positions
        ``0..n-1``. The slope and p-value do not depend on the origin, but
        the intercept does: pass ``month_ordinal`` when the result is drawn
        as ``intercept + slope * month_ordinal``.

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

    x_axis = np.arange(n, dtype=float) if x is None else np.asarray(x, dtype=float)
    fit = fit_linear_trend(monthly_spend.values, x=x_axis)
    slope = fit["slope"]
    p_value = fit["p_value"]

    if pd.notna(p_value) and p_value < 0.05:
        trend_class = "rising" if slope > 0 else "falling"
    else:
        trend_class = "flat"

    return {
        "slope": slope,
        "intercept": fit["intercept"],
        "r_squared": fit["r_squared"],
        "p_value": p_value,
        "n_months": n,
        "trend_class": trend_class,
        "ci_slope_lower": fit["ci_lower"],
        "ci_slope_upper": fit["ci_upper"],
    }


def check_seasonality(
    monthly_spend: pd.DataFrame,
    date_col: str,
    value_col: str,
) -> dict:
    """
    Describe month-of-year variation in spend.

    Metric: CV = σ / μ across month-of-year group means. This is descriptive
    only; the calibrated ``is_seasonal`` label comes from
    ``month_of_year_kruskal`` on detrended residuals.

    Parameters
    ----------
    monthly_spend : pd.DataFrame
        Must contain date_col (parseable as datetime) and value_col.
    date_col : str
    value_col : str

    Returns
    -------
    dict with keys: cv, month_means.
    """
    df = monthly_spend.copy()
    df["_month_of_year"] = pd.to_datetime(df[date_col]).dt.month
    group_means = df.groupby("_month_of_year")[value_col].mean()
    mean_val = group_means.mean()
    cv = group_means.std() / abs(mean_val) if mean_val != 0 else float("inf")
    return {
        "cv": cv,
        "month_means": group_means.to_dict(),
    }


def month_of_year_kruskal(residuals: pd.Series) -> float:
    """
    Kruskal-Wallis p-value for month-of-year differences in residuals.

    Not named ``test_*`` on purpose: pytest would collect it when tests import
    it.

    Parameters
    ----------
    residuals : pd.Series
        Trend residuals with a month-start ``DatetimeIndex``.

    Returns
    -------
    float
        Raw p-value, NaN when fewer than two month groups have data.
    """
    from scipy import stats as sp_stats

    if not isinstance(residuals.index, pd.DatetimeIndex):
        residuals = residuals.copy()
        residuals.index = pd.to_datetime(residuals.index)
    groups = [group.to_numpy(dtype=float) for _, group in residuals.groupby(residuals.index.month)]
    groups = [group for group in groups if len(group) > 0]
    if len(groups) < 2:
        return float("nan")
    try:
        _, p_value = sp_stats.kruskal(*groups)
    except ValueError:
        return float("nan")
    return float(p_value)


# ── Top-level pipeline function ───────────────────────────────────────────────


def _expense_spend_frame(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce monthly aggregates to positive expense spend.

    New-format aggregates carry a ``flow_type`` column, so income rows are
    dropped explicitly. Legacy aggregates (signed net amounts, no ``flow_type``)
    are supported by keeping the negative rows and taking their magnitude.
    """
    df = monthly_df.copy()

    if "flow_type" in df.columns:
        df = df[df["flow_type"].astype(str).str.lower() == "expense"]
    else:
        df = df[pd.to_numeric(df["total_amount"], errors="coerce") < 0]

    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").abs()
    df = df[df["total_amount"].notna()]
    return df.sort_values(["category", "year_month"]).reset_index(drop=True)


def _assert_linear_ordinals(ordinal_map: pd.Series) -> None:
    """Raise when consecutive months do not differ by exactly one ordinal."""
    months = sorted(ordinal_map.index)
    periods = pd.PeriodIndex(months, freq="M")
    ordinals = ordinal_map.reindex(months).to_numpy(dtype=float)
    diffs = np.diff(ordinals)
    expected = np.array([periods[i + 1].ordinal - periods[i].ordinal for i in range(len(periods) - 1)])
    if len(diffs) and not np.array_equal(diffs, expected):
        raise ValueError(
            "month_ordinal is not linear in year_month; cannot extrapolate a zero-filled grid"
        )


def _zero_filled_grid(cat_df: pd.DataFrame, last_month: str, ordinal_map: pd.Series) -> pd.DataFrame:
    """Reindex one category to a continuous month grid, zero-filling gaps."""
    months = (
        pd.period_range(cat_df["year_month"].min(), last_month, freq="M")
        .strftime("%Y-%m")
        .tolist()
    )
    base_ordinal = float(ordinal_map.loc[months[0]])
    filled = cat_df.set_index("year_month").reindex(months)
    filled["total_amount"] = pd.to_numeric(filled["total_amount"], errors="coerce").fillna(0.0)
    filled["category"] = cat_df["category"].iloc[0]
    filled["flow_type"] = "expense"
    filled["month_ordinal"] = base_ordinal + np.arange(len(months), dtype=float)
    filled["mom_delta"] = filled["total_amount"].diff()
    filled["is_partial_month"] = False
    return filled.reset_index().rename(columns={"index": "year_month"})


def compute_expense_trends(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Run trend analysis on monthly category expense aggregates.

    Only expense flow is analysed; income is excluded. Spend is treated as a
    positive magnitude, so a rising expense series always yields a ``rising``
    trend class regardless of the signed convention used upstream.

    Each category is fitted on a continuous month grid that starts at the
    category's first complete month and ends at the last complete month in the
    input; months without spend are zero-filled. The x axis is the existing
    ``month_ordinal`` (one month = +1), so the dashboard trend line
    ``intercept + slope * month_ordinal`` matches the fit. Errors are
    Newey-West HAC, and the ``rising``/``falling``/``flat`` label uses
    Benjamini-Hochberg q-values across categories (``FDR_Q``). The month
    containing the run date (``is_partial_month``) is excluded before fitting.

    Parameters
    ----------
    monthly_df : pd.DataFrame
        Output of build_monthly_aggregates(). Must have columns:
        year_month, category, total_amount, month_ordinal, and optionally
        flow_type / is_partial_month. Legacy frames without flow_type are
        interpreted using the signed convention (negative = expense).

    Returns
    -------
    pd.DataFrame
        One row per (year_month, expense category) on the zero-filled grid,
        with original columns plus:
        trend_slope, trend_intercept, trend_r2, trend_pvalue,
        trend_pvalue_adj, trend_n_months, trend_class, trend_ci_lower,
        trend_ci_upper, seasonality_cv, seasonality_pvalue,
        seasonality_pvalue_adj, is_seasonal, share_pct.

    ``is_seasonal`` is a Kruskal-Wallis test on the Phase 2 OLS residuals by
    month-of-year, for grids of at least 24 months, Benjamini-Hochberg
    adjusted across categories. ``seasonality_cv`` is descriptive only.

    Notes
    -----
    Assumptions:
    1. Spend is approximately linear over time (no structural breaks)
    2. Residuals are approximately normally distributed
    3. No extreme outliers dominating the fit
    4. Residual autocorrelation is handled by the HAC covariance, not assumed away
    """
    df = _expense_spend_frame(monthly_df)
    if "is_partial_month" in df.columns:
        df = df[~df["is_partial_month"].fillna(False).astype(bool)].reset_index(drop=True)
    if df.empty:
        logger.info("Expense trends: 0 expense categories")
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
            "trend_pvalue_adj",
        ):
            df[col] = pd.Series(dtype="bool" if col == "is_seasonal" else "float64")
        return df

    total_spend = df["total_amount"].sum()
    last_month = df["year_month"].max()
    ordinal_map = df.groupby("year_month")["month_ordinal"].min()
    _assert_linear_ordinals(ordinal_map)

    categories = df["category"].unique()
    category_frames: list[pd.DataFrame] = []

    for cat in categories:
        cat_df = df[df["category"] == cat].sort_values("month_ordinal")
        grid = _zero_filled_grid(cat_df, last_month, ordinal_map)
        series = grid["total_amount"]

        trend = fit_category_trend(series, x=grid["month_ordinal"].to_numpy())
        seasonality = check_seasonality(
            grid[["year_month", "total_amount"]],
            date_col="year_month",
            value_col="total_amount",
        )
        if len(grid) >= 24:
            residual_index = pd.to_datetime(grid["year_month"] + "-01")
            residuals = pd.Series(
                series.to_numpy()
                - (trend.get("intercept", np.nan) + trend.get("slope", np.nan) * grid["month_ordinal"].to_numpy()),
                index=residual_index,
            )
            seasonality_pvalue = month_of_year_kruskal(residuals)
        else:
            seasonality_pvalue = float("nan")

        cat_spend = cat_df["total_amount"].sum()
        share_pct = (cat_spend / total_spend * 100) if total_spend > 0 else 0.0

        grid["trend_slope"] = trend.get("slope", float("nan"))
        grid["trend_intercept"] = trend.get("intercept", float("nan"))
        grid["trend_r2"] = trend.get("r_squared", float("nan"))
        grid["trend_pvalue"] = trend.get("p_value", float("nan"))
        grid["trend_n_months"] = trend.get("n_months", len(series))
        grid["trend_class"] = trend.get("trend_class", "insufficient_data")
        grid["trend_ci_lower"] = trend.get("ci_slope_lower", float("nan"))
        grid["trend_ci_upper"] = trend.get("ci_slope_upper", float("nan"))
        grid["seasonality_cv"] = seasonality["cv"]
        grid["seasonality_pvalue"] = seasonality_pvalue
        grid["share_pct"] = share_pct
        category_frames.append(grid)

    result = pd.concat(category_frames, ignore_index=True)

    # Two BH adjustments across categories, each broadcast back to grid rows.
    first_rows = result.drop_duplicates("category").set_index("category")
    adjusted = bh_adjust(first_rows["trend_pvalue"])
    result["trend_pvalue_adj"] = result["category"].map(adjusted)
    labels = np.where(
        result["trend_pvalue_adj"] < FDR_Q,
        np.where(result["trend_slope"] > 0, "rising", "falling"),
        "flat",
    )
    too_short = result["trend_n_months"] < 6
    result["trend_class"] = np.where(too_short, "insufficient_data", labels)

    seasonality_adj = bh_adjust(first_rows["seasonality_pvalue"])
    result["seasonality_pvalue_adj"] = result["category"].map(seasonality_adj)
    result["is_seasonal"] = result["seasonality_pvalue_adj"] < FDR_Q

    n_tested = int(adjusted.notna().sum())
    n_sig = int((adjusted < FDR_Q).sum())
    n_seasonal = int((seasonality_adj < FDR_Q).sum())
    logger.info(
        f"Expense trends: {len(categories)} categories | "
        f"{n_tested} tested, {n_sig} significant at FDR {FDR_Q:.0%} | "
        f"{n_seasonal} seasonal"
    )

    return result
