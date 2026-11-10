"""
Per-category / per-subcategory trend and forecast analysis using Prophet.

Produces three long-format DataFrames consumed by the dashboard:
  - category_trends_summary  (one row per series, numeric trend breakdown)
  - category_trends_series   (monthly amount + in-sample Prophet trend line)
  - category_forecasts       (36-month point estimate + CI per series)

Flow types: ``expense`` == type "withdrawal", ``income`` == type "deposit".
Transfers are excluded from both. Amounts are taken as ``abs(amount_pln)``
(falling back to ``abs(amount)``) so expenses are shown as positive spend.

Prophet is an optional dependency and is imported lazily. Fits are wrapped in
try/except so an unavailable or failing Prophet degrades to
``insufficient_data`` (trend) / omitted rows (forecast) instead of crashing.

Usage:
    from src.analysis.category_prophet import (
        build_category_monthly_series,
        compute_category_trends,
        compute_category_forecasts,
        compute_trend_metrics,
        forecast_series,
    )
"""

import numpy as np
import pandas as pd

from src.analysis.cashflow_forecast import build_prophet_model, forecast_prophet
from src.analysis.feature_engineering import actual_flows, partial_month_flags
from src.analysis.stats_utils import FDR_Q, bh_adjust, fit_linear_trend
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

MIN_MONTHS_TREND = 6
MIN_MONTHS_FORECAST = 12
FORECAST_PERIODS = 36

_SUMMARY_COLUMNS = [
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
]

_SERIES_COLUMNS = [
    "flow_type",
    "level",
    "category",
    "subcategory",
    "month",
    "y",
    "trend",
]

_FORECAST_COLUMNS = [
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
]

_INSUFFICIENT_METRICS = {
    "trend_start": float("nan"),
    "trend_end": float("nan"),
    "trend_change_pln": float("nan"),
    "avg_monthly_change_pln": float("nan"),
    "growth_pct": float("nan"),
    "direction": "insufficient_data",
    "trend_pvalue": float("nan"),
}


def _direction_from_slope(slope: float, p_value: float, trend_change_pln: float) -> tuple[str, bool]:
    """
    Label direction from a significant OLS slope.

    ``mixed`` is used when the fitted slope is significant but the Prophet
    trend change has the opposite sign.
    """
    if p_value is None or not np.isfinite(p_value) or p_value >= FDR_Q:
        return "flat", False
    label = "rising" if slope > 0 else "falling"
    if pd.notna(trend_change_pln) and np.isfinite(trend_change_pln):
        if np.sign(trend_change_pln) != 0 and np.sign(trend_change_pln) != np.sign(slope):
            return "mixed", True
    return label, True


def build_category_monthly_series(
    df_featured: pd.DataFrame,
    as_of=None,
) -> pd.DataFrame:
    """
    Build the long monthly series frame from featured transactions.

    Emits both ``level == "category"`` (subcategory=None) and
    ``level == "subcategory"`` (one row per non-null subcategory) rows, for
    both flow types. Transfers and future-dated rows are excluded through
    ``actual_flows()``. The month containing ``as_of`` is dropped: it is
    still accruing, so fitting or averaging it would use a partial month.

    Parameters
    ----------
    df_featured : pd.DataFrame
        Output of ``engineer_features()``. Must have ``type``, ``category``,
        ``year_month`` and an amount column (``amount_pln`` or ``amount``).
    as_of : date | pd.Timestamp | None
        Run date identifying the partial month. ``None`` means today.

    Returns
    -------
    pd.DataFrame
        Columns: flow_type, level, category, subcategory, month, y.
    """
    df = actual_flows(df_featured)
    if not df.empty and "year_month" in df.columns:
        df = df[~partial_month_flags(df["year_month"], as_of)].copy()

    amount_col = "amount_pln" if "amount_pln" in df.columns else "amount"

    columns = ["flow_type", "level", "category", "subcategory", "month", "y"]
    frames: list[pd.DataFrame] = []

    tx_type = df["type"].astype(str).str.lower()
    for flow_type, type_values in (
        ("expense", ("withdrawal", "expense")),
        ("income", ("deposit", "income")),
    ):
        sub = df[tx_type.isin(type_values)].copy()
        if sub.empty:
            continue
        sub["_amount"] = sub[amount_col].abs()
        sub["_month"] = pd.to_datetime(sub["year_month"] + "-01")

        cat = (
            sub.groupby(["_month", "category"], as_index=False)["_amount"]
            .sum()
            .rename(columns={"_amount": "y", "_month": "month"})
        )
        cat["flow_type"] = flow_type
        cat["level"] = "category"
        cat["subcategory"] = None
        frames.append(cat[columns])

        if "subcategory" in sub.columns:
            sub_nonnull = sub[sub["subcategory"].notna()]
            if not sub_nonnull.empty:
                subcat = (
                    sub_nonnull.groupby(["_month", "category", "subcategory"], as_index=False)[
                        "_amount"
                    ]
                    .sum()
                    .rename(columns={"_amount": "y", "_month": "month"})
                )
                subcat["flow_type"] = flow_type
                subcat["level"] = "subcategory"
                frames.append(subcat[columns])

    if not frames:
        return pd.DataFrame(columns=columns)

    result = pd.concat(frames, ignore_index=True)
    result["month"] = pd.to_datetime(result["month"])
    return (
        result.sort_values(["flow_type", "level", "category", "month"], kind="mergesort")
        .reset_index(drop=True)
    )


def _iter_series(monthly: pd.DataFrame):
    """Yield (flow_type, level, category, subcategory, y) per series."""
    for key, grp in monthly.groupby(
        ["flow_type", "level", "category", "subcategory"], dropna=False, sort=False
    ):
        flow_type, level, category, subcategory = key
        grp = grp.sort_values("month")
        y = pd.Series(grp["y"].values, index=pd.to_datetime(grp["month"]))
        yield flow_type, level, category, subcategory, y


def _continuous_month_series(y: pd.Series) -> pd.Series:
    """Reindex to a continuous month grid, zero-filling inactive months."""
    if len(y) == 0:
        return y
    full_index = pd.date_range(start=y.index.min(), end=y.index.max(), freq="MS")
    return y.reindex(full_index, fill_value=0.0)


def _fit_prophet(series: pd.Series, yearly_seasonality: bool = False) -> tuple:
    """
    Fit a tuned Prophet model to a monthly series.

    Shares the parameter set of ``forecast_prophet`` via ``build_prophet_model``.

    Returns
    -------
    tuple
        (model, in_sample_forecast) where ``in_sample_forecast`` is the result
        of ``model.predict`` on the historical dates (contains the ``trend``
        component).
    """
    hist = pd.DataFrame({"ds": pd.to_datetime(series.index), "y": series.values.astype(float)})
    model = build_prophet_model(yearly_seasonality=yearly_seasonality)
    model.fit(hist)
    forecast = model.predict(hist[["ds"]])
    return model, forecast


def _trend_metrics_from_forecast(
    series: pd.Series,
    trend_values: np.ndarray | None,
    trend_pvalue: float = float("nan"),
) -> dict:
    """
    Compute the numeric trend breakdown from an in-sample Prophet trend.

    ``trend_values`` may be ``None`` when the Prophet fit failed; the Prophet
    metrics are then NaN and only ``trend_pvalue`` carries information.
    """
    n = len(series)
    mean_monthly = float(series.mean()) if n else float("nan")
    if trend_values is None or len(trend_values) == 0:
        return {
            "trend_start": float("nan"),
            "trend_end": float("nan"),
            "trend_change_pln": float("nan"),
            "avg_monthly_change_pln": float("nan"),
            "growth_pct": float("nan"),
            "trend_pvalue": trend_pvalue,
        }
    trend_start = float(trend_values[0])
    trend_end = float(trend_values[-1])
    change = trend_end - trend_start
    avg_monthly_change = change / (n - 1) if n > 1 else 0.0
    growth_pct = (change / mean_monthly * 100.0) if mean_monthly else float("nan")
    return {
        "trend_start": trend_start,
        "trend_end": trend_end,
        "trend_change_pln": change,
        "avg_monthly_change_pln": avg_monthly_change,
        "growth_pct": growth_pct,
        "trend_pvalue": trend_pvalue,
    }


def compute_trend_metrics(series: pd.Series, min_months: int = MIN_MONTHS_TREND) -> dict:
    """
    Compute the numeric trend breakdown for a single monthly series.

    ``direction`` is unadjusted on purpose: a single series has no family to
    adjust over, so the label is OLS slope sign at HAC p < FDR_Q (0.10).
    ``compute_category_trends`` applies the BH adjustment across the family.

    Returns
    -------
    dict
        Keys: n_months, trend_start, trend_end, trend_change_pln,
        avg_monthly_change_pln, growth_pct, trend_pvalue, direction. Trend
        metrics are NaN and direction is ``insufficient_data`` when the series
        is too short.
    """
    n = len(series)
    insufficient = {"n_months": n, **_INSUFFICIENT_METRICS}
    if n < min_months:
        return insufficient

    fit = fit_linear_trend(series.to_numpy(dtype=float), np.arange(n, dtype=float))
    trend_change_pln = float("nan")
    trend_values: np.ndarray | None = None
    try:
        _, forecast = _fit_prophet(series)
        trend_values = forecast["trend"].values
    except Exception as exc:  # noqa: BLE001 — degrade gracefully
        logger.warning("Prophet trend fit failed (%d months): %s", n, exc)

    metrics = _trend_metrics_from_forecast(series, trend_values, trend_pvalue=fit["p_value"])
    trend_change_pln = metrics["trend_change_pln"]
    direction, _ = _direction_from_slope(fit["slope"], fit["p_value"], trend_change_pln)
    return {"n_months": n, **metrics, "direction": direction}


def forecast_series(
    series: pd.Series,
    periods: int = FORECAST_PERIODS,
    min_months: int = MIN_MONTHS_FORECAST,
    yearly_seasonality: bool = False,
) -> pd.DataFrame | None:
    """
    Forecast a monthly series ``periods`` months ahead using Prophet.

    Returns ``None`` when the series has fewer than ``min_months`` months or the
    fit fails. Output shape matches ``forecast_prophet`` (month, point_estimate,
    ci80_*, ci95_*, method).
    """
    n = len(series)
    if n < min_months:
        return None

    hist = pd.DataFrame({"ds": pd.to_datetime(series.index), "y": series.values.astype(float)})
    try:
        return forecast_prophet(hist, periods=periods, yearly_seasonality=yearly_seasonality)
    except Exception as exc:  # noqa: BLE001 — degrade gracefully
        logger.warning("Prophet forecast failed (%d months): %s", n, exc)
        return None


def compute_category_trends(
    df_featured: pd.DataFrame,
    as_of=None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute per-category / per-subcategory trend summary and series frames.

    Parameters
    ----------
    df_featured : pd.DataFrame
        Output of ``engineer_features()``.
    as_of : date | pd.Timestamp | None
        Run date; the month containing it is excluded from the series.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (summary_df, series_df) matching the documented columns for
        ``category_trends_summary`` and ``category_trends_series``.
    """
    monthly = build_category_monthly_series(df_featured, as_of=as_of)

    summary_records: list[dict] = []
    series_records: list[dict] = []

    for flow_type, level, category, subcategory, y in _iter_series(monthly):
        y = _continuous_month_series(y)
        base = {
            "flow_type": flow_type,
            "level": level,
            "category": category,
            "subcategory": subcategory,
            "n_months": len(y),
            "first_month": y.index.min(),
            "last_month": y.index.max(),
            "mean_monthly": float(y.mean()),
        }

        trend_values: np.ndarray | None = None
        if len(y) < MIN_MONTHS_TREND:
            metrics = dict(_INSUFFICIENT_METRICS)
        else:
            # Direction comes from the HAC OLS slope, not the Prophet trend, so
            # it is defined even when the Prophet fit fails.
            fit = fit_linear_trend(y.to_numpy(dtype=float), np.arange(len(y), dtype=float))
            try:
                _, forecast = _fit_prophet(y)
                trend_values = forecast["trend"].values
                logger.info(
                    "Category trends: fitted %s/%s/%s (%d months)",
                    level,
                    category,
                    subcategory or "(all)",
                    len(y),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Category trends: Prophet fit failed for %s/%s/%s: %s",
                    level,
                    category,
                    subcategory or "(all)",
                    exc,
                )
            metrics = _trend_metrics_from_forecast(
                y, trend_values, trend_pvalue=fit["p_value"]
            )
            metrics["_slope"] = fit["slope"]

        summary_records.append({**base, **metrics})

        for i, (month_ts, yval) in enumerate(zip(y.index, y.values)):
            series_records.append(
                {
                    "flow_type": flow_type,
                    "level": level,
                    "category": category,
                    "subcategory": subcategory,
                    "month": month_ts,
                    "y": float(yval),
                    "trend": float(trend_values[i]) if trend_values is not None else float("nan"),
                }
            )

    summary_df = pd.DataFrame(summary_records, columns=_SUMMARY_COLUMNS + ["_slope"])
    # BH within each (flow_type, level) family; a series is significant only
    # against the other series tested at the same granularity.
    summary_df["direction_pvalue_adj"] = np.nan
    for _family, idx in summary_df.groupby(["flow_type", "level"], dropna=False).groups.items():
        adjusted = bh_adjust(summary_df.loc[idx, "trend_pvalue"])
        summary_df.loc[idx, "direction_pvalue_adj"] = adjusted

    directions: list[str] = []
    for _, row in summary_df.iterrows():
        if row["direction"] == "insufficient_data":
            directions.append("insufficient_data")
            continue
        slope = row.get("_slope", float("nan"))
        label, _ = _direction_from_slope(
            float(slope) if pd.notna(slope) else float("nan"),
            row["direction_pvalue_adj"],
            row["trend_change_pln"],
        )
        directions.append(label)
    summary_df["direction"] = directions
    summary_df = summary_df[_SUMMARY_COLUMNS]

    series_df = pd.DataFrame(series_records, columns=_SERIES_COLUMNS)
    logger.info(
        "Category trends: %d summary rows | %d series rows", len(summary_df), len(series_df)
    )
    return summary_df, series_df


def compute_category_forecasts(
    df_featured: pd.DataFrame,
    as_of=None,
) -> pd.DataFrame:
    """
    Precompute 36-month Prophet forecasts per category / subcategory × flow type.

    Series with fewer than ``MIN_MONTHS_FORECAST`` months emit no rows.

    Parameters
    ----------
    df_featured : pd.DataFrame
        Output of ``engineer_features()``.
    as_of : date | pd.Timestamp | None
        Run date; the month containing it is excluded from the series.

    Returns
    -------
    pd.DataFrame
        Columns matching ``category_forecasts``, including ``months_ahead``
        (1..36) so the dashboard can slice 1/2/3-year views from one fit.
    """
    monthly = build_category_monthly_series(df_featured, as_of=as_of)

    records: list[dict] = []
    for flow_type, level, category, subcategory, y in _iter_series(monthly):
        y = _continuous_month_series(y)
        fc = forecast_series(y, periods=FORECAST_PERIODS, min_months=MIN_MONTHS_FORECAST)
        if fc is None:
            continue

        logger.info(
            "Category forecasts: fitted %s/%s/%s (%d months)",
            level,
            category,
            subcategory or "(all)",
            len(y),
        )

        for i, row in fc.iterrows():
            records.append(
                {
                    "flow_type": flow_type,
                    "level": level,
                    "category": category,
                    "subcategory": subcategory,
                    "months_ahead": int(i) + 1,
                    "month": row["month"],
                    "point_estimate": row["point_estimate"],
                    "ci80_lower": row["ci80_lower"],
                    "ci80_upper": row["ci80_upper"],
                    "ci95_lower": row["ci95_lower"],
                    "ci95_upper": row["ci95_upper"],
                    "method": row["method"],
                }
            )

    forecast_df = pd.DataFrame(records, columns=_FORECAST_COLUMNS)
    logger.info("Category forecasts: %d forecast rows", len(forecast_df))
    return forecast_df
