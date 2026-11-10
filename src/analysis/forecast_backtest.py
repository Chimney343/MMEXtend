"""
Rolling-origin backtest and empirical forecast bands.

A forecast interval means what it says only if it has been checked against
outcomes. This module replays the forecaster over historical cut-offs, collects
the errors, and turns them into intervals:

- ``rolling_origin_backtest`` — refit the forecaster at several origins and
  record the realised errors per horizon step.
- ``summarise_backtest`` — MAE, 12-month cumulative error, model-band coverage,
  and a held-out check of the empirical cumulative band.
- ``empirical_bands`` — replace the model's own intervals with bands estimated
  from the backtest errors (``NaN`` 95% bands: too few origins).

The holdout check splits origins: the first two-thirds estimate the cumulative
quantiles, the last third measures coverage. Nothing is tuned to that figure.

Usage:
    from src.analysis.forecast_backtest import (
        rolling_origin_backtest, naive_mean_forecaster, summarise_backtest, empirical_bands,
    )
    bt = rolling_origin_backtest(series, select_and_run_forecast)
    empirical_bands(bt, forecast_df)
"""

import numpy as np
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

MIN_ORIGINS_FOR_BANDS = 10


def naive_mean_forecaster(train: pd.Series, periods: int) -> pd.DataFrame:
    """
    Forecast every future month as the trailing 12-month mean.

    Same output shape as ``select_and_run_forecast``. No intervals are
    produced (all NaN).

    Parameters
    ----------
    train : pd.Series
        History with a month-start ``DatetimeIndex``.
    periods : int
        Months ahead.

    Returns
    -------
    pd.DataFrame
        Columns: month, point_estimate, ci80_lower, ci80_upper,
        ci95_lower, ci95_upper, method.
    """
    tail = train.iloc[-12:] if len(train) >= 12 else train
    point = float(tail.mean())
    future_dates = pd.date_range(
        start=train.index[-1] + pd.DateOffset(months=1),
        periods=periods,
        freq="MS",
    )
    return pd.DataFrame(
        {
            "month": future_dates,
            "point_estimate": [point] * periods,
            "ci80_lower": [float("nan")] * periods,
            "ci80_upper": [float("nan")] * periods,
            "ci95_lower": [float("nan")] * periods,
            "ci95_upper": [float("nan")] * periods,
            "method": ["naive_mean"] * periods,
        }
    )


def rolling_origin_backtest(
    series: pd.Series,
    forecaster,
    initial: int = 36,
    horizon: int = 24,
    step: int = 3,
) -> pd.DataFrame:
    """
    Refit ``forecaster`` at successive origins and record the realised errors.

    Parameters
    ----------
    series : pd.Series
        Complete monthly history with a month-start ``DatetimeIndex``.
    forecaster : callable
        ``forecaster(train: pd.Series, periods: int) -> DataFrame`` using the
        standard forecast shape (month, point_estimate, ci80_lower/upper).
    initial : int
        Minimum training length before the first origin.
    horizon : int
        Months forecast at each origin.
    step : int
        Months between origins.

    Returns
    -------
    pd.DataFrame
        Columns: origin_month, h, month, actual, point, cum_actual, cum_point,
        ci80_lower, ci80_upper. Empty when the series is too short.
    """
    columns = [
        "origin_month",
        "h",
        "month",
        "actual",
        "point",
        "cum_actual",
        "cum_point",
        "ci80_lower",
        "ci80_upper",
    ]
    n = len(series)
    if n < initial + horizon:
        logger.info(
            "Backtest skipped: %d months < initial(%d) + horizon(%d)", n, initial, horizon
        )
        return pd.DataFrame(columns=columns)

    records: list[dict] = []
    for origin in range(initial, n - horizon + 1, step):
        train = series.iloc[:origin]
        actual = series.iloc[origin : origin + horizon]
        forecast = forecaster(train, horizon)
        if forecast is None:
            continue
        point = pd.to_numeric(forecast["point_estimate"], errors="coerce").to_numpy()[:horizon]
        lower = (
            pd.to_numeric(forecast["ci80_lower"], errors="coerce").to_numpy()[:horizon]
            if "ci80_lower" in forecast.columns
            else np.full(horizon, np.nan)
        )
        upper = (
            pd.to_numeric(forecast["ci80_upper"], errors="coerce").to_numpy()[:horizon]
            if "ci80_upper" in forecast.columns
            else np.full(horizon, np.nan)
        )
        actual_values = actual.to_numpy(dtype=float)
        cum_actual = np.cumsum(actual_values)
        cum_point = np.cumsum(point)
        for h in range(min(len(actual_values), len(point))):
            records.append(
                {
                    "origin_month": train.index[-1],
                    "h": h + 1,
                    "month": actual.index[h],
                    "actual": float(actual_values[h]),
                    "point": float(point[h]),
                    "cum_actual": float(cum_actual[h]),
                    "cum_point": float(cum_point[h]),
                    "ci80_lower": float(lower[h]),
                    "ci80_upper": float(upper[h]),
                }
            )

    return pd.DataFrame(records, columns=columns)


def _empirical_quantiles(errors: pd.Series, probs=(0.1, 0.9)) -> tuple[float, float]:
    """Quantiles of a finite error sample; NaN when the sample is empty."""
    values = errors.dropna()
    if values.empty:
        return float("nan"), float("nan")
    low, high = np.quantile(values.to_numpy(), list(probs))
    return float(low), float(high)


def summarise_backtest(bt: pd.DataFrame) -> dict:
    """
    Summarise a backtest frame.

    Returns
    -------
    dict
        n_origins, mae_month, abs_cum_err_12, model_ci80_coverage_month,
        holdout_cum80_k, holdout_cum80_m, holdout_cum80_rate.

        The holdout rate estimates the cumulative 80% band on the first
        two-thirds of origins and counts hits on the last third. It is a
        report, not a tuning target.
    """
    empty = {
        "n_origins": 0,
        "mae_month": float("nan"),
        "abs_cum_err_12": float("nan"),
        "model_ci80_coverage_month": float("nan"),
        "holdout_cum80_k": 0,
        "holdout_cum80_m": 0,
        "holdout_cum80_rate": float("nan"),
    }
    if bt is None or bt.empty:
        return empty

    errors = (bt["actual"] - bt["point"]).abs()
    origins = sorted(bt["origin_month"].unique())
    n_origins = len(origins)

    cum12 = bt[bt["h"] == 12]
    abs_cum_err_12 = (
        float((cum12["cum_actual"] - cum12["cum_point"]).abs().mean())
        if not cum12.empty
        else float("nan")
    )

    model_band = bt["ci80_lower"].notna() & bt["ci80_upper"].notna()
    if model_band.any():
        inside = (bt.loc[model_band, "actual"] >= bt.loc[model_band, "ci80_lower"]) & (
            bt.loc[model_band, "actual"] <= bt.loc[model_band, "ci80_upper"]
        )
        model_cov = float(inside.mean())
    else:
        model_cov = float("nan")

    # Holdout: estimate the cumulative 80% band on the first two-thirds of
    # origins, count hits on the last third.
    split = int(np.floor(n_origins * 2 / 3))
    train_origins = origins[:split]
    holdout_origins = origins[split:]
    hits = 0
    for origin in holdout_origins:
        rows = bt[bt["origin_month"] == origin]
        if rows.empty:
            continue
        cum_actual = rows["cum_actual"]
        cum_point = rows["cum_point"]
        if cum_actual.empty:
            continue
        cum_actual_value = float(cum_actual.iloc[-1])
        cum_point_value = float(cum_point.iloc[-1])
        h = int(rows["h"].iloc[-1])
        train_rows = bt[(bt["origin_month"].isin(train_origins)) & (bt["h"] == h)]
        low, high = _empirical_quantiles(train_rows["cum_actual"] - train_rows["cum_point"])
        if np.isnan(low) or np.isnan(high):
            continue
        if cum_point_value + low <= cum_actual_value <= cum_point_value + high:
            hits += 1
    m = len(holdout_origins)

    return {
        "n_origins": n_origins,
        "mae_month": float(errors.mean()),
        "abs_cum_err_12": abs_cum_err_12,
        "model_ci80_coverage_month": model_cov,
        "holdout_cum80_k": hits,
        "holdout_cum80_m": m,
        "holdout_cum80_rate": (hits / m) if m else float("nan"),
    }


def empirical_bands(bt: pd.DataFrame, forecast_df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace model intervals with bands estimated from backtest errors.

    Monthly: ``point_estimate + quantile(actual - point, [0.1, 0.9])`` per
    horizon step. Cumulative: ``cum_point + quantile(cum_actual - cum_point,
    [0.1, 0.9])`` per horizon step. The 95% bands are ``NaN``: too few origins
    to estimate the 2.5/97.5 percentiles.

    Parameters
    ----------
    bt : pd.DataFrame
        Output of ``rolling_origin_backtest``.
    forecast_df : pd.DataFrame
        The forecast to band; its row order is horizon steps 1..N.

    Returns
    -------
    pd.DataFrame
        A copy of ``forecast_df`` with ci80_*, cum_point, cum_ci80_* set and
        ``interval_method = f"empirical_backtest_n{n_origins}"``. Two count
        columns show which way the backtests erred: ``backtest_cum_low_k`` of
        ``backtest_cum_n`` origins had a cumulative forecast to that step
        below the actual total. A lopsided count (17 of 17) means the point
        forecast is biased, and ``cum_point`` can then lie outside its own
        band.
    """
    result = forecast_df.copy().reset_index(drop=True)
    n_origins = int(bt["origin_month"].nunique()) if bt is not None and not bt.empty else 0

    point = pd.to_numeric(result["point_estimate"], errors="coerce").to_numpy(dtype=float)
    cum_point = np.cumsum(point)
    result["cum_point"] = cum_point

    model_lower = pd.Series(np.nan, index=result.index, dtype=float)
    model_upper = pd.Series(np.nan, index=result.index, dtype=float)
    cum_lower = pd.Series(np.nan, index=result.index, dtype=float)
    cum_upper = pd.Series(np.nan, index=result.index, dtype=float)
    cum_low_k = pd.Series(pd.NA, index=result.index, dtype="Int64")
    cum_n = pd.Series(pd.NA, index=result.index, dtype="Int64")

    for h in range(1, len(result) + 1):
        rows = bt[bt["h"] == h] if bt is not None and not bt.empty else bt
        if rows is None or rows.empty:
            continue
        low, high = _empirical_quantiles(rows["actual"] - rows["point"])
        model_lower.iloc[h - 1] = point[h - 1] + low
        model_upper.iloc[h - 1] = point[h - 1] + high
        low_cum, high_cum = _empirical_quantiles(rows["cum_actual"] - rows["cum_point"])
        cum_lower.iloc[h - 1] = cum_point[h - 1] + low_cum
        cum_upper.iloc[h - 1] = cum_point[h - 1] + high_cum
        cum_errors = (rows["cum_actual"] - rows["cum_point"]).dropna()
        if not cum_errors.empty:
            cum_low_k.iloc[h - 1] = int((cum_errors > 0).sum())
            cum_n.iloc[h - 1] = int(len(cum_errors))

    result["ci80_lower"] = model_lower.to_numpy()
    result["ci80_upper"] = model_upper.to_numpy()
    result["ci95_lower"] = np.nan
    result["ci95_upper"] = np.nan
    result["cum_ci80_lower"] = cum_lower.to_numpy()
    result["cum_ci80_upper"] = cum_upper.to_numpy()
    result["cum_ci95_lower"] = np.nan
    result["cum_ci95_upper"] = np.nan
    result["backtest_cum_low_k"] = cum_low_k
    result["backtest_cum_n"] = cum_n
    result["interval_method"] = f"empirical_backtest_n{n_origins}"
    return result
