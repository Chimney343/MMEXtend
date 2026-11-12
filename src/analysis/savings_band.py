"""
Production savings bands: the V2 scaled empirical band.

Chosen by the band bake-off on dev origins (``configs/empirical_band_experiment.yaml``).
Errors from the series' own past origins are stored divided by the scale known
at that origin, then applied at the forecast origin with the same scale. The
savings series is a single series, so there is no pooling.

The shipped band, ``empirical_bands`` in ``forecast_backtest.py``, sizes errors
in currency with no scale. It is kept because the cashflow backtest report and
dashboard caption read its columns; this module supersedes it for
``cashflow_forecast``.

Usage:
    from src.analysis.savings_band import savings_bands
    forecast_df = savings_bands(backtest, forecast_df, history)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.analysis.empirical_band import (
    MIN_PRIOR_ERRORS,
    apply_offsets,
    band_from_pool,
    origin_scale,
)

SNAP = 0.01

# V2: the 12-month standard deviation at the origin. Chosen on dev by the band
# bake-off: the mean-scaled V1 came out over-wide, while V2 sat inside the
# pre-registered coverage window with a much tighter band.
SCALE_MODE = "origin12_std"


def build_pool(backtest: pd.DataFrame, history: pd.Series, horizon: int = 24) -> pd.DataFrame:
    """
    Scaled error rows from the backtest, scaled by history-known values.

    ``backtest`` supplies the errors; ``history`` supplies the scale, which is
    read only up to each origin.
    """
    if backtest is None or backtest.empty:
        return pd.DataFrame(columns=["series_key", "origin_month", "h", "err", "cum_err", "scale"])
    rows = []
    for origin, group in backtest.groupby("origin_month", sort=True):
        origin = pd.Timestamp(origin)
        history_to_origin = history[history.index <= origin].to_numpy(dtype=float)
        if history_to_origin.size == 0:
            continue
        scale = origin_scale(history_to_origin, mode=SCALE_MODE)
        group = group.sort_values("h")
        actual = group["actual"].to_numpy(dtype=float)
        point = group["point"].to_numpy(dtype=float)
        cumulative = 0.0
        for step in range(min(int(horizon), len(actual))):
            error = float(actual[step] - point[step])
            cumulative += error
            rows.append(
                {
                    "series_key": "savings",
                    "origin_month": origin,
                    "h": step + 1,
                    "err": error,
                    "cum_err": cumulative,
                    "scale": scale,
                }
            )
    return pd.DataFrame(rows)


def savings_bands(
    backtest: pd.DataFrame,
    forecast_df: pd.DataFrame,
    history: pd.Series,
    min_prior: int = MIN_PRIOR_ERRORS,
) -> pd.DataFrame:
    """
    Replace the forecast intervals with the V2 scaled empirical band.

    Returns a copy with ``ci80_*``, ``cum_point``, ``cum_ci80_*`` set and
    ``ci95_*`` null, ``interval_method`` naming the band, and the backtest
    count columns the dashboard caption reads.
    """
    result = forecast_df.copy().reset_index(drop=True)
    if result.empty:
        return result
    point = pd.to_numeric(result["point_estimate"], errors="coerce").to_numpy(dtype=float)
    origin = pd.Timestamp(result["month"].iloc[0]) - pd.DateOffset(months=1)
    horizon = len(result)

    if history is None or len(history) == 0:
        raise RuntimeError("savings V1 band needs history up to the origin")
    if not isinstance(history.index, pd.DatetimeIndex):
        raise RuntimeError("savings V1 band needs a monthly DatetimeIndex history")
    history_to_origin = history[history.index <= origin].to_numpy(dtype=float)
    if history_to_origin.size == 0:
        raise RuntimeError("savings V1 band needs history up to the origin")
    scale = origin_scale(history_to_origin, mode=SCALE_MODE)

    pool = build_pool(backtest, history, horizon)
    # ``band_from_pool`` applies the per-step rule (o' + h <= origin) itself, so
    # nothing whose outcome is still in the future can contribute.
    m_lo, m_hi, c_lo, c_hi = band_from_pool(
        point,
        scale,
        pool,
        key="savings",
        origin=origin,
        horizon=horizon,
        label=None,
        include_peers=False,
        min_prior=min_prior,
    )
    banded = apply_offsets(point, m_lo, m_hi, c_lo, c_hi, snap=SNAP)

    result["point_estimate"] = banded["point_estimate"].to_numpy()
    result["ci80_lower"] = banded["ci80_lower"].to_numpy()
    result["ci80_upper"] = banded["ci80_upper"].to_numpy()
    result["ci95_lower"] = np.nan
    result["ci95_upper"] = np.nan
    result["cum_point"] = banded["cum_point"].to_numpy()
    result["cum_ci80_lower"] = banded["cum_ci80_lower"].to_numpy()
    result["cum_ci80_upper"] = banded["cum_ci80_upper"].to_numpy()
    result["cum_ci95_lower"] = np.nan
    result["cum_ci95_upper"] = np.nan

    # Bias caption: which way the past origins erred. The pool is already
    # restricted to origins whose horizon finished before the forecast origin.
    realised = pool
    low_k = []
    n_k = []
    for step in range(1, horizon + 1):
        at_h = realised[realised["h"] == step]
        if at_h.empty:
            low_k.append(pd.NA)
            n_k.append(pd.NA)
            continue
        low_k.append(int((at_h["cum_err"] > 0).sum()))
        n_k.append(int(len(at_h)))
    result["backtest_cum_low_k"] = pd.array(low_k, dtype="Int64")
    result["backtest_cum_n"] = pd.array(n_k, dtype="Int64")
    result["interval_method"] = f"empirical_scaled_n{int(realised['origin_month'].nunique())}"
    return result
