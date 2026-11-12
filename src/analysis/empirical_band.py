"""
Shared empirical band construction for category and savings forecasts.

The band is a *scaled empirical* interval: errors from past, fully realised
origins are stored divided by a scale that was known at that origin, then
applied at a new origin with the same scale definition. Backtest and
production call these functions, so a shipped band is the same object the
backtest scored.

Scale (per origin ``o``, for series ``s``):

    s_o = mean |y| over the 12 months ending at ``o``, floored at 1

Every scale here is computed from data on or before the origin. Sizing a band
with the outcome it has to cover is hindsight and is not permitted: an earlier
version of this code did exactly that, and the coverage it reported could not
be reproduced by any deployable band.

Two variants are supported, selected by ``scale_mode``:

``origin12``
    ``s_o`` only (candidate K0 / V1).
``max_history``
    ``max(s_o, mean |y| over all history up to o)`` (candidate K1). Quiet
    series get a non-zero scale, so their band has width instead of collapsing
    to a point.

Usage:
    from src.analysis.empirical_band import (
        origin_scale, build_error_pool, band_from_pool,
    )
"""

from __future__ import annotations

import numpy as np
import pandas as pd

WINDOW_MONTHS = 12
MIN_SCALE = 1.0
MIN_PRIOR_ERRORS = 8
QUANTILES = (0.1, 0.9)

SCALE_MODES = ("origin12", "max_history", "origin12_std")


def origin_scale(
    history: np.ndarray | pd.Series,
    mode: str = "origin12",
    window: int = WINDOW_MONTHS,
    group: np.ndarray | pd.Series | None = None,
) -> float:
    """
    Scale known at the forecast origin.

    Parameters
    ----------
    history : array-like
        The series up to and including the origin month. Must not include any
        month after the origin.
    mode : str
        - ``origin12``: mean |y| over the trailing window (default);
        - ``max_history``: the larger of that and mean |y| over all history,
          so a series that went quiet keeps a usable scale;
        - ``origin12_std``: standard deviation of y over the trailing window.
    window : int
        Months in the trailing window (12).
    group : array-like | None
        Optional grouping label per observation, used by the ``grouped``
        candidates to place the series in a size bucket. Ignored here.
    """
    if mode not in SCALE_MODES:
        raise ValueError(f"unknown scale mode: {mode}")
    values = np.asarray(history, dtype=float)
    if values.size == 0:
        return MIN_SCALE
    tail = values[-int(window) :]
    if mode == "origin12_std":
        return float(max(float(np.std(tail, ddof=0)), MIN_SCALE))
    scale = float(np.mean(np.abs(tail)))
    if mode == "max_history":
        scale = max(scale, float(np.mean(np.abs(values))))
    return float(max(scale, MIN_SCALE))


def error_rows(
    series_key: str,
    y: pd.Series,
    origin: pd.Timestamp,
    point: np.ndarray,
    horizon: int,
    scale_mode: str = "origin12",
) -> list[dict] | None:
    """
    Scaled error rows for one (series, origin), or None if not realised.

    ``scale`` is computed from history up to ``origin`` only; the outcomes are
    used solely to measure the error, never to size it.
    """
    history = y[y.index <= origin].to_numpy(dtype=float)
    if history.size == 0:
        return None
    scale = origin_scale(history, mode=scale_mode)
    future = pd.date_range(
        start=pd.Timestamp(origin) + pd.DateOffset(months=1), periods=int(horizon), freq="MS"
    )
    actual = y.reindex(future).to_numpy(dtype=float)
    if len(actual) < int(horizon) or not np.isfinite(actual).all():
        return None
    rows = []
    cumulative = 0.0
    for step in range(int(horizon)):
        error = float(actual[step] - point[step])
        cumulative += error
        rows.append(
            {
                "series_key": series_key,
                "origin_month": pd.Timestamp(origin),
                "h": step + 1,
                "err": error,
                "cum_err": cumulative,
                "scale": scale,
            }
        )
    return rows


def build_error_pool(
    rows: list[dict],
    labels: dict[str, str] | None = None,
    label_column: str = "label",
) -> pd.DataFrame:
    """Assemble error rows into the pool frame used by ``band_from_pool``."""
    frame = pd.DataFrame(
        rows, columns=["series_key", "origin_month", "h", "err", "cum_err", "scale"]
    )
    if labels is not None:
        frame[label_column] = frame["series_key"].map(labels)
    return frame


def _pool_slice(
    pool: pd.DataFrame,
    key: str,
    label: str | None,
    label_column: str,
    horizon: int,
    origin: pd.Timestamp,
    include_peers: bool,
) -> pd.DataFrame:
    """
    Pool rows usable at ``origin`` for one series at step ``horizon``.

    A stored origin qualifies only when the outcome it contributes finished on
    or before the scoring origin (``o' + horizon <= o``, plan rule). The test
    is per step: an origin one month back is usable for h = 1 even when the
    full-horizon window is still open. Filtering on a fixed one month would
    let partially-future errors in; filtering on the whole horizon would throw
    away usable history.

    A supplied ``label`` with a missing ``label_column`` is an error, not a
    silent fall-through to an unpooled-by-label set: that would quietly mix
    every class into one band.
    """
    realised = pool[
        pool["origin_month"] + pd.DateOffset(months=int(horizon)) <= pd.Timestamp(origin)
    ]
    if realised.empty:
        return realised
    own = realised[realised["series_key"] == key]
    if not include_peers:
        return own[own["h"] == horizon]
    if label is not None and label_column not in realised.columns:
        raise ValueError(
            f"pool has no {label_column!r} column but a label was supplied; "
            "refusing to pool across labels"
        )
    if label is None:
        peers = realised[realised["series_key"] != key]
    else:
        peers = realised[
            (realised[label_column] == label) & (realised["series_key"] != key)
        ]
    return pd.concat([own[own["h"] == horizon], peers[peers["h"] == horizon]], ignore_index=True)


def size_tercile_cutpoints(
    pool: pd.DataFrame, origin: pd.Timestamp, horizon: int | None = None
) -> tuple[float, float] | None:
    """
    Tercile cut points of the scale, computed from the pool at ``origin``.

    Only origins fully realised before ``origin`` contribute, so the cut points
    are as of the forecast origin and never use the window being forecast.
    ``horizon`` drops origins whose window has not finished; when omitted every
    origin with a stored row before ``origin`` is used, which is only safe for
    a pool that was already filtered to realised origins.
    """
    if horizon is None:
        realised = pool[pool["origin_month"] < pd.Timestamp(origin)]
    else:
        realised = pool[
            pool["origin_month"] + pd.DateOffset(months=int(horizon)) <= pd.Timestamp(origin)
        ]
    if realised.empty:
        return None
    scales = (
        realised.groupby(["series_key", "origin_month"], sort=False)["scale"]
        .first()
        .to_numpy(dtype=float)
    )
    if scales.size < 3:
        return None
    low, high = np.quantile(scales, [1 / 3, 2 / 3])
    return float(low), float(high)


def size_bucket(scale: float, cutpoints: tuple[float, float] | None) -> str:
    """Bucket label for a scale given the origin's tercile cut points."""
    if cutpoints is None:
        return "all"
    low, high = cutpoints
    if scale <= low:
        return "small"
    if scale <= high:
        return "mid"
    return "large"


def pool_with_size_bucket(
    pool: pd.DataFrame,
    origin: pd.Timestamp,
    label_column: str = "label",
    horizon: int | None = None,
) -> pd.DataFrame:
    """
    Pool copy with a ``size_bucket`` column for the K2 candidate.

    The bucket is assigned per stored origin using that origin's *own* cut
    points, which keeps the pool self-consistent: every stored error is
    labelled with the bucket its scale fell into when the forecast was made.
    Pass ``horizon`` so the cut points only use origins fully realised by each
    stored origin; otherwise they use every earlier stored origin.
    """
    if pool is None or pool.empty:
        return pool
    out = pool.copy()
    out["size_bucket"] = "all"
    for stored_origin, group in out.groupby("origin_month", sort=False):
        cutpoints = size_tercile_cutpoints(out, stored_origin, horizon=horizon)
        scales = group.groupby("series_key", sort=False)["scale"].first()
        mapping = {key: size_bucket(value, cutpoints) for key, value in scales.items()}
        out.loc[group.index, "size_bucket"] = group["series_key"].map(mapping)
    out["size_bucket"] = out["size_bucket"].fillna("all")
    return out


def band_from_pool(
    point: np.ndarray,
    scale: float,
    pool: pd.DataFrame,
    key: str,
    origin: pd.Timestamp,
    horizon: int,
    label: str | None = None,
    label_column: str = "label",
    include_peers: bool = True,
    min_prior: int = MIN_PRIOR_ERRORS,
    quantiles: tuple[float, float] = QUANTILES,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Monthly and cumulative band offsets from the pooled scaled errors.

    Returns signed offsets ``(m_lo, m_hi, c_lo, c_hi)`` aligned to ``point``;
    the caller adds them to the point and clips at 0. Offsets are NaN where the
    pool has fewer than ``min_prior`` observations.
    """
    periods = len(point)
    m_lo = np.full(periods, np.nan)
    m_hi = np.full(periods, np.nan)
    c_lo = np.full(periods, np.nan)
    c_hi = np.full(periods, np.nan)
    if pool is None or pool.empty:
        return m_lo, m_hi, c_lo, c_hi

    for step in range(periods):
        realised = _pool_slice(
            pool,
            key=key,
            label=label,
            label_column=label_column,
            horizon=step + 1,
            origin=origin,
            include_peers=include_peers,
        )
        if len(realised) < min_prior:
            continue
        scaled = (realised["err"] / realised["scale"]).replace(
            [np.inf, -np.inf], np.nan
        ).dropna()
        cum_scaled = (realised["cum_err"] / realised["scale"]).replace(
            [np.inf, -np.inf], np.nan
        ).dropna()
        if len(scaled) < min_prior or len(cum_scaled) < min_prior:
            continue
        lo, hi = np.quantile(scaled.to_numpy(dtype=float), list(quantiles))
        cum_lo, cum_hi = np.quantile(cum_scaled.to_numpy(dtype=float), list(quantiles))
        m_lo[step] = float(lo) * scale
        m_hi[step] = float(hi) * scale
        c_lo[step] = float(cum_lo) * scale
        c_hi[step] = float(cum_hi) * scale
    return m_lo, m_hi, c_lo, c_hi


def apply_offsets(
    point: np.ndarray,
    m_lo: np.ndarray,
    m_hi: np.ndarray,
    c_lo: np.ndarray,
    c_hi: np.ndarray,
    snap: float = 0.01,
) -> pd.DataFrame:
    """
    Turn point + offsets into clipped, snapped band values.

    Values below ``snap`` in absolute value become exactly 0, so the tiny
    residual series (cumulative points around 1e-89) do not carry sub-cent
    noise. Everything is floored at 0.
    """
    point = np.clip(np.asarray(point, dtype=float), 0.0, None)
    cumulative = np.cumsum(point)

    def _clean(values: np.ndarray, anchor: np.ndarray) -> np.ndarray:
        out = np.clip(anchor + values, 0.0, None)
        out = np.where(np.abs(out) < snap, 0.0, out)
        return out

    lower = _clean(m_lo, point)
    upper = _clean(m_hi, point)
    cum_lower = _clean(c_lo, cumulative)
    cum_upper = _clean(c_hi, cumulative)
    return pd.DataFrame(
        {
            "point_estimate": np.where(np.abs(point) < snap, 0.0, point),
            "ci80_lower": lower,
            "ci80_upper": upper,
            "cum_point": np.where(np.abs(cumulative) < snap, 0.0, cumulative),
            "cum_ci80_lower": cum_lower,
            "cum_ci80_upper": cum_upper,
        }
    )
