"""
Metrics for the savings-forecast bake-off.

Everything here works on the long backtest frame produced by
``src.analysis.forecast_backtest.rolling_origin_backtest`` (one row per
origin and horizon step), so the numbers stay comparable with production.

Definitions follow the experiment plan (``private/plans/savings-forecast-bakeoff.md``
§5) and are pre-registered; do not change them after the test phase starts.

Usage:
    from src.analysis.forecast_metrics import arm_summary, purged_band_scores, dm_test_hln
    summary = arm_summary(bt, series, horizon=12)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

BAND_ALPHA = 0.2
MIN_ORIGINS_FOR_BANDS = 10


def _as_float(values) -> np.ndarray:
    return np.asarray(values, dtype=float)


def seasonal_naive_scale(train: pd.Series, m: int = 12) -> float:
    """
    In-sample seasonal-naive MAE over a training window.

    ``mean(|y[t] - y[t-m]|)`` for every t with a full seasonal lag. This is the
    MASE denominator (the skill's rule). Returns NaN when the window is too
    short for even one seasonal difference.
    """
    values = _as_float(train.dropna())
    if len(values) <= m:
        return float("nan")
    diffs = np.abs(values[m:] - values[:-m])
    return float(np.mean(diffs)) if len(diffs) else float("nan")


def mase_scale_by_origin(series: pd.Series, origins, m: int = 12) -> dict:
    """
    Map each origin month (last training month) to its in-sample MASE scale.

    Parameters
    ----------
    series : pd.Series
        Full history with a month-start index.
    origins : iterable of pd.Timestamp
        Origin months, i.e. the last training month of each origin.
    m : int
        Seasonal period.

    Returns
    -------
    dict
        ``origin_month -> seasonal-naive MAE of that origin's train window``.
    """
    position = {ts: i for i, ts in enumerate(series.index)}
    scales = {}
    for origin in pd.to_datetime(list(origins)):
        idx = position.get(origin)
        if idx is None:
            continue
        scales[origin] = seasonal_naive_scale(series.iloc[: idx + 1], m=m)
    return scales


def _slice_horizon(bt: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Rows with ``h <= horizon``; empty frame stays empty."""
    if bt is None or bt.empty:
        return pd.DataFrame(columns=getattr(bt, "columns", None))
    return bt[bt["h"] <= horizon]


def per_origin_metrics(bt: pd.DataFrame, horizon: int = 12, mase_scales: dict | None = None) -> pd.DataFrame:
    """
    Collapse the long backtest frame to one row per origin.

    Columns: origin_month, n_steps, cum_abs, cum_bias, mae, rmse, wape,
    mae_h1, mae_h3, mae_h12, mase, all_finite.
    """
    frame = _slice_horizon(bt, horizon)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "origin_month", "n_steps", "cum_abs", "cum_bias", "mae", "rmse",
                "wape", "mae_h1", "mae_h3", "mae_h12", "mase", "all_finite",
            ]
        )

    rows = []
    for origin, group in frame.groupby("origin_month", sort=True):
        group = group.sort_values("h")
        actual = _as_float(group["actual"])
        point = _as_float(group["point"])
        finite = np.isfinite(actual) & np.isfinite(point)
        if not finite.all():
            # A partially failed origin would silently shrink the cumulative
            # sums, so the whole origin is excluded instead.
            continue
        err = actual - point
        abs_err = np.abs(err)
        cum_actual = float(np.sum(actual))
        cum_point = float(np.sum(point))
        h_index = group["h"].to_numpy()
        mae = float(np.nanmean(abs_err)) if len(abs_err) else float("nan")
        denom = float(np.nansum(np.abs(actual)))

        def _mae_at(step: int) -> float:
            mask = h_index == step
            return float(np.nanmean(abs_err[mask])) if mask.any() else float("nan")

        mase = float("nan")
        if mase_scales and origin in mase_scales:
            scale = mase_scales[origin]
            if np.isfinite(scale) and scale > 0:
                mase = mae / scale

        rows.append(
            {
                "origin_month": origin,
                "n_steps": int(len(group)),
                "cum_abs": abs(cum_actual - cum_point),
                "cum_bias": cum_actual - cum_point,
                "mae": mae,
                "rmse": float(np.sqrt(np.nanmean(err**2))) if len(err) else float("nan"),
                "wape": (float(np.nansum(abs_err)) / denom) if denom else float("nan"),
                "mae_h1": _mae_at(1),
                "mae_h3": _mae_at(3),
                "mae_h12": _mae_at(12),
                "mase": mase,
                "all_finite": bool(finite.all()),
            }
        )

    return pd.DataFrame(rows)


def purged_band_scores(
    bt: pd.DataFrame,
    horizon: int = 12,
    alpha: float = BAND_ALPHA,
    min_origins: int = MIN_ORIGINS_FOR_BANDS,
    origins: list | None = None,
) -> pd.DataFrame:
    """
    Empirical cumulative band from errors fully realised before each origin.

    For origin ``o`` the band is ``cum_point(o) + q`` where ``q`` are the
    ``alpha/2`` and ``1 - alpha/2`` quantiles of ``cum_actual - cum_point`` at
    h = ``horizon`` over origins ``o'`` with ``o' + horizon <= o`` (months).
    Origins with fewer than ``min_origins`` realised predecessors get no band.

    Returns one row per origin with the band, a coverage flag, the width and
    the two pinball losses. Empty when the frame is empty.
    """
    columns = [
        "origin_month", "lower", "upper", "cum_point", "cum_actual",
        "covered", "width", "pinball_lo", "pinball_hi", "n_prior",
    ]
    frame = _slice_horizon(bt, horizon)
    if frame.empty:
        return pd.DataFrame(columns=columns)

    at_h = frame[frame["h"] == horizon].dropna(subset=["cum_actual", "cum_point"]).copy()
    at_h["origin_month"] = pd.to_datetime(at_h["origin_month"])
    at_h = at_h.sort_values("origin_month")
    wanted = set(pd.to_datetime(list(origins))) if origins is not None else set(at_h["origin_month"])

    errors = at_h["cum_actual"] - at_h["cum_point"]
    dates = at_h["origin_month"].to_numpy()
    err_values = errors.to_numpy(dtype=float)

    rows = []
    for _, row in at_h.iterrows():
        origin = row["origin_month"]
        if origin not in wanted:
            continue
        limit = origin - pd.DateOffset(months=horizon)
        prior = (dates <= np.datetime64(limit)) & np.isfinite(err_values)
        if int(prior.sum()) < min_origins:
            continue
        lo, hi = np.quantile(err_values[prior], [alpha / 2, 1 - alpha / 2])
        lower = float(row["cum_point"] + lo)
        upper = float(row["cum_point"] + hi)
        actual = float(row["cum_actual"])
        point = float(row["cum_point"])
        rows.append(
            {
                "origin_month": origin,
                "lower": lower,
                "upper": upper,
                "cum_point": point,
                "cum_actual": actual,
                "covered": bool(lower <= actual <= upper),
                "width": upper - lower,
                "pinball_lo": _pinball(alpha / 2, actual, lower),
                "pinball_hi": _pinball(1 - alpha / 2, actual, upper),
                "n_prior": int(prior.sum()),
            }
        )

    return pd.DataFrame(rows, columns=columns)


def _pinball(tau: float, actual: float, quantile: float) -> float:
    """Pinball (quantile) loss for a single observation."""
    delta = actual - quantile
    return float(tau * delta) if delta >= 0 else float((tau - 1.0) * delta)


def dm_test_hln(
    loss1,
    loss2,
    h: int = 1,
    hac_lags: int | None = None,
) -> dict:
    """
    Diebold–Mariano test with the Harvey–Leybourne–Newbold correction.

    Parameters
    ----------
    loss1, loss2 : array-like
        Paired loss series (same order). The statistic is positive when
        ``loss1`` is larger, i.e. when the first model is worse.
    h : int
        Forecast horizon used for the HLN small-sample correction.
    hac_lags : int | None
        Bartlett-kernel HAC lag length. Defaults to ``h - 1``.

    Returns
    -------
    dict
        ``dm_stat``, ``p_value``, ``mean_diff``, ``n``. NaN p-value when the
        variance is zero and the losses are identical (returns 0 and 1.0 then).
    """
    a = _as_float(loss1)
    b = _as_float(loss2)
    mask = np.isfinite(a) & np.isfinite(b)
    a, b = a[mask], b[mask]
    n = len(a)
    if n < 2:
        return {"dm_stat": float("nan"), "p_value": float("nan"), "mean_diff": float("nan"), "n": n}

    d = a - b
    dbar = float(d.mean())
    lags = (h - 1) if hac_lags is None else hac_lags
    lags = int(min(max(lags, 0), n - 1))

    gamma0 = float(np.mean((d - dbar) ** 2))
    var_d = gamma0
    for k in range(1, lags + 1):
        weight = 1.0 - k / (lags + 1.0)
        gamma_k = float(np.mean((d[k:] - dbar) * (d[:-k] - dbar)))
        var_d += 2.0 * weight * gamma_k

    if var_d <= 0:
        if abs(dbar) < 1e-12:
            return {"dm_stat": 0.0, "p_value": 1.0, "mean_diff": dbar, "n": n}
        return {"dm_stat": float("inf"), "p_value": 0.0, "mean_diff": dbar, "n": n}

    stat = dbar / np.sqrt(var_d / n)
    correction = np.sqrt((n + 1 - 2 * h + h * (h - 1) / n) / n)
    stat *= correction
    p_value = float(2 * stats.t.sf(abs(stat), df=n - 1))
    return {"dm_stat": float(stat), "p_value": p_value, "mean_diff": dbar, "n": n}


def arm_summary(
    bt: pd.DataFrame,
    mase_scales: dict | None = None,
    horizon: int = 12,
    band_alpha: float = BAND_ALPHA,
    min_band_origins: int = MIN_ORIGINS_FOR_BANDS,
    origins: list | None = None,
) -> dict:
    """
    Aggregate one arm's backtest over a set of origins.

    Parameters
    ----------
    bt : pd.DataFrame
        Long backtest frame for the arm.
    mase_scales : dict | None
        Output of :func:`mase_scale_by_origin`.
    horizon : int
        Horizon cut-off for the accuracy metrics (12 for the primary run).
    band_alpha, min_band_origins
        Purged empirical band settings.
    origins : list | None
        Restrict to these origins (the common set used for cross-arm
        comparison). ``None`` uses every origin in the frame.

    Returns
    -------
    dict
        ``n_origins``, ``cum12_abs``, ``cum12_bias``, ``under_k``,
        ``mase_1_12``, ``mae_1_12``, ``relmae_wape_rmse`` helpers,
        per-horizon MAEs and the band scores.
    """
    frame = _slice_horizon(bt, horizon)
    if origins is not None:
        keep = set(pd.to_datetime(list(origins)))
        frame = frame[frame["origin_month"].isin(keep)]

    per_origin = per_origin_metrics(frame, horizon=horizon, mase_scales=mase_scales)
    empty = {
        "n_origins": 0,
        "cum12_abs": float("nan"),
        "cum12_bias": float("nan"),
        "under_k": 0,
        "mase_1_12": float("nan"),
        "mae_1_12": float("nan"),
        "wape_1_12": float("nan"),
        "rmse_1_12": float("nan"),
        "mae_h1": float("nan"),
        "mae_h3": float("nan"),
        "mae_h12": float("nan"),
        "band_coverage": float("nan"),
        "band_width": float("nan"),
        "band_pinball": float("nan"),
        "band_n": 0,
    }
    if per_origin.empty:
        return empty

    actual = _as_float(frame["actual"])
    point = _as_float(frame["point"])
    err = actual - point
    denom = float(np.nansum(np.abs(actual)))

    band = purged_band_scores(
        bt,
        horizon=horizon,
        alpha=band_alpha,
        min_origins=min_band_origins,
        origins=origins,
    )

    return {
        "n_origins": int(len(per_origin)),
        "cum12_abs": float(per_origin["cum_abs"].mean()),
        "cum12_bias": float(per_origin["cum_bias"].mean()),
        "under_k": int((per_origin["cum_bias"] > 0).sum()),
        "mase_1_12": float(per_origin["mase"].mean()),
        "mae_1_12": float(per_origin["mae"].mean()),
        "wape_1_12": (float(np.nansum(np.abs(err))) / denom) if denom else float("nan"),
        "rmse_1_12": float(np.sqrt(np.nanmean(err**2))),
        "mae_h1": float(per_origin["mae_h1"].mean()),
        "mae_h3": float(per_origin["mae_h3"].mean()),
        "mae_h12": float(per_origin["mae_h12"].mean()),
        "band_coverage": float(band["covered"].mean()) if not band.empty else float("nan"),
        "band_width": float(band["width"].mean()) if not band.empty else float("nan"),
        "band_pinball": (
            float(pd.concat([band["pinball_lo"], band["pinball_hi"]]).mean())
            if not band.empty
            else float("nan")
        ),
        "band_n": int(len(band)),
    }


def common_origins(backtests: dict, horizon: int = 12) -> list:
    """
    Origins where every arm produced a finite h=``horizon`` point forecast.

    Cross-arm comparisons use only these (plan §4.1). Returns an empty list
    when any arm is missing or empty.
    """
    if not backtests:
        return []
    sets = []
    for bt in backtests.values():
        if bt is None or bt.empty:
            return []
        at_h = bt[bt["h"] == horizon]
        valid = at_h[np.isfinite(_as_float(at_h["point"]))]["origin_month"]
        sets.append(set(pd.to_datetime(valid)))
    return sorted(set.intersection(*sets))
