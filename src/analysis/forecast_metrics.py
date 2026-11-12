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


# ── Category panel metrics (category forecast bake-off) ──────────────────────


def pair_cumulative(bt: pd.DataFrame, horizon: int = 12) -> pd.DataFrame:
    """
    One row per (series_key, origin) with the cumulative error at ``horizon``.

    Columns: ``series_key``, ``origin_month``, ``cum_actual``, ``cum_point``,
    ``cumerr`` (= actual - point), ``scale`` (12 x mean |y| over the actuals
    of the window, floored at 1 to keep ratios finite).
    """
    if bt is None or bt.empty:
        return pd.DataFrame(
            columns=["series_key", "origin_month", "cum_actual", "cum_point", "cumerr", "scale"]
        )
    frame = bt[bt["h"] <= int(horizon)].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=["series_key", "origin_month", "cum_actual", "cum_point", "cumerr", "scale"]
        )
    rows = []
    for (key, origin), group in frame.groupby(["series_key", "origin_month"], sort=True):
        group = group.sort_values("h")
        actual = _as_float(group["actual"])
        point = _as_float(group["point"])
        if not (np.isfinite(actual).all() and np.isfinite(point).all()):
            continue
        if group["h"].max() < int(horizon):
            continue
        rows.append(
            {
                "series_key": key,
                "origin_month": origin,
                "cum_actual": float(actual.sum()),
                "cum_point": float(point.sum()),
                "cumerr": float(actual.sum() - point.sum()),
                "scale": float(max(12.0 * np.mean(np.abs(actual)), 1.0)),
            }
        )
    return pd.DataFrame(rows)


def avg_rel_cum(pairs: pd.DataFrame, base: pd.DataFrame) -> dict:
    """
    Geometric mean of per-series cumulative-error ratios against ``base``.

    Each series counts once. A series enters only when its B1 cumulative error
    is non-zero over the origins in question (plan §5). Returns ``avg_rel``,
    ``median_rel``, ``win_share`` (r < 1), and the counts used.
    """
    if pairs.empty or base.empty:
        return {"avg_rel": float("nan"), "median_rel": float("nan"), "win_share": float("nan"), "n": 0}
    arm = pairs.groupby("series_key")["cumerr"].apply(lambda s: float(np.abs(s).sum()))
    ref = base.groupby("series_key")["cumerr"].apply(lambda s: float(np.abs(s).sum()))
    joined = pd.DataFrame({"arm": arm, "base": ref}).dropna()
    joined = joined[joined["base"] > 0]
    if joined.empty:
        return {"avg_rel": float("nan"), "median_rel": float("nan"), "win_share": float("nan"), "n": 0}
    ratio = (joined["arm"] / joined["base"]).replace([np.inf, -np.inf], np.nan).dropna()
    ratio = ratio[ratio > 0]
    if ratio.empty:
        return {"avg_rel": float("nan"), "median_rel": float("nan"), "win_share": float("nan"), "n": 0}
    return {
        "avg_rel": float(np.exp(np.mean(np.log(ratio.to_numpy())))),
        "median_rel": float(np.median(ratio.to_numpy())),
        "win_share": float((ratio < 1.0).mean()),
        "n": int(len(ratio)),
    }


def money_weighted_ratio(pairs: pd.DataFrame, base: pd.DataFrame, money_keys: set[str]) -> float:
    """Sum |cumerr| over the money-weighted series, divided by the base's."""
    arm = pairs[pairs["series_key"].isin(money_keys)]
    ref = base[base["series_key"].isin(money_keys)]
    denom = float(np.abs(ref["cumerr"]).sum())
    if denom <= 0:
        return float("nan")
    return float(np.abs(arm["cumerr"]).sum() / denom)


def phantom_spend(pairs: pd.DataFrame) -> float:
    """
    Forecast 12-month total on pairs whose actual total is 0, over all actual.

    ``sum(cum_point for cum_actual == 0) / sum(cum_actual)``. Zero actual
    totals are the "stays quiet" case; a non-zero forecast there is phantom
    spend. Returns NaN when the actual total is 0.
    """
    if pairs.empty:
        return float("nan")
    actual_total = float(pairs["cum_actual"].sum())
    if actual_total <= 0:
        return float("nan")
    quiet = pairs[pairs["cum_actual"] == 0.0]
    return float(quiet["cum_point"].sum() / actual_total)


def pooled_bias(pairs: pd.DataFrame) -> tuple[float, int, int]:
    """
    Signed cumulative error over all actuals, plus under-forecast k / n.

    Positive bias means the forecast was too low. Returns
    ``(bias_ratio, under_k, n_pairs)``.
    """
    if pairs.empty:
        return float("nan"), 0, 0
    total = float(pairs["cum_actual"].sum())
    bias = float(pairs["cumerr"].sum() / total) if total > 0 else float("nan")
    return bias, int((pairs["cumerr"] > 0).sum()), int(len(pairs))


def scaled_pooled_band(
    arm_pairs: pd.DataFrame,
    actual_frame: pd.DataFrame,
    history_errors: pd.DataFrame,
    classes: dict[str, str],
    quantiles: tuple[float, float] = (0.1, 0.9),
    min_prior: int = 8,
) -> pd.DataFrame:
    """
    Pooled scaled empirical 80% band from earlier fully realised errors.

    For each test pair (s, o) the scaled errors ``(actual - point) / scale``
    are pooled over origins ``o'`` with ``o' + h <= o`` (the same series) and
    over series in the same SBC class, then the per-h quantiles become
    ``point + q * scale`` and the cumulative band is built from the cumulative
    sums. Horizons without ``min_prior`` prior errors get no band.

    Parameters
    ----------
    arm_pairs : pd.DataFrame
        ``pair_cumulative`` output for the arm being banded.
    actual_frame : pd.DataFrame
        Long backtest rows for the arm, with ``h`` and monthly actuals.
    history_errors : pd.DataFrame
        Long rows of ``series_key, origin_month, h, err, scale`` used as the
        error pool (the arm's own realised errors). Only rows with
        ``o' + h <= o`` are used.
    classes : dict
        ``series_key -> SBC class``.
    """
    columns = [
        "series_key", "origin_month", "h", "lower", "upper", "cum_lower", "cum_upper",
        "covered", "cum_covered", "width", "cum_width", "n_prior",
    ]
    if arm_pairs.empty or history_errors.empty:
        return pd.DataFrame(columns=columns)

    pool = history_errors.copy()
    pool["origin_month"] = pd.to_datetime(pool["origin_month"])
    pool["sbc"] = pool["series_key"].map(classes)
    if "cum_err" not in pool.columns:
        pool = pool.sort_values(["series_key", "origin_month", "h"])
        pool["cum_err"] = pool.groupby(["series_key", "origin_month"])["err"].cumsum()
    rows = []
    for _, pair in arm_pairs.iterrows():
        key = pair["series_key"]
        origin = pd.Timestamp(pair["origin_month"])
        group = actual_frame[
            (actual_frame["series_key"] == key) & (actual_frame["origin_month"] == origin)
        ].sort_values("h")
        if group.empty:
            continue
        label = classes.get(key)
        same_class = pool[(pool["sbc"] == label) & (pool["series_key"] != key)] if label else pool
        own = pool[pool["series_key"] == key]
        points = _as_float(group["point"])
        actual = _as_float(group["actual"])
        scale = float(pair["scale"])
        cum_point = np.cumsum(points)
        cum_actual = np.cumsum(actual)

        for index, h in enumerate(group["h"].to_numpy()):
            horizon = int(h)
            realised = same_class[same_class["h"] == horizon]
            prior = realised[realised["origin_month"] + pd.DateOffset(months=horizon) <= origin]
            own_prior = own[
                (own["h"] == horizon)
                & (own["origin_month"] + pd.DateOffset(months=horizon) <= origin)
            ]
            combined = pd.concat([own_prior, prior], ignore_index=True)
            scaled = (combined["err"] / combined["scale"]).replace([np.inf, -np.inf], np.nan)
            scaled = scaled.dropna().to_numpy()
            cum_scaled = (
                (combined["cum_err"] / combined["scale"])
                if "cum_err" in combined
                else pd.Series(dtype=float)
            )
            cum_scaled = pd.to_numeric(cum_scaled, errors="coerce").replace(
                [np.inf, -np.inf], np.nan
            ).dropna().to_numpy()
            if len(scaled) < min_prior:
                lower = upper = cum_lower = cum_upper = np.nan
            else:
                lo, hi = np.quantile(scaled, list(quantiles))
                point_at_h = float(points[index])
                lower = max(0.0, point_at_h + float(lo) * scale)
                upper = max(0.0, point_at_h + float(hi) * scale)
                if len(cum_scaled) < min_prior:
                    cum_lower = cum_upper = np.nan
                else:
                    cum_lo, cum_hi = np.quantile(cum_scaled, list(quantiles))
                    cum_lower = max(0.0, float(cum_point[index]) + float(cum_lo) * scale)
                    cum_upper = max(0.0, float(cum_point[index]) + float(cum_hi) * scale)
            rows.append(
                {
                    "series_key": key,
                    "origin_month": origin,
                    "h": horizon,
                    "lower": lower,
                    "upper": upper,
                    "cum_lower": cum_lower,
                    "cum_upper": cum_upper,
                    "covered": bool(lower <= actual[index] <= upper),
                    "cum_covered": bool(cum_lower <= cum_actual[index] <= cum_upper),
                    "width": upper - lower,
                    "cum_width": cum_upper - cum_lower,
                    "n_prior": int(len(scaled)),
                    "cum_point": float(cum_point[index]),
                    "cum_actual": float(cum_actual[index]),
                    "actual": float(actual[index]),
                }
            )
    return pd.DataFrame(rows, columns=columns + ["cum_point", "cum_actual", "actual"])


def band_scores(bands: pd.DataFrame, alpha: float = 0.2, horizon: int = 12) -> dict:
    """
    Coverage, width and pinball loss for an 80% band frame.

    ``monthly_coverage`` counts every (pair, h) hit; ``cum12_coverage`` counts
    the h = ``horizon`` cumulative hit per pair. ``pinball`` is the mean
    monthly quantile loss at tau = alpha/2 and 1 - alpha/2; ``cum_pinball`` is
    the same on the cumulative band at h = ``horizon``. Widths are the mean
    monthly and cumulative width at ``horizon``.
    """
    empty = {
        "monthly_coverage": float("nan"),
        "cum12_coverage": float("nan"),
        "mean_width": float("nan"),
        "mean_cum_width": float("nan"),
        "pinball": float("nan"),
        "cum_pinball": float("nan"),
        "n_pairs": 0,
    }
    if bands is None or bands.empty:
        return empty
    frame = bands[bands["h"] <= int(horizon)]
    at_h = frame[frame["h"] == int(horizon)]
    monthly_pinball = [
        _pinball(alpha / 2, a, lo)
        for a, lo in zip(frame["actual"], frame["lower"])
        if np.isfinite(a) and np.isfinite(lo)
    ] + [
        _pinball(1 - alpha / 2, a, hi)
        for a, hi in zip(frame["actual"], frame["upper"])
        if np.isfinite(a) and np.isfinite(hi)
    ]
    cum_pinball = [
        _pinball(alpha / 2, a, lo)
        for a, lo in zip(at_h["cum_actual"], at_h["cum_lower"])
        if np.isfinite(a) and np.isfinite(lo)
    ] + [
        _pinball(1 - alpha / 2, a, hi)
        for a, hi in zip(at_h["cum_actual"], at_h["cum_upper"])
        if np.isfinite(a) and np.isfinite(hi)
    ]
    return {
        "monthly_coverage": float(frame["covered"].mean()),
        "cum12_coverage": float(at_h["cum_covered"].mean()) if not at_h.empty else float("nan"),
        "mean_width": float(frame["width"].mean()),
        "mean_cum_width": float(at_h["cum_width"].mean()) if not at_h.empty else float("nan"),
        "pinball": float(np.mean(monthly_pinball)) if monthly_pinball else float("nan"),
        "cum_pinball": float(np.mean(cum_pinball)) if cum_pinball else float("nan"),
        "n_pairs": int(len(at_h)),
    }


def native_band_scores(bt: pd.DataFrame, horizon: int = 12) -> dict:
    """Coverage and width of the arm's own 80% interval, where it has one."""
    empty = {"monthly_coverage": float("nan"), "cum12_coverage": float("nan"), "n_pairs": 0}
    if bt is None or bt.empty or "lo80" not in bt:
        return empty
    frame = bt[(bt["h"] <= int(horizon)) & bt["lo80"].notna() & bt["hi80"].notna()]
    if frame.empty:
        return empty
    inside = (frame["actual"] >= frame["lo80"]) & (frame["actual"] <= frame["hi80"])
    at_h = frame[frame["h"] == int(horizon)].copy()
    cum = []
    grouped = frame.groupby(["series_key", "origin_month"])
    for _, group in grouped:
        group = group.sort_values("h")
        if group["h"].max() < int(horizon):
            continue
        cum_cover = (group["actual"].cumsum() >= group["lo80"].cumsum()) & (
            group["actual"].cumsum() <= group["hi80"].cumsum()
        )
        cum.append(bool(cum_cover.iloc[-1]))
    return {
        "monthly_coverage": float(inside.mean()),
        "cum12_coverage": float(np.mean(cum)) if cum else float("nan"),
        "mean_width": float((frame["hi80"] - frame["lo80"]).mean()),
        "n_pairs": int(len(at_h)),
        "pairs": {
            (str(key), pd.Timestamp(origin))
            for key, origin in at_h[["series_key", "origin_month"]].itertuples(index=False)
        },
    }


def per_origin_loss(pairs: pd.DataFrame) -> pd.Series:
    """Plan §5 DM loss: ``sum |cumerr| / scale`` per origin (normalised panel loss)."""
    if pairs.empty:
        return pd.Series(dtype=float)
    frame = pairs.copy()
    frame["loss"] = frame["cumerr"].abs() / frame["scale"].replace(0.0, np.nan)
    return frame.groupby("origin_month")["loss"].sum().sort_index()
