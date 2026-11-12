"""
Empirical band bake-off for the category and savings forecasts.

Sizes an 80% band from errors at past, fully realised origins, using a scale
that was known at those origins. Backtest and production call the same
functions in ``src.analysis.empirical_band``.

Phases
------
``--phase dev``
    Score every candidate on dev origins, apply the pre-registered rule, write
    ``dev_bands.csv`` and the selection. Nothing is frozen yet.
``--freeze``
    Copy the selection into the tracked config and record its digest. Refuses
    unless the rule produced a qualifying winner for both forecasts.
``--phase test``
    Score the frozen winners once on the test window. Indicative only: that
    window was already used to choose the point method.
``--phase all``
    dev, freeze, test in one go (the freeze still happens before test scoring).

Usage:
    poetry run python scripts/experiments/band_bakeoff.py --phase dev
    poetry run python scripts/experiments/band_bakeoff.py --freeze
    poetry run python scripts/experiments/band_bakeoff.py --phase test
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.analysis.category_forecasters import (  # noqa: E402
    build_category_arm,
    panel_rolling_origin_backtest,
)
from src.analysis.category_forecast_production import (  # noqa: E402
    FROZEN_CLASS_MAP,
    FROZEN_DEFAULT,
)
from src.analysis.empirical_band import (  # noqa: E402
    MIN_SCALE,
    apply_offsets,
    band_from_pool,
    origin_scale,
    pool_with_size_bucket,
    size_bucket,
    size_tercile_cutpoints,
)
from src.analysis.forecast_backtest import trailing_mean  # noqa: E402
from src.analysis.intermittent import (  # noqa: E402
    build_panel,
    panel_to_series,
    sbc_class,
)

CONFIG_PATH = Path("configs/empirical_band_experiment.yaml")
CATEGORY_CONFIG = Path("configs/category_forecast_experiment.yaml")
SAVINGS_CONFIG = Path("configs/forecast_experiment.yaml")
CATEGORY_METHOD = "class_selector"


def load_config(path: Path | None = None) -> dict:
    target = Path(path) if path is not None else CONFIG_PATH
    return yaml.safe_load(target.read_text(encoding="utf-8"))


def write_config(config: dict) -> None:
    CONFIG_PATH.write_text(
        yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


def config_sha256(path: Path | None = None) -> str:
    target = Path(path) if path is not None else CONFIG_PATH
    return hashlib.sha256(target.read_bytes()).hexdigest()


def require_frozen(config: dict) -> None:
    meta = config.get("meta", {})
    if not meta.get("frozen"):
        raise SystemExit("Refusing to score test: the band config is not frozen.")
    if not meta.get("savings_selection"):
        raise SystemExit("Frozen config is missing a savings selection.")
    # A null category selection is a decided outcome ("no band qualified"),
    # so the key must be present even when its value is None.
    if "category_selection" not in meta:
        raise SystemExit("Frozen config is missing the category decision.")


# ── Category panel and backtests ─────────────────────────────────────────────


def load_category_panel() -> tuple[pd.DataFrame, pd.DatetimeIndex]:
    category_config = yaml.safe_load(CATEGORY_CONFIG.read_text(encoding="utf-8"))
    featured = pd.read_parquet(category_config["data"]["featured"])
    from src.analysis.category_prophet import build_category_monthly_series

    monthly = build_category_monthly_series(featured, as_of=None)
    panel = build_panel(monthly, last_complete=monthly["month"].max(), include_unassigned=False)
    months = pd.DatetimeIndex(sorted(panel["month"].unique()))
    return panel, months


def category_origins(months: pd.DatetimeIndex, initial: int = 36, horizon: int = 12) -> list:
    return [months[position - 1] for position in range(initial, len(months) - horizon + 1)]


def split_origins(origins: list, test_fraction: float, horizon: int) -> dict:
    n_test = max(1, int(np.floor(len(origins) * float(test_fraction))))
    test = origins[-n_test:]
    first_target = test[0] + pd.DateOffset(months=1)
    dev = [o for o in origins if o + pd.DateOffset(months=horizon) < first_target]
    embargo = [o for o in origins if o not in set(dev) and o not in set(test)]
    return {"all": origins, "dev": dev, "test": test, "embargo": embargo}


def run_category_points(panel: pd.DataFrame, origins: list, horizon: int = 12) -> pd.DataFrame:
    """class_selector point forecasts at every origin (cached on disk)."""
    cache = Path("private/experiments/band_bakeoff/2026-11-11/category_points_h12.parquet")
    if cache.exists():
        frame = pd.read_parquet(cache)
        if set(pd.to_datetime(frame["origin_month"].unique())) >= set(pd.to_datetime(origins)):
            return frame
    cache.parent.mkdir(parents=True, exist_ok=True)
    spec = {"arm_id": CATEGORY_METHOD, "params": {"class_map": FROZEN_CLASS_MAP, "default": FROZEN_DEFAULT}}
    arm = build_category_arm(spec)
    started = time.perf_counter()
    frame, stats = panel_rolling_origin_backtest(
        panel, arm, origins, horizon=horizon, n_jobs=15
    )
    frame.to_parquet(cache, index=False)
    (cache.with_suffix(".json")).write_text(
        json.dumps({"seconds": time.perf_counter() - started, **stats}, indent=2, default=str),
        encoding="utf-8",
    )
    return frame


# ── Error pools ──────────────────────────────────────────────────────────────


def category_pool(
    panel: pd.DataFrame,
    points: pd.DataFrame,
    origins: list,
    scale_mode: str,
    horizon: int = 12,
) -> pd.DataFrame:
    """
    Scaled error rows for every (series, origin) in ``origins``.

    ``scale`` comes from history up to the origin only. Errors are only stored
    where the whole horizon is realised, and ``band_from_pool`` additionally
    filters by date, so nothing observed after a scoring origin can enter.
    """
    series_map = panel_to_series(panel)
    classes = {key: sbc_class(y.to_numpy(dtype=float)) for key, y in series_map.items()}
    rows = []
    grouped = points.groupby(["series_key", "origin_month"], sort=True)
    for (key, origin), group in grouped:
        key = str(key)
        origin = pd.Timestamp(origin)
        y = series_map.get(key)
        if y is None:
            continue
        history = y[y.index <= origin].to_numpy(dtype=float)
        if history.size == 0:
            continue
        group = group.sort_values("h")
        actual = group["actual"].to_numpy(dtype=float)
        point = group["point"].to_numpy(dtype=float)
        if len(actual) < horizon or not np.isfinite(actual).all():
            continue
        if len(point) < horizon or not np.isfinite(point).all():
            continue
        scale = origin_scale(history, mode=scale_mode)
        cumulative = 0.0
        for step in range(horizon):
            error = float(actual[step] - point[step])
            cumulative += error
            rows.append(
                {
                    "series_key": key,
                    "sbc": classes.get(key),
                    "origin_month": origin,
                    "h": step + 1,
                    "err": error,
                    "cum_err": cumulative,
                    "scale": scale,
                }
            )
    return pd.DataFrame(
        rows, columns=["series_key", "sbc", "origin_month", "h", "err", "cum_err", "scale"]
    )


def savings_split_series() -> pd.Series:
    savings_config = yaml.safe_load(SAVINGS_CONFIG.read_text(encoding="utf-8"))
    split = pd.read_parquet(savings_config["data"]["cashflow_split"])
    from src.analysis.feature_engineering import complete_cashflow_series

    series = complete_cashflow_series(split)
    series.index = pd.DatetimeIndex(series.index).to_period("M").to_timestamp()
    return series


def savings_points(series: pd.Series, origins: list, horizon: int = 12) -> pd.DataFrame:
    """naive_mean_12 monthly points and actuals for every origin."""
    rows = []
    for origin in origins:
        history = series[series.index <= origin]
        if len(history) < 12:
            continue
        value = float(trailing_mean(history, 12))
        future = [
            origin + pd.DateOffset(months=step) for step in range(1, horizon + 1)
        ]
        actual = series.reindex(pd.DatetimeIndex(future)).to_numpy(dtype=float)
        if len(actual) < horizon or not np.isfinite(actual).all():
            continue
        for step in range(horizon):
            rows.append(
                {
                    "series_key": "savings",
                    "origin_month": pd.Timestamp(origin),
                    "h": step + 1,
                    "month": pd.DatetimeIndex(future)[step],
                    "actual": float(actual[step]),
                    "point": value,
                }
            )
    return pd.DataFrame(rows)


# ── Scoring ──────────────────────────────────────────────────────────────────


def summarise(frame: pd.DataFrame) -> dict:
    """
    Coverage, width and pinball for one candidate.

    Coverage is reported four ways, and the names are load-bearing:

    ``cum12_coverage``
        cumulative 12-month coverage over every scored pair, with a missing
        band counted as a miss. This is what a user experiences, and it is
        dragged down by the warm-up origins where no prior errors exist.
    ``cum12_coverage_emitted``
        the same over only the pairs that received a band. This is the
        calibration of the band the candidate emits, and it is what the
        selection rule judges: penalising warm-up in the choice would just
        favour whichever candidate hides less early history.
    ``joint12_coverage``
        the share of pairs where *every* month in the window is inside its own
        monthly band at once. This is a joint event and is not monthly
        coverage; it was previously mislabelled ``monthly_coverage``.
    ``monthly_pointwise_coverage``
        the share of (series, origin, month) rows inside their own monthly
        band. This is what the chart's monthly ribbons promise.

    ``missing_band_share`` is reported separately from ``zero_width_share``:
    a pair with no prior errors has no band at all, which is not a narrow band.
    """
    if frame.empty:
        return {
            "cum12_coverage": float("nan"),
            "cum12_coverage_emitted": float("nan"),
            "joint12_coverage": float("nan"),
            "monthly_pointwise_coverage": float("nan"),
            "monthly_rows": 0,
            "zero_width_share": float("nan"),
            "missing_band_share": float("nan"),
            "width_over_point": float("nan"),
            "n": 0,
            "cum_pinball": float("nan"),
        }
    has_band = frame["has_band"].to_numpy(dtype=bool)
    with_band = frame[has_band]
    width_ratio = (
        float(with_band["cum_width"].sum() / with_band["cum_point"].sum())
        if not with_band.empty and with_band["cum_point"].sum() > 0
        else float("nan")
    )
    pinball = []
    for _, row in with_band.iterrows():
        if not np.isfinite(row["cum_lower"]) or not np.isfinite(row["cum_upper"]):
            continue
        actual = row["cum_actual"]
        pinball.append(_pinball(0.1, actual, row["cum_lower"]))
        pinball.append(_pinball(0.9, actual, row["cum_upper"]))
    monthly_rows = int(frame["monthly_rows"].sum())
    monthly_hits = int(frame["monthly_hits"].sum())
    # Zero-width is a property of the *monthly* band, so it is measured over
    # pairs that have any monthly band, not over pairs with a cumulative one.
    with_monthly = frame[frame["monthly_rows"] > 0]
    return {
        "cum12_coverage": float(frame["covered_cum"].mean()),
        "cum12_coverage_emitted": float(with_band["covered_cum"].mean())
        if not with_band.empty
        else float("nan"),
        "joint12_coverage": float(frame["joint12_covered"].mean()),
        "monthly_pointwise_coverage": float(monthly_hits / monthly_rows)
        if monthly_rows > 0
        else float("nan"),
        "monthly_rows": monthly_rows,
        "zero_width_share": float(with_monthly["monthly_zero_width"].mean())
        if not with_monthly.empty
        else float("nan"),
        "missing_band_share": float((~has_band).mean()),
        "width_over_point": width_ratio,
        "n": int(len(frame)),
        "cum_pinball": float(np.mean(pinball)) if pinball else float("nan"),
    }


def _pinball(tau: float, actual: float, predicted: float) -> float:
    delta = actual - predicted
    return float(max(tau * delta, (tau - 1.0) * delta))


def score_category_candidate(
    candidate: dict,
    points: pd.DataFrame,
    panel: pd.DataFrame,
    origins: list,
    pools: dict[str, pd.DataFrame],
    horizon: int,
    rule: dict,
) -> tuple[dict, pd.DataFrame]:
    """Evaluate one category candidate on ``origins``."""
    series_map = panel_to_series(panel)
    scale_mode = str(candidate.get("scale_mode", "origin12"))
    pool_kind = str(candidate.get("pool", "class"))
    pool = pools[scale_mode]

    if pool_kind == "class_size_tercile":
        labelled = pool.copy()
        labelled["group_label"] = labelled["sbc"].astype(str) + "|" + labelled["size_bucket"].astype(str)
        label_column = "group_label"
        # The series' bucket at the scoring origin is resolved inside _score_k2
        # from the pool's own cut points.
        candidate = {**candidate, "_label_value": None}
        frame = _score_k2(
            candidate, points, series_map, labelled, origins, horizon, rule
        )
        return summarise(frame), frame

    label_column = "sbc"
    candidate = {**candidate, "_label_value": None}
    frame = _score_simple(
        candidate, points, series_map, pool, origins, horizon, label_column=label_column
    )
    return summarise(frame), frame


def _score_simple(
    candidate: dict,
    points: pd.DataFrame,
    series_map: dict[str, pd.Series],
    pool: pd.DataFrame,
    origins: list,
    horizon: int,
    label_column: str,
) -> pd.DataFrame:
    """K0/K1: pool by class, label = the series' own SBC class at the origin."""
    rows = []
    keep = set(pd.to_datetime(list(origins)))
    subset = points[points["origin_month"].isin(keep)]
    classes = {key: sbc_class(y.to_numpy(dtype=float)) for key, y in series_map.items()}
    scale_mode = str(candidate.get("scale_mode", "origin12"))
    for (key, origin), group in subset.groupby(["series_key", "origin_month"], sort=True):
        key = str(key)
        origin = pd.Timestamp(origin)
        y = series_map[key]
        history = y[y.index <= origin].to_numpy(dtype=float)
        group = group.sort_values("h")
        point = group["point"].to_numpy(dtype=float)
        actual = group["actual"].to_numpy(dtype=float)
        scale = origin_scale(history, mode=scale_mode)
        m_lo, m_hi, c_lo, c_hi = band_from_pool(
            point,
            scale,
            pool,
            key=key,
            origin=origin,
            horizon=horizon,
            label=classes.get(key),
            label_column=label_column,
            include_peers=True,
        )
        rows.append(_pair_row(key, origin, point, actual, m_lo, m_hi, c_lo, c_hi, scale, horizon))
    return pd.DataFrame(rows)


def _score_k2(
    candidate: dict,
    points: pd.DataFrame,
    series_map: dict[str, pd.Series],
    labelled_pool: pd.DataFrame,
    origins: list,
    horizon: int,
    rule: dict,
) -> pd.DataFrame:
    """K2: pool by class x size tercile, buckets as of each origin."""
    rows = []
    keep = set(pd.to_datetime(list(origins)))
    subset = points[points["origin_month"].isin(keep)]
    classes = {key: sbc_class(y.to_numpy(dtype=float)) for key, y in series_map.items()}
    scale_mode = str(candidate.get("scale_mode", "max_history"))
    cutcache: dict = {}
    for (key, origin), group in subset.groupby(["series_key", "origin_month"], sort=True):
        key = str(key)
        origin = pd.Timestamp(origin)
        y = series_map[key]
        history = y[y.index <= origin].to_numpy(dtype=float)
        group = group.sort_values("h")
        point = group["point"].to_numpy(dtype=float)
        actual = group["actual"].to_numpy(dtype=float)
        scale = origin_scale(history, mode=scale_mode)
        if origin not in cutcache:
            cutcache[origin] = size_tercile_cutpoints(labelled_pool, origin, horizon=horizon)
        bucket = size_bucket(scale, cutcache[origin])
        label = f"{classes.get(key)}|{bucket}"
        m_lo, m_hi, c_lo, c_hi = band_from_pool(
            point,
            scale,
            labelled_pool,
            key=key,
            origin=origin,
            horizon=horizon,
            label=label,
            label_column="group_label",
            include_peers=True,
        )
        rows.append(_pair_row(key, origin, point, actual, m_lo, m_hi, c_lo, c_hi, scale, horizon))
    return pd.DataFrame(rows)


def _pair_row(key, origin, point, actual, m_lo, m_hi, c_lo, c_hi, scale, horizon) -> dict:
    frame = apply_offsets(point, m_lo, m_hi, c_lo, c_hi, snap=0.0)
    last = frame.iloc[horizon - 1]
    cum_actual = float(np.cumsum(actual)[horizon - 1])
    lower = frame["ci80_lower"].to_numpy()
    upper = frame["ci80_upper"].to_numpy()

    # Band presence is per horizon, not per pair. A pair can have a band at
    # h = 1..9 and none at h = 12, because each step needs its own prior
    # errors. Treating "any horizon has a band" as "the pair has a band" made
    # a null h = 12 band look like a zero-width one and let empty cumulative
    # bands be counted as coverage failures instead of missing.
    monthly_present = np.isfinite(lower) & np.isfinite(upper)
    inside = (actual >= lower) & (actual <= upper) & monthly_present
    has_cum_band = bool(np.isfinite(last["cum_ci80_lower"]) and np.isfinite(last["cum_ci80_upper"]))

    # Joint coverage: every month present and inside its own band at once.
    # This is a joint event and is *not* monthly coverage.
    joint12_covered = bool(monthly_present.all() and inside.all())
    covered_cum = bool(
        has_cum_band and last["cum_ci80_lower"] <= cum_actual <= last["cum_ci80_upper"]
    )
    return {
        "series_key": key,
        "origin_month": origin,
        "cum_actual": cum_actual,
        "cum_point": float(last["cum_point"]),
        "cum_lower": float(last["cum_ci80_lower"]),
        "cum_upper": float(last["cum_ci80_upper"]),
        "cum_width": float(last["cum_ci80_upper"] - last["cum_ci80_lower"]) if has_cum_band else np.nan,
        "has_band": has_cum_band,
        "covered_cum": covered_cum,
        "joint12_covered": joint12_covered,
        # Pointwise monthly coverage: only months that actually have a band
        # can be hit or missed. This is what the chart's monthly ribbons mean.
        "monthly_hits": int(inside.sum()),
        "monthly_rows": int(monthly_present.sum()),
        "monthly_zero_width": bool(
            monthly_present.any() and np.all((upper - lower)[monthly_present] <= 0)
        ),
        "scale": scale,
        "quiet": bool(scale <= MIN_SCALE),
    }


def savings_candidates(
    points: pd.DataFrame,
    series: pd.Series,
    origins: list,
    horizon: int,
    pool_points: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """
    V0 / V1 / V2 frames over ``origins``.

    ``pool_points`` supplies every realised origin available for estimating
    quantiles; ``origins`` is only what gets scored. Passing the scored origins
    as the pool would leave the early ones with no history at all.
    """
    out: dict[str, pd.DataFrame] = {}
    keep = set(pd.to_datetime(list(origins)))
    subset = points[points["origin_month"].isin(keep)]
    pool_source = points if pool_points is None else pool_points

    # V0: expanding empirical quantiles, only from origins realised before the
    # scoring origin (the deployable form of what ships today).
    v0_rows = []
    for origin in sorted(keep):
        group = subset[subset["origin_month"] == origin]
        if group.empty:
            continue
        group = group.sort_values("h")
        point = group["point"].to_numpy(dtype=float)
        actual = group["actual"].to_numpy(dtype=float)
        past = pool_source[pool_source["origin_month"] + pd.DateOffset(months=1) <= origin]
        m_lo = np.full(horizon, np.nan)
        m_hi = np.full(horizon, np.nan)
        c_lo = np.full(horizon, np.nan)
        c_hi = np.full(horizon, np.nan)
        for step in range(horizon):
            rows = past[past["h"] == step + 1]
            if len(rows) < 8:
                continue
            monthly = (rows["actual"] - rows["point"]).to_numpy(dtype=float)
            cum = (
                rows.sort_values(["origin_month", "h"])
                .groupby("origin_month")["actual"]
                .cumsum()
                .to_numpy(dtype=float)
            )
            cum_point = (
                rows.sort_values(["origin_month", "h"])
                .groupby("origin_month")["point"]
                .cumsum()
                .to_numpy(dtype=float)
            )
            cum_err = cum - cum_point
            lo, hi = np.quantile(monthly, [0.1, 0.9])
            clo, chi = np.quantile(cum_err, [0.1, 0.9])
            m_lo[step], m_hi[step] = lo, hi
            c_lo[step], c_hi[step] = clo, chi
        v0_rows.append(_pair_row("savings", origin, point, actual, m_lo, m_hi, c_lo, c_hi, 1.0, horizon))
    out["V0"] = pd.DataFrame(v0_rows)

    # V1 / V2: own-series pool, scale from history at the origin. The pool is
    # built from every realised origin, not just the scored ones, so early
    # scored origins still have prior errors to draw on.
    pool_rows = []
    for origin in sorted(set(pd.to_datetime(pool_source["origin_month"]))):
        group = pool_source[pool_source["origin_month"] == origin].sort_values("h")
        history = series[series.index <= origin].to_numpy(dtype=float)
        if history.size == 0 or group.empty:
            continue
        scale = origin_scale(history, mode="origin12")
        actual = group["actual"].to_numpy(dtype=float)
        point = group["point"].to_numpy(dtype=float)
        cumulative = 0.0
        for step in range(min(horizon, len(actual))):
            error = float(actual[step] - point[step])
            cumulative += error
            pool_rows.append(
                {
                    "series_key": "savings",
                    "origin_month": pd.Timestamp(origin),
                    "h": step + 1,
                    "err": error,
                    "cum_err": cumulative,
                    "scale": scale,
                }
            )
    pool = pd.DataFrame(pool_rows)

    for label, scale_mode in (("V1", "origin12"), ("V2", "origin12_std")):
        rows = []
        for origin in sorted(keep):
            group = subset[subset["origin_month"] == origin]
            if group.empty:
                continue
            group = group.sort_values("h")
            history = series[series.index <= origin].to_numpy(dtype=float)
            point = group["point"].to_numpy(dtype=float)
            actual = group["actual"].to_numpy(dtype=float)
            if scale_mode == "origin12_std":
                scale = float(max(np.std(history[-12:], ddof=0), MIN_SCALE))
            else:
                scale = origin_scale(history, mode="origin12")
            m_lo, m_hi, c_lo, c_hi = band_from_pool(
                point,
                scale,
                pool,
                key="savings",
                origin=origin,
                horizon=horizon,
                label=None,
                include_peers=False,
            )
            rows.append(_pair_row("savings", origin, point, actual, m_lo, m_hi, c_lo, c_hi, scale, horizon))
        out[label] = pd.DataFrame(rows)
    return out


# ── Rule ─────────────────────────────────────────────────────────────────────


def choose_winner(table: pd.DataFrame, rule: dict, guardrail: pd.Series | None = None) -> dict:
    """
    Apply the pre-registered rule to a dev table.

    Winner: cum-12 coverage of the band actually emitted, inside the window and
    closest to the target; ties within ``tie_margin`` go to the lower
    cumulative pinball loss. For categories the winner must also clear the
    top-tercile guardrail.

    Pairs with no band are excluded from the decision (they cannot be
    calibrated) but their share is reported: a candidate that emits a band for
    almost nobody must not win on the few it does emit.
    """
    low, high = rule["coverage_window"]
    target = float(rule["target_coverage"])
    margin = float(rule["tie_margin"])
    column = "cum12_coverage_emitted"

    # A candidate that emits a band for less than half its pairs is not a
    # shipping candidate, whatever its coverage on the rest.
    usable = table[np.isfinite(table[column]) & (table["missing_band_share"] <= 0.5)]
    eligible = usable[(usable[column] >= low) & (usable[column] <= high)].copy()
    if eligible.empty:
        detail = ", ".join(
            f"{row['candidate']} {row[column]:.3f}" for _, row in usable.iterrows()
        )
        return {
            "winner": None,
            "reason": f"no candidate fell inside the coverage window ({detail})",
        }

    eligible["distance"] = (eligible[column] - target).abs()
    best_distance = float(eligible["distance"].min())
    # The tie rule only applies to candidates inside best_distance + margin.
    tied = eligible[eligible["distance"] <= best_distance + margin]
    winner = tied.sort_values(["cum_pinball", "distance"]).iloc[0]
    tie_break_used = len(tied) > 1
    # Was the winner also the strictly closest, or did the tie margin pull in a
    # candidate that was further away?
    winner_is_closest = winner["distance"] == best_distance

    if guardrail is not None and float(guardrail.get(winner["candidate"], np.nan)) < float(
        rule["category_guardrail"]["min_coverage"]
    ):
        remaining = eligible.drop(index=winner.name).sort_values(["distance", "cum_pinball"])
        for _, row in remaining.iterrows():
            if float(guardrail.get(row["candidate"], np.nan)) >= float(
                rule["category_guardrail"]["min_coverage"]
            ):
                return {
                    "winner": row["candidate"],
                    "reason": (
                        f"{row['candidate']} selected after the top-tercile guardrail rejected "
                        f"{winner['candidate']}; coverage {row[column]:.3f} inside the window"
                    ),
                    "basis": "guardrail",
                    "row": row.to_dict(),
                }
        return {"winner": None, "reason": "no candidate cleared the top-tercile guardrail"}

    # State the actual basis, not a generic phrase: a candidate can win by
    # being the only one inside the window, which is not a tie.
    if len(eligible) == 1:
        reason = (
            f"{winner['candidate']} was the only candidate inside "
            f"[{low:.2f}, {high:.2f}]; coverage {winner[column]:.3f}, "
            f"{winner['distance']:.3f} from the {target:.2f} target"
        )
        basis = "only_candidate_in_window"
    elif tie_break_used and not winner_is_closest:
        reason = (
            f"{winner['candidate']} selected by the {margin:.2f} tie margin: coverage "
            f"{winner[column]:.3f} is {winner['distance']:.3f} from the target, and it had "
            f"the lowest cumulative pinball ({winner['cum_pinball']:.1f}) among candidates "
            f"within {best_distance + margin:.3f} of it"
        )
        basis = "tie_break_lower_pinball"
    elif tie_break_used:
        reason = (
            f"{winner['candidate']} was closest to the {target:.2f} target (coverage "
            f"{winner[column]:.3f}, {winner['distance']:.3f} away) and had the lowest "
            f"cumulative pinball ({winner['cum_pinball']:.1f}) among the candidates tied "
            f"within {margin:.2f}"
        )
        basis = "closest_and_lowest_pinball"
    else:
        reason = (
            f"{winner['candidate']} was the closest candidate to the {target:.2f} target: "
            f"coverage {winner[column]:.3f}, {winner['distance']:.3f} away, with no other "
            f"candidate inside the {margin:.2f} tie margin"
        )
        basis = "closest_no_tie"
    return {
        "winner": winner["candidate"],
        "reason": reason,
        "basis": basis,
        "coverage": float(winner[column]),
        "distance_from_target": float(winner["distance"]),
        "candidates_in_window": int(len(eligible)),
        "row": winner.to_dict(),
    }


# ── Phases ───────────────────────────────────────────────────────────────────


def dev_phase(config: dict, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    horizon = int(config["split"]["horizon_primary"])
    rule = config["rule"]
    burn_in = int(rule.get("burn_in_origins", 0))

    # Categories
    panel, months = load_category_panel()
    origins = category_origins(months)
    split = split_origins(origins, config["split"]["category_test_fraction"], horizon)
    points = run_category_points(panel, split["all"], horizon)

    # Burn-in is the first N dev origins: a quantile from a handful of errors
    # measures warm-up, not the band that would ship. Reported, not scored.
    dev_scored = split["dev"][burn_in:]
    dev_burn_in = split["dev"][:burn_in]

    pools = {}
    for mode in ("origin12", "max_history"):
        pools[mode] = category_pool(panel, points, split["all"], mode, horizon)

    # K2 needs buckets stored on the pool, labelled by each stored origin.
    k2_pool = pool_with_size_bucket(pools["max_history"], split["all"][-1], horizon=horizon)
    k2_pool["group_label"] = k2_pool["sbc"].astype(str) + "|" + k2_pool["size_bucket"].astype(str)

    rows = []
    frames: dict[str, pd.DataFrame] = {}
    for candidate in config["bands"]["candidates"]["categories"]:
        cid = candidate["id"]
        if str(candidate.get("pool")) == "class_size_tercile":
            frame = _score_k2(
                candidate, points, panel_to_series(panel), k2_pool, dev_scored, horizon, rule
            )
        else:
            frame = _score_simple(
                candidate,
                points,
                panel_to_series(panel),
                pools[str(candidate.get("scale_mode", "origin12"))],
                dev_scored,
                horizon,
                label_column="sbc",
            )
        frames[cid] = frame
        summary = summarise(frame)
        if dev_burn_in:
            burn_frame = _score_simple(
                candidate,
                points,
                panel_to_series(panel),
                pools[str(candidate.get("scale_mode", "origin12"))],
                dev_burn_in,
                horizon,
                label_column="sbc",
            )
            burn_summary = summarise(burn_frame)
            summary["burn_in_cum12_coverage"] = burn_summary["cum12_coverage_emitted"]
            summary["burn_in_n"] = burn_summary["n"]
            summary["burn_in_missing_share"] = burn_summary["missing_band_share"]
        rows.append({"candidate": cid, **summary})
        print(f"  {cid}: " + ", ".join(f"{k}={v:.3f}" if isinstance(v, float) else f"{k}={v}" for k, v in summary.items()))

    dev_table = pd.DataFrame(rows)
    dev_table.to_csv(out_dir / "dev_bands.csv", index=False)

    # Per-tercile and quiet breakdown for the report.
    detail = []
    for cid, frame in frames.items():
        if frame.empty:
            continue
        # Terciles are cut on the scored pairs' scales, and each stratum's
        # coverage counts only pairs that actually received a band.
        low, high = np.quantile(frame["scale"].to_numpy(dtype=float), [1 / 3, 2 / 3])
        bucket = np.where(frame["scale"] <= low, "small", np.where(frame["scale"] <= high, "mid", "large"))

        def _emit(mask) -> tuple[float, int]:
            subset = frame[mask & frame["has_band"].to_numpy(dtype=bool)]
            if subset.empty:
                return float("nan"), 0
            return float(subset["covered_cum"].mean()), int(len(subset))

        for name in ("small", "mid", "large"):
            mask = bucket == name
            if mask.sum() == 0:
                continue
            coverage, count = _emit(mask)
            detail.append(
                {
                    "candidate": cid,
                    "stratum": f"size_{name}",
                    "cum12_coverage": coverage,
                    "n": count,
                }
            )
        quiet = frame["quiet"].to_numpy(dtype=bool)
        if quiet.any():
            coverage, count = _emit(quiet)
            detail.append(
                {
                    "candidate": cid,
                    "stratum": "quiet",
                    "cum12_coverage": coverage,
                    "n": count,
                }
            )
        overall, total = _emit(np.ones(len(frame), dtype=bool))
        detail.append(
            {
                "candidate": cid,
                "stratum": "all",
                "cum12_coverage": overall,
                "n": total,
            }
        )
    pd.DataFrame(detail).to_csv(out_dir / "dev_bands_by_stratum.csv", index=False)

    top_guard = {
        row["candidate"]: row["cum12_coverage"]
        for row in detail
        if row["stratum"] == "size_large"
    }
    category_choice = choose_winner(dev_table, rule, guardrail=pd.Series(top_guard))
    print("category winner:", category_choice["winner"], "-", category_choice["reason"])

    # Savings
    savings_series = savings_split_series()
    savings_origins_all = [
        savings_series.index[position - 1]
        for position in range(36, len(savings_series) - horizon + 1)
    ]
    savings_split = split_origins(
        savings_origins_all, config["split"]["savings_test_fraction"], horizon
    )
    # Pool origins run from the 12-month minimum, which is earlier than the
    # first scored origin, so early dev origins have prior errors to draw on.
    savings_pool_origins = [
        savings_series.index[position - 1]
        for position in range(13, len(savings_series) - horizon + 1)
    ]
    savings_pts = savings_points(savings_series, savings_split["all"], horizon)
    savings_pool_pts = savings_points(savings_series, savings_pool_origins, horizon)
    savings_scored = savings_split["dev"][burn_in:]
    savings_burn_in = savings_split["dev"][:burn_in]
    savings_frames = savings_candidates(
        savings_pts, savings_series, savings_scored, horizon, pool_points=savings_pool_pts
    )
    savings_rows = []
    for cid, frame in savings_frames.items():
        summary = summarise(frame)
        if savings_burn_in:
            burn_frames = savings_candidates(
                savings_pts,
                savings_series,
                savings_burn_in,
                horizon,
                pool_points=savings_pool_pts,
            )
            summary["burn_in_cum12_coverage"] = summarise(burn_frames[cid])[
                "cum12_coverage_emitted"
            ]
        savings_rows.append({"candidate": cid, **summary})
        print(f"  {cid}: " + ", ".join(f"{k}={v:.3f}" if isinstance(v, float) else f"{k}={v}" for k, v in summary.items()))
    savings_table = pd.DataFrame(savings_rows)
    savings_table.to_csv(out_dir / "dev_bands_savings.csv", index=False)
    savings_choice = choose_winner(savings_table, rule)
    print("savings winner:", savings_choice["winner"], "-", savings_choice["reason"])

    payload = {
        "category": category_choice,
        "savings": savings_choice,
        "category_origins": {"dev": len(split["dev"]), "test": len(split["test"])},
        "savings_origins": {"dev": len(savings_split["dev"]), "test": len(savings_split["test"])},
        "config_sha256": config_sha256(),
    }
    (out_dir / "dev_selection.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    return payload


def freeze(config: dict, out_dir: Path) -> None:
    path = out_dir / "dev_selection.json"
    if not path.exists():
        raise SystemExit("No dev_selection.json: run --phase dev first.")
    payload = json.loads(path.read_text(encoding="utf-8"))
    category = payload.get("category", {})
    savings = payload.get("savings", {})
    if not savings.get("winner"):
        raise SystemExit(
            "Refusing to freeze: no savings winner "
            f"({savings.get('reason')}); nothing would ship."
        )

    # The rule says: if nothing qualifies, stop and ship nothing. A missing
    # category winner is recorded explicitly as "no band", which is a valid
    # outcome, not a failure to decide.
    if not category.get("winner"):
        config["meta"]["category_selection"] = None
        config["meta"]["category_reason"] = category.get("reason")
        print(f"No category band ships: {category.get('reason')}")
    else:
        config["meta"]["category_selection"] = category["winner"]
        config["meta"]["category_reason"] = category.get("reason")

    config["meta"]["savings_selection"] = savings["winner"]
    config["meta"]["savings_reason"] = savings.get("reason")
    config["meta"]["frozen"] = True
    config["meta"]["frozen_at"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    write_config(config)
    digest = config_sha256()
    (out_dir / "frozen_config.sha256").write_text(digest + "\n", encoding="utf-8")
    print(f"Frozen band config. sha256 = {digest}")
    print("Commit configs/empirical_band_experiment.yaml and the code before --phase test.")


def test_phase(config: dict, out_dir: Path) -> dict:
    require_frozen(config)
    horizon = int(config["split"]["horizon_primary"])
    category_id = config["meta"]["category_selection"]
    savings_id = config["meta"]["savings_selection"]

    panel, months = load_category_panel()
    origins = category_origins(months)
    split = split_origins(origins, config["split"]["category_test_fraction"], horizon)
    points = run_category_points(panel, split["all"], horizon)

    if category_id is None:
        # Nothing qualified on dev, so no category band ships. Scoring a null
        # band would be meaningless; the outcome is recorded instead.
        category_test = {
            "shipped": False,
            "reason": config["meta"].get("category_reason"),
            "cum12_coverage": None,
        }
    else:
        candidates = {c["id"]: c for c in config["bands"]["candidates"]["categories"]}
        candidate = candidates[category_id]
        pool = category_pool(
            panel, points, split["all"], str(candidate["scale_mode"]), horizon
        )
        series_map = panel_to_series(panel)
        if str(candidate.get("pool")) == "class_size_tercile":
            pool = pool_with_size_bucket(pool, split["all"][-1], horizon=horizon)
            pool["group_label"] = (
                pool["sbc"].astype(str) + "|" + pool["size_bucket"].astype(str)
            )
            frame = _score_k2(
                candidate, points, series_map, pool, split["test"], horizon, config["rule"]
            )
        else:
            frame = _score_simple(
                candidate, points, series_map, pool, split["test"], horizon, label_column="sbc"
            )
        category_test = summarise(frame)

    savings_series = savings_split_series()
    savings_origins_all = [
        savings_series.index[position - 1]
        for position in range(36, len(savings_series) - horizon + 1)
    ]
    savings_split = split_origins(
        savings_origins_all, config["split"]["savings_test_fraction"], horizon
    )
    savings_pts = savings_points(savings_series, savings_split["all"], horizon)
    savings_pool_origins = [
        savings_series.index[position - 1]
        for position in range(13, len(savings_series) - horizon + 1)
    ]
    savings_pool_pts = savings_points(savings_series, savings_pool_origins, horizon)
    savings_frames = savings_candidates(
        savings_pts, savings_series, savings_split["test"], horizon, pool_points=savings_pool_pts
    )
    savings_test = summarise(savings_frames[savings_id])

    summary = {
        "meta": {
            "band_config_sha256": config_sha256(),
            "point_method": CATEGORY_METHOD,
            "category_band": category_id,
            "savings_band": savings_id,
        },
        "category": category_test,
        "savings": savings_test,
        "note": (
            "Indicative only: the test window was already used to choose the point method. "
            "No choice was changed after seeing these numbers."
        ),
    }
    (out_dir / "test_bands.json").write_text(json.dumps(summary, indent=2, default=float), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=float))
    return summary


def main(argv=None) -> int:
    global CONFIG_PATH
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("dev", "test", "all"))
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--config", default=str(CONFIG_PATH))
    args = parser.parse_args(argv)

    CONFIG_PATH = Path(args.config)
    config = load_config(CONFIG_PATH)
    out_dir = Path(config["meta"]["output_dir"])

    if args.freeze:
        freeze(config, out_dir)
        return 0
    if args.phase == "dev":
        dev_phase(config, out_dir)
        return 0
    if args.phase == "test":
        test_phase(config, out_dir)
        return 0
    if args.phase == "all":
        dev_phase(config, out_dir)
        freeze(load_config(CONFIG_PATH), out_dir)
        test_phase(load_config(CONFIG_PATH), out_dir)
        return 0
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
