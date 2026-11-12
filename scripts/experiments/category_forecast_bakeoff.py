"""
Category forecast bake-off runner (Phases 0, 1, 3 and 4).

Reads ``configs/category_forecast_experiment.yaml``, builds the corrected
panel from ``build_category_monthly_series``, backtests the arms from
``src.analysis.category_forecasters`` on dev origins, freezes the selection,
then scores the frozen arms on test origins and applies the plan's section 7
rules mechanically.

Phases
------
``--phase timing``
    Phase 0: time a fixed sample of P0, C1 and I2 fits, project the full run,
    and write ``timing.json`` plus the step decision into the config. Refuses
    to overwrite an existing timing decision.
``--phase diagnostics``
    Phase 1: per-series diagnostics on data up to the first test origin.
``--phase dev``
    Phase 3: evaluate every candidate on dev origins, pick one variant per
    arm, build S1's class map, S2's private table and the H options.
``--freeze``
    Write the dev choices into the config, set ``frozen: true`` and record the
    digest.
``--phase test``
    Phase 4: refuses to run unless frozen. Scores the test origins, writes
    leaderboards, strata, bands, DM tests and the verdict.
``--phase report``
    Re-render ``report.md`` from cached artefacts (see
    ``render_category_report.py``).

Usage:
    poetry run python scripts/experiments/category_forecast_bakeoff.py --phase timing
    poetry run python scripts/experiments/category_forecast_bakeoff.py --phase diagnostics
    poetry run python scripts/experiments/category_forecast_bakeoff.py --phase dev
    poetry run python scripts/experiments/category_forecast_bakeoff.py --freeze
    poetry run python scripts/experiments/category_forecast_bakeoff.py --phase test
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
    build_local_arm,
    cumulative_columns,
    panel_rolling_origin_backtest,
    spec_slug,
)
from src.analysis.category_prophet import build_category_monthly_series  # noqa: E402
from src.analysis.forecast_metrics import (  # noqa: E402
    avg_rel_cum,
    band_scores,
    dm_test_hln,
    money_weighted_ratio,
    native_band_scores,
    pair_cumulative,
    per_origin_loss,
    phantom_spend,
    pooled_bias,
    scaled_pooled_band,
)
from src.analysis.intermittent import (  # noqa: E402
    ADI_CUTOFF,
    CV2_CUTOFF,
    SBC_CLASSES,
    build_hierarchy,
    build_panel,
    demand_stats,
    panel_to_series,
    parse_series_key,
    sbc_class,
)

CONFIG_PATH = Path("configs/category_forecast_experiment.yaml")
BASE_ARM = "naive_mean_12"
_LOCAL_KINDS = ("local", "series_local")


# ── Config helpers ────────────────────────────────────────────────────────────


def load_config(path: Path | None = None) -> dict:
    target = Path(path) if path is not None else CONFIG_PATH
    return yaml.safe_load(target.read_text(encoding="utf-8"))


def config_sha256(path: Path | None = None) -> str:
    target = Path(path) if path is not None else CONFIG_PATH
    return hashlib.sha256(target.read_bytes()).hexdigest()


def require_frozen(config: dict) -> None:
    if not config.get("meta", {}).get("frozen"):
        raise SystemExit(
            "Refusing to run the test phase: the config has `frozen: false`. "
            "Run --phase dev, then --freeze, and commit the config first (plan Phase 3)."
        )


def write_config(config: dict) -> None:
    CONFIG_PATH.write_text(
        yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


# ── Data, panel and split ─────────────────────────────────────────────────────


def load_panel(config: dict) -> pd.DataFrame:
    featured = pd.read_parquet(config["data"]["featured"])
    monthly = build_category_monthly_series(featured, as_of=None)
    return build_panel(monthly, last_complete=monthly["month"].max(), include_unassigned=True)


def panel_months(panel: pd.DataFrame) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(sorted(panel["month"].unique()))


def make_split(panel: pd.DataFrame, config: dict) -> dict:
    """
    Purged/embargoed split by origin date (plan §3).

    ``test`` is the last third of the h=12 origins; ``dev`` holds earlier
    origins whose whole 12-month target window ends before the first test
    month; the rest is the embargo.
    """
    months = panel_months(panel)
    initial = int(config["split"]["initial"])
    horizon = int(config["split"]["horizon_primary"])
    step = int(config["split"]["step"] or 1)
    positions = list(range(initial, len(months) - horizon + 1, step))
    origins = [months[position - 1] for position in positions]

    n_test = max(1, int(np.floor(len(origins) * float(config["split"]["test_fraction"]))))
    test = origins[-n_test:]
    first_test_target = test[0] + pd.DateOffset(months=1)
    dev = [o for o in origins if o + pd.DateOffset(months=horizon) < first_test_target]
    embargo = [o for o in origins if o not in set(dev) and o not in set(test)]
    return {
        "positions": positions,
        "origins": origins,
        "dev": dev,
        "test": test,
        "embargo": embargo,
        "first_test_target": first_test_target,
        "months": months,
    }


def scored_keys(panel: pd.DataFrame) -> list[str]:
    """Every series except the ``unassigned`` nodes (never scored, plan §1)."""
    meta = panel[["series_key"]].drop_duplicates()
    return [
        str(key)
        for key in meta["series_key"]
        if not parse_series_key(str(key))["is_unassigned"]
    ]


def classes_at(panel: pd.DataFrame, origin: pd.Timestamp) -> dict[str, str]:
    """SBC class per series from its training window up to ``origin``."""
    train = panel = panel[panel["month"] <= origin]
    out = {}
    for key, group in train.groupby("series_key", sort=False):
        y = group.sort_values("month")["y"].to_numpy(dtype=float)
        if len(y) >= 12:
            out[str(key)] = sbc_class(y)
    return out


# ── Cache ─────────────────────────────────────────────────────────────────────


def backtest_path(out_dir: Path, phase: str, spec: dict, horizon: int) -> Path:
    folder = out_dir / "backtests" if phase == "test" else out_dir / "backtests" / "dev"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{spec['arm_id']}_{spec_slug(spec)}_h{horizon}.parquet"


def run_arm(
    spec: dict,
    panel: pd.DataFrame,
    config: dict,
    out_dir: Path,
    phase: str,
    horizon: int,
    origins,
    context: dict | None = None,
    force: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """Backtest one configured arm, reusing the cached parquet when possible."""
    path = backtest_path(out_dir, phase, spec, horizon)
    meta_path = path.with_suffix(".json")
    if path.exists() and meta_path.exists() and not force:
        return pd.read_parquet(path), json.loads(meta_path.read_text(encoding="utf-8"))

    arm = build_category_arm(spec, context)
    start = time.perf_counter()
    bt, stats = panel_rolling_origin_backtest(
        panel, arm, origins, horizon=horizon, n_jobs=int(config["cost"]["workers"]), context=context
    )
    stats = {
        **stats,
        "arm": spec["arm_id"],
        "params": spec.get("params", {}),
        "horizon": horizon,
        "seconds": time.perf_counter() - start,
        "origins": len(origins),
        "config_sha256": config_sha256(),
    }
    bt.to_parquet(path, index=False)
    meta_path.write_text(json.dumps(stats, indent=2, default=float), encoding="utf-8")
    return bt, stats


# ── Scoring helpers ──────────────────────────────────────────────────────────


def pairs_for(
    bt: pd.DataFrame,
    panel: pd.DataFrame,
    origins,
    horizon: int,
    exclude_unassigned: bool = True,
) -> pd.DataFrame:
    """``pair_cumulative`` restricted to the scored series and given origins."""
    frame = bt
    if exclude_unassigned:
        meta = panel[["series_key"]].drop_duplicates()
        keep = {
            str(key)
            for key in meta["series_key"]
            if not parse_series_key(str(key))["is_unassigned"]
        }
        frame = frame[frame["series_key"].isin(keep)]
    keep_origins = set(pd.to_datetime(list(origins)))
    frame = frame[frame["origin_month"].isin(keep_origins)]
    return pair_cumulative(frame, horizon=horizon)


def dormancy(panel: pd.DataFrame, origin: pd.Timestamp) -> dict[str, bool]:
    """True when the series had no activity in the 12 months up to ``origin``."""
    train = panel[panel["month"] <= origin]
    window_start = origin - pd.DateOffset(months=11)
    out = {}
    for key, group in train.groupby("series_key", sort=False):
        recent = group[group["month"] >= window_start]["y"]
        out[str(key)] = bool(len(recent) == 0 or recent.sum() <= 0)
    return out


def strata_table(pairs: pd.DataFrame, panel: pd.DataFrame, base_pairs: pd.DataFrame, origin: pd.Timestamp) -> pd.DataFrame:
    """AvgRelCum12 per flow x level, SBC class and dormant/active."""
    if pairs.empty:
        return pd.DataFrame(columns=["stratum", "value", "avg_rel", "n_series", "cum_abs"])
    classes = classes_at(panel, origin)
    dormant = dormancy(panel, origin)
    meta = pd.DataFrame(
        [{"series_key": key, **parse_series_key(key), "sbc": classes.get(key), "dormant": dormant.get(key)} for key in pairs["series_key"].unique()]
    )
    merged = pairs.merge(meta, on="series_key", how="left")
    base = base_pairs.copy()
    rows = []

    def _row(label: str, value, frame: pd.DataFrame, label_col: str | None = None):
        if frame.empty:
            return
        sub_keys = set(frame["series_key"])
        base_sub = base[base["series_key"].isin(sub_keys)]
        metric = avg_rel_cum(frame, base_sub)
        rows.append(
            {
                "stratum": label,
                "value": value,
                "avg_rel": metric["avg_rel"],
                "median_rel": metric["median_rel"],
                "win_share": metric["win_share"],
                "n_series": metric["n"],
                "cum_abs": float(frame.groupby("series_key")["cumerr"].apply(lambda s: s.abs().sum()).mean()),
            }
        )

    for (flow, level), frame in merged.groupby(["flow_type", "level"]):
        _row("flow_level", f"{flow}/{level}", frame)
    for label, frame in merged.groupby("sbc"):
        _row("sbc", label, frame)
    for label, frame in merged.groupby("dormant"):
        _row("dormant", "dormant" if label else "active", frame)
    return pd.DataFrame(rows)


def build_leaderboard(
    backtests: dict[str, pd.DataFrame],
    panel: pd.DataFrame,
    config: dict,
    origins,
    horizon: int = 12,
    mase_scales: dict[str, dict] | None = None,
) -> pd.DataFrame:
    """One row per arm over one origin set, with all primary/guardrail metrics."""
    base_bt = backtests[BASE_ARM]
    base_pairs = pairs_for(base_bt, panel, origins, horizon)
    meta = pd.DataFrame(
        [{"series_key": key, **parse_series_key(str(key))} for key in base_pairs["series_key"].unique()]
    )
    money_keys = set(
        meta[(meta["flow_type"] == "expense") & (meta["level"] == "category")]["series_key"]
    )
    expense_keys = set(meta[meta["flow_type"] == "expense"]["series_key"])
    rows = []
    for arm, bt in backtests.items():
        pairs = pairs_for(bt, panel, origins, horizon)
        metric = avg_rel_cum(pairs, base_pairs)
        bias, under_k, n_pairs = pooled_bias(pairs[pairs["series_key"].isin(expense_keys)])
        mase = np.nan
        if mase_scales is not None:
            mase = _median_mase(bt, panel, origins, mase_scales, horizon)
        rows.append(
            {
                "arm": arm,
                "n_pairs": int(len(pairs)),
                "avg_rel_cum12": metric["avg_rel"],
                "median_rel": metric["median_rel"],
                "win_share": metric["win_share"],
                "money_ratio": money_weighted_ratio(pairs, base_pairs, money_keys),
                "phantom_spend": phantom_spend(pairs),
                "pooled_bias": bias,
                "under_k": under_k,
                "n_origin_series": metric["n"],
                "mase_1_12": mase,
                **{f"native_{k}": v for k, v in native_band_scores(bt, horizon).items()},
            }
        )
    frame = pd.DataFrame(rows)
    return frame.sort_values("avg_rel_cum12").reset_index(drop=True)


def _median_mase(
    bt: pd.DataFrame,
    panel: pd.DataFrame,
    origins,
    mase_scales: dict[str, dict],
    horizon: int,
) -> float:
    """Median per-series MASE over the origin set (plan §5)."""
    frame = bt[bt["origin_month"].isin(set(pd.to_datetime(list(origins))))]
    frame = frame[frame["h"] <= horizon]
    values = []
    for (key, origin), group in frame.groupby(["series_key", "origin_month"], sort=False):
        scale = mase_scales.get(str(key), {}).get(pd.Timestamp(origin))
        if scale is None or not np.isfinite(scale) or scale <= 0:
            continue
        actual = group["actual"].to_numpy(dtype=float)
        point = group["point"].to_numpy(dtype=float)
        if not (np.isfinite(actual).all() and np.isfinite(point).all()):
            continue
        values.append(float(np.mean(np.abs(actual - point))) / scale)
    return float(np.median(values)) if values else float("nan")


def mase_scales_by_series(panel: pd.DataFrame, origins, m: int = 12) -> dict[str, dict]:
    """
    ``series_key -> {origin: in-sample MASE scale}`` on the corrected grid.

    Plan §5: the seasonal-naive (m = 12) MAE, falling back to m = 1 when that
    scale is 0, and skipping the pair when both are 0.
    """
    from src.analysis.forecast_metrics import seasonal_naive_scale

    out: dict[str, dict] = {}
    for key, values in panel_to_series(panel).items():
        per_origin = {}
        for origin in pd.to_datetime(list(origins)):
            history = values[values.index <= pd.Timestamp(origin)].to_numpy(dtype=float)
            if len(history) < 13:
                continue
            series = pd.Series(history)
            scale = seasonal_naive_scale(series, m=m)
            if not np.isfinite(scale) or scale <= 0:
                scale = seasonal_naive_scale(series, m=1)
            if np.isfinite(scale) and scale > 0:
                per_origin[pd.Timestamp(origin)] = float(scale)
        out[str(key)] = per_origin
    return out


# ── Phase 0: timing ──────────────────────────────────────────────────────────


def timing_phase(config: dict, out_dir: Path, force: bool = False) -> dict:
    if config["split"].get("step") and not force:
        raise SystemExit(
            "The step is already decided in the config; the timing probe runs once "
            "(plan Phase 0). Use --force only to redo the probe before any accuracy exists."
        )
    panel = load_panel(config)
    split = make_split(panel, config)
    series = panel_to_series(panel)
    keys = [k for k in series if not parse_series_key(k)["is_unassigned"]]
    origins = split["origins"]
    min_months = int(config["split"]["min_months_for_forecast"])
    rng = np.random.default_rng(0)
    all_pairs = [
        (key, origin)
        for key in keys
        for origin in origins
        if len(series[key][series[key].index <= origin]) >= min_months
    ]
    sample_size = int(config["cost"]["probe_pairs"])
    sample = [all_pairs[i] for i in rng.choice(len(all_pairs), min(sample_size, len(all_pairs)), replace=False)]

    probes = {
        "prophet_shipped": build_local_arm("prophet_shipped", {}),
        "auto_ets": build_local_arm("auto_ets", {"window": 60}),
        "tsb": build_local_arm("tsb", {"alpha": 0.1, "beta": 0.1}),
    }
    results = {}
    warmup = 0.0
    for name, arm in probes.items():
        # Warm the optional imports (cmdstanpy, statsmodels) so the probe times
        # measure fits, not first-import cost.
        if sample:
            first_key, first_origin = sample[0]
            warm_start = time.perf_counter()
            try:
                arm(series[first_key][series[first_key].index <= first_origin], 12)
            except Exception:
                pass
            warmup += time.perf_counter() - warm_start
        start = time.perf_counter()
        ok = 0
        for key, origin in sample:
            values = series[key]
            train = values[values.index <= origin]
            if len(train) < 12:
                continue
            try:
                frame = arm(train, 12)
                if frame is not None and np.isfinite(frame["point_estimate"].to_numpy(dtype=float)).all():
                    ok += 1
            except Exception:
                pass
        elapsed = time.perf_counter() - start
        per_fit = elapsed / max(ok, 1)
        results[name] = {"fits": ok, "sample": len(sample), "seconds": elapsed, "seconds_per_fit": per_fit}

    eligible_pairs = len(all_pairs)
    workers = int(config["cost"]["workers"])
    projection = {
        name: results[name]["seconds_per_fit"] * eligible_pairs * 2 / workers
        for name in results
    }
    prophet_total = projection["prophet_shipped"]
    tier12_total = sum(projection.values())
    step = 1 if prophet_total <= float(config["cost"]["prophet_budget_seconds"]) else 2
    drop_p1 = tier12_total > float(config["cost"]["tier12_budget_seconds"])
    decision = {
        "step": step,
        "warmup_seconds": warmup,
        "prophet_projected_seconds": prophet_total,
        "tier12_projected_seconds": tier12_total,
        "drop_prophet_yearly": bool(drop_p1),
        "eligible_pairs": eligible_pairs,
        "workers": workers,
        "probes": results,
        "decided_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    config["split"]["step"] = step
    config["meta"]["timing_decision"] = {k: v for k, v in decision.items() if k != "probes"}
    write_config(config)
    (out_dir / "timing.json").write_text(json.dumps(decision, indent=2, default=float), encoding="utf-8")
    print(json.dumps({k: v for k, v in decision.items() if k != "probes"}, indent=2, default=float))
    return decision


# ── Phase 1: diagnostics ─────────────────────────────────────────────────────


def shipped_start_counts(forecast_path: str = "data/processed/category_forecasts.parquet") -> dict:
    """
    Count production series by their first forecast month.

    The grid bug's real signature: series that stop at their last non-zero
    month are forecast from a date in the past, so their first forecast month
    is not the next complete month. Measured from production's own output.
    """
    path = Path(forecast_path)
    if not path.exists():
        return {
            "shipped_series": 0,
            "shipped_starting_next_month": 0,
            "shipped_starting_early": 0,
            "shipped_first_forecast_month": None,
            "shipped_negative_point_series": None,
            "shipped_ci80_negative_share": None,
        }
    frame = pd.read_parquet(path)
    key = ["flow_type", "level", "category", "subcategory"]
    grouped = frame.groupby(key, dropna=False)["month"].min()
    next_month = pd.Timestamp(frame["month"].max()) - pd.DateOffset(months=35)
    negative = frame[frame["point_estimate"] < 0]
    return {
        "shipped_series": int(len(grouped)),
        "shipped_starting_next_month": int((grouped == next_month).sum()),
        "shipped_starting_early": int((grouped < next_month).sum()),
        "shipped_first_forecast_month": str(next_month.date()),
        "shipped_negative_point_series": int(negative.groupby(key, dropna=False).ngroups),
        "shipped_ci80_negative_share": float((frame["ci80_lower"] < 0).mean()),
    }


def diagnostics_phase(config: dict, out_dir: Path) -> dict:

    panel = load_panel(config)
    split = make_split(panel, config)
    first_test = split["test"][0]
    train = panel[panel["month"] <= first_test]
    months = panel_months(panel)

    rows = []
    for key, group in train.groupby("series_key", sort=False):
        y = group.sort_values("month")["y"].to_numpy(dtype=float)
        active = y[y > 0]
        adi, cv2 = demand_stats(y)
        rows.append(
            {
                "series_key": key,
                **parse_series_key(key),
                "n_months": int(len(y)),
                "zero_share": float((y == 0).mean()),
                "adi": adi,
                "cv2": cv2,
                "sbc": sbc_class(y),
                "first_nonzero": group.sort_values("month")["month"].iloc[int(np.argmax(y > 0))] if active.size else pd.NaT,
                "last_active": group.sort_values("month")["month"].iloc[int(np.flatnonzero(y > 0)[-1])] if active.size else pd.NaT,
                "n_active": int(active.size),
                "mean_monthly": float(y.mean()),
            }
        )
    profiles = pd.DataFrame(rows)
    dormant_cut = first_test - pd.DateOffset(months=11)
    profiles["dormant"] = pd.to_datetime(profiles["last_active"]) < dormant_cut
    full_history = panel.groupby("series_key")["month"].nunique()
    profiles["n_months_full"] = profiles["series_key"].map(full_history)
    profiles_private = out_dir / "series_profile.csv"
    profiles.to_csv(profiles_private, index=False)

    # STL seasonal strength on qualifying series.
    from statsmodels.tsa.seasonal import STL

    strengths = []
    for key, group in train.groupby("series_key", sort=False):
        y = group.sort_values("month")["y"].to_numpy(dtype=float)
        if len(y) < 36 or (y == 0).mean() >= 0.30:
            continue
        try:
            stl = STL(pd.Series(y), period=12, robust=True).fit()
            var_resid = float(np.var(stl.resid))
            var_total = float(np.var(y))
            strengths.append(1.0 - var_resid / var_total if var_total > 0 else 0.0)
        except Exception:
            continue

    scored = profiles[~profiles["is_unassigned"]]
    hierarchy = build_hierarchy(panel)
    unassigned_ok = bool((panel[panel["is_unassigned"]]["y"] >= 0).all())

    summary = {
        "as_of": str(months[-1].date()),
        "first_month": str(months[0].date()),
        "n_months": int(len(months)),
        "diagnostics_window_end": str(first_test.date()),
        "n_series_total": int(profiles["series_key"].nunique()),
        "n_series_scored": int(scored["series_key"].nunique()),
        "n_series_unassigned": int(profiles["is_unassigned"].sum()),
        "flow_level_counts": {f"{f}/{lvl}": int(v) for (f, lvl), v in scored.groupby(["flow_type", "level"]).size().items()},
        "class_counts": scored["sbc"].value_counts().to_dict(),
        "class_counts_all": profiles["sbc"].value_counts().to_dict(),
        "zero_share": {
            "median": float(scored["zero_share"].median()),
            "q25": float(scored["zero_share"].quantile(0.25)),
            "q75": float(scored["zero_share"].quantile(0.75)),
            "share_over_30pct": float((scored["zero_share"] > 0.30).mean()),
        },
        "dormant": int(scored["dormant"].sum()),
        "dormant_by_flow_level": {f"{f}/{lvl}": int(v) for (f, lvl), v in scored[scored["dormant"]].groupby(["flow_type", "level"]).size().items()},
        # Production's own grid bug, measured from its shipped output rather
        # than recomputed: how many of the shipped series start at the next
        # month versus earlier. This is the plan's claim about the grid, checked
        # against production's output instead of re-derived.
        **shipped_start_counts(),
        "stl_seasonal_strength": {
            "n": len(strengths),
            "median": float(np.median(strengths)) if strengths else float("nan"),
        },
        "unassigned_non_negative": unassigned_ok,
        "hierarchy_flows": list(hierarchy),
        "adi_cutoff": ADI_CUTOFF,
        "cv2_cutoff": CV2_CUTOFF,
        "sbc_classes": list(SBC_CLASSES),
        "n_origins": len(split["origins"]),
        "n_dev": len(split["dev"]),
        "n_embargo": len(split["embargo"]),
        "n_test": len(split["test"]),
        "first_test_origin": str(split["test"][0].date()),
    }
    (out_dir / "diagnostics_profile.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    fig_dir = out_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    _diagnostic_figures(scored, split, fig_dir)
    (out_dir / "diagnostics.md").write_text(_diagnostics_markdown(summary), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("n_series_scored", "class_counts", "dormant", "zero_share")}, indent=2, default=str))
    return summary


def _diagnostic_figures(scored: pd.DataFrame, split: dict, fig_dir: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4.4))
    ax.hist(scored["zero_share"], bins=20, color="C0")
    ax.set_xlabel("zero share of months")
    ax.set_ylabel("series")
    ax.set_title("Zero share by series (pre-test window)")
    fig.tight_layout()
    fig.savefig(fig_dir / "zero_share.png", dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4.4))
    counts = scored["sbc"].value_counts().reindex(["smooth", "erratic", "intermittent", "lumpy"]).fillna(0)
    ax.bar(counts.index, counts.to_numpy(), color="C2")
    ax.set_ylabel("series")
    ax.set_title("SBC demand class")
    fig.tight_layout()
    fig.savefig(fig_dir / "sbc_classes.png", dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4.6))
    ax.scatter(scored["adi"].clip(upper=20), scored["cv2"].clip(upper=10), s=12, alpha=0.6)
    ax.axvline(ADI_CUTOFF, color="0.3", lw=1, ls="--")
    ax.axhline(CV2_CUTOFF, color="0.3", lw=1, ls="--")
    ax.set_xlabel("ADI (clipped at 20)")
    ax.set_ylabel("CV^2 (clipped at 10)")
    ax.set_title("SBC class boundaries")
    fig.tight_layout()
    fig.savefig(fig_dir / "adi_cv2.png", dpi=130)
    plt.close(fig)


def _diagnostics_markdown(summary: dict) -> str:
    lines = [
        "# Category forecast bake-off - diagnostics",
        "",
        f"Data through {summary['as_of']}; diagnostics window ends {summary['diagnostics_window_end']}.",
        "",
        "All figures below are generated from the ledger and live only in this git-ignored directory.",
        "",
        "## Panel",
        "",
        f"- Months: {summary['n_months']} ({summary['first_month']} to {summary['as_of']}).",
        f"- Series: {summary['n_series_total']} total, {summary['n_series_scored']} scored, "
        f"{summary['n_series_unassigned']} unassigned nodes.",
        f"- Flow x level (scored): {summary['flow_level_counts']}.",
        "",
        "## Demand classes",
        "",
        f"- Scored series: {summary['class_counts']} (ADI cutoff {summary['adi_cutoff']}, "
        f"CV^2 cutoff {summary['cv2_cutoff']}).",
        "",
        "## Zeros and dormancy",
        "",
        f"- Zero share: median {summary['zero_share']['median']:.2f} "
        f"(q25 {summary['zero_share']['q25']:.2f}, q75 {summary['zero_share']['q75']:.2f}); "
        f"{summary['zero_share']['share_over_30pct']*100:.0f}% of series above 30%.",
        f"- Dormant (no activity in the last 12 months of the window): {summary['dormant']} "
        f"({summary['dormant_by_flow_level']}).",
        f"- Shipped production forecasts: {summary['shipped_series']} series, "
        f"{summary['shipped_starting_next_month']} start at "
        f"{summary['shipped_first_forecast_month']} and "
        f"{summary['shipped_starting_early']} start earlier (the grid bug).",
        "",
        "## Seasonality",
        "",
        f"- STL seasonal strength on qualifying series (>= 36 months, < 30% zeros): "
        f"n={summary['stl_seasonal_strength']['n']}, median "
        f"{summary['stl_seasonal_strength']['median']:.2f}.",
        "",
        "## Hierarchy",
        "",
        f"- flows with a hierarchy: {summary['hierarchy_flows']}; unassigned nodes non-negative: "
        f"{summary['unassigned_non_negative']}.",
        "",
        "## Split",
        "",
        f"- Origins {summary['n_origins']}: dev {summary['n_dev']}, embargo {summary['n_embargo']}, "
        f"test {summary['n_test']}; first test origin {summary['first_test_origin']}.",
        "",
        "Figures: `figures/zero_share.png`, `figures/sbc_classes.png`, `figures/adi_cv2.png`.",
    ]
    return "\n".join(lines) + "\n"


# ── Phase 3: dev ─────────────────────────────────────────────────────────────


def _arm_specs(config: dict) -> dict[str, list[dict]]:
    specs = {}
    for arm_id, arm_cfg in config["arms"].items():
        variants = arm_cfg.get("variants") or [{}]
        specs[arm_id] = [{"arm_id": arm_id, "params": dict(variant)} for variant in variants]
    return specs


def _eval_specs(
    specs: list[dict],
    panel: pd.DataFrame,
    config: dict,
    out_dir: Path,
    origins,
    phase: str,
    horizon: int,
    context: dict | None = None,
    force: bool = False,
) -> tuple[list[dict], dict[str, pd.DataFrame]]:
    rows = []
    caches: dict[str, pd.DataFrame] = {}
    base_bt, _ = run_arm(
        {"arm_id": BASE_ARM, "params": {}}, panel, config, out_dir, phase, horizon, origins, force=force
    )
    base_pairs = pairs_for(base_bt, panel, origins, horizon)
    for spec in specs:
        bt, stats = run_arm(
            spec, panel, config, out_dir, phase, horizon, origins, context=context, force=force
        )
        pairs = pairs_for(bt, panel, origins, horizon)
        metric = avg_rel_cum(pairs, base_pairs)
        eligible_pairs = _eligible_pair_count(panel, origins, int(config["split"]["min_months_for_forecast"]))
        rows.append(
            {
                "arm": spec["arm_id"],
                "params": json.dumps(spec.get("params", {}), sort_keys=True),
                "avg_rel_cum12": metric["avg_rel"],
                "median_rel": metric["median_rel"],
                "n_series": metric["n"],
                "cum_abs_mean": float(
                    pairs.groupby("series_key")["cumerr"].apply(lambda s: s.abs().sum()).mean()
                )
                if not pairs.empty
                else np.nan,
                "seconds": stats.get("seconds", np.nan),
                "fallbacks": stats.get("fallbacks", 0),
                "fallback_share": float(stats.get("fallback_pairs", 0)) / max(eligible_pairs, 1),
                "fallback_pairs": stats.get("fallback_pairs", 0),
                "eligible_pairs": eligible_pairs,
                "identical_share": identical_share(bt, base_bt),
                "negative_values": stats.get("negative_values", 0),
            }
        )
        caches[spec_slug(spec)] = bt
    return rows, caches


def _eligible_pair_count(panel: pd.DataFrame, origins, min_months: int = 12) -> int:
    """
    (series, origin) pairs any eligible series could have been forecast at.

    Summed over origins of the series with >= 12 months of history there, on
    the corrected grid. This is the denominator of the fallback share.
    """
    if not origins:
        return 0
    series = panel_to_series(panel)
    total = 0
    for values in series.values():
        for origin in pd.to_datetime(list(origins)):
            if len(values[values.index <= origin]) >= int(min_months):
                total += 1
    return total


def _fallback_share(bt: pd.DataFrame, eligible_pairs: int) -> float:
    """
    Share of eligible (series, origin) pairs an arm fell back on.

    Raises when the cached backtest predates the ``fallback`` column: a gate
    that cannot see fallbacks must not silently pass.
    """
    if bt is None or bt.empty or eligible_pairs <= 0:
        return float("nan")
    if "fallback" not in bt.columns:
        raise RuntimeError(
            "cached backtest has no `fallback` column; delete the stale cache and re-run"
        )
    flagged = bt[bt["fallback"].fillna(False).astype(bool)]
    pairs = int(len(flagged.groupby(["series_key", "origin_month"], sort=False)))
    return float(pairs / eligible_pairs)


def identical_share(bt: pd.DataFrame, base_bt: pd.DataFrame, tolerance: float = 1e-9) -> float:
    """
    Share of (series, origin, h) rows where an arm equals ``naive_mean_12``.

    Sanity gate: an arm identical to the benchmark everywhere is a no-op, not a
    result. Compared on the same rows only.
    """
    if bt is None or bt.empty:
        return float("nan")
    left = bt[["series_key", "origin_month", "h", "point"]]
    right = base_bt[["series_key", "origin_month", "h", "point"]].rename(
        columns={"point": "base_point"}
    )
    merged = left.merge(right, on=["series_key", "origin_month", "h"], how="inner")
    if merged.empty:
        return float("nan")
    same = (merged["point"] - merged["base_point"]).abs() <= tolerance
    return float(same.mean())


def sanity_gate(leaderboard: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Block the freeze when an arm is a no-op or falls back too often.

    Plan guardrail: an arm identical to the benchmark (except B1w with window
    12) is a no-op; more than 5% fallbacks means the arm is mostly the
    benchmark. Both must be explained before the freeze.
    """
    limit = float(config["decision"].get("fallback_share_max", 0.05))
    rows = []
    for _, row in leaderboard.iterrows():
        arm = str(row["arm"])
        params = json.loads(row.get("params", "{}") or "{}")
        # The benchmark and its window-12 variant are identical to B1 by
        # construction; neither is a no-op arm.
        by_design = arm == BASE_ARM or (
            arm == "naive_mean_w" and int(params.get("window", 0) or 0) == 12
        )
        identical = float(row.get("identical_share", float("nan")))
        fallback = float(row.get("fallback_share", float("nan")))
        failures = []
        if not by_design and np.isfinite(identical) and identical >= 1.0:
            failures.append("identical to naive_mean_12")
        if np.isfinite(fallback) and fallback > limit:
            failures.append(f"fallbacks {fallback:.1%} > {limit:.0%}")
        rows.append(
            {
                "arm": arm,
                "params": json.dumps(params, sort_keys=True),
                "identical_share": identical,
                "fallback_share": fallback,
                "status": "ok" if not failures else "BLOCKED",
                "reason": "; ".join(failures) if failures else ("benchmark by construction" if by_design else ""),
            }
        )
    return pd.DataFrame(rows)


def _select_variant(rows: list[dict]) -> dict | None:
    valid = [row for row in rows if np.isfinite(row["avg_rel_cum12"])]
    if not valid:
        return None
    return min(valid, key=lambda row: row["avg_rel_cum12"])


def dev_phase(config: dict, out_dir: Path, force: bool = False) -> dict:
    panel = load_panel(config)
    split = make_split(panel, config)
    if not split["dev"]:
        raise SystemExit("No dev origins; check the split.")
    print(
        f"panel series={panel['series_key'].nunique()} months={len(split['months'])} | "
        f"dev={len(split['dev'])} test={len(split['test'])} embargo={len(split['embargo'])}"
    )

    specs = _arm_specs(config)
    # P1 is dropped when the timing probe says the Tier 1-2 budget is blown.
    timing = config["meta"].get("timing_decision") or {}
    if timing.get("drop_prophet_yearly"):
        specs.pop("prophet_yearly", None)
        print("prophet_yearly dropped by the Phase 0 timing decision")

    summary_rows: list[dict] = []
    selection: dict[str, dict] = {}
    dev_backtests: dict[str, pd.DataFrame] = {}
    # RC needs the ledger (and only the ledger): without it the arm raised and
    # fell back for every origin, which is what made it a no-op.
    dev_context = {"transactions": pd.read_parquet(config["data"]["featured"])}
    for arm_id, arm_specs in specs.items():
        if arm_id in ("class_selector", "series_selector", "reconciled"):
            continue
        rows, caches = _eval_specs(
            arm_specs,
            panel,
            config,
            out_dir,
            split["dev"],
            "dev",
            12,
            context=dev_context,
            force=force,
        )
        summary_rows.extend(rows)
        winner = _select_variant(rows)
        if winner is None:
            print(f"  {arm_id:20s} failed on every dev origin")
            continue
        selection[arm_id] = {"arm_id": arm_id, "params": json.loads(winner["params"])}
        chosen_slug = spec_slug(selection[arm_id])
        if chosen_slug in caches:
            dev_backtests[arm_id] = caches[chosen_slug]
        print(f"  {arm_id:20s} dev AvgRelCum12={winner['avg_rel_cum12']:.3f} params={winner['params']}")

    pd.DataFrame(summary_rows).to_csv(out_dir / "dev_candidates.csv", index=False)

    # S1: class -> best arm, chosen on dev by that class's AvgRelCum12.
    # Plan §4.3: the map is drawn from {B1w, B2, C1, C2, I1, I2, E1} only.
    selector_pool = (
        "naive_mean_w",
        "seasonal_naive",
        "auto_ets",
        "theta",
        "croston",
        "tsb",
        "combo",
    )
    origin = max(split["dev"])
    classes = classes_at(panel, origin)
    class_map: dict[str, dict] = {}
    class_rows = []
    eligible = {arm: spec for arm, spec in selection.items() if arm in selector_pool}
    for label in SBC_CLASSES:
        keys = {key for key, value in classes.items() if value == label}
        best = None
        for arm, spec in eligible.items():
            bt = dev_backtests.get(arm)
            if bt is None:
                continue
            pairs = pairs_for(bt, panel, split["dev"], 12)
            pairs = pairs[pairs["series_key"].isin(keys)]
            base_pairs = pairs_for(dev_backtests[BASE_ARM], panel, split["dev"], 12)
            base_pairs = base_pairs[base_pairs["series_key"].isin(keys)]
            metric = avg_rel_cum(pairs, base_pairs)
            class_rows.append({"sbc": label, "arm": arm, "avg_rel_cum12": metric["avg_rel"], "n": metric["n"]})
            if np.isfinite(metric["avg_rel"]) and (best is None or metric["avg_rel"] < best[1]):
                best = (arm, metric["avg_rel"])
        if best is not None:
            class_map[label] = selection[best[0]]
    if class_map:
        fallback = selection.get(BASE_ARM, {"arm_id": BASE_ARM, "params": {}})
        for label in SBC_CLASSES:
            class_map.setdefault(label, fallback)
        spec = {"arm_id": "class_selector", "params": {"class_map": class_map, "default": fallback}}
        bt, stats = run_arm(spec, panel, config, out_dir, "dev", 12, split["dev"], force=force)
        dev_backtests["class_selector"] = bt
        pairs = pairs_for(bt, panel, split["dev"], 12)
        base_pairs = pairs_for(dev_backtests[BASE_ARM], panel, split["dev"], 12)
        metric = avg_rel_cum(pairs, base_pairs)
        selection["class_selector"] = {"arm_id": "class_selector", "params": {"class_map": class_map, "default": fallback}}
        print(f"  class_selector       dev AvgRelCum12={metric['avg_rel']:.3f}")
    pd.DataFrame(class_rows).to_csv(out_dir / "dev_class_choices.csv", index=False)

    # S2: each series takes the arm with the lowest mean cum-12 error on dev,
    # from the same set S1 draws on (plan §4.3).
    member_arms = [arm for arm in selector_pool if arm in dev_backtests]
    per_series_rows = []
    base_pairs = pairs_for(dev_backtests[BASE_ARM], panel, split["dev"], 12)
    for arm in member_arms:
        bt = dev_backtests.get(arm)
        if bt is None:
            continue
        pairs = pairs_for(bt, panel, split["dev"], 12)
        means = pairs.groupby("series_key")["cumerr"].apply(lambda s: s.abs().mean())
        for key, value in means.items():
            per_series_rows.append({"series_key": key, "arm": arm, "mean_abs_cum_err": float(value)})
    series_table = pd.DataFrame(per_series_rows)
    if not series_table.empty:
        series_table.to_csv(out_dir / "dev_series_arms.csv", index=False)
        chosen = (
            series_table.sort_values(["series_key", "mean_abs_cum_err", "arm"])
            .groupby("series_key", as_index=False)
            .first()
        )
        series_map = {
            row["series_key"]: selection[row["arm"]]
            for _, row in chosen.iterrows()
            if row["arm"] in selection
        }
        fallback = selection.get("class_selector", selection.get(BASE_ARM))
        series_spec = {"arm_id": "series_selector", "params": {"series_map": series_map, "fallback": fallback}}
        series_path = out_dir / "series_selection.json"
        series_path.write_text(json.dumps(series_spec, indent=2, sort_keys=True), encoding="utf-8")
        digest = hashlib.sha256(series_path.read_bytes()).hexdigest()
        config["meta"]["s2_table_sha256"] = digest
        selection["series_selector"] = series_spec
        bt, stats = run_arm(series_spec, panel, config, out_dir, "dev", 12, split["dev"], force=force)
        dev_backtests["series_selector"] = bt
        pairs = pairs_for(bt, panel, split["dev"], 12)
        metric = avg_rel_cum(pairs, base_pairs)
        print(f"  series_selector      dev AvgRelCum12={metric['avg_rel']:.3f} (sha256 {digest[:12]})")

    # H: reconcile the two best base arms on dev.
    ranked = sorted(
        [(arm, metric) for arm, metric in _dev_metrics(dev_backtests, panel, split["dev"])],
        key=lambda item: item[1],
    )
    base_candidates = [arm for arm, _ in ranked if arm not in ("class_selector", "series_selector", "reconciled")][:2]
    hierarchy = build_hierarchy(panel)
    reconciled_rows = []
    if base_candidates:
        residual_bt = dev_backtests.get(base_candidates[0])
        residual_frame = residual_bt.copy() if residual_bt is not None else pd.DataFrame()
        if not residual_frame.empty:
            residual_frame = cumulative_columns(residual_frame)
            residual_frame["err"] = residual_frame["actual"] - residual_frame["point"]
        for base in base_candidates:
            for method in ("none", "bottom_up", "wls", "mint"):
                spec = {
                    "arm_id": "reconciled",
                    "params": {"base": selection[base], "method": method},
                }
                context = {"residual_frame": residual_frame, "hierarchy": hierarchy}
                bt, stats = run_arm(spec, panel, config, out_dir, "dev", 12, split["dev"], context=context, force=force)
                pairs = pairs_for(bt, panel, split["dev"], 12)
                metric = avg_rel_cum(pairs, base_pairs)
                reconciled_rows.append(
                    {
                        "arm": f"reconciled[{base},{method}]",
                        "base": base,
                        "method": method,
                        "avg_rel_cum12": metric["avg_rel"],
                    }
                )
                if method != "none":
                    dev_backtests[f"reconciled[{base},{method}]"] = bt
        reconciled_frame = pd.DataFrame(reconciled_rows)
        reconciled_frame.to_csv(out_dir / "dev_reconciled.csv", index=False)
        if not reconciled_frame.empty:
            valid = reconciled_frame[np.isfinite(reconciled_frame["avg_rel_cum12"])]
            top = valid.sort_values("avg_rel_cum12").iloc[0]
            base_arm = str(top["base"])
            base_dev = float(
                valid.loc[valid["base"] == base_arm, "avg_rel_cum12"].min()
            )
            selected_base = selection.get(base_arm, {})
            best_method = str(top["method"])
            # Method ``none`` is the base arm unchanged, so freezing it would
            # enter a duplicate of that arm with a +0.5 complexity penalty.
            # If no real reconciliation beats the base on dev, H is dropped.
            if best_method == "none" or float(top["avg_rel_cum12"]) >= base_dev:
                config["arms"]["reconciled"]["variants"] = []
                selection.pop("reconciled", None)
                print(
                    f"  reconciled           dropped: best option ({best_method}) does not beat "
                    f"{base_arm} on dev ({float(top['avg_rel_cum12']):.3f} vs {base_dev:.3f})"
                )
            else:
                config["arms"]["reconciled"]["variants"] = [
                    {"base": selected_base, "method": best_method}
                ]
                print(
                    f"  reconciled           dev AvgRelCum12={top['avg_rel_cum12']:.3f} "
                    f"base={base_arm} method={best_method}"
                )

    # Sanity gate over every frozen candidate (plan guardrail, post-review).
    gate_rows = []
    base_bt = dev_backtests[BASE_ARM]
    eligible_pairs = _eligible_pair_count(
        panel, split["dev"], int(config["split"]["min_months_for_forecast"])
    )
    for arm, spec in selection.items():
        bt = dev_backtests.get(arm)
        if bt is None:
            try:
                bt, _ = run_arm(
                    spec, panel, config, out_dir, "dev", 12, split["dev"], context=dev_context
                )
            except Exception:
                bt = None
        if bt is None:
            gate_rows.append(
                {
                    "arm": arm,
                    "params": json.dumps(spec.get("params", {}), sort_keys=True),
                    "identical_share": float("nan"),
                    "fallback_share": float("nan"),
                    "status": "BLOCKED",
                    "reason": "arm produced no dev backtest",
                }
            )
            continue
        rows = [{"arm": arm, "params": json.dumps(spec.get("params", {}), sort_keys=True),
                 "identical_share": identical_share(bt, base_bt),
                 "fallback_share": _fallback_share(bt, eligible_pairs)}]
        gate_rows.extend(rows)
    gate = sanity_gate(pd.DataFrame(gate_rows), config)
    gate.to_csv(out_dir / "sanity_gate.csv", index=False)
    blocked = gate[gate["status"] == "BLOCKED"]
    print(f"  sanity gate: {len(gate)} arms, {len(blocked)} blocked")

    payload = {
        "dev_origins": [str(o.date()) for o in split["dev"]],
        "test_origins": [str(o.date()) for o in split["test"]],
        "embargo_origins": [str(o.date()) for o in split["embargo"]],
        "selection": selection,
        "config_sha256": config_sha256(),
        "s2_table_sha256": config["meta"].get("s2_table_sha256"),
    }
    (out_dir / "dev_selection.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return payload


def _dev_metrics(backtests: dict[str, pd.DataFrame], panel: pd.DataFrame, origins) -> list[tuple[str, float]]:
    base_pairs = pairs_for(backtests[BASE_ARM], panel, origins, 12)
    out = []
    for arm, bt in backtests.items():
        pairs = pairs_for(bt, panel, origins, 12)
        out.append((arm, avg_rel_cum(pairs, base_pairs)["avg_rel"]))
    return out


# ── Freeze ───────────────────────────────────────────────────────────────────


def _dev_leaderboard(
    panel: pd.DataFrame,
    config: dict,
    out_dir: Path,
    selection: dict,
    dev_origins,
    horizon: int,
) -> pd.DataFrame:
    """
    Dev leaderboard from the cached dev backtests.

    The test phase only produces test-origin rows, so the dev comparison must
    come from the dev caches written by ``--phase dev``.
    """
    dev_bts: dict[str, pd.DataFrame] = {}
    for arm, spec in selection.items():
        try:
            path = backtest_path(out_dir, "dev", spec, horizon)
        except (KeyError, OSError):
            continue
        if path.exists():
            dev_bts[arm] = pd.read_parquet(path)
    if BASE_ARM not in dev_bts:
        raise SystemExit(
            "No dev backtest cache for the benchmark; re-run --phase dev before --phase test."
        )
    return build_leaderboard(dev_bts, panel, config, dev_origins, horizon)


def _pooled_on_pairs(
    pooled_frame: pd.DataFrame, native_scores: dict, horizon: int, config: dict
) -> dict:
    """
    Pooled band scores over the pairs where a native band exists.

    The native interval is only emitted for some pairs, so comparing the two
    candidates over different populations would reward whichever covers fewer
    series. ``native_scores`` supplies those pairs; both are scored on the
    intersection.
    """
    from src.analysis.forecast_metrics import band_scores as _band_scores

    empty = {"monthly_coverage": float("nan"), "cum12_coverage": float("nan"), "n_pairs": 0}
    if pooled_frame is None or pooled_frame.empty:
        return empty
    keys = native_scores.get("pairs")
    if not keys:
        return empty
    mask = pooled_frame.apply(
        lambda row: (row["series_key"], pd.Timestamp(row["origin_month"])) in keys, axis=1
    )
    subset = pooled_frame[mask]
    if subset.empty:
        return empty
    return _band_scores(subset, alpha=float(config["bands"]["alpha"]), horizon=horizon)


def _native_coverage_all_pairs(bt: pd.DataFrame, horizon: int = 12) -> dict:
    """
    Native coverage over every pair, counting a missing band as a miss.

    This is what a user would experience: a pair with no interval cannot be
    covered by it.
    """
    if bt is None or bt.empty:
        return {"monthly_coverage": float("nan"), "cum12_coverage": float("nan"), "n_pairs": 0}
    frame = bt[bt["h"] <= int(horizon)]
    inside = (frame["actual"] >= frame["lo80"]) & (frame["actual"] <= frame["hi80"])
    inside = inside.fillna(False)
    cum = []
    for _, group in frame.groupby(["series_key", "origin_month"], sort=False):
        group = group.sort_values("h")
        if len(group) < int(horizon):
            continue
        if group[["lo80", "hi80"]].isna().any().any():
            cum.append(False)
            continue
        actual = group["actual"].cumsum()
        cum.append(
            bool((actual.iloc[-1] >= group["lo80"].cumsum().iloc[-1])
                 and (actual.iloc[-1] <= group["hi80"].cumsum().iloc[-1]))
        )
    return {
        "monthly_coverage": float(inside.mean()),
        "cum12_coverage": float(np.mean(cum)) if cum else float("nan"),
        "n_pairs": len(cum),
    }


def resolve_selection(config: dict, out_dir: Path) -> dict:
    """
    Expand the frozen selection into runnable specs.

    S2's table is stored privately and referenced by digest, so it is loaded
    from disk here and the digest verified: a mismatch means the config and the
    table are from different runs.
    """
    selection = dict(config.get("selection") or {})
    spec = selection.get("series_selector")
    if spec is not None:
        params = spec.get("params", {})
        table_path = Path(params.get("table", out_dir / "series_selection.json"))
        expected = params.get("sha256") or config["meta"].get("s2_table_sha256")
        if not table_path.exists():
            raise SystemExit(f"Frozen S2 table is missing: {table_path}")
        digest = hashlib.sha256(table_path.read_bytes()).hexdigest()
        if expected and digest != expected:
            raise SystemExit(
                "Frozen S2 table does not match its recorded digest; refusing to run "
                f"({digest[:12]} != {str(expected)[:12]})"
            )
        selection["series_selector"] = json.loads(table_path.read_text(encoding="utf-8"))
    return selection


def freeze(config: dict, out_dir: Path) -> None:
    selection_path = out_dir / "dev_selection.json"
    if not selection_path.exists():
        raise SystemExit("No dev_selection.json: run --phase dev first.")
    payload = json.loads(selection_path.read_text(encoding="utf-8"))
    selection = payload["selection"]

    # Sanity gate: block the freeze on a no-op arm or excessive fallbacks
    # (plan guardrail, post-review). The table is written either way.
    gate_path = out_dir / "sanity_gate.csv"
    if not gate_path.exists():
        raise SystemExit(
            "No sanity_gate.csv: re-run --phase dev so the gate is computed before freezing."
        )
    gate = pd.read_csv(gate_path)
    blocked = gate[gate["status"] == "BLOCKED"]
    if not blocked.empty:
        detail = "; ".join(
            f"{row['arm']} ({row['reason']})" for _, row in blocked.iterrows()
        )
        raise SystemExit(f"Refusing to freeze: sanity gate blocked -> {detail}")

    # The repo is public. S2's table is keyed by real category names, so only
    # its digest may enter the tracked config; the table itself stays in the
    # git-ignored experiment directory.
    tracked = {}
    for arm, spec in selection.items():
        if arm == "series_selector":
            tracked[arm] = {
                "arm_id": "series_selector",
                "params": {
                    "table": str((out_dir / "series_selection.json").as_posix()),
                    "sha256": payload.get("s2_table_sha256"),
                },
            }
        else:
            tracked[arm] = spec

    config["selection"] = tracked
    config["meta"]["frozen"] = True
    config["meta"]["frozen_at"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    config["meta"]["s2_table_sha256"] = payload.get("s2_table_sha256")
    write_config(config)
    digest = config_sha256()
    (out_dir / "frozen_config.sha256").write_text(digest + "\n", encoding="utf-8")
    print(f"Frozen. config sha256 = {digest}")
    print("Commit configs/category_forecast_experiment.yaml before --phase test.")


# ── Phase 4: test ────────────────────────────────────────────────────────────


def _complexity(arm_id: str, config: dict) -> float:
    order = config["decision"]["complexity_order"]
    plain = arm_id.split("[", 1)[0]
    for index, group in enumerate(order):
        if plain in group:
            return float(index) + (0.5 if "[" in arm_id else 0.0)
    return 99.0


def apply_decision_rule(leaderboard: pd.DataFrame, strata: pd.DataFrame, config: dict) -> dict:
    """Apply plan §7 mechanically; return the verdict with the deciding step."""
    decision = config["decision"]
    threshold = float(decision["relcum12_max"])
    rows = leaderboard.set_index("arm")
    base = rows.loc[BASE_ARM]
    # B0 is diagnostic only (plan §4.2) and never eligible.
    diagnostic_only = {"zero"}

    def _eligible(arm: str) -> bool:
        row = rows.loc[arm]
        return (
            arm not in diagnostic_only
            and np.isfinite(row["avg_rel_cum12"])
            and row["avg_rel_cum12"] < threshold
            and np.isfinite(row["dev_avg_rel_cum12"])
            and row["dev_avg_rel_cum12"] < threshold
            and row["money_ratio"] <= 1.0
        )

    eligible = [arm for arm in rows.index if _eligible(arm)]
    trail: list[dict] = []
    if not eligible:
        return {
            "step": "§7.1 eligibility",
            "chosen": BASE_ARM,
            "reason": "No arm beat the naive mean on both periods with a money-weighted ratio <= 1; naive wins.",
            "trail": trail,
            "last_step": "§7.1 eligibility",
        }

    ordered = sorted(eligible, key=lambda arm: rows.loc[arm, "avg_rel_cum12"])
    candidate = ordered[0]
    trail.append(
        {"step": "§7.2 candidate", "arm": candidate, "test_avg_rel_cum12": float(rows.loc[candidate, "avg_rel_cum12"])}
    )

    margin = float(decision["parsimony_margin"])
    changed = True
    while changed:
        changed = False
        for arm in sorted(eligible, key=lambda a: _complexity(a, config)):
            if _complexity(arm, config) >= _complexity(candidate, config):
                continue
            if rows.loc[arm, "avg_rel_cum12"] <= rows.loc[candidate, "avg_rel_cum12"] * (1 + margin):
                trail.append(
                    {
                        "step": "§7.3 parsimony",
                        "from": candidate,
                        "to": arm,
                        "margin_used": float(
                            rows.loc[arm, "avg_rel_cum12"] / rows.loc[candidate, "avg_rel_cum12"] - 1
                        ),
                    }
                )
                candidate = arm
                changed = True
                break
    last_step = "§7.3 parsimony" if len(trail) > 1 else "§7.2 candidate"

    stratum_limit = float(decision["stratum_limit"])
    phantom_limit = float(base["phantom_spend"]) + float(decision["phantom_margin"])
    bias_limit = abs(float(base["pooled_bias"])) + float(decision["bias_margin"])
    start = ordered.index(candidate)
    for arm in ordered[start:]:
        failures = []
        flow_level = strata[(strata["stratum"] == "flow_level") & (strata["arm"] == arm)]
        worst = flow_level["avg_rel"].max() if not flow_level.empty else np.nan
        if np.isfinite(worst) and worst > stratum_limit:
            failures.append(f"(a) flow x level stratum {worst:.3f} > {stratum_limit:.2f}")
        if np.isfinite(rows.loc[arm, "phantom_spend"]) and rows.loc[arm, "phantom_spend"] > phantom_limit:
            failures.append(
                f"(b) phantom spend {rows.loc[arm, 'phantom_spend']:.4f} > {phantom_limit:.4f}"
            )
        if np.isfinite(rows.loc[arm, "pooled_bias"]) and abs(rows.loc[arm, "pooled_bias"]) > bias_limit:
            failures.append(
                f"(c) |pooled bias| {abs(rows.loc[arm, 'pooled_bias']):.4f} > {bias_limit:.4f}"
            )
        if not failures:
            if arm != candidate:
                trail.append({"step": "§7.4 guardrails", "rejected": candidate, "moved_to": arm})
                last_step = "§7.4 guardrails"
            return {
                "step": last_step,
                "chosen": arm,
                "reason": "All guardrails pass." if arm == candidate else "; ".join(failures) or "moved",
                "trail": trail,
                "last_step": last_step,
            }
        trail.append({"step": "§7.4 guardrails", "rejected": arm, "failures": failures})

    return {
        "step": "§7.1 eligibility",
        "chosen": BASE_ARM,
        "reason": "Every eligible arm failed a guardrail; the naive mean is the fallback.",
        "trail": trail,
        "last_step": "§7.4 guardrails",
    }


def _write_test_fallbacks(
    backtests: dict[str, dict[int, pd.DataFrame]],
    panel: pd.DataFrame,
    out_dir: Path,
    test_origins,
    horizon: int,
    config: dict,
) -> None:
    """Per-arm test fallback pairs as a share of eligible pairs and of rows."""
    eligible = _eligible_pair_count(
        panel, test_origins, int(config["split"]["min_months_for_forecast"])
    )
    rows = []
    for arm, by_horizon in backtests.items():
        bt = by_horizon.get(horizon)
        if bt is None or bt.empty or "fallback" not in bt.columns:
            continue
        frame = bt[bt["origin_month"].isin(set(pd.to_datetime(list(test_origins))))]
        flagged = frame[frame["fallback"].fillna(False).astype(bool)]
        pairs = int(len(flagged.groupby(["series_key", "origin_month"], sort=False)))
        rows.append(
            {
                "arm": arm,
                "fallback_pairs": pairs,
                "eligible_pairs": eligible,
                "fallback_share_pairs": pairs / max(eligible, 1),
                "fallback_share_rows": pairs * int(horizon) / max(len(frame), 1),
            }
        )
    pd.DataFrame(rows).to_csv(out_dir / "test_fallbacks.csv", index=False)


def test_phase(config: dict, out_dir: Path, force: bool = False) -> dict:
    require_frozen(config)
    panel = load_panel(config)
    split = make_split(panel, config)
    selection = resolve_selection(config, out_dir)
    if not selection:
        raise SystemExit("Frozen config has an empty `selection`; refusing to run.")

    hierarchy = build_hierarchy(panel)
    transactions = pd.read_parquet(config["data"]["featured"])
    horizons = (int(config["split"]["horizon_primary"]), int(config["split"]["horizon_secondary"]))

    backtests: dict[str, dict[int, pd.DataFrame]] = {}
    cost_rows = []
    for arm, spec in selection.items():
        backtests[arm] = {}
        for horizon in horizons:
            if arm.startswith("reconciled"):
                base_arm = spec["params"]["base"]["arm_id"]
                base_bt = backtests.get(base_arm, {}).get(horizon)
                residual_frame = cumulative_columns(base_bt) if base_bt is not None else None
                context = {"residual_frame": residual_frame, "hierarchy": hierarchy}
            else:
                context = {"transactions": transactions}
            bt, stats = run_arm(
                spec, panel, config, out_dir, "test", horizon, split["test"], context=context, force=force
            )
            backtests[arm][horizon] = bt
            if horizon == horizons[0]:
                cost_rows.append(stats)

    primary = horizons[0]
    test_origins = split["test"]
    dev_origins = split["dev"]

    dev_frame = _dev_leaderboard(panel, config, out_dir, selection, dev_origins, primary)
    test_frame = build_leaderboard(
        {arm: bts[primary] for arm, bts in backtests.items()}, panel, config, test_origins, primary
    )
    test_frame = test_frame.merge(
        dev_frame[["arm", "avg_rel_cum12"]].rename(columns={"avg_rel_cum12": "dev_avg_rel_cum12"}),
        on="arm",
        how="left",
    )
    test_frame["money_ratio"] = test_frame["money_ratio"].fillna(np.inf)

    # Secondary h = 24 run (plan §3/§5): reported, never used for the decision.
    secondary = horizons[1]
    if secondary in {h for bts in backtests.values() for h in bts}:
        long_frame = build_leaderboard(
            {arm: bts[secondary] for arm, bts in backtests.items() if secondary in bts},
            panel,
            config,
            test_origins,
            secondary,
        )
        long_frame = long_frame.rename(
            columns={"avg_rel_cum12": f"avg_rel_cum{secondary}", "n_origin_series": "n_series_24"}
        )
        test_frame = test_frame.merge(
            long_frame[["arm", f"avg_rel_cum{secondary}"]], on="arm", how="left"
        )
        long_frame.to_csv(out_dir / "test_leaderboard_h24.csv", index=False)

    test_frame.to_csv(out_dir / "test_leaderboard.csv", index=False)
    dev_frame.to_csv(out_dir / "dev_leaderboard.csv", index=False)
    pd.DataFrame(cost_rows).to_csv(out_dir / "arm_costs.csv", index=False)

    # Test-side fallback shares. Pair-level, not row-level: a fallback that
    # replaces one origin-series pair is 12 identical rows, so a row share
    # understates it by an order of magnitude.
    _write_test_fallbacks(backtests, panel, out_dir, test_origins, primary, config)

    # Strata: flow x level, SBC class, dormant vs active, on test origins.
    strata_frames = []
    for arm in test_frame["arm"]:
        pairs = pairs_for(backtests[arm][primary], panel, test_origins, primary)
        base_pairs = pairs_for(backtests[BASE_ARM][primary], panel, test_origins, primary)
        table = strata_table(pairs, panel, base_pairs, pd.to_datetime(max(test_origins)))
        table["arm"] = arm
        strata_frames.append(table)
    strata = pd.concat(strata_frames, ignore_index=True) if strata_frames else pd.DataFrame()
    strata.to_csv(out_dir / "strata_test.csv", index=False)

    verdict = apply_decision_rule(test_frame, strata, config)
    chosen = verdict.get("chosen")

    # Bands for the chosen point method: native vs pooled scaled empirical.
    bands = {}
    if chosen in backtests:
        bt = backtests[chosen][primary]
        classes = classes_at(panel, pd.to_datetime(max(test_origins)))
        arm_pairs = pairs_for(bt, panel, test_origins, primary)
        # The error pool is every realised origin available: the dev backtest
        # of the same frozen arm plus the test one. Test origins after the
        # first are therefore banded from earlier fully realised errors only.
        pool_frames = [bt]
        try:
            dev_path = backtest_path(out_dir, "dev", selection[chosen], primary)
            if dev_path.exists():
                pool_frames.append(pd.read_parquet(dev_path))
        except (KeyError, OSError):
            pass
        history = cumulative_columns(pd.concat(pool_frames, ignore_index=True)).copy()
        history["err"] = history["actual"] - history["point"]
        scale_map = {}
        for key, group in history.groupby("series_key", sort=False):
            scale_map[key] = float(max(12.0 * group["actual"].abs().mean(), 1.0))
        history["scale"] = history["series_key"].map(scale_map)
        pooled_frame = scaled_pooled_band(
            arm_pairs,
            history,
            history,
            classes,
            min_prior=int(config["bands"]["min_prior_errors"]),
        )
        pooled = band_scores(pooled_frame, alpha=float(config["bands"]["alpha"]), horizon=primary)

        # Native bands exist only where the arm emits an interval, which is a
        # subset of pairs. Score both candidates on that *same* subset, plus
        # report native coverage over every pair with a missing band counted as
        # a miss: otherwise the comparison rewards covering fewer series.
        native_subset = native_band_scores(bt, primary)
        pooled_subset = _pooled_on_pairs(
            pooled_frame, native_subset, primary, config
        )
        native_all = _native_coverage_all_pairs(bt, primary)

        # Plan §7 band rule, applied mechanically: whichever candidate's test
        # cum-12 coverage is closer to 0.80; ties on lower pinball. Compared on
        # the common subset so both see the same pairs.
        native_cov = native_subset.get("cum12_coverage")
        pooled_cov = pooled_subset.get("cum12_coverage")
        have_native = native_cov is not None and np.isfinite(native_cov)
        have_pooled = pooled_cov is not None and np.isfinite(pooled_cov)
        if have_native and have_pooled:
            native_dist = abs(float(native_cov) - 0.8)
            pooled_dist = abs(float(pooled_cov) - 0.8)
            if pooled_dist < native_dist:
                version = "pooled_scaled"
            elif pooled_dist > native_dist:
                version = "native"
            else:
                native_pin = float(native_subset.get("pinball", np.inf))
                pooled_pin = float(pooled_subset.get("pinball", np.inf))
                version = "pooled_scaled" if pooled_pin < native_pin else "native"
        elif have_pooled:
            version = "pooled_scaled"
        elif have_native:
            version = "native"
        else:
            version = None

        lo, hi = config["decision"]["band_coverage"]
        chosen_cov = pooled_cov if version == "pooled_scaled" else native_cov
        within = bool(
            chosen_cov is not None
            and np.isfinite(chosen_cov)
            and float(lo) <= float(chosen_cov) <= float(hi)
        )
        bands = {
            "chosen": chosen,
            "chosen_band": version,
            "band_rule": "closest test cum-12 coverage to 0.80 on the common subset, ties on pinball",
            "calibrated": within,
            "flagged": not within,
            "coverage_window": [float(lo), float(hi)],
            "native": native_subset,
            "pooled": pooled_subset,
            "pooled_all_pairs": pooled,
            "native_all_pairs": native_all,
            "common_pairs": int(native_subset.get("n_pairs", 0)),
            "native_coverage_share": float(
                native_subset.get("n_pairs", 0) / max(len(arm_pairs), 1)
            ),
        }

    # DM on per-origin panel loss against B1 and P0.
    dm = {}
    if chosen in backtests:
        chosen_pairs = pairs_for(backtests[chosen][primary], panel, test_origins, primary)
        chosen_loss = per_origin_loss(chosen_pairs)
        for reference in (BASE_ARM, "prophet_shipped"):
            if reference not in backtests or reference == chosen:
                continue
            ref_pairs = pairs_for(backtests[reference][primary], panel, test_origins, primary)
            ref_loss = per_origin_loss(ref_pairs)
            common = chosen_loss.index.intersection(ref_loss.index)
            dm[reference] = dm_test_hln(
                chosen_loss.loc[common].to_numpy(),
                ref_loss.loc[common].to_numpy(),
                h=12,
                hac_lags=11,
            )

    # Per-series table (real names; private).
    chosen_pairs = pairs_for(backtests[chosen][primary], panel, test_origins, primary)
    base_pairs = pairs_for(backtests[BASE_ARM][primary], panel, test_origins, primary)
    series_rows = []
    for key, group in chosen_pairs.groupby("series_key"):
        ref = base_pairs[base_pairs["series_key"] == key]
        arm_err = float(group["cumerr"].abs().sum())
        base_err = float(ref["cumerr"].abs().sum())
        series_rows.append(
            {
                "series_key": key,
                **parse_series_key(key),
                "abs_cum_err": arm_err,
                "base_abs_cum_err": base_err,
                "ratio": arm_err / base_err if base_err > 0 else np.nan,
                "n_origins": int(len(group)),
            }
        )
    pd.DataFrame(series_rows).to_csv(out_dir / "series_test.csv", index=False)

    summary = {
        "meta": {
            "config_sha256": config_sha256(),
            "frozen_at": config["meta"].get("frozen_at"),
            "as_of": str(split["months"][-1].date()),
            "n_months": int(len(split["months"])),
            "n_dev_origins": len(dev_origins),
            "n_test_origins": len(test_origins),
            "n_embargo_origins": len(split["embargo"]),
            "step": int(config["split"]["step"]),
            "s2_table_sha256": config["meta"].get("s2_table_sha256"),
        },
        "verdict": verdict,
        "bands": bands,
        "dm": dm,
    }
    (out_dir / "verdict.json").write_text(json.dumps(verdict, indent=2, default=float), encoding="utf-8")
    def _jsonable(value):
        """Drop non-serialisable helper fields (pair key sets) from the dump."""
        if isinstance(value, dict):
            return {k: _jsonable(v) for k, v in value.items() if k != "pairs"}
        if isinstance(value, (list, tuple)):
            return [_jsonable(v) for v in value]
        if isinstance(value, (np.integer, np.floating)):
            return float(value)
        if isinstance(value, np.bool_):
            return bool(value)
        return value

    (out_dir / "bands.json").write_text(
        json.dumps(_jsonable(bands), indent=2, default=float), encoding="utf-8"
    )
    (out_dir / "dm_tests.json").write_text(json.dumps(dm, indent=2, default=float), encoding="utf-8")
    (out_dir / "test_summary.json").write_text(json.dumps(_jsonable(summary), indent=2, default=float), encoding="utf-8")
    print(json.dumps(verdict, indent=2, default=float))
    return summary


# ── CLI ──────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    global CONFIG_PATH
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase", choices=("timing", "diagnostics", "dev", "test", "report")
    )
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--force", action="store_true", help="ignore the backtest cache")
    args = parser.parse_args(argv)

    CONFIG_PATH = Path(args.config)
    config = load_config(CONFIG_PATH)
    out_dir = Path(config["meta"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.freeze:
        freeze(config, out_dir)
        return 0
    if args.phase == "timing":
        timing_phase(config, out_dir, force=args.force)
        return 0
    if args.phase == "diagnostics":
        diagnostics_phase(config, out_dir)
        return 0
    if args.phase == "dev":
        dev_phase(config, out_dir, force=args.force)
        return 0
    if args.phase == "test":
        test_phase(config, out_dir, force=args.force)
        return 0
    if args.phase == "report":
        print("run scripts/experiments/render_category_report.py to render the report")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
