"""
Savings-forecast bake-off runner (Phase 3 dev, Phase 4 test/report).

Reads ``configs/forecast_experiment.yaml``, backtests the arms from
``src.analysis.forecasters`` on the target series from
``src.analysis.feature_engineering``, caches every arm's backtest, and applies
the pre-registered decision rule in plan §7.

Phases
------
``--phase dev``
    Evaluate every candidate on the dev origins, pick one variant per arm by the
    dev mean ``cum12_abs``, and write ``dev_selection.json``. Nothing here reads
    a test-period actual.
``--phase test``
    Refuses to run unless the config is frozen. Evaluates the frozen selection
    on the test origins, then writes leaderboards, bands, DM tests and figures.
``--phase report``
    Re-renders ``report.md`` from cached test artefacts, without refitting.

Usage:
    poetry run python scripts/experiments/savings_forecast_bakeoff.py --phase dev
    poetry run python scripts/experiments/savings_forecast_bakeoff.py --freeze
    poetry run python scripts/experiments/savings_forecast_bakeoff.py --phase test
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.analysis.feature_engineering import complete_cashflow_series  # noqa: E402
from src.analysis.forecast_backtest import rolling_origin_backtest  # noqa: E402
from src.analysis.forecast_metrics import (  # noqa: E402
    arm_summary,
    common_origins,
    dm_test_hln,
    mase_scale_by_origin,
)
from src.analysis.forecasters import (  # noqa: E402
    ArmRecorder,
    complexity_rank,
    make_arm,
)

CONFIG_PATH = Path("configs/forecast_experiment.yaml")


# ── Config and hashing ───────────────────────────────────────────────────────


def load_config(path: Path | None = None) -> dict:
    target = Path(path) if path is not None else CONFIG_PATH
    return yaml.safe_load(target.read_text(encoding="utf-8"))


def config_sha256(path: Path | None = None) -> str:
    target = Path(path) if path is not None else CONFIG_PATH
    return hashlib.sha256(target.read_bytes()).hexdigest()


def require_frozen(config: dict) -> None:
    if not config.get("meta", {}).get("frozen"):
        raise SystemExit(
            "Refusing to run the test phase: configs/forecast_experiment.yaml has "
            "`frozen: false`. Run --phase dev, then --freeze, and commit the config "
            "before the test phase (plan §6 Phase 3)."
        )


# ── Data and split ───────────────────────────────────────────────────────────


def load_series(config: dict) -> tuple[pd.Series, pd.DataFrame]:
    split = pd.read_parquet(config["data"]["cashflow_split"])
    series = complete_cashflow_series(split)
    series.index = pd.DatetimeIndex(series.index).to_period("M").to_timestamp()
    components = split[~split["is_partial_month"].astype(bool)].copy()
    components.index = pd.to_datetime(components["year_month"] + "-01")
    components = components.reindex(series.index)
    return series, components


def _origin_dates(series: pd.Series, initial: int, horizon: int, step: int) -> list[pd.Timestamp]:
    return [series.index[position - 1] for position in range(initial, len(series) - horizon + 1, step)]


def make_split(series: pd.Series, config: dict) -> dict:
    """
    Purged/embargoed origin split by date (plan §3).

    ``test`` is the last third of the primary-horizon origins. ``dev`` holds
    earlier origins whose whole 12-month target window ends before the first
    test target month. Origins in between form the embargo.
    """
    split_cfg = config["split"]
    initial = int(split_cfg["initial"])
    step = int(split_cfg["step"])
    horizon = int(split_cfg["horizon_primary"])

    primary = _origin_dates(series, initial, horizon, step)
    n_test = int(np.floor(len(primary) * float(split_cfg["test_fraction"])))
    test = primary[-n_test:]
    first_test_target = test[0] + pd.DateOffset(months=1)
    dev = [o for o in primary if o + pd.DateOffset(months=horizon) < first_test_target]
    embargo = [o for o in primary if o not in set(dev) and o not in set(test)]
    return {
        "primary_origins": primary,
        "dev": dev,
        "test": test,
        "embargo": embargo,
        "first_test_target": first_test_target,
    }


# ── Backtesting and caching ──────────────────────────────────────────────────


def slug(value: dict) -> str:
    text = json.dumps(value, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:10]


def backtest_path(out_dir: Path, phase: str, arm: str, params: dict, horizon: int) -> Path:
    tag = "" if not params else f"_{slug(params)}"
    folder = out_dir / "backtests" if phase == "test" else out_dir / "backtests" / "dev"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{arm}{tag}_h{horizon}.parquet"


def run_arm(
    arm_id: str,
    params: dict,
    series: pd.Series,
    components: pd.DataFrame,
    config: dict,
    out_dir: Path,
    phase: str,
    horizon: int,
    force: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """Backtest one configured arm, reusing the cached parquet when possible."""
    path = backtest_path(out_dir, phase, arm_id, params, horizon)
    meta_path = path.with_suffix(".json")
    if path.exists() and meta_path.exists() and not force:
        return pd.read_parquet(path), json.loads(meta_path.read_text(encoding="utf-8"))

    payload = {
        "deposits": components["deposits"].astype(float),
        "withdrawals": components["withdrawals"].astype(float),
    }
    kwargs = dict(params)
    robust = kwargs.pop("robust", False)
    combo_windows = kwargs.pop("combo_windows", None)
    base = make_arm(
        arm_id,
        components=payload,
        combo_windows=combo_windows,
        robust=robust,
        **kwargs,
    )
    recorder = ArmRecorder(arm_id, base)
    split_cfg = config["split"]
    bt = rolling_origin_backtest(
        series,
        recorder,
        initial=int(split_cfg["initial"]),
        horizon=horizon,
        step=int(split_cfg["step"]),
    )
    bt.to_parquet(path, index=False)
    stats = recorder.stats()
    stats["config_sha256"] = config_sha256()
    stats["horizon"] = horizon
    stats["params"] = params
    meta_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    return bt, stats


# ── Dev phase ────────────────────────────────────────────────────────────────


def evaluate_variants(
    arm_id: str,
    variants: list[dict],
    series: pd.Series,
    components: pd.DataFrame,
    config: dict,
    out_dir: Path,
    horizons: tuple[int, ...],
    phase: str,
    force: bool,
) -> tuple[dict, list[dict]]:
    """Score each candidate variant on the dev origins; return the winner."""
    dev = make_split(series, config)["dev"]
    scales = mase_scale_by_origin(series, dev, m=int(config["metrics"]["mase_season"]))
    rows = []
    cache: dict[str, dict] = {}

    for params in variants:
        entry = {"arm": arm_id, "params": params}
        for horizon in horizons:
            bt, stats = run_arm(
                arm_id, params, series, components, config, out_dir, phase, horizon, force=force
            )
            summary = arm_summary(
                bt,
                mase_scales=scales,
                horizon=int(config["split"]["horizon_primary"]),
                band_alpha=float(config["bands"]["alpha"]),
                min_band_origins=int(config["bands"]["min_origins"]),
                origins=dev,
            )
            entry[f"cum12_abs_h{horizon}"] = summary["cum12_abs"]
            entry[f"n_origins_h{horizon}"] = summary["n_origins"]
            if horizon == int(config["split"]["horizon_primary"]):
                entry.update(
                    {
                        "mae_1_12": summary["mae_1_12"],
                        "mase_1_12": summary["mase_1_12"],
                        "cum12_bias": summary["cum12_bias"],
                        "under_k": summary["under_k"],
                        "band_coverage": summary["band_coverage"],
                        "mean_seconds": stats.get("mean_seconds", float("nan")),
                        "failures": stats.get("failures", 0),
                        "skips": stats.get("skips", 0),
                    }
                )
        entry["cum12_abs"] = entry.get("cum12_abs_h12", float("nan"))
        rows.append(entry)
        cache[slug(params)] = entry

    valid = [row for row in rows if np.isfinite(row["cum12_abs"])]
    if not valid:
        return {}, rows
    winner = min(valid, key=lambda row: (row["cum12_abs"], len(row["params"])))
    return winner, rows


def dev_phase(config: dict, out_dir: Path, force: bool) -> dict:
    series, components = load_series(config)
    split = make_split(series, config)
    primary = int(config["split"]["horizon_primary"])
    # Tuning uses the primary horizon only (dev cum12_abs is the selection
    # criterion); the secondary run is evaluated once per chosen variant.
    horizons = (primary,)

    print(
        f"series: n={len(series)} {series.index[0].date()}..{series.index[-1].date()} | "
        f"dev={len(split['dev'])} test={len(split['test'])} embargo={len(split['embargo'])}"
    )

    arms_cfg = config["arms"]
    summary_rows: list[dict] = []
    selection: dict[str, dict] = {}

    for arm_id, arm_cfg in arms_cfg.items():
        variants = arm_cfg.get("variants") or [{}]
        winner, rows = evaluate_variants(
            arm_id, variants, series, components, config, out_dir, horizons, "dev", force
        )
        summary_rows.extend(rows)
        if winner:
            selection[arm_id] = {"params": winner["params"], "dev_cum12_abs": winner["cum12_abs"]}
            print(f"  {arm_id:22s} dev cum12_abs={winner['cum12_abs']:.0f} params={winner['params']}")
        else:
            selection[arm_id] = {"params": variants[0], "dev_cum12_abs": None}
            print(f"  {arm_id:22s} FAILED on every dev origin")

    # Robust-target variant on the three best untuned Tier-1 arms (plan §4.3, R).
    robust_cfg = config.get("robust_target", {})
    robust_decisions: dict[str, dict] = {}
    if robust_cfg.get("enabled"):
        candidates = [
            arm
            for arm in robust_cfg.get("candidates_from", [])
            if selection.get(arm, {}).get("dev_cum12_abs") is not None
        ]
        candidates.sort(key=lambda arm: selection[arm]["dev_cum12_abs"])
        chosen = candidates[: int(robust_cfg.get("top_n", 3))]
        for arm in chosen:
            best = None
            for enabled in (True, False):
                variant = {"robust": enabled}
                winner, rows = evaluate_variants(
                    f"robust__{arm}",
                    [variant],
                    series,
                    components,
                    config,
                    out_dir,
                    horizons,
                    "dev",
                    force,
                )
                summary_rows.extend(rows)
                if winner and (best is None or winner["cum12_abs"] < best["dev_cum12_abs"]):
                    best = {
                        "params": {**selection[arm]["params"], **variant},
                        "dev_cum12_abs": winner["cum12_abs"],
                        "robust": enabled,
                    }
            if best is not None:
                if best["robust"]:
                    selection[f"robust__{arm}"] = best
                    print(
                        f"  robust__{arm:15s} dev cum12_abs={best['dev_cum12_abs']:.0f} "
                        f"robust=True (kept)"
                    )
                else:
                    # Cleaning did not help: the arm is identical to its base
                    # version, so it is not added as a separate entry.
                    robust_decisions[arm] = {"robust": False, "dev_delta": best["dev_cum12_abs"]}
                    print(
                        f"  robust__{arm:15s} dev cum12_abs={best['dev_cum12_abs']:.0f} "
                        f"robust=False (dropped: identical to base)"
                    )

    # Combo arm uses each member's dev-chosen window (composition itself fixed).
    combo_members = ("auto_ets", "auto_arima", "theta")
    if all(member in arms_cfg for member in combo_members):
        combo_windows = {
            member: selection.get(member, {}).get("params", {}).get("window")
            for member in combo_members
        }
        winner, rows = evaluate_variants(
            "combo_classical",
            [{"combo_windows": combo_windows}],
            series,
            components,
            config,
            out_dir,
            horizons,
            "dev",
            force,
        )
        summary_rows.extend(rows)
        if winner:
            selection["combo_classical"] = {
                "params": {"combo_windows": combo_windows},
                "dev_cum12_abs": winner["cum12_abs"],
            }
            print(f"  combo_classical        dev cum12_abs={winner['cum12_abs']:.0f}")

    table = pd.DataFrame(summary_rows)
    table.to_csv(out_dir / "dev_candidates.csv", index=False)

    payload = {
        "dev_origins": [str(o.date()) for o in split["dev"]],
        "test_origins": [str(o.date()) for o in split["test"]],
        "embargo_origins": [str(o.date()) for o in split["embargo"]],
        "selection": selection,
        "robust_decisions": robust_decisions,
        "config_sha256": config_sha256(),
    }
    (out_dir / "dev_selection.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


# ── Freeze ───────────────────────────────────────────────────────────────────


def freeze(config: dict, out_dir: Path) -> None:
    """Copy dev choices into the config and mark it frozen (the pre-registration)."""
    selection_path = out_dir / "dev_selection.json"
    if not selection_path.exists():
        raise SystemExit("No dev_selection.json: run --phase dev first.")
    payload = json.loads(selection_path.read_text(encoding="utf-8"))
    config["selection"] = {
        arm: entry.get("params", {}) for arm, entry in payload["selection"].items()
    }
    config["meta"]["frozen"] = True
    config["meta"]["frozen_at"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    # The digest is not stored inside the file: any self-referential hash would
    # not match the bytes on disk. It is recorded beside the experiment instead
    # and re-computed for every test artefact.
    config["meta"]["config_sha256"] = None
    CONFIG_PATH.write_text(
        yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    digest = config_sha256()
    (out_dir / "frozen_config.sha256").write_text(digest + "\n", encoding="utf-8")
    print(f"Frozen. config sha256 = {digest}")
    print("Commit configs/forecast_experiment.yaml before running --phase test.")


# ── Test phase ───────────────────────────────────────────────────────────────


def apply_decision_rule(leaderboard: pd.DataFrame, config: dict) -> dict:
    """
    Mechanically apply plan §7 and return the verdict with the deciding step.

    ``leaderboard`` has one row per arm with ``test_cum12_abs``,
    ``dev_cum12_abs``, ``test_relcum12``, ``dev_relcum12``, ``mase_1_12``,
    ``cum12_bias``, ``band_coverage``.
    """
    decision = config["decision"]
    threshold = float(decision["relcum12_max"])
    rows = leaderboard.set_index("arm")

    eligible = rows[
        (rows["test_relcum12"] < threshold) & (rows["dev_relcum12"] < threshold)
    ]
    if eligible.empty:
        # §7.1: recommend B1, or B2 if B2 qualifies on its own.
        fallback = "naive_mean_12" if "naive_mean_12" in rows.index else None
        b2 = rows.loc["naive_median_12"] if "naive_median_12" in rows.index else None
        if b2 is not None and b2["test_relcum12"] < threshold and b2["dev_relcum12"] < threshold:
            fallback = "naive_median_12"
        return {
            "step": "§7.1 eligibility",
            "chosen": fallback,
            "reason": (
                "No arm beat the naive mean in both periods; naive wins."
                if fallback != "naive_median_12"
                else "No arm beat the naive mean in both periods; B2 qualifies, so B2 wins."
            ),
            "trail": [],
        }

    trail = []
    ordered = eligible.sort_values("test_relcum12")
    candidate = ordered.index[0]
    trail.append(
        {
            "step": "§7.2 candidate",
            "arm": candidate,
            "test_relcum12": float(rows.loc[candidate, "test_relcum12"]),
        }
    )

    # §7.3 parsimony: walk from the simplest eligible arm upward.
    margin = float(decision["parsimony_margin"])
    ranked = sorted(eligible.index, key=complexity_rank)
    changed = True
    while changed:
        changed = False
        for arm in ranked:
            if complexity_rank(arm) >= complexity_rank(candidate):
                continue
            if rows.loc[arm, "test_cum12_abs"] <= rows.loc[candidate, "test_cum12_abs"] * (1 + margin):
                trail.append(
                    {
                        "step": "§7.3 parsimony",
                        "from": candidate,
                        "to": arm,
                        "margin_used": float(
                            rows.loc[arm, "test_cum12_abs"] / rows.loc[candidate, "test_cum12_abs"] - 1
                        ),
                    }
                )
                candidate = arm
                changed = True
                break

    # §7.4 guardrails, moving down the test RelCum12 order on failure.
    bias_ref = decision["baselines"]["bias_reference"]
    mase_ref = decision["baselines"]["denominator"]
    mase_limit = float(rows.loc[mase_ref, "mase_1_12"]) * (1 + float(decision["mase_margin"]))
    bias_limit = abs(float(rows.loc[bias_ref, "cum12_bias"]))
    cov_lo, cov_hi = decision["band_coverage"]

    order = list(ordered.index)
    start = order.index(candidate)
    for arm in order[start:]:
        failures = []
        if not (rows.loc[arm, "mase_1_12"] <= mase_limit):
            failures.append(f"(a) mase {rows.loc[arm, 'mase_1_12']:.3f} > {mase_limit:.3f}")
        if not (abs(float(rows.loc[arm, "cum12_bias"])) <= bias_limit):
            failures.append(
                f"(b) |bias| {abs(float(rows.loc[arm, 'cum12_bias'])):.0f} > {bias_limit:.0f}"
            )
        coverage = float(rows.loc[arm, "band_coverage"])
        if not (np.isfinite(coverage) and cov_lo <= coverage <= cov_hi):
            failures.append(f"(c) band coverage {coverage:.2f} outside [{cov_lo}, {cov_hi}]")
        if not failures:
            if arm != candidate:
                trail.append({"step": "§7.4 guardrails", "rejected": candidate, "moved_to": arm})
            return {
                "step": "§7.4 guardrails" if arm != candidate else (
                    "§7.3 parsimony" if len(trail) > 1 else "§7.2 candidate"
                ),
                "chosen": arm,
                "reason": "; ".join(failures) if failures else "All guardrails pass.",
                "trail": trail,
            }
        trail.append({"step": "§7.4 guardrails", "rejected": arm, "failures": failures})

    return {
        "step": "§7.1 eligibility",
        "chosen": "naive_mean_12" if "naive_mean_12" in rows.index else None,
        "reason": "Every eligible arm failed a guardrail; fall back to the naive mean.",
        "trail": trail,
    }


def leaderboard_frame(
    backtests: dict,
    series: pd.Series,
    config: dict,
    origins: list,
    denominator_arm: str = "naive_mean_12",
) -> pd.DataFrame:
    """One row per arm over a fixed origin set, with RelCum12 against the naive mean."""
    primary = int(config["split"]["horizon_primary"])
    mase_scales = mase_scale_by_origin(series, origins, m=int(config["metrics"]["mase_season"]))
    rows = []
    for arm, bt in backtests.items():
        summary = arm_summary(
            bt,
            mase_scales=mase_scales,
            horizon=primary,
            band_alpha=float(config["bands"]["alpha"]),
            min_band_origins=int(config["bands"]["min_origins"]),
            origins=origins,
        )
        summary["arm"] = arm
        rows.append(summary)

    frame = pd.DataFrame(rows)
    denominator = frame.loc[frame["arm"] == denominator_arm, "cum12_abs"]
    base = float(denominator.iloc[0]) if not denominator.empty else float("nan")
    frame["relcum12"] = frame["cum12_abs"] / base
    frame["relmae"] = frame["mae_1_12"] / float(
        frame.loc[frame["arm"] == denominator_arm, "mae_1_12"].iloc[0]
    )
    return frame


def test_phase(config: dict, out_dir: Path, force: bool) -> dict:
    require_frozen(config)
    series, components = load_series(config)
    split = make_split(series, config)
    horizons = (int(config["split"]["horizon_primary"]), int(config["split"]["horizon_secondary"]))

    selection = config["selection"]
    if not selection:
        raise SystemExit("Frozen config has an empty `selection`; refusing to run.")

    # The robust variant decision is encoded in the selection entry.
    backtests: dict[str, dict[int, pd.DataFrame]] = {}
    cost_rows = []
    for arm, params in selection.items():
        backtests[arm] = {}
        for horizon in horizons:
            bt, stats = run_arm(
                arm, params, series, components, config, out_dir, "test", horizon, force=force
            )
            backtests[arm][horizon] = bt
            if horizon == horizons[0]:
                cost_rows.append(stats)

    primary = horizons[0]
    test_origins = split["test"]
    dev_origins = split["dev"]

    # Cross-arm comparisons use only origins where every arm produced a
    # forecast (plan §4.1). Arms such as M1/M2 skip early origins because they
    # need a fixed number of training rows, so a per-arm set would compare
    # different windows.
    test_common = [
        origin
        for origin in common_origins(
            {arm: bts[primary] for arm, bts in backtests.items()}, horizon=primary
        )
        if origin in set(pd.to_datetime(test_origins))
    ]
    dev_common = [
        origin
        for origin in common_origins(
            {arm: bts[primary] for arm, bts in backtests.items()}, horizon=primary
        )
        if origin in set(pd.to_datetime(dev_origins))
    ]
    if not test_common or not dev_common:
        raise SystemExit("One or more arms produced no usable forecast; cannot compare.")

    test_frame = leaderboard_frame(
        {arm: bts[primary] for arm, bts in backtests.items()}, series, config, test_common
    )
    dev_frame = leaderboard_frame(
        {arm: bts[primary] for arm, bts in backtests.items()}, series, config, dev_common
    )

    merged = test_frame.rename(
        columns={
            "cum12_abs": "test_cum12_abs",
            "relcum12": "test_relcum12",
            "cum12_bias": "cum12_bias",
            "under_k": "under_k",
            "n_origins": "test_n",
            "mase_1_12": "mase_1_12",
            "band_coverage": "band_coverage",
            "band_width": "band_width",
            "band_n": "band_n",
            "mae_1_12": "test_mae",
            "mae_h1": "mae_h1",
            "mae_h3": "mae_h3",
            "mae_h12": "mae_h12",
        }
    )[
        [
            "arm", "test_n", "test_cum12_abs", "test_relcum12", "cum12_bias", "under_k",
            "mase_1_12", "band_coverage", "band_width", "band_n", "test_mae",
            "mae_h1", "mae_h3", "mae_h12",
        ]
    ]
    merged = merged.merge(
        dev_frame[["arm", "relcum12", "cum12_abs"]].rename(
            columns={"relcum12": "dev_relcum12", "cum12_abs": "dev_cum12_abs"}
        ),
        on="arm",
        how="left",
    )

    # h=24 secondary: cum24_abs on the test origins the 24-month run covers,
    # again restricted to origins every arm could forecast.
    secondary = []
    common24 = common_origins(
        {arm: bts[horizons[1]] for arm, bts in backtests.items()}, horizon=24
    )
    common24 = [origin for origin in common24 if origin in set(pd.to_datetime(test_origins))]
    for arm, bts in backtests.items():
        bt24 = bts[horizons[1]]
        if bt24.empty:
            continue
        summary = arm_summary(bt24, horizon=24, origins=common24)
        secondary.append({"arm": arm, "cum24_abs": summary["cum12_abs"], "n": summary["n_origins"]})
    if secondary:
        merged = merged.merge(pd.DataFrame(secondary), on="arm", how="left")
    merged = merged.sort_values("test_relcum12").reset_index(drop=True)

    merged.to_csv(out_dir / "test_leaderboard.csv", index=False)
    pd.DataFrame(cost_rows).to_csv(out_dir / "arm_costs.csv", index=False)

    verdict = apply_decision_rule(merged, config)

    # Diebold–Mariano for the chosen arm against B1 and P0 only (plan §5).
    dm = {}
    chosen = verdict.get("chosen")
    if chosen and chosen in backtests and primary in backtests[chosen]:
        for reference in ("naive_mean_12", "prophet_shipped"):
            if reference not in backtests:
                continue
            dm[reference] = dm_pair(
                backtests[chosen][primary], backtests[reference][primary], test_common
            )
    (out_dir / "dm_tests.json").write_text(json.dumps(dm, indent=2, default=float), encoding="utf-8")
    (out_dir / "verdict.json").write_text(
        json.dumps(verdict, indent=2, default=float), encoding="utf-8"
    )

    payload = {
        "meta": {
            "config_sha256": config_sha256(),
            "frozen_at": config["meta"].get("frozen_at"),
            "as_of": str(series.index[-1].date()),
            "n_months": int(len(series)),
            "n_dev_origins": len(dev_origins),
            "n_test_origins": len(test_origins),
            "n_embargo_origins": len(split["embargo"]),
            "n_dev_common": len(dev_common),
            "n_test_common": len(test_common),
            "n_test_common_h24": len(common24),
            "dev_common_first": str(dev_common[0].date()),
            "test_common_first": str(test_common[0].date()),
        },
        "verdict": verdict,
        "dm": dm,
    }
    (out_dir / "test_summary.json").write_text(
        json.dumps(payload, indent=2, default=float), encoding="utf-8"
    )
    return payload


def dm_pair(bt_a: pd.DataFrame, bt_b: pd.DataFrame, origins: list) -> dict:
    """DM on |error| at h=1 (lag 0) and cum-12 |error| (lag 11), HLN corrected."""
    keep = set(pd.to_datetime(list(origins)))

    def _h1(frame):
        rows = frame[(frame["h"] == 1) & frame["origin_month"].isin(keep)]
        return (
            (rows["actual"] - rows["point"]).abs().to_numpy(),
            rows["origin_month"].to_numpy(),
        )

    a_h1, a_orig = _h1(bt_a)
    b_h1, b_orig = _h1(bt_b)
    common = np.intersect1d(a_orig, b_orig)
    mask_a = np.isin(a_orig, common)
    mask_b = np.isin(b_orig, common)
    h1 = dm_test_hln(a_h1[mask_a], b_h1[mask_b], h=1, hac_lags=0)

    def _cum(frame):
        rows = frame[(frame["h"] == 12) & frame["origin_month"].isin(keep)]
        return (
            (rows["cum_actual"] - rows["cum_point"]).abs().to_numpy(),
            rows["origin_month"].to_numpy(),
        )

    a_c, a_orig = _cum(bt_a)
    b_c, b_orig = _cum(bt_b)
    common = np.intersect1d(a_orig, b_orig)
    cum = dm_test_hln(
        a_c[np.isin(a_orig, common)], b_c[np.isin(b_orig, common)], h=12, hac_lags=11
    )
    return {"mae_h1": h1, "cum12": cum}


# ── CLI ──────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    global CONFIG_PATH
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("dev", "test", "report"))
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

    if args.phase == "dev":
        dev_phase(config, out_dir, args.force)
        return 0
    if args.phase == "test":
        payload = test_phase(config, out_dir, args.force)
        print(json.dumps(payload["verdict"], indent=2, default=float))
        return 0
    if args.phase == "report":
        print("report phase is rendered by render_report.py")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
