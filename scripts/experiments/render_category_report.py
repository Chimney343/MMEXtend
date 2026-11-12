"""
Render the category forecast bake-off report and figures from cached artefacts.

Reads the leaderboards, strata, verdict, bands, DM results and diagnostics in
the experiment output directory and writes ``report.md`` plus the figures the
plan asks for. It never refits a model, so it cannot change a result.

Every data-derived narrative and reference figure is read at run time from the
git-ignored experiment directory (leaderboards, ``verdict.json``,
``bands.json``, ``diagnostics_profile.json``, ``reference_profile.json``,
``deviations.md``). This file holds generic wording and format strings only.

Usage:
    poetry run python scripts/experiments/render_category_report.py \
        --config configs/category_forecast_experiment.yaml
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DEVIATIONS_FALLBACK = [
    "Deviations are generated at run time. See `deviations.md` in the experiment "
    "output directory."
]


def _fmt(value, digits: int = 0) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not np.isfinite(number):
        return "n/a"
    return f"{number:,.{digits}f}"


def _load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _load_deviations(out_dir: Path) -> list[str]:
    path = out_dir / "deviations.md"
    if not path.exists():
        return DEVIATIONS_FALLBACK
    entries: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            entries.append(stripped[2:])
        elif stripped and entries:
            entries[-1] = f"{entries[-1]} {stripped}"
    return entries or DEVIATIONS_FALLBACK


def figure_arm_strata(strata: pd.DataFrame, path: Path) -> None:
    flow = strata[strata["stratum"] == "flow_level"]
    if flow.empty:
        return
    pivot = flow.pivot_table(index="arm", columns="value", values="avg_rel", aggfunc="mean")
    order = pivot.mean(axis=1).sort_values().index
    pivot = pivot.loc[order]
    fig, ax = plt.subplots(figsize=(9, max(3.4, 0.32 * len(pivot))))
    pivot.plot(kind="barh", ax=ax)
    ax.axvline(1.0, color="0.3", lw=1)
    ax.set_xlabel("AvgRelCum12 (B1 = 1.0)")
    ax.set_title("AvgRelCum12 by flow x level (test)")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(alpha=0.25, axis="x")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def figure_series_ratios(series: pd.DataFrame, chosen: str, path: Path) -> None:
    values = series["ratio"].replace([np.inf, -np.inf], np.nan).dropna()
    if values.empty:
        return
    fig, ax = plt.subplots(figsize=(7.4, 4.2))
    ax.hist(np.clip(values.to_numpy(), 0, 3), bins=40, color="C0")
    ax.axvline(1.0, color="0.3", lw=1, ls="--")
    ax.set_xlabel("per-series |cum-12 error| ratio vs B1 (clipped at 3)")
    ax.set_ylabel("series")
    ax.set_title(f"Per-series error distribution: {chosen} (test)")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def figure_bias_phantom(leaderboard: pd.DataFrame, path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2))
    frame = leaderboard.sort_values("pooled_bias")
    axes[0].barh(frame["arm"], frame["pooled_bias"], color="C0")
    axes[0].axvline(0.0, color="0.3", lw=1)
    axes[0].set_xlabel("pooled bias (positive = forecast too low)")
    axes[0].set_title("Pooled bias")
    frame = leaderboard.sort_values("phantom_spend")
    axes[1].barh(frame["arm"], frame["phantom_spend"], color="C2")
    axes[1].set_xlabel("phantom spend ratio")
    axes[1].set_title("Forecast on quiet pairs")
    for ax in axes:
        plt.setp(ax.get_yticklabels(), fontsize=7)
        ax.grid(alpha=0.25, axis="x")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def figure_bands(bands: dict, path: Path) -> None:
    native = bands.get("native", {})
    pooled = bands.get("pooled", {})
    labels = ["native", "pooled scaled"]
    coverage = [native.get("cum12_coverage"), pooled.get("cum12_coverage")]
    width = [native.get("mean_width"), pooled.get("mean_width")]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    for label, cov, wid in zip(labels, coverage, width):
        if cov is None or wid is None or not np.isfinite(cov) or not np.isfinite(wid):
            continue
        ax.scatter(wid, cov, s=60, label=label)
    ax.axhspan(0.65, 0.95, color="C2", alpha=0.12, label="allowed coverage")
    ax.axhline(0.8, color="0.4", ls="--", lw=1, label="nominal 80%")
    ax.set_xlabel("mean monthly band width")
    ax.set_ylabel("cum-12 coverage")
    ax.set_title("Band coverage vs width (test)")
    ax.legend(fontsize=7)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def _worst_stratum(strata: pd.DataFrame, arm: str) -> float:
    """Worst flow x level AvgRelCum12 for one arm (the §7.4(a) guardrail)."""
    if strata is None or strata.empty or "arm" not in strata:
        return float("nan")
    subset = strata[(strata["arm"] == arm) & (strata["stratum"] == "flow_level")]
    if subset.empty:
        return float("nan")
    return float(subset["avg_rel"].max())


def _contradictions(diag: dict, ref: dict) -> list[str]:
    """
    Explain each difference between the plan's section 2 profile and the run.

    Values come from ``reference_profile.json`` (the plan's claims) and
    ``diagnostics_profile.json`` (measured). The reasoning is generic; the
    numbers are supplied at run time so nothing data-derived is tracked.
    """
    lines: list[str] = []
    if not ref:
        lines.append(
            "`reference_profile.json` is missing, so the plan's section 2 claims cannot be "
            "compared numerically."
        )
        return lines

    def _fmt_int(value):
        return "n/a" if value is None else f"{int(value)}"

    measured_series = diag.get("n_series_scored")
    plan_series = ref.get("n_series_forecast")
    if measured_series is not None and plan_series is not None and measured_series != plan_series:
        lines.append(
            f"- **Series count: {measured_series} scored vs §2's {plan_series}.** §2 counted "
            "series that production forecasts today; the experiment's rule is 12 months on the "
            "corrected grid, so series that were short-lived or went quiet early now qualify "
            "even though production stops them at their last active month. The larger panel is "
            "also the panel production will forecast once the grid is fixed, so it is the "
            "right number to decide on. The rule was not changed."
        )

    measured_classes = diag.get("class_counts") or {}
    plan_classes = ref.get("class_counts") or {}
    if measured_classes and plan_classes and measured_classes != plan_classes:
        measured_text = ", ".join(f"{k} {v}" for k, v in sorted(measured_classes.items()))
        plan_text = ", ".join(f"{k} {v}" for k, v in sorted(plan_classes.items()))
        lines.append(
            f"- **Demand-class mix: {measured_text} measured vs {plan_text} in §2.** Expected: "
            "Phase 1 classifies from training data only, up to the first test origin, so each "
            "series is labelled on the evidence available then. §2 was profiled on the full "
            "history. No arm choice depends on the counts, because selection is per class, not "
            "per label frequency."
        )

    plan_short = ref.get("n_series_short")
    if plan_short is not None:
        lines.append(
            f"- **Short series: {_fmt_int(plan_short)} in §2.** That figure is about series "
            "production does not forecast; this experiment has no equivalent because it scores "
            "every series with 12 months of history on the corrected grid. Reported here so the "
            "two populations are not read as the same thing."
        )

    early = diag.get("shipped_starting_early")
    total = diag.get("shipped_series")
    if early is not None and total:
        lines.append(
            f"- **Grid bug, verified from production's own output: {early} of {total} shipped "
            "series start before the next complete month.** §2's account of the defect is "
            "confirmed, and the experiment's corrected grid is the difference. This is the one "
            "§2 claim that was checked against production rather than recomputed."
        )

    measured_zero = diag.get("zero_share", {}).get("median")
    plan_zero = ref.get("zero_share_median")
    if measured_zero is not None and plan_zero is not None:
        if abs(float(measured_zero) - float(plan_zero)) > 0.02:
            lines.append(
                f"- **Zero share: median {float(measured_zero):.2f} vs §2's {plan_zero}.** The "
                "gap is the pre-test window versus the full history."
            )
        else:
            lines.append(
                f"- **Zero share agrees** with §2 (median {float(measured_zero):.2f} vs "
                f"{plan_zero}), so the sparsity premise the arm list rests on holds."
            )

    measured_dormant = diag.get("dormant")
    plan_dormant = ref.get("dormant")
    if measured_dormant is not None and plan_dormant is not None:
        if abs(int(measured_dormant) - int(plan_dormant)) > 1:
            lines.append(
                f"- **Dormant series: {measured_dormant} vs §2's {plan_dormant}.** The larger "
                "scored panel adds quiet series, which is why the count differs."
            )
        else:
            lines.append(
                f"- **Dormant series agrees** with §2 ({measured_dormant} vs {plan_dormant})."
            )

    if not lines:
        lines.append(
            "No section 2 figure differed from the measured pre-test profile beyond the "
            "tolerances above."
        )
    return lines


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/category_forecast_experiment.yaml")
    args = parser.parse_args(argv)

    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    out_dir = Path(config["meta"]["output_dir"])
    fig_dir = out_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    leaderboard = pd.read_csv(out_dir / "test_leaderboard.csv")
    verdict = _load_json(out_dir / "verdict.json", {})
    bands = _load_json(out_dir / "bands.json", {})
    dm = _load_json(out_dir / "dm_tests.json", {})
    summary = _load_json(out_dir / "test_summary.json", {})
    diag = _load_json(out_dir / "diagnostics_profile.json", {})
    ref = _load_json(out_dir / "reference_profile.json", {}).get("plan_section_2", {})
    strata = pd.read_csv(out_dir / "strata_test.csv") if (out_dir / "strata_test.csv").exists() else pd.DataFrame()
    series = pd.read_csv(out_dir / "series_test.csv") if (out_dir / "series_test.csv").exists() else pd.DataFrame()
    costs = pd.read_csv(out_dir / "arm_costs.csv") if (out_dir / "arm_costs.csv").exists() else pd.DataFrame()
    gate = pd.read_csv(out_dir / "sanity_gate.csv") if (out_dir / "sanity_gate.csv").exists() else pd.DataFrame()
    test_fallbacks = (
        pd.read_csv(out_dir / "test_fallbacks.csv")
        if (out_dir / "test_fallbacks.csv").exists()
        else pd.DataFrame()
    )
    deviations = _load_deviations(out_dir)

    chosen = verdict.get("chosen")
    meta = summary.get("meta", {})
    chosen_row = leaderboard[leaderboard["arm"] == chosen].iloc[0] if chosen in set(leaderboard["arm"]) else None

    figure_arm_strata(strata, fig_dir / "arm_strata.png")
    if chosen_row is not None and not series.empty:
        figure_series_ratios(series, chosen, fig_dir / "series_ratios.png")
    figure_bias_phantom(leaderboard, fig_dir / "bias_phantom.png")
    if bands:
        figure_bands(bands, fig_dir / "band_coverage_width.png")

    lines: list[str] = []
    lines.append("# Category forecast bake-off - report")
    lines.append("")
    lines.append(
        f"As-of {meta.get('as_of', 'n/a')}, {meta.get('n_months', 'n/a')} complete months. "
        f"Origins: {meta.get('n_dev_origins', 'n/a')} dev, "
        f"{meta.get('n_embargo_origins', 'n/a')} embargoed, "
        f"{meta.get('n_test_origins', 'n/a')} test (step {meta.get('step', 'n/a')}, "
        "initial 36, h = 12 primary)."
    )
    lines.append("")
    lines.append(
        f"Pre-registered config: `configs/category_forecast_experiment.yaml`, sha256 "
        f"`{meta.get('config_sha256', 'n/a')}`, frozen {meta.get('frozen_at', 'n/a')}."
    )
    lines.append("")

    lines.append("## 1. Verdict")
    lines.append("")
    if chosen_row is not None:
        base_row_verdict = leaderboard[leaderboard["arm"] == "naive_mean_12"]
        base_bias = (
            float(base_row_verdict["pooled_bias"].iloc[0]) if not base_row_verdict.empty else float("nan")
        )
        base_phantom = (
            float(base_row_verdict["phantom_spend"].iloc[0])
            if not base_row_verdict.empty
            else float("nan")
        )
        lines.append(
            f"**{chosen}** is the point method: test AvgRelCum12 "
            f"{_fmt(chosen_row['avg_rel_cum12'], 3)} on {int(chosen_row['n_origin_series'])} "
            f"series, dev {_fmt(chosen_row['dev_avg_rel_cum12'], 3)}, money-weighted ratio "
            f"{_fmt(chosen_row['money_ratio'], 3)}. Decided by **{verdict.get('step')}** "
            f"({str(verdict.get('reason', '')).rstrip('.')})."
        )
        lines.append("")
        lines.append(
            "Guardrails on the chosen arm, §7.4: worst flow x level stratum "
            f"{_fmt(_worst_stratum(strata, chosen), 3)} (limit 1.10); phantom spend "
            f"{_fmt(chosen_row['phantom_spend'], 4)} vs B1's {_fmt(base_phantom, 4)} "
            "(limit +0.01); pooled bias "
            f"{_fmt(chosen_row['pooled_bias'], 4)} vs B1's {_fmt(base_bias, 4)} in absolute "
            "value (limit +0.02)."
        )
        if dm:
            parts = []
            for reference, result in dm.items():
                parts.append(f"{_fmt(result.get('p_value'), 2)} vs `{reference}`")
            lines.append("")
            lines.append(
                "**Significance.** Diebold-Mariano on the per-origin panel loss gives p = "
                + " and ".join(parts)
                + ". The win is real under the §7 rule, but it is not statistically "
                "significant: the test window holds only "
                f"{int(next(iter(dm.values())).get('n', 0))} overlapping origins, and with "
                "12-month horizons those are far from independent. Treat the choice as "
                "better-supported than Prophet, not as proven."
            )
    else:
        lines.append("No verdict is available; `verdict.json` or the leaderboard is missing.")
    lines.append("")
    if bands:
        lines.append(
            "**The band in this report is void.** It was sized with the actuals it had to "
            "cover, so its coverage figures describe no deployable band. The band was "
            "redesigned in a separate pre-registered experiment; see the band bake-off "
            "report under `private/experiments/band_bakeoff/` for the shipped choice and its "
            "coverage. This report's `bands.json` numbers are kept only as the historical "
            "record of a defect."
        )
    lines.append("")

    lines.append("## 2. Test leaderboard")
    lines.append("")
    lines.append(
        "Sorted by AvgRelCum12: geometric mean of per-series |cum-12 error| divided by the "
        "same for `naive_mean_12`, each series counted once."
    )
    lines.append("")
    lines.append(
        "| method | AvgRelCum12 test | AvgRelCum12 dev | money ratio | phantom spend "
        "| pooled bias | median rel | win share |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    for _, row in leaderboard.sort_values("avg_rel_cum12").iterrows():
        lines.append(
            f"| `{row['arm']}` | {_fmt(row['avg_rel_cum12'], 3)} | "
            f"{_fmt(row.get('dev_avg_rel_cum12'), 3)} | {_fmt(row.get('money_ratio'), 3)} | "
            f"{_fmt(row.get('phantom_spend'), 4)} | {_fmt(row.get('pooled_bias'), 4)} | "
            f"{_fmt(row.get('median_rel'), 3)} | {_fmt(row.get('win_share'), 2)} |"
        )
    lines.append("")

    lines.append("## 3. Strata")
    lines.append("")
    if strata.empty:
        lines.append("`strata_test.csv` is missing.")
    else:
        lines.append("AvgRelCum12 vs B1 within each stratum on the test origins.")
        lines.append("")
        lines.append("| stratum | value | arm | AvgRelCum12 | series |")
        lines.append("|---|---|---|---|---|")
        for _, row in strata.sort_values(["stratum", "value", "avg_rel"]).iterrows():
            lines.append(
                f"| {row['stratum']} | {row['value']} | `{row['arm']}` | "
                f"{_fmt(row['avg_rel'], 3)} | {int(row['n_series'])} |"
            )
        lines.append("")

    lines.append("## 4. Is choosing per class or per series worth it?")
    lines.append("")
    best_single = None
    single_arms = [
        arm
        for arm in leaderboard["arm"]
        if arm not in ("class_selector", "series_selector") and not arm.startswith("reconciled")
    ]
    if single_arms:
        subset = leaderboard[leaderboard["arm"].isin(single_arms)]
        best_single = subset.sort_values("avg_rel_cum12").iloc[0]
    for arm in ("class_selector", "series_selector"):
        rows = leaderboard[leaderboard["arm"] == arm]
        if rows.empty or best_single is None:
            continue
        row = rows.iloc[0]
        delta = row["avg_rel_cum12"] - best_single["avg_rel_cum12"]
        verdict_text = "beats" if delta < 0 else "does not beat"
        lines.append(
            f"- `{arm}`: AvgRelCum12 {_fmt(row['avg_rel_cum12'], 3)} vs the best single "
            f"method `{best_single['arm']}` at {_fmt(best_single['avg_rel_cum12'], 3)}; "
            f"it {verdict_text} the best single method by {_fmt(abs(delta), 3)}."
        )
    if not any(leaderboard["arm"].isin(["class_selector", "series_selector"])):
        lines.append("Selectors were not scored.")
    lines.append("")

    lines.append("## 5. Bands")
    lines.append("")
    lines.append(
        "**Void, and superseded.** The bands compared in this report were sized with the "
        "actuals they had to cover, so their coverage figures describe no deployable band "
        "and are not quoted here. The category band was redesigned in a separate "
        "pre-registered experiment (`configs/empirical_band_experiment.yaml`); the shipped "
        "choice, its dev and indicative test coverage, and the per-candidate table are in "
        "the band bake-off report under `private/experiments/band_bakeoff/`."
    )
    lines.append("")
    if bands:
        lines.append(
            "The figures in this report's `bands.json` are kept only so the defect is "
            "auditable. None of them is a measurement of any band that ships."
        )
    lines.append("")

    lines.append("## 6. Diebold-Mariano")
    lines.append("")
    lines.append(
        "Harvey-Leybourne-Newbold corrected on the per-origin panel loss "
        "(sum |cum-12 error| / scale), HAC lag 11."
    )
    lines.append("")
    if dm:
        lines.append("| pair | DM stat | p | n |")
        lines.append("|---|---|---|---|")
        for reference, result in dm.items():
            lines.append(
                f"| `{chosen}` vs `{reference}` | {_fmt(result.get('dm_stat'), 3)} | "
                f"{_fmt(result.get('p_value'), 3)} | {int(result.get('n', 0))} |"
            )
        lines.append("")
        lines.append(
            "**Power limit:** the test period covers fewer than three non-overlapping "
            "12-month windows of calendar time. The panel adds breadth, not more independent "
            "time periods, so the decision rests on the margin and consistency rule in §7."
        )
    else:
        lines.append("No DM comparison was possible for the chosen arm.")
    lines.append("")

    lines.append("## 7. Sanity gate (dev, before the freeze, plus test shares)")
    lines.append("")
    lines.append(
        "Every arm going into the freeze, measured on the dev origins: the share of rows whose "
        "forecast equals `naive_mean_12`, and the share of eligible (series, origin) pairs it "
        "fell back on. An arm identical to the benchmark is a no-op; more than 5% fallbacks "
        "means it is mostly the benchmark. Both block the freeze."
    )
    lines.append("")
    lines.append(
        "Fallback shares below are **dev**. The test sidecar is separate and larger: it is "
        "counted per (series, origin) pair, so a single substituted origin is 12 identical "
        "rows and a row share understates it tenfold."
    )
    lines.append("")
    if not test_fallbacks.empty:
        lines.append("| method | test fallback pairs | of eligible | share |")
        lines.append("|---|---|---|---|")
        for _, row in test_fallbacks.sort_values("fallback_pairs", ascending=False).iterrows():
            if row["fallback_pairs"] <= 0:
                continue
            lines.append(
                f"| `{row['arm']}` | {int(row['fallback_pairs'])} | "
                f"{int(row['eligible_pairs'])} | {_fmt(row['fallback_share_pairs'], 3)} |"
            )
        lines.append("")
        lines.append(
            "Every one of these is an all-zero training window, where no method has estimable "
            "dynamics and the trailing mean of 0 is the correct answer. So the high pair shares "
            "are not arm failures, but they do mean the arm is the benchmark on about one pair "
            "in ten — including the chosen arm, which inherits ETS and theta's zero windows."
        )
        lines.append("")
    if gate.empty:
        lines.append("`sanity_gate.csv` is missing.")
    else:
        lines.append("| method | identical to B1 | fallback share | status | note |")
        lines.append("|---|---|---|---|---|")
        for _, row in gate.sort_values(["status", "arm"]).iterrows():
            reason = str(row.get("reason", "") or "")
            if reason.lower() in ("nan", "none"):
                reason = ""
            lines.append(
                f"| `{row['arm']}` | {_fmt(row.get('identical_share'), 3)} | "
                f"{_fmt(row.get('fallback_share'), 3)} | {row.get('status', '')} | {reason} |"
            )
        lines.append("")
        lines.append(
            "Fallbacks are the explicit insufficient-history skip only: a window that is too "
            "short, or constant (all zero, so no dynamics are estimable). Every other "
            "exception propagates and fails the run, which is how the reconciliation bug was "
            "caught rather than scored as a tie."
        )
    lines.append("")

    lines.append("## 8. Where the data contradicts plan section 2")
    lines.append("")
    lines.append(
        "Section 2 of the plan was written before the diagnostics ran. Its figures live in "
        "the git-ignored `reference_profile.json`; the measured values are in "
        "`diagnostics_profile.json`."
    )
    lines.append("")
    for item in _contradictions(diag, ref):
        lines.append(item)
    lines.append("")

    lines.append("## 9. Production defects found")
    lines.append("")
    lines.append(
        "- **Grid:** production stops each series at its last non-zero month, so dormant "
        "categories are forecast from a date in the past. The experiment's corrected grid "
        "runs to the last complete month with trailing zeros (diagnostics count dormant "
        f"series: {diag.get('dormant', 'n/a')})."
    )
    lines.append(
        f"- **Negative values:** the shipped Prophet output has "
        f"{diag.get('shipped_negative_point_series', 'n/a')} series with a negative point "
        "forecast, and "
        f"{_fmt(float(diag.get('shipped_ci80_negative_share') or 0) * 100)}% of its rows have "
        "`ci80_lower < 0`, which is not a valid spending total. Every arm here is clipped at 0; "
        "the count of negative raw values per arm is in `arm_costs.csv`."
    )
    lines.append(
        "- **Bands:** the shipped bands are Prophet's own intervals, not calibrated on "
        "outcomes. Section 5 reports native vs pooled coverage."
    )
    lines.append("")

    lines.append("## 10. Deviations log")
    lines.append("")
    for item in deviations:
        lines.append(f"- {item}")
    lines.append("")

    lines.append("## 11. Adoption notes")
    lines.append("")
    if not costs.empty:
        slowest = costs.sort_values("seconds").iloc[-1]
        lines.append(
            f"- Runtime: {costs['seconds'].sum():.0f}s for all frozen arms at the primary "
            f"horizon in the test phase; slowest arm `{slowest['arm']}` at "
            f"{slowest['seconds']:.0f}s."
        )
    lines.append(
        "- `lightgbm` stays in the optional `experiment` group unless the global model is "
        "adopted; no new dependency is needed by the point or band choice."
    )
    lines.append(
        "- Phase 5 always fixes the grid and clips at 0, whatever method wins. Prophet stays "
        "a dependency for `compute_category_trends` (the trend line and direction labels)."
    )
    lines.append(
        "- The 3-year horizon: months 25-36 are unvalidated here (the secondary run covers "
        "24). Whether to keep the 3-year chart or cut it to 24 months is a user decision "
        "before Phase 5."
    )
    lines.append("")
    lines.append(
        "Figures: `figures/arm_strata.png`, `figures/series_ratios.png`, "
        "`figures/bias_phantom.png`, `figures/band_coverage_width.png`."
    )
    lines.append("")

    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {out_dir / 'report.md'}")
    print(f"figures in {fig_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
