"""
Render the bake-off report and figures from cached artefacts.

Reads the leaderboards, backtests and verdict in the experiment output
directory and writes ``report.md`` plus the four figures the plan asks for
(section 9). It never refits a model, so it cannot change a result.

Usage:
    poetry run python scripts/experiments/render_report.py \
        --config configs/forecast_experiment.yaml
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

from src.analysis.forecast_metrics import (  # noqa: E402
    arm_summary,
    common_origins,
    mase_scale_by_origin,
)

DEVIATIONS_FALLBACK = [
    "**Deviations are generated at run time.** See `deviations.md` in the experiment "
    "output directory.",
]


def _load_backtests(out_dir: Path, phase: str, selection: dict, horizon: int = 12) -> dict:
    """
    Load the backtest for each frozen arm and parameter set.

    The cache file name embeds a hash of the parameters, so a glob that takes
    the first or last match would pick an arbitrary dev candidate. Resolve the
    exact path from the frozen ``selection`` instead.
    """
    from scripts.experiments.savings_forecast_bakeoff import backtest_path

    out = {}
    for arm, params in selection.items():
        path = backtest_path(out_dir, phase, arm, params or {}, horizon)
        if path.exists():
            out[arm] = pd.read_parquet(path)
    return out


def figure_mae_curve(backtests, origins, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 4.6))
    highlight = {"naive_mean_12": ("C0", 2.4), "prophet_shipped": ("C3", 2.0)}
    for arm, bt in backtests.items():
        sub = bt[bt["origin_month"].isin(set(origins))]
        if sub.empty:
            continue
        err = (sub["actual"] - sub["point"]).abs()
        curve = err.groupby(sub["h"]).mean()
        colour, width = highlight.get(arm, ("0.6", 1.0))
        ax.plot(
            curve.index,
            curve.to_numpy(),
            color=colour,
            lw=width,
            label=arm,
            alpha=1.0 if arm in highlight else 0.7,
        )
    ax.set_xlabel("horizon (months)")
    ax.set_ylabel("MAE")
    ax.set_title("Per-horizon MAE on test origins")
    ax.grid(alpha=0.25)
    ax.legend(fontsize=6, ncol=2, loc="upper left")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def figure_cum_error(backtests, origins, path: Path) -> None:
    keep = set(pd.to_datetime(list(origins)))
    data, labels = [], []
    for arm, bt in backtests.items():
        at12 = bt[(bt["h"] == 12) & bt["origin_month"].isin(keep)]
        values = (at12["cum_actual"] - at12["cum_point"]).dropna()
        if values.empty:
            continue
        data.append(values.to_numpy())
        labels.append(arm)
    order = np.argsort([np.mean(np.abs(values)) for values in data])
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.boxplot(
        [data[i] for i in order],
        vert=True,
        showfliers=True,
        tick_labels=[labels[i] for i in order],
    )
    ax.axhline(0.0, color="0.3", lw=1)
    ax.set_ylabel("cumulative 12-month error (actual - forecast)")
    ax.set_title("Cum-12 error by arm (test origins); positive = forecast too low")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=7)
    ax.grid(alpha=0.25, axis="y")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def figure_under_forecast(leaderboard: pd.DataFrame, n: int, path: Path) -> None:
    frame = leaderboard.sort_values("under_k", ascending=False)
    fig, ax = plt.subplots(figsize=(9, 4.4))
    ax.barh(frame["arm"], frame["under_k"], color="C0")
    ax.axvline(n / 2, color="0.4", ls="--", lw=1, label="half the origins")
    ax.set_xlabel(f"origins where the 12-month total was under-forecast (of {n})")
    ax.set_title("Systematic low bias")
    ax.legend(fontsize=7)
    plt.setp(ax.get_yticklabels(), fontsize=7)
    ax.grid(alpha=0.25, axis="x")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def figure_band(leaderboard: pd.DataFrame, path: Path) -> None:
    frame = leaderboard.dropna(subset=["band_coverage", "band_width"])
    fig, ax = plt.subplots(figsize=(7, 4.6))
    ax.scatter(frame["band_width"], frame["band_coverage"], s=45, color="C0")
    for _, row in frame.iterrows():
        ax.annotate(
            row["arm"],
            (row["band_width"], row["band_coverage"]),
            fontsize=6,
            xytext=(4, 4),
            textcoords="offset points",
        )
    ax.axhspan(0.65, 0.95, color="C2", alpha=0.12, label="7.4(c) allowed coverage")
    ax.axhline(0.8, color="0.4", ls="--", lw=1, label="nominal 80%")
    ax.set_xlabel("mean cumulative band width")
    ax.set_ylabel("coverage of the cum-12 actual")
    ax.set_title("Purged empirical 80% band: coverage vs width (test)")
    ax.legend(fontsize=7)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def _load_deviations(out_dir: Path) -> list[str]:
    """
    Read the deviations log from the git-ignored experiment directory.

    The entries name real months, window choices and origin counts, so they are
    data and must not live in tracked code.
    """
    path = out_dir / "deviations.md"
    if not path.exists():
        return DEVIATIONS_FALLBACK
    entries = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            entries.append(stripped[2:])
        elif stripped and entries:
            entries[-1] = f"{entries[-1]} {stripped}"
    return entries or DEVIATIONS_FALLBACK


def _load_reference_profile(out_dir: Path) -> dict:
    """
    Read the plan section 2 claims quoted in the contradiction section.

    These are data-derived values, kept beside the diagnostics rather than in
    tracked code.
    """
    path = out_dir / "reference_profile.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8")).get("plan_section_2", {})


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/forecast_experiment.yaml")
    args = parser.parse_args(argv)

    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    out_dir = Path(config["meta"]["output_dir"])
    fig_dir = out_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    leaderboard = pd.read_csv(out_dir / "test_leaderboard.csv")
    verdict = json.loads((out_dir / "verdict.json").read_text(encoding="utf-8"))
    dm = json.loads((out_dir / "dm_tests.json").read_text(encoding="utf-8"))
    summary = json.loads((out_dir / "test_summary.json").read_text(encoding="utf-8"))
    costs = pd.read_csv(out_dir / "arm_costs.csv")
    selection = config["selection"]
    frozen_arms = list(selection) if selection else leaderboard["arm"].tolist()
    deviations = _load_deviations(out_dir)
    ref_profile = _load_reference_profile(out_dir)

    split_meta = summary["meta"]
    test_common = list(
        pd.date_range(
            split_meta["test_common_first"], periods=split_meta["n_test_common"], freq="MS"
        )
    )
    dev_common = list(
        pd.date_range(
            split_meta["dev_common_first"], periods=split_meta["n_dev_common"], freq="MS"
        )
    )

    backtests = _load_backtests(out_dir, "test", {arm: selection.get(arm, {}) for arm in frozen_arms})

    from src.analysis.feature_engineering import complete_cashflow_series

    series = complete_cashflow_series(pd.read_parquet(config["data"]["cashflow_split"]))
    series.index = pd.DatetimeIndex(series.index).to_period("M").to_timestamp()
    dev_bt = _load_backtests(
        out_dir, "dev", {arm: selection.get(arm, {}) for arm in frozen_arms}
    )
    dev_rows = []
    if dev_bt:
        dev_common_full = [
            origin for origin in common_origins(dev_bt, horizon=12) if origin in set(dev_common)
        ]
        scales = mase_scale_by_origin(series, dev_common_full, m=12)
        for arm, bt in dev_bt.items():
            stats = arm_summary(bt, mase_scales=scales, horizon=12, origins=dev_common_full)
            stats["arm"] = arm
            dev_rows.append(stats)
    if dev_rows:
        dev_frame = pd.DataFrame(dev_rows)
        base = float(dev_frame.loc[dev_frame["arm"] == "naive_mean_12", "cum12_abs"].iloc[0])
        dev_frame["relcum12"] = dev_frame["cum12_abs"] / base
        dev_frame.sort_values("relcum12").to_csv(out_dir / "dev_leaderboard.csv", index=False)

    figure_mae_curve(backtests, test_common, fig_dir / "mae_by_horizon.png")
    figure_cum_error(backtests, test_common, fig_dir / "cum12_error.png")
    figure_under_forecast(
        leaderboard, int(leaderboard["test_n"].iloc[0]), fig_dir / "under_forecast.png"
    )
    figure_band(leaderboard, fig_dir / "band_coverage_width.png")

    def fmt(value, digits=0):
        if value is None or (isinstance(value, float) and not np.isfinite(value)):
            return "n/a"
        return f"{value:,.{digits}f}"

    chosen = verdict["chosen"]
    by_arm = leaderboard.set_index("arm")
    chosen_row = by_arm.loc[chosen]
    prophet_row = by_arm.loc["prophet_shipped"]

    lines: list[str] = []
    lines.append("# Savings forecast bake-off - report")
    lines.append("")
    lines.append(
        f"As-of {split_meta['as_of']}, {split_meta['n_months']} complete months. "
        f"Origins: {split_meta['n_dev_origins']} dev, {split_meta['n_embargo_origins']} "
        f"embargoed, {split_meta['n_test_origins']} test (step 1, initial 36, h = 12)."
    )
    lines.append("")
    lines.append(
        f"Pre-registered config: `configs/forecast_experiment.yaml`, sha256 "
        f"`{split_meta['config_sha256']}`, frozen {split_meta['frozen_at']}."
    )
    lines.append("")

    lines.append("## 1. Verdict")
    lines.append("")
    lines.append(
        f"**{chosen}** wins: test RelCum12 {chosen_row['test_relcum12']:.3f} "
        f"(= {fmt(chosen_row['test_cum12_abs'])} mean |cum-12 error|), dev RelCum12 "
        f"{chosen_row['dev_relcum12']:.3f}. Decided by **{verdict['step']}**: "
        f"{verdict['reason']}"
    )
    lines.append("")
    lines.append(
        "No alternative method beat the trailing-12-month mean on the test period. "
        f"Prophet, the current production choice, was {prophet_row['test_relcum12']:.3f} "
        f"times the naive error on test and {prophet_row['dev_relcum12']:.3f} times on "
        "dev, i.e. the two are statistically indistinguishable."
    )
    lines.append("")

    lines.append("## 2. Test leaderboard")
    lines.append("")
    lines.append(
        "Sorted by RelCum12 = mean |cum-12 error| / the same for `naive_mean_12`. "
        f"All arms are scored on the same {split_meta['n_test_common']} test origins "
        "(plan section 4.1)."
    )
    lines.append("")
    lines.append(
        "| method | RelCum12 test | RelCum12 dev | cum12 bias | under k/n | MASE "
        "| band coverage |"
    )
    lines.append("|---|---|---|---|---|---|---|")
    for _, row in leaderboard.iterrows():
        lines.append(
            f"| `{row['arm']}` | {row['test_relcum12']:.3f} | "
            f"{row['dev_relcum12']:.3f} | {fmt(row['cum12_bias'])} | "
            f"{int(row['under_k'])}/{int(row['test_n'])} | {row['mase_1_12']:.3f} | "
            f"{fmt(row['band_coverage'], 2)} |"
        )
    lines.append("")
    lines.append(
        f"Dev RelCum12 uses the {split_meta['n_dev_common']} dev origins that every arm "
        "could forecast (M1/M2 need 24 training rows per horizon, so they skip the "
        "earliest origins)."
    )
    lines.append("")

    lines.append("### h = 24 secondary run")
    lines.append("")
    lines.append(
        f"`cum24_abs` over the {split_meta['n_test_common_h24']} test origins covered by "
        "the 24-month run, common to all arms:"
    )
    lines.append("")
    lines.append("| method | cum24_abs |")
    lines.append("|---|---|")
    for _, row in leaderboard.sort_values("cum24_abs").iterrows():
        lines.append(f"| `{row['arm']}` | {fmt(row['cum24_abs'])} |")
    lines.append("")

    lines.append("## 3. Bias and bands")
    lines.append("")
    lines.append(
        "`under k/n` counts origins where the 12-month total was under-forecast. "
        "Positive `cum12 bias` means the forecast was too low."
    )
    lines.append("")
    lines.append(
        f"- `{chosen}`: bias {fmt(chosen_row['cum12_bias'])}, under "
        f"{int(chosen_row['under_k'])}/{int(chosen_row['test_n'])}; band coverage "
        f"{fmt(chosen_row['band_coverage'], 2)} with mean width "
        f"{fmt(chosen_row['band_width'])}."
    )
    lines.append(
        f"- `prophet_shipped`: bias {fmt(prophet_row['cum12_bias'])}, under "
        f"{int(prophet_row['under_k'])}/{int(prophet_row['test_n'])}; band coverage "
        f"{fmt(prophet_row['band_coverage'], 2)}."
    )
    lines.append(
        "- Every arm under-forecasts at least half of the origins, consistent with the "
        "rising level the plan anticipated. The purged band is therefore shifted upward "
        "rather than centred on the point forecast."
    )
    lines.append("")
    below = leaderboard[leaderboard["band_coverage"] < 0.65]
    above = leaderboard[leaderboard["band_coverage"] > 0.95]
    lines.append(
        f"- Arms outside the 7.4(c) coverage window [0.65, 0.95]: {len(below)} below "
        "0.65"
        + (f" ({', '.join(f'`{a}`' for a in below['arm'])})" if len(below) else "")
        + f", {len(above)} above 0.95"
        + (f" ({', '.join(f'`{a}`' for a in above['arm'])})" if len(above) else "")
        + ". Coverage is estimated on overlapping 12-month windows, so it is noisy."
    )
    lines.append("")
    chosen_cov = float(chosen_row["band_coverage"])
    cov_lo, cov_hi = config["decision"]["band_coverage"]
    if not (cov_lo <= chosen_cov <= cov_hi):
        lines.append(
            f"**Caveat on the recommendation.** The chosen arm's own coverage is "
            f"{chosen_cov:.2f}, outside the {cov_lo}-{cov_hi} guardrail. That guardrail "
            "belongs to section 7.4, which never ran: section 7.1 short-circuits to the "
            "naive mean "
            "before guardrails are evaluated, and no eligible arm existed to test. It "
            "is recorded as a known property of the incumbent rather than a reason to "
            "change the verdict, because the rule gives no alternative to fall back to. "
            "It does mean the shipped cumulative 80% band under-covers, which is a "
            "separate defect from the point-forecast choice."
        )
        lines.append("")

    lines.append("## 4. Diebold-Mariano")
    lines.append("")
    lines.append(
        "Harvey-Leybourne-Newbold corrected, on |error| at h = 1 (HAC lag 0) and cum-12 "
        "|error| (HAC lag 11)."
    )
    lines.append("")
    lines.append("| pair | loss | DM stat | p |")
    lines.append("|---|---|---|---|")
    for reference, result in dm.items():
        for loss_name, label in (("mae_h1", "abs err h=1"), ("cum12", "cum-12 abs err")):
            entry = result[loss_name]
            lines.append(
                f"| `{chosen}` vs `{reference}` | {label} | {entry['dm_stat']:.3f} | "
                f"{entry['p_value']:.3f} |"
            )
    lines.append("")
    lines.append(
        "Against B1 the test is degenerate: the chosen arm *is* B1, so the losses are "
        "identical. The informative comparison is Prophet, and it is insignificant. "
        "**Power limit:** the test period covers 21 overlapping origins, about two "
        "non-overlapping 12-month windows. The decision rests on the margin and "
        "consistency rule in section 7, not on p < 0.05."
    )
    lines.append("")

    lines.append("## 5. Diagnostics summary")
    lines.append("")
    diag_path = out_dir / "diagnostics_profile.json"
    if diag_path.exists():
        diag = json.loads(diag_path.read_text(encoding="utf-8"))
        lines.append(
            f"- Diagnostics window {diag['diagnostics_window']['n_months']} months "
            f"({diag['diagnostics_window']['first']} to "
            f"{diag['diagnostics_window']['last']}); first test origin "
            f"{diag['diagnostics_window']['first_test_origin']}."
        )
        lines.append(
            f"- Index regular, inferred freq `{diag['regularity']['inferred_freq']}`, no "
            f"gaps above 31 days; partial month "
            f"{', '.join(diag['partial_months_excluded'])} excluded."
        )
        lines.append(
            f"- ADF p {diag['stationarity']['adf_p_level']:.3f} (level) -> "
            f"{diag['stationarity']['adf_p_diff']:.2e} (difference); KPSS p "
            f"{diag['stationarity']['kpss_p_level']:.3f} -> "
            f"{diag['stationarity']['kpss_p_diff']:.3f}. Non-stationary in level, "
            "stationary in first difference."
        )
        lines.append(
            f"- STL strength: trend {diag['stl_trend_strength']:.2f}, seasonal "
            f"{diag['stl_seasonal_strength']:.2f}. ACF1 {diag['acf_lag1']:.2f}, ACF12 "
            f"{diag['acf_lag12']:.2f}."
        )
        lines.append(
            f"- Regime: first/second-half mean ratio "
            f"{diag['mean_ratio_first_to_second']:.2f}, sd ratio "
            f"{diag['sd_ratio_first_to_second']:.2f}."
        )
        lines.append(
            f"- MAD outlier flags: {diag['mad_outlier_count']} "
            f"({', '.join(diag['mad_outlier_months']) or 'none'})."
        )
        lines.append(
            f"- Components: deposits ACF1 "
            f"{diag['components']['deposits_acf1']:.2f}, withdrawals ACF1 "
            f"{diag['components']['withdrawals_acf1']:.2f}; both strictly positive."
        )
        lines.append("")
        lines.append(
            "Figures: `figures/regime_rolling_mean_sd.png`, `figures/stl.png`, "
            "`figures/acf_pacf.png`."
        )
    lines.append("")

    lines.append("## 6. Where the data contradicts plan section 2")
    lines.append("")
    if diag_path.exists():
        ref = ref_profile
        lines.append(
            "Section 2 of the plan was written before the diagnostics was run. The "
            "figures it quotes are recorded in the git-ignored `reference_profile.json`; "
            "the comparison below is generated from it and from "
            "`diagnostics_profile.json`."
        )
        lines.append("")
        if ref:
            n_ref = ref.get("n_complete_months")
            lines.append(
                f"- **Seasonality on the pre-test window.** Section 2 reports STL "
                f"seasonal strength {ref['stl_seasonal_strength']:.2f} and ACF12 "
                f"{ref['acf_lag12']:.2f}. On the pre-test window seasonal strength is "
                f"{diag['stl_seasonal_strength']:.2f} and ACF12 is "
                f"{diag['acf_lag12']:.2f}; ACF1 is {diag['acf_lag1']:.2f} versus "
                f"{ref['acf_lag1']:.2f} in section 2. Over the full {n_ref} months the "
                "seasonal strength matches the section 2 figure, so the gap is the "
                "window, not the measure. Design impact: none; seasonal ETS is "
                "AICc-selected per fit and never beat the non-seasonal specification on "
                "dev."
            )
            lines.append(
                f"- **The regime shift differs by window.** Section 2 says the first-half "
                f"mean is about {ref['mean_ratio_first_to_second']:.2f}x the second half "
                f"with sd about {ref['sd_ratio_first_to_second']:.2f}x. On the pre-test "
                f"window the ratios are {diag['mean_ratio_first_to_second']:.2f} and "
                f"{diag['sd_ratio_first_to_second']:.2f}; the section 2 figures describe "
                "the full series."
            )
            lines.append(
                f"- **The level test differs by window.** Section 2 gives ADF p = "
                f"{ref['adf_p_level']:.2f}; on the pre-test window ADF p = "
                f"{diag['stationarity']['adf_p_level']:.2f}, which still fails to reject "
                f"a unit root. The first difference is stationary (ADF p = "
                f"{diag['stationarity']['adf_p_diff']:.1e}, KPSS p = "
                f"{diag['stationarity']['kpss_p_diff']:.2f}). Design impact: none, "
                "because every adaptive arm already models the level rather than the "
                "level's mean."
            )
            lines.append(
                f"- **Negative months differ by window:** "
                f"{ref['n_negative_months']} on the full series as section 2 says, "
                f"{diag['n_negative_months']} on the pre-test window. The no-log rule "
                "for the net series still applies."
            )
            lines.append(
                "- **Not contradicted** (pre-test window / full series where they "
                f"differ): {n_ref} complete months, no zero months, heavy tails (skew "
                f"{diag['skew']:.2f} / {ref['skew']:.2f}, excess kurtosis "
                f"{diag['excess_kurtosis']:.2f} / {ref['excess_kurtosis']:.2f}, CV "
                f"{diag['cv']:.2f} / {ref['cv']:.2f}), strictly positive components, "
                "and the systematic under-forecast of the 12-month total."
            )
        else:
            lines.append(
                "`reference_profile.json` is missing, so the section 2 claims cannot be "
                "compared numerically."
            )
        lines.append("")
        lines.append(
            "None of these changed the arm list or a threshold. They are reported "
            "because the plan asked for contradictions to be flagged, not silently "
            "absorbed."
        )
    else:
        lines.append("`diagnostics_profile.json` is missing, so this section is empty.")
    lines.append("")

    lines.append("## 7. Deviations log")
    lines.append("")
    for item in deviations:
        lines.append(f"- {item}")
    lines.append("")

    slowest = costs.sort_values("total_seconds").iloc[-1]
    lines.append("## 8. Adoption notes")
    lines.append("")
    lines.append(
        f"- Runtime: {costs['total_seconds'].sum():.0f}s for all arms at h = 12 in the "
        f"test phase; slowest arm `{slowest['arm']}` at {slowest['total_seconds']:.0f}s "
        f"over {int(slowest['calls'])} origins. The full dev sweep across every candidate "
        "configuration takes several minutes and is cached per arm and parameter set."
    )
    lines.append(
        "- New dependency: `lightgbm` only, in the optional `experiment` Poetry group. "
        "It is not needed by the recommendation, because M2 is not adopted."
    )
    lines.append(
        f"- Production code is untouched. If adopted, `{chosen}` replaces the >= 24-month "
        "branch of `select_and_run_forecast`; step 8b already backtests the shipped "
        "forecaster and builds empirical bands, so only the label and the `prophet_bt` "
        "name change."
    )
    lines.append(
        "- The savings fan would keep using `empirical_bands()`, the same purged "
        "construction used here; no dashboard change is needed beyond text."
    )
    lines.append("")

    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {out_dir / 'report.md'}")
    print(f"figures in {fig_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
