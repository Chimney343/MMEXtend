"""
Phase 1 diagnostics for the savings-forecast bake-off.

Everything is computed from data available at the first test origin, so the
test period cannot leak into design choices (plan §6, Phase 1). Writes
``diagnostics.md`` and figures into the experiment output directory.

Usage:
    poetry run python scripts/experiments/savings_diagnostics.py \
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
from scipy import stats  # noqa: E402
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf  # noqa: E402
from statsmodels.tsa.seasonal import STL  # noqa: E402
from statsmodels.tsa.stattools import adfuller, kpss  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.analysis.feature_engineering import complete_cashflow_series  # noqa: E402


def _stl_strength(values: pd.Series) -> tuple[float, float]:
    """STL trend and seasonal strength, robust fit (plan §2 check)."""
    fit = STL(values, period=12, robust=True).fit()
    resid_var = float(np.var(fit.resid))
    seasonal = float(max(0.0, 1.0 - resid_var / np.var(fit.seasonal + fit.resid)))
    trend = float(max(0.0, 1.0 - resid_var / np.var(fit.trend + fit.resid)))
    return trend, seasonal


def _adf_kpss(values: pd.Series) -> dict:
    out = {}
    for name, series in (("level", values), ("diff", values.diff().dropna())):
        try:
            out[f"adf_p_{name}"] = float(adfuller(series, autolag="AIC")[1])
        except Exception:
            out[f"adf_p_{name}"] = float("nan")
        try:
            out[f"kpss_p_{name}"] = float(kpss(series, regression="c", nlags="auto")[1])
        except Exception:
            out[f"kpss_p_{name}"] = float("nan")
    return out


def _autocorr(values: pd.Series, lag: int) -> float:
    """Pearson autocorrelation at one lag (robust to pandas API changes)."""
    clean = values.dropna().to_numpy(dtype=float)
    if len(clean) <= lag:
        return float("nan")
    left, right = clean[lag:], clean[:-lag]
    if np.std(left) == 0 or np.std(right) == 0:
        return float("nan")
    return float(np.corrcoef(left, right)[0, 1])


def _mad_flags(values: pd.Series, window: int = 12, threshold: float = 3.5) -> pd.Series:
    median = values.rolling(window, center=True, min_periods=6).median()
    mad = (values - median).abs().rolling(window, center=True, min_periods=6).median()
    z = 0.6745 * (values - median) / mad.replace(0.0, np.nan)
    return z.abs() > threshold


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/forecast_experiment.yaml")
    args = parser.parse_args(argv)

    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    out_dir = Path(config["meta"]["output_dir"])
    fig_dir = out_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    split_frame = pd.read_parquet(config["data"]["cashflow_split"])
    series = complete_cashflow_series(split_frame)
    series.index = pd.DatetimeIndex(series.index).to_period("M").to_timestamp()

    initial = int(config["split"]["initial"])
    horizon = int(config["split"]["horizon_primary"])
    # First test origin = the origin before the last third of the origin grid.
    origin_positions = list(range(initial, len(series) - horizon + 1))
    first_test_position = origin_positions[
        int(np.floor(len(origin_positions) * (1 - 1 / 3)))
    ]
    cutoff_position = first_test_position
    visible = series.iloc[:cutoff_position]

    diffs = series.index.to_series().diff().dropna().dt.days
    regularity = {
        "inferred_freq": pd.infer_freq(series.index),
        "gap_days_min": int(diffs.min()),
        "gap_days_max": int(diffs.max()),
        "n_gaps_above_31d": int((diffs > 31).sum()),
    }

    stl_trend, stl_seasonal = _stl_strength(visible)
    stationarity = _adf_kpss(visible)

    flags = _mad_flags(visible)
    flagged_months = [str(ts.date()) for ts in visible.index[flags.fillna(False)]]

    rolling_mean = visible.rolling(12).mean()
    rolling_sd = visible.rolling(12).std()
    half = len(visible) // 2
    halves = {
        "mean_1": float(visible.iloc[:half].mean()),
        "mean_2": float(visible.iloc[half:].mean()),
        "sd_1": float(visible.iloc[:half].std(ddof=1)),
        "sd_2": float(visible.iloc[half:].std(ddof=1)),
    }

    components = split_frame[~split_frame["is_partial_month"].astype(bool)].copy()
    components.index = pd.to_datetime(components["year_month"] + "-01")
    components = components.reindex(visible.index)

    profile = {
        "as_of_last_complete_month": str(series.index[-1].date()),
        "n_complete_months_full": int(len(series)),
        "partial_months_excluded": split_frame.loc[
            split_frame["is_partial_month"], "year_month"
        ].tolist(),
        "diagnostics_window": {
            "n_months": int(len(visible)),
            "first": str(visible.index[0].date()),
            "last": str(visible.index[-1].date()),
            "first_test_origin": str(series.index[cutoff_position - 1].date()),
        },
        "regularity": regularity,
        "skew": float(stats.skew(visible)),
        "excess_kurtosis": float(stats.kurtosis(visible)),
        "cv": float(visible.std(ddof=1) / visible.mean()),
        "n_negative_months": int((visible < 0).sum()),
        "n_zero_months": int((visible == 0).sum()),
        "stationarity": stationarity,
        "stl_trend_strength": stl_trend,
        "stl_seasonal_strength": stl_seasonal,
        "acf_lag1": _autocorr(visible, 1),
        "acf_lag12": _autocorr(visible, 12),
        "mad_outlier_months": flagged_months,
        "mad_outlier_count": len(flagged_months),
        "halves": halves,
        "mean_ratio_first_to_second": halves["mean_1"] / halves["mean_2"],
        "sd_ratio_first_to_second": halves["sd_1"] / halves["sd_2"],
        "components": {
            "deposits_acf1": _autocorr(components["deposits"], 1),
            "withdrawals_acf1": _autocorr(components["withdrawals"], 1),
            "deposits_min": float(components["deposits"].min()),
            "withdrawals_min": float(components["withdrawals"].min()),
            "n_nonpositive_deposits": int((components["deposits"] <= 0).sum()),
            "n_nonpositive_withdrawals": int((components["withdrawals"] <= 0).sum()),
        },
        "zero_filled_months": "see the pipeline log for IMPUTATION lines",
    }

    (out_dir / "diagnostics_profile.json").write_text(
        json.dumps(profile, indent=2), encoding="utf-8"
    )

    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    axes[0].plot(visible.index, visible.to_numpy(), marker="o", ms=2.5, lw=1, label="savings")
    axes[0].plot(rolling_mean.index, rolling_mean.to_numpy(), lw=2, label="rolling mean (12)")
    axes[0].set_ylabel("monthly savings")
    axes[0].legend(loc="upper left")
    axes[0].set_title("Level and rolling 12-month mean (diagnostics window)")
    axes[1].plot(rolling_sd.index, rolling_sd.to_numpy(), lw=2, color="C3")
    axes[1].set_ylabel("rolling sd (12)")
    axes[1].set_xlabel("month")
    fig.tight_layout()
    fig.savefig(fig_dir / "regime_rolling_mean_sd.png", dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(11, 3.6))
    fit = STL(visible, period=12, robust=True).fit()
    ax.plot(visible.index, visible.to_numpy(), lw=1, label="y")
    ax.plot(visible.index, fit.trend, lw=2, label="trend")
    ax.plot(visible.index, fit.seasonal, lw=1.5, label="seasonal")
    ax.legend(loc="upper left")
    ax.set_title(f"STL (robust): trend strength {stl_trend:.2f}, seasonal {stl_seasonal:.2f}")
    fig.tight_layout()
    fig.savefig(fig_dir / "stl.png", dpi=130)
    plt.close(fig)

    fig, axes = plt.subplots(2, 1, figsize=(11, 6.2))
    plot_acf(visible.dropna(), lags=24, ax=axes[0], zero=True)
    axes[0].set_title("ACF (level), lags 1-24")
    plot_pacf(visible.dropna(), lags=24, ax=axes[1], method="ywm", zero=True)
    axes[1].set_title("PACF (level), lags 1-24")
    fig.tight_layout()
    fig.savefig(fig_dir / "acf_pacf.png", dpi=130)
    plt.close(fig)

    lines = [
        "# Diagnostics (Phase 1)",
        "",
        "Computed from months available at the first test origin, so no test-period",
        "actual informs any design choice.",
        "",
        f"- Window: {profile['diagnostics_window']['n_months']} months, "
        f"{profile['diagnostics_window']['first']} to {profile['diagnostics_window']['last']}",
        f"- First test origin: {profile['diagnostics_window']['first_test_origin']}",
        f"- Index regularity: inferred freq `{regularity['inferred_freq']}`, "
        f"gap range {regularity['gap_days_min']}-{regularity['gap_days_max']} days, "
        f"{regularity['n_gaps_above_31d']} gaps > 31 days",
        f"- Partial month(s) excluded by the pipeline: "
        f"{', '.join(profile['partial_months_excluded']) or 'none'}",
        "",
        "## Distribution",
        "",
        f"- Skew {profile['skew']:.2f}, excess kurtosis {profile['excess_kurtosis']:.2f}, "
        f"CV {profile['cv']:.2f}",
        f"- Negative months {profile['n_negative_months']}, zero months "
        f"{profile['n_zero_months']}",
        "",
        "## Stationarity",
        "",
        f"- ADF p (level) {stationarity['adf_p_level']:.3f}, "
        f"KPSS p (level) {stationarity['kpss_p_level']:.3f}",
        f"- ADF p (first difference) {stationarity['adf_p_diff']:.2e}, "
        f"KPSS p (first difference) {stationarity['kpss_p_diff']:.3f}",
        "",
        "## Seasonality and trend",
        "",
        f"- STL trend strength {stl_trend:.2f}, seasonal strength {stl_seasonal:.2f}",
        f"- ACF at lag 1 {profile['acf_lag1']:.2f}, at lag 12 {profile['acf_lag12']:.2f}",
        "",
        "## Regime",
        "",
        f"- First-half mean / second-half mean = {profile['mean_ratio_first_to_second']:.2f}",
        f"- First-half sd / second-half sd = {profile['sd_ratio_first_to_second']:.2f}",
        "",
        "## Outliers (rolling median/MAD, |z| > 3.5)",
        "",
        f"- {profile['mad_outlier_count']} flag(s): "
        f"{', '.join(flagged_months) or 'none'}",
        "",
        "## Components",
        "",
        f"- Deposits ACF1 {profile['components']['deposits_acf1']:.2f}, "
        f"withdrawals ACF1 {profile['components']['withdrawals_acf1']:.2f}",
        f"- Non-positive deposits {profile['components']['n_nonpositive_deposits']}, "
        f"non-positive withdrawals {profile['components']['n_nonpositive_withdrawals']}",
        "",
        "## Figures",
        "",
        "- `figures/regime_rolling_mean_sd.png`",
        "- `figures/stl.png`",
        "- `figures/acf_pacf.png`",
    ]
    (out_dir / "diagnostics.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(profile, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
