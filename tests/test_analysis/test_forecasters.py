"""Tests for src.analysis.forecasters — synthetic series only, no real data."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.forecasters import (
    ARMS,
    ArmRecorder,
    ArmSkipped,
    build_ml_design,
    clean_target,
    complexity_rank,
    make_arm,
    training_rows,
)


def synthetic_series(n: int = 84, seed: int = 7) -> pd.Series:
    """Trend + noise + negative months + a level shift, month-start index."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2016-01-01", periods=n, freq="MS")
    level = np.linspace(800.0, 2400.0, n)
    level[n // 2 :] += 600.0
    values = level + rng.normal(0.0, 320.0, n) + 120.0 * np.sin(np.arange(n) / 2.6)
    negatives = [position for position in (5, 17, 40) if position < n]
    values[negatives] = -450.0
    return pd.Series(values, index=idx, name="disposable_income")


def synthetic_components(n: int = 84, seed: int = 11) -> dict:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2016-01-01", periods=n, freq="MS")
    deposits = pd.Series(3200.0 + rng.normal(0, 90, n), index=idx)
    withdrawals = pd.Series(2400.0 + rng.normal(0, 110, n), index=idx)
    return {"deposits": deposits, "withdrawals": withdrawals}


ALL_ARMS = [
    "naive_mean_12",
    "naive_median_12",
    "random_walk",
    "seasonal_naive",
    "drift",
    "auto_ets",
    "auto_arima",
    "theta",
    "holt_winters_shipped",
    "decomposed_bu",
    "decomposed_mint",
    "ridge_direct",
    "lgbm_direct",
]


@pytest.mark.parametrize("arm", [a for a in ALL_ARMS if a != "lgbm_direct"])
def test_arm_contract(arm):
    train = synthetic_series()
    forecaster = make_arm(arm, components=synthetic_components())
    result = forecaster(train, 6)

    assert len(result) == 6
    assert list(result.columns[:5]) == [
        "month", "point_estimate", "ci80_lower", "ci80_upper", "method"
    ]
    assert result["month"].iloc[0] == train.index[-1] + pd.DateOffset(months=1)
    assert (result["month"].dt.day == 1).all()
    assert np.isfinite(result["point_estimate"]).all()

    again = forecaster(train, 6)
    pd.testing.assert_frame_equal(result, again)


def test_lgbm_arm_contract():
    pytest.importorskip("lightgbm")
    train = synthetic_series()
    forecaster = make_arm("lgbm_direct", components=synthetic_components(), n_estimators=100)
    result = forecaster(train, 4)
    assert len(result) == 4
    assert np.isfinite(result["point_estimate"]).all()
    pd.testing.assert_frame_equal(result, forecaster(train, 4))


@pytest.mark.parametrize(
    "arm",
    ["drift", "auto_ets", "auto_arima", "theta", "decomposed_bu", "decomposed_mint"],
)
def test_windowed_arm_accepts_short_window(arm):
    train = synthetic_series()
    forecaster = make_arm(arm, components=synthetic_components(), window=36)
    result = forecaster(train, 3)
    assert len(result) == 3
    assert np.isfinite(result["point_estimate"]).all()


def test_robust_variant_toggles_label_and_runs():
    train = synthetic_series()
    on = make_arm("robust__naive_mean_12", robust=True)
    off = make_arm("robust__naive_mean_12", robust=False)
    on_result = on(train, 3)
    off_result = off(train, 3)
    assert on_result["method"].iloc[0].startswith("robust_on")
    assert off_result["method"].iloc[0].startswith("robust_off")


def test_recorder_swallows_failures_and_counts_them():
    def boom(train, periods):
        raise RuntimeError("boom")

    recorder = ArmRecorder("boom", boom)
    result = recorder(synthetic_series(), 4)
    assert len(result) == 4
    assert result["point_estimate"].isna().all()
    assert recorder.failures == 1
    assert recorder.stats()["arm"] == "boom"


def test_recorder_counts_skips_separately():
    def skip(train, periods):
        raise ArmSkipped("nope")

    recorder = ArmRecorder("skip", skip)
    recorder(synthetic_series(), 2)
    assert recorder.skips == 1
    assert recorder.failures == 0


def test_clean_target_replaces_outliers_only():
    idx = pd.date_range("2016-01-01", periods=72, freq="MS")
    values = 1000.0 + 5.0 * np.arange(72) + 40.0 * np.sin(np.arange(72) / 2.0)
    train = pd.Series(values, index=idx)
    dirty = train.copy()
    dirty.iloc[30] = 1e6
    cleaned = clean_target(dirty)
    assert cleaned.iloc[30] < 1e6
    untouched = cleaned.drop(index=dirty.index[30])
    original = dirty.drop(index=dirty.index[30])
    pd.testing.assert_series_equal(untouched, original)


def test_clean_target_does_not_flag_a_level_shift():
    idx = pd.date_range("2016-01-01", periods=48, freq="MS")
    values = np.concatenate([np.full(24, 1000.0), np.full(24, 5000.0)])
    series = pd.Series(values, index=idx)
    cleaned = clean_target(series)
    pd.testing.assert_series_equal(cleaned, series)


def test_ml_design_ignores_the_future():
    train = synthetic_series(60)
    cut = 40
    poisoned = train.copy()
    poisoned.iloc[cut:] = 1e12

    base = build_ml_design(train)
    poisoned_design = build_ml_design(poisoned)

    overlap = base.index[:cut]
    pd.testing.assert_frame_equal(
        base.loc[overlap].drop(columns="__y"), poisoned_design.loc[overlap].drop(columns="__y")
    )


def test_ml_training_rows_never_target_beyond_train_end():
    train = synthetic_series(72)
    features = build_ml_design(train)
    design, target = training_rows(features, horizon=6, scale=False, min_rows=5)
    # The last usable origin is six months before the final training month.
    last_origin_position = train.index.get_loc(design.index[-1])
    assert last_origin_position + 6 == len(train) - 1
    assert len(design) == len(target)


def test_ml_arm_skips_when_rows_are_too_few():
    train = synthetic_series(24)
    forecaster = make_arm("ridge_direct")
    with pytest.raises(ArmSkipped):
        forecaster(train, 3)


def test_every_registered_arm_has_a_complexity_rank():
    for arm in ARMS:
        assert complexity_rank(arm) < 99.0
    assert complexity_rank("naive_mean_12") < complexity_rank("prophet_shipped")
    assert complexity_rank("auto_ets") < complexity_rank("auto_arima")
    assert complexity_rank("robust__naive_mean_12") > complexity_rank("naive_mean_12")


def test_drift_is_a_straight_line_with_the_average_slope():
    train = pd.Series(
        [10.0, 20.0, 30.0, 40.0], index=pd.date_range("2020-01-01", periods=4, freq="MS")
    )
    result = make_arm("drift")(train, 2)
    assert result["point_estimate"].tolist() == pytest.approx([50.0, 60.0])
