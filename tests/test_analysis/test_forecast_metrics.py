"""Tests for src.analysis.forecast_metrics — hand-computed values only."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.forecast_metrics import (
    arm_summary,
    common_origins,
    dm_test_hln,
    mase_scale_by_origin,
)


def _bt(origins, actual, point, horizon=3):
    """Build a long backtest frame from per-origin actual/point matrices."""
    rows = []
    for i, origin in enumerate(pd.to_datetime(origins)):
        for h in range(1, horizon + 1):
            rows.append(
                {
                    "origin_month": origin,
                    "h": h,
                    "month": origin + pd.DateOffset(months=h),
                    "actual": float(actual[i][h - 1]),
                    "point": float(point[i][h - 1]),
                }
            )
    frame = pd.DataFrame(rows)
    frame["cum_actual"] = frame.groupby("origin_month")["actual"].cumsum()
    frame["cum_point"] = frame.groupby("origin_month")["point"].cumsum()
    return frame


def test_mase_denominator_is_the_in_sample_seasonal_naive_mae():
    idx = pd.date_range("2020-01-01", periods=14, freq="MS")
    values = np.arange(1.0, 15.0)
    series = pd.Series(values, index=idx)
    scale = mase_scale_by_origin(series, [idx[-1]], m=12)[idx[-1]]
    # |y_13 - y_1| = 12, |y_14 - y_2| = 12 → mean 12
    assert scale == pytest.approx(12.0)


def test_mase_is_nan_when_the_window_is_shorter_than_the_season():
    idx = pd.date_range("2020-01-01", periods=8, freq="MS")
    series = pd.Series(np.arange(8.0), index=idx)
    assert np.isnan(mase_scale_by_origin(series, [idx[-1]], m=12)[idx[-1]])


def test_wape_and_cumulative_error_match_hand_computation():
    origins = pd.date_range("2020-01-01", periods=2, freq="MS")
    actual = [[10.0, 20.0, 30.0], [12.0, 22.0, 32.0]]
    point = [[8.0, 18.0, 28.0], [11.0, 21.0, 31.0]]
    bt = _bt(origins, actual, point)

    summary = arm_summary(bt, horizon=3)
    errors = np.abs(np.array(actual) - np.array(point))
    assert summary["wape_1_12"] == pytest.approx(errors.sum() / np.abs(actual).sum())
    assert summary["cum12_abs"] == pytest.approx(np.mean([6.0, 3.0]))
    assert summary["cum12_bias"] == pytest.approx(np.mean([6.0, 3.0]))
    assert summary["under_k"] == 2
    assert summary["mae_h1"] == pytest.approx(1.5)
    assert summary["mae_h3"] == pytest.approx(1.5)
    assert summary["mae_h12"] != summary["mae_h12"]  # no h=12 row → NaN


def test_under_k_counts_only_origins_where_the_forecast_was_too_low():
    origins = pd.date_range("2020-01-01", periods=3, freq="MS")
    bt = _bt(origins, [[10.0], [10.0], [10.0]], [[9.0], [11.0], [-5.0]], horizon=1)
    summary = arm_summary(bt, horizon=1)
    assert summary["under_k"] == 2
    assert summary["cum12_bias"] == pytest.approx((1.0 - 1.0 + 15.0) / 3)


def test_pinball_loss_matches_hand_values():
    from src.analysis.forecast_metrics import _pinball

    # tau=0.1, actual above the quantile: loss = 0.1 * delta
    assert _pinball(0.1, 110.0, 100.0) == pytest.approx(1.0)
    # tau=0.9, actual below the quantile: loss = (0.9 - 1) * delta = 0.1 * |delta|
    assert _pinball(0.9, 90.0, 100.0) == pytest.approx(1.0)
    # tau=0.9, actual above the quantile: loss = 0.9 * delta
    assert _pinball(0.9, 120.0, 100.0) == pytest.approx(18.0)


def test_purged_band_uses_only_fully_realised_origins():
    origins = pd.date_range("2018-01-01", periods=14, freq="MS")
    # Errors grow with the origin index so quantiles depend on which rows count.
    actual = [[100.0 + i] for i in range(14)]
    point = [[100.0] for i in range(14)]
    bt = _bt(origins, actual, point, horizon=1)

    from src.analysis.forecast_metrics import purged_band_scores

    band = purged_band_scores(bt, horizon=1, alpha=0.2, min_origins=1)
    # Origin 13 may only use origins with o' + 1 <= o, i.e. 0..12 (13 rows).
    last = band[band["origin_month"] == origins[13]].iloc[0]
    assert last["n_prior"] == 13
    assert last["lower"] == pytest.approx(100.0 + np.quantile(np.arange(13.0), 0.1))


def test_purged_band_needs_min_origins():
    origins = pd.date_range("2018-01-01", periods=6, freq="MS")
    bt = _bt(origins, [[10.0]] * 6, [[10.0]] * 6, horizon=1)

    from src.analysis.forecast_metrics import purged_band_scores

    assert purged_band_scores(bt, horizon=1, min_origins=10).empty


def test_purged_band_excludes_origins_that_are_not_yet_realised():
    origins = pd.date_range("2018-01-01", periods=10, freq="MS")
    bt = _bt(origins, [[10.0]] * 10, [[9.0]] * 10, horizon=12 // 12)
    from src.analysis.forecast_metrics import purged_band_scores

    band = purged_band_scores(bt, horizon=1, min_origins=1)
    for _, row in band.iterrows():
        assert row["n_prior"] <= len(origins) - 1


def test_dm_identical_losses_give_zero_and_p_one():
    losses = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    result = dm_test_hln(losses, losses, h=1, hac_lags=0)
    assert result["dm_stat"] == pytest.approx(0.0)
    assert result["p_value"] == pytest.approx(1.0)


def test_dm_clearly_dominated_loss_gives_a_small_p():
    rng = np.random.default_rng(3)
    good = rng.normal(1.0, 0.05, 40)
    bad = rng.normal(5.0, 0.05, 40)
    result = dm_test_hln(bad, good, h=1, hac_lags=0)
    assert result["p_value"] < 1e-6
    assert result["dm_stat"] > 0  # first model is worse


def test_common_origins_intersects_and_requires_finite_points():
    origins = pd.date_range("2020-01-01", periods=5, freq="MS")
    a = _bt(origins, [[10.0]] * 5, [[10.0]] * 5, horizon=1)
    b = _bt(origins[1:], [[10.0]] * 4, [[10.0]] * 4, horizon=1)
    b.loc[b["origin_month"] == origins[1], "point"] = np.nan
    assert common_origins({"a": a, "b": b}, horizon=1) == list(origins[2:])
    assert common_origins({"a": a, "b": pd.DataFrame()}, horizon=1) == []
