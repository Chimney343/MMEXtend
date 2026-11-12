"""Tests for src.analysis.forecast_backtest."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.forecast_backtest import (
    empirical_bands,
    naive_mean_forecaster,
    rolling_origin_backtest,
    summarise_backtest,
)


def _series(n: int = 83) -> pd.Series:
    idx = pd.date_range("2021-03-01", periods=n, freq="MS")
    return pd.Series(np.sin(np.arange(n) / 3.0) * 1000.0 + 2000.0, index=idx)


def test_naive_mean_forecaster_shape():
    train = _series(40)
    result = naive_mean_forecaster(train, 6)
    assert len(result) == 6
    assert (result["point_estimate"] == train.iloc[-12:].mean()).all()
    assert result["month"].iloc[0] == train.index[-1] + pd.DateOffset(months=1)
    assert result["ci80_lower"].isna().all()
    assert (result["method"] == "naive_mean").all()


def test_backtest_origin_and_horizon_arithmetic():
    series = _series(60)
    bt = rolling_origin_backtest(series, naive_mean_forecaster, initial=36, horizon=12, step=3)

    origins = sorted(bt["origin_month"].unique())
    # origins 36, 39, 42, 45, 48 → 5 origins for n=60, horizon=12
    assert len(origins) == 5
    assert origins[0] == series.index[35]
    assert bt["h"].max() == 12
    assert len(bt) == 5 * 12
    first = bt[bt["origin_month"] == origins[0]]
    assert first["month"].iloc[0] == series.index[36]
    assert first["cum_actual"].iloc[-1] == pytest.approx(
        series.iloc[36:48].sum()
    )


def test_backtest_returns_empty_when_too_short():
    bt = rolling_origin_backtest(_series(40), naive_mean_forecaster, initial=36, horizon=24)
    assert bt.empty


def test_summarise_backtest_known_values():
    bt = pd.DataFrame(
        {
            "origin_month": pd.to_datetime(["2020-01-01"] * 3 + ["2020-02-01"] * 3),
            "h": [1, 2, 3, 1, 2, 3],
            "month": pd.to_datetime(["2020-02-01", "2020-03-01", "2020-04-01"] * 2),
            "actual": [10.0, 20.0, 30.0, 12.0, 22.0, 32.0],
            "point": [8.0, 18.0, 28.0, 11.0, 21.0, 31.0],
            "cum_actual": [10.0, 30.0, 60.0, 12.0, 34.0, 66.0],
            "cum_point": [8.0, 26.0, 54.0, 11.0, 32.0, 63.0],
            "ci80_lower": [5.0, 10.0, 15.0, 5.0, 10.0, 15.0],
            "ci80_upper": [15.0, 25.0, 40.0, 15.0, 25.0, 40.0],
        }
    )
    summary = summarise_backtest(bt)
    assert summary["n_origins"] == 2
    assert summary["mae_month"] == pytest.approx(np.mean([2.0, 2.0, 2.0, 1.0, 1.0, 1.0]))
    assert summary["model_ci80_coverage_month"] == pytest.approx(1.0)
    assert pd.isna(summary["abs_cum_err_12"])  # no h == 12 rows
    # One-origin split for n=2: floor(2*2/3) = 1 train origin, 1 holdout.
    # A single train error gives a degenerate [x, x] band, so the holdout
    # origin cannot fall inside it: the rate is 0/1.
    assert summary["holdout_cum80_m"] == 1
    assert summary["holdout_cum80_k"] == 0


def test_summarise_backtest_holdout_uses_train_quantiles():
    origins = pd.date_range("2020-01-01", periods=6, freq="MS")
    errors = [-40.0, -30.0, -20.0, -10.0, 10.0, 20.0]
    rows = []
    for origin, error in zip(origins, errors):
        rows.append(
            {
                "origin_month": origin,
                "h": 1,
                "month": origin + pd.DateOffset(months=1),
                "actual": 100.0 + error,
                "point": 100.0,
                "cum_actual": 100.0 + error,
                "cum_point": 100.0,
                "ci80_lower": 50.0,
                "ci80_upper": 150.0,
            }
        )
    summary = summarise_backtest(pd.DataFrame(rows))
    # First four origins estimate the band: q10 = -30, q90 = 7.5.
    # Last two origins have errors +10 and +20 → both outside → 0/2.
    assert summary["holdout_cum80_m"] == 2
    assert summary["holdout_cum80_k"] == 0


def test_empirical_bands_hand_computed_quantiles():
    bt = pd.DataFrame(
        {
            "origin_month": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
            "h": [1, 1, 1],
            "month": pd.to_datetime(["2020-02-01"] * 3),
            "actual": [110.0, 90.0, 100.0],
            "point": [100.0, 100.0, 100.0],
            "cum_actual": [110.0, 90.0, 100.0],
            "cum_point": [100.0, 100.0, 100.0],
        }
    )
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-01-01", periods=1, freq="MS"),
            "point_estimate": [200.0],
            "ci80_lower": [0.0],
            "ci80_upper": [400.0],
            "ci95_lower": [0.0],
            "ci95_upper": [400.0],
            "method": ["Prophet"],
        }
    )
    out = empirical_bands(bt, forecast)

    low, high = np.quantile([10.0, -10.0, 0.0], [0.1, 0.9])
    assert out["ci80_lower"].iloc[0] == pytest.approx(200.0 + low)
    assert out["ci80_upper"].iloc[0] == pytest.approx(200.0 + high)
    assert out["cum_point"].iloc[0] == 200.0
    assert out["cum_ci80_lower"].iloc[0] == pytest.approx(200.0 + low)
    assert out["cum_ci80_upper"].iloc[0] == pytest.approx(200.0 + high)
    assert out["ci95_lower"].isna().all()
    assert out["ci95_upper"].isna().all()
    assert out["cum_ci95_lower"].isna().all()
    assert out["interval_method"].iloc[0] == "empirical_backtest_n3"


def test_empirical_bands_uses_cumulative_point_for_cumulative_band():
    bt = pd.DataFrame(
        {
            "origin_month": pd.to_datetime(["2020-01-01", "2020-02-01"]),
            "h": [1, 1],
            "month": pd.to_datetime(["2020-02-01"] * 2),
            "actual": [100.0, 100.0],
            "point": [100.0, 100.0],
            "cum_actual": [100.0, 100.0],
            "cum_point": [90.0, 110.0],
        }
    )
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-01-01", periods=2, freq="MS"),
            "point_estimate": [50.0, 60.0],
        }
    )
    out = empirical_bands(bt, forecast)
    assert out["cum_point"].tolist() == [50.0, 110.0]
    # errors are [+10, -10] at h=1 only; h=2 has no backtest rows → NaN
    assert out["cum_ci80_lower"].iloc[1] != out["cum_ci80_lower"].iloc[1]  # NaN


def test_empirical_bands_counts_backtests_that_came_in_low():
    # h=1: cumulative errors +10, +5, -3 → 2 of 3 below the actual.
    # h=2: errors +20, +15, +8 → 3 of 3, so the band sits wholly above the
    # point forecast.
    origins = pd.to_datetime(["2020-01-01", "2020-04-01", "2020-07-01"])
    rows = []
    for origin, e1, e2 in zip(origins, [10.0, 5.0, -3.0], [20.0, 15.0, 8.0]):
        for h, err in ((1, e1), (2, e2)):
            rows.append(
                {
                    "origin_month": origin,
                    "h": h,
                    "month": origin + pd.DateOffset(months=h),
                    "actual": 100.0,
                    "point": 100.0,
                    "cum_actual": 100.0 * h + err,
                    "cum_point": 100.0 * h,
                }
            )
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-01-01", periods=3, freq="MS"),
            "point_estimate": [100.0, 100.0, 100.0],
        }
    )
    out = empirical_bands(pd.DataFrame(rows), forecast)

    assert out["backtest_cum_low_k"].iloc[0] == 2
    assert out["backtest_cum_n"].iloc[0] == 3
    assert out["backtest_cum_low_k"].iloc[1] == 3
    assert out["backtest_cum_n"].iloc[1] == 3
    assert out["cum_point"].iloc[1] < out["cum_ci80_lower"].iloc[1]
    # h=3 has no backtest rows: counts are missing, not zero.
    assert pd.isna(out["backtest_cum_low_k"].iloc[2])
    assert pd.isna(out["backtest_cum_n"].iloc[2])


@pytest.mark.parametrize("seed", [0, 1])
def test_summarise_backtest_holdout_is_a_report_not_a_target(seed):
    """The holdout rate is computed from the last third of origins only."""
    rng = np.random.default_rng(seed)
    origins = pd.date_range("2020-01-01", periods=6, freq="MS")
    rows = []
    for i, origin in enumerate(origins):
        rows.append(
            {
                "origin_month": origin,
                "h": 1,
                "month": origin + pd.DateOffset(months=1),
                "actual": float(rng.normal(100.0, 10.0)),
                "point": 100.0,
                "cum_actual": float(rng.normal(100.0, 10.0)),
                "cum_point": 100.0,
                "ci80_lower": 50.0,
                "ci80_upper": 150.0,
            }
        )
    summary = summarise_backtest(pd.DataFrame(rows))
    assert summary["holdout_cum80_m"] == 2  # last third of 6 origins
