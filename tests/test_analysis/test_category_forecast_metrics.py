"""Tests for the category forecast metrics and the runner smoke path."""

import numpy as np
import pandas as pd

from src.analysis.forecast_metrics import (
    avg_rel_cum,
    band_scores,
    money_weighted_ratio,
    pair_cumulative,
    phantom_spend,
    pooled_bias,
    scaled_pooled_band,
)


def _pairs(rows):
    return pd.DataFrame(
        rows, columns=["series_key", "origin_month", "cum_actual", "cum_point", "cumerr", "scale"]
    )


def test_geometric_mean_ratio_excludes_zero_base_error():
    arm = _pairs(
        [
            ("a", "2020-01-01", 100.0, 90.0, 10.0, 1200.0),
            ("b", "2020-01-01", 100.0, 100.0, 0.0, 1200.0),
        ]
    )
    base = _pairs(
        [
            ("a", "2020-01-01", 100.0, 80.0, 20.0, 1200.0),
            ("b", "2020-01-01", 100.0, 100.0, 0.0, 1200.0),
        ]
    )
    metric = avg_rel_cum(arm, base)
    assert metric["n"] == 1
    assert metric["avg_rel"] == 0.5
    assert metric["win_share"] == 1.0


def test_geometric_mean_is_not_arithmetic():
    arm = _pairs(
        [
            ("a", "2020-01-01", 100.0, 50.0, 50.0, 1200.0),
            ("b", "2020-01-01", 100.0, 0.0, 100.0, 1200.0),
        ]
    )
    base = _pairs(
        [
            ("a", "2020-01-01", 100.0, 0.0, 100.0, 1200.0),
            ("b", "2020-01-01", 100.0, 0.0, 100.0, 1200.0),
        ]
    )
    metric = avg_rel_cum(arm, base)
    # ratios 0.5 and 1.0 -> geometric mean sqrt(0.5), not 0.75
    assert metric["avg_rel"] == np.sqrt(0.5)


def test_phantom_spend_counts_only_zero_actual_pairs():
    pairs = _pairs(
        [
            ("a", "2020-01-01", 0.0, 40.0, -40.0, 100.0),
            ("b", "2020-01-01", 100.0, 100.0, 0.0, 1200.0),
        ]
    )
    assert phantom_spend(pairs) == 40.0 / 100.0


def test_pooled_bias_sign_and_under_count():
    pairs = _pairs(
        [
            ("a", "2020-01-01", 100.0, 90.0, 10.0, 1200.0),
            ("b", "2020-01-01", 100.0, 120.0, -20.0, 1200.0),
        ]
    )
    bias, under, n = pooled_bias(pairs)
    assert bias == -10.0 / 200.0
    assert under == 1
    assert n == 2


def test_money_weighted_ratio_uses_named_series_only():
    arm = _pairs(
        [
            ("money", "2020-01-01", 100.0, 50.0, 50.0, 1200.0),
            ("other", "2020-01-01", 100.0, 0.0, 100.0, 1200.0),
        ]
    )
    base = _pairs(
        [
            ("money", "2020-01-01", 100.0, 90.0, 10.0, 1200.0),
            ("other", "2020-01-01", 100.0, 90.0, 10.0, 1200.0),
        ]
    )
    assert money_weighted_ratio(arm, base, {"money"}) == 5.0


def test_pair_cumulative_needs_a_full_window():
    rows = []
    for h in range(1, 13):
        rows.append(
            {
                "series_key": "a",
                "origin_month": pd.Timestamp("2020-01-01"),
                "h": h,
                "month": pd.Timestamp("2020-01-01") + pd.DateOffset(months=h),
                "actual": 10.0,
                "point": 8.0,
            }
        )
    bt = pd.DataFrame(rows)
    pairs = pair_cumulative(bt, horizon=12)
    assert len(pairs) == 1
    assert pairs["cumerr"].iloc[0] == 24.0
    short = pair_cumulative(bt[bt["h"] <= 6], horizon=12)
    assert short.empty


def test_pooled_band_uses_only_fully_realised_errors():
    # Ten realised errors of +0.1 scale, plus one huge error at an origin whose
    # window has not finished by the scoring origin. The huge one must be ignored.
    rows = []
    for i in range(10):
        origin = pd.Timestamp("2019-01-01") + pd.DateOffset(months=i)
        rows.append(
            {
                "series_key": "a",
                "origin_month": origin,
                "h": 1,
                "err": 10.0,
                "scale": 100.0,
                "actual": 0.0,
                "month": origin + pd.DateOffset(months=1),
            }
        )
    rows.append(
        {
            "series_key": "a",
            "origin_month": pd.Timestamp("2020-08-01"),
            "h": 1,
            "err": 1_000_000.0,
            "scale": 100.0,
            "actual": 0.0,
            "month": pd.Timestamp("2020-09-01"),
        }
    )
    history = pd.DataFrame(rows)

    arm_rows = []
    for h in range(1, 13):
        arm_rows.append(
            {
                "series_key": "b",
                "origin_month": pd.Timestamp("2020-08-01"),
                "h": h,
                "month": pd.Timestamp("2020-08-01") + pd.DateOffset(months=h),
                "actual": 100.0,
                "point": 90.0,
            }
        )
    arm_bt = pd.DataFrame(arm_rows)
    pairs = pair_cumulative(arm_bt, horizon=12)
    bands = scaled_pooled_band(
        pairs, arm_bt, history, {"a": "lumpy", "b": "lumpy"}, min_prior=8
    )
    assert not bands.empty
    # Eleven errors exist at h=1 for series a, but only the ten realised ones
    # may enter: 2020-08 + 1 month is 2020-09, i.e. not on or before 2020-08.
    assert bands["n_prior"].iloc[0] == 10
    assert bands["lower"].iloc[0] < 1e6 * 100
    scores = band_scores(bands)
    assert 0.0 <= scores["cum12_coverage"] <= 1.0


def test_pooled_band_ignores_other_classes():
    rows = []
    for i in range(10):
        origin = pd.Timestamp("2019-01-01") + pd.DateOffset(months=i)
        rows.append(
            {
                "series_key": f"a{i}",
                "origin_month": origin,
                "h": 1,
                "err": 10.0,
                "scale": 100.0,
            }
        )
    classes = {f"a{i}": "smooth" for i in range(10)}
    classes["b"] = "lumpy"
    history = pd.DataFrame(rows)
    arm_rows = [
        {
            "series_key": "b",
            "origin_month": pd.Timestamp("2020-08-01"),
            "h": h,
            "month": pd.Timestamp("2020-08-01") + pd.DateOffset(months=h),
            "actual": 100.0,
            "point": 90.0,
        }
        for h in range(1, 13)
    ]
    arm_bt = pd.DataFrame(arm_rows)
    pairs = pair_cumulative(arm_bt, horizon=12)
    bands = scaled_pooled_band(pairs, arm_bt, history, classes, min_prior=8)
    # No smooth-class error may be borrowed for a lumpy series.
    assert bands["n_prior"].iloc[0] == 0
    assert bands["lower"].iloc[0] != bands["lower"].iloc[0]  # NaN
