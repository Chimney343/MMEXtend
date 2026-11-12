"""Tests for src.analysis.category_forecasters — synthetic data only."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.category_forecasters import (
    build_category_arm,
    build_local_arm,
    cumulative_columns,
    panel_rolling_origin_backtest,
)
from src.analysis.intermittent import build_hierarchy


def _panel(months: int = 30, keys: int = 3) -> pd.DataFrame:
    rows = []
    index = pd.date_range("2020-01-01", periods=months, freq="MS")
    rng = np.random.default_rng(7)
    for i in range(keys):
        sub = f"s{i}"
        values = rng.integers(0, 4, size=months).astype(float) * 10.0
        for month, value in zip(index, values):
            rows.append(
                {
                    "series_key": f"expense|subcategory|A|{sub}",
                    "flow_type": "expense",
                    "level": "subcategory",
                    "category": "A",
                    "subcategory": sub,
                    "is_unassigned": False,
                    "month": month,
                    "y": float(value),
                }
            )
        rows.append(
            {
                "series_key": "expense|category|A|",
                "flow_type": "expense",
                "level": "category",
                "category": "A",
                "subcategory": None,
                "is_unassigned": False,
                "month": index[i],
                "y": float(values[i]),
            }
        )
    return pd.DataFrame(rows)


LOCAL_ARMS = [
    ("naive_mean_12", {}),
    ("naive_mean_w", {"window": 6}),
    ("seasonal_naive", {}),
    ("croston", {"variant": "sba", "alpha": 0.1}),
    ("tsb", {"alpha": 0.1, "beta": 0.1}),
    ("auto_ets", {"window": 24}),
    ("theta", {"window": 24}),
    ("combo", {}),
]


@pytest.mark.parametrize("arm_id,params", LOCAL_ARMS)
def test_local_arms_are_non_negative_contiguous_and_deterministic(arm_id, params):
    index = pd.date_range("2019-01-01", periods=40, freq="MS")
    values = np.zeros(40)
    values[::3] = 12.0
    values[5] = 80.0
    train = pd.Series(values, index=index)
    arm = build_local_arm(arm_id, params)
    first = arm(train, 12)
    second = arm(train, 12)
    assert len(first) == 12
    assert (first["point_estimate"].to_numpy(dtype=float) >= 0).all()
    assert first["month"].iloc[0] == index[-1] + pd.DateOffset(months=1)
    pd.testing.assert_frame_equal(first, second)


def test_all_zero_series_falls_back_instead_of_failing():
    panel = _panel(months=24, keys=1)
    panel.loc[:, "y"] = panel["y"] * 0.0
    arm = build_category_arm({"arm_id": "theta", "params": {"window": 24}})
    bt, stats = panel_rolling_origin_backtest(
        panel, arm, [pd.Timestamp("2021-01-01")], horizon=6, n_jobs=1
    )
    assert stats["fallbacks"] == 1
    assert not bt.empty
    assert (bt["point"] == 0.0).all()


def test_panel_backtest_rows_and_clipping():
    panel = _panel(months=30)
    arm = build_category_arm({"arm_id": "naive_mean_12", "params": {}})
    origins = [pd.Timestamp("2021-01-01"), pd.Timestamp("2021-06-01")]
    bt, stats = panel_rolling_origin_backtest(panel, arm, origins, horizon=12, n_jobs=1)
    assert set(bt["arm"]) == {"naive_mean_12"}
    assert bt["h"].max() == 12
    assert (bt["point"] >= 0).all()
    assert stats["pairs"] > 0


def test_cumulative_columns_are_within_pair_cumulative():
    panel = _panel(months=30, keys=1)
    arm = build_category_arm({"arm_id": "naive_mean_12", "params": {}})
    bt, _ = panel_rolling_origin_backtest(
        panel, arm, [pd.Timestamp("2021-01-01")], horizon=12, n_jobs=1
    )
    out = cumulative_columns(bt)
    for _, group in out.groupby(["series_key", "origin_month"]):
        group = group.sort_values("h")
        assert np.allclose(group["cum_actual"].to_numpy(), np.cumsum(group["actual"].to_numpy()))


def test_reconciliation_bottom_up_is_coherent():
    panel = _panel(months=24, keys=2)
    hierarchy = build_hierarchy(panel)
    spec = {"arm_id": "reconciled", "params": {"base": {"arm_id": "naive_mean_12", "params": {}}, "method": "bottom_up"}}
    arm = build_category_arm(spec, {"hierarchy": hierarchy})
    train = panel[panel["month"] <= panel["month"].max()]
    frame = arm.forecaster(train, 6)
    months = pd.DatetimeIndex(sorted(frame["month"].unique()))
    for month in months:
        table = frame[frame["month"] == month].set_index("series_key")["point_estimate"]
        category = table.filter(like="expense|category|A|").sum()
        children = table.filter(like="subcategory").sum() + table.filter(like="unassigned").sum()
        assert category == pytest.approx(children, rel=1e-6, abs=1e-6)


def test_panel_backtest_rejects_empty_arm_result():
    panel = _panel(months=24)
    arm = build_category_arm({"arm_id": "global_lgbm", "params": {"min_rows": 10_000}})
    bt, stats = panel_rolling_origin_backtest(
        panel, arm, [pd.Timestamp("2021-01-01")], horizon=6, n_jobs=1
    )
    # Falls back to the trailing mean rather than dropping the origin.
    assert not bt.empty
    assert stats["fallbacks"] == 1
