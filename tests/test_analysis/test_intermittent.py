import numpy as np
import pandas as pd

from src.analysis.intermittent import (
    build_hierarchy,
    build_panel,
    croston,
    demand_stats,
    sbc_class,
    tsb,
)


def _monthly(rows):
    return pd.DataFrame(rows, columns=["flow_type", "level", "category", "subcategory", "month", "y"])


def test_grid_starts_at_first_nonzero_and_keeps_trailing_zeros():
    rows = [
        ("expense", "category", "A", None, "2020-01-01", 5.0),
        ("expense", "category", "A", None, "2020-03-01", 7.0),
        ("expense", "category", "A", None, "2020-05-01", 9.0),
    ]
    panel = build_panel(_monthly(rows), last_complete="2020-08-01", include_unassigned=False)
    series = panel[panel["series_key"] == "expense|category|A|"]
    assert series["month"].min() == pd.Timestamp("2020-01-01")
    assert series["month"].max() == pd.Timestamp("2020-08-01")
    assert len(series) == 8
    assert series.loc[series["month"] == "2020-02-01", "y"].iloc[0] == 0.0
    assert series.loc[series["month"] == "2020-08-01", "y"].iloc[0] == 0.0


def test_partial_month_is_excluded_when_not_in_input():
    rows = [("expense", "category", "A", None, "2020-01-01", 4.0)]
    panel = build_panel(_monthly(rows), last_complete="2020-02-01", include_unassigned=False)
    assert panel["month"].max() == pd.Timestamp("2020-02-01")
    assert (panel["y"] >= 0).all()


def test_unassigned_is_category_minus_subcategories_and_non_negative():
    rows = [
        ("expense", "category", "A", None, "2020-01-01", 10.0),
        ("expense", "category", "A", None, "2020-02-01", 8.0),
        ("expense", "subcategory", "A", "x", "2020-01-01", 6.0),
        ("expense", "subcategory", "A", "x", "2020-02-01", 8.0),
    ]
    panel = build_panel(_monthly(rows), last_complete="2020-03-01")
    unassigned = panel[panel["is_unassigned"]]
    assert unassigned["series_key"].nunique() == 1
    values = unassigned.sort_values("month")["y"].to_numpy()
    assert list(values) == [4.0, 0.0, 0.0]
    assert (panel["y"] >= 0).all()


def test_hierarchy_sums_categories_from_children():
    rows = [
        ("expense", "category", "A", None, "2020-01-01", 10.0),
        ("expense", "subcategory", "A", "x", "2020-01-01", 6.0),
    ]
    panel = build_panel(_monthly(rows), last_complete="2020-02-01")
    hierarchy = build_hierarchy(panel)
    spec = hierarchy["expense"]
    keys = spec["keys"]
    position = {key: i for i, key in enumerate(keys)}
    root = position["expense|category|A|"]
    assert spec["summing"][root].sum() == 2.0
    child = position["expense|subcategory|A|x"]
    unassigned = position["expense|unassigned|A|(unassigned)"]
    assert spec["summing"][child, child] == 1.0
    assert spec["summing"][unassigned, unassigned] == 1.0
    assert spec["summing"][root, child] == 1.0
    assert spec["summing"][root, unassigned] == 1.0


def test_demand_stats_and_sbc_boundaries():
    smooth = np.array([10.0, 11.0, 9.0, 10.0, 10.5, 9.5])
    adi, cv2 = demand_stats(smooth)
    assert adi == 1.0
    assert cv2 < 0.49
    assert sbc_class(smooth) == "smooth"

    erratic = np.array([1.0, 100.0, 2.0, 90.0, 3.0, 80.0])
    assert demand_stats(erratic)[0] == 1.0
    assert sbc_class(erratic) == "erratic"

    intermittent = np.array([0.0, 0.0, 5.0, 0.0, 0.0, 0.0, 5.0, 0.0])
    adi, cv2 = demand_stats(intermittent)
    assert adi > 1.32
    assert cv2 == 0.0
    assert sbc_class(intermittent) == "intermittent"

    lumpy = np.array([0.0, 1.0, 0.0, 0.0, 90.0, 0.0, 0.0, 0.0])
    assert demand_stats(lumpy)[0] > 1.32
    assert sbc_class(lumpy) == "lumpy"


def test_croston_matches_hand_computation():
    # Demands at t=1 (size 10) and t=4 (size 20), alpha=0.5.
    y = np.array([10.0, 0.0, 0.0, 20.0])
    # z: 10 -> 10 + 0.5*(20-10) = 15; p: 1 -> 1 + 0.5*(3-1) = 2
    assert croston(y, alpha=0.5, variant="classic") == 7.5
    assert croston(y, alpha=0.5, variant="sba") == 7.5 * (1 - 0.25)


def test_croston_flat_after_single_demand():
    y = np.array([0.0, 0.0, 4.0, 0.0])
    assert croston(y, alpha=0.3) == 4.0 / 3.0


def test_tsb_matches_hand_computation():
    y = np.array([10.0, 0.0, 20.0, 0.0])
    # initial p = 0.5, z = 15.
    # t0: p = .5 + .5*(1-.5) = .75, z = 15 + .5*(10-15) = 12.5
    # t1: p = .75 + .5*(0-.75) = .375
    # t2: p = .375 + .5*(1-.375) = .6875, z = 12.5 + .5*(20-12.5) = 16.25
    # t3: p = .6875 + .5*(0-.6875) = .34375
    assert tsb(y, alpha=0.5, beta=0.5) == 0.34375 * 16.25


def test_tsb_decays_towards_zero_and_croston_does_not():
    y = np.concatenate([np.array([50.0, 50.0, 50.0]), np.zeros(60)])
    assert tsb(y, alpha=0.2, beta=0.2) < 0.01
    assert croston(y, alpha=0.2) > 1.0


def test_all_zero_series_forecasts_zero():
    zeros = np.zeros(24)
    assert croston(zeros, 0.1) == 0.0
    assert tsb(zeros, 0.1, 0.1) == 0.0
    assert sbc_class(zeros) == "intermittent"
    adi, cv2 = demand_stats(zeros)
    assert adi == float("inf")
    assert cv2 == 0.0
