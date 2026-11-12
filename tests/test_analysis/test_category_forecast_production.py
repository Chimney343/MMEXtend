"""Tests for src.analysis.category_forecast_production (synthetic data only).

Covers the Phase 5 requirements: the corrected grid, the 24-month horizon,
clipping, the frozen class map, null ci95, and a pooled band that never uses
data at or after the forecast origin.
"""

import numpy as np
import pandas as pd
import pytest

from src.analysis.category_forecast_production import (
    FROZEN_BAND_POOL,
    FROZEN_BAND_SCALE,
    FROZEN_CLASS_MAP,
    FROZEN_DEFAULT,
    _error_pool,
    build_production_panel,
    compute_category_forecast_rows,
)
from src.analysis.feature_engineering import engineer_features
from src.analysis.intermittent import SBC_CLASSES, panel_to_series, sbc_class


def _featured(rows) -> pd.DataFrame:
    """Featured frame from minimal transaction dicts."""
    frame = pd.DataFrame(rows)
    frame["year_month"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m")
    return engineer_features(frame)


def _synthetic(as_of: str | None = "2024-09-15", months: int = 48):
    """
    Spend shaped so every SBC class appears.

    - ``CatSmooth``: a demand month every month with near-constant size.
    - ``CatErratic``: a demand month every month with wild size.
    - ``CatIntermittent``: frequent zeros, constant size when it does spend.
    - ``CatLumpy``: frequent zeros and wild size.
    """
    rng = np.random.default_rng(11)
    start = pd.Timestamp("2019-03-01")
    index = pd.date_range(start, periods=months, freq="MS")
    rows = []

    def add(category: str, month: pd.Timestamp, amount: float, subcategory=None):
        rows.append(
            {
                "date": month + pd.Timedelta(days=3),
                "account": "Checking",
                "type": "withdrawal",
                "category": category,
                "subcategory": subcategory,
                "amount": -float(amount),
            }
        )

    for i, month in enumerate(index):
        add("CatSmooth", month, 300.0)
        add("CatErratic", month, 40.0 if i % 2 else 400.0)
        if i % 4 == 0:
            add("CatIntermittent", month, 150.0)
        # Lumpy needs ADI >= 1.32 *and* CV^2 >= 0.49, so the non-zero sizes
        # must vary widely as well as being rare.
        if i % 3 == 1:
            add("CatLumpy", month, 30.0 + 40.0 * (i % 11) ** 2)
        # A subcategory so the subcategory level is exercised too.
        add("CatSmooth", month, float(rng.integers(20, 90)), subcategory="Sub")
    return _featured(rows)


def test_grid_starts_at_first_nonzero_and_ends_at_last_complete():
    featured = _synthetic(months=30)
    panel, months = build_production_panel(featured, as_of="2024-09-15")

    assert not panel.empty
    # 30 months from 2019-03 end at 2021-08, which is the last complete month.
    last_complete = pd.Timestamp("2019-03-01") + pd.DateOffset(months=29)
    assert panel["month"].max() == last_complete
    assert months[-1] == last_complete
    for key, group in panel.groupby("series_key"):
        ordered = group.sort_values("month")
        nonzero = ordered[ordered["y"] > 0]
        assert ordered["month"].iloc[0] == nonzero["month"].iloc[0], f"{key} starts early"
        assert ordered["month"].iloc[-1] == last_complete


def test_trailing_zeros_are_kept_for_a_series_that_went_quiet():
    # A series that stops spending keeps its zero months to the last complete one.
    start = pd.Timestamp("2019-03-01")
    quiet_months = 18
    rows = [
        {
            "date": month + pd.Timedelta(days=2),
            "account": "Checking",
            "type": "withdrawal",
            "category": "Quiet",
            "subcategory": None,
            "amount": -100.0,
        }
        for month in pd.date_range(start, periods=quiet_months, freq="MS")
    ]
    # A second series keeps the panel running one month longer.
    rows += [
        {
            "date": month + pd.Timedelta(days=2),
            "account": "Checking",
            "type": "withdrawal",
            "category": "Active",
            "subcategory": None,
            "amount": -80.0,
        }
        for month in pd.date_range(start, periods=quiet_months + 1, freq="MS")
    ]
    featured = _featured(rows)
    panel, _ = build_production_panel(featured, as_of="2021-05-20")

    last_complete = start + pd.DateOffset(months=quiet_months)
    series = panel[panel["series_key"] == "expense|category|Quiet|"].sort_values("month")
    assert series["month"].min() == start
    assert series["month"].max() == last_complete
    trailing = series[series["month"] == last_complete]
    assert len(trailing) == 1
    assert (trailing["y"] == 0.0).all()


def test_horizon_is_24_months():
    featured = _synthetic()
    frame, outcome = compute_category_forecast_rows(featured, as_of="2024-09-15")

    assert not frame.empty
    assert frame["months_ahead"].min() == 1
    assert frame["months_ahead"].max() == 24
    counts = frame.groupby(["flow_type", "level", "category", "subcategory"], dropna=False).size()
    assert set(counts.unique()) == {24}
    # Every series has exactly 24 consecutive months ahead.
    for _, group in frame.groupby(["flow_type", "level", "category", "subcategory"], dropna=False):
        assert len(group) == 24
        months = pd.to_datetime(group.sort_values("months_ahead")["month"])
        assert months.is_monotonic_increasing
        assert len(months.dt.to_period("M").unique()) == 24
    assert outcome.rows is frame


def test_no_negative_point_or_band_values():
    featured = _synthetic()
    frame, _ = compute_category_forecast_rows(featured, as_of="2024-09-15")

    assert (frame["point_estimate"] >= 0).all()
    bands = frame[["ci80_lower", "ci80_upper", "cum_ci80_lower", "cum_ci80_upper"]]
    assert (bands.fillna(0.0) >= 0).all().all()


def test_ci95_columns_are_null():
    featured = _synthetic()
    frame, _ = compute_category_forecast_rows(featured, as_of="2024-09-15")

    assert frame["ci95_lower"].isna().all()
    assert frame["ci95_upper"].isna().all()


def test_every_sbc_class_is_exercised_by_the_fixture():
    """Guard against a fixture that only ever hits one arm."""
    featured = _synthetic()
    _frame, outcome = compute_category_forecast_rows(featured, as_of="2024-09-15")

    assert set(outcome.classified.values()) == set(SBC_CLASSES), (
        f"fixture only produced classes {set(outcome.classified.values())}"
    )


def test_class_to_method_map_is_applied_as_configured():
    featured = _synthetic()
    frame, outcome = compute_category_forecast_rows(featured, as_of="2024-09-15")

    assert not frame.empty
    assert outcome.classified, "no series were classified"
    # Each series' method label must name the arm the frozen map assigns to its
    # own class, and nothing else.
    labels_by_method = frame["method"].unique()
    assert len(labels_by_method) >= 1
    observed = {}
    for method in labels_by_method:
        prefix, rest = method.split(": ", 1)
        assert prefix == "class_selector"
        label, arm_part = rest.split(" -> ", 1)
        arm = arm_part.split(" ")[0]
        assert label in FROZEN_CLASS_MAP, f"unexpected class label {label!r}"
        assert FROZEN_CLASS_MAP[label]["arm_id"] == arm, f"{label} mapped to {arm}"
        observed[label] = arm
    # And every series actually carries the label of its own classification.
    for key, label in outcome.classified.items():
        if key in outcome.skipped_short:
            continue
        expected = FROZEN_CLASS_MAP.get(label, FROZEN_DEFAULT)["arm_id"]
        assert f"{label} -> {expected}" in " ".join(labels_by_method)


def test_custom_map_is_honoured():
    featured = _synthetic()
    custom = {
        "smooth": {"arm_id": "naive_mean_12", "params": {}},
        "erratic": {"arm_id": "naive_mean_12", "params": {}},
        "intermittent": {"arm_id": "zero", "params": {}},
        "lumpy": {"arm_id": "zero", "params": {}},
    }
    frame, _ = compute_category_forecast_rows(
        featured, as_of="2024-09-15", class_map=custom, default={"arm_id": "zero", "params": {}}
    )

    assert not frame.empty
    assert set(frame["method"].str.split(" -> ").str[1].str.split(" ").str[0]) <= {
        "naive_mean_12",
        "zero",
    }


def test_pooled_band_uses_no_data_after_the_origin():
    """
    Every error in the pool comes from an origin whose whole horizon finished
    on or before the last complete month, i.e. before the forecast origin.
    """
    featured = _synthetic()
    panel, months = build_production_panel(featured, as_of="2024-09-15")
    series_map = panel_to_series(panel)
    classes = {key: sbc_class(y.to_numpy(dtype=float)) for key, y in series_map.items()}
    spec_by_class = {label: FROZEN_CLASS_MAP.get(label, FROZEN_DEFAULT) for label in SBC_CLASSES}

    errors = _error_pool(series_map, classes, spec_by_class, months, 24, scale_mode="max_history")

    assert not errors.empty, "pool is empty; the test would not prove anything"
    last_complete = months[-1]
    # No pooled error may have observed a month after the last complete month.
    horizon_end = errors["origin_month"] + pd.DateOffset(months=1) + pd.DateOffset(months=23)
    assert (horizon_end <= last_complete).all()
    # And no origin may be the forecast origin itself.
    assert (errors["origin_month"] < last_complete).all()


def test_bands_are_finite_or_explicitly_missing():
    featured = _synthetic()
    frame, outcome = compute_category_forecast_rows(featured, as_of="2024-09-15")

    # Each series either has a full band or none; never a partial stripe.
    for _, group in frame.groupby(["flow_type", "level", "category", "subcategory"], dropna=False):
        has_lower = group["ci80_lower"].notna()
        assert has_lower.all() or (~has_lower).all()


def test_shipped_band_is_the_k1_candidate():
    """
    K1 is what ships: scale = max(trailing mean, all-history mean), pooled by
    SBC class. The band must be present and bracketed.
    """
    featured = _synthetic()
    frame, outcome = compute_category_forecast_rows(featured, as_of="2024-09-15")

    assert not frame.empty
    assert FROZEN_BAND_SCALE == "max_history"
    assert FROZEN_BAND_POOL == "class"
    assert frame["ci80_lower"].notna().any()
    assert (frame["ci80_lower"] <= frame["ci80_upper"]).all()
    # A scale is emitted for the series that had enough prior origins.
    assert not outcome.band_missing or len(outcome.band_missing) < len(outcome.classified)


def test_no_band_mode_emits_null_bands():
    """
    Setting ``band_scale=None`` is the "nothing qualified, ship nothing" path:
    ci80 stays null and the chart degrades to a point line.
    """
    featured = _synthetic()
    frame, outcome = compute_category_forecast_rows(
        featured, as_of="2024-09-15", band_scale=None, band_pool=None
    )

    assert not frame.empty
    assert frame["ci80_lower"].isna().all()
    assert frame["ci80_upper"].isna().all()
    assert frame["cum_ci80_lower"].isna().all()
    assert frame["cum_ci80_upper"].isna().all()
    assert frame["point_estimate"].notna().all()
    assert frame["cum_point"].notna().all()
    assert outcome.band_missing


def test_band_path_still_works_when_a_candidate_is_supplied():
    """Passing a scale mode opts into the band, proving the path is wired."""
    featured = _synthetic()
    frame, _ = compute_category_forecast_rows(
        featured, as_of="2024-09-15", band_scale="max_history", band_pool="class"
    )
    assert frame["ci80_lower"].notna().any()
    assert (frame["ci80_lower"] <= frame["ci80_upper"]).all()


def test_band_offsets_are_within_observed_scale():
    """A band must not be orders of magnitude wider than the series itself."""
    featured = _synthetic()
    frame, _ = compute_category_forecast_rows(
        featured, as_of="2024-09-15", band_scale="max_history", band_pool="class"
    )
    usable = frame[frame["ci80_lower"].notna()]
    if usable.empty:
        pytest.skip("no series had enough prior origins for a band")

    width = (usable["ci80_upper"] - usable["ci80_lower"]).abs()
    # 24 x the largest monthly point is a very loose ceiling; this catches a
    # sign or scale error rather than judging calibration.
    assert (width <= 24 * usable["point_estimate"].abs().max() + 1.0).all()
