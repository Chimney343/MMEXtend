"""Tests for the shared band module and prospective tracking (synthetic only)."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.empirical_band import (
    MIN_SCALE,
    apply_offsets,
    band_from_pool,
    origin_scale,
    pool_with_size_bucket,
    size_bucket,
    size_tercile_cutpoints,
)
from src.analysis.forecast_tracking import save_snapshot, score_snapshots


# ── Scale ────────────────────────────────────────────────────────────────────


def test_origin_scale_uses_only_the_trailing_window():
    history = np.array([1000.0] * 24 + [10.0] * 12)
    # Trailing 12 months average 10, and the earlier large values must not leak
    # into the origin scale.
    assert origin_scale(history, mode="origin12") == pytest.approx(10.0)


def test_origin_scale_max_history_mode_takes_the_larger():
    history = np.array([1000.0] * 24 + [10.0] * 12)
    # max(mean of last 12 = 10, mean of all history = 670) = 670.
    assert origin_scale(history, mode="max_history") == pytest.approx(670.0)
    assert origin_scale(history, mode="max_history") > origin_scale(history, mode="origin12")


def test_origin_scale_is_floored():
    assert origin_scale(np.zeros(12), mode="origin12") == MIN_SCALE
    assert origin_scale(np.array([]), mode="origin12") == MIN_SCALE


def test_origin_scale_rejects_unknown_mode():
    with pytest.raises(ValueError):
        origin_scale(np.arange(12.0), mode="hindsight")


def test_origin_scale_never_sees_the_future():
    """Appending future values must not change a scale computed at the origin."""
    history = np.arange(1.0, 25.0)
    base = origin_scale(history, mode="max_history")
    extended = np.concatenate([history, np.array([1e9])])
    assert origin_scale(history, mode="max_history") == base
    # The future value is only visible if it is passed in; a caller that stops
    # at the origin cannot see it.
    assert origin_scale(extended, mode="max_history") > base


# ── Pool and band ────────────────────────────────────────────────────────────


def _pool(rows):
    return pd.DataFrame(
        rows, columns=["series_key", "sbc", "origin_month", "h", "err", "cum_err", "scale"]
    )


def test_band_uses_only_fully_realised_origins():
    rows = []
    for index in range(10):
        origin = pd.Timestamp("2020-01-01") + pd.DateOffset(months=index)
        rows.append(
            {
                "series_key": "a",
                "sbc": "smooth",
                "origin_month": origin,
                "h": 1,
                "err": 10.0,
                "cum_err": 10.0,
                "scale": 100.0,
            }
        )
    # An origin whose horizon has not finished by the scoring origin.
    rows.append(
        {
            "series_key": "a",
            "sbc": "smooth",
            "origin_month": pd.Timestamp("2020-12-01"),
            "h": 1,
            "err": 1e6,
            "cum_err": 1e6,
            "scale": 100.0,
        }
    )
    pool = _pool(rows)
    point = np.full(12, 50.0)
    m_lo, m_hi, _, _ = band_from_pool(
        point, 100.0, pool, key="a", origin=pd.Timestamp("2020-11-01"), horizon=12
    )
    # Ten usable errors, and the 1e6 outlier cannot influence the band.
    assert np.isfinite(m_lo[0])
    assert m_hi[0] < 1e5


def test_band_is_empty_below_min_prior():
    pool = _pool(
        [
            {
                "series_key": "a",
                "sbc": "smooth",
                "origin_month": pd.Timestamp("2020-01-01"),
                "h": 1,
                "err": 1.0,
                "cum_err": 1.0,
                "scale": 10.0,
            }
        ]
    )
    point = np.full(12, 5.0)
    m_lo, _, _, _ = band_from_pool(
        point, 10.0, pool, key="a", origin=pd.Timestamp("2021-01-01"), horizon=12
    )
    assert np.isnan(m_lo).all()


def test_band_scales_with_the_origin_scale():
    rows = [
        {
            "series_key": "a",
            "sbc": "smooth",
            "origin_month": pd.Timestamp("2020-01-01") + pd.DateOffset(months=i),
            "h": 1,
            "err": 1.0,
            "cum_err": 1.0,
            "scale": 1.0,
        }
        for i in range(12)
    ]
    pool = _pool(rows)
    point = np.full(12, 100.0)
    origin = pd.Timestamp("2021-06-01")
    small, _, _, _ = band_from_pool(point, 1.0, pool, key="a", origin=origin, horizon=12)
    large, _, _, _ = band_from_pool(point, 100.0, pool, key="a", origin=origin, horizon=12)
    assert large[0] == pytest.approx(small[0] * 100.0)


def test_pool_is_split_by_label():
    """A label's band must come from that label's errors only."""
    rows = []
    for i in range(12):
        origin = pd.Timestamp("2020-01-01") + pd.DateOffset(months=i)
        # Smooth: constant +1 error. Lumpy: constant -50 error.
        rows.append(
            {"series_key": f"a{i}", "sbc": "smooth", "origin_month": origin,
             "h": 1, "err": 1.0, "cum_err": 1.0, "scale": 1.0}
        )
        rows.append(
            {"series_key": f"b{i}", "sbc": "lumpy", "origin_month": origin,
             "h": 1, "err": -50.0, "cum_err": -50.0, "scale": 1.0}
        )
    pool = _pool(rows)
    point = np.full(12, 20.0)
    origin = pd.Timestamp("2021-06-01")
    smooth, _, _, _ = band_from_pool(
        point, 1.0, pool, key="target", origin=origin, horizon=12, label="smooth", label_column="sbc"
    )
    lumpy, _, _, _ = band_from_pool(
        point, 1.0, pool, key="target", origin=origin, horizon=12, label="lumpy", label_column="sbc"
    )
    # Constant errors give equal quantiles, so the bands differ by the offset
    # sign, not by width: smooth sits above the point, lumpy below it.
    assert smooth[0] == pytest.approx(1.0)
    assert lumpy[0] == pytest.approx(-50.0)


# ── Size terciles ────────────────────────────────────────────────────────────


def test_tercile_cutpoints_and_buckets():
    rows = []
    for i, scale in enumerate([1.0] * 4 + [10.0] * 4 + [100.0] * 4):
        rows.append(
            {
                "series_key": f"s{i}",
                "sbc": "smooth",
                "origin_month": pd.Timestamp("2020-01-01"),
                "h": 1,
                "err": 1.0,
                "cum_err": 1.0,
                "scale": scale,
            }
        )
    pool = _pool(rows)
    cutpoints = size_tercile_cutpoints(pool, pd.Timestamp("2021-01-01"))
    assert cutpoints is not None
    assert size_bucket(1.0, cutpoints) == "small"
    assert size_bucket(100.0, cutpoints) == "large"


def test_size_bucket_column_is_labelled_per_stored_origin():
    rows = []
    for origin_index in range(3):
        origin = pd.Timestamp("2020-01-01") + pd.DateOffset(months=origin_index)
        for i, scale in enumerate([1.0, 5.0, 50.0]):
            rows.append(
                {
                    "series_key": f"s{origin_index}_{i}",
                    "sbc": "smooth",
                    "origin_month": origin,
                    "h": 1,
                    "err": 1.0,
                    "cum_err": 1.0,
                    "scale": scale,
                }
            )
    pool = pool_with_size_bucket(_pool(rows), pd.Timestamp("2021-01-01"))
    assert set(pool["size_bucket"]) <= {"small", "mid", "large", "all"}
    assert pool["size_bucket"].notna().all()


# ── Offsets and snapping ─────────────────────────────────────────────────────


def test_missing_label_column_raises_instead_of_pooling_everything():
    """A labelled band must never silently mix all classes."""
    rows = [
        {
            "series_key": f"s{i}",
            "sbc": "smooth",
            "origin_month": pd.Timestamp("2020-01-01") + pd.DateOffset(months=i),
            "h": 1,
            "err": 1.0,
            "cum_err": 1.0,
            "scale": 1.0,
        }
        for i in range(12)
    ]
    pool = _pool(rows)
    point = np.full(12, 5.0)
    with pytest.raises(ValueError):
        band_from_pool(
            point,
            1.0,
            pool,
            key="target",
            origin=pd.Timestamp("2021-06-01"),
            horizon=12,
            label="smooth",
            label_column="missing_column",
        )


def test_apply_offsets_clips_and_snaps():
    point = np.array([5.0, 0.001, 12.0])
    m_lo = np.array([-100.0, -0.5, -1.0])
    m_hi = np.array([1.0, 0.5, 2.0])
    c_lo = np.array([-1.0, -0.2, -0.4])
    c_hi = np.array([9.0, 0.004, 3.0])
    frame = apply_offsets(point, m_lo, m_hi, c_lo, c_hi, snap=0.01)
    assert (frame[["point_estimate", "ci80_lower", "ci80_upper",
                   "cum_ci80_lower", "cum_ci80_upper"]] >= 0).all().all()
    # The tiny point is snapped to exactly 0.
    assert frame["point_estimate"].iloc[1] == 0.0
    assert frame["point_estimate"].iloc[0] == 5.0


def test_apply_offsets_cumulative_tracks_the_point():
    point = np.array([10.0, 10.0, 10.0])
    zeros = np.zeros(3)
    frame = apply_offsets(point, zeros, zeros, zeros, zeros)
    assert list(frame["cum_point"]) == [10.0, 20.0, 30.0]


# ── Prospective tracking ─────────────────────────────────────────────────────


def _snapshot_frame(months: int = 12) -> pd.DataFrame:
    month = pd.date_range("2024-01-01", periods=months, freq="MS")
    return pd.DataFrame(
        {
            "flow_type": "expense",
            "level": "category",
            "category": "Cat",
            "subcategory": None,
            "months_ahead": range(1, months + 1),
            "month": month,
            "point_estimate": 100.0,
            "ci80_lower": 50.0,
            "ci80_upper": 150.0,
            "cum_point": np.arange(1, months + 1) * 100.0,
            "cum_ci80_lower": np.arange(1, months + 1) * 50.0,
            "cum_ci80_upper": np.arange(1, months + 1) * 150.0,
        }
    )


def test_save_snapshot_is_idempotent_and_private(tmp_path):
    frame = _snapshot_frame()
    first = save_snapshot("category", frame, as_of="2024-01-31", root=tmp_path)
    second = save_snapshot("category", frame, as_of="2024-01-31", root=tmp_path)
    assert first == second
    assert (first / "forecast.parquet").exists()
    assert len(list((tmp_path / "category").iterdir())) == 1


def test_save_snapshot_handles_empty_forecast(tmp_path):
    target = save_snapshot(
        "savings", pd.DataFrame(), as_of="2024-01-31", root=tmp_path
    )
    assert (target / "empty.json").exists()


def test_score_reports_coverage_and_error(tmp_path):
    frame = _snapshot_frame(months=6)
    save_snapshot("category", frame, as_of="2023-12-31", root=tmp_path)
    actual = pd.DataFrame(
        {
            "flow_type": "expense",
            "level": "category",
            "category": "Cat",
            "month": pd.date_range("2024-01-01", periods=6, freq="MS"),
            "y": [120.0, 80.0, 140.0, 90.0, 160.0, 70.0],
        }
    )
    report = score_snapshots(actual, kind="category", root=tmp_path)
    assert report["scored"] == 1
    assert 0.0 <= report["monthly_coverage_mean"] <= 1.0
    assert report["per_snapshot"]
    # 660 actual vs 600 forecast.
    assert report["per_snapshot"][0]["cum_error"] == pytest.approx(60.0)


def test_score_handles_no_snapshots(tmp_path):
    report = score_snapshots(
        pd.DataFrame({"month": [], "y": []}), kind="category", root=tmp_path
    )
    assert report["snapshots"] == 0


def test_score_ignores_unrealised_months(tmp_path):
    frame = _snapshot_frame(months=12)
    save_snapshot("category", frame, as_of="2023-12-31", root=tmp_path)
    actual = pd.DataFrame(
        {
            "flow_type": "expense",
            "level": "category",
            "category": "Cat",
            "month": pd.date_range("2024-01-01", periods=2, freq="MS"),
            "y": [100.0, 100.0],
        }
    )
    report = score_snapshots(actual, kind="category", root=tmp_path)
    # Only the two realised months are scored; the other ten are not counted
    # as misses.
    assert report["per_snapshot"][0]["months_realised"] == 2
    assert report["monthly_coverage_mean"] == pytest.approx(1.0)
