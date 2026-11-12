"""Tests for the band bake-off's summarise() metrics (synthetic only).

These lock the distinction between joint-12 coverage and pointwise monthly
coverage: they are different numbers, and calling one the other misreports
which promise the chart's monthly ribbons keep.
"""

import numpy as np
import pandas as pd

from scripts.experiments.band_bakeoff import _pair_row, summarise


def _pair(lower, upper, actual, cum_lower=None, cum_upper=None, key="s", origin="2020-01-01"):
    """Build one pair row through the real code path."""
    point = np.full(len(actual), 100.0)
    lower = np.asarray(lower, dtype=float)
    upper = np.asarray(upper, dtype=float)
    m_lo = lower - point
    m_hi = upper - point
    horizon = len(point)
    if cum_lower is None:
        cum_lower = np.full(horizon, np.nan)
    if cum_upper is None:
        cum_upper = np.full(horizon, np.nan)
    c_lo = np.asarray(cum_lower, dtype=float) - np.cumsum(point)
    c_hi = np.asarray(cum_upper, dtype=float) - np.cumsum(point)
    return _pair_row(
        key,
        pd.Timestamp(origin),
        point,
        np.asarray(actual, dtype=float),
        m_lo,
        m_hi,
        c_lo,
        c_hi,
        100.0,
        horizon,
    )


def test_monthly_pointwise_differs_from_joint12():
    """11 of 12 months inside is 0.92 pointwise but 0 joint."""
    actual = np.full(12, 100.0)
    actual[0] = 1000.0  # one month wildly outside
    lower = np.full(12, 50.0)
    upper = np.full(12, 150.0)
    row = _pair(lower, upper, actual)
    frame = pd.DataFrame([row])
    summary = summarise(frame)

    assert summary["joint12_coverage"] == 0.0
    assert summary["monthly_pointwise_coverage"] == 11 / 12
    assert summary["monthly_rows"] == 12


def test_full_cover_scores_one_on_both():
    actual = np.full(12, 100.0)
    row = _pair(np.full(12, 50.0), np.full(12, 150.0), actual)
    summary = summarise(pd.DataFrame([row]))

    assert summary["joint12_coverage"] == 1.0
    assert summary["monthly_pointwise_coverage"] == 1.0


def test_missing_monthly_band_is_excluded_not_counted_as_miss():
    """A month with no band has no monthly row to score."""
    actual = np.full(12, 100.0)
    lower = np.full(12, 50.0)
    upper = np.full(12, 150.0)
    lower[6:] = np.nan
    upper[6:] = np.nan
    row = _pair(lower, upper, actual)
    summary = summarise(pd.DataFrame([row]))

    # Only the six months that exist count.
    assert summary["monthly_rows"] == 6
    assert summary["monthly_pointwise_coverage"] == 1.0
    # A window with missing months cannot be jointly covered.
    assert summary["joint12_coverage"] == 0.0


def test_cumulative_band_presence_is_judged_at_h12_not_any_horizon():
    """
    A pair whose earlier months have bands but whose h = 12 cumulative band is
    missing must count as a missing cumulative band, not a zero-width one.
    """
    actual = np.full(12, 100.0)
    lower = np.full(12, 50.0)
    upper = np.full(12, 150.0)
    # Cumulative band exists only for the first 9 months.
    cum_lower = np.full(12, np.nan)
    cum_upper = np.full(12, np.nan)
    cum_lower[:9] = np.arange(1, 10) * 50.0
    cum_upper[:9] = np.arange(1, 10) * 150.0
    row = _pair(lower, upper, actual, cum_lower=cum_lower, cum_upper=cum_upper)

    assert row["has_band"] is False
    assert row["covered_cum"] is False
    assert not (isinstance(row["cum_width"], float) and row["cum_width"] == 0.0)


def test_zero_width_monthly_band_is_flagged():
    actual = np.full(12, 100.0)
    row = _pair(np.full(12, 100.0), np.full(12, 100.0), actual)
    summary = summarise(pd.DataFrame([row]))

    assert summary["zero_width_share"] == 1.0
    # A zero-width band at exactly the actual still counts as inside.
    assert summary["monthly_pointwise_coverage"] == 1.0
