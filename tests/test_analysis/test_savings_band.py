"""Tests for the shipped savings V1 band (synthetic data only)."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.savings_band import build_pool, savings_bands


def _history(months: int = 96, level: float = 1000.0) -> pd.Series:
    index = pd.date_range("2018-01-01", periods=months, freq="MS")
    rng = np.random.default_rng(5)
    values = level + rng.normal(0, 80, months)
    return pd.Series(np.abs(values), index=index)


def _backtest(history: pd.Series, horizon: int = 24) -> pd.DataFrame:
    rows = []
    for origin in history.index[36:-horizon]:
        point = float(history[history.index <= origin].iloc[-12:].mean())
        future = history.reindex(
            pd.date_range(origin + pd.DateOffset(months=1), periods=horizon, freq="MS")
        ).to_numpy(dtype=float)
        cumulative = 0.0
        for step in range(horizon):
            cumulative += future[step]
            rows.append(
                {
                    "origin_month": origin,
                    "h": step + 1,
                    "month": origin + pd.DateOffset(months=step + 1),
                    "actual": float(future[step]),
                    "point": point,
                    "cum_actual": float(np.cumsum(future)[step]),
                    "cum_point": point * (step + 1),
                }
            )
    return pd.DataFrame(rows)


def _forecast(point: float = 1000.0, months: int = 24) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "month": pd.date_range("2025-01-01", periods=months, freq="MS"),
            "point_estimate": point,
            "ci80_lower": np.nan,
            "ci80_upper": np.nan,
        }
    )


def test_pool_scale_never_uses_future_actuals():
    """Every stored scale must be derived from history at its own origin only."""
    history = _history()
    backtest = _backtest(history)
    pool = build_pool(backtest, history, horizon=24)
    assert not pool.empty
    for origin, group in pool.groupby("origin_month"):
        expected = float(np.std(history[history.index <= origin].iloc[-12:], ddof=0))
        assert group["scale"].iloc[0] == pytest.approx(max(expected, 1.0), rel=1e-6)


def test_savings_bands_are_wide_and_bracketed():
    history = _history()
    backtest = _backtest(history)
    forecast = _forecast()
    out = savings_bands(backtest, forecast, history)

    assert out["ci80_lower"].notna().any()
    assert (out["ci80_upper"] >= out["ci80_lower"]).all()
    # A roughly 8% monthly noise series must produce a band with real width,
    # not the sub-percent sliver the old shipped band had.
    width = (out["ci80_upper"] - out["ci80_lower"]).iloc[:12].median()
    assert width > 0.1 * 1000.0


def test_savings_bands_leave_ci95_null_and_name_the_method():
    history = _history()
    out = savings_bands(_backtest(history), _forecast(), history)
    assert out["ci95_lower"].isna().all()
    assert out["ci95_upper"].isna().all()
    assert out["interval_method"].iloc[0].startswith("empirical_scaled_n")
    assert out["backtest_cum_n"].notna().any()


def test_savings_bands_use_only_realised_origins():
    """
    A stored origin contributes to step h only once its h-month outcome has
    finished by the forecast origin (``o' + h <= o``).

    Planting an enormous error at an origin whose outcome is still in the
    future must not move that step's band, while planting one at a finished
    origin must.
    """
    history = _history(months=96)
    backtest = _backtest(history)
    forecast = _forecast()
    origin = pd.Timestamp(forecast["month"].iloc[0]) - pd.DateOffset(months=1)

    # The most recent origin has ``gap`` steps realised by the forecast origin.
    latest = backtest["origin_month"].max()
    gap = (origin.year - latest.year) * 12 + (origin.month - latest.month)
    assert gap == 12, f"fixture expected 12 realised steps at the latest origin, got {gap}"

    baseline = savings_bands(backtest, forecast, history)

    # Poison steps that have NOT finished: h > gap at the latest origin.
    future_rows = (backtest["origin_month"] == latest) & (backtest["h"] > gap)
    assert future_rows.any(), "fixture must contain unrealised steps at the latest origin"
    poisoned_future = backtest.copy()
    poisoned_future.loc[future_rows, "actual"] = (
        poisoned_future.loc[future_rows, "actual"] + 1e9
    )
    unchanged = savings_bands(poisoned_future, forecast, history)
    pd.testing.assert_series_equal(
        baseline["ci80_lower"].reset_index(drop=True),
        unchanged["ci80_lower"].reset_index(drop=True),
        check_names=False,
    )

    # Poison a finished step across every origin with a realised h = 1 and
    # expect a change. One outlier cannot move a 10th percentile over 30+
    # observations, so the whole step is shifted.
    past_rows = (backtest["origin_month"] != latest) & (backtest["h"] == 1)
    poisoned_past = backtest.copy()
    poisoned_past.loc[past_rows, "actual"] = (
        poisoned_past.loc[past_rows, "actual"] + 1e9
    )
    changed = savings_bands(poisoned_past, forecast, history)
    assert (changed["ci80_lower"].to_numpy() != baseline["ci80_lower"].to_numpy()).any()


def test_savings_bands_clip_and_snap():
    history = _history()
    out = savings_bands(_backtest(history), _forecast(point=0.005), history)
    assert (out[["ci80_lower", "ci80_upper"]] >= 0).all().all()
    # A sub-cent point is snapped to exactly zero rather than carried as noise.
    assert out["point_estimate"].iloc[0] == 0.0


def test_savings_bands_require_history():
    empty = pd.Series(dtype=float)
    with pytest.raises(RuntimeError):
        savings_bands(_backtest(_history()), _forecast(), empty)
