"""Tests for dashboard chart builders."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest

from dashboard.charts import (
    make_empty_figure,
    plot_balances_by_group,
    plot_big_wins,
    plot_category_forecast,
    plot_category_trend,
    plot_lifestyle_creep,
    plot_mc_fan,
    plot_reduction_scenario,
    plot_reserve_transfers,
    plot_reserve_vs_ceiling,
    plot_savings_fan,
    plot_spending_plan_vs_target,
    plot_take_home_allocation,
    plot_take_home_waterfall,
)


def test_plot_mc_fan_returns_figure_with_expected_traces():
    result = {
        "months": pd.date_range("2026-01-01", periods=3, freq="MS"),
        "p10": np.array([1000.0, 950.0, 900.0]),
        "p25": np.array([1100.0, 1050.0, 1000.0]),
        "p50": np.array([1200.0, 1150.0, 1100.0]),
        "p75": np.array([1300.0, 1250.0, 1200.0]),
        "p90": np.array([1400.0, 1350.0, 1300.0]),
    }

    fig = plot_mc_fan(result)

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 3
    assert fig.layout.title.text == "Monte Carlo Cash Balance Forecast"
    assert fig.layout.hovermode == "x unified"
    assert fig.layout.yaxis.tickformat == ",.0f"


def test_plot_reduction_scenario_returns_dual_axis_figure():
    result = pd.DataFrame(
        {
            "month": [1, 2, 3],
            "delta_monthly": [200.0, 200.0, 200.0],
            "delta_cumulative": [200.0, 400.0, 600.0],
        }
    )

    fig = plot_reduction_scenario(result)

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 2
    assert fig.layout.title.text == "Expense Reduction Scenario"
    assert fig.layout.hovermode == "x unified"
    assert fig.layout.yaxis.tickformat == ",.0f"


def test_make_empty_figure_renders_message_annotation():
    fig = make_empty_figure("No data available for this view")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 0
    assert len(fig.layout.annotations) == 1
    assert fig.layout.annotations[0].text == "No data available for this view"
    assert fig.layout.showlegend is False


def test_plot_savings_fan_returns_figure_with_expected_traces():
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=6, freq="MS"),
            "point_estimate": [500.0, 500.0, 500.0, 500.0, 500.0, 500.0],
            "cum_point": [500.0, 1000.0, 1500.0, 2000.0, 2500.0, 3000.0],
            "cum_ci80_lower": [300.0, 600.0, 900.0, 1200.0, 1500.0, 1800.0],
            "cum_ci80_upper": [700.0, 1400.0, 2100.0, 2800.0, 3500.0, 4200.0],
            "interval_method": "model",
            "method": "Prophet",
        }
    )

    hist_months = ["2026-04", "2026-05", "2026-06"]
    hist_cumulative = [3500.0, 4250.0, 5000.0]

    fig = plot_savings_fan(
        forecast,
        hist_months=hist_months,
        hist_cumulative=hist_cumulative,
        current_balance=5000,
        target_amount=12000,
        horizon=6,
    )

    assert isinstance(fig, go.Figure)
    assert fig.layout.title.text == "Savings Forecast"
    assert fig.layout.hovermode == "x unified"
    assert fig.layout.yaxis.tickformat == ",.0f"

    trace_names = [t.name for t in fig.data]
    assert "80% band (model, uncalibrated)" in trace_names
    assert "Cumulative balance" in trace_names

    band_trace = next(t for t in fig.data if t.name == "80% band (model, uncalibrated)")
    assert band_trace.fill == "toself"


def test_plot_savings_fan_skips_ci_ribbons_when_nan():
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=3, freq="MS"),
            "point_estimate": [400.0, 400.0, 400.0],
            "ci80_lower": [float("nan")] * 3,
            "ci80_upper": [float("nan")] * 3,
            "ci95_lower": [float("nan")] * 3,
            "ci95_upper": [float("nan")] * 3,
            "method": "rolling_mean",
        }
    )

    fig = plot_savings_fan(
        forecast,
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=None,
        horizon=3,
    )

    trace_names = [t.name for t in fig.data]
    assert "Cumulative balance" in trace_names
    assert "95% CI" not in trace_names
    assert "80% CI" not in trace_names


def test_plot_savings_fan_draws_cumulative_band_from_backtest():
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=6, freq="MS"),
            "point_estimate": [500.0] * 6,
            "cum_point": [500.0, 1000.0, 1500.0, 2000.0, 2500.0, 3000.0],
            "cum_ci80_lower": [200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0],
            "cum_ci80_upper": [800.0, 1600.0, 2400.0, 3200.0, 4000.0, 4800.0],
            "interval_method": "empirical_backtest_n17",
            "method": "Prophet",
        }
    )

    fig = plot_savings_fan(
        forecast,
        hist_months=["2026-04", "2026-05", "2026-06"],
        hist_cumulative=[3500.0, 4250.0, 5000.0],
        current_balance=5000,
        target_amount=None,
        horizon=6,
    )

    trace_names = [t.name for t in fig.data]
    assert "80% band (17 backtests)" in trace_names
    assert "Cumulative balance" in trace_names
    band = next(t for t in fig.data if t.name == "80% band (17 backtests)")
    assert band.fill == "toself"
    # The band is last_hist + cumulative quantiles, not a sum of monthly bands.
    assert max(band.y) == 5000.0 + 4800.0
    assert min(band.y) == 5000.0
    assert 5000.0 + 1200.0 in list(band.y)


def _biased_backtest_forecast() -> pd.DataFrame:
    # Every backtest came in above the forecast; by month 3 the point
    # forecast (1500) is below the band (1600-2400).
    return pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=3, freq="MS"),
            "point_estimate": [500.0] * 3,
            "cum_point": [500.0, 1000.0, 1500.0],
            "cum_ci80_lower": [450.0, 1000.0, 1600.0],
            "cum_ci80_upper": [800.0, 1600.0, 2400.0],
            "backtest_cum_low_k": pd.array([12, 16, 17], dtype="Int64"),
            "backtest_cum_n": pd.array([17, 17, 17], dtype="Int64"),
            "interval_method": "empirical_backtest_n17",
            "method": "Prophet",
        }
    )


def test_plot_savings_fan_captions_backtest_bias_at_chart_horizon():
    fig = plot_savings_fan(
        _biased_backtest_forecast(),
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=None,
        horizon=3,
    )

    title = fig.layout.title.text
    assert "the actual 3-month total came in above the forecast in 17 of 17" in title
    assert "runs below its 80% band" in title


def test_plot_savings_fan_caption_follows_selected_horizon():
    # At a 2-month horizon the point (1000) is on the band's lower edge, not
    # outside it, and the count is the h=2 one.
    fig = plot_savings_fan(
        _biased_backtest_forecast(),
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=None,
        horizon=2,
    )

    title = fig.layout.title.text
    assert "2-month total came in above the forecast in 16 of 17" in title
    assert "80% band" not in title


def test_plot_savings_fan_has_no_caption_without_backtest_counts():
    forecast = _biased_backtest_forecast().drop(
        columns=["backtest_cum_low_k", "backtest_cum_n"]
    )
    forecast["interval_method"] = "model"

    fig = plot_savings_fan(
        forecast,
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=None,
        horizon=3,
    )

    assert fig.layout.title.text == "Savings Forecast"


def test_plot_savings_fan_labels_model_band_uncalibrated():
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=3, freq="MS"),
            "point_estimate": [500.0] * 3,
            "cum_point": [500.0, 1000.0, 1500.0],
            "cum_ci80_lower": [400.0, 800.0, 1200.0],
            "cum_ci80_upper": [600.0, 1200.0, 1800.0],
            "interval_method": "model",
            "method": "Prophet",
        }
    )

    fig = plot_savings_fan(
        forecast,
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=None,
        horizon=3,
    )

    trace_names = [t.name for t in fig.data]
    assert "80% band (model, uncalibrated)" in trace_names


def test_plot_savings_fan_skips_band_and_annotates_without_cum_columns():
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=3, freq="MS"),
            "point_estimate": [400.0, 400.0, 400.0],
            "ci80_lower": [300.0] * 3,
            "ci80_upper": [500.0] * 3,
            "ci95_lower": [200.0] * 3,
            "ci95_upper": [600.0] * 3,
            "method": "rolling_mean",
        }
    )

    fig = plot_savings_fan(
        forecast,
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=None,
        horizon=3,
    )

    trace_names = [t.name for t in fig.data]
    assert "Cumulative balance" in trace_names
    assert not any("band" in name.lower() for name in trace_names)
    assert any(
        "just run" in (a.text or "") for a in fig.layout.annotations or []
    )


def test_plot_savings_fan_includes_target_hline_only_when_provided():
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=3, freq="MS"),
            "point_estimate": [500.0, 500.0, 500.0],
            "ci80_lower": [float("nan")] * 3,
            "ci80_upper": [float("nan")] * 3,
            "ci95_lower": [float("nan")] * 3,
            "ci95_upper": [float("nan")] * 3,
            "method": "Prophet",
        }
    )

    fig_with_target = plot_savings_fan(
        forecast,
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=5000,
        horizon=3,
    )
    fig_without_target = plot_savings_fan(
        forecast,
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=None,
        horizon=3,
    )
    fig_zero_target = plot_savings_fan(
        forecast,
        hist_months=["2026-06"],
        hist_cumulative=[0.0],
        current_balance=0,
        target_amount=0,
        horizon=3,
    )

    hline_texts_with = [a.text for a in fig_with_target.layout.annotations or []]
    hline_texts_without = [a.text for a in fig_without_target.layout.annotations or []]
    hline_texts_zero = [a.text for a in fig_zero_target.layout.annotations or []]

    assert any("Target" in t for t in hline_texts_with)
    assert not any("Target" in t for t in hline_texts_without)
    assert not any("Target" in t for t in hline_texts_zero)


def test_plot_category_trend_returns_figure_with_bar_and_line():
    idx = pd.date_range("2025-01-01", periods=6, freq="MS")
    hist = pd.Series([100.0, 110.0, 120.0, 130.0, 140.0, 150.0], index=idx)
    trend = pd.Series([95.0, 105.0, 115.0, 125.0, 135.0, 145.0], index=idx)

    fig = plot_category_trend(hist, trend, title="Food Trend")

    assert isinstance(fig, go.Figure)
    names = [t.name for t in fig.data]
    assert "Monthly amount" in names
    assert "Prophet trend" in names
    assert fig.layout.title.text == "Food Trend"
    assert fig.layout.hovermode == "x unified"
    assert fig.layout.yaxis.tickformat == ",.0f"


def test_plot_category_trend_skips_line_when_trend_all_nan():
    idx = pd.date_range("2025-01-01", periods=6, freq="MS")
    hist = pd.Series([100.0] * 6, index=idx)
    trend = pd.Series([float("nan")] * 6, index=idx)

    fig = plot_category_trend(hist, trend)

    names = [t.name for t in fig.data]
    assert "Monthly amount" in names
    assert "Prophet trend" not in names


def test_plot_category_forecast_returns_figure_with_ci_ribbons():
    idx = pd.date_range("2025-01-01", periods=6, freq="MS")
    hist = pd.Series([100.0] * 6, index=idx)
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-01-01", periods=12, freq="MS"),
            "point_estimate": [110.0] * 12,
            "ci80_lower": [100.0] * 12,
            "ci80_upper": [120.0] * 12,
            "ci95_lower": [90.0] * 12,
            "ci95_upper": [130.0] * 12,
            "method": ["Prophet"] * 12,
        }
    )

    fig = plot_category_forecast(hist, forecast, title="Food Forecast", horizon_months=12)

    names = [t.name for t in fig.data]
    assert "Historical" in names
    assert "95% CI" in names
    assert "80% CI" in names
    assert "Forecast (Prophet)" in names
    assert "12 months" in fig.layout.title.text


def test_plot_category_forecast_skips_ci_when_nan():
    idx = pd.date_range("2025-01-01", periods=6, freq="MS")
    hist = pd.Series([100.0] * 6, index=idx)
    forecast = pd.DataFrame(
        {
            "month": pd.date_range("2026-01-01", periods=3, freq="MS"),
            "point_estimate": [400.0, 400.0, 400.0],
            "ci80_lower": [float("nan")] * 3,
            "ci80_upper": [float("nan")] * 3,
            "ci95_lower": [float("nan")] * 3,
            "ci95_upper": [float("nan")] * 3,
            "method": ["Prophet"] * 3,
        }
    )

    fig = plot_category_forecast(hist, forecast)

    names = [t.name for t in fig.data]
    assert "Historical" in names
    assert "95% CI" not in names
    assert "80% CI" not in names
    assert "Forecast (Prophet)" in names


# ---------------------------------------------------------------------------
# Budget health charts
# ---------------------------------------------------------------------------


def _is_empty_state(fig: go.Figure) -> bool:
    return len(fig.data) == 0 and len(fig.layout.annotations) == 1


def test_plot_spending_plan_vs_target_draws_bars_and_target_bands():
    shares = {"fixed": 0.4, "guilt_free": 0.3, "savings": 0.05, "investments": 0.0,
              "one_off": 0.0, "unallocated": 0.25}
    targets = {"fixed": [0.5, 0.6], "savings": [0.05, 0.10]}

    fig = plot_spending_plan_vs_target(shares, targets, "2026-09")

    assert len(fig.data) == 1
    assert list(fig.data[0].y)[-1] == "Fixed costs"
    assert len(fig.layout.shapes) == 2
    assert "2026-09" in fig.layout.title.text


def test_plot_spending_plan_vs_target_empty_without_shares():
    assert _is_empty_state(plot_spending_plan_vs_target({}, {}, None))


def test_plot_take_home_allocation_one_line_per_bucket():
    monthly = pd.DataFrame(
        {
            "year_month": ["2025-12", "2026-01"],
            "take_home_ttm": [np.nan, 100.0],
            "fixed_share_ttm": [np.nan, 0.5],
            "guilt_free_share_ttm": [np.nan, 0.2],
            "savings_share_ttm": [np.nan, 0.1],
            "investments_share_ttm": [np.nan, np.nan],
            "one_off_share_ttm": [np.nan, 0.0],
            "unallocated_share_ttm": [np.nan, 0.2],
        }
    )

    fig = plot_take_home_allocation(monthly)

    assert [t.name for t in fig.data] == [
        "Fixed costs",
        "Guilt-free",
        "Savings goals",
        "Investments",
        "One-off",
        "Unallocated",
    ]
    assert len(fig.data[0].x) == 1
    assert fig.data[2].y[0] == pytest.approx(0.1)
    savings_color = fig.data[2].line.color
    investments_color = fig.data[3].line.color
    assert savings_color != investments_color
    assert fig.data[5].line.color == "#008300"


def test_plot_balances_by_group_stacks_known_groups():
    balances = pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-01", "2026-02"],
            "group": ["cash", "savings", "cash"],
            "balance_pln": [100.0, 50.0, 120.0],
        }
    )

    fig = plot_balances_by_group(balances)

    assert len(fig.data) == 5
    assert all(t.stackgroup == "balances" for t in fig.data)
    assert list(fig.data[1].y) == [50.0, 0.0]


def test_plot_balances_by_group_empty_without_balances():
    assert _is_empty_state(plot_balances_by_group(None))


def test_plot_lifestyle_creep_two_bar_series():
    annual = pd.DataFrame(
        {
            "year": [2024, 2025],
            "take_home": [100.0, 150.0],
            "living_spend": [60.0, 70.0],
            "living_share": [0.6, 0.47],
            "marginal_spend_rate": [np.nan, 0.2],
            "delta_take_home": [np.nan, 50.0],
            "delta_living_spend": [np.nan, 10.0],
        }
    )

    fig = plot_lifestyle_creep(annual)

    assert [t.type for t in fig.data] == ["bar", "bar"]
    assert list(fig.data[0].x) == ["2024", "2025"]
    # Both deltas are exposed to the hover text.
    custom = fig.data[1].customdata
    assert custom.shape == (2, 4)
    assert custom[1][2] == 50.0
    assert custom[1][3] == 10.0
    hover = fig.data[1].hovertemplate
    assert "Δ take-home" in hover
    assert "Δ living spend" in hover


def test_plot_reserve_vs_ceiling_two_lines():
    trend = pd.DataFrame(
        {
            "year_month": ["2025-11", "2025-12"],
            "savings_balance": [10_000.0, 36_000.0],
            "ceiling": [np.nan, 22_800.0],
            "above_ceiling": [np.nan, 13_200.0],
        }
    )

    fig = plot_reserve_vs_ceiling(trend)

    assert [t.name for t in fig.data] == [
        "Reserve (savings accounts)",
        "6-month ceiling",
    ]
    assert [t.type for t in fig.data] == ["scatter", "scatter"]
    assert fig.layout.title.text.endswith("2 months to 2025-12")
    assert list(fig.data[0].y) == [10_000.0, 36_000.0]


def test_plot_reserve_vs_ceiling_empty_state():
    fig = plot_reserve_vs_ceiling(pd.DataFrame())
    assert _is_empty_state(fig)


def _waterfall_values(savings: float = 500.0) -> dict[str, float]:
    return {
        "gross_income": 30_000.0,
        "deductions": 6_000.0,
        "take_home": 24_000.0,
        "fixed": 4_000.0,
        "guilt_free": 3_000.0,
        "one_off": 500.0,
        "unmapped": 0.0,
        "savings": savings,
        "investments": 0.0,
        "unallocated": 16_000.0,
    }


def test_plot_take_home_waterfall_measure_list():
    fig = plot_take_home_waterfall(_waterfall_values(), "12 months to 2026-09")

    assert len(fig.data) == 1
    assert fig.data[0].type == "waterfall"
    assert list(fig.data[0].measure) == [
        "absolute",
        "relative",
        "total",
        "relative",
        "relative",
        "relative",
        "relative",
        "relative",
        "total",
    ]
    assert fig.data[0].x[6] == "To savings"
    assert fig.data[0].x[1] == "Taxes<br>(VAT, PIT, ZUS)"
    assert fig.data[0].x[5] == "One-off &<br>unmapped"
    assert fig.data[0].y[1] == -6_000.0
    assert list(fig.data[0].text) == [
        "30k", "−6k", "24k", "−4k", "−3k", "−500", "−500", "0", "16k",
    ]
    assert fig.data[0].texttemplate == "%{text}"
    assert fig.data[0].textposition == "auto"
    assert fig.layout.xaxis.tickangle == 0


def test_plot_take_home_waterfall_labels_savings_drawdown_as_from_savings():
    fig = plot_take_home_waterfall(_waterfall_values(savings=-300.0), "September 2026")

    assert fig.data[0].x[6] == "From savings"
    assert fig.data[0].y[6] == 300.0


def test_plot_take_home_waterfall_empty_without_gross_income():
    assert _is_empty_state(plot_take_home_waterfall({}, "No data"))


def test_plot_reserve_transfers_two_bar_series():
    transfers = pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-02"],
            "is_partial_month": [False, False],
            "moved_to_cash": [1_000.0, 1_200.0],
        }
    )
    monthly = pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-02"],
            "deductions": [2_000.0, 2_100.0],
            "fixed": [3_000.0, 3_100.0],
            "guilt_free": [1_000.0, 1_100.0],
            "one_off": [0.0, 0.0],
            "unmapped": [0.0, 0.0],
        }
    )

    fig = plot_reserve_transfers(transfers, monthly)

    assert [t.type for t in fig.data] == ["bar", "bar"]
    assert [t.name for t in fig.data] == [
        "Moved from reserve to spending accounts",
        "Spent + taxes paid",
    ]
    assert list(fig.data[1].y) == [6_000.0, 6_300.0]
    assert "2 complete months" in fig.layout.title.text


def test_plot_big_wins_three_lines_with_band():
    scenario = pd.DataFrame(
        {
            "month": list(range(13)),
            "contributions": [0.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0,
                              700.0, 800.0, 900.0, 1_000.0, 1_100.0, 1_200.0],
            "kept_in_reserve": [0.0] * 13,
            "invested_low": [0.0, 100.0, 210.0, 330.0, 460.0, 600.0, 750.0,
                             910.0, 1_080.0, 1_260.0, 1_450.0, 1_650.0, 1_860.0],
            "invested_high": [0.0, 110.0, 230.0, 360.0, 500.0, 650.0, 810.0,
                              980.0, 1_160.0, 1_350.0, 1_550.0, 1_760.0, 1_980.0],
        }
    )

    fig = plot_big_wins(scenario)

    assert [t.name for t in fig.data] == [
        "Kept in reserve",
        "Invested at your low rate",
        "Invested at your high rate",
    ]
    assert fig.data[2].fill == "tonexty"
    assert fig.data[2].fillcolor == "rgba(27,175,122,0.18)"
    assert fig.data[1].line.color == fig.data[2].line.color
    assert fig.data[1].line.dash == "dash"
    assert fig.data[2].line.dash is None
    assert fig.layout.title.text == "Scenario at your assumed rates, not a forecast"
    assert fig.layout.xaxis.title.text == "Years"
    assert fig.data[0].x[-1] == 1.0


def test_plot_big_wins_empty_state():
    assert _is_empty_state(plot_big_wins(pd.DataFrame()))
