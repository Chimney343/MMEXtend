"""Tests for dashboard drill-down callbacks."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import html

from dashboard import callbacks
from dashboard.charts import plot_recurring_by_category


def _collect_ids(component) -> set[str]:
    ids: set[str] = set()
    stack = [component]

    while stack:
        node = stack.pop()
        node_id = getattr(node, "id", None)
        if isinstance(node_id, str):
            ids.add(node_id)

        children = getattr(node, "children", None)
        if children is None:
            continue

        if isinstance(children, list):
            stack.extend(children)
        else:
            stack.append(children)

    return ids


def _make_synthetic_forecast(n_months: int = 6) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "month": pd.date_range("2026-07-01", periods=n_months, freq="MS"),
            "point_estimate": [500.0] * n_months,
            "ci80_lower": [300.0] * n_months,
            "ci80_upper": [700.0] * n_months,
            "ci95_lower": [200.0] * n_months,
            "ci95_upper": [800.0] * n_months,
            "method": "Prophet",
        }
    )


def _make_synthetic_cashflow_split() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-02", "2026-03"],
            "deposits": [5000.0, 5000.0, 5000.0],
            "withdrawals": [4000.0, 4000.0, 4000.0],
            "disposable_income": [1000.0, 1000.0, 1000.0],
            "savings_rate": [0.2, 0.2, 0.2],
        }
    )


def test_expense_monthly_agg_keeps_only_expense_flow():
    monthly = pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-01"],
            "category": ["Food", "Income"],
            "flow_type": ["expense", "income"],
            "total_amount": [400.0, 3000.0],
        }
    )

    result = callbacks._expense_monthly_agg(monthly)

    assert result["category"].tolist() == ["Food"]
    assert result["total_amount"].tolist() == [400.0]


def test_expense_monthly_agg_supports_legacy_signed_aggregates():
    monthly = pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-01"],
            "category": ["Food", "Income"],
            "total_amount": [-400.0, 3000.0],
        }
    )

    result = callbacks._expense_monthly_agg(monthly)

    assert result["category"].tolist() == ["Food"]
    assert result["total_amount"].tolist() == [400.0]


def test_apply_aggregation_returns_raw_series_for_monthly_mode():
    series = pd.Series([100.0, 50.0, 75.0])

    result = callbacks._apply_aggregation(series, "monthly")

    assert result.tolist() == [100.0, 50.0, 75.0]


def test_apply_aggregation_returns_rolling_means_for_3m_and_6m():
    series = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0, 60.0])

    result_3m = callbacks._apply_aggregation(series, "rolling_3m")
    result_6m = callbacks._apply_aggregation(series, "rolling_6m")

    assert result_3m.tolist() == [10.0, 15.0, 20.0, 30.0, 40.0, 50.0]
    assert result_6m.tolist() == [10.0, 15.0, 20.0, 25.0, 30.0, 35.0]


def test_drilldown_panel_includes_aggregate_dropdown(monkeypatch):
    featured = pd.DataFrame(
        {
            "category": ["Food", "Food"],
            "year_month": ["2026-01", "2026-02"],
        }
    )
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: featured)

    panel = callbacks._drilldown_panel()
    component_ids = _collect_ids(panel)

    assert "drilldown-category" in component_ids
    assert "drilldown-aggregate" in component_ids


def test_subcategory_panel_excludes_future_rows(monkeypatch):
    monthly = pd.DataFrame(
        {
            "year_month": ["2026-01"],
            "category": ["Food"],
            "flow_type": ["expense"],
            "total_amount": [400.0],
        }
    )
    featured = pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-09"],
            "type": ["withdrawal", "withdrawal"],
            "category": ["Food", "Food"],
            "subcategory": ["Groceries", "FutureSnacks"],
            "amount": [-400.0, -999.0],
            "is_transfer": [False, False],
            "is_future": [False, True],
        }
    )

    def _mock_load(stage: str, name: str):
        if name == "monthly_aggregates":
            return monthly
        if name == "transactions_featured":
            return featured
        return None

    monkeypatch.setattr(callbacks, "load_parquet", _mock_load)

    panel = callbacks._subcategory_panel()
    figure = panel.children[2].figure
    by_name = {trace.name: trace for trace in figure.data}

    assert "FutureSnacks" not in by_name
    assert by_name["Groceries"].y[0] == 400.0


def test_savings_panel_includes_expected_component_ids(monkeypatch):
    forecast = _make_synthetic_forecast()
    cashflow_split = _make_synthetic_cashflow_split()

    def _mock_load(stage: str, name: str):
        if stage == "processed" and name == "cashflow_forecast":
            return forecast
        if stage == "interim" and name == "monthly_cashflow_split":
            return cashflow_split
        return None

    monkeypatch.setattr(callbacks, "load_parquet", _mock_load)

    panel = callbacks._savings_panel()
    component_ids = _collect_ids(panel)

    assert "savings-balance" in component_ids
    assert "savings-target" in component_ids
    assert "savings-horizon" in component_ids
    assert "savings-chart" in component_ids
    assert "savings-summary" in component_ids


def test_savings_panel_shows_missing_when_forecast_absent(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    panel = callbacks._savings_panel()
    component_ids = _collect_ids(panel)

    assert "savings-balance" not in component_ids


def _savings_load_parquet_mock(stage: str, name: str):
    if stage == "processed" and name == "cashflow_forecast":
        return _make_synthetic_forecast(12)
    if stage == "interim" and name == "monthly_cashflow_split":
        return _make_synthetic_cashflow_split()
    return None


def test_update_savings_forecast_returns_figure_and_summary(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _savings_load_parquet_mock)

    fig, summary = callbacks.update_savings_forecast(5000, 10000, 6)

    assert fig is not None
    assert fig.layout.title.text == "Savings Forecast"
    assert len(fig.data) >= 1
    assert "Projected balance after 6 months" in str(summary)


def test_update_savings_forecast_shows_target_reached(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _savings_load_parquet_mock)

    fig, summary = callbacks.update_savings_forecast(0, 1000, 6)

    summary_str = str(summary)
    assert "reached" in summary_str.lower()
    assert "1,000" in summary_str


def test_update_savings_forecast_shows_target_unreachable(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _savings_load_parquet_mock)

    fig, summary = callbacks.update_savings_forecast(0, 999999, 6)

    summary_str = str(summary)
    assert "not reachable" in summary_str.lower()


def test_update_savings_forecast_handles_none_inputs(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _savings_load_parquet_mock)

    fig, summary = callbacks.update_savings_forecast(None, None, None)

    assert fig is not None
    assert fig.layout.title.text == "Savings Forecast"


def test_update_savings_forecast_handles_missing_forecast(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    fig, summary = callbacks.update_savings_forecast(5000, 10000, 6)

    assert "Pipeline data not found" in fig.layout.annotations[0].text


# ---------------------------------------------------------------------------
# Category Trends & Category Forecasts panels
# ---------------------------------------------------------------------------


def _make_category_summary() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "flow_type": ["expense", "expense", "expense"],
            "level": ["category", "subcategory", "subcategory"],
            "category": ["Food", "Food", "Food"],
            "subcategory": [None, "Groceries", "Dining"],
            "n_months": [12, 12, 12],
            "first_month": pd.to_datetime(["2025-01-01"] * 3),
            "last_month": pd.to_datetime(["2025-12-01"] * 3),
            "mean_monthly": [500.0, 400.0, 100.0],
            "trend_start": [450.0, 350.0, 90.0],
            "trend_end": [550.0, 450.0, 110.0],
            "trend_change_pln": [100.0, 100.0, 20.0],
            "avg_monthly_change_pln": [9.09, 9.09, 1.8],
            "growth_pct": [20.0, 25.0, 20.0],
            "direction": ["rising", "rising", "rising"],
            "trend_pvalue": [0.001, 0.002, 0.003],
            "direction_pvalue_adj": [0.006, 0.006, 0.006],
        }
    )


def _make_category_series() -> pd.DataFrame:
    months = pd.date_range("2025-01-01", periods=12, freq="MS")
    rows = []
    for m in months:
        rows.append(
            {
                "flow_type": "expense",
                "level": "category",
                "category": "Food",
                "subcategory": None,
                "month": m,
                "y": 500.0,
                "trend": 480.0,
            }
        )
        rows.append(
            {
                "flow_type": "expense",
                "level": "subcategory",
                "category": "Food",
                "subcategory": "Groceries",
                "month": m,
                "y": 400.0,
                "trend": 380.0,
            }
        )
    return pd.DataFrame(rows)


def _make_category_forecast() -> pd.DataFrame:
    months = pd.date_range("2026-01-01", periods=36, freq="MS")
    rows = []
    for i, m in enumerate(months):
        rows.append(
            {
                "flow_type": "expense",
                "level": "category",
                "category": "Food",
                "subcategory": None,
                "months_ahead": i + 1,
                "month": m,
                "point_estimate": 520.0,
                "ci80_lower": 480.0,
                "ci80_upper": 560.0,
                "ci95_lower": 460.0,
                "ci95_upper": 580.0,
                "method": "Prophet",
            }
        )
    return pd.DataFrame(rows)


def _cat_load_parquet_mock(stage: str, name: str):
    if stage == "processed" and name == "category_trends_summary":
        return _make_category_summary()
    if stage == "processed" and name == "category_trends_series":
        return _make_category_series()
    if stage == "processed" and name == "category_forecasts":
        return _make_category_forecast()
    return None


def test_cat_trends_panel_contains_expected_ids(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _cat_load_parquet_mock)

    panel = callbacks._cat_trends_panel()
    component_ids = _collect_ids(panel)

    for expected in (
        "cat-trends-type",
        "cat-trends-include-subcats",
        "cat-trends-summary",
        "cat-trends-category",
        "cat-trends-subcategory",
        "cat-trends-chart",
    ):
        assert expected in component_ids, f"Missing id: {expected}"


def test_cat_trends_panel_shows_alert_when_data_absent(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    panel = callbacks._cat_trends_panel()
    component_ids = _collect_ids(panel)

    assert "cat-trends-type" not in component_ids


def test_cat_forecast_panel_contains_expected_ids(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _cat_load_parquet_mock)

    panel = callbacks._cat_forecast_panel()
    component_ids = _collect_ids(panel)

    for expected in (
        "cat-forecast-type",
        "cat-forecast-category",
        "cat-forecast-subcategory",
        "cat-forecast-1y",
        "cat-forecast-2y",
        "cat-forecast-3y",
    ):
        assert expected in component_ids, f"Missing id: {expected}"


def test_update_cat_forecast_charts_returns_three_figures(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _cat_load_parquet_mock)

    figs = callbacks.update_cat_forecast_charts("Food", "__ALL__", "expense")

    assert len(figs) == 3
    for fig in figs:
        assert fig is not None


def test_update_cat_forecast_charts_handles_missing_data(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    figs = callbacks.update_cat_forecast_charts("Food", "__ALL__", "expense")

    assert len(figs) == 3
    for fig in figs:
        assert "Category forecast data not found" in fig.layout.annotations[0].text


def test_update_cat_trends_chart_returns_figure(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _cat_load_parquet_mock)

    fig = callbacks.update_cat_trends_chart("Food", "__ALL__", "expense")

    assert fig is not None
    assert "Food" in fig.layout.title.text


# ---------------------------------------------------------------------------
# Recurring Costs panel & callback
# ---------------------------------------------------------------------------


def _make_synthetic_register() -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    return pd.DataFrame(
        {
            "payee": ["Rent", "ZUS", "Streaming", "Annual Policy"],
            "category": ["Housing", "Taxes", "Subscriptions", "Insurance"],
            "subcategory": ["Rent", "ZUS", None, "Policy"],
            "n_transactions": [12, 12, 12, 6],
            "n_distinct_months": [12, 12, 12, 6],
            "first_seen": [
                today - pd.Timedelta(days=365),
                today - pd.Timedelta(days=365),
                today - pd.Timedelta(days=365),
                today - pd.Timedelta(days=2000),
            ],
            "last_seen": [
                today - pd.Timedelta(days=10),
                today - pd.Timedelta(days=10),
                today - pd.Timedelta(days=10),
                today - pd.Timedelta(days=347),
            ],
            "median_gap_days": [31.0, 31.0, 30.0, 365.0],
            "cadence_class": ["monthly", "monthly", "monthly", "annual"],
            "is_active": [True, True, False, False],
            "amount_current": [1350.0, 2510.0, 20.0, 1000.0],
            "amount_median": [1350.0, 2510.0, 20.0, 1000.0],
            "amount_cv_trailing": [0.0, 0.02, 0.0, 0.0],
            "monthly_run_rate": [1350.0, 2491.67, 20.0, 83.33],
            "run_rate_window_months": [12, 12, 12, 12],
            "next_expected_date": [
                today + pd.Timedelta(days=20),
                today + pd.Timedelta(days=25),
                today - pd.Timedelta(days=5),
                today + pd.Timedelta(days=18),
            ],
            "days_overdue": [0, 0, 5, 0],
            "is_lapsed": [False, False, True, False],
            "n_price_changes": [0, 1, 1, 0],
            "last_price_change_date": [
                pd.NaT,
                today - pd.Timedelta(days=200),
                today - pd.Timedelta(days=40),
                pd.NaT,
            ],
            "last_price_change_pct": [np.nan, 0.0458, 0.2, np.nan],
            "has_price_change_12m": [False, True, True, False],
            "is_renewal_due_30d": [True, True, False, True],
        }
    )


def _make_synthetic_events() -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    months = pd.date_range(end=today, periods=12, freq="MS")
    rows = []
    for i, month in enumerate(months):
        rows.append(
            {
                "payee": "Rent",
                "date": month,
                "year_month": month.strftime("%Y-%m"),
                "amount": 1350.0,
                "prev_amount": 1350.0 if i else np.nan,
                "is_price_change": False,
            }
        )
        amount = 2510.0 if i >= 2 else 2400.0
        prev = np.nan if i == 0 else (2400.0 if i <= 2 else 2510.0)
        rows.append(
            {
                "payee": "ZUS",
                "date": month,
                "year_month": month.strftime("%Y-%m"),
                "amount": amount,
                "prev_amount": prev,
                "is_price_change": i == 2,
            }
        )
    return pd.DataFrame(rows)


def _recurring_load_parquet_mock(stage: str, name: str):
    if stage == "processed" and name == "recurring_commitments":
        return _make_synthetic_register()
    if stage == "processed" and name == "recurring_commitment_events":
        return _make_synthetic_events()
    if stage == "processed" and name == "recurring_categories":
        return _make_synthetic_category_summary()
    if stage == "interim" and name == "monthly_cashflow_split":
        return _make_synthetic_cashflow_split()
    return None


def test_recurring_panel_contains_expected_ids(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _recurring_load_parquet_mock)

    panel = callbacks._recurring_panel()
    component_ids = _collect_ids(panel)

    for expected in (
        "recurring-category-chart",
        "recurring-category-table",
        "recurring-runrate-chart",
        "recurring-register-table",
        "recurring-renewal-table",
        "recurring-payee-select",
        "recurring-payee-chart",
    ):
        assert expected in component_ids, f"Missing id: {expected}"


def test_recurring_panel_shows_missing_when_register_absent(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    panel = callbacks._recurring_panel()
    component_ids = _collect_ids(panel)

    assert "recurring-register-table" not in component_ids


def test_update_recurring_payee_chart_returns_history_for_known_payee(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _recurring_load_parquet_mock)

    fig = callbacks.update_recurring_payee_chart("Rent")

    assert fig is not None
    assert "Rent" in fig.layout.title.text
    assert len(fig.data) >= 1


def test_update_recurring_payee_chart_returns_empty_for_unknown_payee(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _recurring_load_parquet_mock)

    fig = callbacks.update_recurring_payee_chart("Unknown Payee")

    assert "No transaction history" in fig.layout.annotations[0].text


def test_update_recurring_payee_chart_handles_missing_data(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    fig = callbacks.update_recurring_payee_chart("Rent")

    assert "not found" in fig.layout.annotations[0].text

def test_recurring_panel_renders_empty_register_without_crashing(monkeypatch):
    from src.analysis.recurring_commitments import EVENT_COLUMNS, REGISTER_COLUMNS

    empty_register = pd.DataFrame(columns=REGISTER_COLUMNS)
    empty_events = pd.DataFrame(columns=EVENT_COLUMNS)

    def _empty_load(stage: str, name: str):
        if name == "recurring_commitments":
            return empty_register
        if name == "recurring_commitment_events":
            return empty_events
        return None

    monkeypatch.setattr(callbacks, "load_parquet", _empty_load)

    panel = callbacks._recurring_panel()
    component_ids = _collect_ids(panel)

    assert "recurring-register-table" in component_ids
    assert "recurring-renewal-table" in component_ids

def test_renewal_table_includes_annual_renewal_and_excludes_lapsed():
    register = _make_synthetic_register()
    as_of = pd.Timestamp.today().normalize()

    table = callbacks._recurring_renewal_table(register, as_of)
    text = str(table)

    assert "Annual Policy" in text
    assert "Streaming" not in text


def test_share_kpi_ignores_partial_current_month():
    today = pd.Timestamp.today().normalize()
    current_month_start = today.replace(day=1)
    complete_months = pd.date_range(
        end=current_month_start - pd.DateOffset(months=1), periods=12, freq="MS"
    )
    split = pd.DataFrame(
        {
            "year_month": [m.strftime("%Y-%m") for m in complete_months]
            + [today.strftime("%Y-%m")],
            "withdrawals": [1000.0] * 12 + [99999.0],
        }
    )

    value, subtitle = callbacks._recurring_ttm_share(100.0, split, today)

    assert value == "10.0%"
    assert subtitle == "vs average TTM month"

def _make_synthetic_category_summary() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "category": ["Bills", "Taxes", "Food"],
            "n_months": [109, 110, 110],
            "active_months_ttm": [12, 12, 12],
            "last_month": ["2026-10", "2026-10", "2026-10"],
            "cadence_class": ["monthly", "monthly", "monthly"],
            "is_active": [True, True, True],
            "avg_monthly_spend": [2500.0, 14000.0, 1900.0],
            "spend_cv": [0.13, 0.22, 0.25],
            "is_steady": [True, True, True],
            "n_commitments": [2, 0, 2],
            "n_active_commitments": [2, 0, 2],
            "n_lapsed_commitments": [0, 0, 0],
            "recurring_run_rate": [1523.58, 0.0, 73.49],
            "recurring_coverage": [0.609, 0.0, 0.039],
            "share_of_spend": [0.14, 0.76, 0.10],
            "n_price_changes_12m": [1, 0, 2],
            "top_payee": ["Property Manager", "\u2014", "gym.example"],
        }
    )

def test_recurring_panel_handles_missing_category_summary(monkeypatch):
    register = _make_synthetic_register()
    events = _make_synthetic_events()

    def _missing_category(stage: str, name: str):
        if name == "recurring_commitments":
            return register
        if name == "recurring_commitment_events":
            return events
        if name == "monthly_cashflow_split":
            return _make_synthetic_cashflow_split()
        return None

    monkeypatch.setattr(callbacks, "load_parquet", _missing_category)

    panel = callbacks._recurring_panel()
    component_ids = _collect_ids(panel)

    assert "recurring-category-empty" in component_ids
    assert "recurring-category-table" not in component_ids
    assert "recurring-register-table" in component_ids


def test_plot_recurring_by_category_empty_state():
    fig = plot_recurring_by_category(pd.DataFrame())

    assert "No category summary available" in fig.layout.annotations[0].text


# ---------------------------------------------------------------------------
# Budget Health tab
# ---------------------------------------------------------------------------


def _make_synthetic_budget_monthly(n_months: int = 13) -> pd.DataFrame:
    months = pd.period_range("2025-01", periods=n_months, freq="M").strftime("%Y-%m")
    df = pd.DataFrame(
        {
            "year_month": months,
            "is_partial_month": [False] * n_months,
            "gross_income": 10_000.0,
            "deductions": 2_000.0,
            "take_home": 8_000.0,
            "fixed": 3_800.0,
            "guilt_free": 1_200.0,
            "one_off": 0.0,
            "unmapped": 0.0,
            "savings": 500.0,
            "investments": 0.0,
            "unallocated": 2_500.0,
        }
    )
    value_cols = ["gross_income", "deductions", "take_home", "fixed", "guilt_free",
                  "one_off", "unmapped", "savings", "investments", "unallocated"]
    ttm = df[value_cols].rolling(12, min_periods=12).sum().add_suffix("_ttm")
    df = df.join(ttm)
    for col in ("fixed", "guilt_free", "one_off", "unmapped", "savings", "investments",
                "unallocated"):
        df[f"{col}_share_ttm"] = df[f"{col}_ttm"] / df["take_home_ttm"]
    return df


def _make_synthetic_balances() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-01", "2026-01"],
            "account": ["Checking", "Savings", "EUR"],
            "account_type": ["Checking", "Checking", "Checking"],
            "group": ["cash", "savings", "cash"],
            "currency": ["PLN", "PLN", "EUR"],
            "balance": [5_000.0, 6_000.0, 1_000.0],
            "fx_rate": [1.0, 1.0, 4.3],
            "balance_pln": [5_000.0, 6_000.0, 4_300.0],
        }
    )


def _budget_load_parquet_mock(stage: str, name: str):
    if stage == "processed" and name == "budget_health_monthly":
        return _make_synthetic_budget_monthly()
    if stage == "processed" and name == "account_balances":
        return _make_synthetic_balances()
    return None


def test_budget_health_panel_contains_expected_ids(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _budget_load_parquet_mock)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    component_ids = _collect_ids(callbacks._budget_health_panel())

    for expected in (
        "budget-mapping-warning",
        "budget-next100-card",
        "budget-scorecard-table",
        "budget-reserve-chart",
        "budget-plan-chart",
        "budget-allocation-chart",
        "budget-balances-chart",
        "budget-balances-table",
        "budget-creep-chart",
        "budget-creep-table",
    ):
        assert expected in component_ids, f"Missing id: {expected}"


def test_budget_health_panel_hides_warning_once_mapping_confirmed(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _budget_load_parquet_mock)
    monkeypatch.setattr(
        callbacks, "_budget_health_config", lambda: {"mapping_confirmed": True}
    )

    component_ids = _collect_ids(callbacks._budget_health_panel())

    assert "budget-mapping-warning" not in component_ids
    assert "budget-scorecard-table" in component_ids


def test_budget_health_panel_without_balances_still_renders(monkeypatch):
    def loader(stage: str, name: str):
        return _make_synthetic_budget_monthly() if name == "budget_health_monthly" else None

    monkeypatch.setattr(callbacks, "load_parquet", loader)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    component_ids = _collect_ids(callbacks._budget_health_panel())

    assert "budget-scorecard-table" in component_ids
    assert "budget-balances-table" in component_ids  # info alert keeps the id


def test_budget_health_panel_shows_missing_when_monthly_absent(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    panel = callbacks._budget_health_panel()

    assert panel is callbacks._MISSING


def test_fmt_check_value_uses_unit():
    assert callbacks._fmt_check_value({"value": 0.256, "unit": "share"}) == "25.6%"
    assert callbacks._fmt_check_value({"value": 3.25, "unit": "months"}) == "3.2 months"
    assert callbacks._fmt_check_value({"value": -250.0, "unit": "currency"}) == "-250 PLN"
    assert callbacks._fmt_check_value({"value": None}) == "—"


def _collect_h6_text(component) -> list[str]:
    texts: list[str] = []
    stack = [component]
    while stack:
        node = stack.pop()
        if isinstance(node, html.H6):
            texts.append(str(node.children))
        children = getattr(node, "children", None)
        if children is None:
            continue
        if isinstance(children, list):
            stack.extend(children)
        else:
            stack.append(children)
    return texts


def test_budget_health_kpis_have_no_saved_and_invested_card(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _budget_load_parquet_mock)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    panel = callbacks._budget_health_panel()
    headings = _collect_h6_text(panel)

    assert "Saved & invested" not in headings
    assert "Investments" in headings
    assert "Savings goals" in headings
    assert "Reserve above ceiling" in headings


# ---------------------------------------------------------------------------
# Money Flow tab
# ---------------------------------------------------------------------------


def _make_synthetic_transfers() -> pd.DataFrame:
    monthly = _make_synthetic_budget_monthly()
    return pd.DataFrame(
        {
            "year_month": monthly["year_month"],
            "is_partial_month": monthly["is_partial_month"],
            "moved_to_cash": [500.0] * len(monthly),
            "n_transfers": [1] * len(monthly),
            "transfer_days": ["10"] * len(monthly),
            "income_into_reserve": [0.0] * len(monthly),
            "income_days": [""] * len(monthly),
        }
    )


def _flow_load_parquet_mock(stage: str, name: str):
    if name == "budget_health_monthly":
        return _make_synthetic_budget_monthly()
    if name == "reserve_transfers":
        return _make_synthetic_transfers()
    return None


def test_money_flow_panel_contains_expected_ids(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _flow_load_parquet_mock)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    component_ids = _collect_ids(callbacks._money_flow_panel())

    for expected in (
        "flow-month-select",
        "flow-waterfall-chart",
        "flow-transfers-chart",
        "flow-regularity-table",
        "flow-plan-adherence",
    ):
        assert expected in component_ids, f"Missing id: {expected}"


def test_money_flow_panel_without_transfers_keeps_alert_ids(monkeypatch):
    def loader(stage: str, name: str):
        return _make_synthetic_budget_monthly() if name == "budget_health_monthly" else None

    monkeypatch.setattr(callbacks, "load_parquet", loader)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    component_ids = _collect_ids(callbacks._money_flow_panel())

    assert "flow-transfers-chart" in component_ids
    assert "flow-regularity-table" in component_ids
    assert "flow-waterfall-chart" in component_ids


def test_money_flow_panel_shows_missing_when_monthly_absent(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: None)

    assert callbacks._money_flow_panel() is callbacks._MISSING


def test_update_flow_waterfall_returns_figure_for_ttm_and_month(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _flow_load_parquet_mock)

    ttm = callbacks.update_flow_waterfall("ttm")
    month = callbacks.update_flow_waterfall("2026-01")

    assert isinstance(ttm, go.Figure)
    assert isinstance(month, go.Figure)
    assert "12 months to 2026-01" in ttm.layout.title.text
    assert "2026-01" in month.layout.title.text


# ---------------------------------------------------------------------------
# Big Wins tab
# ---------------------------------------------------------------------------


def _collect_dict_ids(component) -> list[dict]:
    found: list[dict] = []
    stack = [component]
    while stack:
        node = stack.pop()
        node_id = getattr(node, "id", None)
        if isinstance(node_id, dict):
            found.append(node_id)
        children = getattr(node, "children", None)
        if children is None:
            continue
        if isinstance(children, list):
            stack.extend(children)
        else:
            stack.append(children)
    return found


def test_big_wins_panel_contains_expected_ids(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _budget_load_parquet_mock)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    component_ids = _collect_ids(callbacks._big_wins_panel())

    for expected in (
        "wins-subtabs",
        "wins-subtab-content",
        "wins-lump",
        "wins-monthly",
        "wins-raise-pct",
        "wins-rate-low",
        "wins-rate-high",
        "wins-reserve-rate",
        "wins-horizon",
        "wins-chart",
        "wins-summary",
    ):
        assert expected in component_ids, f"Missing id: {expected}"


def test_update_big_wins_empty_rates_shows_enter_your_own_alert(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _budget_load_parquet_mock)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    _figure, summary = callbacks.update_big_wins(0, 2_500, 0, None, None, None, 10)

    assert "Enter your own" in str(summary)


def test_update_big_wins_rejects_low_above_high(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _budget_load_parquet_mock)
    monkeypatch.setattr(callbacks, "_budget_health_config", lambda: {})

    figure, summary = callbacks.update_big_wins(0, 2_500, 0, 8, 4, None, 10)

    assert isinstance(figure, go.Figure)
    assert "rate_low" in str(summary)


def test_whatif_sliders_exclude_taxes_income_and_one_offs(monkeypatch):
    monthly = pd.DataFrame(
        {
            "year_month": ["2026-01"] * 5,
            "category": ["Taxes", "Income", "Wedding", "Food", "Bills"],
            "flow_type": ["expense"] * 5,
            "total_amount": [100.0] * 5,
        }
    )
    monkeypatch.setattr(callbacks, "load_parquet", lambda *_args, **_kwargs: monthly)
    monkeypatch.setattr(
        callbacks,
        "_budget_health_config",
        lambda: {
            "buckets": {"Food": "guilt_free", "Bills": "fixed", "Wedding": "one_off"},
            "income_categories": ["Income"],
            "take_home_deductions": ["Taxes"],
        },
    )

    panel = callbacks._whatif_panel()
    indices = {
        node_id["index"]
        for node_id in _collect_dict_ids(panel)
        if node_id.get("type") == "reduction-slider"
    }

    assert indices == {"Food", "Bills"}


# ---------------------------------------------------------------------------
# Restructured tabs: Spending, Runway, layout
# ---------------------------------------------------------------------------


def _make_aggregates() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-01"],
            "category": ["Food", "Income"],
            "flow_type": ["expense", "income"],
            "total_amount": [400.0, 3_000.0],
        }
    )


def _make_featured() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-02"],
            "type": ["withdrawal", "withdrawal"],
            "category": ["Food", "Food"],
            "subcategory": ["Groceries", "Dining out"],
            "amount": [-400.0, -200.0],
            "is_transfer": [False, False],
            "is_future": [False, False],
        }
    )


def _make_expense_trends() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "category": ["Food"],
            "trend_class": ["flat"],
            "trend_slope": [0.0],
            "trend_intercept": [400.0],
            "trend_r2": [0.5],
            "trend_pvalue": [0.2],
            "trend_pvalue_adj": [0.2],
            "trend_n_months": [12],
            "share_pct": [10.0],
            "is_seasonal": [False],
            "year_month": ["2026-01"],
            "month_ordinal": [0],
            "total_amount": [400.0],
        }
    )


def _all_panels_load_parquet_mock(stage: str, name: str):
    if name == "monthly_aggregates":
        return _make_aggregates()
    if name == "transactions_featured":
        return _make_featured()
    if name == "expense_trends":
        return _make_expense_trends()
    if name == "category_trends_summary":
        return _make_category_summary()
    if name == "category_trends_series":
        return _make_category_series()
    if name == "category_forecasts":
        return _make_category_forecast()
    if name == "cashflow_forecast":
        return _make_synthetic_forecast()
    if name == "monthly_cashflow_split":
        return _make_synthetic_cashflow_split()
    if name == "account_balances":
        return _make_synthetic_balances()
    return None


def _collect_h4_text(component) -> list[str]:
    texts: list[str] = []
    stack = [component]
    while stack:
        node = stack.pop()
        if isinstance(node, html.H4):
            texts.append(str(node.children))
        children = getattr(node, "children", None)
        if children is None:
            continue
        if isinstance(children, list):
            stack.extend(children)
        else:
            stack.append(children)
    return texts


def _find_component(component, component_id: str):
    stack = [component]
    while stack:
        node = stack.pop()
        if getattr(node, "id", None) == component_id:
            return node
        children = getattr(node, "children", None)
        if children is None:
            continue
        if isinstance(children, list):
            stack.extend(children)
        else:
            stack.append(children)
    return None


def test_render_tab_spending_contains_subtabs_and_default_panel(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _all_panels_load_parquet_mock)

    panel = callbacks.render_tab("tab-spending")

    assert "spending-subtabs" in _collect_ids(panel)
    assert "Category Breakdown" in _collect_h4_text(panel)


def test_render_tab_runway_contains_subtabs_and_balance_input(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _all_panels_load_parquet_mock)

    panel = callbacks.render_tab("tab-runway")

    component_ids = _collect_ids(panel)
    assert "runway-subtabs" in component_ids
    assert "savings-balance" in component_ids


def test_render_tab_overview_is_unknown(monkeypatch):
    panel = callbacks.render_tab("tab-overview")
    assert panel.children == "Unknown tab"


def test_spending_subtab_dispatch_returns_each_panel(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _all_panels_load_parquet_mock)

    expected_ids = {
        "sub-breakdown": None,
        "sub-drilldown": "drilldown-category",
        "sub-trends": "trends-category-select",
        "sub-cat-trends": "cat-trends-type",
        "sub-cat-forecast": "cat-forecast-type",
    }
    for subtab, expected_id in expected_ids.items():
        panel = callbacks.render_spending_subtab(subtab)
        if expected_id is not None:
            assert expected_id in _collect_ids(panel)


def test_runway_subtab_dispatch_returns_each_panel(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _all_panels_load_parquet_mock)

    assert "savings-balance" in _collect_ids(callbacks.render_runway_subtab("sub-savings"))
    assert "mc-balance" in _collect_ids(callbacks.render_runway_subtab("sub-mc"))
    assert "Cash Flow Forecast" in _collect_h4_text(
        callbacks.render_runway_subtab("sub-cashflow")
    )


def test_subtab_dispatch_unknown_returns_message(monkeypatch):
    monkeypatch.setattr(callbacks, "load_parquet", _all_panels_load_parquet_mock)

    assert callbacks.render_spending_subtab("bogus").children == "Unknown sub-tab"
    assert callbacks.render_runway_subtab("bogus").children == "Unknown sub-tab"
    assert callbacks.render_wins_subtab("bogus").children == "Unknown sub-tab"


def test_savings_panel_prefills_liquid_balance(monkeypatch):
    balances = pd.DataFrame(
        {
            "year_month": ["2026-01", "2026-01", "2026-01", "2026-02", "2026-02"],
            "group": ["cash", "savings", "receivables", "cash", "savings"],
            "currency": ["PLN"] * 5,
            "balance_pln": [1_000.0, 2_000.0, 5_000.0, 5_000.5, 6_000.25],
        }
    )

    def loader(stage: str, name: str):
        if name == "cashflow_forecast":
            return _make_synthetic_forecast()
        if name == "monthly_cashflow_split":
            return _make_synthetic_cashflow_split()
        if name == "account_balances":
            return balances
        return None

    monkeypatch.setattr(callbacks, "load_parquet", loader)

    panel = callbacks._savings_panel()
    balance_input = _find_component(panel, "savings-balance")
    target_input = _find_component(panel, "savings-target")

    assert balance_input.value == 11_001  # round(5,000.5 + 6,000.25) = 11,001
    assert target_input.value is None
    assert target_input.placeholder == "e.g. 50000"


def test_mc_panel_prefills_liquid_balance(monkeypatch):
    balances = pd.DataFrame(
        {
            "year_month": ["2026-02", "2026-02"],
            "group": ["cash", "savings"],
            "currency": ["PLN", "PLN"],
            "balance_pln": [5_000.0, 6_000.0],
        }
    )
    monkeypatch.setattr(
        callbacks,
        "load_parquet",
        lambda *_args, **_kwargs: balances,
    )

    panel = callbacks._mc_panel()
    assert _find_component(panel, "mc-balance").value == 11_000


def test_build_layout_has_seven_tabs_and_health_lands_first():
    from dashboard.layout import build_layout

    layout = build_layout()
    tabs = _find_component(layout, "tabs")

    tab_ids = [tab.tab_id for tab in tabs.children]
    assert tab_ids == [
        "tab-health",
        "tab-flow",
        "tab-wins",
        "tab-spending",
        "tab-recurring",
        "tab-runway",
        "tab-quality",
    ]
    assert tabs.active_tab == "tab-health"
