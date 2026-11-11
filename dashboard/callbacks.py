"""
All Dash callbacks for the MMEX Finance Dashboard.

Tab map
-------
tab-health     → Budget Health        (landing: Conscious Spending Plan scorecard)
tab-flow       → Money Flow           (take-home waterfall, reserve transfers, adherence)
tab-wins       → Big Wins             (sub-invest: invest scenario; sub-cut: cut costs)
tab-spending   → Spending             (sub-breakdown, sub-drilldown, sub-trends,
                                       sub-cat-trends, sub-cat-forecast)
tab-recurring  → Recurring Costs      (commitment register, calendar, payee chart)
tab-runway     → Runway & Forecast    (sub-savings, sub-cashflow, sub-mc)
tab-quality    → Data Quality         (notebook 00 equivalent, static)
"""
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dash
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import yaml
from dash import Input, Output, State, callback, dcc, html

from dashboard.charts import (
    apply_standard_layout,
    make_empty_figure,
    plot_balances_by_group,
    plot_big_wins,
    plot_category_forecast,
    plot_category_trend,
    plot_lifestyle_creep,
    plot_mc_fan,
    plot_payee_amount_history,
    plot_recurring_by_category,
    plot_recurring_run_rate,
    plot_reduction_scenario,
    plot_reserve_transfers,
    plot_reserve_vs_ceiling,
    plot_savings_fan,
    plot_spending_plan_vs_target,
    plot_take_home_allocation,
    plot_take_home_waterfall,
)
from dashboard.data_loader import load_parquet
from src.analysis.big_wins import build_big_wins_scenario, rule_of_72_years
from src.analysis.budget_health import (
    LIQUID_GROUPS,
    assign_buckets,
    build_annual_creep,
    build_reserve_trend,
    next_100_steps,
    summarise_budget_health,
    summarise_reserve_transfers,
)
from src.analysis.cashflow_forecast import (
    compute_expense_reduction_scenario,
    months_to_savings_target,
    run_expenditure_mc,
)
from src.analysis.feature_engineering import actual_flows, complete_cashflow_series
from src.utils.config_loader import load_config

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CATEGORIES_CONFIG = _PROJECT_ROOT / "configs" / "categories.yaml"

_MISSING = dbc.Alert(
    "Pipeline data not found. Run: poetry run python scripts/run_pipeline.py --mmb <file>",
    color="warning",
)


def _expense_monthly_agg(monthly_agg: pd.DataFrame | None) -> pd.DataFrame | None:
    """
    Return only expense rows from monthly aggregates, as positive spend.

    New-format aggregates carry ``flow_type``; legacy signed aggregates are
    supported by keeping negative rows and taking their magnitude.
    """
    if monthly_agg is None or monthly_agg.empty or "total_amount" not in monthly_agg.columns:
        return monthly_agg

    if "flow_type" in monthly_agg.columns:
        expense = monthly_agg[
            monthly_agg["flow_type"].astype(str).str.lower() == "expense"
        ].copy()
    else:
        expense = monthly_agg[
            pd.to_numeric(monthly_agg["total_amount"], errors="coerce") < 0
        ].copy()

    expense["total_amount"] = pd.to_numeric(
        expense["total_amount"], errors="coerce"
    ).abs()
    return expense


def _latest_liquid_balance() -> tuple[float | None, str | None]:
    """
    Sum cash + savings balances at the latest month end.

    Returns
    -------
    tuple[float | None, str | None]
        Whole-PLN balance and the month it comes from; ``(None, None)`` when
        ``account_balances`` is absent (CSV input).
    """
    balances = load_parquet("processed", "account_balances")
    if balances is None or balances.empty:
        return None, None
    month = balances["year_month"].max()
    latest = balances[balances["year_month"] == month]
    liquid = latest[latest["group"].isin(LIQUID_GROUPS)]
    if liquid.empty:
        return None, month
    total = liquid["balance_pln"].sum()
    if pd.isna(total):
        return None, month
    return float(round(total)), month


def _load_category_type_map() -> dict[str, str]:
    if not _CATEGORIES_CONFIG.exists():
        return {}
    with open(_CATEGORIES_CONFIG) as fh:
        cfg = yaml.safe_load(fh) or {}
    cats = cfg.get("categories") or {}
    return {k: (v or {}).get("type", "variable") for k, v in cats.items()}


_CATEGORY_TYPE_MAP: dict[str, str] = _load_category_type_map()


# ── Tab router ────────────────────────────────────────────────────────────────


@callback(Output("tab-content", "children"), Input("tabs", "active_tab"))
def render_tab(active_tab: str) -> html.Div:
    dispatch = {
        "tab-quality":   _quality_panel,
        "tab-health":    _budget_health_panel,
        "tab-flow":      _money_flow_panel,
        "tab-wins":      _big_wins_panel,
        "tab-spending":  _spending_panel,
        "tab-recurring": _recurring_panel,
        "tab-runway":    _runway_panel,
    }
    fn = dispatch.get(active_tab)
    return fn() if fn else html.Div("Unknown tab")


# ══════════════════════════════════════════════════════════════════════════════
# Tab 1 — Data Quality
# ══════════════════════════════════════════════════════════════════════════════


def _quality_panel() -> html.Div:
    df = load_parquet("interim", "transactions_validated")
    if df is None:
        return _MISSING

    date_col = "date" in df.columns
    date_min = str(pd.to_datetime(df["date"]).min().date()) if date_col else "N/A"
    date_max = str(pd.to_datetime(df["date"]).max().date()) if date_col else "N/A"

    n_uncategorised = (
        int(df["is_uncategorised"].sum())
        if "is_uncategorised" in df.columns
        else int(df["category"].isna().sum())
    )
    key_cols = [c for c in ["date", "account", "amount", "type"] if c in df.columns]
    n_dups = int(df.duplicated(subset=key_cols).sum()) if key_cols else 0

    summary_row = dbc.Row(
        [
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Total rows"), html.H4(f"{len(df):,}")]), color="light"), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Date range"), html.P(f"{date_min} → {date_max}")]), color="light"), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Uncategorised"), html.H4(str(n_uncategorised))]), color="warning" if n_uncategorised else "light"), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Duplicates"), html.H4(str(n_dups))]), color="warning" if n_dups else "success"), width=3),
        ],
        className="mb-4",
    )

    # Null rate table (columns with at least one null)
    null_records = [
        {"Column": col, "Null count": int(df[col].isna().sum()), "Null %": f"{df[col].isna().mean() * 100:.1f}%"}
        for col in df.columns
    ]
    has_nulls = pd.DataFrame(null_records)
    has_nulls = has_nulls[has_nulls["Null count"] > 0]
    if has_nulls.empty:
        null_section = dbc.Alert("No nulls detected in any column.", color="success")
    else:
        null_section = dbc.Table.from_dataframe(has_nulls, striped=True, bordered=True, size="sm")

    # Date gap detection
    if date_col:
        dates = pd.to_datetime(df["date"]).sort_values().drop_duplicates().reset_index(drop=True)
        diffs = dates.diff().dt.days.dropna()
        big_gaps = [(int(d), str(dates.iloc[i].date())) for i, d in zip(diffs.index, diffs) if d > 14]
        if big_gaps:
            gap_section = dbc.Alert(
                [html.P(f"Gap of {d} days ending {end}") for d, end in big_gaps[:10]],
                color="warning",
            )
        else:
            gap_section = dbc.Alert("No date gaps > 14 days detected.", color="success")
    else:
        gap_section = dbc.Alert("'date' column not found.", color="danger")

    sample = dbc.Table.from_dataframe(df.head(10), striped=True, bordered=True, size="sm", responsive=True)

    return html.Div(
        [
            html.H4("Data Quality Report", className="mb-3"),
            summary_row,
            html.H5("Null rates"),
            null_section,
            html.H5("Date gaps", className="mt-3"),
            gap_section,
            html.H5("10-row sample", className="mt-3"),
            sample,
        ]
    )


def _kpi_card(title: str, value: str, subtitle: str = "", color: str = "light") -> dbc.Col:
    body_children = [html.H6(title, className="text-muted small"), html.H4(value, className="mb-0")]
    if subtitle:
        body_children.append(html.P(subtitle, className="text-muted small mb-0"))
    return dbc.Col(
        dbc.Card(dbc.CardBody(body_children, className="py-2"), color=color, outline=True),
        width=3,
        className="mb-3",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Category Breakdown
# ══════════════════════════════════════════════════════════════════════════════


def _subcategory_panel() -> html.Div:
    monthly_agg = _expense_monthly_agg(load_parquet("interim", "monthly_aggregates"))
    tx = load_parquet("interim", "transactions_featured")
    if monthly_agg is None or tx is None:
        return _MISSING

    tx = actual_flows(tx)

    top15_cats = (
        monthly_agg.groupby("category")["total_amount"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
        .index.tolist()
    )

    amount_col = "amount_pln" if "amount_pln" in tx.columns else "amount"
    is_expense = tx["type"].astype(str).str.lower().isin(["withdrawal", "expense"])
    tx_exp = tx[is_expense & (tx["category"].isin(top15_cats))].copy()
    tx_exp["subcategory"] = tx_exp["subcategory"].fillna("(none)")
    subcat_totals = (
        tx_exp.groupby(["category", "subcategory"])[amount_col]
        .apply(lambda s: s.abs().sum())
        .reset_index()
        .rename(columns={amount_col: "spend"})
    )
    subcat_totals["cat_order"] = subcat_totals["category"].map(
        {c: i for i, c in enumerate(top15_cats)}
    )
    subcat_totals = subcat_totals.sort_values("cat_order")

    fig = go.Figure()
    for sc in subcat_totals["subcategory"].unique().tolist():
        row = subcat_totals[subcat_totals["subcategory"] == sc]
        spend_map = dict(zip(row["category"], row["spend"]))
        y_vals = [spend_map.get(c, 0) for c in top15_cats]
        fig.add_trace(go.Bar(name=sc, x=top15_cats, y=y_vals))

    fig.update_layout(
        barmode="stack",
        title="Top 15 Categories — Subcategory Breakdown",
        xaxis_title="Category",
        yaxis_title="PLN (abs)",
        xaxis_tickangle=-35,
        legend_title="Subcategory",
        height=420,
    )
    apply_standard_layout(fig, height=420)

    return html.Div(
        [
            html.H4("Category Breakdown", className="mb-3"),
            html.P(
                "Stacked subcategory spend for the top 15 categories by absolute spend.",
                className="text-muted",
            ),
            dcc.Graph(figure=fig),
        ]
    )


# ══════════════════════════════════════════════════════════════════════════════
# Tab 3 — Expense Trends
# ══════════════════════════════════════════════════════════════════════════════


def _build_trends_timeseries(trends: pd.DataFrame, category: str) -> go.Figure:
    """Shared figure builder: monthly spend bars + OLS trend overlay."""
    cat_df = trends[trends["category"] == category].sort_values("year_month")
    if cat_df.empty:
        return make_empty_figure("No trend data for the selected category.", title="Expense Trends")

    trend_class = cat_df["trend_class"].iloc[0]
    color_map = {
        "rising": "#e74c3c",
        "falling": "#3498db",
        "flat": "#95a5a6",
        "insufficient_data": "#95a5a6",
    }
    line_color = color_map.get(trend_class, "#95a5a6")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=cat_df["year_month"],
        y=cat_df["total_amount"],
        name="Monthly spend",
        marker_color="rgba(149,165,166,0.5)",
    ))

    slope = cat_df["trend_slope"].iloc[0]
    intercept = cat_df["trend_intercept"].iloc[0]
    if trend_class != "insufficient_data" and not pd.isna(slope):
        ordinals = cat_df["month_ordinal"].values
        trend_vals = intercept + slope * ordinals
        fig.add_trace(go.Scatter(
            x=cat_df["year_month"],
            y=trend_vals,
            name=f"Trend ({trend_class})",
            line=dict(color=line_color, width=2, dash="dash"),
        ))

    fig.update_layout(
        title=f"{category} — Monthly Spend with OLS Trend",
        xaxis_title="Month",
        yaxis_title="PLN",
    )
    apply_standard_layout(fig, height=420)
    return fig


def _trends_panel() -> html.Div:
    trends = load_parquet("processed", "expense_trends")
    if trends is None:
        return dbc.Alert("Run the pipeline first to generate expense_trends.parquet.", color="warning")

    cat_stats = trends.drop_duplicates("category").reset_index(drop=True)

    # Summary table
    display_cols = ["category", "trend_class", "trend_slope", "trend_r2",
                    "trend_pvalue", "trend_pvalue_adj", "trend_n_months", "share_pct", "is_seasonal"]
    # trend_pvalue_adj was added by Fix 2; fall back gracefully for old parquet files
    if "trend_pvalue_adj" not in cat_stats.columns:
        cat_stats["trend_pvalue_adj"] = float("nan")
    summary = cat_stats[display_cols].sort_values("share_pct", ascending=False).copy()
    summary["trend_slope"] = summary["trend_slope"].map(lambda v: f"{v:.2f}" if pd.notna(v) else "—")
    summary["trend_r2"] = summary["trend_r2"].map(lambda v: f"{v:.3f}" if pd.notna(v) else "—")
    summary["trend_pvalue"] = summary["trend_pvalue"].map(lambda v: f"{v:.3f}" if pd.notna(v) else "—")
    summary["trend_pvalue_adj"] = summary["trend_pvalue_adj"].map(lambda v: f"{v:.3f}" if pd.notna(v) else "—")
    summary["share_pct"] = summary["share_pct"].map(lambda v: f"{v:.1f}%")
    summary.columns = ["Category", "Trend", "Slope", "R²", "p-value", "q (BH)", "N months", "Share %", "Seasonal"]
    summary_table = dbc.Table.from_dataframe(summary, striped=True, bordered=True, size="sm", responsive=True)

    # Rising categories slope chart
    rising = cat_stats[cat_stats["trend_class"] == "rising"].sort_values("trend_slope")
    if not rising.empty:
        err_up = (rising["trend_ci_upper"] - rising["trend_slope"]).clip(lower=0)
        err_down = (rising["trend_slope"] - rising["trend_ci_lower"]).clip(lower=0)
        slope_fig = go.Figure(go.Bar(
            x=rising["trend_slope"],
            y=rising["category"],
            orientation="h",
            error_x=dict(type="data", symmetric=False,
                         array=list(err_up), arrayminus=list(err_down)),
            marker_color="#e74c3c",
        ))
        slope_fig.update_layout(
            title="Rising Categories — Monthly Slope ± 95% CI (autocorrelation-robust)",
            xaxis_title="Slope (PLN/month)",
            height=max(300, len(rising) * 35 + 100),
        )
        apply_standard_layout(slope_fig, height=max(300, len(rising) * 35 + 100))
    else:
        slope_fig = make_empty_figure(
            "No rising categories detected.",
            title="Rising Categories — Slope ± 95% CI (autocorrelation-robust)",
        )

    # Category time series with initial figure pre-computed
    categories = sorted(trends["category"].unique().tolist())
    initial_cat = categories[0] if categories else None
    initial_fig = (
        _build_trends_timeseries(trends, initial_cat)
        if initial_cat
        else make_empty_figure("No categories available.", title="Expense Trends")
    )

    return html.Div(
        [
            html.H4("Expense Trends", className="mb-3"),
            html.H5("Category Summary"),
            summary_table,
            html.H5("Rising Categories — Slope ± 95% CI", className="mt-4"),
            dcc.Graph(figure=slope_fig),
            html.H5("Category Time Series", className="mt-4"),
            dcc.Dropdown(
                id="trends-category-select",
                options=[{"label": c, "value": c} for c in categories],
                value=initial_cat,
                clearable=False,
                style={"width": "50%"},
            ),
            dcc.Graph(id="trends-timeseries-chart", figure=initial_fig),
        ]
    )


@callback(
    Output("trends-timeseries-chart", "figure"),
    Input("trends-category-select", "value"),
    prevent_initial_call=True,
)
def update_trends_timeseries(category: str) -> go.Figure:
    if not category:
        return make_empty_figure("Choose a category to display its trend.", title="Expense Trends")
    trends = load_parquet("processed", "expense_trends")
    if trends is None:
        return make_empty_figure("Expense trends data not found.", title="Expense Trends")
    return _build_trends_timeseries(trends, category)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 4 — Cash Flow Forecast
# ══════════════════════════════════════════════════════════════════════════════


def _cashflow_panel() -> html.Div:
    forecast = load_parquet("processed", "cashflow_forecast")
    hist = load_parquet("interim", "monthly_cashflow_split")

    if forecast is None or hist is None:
        return _MISSING

    method = forecast["method"].iloc[0] if "method" in forecast.columns else "Unknown"
    forecast_months = pd.to_datetime(forecast["month"]).dt.strftime("%Y-%m").tolist()
    months_rev = forecast_months[::-1]

    # Historical cashflow bars
    hist_di = hist["disposable_income"]
    hist_colors = ["#1a73e8" if v >= 0 else "#ea4335" for v in hist_di]
    hist_mean = float(hist_di.mean())

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=hist["year_month"],
        y=hist_di,
        marker_color=hist_colors,
        name="Historical",
        opacity=0.7,
    ))
    fig.add_hline(y=hist_mean, line_dash="dot", line_color="gray",
                  annotation_text=f"Historical mean {hist_mean:,.0f}",
                  annotation_position="top right")

    # 95% CI ribbon
    if forecast["ci95_lower"].notna().any():
        fig.add_trace(go.Scatter(
            x=forecast_months + months_rev,
            y=list(forecast["ci95_upper"]) + list(forecast["ci95_lower"].iloc[::-1]),
            fill="toself",
            fillcolor="rgba(100,149,237,0.12)",
            line=dict(width=0),
            name="95% CI",
            hoverinfo="skip",
        ))

    # 80% CI ribbon
    if forecast["ci80_lower"].notna().any():
        fig.add_trace(go.Scatter(
            x=forecast_months + months_rev,
            y=list(forecast["ci80_upper"]) + list(forecast["ci80_lower"].iloc[::-1]),
            fill="toself",
            fillcolor="rgba(100,149,237,0.28)",
            line=dict(width=0),
            name="80% CI",
            hoverinfo="skip",
        ))

    # Point estimate line
    fig.add_trace(go.Scatter(
        x=forecast_months,
        y=forecast["point_estimate"],
        line=dict(color="royalblue", width=2),
        mode="lines+markers",
        name=f"Forecast ({method})",
    ))

    fig.add_hline(y=0, line_dash="dash", line_color="red", opacity=0.4)
    fig.update_layout(
        title="Cash Flow Forecast",
        xaxis_title="Month",
        yaxis_title="PLN",
    )
    apply_standard_layout(fig, height=440)

    # Rolling-mean note if present
    note_el = html.Div()
    if "note" in forecast.columns:
        notes = forecast["note"].dropna()
        if not notes.empty:
            note_el = dbc.Alert(str(notes.iloc[0]), color="info", className="mt-2")

    return html.Div(
        [
            html.H4("Cash Flow Forecast", className="mb-3"),
            dbc.Badge(f"Method: {method}", color="secondary", className="mb-3"),
            dcc.Graph(figure=fig),
            note_el,
        ]
    )


# ══════════════════════════════════════════════════════════════════════════════
# Tab 4b — Savings Forecast
# ══════════════════════════════════════════════════════════════════════════════


def _savings_panel() -> html.Div:
    forecast = load_parquet("processed", "cashflow_forecast")
    hist = load_parquet("interim", "monthly_cashflow_split")

    if forecast is None or hist is None:
        return _MISSING

    method = forecast["method"].iloc[0] if "method" in forecast.columns else "Unknown"
    max_horizon = len(forecast)
    default_horizon = min(12, max_horizon)

    note_el = html.Div()
    if "note" in forecast.columns:
        notes = forecast["note"].dropna()
        if not notes.empty:
            note_el = dbc.Alert(str(notes.iloc[0]), color="info", className="mt-2")

    liquid_balance, liquid_month = _latest_liquid_balance()
    balance_caption = (
        html.P(
            f"Prefilled from account balances at {liquid_month}",
            className="text-muted small",
        )
        if liquid_month
        else html.Div()
    )

    return html.Div(
        [
            html.H4("Savings Forecast", className="mb-3"),
            dbc.Badge(f"Method: {method}", color="secondary", className="mb-3"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Current balance (PLN)"),
                            dbc.Input(
                                id="savings-balance", type="number", value=liquid_balance,
                                placeholder="e.g. 50000", debounce=True, className="mb-1",
                            ),
                            balance_caption,
                            html.Label("Savings target (PLN)", className="mt-2"),
                            dbc.Input(
                                id="savings-target", type="number", value=None,
                                placeholder="e.g. 50000", debounce=True, className="mb-3",
                            ),
                            html.Label("Horizon (months)"),
                            dcc.Slider(
                                id="savings-horizon",
                                min=1, max=max_horizon, step=1,
                                value=default_horizon,
                                marks={1: "1", max_horizon: str(max_horizon),
                                       default_horizon: str(default_horizon)},
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            dcc.Loading(dcc.Graph(id="savings-chart")),
                            html.Div(id="savings-summary", className="mt-3"),
                        ],
                        width=9,
                    ),
                ]
            ),
            note_el,
        ]
    )


@callback(
    Output("savings-chart", "figure"),
    Output("savings-summary", "children"),
    Input("savings-balance", "value"),
    Input("savings-target", "value"),
    Input("savings-horizon", "value"),
)
def update_savings_forecast(balance, target, horizon):
    forecast = load_parquet("processed", "cashflow_forecast")
    hist = load_parquet("interim", "monthly_cashflow_split")

    if forecast is None or hist is None:
        return (
            make_empty_figure("Pipeline data not found.", title="Savings Forecast"),
            dbc.Alert("Pipeline data not found.", color="warning"),
        )

    balance = float(balance) if balance else 0.0
    target = float(target) if target else None
    horizon = int(horizon or 12)
    horizon = min(horizon, len(forecast))

    hist_di = hist["disposable_income"].values
    prefix_sum = np.cumsum(hist_di)
    if balance > 0:
        offset = balance - float(prefix_sum[-1])
    else:
        offset = 0.0
    hist_cumulative = (offset + prefix_sum).tolist()
    hist_months = hist["year_month"].tolist()

    fig = plot_savings_fan(forecast, hist_months, hist_cumulative, balance, target, horizon)

    cum_balance = hist_cumulative[-1] + forecast["point_estimate"].iloc[:horizon].cumsum().reset_index(drop=True)
    final_balance = cum_balance.iloc[-1]

    summary_lines = [
        html.H6("Summary"),
        html.P(f"Projected balance after {horizon} months: {final_balance:,.0f} PLN"),
    ]

    if target is not None and target > 0:
        idx = np.argmax(cum_balance.values >= target) if (cum_balance.values >= target).any() else None
        if idx is not None and idx < len(cum_balance):
            forecast_months = pd.to_datetime(forecast["month"]).dt.strftime("%Y-%m").tolist()
            reached_month = forecast_months[idx]
            summary_lines.append(
                dbc.Alert(
                    f"Target of {target:,.0f} PLN reached in {int(idx) + 1} month(s) ({reached_month}).",
                    color="success",
                )
            )
        else:
            summary_lines.append(
                dbc.Alert(
                    f"Target of {target:,.0f} PLN not reachable within {horizon} months.",
                    color="warning",
                )
            )

    return fig, html.Div(summary_lines)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 5 — Monte Carlo
# ══════════════════════════════════════════════════════════════════════════════


def _mc_panel() -> html.Div:
    liquid_balance, liquid_month = _latest_liquid_balance()
    balance_caption = (
        html.P(
            f"Prefilled from account balances at {liquid_month}",
            className="text-muted small",
        )
        if liquid_month
        else html.Div()
    )
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Simulations"),
                            dcc.Slider(
                                id="mc-n-sims",
                                min=500, max=5000, step=500, value=2000,
                                marks={500: "500", 2000: "2 000", 5000: "5 000"},
                            ),
                            html.Label("Horizon (months)", className="mt-3"),
                            dcc.Slider(id="mc-horizon", min=3, max=24, step=1, value=12),
                            html.Label("Current balance (PLN)", className="mt-3"),
                            dbc.Input(
                                id="mc-balance",
                                type="number",
                                value=liquid_balance if liquid_balance is not None else 0,
                                debounce=True,
                            ),
                            balance_caption,
                            dbc.Checklist(
                                options=[{"label": "Detrend cashflow", "value": "detrend"}],
                                value=["detrend"],
                                id="mc-detrend",
                                className="mt-3",
                            ),
                            dbc.Button("Run simulation", id="mc-run-btn", color="primary",
                                       className="mt-3"),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            dcc.Loading(dcc.Graph(id="mc-fan-chart")),
                            html.Div(id="mc-prob-table", className="mt-3"),
                        ],
                        width=9,
                    ),
                ]
            ),
        ]
    )


@callback(
    Output("mc-fan-chart", "figure"),
    Output("mc-prob-table", "children"),
    Input("mc-run-btn", "n_clicks"),
    State("mc-n-sims", "value"),
    State("mc-horizon", "value"),
    State("mc-balance", "value"),
    State("mc-detrend", "value"),
    prevent_initial_call=True,
)
def run_mc(n_clicks, n_sims, horizon, balance, detrend_val):
    cashflow_split = load_parquet("interim", "monthly_cashflow_split")
    if cashflow_split is None:
        return (
            make_empty_figure("Pipeline data not found.", title="Monte Carlo Forecast"),
            dbc.Alert("Pipeline data not found.", color="warning"),
        )

    net_cashflow = complete_cashflow_series(cashflow_split)

    result = run_expenditure_mc(
        monthly_net_cashflow=net_cashflow,
        current_balance=float(balance or 0),
        n_simulations=int(n_sims or 2000),
        horizon_months=int(horizon or 12),
        detrend=bool("detrend" in (detrend_val or [])),
    )

    fig = plot_mc_fan(result)

    prob_df = result["prob_negative"].reset_index()
    prob_df.columns = ["Month", "P(balance < 0)"]
    prob_df["Month"] = prob_df["Month"].dt.strftime("%Y-%m")
    prob_df["P(balance < 0)"] = prob_df["P(balance < 0)"].map("{:.1%}".format)
    table = dbc.Table.from_dataframe(prob_df, striped=True, bordered=True, size="sm")

    return fig, table


# ══════════════════════════════════════════════════════════════════════════════
# Tab 6 — What-If Scenarios
# ══════════════════════════════════════════════════════════════════════════════


def _whatif_categories(monthly_agg: pd.DataFrame) -> list[str]:
    """
    Categories that the Cut Costs sliders may touch.

    Only categories mapped to the ``fixed`` or ``guilt_free`` buckets are
    included, so Taxes, Income, one-offs and unmapped rows cannot be cut. If
    no bucket mapping exists, fall back to everything except the income and
    take-home-deduction categories.
    """
    categories = sorted(monthly_agg["category"].dropna().unique().tolist())
    cfg = _budget_health_config()
    buckets = cfg.get("buckets") or {}
    if buckets:
        frame = pd.DataFrame({"category": categories, "subcategory": None})
        mapped = assign_buckets(frame, buckets)
        return [
            category
            for category, bucket in zip(categories, mapped)
            if bucket in ("fixed", "guilt_free")
        ]
    excluded = set(cfg.get("income_categories") or ["Income"])
    excluded |= set(cfg.get("take_home_deductions") or ["Taxes"])
    return [category for category in categories if category not in excluded]


def _whatif_panel() -> html.Div:
    monthly_agg = _expense_monthly_agg(load_parquet("interim", "monthly_aggregates"))
    if monthly_agg is None or monthly_agg.empty:
        return dbc.Alert("Run the pipeline first to generate monthly_aggregates.parquet.", color="warning")

    categories = _whatif_categories(monthly_agg)

    sliders = [
        html.Div(
            [
                html.Label(cat, className="fw-semibold"),
                dcc.Slider(
                    id={"type": "reduction-slider", "index": cat},
                    min=0, max=100, step=5, value=0,
                    marks={0: "0%", 50: "50%", 100: "100%"},
                    tooltip={"placement": "bottom"},
                ),
            ],
            className="mb-3",
        )
        for cat in categories
    ]

    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H5("Set reduction % per category"),
                            html.Hr(),
                            *sliders,
                            html.Label("Horizon (months)", className="mt-2"),
                            dcc.Slider(id="whatif-horizon", min=3, max=36, step=1, value=12),
                            html.Label("Savings target (PLN)", className="mt-3"),
                            dbc.Input(id="whatif-target", type="number",
                                      placeholder="e.g. 5000", debounce=True),
                            dbc.Button("Calculate", id="whatif-run-btn", color="success",
                                       className="mt-3"),
                        ],
                        width=3,
                        style={"maxHeight": "80vh", "overflowY": "auto"},
                    ),
                    dbc.Col(
                        [
                            dcc.Loading(dcc.Graph(id="whatif-chart")),
                            html.Div(id="whatif-summary", className="mt-3"),
                        ],
                        width=9,
                    ),
                ]
            ),
        ]
    )


@callback(
    Output("whatif-chart", "figure"),
    Output("whatif-summary", "children"),
    Input("whatif-run-btn", "n_clicks"),
    State({"type": "reduction-slider", "index": dash.ALL}, "value"),
    State({"type": "reduction-slider", "index": dash.ALL}, "id"),
    State("whatif-horizon", "value"),
    State("whatif-target", "value"),
    prevent_initial_call=True,
)
def run_whatif(n_clicks, slider_values, slider_ids, horizon, target):
    reductions = {
        sid["index"]: val / 100.0
        for sid, val in zip(slider_ids, slider_values)
        if (val or 0) > 0
    }
    if not reductions:
        return (
            make_empty_figure("Set at least one reduction greater than 0%.", title="What-If Scenario"),
            dbc.Alert("Set at least one reduction > 0%.", color="info"),
        )

    monthly_agg = load_parquet("interim", "monthly_aggregates")
    if monthly_agg is None:
        return (
            make_empty_figure("Pipeline data not found.", title="What-If Scenario"),
            dbc.Alert("Pipeline data not found.", color="warning"),
        )

    cat_spend_df = _expense_monthly_agg(monthly_agg)
    if cat_spend_df is not None and "is_partial_month" in cat_spend_df.columns:
        cat_spend_df = cat_spend_df[
            ~cat_spend_df["is_partial_month"].fillna(False).astype(bool)
        ]
    cat_spend = cat_spend_df.pivot_table(
        index="year_month", columns="category", values="total_amount", aggfunc="sum"
    ).fillna(0)

    category_type_map = _CATEGORY_TYPE_MAP

    try:
        result = compute_expense_reduction_scenario(
            monthly_category_spend=cat_spend,
            category_type_map=category_type_map,
            reductions=reductions,
            horizon_months=int(horizon or 12),
        )
    except ValueError as exc:
        return (
            make_empty_figure("Scenario inputs are invalid.", title="What-If Scenario"),
            dbc.Alert(str(exc), color="danger"),
        )

    fig = plot_reduction_scenario(result)

    monthly_saving = result.attrs["total_monthly_saving"]
    cumulative_total = result["delta_cumulative"].iloc[-1]

    summary_lines = [
        html.H6("Scenario Summary"),
        html.P(
            f"Monthly saving: {monthly_saving:,.0f} PLN  |  "
            f"Cumulative over {horizon} months: {cumulative_total:,.0f} PLN"
        ),
    ]

    if target:
        tgt = months_to_savings_target(result, float(target))
        if "error" not in tgt:
            summary_lines.append(html.P(tgt["note"]))
        else:
            summary_lines.append(dbc.Alert(tgt["error"], color="warning"))

    for warning in result.attrs.get("warnings", []):
        summary_lines.append(dbc.Alert(warning, color="warning"))

    return fig, html.Div(summary_lines)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 7 — Spending Drill-Down
# ══════════════════════════════════════════════════════════════════════════════


def _drilldown_panel() -> html.Div:
    featured = load_parquet("interim", "transactions_featured")
    if featured is None:
        return _MISSING

    featured = actual_flows(featured)
    categories = sorted(featured["category"].dropna().unique().tolist())
    all_months = sorted(featured["year_month"].dropna().unique().tolist())
    n = len(all_months)

    # Build slider marks: label every ~6 months so the axis stays readable
    step = max(1, n // 8)
    marks = {i: all_months[i] for i in range(0, n, step)}
    if (n - 1) not in marks:
        marks[n - 1] = all_months[-1]

    return html.Div(
        [
            html.H4("Spending Drill-Down", className="mb-3"),
            html.P(
                "Select a category and a time window to explore monthly spending, "
                "sub-category breakdowns, and descriptive statistics.",
                className="text-muted",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Dropdown(
                            id="drilldown-category",
                            options=[{"label": c, "value": c} for c in categories],
                            placeholder="Select a category…",
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        dcc.Dropdown(
                            id="drilldown-aggregate",
                            options=[
                                {"label": "Monthly", "value": "monthly"},
                                {"label": "3-Month Rolling Mean", "value": "rolling_3m"},
                                {"label": "6-Month Rolling Mean", "value": "rolling_6m"},
                            ],
                            value="monthly",
                            clearable=False,
                        ),
                        width=4,
                    ),
                ],
                className="mb-3",
            ),
            html.Label("Time window", className="fw-semibold"),
            dcc.RangeSlider(
                id="drilldown-time-slider",
                min=0,
                max=max(n - 1, 0),
                step=1,
                value=[0, max(n - 1, 0)],
                marks=marks,
                tooltip={"always_visible": False, "placement": "bottom"},
                allowCross=False,
            ),
            dcc.Loading(dcc.Graph(id="drilldown-chart"), className="mt-3"),
            html.Div(id="drilldown-stats", className="mt-3"),
        ]
    )


# ── Helpers for drill-down stats ──────────────────────────────────────────────

def _monthly_stats(series: pd.Series, label: str) -> dict:
    """Return descriptive stats for a monthly-spend Series (only active months)."""
    active = series[series > 0]
    n_active = len(active)
    n_total = len(series)
    mean_all = series.mean()
    mean_active = active.mean() if n_active else float("nan")
    median_all = series.median()
    std = series.std(ddof=1) if len(series) > 1 else float("nan")
    cov = (std / mean_all * 100) if mean_all and not np.isnan(std) else float("nan")
    return {
        "Name": label,
        "Total (PLN)": f"{series.sum():,.0f}",
        "Mean/month (PLN)": f"{mean_all:,.0f}",
        "Mean active months (PLN)": f"{mean_active:,.0f}" if not np.isnan(mean_active) else "—",
        "Median/month (PLN)": f"{median_all:,.0f}",
        "Std dev (PLN)": f"{std:,.0f}" if not np.isnan(std) else "—",
        "CoV (%)": f"{cov:.1f}" if not np.isnan(cov) else "—",
        "Min (PLN)": f"{series.min():,.0f}",
        "Max (PLN)": f"{series.max():,.0f}",
        "Active months": f"{n_active}/{n_total}",
    }


def _build_stats_table(rows: list[dict]) -> dbc.Table:
    if not rows:
        return dbc.Alert("No data for statistics.", color="info")
    df = pd.DataFrame(rows)
    return dbc.Table.from_dataframe(df, striped=True, bordered=True, size="sm", responsive=True)


def _apply_aggregation(series: pd.Series, aggregate_mode: str) -> pd.Series:
    """Apply a display aggregation mode to a monthly series."""
    if aggregate_mode == "monthly":
        return series.astype(float)

    window_by_mode = {
        "rolling_3m": 3,
        "rolling_6m": 6,
    }
    window = window_by_mode.get(aggregate_mode)
    if window is None:
        raise ValueError(f"Unknown aggregate mode: {aggregate_mode}")

    return series.astype(float).rolling(window=window, min_periods=1).mean()


def _aggregate_mode_label(aggregate_mode: str) -> str:
    labels = {
        "monthly": "Monthly",
        "rolling_3m": "3-Month Rolling Mean",
        "rolling_6m": "6-Month Rolling Mean",
    }
    return labels.get(aggregate_mode, "Monthly")


@callback(
    Output("drilldown-chart", "figure"),
    Output("drilldown-stats", "children"),
    Input("drilldown-category", "value"),
    Input("drilldown-time-slider", "value"),
    Input("drilldown-aggregate", "value"),
    prevent_initial_call=True,
)
def update_drilldown(
    category: str,
    slider_range: list[int],
    aggregate_mode: str,
) -> tuple[go.Figure, html.Div]:
    empty = (
        make_empty_figure(
            "Select a category and time window to view spending details.",
            title="Spending Drill-Down",
        ),
        html.Div(),
    )

    if not category or not slider_range:
        return empty

    aggregate_mode = aggregate_mode or "monthly"

    featured = load_parquet("interim", "transactions_featured")
    if featured is None:
        return empty

    featured = actual_flows(featured)

    # Map slider indices back to month strings
    all_months_global = sorted(featured["year_month"].dropna().unique().tolist())
    lo_idx, hi_idx = int(slider_range[0]), int(slider_range[1])
    lo_month = all_months_global[lo_idx] if lo_idx < len(all_months_global) else all_months_global[0]
    hi_month = all_months_global[hi_idx] if hi_idx < len(all_months_global) else all_months_global[-1]

    amount_col = "amount_pln" if "amount_pln" in featured.columns else "amount"
    is_expense = featured["type"].astype(str).str.lower().isin(["withdrawal", "expense"])
    df = featured[
        (featured["category"] == category)
        & is_expense
        & (featured["year_month"] >= lo_month)
        & (featured["year_month"] <= hi_month)
    ].copy()
    df["_amount"] = df[amount_col].abs()

    # Window months for consistent x-axis and zero-fill
    window_months = [m for m in all_months_global if lo_month <= m <= hi_month]

    if df.empty or not window_months:
        fig = make_empty_figure(
            f"No withdrawal transactions for '{category}' in the selected window.",
            title="Spending Drill-Down",
        )
        return fig, dbc.Alert(
            f"No withdrawal transactions for '{category}' in the selected time window.",
            color="info",
        )

    fig = go.Figure()
    aggregate_label = _aggregate_mode_label(aggregate_mode)

    # ── Category total ────────────────────────────────────────────────────────
    cat_monthly_series = (
        df.groupby("year_month")["_amount"].sum()
        .reindex(window_months, fill_value=0)
    )
    try:
        cat_series = _apply_aggregation(cat_monthly_series, aggregate_mode)
    except ValueError as exc:
        fig = make_empty_figure(str(exc), title="Spending Drill-Down")
        return fig, dbc.Alert(str(exc), color="warning")

    cat_mean = cat_series.mean()
    cat_median = cat_series.median()

    fig.add_trace(go.Scatter(
        x=window_months,
        y=cat_series.values,
        mode="lines+markers",
        name=f"{category} - total",
        line=dict(width=3),
        legendgroup="total",
        legendgrouptitle_text="Category Total",
    ))
    fig.add_hline(
        y=float(cat_mean), line_dash="dot", line_color="gray",
        annotation_text=f"Mean {cat_mean:,.0f}",
        annotation_position="top right",
    )
    fig.add_hline(
        y=float(cat_median), line_dash="dash", line_color="silver",
        annotation_text=f"Median {cat_median:,.0f}",
        annotation_position="bottom right",
    )

    stats_rows = [_monthly_stats(cat_series, f"{category} (total)")]

    # ── Per subcategory ───────────────────────────────────────────────────────
    if "subcategory" in df.columns:
        for sub in sorted(df["subcategory"].dropna().unique()):
            sub_series = (
                df[df["subcategory"] == sub]
                .groupby("year_month")["_amount"].sum()
                .reindex(window_months, fill_value=0)
            )
            sub_series = _apply_aggregation(sub_series, aggregate_mode)
            fig.add_trace(go.Scatter(
                x=window_months,
                y=sub_series.values,
                mode="lines+markers",
                name=sub,
                line=dict(width=1.5),
                legendgroup="subcategory",
                legendgrouptitle_text="Subcategory",
            ))
            stats_rows.append(_monthly_stats(sub_series, f"  ↳ {sub}"))

    # ── Per subcategory_detail ────────────────────────────────────────────────
    if "subcategory_detail" in df.columns:
        for det in sorted(df["subcategory_detail"].dropna().unique()):
            det_series = (
                df[df["subcategory_detail"] == det]
                .groupby("year_month")["_amount"].sum()
                .reindex(window_months, fill_value=0)
            )
            det_series = _apply_aggregation(det_series, aggregate_mode)
            fig.add_trace(go.Scatter(
                x=window_months,
                y=det_series.values,
                mode="lines+markers",
                name=det,
                line=dict(width=1.5, dash="dash"),
                legendgroup="detail",
                legendgrouptitle_text="Sub-subcategory",
            ))
            stats_rows.append(_monthly_stats(det_series, f"    ↳↳ {det}"))

    fig.update_layout(
        title=f"{category} - {aggregate_label} spending breakdown [{lo_month} -> {hi_month}]",
        xaxis_title="Month",
        yaxis_title="PLN (withdrawals)",
        legend=dict(groupclick="toggleitem"),
    )
    apply_standard_layout(fig, height=500)

    stats_section = html.Div(
        [
            html.H5(
                f"Descriptive Statistics ({aggregate_label}) for Selected Window",
                className="mt-4",
            ),
            html.P(
                "Mean/month and Median/month are computed over ALL months in the window "
                "(zero-filled when no spend) after applying the selected aggregation. "
                "Mean active months excludes zero-spend months. "
                "CoV (%) = std dev ÷ mean × 100 — higher means more variable spending.",
                className="text-muted small",
            ),
            _build_stats_table(stats_rows),
        ]
    )

    return fig, stats_section


# ══════════════════════════════════════════════════════════════════════════════
# Tab 8 — Category Trends
# ══════════════════════════════════════════════════════════════════════════════

_ALL_SUBCATS = "__ALL__"
_FLOW_TYPE_OPTIONS = [
    {"label": "Expense", "value": "expense"},
    {"label": "Income", "value": "income"},
]


def _subcategory_options(summary_df, category, flow_type) -> list[dict]:
    """Build the subcategory dropdown options for a category/flow_type."""
    options = [{"label": "(all subcategories)", "value": _ALL_SUBCATS}]
    if summary_df is not None and category:
        subcats = summary_df[
            (summary_df["flow_type"] == flow_type)
            & (summary_df["level"] == "subcategory")
            & (summary_df["category"] == category)
        ]["subcategory"].dropna().unique().tolist()
        options += [{"label": s, "value": s} for s in sorted(subcats)]
    return options


def _select_category_series(series_df, category, subcategory, flow_type):
    """Return (hist, trend) monthly Series for a selection, or None."""
    level = "subcategory" if subcategory not in (None, _ALL_SUBCATS) else "category"
    mask = (
        (series_df["flow_type"] == flow_type)
        & (series_df["category"] == category)
        & (series_df["level"] == level)
    )
    if level == "subcategory":
        mask &= series_df["subcategory"] == subcategory
    else:
        mask &= series_df["subcategory"].isna()

    sel = series_df[mask].sort_values("month")
    if sel.empty:
        return None

    idx = pd.to_datetime(sel["month"])
    hist = pd.Series(sel["y"].values, index=idx)
    trend = pd.Series(sel["trend"].values, index=idx)
    return hist, trend


def _build_cat_trend_figure(series_df, category, subcategory, flow_type) -> go.Figure:
    if not category:
        return make_empty_figure("Select a category to display its trend.", title="Category Trends")

    selection = _select_category_series(series_df, category, subcategory, flow_type)
    if selection is None:
        return make_empty_figure("No trend data for the selected category.", title="Category Trends")

    hist, trend = selection
    sub_label = " (all subcategories)" if subcategory in (None, _ALL_SUBCATS) else f" — {subcategory}"
    return plot_category_trend(hist, trend, title=f"{category}{sub_label} — {flow_type.title()} Trend")


def _build_cat_trends_summary(summary_df, flow_type, include_subcats):
    if summary_df is None:
        return dbc.Alert("Run the pipeline first to generate category trend data.", color="warning")

    df = summary_df[summary_df["flow_type"] == flow_type].copy()
    if not include_subcats:
        df = df[df["level"] == "category"]
    if df.empty:
        return dbc.Alert("No trend data for this selection.", color="info")

    display = df[
        [
            "level",
            "category",
            "subcategory",
            "n_months",
            "mean_monthly",
            "trend_change_pln",
            "avg_monthly_change_pln",
            "growth_pct",
            "direction",
        ]
    ].copy()
    if "direction_pvalue_adj" in df.columns:
        display["direction_pvalue_adj"] = df["direction_pvalue_adj"].to_numpy()
    else:
        display["direction_pvalue_adj"] = float("nan")
    display["subcategory"] = display["subcategory"].fillna("—")
    display["mean_monthly"] = display["mean_monthly"].map(
        lambda v: f"{v:,.0f}" if pd.notna(v) else "—"
    )
    display["trend_change_pln"] = display["trend_change_pln"].map(
        lambda v: f"{v:+,.0f}" if pd.notna(v) else "—"
    )
    display["avg_monthly_change_pln"] = display["avg_monthly_change_pln"].map(
        lambda v: f"{v:+,.0f}" if pd.notna(v) else "—"
    )
    display["growth_pct"] = display["growth_pct"].map(
        lambda v: f"{v:+.1f}%" if pd.notna(v) else "—"
    )
    display["direction_pvalue_adj"] = display["direction_pvalue_adj"].map(
        lambda v: f"{v:.3f}" if pd.notna(v) else "—"
    )
    display.columns = [
        "Level",
        "Category",
        "Subcategory",
        "N months",
        "Mean/month (PLN)",
        "Trend change (PLN)",
        "Avg monthly change (PLN)",
        "Growth %",
        "Direction",
        "q (BH)",
    ]
    return dbc.Table.from_dataframe(display, striped=True, bordered=True, size="sm", responsive=True)


def _cat_trends_panel() -> html.Div:
    summary = load_parquet("processed", "category_trends_summary")
    series = load_parquet("processed", "category_trends_series")
    if summary is None or series is None:
        return dbc.Alert(
            "Run the pipeline first to generate category trend data "
            "(requires the forecast extra: poetry install --with forecast).",
            color="warning",
        )

    categories = sorted(summary["category"].dropna().unique().tolist())
    initial_cat = categories[0] if categories else None

    initial_fig = _build_cat_trend_figure(series, initial_cat, _ALL_SUBCATS, "expense")
    initial_summary = _build_cat_trends_summary(summary, "expense", False)

    return html.Div(
        [
            html.H4("Category Trends", className="mb-3"),
            dbc.RadioItems(
                id="cat-trends-type",
                options=_FLOW_TYPE_OPTIONS,
                value="expense",
                inline=True,
                className="mb-3",
            ),
            dbc.Checkbox(
                id="cat-trends-include-subcats",
                label="Include subcategory rows in summary",
                value=False,
                className="mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Dropdown(
                            id="cat-trends-category",
                            options=[{"label": c, "value": c} for c in categories],
                            value=initial_cat,
                            clearable=False,
                        ),
                        width=6,
                    ),
                    dbc.Col(
                        dcc.Dropdown(
                            id="cat-trends-subcategory",
                            options=_subcategory_options(summary, initial_cat, "expense"),
                            value=_ALL_SUBCATS,
                            clearable=False,
                        ),
                        width=6,
                    ),
                ],
                className="mb-3",
            ),
            dcc.Graph(id="cat-trends-chart", figure=initial_fig),
            html.H5("Summary", className="mt-4"),
            html.Div(id="cat-trends-summary", children=initial_summary),
        ]
    )


@callback(
    Output("cat-trends-subcategory", "options"),
    Output("cat-trends-subcategory", "value"),
    Input("cat-trends-category", "value"),
    Input("cat-trends-type", "value"),
    prevent_initial_call=True,
)
def update_cat_trends_subcats(category, flow_type):
    summary = load_parquet("processed", "category_trends_summary")
    return _subcategory_options(summary, category, flow_type), _ALL_SUBCATS


@callback(
    Output("cat-trends-chart", "figure"),
    Input("cat-trends-category", "value"),
    Input("cat-trends-subcategory", "value"),
    Input("cat-trends-type", "value"),
    prevent_initial_call=True,
)
def update_cat_trends_chart(category, subcategory, flow_type):
    series = load_parquet("processed", "category_trends_series")
    if series is None:
        return make_empty_figure("Category trend data not found.", title="Category Trends")
    return _build_cat_trend_figure(series, category, subcategory, flow_type)


@callback(
    Output("cat-trends-summary", "children"),
    Input("cat-trends-type", "value"),
    Input("cat-trends-include-subcats", "value"),
    prevent_initial_call=True,
)
def update_cat_trends_summary(flow_type, include_subcats):
    summary = load_parquet("processed", "category_trends_summary")
    return _build_cat_trends_summary(summary, flow_type, bool(include_subcats))


# ══════════════════════════════════════════════════════════════════════════════
# Tab 9 — Category Forecasts
# ══════════════════════════════════════════════════════════════════════════════


def _build_cat_forecast_figures(series_df, forecast_df, category, subcategory, flow_type):
    if not category:
        empty = make_empty_figure(
            "Select a category to view its forecast.", title="Category Forecasts"
        )
        return empty, empty, empty

    selection = _select_category_series(series_df, category, subcategory, flow_type)
    sub_label = " (all subcategories)" if subcategory in (None, _ALL_SUBCATS) else f" — {subcategory}"
    base_title = f"{category}{sub_label} — {flow_type.title()} Forecast"

    figures = []
    for horizon in (12, 24, 36):
        fc = None
        if forecast_df is not None:
            level = "subcategory" if subcategory not in (None, _ALL_SUBCATS) else "category"
            mask = (
                (forecast_df["flow_type"] == flow_type)
                & (forecast_df["category"] == category)
                & (forecast_df["level"] == level)
                & (forecast_df["months_ahead"] <= horizon)
            )
            if level == "subcategory":
                mask &= forecast_df["subcategory"] == subcategory
            else:
                mask &= forecast_df["subcategory"].isna()
            fc = forecast_df[mask]

        if selection is None or fc is None or fc.empty:
            figures.append(
                make_empty_figure(
                    "Insufficient history for a category forecast.",
                    title=f"{base_title} ({horizon} months)",
                )
            )
        else:
            hist, _ = selection
            figures.append(
                plot_category_forecast(hist, fc, title=base_title, horizon_months=horizon)
            )

    return tuple(figures)


def _cat_forecast_panel() -> html.Div:
    forecast = load_parquet("processed", "category_forecasts")
    series = load_parquet("processed", "category_trends_series")
    if forecast is None or series is None:
        return dbc.Alert(
            "Run the pipeline first to generate category forecast data "
            "(requires the forecast extra: poetry install --with forecast).",
            color="warning",
        )

    categories = sorted(forecast["category"].dropna().unique().tolist())
    initial_cat = categories[0] if categories else None

    fig1, fig2, fig3 = _build_cat_forecast_figures(
        series, forecast, initial_cat, _ALL_SUBCATS, "expense"
    )

    return html.Div(
        [
            html.H4("Category Forecasts", className="mb-3"),
            dbc.RadioItems(
                id="cat-forecast-type",
                options=_FLOW_TYPE_OPTIONS,
                value="expense",
                inline=True,
                className="mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Dropdown(
                            id="cat-forecast-category",
                            options=[{"label": c, "value": c} for c in categories],
                            value=initial_cat,
                            clearable=False,
                        ),
                        width=6,
                    ),
                    dbc.Col(
                        dcc.Dropdown(
                            id="cat-forecast-subcategory",
                            options=_subcategory_options(forecast, initial_cat, "expense"),
                            value=_ALL_SUBCATS,
                            clearable=False,
                        ),
                        width=6,
                    ),
                ],
                className="mb-3",
            ),
            dcc.Graph(id="cat-forecast-1y", figure=fig1),
            dcc.Graph(id="cat-forecast-2y", figure=fig2),
            dcc.Graph(id="cat-forecast-3y", figure=fig3),
        ]
    )


@callback(
    Output("cat-forecast-subcategory", "options"),
    Output("cat-forecast-subcategory", "value"),
    Input("cat-forecast-category", "value"),
    Input("cat-forecast-type", "value"),
    prevent_initial_call=True,
)
def update_cat_forecast_subcats(category, flow_type):
    forecast = load_parquet("processed", "category_forecasts")
    return _subcategory_options(forecast, category, flow_type), _ALL_SUBCATS


@callback(
    Output("cat-forecast-1y", "figure"),
    Output("cat-forecast-2y", "figure"),
    Output("cat-forecast-3y", "figure"),
    Input("cat-forecast-category", "value"),
    Input("cat-forecast-subcategory", "value"),
    Input("cat-forecast-type", "value"),
    prevent_initial_call=True,
)
def update_cat_forecast_charts(category, subcategory, flow_type):
    forecast = load_parquet("processed", "category_forecasts")
    series = load_parquet("processed", "category_trends_series")
    if forecast is None or series is None:
        empty = make_empty_figure("Category forecast data not found.", title="Category Forecasts")
        return empty, empty, empty
    return _build_cat_forecast_figures(series, forecast, category, subcategory, flow_type)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 10 — Recurring Costs
# ══════════════════════════════════════════════════════════════════════════════


def _fmt_pln(value) -> str:
    """Format a PLN amount for display; dashes for missing values."""
    return f"{value:,.0f}" if pd.notna(value) else "—"


def _fmt_date(value) -> str:
    """Format a Timestamp/date for display; dashes for missing values."""
    if pd.isna(value):
        return "—"
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _fmt_text(value) -> str:
    """Format a free-text cell; dashes for missing/blank values."""
    if pd.isna(value) or not str(value).strip():
        return "—"
    return str(value)


def _recurring_status_badge(row: pd.Series) -> dbc.Badge:
    """Colour-coded factual status for one register row."""
    if bool(row.get("is_lapsed", False)):
        return dbc.Badge("Lapsed", color="warning")
    if bool(row.get("is_active", False)):
        return dbc.Badge("Active", color="success")
    return dbc.Badge("Inactive", color="secondary")


def _recurring_register_table(register: pd.DataFrame) -> html.Table:
    """Render the commitment register as a badge-coloured table."""
    headers = [
        "Payee",
        "Category",
        "Cadence",
        "Current amount (PLN)",
        "Monthly run-rate (PLN)",
        "Last seen",
        "Next expected",
        "Price changes",
        "Status",
    ]
    body_rows = []
    for _, row in register.iterrows():
        body_rows.append(
            html.Tr(
                [
                    html.Td(str(row["payee"])),
                    html.Td(_fmt_text(row.get("category"))),
                    html.Td(_fmt_text(row.get("cadence_class"))),
                    html.Td(_fmt_pln(row.get("amount_current"))),
                    html.Td(_fmt_pln(row.get("monthly_run_rate"))),
                    html.Td(_fmt_date(row.get("last_seen"))),
                    html.Td(_fmt_date(row.get("next_expected_date"))),
                    html.Td(
                        str(int(row["n_price_changes"]))
                        if pd.notna(row.get("n_price_changes"))
                        else "—"
                    ),
                    html.Td(_recurring_status_badge(row)),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(html.Tr([html.Th(header) for header in headers])),
            html.Tbody(body_rows),
        ],
        id="recurring-register-table",
        className="table table-sm table-striped table-bordered",
    )


def _recurring_renewal_table(register: pd.DataFrame, as_of: pd.Timestamp) -> html.Div:
    """Renewal calendar: known-cadence commitments due within the next 60 days.

    Deliberately not filtered on ``is_active``: semiannual and annual renewals
    are due precisely when the last payment is more than 90 days old.
    """
    expected = register[
        (~register["is_lapsed"].fillna(False))
        & (register["cadence_class"].fillna("irregular") != "irregular")
        & register["next_expected_date"].notna()
        & (
            pd.to_datetime(register["next_expected_date"])
            <= as_of + pd.Timedelta(days=60)
        )
    ].copy()

    if expected.empty:
        return dbc.Alert(
            "No renewals expected in the next 60 days.", color="info", id="recurring-renewal-table"
        )

    expected["next_expected_date"] = pd.to_datetime(expected["next_expected_date"])
    expected = expected.sort_values("next_expected_date")

    headers = [
        "Payee",
        "Category",
        "Cadence",
        "Expected",
        "Due in (days)",
        "Current amount (PLN)",
        "Monthly run-rate (PLN)",
    ]
    body_rows = []
    for _, row in expected.iterrows():
        due_in = int((row["next_expected_date"] - as_of).days)
        body_rows.append(
            html.Tr(
                [
                    html.Td(str(row["payee"])),
                    html.Td(_fmt_text(row.get("category"))),
                    html.Td(_fmt_text(row.get("cadence_class"))),
                    html.Td(_fmt_date(row["next_expected_date"])),
                    html.Td("overdue" if due_in < 0 else str(due_in)),
                    html.Td(_fmt_pln(row.get("amount_current"))),
                    html.Td(_fmt_pln(row.get("monthly_run_rate"))),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(html.Tr([html.Th(header) for header in headers])),
            html.Tbody(body_rows),
        ],
        id="recurring-renewal-table",
        className="table table-sm table-striped table-bordered",
    )


def _recurring_ttm_share(run_rate: float, split: pd.DataFrame | None, as_of: pd.Timestamp):
    """Return (value, subtitle) for the share-of-TTM-expense KPI."""
    if split is None or split.empty or "withdrawals" not in split.columns:
        return "—", "TTM expense unavailable"

    as_of_month = as_of.strftime("%Y-%m")
    # Complete months only: the current month is partial and would understate
    # the average monthly denominator (and inflate the share).
    history = (
        split[split["year_month"].astype(str) < as_of_month]
        .sort_values("year_month")
        .tail(12)
    )
    if history.empty:
        return "—", "TTM expense unavailable"

    ttm_expense = float(history["withdrawals"].sum())
    if ttm_expense <= 0:
        return "—", "TTM expense unavailable"

    # Compare the monthly run-rate with an average TTM month, which is the same
    # as annualising the run-rate and dividing by TTM withdrawals.
    avg_monthly_expense = ttm_expense / len(history)
    return f"{run_rate / avg_monthly_expense * 100:.1f}%", "vs average TTM month"


def _recurring_category_table(summary: pd.DataFrame) -> html.Table:
    """Render the category roll-up table."""
    headers = [
        "Category",
        "Stability",
        "Avg spend (PLN)",
        "Share of spend",
        "Recurring (PLN)",
        "Coverage",
        "Spend CV",
        "Commitments (active/total)",
        "Price changes (12m)",
        "Top payee",
    ]
    body_rows = []
    for _, row in summary.iterrows():
        active = row.get("n_active_commitments")
        total = row.get("n_commitments")
        coverage = row.get("recurring_coverage")
        spend_cv = row.get("spend_cv")
        price_changes = row.get("n_price_changes_12m")
        steady = bool(row.get("is_steady", False))
        body_rows.append(
            html.Tr(
                [
                    html.Td(_fmt_text(row.get("category"))),
                    html.Td(
                        dbc.Badge(
                            "Steady" if steady else "Variable",
                            color="success" if steady else "secondary",
                        )
                    ),
                    html.Td(_fmt_pln(row.get("avg_monthly_spend"))),
                    html.Td(
                        f"{row.get('share_of_spend'):.1%}"
                        if pd.notna(row.get("share_of_spend"))
                        else "—"
                    ),
                    html.Td(_fmt_pln(row.get("recurring_run_rate"))),
                    html.Td(f"{coverage:.0%}" if pd.notna(coverage) else "—"),
                    html.Td(f"{spend_cv:.1%}" if pd.notna(spend_cv) else "—"),
                    html.Td(
                        f"{int(active) if pd.notna(active) else 0}/"
                        f"{int(total) if pd.notna(total) else 0}"
                    ),
                    html.Td(
                        str(int(price_changes)) if pd.notna(price_changes) else "—"
                    ),
                    html.Td(_fmt_text(row.get("top_payee"))),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(html.Tr([html.Th(header) for header in headers])),
            html.Tbody(body_rows),
        ],
        id="recurring-category-table",
        className="table table-sm table-striped table-bordered",
    )


def _recurring_panel() -> html.Div:
    register = load_parquet("processed", "recurring_commitments")
    if register is None:
        return _MISSING

    events = load_parquet("processed", "recurring_commitment_events")
    split = load_parquet("interim", "monthly_cashflow_split")
    category_summary = load_parquet("processed", "recurring_categories")

    as_of = pd.Timestamp.today().normalize()
    active = register[register["is_active"].fillna(False)]
    total_run_rate = (
        float(active["monthly_run_rate"].sum()) if not active.empty else 0.0
    )
    share_value, share_subtitle = _recurring_ttm_share(total_run_rate, split, as_of)

    kpi_row = dbc.Row(
        [
            _kpi_card(
                "Active commitments",
                str(len(active)),
                f"{len(register)} detected in total",
            ),
            _kpi_card(
                "Active monthly run-rate",
                f"{total_run_rate:,.0f} PLN",
                "trailing 12 months",
            ),
            _kpi_card("Share of TTM expense", share_value, share_subtitle),
        ]
    )

    if category_summary is None or category_summary.empty:
        category_children = [
            html.H5("Recurring spend by category", className="mt-3"),
            dbc.Alert(
                "Category summary not found. Re-run the pipeline to generate "
                "recurring_categories.parquet.",
                color="info",
                id="recurring-category-empty",
            ),
            dcc.Graph(
                id="recurring-category-chart",
                figure=make_empty_figure(
                    "Category summary not available.",
                    title="Recurring Spend by Category",
                ),
            ),
        ]
    else:
        category_children = [
            html.H5("Recurring spend by category", className="mt-3"),
            dcc.Graph(
                id="recurring-category-chart",
                figure=plot_recurring_by_category(category_summary),
            ),
            _recurring_category_table(category_summary),
        ]

    payees = [
        str(p) for p in register["payee"].dropna().tolist()
    ]
    initial_payee = payees[0] if payees else None
    initial_fig = (
        plot_payee_amount_history(events, initial_payee)
        if events is not None and not events.empty and initial_payee
        else make_empty_figure(
            "No payment history available.", title="Recurring Payee History"
        )
    )

    return html.Div(
        [
            html.H4("Recurring Costs", className="mb-3"),
            kpi_row,
            *category_children,
            html.H5("Top commitments by monthly run-rate", className="mt-3"),
            dcc.Graph(
                id="recurring-runrate-chart",
                figure=plot_recurring_run_rate(register),
            ),
            html.H5("Commitment register", className="mt-4"),
            _recurring_register_table(register),
            html.H5("Renewal calendar — next 60 days", className="mt-4"),
            _recurring_renewal_table(register, as_of),
            html.H5("Payee payment history", className="mt-4"),
            dcc.Dropdown(
                id="recurring-payee-select",
                options=[{"label": p, "value": p} for p in payees],
                value=initial_payee,
                clearable=False,
                style={"width": "50%"},
            ),
            dcc.Graph(id="recurring-payee-chart", figure=initial_fig),
        ]
    )


@callback(
    Output("recurring-payee-chart", "figure"),
    Input("recurring-payee-select", "value"),
    prevent_initial_call=True,
)
def update_recurring_payee_chart(payee: str) -> go.Figure:
    if not payee:
        return make_empty_figure(
            "Choose a payee to display its payment history.",
            title="Recurring Payee History",
        )
    events = load_parquet("processed", "recurring_commitment_events")
    if events is None:
        return make_empty_figure(
            "Recurring commitment data not found.",
            title="Recurring Payee History",
        )
    return plot_payee_amount_history(events, payee)


# ══════════════════════════════════════════════════════════════════════════════
# Tab — Budget Health (Conscious Spending Plan)
# ══════════════════════════════════════════════════════════════════════════════

_STATUS_BADGES: dict[str, tuple[str, str]] = {
    "ok": ("✔ ok", "success"),
    "attention": ("⚠ attention", "warning"),
    "info": ("ℹ info", "secondary"),
    "insufficient data": ("— insufficient data", "light"),
    "not applicable": ("– not applicable", "light"),
    "not measured": ("? not measured", "light"),
}

_BUDGET_ASSUMPTIONS = (
    "Take-home = income-category rows minus the take_home_deductions categories "
    "(VAT, PIT, ZUS), trailing 12 complete months.",
    "Buckets come from the category mapping in configs/pipeline.yaml; refunds reduce "
    "the bucket they land in.",
    "Savings and investments = net inflow into accounts in those account groups. "
    "Money held outside MMEX is invisible.",
    "Balances = MMEX initial balance + every transaction leg; foreign currency is valued "
    "at the latest rate implied by converted transactions on or before each month end.",
    "Targets are Ramit Sethi's (I Will Teach You to Be Rich, 2nd ed.), not norms "
    "derived from this data.",
)


def _budget_health_config() -> dict:
    try:
        return load_config().get("budget_health") or {}
    except FileNotFoundError:
        return {}


def _fmt_check_value(check: dict) -> str:
    """Format a scorecard value by its unit; dashes for missing values."""
    value = check.get("value")
    if value is None or pd.isna(value):
        return "—"
    unit = check.get("unit", "share")
    if unit == "months":
        return f"{value:,.1f} months"
    if unit == "currency":
        return f"{value:,.0f} PLN"
    return f"{value:.1%}"


def _budget_scorecard_table(checks: list[dict]) -> html.Table:
    """Render the Conscious Spending Plan checks with icon + label status badges."""
    headers = ["Check", "Value", "Sethi target", "Status", "Detail"]
    rows = []
    for check in checks:
        label, color = _STATUS_BADGES.get(check["status"], (check["status"], "light"))
        text_color = "dark" if color in ("warning", "light") else None
        rows.append(
            html.Tr(
                [
                    html.Td(check["check"]),
                    html.Td(_fmt_check_value(check)),
                    html.Td(check["target"]),
                    html.Td(dbc.Badge(label, color=color, text_color=text_color)),
                    html.Td(check["detail"], className="small"),
                ]
            )
        )
    return html.Table(
        [html.Thead(html.Tr([html.Th(h) for h in headers])), html.Tbody(rows)],
        id="budget-scorecard-table",
        className="table table-sm table-striped table-bordered",
    )


def _budget_balances_table(balances: pd.DataFrame | None, top_n: int = 15) -> html.Div:
    """Latest month-end balance per account, largest absolute value first."""
    if balances is None or balances.empty:
        return dbc.Alert(
            "Account balances need the .mmb input (not available for CSV).",
            color="info",
            id="budget-balances-table",
        )
    latest = balances[balances["year_month"] == balances["year_month"].max()]
    latest = latest[latest["balance_pln"].abs() >= 0.5]
    latest = latest.loc[latest["balance_pln"].abs().sort_values(ascending=False).index]
    headers = ["Account", "Group", "Currency", "Balance", "Balance (PLN)"]
    rows = [
        html.Tr(
            [
                html.Td(str(row["account"])),
                html.Td(str(row["group"])),
                html.Td(_fmt_text(row["currency"])),
                html.Td(f"{row['balance']:,.2f}"),
                html.Td(_fmt_pln(row["balance_pln"])),
            ]
        )
        for _, row in latest.head(top_n).iterrows()
    ]
    return html.Table(
        [html.Thead(html.Tr([html.Th(h) for h in headers])), html.Tbody(rows)],
        id="budget-balances-table",
        className="table table-sm table-striped table-bordered",
    )


def _budget_creep_table(annual: pd.DataFrame) -> html.Table:
    """Annual take-home vs living spend with the deltas and marginal spend rate."""
    headers = [
        "Year",
        "Months",
        "Take-home (PLN)",
        "Living spend (PLN)",
        "Share",
        "Δ take-home (PLN)",
        "Δ living spend (PLN)",
        "Marginal spend rate",
    ]
    rows = [
        html.Tr(
            [
                html.Td(str(int(row["year"]))),
                html.Td(str(int(row["n_months"]))),
                html.Td(_fmt_pln(row["take_home"])),
                html.Td(_fmt_pln(row["living_spend"])),
                html.Td(f"{row['living_share']:.0%}" if pd.notna(row["living_share"]) else "—"),
                html.Td(_fmt_pln(row["delta_take_home"])),
                html.Td(_fmt_pln(row["delta_living_spend"])),
                html.Td(
                    f"{row['marginal_spend_rate']:.2f}"
                    if pd.notna(row["marginal_spend_rate"])
                    else "—"
                ),
            ]
        )
        for _, row in annual.iterrows()
    ]
    return html.Table(
        [html.Thead(html.Tr([html.Th(h) for h in headers])), html.Tbody(rows)],
        id="budget-creep-table",
        className="table table-sm table-striped table-bordered",
    )


def _budget_health_kpis(summary: dict, checks: dict[str, dict]) -> list[dbc.Row]:
    shares = summary["shares"]

    def share(value: float | None) -> str:
        return f"{value:.0%}" if value is not None and pd.notna(value) else "—"

    take_home = summary["take_home_monthly"]
    window = f"12 months to {summary['as_of_month']}" if summary["as_of_month"] else "—"
    return [
        dbc.Row(
            [
                _kpi_card(
                    "Take-home / month",
                    f"{take_home:,.0f} PLN" if pd.notna(take_home) else "—",
                    window,
                ),
                _kpi_card("Fixed costs", share(shares.get("fixed")), "Sethi: 50–60%"),
                _kpi_card("Guilt-free", share(shares.get("guilt_free")), "Sethi: 20–35%"),
                _kpi_card("Investments", share(shares.get("investments")), "Sethi: 10%"),
            ]
        ),
        dbc.Row(
            [
                _kpi_card("Savings goals", share(shares.get("savings")), "Sethi: 5–10%"),
                _kpi_card(
                    "Unallocated", share(shares.get("unallocated")), "left in cash / IOUs"
                ),
                _kpi_card(
                    "Emergency fund",
                    _fmt_check_value(checks.get("Emergency fund", {})),
                    "savings accounts ÷ fixed costs",
                ),
                _kpi_card(
                    "Reserve above ceiling",
                    _fmt_check_value(checks.get("Reserve above emergency-fund ceiling", {})),
                    "beyond 6 months of fixed costs",
                ),
            ]
        ),
    ]


def _next_100_card(steps: list[dict]) -> dbc.Card:
    """Render Sethi's next-100 priority order, marking the single next step."""
    items = []
    for step in steps:
        label, color = _STATUS_BADGES.get(step["status"], (step["status"], "light"))
        text_color = "dark" if color in ("warning", "light") else None
        heading = step["step"] + (" ← next step" if step["is_next"] else "")
        items.append(
            dbc.ListGroupItem(
                [
                    html.Div(
                        [
                            html.Span(heading, className="fw-semibold"),
                            dbc.Badge(
                                label,
                                color=color,
                                text_color=text_color,
                                className="ms-2",
                            ),
                        ]
                    ),
                    html.Div(step["detail"], className="small text-muted"),
                ],
                color="primary" if step["is_next"] else None,
            )
        )
    return dbc.Card(
        [
            dbc.CardHeader("Where the next 100 PLN goes (Sethi's priority order)"),
            dbc.ListGroup(items, flush=True),
        ],
        id="budget-next100-card",
        className="mb-3",
    )


def _budget_health_panel() -> html.Div:
    monthly = load_parquet("processed", "budget_health_monthly")
    if monthly is None:
        return _MISSING
    balances = load_parquet("processed", "account_balances")
    cfg = _budget_health_config()
    summary = summarise_budget_health(monthly, balances, config=cfg)
    annual = build_annual_creep(monthly)
    checks = {c["check"]: c for c in summary["checks"]}
    steps = next_100_steps(summary, config=cfg)
    reserve_trend = build_reserve_trend(monthly, balances, config=cfg)

    children: list = [html.H4("Budget Health — Conscious Spending Plan", className="mb-3")]
    if not summary["mapping_confirmed"]:
        children.append(
            dbc.Alert(
                "Draft mapping (HR-6): the category → bucket and account → group mappings "
                "in configs/pipeline.yaml (budget_health) are not confirmed yet. Review "
                "them, then set mapping_confirmed: true.",
                color="warning",
                id="budget-mapping-warning",
            )
        )
    children += [
        html.Details(
            [
                html.Summary("Assumptions"),
                html.Ol([html.Li(a) for a in _BUDGET_ASSUMPTIONS], className="small"),
            ],
            className="mb-3",
        ),
        *_budget_health_kpis(summary, checks),
        _next_100_card(steps),
        html.H5("Scorecard", className="mt-2"),
        _budget_scorecard_table(summary["checks"]),
        dcc.Graph(
            id="budget-reserve-chart",
            figure=plot_reserve_vs_ceiling(reserve_trend),
        ),
        dcc.Graph(
            id="budget-plan-chart",
            figure=plot_spending_plan_vs_target(
                summary["shares"], cfg.get("targets") or {}, summary["as_of_month"]
            ),
        ),
        dcc.Graph(id="budget-allocation-chart", figure=plot_take_home_allocation(monthly)),
        dcc.Graph(id="budget-balances-chart", figure=plot_balances_by_group(balances)),
        html.H5("Largest balances (latest month end)", className="mt-3"),
        _budget_balances_table(balances),
        dcc.Graph(id="budget-creep-chart", figure=plot_lifestyle_creep(annual)),
        html.P(
            "Marginal spend rate = change in living spend ÷ change in take-home versus the "
            "previous year; it is blank when take-home did not rise (the ratio is defined "
            "only for extra take-home). Complete calendar years only.",
            className="text-muted small",
        ),
        _budget_creep_table(annual),
    ]
    return html.Div(children)


# ══════════════════════════════════════════════════════════════════════════════
# Tab — Money Flow (Sethi's automation and buffer system)
# ══════════════════════════════════════════════════════════════════════════════

_FLOW_ASSUMPTIONS = (
    "A reserve transfer is a transfer from a savings-group account to a cash-group "
    "account; its amount is the positive source leg.",
    "Take-home = income-category rows minus the take_home_deductions categories. "
    "The TTM view uses trailing 12 complete months.",
    "Spent + taxes = deductions + fixed + guilt-free + one-off + unmapped for the month.",
    "Adherence uses budget_health.plan. Nothing is inferred when salary_pln, "
    "salary_day or investment_pln is null.",
    "Every view states the number of complete months it covers.",
)

_FLOW_KEYS = (
    "gross_income",
    "deductions",
    "take_home",
    "fixed",
    "guilt_free",
    "one_off",
    "unmapped",
    "savings",
    "investments",
    "unallocated",
)


def _flow_waterfall_values(row: pd.Series, suffix: str = "") -> dict[str, float]:
    """Pick the waterfall inputs from a monthly row, with an optional _ttm suffix."""
    return {key: row.get(f"{key}{suffix}") for key in _FLOW_KEYS}


def _flow_waterfall_figure(monthly: pd.DataFrame, month: str) -> go.Figure:
    """Waterfall for the TTM window or a single calendar month."""
    complete = monthly[~monthly["is_partial_month"].astype(bool)]
    if month == "ttm":
        if complete.empty:
            return plot_take_home_waterfall({}, "Where the last 12 months' take-home went")
        row = complete.iloc[-1]
        values = _flow_waterfall_values(row, "_ttm")
        title = f"Where the last 12 months' take-home went — 12 months to {row['year_month']}"
        return plot_take_home_waterfall(values, title)

    match = monthly[monthly["year_month"] == month]
    if match.empty:
        return make_empty_figure(
            "No budget health data for the selected month.", title="Take-home waterfall"
        )
    row = match.iloc[0]
    partial = " (partial month)" if bool(row.get("is_partial_month")) else ""
    title = f"Where {month} take-home went — 1 month{partial}"
    return plot_take_home_waterfall(_flow_waterfall_values(row), title)


def _flow_regularity_table(summary: dict) -> html.Table:
    """One-row summary of the last 12 complete months of reserve transfers."""

    def number(value: float) -> str:
        return "—" if value is None or pd.isna(value) else f"{value:,.0f}"

    mean = number(summary.get("mean"))
    lo = number(summary.get("min"))
    hi = number(summary.get("max"))
    cv = summary.get("cv")
    headers = [
        "Complete months",
        "Months with transfer",
        "Mean moved (PLN)",
        "Range (PLN)",
        "CV",
        "Distinct transfer days",
        "Invoice arrival days",
    ]
    cells = [
        summary.get("n_months", 0),
        summary.get("months_with_transfer", 0),
        mean,
        "—" if lo == "—" or hi == "—" else f"{lo}–{hi}",
        "—" if cv is None or pd.isna(cv) else f"{cv:.2f}",
        summary.get("distinct_transfer_days", 0),
        summary.get("income_days_range") or "—",
    ]
    return html.Table(
        [
            html.Caption(
                "Sethi's buffer system: a fixed salary from the reserve on a fixed day",
                className="text-muted small",
            ),
            html.Thead(html.Tr([html.Th(header) for header in headers])),
            html.Tbody(html.Tr([html.Td(str(cell)) for cell in cells])),
        ],
        id="flow-regularity-table",
        className="table table-sm table-striped table-bordered",
    )


def _flow_adherence(summary: dict, cfg: dict) -> html.Div:
    """Adherence counts, or an info alert explaining how to set budget_health.plan."""
    plan = cfg.get("plan") or {}
    salary_plan = plan.get("salary_pln")
    investment_plan = plan.get("investment_pln")
    if salary_plan is None and investment_plan is None:
        return html.Div(
            dbc.Alert(
                "Set budget_health.plan.salary_pln and investment_pln in "
                "configs/pipeline.yaml to measure plan adherence.",
                color="info",
            ),
            id="flow-plan-adherence",
        )

    lines: list[html.P] = []
    if salary_plan is not None:
        adherence = summary.get("salary_adherence")
        if adherence is None:
            lines.append(html.P(
                "Salary adherence: needs reserve transfers from the .mmb input.",
                className="small mb-1",
            ))
        else:
            lines.append(html.P(
                f"Salary transfer on plan: {adherence['on_plan']} of {adherence['of']} "
                "months (±10% amount, ±3 days).",
                className="small mb-1",
            ))
    if investment_plan is not None:
        adherence = summary.get("investment_adherence")
        if adherence is None:
            lines.append(html.P(
                "Investment adherence: no complete months to measure.",
                className="small mb-1",
            ))
        else:
            lines.append(html.P(
                f"Investment on plan: {adherence['on_plan']} of {adherence['of']} months.",
                className="small mb-1",
            ))
    return html.Div(lines, id="flow-plan-adherence")


def _money_flow_panel() -> html.Div:
    monthly = load_parquet("processed", "budget_health_monthly")
    if monthly is None:
        return _MISSING

    transfers = load_parquet("processed", "reserve_transfers")
    cfg = _budget_health_config()
    summary = summarise_reserve_transfers(transfers, monthly, config=cfg)

    complete = monthly[~monthly["is_partial_month"].astype(bool)]
    month_options = [{"label": "Last 12 months", "value": "ttm"}]
    month_options += [
        {"label": month, "value": month} for month in reversed(complete["year_month"].tolist())
    ]

    if transfers is None or transfers.empty:
        alert_text = "Reserve transfers need the .mmb input (not available for CSV)."
        transfers_chart = dbc.Alert(alert_text, color="info", id="flow-transfers-chart")
        regularity: html.Table | dbc.Alert = dbc.Alert(
            alert_text, color="info", id="flow-regularity-table"
        )
    else:
        transfers_chart = dcc.Graph(
            id="flow-transfers-chart",
            figure=plot_reserve_transfers(transfers, monthly),
        )
        regularity = _flow_regularity_table(summary)

    return html.Div(
        [
            html.H4("Money Flow", className="mb-3"),
            html.P(
                "Where take-home goes, and whether money moves automatically.",
                className="text-muted",
            ),
            html.Details(
                [
                    html.Summary("Assumptions"),
                    html.Ol([html.Li(a) for a in _FLOW_ASSUMPTIONS], className="small"),
                ],
                className="mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Month"),
                            dcc.Dropdown(
                                id="flow-month-select",
                                options=month_options,
                                value="ttm",
                                clearable=False,
                            ),
                        ],
                        width=4,
                    )
                ],
                className="mb-3",
            ),
            dcc.Graph(
                id="flow-waterfall-chart",
                figure=_flow_waterfall_figure(monthly, "ttm"),
            ),
            html.H5("Reserve → spending accounts", className="mt-3"),
            transfers_chart,
            regularity,
            html.H5("Plan adherence", className="mt-3"),
            _flow_adherence(summary, cfg),
        ]
    )


@callback(
    Output("flow-waterfall-chart", "figure"),
    Input("flow-month-select", "value"),
    prevent_initial_call=True,
)
def update_flow_waterfall(month: str | None) -> go.Figure:
    monthly = load_parquet("processed", "budget_health_monthly")
    if monthly is None:
        return make_empty_figure(
            "Pipeline data not found. Run the pipeline first.",
            title="Take-home waterfall",
        )
    return _flow_waterfall_figure(monthly, month or "ttm")


# ══════════════════════════════════════════════════════════════════════════════
# Tab — Big Wins (invest scenario + cut costs)
# ══════════════════════════════════════════════════════════════════════════════

_WINS_ASSUMPTIONS = (
    "This is a scenario at your assumed rates, not a forecast (HR-1).",
    "No default return, inflation rate or benchmark: you type both annual rates (HR-3).",
    "Compounding: monthly rate = (1 + r)^(1/12) − 1; contributions arrive at month end.",
    "Extra take-home from a raise = raise % × take-home. Deductions are assumed to "
    "scale proportionally with the invoice.",
    "The extra take-home is added to the monthly investment (Sethi's CEO method).",
    "Rule of 72: doubling years = 72 ÷ (annual rate × 100); no value for a rate ≤ 0.",
    "No instrument, product or tax action is named (HR-5).",
)


def _subtab_container(
    prefix: str,
    tabs: list[tuple[str, str]],
    default: str,
    render: Callable[[str], Any],
) -> html.Div:
    """Sub-tab bar plus inline default content (Decision 3)."""
    return html.Div(
        [
            dbc.Tabs(
                [dbc.Tab(label=label, tab_id=tab_id) for tab_id, label in tabs],
                id=f"{prefix}-subtabs",
                active_tab=default,
            ),
            html.Div(id=f"{prefix}-subtab-content", children=render(default)),
        ]
    )


def _as_float(value: Any, default: float | None = None) -> float | None:
    """Coerce an input value to float, returning ``default`` for blank/invalid."""
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_rate(value: Any) -> float | None:
    """Convert a percent-per-year input to a decimal, or None when blank."""
    parsed = _as_float(value)
    return None if parsed is None else parsed / 100.0


def _fmt_pln_or_dash(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.0f} PLN"


def _big_wins_summary(
    scenario: pd.DataFrame, rate_low: float | None, rate_high: float | None
) -> html.Div:
    """Scenario totals at the horizon plus Rule-of-72 doubling times."""
    last = scenario.iloc[-1]
    rows = [
        ("Total contributed", _fmt_pln_or_dash(last["contributions"])),
        ("Kept in reserve at horizon", _fmt_pln_or_dash(last["kept_in_reserve"])),
        ("Invested at your low rate", _fmt_pln_or_dash(last["invested_low"])),
        ("Invested at your high rate", _fmt_pln_or_dash(last["invested_high"])),
    ]
    for label, rate in (("low", rate_low), ("high", rate_high)):
        years = rule_of_72_years(rate)
        rows.append((
            f"Rule of 72: doubling years at your {label} rate",
            "—" if years is None else f"{years:.1f} years",
        ))

    children: list = [
        html.Table(
            [
                html.Thead(html.Tr([html.Th("Measure"), html.Th("At horizon")])),
                html.Tbody([
                    html.Tr([html.Td(label), html.Td(value)]) for label, value in rows
                ]),
            ],
            className="table table-sm table-striped table-bordered",
        )
    ]
    if rate_low is None or rate_high is None:
        children.insert(
            0,
            dbc.Alert(
                "Enter your own low/high annual return to draw the range band.",
                color="info",
            ),
        )
    return html.Div(children)


def _big_wins_outputs(
    lump: Any,
    monthly: Any,
    raise_pct: Any,
    rate_low: Any,
    rate_high: Any,
    reserve_rate: Any,
    horizon_years: Any,
    take_home_monthly: float | None,
) -> tuple[go.Figure, html.Div]:
    """Build the Big Wins figure and summary from raw input values."""
    lump_value = _as_float(lump, 0.0) or 0.0
    monthly_value = _as_float(monthly, 0.0) or 0.0
    raise_value = _as_float(raise_pct, 0.0) or 0.0
    horizon = int(_as_float(horizon_years, 10) or 10)
    low = _as_rate(rate_low)
    high = _as_rate(rate_high)
    reserve = _as_rate(reserve_rate)
    take_home = 0.0 if take_home_monthly is None or pd.isna(take_home_monthly) else float(
        take_home_monthly
    )
    raise_monthly = raise_value / 100.0 * take_home

    try:
        scenario = build_big_wins_scenario(
            lump_sum=lump_value,
            monthly=monthly_value,
            raise_monthly=raise_monthly,
            rate_low=low,
            rate_high=high,
            reserve_rate=reserve,
            months=horizon * 12,
        )
    except ValueError as exc:
        return (
            make_empty_figure("Scenario inputs are invalid.", title="Big Wins"),
            dbc.Alert(str(exc), color="danger"),
        )
    return plot_big_wins(scenario), _big_wins_summary(scenario, low, high)


def _invest_panel() -> html.Div:
    monthly = load_parquet("processed", "budget_health_monthly")
    if monthly is None:
        return _MISSING
    balances = load_parquet("processed", "account_balances")
    cfg = _budget_health_config()
    summary = summarise_budget_health(monthly, balances, config=cfg)
    checks = {c["check"]: c for c in summary["checks"]}
    take_home = summary["take_home_monthly"]
    default_monthly = int(round(0.10 * take_home, -2)) if pd.notna(take_home) else None
    reserve_above = _fmt_check_value(checks.get("Reserve above emergency-fund ceiling", {}))

    default_fig, default_summary = _big_wins_outputs(
        lump=0.0,
        monthly=default_monthly,
        raise_pct=0.0,
        rate_low=None,
        rate_high=None,
        reserve_rate=None,
        horizon_years=10,
        take_home_monthly=take_home,
    )

    inputs = [
        html.Label("Lump sum available to invest (PLN)", className="fw-semibold"),
        dbc.Input(id="wins-lump", type="number", min=0, value=0, debounce=True),
        html.P(f"Reserve above ceiling: {reserve_above}", className="text-muted small"),
        html.Label("Monthly investment (PLN)", className="fw-semibold mt-2"),
        dbc.Input(
            id="wins-monthly",
            type="number",
            min=0,
            value=default_monthly,
            placeholder="e.g. 2500",
            debounce=True,
        ),
        html.Label("Raise (%)", className="fw-semibold mt-2"),
        dcc.Slider(
            id="wins-raise-pct",
            min=0,
            max=30,
            step=1,
            value=0,
            marks={0: "0%", 10: "10%", 20: "20%", 30: "30%"},
        ),
        html.Label("Annual return: low (%)", className="fw-semibold mt-2"),
        dbc.Input(
            id="wins-rate-low",
            type="number",
            min=0,
            max=100,
            placeholder="e.g. 4",
            debounce=True,
        ),
        html.Label("Annual return: high (%)", className="fw-semibold mt-1"),
        dbc.Input(
            id="wins-rate-high",
            type="number",
            min=0,
            max=100,
            placeholder="e.g. 8",
            debounce=True,
        ),
        html.Label(
            "Reserve account's interest rate (% per year; empty = 0)",
            className="fw-semibold mt-2",
        ),
        dbc.Input(
            id="wins-reserve-rate",
            type="number",
            min=0,
            max=100,
            placeholder="e.g. 2",
            debounce=True,
        ),
        html.Label("Horizon (years)", className="fw-semibold mt-2"),
        dcc.Slider(
            id="wins-horizon",
            min=1,
            max=30,
            step=1,
            value=10,
            marks={1: "1", 10: "10", 20: "20", 30: "30"},
        ),
    ]

    return html.Div(
        [
            html.H4("Big Wins — what moves the needle", className="mb-3"),
            html.P(
                "A scenario at your assumed rates, not a forecast. The band is your "
                "low/high annual return.",
                className="text-muted",
            ),
            html.Details(
                [
                    html.Summary("Assumptions"),
                    html.Ol([html.Li(a) for a in _WINS_ASSUMPTIONS], className="small"),
                ],
                className="mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(inputs, width=4, style={"maxHeight": "80vh", "overflowY": "auto"}),
                    dbc.Col(
                        [
                            dcc.Graph(id="wins-chart", figure=default_fig),
                            html.Div(id="wins-summary", children=default_summary),
                        ],
                        width=8,
                    ),
                ]
            ),
        ]
    )


@callback(
    Output("wins-chart", "figure"),
    Output("wins-summary", "children"),
    Input("wins-lump", "value"),
    Input("wins-monthly", "value"),
    Input("wins-raise-pct", "value"),
    Input("wins-rate-low", "value"),
    Input("wins-rate-high", "value"),
    Input("wins-reserve-rate", "value"),
    Input("wins-horizon", "value"),
    prevent_initial_call=True,
)
def update_big_wins(
    lump: Any,
    monthly: Any,
    raise_pct: Any,
    rate_low: Any,
    rate_high: Any,
    reserve_rate: Any,
    horizon_years: Any,
) -> tuple[go.Figure, html.Div]:
    monthly_df = load_parquet("processed", "budget_health_monthly")
    take_home = np.nan
    if monthly_df is not None:
        summary = summarise_budget_health(monthly_df, None, config=_budget_health_config())
        take_home = summary["take_home_monthly"]
    return _big_wins_outputs(
        lump,
        monthly,
        raise_pct,
        rate_low,
        rate_high,
        reserve_rate,
        horizon_years,
        take_home,
    )


_WINS_SUBTABS: dict[str, Callable[[], Any]] = {
    "sub-invest": _invest_panel,
    "sub-cut": _whatif_panel,
}


def _render_wins_subtab(subtab: str) -> Any:
    fn = _WINS_SUBTABS.get(subtab or "")
    return fn() if fn else html.Div("Unknown sub-tab")


def _big_wins_panel() -> html.Div:
    return _subtab_container(
        "wins",
        [("sub-invest", "Invest"), ("sub-cut", "Cut costs")],
        "sub-invest",
        _render_wins_subtab,
    )


@callback(
    Output("wins-subtab-content", "children"),
    Input("wins-subtabs", "active_tab"),
    prevent_initial_call=True,
)
def render_wins_subtab(subtab: str | None) -> Any:
    return _render_wins_subtab(subtab or "")


# ══════════════════════════════════════════════════════════════════════════════
# Tab — Spending (five existing panels behind sub-tabs)
# ══════════════════════════════════════════════════════════════════════════════

_SPENDING_SUBTABS: dict[str, Callable[[], Any]] = {
    "sub-breakdown": _subcategory_panel,
    "sub-drilldown": _drilldown_panel,
    "sub-trends": _trends_panel,
    "sub-cat-trends": _cat_trends_panel,
    "sub-cat-forecast": _cat_forecast_panel,
}


def _render_spending_subtab(subtab: str) -> Any:
    fn = _SPENDING_SUBTABS.get(subtab or "")
    return fn() if fn else html.Div("Unknown sub-tab")


def _spending_panel() -> html.Div:
    return _subtab_container(
        "spending",
        [
            ("sub-breakdown", "Category breakdown"),
            ("sub-drilldown", "Drill-down"),
            ("sub-trends", "Expense trends"),
            ("sub-cat-trends", "Category trends"),
            ("sub-cat-forecast", "Category forecasts"),
        ],
        "sub-breakdown",
        _render_spending_subtab,
    )


@callback(
    Output("spending-subtab-content", "children"),
    Input("spending-subtabs", "active_tab"),
    prevent_initial_call=True,
)
def render_spending_subtab(subtab: str | None) -> Any:
    return _render_spending_subtab(subtab or "")


# ══════════════════════════════════════════════════════════════════════════════
# Tab — Runway & Forecast (savings, cash flow, Monte Carlo)
# ══════════════════════════════════════════════════════════════════════════════

_RUNWAY_SUBTABS: dict[str, Callable[[], Any]] = {
    "sub-savings": _savings_panel,
    "sub-cashflow": _cashflow_panel,
    "sub-mc": _mc_panel,
}


def _render_runway_subtab(subtab: str) -> Any:
    fn = _RUNWAY_SUBTABS.get(subtab or "")
    return fn() if fn else html.Div("Unknown sub-tab")


def _runway_panel() -> html.Div:
    return _subtab_container(
        "runway",
        [
            ("sub-savings", "Savings forecast"),
            ("sub-cashflow", "Cash flow forecast"),
            ("sub-mc", "Monte Carlo"),
        ],
        "sub-savings",
        _render_runway_subtab,
    )


@callback(
    Output("runway-subtab-content", "children"),
    Input("runway-subtabs", "active_tab"),
    prevent_initial_call=True,
)
def render_runway_subtab(subtab: str | None) -> Any:
    return _render_runway_subtab(subtab or "")
