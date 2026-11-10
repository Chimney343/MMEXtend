"""Plotly chart builders for dashboard scenario and Monte Carlo views.

Usage:
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
    fig_mc = plot_mc_fan(mc_result)
    fig_whatif = plot_reduction_scenario(scenario_df)
    fig_savings = plot_savings_fan(forecast_df, 5000, 10000, 12)
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


_DASH_TEMPLATE = "plotly_white"


def apply_standard_layout(
    fig: go.Figure,
    *,
    height: int | None = None,
    showlegend: bool = True,
) -> go.Figure:
    """Apply shared dashboard styling to a Plotly figure."""
    fig.update_layout(
        template=_DASH_TEMPLATE,
        hovermode="x unified",
        margin=dict(l=48, r=24, t=72, b=48),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0),
        showlegend=showlegend,
    )
    if height is not None:
        fig.update_layout(height=height)

    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(0,0,0,0.08)", tickformat=",.0f")
    return fig


def make_empty_figure(message: str, *, title: str = "No data available") -> go.Figure:
    """Create a consistent empty-state figure with a centered annotation."""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        align="center",
    )
    fig.update_layout(title=title)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return apply_standard_layout(fig, height=320, showlegend=False)


def plot_mc_fan(result: dict) -> go.Figure:
    """Build a fan chart for Monte Carlo cash balance simulation output."""
    months = pd.to_datetime(pd.Series(result["months"]))
    months_rev = list(months.iloc[::-1])

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=list(months) + months_rev,
            y=list(result["p90"]) + list(result["p10"][::-1]),
            fill="toself",
            fillcolor="rgba(100,149,237,0.15)",
            line=dict(width=0),
            name="10-90 percentile",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=list(months) + months_rev,
            y=list(result["p75"]) + list(result["p25"][::-1]),
            fill="toself",
            fillcolor="rgba(100,149,237,0.35)",
            line=dict(width=0),
            name="25-75 percentile",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=months,
            y=result["p50"],
            line=dict(color="royalblue", width=2),
            mode="lines+markers",
            name="Median",
            hovertemplate="Month: %{x|%Y-%m}<br>Median: %{y:,.0f} PLN<extra></extra>",
        )
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="red",
        opacity=0.5,
        annotation_text="Balance = 0",
        annotation_position="bottom right",
    )
    fig.update_layout(
        title="Monte Carlo Cash Balance Forecast",
        xaxis_title="Month",
        yaxis_title="Projected Balance (PLN)",
    )
    return apply_standard_layout(fig, height=430)


def plot_reduction_scenario(result: pd.DataFrame) -> go.Figure:
    """Build monthly and cumulative savings chart for reduction scenarios."""
    x_values = pd.to_datetime(result["month"], errors="coerce")
    if x_values.notna().all():
        x_axis = x_values
        x_hover = "%{x|%Y-%m}"
    else:
        x_axis = result["month"]
        x_hover = "%{x}"

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=x_axis,
            y=result["delta_monthly"],
            name="Monthly saving",
            marker_color="rgba(46, 204, 113, 0.7)",
            hovertemplate=(
                f"Month: {x_hover}<br>Monthly saving: %{{y:,.0f}} PLN<extra></extra>"
            ),
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=x_axis,
            y=result["delta_cumulative"],
            name="Cumulative saving",
            line=dict(color="#e74c3c", width=2),
            mode="lines+markers",
            hovertemplate=(
                f"Month: {x_hover}<br>Cumulative saving: %{{y:,.0f}} PLN<extra></extra>"
            ),
        ),
        secondary_y=True,
    )

    fig.update_layout(title="Expense Reduction Scenario", xaxis_title="Month")
    fig.update_yaxes(title_text="Monthly saving (PLN)", secondary_y=False)
    fig.update_yaxes(title_text="Cumulative saving (PLN)", secondary_y=True)
    return apply_standard_layout(fig, height=430)


def plot_recurring_run_rate(register_df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    """Horizontal bar chart of the largest monthly recurring run-rates.

    Parameters
    ----------
    register_df : pd.DataFrame
        Recurring-commitment register with ``payee`` and ``monthly_run_rate``.
    top_n : int
        Number of commitments to display (largest first).
    """
    if register_df is None or register_df.empty or "monthly_run_rate" not in register_df.columns:
        return make_empty_figure(
            "No recurring commitments detected.",
            title="Recurring Commitments — Monthly Run-Rate",
        )

    top = (
        register_df.dropna(subset=["monthly_run_rate"])
        .nlargest(top_n, "monthly_run_rate")
        .sort_values("monthly_run_rate")
    )
    if top.empty:
        return make_empty_figure(
            "No recurring commitments detected.",
            title="Recurring Commitments — Monthly Run-Rate",
        )

    fig = go.Figure(
        go.Bar(
            x=top["monthly_run_rate"],
            y=top["payee"],
            orientation="h",
            marker_color="#1a73e8",
            hovertemplate="%{y}<br>%{x:,.0f} PLN/month<extra></extra>",
        )
    )
    fig.update_layout(
        title="Recurring Commitments — Monthly Run-Rate",
        xaxis_title="PLN / month",
        yaxis_title="",
    )
    return apply_standard_layout(
        fig, height=max(320, len(top) * 28 + 120), showlegend=False
    )


def plot_recurring_by_category(
    summary_df: pd.DataFrame, top_n: int = 15
) -> go.Figure:
    """Stacked bars comparing recurring and other spend per category.

    Parameters
    ----------
    summary_df : pd.DataFrame
        Category summary with ``category``, ``avg_monthly_spend`` and
        ``recurring_run_rate``.
    top_n : int
        Number of categories to display (largest average spend first).
    """
    if (
        summary_df is None
        or summary_df.empty
        or "avg_monthly_spend" not in summary_df.columns
    ):
        return make_empty_figure(
            "No category summary available.",
            title="Recurring Spend by Category",
        )

    top = summary_df.nlargest(top_n, "avg_monthly_spend").sort_values(
        "avg_monthly_spend"
    )
    recurring = top["recurring_run_rate"].clip(lower=0)
    other = (top["avg_monthly_spend"] - recurring).clip(lower=0)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=recurring,
            y=top["category"],
            orientation="h",
            name="Recurring commitments",
            marker_color="#1a73e8",
            hovertemplate="%{y}<br>Recurring: %{x:,.0f} PLN/month<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=other,
            y=top["category"],
            orientation="h",
            name="Other spend",
            marker_color="#bdc3c7",
            hovertemplate="%{y}<br>Other: %{x:,.0f} PLN/month<extra></extra>",
        )
    )
    fig.update_layout(
        barmode="stack",
        title="Recurring vs other spend by category (average month)",
        xaxis_title="PLN / month",
        yaxis_title="",
    )
    return apply_standard_layout(
        fig, height=max(340, len(top) * 30 + 120)
    )


def plot_payee_amount_history(events_df: pd.DataFrame, payee: str) -> go.Figure:
    """Amount-over-time line for one payee, with price-change points marked.

    Parameters
    ----------
    events_df : pd.DataFrame
        Recurring-commitment events with ``payee``, ``date``, ``amount`` and
        ``is_price_change``.
    payee : str
        Exact payee string to plot.
    """
    if (
        events_df is None
        or events_df.empty
        or not payee
        or "payee" not in events_df.columns
    ):
        return make_empty_figure(
            f"No transaction history for '{payee}'.",
            title="Recurring Payee History",
        )

    selected = events_df[events_df["payee"] == payee].sort_values("date")
    if selected.empty:
        return make_empty_figure(
            f"No transaction history for '{payee}'.",
            title="Recurring Payee History",
        )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=selected["date"],
            y=selected["amount"],
            mode="lines+markers",
            name="Amount",
            line=dict(color="royalblue", width=2),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f} PLN<extra></extra>",
        )
    )

    changes = selected[selected["is_price_change"]]
    if not changes.empty:
        fig.add_trace(
            go.Scatter(
                x=changes["date"],
                y=changes["amount"],
                mode="markers",
                name="Price change",
                marker=dict(color="#e74c3c", size=10, symbol="diamond"),
                hovertemplate="%{x|%Y-%m-%d}<br>New amount: %{y:,.2f} PLN<extra></extra>",
            )
        )

    fig.update_layout(
        title=f"{payee} — Recurring Amount History",
        xaxis_title="Date",
        yaxis_title="PLN",
    )
    return apply_standard_layout(fig, height=380)


def plot_category_trend(hist_y, trend, title: str = "Category Trend") -> go.Figure:
    """Monthly amount bars with a dashed Prophet in-sample trend line.

    Parameters
    ----------
    hist_y : pd.Series
        Monthly amount with a DatetimeIndex.
    trend : pd.Series
        Prophet in-sample trend component aligned to the same DatetimeIndex
        (NaN where the trend is insufficient).
    title : str
        Chart title.
    """
    months = pd.to_datetime(hist_y.index).strftime("%Y-%m").tolist()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=months,
            y=hist_y.values,
            name="Monthly amount",
            marker_color="rgba(149,165,166,0.5)",
        )
    )

    if trend is not None and len(trend) == len(hist_y) and trend.notna().any():
        fig.add_trace(
            go.Scatter(
                x=months,
                y=trend.values,
                name="Prophet trend",
                line=dict(color="#e74c3c", width=2, dash="dash"),
                mode="lines",
            )
        )

    fig.update_layout(title=title, xaxis_title="Month", yaxis_title="PLN")
    return apply_standard_layout(fig, height=420)


def plot_category_forecast(
    hist_y,
    forecast_df: pd.DataFrame,
    title: str = "Category Forecast",
    horizon_months: int | None = None,
) -> go.Figure:
    """Monthly amount bars plus a Prophet forecast line and CI ribbons.

    Parameters
    ----------
    hist_y : pd.Series
        Monthly amount with a DatetimeIndex.
    forecast_df : pd.DataFrame
        Forecast rows (already sliced to the requested horizon) with columns
        month, point_estimate, ci80_lower, ci80_upper, ci95_lower, ci95_upper.
    title : str
        Chart title.
    horizon_months : int | None
        Requested horizon, appended to the title when provided.
    """
    hist_months = pd.to_datetime(hist_y.index).strftime("%Y-%m").tolist()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=hist_months,
            y=hist_y.values,
            name="Historical",
            marker_color="rgba(149,165,166,0.5)",
            opacity=0.7,
        )
    )

    if forecast_df is not None and not forecast_df.empty:
        forecast_df = forecast_df.sort_values("month").reset_index(drop=True)
        fc_months = pd.to_datetime(forecast_df["month"]).dt.strftime("%Y-%m").tolist()
        fc_months_rev = fc_months[::-1]

        if forecast_df["ci95_lower"].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=fc_months + fc_months_rev,
                    y=list(forecast_df["ci95_upper"]) + list(forecast_df["ci95_lower"].iloc[::-1]),
                    fill="toself",
                    fillcolor="rgba(100,149,237,0.12)",
                    line=dict(width=0),
                    name="95% CI",
                    hoverinfo="skip",
                )
            )

        if forecast_df["ci80_lower"].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=fc_months + fc_months_rev,
                    y=list(forecast_df["ci80_upper"]) + list(forecast_df["ci80_lower"].iloc[::-1]),
                    fill="toself",
                    fillcolor="rgba(100,149,237,0.28)",
                    line=dict(width=0),
                    name="80% CI",
                    hoverinfo="skip",
                )
            )

        method = forecast_df["method"].iloc[0] if "method" in forecast_df.columns else "Prophet"
        fig.add_trace(
            go.Scatter(
                x=fc_months,
                y=forecast_df["point_estimate"],
                line=dict(color="royalblue", width=2),
                mode="lines+markers",
                name=f"Forecast ({method})",
            )
        )

    if horizon_months is not None:
        title = f"{title} ({horizon_months} months)"

    fig.update_layout(title=title, xaxis_title="Month", yaxis_title="PLN")
    return apply_standard_layout(fig, height=440)


def _backtest_bias_caption(forecast: pd.DataFrame, horizon: int) -> str | None:
    """
    One-line note on which way the backtests erred at the chart's last month.

    Returns None when the forecast has no backtest counts (model intervals or
    an old parquet).
    """
    needed = {
        "backtest_cum_low_k",
        "backtest_cum_n",
        "cum_point",
        "cum_ci80_lower",
        "cum_ci80_upper",
    }
    if not needed.issubset(forecast.columns):
        return None
    rows = forecast.iloc[:horizon]
    if rows.empty:
        return None
    last = rows.iloc[-1]
    if pd.isna(last["backtest_cum_low_k"]) or pd.isna(last["backtest_cum_n"]):
        return None

    k, n = int(last["backtest_cum_low_k"]), int(last["backtest_cum_n"])
    caption = f"Backtests: the actual {len(rows)}-month total came in above the forecast in {k} of {n}"
    if last["cum_point"] < last["cum_ci80_lower"]:
        caption += "; the forecast line runs below its 80% band"
    elif last["cum_point"] > last["cum_ci80_upper"]:
        caption += "; the forecast line runs above its 80% band"
    return caption


def plot_savings_fan(
    forecast: pd.DataFrame,
    hist_months: list[str],
    hist_cumulative: list[float],
    current_balance: float,
    target_amount: float | None,
    horizon: int,
) -> go.Figure:
    forecast_months_list = pd.to_datetime(forecast["month"]).dt.strftime("%Y-%m").tolist()[:horizon]

    last_hist = hist_cumulative[-1]
    cum_forecast = last_hist + forecast["point_estimate"].iloc[:horizon].cumsum().reset_index(drop=True)

    fig = go.Figure()

    # Bands are drawn from cumulative quantiles computed per path (or from the
    # backtest). Summing monthly quantiles overstates the band, so those
    # columns are not used.
    has_cum_band = {
        "cum_ci80_lower",
        "cum_ci80_upper",
    }.issubset(forecast.columns) and (
        forecast["cum_ci80_lower"].iloc[:horizon].notna().any()
        and forecast["cum_ci80_upper"].iloc[:horizon].notna().any()
    )

    if has_cum_band:
        lower = last_hist + forecast["cum_ci80_lower"].iloc[:horizon].reset_index(drop=True)
        upper = last_hist + forecast["cum_ci80_upper"].iloc[:horizon].reset_index(drop=True)
        ci_x = [hist_months[-1]] + forecast_months_list + forecast_months_list[::-1] + [hist_months[-1]]
        ci_y = [last_hist] + list(upper) + list(lower[::-1]) + [last_hist]
        method = str(forecast["interval_method"].iloc[0]) if "interval_method" in forecast.columns else "model"
        if method.startswith("empirical"):
            n_origins = method.rsplit("n", 1)[-1]
            band_label = f"80% band ({n_origins} backtests)"
        else:
            band_label = "80% band (model, uncalibrated)"
        fig.add_trace(go.Scatter(
            x=ci_x,
            y=ci_y,
            fill="toself",
            fillcolor="rgba(100,149,237,0.28)",
            line=dict(width=0),
            name=band_label,
            hoverinfo="skip",
        ))
    else:
        fig.add_annotation(
            text="Re-run `just run` to refresh bands",
            xref="paper",
            yref="paper",
            x=0.99,
            y=0.02,
            showarrow=False,
            font=dict(size=11, color="gray"),
            xanchor="right",
        )

    all_months = hist_months + forecast_months_list
    all_balance = hist_cumulative + list(cum_forecast)

    point_label = [None] * len(all_months)
    point_label[-1] = f"{all_balance[-1]:,.0f}"

    fig.add_trace(go.Scatter(
        x=all_months,
        y=all_balance,
        line=dict(color="royalblue", width=2),
        mode="lines+markers",
        name="Cumulative balance",
        text=point_label,
        textposition="top center",
        hovertemplate="Month: %{x}<br>Balance: %{y:,.0f} PLN<extra></extra>",
    ))

    if current_balance > 0:
        fig.add_hline(
            y=current_balance,
            line_dash="dash",
            line_color="gray",
            opacity=0.6,
            annotation_text="Current balance",
            annotation_position="bottom right",
        )

    if target_amount is not None and target_amount > 0:
        fig.add_hline(
            y=target_amount,
            line_dash="dot",
            line_color="green",
            opacity=0.6,
            annotation_text=f"Target {target_amount:,.0f}",
            annotation_position="top right",
        )

    title = "Savings Forecast"
    caption = _backtest_bias_caption(forecast, horizon) if has_cum_band else None
    if caption:
        title += f"<br><span style='font-size:12px;color:gray'>{caption}</span>"

    fig.update_layout(
        title=title,
        xaxis_title="Month",
        yaxis_title="Projected Balance (PLN)",
    )
    return apply_standard_layout(fig, height=440)


# ── Budget health (Conscious Spending Plan) ──────────────────────────────────

# Categorical slots in fixed order (dataviz reference palette, validated for
# adjacent pairs). Series take slots in the order they are listed per chart.
_SERIES_COLORS = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4")
_TARGET_BAND = "rgba(82,81,78,0.14)"

_PLAN_ROWS: tuple[tuple[str, str], ...] = (
    ("fixed", "Fixed costs"),
    ("investments", "Investments"),
    ("savings", "Savings goals"),
    ("guilt_free", "Guilt-free spending"),
    ("one_off", "One-off"),
    ("unallocated", "Unallocated"),
)


def plot_spending_plan_vs_target(
    shares: dict[str, float], targets: dict[str, list[float]], as_of_month: str | None
) -> go.Figure:
    """Bars of each bucket's TTM share of take-home against Sethi's target band.

    Parameters
    ----------
    shares : dict[str, float]
        Bucket → share of trailing-12-month take-home (``summarise_budget_health``).
    targets : dict[str, list[float]]
        Bucket → [low, high] target share. Buckets without a target get no band.
    as_of_month : str | None
        Last month of the TTM window, shown in the title.
    """
    if not shares or all(pd.isna(v) for v in shares.values()):
        return make_empty_figure(
            "Needs 12 complete months of data.", title="Conscious Spending Plan"
        )

    rows = [(key, label) for key, label in _PLAN_ROWS if key in shares]
    labels = [label for _, label in rows][::-1]
    values = [float(shares[key]) for key, _ in rows][::-1]

    fig = go.Figure()
    for i, (key, _) in enumerate(rows[::-1]):
        if key not in targets:
            continue
        low, high = targets[key]
        if high - low < 0.005:
            low, high = low - 0.0025, high + 0.0025
        fig.add_shape(
            type="rect", xref="x", yref="y",
            x0=low, x1=high, y0=i - 0.42, y1=i + 0.42,
            fillcolor=_TARGET_BAND, line_width=0, layer="below",
        )
    fig.add_trace(
        go.Bar(
            x=values,
            y=labels,
            orientation="h",
            marker_color=_SERIES_COLORS[0],
            text=[f"{v:.0%}" for v in values],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="%{y}: %{x:.1%} of take-home<extra></extra>",
            name="Share of take-home",
        )
    )
    window = f"12 months to {as_of_month}" if as_of_month else "trailing 12 months"
    fig.update_layout(
        title=f"Share of take-home pay — {window} (gray band = Sethi target)",
        barcornerradius=4,
        bargap=0.35,
        hovermode="closest",
    )
    fig = apply_standard_layout(fig, height=360, showlegend=False)
    upper = max(0.7, max(values) + 0.1)
    lower = min(0.0, min(values) - 0.05)
    fig.update_xaxes(tickformat=".0%", range=[lower, upper], showgrid=True,
                     gridcolor="rgba(0,0,0,0.08)", zeroline=True,
                     zerolinecolor="rgba(0,0,0,0.25)")
    fig.update_yaxes(tickformat=None, gridcolor="rgba(0,0,0,0)")
    return fig


def plot_take_home_allocation(monthly: pd.DataFrame) -> go.Figure:
    """Lines of each bucket's trailing-12-month share of take-home over time."""
    if monthly is None or monthly.empty or "take_home_ttm" not in monthly.columns:
        return make_empty_figure("No budget health data.", title="Take-home allocation")
    data = monthly.dropna(subset=["take_home_ttm"])
    if data.empty:
        return make_empty_figure(
            "Needs 12 complete months of data.", title="Take-home allocation"
        )
    months = pd.to_datetime(data["year_month"] + "-01")
    series = (
        ("Fixed costs", data["fixed_share_ttm"]),
        ("Guilt-free", data["guilt_free_share_ttm"]),
        ("Savings goals", data["savings_share_ttm"]),
        ("Investments", data["investments_share_ttm"]),
        ("One-off", data["one_off_share_ttm"]),
        ("Unallocated", data["unallocated_share_ttm"]),
    )
    colors = (*_SERIES_COLORS, "#008300")
    fig = go.Figure()
    for (name, values), color in zip(series, colors):
        fig.add_trace(
            go.Scatter(
                x=months, y=values, name=name, mode="lines",
                line=dict(color=color, width=2),
                hovertemplate=f"{name}: %{{y:.1%}}<extra></extra>",
            )
        )
    fig.add_hline(y=0, line_color="rgba(0,0,0,0.25)", line_width=1)
    fig.update_layout(title="Where take-home pay went — trailing 12-month share by bucket")
    fig = apply_standard_layout(fig, height=420)
    fig.update_yaxes(tickformat=".0%")
    return fig


def plot_balances_by_group(balances: pd.DataFrame) -> go.Figure:
    """Stacked area of month-end balances (PLN) by account group."""
    if balances is None or balances.empty:
        return make_empty_figure(
            "Account balances need the .mmb input.", title="Balances by account group"
        )
    groups = (
        ("cash", "Cash (checking & wallets)"),
        ("savings", "Savings accounts"),
        ("investments", "Investments"),
        ("receivables", "Owed to you"),
        ("other", "Other"),
    )
    pivot = balances.pivot_table(
        index="year_month", columns="group", values="balance_pln", aggfunc="sum"
    ).fillna(0.0)
    months = pd.to_datetime(pivot.index + "-01")
    fig = go.Figure()
    for (key, label), color in zip(groups, _SERIES_COLORS):
        values = pivot[key] if key in pivot.columns else pd.Series(0.0, index=pivot.index)
        fig.add_trace(
            go.Scatter(
                x=months, y=values.values, name=label, mode="lines",
                stackgroup="balances", line=dict(color=color, width=1),
                hovertemplate=f"{label}: %{{y:,.0f}} PLN<extra></extra>",
            )
        )
    fig.update_layout(
        title="Month-end balances by account group (PLN; debt accounts in the scorecard)",
        yaxis_title="PLN",
    )
    return apply_standard_layout(fig, height=420)


def plot_lifestyle_creep(annual: pd.DataFrame) -> go.Figure:
    """Grouped bars of annual take-home vs living spend (fixed + guilt-free)."""
    if annual is None or annual.empty:
        return make_empty_figure(
            "Needs at least one complete calendar year.", title="Lifestyle creep"
        )
    years = annual["year"].astype(str)
    custom = annual[
        ["living_share", "marginal_spend_rate", "delta_take_home", "delta_living_spend"]
    ].to_numpy()
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=years, y=annual["take_home"], name="Take-home",
            marker_color=_SERIES_COLORS[0],
            hovertemplate="Take-home: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=years, y=annual["living_spend"], name="Living spend (fixed + guilt-free)",
            marker_color=_SERIES_COLORS[1], customdata=custom,
            hovertemplate=(
                "Living spend: %{y:,.0f} PLN<br>Share of take-home: %{customdata[0]:.0%}"
                "<br>Δ take-home: %{customdata[2]:+,.0f} PLN"
                "<br>Δ living spend: %{customdata[3]:+,.0f} PLN"
                "<br>Marginal spend rate: %{customdata[1]:.2f}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Take-home vs living spend per calendar year",
        barmode="group", barcornerradius=4, bargap=0.25, bargroupgap=0.08,
        yaxis_title="PLN",
    )
    return apply_standard_layout(fig, height=400)


def plot_reserve_vs_ceiling(trend: pd.DataFrame) -> go.Figure:
    """Two lines: the savings reserve and the emergency-fund ceiling, in PLN."""
    if trend is None or trend.empty or "savings_balance" not in trend.columns:
        return make_empty_figure(
            "Needs account balances and 12 complete months.",
            title="Reserve vs emergency-fund ceiling",
        )
    months = pd.to_datetime(trend["year_month"] + "-01")
    n_months = len(trend)
    last_month = trend["year_month"].iloc[-1]
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=months,
            y=trend["savings_balance"],
            mode="lines",
            name="Reserve (savings accounts)",
            line=dict(color=_SERIES_COLORS[0], width=2),
            hovertemplate="Reserve: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=months,
            y=trend["ceiling"],
            mode="lines",
            name="6-month ceiling",
            line=dict(color=_SERIES_COLORS[1], width=2, dash="dash"),
            hovertemplate="Ceiling: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.update_layout(
        title=(
            "Reserve vs emergency-fund ceiling — "
            f"{n_months} months to {last_month}"
        ),
        yaxis_title="PLN",
    )
    return apply_standard_layout(fig, height=380)


# ── Money Flow charts ────────────────────────────────────────────────────────


def _flow_value(values: dict[str, float], key: str) -> float:
    """Read a flow value, treating missing and NaN as zero."""
    value = values.get(key)
    return 0.0 if value is None or pd.isna(value) else float(value)


def _short_pln(value: float) -> str:
    """Compact PLN label for a waterfall step: 316k, −171k, 1.2M."""
    sign = "−" if value < 0 else ""
    magnitude = abs(value)
    if magnitude >= 1_000_000:
        return f"{sign}{magnitude / 1_000_000:,.1f}M"
    if magnitude >= 1_000:
        return f"{sign}{magnitude / 1_000:,.0f}k"
    return f"{sign}{magnitude:,.0f}"


def plot_take_home_waterfall(values: dict[str, float], title: str) -> go.Figure:
    """
    Waterfall from gross income to unallocated take-home.

    Parameters
    ----------
    values : dict[str, float]
        Keys: gross_income, deductions, take_home, fixed, guilt_free, one_off,
        unmapped, savings, investments, unallocated. A negative savings value
        is a drawdown and is labelled "From savings".
    title : str
        Figure title, including the number of months it covers (HR-8).
    """
    if not values or pd.isna(values.get("gross_income")):
        return make_empty_figure(
            "Needs 12 complete months for the TTM view, or pick a single month.",
            title=title,
        )

    savings = _flow_value(values, "savings")
    savings_label = "From savings" if savings < 0 else "To savings"
    one_off_unmapped = _flow_value(values, "one_off") + _flow_value(values, "unmapped")
    labels = [
        "Gross income",
        "Taxes<br>(VAT, PIT, ZUS)",
        "Take-home",
        "Fixed",
        "Guilt-free",
        "One-off &<br>unmapped",
        savings_label,
        "To investments",
        "Unallocated",
    ]
    steps = [
        _flow_value(values, "gross_income"),
        -_flow_value(values, "deductions"),
        _flow_value(values, "take_home"),
        -_flow_value(values, "fixed"),
        -_flow_value(values, "guilt_free"),
        -one_off_unmapped,
        -savings,
        -_flow_value(values, "investments"),
        _flow_value(values, "unallocated"),
    ]
    y = [
        _flow_value(values, "gross_income"),
        -_flow_value(values, "deductions"),
        0.0,
        -_flow_value(values, "fixed"),
        -_flow_value(values, "guilt_free"),
        -one_off_unmapped,
        -savings,
        -_flow_value(values, "investments"),
        0.0,
    ]
    measure = [
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

    fig = go.Figure(
        go.Waterfall(
            x=labels,
            y=y,
            measure=measure,
            text=[_short_pln(value) for value in steps],
            texttemplate="%{text}",
            textposition="auto",
            connector=dict(line=dict(color="rgba(0,0,0,0.25)", width=1)),
            increasing=dict(marker=dict(color=_SERIES_COLORS[2])),
            decreasing=dict(marker=dict(color=_SERIES_COLORS[1])),
            totals=dict(marker=dict(color=_SERIES_COLORS[0])),
            hovertemplate="%{x}: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.update_layout(title=title, yaxis_title="PLN")
    fig.update_xaxes(tickangle=0)
    return apply_standard_layout(fig, height=440, showlegend=False)


def plot_reserve_transfers(
    transfers: pd.DataFrame, monthly: pd.DataFrame, months: int = 24
) -> go.Figure:
    """Grouped bars of reserve → cash transfers and the spending they fund."""
    if transfers is None or transfers.empty or monthly is None or monthly.empty:
        return make_empty_figure(
            "Reserve transfers need the .mmb input.", title="Reserve transfers"
        )
    complete = transfers[~transfers["is_partial_month"].astype(bool)].tail(months)
    if complete.empty:
        return make_empty_figure(
            "No complete months of reserve transfers.", title="Reserve transfers"
        )

    spend_cols = ["deductions", "fixed", "guilt_free", "one_off", "unmapped"]
    merged = complete.merge(
        monthly[["year_month", *spend_cols]], on="year_month", how="left"
    )
    spent = merged[spend_cols].sum(axis=1)
    x = pd.to_datetime(merged["year_month"] + "-01")
    n_months = len(merged)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=x,
            y=merged["moved_to_cash"],
            name="Moved from reserve to spending accounts",
            marker_color=_SERIES_COLORS[0],
            hovertemplate="Moved: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=x,
            y=spent,
            name="Spent + taxes paid",
            marker_color=_SERIES_COLORS[1],
            hovertemplate="Spent + taxes: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Reserve transfers vs spending — last {n_months} complete months",
        barmode="group",
        barcornerradius=4,
        bargap=0.25,
        bargroupgap=0.08,
        yaxis_title="PLN",
    )
    return apply_standard_layout(fig, height=400)


def plot_big_wins(scenario: pd.DataFrame) -> go.Figure:
    """
    Scenario lines at the user's assumed rates, with the low/high band.

    Not a forecast (HR-1): the rates are the user's inputs (HR-3). Missing
    ``invested_*`` columns draw no line, leaving the reserve line only.
    """
    if scenario is None or scenario.empty or "kept_in_reserve" not in scenario.columns:
        return make_empty_figure(
            "Enter a horizon to build the scenario.", title="Big Wins"
        )
    years = scenario["month"] / 12.0
    band_color = "rgba(27,175,122,0.18)"
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=years,
            y=scenario["kept_in_reserve"],
            mode="lines",
            name="Kept in reserve",
            line=dict(color=_SERIES_COLORS[0], width=2),
            hovertemplate="Kept in reserve: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=years,
            y=scenario["invested_low"],
            mode="lines",
            name="Invested at your low rate",
            line=dict(color=_SERIES_COLORS[2], width=2, dash="dash"),
            hovertemplate="Low rate: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=years,
            y=scenario["invested_high"],
            mode="lines",
            name="Invested at your high rate",
            line=dict(color=_SERIES_COLORS[2], width=2),
            fill="tonexty",
            fillcolor=band_color,
            hovertemplate="High rate: %{y:,.0f} PLN<extra></extra>",
        )
    )
    fig.update_layout(
        title="Scenario at your assumed rates, not a forecast",
        xaxis_title="Years",
        yaxis_title="PLN",
    )
    fig.update_xaxes(tickformat=".0f")
    return apply_standard_layout(fig, height=420)
