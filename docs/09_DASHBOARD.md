# Interactive Dashboard (Dash)

## Overview

The dashboard is a local web application that provides interactive exploration of
pipeline outputs and live what-if scenario modelling. It is a **presentation layer**
over the pipeline outputs.

### Naming note

**Dash** (by Plotly, `pip install dash`) is the correct library. It is unrelated to
**Dask** (a parallel computing library for large datasets). They have similar names
but entirely different purposes — Dask is not used in this pipeline.

---

## Architecture: Presentation Layer, Not Computation Layer

The Dash app reads pre-computed Parquet files from `data/interim/` and
`data/processed/`. It does **not** compute them: `scripts/run_pipeline.py` is the
computation layer, and `just dashboard` rebuilds every output before starting
the app. The Dash app is the exploration and what-if layer.

```
scripts/run_pipeline.py       ← clears and rebuilds every output
        │
        ▼ writes
data/interim/*.parquet
data/processed/*.parquet
        │
        ▼ reads (read-only)
dashboard/app.py              ← Dash server (localhost:8050)
        │
        ├── Tab: Budget Health      (landing; budget_health_monthly + account_balances)
        ├── Tab: Money Flow         (take-home waterfall, reserve_transfers, adherence)
        ├── Tab: Big Wins           (sub-invest: scenario; sub-cut: what-if)
        ├── Tab: Spending           (sub-breakdown, sub-drilldown, sub-trends,
        │                            sub-cat-trends, sub-cat-forecast)
        ├── Tab: Recurring Costs    (commitment register, calendar, payee chart)
        ├── Tab: Runway & Forecast  (sub-savings, sub-cashflow, sub-mc)
        └── Tab: Data Quality
```

**Sequencing constraint:** most panels read the processed/interim Parquet files, so
`just run` must have produced them first (`just dashboard` does this itself). Monte Carlo and the
cut-cost sliders call `src/analysis` functions directly and only need the interim
DataFrames. The app detects missing Parquet files and displays a clear user-facing
message rather than raising an unhandled exception.

---

## Installation

Add to `pyproject.toml` as an optional dependency group:

```toml
[project.optional-dependencies]
dashboard = [
    "dash>=2.14",
    "dash-bootstrap-components>=1.5",
    "openai>=3.40",
    "duckdb>=2.0",
]
```

Install: `pip install -e ".[dashboard]"`. With Poetry, the same packages live in
the `dashboard` group: `poetry install --with dashboard`.

---

## File Structure

```
mmex-forecast/
└── dashboard/
    ├── app.py          # Dash server entry point
    ├── layout.py       # Component tree (tabs, controls, chat drawer)
    ├── callbacks.py    # All @app.callback definitions for tabs
    ├── chat.py         # AI Advisor drawer + its callbacks
    └── data_loader.py  # Parquet loading with missing-file guards
```

The AI Advisor also uses `src/agent/` (`sql_sandbox.py`, `skill.py`,
`advisor.py`), which is backend code and has no Dash dependency.

---

## `dashboard/data_loader.py`

```python
"""
Load pre-computed Parquet files for the Dash app.
Returns None with a log warning if the file does not exist,
so callbacks can show a user-facing message rather than crashing.
"""
import pandas as pd
from pathlib import Path
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_parquet(stage: str, name: str) -> pd.DataFrame | None:
    """
    Load data/{stage}/{name}.parquet relative to project root.

    Returns None if the file is absent (pipeline not yet run for this stage).
    """
    path = PROJECT_ROOT / "data" / stage / f"{name}.parquet"
    if not path.exists():
        logger.warning(f"Parquet not found: {path}. Run `just run` first.")
        return None
    df = pd.read_parquet(path)
    logger.info(f"Loaded {len(df)} rows from {path}")
    return df
```

---

## `dashboard/app.py`

```python
import dash
import dash_bootstrap_components as dbc
from dashboard.layout import build_layout

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="MMEX Finance Dashboard",
)
app.layout = build_layout()

# Import callbacks (side-effect registration)
import dashboard.callbacks  # noqa: F401, E402

if __name__ == "__main__":
    app.run(debug=True, port=8050)
```

---

## `dashboard/layout.py`

```python
import dash_bootstrap_components as dbc
from dash import dcc, html


def build_layout() -> dbc.Container:
    return dbc.Container(
        fluid=True,
        children=[
            dbc.NavbarSimple(brand="MMEX Personal Finance", color="primary", dark=True),
            dbc.Tabs([
                dbc.Tab(label="Budget Health",     tab_id="tab-health"),
                dbc.Tab(label="Money Flow",        tab_id="tab-flow"),
                dbc.Tab(label="Big Wins",          tab_id="tab-wins"),
                dbc.Tab(label="Spending",          tab_id="tab-spending"),
                dbc.Tab(label="Recurring Costs",   tab_id="tab-recurring"),
                dbc.Tab(label="Runway & Forecast", tab_id="tab-runway"),
                dbc.Tab(label="Data Quality",      tab_id="tab-quality"),
            ], id="tabs", active_tab="tab-health"),
            html.Div(id="tab-content", className="mt-3"),
        ],
    )
```

---

## `dashboard/callbacks.py` (abridged — key patterns)

```python
import pandas as pd
import numpy as np
from dash import Input, Output, State, callback, html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from dashboard.data_loader import load_parquet
from src.analysis.cashflow_forecast import (
    run_expenditure_mc,
    compute_expense_reduction_scenario,
    months_to_savings_target,
)
from src.analysis.expense_trends import fit_category_trend


# ── Tab routing ──────────────────────────────────────────────────────────────

@callback(Output("tab-content", "children"), Input("tabs", "active_tab"))
def render_tab(active_tab: str) -> html.Div:
    dispatch = {
        "tab-health":    _budget_health_panel,
        "tab-flow":      _money_flow_panel,
        "tab-wins":      _big_wins_panel,
        "tab-spending":  _spending_panel,
        "tab-recurring": _recurring_panel,
        "tab-runway":    _runway_panel,
        "tab-quality":   _quality_panel,
    }
    fn = dispatch.get(active_tab)
    return fn() if fn else html.Div("Unknown tab")


# ── Tab 3: Monte Carlo ───────────────────────────────────────────────────────

def _mc_panel() -> html.Div:
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Label("Simulations"),
                dcc.Slider(
                    id="mc-n-sims", min=500, max=5000, step=500, value=2000,
                    marks={500: "500", 2000: "2 000", 5000: "5 000"},
                ),
                html.Label("Horizon (months)", className="mt-3"),
                dcc.Slider(id="mc-horizon", min=3, max=24, step=1, value=12),
                dbc.Checklist(
                    options=[{"label": "Detrend categories", "value": "detrend"}],
                    value=["detrend"], id="mc-detrend", className="mt-3",
                ),
                dbc.Button("Run simulation", id="mc-run-btn", color="primary",
                           className="mt-3"),
            ], width=3),
            dbc.Col([
                dcc.Loading(dcc.Graph(id="mc-fan-chart")),
                html.Div(id="mc-prob-table", className="mt-3"),
            ], width=9),
        ]),
    ])


@callback(
    Output("mc-fan-chart", "figure"),
    Output("mc-prob-table", "children"),
    Input("mc-run-btn", "n_clicks"),
    State("mc-n-sims", "value"),
    State("mc-horizon", "value"),
    State("mc-detrend", "value"),
    prevent_initial_call=True,
)
def run_mc(n_clicks, n_sims, horizon, detrend_val):
    monthly_cat = load_parquet("interim", "transactions_featured")
    monthly_income = load_parquet("interim", "monthly_aggregates")

    if monthly_cat is None or monthly_income is None:
        return go.Figure(), dbc.Alert(
            "Pipeline data not found. Run `just run` first.", color="warning"
        )

    # Derive category spend pivot and income series from stored aggregates.
    # Exact column names depend on feature engineering output — adjust as needed.
    cat_cols = [c for c in monthly_cat.columns if c.startswith("spend_")]
    cat_spend = monthly_cat.set_index("month")[cat_cols].rename(
        columns=lambda c: c.replace("spend_", "")
    )
    income_series = monthly_income.set_index("month")["income"]

    # current_balance must be provided by user — placeholder here.
    current_balance = 0.0  # TODO: wire to a user-input field

    result = run_expenditure_mc(
        monthly_category_spend=cat_spend,
        monthly_income=income_series,
        current_balance=current_balance,
        n_simulations=n_sims,
        horizon_months=horizon,
        detrend=("detrend" in (detrend_val or [])),
    )

    from dashboard.charts import plot_mc_fan
    fig = plot_mc_fan(result)

    prob_df = result["prob_negative"].reset_index()
    prob_df.columns = ["Month", "P(balance < 0)"]
    prob_df["P(balance < 0)"] = prob_df["P(balance < 0)"].map("{:.1%}".format)
    table = dbc.Table.from_dataframe(prob_df, striped=True, bordered=True, size="sm")

    return fig, table


# ── Tab 4: What-If Scenarios ─────────────────────────────────────────────────

def _whatif_panel() -> html.Div:
    """
    Dynamically builds category sliders from available expense data.
    Category type map (fixed/variable) is read from configs/categories.yaml.
    """
    monthly_cat = load_parquet("processed", "expense_trends")
    if monthly_cat is None:
        return dbc.Alert("Run `just run` first.", color="warning")

    categories = list(monthly_cat["category"].unique())

    sliders = []
    for cat in categories:
        sliders.append(html.Div([
            html.Label(cat, className="fw-semibold"),
            dcc.Slider(
                id={"type": "reduction-slider", "index": cat},
                min=0, max=100, step=5, value=0,
                marks={0: "0%", 25: "25%", 50: "50%", 75: "75%", 100: "100%"},
                tooltip={"placement": "bottom"},
            ),
        ], className="mb-3"))

    return html.Div([
        dbc.Row([
            dbc.Col([
                html.H5("Set reduction % per category"),
                html.Hr(),
                *sliders,
                html.Label("Horizon (months)", className="mt-2"),
                dcc.Slider(id="whatif-horizon", min=3, max=36, step=1, value=12),
                html.Label("Savings target", className="mt-3"),
                dbc.Input(id="whatif-target", type="number", placeholder="e.g. 5000",
                          debounce=True),
                dbc.Button("Calculate", id="whatif-run-btn", color="success",
                           className="mt-3"),
            ], width=3),
            dbc.Col([
                dcc.Loading(dcc.Graph(id="whatif-chart")),
                html.Div(id="whatif-summary", className="mt-3"),
            ], width=9),
        ]),
    ])


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
    import dash

    reductions = {
        sid["index"]: val / 100.0
        for sid, val in zip(slider_ids, slider_values)
        if val > 0
    }

    if not reductions:
        return go.Figure(), dbc.Alert("Set at least one reduction > 0%.", color="info")

    baseline = load_parquet("processed", "cashflow_forecast")
    monthly_cat = load_parquet("interim", "monthly_aggregates")

    if baseline is None or monthly_cat is None:
        return go.Figure(), dbc.Alert(
            "Pipeline data not found. Run `just run` first.", color="warning"
        )

    # Load category type map from configs/categories.yaml
    import yaml
    from pathlib import Path
    config_path = Path(__file__).parent.parent / "configs" / "categories.yaml"
    with open(config_path) as f:
        cat_config = yaml.safe_load(f)
    category_type_map = cat_config.get("category_types", {})

    cat_cols = [c for c in monthly_cat.columns if c.startswith("spend_")]
    cat_spend = monthly_cat.set_index("month")[cat_cols].rename(
        columns=lambda c: c.replace("spend_", "")
    )

    try:
        result = compute_expense_reduction_scenario(
            baseline_forecast=baseline,
            monthly_category_spend=cat_spend,
            category_type_map=category_type_map,
            reductions=reductions,
            horizon_months=horizon,
        )
    except ValueError as e:
        return go.Figure(), dbc.Alert(str(e), color="danger")

    from dashboard.charts import plot_reduction_scenario
    fig = plot_reduction_scenario(result)

    summary_lines = [
        html.H6("Scenario Summary"),
        html.P(
            f"Total monthly saving: {result.attrs['total_monthly_saving']:,.0f}  |  "
            f"Cumulative over {horizon} months: {result['delta_cumulative'].iloc[-1]:,.0f}"
        ),
    ]

    if target:
        tgt = months_to_savings_target(result, float(target))
        if "error" not in tgt:
            summary_lines.append(html.P(tgt["note"]))
        else:
            summary_lines.append(dbc.Alert(tgt["error"], color="warning"))

    if result.attrs.get("warnings"):
        for w in result.attrs["warnings"]:
            summary_lines.append(dbc.Alert(w, color="warning"))

    return fig, html.Div(summary_lines)
```

---

## Budget Health tab

Scores the last 12 complete months against Ramit Sethi's Conscious Spending
Plan (*I Will Teach You to Be Rich*, 2nd ed.). Computed by
`src/analysis/budget_health.py` in pipeline step 7c; the tab only reads
`data/processed/budget_health_monthly.parquet` and
`data/processed/account_balances.parquet`.

| Measure | Definition |
|---|---|
| Take-home | Income-category rows minus `take_home_deductions` (VAT, PIT, ZUS) |
| Fixed / guilt-free / one-off | Net spend by `budget_health.buckets` (category or `Category\|Subcategory`) |
| Savings / investments | Net inflow into accounts in those `account_groups` |
| Unallocated | Take-home left after all of the above |
| Emergency fund | Savings-group balance ÷ average monthly fixed costs |
| Liquid runway | Cash + savings balance ÷ average monthly fixed + guilt-free spend |
| Lifestyle creep | Per calendar year: Δ living spend ÷ Δ take-home; blank when take-home did not rise (the ratio is defined only for extra take-home) |

Balances are rebuilt from MMEX `INITIALBAL` plus every transaction leg, so they
need `--mmb` input; with `--csv` the savings/investment columns are NaN and the
balance views show an info message. The mappings in `configs/pipeline.yaml`
are drafts until `budget_health.mapping_confirmed: true` (HR-6); the tab shows
a warning until then.

**Next 100 PLN card.** Below the KPI rows, the tab orders the scorecard into
Sethi's priority list (rendered without product names, HR-5): high-interest debt,
employer match, emergency fund, invest ≥10% of take-home, then savings goals and
guilt-free money. Exactly one step is marked `← next step`: the first with status
`attention` or `insufficient data`. Step 2 is `not applicable` on a JDG (no
employer plan) unless `budget_health.employer_match` is true, in which case it is
`not measured` because MMEX does not show it.

**Reserve chart.** After the scorecard, `budget-reserve-chart` plots the
savings-group reserve against the emergency-fund ceiling
(`emergency_fund_months[1] × fixed_ttm / 12`) in PLN, with the number of months
in the title (HR-8). The ceiling is NaN until 12 complete months exist. The KPI
"Reserve above ceiling" comes from `Reserve above emergency-fund ceiling`.

---

## Money Flow tab

The automation side of the Conscious Spending Plan. Reads
`data/processed/budget_health_monthly.parquet` and, when present,
`data/processed/reserve_transfers.parquet`. With `--csv` input the transfer
section shows an info message; the waterfall still renders.

| Measure | Definition |
|---|---|
| Waterfall | Gross income → taxes (VAT, PIT, ZUS) → take-home → fixed → guilt-free → one-off & unmapped → savings → investments → unallocated |
| TTM option | The `*_ttm` columns of the latest complete month (12 months); individual months use their own columns |
| Reserve transfer | A transfer from a `savings`-group account to a `cash`-group account; amount = positive source leg |
| Moved to cash | Sum of reserve transfers in the month |
| Income into reserve | Income-category rows landing in savings-group accounts (invoice arrival) |
| Invoice arrival days | Distinct days of the month those income rows land on |
| Spent + taxes | Deductions + fixed + guilt-free + one-off + unmapped for the month |
| Regularity | Complete months, months with a transfer, mean/range/CV of the amount, distinct transfer and invoice days |
| Salary adherence | Share of months within ±10% of `plan.salary_pln` and, when set, ±3 days of `plan.salary_day` |
| Investment adherence | Share of months where `monthly.investments ≥ plan.investment_pln` |

Adherence is `None` (and the panel shows how to set `budget_health.plan`) when
the matching plan key is null. Nothing is inferred.

---

## Spending tab — trends and forecasts

Reads `data/processed/expense_trends.parquet`,
`category_trends_summary.parquet`, `category_trends_series.parquet` and
`category_forecasts.parquet`. The tab draws them; it does not test anything.

| Column | Meaning |
|---|---|
| `Trend` | `rising` / `falling` only where `trend_pvalue_adj < 0.10`; else `flat` (or `insufficient_data` under 6 months) |
| `Slope` | PLN per month from an OLS fit on a zero-filled continuous month grid |
| `p-value` | Newey-West (HAC) p-value: residual autocorrelation does not shrink the standard error |
| `q (BH)` | Benjamini-Hochberg adjusted p-value across categories, FDR 0.10 |
| `Slope ± 95% CI` chart | Rising categories only, with the HAC interval as the error bar |
| `Seasonal` | Kruskal–Wallis on detrended residuals, BH-adjusted; never the raw CV |
| `Direction` (category trends) | HAC OLS slope sign with `q (BH)`; `mixed` when the Prophet trend change has the opposite sign |

The month containing the run date is excluded from every fit and average; it
is still reported as an actual.

Forecast tab notes: `cashflow_forecast.parquet` carries `cum_ci80_lower` /
`cum_ci80_upper`, the cumulative quantiles computed per simulated path. The
savings fan uses those and never sums the monthly 80% columns. Its label says
either `80% band (N backtests)` when `interval_method = empirical_backtest_nN`
or `80% band (model, uncalibrated)` otherwise; with no cumulative columns at
all it draws the point line and a "re-run `just run`" note. The 95% bands
are blank: the backtest has too few origins to estimate 2.5/97.5 percentiles.
With backtest bands, a grey subtitle gives `backtest_cum_low_k` of
`backtest_cum_n` at the selected horizon ("the actual 24-month total came in
above the forecast in K of N") and says when the forecast line runs outside
its 80% band.

---

## Big Wins tab

Two sub-tabs. **Invest** answers "what moves the needle?" with the CEO method:
route new income to investments and let compounding work. **Cut costs** is the
old What-If scenario, restricted to categories mapped to `fixed` or `guilt_free`
so taxes, income, one-offs and unmapped rows cannot be cut.

Big Wins follows the hard rules:

- **HR-1:** it is a *scenario at your assumed rates, not a forecast*. The low and
  high annual rates are drawn as a band, and the title says so.
- **HR-3:** no default return, inflation rate or benchmark. Both rates start empty
  and the user types them; an empty rate leaves that line out and shows an info
  alert. The reserve rate defaults to 0.
- **HR-5:** no instrument, product or tax action is named. The skill's
  "Roth IRA / index fund" step renders as "Invest".

| Measure | Definition |
|---|---|
| Monthly rate | `(1 + r) ** (1/12) − 1` |
| Projection | `V_t = V_{t−1} × (1 + m) + C`, `V_0 = lump sum`; contributions at month end |
| Contributions | `lump_sum + (monthly + raise_monthly) × t` |
| Raise lever | `raise_pct / 100 × take_home_monthly`; deductions are assumed to scale with the invoice |
| Kept in reserve | Same contributions compounded at `plan`/reserve interest rate (empty = 0) |
| Rule of 72 | `72 / (annual rate × 100)` years; none for a rate ≤ 0 |

The monthly default is `round(0.10 × take_home_monthly, -2)` and is blank when
there is no TTM window. The lump-sum help text shows the reserve above the
emergency-fund ceiling.

---

## AI Advisor

The dashboard has an optional right-side **AI Advisor** drawer, opened with the
**💬 Advisor** button in the navbar. It is not a tab: the conversation survives
tab switches, so you can look at a chart while asking about it.

The advisor is backed by DeepSeek's OpenAI-compatible API (`deepseek-flash` by
default) and loads the
[`i-will-teach-you-to-be-rich`](../.agents/skills/i-will-teach-you-to-be-rich/SKILL.md)
skill as its operating method. It can query the same data the dashboard reads
through a read-only SQL tool. Tool calls appear as collapsible steps in the
transcript.

### Setup

1. Install the dashboard extras (includes `dash`, `dash-bootstrap-components`,
   `openai` and `duckdb`):

   ```bash
   poetry install --with dashboard
   ```

2. Put the secrets and paths in `.env` at the project root (already gitignored):

   ```dotenv
   DEEPSEEK_API_KEY=sk-...
   MMB_PATH=C:\path\to\finances.mmb

   # optional
   DEEPSEEK_MODEL=deepseek-flash
   DEEPSEEK_BASE_URL=https://api.deepseek.com
   ```

   `DEEPSEEK_API_KEY` is required. Without it the drawer shows a
   "Set DEEPSEEK_API_KEY in .env" alert and the rest of the dashboard works
   normally. `MMB_PATH` is used to load the raw MMEX tables; without it the
   advisor still sees the Parquet tables.

3. Optional tuning in [`configs/pipeline.yaml`](../configs/pipeline.yaml):

   ```yaml
   chat:
     model: deepseek-flash         # env DEEPSEEK_MODEL overrides
     thinking: false               # true is not supported yet (raises at startup)
     temperature: 0.3
     max_tool_iterations: 8
     max_rows: 200                 # rows returned to the model per query
     max_result_chars: 12000       # hard cap on serialised tool output
     history_turns: 20             # user/assistant turns kept in context
     request_timeout_s: 90
     skill_dir: .agents/skills/i-will-teach-you-to-be-rich
     include_mmex_tables: true
   ```

   Thinking mode is deliberately disabled. DeepSeek's thinking mode with tools
   requires every prior `reasoning_content` value to be sent back; setting
   `thinking: true` raises a clear startup error instead of failing mid-chat.

### What data the advisor can see

At startup (and whenever a source file changes), every Parquet file in
`data/interim/` and `data/processed/` is loaded into an in-memory DuckDB
database. Table names are the file stems, for example:

- `transactions_featured` — the main fact table
- `monthly_aggregates`, `expense_trends`
- `monthly_cashflow_split`
- `cashflow_forecast`, `forecast_backtest`, `category_forecasts`
- `category_trends_summary`, `category_trends_series`
- `recurring_commitments`, `recurring_commitment_events`, `recurring_categories`

If `include_mmex_tables` is true and `MMB_PATH` exists, the raw MMEX SQLite
tables are copied too, prefixed with `mmex_` and lowercased:

- `mmex_accountlist_v1`, `mmex_assets_v1`, `mmex_stock_v1`
- `mmex_budgettable_v1`, `mmex_budgetyear_v1`, `mmex_billsdeposits_v1`
- `mmex_currencyformats_v1`, `mmex_currencyhistory_v1`
- and the rest of the MMEX schema except `ATTACHMENT_V1`

The system prompt includes a live catalog: table names, row counts, column
types, and the actual values of short low-cardinality text columns (for
example `type VARCHAR ∈ {deposit, transfer, withdrawal}`). This lets the model
avoid guessing enum values. Amounts in `mmex_*` tables are in each account's
own currency, not PLN; the pipeline tables are the PLN reporting layer.

### Read-only guarantees

- All data is copied into **in-memory** DuckDB tables. The connection then runs
  `SET enable_external_access = false` and `SET lock_configuration = true`.
- The SQL tool accepts exactly **one** statement, and only
  `SELECT`/`WITH`/`DESCRIBE`/`SUMMARIZE`/`SHOW`. `INSERT`, `UPDATE`, `DELETE`,
  `CREATE`, `DROP`, `COPY`, `ATTACH`, `PRAGMA`, `CALL`, `EXPLAIN` and
  multi-statement input are rejected.
- The `.mmb` file is opened **read-only** (`?mode=ro` on a `Path.as_uri()` URI),
  so it is safe while MMEX or Google Drive has the file open. The non-ASCII
  paths (for example Polish folder names) are handled by `as_uri()`.
- File reads outside the loaded tables (for example `read_csv('...')`) fail
  because external access is disabled. The advisor cannot change dashboard
  data or write files.
- Query results are capped by `max_rows` and `max_result_chars`; large results
  are truncated and the model is told to aggregate in SQL.

### Privacy

The advisor sends your question, the table catalog and the **query results**
(which contain real financial data) to DeepSeek's API. Keep that in mind when
asking about sensitive accounts. The browser stores the transcript and LLM
history in `sessionStorage`; they disappear when the browser session ends.
Nothing is written back to MMEX or the Parquet files.

### Failure behaviour

- **No API key**: the drawer shows an alert; the other tabs work as before.
- **Pipeline not run / no Parquet files**: the advisor sees only `mmex_*`
  tables, or no tables at all, and tells you to run `just run`.
- **`.mmb` missing, locked or unreadable**: the advisor logs a warning and
  continues with Parquet only.
- **DeepSeek error or timeout**: the transcript shows a friendly error. Failed
  turns are not added to the LLM history, so you can simply retry.
- **Tool budget exhausted**: the final call is made without tools; if the model
  still returns tool markup instead of prose, the turn becomes a fallback
  message and is not stored.

### Localisation

The skill is US-centric (401k, Roth IRA, HSA, USD). The system prompt tells the
advisor to apply the underlying principle and name the Polish counterpart
(PPK, IKE, IKZE, Treasury retail bonds, etc.). It must flag that contribution
limits, tax rules and product availability have to be checked against current
Polish regulations.

---

## Directory Setup

Add the following to `02_DIRECTORY_SETUP.md` under the project tree:

```
mmex-forecast/
└── dashboard/
    ├── app.py
    ├── layout.py
    ├── callbacks.py
    └── data_loader.py
```

Also add `dashboard/` to `.gitignore` exclusions for any auto-generated Dash cache
files (`.dash_cache/`).
