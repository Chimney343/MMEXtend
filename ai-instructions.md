# AI Agent Instructions — MMEX Personal Finance Pipeline

> **This file is the single source of truth for AI coding agents.**
> Both GitHub Copilot and Roo read this file. It lives at `.ai-instructions.md`
> in the project root. The agent-specific config files (`.github/copilot-instructions.md`
> and `.roo/rules.md`) import this file by reference.

---

## Project Context

You are working on a modular personal finance analysis pipeline that ingests data
from Money Manager EX (MMEX), validates it, engineers financial features, and produces
statistically grounded forecasts with confidence intervals. Output is delivered via
JupyterLab notebooks.

**Read these docs before writing code in a module:**

| Module | Read First |
|--------|-----------|
| `src/ingestion/` | `docs/04_INGESTION.md` |
| `src/storage/` | `docs/05_STORAGE_GCP.md`, `docs/06_STORAGE_LOCAL.md` |
| `src/analysis/` | `docs/08_FINANCE_STATISTICS.md` |
| `src/utils/` | `docs/03_LOGGING.md` |
| `notebooks/` | `docs/07_NOTEBOOK_INTERVIEW.md` |
| Any module | `docs/01_PROJECT_OVERVIEW.md`, `docs/02_DIRECTORY_SETUP.md` |

---

## Hard Rules (NEVER Violate)

These rules are absolute. If a rule conflicts with a user request, follow the rule
and explain the conflict.

### HR-1: No Point Forecast Without Confidence Interval
Every forecast MUST include a confidence interval. State the CI method
(e.g. "Prophet 80% uncertainty interval", "bootstrapped residuals from Holt-Winters",
"OLS 95% CI for slope").
If a CI cannot be computed (e.g. < 12 months of data for cash flow), state explicitly:
"Insufficient history for reliable interval estimation. Forecast is indicative only."
NEVER output a bare point estimate.

### HR-2: No Financial Conclusion Without Listed Assumptions
Every advisory output, trend classification, or forecast MUST be preceded by
an explicit list of assumptions it depends on. Format:
```
Assumptions:
1. Income sources remain stable
2. No structural breaks in spending pattern
3. ...
```

### HR-3: No Fabricated Statistics
NEVER invent benchmarks, norms, percentiles, or reference figures.
If data is insufficient, output `[insufficient data]` and state the minimum
sample required. Example: "Trend analysis requires >= 6 months. Current data
has 4 months. [insufficient data]"

### HR-4: No Silent Imputation
NEVER fill missing values without logging. Every imputation MUST produce:
```
logger.warning(f"IMPUTATION | column={col} | method={method} | rows_affected={n} | detail={desc}")
```
State: what was missing, which method was used, how many rows were affected.

### HR-5: No Financial Recommendations
NEVER recommend a specific financial product, investment instrument, or tax action.
Confine output to: descriptive statistics, forecasts, trend signals.
If asked, respond: "This pipeline produces analysis and signals only.
Product/tax recommendations are outside scope."

### HR-6: No Trusting MMEX Categories
NEVER treat MMEX category names as canonical. Always surface uncategorised
or ambiguous transactions. Ask the user to confirm category mappings before
including them in analysis.

### HR-7: No Unsolicited CSV Output
NEVER write CSV files without explicit user instruction. When instructed:
1. Confirm the output path before writing
2. Use date-suffixed filename: `{type}_{YYYY-MM-DD}.csv`
3. NEVER overwrite existing files
4. Log the absolute path and row count

### HR-8: No Trend Analysis on < 6 Months
NEVER state a trend conclusion (rising/falling/flat) with fewer than 6 monthly
observations. State sample size (n months) next to every trend result.

---

## Environment

**The project uses [Poetry](https://python-poetry.org/) for all dependency and
virtualenv management.** This is the single source of truth — do not use pip,
conda, or any other tool to install packages inside this project.

| Task | Command |
|------|---------|
| Install all dependencies | `poetry install` |
| Install incl. forecast extras | `poetry install --with forecast` |
| Add a runtime dependency | `poetry add <pkg>` |
| Add a dev-only dependency | `poetry add --group dev <pkg>` |
| Run a command in the environment | `poetry run <cmd>` |
| Activate the virtualenv shell | `poetry shell` |
| Update lock file | `poetry lock` |

- `poetry.lock` is committed to version control. Always keep it in sync after
  adding or removing dependencies.
- NEVER call `pip install` directly inside this project.
- NEVER generate `requirements.txt` — `pyproject.toml` + `poetry.lock` are canonical.

---

## Coding Standards

### Python Style
- Python >= 3.11
- Type hints on all function signatures
- Docstrings: Google style (Parameters, Returns, Raises sections)
- Max line length: 100 characters
- Imports: stdlib → third-party → local, separated by blank lines
- No wildcard imports (`from x import *`)

### Data Conventions
- All DataFrames use lowercase `snake_case` column names
- Date columns are `datetime64[ns]` dtype, named `date` or `month`
- Amount columns are `float64`, named `amount` or descriptive (e.g. `income`, `expenditure`)
- Sign convention: income positive, expenditure negative
- Currency amounts are in the user's base currency (single-currency after validation)
- Intermediate DataFrames saved as `.parquet` to `data/interim/` or `data/processed/`

### Logging
- Every module gets a logger: `logger = setup_logging(__name__)`
- Use the logging levels defined in `docs/03_LOGGING.md`
- Every imputation → WARNING with the IMPUTATION template
- Every file write → INFO with absolute path and row count
- Every pipeline halt → ERROR with reason

### Error Handling
- Validation failures → raise `ValueError` with descriptive message
- File conflicts → raise `FileExistsError` (never overwrite)
- Missing user input → log WARNING, return a sentinel or partial result, do NOT guess

### Testing
- Tests live in `tests/` mirroring `src/` structure
- Use pytest fixtures in `conftest.py` for sample DataFrames
- Every function that implements a hard rule MUST have a test proving enforcement
- Test with: edge cases (empty DataFrame, 1 row, all nulls), normal cases, boundary conditions (exactly 6 months, exactly 12 months)

---

## Module-by-Module Instructions

### `src/ingestion/`

**Purpose:** Parse MMEX exports (CSV or SQLite) into a validated DataFrame.

**Files:**
- `mmex_csv_parser.py` — CSV parsing with encoding/separator detection
- `mmex_sqlite_parser.py` — SQLite (.mmb) query and extraction
- `validator.py` — Data quality checks, produces `DataQualityReport`
- `schemas.py` — Pydantic models for row-level validation

**Key rules:**
- Detect encoding with `chardet`
- Try multiple date formats before falling back to inference
- Normalise column names to canonical lowercase snake_case
- Normalise amount signs: income positive, expenditure negative
- Halt if null rate in `amount` or `date` exceeds 2%
- Report uncategorised transactions — do not silently drop them
- Flag date gaps > 14 days

**Output:** `data/interim/transactions_validated.parquet`

---

### `src/storage/`

**Purpose:** Write DataFrames to local filesystem and optionally GCP.

**Files:**
- `local_writer.py` — Parquet for intermediates, date-suffixed CSV for exports
- `gcp_writer.py` — GCS parquet upload + BigQuery table load

**Key rules:**
- Parquet for all pipeline-internal I/O
- CSV only on explicit user instruction, never automatic
- Never overwrite: date suffix on CSV filenames
- Log absolute path + row count on every write
- GCP is gated by `configs/pipeline.yaml → gcp.enabled`

---

### `src/analysis/`

**Purpose:** Feature engineering, trend detection, forecasting.

**Files:**
- `feature_engineering.py` — Monthly aggregates, rolling averages, savings rate, expense features
- `expense_trends.py` — OLS trend per category, seasonality check, MoM delta, consecutive rise flags
- `cashflow_forecast.py` — Method selection (Prophet / Holt-Winters / rolling mean), runway calculation
- `networth_trajectory.py` — Net worth trend, projection, savings vs market decomposition
- `advisory.py` — Structured summary generation

**Key rules:**
- Method selection is automatic based on data length (see `docs/08_FINANCE_STATISTICS.md`)
- Every trend result includes: slope, R², p-value, n_months, trend_class, CI for slope
- Every forecast includes: point estimate + CI columns (NaN if not computable, with explanation)
- Seasonality check flags CV > 0.4
- Fixed/variable classification requires user confirmation before use
- Opening balance required for cumulative cash balance — ask if not available
- NEVER project investment returns using assumed market rates

---

### `src/utils/`

**Purpose:** Shared infrastructure.

**Files:**
- `logging_config.py` — Central logger setup (console + file)
- `config_loader.py` — Load and validate `configs/pipeline.yaml`
- `plot_helpers.py` — Standardised plotting functions

**Plot helper requirements:**
- Every plot: title, axis labels with units, data source annotation, date range note
- Static categorical plots → seaborn
- Time-series with CI bands → plotly
- Correlation matrices → seaborn heatmap
- Never produce bare `plt.show()` without assigning figure to variable

---

### `notebooks/`

**Purpose:** Orchestration and user-facing output.

**Structure:**
- `00_data_quality.ipynb` — Ingestion + validation + quality report
- `01_wrangle.ipynb` — Feature engineering + save intermediates
- `02_expense_trends.ipynb` — Category trends, seasonality, MoM analysis
- `03_cashflow_forecast.ipynb` — Cash flow forecast + runway
- `04_networth_trajectory.ipynb` — Net worth analysis
- `05_advisory_summary.ipynb` — Structured summary

**Key rules:**
- Each notebook is independently re-runnable given prior notebook outputs
- First cell: `sys.path.insert(0, "..")` + logger setup
- Use `display(df)` not `print(df)`
- Assign all figures and DataFrames to named variables
- Markdown header above every code cell
- No hidden state between cells

---

## Config Schema: `configs/pipeline.yaml`

```yaml
# Input
input:
  path: "data/raw/mmex_export.csv"    # or .mmb
  type: "csv"                          # or "sqlite"
  base_currency: "PLN"                 # single currency; halt if mismatch

# Feature engineering
features:
  rolling_windows: [3, 6]              # months for rolling averages
  mom_rise_threshold: 0.20             # 20% MoM increase flag
  mom_consecutive_months: 2            # consecutive months to trigger flag
  cv_seasonality_threshold: 0.40       # CV above this = seasonal

# Forecasting
forecast:
  horizon_months: 12
  prophet_changepoint_prior: 0.05
  hw_bootstrap_draws: 500
  opening_balance: null                # user provides this; null = ask

# Categories
categories:
  fixed_variable_confirmed: false      # set to true after user confirms
  uncategorised_action: "flag"         # "flag" or "exclude"

# Storage
local:
  interim_dir: "data/interim"
  processed_dir: "data/processed"
  export_dir: "outputs"

gcp:
  enabled: false
  project_id: ""
  gcs:
    bucket: ""
    prefix: "processed"
  bigquery:
    dataset: "personal_finance"
    location: "EU"
    write_disposition: "WRITE_TRUNCATE"
```

---

## Dependency Versions

Pin these in `pyproject.toml`:

```toml
[project]
requires-python = ">=3.11"
dependencies = [
    "pandas>=2.0",
    "pyarrow>=14.0",
    "pydantic>=2.0",
    "statsmodels>=0.14",
    "prophet>=1.1",
    "scikit-learn>=1.3",
    "matplotlib>=3.8",
    "seaborn>=0.13",
    "plotly>=5.18",
    "pyyaml>=6.0",
    "chardet>=5.0",
    "jupyterlab>=4.0",
]

[project.optional-dependencies]
gcp = [
    "google-cloud-storage>=2.10",
    "google-cloud-bigquery>=3.12",
]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
]
```

---

## Quick Reference: Common Mistakes to Catch

| Mistake | Rule Violated | Correct Action |
|---------|---------------|----------------|
| Outputting a forecast with no CI | HR-1 | Add CI columns; if not computable, add NaN + note |
| Writing "Rent is trending up" with no assumptions | HR-2 | Prepend assumptions block |
| Saying "average savings rate is 20%" without source | HR-3 | Use `[insufficient data]` or cite the user's own data |
| Filling blank categories with "Other" without logging | HR-4 | Log IMPUTATION warning |
| Suggesting "consider index funds" | HR-5 | Remove; out of scope |
| Using MMEX "Food" category without confirmation | HR-6 | Surface and ask user |
| Auto-exporting a CSV at end of notebook | HR-7 | Remove; only export on request |
| Claiming "Housing costs are flat" from 4 months | HR-8 | State insufficient data |
