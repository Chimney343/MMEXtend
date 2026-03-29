# MMEX Personal Finance Forecasting Pipeline

## Project Overview

This project is a modular, reproducible personal finance analysis pipeline that ingests data from **Money Manager EX (MMEX)**, stores it locally and optionally in GCP, and produces statistically grounded forecasts and trend signals — all delivered through JupyterLab notebooks.

### What This Project Does

| Domain | Output |
|--------|--------|
| Expense categorisation & trends | OLS trend lines, seasonality flags, MoM deltas per category |
| Cash flow & runway modelling | 12-month net cash flow forecast with 80%/95% CIs, runway at pessimistic/median/optimistic scenarios |
| Net worth & investment trajectory | Net worth trend, forward projection, savings vs market decomposition |

### What This Project Does NOT Do

- Recommend specific financial products, instruments, or tax actions
- Fabricate benchmarks or statistical norms
- Apply assumed market return rates
- Produce point forecasts without confidence intervals

---

## Architecture

```
mmex-forecast/
├── configs/              # YAML config files (paths, params, feature flags)
│   ├── pipeline.yaml     # Master pipeline config
│   └── categories.yaml   # User-confirmed fixed/variable category mapping
├── data/
│   ├── raw/              # Untouched MMEX exports (.csv, .mmb SQLite)
│   ├── interim/          # Intermediate wrangled DataFrames (.parquet)
│   └── processed/        # Analysis-ready monthly aggregates (.parquet)
├── docs/                 # All project documentation (you are here)
├── notebooks/            # JupyterLab notebooks — one per pipeline step
│   ├── 00_data_quality.ipynb
│   ├── 01_wrangle.ipynb
│   ├── 02_expense_trends.ipynb
│   ├── 03_cashflow_forecast.ipynb
│   ├── 04_networth_trajectory.ipynb
│   └── 05_advisory_summary.ipynb
├── outputs/              # CSV exports (opt-in, date-suffixed, never overwritten)
├── src/                  # Python package — importable from notebooks
│   ├── ingestion/        # MMEX CSV + SQLite parsers, validators
│   ├── storage/          # Local CSV + GCP BigQuery/GCS writers
│   ├── analysis/         # Feature engineering, forecasting, trend detection
│   └── utils/            # Logging, config loading, plotting helpers
├── tests/                # pytest suite mirroring src/ structure
├── .github/              # GitHub Copilot instructions
│   └── copilot-instructions.md
├── .roo/                 # Roo instructions
│   └── rules.md
├── .ai-instructions.md   # Shared doc readable by both Copilot and Roo
├── pyproject.toml
└── README.md             # Points to docs/01_PROJECT_OVERVIEW.md
```

---

## Module Dependency Graph

Modules are designed to be built and tested incrementally. Each module depends only on modules above it in this list:

```
configs/pipeline.yaml          ← no dependencies
src/utils/logging_config.py    ← configs
src/utils/config_loader.py     ← configs
src/ingestion/                 ← utils
src/storage/                   ← utils, ingestion
src/analysis/                  ← utils, storage (reads from interim/processed)
notebooks/                     ← all of src
```

### Build Order

1. `configs/` — define paths, parameters, feature flags
2. `src/utils/` — logging, config loader, plot helpers
3. `src/ingestion/` — MMEX parsers and validators
4. `src/storage/` — local parquet/CSV + optional GCP writers
5. `src/analysis/` — feature engineering, OLS trends, forecasting
6. `notebooks/` — orchestration layer calling src modules
7. `tests/` — unit + integration tests per module

---

## Technology Stack

| Layer | Tool | Version Constraint |
|-------|------|--------------------|
| Language | Python | >= 3.11 |
| Data | pandas, pyarrow | pandas >= 2.0 |
| Validation | Pydantic | >= 2.0 |
| Statistics | statsmodels | >= 0.14 |
| Forecasting | Prophet | >= 1.1 |
| ML utilities | scikit-learn | >= 1.3 |
| Static plots | matplotlib, seaborn | latest stable |
| Interactive plots | plotly | >= 5.18 |
| Notebooks | JupyterLab | >= 4.0 |
| Config | PyYAML | latest stable |
| GCP (optional) | google-cloud-storage, google-cloud-bigquery | latest stable |
| Testing | pytest, pytest-cov | latest stable |

---

## Hard Rules (Enforced Across All Code)

These rules apply to every module, notebook, and AI-assisted code generation session. They are duplicated in `.ai-instructions.md` for Copilot/Roo enforcement.

1. **No point forecast without a confidence interval.** State the CI method.
2. **No financial conclusion without listed assumptions.**
3. **No fabricated statistics, benchmarks, or norms.** Use `[insufficient data]` + minimum sample size.
4. **No silent imputation.** Log: what was missing, method used, rows affected.
5. **No financial product / tax / investment recommendations.**
6. **No trusting MMEX category names as canonical.** Surface ambiguous transactions.
7. **No CSV output without explicit user instruction.** Confirm path. Never overwrite — date-suffix.
8. **No trend conclusions on < 6 monthly observations.**

---

## Getting Started

```bash
# 1. Clone and install
git clone <repo-url> && cd mmex-forecast
pip install -e ".[dev]"

# 2. Place your MMEX export
cp ~/Downloads/my_export.csv data/raw/

# 3. Configure
cp configs/pipeline.yaml.example configs/pipeline.yaml
# Edit pipeline.yaml: set input_path, base_currency, etc.

# 4. Run notebooks in order
jupyter lab notebooks/
```

See individual docs for each module:
- `docs/02_DIRECTORY_SETUP.md`
- `docs/03_LOGGING.md`
- `docs/04_INGESTION.md`
- `docs/05_STORAGE_GCP.md`
- `docs/06_STORAGE_LOCAL.md`
- `docs/07_NOTEBOOK_INTERVIEW.md`
- `docs/08_FINANCE_STATISTICS.md`
- `docs/09_AI_AGENT_INSTRUCTIONS.md`
