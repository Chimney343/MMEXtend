# MMEXtend

**Personal finance forecasting pipeline for Money Manager EX (MMEX) data**

Transform your MMEX transaction exports into actionable financial insights with statistical forecasting, expense trend analysis, and cash flow projections — rebuilt from your MMEX file on every run and shown in a Dash dashboard. The repository is the calculator only: your data, results and account names stay on your machine.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/dependency-Poetry-blue)](https://python-poetry.org/)

---

## What It Does

MMEXtend ingests your MMEX data (CSV or SQLite), validates and enriches it with features, then produces:

- **📊 Expense Trend Analysis** – OLS trend lines, seasonality detection, month-over-month deltas per category
- **🔁 Recurring Costs Audit** – commitment register, category summary, renewal calendar, and price-change detection
- **💰 Cash Flow Forecasting** – 12-month projections with 80%/95% confidence intervals
- **📈 Statistical Reporting** – Transaction validation, outlier detection, feature engineering
- **🔄 Multi-Currency Support** – Automatic exchange rate fetching from NBP API with local caching

All outputs include confidence intervals, documented assumptions, and no fabricated benchmarks.

---

## Quick Start

### Prerequisites

- **Python 3.11 or 3.12**
- **Poetry** (recommended) or pip
- Your MMEX data exported as CSV or SQLite (`.mmb`) file

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd MMEXtend

# Install with Poetry (recommended)
poetry install

# OR install with pip
pip install -e ".[dev]"
```

---

## Loading Your Data

MMEXtend supports two input formats from Money Manager EX:

### Option 1: CSV Export (Simplest)

1. **Export from MMEX**: File → Export → CSV
2. **Place your CSV** in the `data/raw/` directory:
   ```bash
   cp ~/Downloads/transactions.csv data/raw/
   ```

3. **Run the pipeline**:
   ```bash
   python scripts/run_pipeline.py --csv data/raw/transactions.csv
   ```

### Option 2: SQLite Database (`.mmb` file)

This method provides automatic multi-currency conversion using NBP exchange rates.

1. **Locate your MMEX database** (typically `MyMoney.mmb`)
2. **Run the pipeline**:
   ```bash
   python scripts/run_pipeline.py --mmb path/to/MyMoney.mmb
   ```

   NBP rates are cached in `.cache/nbp_rates.json` by default (git-ignored,
   kept between runs). To use another cache file:
   ```bash
   python scripts/run_pipeline.py --mmb path/to/MyMoney.mmb --rate-cache path/to/rates.json
   ```

   **Without external rate fetching** (use only MMEX stored rates):
   ```bash
   python scripts/run_pipeline.py --mmb path/to/MyMoney.mmb --no-rate-cache
   ```

### What Happens During Data Loading

Every run rebuilds every output from the source file. It first empties
`data/interim/`, `data/processed/` and `outputs/` (keeping `outputs/logs/`),
so nothing from an earlier run survives. All of these folders are git-ignored:
this repository holds the calculator, never data or results.

The pipeline performs these steps automatically:

1. **Parse** – Reads CSV or SQLite, extracts transactions, categories, accounts
2. **Validate** – Checks data quality, reports missing or invalid fields
3. **Enrich** – Adds currency conversions, date features, rolling statistics
4. **Save** – Writes validated and featured data to `data/interim/` as Parquet files

All steps are logged with structured output showing progress and any issues detected.

---

## Getting Outputs

### Automated Pipeline Outputs

After running [`run_pipeline.py`](scripts/run_pipeline.py), three files are created in **`data/interim/`**:

| File | Description | Use Case |
|------|-------------|----------|
| `transactions_validated.parquet` | Cleaned, validated transactions | Data quality review |
| `transactions_featured.parquet` | Enriched with engineered features | Trend analysis input |
| `monthly_aggregates.parquet` | Monthly category flows, income and expense kept separate | Forecasting and reporting |

**Load outputs in Python**:
```python
import pandas as pd

# Load validated transactions
df = pd.read_parquet("data/interim/transactions_validated.parquet")

# Load featured transactions
df_featured = pd.read_parquet("data/interim/transactions_featured.parquet")

# Load monthly aggregates — one row per (year_month, category, flow_type)
# flow_type is "expense" or "income"; total_amount is always a positive magnitude
monthly = pd.read_parquet("data/interim/monthly_aggregates.parquet")
```

### Interactive Dashboard and AI Advisor

`just dashboard` rebuilds every output, then launches the Dash dashboard:

```bash
just dashboard                        # source file from MMB_PATH in .env
just dashboard "C:\path\to\finances.mmb"
```

The dashboard reads the Parquet files in `data/interim/` and
`data/processed/`. It includes an optional **AI Advisor** drawer (the
**💬 Advisor** button in the navbar) backed by DeepSeek. The advisor can query
the dashboard data read-only, so it can answer questions like "What were my top
5 expense categories over the last 12 months?" or "Build me a Conscious
Spending Plan from my actual income and spending."

To enable it, install the dashboard group and add `DEEPSEEK_API_KEY` to
`.env` (see [Configuration](#configuration)). Without the key the drawer shows
an alert and the rest of the dashboard still works. See
[`09_DASHBOARD.md`](docs/09_DASHBOARD.md#ai-advisor) for setup, the read-only
guarantees, the data it can see, and the privacy note.

### Exporting Results

To export analysis results to CSV:

```python
# In your own Python session
monthly.to_csv("outputs/monthly_summary_2026-03-30.csv", index=False)
```

`outputs/` is emptied on the next pipeline run, so copy anything you want to
keep elsewhere.

**CSV export rules**:
- Never overwrites existing files
- Always uses date suffixes (e.g., `_2026-03-30`)
- Only created when explicitly requested
- Saved to `outputs/` directory

---

## Project Structure

```
MMEXtend/
├── configs/              # Configuration files
│   ├── pipeline.yaml     # Pipeline settings with example names
│   ├── pipeline.local.yaml  # Your real account names and buckets (git-ignored)
│   └── categories.yaml   # Category classification rules
├── data/                 # git-ignored; rebuilt on every run
│   ├── raw/              # Your MMEX exports (.csv, .mmb) — never cleared
│   ├── interim/          # Validated and featured data (.parquet)
│   └── processed/        # Analysis outputs (.parquet)
├── outputs/              # git-ignored; reports and logs
├── .cache/               # git-ignored; NBP exchange-rate cache
├── dashboard/            # Dash app
├── scripts/
│   ├── run_pipeline.py   # Main entry point
│   └── build_frozen_report.py
├── src/                  # Python package
│   ├── ingestion/        # MMEX parsers, validators
│   ├── storage/          # Data writers
│   ├── analysis/         # Feature engineering, forecasting
│   └── utils/            # Config, logging helpers
└── tests/                # Pytest test suite
```

---

## Usage Examples

### Minimal Workflow

```bash
# 1. Install dependencies
poetry install

# 2. Point the pipeline at your MMEX file
echo 'MMB_PATH=C:\path\to\finances.mmb' >> .env

# 3. Rebuild every output and open the dashboard
just dashboard
```

### Multi-Currency with Rate Caching

```bash
# First run fetches NBP rates and caches them in .cache/nbp_rates.json;
# later runs reuse the cache for rates already fetched.
python scripts/run_pipeline.py --mmb ~/Documents/MyMoney.mmb
```

### Programmatic Usage

```python
from src.ingestion.mmex_sqlite_parser import parse_mmex_sqlite
from src.analysis.feature_engineering import engineer_features, build_monthly_aggregates

# Parse SQLite database
df = parse_mmex_sqlite("path/to/MyMoney.mmb")

# Add features
df_featured = engineer_features(df)

# Create monthly aggregates
monthly = build_monthly_aggregates(df_featured)

# Analyze
print(monthly.groupby("category")["amount_pln"].sum())
```

---

## Features & Capabilities

### Data Ingestion
- ✅ MMEX CSV exports (universal)
- ✅ MMEX SQLite databases (`.mmb` files)
- ✅ Multi-currency transaction support
- ✅ Automatic exchange rate fetching (NBP API)
- ✅ Local rate caching for offline/fast operation

### Data Validation
- ✅ Schema validation with Pydantic
- ✅ Missing field detection and reporting
- ✅ Duplicate transaction identification
- ✅ Category consistency checks
- ✅ Outlier detection

### Feature Engineering
- ✅ Date features (day of week, month, quarter)
- ✅ Rolling averages (7, 30, 90 days)
- ✅ Lag features for time series
- ✅ Category-based aggregations
- ✅ Income/expense classification

### Analysis
- ✅ OLS trend estimation per category
- ✅ Seasonality testing (KPSS, ADF)
- ✅ Cash flow forecasting with confidence intervals
- ✅ Month-over-month delta analysis
- ✅ Statistical summary reports

---

## Configuration

Configuration files in [`configs/`](configs/):

- **[`pipeline.yaml`](configs/pipeline.yaml)** – Main settings (paths, currency, parameters)
- **`pipeline.local.yaml`** – Private overrides, git-ignored. Deep-merged over `pipeline.yaml` (dicts merge, lists replace); put real account names under `budget_health.account_groups` here
- **[`categories.yaml`](configs/categories.yaml)** – Fixed vs. variable expense classification

Example `pipeline.yaml`:
```yaml
input:
  type: csv  # or sqlite
  path: data/raw/transactions.csv

processing:
  base_currency: PLN
  rate_cache_enabled: true
  rate_cache_path: .cache/nbp_rates.json

output:
  interim_dir: data/interim
  format: parquet
```

### Environment variables (`.env`)

Create a `.env` file in the project root. It is gitignored and must never be
committed:

```dotenv
# Required for the AI Advisor
DEEPSEEK_API_KEY=sk-...

# Required for the dashboard's raw MMEX tables and `just dashboard`
MMB_PATH=C:\path\to\finances.mmb

# Optional
DEEPSEEK_MODEL=deepseek-flash
DEEPSEEK_BASE_URL=https://api.deepseek.com
```

The dashboard loads `.env` at startup. If `DEEPSEEK_API_KEY` is absent, the AI
Advisor is disabled and the rest of the dashboard works normally.

---

## Documentation

Full documentation in [`docs/`](docs/):

| Document | Contents |
|----------|----------|
| [`01_PROJECT_OVERVIEW.md`](docs/01_PROJECT_OVERVIEW.md) | Architecture, design philosophy, tech stack |
| [`02_DIRECTORY_SETUP.md`](docs/02_DIRECTORY_SETUP.md) | Detailed directory structure guide |
| [`03_LOGGING.md`](docs/03_LOGGING.md) | Structured logging configuration |
| [`04_INGESTION.md`](docs/04_INGESTION.md) | MMEX parsing internals |
| [`05_STORAGE_GCP.md`](docs/05_STORAGE_GCP.md) | Optional Google Cloud Platform integration |
| [`06_STORAGE_LOCAL.md`](docs/06_STORAGE_LOCAL.md) | Local storage with Parquet |
| [`08_FINANCE_STATISTICS.md`](docs/08_FINANCE_STATISTICS.md) | Statistical methods reference |
| [`09_DASHBOARD.md`](docs/09_DASHBOARD.md) | Dashboard tabs, callbacks, data flow, and the AI Advisor |
| [`10_RECURRING_COMMITMENTS.md`](docs/10_RECURRING_COMMITMENTS.md) | Recurring-commitment detection, register, renewal calendar |

---

## Development

### Running Tests

```bash
# Run all tests
poetry run pytest

# With coverage report
poetry run pytest --cov=src --cov-report=html

# Run specific test module
poetry run pytest tests/test_ingestion/test_mmex_csv_parser.py
```

### Code Quality

```bash
# Lint with Ruff
poetry run ruff check src/ tests/

# Format code
poetry run ruff format src/ tests/
```

### Adding Dependencies

```bash
# Add runtime dependency
poetry add package-name

# Add dev dependency
poetry add --group dev package-name

# Add forecasting dependency
poetry add --group forecast package-name
```

---

## Design Principles

This project follows strict financial analysis principles:

1. **No point forecasts without confidence intervals** – All predictions include uncertainty quantification
2. **No fabricated statistics** – Only compute from available data; state minimum sample sizes
3. **No silent imputation** – All data cleaning is logged and documented
4. **No financial advice** – Provides analysis tools only, not investment recommendations
5. **Transparency first** – All assumptions, methods, and limitations are documented

See [`docs/01_PROJECT_OVERVIEW.md`](docs/01_PROJECT_OVERVIEW.md) for the complete rule set.

---

## Troubleshooting

### Common Issues

**Problem**: `ModuleNotFoundError: No module named 'src'`
```bash
# Solution: Install package in editable mode
pip install -e .
```

**Problem**: Rate fetching fails
```bash
# Solution: Use cached rates or disable rate fetching
python scripts/run_pipeline.py --mmb file.mmb --no-rate-cache
```

**Problem**: Validation errors in pipeline
- Review validation report in pipeline output
- See [`docs/04_INGESTION.md`](docs/04_INGESTION.md) for data requirements

---

## Requirements

- **Python**: 3.11 or 3.12
- **OS**: Windows, macOS, Linux
- **Memory**: 2GB+ RAM (depends on transaction volume)
- **Storage**: ~100MB for code + your data

Core dependencies: pandas, pyarrow, pydantic, statsmodels, matplotlib, seaborn, plotly, jupyterlab

Optional: prophet, scikit-learn, pmdarima (install with `poetry install --with forecast`)

---

## License

[Specify your license here]

---

## Contributing

Contributions welcome! Please:

1. Read [`docs/01_PROJECT_OVERVIEW.md`](docs/01_PROJECT_OVERVIEW.md) for architecture overview
2. Follow existing code style (Ruff enforced)
3. Add tests for new features
4. Update documentation as needed

---

## Questions?

- **For usage questions**: See [`docs/`](docs/) directory
- **For bugs**: Open an issue with reproduction steps
- **For features**: Open an issue describing the use case

**Note**: This project provides analytical tools only. It does not offer financial, investment, or tax advice. Always consult qualified professionals for financial decisions.
