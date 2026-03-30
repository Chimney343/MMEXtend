# MMEXtend

**Personal finance forecasting pipeline for Money Manager EX (MMEX) data**

Transform your MMEX transaction exports into actionable financial insights with statistical forecasting, expense trend analysis, and cash flow projections — all delivered through interactive Jupyter notebooks.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/dependency-Poetry-blue)](https://python-poetry.org/)

---

## What It Does

MMEXtend ingests your MMEX data (CSV or SQLite), validates and enriches it with features, then produces:

- **📊 Expense Trend Analysis** – OLS trend lines, seasonality detection, month-over-month deltas per category
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

   **With rate caching** (recommended for repeat runs):
   ```bash
   python scripts/run_pipeline.py --mmb path/to/MyMoney.mmb --rate-cache data/interim/rate_cache.json
   ```

   **Without external rate fetching** (use only MMEX stored rates):
   ```bash
   python scripts/run_pipeline.py --mmb path/to/MyMoney.mmb --no-rate-cache
   ```

### What Happens During Data Loading

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
| `monthly_aggregates.parquet` | Monthly category summaries | Forecasting and reporting |

**Load outputs in Python**:
```python
import pandas as pd

# Load validated transactions
df = pd.read_parquet("data/interim/transactions_validated.parquet")

# Load featured transactions
df_featured = pd.read_parquet("data/interim/transactions_featured.parquet")

# Load monthly aggregates
monthly = pd.read_parquet("data/interim/monthly_aggregates.parquet")
```

### Interactive Analysis with Notebooks

Launch JupyterLab to explore your data interactively:

```bash
# With Poetry
poetry run jupyter lab notebooks/

# OR with pip installation
jupyter lab notebooks/
```

**Available notebooks** (run in order):

| Notebook | Purpose | Outputs |
|----------|---------|---------|
| [`00_data_quality.ipynb`](notebooks/00_data_quality.ipynb) | Validation report, data profiling | Quality metrics, issue summary |
| [`01_wrangle.ipynb`](notebooks/01_wrangle.ipynb) | Feature exploration, distributions | Statistical summaries |
| [`02_expense_trends.ipynb`](notebooks/02_expense_trends.ipynb) | Category trends, seasonality tests | Trend plots, seasonal flags |
| [`03_cashflow_forecast.ipynb`](notebooks/03_cashflow_forecast.ipynb) | 12-month cash flow projection | Forecast with confidence intervals |

Each notebook:
- Loads data from `data/interim/`
- Performs analysis with full transparency
- Generates publication-ready plots
- Optionally exports results to CSV (with explicit confirmation)

### Exporting Results

To export analysis results to CSV:

```python
# In any notebook
monthly.to_csv("outputs/monthly_summary_2026-03-30.csv", index=False)
```

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
│   ├── pipeline.yaml     # Pipeline settings (create from template)
│   └── categories.yaml   # Category classification rules
├── data/
│   ├── raw/              # Your MMEX exports (.csv, .mmb)
│   ├── interim/          # Processed data (.parquet) ← Pipeline outputs here
│   └── processed/        # Analysis-ready aggregates
├── notebooks/            # Jupyter notebooks for analysis
│   ├── 00_data_quality.ipynb
│   ├── 01_wrangle.ipynb
│   ├── 02_expense_trends.ipynb
│   └── 03_cashflow_forecast.ipynb
├── scripts/
│   └── run_pipeline.py   # Main entry point
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

# 2. Load and process data
python scripts/run_pipeline.py --csv data/raw/my_transactions.csv

# 3. Open notebooks for analysis
poetry run jupyter lab notebooks/
```

### Advanced: Multi-Currency with Rate Caching

```bash
# First run: fetches rates from NBP API and caches them
python scripts/run_pipeline.py \
  --mmb ~/Documents/MyMoney.mmb \
  --rate-cache data/interim/rate_cache.json

# Subsequent runs: uses cached rates (much faster)
python scripts/run_pipeline.py \
  --mmb ~/Documents/MyMoney.mmb \
  --rate-cache data/interim/rate_cache.json
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
- **[`categories.yaml`](configs/categories.yaml)** – Fixed vs. variable expense classification

Example `pipeline.yaml`:
```yaml
input:
  type: csv  # or sqlite
  path: data/raw/transactions.csv

processing:
  base_currency: PLN
  rate_cache_enabled: true
  rate_cache_path: data/interim/rate_cache.json

output:
  interim_dir: data/interim
  format: parquet
```

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
| [`07_NOTEBOOK_INTERVIEW.md`](docs/07_NOTEBOOK_INTERVIEW.md) | Notebook workflow guide |
| [`08_FINANCE_STATISTICS.md`](docs/08_FINANCE_STATISTICS.md) | Statistical methods reference |

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
- Check data quality with [`notebooks/00_data_quality.ipynb`](notebooks/00_data_quality.ipynb)
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
