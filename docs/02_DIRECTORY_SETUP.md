# Directory Setup

## Overview

This document explains every directory and its purpose. Follow this structure exactly — AI agents (Copilot, Roo) reference these paths in their instructions.

---

## Directory Tree

```
mmex-forecast/
│
├── configs/
│   ├── pipeline.yaml          # Master config: paths, params, feature flags
│   └── categories.yaml        # User-confirmed fixed vs variable category map
│
├── data/
│   ├── raw/                   # UNTOUCHED MMEX exports — never modified
│   │   ├── .gitkeep
│   │   └── (user places files here)
│   ├── interim/               # Wrangled DataFrames after Step 1-2 (.parquet)
│   │   └── .gitkeep
│   └── processed/             # Analysis-ready monthly aggregates (.parquet)
│       └── .gitkeep
│
├── docs/                      # All project documentation
│   ├── 01_PROJECT_OVERVIEW.md
│   ├── 02_DIRECTORY_SETUP.md  # (this file)
│   ├── 03_LOGGING.md
│   ├── 04_INGESTION.md
│   ├── 05_STORAGE_GCP.md
│   ├── 06_STORAGE_LOCAL.md
│   ├── 08_FINANCE_STATISTICS.md
│   └── 09_AI_AGENT_INSTRUCTIONS.md
│
├── outputs/                   # git-ignored; reports and logs, emptied on every run
│
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── mmex_csv_parser.py
│   │   ├── mmex_sqlite_parser.py
│   │   ├── validator.py
│   │   └── schemas.py         # Pydantic models for raw transaction rows
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── local_writer.py    # Parquet + CSV (date-suffixed) writer
│   │   └── gcp_writer.py      # BigQuery + GCS upload (optional)
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── feature_engineering.py
│   │   ├── expense_trends.py
│   │   ├── cashflow_forecast.py
│   │   ├── networth_trajectory.py
│   │   └── advisory.py
│   └── utils/
│       ├── __init__.py
│       ├── logging_config.py
│       ├── config_loader.py
│       └── plot_helpers.py
│
├── tests/
│   ├── conftest.py            # Shared fixtures (sample DataFrames, tmp paths)
│   ├── test_ingestion/
│   ├── test_storage/
│   ├── test_analysis/
│   └── test_utils/
│
├── .gitignore
├── pyproject.toml
└── README.md
```

---

## Conventions

### File Naming

| Context | Convention | Example |
|---------|-----------|---------|
| Source modules | `snake_case.py` | `mmex_csv_parser.py` |
| Notebooks | `NN_descriptive_name.ipynb` | `02_expense_trends.ipynb` |
| Config files | `snake_case.yaml` | `pipeline.yaml` |
| CSV exports | `{type}_{YYYY-MM-DD}.csv` | `cashflow_forecast_2025-03-29.csv` |
| Parquet intermediates | `{stage}_{descriptor}.parquet` | `interim_transactions_clean.parquet` |
| Docs | `NN_UPPER_SNAKE.md` | `04_INGESTION.md` |

### Data Immutability

- `data/raw/` is **read-only after initial placement**. Never modify files here.
- `data/interim/`, `data/processed/` and `outputs/` are emptied and rebuilt by every
  pipeline run (`outputs/logs/` is kept). Copy anything you want to keep elsewhere.
- Nothing under `data/`, `outputs/` or `.cache/` is ever committed: the repository
  holds the calculator only. Real account names and category buckets live in the
  git-ignored `configs/pipeline.local.yaml`; `tests/test_repo_hygiene.py` fails if
  any of them, or any data file, is tracked.

### Import Paths

Scripts and the dashboard import from `src/` as a package:

```python
# At the top of a script outside the package:
import sys
sys.path.insert(0, "..")  # or pip install -e .

from src.ingestion.mmex_csv_parser import parse_mmex_csv
from src.analysis.cashflow_forecast import forecast_cashflow
from src.utils.logging_config import setup_logging
```

---

## Initialisation Script

Run this once to scaffold empty files:

```bash
#!/usr/bin/env bash
# scripts/init_project.sh

dirs=(
  configs data/raw data/interim data/processed
  docs outputs
  src/ingestion src/storage src/analysis src/utils
  tests/test_ingestion tests/test_storage tests/test_analysis tests/test_utils
)

for d in "${dirs[@]}"; do
  mkdir -p "$d"
done

# Create __init__.py files
for d in src src/ingestion src/storage src/analysis src/utils; do
  touch "$d/__init__.py"
done

echo "Project scaffold created."
```

---

## .gitignore Essentials

```gitignore
# Data (too large / personal)
data/raw/*
data/interim/*
data/processed/*
!data/**/.gitkeep

# Outputs
outputs/*
!outputs/.gitkeep

# Python
__pycache__/
*.pyc
*.egg-info/
.venv/

# Jupyter
.ipynb_checkpoints/

# GCP credentials
*.json
!configs/*.yaml

# OS
.DS_Store
Thumbs.db
```
