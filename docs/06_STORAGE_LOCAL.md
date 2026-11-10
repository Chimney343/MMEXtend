# Storing Locally

## Overview

Local storage is the **default and primary** storage path. Every pipeline run writes intermediate and final outputs to the local filesystem. GCP is optional and supplementary.

---

## Storage Layout

```
data/
├── raw/                          # Input: untouched MMEX exports
│   └── mmex_export_2025-03-29.csv
├── interim/                      # Step 1-2 outputs: validated, wrangled
│   ├── transactions_validated.parquet
│   ├── transactions_featured.parquet
│   └── monthly_aggregates.parquet
└── processed/                    # Step 3-5 outputs: analysis results
    ├── expense_trends.parquet
    ├── cashflow_forecast.parquet
    └── networth_trajectory.parquet

outputs/                          # User-requested CSV exports (opt-in only)
├── cashflow_forecast_2025-03-29.csv
└── expense_trends_2025-03-29.csv
```

---

## File Format Decisions

| Stage | Format | Why |
|-------|--------|-----|
| raw | `.csv` or `.mmb` (as-is from MMEX) | Preserves original export |
| interim | `.parquet` | Typed columns, fast I/O, small on disk, supports dates natively |
| processed | `.parquet` | Same benefits; these are the analysis-ready tables |
| exports | `.csv` (opt-in) | User-facing output for spreadsheets, sharing |

**Why Parquet over CSV for intermediates:**
- Column types are preserved (no re-parsing dates/floats on reload)
- 5-10x smaller than CSV for typical financial data
- Faster read/write via `pyarrow`
- Supports nullable integer types (no `NaN` in int columns)

---

## Implementation: `src/storage/local_writer.py`

```python
"""
Write DataFrames to local storage with enforced naming and safety rules.

Hard rules enforced:
  - CSV exports only on explicit instruction
  - Never overwrite: date suffix on every CSV
  - Always log: absolute path + row count

Usage:
    from src.storage.local_writer import LocalWriter
    writer = LocalWriter()
    writer.save_interim(df, "transactions_validated")
    writer.save_processed(df, "expense_trends")
    writer.export_csv(df, "cashflow_forecast")  # only when user asks
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

# Base directories (relative to project root)
INTERIM_DIR = "data/interim"
PROCESSED_DIR = "data/processed"
EXPORT_DIR = "outputs"


class LocalWriter:
    def __init__(self, project_root: str = "."):
        self.root = Path(project_root).resolve()
        self.interim_dir = self.root / INTERIM_DIR
        self.processed_dir = self.root / PROCESSED_DIR
        self.export_dir = self.root / EXPORT_DIR

    def _ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {path}")

    def save_interim(self, df: pd.DataFrame, name: str) -> Path:
        """
        Save DataFrame to data/interim/ as parquet.
        Overwrites previous version (these are regenerated each run).
        """
        self._ensure_dir(self.interim_dir)
        filepath = self.interim_dir / f"{name}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"Wrote {len(df)} rows to {filepath}")
        return filepath

    def save_processed(self, df: pd.DataFrame, name: str) -> Path:
        """
        Save DataFrame to data/processed/ as parquet.
        Overwrites previous version (these are regenerated each run).
        """
        self._ensure_dir(self.processed_dir)
        filepath = self.processed_dir / f"{name}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"Wrote {len(df)} rows to {filepath}")
        return filepath

    def export_csv(self, df: pd.DataFrame, name: str) -> Path:
        """
        Export DataFrame to outputs/ as date-suffixed CSV.
        NEVER overwrites — date suffix guarantees uniqueness.
        Only call this when the user explicitly requests CSV export.

        Parameters
        ----------
        name : str
            Base name without extension (e.g. "cashflow_forecast")

        Returns
        -------
        Path
            Absolute path of the written file.

        Raises
        ------
        FileExistsError
            If the target file somehow already exists.
        """
        self._ensure_dir(self.export_dir)
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"{name}_{today}.csv"
        filepath = self.export_dir / filename

        if filepath.exists():
            msg = f"File already exists: {filepath}. Write aborted (never overwrite rule)."
            logger.error(msg)
            raise FileExistsError(msg)

        df.to_csv(filepath, index=False)
        abs_path = filepath.resolve()
        logger.info(f"Exported {len(df)} rows to {abs_path}")
        print(f"CSV exported: {abs_path} ({len(df)} rows)")
        return abs_path

    def load_interim(self, name: str) -> pd.DataFrame:
        """Load a parquet file from data/interim/."""
        filepath = self.interim_dir / f"{name}.parquet"
        df = pd.read_parquet(filepath)
        logger.info(f"Loaded {len(df)} rows from {filepath}")
        return df

    def load_processed(self, name: str) -> pd.DataFrame:
        """Load a parquet file from data/processed/."""
        filepath = self.processed_dir / f"{name}.parquet"
        df = pd.read_parquet(filepath)
        logger.info(f"Loaded {len(df)} rows from {filepath}")
        return df
```

---

## Notebook Usage Pattern

```python
# In any notebook:
from src.storage.local_writer import LocalWriter
writer = LocalWriter(project_root="..")

# Saving intermediates (automatic, every run)
writer.save_interim(df_validated, "transactions_validated")

# Loading in next notebook
df = writer.load_interim("transactions_validated")

# CSV export (only when user asks)
# User says: "export the cashflow forecast to CSV"
writer.export_csv(df_forecast, "cashflow_forecast")
# → outputs/cashflow_forecast_2025-03-29.csv
```

---

## Safety Rules (Enforced in Code)

| Rule | Enforcement |
|------|-------------|
| Never overwrite CSV | Date suffix + `FileExistsError` guard |
| Log every write | `logger.info` with path + row count |
| CSV only on request | `export_csv()` is never called automatically |
| Parquet for intermediates | `save_interim()` and `save_processed()` only write `.parquet` |
| Absolute path confirmation | Every export prints the resolved absolute path |

---

## Disk Usage Estimate

For a typical 5-year personal finance history (50k transactions):

| File | Approx Size |
|------|-------------|
| `transactions_validated.parquet` | ~2 MB |
| `monthly_aggregates.parquet` | ~10 KB |
| `expense_trends.parquet` | ~50 KB |
| `cashflow_forecast.parquet` | ~5 KB |
| `networth_trajectory.parquet` | ~5 KB |
| **Total interim + processed** | **~2.1 MB** |
| CSV export (if requested) | ~5 MB |
