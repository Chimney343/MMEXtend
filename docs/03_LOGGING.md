# Logging Strategy

## Overview

Every pipeline step produces structured log output so you can audit what happened, what was imputed, what was skipped, and what assumptions were made. This is critical for reproducibility and for the hard rule: **never silently impute missing values**.

---

## Architecture

```
src/utils/logging_config.py    → Central setup, called once per session
Each module                    → Gets its own named logger
notebooks/                     → Logger output appears in cell output
outputs/logs/                  → Persistent log files (one per run)
```

---

## Implementation: `src/utils/logging_config.py`

```python
"""
Centralised logging configuration.

Usage (in any module):
    from src.utils.logging_config import setup_logging
    logger = setup_logging(__name__)

Usage (in notebooks):
    from src.utils.logging_config import setup_notebook_logging
    logger = setup_notebook_logging()
"""

import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logging(
    name: str = "mmex_pipeline",
    level: str = "INFO",
    log_dir: str = "outputs/logs",
) -> logging.Logger:
    """
    Configure and return a named logger.

    Outputs to:
      - stderr (for notebook cell display)
      - a date-stamped file in log_dir

    Parameters
    ----------
    name : str
        Logger name. Use __name__ from the calling module.
    level : str
        One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
    log_dir : str
        Directory for persistent log files. Created if absent.

    Returns
    -------
    logging.Logger
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers on re-import
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (shows in notebook cells)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler (persistent)
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"pipeline_{today}.log"),
        mode="a",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def setup_notebook_logging(level: str = "INFO") -> logging.Logger:
    """Convenience wrapper for Jupyter notebooks."""
    return setup_logging(name="notebook", level=level)
```

---

## What to Log (Mandatory Events)

Every module MUST log these events. This is not optional — it enforces the hard rules.

### Ingestion (`src/ingestion/`)

| Event | Level | Message Template |
|-------|-------|------------------|
| File loaded | INFO | `Loaded {n_rows} rows from {filepath}` |
| Date range | INFO | `Date range: {min_date} to {max_date}` |
| Date gap detected | WARNING | `Date gap > 14 days: {gap_start} to {gap_end} ({n_days} days)` |
| Null amounts | ERROR | `{n} rows with null Amount ({pct:.1f}%). Pipeline halted.` |
| Null dates | ERROR | `{n} rows with null Date ({pct:.1f}%). Pipeline halted.` |
| Multi-currency | WARNING | `Multiple currencies detected: {currencies}. Awaiting user input.` |
| Uncategorised txns | WARNING | `{n} uncategorised transactions ({pct:.1f}% of total)` |
| Null accounts | WARNING | `{n} transactions with null/blank account name` |

### Imputation (anywhere)

**CRITICAL**: Every imputation must produce a log entry. Template:

```
IMPUTATION | column={col} | method={method} | rows_affected={n} | detail={description}
```

Examples:
```
WARNING | IMPUTATION | column=Category | method=filled_as_Uncategorised | rows_affected=42 | detail=Blank category set to 'Uncategorised'
WARNING | IMPUTATION | column=Amount | method=none | rows_affected=3 | detail=Rows dropped (null Amount violates hard rule)
```

### Feature Engineering (`src/analysis/`)

| Event | Level | Message Template |
|-------|-------|------------------|
| Monthly aggregation | INFO | `Aggregated {n_months} months: {first_month} to {last_month}` |
| Category classification | INFO | `Fixed/variable split: {n_fixed} fixed, {n_variable} variable, {n_unclassified} unclassified` |
| Opening balance missing | WARNING | `No opening balance provided. Cumulative cash balance cannot be computed.` |
| Insufficient history | WARNING | `Category '{cat}' has only {n} months of data (< 6). Trend analysis skipped.` |

### Forecasting (`src/analysis/`)

| Event | Level | Message Template |
|-------|-------|------------------|
| Method selected | INFO | `Forecast method: {method} (reason: {n_months} months of history)` |
| Forecast complete | INFO | `12-month forecast generated. Median endpoint: {value:.0f}` |
| CI computed | INFO | `Confidence intervals: 80% [{lo80:.0f}, {hi80:.0f}], 95% [{lo95:.0f}, {hi95:.0f}]` |
| CI not computable | WARNING | `Confidence interval could not be computed. Reason: {reason}` |

### File I/O

| Event | Level | Message Template |
|-------|-------|------------------|
| File written | INFO | `Wrote {n_rows} rows to {abs_path}` |
| File exists guard | ERROR | `File already exists: {path}. Write aborted (never overwrite rule).` |
| Directory created | INFO | `Created directory: {path}` |

---

## Log Levels Guide

| Level | Use When |
|-------|----------|
| DEBUG | Intermediate DataFrame shapes, column dtypes, config values loaded |
| INFO | Successful completions, counts, date ranges, method selections |
| WARNING | Imputations, missing data, user input needed, skipped analyses |
| ERROR | Pipeline-halting conditions (null rate > 2%, file conflicts) |
| CRITICAL | Should not occur in normal operation. Reserved for unrecoverable failures. |

---

## Notebook Integration

In every notebook, the first code cell should be:

```python
# Cell 1: Setup
import sys
sys.path.insert(0, "..")

from src.utils.logging_config import setup_notebook_logging
logger = setup_notebook_logging(level="INFO")

logger.info("Notebook started: 00_data_quality")
```

Log output appears directly in cell output. The persistent log file in `outputs/logs/` captures the full session across all notebooks run that day.

---

## Reviewing Logs

```bash
# Tail today's log
tail -f outputs/logs/pipeline_$(date +%Y-%m-%d).log

# Find all imputations
grep "IMPUTATION" outputs/logs/pipeline_*.log

# Find all warnings and errors
grep -E "WARNING|ERROR" outputs/logs/pipeline_*.log
```
