# Ingestion: Getting Data Out of MMEX

## Overview

Money Manager EX (MMEX) stores data in either a **SQLite database** (`.mmb` file) or can export to **CSV**. This document covers both extraction paths and the validation layer that sits between raw data and the rest of the pipeline.

---

## Option A: CSV Export (Recommended for Simplicity)

### How to Export from MMEX

1. Open MMEX desktop application
2. Go to **File → Export → Export as CSV...**
3. Select **All Accounts** (or specific accounts)
4. Ensure these columns are included:
   - `Date` (or `TransDate`)
   - `Account`
   - `ToAccount` (for transfers)
   - `Payee`
   - `Category`
   - `SubCategory`
   - `Amount`
   - `Currency`
   - `Type` (Deposit / Withdrawal / Transfer)
   - `Notes`
5. Save to `data/raw/mmex_export_YYYY-MM-DD.csv`

### Known CSV Quirks

| Issue | Handling |
|-------|----------|
| Date format varies by locale | Parser tries `%Y-%m-%d`, `%d/%m/%Y`, `%m/%d/%Y` in order |
| Amount sign convention differs | Some exports: positive=income, negative=expense. Others use Type column. Normalise in parser. |
| Encoding | MMEX may export as UTF-8 or CP-1252. Parser detects via `chardet`. |
| Separator | Usually `,` but some locales use `;`. Parser tries both. |
| Transfer double-counting | Transfers appear as two rows (debit + credit). Flag via `Type == 'Transfer'`. |

### Implementation: `src/ingestion/mmex_csv_parser.py`

```python
"""
Parse MMEX CSV exports into a validated DataFrame.

Usage:
    from src.ingestion.mmex_csv_parser import parse_mmex_csv
    df = parse_mmex_csv("data/raw/mmex_export_2025-03-29.csv")
"""

import pandas as pd
import chardet
from pathlib import Path
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

# Date formats to try in priority order
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"]

# Column name normalisation map (MMEX exports vary)
COLUMN_MAP = {
    "transdate": "date",
    "trans_date": "date",
    "date": "date",
    "account_name": "account",
    "account": "account",
    "toaccount": "to_account",
    "to_account": "to_account",
    "payee_name": "payee",
    "payee": "payee",
    "category_name": "category",
    "category": "category",
    "subcategory_name": "subcategory",
    "subcategory": "subcategory",
    "amount": "amount",
    "currency": "currency",
    "type": "type",
    "notes": "notes",
    "transid": "transaction_id",
}


def detect_encoding(filepath: str) -> str:
    """Detect file encoding using chardet."""
    with open(filepath, "rb") as f:
        result = chardet.detect(f.read(10000))
    encoding = result["encoding"]
    logger.info(f"Detected encoding: {encoding} (confidence: {result['confidence']:.0%})")
    return encoding


def detect_separator(filepath: str, encoding: str) -> str:
    """Detect CSV separator by counting candidates in first 5 lines."""
    with open(filepath, "r", encoding=encoding) as f:
        head = "".join(f.readline() for _ in range(5))
    counts = {sep: head.count(sep) for sep in [",", ";", "\t"]}
    sep = max(counts, key=counts.get)
    logger.info(f"Detected separator: {repr(sep)}")
    return sep


def parse_dates(series: pd.Series) -> pd.Series:
    """Try multiple date formats. Return parsed dates or raise."""
    for fmt in DATE_FORMATS:
        try:
            parsed = pd.to_datetime(series, format=fmt, dayfirst=("d" == fmt[1]))
            logger.info(f"Date format matched: {fmt}")
            return parsed
        except (ValueError, TypeError):
            continue
    # Fallback: let pandas infer
    logger.warning("No explicit format matched. Using pandas inference (less reliable).")
    return pd.to_datetime(series, infer_datetime_format=True)


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map MMEX column names to canonical names."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    logger.info(f"Columns after normalisation: {list(df.columns)}")
    return df


def normalise_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure consistent sign convention:
      income (Deposit) → positive
      expense (Withdrawal) → negative
      transfer → signed based on account perspective
    """
    if "type" in df.columns:
        mask_withdrawal = df["type"].str.lower().isin(["withdrawal", "expense"])
        # Only flip sign if amounts are all positive (some exports pre-sign)
        if (df.loc[mask_withdrawal, "amount"] > 0).all():
            df.loc[mask_withdrawal, "amount"] *= -1
            logger.info(f"Flipped sign on {mask_withdrawal.sum()} withdrawal rows")
    return df


def parse_mmex_csv(filepath: str) -> pd.DataFrame:
    """
    Full parse pipeline: detect encoding → detect separator → read →
    normalise columns → parse dates → normalise amounts.

    Returns a DataFrame with canonical column names ready for validation.
    """
    filepath = str(Path(filepath).resolve())
    logger.info(f"Parsing MMEX CSV: {filepath}")

    encoding = detect_encoding(filepath)
    sep = detect_separator(filepath, encoding)

    df = pd.read_csv(filepath, encoding=encoding, sep=sep, dtype=str)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    df = normalise_columns(df)
    df["date"] = parse_dates(df["date"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = normalise_amounts(df)

    return df
```

---

## Option B: Direct SQLite Read (`.mmb` Files)

MMEX stores its database as a SQLite file with extension `.mmb`. This gives you richer data than CSV export.

### Key Tables in MMEX SQLite

| Table | Contains |
|-------|----------|
| `CHECKINGACCOUNT_V1` | All transactions |
| `ACCOUNTLIST_V1` | Account names, types, initial balances |
| `CATEGORY_V1` | Top-level categories |
| `SUBCATEGORY_V1` | Subcategories (linked to CATEGORY_V1) |
| `PAYEE_V1` | Payee names |
| `CURRENCYFORMATS_V1` | Currency definitions |
| `INFOTABLE_V1` | App settings including base currency |

### Implementation: `src/ingestion/mmex_sqlite_parser.py`

```python
"""
Parse MMEX SQLite database (.mmb) into a validated DataFrame.

Usage:
    from src.ingestion.mmex_sqlite_parser import parse_mmex_sqlite
    df = parse_mmex_sqlite("data/raw/my_finances.mmb")
"""

import sqlite3
import pandas as pd
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

TRANSACTION_QUERY = """
SELECT
    t.TRANSID          AS transaction_id,
    t.TRANSDATE        AS date,
    a.ACCOUNTNAME      AS account,
    a2.ACCOUNTNAME     AS to_account,
    p.PAYEENAME        AS payee,
    c.CATEGNAME        AS category,
    sc.SUBCATEGNAME    AS subcategory,
    t.TRANSAMOUNT      AS amount,
    t.TRANSCODE        AS type,
    t.NOTES            AS notes,
    t.TOAMOUNT         AS to_amount,
    cur.CURRENCY_SYMBOL AS currency
FROM CHECKINGACCOUNT_V1 t
LEFT JOIN ACCOUNTLIST_V1 a   ON t.ACCOUNTID = a.ACCOUNTID
LEFT JOIN ACCOUNTLIST_V1 a2  ON t.TOACCOUNTID = a2.ACCOUNTID
LEFT JOIN PAYEE_V1 p         ON t.PAYEEID = p.PAYEEID
LEFT JOIN CATEGORY_V1 c      ON t.CATEGID = c.CATEGID
LEFT JOIN SUBCATEGORY_V1 sc  ON t.SUBCATEGID = sc.SUBCATEGID
LEFT JOIN ACCOUNTLIST_V1 acur ON t.ACCOUNTID = acur.ACCOUNTID
LEFT JOIN CURRENCYFORMATS_V1 cur ON acur.CURRENCYID = cur.CURRENCYID
ORDER BY t.TRANSDATE
"""

ACCOUNT_BALANCES_QUERY = """
SELECT
    ACCOUNTNAME AS account,
    INITIALBAL  AS opening_balance,
    STATUS      AS status
FROM ACCOUNTLIST_V1
WHERE STATUS = 'Open'
"""


def parse_mmex_sqlite(filepath: str) -> pd.DataFrame:
    """Read MMEX .mmb SQLite file and return normalised transaction DataFrame."""
    logger.info(f"Parsing MMEX SQLite: {filepath}")

    conn = sqlite3.connect(filepath)
    try:
        df = pd.read_sql_query(TRANSACTION_QUERY, conn)
        logger.info(f"Loaded {len(df)} transactions from SQLite")

        # Parse dates
        df["date"] = pd.to_datetime(df["date"])

        # Normalise amounts: MMEX stores all as positive, type indicates direction
        mask_withdrawal = df["type"].str.lower() == "withdrawal"
        df.loc[mask_withdrawal, "amount"] *= -1
        logger.info(f"Sign-flipped {mask_withdrawal.sum()} withdrawals")

        return df
    finally:
        conn.close()


def get_account_balances(filepath: str) -> pd.DataFrame:
    """Extract opening balances per account from MMEX SQLite."""
    conn = sqlite3.connect(filepath)
    try:
        balances = pd.read_sql_query(ACCOUNT_BALANCES_QUERY, conn)
        logger.info(f"Found {len(balances)} open accounts with initial balances")
        return balances
    finally:
        conn.close()
```

---

## Validation Layer: `src/ingestion/validator.py`

After parsing (CSV or SQLite), every DataFrame passes through validation before entering the pipeline.

```python
"""
Validate parsed MMEX data. Produces a data quality report.
Halts pipeline if critical thresholds are breached.

Usage:
    from src.ingestion.validator import validate_transactions
    report = validate_transactions(df)
"""

import pandas as pd
from dataclasses import dataclass, field
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

CRITICAL_NULL_THRESHOLD = 0.02  # 2% — pipeline halts above this


@dataclass
class DataQualityReport:
    rows_loaded: int = 0
    date_min: str = ""
    date_max: str = ""
    null_rates: dict = field(default_factory=dict)  # column → float
    currency_count: int = 0
    currencies_found: list = field(default_factory=list)
    uncategorised_count: int = 0
    uncategorised_pct: float = 0.0
    null_account_count: int = 0
    date_gaps: list = field(default_factory=list)  # list of (start, end, n_days)
    critical_failures: list = field(default_factory=list)

    def is_passing(self) -> bool:
        return len(self.critical_failures) == 0

    def display(self):
        """Pretty-print the report (for notebook display)."""
        print("=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)
        print(f"  Rows loaded:           {self.rows_loaded}")
        print(f"  Date range:            {self.date_min} → {self.date_max}")
        print(f"  Currencies:            {self.currencies_found}")
        print(f"  Uncategorised txns:    {self.uncategorised_count} ({self.uncategorised_pct:.1f}%)")
        print(f"  Null accounts:         {self.null_account_count}")
        print(f"\n  Null rates per column:")
        for col, rate in sorted(self.null_rates.items()):
            flag = " *** CRITICAL" if rate > CRITICAL_NULL_THRESHOLD and col in ("date", "amount") else ""
            print(f"    {col:25s} {rate:6.2%}{flag}")
        if self.date_gaps:
            print(f"\n  Date gaps > 14 days:")
            for start, end, days in self.date_gaps:
                print(f"    {start} → {end} ({days} days)")
        if self.critical_failures:
            print(f"\n  *** CRITICAL FAILURES (pipeline halted):")
            for f in self.critical_failures:
                print(f"    - {f}")
        print("=" * 60)


def validate_transactions(df: pd.DataFrame) -> DataQualityReport:
    """Run all validation checks and return a DataQualityReport."""
    report = DataQualityReport()
    report.rows_loaded = len(df)

    # Date validation
    report.date_min = str(df["date"].min().date()) if df["date"].notna().any() else "N/A"
    report.date_max = str(df["date"].max().date()) if df["date"].notna().any() else "N/A"

    # Null rates
    for col in df.columns:
        rate = df[col].isna().mean()
        report.null_rates[col] = rate

    # Critical null checks
    for col in ["date", "amount"]:
        if col in df.columns:
            rate = df[col].isna().mean()
            if rate > CRITICAL_NULL_THRESHOLD:
                msg = f"Null rate in '{col}' is {rate:.1%} (threshold: {CRITICAL_NULL_THRESHOLD:.0%})"
                report.critical_failures.append(msg)
                logger.error(msg)

    # Currency check
    if "currency" in df.columns:
        currencies = df["currency"].dropna().unique().tolist()
        report.currency_count = len(currencies)
        report.currencies_found = currencies
        if len(currencies) > 1:
            logger.warning(f"Multiple currencies detected: {currencies}")

    # Uncategorised transactions
    if "category" in df.columns:
        uncat_mask = df["category"].isna() | (df["category"].str.strip() == "")
        report.uncategorised_count = uncat_mask.sum()
        report.uncategorised_pct = 100 * uncat_mask.mean()
        if report.uncategorised_count > 0:
            logger.warning(f"{report.uncategorised_count} uncategorised transactions")

    # Null accounts
    if "account" in df.columns:
        null_acct = df["account"].isna() | (df["account"].str.strip() == "")
        report.null_account_count = null_acct.sum()

    # Date gap check
    if df["date"].notna().any():
        dates_sorted = df["date"].dropna().sort_values().reset_index(drop=True)
        gaps = dates_sorted.diff()
        big_gaps = gaps[gaps > pd.Timedelta(days=14)]
        for idx in big_gaps.index:
            gap_start = dates_sorted.iloc[idx - 1]
            gap_end = dates_sorted.iloc[idx]
            n_days = (gap_end - gap_start).days
            report.date_gaps.append((str(gap_start.date()), str(gap_end.date()), n_days))
            logger.warning(f"Date gap: {gap_start.date()} → {gap_end.date()} ({n_days} days)")

    return report
```

---

## Pydantic Schema: `src/ingestion/schemas.py`

```python
"""Pydantic models for validating individual transaction rows."""

from pydantic import BaseModel, field_validator
from datetime import date
from typing import Optional


class TransactionRow(BaseModel):
    date: date
    account: str
    amount: float
    type: str  # Deposit, Withdrawal, Transfer
    category: Optional[str] = None
    subcategory: Optional[str] = None
    payee: Optional[str] = None
    to_account: Optional[str] = None
    currency: Optional[str] = None
    notes: Optional[str] = None
    transaction_id: Optional[int] = None

    @field_validator("account")
    @classmethod
    def account_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Account name must not be blank")
        return v.strip()

    @field_validator("type")
    @classmethod
    def valid_type(cls, v: str) -> str:
        allowed = {"deposit", "withdrawal", "transfer"}
        if v.lower() not in allowed:
            raise ValueError(f"Type must be one of {allowed}, got '{v}'")
        return v.lower()
```

---

## Decision Flow

```
User has .mmb file? ──Yes──→ parse_mmex_sqlite()
         │
         No
         │
User has .csv file? ──Yes──→ parse_mmex_csv()
         │
         No
         │
         └──→ Ask user to export from MMEX (instructions above)
         
         ↓ (either path)
         
validate_transactions(df) → DataQualityReport
         │
    is_passing()?
     │         │
    Yes        No → Log critical failures, HALT, display report
     │
     ↓
Save to data/interim/transactions_validated.parquet
Proceed to Step 2 (Wrangle & Feature Engineer)
```
