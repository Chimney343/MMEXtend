"""
Parse MMEX CSV exports into a validated DataFrame.

Usage:
    from src.ingestion.mmex_csv_parser import parse_mmex_csv
    df = parse_mmex_csv("data/raw/mmex_export_2025-03-29.csv")
"""

from pathlib import Path

import chardet
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

# Date formats to try in priority order
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"]

# Column name normalisation map (MMEX exports vary by locale/version)
COLUMN_MAP: dict[str, str] = {
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
        result = chardet.detect(f.read(10_000))
    encoding = result["encoding"] or "utf-8"
    logger.info(f"Detected encoding: {encoding} (confidence: {result['confidence']:.0%})")
    return encoding


def detect_separator(filepath: str, encoding: str) -> str:
    """Detect CSV separator by counting candidates in the first 5 lines."""
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        head = "".join(f.readline() for _ in range(5))
    counts = {sep: head.count(sep) for sep in [",", ";", "\t"]}
    sep = max(counts, key=counts.get)  # type: ignore[arg-type]
    logger.info(f"Detected separator: {repr(sep)}")
    return sep


def parse_dates(series: pd.Series) -> pd.Series:
    """Try explicit date formats first, then fall back to pandas inference."""
    for fmt in DATE_FORMATS:
        try:
            parsed = pd.to_datetime(series, format=fmt)
            logger.info(f"Date format matched: {fmt}")
            return parsed
        except (ValueError, TypeError):
            continue
    logger.warning("No explicit format matched. Using pandas mixed-format inference.")
    return pd.to_datetime(series, format="mixed", dayfirst=False)


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
      income (Deposit)    → positive
      expense (Withdrawal)→ negative
      transfer            → signed based on account perspective
    """
    if "type" in df.columns:
        mask_withdrawal = df["type"].str.lower().isin(["withdrawal", "expense"])
        # Only flip sign if all withdrawal amounts are positive (some exports pre-sign them)
        if mask_withdrawal.any() and (df.loc[mask_withdrawal, "amount"] > 0).all():
            df.loc[mask_withdrawal, "amount"] *= -1
            logger.info(f"Flipped sign on {mask_withdrawal.sum()} withdrawal rows")
    return df


def parse_mmex_csv(filepath: str) -> pd.DataFrame:
    """
    Full parse pipeline: detect encoding → detect separator → read →
    normalise columns → parse dates → normalise amounts.

    Parameters
    ----------
    filepath : str
        Path to the MMEX CSV export.

    Returns
    -------
    pd.DataFrame
        DataFrame with canonical column names ready for validation.
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

    logger.info(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    return df
