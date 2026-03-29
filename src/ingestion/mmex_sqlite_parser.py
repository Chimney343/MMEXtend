"""
Parse MMEX SQLite database (.mmb) into a validated DataFrame.

Usage:
    from src.ingestion.mmex_sqlite_parser import parse_mmex_sqlite
    df = parse_mmex_sqlite("data/raw/my_finances.mmb")
"""

import sqlite3
from pathlib import Path

import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

TRANSACTION_QUERY = """
SELECT
    t.TRANSID           AS transaction_id,
    t.TRANSDATE         AS date,
    a.ACCOUNTNAME       AS account,
    a2.ACCOUNTNAME      AS to_account,
    p.PAYEENAME         AS payee,
    c.CATEGNAME         AS category,
    sc.SUBCATEGNAME     AS subcategory,
    t.TRANSAMOUNT       AS amount,
    t.TRANSCODE         AS type,
    t.NOTES             AS notes,
    t.TOAMOUNT          AS to_amount,
    cur.CURRENCY_SYMBOL AS currency
FROM CHECKINGACCOUNT_V1 t
LEFT JOIN ACCOUNTLIST_V1 a    ON t.ACCOUNTID    = a.ACCOUNTID
LEFT JOIN ACCOUNTLIST_V1 a2   ON t.TOACCOUNTID  = a2.ACCOUNTID
LEFT JOIN PAYEE_V1 p          ON t.PAYEEID       = p.PAYEEID
LEFT JOIN CATEGORY_V1 c       ON t.CATEGID       = c.CATEGID
LEFT JOIN SUBCATEGORY_V1 sc   ON t.SUBCATEGID    = sc.SUBCATEGID
LEFT JOIN ACCOUNTLIST_V1 acur ON t.ACCOUNTID     = acur.ACCOUNTID
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
    """
    Read MMEX .mmb SQLite file and return a normalised transaction DataFrame.

    Parameters
    ----------
    filepath : str
        Path to the .mmb file.

    Returns
    -------
    pd.DataFrame
        DataFrame with canonical column names ready for validation.
    """
    filepath = str(Path(filepath).resolve())
    logger.info(f"Parsing MMEX SQLite: {filepath}")

    conn = sqlite3.connect(filepath)
    try:
        df = pd.read_sql_query(TRANSACTION_QUERY, conn)
        logger.info(f"Loaded {len(df)} transactions from SQLite")

        df["date"] = pd.to_datetime(df["date"])

        # MMEX stores all amounts as positive; type indicates direction
        mask_withdrawal = df["type"].str.lower() == "withdrawal"
        df.loc[mask_withdrawal, "amount"] *= -1
        logger.info(f"Sign-flipped {mask_withdrawal.sum()} withdrawal rows")

        # Normalise type to lowercase
        df["type"] = df["type"].str.lower()

        logger.info(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
        return df
    finally:
        conn.close()


def get_account_balances(filepath: str) -> pd.DataFrame:
    """
    Extract opening balances per account from MMEX SQLite.

    Parameters
    ----------
    filepath : str
        Path to the .mmb file.

    Returns
    -------
    pd.DataFrame
        Columns: account, opening_balance, status.
    """
    filepath = str(Path(filepath).resolve())
    conn = sqlite3.connect(filepath)
    try:
        balances = pd.read_sql_query(ACCOUNT_BALANCES_QUERY, conn)
        logger.info(f"Found {len(balances)} open accounts with initial balances")
        return balances
    finally:
        conn.close()
