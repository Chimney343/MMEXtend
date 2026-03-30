"""
Shared pytest fixtures for the MMEX pipeline test suite.

Available fixtures:
    sample_transactions  — 14 months of synthetic transactions (passes validation)
    short_transactions   — 5 months of data (triggers insufficient_data in trends)
    null_amounts_df      — all-null amounts (triggers pipeline halt)
    tmp_mmb              — a real, minimal MMEX SQLite file written to tmp_path
"""

import sqlite3
from datetime import date, timedelta

import pandas as pd
import pytest


def _make_transactions(n_months: int, start: date = date(2024, 1, 1)) -> pd.DataFrame:
    """Generate one income + one expense row per month for n_months."""
    rows = []
    for i in range(n_months):
        month_start = date(start.year + (start.month + i - 1) // 12,
                           (start.month + i - 1) % 12 + 1, 1)
        rows.append({
            "date": pd.Timestamp(month_start),
            "account": "Checking",
            "to_account": None,
            "payee": "Employer",
            "category": "Income",
            "subcategory": "Salary",
            "amount": 3000.0,
            "type": "deposit",
            "currency": "USD",
            "notes": None,
            "transaction_id": i * 2,
        })
        rows.append({
            "date": pd.Timestamp(month_start + timedelta(days=5)),
            "account": "Checking",
            "to_account": None,
            "payee": "Supermarket",
            "category": "Food",
            "subcategory": "Groceries",
            "amount": -400.0,
            "type": "withdrawal",
            "currency": "USD",
            "notes": None,
            "transaction_id": i * 2 + 1,
        })
    return pd.DataFrame(rows)


@pytest.fixture
def sample_transactions() -> pd.DataFrame:
    """14 months of synthetic transactions — passes all validation checks."""
    return _make_transactions(14)


@pytest.fixture
def short_transactions() -> pd.DataFrame:
    """5 months — triggers insufficient_data for trend analysis."""
    return _make_transactions(5)


@pytest.fixture
def null_amounts_df(sample_transactions: pd.DataFrame) -> pd.DataFrame:
    """All-null amount column — triggers pipeline halt."""
    df = sample_transactions.copy()
    df["amount"] = float("nan")
    return df


@pytest.fixture
def tmp_mmb(tmp_path: "pytest.TempPathFactory") -> str:
    """
    Write a minimal, schema-correct MMEX SQLite file to a temporary directory.
    Returns the absolute path as a string.

    Schema matches MMEX v21 (tables.sql) so it is compatible with
    parse_mmex_sqlite() and get_account_balances().
    """
    mmb_path = tmp_path / "test.mmb"
    conn = sqlite3.connect(str(mmb_path))
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE CHECKINGACCOUNT_V1 (
            TRANSID           INTEGER PRIMARY KEY,
            TRANSDATE         TEXT,
            ACCOUNTID         INTEGER,
            TOACCOUNTID       INTEGER,
            PAYEEID           INTEGER,
            CATEGID           INTEGER,
            TRANSAMOUNT       REAL,
            TRANSCODE         TEXT,
            STATUS            TEXT,
            DELETEDTIME       TEXT,
            NOTES             TEXT,
            TOTRANSAMOUNT     REAL,
            TRANSACTIONNUMBER TEXT
        );
        CREATE TABLE ACCOUNTLIST_V1 (
            ACCOUNTID   INTEGER PRIMARY KEY,
            ACCOUNTNAME TEXT,
            CURRENCYID  INTEGER,
            STATUS      TEXT,
            INITIALBAL  REAL
        );
        CREATE TABLE PAYEE_V1 (PAYEEID INTEGER PRIMARY KEY, PAYEENAME TEXT);
        CREATE TABLE CATEGORY_V1 (
            CATEGID  INTEGER PRIMARY KEY,
            CATEGNAME TEXT,
            PARENTID  INTEGER DEFAULT -1
        );
        CREATE TABLE SPLITTRANSACTIONS_V1 (
            SPLITTRANSID      INTEGER PRIMARY KEY,
            TRANSID           INTEGER,
            CATEGID           INTEGER,
            SPLITTRANSAMOUNT  REAL,
            NOTES             TEXT
        );
        CREATE TABLE CURRENCYFORMATS_V1 (
            CURRENCYID      INTEGER PRIMARY KEY,
            CURRENCY_SYMBOL TEXT
        );
        CREATE TABLE INFOTABLE_V1 (INFONAME TEXT, INFOVALUE TEXT);
    """)

    cursor.executemany("INSERT INTO ACCOUNTLIST_V1 VALUES (?,?,?,?,?)", [
        (1, "Checking", 1, "Open", 0.0),
    ])
    cursor.execute("INSERT INTO PAYEE_V1 VALUES (1, 'Employer')")
    cursor.execute("INSERT INTO CATEGORY_V1 VALUES (1, 'Income', -1)")
    cursor.execute("INSERT INTO CURRENCYFORMATS_V1 VALUES (1, 'USD')")
    cursor.execute("INSERT INTO INFOTABLE_V1 VALUES ('DATAVERSION', '3')")

    cursor.executemany(
        "INSERT INTO CHECKINGACCOUNT_V1 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, "2024-01-01", 1, None, 1, 1, 3000.0, "Deposit",    None, None, None, None, None),
            (2, "2024-02-01", 1, None, 1, 1, 3000.0, "Deposit",    None, None, None, None, None),
        ],
    )
    conn.commit()
    conn.close()
    return str(mmb_path)
