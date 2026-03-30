"""Tests for src.ingestion.mmex_sqlite_parser."""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.mmex_sqlite_parser import get_account_balances, parse_mmex_sqlite


# ---------------------------------------------------------------------------
# Schema-correct MMEX SQLite factory
# ---------------------------------------------------------------------------

def _create_mmb(
    path: Path,
    *,
    include_transfer: bool = False,
    include_splits: bool = False,
    data_version: str = "3",
) -> None:
    """Create a minimal but schema-correct MMEX SQLite file."""
    conn = sqlite3.connect(str(path))
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE CHECKINGACCOUNT_V1 (
            TRANSID         INTEGER PRIMARY KEY,
            TRANSDATE       TEXT,
            ACCOUNTID       INTEGER,
            TOACCOUNTID     INTEGER,
            PAYEEID         INTEGER,
            CATEGID         INTEGER,
            TRANSAMOUNT     REAL,
            TRANSCODE       TEXT,
            STATUS          TEXT,
            DELETEDTIME     TEXT,
            NOTES           TEXT,
            TOTRANSAMOUNT   REAL,
            TRANSACTIONNUMBER TEXT
        );
        CREATE TABLE ACCOUNTLIST_V1 (
            ACCOUNTID   INTEGER PRIMARY KEY,
            ACCOUNTNAME TEXT,
            CURRENCYID  INTEGER,
            STATUS      TEXT,
            INITIALBAL  REAL
        );
        CREATE TABLE PAYEE_V1   (PAYEEID INTEGER PRIMARY KEY, PAYEENAME TEXT);
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
            CURRENCY_SYMBOL TEXT,
            BASECONVRATE    REAL DEFAULT 1.0
        );
        CREATE TABLE CURRENCYHISTORY_V1 (
            CURRHISTID  INTEGER PRIMARY KEY,
            CURRENCYID  INTEGER NOT NULL,
            CURRDATE    TEXT NOT NULL,
            CURRVALUE   REAL NOT NULL
        );
        CREATE TABLE INFOTABLE_V1 (INFONAME TEXT, INFOVALUE TEXT);
    """)

    c.executemany("INSERT INTO ACCOUNTLIST_V1 VALUES (?,?,?,?,?)", [
        (1, "Checking", 1, "Open", 0.0),
        (2, "Savings",  1, "Open", 5000.0),
    ])
    c.execute("INSERT INTO PAYEE_V1 VALUES (1, 'Employer')")
    # USD is base currency (BASECONVRATE = 1.0); no conversion → amount_pln == amount
    c.execute("INSERT INTO CURRENCYFORMATS_V1 VALUES (1, 'USD', 1.0)")
    c.execute(f"INSERT INTO INFOTABLE_V1 VALUES ('DATAVERSION', '{data_version}')")
    c.execute("INSERT INTO INFOTABLE_V1 VALUES ('BASECURRENCYID', '1')")

    # Top-level categories (PARENTID = -1)
    c.execute("INSERT INTO CATEGORY_V1 VALUES (1, 'Income', -1)")
    c.execute("INSERT INTO CATEGORY_V1 VALUES (2, 'Food',   -1)")

    # Base transactions: 1 deposit + 1 withdrawal
    c.execute("INSERT INTO CHECKINGACCOUNT_V1 VALUES (1,'2024-01-01',1,NULL,1,1,3000.0,'Deposit',NULL,NULL,NULL,NULL,NULL)")
    c.execute("INSERT INTO CHECKINGACCOUNT_V1 VALUES (2,'2024-02-01',1,NULL,1,2,400.0,'Withdrawal',NULL,NULL,NULL,NULL,NULL)")

    if include_transfer:
        # Transfer from Checking (id=1) to Savings (id=2)
        c.execute("INSERT INTO CHECKINGACCOUNT_V1 VALUES (3,'2024-03-01',1,2,1,NULL,500.0,'Transfer',NULL,NULL,NULL,500.0,NULL)")

    if include_splits:
        # Parent row with null CATEGID (triggers split expansion)
        c.execute("INSERT INTO CHECKINGACCOUNT_V1 VALUES (4,'2024-04-01',1,NULL,1,NULL,800.0,'Withdrawal',NULL,NULL,NULL,NULL,NULL)")
        c.execute("INSERT INTO SPLITTRANSACTIONS_V1 VALUES (1,4,1,300.0,'split-income')")
        c.execute("INSERT INTO SPLITTRANSACTIONS_V1 VALUES (2,4,2,500.0,'split-food')")

    conn.commit()
    conn.close()


@pytest.fixture
def mmb_path(tmp_path) -> str:
    p = tmp_path / "test.mmb"
    _create_mmb(p)
    return str(p)


@pytest.fixture
def mmb_with_transfer(tmp_path) -> str:
    p = tmp_path / "transfer.mmb"
    _create_mmb(p, include_transfer=True)
    return str(p)


@pytest.fixture
def mmb_with_splits(tmp_path) -> str:
    p = tmp_path / "splits.mmb"
    _create_mmb(p, include_splits=True)
    return str(p)


# ---------------------------------------------------------------------------
# parse_mmex_sqlite — basic
# ---------------------------------------------------------------------------

def test_returns_dataframe(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    assert isinstance(df, pd.DataFrame)


def test_row_count_basic(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    assert len(df) == 2  # 1 deposit + 1 withdrawal


def test_date_column_is_datetime(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_type_column_lowercase(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    assert df["type"].str.islower().all()


def test_account_name_resolved(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    assert (df["account"] == "Checking").all()


def test_category_resolved(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    categories = set(df["category"].dropna())
    assert "Income" in categories
    assert "Food" in categories


def test_currency_resolved(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    assert (df["currency"] == "USD").all()


# ---------------------------------------------------------------------------
# Sign convention
# ---------------------------------------------------------------------------

def test_deposit_positive(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    deposits = df[df["type"] == "deposit"]
    assert (deposits["amount"] > 0).all()


def test_withdrawal_negative(mmb_path):
    df = parse_mmex_sqlite(mmb_path)
    withdrawals = df[df["type"] == "withdrawal"]
    assert (withdrawals["amount"] < 0).all()


def test_transfer_amount_negative(mmb_with_transfer):
    df = parse_mmex_sqlite(mmb_with_transfer)
    transfers = df[df["type"] == "transfer"]
    assert len(transfers) == 1
    assert transfers["amount"].iloc[0] < 0


def test_transfer_total_row_count(mmb_with_transfer):
    df = parse_mmex_sqlite(mmb_with_transfer)
    # deposit + withdrawal + transfer = 3, but transfer has null category → expanded if splits exist
    # here there are no splits for the transfer, so it stays as 1 row
    assert len(df) == 3


# ---------------------------------------------------------------------------
# Split transactions
# ---------------------------------------------------------------------------

def test_split_expansion_produces_correct_row_count(mmb_with_splits):
    df = parse_mmex_sqlite(mmb_with_splits)
    # deposit(1) + withdrawal(2) + split-line-a(4) + split-line-b(4) = 4
    assert len(df) == 4


def test_split_lines_have_categories(mmb_with_splits):
    df = parse_mmex_sqlite(mmb_with_splits)
    # All rows should have a resolved category after expansion
    # (the parent row with null category is replaced by its split lines)
    assert df["category"].notna().all()


def test_split_amounts_negative_for_withdrawal_parent(mmb_with_splits):
    df = parse_mmex_sqlite(mmb_with_splits)
    # The split lines inherit type=withdrawal → amounts should be negative
    split_rows = df[df["transaction_id"] == 4]
    assert (split_rows["amount"] < 0).all()


# ---------------------------------------------------------------------------
# Void / deleted filtering
# ---------------------------------------------------------------------------

def test_void_transactions_excluded(tmp_path):
    p = tmp_path / "void.mmb"
    _create_mmb(p)
    conn = sqlite3.connect(str(p))
    conn.execute(
        "INSERT INTO CHECKINGACCOUNT_V1 VALUES (10,'2024-05-01',1,NULL,1,1,999.0,'Deposit','Void',NULL,NULL,NULL,NULL)"
    )
    conn.commit()
    conn.close()

    df = parse_mmex_sqlite(str(p))
    assert 10 not in df["transaction_id"].values


def test_deleted_transactions_excluded(tmp_path):
    p = tmp_path / "deleted.mmb"
    _create_mmb(p)
    conn = sqlite3.connect(str(p))
    conn.execute(
        "INSERT INTO CHECKINGACCOUNT_V1 VALUES (11,'2024-05-01',1,NULL,1,1,999.0,'Deposit',NULL,'2024-05-02',NULL,NULL,NULL)"
    )
    conn.commit()
    conn.close()

    df = parse_mmex_sqlite(str(p))
    assert 11 not in df["transaction_id"].values


# ---------------------------------------------------------------------------
# Schema version warning (smoke — just ensure no crash)
# ---------------------------------------------------------------------------

def test_unexpected_schema_version_does_not_raise(tmp_path):
    p = tmp_path / "v99.mmb"
    _create_mmb(p, data_version="99")
    # Should not raise even though version is unexpected
    df = parse_mmex_sqlite(str(p))
    assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# get_account_balances
# ---------------------------------------------------------------------------

def test_get_account_balances_returns_dataframe(mmb_path):
    df = get_account_balances(mmb_path)
    assert isinstance(df, pd.DataFrame)


def test_get_account_balances_columns(mmb_path):
    df = get_account_balances(mmb_path)
    assert "account" in df.columns
    assert "opening_balance" in df.columns
    assert "status" in df.columns


# ---------------------------------------------------------------------------
# Currency conversion (amount_pln column)
# ---------------------------------------------------------------------------

def test_amount_pln_column_present(mmb_path):
    """parse_mmex_sqlite must always produce an amount_pln column."""
    df = parse_mmex_sqlite(mmb_path)
    assert "amount_pln" in df.columns


def test_amount_pln_equals_amount_for_base_currency(mmb_path):
    """When the account currency equals the base currency, amount_pln == amount."""
    df = parse_mmex_sqlite(mmb_path)
    # The test DB uses USD as both the account currency and base currency
    import numpy.testing as npt
    npt.assert_array_almost_equal(df["amount_pln"].values, df["amount"].values)


def test_get_account_balances_only_open_accounts(mmb_path):
    df = get_account_balances(mmb_path)
    assert (df["status"] == "Open").all()


def test_get_account_balances_values(mmb_path):
    df = get_account_balances(mmb_path)
    savings = df[df["account"] == "Savings"]
    assert len(savings) == 1
    assert savings["opening_balance"].iloc[0] == pytest.approx(5000.0)


def test_get_account_balances_closed_excluded(tmp_path):
    p = tmp_path / "closed.mmb"
    _create_mmb(p)
    conn = sqlite3.connect(str(p))
    conn.execute("INSERT INTO ACCOUNTLIST_V1 VALUES (99,'OldAccount',1,'Closed',0.0)")
    conn.commit()
    conn.close()

    df = get_account_balances(str(p))
    assert "OldAccount" not in df["account"].values


# ---------------------------------------------------------------------------
# _check_schema_version — except branch (L95-96): table missing
# ---------------------------------------------------------------------------

from src.ingestion.mmex_sqlite_parser import _check_schema_version


def test_check_schema_version_handles_missing_infotable(tmp_path):
    """When INFOTABLE_V1 doesn't exist the function logs a warning and does not raise."""
    p = tmp_path / "no_infotable.mmb"
    conn = sqlite3.connect(str(p))
    # Create an empty DB — no tables
    conn.commit()
    conn.close()

    # Must not raise; the except branch logs the error and returns
    check_conn = sqlite3.connect(str(p))
    try:
        _check_schema_version(check_conn)  # raises OperationalError internally, caught
    finally:
        check_conn.close()


# ---------------------------------------------------------------------------
# _expand_splits — deep nesting warning (L142)
# ---------------------------------------------------------------------------


def _create_mmb_with_deep_categories(path: Path) -> None:
    """MMEX file with 3-level category nesting + a split transaction."""
    conn = sqlite3.connect(str(path))
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE CHECKINGACCOUNT_V1 (
            TRANSID INTEGER PRIMARY KEY, TRANSDATE TEXT,
            ACCOUNTID INTEGER, TOACCOUNTID INTEGER, PAYEEID INTEGER,
            CATEGID INTEGER, TRANSAMOUNT REAL, TRANSCODE TEXT,
            STATUS TEXT, DELETEDTIME TEXT, NOTES TEXT,
            TOTRANSAMOUNT REAL, TRANSACTIONNUMBER TEXT
        );
        CREATE TABLE ACCOUNTLIST_V1 (
            ACCOUNTID INTEGER PRIMARY KEY, ACCOUNTNAME TEXT,
            CURRENCYID INTEGER, STATUS TEXT, INITIALBAL REAL
        );
        CREATE TABLE PAYEE_V1 (PAYEEID INTEGER PRIMARY KEY, PAYEENAME TEXT);
        CREATE TABLE CATEGORY_V1 (
            CATEGID INTEGER PRIMARY KEY, CATEGNAME TEXT,
            PARENTID INTEGER DEFAULT -1
        );
        CREATE TABLE SPLITTRANSACTIONS_V1 (
            SPLITTRANSID INTEGER PRIMARY KEY, TRANSID INTEGER,
            CATEGID INTEGER, SPLITTRANSAMOUNT REAL, NOTES TEXT
        );
        CREATE TABLE CURRENCYFORMATS_V1 (
            CURRENCYID INTEGER PRIMARY KEY,
            CURRENCY_SYMBOL TEXT, BASECONVRATE REAL DEFAULT 1.0
        );
        CREATE TABLE CURRENCYHISTORY_V1 (
            CURRHISTID INTEGER PRIMARY KEY, CURRENCYID INTEGER,
            CURRDATE TEXT, CURRVALUE REAL
        );
        CREATE TABLE INFOTABLE_V1 (INFONAME TEXT, INFOVALUE TEXT);
    """)

    c.execute("INSERT INTO ACCOUNTLIST_V1 VALUES (1,'Checking',1,'Open',0.0)")
    c.execute("INSERT INTO PAYEE_V1 VALUES (1,'Employer')")
    c.execute("INSERT INTO CURRENCYFORMATS_V1 VALUES (1,'PLN',1.0)")
    c.execute("INSERT INTO INFOTABLE_V1 VALUES ('DATAVERSION','3')")
    c.execute("INSERT INTO INFOTABLE_V1 VALUES ('BASECURRENCYID','1')")

    # 3-level nesting: Root(1) → Parent(2) → Child(3)
    c.execute("INSERT INTO CATEGORY_V1 VALUES (1,'Root',-1)")
    c.execute("INSERT INTO CATEGORY_V1 VALUES (2,'Parent',1)")
    c.execute("INSERT INTO CATEGORY_V1 VALUES (3,'Child',2)")

    # Parent transaction with null CATEGID → triggers split expansion
    c.execute(
        "INSERT INTO CHECKINGACCOUNT_V1 VALUES "
        "(1,'2024-01-01',1,NULL,1,NULL,500.0,'Withdrawal',NULL,NULL,NULL,NULL,NULL)"
    )
    # Split lines reference the deeply nested Child category
    c.execute("INSERT INTO SPLITTRANSACTIONS_V1 VALUES (1,1,3,300.0,'deep-split')")
    c.execute("INSERT INTO SPLITTRANSACTIONS_V1 VALUES (2,1,1,200.0,'root-split')")

    conn.commit()
    conn.close()


def test_deep_category_nesting_warning_does_not_raise(tmp_path):
    """parse_mmex_sqlite warns about >2-level nesting but still returns a DataFrame."""
    p = tmp_path / "deep.mmb"
    _create_mmb_with_deep_categories(p)
    df = parse_mmex_sqlite(str(p))
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 1
