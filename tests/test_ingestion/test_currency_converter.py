"""Tests for src.ingestion.currency_converter."""

import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest

from src.ingestion.currency_converter import convert_to_base

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE = "PLN"


def _make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).assign(
        date=lambda df: pd.to_datetime(df["date"]),
        amount=lambda df: df["amount"].astype(float),
    )


def _make_history(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["currency", "rate_date", "rate"])
    return pd.DataFrame(rows).assign(
        rate_date=lambda df: pd.to_datetime(df["rate_date"]),
        rate=lambda df: df["rate"].astype(float),
    )


# ---------------------------------------------------------------------------
# PLN (base currency) rows — no conversion
# ---------------------------------------------------------------------------

class TestBaseCurrencyPassthrough:
    def test_pln_deposit_unchanged(self):
        df = _make_df([{"date": "2024-01-15", "currency": "PLN", "amount": 1000.0}])
        result = convert_to_base(df, BASE, _make_history([]), {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 1000.0)

    def test_pln_withdrawal_sign_preserved(self):
        df = _make_df([{"date": "2024-02-15", "currency": "PLN", "amount": -500.0}])
        result = convert_to_base(df, BASE, _make_history([]), {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], -500.0)

    def test_original_amount_column_not_modified(self):
        df = _make_df([{"date": "2024-01-15", "currency": "PLN", "amount": 1000.0}])
        result = convert_to_base(df, BASE, _make_history([]), {})
        npt.assert_almost_equal(result["amount"].iloc[0], 1000.0)

    def test_amount_pln_column_added(self):
        df = _make_df([{"date": "2024-01-15", "currency": "PLN", "amount": 1000.0}])
        result = convert_to_base(df, BASE, _make_history([]), {})
        assert "amount_pln" in result.columns


# ---------------------------------------------------------------------------
# Historical rate lookup
# ---------------------------------------------------------------------------

class TestHistoricalRateLookup:
    def test_uses_most_recent_rate_before_transaction(self):
        # History: Jan rate 4.20, Mar rate 4.30, Apr rate 4.40
        # Transaction on Mar 10 → should use the Mar 01 rate (4.30)
        df = _make_df([{"date": "2024-03-10", "currency": "EUR", "amount": 100.0}])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.20},
            {"currency": "EUR", "rate_date": "2024-03-01", "rate": 4.30},
            {"currency": "EUR", "rate_date": "2024-04-01", "rate": 4.40},
        ])
        result = convert_to_base(df, BASE, history, {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 430.0)

    def test_exact_date_match_uses_that_rate(self):
        df = _make_df([{"date": "2024-03-01", "currency": "EUR", "amount": 100.0}])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-03-01", "rate": 4.30},
        ])
        result = convert_to_base(df, BASE, history, {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 430.0)

    def test_negative_amount_sign_preserved(self):
        df = _make_df([{"date": "2024-01-15", "currency": "EUR", "amount": -200.0}])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.25},
        ])
        result = convert_to_base(df, BASE, history, {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], -850.0)

    def test_multiple_transactions_different_rates(self):
        df = _make_df([
            {"date": "2024-01-15", "currency": "EUR", "amount": 100.0},
            {"date": "2024-06-15", "currency": "EUR", "amount": 100.0},
        ])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.20},
            {"currency": "EUR", "rate_date": "2024-06-01", "rate": 4.35},
        ])
        result = convert_to_base(df, BASE, history, {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 420.0)
        npt.assert_almost_equal(result["amount_pln"].iloc[1], 435.0)


# ---------------------------------------------------------------------------
# Static rate fallback
# ---------------------------------------------------------------------------

class TestStaticRateFallback:
    def test_uses_static_when_no_preceding_history(self):
        # Transaction is BEFORE any history entry → static fallback
        df = _make_df([{"date": "2023-12-01", "currency": "EUR", "amount": 100.0}])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.25},
        ])
        static = {"EUR": 4.15}
        result = convert_to_base(df, BASE, history, static)
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 415.0)

    def test_uses_static_when_no_history_rows_at_all(self):
        df = _make_df([{"date": "2024-01-15", "currency": "EUR", "amount": 100.0}])
        static = {"EUR": 4.20}
        result = convert_to_base(df, BASE, _make_history([]), static)
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 420.0)

    def test_rate_1_applied_when_no_data_at_all(self):
        # No history, no static → rate 1.0 (no conversion, just a warning)
        df = _make_df([{"date": "2024-01-15", "currency": "USD", "amount": 100.0}])
        result = convert_to_base(df, BASE, _make_history([]), {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 100.0)

    def test_history_takes_precedence_over_static(self):
        # History has a rate for the exact date — should NOT fall back to static
        df = _make_df([{"date": "2024-03-01", "currency": "EUR", "amount": 100.0}])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-03-01", "rate": 4.30},
        ])
        static = {"EUR": 4.00}  # static is lower — should NOT be used
        result = convert_to_base(df, BASE, history, static)
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 430.0)


# ---------------------------------------------------------------------------
# Mixed-currency DataFrame
# ---------------------------------------------------------------------------

class TestMixedCurrencies:
    def test_pln_and_eur_rows(self):
        df = _make_df([
            {"date": "2024-01-15", "currency": "PLN", "amount": 1000.0},
            {"date": "2024-01-15", "currency": "EUR", "amount": 100.0},
        ])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.25},
        ])
        result = convert_to_base(df, BASE, history, {})
        npt.assert_almost_equal(result["amount_pln"].iloc[0], 1000.0)   # PLN unchanged
        npt.assert_almost_equal(result["amount_pln"].iloc[1], 425.0)    # EUR converted

    def test_original_df_not_mutated(self):
        df = _make_df([{"date": "2024-01-15", "currency": "EUR", "amount": 100.0}])
        history = _make_history([
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.25},
        ])
        _ = convert_to_base(df, BASE, history, {})
        assert "amount_pln" not in df.columns   # original must not be modified

    def test_output_row_count_unchanged(self):
        df = _make_df([
            {"date": "2024-01-15", "currency": "PLN", "amount": 500.0},
            {"date": "2024-01-16", "currency": "EUR", "amount": 200.0},
            {"date": "2024-01-17", "currency": "PLN", "amount": -300.0},
        ])
        result = convert_to_base(df, BASE, _make_history([]), {"EUR": 4.20})
        assert len(result) == 3


# ---------------------------------------------------------------------------
# load_rates_from_mmb — branch coverage for error paths (L111-115, L122-124)
# ---------------------------------------------------------------------------

import sqlite3 as _sqlite3
from pathlib import Path as _Path

from src.ingestion.currency_converter import load_rates_from_mmb


def _make_minimal_mmb(path: _Path, *, with_currency_history: bool = True, with_currency_formats: bool = True) -> None:
    """Create a minimal MMEX SQLite file for load_rates_from_mmb tests."""
    conn = _sqlite3.connect(str(path))
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE INFOTABLE_V1 (INFONAME TEXT, INFOVALUE TEXT);
    """)
    c.execute("INSERT INTO INFOTABLE_V1 VALUES ('BASECURRENCYID', '1')")

    if with_currency_formats:
        c.executescript("""
            CREATE TABLE CURRENCYFORMATS_V1 (
                CURRENCYID      INTEGER PRIMARY KEY,
                CURRENCY_SYMBOL TEXT,
                BASECONVRATE    REAL DEFAULT 1.0
            );
        """)
        c.execute("INSERT INTO CURRENCYFORMATS_V1 VALUES (1, 'PLN', 1.0)")
        c.execute("INSERT INTO CURRENCYFORMATS_V1 VALUES (2, 'EUR', 4.25)")

    if with_currency_history:
        c.executescript("""
            CREATE TABLE CURRENCYHISTORY_V1 (
                CURRHISTID  INTEGER PRIMARY KEY,
                CURRENCYID  INTEGER NOT NULL,
                CURRDATE    TEXT NOT NULL,
                CURRVALUE   REAL NOT NULL
            );
        """)

    conn.commit()
    conn.close()


class TestLoadRatesFromMmb:
    def test_corrupted_rate_is_discarded(self, tmp_path):
        """Rates > 1_000_000 in CURRENCYHISTORY_V1 are silently dropped."""
        p = tmp_path / "corrupted.mmb"
        _make_minimal_mmb(p)
        conn = _sqlite3.connect(str(p))
        conn.execute("INSERT INTO CURRENCYHISTORY_V1 VALUES (1, 2, '2024-01-01', 5000000.0)")
        conn.execute("INSERT INTO CURRENCYHISTORY_V1 VALUES (2, 2, '2024-02-01', 4.25)")
        conn.commit()
        conn.close()

        _, history, _ = load_rates_from_mmb(str(p))
        # The corrupted row (5000000.0) must be gone; the valid row must remain
        assert (history["rate"] <= 1_000_000).all()
        assert len(history) == 1

    def test_corrupted_rate_only_entry_returns_empty_history(self, tmp_path):
        p = tmp_path / "corrupted_only.mmb"
        _make_minimal_mmb(p)
        conn = _sqlite3.connect(str(p))
        conn.execute("INSERT INTO CURRENCYHISTORY_V1 VALUES (1, 2, '2024-01-01', 9999999.0)")
        conn.commit()
        conn.close()

        _, history, _ = load_rates_from_mmb(str(p))
        assert len(history) == 0

    def test_missing_currency_history_table_returns_empty_history(self, tmp_path):
        """When CURRENCYHISTORY_V1 does not exist, history is empty (no exception raised)."""
        p = tmp_path / "no_history.mmb"
        _make_minimal_mmb(p, with_currency_history=False)

        _, history, _ = load_rates_from_mmb(str(p))
        assert len(history) == 0
        assert list(history.columns) == ["currency", "rate_date", "rate"]

    def test_missing_baseconvrate_column_returns_empty_static(self, tmp_path):
        """When CURRENCYFORMATS_V1 has no BASECONVRATE column, static dict is empty."""
        p = tmp_path / "no_baseconvrate.mmb"
        conn = _sqlite3.connect(str(p))
        # CURRENCYFORMATS_V1 without BASECONVRATE — base-currency JOIN still works
        conn.executescript("""
            CREATE TABLE CURRENCYFORMATS_V1 (
                CURRENCYID INTEGER PRIMARY KEY, CURRENCY_SYMBOL TEXT
            );
            CREATE TABLE INFOTABLE_V1 (INFONAME TEXT, INFOVALUE TEXT);
            CREATE TABLE CURRENCYHISTORY_V1 (
                CURRHISTID INTEGER PRIMARY KEY, CURRENCYID INTEGER,
                CURRDATE TEXT, CURRVALUE REAL
            );
        """)
        conn.execute("INSERT INTO CURRENCYFORMATS_V1 VALUES (1, 'PLN')")
        conn.execute("INSERT INTO INFOTABLE_V1 VALUES ('BASECURRENCYID', '1')")
        conn.commit()
        conn.close()

        _, _, static = load_rates_from_mmb(str(p))
        assert static == {}
