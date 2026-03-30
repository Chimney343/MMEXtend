"""Tests for src.ingestion.validator — validate_transactions and DataQualityReport."""

import pandas as pd
import pytest

from src.ingestion.validator import (
    CRITICAL_NULL_THRESHOLD,
    DataQualityReport,
    validate_transactions,
)


# ---- happy-path ---------------------------------------------------------------

def test_passing_report(sample_transactions):
    report = validate_transactions(sample_transactions)
    assert report.is_passing()


def test_rows_loaded(sample_transactions):
    report = validate_transactions(sample_transactions)
    assert report.rows_loaded == len(sample_transactions)


def test_date_range_populated(sample_transactions):
    report = validate_transactions(sample_transactions)
    assert report.date_min == "2024-01-01"
    assert report.date_max is not None
    assert report.date_max >= report.date_min


def test_null_rates_contain_all_columns(sample_transactions):
    report = validate_transactions(sample_transactions)
    for col in sample_transactions.columns:
        assert col in report.null_rates


def test_null_rates_zero_for_required_cols(sample_transactions):
    report = validate_transactions(sample_transactions)
    assert report.null_rates["date"] == 0.0
    assert report.null_rates["amount"] == 0.0


def test_uncategorised_count_zero(sample_transactions):
    report = validate_transactions(sample_transactions)
    assert report.uncategorised_count == 0
    assert report.uncategorised_pct == pytest.approx(0.0)


# ---- critical null failures ---------------------------------------------------

def test_null_amounts_critical_failure(null_amounts_df):
    report = validate_transactions(null_amounts_df)
    assert not report.is_passing()
    assert any("amount" in f for f in report.critical_failures)


def test_null_dates_critical_failure():
    df = pd.DataFrame(
        {
            "date": [None, None, None],
            "account": ["A", "B", "C"],
            "amount": [1.0, 2.0, 3.0],
            "type": ["deposit", "deposit", "deposit"],
        }
    )
    report = validate_transactions(df)
    assert not report.is_passing()
    assert any("date" in f for f in report.critical_failures)


def test_critical_null_threshold_constant():
    assert CRITICAL_NULL_THRESHOLD == 0.02


def test_below_threshold_passes():
    """Exactly at or below threshold: no critical failure."""
    n = 100
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"] * n),
            "account": ["Checking"] * n,
            "amount": [1.0] * (n - 1) + [float("nan")],  # 1 % null — below 2 %
            "type": ["deposit"] * n,
        }
    )
    report = validate_transactions(df)
    assert "amount" not in " ".join(report.critical_failures)


# ---- uncategorised transactions -----------------------------------------------

def test_uncategorised_none_category_detected():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "account": ["Checking", "Checking"],
            "amount": [100.0, -50.0],
            "type": ["deposit", "withdrawal"],
            "category": [None, "Food"],
        }
    )
    report = validate_transactions(df)
    assert report.uncategorised_count == 1
    assert report.uncategorised_pct == pytest.approx(50.0)


def test_uncategorised_blank_string_detected():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "account": ["Checking", "Checking"],
            "amount": [100.0, -50.0],
            "type": ["deposit", "withdrawal"],
            "category": ["Income", ""],
        }
    )
    report = validate_transactions(df)
    assert report.uncategorised_count == 1


def test_all_uncategorised():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "account": ["A", "B"],
            "amount": [1.0, 2.0],
            "type": ["deposit", "deposit"],
            "category": [None, ""],
        }
    )
    report = validate_transactions(df)
    assert report.uncategorised_count == 2
    assert report.uncategorised_pct == pytest.approx(100.0)


# ---- null account check -------------------------------------------------------

def test_null_account_detected():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "account": [None, "Checking"],
            "amount": [100.0, 50.0],
            "type": ["deposit", "deposit"],
        }
    )
    report = validate_transactions(df)
    assert report.null_account_count == 1


def test_blank_account_detected():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "account": ["   "],
            "amount": [100.0],
            "type": ["deposit"],
        }
    )
    report = validate_transactions(df)
    assert report.null_account_count == 1


# ---- currency check -----------------------------------------------------------

def test_single_currency(sample_transactions):
    report = validate_transactions(sample_transactions)
    assert report.currency_count == 1
    assert "USD" in report.currencies_found


def test_multiple_currencies_detected():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "account": ["A", "B"],
            "amount": [100.0, 200.0],
            "type": ["deposit", "deposit"],
            "currency": ["USD", "EUR"],
        }
    )
    report = validate_transactions(df)
    assert report.currency_count == 2
    assert set(report.currencies_found) == {"USD", "EUR"}


def test_no_currency_column():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "account": ["Checking"],
            "amount": [100.0],
            "type": ["deposit"],
        }
    )
    report = validate_transactions(df)
    assert report.currency_count == 0
    assert report.currencies_found == []


# ---- date gap detection -------------------------------------------------------

def test_date_gap_above_14_days_detected():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-04-01"]),  # ~91-day gap
            "account": ["Checking", "Checking"],
            "amount": [100.0, -50.0],
            "type": ["deposit", "withdrawal"],
        }
    )
    report = validate_transactions(df)
    assert len(report.date_gaps) > 0
    start, end, days = report.date_gaps[0]
    assert days > 14


def test_date_gap_below_threshold_not_reported():
    # Consecutive days — no gaps > 14 days
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=20, freq="D"),
            "account": ["Checking"] * 20,
            "amount": [1.0] * 20,
            "type": ["deposit"] * 20,
        }
    )
    report = validate_transactions(df)
    assert len(report.date_gaps) == 0


# ---- DataQualityReport display ------------------------------------------------

def test_display_runs_without_error(sample_transactions, capsys):
    report = validate_transactions(sample_transactions)
    report.display()
    captured = capsys.readouterr()
    assert "DATA QUALITY REPORT" in captured.out


def test_display_shows_critical_failure_label(null_amounts_df, capsys):
    report = validate_transactions(null_amounts_df)
    report.display()
    captured = capsys.readouterr()
    assert "CRITICAL" in captured.out


def test_display_shows_row_count(sample_transactions, capsys):
    report = validate_transactions(sample_transactions)
    report.display()
    captured = capsys.readouterr()
    assert str(len(sample_transactions)) in captured.out


# ---- DataQualityReport.is_passing ---------------------------------------------

def test_is_passing_empty_failures():
    report = DataQualityReport()
    assert report.is_passing()


def test_is_passing_with_failure():
    report = DataQualityReport(critical_failures=["null rate too high"])
    assert not report.is_passing()
