"""Tests for src.ingestion.mmex_csv_parser."""

from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.mmex_csv_parser import (
    detect_encoding,
    detect_separator,
    normalise_amounts,
    normalise_columns,
    parse_dates,
    parse_mmex_csv,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(tmp_path: Path, content: str, filename: str = "test.csv", encoding: str = "utf-8") -> Path:
    p = tmp_path / filename
    p.write_text(content, encoding=encoding)
    return p


_SAMPLE_CSV = (
    "Date,Account,ToAccount,Payee,Category,Subcategory,Amount,TransCode,Currency,Notes\n"
    "2024-01-01,Checking,,Employer,Income,Salary,3000.00,Deposit,USD,\n"
    "2024-01-06,Checking,,Supermarket,Food,Groceries,400.00,Withdrawal,USD,\n"
)


# ---------------------------------------------------------------------------
# detect_encoding
# ---------------------------------------------------------------------------

def test_detect_encoding_utf8_returns_something(tmp_path):
    p = _write_csv(tmp_path, "date,amount\n2024-01-01,100\n")
    enc = detect_encoding(str(p))
    assert enc is not None
    assert isinstance(enc, str)


def test_detect_encoding_latin1(tmp_path):
    content = "date,amount,payee\n2024-01-01,100,Café\n"
    p = tmp_path / "latin.csv"
    p.write_bytes(content.encode("latin-1"))
    enc = detect_encoding(str(p))
    # chardet may report iso-8859-1, windows-1252, latin-1 etc.
    assert enc is not None


# ---------------------------------------------------------------------------
# detect_separator
# ---------------------------------------------------------------------------

def test_detect_separator_comma(tmp_path):
    p = _write_csv(tmp_path, "date,amount,payee\n2024-01-01,100,Shop\n")
    assert detect_separator(str(p), "utf-8") == ","


def test_detect_separator_semicolon(tmp_path):
    p = _write_csv(tmp_path, "date;amount;payee\n2024-01-01;100;Shop\n")
    assert detect_separator(str(p), "utf-8") == ";"


def test_detect_separator_tab(tmp_path):
    p = _write_csv(tmp_path, "date\tamount\tpayee\n2024-01-01\t100\tShop\n")
    assert detect_separator(str(p), "utf-8") == "\t"


# ---------------------------------------------------------------------------
# parse_dates
# ---------------------------------------------------------------------------

def test_parse_dates_iso():
    result = parse_dates(pd.Series(["2024-01-15", "2024-02-28"]))
    assert result.iloc[0] == pd.Timestamp("2024-01-15")
    assert result.iloc[1] == pd.Timestamp("2024-02-28")


def test_parse_dates_dmy():
    result = parse_dates(pd.Series(["15/01/2024", "28/02/2024"]))
    assert result.iloc[0] == pd.Timestamp("2024-01-15")
    assert result.iloc[1] == pd.Timestamp("2024-02-28")


def test_parse_dates_mdy():
    result = parse_dates(pd.Series(["01/15/2024", "02/28/2024"]))
    assert result.iloc[0].month == 1
    assert result.iloc[0].day == 15


def test_parse_dates_ymd_slash():
    result = parse_dates(pd.Series(["2024/03/25"]))
    assert result.iloc[0] == pd.Timestamp("2024-03-25")


def test_parse_dates_dmy_dash():
    result = parse_dates(pd.Series(["25-03-2024"]))
    assert result.iloc[0] == pd.Timestamp("2024-03-25")


# ---------------------------------------------------------------------------
# normalise_columns
# ---------------------------------------------------------------------------

def test_normalise_columns_renames_transdate():
    df = pd.DataFrame({"TransDate": ["2024-01-01"], "TransAMOUNT": ["100"]})
    result = normalise_columns(df)
    assert "date" in result.columns
    assert "amount" in result.columns


def test_normalise_columns_renames_transcode():
    df = pd.DataFrame({"TransCode": ["Deposit"]})
    result = normalise_columns(df)
    assert "type" in result.columns


def test_normalise_columns_renames_payee_name():
    df = pd.DataFrame({"Payee_Name": ["Shop"]})
    result = normalise_columns(df)
    assert "payee" in result.columns


def test_normalise_columns_renames_category_name():
    df = pd.DataFrame({"Category_Name": ["Food"]})
    result = normalise_columns(df)
    assert "category" in result.columns


def test_normalise_columns_strips_spaces():
    df = pd.DataFrame({" Date ": ["2024-01-01"]})
    result = normalise_columns(df)
    assert "date" in result.columns


def test_normalise_columns_unknown_columns_preserved():
    df = pd.DataFrame({"TransDate": ["2024-01-01"], "custom_col": [42]})
    result = normalise_columns(df)
    assert "custom_col" in result.columns


# ---------------------------------------------------------------------------
# normalise_amounts
# ---------------------------------------------------------------------------

def test_normalise_amounts_flips_positive_withdrawals():
    df = pd.DataFrame({"type": ["Withdrawal", "Withdrawal"], "amount": [100.0, 200.0]})
    result = normalise_amounts(df)
    assert (result["amount"] < 0).all()


def test_normalise_amounts_keeps_deposits_positive():
    df = pd.DataFrame({"type": ["Deposit", "Deposit"], "amount": [100.0, 200.0]})
    result = normalise_amounts(df)
    assert (result["amount"] > 0).all()


def test_normalise_amounts_no_flip_when_already_negative():
    df = pd.DataFrame({"type": ["Withdrawal"], "amount": [-100.0]})
    result = normalise_amounts(df)
    # Not all withdrawal amounts are positive → condition not triggered
    assert result["amount"].iloc[0] == pytest.approx(-100.0)


def test_normalise_amounts_no_type_column_passthrough():
    df = pd.DataFrame({"amount": [100.0, -200.0]})
    result = normalise_amounts(df)
    assert list(result["amount"]) == [100.0, -200.0]


def test_normalise_amounts_expense_synonym():
    df = pd.DataFrame({"type": ["expense", "expense"], "amount": [50.0, 75.0]})
    result = normalise_amounts(df)
    assert (result["amount"] < 0).all()


# ---------------------------------------------------------------------------
# parse_mmex_csv — integration
# ---------------------------------------------------------------------------

def test_parse_mmex_csv_loads_rows(tmp_path):
    p = _write_csv(tmp_path, _SAMPLE_CSV)
    df = parse_mmex_csv(str(p))
    assert len(df) == 2


def test_parse_mmex_csv_date_is_datetime(tmp_path):
    p = _write_csv(tmp_path, _SAMPLE_CSV)
    df = parse_mmex_csv(str(p))
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_parse_mmex_csv_amount_is_numeric(tmp_path):
    p = _write_csv(tmp_path, _SAMPLE_CSV)
    df = parse_mmex_csv(str(p))
    assert pd.api.types.is_float_dtype(df["amount"])


def test_parse_mmex_csv_withdrawal_negative(tmp_path):
    p = _write_csv(tmp_path, _SAMPLE_CSV)
    df = parse_mmex_csv(str(p))
    withdrawal = df[df["type"].str.lower() == "withdrawal"]
    assert (withdrawal["amount"] < 0).all()


def test_parse_mmex_csv_deposit_positive(tmp_path):
    p = _write_csv(tmp_path, _SAMPLE_CSV)
    df = parse_mmex_csv(str(p))
    deposit = df[df["type"].str.lower() == "deposit"]
    assert (deposit["amount"] > 0).all()


def test_parse_mmex_csv_semicolon_separator(tmp_path):
    content = _SAMPLE_CSV.replace(",", ";")
    p = _write_csv(tmp_path, content)
    df = parse_mmex_csv(str(p))
    assert len(df) == 2


def test_parse_mmex_csv_canonical_type_column_present(tmp_path):
    p = _write_csv(tmp_path, _SAMPLE_CSV)
    df = parse_mmex_csv(str(p))
    assert "type" in df.columns


def test_parse_mmex_csv_canonical_account_column_present(tmp_path):
    p = _write_csv(tmp_path, _SAMPLE_CSV)
    df = parse_mmex_csv(str(p))
    assert "account" in df.columns


def test_parse_mmex_csv_transcode_header(tmp_path):
    """Files exported with 'TransCode' header are handled."""
    content = (
        "TransDate,Account,Payee,Category,TransAMOUNT,TransCode\n"
        "2024-03-01,Checking,Employer,Income,1000.00,Deposit\n"
    )
    p = _write_csv(tmp_path, content)
    df = parse_mmex_csv(str(p))
    assert "type" in df.columns
    assert "date" in df.columns


# ---------------------------------------------------------------------------
# parse_dates — fallback to mixed-format inference (L89-90)
# ---------------------------------------------------------------------------

def test_parse_dates_fallback_to_mixed_format():
    """Date strings that match no explicit format fall back to pandas mixed inference."""
    # "Jan 1, 2024" matches none of DATE_FORMATS ("%Y-%m-%d", "%d/%m/%Y", …)
    series = pd.Series(["Jan 1, 2024", "Feb 15, 2024", "Mar 31, 2024"])
    result = parse_dates(series)
    assert pd.api.types.is_datetime64_any_dtype(result)
    assert result.iloc[0].year == 2024
    assert result.iloc[0].month == 1
    assert result.iloc[0].day == 1
