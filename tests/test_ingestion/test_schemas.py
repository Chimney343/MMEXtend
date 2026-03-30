"""Tests for src.ingestion.schemas — TransactionRow Pydantic model."""

from datetime import date

import pytest
from pydantic import ValidationError

from src.ingestion.schemas import TransactionRow


# ---- valid rows ----------------------------------------------------------------

def test_valid_deposit():
    row = TransactionRow(date=date(2024, 1, 1), account="Checking", amount=1000.0, type="deposit")
    assert row.type == "deposit"
    assert row.account == "Checking"


def test_valid_withdrawal():
    row = TransactionRow(date=date(2024, 1, 1), account="Savings", amount=-500.0, type="withdrawal")
    assert row.type == "withdrawal"


def test_valid_transfer():
    row = TransactionRow(
        date=date(2024, 1, 1),
        account="Checking",
        amount=-200.0,
        type="transfer",
        to_account="Savings",
    )
    assert row.type == "transfer"


# ---- type normalisation --------------------------------------------------------

def test_type_normalised_to_lowercase():
    row = TransactionRow(date=date(2024, 1, 1), account="Checking", amount=100.0, type="Deposit")
    assert row.type == "deposit"


def test_type_normalised_uppercase():
    row = TransactionRow(date=date(2024, 1, 1), account="Checking", amount=100.0, type="WITHDRAWAL")
    assert row.type == "withdrawal"


# ---- type validation -----------------------------------------------------------

def test_type_invalid_raises():
    with pytest.raises(ValidationError, match="type must be one of"):
        TransactionRow(date=date(2024, 1, 1), account="Checking", amount=100.0, type="unknown")


def test_type_empty_raises():
    with pytest.raises(ValidationError):
        TransactionRow(date=date(2024, 1, 1), account="Checking", amount=100.0, type="")


# ---- account validation --------------------------------------------------------

def test_blank_account_raises():
    with pytest.raises(ValidationError, match="Account name must not be blank"):
        TransactionRow(date=date(2024, 1, 1), account="   ", amount=100.0, type="deposit")


def test_empty_account_raises():
    with pytest.raises(ValidationError, match="Account name must not be blank"):
        TransactionRow(date=date(2024, 1, 1), account="", amount=100.0, type="deposit")


def test_account_stripped():
    row = TransactionRow(date=date(2024, 1, 1), account="  Checking  ", amount=100.0, type="deposit")
    assert row.account == "Checking"


# ---- optional fields -----------------------------------------------------------

def test_optional_fields_default_none():
    row = TransactionRow(date=date(2024, 1, 1), account="Checking", amount=100.0, type="deposit")
    assert row.category is None
    assert row.subcategory is None
    assert row.payee is None
    assert row.to_account is None
    assert row.to_amount is None
    assert row.currency is None
    assert row.notes is None
    assert row.transaction_id is None
    assert row.transaction_number is None


def test_all_optional_fields():
    row = TransactionRow(
        date=date(2024, 1, 15),
        account="Checking",
        amount=-50.0,
        type="withdrawal",
        category="Food",
        subcategory="Groceries",
        payee="Supermarket",
        currency="USD",
        notes="Weekly shop",
        transaction_id=42,
        transaction_number="CHK001",
    )
    assert row.category == "Food"
    assert row.subcategory == "Groceries"
    assert row.payee == "Supermarket"
    assert row.currency == "USD"
    assert row.notes == "Weekly shop"
    assert row.transaction_id == 42
    assert row.transaction_number == "CHK001"


def test_negative_amount_accepted():
    row = TransactionRow(date=date(2024, 1, 1), account="A", amount=-99.99, type="withdrawal")
    assert row.amount == pytest.approx(-99.99)


def test_zero_amount_accepted():
    row = TransactionRow(date=date(2024, 1, 1), account="A", amount=0.0, type="deposit")
    assert row.amount == 0.0
