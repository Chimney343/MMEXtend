"""Pydantic models for validating individual transaction rows."""

from datetime import date
from typing import Optional

from pydantic import BaseModel, field_validator


class TransactionRow(BaseModel):
    date: date
    account: str
    amount: float
    type: str  # deposit, withdrawal, transfer
    category: Optional[str] = None
    subcategory: Optional[str] = None
    payee: Optional[str] = None
    to_account: Optional[str] = None
    to_amount: Optional[float] = None
    currency: Optional[str] = None
    notes: Optional[str] = None
    transaction_id: Optional[int] = None
    transaction_number: Optional[str] = None

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
            raise ValueError(f"type must be one of {allowed}, got '{v}'")
        return v.lower()
