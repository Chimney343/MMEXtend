"""Tests for src.analysis.feature_engineering."""

from datetime import date

import pandas as pd
import pytest

from src.analysis.feature_engineering import build_monthly_aggregates, engineer_features


# ---------------------------------------------------------------------------
# Local helper — avoids importing conftest private functions
# ---------------------------------------------------------------------------

def _make_df(n_months: int, start: date = date(2024, 1, 1)) -> pd.DataFrame:
    """One deposit row per month for n_months."""
    rows = []
    for i in range(n_months):
        y = start.year + (start.month + i - 1) // 12
        m = (start.month + i - 1) % 12 + 1
        rows.append(
            {
                "date": pd.Timestamp(date(y, m, 1)),
                "account": "Checking",
                "type": "deposit",
                "category": "Income",
                "amount": 3000.0,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# engineer_features — column presence
# ---------------------------------------------------------------------------

def test_engineer_features_adds_year(sample_transactions):
    df = engineer_features(sample_transactions)
    assert "year" in df.columns


def test_engineer_features_adds_month(sample_transactions):
    df = engineer_features(sample_transactions)
    assert "month" in df.columns


def test_engineer_features_adds_quarter(sample_transactions):
    df = engineer_features(sample_transactions)
    assert "quarter" in df.columns


def test_engineer_features_adds_year_month(sample_transactions):
    df = engineer_features(sample_transactions)
    assert "year_month" in df.columns


def test_engineer_features_adds_month_ordinal(sample_transactions):
    df = engineer_features(sample_transactions)
    assert "month_ordinal" in df.columns


def test_engineer_features_adds_is_transfer(sample_transactions):
    df = engineer_features(sample_transactions)
    assert "is_transfer" in df.columns


def test_engineer_features_adds_is_uncategorised(sample_transactions):
    df = engineer_features(sample_transactions)
    assert "is_uncategorised" in df.columns


# ---------------------------------------------------------------------------
# engineer_features — year_month format
# ---------------------------------------------------------------------------

def test_year_month_format(sample_transactions):
    df = engineer_features(sample_transactions)
    assert df["year_month"].iloc[0] == "2024-01"


def test_year_month_is_string(sample_transactions):
    df = engineer_features(sample_transactions)
    # Accept both legacy object dtype and pandas StringDtype
    assert pd.api.types.is_string_dtype(df["year_month"])


# ---------------------------------------------------------------------------
# engineer_features — month_ordinal
# ---------------------------------------------------------------------------

def test_month_ordinal_starts_at_zero(sample_transactions):
    df = engineer_features(sample_transactions)
    assert df["month_ordinal"].min() == 0


def test_month_ordinal_monotonically_non_decreasing(sample_transactions):
    df = engineer_features(sample_transactions)
    ordinals = df.sort_values("date")["month_ordinal"].tolist()
    assert ordinals == sorted(ordinals)


def test_month_ordinal_differences_equal_month_count():
    df = _make_df(12)
    result = engineer_features(df)
    # Ordinals 0..11 → range = 11
    assert result["month_ordinal"].max() == 11


# ---------------------------------------------------------------------------
# engineer_features — is_transfer
# ---------------------------------------------------------------------------

def test_is_transfer_false_for_deposits(sample_transactions):
    df = engineer_features(sample_transactions)
    deposits = df[df["type"] == "deposit"]
    assert deposits["is_transfer"].sum() == 0


def test_is_transfer_false_for_withdrawals(sample_transactions):
    df = engineer_features(sample_transactions)
    withdrawals = df[df["type"] == "withdrawal"]
    assert withdrawals["is_transfer"].sum() == 0


def test_is_transfer_true_for_transfer_type():
    df = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-01")],
            "account": ["Checking"],
            "type": ["transfer"],
            "category": ["Transfer"],
            "amount": [-100.0],
        }
    )
    result = engineer_features(df)
    assert result["is_transfer"].iloc[0]


# ---------------------------------------------------------------------------
# engineer_features — is_uncategorised
# ---------------------------------------------------------------------------

def test_is_uncategorised_false_for_categorised(sample_transactions):
    df = engineer_features(sample_transactions)
    assert df["is_uncategorised"].sum() == 0


def test_is_uncategorised_true_for_none():
    df = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-01")],
            "account": ["Checking"],
            "type": ["deposit"],
            "category": [None],
            "amount": [100.0],
        }
    )
    result = engineer_features(df)
    assert result["is_uncategorised"].iloc[0]


def test_is_uncategorised_true_for_blank_string():
    df = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-01")],
            "account": ["Checking"],
            "type": ["deposit"],
            "category": [""],
            "amount": [100.0],
        }
    )
    result = engineer_features(df)
    assert result["is_uncategorised"].iloc[0]


# ---------------------------------------------------------------------------
# engineer_features — immutability
# ---------------------------------------------------------------------------

def test_does_not_mutate_input(sample_transactions):
    original_cols = set(sample_transactions.columns)
    engineer_features(sample_transactions)
    assert set(sample_transactions.columns) == original_cols


# ---------------------------------------------------------------------------
# build_monthly_aggregates — columns
# ---------------------------------------------------------------------------

def test_build_monthly_aggregates_has_required_columns(sample_transactions):
    df = engineer_features(sample_transactions)
    monthly = build_monthly_aggregates(df)
    required = {"year_month", "category", "total_amount", "month_ordinal", "mom_delta"}
    assert required.issubset(monthly.columns)


# ---------------------------------------------------------------------------
# build_monthly_aggregates — transfer exclusion
# ---------------------------------------------------------------------------

def test_transfers_excluded_from_aggregates(sample_transactions):
    transfer = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-15"),
                "account": "Checking",
                "to_account": "Savings",
                "payee": None,
                "category": "Transfer",
                "subcategory": None,
                "amount": -500.0,
                "type": "transfer",
                "currency": "USD",
                "notes": None,
                "transaction_id": 999,
            }
        ]
    )
    df_combined = pd.concat([sample_transactions, transfer], ignore_index=True)
    monthly = build_monthly_aggregates(engineer_features(df_combined))
    assert "Transfer" not in monthly["category"].values


# ---------------------------------------------------------------------------
# build_monthly_aggregates — aggregation correctness
# ---------------------------------------------------------------------------

def test_food_totals_equal_minus_400(sample_transactions):
    monthly = build_monthly_aggregates(engineer_features(sample_transactions))
    food = monthly[monthly["category"] == "Food"]
    assert (food["total_amount"] == -400.0).all()


def test_income_totals_equal_3000(sample_transactions):
    monthly = build_monthly_aggregates(engineer_features(sample_transactions))
    income = monthly[monthly["category"] == "Income"]
    assert (income["total_amount"] == 3000.0).all()


# ---------------------------------------------------------------------------
# build_monthly_aggregates — month-over-month delta
# ---------------------------------------------------------------------------

def test_mom_delta_first_month_is_nan(sample_transactions):
    monthly = build_monthly_aggregates(engineer_features(sample_transactions))
    for _cat, group in monthly.groupby("category"):
        first_row = group.sort_values("year_month").iloc[0]
        assert pd.isna(first_row["mom_delta"])


def test_mom_delta_constant_spend_is_zero(sample_transactions):
    monthly = build_monthly_aggregates(engineer_features(sample_transactions))
    food = monthly[monthly["category"] == "Food"].sort_values("year_month")
    # Constant -400 each month → delta = 0.0 for all months after the first
    non_first_deltas = food["mom_delta"].iloc[1:]
    assert (non_first_deltas == 0.0).all()


# ---------------------------------------------------------------------------
# Boundary tests (per copilot-instructions.md)
# ---------------------------------------------------------------------------

def test_6_months_boundary():
    """Exactly 6 months — boundary for trend analysis."""
    df = _make_df(6)
    monthly = build_monthly_aggregates(engineer_features(df))
    assert monthly["year_month"].nunique() == 6


def test_5_months_returns_aggregates():
    """5 months of data still produces aggregates (trend guards live elsewhere)."""
    df = _make_df(5)
    monthly = build_monthly_aggregates(engineer_features(df))
    assert monthly["year_month"].nunique() == 5


def test_all_null_amounts_generates_features(null_amounts_df):
    """All-null amounts: feature engineering must not crash (halt is in validator)."""
    df = engineer_features(null_amounts_df)
    assert "year_month" in df.columns
    assert len(df) == len(null_amounts_df)


# ---------------------------------------------------------------------------
# engineer_features — subcategory pipe splitting
# ---------------------------------------------------------------------------

def _tx(subcategory) -> pd.DataFrame:
    """One-row DataFrame with the given subcategory value."""
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-01"),
                "account": "Checking",
                "type": "deposit",
                "category": "Income",
                "subcategory": subcategory,
                "amount": 100.0,
            }
        ]
    )


def test_subcategory_detail_column_always_present(sample_transactions):
    """`subcategory_detail` column must exist after engineer_features."""
    df = engineer_features(sample_transactions)
    assert "subcategory_detail" in df.columns


def test_subcategory_no_pipe_unchanged():
    """Subcategory without pipe is left intact; detail is NaN."""
    result = engineer_features(_tx("Food"))
    assert result["subcategory"].iloc[0] == "Food"
    assert pd.isna(result["subcategory_detail"].iloc[0])


def test_subcategory_none_stays_none():
    """None subcategory produces NaN in both subcategory and subcategory_detail."""
    result = engineer_features(_tx(None))
    assert pd.isna(result["subcategory"].iloc[0])
    assert pd.isna(result["subcategory_detail"].iloc[0])


def test_subcategory_pipe_two_levels():
    """Two-level subcategory 'Interests|Saving accounts' is split correctly."""
    result = engineer_features(_tx("Interests|Saving accounts"))
    assert result["subcategory"].iloc[0] == "Interests"
    assert result["subcategory_detail"].iloc[0] == "Saving accounts"


def test_subcategory_pipe_three_levels():
    """Three-level path 'Reselling|Tabletop|Board Games': first segment in subcategory,
    remainder preserved in subcategory_detail."""
    result = engineer_features(_tx("Reselling|Tabletop|Board Games"))
    assert result["subcategory"].iloc[0] == "Reselling"
    assert result["subcategory_detail"].iloc[0] == "Tabletop|Board Games"


def test_subcategory_split_does_not_mutate_input():
    """engineer_features must not modify the caller's DataFrame."""
    df = _tx("Interests|Saving accounts")
    original_val = df["subcategory"].iloc[0]
    engineer_features(df)
    assert df["subcategory"].iloc[0] == original_val


# ---------------------------------------------------------------------------
# build_monthly_aggregates — amount_pln vs amount column selection
# ---------------------------------------------------------------------------

from src.analysis.feature_engineering import build_monthly_cashflow_split  # noqa: E402


def _make_df_with_pln(n_months: int, pln_multiplier: float = 2.0) -> pd.DataFrame:
    """One deposit + one withdrawal per month, with both amount and amount_pln.

    amount_pln = amount * pln_multiplier, simulating a non-PLN account.
    """
    rows = []
    for i in range(n_months):
        y = date(2024, 1, 1).year + (0 + i) // 12
        m = (0 + i) % 12 + 1
        ts = pd.Timestamp(date(y, m, 1))
        rows.append({
            "date": ts,
            "account": "EUR Account",
            "type": "deposit",
            "category": "Income",
            "subcategory": None,
            "amount": 1000.0,
            "amount_pln": 1000.0 * pln_multiplier,
            "currency": "EUR",
        })
        rows.append({
            "date": ts,
            "account": "EUR Account",
            "type": "withdrawal",
            "category": "Food",
            "subcategory": None,
            "amount": -200.0,
            "amount_pln": -200.0 * pln_multiplier,
            "currency": "EUR",
        })
    return pd.DataFrame(rows)


def test_build_monthly_aggregates_uses_amount_pln_when_present():
    """When amount_pln exists, total_amount should reflect PLN values."""
    df_raw = _make_df_with_pln(3, pln_multiplier=4.25)
    df = engineer_features(df_raw)
    monthly = build_monthly_aggregates(df)
    income = monthly[monthly["category"] == "Income"]["total_amount"].iloc[0]
    # amount_pln = 1000 * 4.25 = 4250
    assert abs(income - 4250.0) < 0.01


def test_build_monthly_aggregates_falls_back_to_amount_without_pln():
    """When amount_pln is absent (CSV path), total_amount uses amount."""
    df_raw = _make_df(3)          # no amount_pln column
    df = engineer_features(df_raw)
    assert "amount_pln" not in df.columns
    monthly = build_monthly_aggregates(df)
    income = monthly[monthly["category"] == "Income"]["total_amount"].iloc[0]
    assert abs(income - 3000.0) < 0.01


def test_build_monthly_cashflow_split_uses_amount_pln_when_present():
    """Deposits and withdrawals in the split should use PLN amounts."""
    df_raw = _make_df_with_pln(3, pln_multiplier=4.25)
    df = engineer_features(df_raw)
    split = build_monthly_cashflow_split(df)
    # month 1 deposits: 1000 EUR * 4.25 = 4250 PLN
    assert abs(split["deposits"].iloc[0] - 4250.0) < 0.01


def test_build_monthly_cashflow_split_falls_back_to_amount_without_pln():
    """Deposits/withdrawals use amount when amount_pln is absent."""
    df_raw = _make_df(3)
    df = engineer_features(df_raw)
    split = build_monthly_cashflow_split(df)
    assert abs(split["deposits"].iloc[0] - 3000.0) < 0.01


# ---------------------------------------------------------------------------
# build_monthly_cashflow_split — exclude_categories
# ---------------------------------------------------------------------------

def _make_df_with_wedding(n_months: int) -> pd.DataFrame:
    """One deposit + one regular withdrawal + one Wedding withdrawal per month."""
    rows = []
    for i in range(n_months):
        y = date(2024, 1, 1).year + i // 12
        m = i % 12 + 1
        ts = pd.Timestamp(date(y, m, 1))
        rows.append({"date": ts, "account": "Checking", "type": "deposit",
                     "category": "Income", "subcategory": None, "amount": 3000.0})
        rows.append({"date": ts, "account": "Checking", "type": "withdrawal",
                     "category": "Food", "subcategory": None, "amount": -500.0})
        rows.append({"date": ts, "account": "Checking", "type": "withdrawal",
                     "category": "Wedding", "subcategory": None, "amount": -2000.0})
    return pd.DataFrame(rows)


def test_exclude_categories_reduces_withdrawals():
    """Withdrawals should drop by the excluded category's amount."""
    df = engineer_features(_make_df_with_wedding(3))
    split_all = build_monthly_cashflow_split(df)
    split_excl = build_monthly_cashflow_split(df, exclude_categories=["Wedding"])
    # Each month: 500 Food + 2000 Wedding = 2500 total → 500 without Wedding
    assert abs(split_all["withdrawals"].iloc[0] - 2500.0) < 0.01
    assert abs(split_excl["withdrawals"].iloc[0] - 500.0) < 0.01


def test_exclude_categories_does_not_affect_deposits():
    """Excluding a withdrawal category must not change deposit totals."""
    df = engineer_features(_make_df_with_wedding(3))
    split_all = build_monthly_cashflow_split(df)
    split_excl = build_monthly_cashflow_split(df, exclude_categories=["Wedding"])
    assert split_all["deposits"].equals(split_excl["deposits"])


def test_exclude_categories_none_is_identity():
    """Passing None (the default) must produce the same result as no argument."""
    df = engineer_features(_make_df_with_wedding(3))
    split_default = build_monthly_cashflow_split(df)
    split_none = build_monthly_cashflow_split(df, exclude_categories=None)
    pd.testing.assert_frame_equal(split_default, split_none)


def test_exclude_categories_empty_list_is_identity():
    """An empty list must produce the same result as the default."""
    df = engineer_features(_make_df_with_wedding(3))
    split_default = build_monthly_cashflow_split(df)
    split_empty = build_monthly_cashflow_split(df, exclude_categories=[])
    pd.testing.assert_frame_equal(split_default, split_empty)

