"""Tests for src.analysis.recurring_commitments."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.recurring_commitments import (
    CATEGORY_SUMMARY_COLUMNS,
    EVENT_COLUMNS,
    REGISTER_COLUMNS,
    _classify_cadence,
    build_recurring_category_summary,
    build_recurring_commitments,
)

AS_OF = pd.Timestamp("2026-10-28")


# ---------------------------------------------------------------------------
# Fixture builder — one row per scenario
# ---------------------------------------------------------------------------


def _make_recurring_df() -> pd.DataFrame:
    """Synthetic featured-transaction frame with recurring and non-recurring payees."""
    rows: list[dict] = []
    next_id = 0

    def add(
        date: pd.Timestamp,
        payee,
        amount: float,
        category: str,
        subcategory=None,
        txn_type: str = "withdrawal",
        is_transfer: bool = False,
    ) -> None:
        nonlocal next_id
        rows.append(
            {
                "date": pd.Timestamp(date),
                "account": "Checking",
                "payee": payee,
                "category": category,
                "subcategory": subcategory,
                "amount": amount,
                "type": txn_type,
                "currency": "PLN",
                "is_transfer": is_transfer,
                "transaction_id": next_id,
            }
        )
        next_id += 1

    # Rent: 12 identical monthly payments, plus a future-dated scheduled row.
    for month in pd.date_range("2025-11-01", periods=12, freq="MS"):
        add(month, "Rent", -1350.0, "Housing", "Rent")
    add(pd.Timestamp("2027-03-01"), "Rent", -1350.0, "Housing", "Rent")

    # ZUS: yearly step 2,400 -> 2,510 (abs jump > 5 PLN, pct < 5%).
    for month in pd.date_range("2025-11-01", periods=2, freq="MS"):
        add(month, "ZUS", -2400.0, "Taxes", "ZUS")
    for month in pd.date_range("2026-01-01", periods=10, freq="MS"):
        add(month, "ZUS", -2510.0, "Taxes", "ZUS")

    # Streaming subscription: 20 -> 24 in the last two months.
    for month in pd.date_range("2025-11-01", periods=12, freq="MS"):
        amount = -24.0 if month >= pd.Timestamp("2026-09-01") else -20.0
        add(month, "Streaming", amount, "Subscriptions", "Streaming")

    # Annual insurance: six yearly payments, last one 43 days before as_of.
    for year in range(2021, 2027):
        add(pd.Timestamp(f"{year}-09-15"), "Insurance", -1200.0, "Insurance", "Policy")

    # Gym: 12 monthly payments ending ~8 months before as_of -> lapsed.
    for month in pd.date_range("2025-03-01", periods=12, freq="MS"):
        add(month, "Gym", -120.0, "Health", "Gym")

    # Supermarket: weekly but highly variable -> excluded by CV.
    supermarket_amounts = [-120.0, -340.0, -85.5, -410.0, -160.0]
    for i, day in enumerate(pd.date_range("2026-01-01", "2026-10-22", freq="7D")):
        add(day, "Supermarket", supermarket_amounts[i % len(supermarket_amounts)], "Food", "Groceries")

    # One-off large purchase.
    add(pd.Timestamp("2026-05-20"), "Furniture Store", -8000.0, "Home", "Furniture")

    # Transfer (never a cost series).
    for month in pd.date_range("2025-11-01", periods=12, freq="MS"):
        add(
            month,
            "Savings Transfer",
            -500.0,
            "Transfer",
            None,
            txn_type="transfer",
            is_transfer=True,
        )

    # Unkeyable rows: null and blank payees.
    for i, month in enumerate(pd.date_range("2026-05-01", periods=6, freq="MS")):
        add(month, None if i % 2 == 0 else "   ", -50.0, "Misc", None)

    return pd.DataFrame(rows)


@pytest.fixture()
def recurring_result():
    return build_recurring_commitments(_make_recurring_df(), as_of=AS_OF)


def _row(register: pd.DataFrame, payee: str) -> pd.Series:
    matches = register[register["payee"] == payee]
    assert not matches.empty, f"{payee} missing from register: {register['payee'].tolist()}"
    return matches.iloc[0]


# ---------------------------------------------------------------------------
# Shape and contract
# ---------------------------------------------------------------------------


def test_returns_frames_with_documented_columns(recurring_result):
    register, events = recurring_result
    assert list(register.columns) == REGISTER_COLUMNS
    assert list(events.columns) == EVENT_COLUMNS


def test_rent_is_active_monthly_commitment(recurring_result):
    register, _ = recurring_result
    rent = _row(register, "Rent")
    assert bool(rent["is_active"]) is True
    assert bool(rent["is_lapsed"]) is False
    assert rent["cadence_class"] == "monthly"
    assert rent["n_distinct_months"] == 12


def test_rent_run_rate_is_hand_computed(recurring_result):
    register, _ = recurring_result
    rent = _row(register, "Rent")
    # 12 x 1,350 PLN inside a 12-month window -> 1,350 PLN/month exactly.
    assert rent["monthly_run_rate"] == pytest.approx(1350.0)
    assert rent["amount_current"] == pytest.approx(1350.0)
    assert rent["amount_median"] == pytest.approx(1350.0)
    assert rent["amount_cv_trailing"] == pytest.approx(0.0)


def test_next_expected_is_last_seen_plus_median_gap(recurring_result):
    register, _ = recurring_result
    rent = _row(register, "Rent")
    expected = rent["last_seen"] + pd.Timedelta(days=rent["median_gap_days"])
    assert rent["next_expected_date"] == expected
    assert rent["days_overdue"] == 0


def test_rent_has_no_price_change(recurring_result):
    register, events = recurring_result
    rent = _row(register, "Rent")
    assert rent["n_price_changes"] == 0
    assert bool(rent["has_price_change_12m"]) is False
    assert not events[events["payee"] == "Rent"]["is_price_change"].any()


def test_zus_step_is_a_price_change(recurring_result):
    register, events = recurring_result
    zus = _row(register, "ZUS")
    # 2,400 -> 2,510: +110 PLN absolute (4.58% relative) -> flagged by the abs rule.
    assert zus["n_price_changes"] == 1
    assert bool(zus["has_price_change_12m"]) is True
    assert zus["last_price_change_pct"] == pytest.approx(110.0 / 2400.0)
    assert zus["amount_current"] == pytest.approx(2510.0)
    zus_events = events[events["payee"] == "ZUS"]
    assert zus_events["is_price_change"].sum() == 1


def test_streaming_hike_pct_flagged(recurring_result):
    register, _ = recurring_result
    streaming = _row(register, "Streaming")
    assert streaming["n_price_changes"] == 1
    assert streaming["last_price_change_pct"] == pytest.approx(0.20)
    assert bool(streaming["has_price_change_12m"]) is True


def test_insurance_is_annual_and_active(recurring_result):
    register, _ = recurring_result
    insurance = _row(register, "Insurance")
    assert insurance["cadence_class"] == "annual"
    assert bool(insurance["is_active"]) is True
    assert bool(insurance["is_lapsed"]) is False
    assert insurance["next_expected_date"] > AS_OF


def test_gym_is_lapsed(recurring_result):
    register, _ = recurring_result
    gym = _row(register, "Gym")
    assert bool(gym["is_active"]) is False
    assert bool(gym["is_lapsed"]) is True
    assert bool(gym["is_renewal_due_30d"]) is False
    assert gym["cadence_class"] == "monthly"
    assert gym["days_overdue"] > 1.5 * gym["median_gap_days"]


def test_variable_supermarket_is_excluded(recurring_result):
    register, _ = recurring_result
    assert "Supermarket" not in register["payee"].tolist()


def test_one_off_purchase_is_excluded(recurring_result):
    register, _ = recurring_result
    assert "Furniture Store" not in register["payee"].tolist()


def test_transfer_series_is_excluded(recurring_result):
    register, _ = recurring_result
    assert "Savings Transfer" not in register["payee"].tolist()


def test_future_dated_rows_are_ignored(recurring_result):
    register, events = recurring_result
    rent = _row(register, "Rent")
    assert rent["last_seen"] == pd.Timestamp("2026-10-01")
    assert (events["date"] <= AS_OF).all()
    assert events[events["payee"] == "Rent"].shape[0] == 12


def test_blank_payees_are_never_keyed(recurring_result):
    register, _ = recurring_result
    assert register["payee"].notna().all()
    assert (register["payee"].str.strip() != "").all()


# ---------------------------------------------------------------------------
# Configuration overrides
# ---------------------------------------------------------------------------


def test_config_override_min_distinct_months_excludes_everything():
    register, events = build_recurring_commitments(
        _make_recurring_df(), as_of=AS_OF, config={"min_distinct_months": 24}
    )
    assert register.empty
    assert events.empty


def test_config_override_cv_threshold_excludes_continuous_variation():
    register, _ = build_recurring_commitments(
        _make_recurring_df(), as_of=AS_OF, config={"amount_cv_threshold": 0.001}
    )
    assert "Supermarket" not in register["payee"].tolist()
    assert "Rent" in register["payee"].tolist()
    # A single step is tolerated, so the subscription survives a strict threshold.
    assert "Streaming" in register["payee"].tolist()


# ---------------------------------------------------------------------------
# Same-day splits, large hikes, and renewal flags
# ---------------------------------------------------------------------------


def _series_frame(rows: list[tuple[pd.Timestamp, str, float, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp(date),
                "account": "Checking",
                "payee": payee,
                "category": category,
                "subcategory": None,
                "amount": amount,
                "type": "withdrawal",
                "currency": "PLN",
                "is_transfer": False,
                "transaction_id": i,
            }
            for i, (date, payee, amount, category) in enumerate(rows)
        ]
    )


def test_large_price_hike_stays_in_register():
    rows = [
        (month, "Hiked Service", -100.0, "Subscriptions")
        for month in pd.date_range("2025-11-01", periods=6, freq="MS")
    ] + [
        (month, "Hiked Service", -150.0, "Subscriptions")
        for month in pd.date_range("2026-05-01", periods=6, freq="MS")
    ]

    register, events = build_recurring_commitments(_series_frame(rows), as_of=AS_OF)

    hiked = _row(register, "Hiked Service")
    assert hiked["monthly_run_rate"] == pytest.approx(125.0)
    assert hiked["n_price_changes"] == 1
    assert bool(hiked["has_price_change_12m"]) is True
    assert hiked["last_price_change_pct"] == pytest.approx(0.5)
    assert hiked["amount_current"] == pytest.approx(150.0)
    assert events["is_price_change"].sum() == 1


def test_same_day_split_purchases_are_one_charge():
    rows = []
    for month in pd.date_range("2025-11-01", periods=12, freq="MS"):
        rows.append((month, "Two-Part Bill", -500.0, "Bills"))
        rows.append((month, "Two-Part Bill", -600.0, "Bills"))

    register, events = build_recurring_commitments(_series_frame(rows), as_of=AS_OF)

    bill = _row(register, "Two-Part Bill")
    assert bill["amount_cv_trailing"] == pytest.approx(0.0)
    assert bill["n_price_changes"] == 0
    assert bool(bill["has_price_change_12m"]) is False
    assert events.shape[0] == 24
    assert not events["is_price_change"].any()


def test_annual_renewal_is_due_without_being_active():
    first = AS_OF - pd.Timedelta(days=347)
    rows = [
        (first - pd.DateOffset(years=offset), "Annual Policy", -1000.0, "Insurance")
        for offset in range(5, -1, -1)
    ]

    register, _ = build_recurring_commitments(_series_frame(rows), as_of=AS_OF)

    policy = _row(register, "Annual Policy")
    assert policy["cadence_class"] == "annual"
    assert bool(policy["is_active"]) is False
    assert bool(policy["is_lapsed"]) is False
    assert bool(policy["is_renewal_due_30d"]) is True
    assert policy["next_expected_date"] == first + pd.Timedelta(days=policy["median_gap_days"])


# ---------------------------------------------------------------------------
# Empty input and immutability
# ---------------------------------------------------------------------------


def test_empty_dataframe_returns_empty_frames():
    register, events = build_recurring_commitments(pd.DataFrame(), as_of=AS_OF)
    assert register.empty and events.empty
    assert list(register.columns) == REGISTER_COLUMNS
    assert list(events.columns) == EVENT_COLUMNS


def test_input_frame_is_not_mutated():
    df = _make_recurring_df()
    original = df.copy(deep=True)
    build_recurring_commitments(df, as_of=AS_OF)
    pd.testing.assert_frame_equal(df, original)


# ---------------------------------------------------------------------------
# Cadence classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("gap", "expected"),
    [
        (7, "weekly"),
        (14, "biweekly"),
        (30, "monthly"),
        (90, "quarterly"),
        (180, "semiannual"),
        (365, "annual"),
        (50, "irregular"),
        (20, "irregular"),
        (np.nan, "irregular"),
        (None, "irregular"),
    ],
)
def test_classify_cadence_windows(gap, expected):
    assert _classify_cadence(gap) == expected


# ---------------------------------------------------------------------------
# Category summary
# ---------------------------------------------------------------------------


def _category_row(summary: pd.DataFrame, category: str) -> pd.Series:
    matches = summary[summary["category"] == category]
    assert not matches.empty, f"{category} missing: {summary['category'].tolist()}"
    return matches.iloc[0]


def test_category_summary_rolls_up_commitments_and_spend(recurring_result):
    register, _ = recurring_result
    summary = build_recurring_category_summary(
        register, _make_recurring_df(), as_of=AS_OF
    )

    assert list(summary.columns) == CATEGORY_SUMMARY_COLUMNS
    assert summary["share_of_spend"].sum() == pytest.approx(1.0)

    housing = _category_row(summary, "Housing")
    assert housing["avg_monthly_spend"] == pytest.approx(1350.0)
    assert housing["recurring_run_rate"] == pytest.approx(1350.0)
    assert housing["recurring_coverage"] == pytest.approx(1.0)
    assert housing["n_active_commitments"] == 1
    assert bool(housing["is_steady"]) is True

    taxes = _category_row(summary, "Taxes")
    zus_rate = float(register.loc[register["payee"] == "ZUS", "monthly_run_rate"].iloc[0])
    assert taxes["recurring_run_rate"] == pytest.approx(zus_rate)
    assert taxes["n_active_commitments"] == 1


def test_category_summary_includes_categories_without_commitments(recurring_result):
    register, _ = recurring_result
    summary = build_recurring_category_summary(
        register, _make_recurring_df(), as_of=AS_OF
    )

    home = _category_row(summary, "Home")
    assert home["n_commitments"] == 0
    assert home["recurring_run_rate"] == 0.0
    assert home["recurring_coverage"] == 0.0
    assert bool(home["is_steady"]) is False

    food = _category_row(summary, "Food")
    assert food["n_commitments"] == 0
    assert food["recurring_run_rate"] == 0.0
    assert bool(food["is_active"]) is True


def test_category_summary_empty_inputs_return_empty_frame():
    summary = build_recurring_category_summary(
        pd.DataFrame(), pd.DataFrame(), as_of=AS_OF
    )
    assert summary.empty
    assert list(summary.columns) == CATEGORY_SUMMARY_COLUMNS


def test_category_summary_omits_categories_without_window_spend():
    rows = [(pd.Timestamp("2020-01-01"), "Old Shop", -100.0, "Old Category")] + [
        (month, "Rent", -1000.0, "Housing")
        for month in pd.date_range("2025-11-01", periods=12, freq="MS")
    ]

    summary = build_recurring_category_summary(
        pd.DataFrame(), _series_frame(rows), as_of=AS_OF
    )

    assert "Old Category" not in summary["category"].tolist()
    assert "Housing" in summary["category"].tolist()

def test_category_summary_threshold_is_configurable(recurring_result):
    register, _ = recurring_result
    df = _make_recurring_df()

    strict = build_recurring_category_summary(
        register, df, as_of=AS_OF, config={"category_cv_threshold": 0.0}
    )
    loose = build_recurring_category_summary(
        register, df, as_of=AS_OF, config={"category_cv_threshold": 1.0}
    )

    assert bool(_category_row(strict, "Taxes")["is_steady"]) is False
    assert bool(_category_row(loose, "Taxes")["is_steady"]) is True
