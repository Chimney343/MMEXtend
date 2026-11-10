"""Tests for src.analysis.budget_health."""

import numpy as np
import pandas as pd
import pytest

from src.analysis.budget_health import (
    ALLOCATION_COLUMNS,
    BALANCE_COLUMNS,
    RESERVE_TRANSFER_COLUMNS,
    RESERVE_TREND_COLUMNS,
    assign_account_groups,
    assign_buckets,
    build_account_balances,
    build_annual_creep,
    build_budget_health_monthly,
    build_reserve_transfers,
    build_reserve_trend,
    implied_fx_rates,
    next_100_steps,
    summarise_budget_health,
    summarise_reserve_transfers,
)

CONFIG = {
    "income_categories": ["Income"],
    "take_home_deductions": ["Taxes"],
    "buckets": {
        "Bills": "fixed",
        "Food|Groceries": "fixed",
        "Food": "guilt_free",
        "Leisure": "guilt_free",
        "Wedding": "one_off",
    },
    "account_groups": {"savings": ["Savings"], "receivables": ["Mum"]},
}

ACCOUNTS = pd.DataFrame(
    {
        "account": ["Checking", "Savings", "EUR Acc", "Mum", "Card"],
        "account_type": ["Checking", "Checking", "Checking", "Credit Card", "Credit Card"],
        "status": ["Open"] * 5,
        "initial_balance": [1000.0, 0.0, 0.0, 0.0, 0.0],
        "initial_date": pd.to_datetime(["2025-01-01"] * 5),
        "currency": ["PLN", "PLN", "EUR", "PLN", "PLN"],
    }
)


# ---------------------------------------------------------------------------
# Fixture builder
# ---------------------------------------------------------------------------


def _row(
    date: str,
    account: str,
    amount: float,
    category: str | None,
    subcategory: str | None = None,
    txn_type: str = "withdrawal",
    to_account: str | None = None,
    to_amount: float | None = None,
    currency: str = "PLN",
    amount_pln: float | None = None,
) -> dict:
    ts = pd.Timestamp(date)
    return {
        "date": ts,
        "account": account,
        "to_account": to_account,
        "category": category,
        "subcategory": subcategory,
        "amount": amount,
        "to_amount": abs(amount) if to_amount is None else to_amount,
        "type": txn_type,
        "currency": currency,
        "amount_pln": amount if amount_pln is None else amount_pln,
        "year_month": ts.strftime("%Y-%m"),
        "is_transfer": txn_type == "transfer",
    }


def _standard_month(year_month: str) -> list[dict]:
    """10k gross, 2k taxes, 3.8k fixed, 1.2k guilt-free, 500 to savings → 2.5k unallocated."""
    d = f"{year_month}-10"
    return [
        _row(d, "Checking", 10_000.0, "Income", "Salary", txn_type="deposit"),
        _row(d, "Checking", -1_000.0, "Taxes", "VAT"),
        _row(d, "Checking", -1_000.0, "Taxes", "Income Tax"),
        _row(d, "Checking", -3_000.0, "Bills", "Rent"),
        _row(d, "Checking", -800.0, "Food", "Groceries"),
        _row(d, "Checking", -1_200.0, "Food", "Dining out"),
        _row(d, "Checking", -500.0, "Transfer", txn_type="transfer", to_account="Savings"),
    ]


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


@pytest.fixture
def twelve_months() -> pd.DataFrame:
    months = pd.period_range("2025-01", "2025-12", freq="M").strftime("%Y-%m")
    return _frame([r for m in months for r in _standard_month(m)])


# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------


def test_assign_buckets_subcategory_key_wins_and_unknown_is_unmapped():
    df = pd.DataFrame(
        {
            "category": ["Food", "Food", "Gadgets", None],
            "subcategory": ["Groceries", "Dining out", None, None],
        }
    )
    result = assign_buckets(df, CONFIG["buckets"])
    assert result.tolist() == ["fixed", "guilt_free", "unmapped", "unmapped"]


def test_assign_buckets_rejects_unknown_bucket_name():
    df = pd.DataFrame({"category": ["Food"], "subcategory": [None]})
    with pytest.raises(ValueError, match="Unknown bucket"):
        assign_buckets(df, {"Food": "luxuries"})


def test_assign_account_groups_override_beats_account_type(caplog):
    accounts = pd.DataFrame(
        {
            "account": ["Checking", "Mum", "Card", "Voucher", "Odd"],
            "account_type": ["Checking", "Credit Card", "Credit Card", "Term", "Teleporter"],
        }
    )
    groups = assign_account_groups(accounts, {"receivables": ["Mum", "Ghost"]})
    assert groups.tolist() == ["cash", "receivables", "debt", "other", "other"]
    assert any("Ghost" in r.message for r in caplog.records)


def test_assign_account_groups_rejects_unknown_group():
    accounts = pd.DataFrame({"account": ["A"], "account_type": ["Checking"]})
    with pytest.raises(ValueError, match="Unknown account group"):
        assign_account_groups(accounts, {"rainy_day": ["A"]})


# ---------------------------------------------------------------------------
# Monthly Conscious Spending Plan
# ---------------------------------------------------------------------------


def test_monthly_buckets_are_hand_computed(twelve_months):
    monthly = build_budget_health_monthly(
        twelve_months, ACCOUNTS, as_of="2025-12-31", config=CONFIG
    )
    row = monthly.iloc[0]
    assert row["gross_income"] == pytest.approx(10_000)
    assert row["deductions"] == pytest.approx(2_000)
    assert row["take_home"] == pytest.approx(8_000)
    assert row["fixed"] == pytest.approx(3_800)
    assert row["guilt_free"] == pytest.approx(1_200)
    assert row["savings"] == pytest.approx(500)
    assert row["investments"] == pytest.approx(0)
    assert row["unallocated"] == pytest.approx(2_500)

    last = monthly.iloc[-1]
    assert last["take_home_ttm"] == pytest.approx(96_000)
    assert last["fixed_share_ttm"] == pytest.approx(3_800 / 8_000)
    assert last["savings_share_ttm"] == pytest.approx(500 / 8_000)
    assert last["unallocated_share_ttm"] == pytest.approx(2_500 / 8_000)


def test_unallocated_equals_change_in_non_savings_balances(twelve_months):
    extra = _frame(
        list(twelve_months.to_dict("records"))
        + [
            _row("2025-03-05", "Checking", -400.0, "Transfer", txn_type="transfer",
                 to_account="Mum"),
            _row("2025-03-06", "Card", -250.0, "Leisure"),
            _row("2025-04-07", "Savings", -300.0, "Transfer", txn_type="transfer",
                 to_account="Checking"),
        ]
    )
    monthly = build_budget_health_monthly(extra, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    balances = build_account_balances(extra, ACCOUNTS, as_of="2025-12-31", config=CONFIG)

    other_groups = balances[~balances["group"].isin(["savings", "investments"])]
    level = other_groups.groupby("year_month")["balance_pln"].sum()
    change = level.diff()
    change.iloc[0] = level.iloc[0] - ACCOUNTS["initial_balance"].sum()
    merged = monthly.set_index("year_month")["unallocated"]
    np.testing.assert_allclose(merged.to_numpy(), change.reindex(merged.index).to_numpy())

    april = monthly.set_index("year_month").loc["2025-04"]
    assert april["savings"] == pytest.approx(200)  # 500 in, 300 back out


def test_refund_reduces_its_bucket_not_income(twelve_months):
    refunded = _frame(
        list(twelve_months.to_dict("records"))
        + [_row("2025-05-20", "Checking", 200.0, "Food", "Dining out", txn_type="deposit")]
    )
    monthly = build_budget_health_monthly(refunded, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    may = monthly.set_index("year_month").loc["2025-05"]
    assert may["guilt_free"] == pytest.approx(1_000)
    assert may["gross_income"] == pytest.approx(10_000)


def test_unmapped_category_is_reported(twelve_months, caplog):
    rows = list(twelve_months.to_dict("records"))
    rows.append(_row("2025-12-01", "Checking", -90.0, "Gadgets"))
    monthly = build_budget_health_monthly(_frame(rows), ACCOUNTS, as_of="2025-12-31",
                                          config=CONFIG)
    assert monthly.iloc[-1]["unmapped_ttm"] == pytest.approx(90)
    assert any("no bucket" in r.message for r in caplog.records)


def test_ttm_needs_exactly_twelve_complete_months(twelve_months):
    monthly = build_budget_health_monthly(
        twelve_months, ACCOUNTS, as_of="2025-12-31", config=CONFIG
    )
    assert monthly["take_home_ttm"].iloc[:11].isna().all()
    assert monthly["take_home_ttm"].iloc[11] == pytest.approx(96_000)


def test_partial_month_is_flagged_and_has_no_ttm(twelve_months):
    rows = list(twelve_months.to_dict("records")) + _standard_month("2026-01")
    monthly = build_budget_health_monthly(
        _frame(rows), ACCOUNTS, as_of="2026-01-15", config=CONFIG
    )
    last = monthly.iloc[-1]
    assert last["year_month"] == "2026-01"
    assert bool(last["is_partial_month"]) is True
    assert np.isnan(last["take_home_ttm"])
    assert monthly.iloc[-2]["take_home_ttm"] == pytest.approx(96_000)


def test_last_day_of_month_is_not_partial(twelve_months):
    monthly = build_budget_health_monthly(
        twelve_months, ACCOUNTS, as_of="2025-12-31", config=CONFIG
    )
    assert not monthly["is_partial_month"].any()


def test_future_rows_are_ignored(twelve_months):
    rows = list(twelve_months.to_dict("records"))
    rows.append(_row("2026-03-01", "Checking", -5_000.0, "Leisure"))
    monthly = build_budget_health_monthly(_frame(rows), ACCOUNTS, as_of="2025-12-31",
                                          config=CONFIG)
    assert monthly["year_month"].max() == "2025-12"
    assert monthly["guilt_free"].sum() == pytest.approx(12 * 1_200)


def test_without_accounts_flow_buckets_are_nan(twelve_months, caplog):
    monthly = build_budget_health_monthly(twelve_months, None, as_of="2025-12-31",
                                          config=CONFIG)
    assert monthly["savings"].isna().all()
    assert monthly["investments"].isna().all()
    assert monthly.iloc[0]["unallocated"] == pytest.approx(3_000)
    assert any("no account metadata" in r.message for r in caplog.records)


def test_empty_frame_returns_empty_result():
    empty = _frame([_row("2025-01-01", "Checking", 1.0, "Income")]).iloc[0:0]
    monthly = build_budget_health_monthly(empty, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    assert monthly.empty


# ---------------------------------------------------------------------------
# Balances and FX
# ---------------------------------------------------------------------------


def test_implied_fx_rates_from_converted_rows():
    df = _frame(
        [
            _row("2025-01-10", "EUR Acc", 100.0, "Income", txn_type="deposit",
                 currency="EUR", amount_pln=400.0),
            _row("2025-01-10", "Checking", -10.0, "Food"),
        ]
    )
    rates = implied_fx_rates(df)
    assert rates["currency"].tolist() == ["EUR"]
    assert rates["rate"].iloc[0] == pytest.approx(4.0)


def test_balances_add_initial_balance_and_value_fx_at_month_end():
    df = _frame(
        [
            _row("2025-01-10", "EUR Acc", 100.0, "Income", txn_type="deposit",
                 currency="EUR", amount_pln=400.0),
            _row("2025-02-10", "EUR Acc", -50.0, "Transfer", txn_type="transfer",
                 to_account="Checking", to_amount=205.0, currency="EUR", amount_pln=-205.0),
            _row("2025-02-11", "Checking", -5.0, "Food", "Groceries"),
        ]
    )
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-02-28", config=CONFIG)
    assert list(balances.columns) == BALANCE_COLUMNS
    feb = balances[balances["year_month"] == "2025-02"].set_index("account")
    assert feb.loc["EUR Acc", "balance"] == pytest.approx(50.0)
    assert feb.loc["EUR Acc", "balance_pln"] == pytest.approx(50.0 * 4.1)
    assert feb.loc["Checking", "balance"] == pytest.approx(1000.0 + 205.0 - 5.0)
    jan = balances[balances["year_month"] == "2025-01"].set_index("account")
    assert jan.loc["EUR Acc", "balance_pln"] == pytest.approx(400.0)


def test_balances_start_at_the_account_open_month():
    accounts = ACCOUNTS.copy()
    accounts.loc[accounts["account"] == "Savings", "initial_date"] = pd.Timestamp("2025-03-01")
    df = _frame(
        [
            _row("2025-01-10", "Checking", -5.0, "Food", "Groceries"),
            _row("2025-03-10", "Checking", -100.0, "Transfer", txn_type="transfer",
                 to_account="Savings"),
        ]
    )
    balances = build_account_balances(df, accounts, as_of="2025-03-31", config=CONFIG)
    savings_months = balances.loc[balances["account"] == "Savings", "year_month"].tolist()
    assert savings_months == ["2025-03"]


def test_unknown_account_takes_currency_from_its_rows(caplog):
    df = _frame(
        [
            _row("2025-01-10", "Lost EUR", 10.0, "Income", txn_type="deposit",
                 currency="EUR", amount_pln=43.0),
        ]
    )
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-01-31", config=CONFIG)
    lost = balances[balances["account"] == "Lost EUR"].iloc[0]
    assert lost["currency"] == "EUR"
    assert lost["group"] == "other"
    assert lost["balance_pln"] == pytest.approx(43.0)
    assert any("not in MMEX account list" in r.message for r in caplog.records)


def test_fx_before_first_known_rate_is_logged_as_imputation(caplog):
    df = _frame(
        [
            _row("2025-01-10", "EUR Acc", 100.0, "Income", txn_type="deposit",
                 currency="EUR", amount_pln=np.nan),
            _row("2025-03-10", "EUR Acc", -10.0, "Food", currency="EUR", amount_pln=-42.0),
        ]
    )
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-03-31", config=CONFIG)
    jan = balances[(balances["account"] == "EUR Acc") & (balances["year_month"] == "2025-01")]
    assert jan["fx_rate"].iloc[0] == pytest.approx(4.2)
    assert any("IMPUTATION | column=fx_rate" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Lifestyle creep
# ---------------------------------------------------------------------------


def _monthly_stub(year_values: dict[int, tuple[float, float]], months: int = 12) -> pd.DataFrame:
    rows = []
    for year, (take_home, living) in year_values.items():
        for m in range(1, months + 1):
            rows.append(
                {
                    "year_month": f"{year}-{m:02d}",
                    "is_partial_month": False,
                    "take_home": take_home / 12,
                    "fixed": living / 12,
                    "guilt_free": 0.0,
                }
            )
    return pd.DataFrame(rows)


def test_annual_creep_marginal_spend_rate_is_hand_computed():
    monthly = _monthly_stub({2023: (100_000, 60_000), 2024: (150_000, 70_000)})
    annual = build_annual_creep(monthly).set_index("year")
    assert annual.loc[2024, "n_months"] == 12
    assert annual.loc[2024, "living_share"] == pytest.approx(70_000 / 150_000)
    assert annual.loc[2024, "marginal_spend_rate"] == pytest.approx(10_000 / 50_000)
    assert np.isnan(annual.loc[2023, "marginal_spend_rate"])


def test_annual_creep_decline_year_rate_is_nan():
    """D10: the ratio is defined only for extra take-home."""
    monthly = _monthly_stub({2023: (100_000, 60_000), 2024: (90_000, 70_000)})
    annual = build_annual_creep(monthly).set_index("year")
    assert annual.loc[2024, "delta_take_home"] == pytest.approx(-10_000)
    assert np.isnan(annual.loc[2024, "marginal_spend_rate"])


def test_annual_creep_flat_pay_rate_is_nan():
    monthly = _monthly_stub({2023: (100_000, 60_000), 2024: (100_000, 70_000)})
    annual = build_annual_creep(monthly).set_index("year")
    assert np.isnan(annual.loc[2024, "marginal_spend_rate"])


def test_annual_creep_growth_year_rate_unchanged():
    monthly = _monthly_stub({2023: (100_000, 60_000), 2024: (150_000, 70_000)})
    annual = build_annual_creep(monthly).set_index("year")
    assert annual.loc[2024, "marginal_spend_rate"] == pytest.approx(10_000 / 50_000)


def test_annual_creep_drops_incomplete_years_and_gap_deltas():
    full = _monthly_stub({2021: (100_000, 50_000), 2023: (120_000, 60_000)})
    partial = _monthly_stub({2022: (110_000, 55_000)}, months=6)
    annual = build_annual_creep(pd.concat([full, partial]).sort_values("year_month"))
    assert annual["year"].tolist() == [2021, 2023]
    assert annual["delta_take_home"].isna().all()


# ---------------------------------------------------------------------------
# Scorecard
# ---------------------------------------------------------------------------


def _checks(summary: dict) -> dict[str, dict]:
    return {c["check"]: c for c in summary["checks"]}


def test_summary_scores_against_sethi_targets(twelve_months):
    monthly = build_budget_health_monthly(twelve_months, ACCOUNTS, as_of="2025-12-31",
                                          config=CONFIG)
    balances = build_account_balances(twelve_months, ACCOUNTS, as_of="2025-12-31",
                                      config=CONFIG)
    summary = summarise_budget_health(monthly, balances, config=CONFIG)
    checks = _checks(summary)

    assert summary["as_of_month"] == "2025-12"
    assert summary["take_home_monthly"] == pytest.approx(8_000)
    assert set(summary["shares"]) == set(ALLOCATION_COLUMNS)
    assert checks["Fixed costs"]["status"] == "ok"            # 47.5% <= 60%
    assert checks["Guilt-free spending"]["status"] == "ok"    # 15% <= 35%
    assert checks["Savings goals"]["status"] == "ok"          # 6.25% >= 5%
    assert checks["Investments"]["status"] == "attention"     # 0% < 10%
    assert checks["Unallocated surplus"]["status"] == "info"
    # 12 × 500 in savings ÷ 3,800 fixed per month
    assert checks["Emergency fund"]["value"] == pytest.approx(6_000 / 3_800)
    assert checks["Emergency fund"]["status"] == "attention"
    assert checks["Emergency fund"]["unit"] == "months"
    assert checks["Reserve above emergency-fund ceiling"]["value"] == 0.0  # 6,000 < 6 × 3,800
    assert checks["Debt balance"]["status"] == "ok"
    assert summary["mapping_confirmed"] is False


def test_fixed_share_exactly_at_upper_target_is_ok():
    monthly = pd.DataFrame(
        {
            "year_month": ["2025-12"],
            "take_home_ttm": [100.0],
            "fixed_ttm": [60.0],
            "guilt_free_ttm": [0.0],
            **{f"{c}_share_ttm": [0.0] for c in ALLOCATION_COLUMNS},
        }
    )
    monthly["fixed_share_ttm"] = 0.60
    checks = _checks(summarise_budget_health(monthly, None, config=CONFIG))
    assert checks["Fixed costs"]["status"] == "ok"


def test_debt_balance_is_flagged(twelve_months):
    rows = list(twelve_months.to_dict("records"))
    rows.append(_row("2025-12-05", "Card", -250.0, "Leisure"))
    df = _frame(rows)
    monthly = build_budget_health_monthly(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    debt = _checks(summarise_budget_health(monthly, balances, config=CONFIG))["Debt balance"]
    assert debt["status"] == "attention"
    assert debt["value"] == pytest.approx(-250)


def test_summary_with_short_history_is_insufficient_data():
    rows = [r for m in ("2025-01", "2025-02") for r in _standard_month(m)]
    df = _frame(rows)
    monthly = build_budget_health_monthly(df, ACCOUNTS, as_of="2025-02-28", config=CONFIG)
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-02-28", config=CONFIG)
    summary = summarise_budget_health(monthly, balances, config=CONFIG)
    statuses = {c["status"] for c in summary["checks"]}
    assert statuses == {"insufficient data"}
    assert summary["as_of_month"] is None


def test_reserve_above_ceiling_is_savings_minus_six_months_of_fixed(twelve_months):
    rows = list(twelve_months.to_dict("records"))
    rows.append(_row("2025-12-20", "Checking", -30_000.0, "Transfer", txn_type="transfer",
                     to_account="Savings"))
    rows.append(_row("2025-12-20", "Checking", 30_000.0, "Income", "Bonus", txn_type="deposit"))
    df = _frame(rows)
    monthly = build_budget_health_monthly(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    checks = _checks(summarise_budget_health(monthly, balances, config=CONFIG))
    # savings 6,000 + 30,000 = 36,000; ceiling 6 × 3,800 = 22,800
    assert checks["Reserve above emergency-fund ceiling"]["value"] == pytest.approx(13_200)
    assert checks["Emergency fund"]["status"] == "ok"


# ---------------------------------------------------------------------------
# Next 100 PLN priority order
# ---------------------------------------------------------------------------


def test_resolve_config_deep_merges_plan():
    from src.analysis.budget_health import _resolve_config

    resolved = _resolve_config({"plan": {"salary_pln": 5_000.0}})
    assert resolved["plan"] == {
        "salary_pln": 5_000.0,
        "salary_day": None,
        "investment_pln": None,
    }


def _summary_with_reserve(twelve_months: pd.DataFrame, transfer: float = 0.0) -> dict:
    rows = list(twelve_months.to_dict("records"))
    if transfer:
        rows.append(
            _row(
                "2025-12-20",
                "Checking",
                -transfer,
                "Transfer",
                txn_type="transfer",
                to_account="Savings",
            )
        )
    df = _frame(rows)
    monthly = build_budget_health_monthly(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    return summarise_budget_health(monthly, balances, config=CONFIG)


def test_next_100_steps_invest_is_next_when_emergency_fund_is_ok(twelve_months):
    summary = _summary_with_reserve(twelve_months, transfer=30_000.0)
    steps = next_100_steps(summary, config=CONFIG)

    assert [s["step"] for s in steps] == [
        "High-interest debt",
        "Employer match",
        "Emergency fund 3–6 months",
        "Invest ≥10% of take-home",
        "Savings goals, then guilt-free",
    ]
    assert steps[0]["status"] == "ok"
    assert steps[1]["status"] == "not applicable"
    assert steps[1]["detail"] == "no employer plan (employer_match: false)"
    assert steps[2]["status"] == "ok"
    assert steps[3]["status"] == "attention"
    assert steps[3]["is_next"] is True
    assert steps[4]["status"] == "ok"
    assert sum(s["is_next"] for s in steps) == 1


def test_next_100_steps_marks_debt_first_and_only_one_next(twelve_months):
    rows = list(twelve_months.to_dict("records"))
    rows.append(_row("2025-12-05", "Card", -250.0, "Leisure"))
    df = _frame(rows)
    monthly = build_budget_health_monthly(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    summary = summarise_budget_health(monthly, balances, config=CONFIG)

    steps = next_100_steps(summary, config=CONFIG)

    assert steps[0]["status"] == "attention"
    assert steps[0]["is_next"] is True
    assert sum(s["is_next"] for s in steps) == 1


def test_next_100_steps_short_history_marks_first_insufficient_step_next():
    rows = [r for m in ("2025-01", "2025-02") for r in _standard_month(m)]
    df = _frame(rows)
    monthly = build_budget_health_monthly(df, ACCOUNTS, as_of="2025-02-28", config=CONFIG)
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-02-28", config=CONFIG)
    summary = summarise_budget_health(monthly, balances, config=CONFIG)

    steps = next_100_steps(summary, config=CONFIG)

    assert steps[0]["status"] == "insufficient data"
    assert steps[0]["is_next"] is True
    assert all(not s["is_next"] for s in steps[1:])


def test_next_100_steps_employer_match_true_is_not_measured():
    steps = next_100_steps(summary={"checks": []}, config={"employer_match": True})
    assert steps[1]["status"] == "not measured"
    assert steps[1]["detail"] == "not visible in MMEX"


# ---------------------------------------------------------------------------
# Reserve vs ceiling trend
# ---------------------------------------------------------------------------


def test_build_reserve_trend_hand_computed(twelve_months):
    rows = list(twelve_months.to_dict("records"))
    rows.append(
        _row(
            "2025-12-20",
            "Checking",
            -30_000.0,
            "Transfer",
            txn_type="transfer",
            to_account="Savings",
        )
    )
    df = _frame(rows)
    monthly = build_budget_health_monthly(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)
    balances = build_account_balances(df, ACCOUNTS, as_of="2025-12-31", config=CONFIG)

    trend = build_reserve_trend(monthly, balances, config=CONFIG)

    assert list(trend.columns) == RESERVE_TREND_COLUMNS
    assert len(trend) == 12
    assert trend["ceiling"].iloc[:-1].isna().all()
    assert trend["above_ceiling"].iloc[:-1].isna().all()
    assert trend["ceiling"].iloc[-1] == pytest.approx(6 * 3_800.0)
    assert trend["savings_balance"].iloc[-1] == pytest.approx(36_000.0)
    assert trend["above_ceiling"].iloc[-1] == pytest.approx(36_000.0 - 22_800.0)


def test_build_reserve_trend_empty_without_balances(twelve_months):
    monthly = build_budget_health_monthly(
        twelve_months, ACCOUNTS, as_of="2025-12-31", config=CONFIG
    )
    trend = build_reserve_trend(monthly, None, config=CONFIG)
    assert list(trend.columns) == RESERVE_TREND_COLUMNS
    assert trend.empty


def test_build_reserve_trend_empty_without_savings_group(twelve_months):
    monthly = build_budget_health_monthly(
        twelve_months, ACCOUNTS, as_of="2025-12-31", config=CONFIG
    )
    balances = pd.DataFrame(
        {
            "year_month": ["2025-12"],
            "group": ["cash"],
            "balance_pln": [1_000.0],
        }
    )
    trend = build_reserve_trend(monthly, balances, config=CONFIG)
    assert trend.empty


# ---------------------------------------------------------------------------
# Reserve transfers
# ---------------------------------------------------------------------------


def _reserve_fixture() -> pd.DataFrame:
    return _frame([
        _row("2025-01-05", "Savings", -1_000.0, "Transfer", txn_type="transfer",
             to_account="Checking"),
        _row("2025-01-12", "Savings", -500.0, "Transfer", txn_type="transfer",
             to_account="Mum"),
        _row("2025-01-10", "Checking", -800.0, "Transfer", txn_type="transfer",
             to_account="Savings"),
        _row("2025-01-28", "Savings", 200.0, "Income", "Interest", txn_type="deposit"),
        _row("2025-02-20", "Savings", -1_500.0, "Transfer", txn_type="transfer",
             to_account="Checking"),
        _row("2025-04-05", "Savings", -999.0, "Transfer", txn_type="transfer",
             to_account="Checking"),
    ])


def test_build_reserve_transfers_counts_only_savings_to_cash():
    df = _reserve_fixture()
    transfers = build_reserve_transfers(df, ACCOUNTS, as_of="2025-03-15", config=CONFIG)
    jan = transfers[transfers["year_month"] == "2025-01"].iloc[0]

    assert jan["moved_to_cash"] == pytest.approx(1_000.0)
    assert jan["n_transfers"] == 1
    assert jan["transfer_days"] == "5"


def test_build_reserve_transfers_income_into_reserve():
    df = _reserve_fixture()
    transfers = build_reserve_transfers(df, ACCOUNTS, as_of="2025-03-15", config=CONFIG)
    jan = transfers[transfers["year_month"] == "2025-01"].iloc[0]

    assert jan["income_into_reserve"] == pytest.approx(200.0)
    assert jan["income_days"] == "28"


def test_build_reserve_transfers_continuous_months_and_partial_flag():
    df = _reserve_fixture()
    transfers = build_reserve_transfers(df, ACCOUNTS, as_of="2025-03-15", config=CONFIG)

    assert transfers["year_month"].tolist() == ["2025-01", "2025-02", "2025-03"]
    assert list(transfers["is_partial_month"]) == [False, False, True]
    march = transfers[transfers["year_month"] == "2025-03"].iloc[0]
    assert march["moved_to_cash"] == 0.0
    assert march["transfer_days"] == ""


def test_build_reserve_transfers_excludes_future_rows():
    df = _reserve_fixture()
    transfers = build_reserve_transfers(df, ACCOUNTS, as_of="2025-03-15", config=CONFIG)
    assert int(transfers["n_transfers"].sum()) == 2  # January + February, not April
    assert "2025-04" not in transfers["year_month"].tolist()


def test_build_reserve_transfers_days_string_lists_sorted_unique_days():
    df = _frame([
        _row("2025-01-20", "Savings", -100.0, "Transfer", txn_type="transfer",
             to_account="Checking"),
        _row("2025-01-05", "Savings", -200.0, "Transfer", txn_type="transfer",
             to_account="Checking"),
        _row("2025-01-20", "Savings", -300.0, "Transfer", txn_type="transfer",
             to_account="Checking"),
    ])
    transfers = build_reserve_transfers(df, ACCOUNTS, as_of="2025-01-31", config=CONFIG)
    jan = transfers.iloc[0]
    assert jan["transfer_days"] == "5, 20"
    assert jan["n_transfers"] == 3
    assert jan["moved_to_cash"] == pytest.approx(600.0)


def test_build_reserve_transfers_without_accounts_is_empty_and_warns(caplog):
    df = _reserve_fixture()
    with caplog.at_level("WARNING", logger="src.analysis.budget_health"):
        transfers = build_reserve_transfers(df, None, as_of="2025-03-15", config=CONFIG)

    assert list(transfers.columns) == RESERVE_TRANSFER_COLUMNS
    assert transfers.empty
    assert any("no account metadata" in record.message for record in caplog.records)


def _transfer_summary_frame(
    moved: list[float],
    days: list[str] | None = None,
    months: list[str] | None = None,
    income_days: list[str] | None = None,
) -> pd.DataFrame:
    n = len(moved)
    return pd.DataFrame(
        {
            "year_month": months or [f"2025-{i:02d}" for i in range(1, n + 1)],
            "is_partial_month": [False] * n,
            "moved_to_cash": moved,
            "n_transfers": [1] * n,
            "transfer_days": days or ["10"] * n,
            "income_into_reserve": [0.0] * n,
            "income_days": income_days or [""] * n,
        }
    )


def test_summarise_reserve_transfers_cv_on_known_series():
    transfers = _transfer_summary_frame([10.0, 20.0, 30.0], days=["10", "10, 20", "15"])
    summary = summarise_reserve_transfers(transfers, None, config=CONFIG)

    assert summary["n_months"] == 3
    assert summary["mean"] == pytest.approx(20.0)
    assert summary["min"] == pytest.approx(10.0)
    assert summary["max"] == pytest.approx(30.0)
    assert summary["cv"] == pytest.approx(0.5)
    assert summary["months_with_transfer"] == 3
    assert summary["distinct_transfer_days"] == 3


def test_summarise_reserve_transfers_income_days_range():
    transfers = _transfer_summary_frame(
        [1_000.0] * 3, income_days=["28", "28, 31", "30"]
    )
    summary = summarise_reserve_transfers(transfers, None, config=CONFIG)
    assert summary["income_days_range"] == "28–31"


def test_summarise_reserve_transfers_income_days_range_single_day():
    transfers = _transfer_summary_frame([1_000.0], income_days=["28"])
    summary = summarise_reserve_transfers(transfers, None, config=CONFIG)
    assert summary["income_days_range"] == "28"


def test_salary_adherence_amount_boundary_includes_exactly_ten_percent():
    transfers = _transfer_summary_frame([1_100.0, 900.0, 1_001.0, 899.0])
    summary = summarise_reserve_transfers(
        transfers, None, config={**CONFIG, "plan": {"salary_pln": 1_000.0}}
    )
    assert summary["salary_adherence"] == {"on_plan": 3, "of": 4}


def test_salary_adherence_day_boundary_is_plus_minus_three():
    transfers = _transfer_summary_frame([1_000.0] * 4, days=["10", "13", "14", "13, 20"])
    summary = summarise_reserve_transfers(
        transfers,
        None,
        config={**CONFIG, "plan": {"salary_pln": 1_000.0, "salary_day": 10}},
    )
    # 10, 13 and 13 (±3 on the earliest day) count; 14 does not.
    assert summary["salary_adherence"] == {"on_plan": 3, "of": 4}


def test_investment_adherence_counts_months_at_or_above_plan():
    transfers = _transfer_summary_frame([1_000.0] * 3)
    monthly = pd.DataFrame(
        {
            "year_month": ["2025-01", "2025-02", "2025-03"],
            "is_partial_month": [False, False, False],
            "investments": [100.0, 50.0, 200.0],
        }
    )
    summary = summarise_reserve_transfers(
        transfers, monthly, config={**CONFIG, "plan": {"investment_pln": 100.0}}
    )
    assert summary["investment_adherence"] == {"on_plan": 2, "of": 3}


def test_adherence_is_none_when_plan_keys_are_null():
    transfers = _transfer_summary_frame([1_000.0] * 3)
    summary = summarise_reserve_transfers(transfers, None, config=CONFIG)
    assert summary["salary_adherence"] is None
    assert summary["investment_adherence"] is None


def test_investment_adherence_none_without_monthly():
    transfers = _transfer_summary_frame([1_000.0] * 3)
    summary = summarise_reserve_transfers(
        transfers, None, config={**CONFIG, "plan": {"investment_pln": 100.0}}
    )
    assert summary["investment_adherence"] is None
