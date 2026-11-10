"""Tests for src.analysis.frozen_report — frozen-money report logic."""

import pandas as pd
import pytest

from src.analysis.frozen_report import (
    _base_description,
    _note_status,
    _return_label,
    _sanitise_sheet_name,
    build_frozen_report,
    build_frozen_rows,
    build_summary,
    detect_frozen_accounts,
    parse_account_currency,
)


@pytest.fixture
def frozen_test_df() -> pd.DataFrame:
    """Synthetic validated DataFrame with PLN and EUR Frozen accounts.

    Covers: normal IN→OUT (;zwrot), partial release, OUT-without-;zwrot,
    IN-with-;zwrot, blank notes, reused description, EUR cross-currency,
    OVER-released case, no-return case (IN only, no matching OUT).
    """
    df = pd.DataFrame(
        {
            "transaction_id": range(1, 21),
            "date": pd.to_datetime(
                [
                    "2024-01-05",  # 1:  IN  PLN  normal (;zwrot on release)
                    "2024-01-20",  # 2:  OUT PLN  release of #1
                    "2024-02-01",  # 3:  IN  PLN  partial-release case
                    "2024-02-10",  # 4:  IN  PLN  partial-release case (same process)
                    "2024-03-01",  # 5:  OUT PLN  partial release (only half)
                    "2024-03-15",  # 6:  OUT PLN  OUT-without-;zwrot (must still count as release)
                    "2024-03-20",  # 7:  IN  PLN  IN carrying ;zwrot (flag but still IN by direction)
                    "2024-03-30",  # 8:  OUT PLN  release of #7 (without ;zwrot)
                    "2024-04-01",  # 9:  IN  PLN  blank notes
                    "2024-04-02",  # 10: IN  PLN  blank notes
                    "2024-04-03",  # 11: OUT PLN  blank notes release
                    "2024-04-10",  # 12: IN  PLN  reused desc "Acme Rentals"
                    "2024-04-15",  # 13: IN  PLN  reused desc "Acme Rentals"
                    "2024-04-20",  # 14: OUT PLN  reused desc "Acme Rentals" release
                    "2024-04-22",  # 15: OUT PLN  reused desc "Acme Rentals" release
                    "2024-05-01",  # 16: IN  EUR  EUR IN
                    "2024-05-10",  # 17: OUT EUR  EUR OUT (;zwrot)
                    "2024-06-01",  # 18: IN  PLN  OVER-released: 100 IN, 150 OUT
                    "2024-06-10",  # 19: OUT PLN  OVER-released: release exceeds IN
                    "2024-07-01",  # 20: IN  PLN  no-return case (no OUT)
                ]
            ),
            "account": [
                "Current Account",
                "Frozen",  # 1-2
                "Current Account",
                "Current Account",  # 3-4
                "Frozen",
                "Frozen",  # 5-6
                "Current Account",
                "Frozen",  # 7-8 (#7=IN from Current Account)
                "Current Account",
                "Current Account",  # 9-10
                "Frozen",  # 11
                "Current Account",
                "Current Account",  # 12-13
                "Frozen",
                "Frozen",  # 14-15
                "Current Account",  # 16
                "Frozen [EUR]",  # 17
                "Current Account",
                "Frozen",  # 18-19
                "Current Account",  # 20
            ],
            "to_account": [
                "Frozen",
                "Current Account",  # 1-2
                "Frozen",
                "Frozen",  # 3-4
                "Current Account",
                "Current Account",  # 5-6
                "Frozen",
                "Current Account",  # 7-8 (#7=IN to Frozen)
                "Frozen",
                "Frozen",  # 9-10
                "Current Account",  # 11
                "Frozen",
                "Frozen",  # 12-13
                "Current Account",
                "Current Account",  # 14-15
                "Frozen [EUR]",  # 16
                "Current Account",  # 17
                "Frozen",
                "Current Account",  # 18-19
                "Frozen",  # 20
            ],
            "payee": [
                "Shop A",
                "Shop A",  # 1-2
                "PartialCo",
                "PartialCo",  # 3-4
                "PartialCo",
                "NoZwrot Co",  # 5-6
                "ZwrotIn",
                "ZwrotIn",  # 7-8
                "Blank Co",
                "Blank Co",  # 9-10
                "Blank Co",  # 11
                "Acme Rentals",
                "Acme Rentals",  # 12-13
                "Acme Rentals",
                "Acme Rentals",  # 14-15
                "Herring",  # 16
                "Herring",  # 17
                "OverCo",
                "OverCo",  # 18-19
                "Frozen Only",  # 20
            ],
            "amount": [
                -200.0,
                -200.0,  # 1-2
                -150.0,
                -100.0,  # 3-4
                -150.0,
                -150.0,  # 5-6
                -100.0,
                -100.0,  # 7-8
                -200.0,
                -200.0,  # 9-10
                -200.0,  # 11
                -300.0,
                -300.0,  # 12-13
                -300.0,
                -300.0,  # 14-15
                -200.0,  # 16
                -180.0,  # 17 (EUR: 180)
                -100.0,
                -150.0,  # 18-19
                -120.0,  # 20
            ],
            "to_amount": [
                200.0,
                200.0,  # 1-2
                150.0,
                100.0,  # 3-4
                150.0,
                150.0,  # 5-6
                100.0,
                100.0,  # 7-8
                200.0,
                200.0,  # 9-10
                200.0,  # 11
                300.0,
                300.0,  # 12-13
                300.0,
                300.0,  # 14-15
                200.0,  # 16
                180.0,  # 17 (EUR: 180)
                100.0,
                150.0,  # 18-19
                120.0,  # 20
            ],
            "amount_pln": [
                -200.0,
                -200.0,  # 1-2
                -150.0,
                -100.0,  # 3-4
                -150.0,
                -150.0,  # 5-6
                -100.0,
                -100.0,  # 7-8
                -200.0,
                -200.0,  # 9-10
                -200.0,  # 11
                -300.0,
                -300.0,  # 12-13
                -300.0,
                -300.0,  # 14-15
                -200.0,  # 16
                -900.0,  # 17 (EUR 180 ≈ 900 PLN)
                -100.0,
                -150.0,  # 18-19
                -120.0,  # 20
            ],
            "currency": [
                "PLN",
                "PLN",  # 1-2
                "PLN",
                "PLN",  # 3-4
                "PLN",
                "PLN",  # 5-6
                "PLN",
                "PLN",  # 7-8
                "PLN",
                "PLN",  # 9-10
                "PLN",  # 11
                "PLN",
                "PLN",  # 12-13
                "PLN",
                "PLN",  # 14-15
                "EUR",  # 16
                "EUR",  # 17
                "PLN",
                "PLN",  # 18-19
                "PLN",  # 20
            ],
            "type": [
                "transfer",
                "transfer",  # 1-2
                "transfer",
                "transfer",  # 3-4
                "transfer",
                "transfer",  # 5-6
                "transfer",
                "transfer",  # 7-8
                "transfer",
                "transfer",  # 9-10
                "transfer",  # 11
                "transfer",
                "transfer",  # 12-13
                "transfer",
                "transfer",  # 14-15
                "transfer",  # 16
                "transfer",  # 17
                "transfer",
                "transfer",  # 18-19
                "transfer",  # 20
            ],
            "notes": [
                "reservation;zwrot",
                "reservation;zwrot",  # 1-2
                "partial test;zwrot",
                "partial test;zwrot",  # 3-4
                "partial test;zwrot",
                "payment auth",  # 5-6 (OUT #6 has NO ;zwrot)
                "IN with zwrot;zwrot",
                "IN with zwrot",  # 7 (IN carries ;zwrot), 8
                None,
                None,  # 9-10 blank
                None,  # 11 blank
                "Acme Rentals;zwrot",
                "Acme Rentals;zwrot",  # 12-13
                "Acme Rentals;zwrot",
                "Acme Rentals;zwrot",  # 14-15
                "Herring Shoes EUR;zwrot",  # 16
                "Herring Shoes EUR;zwrot",  # 17
                "over test;zwrot",
                "over test;zwrot",  # 18-19
                "frozen only;zwrot",  # 20
            ],
            "category": ["Shopping"] * 20,
            "subcategory": [""] * 20,
            "transaction_number": [""] * 20,
        }
    )
    return df


# ── Unit tests: helpers ──────────────────────────────────────────────────────


def test_parse_account_currency_suffix():
    assert parse_account_currency("Frozen [EUR]") == "EUR"
    assert parse_account_currency("Frozen [GBP]") == "GBP"


def test_parse_account_currency_no_suffix():
    assert parse_account_currency("Frozen") == "PLN"
    assert parse_account_currency("Checking") == "PLN"


def test_parse_account_currency_whitespace():
    assert parse_account_currency("Frozen [USD]  ") == "USD"


def test_base_description_strips_zwrot():
    assert _base_description("reservation;zwrot") == "reservation"
    assert _base_description("partial test;zwrot") == "partial test"


def test_base_description_keeps_mid_semicolon():
    assert _base_description("a;b;zwrot") == "a;b"


def test_base_description_blank():
    assert _base_description(None) == "<no description>"
    assert _base_description("") == "<no description>"
    assert _base_description("   ") == "<no description>"


def test_base_description_no_zwrot():
    assert _base_description("payment auth") == "payment auth"


def test_note_status_zwrot():
    assert _note_status("reservation;zwrot") == "IN_with_zwrot"


def test_note_status_ok():
    assert _note_status("payment auth") == "ok"


def test_note_status_blank():
    assert _note_status(None) == "blank"
    assert _note_status("") == "blank"


def test_sanitise_sheet_name():
    assert _sanitise_sheet_name("Frozen [EUR]") == "Frozen EUR"
    assert _sanitise_sheet_name("Frozen") == "Frozen"
    assert _sanitise_sheet_name("Frozen [GBP]") == "Frozen GBP"


# ── detect_frozen_accounts ──────────────────────────────────────────────────


def test_detect_frozen_accounts(frozen_test_df):
    accounts = detect_frozen_accounts(frozen_test_df)
    assert "Frozen" in accounts
    assert "Frozen [EUR]" in accounts
    assert len(accounts) == 2


def test_detect_no_frozen():
    df = pd.DataFrame(
        {
            "account": ["Checking"],
            "to_account": ["Savings"],
        }
    )
    assert detect_frozen_accounts(df) == []


# ── build_frozen_rows ──────────────────────────────────────────────────────


def test_build_frozen_rows_direction_pln(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    assert list(rows["direction"]) == [
        "IN",
        "OUT",  # 1-2  normal
        "IN",
        "IN",  # 3-4  partial INs
        "OUT",
        "OUT",  # 5-6  partial OUT + OUT-no-zwrot
        "IN",
        "OUT",  # 7-8  IN-with-zwrot (IN by direction) + OUT
        "IN",
        "IN",  # 9-10 blank
        "OUT",  # 11   blank release
        "IN",
        "IN",  # 12-13 Acme Rentals INs
        "OUT",
        "OUT",  # 14-15 Acme Rentals OUTs
        "IN",
        "OUT",  # 18-19 OVER-released
        "IN",  # 20   no-return (IN only)
    ]


def test_build_frozen_rows_frozen_amount_in(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    in_row = rows[rows["transaction_id"] == 1].iloc[0]
    assert in_row["frozen_amount"] == 200.0
    assert in_row["from_account"] == "Current Account"


def test_build_frozen_rows_frozen_amount_out(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    out_row = rows[rows["transaction_id"] == 2].iloc[0]
    assert out_row["frozen_amount"] == 200.0
    assert out_row["from_account"] == "Current Account"


def test_build_frozen_rows_eur_account(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen [EUR]")
    assert len(rows) == 2
    # IN: #16 — frozen_amount comes from abs(to_amount)
    in_row = rows[rows["transaction_id"] == 16].iloc[0]
    assert in_row["direction"] == "IN"
    assert in_row["frozen_amount"] == 200.0
    # OUT: #17 — frozen_amount comes from abs(amount)
    out_row = rows[rows["transaction_id"] == 17].iloc[0]
    assert out_row["direction"] == "OUT"
    assert out_row["frozen_amount"] == 180.0


def test_build_frozen_rows_blank_label(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    blank_rows = rows[rows["process"] == "<no description>"]
    assert len(blank_rows) == 3
    assert (blank_rows["return_label"] == "blank").all()


def test_build_frozen_rows_in_with_zwrot(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    t7 = rows[rows["transaction_id"] == 7].iloc[0]
    assert t7["direction"] == "IN"
    assert t7["return_label"] == "full returns"


def test_build_frozen_rows_out_without_zwrot(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    t6 = rows[rows["transaction_id"] == 6].iloc[0]
    assert t6["direction"] == "OUT"
    assert t6["frozen_amount"] == 150.0
    assert t6["return_label"] == "overflow"


# ── build_summary ───────────────────────────────────────────────────────────


def test_build_summary_released(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    process_row = summary[summary["process"] == "reservation"]
    assert process_row.iloc[0]["n_in"] == 1
    assert process_row.iloc[0]["n_out"] == 1
    assert process_row.iloc[0]["net"] == 0.0
    assert process_row.iloc[0]["return_label"] == "full returns"


def test_build_summary_partial(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    process_row = summary[summary["process"] == "partial test"]
    assert process_row.iloc[0]["n_in"] == 2
    assert process_row.iloc[0]["n_out"] == 1
    assert process_row.iloc[0]["sum_in"] > process_row.iloc[0]["sum_out"]
    assert process_row.iloc[0]["return_label"] == "partial returns"


def test_build_summary_out_without_zwrot(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    process_row = summary[summary["process"] == "payment auth"]
    assert process_row.iloc[0]["n_in"] == 0
    assert process_row.iloc[0]["n_out"] == 1
    assert process_row.iloc[0]["return_label"] == "overflow"


def test_build_summary_in_with_zwrot(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    process_row = summary[summary["process"] == "IN with zwrot"]
    assert process_row.iloc[0]["n_in"] == 1
    assert process_row.iloc[0]["n_out"] == 1
    assert process_row.iloc[0]["net"] == 0.0
    assert process_row.iloc[0]["return_label"] == "full returns"


def test_build_summary_blank_notes(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    process_row = summary[summary["process"] == "<no description>"]
    assert process_row.iloc[0]["n_in"] == 2
    assert process_row.iloc[0]["n_out"] == 1
    assert process_row.iloc[0]["return_label"] == "blank"


def test_build_summary_reused_description(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    process_row = summary[summary["process"] == "Acme Rentals"]
    assert process_row.iloc[0]["n_in"] == 2
    assert process_row.iloc[0]["n_out"] == 2
    assert process_row.iloc[0]["net"] == 0.0
    assert process_row.iloc[0]["return_label"] == "full returns"


def test_build_summary_over_released(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    process_row = summary[summary["process"] == "over test"]
    assert process_row.iloc[0]["n_in"] == 1
    assert process_row.iloc[0]["n_out"] == 1
    assert process_row.iloc[0]["net"] == -50.0
    assert process_row.iloc[0]["return_label"] == "overflow"


def test_build_summary_eur_account(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen [EUR]")
    summary = build_summary({"Frozen [EUR]": frozen_rows})
    process_row = summary[summary["process"] == "Herring Shoes EUR"]
    assert process_row.iloc[0]["currency"] == "EUR"
    assert process_row.iloc[0]["n_in"] == 1
    assert process_row.iloc[0]["n_out"] == 1
    assert process_row.iloc[0]["sum_in"] == 200.0
    assert process_row.iloc[0]["sum_out"] == 180.0
    assert process_row.iloc[0]["net"] == 20.0
    assert process_row.iloc[0]["return_label"] == "partial returns"


def test_build_summary_sorting(frozen_test_df):
    frozen_rows = build_frozen_rows(frozen_test_df, "Frozen")
    summary = build_summary({"Frozen": frozen_rows})
    nets = list(summary["net"])
    assert nets[0] > 0  # largest net first
    net_positive = [n for n in nets if n > 0]
    assert all(n > 0 for n in net_positive)
    after_positive = nets[len(net_positive) :]
    assert all(n <= 0 for n in after_positive)


# ── _return_label unit tests ─────────────────────────────────────────────────


def test_return_label_helper():
    assert _return_label(100, 0, True) == "blank"
    assert _return_label(100, 0, False) == "no return"
    assert _return_label(100, 100, False) == "full returns"
    assert _return_label(100, 100.003, False) == "full returns"
    assert _return_label(100, 50, False) == "partial returns"
    assert _return_label(100, 150, False) == "overflow"
    assert _return_label(0, 100, False) == "overflow"  # orphan OUT


# ── build_frozen_rows new tests ──────────────────────────────────────────────


def test_build_frozen_rows_total_in_out_side_by_side(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    partial_rows = rows[rows["process"] == "partial test"]
    assert (partial_rows["total_in"] == 250.0).all()
    assert (partial_rows["total_out"] == 150.0).all()
    cols = list(rows.columns)
    ti_idx = cols.index("total_in")
    to_idx = cols.index("total_out")
    assert to_idx == ti_idx + 1


def test_build_frozen_rows_no_return_label(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    t20 = rows[rows["transaction_id"] == 20].iloc[0]
    assert t20["total_in"] == 120.0
    assert t20["total_out"] == 0.0
    assert t20["return_label"] == "no return"


def test_build_frozen_rows_total_in_shown_on_in_row(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    sartolane_in = rows[(rows["process"] == "Acme Rentals") & (rows["direction"] == "IN")]
    assert (sartolane_in["total_in"] == 600.0).all()


# ── pct_returned, days_inactive, stale ──────────────────────────────────────


def test_build_frozen_rows_pct_returned(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    cols = list(rows.columns)
    ni = cols.index("net")
    assert cols.index("pct_returned") == ni + 1

    reservation = rows[rows["process"] == "reservation"]
    assert (reservation["pct_returned"] == 100.0).all()

    partial = rows[rows["process"] == "partial test"]
    assert (partial["pct_returned"] == 60.0).all()

    no_return = rows[rows["process"] == "frozen only"]
    assert (no_return["pct_returned"] == 0.0).all()

    overflow = rows[rows["process"] == "over test"]
    assert (overflow["pct_returned"] == 150.0).all()

    orphan = rows[rows["process"] == "payment auth"]
    assert (orphan["pct_returned"].isna()).all()


def test_build_frozen_rows_days_inactive_and_stale(frozen_test_df):
    ref = pd.Timestamp("2025-06-01")
    rows = build_frozen_rows(frozen_test_df, "Frozen", reference_date=ref)
    cols = list(rows.columns)
    rl = cols.index("return_label")
    assert cols.index("days_inactive") == rl + 1
    assert cols.index("stale") == rl + 2

    no_return = rows[rows["process"] == "frozen only"]
    assert (no_return["days_inactive"] > 300).all()
    assert no_return["stale"].all()

    partial = rows[rows["process"] == "partial test"]
    assert partial["stale"].all()

    released = rows[rows["process"] == "reservation"]
    assert not released["stale"].all()

    overflow = rows[rows["process"] == "payment auth"]
    assert not overflow["stale"].all()

    blank = rows[rows["process"] == "<no description>"]
    assert not blank["stale"].all()


def test_build_frozen_rows_days_inactive_defaults_to_today(frozen_test_df):
    rows = build_frozen_rows(frozen_test_df, "Frozen")
    assert "days_inactive" in rows.columns
    assert "stale" in rows.columns
    assert rows["days_inactive"].dtype.kind == "i"


# ── build_frozen_report (integration) ───────────────────────────────────────


def test_build_frozen_report_sheets(frozen_test_df):
    sheets = build_frozen_report(frozen_test_df)
    assert "Frozen" in sheets
    assert "Frozen EUR" in sheets
    assert "Summary" in sheets


def test_build_frozen_report_no_frozen():
    df = pd.DataFrame(
        {
            "account": ["Checking"],
            "to_account": ["Savings"],
            "amount": [100.0],
            "to_amount": [100.0],
            "amount_pln": [100.0],
            "currency": ["PLN"],
            "type": ["deposit"],
            "transaction_id": [1],
            "date": [pd.Timestamp("2024-01-01")],
            "payee": ["Test"],
            "notes": [None],
            "category": [""],
            "subcategory": [""],
            "transaction_number": [""],
        }
    )
    sheets = build_frozen_report(df)
    assert sheets == {}


def test_build_frozen_report_summary_row_count(frozen_test_df):
    sheets = build_frozen_report(frozen_test_df)
    assert "Summary" in sheets
    assert len(sheets["Summary"]) == 9
