"""
Frozen-money report: detect Frozen accounts, classify freeze-in / release-out
transactions, and produce per-account + summary DataFrames.

Usage:
    from src.analysis.frozen_report import build_frozen_report
    sheets = build_frozen_report(validated_df)
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


def detect_frozen_accounts(df: pd.DataFrame) -> list[str]:
    """Return sorted list of account names starting with 'Frozen' (case-insensitive)."""
    accounts: set[str] = set()
    for col in ("account", "to_account"):
        series = df[col].dropna().astype(str)
        accounts.update(series[series.str.lower().str.startswith("frozen")])
    frozen = sorted(accounts)
    logger.info(f"Detected {len(frozen)} frozen account(s): {frozen}")
    return frozen


def parse_account_currency(account_name: str) -> str:
    """Extract currency from a '[XXX]' suffix, otherwise return 'PLN'."""
    match = re.search(r"\[([A-Z]{3})\]$", account_name.strip())
    return match.group(1) if match else "PLN"


def _base_description(notes: Any) -> str:
    """Strip the final ';zwrot' segment (case-insensitive) from notes. NaN/blank → '<no description>'."""
    if pd.isna(notes) or str(notes).strip() == "":
        return "<no description>"
    text = str(notes).strip()
    return re.sub(r";\s*zwrot\s*$", "", text, flags=re.IGNORECASE).strip()


def _note_status(notes: Any) -> str:
    if pd.isna(notes) or str(notes).strip() == "":
        return "blank"
    text = str(notes).strip()
    if re.search(r";zwrot$", text, re.IGNORECASE):
        return "IN_with_zwrot"
    return "ok"


def _return_label(total_in: float, total_out: float, is_blank: bool) -> str:
    if is_blank:
        return "blank"
    if total_out == 0 and total_in > 0:
        return "no return"
    if abs(total_out - total_in) <= 0.005:
        return "full returns"
    if total_out < total_in:
        return "partial returns"
    return "overflow"


def _process_totals(rows: pd.DataFrame, reference_date: pd.Timestamp | None = None) -> pd.DataFrame:
    if reference_date is None:
        reference_date = pd.Timestamp.now().normalize()

    ins = rows[rows["direction"] == "IN"]
    outs = rows[rows["direction"] == "OUT"]
    totals_in = ins.groupby("process", sort=False)["frozen_amount"].sum()
    totals_out = outs.groupby("process", sort=False)["frozen_amount"].sum()
    last_date = rows.groupby("process", sort=False)["date"].max()

    process_df = (
        pd.DataFrame({"total_in": totals_in, "total_out": totals_out, "last_date": last_date})
        .fillna({"total_in": 0.0, "total_out": 0.0})
        .reset_index()
    )
    process_df["net"] = process_df["total_in"] - process_df["total_out"]
    process_df["pct_returned"] = (
        (process_df["total_out"] / process_df["total_in"].replace(0, float("nan")) * 100)
        .round(1)
        .where(process_df["total_in"] > 0, float("nan"))
    )
    process_df["days_inactive"] = (reference_date - process_df["last_date"]).dt.days
    process_df["is_blank"] = process_df["process"] == "<no description>"
    process_df["return_label"] = process_df.apply(
        lambda r: _return_label(r["total_in"], r["total_out"], r["is_blank"]),
        axis=1,
    )
    process_df["stale"] = (process_df["days_inactive"] > 30) & (
        process_df["return_label"].isin(["no return", "partial returns"])
    )

    return rows.merge(
        process_df[
            [
                "process",
                "total_in",
                "total_out",
                "net",
                "pct_returned",
                "return_label",
                "days_inactive",
                "stale",
            ]
        ],
        on="process",
        how="left",
    )


def build_frozen_rows(
    df: pd.DataFrame,
    frozen_account: str,
    reference_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Filter the validated DataFrame to rows involving `frozen_account` and derive
    report columns: direction, frozen_amount, process, total_in, total_out,
    net, pct_returned, return_label, days_inactive, stale, from_account.

    Parameters
    ----------
    df : pd.DataFrame
        Validated transactions (must include account, to_account, amount,
        to_amount, currency, notes, type, transaction_id, date, payee).
    frozen_account : str
        Exact account name of the Frozen account.
    reference_date : pd.Timestamp, optional
        Reference date for computing days_inactive. Defaults to today.

    Returns
    -------
    pd.DataFrame
        One row per raw transaction involving the Frozen account.
        Columns: transaction_id, date, from_account, to_account, payee,
        process, direction, frozen_amount, total_in, total_out, net,
        pct_returned, return_label, days_inactive, stale, amount_pln,
        currency, raw_notes, type.
    """
    mask = (df["account"] == frozen_account) | (df["to_account"] == frozen_account)
    rows = df.loc[mask].copy()

    direction: list[str] = []
    frozen_amounts: list[float] = []
    processes: list[str] = []
    note_statuses: list[str] = []
    from_accounts: list[str] = []

    for _, row in rows.iterrows():
        if row["to_account"] == frozen_account:
            direction.append("IN")
            frozen_amounts.append(abs(row["to_amount"]) if pd.notna(row["to_amount"]) else 0.0)
            from_accounts.append(str(row["account"]) if pd.notna(row["account"]) else "")
        else:
            direction.append("OUT")
            frozen_amounts.append(abs(row["amount"]) if pd.notna(row["amount"]) else 0.0)
            from_accounts.append(str(row["to_account"]) if pd.notna(row["to_account"]) else "")

        processes.append(_base_description(row.get("notes")))
        note_statuses.append(_note_status(row.get("notes")))

    result = rows.assign(
        direction=direction,
        frozen_amount=frozen_amounts,
        amount_pln=rows["amount_pln"].abs(),
        process=processes,
        from_account=from_accounts,
        raw_notes=rows["notes"].fillna(""),
        _note_status=note_statuses,
    )

    result = _process_totals(result, reference_date)

    cols = [
        "transaction_id",
        "date",
        "from_account",
        "to_account",
        "payee",
        "process",
        "direction",
        "frozen_amount",
        "total_in",
        "total_out",
        "net",
        "pct_returned",
        "return_label",
        "days_inactive",
        "stale",
        "amount_pln",
        "currency",
        "raw_notes",
        "type",
    ]
    result = result[cols + ["_note_status"]]
    result = result.sort_values(["date", "transaction_id"]).reset_index(drop=True)

    n_in = (result["direction"] == "IN").sum()
    n_out = (result["direction"] == "OUT").sum()
    logger.info(f"  {frozen_account}: {n_in} IN, {n_out} OUT")

    blank_rows = result[result["_note_status"] == "blank"]
    if len(blank_rows) > 0:
        logger.warning(
            f"  {frozen_account}: {len(blank_rows)} row(s) with blank notes "
            f"(txns: {blank_rows['transaction_id'].tolist()})"
        )

    inverted_in = result[
        (result["direction"] == "IN") & (result["_note_status"] == "IN_with_zwrot")
    ]
    if len(inverted_in) > 0:
        logger.warning(
            f"  {frozen_account}: {len(inverted_in)} IN row(s) unexpectedly carrying ';zwrot' "
            f"(txns: {inverted_in['transaction_id'].tolist()})"
        )

    return result.drop(columns=["_note_status"])


def build_summary(
    frozen_rows_by_account: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Build a Summary DataFrame with one row per (frozen_account, process).

    Parameters
    ----------
    frozen_rows_by_account : dict[str, pd.DataFrame]
        Output of build_frozen_rows keyed by frozen account name.

    Returns
    -------
    pd.DataFrame
        Columns: frozen_account, currency, process, n_in, n_out,
        sum_in, sum_out, net, return_label, first_date, last_date.
    """
    summary_rows: list[dict[str, Any]] = []

    for frozen_account, df in frozen_rows_by_account.items():
        account_currency = parse_account_currency(frozen_account)
        for process, grp in df.groupby("process", sort=False):
            ins = grp[grp["direction"] == "IN"]
            outs = grp[grp["direction"] == "OUT"]

            n_in = len(ins)
            n_out = len(outs)
            sum_in = ins["frozen_amount"].sum()
            sum_out = outs["frozen_amount"].sum()
            net = round(sum_in - sum_out, 10)

            is_blank = process == "<no description>"
            return_label = _return_label(round(sum_in, 10), round(sum_out, 10), is_blank)

            summary_rows.append(
                {
                    "frozen_account": frozen_account,
                    "currency": account_currency,
                    "process": process,
                    "n_in": n_in,
                    "n_out": n_out,
                    "sum_in": round(sum_in, 2),
                    "sum_out": round(sum_out, 2),
                    "net": round(net, 2),
                    "return_label": return_label,
                    "first_date": grp["date"].min(),
                    "last_date": grp["date"].max(),
                }
            )

    summary_df = pd.DataFrame(summary_rows)

    if not summary_df.empty:
        net_positive = summary_df[summary_df["net"] > 0].sort_values("net", ascending=False)
        net_non_positive = summary_df[summary_df["net"] <= 0].sort_values(["last_date", "process"])
        summary_df = pd.concat([net_positive, net_non_positive], ignore_index=True)

    return summary_df


def build_frozen_report(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Produce a complete frozen-money report as a dict of DataFrames keyed by sheet name.

    Parameters
    ----------
    df : pd.DataFrame
        Validated transactions from data/interim/transactions_validated.parquet.

    Returns
    -------
    dict[str, pd.DataFrame]
        One entry per Frozen account (keyed by sanitised sheet name) plus a
        'Summary' entry.
    """
    frozen_accounts = detect_frozen_accounts(df)
    if not frozen_accounts:
        logger.info("No Frozen accounts found in the data.")
        return {}

    frozen_rows_by_account: dict[str, pd.DataFrame] = {}
    for account in frozen_accounts:
        frozen_rows_by_account[account] = build_frozen_rows(df, account)

    sheets: dict[str, pd.DataFrame] = {}
    for account, rows_df in frozen_rows_by_account.items():
        sheet_name = _sanitise_sheet_name(account)
        sheets[sheet_name] = rows_df

    sheets["Summary"] = build_summary(frozen_rows_by_account)
    return sheets


def _sanitise_sheet_name(name: str) -> str:
    """Replace '[' and ']' with spaces, strip, truncate to 31 chars (Excel limit)."""
    sanitised = name.replace("[", " ").replace("]", " ").strip()
    # Collapse multiple spaces
    sanitised = " ".join(sanitised.split())
    return sanitised[:31]
