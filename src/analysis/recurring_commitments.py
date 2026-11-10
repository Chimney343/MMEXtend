"""
Recurring-commitment detection for MMEX transaction data.

Builds a register of payees whose withdrawals recur on a stable cadence
(rent, utilities, subscriptions, installments), a renewal calendar derived
from each series' median gap, and per-occurrence events that flag price
changes. Detection is descriptive: the output carries factual flags only,
never action prescriptions.

Usage:
    from src.analysis.recurring_commitments import (
        build_recurring_category_summary,
        build_recurring_commitments,
    )
    register, events = build_recurring_commitments(df_featured)
    category_summary = build_recurring_category_summary(register, df_featured)
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

REGISTER_COLUMNS = [
    "payee",
    "category",
    "subcategory",
    "n_transactions",
    "n_distinct_months",
    "first_seen",
    "last_seen",
    "median_gap_days",
    "cadence_class",
    "is_active",
    "amount_current",
    "amount_median",
    "amount_cv_trailing",
    "monthly_run_rate",
    "run_rate_window_months",
    "next_expected_date",
    "days_overdue",
    "is_lapsed",
    "n_price_changes",
    "last_price_change_date",
    "last_price_change_pct",
    "has_price_change_12m",
    "is_renewal_due_30d",
]

EVENT_COLUMNS = [
    "payee",
    "date",
    "year_month",
    "amount",
    "prev_amount",
    "is_price_change",
]

CATEGORY_SUMMARY_COLUMNS = [
    "category",
    "n_months",
    "active_months_ttm",
    "last_month",
    "cadence_class",
    "is_active",
    "avg_monthly_spend",
    "spend_cv",
    "is_steady",
    "n_commitments",
    "n_active_commitments",
    "n_lapsed_commitments",
    "recurring_run_rate",
    "recurring_coverage",
    "share_of_spend",
    "n_price_changes_12m",
    "top_payee",
]

DEFAULT_CONFIG: dict[str, Any] = {
    "min_distinct_months": 6,
    "active_window_days": 90,
    "amount_cv_threshold": 0.10,
    "price_change_pct": 0.05,
    "price_change_abs_pln": 5.0,
    "run_rate_window_months": 12,
    "min_amount_pln": 0.0,
}

# (cadence_class, min median gap days, max median gap days)
CADENCE_WINDOWS: tuple[tuple[str, int, int], ...] = (
    ("weekly", 5, 10),
    ("biweekly", 11, 18),
    ("monthly", 24, 38),
    ("quarterly", 80, 105),
    ("semiannual", 165, 200),
    ("annual", 340, 390),
)

UNCATEGORISED = "(uncategorised)"


def _resolve_config(config: dict[str, Any] | None) -> dict[str, Any]:
    """Merge caller overrides over the code defaults, ignoring unknown keys."""
    resolved = dict(DEFAULT_CONFIG)
    if config:
        resolved.update({key: value for key, value in config.items() if key in DEFAULT_CONFIG})
    return resolved


def _as_of_timestamp(as_of: Any) -> pd.Timestamp:
    """Coerce as_of to a naive, date-normalised Timestamp (default: today)."""
    if as_of is None:
        return pd.Timestamp.today().normalize()
    ts = pd.Timestamp(as_of)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def _empty_register() -> pd.DataFrame:
    return pd.DataFrame(columns=REGISTER_COLUMNS)


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=EVENT_COLUMNS)


def _classify_cadence(median_gap_days: float) -> str:
    """
    Map a median gap in days to a cadence class.

    Windows: weekly 5–10, biweekly 11–18, monthly 24–38, quarterly 80–105,
    semiannual 165–200, annual 340–390. Anything else (including NaN) is
    ``"irregular"``.
    """
    if median_gap_days is None or pd.isna(median_gap_days):
        return "irregular"
    gap = float(median_gap_days)
    for name, low, high in CADENCE_WINDOWS:
        if low <= gap <= high:
            return name
    return "irregular"


def _filter_series_rows(
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, int]:
    """
    Keep the rows eligible for series detection.

    Applies the local as_of cutoff, withdrawals only, non-transfers, non-blank
    payees and the minimum absolute amount. Returns the filtered frame with
    standardised ``amount`` (positive PLN) plus the count of rows dropped for a
    null/blank payee.
    """
    columns = ["payee", "date", "year_month", "amount", "category", "subcategory"]
    if df is None or df.empty or "date" not in df.columns or "type" not in df.columns:
        return pd.DataFrame(columns=columns), 0

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work[work["date"].notna() & (work["date"] <= as_of)]
    work = work[
        work["type"].astype(str).str.lower().isin(["withdrawal", "expense"])
    ]

    if "is_transfer" in work.columns:
        work = work[~work["is_transfer"].fillna(False).astype(bool)]

    payees = work["payee"].astype("string").str.strip()
    blank = payees.isna() | (payees == "")
    n_no_payee = int(blank.sum())
    work = work[~blank]
    work["payee"] = payees[~blank]

    amount_col = "amount_pln" if "amount_pln" in work.columns else "amount"
    amounts = pd.to_numeric(work[amount_col], errors="coerce").abs()
    keep = amounts.notna() & (amounts >= float(config["min_amount_pln"]))
    work = work[keep]
    work["amount"] = amounts[keep]

    work["year_month"] = work["date"].dt.strftime("%Y-%m")
    for optional in ("category", "subcategory"):
        if optional not in work.columns:
            work[optional] = None

    return work[columns].reset_index(drop=True), n_no_payee


def _detect_price_changes(
    series_df: pd.DataFrame,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Flag payment dates whose total jumped materially.

    Amounts are summed per distinct payment date first, so same-day split
    purchases and installments are compared as one charge instead of
    alternating against each other. A change is flagged when
    ``|Δ| / prev > price_change_pct`` OR ``|Δ| > price_change_abs_pln``. The
    first date never has a change. Returns the series sorted by date with
    ``prev_amount`` (previous payment date's total) and ``is_price_change``
    columns added to every occurrence on that date.
    """
    cfg = _resolve_config(config)
    series = series_df.sort_values("date").reset_index(drop=True).copy()

    daily = series.groupby("date", as_index=False)["amount"].sum().sort_values("date")
    prev = daily["amount"].shift(1)

    with np.errstate(divide="ignore", invalid="ignore"):
        pct = (daily["amount"] - prev) / prev

    abs_delta = (daily["amount"] - prev).abs()
    flagged = (
        prev.notna()
        & (
            (pct.abs() > float(cfg["price_change_pct"]))
            | (abs_delta > float(cfg["price_change_abs_pln"]))
        )
    )

    daily["prev_amount"] = prev
    daily["is_price_change"] = flagged.fillna(False).astype(bool)
    daily["change_pct"] = pct

    return series.merge(
        daily[["date", "prev_amount", "is_price_change", "change_pct"]],
        on="date",
        how="left",
    )


def _coefficient_of_variation(values: np.ndarray) -> float:
    """Coefficient of variation (std / mean), 0.0 for a single observation."""
    if len(values) == 0:
        return np.nan
    mean = float(values.mean())
    if mean == 0:
        return np.nan
    if len(values) < 2:
        return 0.0
    return float(np.std(values, ddof=1) / mean)


def _trailing_cv(daily: pd.Series, config: dict[str, Any]) -> float:
    """
    Change-tolerant amount CV for the trailing window.

    CV is computed on daily totals so split purchases do not inflate it. When
    raw CV exceeds the threshold, the window is split at the largest material
    price change and the within-regime CV (max of the segments) is returned.
    This keeps a payee with one large step change in the register while still
    rejecting payees whose amounts vary continuously (groceries).
    """
    amounts = daily.to_numpy(dtype=float)
    raw_cv = _coefficient_of_variation(amounts)
    threshold = float(config["amount_cv_threshold"])
    if not np.isfinite(raw_cv) or raw_cv <= threshold or len(amounts) < 3:
        return raw_cv

    prev = daily.shift(1)
    delta = daily - prev
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = delta / prev
    material = prev.notna() & (
        (pct.abs() > float(config["price_change_pct"]))
        | (delta.abs() > float(config["price_change_abs_pln"]))
    )
    if not material.any():
        return raw_cv

    change_date = delta.abs().where(material).idxmax()
    before = daily[daily.index < change_date]
    after = daily[daily.index >= change_date]
    if len(before) < 2:
        return raw_cv

    cv_before = _coefficient_of_variation(before.to_numpy(dtype=float))
    cv_after = (
        _coefficient_of_variation(after.to_numpy(dtype=float))
        if len(after) >= 2
        else 0.0
    )
    effective = max(cv_before, cv_after)
    return effective if np.isfinite(effective) else raw_cv


def _month_span(start: pd.Timestamp, end: pd.Timestamp) -> int:
    """Inclusive number of calendar months touched between two dates."""
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def _modal_amount(window: pd.DataFrame) -> float:
    """Most frequent amount in the window; ties resolve to the most recent."""
    rounded = window["amount"].round(2)
    counts = rounded.value_counts()
    modal_values = counts[counts == counts.max()].index
    candidates = window[rounded.isin(modal_values)].sort_values("date")
    return float(candidates["amount"].iloc[-1])


def _dominant_category(series: pd.DataFrame, column: str) -> str | None:
    """Most frequent non-blank value of a categorical column."""
    values = series[column].dropna().astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return None
    return str(values.value_counts().index[0])


def _series_record(
    payee: str,
    series: pd.DataFrame,
    as_of: pd.Timestamp,
    config: dict[str, Any],
) -> tuple[dict[str, Any] | None, pd.DataFrame | None, str | None]:
    """
    Build one register row plus its event frame.

    Returns ``(record, events, exclusion_reason)``. ``record`` is None when the
    series is not a commitment; ``exclusion_reason`` is one of
    ``"too_few_months"``, ``"no_recent_activity"`` or ``"variable"``.
    """
    n_distinct_months = int(series["year_month"].nunique())
    if n_distinct_months < int(config["min_distinct_months"]):
        return None, None, "too_few_months"

    first_seen = series["date"].min()
    last_seen = series["date"].max()

    unique_dates = series["date"].sort_values().drop_duplicates()
    gaps = unique_dates.diff().dt.days.dropna()
    median_gap_days = float(gaps.median()) if not gaps.empty else np.nan
    cadence_class = _classify_cadence(median_gap_days)

    window_months = int(config["run_rate_window_months"])
    window_start = as_of - pd.DateOffset(months=window_months)
    window = series[series["date"] >= window_start]
    if window.empty:
        return None, None, "no_recent_activity"

    # A single observation cannot evidence a current cadence for frequent
    # payees (it is stale activity); only semiannual/annual series are
    # expected to appear once or twice in a 12-month window.
    min_window_rows = 1 if pd.notna(median_gap_days) and median_gap_days >= 150 else 2
    if len(window) < min_window_rows:
        return None, None, "no_recent_activity"

    window_amounts = window["amount"].to_numpy(dtype=float)
    window_daily = window.groupby("date")["amount"].sum().sort_index()
    amount_cv = _trailing_cv(window_daily, config)

    if not np.isfinite(amount_cv) or amount_cv > float(config["amount_cv_threshold"]):
        return None, None, "variable"

    effective_start = max(window_start, first_seen)
    months_spanned = max(
        1,
        min(window_months, _month_span(effective_start, as_of)),
    )
    monthly_run_rate = float(window_amounts.sum() / months_spanned)

    next_expected_date = (
        last_seen + pd.Timedelta(days=median_gap_days)
        if pd.notna(median_gap_days)
        else pd.NaT
    )
    days_overdue = (
        max(0, int((as_of - next_expected_date).days))
        if pd.notna(next_expected_date)
        else 0
    )

    is_active = bool((as_of - last_seen).days <= int(config["active_window_days"]))
    is_lapsed = bool(
        not is_active
        and cadence_class != "irregular"
        and pd.notna(median_gap_days)
        and days_overdue > 1.5 * median_gap_days
    )
    is_renewal_due_30d = bool(
        not is_lapsed
        and cadence_class != "irregular"
        and pd.notna(next_expected_date)
        and next_expected_date <= as_of + pd.Timedelta(days=30)
    )

    events = _detect_price_changes(series, config)
    changes = events[events["is_price_change"]]
    last_change = changes.iloc[-1] if not changes.empty else None

    change_window_start = as_of - pd.DateOffset(months=12)
    has_price_change_12m = bool(
        (not changes.empty) and (changes["date"] >= change_window_start).any()
    )

    record = {
        "payee": payee,
        "category": _dominant_category(series, "category") or UNCATEGORISED,
        "subcategory": _dominant_category(series, "subcategory"),
        "n_transactions": int(len(series)),
        "n_distinct_months": n_distinct_months,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "median_gap_days": median_gap_days,
        "cadence_class": cadence_class,
        "is_active": is_active,
        "amount_current": _modal_amount(window),
        "amount_median": float(np.median(window_amounts)),
        "amount_cv_trailing": amount_cv,
        "monthly_run_rate": monthly_run_rate,
        "run_rate_window_months": months_spanned,
        "next_expected_date": next_expected_date,
        "days_overdue": days_overdue,
        "is_lapsed": is_lapsed,
        "n_price_changes": int(changes["date"].nunique()),
        "last_price_change_date": last_change["date"] if last_change is not None else pd.NaT,
        "last_price_change_pct": (
            float(last_change["change_pct"])
            if last_change is not None
            else np.nan
        ),
        "has_price_change_12m": has_price_change_12m,
        "is_renewal_due_30d": is_renewal_due_30d,
    }

    event_frame = events[["payee", "date", "year_month", "amount", "prev_amount", "is_price_change"]]
    return record, event_frame, None


def build_recurring_commitments(
    df: pd.DataFrame,
    as_of: pd.Timestamp | str | None = None,
    config: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect recurring commitments and return (register, events).

    A *series* is every non-transfer withdrawal for one exact payee string on
    or before ``as_of``. A *commitment* is a series with at least
    ``min_distinct_months`` distinct months whose trailing-window daily-total
    CV (tolerant of one material price step) is at or below
    ``amount_cv_threshold``.

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features(). Uses ``amount_pln`` when present,
        otherwise ``amount``. The input frame is never mutated.
    as_of : Timestamp | str | None
        Analysis cutoff (default: today). Future-dated scheduled rows inside
        ``df`` are ignored by this module only.
    config : dict | None
        Overrides for the ``recurring:`` config section. Missing keys fall
        back to DEFAULT_CONFIG, so a missing section is safe.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        Register (one row per commitment, see REGISTER_COLUMNS) and events
        (one row per occurrence of a register member, see EVENT_COLUMNS).
        Both are empty — with the documented columns — when nothing qualifies.
    """
    cfg = _resolve_config(config)
    cutoff = _as_of_timestamp(as_of)

    rows, n_no_payee = _filter_series_rows(df, cutoff, cfg)
    if rows.empty:
        logger.info(
            "Recurring commitments: 0 series detected | 0 commitments | 0 active | "
            f"0 excluded (variable) | {n_no_payee} rows without payee"
        )
        return _empty_register(), _empty_events()

    records: list[dict[str, Any]] = []
    event_frames: list[pd.DataFrame] = []
    n_excluded_variable = 0
    n_excluded_stale = 0

    for payee, series in rows.groupby("payee", sort=False):
        record, events, reason = _series_record(str(payee), series, cutoff, cfg)
        if record is None:
            if reason == "variable":
                n_excluded_variable += 1
            elif reason == "no_recent_activity":
                n_excluded_stale += 1
            continue
        records.append(record)
        event_frames.append(events)

    if not records:
        register = _empty_register()
        events = _empty_events()
    else:
        register = pd.DataFrame(records, columns=REGISTER_COLUMNS)
        register = register.sort_values(
            ["monthly_run_rate", "payee"], ascending=[False, True]
        ).reset_index(drop=True)
        events = pd.concat(event_frames, ignore_index=True)
        events = events.sort_values(["payee", "date"]).reset_index(drop=True)

    n_active = int(register["is_active"].sum()) if not register.empty else 0
    logger.info(
        f"Recurring commitments: {rows['payee'].nunique()} series detected | "
        f"{len(register)} commitments | {n_active} active | "
        f"{n_excluded_variable} excluded (CV) | "
        f"{n_excluded_stale} excluded (no recent activity) | "
        f"{n_no_payee} rows without payee"
    )
    return register, events


def _empty_category_summary() -> pd.DataFrame:
    return pd.DataFrame(columns=CATEGORY_SUMMARY_COLUMNS)


def build_recurring_category_summary(
    register: pd.DataFrame,
    df: pd.DataFrame,
    as_of: pd.Timestamp | str | None = None,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Summarise recurring commitments and spend stability by category.

    Category-level spend totals are much more variable than a single payee's
    series, so this is a roll-up rather than a second payee-style register:
    the active payee commitments are summed per category and compared with the
    category's average monthly spend over up to 12 complete months.

    Parameters
    ----------
    register : pd.DataFrame
        Output of build_recurring_commitments(). May be empty.
    df : pd.DataFrame
        Output of engineer_features(). Used for category spend, not mutated.
    as_of : Timestamp | str | None
        Analysis cutoff (default: today). The current partial month is excluded
        from the average-spend window.
    config : dict | None
        The ``recurring:`` config section. ``category_cv_threshold`` (default
        0.25) sets the ``is_steady`` boundary; other keys are reused.

    Returns
    -------
    pd.DataFrame
        One row per category, columns CATEGORY_SUMMARY_COLUMNS, sorted by
        average monthly spend.
    """
    cfg = _resolve_config(config)
    category_cv_threshold = float(
        (config or {}).get("category_cv_threshold", 0.25)
    )
    cutoff = _as_of_timestamp(as_of)

    if df is None or df.empty or "date" not in df.columns or "type" not in df.columns:
        logger.info("Category summary: 0 categories")
        return _empty_category_summary()

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work[work["date"].notna() & (work["date"] <= cutoff)]
    work = work[
        work["type"].astype(str).str.lower().isin(["withdrawal", "expense"])
    ]
    if "is_transfer" in work.columns:
        work = work[~work["is_transfer"].fillna(False).astype(bool)]
    if work.empty:
        logger.info("Category summary: 0 categories")
        return _empty_category_summary()

    amount_col = "amount_pln" if "amount_pln" in work.columns else "amount"
    work["amount"] = pd.to_numeric(work[amount_col], errors="coerce").abs()
    work = work[work["amount"].notna()]
    if "category" not in work.columns:
        work["category"] = UNCATEGORISED
    work["category"] = work["category"].astype("string").str.strip()
    work["category"] = work["category"].fillna(UNCATEGORISED)
    work.loc[work["category"] == "", "category"] = UNCATEGORISED
    work["year_month"] = work["date"].dt.strftime("%Y-%m")

    # Fixed calendar window of complete months: the month before as_of and the
    # preceding months. Data gaps inside the window count as zero spend.
    window_size = int(cfg["run_rate_window_months"])
    end_month = cutoff.replace(day=1) - pd.DateOffset(months=1)
    recent_months = pd.date_range(
        end=end_month, periods=window_size, freq="MS"
    ).strftime("%Y-%m").tolist()

    reg = register if register is not None else pd.DataFrame()
    if not reg.empty:
        reg = reg.copy()
        reg["category"] = reg["category"].astype("string").str.strip()
        reg["category"] = reg["category"].fillna(UNCATEGORISED)
        reg.loc[reg["category"] == "", "category"] = UNCATEGORISED
        active_reg = reg[reg["is_active"].fillna(False).astype(bool)]
    else:
        reg = pd.DataFrame(
            columns=[
                "category",
                "payee",
                "monthly_run_rate",
                "is_active",
                "is_lapsed",
                "has_price_change_12m",
            ]
        )
        active_reg = reg

    n_commitments = reg.groupby("category").size()
    n_active = active_reg.groupby("category").size()
    n_lapsed = reg[reg["is_lapsed"].fillna(False).astype(bool)].groupby("category").size()
    recurring = active_reg.groupby("category")["monthly_run_rate"].sum()
    price_changes = reg[reg["has_price_change_12m"].fillna(False).astype(bool)].groupby(
        "category"
    ).size()
    top_payee = (
        reg.sort_values("monthly_run_rate", ascending=False)
        .drop_duplicates("category")
        .set_index("category")["payee"]
    )

    records: list[dict[str, Any]] = []
    for category, group in work.groupby("category", sort=False):
        first_month = str(group["year_month"].min())
        last_month = str(group["year_month"].max())
        # Skip categories with no spend in the recent complete-month window.
        if last_month < recent_months[0]:
            continue

        # Average over up to 12 recent complete months, but never before the
        # category's first spend (a new category is not penalised for zeros).
        start_month = max(first_month, recent_months[0])
        window_months = [month for month in recent_months if month >= start_month]
        if not window_months:
            continue
        monthly = (
            group.groupby("year_month")["amount"]
            .sum()
            .reindex(window_months, fill_value=0.0)
        )
        avg_monthly_spend = float(monthly.mean())
        spend_cv = _coefficient_of_variation(monthly.to_numpy(dtype=float))

        spend_dates = pd.to_datetime(
            [f"{month}-01" for month in sorted(group["year_month"].unique())]
        )
        gaps = pd.Series(spend_dates).diff().dt.days.dropna()
        median_gap = float(gaps.median()) if not gaps.empty else np.nan
        cadence_class = _classify_cadence(median_gap)
        last_seen = group["date"].max()
        is_active = bool(
            (cutoff - last_seen).days <= int(cfg["active_window_days"])
        )
        n_months = int(group["year_month"].nunique())
        active_months_ttm = int((monthly > 0).sum())

        top = top_payee.get(category)
        records.append(
            {
                "category": str(category),
                "n_months": n_months,
                "active_months_ttm": active_months_ttm,
                "last_month": last_month,
                "cadence_class": cadence_class,
                "is_active": is_active,
                "avg_monthly_spend": avg_monthly_spend,
                "spend_cv": spend_cv,
                "is_steady": bool(
                    active_months_ttm >= int(cfg["min_distinct_months"])
                    and is_active
                    and np.isfinite(spend_cv)
                    and spend_cv <= category_cv_threshold
                ),
                "n_commitments": int(n_commitments.get(category, 0)),
                "n_active_commitments": int(n_active.get(category, 0)),
                "n_lapsed_commitments": int(n_lapsed.get(category, 0)),
                "recurring_run_rate": float(recurring.get(category, 0.0)),
                "n_price_changes_12m": int(price_changes.get(category, 0)),
                "top_payee": str(top) if pd.notna(top) else "\u2014",
            }
        )

    if not records:
        logger.info("Category summary: 0 categories")
        return _empty_category_summary()

    summary = pd.DataFrame(records)
    # Categories with no spend in the complete-month window are not part of the
    # current baseline; keeping them would add empty rows to the dashboard.
    summary = summary[summary["avg_monthly_spend"] > 0]
    if summary.empty:
        logger.info("Category summary: 0 categories")
        return _empty_category_summary()
    total_spend = float(summary["avg_monthly_spend"].sum())
    summary["share_of_spend"] = (
        summary["avg_monthly_spend"] / total_spend if total_spend > 0 else 0.0
    )
    coverage = summary["recurring_run_rate"] / summary["avg_monthly_spend"].replace(
        0, np.nan
    )
    summary["recurring_coverage"] = coverage.clip(lower=0, upper=1).fillna(0.0)
    summary = summary[CATEGORY_SUMMARY_COLUMNS].sort_values(
        ["avg_monthly_spend", "recurring_run_rate"], ascending=False
    ).reset_index(drop=True)

    logger.info(
        f"Category summary: {len(summary)} categories | "
        f"{int(summary['is_steady'].sum())} steady | "
        f"recurring {summary['recurring_run_rate'].sum():.0f} PLN/mo | "
        f"spend {total_spend:.0f} PLN/mo"
    )
    return summary
