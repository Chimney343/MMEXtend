"""
Budget health: a Conscious Spending Plan view of MMEX data.

Measures the parts of Ramit Sethi's Conscious Spending Plan (*I Will Teach You
to Be Rich*, 2nd ed., 2019) that the data can answer: take-home pay, the four
buckets as a share of take-home, emergency-fund coverage, debt and lifestyle
creep. The rest of the pipeline drops transfers, so savings and investment
flows are invisible there. This module rebuilds account balances from MMEX
initial balances plus every transaction leg and measures those flows directly.

The bucket targets come from the book. They are reference ranges, not norms
derived from this data. Category→bucket and account→group mappings come from
``configs/pipeline.yaml`` and stay drafts until the user confirms them (HR-6).

Usage:
    from src.analysis.budget_health import (
        build_account_balances,
        build_annual_creep,
        build_budget_health_monthly,
        build_reserve_trend,
        next_100_steps,
        summarise_budget_health,
    )
    balances = build_account_balances(df_featured, accounts, config=cfg)
    monthly = build_budget_health_monthly(df_featured, accounts, config=cfg)
    summary = summarise_budget_health(monthly, balances, config=cfg)
    steps = next_100_steps(summary, config=cfg)
    trend = build_reserve_trend(monthly, balances, config=cfg)
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

SPENDING_BUCKETS: tuple[str, ...] = ("fixed", "guilt_free", "one_off", "unmapped")
FLOW_BUCKETS: tuple[str, ...] = ("savings", "investments")
ALLOCATION_COLUMNS: tuple[str, ...] = SPENDING_BUCKETS + FLOW_BUCKETS + ("unallocated",)
ACCOUNT_GROUPS: tuple[str, ...] = (
    "cash",
    "savings",
    "investments",
    "receivables",
    "debt",
    "other",
)
LIQUID_GROUPS: tuple[str, ...] = ("cash", "savings")

# MMEX ACCOUNTTYPE → default group. Accounts used against their MMEX type
# (e.g. a "Credit Card" account that tracks what a person owes you) are
# overridden per name in budget_health.account_groups.
_GROUP_BY_ACCOUNT_TYPE: dict[str, str] = {
    "checking": "cash",
    "cash": "cash",
    "term": "other",
    "credit card": "debt",
    "loan": "debt",
    "investment": "investments",
    "shares": "investments",
    "asset": "other",
}

DEFAULT_CONFIG: dict[str, Any] = {
    "mapping_confirmed": False,
    "base_currency": "PLN",
    "income_categories": ["Income"],
    "take_home_deductions": ["Taxes"],
    "buckets": {},
    "account_groups": {},
    "targets": {
        "fixed": [0.50, 0.60],
        "investments": [0.10, 0.10],
        "savings": [0.05, 0.10],
        "guilt_free": [0.20, 0.35],
    },
    "emergency_fund_months": [3, 6],
    "employer_match": False,
    "plan": {
        "salary_pln": None,
        "salary_day": None,
        "investment_pln": None,
    },
}

MONTHLY_BASE_COLUMNS = [
    "year_month",
    "is_partial_month",
    "gross_income",
    "deductions",
    "take_home",
    *ALLOCATION_COLUMNS,
]

BALANCE_COLUMNS = [
    "year_month",
    "account",
    "account_type",
    "group",
    "currency",
    "balance",
    "fx_rate",
    "balance_pln",
]

RESERVE_TREND_COLUMNS = ["year_month", "savings_balance", "ceiling", "above_ceiling"]
RESERVE_TRANSFER_COLUMNS = [
    "year_month",
    "is_partial_month",
    "moved_to_cash",
    "n_transfers",
    "transfer_days",
    "income_into_reserve",
    "income_days",
]


def _resolve_config(config: dict[str, Any] | None) -> dict[str, Any]:
    """Merge caller overrides over the code defaults, ignoring unknown keys."""
    resolved = dict(DEFAULT_CONFIG)
    config = config or {}
    resolved.update({key: value for key, value in config.items() if key in DEFAULT_CONFIG})
    resolved["targets"] = {**DEFAULT_CONFIG["targets"], **(config.get("targets") or {})}
    resolved["plan"] = {**DEFAULT_CONFIG["plan"], **(config.get("plan") or {})}
    resolved["buckets"] = resolved.get("buckets") or {}
    resolved["account_groups"] = resolved.get("account_groups") or {}
    return resolved


def _as_of_timestamp(as_of: Any) -> pd.Timestamp:
    """Coerce as_of to a naive, date-normalised Timestamp (default: today)."""
    if as_of is None:
        return pd.Timestamp.today().normalize()
    ts = pd.Timestamp(as_of)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def _amount_column(df: pd.DataFrame) -> str:
    return "amount_pln" if "amount_pln" in df.columns else "amount"


# ── Mappings ─────────────────────────────────────────────────────────────────


def assign_buckets(df: pd.DataFrame, buckets: dict[str, str]) -> pd.Series:
    """
    Map each row to a Conscious Spending Plan bucket.

    A ``"Category|Subcategory"`` key wins over a plain ``"Category"`` key.
    Rows matching neither, including uncategorised rows, get ``"unmapped"``.

    Parameters
    ----------
    df : pd.DataFrame
        Must have ``category``; ``subcategory`` is optional.
    buckets : dict[str, str]
        Mapping key → bucket name (one of SPENDING_BUCKETS).

    Returns
    -------
    pd.Series
        Bucket name per row, aligned to ``df.index``.
    """
    unknown = sorted({b for b in buckets.values() if b not in SPENDING_BUCKETS})
    if unknown:
        raise ValueError(
            f"Unknown bucket name(s) in budget_health.buckets: {unknown}. "
            f"Allowed: {list(SPENDING_BUCKETS)}"
        )
    category = df["category"].astype("string")
    if "subcategory" in df.columns:
        sub_key = category + "|" + df["subcategory"].astype("string")
    else:
        sub_key = pd.Series(pd.NA, index=df.index, dtype="string")
    by_sub = sub_key.map(buckets)
    by_cat = category.map(buckets)
    return by_sub.fillna(by_cat).fillna("unmapped").astype(str)


def assign_account_groups(accounts: pd.DataFrame, account_groups: dict[str, list[str]]) -> pd.Series:
    """
    Map each account to a balance group.

    Explicit names in ``account_groups`` win. Otherwise the MMEX account type
    decides (see ``_GROUP_BY_ACCOUNT_TYPE``); unknown types fall back to
    ``"other"``. Configured names that match no account are logged.

    Parameters
    ----------
    accounts : pd.DataFrame
        Output of ``get_account_metadata()``: ``account`` and ``account_type``.
    account_groups : dict[str, list[str]]
        Group name → list of account names.

    Returns
    -------
    pd.Series
        Group name per account, aligned to ``accounts.index``.
    """
    unknown = sorted(g for g in account_groups if g not in ACCOUNT_GROUPS)
    if unknown:
        raise ValueError(
            f"Unknown account group(s) in budget_health.account_groups: {unknown}. "
            f"Allowed: {list(ACCOUNT_GROUPS)}"
        )
    override = {
        name: group for group, names in account_groups.items() for name in (names or [])
    }
    missing = sorted(set(override) - set(accounts["account"]))
    if missing:
        logger.warning(f"budget_health.account_groups names not found in MMEX: {missing}")

    by_type = (
        accounts["account_type"].astype(str).str.strip().str.lower().map(_GROUP_BY_ACCOUNT_TYPE)
    )
    return accounts["account"].map(override).fillna(by_type).fillna("other")


# ── FX valuation ─────────────────────────────────────────────────────────────


def implied_fx_rates(df: pd.DataFrame, base_currency: str = "PLN") -> pd.DataFrame:
    """
    Derive per-date FX rates from the converted transaction amounts.

    Using ``amount_pln / amount`` keeps balance valuation consistent with the
    rates the ingestion step already applied to every row.

    Returns
    -------
    pd.DataFrame
        Columns: currency, date, rate (base units per 1 unit of currency).
    """
    if "amount_pln" not in df.columns or "currency" not in df.columns:
        return pd.DataFrame(columns=["currency", "date", "rate"])
    rows = df[
        (df["currency"] != base_currency)
        & df["amount"].notna()
        & df["amount_pln"].notna()
        & (df["amount"] != 0)
    ]
    rates = (
        (rows["amount_pln"] / rows["amount"])
        .groupby([rows["currency"], rows["date"].dt.normalize()])
        .median()
        .rename("rate")
        .reset_index()
    )
    return rates.sort_values(["currency", "date"]).reset_index(drop=True)


def _rate_lookup(
    currencies: pd.Series,
    dates: pd.Series,
    rates: pd.DataFrame,
    base_currency: str,
) -> pd.Series:
    """
    Return the latest rate on or before each date, per currency.

    Dates before a currency's first known rate take that first rate, logged
    as an IMPUTATION (HR-4). Currencies with no rate at all stay NaN.
    """
    out = pd.Series(np.nan, index=currencies.index, dtype=float)
    out[currencies == base_currency] = 1.0
    for ccy in currencies[currencies != base_currency].dropna().unique():
        mask = currencies == ccy
        series = rates.loc[rates["currency"] == ccy].set_index("date")["rate"]
        if series.empty:
            logger.warning(
                f"No FX rate for {ccy}: {int(mask.sum())} balance rows left unvalued (NaN)."
            )
            continue
        series = series[~series.index.duplicated(keep="last")].sort_index()
        looked_up = series.reindex(pd.DatetimeIndex(dates[mask]), method="ffill").to_numpy(
            dtype=float, copy=True
        )
        before_first = np.isnan(looked_up)
        if before_first.any():
            looked_up[before_first] = series.iloc[0]
            logger.warning(
                f"IMPUTATION | column=fx_rate | method=earliest_known_rate | "
                f"rows_affected={int(before_first.sum())} | detail={ccy} dates before "
                f"{series.index[0].date()} valued at the first known rate"
            )
        out[mask] = looked_up
    return out


# ── Account balances ─────────────────────────────────────────────────────────


def _transaction_legs(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per account movement: the source row, plus the destination of transfers.

    ``amount`` is already signed for the source account; ``to_amount`` is the
    positive amount received by ``to_account`` in that account's currency.
    """
    source = df[["date", "account", "amount"]].rename(columns={"amount": "delta"})
    transfers = df[df["is_transfer"] & df["to_account"].notna()]
    dest = transfers[["date", "to_account", "to_amount"]].rename(
        columns={"to_account": "account", "to_amount": "delta"}
    )
    legs = pd.concat([source, dest], ignore_index=True)
    legs = legs[legs["account"].notna()]
    legs["delta"] = pd.to_numeric(legs["delta"], errors="coerce").fillna(0.0)
    return legs


def _account_frame(
    df: pd.DataFrame, legs: pd.DataFrame, accounts: pd.DataFrame, cfg: dict[str, Any]
) -> pd.DataFrame:
    """Account metadata with groups, extended by any account seen only in transactions."""
    meta = accounts.copy()
    unknown = sorted(set(legs["account"]) - set(meta["account"]))
    if unknown:
        if "currency" in df.columns:
            row_ccy = df[df["account"].isin(unknown)].groupby("account")["currency"].agg(
                lambda s: s.mode().iloc[0] if not s.mode().empty else cfg["base_currency"]
            )
        else:
            row_ccy = pd.Series(dtype=object)
        logger.warning(
            f"{len(unknown)} account(s) in transactions but not in MMEX account list; "
            f"zero initial balance, group 'other', currency from their rows: {unknown}"
        )
        meta = pd.concat(
            [
                meta,
                pd.DataFrame(
                    {
                        "account": unknown,
                        "account_type": "unknown",
                        "currency": [row_ccy.get(a, cfg["base_currency"]) for a in unknown],
                        "initial_balance": 0.0,
                        "initial_date": pd.NaT,
                    }
                ),
            ],
            ignore_index=True,
        )
    meta["group"] = assign_account_groups(meta, cfg["account_groups"])
    meta["initial_balance"] = pd.to_numeric(meta["initial_balance"], errors="coerce").fillna(0.0)
    meta["initial_date"] = pd.to_datetime(meta.get("initial_date"), errors="coerce")
    return meta.set_index("account")


def build_account_balances(
    df: pd.DataFrame,
    accounts: pd.DataFrame,
    as_of: Any = None,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Reconstruct month-end balances for every account.

    Balance = MMEX initial balance + cumulative sum of all transaction legs.
    Foreign-currency balances are valued at the latest implied rate on or
    before each month end. An account appears from the month of its initial
    date or first transaction, whichever is earlier.

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features(). Needs date, account, to_account,
        amount, to_amount, is_transfer; amount_pln and currency for FX.
    accounts : pd.DataFrame
        Output of get_account_metadata(): account, account_type, currency,
        initial_balance, initial_date.
    as_of : date-like, optional
        Rows after this date are ignored (scheduled entries). Default: today.
    config : dict, optional
        The ``budget_health`` config section.

    Returns
    -------
    pd.DataFrame
        Columns: BALANCE_COLUMNS, one row per account per month.
    """
    cfg = _resolve_config(config)
    as_of_ts = _as_of_timestamp(as_of)
    if df.empty or accounts is None or accounts.empty:
        return pd.DataFrame(columns=BALANCE_COLUMNS)

    actual = df[df["date"] <= as_of_ts]
    n_future = len(df) - len(actual)
    if n_future:
        logger.info(f"Balances: {n_future} rows dated after {as_of_ts.date()} ignored")

    legs = _transaction_legs(actual)
    meta = _account_frame(actual, legs, accounts, cfg)

    legs["month"] = legs["date"].dt.to_period("M")
    months = pd.period_range(legs["month"].min(), as_of_ts.to_period("M"), freq="M")
    flows = (
        legs.pivot_table(index="month", columns="account", values="delta", aggfunc="sum")
        .reindex(index=months, columns=meta.index)
        .fillna(0.0)
    )
    balance = flows.cumsum() + meta["initial_balance"]

    first_leg = legs.groupby("account")["date"].min().reindex(meta.index)
    opened = pd.concat([first_leg, meta["initial_date"]], axis=1).min(axis=1)
    open_month_start = opened.dt.to_period("M").dt.start_time

    long = balance.stack().rename("balance").reset_index()
    long.columns = ["month", "account", "balance"]
    open_start = long["account"].map(open_month_start)
    long = long[open_start.isna() | (long["month"].dt.start_time >= open_start)].copy()

    long["account_type"] = long["account"].map(meta["account_type"])
    long["group"] = long["account"].map(meta["group"])
    long["currency"] = long["account"].map(meta["currency"])
    month_end = long["month"].dt.end_time.dt.normalize()
    rates = implied_fx_rates(actual, cfg["base_currency"])
    long["fx_rate"] = _rate_lookup(long["currency"], month_end, rates, cfg["base_currency"])
    long["balance_pln"] = long["balance"] * long["fx_rate"]
    long["year_month"] = long["month"].dt.strftime("%Y-%m")

    result = long[BALANCE_COLUMNS].sort_values(["year_month", "account"]).reset_index(drop=True)
    latest = result[result["year_month"] == result["year_month"].max()]
    logger.info(
        f"Account balances: {result['account'].nunique()} accounts | "
        f"{result['year_month'].nunique()} months | latest total "
        f"{latest['balance_pln'].sum():,.0f} {cfg['base_currency']}"
    )
    return result


def _group_flows_by_month(
    df: pd.DataFrame, accounts: pd.DataFrame, cfg: dict[str, Any]
) -> pd.DataFrame:
    """Net movement per balance group per month, valued at transaction-date rates."""
    legs = _transaction_legs(df)
    meta = _account_frame(df, legs, accounts, cfg)
    legs["currency"] = legs["account"].map(meta["currency"])
    legs["group"] = legs["account"].map(meta["group"])
    rates = implied_fx_rates(df, cfg["base_currency"])
    legs["rate"] = _rate_lookup(
        legs["currency"], legs["date"].dt.normalize(), rates, cfg["base_currency"]
    )
    legs["delta_pln"] = legs["delta"] * legs["rate"]
    legs["year_month"] = legs["date"].dt.strftime("%Y-%m")
    return legs.pivot_table(
        index="year_month", columns="group", values="delta_pln", aggfunc="sum"
    ).reindex(columns=list(ACCOUNT_GROUPS), fill_value=0.0)


# ── Monthly Conscious Spending Plan ──────────────────────────────────────────


def build_budget_health_monthly(
    df: pd.DataFrame,
    accounts: pd.DataFrame | None = None,
    as_of: Any = None,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Split each month's take-home pay into Conscious Spending Plan buckets.

    - ``gross_income``: net sum of rows in ``income_categories``.
    - ``deductions``: amounts paid in ``take_home_deductions`` categories
      (VAT, PIT, social insurance). ``take_home = gross_income - deductions``.
    - ``fixed`` / ``guilt_free`` / ``one_off`` / ``unmapped``: net spend of
      all other non-transfer rows by bucket. Refunds reduce their bucket.
    - ``savings`` / ``investments``: net inflow into accounts in those
      balance groups. Needs ``accounts``; NaN without it.
    - ``unallocated``: take-home left after all of the above, i.e. what
      stayed in cash accounts, IOU ledgers and other balances.

    ``*_ttm`` columns are trailing 12-month sums over complete months; they
    stay NaN until 12 complete months exist. ``*_share_ttm`` divide by
    ``take_home_ttm`` (NaN when take-home is not positive).

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features().
    accounts : pd.DataFrame | None
        Output of get_account_metadata(). None for CSV input.
    as_of : date-like, optional
        Rows after this date are scheduled entries and ignored. The month
        containing ``as_of`` is flagged partial unless ``as_of`` is its last day.
    config : dict, optional
        The ``budget_health`` config section.

    Returns
    -------
    pd.DataFrame
        One row per calendar month, continuous from the first transaction.
    """
    cfg = _resolve_config(config)
    as_of_ts = _as_of_timestamp(as_of)
    actual = df[df["date"] <= as_of_ts].copy()
    if actual.empty:
        return pd.DataFrame(columns=MONTHLY_BASE_COLUMNS)

    amount_col = _amount_column(actual)
    flows = actual[~actual["is_transfer"]].copy()
    flows["_amount"] = pd.to_numeric(flows[amount_col], errors="coerce").fillna(0.0)
    is_income = flows["category"].isin(cfg["income_categories"])
    is_deduction = flows["category"].isin(cfg["take_home_deductions"])
    spending = flows[~is_income & ~is_deduction].copy()
    spending["_bucket"] = assign_buckets(spending, cfg["buckets"])

    months = pd.period_range(
        actual["date"].min().to_period("M"), as_of_ts.to_period("M"), freq="M"
    ).strftime("%Y-%m")
    monthly = pd.DataFrame(index=pd.Index(months, name="year_month"))
    monthly["gross_income"] = flows[is_income].groupby("year_month")["_amount"].sum()
    monthly["deductions"] = -flows[is_deduction].groupby("year_month")["_amount"].sum()
    by_bucket = -spending.pivot_table(
        index="year_month", columns="_bucket", values="_amount", aggfunc="sum"
    )
    for bucket in SPENDING_BUCKETS:
        monthly[bucket] = by_bucket[bucket] if bucket in by_bucket.columns else 0.0
    monthly = monthly.fillna(0.0)
    monthly["take_home"] = monthly["gross_income"] - monthly["deductions"]

    if accounts is not None and not accounts.empty:
        group_flows = _group_flows_by_month(actual, accounts, cfg).reindex(months).fillna(0.0)
        for bucket in FLOW_BUCKETS:
            monthly[bucket] = group_flows[bucket]
        flow_total = monthly[list(FLOW_BUCKETS)].sum(axis=1)
    else:
        logger.warning(
            "Budget health: no account metadata (CSV input?). savings and investments "
            "are NaN; unallocated = take-home minus spending."
        )
        for bucket in FLOW_BUCKETS:
            monthly[bucket] = np.nan
        flow_total = 0.0
    monthly["unallocated"] = (
        monthly["take_home"] - monthly[list(SPENDING_BUCKETS)].sum(axis=1) - flow_total
    )

    last_month_end = as_of_ts.to_period("M").end_time.normalize()
    monthly["is_partial_month"] = False
    monthly.loc[as_of_ts.strftime("%Y-%m"), "is_partial_month"] = as_of_ts < last_month_end
    monthly = monthly.reset_index()[MONTHLY_BASE_COLUMNS]

    monthly = _add_ttm(monthly)
    unmapped_ttm = monthly["unmapped_ttm"].dropna()
    if not unmapped_ttm.empty and unmapped_ttm.iloc[-1] != 0:
        logger.warning(
            f"Budget health: {unmapped_ttm.iloc[-1]:,.0f} {cfg['base_currency']} of TTM "
            "spend has no bucket. Add its categories to budget_health.buckets."
        )
    logger.info(
        f"Budget health monthly: {len(monthly)} months | "
        f"{int(monthly['take_home_ttm'].notna().sum())} with a full TTM window"
    )
    return monthly


def _add_ttm(monthly: pd.DataFrame) -> pd.DataFrame:
    """Append trailing-12-month sums and shares of take-home (complete months only)."""
    complete = monthly[~monthly["is_partial_month"]]
    value_cols = ["gross_income", "deductions", "take_home", *ALLOCATION_COLUMNS]
    ttm = complete[value_cols].rolling(12, min_periods=12).sum().add_suffix("_ttm")
    monthly = monthly.join(ttm)
    denom = monthly["take_home_ttm"].where(monthly["take_home_ttm"] > 0)
    for col in ALLOCATION_COLUMNS:
        monthly[f"{col}_share_ttm"] = monthly[f"{col}_ttm"] / denom
    return monthly


# ── Lifestyle creep ──────────────────────────────────────────────────────────


def build_annual_creep(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Compare year-on-year growth of take-home pay and living spend.

    Living spend = fixed + guilt-free (one-offs excluded). The marginal
    spend rate is the share of each extra unit of take-home that went to
    living spend: ``delta_living_spend / delta_take_home``. It is NaN when
    ``delta_take_home <= 0``: the ratio is defined only for extra take-home,
    so a year with flat or falling pay has no meaningful rate. Only calendar
    years with 12 complete months are included.

    Returns
    -------
    pd.DataFrame
        Columns: year, n_months, take_home, living_spend, living_share,
        delta_take_home, delta_living_spend, marginal_spend_rate.
    """
    columns = [
        "year",
        "n_months",
        "take_home",
        "living_spend",
        "living_share",
        "delta_take_home",
        "delta_living_spend",
        "marginal_spend_rate",
    ]
    if monthly is None or monthly.empty:
        return pd.DataFrame(columns=columns)
    complete = monthly[~monthly["is_partial_month"]].copy()
    complete["year"] = complete["year_month"].str[:4].astype(int)
    complete["living_spend"] = complete["fixed"] + complete["guilt_free"]
    annual = complete.groupby("year").agg(
        n_months=("year_month", "size"),
        take_home=("take_home", "sum"),
        living_spend=("living_spend", "sum"),
    )
    annual = annual[annual["n_months"] == 12].reset_index()
    annual["living_share"] = annual["living_spend"] / annual["take_home"].where(
        annual["take_home"] > 0
    )
    annual["delta_take_home"] = annual["take_home"].diff()
    annual["delta_living_spend"] = annual["living_spend"].diff()
    # The ratio is defined only for extra take-home: with flat or falling pay
    # its sign flips meaning, so report NaN instead of a negative rate.
    positive_delta = annual["delta_take_home"].where(annual["delta_take_home"] > 0)
    annual["marginal_spend_rate"] = annual["delta_living_spend"] / positive_delta
    # diff() spans a gap when a year in between was incomplete
    gap = annual["year"].diff() != 1
    annual.loc[gap, ["delta_take_home", "delta_living_spend", "marginal_spend_rate"]] = np.nan
    return annual[columns]


# ── Scorecard ────────────────────────────────────────────────────────────────


def _check(
    name: str, value: float | None, target: str, status: str, detail: str, unit: str = "share"
) -> dict:
    """One scorecard row. ``unit`` is share, months or currency (for display)."""
    return {
        "check": name,
        "value": value,
        "unit": unit,
        "target": target,
        "status": status,
        "detail": detail,
    }


def _share_check(name: str, share: float, target: list[float], higher_is_better: bool) -> dict:
    low, high = target
    target_txt = f"{low:.0%}" if low == high else f"{low:.0%}–{high:.0%}"
    if pd.isna(share):
        return _check(name, None, target_txt, "insufficient data", "needs 12 complete months")
    if higher_is_better:
        ok = share >= low
        detail = f"{share:.1%} of take-home" + ("" if ok else f", below {low:.0%}")
    else:
        ok = share <= high
        detail = f"{share:.1%} of take-home" + ("" if ok else f", above {high:.0%}")
    return _check(name, float(share), target_txt, "ok" if ok else "attention", detail)


def summarise_budget_health(
    monthly: pd.DataFrame,
    balances: pd.DataFrame | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Score the latest full TTM window against the Conscious Spending Plan.

    Returns
    -------
    dict
        ``as_of_month`` (str | None), ``take_home_monthly`` (float), ``shares``
        (bucket → TTM share), ``group_balances`` (group → base currency at the
        latest month end), ``liquid_fx_share`` (share of cash + savings held
        outside the base currency), ``mapping_confirmed`` (bool) and
        ``checks``: a list of dicts with check, value, unit, target, status
        (ok / attention / info / insufficient data) and detail.
    """
    cfg = _resolve_config(config)
    targets = cfg["targets"]
    ccy = cfg["base_currency"]
    summary: dict[str, Any] = {
        "as_of_month": None,
        "take_home_monthly": np.nan,
        "shares": {},
        "group_balances": {},
        "liquid_fx_share": np.nan,
        "mapping_confirmed": bool(cfg["mapping_confirmed"]),
        "checks": [],
    }
    full = monthly.dropna(subset=["take_home_ttm"]) if monthly is not None else pd.DataFrame()
    last = full.iloc[-1] if not full.empty else None
    if last is not None:
        summary["as_of_month"] = last["year_month"]
        summary["take_home_monthly"] = float(last["take_home_ttm"]) / 12
        summary["shares"] = {c: float(last[f"{c}_share_ttm"]) for c in ALLOCATION_COLUMNS}
    shares = summary["shares"]
    checks: list[dict] = summary["checks"]

    for name, key, higher_is_better in (
        ("Fixed costs", "fixed", False),
        ("Investments", "investments", True),
        ("Savings goals", "savings", True),
        ("Guilt-free spending", "guilt_free", False),
    ):
        checks.append(
            _share_check(name, shares.get(key, np.nan), targets[key], higher_is_better)
        )

    unallocated = shares.get("unallocated", np.nan)
    if pd.notna(unallocated):
        checks.append(_check(
            "Unallocated surplus", float(unallocated), "—", "info",
            "take-home not spent, saved or invested: it stays in cash and IOU accounts",
        ))
    else:
        checks.append(_check("Unallocated surplus", None, "—", "insufficient data",
                             "needs 12 complete months"))

    ef_low, ef_high = cfg["emergency_fund_months"]
    ef_target = f"{ef_low}–{ef_high} months"
    if balances is None or balances.empty or last is None:
        checks.append(_check("Emergency fund", None, ef_target, "insufficient data",
                             "needs account balances and 12 complete months", unit="months"))
        checks.append(_check("Debt balance", None, "0", "insufficient data",
                             "needs account balances", unit="currency"))
        return summary

    latest = balances[balances["year_month"] == balances["year_month"].max()]
    groups = latest.groupby("group")["balance_pln"].sum().reindex(ACCOUNT_GROUPS, fill_value=0.0)
    # Reconstructed balances carry float residue (e.g. -1e-10 on an emptied account)
    groups = groups.where(groups.abs() >= 0.5, 0.0)
    summary["group_balances"] = {g: float(v) for g, v in groups.items()}

    liquid = latest[latest["group"].isin(LIQUID_GROUPS)]
    liquid_total = liquid["balance_pln"].sum()
    if liquid_total > 0:
        foreign = liquid.loc[liquid["currency"] != ccy, "balance_pln"].sum()
        summary["liquid_fx_share"] = float(foreign / liquid_total)

    fixed_monthly = float(last["fixed_ttm"]) / 12
    living_monthly = float(last["fixed_ttm"] + last["guilt_free_ttm"]) / 12
    if fixed_monthly > 0:
        ef_months = groups["savings"] / fixed_monthly
        checks.append(_check(
            "Emergency fund", float(ef_months), ef_target,
            "ok" if ef_months >= ef_low else "attention",
            f"{groups['savings']:,.0f} {ccy} in savings accounts = {ef_months:.1f} months "
            "of fixed costs",
            unit="months",
        ))
        ceiling = ef_high * fixed_monthly
        above = max(groups["savings"] - ceiling, 0.0)
        checks.append(_check(
            "Reserve above emergency-fund ceiling", float(above), "—", "info",
            f"savings balance beyond {ef_high} months of fixed costs "
            f"({ceiling:,.0f} {ccy})",
            unit="currency",
        ))
    else:
        checks.append(_check("Emergency fund", None, ef_target, "insufficient data",
                             "no fixed costs in the TTM window", unit="months"))
    if living_monthly > 0:
        liquid_months = (groups["cash"] + groups["savings"]) / living_monthly
        checks.append(_check(
            "Liquid runway", float(liquid_months), "—", "info",
            f"cash + savings = {liquid_months:.1f} months of fixed + guilt-free spend",
            unit="months",
        ))

    debt = groups["debt"]
    checks.append(_check(
        "Debt balance", float(debt), "0", "ok" if debt >= 0 else "attention",
        "no balance owed on debt accounts" if debt >= 0
        else f"{-debt:,.0f} {ccy} owed on debt accounts",
        unit="currency",
    ))
    if pd.notna(summary["liquid_fx_share"]):
        checks.append(_check(
            "Liquid cash in foreign currency", summary["liquid_fx_share"], "—", "info",
            f"{summary['liquid_fx_share']:.0%} of cash + savings is not in {ccy}",
        ))
    return summary


# ── Next 100 PLN priority order ──────────────────────────────────────────────

_STATUS_RANK: dict[str, int] = {
    "ok": 0,
    "info": 1,
    "insufficient data": 2,
    "attention": 3,
}


def next_100_steps(
    summary: dict[str, Any], config: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """
    Order the scorecard into Sethi's "next 100" priority list.

    The five steps follow ``spending-plan-automation.md`` § "Next $100
    Allocation System", rendered without product names (HR-5): high-interest
    debt, employer match, emergency fund, investing, then savings goals and
    guilt-free money.

    Parameters
    ----------
    summary : dict[str, Any]
        Output of :func:`summarise_budget_health`.
    config : dict[str, Any] | None
        The ``budget_health`` config section. ``employer_match`` decides
        whether step 2 applies.

    Returns
    -------
    list[dict[str, Any]]
        Five dicts with keys ``step``, ``status``, ``detail`` and ``is_next``.
        ``is_next`` is True on the first step whose status is ``attention`` or
        ``insufficient data``; at most one step is next.
    """
    cfg = _resolve_config(config)
    checks = {check.get("check"): check for check in (summary or {}).get("checks", [])}

    def from_check(step: str, check_name: str, fallback: str) -> dict[str, Any]:
        check = checks.get(check_name) or {}
        return {
            "step": step,
            "status": check.get("status", "insufficient data"),
            "detail": check.get("detail", fallback),
            "is_next": False,
        }

    steps = [from_check("High-interest debt", "Debt balance", "needs account balances")]

    if cfg["employer_match"]:
        steps.append({
            "step": "Employer match",
            "status": "not measured",
            "detail": "not visible in MMEX",
            "is_next": False,
        })
    else:
        steps.append({
            "step": "Employer match",
            "status": "not applicable",
            "detail": "no employer plan (employer_match: false)",
            "is_next": False,
        })

    steps.append(
        from_check(
            "Emergency fund 3–6 months",
            "Emergency fund",
            "needs 12 complete months and account balances",
        )
    )
    steps.append(
        from_check("Invest ≥10% of take-home", "Investments", "needs 12 complete months")
    )

    savings_check = checks.get("Savings goals") or {}
    guilt_check = checks.get("Guilt-free spending") or {}
    savings_status = savings_check.get("status", "insufficient data")
    guilt_status = guilt_check.get("status", "insufficient data")
    if savings_status == "ok" and guilt_status == "ok":
        fifth_status = "ok"
        fifth_detail = "savings goals and guilt-free spending both within target"
    else:
        worse = (
            savings_check
            if _STATUS_RANK.get(savings_status, 2) >= _STATUS_RANK.get(guilt_status, 2)
            else guilt_check
        )
        fifth_status = worse.get("status", "insufficient data")
        fifth_detail = worse.get("detail", "needs 12 complete months")
    steps.append({
        "step": "Savings goals, then guilt-free",
        "status": fifth_status,
        "detail": fifth_detail,
        "is_next": False,
    })

    for step in steps:
        if step["status"] in ("attention", "insufficient data"):
            step["is_next"] = True
            break
    return steps


# ── Reserve vs emergency-fund ceiling ────────────────────────────────────────


def build_reserve_trend(
    monthly: pd.DataFrame,
    balances: pd.DataFrame | None,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Compare the savings-group reserve with the emergency-fund ceiling by month.

    The ceiling is ``emergency_fund_months[1] × fixed_ttm / 12``: six months of
    fixed costs, by default. It is NaN until 12 complete months exist. The
    savings balance is the sum of ``balance_pln`` for accounts in the
    ``savings`` group.

    Parameters
    ----------
    monthly : pd.DataFrame
        Output of :func:`build_budget_health_monthly` (needs ``year_month``
        and ``fixed_ttm``).
    balances : pd.DataFrame | None
        Output of :func:`build_account_balances`. ``None`` or empty returns an
        empty frame.
    config : dict[str, Any] | None
        The ``budget_health`` config section.

    Returns
    -------
    pd.DataFrame
        Columns: year_month, savings_balance, ceiling, above_ceiling. Inner-
        joins the two inputs on ``year_month``.
    """
    cfg = _resolve_config(config)
    empty = pd.DataFrame(columns=RESERVE_TREND_COLUMNS)
    if balances is None or balances.empty:
        return empty
    if not {"year_month", "group", "balance_pln"}.issubset(balances.columns):
        return empty
    if monthly is None or monthly.empty:
        return empty
    if not {"year_month", "fixed_ttm"}.issubset(monthly.columns):
        return empty

    savings = balances.loc[balances["group"] == "savings"]
    if savings.empty:
        return empty
    savings_balance = (
        savings.groupby("year_month")["balance_pln"].sum().rename("savings_balance").reset_index()
    )

    ef_high = cfg["emergency_fund_months"][1]
    trend = savings_balance.merge(
        monthly[["year_month", "fixed_ttm"]], on="year_month", how="inner"
    )
    trend["ceiling"] = ef_high * trend["fixed_ttm"] / 12.0
    trend["above_ceiling"] = (trend["savings_balance"] - trend["ceiling"]).clip(lower=0.0)
    trend.loc[trend["ceiling"].isna(), "above_ceiling"] = np.nan
    return trend.sort_values("year_month").reset_index(drop=True)[RESERVE_TREND_COLUMNS]


# ── Reserve transfers (Sethi's buffer system) ────────────────────────────────


def _days_label(days: pd.Series) -> str:
    """Sorted unique day-of-month numbers as ``"5, 20"`` (empty string when none)."""
    values = sorted({int(day) for day in days.dropna()})
    return ", ".join(str(day) for day in values)


def build_reserve_transfers(
    df: pd.DataFrame,
    accounts: pd.DataFrame | None,
    as_of: Any = None,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Measure the monthly flow from the reserve to spending accounts.

    A **reserve transfer** is a transfer row whose source account is in the
    ``savings`` group and whose destination is in the ``cash`` group. Its
    amount is the positive magnitude of the source leg (``−amount_pln``).
    Income arriving in reserve accounts is reported separately: invoices
    land there before the monthly salary transfer.

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features(). Needs date, account, to_account,
        category, amount/amount_pln and is_transfer.
    accounts : pd.DataFrame | None
        Output of get_account_metadata(). ``None`` for CSV input: no groups
        can be resolved, so an empty frame is returned with a warning.
    as_of : date-like, optional
        Rows after this date are excluded. Default: today.
    config : dict, optional
        The ``budget_health`` config section.

    Returns
    -------
    pd.DataFrame
        Columns: RESERVE_TRANSFER_COLUMNS, one row per calendar month,
        continuous from the first transaction to the as-of month.
    """
    cfg = _resolve_config(config)
    empty = pd.DataFrame(columns=RESERVE_TRANSFER_COLUMNS)
    if accounts is None or accounts.empty:
        logger.warning(
            "Reserve transfers: no account metadata (CSV input?). "
            "Cannot detect reserve → cash transfers; returning an empty frame."
        )
        return empty

    as_of_ts = _as_of_timestamp(as_of)
    actual = df[df["date"] <= as_of_ts].copy()
    if actual.empty:
        return empty

    legs = _transaction_legs(actual)
    meta = _account_frame(actual, legs, accounts, cfg)
    groups = meta["group"]

    amount_col = _amount_column(actual)
    actual["_amount_pln"] = pd.to_numeric(actual[amount_col], errors="coerce").fillna(0.0)
    actual["_day"] = actual["date"].dt.day

    is_transfer = actual["is_transfer"].fillna(False).astype(bool)
    source_group = actual["account"].map(groups)
    dest_group = actual["to_account"].map(groups)
    reserve_out = is_transfer & (source_group == "savings") & (dest_group == "cash")
    income_in = actual["category"].isin(cfg["income_categories"]) & (source_group == "savings")

    months = pd.period_range(
        actual["date"].min().to_period("M"), as_of_ts.to_period("M"), freq="M"
    ).strftime("%Y-%m")
    index = pd.Index(months, name="year_month")

    out = pd.DataFrame(index=index)
    out["moved_to_cash"] = (
        -actual.loc[reserve_out].groupby("year_month")["_amount_pln"].sum()
    ).reindex(index, fill_value=0.0)
    out["n_transfers"] = (
        actual.loc[reserve_out].groupby("year_month").size().reindex(index, fill_value=0)
    ).astype(int)
    out["transfer_days"] = (
        actual.loc[reserve_out].groupby("year_month")["_day"].apply(_days_label)
    ).reindex(index, fill_value="")
    out["income_into_reserve"] = (
        actual.loc[income_in].groupby("year_month")["_amount_pln"].sum()
    ).reindex(index, fill_value=0.0)
    out["income_days"] = (
        actual.loc[income_in].groupby("year_month")["_day"].apply(_days_label)
    ).reindex(index, fill_value="")

    period_index = pd.PeriodIndex(months, freq="M")
    month_start = period_index.to_timestamp(how="start")
    month_end = period_index.to_timestamp(how="end").normalize()
    out["is_partial_month"] = (as_of_ts >= month_start) & (as_of_ts < month_end)

    logger.info(
        f"Reserve transfers: {int(out['n_transfers'].sum())} transfer rows | "
        f"{int((out['n_transfers'] > 0).sum())} months with a transfer | "
        f"{out['moved_to_cash'].sum():,.0f} {cfg['base_currency']} moved to cash"
    )
    return out.reset_index()[RESERVE_TRANSFER_COLUMNS]


def summarise_reserve_transfers(
    transfers: pd.DataFrame | None,
    monthly: pd.DataFrame | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Summarise the reserve transfers over the last 12 complete months.

    Parameters
    ----------
    transfers : pd.DataFrame | None
        Output of :func:`build_reserve_transfers`.
    monthly : pd.DataFrame | None
        Output of :func:`build_budget_health_monthly`, used for investment
        adherence.
    config : dict[str, Any] | None
        The ``budget_health`` config section. ``plan.salary_pln``,
        ``plan.salary_day`` and ``plan.investment_pln`` drive adherence.

    Returns
    -------
    dict
        ``n_months``, ``mean``, ``min``, ``max``, ``cv``,
        ``months_with_transfer``, ``distinct_transfer_days``,
        ``income_days_range``, ``salary_adherence`` and
        ``investment_adherence``. Adherence values are ``{"on_plan", "of"}`` or
        None when the matching plan key is null.
    """
    cfg = _resolve_config(config)
    summary: dict[str, Any] = {
        "n_months": 0,
        "mean": np.nan,
        "min": np.nan,
        "max": np.nan,
        "cv": np.nan,
        "months_with_transfer": 0,
        "distinct_transfer_days": 0,
        "income_days_range": "",
        "salary_adherence": None,
        "investment_adherence": None,
    }
    if transfers is None or transfers.empty:
        return summary

    complete = transfers[~transfers["is_partial_month"].astype(bool)]
    window = complete.tail(12).reset_index(drop=True)
    if window.empty:
        return summary

    moved = pd.to_numeric(window["moved_to_cash"], errors="coerce")
    mean = float(moved.mean())
    summary["n_months"] = int(len(window))
    summary["mean"] = mean
    summary["min"] = float(moved.min())
    summary["max"] = float(moved.max())
    summary["cv"] = float(moved.std() / mean) if mean else np.nan
    summary["months_with_transfer"] = int((window["n_transfers"] > 0).sum())

    days: set[int] = set()
    for label in window["transfer_days"].astype(str):
        for part in label.split(","):
            part = part.strip()
            if part.isdigit():
                days.add(int(part))
    summary["distinct_transfer_days"] = len(days)

    income_days: set[int] = set()
    for label in window["income_days"].astype(str):
        for part in label.split(","):
            part = part.strip()
            if part.isdigit():
                income_days.add(int(part))
    if income_days:
        low, high = min(income_days), max(income_days)
        summary["income_days_range"] = str(low) if low == high else f"{low}–{high}"


    plan = cfg["plan"]
    salary = plan.get("salary_pln")
    if salary is not None:
        salary = float(salary)
        salary_day = plan.get("salary_day")
        on_plan = 0
        for row in window.itertuples(index=False):
            if abs(float(row.moved_to_cash) - salary) > 0.10 * salary:
                continue
            if salary_day is not None:
                transfer_days = [
                    int(part) for part in str(row.transfer_days).split(",") if part.strip()
                ]
                if not transfer_days or abs(min(transfer_days) - int(salary_day)) > 3:
                    continue
            on_plan += 1
        summary["salary_adherence"] = {"on_plan": on_plan, "of": int(len(window))}

    investment = plan.get("investment_pln")
    if investment is not None and monthly is not None and not monthly.empty:
        monthly_complete = monthly[~monthly["is_partial_month"].astype(bool)]
        months_in_window = set(window["year_month"])
        monthly_window = monthly_complete[monthly_complete["year_month"].isin(months_in_window)]
        if not monthly_window.empty:
            invested = pd.to_numeric(monthly_window["investments"], errors="coerce")
            summary["investment_adherence"] = {
                "on_plan": int((invested >= float(investment)).sum()),
                "of": int(len(monthly_window)),
            }
    return summary
