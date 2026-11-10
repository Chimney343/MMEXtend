"""
Feature engineering for MMEX transaction data.

Transforms a validated transactions DataFrame into analysis-ready features,
and builds monthly aggregates per category.

Usage:
    from src.analysis.feature_engineering import engineer_features, build_monthly_aggregates
    df_featured = engineer_features(df_validated)
    monthly = build_monthly_aggregates(df_featured)
"""

from datetime import date

import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


def _as_of_timestamp(as_of: date | pd.Timestamp | None) -> pd.Timestamp:
    """
    Coerce ``as_of`` to a naive, date-normalised Timestamp.

    Parameters
    ----------
    as_of : date | pd.Timestamp | None
        Cut-off date. ``None`` means today.

    Returns
    -------
    pd.Timestamp
        Midnight on the cut-off date.
    """
    if as_of is None:
        return pd.Timestamp.today().normalize()
    ts = pd.Timestamp(as_of)
    if ts.tz is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def partial_month_flags(year_month: pd.Series, as_of: date | pd.Timestamp | None) -> pd.Series:
    """
    Flag the calendar month that contains ``as_of`` as partial.

    A month is partial when ``month_start <= as_of < month_end`` (both
    normalised to midnight). ``as_of`` on the last day of a month therefore
    counts as a complete month, matching the cashflow-split rule.

    Parameters
    ----------
    year_month : pd.Series
        Month labels as ``"YYYY-MM"`` strings.
    as_of : date | pd.Timestamp | None
        Cut-off date. ``None`` means today.

    Returns
    -------
    pd.Series[bool]
        Same index as ``year_month``.
    """
    as_of_ts = _as_of_timestamp(as_of)
    period_index = pd.PeriodIndex(year_month.astype(str), freq="M")
    month_start = period_index.to_timestamp(how="start")
    month_end = period_index.to_timestamp(how="end").normalize()
    flags = (as_of_ts >= month_start) & (as_of_ts < month_end)
    return pd.Series(flags, index=year_month.index, dtype=bool)


def engineer_features(df: pd.DataFrame, as_of: date | None = None) -> pd.DataFrame:
    """
    Add derived time and classification columns to validated transactions.

    Adds:
    - year, month, quarter (integers)
    - year_month (str, "YYYY-MM") — parquet-safe period representation
    - month_ordinal (int, 0-based from earliest transaction) — for OLS fitting
    - is_transfer (bool)
    - is_uncategorised (bool)
    - is_future (bool) — dated after ``as_of``

    Parameters
    ----------
    df : pd.DataFrame
        Output of validate_transactions().
    as_of : date | None
        Run date. Rows dated after it are flagged ``is_future``. Defaults to
        today, so a pre-entered salary dated at month end is not treated as an
        actual until that day arrives.

    Returns
    -------
    pd.DataFrame
        Original columns plus engineered features.
    """
    df = df.copy()
    as_of_ts = _as_of_timestamp(as_of)

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["quarter"] = df["date"].dt.quarter
    df["year_month"] = df["date"].dt.strftime("%Y-%m")

    # 0-based ordinal for OLS trend fitting
    min_year = int(df["year"].min())
    min_month = int(df.loc[df["year"] == min_year, "month"].min())
    df["month_ordinal"] = (df["year"] - min_year) * 12 + (df["month"] - min_month)

    df["is_transfer"] = df["type"].str.lower() == "transfer"
    df["is_uncategorised"] = df["category"].isna() | (df["category"].str.strip() == "")
    df["is_future"] = df["date"] > as_of_ts

    # MMEX stores multi-level subcategory paths in CATEGNAME using "|" as a
    # separator (e.g. "Interests|Saving accounts", "Reselling|Tabletop|Board Games").
    # Split so that subcategory holds the first (main) level and subcategory_detail
    # holds the remainder, preserving None for rows without a subcategory.
    if "subcategory" in df.columns:
        split = df["subcategory"].str.split("|", n=1, expand=True)
        df["subcategory"] = split[0]
        df["subcategory_detail"] = split[1] if 1 in split.columns else None
    else:
        df["subcategory_detail"] = None

    n_pipe = (df.get("subcategory_detail", pd.Series(dtype=object)).notna()).sum()
    logger.info(
        f"Features engineered: {len(df)} rows | "
        f"{df['year_month'].nunique()} months | "
        f"{df['is_transfer'].sum()} transfers | "
        f"{df['is_future'].sum()} future-dated | "
        f"{df['is_uncategorised'].sum()} uncategorised | "
        f"{n_pipe} pipe-split subcategories"
    )
    return df


def actual_flows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return the rows that are real income and expense.

    A row is an actual flow when it is neither a transfer between own accounts
    nor future-dated. This is the single definition shared by the monthly
    builders, the category series and the dashboard.

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features(), or an older parquet that may lack the
        ``is_future`` column (every row then counts as actual).

    Returns
    -------
    pd.DataFrame
        A copy of the qualifying rows, preserving the input index.
    """
    mask = pd.Series(True, index=df.index)
    if "is_transfer" in df.columns:
        mask &= ~df["is_transfer"].fillna(False).astype(bool)
    if "is_future" in df.columns:
        mask &= ~df["is_future"].fillna(False).astype(bool)
    return df[mask].copy()


FLOW_TYPE_MAP: dict[str, str] = {
    "deposit": "income",
    "income": "income",
    "withdrawal": "expense",
    "expense": "expense",
}


def build_monthly_aggregates(df: pd.DataFrame, as_of: date | None = None) -> pd.DataFrame:
    """
    Aggregate gross flow per category, flow type and month.

    Deposits and withdrawals are never netted against each other: each
    (year_month, category) pair produces a separate ``income`` row for
    deposits and an ``expense`` row for withdrawals. ``total_amount`` is the
    positive magnitude of that flow, so expenses are positive spend and income
    is positive income. Transfers are excluded to avoid double-counting.

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features().
    as_of : date | None
        Run date, used to flag the partial month. Defaults to today.

    Returns
    -------
    pd.DataFrame
        Columns: year_month, category, flow_type, total_amount, month_ordinal,
        mom_delta, is_partial_month. ``flow_type`` is ``"expense"`` or
        ``"income"``.
    """
    df_no_transfers = actual_flows(df)

    amount_col = "amount_pln" if "amount_pln" in df_no_transfers.columns else "amount"
    df_no_transfers["flow_type"] = (
        df_no_transfers["type"].astype(str).str.lower().map(FLOW_TYPE_MAP)
    )
    df_no_transfers = df_no_transfers[df_no_transfers["flow_type"].notna()].copy()
    df_no_transfers["_amount"] = pd.to_numeric(
        df_no_transfers[amount_col], errors="coerce"
    ).abs()

    monthly = (
        df_no_transfers.groupby(["year_month", "category", "flow_type"])["_amount"]
        .sum()
        .reset_index()
        .rename(columns={"_amount": "total_amount"})
    )

    # Re-attach month_ordinal (minimum per year_month; all rows in same month share it)
    ordinal_map = df_no_transfers.groupby("year_month")["month_ordinal"].min()
    monthly["month_ordinal"] = monthly["year_month"].map(ordinal_map)

    # Month-over-month delta per category and flow type
    monthly = monthly.sort_values(["category", "flow_type", "year_month"]).reset_index(drop=True)
    monthly["mom_delta"] = monthly.groupby(["category", "flow_type"])["total_amount"].diff()

    monthly["is_partial_month"] = partial_month_flags(monthly["year_month"], as_of).values

    n_months = monthly["year_month"].nunique()
    n_cats = monthly["category"].nunique()
    logger.info(
        f"Monthly aggregates: {len(monthly)} rows | {n_months} months | "
        f"{n_cats} categories | {monthly['flow_type'].nunique()} flow types"
    )
    return monthly


def build_monthly_cashflow_split(
    df: pd.DataFrame,
    exclude_categories: list[str] | None = None,
    income_categories: list[str] | None = None,
    as_of: date | None = None,
) -> pd.DataFrame:
    """
    Aggregate total deposits and withdrawals per month.

    The category decides which side a row lands on; the sign decides the
    direction. Rows in an income category count toward deposits (a negative
    row there reduces income). Rows in any other category count toward
    withdrawals (a positive refund reduces spend). Uncategorised rows fall
    back to their ``type``, as before.

    Transfers and future-dated rows are excluded. Withdrawals are returned as
    positive values (absolute spend). Disposable income =
    deposits − withdrawals. Savings rate = disposable_income / deposits (NaN
    when deposits are 0). Months without transactions between the first and
    last month are zero-filled and logged as an imputation (HR-4).

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features(). Must have columns:
        year_month, month_ordinal, type, category, amount/is amount_pln.
    exclude_categories : list[str] | None
        Category names to strip before aggregation. Useful for one-off events
        (e.g. ``["Wedding"]``) that would distort the forecast baseline.
        Matched case-sensitively against the ``category`` column.
    income_categories : list[str] | None
        Categories whose rows are income. Defaults to ``["Income"]``.
    as_of : date | None
        Run date, used to flag the partial month and to drop future rows when
        the input has no ``is_future`` column. Defaults to today.

    Returns
    -------
    pd.DataFrame
        Columns: year_month, month_ordinal, deposits, withdrawals,
        disposable_income, savings_rate, is_partial_month.
    """
    as_of_ts = _as_of_timestamp(as_of)
    income_categories = list(income_categories) if income_categories is not None else ["Income"]

    columns = [
        "year_month",
        "month_ordinal",
        "deposits",
        "withdrawals",
        "disposable_income",
        "savings_rate",
        "is_partial_month",
    ]

    flows = actual_flows(df)

    if exclude_categories:
        mask = flows["category"].isin(exclude_categories)
        n_excluded = mask.sum()
        logger.info(
            f"Forecast exclusions: {n_excluded} transactions removed "
            f"({', '.join(exclude_categories)})"
        )
        flows = flows[~mask]

    if flows.empty:
        return pd.DataFrame(columns=columns)

    amount_col = "amount_pln" if "amount_pln" in flows.columns else "amount"
    category = flows["category"]
    if "is_uncategorised" in flows.columns:
        uncategorised = flows["is_uncategorised"].fillna(False).astype(bool)
    else:
        uncategorised = category.isna() | (category.astype(str).str.strip() == "")
    flow_type = flows["type"].astype(str).str.lower()

    deposit_mask = (category.isin(income_categories) & ~uncategorised) | (
        uncategorised & flow_type.isin(["deposit", "income"])
    )

    deposits = flows.loc[deposit_mask].groupby("year_month")[amount_col].sum().rename("deposits")
    withdrawals = (
        -flows.loc[~deposit_mask].groupby("year_month")[amount_col].sum()
    ).rename("withdrawals")

    split = pd.concat([deposits, withdrawals], axis=1).fillna(0.0).reset_index()

    # Continuous monthly grid from the first to the last actual month.
    required_months = (
        pd.period_range(split["year_month"].min(), split["year_month"].max(), freq="M")
        .strftime("%Y-%m")
        .tolist()
    )
    missing = [month for month in required_months if month not in set(split["year_month"])]
    if missing:
        logger.warning(
            "IMPUTATION | column=deposits,withdrawals | method=zero_fill_empty_month | "
            f"rows_affected={len(missing)} | "
            f"detail=months_without_transactions={', '.join(missing)}"
        )
    split = (
        split.set_index("year_month")
        .reindex(required_months, fill_value=0.0)
        .rename_axis("year_month")
        .reset_index()
    )
    split["month_ordinal"] = range(len(split))

    split["disposable_income"] = split["deposits"] - split["withdrawals"]
    split["savings_rate"] = split["disposable_income"] / split["deposits"].replace(0, float("nan"))

    negative = split.loc[split["withdrawals"] < 0, "year_month"].tolist()
    if negative:
        logger.warning(
            f"Negative withdrawals (refunds exceed spend) in {len(negative)} month(s): "
            f"{', '.join(negative)}. Sign kept, not clipped."
        )

    split["is_partial_month"] = partial_month_flags(split["year_month"], as_of_ts)
    split = split.sort_values("year_month").reset_index(drop=True)[columns]
    logger.info(
        f"Cashflow split: {len(split)} months | "
        f"avg deposits={split['deposits'].mean():.0f} | "
        f"avg withdrawals={split['withdrawals'].mean():.0f} | "
        f"avg savings_rate={split['savings_rate'].mean():.1%} | "
        f"partial month(s)={split['is_partial_month'].sum()}"
    )
    return split


def complete_cashflow_series(cashflow_split: pd.DataFrame) -> pd.Series:
    """
    Disposable income from complete months only, indexed by month start.

    The current month is still accruing, so it is excluded from any fitted or
    averaged series. ``build_monthly_cashflow_split`` marks it in
    ``is_partial_month``.

    Parameters
    ----------
    cashflow_split : pd.DataFrame
        Output of build_monthly_cashflow_split(). Must have columns
        year_month, disposable_income and is_partial_month.

    Returns
    -------
    pd.Series
        ``disposable_income`` for non-partial months, with a month-start
        ``DatetimeIndex``.
    """
    if "is_partial_month" in cashflow_split.columns:
        complete = cashflow_split[~cashflow_split["is_partial_month"].astype(bool)]
    else:
        complete = cashflow_split
    return pd.Series(
        complete["disposable_income"].values.astype(float),
        index=pd.to_datetime(complete["year_month"] + "-01"),
        name="disposable_income",
    )

