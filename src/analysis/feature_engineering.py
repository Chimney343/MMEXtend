"""
Feature engineering for MMEX transaction data.

Transforms a validated transactions DataFrame into analysis-ready features,
and builds monthly aggregates per category.

Usage:
    from src.analysis.feature_engineering import engineer_features, build_monthly_aggregates
    df_featured = engineer_features(df_validated)
    monthly = build_monthly_aggregates(df_featured)
"""

import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived time and classification columns to validated transactions.

    Adds:
    - year, month, quarter (integers)
    - year_month (str, "YYYY-MM") — parquet-safe period representation
    - month_ordinal (int, 0-based from earliest transaction) — for OLS fitting
    - is_transfer (bool)
    - is_uncategorised (bool)

    Parameters
    ----------
    df : pd.DataFrame
        Output of validate_transactions().

    Returns
    -------
    pd.DataFrame
        Original columns plus engineered features.
    """
    df = df.copy()

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["quarter"] = df["date"].dt.quarter
    df["year_month"] = df["date"].dt.strftime("%Y-%m")

    # 0-based ordinal for OLS trend fitting
    min_ym = df["year_month"].min()
    min_year = int(min_ym[:4])
    min_month = int(min_ym[5:7])

    def _to_ordinal(ym: str) -> int:
        y, m = int(ym[:4]), int(ym[5:7])
        return (y - min_year) * 12 + (m - min_month)

    df["month_ordinal"] = df["year_month"].map(_to_ordinal)

    df["is_transfer"] = df["type"].str.lower() == "transfer"
    df["is_uncategorised"] = df["category"].isna() | (df["category"].str.strip() == "")

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
        f"{df['is_uncategorised'].sum()} uncategorised | "
        f"{n_pipe} pipe-split subcategories"
    )
    return df


def build_monthly_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate net spend per category per month.

    Excludes transfers to avoid double-counting.
    Adds month-over-month delta per category.

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features().

    Returns
    -------
    pd.DataFrame
        Columns: year_month, category, total_amount, month_ordinal, mom_delta.
    """
    df_no_transfers = df[~df["is_transfer"]].copy()

    amount_col = "amount_pln" if "amount_pln" in df_no_transfers.columns else "amount"
    monthly = (
        df_no_transfers.groupby(["year_month", "category"])[amount_col]
        .sum()
        .reset_index()
        .rename(columns={amount_col: "total_amount"})
    )

    # Re-attach month_ordinal (minimum per year_month; all rows in same month share it)
    ordinal_map = (
        df_no_transfers.groupby("year_month")["month_ordinal"].min()
    )
    monthly["month_ordinal"] = monthly["year_month"].map(ordinal_map)

    # Month-over-month delta per category
    monthly = monthly.sort_values(["category", "year_month"]).reset_index(drop=True)
    monthly["mom_delta"] = monthly.groupby("category")["total_amount"].diff()

    n_months = monthly["year_month"].nunique()
    n_cats = monthly["category"].nunique()
    logger.info(f"Monthly aggregates: {len(monthly)} rows | {n_months} months | {n_cats} categories")
    return monthly


def build_monthly_cashflow_split(
    df: pd.DataFrame,
    exclude_categories: list[str] | None = None,
) -> pd.DataFrame:
    """
    Aggregate total deposits and withdrawals per month.

    Excludes transfers. Withdrawals are returned as positive values (absolute spend).
    Disposable income = deposits − withdrawals.
    Savings rate = disposable_income / deposits (NaN when deposits are 0).

    Parameters
    ----------
    df : pd.DataFrame
        Output of engineer_features(). Must have columns:
        year_month, month_ordinal, type, amount, is_transfer.
    exclude_categories : list[str] | None
        Category names to strip before aggregation. Useful for one-off events
        (e.g. ``["Wedding"]``) that would distort the forecast baseline.
        Matched case-sensitively against the ``category`` column.

    Returns
    -------
    pd.DataFrame
        Columns: year_month, month_ordinal, deposits, withdrawals,
                 disposable_income, savings_rate.
    """
    df_no_transfers = df[~df["is_transfer"]].copy()

    if exclude_categories:
        mask = df_no_transfers["category"].isin(exclude_categories)
        n_excluded = mask.sum()
        logger.info(
            f"Forecast exclusions: {n_excluded} transactions removed "
            f"({', '.join(exclude_categories)})"
        )
        df_no_transfers = df_no_transfers[~mask]

    amount_col = "amount_pln" if "amount_pln" in df_no_transfers.columns else "amount"
    deposits = (
        df_no_transfers[df_no_transfers["type"] == "deposit"]
        .groupby("year_month")[amount_col]
        .sum()
        .rename("deposits")
    )
    withdrawals = (
        df_no_transfers[df_no_transfers["type"] == "withdrawal"]
        .groupby("year_month")[amount_col]
        .sum()
        .abs()
        .rename("withdrawals")
    )

    split = pd.concat([deposits, withdrawals], axis=1).fillna(0).reset_index()
    split["disposable_income"] = split["deposits"] - split["withdrawals"]
    split["savings_rate"] = (
        split["disposable_income"] / split["deposits"].replace(0, float("nan"))
    )

    ordinal_map = df_no_transfers.groupby("year_month")["month_ordinal"].min()
    split["month_ordinal"] = split["year_month"].map(ordinal_map)

    split = split.sort_values("year_month").reset_index(drop=True)
    split = split[["year_month", "month_ordinal", "deposits", "withdrawals",
                   "disposable_income", "savings_rate"]]

    logger.info(
        f"Cashflow split: {len(split)} months | "
        f"avg deposits={split['deposits'].mean():.0f} | "
        f"avg withdrawals={split['withdrawals'].mean():.0f} | "
        f"avg savings_rate={split['savings_rate'].mean():.1%}"
    )
    return split
