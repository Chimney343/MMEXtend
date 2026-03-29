"""
Validate parsed MMEX data. Produces a data quality report.
Halts pipeline if critical thresholds are breached.

Usage:
    from src.ingestion.validator import validate_transactions
    report = validate_transactions(df)
    report.display()
    if not report.is_passing():
        raise SystemExit("Validation failed")
"""

from dataclasses import dataclass, field

import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

CRITICAL_NULL_THRESHOLD = 0.02  # 2% — pipeline halts above this for date/amount


@dataclass
class DataQualityReport:
    rows_loaded: int = 0
    date_min: str = ""
    date_max: str = ""
    null_rates: dict[str, float] = field(default_factory=dict)
    currency_count: int = 0
    currencies_found: list[str] = field(default_factory=list)
    uncategorised_count: int = 0
    uncategorised_pct: float = 0.0
    null_account_count: int = 0
    date_gaps: list[tuple[str, str, int]] = field(default_factory=list)
    critical_failures: list[str] = field(default_factory=list)

    def is_passing(self) -> bool:
        return len(self.critical_failures) == 0

    def display(self) -> None:
        """Pretty-print the report (designed for notebook cell output)."""
        print("=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)
        print(f"  Rows loaded:        {self.rows_loaded}")
        print(f"  Date range:         {self.date_min} → {self.date_max}")
        print(f"  Currencies:         {self.currencies_found}")
        print(
            f"  Uncategorised txns: {self.uncategorised_count}"
            f" ({self.uncategorised_pct:.1f}%)"
        )
        print(f"  Null accounts:      {self.null_account_count}")
        print(f"\n  Null rates per column:")
        for col, rate in sorted(self.null_rates.items()):
            critical = (
                " *** CRITICAL"
                if rate > CRITICAL_NULL_THRESHOLD and col in ("date", "amount")
                else ""
            )
            print(f"    {col:25s} {rate:6.2%}{critical}")
        if self.date_gaps:
            print(f"\n  Date gaps > 14 days:")
            for start, end, days in self.date_gaps:
                print(f"    {start} → {end} ({days} days)")
        if self.critical_failures:
            print(f"\n  *** CRITICAL FAILURES (pipeline halted):")
            for failure in self.critical_failures:
                print(f"    - {failure}")
        print("=" * 60)


def validate_transactions(df: pd.DataFrame) -> DataQualityReport:
    """
    Run all validation checks and return a DataQualityReport.

    Parameters
    ----------
    df : pd.DataFrame
        Output of parse_mmex_csv() or parse_mmex_sqlite().

    Returns
    -------
    DataQualityReport
        Call .is_passing() to check whether the pipeline should continue.
    """
    report = DataQualityReport()
    report.rows_loaded = len(df)

    logger.info(f"Validating {len(df)} rows")

    # Date range
    if "date" in df.columns and df["date"].notna().any():
        report.date_min = str(df["date"].min().date())
        report.date_max = str(df["date"].max().date())
        logger.info(f"Date range: {report.date_min} to {report.date_max}")
    else:
        report.date_min = "N/A"
        report.date_max = "N/A"

    # Null rates
    for col in df.columns:
        report.null_rates[col] = df[col].isna().mean()

    # Critical null checks (date, amount)
    for col in ("date", "amount"):
        if col in df.columns:
            rate = df[col].isna().mean()
            if rate > CRITICAL_NULL_THRESHOLD:
                msg = (
                    f"Null rate in '{col}' is {rate:.1%}"
                    f" (threshold: {CRITICAL_NULL_THRESHOLD:.0%})"
                )
                report.critical_failures.append(msg)
                logger.error(msg)

    # Currency check
    if "currency" in df.columns:
        currencies = df["currency"].dropna().unique().tolist()
        report.currency_count = len(currencies)
        report.currencies_found = currencies
        if len(currencies) > 1:
            logger.warning(f"Multiple currencies detected: {currencies}. Awaiting user input.")

    # Uncategorised transactions
    if "category" in df.columns:
        uncat_mask = df["category"].isna() | (df["category"].str.strip() == "")
        report.uncategorised_count = int(uncat_mask.sum())
        report.uncategorised_pct = 100.0 * uncat_mask.mean()
        if report.uncategorised_count > 0:
            logger.warning(
                f"{report.uncategorised_count} uncategorised transactions"
                f" ({report.uncategorised_pct:.1f}% of total)"
            )

    # Null accounts
    if "account" in df.columns:
        null_acct = df["account"].isna() | (df["account"].str.strip() == "")
        report.null_account_count = int(null_acct.sum())
        if report.null_account_count > 0:
            logger.warning(f"{report.null_account_count} transactions with null/blank account name")

    # Date gap check
    if "date" in df.columns and df["date"].notna().any():
        dates_sorted = df["date"].dropna().sort_values().reset_index(drop=True)
        gaps = dates_sorted.diff()
        big_gaps = gaps[gaps > pd.Timedelta(days=14)]
        for idx in big_gaps.index:
            gap_start = dates_sorted.iloc[idx - 1]
            gap_end = dates_sorted.iloc[idx]
            n_days = int((gap_end - gap_start).days)
            report.date_gaps.append((str(gap_start.date()), str(gap_end.date()), n_days))
            logger.warning(
                f"Date gap > 14 days: {gap_start.date()} to {gap_end.date()} ({n_days} days)"
            )

    if report.is_passing():
        logger.info("Validation passed")
    else:
        logger.error(f"Validation FAILED: {len(report.critical_failures)} critical issue(s)")

    return report
