"""Currency conversion: adds ``amount_pln`` to a transaction DataFrame.

Reads exchange-rate history and static base rates from an MMEX .mmb file to
convert each transaction amount into the database base currency (PLN in this
project).  Optionally augments (or replaces) those rates with live data from the
NBP (National Bank of Poland) API via :class:`~src.ingestion.rate_fetcher.RateFetcher`.

Usage:
    from src.ingestion.currency_converter import apply_pln_conversion

    # Legacy: use only rates stored in the .mmb file
    df = apply_pln_conversion(df, "data/raw/my_finances.mmb")

    # Recommended: augment with fetched NBP rates (cached locally)
    df = apply_pln_conversion(df, "data/raw/my_finances.mmb",
                              rate_cache="data/interim/rate_cache.json")

Rate resolution order (per transaction row):
  1. NBP-fetched rate cached in *rate_cache* (when provided).
  2. Most recent ``CURRENCYHISTORY_V1`` entry whose date ≤ transaction date
     (only for currencies that have no fetched rates).
  3. ``CURRENCYFORMATS_V1.BASECONVRATE`` (static fallback).
  4. 1.0 with a logged WARNING (no conversion performed).
"""

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

# ---------------------------------------------------------------------------
# SQL — read from MMEX .mmb
# ---------------------------------------------------------------------------

_BASE_CURRENCY_QUERY = """
SELECT c.CURRENCY_SYMBOL
FROM CURRENCYFORMATS_V1 c
JOIN INFOTABLE_V1 i ON CAST(i.INFOVALUE AS INTEGER) = c.CURRENCYID
WHERE i.INFONAME = 'BASECURRENCYID'
"""

_STATIC_RATES_QUERY = """
SELECT CURRENCY_SYMBOL AS currency, BASECONVRATE AS rate
FROM CURRENCYFORMATS_V1
"""

_RATE_HISTORY_QUERY = """
SELECT
    c.CURRENCY_SYMBOL  AS currency,
    h.CURRDATE         AS rate_date,
    h.CURRVALUE        AS rate
FROM CURRENCYHISTORY_V1 h
JOIN CURRENCYFORMATS_V1 c ON h.CURRENCYID = c.CURRENCYID
ORDER BY c.CURRENCY_SYMBOL, h.CURRDATE
"""

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

_EMPTY_HISTORY = pd.DataFrame(columns=["currency", "rate_date", "rate"])


def load_rates_from_mmb(
    filepath: str,
) -> tuple[str, pd.DataFrame, dict[str, float]]:
    """Load exchange-rate data from an MMEX .mmb file.

    Parameters
    ----------
    filepath : str
        Path to the .mmb SQLite file.

    Returns
    -------
    base_symbol : str
        Base currency symbol (e.g. ``"PLN"``).
    history : pd.DataFrame
        Columns: ``currency`` (str), ``rate_date`` (datetime64[ns]),
        ``rate`` (float).  Sorted by (currency, rate_date).
    static : dict[str, float]
        Fallback rates: currency_symbol → BASECONVRATE.
    """
    conn = sqlite3.connect(str(Path(filepath).resolve()))
    try:
        row = conn.execute(_BASE_CURRENCY_QUERY).fetchone()
        base_symbol: str = row[0] if row else "PLN"
        logger.info(f"Base currency: {base_symbol}")

        try:
            history = pd.read_sql_query(_RATE_HISTORY_QUERY, conn)
            history["rate_date"] = pd.to_datetime(history["rate_date"])
            # Guard against corrupted DB entries (e.g. rate accidentally entered
            # as an integer scaled value instead of a decimal).  Any rate
            # > 1 000 000 is physically impossible for a fiat-vs-PLN pair.
            bad_rates = history[history["rate"] > 1_000_000]
            if not bad_rates.empty:
                for _, row in bad_rates.iterrows():
                    logger.warning(
                        f"Corrupted rate in MMEX DB discarded: "
                        f"{row['currency']} {row['rate_date'].date()} "
                        f"rate={row['rate']:.3e} — fix this entry in MMEX "
                        "or it will be used when NBP cache is disabled."
                    )
                history = history[history["rate"] <= 1_000_000].reset_index(drop=True)
        except Exception:
            logger.warning(
                "CURRENCYHISTORY_V1 not found or unreadable; using static rates only."
            )
            history = _EMPTY_HISTORY.copy()

        try:
            static_df = pd.read_sql_query(_STATIC_RATES_QUERY, conn)
            static: dict[str, float] = dict(
                zip(static_df["currency"], static_df["rate"].astype(float))
            )
        except Exception:
            logger.warning("Could not read BASECONVRATE from CURRENCYFORMATS_V1.")
            static = {}
    finally:
        conn.close()

    logger.info(
        f"Loaded {len(history)} historical rate records "
        f"({history['currency'].nunique()} currencies with history)"
    )
    return base_symbol, history, static


def convert_to_base(
    df: pd.DataFrame,
    base_symbol: str,
    history: pd.DataFrame,
    static: dict[str, float],
) -> pd.DataFrame:
    """Add an ``amount_pln`` column: each amount converted to the base currency.

    For rows where ``currency == base_symbol`` the amount is copied as-is.
    For other currencies, the rate is resolved by finding the most recent
    ``CURRENCYHISTORY_V1`` entry whose date is ≤ the transaction date.  If no
    qualifying entry exists, ``CURRENCYFORMATS_V1.BASECONVRATE`` is used.  If
    neither source has a rate, 1.0 is applied and a WARNING is logged.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain ``amount`` (numeric), ``currency`` (str), ``date``
        (datetime64[ns]) columns.
    base_symbol : str
        Symbol of the base currency (e.g. ``"PLN"``).
    history : pd.DataFrame
        Output of :func:`load_rates_from_mmb` — columns: currency,
        rate_date, rate.
    static : dict[str, float]
        Fallback rates by currency symbol.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with ``amount_pln`` appended as the last column.
    """
    df = df.copy()

    # Guard: ensure history has the expected columns even when empty
    if history.empty or "currency" not in history.columns:
        history = _EMPTY_HISTORY.copy()

    # Build per-currency sorted numpy arrays for fast searchsorted lookup
    lookup: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for currency, grp in history.groupby("currency"):
        grp_sorted = grp.sort_values("rate_date")
        lookup[currency] = (
            grp_sorted["rate_date"].values,        # datetime64[ns]
            grp_sorted["rate"].values.astype(float),
        )

    rates = np.ones(len(df), dtype=float)

    for currency in df["currency"].dropna().unique():
        if currency == base_symbol:
            continue  # rate stays 1.0

        mask = (df["currency"] == currency).values
        txn_dates = df.loc[mask, "date"].values          # datetime64[ns]
        static_rate = float(static.get(currency, 1.0))

        if currency in lookup:
            hist_dates, hist_rates = lookup[currency]
            row_rates: list[float] = []
            for d in txn_dates:
                idx = int(np.searchsorted(hist_dates, d, side="right")) - 1
                row_rates.append(hist_rates[idx] if idx >= 0 else static_rate)
            rates[mask] = row_rates
        else:
            if static_rate == 1.0 and currency not in static:
                logger.warning(
                    f"No exchange-rate data found for '{currency}'; "
                    "amount_pln will equal amount (rate 1.0 applied)."
                )
            rates[mask] = static_rate

    df["amount_pln"] = df["amount"] * rates

    non_base_count = int((df["currency"] != base_symbol).sum())
    logger.info(
        f"Currency conversion complete: {len(df)} rows total, "
        f"{non_base_count} non-{base_symbol} rows converted."
    )
    return df


def _merge_histories(fetched: pd.DataFrame, db: pd.DataFrame) -> pd.DataFrame:
    """Merge fetched and DB history DataFrames.

    For any currency that appears in *fetched*, only the fetched rates are kept
    (preventing corrupted DB values from leaking through).  Currencies that are
    not covered by *fetched* fall back to the DB history.
    """
    if fetched.empty:
        return db
    if db.empty:
        return fetched
    fetched_currencies = set(fetched["currency"])
    db_only = db[~db["currency"].isin(fetched_currencies)]
    return (
        pd.concat([fetched, db_only], ignore_index=True)
        .sort_values(["currency", "rate_date"])
        .reset_index(drop=True)
    )


def apply_pln_conversion(
    df: pd.DataFrame,
    filepath: str,
    rate_cache: str | None = None,
) -> pd.DataFrame:
    """Load exchange rates and add an ``amount_pln`` column to *df*.

    Parameters
    ----------
    df : pd.DataFrame
        Must have ``amount``, ``currency``, ``date`` columns.
    filepath : str
        Path to the .mmb SQLite file.
    rate_cache : str | None
        Path to the local JSON rate cache used by
        :class:`~src.ingestion.rate_fetcher.RateFetcher`.  When provided,
        NBP rates are fetched (and cached) for all unique (currency, date)
        pairs present in *df*.  Fetched rates take full priority over DB rates
        for any currency they cover; the DB history is still used as a fallback
        for currencies the NBP API does not support.
        Pass ``None`` to use only MMEX DB rates (legacy behaviour).

    Returns
    -------
    pd.DataFrame
        *df* with ``amount_pln`` column added.
    """
    base_symbol, db_history, static = load_rates_from_mmb(filepath)

    if rate_cache is not None:
        from src.ingestion.rate_fetcher import RateFetcher  # lazy import

        fetcher = RateFetcher(rate_cache)
        fetcher.ensure_rates(df, base_currency=base_symbol)
        fetched = fetcher.as_history_df()
        history = _merge_histories(fetched, db_history)
        logger.info(
            f"Using fetched NBP rates for {fetched['currency'].nunique()} currency/ies; "
            f"DB history retained for remaining currencies."
        )
    else:
        history = db_history

    return convert_to_base(df, base_symbol, history, static)
