"""
Parse MMEX SQLite database (.mmb) into a validated DataFrame.

Usage:
    from src.ingestion.mmex_sqlite_parser import parse_mmex_sqlite
    df = parse_mmex_sqlite("data/raw/my_finances.mmb")
"""

import sqlite3
from pathlib import Path

import pandas as pd

from src.ingestion.currency_converter import apply_pln_conversion
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

# Default NBP rate cache path — resolved relative to this file so it works
# regardless of the caller's working directory.
_DEFAULT_RATE_CACHE: str = str(
    Path(__file__).parents[2] / "data" / "interim" / "rate_cache.json"
)

TRANSACTION_QUERY = """
SELECT
    t.TRANSID               AS transaction_id,
    t.TRANSDATE             AS date,
    a.ACCOUNTNAME           AS account,
    a2.ACCOUNTNAME          AS to_account,
    p.PAYEENAME             AS payee,
    CASE WHEN c.PARENTID = -1 OR c.PARENTID IS NULL
         THEN c.CATEGNAME ELSE par.CATEGNAME END AS category,
    CASE WHEN c.PARENTID = -1 OR c.PARENTID IS NULL
         THEN NULL ELSE c.CATEGNAME END AS subcategory,
    t.TRANSAMOUNT           AS amount,
    t.TRANSCODE             AS type,
    t.NOTES                 AS notes,
    t.TOTRANSAMOUNT         AS to_amount,
    t.TRANSACTIONNUMBER     AS transaction_number,
    cur.CURRENCY_SYMBOL     AS currency
FROM CHECKINGACCOUNT_V1 t
LEFT JOIN ACCOUNTLIST_V1 a    ON t.ACCOUNTID    = a.ACCOUNTID
LEFT JOIN ACCOUNTLIST_V1 a2   ON t.TOACCOUNTID  = a2.ACCOUNTID
LEFT JOIN PAYEE_V1 p          ON t.PAYEEID       = p.PAYEEID
LEFT JOIN CATEGORY_V1 c       ON t.CATEGID       = c.CATEGID
LEFT JOIN CATEGORY_V1 par     ON c.PARENTID      = par.CATEGID AND c.PARENTID != -1
LEFT JOIN ACCOUNTLIST_V1 acur ON t.ACCOUNTID     = acur.ACCOUNTID
LEFT JOIN CURRENCYFORMATS_V1 cur ON acur.CURRENCYID = cur.CURRENCYID
WHERE (t.STATUS IS NULL OR t.STATUS NOT IN ('Void', 'Duplicate'))
  AND (t.DELETEDTIME IS NULL OR t.DELETEDTIME = '')
ORDER BY t.TRANSDATE
"""

# Fetches split lines for a set of parent TRANSIDs.
# Placeholder list must be formatted in before passing to read_sql_query.
_SPLIT_QUERY = """
SELECT
    s.TRANSID           AS transaction_id,
    s.SPLITTRANSAMOUNT  AS amount,
    s.NOTES             AS split_notes,
    CASE WHEN c.PARENTID = -1 OR c.PARENTID IS NULL
         THEN c.CATEGNAME ELSE par.CATEGNAME END AS category,
    CASE WHEN c.PARENTID = -1 OR c.PARENTID IS NULL
         THEN NULL ELSE c.CATEGNAME END AS subcategory
FROM SPLITTRANSACTIONS_V1 s
LEFT JOIN CATEGORY_V1 c   ON s.CATEGID  = c.CATEGID
LEFT JOIN CATEGORY_V1 par ON c.PARENTID = par.CATEGID AND c.PARENTID != -1
WHERE s.TRANSID IN ({placeholders})
"""

ACCOUNT_BALANCES_QUERY = """
SELECT
    ACCOUNTNAME AS account,
    INITIALBAL  AS opening_balance,
    STATUS      AS status
FROM ACCOUNTLIST_V1
WHERE STATUS = 'Open'
"""


def _check_schema_version(conn: sqlite3.Connection) -> None:
    """Warn if MMEX database version differs from the expected v3 schema."""
    try:
        row = conn.execute(
            "SELECT INFOVALUE FROM INFOTABLE_V1 WHERE INFONAME = 'DATAVERSION'"
        ).fetchone()
        if row and row[0] != "3":
            logger.warning(
                f"MMEX DATAVERSION is '{row[0]}', expected '3'. "
                "Schema may differ from what the parser expects."
            )
        else:
            logger.debug(f"MMEX DATAVERSION: {row[0] if row else 'not found'}")
    except Exception as exc:
        logger.warning(f"Could not read INFOTABLE_V1.DATAVERSION: {exc}")


def _expand_splits(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Replace split-transaction parent rows with their individual split lines.

    In MMEX, split transactions have CATEGID = NULL on the parent row in
    CHECKINGACCOUNT_V1. Each split line in SPLITTRANSACTIONS_V1 carries its
    own CATEGID and SPLITTRANSAMOUNT. This function expands those parent rows
    so every output row has a resolvable category.

    Amounts in the split lines are raw (unsigned) — sign convention is applied
    by the caller after this function returns.
    """
    null_cat_ids = df.loc[df["category"].isna(), "transaction_id"].tolist()
    if not null_cat_ids:
        return df

    placeholders = ",".join("?" * len(null_cat_ids))
    split_id_rows = conn.execute(
        f"SELECT DISTINCT TRANSID FROM SPLITTRANSACTIONS_V1 "
        f"WHERE TRANSID IN ({placeholders})",
        null_cat_ids,
    ).fetchall()
    split_ids = [r[0] for r in split_id_rows]

    if not split_ids:
        logger.debug("Null-category rows found but none have split entries in SPLITTRANSACTIONS_V1")
        return df

    logger.info(f"Expanding {len(split_ids)} split-transaction parents into individual lines")

    # Warn if category nesting deeper than 2 levels is detected.
    # The parser only resolves one parent-level join; deeper names collapse silently.
    deep_count = conn.execute("""
        SELECT COUNT(*) FROM CATEGORY_V1 c
        WHERE c.PARENTID != -1 AND c.PARENTID IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM CATEGORY_V1 p
              WHERE p.CATEGID = c.PARENTID
                AND p.PARENTID != -1
                AND p.PARENTID IS NOT NULL
          )
    """).fetchone()[0]
    if deep_count:
        logger.warning(
            f"{deep_count} categories are nested more than 2 levels deep. "
            "The parser resolves only parent → child; deeper names will be collapsed."
        )

    sp = ",".join("?" * len(split_ids))
    splits_df = pd.read_sql_query(
        _SPLIT_QUERY.format(placeholders=sp), conn, params=split_ids
    )

    # Preserve all parent-level context columns except amount/category/subcategory
    context_cols = [
        c for c in df.columns
        if c not in ("amount", "category", "subcategory", "notes")
        and c in df.columns
    ]
    parents_context = df[df["transaction_id"].isin(split_ids)][context_cols]
    parent_notes = (
        df[df["transaction_id"].isin(split_ids)][["transaction_id", "notes"]]
        .rename(columns={"notes": "parent_notes"})
    )

    expanded = splits_df.merge(parents_context, on="transaction_id", how="left")
    expanded = expanded.merge(parent_notes, on="transaction_id", how="left")
    # Prefer the split-level note; fall back to the parent note
    expanded["notes"] = expanded["split_notes"].where(
        expanded["split_notes"].notna() & expanded["split_notes"].ne(""),
        expanded["parent_notes"],
    )
    expanded = expanded.drop(columns=["split_notes", "parent_notes"])
    expanded = expanded.reindex(columns=df.columns)

    return (
        pd.concat([df[~df["transaction_id"].isin(split_ids)], expanded], ignore_index=True)
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )


def parse_mmex_sqlite(
    filepath: str,
    rate_cache: str | None = _DEFAULT_RATE_CACHE,
) -> pd.DataFrame:
    """
    Read MMEX .mmb SQLite file and return a normalised transaction DataFrame.

    Exchange-rate resolution order for non-PLN transactions:
      1. NBP rate cached in *rate_cache* (fetched from api.nbp.pl on first use,
         then served from the local JSON file on every subsequent run).
      2. Most-recent ``CURRENCYHISTORY_V1`` entry from the .mmb whose date is
         <= the transaction date  (only for currencies the NBP cache does not
         cover — e.g. GEL).
      3. ``CURRENCYFORMATS_V1.BASECONVRATE`` static fallback.
      4. Rate 1.0 with a logged WARNING (no conversion performed).

    Parameters
    ----------
    filepath : str
        Path to the .mmb file.
    rate_cache : str | None
        Path to the local JSON NBP rate cache.  Defaults to
        ``data/interim/rate_cache.json`` inside the project root so that
        callers (notebooks, pipeline script, tests) all hit the same cache
        automatically.  Pass ``None`` only to force DB-only rates (legacy /
        offline mode — use with caution if the DB contains corrupted rates).

    Returns
    -------
    pd.DataFrame
        DataFrame with canonical column names ready for validation.
    """
    filepath = str(Path(filepath).resolve())
    logger.info(f"Parsing MMEX SQLite: {filepath}")

    conn = sqlite3.connect(filepath)
    try:
        _check_schema_version(conn)

        df = pd.read_sql_query(TRANSACTION_QUERY, conn)
        logger.info(f"Loaded {len(df)} transactions from SQLite (Void/Deleted excluded)")

        df["date"] = pd.to_datetime(df["date"])

        # Expand split transactions before applying sign convention so that
        # each split line (with its raw unsigned amount) gets signed correctly.
        df = _expand_splits(df, conn)
        logger.info(f"{len(df)} rows after split expansion")

        # Sign convention: MMEX stores all amounts as positive.
        # Withdrawals → negative (money leaves the account).
        # Transfers   → negative on the source-account side
        #               (TRANSAMOUNT is the outflow; TOTRANSAMOUNT is the inflow
        #               to the destination account in its currency).
        # Deposits    → remain positive.
        mask_outflow = df["type"].str.lower().isin(["withdrawal", "transfer"])
        df.loc[mask_outflow, "amount"] *= -1
        n_wd = (df["type"].str.lower() == "withdrawal").sum()
        n_tr = (df["type"].str.lower() == "transfer").sum()
        logger.info(f"Sign-flipped {mask_outflow.sum()} outflow rows ({n_wd} withdrawals, {n_tr} transfers)")

        # Normalise type to lowercase
        df["type"] = df["type"].str.lower()

        # Convert non-base-currency amounts to PLN (adds `amount_pln` column).
        # Rows already in the base currency get amount_pln == amount.
        df = apply_pln_conversion(df, filepath, rate_cache=rate_cache)

        logger.info(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
        return df
    finally:
        conn.close()


def get_account_balances(filepath: str) -> pd.DataFrame:
    """
    Extract opening balances per account from MMEX SQLite.

    Parameters
    ----------
    filepath : str
        Path to the .mmb file.

    Returns
    -------
    pd.DataFrame
        Columns: account, opening_balance, status.
    """
    filepath = str(Path(filepath).resolve())
    conn = sqlite3.connect(filepath)
    try:
        balances = pd.read_sql_query(ACCOUNT_BALANCES_QUERY, conn)
        logger.info(f"Found {len(balances)} open accounts with initial balances")
        return balances
    finally:
        conn.close()
