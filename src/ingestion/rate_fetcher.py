"""Fetch and cache exchange rates from the NBP (National Bank of Poland) API.

For a project using PLN as the base currency, NBP provides free, official,
no-auth mid-rates for all major currencies (EUR, GBP, USD, GEL, …).

The rate cache is a JSON file keyed ``{currency: {date_str: rate}}``.  Historical
rates are immutable, so anything already cached is still valid on every future run;
only the dates not yet cached are fetched from the API.

Usage:
    from src.ingestion.rate_fetcher import RateFetcher

    fetcher = RateFetcher("data/interim/rate_cache.json")
    n_new = fetcher.ensure_rates(df)  # df with 'currency' and 'date' columns
    history_df = fetcher.as_history_df()  # same format as load_rates_from_mmb
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

_NBP_URL = "https://api.nbp.pl/api/exchangerates/rates/a/{code}/{date}/?format=json"
_MAX_LOOKBACK_DAYS = 7   # search window for weekends / Polish public holidays
_REQUEST_TIMEOUT = 10    # seconds


class RateFetcher:
    """Download, cache, and serve PLN mid-rates from NBP.

    Rates are stored as *units of foreign currency per 1 PLN*, i.e.
    the value returned by NBP's "mid" field which is PLN per 1 unit of
    the foreign currency.

    On weekends and Polish public holidays NBP does not publish rates.
    When an exact date is unavailable the fetcher walks back up to
    ``_MAX_LOOKBACK_DAYS`` days to return the most recent published rate,
    and caches the result under the requested transaction date so that
    subsequent lookups are instant.
    """

    def __init__(self, cache_path: str | Path = "data/interim/rate_cache.json") -> None:
        self._cache_path = Path(cache_path)
        self._cache: dict[str, dict[str, float]] = self._load_cache()

    # ──────────────────────────────────────────────────────── cache helpers ──

    def _load_cache(self) -> dict[str, dict[str, float]]:
        if self._cache_path.exists():
            with self._cache_path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        return {}

    def _save_cache(self) -> None:
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        with self._cache_path.open("w", encoding="utf-8") as fh:
            json.dump(self._cache, fh, indent=2, sort_keys=True)

    # ──────────────────────────────────────────────────── NBP network call ──

    def _fetch_from_nbp(self, currency: str, on_date: date) -> float | None:
        """GET a single mid-rate from NBP.

        Returns
        -------
        float
            Mid-rate (PLN per 1 unit of *currency*) if published for *on_date*.
        None
            If the API responds 404 — the date has no rate (weekend / holiday).

        Raises
        ------
        requests.RequestException
            On any network error or non-200/404 HTTP status so callers can decide
            how to handle partial failures.
        """
        url = _NBP_URL.format(code=currency.lower(), date=on_date.isoformat())
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return float(resp.json()["rates"][0]["mid"])

    def _fetch_effective_rate(
        self, currency: str, on_date: date
    ) -> tuple[date, float] | None:
        """Return ``(effective_date, rate)`` for *currency* on or before *on_date*.

        Walks back up to ``_MAX_LOOKBACK_DAYS`` days when the exact date has no
        published rate.  Returns ``None`` if no rate is found in that window
        (unsupported currency or very old / future date).

        Raises
        ------
        requests.RequestException
            Propagated from :meth:`_fetch_from_nbp` on network failures.
        """
        for days_back in range(_MAX_LOOKBACK_DAYS + 1):
            lookup_date = on_date - timedelta(days=days_back)
            rate = self._fetch_from_nbp(currency, lookup_date)
            if rate is not None:
                return lookup_date, rate
        logger.warning(
            f"No NBP rate for {currency} within {_MAX_LOOKBACK_DAYS} days of {on_date}. "
            "Currency may not be covered by NBP Table A."
        )
        return None

    # ──────────────────────────────────────────────────────── public API ──

    def get_rate(self, currency: str, on_date: date) -> float | None:
        """Return the cached rate for *(currency, on_date)*, or ``None``."""
        return self._cache.get(currency, {}).get(on_date.isoformat())

    def ensure_rates(self, df: pd.DataFrame, base_currency: str = "PLN") -> int:
        """Ensure every unique (currency, date) pair in *df* is cached.

        Only non-base-currency rows are considered.  Already-cached pairs are
        skipped; missing ones are fetched from NBP.  If a network call fails for
        a particular pair a WARNING is logged and execution continues — partial
        success is possible.

        Parameters
        ----------
        df : pd.DataFrame
            Must have ``currency`` (str) and ``date`` (datetime-like) columns.
        base_currency : str
            Rows whose ``currency`` equals *base_currency* are ignored.

        Returns
        -------
        int
            Number of new rates fetched and added to the cache.
        """
        if df.empty:
            return 0

        non_base = df.loc[df["currency"] != base_currency, ["currency", "date"]].drop_duplicates()
        if non_base.empty:
            return 0

        fetched_count = 0
        save_needed = False

        pairs = [
            (row["currency"], row["date"].date() if hasattr(row["date"], "date") else row["date"])
            for _, row in non_base.iterrows()
        ]
        uncached = [(ccy, d) for ccy, d in pairs if self.get_rate(ccy, d) is None]

        with tqdm(
            uncached,
            desc="Fetching NBP rates",
            unit="rate",
            disable=len(uncached) == 0,
        ) as pbar:
            for currency, txn_date in pbar:
                pbar.set_postfix_str(f"{currency} {txn_date}")

                try:
                    result = self._fetch_effective_rate(currency, txn_date)
                except requests.RequestException as exc:
                    logger.warning(
                        f"Network error fetching NBP rate for {currency} on {txn_date}: {exc}. "
                        "Skipping; DB rate or static fallback will be used."
                    )
                    continue

                if result is None:
                    continue  # unsupported currency — silently skip, DB fallback applies

                effective_date, rate = result
                if currency not in self._cache:
                    self._cache[currency] = {}

                # Cache under the transaction date for direct lookup
                self._cache[currency][txn_date.isoformat()] = rate
                # Also propagate to the actual effective date to avoid re-fetching
                if effective_date != txn_date:
                    self._cache[currency][effective_date.isoformat()] = rate

                fetched_count += 1
                save_needed = True
                logger.debug(
                    f"Fetched NBP: {currency} {txn_date} → {rate:.6f} "
                    f"(effective date: {effective_date})"
                )

        if save_needed:
            self._save_cache()
            logger.info(
                f"Rate cache updated: {fetched_count} new rate(s) → {self._cache_path}"
            )
        else:
            logger.debug("All required rates already cached — 0 new fetches.")

        return fetched_count

    def as_history_df(self) -> pd.DataFrame:
        """Return all cached rates as a history DataFrame.

        The returned DataFrame has columns ``currency``, ``rate_date``, ``rate``
        — the same schema that :func:`~src.ingestion.currency_converter.load_rates_from_mmb`
        returns — so it can be passed directly to
        :func:`~src.ingestion.currency_converter.convert_to_base`.
        """
        rows: list[dict] = [
            {"currency": ccy, "rate_date": date_str, "rate": rate}
            for ccy, dated_rates in self._cache.items()
            for date_str, rate in dated_rates.items()
        ]
        if not rows:
            return pd.DataFrame(columns=["currency", "rate_date", "rate"])

        df = pd.DataFrame(rows)
        df["rate_date"] = pd.to_datetime(df["rate_date"])
        df["rate"] = df["rate"].astype(float)
        return df.sort_values(["currency", "rate_date"]).reset_index(drop=True)
