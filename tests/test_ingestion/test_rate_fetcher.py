"""Tests for src.ingestion.rate_fetcher.RateFetcher.

Covers:
- Cache read / write (no network)
- NBP API response parsing
- Weekend / holiday walkback (404 → try previous day)
- Partial failure handling (network error skips currency-date, others continue)
- base-currency rows are ignored
- as_history_df() schema and types
- Integration: fetched rates override corrupted DB rates in apply_pln_conversion
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import Mock, call, patch

import numpy.testing as npt
import pandas as pd
import pytest
import requests

from src.ingestion.currency_converter import apply_pln_conversion, _merge_histories
from src.ingestion.rate_fetcher import RateFetcher

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).assign(
        date=lambda df: pd.to_datetime(df["date"]),
        amount=lambda df: df["amount"].astype(float),
    )


def _nbp_ok(rate: float, date_str: str = "2024-02-01") -> Mock:
    """Simulate a successful NBP response."""
    m = Mock()
    m.status_code = 200
    m.json.return_value = {
        "table": "A",
        "currency": "British Pound",
        "code": "GBP",
        "rates": [{"no": "001/A/NBP/2024", "effectiveDate": date_str, "mid": rate}],
    }
    m.raise_for_status.return_value = None
    return m


def _nbp_404() -> Mock:
    """Simulate a 404 (no rate for that date)."""
    m = Mock()
    m.status_code = 404
    return m


@pytest.fixture
def cache_path(tmp_path: Path) -> str:
    return str(tmp_path / "rate_cache.json")


@pytest.fixture
def fetcher(cache_path: str) -> RateFetcher:
    return RateFetcher(cache_path)


# ---------------------------------------------------------------------------
# Cache read / write
# ---------------------------------------------------------------------------

class TestCachePersistence:
    def test_get_rate_returns_none_when_empty(self, fetcher: RateFetcher) -> None:
        assert fetcher.get_rate("GBP", date(2024, 2, 1)) is None

    def test_fetched_rate_survives_reload(self, cache_path: str) -> None:
        """A rate saved during one run must be readable in a new RateFetcher instance."""
        f1 = RateFetcher(cache_path)
        with patch("requests.get", return_value=_nbp_ok(5.03)):
            f1.ensure_rates(_make_df([{"date": "2024-02-01", "currency": "GBP", "amount": -10.0}]))

        f2 = RateFetcher(cache_path)
        assert f2.get_rate("GBP", date(2024, 2, 1)) == pytest.approx(5.03)

    def test_cache_file_is_created(self, cache_path: str) -> None:
        f = RateFetcher(cache_path)
        with patch("requests.get", return_value=_nbp_ok(5.03)):
            f.ensure_rates(_make_df([{"date": "2024-02-01", "currency": "GBP", "amount": -10.0}]))
        assert Path(cache_path).exists()

    def test_cache_file_is_valid_json(self, cache_path: str) -> None:
        f = RateFetcher(cache_path)
        with patch("requests.get", return_value=_nbp_ok(5.03)):
            f.ensure_rates(_make_df([{"date": "2024-02-01", "currency": "GBP", "amount": -10.0}]))
        with open(cache_path) as fh:
            data = json.load(fh)
        assert data["GBP"]["2024-02-01"] == pytest.approx(5.03)


# ---------------------------------------------------------------------------
# ensure_rates: skip / fetch behaviour
# ---------------------------------------------------------------------------

class TestEnsureRates:
    def test_returns_count_of_new_fetches(self, fetcher: RateFetcher) -> None:
        df = _make_df([
            {"date": "2024-02-01", "currency": "GBP", "amount": -10.0},
            {"date": "2024-02-05", "currency": "GBP", "amount": -5.0},
        ])
        with patch("requests.get", return_value=_nbp_ok(5.03)) as mock_get:
            count = fetcher.ensure_rates(df)
        assert count == 2
        assert mock_get.call_count == 2

    def test_already_cached_rate_not_re_fetched(self, fetcher: RateFetcher) -> None:
        df = _make_df([{"date": "2024-02-01", "currency": "GBP", "amount": -10.0}])
        with patch("requests.get", return_value=_nbp_ok(5.03)):
            fetcher.ensure_rates(df)

        with patch("requests.get") as mock_get:
            count = fetcher.ensure_rates(df)  # second call — nothing to fetch
        assert count == 0
        mock_get.assert_not_called()

    def test_base_currency_rows_are_ignored(self, fetcher: RateFetcher) -> None:
        df = _make_df([{"date": "2024-02-01", "currency": "PLN", "amount": -500.0}])
        with patch("requests.get") as mock_get:
            count = fetcher.ensure_rates(df, base_currency="PLN")
        assert count == 0
        mock_get.assert_not_called()

    def test_empty_df_returns_zero(self, fetcher: RateFetcher) -> None:
        assert fetcher.ensure_rates(pd.DataFrame()) == 0

    def test_multiple_currencies_fetched_independently(self, cache_path: str) -> None:
        f = RateFetcher(cache_path)
        df = _make_df([
            {"date": "2024-02-01", "currency": "GBP", "amount": -10.0},
            {"date": "2024-02-01", "currency": "EUR", "amount": -20.0},
        ])
        responses = [_nbp_ok(5.03), _nbp_ok(4.31)]
        with patch("requests.get", side_effect=responses):
            count = f.ensure_rates(df)
        assert count == 2
        assert f.get_rate("GBP", date(2024, 2, 1)) == pytest.approx(5.03)
        assert f.get_rate("EUR", date(2024, 2, 1)) == pytest.approx(4.31)

    def test_duplicate_dates_result_in_single_fetch(self, fetcher: RateFetcher) -> None:
        """Multiple rows with the same (currency, date) should only fetch once."""
        df = _make_df([
            {"date": "2024-02-01", "currency": "GBP", "amount": -10.0},
            {"date": "2024-02-01", "currency": "GBP", "amount": -5.0},  # same date
        ])
        with patch("requests.get", return_value=_nbp_ok(5.03)) as mock_get:
            count = fetcher.ensure_rates(df)
        assert count == 1
        assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
# Walkback for weekends / holidays
# ---------------------------------------------------------------------------

class TestWeekendWalkback:
    def test_404_walks_back_to_previous_day(self, fetcher: RateFetcher) -> None:
        """Saturday 2024-02-10 → 404, then Friday 2024-02-09 → 5.03."""
        responses = [_nbp_404(), _nbp_ok(5.03, "2024-02-09")]
        with patch("requests.get", side_effect=responses):
            result = fetcher._fetch_effective_rate("GBP", date(2024, 2, 10))
        assert result is not None
        effective_date, rate = result
        assert effective_date == date(2024, 2, 9)
        assert rate == pytest.approx(5.03)

    def test_transaction_date_cached_after_walkback(self, fetcher: RateFetcher) -> None:
        """The requested transaction date (not just the effective date) must be cached."""
        responses = [_nbp_404(), _nbp_ok(5.03, "2024-02-09")]
        txn_date = date(2024, 2, 10)
        with patch("requests.get", side_effect=responses):
            fetcher._fetch_effective_rate("GBP", txn_date)
            # manually store as ensure_rates would
            fetcher._cache.setdefault("GBP", {})["2024-02-10"] = 5.03

        assert fetcher.get_rate("GBP", txn_date) == pytest.approx(5.03)

    def test_all_seven_days_404_returns_none(self, fetcher: RateFetcher) -> None:
        """If no rate found in the entire lookback window, returns None gracefully."""
        with patch("requests.get", return_value=_nbp_404()):
            result = fetcher._fetch_effective_rate("GBP", date(2024, 2, 10))
        assert result is None

    def test_walkback_via_ensure_rates_full_run(self, fetcher: RateFetcher) -> None:
        """ensure_rates should still cache and count a rate found via walkback."""
        df = _make_df([{"date": "2024-02-10", "currency": "GBP", "amount": -12.0}])
        responses = [_nbp_404(), _nbp_ok(5.03, "2024-02-09")]
        with patch("requests.get", side_effect=responses):
            count = fetcher.ensure_rates(df)
        assert count == 1
        # Rate must be retrievable under the original transaction date
        assert fetcher.get_rate("GBP", date(2024, 2, 10)) == pytest.approx(5.03)


# ---------------------------------------------------------------------------
# Partial failure / network errors
# ---------------------------------------------------------------------------

class TestNetworkFailures:
    def test_network_error_is_skipped_gracefully(self, fetcher: RateFetcher) -> None:
        """A requests.RequestException on one date must not abort the whole run."""
        df = _make_df([
            {"date": "2024-02-01", "currency": "GBP", "amount": -10.0},
            {"date": "2024-02-05", "currency": "GBP", "amount": -5.0},
        ])
        # First call raises, second succeeds
        responses = [
            requests.ConnectionError("timeout"),
            _nbp_ok(5.03, "2024-02-05"),
        ]
        with patch("requests.get", side_effect=responses):
            count = fetcher.ensure_rates(df)

        # Only the successful fetch is counted
        assert count == 1
        assert fetcher.get_rate("GBP", date(2024, 2, 1)) is None
        assert fetcher.get_rate("GBP", date(2024, 2, 5)) == pytest.approx(5.03)

    def test_unsupported_currency_returns_zero_fetches(self, fetcher: RateFetcher) -> None:
        """A currency not in NBP Table A (all 8 days return 404) counts as 0 fetches."""
        df = _make_df([{"date": "2024-02-01", "currency": "XYZ", "amount": -10.0}])
        with patch("requests.get", return_value=_nbp_404()):
            count = fetcher.ensure_rates(df)
        assert count == 0


# ---------------------------------------------------------------------------
# as_history_df
# ---------------------------------------------------------------------------

class TestAsHistoryDf:
    def test_empty_cache_returns_empty_df(self, fetcher: RateFetcher) -> None:
        df = fetcher.as_history_df()
        assert list(df.columns) == ["currency", "rate_date", "rate"]
        assert len(df) == 0

    def test_columns_and_dtypes(self, fetcher: RateFetcher) -> None:
        with patch("requests.get", return_value=_nbp_ok(5.03)):
            fetcher.ensure_rates(
                _make_df([{"date": "2024-02-01", "currency": "GBP", "amount": -10.0}])
            )
        hist = fetcher.as_history_df()
        assert list(hist.columns) == ["currency", "rate_date", "rate"]
        assert pd.api.types.is_datetime64_any_dtype(hist["rate_date"])
        assert hist["rate"].dtype == float

    def test_sorted_by_currency_then_date(self, fetcher: RateFetcher) -> None:
        df_in = _make_df([
            {"date": "2024-02-10", "currency": "GBP", "amount": -10.0},
            {"date": "2024-02-01", "currency": "GBP", "amount": -5.0},
            {"date": "2024-02-01", "currency": "EUR", "amount": -20.0},
        ])
        with patch("requests.get", return_value=_nbp_ok(5.0)):
            fetcher.ensure_rates(df_in)
        hist = fetcher.as_history_df()
        currencies = hist["currency"].tolist()
        assert currencies == sorted(currencies)
        gbp_dates = hist.loc[hist["currency"] == "GBP", "rate_date"].tolist()
        assert gbp_dates == sorted(gbp_dates)


# ---------------------------------------------------------------------------
# _merge_histories
# ---------------------------------------------------------------------------

class TestMergeHistories:
    def _make_history(self, rows: list[dict]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["currency", "rate_date", "rate"])
        df = pd.DataFrame(rows)
        df["rate_date"] = pd.to_datetime(df["rate_date"])
        return df

    def test_fetched_currencies_replace_db_entirely(self) -> None:
        """If GBP is in fetched, the corrupted DB row for GBP must be dropped."""
        fetched = self._make_history([
            {"currency": "GBP", "rate_date": "2024-02-01", "rate": 5.03},
        ])
        db = self._make_history([
            {"currency": "GBP", "rate_date": "2018-12-02", "rate": 483_527_067.0},  # corrupted
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.28},
        ])
        merged = _merge_histories(fetched, db)
        gbp_rows = merged[merged["currency"] == "GBP"]
        assert len(gbp_rows) == 1
        assert gbp_rows["rate"].iloc[0] == pytest.approx(5.03)

    def test_db_rate_kept_for_uncovered_currency(self) -> None:
        fetched = self._make_history([
            {"currency": "GBP", "rate_date": "2024-02-01", "rate": 5.03},
        ])
        db = self._make_history([
            {"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.28},
        ])
        merged = _merge_histories(fetched, db)
        eur_rows = merged[merged["currency"] == "EUR"]
        assert len(eur_rows) == 1
        assert eur_rows["rate"].iloc[0] == pytest.approx(4.28)

    def test_empty_fetched_returns_db(self) -> None:
        db = self._make_history([{"currency": "EUR", "rate_date": "2024-01-01", "rate": 4.28}])
        merged = _merge_histories(pd.DataFrame(columns=["currency", "rate_date", "rate"]), db)
        assert len(merged) == 1

    def test_empty_db_returns_fetched(self) -> None:
        fetched = self._make_history([{"currency": "GBP", "rate_date": "2024-02-01", "rate": 5.03}])
        merged = _merge_histories(fetched, pd.DataFrame(columns=["currency", "rate_date", "rate"]))
        assert len(merged) == 1


# ---------------------------------------------------------------------------
# Integration: apply_pln_conversion uses fetched rates
# ---------------------------------------------------------------------------

class TestApplyConversionWithFetcher:
    def test_corrupted_db_rate_overridden_by_fetched(self, tmp_path: Path) -> None:
        """The 483M GBP bug: fetched ~5.03 must be used, not the bad DB value."""
        import sqlite3

        # Minimal .mmb file with corrupted GBP rate
        mmb = str(tmp_path / "test.mmb")
        conn = sqlite3.connect(mmb)
        conn.executescript("""
            CREATE TABLE CURRENCYFORMATS_V1 (
                CURRENCYID INTEGER PRIMARY KEY, CURRENCY_SYMBOL TEXT, BASECONVRATE REAL
            );
            CREATE TABLE CURRENCYHISTORY_V1 (
                CURRENCYID INTEGER, CURRDATE TEXT, CURRVALUE REAL
            );
            CREATE TABLE INFOTABLE_V1 (INFONAME TEXT, INFOVALUE TEXT);
            INSERT INTO CURRENCYFORMATS_V1 VALUES (1, 'PLN', 1.0);
            INSERT INTO CURRENCYFORMATS_V1 VALUES (3, 'GBP', 1.0);
            INSERT INTO CURRENCYHISTORY_V1 VALUES (3, '2018-12-02', 483527067.0);
            INSERT INTO INFOTABLE_V1 VALUES ('BASECURRENCYID', '1');
        """)
        conn.commit()
        conn.close()

        df = _make_df([{"date": "2024-02-24", "currency": "GBP", "amount": -24.44}])
        cache_file = str(tmp_path / "rate_cache.json")

        with patch("requests.get", return_value=_nbp_ok(5.03, "2024-02-24")):
            result = apply_pln_conversion(df, mmb, rate_cache=cache_file)

        # Expected: -24.44 * 5.03 ≈ -122.93, not -24.44 * 483527067
        npt.assert_almost_equal(result["amount_pln"].iloc[0], -24.44 * 5.03, decimal=2)

    def test_legacy_no_cache_uses_db_rate(self, tmp_path: Path) -> None:
        """Passing no rate_cache must still produce correct DB-based conversion."""
        import sqlite3

        mmb = str(tmp_path / "test.mmb")
        conn = sqlite3.connect(mmb)
        conn.executescript("""
            CREATE TABLE CURRENCYFORMATS_V1 (
                CURRENCYID INTEGER PRIMARY KEY, CURRENCY_SYMBOL TEXT, BASECONVRATE REAL
            );
            CREATE TABLE CURRENCYHISTORY_V1 (
                CURRENCYID INTEGER, CURRDATE TEXT, CURRVALUE REAL
            );
            CREATE TABLE INFOTABLE_V1 (INFONAME TEXT, INFOVALUE TEXT);
            INSERT INTO CURRENCYFORMATS_V1 VALUES (1, 'PLN', 1.0);
            INSERT INTO CURRENCYFORMATS_V1 VALUES (3, 'GBP', 5.0);
            INSERT INTO CURRENCYHISTORY_V1 VALUES (3, '2024-01-01', 5.0);
            INSERT INTO INFOTABLE_V1 VALUES ('BASECURRENCYID', '1');
        """)
        conn.commit()
        conn.close()

        df = _make_df([{"date": "2024-02-01", "currency": "GBP", "amount": -10.0}])
        result = apply_pln_conversion(df, mmb)  # no rate_cache
        npt.assert_almost_equal(result["amount_pln"].iloc[0], -50.0, decimal=4)
