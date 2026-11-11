"""
End-to-end pipeline runner: parse → validate → feature-engineer → analyse → save.

Every run rebuilds every output: data/interim/, data/processed/ and outputs/
are emptied first (outputs/logs/ is kept), then all steps run, forecasts
included. Nothing is reused from an earlier run.

Usage:
    python scripts/run_pipeline.py                       # MMB_PATH from .env
    python scripts/run_pipeline.py --mmb path/to/file.mmb
    python scripts/run_pipeline.py --csv path/to/file.csv

NBP exchange rates are fetched and cached in .cache/nbp_rates.json (override
with --rate-cache or disable with --no-rate-cache). The cache holds public
reference rates, not results, so it is not cleared between runs.
"""

import argparse
import io
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

# Ensure the project root is on sys.path so `src` is importable
sys.path.insert(0, str(Path(__file__).parents[1]))

from src.analysis.budget_health import (
    build_account_balances,
    build_budget_health_monthly,
    build_reserve_transfers,
)
from src.analysis.cashflow_forecast import select_and_run_forecast
from src.analysis.category_prophet import compute_category_forecasts, compute_category_trends
from src.analysis.expense_trends import compute_expense_trends
from src.analysis.feature_engineering import (
    build_monthly_aggregates,
    build_monthly_cashflow_split,
    complete_cashflow_series,
    engineer_features,
)
from src.analysis.forecast_backtest import (
    MIN_ORIGINS_FOR_BANDS,
    empirical_bands,
    rolling_origin_backtest,
    summarise_backtest,
)
from src.analysis.recurring_commitments import (
    build_recurring_category_summary,
    build_recurring_commitments,
)
from src.ingestion.mmex_csv_parser import parse_mmex_csv
from src.ingestion.mmex_sqlite_parser import (
    DEFAULT_RATE_CACHE,
    get_account_metadata,
    parse_mmex_sqlite,
)
from src.ingestion.validator import validate_transactions
from src.storage.local_writer import LocalWriter
from src.utils.config_loader import load_config
from src.utils.logging_config import setup_logging

logger = setup_logging("run_pipeline")

PROJECT_ROOT = Path(__file__).parents[1]


def _utf8_console() -> None:
    """
    Make stdout/stderr survive characters the console codepage cannot encode.

    A default Windows console uses cp1250 (or another legacy codepage), where
    characters such as an arrow raise UnicodeEncodeError and abort the run.
    Reconfiguring to UTF-8 with ``errors="replace"`` keeps every log line
    printable; on an already-UTF-8 stream it is a no-op.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError, io.UnsupportedOperation):
            pass


def main() -> None:
    _utf8_console()
    load_dotenv(PROJECT_ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="MMEX pipeline: rebuild every output from the source file"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--mmb",
        type=str,
        default="",
        help="Path to MMEX .mmb SQLite file. Defaults to MMB_PATH from .env.",
    )
    group.add_argument("--csv", type=str, help="Path to MMEX CSV export")
    parser.add_argument(
        "--rate-cache",
        type=str,
        default=DEFAULT_RATE_CACHE,
        metavar="PATH",
        help=(
            "Path to the local JSON rate cache for NBP exchange rates. "
            "Defaults to .cache/nbp_rates.json. "
            "Live rates are fetched from api.nbp.pl for any uncached (currency, date) pair."
        ),
    )
    parser.add_argument(
        "--no-rate-cache",
        action="store_true",
        default=False,
        help="Disable NBP rate fetching and use only the rates stored in the .mmb file.",
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Run date. Rows dated after it are treated as future, not actuals, and "
            "the month containing it is flagged partial. Defaults to today; pass an "
            "explicit date for reproducible runs."
        ),
    )
    args = parser.parse_args()
    as_of = args.as_of or date.today()
    rate_cache = None if args.no_rate_cache else args.rate_cache

    mmb_path = None
    if not args.csv:
        mmb_path = (args.mmb or os.getenv("MMB_PATH", "")).strip()
        if not mmb_path:
            parser.error("pass --mmb or --csv, or set MMB_PATH in .env")

    cfg = load_config()
    writer = LocalWriter(project_root=PROJECT_ROOT)

    # ── Step 0: Clear every output so nothing from an earlier run survives ────
    writer.reset_outputs()

    # ── Step 1: Parse ─────────────────────────────────────────────────────────
    if mmb_path:
        df = parse_mmex_sqlite(mmb_path, rate_cache=rate_cache)
    else:
        df = parse_mmex_csv(args.csv)

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    report = validate_transactions(df)
    report.display()

    if not report.is_passing():
        logger.error("Validation failed. Pipeline halted.")
        sys.exit(1)

    # ── Step 3: Save validated transactions ───────────────────────────────────
    writer.save_interim(df, "transactions_validated")

    # ── Step 4: Feature engineering ───────────────────────────────────────────
    df_featured = engineer_features(df, as_of=as_of)
    future_rows = df_featured[df_featured["is_future"]]
    if not future_rows.empty:
        logger.info(
            f"Future-dated rows: {len(future_rows)} | "
            f"latest={future_rows['date'].max().date()} | as_of={as_of}"
        )
    else:
        logger.info(f"Future-dated rows: 0 | as_of={as_of}")
    writer.save_interim(df_featured, "transactions_featured")

    # ── Step 5: Monthly aggregates ────────────────────────────────────────────
    monthly = build_monthly_aggregates(df_featured, as_of=as_of)
    writer.save_interim(monthly, "monthly_aggregates")

    # ── Step 6: Monthly cashflow split ────────────────────────────────────────
    exclude_cats = cfg.get("forecast", {}).get("exclude_categories") or []
    income_cats = cfg.get("cashflow", {}).get("income_categories") or ["Income"]
    cashflow_split = build_monthly_cashflow_split(
        df_featured,
        exclude_categories=exclude_cats,
        income_categories=income_cats,
        as_of=as_of,
    )
    writer.save_interim(cashflow_split, "monthly_cashflow_split")

    # ── Step 7: Expense trends ────────────────────────────────────────────────
    expense_trends_df = compute_expense_trends(monthly)
    writer.save_processed(expense_trends_df, "expense_trends")
    # ── Step 7b: Recurring commitments ────────────────────────────────────────
    recurring_cfg = cfg.get("recurring", {}) or {}
    commitments, commitment_events = build_recurring_commitments(
        df_featured, config=recurring_cfg
    )
    writer.save_processed(commitments, "recurring_commitments")
    writer.save_processed(commitment_events, "recurring_commitment_events")
    category_summary = build_recurring_category_summary(
        commitments, df_featured, config=recurring_cfg
    )
    writer.save_processed(category_summary, "recurring_categories")

    # ── Step 7c: Budget health (Conscious Spending Plan) ──────────────────────
    budget_cfg = cfg.get("budget_health", {}) or {}
    accounts = get_account_metadata(mmb_path) if mmb_path else None
    if accounts is not None:
        balances = build_account_balances(df_featured, accounts, as_of=as_of, config=budget_cfg)
        writer.save_processed(balances, "account_balances")
        reserve_transfers = build_reserve_transfers(
            df_featured, accounts, as_of=as_of, config=budget_cfg
        )
        writer.save_processed(reserve_transfers, "reserve_transfers")
    budget_monthly = build_budget_health_monthly(
        df_featured, accounts, as_of=as_of, config=budget_cfg
    )
    writer.save_processed(budget_monthly, "budget_health_monthly")

    # ── Step 8: Cashflow forecast ─────────────────────────────────────────────
    # Train on complete months only: the current month is still accruing.
    partial = cashflow_split[cashflow_split["is_partial_month"].astype(bool)]
    if not partial.empty:
        logger.info(
            "Forecast series excludes partial month(s): "
            f"{', '.join(partial['year_month'])} (as_of={as_of}); "
            "they are still reported as actuals in monthly_cashflow_split."
        )
    cashflow_series = complete_cashflow_series(cashflow_split)
    forecast_df = select_and_run_forecast(cashflow_series, periods=24)

    # ── Step 8b: Rolling-origin backtest and empirical bands ──────────────────
    if len(cashflow_series) < 36 + 24:
        logger.info(
            "Forecast backtest skipped: %d complete months < 60 (36 initial + "
            "24 horizon). Keeping model intervals (interval_method=model).",
            len(cashflow_series),
        )
    else:
        # The shipped forecaster IS the trailing-average baseline, so replaying
        # a separate naive arm would just duplicate it. The backtest exists to
        # estimate the bands and report the error, nothing else.
        backtest = rolling_origin_backtest(cashflow_series, select_and_run_forecast)
        backtest = backtest.assign(forecaster="shipped")
        writer.save_processed(backtest, "forecast_backtest")

        summary = summarise_backtest(backtest)
        logger.info(
            "Backtest (%d origins): MAE/month %.0f | 12-mo |cum err| %.0f | "
            "model-band coverage %.2f | holdout cumulative 80%% band %d/%d",
            summary["n_origins"],
            summary["mae_month"],
            summary["abs_cum_err_12"],
            summary["model_ci80_coverage_month"],
            summary["holdout_cum80_k"],
            summary["holdout_cum80_m"],
        )

        n_origins = int(backtest["origin_month"].nunique())
        if n_origins >= MIN_ORIGINS_FOR_BANDS:
            forecast_df = empirical_bands(backtest, forecast_df)
    writer.save_processed(forecast_df, "cashflow_forecast")

    # ── Step 9: Category trends ───────────────────────────────────────────────
    # ── Step 10: Category forecasts ───────────────────────────────────────────
    try:
        import prophet  # noqa: F401  (optional [forecast] extra)
    except ImportError:
        logger.warning(
            "Prophet not installed — skipping category trends & forecasts. "
            "Install with: poetry install --with forecast"
        )
    else:
        cat_trends_summary, cat_trends_series = compute_category_trends(df_featured, as_of=as_of)
        writer.save_processed(cat_trends_summary, "category_trends_summary")
        writer.save_processed(cat_trends_series, "category_trends_series")

        cat_forecasts = compute_category_forecasts(df_featured, as_of=as_of)
        writer.save_processed(cat_forecasts, "category_forecasts")

    print("\n" + "=" * 60)
    print("Pipeline complete. Outputs written to data/interim/:")
    print("  transactions_validated.parquet")
    print("  transactions_featured.parquet")
    print("  monthly_aggregates.parquet")
    print("  monthly_cashflow_split.parquet")
    print("\nOutputs written to data/processed/:")
    print("  expense_trends.parquet")
    print("  recurring_commitments.parquet")
    print("  recurring_commitment_events.parquet")
    print("  recurring_categories.parquet")
    print("  budget_health_monthly.parquet")
    if accounts is not None:
        print("  account_balances.parquet")
        print("  reserve_transfers.parquet")
    print("  cashflow_forecast.parquet (24-month horizon)")
    print("  forecast_backtest.parquet")
    print("  category_trends_summary.parquet")
    print("  category_trends_series.parquet")
    print("  category_forecasts.parquet (36-month horizon)")
    print("\nNext steps:")
    print("  just dashboard    -> rebuild and open the dashboard at http://localhost:8050")
    print("=" * 60)


if __name__ == "__main__":
    main()
