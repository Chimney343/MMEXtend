"""
End-to-end pipeline runner: parse → validate → feature-engineer → save.

Usage:
    python scripts/run_pipeline.py --mmb path/to/file.mmb
    python scripts/run_pipeline.py --csv path/to/file.csv

NBP exchange rates are fetched and cached automatically in
  data/interim/rate_cache.json (override with --rate-cache or disable with
  --no-rate-cache).
"""

import argparse
import sys
from pathlib import Path

# Ensure the project root is on sys.path so `src` is importable
sys.path.insert(0, str(Path(__file__).parents[1]))

from src.analysis.feature_engineering import build_monthly_aggregates, engineer_features
from src.ingestion.mmex_csv_parser import parse_mmex_csv
from src.ingestion.mmex_sqlite_parser import parse_mmex_sqlite
from src.ingestion.validator import validate_transactions
from src.storage.local_writer import LocalWriter
from src.utils.logging_config import setup_logging

logger = setup_logging("run_pipeline")

PROJECT_ROOT = Path(__file__).parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MMEX pipeline: ingest → validate → feature-engineer → save to interim"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--mmb", type=str, help="Path to MMEX .mmb SQLite file")
    group.add_argument("--csv", type=str, help="Path to MMEX CSV export")
    parser.add_argument(
        "--rate-cache",
        type=str,
        default=str(PROJECT_ROOT / "data" / "interim" / "rate_cache.json"),
        metavar="PATH",
        help=(
            "Path to the local JSON rate cache for NBP exchange rates. "
            "Defaults to data/interim/rate_cache.json. "
            "Live rates are fetched from api.nbp.pl for any uncached (currency, date) pair."
        ),
    )
    parser.add_argument(
        "--no-rate-cache",
        action="store_true",
        default=False,
        help="Disable NBP rate fetching and use only the rates stored in the .mmb file.",
    )
    args = parser.parse_args()
    rate_cache = None if args.no_rate_cache else args.rate_cache

    # ── Step 1: Parse ─────────────────────────────────────────────────────────
    if args.mmb:
        df = parse_mmex_sqlite(args.mmb, rate_cache=rate_cache)
    else:
        df = parse_mmex_csv(args.csv)

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    report = validate_transactions(df)
    report.display()

    if not report.is_passing():
        logger.error("Validation failed. Pipeline halted.")
        sys.exit(1)

    writer = LocalWriter(project_root=PROJECT_ROOT)

    # ── Step 3: Save validated transactions ───────────────────────────────────
    writer.save_interim(df, "transactions_validated")

    # ── Step 4: Feature engineering ───────────────────────────────────────────
    df_featured = engineer_features(df)
    writer.save_interim(df_featured, "transactions_featured")

    # ── Step 5: Monthly aggregates ────────────────────────────────────────────
    monthly = build_monthly_aggregates(df_featured)
    writer.save_interim(monthly, "monthly_aggregates")

    print("\n" + "=" * 60)
    print("Pipeline complete. Outputs written to data/interim/:")
    print("  transactions_validated.parquet")
    print("  transactions_featured.parquet")
    print("  monthly_aggregates.parquet")
    print("\nNext steps:")
    print("  just lab   -> open JupyterLab")
    print("  Open notebooks/00_data_quality.ipynb  to review validation")
    print("  Open notebooks/01_wrangle.ipynb       to explore features")
    print("=" * 60)


if __name__ == "__main__":
    main()
