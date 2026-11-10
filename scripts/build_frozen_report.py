"""
Build the frozen-money .xlsx report.

By default re-parses the MMEX .mmb file (configured via MMB_PATH in .env) to
always pick up the latest account names. Falls back to the cached parquet if
--input is passed explicitly.

Usage:
    poetry run python scripts/build_frozen_report.py
    poetry run python scripts/build_frozen_report.py --input data/interim/transactions_validated.parquet
    poetry run python scripts/build_frozen_report.py --output-dir custom_outputs
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parents[1]))

import pandas as pd

from src.analysis.frozen_report import build_frozen_report
from src.ingestion.mmex_sqlite_parser import parse_mmex_sqlite
from src.storage.local_writer import LocalWriter
from src.utils.logging_config import setup_logging

logger = setup_logging("build_frozen_report")

PROJECT_ROOT = Path(__file__).parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "interim" / "transactions_validated.parquet"
DEFAULT_OUTPUT_DIR = "outputs"


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="Build the frozen-money xlsx report from the validated pipeline output."
    )
    parser.add_argument(
        "--mmb",
        type=str,
        default="",
        metavar="PATH",
        help="MMEX .mmb file to parse (default: MMB_PATH from .env)",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to transactions_validated.parquet instead of parsing a .mmb file",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Output directory for the .xlsx file (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            logger.error(
                f"Input file not found: {input_path.resolve()}\n"
                "Run `just run` first to produce the validated pipeline output."
            )
            sys.exit(1)
        logger.info(f"Reading validated transactions from {input_path}")
        df = pd.read_parquet(input_path)
    else:
        mmb_path = (args.mmb or os.getenv("MMB_PATH", "")).strip()
        if not mmb_path:
            logger.error(
                "No --mmb, no --input and MMB_PATH not set in .env.\n"
                "Set MMB_PATH in .env to your .mmb file path."
            )
            sys.exit(1)
        mmb_file = Path(mmb_path)
        if not mmb_file.exists():
            logger.error(f"MMB file not found: {mmb_file.resolve()}")
            sys.exit(1)
        logger.info(f"Parsing fresh data from {mmb_file}")
        df = parse_mmex_sqlite(str(mmb_file), rate_cache=None)

    logger.info(f"Loaded {len(df)} transaction rows")

    sheets = build_frozen_report(df)

    if not sheets:
        print("No Frozen accounts found. Nothing to export.")
        return

    writer = LocalWriter(project_root=PROJECT_ROOT)
    writer.export_dir = PROJECT_ROOT / args.output_dir
    writer.export_xlsx(sheets, "frozen_report")

    print_summary(sheets)


def print_summary(sheets: dict[str, pd.DataFrame]) -> None:
    summary_df = sheets.get("Summary")
    if summary_df is None or summary_df.empty:
        return

    frozen_accounts = summary_df["frozen_account"].unique()
    print(f"\nFrozen accounts: {len(frozen_accounts)}")

    for account in frozen_accounts:
        account_summary = summary_df[summary_df["frozen_account"] == account]
        still_frozen = account_summary[account_summary["net"] > 0]
        total_still = still_frozen["net"].sum()
        print(
            f"  {account}: {len(still_frozen)} still-frozen process(es) "
            f"({total_still:.2f} {still_frozen['currency'].iloc[0] if len(still_frozen) > 0 else ''})"
        )


if __name__ == "__main__":
    main()
