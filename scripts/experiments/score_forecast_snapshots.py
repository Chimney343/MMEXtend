"""
Score the saved monthly forecast snapshots against realised actuals.

Reads the private snapshot folders written by the pipeline and reports realised
monthly and cumulative coverage plus the error against the plain 12-month
average. Output goes to ``private/forecast_snapshots/`` only.

Usage:
    poetry run python scripts/experiments/score_forecast_snapshots.py --kind category
    poetry run python scripts/experiments/score_forecast_snapshots.py --kind savings
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.analysis.category_prophet import build_category_monthly_series  # noqa: E402
from src.analysis.feature_engineering import complete_cashflow_series  # noqa: E402
from src.analysis.forecast_tracking import (  # noqa: E402
    score_category_snapshots,
    score_savings_snapshots,
)


def _featured_path() -> Path:
    return Path("data/interim/transactions_featured.parquet")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=("category", "savings", "both"), default="both")
    args = parser.parse_args(argv)

    output = {}
    if args.kind in ("category", "both"):
        featured = pd.read_parquet(_featured_path())
        monthly = build_category_monthly_series(featured, as_of=None)
        output["category"] = score_category_snapshots(monthly)
    if args.kind in ("savings", "both"):
        split = pd.read_parquet("data/interim/monthly_cashflow_split.parquet")
        series = complete_cashflow_series(split)
        series.index = pd.DatetimeIndex(series.index).to_period("M").to_timestamp()
        output["savings"] = score_savings_snapshots(series)

    print(json.dumps(output, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
