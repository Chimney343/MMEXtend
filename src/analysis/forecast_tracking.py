"""
Prospective forecast tracking: monthly snapshots and realised scoring.

Unseen months are the only clean evidence left once a test window has been
used to choose a method. Each pipeline run saves what it forecast, and this
module scores those snapshots against what actually happened as months
complete.

Everything is written under ``private/forecast_snapshots/`` and never enters a
tracked file.

Usage:
    from src.analysis.forecast_tracking import save_snapshot, score_snapshots
    save_snapshot("category", forecast_frame, as_of=as_of)
    report = score_snapshots(category_history=monthly_series_frame)
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

SNAPSHOT_ROOT = Path("private/forecast_snapshots")
MIN_COMPARABLE_MONTHS = 3


def _snapshot_dir(kind: str, as_of: pd.Timestamp) -> Path:
    return SNAPSHOT_ROOT / kind / f"as_of_{pd.Timestamp(as_of).strftime('%Y-%m')}"


def save_snapshot(
    kind: str,
    forecast: pd.DataFrame,
    as_of,
    root: Path | None = None,
) -> Path:
    """
    Save one run's forecast, keyed by the as-of month.

    Idempotent: a second run in the same month overwrites that month's
    snapshot, so a re-run does not create duplicates. Returns the directory.
    """
    if kind not in ("category", "savings"):
        raise ValueError(f"unknown snapshot kind: {kind}")
    as_of_ts = pd.Timestamp(as_of)
    target = (_snapshot_dir(kind, as_of_ts) if root is None else Path(root) / kind / f"as_of_{as_of_ts.strftime('%Y-%m')}")
    target.mkdir(parents=True, exist_ok=True)
    if forecast is None or forecast.empty:
        (target / "empty.json").write_text(
            json.dumps({"as_of": str(as_of_ts.date()), "rows": 0}), encoding="utf-8"
        )
        return target

    frame = forecast.copy()
    frame.to_parquet(target / "forecast.parquet", index=False)
    (target / "meta.json").write_text(
        json.dumps(
            {
                "as_of": str(as_of_ts.date()),
                "rows": int(len(frame)),
                "first_month": str(pd.to_datetime(frame["month"]).min().date()),
                "last_month": str(pd.to_datetime(frame["month"]).max().date()),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return target


def _load_snapshots(kind: str, root: Path | None = None) -> list[pd.DataFrame]:
    base = (SNAPSHOT_ROOT if root is None else Path(root)) / kind
    if not base.exists():
        return []
    frames = []
    for folder in sorted(base.iterdir()):
        path = folder / "forecast.parquet"
        if not path.exists():
            continue
        frame = pd.read_parquet(path)
        meta = folder / "meta.json"
        as_of = (
            json.loads(meta.read_text(encoding="utf-8"))["as_of"]
            if meta.exists()
            else folder.name.replace("as_of_", "") + "-01"
        )
        frame["snapshot_as_of"] = pd.Timestamp(as_of)
        frames.append(frame)
    return frames


def score_snapshots(
    actual: pd.DataFrame,
    kind: str = "category",
    root: Path | None = None,
    min_months: int = MIN_COMPARABLE_MONTHS,
) -> dict:
    """
    Score saved snapshots against realised monthly actuals.

    ``actual`` must have the key columns and ``month`` / ``y`` columns; the key
    columns are taken from the snapshot frame, so the caller only has to supply
    the same granularity. Only months with a realised actual are scored.

    Returns per-horizon realised-month coverage, cumulative coverage at the
    longest realised horizon, and the forecast error against the plain
    12-month average.
    """
    snapshots = _load_snapshots(kind, root)
    if not snapshots:
        return {"snapshots": 0, "scored": 0, "message": "no snapshots yet"}

    realised = actual.copy()
    realised["month"] = pd.to_datetime(realised["month"])
    key_columns = [
        column
        for column in ("flow_type", "level", "category", "subcategory")
        if column in realised.columns and column in snapshots[0].columns
    ]

    rows = []
    for snapshot in snapshots:
        snapshot = snapshot.copy()
        snapshot["month"] = pd.to_datetime(snapshot["month"])
        merged = snapshot.merge(
            realised[key_columns + ["month", "y"]],
            on=key_columns + ["month"],
            how="inner",
        )
        if merged.empty:
            continue
        merged["cum_point"] = merged.groupby(key_columns + ["snapshot_as_of"])[
            "point_estimate"
        ].cumsum()
        merged["cum_actual"] = merged.groupby(key_columns + ["snapshot_as_of"])["y"].cumsum()
        merged["covered"] = (merged["y"] >= merged["ci80_lower"]) & (
            merged["y"] <= merged["ci80_upper"]
        )
        merged["cum_covered"] = (merged["cum_actual"] >= merged["cum_ci80_lower"]) & (
            merged["cum_actual"] <= merged["cum_ci80_upper"]
        )
        last = merged.sort_values("months_ahead").groupby(key_columns + ["snapshot_as_of"]).tail(1)
        rows.append(
            {
                "snapshot_as_of": merged["snapshot_as_of"].iloc[0],
                "months_realised": int(merged["months_ahead"].nunique()),
                "monthly_coverage": float(merged["covered"].mean()),
                "cum_coverage": float(last["cum_covered"].mean()),
                "forecast_total": float(last["cum_point"].sum()),
                "actual_total": float(last["cum_actual"].sum()),
                "cum_error": float(last["cum_actual"].sum() - last["cum_point"].sum()),
            }
        )

    if not rows:
        return {"snapshots": len(snapshots), "scored": 0, "message": "no realised months yet"}

    frame = pd.DataFrame(rows).sort_values("snapshot_as_of")
    frame["error_vs_actual"] = frame["cum_error"] / frame["actual_total"].replace(0, np.nan)
    summary = {
        "snapshots": len(snapshots),
        "scored": int(len(frame)),
        "monthly_coverage_mean": float(frame["monthly_coverage"].mean()),
        "cum_coverage_mean": float(frame["cum_coverage"].mean()),
        "mean_signed_error_ratio": float(frame["error_vs_actual"].mean()),
        "per_snapshot": frame.to_dict(orient="records"),
    }
    target = (SNAPSHOT_ROOT if root is None else Path(root)) / f"{kind}_scoring.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary


def score_category_snapshots(monthly_frame: pd.DataFrame, root: Path | None = None) -> dict:
    """Convenience wrapper for the category forecasts."""
    return score_snapshots(monthly_frame, kind="category", root=root)


def score_savings_snapshots(series: pd.Series, root: Path | None = None) -> dict:
    """Convenience wrapper for the single savings series."""
    frame = pd.DataFrame(
        {"month": pd.to_datetime(series.index), "y": series.to_numpy(dtype=float)}
    )
    return score_snapshots(frame, kind="savings", root=root)
