"""
Production category forecasts from the winning bake-off arm (``class_selector``).

This is the shipped path, not the experiment's: it reuses the same arm code
(``src.analysis.category_forecasters`` and ``src.analysis.intermittent``) and
the same band functions (``src.analysis.empirical_band``) the backtest scored.

Method, frozen in ``configs/category_forecast_experiment.yaml``:

- classify each series on its **full history** with the SBC thresholds;
- apply the frozen class -> method map;
- fill the point forecast from that method, held flat over the horizon;
- fill ``ci80_*`` from the pooled scaled empirical band chosen by the band
  bake-off (``configs/empirical_band_experiment.yaml``);
- leave ``ci95_*`` null: the 95% band was never tested.

Every band scale is computed from history up to the forecast origin, and the
error pool only contains origins whose whole horizon finished before it. An
earlier version sized the pool with the outcomes it had to cover; that is
hindsight and is not used anywhere now.

Grid: each series runs from its first non-zero month to the last complete
month, with trailing zeros. Point and band values are clipped at 0 and
magnitudes below 0.01 are snapped to exactly 0.

Usage:
    from src.analysis.category_forecast_production import compute_category_forecast_rows
    frame, outcome = compute_category_forecast_rows(df_featured, as_of=None)
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.analysis.category_forecasters import ArmSkip, build_local_arm
from src.analysis.category_prophet import (
    FORECAST_PERIODS,
    MIN_MONTHS_FORECAST,
    build_category_monthly_series,
)
from src.analysis.empirical_band import (
    MIN_PRIOR_ERRORS,
    apply_offsets,
    band_from_pool,
    origin_scale,
    pool_with_size_bucket,
    size_bucket,
    size_tercile_cutpoints,
)
from src.analysis.intermittent import (
    SBC_CLASSES,
    build_panel,
    panel_to_series,
    parse_series_key,
    sbc_class,
)
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

# Frozen at config sha256 recorded in ``configs/category_forecast_experiment.yaml``.
# Do not edit without re-running the bake-off: changing this map invalidates the
# backtest that chose it.
FROZEN_CLASS_MAP: dict[str, dict] = {
    "smooth": {"arm_id": "croston", "params": {"alpha": 0.3, "variant": "classic"}},
    "erratic": {"arm_id": "theta", "params": {"window": 60}},
    "intermittent": {"arm_id": "auto_ets", "params": {"window": 60}},
    "lumpy": {"arm_id": "naive_mean_w", "params": {"window": 12}},
}
FROZEN_DEFAULT = {"arm_id": "naive_mean_12", "params": {}}

# Band candidate chosen on dev by the band bake-off: K1 (scale =
# max(origin 12-month mean, all-history mean), pooled by SBC class). The scale
# is known at the forecast origin and the same definition builds the pool and
# applies the band.
FROZEN_BAND_SCALE: str | None = "max_history"
FROZEN_BAND_POOL: str | None = "class"

# Values below this are snapped to exactly 0.
SNAP = 0.01


def _method_label(label: str, spec: dict) -> str:
    """Readable ``method`` value naming the class and the method used."""
    arm_id = str(spec.get("arm_id", "unknown"))
    params = spec.get("params", {}) or {}
    detail = ""
    if arm_id == "croston":
        detail = f" a={params.get('alpha')} {params.get('variant', 'classic')}"
    elif arm_id in ("auto_ets", "theta"):
        window = params.get("window")
        detail = f" w={window}" if window else ""
    elif arm_id == "naive_mean_w":
        detail = f" w={params.get('window')}"
    return f"class_selector: {label} -> {arm_id}{detail}"


@dataclass
class ForecastOutcome:
    """Per-series forecast plus the diagnostics the caller may log."""

    rows: pd.DataFrame
    classified: dict[str, str] = field(default_factory=dict)
    skipped_short: list[str] = field(default_factory=list)
    band_missing: list[str] = field(default_factory=list)


def _forecast_columns() -> list[str]:
    return [
        "flow_type",
        "level",
        "category",
        "subcategory",
        "months_ahead",
        "month",
        "point_estimate",
        "ci80_lower",
        "ci80_upper",
        "ci95_lower",
        "ci95_upper",
        "method",
        "cum_point",
        "cum_ci80_lower",
        "cum_ci80_upper",
    ]


def build_production_panel(
    df_featured: pd.DataFrame, as_of=None
) -> tuple[pd.DataFrame, pd.DatetimeIndex]:
    """
    Corrected grid for production.

    Each series runs from its first non-zero month to the last complete month,
    with zeros filled in, so a series that went quiet keeps its trailing zeros
    instead of being forecast from a date in the past.
    """
    monthly = build_category_monthly_series(df_featured, as_of=as_of)
    if monthly.empty:
        return monthly, pd.DatetimeIndex([])
    last_complete = pd.to_datetime(monthly["month"]).max()
    panel = build_panel(monthly, last_complete=last_complete, include_unassigned=False)
    months = (
        pd.DatetimeIndex(sorted(panel["month"].unique()))
        if not panel.empty
        else pd.DatetimeIndex([])
    )
    return panel, months


def _forecast_one(y: pd.Series, periods: int, label: str, spec: dict) -> np.ndarray | None:
    """
    Point forecast from the frozen method for one series.

    Returns None only for the arm's explicit insufficient-history skip. Any
    other failure propagates: a silently substituted benchmark is what hid the
    reconciliation bug during the bake-off.
    """
    arm = build_local_arm(str(spec["arm_id"]), spec.get("params", {}))
    try:
        frame = arm(y, int(periods))
    except ArmSkip as exc:
        logger.info("Category forecast: %s skipped (%s)", label, exc)
        return None
    values = pd.to_numeric(frame["point_estimate"], errors="coerce").to_numpy(dtype=float)
    if len(values) < int(periods) or not np.isfinite(values).all():
        raise RuntimeError(
            f"category forecast for {label} is short or non-finite; "
            "an arm must not silently degrade"
        )
    return np.clip(values[: int(periods)], 0.0, None)


def _error_pool(
    series_map: dict[str, pd.Series],
    classes: dict[str, str],
    spec_by_class: dict[str, dict],
    months: pd.DatetimeIndex,
    periods: int,
    scale_mode: str = FROZEN_BAND_SCALE,
) -> pd.DataFrame:
    """
    Realised errors at past, fully complete origins only.

    An origin qualifies only when ``origin + periods`` is on or before the last
    complete month, so no pooled error can have observed any part of the
    forecast window. ``scale`` is the origin's own trailing scale, matching how
    the band is applied.
    """
    last_complete = months[-1]
    records: list[dict] = []
    for key, y in series_map.items():
        label = classes.get(key)
        spec = spec_by_class.get(label, FROZEN_DEFAULT)
        for origin in months[:-1]:
            if origin + pd.DateOffset(months=periods) > last_complete:
                break
            history = y[y.index <= origin]
            if len(history) < MIN_MONTHS_FORECAST:
                continue
            try:
                point = _forecast_one(history, periods, key, spec)
            except RuntimeError:
                continue
            if point is None:
                continue
            future = pd.date_range(
                start=origin + pd.DateOffset(months=1), periods=periods, freq="MS"
            )
            actual = y.reindex(future).to_numpy(dtype=float)
            if len(actual) < periods or not np.isfinite(actual).all():
                continue
            scale = origin_scale(history.to_numpy(dtype=float), mode=scale_mode)
            cumulative = 0.0
            for step in range(periods):
                error = float(actual[step] - point[step])
                cumulative += error
                records.append(
                    {
                        "series_key": key,
                        "sbc": label,
                        "origin_month": origin,
                        "h": step + 1,
                        "err": error,
                        "cum_err": cumulative,
                        "scale": scale,
                    }
                )
    return pd.DataFrame(
        records, columns=["series_key", "sbc", "origin_month", "h", "err", "cum_err", "scale"]
    )


def compute_category_forecast_rows(
    df_featured: pd.DataFrame,
    as_of=None,
    periods: int = FORECAST_PERIODS,
    class_map: dict[str, dict] | None = None,
    default: dict | None = None,
    band_scale: str = FROZEN_BAND_SCALE,
    band_pool: str = FROZEN_BAND_POOL,
) -> tuple[pd.DataFrame, ForecastOutcome]:
    """
    Production category forecasts: frozen selector, pooled band, 24 months.

    Parameters
    ----------
    df_featured : pd.DataFrame
        Output of ``engineer_features()``.
    as_of : date | pd.Timestamp | None
        Run date; the month containing it is excluded (partial).
    periods : int
        Forecast horizon. Defaults to ``FORECAST_PERIODS`` (24).
    class_map, default : dict | None
        Override the frozen map (tests only).
    band_scale, band_pool : str
        Override the frozen band candidate (tests only).
    """
    class_map = class_map if class_map is not None else FROZEN_CLASS_MAP
    default = default if default is not None else FROZEN_DEFAULT

    panel, months = build_production_panel(df_featured, as_of=as_of)
    columns = _forecast_columns()
    if panel.empty or len(months) == 0:
        return pd.DataFrame(columns=columns), ForecastOutcome(pd.DataFrame(columns=columns))

    series_map = panel_to_series(panel)
    origin = pd.Timestamp(months[-1])
    future_months = pd.date_range(
        start=origin + pd.DateOffset(months=1), periods=int(periods), freq="MS"
    )

    # Classify on the full history, as the backtest did at run time.
    classes = {key: sbc_class(y.to_numpy(dtype=float)) for key, y in series_map.items()}
    spec_by_class = {label: class_map.get(label, default) for label in SBC_CLASSES}

    # ``band_scale is None`` means no band candidate qualified on dev, so no
    # band ships: every ci80 value stays null and the chart shows the point
    # line alone. Silently substituting an uncalibrated band would be worse
    # than showing none.
    pool = pd.DataFrame()
    label_column = "sbc"
    if band_scale is not None:
        pool = _error_pool(
            series_map, classes, spec_by_class, months, int(periods), scale_mode=band_scale
        )
        if band_pool == "class_size_tercile" and not pool.empty:
            pool = pool_with_size_bucket(pool, origin, horizon=int(periods))
            pool["group_label"] = (
                pool["sbc"].astype(str) + "|" + pool["size_bucket"].astype(str)
            )
            label_column = "group_label"

    records: list[dict] = []
    skipped: list[str] = []
    band_missing: list[str] = []
    for key, y in series_map.items():
        if len(y) < MIN_MONTHS_FORECAST:
            skipped.append(key)
            continue
        label = classes[key]
        spec = spec_by_class.get(label, default)
        point = _forecast_one(y, int(periods), key, spec)
        if point is None:
            skipped.append(key)
            continue

        history = y[y.index <= origin].to_numpy(dtype=float)
        if band_scale is None:
            m_lo = m_hi = c_lo = c_hi = np.full(int(periods), np.nan)
        else:
            scale = origin_scale(history, mode=band_scale)
            if label_column == "group_label":
                cutpoints = size_tercile_cutpoints(pool, origin, horizon=int(periods))
                group_label = f"{label}|{size_bucket(scale, cutpoints)}"
            else:
                group_label = label
            m_lo, m_hi, c_lo, c_hi = band_from_pool(
                point,
                scale,
                pool,
                key=key,
                origin=origin,
                horizon=int(periods),
                label=group_label,
                label_column=label_column,
                include_peers=True,
                min_prior=MIN_PRIOR_ERRORS,
            )
        banded = apply_offsets(point, m_lo, m_hi, c_lo, c_hi, snap=SNAP)
        if not np.isfinite(m_lo).any():
            band_missing.append(key)

        meta = parse_series_key(key)
        method = _method_label(label, spec)
        for step in range(int(periods)):
            record = banded.iloc[step]
            records.append(
                {
                    "flow_type": meta["flow_type"],
                    "level": meta["level"],
                    "category": meta["category"],
                    "subcategory": meta["subcategory"],
                    "months_ahead": step + 1,
                    "month": future_months[step],
                    "point_estimate": float(record["point_estimate"]),
                    "ci80_lower": float(record["ci80_lower"]),
                    "ci80_upper": float(record["ci80_upper"]),
                    "ci95_lower": np.nan,
                    "ci95_upper": np.nan,
                    "method": method,
                    "cum_point": float(record["cum_point"]),
                    "cum_ci80_lower": float(record["cum_ci80_lower"]),
                    "cum_ci80_upper": float(record["cum_ci80_upper"]),
                }
            )

    frame = pd.DataFrame(records, columns=columns)
    series_count = (
        frame.groupby(["flow_type", "level", "category", "subcategory"], dropna=False).ngroups
        if not frame.empty
        else 0
    )
    logger.info(
        "Category forecasts: %d rows over %d series (%d skipped, %d without a band)",
        len(frame),
        series_count,
        len(skipped),
        len(band_missing),
    )
    outcome = ForecastOutcome(
        rows=frame,
        classified=classes,
        skipped_short=skipped,
        band_missing=band_missing,
    )
    return frame, outcome
