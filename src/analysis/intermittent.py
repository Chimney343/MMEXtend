"""
Intermittent-demand helpers for the category forecast bake-off.

Four things live here, all of them reusable outside the experiment:

- ``build_panel`` - the *corrected* grid. Every series runs from its first
  non-zero month to the last complete month, with zeros filled in (including
  trailing zeros). Production's ``_continuous_month_series`` stops at the last
  non-zero month instead, so dormant series are "forecast" from a date in the
  past.
- ``build_hierarchy`` - the within-flow hierarchy used for reconciliation:
  ``category = sum(subcategories) + unassigned``.
- ``demand_stats`` / ``sbc_class`` - Syntetos-Boylan-Croston demand classes.
- ``croston`` / ``tsb`` - the intermittent point forecasters, written from the
  equations in the experiment plan (section 4.2). The forecast is a flat
  per-period rate.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

UNASSIGNED = "(unassigned)"

ADI_CUTOFF = 1.32
CV2_CUTOFF = 0.49

SBC_CLASSES = ("smooth", "erratic", "intermittent", "lumpy")

PANEL_COLUMNS = [
    "series_key",
    "flow_type",
    "level",
    "category",
    "subcategory",
    "is_unassigned",
    "month",
    "y",
]


def series_key(flow_type: str, level: str, category: str, subcategory) -> str:
    """Stable readable key for one series (private outputs only)."""
    sub = "" if subcategory is None or (isinstance(subcategory, float) and np.isnan(subcategory)) else str(subcategory)
    return f"{flow_type}|{level}|{category}|{sub}"


def parse_series_key(key: str) -> dict:
    flow_type, level, category, sub = str(key).split("|", 3)
    return {
        "flow_type": flow_type,
        "level": level,
        "category": category,
        "subcategory": sub if sub else None,
        "is_unassigned": bool(sub == UNASSIGNED),
    }


def _month_grid(first: pd.Timestamp, last: pd.Timestamp) -> pd.DatetimeIndex:
    return pd.date_range(start=first, end=last, freq="MS")


def build_panel(
    monthly: pd.DataFrame,
    last_complete=None,
    include_unassigned: bool = True,
) -> pd.DataFrame:
    """
    Build the corrected long panel from the sparse monthly frame.

    Parameters
    ----------
    monthly : pd.DataFrame
        Columns ``flow_type, level, category, subcategory, month, y`` as
        produced by ``build_category_monthly_series``. Sparse: months with no
        activity may be absent.
    last_complete : Timestamp | None
        Last complete month. Defaults to the maximum month in ``monthly``.
    include_unassigned : bool
        Add one ``unassigned`` node per (flow, category), equal to the category
        total minus the sum of its subcategories.

    Returns
    -------
    pd.DataFrame
        Long panel with ``PANEL_COLUMNS``. Each series spans its first non-zero
        month to ``last_complete``, zeros included, and trailing zeros kept.
    """
    frame = monthly.copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.to_period("M").dt.to_timestamp()
    frame["y"] = frame["y"].astype(float)
    last = pd.to_datetime(last_complete) if last_complete is not None else frame["month"].max()
    last = pd.Timestamp(last).to_period("M").to_timestamp()

    records: list[pd.DataFrame] = []
    for (flow_type, level, category, subcategory), group in frame.groupby(
        ["flow_type", "level", "category", "subcategory"], dropna=False
    ):
        y = group.groupby("month")["y"].sum().sort_index()
        active = y[y > 0]
        if active.empty:
            continue
        grid = _month_grid(active.index.min(), last)
        series = y.reindex(grid, fill_value=0.0)
        records.append(
            pd.DataFrame(
                {
                    "series_key": series_key(flow_type, level, category, subcategory),
                    "flow_type": flow_type,
                    "level": level,
                    "category": category,
                    "subcategory": subcategory,
                    "is_unassigned": False,
                    "month": grid,
                    "y": series.to_numpy(dtype=float),
                }
            )
        )

    if include_unassigned:
        records.extend(_unassigned_frames(frame, last))

    if not records:
        return pd.DataFrame(columns=PANEL_COLUMNS)
    panel = pd.concat(records, ignore_index=True)
    return panel[PANEL_COLUMNS].sort_values(
        ["flow_type", "level", "category", "subcategory", "month"], kind="mergesort"
    ).reset_index(drop=True)


def _unassigned_frames(frame: pd.DataFrame, last: pd.Timestamp) -> list[pd.DataFrame]:
    """One ``unassigned`` series per (flow, category): total minus subcategories."""
    frames: list[pd.DataFrame] = []
    total = (
        frame[frame["level"] == "category"]
        .groupby(["flow_type", "category", "month"], dropna=False)["y"]
        .sum()
        .reset_index()
    )
    parts = (
        frame[frame["level"] == "subcategory"]
        .groupby(["flow_type", "category", "month"], dropna=False)["y"]
        .sum()
        .reset_index()
    )
    for (flow_type, category), cat_group in total.groupby(["flow_type", "category"], dropna=False):
        cat_total = cat_group.set_index("month")["y"].sort_index()
        part_rows = parts[
            (parts["flow_type"] == flow_type) & (parts["category"] == category)
        ]
        part = (
            part_rows.set_index("month")["y"]
            if not part_rows.empty
            else pd.Series(dtype=float)
        )
        diff = cat_total.sub(part.reindex(cat_total.index, fill_value=0.0), fill_value=0.0)
        diff = diff.clip(lower=0.0)
        active = diff[diff > 0]
        if active.empty:
            continue
        grid = _month_grid(active.index.min(), last)
        series = diff.reindex(grid, fill_value=0.0)
        frames.append(
            pd.DataFrame(
                {
                    "series_key": series_key(flow_type, "unassigned", category, UNASSIGNED),
                    "flow_type": flow_type,
                    "level": "unassigned",
                    "category": category,
                    "subcategory": UNASSIGNED,
                    "is_unassigned": True,
                    "month": grid,
                    "y": series.to_numpy(dtype=float),
                }
            )
        )
    return frames


def panel_series(panel: pd.DataFrame, key: str) -> pd.Series:
    """One series from the panel, indexed by month."""
    return panel_to_series(panel)[key]


def panel_to_series(panel: pd.DataFrame) -> dict[str, pd.Series]:
    """``{series_key: pd.Series}`` with a month-start index."""
    out: dict[str, pd.Series] = {}
    for key, group in panel.groupby("series_key", sort=False):
        out[str(key)] = pd.Series(
            group["y"].to_numpy(dtype=float),
            index=pd.DatetimeIndex(group["month"]),
            name=str(key),
        )
    return out


def build_hierarchy(panel: pd.DataFrame) -> dict:
    """
    Within-flow hierarchy for reconciliation.

    Returns ``{flow_type: {"keys": [...], "summing": S, "levels": [...]}}`` where
    ``S`` maps the stacked base forecasts (in ``keys`` order) to coherent ones:
    each category row sums its subcategory rows plus the unassigned row.
    """
    out: dict[str, dict] = {}
    for flow, group in panel.groupby("flow_type", sort=False):
        keys = list(dict.fromkeys(group["series_key"].astype(str)))
        meta = {key: parse_series_key(key) for key in keys}
        categories = [k for k in keys if meta[k]["level"] == "category"]
        children = [k for k in keys if meta[k]["level"] in ("subcategory", "unassigned")]
        if not categories:
            continue
        position = {key: i for i, key in enumerate(keys)}
        n = len(keys)
        summing = np.zeros((n, n))
        for key in children:
            summing[position[key], position[key]] = 1.0
        for key in categories:
            category = meta[key]["category"]
            members = [
                position[child] for child in children if meta[child]["category"] == category
            ]
            if members:
                for column in members:
                    summing[position[key], column] = 1.0
            else:
                # A category with no subcategory and no unassigned remainder
                # cannot be rebuilt from children; keep it as its own base.
                summing[position[key], position[key]] = 1.0
        out[flow] = {
            "keys": keys,
            "summing": summing,
            "meta": meta,
            "bottom": [position[key] for key in children],
        }
    return out


def is_coherent(values: dict[str, float], hierarchy: dict) -> bool:
    """True when every category equals its children plus unassigned (tolerance)."""
    for flow, spec in hierarchy.items():
        meta = spec["meta"]
        for key, node in meta.items():
            if node["level"] != "category" or key not in values:
                continue
            total = values.get(key, 0.0)
            parts = [
                values.get(child, 0.0)
                for child, child_node in meta.items()
                if child_node["level"] in ("subcategory", "unassigned")
                and child_node["category"] == node["category"]
            ]
            if abs(total - sum(parts)) > 1e-6 * max(1.0, abs(total)):
                return False
    return True


def demand_stats(y) -> tuple[float, float]:
    """
    Average demand interval and squared coefficient of variation of demand size.

    ``ADI = n / n_nonzero``; ``CV2`` uses only non-zero demand sizes. An
    all-zero window has ``ADI = inf`` and ``CV2 = 0.0``. A single demand month
    also has ``CV2 = 0.0`` (no dispersion can be estimated).
    """
    values = np.asarray(y, dtype=float)
    n = int(values.size)
    if n == 0:
        return float("inf"), 0.0
    nonzero = values[values > 0]
    k = int(nonzero.size)
    if k == 0:
        return float("inf"), 0.0
    adi = n / k
    if k < 2:
        return adi, 0.0
    mean = float(nonzero.mean())
    if mean <= 0:
        return adi, 0.0
    cv2 = float(np.var(nonzero, ddof=0) / (mean**2))
    return adi, cv2


def sbc_class(y) -> str:
    """
    Syntetos-Boylan-Croston class for one demand series.

    ``smooth`` and ``erratic`` have ADI below the cutoff; ``intermittent`` and
    ``lumpy`` at or above it. ``erratic`` and ``lumpy`` have CV2 at or above
    the cutoff. An all-zero window classifies as ``intermittent`` (ADI
    infinite, CV2 zero).
    """
    adi, cv2 = demand_stats(y)
    if adi < ADI_CUTOFF:
        return "smooth" if cv2 < CV2_CUTOFF else "erratic"
    return "intermittent" if cv2 < CV2_CUTOFF else "lumpy"


def croston(y, alpha: float = 0.1, variant: str = "classic") -> float:
    """
    Croston (or SBA) per-period demand rate.

    ``z`` is the exponentially smoothed demand size, updated only in demand
    months. ``p`` is the smoothed interval between demand months. The forecast
    is ``z / p``; SBA multiplies it by ``(1 - alpha / 2)``. The first demand
    sets ``z`` to its size and ``p`` to its position from the series start
    (one-based). An empty or all-zero series returns 0.
    """
    values = np.asarray(y, dtype=float)
    alpha = float(alpha)
    size = None
    interval = None
    last_demand = None
    for t, value in enumerate(values):
        if value > 0:
            if size is None:
                size = float(value)
                interval = float(t + 1)
            else:
                gap = float(t - last_demand)
                size = size + alpha * (value - size)
                interval = interval + alpha * (gap - interval)
            last_demand = t
    if size is None or interval is None or interval <= 0:
        return 0.0
    forecast = size / interval
    if variant == "sba":
        forecast *= 1.0 - alpha / 2.0
    elif variant != "classic":
        raise ValueError(f"unknown Croston variant: {variant}")
    return float(max(forecast, 0.0))


def tsb(y, alpha: float = 0.1, beta: float = 0.1) -> float:
    """
    Teunter-Syntetos-Babai per-period demand rate ``p * z``.

    ``p`` (demand probability) is updated every month from the indicator;
    ``z`` (demand size) only in demand months. Initialised with the observed
    demand frequency and the mean non-zero size, so the first forecast equals
    the sample mean rate. An all-zero series returns 0.
    """
    values = np.asarray(y, dtype=float)
    n = int(values.size)
    if n == 0:
        return 0.0
    nonzero = values[values > 0]
    if nonzero.size == 0:
        return 0.0
    probability = float(nonzero.size) / n
    size = float(nonzero.mean())
    for value in values:
        indicator = 1.0 if value > 0 else 0.0
        probability = probability + float(beta) * (indicator - probability)
        if indicator:
            size = size + float(alpha) * (value - size)
    return float(max(probability * size, 0.0))
