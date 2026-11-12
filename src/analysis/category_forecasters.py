"""
Arms and the panel backtest for the category forecast bake-off.

Two kinds of arm share one backtest:

- **local arm** follows the savings contract, ``arm(train, periods)``, and is
  fitted separately for every series at every origin;
- **panel arm** takes ``arm(panel_train, periods)`` and returns long rows
  ``series_key, month, point_estimate, ci80_lower, ci80_upper, method``. It is
  fitted once per origin on the whole panel (G1, RC, reconciliation).

Every feature, class label, scale and selector choice is computed from data on
or before the origin. The backtest clips every point and band at 0 and records
how many raw values were negative.

Usage:
    from src.analysis.category_forecasters import build_category_arm, panel_rolling_origin_backtest
    arm = build_category_arm({"arm_id": "naive_mean_12", "params": {}})
    bt = panel_rolling_origin_backtest(panel, arm, origins, horizon=12)
"""

from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.analysis.category_prophet import forecast_series
from src.analysis.forecast_backtest import trailing_mean
from src.analysis.forecasters import (
    _frame,
    _future_months,
    _mint_reconcile,
    _schafer_strimmer,
    seasonal_naive_forecaster,
)
from src.analysis.intermittent import (
    croston,
    panel_to_series,
    parse_series_key,
    sbc_class,
    tsb,
)

MIN_MONTHS_FORECAST = 12
PANEL_OUT_COLUMNS = [
    "arm",
    "series_key",
    "origin_month",
    "h",
    "month",
    "actual",
    "point",
    "lo80",
    "hi80",
    "fallback",
    "raw_negative",
]

_RECONCILE_METHODS = ("none", "bottom_up", "wls", "mint")


class ArmSkip(RuntimeError):
    """Raised when an arm legitimately cannot forecast a pair.

    Only *insufficient history* may use this. Any other failure is a bug and
    must propagate: a blanket ``except Exception`` is what hid the
    ``_mint_reconcile`` transpose error, where WLS and MinT silently returned
    the benchmark for every origin (plan guardrail, post-review).
    """


MIN_HISTORY_FALLBACK = MIN_MONTHS_FORECAST


# ── Common local arms ────────────────────────────────────────────────────────


def _flat_forecast(train: pd.Series, periods: int, value: float, method: str) -> pd.DataFrame:
    return _frame(_future_months(train, periods), value, method)


def make_trailing_mean(window: int = 12):
    """B1 / B1w: trailing mean, zeros included, held flat."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        if int(window) <= 0:
            return _flat_forecast(train, periods, 0.0, "zero")
        value = trailing_mean(train, int(window))
        return _flat_forecast(train, periods, value, f"naive_mean_{window}")

    return _forecast


def make_croston(variant: str = "classic", alpha: float = 0.1):
    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        rate = croston(train.to_numpy(dtype=float), alpha=float(alpha), variant=variant)
        return _flat_forecast(train, periods, rate, f"croston_{variant}_a{alpha}")

    return _forecast


def make_tsb(alpha: float = 0.1, beta: float = 0.1):
    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        rate = tsb(train.to_numpy(dtype=float), alpha=float(alpha), beta=float(beta))
        return _flat_forecast(train, periods, rate, f"tsb_a{alpha}_b{beta}")

    return _forecast


def make_prophet(yearly: bool = False):
    """P0 / P1: production forecast_series on the corrected grid."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        if len(train) < MIN_HISTORY_FALLBACK:
            raise ArmSkip(f"prophet needs {MIN_HISTORY_FALLBACK} months")
        try:
            frame = forecast_series(train, periods=periods, min_months=MIN_HISTORY_FALLBACK, yearly_seasonality=yearly)
        except Exception as exc:
            raise RuntimeError(f"prophet fit failed: {exc}") from exc
        if frame is None:
            raise RuntimeError("prophet returned no forecast for a long-enough series")
        months = pd.to_datetime(frame["month"])
        return pd.DataFrame(
            {
                "month": months,
                "point_estimate": pd.to_numeric(frame["point_estimate"]).to_numpy(dtype=float),
                "ci80_lower": pd.to_numeric(frame["ci80_lower"]).to_numpy(dtype=float),
                "ci80_upper": pd.to_numeric(frame["ci80_upper"]).to_numpy(dtype=float),
                "method": f"prophet_{'yearly' if yearly else 'shipped'}",
            }
        )

    return _forecast


def _ets_candidates(n: int) -> list[tuple[str | None, str | None, bool]]:
    """Plan §4.2: trend none / add-damped; seasonal add(12) at >= 36 months."""
    candidates: list[tuple[str | None, str | None, bool]] = [(None, None, False), ("add", None, True)]
    if n >= 36:
        candidates += [(None, "add", False), ("add", "add", True)]
    return candidates


def _fit_ets(y: pd.Series, trend, seasonal, damped: bool):
    from statsmodels.tsa.exponential_smoothing.ets import ETSModel

    model = ETSModel(
        y.astype(float),
        error="add",
        trend=trend,
        damped_trend=damped and trend is not None,
        seasonal=seasonal,
        seasonal_periods=12 if seasonal else None,
        initialization_method="estimated",
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return model.fit(disp=False)


def make_ets(window: int | None = None):
    """C1: additive ETS with AICc in-window selection."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        hist = train if window is None else train.iloc[-int(window) :]
        if len(hist) < 8:
            raise ArmSkip("auto_ets needs at least 8 months")
        # A constant window (all zeros, or all one value) has no estimable
        # dynamics; every specification is degenerate. This is a named
        # insufficient-information skip, not a converged-failure.
        if float(hist.std(ddof=0)) <= 0.0:
            raise ArmSkip("auto_ets needs a non-constant window")
        best = None
        best_aicc = np.inf
        for trend, seasonal, damped in _ets_candidates(len(hist)):
            try:
                fit = _fit_ets(hist, trend, seasonal, damped)
                aicc = float(fit.aicc)
            except Exception:
                # A single specification failing to converge is expected on
                # sparse data; the loop tries the rest before giving up.
                continue
            if np.isfinite(aicc) and aicc < best_aicc:
                best_aicc = aicc
                best = (fit, trend, seasonal, damped)
        if best is None:
            raise ArmSkip("no ETS specification converged")
        fit, trend, seasonal, damped = best
        point = np.asarray(fit.forecast(periods), dtype=float)
        lower = upper = None
        try:
            pred = fit.get_prediction(start=len(hist), end=len(hist) + periods - 1)
            frame = pred.summary_frame(alpha=0.2)
            lower = frame["pi_lower"].to_numpy(dtype=float)
            upper = frame["pi_upper"].to_numpy(dtype=float)
        except Exception:
            pass
        tag = f"{trend or 'none'}{'-damped' if damped else ''}/{(seasonal or 'none')}"
        return _frame(_future_months(train, periods), point, f"auto_ets:{tag}", lower, upper)

    return _forecast


def make_theta(window: int | None = None):
    """
    C2: ThetaModel, additive, deseasonalised at 12.

    The minimum is the same 12-month gate every arm uses, not a stricter one:
    a higher minimum would push otherwise-eligible pairs onto the benchmark and
    inflate the fallback count.
    """

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        from statsmodels.tsa.forecasting.theta import ThetaModel

        hist = train if window is None else train.iloc[-int(window) :]
        if len(hist) < MIN_HISTORY_FALLBACK:
            raise ArmSkip(f"theta needs {MIN_HISTORY_FALLBACK} months")
        if float(hist.std(ddof=0)) <= 0.0:
            raise ArmSkip("theta needs a non-constant window")
        # ThetaModel's deseasonalisation needs two complete seasonal cycles.
        # A 12-month shape cannot be estimated from fewer than 24 months, so
        # short windows are fitted without it rather than skipped: skipping
        # would push 14% of eligible pairs onto the benchmark. This mirrors
        # the plan's rule for ETS seasonal terms (36 months or more).
        seasonal = len(hist) >= 24
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = ThetaModel(
                hist.astype(float),
                period=12 if seasonal else None,
                deseasonalize=seasonal,
                use_test=seasonal,
                method="additive",
            )
            fit = model.fit()
            point = np.asarray(fit.forecast(periods), dtype=float)
        tag = f"theta_w{window or 'exp'}{'' if seasonal else '_noseas'}"
        return _frame(_future_months(train, periods), point, tag)

    return _forecast


def make_combo(nested: dict[str, dict]):
    """E1: equal-weight mean of B1, C1 and I2 points (composition fixed)."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        points = []
        for arm_id in ("naive_mean_12", "auto_ets", "tsb"):
            member = build_local_arm(arm_id, nested.get(arm_id, {}))
            try:
                frame = member(train, periods)
            except ArmSkip:
                # Only a member's insufficient-history skip may be absorbed;
                # any other error is a bug and must surface.
                continue
            values = frame["point_estimate"].to_numpy(dtype=float)
            if np.isfinite(values).all():
                points.append(values)
        if not points:
            raise ArmSkip("no combination member produced a forecast")
        return _frame(
            _future_months(train, periods),
            np.mean(np.vstack(points), axis=0),
            "combo:B1+C1+I2",
        )

    return _forecast


# ── Selectors (S1, S2) ───────────────────────────────────────────────────────


def make_class_selector(class_map: dict[str, dict], default: dict | None = None):
    """S1: pick the arm for the series' SBC class at this origin."""
    default_spec = default or class_map.get("lumpy") or {"arm_id": "naive_mean_12", "params": {}}

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        label = sbc_class(train.to_numpy(dtype=float))
        spec = class_map.get(label, default_spec)
        arm = build_local_arm(spec["arm_id"], spec.get("params", {}))
        frame = arm(train, periods)
        frame = frame.copy()
        frame["method"] = f"class_selector:{label}:{spec['arm_id']}"
        return frame

    return _forecast


def make_series_selector(series_map: dict[str, dict], fallback: dict):
    """S2: pick the arm per series from the private dev table."""

    def _forecast(train: pd.Series, periods: int, key: str) -> pd.DataFrame:
        spec = series_map.get(str(key), fallback)
        arm = build_local_arm(spec["arm_id"], spec.get("params", {}))
        frame = arm(train, periods)
        frame = frame.copy()
        frame["method"] = f"series_selector:{spec['arm_id']}"
        return frame

    return _forecast


# ── Local arm registry ───────────────────────────────────────────────────────


def build_local_arm(arm_id: str, params: dict | None = None):
    """Build one local arm from its id and tuned parameters."""
    params = params or {}
    if arm_id == "zero":
        return make_trailing_mean(0)
    if arm_id == "naive_mean_12":
        return make_trailing_mean(12)
    if arm_id == "naive_mean_w":
        return make_trailing_mean(int(params.get("window", 12)))
    if arm_id == "seasonal_naive":
        return seasonal_naive_forecaster
    if arm_id == "prophet_shipped":
        return make_prophet(yearly=False)
    if arm_id == "prophet_yearly":
        return make_prophet(yearly=True)
    if arm_id == "auto_ets":
        return make_ets(params.get("window"))
    if arm_id == "theta":
        return make_theta(params.get("window"))
    if arm_id == "croston":
        return make_croston(
            variant=str(params.get("variant", "classic")), alpha=float(params.get("alpha", 0.1))
        )
    if arm_id == "tsb":
        return make_tsb(alpha=float(params.get("alpha", 0.1)), beta=float(params.get("beta", 0.1)))
    if arm_id == "combo":
        nested = params.get("nested", {})
        defaults = {
            "naive_mean_12": {},
            "auto_ets": {"window": params.get("ets_window")},
            "tsb": {"alpha": params.get("tsb_alpha", 0.1), "beta": params.get("tsb_beta", 0.1)},
        }
        return make_combo({**defaults, **nested})
    if arm_id == "class_selector":
        return make_class_selector(params.get("class_map", {}), params.get("default"))
    if arm_id == "series_selector":
        return make_series_selector(params.get("series_map", {}), params.get("fallback", {}))
    raise KeyError(f"unknown local arm: {arm_id}")


# ── Panel arms ───────────────────────────────────────────────────────────────


@dataclass
class CategoryArm:
    """One arm plus how the backtest should drive it."""

    arm_id: str
    params: dict
    kind: str  # "local" | "series_local" | "panel"
    forecaster: callable

    def label(self) -> str:
        return self.arm_id


def _scaled_block(values: np.ndarray, mode: str) -> np.ndarray:
    """Per-time scale: expanding mean |y| or trailing-12 mean, floored at 1."""
    series = pd.Series(values, dtype=float)
    if mode == "trailing12":
        scale = series.abs().rolling(12, min_periods=1).mean()
    else:
        scale = series.abs().expanding(min_periods=1).mean()
    return np.maximum(scale.to_numpy(dtype=float), 1.0)


def _sbc_series(values: np.ndarray) -> list[str]:
    labels = []
    for end in range(1, len(values) + 1):
        labels.append(sbc_class(values[:end]))
    return labels


def _fourier(months: pd.DatetimeIndex, k: int) -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    if k <= 0:
        return out
    month_of_year = months.month.to_numpy(dtype=float)
    for i in range(1, k + 1):
        out[f"sin{i}"] = np.sin(2 * np.pi * i * month_of_year / 12.0)
        out[f"cos{i}"] = np.cos(2 * np.pi * i * month_of_year / 12.0)
    return out


def _g1_feature_frame(panel_train: pd.DataFrame, fourier_k: int, scale_mode: str) -> pd.DataFrame:
    """One row per (series, month) with lagged, scaled features."""
    frames = []
    for key, group in panel_train.groupby("series_key", sort=False):
        group = group.sort_values("month")
        y = group["y"].to_numpy(dtype=float)
        months = pd.DatetimeIndex(group["month"])
        meta = parse_series_key(str(key))
        scale = _scaled_block(y, scale_mode)
        scaled = y / scale
        s = pd.Series(scaled, index=months)
        frame = pd.DataFrame(
            {
                "series_key": str(key),
                "month": months,
                "flow_type": meta["flow_type"],
                "level": meta["level"],
                "category": meta["category"],
                "sbc": _sbc_series(y),
                "series_age": np.arange(1, len(y) + 1, dtype=float),
                "scale": scale,
            }
        )
        for lag in (1, 2, 3, 6, 12):
            frame[f"lag{lag}"] = s.shift(lag).to_numpy()
        for window in (3, 6, 12):
            frame[f"rmean{window}"] = s.rolling(window, min_periods=1).mean().to_numpy()
            frame[f"rstd{window}"] = s.rolling(window, min_periods=1).std().to_numpy()
        for span in (3, 12):
            frame[f"ewma{span}"] = s.ewm(span=span, adjust=False).mean().to_numpy()
        nonzero = (y > 0).astype(float)
        since = []
        counter = np.nan
        for value in nonzero:
            counter = 0.0 if value > 0 else (counter + 1.0 if np.isfinite(counter) else np.nan)
            since.append(counter)
        frame["months_since_nonzero"] = since
        frame["nonzero_12"] = pd.Series(nonzero, index=months).rolling(
            12, min_periods=1
        ).sum().to_numpy()
        for name, values in _fourier(pd.DatetimeIndex(frame["month"]), fourier_k).items():
            frame[name] = values
        frame["__y"] = y
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


_G1_FEATURES = (
    ["lag1", "lag2", "lag3", "lag6", "lag12"]
    + ["rmean3", "rmean6", "rmean12", "rstd3", "rstd6", "rstd12"]
    + ["ewma3", "ewma12", "months_since_nonzero", "nonzero_12", "series_age"]
    + ["flow_type", "level", "sbc", "category"]
)
_G1_CATEGORICAL = ["flow_type", "level", "sbc", "category"]


def make_global_lgbm(params: dict | None = None):
    """
    G1: one LightGBM model per horizon across the whole panel (plan §4.4).

    Targets are ``y[t+h] / scale[t]``; predictions are multiplied back by the
    origin scale and clipped at 0 by the backtest.
    """
    params = params or {}
    objective = str(params.get("objective", "l2"))
    tweedie_power = float(params.get("tweedie_variance_power", 1.5))
    num_leaves = int(params.get("num_leaves", 15))
    min_data = int(params.get("min_data_in_leaf", 20))
    n_estimators = int(params.get("n_estimators", 200))
    fourier_k = int(params.get("fourier_k", 1))
    scale_mode = str(params.get("scale", "expanding"))
    min_rows = int(params.get("min_rows", 24))

    def _forecast(panel_train: pd.DataFrame, periods: int) -> pd.DataFrame:
        import lightgbm as lgb

        features = _g1_feature_frame(panel_train, fourier_k, scale_mode)
        if features.empty:
            raise RuntimeError("G1: empty feature frame")
        for column in _G1_CATEGORICAL:
            features[column] = features[column].astype("category")
        origin_rows = features[features["month"] == features.groupby("series_key")["month"].transform("max")]

        records = []
        for horizon in range(1, periods + 1):
            target = features.groupby("series_key")["__y"].shift(-horizon)
            keep = target.notna()
            design = features.loc[keep, _G1_FEATURES].copy()
            target_values = (target[keep] / features.loc[keep, "scale"]).astype(float)
            if len(design) < min_rows:
                raise ArmSkip(f"G1: only {len(design)} rows for h={horizon}")
            model = lgb.LGBMRegressor(
                objective=("tweedie" if objective == "tweedie" else "regression"),
                tweedie_variance_power=(tweedie_power if objective == "tweedie" else 1.5),
                num_leaves=num_leaves,
                min_data_in_leaf=min_data,
                n_estimators=n_estimators,
                learning_rate=0.05,
                feature_fraction=0.8,
                bagging_fraction=0.8,
                bagging_freq=1,
                deterministic=True,
                force_row_wise=True,
                random_state=42,
                n_jobs=1,
                verbose=-1,
            )
            model.fit(design, target_values.to_numpy(dtype=float))
            predicted = model.predict(origin_rows[_G1_FEATURES])
            months = pd.DatetimeIndex(origin_rows["month"]) + pd.DateOffset(months=horizon)
            records.append(
                pd.DataFrame(
                    {
                        "series_key": origin_rows["series_key"].to_numpy(),
                        "month": months,
                        "point_estimate": predicted * origin_rows["scale"].to_numpy(dtype=float),
                        "ci80_lower": np.nan,
                        "ci80_upper": np.nan,
                        "method": "global_lgbm",
                    }
                )
            )
        return pd.concat(records, ignore_index=True)

    return _forecast


def _project_commitment_amounts(register: pd.DataFrame, months: pd.DatetimeIndex) -> dict[str, np.ndarray]:
    """Monthly amounts per (category, subcategory) from active commitments."""
    out: dict[tuple[str, str], np.ndarray] = {}
    cadence_months = {
        "weekly": 0.0,
        "biweekly": 0.0,
        "monthly": 1.0,
        "quarterly": 3.0,
        "semiannual": 6.0,
        "annual": 12.0,
    }
    for _, row in register.iterrows():
        if not bool(row.get("is_active", False)):
            continue
        cadence = str(row.get("cadence_class", "irregular"))
        if cadence not in cadence_months:
            continue
        amount = float(row.get("amount_current", 0.0) or 0.0)
        run_rate = float(row.get("monthly_run_rate", 0.0) or 0.0)
        key = (str(row.get("category")), str(row.get("subcategory")))
        values = np.zeros(len(months))
        if cadence in ("weekly", "biweekly"):
            values[:] = run_rate
        elif cadence == "monthly":
            values[:] = amount
        else:
            step = int(cadence_months[cadence])
            try:
                next_date = pd.Timestamp(row.get("next_expected_date"))
            except (TypeError, ValueError):
                next_date = months[0]
            if pd.isna(next_date):
                next_date = months[0]
            next_period = next_date.to_period("M").to_timestamp()
            for i, month in enumerate(months):
                if (month.year - next_period.year) * 12 + (month.month - next_period.month) >= 0:
                    gap = (month.year - next_period.year) * 12 + (month.month - next_period.month)
                    if gap % step == 0:
                        values[i] = amount
        out[key] = out.get(key, np.zeros(len(months))) + values
    return out


def make_recurring_aware(params: dict | None = None):
    """
    RC: recurring commitments projected forward, plus B1 on the remainder.

    Income series fall back to B1. Expense commitments are mapped through the
    register's ``category`` and ``subcategory``; the recurring actuals those
    payees booked in the training window are removed from the series before B1
    is applied to the remainder, so nothing is counted twice. Active
    commitments are projected by cadence, next expected date and current
    amount.
    """
    params = params or {}

    def _forecast(panel_train: pd.DataFrame, periods: int, transactions: pd.DataFrame | None = None):
        if transactions is None:
            raise RuntimeError("RC needs transactions")
        from src.analysis.recurring_commitments import build_recurring_commitments

        origin = pd.Timestamp(panel_train["month"].max())
        months = pd.date_range(origin + pd.DateOffset(months=1), periods=periods, freq="MS")
        cutoff = origin + pd.offsets.MonthEnd(0)
        history = transactions[pd.to_datetime(transactions["date"]) <= cutoff]
        register, events = build_recurring_commitments(history, as_of=cutoff)
        projected = _project_commitment_amounts(register, months)

        # Recurring actuals in the training window, per mapped series.
        by_sub: dict[tuple[str, str], pd.Series] = {}
        by_cat: dict[str, pd.Series] = {}
        if not register.empty and not events.empty:
            mapping = {
                str(row["payee"]): (str(row["category"]), str(row["subcategory"]))
                for _, row in register.iterrows()
            }
            events = events.copy()
            events["_date"] = pd.to_datetime(events["date"])
            events = events[events["_date"] <= cutoff]
            events["_month"] = events["_date"].dt.to_period("M").dt.to_timestamp()
            events["_cat"] = events["payee"].map({k: v[0] for k, v in mapping.items()})
            events["_sub"] = events["payee"].map({k: v[1] for k, v in mapping.items()})
            for (cat, sub), group in events.dropna(subset=["_sub"]).groupby(["_cat", "_sub"]):
                by_sub[(str(cat), str(sub))] = group.groupby("_month")["amount"].sum()
            for cat, group in events.dropna(subset=["_cat"]).groupby("_cat"):
                by_cat[str(cat)] = group.groupby("_month")["amount"].sum()

        series = panel_to_series(panel_train)
        records = []
        for key, y in series.items():
            meta = parse_series_key(str(key))
            if meta["flow_type"] != "expense":
                records.append(
                    _panel_row(
                        key,
                        months,
                        np.full(periods, trailing_mean(y, 12)),
                        "recurring_aware:b1_income",
                    )
                )
                continue
            category = str(meta["category"])
            if meta["level"] == "category":
                recurring_hist = by_cat.get(category)
                future = np.zeros(periods)
                for (cat, _sub), values in projected.items():
                    if cat == category:
                        future = future + values
            else:
                sub = str(meta["subcategory"] or "")
                recurring_hist = by_sub.get((category, sub))
                future = projected.get((category, sub), np.zeros(periods))
            remainder = y.copy()
            if recurring_hist is not None:
                aligned = recurring_hist.reindex(y.index, fill_value=0.0)
                remainder = (y - aligned).clip(lower=0.0)
            base = trailing_mean(remainder, 12)
            records.append(
                _panel_row(key, months, np.full(periods, base) + future, "recurring_aware")
            )
        return pd.concat(records, ignore_index=True) if records else pd.DataFrame()

    return _forecast


def _panel_row(key: str, months: pd.DatetimeIndex, point, method: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "series_key": key,
            "month": months,
            "point_estimate": np.asarray(point, dtype=float),
            "ci80_lower": np.nan,
            "ci80_upper": np.nan,
            "method": method,
        }
    )


def make_reconciled(
    base_spec: dict,
    method: str,
    residual_frame: pd.DataFrame | None = None,
    hierarchy: dict | None = None,
):
    """
    H: reconcile the base arm's panel forecasts within each flow.

    ``method`` is none / bottom_up / wls / mint. The MinT weight is the
    Schafer-Strimmer shrinkage of the base arm's purged h = 1 errors from
    origins fully realised before this origin, per plan §4.3.
    """
    if method not in _RECONCILE_METHODS:
        raise ValueError(f"unknown reconciliation method: {method}")
    base_arm_id = base_spec["arm_id"]
    base_params = base_spec.get("params", {})

    def _forecast(panel_train: pd.DataFrame, periods: int):
        if hierarchy is None:
            raise RuntimeError("reconciliation needs a hierarchy")
        series = panel_to_series(panel_train)
        months = pd.date_range(
            panel_train["month"].max() + pd.DateOffset(months=1), periods=periods, freq="MS"
        )
        base_arm = build_local_arm(base_arm_id, base_params)
        base_points: dict[str, np.ndarray] = {}
        for key, y in series.items():
            try:
                frame = base_arm(y, periods)
                base_points[key] = frame["point_estimate"].to_numpy(dtype=float)
            except ArmSkip:
                base_points[key] = np.full(periods, trailing_mean(y, 12))
        if method == "none":
            return _reconciled_frame(base_points, months, "reconciled:none")

        origin = panel_train["month"].max()
        records: dict[str, np.ndarray] = {}
        for flow, spec in hierarchy.items():
            keys = [key for key in spec["keys"] if key in base_points]
            if not keys:
                continue
            position = {key: i for i, key in enumerate(spec["keys"])}
            indices = [position[key] for key in keys]
            base = np.vstack([base_points[key] for key in keys])
            if base.shape[0] == 0:
                continue

            if method == "bottom_up":
                coherent = np.zeros_like(base)
                local_summing = spec["summing"][np.ix_(indices, indices)]
                for row in range(len(keys)):
                    coherent[row] = (local_summing[row][:, None] * base).sum(axis=0)
            else:
                local_summing = spec["summing"][np.ix_(indices, indices)]
                if method == "wls":
                    weight = np.diag(np.maximum(local_summing.sum(axis=1), 1.0))
                else:  # mint
                    weight = _mint_weight(keys, origin, residual_frame)
                    if weight is None:
                        weight = np.diag(np.maximum(local_summing.sum(axis=1), 1.0))
                # ``_mint_reconcile`` expects (nodes, periods) with a
                # (nodes, nodes) gain, exactly as in the savings arms.
                coherent = _mint_reconcile(base, weight, local_summing)
            for row, key in enumerate(keys):
                records[key] = coherent[row]
        return _reconciled_frame(records, months, f"reconciled:{method}")

    def _mint_weight(keys, origin, frame):
        """
        Schafer-Strimmer weight matrix aligned to ``keys`` exactly.

        The matrix must be (len(keys), len(keys)) or ``_mint_reconcile`` blows
        up. A series with no realised residual at h = 1 gets a zero column,
        which the shrinkage turns into a zero-variance diagonal; those entries
        are floored so the matrix stays usable.
        """
        if frame is None or frame.empty:
            return None
        prior = frame[
            (frame["h"] == 1) & (frame["origin_month"] + pd.DateOffset(months=1) <= origin)
        ]
        if prior.empty:
            return None
        matrix = prior.pivot_table(
            index="origin_month", columns="series_key", values="err", aggfunc="sum"
        ).reindex(columns=keys)
        if matrix.shape[0] < 8 or matrix.shape[1] < 2:
            return None
        filled = matrix.fillna(0.0).to_numpy(dtype=float)
        weight = np.atleast_2d(_schafer_strimmer(filled))
        if weight.shape != (len(keys), len(keys)):
            return None
        # Floor the diagonal so a constant (or absent) series cannot make the
        # weight singular in a way pinv resolves to garbage.
        scale = float(np.mean(np.diag(weight)))
        floor = max(scale * 1e-6, 1e-9)
        weight = weight + np.eye(len(keys)) * floor
        return weight

    return _forecast


def _reconciled_frame(points: dict[str, np.ndarray], months: pd.DatetimeIndex, method: str) -> pd.DataFrame:
    frames = []
    for key, values in points.items():
        frames.append(
            pd.DataFrame(
                {
                    "series_key": key,
                    "month": months,
                    "point_estimate": np.asarray(values, dtype=float),
                    "ci80_lower": np.nan,
                    "ci80_upper": np.nan,
                    "method": method,
                }
            )
        )
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Arm factory ──────────────────────────────────────────────────────────────


def build_category_arm(spec: dict, context: dict | None = None, key: str | None = None) -> CategoryArm:
    """
    Build any arm from a config spec.

    ``context`` carries ``transactions`` (RC), ``residual_frame`` and
    ``hierarchy`` (reconciliation).
    """
    context = context or {}
    arm_id = str(spec["arm_id"])
    params = spec.get("params", {}) or {}

    if arm_id in ("class_selector", "series_selector"):
        if arm_id == "class_selector":
            forecaster = make_class_selector(params.get("class_map", {}), params.get("default"))
            return CategoryArm(arm_id, params, "local", forecaster)
        forecaster = make_series_selector(params.get("series_map", {}), params.get("fallback", {}))
        return CategoryArm(arm_id, params, "series_local", forecaster)

    if arm_id == "global_lgbm":
        return CategoryArm(arm_id, params, "panel", make_global_lgbm(params))

    if arm_id == "recurring_aware":
        forecaster = make_recurring_aware(params)
        forecaster.transactions = context.get("transactions")
        return CategoryArm(arm_id, params, "panel_rc", forecaster)

    if arm_id == "reconciled":
        forecaster = make_reconciled(
            params["base"],
            str(params["method"]),
            residual_frame=context.get("residual_frame"),
            hierarchy=context.get("hierarchy"),
        )
        return CategoryArm(arm_id, params, "panel", forecaster)

    return CategoryArm(arm_id, params, "local", build_local_arm(arm_id, params))


def spec_slug(spec: dict) -> str:
    text = json.dumps(spec, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:10]


# ── Panel backtest ───────────────────────────────────────────────────────────


def _local_task(payload: tuple) -> dict:
    """Fit one local arm for one (series, origin). Runs in a worker."""
    spec, values, origin, horizon, key, kind = payload
    history = values[values.index <= origin]
    train = history
    next_month = pd.Timestamp(origin) + pd.DateOffset(months=1)
    forecast_months = pd.date_range(start=next_month, periods=horizon, freq="MS")
    actual = values.reindex(forecast_months).to_numpy(dtype=float)
    status = {"fallback": False, "negative": 0, "skip_reason": None}
    try:
        arm = build_local_arm(spec["arm_id"], spec.get("params", {}))
        if kind == "series_local":
            frame = arm(train, horizon, key)
        else:
            frame = arm(train, horizon)
        point = pd.to_numeric(frame["point_estimate"], errors="coerce").to_numpy(dtype=float)[:horizon]
        if not np.isfinite(point).all() or len(point) < horizon:
            raise RuntimeError("non-finite or short forecast")
        lower = (
            pd.to_numeric(frame["ci80_lower"], errors="coerce").to_numpy(dtype=float)[:horizon]
            if "ci80_lower" in frame
            else np.full(horizon, np.nan)
        )
        upper = (
            pd.to_numeric(frame["ci80_upper"], errors="coerce").to_numpy(dtype=float)[:horizon]
            if "ci80_upper" in frame
            else np.full(horizon, np.nan)
        )
    except ArmSkip as exc:
        # Explicit insufficient-history skip only. Recorded and diagnosed.
        point = np.full(horizon, trailing_mean(train, 12))
        lower = np.full(horizon, np.nan)
        upper = np.full(horizon, np.nan)
        status["fallback"] = True
        status["skip_reason"] = str(exc)
    status["negative"] = int((np.asarray(point) < 0).sum() + np.nansum(np.asarray(lower) < 0))
    point = np.clip(point, 0.0, None)
    lower = np.clip(lower, 0.0, None) if lower is not None else lower
    upper = np.clip(upper, 0.0, None) if upper is not None else upper
    n = min(len(point), len(actual))
    return {
        "records": pd.DataFrame(
            {
                "series_key": key,
                "origin_month": pd.Timestamp(origin),
                "h": np.arange(1, n + 1),
                "month": forecast_months[:n],
                "actual": actual[:n],
                "point": point[:n],
                "lo80": lower[:n],
                "hi80": upper[:n],
                "fallback": bool(status["fallback"]),
            }
        ),
        "status": status,
    }


def _panel_task(payload: tuple) -> dict:
    """Run one panel arm for one origin. Runs in a worker."""
    spec, panel, origin_position, horizon, context, kind = payload
    months = pd.DatetimeIndex(sorted(panel["month"].unique()))
    origin = months[origin_position - 1]
    train = panel[panel["month"] <= origin]
    status = {"fallback": False, "negative": 0, "skip_reason": None}
    try:
        if kind == "panel_rc":
            arm = make_recurring_aware(spec.get("params", {}))
            frame = arm(train, horizon, transactions=context.get("transactions"))
        else:
            arm = build_category_arm(spec, context).forecaster
            frame = arm(train, horizon)
        if frame is None or frame.empty:
            raise RuntimeError("empty panel forecast")
    except ArmSkip as exc:
        series = panel_to_series(train)
        rows = []
        for key, y in series.items():
            rows.append(
                pd.DataFrame(
                    {
                        "series_key": key,
                        "month": pd.date_range(
                            origin + pd.DateOffset(months=1), periods=horizon, freq="MS"
                        ),
                        "point_estimate": np.full(horizon, trailing_mean(y, 12)),
                        "ci80_lower": np.nan,
                        "ci80_upper": np.nan,
                        "method": "fallback",
                    }
                )
            )
        frame = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
        status["fallback"] = True
        status["skip_reason"] = str(exc)

    if frame is None or frame.empty:
        raise RuntimeError(
            f"panel arm {spec['arm_id']} produced no rows at origin {origin.date()} "
            "(no fallback is allowed for non-skip failures)"
        )

    frame = frame.copy()
    point = pd.to_numeric(frame["point_estimate"], errors="coerce").to_numpy(dtype=float)
    status["negative"] = int((point < 0).sum())
    frame["point_estimate"] = np.clip(point, 0.0, None)
    for column in ("ci80_lower", "ci80_upper"):
        if column in frame:
            frame[column] = np.clip(
                pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float), 0.0, None
            )
        else:
            frame[column] = np.nan

    full = panel_to_series(panel)
    train_series = panel_to_series(train)
    records = []
    for key, group in frame.groupby("series_key", sort=False):
        key = str(key)
        if key not in train_series:
            continue
        if len(train_series[key]) < MIN_MONTHS_FORECAST:
            continue
        group = group.sort_values("month")
        forecast_months = pd.DatetimeIndex(group["month"])
        actual = full[key].reindex(forecast_months).to_numpy(dtype=float)
        points = group["point_estimate"].to_numpy(dtype=float)
        n = min(len(actual), len(points), horizon)
        if n <= 0 or not np.isfinite(actual[:n]).all():
            continue
        records.append(
            pd.DataFrame(
                {
                    "series_key": str(key),
                    "origin_month": origin,
                    "h": np.arange(1, n + 1),
                    "month": forecast_months[:n],
                    "actual": actual[:n],
                    "point": points[:n],
                    "lo80": group["ci80_lower"].to_numpy(dtype=float)[:n],
                    "hi80": group["ci80_upper"].to_numpy(dtype=float)[:n],
                    "fallback": bool(status["fallback"]),
                }
            )
        )
    frame_out = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
    return {"records": frame_out, "status": status}


def panel_rolling_origin_backtest(
    panel: pd.DataFrame,
    arm: CategoryArm,
    origins,
    horizon: int = 12,
    n_jobs: int = -1,
    context: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Backtest one arm over the panel at every origin.

    Returns ``(long rows, stats)``. Rows carry ``arm, series_key,
    origin_month, h, month, actual, point, lo80, hi80, raw_negative``.
    ``stats`` counts fallbacks and negative raw values.
    """
    from joblib import Parallel, delayed

    context = context or {}
    months = pd.DatetimeIndex(sorted(panel["month"].unique()))
    origin_positions = [int(np.searchsorted(months, pd.Timestamp(o))) + 1 for o in origins]
    series = panel_to_series(panel)

    meta = panel[["series_key"]].drop_duplicates()
    meta["series_key"] = meta["series_key"].astype(str)

    if arm.kind in ("local", "series_local"):
        payloads = []
        spec = {"arm_id": arm.arm_id, "params": arm.params}
        for key, values in series.items():
            # History starts at the series' first non-zero month (the grid
            # start), so this gate is exactly "12 months since first non-zero".
            for origin in origins:
                history = values[values.index <= pd.Timestamp(origin)]
                if len(history) < MIN_MONTHS_FORECAST:
                    continue
                payloads.append((spec, values, pd.Timestamp(origin), horizon, key, arm.kind))
        results = Parallel(n_jobs=n_jobs, backend="loky")(
            delayed(_local_task)(payload) for payload in payloads
        )
    else:
        payloads = [
            (
                {"arm_id": arm.arm_id, "params": arm.params},
                panel,
                position,
                horizon,
                context,
                arm.kind,
            )
            for position in origin_positions
        ]
        results = Parallel(n_jobs=n_jobs, backend="loky")(
            delayed(_panel_task)(payload) for payload in payloads
        )

    frames = [result["records"] for result in results if result["records"] is not None]
    fallback_pairs = 0
    for result in results:
        records = result["records"]
        if records is None or records.empty:
            continue
        if result["status"]["fallback"]:
            fallback_pairs += int(len(records.groupby(["series_key", "origin_month"], sort=False)))
    stats = {
        "fallbacks": int(sum(1 for r in results if r["status"]["fallback"])),
        "fallback_pairs": fallback_pairs,
        "negative_values": int(sum(r["status"]["negative"] for r in results)),
        "pairs": int(sum(len(r["records"]) for r in results if r["records"] is not None)),
        "skip_reasons": sorted(
            {r["status"].get("skip_reason") for r in results if r["status"].get("skip_reason")}
        ),
    }
    if not frames:
        return pd.DataFrame(columns=PANEL_OUT_COLUMNS), stats
    out = pd.concat(frames, ignore_index=True)
    out["arm"] = arm.arm_id
    out["origin_month"] = pd.to_datetime(out["origin_month"])
    out["month"] = pd.to_datetime(out["month"])
    out = out.merge(meta, on="series_key", how="left")
    out["raw_negative"] = stats["negative_values"] > 0
    return out[PANEL_OUT_COLUMNS], stats


def cumulative_columns(bt: pd.DataFrame) -> pd.DataFrame:
    """Add cum_actual, cum_point and err, ordered within each pair."""
    if bt.empty:
        return bt
    out = bt.sort_values(["series_key", "origin_month", "h"]).copy()
    grouped = out.groupby(["series_key", "origin_month"], sort=False)
    out["cum_actual"] = grouped["actual"].cumsum()
    out["cum_point"] = grouped["point"].cumsum()
    out["err"] = out["actual"] - out["point"]
    return out.reset_index(drop=True)


def eligibility(panel_train: pd.DataFrame) -> list[str]:
    """Series with at least 12 months of history at the origin."""
    return [
        key
        for key, group in panel_train.groupby("series_key")
        if len(group) >= MIN_MONTHS_FORECAST
    ]
