"""
Forecaster arms for the savings-forecast bake-off.

Every arm meets the contract that ``rolling_origin_backtest`` already accepts::

    forecaster(train: pd.Series, periods: int) -> pd.DataFrame
    # columns: month, point_estimate, ci80_lower, ci80_upper, method

All state is fitted from ``train`` only. Arms are built with factories so the
runner can vary a training window, a grid point or a preprocessing switch, and
the winner can drop into production without glue code.

Usage:
    from src.analysis.forecasters import build_registry, make_arm, ARMS
    registry = build_registry(components={"deposits": dep, "withdrawals": wd})
    forecast = registry["naive_mean_12"](train, 12)
"""

from __future__ import annotations

import math
import warnings

import numpy as np
import pandas as pd

from src.analysis.cashflow_forecast import (
    forecast_holtwinters,
    select_and_run_forecast,
)
from src.analysis.forecast_backtest import naive_mean_forecaster

SEED = 42
WINDOW_OPTIONS: tuple[int | None, ...] = (None, 60, 48, 36)
FOURIER_OPTIONS: tuple[int, ...] = (0, 1, 2)

_OUT_COLUMNS = ["month", "point_estimate", "ci80_lower", "ci80_upper", "method"]


class ArmSkipped(Exception):
    """Raised when an arm cannot forecast an origin (not enough training rows)."""


# ── Static arm metadata ──────────────────────────────────────────────────────


# Complexity order from plan §7: lower rank = simpler.
BASE_RANK: dict[str, float] = {
    "naive_mean_12": 1.0,
    "naive_median_12": 2.0,
    "random_walk": 3.0,
    "seasonal_naive": 4.0,
    "drift": 5.0,
    "auto_ets": 6.0,
    "theta": 6.0,
    "auto_arima": 7.0,
    "holt_winters_shipped": 7.0,
    "combo_classical": 8.0,
    "decomposed_bu": 9.0,
    "decomposed_mint": 10.0,
    "ridge_direct": 11.0,
    "prophet_shipped": 12.0,
    "lgbm_direct": 13.0,
}

# Arms whose training window is tuned on dev (plan §4.2–4.3).
WINDOWED_ARMS = {
    "drift",
    "auto_ets",
    "auto_arima",
    "theta",
    "decomposed_bu",
    "decomposed_mint",
}

# Tier-1 arms with no tuning at all; the robust-target variant is applied to the
# three best of these on dev (plan §4.3, row R).
UNTUNED_TIER1 = [
    "naive_mean_12",
    "naive_median_12",
    "random_walk",
    "seasonal_naive",
    "prophet_shipped",
    "holt_winters_shipped",
]

ARMS: dict[str, dict] = {
    "naive_mean_12": {"family": "baseline", "tier": 1, "windowed": False},
    "naive_median_12": {"family": "baseline", "tier": 1, "windowed": False},
    "random_walk": {"family": "baseline", "tier": 1, "windowed": False},
    "seasonal_naive": {"family": "baseline", "tier": 1, "windowed": False},
    "drift": {"family": "baseline", "tier": 1, "windowed": True},
    "prophet_shipped": {"family": "incumbent", "tier": 1, "windowed": False},
    "auto_ets": {"family": "classical", "tier": 1, "windowed": True},
    "auto_arima": {"family": "classical", "tier": 1, "windowed": True},
    "theta": {"family": "classical", "tier": 1, "windowed": True},
    "holt_winters_shipped": {"family": "classical", "tier": 1, "windowed": False},
    "decomposed_bu": {"family": "structural", "tier": 2, "windowed": True},
    "decomposed_mint": {"family": "structural", "tier": 2, "windowed": True},
    "ridge_direct": {"family": "tabular_ml", "tier": 2, "windowed": False},
    "lgbm_direct": {"family": "tabular_ml", "tier": 2, "windowed": False},
    "combo_classical": {"family": "combination", "tier": 2, "windowed": False},
}


def complexity_rank(arm_id: str) -> float:
    """Plan §7 complexity rank; robust variants sit just above their base arm."""
    base = arm_id
    robust = False
    if arm_id.startswith("robust__"):
        base = arm_id.split("__", 1)[1]
        robust = True
    rank = BASE_RANK.get(base)
    if rank is None:
        base_arm = base.split("|", 1)[0]
        rank = BASE_RANK.get(base_arm, 99.0)
    return rank + (0.5 if robust else 0.0)


# ── Shared helpers ───────────────────────────────────────────────────────────


def _future_months(train: pd.Series, periods: int) -> pd.DatetimeIndex:
    start = train.index[-1] + pd.DateOffset(months=1)
    return pd.date_range(start=start, periods=periods, freq="MS")


def _frame(
    months: pd.DatetimeIndex,
    point,
    method: str,
    lower=None,
    upper=None,
) -> pd.DataFrame:
    point_values = np.asarray(point, dtype=float)
    if point_values.ndim == 0:
        point_values = np.full(len(months), float(point_values))
    n = len(months)
    if point_values.shape[0] != n:
        padded = np.full(n, np.nan)
        k = min(n, point_values.shape[0])
        padded[:k] = point_values[:k]
        point_values = padded

    def _pad(values):
        if values is None:
            return np.full(n, np.nan)
        arr = np.asarray(values, dtype=float).ravel()
        if arr.shape[0] == n:
            return arr
        out = np.full(n, np.nan)
        k = min(n, arr.shape[0])
        out[:k] = arr[:k]
        return out

    return pd.DataFrame(
        {
            "month": months,
            "point_estimate": point_values,
            "ci80_lower": _pad(lower),
            "ci80_upper": _pad(upper),
            "method": method,
        }
    )[_OUT_COLUMNS]


def _skip_frame(train: pd.Series, periods: int, method: str) -> pd.DataFrame:
    months = _future_months(train, periods)
    return _frame(months, np.full(periods, np.nan), method)


def windowed(train: pd.Series, window: int | None) -> pd.Series:
    """Apply a sliding training window (``None`` = expanding)."""
    if window is None:
        return train
    return train.iloc[-int(window) :]


def clean_target(
    train: pd.Series,
    window: int = 12,
    z_threshold: float = 3.5,
    min_periods: int = 6,
) -> pd.Series:
    """
    Replace extreme training values with the rolling median (plan §4.3, R).

    Modified z-score ``0.6745 * (y - median) / MAD`` over a centred rolling
    window. Values above ``z_threshold`` are replaced by the window median.
    Only the training target is cleaned; actuals never are. A level shift is
    not an outlier: the rolling median follows it, so the z-score stays small.
    """
    y = train.astype(float)
    median = y.rolling(window, center=True, min_periods=min_periods).median()
    mad = (y - median).abs().rolling(window, center=True, min_periods=min_periods).median()
    z = 0.6745 * (y - median) / mad.replace(0.0, np.nan)
    flags = z.abs() > z_threshold
    cleaned = y.copy()
    cleaned[flags] = median[flags]
    return cleaned


def _safe_float(value) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return out if math.isfinite(out) else float("nan")


# ── Baselines ────────────────────────────────────────────────────────────────


def naive_mean_12_forecaster(train: pd.Series, periods: int) -> pd.DataFrame:
    """Production trailing-12-month mean (B1), narrowed to the arm contract."""
    result = naive_mean_forecaster(train, periods)
    return _frame(
        _future_months(train, periods),
        result["point_estimate"].to_numpy(dtype=float),
        "naive_mean",
    )


def naive_median_forecaster(train: pd.Series, periods: int) -> pd.DataFrame:
    """Trailing-12-month median, held flat (robust to heavy tails)."""
    tail = train.iloc[-12:] if len(train) >= 12 else train
    point = float(np.median(tail.to_numpy(dtype=float)))
    return _frame(_future_months(train, periods), point, "naive_median")


def random_walk_forecaster(train: pd.Series, periods: int) -> pd.DataFrame:
    """Last observed value, held flat."""
    point = float(train.iloc[-1])
    return _frame(_future_months(train, periods), point, "random_walk")


def seasonal_naive_forecaster(train: pd.Series, periods: int) -> pd.DataFrame:
    """``y[t-12]`` repeated; falls back to the last value when history is short."""
    values = train.to_numpy(dtype=float)
    if len(values) >= 12:
        point = np.full(periods, values[-12])
    else:
        point = np.full(periods, values[-1])
    return _frame(_future_months(train, periods), point, "seasonal_naive")


def make_drift(window: int | None = None):
    """Random walk plus the average historical slope over the training window."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        hist = windowed(train, window).to_numpy(dtype=float)
        if len(hist) < 2:
            raise ArmSkipped("drift needs two observations")
        slope = (hist[-1] - hist[0]) / (len(hist) - 1)
        steps = np.arange(1, periods + 1, dtype=float)
        point = hist[-1] + slope * steps
        return _frame(_future_months(train, periods), point, f"drift_w{window or 'exp'}")

    return _forecast


# ── Classical arms ───────────────────────────────────────────────────────────


def _fit_ets(
    y: pd.Series,
    trend: str | None,
    seasonal: str | None,
    damped: bool = False,
    log: bool = False,
):
    from statsmodels.tsa.exponential_smoothing.ets import ETSModel

    values = np.log(y.astype(float)) if log else y.astype(float)
    model = ETSModel(
        values,
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


def _select_ets(y: pd.Series, allow_seasonal: bool = True):
    """Pick the ETS specification with the lowest in-sample AICc."""
    candidates = [(None, None, False), ("add", None, False), ("add", None, True)]
    if allow_seasonal and len(y) >= 24:
        candidates += [
            (None, "add", False),
            ("add", "add", False),
            ("add", "add", True),
        ]
    best = None
    best_aicc = np.inf
    for trend, seasonal, damped in candidates:
        try:
            fit = _fit_ets(y, trend, seasonal, damped=damped)
            aicc = _safe_float(fit.aicc)
        except Exception:
            continue
        if np.isfinite(aicc) and aicc < best_aicc:
            best_aicc = aicc
            best = (fit, trend, seasonal, damped)
    if best is None:
        raise ArmSkipped("no ETS specification converged")
    return best


def _ets_forecast(train: pd.Series, periods: int, label: str) -> pd.DataFrame:
    fit, trend, seasonal, damped = _select_ets(train)
    point = np.asarray(fit.forecast(periods), dtype=float)
    lower = upper = None
    try:
        pred = fit.get_prediction(start=len(train), end=len(train) + periods - 1)
        frame = pred.summary_frame(alpha=0.2)
        lower = frame["pi_lower"].to_numpy(dtype=float)
        upper = frame["pi_upper"].to_numpy(dtype=float)
    except Exception:
        pass
    tag = f"{trend or 'none'}{'-damped' if damped else ''}/{(seasonal or 'none')}"
    return _frame(_future_months(train, periods), point, f"{label}:{tag}", lower, upper)


def make_auto_ets(window: int | None = None):
    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        hist = windowed(train, window)
        if len(hist) < 8:
            raise ArmSkipped("auto_ets needs at least 8 months")
        return _ets_forecast(hist, periods, f"auto_ets_w{window or 'exp'}")

    return _forecast


def make_auto_arima(window: int | None = None):
    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        import pmdarima as pm

        hist = windowed(train, window)
        if len(hist) < 24:
            raise ArmSkipped("auto_arima needs at least 24 months")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = pm.auto_arima(
                hist.astype(float),
                m=12,
                seasonal=True,
                information_criterion="aicc",
                stepwise=True,
                suppress_warnings=True,
                error_action="ignore",
                start_p=1,
                start_q=1,
                max_p=2,
                max_q=2,
                max_P=1,
                max_Q=1,
                max_d=2,
                max_D=1,
                maxiter=50,
                random=False,
            )
            point, interval = model.predict(n_periods=periods, return_conf_int=True, alpha=0.2)
        tag = f"{model.order}x{model.seasonal_order}"
        return _frame(
            _future_months(hist, periods),
            np.asarray(point, dtype=float),
            f"auto_arima_w{window or 'exp'}:{tag}",
            np.asarray(interval, dtype=float)[:, 0],
            np.asarray(interval, dtype=float)[:, 1],
        )

    return _forecast


def make_theta(window: int | None = None):
    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        from statsmodels.tsa.forecasting.theta import ThetaModel

        hist = windowed(train, window)
        if len(hist) < 24:
            raise ArmSkipped("theta needs at least 24 months")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = ThetaModel(
                hist.astype(float), period=12, deseasonalize=True, use_test=True
            )
            fit = model.fit()
            point = np.asarray(fit.forecast(periods), dtype=float)
            interval = fit.prediction_intervals(alpha=0.2)
        return _frame(
            _future_months(hist, periods),
            point,
            f"theta_w{window or 'exp'}",
            np.asarray(interval.iloc[:, 0], dtype=float),
            np.asarray(interval.iloc[:, 1], dtype=float),
        )

    return _forecast


def holt_winters_shipped(train: pd.Series, periods: int) -> pd.DataFrame:
    """Production Holt-Winters, exactly as shipped."""
    result = forecast_holtwinters(train, periods=periods)
    return _frame(
        _future_months(train, periods),
        result["point_estimate"].to_numpy(dtype=float),
        "holt_winters_shipped",
        result["ci80_lower"].to_numpy(dtype=float),
        result["ci80_upper"].to_numpy(dtype=float),
    )


def prophet_shipped(train: pd.Series, periods: int) -> pd.DataFrame:
    """Production method selector (Prophet at ≥ 24 months), exactly as shipped."""
    result = select_and_run_forecast(train, periods=periods)
    return _frame(
        _future_months(train, periods),
        result["point_estimate"].to_numpy(dtype=float),
        "prophet_shipped",
        result.get("ci80_lower", pd.Series([np.nan] * len(result))).to_numpy(dtype=float),
        result.get("ci80_upper", pd.Series([np.nan] * len(result))).to_numpy(dtype=float),
    )


# ── Structural arms ──────────────────────────────────────────────────────────


def _component_forecast(series: pd.Series, periods: int, use_log: bool) -> tuple[np.ndarray, np.ndarray]:
    """ETS forecast of one strictly positive component, with bias adjustment.

    Returns ``(point_forecast, in_sample_residuals)``.
    """
    fit, _, _, _ = _select_ets(series, allow_seasonal=len(series) >= 24)
    residuals = np.asarray(fit.resid, dtype=float)
    if use_log:
        raw = np.asarray(fit.forecast(periods), dtype=float)
        sigma = np.full(periods, np.nan)
        try:
            pred = fit.get_prediction(start=len(series), end=len(series) + periods - 1)
            frame = pred.summary_frame(alpha=0.2)
            sigma = (frame["pi_upper"] - frame["pi_lower"]).to_numpy(dtype=float) / (
                2 * 1.2815515655446004
            )
        except Exception:
            finite = residuals[np.isfinite(residuals)]
            if finite.size:
                sigma = np.full(periods, float(finite.std(ddof=1)))
        sigma = np.where(np.isfinite(sigma), sigma, 0.0)
        point = np.exp(raw + sigma**2 / 2.0)
    else:
        point = np.asarray(fit.forecast(periods), dtype=float)
    return point, residuals


def make_decomposed_bu(window: int | None = None, dep_log: bool = False, wd_log: bool = False):
    """
    Bottom-up: forecast deposits and withdrawals separately, savings = difference.

    The raw/log choice per component is a dev-tuned switch (plan §4.3, D1).
    """

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        components = getattr(_forecast, "components", None)
        if components is None:
            raise ArmSkipped("components not wired")
        hist_dep = windowed(components["deposits"].reindex(train.index), window)
        hist_wd = windowed(components["withdrawals"].reindex(train.index), window)
        if (hist_dep <= 0).any() or (hist_wd <= 0).any():
            raise ArmSkipped("decomposed_bu needs strictly positive components")
        dep, _ = _component_forecast(hist_dep, periods, bool(dep_log))
        wd, _ = _component_forecast(hist_wd, periods, bool(wd_log))
        return _frame(
            _future_months(train, periods),
            dep - wd,
            f"decomposed_bu_w{window or 'exp'}_dep{'log' if dep_log else 'raw'}"
            f"_wd{'log' if wd_log else 'raw'}",
        )

    _forecast.components = None
    return _forecast


def _schafer_strimmer(residuals: np.ndarray) -> np.ndarray:
    """
    Shrink the sample covariance toward its diagonal (Schäfer–Strimmer).

    ``W = (1 - lam) * S + lam * diag(S)`` with the standard intensity estimate.
    """
    n, k = residuals.shape
    centered = residuals - residuals.mean(axis=0, keepdims=True)
    sample = np.cov(centered, rowvar=False, ddof=1)
    sample = np.atleast_2d(sample)
    target = np.diag(np.diag(sample))
    if k == 1:
        return sample
    var_off = 0.0
    sq_off = 0.0
    for i in range(k):
        for j in range(k):
            if i == j:
                continue
            var_off += (sample[i, i] * sample[j, j]) / n
            sq_off += sample[i, j] ** 2
    lam = 1.0 if sq_off <= 0 else float(np.clip(var_off / sq_off, 0.0, 1.0))
    return (1.0 - lam) * sample + lam * target


def _mint_reconcile(base: np.ndarray, weight: np.ndarray, summing: np.ndarray) -> np.ndarray:
    """MinT: ``S (S' W^-1 S)^-1 S' W^-1`` applied to the base forecasts."""
    inv_weight = np.linalg.pinv(weight)
    inner = summing.T @ inv_weight @ summing
    gain = summing @ np.linalg.pinv(inner) @ summing.T @ inv_weight
    return gain @ base


def make_decomposed_mint(window: int | None = None):
    """Base forecasts for {net, deposits, withdrawals}, reconciled with MinT."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        components = getattr(_forecast, "components", None)
        if components is None:
            raise ArmSkipped("components not wired")
        hist_net = windowed(train, window)
        hist_dep = windowed(components["deposits"].reindex(train.index), window)
        hist_wd = windowed(components["withdrawals"].reindex(train.index), window)
        if (hist_dep <= 0).any() or (hist_wd <= 0).any():
            raise ArmSkipped("decomposed_mint needs strictly positive components")

        fits = []
        residuals = []
        for hist in (hist_net, hist_dep, hist_wd):
            fit, _, _, _ = _select_ets(hist, allow_seasonal=len(hist) >= 24)
            fits.append(np.asarray(fit.forecast(periods), dtype=float))
            resid = pd.Series(np.asarray(fit.resid, dtype=float), index=hist.index).dropna()
            residuals.append(resid)

        common = residuals[0].index
        for resid in residuals[1:]:
            common = common.intersection(resid.index)
        if len(common) < 8:
            raise ArmSkipped("too few aligned residuals for MinT")
        matrix = np.column_stack([resid.loc[common].to_numpy() for resid in residuals])
        weight = _schafer_strimmer(matrix)

        summing = np.array([[1.0, -1.0], [1.0, 0.0], [0.0, 1.0]])
        base = np.vstack(fits)
        reconciled = _mint_reconcile(base, weight, summing)
        return _frame(
            _future_months(train, periods),
            reconciled[0, :],
            f"decomposed_mint_w{window or 'exp'}",
        )

    _forecast.components = None
    return _forecast


# ── Tabular ML arms ──────────────────────────────────────────────────────────


def _fourier_columns(month_of_year: np.ndarray, k: int) -> dict[str, np.ndarray]:
    columns = {}
    angle = 2 * np.pi * (month_of_year - 1) / 12.0
    for order in range(1, k + 1):
        columns[f"sin{order}"] = np.sin(order * angle)
        columns[f"cos{order}"] = np.cos(order * angle)
    return columns


def build_ml_design(
    train: pd.Series,
    dep: pd.Series | None = None,
    wd: pd.Series | None = None,
    fourier_k: int = 0,
    scale: bool = False,
) -> pd.DataFrame:
    """
    Lagged design matrix at every month ``t`` of ``train`` (plan §4.4).

    Every predictor is known at ``t``: lags and rolling statistics are built on
    ``y.shift(1)``, deposits/withdrawals enter at lag 1, and the Fourier terms
    describe the target month. Nothing reads past the series it is given, so
    the same function is safe at an origin.

    Returns
    -------
    pd.DataFrame
        Index = months of ``train``; columns are predictors plus ``level12``,
        ``scale``, ``target_month``. Rows with any missing predictor stay NaN.
    """
    y = train.astype(float)
    lagged = y.shift(1)
    features = pd.DataFrame(index=y.index)

    for lag in (1, 2, 3, 6, 12):
        features[f"lag{lag}"] = y.shift(lag)
    for span in (3, 6, 12):
        features[f"rmean{span}"] = lagged.rolling(span).mean()
        features[f"rmed{span}"] = lagged.rolling(span).median()
        features[f"rstd{span}"] = lagged.rolling(span).std()
    for span in (3, 12):
        features[f"ewma{span}"] = lagged.ewm(span=span, min_periods=1).mean()

    features["level12"] = y.rolling(12).mean()
    features["scale"] = lagged.abs().rolling(12).mean()

    if dep is not None:
        features["dep_lag1"] = dep.reindex(y.index).shift(1)
    if wd is not None:
        features["wd_lag1"] = wd.reindex(y.index).shift(1)

    month_of_year = y.index.month.to_numpy()
    features["target_month"] = month_of_year
    for name, values in _fourier_columns(month_of_year, fourier_k).items():
        features[name] = values

    features["__y"] = y
    return features


def _level_columns(columns) -> list[str]:
    """Predictors that carry the level, i.e. get divided by ``s_t`` when scaling."""
    return [column for column in columns if not column.startswith(("sin", "cos", "target_month"))]


def _scale_block(frame: pd.DataFrame, divisor: pd.Series) -> pd.DataFrame:
    """Divide the level predictors by a per-row scale (plan §4.4 scale option)."""
    scaled = frame.copy()
    columns = _level_columns(scaled.columns)
    scaled[columns] = scaled[columns].div(divisor, axis=0)
    return scaled


def training_rows(
    features: pd.DataFrame,
    horizon: int,
    scale: bool,
    min_rows: int = 24,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Direct-strategy rows for one horizon: target ``y[t+h] - level12[t]``.

    Only rows with ``t + h`` inside the training index are used, so no target
    ever sits beyond the last training month.
    """
    level = features["level12"]
    target = features["__y"].shift(-horizon) - level
    keep = target.notna() & np.isfinite(target)
    # ``scale`` is the per-row divisor, not a predictor; ``target_month`` is
    # represented by the Fourier pairs.
    predictors = [
        column
        for column in features.columns
        if column not in ("__y", "target_month", "scale")
    ]
    design = features.loc[keep, predictors].copy()
    y = target.loc[keep].copy()

    if scale:
        divisor = features.loc[keep, "scale"].replace(0.0, np.nan)
        divisor = divisor.fillna(features.loc[keep, "scale"].abs().median() or 1.0)
        design = _scale_block(design, divisor)
        y = y / divisor

    valid = design.notna().all(axis=1) & np.isfinite(y)
    design = design.loc[valid]
    y = y.loc[valid]
    if len(design) < min_rows:
        raise ArmSkipped(f"needs {min_rows} training rows for h={horizon}, got {len(design)}")
    return design, y


def _origin_row(features: pd.DataFrame, predictors: list[str], scale: bool) -> np.ndarray:
    """Predictor row for the last training month, scaled exactly like training rows."""
    origin = features.iloc[[-1]][predictors].copy()
    if scale:
        divisor = float(features.iloc[-1]["scale"])
        if not np.isfinite(divisor) or divisor == 0.0:
            divisor = 1.0
        origin = _scale_block(origin, divisor)
    return origin.to_numpy(dtype=float).ravel()


def _ml_forecast(
    train: pd.Series,
    periods: int,
    dep: pd.Series | None,
    wd: pd.Series | None,
    fourier_k: int,
    scale: bool,
    fit_predict,
    label: str,
) -> pd.DataFrame:
    features = build_ml_design(train, dep=dep, wd=wd, fourier_k=fourier_k, scale=scale)
    origin = features.iloc[[-1]]
    origin_level = float(origin["level12"].iloc[0])
    if not np.isfinite(origin_level):
        raise ArmSkipped("origin level is not finite")
    origin_scale = 1.0
    if scale:
        origin_scale = float(origin["scale"].iloc[0])
        if not np.isfinite(origin_scale) or origin_scale == 0.0:
            origin_scale = 1.0

    point = np.full(periods, np.nan)
    for horizon in range(1, periods + 1):
        design, target = training_rows(features, horizon, scale)
        predictors = list(design.columns)
        x_origin = _origin_row(features, predictors, scale)
        if not np.isfinite(x_origin).all():
            raise ArmSkipped("origin predictors are not finite")
        prediction = float(fit_predict(design, target, x_origin))
        point[horizon - 1] = prediction * (origin_scale if scale else 1.0) + origin_level

    return _frame(_future_months(train, periods), point, label)


def make_ridge_direct(
    alpha: float = 1.0,
    fourier_k: int = 0,
    scale: bool = False,
    components=None,
):
    """One Ridge model per horizon, on the standardised design matrix."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        from sklearn.linear_model import Ridge
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        def _fit(design, target, x_origin):
            model = make_pipeline(StandardScaler(), Ridge(alpha=float(alpha)))
            model.fit(design.to_numpy(dtype=float), target.to_numpy(dtype=float))
            return model.predict(x_origin.reshape(1, -1))[0]

        return _ml_forecast(
            train,
            periods,
            None if components is None else components.get("deposits"),
            None if components is None else components.get("withdrawals"),
            fourier_k,
            scale,
            _fit,
            f"ridge_direct_a{alpha}_K{fourier_k}_{'scaled' if scale else 'raw'}",
        )

    return _forecast


def make_lgbm_direct(
    num_leaves: int = 4,
    min_data_in_leaf: int = 5,
    n_estimators: int = 100,
    fourier_k: int = 0,
    scale: bool = False,
    components=None,
):
    """One LightGBM model per horizon, deterministic, no early stopping."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        import lightgbm as lgb

        def _fit(design, target, x_origin):
            model = lgb.LGBMRegressor(
                num_leaves=int(num_leaves),
                min_data_in_leaf=int(min_data_in_leaf),
                n_estimators=int(n_estimators),
                learning_rate=0.05,
                subsample=0.8,
                subsample_freq=1,
                colsample_bytree=0.8,
                deterministic=True,
                force_row_wise=True,
                random_state=SEED,
                n_jobs=1,
                verbose=-1,
            )
            model.fit(design.to_numpy(dtype=float), target.to_numpy(dtype=float))
            return model.predict(x_origin.reshape(1, -1))[0]

        return _ml_forecast(
            train,
            periods,
            None if components is None else components.get("deposits"),
            None if components is None else components.get("withdrawals"),
            fourier_k,
            scale,
            _fit,
            f"lgbm_direct_l{num_leaves}_m{min_data_in_leaf}_n{n_estimators}"
            f"_K{fourier_k}_{'scaled' if scale else 'raw'}",
        )

    return _forecast


# ── Combination and preprocessing variants ───────────────────────────────────


def make_combo_classical(components=None, windows: dict | None = None):
    """
    Equal-weight mean of C1, C2 and C3 point forecasts (composition fixed).

    Each component uses its own dev-chosen training window (plan §4.2, E1).
    """
    windows = windows or {}
    members = {
        "auto_ets": make_auto_ets(windows.get("auto_ets")),
        "auto_arima": make_auto_arima(windows.get("auto_arima")),
        "theta": make_theta(windows.get("theta")),
    }

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        points = []
        for member in members.values():
            try:
                frame = member(train, periods)
            except ArmSkipped:
                continue
            values = frame["point_estimate"].to_numpy(dtype=float)
            if np.isfinite(values).all():
                points.append(values)
        if not points:
            raise ArmSkipped("no combination member produced a forecast")
        return _frame(_future_months(train, periods), np.mean(np.vstack(points), axis=0), "combo_classical")

    return _forecast


def make_robust(base_id: str, base_callable, enabled: bool = True):
    """Wrap an arm with the training-target outlier clean (plan §4.3, R)."""

    def _forecast(train: pd.Series, periods: int) -> pd.DataFrame:
        hist = clean_target(train) if enabled else train
        frame = base_callable(hist, periods)
        frame = frame.copy()
        frame["method"] = f"robust_{'on' if enabled else 'off'}__{base_id}"
        return frame

    return _forecast


# ── Counting wrapper and registry ────────────────────────────────────────────


class ArmRecorder:
    """
    Wrap a forecaster, count failures/skips and time the fits.

    ``rolling_origin_backtest`` concatenates whatever comes back, so a failed
    origin must return a NaN frame instead of raising.
    """

    def __init__(self, arm_id: str, forecaster):
        self.arm_id = arm_id
        self.forecaster = forecaster
        self.failures = 0
        self.skips = 0
        self.seconds = 0.0
        self.calls = 0

    def __call__(self, train: pd.Series, periods: int) -> pd.DataFrame:
        import time

        start = time.perf_counter()
        self.calls += 1
        try:
            result = self.forecaster(train, periods)
        except ArmSkipped:
            self.skips += 1
            result = _skip_frame(train, periods, self.arm_id)
        except Exception:
            self.failures += 1
            result = _skip_frame(train, periods, self.arm_id)
        finally:
            self.seconds += time.perf_counter() - start
        return result

    @property
    def mean_seconds(self) -> float:
        return self.seconds / self.calls if self.calls else float("nan")

    def stats(self) -> dict:
        return {
            "arm": self.arm_id,
            "calls": self.calls,
            "failures": self.failures,
            "skips": self.skips,
            "mean_seconds": self.mean_seconds,
            "total_seconds": self.seconds,
        }


def make_arm(
    arm_id: str,
    components: dict | None = None,
    window: int | None = None,
    dep_log: bool = False,
    wd_log: bool = False,
    alpha: float = 1.0,
    num_leaves: int = 4,
    min_data_in_leaf: int = 5,
    n_estimators: int = 100,
    fourier_k: int = 0,
    scale: bool = False,
    robust: bool = False,
    robust_base: str | None = None,
    combo_windows: dict | None = None,
):
    """Build one configured arm; ``arm_id`` is the registry key."""
    payload = components or {}

    if arm_id == "naive_mean_12":
        return naive_mean_12_forecaster
    if arm_id == "naive_median_12":
        return naive_median_forecaster
    if arm_id == "random_walk":
        return random_walk_forecaster
    if arm_id == "seasonal_naive":
        return seasonal_naive_forecaster
    if arm_id == "drift":
        return make_drift(window)
    if arm_id == "prophet_shipped":
        return prophet_shipped
    if arm_id == "holt_winters_shipped":
        return holt_winters_shipped
    if arm_id == "auto_ets":
        return make_auto_ets(window)
    if arm_id == "auto_arima":
        return make_auto_arima(window)
    if arm_id == "theta":
        return make_theta(window)
    if arm_id == "decomposed_bu":
        arm = make_decomposed_bu(window, dep_log=dep_log, wd_log=wd_log)
        arm.components = payload
        return arm
    if arm_id == "decomposed_mint":
        arm = make_decomposed_mint(window)
        arm.components = payload
        return arm
    if arm_id == "ridge_direct":
        return make_ridge_direct(alpha, fourier_k, scale, components=payload)
    if arm_id == "lgbm_direct":
        return make_lgbm_direct(
            num_leaves, min_data_in_leaf, n_estimators, fourier_k, scale, components=payload
        )
    if arm_id == "combo_classical":
        return make_combo_classical(components=payload, windows=combo_windows)

    if arm_id.startswith("robust__"):
        base_id = arm_id.split("__", 1)[1]
        base = make_arm(base_id, components=payload, window=window, robust_base=robust_base)
        return make_robust(base_id, base, enabled=robust)

    raise KeyError(f"unknown arm: {arm_id}")


def build_registry(components: dict | None = None, **options) -> dict:
    """``{arm_id: callable}`` for every arm, with ``options`` applied globally."""
    return {
        arm_id: ArmRecorder(arm_id, make_arm(arm_id, components=components, **options))
        for arm_id in ARMS
    }
