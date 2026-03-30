"""
Cash flow forecasting using method selection by data length.

Selects the appropriate method automatically:
  >= 24 months → Prophet (with 80% and 95% CIs)
  12–23 months → Holt-Winters Exponential Smoothing (bootstrapped CIs)
  < 12 months  → 3-month rolling mean (no CI, indicative only)

Usage:
    from src.analysis.cashflow_forecast import select_and_run_forecast, compute_runway
    forecast_df = select_and_run_forecast(monthly_net_cashflow)
    runway = compute_runway(current_balance, forecast_df)
"""

import numpy as np
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


# ── Individual forecast methods ───────────────────────────────────────────────


def forecast_prophet(
    monthly_cashflow: pd.DataFrame,
    periods: int = 12,
) -> pd.DataFrame:
    """
    Forecast monthly net cash flow using Facebook Prophet.

    Parameters
    ----------
    monthly_cashflow : pd.DataFrame
        Must have columns 'ds' (datetime, monthly) and 'y' (net cash flow).
    periods : int
        Months ahead to forecast.

    Returns
    -------
    pd.DataFrame
        Columns: month, point_estimate, ci80_lower, ci80_upper,
                 ci95_lower, ci95_upper, method.

    Notes
    -----
    Assumptions:
    1. Income sources remain approximately stable
    2. No major one-off events (job loss, inheritance, etc.)
    3. Spending patterns continue current trajectory
    4. No structural breaks in the forecast period
    """
    from prophet import Prophet  # lazy import — optional dependency

    # Parameter rationale (verified by seasonality_tests.run_seasonality_battery):
    #   yearly_seasonality=False  — 0/5 tests detected significant yearly seasonality
    #   seasonality_mode="additive" — no amplitude/level scaling observed (Spearman ρ<0.5)
    #   seasonality_prior_scale=1.0 — tighter than the 10.0 default; with no clear
    #                                  seasonality, prevents the model fitting noise as
    #                                  seasonal structure
    #   changepoint_prior_scale=0.1 — default 0.05 underfits structural breaks (e.g. new
    #                                  job, rent changes); 0.1 gives more trend flexibility
    #   changepoint_range=0.9       — allows trend changes in the last 10% of history
    #                                  (was 20%); captures recent shifts in spending level
    #   uncertainty_samples=500     — smoother CI edges vs 300; still fast on monthly data
    m = Prophet(
        weekly_seasonality=False,
        daily_seasonality=False,
        yearly_seasonality=False,
        seasonality_mode="additive",
        seasonality_prior_scale=1.0,
        interval_width=0.95,  # wide interval; we'll slice narrower percentiles below
        changepoint_prior_scale=0.1,
        changepoint_range=0.9,
        uncertainty_samples=500,
    )
    m.add_country_holidays(country_name="PL")
    m.fit(monthly_cashflow)
    future = m.make_future_dataframe(periods=periods, freq="MS")
    forecast = m.predict(future)

    # Draw raw posterior samples to compute multiple interval widths at once.
    # Prophet returns shape (n_time_points, n_samples) — rows=time, cols=samples.
    raw = m.predictive_samples(future)  # dict with key "yhat", shape (n_rows, n_samples)
    samples = raw["yhat"]  # numpy array (n_rows, uncertainty_samples)

    n_historical = len(monthly_cashflow)
    result = forecast[["ds", "yhat"]].iloc[n_historical:].copy()
    result = result.rename(columns={"ds": "month", "yhat": "point_estimate"})

    future_samples = samples[n_historical:, :]  # (periods, n_samples)
    result["ci80_lower"] = np.percentile(future_samples, 10, axis=1)
    result["ci80_upper"] = np.percentile(future_samples, 90, axis=1)
    result["ci95_lower"] = np.percentile(future_samples, 2.5, axis=1)
    result["ci95_upper"] = np.percentile(future_samples, 97.5, axis=1)
    result["method"] = "Prophet"
    return result.reset_index(drop=True)


def forecast_holtwinters(
    monthly_values: pd.Series,
    periods: int = 12,
    n_bootstrap: int = 500,
) -> pd.DataFrame:
    """
    Forecast using Holt-Winters Exponential Smoothing with bootstrapped CIs.

    Parameters
    ----------
    monthly_values : pd.Series
        Monthly net cash flow with DatetimeIndex (monthly frequency).
    periods : int
        Months ahead to forecast.
    n_bootstrap : int
        Bootstrap draws for CI estimation.

    Returns
    -------
    pd.DataFrame
        Columns: month, point_estimate, ci80_lower, ci80_upper,
                 ci95_lower, ci95_upper, method.

    Notes
    -----
    Assumptions:
    1. Level, trend, and seasonal components are stable
    2. Residuals are exchangeable (can be resampled)
    3. No structural breaks in the forecast period
    """
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    n = len(monthly_values)
    use_seasonal = n >= 24
    seasonal_periods = 12 if use_seasonal else None

    model = ExponentialSmoothing(
        monthly_values,
        trend="add",
        seasonal="add" if use_seasonal else None,
        seasonal_periods=seasonal_periods,
    ).fit(optimized=True)

    point_forecast = model.forecast(periods)
    residuals = model.resid

    rng = np.random.default_rng(42)
    simulations = np.zeros((n_bootstrap, periods))
    for i in range(n_bootstrap):
        noise = rng.choice(residuals.values, size=periods, replace=True)
        simulations[i, :] = point_forecast.values + noise

    future_dates = pd.date_range(
        start=monthly_values.index[-1] + pd.DateOffset(months=1),
        periods=periods,
        freq="MS",
    )

    return pd.DataFrame({
        "month": future_dates,
        "point_estimate": point_forecast.values,
        "ci80_lower": np.percentile(simulations, 10, axis=0),
        "ci80_upper": np.percentile(simulations, 90, axis=0),
        "ci95_lower": np.percentile(simulations, 2.5, axis=0),
        "ci95_upper": np.percentile(simulations, 97.5, axis=0),
        "method": "Holt-Winters",
    })


def forecast_rolling_mean(
    monthly_values: pd.Series,
    periods: int = 12,
    window: int = 3,
) -> pd.DataFrame:
    """
    Naive forecast using N-month rolling mean. No confidence interval.

    Used only when < 12 months of history are available.

    Parameters
    ----------
    monthly_values : pd.Series
        Monthly net cash flow with DatetimeIndex.
    periods : int
        Months ahead to forecast.
    window : int
        Rolling window size.

    Returns
    -------
    pd.DataFrame
        Columns: month, point_estimate, ci80_lower, ci80_upper,
                 ci95_lower, ci95_upper, note, method.
        CI columns are NaN — forecast is indicative only.
    """
    point_estimate = monthly_values.rolling(window).mean().iloc[-1]

    future_dates = pd.date_range(
        start=monthly_values.index[-1] + pd.DateOffset(months=1),
        periods=periods,
        freq="MS",
    )

    return pd.DataFrame({
        "month": future_dates,
        "point_estimate": [point_estimate] * periods,
        "ci80_lower": [float("nan")] * periods,
        "ci80_upper": [float("nan")] * periods,
        "ci95_lower": [float("nan")] * periods,
        "ci95_upper": [float("nan")] * periods,
        "note": ["Insufficient history for CI estimation. Forecast is indicative only."] * periods,
        "method": "rolling_mean",
    })


# ── Method selector ───────────────────────────────────────────────────────────


def select_and_run_forecast(
    monthly_net_cashflow: pd.Series,
    periods: int = 12,
) -> pd.DataFrame:
    """
    Select and run the appropriate forecast method based on data length.

    Selection logic:
      >= 24 months → Prophet
      12–23 months → Holt-Winters
      < 12 months  → rolling mean (no CI; indicative only)

    Parameters
    ----------
    monthly_net_cashflow : pd.Series
        Monthly net cash flow with DatetimeIndex (monthly frequency).
    periods : int
        Months ahead to forecast.

    Returns
    -------
    pd.DataFrame
        Columns: month, point_estimate, ci80_lower, ci80_upper,
                 ci95_lower, ci95_upper, method.
    """
    n = len(monthly_net_cashflow)

    if n >= 24:
        logger.info(f"Forecast method: Prophet ({n} months of history)")
        prophet_df = pd.DataFrame({
            "ds": monthly_net_cashflow.index,
            "y": monthly_net_cashflow.values,
        })
        return forecast_prophet(prophet_df, periods=periods)
    elif n >= 12:
        logger.info(f"Forecast method: Holt-Winters ({n} months of history)")
        return forecast_holtwinters(monthly_net_cashflow, periods=periods)
    else:
        logger.warning(
            f"Forecast method: rolling mean ({n} months). "
            "Insufficient history for CI. Forecast is indicative only."
        )
        return forecast_rolling_mean(monthly_net_cashflow, periods=periods)


# ── Runway calculation ────────────────────────────────────────────────────────


def compute_runway(
    current_balance: float,
    forecast_df: pd.DataFrame,
) -> dict:
    """
    Compute months of positive cash balance under three scenarios.

    Parameters
    ----------
    current_balance : float
        Current total liquid cash balance.
    forecast_df : pd.DataFrame
        Must have columns: point_estimate, ci80_lower, ci80_upper.

    Returns
    -------
    dict with keys: pessimistic_months, median_months, optimistic_months.
        A value equal to len(forecast_df) means balance stays positive
        throughout the entire forecast window.
    """
    ci_available = forecast_df["ci80_lower"].notna().all()

    scenarios = {
        "pessimistic": forecast_df["ci80_lower"] if ci_available else forecast_df["point_estimate"],
        "median": forecast_df["point_estimate"],
        "optimistic": forecast_df["ci80_upper"] if ci_available else forecast_df["point_estimate"],
    }

    runway = {}
    for name, monthly_cf in scenarios.items():
        cumulative = current_balance + monthly_cf.cumsum()
        runway[f"{name}_months"] = int((cumulative > 0).sum())

    logger.info(
        f"Runway — pessimistic: {runway['pessimistic_months']}m | "
        f"median: {runway['median_months']}m | "
        f"optimistic: {runway['optimistic_months']}m"
    )
    return runway
