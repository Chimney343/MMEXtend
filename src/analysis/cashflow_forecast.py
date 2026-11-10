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


def build_prophet_model(yearly_seasonality: bool = False):
    """
    Construct a Prophet model with the project's tuned parameters.

    Parameter rationale:
      yearly_seasonality=False  — the seasonality battery is 1/5 positive: only
                                  ACF(12) on an *undetrended* series is
                                  significant; STL strength is 0.07 (negligible)
                                  and yearly terms gave no backtest gain
      seasonality_mode="additive" — no amplitude/level scaling observed (Spearman ρ<0.5)
      seasonality_prior_scale=1.0 — tighter than the 10.0 default; with no clear
                                     seasonality, prevents the model fitting noise as
                                     seasonal structure
      changepoint_prior_scale=0.1 — default 0.05 underfits structural breaks (e.g. new
                                     job, rent changes); 0.1 gives more trend flexibility
      changepoint_range=0.9       — allows trend changes in the last 10% of history
                                     (was 20%); captures recent shifts in spending level
      uncertainty_samples=500     — smoother CI edges vs 300; still fast on monthly data

    The other hyperparameters were set without a backtest; only the
    yearly_seasonality choice above was checked against one.

    No country holidays: monthly ``ds`` values fall on the 1st, so New Year,
    1 May and All Saints' Day act as January/May/November dummies rather than
    holiday effects.
    """
    from prophet import Prophet  # lazy import — optional dependency

    return Prophet(
        weekly_seasonality=False,
        daily_seasonality=False,
        yearly_seasonality=yearly_seasonality,
        seasonality_mode="additive",
        seasonality_prior_scale=1.0,
        interval_width=0.95,  # wide interval; we'll slice narrower percentiles below
        changepoint_prior_scale=0.1,
        changepoint_range=0.9,
        uncertainty_samples=500,
    )


def forecast_prophet(
    monthly_cashflow: pd.DataFrame,
    periods: int = 12,
    yearly_seasonality: bool = False,
) -> pd.DataFrame:
    """
    Forecast monthly net cash flow using Facebook Prophet.

    Parameters
    ----------
    monthly_cashflow : pd.DataFrame
        Must have columns 'ds' (datetime, monthly) and 'y' (net cash flow).
    periods : int
        Months ahead to forecast.
    yearly_seasonality : bool
        Whether to enable yearly seasonality. Defaults to False, matching the
        evidence that monthly net cashflow shows no significant yearly seasonality.
        Category-level series (e.g. heating) may warrant True later.

    Returns
    -------
    pd.DataFrame
        Columns: month, point_estimate, ci80_lower, ci80_upper,
                 ci95_lower, ci95_upper, cum_point, cum_ci80_lower,
                 cum_ci80_upper, cum_ci95_lower, cum_ci95_upper,
                 interval_method, method.

    Notes
    -----
    Cumulative bands come from the percentiles of the summed posterior paths,
    not from summing monthly quantiles: summing quantiles assumes perfect
    correlation and understates the band.

    Assumptions:
    1. Income sources remain approximately stable
    2. No major one-off events (job loss, inheritance, etc.)
    3. Spending patterns continue current trajectory
    4. No structural breaks in the forecast period
    """
    m = build_prophet_model(yearly_seasonality=yearly_seasonality)
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

    # Per-path cumulative quantiles: sum each simulated path, then take
    # percentiles of the sums.
    cumulative = np.cumsum(future_samples, axis=0)
    result["cum_point"] = np.cumsum(result["point_estimate"].to_numpy())
    result["cum_ci80_lower"] = np.percentile(cumulative, 10, axis=1)
    result["cum_ci80_upper"] = np.percentile(cumulative, 90, axis=1)
    result["cum_ci95_lower"] = np.percentile(cumulative, 2.5, axis=1)
    result["cum_ci95_upper"] = np.percentile(cumulative, 97.5, axis=1)
    result["interval_method"] = "model"
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
                 ci95_lower, ci95_upper, cum_point, cum_ci80_lower,
                 cum_ci80_upper, cum_ci95_lower, cum_ci95_upper,
                 interval_method, method.

    Notes
    -----
    Cumulative bands come from the percentiles of the summed simulated paths.

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

    # simulations is (n_bootstrap, periods); cumsum along the horizon axis,
    # then percentiles across paths.
    cumulative = np.cumsum(simulations, axis=1)

    return pd.DataFrame(
        {
            "month": future_dates,
            "point_estimate": point_forecast.values,
            "ci80_lower": np.percentile(simulations, 10, axis=0),
            "ci80_upper": np.percentile(simulations, 90, axis=0),
            "ci95_lower": np.percentile(simulations, 2.5, axis=0),
            "ci95_upper": np.percentile(simulations, 97.5, axis=0),
            "cum_point": np.cumsum(point_forecast.values),
            "cum_ci80_lower": np.percentile(cumulative, 10, axis=0),
            "cum_ci80_upper": np.percentile(cumulative, 90, axis=0),
            "cum_ci95_lower": np.percentile(cumulative, 2.5, axis=0),
            "cum_ci95_upper": np.percentile(cumulative, 97.5, axis=0),
            "interval_method": "model",
            "method": "Holt-Winters",
        }
    )


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
        CI columns are NaN — forecast is indicative only. Cumulative bands are
        NaN too: there is no residual sample to bootstrap.
    """
    point_estimate = monthly_values.rolling(window).mean().iloc[-1]

    future_dates = pd.date_range(
        start=monthly_values.index[-1] + pd.DateOffset(months=1),
        periods=periods,
        freq="MS",
    )

    return pd.DataFrame(
        {
            "month": future_dates,
            "point_estimate": [point_estimate] * periods,
            "ci80_lower": [float("nan")] * periods,
            "ci80_upper": [float("nan")] * periods,
            "ci95_lower": [float("nan")] * periods,
            "ci95_upper": [float("nan")] * periods,
            "cum_point": [float(point_estimate) * (i + 1) for i in range(periods)],
            "cum_ci80_lower": [float("nan")] * periods,
            "cum_ci80_upper": [float("nan")] * periods,
            "cum_ci95_lower": [float("nan")] * periods,
            "cum_ci95_upper": [float("nan")] * periods,
            "interval_method": "model",
            "note": ["Insufficient history for CI estimation. Forecast is indicative only."]
            * periods,
            "method": "rolling_mean",
        }
    )


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
        prophet_df = pd.DataFrame(
            {
                "ds": monthly_net_cashflow.index,
                "y": monthly_net_cashflow.values,
            }
        )
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
        Monthly forecast. When ``cum_ci80_lower`` / ``cum_ci80_upper`` are
        present they are used directly; summing monthly quantiles would
        assume perfect correlation and overstate the band. Otherwise the
        point estimate alone is used.

    Returns
    -------
    dict with keys: pessimistic_months, median_months, optimistic_months.
        A value equal to len(forecast_df) means balance stays positive
        throughout the entire forecast window.
    """
    point = pd.to_numeric(forecast_df["point_estimate"], errors="coerce")
    cumulative_point = current_balance + point.cumsum()

    if {"cum_ci80_lower", "cum_ci80_upper"}.issubset(forecast_df.columns) and (
        forecast_df["cum_ci80_lower"].notna().all()
        and forecast_df["cum_ci80_upper"].notna().all()
    ):
        pessimistic_balance = current_balance + pd.to_numeric(
            forecast_df["cum_ci80_lower"], errors="coerce"
        )
        optimistic_balance = current_balance + pd.to_numeric(
            forecast_df["cum_ci80_upper"], errors="coerce"
        )
    else:
        pessimistic_balance = cumulative_point
        optimistic_balance = cumulative_point

    runway = {
        "pessimistic_months": int((pessimistic_balance > 0).sum()),
        "median_months": int((cumulative_point > 0).sum()),
        "optimistic_months": int((optimistic_balance > 0).sum()),
    }

    if current_balance <= 0:
        balance_label = "zero" if current_balance == 0 else f"negative ({current_balance:.2f})"
        runway["note"] = (
            f"Starting balance is {balance_label}. Runway counts months where "
            "the cumulative balance turns positive, not consecutive months of "
            "solvency from the current date."
        )

    logger.info(
        f"Runway — pessimistic: {runway['pessimistic_months']}m | "
        f"median: {runway['median_months']}m | "
        f"optimistic: {runway['optimistic_months']}m"
    )
    return runway


# ── Monte Carlo simulation ────────────────────────────────────────────────────


def run_expenditure_mc(
    monthly_net_cashflow: pd.Series,
    current_balance: float,
    n_simulations: int = 2000,
    horizon_months: int = 12,
    detrend: bool = True,
) -> dict:
    """
    Monte Carlo simulation of future cash balance via bootstrap resampling.

    Parameters
    ----------
    monthly_net_cashflow : pd.Series
        Monthly net cash flow (disposable_income) with DatetimeIndex.
    current_balance : float
        Starting liquid balance for the simulation.
    n_simulations : int
        Number of bootstrap replicates.
    horizon_months : int
        Months to project forward.
    detrend : bool
        If True, fit OLS trend and resample residuals; otherwise resample
        raw values around their mean.

    Returns
    -------
    dict with keys:
        months          – DatetimeIndex of future months
        simulations     – ndarray (horizon_months, n_simulations) of cumulative balances
        prob_negative   – Series P(balance < 0) per future month
        p10, p25, p50, p75, p90 – percentile arrays for fan chart
    """
    import statsmodels.api as sm

    values = monthly_net_cashflow.values.astype(float)
    n = len(values)

    if detrend and n >= 6:
        X = sm.add_constant(np.arange(n, dtype=float))
        ols = sm.OLS(values, X).fit()
        residuals = values - ols.predict(X)
        future_ordinals = n + np.arange(horizon_months, dtype=float)
        trend_projection = ols.params[0] + ols.params[1] * future_ordinals
    else:
        residuals = values - values.mean()
        trend_projection = np.full(horizon_months, values.mean())

    rng = np.random.default_rng(42)
    simulations = np.zeros((horizon_months, n_simulations))
    for i in range(n_simulations):
        sampled = rng.choice(residuals, size=horizon_months, replace=True)
        monthly_cf = trend_projection + sampled
        simulations[:, i] = np.cumsum(monthly_cf) + current_balance

    last_date = pd.Timestamp(monthly_net_cashflow.index[-1])
    future_months = pd.date_range(
        start=last_date + pd.DateOffset(months=1),
        periods=horizon_months,
        freq="MS",
    )

    prob_negative = pd.Series(
        (simulations < 0).mean(axis=1),
        index=future_months,
        name="prob_negative",
    )

    logger.info(
        f"MC simulation: {n_simulations} runs, {horizon_months} months, "
        f"detrend={detrend}, balance={current_balance:.0f}"
    )
    return {
        "months": future_months,
        "simulations": simulations,
        "prob_negative": prob_negative,
        "p10": np.percentile(simulations, 10, axis=1),
        "p25": np.percentile(simulations, 25, axis=1),
        "p50": np.percentile(simulations, 50, axis=1),
        "p75": np.percentile(simulations, 75, axis=1),
        "p90": np.percentile(simulations, 90, axis=1),
    }


# ── Expense reduction scenario ────────────────────────────────────────────────


def compute_expense_reduction_scenario(
    monthly_category_spend: pd.DataFrame,
    category_type_map: dict[str, str],
    reductions: dict[str, float],
    horizon_months: int = 12,
) -> pd.DataFrame:
    """
    Project monthly and cumulative savings from category spending reductions.

    Parameters
    ----------
    monthly_category_spend : pd.DataFrame
        Wide-format DataFrame: index=year_month, columns=categories,
        values=absolute monthly spend.
    category_type_map : dict[str, str]
        Maps category name → "fixed" | "variable".
    reductions : dict[str, float]
        Maps category name → reduction fraction (0–1).
    horizon_months : int
        Months to project the cumulative saving.

    Returns
    -------
    pd.DataFrame
        Columns: month (1..N), delta_monthly, delta_cumulative.
        DataFrame.attrs: total_monthly_saving (float), warnings (list[str]).
    """
    avg_spend = monthly_category_spend.mean(axis=0)
    total_monthly_saving = 0.0
    warnings_list: list[str] = []

    for cat, frac in reductions.items():
        if cat not in avg_spend.index:
            warnings_list.append(f"Category '{cat}' not found in spending data.")
            continue
        cat_type = category_type_map.get(cat, "variable")
        if cat_type == "fixed":
            warnings_list.append(
                f"'{cat}' is classified as fixed — reduction may not be achievable."
            )
        total_monthly_saving += float(avg_spend[cat]) * frac

    months = list(range(1, horizon_months + 1))
    result = pd.DataFrame(
        {
            "month": months,
            "delta_monthly": [total_monthly_saving] * horizon_months,
            "delta_cumulative": [total_monthly_saving * m for m in months],
        }
    )
    result.attrs["total_monthly_saving"] = total_monthly_saving
    result.attrs["warnings"] = warnings_list

    logger.info(
        f"Reduction scenario: {len(reductions)} categories, "
        f"total saving={total_monthly_saving:.0f} PLN/month, horizon={horizon_months}m"
    )
    return result


def months_to_savings_target(result: pd.DataFrame, target: float) -> dict:
    """
    Find how many months until cumulative savings reach a target amount.

    Parameters
    ----------
    result : pd.DataFrame
        Output of compute_expense_reduction_scenario().
    target : float
        Savings target in PLN.

    Returns
    -------
    dict with key "note" (success) or "error" (target unreachable).
    """
    reached = result[result["delta_cumulative"] >= target]
    if reached.empty:
        monthly = result.attrs.get("total_monthly_saving", 0)
        return {
            "error": (
                f"Target of {target:,.0f} PLN not reached within {len(result)} months "
                f"at {monthly:,.0f} PLN/month."
            )
        }
    month_num = int(reached["month"].iloc[0])
    return {"note": f"You will reach {target:,.0f} PLN in {month_num} month(s)."}
