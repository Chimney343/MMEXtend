# Finance Statistics: Gold Standards for Personal Financial Forecasting

## Overview

This document catalogues the statistical methods used in each pipeline step, explains when each method is appropriate, states its assumptions, and provides implementation guidance. These are the **gold standard** approaches for personal (not institutional) financial forecasting.

---

## 1. Expense Trend Analysis

### Method: Ordinary Least Squares (OLS) Linear Regression

**What it does:** Fits a straight line through monthly category spend over time.

**When to use:** Category has >= 6 months of data AND coefficient of variation (CV) < 0.4.

**Key outputs:**
| Metric | Interpretation |
|--------|---------------|
| Slope (β₁) | Change in spend per month (in currency units) |
| R² | Proportion of variance explained by time (0–1) |
| p-value (for slope) | Probability the slope is zero. < 0.05 → statistically significant. |

**Classification rules:**
- Rising: slope > 0 AND p < 0.05
- Falling: slope < 0 AND p < 0.05
- Flat: p >= 0.05 (cannot reject null hypothesis of no trend)

**Implementation:**

```python
import statsmodels.api as sm
import pandas as pd
import numpy as np

def fit_category_trend(monthly_spend: pd.Series) -> dict:
    """
    Fit OLS trend to monthly category spend.

    Parameters
    ----------
    monthly_spend : pd.Series
        Monthly spend values, indexed by month ordinal (0, 1, 2, ...).

    Returns
    -------
    dict with keys: slope, intercept, r_squared, p_value, n_months, trend_class,
                    ci_slope_lower, ci_slope_upper
    """
    n = len(monthly_spend)
    if n < 6:
        return {"trend_class": "insufficient_data", "n_months": n}

    X = sm.add_constant(np.arange(n))  # [1, 0], [1, 1], ..., [1, n-1]
    model = sm.OLS(monthly_spend.values, X).fit()

    slope = model.params[1]
    p_value = model.pvalues[1]
    ci = model.conf_int(alpha=0.05)  # 95% CI for slope

    if p_value < 0.05:
        trend_class = "rising" if slope > 0 else "falling"
    else:
        trend_class = "flat"

    return {
        "slope": slope,
        "intercept": model.params[0],
        "r_squared": model.rsquared,
        "p_value": p_value,
        "n_months": n,
        "trend_class": trend_class,
        "ci_slope_lower": ci[1][0],  # 95% CI lower bound for slope
        "ci_slope_upper": ci[1][1],  # 95% CI upper bound for slope
    }
```

**Confidence interval for the slope:** The OLS model provides a 95% CI for the slope coefficient. Report this alongside the point estimate. This tells the user: "the monthly increase is between X and Y with 95% confidence."

**Assumptions (must be stated in output):**
1. Spend is approximately linear over time (no structural breaks)
2. Residuals are approximately normally distributed
3. No extreme outliers dominating the fit
4. Monthly observations are independent (may be violated if recurring subscriptions dominate)

---

### Method: Seasonality Detection via Coefficient of Variation

**What it does:** Flags categories where spend varies dramatically by month-of-year.

**Metric:** CV = σ / μ across month-of-year group means.

**Threshold:** CV > 0.4 → "highly seasonal." Linear trend is unreliable; recommend STL decomposition or manual review.

```python
def check_seasonality(monthly_spend: pd.DataFrame, date_col: str, value_col: str) -> dict:
    """
    Check for seasonality using month-of-year grouping.

    Parameters
    ----------
    monthly_spend : DataFrame with columns [date_col, value_col]
    """
    monthly_spend = monthly_spend.copy()
    monthly_spend["month_of_year"] = pd.to_datetime(monthly_spend[date_col]).dt.month

    group_means = monthly_spend.groupby("month_of_year")[value_col].mean()
    cv = group_means.std() / group_means.mean() if group_means.mean() != 0 else float("inf")

    return {
        "cv": cv,
        "is_seasonal": cv > 0.4,
        "month_means": group_means.to_dict(),
    }
```

---

### Method: MoM Delta and Consecutive Rise Detection

**What it does:** Flags categories with spend increasing > 20% MoM for 2+ consecutive months.

```python
def detect_consecutive_rises(monthly_series: pd.Series, threshold: float = 0.20, consecutive: int = 2) -> list:
    """
    Find periods where MoM % increase exceeds threshold for N consecutive months.

    Returns list of (start_month_idx, end_month_idx) tuples.
    """
    pct_change = monthly_series.pct_change()
    rising = (pct_change > threshold).astype(int)

    # Find runs of consecutive rises
    runs = []
    run_start = None
    run_length = 0

    for i, val in enumerate(rising):
        if val == 1:
            if run_start is None:
                run_start = i
            run_length += 1
        else:
            if run_length >= consecutive:
                runs.append((run_start, i - 1))
            run_start = None
            run_length = 0

    if run_length >= consecutive:
        runs.append((run_start, len(rising) - 1))

    return runs
```

---

## 2. Cash Flow Forecasting

### Method Selection Logic

| History Available | Method | CI Approach |
|-------------------|--------|-------------|
| >= 24 months | **Prophet** | Built-in uncertainty intervals (80%, 95%) |
| 12–23 months | **Holt-Winters Exponential Smoothing** | Bootstrapped residuals (500 draws) |
| < 12 months | **3-month rolling mean** | No CI (state explicitly: "indicative only") |

---

### Method: Prophet (>= 24 months)

**What it does:** Decomposes time series into trend + yearly seasonality + residuals. Produces probabilistic forecasts with uncertainty intervals.

**Configuration:**

```python
from prophet import Prophet
import pandas as pd

def forecast_prophet(monthly_cashflow: pd.DataFrame, periods: int = 12) -> pd.DataFrame:
    """
    Forecast monthly net cash flow using Prophet.

    Parameters
    ----------
    monthly_cashflow : DataFrame
        Must have columns 'ds' (date) and 'y' (net cash flow value).
    periods : int
        Number of months to forecast.

    Returns
    -------
    DataFrame with columns: ds, yhat, yhat_lower_80, yhat_upper_80,
                            yhat_lower_95, yhat_upper_95
    """
    # Prophet with sensible defaults for monthly personal finance
    model = Prophet(
        weekly_seasonality=False,     # Monthly data, no weekly signal
        daily_seasonality=False,
        yearly_seasonality=True,      # Capture annual patterns (bonuses, holidays)
        seasonality_mode="additive",  # Multiplicative if income varies a lot
        interval_width=0.80,          # 80% CI by default
        changepoint_prior_scale=0.05, # Conservative: personal finance is stable
    )

    model.fit(monthly_cashflow)

    future = model.make_future_dataframe(periods=periods, freq="MS")
    forecast_80 = model.predict(future)

    # Re-run for 95% CI
    model_95 = Prophet(
        weekly_seasonality=False,
        daily_seasonality=False,
        yearly_seasonality=True,
        seasonality_mode="additive",
        interval_width=0.95,
        changepoint_prior_scale=0.05,
    )
    model_95.fit(monthly_cashflow)
    forecast_95 = model_95.predict(future)

    result = forecast_80[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    result = result.rename(columns={
        "yhat_lower": "ci80_lower",
        "yhat_upper": "ci80_upper",
    })
    result["ci95_lower"] = forecast_95["yhat_lower"]
    result["ci95_upper"] = forecast_95["yhat_upper"]

    # Only return forecast periods (not historical)
    n_historical = len(monthly_cashflow)
    result = result.iloc[n_historical:].reset_index(drop=True)
    result = result.rename(columns={"ds": "month", "yhat": "point_estimate"})

    return result
```

**Assumptions (must be stated):**
1. Income sources remain approximately stable
2. No major one-off events (job loss, inheritance, etc.)
3. Spending patterns continue current trajectory
4. No structural breaks in the forecast period

---

### Method: Holt-Winters Exponential Smoothing (12–23 months)

**What it does:** Captures level, trend, and seasonal components. More robust than Prophet with limited data.

```python
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import numpy as np
import pandas as pd

def forecast_holtwinters(
    monthly_values: pd.Series,
    periods: int = 12,
    n_bootstrap: int = 500,
) -> pd.DataFrame:
    """
    Forecast using Holt-Winters with bootstrapped CIs.

    Parameters
    ----------
    monthly_values : Series
        Monthly net cash flow values with DatetimeIndex (monthly freq).
    periods : int
        Months to forecast.
    n_bootstrap : int
        Number of bootstrap simulations for CI estimation.

    Returns
    -------
    DataFrame with: month, point_estimate, ci80_lower, ci80_upper,
                    ci95_lower, ci95_upper
    """
    n = len(monthly_values)
    seasonal_periods = min(12, n // 2)  # Need at least 2 full cycles

    model = ExponentialSmoothing(
        monthly_values,
        trend="add",
        seasonal="add" if n >= 24 else None,
        seasonal_periods=seasonal_periods if n >= 24 else None,
    ).fit(optimized=True)

    point_forecast = model.forecast(periods)
    residuals = model.resid

    # Bootstrap: simulate future paths by resampling residuals
    simulations = np.zeros((n_bootstrap, periods))
    for i in range(n_bootstrap):
        noise = np.random.choice(residuals, size=periods, replace=True)
        simulations[i, :] = point_forecast.values + noise

    ci80_lower = np.percentile(simulations, 10, axis=0)
    ci80_upper = np.percentile(simulations, 90, axis=0)
    ci95_lower = np.percentile(simulations, 2.5, axis=0)
    ci95_upper = np.percentile(simulations, 97.5, axis=0)

    future_dates = pd.date_range(
        start=monthly_values.index[-1] + pd.DateOffset(months=1),
        periods=periods,
        freq="MS",
    )

    return pd.DataFrame({
        "month": future_dates,
        "point_estimate": point_forecast.values,
        "ci80_lower": ci80_lower,
        "ci80_upper": ci80_upper,
        "ci95_lower": ci95_lower,
        "ci95_upper": ci95_upper,
    })
```

---

### Method: Rolling Mean (< 12 months)

```python
def forecast_rolling_mean(monthly_values: pd.Series, periods: int = 12, window: int = 3) -> pd.DataFrame:
    """
    Naive forecast using 3-month rolling mean. NO confidence interval.
    Used only when < 12 months of history.
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
        "ci80_lower": [float("nan")] * periods,  # Explicitly NaN
        "ci80_upper": [float("nan")] * periods,
        "ci95_lower": [float("nan")] * periods,
        "ci95_upper": [float("nan")] * periods,
        "note": ["Insufficient history for CI estimation"] * periods,
    })
```

---

## 3. Runway Calculation

**What it does:** Given a cash balance and forecast net cash flow scenarios, compute how many months until the balance hits zero.

```python
def compute_runway(
    current_balance: float,
    forecast_df: pd.DataFrame,
) -> dict:
    """
    Compute months of runway at pessimistic, median, optimistic scenarios.

    Parameters
    ----------
    current_balance : float
        Current total cash balance.
    forecast_df : DataFrame
        Must have columns: point_estimate, ci80_lower, ci80_upper.

    Returns
    -------
    dict with keys: pessimistic_months, median_months, optimistic_months
    """
    scenarios = {
        "pessimistic": forecast_df["ci80_lower"] if forecast_df["ci80_lower"].notna().all() else forecast_df["point_estimate"],
        "median": forecast_df["point_estimate"],
        "optimistic": forecast_df["ci80_upper"] if forecast_df["ci80_upper"].notna().all() else forecast_df["point_estimate"],
    }

    runway = {}
    for name, monthly_cf in scenarios.items():
        cumulative = current_balance + monthly_cf.cumsum()
        months_positive = (cumulative > 0).sum()
        runway[f"{name}_months"] = int(months_positive)

    return runway
```

---

## 4. Net Worth Trajectory

### Method: Same as Cash Flow

Uses the same method selection logic (Prophet / Holt-Winters / rolling mean) based on months of history.

**Additional decomposition (if investment accounts present):**

```python
def decompose_networth_change(
    net_worth_series: pd.Series,
    savings_contribution_series: pd.Series,
) -> pd.DataFrame:
    """
    Decompose net worth change into:
      - Savings contribution (income - expenditure flowing into accounts)
      - Market movement (residual = total change - savings contribution)

    IMPORTANT: This is a rough decomposition. "Market movement" includes
    unrealised gains, dividends, fees, and any measurement error.
    """
    total_change = net_worth_series.diff()
    market_movement = total_change - savings_contribution_series

    return pd.DataFrame({
        "month": net_worth_series.index,
        "total_change": total_change,
        "savings_contribution": savings_contribution_series,
        "market_movement": market_movement,
    })
```

**Assumptions (must be stated):**
1. Account balances reflect mark-to-market valuations (user must confirm)
2. Savings contribution equals net cash flow (no off-book transfers)
3. "Market movement" is a residual — it includes fees, dividends, and measurement error

---

## 5. Summary: When to Use What

| Question | Method | Min Data | CI Method |
|----------|--------|----------|-----------|
| Is category spend trending up? | OLS linear regression | 6 months | OLS slope CI |
| Is a category seasonal? | CV of month-of-year means | 12 months | N/A (descriptive) |
| Category spend spiking? | MoM % change, consecutive rise | 3 months | N/A (rule-based flag) |
| 12-month cash flow forecast? | Prophet | 24 months | Prophet intervals |
| 12-month cash flow forecast? | Holt-Winters | 12 months | Bootstrapped residuals |
| 12-month cash flow forecast? | Rolling mean | < 12 months | None (state explicitly) |
| Months of runway? | Cumulative sum under scenarios | Depends on forecast method | Derived from forecast CIs |
| Net worth trajectory? | Same as cash flow | 6 months (trend), 12+ (forecast) | Same as cash flow |
| Savings vs market split? | Residual decomposition | 6 months | None (point estimate only) |

---

## 6. What This Pipeline Does NOT Do

These are outside scope. Document them so no one expects them:

- **Monte Carlo simulation of portfolio returns.** Requires asset-level data and return distributions.
- **Tax optimisation modelling.** Requires jurisdiction-specific tax rules.
- **Scenario analysis with assumed market returns.** We never project investment returns using assumed rates.
- **Budget optimisation.** We detect trends and signals; we don't prescribe budgets.
- **Peer benchmarking.** We have no reference dataset of other people's finances.
