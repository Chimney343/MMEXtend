# Finance Statistics: Gold Standards for Personal Financial Forecasting

## Overview

This document catalogues the statistical methods used in each pipeline step, explains when each method is appropriate, states its assumptions, and provides implementation guidance. These are the **gold standard** approaches for personal (not institutional) financial forecasting.

---

## 1. Expense Trend Analysis

### Method: OLS trend with Newey-West (HAC) standard errors, on a zero-filled month grid

**What it does:** Fits a straight line through monthly category spend over time. The monthly axis is the category's continuous calendar grid: from its first complete month to the last complete month in the input, with months that had no spend zero-filled. The x axis is the `month_ordinal` already stored upstream (one month = +1), so the dashboard trend line `intercept + slope * month_ordinal` matches the fit.

**When to use:** Category has >= 6 months of data. There is no coefficient-of-variation gate; zero-filled months are the honest way to represent inactivity.

**Key outputs:**
| Metric | Interpretation |
|--------|---------------|
| Slope (β₁) | Change in spend per month (in currency units) |
| R² | Proportion of variance explained by time (0–1) |
| p-value (for slope) | The probability of a slope at least this far from zero if the true slope were zero. It is not the probability that the trend is real. |
| q-value (`trend_pvalue_adj`) | Benjamini–Hochberg adjusted p-value across categories (FDR 0.10) |

**Classification rules:**
- Rising: slope > 0 and q < 0.10
- Falling: slope < 0 and q < 0.10
- Flat: q >= 0.10 (cannot reject the null at the family-wise FDR)
- Insufficient data: the fitted grid is shorter than 6 months

**Standard errors:** Monthly observations are serially correlated, so OLS standard errors are too small. The p-value and 95% CI come from `fit(cov_type="HAC", cov_kwds={"maxlags": max(1, floor(4 · (n/100)^(2/9)))})`. The point estimates are unchanged; only the uncertainty is honest.

**Multiple comparisons:** Screening 31 categories at α = 0.05 produces false labels by construction. Labels use Benjamini–Hochberg with `FDR_Q = 0.10`.

```python
from src.analysis.stats_utils import FDR_Q, bh_adjust, fit_linear_trend

fit = fit_linear_trend(y, x=month_ordinal)   # slope, intercept, r_squared,
                                             # p_value (HAC), ci_lower/upper, n
q = bh_adjust(p_values)                      # NaN-safe, index-preserving
```

**Confidence interval for the slope:** Report it alongside the point estimate. It is a HAC interval: "the monthly increase is between X and Y, allowing for residual autocorrelation."

**Assumptions (must be stated in output):**
1. Spend is approximately linear over time (no structural breaks)
2. Residuals are approximately normally distributed
3. No extreme outliers dominating the fit
4. Residual autocorrelation is handled by the HAC covariance, not assumed away

---

### Method: Seasonality Detection via Kruskal–Wallis on trend residuals

**What it does:** Flags categories whose spend pattern differs by month-of-year after the linear trend is removed. Testing residuals avoids mistaking a rising trend for seasonality.

**Method:** For grids of at least 24 months, run `scipy.stats.kruskal` over month-of-year groups of the Phase 2 OLS residuals; the raw p-value is `seasonality_pvalue`. Adjust across categories with Benjamini–Hochberg and label `is_seasonal = seasonality_pvalue_adj < 0.10`.

`check_seasonality` remains a descriptive helper returning `cv` and `month_means` only. The CV is not a calibrated test: noise series routinely exceed CV 0.4.

```python
from src.analysis.expense_trends import month_of_year_kruskal

p_raw = month_of_year_kruskal(residuals)   # DatetimeIndex, month-start
```

**Consecutive rises:** the MoM "consecutive rise" flag was removed. On pure noise series it fired 100% of the time at the median, and no calibrated replacement exists.

---

## 2. Cash Flow Forecasting

### Method Selection Logic

| History Available | Method | Interval Approach |
|-------------------|--------|-------------------|
| >= 24 months | **Trailing 12-month average** | Residual-bootstrap band, replaced by backtest-derived bands once >= 10 rolling origins exist |
| 12–23 months | **Holt-Winters Exponential Smoothing** | Bootstrapped residuals (500 draws) |
| < 12 months | **3-month rolling mean** | No interval (state explicitly: "indicative only") |

Training series excludes the month containing the run date: the current month
is still accruing, so it is reported as an actual but never fitted.

---

### Method: Trailing 12-month average (>= 24 months)

**What it does:** Sets every future month to the mean of the last 12 observed
months, and derives intervals by resampling the residuals of those 12 months
around that level. It is deliberately a flat forecast: it carries no trend or
seasonal term.

**Why this method:** A rolling-origin backtest over the same history compared
this average with fitted alternatives (exponential smoothing, ARIMA, Theta,
decomposition, gradient boosting, Prophet) and no method beat it on held-out
origins. It is also the simplest option, so it is what ships.

**Point forecast:**

```python
from src.analysis.forecast_backtest import trailing_mean
point = trailing_mean(monthly_values, window=12)   # mean of the last 12 months
```

The same helper backs the baseline used in the backtest, so the shipped
forecast and the baseline cannot diverge.

**Intervals:** the residuals of the trailing window are resampled with a fixed
seed; the 10/90 and 2.5/97.5 percentiles give the 80% and 95% bands, and the
cumulative bands are percentiles of the summed simulated paths. These are the
model's own intervals. Step 8b of the pipeline replaces them with
backtest-derived bands as soon as at least 10 rolling origins exist, so the
`interval_method` column reads `empirical_backtest_nN` in a normal run.

**Assumptions (must be stated):**
1. The recent 12-month level persists over the forecast window
2. No trend or seasonal pattern is worth modelling separately
3. No structural breaks in the forecast period

---

### Method: Prophet (category forecasts)

Prophet remains the model for **category-level** forecasts
(`src/analysis/category_prophet.py`), where a per-category trend and yearly
shape are the point of the exercise. It is no longer used for the total
savings/cash-flow line.

Tuned parameters shared by every Prophet fit (from `build_prophet_model`):

```python
from prophet import Prophet
import numpy as np
import pandas as pd

def forecast_prophet(monthly_cashflow: pd.DataFrame, periods: int = 12) -> pd.DataFrame:
    """
    Forecast a monthly category total using Prophet.

    Parameters
    ----------
    monthly_cashflow : DataFrame
        Must have columns 'ds' (date) and 'y' (net cash flow value).
    periods : int
        Number of months to forecast.

    Returns
    -------
    DataFrame with columns: month, point_estimate,
                            ci80_lower, ci80_upper,
                            ci95_lower, ci95_upper,
                            cum_point, cum_ci80_lower, cum_ci80_upper,
                            cum_ci95_lower, cum_ci95_upper,
                            interval_method, method.
    """
    # Parameter rationale:
    #   yearly_seasonality=False  — seasonality tests are mostly negative on monthly cash flow
    #   seasonality_mode="additive" — no amplitude/level scaling
    #   seasonality_prior_scale=1.0 — tighter than default 10.0; prevents fitting noise
    #   changepoint_prior_scale=0.1 — more trend flexibility than default 0.05 to
    #                                  capture structural breaks (new job, rent changes)
    #   changepoint_range=0.9       — allow trend changes in the last 10% of history
    #   uncertainty_samples=500     — smoother CI edges vs 300; fast on monthly data
    m = Prophet(
        weekly_seasonality=False,
        daily_seasonality=False,
        yearly_seasonality=False,
        seasonality_mode="additive",
        seasonality_prior_scale=1.0,
        interval_width=0.95,
        changepoint_prior_scale=0.1,
        changepoint_range=0.9,
        uncertainty_samples=500,
    )
    # No country holidays: monthly ds values fall on the 1st, so New Year,
    # 1 May and All Saints' Day act as January/May/November dummies rather
    # than holiday effects.
    m.fit(monthly_cashflow)
    future = m.make_future_dataframe(periods=periods, freq="MS")
    forecast = m.predict(future)

    # Draw posterior samples to compute both 80% and 95% intervals in one pass.
    raw = m.predictive_samples(future)   # {"yhat": ndarray (n_rows, n_samples)}
    samples = raw["yhat"]

    n_historical = len(monthly_cashflow)
    result = forecast[["ds", "yhat"]].iloc[n_historical:].copy()
    result = result.rename(columns={"ds": "month", "yhat": "point_estimate"})

    future_samples = samples[n_historical:, :]
    result["ci80_lower"] = np.percentile(future_samples, 10, axis=1)
    result["ci80_upper"] = np.percentile(future_samples, 90, axis=1)
    result["ci95_lower"] = np.percentile(future_samples, 2.5, axis=1)
    result["ci95_upper"] = np.percentile(future_samples, 97.5, axis=1)

    # Cumulative bands: percentiles of the summed posterior paths. Summing the
    # monthly quantiles would assume perfect correlation and understate the band.
    cumulative = np.cumsum(future_samples, axis=0)
    result["cum_point"] = np.cumsum(result["point_estimate"].to_numpy())
    result["cum_ci80_lower"] = np.percentile(cumulative, 10, axis=1)
    result["cum_ci80_upper"] = np.percentile(cumulative, 90, axis=1)
    result["cum_ci95_lower"] = np.percentile(cumulative, 2.5, axis=1)
    result["cum_ci95_upper"] = np.percentile(cumulative, 97.5, axis=1)
    result["interval_method"] = "model"
    result["method"] = "Prophet"
    return result.reset_index(drop=True)
```

**Assumptions for the category Prophet fits (must be stated):**
1. A category's recent income/spend pattern is approximately stable
2. No major one-off events (job loss, inheritance, etc.)
3. Spending patterns continue current trajectory
4. No structural breaks in the forecast period

---

### Backtest and Empirical Bands

The pipeline replays the shipped forecaster over historical cut-offs and
replaces its own intervals with bands estimated from those errors. A model's
self-reported interval is not evidence that it covers reality.

**Rolling-origin backtest** (`src/analysis/forecast_backtest.py`):

- Origins start at 36 complete months and step by 3, each forecasting 24 months.
- Only the shipped forecaster is replayed. Since the shipped method is itself
  the trailing 12-month average, replaying a separate naive arm would duplicate
  it, so the backtest's purpose here is the bands and the reported error.
- Output (`forecast_backtest.parquet`): one row per origin × horizon step with
  `actual`, `point`, `cum_actual`, `cum_point`, `ci80_lower`, `ci80_upper`, and
  `forecaster` (always `shipped`).

**Reported numbers** (`summarise_backtest`):

| Figure | Meaning |
|--------|---------|
| `mae_month` | Mean absolute monthly error |
| `abs_cum_err_12` | Mean absolute 12-month cumulative error |
| `model_ci80_coverage_month` | Share of months inside the model's own 80% band |
| `holdout_cum80_k / m` | Hits / trials of the backtest-derived cumulative band on the last third of origins |

The holdout split is fixed: the first two-thirds of origins estimate the
cumulative quantiles, the last third counts hits. It is a report, not a tuning
target.

**Empirical bands** (`empirical_bands`): monthly `point + quantile(actual − point, [0.1, 0.9])`
and cumulative `cum_point + quantile(cum_actual − cum_point, [0.1, 0.9])` per
horizon step. The 95% bands are NaN: too few origins to estimate 2.5/97.5
percentiles. `interval_method` becomes `empirical_backtest_n<N>`; below
`MIN_ORIGINS_FOR_BANDS = 10` origins the model intervals are kept and the
dashboard labels them "(model, uncalibrated)".

`backtest_cum_low_k` of `backtest_cum_n` counts, per horizon step, the origins
whose cumulative forecast came in below the actual total. A lopsided count
means the point forecast is biased: the band is built from the errors, so it
shifts away from the point and can leave it outside. The savings fan states
the count in its subtitle, and notes when the line runs outside the band.

What to say about the result: "The 80% band for the 12-month cumulative total
is based on the last N backtests, and K of them came in below the actual
total." Do not present the model's raw intervals as calibrated.

---

### Method: Holt-Winters Exponential Smoothing (12–23 months)

**What it does:** Captures level, trend, and seasonal components. Used for medium histories, where the trailing average has too few observations to be stable.

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
        Monthly forecast. Uses ``cum_ci80_lower`` / ``cum_ci80_upper`` when
        present (cumulative quantiles computed per path); otherwise falls back
        to the point estimate. Never sums monthly quantiles.

    Returns
    -------
    dict with keys: pessimistic_months, median_months, optimistic_months
    """
    point = pd.to_numeric(forecast_df["point_estimate"], errors="coerce")
    cumulative_point = current_balance + point.cumsum()

    if {"cum_ci80_lower", "cum_ci80_upper"}.issubset(forecast_df.columns) and (
        forecast_df["cum_ci80_lower"].notna().all()
        and forecast_df["cum_ci80_upper"].notna().all()
    ):
        pessimistic = current_balance + forecast_df["cum_ci80_lower"]
        optimistic = current_balance + forecast_df["cum_ci80_upper"]
    else:
        pessimistic = optimistic = cumulative_point

    return {
        "pessimistic_months": int((pessimistic > 0).sum()),
        "median_months": int((cumulative_point > 0).sum()),
        "optimistic_months": int((optimistic > 0).sum()),
    }
```

---

## 4. Net Worth Trajectory

### Method: Same as Cash Flow

Uses the same method selection logic (trailing 12-month average / Holt-Winters / rolling mean) based on months of history.

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
| Is category spend trending up? | OLS on a zero-filled grid, Newey-West HAC errors | 6 months | HAC slope CI |
| Is a category seasonal? | Kruskal–Wallis on trend residuals, BH-adjusted | 24 months | N/A (test q-value) |
| Category spend spiking? | — (consecutive-rise flag removed) | — | — |
| 12-month cash flow forecast? | Trailing 12-month average | 24 months | Backtest-derived bands (residual bootstrap below 10 origins) |
| 12-month cash flow forecast? | Holt-Winters | 12 months | Bootstrapped residuals |
| 12-month cash flow forecast? | Rolling mean | < 12 months | None (state explicitly) |
| Months of runway? | Cumulative sum under scenarios | Depends on forecast method | Cumulative quantiles per path |
| Net worth trajectory? | Same as cash flow | 6 months (trend), 12+ (forecast) | Same as cash flow |
| Savings vs market split? | Residual decomposition | 6 months | None (point estimate only) |
| Did lifestyle creep eat raises? | Annual marginal spend rate (undefined when pay did not rise) | 2 complete years | None |

---

## 6. What This Pipeline Does NOT Do

These are outside scope. Document them so no one expects them:

- **Monte Carlo simulation of portfolio returns.** Requires asset-level data and return distributions.
- **Tax optimisation modelling.** Requires jurisdiction-specific tax rules.
- **Scenario analysis with assumed market returns.** We never project investment returns using assumed rates.
- **Budget optimisation.** We detect trends and signals; we don't prescribe budgets.
- **Peer benchmarking.** We have no reference dataset of other people's finances.
