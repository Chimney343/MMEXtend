"""
Big Wins scenarios: what moves the needle, at rates the user assumes.

Implements Ramit Sethi's CEO method (``spending-plan-automation.md``
§ "Next $100 Allocation System" and the CEO method): route new income to
investments and let compounding do the work. The Rule of 72 gives a rough
doubling time: ``72 / (annual_rate × 100)`` years.

Framing (binding rules):
- **HR-1:** output is a *scenario at your assumed rates*, not a forecast, and
  the dashboard draws the low/high rates as a band.
- **HR-3:** no default return, inflation rate or benchmark is supplied. The
  caller passes the rates; ``None`` means "not entered".
- **HR-5:** no instrument, product or tax action is named. The skill's
  "Roth IRA / index fund" steps render generically as "Invest".

Usage:
    from src.analysis.big_wins import build_big_wins_scenario, rule_of_72_years
    scenario = build_big_wins_scenario(
        lump_sum=0.0,
        monthly=2_500.0,
        raise_monthly=0.0,
        rate_low=0.04,
        rate_high=0.08,
        reserve_rate=None,
        months=120,
    )
    doubling = rule_of_72_years(0.08)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

BIG_WINS_COLUMNS = [
    "month",
    "contributions",
    "kept_in_reserve",
    "invested_low",
    "invested_high",
]


def monthly_rate(annual_rate: float) -> float:
    """
    Convert an annual rate to the equivalent monthly compounding rate.

    Parameters
    ----------
    annual_rate : float
        Annual rate as a decimal (0.08 for 8%).

    Returns
    -------
    float
        ``(1 + annual_rate) ** (1 / 12) - 1``.
    """
    return (1.0 + annual_rate) ** (1.0 / 12.0) - 1.0


def rule_of_72_years(annual_rate: float | None) -> float | None:
    """
    Years to double at a constant annual rate, using the Rule of 72.

    Parameters
    ----------
    annual_rate : float | None
        Annual rate as a decimal. ``None``, zero or a negative rate has no
        doubling time.

    Returns
    -------
    float | None
        ``72 / (annual_rate × 100)``, or None when the rate is not positive.
    """
    if annual_rate is None or annual_rate <= 0:
        return None
    return 72.0 / (annual_rate * 100.0)


def project_value(
    lump_sum: float, monthly: float, annual_rate: float, months: int
) -> pd.Series:
    """
    Project a lump sum plus end-of-month contributions.

    ``V_t = V_{t−1} × (1 + m) + C`` with ``V_0 = lump_sum`` and
    ``m = monthly_rate(annual_rate)``.

    Parameters
    ----------
    lump_sum : float
        Starting balance.
    monthly : float
        Contribution added at the end of every month.
    annual_rate : float
        Annual rate as a decimal.
    months : int
        Number of months to project.

    Returns
    -------
    pd.Series
        Values for months 0..months, indexed by month number.

    Raises
    ------
    ValueError
        If ``months`` is negative.
    """
    if months < 0:
        raise ValueError("months must be >= 0")
    rate = monthly_rate(annual_rate)
    values = [float(lump_sum)]
    for _ in range(months):
        values.append(values[-1] * (1.0 + rate) + float(monthly))
    return pd.Series(values, index=range(months + 1), dtype=float)


def build_big_wins_scenario(
    lump_sum: float,
    monthly: float,
    raise_monthly: float,
    rate_low: float | None,
    rate_high: float | None,
    reserve_rate: float | None,
    months: int,
) -> pd.DataFrame:
    """
    Build the invest scenario: contributions, reserve, and a low/high band.

    Parameters
    ----------
    lump_sum : float
        Amount available to invest today.
    monthly : float
        Planned monthly investment.
    raise_monthly : float
        Extra monthly take-home routed to investments (raise lever).
    rate_low, rate_high : float | None
        Annual rates as decimals, entered by the user (HR-3). ``None`` leaves
        the matching ``invested_*`` column NaN.
    reserve_rate : float | None
        Annual rate on the reserve account. ``None`` means 0.
    months : int
        Horizon in months.

    Returns
    -------
    pd.DataFrame
        Columns: month, contributions, kept_in_reserve, invested_low,
        invested_high.

    Raises
    ------
    ValueError
        If a lump sum, contribution or raise is negative, the horizon is
        negative, or ``rate_low > rate_high``.
    """
    if lump_sum < 0:
        raise ValueError("Lump sum must be >= 0, got a negative amount.")
    if monthly < 0:
        raise ValueError("Monthly contribution must be >= 0, got a negative amount.")
    if raise_monthly < 0:
        raise ValueError("Raise contribution must be >= 0, got a negative amount.")
    if months < 0:
        raise ValueError("Horizon must be >= 0 months.")
    if rate_low is not None and rate_high is not None and rate_low > rate_high:
        raise ValueError("rate_low must be less than or equal to rate_high.")

    contribution = float(monthly) + float(raise_monthly)
    index = range(months + 1)
    contributions = pd.Series(
        [float(lump_sum) + contribution * month for month in index], index=index, dtype=float
    )
    kept_in_reserve = project_value(
        lump_sum, contribution, 0.0 if reserve_rate is None else reserve_rate, months
    )
    invested_low = (
        project_value(lump_sum, contribution, rate_low, months)
        if rate_low is not None
        else pd.Series(np.nan, index=index, dtype=float)
    )
    invested_high = (
        project_value(lump_sum, contribution, rate_high, months)
        if rate_high is not None
        else pd.Series(np.nan, index=index, dtype=float)
    )

    scenario = pd.DataFrame(
        {
            "month": list(index),
            "contributions": contributions,
            "kept_in_reserve": kept_in_reserve,
            "invested_low": invested_low,
            "invested_high": invested_high,
        }
    )
    logger.info(
        "Big wins scenario: %d months | monthly contribution %.0f | rates low=%s high=%s",
        months,
        contribution,
        rate_low,
        rate_high,
    )
    return scenario[BIG_WINS_COLUMNS]
