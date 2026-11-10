"""Tests for src.analysis.big_wins."""

import numpy as np
import pytest

from src.analysis.big_wins import (
    BIG_WINS_COLUMNS,
    build_big_wins_scenario,
    monthly_rate,
    project_value,
    rule_of_72_years,
)


def test_project_value_zero_rate_is_lump_plus_linear_contributions():
    series = project_value(1_000.0, 100.0, 0.0, 12)

    assert series.iloc[0] == 1_000.0
    assert series.iloc[12] == pytest.approx(1_000.0 + 100.0 * 12)


def test_project_value_matches_closed_form():
    lump_sum, contribution, annual_rate, months = 1_000.0, 100.0, 0.08, 24
    series = project_value(lump_sum, contribution, annual_rate, months)
    rate = monthly_rate(annual_rate)

    for month in (1, 6, months):
        growth = (1.0 + rate) ** month
        expected = lump_sum * growth + contribution * (growth - 1.0) / rate
        assert series.iloc[month] == pytest.approx(expected, abs=1e-9)


def test_project_value_month_zero_is_the_lump_sum():
    series = project_value(500.0, 100.0, 0.05, 10)
    assert series.iloc[0] == 500.0
    assert len(series) == 11


def test_project_value_rejects_negative_months():
    with pytest.raises(ValueError, match="months"):
        project_value(0.0, 0.0, 0.05, -1)


def test_rule_of_72_years_is_nine_at_eight_percent():
    assert rule_of_72_years(0.08) == pytest.approx(9.0)


def test_rule_of_72_years_none_for_zero_negative_and_none():
    assert rule_of_72_years(0.0) is None
    assert rule_of_72_years(-0.02) is None
    assert rule_of_72_years(None) is None


def test_build_big_wins_scenario_columns_and_nan_rates():
    scenario = build_big_wins_scenario(
        lump_sum=1_000.0,
        monthly=500.0,
        raise_monthly=0.0,
        rate_low=None,
        rate_high=None,
        reserve_rate=None,
        months=12,
    )

    assert list(scenario.columns) == BIG_WINS_COLUMNS
    assert scenario["invested_low"].isna().all()
    assert scenario["invested_high"].isna().all()
    assert scenario["kept_in_reserve"].notna().all()
    assert scenario["month"].tolist() == list(range(13))


def test_build_big_wins_scenario_rejects_low_above_high():
    with pytest.raises(ValueError, match="rate_low"):
        build_big_wins_scenario(0.0, 100.0, 0.0, 0.08, 0.04, None, 12)


@pytest.mark.parametrize(
    ("lump_sum", "monthly", "raise_monthly", "message"),
    [
        (-1.0, 0.0, 0.0, "Lump sum"),
        (0.0, -1.0, 0.0, "Monthly contribution"),
        (0.0, 0.0, -1.0, "Raise contribution"),
    ],
)
def test_build_big_wins_scenario_rejects_negative_inputs(
    lump_sum, monthly, raise_monthly, message
):
    with pytest.raises(ValueError, match=message):
        build_big_wins_scenario(lump_sum, monthly, raise_monthly, None, None, None, 12)


def test_raise_monthly_adds_to_contributions():
    base = build_big_wins_scenario(0.0, 1_000.0, 0.0, None, None, None, 12)
    raised = build_big_wins_scenario(0.0, 1_000.0, 500.0, None, None, None, 12)

    assert base["contributions"].iloc[5] == pytest.approx(5_000.0)
    assert raised["contributions"].iloc[5] == pytest.approx(7_500.0)


def test_reserve_rate_none_compounds_at_zero():
    no_rate = build_big_wins_scenario(1_000.0, 100.0, 0.0, None, None, None, 12)
    zero_rate = build_big_wins_scenario(1_000.0, 100.0, 0.0, None, None, 0.0, 12)

    assert no_rate["kept_in_reserve"].equals(zero_rate["kept_in_reserve"])
    assert no_rate["kept_in_reserve"].iloc[12] == pytest.approx(2_200.0)


def test_build_big_wins_scenario_logs_nothing_unexpected():
    scenario = build_big_wins_scenario(0.0, 100.0, 0.0, 0.02, 0.06, 0.01, 12)
    assert np.isfinite(scenario["invested_low"].iloc[-1])
    assert scenario["invested_low"].iloc[-1] < scenario["invested_high"].iloc[-1]
