"""Runner smoke test for the category forecast bake-off (synthetic only)."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from scripts.experiments import category_forecast_bakeoff as runner


def _synthetic_panel(months: int = 80, keys: int = 4) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "flow_type": "expense",
            "level": "subcategory",
            "category": "C",
            "subcategory": [f"s{i}" for i in range(keys) for _ in range(months)],
            "is_unassigned": False,
        }
    )
    frame["month"] = list(pd.date_range("2020-01-01", periods=months, freq="MS")) * keys
    rng = np.random.default_rng(3)
    values = []
    for i in range(keys):
        base = rng.integers(0, 3, size=months).astype(float)
        values.extend(base * 10.0)
    frame["y"] = values
    frame["series_key"] = frame.apply(
        lambda row: f"expense|subcategory|C|{row['subcategory']}", axis=1
    )
    return frame


def _config(tmp_path: Path) -> dict:
    return {
        "meta": {
            "experiment": "category-forecast-bakeoff",
            "output_dir": str(tmp_path / "out"),
            "frozen": False,
        },
        "data": {"featured": "unused.parquet"},
        "split": {
            "initial": 24,
            "step": 2,
            "horizon_primary": 12,
            "horizon_secondary": 24,
            "test_fraction": 1 / 3,
            "min_months_for_forecast": 12,
        },
        "bands": {"alpha": 0.2, "min_prior_errors": 4},
        "metrics": {"mase_season": 12, "primary_metric": "AvgRelCum12"},
        "decision": {
            "relcum12_max": 1.0,
            "parsimony_margin": 0.05,
            "stratum_limit": 1.1,
            "phantom_margin": 0.01,
            "bias_margin": 0.02,
            "band_coverage": [0.65, 0.95],
            "baselines": {"denominator": "naive_mean_12", "bias_reference": "naive_mean_12"},
            "complexity_order": [
                ["naive_mean_12"],
                ["naive_mean_w"],
                ["seasonal_naive"],
                ["croston", "tsb"],
            ],
        },
        "cost": {"workers": 1, "probe_pairs": 4, "prophet_budget_seconds": 1e9, "tier12_budget_seconds": 1e9},
        "arms": {
            "naive_mean_12": {"variants": [{}]},
            "croston": {"variants": [{"variant": "sba", "alpha": 0.1}]},
        },
    }


def test_runner_refuses_test_phase_until_frozen(tmp_path, monkeypatch):
    config = _config(tmp_path)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    monkeypatch.setattr(runner, "CONFIG_PATH", config_path)
    monkeypatch.setattr(runner, "load_panel", lambda cfg: _synthetic_panel())
    with pytest.raises(SystemExit):
        runner.test_phase(runner.load_config(config_path), Path(config["meta"]["output_dir"]))


def test_runner_split_is_purged_and_embargoed():
    panel = _synthetic_panel(months=80)
    config = _config(Path("."))
    split = runner.make_split(panel, config)
    assert split["dev"] and split["test"]
    first_test_target = split["test"][0] + pd.DateOffset(months=1)
    for origin in split["dev"]:
        assert origin + pd.DateOffset(months=12) < first_test_target
    assert not (set(split["dev"]) & set(split["test"]))
    assert not (set(split["embargo"]) & set(split["test"]))


def test_runner_backtest_and_leaderboard_on_synthetic_panel(tmp_path, monkeypatch):
    panel = _synthetic_panel(months=80)
    config = _config(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    monkeypatch.setattr(runner, "CONFIG_PATH", config_path)

    split = runner.make_split(panel, config)
    backtests = {}
    for spec in (
        {"arm_id": "naive_mean_12", "params": {}},
        {"arm_id": "croston", "params": {"variant": "sba", "alpha": 0.1}},
    ):
        bt, stats = runner.run_arm(
            spec, panel, config, out_dir, "dev", 12, split["dev"]
        )
        assert stats["fallbacks"] >= 0
        backtests[spec["arm_id"]] = bt
    assert not backtests["naive_mean_12"].empty
    board = runner.build_leaderboard(backtests, panel, config, split["dev"], 12)
    assert set(board["arm"]) == {"naive_mean_12", "croston"}
    naive = board[board["arm"] == "naive_mean_12"].iloc[0]
    assert naive["avg_rel_cum12"] == pytest.approx(1.0)


def test_decision_rule_falls_back_to_naive_when_nothing_eligible():
    config = _config(Path("."))
    leaderboard = pd.DataFrame(
        [
            {
                "arm": "naive_mean_12",
                "avg_rel_cum12": 1.0,
                "dev_avg_rel_cum12": 1.0,
                "money_ratio": 1.0,
                "phantom_spend": 0.0,
                "pooled_bias": 0.0,
            },
            {
                "arm": "croston",
                "avg_rel_cum12": 1.4,
                "dev_avg_rel_cum12": 1.2,
                "money_ratio": 1.1,
                "phantom_spend": 0.0,
                "pooled_bias": 0.0,
            },
        ]
    )
    strata = pd.DataFrame(
        [{"arm": "naive_mean_12", "stratum": "flow_level", "avg_rel": 1.0}]
    )
    verdict = runner.apply_decision_rule(leaderboard, strata, config)
    assert verdict["chosen"] == "naive_mean_12"
    assert verdict["step"] == "§7.1 eligibility"


def test_decision_rule_parsimony_prefers_simpler_arm():
    config = _config(Path("."))
    leaderboard = pd.DataFrame(
        [
            {
                "arm": "naive_mean_12",
                "avg_rel_cum12": 0.98,
                "dev_avg_rel_cum12": 0.99,
                "money_ratio": 0.99,
                "phantom_spend": 0.0,
                "pooled_bias": 0.0,
            },
            {
                "arm": "croston",
                "avg_rel_cum12": 0.95,
                "dev_avg_rel_cum12": 0.96,
                "money_ratio": 0.99,
                "phantom_spend": 0.0,
                "pooled_bias": 0.0,
            },
        ]
    )
    strata = pd.DataFrame(
        [
            {"arm": "naive_mean_12", "stratum": "flow_level", "avg_rel": 0.98},
            {"arm": "croston", "stratum": "flow_level", "avg_rel": 0.95},
        ]
    )
    verdict = runner.apply_decision_rule(leaderboard, strata, config)
    assert verdict["chosen"] == "naive_mean_12"
    assert verdict["step"] == "§7.3 parsimony"
