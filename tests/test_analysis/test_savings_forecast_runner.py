"""Runner smoke test: synthetic series, two cheap arms, dev then test phase."""

import numpy as np
import pandas as pd
import pytest
import yaml

from scripts.experiments import savings_forecast_bakeoff as runner


def _write_dataset(tmp_path, n=72, seed=5):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2016-01-01", periods=n, freq="MS")
    savings = pd.Series(1_000.0 + rng.normal(0, 120.0, n), index=idx)
    split = pd.DataFrame(
        {
            "year_month": idx.strftime("%Y-%m"),
            "month_ordinal": range(n),
            "deposits": savings + 2_000.0,
            "withdrawals": pd.Series(2_000.0, index=idx),
            "disposable_income": savings,
            "savings_rate": savings / (savings + 2_000.0),
            "is_partial_month": False,
        }
    )
    path = tmp_path / "cashflow_split.parquet"
    split.to_parquet(path, index=False)
    return path


def _config(tmp_path, dataset, frozen=False):
    return {
        "meta": {
            "experiment": "smoke",
            "output_dir": str(tmp_path / "out"),
            "frozen": frozen,
            "frozen_at": None,
        },
        "data": {"cashflow_split": str(dataset)},
        "split": {
            "initial": 36,
            "step": 1,
            "horizon_primary": 12,
            "horizon_secondary": 24,
            "test_fraction": 1 / 3,
        },
        "bands": {"alpha": 0.2, "min_origins": 2},
        "metrics": {"mase_season": 12},
        "decision": {
            "relcum12_max": 1.0,
            "parsimony_margin": 0.10,
            "mase_margin": 0.05,
            "band_coverage": [0.0, 1.0],
            "baselines": {
                "denominator": "naive_mean_12",
                "bias_reference": "naive_mean_12",
            },
        },
        "arms": {
            "naive_mean_12": {"variants": [{}]},
            "random_walk": {"variants": [{}]},
        },
        "robust_target": {"enabled": False, "candidates_from": [], "top_n": 0},
        "selection": {},
    }


def test_test_phase_refuses_to_run_while_unfrozen(tmp_path):
    dataset = _write_dataset(tmp_path)
    config = _config(tmp_path, dataset, frozen=False)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(SystemExit, match="frozen"):
        runner.main(["--phase", "test", "--config", str(config_path)])


def test_dev_then_test_phase_writes_artefacts(tmp_path):
    dataset = _write_dataset(tmp_path)
    config = _config(tmp_path, dataset)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    assert runner.main(["--phase", "dev", "--config", str(config_path)]) == 0
    out = tmp_path / "out"
    assert (out / "dev_selection.json").exists()
    assert (out / "dev_candidates.csv").exists()

    assert runner.main(["--freeze", "--config", str(config_path)]) == 0
    frozen = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert frozen["meta"]["frozen"] is True
    assert set(frozen["selection"]) == {"naive_mean_12", "random_walk"}

    assert runner.main(["--phase", "test", "--config", str(config_path)]) == 0
    assert (out / "test_leaderboard.csv").exists()
    assert (out / "verdict.json").exists()
    leaderboard = pd.read_csv(out / "test_leaderboard.csv")
    assert "naive_mean_12" in set(leaderboard["arm"])
    assert leaderboard.loc[leaderboard["arm"] == "naive_mean_12", "test_relcum12"].iloc[0] == 1.0


def test_split_is_purged_and_embargoes_in_between_origins():
    # Arbitrary length: the test asserts the split rule, not a real series.
    n_months = 97
    initial, horizon, step = 36, 12, 1
    series = pd.Series(
        np.arange(float(n_months)),
        index=pd.date_range("2015-03-01", periods=n_months, freq="MS"),
    )
    config = {
        "split": {
            "initial": initial,
            "step": step,
            "horizon_primary": horizon,
            "horizon_secondary": 24,
            "test_fraction": 1 / 3,
        }
    }
    split = runner.make_split(series, config)

    expected_origins = len(range(initial, n_months - horizon + 1, step))
    expected_test = int(np.floor(expected_origins * (1 / 3)))
    assert len(split["primary_origins"]) == expected_origins
    assert len(split["test"]) == expected_test
    assert len(split["dev"]) > 0
    assert split["test"][0] not in split["dev"]
    assert all(o not in split["test"] for o in split["dev"])
    for origin in split["dev"]:
        assert origin + pd.DateOffset(months=horizon) < split["first_test_target"]
    assert set(split["dev"]) | set(split["embargo"]) | set(split["test"]) == set(
        split["primary_origins"]
    )


def _decision_config():
    return {
        "decision": {
            "relcum12_max": 1.0,
            "parsimony_margin": 0.10,
            "mase_margin": 0.05,
            "band_coverage": [0.65, 0.95],
            "baselines": {
                "denominator": "naive_mean_12",
                "bias_reference": "naive_mean_12",
            },
        }
    }


def _board(rows):
    import pandas as pd

    return pd.DataFrame(rows)


def test_decision_rule_falls_back_to_b1_when_nothing_is_eligible():
    board = _board(
        [
            {"arm": "naive_mean_12", "test_cum12_abs": 100.0, "test_relcum12": 1.0,
             "dev_relcum12": 1.0, "mase_1_12": 1.0, "cum12_bias": 10.0, "band_coverage": 0.8},
            {"arm": "naive_median_12", "test_cum12_abs": 110.0, "test_relcum12": 1.1,
             "dev_relcum12": 1.2, "mase_1_12": 1.0, "cum12_bias": 5.0, "band_coverage": 0.8},
            {"arm": "auto_ets", "test_cum12_abs": 120.0, "test_relcum12": 1.2,
             "dev_relcum12": 0.9, "mase_1_12": 1.0, "cum12_bias": 5.0, "band_coverage": 0.8},
        ]
    )
    verdict = runner.apply_decision_rule(board, _decision_config())
    assert verdict["chosen"] == "naive_mean_12"
    assert "7.1" in verdict["step"]


def test_decision_rule_uses_b2_as_candidate_when_b1_is_not_eligible():
    # B1's RelCum12 is exactly 1.00 by construction, so it can never satisfy
    # "< 1.00" and is reachable only through the §7.1 fallback. B2 qualifying
    # alone therefore makes B2 the §7.2 candidate, not a §7.1 fallback.
    board = _board(
        [
            {"arm": "naive_mean_12", "test_cum12_abs": 100.0, "test_relcum12": 1.0,
             "dev_relcum12": 1.0, "mase_1_12": 1.0, "cum12_bias": 10.0, "band_coverage": 0.8},
            {"arm": "naive_median_12", "test_cum12_abs": 90.0, "test_relcum12": 0.9,
             "dev_relcum12": 0.9, "mase_1_12": 1.0, "cum12_bias": 5.0, "band_coverage": 0.8},
        ]
    )
    verdict = runner.apply_decision_rule(board, _decision_config())
    assert verdict["chosen"] == "naive_median_12"


def test_decision_rule_parsimony_never_picks_b1_because_it_cannot_be_eligible():
    # B1 is only reachable via the fallback. If a more complex arm is eligible,
    # §7.3 can only move to another *eligible* simpler arm; B1 is not in that
    # set, so the eligible arm keeps the crown.
    board = _board(
        [
            {"arm": "naive_mean_12", "test_cum12_abs": 100.0, "test_relcum12": 1.0,
             "dev_relcum12": 1.0, "mase_1_12": 1.0, "cum12_bias": 10.0, "band_coverage": 0.8},
            {"arm": "naive_median_12", "test_cum12_abs": 99.0, "test_relcum12": 0.99,
             "dev_relcum12": 0.98, "mase_1_12": 1.0, "cum12_bias": 5.0, "band_coverage": 0.8},
            {"arm": "auto_ets", "test_cum12_abs": 100.0, "test_relcum12": 0.995,
             "dev_relcum12": 0.90, "mase_1_12": 1.0, "cum12_bias": 5.0, "band_coverage": 0.8},
        ]
    )
    verdict = runner.apply_decision_rule(board, _decision_config())
    assert verdict["chosen"] == "naive_median_12"


def test_decision_rule_guardrail_rejects_bad_band_coverage():
    board = _board(
        [
            {"arm": "naive_mean_12", "test_cum12_abs": 120.0, "test_relcum12": 1.2,
             "dev_relcum12": 1.2, "mase_1_12": 1.0, "cum12_bias": 10.0, "band_coverage": 0.8},
            {"arm": "auto_ets", "test_cum12_abs": 100.0, "test_relcum12": 0.95,
             "dev_relcum12": 0.90, "mase_1_12": 1.0, "cum12_bias": 5.0, "band_coverage": 0.3},
            {"arm": "theta", "test_cum12_abs": 110.0, "test_relcum12": 0.97,
             "dev_relcum12": 0.90, "mase_1_12": 1.0, "cum12_bias": 5.0, "band_coverage": 0.8},
        ]
    )
    verdict = runner.apply_decision_rule(board, _decision_config())
    assert verdict["chosen"] == "theta"
    assert any(entry.get("rejected") == "auto_ets" for entry in verdict["trail"])
