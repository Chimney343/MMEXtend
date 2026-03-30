"""Tests for src.utils.config_loader."""

from pathlib import Path

import pytest
import yaml

from src.utils.config_loader import get_paths, load_config


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

def test_load_config_returns_dict():
    config = load_config()
    assert isinstance(config, dict)


def test_load_config_contains_paths_key():
    config = load_config()
    assert "paths" in config


def test_load_config_default_has_expected_paths():
    config = load_config()
    paths = config["paths"]
    assert "data_raw" in paths
    assert "data_interim" in paths
    assert "data_processed" in paths
    assert "outputs" in paths


def test_load_config_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/pipeline.yaml")


def test_load_config_custom_path(tmp_path):
    custom = {"paths": {"data_raw": "raw"}, "logging": {"level": "DEBUG"}}
    config_file = tmp_path / "custom.yaml"
    config_file.write_text(yaml.dump(custom), encoding="utf-8")
    config = load_config(config_file)
    assert config["logging"]["level"] == "DEBUG"


def test_load_config_accepts_string_path(tmp_path):
    custom = {"paths": {"data_raw": "raw"}}
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(yaml.dump(custom), encoding="utf-8")
    config = load_config(str(config_file))
    assert isinstance(config, dict)


def test_load_config_accepts_path_object(tmp_path):
    custom = {"key": "value"}
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(yaml.dump(custom), encoding="utf-8")
    config = load_config(Path(config_file))
    assert config["key"] == "value"


# ---------------------------------------------------------------------------
# get_paths
# ---------------------------------------------------------------------------

def test_get_paths_returns_dict(tmp_path):
    config = {"paths": {"data_raw": "data/raw", "data_interim": "data/interim"}}
    paths = get_paths(config, project_root=tmp_path)
    assert isinstance(paths, dict)


def test_get_paths_values_are_path_objects(tmp_path):
    config = {"paths": {"data_raw": "data/raw", "outputs": "outputs"}}
    paths = get_paths(config, project_root=tmp_path)
    for val in paths.values():
        assert isinstance(val, Path)


def test_get_paths_resolves_to_absolute(tmp_path):
    config = {"paths": {"data_raw": "data/raw"}}
    paths = get_paths(config, project_root=tmp_path)
    assert paths["data_raw"].is_absolute()


def test_get_paths_combines_root_and_relative(tmp_path):
    config = {"paths": {"data_raw": "data/raw"}}
    paths = get_paths(config, project_root=tmp_path)
    assert str(tmp_path) in str(paths["data_raw"])


def test_get_paths_empty_paths_key():
    paths = get_paths({"paths": {}}, project_root=Path("/tmp"))
    assert paths == {}


def test_get_paths_no_paths_key():
    paths = get_paths({}, project_root=Path("/tmp"))
    assert paths == {}


def test_get_paths_uses_default_root_when_none():
    """Calling without project_root should not raise."""
    config = {"paths": {"data_raw": "data/raw"}}
    paths = get_paths(config)
    assert "data_raw" in paths
    assert paths["data_raw"].is_absolute()
