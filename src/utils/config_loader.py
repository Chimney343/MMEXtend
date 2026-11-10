"""
Load and provide access to pipeline YAML configuration.

Usage:
    from src.utils.config_loader import load_config, get_paths
    config = load_config()
    paths = get_paths(config)
"""

from pathlib import Path
from typing import Any

import yaml


_DEFAULT_CONFIG_PATH = Path(__file__).parents[2] / "configs" / "pipeline.yaml"


def local_override_path(config_path: Path) -> Path:
    """Return the private override that sits next to a config file (``x.local.yaml``)."""
    return config_path.with_name(f"{config_path.stem}.local{config_path.suffix}")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge ``override`` into ``base``: dicts merge by key, anything else replaces."""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """
    Load pipeline.yaml and return its contents as a dict.

    When a sibling ``<name>.local.yaml`` exists (``configs/pipeline.local.yaml``
    for the default config), it is deep-merged over the file: dicts merge key
    by key, while lists and scalars replace. That file holds private values,
    such as real account names, and is git-ignored.

    Parameters
    ----------
    config_path : str or Path, optional
        Explicit path to pipeline.yaml. Defaults to configs/pipeline.yaml
        relative to the project root.

    Returns
    -------
    dict
    """
    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)

    local_path = local_override_path(path)
    if isinstance(config, dict) and local_path.exists():
        with local_path.open("r", encoding="utf-8") as fh:
            override = yaml.safe_load(fh)
        if isinstance(override, dict):
            config = _deep_merge(config, override)
    return config


def get_paths(config: dict[str, Any], project_root: str | Path | None = None) -> dict[str, Path]:
    """
    Resolve path strings from config into absolute Path objects.

    Parameters
    ----------
    config : dict
        Loaded pipeline config (output of load_config()).
    project_root : str or Path, optional
        Root directory for resolving relative paths. Defaults to two levels
        above this file (i.e. the project root).

    Returns
    -------
    dict[str, Path]
        Keys: data_raw, data_interim, data_processed, outputs, logs.
    """
    root = Path(project_root) if project_root else Path(__file__).parents[2]
    raw_paths: dict[str, str] = config.get("paths", {})
    return {key: (root / value).resolve() for key, value in raw_paths.items()}
