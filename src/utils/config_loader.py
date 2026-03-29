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


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """
    Load pipeline.yaml and return its contents as a dict.

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
        return yaml.safe_load(fh)


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
