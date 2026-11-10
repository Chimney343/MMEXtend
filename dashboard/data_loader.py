"""
Load pre-computed Parquet files for the Dash app.
Returns None with a log warning if the file does not exist,
so callbacks can show a user-facing message rather than crashing.
"""
import pandas as pd
from pathlib import Path
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_parquet(stage: str, name: str) -> pd.DataFrame | None:
    """
    Load data/{stage}/{name}.parquet relative to project root.

    Returns None if the file is absent (pipeline not yet run for this stage).
    """
    path = PROJECT_ROOT / "data" / stage / f"{name}.parquet"
    if not path.exists():
        logger.warning(f"Parquet not found: {path}. Run the pipeline first.")
        return None
    df = pd.read_parquet(path)
    logger.info(f"Loaded {len(df)} rows from {path}")
    return df
