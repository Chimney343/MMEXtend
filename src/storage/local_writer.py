"""
Write DataFrames to local storage with enforced naming and safety rules.

Hard rules enforced:
  - CSV exports only on explicit user instruction
  - Never overwrite: date suffix on every CSV
  - Always log: absolute path + row count

Usage:
    from src.storage.local_writer import LocalWriter
    writer = LocalWriter()
    writer.save_interim(df, "transactions_validated")
    writer.save_processed(df, "expense_trends")
    writer.export_csv(df, "cashflow_forecast")   # only when user asks
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

INTERIM_DIR = "data/interim"
PROCESSED_DIR = "data/processed"
EXPORT_DIR = "outputs"


class LocalWriter:
    def __init__(self, project_root: str | Path = ".") -> None:
        self.root = Path(project_root).resolve()
        self.interim_dir = self.root / INTERIM_DIR
        self.processed_dir = self.root / PROCESSED_DIR
        self.export_dir = self.root / EXPORT_DIR

    def _ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def save_interim(self, df: pd.DataFrame, name: str) -> Path:
        """
        Save DataFrame to data/interim/ as Parquet.
        Overwrites the previous version (re-generated each run).

        Parameters
        ----------
        df : pd.DataFrame
        name : str
            File stem (e.g. "transactions_validated").

        Returns
        -------
        Path
            Absolute path of the written file.
        """
        self._ensure_dir(self.interim_dir)
        filepath = self.interim_dir / f"{name}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"Wrote {len(df)} rows to {filepath}")
        return filepath

    def save_processed(self, df: pd.DataFrame, name: str) -> Path:
        """
        Save DataFrame to data/processed/ as Parquet.
        Overwrites the previous version.

        Parameters
        ----------
        df : pd.DataFrame
        name : str
            File stem (e.g. "expense_trends").

        Returns
        -------
        Path
        """
        self._ensure_dir(self.processed_dir)
        filepath = self.processed_dir / f"{name}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"Wrote {len(df)} rows to {filepath}")
        return filepath

    def export_csv(self, df: pd.DataFrame, name: str) -> Path:
        """
        Export DataFrame to outputs/ as a date-suffixed CSV.
        NEVER overwrites — raises FileExistsError if the target already exists.
        Only call this when the user explicitly requests a CSV export.

        Parameters
        ----------
        df : pd.DataFrame
        name : str
            Base name without extension (e.g. "cashflow_forecast").

        Returns
        -------
        Path
            Absolute path of the written file.

        Raises
        ------
        FileExistsError
            If the target file already exists.
        """
        self._ensure_dir(self.export_dir)
        today = datetime.now().strftime("%Y-%m-%d")
        filepath = self.export_dir / f"{name}_{today}.csv"

        if filepath.exists():
            msg = f"File already exists: {filepath}. Write aborted (never-overwrite rule)."
            logger.error(msg)
            raise FileExistsError(msg)

        df.to_csv(filepath, index=False)
        abs_path = filepath.resolve()
        logger.info(f"Exported {len(df)} rows to {abs_path}")
        print(f"CSV exported: {abs_path} ({len(df)} rows)")
        return abs_path

    def load_interim(self, name: str) -> pd.DataFrame:
        """
        Load a Parquet file from data/interim/.

        Parameters
        ----------
        name : str
            File stem (e.g. "transactions_validated").
        """
        filepath = self.interim_dir / f"{name}.parquet"
        df = pd.read_parquet(filepath)
        logger.info(f"Loaded {len(df)} rows from {filepath}")
        return df

    def load_processed(self, name: str) -> pd.DataFrame:
        """
        Load a Parquet file from data/processed/.

        Parameters
        ----------
        name : str
            File stem (e.g. "expense_trends").
        """
        filepath = self.processed_dir / f"{name}.parquet"
        df = pd.read_parquet(filepath)
        logger.info(f"Loaded {len(df)} rows from {filepath}")
        return df
