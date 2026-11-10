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

import shutil
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

    def reset_outputs(self, keep: tuple[str, ...] = ("logs",)) -> None:
        """
        Empty data/interim/, data/processed/ and outputs/ before a run.

        Every run rebuilds all outputs, so nothing from an earlier run (or from a
        step that no longer exists) may survive. Entries of outputs/ named in
        ``keep`` stay: the log file for the current run is already open there.
        Inputs in data/raw/ are never touched.
        """
        for directory in (self.interim_dir, self.processed_dir, self.export_dir):
            if directory == self.root or not directory.is_relative_to(self.root):
                raise ValueError(f"Refusing to clear {directory}: not inside {self.root}")
            self._ensure_dir(directory)
            removed = 0
            for child in directory.iterdir():
                if directory == self.export_dir and child.name in keep:
                    continue
                if child.is_dir() and not child.is_symlink():
                    shutil.rmtree(child)
                else:
                    child.unlink()
                removed += 1
            logger.info(f"Cleared {removed} entries from {directory}")

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

        Parameters
        ----------
        df : pd.DataFrame
        name : str
            Base name without extension (e.g. "cashflow_forecast").

        Returns
        -------
        Path
            Absolute path of the written file.
        """
        self._ensure_dir(self.export_dir)
        today = datetime.now().strftime("%Y-%m-%d")
        filepath = self.export_dir / f"{name}_{today}.csv"

        df.to_csv(filepath, index=False)
        abs_path = filepath.resolve()
        logger.info(f"Exported {len(df)} rows to {abs_path}")
        print(f"CSV exported: {abs_path} ({len(df)} rows)")
        return abs_path

    def export_xlsx(self, sheets: dict[str, pd.DataFrame], name: str) -> Path:
        """
        Export a dict of DataFrames to outputs/ as a date-suffixed .xlsx workbook.

        Each key becomes a sheet name (sanitised to ≤31 chars, no []).

        Parameters
        ----------
        sheets : dict[str, pd.DataFrame]
            Sheet name → DataFrame to write.
        name : str
            Base name without extension (e.g. "frozen_report").

        Returns
        -------
        Path
            Absolute path of the written file.
        """

        self._ensure_dir(self.export_dir)
        today = datetime.now().strftime("%Y-%m-%d")
        filepath = self.export_dir / f"{name}_{today}.xlsx"

        total_rows = 0
        sheet_names_used: set[str] = set()
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            for sheet_name, df_sheet in sheets.items():
                safe_name = sheet_name.replace("[", " ").replace("]", " ").strip()
                safe_name = " ".join(safe_name.split())[:31] or "Sheet"
                if safe_name in sheet_names_used:
                    suffix = 2
                    while f"{safe_name}_{suffix}"[:31] in sheet_names_used:
                        suffix += 1
                    safe_name = f"{safe_name}_{suffix}"[:31]
                sheet_names_used.add(safe_name)
                df_sheet.to_excel(writer, sheet_name=safe_name, index=False)
                total_rows += len(df_sheet)

        abs_path = filepath.resolve()
        logger.info(f"Exported {total_rows} rows across {len(sheets)} sheet(s) to {abs_path}")
        print(f"XLSX exported: {abs_path} ({total_rows} rows)")
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
