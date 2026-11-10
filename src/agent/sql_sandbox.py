"""
Read-only DuckDB catalog over the dashboard data.

Every Parquet file from ``data/interim`` and ``data/processed`` and every
table from the MMEX ``.mmb`` SQLite database is copied into in-memory DuckDB
tables. The connection is then locked down with ``enable_external_access``
disabled and ``lock_configuration`` enabled, so the agent can only query the
loaded data and cannot touch the filesystem.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

_ALLOWED_STATEMENT_TYPES = {"SELECT"}
_ALLOWED_KEYWORDS = {"SELECT", "WITH", "DESCRIBE", "SUMMARIZE", "SHOW"}
_SKIPPED_MMEX_TABLES = {"ATTACHMENT_V1"}


def quote_identifier(name: str) -> str:
    """Quote a DuckDB identifier, escaping embedded double quotes."""
    return '"' + str(name).replace('"', '""') + '"'


def _execute_ddl(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> None:
    """Run a statement and consume its result so the connection stays reusable."""
    con.execute(sql, params or []).fetchall()


@dataclass
class QueryResult:
    """Result of one read-only query."""

    columns: list[str] = field(default_factory=list)
    rows_csv: str = ""
    row_count: int = 0
    truncated: bool = False
    error: str | None = None


class SqlSandbox:
    """In-memory, read-only DuckDB view of the dashboard data."""

    def __init__(
        self,
        project_root: Path,
        mmb_path: Path | None,
        include_mmex: bool,
        max_result_chars: int = 12000,
    ) -> None:
        self.project_root = Path(project_root)
        self.mmb_path = Path(mmb_path) if mmb_path else None
        self.include_mmex = include_mmex
        self.max_result_chars = int(max_result_chars)
        self._lock = threading.RLock()
        self._con: duckdb.DuckDBPyConnection | None = None
        self._catalog: list[dict[str, Any]] = []
        self._fingerprint: tuple[tuple[str, float], ...] = ()
        self._rebuild()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _source_files(self) -> list[Path]:
        files = self._parquet_files()
        if self.include_mmex and self.mmb_path and self.mmb_path.exists():
            files.append(self.mmb_path)
        return sorted(files, key=lambda path: str(path))

    def _fingerprint_now(self) -> tuple[tuple[str, float], ...]:
        entries: list[tuple[str, float]] = []
        for path in self._source_files():
            try:
                entries.append((str(path.resolve()), path.stat().st_mtime))
            except OSError:
                continue
        return tuple(entries)

    def ensure_fresh(self) -> None:
        """Rebuild the catalog when a source file is added, removed or rewritten."""
        with self._lock:
            if self._fingerprint_now() == self._fingerprint:
                return
            logger.info("Dashboard data changed; rebuilding the SQL sandbox")
            self._rebuild()

    def _rebuild(self) -> None:
        with self._lock:
            new_con = self._build()
            new_catalog = self._build_catalog(new_con)
            old_con = self._con
            self._con = new_con
            self._catalog = new_catalog
            self._fingerprint = self._fingerprint_now()
            if old_con is not None:
                try:
                    old_con.close()
                except Exception:
                    logger.debug("Could not close the previous DuckDB connection", exc_info=True)

    def _build(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        try:
            for path in self._parquet_files():
                self._load_parquet(con, path)
            if self.include_mmex:
                self._load_mmex(con)
            _execute_ddl(con, "SET enable_external_access = false")
            _execute_ddl(con, "SET lock_configuration = true")
        except Exception:
            con.close()
            raise
        return con

    def _parquet_files(self) -> list[Path]:
        files: list[Path] = []
        for stage in ("interim", "processed"):
            stage_dir = self.project_root / "data" / stage
            if stage_dir.is_dir():
                files.extend(sorted(stage_dir.glob("*.parquet")))
        return files

    def _load_parquet(self, con: duckdb.DuckDBPyConnection, path: Path) -> None:
        table = path.stem
        try:
            _execute_ddl(
                con,
                f"CREATE OR REPLACE TABLE {quote_identifier(table)} "
                "AS SELECT * FROM read_parquet(?)",
                [str(path)],
            )
            count = con.execute(f"SELECT COUNT(*) FROM {quote_identifier(table)}").fetchone()[0]
            logger.info("Loaded Parquet table %s (%s rows)", table, count)
        except Exception as exc:
            logger.warning("Could not load Parquet %s: %s", path, exc)

    def _load_mmex(self, con: duckdb.DuckDBPyConnection) -> None:
        if not self.mmb_path or not self.mmb_path.exists():
            logger.warning("MMEX database not found at %s; continuing with Parquet only", self.mmb_path)
            return

        try:
            uri = self.mmb_path.resolve().as_uri() + "?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
        except Exception as exc:
            logger.warning("Could not open MMEX database read-only: %s", exc)
            return

        try:
            names = [
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
                )
            ]
            for name in names:
                if name.upper() in _SKIPPED_MMEX_TABLES:
                    continue
                table = f"mmex_{name.lower()}"
                try:
                    frame = pd.read_sql_query(
                        f"SELECT * FROM {quote_identifier(name)}", connection
                    )
                except Exception as exc:
                    logger.warning("Could not read MMEX table %s: %s", name, exc)
                    continue

                try:
                    # Replacement scan: ``frame`` is a local variable in this scope,
                    # so DuckDB can read the DataFrame without registering a view.
                    con.execute(
                        f"CREATE OR REPLACE TABLE {quote_identifier(table)} "
                        "AS SELECT * FROM frame"
                    ).fetchall()
                    logger.info("Loaded MMEX table %s (%s rows)", table, len(frame))
                except Exception as exc:
                    logger.warning("Could not load MMEX table %s into DuckDB: %s", table, exc)
        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Catalog and queries
    # ------------------------------------------------------------------

    def catalog(self) -> list[dict[str, Any]]:
        """Return table names, row counts, column types and low-cardinality values."""
        with self._lock:
            return self._catalog

    def _build_catalog(self, con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
        try:
            rows = con.execute(
                """
                SELECT table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'main'
                ORDER BY table_name, ordinal_position
                """
            ).fetchall()
        except Exception as exc:
            logger.warning("Could not read the DuckDB catalog: %s", exc)
            return []

        grouped: dict[str, list[tuple[str, str]]] = {}
        for table_name, column_name, data_type, _position in rows:
            grouped.setdefault(table_name, []).append((column_name, data_type))

        result: list[dict[str, Any]] = []
        for table_name in sorted(grouped):
            try:
                count = con.execute(
                    f"SELECT COUNT(*) FROM {quote_identifier(table_name)}"
                ).fetchone()[0]
            except Exception as exc:
                logger.warning("Could not count table %s: %s", table_name, exc)
                continue
            columns = grouped[table_name]
            result.append(
                {
                    "name": table_name,
                    "rows": int(count),
                    "columns": columns,
                    "values": self._distinct_values(con, table_name, columns),
                }
            )
        return result

    def _distinct_values(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        columns: list[tuple[str, str]],
    ) -> dict[str, list[str]]:
        """List values for short VARCHAR columns (at most 15 distinct values)."""
        values: dict[str, list[str]] = {}
        for column_name, data_type in columns:
            if "VARCHAR" not in data_type.upper() and data_type.upper() not in {
                "TEXT",
                "STRING",
            }:
                continue
            try:
                rows = con.execute(
                    f"SELECT DISTINCT CAST({quote_identifier(column_name)} AS VARCHAR) AS value "
                    f"FROM {quote_identifier(table_name)} "
                    f"WHERE {quote_identifier(column_name)} IS NOT NULL "
                    "ORDER BY value LIMIT 16"
                ).fetchall()
            except Exception:
                continue
            distinct = [str(row[0]) for row in rows]
            if not distinct or len(distinct) > 15:
                continue
            if any(len(value) > 40 for value in distinct):
                continue
            values[column_name] = distinct
        return values

    def query(self, sql: str, max_rows: int) -> QueryResult:
        """Run exactly one read-only statement. Errors are returned, never raised."""
        limit = max(1, int(max_rows))
        try:
            self._validate_statement(sql)
        except Exception as exc:
            logger.warning("Rejected SQL: %s", exc)
            return QueryResult(error=str(exc))

        with self._lock:
            con = self._require_con()
            cursor = con.cursor()
            try:
                cursor.execute(sql)
                description = cursor.description
                if not description:
                    return QueryResult()
                columns = [str(column[0]) for column in description]
                rows = cursor.fetchmany(limit + 1)
                truncated = len(rows) > limit
                rows = rows[:limit]
                frame = pd.DataFrame(rows, columns=columns)
                csv = frame.to_csv(index=False)
                if len(csv) > self.max_result_chars:
                    suffix = "\n...[truncated]"
                    csv = csv[: max(0, self.max_result_chars - len(suffix))] + suffix
                logger.info(
                    "SQL ok: %s rows%s; query=%s",
                    len(frame),
                    " (truncated)" if truncated else "",
                    sql,
                )
                return QueryResult(
                    columns=columns,
                    rows_csv=csv,
                    row_count=len(frame),
                    truncated=truncated,
                )
            except Exception as exc:
                logger.warning("SQL error: %s; query=%s", exc, sql)
                return QueryResult(error=f"{type(exc).__name__}: {exc}")
            finally:
                cursor.close()

    def _validate_statement(self, sql: str) -> None:
        text = (sql or "").strip()
        if not text:
            raise ValueError("Empty SQL statement")

        extract = getattr(duckdb, "extract_statements", None)
        if callable(extract):
            statements = extract(text)
            if len(statements) != 1:
                raise ValueError("Exactly one SQL statement is allowed")
            statement_type = getattr(statements[0].type, "name", str(statements[0].type))
            if statement_type.upper() not in _ALLOWED_STATEMENT_TYPES:
                raise ValueError(
                    f"Only read-only SELECT statements are allowed, not {statement_type}"
                )
            return

        # Fallback for DuckDB versions without extract_statements().
        if ";" in text.rstrip().rstrip(";"):
            raise ValueError("Exactly one SQL statement is allowed")
        keyword = text.split(None, 1)[0].upper()
        if keyword not in _ALLOWED_KEYWORDS:
            raise ValueError(f"Only read-only SELECT statements are allowed, not {keyword}")

    def _require_con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("SQL sandbox is not initialised")
        return self._con
