"""Tests for the read-only DuckDB sandbox."""

import os
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.agent.sql_sandbox import SqlSandbox


def _write_parquet(root, stage: str, name: str, frame: pd.DataFrame):
    directory = root / "data" / stage
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.parquet"
    frame.to_parquet(path, index=False)
    return path


def _make_sandbox(tmp_path, mmb_path=None, include_mmex=False, max_result_chars=12000):
    return SqlSandbox(
        project_root=tmp_path,
        mmb_path=mmb_path,
        include_mmex=include_mmex,
        max_result_chars=max_result_chars,
    )


def _make_mmb(tmp_path) -> Path:
    directory = tmp_path / "Za\u017c\u00f3\u0142\u0107 pliki"
    directory.mkdir(parents=True)
    path = directory / "finances.mmb"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE ACCOUNTLIST_V1 "
        "(ACCOUNTID INTEGER PRIMARY KEY, ACCOUNTNAME TEXT, CURRENCYID INTEGER)"
    )
    connection.execute("INSERT INTO ACCOUNTLIST_V1 VALUES (1, 'Checking', 1)")
    connection.execute("CREATE TABLE ATTACHMENT_V1 (ATTACHMENTID INTEGER)")
    connection.commit()
    connection.close()
    return path


def test_catalog_lists_parquet_tables(tmp_path):
    _write_parquet(tmp_path, "interim", "alpha", pd.DataFrame({"x": [1, 2, 3]}))
    _write_parquet(tmp_path, "processed", "beta", pd.DataFrame({"y": [1.5, 2.5]}))

    catalog = {entry["name"]: entry for entry in _make_sandbox(tmp_path).catalog()}

    assert set(catalog) == {"alpha", "beta"}
    assert catalog["alpha"]["rows"] == 3
    assert catalog["beta"]["rows"] == 2
    assert [column for column, _type in catalog["alpha"]["columns"]] == ["x"]


def test_select_works_and_truncates(tmp_path):
    _write_parquet(tmp_path, "interim", "nums", pd.DataFrame({"n": [1, 2, 3, 4, 5]}))
    sandbox = _make_sandbox(tmp_path)

    full = sandbox.query("SELECT * FROM nums LIMIT 1", max_rows=10)
    assert full.error is None
    assert full.row_count == 1
    assert full.truncated is False

    clipped = sandbox.query("SELECT * FROM nums ORDER BY n", max_rows=2)
    assert clipped.error is None
    assert clipped.row_count == 2
    assert clipped.truncated is True
    assert clipped.rows_csv.splitlines()[1:] == ["1", "2"]


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE nums",
        "INSERT INTO nums VALUES (6)",
        "CREATE TABLE other (n INTEGER)",
        "SELECT 1; SELECT 2",
        "COPY nums TO 'export.csv'",
        "SELECT * FROM read_csv('secret.csv')",
    ],
)
def test_rejects_unsafe_statements(tmp_path, sql):
    _write_parquet(tmp_path, "interim", "nums", pd.DataFrame({"n": [1, 2, 3]}))
    sandbox = _make_sandbox(tmp_path)

    result = sandbox.query(sql, max_rows=10)

    assert result.error is not None
    unchanged = sandbox.query("SELECT COUNT(*) AS n FROM nums", max_rows=10)
    assert unchanged.rows_csv.splitlines() == ["n", "3"]


def test_external_access_is_locked_even_without_statement_validation(tmp_path):
    _write_parquet(tmp_path, "interim", "nums", pd.DataFrame({"n": [1, 2, 3]}))
    sandbox = _make_sandbox(tmp_path)
    target = tmp_path / "written.csv"

    with pytest.raises(Exception):
        sandbox._con.execute("SELECT * FROM read_csv('secret.csv')").fetchall()

    with pytest.raises(Exception):
        sandbox._con.execute(f"COPY nums TO '{target.as_posix()}'").fetchall()

    assert not target.exists()


def test_mmex_tables_load_from_non_ascii_path(tmp_path):
    mmb_path = _make_mmb(tmp_path)
    sandbox = _make_sandbox(tmp_path, mmb_path=mmb_path, include_mmex=True)

    catalog = {entry["name"]: entry for entry in sandbox.catalog()}
    assert "mmex_accountlist_v1" in catalog
    assert "mmex_attachment_v1" not in catalog
    assert catalog["mmex_accountlist_v1"]["rows"] == 1

    result = sandbox.query("SELECT ACCOUNTNAME FROM mmex_accountlist_v1", max_rows=10)
    assert result.error is None
    assert "Checking" in result.rows_csv


def test_missing_mmb_leaves_parquet_only(tmp_path, caplog):
    _write_parquet(tmp_path, "interim", "nums", pd.DataFrame({"n": [1]}))
    sandbox = _make_sandbox(
        tmp_path, mmb_path=tmp_path / "missing.mmb", include_mmex=True
    )

    names = [entry["name"] for entry in sandbox.catalog()]

    assert names == ["nums"]
    assert any("MMEX database not found" in record.message for record in caplog.records)


def test_ensure_fresh_rebuilds_after_parquet_rewrite(tmp_path):
    path = _write_parquet(tmp_path, "interim", "nums", pd.DataFrame({"n": [1]}))
    sandbox = _make_sandbox(tmp_path)
    assert sandbox.query("SELECT COUNT(*) AS n FROM nums", max_rows=10).rows_csv.splitlines() == [
        "n",
        "1",
    ]

    path.write_bytes(b"")
    pd.DataFrame({"n": [1, 2, 3]}).to_parquet(path, index=False)
    future = path.stat().st_mtime + 100
    os.utime(path, (future, future))

    sandbox.ensure_fresh()

    assert sandbox.query("SELECT COUNT(*) AS n FROM nums", max_rows=10).rows_csv.splitlines() == [
        "n",
        "3",
    ]


def test_query_error_is_returned_not_raised(tmp_path):
    _write_parquet(tmp_path, "interim", "nums", pd.DataFrame({"n": [1]}))
    sandbox = _make_sandbox(tmp_path)

    result = sandbox.query("SELECT * FROM does_not_exist", max_rows=10)

    assert result.error is not None
    assert "does_not_exist" in result.error


def test_result_chars_are_clipped(tmp_path):
    _write_parquet(
        tmp_path,
        "interim",
        "texts",
        pd.DataFrame({"text": ["x" * 500, "y" * 500]}),
    )
    sandbox = _make_sandbox(tmp_path, max_result_chars=80)

    result = sandbox.query("SELECT * FROM texts", max_rows=10)

    assert len(result.rows_csv) <= 80 + len("\n...[truncated]")
    assert result.rows_csv.endswith("[truncated]")


def test_catalog_includes_low_cardinality_text_values(tmp_path):
    _write_parquet(
        tmp_path,
        "interim",
        "cats",
        pd.DataFrame(
            {
                "kind": ["alpha", "beta", "alpha", "gamma"] * 5,
                "many": [f"v{index}" for index in range(20)],
                "long": ["x" * 50, "y" * 50] * 10,
            }
        ),
    )
    entry = _make_sandbox(tmp_path).catalog()[0]

    assert entry["values"]["kind"] == ["alpha", "beta", "gamma"]
    assert "many" not in entry["values"]
    assert "long" not in entry["values"]


def test_float_precision_is_preserved(tmp_path):
    _write_parquet(
        tmp_path,
        "interim",
        "rates",
        pd.DataFrame({"rate": [4.2345], "price": [123.456789]}),
    )
    sandbox = _make_sandbox(tmp_path)

    result = sandbox.query("SELECT * FROM rates", max_rows=10)

    assert result.rows_csv.splitlines() == ["rate,price", "4.2345,123.456789"]


def test_catalog_is_empty_without_data(tmp_path):
    assert _make_sandbox(tmp_path).catalog() == []
