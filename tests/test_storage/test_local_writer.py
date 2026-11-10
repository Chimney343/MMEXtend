"""Tests for src.storage.local_writer — LocalWriter."""

import pandas as pd
import pytest

from src.storage.local_writer import LocalWriter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def writer(tmp_path):
    return LocalWriter(project_root=tmp_path)


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "amount": [100.0, -50.0],
            "category": ["Income", "Food"],
        }
    )


# ---------------------------------------------------------------------------
# save_interim / load_interim
# ---------------------------------------------------------------------------


def test_save_interim_creates_parquet(writer, sample_df):
    path = writer.save_interim(sample_df, "test_interim")
    assert path.exists()
    assert path.suffix == ".parquet"


def test_save_interim_path_contains_interim(writer, sample_df):
    path = writer.save_interim(sample_df, "transactions")
    assert "interim" in str(path)
    assert "transactions.parquet" in path.name


def test_save_interim_returns_absolute_path(writer, sample_df):
    path = writer.save_interim(sample_df, "test_interim")
    assert path.is_absolute()


def test_load_interim_round_trips(writer, sample_df):
    writer.save_interim(sample_df, "rt_interim")
    loaded = writer.load_interim("rt_interim")
    assert len(loaded) == len(sample_df)
    assert list(loaded.columns) == list(sample_df.columns)


def test_save_interim_overwrites_previous(writer, sample_df):
    writer.save_interim(sample_df, "overwrite_test")
    new_df = sample_df.copy()
    new_df["extra"] = "x"
    writer.save_interim(new_df, "overwrite_test")
    loaded = writer.load_interim("overwrite_test")
    assert "extra" in loaded.columns


def test_load_interim_raises_for_missing(writer):
    with pytest.raises(Exception):
        writer.load_interim("nonexistent_file")


# ---------------------------------------------------------------------------
# save_processed / load_processed
# ---------------------------------------------------------------------------


def test_save_processed_creates_parquet(writer, sample_df):
    path = writer.save_processed(sample_df, "test_processed")
    assert path.exists()
    assert path.suffix == ".parquet"


def test_save_processed_path_contains_processed(writer, sample_df):
    path = writer.save_processed(sample_df, "expense_trends")
    assert "processed" in str(path)


def test_load_processed_round_trips(writer, sample_df):
    writer.save_processed(sample_df, "rt_processed")
    loaded = writer.load_processed("rt_processed")
    assert len(loaded) == len(sample_df)


def test_load_processed_raises_for_missing(writer):
    with pytest.raises(Exception):
        writer.load_processed("nonexistent_file")


# ---------------------------------------------------------------------------
# export_csv
# ---------------------------------------------------------------------------


def test_export_csv_creates_csv(writer, sample_df):
    path = writer.export_csv(sample_df, "cashflow")
    assert path.exists()
    assert path.suffix == ".csv"


def test_export_csv_name_in_stem(writer, sample_df):
    path = writer.export_csv(sample_df, "my_export")
    assert "my_export" in path.stem


def test_export_csv_date_suffix_in_stem(writer, sample_df):
    import re

    path = writer.export_csv(sample_df, "my_export")
    # Stem format: my_export_YYYY-MM-DD
    assert re.search(r"\d{4}-\d{2}-\d{2}$", path.stem)


def test_export_csv_row_count_preserved(writer, sample_df):
    path = writer.export_csv(sample_df, "count_test")
    loaded = pd.read_csv(path)
    assert len(loaded) == len(sample_df)


def test_export_csv_columns_preserved(writer, sample_df):
    path = writer.export_csv(sample_df, "cols_test")
    loaded = pd.read_csv(path)
    assert set(loaded.columns) == set(sample_df.columns)


def test_export_csv_overwrites(writer, sample_df):
    writer.export_csv(sample_df, "overwrite_test")
    path = writer.export_csv(sample_df, "overwrite_test")
    loaded = pd.read_csv(path)
    assert len(loaded) == len(sample_df)


def test_export_csv_returns_absolute_path(writer, sample_df):
    path = writer.export_csv(sample_df, "abs_path_test")
    assert path.is_absolute()


def test_export_csv_outputs_dir(writer, sample_df):
    path = writer.export_csv(sample_df, "dir_test")
    assert "outputs" in str(path)


# ---------------------------------------------------------------------------
# Directory creation (implicit)
# ---------------------------------------------------------------------------


def test_save_interim_creates_interim_dir(tmp_path):
    new_writer = LocalWriter(project_root=tmp_path / "fresh_project")
    df = pd.DataFrame({"x": [1, 2]})
    new_writer.save_interim(df, "init_test")
    assert (tmp_path / "fresh_project" / "data" / "interim").is_dir()


def test_save_processed_creates_processed_dir(tmp_path):
    new_writer = LocalWriter(project_root=tmp_path / "fresh_project2")
    df = pd.DataFrame({"x": [1, 2]})
    new_writer.save_processed(df, "init_test")
    assert (tmp_path / "fresh_project2" / "data" / "processed").is_dir()


# ---------------------------------------------------------------------------
# export_xlsx
# ---------------------------------------------------------------------------


def test_export_xlsx_creates_xlsx(writer, sample_df):
    sheets = {"Sheet1": sample_df}
    path = writer.export_xlsx(sheets, "frozen_report")
    assert path.exists()
    assert path.suffix == ".xlsx"


def test_export_xlsx_date_suffix_in_stem(writer, sample_df):
    import re

    sheets = {"Sheet1": sample_df}
    path = writer.export_xlsx(sheets, "my_export")
    assert re.search(r"\d{4}-\d{2}-\d{2}$", path.stem)


def test_export_xlsx_multi_sheet(writer, sample_df):
    sheets = {"Alpha": sample_df, "Beta": sample_df.copy()}
    path = writer.export_xlsx(sheets, "multi")
    xl = pd.ExcelFile(path)
    assert set(xl.sheet_names) == {"Alpha", "Beta"}


def test_export_xlsx_sheet_data_round_trip(writer, sample_df):
    sheets = {"Data": sample_df}
    path = writer.export_xlsx(sheets, "rt_xlsx")
    loaded = pd.read_excel(path, sheet_name="Data")
    assert len(loaded) == len(sample_df)
    assert set(loaded.columns) == set(sample_df.columns)


def test_export_xlsx_overwrites(writer, sample_df):
    sheets = {"Sheet1": sample_df}
    writer.export_xlsx(sheets, "overwrite_xlsx")
    path = writer.export_xlsx(sheets, "overwrite_xlsx")
    loaded = pd.read_excel(path, sheet_name="Sheet1")
    assert len(loaded) == len(sample_df)


def test_export_xlsx_returns_absolute_path(writer, sample_df):
    sheets = {"Sheet1": sample_df}
    path = writer.export_xlsx(sheets, "abs_test_xlsx")
    assert path.is_absolute()


def test_export_xlsx_outputs_dir(writer, sample_df):
    sheets = {"Sheet1": sample_df}
    path = writer.export_xlsx(sheets, "dir_test_xlsx")
    assert "outputs" in str(path)


def test_export_xlsx_sanitises_sheet_names(writer, sample_df):
    sheets = {"Frozen [EUR]": sample_df, "Frozen [GBP]": sample_df}
    path = writer.export_xlsx(sheets, "sanitised")
    xl = pd.ExcelFile(path)
    names = xl.sheet_names
    assert all("[" not in n for n in names)
    assert all(len(n) <= 31 for n in names)
    assert "Frozen EUR" in names
    assert "Frozen GBP" in names


def test_export_xlsx_total_rows_printed(writer, sample_df, capsys):
    sheets = {"A": sample_df, "B": sample_df}
    writer.export_xlsx(sheets, "total_test")
    captured = capsys.readouterr().out
    assert str(len(sample_df) * 2) in captured


# ---------------------------------------------------------------------------
# reset_outputs
# ---------------------------------------------------------------------------


def test_reset_outputs_clears_outputs_and_keeps_inputs_and_logs(writer, sample_df):
    writer.save_interim(sample_df, "transactions_validated")
    writer.save_processed(sample_df, "expense_trends")
    stale_dir = writer.processed_dir / "old_step"
    stale_dir.mkdir()
    (stale_dir / "leftover.parquet").write_bytes(b"x")
    report = writer.export_dir / "frozen_report_2026-01-01.xlsx"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_bytes(b"x")
    log = writer.export_dir / "logs" / "pipeline_2026-01-01.log"
    log.parent.mkdir(parents=True)
    log.write_text("log", encoding="utf-8")
    raw = writer.root / "data" / "raw" / "input.mmb"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"x")

    writer.reset_outputs()

    assert list(writer.interim_dir.iterdir()) == []
    assert list(writer.processed_dir.iterdir()) == []
    assert [p.name for p in writer.export_dir.iterdir()] == ["logs"]
    assert log.exists()
    assert raw.exists()


def test_reset_outputs_creates_missing_dirs(writer):
    writer.reset_outputs()
    assert writer.interim_dir.is_dir()
    assert writer.processed_dir.is_dir()
    assert writer.export_dir.is_dir()
