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


def test_export_csv_never_overwrites(writer, sample_df):
    writer.export_csv(sample_df, "no_overwrite")
    with pytest.raises(FileExistsError):
        writer.export_csv(sample_df, "no_overwrite")


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
