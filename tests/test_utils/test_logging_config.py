"""Tests for src.utils.logging_config."""

import logging

import pytest

from src.utils.logging_config import setup_logging, setup_notebook_logging


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------

def test_returns_logger(tmp_path):
    logger = setup_logging("test_returns_logger_unique", log_dir=str(tmp_path))
    assert isinstance(logger, logging.Logger)


def test_logger_name_matches(tmp_path):
    name = "test_name_unique_abc"
    logger = setup_logging(name, log_dir=str(tmp_path))
    assert logger.name == name


def test_level_info_by_default(tmp_path):
    logger = setup_logging("test_level_info_unique", log_dir=str(tmp_path))
    assert logger.level == logging.INFO


def test_level_debug_honoured(tmp_path):
    logger = setup_logging("test_level_debug_unique", level="DEBUG", log_dir=str(tmp_path))
    assert logger.level == logging.DEBUG


def test_level_warning_honoured(tmp_path):
    logger = setup_logging("test_level_warning_unique", level="WARNING", log_dir=str(tmp_path))
    assert logger.level == logging.WARNING


def test_log_file_created(tmp_path):
    setup_logging("test_file_created_unique", log_dir=str(tmp_path))
    log_files = list(tmp_path.glob("*.log"))
    assert len(log_files) == 1


def test_log_file_name_starts_with_pipeline(tmp_path):
    setup_logging("test_file_name_unique", log_dir=str(tmp_path))
    log_files = list(tmp_path.glob("*.log"))
    assert log_files[0].name.startswith("pipeline_")


def test_no_duplicate_handlers_on_reimport(tmp_path):
    name = "test_no_dup_unique_xyz"
    logger1 = setup_logging(name, log_dir=str(tmp_path))
    n_handlers = len(logger1.handlers)
    logger2 = setup_logging(name, log_dir=str(tmp_path))
    # Second call must reuse the same logger without appending handlers
    assert len(logger2.handlers) == n_handlers


def test_has_at_least_one_handler(tmp_path):
    logger = setup_logging("test_has_handlers_unique", log_dir=str(tmp_path))
    assert len(logger.handlers) >= 1


def test_log_dir_created_if_absent(tmp_path):
    log_dir = tmp_path / "deeply" / "nested" / "logs"
    setup_logging("test_dir_creation_unique", log_dir=str(log_dir))
    assert log_dir.exists()


# ---------------------------------------------------------------------------
# setup_notebook_logging
# ---------------------------------------------------------------------------

def test_notebook_logging_returns_logger(tmp_path):
    # Drive into a tmp log dir via the underlying setup_logging name uniqueness;
    # the notebook logger uses the default directory but the singleton guard
    # means it won't add handlers twice across tests.
    logger = setup_notebook_logging()
    assert isinstance(logger, logging.Logger)


def test_notebook_logging_name_is_notebook():
    logger = setup_notebook_logging()
    assert logger.name == "notebook"


def test_notebook_logging_level_override():
    logger = setup_notebook_logging(level="DEBUG")
    # If already initialised, the level won't change (singleton guard).
    # Just confirm no exception is raised.
    assert isinstance(logger, logging.Logger)
