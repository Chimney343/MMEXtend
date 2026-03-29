"""
Centralised logging configuration.

Usage (in any module):
    from src.utils.logging_config import setup_logging
    logger = setup_logging(__name__)

Usage (in notebooks):
    from src.utils.logging_config import setup_notebook_logging
    logger = setup_notebook_logging()
"""

import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logging(
    name: str = "mmex_pipeline",
    level: str = "INFO",
    log_dir: str = "outputs/logs",
) -> logging.Logger:
    """
    Configure and return a named logger.

    Outputs to stderr (for notebook cell display) and a date-stamped file in
    log_dir.

    Parameters
    ----------
    name : str
        Logger name. Use __name__ from the calling module.
    level : str
        One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
    log_dir : str
        Directory for persistent log files. Created if absent.

    Returns
    -------
    logging.Logger
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers on re-import
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (shows in notebook cells / terminal)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler (persistent)
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"pipeline_{today}.log"),
        mode="a",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def setup_notebook_logging(level: str = "INFO") -> logging.Logger:
    """Convenience wrapper for Jupyter notebooks."""
    return setup_logging(name="notebook", level=level)
