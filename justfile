# MMEXtend justfile
# Requires: https://github.com/casey/just  +  https://python-poetry.org/
#
# Usage:
#   just run /path/to/my_finances.mmb
#   just run-csv /path/to/export.csv
#   just lab
#   just install
#   just test

# Default: list available recipes
default:
    @just --list

# ── Setup ─────────────────────────────────────────────────────────────────────

# Install all dependencies (creates / updates the Poetry virtualenv).
install:
    poetry install

# Install including the Prophet/sklearn forecast extras.
install-forecast:
    poetry install --with forecast

# ── Pipeline ──────────────────────────────────────────────────────────────────

# Parse, validate and feature-engineer an MMEX .mmb file.
# Writes parquet checkpoints to data/interim/.
# Example: just run /home/user/finances.mmb
run mmb_path:
    poetry run python scripts/run_pipeline.py --mmb "{{mmb_path}}"

# Same as `run` but for a CSV export.
# Example: just run-csv data/raw/mmex_export_2025-03-29.csv
run-csv csv_path:
    poetry run python scripts/run_pipeline.py --csv "{{csv_path}}"

# ── Notebooks ─────────────────────────────────────────────────────────────────

# Launch JupyterLab in the notebooks/ directory.
lab:
    poetry run jupyter lab notebooks/

# ── Development ───────────────────────────────────────────────────────────────

# Run the test suite.
test:
    poetry run pytest tests/ -v

# Run the test suite with coverage report.
test-cov:
    poetry run pytest tests/ -v --cov=src --cov-report=term-missing

# Lint and auto-fix with ruff.
lint:
    poetry run ruff check src tests --fix

