# MMEXtend justfile
# Requires: https://github.com/casey/just
#
# Usage:
#   just run /path/to/my_finances.mmb
#   just run-csv /path/to/export.csv
#   just lab
#   just setup
#   just test

# Default: list available recipes
default:
    @just --list

# ── Pipeline ──────────────────────────────────────────────────────────────────

# Parse, validate and feature-engineer an MMEX .mmb file.
# Writes parquet checkpoints to data/interim/.
# Example: just run /home/user/finances.mmb
run mmb_path:
    python scripts/run_pipeline.py --mmb "{{mmb_path}}"

# Same as `run` but for a CSV export.
# Example: just run-csv data/raw/mmex_export_2025-03-29.csv
run-csv csv_path:
    python scripts/run_pipeline.py --csv "{{csv_path}}"

# ── Notebooks ─────────────────────────────────────────────────────────────────

# Launch JupyterLab in the notebooks/ directory.
lab:
    jupyter lab notebooks/

# ── Development ───────────────────────────────────────────────────────────────

# Install the package in editable mode with dev dependencies.
setup:
    pip install -e ".[dev]"

# Run the test suite.
test:
    pytest tests/ -v

# Run the test suite with coverage report.
test-cov:
    pytest tests/ -v --cov=src --cov-report=term-missing
