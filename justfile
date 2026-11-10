# MMEXtend justfile
# Requires: https://github.com/casey/just  +  https://python-poetry.org/
#
# Every pipeline recipe rebuilds all outputs from the source file: data/interim/,
# data/processed/ and outputs/ are emptied first, then every step runs. Nothing
# is reused from an earlier run. Outputs are git-ignored.
#
# The source file comes from MMB_PATH in .env unless you pass a path:
#   just run
#   just run "C:\path\to\finances.mmb"
#   just run-csv "C:\path\to\export.csv"
#   just dashboard
#   just frozen
#   just test

set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

# Default: list available recipes
default:
    @just --list

# ── Setup ─────────────────────────────────────────────────────────────────────

# Install all dependencies (creates / updates the Poetry virtualenv).
install:
    poetry install

# Install including forecast + dashboard extras (Prophet, sklearn, dash, etc.).
install-forecast:
    poetry install --with forecast,dashboard

# Install ALL dependency groups (dev, forecast, dashboard).
install-all:
    poetry install --with dev,forecast,dashboard

# ── Pipeline ──────────────────────────────────────────────────────────────────

# Rebuild every output from an MMEX .mmb file (MMB_PATH from .env by default).
# NBP exchange rates are cached in .cache/nbp_rates.json (public reference data,
# kept between runs).
run mmb_path="":
    poetry run python scripts/run_pipeline.py {{ if mmb_path == "" { "" } else { "--mmb \"" + mmb_path + "\"" } }}

# Rebuild every output from an MMEX CSV export.
run-csv csv_path:
    poetry run python scripts/run_pipeline.py --csv "{{csv_path}}"

# Rebuild every output, then write the frozen-money .xlsx report to outputs/.
frozen mmb_path="": (run mmb_path)
    poetry run python scripts/build_frozen_report.py {{ if mmb_path == "" { "" } else { "--mmb \"" + mmb_path + "\"" } }}

# ── Dashboard ─────────────────────────────────────────────────────────────────

# Rebuild every output, then serve the dashboard at http://localhost:8050.
# Installs the forecast+dashboard extras first when they are missing.
dashboard mmb_path="":
    poetry run python -c "import dash, prophet, openai, duckdb" 2>$null; if (-not $?) { poetry install --with forecast,dashboard }
    poetry run python scripts/run_pipeline.py {{ if mmb_path == "" { "" } else { "--mmb \"" + mmb_path + "\"" } }}
    Start-Process "http://localhost:8050"
    poetry run python dashboard/app.py

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
