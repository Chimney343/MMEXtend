# mmex-forecast

Personal finance forecasting pipeline for Money Manager EX (MMEX) data.

**Full documentation:** [`docs/01_PROJECT_OVERVIEW.md`](docs/01_PROJECT_OVERVIEW.md)

## Quick Start

```bash
pip install -e ".[dev]"
cp configs/pipeline.yaml.example configs/pipeline.yaml
# Place your MMEX export in data/raw/
jupyter lab notebooks/
```

## Documentation Index

| Doc | Contents |
|-----|----------|
| [`01_PROJECT_OVERVIEW.md`](docs/01_PROJECT_OVERVIEW.md) | Architecture, build order, tech stack |
| [`02_DIRECTORY_SETUP.md`](docs/02_DIRECTORY_SETUP.md) | Every directory explained |
| [`03_LOGGING.md`](docs/03_LOGGING.md) | Structured logging strategy |
| [`04_INGESTION.md`](docs/04_INGESTION.md) | MMEX CSV + SQLite parsing |
| [`05_STORAGE_GCP.md`](docs/05_STORAGE_GCP.md) | Optional GCP integration |
| [`06_STORAGE_LOCAL.md`](docs/06_STORAGE_LOCAL.md) | Local parquet + CSV storage |
| [`07_NOTEBOOK_INTERVIEW.md`](docs/07_NOTEBOOK_INTERVIEW.md) | Data inspection at every stage |
| [`08_FINANCE_STATISTICS.md`](docs/08_FINANCE_STATISTICS.md) | Statistical methods reference |
| [`09_AI_AGENT_INSTRUCTIONS.md`](docs/09_AI_AGENT_INSTRUCTIONS.md) | Copilot + Roo setup |
