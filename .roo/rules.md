# Roo Rules — MMEX Personal Finance Pipeline

> **Primary instructions are in `../.ai-instructions.md` at the project root.**
> Read that file first. Everything below supplements it for Roo-specific behaviour.

## How Roo Should Use This Project

1. **At the start of every task**, read `.ai-instructions.md` in the project root.
   It contains the hard rules, coding standards, and module-by-module instructions.

2. **Before working on a specific module**, read the corresponding doc:
   - `src/ingestion/` → `docs/04_INGESTION.md`
   - `src/storage/` → `docs/05_STORAGE_GCP.md`, `docs/06_STORAGE_LOCAL.md`
   - `src/analysis/` → `docs/08_FINANCE_STATISTICS.md`
   - `src/utils/` → `docs/03_LOGGING.md`
   - `notebooks/` → `docs/07_NOTEBOOK_INTERVIEW.md`

3. **Before any statistical implementation**, read `docs/08_FINANCE_STATISTICS.md`
   and use the exact function signatures and method selection logic specified there.

## Roo-Specific Rules

### Task Planning
- When given a broad task like "implement the ingestion module," break it into
  subtasks following the build order in `docs/01_PROJECT_OVERVIEW.md`:
  1. `schemas.py` (Pydantic models)
  2. `mmex_csv_parser.py`
  3. `mmex_sqlite_parser.py`
  4. `validator.py`
  5. Tests for each

### Code Generation
- Always check for existing code in the target file before writing. Extend, don't
  replace, unless explicitly asked to rewrite.
- When generating a new function in `src/analysis/`, ALWAYS include:
  - Type hints on all parameters and return type
  - Google-style docstring with Assumptions section
  - Logger calls using templates from `docs/03_LOGGING.md`
  - Confidence interval in return value (or explicit NaN + explanation)

### Environment
- The project uses **Poetry** for dependency and virtualenv management.
- The lock file `poetry.lock` MUST be committed.
- When adding a dependency, use `poetry add <pkg>` (or `poetry add --group dev <pkg>` for dev-only).
- NEVER use `pip install` directly inside the project; always use Poetry.
- The virtualenv is managed by Poetry; do not create or activate `.venv` manually.

### File Operations
- NEVER create files in `data/raw/`. That directory is user-managed.
- NEVER create CSV files in `outputs/` without the user explicitly requesting it.
- When creating new source files, follow the naming convention in `docs/02_DIRECTORY_SETUP.md`.
- When adding dependencies, add them to `pyproject.toml` via `poetry add` and justify the addition.

### Notebook Tasks
- When asked to create or edit a notebook, follow the cell structure in
  `docs/07_NOTEBOOK_INTERVIEW.md`:
  - Cell 1: imports + logger setup
  - Markdown header above every code cell
  - `display(df)` not `print(df)`
  - Named variables for all figures and DataFrames

### Error Handling
- If a task requires user input that hasn't been provided (opening balance,
  category confirmation, base currency), STOP and ask rather than guessing.
- If data is insufficient for the requested analysis, explain what's needed
  and what the minimum sample requirements are per `docs/08_FINANCE_STATISTICS.md`.

## Hard Rules Quick Reference (from .ai-instructions.md)

For fast lookup during code generation:

| ID | Rule | Check |
|----|------|-------|
| HR-1 | No forecast without CI | Does the return DataFrame have CI columns? |
| HR-2 | No conclusion without assumptions | Is there an assumptions block? |
| HR-3 | No fabricated stats | Is every number derived from the user's data? |
| HR-4 | No silent imputation | Is there a logger.warning("IMPUTATION...") call? |
| HR-5 | No financial recommendations | Does the output avoid product/tax advice? |
| HR-6 | No trusting MMEX categories | Are uncategorised transactions surfaced? |
| HR-7 | No unsolicited CSV | Is export_csv() only called on user request? |
| HR-8 | No trends on < 6 months | Is n_months checked before classification? |

## Reference Files

```
.ai-instructions.md          ← START HERE
docs/01_PROJECT_OVERVIEW.md
docs/02_DIRECTORY_SETUP.md
docs/03_LOGGING.md
docs/04_INGESTION.md
docs/05_STORAGE_GCP.md
docs/06_STORAGE_LOCAL.md
docs/07_NOTEBOOK_INTERVIEW.md
docs/08_FINANCE_STATISTICS.md
```
