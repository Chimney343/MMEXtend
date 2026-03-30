# GitHub Copilot Instructions

> **Primary instructions are in `../.ai-instructions.md` at the project root.**
> Read that file first. Everything below supplements it for Copilot-specific behaviour.

## Codebase Memory

Use codebase-memory-mcp first when you need to understand relationships between
functions or search across indexed files. Prefer `search_graph` and
`trace_call_path` for structure, and `search_code` for repository-wide text
search. Use ordinary file search or reads only when the memory index is not
available or the task is limited to a single file.

## How Copilot Should Use This Project

1. **Before generating code in any `src/` module**, read the corresponding doc in `docs/`.
   The mapping is in `.ai-instructions.md` under "Read these docs before writing code."

2. **Before generating code in `notebooks/`**, read `docs/07_NOTEBOOK_INTERVIEW.md`
   for the cell structure and inspection patterns.

3. **Before generating any statistical method**, read `docs/08_FINANCE_STATISTICS.md`
   to use the exact implementation specified there.

## Copilot-Specific Rules

### Inline Completions
- When completing a function that produces a forecast, ALWAYS include CI columns
  in the return DataFrame. If you are unsure whether a CI is computable, add
  columns with `float("nan")` and a `note` column explaining why.
- When completing a logging call, use the templates from `docs/03_LOGGING.md`.
- When completing an imputation, emit the IMPUTATION warning template BEFORE
  the imputation code.

### Chat / Edit Mode
- If asked to "add a forecast," ask which method to use (Prophet, Holt-Winters,
  or rolling mean) unless the data length makes it obvious per the selection logic.
- If asked to "export results," confirm the filename and path before generating
  the `export_csv()` call.
- If asked to "fix categories," surface the uncategorised transactions and ask
  the user for mapping rather than guessing.

### Test Generation
- When generating tests for analysis functions, always include:
  - A test with exactly 6 months of data (boundary for trend analysis)
  - A test with 5 months (should return `insufficient_data`)
  - A test with all-null amounts (should halt or raise)
  - A test verifying CI columns are present in forecast output

### File Generation
- New files in `src/` must include a module docstring with Usage example.
- New files must start with appropriate imports (stdlib → third-party → local).
- Every function needs type hints and a Google-style docstring.

## Environment

This project uses **Poetry** for all dependency and virtualenv management.
- Lock file `poetry.lock` is committed and must stay up to date.
- To install: `poetry install` (add `--with forecast` for Prophet/sklearn).
- To add a dependency: `poetry add <pkg>` or `poetry add --group dev <pkg>`.
- NEVER suggest `pip install` inside this project.
- Run any command inside the environment with `poetry run <cmd>` or activate with `poetry shell`.

## MMEX Database Schema

When any error or question relates to the MMEX `.mmb` file structure — table names,
column names, foreign keys, or polymorphic `REFTYPE` relations — READ
`docs/schema/mmex_schema.md` FIRST before writing or suggesting any fix.
Only suggest that the file may be outdated (source: v21) if the issue persists
after applying the information it contains.

## Reference Files

These files define project behaviour. Consult them as needed:

```
.ai-instructions.md           ← START HERE (shared rules, coding standards, module guide)
docs/01_PROJECT_OVERVIEW.md   ← Architecture, build order, tech stack
docs/02_DIRECTORY_SETUP.md    ← File naming, import paths
docs/03_LOGGING.md            ← Log levels, mandatory events, templates
docs/04_INGESTION.md          ← MMEX parsing, validation, schemas
docs/05_STORAGE_GCP.md        ← Optional GCP setup
docs/06_STORAGE_LOCAL.md      ← Parquet/CSV writing rules
docs/07_NOTEBOOK_INTERVIEW.md ← Data inspection protocol
docs/08_FINANCE_STATISTICS.md ← Statistical methods, implementation
docs/schema/mmex_schema.md    ← MMEX SQLite schema (tables, columns, relations)
```
