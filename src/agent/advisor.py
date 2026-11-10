"""
The AI Advisor: system prompt, tool schemas and the agent loop.

The model is DeepSeek's OpenAI-compatible endpoint. Thinking mode is disabled
explicitly because the non-thinking loop does not round-trip
``reasoning_content``. Enabling it raises a clear error at construction time.
"""

from __future__ import annotations

import copy
import json
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import openai
from openai import OpenAI

from src.agent.skill import Skill, load_skill, read_reference
from src.agent.sql_sandbox import SqlSandbox, quote_identifier
from src.utils.config_loader import load_config
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_TOOL_CONTENT_LIMIT = 2000
_THINKING_DISABLED = {"thinking": {"type": "disabled"}}
_TOOL_BUDGET_MESSAGE = (
    "Tool budget exhausted. Answer now from the results above. "
    "Say what you could not verify."
)
_TOOL_MARKUP_BLOCK_RE = re.compile(r"<｜[^>]*?calls>.*?</｜[^>]*?calls>", re.S)
_DSML_BLOCK_RE = re.compile(r"<\|DSML\|>.*?</\|DSML\|>", re.S | re.I)
_SPECIAL_TAG_RE = re.compile(r"</?｜[^>]*>")
_DSML_TAG_RE = re.compile(r"<\|/?DSML\|>", re.I)
_PIPE_TAG_RE = re.compile(r"<\|[^>]*\|>")
_THINKING_ERROR = (
    "thinking mode is not supported yet: DeepSeek requires every prior "
    "assistant message's reasoning_content to be passed back when tools are "
    "enabled. Set chat.thinking: false. "
    "TODO: implement reasoning_content round-tripping per "
    "https://api-docs.deepseek.com/guides/thinking_mode#tool-calls"
)

_TOOL_SPECS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Run one read-only DuckDB SELECT over the tables listed in the Data "
                "section. Returns CSV, at most the configured row limit."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A single SELECT/WITH/DESCRIBE/SUMMARIZE/SHOW query.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "describe_table",
            "description": (
                "Show a table's columns, types and five sample rows. Useful for the "
                "mmex_* tables, whose columns are not fully described in the prompt."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Exact table name."}
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_skill_reference",
            "description": (
                "Read one reference file from the I Will Teach You to Be Rich skill "
                "when detailed guidance in that area is needed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Reference name, for example spending-plan-automation.",
                    }
                },
                "required": ["name"],
            },
        },
    },
]

TOOLS: list[dict[str, Any]] = _TOOL_SPECS


def build_tools(skill: Skill, max_rows: int | None = None) -> list[dict[str, Any]]:
    """Return the tool schemas, adding the skill's reference enum."""
    tools = copy.deepcopy(_TOOL_SPECS)
    references = sorted(skill.references)
    if references:
        tools[2]["function"]["parameters"]["properties"]["name"]["enum"] = references
    if max_rows is not None:
        tools[0]["function"]["description"] = (
            "Run one read-only DuckDB SELECT over the tables listed in the Data "
            f"section. Returns CSV, at most {int(max_rows)} rows."
        )
    return tools


@dataclass
class ToolStep:
    """One tool call, recorded for the chat transcript."""

    name: str
    args: dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    error: bool = False


@dataclass
class TurnResult:
    """The answer, the tool steps and the updated conversation history."""

    answer: str
    steps: list[ToolStep] = field(default_factory=list)
    new_history: list[dict[str, Any]] = field(default_factory=list)
    error: bool = False


def build_system_prompt(
    skill: Skill,
    catalog: list[dict[str, Any]],
    today: date,
    active_tab: str | None,
) -> str:
    """Build the advisor system prompt from the skill and the live catalog."""
    references = ", ".join(sorted(skill.references)) or "(none)"

    sections = [
        (
            "You are a personal-finance advisor working on the user's own Money "
            "Manager EX (MMEX) data. The reporting currency is PLN. The user lives "
            "in Poland."
        ),
        "## Operating method: I Will Teach You to Be Rich\n" + skill.body.strip(),
        (
            "Before giving detailed advice in one of these areas, call "
            f"`read_skill_reference` with one of: {references}."
        ),
        (
            "## Localisation\n"
            "The skill is US-centric (401k, Roth IRA, HSA, USD). Apply the underlying "
            "principle and name the Polish counterpart (PPK, IKE, IKZE, Treasury "
            "retail bonds, etc.). Always flag that contribution limits, tax rules and "
            "product availability must be checked against current Polish regulations."
        ),
    ]

    data_lines = [
        "## Data",
        "Query these tables with `run_sql`. Table catalog (name, rows, columns):",
    ]
    if catalog:
        for table in catalog:
            values = table.get("values") or {}
            column_parts = []
            for name, type_ in table["columns"]:
                part = f"{name} {type_}"
                if name in values:
                    part += " \u2208 {" + ", ".join(values[name]) + "}"
                column_parts.append(part)
            data_lines.append(
                f"- `{table['name']}` ({table['rows']} rows): " + ", ".join(column_parts)
            )
    else:
        data_lines.append(
            "- (no tables loaded; the pipeline has not been run yet. Tell the user to "
            "run `just run`.)"
        )

    data_lines.extend(
        [
            "",
            "Semantics:",
            (
                "- `transactions_featured` is the main fact table. `amount_pln` is "
                "signed: deposits are positive, withdrawals and transfers are "
                "negative. `is_transfer` rows are moves between own accounts and must "
                "be excluded from income/spending. `year_month` is the string 'YYYY-MM'."
            ),
            (
                "- `monthly_aggregates` / `expense_trends`: `total_amount` is positive "
                "for both `flow_type` values ('expense', 'income'). Neither table has a "
                "`subcategory` column; use `transactions_featured` or "
                "`category_trends_*` for subcategories. `expense_trends` is fitted on a "
                "zero-filled continuous month grid, so a row with `total_amount = 0` "
                "means no spend that month, not missing data."
            ),
            (
                "- `monthly_cashflow_split`: monthly `deposits`, `withdrawals`, "
                "`disposable_income`, `savings_rate`. `savings_rate` is a fraction "
                "(0.4 = 40%)."
            ),
            (
                "- `year_month` is VARCHAR 'YYYY-MM': compare it as a string, or use "
                "the `date` column for date arithmetic. Never cast `year_month` to DATE."
            ),
            (
                "- A few future-dated rows exist. For 'last N months', bound "
                "`year_month` by the current month, not by `MAX(year_month)`. Use a "
                "strict `>` on the cutoff month to get N complete months."
            ),
            (
                "- `*_forecast*` tables: `point_estimate` with `ci80_*` / `ci95_*` "
                "bands. When `interval_method` starts with `empirical_backtest_n`, the "
                "bands come from rolling-origin backtest errors and `ci95_*` is NULL; "
                "describe them as backtest-derived, not as the model's own intervals. "
                "`cum_ci80_*` are cumulative quantiles computed per simulated path — "
                "never sum the monthly `ci80_*` columns to get a cumulative band. "
                "`backtest_cum_low_k` of `backtest_cum_n` counts the backtests whose "
                "cumulative forecast to that month came in below the actual total. "
                "When you quote a backtest-banded forecast, give that count; if it is "
                "lopsided, say the forecast has been biased in that direction, and say "
                "so plainly when `cum_point` lies outside `cum_ci80_*`. "
                "`forecast_backtest` holds the per-origin, per-horizon errors used to "
                "estimate those bands (columns `actual`, `point`, `cum_actual`, "
                "`cum_point`, `forecaster`)."
            ),
            (
                "- `expense_trends`: `trend_pvalue` is a Newey-West (HAC) p-value; "
                "`trend_pvalue_adj` is Benjamini-Hochberg adjusted. Label rising/falling "
                "only when `trend_pvalue_adj < 0.10` (the `trend_class` column already "
                "does this). `is_seasonal` is a Kruskal-Wallis test on detrended "
                "residuals, adjusted across categories; `seasonality_cv` is descriptive "
                "only. There is no consecutive-rise column."
            ),
            (
                "- `category_trends_summary`: `direction` uses the HAC OLS slope with "
                "`direction_pvalue_adj` (BH within flow type and level); `mixed` means "
                "the Prophet trend change has the opposite sign to a significant slope."
            ),
            (
                "- `budget_health` annual creep: `marginal_spend_rate` is NULL when "
                "`delta_take_home <= 0`. The ratio is defined only for extra take-home; "
                "do not treat a NULL as zero or infer a rate from falling pay."
            ),
            "- `recurring_*`: the subscription/commitment register.",
            (
                "- `mmex_*`: raw MMEX tables. Amounts there are in the account's own "
                "currency, not PLN. Use them for accounts, initial balances, assets, "
                "stocks, budgets and scheduled bills. Use the pipeline tables for "
                "spending and income figures."
            ),
        ]
    )
    sections.append("\n".join(data_lines))

    today_str = today.isoformat()
    today_month = today.strftime("%Y-%m")
    sections.append(
        f"""## Worked examples
Top expense categories over the last 12 months (from `monthly_aggregates`):
```sql
SELECT category, ROUND(SUM(total_amount), 2) AS spend_pln
FROM monthly_aggregates
WHERE flow_type = 'expense'
  AND year_month > strftime(DATE '{today_str}' - INTERVAL 12 MONTH, '%Y-%m')
  AND year_month <= '{today_month}'
GROUP BY category
ORDER BY spend_pln DESC
LIMIT 5;
```

Monthly savings rate (from `monthly_cashflow_split`):
```sql
SELECT year_month, ROUND(savings_rate * 100, 1) AS savings_rate_pct
FROM monthly_cashflow_split
WHERE year_month <= '{today_month}'
ORDER BY year_month DESC
LIMIT 12;
```"""
    )

    sections.append(
        "## Rules\n"
        "- Query before stating any number. Never invent figures.\n"
        "- Aggregate in SQL instead of pulling raw rows.\n"
        "- Say when data is missing or the history is too short.\n"
        "- Give PLN amounts with thousands separators.\n"
        "- Be concise. End actionable answers with concrete next steps, biggest wins "
        "first (Sethi style), not small expenses.\n"
        "- Never change data: the SQL tool is read-only."
    )

    tab = active_tab or "(unknown)"
    sections.append(f"Today is {today.isoformat()}. The user is viewing dashboard tab \"{tab}\".")
    return "\n\n".join(sections)


class Advisor:
    """Run one chat turn, including any tool calls, against a DeepSeek model."""

    def __init__(
        self,
        client: Any,
        model: str,
        sandbox: Any,
        skill: Skill,
        cfg: dict[str, Any],
    ) -> None:
        validate_chat_config(cfg)
        self.client = client
        self.model = model
        self.sandbox = sandbox
        self.skill = skill
        self.cfg = dict(cfg)
        self.temperature = float(cfg.get("temperature", 0.3))
        self.max_tool_iterations = max(1, int(cfg.get("max_tool_iterations", 8)))
        self.max_rows = max(1, int(cfg.get("max_rows", 200)))
        self.max_result_chars = max(1, int(cfg.get("max_result_chars", 12000)))
        self.history_turns = max(1, int(cfg.get("history_turns", 20)))

    def run_turn(
        self,
        history: list[dict[str, Any]] | None,
        user_text: str,
        active_tab: str | None = None,
    ) -> TurnResult:
        """Run the full tool-call loop and return the answer plus stored history."""
        trimmed = _trim_history(history or [], self.history_turns)
        steps: list[ToolStep] = []
        messages: list[dict[str, Any]] = []

        try:
            self.sandbox.ensure_fresh()
            system = build_system_prompt(
                self.skill,
                self.sandbox.catalog(),
                date.today(),
                active_tab,
            )
            messages = [
                {"role": "system", "content": system},
                *trimmed,
                {"role": "user", "content": user_text},
            ]
            tools = build_tools(self.skill, self.max_rows)

            for _ in range(self.max_tool_iterations):
                message = self._create(messages, tools)
                tool_calls = getattr(message, "tool_calls", None) or []

                if not tool_calls:
                    answer = _strip_tool_markup(
                        (getattr(message, "content", None) or "").strip()
                    )
                    if not answer:
                        return _fallback_result(steps, trimmed)
                    return TurnResult(
                        answer=answer,
                        steps=steps,
                        new_history=_stored_history(trimmed, user_text, messages, answer),
                    )

                messages.append(_assistant_message(message))
                for call in tool_calls:
                    name = call.function.name
                    raw_arguments = call.function.arguments or "{}"
                    try:
                        arguments = json.loads(raw_arguments)
                        if not isinstance(arguments, dict):
                            raise ValueError("tool arguments must be a JSON object")
                    except Exception as exc:
                        result = f"Invalid tool arguments: {type(exc).__name__}: {exc}"
                        steps.append(
                            ToolStep(name, {"raw": raw_arguments}, "invalid arguments", True)
                        )
                    else:
                        try:
                            result, summary, error = self._execute_tool(name, arguments)
                        except Exception as exc:
                            result = f"Tool {name} failed: {type(exc).__name__}: {exc}"
                            summary = "error"
                            error = True
                        steps.append(ToolStep(name, arguments, summary, error))

                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": call.id,
                            "content": _clip_text(result, self.max_result_chars),
                        }
                    )

            final_messages = [
                *messages,
                {"role": "system", "content": _TOOL_BUDGET_MESSAGE},
            ]
            message = self._create(final_messages, tools=None)
            answer = _strip_tool_markup((getattr(message, "content", None) or "").strip())
            if not answer:
                return _fallback_result(steps, trimmed)
            return TurnResult(
                answer=answer,
                steps=steps,
                new_history=_stored_history(trimmed, user_text, messages, answer),
            )
        except Exception as exc:
            logger.exception("Advisor turn failed")
            friendly = _friendly_api_error(exc)
            # Do not persist a failed turn: the error text is not an assistant
            # reply, and storing it would poison the next request's context.
            return TurnResult(
                answer=friendly,
                steps=steps,
                new_history=list(trimmed),
                error=True,
            )

    # ------------------------------------------------------------------
    # Model and tool plumbing
    # ------------------------------------------------------------------

    def _create(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | None = None,
    ) -> Any:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "extra_body": copy.deepcopy(_THINKING_DISABLED),
        }
        if tools is not None:
            kwargs["tools"] = tools
        if tool_choice is not None:
            kwargs["tool_choice"] = tool_choice
        response = self.client.chat.completions.create(**kwargs)
        return response.choices[0].message

    def _execute_tool(self, name: str, arguments: dict[str, Any]) -> tuple[str, str, bool]:
        if name == "run_sql":
            query = str(arguments.get("query") or "").strip()
            if not query:
                return "Error: `query` is required.", "error", True
            result = self.sandbox.query(query, self.max_rows)
            if result.error:
                return f"SQL error: {result.error}", "error", True
            summary = f"{result.row_count} rows"
            text = result.rows_csv
            if result.truncated:
                summary += f" (truncated at {self.max_rows})"
                text += (
                    f"\n\n[Result truncated to {self.max_rows} rows. "
                    "Aggregate in SQL for totals.]"
                )
            return text, summary, False

        if name == "describe_table":
            table = str(arguments.get("table") or "").strip()
            return self._describe_table(table)

        if name == "read_skill_reference":
            reference = str(arguments.get("name") or "").strip()
            if reference not in self.skill.references:
                return read_reference(self.skill, reference), "unknown reference", True
            text = read_reference(self.skill, reference)
            return text, f"reference: {reference}", text.startswith("Could not read")

        return f"Unknown tool: {name}", "unknown tool", True

    def _describe_table(self, table: str) -> tuple[str, str, bool]:
        catalog = {entry["name"]: entry for entry in self.sandbox.catalog()}
        entry = catalog.get(table)
        if entry is None:
            valid = ", ".join(sorted(catalog)) or "(none)"
            return f"Unknown table {table!r}. Valid tables: {valid}.", "unknown table", True

        columns = "\n".join(f"- {name} {type_}" for name, type_ in entry["columns"])
        sample = self.sandbox.query(
            f"SELECT * FROM {quote_identifier(table)} LIMIT 5", max_rows=5
        )
        sample_text = sample.rows_csv if not sample.error else f"sample error: {sample.error}"
        text = (
            f"Table {table} ({entry['rows']} rows)\nColumns:\n{columns}"
            f"\n\nSample rows:\n{sample_text}"
        )
        return text, f"{len(entry['columns'])} columns", False


def validate_chat_config(cfg: dict[str, Any]) -> None:
    """Fail loudly for unsupported settings before the first chat request."""
    if cfg.get("thinking", False):
        raise ValueError(_THINKING_ERROR)


def _assistant_message(message: Any) -> dict[str, Any]:
    tool_calls = []
    for call in getattr(message, "tool_calls", None) or []:
        tool_calls.append(
            {
                "id": call.id,
                "type": getattr(call, "type", "function"),
                "function": {
                    "name": call.function.name,
                    "arguments": call.function.arguments,
                },
            }
        )
    return {
        "role": "assistant",
        "content": getattr(message, "content", None),
        "tool_calls": tool_calls,
    }


def _trim_history(history: list[dict[str, Any]], max_turns: int) -> list[dict[str, Any]]:
    user_indices = [index for index, item in enumerate(history) if item.get("role") == "user"]
    if len(user_indices) <= max_turns:
        return list(history)
    return list(history[user_indices[-max_turns] :])


def _stored_history(
    trimmed: list[dict[str, Any]],
    user_text: str,
    messages: list[dict[str, Any]],
    answer: str,
) -> list[dict[str, Any]]:
    body = messages[1 + len(trimmed) + 1 :]
    stored = list(trimmed) + [{"role": "user", "content": user_text}]
    for message in body:
        stored.append(_clip_message(message))
    stored.append({"role": "assistant", "content": answer})
    return stored


def _clip_message(message: dict[str, Any]) -> dict[str, Any]:
    copied = dict(message)
    if copied.get("role") == "tool":
        copied["content"] = _clip_text(str(copied.get("content") or ""), _TOOL_CONTENT_LIMIT)
    return copied


def _clip_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    suffix = "\n...[truncated]"
    return text[: max(0, limit - len(suffix))] + suffix


def _strip_tool_markup(text: str) -> str:
    """Remove leaked DeepSeek/DSML tool-call markup from an answer."""
    if not text:
        return ""
    if "\uff5c" not in text and "<|dsml|" not in text.lower():
        return text.strip()

    cleaned = _TOOL_MARKUP_BLOCK_RE.sub("", text)
    cleaned = _DSML_BLOCK_RE.sub("", cleaned)
    cleaned = _SPECIAL_TAG_RE.sub("", cleaned)
    cleaned = _DSML_TAG_RE.sub("", cleaned)
    cleaned = _PIPE_TAG_RE.sub("", cleaned)
    return cleaned.strip()


def _fallback_result(steps: list[ToolStep], trimmed: list[dict[str, Any]]) -> TurnResult:
    return TurnResult(
        answer="I ran out of steps before finishing. Try a narrower question.",
        steps=steps,
        new_history=list(trimmed),
        error=True,
    )


def _friendly_api_error(exc: Exception) -> str:
    if isinstance(exc, openai.AuthenticationError):
        return (
            "DeepSeek rejected the API key. Check DEEPSEEK_API_KEY in .env and "
            "restart the dashboard."
        )
    if isinstance(exc, openai.APITimeoutError):
        return "DeepSeek timed out. Try again, or raise chat.request_timeout_s in configs/pipeline.yaml."
    if isinstance(exc, openai.APIConnectionError):
        return "Could not reach DeepSeek. Check your network connection and try again."
    if isinstance(exc, openai.APIError):
        return f"DeepSeek returned an API error: {exc}"
    return f"The AI advisor could not complete the request: {type(exc).__name__}: {exc}"


_ADVISOR: Advisor | None = None
_ADVISOR_LOCK = threading.Lock()


def is_configured() -> bool:
    """Return True when DEEPSEEK_API_KEY is present, without building the advisor."""
    return bool(os.getenv("DEEPSEEK_API_KEY"))


def get_advisor() -> Advisor | None:
    """Lazily build the singleton advisor. Returns None when the API key is absent."""
    global _ADVISOR
    with _ADVISOR_LOCK:
        if _ADVISOR is not None:
            return _ADVISOR

        api_key = os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            logger.warning("DEEPSEEK_API_KEY is not set; the AI Advisor is disabled")
            return None

        cfg = (load_config().get("chat") or {})
        validate_chat_config(cfg)

        mmb_raw = os.getenv("MMB_PATH")
        mmb_path = Path(mmb_raw).expanduser() if mmb_raw else None
        sandbox = SqlSandbox(
            project_root=_PROJECT_ROOT,
            mmb_path=mmb_path,
            include_mmex=bool(cfg.get("include_mmex_tables", True)),
            max_result_chars=int(cfg.get("max_result_chars", 12000)),
        )
        # The skill is a local, optional add-on: it is not shipped with the repo.
        skill_dir = _PROJECT_ROOT / str(
            cfg.get("skill_dir") or ".agents/skills/i-will-teach-you-to-be-rich"
        )
        if (skill_dir / "SKILL.md").is_file():
            skill = load_skill(skill_dir)
        else:
            logger.warning("Advisor skill not found at %s; running without it", skill_dir)
            skill = Skill(name="none", description="", body="")
        model = os.getenv("DEEPSEEK_MODEL") or str(cfg.get("model", "deepseek-flash"))
        base_url = os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com"
        client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=float(cfg.get("request_timeout_s", 90)),
            max_retries=2,
        )
        _ADVISOR = Advisor(client=client, model=model, sandbox=sandbox, skill=skill, cfg=cfg)
        logger.info("AI Advisor ready (model=%s)", model)
        return _ADVISOR


def reset_advisor() -> None:
    """Clear the singleton (used by tests and after config changes)."""
    global _ADVISOR
    with _ADVISOR_LOCK:
        _ADVISOR = None
