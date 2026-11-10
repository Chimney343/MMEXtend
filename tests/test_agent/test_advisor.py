"""Tests for the advisor loop, prompt and tool handling. No network calls."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.agent.advisor import (
    Advisor,
    build_system_prompt,
    build_tools,
    get_advisor,
    reset_advisor,
    validate_chat_config,
)
from src.agent.skill import Skill
from src.agent.sql_sandbox import QueryResult


def _skill(references: dict[str, Path] | None = None) -> Skill:
    return Skill(
        name="test-skill",
        description="test description",
        body="BODY: automate big wins first.",
        references=references or {},
    )


def _cfg(**overrides):
    cfg = {
        "model": "deepseek-flash",
        "thinking": False,
        "temperature": 0.3,
        "max_tool_iterations": 2,
        "max_rows": 10,
        "max_result_chars": 12000,
        "history_turns": 20,
    }
    cfg.update(overrides)
    return cfg


def _tool_call(call_id: str, name: str, arguments: str):
    return SimpleNamespace(
        id=call_id,
        type="function",
        function=SimpleNamespace(name=name, arguments=arguments),
    )


def _message(content=None, tool_calls=None):
    return SimpleNamespace(content=content, tool_calls=tool_calls)


def _response(message):
    return SimpleNamespace(choices=[SimpleNamespace(message=message)])


class _FakeCompletions:
    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _FakeClient:
    def __init__(self, script):
        self.chat = SimpleNamespace(completions=_FakeCompletions(script))


class _FakeSandbox:
    def __init__(self, result=None, catalog=None):
        self.result = result or QueryResult(columns=["n"], rows_csv="n\r\n1\r\n", row_count=1)
        self.catalog_entries = catalog or []
        self.queries = []
        self.refreshed = False

    def ensure_fresh(self):
        self.refreshed = True

    def catalog(self):
        return self.catalog_entries

    def query(self, sql, max_rows):
        self.queries.append((sql, max_rows))
        return self.result


def test_tool_call_then_answer_runs_sql_and_keeps_matching_tool_id():
    client = _FakeClient(
        [
            _response(
                _message(
                    tool_calls=[
                        _tool_call("call_1", "run_sql", '{"query": "SELECT COUNT(*) FROM t"}')
                    ]
                )
            ),
            _response(_message(content="There is one row.")),
        ]
    )
    sandbox = _FakeSandbox()
    advisor = Advisor(client, "deepseek-flash", sandbox, _skill(), _cfg())

    result = advisor.run_turn([], "How many rows?", "tab-overview")

    assert result.error is False
    assert result.answer == "There is one row."
    assert len(result.steps) == 1
    assert result.steps[0].name == "run_sql"
    assert result.steps[0].error is False
    assert sandbox.queries == [("SELECT COUNT(*) FROM t", 10)]
    assert sandbox.refreshed is True

    second_messages = client.chat.completions.calls[1]["messages"]
    tool_messages = [message for message in second_messages if message["role"] == "tool"]
    assert tool_messages[0]["tool_call_id"] == "call_1"
    assert tool_messages[0]["content"].strip() == "n\r\n1"
    assert result.new_history[-2]["role"] == "tool"


def test_every_call_disables_thinking():
    client = _FakeClient([_response(_message(content="Hello"))])
    advisor = Advisor(client, "deepseek-flash", _FakeSandbox(), _skill(), _cfg())

    advisor.run_turn([], "hi")

    assert client.chat.completions.calls[0]["extra_body"] == {
        "thinking": {"type": "disabled"}
    }
    assert all(
        call["extra_body"] == {"thinking": {"type": "disabled"}}
        for call in client.chat.completions.calls
    )


def test_iteration_limit_forces_a_final_answer():
    client = _FakeClient(
        [
            _response(
                _message(tool_calls=[_tool_call("call_1", "run_sql", '{"query": "SELECT 1"}')])
            ),
            _response(_message(content="Final answer from what I have.")),
        ]
    )
    advisor = Advisor(
        client,
        "deepseek-flash",
        _FakeSandbox(),
        _skill(),
        _cfg(max_tool_iterations=1),
    )

    result = advisor.run_turn([], "question")

    assert result.answer == "Final answer from what I have."
    assert len(client.chat.completions.calls) == 2
    final_call = client.chat.completions.calls[1]
    assert "tools" not in final_call
    assert "tool_choice" not in final_call
    assert final_call["messages"][-1]["role"] == "system"
    assert "Tool budget exhausted" in final_call["messages"][-1]["content"]
    assert final_call["extra_body"] == {"thinking": {"type": "disabled"}}


def test_api_exception_becomes_a_friendly_error_result():
    client = _FakeClient([RuntimeError("boom")])
    advisor = Advisor(client, "deepseek-flash", _FakeSandbox(), _skill(), _cfg())

    result = advisor.run_turn([], "question")

    assert result.error is True
    assert "boom" in result.answer
    assert result.new_history == []


def test_tool_markup_only_answer_returns_fallback_and_is_not_stored():
    markup = (
        "<\uff5c｜DSML｜\uff5c calls>\n"
        '<\uff5c｜DSML｜\uff5c invoke name="run_sql">\n'
        '<\uff5c｜DSML｜\uff5c parameter name="query" string="true">SELECT 1</\uff5c｜DSML｜\uff5c parameter>\n'
        "</\uff5c｜DSML｜\uff5c invoke>\n"
        "</\uff5c｜DSML｜\uff5c calls>"
    )
    client = _FakeClient(
        [
            _response(
                _message(
                    tool_calls=[
                        _tool_call("call_1", "run_sql", '{"query": "SELECT 1"}')
                    ]
                )
            ),
            _response(_message(content=markup)),
        ]
    )
    history = [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
    ]
    advisor = Advisor(
        client,
        "deepseek-flash",
        _FakeSandbox(),
        _skill(),
        _cfg(max_tool_iterations=1),
    )

    result = advisor.run_turn(history, "u2")

    assert result.error is True
    assert result.answer == "I ran out of steps before finishing. Try a narrower question."
    assert result.new_history == history


def test_tool_markup_is_stripped_from_a_normal_answer():
    content = (
        "Here are your top categories.\n\n"
        "<\uff5c｜DSML｜\uff5c calls>\n"
        '<\uff5c｜DSML｜\uff5c invoke name="run_sql">\n'
        "</\uff5c｜DSML｜\uff5c invoke>\n"
        "</\uff5c｜DSML｜\uff5c calls>"
    )
    client = _FakeClient([_response(_message(content=content))])
    advisor = Advisor(client, "deepseek-flash", _FakeSandbox(), _skill(), _cfg())

    result = advisor.run_turn([], "hi")

    assert result.error is False
    assert result.answer == "Here are your top categories."
    assert result.new_history[-1]["content"] == "Here are your top categories."


def test_failed_turn_is_not_persisted_and_retry_succeeds():
    client = _FakeClient(
        [RuntimeError("boom"), _response(_message(content="Recovered"))]
    )
    history = [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
    ]
    advisor = Advisor(client, "deepseek-flash", _FakeSandbox(), _skill(), _cfg())

    failed = advisor.run_turn(history, "u2")

    assert failed.error is True
    assert failed.new_history == history

    retried = advisor.run_turn(failed.new_history, "u2")

    assert retried.error is False
    assert retried.answer == "Recovered"
    retry_messages = client.chat.completions.calls[1]["messages"]
    assert not any("boom" in str(message.get("content")) for message in retry_messages)


def test_thinking_mode_fails_loudly():
    with pytest.raises(ValueError, match="thinking mode is not supported yet"):
        validate_chat_config({"thinking": True})

    with pytest.raises(ValueError, match="thinking mode is not supported yet"):
        Advisor(
            _FakeClient([]),
            "deepseek-flash",
            _FakeSandbox(),
            _skill(),
            _cfg(thinking=True),
        )


def test_system_prompt_contains_skill_catalog_references_and_tab():
    skill = _skill(
        {
            "banking-credit-debt": Path("banking.md"),
            "spending-plan-automation": Path("spending.md"),
        }
    )
    catalog = [
        {
            "name": "transactions_featured",
            "rows": 1000,
            "columns": [("amount_pln", "DOUBLE"), ("type", "VARCHAR")],
            "values": {"type": ["deposit", "transfer", "withdrawal"]},
        },
        {
            "name": "mmex_accountlist_v1",
            "rows": 52,
            "columns": [("ACCOUNTNAME", "VARCHAR")],
        },
    ]

    prompt = build_system_prompt(skill, catalog, date(2026, 10, 28), "tab-recurring")

    assert skill.body in prompt
    assert "banking-credit-debt" in prompt
    assert "spending-plan-automation" in prompt
    assert "transactions_featured" in prompt
    assert "mmex_accountlist_v1" in prompt
    assert "type VARCHAR \u2208 {deposit, transfer, withdrawal}" in prompt
    assert "Worked examples" in prompt
    assert "year_month > strftime" in prompt
    assert "tab-recurring" in prompt
    assert "PLN" in prompt
    assert "PPK" in prompt


def test_tools_include_reference_enum_and_row_limit():
    skill = _skill({"banking-credit-debt": Path("banking.md")})

    tools = build_tools(skill, max_rows=200)

    reference_tool = next(
        tool for tool in tools if tool["function"]["name"] == "read_skill_reference"
    )
    assert reference_tool["function"]["parameters"]["properties"]["name"]["enum"] == [
        "banking-credit-debt"
    ]
    assert "200" in tools[0]["function"]["description"]


def test_history_is_trimmed_and_stored_tool_content_is_clipped():
    client = _FakeClient(
        [
            _response(
                _message(tool_calls=[_tool_call("call_1", "run_sql", '{"query": "SELECT 1"}')])
            ),
            _response(_message(content="Done")),
        ]
    )
    sandbox = _FakeSandbox(
        result=QueryResult(columns=["x"], rows_csv="x\r\n" + "a" * 5000, row_count=1)
    )
    history = [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "content": "u2"},
        {"role": "assistant", "content": "a2"},
        {"role": "user", "content": "u3"},
        {"role": "assistant", "content": "a3"},
    ]
    advisor = Advisor(
        client,
        "deepseek-flash",
        sandbox,
        _skill(),
        _cfg(history_turns=2),
    )

    result = advisor.run_turn(history, "u4")

    user_messages = [item["content"] for item in result.new_history if item["role"] == "user"]
    assert user_messages == ["u2", "u3", "u4"]
    stored_tool = next(item for item in result.new_history if item["role"] == "tool")
    assert stored_tool["content"].endswith("[truncated]")
    assert len(stored_tool["content"]) <= 2000 + len("\n...[truncated]")

    model_tool = next(
        item
        for item in client.chat.completions.calls[1]["messages"]
        if item["role"] == "tool"
    )
    assert len(model_tool["content"]) > 2000


def test_get_advisor_returns_none_without_api_key(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    reset_advisor()

    assert get_advisor() is None


def test_get_advisor_runs_without_the_optional_skill(monkeypatch, caplog):
    import src.agent.advisor as advisor_module

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.delenv("MMB_PATH", raising=False)
    monkeypatch.setattr(
        advisor_module, "load_config", lambda: {"chat": {"skill_dir": "no/such/skill"}}
    )
    monkeypatch.setattr(advisor_module, "SqlSandbox", lambda **_kwargs: object())
    reset_advisor()
    try:
        advisor = get_advisor()
    finally:
        reset_advisor()

    assert advisor is not None
    assert advisor.skill.name == "none"
    assert advisor.skill.references == {}
    assert any("skill not found" in record.message for record in caplog.records)


def test_describe_table_includes_columns_and_sample():
    sandbox = _FakeSandbox(
        result=QueryResult(columns=["ACCOUNTNAME"], rows_csv="ACCOUNTNAME\r\nChecking\r\n"),
        catalog=[
            {
                "name": "mmex_accountlist_v1",
                "rows": 1,
                "columns": [("ACCOUNTNAME", "VARCHAR")],
            }
        ],
    )
    advisor = Advisor(_FakeClient([]), "deepseek-flash", sandbox, _skill(), _cfg())

    text, summary, error = advisor._execute_tool(
        "describe_table", {"table": "mmex_accountlist_v1"}
    )

    assert error is False
    assert "ACCOUNTNAME VARCHAR" in text
    assert "Checking" in text
    assert summary == "1 columns"
