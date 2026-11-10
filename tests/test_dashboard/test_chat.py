"""Tests for the AI Advisor chat drawer."""

from dash import dcc, html, no_update

from dashboard import chat
from src.agent.advisor import ToolStep, TurnResult


def _collect_ids(component) -> set[str]:
    ids: set[str] = set()
    stack = [component]

    while stack:
        node = stack.pop()
        node_id = getattr(node, "id", None)
        if isinstance(node_id, str):
            ids.add(node_id)

        children = getattr(node, "children", None)
        if children is None:
            continue
        if isinstance(children, list):
            stack.extend(children)
        else:
            stack.append(children)

    return ids


def _flatten(component) -> list:
    nodes = []
    stack = [component]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            continue
        nodes.append(node)
        children = getattr(node, "children", None)
        if children is None:
            continue
        if isinstance(children, list):
            stack.extend(children)
        else:
            stack.append(children)
    return nodes


def _collect_text(component) -> str:
    return "\n".join(node for node in _flatten(component) if isinstance(node, str))


def test_drawer_shows_missing_key_alert(monkeypatch):
    monkeypatch.setattr(chat, "is_configured", lambda: False)

    drawer = chat.build_chat_drawer()
    ids = _collect_ids(drawer)
    text = _collect_text(drawer)

    assert "chat-offcanvas" in ids
    assert "chat-messages" in ids
    assert "chat-reset" in ids
    assert "chat-input" not in ids
    assert "chat-send" in ids  # hidden, keeps running= valid
    assert "Set DEEPSEEK_API_KEY" in text


def test_drawer_shows_input_when_configured(monkeypatch):
    monkeypatch.setattr(chat, "is_configured", lambda: True)

    drawer = chat.build_chat_drawer()
    ids = _collect_ids(drawer)

    assert "chat-input" in ids
    assert "chat-send" in ids
    assert "Set DEEPSEEK_API_KEY" not in _collect_text(drawer)


def test_drawer_does_not_build_the_advisor(monkeypatch):
    monkeypatch.setattr(chat, "is_configured", lambda: True)

    def fail():
        raise AssertionError("build_chat_drawer must not call get_advisor")

    monkeypatch.setattr(chat, "get_advisor", fail)

    chat.build_chat_drawer()


def test_layout_includes_chat_toggle_and_drawer(monkeypatch):
    monkeypatch.setattr(chat, "is_configured", lambda: True)
    from dashboard.layout import build_layout

    ids = _collect_ids(build_layout())

    assert "chat-toggle" in ids
    assert "chat-offcanvas" in ids


def test_render_transcript_renders_markdown_and_tool_details():
    items = [
        {"role": "user", "text": "Hello"},
        {
            "role": "assistant",
            "text": "**Answer**",
            "steps": [
                {
                    "name": "run_sql",
                    "args": {"query": "SELECT 1"},
                    "summary": "1 rows",
                    "error": False,
                }
            ],
            "error": False,
        },
    ]

    rendered = chat.render_transcript(items)
    nodes = _flatten(rendered)
    text = _collect_text(rendered)

    assert "Hello" in text
    assert "run_sql" in text
    assert "1 rows" in text
    assert "SELECT 1" in text
    assert any(isinstance(node, dcc.Markdown) for node in nodes)
    assert any(isinstance(node, html.Details) for node in nodes)


def test_render_transcript_marks_failed_turn():
    rendered = chat.render_transcript(
        [{"role": "assistant", "text": "boom", "steps": [], "error": True}]
    )

    assert "This turn was not saved" in _collect_text(rendered)


def test_submit_ignores_empty_input():
    result = chat.submit_chat(1, "   ", [])

    assert result[0] is no_update
    assert result[1] is no_update
    assert result[2] is no_update


def test_submit_appends_user_item_without_mutating_store():
    transcript = [{"role": "assistant", "text": "old"}]

    items, value, pending = chat.submit_chat(1, "  hello  ", transcript)

    assert items == [
        {"role": "assistant", "text": "old"},
        {"role": "user", "text": "hello"},
    ]
    assert transcript == [{"role": "assistant", "text": "old"}]
    assert value == ""
    assert pending == "hello"


class _FakeAdvisor:
    def __init__(self):
        self.calls = []

    def run_turn(self, history, user_text, active_tab):
        self.calls.append((history, user_text, active_tab))
        return TurnResult(
            answer="The answer",
            steps=[ToolStep("run_sql", {"query": "SELECT 1"}, "1 rows", False)],
            new_history=[
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "The answer"},
            ],
            error=False,
        )


def test_answer_updates_both_stores(monkeypatch):
    fake = _FakeAdvisor()
    monkeypatch.setattr(chat, "get_advisor", lambda: fake)
    history = [{"role": "user", "content": "old"}]
    transcript = [{"role": "user", "text": "hi"}]

    new_history, items, pending = chat.answer_chat(
        "hi", history, transcript, "tab-overview"
    )

    assert new_history == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "The answer"},
    ]
    assert items[-1]["role"] == "assistant"
    assert items[-1]["text"] == "The answer"
    assert items[-1]["steps"] == [
        {
            "name": "run_sql",
            "args": {"query": "SELECT 1"},
            "summary": "1 rows",
            "error": False,
        }
    ]
    assert pending is None
    assert fake.calls == [(history, "hi", "tab-overview")]


def test_answer_handles_missing_advisor(monkeypatch):
    monkeypatch.setattr(chat, "get_advisor", lambda: None)
    history = [{"role": "user", "content": "old"}]

    new_history, items, pending = chat.answer_chat("hi", history, [], "tab-overview")

    assert new_history == history
    assert items[-1]["error"] is True
    assert "DEEPSEEK_API_KEY" in items[-1]["text"]
    assert pending is None


def test_answer_handles_advisor_exception(monkeypatch):
    def boom():
        raise RuntimeError("no advisor")

    monkeypatch.setattr(chat, "get_advisor", boom)
    history = [{"role": "user", "content": "old"}]

    new_history, items, pending = chat.answer_chat("hi", history, [], "tab-overview")

    assert new_history == history
    assert items[-1]["error"] is True
    assert "no advisor" in items[-1]["text"]
    assert pending is None


def test_toggle_chat():
    assert chat.toggle_chat(1, False) is True
    assert chat.toggle_chat(1, True) is False
    assert chat.toggle_chat(None, False) is False


def test_reset_chat():
    assert chat.reset_chat(1) == ([], [], None)
    assert chat.reset_chat(None)[0] is no_update


def test_render_chat_delegates_to_transcript_renderer():
    assert _collect_text(chat.render_chat(None)) == _collect_text(
        chat.render_transcript(None)
    )
