"""
AI Advisor chat drawer for the MMEX Finance Dashboard.

The drawer is deliberately separate from ``dashboard/callbacks.py``: it is not
a tab, and the conversation survives tab switches because the Offcanvas sits
outside ``#tab-content``.
"""

import json

import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update

from src.agent.advisor import get_advisor, is_configured
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)

_PRIVACY_NOTICE = "Your question and query results are sent to DeepSeek's API."


def build_chat_drawer() -> html.Div:
    """Build the right-side chat drawer and its session stores."""
    configured = is_configured()

    header = html.Div(
        [
            html.Small(_PRIVACY_NOTICE, className="text-muted d-block"),
            dbc.Button(
                "New chat",
                id="chat-reset",
                color="secondary",
                outline=True,
                size="sm",
            ),
        ],
        className="d-flex justify-content-between align-items-center gap-2 mb-2",
    )

    if configured:
        controls = html.Div(
            [
                dbc.Textarea(
                    id="chat-input",
                    placeholder="Ask about your finances\u2026",
                    rows=3,
                    className="mb-2",
                ),
                dbc.Button("Send", id="chat-send", color="primary", className="w-100"),
            ]
        )
    else:
        controls = html.Div(
            [
                dbc.Alert(
                    "Set DEEPSEEK_API_KEY in .env to enable the AI Advisor.",
                    color="warning",
                    className="mb-0",
                ),
                # Keep the running= output target present even when the input row
                # is replaced by the alert.
                dbc.Button("Send", id="chat-send", disabled=True, style={"display": "none"}),
            ]
        )

    offcanvas = dbc.Offcanvas(
        id="chat-offcanvas",
        title="AI Advisor",
        placement="end",
        is_open=False,
        backdrop=False,
        scrollable=True,
        style={"width": "480px"},
        children=[
            header,
            dcc.Loading(
                html.Div(
                    id="chat-messages",
                    style={
                        "display": "flex",
                        "flexDirection": "column",
                        "gap": "0.75rem",
                        "minHeight": "200px",
                        "maxHeight": "55vh",
                        "overflowY": "auto",
                        "padding": "0.25rem",
                    },
                ),
                type="circle",
            ),
            controls,
        ],
    )

    return html.Div(
        [
            offcanvas,
            dcc.Store(id="chat-history", storage_type="session"),
            dcc.Store(id="chat-transcript", storage_type="session"),
            dcc.Store(id="chat-pending"),
        ]
    )


def render_transcript(items: list[dict] | None) -> list:
    """Render the stored transcript as user bubbles, Markdown and tool steps."""
    if not items:
        return [
            html.Div(
                "Ask about your finances to get started.",
                className="text-muted small p-2",
            )
        ]

    rendered = []
    for item in items:
        role = item.get("role")
        text = str(item.get("text") or "")

        if role == "user":
            rendered.append(
                html.Div(
                    text,
                    className="align-self-end bg-primary text-white rounded p-2",
                    style={"maxWidth": "85%", "whiteSpace": "pre-wrap"},
                )
            )
            continue

        if role != "assistant":
            continue

        children = [_render_step(step) for step in item.get("steps") or []]
        if text:
            children.append(dcc.Markdown(text, link_target="_blank"))
        if item.get("error"):
            children.append(
                html.Div(
                    "This turn was not saved. You can retry.",
                    className="text-danger small mt-1",
                )
            )
        rendered.append(
            html.Div(
                children,
                className="align-self-start bg-light rounded p-2",
                style={"maxWidth": "95%"},
            )
        )

    return rendered


def _render_step(step: dict) -> html.Details:
    error = bool(step.get("error"))
    name = step.get("name") or "tool"
    summary = step.get("summary") or ""
    return html.Details(
        [
            html.Summary(f"\U0001f50e {name} \u00b7 {summary}", className="small"),
            html.Pre(
                _step_body(step),
                className="small mt-2 mb-0",
                style={
                    "whiteSpace": "pre-wrap",
                    "maxHeight": "200px",
                    "overflowY": "auto",
                },
            ),
        ],
        className="text-danger mb-1" if error else "mb-1",
    )


def _step_body(step: dict) -> str:
    args = step.get("args") or {}
    if step.get("name") == "run_sql" and "query" in args:
        return str(args["query"])
    if "raw" in args:
        return str(args["raw"])
    return json.dumps(args, indent=2, ensure_ascii=False)


@callback(
    Output("chat-offcanvas", "is_open"),
    Input("chat-toggle", "n_clicks"),
    State("chat-offcanvas", "is_open"),
    prevent_initial_call=True,
)
def toggle_chat(n_clicks, is_open):
    """Open or close the drawer from the navbar button."""
    if not n_clicks:
        return is_open
    return not is_open


@callback(
    Output("chat-transcript", "data", allow_duplicate=True),
    Output("chat-input", "value"),
    Output("chat-pending", "data", allow_duplicate=True),
    Input("chat-send", "n_clicks"),
    State("chat-input", "value"),
    State("chat-transcript", "data"),
    prevent_initial_call=True,
)
def submit_chat(n_clicks, text, transcript):
    """Append the user's message immediately and queue the slow answer."""
    if not n_clicks:
        return no_update, no_update, no_update
    text = (text or "").strip()
    if not text:
        return no_update, no_update, no_update

    items = list(transcript or [])
    items.append({"role": "user", "text": text})
    return items, "", text


@callback(
    Output("chat-history", "data"),
    Output("chat-transcript", "data"),
    Output("chat-pending", "data"),
    Input("chat-pending", "data"),
    State("chat-history", "data"),
    State("chat-transcript", "data"),
    State("tabs", "active_tab"),
    running=[
        (Output("chat-send", "disabled"), True, False),
        (Output("chat-reset", "disabled"), True, False),
    ],
    prevent_initial_call=True,
)
def answer_chat(pending, history, transcript, active_tab):
    """Run the advisor loop and append the answer plus tool steps."""
    if not pending:
        return no_update, no_update, no_update

    items = list(transcript or [])
    try:
        advisor = get_advisor()
        if advisor is None:
            items.append(
                {
                    "role": "assistant",
                    "text": "Set DEEPSEEK_API_KEY in .env to enable the AI Advisor.",
                    "steps": [],
                    "error": True,
                }
            )
            return history or [], items, None

        result = advisor.run_turn(history or [], pending, active_tab)
        steps = [
            {
                "name": step.name,
                "args": step.args,
                "summary": step.summary,
                "error": step.error,
            }
            for step in result.steps
        ]
        items.append(
            {
                "role": "assistant",
                "text": result.answer,
                "steps": steps,
                "error": result.error,
            }
        )
        return result.new_history, items, None
    except Exception as exc:
        logger.exception("Chat turn failed")
        items.append(
            {
                "role": "assistant",
                "text": (
                    "The AI Advisor could not start: "
                    f"{type(exc).__name__}: {exc}"
                ),
                "steps": [],
                "error": True,
            }
        )
        return history or [], items, None


@callback(Output("chat-messages", "children"), Input("chat-transcript", "data"))
def render_chat(transcript):
    """Render the transcript whenever it changes."""
    return render_transcript(transcript)


@callback(
    Output("chat-history", "data", allow_duplicate=True),
    Output("chat-transcript", "data", allow_duplicate=True),
    Output("chat-pending", "data", allow_duplicate=True),
    Input("chat-reset", "n_clicks"),
    prevent_initial_call=True,
)
def reset_chat(n_clicks):
    """Clear the conversation and any pending question."""
    if not n_clicks:
        return no_update, no_update, no_update
    return [], [], None
