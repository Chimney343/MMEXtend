"""
MMEX Finance Dashboard — Dash server entry point.

Usage:
    poetry run python dashboard/app.py
    # -> http://localhost:8050
"""
import sys
from pathlib import Path

# Ensure project root is on sys.path so `src` and `dashboard` are importable.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from src.agent.advisor import validate_chat_config  # noqa: E402
from src.utils.config_loader import load_config  # noqa: E402

# Fail loudly at startup for unsupported settings (e.g. chat.thinking: true).
validate_chat_config(load_config().get("chat") or {})

import dash  # noqa: E402
import dash_bootstrap_components as dbc  # noqa: E402
from dashboard.layout import build_layout  # noqa: E402

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="MMEX Finance Dashboard",
    suppress_callback_exceptions=True,
)
app.layout = build_layout()

# Import callbacks — registers all @callback decorators as a side-effect.
import dashboard.callbacks  # noqa: F401, E402
import dashboard.chat  # noqa: F401, E402

if __name__ == "__main__":
    app.run(debug=True, port=8050)
