"""
Component tree for the MMEX Finance Dashboard.
Defines the tab bar and the content placeholder filled by callbacks.
"""
import dash_bootstrap_components as dbc
from dash import dcc, html

from dashboard.chat import build_chat_drawer


def build_layout() -> dbc.Container:
    return dbc.Container(
        fluid=True,
        children=[
            dbc.NavbarSimple(
                brand="MMEX Personal Finance",
                color="primary",
                dark=True,
                children=[
                    dbc.Button(
                        "\U0001f4ac Advisor",
                        id="chat-toggle",
                        color="light",
                        outline=True,
                        size="sm",
                    )
                ],
            ),
            dbc.Tabs(
                [
                    dbc.Tab(label="Budget Health",      tab_id="tab-health"),
                    dbc.Tab(label="Money Flow",         tab_id="tab-flow"),
                    dbc.Tab(label="Big Wins",           tab_id="tab-wins"),
                    dbc.Tab(label="Spending",           tab_id="tab-spending"),
                    dbc.Tab(label="Recurring Costs",    tab_id="tab-recurring"),
                    dbc.Tab(label="Runway & Forecast",  tab_id="tab-runway"),
                    dbc.Tab(label="Data Quality",       tab_id="tab-quality"),
                ],
                id="tabs",
                active_tab="tab-health",
            ),
            dcc.Loading(
                html.Div(id="tab-content", className="mt-3"),
                type="circle",
            ),
            build_chat_drawer(),
        ],
    )
