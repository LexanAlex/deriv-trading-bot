"""
dashboard.py
------------
Streamlit dashboard for MomentumMaster — Premium Edition.

A hedge-fund-grade trading terminal with:
  - Clean dark-navy Bloomberg-style aesthetic
  - Live tick chart with minimal chrome
  - Large, high-impact P&L / Win Rate / Trade Count metrics
  - Compact strategy state panel
  - Professional trade ledger

The trading engine runs in a background asyncio thread so the UI stays responsive.

Deployment:
  streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
"""

import asyncio
import html
import threading
import time
import sys
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(__file__))

from src.state_manager import StateManager, TradeRecord
from src.trading_engine import TradingEngine, normalize_account_type, resolve_execution_mode
from src.api_client import DerivAPIClient, DerivAPIError
from config import (
    SYMBOL, SYMBOL_DISPLAY,
    CONTRACT_TYPE_BUY, CONTRACT_TYPE_SELL,
    CONTRACT_DURATION, CONTRACT_DURATION_UNIT,
    BARRIER_BUY, BARRIER_SELL, CURRENCY,
    MARTINGALE_MULTIPLIER,
    DEFAULT_INITIAL_STAKE,
    DEFAULT_MAX_MARTINGALE_STEPS,
    STRATEGY_SENSITIVITY_PRESETS,
    DEFAULT_STRATEGY_SENSITIVITY,
    DERIV_APP_ID,
    DERIV_API_TOKEN,
)

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="MomentumMaster",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

configured_pat = DERIV_API_TOKEN.strip()


@st.cache_data(ttl=60, show_spinner=False)
def _load_accounts(app_id: str, token: str):
    """Fetch account details server-side; the PAT is never shown in the UI."""
    return asyncio.run(DerivAPIClient.get_accounts(token, app_id))


# ---------------------------------------------------------------------------
# Premium CSS — Hedge Fund Terminal Aesthetic
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* ---- Global Reset ---- */
    body, .stApp {
        background-color: #0a0e17;
        color: #c9d1d9;
        font-family: 'Inter', 'SF Pro Display', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    [data-testid="stAppViewContainer"] { background: transparent; }
    [data-testid="stMainBlockContainer"] { max-width: 1500px; padding-top: 1.5rem; }

    /* ---- Sidebar ---- */
    [data-testid="stSidebar"] {
        background-color: #0d1117;
        border-right: 1px solid #1e2d3d;
    }
    [data-testid="stSidebar"] .stMarkdown h2 {
        color: #58a6ff;
        font-size: 0.85rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }

    /* ---- Big Metric Cards ---- */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #0d1520, #111b27);
        border: 1px solid #1e2d3d;
        border-radius: 8px;
        padding: 16px 20px;
        min-height: 90px;
    }
    [data-testid="stMetricValue"] {
        font-size: 2.2rem !important;
        font-weight: 800;
        letter-spacing: -0.02em;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.72rem !important;
        color: #6e7681;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
    }

    /* ---- PnL color overrides ---- */
    .metric-pnl-positive { color: #00d4aa !important; }
    .metric-pnl-negative { color: #ff4d4f !important; }

    /* ---- Header ---- */
    .terminal-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0 0 12px 0;
        margin-bottom: 16px;
        border-bottom: 1px solid #1e2d3d;
    }
    .terminal-logo {
        font-size: 1.1rem;
        font-weight: 700;
        color: #e6edf3;
        letter-spacing: 0.12em;
        text-transform: uppercase;
    }
    .terminal-subtitle {
        font-size: 0.7rem;
        color: #6e7681;
        font-weight: 500;
        letter-spacing: 0.05em;
    }

    /* ---- Status Indicator ---- */
    .status-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 6px;
    }
    .status-running { background: #00d4aa; box-shadow: 0 0 8px #00d4aa80; }
    .status-stopped { background: #6e7681; }
    .status-error { background: #ff4d4f; box-shadow: 0 0 8px #ff4d4f80; }

    .status-bar {
        padding: 8px 14px;
        border-radius: 6px;
        font-size: 0.78rem;
        font-weight: 500;
        margin-bottom: 14px;
        letter-spacing: 0.02em;
    }
    .status-bar-running {
        background: rgba(0, 212, 170, 0.08);
        border: 1px solid rgba(0, 212, 170, 0.25);
        color: #00d4aa;
    }
    .status-bar-stopped {
        background: rgba(110, 118, 129, 0.06);
        border: 1px solid rgba(110, 118, 129, 0.2);
        color: #6e7681;
    }
    .status-bar-error {
        background: rgba(255, 77, 79, 0.08);
        border: 1px solid rgba(255, 77, 79, 0.25);
        color: #ff4d4f;
    }

    /* ---- Strategy State Panel ---- */
    .state-panel {
        background: #0d1520;
        border: 1px solid #1e2d3d;
        border-radius: 8px;
        padding: 16px;
    }
    .state-label {
        font-size: 0.68rem;
        color: #6e7681;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .state-value {
        font-size: 1.1rem;
        font-weight: 700;
        color: #e6edf3;
    }
    .state-value-up { color: #00d4aa; }
    .state-value-down { color: #ff4d4f; }
    .state-value-neutral { color: #6e7681; }

    /* ---- Trade Ledger ---- */
    .trade-ledger-header {
        font-size: 0.72rem;
        color: #6e7681;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin-bottom: 10px;
        padding-bottom: 6px;
        border-bottom: 1px solid #1e2d3d;
    }
    .trade-row-won { color: #00d4aa; font-weight: 600; }
    .trade-row-lost { color: #ff4d4f; font-weight: 600; }
    .trade-row-open { color: #f0b429; font-weight: 600; }
    .trade-row-cancelled { color: #6e7681; }

    /* ---- Buttons ---- */
    [data-testid="stButton"] button {
        border-radius: 6px;
        font-weight: 600;
        min-height: 2.5rem;
        font-size: 0.82rem;
        letter-spacing: 0.04em;
    }
    [data-testid="stDataFrame"] { border: 0; }

    /* ---- Section dividers ---- */
    [data-testid="stHorizontalRule"] {
        border-color: #1e2d3d;
        margin: 10px 0;
    }

    /* ---- Hide Streamlit branding ---- */
    #MainMenu, footer { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Session State Initialisation
# ---------------------------------------------------------------------------
if "state_manager" not in st.session_state:
    st.session_state.state_manager = StateManager()

if "engine_thread" not in st.session_state:
    st.session_state.engine_thread = None

if "engine_loop" not in st.session_state:
    st.session_state.engine_loop = None

if "engine_instance" not in st.session_state:
    st.session_state.engine_instance = None

state: StateManager = st.session_state.state_manager


# ---------------------------------------------------------------------------
# Background Engine Thread
# ---------------------------------------------------------------------------
def _run_engine_in_thread(engine: TradingEngine, loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(engine.run())


def start_bot(
    api_token: str,
    app_id: str,
    account_id: str,
    account_currency: str,
    account_type: str,
    real_execution_confirmed: bool,
    initial_stake: float,
    max_steps: int,
    barrier_buy: str,
    barrier_sell: str,
    strategy_sensitivity: str,
    martingale_multiplier: float = 2.5,
):
    if st.session_state.engine_thread and st.session_state.engine_thread.is_alive():
        return

    state.reset_for_new_session(initial_stake)
    state.set_running(True)

    engine = TradingEngine(
        api_token=api_token,
        app_id=app_id,
        account_id=account_id,
        account_currency=account_currency,
        state=state,
        initial_stake=initial_stake,
        max_martingale_steps=max_steps,
        barrier_buy=barrier_buy,
        barrier_sell=barrier_sell,
        strategy_sensitivity=strategy_sensitivity,
        account_type=account_type,
        real_execution_confirmed=real_execution_confirmed,
        martingale_multiplier=martingale_multiplier,
    )

    st.session_state.engine_instance = engine

    loop = asyncio.new_event_loop()
    st.session_state.engine_loop = loop

    thread = threading.Thread(
        target=_run_engine_in_thread,
        args=(engine, loop),
        daemon=True,
        name="TradingEngineThread",
    )
    add_script_run_ctx(thread)
    thread.start()
    st.session_state.engine_thread = thread


def stop_bot():
    state.request_stop()
    state.set_status("Stop requested.")


# ---------------------------------------------------------------------------
# Sidebar — Controls
# ---------------------------------------------------------------------------
with st.sidebar:
    # Clean sidebar header
    st.markdown(
        "<div style='font-size:0.75rem;color:#6e7681;font-weight:600;letter-spacing:0.1em;"
        "text-transform:uppercase;margin-bottom:16px;'>Configuration</div>",
        unsafe_allow_html=True,
    )

    accounts = []
    account_load_error = ""
    if not DERIV_APP_ID or not configured_pat:
        account_load_error = "Add DERIV_APP_ID and DERIV_API_TOKEN to Streamlit Secrets."
    else:
        try:
            accounts = _load_accounts(DERIV_APP_ID, configured_pat)
        except DerivAPIError as exc:
            account_load_error = f"Deriv PAT check failed: {exc.message}"
        except Exception:
            account_load_error = "Deriv account check failed. Confirm App ID, PAT, and network."

    account_id = ""
    account_currency = "USD"
    selected_account = None
    selected_account_type = "UNKNOWN"
    real_execution_confirmed = False
    execution_mode = "UNCONFIGURED"

    if account_load_error:
        st.error(account_load_error)
    elif not accounts:
        st.warning("No active Options accounts found.")
    else:
        account_map = {a["account_id"]: a for a in accounts if a.get("account_id")}
        if not account_map:
            st.warning("No usable account IDs returned.")
        else:
            account_id = st.selectbox(
                "Account",
                options=list(account_map),
                disabled=state.is_running,
                format_func=lambda v: (
                    f"{normalize_account_type(account_map[v].get('account_type','unknown'))} | "
                    f"{v} | {account_map[v].get('currency','USD')} "
                    f"{float(account_map[v].get('balance',0)):,.2f}"
                ),
            )
            selected_account = account_map[account_id]
            selected_account_type = normalize_account_type(selected_account.get("account_type", "unknown"))
            account_currency = str(selected_account.get("currency", "USD")).upper()
            st.success("Connected.")
            st.metric("Balance", f"{account_currency} {float(selected_account.get('balance', 0)):,.2f}")

    st.divider()

    # Execution safety
    st.markdown(
        "<div style='font-size:0.72rem;color:#6e7681;font-weight:600;letter-spacing:0.1em;"
        "text-transform:uppercase;margin-bottom:8px;'>Execution</div>",
        unsafe_allow_html=True,
    )

    if selected_account:
        if selected_account_type == "DEMO":
            st.success("Demo account — orders use demo funds.")
        elif selected_account_type == "REAL":
            live_confirmation = st.text_input(
                "Type LIVE to enable real-money orders",
                value="", max_chars=4, disabled=state.is_running,
            )
            real_execution_confirmed = live_confirmation == "LIVE"
            if real_execution_confirmed:
                st.warning("Real-money execution armed.")
            else:
                st.warning("Real account — orders blocked until LIVE confirmed.")
        else:
            st.error("Unknown account type. Orders blocked.")

        execution_mode = resolve_execution_mode(selected_account_type, real_execution_confirmed)

    st.divider()

    # Money management
    st.markdown(
        "<div style='font-size:0.72rem;color:#6e7681;font-weight:600;letter-spacing:0.1em;"
        "text-transform:uppercase;margin-bottom:8px;'>Money Management</div>",
        unsafe_allow_html=True,
    )

    initial_stake = st.number_input(
        "Initial Stake",
        min_value=0.35, max_value=10000.0,
        value=float(DEFAULT_INITIAL_STAKE), step=0.5, format="%.2f",
    )

    martingale_multiplier = st.slider(
        "Martingale Multiplier", 1.5, 4.0, float(MARTINGALE_MULTIPLIER),
        step=0.1, format="%.1f",
        help="Stake is multiplied by this factor after each loss.",
    )

    max_martingale_steps = st.slider(
        "Max Martingale Steps", 1, 6, DEFAULT_MAX_MARTINGALE_STEPS,
        help="Maximum consecutive recovery steps before resetting to initial stake.",
    )

    # Stake progression preview
    stakes = [initial_stake]
    for _ in range(1, max_martingale_steps):
        stakes.append(round(stakes[-1] * martingale_multiplier, 2))
    total_exposure = sum(stakes)
    stake_preview = " ".join([f"{s:.2f}" for s in stakes])
    st.caption(f"Stake progression: {stake_preview}  (Total exposure: {total_exposure:.2f})")

    st.divider()

    # Strategy sensitivity
    st.markdown(
        "<div style='font-size:0.72rem;color:#6e7681;font-weight:600;letter-spacing:0.1em;"
        "text-transform:uppercase;margin-bottom:8px;'>Sensitivity</div>",
        unsafe_allow_html=True,
    )

    strategy_sensitivity = st.select_slider(
        "Entry",
        options=list(STRATEGY_SENSITIVITY_PRESETS.keys()),
        value=DEFAULT_STRATEGY_SENSITIVITY,
    )
    preset = STRATEGY_SENSITIVITY_PRESETS[strategy_sensitivity]
    st.caption(
        f"ER {preset['velocity_threshold']:.2f} | Burst {preset['burst_threshold']:.2f} | "
        f"MTF {preset['mtf_min_agreement']}/3"
    )

    st.divider()

    # Trade parameters
    st.markdown(
        "<div style='font-size:0.72rem;color:#6e7681;font-weight:600;letter-spacing:0.1em;"
        "text-transform:uppercase;margin-bottom:8px;'>Trade Parameters</div>",
        unsafe_allow_html=True,
    )

    barrier_buy_input = st.text_input("Buy Barrier", value=BARRIER_BUY)
    barrier_sell_input = st.text_input("Sell Barrier", value=BARRIER_SELL)

    st.divider()

    # Start / Stop
    col_start, col_stop = st.columns(2)
    with col_start:
        start_pressed = st.button("START", type="primary", width='stretch', disabled=state.is_running)
    with col_stop:
        stop_pressed = st.button("STOP", type="secondary", width='stretch', disabled=not state.is_running)

    if start_pressed:
        if not selected_account:
            st.error("Select a valid Deriv account first.")
        else:
            start_bot(
                api_token=configured_pat,
                app_id=DERIV_APP_ID,
                account_id=account_id,
                account_currency=account_currency,
                account_type=selected_account_type,
                real_execution_confirmed=real_execution_confirmed,
                initial_stake=initial_stake,
        max_steps=max_martingale_steps,
        barrier_buy=barrier_buy_input,
        barrier_sell=barrier_sell_input,
        strategy_sensitivity=strategy_sensitivity,
        martingale_multiplier=martingale_multiplier,
    )
            st.rerun()

    if stop_pressed:
        stop_bot()
        st.rerun()

    st.divider()
    st.caption(
        f"**Symbol:** {SYMBOL_DISPLAY}\n\n"
        f"**Contract:** Touch | 5 Ticks\n\n"
        f"**MTF:** 5m / 15m / 30m"
    )


# ---------------------------------------------------------------------------
# Main Dashboard
# ---------------------------------------------------------------------------

# --- Header ---
active_ctx = state.get_execution_context()
if state.is_running:
    display_account_id = active_ctx["account_id"]
    display_account_type = active_ctx["account_type"]
    display_currency = active_ctx["currency"]
    display_mode = active_ctx["execution_mode"]
elif selected_account:
    display_account_id = account_id
    display_account_type = selected_account_type
    display_currency = account_currency
    display_mode = execution_mode
else:
    display_account_id = ""
    display_account_type = "UNKNOWN"
    display_currency = account_currency
    display_mode = "UNCONFIGURED"

mode_lines = {
    "DEMO": "DEMO EXECUTION",
    "REAL": "LIVE EXECUTION",
    "SIGNAL_ONLY": "SIGNAL PREVIEW",
    "BLOCKED": "EXECUTION BLOCKED",
    "UNCONFIGURED": "AWAITING ACCOUNT",
}
mode_line = mode_lines.get(display_mode, "EXECUTION BLOCKED")

st.markdown(
    f"<div class='terminal-header'>"
    f"<div><div class='terminal-logo'>MOMENTUMMASTER</div>"
    f"<div class='terminal-subtitle'>Quality Pullback Momentum | {SYMBOL_DISPLAY}</div></div>"
    f"<div style='text-align:right;'>"
    f"<span class='status-dot status-{'running' if state.is_running else 'error' if state.error_message else 'stopped'}'></span>"
    f"<span style='color:#e6edf3;font-size:0.78rem;font-weight:600;'>{mode_line}</span><br>"
    f"<span style='color:#6e7681;font-size:0.68rem;'>"
    f"{display_account_type} | {display_account_id[:8] if display_account_id else '—'} | {display_currency}"
    f"</span></div></div>",
    unsafe_allow_html=True,
)

# --- Status Bar ---
error_msg = state.error_message
status_msg = state.status_message

if error_msg:
    st.markdown(
        f'<div class="status-bar status-bar-error">{html.escape(error_msg)}</div>',
        unsafe_allow_html=True,
    )
elif state.is_running:
    st.markdown(
        f'<div class="status-bar status-bar-running">'
        f'<span class="status-dot status-running"></span>'
        f'{html.escape(status_msg)}</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f'<div class="status-bar status-bar-stopped">'
        f'<span class="status-dot status-stopped"></span>'
        f'{html.escape(status_msg)}</div>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Row 1: Big Metrics
# ---------------------------------------------------------------------------
stats = state.get_performance_stats()

col_pnl, col_wr, col_tc = st.columns(3)

with col_pnl:
    pnl = stats["total_pnl"]
    pnl_display = f"+{pnl:.2f}" if pnl > 0 else f"{pnl:.2f}" if pnl < 0 else "0.00"
    pnl_color_class = "metric-pnl-positive" if pnl > 0 else ("metric-pnl-negative" if pnl < 0 else "")
    st.metric(
        f"Net P&L ({display_currency})",
        pnl_display,
        delta=None,
        help="Total session profit/loss",
    )
    # Override metric value color via custom HTML
    st.markdown(
        f'<style>div[data-testid="stMetric"] div[data-testid="stMetricValue"] {{ color: '
        f'{"#00d4aa" if pnl > 0 else "#ff4d4f" if pnl < 0 else "#e6edf3"} !important; }}</style>',
        unsafe_allow_html=True,
    )

with col_wr:
    wr = stats["win_rate"]
    wr_color = "#00d4aa" if wr >= 55 else ("#ff4d4f" if wr < 45 else "#f0b429")
    st.metric("Win Rate", f"{wr:.1f}%")
    st.markdown(
        f'<style>div[data-testid="stMetric"]:nth-of-type(2) div[data-testid="stMetricValue"] {{ color: {wr_color} !important; }}</style>',
        unsafe_allow_html=True,
    )

with col_tc:
    mart_state = state.get_martingale_state()
    step = mart_state["step"]
    current_stake = mart_state["stake"]
    initial_stake = mart_state["initial_stake"]

    st.metric("Trades", f"{stats['total_trades']}")
    if stats["total_trades"] > 0:
        stake_label = f"{current_stake:.2f}" if current_stake != initial_stake else f"{initial_stake:.2f}"
        step_label = f"Step {step}" if step > 0 else ""
        caption_parts = [f"{stats['wins']}W / {stats['losses']}L", f"Stake: {stake_label} {display_currency}"]
        if step_label:
            caption_parts.append(step_label)
        st.caption(" | ".join(caption_parts))


# ---------------------------------------------------------------------------
# Row 2: Live Chart + Strategy State
# ---------------------------------------------------------------------------
col_chart, col_state_panel = st.columns([4, 1])

with col_chart:
    ticks = state.get_recent_ticks()
    current_price = state.current_price

    if ticks:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=list(range(len(ticks))),
            y=ticks,
            mode="lines",
            line=dict(color="#58a6ff", width=1.5),
            name="Price",
            hovertemplate="Tick %{x}<br>Price: %{y:.4f}<extra></extra>",
        ))

        # Highlight current price
        fig.add_trace(go.Scatter(
            x=[len(ticks) - 1],
            y=[ticks[-1]],
            mode="markers",
            marker=dict(color="#00d4aa", size=8, symbol="circle"),
            name=f"Current: {ticks[-1]:.4f}",
            hovertemplate="Current: %{y:.4f}<extra></extra>",
        ))

        fig.update_layout(
            paper_bgcolor="#0a0e17",
            plot_bgcolor="#0d1520",
            font=dict(color="#6e7681", size=10, family="Inter"),
            xaxis=dict(
                title="",
                gridcolor="#1e2d3d",
                showgrid=True,
                zeroline=False,
                tickcolor="#1e2d3d",
            ),
            yaxis=dict(
                title="",
                gridcolor="#1e2d3d",
                showgrid=True,
                zeroline=False,
                tickcolor="#1e2d3d",
            ),
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Waiting for tick data...")

with col_state_panel:
    strategy_state = state.get_strategy_state()

    st.markdown('<div class="state-panel">', unsafe_allow_html=True)

    # Live price
    st.markdown('<div class="state-label">Price</div>', unsafe_allow_html=True)
    if current_price:
        st.markdown(f'<div class="state-value" style="font-size:1.4rem;">{current_price:.4f}</div>', unsafe_allow_html=True)

    # Trend
    trend = strategy_state.get("trend_direction") or "—"
    trend_cls = "state-value-up" if trend == "UP" else ("state-value-down" if trend == "DOWN" else "state-value-neutral")
    st.markdown(f'<div class="state-label">Trend</div><div class="state-value {trend_cls}">{trend}</div>', unsafe_allow_html=True)

    # MTF
    mtf = strategy_state.get("mtf_bias") or "—"
    mtf_agreement = strategy_state.get("mtf_agreement", 0)
    mtf_cls = "state-value-up" if mtf == "UP" else ("state-value-down" if mtf == "DOWN" else "state-value-neutral")
    st.markdown(
        f'<div class="state-label">MTF Bias</div>'
        f'<div class="state-value {mtf_cls}">{mtf} <span style="font-size:0.75rem;color:#6e7681;">({mtf_agreement}/3)</span></div>',
        unsafe_allow_html=True,
    )

    # Per-TF
    tf_biases = strategy_state.get("mtf_tf_biases", {})
    if tf_biases:
        tf_parts = []
        for tf_key in ["5m", "15m", "30m"]:
            if tf_key in tf_biases:
                v = tf_biases[tf_key]
                c = "#00d4aa" if v == "UP" else ("#ff4d4f" if v == "DOWN" else "#f0b429")
                icon = "▲" if v == "UP" else ("▼" if v == "DOWN" else "▶")
                tf_parts.append(f'<span style="color:#6e7681;font-size:0.7rem;">{tf_key}</span>: '
                                f'<span style="color:{c};font-weight:600;font-size:0.75rem;">{icon} {v}</span>')
        if tf_parts:
            st.markdown(
                '<div style="margin-top:6px;display:flex;gap:10px;">' + " | ".join(tf_parts) + '</div>',
                unsafe_allow_html=True,
            )

    # Micro bias (v4 tick-derived seconds-scale flow)
    micro = strategy_state.get("micro_bias")
    if micro:
        m_color = "#00d4aa" if micro == "UP" else "#ff4d4f"
        m_icon = "\u25b2" if micro == "UP" else "\u25bc"
        st.markdown(
            f'<div style="margin-top:4px;"><span style="color:#6e7681;font-size:0.7rem;">1m flow</span>: '
            f'<span style="color:{m_color};font-weight:600;font-size:0.75rem;">{m_icon} {micro}</span></div>',
            unsafe_allow_html=True,
        )

    # Pattern stage
    stage = strategy_state.get("pattern_stage", "IDLE")
    stage_colors = {"IDLE": "#6e7681", "TREND": "#a78bfa", "PULLBACK": "#f0b429", "MOMENTUM": "#58a6ff", "SIGNAL": "#00d4aa"}
    st.markdown(
        f'<div class="state-label">Stage</div>'
        f'<div style="color:{stage_colors.get(stage, "#6e7681")};font-weight:600;font-size:0.85rem;">{stage}</div>',
        unsafe_allow_html=True,
    )

    # Live heartbeat: proves the engine is actively processing ticks, so
    # sitting on IDLE for a while can be told apart from being frozen/stuck.
    # Green = a tick landed in the last 3s, amber = 3-10s, red = stale/no
    # ticks (worth checking the connection or logs).
    heartbeat = state.get_tick_heartbeat()
    total_ticks = heartbeat.get("total_ticks_processed", 0)
    last_tick_time = heartbeat.get("last_tick_time")
    if last_tick_time is not None:
        age = time.time() - last_tick_time
        if age <= 3:
            hb_color, hb_label = "#00d4aa", "live"
        elif age <= 10:
            hb_color, hb_label = "#f0b429", f"{age:.0f}s ago"
        else:
            hb_color, hb_label = "#ff4d4f", f"stale ({age:.0f}s ago)"
        st.markdown(
            f'<div style="margin-top:4px;font-size:0.72rem;color:#6e7681;">'
            f'<span style="color:{hb_color};">&#9679;</span> '
            f'{total_ticks:,} ticks processed &middot; {hb_label}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="margin-top:4px;font-size:0.72rem;color:#ff4d4f;">'
            '&#9679; no ticks received yet</div>',
            unsafe_allow_html=True,
        )

    # Signal score
    score = strategy_state.get("last_signal_score", 0)
    score_breakdown = strategy_state.get("last_signal_score_breakdown", {})
    entry_mode = strategy_state.get("last_entry_mode")
    mode_suffix = f' <span style="font-size:0.68rem;color:#6e7681;">({entry_mode})</span>' if entry_mode else ""
    st.markdown(
        f'<div class="state-label">Last Score</div>'
        f'<div style="color:#e6edf3;font-weight:700;font-size:1rem;">{score}/14{mode_suffix}</div>',
        unsafe_allow_html=True,
    )

    # Cooldown (sniper pacing)
    cooldown_remaining = stats.get("cooldown_remaining", 0)
    consecutive_losses = stats.get("consecutive_losses", 0)
    if cooldown_remaining > 0:
        remaining_secs = int(cooldown_remaining)
        st.markdown(
            f'<div style="color:#f0b429;font-size:0.75rem;font-weight:600;margin-top:6px;">'
            f'Cooldown: {remaining_secs}s</div>',
            unsafe_allow_html=True,
        )
    elif consecutive_losses > 0:
        st.markdown(
            f'<div style="color:#58a6ff;font-size:0.75rem;font-weight:600;margin-top:6px;">'
            f'Consecutive losses: {consecutive_losses} | Ready</div>',
            unsafe_allow_html=True,
        )

    st.markdown('</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Row 3: Trade Ledger
# ---------------------------------------------------------------------------
st.markdown('<div class="trade-ledger-header">Trade Ledger</div>', unsafe_allow_html=True)

trade_history = state.get_trade_history()

if trade_history:
    records = []
    for t in trade_history[:50]:
        pnl_str = f"+{t.pnl:.2f}" if t.pnl > 0 else f"{t.pnl:.2f}" if t.pnl < 0 else "0.00"

        records.append({
            "Time": t.timestamp,
            "Direction": t.direction,
            "Stake": t.stake,
            "Entry": t.entry_price,
            "Status": t.status,
            "P&L": pnl_str,
            "Step": t.martingale_step,
        })

    df = pd.DataFrame(records)

    def style_status(val):
        if val == "WON":
            return "color: #00d4aa; font-weight: 700;"
        elif val == "LOST":
            return "color: #ff4d4f; font-weight: 700;"
        elif val == "OPEN":
            return "color: #f0b429; font-weight: 600;"
        elif val == "CANCELLED":
            return "color: #6e7681;"
        return ""

    def style_pnl(val):
        try:
            num = float(val.replace("+", ""))
            if num > 0:
                return "color: #00d4aa; font-weight: 700;"
            elif num < 0:
                return "color: #ff4d4f; font-weight: 700;"
        except Exception:
            pass
        return ""

    def style_direction(val):
        if val == "BUY":
            return "color: #00d4aa;"
        elif val == "SELL":
            return "color: #ff4d4f;"
        return ""

    styled_df = (
        df.style
        .map(style_status, subset=["Status"])
        .map(style_pnl, subset=["P&L"])
        .map(style_direction, subset=["Direction"])
    )

    st.dataframe(styled_df, width='stretch', height=320, hide_index=True)
else:
    st.markdown(
        '<div style="color:#6e7681;font-size:0.82rem;padding:20px 0;text-align:center;">'
        'No trades yet. Start the bot and wait for a qualifying signal.</div>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Auto-refresh while bot is running
# ---------------------------------------------------------------------------
if state.is_running:
    time.sleep(1)
    st.rerun()
