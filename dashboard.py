"""
app.py
------
Streamlit dashboard for the MomentumMaster Dashboard.

Provides:
  - Sidebar controls: Deriv OAuth login, start/stop, stake, Martingale settings, barriers.
  - Live tick chart (Plotly).
  - Performance metrics: Win Rate, Total P&L, Current Stake, Martingale Step.
  - Strategy state panel: Trend direction, MTF bias, pattern stage.
  - Trade history table with colour-coded outcomes.

The trading engine runs in a background asyncio thread so that the
Streamlit UI remains responsive.

Deployment:
  streamlit run app.py --server.port 8501 --server.address 0.0.0.0

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

from src.state_manager import StateManager
from src.trading_engine import TradingEngine, normalize_account_type, resolve_execution_mode
from src.api_client import DerivAPIClient, DerivAPIError
from config import (
    SYMBOL_DISPLAY,
    DEFAULT_INITIAL_STAKE,
    DEFAULT_MAX_MARTINGALE_STEPS,
    MARTINGALE_MULTIPLIER,
    BARRIER_BUY,
    BARRIER_SELL,
    DERIV_APP_ID,
    DERIV_API_TOKEN,
    STRATEGY_SENSITIVITY_PRESETS,
    DEFAULT_STRATEGY_SENSITIVITY,
)

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="MomentumMaster",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


configured_pat = DERIV_API_TOKEN.strip()


@st.cache_data(ttl=60, show_spinner=False)
def _load_accounts(app_id: str, token: str):
    """Fetch account details server-side; the PAT is never shown in the UI."""
    return asyncio.run(DerivAPIClient.get_accounts(token, app_id))

# ---------------------------------------------------------------------------
# Custom CSS for professional dark-themed dashboard
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* ---- Global ---- */
    body, .stApp { background-color: #0e1117; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; }
    [data-testid="stAppViewContainer"] { background: transparent; }
    [data-testid="stMainBlockContainer"] { max-width: 1450px; padding-top: 2.2rem; }

    /* ---- Sidebar ---- */
    [data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #30363d; }
    [data-testid="stSidebar"] .stMarkdown h2 { color: #58a6ff; font-size: 1.1rem; }

    /* ---- Metric cards ---- */
    [data-testid="stMetric"] {
        background: transparent; border: 0;
        border-radius: 12px; padding: 12px 14px; min-height: 100px;
    }
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; font-weight: 700; }
    [data-testid="stMetricLabel"] { font-size: .8rem !important; color: #8b949e; }
    .hero-card {
        background: linear-gradient(135deg, #111d2c, #161b22);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 14px 18px;
        margin: 0 0 14px;
    }
    .hero-kicker { color: #dff5ff; font-size: .78rem; font-weight: 700; letter-spacing: .09em; text-transform: uppercase; }
    .hero-title { color: #fff; font-size: 1.55rem; font-weight: 750; margin: 3px 0 6px; }
    .hero-detail { color: #e7f6ff; font-size: .92rem; }
    .risk-card { background: #fff7e6; border: 1px solid #f2cf8e; border-radius: 10px; padding: 10px 12px; color: #755213; }

    /* ---- Status banner ---- */
    .status-banner {
        padding: 12px 16px; border-radius: 10px;
        font-size: 0.95rem; font-weight: 500; margin-bottom: 12px;
    }
    .status-running { background-color: #1a3a1a; border: 1px solid #3fb950; color: #3fb950; }
    .status-stopped { background-color: #1f1f1f; border: 1px solid #484f58; color: #8b949e; }
    .status-error   { background-color: #3a1a1a; border: 1px solid #f85149; color: #f85149; }

    /* ---- Trade history table ---- */
    .trade-won  { color: #3fb950; font-weight: 600; }
    .trade-lost { color: #f85149; font-weight: 600; }
    .trade-open { color: #d29922; font-weight: 600; }
    .trade-cancelled { color: #8b949e; }

    /* ---- Section headers ---- */
    h3 { color: #58a6ff; border-bottom: 1px solid #30363d; padding-bottom: 4px; }
    [data-testid="stButton"] button { border-radius: 9px; font-weight: 650; min-height: 2.5rem; }
    [data-testid="stDataFrame"] { border: 0; }

    /* ---- Hide Streamlit branding, but keep the header visible so users can
       open the sidebar and reach the Deriv OAuth login controls. ---- */
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

state: StateManager = st.session_state.state_manager


# ---------------------------------------------------------------------------
# Background Engine Thread
# ---------------------------------------------------------------------------

def _run_engine_in_thread(engine: TradingEngine, loop: asyncio.AbstractEventLoop):
    """Target function for the background trading thread."""
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
):
    """Start the trading engine in a background thread."""
    if st.session_state.engine_thread and st.session_state.engine_thread.is_alive():
        return  # Already running

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
    )

    loop = asyncio.new_event_loop()
    st.session_state.engine_loop = loop

    thread = threading.Thread(
        target=_run_engine_in_thread,
        args=(engine, loop),
        daemon=True,
        name="TradingEngineThread",
    )
    # Attach Streamlit script context so the thread can use st functions if needed
    add_script_run_ctx(thread)
    thread.start()
    st.session_state.engine_thread = thread


def stop_bot():
    """Request the trading engine to stop."""
    state.request_stop()
    state.set_status("Stop requested. Waiting for engine to shut down...")


# ---------------------------------------------------------------------------
# Sidebar — Controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## ⚙️ Bot Configuration")
    st.divider()

    accounts = []
    account_load_error = ""
    if not DERIV_APP_ID or not configured_pat:
        account_load_error = "Add DERIV_APP_ID and DERIV_API_TOKEN to Streamlit Secrets, then reboot the app."
    else:
        try:
            accounts = _load_accounts(DERIV_APP_ID, configured_pat)
        except DerivAPIError as exc:
            account_load_error = f"Deriv PAT check failed: {exc.message}"
        except Exception:
            account_load_error = "Deriv account check failed. Confirm the App ID, PAT, scopes, and network connection."

    account_id = ""
    account_currency = "USD"
    selected_account = None
    selected_account_type = "UNKNOWN"
    live_confirmation = ""
    real_execution_confirmed = False
    execution_mode = "UNCONFIGURED"
    if account_load_error:
        st.error(account_load_error)
    elif not accounts:
        st.warning("Deriv accepted the PAT but returned no active Options accounts.")
    else:
        account_map = {account["account_id"]: account for account in accounts if account.get("account_id")}
        if not account_map:
            st.warning("Deriv returned accounts without usable account IDs.")
        else:
            account_id = st.selectbox(
                "Deriv account",
                options=list(account_map),
                disabled=state.is_running,
                format_func=lambda value: (
                    f"{normalize_account_type(account_map[value].get('account_type', 'unknown'))} | {value} | "
                    f"{account_map[value].get('currency', 'USD')} {float(account_map[value].get('balance', 0)):,.2f}"
                ),
            )
            selected_account = account_map[account_id]
            selected_account_type = normalize_account_type(selected_account.get("account_type", "unknown"))
            account_currency = str(selected_account.get("currency", "USD")).upper()
            st.success("Deriv PAT connected securely.")
            st.metric("Deriv balance", f"{account_currency} {float(selected_account.get('balance', 0)):,.2f}")
            st.caption("The PAT stays in Streamlit Secrets and is never displayed in this app.")

    st.divider()
    st.markdown("## ⚡ Execution Safety")
    if selected_account:
        if selected_account_type == "DEMO":
            st.success("Demo account selected. START will send real Deriv proposal and buy requests using demo funds.")
        elif selected_account_type == "REAL":
            live_confirmation = st.text_input(
                "Type LIVE exactly to permit real-money orders",
                value="",
                max_chars=4,
                disabled=state.is_running,
                help="This confirmation only enables real orders for the currently selected REAL account and must be re-entered before each new real-money session.",
            )
            real_execution_confirmed = live_confirmation == "LIVE"
            if real_execution_confirmed:
                st.warning("Real-money order execution is armed for this session. Orders will be sent only after a qualifying signal.")
            else:
                st.warning("Real account selected. The bot may monitor signals, but all orders remain blocked until LIVE is entered exactly.")
        else:
            st.error("The selected account has an unrecognised type. Order execution is fail-closed until Deriv returns DEMO or REAL.")

        execution_mode = resolve_execution_mode(
            selected_account_type, real_execution_confirmed
        )

    st.divider()
    st.markdown("## 💰 Money Management")

    st.caption(
        "Demo and confirmed real sessions submit orders to Deriv. Real accounts require an exact LIVE confirmation."
    )

    initial_stake = st.number_input(
        f"Initial Stake ({account_currency})",
        min_value=0.35,
        max_value=10000.0,
        value=float(DEFAULT_INITIAL_STAKE),
        step=0.5,
        format="%.2f",
        help="Starting stake for each Martingale sequence.",
    )

    max_martingale_steps = st.slider(
        "Max Martingale Steps",
        min_value=1,
        max_value=6,
        value=DEFAULT_MAX_MARTINGALE_STEPS,
        help=f"Maximum consecutive recovery steps before resetting. Multiplier: {MARTINGALE_MULTIPLIER}x",
    )

    # Show Martingale stake progression
    stakes = [initial_stake]
    for i in range(max_martingale_steps):
        stakes.append(round(stakes[-1] * MARTINGALE_MULTIPLIER, 2))
    stake_labels = [f"Step {i}: {account_currency} {s:.2f}" for i, s in enumerate(stakes)]
    st.caption("Stake Progression: " + " → ".join(stake_labels))

    st.divider()
    st.markdown("## 📡 Strategy Sensitivity")

    strategy_sensitivity = st.select_slider(
        "Entry sensitivity",
        options=list(STRATEGY_SENSITIVITY_PRESETS.keys()),
        value=DEFAULT_STRATEGY_SENSITIVITY,
        help=(
            "Conservative waits for the cleanest, most unanimous setups (fewer, "
            "higher-conviction trades). Aggressive reacts faster to shorter bursts "
            "of momentum and only needs a 2-of-3 MTF majority, so it trades more "
            "often at the cost of some precision."
        ),
    )
    st.caption(
        f"Trend quality bar: {STRATEGY_SENSITIVITY_PRESETS[strategy_sensitivity]['velocity_threshold']:.2f} "
        f"(burst {STRATEGY_SENSITIVITY_PRESETS[strategy_sensitivity]['burst_threshold']:.2f}) | "
        f"MTF agreement needed: {STRATEGY_SENSITIVITY_PRESETS[strategy_sensitivity]['mtf_min_agreement']}/3"
    )

    st.divider()
    st.markdown("## 🎯 Trade Parameters")

    barrier_buy_input = st.text_input(
        "Buy Barrier Offset",
        value=BARRIER_BUY,
        help="Positive offset for Touch-Up trades (e.g., +0.08).",
    )

    barrier_sell_input = st.text_input(
        "Sell Barrier Offset",
        value=BARRIER_SELL,
        help="Negative offset for Touch-Down trades (e.g., -0.08).",
    )

    st.divider()

    # Start / Stop buttons
    col_start, col_stop = st.columns(2)
    with col_start:
        start_pressed = st.button(
            "▶ START",
            type="primary",
            use_container_width=True,
            disabled=state.is_running,
        )
    with col_stop:
        stop_pressed = st.button(
            "⏹ STOP",
            type="secondary",
            use_container_width=True,
            disabled=not state.is_running,
        )

    if start_pressed:
        if not selected_account:
            st.error("A valid Deriv PAT and an active account are required before starting.")
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
            )
            st.rerun()

    if stop_pressed:
        stop_bot()
        st.rerun()

    st.divider()
    st.caption(
        "**Symbol:** " + SYMBOL_DISPLAY + "\n\n"
        "**Strategy:** Quality Pullback Momentum\n\n"
        "**MTF:** 5m / 15m / 30m\n\n"
        "**Contract:** Touch | 5 Ticks"
    )


# ---------------------------------------------------------------------------
# Main Dashboard
# ---------------------------------------------------------------------------
st.markdown("# 📈 MomentumMaster Dashboard")

active_execution_context = state.get_execution_context()
if state.is_running:
    display_account_id = active_execution_context["account_id"]
    display_account_type = active_execution_context["account_type"]
    display_currency = active_execution_context["currency"]
    display_mode = active_execution_context["execution_mode"]
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
    "DEMO": "DEMO API ORDERS — real Deriv orders using demo funds",
    "REAL": "REAL-MONEY API ORDERS — LIVE confirmation accepted",
    "SIGNAL_ONLY": "SIGNAL-ONLY PREVIEW — no Deriv orders will be sent",
    "BLOCKED": "ORDER EXECUTION BLOCKED — LIVE confirmation required or account type unknown",
    "UNCONFIGURED": "SELECT A DERIV ACCOUNT TO CONFIGURE EXECUTION",
}
mode_line = mode_lines.get(display_mode, "ORDER EXECUTION BLOCKED — unrecognised account configuration")
if display_account_id:
    account_line = f"{display_account_type} account • {display_account_id} • {display_currency}"
else:
    account_line = "Awaiting a secure Deriv account connection"
st.markdown(
    f"<div class='hero-card'><div class='hero-kicker'>Deriv Options Control Centre</div>"
    f"<div class='hero-title'>{html.escape(mode_line)}</div><div class='hero-detail'>{html.escape(account_line)} · "
    f"Quality pullback momentum · Proposal → buy execution is traceable in status and history</div></div>",
    unsafe_allow_html=True,
)

# --- Status Banner ---
error_msg = state.error_message
status_msg = state.status_message

if error_msg:
    st.markdown(
        f'<div class="status-banner status-error">⚠️ {html.escape(error_msg)}</div>',
        unsafe_allow_html=True,
    )
elif state.is_running:
    st.markdown(
        f'<div class="status-banner status-running">● RUNNING &nbsp;|&nbsp; {html.escape(status_msg)}</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f'<div class="status-banner status-stopped">◼ STOPPED &nbsp;|&nbsp; {html.escape(status_msg)}</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Row 1: Performance Metrics
# ---------------------------------------------------------------------------
st.markdown("### Performance Metrics")
stats = state.get_performance_stats()
strategy_state = state.get_strategy_state()

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    pnl_color = "normal" if stats["total_pnl"] == 0 else ("off" if stats["total_pnl"] < 0 else "normal")
    st.metric(f"Total P&L ({display_currency})", f"{stats['total_pnl']:+.2f}")

with col2:
    st.metric("Win Rate", f"{stats['win_rate']:.1f}%")

with col3:
    st.metric("Total Trades", stats["total_trades"])

with col4:
    st.metric("Wins / Losses", f"{stats['wins']} / {stats['losses']}")

with col5:
    st.metric(f"Current Stake ({display_currency})", f"{stats['current_stake']:.2f}")

with col6:
    st.metric("Martingale Step", f"{stats['martingale_step']} / {max_martingale_steps}")

# ---------------------------------------------------------------------------
# Row 2: Live Chart + Strategy State
# ---------------------------------------------------------------------------
col_chart, col_state = st.columns([3, 1])

with col_chart:
    st.markdown("### Live Tick Chart")
    ticks = state.get_recent_ticks()
    timestamps = state.get_tick_timestamps()
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

        # Highlight last tick
        fig.add_trace(go.Scatter(
            x=[len(ticks) - 1],
            y=[ticks[-1]],
            mode="markers",
            marker=dict(color="#3fb950", size=8, symbol="circle"),
            name=f"Current: {ticks[-1]:.4f}",
            hovertemplate="Current Price: %{y:.4f}<extra></extra>",
        ))

        fig.update_layout(
            paper_bgcolor="#0e1117",
            plot_bgcolor="#161b22",
            font=dict(color="#e0e0e0", size=11),
            xaxis=dict(
                title="Tick Number",
                gridcolor="#30363d",
                showgrid=True,
                zeroline=False,
            ),
            yaxis=dict(
                title="Price",
                gridcolor="#30363d",
                showgrid=True,
                zeroline=False,
            ),
            margin=dict(l=40, r=20, t=20, b=40),
            height=320,
            showlegend=True,
            legend=dict(
                bgcolor="#161b22",
                bordercolor="#30363d",
                borderwidth=1,
                font=dict(size=10),
            ),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for tick data... Start the bot to begin streaming.")

with col_state:
    st.markdown("### Strategy State")

    # Trend direction
    trend = strategy_state.get("trend_direction") or "—"
    trend_color = "#3fb950" if trend == "UP" else ("#f85149" if trend == "DOWN" else "#8b949e")
    st.markdown(
        f"**Trend Direction**<br>"
        f"<span style='color:{trend_color}; font-size:1.4rem; font-weight:700;'>{trend}</span>",
        unsafe_allow_html=True,
    )

    trend_kind = strategy_state.get("trend_kind")
    trend_kind_label = f" ({trend_kind})" if trend_kind else ""
    st.markdown(f"**Trend Ticks:** {strategy_state.get('trend_tick_count', 0)}{trend_kind_label}")

    # MTF Bias
    mtf = strategy_state.get("mtf_bias") or "—"
    mtf_color = "#3fb950" if mtf == "UP" else ("#f85149" if mtf == "DOWN" else "#8b949e")
    st.markdown(
        f"**MTF Bias**<br>"
        f"<span style='color:{mtf_color}; font-size:1.4rem; font-weight:700;'>{mtf}</span>",
        unsafe_allow_html=True,
    )

    # Pattern stage
    stage = strategy_state.get("pattern_stage", "IDLE")
    stage_colors = {
        "IDLE": "#8b949e",
        "PULLBACK": "#c88214",
        "MOMENTUM": "#155eef",
        "SIGNAL": "#3fb950",
    }
    stage_color = stage_colors.get(stage, "#8b949e")
    st.markdown(
        f"**Pattern Stage**<br>"
        f"<span style='color:{stage_color}; font-size:1.0rem; font-weight:600;'>{stage}</span>",
        unsafe_allow_html=True,
    )

    cooldown = strategy_state.get("in_cooldown", False)
    trades_in_trend = strategy_state.get("trades_in_trend", 0)
    st.markdown(f"**Trades in Trend:** {trades_in_trend}")
    if cooldown:
        st.markdown(
            "<span style='color:#d29922; font-weight:600;'>⏸ Cooldown Active</span>",
            unsafe_allow_html=True,
        )

    st.divider()
    if current_price:
        st.markdown(
            f"**Live Price**<br>"
            f"<span style='color:#e0e0e0; font-size:1.6rem; font-weight:700;'>{current_price:.4f}</span>",
            unsafe_allow_html=True,
        )

# ---------------------------------------------------------------------------
# Row 3: Trade History
# ---------------------------------------------------------------------------
st.markdown("### Trade History")

trade_history = state.get_trade_history()

if trade_history:
    records = []
    for t in trade_history[:50]:  # Show last 50 signals and order attempts
        pnl_str = f"+{t.pnl:.2f}" if t.pnl > 0 else f"{t.pnl:.2f}"
        execution_label = {
            "DEMO": "DEMO API ORDER",
            "REAL": "REAL API ORDER",
            "SIGNAL_ONLY": "SIGNAL-ONLY PREVIEW",
            "BLOCKED": "SAFETY BLOCKED",
        }.get(t.execution_mode, t.execution_mode)

        records.append({
            "Time (UTC)": t.timestamp,
            "Direction": t.direction,
            "Execution": execution_label,
            "Stake": f"{display_currency} {t.stake:.2f}",
            "Barrier": t.barrier,
            "Entry Price": f"{t.entry_price:.4f}",
            "Step": t.martingale_step,
            "Contract ID": t.contract_id or "—",
            "Status": t.status,
            "P&L": pnl_str,
            "Detail": t.error_message or "—",
        })

    df = pd.DataFrame(records)

    # Style the dataframe
    def style_status(val):
        if val == "WON":
            return "color: #3fb950; font-weight: bold;"
        elif val == "LOST":
            return "color: #f85149; font-weight: bold;"
        elif val == "OPEN":
            return "color: #d29922; font-weight: bold;"
        elif val == "PREVIEW":
            return "color: #58a6ff; font-weight: bold;"
        elif val == "UNKNOWN":
            return "color: #d29922; font-weight: bold;"
        elif val == "CANCELLED":
            return "color: #8b949e; font-weight: bold;"
        return "color: #8b949e;"

    def style_pnl(val):
        try:
            num = float(val.replace("+", ""))
            if num > 0:
                return "color: #3fb950; font-weight: bold;"
            elif num < 0:
                return "color: #f85149; font-weight: bold;"
        except Exception:
            pass
        return ""

    def style_direction(val):
        if val == "BUY":
            return "color: #3fb950;"
        elif val == "SELL":
            return "color: #f85149;"
        return ""

    # pandas removed Styler.applymap() in favour of Styler.map() (same
    # element-wise signature). Using .map() here keeps this working on
    # both the pandas version pinned in requirements.txt and whatever
    # newer version Streamlit Cloud may actually resolve to.
    styled_df = df.style.map(style_status, subset=["Status"]) \
                        .map(style_pnl, subset=["P&L"]) \
                        .map(style_direction, subset=["Direction"])

    st.dataframe(styled_df, use_container_width=True, height=300)
else:
    st.info("No signals, previews, or API order attempts have been recorded yet. Start the bot and wait for a qualifying signal.")

# ---------------------------------------------------------------------------
# Auto-refresh while bot is running
# ---------------------------------------------------------------------------
if state.is_running:
    time.sleep(1)
    st.rerun()
