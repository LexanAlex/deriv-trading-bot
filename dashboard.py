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
import threading
import time
import sys
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from streamlit.runtime.scriptrunner import add_script_run_ctx

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(__file__))

from src.state_manager import StateManager
from src.trading_engine import TradingEngine
from config import (
    SYMBOL_DISPLAY,
    DEFAULT_INITIAL_STAKE,
    DEFAULT_MAX_MARTINGALE_STEPS,
    MARTINGALE_MULTIPLIER,
    BARRIER_BUY,
    BARRIER_SELL,
    DERIV_APP_ID,
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

# ---------------------------------------------------------------------------
# Deriv legacy WebSocket OAuth callback
# ---------------------------------------------------------------------------
# A Deriv application registered for the legacy WebSocket API redirects back
# to its configured Redirect URL with token1, acct1, and cur1.  The returned
# token is a short-lived session token accepted by the WebSocket `authorize`
# request; it is deliberately kept only in this Streamlit browser session.
oauth_error = st.query_params.get("error")
callback_token = st.query_params.get("token1")
if callback_token:
    st.session_state.deriv_oauth_token = callback_token
    st.session_state.deriv_account_id = st.query_params.get("acct1", "")
    st.session_state.deriv_account_currency = st.query_params.get("cur1", "USD").upper()

    # Do not leave a bearer-like session token in the browser address bar,
    # history, or a subsequently copied URL.
    st.query_params.clear()

# ---------------------------------------------------------------------------
# Custom CSS for professional dark-themed dashboard
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* ---- Global ---- */
    body, .stApp { background-color: #0e1117; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; }

    /* ---- Sidebar ---- */
    [data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #30363d; }
    [data-testid="stSidebar"] .stMarkdown h2 { color: #58a6ff; font-size: 1.1rem; }

    /* ---- Metric cards ---- */
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; font-weight: 700; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; color: #8b949e; }

    /* ---- Status banner ---- */
    .status-banner {
        padding: 10px 16px; border-radius: 6px;
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


def start_bot(oauth_token: str, app_id: str, account_currency: str,
              initial_stake: float, max_steps: int, barrier_buy: str,
              barrier_sell: str, paper_trading: bool):
    """Start the trading engine in a background thread."""
    if st.session_state.engine_thread and st.session_state.engine_thread.is_alive():
        return  # Already running

    state.reset_for_new_session(initial_stake)
    state.set_running(True)

    engine = TradingEngine(
        api_token=oauth_token,
        app_id=app_id,
        account_currency=account_currency,
        state=state,
        initial_stake=initial_stake,
        max_martingale_steps=max_steps,
        barrier_buy=barrier_buy,
        barrier_sell=barrier_sell,
        paper_trading=paper_trading,
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

    oauth_token = st.session_state.get("deriv_oauth_token")
    account_id = st.session_state.get("deriv_account_id", "")
    account_currency = st.session_state.get("deriv_account_currency", "USD")

    if oauth_error:
        st.error(f"Deriv login was not completed: {oauth_error}")

    if oauth_token:
        st.success(f"Logged in: {account_id or 'Deriv account'} ({account_currency})")
        if st.button("Log out", use_container_width=True):
            for key in ("deriv_oauth_token", "deriv_account_id", "deriv_account_currency"):
                st.session_state.pop(key, None)
            st.rerun()
    elif DERIV_APP_ID == "1089":
        st.error("Set DERIV_APP_ID to your registered Deriv application ID before logging in.")
    else:
        oauth_login_url = (
            "https://oauth.deriv.com/oauth2/authorize"
            f"?app_id={DERIV_APP_ID}&l=en"
        )
        # This self-contained login control performs a user-clicked top-level
        # navigation so Deriv can return to the Streamlit app after login.
        components.html(
            f'''<!doctype html><html><head><style>
              body {{ margin: 0; font-family: sans-serif; }}
              a {{ display: block; box-sizing: border-box; width: 100%; text-align: center;
                   background: #ff444f; color: white; padding: 0.62rem 1rem;
                   border-radius: 0.45rem; text-decoration: none; font-weight: 600; }}
              a:hover {{ background: #e03a44; }}
            </style></head><body>
              <a href="{oauth_login_url}" target="_top">Login with Deriv</a>
            </body></html>''',
            height=48,
            scrolling=False,
        )
        st.caption("Deriv must have this exact Streamlit URL registered as its OAuth Redirect URL.")

    st.caption(f"Registered Deriv App ID: `{DERIV_APP_ID}`")

    st.divider()
    st.markdown("## 💰 Money Management")

    trading_mode = st.radio(
        "Trading mode",
        options=("Paper", "Live"),
        index=0,
        help="Paper mode records signals but never sends a buy request. Live mode can place real trades.",
    )
    paper_trading = trading_mode == "Paper"
    live_confirmation = ""
    if not paper_trading:
        live_confirmation = st.text_input(
            "Type LIVE to enable real trading",
            help="This confirmation is required before the bot can place live trades.",
        )

    initial_stake = st.number_input(
        "Initial Stake (USD)",
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
        help=f"Maximum consecutive loss doublings before resetting. Multiplier: {MARTINGALE_MULTIPLIER}x",
    )

    # Show Martingale stake progression
    stakes = [initial_stake]
    for i in range(max_martingale_steps):
        stakes.append(round(stakes[-1] * MARTINGALE_MULTIPLIER, 2))
    stake_labels = [f"Step {i}: ${s:.2f}" for i, s in enumerate(stakes)]
    st.caption("Stake Progression: " + " → ".join(stake_labels))

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
        if not oauth_token:
            st.error("Log in with Deriv before starting the bot.")
        elif not paper_trading and live_confirmation != "LIVE":
            st.error("Type LIVE exactly to start live trading.")
        else:
            start_bot(
                oauth_token=oauth_token,
                app_id=DERIV_APP_ID,
                account_currency=account_currency,
                initial_stake=initial_stake,
                max_steps=max_martingale_steps,
                barrier_buy=barrier_buy_input,
                barrier_sell=barrier_sell_input,
                paper_trading=paper_trading,
            )
            st.rerun()

    if stop_pressed:
        stop_bot()
        st.rerun()

    st.divider()
    st.caption(
        "**Symbol:** " + SYMBOL_DISPLAY + "\n\n"
        "**Strategy:** 1-3-1 Tick Pattern\n\n"
        "**MTF:** 5m / 15m / 30m\n\n"
        "**Contract:** Touch | 5 Ticks"
    )


# ---------------------------------------------------------------------------
# Main Dashboard
# ---------------------------------------------------------------------------
st.markdown("# 📈 MomentumMaster Dashboard")

# --- Status Banner ---
error_msg = state.error_message
status_msg = state.status_message

if error_msg:
    st.markdown(
        f'<div class="status-banner status-error">⚠️ {error_msg}</div>',
        unsafe_allow_html=True,
    )
elif state.is_running:
    st.markdown(
        f'<div class="status-banner status-running">● RUNNING &nbsp;|&nbsp; {status_msg}</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f'<div class="status-banner status-stopped">◼ STOPPED &nbsp;|&nbsp; {status_msg}</div>',
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
    st.metric("Total P&L (USD)", f"${stats['total_pnl']:+.2f}")

with col2:
    st.metric("Win Rate", f"{stats['win_rate']:.1f}%")

with col3:
    st.metric("Total Trades", stats["total_trades"])

with col4:
    st.metric("Wins / Losses", f"{stats['wins']} / {stats['losses']}")

with col5:
    st.metric("Current Stake", f"${stats['current_stake']:.2f}")

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

    st.markdown(f"**Trend Ticks:** {strategy_state.get('trend_tick_count', 0)}")

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
        "INITIAL_RETRACE": "#d29922",
        "MOMENTUM_1": "#388bfd",
        "MOMENTUM_2": "#388bfd",
        "MOMENTUM_3": "#388bfd",
        "FINAL_RETRACE": "#bc8cff",
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
    for t in trade_history[:50]:  # Show last 50 trades
        status_html = {
            "WON": f"<span class='trade-won'>✔ WON</span>",
            "LOST": f"<span class='trade-lost'>✘ LOST</span>",
            "OPEN": f"<span class='trade-open'>◌ OPEN</span>",
            "CANCELLED": f"<span class='trade-cancelled'>— CANCELLED</span>",
        }.get(t.status, t.status)

        pnl_str = f"+{t.pnl:.2f}" if t.pnl > 0 else f"{t.pnl:.2f}"
        pnl_color = "color:#3fb950;" if t.pnl > 0 else ("color:#f85149;" if t.pnl < 0 else "")

        records.append({
            "Time (UTC)": t.timestamp,
            "Direction": t.direction,
            "Stake": f"${t.stake:.2f}",
            "Barrier": t.barrier,
            "Entry Price": f"{t.entry_price:.4f}",
            "Step": t.martingale_step,
            "Status": t.status,
            "P&L": pnl_str,
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

    styled_df = df.style.applymap(style_status, subset=["Status"]) \
                        .applymap(style_pnl, subset=["P&L"]) \
                        .applymap(style_direction, subset=["Direction"])

    st.dataframe(styled_df, use_container_width=True, height=300)
else:
    st.info("No trades executed yet. Start the bot to begin trading.")

# ---------------------------------------------------------------------------
# Auto-refresh while bot is running
# ---------------------------------------------------------------------------
if state.is_running:
    time.sleep(1)
    st.rerun()
