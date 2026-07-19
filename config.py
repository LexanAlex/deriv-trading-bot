"""
config.py
---------
Central configuration file for the Deriv Volatility 10 (1s) Trading Bot.
All constants, defaults, and environment-variable lookups are defined here.

The current Deriv PAT flow creates its account-specific WebSocket URL at
runtime, after an authenticated REST request. No fixed WebSocket URL belongs
in configuration.
"""

import os
try:
    from dotenv import load_dotenv
except ImportError:
    # Streamlit Cloud secrets work without python-dotenv. This fallback avoids
    # preventing the dashboard from starting while dependencies are installing.
    def load_dotenv() -> bool:
        return False

load_dotenv()

# ---------------------------------------------------------------------------
# Deriv API Connection Settings
# ---------------------------------------------------------------------------
# This application uses the current Deriv PAT flow.  Both values are required:
# DERIV_APP_ID is the PAT application ID; DERIV_API_TOKEN is the separate
# Personal Access Token created for that application.  No redirect URL or
# browser login is used.
def _streamlit_secret(name: str) -> str:
    """Use Streamlit Cloud secrets when no local environment variable exists."""
    try:
        import streamlit as st
        return str(st.secrets.get(name, ""))
    except Exception:
        return ""


DERIV_APP_ID = os.getenv("DERIV_APP_ID") or _streamlit_secret("DERIV_APP_ID") or ""
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN") or _streamlit_secret("DERIV_API_TOKEN") or ""

# Compatibility only: older source files may import this name. The PAT client
# does not use it; it obtains an account-specific OTP WebSocket URL at runtime.
DERIV_WS_URL = ""

# ---------------------------------------------------------------------------
# Market / Symbol Settings
# ---------------------------------------------------------------------------
SYMBOL = "1HZ10V"          # Volatility 10 (1s) Index
SYMBOL_DISPLAY = "Volatility 10 (1s) Index"

# ---------------------------------------------------------------------------
# Strategy Parameters
# ---------------------------------------------------------------------------
# v2: trend quality is now measured with Kaufman's Efficiency Ratio (ER) -
# net displacement over total path length - instead of counting what
# fraction of individual ticks moved in the trend direction. ER tolerates
# occasional counter-ticks as long as net progress is strong, which is why
# these thresholds (0.55 / 0.72) look different from the old 0.70 tick-count
# ratio; they are not directly comparable numbers. See src/strategy.py for
# the full rationale.
TREND_WINDOW_MIN = 8           # Minimum ticks for classic trend identification
TREND_WINDOW_MAX = 12          # Maximum ticks for classic trend identification
VELOCITY_THRESHOLD = 0.55      # Minimum efficiency ratio for a classic-window trend

# Fast "burst" path: catches strong, quick momentum (e.g. the move in your
# screenshot) without waiting out the full classic window. Checked first on
# every tick; falls back to the classic window if no burst qualifies.
BURST_WINDOW_MIN = 4             # Minimum ticks for a burst trend
BURST_WINDOW_MAX = 6             # Maximum ticks for a burst trend
BURST_VELOCITY_THRESHOLD = 0.72  # Higher bar than classic since the window is shorter/noisier

MAX_TRADES_PER_TREND = 1       # Maximum trades allowed per identified trend
TICK_BUFFER_SIZE = 50          # Number of recent ticks to keep in memory
MOMENTUM_CONFIRM_TICKS = 2     # Continuation ticks after a pullback to enter

# ---------------------------------------------------------------------------
# Contract / Trade Parameters
# ---------------------------------------------------------------------------
CONTRACT_TYPE_BUY = "ONETOUCH"
CONTRACT_TYPE_SELL = "ONETOUCH"
CONTRACT_DURATION = 5           # Duration in ticks
CONTRACT_DURATION_UNIT = "t"    # 't' = ticks
BARRIER_BUY = "+0.08"           # Barrier offset for Buy (Touch above)
BARRIER_SELL = "-0.08"          # Barrier offset for Sell (Touch below)
CURRENCY = "USD"

# ---------------------------------------------------------------------------
# Multi-Timeframe Confirmation Settings
# ---------------------------------------------------------------------------
# Granularity values in seconds (Deriv API supported values)
MTF_GRANULARITIES = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
}
MTF_CANDLE_COUNT = 10           # Number of candles to fetch per timeframe for trend analysis
MTF_MIN_AGREEMENT = 2           # v2: majority (2-of-3) instead of unanimous (3-of-3) alignment

# ---------------------------------------------------------------------------
# Strategy Sensitivity Presets
# ---------------------------------------------------------------------------
# Lets the dashboard offer a simple Conservative/Balanced/Aggressive control
# without touching code. "Balanced" reproduces the defaults above.
STRATEGY_SENSITIVITY_PRESETS = {
    "Conservative": {
        "velocity_threshold": 0.62,
        "burst_threshold": 0.80,
        "mtf_min_agreement": 3,
    },
    "Balanced": {
        "velocity_threshold": VELOCITY_THRESHOLD,
        "burst_threshold": BURST_VELOCITY_THRESHOLD,
        "mtf_min_agreement": MTF_MIN_AGREEMENT,
    },
    "Aggressive": {
        "velocity_threshold": 0.48,
        "burst_threshold": 0.65,
        "mtf_min_agreement": 2,
    },
}
DEFAULT_STRATEGY_SENSITIVITY = "Balanced"

# ---------------------------------------------------------------------------
# Martingale Settings
# ---------------------------------------------------------------------------
MARTINGALE_MULTIPLIER = 3.0     # User-configured stake multiplier on loss
DEFAULT_INITIAL_STAKE = 1.0     # Default initial stake in USD
DEFAULT_MAX_MARTINGALE_STEPS = 3  # Default max consecutive recovery steps

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "deriv_bot.log")
LOG_LEVEL = "INFO"
