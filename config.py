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
# v4: reduced from 2 to 1 - waiting for two continuation ticks on a 5-tick
# contract gave away too much of the move; the scoring gate now carries the
# quality filtering instead of the tick count.
MOMENTUM_CONFIRM_TICKS = 1     # Continuation ticks after a pullback to enter

# ---------------------------------------------------------------------------
# v4: Multi-Factor Scoring Gate (early-capture edition)
# ---------------------------------------------------------------------------
# Every candidate signal is scored across four dimensions:
#   Trend Quality / ER (0-5), Higher-TF Context (0-4, hard block only on
#   unanimous 3-of-3 against), Momentum Consistency (0-3), Early Capture
#   (0-2), with a -2 penalty when the tick micro-bias opposes the entry.
# A minimum composite score of ENTRY_SCORE_THRESHOLD (out of 14 possible)
# is required to fire a trade.
ENTRY_SCORE_THRESHOLD = 7      # Minimum composite score to enter a trade (out of 14)

# A trend is "young" for this many ticks after it is first detected. Young
# trends may fire IMMEDIATE entries (no pullback needed) and earn the full
# early-capture score. On the 1s index this is roughly the first ~10 seconds
# of a fresh directional move.
EARLY_TREND_MAX_AGE = 10

# Window (in ticks) for the tick-derived micro-bias ("1m" pseudo-timeframe).
# On a 1-second index, 45 ticks ~= 45 seconds of the most recent flow.
MICRO_BIAS_WINDOW = 45

# ---------------------------------------------------------------------------
# Regime filters (v3): distinguish a genuine push from noise
# ---------------------------------------------------------------------------
# Rolling stdev of tick-to-tick deltas, computed over the last VOLATILITY_WINDOW
# ticks.
# v4: DISABLED by default. The previous hand-picked band (0.002 / 0.04) was
# rejecting valid trends on Volatility 10 (1s), whose typical tick-to-tick
# stdev sits near or above the old MAX. Calibrate from your own tick history
# before re-enabling (set numbers instead of None).
VOLATILITY_WINDOW = 20
MIN_TICK_VOLATILITY = None     # e.g. 0.002 to reject a near-flat market
MAX_TICK_VOLATILITY = None     # e.g. 0.25 to reject an erratic/spiky market

# Require the most recent window's velocity (net move / tick) to be at least
# this fraction of the prior equal-length window's velocity, same direction.
# 0 disables the check entirely (a decaying move can still trigger, as today).
# A value like 0.8 requires the move to be holding pace or accelerating.
# v4: DISABLED (0). Requiring acceleration rejected clean constant-velocity
# trends - exactly the markets that work best for this trade type. Momentum
# health is now scored via tick consistency inside the strategy instead.
ACCELERATION_MIN_RATIO = 0     # 0 disables; e.g. 0.8 requires holding pace

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
        "velocity_threshold": 0.68,
        "burst_threshold": 0.82,
        "mtf_min_agreement": 3,
        "entry_score_threshold": 9,   # High bar: strong multi-axis setups only
    },
    "Balanced": {
        "velocity_threshold": VELOCITY_THRESHOLD,
        "burst_threshold": BURST_VELOCITY_THRESHOLD,
        "mtf_min_agreement": MTF_MIN_AGREEMENT,
        "entry_score_threshold": ENTRY_SCORE_THRESHOLD,  # Default: 7/14
    },
    "Aggressive": {
        "velocity_threshold": 0.50,
        "burst_threshold": 0.68,
        "mtf_min_agreement": 2,
        "entry_score_threshold": 5,  # Lower bar: more signals, less filtering
    },
}
DEFAULT_STRATEGY_SENSITIVITY = "Balanced"

# ---------------------------------------------------------------------------
# Stake / Risk Management Settings
# ---------------------------------------------------------------------------
# Martingale recovers losing streaks by scaling stake after each loss.
# The multiplier controls how aggressively the stake grows per step.
# With MARTINGALE_MULTIPLIER=2.5 over 3 steps: 1 + 2.5 + 6.25 = 9.75x base.
# The bot always resets to the initial stake after a win.
STAKE_MODE = "MARTINGALE"       # "FLAT" or "MARTINGALE"
MARTINGALE_MULTIPLIER = 2.5     # Stake multiplier per loss step
DEFAULT_INITIAL_STAKE = 1.0     # Default initial stake in USD
DEFAULT_MAX_MARTINGALE_STEPS = 3  # Max consecutive recovery steps before reset

# No session-level circuit breaker — the strategy and martingale manage risk.

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "deriv_bot.log")
LOG_LEVEL = "INFO"
