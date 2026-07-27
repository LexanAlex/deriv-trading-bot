"""
config.py
Central configuration for the Deriv Volatility 10 (1s) Trading Bot.
"""

import os

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False

load_dotenv()


def _streamlit_secret(name: str) -> str:
    try:
        import streamlit as st
        return str(st.secrets.get(name, ""))
    except Exception:
        return ""


# Deriv API credentials.
DERIV_APP_ID = os.getenv("DERIV_APP_ID") or _streamlit_secret("DERIV_APP_ID") or ""
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN") or _streamlit_secret("DERIV_API_TOKEN") or ""

# Compatibility only. The PAT client obtains an account-specific OTP WebSocket URL at runtime.
DERIV_WS_URL = ""


# Market / symbol.
SYMBOL = "1HZ10V"
SYMBOL_DISPLAY = "Volatility 10 (1s) Index"


# Strategy parameters.
TREND_WINDOW_MIN = 8
TREND_WINDOW_MAX = 12
VELOCITY_THRESHOLD = 0.55

BURST_WINDOW_MIN = 4
BURST_WINDOW_MAX = 6
BURST_VELOCITY_THRESHOLD = 0.72

MAX_TRADES_PER_TREND = 1
TICK_BUFFER_SIZE = 50
MOMENTUM_CONFIRM_TICKS = 1

ENTRY_SCORE_THRESHOLD = 7
EARLY_TREND_MAX_AGE = 10
MICRO_BIAS_WINDOW = 45


# Regime filters.
VOLATILITY_WINDOW = 20
MIN_TICK_VOLATILITY = None
MAX_TICK_VOLATILITY = None
ACCELERATION_MIN_RATIO = 0


# Contract parameters.
CONTRACT_TYPE_BUY = "ONETOUCH"
CONTRACT_TYPE_SELL = "ONETOUCH"
CONTRACT_DURATION = 5
CONTRACT_DURATION_UNIT = "t"

BARRIER_BUY = "+0.08"
BARRIER_SELL = "-0.08"

CURRENCY = "USD"


# Multi-timeframe confirmation.
MTF_GRANULARITIES = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
}

MTF_CANDLE_COUNT = 10
MTF_MIN_AGREEMENT = 2


# Strategy sensitivity presets.
STRATEGY_SENSITIVITY_PRESETS = {
    "Conservative": {
        "velocity_threshold": 0.68,
        "burst_threshold": 0.82,
        "mtf_min_agreement": 3,
        "entry_score_threshold": 9,
    },
    "Balanced": {
        "velocity_threshold": VELOCITY_THRESHOLD,
        "burst_threshold": BURST_VELOCITY_THRESHOLD,
        "mtf_min_agreement": MTF_MIN_AGREEMENT,
        "entry_score_threshold": ENTRY_SCORE_THRESHOLD,
    },
    "Aggressive": {
        "velocity_threshold": 0.50,
        "burst_threshold": 0.68,
        "mtf_min_agreement": 2,
        "entry_score_threshold": 5,
    },
}

DEFAULT_STRATEGY_SENSITIVITY = "Balanced"


# Stake / risk settings.
#
# MAX_MARTINGALE_STEPS semantics:
# This is the TOTAL number of stake levels, including the initial stake.
#
# Example with multiplier 2.5 and max steps 3:
#   step 0: 1.00
#   step 1: 2.50
#   step 2: 6.25
#   next loss resets to 1.00
#
# Total possible exposure before reset: 1 + 2.5 + 6.25 = 9.75x base.
STAKE_MODE = "MARTINGALE"
MARTINGALE_MULTIPLIER = 2.5
DEFAULT_INITIAL_STAKE = 1.0
DEFAULT_MAX_MARTINGALE_STEPS = 3


# Logging.
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "deriv_bot.log")
LOG_LEVEL = "INFO"