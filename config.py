"""
config.py
---------
Central configuration file for the Deriv Volatility 10 (1s) Trading Bot.
All constants, defaults, and environment-variable lookups are defined here.

--- FIX BUG-6 ---
DERIV_WS_URL was built with the retired frontend.binaryws.com host, which
now returns HTTP 403 for all WebSocket upgrade requests. Updated to the
current live endpoint: ws.derivws.com.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Deriv API Connection Settings
# ---------------------------------------------------------------------------
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")          # Default Deriv demo app_id

# FIX BUG-6: frontend.binaryws.com is retired (HTTP 403).
# The correct, live endpoint is wss://ws.derivws.com/websockets/v3
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?l=EN&app_id={DERIV_APP_ID}"

# ---------------------------------------------------------------------------
# Market / Symbol Settings
# ---------------------------------------------------------------------------
SYMBOL = "1HZ10V"          # Volatility 10 (1s) Index
SYMBOL_DISPLAY = "Volatility 10 (1s) Index"

# ---------------------------------------------------------------------------
# Strategy Parameters
# ---------------------------------------------------------------------------
TREND_WINDOW_MIN = 10          # Minimum ticks for trend identification
TREND_WINDOW_MAX = 15          # Maximum ticks for trend identification
VELOCITY_THRESHOLD = 0.70      # Minimum fraction of ticks in one direction to confirm trend
MAX_TRADES_PER_TREND = 1       # Maximum trades allowed per identified trend
TICK_BUFFER_SIZE = 50          # Number of recent ticks to keep in memory

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

# ---------------------------------------------------------------------------
# Martingale Settings
# ---------------------------------------------------------------------------
MARTINGALE_MULTIPLIER = 3.0     # Stake multiplier on loss
DEFAULT_INITIAL_STAKE = 1.0     # Default initial stake in USD
DEFAULT_MAX_MARTINGALE_STEPS = 3  # Default max consecutive Martingale steps

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "deriv_bot.log")
LOG_LEVEL = "INFO"
