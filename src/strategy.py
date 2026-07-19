"""
src/strategy.py
---------------
Core trading strategy implementation for the Deriv Volatility 10 (1s) Bot.

Implements:
  1. Trend identification: Detects a strong 10-15 tick unidirectional push
     using a velocity/direction filter.
  2. 1-3-1 pattern detection: Identifies the precise entry signal within
     a confirmed trend.
  3. Multi-timeframe (MTF) confirmation: Validates the signal direction
     against 5m, 15m, and 30m candle trends.
"""

from collections import deque
from typing import Optional, List, Dict, Any

from src.logger import get_logger
from config import (
    TREND_WINDOW_MIN,
    TREND_WINDOW_MAX,
    VELOCITY_THRESHOLD,
    MAX_TRADES_PER_TREND,
    MTF_GRANULARITIES,
    MTF_CANDLE_COUNT,
)

logger = get_logger("strategy")


# ---------------------------------------------------------------------------
# Pattern State Machine
# ---------------------------------------------------------------------------
# The 1-3-1 pattern is detected as a state machine with the following stages:
#
#  IDLE           -> No active pattern being tracked.
#  INITIAL_RETRACE-> First retracement tick observed (1 tick against trend).
#  MOMENTUM_1     -> First momentum tick (in trend direction).
#  MOMENTUM_2     -> Second consecutive momentum tick.
#  MOMENTUM_3     -> Third consecutive momentum tick (3-tick confirmation).
#  FINAL_RETRACE  -> Final retracement tick (1 tick against trend).
#  SIGNAL         -> Entry signal generated; awaiting trade execution.
# ---------------------------------------------------------------------------

PATTERN_STAGES = [
    "IDLE",
    "INITIAL_RETRACE",
    "MOMENTUM_1",
    "MOMENTUM_2",
    "MOMENTUM_3",
    "FINAL_RETRACE",
    "SIGNAL",
]


class StrategyEngine:
    """
    Stateful strategy engine that processes incoming ticks and generates
    trade signals based on the 1-3-1 pattern within confirmed trends.
    """

    def __init__(self):
        # Tick buffer for trend analysis (stores raw prices)
        self._tick_buffer: deque = deque(maxlen=TREND_WINDOW_MAX + 5)

        # Current trend state
        self._trend_direction: Optional[str] = None   # "UP" or "DOWN"
        self._trend_start_index: int = 0
        self._trend_tick_count: int = 0
        self._in_cooldown: bool = False
        self._trades_in_trend: int = 0

        # Pattern state machine
        self._pattern_stage: str = "IDLE"
        self._pattern_ticks: List[float] = []

        # MTF bias cache
        self._mtf_bias: Optional[str] = None

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def process_tick(self, price: float) -> Optional[str]:
        """
        Process a new tick price and return a trade signal if detected.

        Args:
            price: The latest tick price from the Deriv API.

        Returns:
            "BUY", "SELL", or None.
        """
        self._tick_buffer.append(price)

        # Need at least TREND_WINDOW_MIN ticks before analysis
        if len(self._tick_buffer) < TREND_WINDOW_MIN:
            return None

        # Step 1: Identify or validate the current trend
        self._update_trend()

        # Step 2: If no trend or in cooldown, skip pattern detection
        if self._trend_direction is None or self._in_cooldown:
            return None

        # Step 3: Check trade limit for this trend
        if self._trades_in_trend >= MAX_TRADES_PER_TREND:
            logger.debug("Max trades per trend reached. Entering cooldown.")
            self._enter_cooldown()
            return None

        # Step 4: Run the 1-3-1 pattern state machine
        signal = self._update_pattern(price)

        return signal

    def on_trade_executed(self):
        """
        Called by the trading engine after a trade has been placed.
        Increments the trade counter for the current trend.
        """
        self._trades_in_trend += 1
        self._pattern_stage = "IDLE"
        self._pattern_ticks = []
        logger.info(f"Trade executed. Trades in current trend: {self._trades_in_trend}")

    def update_mtf_bias(self, bias: Optional[str]):
        """
        Update the multi-timeframe directional bias.
        Called by the trading engine after MTF analysis.

        Args:
            bias: "UP", "DOWN", or None (no clear consensus).
        """
        self._mtf_bias = bias
        logger.info(f"MTF bias updated to: {bias}")

    def get_state(self) -> Dict[str, Any]:
        """Return a snapshot of the current strategy state for the UI."""
        return {
            "trend_direction": self._trend_direction,
            "trend_tick_count": self._trend_tick_count,
            "trades_in_trend": self._trades_in_trend,
            "in_cooldown": self._in_cooldown,
            "pattern_stage": self._pattern_stage,
            "mtf_bias": self._mtf_bias,
        }

    def reset(self):
        """Full reset of strategy state."""
        self._tick_buffer.clear()
        self._trend_direction = None
        self._trend_tick_count = 0
        self._in_cooldown = False
        self._trades_in_trend = 0
        self._pattern_stage = "IDLE"
        self._pattern_ticks = []
        self._mtf_bias = None

    # ------------------------------------------------------------------
    # Trend Identification
    # ------------------------------------------------------------------

    def _update_trend(self):
        """
        Analyze the recent TREND_WINDOW_MIN to TREND_WINDOW_MAX ticks to
        determine if a strong unidirectional trend is present.

        A trend is confirmed when at least VELOCITY_THRESHOLD fraction of
        the last N ticks move in a single direction.
        """
        ticks = list(self._tick_buffer)

        # Try windows from TREND_WINDOW_MAX down to TREND_WINDOW_MIN
        for window in range(TREND_WINDOW_MAX, TREND_WINDOW_MIN - 1, -1):
            if len(ticks) < window:
                continue
            window_ticks = ticks[-window:]
            up_moves = sum(1 for i in range(1, len(window_ticks)) if window_ticks[i] > window_ticks[i - 1])
            down_moves = sum(1 for i in range(1, len(window_ticks)) if window_ticks[i] < window_ticks[i - 1])
            total_moves = len(window_ticks) - 1

            if total_moves == 0:
                continue

            up_ratio = up_moves / total_moves
            down_ratio = down_moves / total_moves

            if up_ratio >= VELOCITY_THRESHOLD:
                if self._trend_direction != "UP":
                    logger.info(f"New UP trend detected. Window={window}, Velocity={up_ratio:.2f}")
                    self._new_trend("UP")
                self._trend_tick_count = window
                return

            if down_ratio >= VELOCITY_THRESHOLD:
                if self._trend_direction != "DOWN":
                    logger.info(f"New DOWN trend detected. Window={window}, Velocity={down_ratio:.2f}")
                    self._new_trend("DOWN")
                self._trend_tick_count = window
                return

        # No strong trend found
        if self._trend_direction is not None:
            logger.debug("Trend lost. Resetting trend state.")
        self._trend_direction = None
        self._trend_tick_count = 0

    def _new_trend(self, direction: str):
        """Initialize state for a newly identified trend."""
        self._trend_direction = direction
        self._trades_in_trend = 0
        self._in_cooldown = False
        self._pattern_stage = "IDLE"
        self._pattern_ticks = []

    def _enter_cooldown(self):
        """Enter cooldown state, waiting for a new trend."""
        self._in_cooldown = True
        self._trend_direction = None
        self._pattern_stage = "IDLE"
        self._pattern_ticks = []
        logger.info("Cooldown activated. Waiting for new trend setup.")

    # ------------------------------------------------------------------
    # 1-3-1 Pattern State Machine
    # ------------------------------------------------------------------

    def _update_pattern(self, price: float) -> Optional[str]:
        """
        Advance the 1-3-1 pattern state machine with the latest tick price.

        For a BUY signal (in an UP trend):
          Stage 1 (INITIAL_RETRACE):  Price moves DOWN by 1 tick.
          Stage 2-4 (MOMENTUM_1-3):  Price moves UP for 3 consecutive ticks.
          Stage 5 (FINAL_RETRACE):   Price moves DOWN by 1 tick.
          -> BUY signal generated.

        For a SELL signal (in a DOWN trend):
          Stage 1 (INITIAL_RETRACE):  Price moves UP by 1 tick.
          Stage 2-4 (MOMENTUM_1-3):  Price moves DOWN for 3 consecutive ticks.
          Stage 5 (FINAL_RETRACE):   Price moves UP by 1 tick.
          -> SELL signal generated.

        Any deviation from the expected sequence resets the pattern to IDLE.
        """
        if len(self._pattern_ticks) == 0:
            self._pattern_ticks.append(price)
            return None

        prev_price = self._pattern_ticks[-1]
        direction = self._trend_direction

        # Determine tick direction
        if price > prev_price:
            tick_dir = "UP"
        elif price < prev_price:
            tick_dir = "DOWN"
        else:
            # Flat tick: does not advance or reset the pattern
            return None

        self._pattern_ticks.append(price)

        # Define what constitutes a "retracement" and "momentum" tick
        retrace_dir = "DOWN" if direction == "UP" else "UP"
        momentum_dir = direction  # "UP" for buy trend, "DOWN" for sell trend

        # --- State machine transitions ---

        if self._pattern_stage == "IDLE":
            if tick_dir == retrace_dir:
                self._pattern_stage = "INITIAL_RETRACE"
                logger.debug(f"Pattern: IDLE -> INITIAL_RETRACE (price={price})")
            # else: stay IDLE

        elif self._pattern_stage == "INITIAL_RETRACE":
            if tick_dir == momentum_dir:
                self._pattern_stage = "MOMENTUM_1"
                logger.debug(f"Pattern: INITIAL_RETRACE -> MOMENTUM_1 (price={price})")
            else:
                # Another retrace tick; restart from INITIAL_RETRACE
                self._pattern_stage = "INITIAL_RETRACE"
                logger.debug(f"Pattern: Reset to INITIAL_RETRACE (price={price})")

        elif self._pattern_stage == "MOMENTUM_1":
            if tick_dir == momentum_dir:
                self._pattern_stage = "MOMENTUM_2"
                logger.debug(f"Pattern: MOMENTUM_1 -> MOMENTUM_2 (price={price})")
            elif tick_dir == retrace_dir:
                # Retrace interrupted momentum; restart from INITIAL_RETRACE
                self._pattern_stage = "INITIAL_RETRACE"
                logger.debug(f"Pattern: Reset to INITIAL_RETRACE from MOMENTUM_1 (price={price})")

        elif self._pattern_stage == "MOMENTUM_2":
            if tick_dir == momentum_dir:
                self._pattern_stage = "MOMENTUM_3"
                logger.debug(f"Pattern: MOMENTUM_2 -> MOMENTUM_3 (price={price})")
            elif tick_dir == retrace_dir:
                self._pattern_stage = "INITIAL_RETRACE"
                logger.debug(f"Pattern: Reset to INITIAL_RETRACE from MOMENTUM_2 (price={price})")

        elif self._pattern_stage == "MOMENTUM_3":
            if tick_dir == retrace_dir:
                self._pattern_stage = "FINAL_RETRACE"
                logger.debug(f"Pattern: MOMENTUM_3 -> FINAL_RETRACE (price={price})")
            elif tick_dir == momentum_dir:
                # Extra momentum tick; stay at MOMENTUM_3 (still valid for final retrace)
                logger.debug(f"Pattern: Extra momentum tick at MOMENTUM_3 (price={price})")

        elif self._pattern_stage == "FINAL_RETRACE":
            # The final retrace was the previous tick; this tick is the entry point
            # We already recorded the final retrace in MOMENTUM_3 -> FINAL_RETRACE
            # The signal fires immediately AFTER the final retrace tick
            pass

        # Check if FINAL_RETRACE was just reached (signal fires on the tick AFTER)
        if self._pattern_stage == "FINAL_RETRACE":
            # Validate MTF bias before generating signal
            signal = "BUY" if direction == "UP" else "SELL"
            if self._validate_mtf(signal):
                logger.info(
                    f"*** {signal} SIGNAL GENERATED *** | "
                    f"Trend={direction} | MTF Bias={self._mtf_bias} | Price={price}"
                )
                self._pattern_stage = "IDLE"
                self._pattern_ticks = []
                return signal
            else:
                logger.info(
                    f"Signal {signal} rejected: MTF bias ({self._mtf_bias}) does not confirm."
                )
                self._pattern_stage = "IDLE"
                self._pattern_ticks = []
                return None

        return None

    # ------------------------------------------------------------------
    # Multi-Timeframe Validation
    # ------------------------------------------------------------------

    def _validate_mtf(self, signal: str) -> bool:
        """
        Validate the trade signal against the current MTF bias.

        A BUY signal requires MTF bias of "UP".
        A SELL signal requires MTF bias of "DOWN".
        If MTF bias is None (not yet determined), the trade is blocked.
        """
        if self._mtf_bias is None:
            logger.warning("MTF bias not available. Trade blocked pending MTF analysis.")
            return False

        if signal == "BUY" and self._mtf_bias == "UP":
            return True
        if signal == "SELL" and self._mtf_bias == "DOWN":
            return True

        return False


# ---------------------------------------------------------------------------
# Multi-Timeframe Analyzer (standalone utility)
# ---------------------------------------------------------------------------

class MTFAnalyzer:
    """
    Analyzes candle data from multiple timeframes to determine the
    prevailing directional bias.

    A timeframe is considered "bullish" if the last N candles show a
    net upward close-to-close movement, and "bearish" if net downward.
    Consensus across all three timeframes is required.
    """

    def analyze(self, candles_by_tf: Dict[str, List[Dict]]) -> Optional[str]:
        """
        Determine the MTF directional bias.

        Args:
            candles_by_tf: Dict mapping timeframe label (e.g., '5m') to
                           a list of candle dicts with 'open', 'high', 'low', 'close'.

        Returns:
            "UP" if all timeframes are bullish,
            "DOWN" if all timeframes are bearish,
            None if there is no consensus.
        """
        biases = []
        for tf_label, candles in candles_by_tf.items():
            if not candles or len(candles) < 3:
                logger.warning(f"Insufficient candles for {tf_label} MTF analysis.")
                return None

            bias = self._analyze_single_tf(candles, tf_label)
            if bias is None:
                return None
            biases.append(bias)

        if not biases:
            return None

        if all(b == "UP" for b in biases):
            logger.info(f"MTF consensus: UP (all {len(biases)} timeframes bullish)")
            return "UP"
        if all(b == "DOWN" for b in biases):
            logger.info(f"MTF consensus: DOWN (all {len(biases)} timeframes bearish)")
            return "DOWN"

        logger.info(f"MTF no consensus: biases={biases}")
        return None

    def _analyze_single_tf(self, candles: List[Dict], label: str) -> Optional[str]:
        """
        Determine bias for a single timeframe using EMA slope and recent close direction.

        Uses a simple approach:
        - Compare the close of the most recent candle to the close N candles ago.
        - If recent close > older close: bullish.
        - If recent close < older close: bearish.
        """
        try:
            closes = [float(c["close"]) for c in candles]
            if len(closes) < 3:
                return None

            # Compare last close to close 3 candles ago for trend direction
            recent_close = closes[-1]
            reference_close = closes[-3]

            if recent_close > reference_close:
                logger.debug(f"MTF {label}: BULLISH (close {recent_close:.4f} > ref {reference_close:.4f})")
                return "UP"
            elif recent_close < reference_close:
                logger.debug(f"MTF {label}: BEARISH (close {recent_close:.4f} < ref {reference_close:.4f})")
                return "DOWN"
            else:
                logger.debug(f"MTF {label}: NEUTRAL")
                return None
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error analyzing {label} candles: {e}")
            return None
