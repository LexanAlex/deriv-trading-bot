"""Quality-first, pullback-continuation strategy for synthetic-index tick data.

The previous 1-3-1-final-retrace state machine rejected most otherwise valid
momentum moves. This engine uses a less brittle entry: a strong 8-12 tick
trend, one pullback, then two continuation ticks. It deliberately still
permits only one trade per trend and requires 3-of-3 MTF alignment.
"""

from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from config import (
    MOMENTUM_CONFIRM_TICKS,
    MTF_CANDLE_COUNT,
    TREND_WINDOW_MAX,
    TREND_WINDOW_MIN,
    VELOCITY_THRESHOLD,
    MAX_TRADES_PER_TREND,
)
from src.logger import get_logger

logger = get_logger("strategy")

PATTERN_STAGES = ["IDLE", "PULLBACK", "MOMENTUM", "SIGNAL"]


class StrategyEngine:
    """Generate quality-first signals from a confirmed trend and pullback."""

    def __init__(self):
        self._tick_buffer: deque = deque(maxlen=TREND_WINDOW_MAX + 2)
        self._trend_direction: Optional[str] = None
        self._trend_tick_count = 0
        self._trades_in_trend = 0
        self._in_cooldown = False
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._previous_price: Optional[float] = None
        self._mtf_bias: Optional[str] = None
        self._mtf_agreement = 0

    def process_tick(self, price: float) -> Optional[str]:
        self._tick_buffer.append(price)
        if len(self._tick_buffer) < TREND_WINDOW_MIN:
            self._previous_price = price
            return None

        self._update_trend()
        if self._trend_direction is None or self._in_cooldown:
            self._previous_price = price
            return None
        if self._trades_in_trend >= MAX_TRADES_PER_TREND:
            self._enter_cooldown()
            self._previous_price = price
            return None

        signal = self._update_pattern(price)
        self._previous_price = price
        return signal

    def on_trade_executed(self) -> None:
        self._trades_in_trend += 1
        self._pattern_stage = "IDLE"
        logger.info("Trade executed. Trades in current trend: %s", self._trades_in_trend)

    def update_mtf_bias(self, bias: Optional[str], agreement: int = 0) -> None:
        self._mtf_bias = bias
        self._mtf_agreement = agreement

    def get_state(self) -> Dict[str, Any]:
        return {
            "trend_direction": self._trend_direction,
            "trend_tick_count": self._trend_tick_count,
            "trades_in_trend": self._trades_in_trend,
            "in_cooldown": self._in_cooldown,
            "pattern_stage": self._pattern_stage,
            "mtf_bias": self._mtf_bias,
            "mtf_agreement": self._mtf_agreement,
        }

    def reset(self) -> None:
        self._tick_buffer.clear()
        self._trend_direction = None
        self._trend_tick_count = 0
        self._trades_in_trend = 0
        self._in_cooldown = False
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._previous_price = None
        self._mtf_bias = None
        self._mtf_agreement = 0

    def _update_trend(self) -> None:
        ticks = list(self._tick_buffer)
        detected_direction = None
        detected_window = 0
        for window in range(min(TREND_WINDOW_MAX, len(ticks)), TREND_WINDOW_MIN - 1, -1):
            sample = ticks[-window:]
            changes = [sample[index] - sample[index - 1] for index in range(1, len(sample))]
            non_flat = [change for change in changes if change != 0]
            if not non_flat:
                continue
            up_ratio = sum(change > 0 for change in non_flat) / len(non_flat)
            down_ratio = sum(change < 0 for change in non_flat) / len(non_flat)
            if up_ratio >= VELOCITY_THRESHOLD:
                detected_direction, detected_window = "UP", window
                break
            if down_ratio >= VELOCITY_THRESHOLD:
                detected_direction, detected_window = "DOWN", window
                break

        if detected_direction is None:
            self._trend_direction = None
            self._trend_tick_count = 0
            self._pattern_stage = "IDLE"
            self._continuation_ticks = 0
            return
        if detected_direction != self._trend_direction:
            self._trend_direction = detected_direction
            self._trades_in_trend = 0
            self._in_cooldown = False
            self._pattern_stage = "IDLE"
            self._continuation_ticks = 0
            logger.info("New %s trend: %s-tick window", detected_direction, detected_window)
        self._trend_tick_count = detected_window

    def _enter_cooldown(self) -> None:
        self._in_cooldown = True
        self._trend_direction = None
        self._pattern_stage = "IDLE"

    def _update_pattern(self, price: float) -> Optional[str]:
        if self._previous_price is None or price == self._previous_price:
            return None
        tick_direction = "UP" if price > self._previous_price else "DOWN"
        trend = self._trend_direction
        assert trend in ("UP", "DOWN")
        pullback_direction = "DOWN" if trend == "UP" else "UP"

        if self._pattern_stage == "IDLE":
            if tick_direction == pullback_direction:
                self._pattern_stage = "PULLBACK"
                self._continuation_ticks = 0
            return None

        if self._pattern_stage == "PULLBACK":
            if tick_direction == pullback_direction:
                return None  # A deeper pullback still counts as one setup.
            self._continuation_ticks = 1
            return self._signal_if_confirmed(trend)

        if self._pattern_stage == "MOMENTUM":
            if tick_direction == trend:
                self._continuation_ticks += 1
                return self._signal_if_confirmed(trend)
            self._pattern_stage = "PULLBACK" if tick_direction == pullback_direction else "IDLE"
            self._continuation_ticks = 0
        return None

    def _signal_if_confirmed(self, trend: str) -> Optional[str]:
        if self._continuation_ticks < MOMENTUM_CONFIRM_TICKS:
            self._pattern_stage = "MOMENTUM"
            return None
        signal = "BUY" if trend == "UP" else "SELL"
        if self._validate_mtf(signal):
            self._pattern_stage = "SIGNAL"
            logger.info(
                "%s signal: one pullback + %s continuation tick(s), 3/3 MTF alignment",
                signal, self._continuation_ticks,
            )
            return signal
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        return None

    def _validate_mtf(self, signal: str) -> bool:
        if self._mtf_agreement != 3:
            return False
        return (signal == "BUY" and self._mtf_bias == "UP") or (signal == "SELL" and self._mtf_bias == "DOWN")


class MTFAnalyzer:
    """Require all three configured timeframes to point in the same direction."""

    def analyze(self, candles_by_tf: Dict[str, List[Dict]]) -> Optional[str]:
        """Return the fully-aligned direction for compatibility with existing callers."""
        return self.analyze_with_strength(candles_by_tf)[0]

    def analyze_with_strength(self, candles_by_tf: Dict[str, List[Dict]]) -> Tuple[Optional[str], int]:
        """Return direction plus the number of supporting timeframes (0–3)."""
        votes: List[str] = []
        for label, candles in candles_by_tf.items():
            bias = self._analyze_single_tf(candles, label)
            if bias:
                votes.append(bias)
        if len(votes) == 3 and votes.count("UP") == 3:
            return "UP", 3
        if len(votes) == 3 and votes.count("DOWN") == 3:
            return "DOWN", 3
        return None, 0

    @staticmethod
    def _analyze_single_tf(candles: List[Dict], label: str) -> Optional[str]:
        if len(candles) < 3:
            logger.warning("Insufficient candles for %s MTF analysis.", label)
            return None
        try:
            closes = [float(candle["close"]) for candle in candles]
        except (KeyError, TypeError, ValueError):
            return None
        if closes[-1] > closes[-3]:
            return "UP"
        if closes[-1] < closes[-3]:
            return "DOWN"
        return None
