"""Quality-first, pullback-continuation strategy for synthetic-index tick data.

v2 upgrade
----------
The original engine classified a trend by *counting* how many individual
ticks moved up vs. down inside an 8-12 tick window and required 70% of them
to agree. That measure ignores tick *magnitude*: a real, tradeable push that
happens to include a couple of larger counter-ticks can easily drop below a
70%-of-ticks bar even though the net move is strongly directional. That is
the main reason the bot was sitting out promising moves.

This version replaces tick-counting with Kaufman's Efficiency Ratio (ER), a
standard technique from adaptive moving-average research (used in KAMA):

    ER = |price[end] - price[start]| / sum(|price[i] - price[i-1]|)

ER is 1.0 for a perfectly straight move and falls toward 0 as more of the
path is "wasted" on back-and-forth noise. It rewards net progress rather
than tick-by-tick unanimity, so a trend with occasional pullbacks still
scores well as long as it is *net* efficient - which is exactly the kind of
move you highlighted as missed.

On top of that:
  - A short "burst" window checks for strong, fast momentum (e.g. 4-6 ticks)
    in parallel with the classic 8-12 tick window, so quick, aggressive
    pushes don't have to wait out a slower classic confirmation.
  - MTF confirmation now accepts a configurable majority (default 2-of-3)
    instead of requiring all three timeframes to agree, which was rejecting
    a lot of otherwise-valid setups.
  - The pullback -> two-continuation-tick entry sequence itself is
    unchanged, since that's the "one tick reversal, two ticks, then trade"
    behaviour you described wanting.

All of the new thresholds are constructor parameters with config.py
defaults, so the dashboard can offer a sensitivity control without code
changes.
"""

from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from config import (
    BURST_VELOCITY_THRESHOLD,
    BURST_WINDOW_MAX,
    BURST_WINDOW_MIN,
    MAX_TRADES_PER_TREND,
    MOMENTUM_CONFIRM_TICKS,
    MTF_CANDLE_COUNT,
    MTF_MIN_AGREEMENT,
    TREND_WINDOW_MAX,
    TREND_WINDOW_MIN,
    VELOCITY_THRESHOLD,
)
from src.logger import get_logger

logger = get_logger("strategy")

PATTERN_STAGES = ["IDLE", "PULLBACK", "MOMENTUM", "SIGNAL"]


def _efficiency_ratio(sample: List[float]) -> float:
    """Kaufman's Efficiency Ratio: net displacement / total path length.

    1.0 = a perfectly straight move; values fall toward 0.0 as more of the
    path is spent on back-and-forth noise instead of net progress.
    """
    if len(sample) < 2:
        return 0.0
    net = abs(sample[-1] - sample[0])
    path = sum(abs(sample[i] - sample[i - 1]) for i in range(1, len(sample)))
    if path == 0:
        return 0.0
    return net / path


class StrategyEngine:
    """Generate quality-first signals from a confirmed trend and pullback."""

    def __init__(
        self,
        velocity_threshold: float = VELOCITY_THRESHOLD,
        burst_threshold: float = BURST_VELOCITY_THRESHOLD,
        mtf_min_agreement: int = MTF_MIN_AGREEMENT,
        trend_window_min: int = TREND_WINDOW_MIN,
        trend_window_max: int = TREND_WINDOW_MAX,
        burst_window_min: int = BURST_WINDOW_MIN,
        burst_window_max: int = BURST_WINDOW_MAX,
        momentum_confirm_ticks: int = MOMENTUM_CONFIRM_TICKS,
    ):
        # Tunable thresholds (a dashboard "sensitivity" control can override
        # these per-session without touching this file).
        self._velocity_threshold = velocity_threshold
        self._burst_threshold = burst_threshold
        self._mtf_min_agreement = mtf_min_agreement
        self._trend_window_min = trend_window_min
        self._trend_window_max = trend_window_max
        self._burst_window_min = burst_window_min
        self._burst_window_max = burst_window_max
        self._momentum_confirm_ticks = momentum_confirm_ticks

        buffer_size = max(trend_window_max, burst_window_max) + 2
        self._tick_buffer: deque = deque(maxlen=buffer_size)
        self._trend_direction: Optional[str] = None
        self._trend_tick_count = 0
        self._trend_kind: Optional[str] = None  # "classic" or "burst"
        self._trades_in_trend = 0
        self._in_cooldown = False
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._previous_price: Optional[float] = None
        self._mtf_bias: Optional[str] = None
        self._mtf_agreement = 0

    def process_tick(self, price: float) -> Optional[str]:
        self._tick_buffer.append(price)
        if len(self._tick_buffer) < self._burst_window_min:
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
            "trend_kind": self._trend_kind,
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
        self._trend_kind = None
        self._trades_in_trend = 0
        self._in_cooldown = False
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._previous_price = None
        self._mtf_bias = None
        self._mtf_agreement = 0

    # ------------------------------------------------------------------
    # Trend detection
    # ------------------------------------------------------------------
    def _update_trend(self) -> None:
        ticks = list(self._tick_buffer)

        # 1) Fast path: a short, high-conviction burst. Checked first so
        #    strong, quick momentum doesn't have to wait for the slower
        #    classic window to fill.
        detected = self._scan_windows(
            ticks, self._burst_window_min, self._burst_window_max, self._burst_threshold
        )
        kind = "burst"

        # 2) Classic path: the original, slower 8-12 tick confirmation,
        #    with the counting-ratio replaced by the efficiency ratio.
        if detected is None:
            detected = self._scan_windows(
                ticks, self._trend_window_min, self._trend_window_max, self._velocity_threshold
            )
            kind = "classic"

        if detected is None:
            self._trend_direction = None
            self._trend_tick_count = 0
            self._trend_kind = None
            self._pattern_stage = "IDLE"
            self._continuation_ticks = 0
            return

        direction, window = detected
        if direction != self._trend_direction:
            self._trend_direction = direction
            self._trades_in_trend = 0
            self._in_cooldown = False
            self._pattern_stage = "IDLE"
            self._continuation_ticks = 0
            logger.info("New %s trend (%s path): %s-tick window", direction, kind, window)
        self._trend_tick_count = window
        self._trend_kind = kind

    @staticmethod
    def _scan_windows(
        ticks: List[float], window_min: int, window_max: int, threshold: float
    ) -> Optional[Tuple[str, int]]:
        """Scan window sizes from largest to smallest for the first one whose
        efficiency ratio clears `threshold`. Returns (direction, window) or
        None."""
        if len(ticks) < window_min:
            return None
        for window in range(min(window_max, len(ticks)), window_min - 1, -1):
            sample = ticks[-window:]
            er = _efficiency_ratio(sample)
            if er >= threshold:
                direction = "UP" if sample[-1] > sample[0] else "DOWN"
                return direction, window
        return None

    def _enter_cooldown(self) -> None:
        self._in_cooldown = True
        self._trend_direction = None
        self._trend_kind = None
        self._pattern_stage = "IDLE"

    # ------------------------------------------------------------------
    # Pullback -> continuation entry pattern (unchanged behaviour)
    # ------------------------------------------------------------------
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
        if self._continuation_ticks < self._momentum_confirm_ticks:
            self._pattern_stage = "MOMENTUM"
            return None
        signal = "BUY" if trend == "UP" else "SELL"
        if self._validate_mtf(signal):
            self._pattern_stage = "SIGNAL"
            logger.info(
                "%s signal: one pullback + %s continuation tick(s), MTF %s/3 agreement",
                signal, self._continuation_ticks, self._mtf_agreement,
            )
            return signal
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        return None

    def _validate_mtf(self, signal: str) -> bool:
        if self._mtf_agreement < self._mtf_min_agreement:
            return False
        return (signal == "BUY" and self._mtf_bias == "UP") or (signal == "SELL" and self._mtf_bias == "DOWN")


class MTFAnalyzer:
    """Multi-timeframe bias with a configurable majority requirement.

    Previously required all three timeframes to agree (3/3), which is a very
    strict bar in practice. Default is now 2-of-3, still configurable.
    """

    def __init__(self, min_agreement: int = MTF_MIN_AGREEMENT):
        self._min_agreement = min_agreement

    def analyze(self, candles_by_tf: Dict[str, List[Dict]]) -> Optional[str]:
        """Return the majority-aligned direction for compatibility with existing callers."""
        return self.analyze_with_strength(candles_by_tf)[0]

    def analyze_with_strength(self, candles_by_tf: Dict[str, List[Dict]]) -> Tuple[Optional[str], int]:
        """Return direction plus the number of supporting timeframes (0-3)."""
        votes: List[str] = []
        for label, candles in candles_by_tf.items():
            bias = self._analyze_single_tf(candles, label)
            if bias:
                votes.append(bias)
        if not votes:
            return None, 0
        up_votes = votes.count("UP")
        down_votes = votes.count("DOWN")
        if up_votes >= self._min_agreement and up_votes >= down_votes:
            return "UP", up_votes
        if down_votes >= self._min_agreement and down_votes > up_votes:
            return "DOWN", down_votes
        return None, max(up_votes, down_votes)

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
