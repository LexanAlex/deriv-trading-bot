"""
src/strategy.py
Stable v5.3 tick-momentum strategy.

This is the stable strategy core for MomentumMaster.

Public interface preserved for TradingEngine:
    process_tick()
    get_state()
    on_trade_executed()
    on_signal_skipped()
    update_mtf_bias()
    reset()
    state_version
"""

import math
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from config import (
    ACCELERATION_MIN_RATIO,
    BURST_VELOCITY_THRESHOLD,
    BURST_WINDOW_MAX,
    BURST_WINDOW_MIN,
    EARLY_TREND_MAX_AGE,
    MAX_TICK_VOLATILITY,
    MAX_TRADES_PER_TREND,
    MICRO_BIAS_WINDOW,
    MIN_TICK_VOLATILITY,
    MOMENTUM_CONFIRM_TICKS,
    MTF_MIN_AGREEMENT,
    TREND_WINDOW_MAX,
    TREND_WINDOW_MIN,
    VELOCITY_THRESHOLD,
    VOLATILITY_WINDOW,
)
from src.logger import get_logger

logger = get_logger("strategy")

PATTERN_STAGES = ["IDLE", "TREND", "PULLBACK", "MOMENTUM", "SIGNAL"]

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

_SCORE_ER_HIGH = 5
_SCORE_ER_GOOD = 4
_SCORE_ER_OK = 2

_SCORE_MTF_UNANIMOUS = 4
_SCORE_MTF_MAJORITY = 3
_SCORE_MTF_NEUTRAL = 1

_SCORE_CONSISTENCY_HIGH = 3
_SCORE_CONSISTENCY_GOOD = 2

_SCORE_EARLY_STRONG = 2
_SCORE_EARLY_MILD = 1

_PENALTY_MICRO_DISAGREE = 2

ENTRY_SCORE_THRESHOLD = 7
SCORE_MAX = 14

# ---------------------------------------------------------------------------
# Quality profiles
# ---------------------------------------------------------------------------

_PROFILES = {
    "aggressive": {
        "min_displacement_noise": 1.00,
        "max_whipsaw_ratio": 0.55,
        "immediate_min_er": 0.65,
        "immediate_min_consistency": 0.60,
        "pullback_min_er": 0.58,
        "pullback_min_consistency": 0.55,
        "min_pullback_retrace": 0.05,
        "max_pullback_retrace": 0.78,
        "continuation_noise_mult": 0.45,
        "counter_trend_tick_noise_mult": 1.50,
        "reject_cooldown_ticks": 8,
        "require_micro_agree_immediate": False,
        "require_micro_agree_pullback": False,
        "require_htf_not_against_majority_immediate": False,
        "require_htf_not_against_majority_pullback": False,
    },
    "balanced": {
        "min_displacement_noise": 1.35,
        "max_whipsaw_ratio": 0.45,
        "immediate_min_er": 0.72,
        "immediate_min_consistency": 0.68,
        "pullback_min_er": 0.64,
        "pullback_min_consistency": 0.60,
        "min_pullback_retrace": 0.08,
        "max_pullback_retrace": 0.70,
        "continuation_noise_mult": 0.60,
        "counter_trend_tick_noise_mult": 1.25,
        "reject_cooldown_ticks": 12,
        "require_micro_agree_immediate": True,
        "require_micro_agree_pullback": False,
        "require_htf_not_against_majority_immediate": True,
        "require_htf_not_against_majority_pullback": False,
    },
    "conservative": {
        "min_displacement_noise": 1.80,
        "max_whipsaw_ratio": 0.35,
        "immediate_min_er": 0.78,
        "immediate_min_consistency": 0.75,
        "pullback_min_er": 0.70,
        "pullback_min_consistency": 0.68,
        "min_pullback_retrace": 0.12,
        "max_pullback_retrace": 0.62,
        "continuation_noise_mult": 0.80,
        "counter_trend_tick_noise_mult": 1.00,
        "reject_cooldown_ticks": 18,
        "require_micro_agree_immediate": True,
        "require_micro_agree_pullback": True,
        "require_htf_not_against_majority_immediate": True,
        "require_htf_not_against_majority_pullback": True,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deltas(sample: List[float]) -> List[float]:
    return [sample[i] - sample[i - 1] for i in range(1, len(sample))]


def _noise_from_deltas(deltas: List[float]) -> Optional[float]:
    if not deltas:
        return None

    mean = sum(deltas) / len(deltas)
    variance = sum((d - mean) ** 2 for d in deltas) / len(deltas)
    return math.sqrt(variance)


def _tick_volatility(ticks: List[float], window: int) -> Optional[float]:
    if len(ticks) < window + 1:
        return None

    return _noise_from_deltas(_deltas(ticks[-(window + 1):]))


def _window_velocity(ticks: List[float], window: int) -> Optional[float]:
    if len(ticks) < window:
        return None

    sample = ticks[-window:]
    return (sample[-1] - sample[0]) / window


def _prior_window_velocity(ticks: List[float], window: int) -> Optional[float]:
    if len(ticks) < window * 2:
        return None

    sample = ticks[-(window * 2):-window]
    return (sample[-1] - sample[0]) / window


def _efficiency_ratio_from_deltas(net: float, deltas: List[float]) -> float:
    path = 0.0

    for d in deltas:
        path += d if d >= 0 else -d

    if path == 0.0:
        return 0.0

    return abs(net) / path


def _efficiency_ratio(sample: List[float]) -> float:
    if len(sample) < 2:
        return 0.0

    return _efficiency_ratio_from_deltas(
        sample[-1] - sample[0],
        _deltas(sample),
    )


def _consistency_from_deltas(deltas: List[float], direction: str) -> float:
    up = direction == "UP"
    with_trend = 0
    against = 0

    for d in deltas:
        if d > 0:
            if up:
                with_trend += 1
            else:
                against += 1
        elif d < 0:
            if up:
                against += 1
            else:
                with_trend += 1

    total = with_trend + against

    if total == 0:
        return 0.0

    return with_trend / total


def _tick_consistency(sample: List[float], direction: str) -> float:
    if len(sample) < 2:
        return 0.0

    return _consistency_from_deltas(_deltas(sample), direction)


def _whipsaw_ratio_from_deltas(deltas: List[float]) -> float:
    previous_sign = 0
    changes = 0
    moves = 0

    for d in deltas:
        if d > 0:
            sign = 1
        elif d < 0:
            sign = -1
        else:
            continue

        if previous_sign != 0 and sign != previous_sign:
            changes += 1

        moves += 1
        previous_sign = sign

    if moves <= 1:
        return 0.0

    return changes / (moves - 1)


def _micro_bias(ticks: List[float], window: int) -> Optional[str]:
    if len(ticks) < max(4, window // 4):
        return None

    sample = ticks[-window:] if len(ticks) >= window else list(ticks)
    net = sample[-1] - sample[0]

    if net == 0:
        return None

    er = _efficiency_ratio_from_deltas(net, _deltas(sample))

    if er < 0.25:
        return None

    return "UP" if net > 0 else "DOWN"


def _candle_slope_bias(closes: List[float]) -> Optional[str]:
    n = len(closes)

    if n < 3:
        return None

    x_mean = (n - 1) / 2.0
    y_mean = sum(closes) / n

    numerator = sum((i - x_mean) * (closes[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return None

    slope = numerator / denominator

    if y_mean == 0:
        return None

    relative_slope = abs(slope) / abs(y_mean)

    if relative_slope >= 0.0001:
        return "UP" if slope > 0 else "DOWN"

    head = sum(closes[:3]) / 3
    tail = sum(closes[-3:]) / 3

    if tail > head:
        return "UP"

    if tail < head:
        return "DOWN"

    return None


def _htf_vote_counts(
    tf_biases: Dict[str, str],
    direction: str,
) -> Tuple[int, int, int]:
    opposite = "DOWN" if direction == "UP" else "UP"

    with_votes = 0
    against_votes = 0
    total_votes = 0

    for key, bias in tf_biases.items():
        if key == "1m" or bias not in ("UP", "DOWN"):
            continue

        total_votes += 1

        if bias == direction:
            with_votes += 1
        elif bias == opposite:
            against_votes += 1

    return with_votes, against_votes, total_votes


# ---------------------------------------------------------------------------
# Strategy Engine
# ---------------------------------------------------------------------------

class StrategyEngine:
    """Stable v5.3 tick-momentum strategy."""

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
        entry_score_threshold: int = ENTRY_SCORE_THRESHOLD,
        early_trend_max_age: int = EARLY_TREND_MAX_AGE,
        micro_bias_window: int = MICRO_BIAS_WINDOW,
    ):
        self._velocity_threshold = velocity_threshold
        self._burst_threshold = burst_threshold
        self._mtf_min_agreement = mtf_min_agreement
        self._trend_window_min = trend_window_min
        self._trend_window_max = trend_window_max
        self._burst_window_min = burst_window_min
        self._burst_window_max = burst_window_max
        self._momentum_confirm_ticks = momentum_confirm_ticks
        self._entry_score_threshold = entry_score_threshold
        self._early_trend_max_age = early_trend_max_age
        self._micro_bias_window = micro_bias_window

        if self._entry_score_threshold <= 5:
            self._profile = "aggressive"
        elif self._entry_score_threshold <= 8:
            self._profile = "balanced"
        else:
            self._profile = "conservative"

        self._limits = _PROFILES[self._profile]

        buffer_size = max(trend_window_max, burst_window_max, micro_bias_window) * 2 + 4
        self._tick_buffer: deque = deque(maxlen=buffer_size)

        self._trend_direction: Optional[str] = None
        self._trend_tick_count = 0
        self._trend_kind: Optional[str] = None
        self._trend_age = 0
        self._trades_in_trend = 0
        self._in_cooldown = False
        self._cooldown_direction: Optional[str] = None
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._previous_price: Optional[float] = None

        self._mtf_bias: Optional[str] = None
        self._mtf_agreement = 0
        self._mtf_tf_biases: Dict[str, str] = {}
        self._micro: Optional[str] = None

        self._last_signal_score: int = 0
        self._last_signal_score_breakdown: Dict[str, int] = {}
        self._last_entry_mode: Optional[str] = None

        self._pullback_start_price: Optional[float] = None
        self._immediate_disabled_for_trend = False
        self._trend_start_price: Optional[float] = None
        self._trend_extreme_price: Optional[float] = None

        self._reject_cooldown_ticks: int = 0
        self._consecutive_rejects: int = 0
        self._last_reject_reason: Optional[str] = None

        self._state_version: int = 0
        self._state_signature: Optional[Tuple[Any, ...]] = None
        self._mtf_sig: Tuple[Any, ...] = ()
        self._breakdown_sig: Tuple[Any, ...] = ()

        logger.info(
            "StrategyEngine initialised | profile=%s | score_threshold=%d",
            self._profile,
            self._entry_score_threshold,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def state_version(self) -> int:
        return self._state_version

    def process_tick(self, price: float) -> Optional[str]:
        try:
            signal = self._process_tick_inner(price)
            self._sync_state_version()
            return signal
        except Exception:
            logger.exception(
                "Strategy tick processing failed; continuing in market-only mode."
            )
            self._previous_price = price
            return None

    def on_trade_executed(self) -> None:
        self._trades_in_trend += 1
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._pullback_start_price = None
        self._immediate_disabled_for_trend = True
        self._last_entry_mode = None
        self._reject_cooldown_ticks = 0
        self._consecutive_rejects = 0
        self._last_reject_reason = None

        logger.info(
            "Trade executed. Trades in current trend: %s",
            self._trades_in_trend,
        )

        self._sync_state_version()

    def on_signal_skipped(self) -> None:
        if self._last_entry_mode == "immediate":
            self._immediate_disabled_for_trend = True

        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._pullback_start_price = None
        self._consecutive_rejects = 0
        self._last_reject_reason = None

        self._sync_state_version()

    def update_mtf_bias(
        self,
        bias: Optional[str],
        agreement: int = 0,
        tf_biases: Optional[Dict[str, str]] = None,
    ) -> None:
        self._mtf_bias = bias
        self._mtf_agreement = agreement

        if tf_biases is not None:
            self._mtf_tf_biases = tf_biases
            self._mtf_sig = tuple(sorted(tf_biases.items()))

        self._sync_state_version()

    def get_state(self) -> Dict[str, Any]:
        try:
            tf_biases = dict(self._mtf_tf_biases)

            if self._micro:
                tf_biases["1m"] = self._micro

            return {
                "trend_direction": self._trend_direction,
                "trend_tick_count": self._trend_tick_count,
                "trend_kind": self._trend_kind,
                "trend_age": self._trend_age,
                "trades_in_trend": self._trades_in_trend,
                "in_cooldown": self._in_cooldown,
                "pattern_stage": self._pattern_stage,
                "mtf_bias": self._mtf_bias,
                "mtf_agreement": self._mtf_agreement,
                "mtf_tf_biases": tf_biases,
                "micro_bias": self._micro,
                "last_signal_score": self._last_signal_score,
                "last_signal_score_breakdown": dict(self._last_signal_score_breakdown),
                "last_entry_mode": self._last_entry_mode,
            }

        except Exception:
            logger.exception("Strategy get_state failed; returning safe fallback state.")

            return {
                "trend_direction": None,
                "trend_tick_count": 0,
                "trend_kind": None,
                "trend_age": 0,
                "trades_in_trend": 0,
                "in_cooldown": False,
                "pattern_stage": "IDLE",
                "mtf_bias": None,
                "mtf_agreement": 0,
                "mtf_tf_biases": {},
                "micro_bias": None,
                "last_signal_score": 0,
                "last_signal_score_breakdown": {},
                "last_entry_mode": None,
            }

    def reset(self) -> None:
        self._tick_buffer.clear()

        self._trend_direction = None
        self._trend_tick_count = 0
        self._trend_kind = None
        self._trend_age = 0
        self._trades_in_trend = 0
        self._in_cooldown = False
        self._cooldown_direction = None
        self._pattern_stage = "IDLE"
        self._continuation_ticks = 0
        self._previous_price = None

        self._mtf_bias = None
        self._mtf_agreement = 0
        self._mtf_tf_biases = {}
        self._micro = None

        self._last_signal_score = 0
        self._last_signal_score_breakdown = {}
        self._last_entry_mode = None

        self._pullback_start_price = None
        self._immediate_disabled_for_trend = False
        self._trend_start_price = None
        self._trend_extreme_price = None

        self._reject_cooldown_ticks = 0
        self._consecutive_rejects = 0
        self._last_reject_reason = None

        self._mtf_sig = ()
        self._breakdown_sig = ()
        self._state_signature = None
        self._state_version += 1

    # ------------------------------------------------------------------
    # State versioning
    # ------------------------------------------------------------------

    def _sync_state_version(self) -> None:
        signature = (
            self._trend_direction,
            self._trend_tick_count,
            self._trend_kind,
            self._trades_in_trend,
            self._in_cooldown,
            self._pattern_stage,
            self._mtf_bias,
            self._mtf_agreement,
            self._last_entry_mode,
            self._last_signal_score,
            self._mtf_sig,
            self._breakdown_sig,
        )

        if signature != self._state_signature:
            self._state_signature = signature
            self._state_version += 1

    # ------------------------------------------------------------------
    # Tick processing
    # ------------------------------------------------------------------

    def _process_tick_inner(self, price: float) -> Optional[str]:
        self._tick_buffer.append(price)

        if len(self._tick_buffer) < self._burst_window_min:
            self._previous_price = price
            return None

        ticks = list(self._tick_buffer)

        new_micro = _micro_bias(ticks, self._micro_bias_window)

        if new_micro != self._micro:
            self._micro = new_micro

        self._update_trend(ticks)

        if self._trend_direction is None or self._in_cooldown:
            self._previous_price = price
            return None

        if self._trades_in_trend >= MAX_TRADES_PER_TREND:
            self._enter_cooldown()
            self._previous_price = price
            return None

        if self._reject_cooldown_ticks > 0:
            self._reject_cooldown_ticks -= 1
            self._previous_price = price
            return None

        signal = self._evaluate_entry(price, ticks)
        self._previous_price = price
        return signal

    # ------------------------------------------------------------------
    # Trend detection
    # ------------------------------------------------------------------

    def _update_trend(self, ticks: List[float]) -> None:
        detected = self._scan_windows(
            ticks,
            self._burst_window_min,
            self._burst_window_max,
            self._burst_threshold,
        )
        kind = "burst"

        if detected is None:
            detected = self._scan_windows(
                ticks,
                self._trend_window_min,
                self._trend_window_max,
                self._velocity_threshold,
            )
            kind = "classic"

        if detected is not None and not self._passes_regime_filters(ticks, detected[1]):
            detected = None

        if detected is None:
            if self._trend_direction is not None:
                logger.debug("Trend %s dissolved.", self._trend_direction)

            self._trend_direction = None
            self._trend_tick_count = 0
            self._trend_kind = None
            self._trend_age = 0
            self._pattern_stage = "IDLE"
            self._continuation_ticks = 0
            self._pullback_start_price = None
            self._immediate_disabled_for_trend = False
            self._in_cooldown = False
            self._cooldown_direction = None
            self._trend_start_price = None
            self._trend_extreme_price = None
            self._reject_cooldown_ticks = 0
            self._consecutive_rejects = 0
            self._last_reject_reason = None
            return

        direction, window = detected

        if direction != self._trend_direction:
            if self._in_cooldown and direction == self._cooldown_direction:
                return

            self._trend_direction = direction
            self._trend_age = 0
            self._trades_in_trend = 0
            self._in_cooldown = False
            self._cooldown_direction = None
            self._pattern_stage = "TREND"
            self._continuation_ticks = 0
            self._pullback_start_price = None
            self._immediate_disabled_for_trend = False
            self._reject_cooldown_ticks = 0
            self._consecutive_rejects = 0
            self._last_reject_reason = None

            sample = ticks[-window:] if len(ticks) >= window else list(ticks)

            if sample:
                self._trend_start_price = sample[0]
                self._trend_extreme_price = (
                    max(sample) if direction == "UP" else min(sample)
                )
            else:
                self._trend_start_price = ticks[-1]
                self._trend_extreme_price = ticks[-1]

            logger.info(
                "New %s trend (%s path): %s-tick window",
                direction,
                kind,
                window,
            )
        else:
            self._trend_age += 1

            if self._pattern_stage == "IDLE":
                self._pattern_stage = "TREND"

            current_price = ticks[-1]

            if self._trend_extreme_price is None:
                self._trend_extreme_price = current_price
            elif direction == "UP":
                self._trend_extreme_price = max(
                    self._trend_extreme_price,
                    current_price,
                )
            else:
                self._trend_extreme_price = min(
                    self._trend_extreme_price,
                    current_price,
                )

            if self._trend_start_price is None:
                sample = ticks[-window:] if len(ticks) >= window else list(ticks)
                self._trend_start_price = sample[0] if sample else current_price

        self._trend_tick_count = window
        self._trend_kind = kind

    @staticmethod
    def _passes_regime_filters(ticks: List[float], window: int) -> bool:
        if MIN_TICK_VOLATILITY is not None or MAX_TICK_VOLATILITY is not None:
            vol = _tick_volatility(ticks, VOLATILITY_WINDOW)

            if vol is not None:
                if MIN_TICK_VOLATILITY is not None and vol < MIN_TICK_VOLATILITY:
                    return False

                if MAX_TICK_VOLATILITY is not None and vol > MAX_TICK_VOLATILITY:
                    return False

        if ACCELERATION_MIN_RATIO > 0:
            recent = _window_velocity(ticks, window)
            prior = _prior_window_velocity(ticks, window)

            if recent is not None and prior is not None:
                if abs(prior) > 0:
                    if (recent < 0) != (prior < 0):
                        return False

                    if abs(recent) < abs(prior) * ACCELERATION_MIN_RATIO:
                        return False

        return True

    @staticmethod
    def _scan_windows(
        ticks: List[float],
        window_min: int,
        window_max: int,
        threshold: float,
    ) -> Optional[Tuple[str, int]]:
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
        self._cooldown_direction = self._trend_direction
        self._trend_direction = None
        self._trend_kind = None
        self._trend_age = 0
        self._pattern_stage = "IDLE"
        self._trend_start_price = None
        self._trend_extreme_price = None
        self._reject_cooldown_ticks = 0
        self._consecutive_rejects = 0
        self._last_reject_reason = None

    # ------------------------------------------------------------------
    # Entry evaluation
    # ------------------------------------------------------------------

    def _reject_signal(
        self,
        reason: str,
        cooldown: Optional[int] = None,
        disable_immediate: bool = False,
    ) -> None:
        base = (
            self._limits["reject_cooldown_ticks"]
            if cooldown is None
            else cooldown
        )

        self._consecutive_rejects += 1

        if self._consecutive_rejects >= 2:
            base = int(base * 1.5)

        if self._consecutive_rejects >= 3:
            disable_immediate = True

        self._reject_cooldown_ticks = max(1, base)
        self._last_reject_reason = reason

        self._pattern_stage = "TREND" if self._trend_direction is not None else "IDLE"
        self._continuation_ticks = 0
        self._pullback_start_price = None

        if disable_immediate:
            self._immediate_disabled_for_trend = True

        logger.debug(
            "Setup rejected: %s | quiet=%d ticks",
            reason,
            self._reject_cooldown_ticks,
        )

    def _evaluate_entry(self, price: float, ticks: List[float]) -> Optional[str]:
        trend = self._trend_direction

        if trend not in ("UP", "DOWN"):
            return None

        signal = "BUY" if trend == "UP" else "SELL"

        if (
            not self._immediate_disabled_for_trend
            and self._trend_age <= self._early_trend_max_age
            and self._pattern_stage in ("TREND", "IDLE")
        ):
            allowed, reason = self._passes_entry_filters(
                signal,
                ticks,
                price,
                "immediate",
            )

            if allowed:
                score, breakdown, blocked = self._score_signal(
                    signal,
                    ticks,
                    entry_mode="immediate",
                )

                if blocked:
                    self._reject_signal(
                        "unanimous HTF against",
                        disable_immediate=True,
                    )
                    return None

                if score >= self._entry_score_threshold:
                    self._last_signal_score = score
                    self._last_signal_score_breakdown = breakdown
                    self._breakdown_sig = tuple(sorted(breakdown.items()))
                    self._pattern_stage = "SIGNAL"
                    self._last_entry_mode = "immediate"
                    self._consecutive_rejects = 0
                    self._reject_cooldown_ticks = 0
                    self._last_reject_reason = None

                    logger.info(
                        "IMMEDIATE %s entry | Score %d/%d | %s | trend_age=%d",
                        signal,
                        score,
                        SCORE_MAX,
                        breakdown,
                        self._trend_age,
                    )

                    return signal

                self._reject_signal(
                    f"score {score}/{SCORE_MAX} below threshold "
                    f"{self._entry_score_threshold}",
                    cooldown=max(3, self._limits["reject_cooldown_ticks"] // 2),
                )
                return None

            disable = (
                reason.startswith("unanimous HTF")
                or reason.startswith("HTF majority")
            )
            self._reject_signal(reason, disable_immediate=disable)
            return None

        return self._update_pattern(price, ticks, trend, signal)

    def _update_pattern(
        self,
        price: float,
        ticks: List[float],
        trend: str,
        signal: str,
    ) -> Optional[str]:
        if self._previous_price is None or price == self._previous_price:
            return None

        tick_direction = "UP" if price > self._previous_price else "DOWN"
        pullback_direction = "DOWN" if trend == "UP" else "UP"

        if self._pattern_stage in ("IDLE", "TREND"):
            if tick_direction == pullback_direction:
                self._pattern_stage = "PULLBACK"
                self._continuation_ticks = 0
                self._pullback_start_price = self._previous_price
            return None

        if self._pattern_stage == "PULLBACK":
            if tick_direction == pullback_direction:
                return None

            self._continuation_ticks = 1
            return self._signal_if_confirmed(trend, signal, ticks, price)

        if self._pattern_stage == "MOMENTUM":
            if tick_direction == trend:
                self._continuation_ticks += 1
                return self._signal_if_confirmed(trend, signal, ticks, price)

            if tick_direction == pullback_direction:
                self._pattern_stage = "PULLBACK"
                self._pullback_start_price = self._previous_price
            else:
                self._pattern_stage = "TREND"

            self._continuation_ticks = 0

        return None

    def _signal_if_confirmed(
        self,
        trend: str,
        signal: str,
        ticks: List[float],
        price: float,
    ) -> Optional[str]:
        if self._continuation_ticks < self._momentum_confirm_ticks:
            self._pattern_stage = "MOMENTUM"
            return None

        allowed, reason = self._passes_entry_filters(
            signal,
            ticks,
            price,
            "pullback",
        )

        if not allowed:
            disable = (
                reason.startswith("unanimous HTF")
                or reason.startswith("HTF majority")
            )
            self._reject_signal(reason, disable_immediate=disable)
            return None

        score, breakdown, blocked = self._score_signal(
            signal,
            ticks,
            entry_mode="pullback",
        )

        if blocked:
            self._reject_signal(
                "unanimous HTF against",
                disable_immediate=True,
            )
            return None

        if score >= self._entry_score_threshold:
            self._last_signal_score = score
            self._last_signal_score_breakdown = breakdown
            self._breakdown_sig = tuple(sorted(breakdown.items()))
            self._pattern_stage = "SIGNAL"
            self._last_entry_mode = "pullback"
            self._consecutive_rejects = 0
            self._reject_cooldown_ticks = 0
            self._last_reject_reason = None

            logger.info(
                "PULLBACK %s entry | Score %d/%d | %s | continuation=%d",
                signal,
                score,
                SCORE_MAX,
                breakdown,
                self._continuation_ticks,
            )

            return signal

        self._reject_signal(
            f"score {score}/{SCORE_MAX} below threshold "
            f"{self._entry_score_threshold}",
            cooldown=max(3, self._limits["reject_cooldown_ticks"] // 2),
        )
        return None

    # ------------------------------------------------------------------
    # Filters
    # ------------------------------------------------------------------

    def _passes_entry_filters(
        self,
        signal: str,
        ticks: List[float],
        price: float,
        entry_mode: str,
    ) -> Tuple[bool, str]:
        direction = "UP" if signal == "BUY" else "DOWN"
        limits = self._limits

        window = self._trend_tick_count

        if window < 3:
            window = min(len(ticks), max(3, self._burst_window_min))

        sample = ticks[-window:] if len(ticks) >= window else list(ticks)

        if len(sample) < 3:
            return False, "insufficient tick history"

        deltas = _deltas(sample)
        net = sample[-1] - sample[0]
        signed_net = net if direction == "UP" else -net

        if signed_net <= 0:
            return False, "net displacement against signal"

        er = _efficiency_ratio_from_deltas(net, deltas)

        min_er = (
            limits["immediate_min_er"]
            if entry_mode == "immediate"
            else limits["pullback_min_er"]
        )

        if er < min_er:
            return False, f"efficiency {er:.2f} below {min_er:.2f}"

        consistency = _consistency_from_deltas(deltas, direction)

        min_consistency = (
            limits["immediate_min_consistency"]
            if entry_mode == "immediate"
            else limits["pullback_min_consistency"]
        )

        if consistency < min_consistency:
            return (
                False,
                f"consistency {consistency:.2f} below {min_consistency:.2f}",
            )

        noise = _noise_from_deltas(deltas)

        if noise is not None and noise > 1e-12:
            displacement_ratio = signed_net / noise

            if displacement_ratio < limits["min_displacement_noise"]:
                return (
                    False,
                    f"weak displacement/noise {displacement_ratio:.2f} "
                    f"below {limits['min_displacement_noise']:.2f}",
                )

        whipsaw = _whipsaw_ratio_from_deltas(deltas)

        if whipsaw > limits["max_whipsaw_ratio"]:
            return (
                False,
                f"choppy whipsaw {whipsaw:.2f} above "
                f"{limits['max_whipsaw_ratio']:.2f}",
            )

        require_micro = (
            limits["require_micro_agree_immediate"]
            if entry_mode == "immediate"
            else limits["require_micro_agree_pullback"]
        )

        if (
            require_micro
            and self._micro is not None
            and self._micro != direction
        ):
            return False, "micro-bias disagrees"

        with_votes, against_votes, total_votes = _htf_vote_counts(
            self._mtf_tf_biases,
            direction,
        )

        if total_votes >= 3 and against_votes == total_votes:
            return False, "unanimous HTF against"

        require_htf_majority = (
            limits["require_htf_not_against_majority_immediate"]
            if entry_mode == "immediate"
            else limits["require_htf_not_against_majority_pullback"]
        )

        if (
            require_htf_majority
            and total_votes >= 2
            and against_votes > with_votes
        ):
            return False, "HTF majority against"

        if entry_mode == "immediate" and self._previous_price is not None:
            delta = price - self._previous_price
            signed_delta = delta if direction == "UP" else -delta

            if (
                signed_delta < 0
                and noise is not None
                and noise > 1e-12
                and abs(signed_delta) / noise
                > limits["counter_trend_tick_noise_mult"]
            ):
                return False, "counter-trend tick before entry"

        if entry_mode == "pullback":
            if (
                self._trend_start_price is None
                or self._trend_extreme_price is None
            ):
                return False, "impulse not tracked"

            if direction == "UP":
                impulse = self._trend_extreme_price - self._trend_start_price

                if impulse <= 0:
                    return False, "no valid bullish impulse"

                retrace = (self._trend_extreme_price - price) / impulse
            else:
                impulse = self._trend_start_price - self._trend_extreme_price

                if impulse <= 0:
                    return False, "no valid bearish impulse"

                retrace = (price - self._trend_extreme_price) / impulse

            if retrace < limits["min_pullback_retrace"]:
                return (
                    False,
                    f"pullback too shallow {retrace:.2f} "
                    f"below {limits['min_pullback_retrace']:.2f}",
                )

            if retrace > limits["max_pullback_retrace"]:
                return (
                    False,
                    f"pullback too deep {retrace:.2f} "
                    f"above {limits['max_pullback_retrace']:.2f}",
                )

            if self._previous_price is not None:
                delta = price - self._previous_price
                signed_delta = delta if direction == "UP" else -delta

                if signed_delta <= 0:
                    return False, "continuation tick wrong direction"

                if noise is not None and noise > 1e-12:
                    continuation_strength = signed_delta / noise

                    if continuation_strength < limits["continuation_noise_mult"]:
                        return (
                            False,
                            f"weak continuation {continuation_strength:.2f} "
                            f"below {limits['continuation_noise_mult']:.2f}",
                        )

        return True, "ok"

    # ------------------------------------------------------------------
    # Composite scoring
    # ------------------------------------------------------------------

    def _score_signal(
        self,
        signal: str,
        ticks: List[float],
        entry_mode: str,
    ) -> Tuple[int, Dict[str, int], bool]:
        direction = "UP" if signal == "BUY" else "DOWN"
        breakdown: Dict[str, int] = {}
        opposite = "DOWN" if direction == "UP" else "UP"

        candle_votes = [
            v
            for k, v in self._mtf_tf_biases.items()
            if k != "1m" and v in ("UP", "DOWN")
        ]

        against_votes = sum(1 for v in candle_votes if v == opposite)
        with_votes = sum(1 for v in candle_votes if v == direction)

        if len(candle_votes) >= 3 and against_votes == len(candle_votes):
            breakdown["htf"] = 0
            return 0, breakdown, True

        if with_votes >= 3:
            htf_pts = _SCORE_MTF_UNANIMOUS
        elif with_votes == 2 and with_votes > against_votes:
            htf_pts = _SCORE_MTF_MAJORITY
        elif against_votes >= 2 and against_votes > with_votes:
            htf_pts = 0
        else:
            htf_pts = _SCORE_MTF_NEUTRAL

        breakdown["htf"] = htf_pts

        window = self._trend_tick_count
        er_pts = 0

        if window > 1 and len(ticks) >= window:
            er = _efficiency_ratio(ticks[-window:])

            if er >= 0.80:
                er_pts = _SCORE_ER_HIGH
            elif er >= 0.70:
                er_pts = _SCORE_ER_GOOD
            elif er >= 0.60:
                er_pts = _SCORE_ER_OK

        breakdown["quality"] = er_pts

        cons_pts = 0

        if window > 1 and len(ticks) >= window:
            consistency = _tick_consistency(ticks[-window:], direction)

            if consistency >= 0.75:
                cons_pts = _SCORE_CONSISTENCY_HIGH
            elif consistency >= 0.60:
                cons_pts = _SCORE_CONSISTENCY_GOOD

        breakdown["consistency"] = cons_pts

        if self._trend_age <= self._early_trend_max_age:
            early_pts = _SCORE_EARLY_STRONG
        elif self._trend_age <= 2 * self._early_trend_max_age:
            early_pts = _SCORE_EARLY_MILD
        else:
            early_pts = 0

        breakdown["early"] = early_pts

        micro_penalty = 0

        if self._micro is not None and self._micro != direction:
            micro_penalty = _PENALTY_MICRO_DISAGREE

        breakdown["micro_penalty"] = -micro_penalty

        score = max(0, er_pts + htf_pts + cons_pts + early_pts - micro_penalty)

        return score, breakdown, False

    def _validate_mtf(self, signal: str) -> bool:
        """Legacy helper retained for compatibility with existing tests."""
        if self._mtf_agreement < self._mtf_min_agreement:
            return False

        return (signal == "BUY" and self._mtf_bias == "UP") or (
            signal == "SELL" and self._mtf_bias == "DOWN"
        )


# ---------------------------------------------------------------------------
# MTF Analyzer
# ---------------------------------------------------------------------------

class MTFAnalyzer:
    """Multi-timeframe bias with a configurable majority requirement."""

    def __init__(self, min_agreement: int = MTF_MIN_AGREEMENT):
        self._min_agreement = min_agreement

    def analyze(self, candles_by_tf: Dict[str, List[Dict]]) -> Optional[str]:
        return self.analyze_with_strength(candles_by_tf)[0]

    def analyze_with_strength(
        self,
        candles_by_tf: Dict[str, List[Dict]],
    ) -> Tuple[Optional[str], int, Dict[str, str]]:
        votes: List[str] = []
        tf_biases: Dict[str, str] = {}

        for label, candles in candles_by_tf.items():
            bias = self._analyze_single_tf(candles, label)

            if bias:
                votes.append(bias)
                tf_biases[label] = bias
            else:
                tf_biases[label] = "FLAT"

        if not votes:
            return None, 0, tf_biases

        up_votes = votes.count("UP")
        down_votes = votes.count("DOWN")

        if up_votes >= self._min_agreement and up_votes >= down_votes:
            return "UP", up_votes, tf_biases

        if down_votes >= self._min_agreement and down_votes > up_votes:
            return "DOWN", down_votes, tf_biases

        return None, max(up_votes, down_votes), tf_biases

    @staticmethod
    def _analyze_single_tf(candles: List[Dict], label: str) -> Optional[str]:
        if len(candles) < 3:
            logger.warning("Insufficient candles for %s MTF analysis.", label)
            return None

        try:
            closes = [float(candle["close"]) for candle in candles]
        except (KeyError, TypeError, ValueError):
            return None

        bias = _candle_slope_bias(closes)

        logger.debug(
            "MTF %s: bias=%s (first_close=%.4f, last_close=%.4f)",
            label,
            bias,
            closes[0],
            closes[-1],
        )

        return bias