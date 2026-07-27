"""
src/strategy.py
MomentumMaster X — v6 Aurora
Institutional tick-momentum engine with adaptive regime detection,
momentum ignition, trend intuition, anti-chop protection, and
dynamic contract suggestions.

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
    ENTRY_SCORE_THRESHOLD,
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

_REGIME_MODS = {
    "INIT": {
        "er": 1.05,
        "cons": 1.05,
        "disp": 1.05,
        "whip": 0.95,
        "retrace_min": 1.00,
        "retrace_max": 1.00,
        "ignition": 5,
    },
    "TRENDING": {
        "er": 0.94,
        "cons": 0.95,
        "disp": 0.90,
        "whip": 1.15,
        "retrace_min": 0.90,
        "retrace_max": 1.08,
        "ignition": -6,
    },
    "MIXED": {
        "er": 1.00,
        "cons": 1.00,
        "disp": 1.00,
        "whip": 1.00,
        "retrace_min": 1.00,
        "retrace_max": 1.00,
        "ignition": 0,
    },
    "QUIET": {
        "er": 1.04,
        "cons": 1.00,
        "disp": 1.08,
        "whip": 0.92,
        "retrace_min": 0.85,
        "retrace_max": 1.15,
        "ignition": 4,
    },
    "WILD": {
        "er": 1.10,
        "cons": 1.15,
        "disp": 1.18,
        "whip": 0.78,
        "retrace_min": 1.10,
        "retrace_max": 0.90,
        "ignition": 10,
    },
    "CHOPPY": {
        "er": 1.22,
        "cons": 1.25,
        "disp": 1.35,
        "whip": 0.60,
        "retrace_min": 1.25,
        "retrace_max": 0.75,
        "ignition": 22,
    },
    "NOISY": {
        "er": 1.14,
        "cons": 1.16,
        "disp": 1.22,
        "whip": 0.72,
        "retrace_min": 1.15,
        "retrace_max": 0.85,
        "ignition": 14,
    },
}

_IGNITION_MIN = {
    "aggressive": 52,
    "balanced": 62,
    "conservative": 72,
}


# ---------------------------------------------------------------------------
# Small math helpers
# ---------------------------------------------------------------------------

def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    if n % 2 == 1:
        return float(ordered[n // 2])
    return float((ordered[n // 2 - 1] + ordered[n // 2]) / 2.0)


def _percentile_rank(values, x: float) -> float:
    if not values:
        return 0.5
    count = 0
    for v in values:
        if v <= x:
            count += 1
    return count / float(len(values))


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


def _magnitude_asymmetry(deltas: List[float], direction: str) -> float:
    with_sum = 0.0
    with_n = 0
    against_sum = 0.0
    against_n = 0

    for d in deltas:
        if d == 0:
            continue

        with_trend = (direction == "UP" and d > 0) or (direction == "DOWN" and d < 0)

        if with_trend:
            with_sum += abs(d)
            with_n += 1
        else:
            against_sum += abs(d)
            against_n += 1

    if with_n == 0:
        return 0.0

    if against_n == 0:
        return 3.0

    with_avg = with_sum / with_n
    against_avg = against_sum / against_n

    if against_avg <= 1e-12:
        return 3.0

    return min(3.0, with_avg / against_avg)


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


def _acceleration_score(ticks: List[float], direction: str) -> int:
    if len(ticks) < 9:
        return 0

    v3 = _window_velocity(ticks, 3)
    v8 = _window_velocity(ticks, 8)

    if v3 is None or v8 is None:
        return 0

    signed3 = v3 if direction == "UP" else -v3
    signed8 = v8 if direction == "UP" else -v8

    if signed3 <= 0:
        return 0

    if signed8 <= 0:
        return 6

    if signed3 >= signed8 * 1.25:
        return 15
    if signed3 >= signed8 * 1.05:
        return 11
    if signed3 >= signed8 * 0.85:
        return 6

    return 2


def _horizon_features(
    ticks: List[float],
    window: int,
    direction: str,
) -> Optional[Dict[str, float]]:
    if len(ticks) < max(3, window):
        return None

    sample = ticks[-window:]
    if len(sample) < 3:
        return None

    deltas = _deltas(sample)
    net = sample[-1] - sample[0]
    signed_net = net if direction == "UP" else -net

    if signed_net <= 0:
        return None

    er = _efficiency_ratio_from_deltas(net, deltas)
    consistency = _consistency_from_deltas(deltas, direction)
    noise = _noise_from_deltas(deltas)
    displacement = signed_net / noise if noise is not None and noise > 1e-12 else 0.0
    whipsaw = _whipsaw_ratio_from_deltas(deltas)
    asymmetry = _magnitude_asymmetry(deltas, direction)

    return {
        "window": float(window),
        "signed_net": signed_net,
        "er": er,
        "consistency": consistency,
        "noise": noise if noise is not None else 0.0,
        "displacement": displacement,
        "whipsaw": whipsaw,
        "asymmetry": asymmetry,
    }


def _raw_feature_score(feats: Dict[str, float], accel: int) -> float:
    score = 0.0

    disp = feats["displacement"]
    if disp >= 2.6:
        score += 40
    elif disp >= 2.1:
        score += 34
    elif disp >= 1.7:
        score += 28
    elif disp >= 1.4:
        score += 22
    elif disp >= 1.2:
        score += 16
    elif disp >= 1.0:
        score += 10

    er = feats["er"]
    if er >= 0.88:
        score += 25
    elif er >= 0.80:
        score += 21
    elif er >= 0.72:
        score += 17
    elif er >= 0.64:
        score += 12
    elif er >= 0.56:
        score += 7

    cons = feats["consistency"]
    if cons >= 0.82:
        score += 20
    elif cons >= 0.74:
        score += 16
    elif cons >= 0.66:
        score += 12
    elif cons >= 0.58:
        score += 8

    asym = feats["asymmetry"]
    if asym >= 2.0:
        score += 10
    elif asym >= 1.6:
        score += 8
    elif asym >= 1.3:
        score += 5
    elif asym >= 1.1:
        score += 2

    score += accel

    whip = feats["whipsaw"]
    if whip > 0.58:
        score -= 26
    elif whip > 0.48:
        score -= 14
    elif whip > 0.38:
        score -= 6

    return _clamp(score, 0.0, 100.0)


# ---------------------------------------------------------------------------
# Strategy Engine
# ---------------------------------------------------------------------------

class StrategyEngine:
    """MomentumMaster X v6 Aurora tick-momentum strategy."""

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
        self._last_signal_confidence: float = 0.0

        self._pullback_start_price: Optional[float] = None
        self._immediate_disabled_for_trend = False
        self._trend_start_price: Optional[float] = None
        self._trend_extreme_price: Optional[float] = None

        self._reject_cooldown_ticks: int = 0
        self._consecutive_rejects: int = 0
        self._last_reject_reason: Optional[str] = None

        # Momentum intelligence.
        self._regime: str = "INIT"
        self._regime_quality: float = 0.5
        self._vol_history: deque = deque(maxlen=300)
        self._er_history: deque = deque(maxlen=300)
        self._whip_history: deque = deque(maxlen=300)

        self._ignition_score: float = 0.0
        self._ignition_consistency: float = 0.0
        self._impulse_score: float = 0.0
        self._impulse_consistency: float = 0.0
        self._intuition_score: float = 0.0

        self._suggested_barrier_abs: Optional[float] = None
        self._suggested_duration: int = 5

        # State versioning for calm dashboard refresh.
        self._state_version: int = 0
        self._state_signature: Optional[Tuple[Any, ...]] = None
        self._mtf_sig: Tuple[Any, ...] = ()
        self._breakdown_sig: Tuple[Any, ...] = ()

        logger.info(
            "StrategyEngine initialised | profile=%s | score_threshold=%d | engine=MomentumMasterX-v6",
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
        signal = self._process_tick_inner(price)
        self._sync_state_version()
        return signal

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
            "last_signal_confidence": round(self._last_signal_confidence, 1),
            "regime": self._regime,
            "regime_quality": round(self._regime_quality, 2),
            "ignition_score": round(self._ignition_score, 1),
            "impulse_score": round(self._impulse_score, 1),
            "intuition_score": round(self._intuition_score, 1),
            "suggested_barrier_abs": self._suggested_barrier_abs,
            "suggested_duration": self._suggested_duration,
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
        self._last_signal_confidence = 0.0

        self._pullback_start_price = None
        self._immediate_disabled_for_trend = False
        self._trend_start_price = None
        self._trend_extreme_price = None

        self._reject_cooldown_ticks = 0
        self._consecutive_rejects = 0
        self._last_reject_reason = None

        self._regime = "INIT"
        self._regime_quality = 0.5
        self._vol_history.clear()
        self._er_history.clear()
        self._whip_history.clear()

        self._ignition_score = 0.0
        self._ignition_consistency = 0.0
        self._impulse_score = 0.0
        self._impulse_consistency = 0.0
        self._intuition_score = 0.0

        self._suggested_barrier_abs = None
        self._suggested_duration = 5

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
            self._regime,
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

        self._update_regime(ticks)
        self._update_contract_suggestions(ticks)
        self._update_trend(ticks)

        if self._trend_direction is None or self._in_cooldown:
            self._ignition_score = 0.0
            self._ignition_consistency = 0.0
            self._impulse_score = 0.0
            self._impulse_consistency = 0.0
            self._intuition_score = 0.0
            self._previous_price = price
            return None

        self._ignition_score, self._ignition_consistency = self._compute_ignition(
            ticks,
            self._trend_direction,
        )

        self._impulse_score, self._impulse_consistency = self._compute_impulse(
            ticks,
            self._trend_direction,
            max(self._trend_tick_count, 5),
        )

        self._intuition_score = self._compute_intuition(self._trend_direction)

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
    # Regime detection
    # ------------------------------------------------------------------

    def _update_regime(self, ticks: List[float]) -> None:
        if len(ticks) < 31:
            self._regime = "INIT"
            self._regime_quality = 0.5
            return

        deltas = _deltas(ticks[-31:])
        vol = _noise_from_deltas(deltas)

        if vol is None:
            self._regime = "INIT"
            self._regime_quality = 0.5
            return

        self._vol_history.append(vol)

        er20 = _efficiency_ratio(ticks[-21:]) if len(ticks) >= 21 else _efficiency_ratio(ticks)
        whip = _whipsaw_ratio_from_deltas(deltas)

        self._er_history.append(er20)
        self._whip_history.append(whip)

        vol_pct = _percentile_rank(self._vol_history, vol)

        if vol_pct < 0.18:
            regime = "QUIET"
        elif whip > 0.58 and er20 < 0.42:
            regime = "CHOPPY"
        elif vol_pct > 0.85 and er20 < 0.45:
            regime = "WILD"
        elif er20 >= 0.55 and whip <= 0.48:
            regime = "TRENDING"
        elif er20 >= 0.45 and whip <= 0.55:
            regime = "MIXED"
        else:
            regime = "NOISY"

        er_q = _clamp((er20 - 0.30) / 0.50, 0.0, 1.0)
        whip_q = _clamp((0.65 - whip) / 0.45, 0.0, 1.0)
        vol_q = _clamp(1.0 - abs(vol_pct - 0.55) / 0.45, 0.0, 1.0)

        quality = 0.45 * er_q + 0.35 * whip_q + 0.20 * vol_q

        if regime in ("CHOPPY", "WILD"):
            quality *= 0.60
        elif regime == "QUIET":
            quality *= 0.80
        elif regime == "NOISY":
            quality *= 0.70

        self._regime = regime
        self._regime_quality = _clamp(quality, 0.0, 1.0)

    # ------------------------------------------------------------------
    # Momentum intelligence
    # ------------------------------------------------------------------

    def _compute_ignition(
        self,
        ticks: List[float],
        direction: str,
    ) -> Tuple[float, float]:
        best_raw = 0.0
        best_consistency = 0.0

        for horizon in (3, 5, 8, 13):
            feats = _horizon_features(ticks, horizon, direction)
            if feats is None:
                continue

            accel = _acceleration_score(ticks, direction)
            raw = _raw_feature_score(feats, accel)

            if raw > best_raw:
                best_raw = raw
                best_consistency = feats["consistency"]

        mods = _REGIME_MODS.get(self._regime, _REGIME_MODS["MIXED"])
        adjusted = best_raw + float(mods.get("ignition", 0))

        return _clamp(adjusted, 0.0, 100.0), best_consistency

    def _compute_impulse(
        self,
        ticks: List[float],
        direction: str,
        window: int,
    ) -> Tuple[float, float]:
        window = min(len(ticks), max(3, window))
        feats = _horizon_features(ticks, window, direction)

        if feats is None:
            return 0.0, 0.0

        accel = _acceleration_score(ticks, direction)
        raw = _raw_feature_score(feats, accel)

        mods = _REGIME_MODS.get(self._regime, _REGIME_MODS["MIXED"])
        adjusted = raw + float(mods.get("ignition", 0)) * 0.5

        return _clamp(adjusted, 0.0, 100.0), feats["consistency"]

    def _compute_intuition(self, direction: str) -> float:
        power = max(self._ignition_score, self._impulse_score)

        base = 0.55 * power + 25.0 * self._regime_quality

        with_votes, against_votes, total_votes = _htf_vote_counts(
            self._mtf_tf_biases,
            direction,
        )

        if total_votes > 0:
            htf_component = 12.0 * max(0.0, (with_votes - against_votes) / total_votes)
        else:
            htf_component = 4.0

        if self._micro == direction:
            micro_component = 8.0
        elif self._micro is None:
            micro_component = 0.0
        else:
            micro_component = -6.0

        if self._trend_age <= self._early_trend_max_age:
            early_component = 5.0
        elif self._trend_age <= 2 * self._early_trend_max_age:
            early_component = 2.0
        else:
            early_component = 0.0

        return _clamp(base + htf_component + micro_component + early_component, 0.0, 100.0)

    def _update_contract_suggestions(self, ticks: List[float]) -> None:
        if len(ticks) < 31:
            return

        deltas = _deltas(ticks[-31:])
        abs_deltas = [abs(d) for d in deltas if d != 0]

        if not abs_deltas:
            return

        median_tick_move = _median(abs_deltas)
        if median_tick_move <= 1e-12:
            return

        price = ticks[-1]

        low = max(0.02, price * 0.00004)
        high = max(low * 2.0, price * 0.0025)

        barrier = median_tick_move * 3.6
        self._suggested_barrier_abs = round(_clamp(barrier, low, high), 3)

        if self._vol_history:
            vol_pct = _percentile_rank(self._vol_history, self._vol_history[-1])
        else:
            vol_pct = 0.5

        if vol_pct > 0.80:
            self._suggested_duration = 6
        elif vol_pct < 0.25:
            self._suggested_duration = 4
        else:
            self._suggested_duration = 5

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
                "New %s trend (%s path): %s-tick window | regime=%s",
                direction,
                kind,
                window,
                self._regime,
            )
        else:
            self._trend_age += 1

            if self._pattern_stage == "IDLE":
                self._pattern_stage = "TREND"

            current_price = ticks[-1]

            if self._trend_extreme_price is None:
                self._trend_extreme_price = current_price
            elif direction == "UP":
                self._trend_extreme_price = max(self._trend_extreme_price, current_price)
            else:
                self._trend_extreme_price = min(self._trend_extreme_price, current_price)

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

            if recent is not None and prior is not None and abs(prior) > 0:
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
            "Setup rejected: %s | quiet=%d ticks | regime=%s",
            reason,
            self._reject_cooldown_ticks,
            self._regime,
        )

    def _evaluate_entry(self, price: float, ticks: List[float]) -> Optional[str]:
        trend = self._trend_direction

        if trend not in ("UP", "DOWN"):
            return None

        signal = "BUY" if trend == "UP" else "SELL"

        # Immediate early-capture entry.
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
                    self._last_signal_confidence = self._intuition_score
                    self._consecutive_rejects = 0
                    self._reject_cooldown_ticks = 0
                    self._last_reject_reason = None

                    logger.info(
                        "IGNITION %s entry | Score %d/%d | Confidence %.1f | "
                        "Regime %s | Ignition %.1f | Intuition %.1f | %s",
                        signal,
                        score,
                        SCORE_MAX,
                        self._intuition_score,
                        self._regime,
                        self._ignition_score,
                        self._intuition_score,
                        breakdown,
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

        # Pullback continuation entry.
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
            self._last_signal_confidence = self._intuition_score
            self._consecutive_rejects = 0
            self._reject_cooldown_ticks = 0
            self._last_reject_reason = None

            logger.info(
                "PULLBACK %s entry | Score %d/%d | Confidence %.1f | "
                "Regime %s | Impulse %.1f | Intuition %.1f | continuation=%d",
                signal,
                score,
                SCORE_MAX,
                self._intuition_score,
                self._regime,
                self._impulse_score,
                self._intuition_score,
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
    # Adaptive institutional filters
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
        mods = _REGIME_MODS.get(self._regime, _REGIME_MODS["MIXED"])

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
        min_er = _clamp(
            (
                limits["immediate_min_er"]
                if entry_mode == "immediate"
                else limits["pullback_min_er"]
            )
            * mods["er"],
            0.45,
            0.95,
        )

        if er < min_er:
            return False, f"efficiency {er:.2f} below {min_er:.2f}"

        consistency = _consistency_from_deltas(deltas, direction)
        min_consistency = _clamp(
            (
                limits["immediate_min_consistency"]
                if entry_mode == "immediate"
                else limits["pullback_min_consistency"]
            )
            * mods["cons"],
            0.45,
            0.95,
        )

        if consistency < min_consistency:
            return (
                False,
                f"consistency {consistency:.2f} below {min_consistency:.2f}",
            )

        noise = _noise_from_deltas(deltas)

        if noise is not None and noise > 1e-12:
            displacement_ratio = signed_net / noise
            min_displacement = _clamp(
                limits["min_displacement_noise"] * mods["disp"],
                0.80,
                3.50,
            )

            if displacement_ratio < min_displacement:
                return (
                    False,
                    f"weak displacement/noise {displacement_ratio:.2f} "
                    f"below {min_displacement:.2f}",
                )

        whipsaw = _whipsaw_ratio_from_deltas(deltas)
        max_whipsaw = _clamp(
            limits["max_whipsaw_ratio"] * mods["whip"],
            0.10,
            0.80,
        )

        if whipsaw > max_whipsaw:
            return (
                False,
                f"choppy whipsaw {whipsaw:.2f} above {max_whipsaw:.2f}",
            )

        power = max(self._ignition_score, self._impulse_score)
        ignition_min = _IGNITION_MIN[self._profile] + float(mods.get("ignition", 0))

        if entry_mode == "pullback":
            ignition_min -= 4.0

        ignition_min = _clamp(ignition_min, 35.0, 92.0)

        if power < ignition_min:
            return (
                False,
                f"momentum power {power:.0f} below {ignition_min:.0f}",
            )

        if self._regime == "CHOPPY" and entry_mode == "immediate":
            return False, "choppy regime blocks immediate entries"

        if self._regime == "NOISY" and power < ignition_min + 8.0:
            return False, "noisy regime requires extreme momentum"

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

            min_retrace = _clamp(
                limits["min_pullback_retrace"] * mods["retrace_min"],
                0.03,
                0.45,
            )
            max_retrace = _clamp(
                limits["max_pullback_retrace"] * mods["retrace_max"],
                0.35,
                0.90,
            )

            if min_retrace >= max_retrace:
                min_retrace = max_retrace * 0.6

            if retrace < min_retrace:
                return (
                    False,
                    f"pullback too shallow {retrace:.2f} below {min_retrace:.2f}",
                )

            if retrace > max_retrace:
                return (
                    False,
                    f"pullback too deep {retrace:.2f} above {max_retrace:.2f}",
                )

            if self._previous_price is not None:
                delta = price - self._previous_price
                signed_delta = delta if direction == "UP" else -delta

                if signed_delta <= 0:
                    return False, "continuation tick wrong direction"

                continuation_multiplier = limits["continuation_noise_mult"]
                if self._regime == "WILD":
                    continuation_multiplier *= 1.15

                if noise is not None and noise > 1e-12:
                    continuation_strength = signed_delta / noise

                    if continuation_strength < continuation_multiplier:
                        return (
                            False,
                            f"weak continuation {continuation_strength:.2f} "
                            f"below {continuation_multiplier:.2f}",
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

        power = max(self._ignition_score, self._impulse_score)

        if power >= 85:
            quality_pts = _SCORE_ER_HIGH
        elif power >= 76:
            quality_pts = _SCORE_ER_GOOD
        elif power >= 66:
            quality_pts = 3
        elif power >= 56:
            quality_pts = _SCORE_ER_OK
        elif power >= 46:
            quality_pts = 1
        else:
            quality_pts = 0

        breakdown["quality"] = quality_pts

        consistency = max(self._ignition_consistency, self._impulse_consistency)

        if consistency >= 0.80:
            cons_pts = _SCORE_CONSISTENCY_HIGH
        elif consistency >= 0.68:
            cons_pts = _SCORE_CONSISTENCY_GOOD
        elif consistency >= 0.58:
            cons_pts = 1
        else:
            cons_pts = 0

        breakdown["consistency"] = cons_pts

        if self._trend_age <= self._early_trend_max_age and self._regime_quality >= 0.45:
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

        score = max(0, quality_pts + htf_pts + cons_pts + early_pts - micro_penalty)

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