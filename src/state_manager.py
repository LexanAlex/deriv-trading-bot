"""
src/state_manager.py
Thread-safe shared state container used by the async trading engine and UI.
"""

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from config import DEFAULT_INITIAL_STAKE, STAKE_MODE, TICK_BUFFER_SIZE, TRADE_HISTORY_LIMIT


@dataclass
class TradeRecord:
    """Represents a single trade attempt and its outcome."""

    trade_id: str
    direction: str
    stake: float
    barrier: str
    entry_price: float
    timestamp: str
    status: str

    pnl: float = 0.0
    contract_id: Optional[int] = None
    martingale_step: int = 0
    execution_mode: str = "UNSPECIFIED"
    account_type: str = "UNKNOWN"
    error_message: str = ""

    payout: float = 0.0
    score: int = 0
    score_breakdown: Dict[str, int] = field(default_factory=dict)
    entry_mode: str = ""
    trend_kind: str = ""
    trend_age: int = 0
    micro_bias: str = ""
    mtf_bias: str = ""
    mtf_agreement: int = 0
    prefetch_used: bool = False
    proposal_latency: float = -1.0
    buy_latency: float = -1.0
    settlement_latency: float = -1.0
    exit_reason: str = ""


class StateManager:
    """Thread-safe shared runtime state."""

    FINAL_TRADE_STATUSES = {"WON", "LOST", "UNKNOWN", "CANCELLED"}

    _STRATEGY_STATE_ATTRS = {
        "trend_direction": "_current_trend_direction",
        "trend_tick_count": "_trend_tick_count",
        "trend_kind": "_trend_kind",
        "trades_in_trend": "_trades_in_current_trend",
        "in_cooldown": "_in_cooldown",
        "pattern_stage": "_pattern_stage",
        "pattern_ticks": "_pattern_ticks",
        "mtf_bias": "_mtf_bias",
        "mtf_agreement": "_mtf_agreement",
        "mtf_tf_biases": "_mtf_tf_biases",
        "micro_bias": "_micro_bias",
        "last_entry_mode": "_last_entry_mode",
        "last_signal_score": "_last_signal_score",
        "last_signal_score_breakdown": "_last_signal_score_breakdown",
    }

    def __init__(self):
        self._lock = threading.Lock()

        self._is_running = False
        self._stop_requested = False

        self._current_price = 0.0
        self._recent_ticks: deque = deque(maxlen=TICK_BUFFER_SIZE)
        self._tick_timestamps: deque = deque(maxlen=TICK_BUFFER_SIZE)
        self._tick_receipt_times: deque = deque(maxlen=max(TICK_BUFFER_SIZE, 240))
        self._total_ticks_processed = 0

        self._current_trend_direction: Optional[str] = None
        self._trend_tick_count = 0
        self._trend_kind: Optional[str] = None
        self._trades_in_current_trend = 0
        self._in_cooldown = False
        self._pattern_stage = "IDLE"
        self._pattern_ticks: List[float] = []

        self._mtf_bias: Optional[str] = None
        self._mtf_agreement = 0
        self._mtf_tf_biases: Dict[str, str] = {}
        self._micro_bias: Optional[str] = None
        self._last_entry_mode: Optional[str] = None

        self._last_signal_score = 0
        self._last_signal_score_breakdown: Dict[str, int] = {}

        self._current_martingale_step = 0
        self._current_stake = DEFAULT_INITIAL_STAKE
        self._initial_stake = DEFAULT_INITIAL_STAKE

        self._last_trade_time = 0.0
        self._session_pnl = 0.0
        self._consecutive_losses = 0

        self._trade_history: deque = deque(maxlen=TRADE_HISTORY_LIMIT)
        self._trades_by_id: Dict[str, TradeRecord] = {}
        self._total_pnl = 0.0
        self._wins = 0
        self._losses = 0

        self._execution_context: Dict[str, str] = {
            "account_id": "",
            "account_type": "UNKNOWN",
            "currency": "USD",
            "execution_mode": "UNCONFIGURED",
        }

        self._status_message = "Bot is stopped."
        self._error_message = ""

        self._signals_generated = 0
        self._signals_executed = 0
        self._signals_skipped_gate = 0
        self._signals_rejected_strategy = 0
        self._skip_reason_counts: Dict[str, int] = {}
        self._reject_reason_counts: Dict[str, int] = {}
        self._last_skip_reason = ""
        self._last_reject_reason = ""
        self._last_signal_time: Optional[float] = None

        self._prefetch_hits = 0
        self._prefetch_misses = 0
        self._reconnects = 0
        self._latency_stats = self._new_latency_stats()

    @staticmethod
    def _new_latency_stats() -> Dict[str, Dict[str, float]]:
        return {
            "proposal": {"sum": 0.0, "count": 0, "last": -1.0},
            "buy": {"sum": 0.0, "count": 0, "last": -1.0},
            "settlement": {"sum": 0.0, "count": 0, "last": -1.0},
        }

    # ------------------------------------------------------------------
    # Bot control
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def set_running(self, value: bool):
        with self._lock:
            self._is_running = value
            if value:
                self._stop_requested = False

    @property
    def stop_requested(self) -> bool:
        with self._lock:
            return self._stop_requested

    def request_stop(self):
        with self._lock:
            self._stop_requested = True

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def update_tick(self, price: float, timestamp: float):
        with self._lock:
            self._current_price = price
            self._recent_ticks.append(price)
            self._tick_timestamps.append(timestamp)
            self._tick_receipt_times.append(time.time())
            self._total_ticks_processed += 1

    @property
    def current_price(self) -> float:
        with self._lock:
            return self._current_price

    def get_recent_ticks(self) -> List[float]:
        with self._lock:
            return list(self._recent_ticks)

    def get_tick_timestamps(self) -> List[float]:
        with self._lock:
            return list(self._tick_timestamps)

    def get_tick_heartbeat(self) -> Dict[str, Any]:
        with self._lock:
            last_server_tick_time = self._tick_timestamps[-1] if self._tick_timestamps else None
            last_local_tick_time = self._tick_receipt_times[-1] if self._tick_receipt_times else None
            return {
                "total_ticks_processed": self._total_ticks_processed,
                "last_tick_time": last_server_tick_time,
                "last_tick_local_time": last_local_tick_time,
            }

    # ------------------------------------------------------------------
    # Strategy state
    # ------------------------------------------------------------------

    def get_strategy_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "trend_direction": self._current_trend_direction,
                "trend_tick_count": self._trend_tick_count,
                "trend_kind": self._trend_kind,
                "trades_in_trend": self._trades_in_current_trend,
                "in_cooldown": self._in_cooldown,
                "pattern_stage": self._pattern_stage,
                "pattern_ticks": list(self._pattern_ticks),
                "mtf_bias": self._mtf_bias,
                "mtf_agreement": self._mtf_agreement,
                "mtf_tf_biases": dict(self._mtf_tf_biases),
                "micro_bias": self._micro_bias,
                "last_entry_mode": self._last_entry_mode,
                "last_signal_score": self._last_signal_score,
                "last_signal_score_breakdown": dict(self._last_signal_score_breakdown),
            }

    def update_strategy_state(self, **kwargs):
        with self._lock:
            self._apply_strategy_state(kwargs)

    def _apply_strategy_state(self, kwargs: Dict[str, Any]) -> None:
        for key, value in kwargs.items():
            attr = self._STRATEGY_STATE_ATTRS.get(key)
            if attr is not None:
                setattr(self, attr, value)

    def update_tick_and_strategy_state(self, price: float, timestamp: float, **strategy_kwargs) -> None:
        with self._lock:
            self._current_price = price
            self._recent_ticks.append(price)
            self._tick_timestamps.append(timestamp)
            self._tick_receipt_times.append(time.time())
            self._total_ticks_processed += 1
            self._apply_strategy_state(strategy_kwargs)

    # ------------------------------------------------------------------
    # Martingale / stake state
    # ------------------------------------------------------------------

    def get_martingale_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "step": self._current_martingale_step,
                "stake": self._current_stake,
                "initial_stake": self._initial_stake,
                "stake_mode": str(STAKE_MODE or "MARTINGALE").upper(),
            }

    def set_initial_stake(self, stake: float):
        with self._lock:
            self._initial_stake = stake
            self._current_stake = stake

    def on_trade_win(self):
        with self._lock:
            self._current_martingale_step = 0
            self._current_stake = self._initial_stake

    def on_trade_loss(self, multiplier: float, max_steps: int):
        """
        Advance stake recovery on a loss.

        max_steps is the TOTAL number of stake levels including the initial stake.
        """
        with self._lock:
            mode = str(STAKE_MODE or "MARTINGALE").upper()
            safe_max_steps = max(1, int(max_steps))

            if mode == "FLAT":
                self._current_martingale_step += 1
                self._current_stake = self._initial_stake
                if self._current_martingale_step >= safe_max_steps:
                    self._current_martingale_step = 0
                return

            if self._current_martingale_step + 1 < safe_max_steps:
                self._current_martingale_step += 1
                self._current_stake = round(self._current_stake * multiplier, 2)
            else:
                self._current_martingale_step = 0
                self._current_stake = self._initial_stake

    def drawdown_limit_hit(self) -> bool:
        """No drawdown limit is enforced by design."""
        return False

    # ------------------------------------------------------------------
    # Trade pacing / cooldown
    # ------------------------------------------------------------------

    def _required_cooldown_unsafe(self) -> float:
        if self._consecutive_losses >= 2:
            return 180.0
        if self._consecutive_losses >= 1:
            return 90.0
        return 30.0

    def _cooldown_remaining_unsafe(self) -> float:
        if self._last_trade_time == 0.0:
            return 0.0
        elapsed = time.time() - self._last_trade_time
        remaining = self._required_cooldown_unsafe() - elapsed
        return max(0.0, remaining)

    def can_trade(self) -> bool:
        with self._lock:
            return self._cooldown_remaining_unsafe() <= 0.0

    def get_cooldown_remaining(self) -> float:
        with self._lock:
            return self._cooldown_remaining_unsafe()

    def update_trade_pacing(self):
        with self._lock:
            self._last_trade_time = time.time()

    # ------------------------------------------------------------------
    # Trade history and performance
    # ------------------------------------------------------------------

    def add_trade(self, trade: TradeRecord):
        with self._lock:
            if len(self._trade_history) == self._trade_history.maxlen:
                old = self._trade_history.popleft()
                self._trades_by_id.pop(old.trade_id, None)

            self._trade_history.append(trade)
            self._trades_by_id[trade.trade_id] = trade

    def confirm_trade(
        self,
        trade_id: str,
        contract_id: int,
        payout: float,
        proposal_latency: float,
        buy_latency: float,
        prefetch_used: bool,
    ):
        with self._lock:
            trade = self._trades_by_id.get(trade_id)
            if trade is None:
                return

            trade.contract_id = contract_id
            trade.payout = payout
            trade.proposal_latency = proposal_latency
            trade.buy_latency = buy_latency
            trade.prefetch_used = prefetch_used

    def update_trade_outcome(
        self,
        trade_id: str,
        status: str,
        pnl: float,
        error_message: str = "",
        payout: Optional[float] = None,
        exit_reason: str = "",
        settlement_latency: float = -1.0,
    ):
        with self._lock:
            trade = self._trades_by_id.get(trade_id)
            if trade is None:
                return

            if trade.status in self.FINAL_TRADE_STATUSES:
                return

            trade.status = status
            trade.pnl = pnl
            trade.settlement_latency = settlement_latency

            if error_message:
                trade.error_message = error_message

            if payout is not None:
                trade.payout = payout

            if exit_reason:
                trade.exit_reason = exit_reason

            self._total_pnl += pnl
            self._session_pnl += pnl

            if status == "WON":
                self._wins += 1
                self._consecutive_losses = 0
            elif status == "LOST":
                self._losses += 1
                self._consecutive_losses += 1

    def get_trade_history(self) -> List[TradeRecord]:
        with self._lock:
            return list(reversed(self._trade_history))

    def get_performance_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._wins + self._losses
            win_rate = (self._wins / total * 100) if total > 0 else 0.0
            return {
                "total_trades": total,
                "wins": self._wins,
                "losses": self._losses,
                "win_rate": round(win_rate, 1),
                "total_pnl": round(self._total_pnl, 2),
                "session_pnl": round(self._session_pnl, 2),
                "current_stake": self._current_stake,
                "initial_stake": self._initial_stake,
                "martingale_step": self._current_martingale_step,
                "consecutive_losses": self._consecutive_losses,
                "cooldown_remaining": self._cooldown_remaining_unsafe(),
            }

    # ------------------------------------------------------------------
    # Execution context
    # ------------------------------------------------------------------

    def set_execution_context(
        self,
        account_id: str,
        account_type: str,
        currency: str,
        execution_mode: str,
    ):
        with self._lock:
            self._execution_context = {
                "account_id": str(account_id or ""),
                "account_type": str(account_type or "UNKNOWN").upper(),
                "currency": str(currency or "USD").upper(),
                "execution_mode": str(execution_mode or "UNCONFIGURED").upper(),
            }

    def get_execution_context(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._execution_context)

    # ------------------------------------------------------------------
    # Status messages
    # ------------------------------------------------------------------

    @property
    def status_message(self) -> str:
        with self._lock:
            return self._status_message

    def set_status(self, message: str):
        with self._lock:
            self._status_message = message

    @property
    def error_message(self) -> str:
        with self._lock:
            return self._error_message

    def set_error(self, message: str):
        with self._lock:
            self._error_message = message

    def clear_error(self):
        with self._lock:
            self._error_message = ""

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @staticmethod
    def _increment_reason_unsafe(counter: Dict[str, int], reason: str) -> None:
        reason = str(reason or "unknown").strip()[:120]
        if not reason:
            reason = "unknown"

        if len(counter) > 50 and reason not in counter:
            return

        counter[reason] = counter.get(reason, 0) + 1

    def record_signal_generated(self):
        with self._lock:
            self._signals_generated += 1
            self._last_signal_time = time.time()

    def record_signal_executed(self):
        with self._lock:
            self._signals_executed += 1

    def record_signal_skipped_gate(self, reason: str):
        with self._lock:
            self._signals_skipped_gate += 1
            self._last_skip_reason = str(reason or "unknown")
            self._increment_reason_unsafe(self._skip_reason_counts, reason)

    def record_signal_rejected_strategy(self, reason: str):
        with self._lock:
            self._signals_rejected_strategy += 1
            self._last_reject_reason = str(reason or "unknown")
            self._increment_reason_unsafe(self._reject_reason_counts, reason)

    def record_prefetch_hit(self):
        with self._lock:
            self._prefetch_hits += 1

    def record_prefetch_miss(self):
        with self._lock:
            self._prefetch_misses += 1

    def record_reconnect(self):
        with self._lock:
            self._reconnects += 1

    def record_latency(self, kind: str, seconds: float):
        with self._lock:
            bucket = self._latency_stats.get(kind)
            if bucket is None or seconds is None or seconds < 0:
                return
            bucket["last"] = float(seconds)
            bucket["sum"] += float(seconds)
            bucket["count"] += 1

    def _latency_view_unsafe(self, kind: str) -> Dict[str, float]:
        bucket = self._latency_stats.get(kind, {"last": -1.0, "sum": 0.0, "count": 0})
        count = int(bucket.get("count", 0))
        avg = (bucket.get("sum", 0.0) / count) if count > 0 else -1.0
        return {
            "last": round(float(bucket.get("last", -1.0)), 3),
            "avg": round(float(avg), 3),
            "count": count,
        }

    def get_diagnostics(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            ticks_last_60s = sum(1 for t in self._tick_receipt_times if now - t <= 60.0)
            ticks_last_10s = sum(1 for t in self._tick_receipt_times if now - t <= 10.0)

            prefetch_total = self._prefetch_hits + self._prefetch_misses
            prefetch_hit_rate = (
                round(self._prefetch_hits / prefetch_total * 100.0, 1)
                if prefetch_total > 0
                else 0.0
            )

            return {
                "total_ticks_processed": self._total_ticks_processed,
                "tick_rate_per_min": ticks_last_60s,
                "tick_rate_per_sec_10s": round(ticks_last_10s / 10.0, 2),
                "signals_generated": self._signals_generated,
                "signals_executed": self._signals_executed,
                "signals_skipped_gate": self._signals_skipped_gate,
                "signals_rejected_strategy": self._signals_rejected_strategy,
                "last_signal_time": self._last_signal_time,
                "last_skip_reason": self._last_skip_reason,
                "last_reject_reason": self._last_reject_reason,
                "skip_reason_counts": dict(self._skip_reason_counts),
                "reject_reason_counts": dict(self._reject_reason_counts),
                "prefetch_hits": self._prefetch_hits,
                "prefetch_misses": self._prefetch_misses,
                "prefetch_hit_rate": prefetch_hit_rate,
                "reconnects": self._reconnects,
                "proposal_latency": self._latency_view_unsafe("proposal"),
                "buy_latency": self._latency_view_unsafe("buy"),
                "settlement_latency": self._latency_view_unsafe("settlement"),
            }

    # ------------------------------------------------------------------
    # Session reset
    # ------------------------------------------------------------------

    def reset_for_new_session(self, initial_stake: float):
        with self._lock:
            self._is_running = False
            self._stop_requested = False

            self._current_price = 0.0
            self._recent_ticks.clear()
            self._tick_timestamps.clear()
            self._tick_receipt_times.clear()
            self._total_ticks_processed = 0

            self._current_trend_direction = None
            self._trend_tick_count = 0
            self._trend_kind = None
            self._trades_in_current_trend = 0
            self._in_cooldown = False
            self._pattern_stage = "IDLE"
            self._pattern_ticks = []

            self._mtf_bias = None
            self._mtf_agreement = 0
            self._mtf_tf_biases = {}
            self._micro_bias = None
            self._last_entry_mode = None

            self._last_signal_score = 0
            self._last_signal_score_breakdown = {}

            self._current_martingale_step = 0
            self._initial_stake = initial_stake
            self._current_stake = initial_stake

            self._last_trade_time = 0.0
            self._session_pnl = 0.0
            self._consecutive_losses = 0

            self._trade_history = deque(maxlen=TRADE_HISTORY_LIMIT)
            self._trades_by_id = {}
            self._total_pnl = 0.0
            self._wins = 0
            self._losses = 0

            self._execution_context = {
                "account_id": "",
                "account_type": "UNKNOWN",
                "currency": "USD",
                "execution_mode": "UNCONFIGURED",
            }

            self._status_message = "Bot is stopped."
            self._error_message = ""

            self._signals_generated = 0
            self._signals_executed = 0
            self._signals_skipped_gate = 0
            self._signals_rejected_strategy = 0
            self._skip_reason_counts = {}
            self._reject_reason_counts = {}
            self._last_skip_reason = ""
            self._last_reject_reason = ""
            self._last_signal_time = None

            self._prefetch_hits = 0
            self._prefetch_misses = 0
            self._reconnects = 0
            self._latency_stats = self._new_latency_stats()