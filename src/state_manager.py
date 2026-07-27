"""
src/state_manager.py
--------------------
Thread-safe shared state container used by both the async trading engine
(running in a background thread) and the Streamlit UI (running in the main thread).

All mutable state is protected by a threading.Lock to prevent race conditions.
"""

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from config import TICK_BUFFER_SIZE, DEFAULT_INITIAL_STAKE


@dataclass
class TradeRecord:
    """Represents a single executed trade and its outcome."""
    trade_id: str
    direction: str          # "BUY" or "SELL"
    stake: float
    barrier: str
    entry_price: float
    timestamp: str
    status: str             # "OPEN", "WON", "LOST", "CANCELLED", "UNKNOWN", or "PREVIEW"
    pnl: float = 0.0
    contract_id: Optional[int] = None
    martingale_step: int = 0
    execution_mode: str = "UNSPECIFIED"  # "DEMO", "REAL", or "SIGNAL_ONLY"
    account_type: str = "UNKNOWN"
    error_message: str = ""


class StateManager:
    """
    Singleton-style shared state object.
    Provides thread-safe access to all runtime variables.
    """

    def __init__(self):
        self._lock = threading.Lock()

        # Bot control
        self._is_running: bool = False
        self._stop_requested: bool = False

        # Market data
        self._current_price: float = 0.0
        self._recent_ticks: deque = deque(maxlen=TICK_BUFFER_SIZE)
        self._tick_timestamps: deque = deque(maxlen=TICK_BUFFER_SIZE)
        # Monotonically increasing count of every tick ever processed by the
        # engine, independent of the bounded recent-ticks buffer above. Lets
        # the UI show "is the engine actually alive" (count still going up)
        # separately from "is there a trade setup" (pattern_stage).
        self._total_ticks_processed: int = 0

        # Strategy state
        self._current_trend_direction: Optional[str] = None  # "UP" or "DOWN"
        self._trend_tick_count: int = 0
        self._trend_kind: Optional[str] = None  # "burst" or "classic"
        self._trades_in_current_trend: int = 0
        self._in_cooldown: bool = False
        self._pattern_stage: str = "IDLE"   # IDLE, PULLBACK, MOMENTUM, SIGNAL
        self._pattern_ticks: List[float] = []

        # MTF state
        self._mtf_bias: Optional[str] = None  # "UP", "DOWN", or None
        self._mtf_agreement: int = 0
        self._mtf_tf_biases: Dict[str, str] = {}  # Per-timeframe individual biases
        self._micro_bias: Optional[str] = None    # v4 tick-derived seconds-scale bias
        self._last_entry_mode: Optional[str] = None  # "immediate" or "pullback"

        # Signal scoring (v3)
        self._last_signal_score: int = 0
        self._last_signal_score_breakdown: Dict[str, int] = {}

        # Martingale state
        self._current_martingale_step: int = 0
        self._current_stake: float = DEFAULT_INITIAL_STAKE
        self._initial_stake: float = DEFAULT_INITIAL_STAKE

        # Sniper Pacing state
        self._last_trade_time: float = 0.0
        self._session_pnl: float = 0.0
        self._consecutive_losses: int = 0

        # Trade history and performance
        self._trade_history: List[TradeRecord] = []
        self._total_pnl: float = 0.0
        self._wins: int = 0
        self._losses: int = 0

        # Execution context shown in the UI. This is intentionally stored separately
        # from individual records so the dashboard can accurately describe the running
        # session even before the first signal arrives.
        self._execution_context: Dict[str, str] = {
            "account_id": "",
            "account_type": "UNKNOWN",
            "currency": "USD",
            "execution_mode": "UNCONFIGURED",
        }

        # Status messages
        self._status_message: str = "Bot is stopped."
        self._error_message: str = ""

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
        """Lightweight "is the engine actually alive" snapshot for the UI:
        total ticks ever processed (proves the engine loop is running) and
        the wall-clock time of the most recent tick (proves it's current,
        not just running against stale/frozen data). Cheap read-only call,
        meant for the dashboard's polling loop, not the tick hot path."""
        with self._lock:
            last_tick_time = self._tick_timestamps[-1] if self._tick_timestamps else None
            return {
                "total_ticks_processed": self._total_ticks_processed,
                "last_tick_time": last_tick_time,
            }

    # ------------------------------------------------------------------
    # Strategy state
    # ------------------------------------------------------------------
    # Precomputed once so update_strategy_state doesn't re-derive "_<key>"
    # and re-check hasattr() on every tick - same validity guard against
    # unknown keys, cheaper per call.
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
        """Caller must already hold self._lock."""
        for key, value in kwargs.items():
            attr = self._STRATEGY_STATE_ATTRS.get(key)
            if attr is not None:
                setattr(self, attr, value)

    def update_tick_and_strategy_state(
        self, price: float, timestamp: float, **strategy_kwargs
    ) -> None:
        """Combined market-tick + strategy-state update in a single lock
        acquisition. Used on the tick hot path (see TradingEngine._on_tick)
        instead of calling update_tick() and update_strategy_state()
        separately, which took the lock twice per tick for no benefit -
        both updates always happen together on that path anyway."""
        with self._lock:
            self._current_price = price
            self._recent_ticks.append(price)
            self._tick_timestamps.append(timestamp)
            self._total_ticks_processed += 1
            self._apply_strategy_state(strategy_kwargs)

    # ------------------------------------------------------------------
    # Martingale state
    # ------------------------------------------------------------------
    def get_martingale_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "step": self._current_martingale_step,
                "stake": self._current_stake,
                "initial_stake": self._initial_stake,
            }

    def set_initial_stake(self, stake: float):
        with self._lock:
            self._initial_stake = stake
            self._current_stake = stake

    def on_trade_win(self):
        """Reset Martingale to initial stake after a win."""
        with self._lock:
            self._current_martingale_step = 0
            self._current_stake = self._initial_stake

    def on_trade_loss(self, multiplier: float, max_steps: int):
        """Advance Martingale step on a loss, or reset if max steps exceeded.

        In FLAT mode the step counter still advances (useful context in the
        UI / trade history) but get_martingale_state() ignores it for stake
        sizing, so this is safe to call regardless of STAKE_MODE.
        """
        with self._lock:
            if self._current_martingale_step < max_steps:
                self._current_martingale_step += 1
                self._current_stake = round(self._current_stake * multiplier, 2)
            else:
                # Max steps reached: reset to initial stake
                self._current_martingale_step = 0
                self._current_stake = self._initial_stake

    def drawdown_limit_hit(self) -> bool:
        """No session drawdown limit — strict entry quality gates manage risk."""
        return False

    def can_trade(self) -> bool:
        """Check if enough time has passed since the last trade (sniper pacing).

        Enforces escalating cooldown after consecutive losses to let the market
        reset before the next entry. After a win the cooldown resets to base.
          - 0 consecutive losses: 30 seconds (patient but responsive)
          - 1 consecutive loss: 90 seconds (let the market cool)
          - 2+ consecutive losses: 180 seconds (full market reset)
        """
        import time
        with self._lock:
            if self._last_trade_time == 0.0:
                return True

            elapsed = time.time() - self._last_trade_time

            if self._consecutive_losses >= 2:
                required_cooldown = 180.0  # 3 minutes after 2+ losses
            elif self._consecutive_losses >= 1:
                required_cooldown = 90.0   # 90 seconds after 1 loss
            else:
                required_cooldown = 30.0   # 30 seconds base cooldown

            return elapsed >= required_cooldown

    def get_cooldown_remaining(self) -> float:
        """Return seconds remaining until next trade is allowed (0 if ready).

        Thread-safe: acquires its own lock so it can be called from outside
        even when other methods hold the lock internally (avoids re-entry
        deadlock since threading.Lock is not reentrant).
        """
        import time
        with self._lock:
            if self._last_trade_time == 0.0:
                return 0.0

            elapsed = time.time() - self._last_trade_time

            if self._consecutive_losses >= 2:
                required_cooldown = 180.0
            elif self._consecutive_losses >= 1:
                required_cooldown = 90.0
            else:
                required_cooldown = 30.0

            remaining = required_cooldown - elapsed
            return max(0.0, remaining)

    def _get_cooldown_remaining_unsafe(self) -> float:
        """Same as get_cooldown_remaining but assumes caller already holds _lock."""
        import time
        if self._last_trade_time == 0.0:
            return 0.0

        elapsed = time.time() - self._last_trade_time

        if self._consecutive_losses >= 2:
            required_cooldown = 180.0
        elif self._consecutive_losses >= 1:
            required_cooldown = 90.0
        else:
            required_cooldown = 30.0

        remaining = required_cooldown - elapsed
        return max(0.0, remaining)

    def update_trade_pacing(self):
        """Record the time a trade was executed."""
        import time
        with self._lock:
            self._last_trade_time = time.time()

    # ------------------------------------------------------------------
    # Trade history and performance
    # ------------------------------------------------------------------
    def add_trade(self, trade: TradeRecord):
        with self._lock:
            self._trade_history.append(trade)

    def update_trade_outcome(
        self, trade_id: str, status: str, pnl: float, error_message: str = ""
    ):
        with self._lock:
            for trade in self._trade_history:
                if trade.trade_id == trade_id:
                    trade.status = status
                    trade.pnl = pnl
                    if error_message:
                        trade.error_message = error_message
                    self._total_pnl += pnl
                    if status == "WON":
                        self._wins += 1
                        self._consecutive_losses = 0
                    elif status == "LOST":
                        self._losses += 1
                        self._consecutive_losses += 1
                    self._session_pnl += pnl
                    break

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
                "current_stake": self._current_stake,
                "martingale_step": self._current_martingale_step,
                "consecutive_losses": self._consecutive_losses,
                "cooldown_remaining": self._get_cooldown_remaining_unsafe(),
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
        """Store the selected account and execution policy for UI display."""
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

    def reset_for_new_session(self, initial_stake: float):
        """Full reset when starting a new bot session."""
        with self._lock:
            self._is_running = False
            self._stop_requested = False
            self._current_price = 0.0
            self._recent_ticks.clear()
            self._tick_timestamps.clear()
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
            self._trade_history = []
            self._total_pnl = 0.0
            self._wins = 0
            self._losses = 0
            self._session_pnl = 0.0
            self._last_trade_time = 0.0
            self._consecutive_losses = 0
            self._execution_context = {
                "account_id": "",
                "account_type": "UNKNOWN",
                "currency": "USD",
                "execution_mode": "UNCONFIGURED",
            }
            self._status_message = "Bot is stopped."
            self._error_message = ""
