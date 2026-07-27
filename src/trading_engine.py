"""
src/trading_engine.py
Hardened async trading engine.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from config import (
    BARRIER_BUY,
    BARRIER_SELL,
    CONTRACT_DURATION,
    CONTRACT_DURATION_UNIT,
    CONTRACT_TYPE_BUY,
    CONTRACT_TYPE_SELL,
    CURRENCY,
    DEFAULT_STRATEGY_SENSITIVITY,
    ENTRY_SCORE_THRESHOLD,
    MARTINGALE_MULTIPLIER,
    MTF_CANDLE_COUNT,
    MTF_GRANULARITIES,
    STRATEGY_SENSITIVITY_PRESETS,
    SYMBOL,
)
from src.api_client import DerivAPIClient, DerivAPIError
from src.logger import get_logger
from src.state_manager import StateManager, TradeRecord
from src.strategy import MTFAnalyzer, StrategyEngine

logger = get_logger("trading_engine")


@dataclass(frozen=True)
class _PrefetchedProposal:
    proposal_id: str
    ask_price: float
    signal: str
    contract_type: str
    stake: float
    barrier: str
    created_at: float

    def matches(
        self,
        signal: str,
        contract_type: str,
        stake: float,
        barrier: str,
        max_age: float,
    ) -> bool:
        return (
            self.signal == signal
            and self.contract_type == contract_type
            and abs(self.stake - stake) < 1e-9
            and self.barrier == barrier
            and time.monotonic() - self.created_at <= max_age
        )


MTF_REFRESH_INTERVAL = 30
PREFETCH_MAX_AGE_SECONDS = 2.5

# Fast but safe execution gate.
INITIAL_WARMUP_COOLDOWN_SECONDS = 15.0
POST_COOLDOWN_FRESH_SIGNAL_GRACE_SECONDS = 3.0

_DEMO_ACCOUNT_TYPES = {"DEMO", "VIRTUAL", "PRACTICE", "VIRTUAL_ACCOUNT"}
_REAL_ACCOUNT_TYPES = {"REAL", "LIVE", "REAL_MONEY"}


def normalize_account_type(account_type: str) -> str:
    normalized = " ".join(
        str(account_type or "")
        .strip()
        .upper()
        .replace("-", " ")
        .split()
    )

    if normalized in _DEMO_ACCOUNT_TYPES:
        return "DEMO"

    if normalized in _REAL_ACCOUNT_TYPES:
        return "REAL"

    return normalized or "UNKNOWN"


def resolve_execution_mode(account_type: str, real_execution_confirmed: bool) -> str:
    normalized_type = normalize_account_type(account_type)

    if normalized_type == "DEMO":
        return "DEMO"

    if normalized_type == "REAL" and real_execution_confirmed:
        return "REAL"

    return "BLOCKED"


class TradingEngine:
    def __init__(
        self,
        api_token: str,
        app_id: str,
        account_id: str,
        account_currency: str,
        state: StateManager,
        initial_stake: float,
        max_martingale_steps: int,
        barrier_buy: str = BARRIER_BUY,
        barrier_sell: str = BARRIER_SELL,
        strategy_sensitivity: str = DEFAULT_STRATEGY_SENSITIVITY,
        account_type: str = "UNKNOWN",
        real_execution_confirmed: bool = False,
        martingale_multiplier: float = MARTINGALE_MULTIPLIER,
    ):
        self.api_token = api_token.strip()
        self.app_id = app_id.strip()
        self.account_id = account_id.strip()
        self.account_currency = (account_currency or CURRENCY).upper()
        self.account_type = normalize_account_type(account_type)
        self.real_execution_confirmed = bool(real_execution_confirmed)
        self.execution_mode = resolve_execution_mode(
            self.account_type,
            self.real_execution_confirmed,
        )

        self.state = state
        self.initial_stake = initial_stake
        self.max_martingale_steps = max_martingale_steps
        self.martingale_multiplier = martingale_multiplier
        self.barrier_buy = barrier_buy
        self.barrier_sell = barrier_sell

        preset = STRATEGY_SENSITIVITY_PRESETS.get(
            strategy_sensitivity,
            STRATEGY_SENSITIVITY_PRESETS[DEFAULT_STRATEGY_SENSITIVITY],
        )

        self._client: Optional[DerivAPIClient] = None
        self._strategy = StrategyEngine(
            velocity_threshold=preset["velocity_threshold"],
            burst_threshold=preset["burst_threshold"],
            mtf_min_agreement=preset["mtf_min_agreement"],
            entry_score_threshold=preset.get("entry_score_threshold", ENTRY_SCORE_THRESHOLD),
        )

        self._signal_monotonic = 0.0
        self._mtf_analyzer = MTFAnalyzer(min_agreement=preset["mtf_min_agreement"])
        self._last_mtf_refresh = 0.0

        self._active_contract_id: Optional[int] = None
        self._trade_in_progress = False
        self._prefetched_proposal: Optional[_PrefetchedProposal] = None
        self._pre_fetch_task: Optional[asyncio.Task] = None
        self._reconnect_lock = asyncio.Lock()

        self._engine_ready_monotonic = 0.0
        self._gate_ready_monotonic: Optional[float] = None
        self._last_signal_monotonic = 0.0
        self._last_strategy_state_version = 0

    async def run(self):
        logger.info(
            "Trading engine starting with execution mode %s for %s account.",
            self.execution_mode,
            self.account_type,
        )

        self.state.set_execution_context(
            account_id=self.account_id,
            account_type=self.account_type,
            currency=self.account_currency,
            execution_mode=self.execution_mode,
        )

        if self.execution_mode == "BLOCKED":
            self.state.set_error(
                "Order execution is blocked: the selected account is real without an exact LIVE "
                "confirmation, or its account type is unknown."
            )
            self.state.set_status("Signal monitoring is active, but no orders can be sent.")
        else:
            self.state.set_status(
                f"Connecting to Deriv API for {self.execution_mode.lower()} order execution..."
            )

        self._client = DerivAPIClient(self.api_token, self.app_id, self.account_id)
        connected = await self._client.connect()

        if not connected:
            detail = self._client.last_error if self._client else ""
            msg = (
                f"Failed to connect to Deriv API. "
                f"{detail or 'Check your App ID, PAT scopes, and internet connection.'}"
            )
            logger.error(msg)
            self.state.set_error(msg)
            self.state.set_status("Connection failed.")
            self.state.set_running(False)
            return

        self._pre_fetch_task = asyncio.create_task(
            self._pre_fetch_loop(),
            name="deriv-proposal-prefetch",
        )

        self.state.set_status("Connected. Fetching initial MTF data...")
        logger.info("Connected to Deriv API. Fetching initial MTF data...")

        await self._refresh_mtf()

        self._engine_ready_monotonic = time.monotonic()
        self._gate_ready_monotonic = None
        self._last_signal_monotonic = 0.0
        self._last_strategy_state_version = self._strategy.state_version

        self.state.set_status(
            f"Subscribed to tick stream. Warming up for "
            f"{INITIAL_WARMUP_COOLDOWN_SECONDS:.0f}s before first trade..."
        )

        logger.info("Subscribing to tick stream for %s...", SYMBOL)

        try:
            await self._client.subscribe_ticks(SYMBOL, self._on_tick)
            logger.info("Tick subscription active. Entering main loop.")

            while not self.state.stop_requested:
                await asyncio.sleep(1)

                if not self._client.connected:
                    await self._reconnect()

                strategy_state = self._strategy.get_state()
                active_setup = strategy_state.get("pattern_stage") in (
                    "TREND",
                    "PULLBACK",
                    "MOMENTUM",
                )

                current_refresh_interval = 10.0 if active_setup else MTF_REFRESH_INTERVAL

                if time.time() - self._last_mtf_refresh > current_refresh_interval:
                    await self._refresh_mtf()

        except DerivAPIError as e:
            logger.error("API error in main loop: %s", e)
            self.state.set_error(str(e))
        except Exception as e:
            logger.exception("Unexpected error in main loop: %s", e)
            self.state.set_error(f"Unexpected error: {e}")
        finally:
            await self._shutdown()

    async def _reconnect(self, max_attempts: int = 5) -> bool:
        async with self._reconnect_lock:
            if self._client.connected:
                return True

            for attempt in range(1, max_attempts + 1):
                self.state.set_status(f"Connection lost. Reconnecting ({attempt}/{max_attempts})...")
                logger.warning("Reconnecting to Deriv (attempt %d/%d)...", attempt, max_attempts)

                try:
                    await self._client.disconnect(cancel_callbacks=False)
                except Exception:
                    logger.debug("Error while clearing the old Deriv socket.", exc_info=True)

                try:
                    if await self._client.connect():
                        self._prefetched_proposal = None

                        if not self._trade_in_progress:
                            self._strategy.reset()
                            self._engine_ready_monotonic = time.monotonic()

                        self._gate_ready_monotonic = None
                        self._last_signal_monotonic = 0.0

                        await self._client.subscribe_ticks(SYMBOL, self._on_tick)

                        self._last_mtf_refresh = 0.0
                        self._last_strategy_state_version = self._strategy.state_version

                        self.state.set_status("Reconnected to Deriv. Bot is active.")
                        logger.info("Reconnected to Deriv successfully.")
                        return True

                    logger.warning(
                        "Reconnect attempt %d failed: %s",
                        attempt,
                        self._client.last_error or "connection was not established",
                    )
                except DerivAPIError as exc:
                    logger.warning("Reconnect attempt %d failed: %s", attempt, exc)

                if attempt < max_attempts:
                    await asyncio.sleep(min(2**attempt, 15))

            self.state.set_error("Could not reconnect to Deriv after repeated attempts.")
            logger.error("Reconnect failed after %d attempts.", max_attempts)
            return False

    async def _shutdown(self):
        logger.info("Trading engine shutting down...")

        self._prefetched_proposal = None

        if self._pre_fetch_task and not self._pre_fetch_task.done():
            self._pre_fetch_task.cancel()
            await asyncio.gather(self._pre_fetch_task, return_exceptions=True)

        self._pre_fetch_task = None

        if self._trade_in_progress:
            self.state.set_status(
                "Stop requested. Finishing the active order/contract check before disconnecting..."
            )

            deadline = time.monotonic() + 90.0

            while self._trade_in_progress and time.monotonic() < deadline:
                await asyncio.sleep(0.1)

            if self._trade_in_progress:
                logger.critical(
                    "Shutdown safety wait expired while an order or contract was still active."
                )
                self.state.set_error(
                    "Shutdown could not confirm the active order/contract. Check the Deriv statement."
                )

        if self._client:
            await self._client.unsubscribe_ticks()
            await self._client.disconnect()

        self.state.set_running(False)
        self.state.set_status("Bot stopped.")
        logger.info("Trading engine stopped.")

    def _update_trade_gate(self, now_mono: float) -> None:
        if self._engine_ready_monotonic == 0.0:
            self._gate_ready_monotonic = None
            return

        warmup_active = (now_mono - self._engine_ready_monotonic) < INITIAL_WARMUP_COOLDOWN_SECONDS
        cooldown_remaining = self.state.get_cooldown_remaining()

        if warmup_active or cooldown_remaining > 0:
            self._gate_ready_monotonic = None
        elif self._gate_ready_monotonic is None:
            self._gate_ready_monotonic = now_mono
            logger.info("Trade gate open: waiting for a fresh signal before executing.")

    def _trade_gate_allows_trade(self, now_mono: float) -> Tuple[bool, str]:
        if self._engine_ready_monotonic == 0.0:
            return False, "engine not ready"

        warmup_remaining = INITIAL_WARMUP_COOLDOWN_SECONDS - (now_mono - self._engine_ready_monotonic)
        if warmup_remaining > 0:
            return False, f"initial warm-up ({warmup_remaining:.0f}s remaining)"

        cooldown_remaining = self.state.get_cooldown_remaining()
        if cooldown_remaining > 0:
            return False, f"sniper pacing ({cooldown_remaining:.0f}s remaining)"

        if self._gate_ready_monotonic is None:
            self._gate_ready_monotonic = now_mono

        if self._last_signal_monotonic <= 0.0:
            return False, "waiting for first fresh signal"

        if self._last_signal_monotonic < self._gate_ready_monotonic:
            return False, "waiting for a fresh signal after cooldown/warm-up"

        grace_deadline = self._gate_ready_monotonic + POST_COOLDOWN_FRESH_SIGNAL_GRACE_SECONDS
        if self._last_signal_monotonic < grace_deadline:
            remaining_grace = grace_deadline - self._last_signal_monotonic
            return False, f"waiting for a fresh post-cooldown signal ({remaining_grace:.1f}s)"

        return True, "signal allowed"

    async def _on_tick(self, tick_data: Dict[str, Any]):
        try:
            price = float(tick_data.get("quote", 0))
            epoch = float(tick_data.get("epoch", time.time()))

            if price == 0:
                return

            if self._trade_in_progress:
                try:
                    self._strategy.process_tick(price)
                except Exception:
                    logger.exception(
                        "Strategy processing failed during active trade; tick feed continues."
                    )

                self._push_tick_and_strategy_state(price, epoch)
                return

            now_mono = time.monotonic()
            self._update_trade_gate(now_mono)

            try:
                signal = self._strategy.process_tick(price)
            except Exception:
                logger.exception("Strategy processing failed; tick will still be pushed to the UI.")
                signal = None

            if signal in ("BUY", "SELL"):
                self._last_signal_monotonic = now_mono
                allowed, gate_reason = self._trade_gate_allows_trade(now_mono)

                if allowed:
                    self._signal_monotonic = now_mono
                    self._gate_ready_monotonic = None
                    self._last_signal_monotonic = 0.0

                    logger.info("Signal received: %s at price %s", signal, price)
                    self.state.set_status(f"Signal: {signal} at {price:.4f}. Placing trade...")

                    await self._execute_trade(signal, price)
                    return

                logger.info("Signal %s at %s seen but not executed: %s", signal, price, gate_reason)
                self.state.set_status(f"Signal {signal} seen, waiting: {gate_reason}")
                self._strategy.on_signal_skipped()

            self._push_tick_and_strategy_state(price, epoch)

        except Exception as e:
            logger.exception("Error processing tick: %s", e)

    def _push_tick_and_strategy_state(self, price: float, epoch: float) -> None:
        try:
            current_version = self._strategy.state_version

            if current_version != self._last_strategy_state_version:
                strategy_state = self._strategy.get_state()
                self._last_strategy_state_version = current_version

                self.state.update_tick_and_strategy_state(
                    price,
                    epoch,
                    trend_direction=strategy_state["trend_direction"],
                    trend_tick_count=strategy_state["trend_tick_count"],
                    trend_kind=strategy_state["trend_kind"],
                    trades_in_trend=strategy_state["trades_in_trend"],
                    in_cooldown=strategy_state["in_cooldown"],
                    pattern_stage=strategy_state["pattern_stage"],
                    mtf_bias=strategy_state["mtf_bias"],
                    mtf_agreement=strategy_state["mtf_agreement"],
                    mtf_tf_biases=strategy_state.get("mtf_tf_biases", {}),
                    micro_bias=strategy_state.get("micro_bias"),
                    last_entry_mode=strategy_state.get("last_entry_mode"),
                    last_signal_score=strategy_state["last_signal_score"],
                    last_signal_score_breakdown=strategy_state["last_signal_score_breakdown"],
                )
            else:
                self.state.update_tick(price, epoch)

        except Exception:
            logger.exception("Strategy state push failed; updating market tick only.")
            self.state.update_tick(price, epoch)

    async def _refresh_mtf(self):
        logger.info("Refreshing MTF data...")
        self.state.set_status("Refreshing multi-timeframe data...")

        candles_by_tf = {}

        try:
            for tf_label, granularity in MTF_GRANULARITIES.items():
                candles = await self._client.get_candles(
                    SYMBOL,
                    granularity,
                    MTF_CANDLE_COUNT,
                )
                candles_by_tf[tf_label] = candles
                logger.debug("MTF %s: %s candles fetched.", tf_label, len(candles))

            bias, agreement, tf_biases = self._mtf_analyzer.analyze_with_strength(candles_by_tf)

            self._strategy.update_mtf_bias(bias, agreement, tf_biases=tf_biases)
            self.state.update_strategy_state(
                mtf_bias=bias,
                mtf_agreement=agreement,
                mtf_tf_biases=tf_biases,
            )

            self._last_mtf_refresh = time.time()
            self._last_strategy_state_version = self._strategy.state_version

            bias_str = f"{bias} ({agreement}/3)" if bias else "No consensus"

            logger.info("MTF analysis complete. Bias: %s | TF: %s", bias_str, tf_biases)
            self.state.set_status(f"Bot active | MTF Bias: {bias_str}")

        except DerivAPIError as e:
            logger.warning("MTF refresh failed: %s", e)
            self.state.set_status("MTF refresh failed. Using last known bias.")
        except Exception as e:
            logger.exception("Unexpected error during MTF refresh: %s", e)

    async def _execute_trade(self, signal: str, entry_price: float):
        if self._trade_in_progress:
            logger.warning("Trade already in progress. Skipping signal.")
            return

        self._trade_in_progress = True

        martingale_state = self.state.get_martingale_state()
        stake = martingale_state["stake"]
        martingale_step = martingale_state["step"]

        contract_type = CONTRACT_TYPE_BUY if signal == "BUY" else CONTRACT_TYPE_SELL
        barrier = self.barrier_buy if signal == "BUY" else self.barrier_sell

        trade_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        logger.info(
            "Handling %s signal | Mode=%s | Stake=%s | Barrier=%s | Step=%s | TradeID=%s",
            signal,
            self.execution_mode,
            stake,
            barrier,
            martingale_step,
            trade_id,
        )

        if self.execution_mode == "BLOCKED":
            reason = (
                "Order blocked: select a recognised DEMO account, or type LIVE exactly "
                "to enable orders on a REAL account. No proposal or buy request was sent."
            )

            self.state.add_trade(
                TradeRecord(
                    trade_id=trade_id,
                    direction=signal,
                    stake=stake,
                    barrier=barrier,
                    entry_price=entry_price,
                    timestamp=timestamp,
                    status="CANCELLED",
                    martingale_step=martingale_step,
                    execution_mode="BLOCKED",
                    account_type=self.account_type,
                    error_message=reason,
                )
            )

            self.state.set_error(reason)
            self.state.set_status(f"Signal: {signal} at {entry_price:.4f}. Order blocked by safety gate.")
            logger.warning("Blocked order signal TradeID=%s: %s", trade_id, reason)

            self._trade_in_progress = False
            return

        trade_record = TradeRecord(
            trade_id=trade_id,
            direction=signal,
            stake=stake,
            barrier=barrier,
            entry_price=entry_price,
            timestamp=timestamp,
            status="OPEN",
            martingale_step=martingale_step,
            execution_mode=self.execution_mode,
            account_type=self.account_type,
        )

        self.state.add_trade(trade_record)
        self.state.clear_error()

        execution_stage = "proposal request"

        if not self._client.connected:
            logger.warning("Connection was down when signal fired. Reconnecting before placing the order...")

            if not await self._reconnect():
                reason = "Order not sent: the Deriv connection could not be restored before the proposal request."
                self.state.update_trade_outcome(trade_id, "CANCELLED", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status("Order cancelled: could not reach Deriv before requesting a proposal.")
                self._trade_in_progress = False
                return

        try:
            self.state.set_status(
                f"{self.execution_mode} order: preparing to buy {signal} "
                f"(stake {stake:.2f} {self.account_currency})..."
            )

            cached = self._prefetched_proposal
            self._prefetched_proposal = None

            if cached and cached.matches(
                signal=signal,
                contract_type=contract_type,
                stake=stake,
                barrier=barrier,
                max_age=PREFETCH_MAX_AGE_SECONDS,
            ):
                proposal_id = cached.proposal_id
                ask_price = cached.ask_price

                logger.info(
                    "Using fresh matching pre-fetched proposal %s for TradeID=%s.",
                    proposal_id,
                    trade_id,
                )
            else:
                if cached:
                    logger.info(
                        "Discarded stale or mismatched pre-fetched proposal %s for TradeID=%s.",
                        cached.proposal_id,
                        trade_id,
                    )

                proposal = None
                max_retries = 2

                for attempt in range(max_retries + 1):
                    try:
                        logger.info(
                            "Requesting Deriv proposal (attempt %d) for TradeID=%s.",
                            attempt + 1,
                            trade_id,
                        )

                        proposal = await self._client.get_proposal(
                            symbol=SYMBOL,
                            contract_type=contract_type,
                            stake=stake,
                            duration=CONTRACT_DURATION,
                            duration_unit=CONTRACT_DURATION_UNIT,
                            barrier=barrier,
                            currency=self.account_currency,
                        )
                        break
                    except DerivAPIError as exc:
                        if exc.code == "TIMEOUT" and attempt < max_retries:
                            logger.warning("Proposal timeout on attempt %d; retrying immediately.", attempt + 1)
                            continue
                        raise

                if not proposal:
                    raise DerivAPIError("Failed to get a valid proposal after retries.", "RETRY_EXHAUSTED")

                proposal_id = proposal.get("id")
                if not isinstance(proposal_id, str) or not proposal_id:
                    raise DerivAPIError("Deriv did not return a valid proposal ID.", "INVALID_RESPONSE")

                try:
                    ask_price = float(proposal["ask_price"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise DerivAPIError(
                        "Deriv did not return a valid proposal ask price.",
                        "INVALID_RESPONSE",
                    ) from exc

            if ask_price <= 0:
                raise DerivAPIError("Deriv returned a non-positive proposal ask price.", "INVALID_RESPONSE")

            if self.state.stop_requested:
                raise DerivAPIError("Stop was requested before the buy; no order was submitted.", "STOP_REQUESTED")

            execution_stage = "buy request"

            self.state.set_status(f"Proposal received. Submitting {self.execution_mode.lower()} buy request...")

            logger.info(
                "Proposal %s received for TradeID=%s; submitting buy request.",
                proposal_id,
                trade_id,
            )

            try:
                buy_response = await self._client.buy_contract(
                    proposal_id=proposal_id,
                    price=ask_price,
                )
            except DerivAPIError as buy_exc:
                if buy_exc.code in ("TIMEOUT", "CONNECTION_LOST", "INVALID_RESPONSE"):
                    buy_response = await self._reconcile_after_buy_timeout(
                        stake=stake,
                        contract_type=contract_type,
                        signal_time=time.time(),
                    )

                    if buy_response is None:
                        raise DerivAPIError(
                            "The buy request may have reached Deriv, but no receipt "
                            "or matching open contract could be confirmed. "
                            "Check the Deriv statement before restarting.",
                            "AMBIGUOUS_BUY",
                        ) from buy_exc
                else:
                    raise

            contract_id = buy_response.get("contract_id")
            buy_price = float(buy_response.get("buy_price", stake))
            payout = float(buy_response.get("payout", 0.0))

            if not contract_id:
                raise DerivAPIError("No contract ID in buy response.", "NO_CONTRACT_ID")

            self._active_contract_id = contract_id
            trade_record.contract_id = contract_id

            self.state.update_trade_pacing()
            self._strategy.on_trade_executed()

            fill_latency = (
                time.monotonic() - self._signal_monotonic
                if self._signal_monotonic > 0
                else -1.0
            )

            logger.info(
                "Contract %s bought | Buy Price=%s | Payout=%s | TradeID=%s | Signal-to-fill latency: %.3fs",
                contract_id,
                buy_price,
                payout,
                trade_id,
                fill_latency,
            )

            self.state.set_status(
                f"{self.execution_mode} contract {contract_id} active | Waiting for Deriv settlement..."
            )

            outcome, pnl = await self._monitor_contract(contract_id, buy_price, payout)

            if outcome == "UNKNOWN":
                reason = (
                    f"Contract {contract_id} was bought but its settlement was not "
                    "confirmed within the monitoring window. "
                    "Check the Deriv account statement; Martingale was not changed."
                )

                self.state.update_trade_outcome(trade_id, "UNKNOWN", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status(
                    f"Trading stopped: contract {contract_id} outcome is unresolved; check Deriv before restarting."
                )
                self.state.request_stop()

                logger.warning("Trade outcome unresolved | TradeID=%s | Contract=%s", trade_id, contract_id)
            else:
                self.state.update_trade_outcome(trade_id, outcome, pnl)

                if outcome == "WON":
                    logger.info("Trade WON | PnL=%.2f | TradeID=%s", pnl, trade_id)
                    self.state.on_trade_win()
                    self.state.set_status(f"Trade WON! P&L: +{pnl:.2f}")
                else:
                    logger.info("Trade LOST | PnL=%.2f | TradeID=%s", pnl, trade_id)
                    self.state.on_trade_loss(self.martingale_multiplier, self.max_martingale_steps)

                    new_state = self.state.get_martingale_state()
                    self.state.set_status(
                        f"Trade LOST. Next stake: {new_state['stake']:.2f} (Step {new_state['step']})"
                    )

        except DerivAPIError as e:
            if e.code == "AMBIGUOUS_BUY":
                reason = e.message

                logger.critical("Ambiguous buy outcome for TradeID=%s: %s", trade_id, e)

                self.state.update_trade_outcome(trade_id, "UNKNOWN", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status(
                    "Trading stopped: a buy outcome is ambiguous. Check the Deriv statement before restarting."
                )
                self.state.request_stop()

            elif self._active_contract_id is not None:
                reason = (
                    f"Contract {self._active_contract_id} was bought, but monitoring failed: "
                    f"{e.message} ({e.code}). Check the Deriv statement; Martingale was not changed."
                )

                logger.critical(
                    "Post-buy API failure for TradeID=%s, Contract=%s: %s",
                    trade_id,
                    self._active_contract_id,
                    e,
                )

                self.state.update_trade_outcome(trade_id, "UNKNOWN", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status("Trading stopped: a purchased contract outcome is unresolved.")
                self.state.request_stop()

            else:
                reason = f"Deriv rejected the {execution_stage}: {e.message} ({e.code})."

                logger.error("API error during %s for TradeID=%s: %s", execution_stage, trade_id, e)

                self.state.update_trade_outcome(trade_id, "CANCELLED", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status(f"Order cancelled during {execution_stage}: {e.message}")

        except asyncio.CancelledError:
            if self._active_contract_id is not None:
                reason = (
                    f"Monitoring for purchased contract {self._active_contract_id} was interrupted. "
                    "Check the Deriv statement; Martingale was not changed."
                )

                self.state.update_trade_outcome(trade_id, "UNKNOWN", 0.0, reason)
                self.state.set_error(reason)
                self.state.request_stop()
            else:
                self.state.update_trade_outcome(
                    trade_id,
                    "CANCELLED",
                    0.0,
                    "Order attempt was stopped before any contract was confirmed.",
                )

            raise

        except Exception as e:
            if self._active_contract_id is not None:
                reason = (
                    f"Unexpected monitoring failure for purchased contract "
                    f"{self._active_contract_id}: {e}. "
                    "Check the Deriv statement; Martingale was not changed."
                )

                logger.exception(
                    "Unexpected post-buy error for TradeID=%s, Contract=%s",
                    trade_id,
                    self._active_contract_id,
                )

                self.state.update_trade_outcome(trade_id, "UNKNOWN", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status("Trading stopped: a purchased contract outcome is unresolved.")
                self.state.request_stop()

            else:
                reason = f"Unexpected failure during the {execution_stage}: {e}"

                logger.exception("Unexpected error during %s for TradeID=%s: %s", execution_stage, trade_id, e)

                self.state.update_trade_outcome(trade_id, "CANCELLED", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status(f"Order cancelled during {execution_stage}; see error details.")

        finally:
            self._active_contract_id = None
            self._trade_in_progress = False
            self._prefetched_proposal = None

    async def _reconcile_after_buy_timeout(
        self,
        stake: float,
        contract_type: str,
        signal_time: float,
    ) -> Optional[Dict[str, Any]]:
        logger.warning(
            "Buy receipt was not confirmed. Checking the live portfolio for a matching untracked fill before stopping."
        )

        known_ids = {
            str(trade.contract_id)
            for trade in self.state.get_trade_history()
            if getattr(trade, "contract_id", None) is not None
        }

        for attempt in range(1, 4):
            if attempt > 1:
                await asyncio.sleep(1.0)

            if not self._client.connected:
                logger.warning("Reconnecting before portfolio reconciliation (%d/3).", attempt)

                if not await self._reconnect(max_attempts=2):
                    continue

            try:
                contracts = await self._client.get_portfolio()
            except DerivAPIError as exc:
                logger.warning("Portfolio reconciliation attempt %d/3 failed: %s", attempt, exc)
                continue

            matches = []

            for contract in contracts:
                contract_id = contract.get("contract_id")

                if isinstance(contract_id, bool) or not isinstance(contract_id, int):
                    continue

                if str(contract_id) in known_ids:
                    continue

                if contract.get("underlying_symbol") != SYMBOL:
                    continue

                if contract.get("contract_type") != contract_type:
                    continue

                try:
                    buy_price = float(contract["buy_price"])
                    purchase_time = float(contract["purchase_time"])
                    payout = float(contract.get("payout", 0.0))
                except (KeyError, TypeError, ValueError):
                    continue

                if abs(buy_price - stake) > max(0.01, stake * 0.05):
                    continue

                if not signal_time - 15.0 <= purchase_time <= time.time() + 5.0:
                    continue

                matches.append(
                    {
                        "contract_id": contract_id,
                        "buy_price": buy_price,
                        "payout": payout,
                    }
                )

            if len(matches) == 1:
                match = matches[0]

                logger.warning(
                    "Recovered untracked contract %s after the missing buy receipt.",
                    match["contract_id"],
                )

                return match

            if len(matches) > 1:
                logger.error(
                    "Portfolio reconciliation found multiple possible fills; automatic adoption would be unsafe."
                )
                return None

        logger.error(
            "No unique matching contract was found. The buy outcome remains ambiguous and trading must stay stopped "
            "until the account statement is checked."
        )

        return None

    async def _pre_fetch_loop(self):
        try:
            while not self.state.stop_requested:
                if self.execution_mode == "BLOCKED" or self._trade_in_progress:
                    self._prefetched_proposal = None
                    await asyncio.sleep(0.5)
                    continue

                strategy_state = self._strategy.get_state()
                stage = strategy_state.get("pattern_stage")
                trend = strategy_state.get("trend_direction")

                if stage not in ("TREND", "PULLBACK", "MOMENTUM") or trend not in ("UP", "DOWN"):
                    self._prefetched_proposal = None
                    await asyncio.sleep(0.2)
                    continue

                signal = "BUY" if trend == "UP" else "SELL"
                contract_type = CONTRACT_TYPE_BUY if signal == "BUY" else CONTRACT_TYPE_SELL
                barrier = self.barrier_buy if signal == "BUY" else self.barrier_sell
                stake = float(self.state.get_martingale_state()["stake"])

                cached = self._prefetched_proposal

                if cached and cached.matches(
                    signal=signal,
                    contract_type=contract_type,
                    stake=stake,
                    barrier=barrier,
                    max_age=PREFETCH_MAX_AGE_SECONDS,
                ):
                    await asyncio.sleep(0.25)
                    continue

                self._prefetched_proposal = None

                if not self._client.connected:
                    await asyncio.sleep(0.5)
                    continue

                try:
                    proposal = await self._client.get_proposal(
                        symbol=SYMBOL,
                        contract_type=contract_type,
                        stake=stake,
                        duration=CONTRACT_DURATION,
                        duration_unit=CONTRACT_DURATION_UNIT,
                        barrier=barrier,
                        currency=self.account_currency,
                    )

                    proposal_id = proposal.get("id")
                    ask_price = float(proposal["ask_price"])

                    if not isinstance(proposal_id, str) or not proposal_id or ask_price <= 0:
                        raise DerivAPIError("Prefetch returned an invalid proposal.", "INVALID_RESPONSE")

                    latest_state = self._strategy.get_state()
                    latest_stake = float(self.state.get_martingale_state()["stake"])

                    if (
                        not self._trade_in_progress
                        and latest_state.get("pattern_stage") in ("TREND", "PULLBACK", "MOMENTUM")
                        and latest_state.get("trend_direction") == trend
                        and abs(latest_stake - stake) < 1e-9
                    ):
                        self._prefetched_proposal = _PrefetchedProposal(
                            proposal_id=proposal_id,
                            ask_price=ask_price,
                            signal=signal,
                            contract_type=contract_type,
                            stake=stake,
                            barrier=barrier,
                            created_at=time.monotonic(),
                        )

                        logger.info(
                            "Pre-fetched %s proposal %s for stake %.2f and barrier %s.",
                            signal,
                            proposal_id,
                            stake,
                            barrier,
                        )

                except asyncio.CancelledError:
                    raise
                except (DerivAPIError, KeyError, TypeError, ValueError) as exc:
                    logger.debug("Proposal prefetch failed: %s", exc)
                    await asyncio.sleep(1.0)
                    continue

                await asyncio.sleep(0.25)

        except asyncio.CancelledError:
            self._prefetched_proposal = None
            raise

    async def _monitor_contract(
        self,
        contract_id: int,
        buy_price: float,
        payout: float,
        poll_interval: float = 1.0,
        max_wait: float = 60.0,
    ):
        start_time = time.time()

        logger.info("Monitoring contract %s...", contract_id)

        while time.time() - start_time < max_wait:
            if not self._client.connected:
                logger.warning("Connection lost while monitoring contract %s; reconnecting.", contract_id)

                if not await self._reconnect(max_attempts=2):
                    await asyncio.sleep(poll_interval * 2)
                    continue

            try:
                status = await self._client.get_open_contract_status(contract_id)

                is_expired = bool(status.get("is_expired", 0))
                is_sold = bool(status.get("is_sold", 0))
                status_name = str(status.get("status", "")).lower()

                if is_expired or is_sold or status_name in {"won", "lost", "sold"}:
                    sell_price = float(status.get("sell_price", 0) or 0)
                    raw_profit = status.get("profit")

                    profit = (
                        float(raw_profit)
                        if raw_profit is not None
                        else sell_price - buy_price
                    )

                    if status_name == "won" or profit > 0:
                        return "WON", round(profit, 2)

                    return "LOST", round(profit, 2)

                await asyncio.sleep(poll_interval)

            except DerivAPIError as exc:
                logger.warning("Error polling contract %s: %s", contract_id, exc)
                await asyncio.sleep(poll_interval * 2)
            except (TypeError, ValueError) as exc:
                logger.warning(
                    "Deriv returned invalid settlement numbers for contract %s: %s",
                    contract_id,
                    exc,
                )
                await asyncio.sleep(poll_interval * 2)

        logger.warning("Contract %s monitoring timed out without a confirmed outcome.", contract_id)

        return "UNKNOWN", 0.0