"""
src/trading_engine.py
---------------------
Main trading engine for the Deriv Volatility 10 (1s) Bot.

Orchestrates:
  - Deriv API connection and tick subscription.
  - Quality pullback-momentum signal generation.
  - Multi-timeframe analysis (5m, 15m, 30m).
  - Trade execution with Martingale stake management.
  - Contract outcome monitoring and P&L tracking.
  - State updates for the Streamlit UI.

--- FIXES APPLIED (see audit report for full details) ---
BUG-7: API token is taken raw from a Streamlit text_input widget and passed
       directly to DerivAPIClient without stripping whitespace. Users who
       accidentally paste a token with a leading/trailing space or newline
       will always get an InvalidToken error from the API even though the
       token itself is valid. The fix strips the token before use.
"""

import asyncio
import time
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from src.api_client import DerivAPIClient, DerivAPIError
from src.strategy import StrategyEngine, MTFAnalyzer
from src.state_manager import StateManager, TradeRecord
from src.logger import get_logger
from config import (
    SYMBOL,
    CONTRACT_TYPE_BUY,
    CONTRACT_TYPE_SELL,
    CONTRACT_DURATION,
    CONTRACT_DURATION_UNIT,
    BARRIER_BUY,
    BARRIER_SELL,
    CURRENCY,
    MARTINGALE_MULTIPLIER,
    MTF_GRANULARITIES,
    MTF_CANDLE_COUNT,
    DEFAULT_STRATEGY_SENSITIVITY,
    STRATEGY_SENSITIVITY_PRESETS,
)

logger = get_logger("trading_engine")

# How often (in seconds) to refresh MTF candle data
MTF_REFRESH_INTERVAL = 60

# Deriv's current Options API documents account_type values as ``demo`` and
# ``real``. ``virtual`` is accepted as a defensive compatibility alias because
# older Deriv surfaces and the product UI commonly use that term.
_DEMO_ACCOUNT_TYPES = {"DEMO", "VIRTUAL", "PRACTICE", "VIRTUAL_ACCOUNT"}
_REAL_ACCOUNT_TYPES = {"REAL", "LIVE", "REAL_MONEY"}


def normalize_account_type(account_type: str) -> str:
    """Return a stable, display-safe Deriv account type."""
    normalized = str(account_type or "").strip().upper().replace("-", "_").replace(" ", "_")
    if normalized in _DEMO_ACCOUNT_TYPES:
        return "DEMO"
    if normalized in _REAL_ACCOUNT_TYPES:
        return "REAL"
    return normalized or "UNKNOWN"


def resolve_execution_mode(
    account_type: str, real_execution_confirmed: bool
) -> str:
    """Resolve the immutable execution policy for one engine session.

    Demo accounts can send real API orders immediately. Real-money accounts
    require an explicit LIVE confirmation. Unknown account types are fail-closed.
    """
    normalized_type = normalize_account_type(account_type)
    if normalized_type == "DEMO":
        return "DEMO"
    if normalized_type == "REAL" and real_execution_confirmed:
        return "REAL"
    return "BLOCKED"


class TradingEngine:
    """
    Async trading engine that runs in a background thread.
    Communicates with the Streamlit UI via the shared StateManager.
    """

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
    ):
        # FIX BUG-7: Strip whitespace from the token before storing it.
        # Streamlit text_input widgets can return tokens with leading/trailing
        # spaces or newlines when users paste from a clipboard. The Deriv API
        # treats such tokens as invalid and returns an InvalidToken error even
        # though the underlying token string is correct.
        self.api_token = api_token.strip()
        self.app_id = app_id.strip()
        self.account_id = account_id.strip()
        self.account_currency = (account_currency or CURRENCY).upper()
        self.account_type = normalize_account_type(account_type)
        self.real_execution_confirmed = bool(real_execution_confirmed)
        self.execution_mode = resolve_execution_mode(
            self.account_type, self.real_execution_confirmed
        )
        self.state = state
        self.initial_stake = initial_stake
        self.max_martingale_steps = max_martingale_steps
        self.barrier_buy = barrier_buy
        self.barrier_sell = barrier_sell

        preset = STRATEGY_SENSITIVITY_PRESETS.get(
            strategy_sensitivity, STRATEGY_SENSITIVITY_PRESETS[DEFAULT_STRATEGY_SENSITIVITY]
        )

        self._client: Optional[DerivAPIClient] = None
        self._strategy = StrategyEngine(
            velocity_threshold=preset["velocity_threshold"],
            burst_threshold=preset["burst_threshold"],
            mtf_min_agreement=preset["mtf_min_agreement"],
        )
        self._mtf_analyzer = MTFAnalyzer(min_agreement=preset["mtf_min_agreement"])
        self._last_mtf_refresh: float = 0.0
        self._active_contract_id: Optional[str] = None
        self._trade_in_progress: bool = False

    # ------------------------------------------------------------------
    # Main Entry Point
    # ------------------------------------------------------------------

    async def run(self):
        """
        Main async loop. Connects to Deriv API, subscribes to ticks,
        and processes signals until stop is requested.
        """
        logger.info("Trading engine starting with execution mode %s for %s account.", self.execution_mode, self.account_type)
        self.state.set_execution_context(
            account_id=self.account_id,
            account_type=self.account_type,
            currency=self.account_currency,
            execution_mode=self.execution_mode,
        )
        if self.execution_mode == "BLOCKED":
            self.state.set_error(
                "Order execution is blocked: the selected account is real without an exact LIVE confirmation, or its account type is unknown."
            )
            self.state.set_status("Signal monitoring is active, but no orders can be sent.")
        else:
            self.state.set_status(f"Connecting to Deriv API for {self.execution_mode.lower()} order execution...")

        self._client = DerivAPIClient(self.api_token, self.app_id, self.account_id)
        connected = await self._client.connect()

        if not connected:
            detail = self._client.last_error if self._client else ""
            msg = f"Failed to connect to Deriv API. {detail or 'Check your App ID, PAT scopes, and internet connection.'}"
            logger.error(msg)
            self.state.set_error(msg)
            self.state.set_status("Connection failed.")
            self.state.set_running(False)
            return

        self.state.set_status("Connected. Fetching initial MTF data...")
        logger.info("Connected to Deriv API. Fetching initial MTF data...")

        # Perform initial MTF analysis
        await self._refresh_mtf()

        # Subscribe to live ticks
        self.state.set_status("Subscribed to tick stream. Bot is active.")
        logger.info(f"Subscribing to tick stream for {SYMBOL}...")

        try:
            await self._client.subscribe_ticks(SYMBOL, self._on_tick)
            logger.info("Tick subscription active. Entering main loop.")

            # Keep running until stop is requested
            while not self.state.stop_requested:
                await asyncio.sleep(1)

                if not self._client.connected:
                    await self._reconnect()

                # Periodically refresh MTF data
                if time.time() - self._last_mtf_refresh > MTF_REFRESH_INTERVAL:
                    await self._refresh_mtf()

        except DerivAPIError as e:
            logger.error(f"API error in main loop: {e}")
            self.state.set_error(str(e))
        except Exception as e:
            logger.exception(f"Unexpected error in main loop: {e}")
            self.state.set_error(f"Unexpected error: {e}")
        finally:
            await self._shutdown()

    async def _reconnect(self, max_attempts: int = 5) -> bool:
        """
        Re-establish the Deriv connection and tick subscription after the
        socket has died (e.g. a dropped network path or an idle-connection
        cutoff). Without this, a single lost connection would either kill
        the whole bot or leave every subsequent request doomed to time out
        against a socket that's already gone.
        """
        for attempt in range(1, max_attempts + 1):
            self.state.set_status(f"Connection lost. Reconnecting ({attempt}/{max_attempts})...")
            logger.warning("Reconnecting to Deriv (attempt %d/%d)...", attempt, max_attempts)
            try:
                await self._client.disconnect()
            except Exception:
                pass
            try:
                if await self._client.connect():
                    await self._client.subscribe_ticks(SYMBOL, self._on_tick)
                    self._last_mtf_refresh = 0.0  # force a fresh MTF fetch
                    self.state.set_status("Reconnected to Deriv. Bot is active.")
                    logger.info("Reconnected to Deriv successfully.")
                    return True
            except DerivAPIError as exc:
                logger.warning("Reconnect attempt %d failed: %s", attempt, exc)
            await asyncio.sleep(min(2 ** attempt, 15))

        self.state.set_error("Could not reconnect to Deriv after repeated attempts.")
        logger.error("Reconnect failed after %d attempts.", max_attempts)
        return False

    async def _shutdown(self):
        """Clean up resources on shutdown."""
        logger.info("Trading engine shutting down...")
        self.state.set_status("Bot stopped.")
        if self._client:
            await self._client.unsubscribe_ticks()
            await self._client.disconnect()
        self.state.set_running(False)
        logger.info("Trading engine stopped.")

    # ------------------------------------------------------------------
    # Tick Processing
    # ------------------------------------------------------------------

    async def _on_tick(self, tick_data: Dict[str, Any]):
        """
        Async callback invoked for every new tick received from the API.
        Updates state, runs strategy, and triggers trade execution if signalled.
        """
        try:
            price = float(tick_data.get("quote", 0))
            epoch = float(tick_data.get("epoch", time.time()))

            if price == 0:
                return

            # Update shared state for UI
            self.state.update_tick(price, epoch)

            # Skip signal generation if a trade is currently in progress
            if self._trade_in_progress:
                return

            # Process tick through strategy engine
            signal = self._strategy.process_tick(price)

            # Update strategy state in shared state for UI
            strategy_state = self._strategy.get_state()
            self.state.update_strategy_state(
                current_trend_direction=strategy_state["trend_direction"],
                trend_tick_count=strategy_state["trend_tick_count"],
                trend_kind=strategy_state["trend_kind"],
                trades_in_current_trend=strategy_state["trades_in_trend"],
                in_cooldown=strategy_state["in_cooldown"],
                pattern_stage=strategy_state["pattern_stage"],
                mtf_bias=strategy_state["mtf_bias"],
            )

            if signal in ("BUY", "SELL"):
                logger.info(f"Signal received: {signal} at price {price}")
                self.state.set_status(f"Signal: {signal} at {price:.4f}. Placing trade...")
                await self._execute_trade(signal, price)

        except Exception as e:
            logger.exception(f"Error processing tick: {e}")

    # ------------------------------------------------------------------
    # Multi-Timeframe Analysis
    # ------------------------------------------------------------------

    async def _refresh_mtf(self):
        """
        Fetch candle data for all configured timeframes and compute MTF bias.
        Updates the strategy engine and shared state with the result.
        """
        logger.info("Refreshing MTF data...")
        self.state.set_status("Refreshing multi-timeframe data...")
        candles_by_tf = {}

        try:
            for tf_label, granularity in MTF_GRANULARITIES.items():
                candles = await self._client.get_candles(SYMBOL, granularity, MTF_CANDLE_COUNT)
                candles_by_tf[tf_label] = candles
                logger.debug(f"MTF {tf_label}: {len(candles)} candles fetched.")

            bias, agreement = self._mtf_analyzer.analyze_with_strength(candles_by_tf)
            self._strategy.update_mtf_bias(bias, agreement)
            self.state.update_strategy_state(mtf_bias=bias)
            self._last_mtf_refresh = time.time()

            bias_str = f"{bias} ({agreement}/3)" if bias else "No consensus"
            logger.info(f"MTF analysis complete. Bias: {bias_str}")
            self.state.set_status(f"Bot active | MTF Bias: {bias_str}")

        except DerivAPIError as e:
            logger.warning(f"MTF refresh failed: {e}")
            self.state.set_status("MTF refresh failed. Using last known bias.")
        except Exception as e:
            logger.exception(f"Unexpected error during MTF refresh: {e}")

    # ------------------------------------------------------------------
    # Trade Execution
    # ------------------------------------------------------------------

    async def _execute_trade(self, signal: str, entry_price: float):
        """
        Execute a Touch contract trade based on the given signal.

        Workflow:
          1. Get current Martingale stake.
          2. Request a price proposal from the API.
          3. Buy the contract using the proposal ID.
          4. Monitor the contract until settlement.
          5. Update Martingale state based on outcome.
        """
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
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        logger.info(
            "Handling %s signal | Mode=%s | Stake=%s | Barrier=%s | Step=%s | TradeID=%s",
            signal,
            self.execution_mode,
            stake,
            barrier,
            martingale_step,
            trade_id,
        )

        # Unknown account types and unconfirmed real accounts are fail-closed.
        # A visible cancelled record lets the UI distinguish this from a signal
        # that has not yet occurred or a Deriv-side rejection.
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
            self._strategy.on_trade_executed()
            self.state.set_error(reason)
            self.state.set_status(f"Signal: {signal} at {entry_price:.4f}. Order blocked by safety gate.")
            logger.warning("Blocked order signal TradeID=%s: %s", trade_id, reason)
            self._trade_in_progress = False
            return

        # Record an API-backed order attempt before asking Deriv for a proposal.
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
        self._strategy.on_trade_executed()
        self.state.clear_error()
        execution_stage = "proposal request"

        # Don't gamble a real order on a socket we already know is dead.
        # This is the single most important guard for live trading: without
        # it, a connection that died between ticks would only be discovered
        # by watching the buy request time out 20 seconds later — by which
        # point it's ambiguous whether Deriv ever saw it.
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
            # Step 1: Get a price proposal. The record already exists as OPEN,
            # so a rejection cannot be mistaken for a missing strategy signal.
            self.state.set_status(
                f"{self.execution_mode} order: requesting proposal for {signal} (stake {stake:.2f} {self.account_currency})..."
            )
            logger.info("Requesting Deriv proposal for TradeID=%s.", trade_id)
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
            ask_price = float(proposal.get("ask_price", stake))

            if not proposal_id:
                raise DerivAPIError("No proposal ID received.", "NO_PROPOSAL")

            # Step 2: Submit the buy request using the proposal identifier.
            execution_stage = "buy request"
            self.state.set_status(
                f"Proposal received. Submitting {self.execution_mode.lower()} buy request..."
            )
            logger.info("Proposal %s received for TradeID=%s; submitting buy request.", proposal_id, trade_id)

            # A timeout here is ambiguous: our request may still have reached
            # Deriv and been filled even though no confirmation frame arrived
            # in time. Treating that as a plain failure would leave a real,
            # live contract untracked. Reconcile against the account's open
            # positions before concluding the trade never happened.
            try:
                buy_response = await self._client.buy_contract(
                    proposal_id=proposal_id,
                    price=ask_price,
                )
            except DerivAPIError as buy_exc:
                if buy_exc.code in ("TIMEOUT", "CONNECTION_LOST"):
                    buy_response = await self._reconcile_after_buy_timeout(stake, contract_type)
                    if buy_response is None:
                        raise
                else:
                    raise

            contract_id = buy_response.get("contract_id")
            buy_price = float(buy_response.get("buy_price", stake))
            payout = float(buy_response.get("payout", 0))

            if not contract_id:
                raise DerivAPIError("No contract ID in buy response.", "NO_CONTRACT_ID")

            self._active_contract_id = contract_id
            trade_record.contract_id = contract_id

            logger.info(
                f"Contract {contract_id} bought | Buy Price={buy_price} | "
                f"Payout={payout} | TradeID={trade_id}"
            )
            self.state.set_status(
                f"{self.execution_mode} contract {contract_id} active | Waiting for Deriv settlement..."
            )

            # Step 3: Monitor contract until settlement
            outcome, pnl = await self._monitor_contract(contract_id, buy_price, payout)

            # Step 4: Update trade record and Martingale. A monitoring timeout
            # is intentionally not converted into a loss: Deriv may still settle
            # the contract after a temporary connectivity problem.
            if outcome == "UNKNOWN":
                reason = (
                    f"Contract {contract_id} was bought but its settlement was not confirmed within the monitoring window. "
                    "Check the Deriv account statement; Martingale was not changed."
                )
                self.state.update_trade_outcome(trade_id, "UNKNOWN", 0.0, reason)
                self.state.set_error(reason)
                self.state.set_status(f"Contract {contract_id} outcome is unresolved; check Deriv before continuing.")
                logger.warning("Trade outcome unresolved | TradeID=%s | Contract=%s", trade_id, contract_id)
            else:
                self.state.update_trade_outcome(trade_id, outcome, pnl)
                if outcome == "WON":
                    logger.info(f"Trade WON | PnL={pnl:.2f} | TradeID={trade_id}")
                    self.state.on_trade_win()
                    self.state.set_status(f"Trade WON! P&L: +{pnl:.2f}")
                else:
                    logger.info(f"Trade LOST | PnL={pnl:.2f} | TradeID={trade_id}")
                    self.state.on_trade_loss(MARTINGALE_MULTIPLIER, self.max_martingale_steps)
                    new_stake = self.state.get_martingale_state()["stake"]
                    self.state.set_status(
                        f"Trade LOST. Next stake: {new_stake:.2f} (Step {self.state.get_martingale_state()['step']})"
                    )

        except DerivAPIError as e:
            reason = f"Deriv rejected the {execution_stage}: {e.message} ({e.code})."
            logger.error("API error during %s for TradeID=%s: %s", execution_stage, trade_id, e)
            self.state.update_trade_outcome(trade_id, "CANCELLED", 0.0, reason)
            self.state.set_error(reason)
            self.state.set_status(f"Order cancelled during {execution_stage}: {e.message}")

        except Exception as e:
            reason = f"Unexpected failure during the {execution_stage}: {e}"
            logger.exception("Unexpected error during %s for TradeID=%s: %s", execution_stage, trade_id, e)
            self.state.update_trade_outcome(trade_id, "CANCELLED", 0.0, reason)
            self.state.set_error(reason)
            self.state.set_status(f"Order cancelled during {execution_stage}; see error details.")

        finally:
            self._active_contract_id = None
            self._trade_in_progress = False

    async def _reconcile_after_buy_timeout(
        self, stake: float, contract_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        After a buy_contract() call times out waiting for Deriv's response,
        check the account's live portfolio for a matching contract that we
        haven't already recorded. If Deriv actually filled the order, this
        lets the engine pick it up and monitor it normally instead of
        silently orphaning a real position.

        Returns a dict shaped like a buy response (contract_id, buy_price,
        payout) if a match is found, otherwise None.
        """
        logger.warning(
            "Buy request timed out waiting for Deriv's response. Checking "
            "the live portfolio for an untracked fill before giving up..."
        )
        try:
            # Give Deriv a brief moment in case the fill is still propagating,
            # then check.
            await asyncio.sleep(2)
            contracts = await self._client.get_portfolio()
        except DerivAPIError as exc:
            logger.error("Could not fetch portfolio for reconciliation: %s", exc)
            return None

        known_ids = {
            trade.contract_id
            for trade in self.state.get_trade_history()
            if getattr(trade, "contract_id", None)
        }

        for contract in contracts:
            if str(contract.get("contract_id")) in known_ids:
                continue
            if contract.get("underlying") not in (None, SYMBOL):
                continue
            if contract.get("contract_type") != contract_type:
                continue
            if abs(float(contract.get("buy_price", -1)) - stake) > max(0.01, stake * 0.05):
                continue
            logger.warning(
                "Found an untracked live contract %s matching this order after "
                "the timeout — adopting it instead of marking the trade CANCELLED.",
                contract.get("contract_id"),
            )
            return {
                "contract_id": contract.get("contract_id"),
                "buy_price": contract.get("buy_price", stake),
                "payout": contract.get("payout", 0),
            }

        logger.warning(
            "No matching untracked contract found in the portfolio. The buy "
            "most likely never reached Deriv, but verify manually in your "
            "Deriv account statement before assuming no money moved."
        )
        return None

    async def _monitor_contract(
        self,
        contract_id: str,
        buy_price: float,
        payout: float,
        poll_interval: float = 1.0,
        max_wait: float = 60.0,
    ):
        """
        Poll the open contract status until it is settled.

        Returns:
            Tuple of (outcome: str, pnl: float), where outcome is "WON",
            "LOST", or "UNKNOWN" if settlement cannot be confirmed safely.

        Note: get_open_contract_status() now uses one-shot polling (no subscribe=1)
        so this loop is safe to call repeatedly without leaking subscriptions.
        See BUG-5 fix in api_client.py.
        """
        start_time = time.time()
        logger.info(f"Monitoring contract {contract_id}...")

        while time.time() - start_time < max_wait:
            try:
                status = await self._client.get_open_contract_status(contract_id)
                is_expired = status.get("is_expired", 0)
                is_sold = status.get("is_sold", 0)

                if is_expired or is_sold:
                    sell_price = float(status.get("sell_price", 0) or 0)
                    raw_profit = status.get("profit")
                    profit = float(raw_profit) if raw_profit is not None else sell_price - buy_price

                    if profit > 0:
                        return "WON", round(profit, 2)
                    return "LOST", round(profit if raw_profit is not None else sell_price - buy_price, 2)

                await asyncio.sleep(poll_interval)

            except DerivAPIError as e:
                logger.warning(f"Error polling contract {contract_id}: {e}")
                await asyncio.sleep(poll_interval * 2)

        # Do not classify an unconfirmed contract as a loss. The account
        # statement remains authoritative if polling cannot confirm settlement.
        logger.warning(f"Contract {contract_id} monitoring timed out without a confirmed outcome.")
        return "UNKNOWN", 0.0
