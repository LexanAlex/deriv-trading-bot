"""
src/trading_engine.py
---------------------
Main trading engine for the Deriv Volatility 10 (1s) Bot.

Orchestrates:
  - Deriv API connection and tick subscription.
  - Strategy signal generation (1-3-1 pattern).
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
)

logger = get_logger("trading_engine")

# How often (in seconds) to refresh MTF candle data
MTF_REFRESH_INTERVAL = 60


class TradingEngine:
    """
    Async trading engine that runs in a background thread.
    Communicates with the Streamlit UI via the shared StateManager.
    """

    def __init__(
        self,
        api_token: str,
        app_id: str,
        account_currency: str,
        state: StateManager,
        initial_stake: float,
        max_martingale_steps: int,
        barrier_buy: str = BARRIER_BUY,
        barrier_sell: str = BARRIER_SELL,
    ):
        # FIX BUG-7: Strip whitespace from the token before storing it.
        # Streamlit text_input widgets can return tokens with leading/trailing
        # spaces or newlines when users paste from a clipboard. The Deriv API
        # treats such tokens as invalid and returns an InvalidToken error even
        # though the underlying token string is correct.
        self.api_token = api_token.strip()
        self.app_id = app_id.strip()
        self.account_currency = (account_currency or CURRENCY).upper()
        self.state = state
        self.initial_stake = initial_stake
        self.max_martingale_steps = max_martingale_steps
        self.barrier_buy = barrier_buy
        self.barrier_sell = barrier_sell

        self._client: Optional[DerivAPIClient] = None
        self._strategy = StrategyEngine()
        self._mtf_analyzer = MTFAnalyzer()
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
        logger.info("Trading engine starting...")
        self.state.set_status("Connecting to Deriv API...")

        self._client = DerivAPIClient(self.api_token, self.app_id)
        connected = await self._client.connect()

        if not connected:
            msg = "Failed to connect to Deriv API. Check your API token and internet connection."
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

            bias = self._mtf_analyzer.analyze(candles_by_tf)
            self._strategy.update_mtf_bias(bias)
            self.state.update_strategy_state(mtf_bias=bias)
            self._last_mtf_refresh = time.time()

            bias_str = bias if bias else "No consensus"
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
            f"Executing {signal} trade | Stake={stake} | Barrier={barrier} | "
            f"Step={martingale_step} | TradeID={trade_id}"
        )

        # Record the trade as OPEN immediately
        trade_record = TradeRecord(
            trade_id=trade_id,
            direction=signal,
            stake=stake,
            barrier=barrier,
            entry_price=entry_price,
            timestamp=timestamp,
            status="OPEN",
            martingale_step=martingale_step,
        )
        self.state.add_trade(trade_record)
        self._strategy.on_trade_executed()

        try:
            # Step 1: Get proposal
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

            # Step 2: Buy the contract
            buy_response = await self._client.buy_contract(
                proposal_id=proposal_id,
                price=ask_price,
            )

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
                f"Contract {contract_id} active | Waiting for settlement..."
            )

            # Step 3: Monitor contract until settlement
            outcome, pnl = await self._monitor_contract(contract_id, buy_price, payout)

            # Step 4: Update trade record and Martingale
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
            logger.error(f"API error during trade execution: {e}")
            self.state.update_trade_outcome(trade_id, "CANCELLED", 0.0)
            self.state.set_error(f"Trade error: {e}")
            self.state.set_status(f"Trade cancelled: {e.message}")

        except Exception as e:
            logger.exception(f"Unexpected error during trade execution: {e}")
            self.state.update_trade_outcome(trade_id, "CANCELLED", 0.0)
            self.state.set_error(f"Unexpected trade error: {e}")

        finally:
            self._active_contract_id = None
            self._trade_in_progress = False

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
            Tuple of (outcome: str, pnl: float) where outcome is "WON" or "LOST".

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
                    profit = float(status.get("profit", 0))
                    sell_price = float(status.get("sell_price", 0))

                    if profit > 0:
                        pnl = round(profit, 2)
                        return "WON", pnl
                    else:
                        pnl = round(-buy_price, 2)
                        return "LOST", pnl

                await asyncio.sleep(poll_interval)

            except DerivAPIError as e:
                logger.warning(f"Error polling contract {contract_id}: {e}")
                await asyncio.sleep(poll_interval * 2)

        # Timeout: treat as unknown/lost
        logger.warning(f"Contract {contract_id} monitoring timed out.")
        return "LOST", round(-buy_price, 2)
