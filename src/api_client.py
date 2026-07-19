"""
src/api_client.py
-----------------
Deriv API WebSocket client.
Handles connection, authentication, tick subscriptions, candle fetching,
proposal generation, and contract buying.

All methods are async and designed to be called from within an asyncio event loop.

--- FIXES APPLIED (see audit report for full details) ---
BUG-1: Wrong WebSocket endpoint — changed from deprecated
       frontend.binaryws.com (returns HTTP 403) to ws.derivws.com.
BUG-2: Wrong type annotation — WebSocketClientProtocol is the legacy class;
       replaced with ClientConnection from websockets.asyncio.client.
BUG-3: Wrong exception import — WebSocketException no longer exists in the
       top-level websockets package for the new asyncio implementation;
       replaced with the correct websockets.exceptions.WebSocketException
       (still present) and added InvalidHandshake for connection errors.
BUG-4: asyncio.get_event_loop() is deprecated inside a running loop;
       replaced with asyncio.get_running_loop().
BUG-5: _monitor_contract creates a new subscribe call on every poll tick,
       flooding the server with duplicate subscriptions; changed to a
       one-shot (non-subscribing) poll instead.
BUG-6: config.py hardcodes DERIV_WS_URL with the broken binaryws.com host;
       the client now builds its own URL from the correct host so the
       imported constant is no longer used for the connection.
"""

import asyncio
import json
import time
import uuid
from typing import Optional, Dict, Any, List, Callable

import websockets
from websockets.asyncio.client import ClientConnection          # FIX BUG-2
from websockets.exceptions import (
    ConnectionClosed,
    WebSocketException,
    InvalidHandshake,                                           # FIX BUG-3
)

from src.logger import get_logger
from config import SYMBOL, MTF_GRANULARITIES, MTF_CANDLE_COUNT

logger = get_logger("api_client")

# FIX BUG-1 / BUG-6: Correct, live Deriv WebSocket host.
# frontend.binaryws.com is retired and returns HTTP 403.
_DERIV_WS_HOST = "ws.derivws.com"


class DerivAPIClient:
    """
    Low-level Deriv WebSocket API client.

    Manages a persistent WebSocket connection with automatic reconnection.
    Provides async methods for all required API interactions.
    """

    RECONNECT_DELAY_SECONDS = 5
    MAX_RECONNECT_ATTEMPTS = 10
    PING_INTERVAL_SECONDS = 30

    def __init__(self, api_token: str, app_id: str = "1089"):
        # `api_token` may be a personal token or a token1 value returned by
        # Deriv's legacy WebSocket OAuth redirect.  Both are authorized by
        # the legacy WebSocket API using the same `authorize` request.
        self.api_token = api_token.strip()
        self.app_id = app_id
        # FIX BUG-1: Use the current, live endpoint (ws.derivws.com).
        # The old frontend.binaryws.com endpoint is retired and rejects
        # connections with HTTP 403.
        self.ws_url = f"wss://{_DERIV_WS_HOST}/websockets/v3?l=EN&app_id={app_id}"
        self._ws: Optional[ClientConnection] = None             # FIX BUG-2
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._tick_callback: Optional[Callable] = None
        self._contract_callback: Optional[Callable] = None
        self._is_connected: bool = False
        self._is_authorized: bool = False
        self._listener_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._tick_subscription_id: Optional[str] = None
        self._req_id_counter: int = 1

    # ------------------------------------------------------------------
    # Connection Management
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Establish WebSocket connection and authenticate."""
        for attempt in range(1, self.MAX_RECONNECT_ATTEMPTS + 1):
            try:
                logger.info(f"Connecting to Deriv API (attempt {attempt})...")
                self._ws = await websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # We manage pings manually
                    close_timeout=10,
                    open_timeout=15,
                )
                self._is_connected = True
                logger.info("WebSocket connection established.")

                # Start background listener
                self._listener_task = asyncio.create_task(self._message_listener())
                self._ping_task = asyncio.create_task(self._ping_loop())

                # Authenticate
                authorized = await self.authorize(self.api_token)
                if authorized:
                    return True
                else:
                    await self.disconnect()
                    return False

            except (ConnectionClosed, WebSocketException,
                    InvalidHandshake, OSError) as e:             # FIX BUG-3
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                self._is_connected = False
                if attempt < self.MAX_RECONNECT_ATTEMPTS:
                    await asyncio.sleep(self.RECONNECT_DELAY_SECONDS)
                else:
                    logger.error("Max reconnection attempts reached.")
                    return False
        return False

    async def disconnect(self):
        """Gracefully close the WebSocket connection."""
        self._is_connected = False
        self._is_authorized = False
        if self._listener_task:
            self._listener_task.cancel()
        if self._ping_task:
            self._ping_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        logger.info("Disconnected from Deriv API.")

    async def reconnect(self) -> bool:
        """Attempt to reconnect after a disconnection."""
        logger.info("Attempting to reconnect...")
        await self.disconnect()
        await asyncio.sleep(self.RECONNECT_DELAY_SECONDS)
        return await self.connect()

    # ------------------------------------------------------------------
    # Message Listener
    # ------------------------------------------------------------------

    async def _message_listener(self):
        """
        Background task that continuously reads messages from the WebSocket
        and routes them to the appropriate pending request or callback.
        """
        try:
            async for raw_message in self._ws:
                try:
                    message = json.loads(raw_message)
                    msg_type = message.get("msg_type")
                    req_id = message.get("req_id")

                    # Route to pending request future if applicable
                    if req_id and req_id in self._pending_requests:
                        future = self._pending_requests.pop(req_id)
                        if not future.done():
                            if "error" in message:
                                future.set_exception(
                                    DerivAPIError(message["error"]["message"], message["error"]["code"])
                                )
                            else:
                                future.set_result(message)

                    # Route tick updates to callback
                    elif msg_type == "tick" and self._tick_callback:
                        tick_data = message.get("tick", {})
                        await self._tick_callback(tick_data)

                    # Route open contract updates to callback
                    elif msg_type == "proposal_open_contract" and self._contract_callback:
                        poc_data = message.get("proposal_open_contract", {})
                        await self._contract_callback(poc_data)

                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse message: {e}")

        except (ConnectionClosed, WebSocketException) as e:
            logger.warning(f"WebSocket connection closed in listener: {e}")
            self._is_connected = False

    async def _ping_loop(self):
        """Send periodic pings to keep the connection alive."""
        while self._is_connected:
            await asyncio.sleep(self.PING_INTERVAL_SECONDS)
            try:
                await self._send_request({"ping": 1})
                logger.debug("Ping sent.")
            except Exception as e:
                logger.warning(f"Ping failed: {e}")
                break

    # ------------------------------------------------------------------
    # Request/Response Helpers
    # ------------------------------------------------------------------

    def _next_req_id(self) -> int:
        req_id = self._req_id_counter
        self._req_id_counter += 1
        return req_id

    async def _send_request(self, payload: Dict[str, Any], timeout: float = 15.0) -> Dict[str, Any]:
        """
        Send a request and await its response.
        Assigns a unique req_id and registers a Future for the response.
        """
        if not self._is_connected or self._ws is None:
            raise DerivAPIError("Not connected to Deriv API.", "NOT_CONNECTED")

        req_id = self._next_req_id()
        payload["req_id"] = req_id

        # FIX BUG-4: asyncio.get_event_loop() is deprecated inside a running
        # coroutine (raises DeprecationWarning in Python 3.10+ and will raise
        # RuntimeError in future versions). Use get_running_loop() instead,
        # which is always correct when called from within an async context.
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_requests[req_id] = future

        await self._ws.send(json.dumps(payload))

        try:
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            raise DerivAPIError(f"Request timed out after {timeout}s.", "TIMEOUT")

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def authorize(self, oauth_token: Optional[str] = None) -> bool:
        """Authorize a personal token or OAuth redirect token with Deriv.

        Args:
            oauth_token: The ``token1`` session token returned by Deriv's
                legacy OAuth redirect. If omitted, uses the token supplied to
                the client constructor for backwards compatibility.
        """
        try:
            token = (oauth_token or self.api_token).strip()
            if not token:
                raise DerivAPIError("Missing OAuth token.", "MISSING_TOKEN")
            response = await self._send_request({"authorize": token})
            if "authorize" in response:
                self._is_authorized = True
                account = response["authorize"]
                logger.info(
                    f"Authorized as: {account.get('loginid')} | "
                    f"Balance: {account.get('balance')} {account.get('currency')}"
                )
                return True
            return False
        except DerivAPIError as e:
            logger.error(f"Authorization failed: {e}")
            self._is_authorized = False
            return False

    # ------------------------------------------------------------------
    # Market Data
    # ------------------------------------------------------------------

    async def subscribe_ticks(self, symbol: str, callback: Callable):
        """
        Subscribe to live tick stream for a symbol.
        The callback is called with each new tick dict: {symbol, quote, epoch}.
        """
        self._tick_callback = callback
        response = await self._send_request({
            "ticks": symbol,
            "subscribe": 1,
        })
        self._tick_subscription_id = response.get("subscription", {}).get("id")
        logger.info(f"Subscribed to tick stream for {symbol}. Sub ID: {self._tick_subscription_id}")
        return response

    async def unsubscribe_ticks(self):
        """Unsubscribe from the active tick stream."""
        if self._tick_subscription_id:
            try:
                await self._send_request({
                    "forget": self._tick_subscription_id,
                })
                logger.info(f"Unsubscribed from tick stream {self._tick_subscription_id}.")
            except Exception as e:
                logger.warning(f"Failed to unsubscribe ticks: {e}")
            finally:
                self._tick_subscription_id = None
                self._tick_callback = None

    async def get_candles(self, symbol: str, granularity: int, count: int = MTF_CANDLE_COUNT) -> List[Dict]:
        """
        Fetch historical OHLC candles for multi-timeframe analysis.

        Args:
            symbol:      Market symbol (e.g., '1HZ10V').
            granularity: Candle width in seconds (300=5m, 900=15m, 1800=30m).
            count:       Number of candles to retrieve.

        Returns:
            List of candle dicts with keys: open, high, low, close, epoch.
        """
        response = await self._send_request({
            "ticks_history": symbol,
            "style": "candles",
            "granularity": granularity,
            "count": count,
            "end": "latest",
        })
        candles = response.get("candles", [])
        logger.debug(f"Fetched {len(candles)} candles for {symbol} @ {granularity}s granularity.")
        return candles

    async def get_tick_history(self, symbol: str, count: int = 50) -> List[Dict]:
        """Fetch recent tick history for a symbol."""
        response = await self._send_request({
            "ticks_history": symbol,
            "style": "ticks",
            "count": count,
            "end": "latest",
        })
        history = response.get("history", {})
        prices = history.get("prices", [])
        times = history.get("times", [])
        return [{"quote": p, "epoch": t} for p, t in zip(prices, times)]

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------

    async def get_proposal(
        self,
        symbol: str,
        contract_type: str,
        stake: float,
        duration: int,
        duration_unit: str,
        barrier: str,
        currency: str = "USD",
    ) -> Dict[str, Any]:
        """
        Request a price proposal for a Touch contract.

        Args:
            symbol:        Market symbol.
            contract_type: 'ONETOUCH' for touch contracts.
            stake:         Trade amount.
            duration:      Contract duration value.
            duration_unit: 't' for ticks.
            barrier:       Barrier offset string (e.g., '+0.08' or '-0.08').
            currency:      Account currency.

        Returns:
            Proposal response dict containing 'id' and 'ask_price'.
        """
        response = await self._send_request({
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": currency,
            "duration": duration,
            "duration_unit": duration_unit,
            "symbol": symbol,
            "barrier": barrier,
        })
        proposal = response.get("proposal", {})
        logger.info(
            f"Proposal received: ID={proposal.get('id')} | "
            f"Ask={proposal.get('ask_price')} | Barrier={barrier}"
        )
        return proposal

    async def buy_contract(
        self,
        proposal_id: str,
        price: float,
        contract_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Buy a contract using a proposal ID.

        Args:
            proposal_id:       The proposal ID from get_proposal().
            price:             Maximum acceptable price (use ask_price from proposal).
            contract_callback: Optional async callback for open contract status updates.

        Returns:
            Buy response dict containing contract details.
        """
        if contract_callback:
            self._contract_callback = contract_callback

        payload = {
            "buy": proposal_id,
            "price": price,
        }
        if contract_callback:
            payload["subscribe"] = 1

        response = await self._send_request(payload)
        buy_data = response.get("buy", {})
        logger.info(
            f"Contract bought: ID={buy_data.get('contract_id')} | "
            f"Buy Price={buy_data.get('buy_price')} | "
            f"Payout={buy_data.get('payout')}"
        )
        return buy_data

    async def get_open_contract_status(self, contract_id: str) -> Dict[str, Any]:
        """
        Poll the status of an open contract (one-shot, no subscription).

        FIX BUG-5: The original code sent subscribe=1 on every poll call,
        creating a new server-side subscription on each iteration of the
        monitoring loop. This floods the server with duplicate subscriptions,
        exhausts the subscription limit, and causes the response routing to
        break because multiple unsolicited push messages arrive with no
        matching req_id. The fix removes subscribe=1 so each call is a
        simple one-shot query.
        """
        response = await self._send_request({
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            # subscribe: 1 intentionally removed — use polling instead
        })
        return response.get("proposal_open_contract", {})

    async def get_balance(self) -> Dict[str, Any]:
        """Fetch current account balance."""
        response = await self._send_request({"balance": 1})
        return response.get("balance", {})


# ------------------------------------------------------------------
# Custom Exception
# ------------------------------------------------------------------

class DerivAPIError(Exception):
    """Raised when the Deriv API returns an error or a request fails."""

    def __init__(self, message: str, code: str = "UNKNOWN"):
        super().__init__(message)
        self.code = code
        self.message = message

    def __str__(self):
        return f"[{self.code}] {self.message}"
