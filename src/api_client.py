"""Current Deriv PAT client.

A Personal Access Token is not sent in the legacy WebSocket ``authorize``
request.  It is used as a Bearer token for the Options REST API.  Deriv then
returns a short-lived, account-specific WebSocket URL (OTP) for market data
and trading requests.

--- REWRITE NOTES (execution-path fix) ---
The previous version tore down the connection whenever a single *application
level* heartbeat ("ping": 1) reply was slow, even though the socket itself
was still perfectly healthy. Because every trading call shares the same
"not connected -> refuse immediately" guard, one slow heartbeat reply was
enough to make every proposal/buy request fail instantly with
[NOT_CONNECTED], right after an unrelated [TIMEOUT] on some other request.
That is the exact failure loop this rewrite removes. See _heartbeat_loop()
and the "connected" property below for the fix.
"""

import asyncio
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from typing import Any, Callable, Dict, List, Optional

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed, WebSocketException

try:
    # Used only for an extra, defensive liveness check on the socket object
    # itself. If this import ever fails on a different websockets version,
    # we simply skip that extra check rather than crash.
    from websockets.protocol import State as _WSState
except Exception:  # pragma: no cover - defensive only
    _WSState = None

from config import MTF_CANDLE_COUNT
from src.logger import get_logger

logger = get_logger("api_client")

OPTIONS_API_BASE = "https://api.derivws.com/trading/v1/options"


class DerivAPIError(Exception):
    """A safe, user-displayable Deriv API error."""

    def __init__(self, message: str, code: str = "UNKNOWN"):
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


class DerivAPIClient:
    """Authenticated Options WebSocket session for one Deriv account."""

    # A 5-tick contract expires in ~5 seconds, so the proposal/buy round trip
    # has to be fast. But the earlier 3.0s value was tuned too tight for
    # Streamlit Cloud -> Deriv latency and was firing on ordinary jitter, not
    # genuine failures. 4.0s keeps us well inside the contract window while
    # giving real-world network variance enough room that a normal response
    # doesn't get treated as a timeout.
    TRADE_TIMEOUT = 4.0

    # Non-latency-critical calls (candles, portfolio reconciliation, balance,
    # contract status polling) don't need to race the market, so they get a
    # more generous timeout instead of competing with trade requests for the
    # same tight deadline.
    DEFAULT_TIMEOUT = 8.0

    # Transport-level ping/pong, handled entirely by the `websockets` library
    # via the parameters passed to `websockets.connect()` below. This is the
    # single authoritative signal for "the socket is actually dead": if a
    # pong doesn't arrive within PING_TIMEOUT_SECONDS, the library itself
    # closes the connection, which _message_listener() detects via
    # ConnectionClosed.
    PING_INTERVAL_SECONDS = 15
    PING_TIMEOUT_SECONDS = 12

    # Application-level heartbeat. Separate from the transport ping above:
    # some proxies/load balancers only reset idle timers on real application
    # traffic, so we still send a periodic {"ping": 1}. Critically, this is
    # now advisory only — a slow or missing reply logs a warning but never
    # by itself marks the connection as dead. Only a genuine transport close
    # (caught in _message_listener) does that.
    HEARTBEAT_INTERVAL_SECONDS = 20
    HEARTBEAT_TIMEOUT_SECONDS = 10

    def __init__(self, api_token: str, app_id: str, account_id: str):
        self.api_token = api_token.strip()
        self.app_id = app_id.strip()
        self.account_id = account_id.strip()
        self._ws: Optional[ClientConnection] = None
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._tick_callback: Optional[Callable] = None
        self._tick_subscription_id: Optional[str] = None
        self._tick_symbol: Optional[str] = None
        self._req_id = 0
        self._connected = False
        self.last_error = ""

    @staticmethod
    def _headers(api_token: str, app_id: str) -> Dict[str, str]:
        token = api_token.strip()
        identifier = app_id.strip()
        if not token or not identifier:
            raise DerivAPIError("DERIV_API_TOKEN and DERIV_APP_ID must both be set in Streamlit Secrets.", "MISSING_CREDENTIALS")
        return {"Authorization": f"Bearer {token}", "Deriv-App-ID": identifier}

    @classmethod
    async def get_accounts(cls, api_token: str, app_id: str) -> List[Dict[str, Any]]:
        """Return active Options accounts without exposing the PAT."""
        status, payload = await cls._rest_request("GET", f"{OPTIONS_API_BASE}/accounts", api_token, app_id)
        if not 200 <= status < 300:
            raise DerivAPIError(cls._error_message(payload), f"HTTP_{status}")
        accounts = payload.get("data", payload.get("accounts", []))
        if not isinstance(accounts, list):
            raise DerivAPIError("Deriv returned an unexpected accounts response.", "INVALID_RESPONSE")
        return [account for account in accounts if account.get("status", "active") == "active"]

    @staticmethod
    def _error_message(body: Any) -> str:
        if isinstance(body, dict):
            error = body.get("error", body)
            if isinstance(error, dict):
                return str(error.get("message") or error.get("error_description") or "Deriv rejected the request.")
        return "Deriv rejected the request. Check the PAT, App ID, and PAT scopes."

    @classmethod
    async def _rest_request(cls, method: str, url: str, api_token: str, app_id: str) -> tuple[int, Any]:
        """Run a small HTTPS request outside the event loop using stdlib only."""
        headers = cls._headers(api_token, app_id)

        def send() -> tuple[int, Any]:
            request = Request(url, method=method, headers=headers)
            try:
                with urlopen(request, timeout=cls.DEFAULT_TIMEOUT) as response:
                    raw = response.read().decode("utf-8")
                    return response.status, json.loads(raw)
            except HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    payload = {"error": {"message": raw or "Deriv rejected the request."}}
                return exc.code, payload
            except (URLError, OSError) as exc:
                raise DerivAPIError("Could not reach Deriv. Check your internet connection and try again.", "NETWORK_ERROR") from exc

        return await asyncio.to_thread(send)

    async def authorize(self) -> bool:
        """Validate this PAT and ensure it is allowed to use the chosen account."""
        accounts = await self.get_accounts(self.api_token, self.app_id)
        if not any(account.get("account_id") == self.account_id for account in accounts):
            raise DerivAPIError("The selected Deriv account is unavailable to this PAT.", "ACCOUNT_NOT_AVAILABLE")
        return True

    async def _websocket_url(self) -> str:
        # The OTP URL is short-lived and effectively single-use, so this is
        # always fetched fresh — including on every reconnect. Never cache
        # or reuse a previously issued OTP URL.
        endpoint = f"{OPTIONS_API_BASE}/accounts/{self.account_id}/otp"
        status, payload = await self._rest_request("POST", endpoint, self.api_token, self.app_id)
        if not 200 <= status < 300:
            raise DerivAPIError(self._error_message(payload), f"HTTP_{status}")
        data = payload.get("data", payload)
        url = data.get("url") if isinstance(data, dict) else None
        if not isinstance(url, str) or not url.startswith("wss://"):
            raise DerivAPIError("Deriv did not return a valid WebSocket URL.", "INVALID_OTP_RESPONSE")
        return url

    async def connect(self) -> bool:
        """Validate PAT, create an OTP session, and open its WebSocket."""
        try:
            await self.authorize()
            self._ws = await websockets.connect(
                await self._websocket_url(),
                ping_interval=self.PING_INTERVAL_SECONDS,
                ping_timeout=self.PING_TIMEOUT_SECONDS,
                open_timeout=15,
                close_timeout=5,
            )
            self._connected = True
            self._listener_task = asyncio.create_task(self._message_listener())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("Connected to Deriv Options WebSocket for account %s.", self.account_id)
            return True
        except (DerivAPIError, WebSocketException, OSError) as exc:
            self.last_error = str(exc)
            logger.warning("Deriv connection failed: %s", exc)
            await self.disconnect()
            return False

    @property
    def connected(self) -> bool:
        if not self._connected or self._ws is None:
            return False
        # Defensive extra check: trust the socket's own reported state over
        # our internal flag when we can read it, so a connection that closed
        # between heartbeats but hasn't been noticed yet by the listener
        # doesn't look "connected" for one extra request.
        if _WSState is not None:
            state = getattr(self._ws, "state", None)
            if state is not None and state != _WSState.OPEN:
                return False
        return True

    def _fail_all_pending(self, error: "DerivAPIError") -> None:
        """Immediately reject every in-flight request instead of letting them
        sit until their individual timeouts expire. Called the moment we
        know the connection is gone, so a dead socket fails fast and loud
        rather than looking like an ordinary slow response."""
        for future in self._pending_requests.values():
            if not future.done():
                future.set_exception(error)
        self._pending_requests.clear()

    async def disconnect(self) -> None:
        self._connected = False
        for task in (self._listener_task, self._heartbeat_task):
            if task:
                task.cancel()
        self._fail_all_pending(DerivAPIError("Deriv connection closed.", "CONNECTION_CLOSED"))
        if self._ws:
            try:
                await self._ws.close()
            except WebSocketException:
                pass
        self._ws = None

    async def _message_listener(self) -> None:
        try:
            assert self._ws is not None
            async for raw_message in self._ws:
                message = json.loads(raw_message)
                request_id = message.get("req_id")
                if request_id in self._pending_requests:
                    future = self._pending_requests.pop(request_id)
                    if future.done():
                        continue
                    if "error" in message:
                        error = message["error"]
                        future.set_exception(DerivAPIError(error.get("message", "Deriv request failed."), error.get("code", "API_ERROR")))
                    else:
                        future.set_result(message)
                elif message.get("msg_type") == "tick" and self._tick_callback:
                    result = self._tick_callback(message.get("tick", {}))
                    if asyncio.iscoroutine(result):
                        await result
        except (ConnectionClosed, WebSocketException, json.JSONDecodeError) as exc:
            logger.warning("Deriv WebSocket listener stopped: %s", exc)
            # This is the ONLY place normal request handling marks the
            # connection dead. Timeouts on individual requests (in
            # _send_request) are ambiguous and do not imply the socket is
            # gone; an actual close/protocol error here is unambiguous.
            self._connected = False
            self._fail_all_pending(DerivAPIError("Deriv connection lost.", "CONNECTION_LOST"))

    async def _heartbeat_loop(self) -> None:
        """Advisory application-level keepalive.

        Deliberately does NOT set self._connected = False on a slow or
        missing reply. A single delayed heartbeat used to be treated as
        proof the connection was dead, which then made every subsequent
        trade request fail immediately with [NOT_CONNECTED] even though the
        socket was fine — the exact bug reported. The transport-level
        ping/pong configured on websockets.connect() is what actually
        detects a dead socket, surfaced through _message_listener().
        """
        consecutive_failures = 0
        while self._connected:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL_SECONDS)
            if not self._connected:
                return
            try:
                await self._send_request({"ping": 1}, timeout=self.HEARTBEAT_TIMEOUT_SECONDS)
                consecutive_failures = 0
            except DerivAPIError as exc:
                consecutive_failures += 1
                logger.warning(
                    "Application heartbeat failed (%d consecutive, non-fatal): %s",
                    consecutive_failures, exc,
                )

    async def _send_request(self, payload: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
        if not self.connected or self._ws is None:
            raise DerivAPIError("Not connected to Deriv.", "NOT_CONNECTED")
        effective_timeout = self.TRADE_TIMEOUT if timeout is None else timeout
        self._req_id += 1
        request_id = self._req_id
        request = dict(payload, req_id=request_id)
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[request_id] = future
        try:
            await self._ws.send(json.dumps(request))
            return await asyncio.wait_for(future, timeout=effective_timeout)
        except (ConnectionClosed, WebSocketException) as exc:
            # The send itself failed — the frame never left this process, so
            # there is no ambiguity about whether Deriv received it.
            self._pending_requests.pop(request_id, None)
            self._connected = False
            raise DerivAPIError("Deriv connection lost while sending the request.", "CONNECTION_LOST") from exc
        except asyncio.TimeoutError as exc:
            # Ambiguous: the request may still be in flight or may have
            # reached Deriv and been filled. We deliberately do NOT flip
            # self._connected here — a slow reply is not proof of a dead
            # socket, and doing so previously caused every following request
            # to fail instantly with NOT_CONNECTED regardless of whether the
            # socket was actually still usable.
            self._pending_requests.pop(request_id, None)
            raise DerivAPIError("Deriv did not answer in time.", "TIMEOUT") from exc

    async def subscribe_ticks(self, symbol: str, callback: Callable) -> Dict[str, Any]:
        self._tick_callback = callback
        self._tick_symbol = symbol
        response = await self._send_request({"ticks": symbol, "subscribe": 1}, timeout=self.DEFAULT_TIMEOUT)
        self._tick_subscription_id = response.get("subscription", {}).get("id")
        return response

    async def unsubscribe_ticks(self) -> None:
        if self._tick_subscription_id and self.connected:
            try:
                await self._send_request({"forget": self._tick_subscription_id}, timeout=self.DEFAULT_TIMEOUT)
            except DerivAPIError:
                pass  # Best-effort; the socket may already be going away.
        self._tick_subscription_id = None
        self._tick_callback = None
        self._tick_symbol = None

    async def resubscribe_ticks(self) -> Optional[Dict[str, Any]]:
        """Re-arm the last known tick subscription after a reconnect.

        Uses the symbol/callback recorded by the most recent subscribe_ticks()
        call, so callers only need to reconnect() and then call this — no
        need to remember and re-pass the symbol/callback themselves.
        """
        if not self._tick_symbol or not self._tick_callback:
            return None
        callback = self._tick_callback
        symbol = self._tick_symbol
        self._tick_subscription_id = None
        return await self.subscribe_ticks(symbol, callback)

    async def get_candles(self, symbol: str, granularity: int, count: int = MTF_CANDLE_COUNT) -> List[Dict[str, Any]]:
        response = await self._send_request(
            {"ticks_history": symbol, "style": "candles", "granularity": granularity, "count": count, "end": "latest"},
            timeout=self.DEFAULT_TIMEOUT,
        )
        return response.get("candles", [])

    async def get_proposal(self, symbol: str, contract_type: str, stake: float, duration: int, duration_unit: str, barrier: str, currency: str) -> Dict[str, Any]:
        response = await self._send_request({
            "proposal": 1, "amount": stake, "basis": "stake", "contract_type": contract_type,
            "currency": currency, "duration": duration, "duration_unit": duration_unit,
            "symbol": symbol, "barrier": barrier,
        }, timeout=self.TRADE_TIMEOUT)
        return response.get("proposal", {})

    async def buy_contract(self, proposal_id: str, price: float, contract_callback: Optional[Callable] = None) -> Dict[str, Any]:
        response = await self._send_request({"buy": proposal_id, "price": price}, timeout=self.TRADE_TIMEOUT)
        return response.get("buy", {})

    async def get_open_contract_status(self, contract_id: str) -> Dict[str, Any]:
        response = await self._send_request({"proposal_open_contract": 1, "contract_id": contract_id}, timeout=self.DEFAULT_TIMEOUT)
        return response.get("proposal_open_contract", {})

    async def get_portfolio(self) -> List[Dict[str, Any]]:
        """List all currently open contracts on this account.

        Used to reconcile after a buy request times out locally: the order
        may still have reached Deriv and been filled even though no response
        frame arrived before our wait_for() deadline.
        """
        response = await self._send_request({"portfolio": 1}, timeout=self.DEFAULT_TIMEOUT)
        return response.get("portfolio", {}).get("contracts", [])

    async def get_balance(self) -> Dict[str, Any]:
        response = await self._send_request({"balance": 1}, timeout=self.DEFAULT_TIMEOUT)
        return response.get("balance", {})
