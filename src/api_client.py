"""Current Deriv PAT client.

A Personal Access Token is not sent in the legacy WebSocket ``authorize``
request.  It is used as a Bearer token for the Options REST API.  Deriv then
returns a short-lived, account-specific WebSocket URL (OTP) for market data
and trading requests.
"""

import asyncio
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from typing import Any, Callable, Dict, List, Optional

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed, WebSocketException

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

    REQUEST_TIMEOUT = 20.0
    PING_INTERVAL_SECONDS = 30

    def __init__(self, api_token: str, app_id: str, account_id: str):
        self.api_token = api_token.strip()
        self.app_id = app_id.strip()
        self.account_id = account_id.strip()
        self._ws: Optional[ClientConnection] = None
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._tick_callback: Optional[Callable] = None
        self._tick_subscription_id: Optional[str] = None
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
                with urlopen(request, timeout=cls.REQUEST_TIMEOUT) as response:
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
            self._ws = await websockets.connect(await self._websocket_url(), ping_interval=15, ping_timeout=10, open_timeout=20, close_timeout=10)
            self._connected = True
            self._listener_task = asyncio.create_task(self._message_listener())
            self._ping_task = asyncio.create_task(self._ping_loop())
            logger.info("Connected to Deriv Options WebSocket for account %s.", self.account_id)
            return True
        except (DerivAPIError, WebSocketException, OSError) as exc:
            self.last_error = str(exc)
            logger.warning("Deriv connection failed: %s", exc)
            await self.disconnect()
            return False

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None

    def _fail_all_pending(self, error: "DerivAPIError") -> None:
        """Immediately reject every in-flight request instead of letting them
        sit until their individual 20s timeouts expire. Called the moment we
        know the connection is gone, so a dead socket fails fast and loud
        rather than looking like an ordinary slow response."""
        for future in self._pending_requests.values():
            if not future.done():
                future.set_exception(error)
        self._pending_requests.clear()

    async def disconnect(self) -> None:
        self._connected = False
        for task in (self._listener_task, self._ping_task):
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
            self._connected = False
            # Without this, any request already sent (including a buy that
            # may have reached Deriv) would just sit in _pending_requests
            # until its own 20s timeout expired, surfacing as an ambiguous
            # "Deriv did not answer in time" instead of a clear, immediate
            # "connection lost" — and losing precious seconds where a
            # reconnect-and-check could have happened instead.
            self._fail_all_pending(DerivAPIError("Deriv connection lost.", "CONNECTION_LOST"))

    async def _ping_loop(self) -> None:
        while self._connected:
            await asyncio.sleep(self.PING_INTERVAL_SECONDS)
            try:
                await self._send_request({"ping": 1})
            except DerivAPIError:
                self._connected = False
                return

    async def _send_request(self, payload: Dict[str, Any], timeout: float = REQUEST_TIMEOUT) -> Dict[str, Any]:
        if not self._connected or self._ws is None:
            raise DerivAPIError("Not connected to Deriv.", "NOT_CONNECTED")
        self._req_id += 1
        request_id = self._req_id
        request = dict(payload, req_id=request_id)
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[request_id] = future
        try:
            await self._ws.send(json.dumps(request))
            return await asyncio.wait_for(future, timeout=timeout)
        except (ConnectionClosed, WebSocketException) as exc:
            # The send itself failed — the frame never left this process, so
            # there is no ambiguity about whether Deriv received it.
            self._pending_requests.pop(request_id, None)
            self._connected = False
            raise DerivAPIError("Deriv connection lost while sending the request.", "CONNECTION_LOST") from exc
        except asyncio.TimeoutError as exc:
            self._pending_requests.pop(request_id, None)
            raise DerivAPIError("Deriv did not answer in time.", "TIMEOUT") from exc

    async def subscribe_ticks(self, symbol: str, callback: Callable) -> Dict[str, Any]:
        self._tick_callback = callback
        response = await self._send_request({"ticks": symbol, "subscribe": 1})
        self._tick_subscription_id = response.get("subscription", {}).get("id")
        return response

    async def unsubscribe_ticks(self) -> None:
        if self._tick_subscription_id:
            await self._send_request({"forget": self._tick_subscription_id})
        self._tick_subscription_id = None
        self._tick_callback = None

    async def get_candles(self, symbol: str, granularity: int, count: int = MTF_CANDLE_COUNT) -> List[Dict[str, Any]]:
        response = await self._send_request({"ticks_history": symbol, "style": "candles", "granularity": granularity, "count": count, "end": "latest"})
        return response.get("candles", [])

    async def get_proposal(self, symbol: str, contract_type: str, stake: float, duration: int, duration_unit: str, barrier: str, currency: str) -> Dict[str, Any]:
        response = await self._send_request({
            "proposal": 1, "amount": stake, "basis": "stake", "contract_type": contract_type,
            "currency": currency, "duration": duration, "duration_unit": duration_unit,
            "underlying_symbol": symbol, "barrier": barrier,
        })
        return response.get("proposal", {})

    async def buy_contract(self, proposal_id: str, price: float, contract_callback: Optional[Callable] = None) -> Dict[str, Any]:
        response = await self._send_request({"buy": proposal_id, "price": price})
        return response.get("buy", {})

    async def get_open_contract_status(self, contract_id: str) -> Dict[str, Any]:
        response = await self._send_request({"proposal_open_contract": 1, "contract_id": contract_id})
        return response.get("proposal_open_contract", {})

    async def get_portfolio(self) -> List[Dict[str, Any]]:
        """List all currently open contracts on this account.

        Used to reconcile after a buy request times out locally: the order
        may still have reached Deriv and been filled even though no response
        frame arrived before our wait_for() deadline.
        """
        response = await self._send_request({"portfolio": 1})
        return response.get("portfolio", {}).get("contracts", [])

    async def get_balance(self) -> Dict[str, Any]:
        response = await self._send_request({"balance": 1})
        return response.get("balance", {})
