"""
src/api_client.py
Deriv Options API client using the current PAT -> account OTP flow.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed, WebSocketException

from config import MTF_CANDLE_COUNT
from src.logger import get_logger

logger = get_logger("api_client")

OPTIONS_API_BASE = "https://api.derivws.com/trading/v1/options"


class DerivAPIError(Exception):
    def __init__(self, message: str, code: str = "UNKNOWN"):
        super().__init__(message)
        self.message = str(message)
        self.code = str(code or "UNKNOWN")

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


class DerivAPIClient:
    REQUEST_TIMEOUT = 5.0
    REST_TIMEOUT = 15.0
    PING_INTERVAL_SECONDS = 15.0

    def __init__(self, api_token: str, app_id: str, account_id: str):
        self.api_token = api_token.strip()
        self.app_id = app_id.strip()
        self.account_id = account_id.strip()

        self._ws: Optional[ClientConnection] = None
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._tick_callback: Optional[Callable[[Dict[str, Any]], Any]] = None
        self._tick_callback_tasks: Set[asyncio.Task] = set()
        self._tick_subscription_id: Optional[str] = None
        self._req_id = 0
        self._connected = False
        self.last_error = ""

    @staticmethod
    def _headers(api_token: str, app_id: str) -> Dict[str, str]:
        token = api_token.strip()
        identifier = app_id.strip()

        if not token or not identifier:
            raise DerivAPIError(
                "DERIV_API_TOKEN and DERIV_APP_ID must both be set.",
                "MISSING_CREDENTIALS",
            )

        return {
            "Authorization": f"Bearer {token}",
            "Deriv-App-ID": identifier,
            "Accept": "application/json",
        }

    @staticmethod
    def _error_message(body: Any) -> str:
        if isinstance(body, dict):
            errors = body.get("errors")
            if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                return str(errors[0].get("message") or "Deriv rejected the request.")

            error = body.get("error")
            if isinstance(error, dict):
                return str(
                    error.get("message")
                    or error.get("error_description")
                    or "Deriv rejected the request."
                )
            if isinstance(error, str) and error:
                return error

            message = body.get("message")
            if isinstance(message, str) and message:
                return message

        return "Deriv rejected the request. Check the PAT, App ID, and PAT scopes."

    @staticmethod
    def _error_code(body: Any, fallback: str) -> str:
        if isinstance(body, dict):
            errors = body.get("errors")
            if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                return str(errors[0].get("code") or fallback)

            error = body.get("error")
            if isinstance(error, dict):
                return str(error.get("code") or fallback)

        return fallback

    @classmethod
    async def _rest_request(
        cls,
        method: str,
        url: str,
        api_token: str,
        app_id: str,
    ) -> tuple[int, Any]:
        headers = cls._headers(api_token, app_id)

        def decode(raw: str) -> Any:
            if not raw.strip():
                return {}
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {"error": {"message": "Deriv returned a non-JSON response."}}

        def send() -> tuple[int, Any]:
            request = Request(url, method=method, headers=headers)
            try:
                with urlopen(request, timeout=cls.REST_TIMEOUT) as response:
                    raw = response.read().decode("utf-8", errors="replace")
                    return response.status, decode(raw)
            except HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                return exc.code, decode(raw)
            except (URLError, OSError, TimeoutError) as exc:
                raise DerivAPIError(
                    "Could not reach Deriv. Check your internet connection and try again.",
                    "NETWORK_ERROR",
                ) from exc

        return await asyncio.to_thread(send)

    @classmethod
    async def get_accounts(cls, api_token: str, app_id: str) -> List[Dict[str, Any]]:
        status, payload = await cls._rest_request(
            "GET",
            f"{OPTIONS_API_BASE}/accounts",
            api_token,
            app_id,
        )

        if not 200 <= status < 300:
            raise DerivAPIError(
                cls._error_message(payload),
                cls._error_code(payload, f"HTTP_{status}"),
            )

        accounts = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(accounts, list):
            raise DerivAPIError(
                "Deriv returned an unexpected accounts response.",
                "INVALID_ACCOUNTS_RESPONSE",
            )

        active_accounts: List[Dict[str, Any]] = []

        for account in accounts:
            if not isinstance(account, dict):
                continue
            if account.get("status") != "active":
                continue
            if not isinstance(account.get("account_id"), str):
                continue
            active_accounts.append(account)

        return active_accounts

    async def authorize(self) -> bool:
        accounts = await self.get_accounts(self.api_token, self.app_id)

        if not any(str(account.get("account_id")) == self.account_id for account in accounts):
            raise DerivAPIError(
                "The selected Deriv account is inactive or unavailable to this PAT.",
                "ACCOUNT_NOT_AVAILABLE",
            )

        return True

    async def _websocket_url(self) -> str:
        endpoint = f"{OPTIONS_API_BASE}/accounts/{self.account_id}/otp"

        status, payload = await self._rest_request(
            "POST",
            endpoint,
            self.api_token,
            self.app_id,
        )

        if not 200 <= status < 300:
            raise DerivAPIError(
                self._error_message(payload),
                self._error_code(payload, f"HTTP_{status}"),
            )

        data = payload.get("data") if isinstance(payload, dict) else None
        url = data.get("url") if isinstance(data, dict) else None

        if not isinstance(url, str) or not url.startswith("wss://"):
            raise DerivAPIError(
                "Deriv did not return a valid account WebSocket URL.",
                "INVALID_OTP_RESPONSE",
            )

        return url

    async def connect(self) -> bool:
        if self._ws is not None or self._listener_task is not None:
            await self.disconnect()

        self.last_error = ""

        try:
            await self.authorize()
            websocket_url = await self._websocket_url()

            self._ws = await websockets.connect(
                websocket_url,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=20,
                close_timeout=10,
                max_size=2**20,
            )

            self._connected = True
            self._listener_task = asyncio.create_task(
                self._message_listener(),
                name="deriv-message-listener",
            )
            self._ping_task = asyncio.create_task(
                self._ping_loop(),
                name="deriv-api-ping",
            )

            logger.info(
                "Connected to Deriv Options WebSocket for account %s.",
                self.account_id,
            )

            return True

        except (
            DerivAPIError,
            WebSocketException,
            OSError,
            asyncio.TimeoutError,
        ) as exc:
            self.last_error = str(exc)
            logger.warning("Deriv connection failed: %s", exc)
            await self.disconnect()
            return False

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None

    def _fail_all_pending(self, error: DerivAPIError) -> None:
        for future in list(self._pending_requests.values()):
            if not future.done():
                future.set_exception(error)
        self._pending_requests.clear()

    async def disconnect(self, cancel_callbacks: bool = True) -> None:
        self._connected = False
        self._tick_callback = None
        self._tick_subscription_id = None

        self._fail_all_pending(
            DerivAPIError("Deriv connection closed.", "CONNECTION_CLOSED")
        )

        current = asyncio.current_task()
        callback_tasks = list(self._tick_callback_tasks) if cancel_callbacks else []
        tasks = [self._listener_task, self._ping_task, *callback_tasks]

        for task in tasks:
            if task is not None and task is not current and not task.done():
                task.cancel()

        if self._ws is not None:
            try:
                await self._ws.close()
            except (WebSocketException, OSError):
                pass

        wait_for = [task for task in tasks if task is not None and task is not current]
        if wait_for:
            await asyncio.gather(*wait_for, return_exceptions=True)

        if cancel_callbacks:
            self._tick_callback_tasks.clear()

        self._listener_task = None
        self._ping_task = None
        self._ws = None

    @staticmethod
    def _websocket_error(message: Dict[str, Any]) -> DerivAPIError:
        error = message.get("error")

        if isinstance(error, dict):
            return DerivAPIError(
                str(error.get("message") or "Deriv request failed."),
                str(error.get("code") or "API_ERROR"),
            )

        return DerivAPIError("Deriv request failed.", "API_ERROR")

    def _tick_task_finished(self, task: asyncio.Task) -> None:
        self._tick_callback_tasks.discard(task)

        if task.cancelled():
            return

        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return

        if exception is not None:
            logger.error(
                "Tick callback failed without stopping the WebSocket reader: %s",
                exception,
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    def _dispatch_tick(self, tick: Dict[str, Any]) -> None:
        callback = self._tick_callback

        if callback is None:
            return

        try:
            result = callback(tick)
        except Exception:
            logger.exception("Tick callback raised before returning.")
            return

        if inspect.isawaitable(result):
            task = asyncio.create_task(result, name="deriv-tick-callback")
            self._tick_callback_tasks.add(task)
            task.add_done_callback(self._tick_task_finished)

    async def _message_listener(self) -> None:
        try:
            assert self._ws is not None

            async for raw_message in self._ws:
                message = json.loads(raw_message)

                if not isinstance(message, dict):
                    raise ValueError("Deriv WebSocket message was not an object.")

                request_id = message.get("req_id")

                if request_id in self._pending_requests:
                    future = self._pending_requests.pop(request_id)

                    if "error" in message:
                        if not future.done():
                            future.set_exception(self._websocket_error(message))
                    elif not future.done():
                        future.set_result(message)

                    continue

                if message.get("msg_type") == "tick":
                    tick = message.get("tick")
                    if isinstance(tick, dict):
                        self._dispatch_tick(tick)

        except asyncio.CancelledError:
            raise
        except (
            ConnectionClosed,
            WebSocketException,
            json.JSONDecodeError,
            OSError,
            TypeError,
            ValueError,
        ) as exc:
            if self._connected:
                logger.warning("Deriv WebSocket listener stopped: %s", exc)
        finally:
            if self._connected:
                self._connected = False
                self._fail_all_pending(
                    DerivAPIError("Deriv connection lost.", "CONNECTION_LOST")
                )

    async def _ping_loop(self) -> None:
        try:
            while self._connected:
                await asyncio.sleep(self.PING_INTERVAL_SECONDS)
                await self._send_request({"ping": 1}, timeout=self.REQUEST_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except DerivAPIError as exc:
            if self._connected:
                logger.warning("Deriv API ping failed: %s", exc)
                self._connected = False
                self._fail_all_pending(
                    DerivAPIError("Deriv connection lost.", "CONNECTION_LOST")
                )

    async def _send_request(
        self,
        payload: Dict[str, Any],
        timeout: float = REQUEST_TIMEOUT,
    ) -> Dict[str, Any]:
        if not self._connected or self._ws is None:
            raise DerivAPIError("Not connected to Deriv.", "NOT_CONNECTED")

        if not isinstance(payload, dict) or not payload:
            raise DerivAPIError("Cannot send an empty Deriv request.", "INVALID_REQUEST")

        self._req_id += 1
        request_id = self._req_id
        request = dict(payload, req_id=request_id)

        future = asyncio.get_running_loop().create_future()
        self._pending_requests[request_id] = future

        try:
            await self._ws.send(json.dumps(request))
            response = await asyncio.wait_for(future, timeout=timeout)

            if not isinstance(response, dict):
                raise DerivAPIError(
                    "Deriv returned an invalid response envelope.",
                    "INVALID_RESPONSE",
                )

            return response

        except (ConnectionClosed, WebSocketException, OSError) as exc:
            self._pending_requests.pop(request_id, None)
            self._connected = False
            raise DerivAPIError(
                "Deriv connection lost while sending the request.",
                "CONNECTION_LOST",
            ) from exc
        except asyncio.TimeoutError as exc:
            self._pending_requests.pop(request_id, None)
            raise DerivAPIError("Deriv did not answer in time.", "TIMEOUT") from exc
        except (TypeError, ValueError) as exc:
            self._pending_requests.pop(request_id, None)
            raise DerivAPIError(
                "The Deriv request could not be encoded.",
                "INVALID_REQUEST",
            ) from exc

    @staticmethod
    def _require_object(
        response: Dict[str, Any],
        key: str,
        required_fields: tuple[str, ...] = (),
    ) -> Dict[str, Any]:
        value = response.get(key)

        if not isinstance(value, dict):
            raise DerivAPIError(
                f"Deriv response did not include a valid '{key}' object.",
                "INVALID_RESPONSE",
            )

        missing = [field for field in required_fields if field not in value]
        if missing:
            raise DerivAPIError(
                f"Deriv '{key}' response is missing: {', '.join(missing)}.",
                "INVALID_RESPONSE",
            )

        return value

    @staticmethod
    def _require_number(value: Any, field: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DerivAPIError(
                f"Deriv returned an invalid numeric '{field}' value.",
                "INVALID_RESPONSE",
            )

        return float(value)

    async def subscribe_ticks(
        self,
        symbol: str,
        callback: Callable[[Dict[str, Any]], Any],
    ) -> Dict[str, Any]:
        self._tick_callback = callback

        try:
            response = await self._send_request({"ticks": symbol, "subscribe": 1})
            subscription = self._require_object(response, "subscription", ("id",))
            subscription_id = subscription.get("id")

            if not isinstance(subscription_id, str) or not subscription_id:
                raise DerivAPIError(
                    "Deriv returned an invalid tick subscription ID.",
                    "INVALID_RESPONSE",
                )

            self._tick_subscription_id = subscription_id
            return response
        except Exception:
            self._tick_callback = None
            raise

    async def unsubscribe_ticks(self) -> None:
        subscription_id = self._tick_subscription_id
        self._tick_subscription_id = None
        self._tick_callback = None

        if subscription_id and self.connected:
            try:
                await self._send_request({"forget": subscription_id})
            except DerivAPIError as exc:
                logger.debug("Tick unsubscribe could not be confirmed: %s", exc)

    async def get_candles(
        self,
        symbol: str,
        granularity: int,
        count: int = MTF_CANDLE_COUNT,
    ) -> List[Dict[str, Any]]:
        response = await self._send_request(
            {
                "ticks_history": symbol,
                "style": "candles",
                "granularity": granularity,
                "count": count,
                "end": "latest",
            }
        )

        candles = response.get("candles")
        if not isinstance(candles, list):
            raise DerivAPIError(
                "Deriv returned an invalid candle-history response.",
                "INVALID_RESPONSE",
            )

        return [candle for candle in candles if isinstance(candle, dict)]

    async def get_proposal(
        self,
        symbol: str,
        contract_type: str,
        stake: float,
        duration: int,
        duration_unit: str,
        barrier: str,
        currency: str,
    ) -> Dict[str, Any]:
        response = await self._send_request(
            {
                "proposal": 1,
                "amount": float(stake),
                "basis": "stake",
                "contract_type": contract_type,
                "currency": currency,
                "duration": int(duration),
                "duration_unit": duration_unit,
                "underlying_symbol": symbol,
                "barrier": str(barrier),
            }
        )

        proposal = self._require_object(
            response,
            "proposal",
            ("id", "ask_price", "payout", "spot", "spot_time"),
        )

        proposal_id = proposal.get("id")
        if not isinstance(proposal_id, str) or not proposal_id:
            raise DerivAPIError(
                "Deriv returned an invalid proposal ID.",
                "INVALID_RESPONSE",
            )

        self._require_number(proposal.get("ask_price"), "proposal.ask_price")
        self._require_number(proposal.get("payout"), "proposal.payout")

        return proposal

    async def buy_contract(
        self,
        proposal_id: str,
        price: float,
        contract_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        del contract_callback

        if not isinstance(proposal_id, str) or not proposal_id:
            raise DerivAPIError("A valid proposal ID is required.", "INVALID_REQUEST")

        response = await self._send_request(
            {"buy": proposal_id, "price": float(price)}
        )

        buy = self._require_object(
            response,
            "buy",
            ("contract_id", "buy_price", "payout"),
        )

        contract_id = buy.get("contract_id")
        if isinstance(contract_id, bool) or not isinstance(contract_id, int):
            raise DerivAPIError(
                "Deriv returned an invalid contract ID in the buy receipt.",
                "INVALID_RESPONSE",
            )

        self._require_number(buy.get("buy_price"), "buy.buy_price")
        self._require_number(buy.get("payout"), "buy.payout")

        return buy

    async def get_open_contract_status(self, contract_id: int) -> Dict[str, Any]:
        if isinstance(contract_id, bool) or not isinstance(contract_id, int):
            raise DerivAPIError("A numeric contract ID is required.", "INVALID_REQUEST")

        response = await self._send_request(
            {"proposal_open_contract": 1, "contract_id": contract_id}
        )

        return self._require_object(response, "proposal_open_contract", ("contract_id",))

    async def get_portfolio(self) -> List[Dict[str, Any]]:
        response = await self._send_request({"portfolio": 1})
        portfolio = self._require_object(response, "portfolio", ("contracts",))

        contracts = portfolio.get("contracts")
        if not isinstance(contracts, list):
            raise DerivAPIError(
                "Deriv returned an invalid portfolio response.",
                "INVALID_RESPONSE",
            )

        return [contract for contract in contracts if isinstance(contract, dict)]

    async def get_balance(self) -> Dict[str, Any]:
        response = await self._send_request({"balance": 1})
        return self._require_object(response, "balance", ("balance", "currency"))