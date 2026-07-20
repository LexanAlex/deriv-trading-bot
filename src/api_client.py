"""Deriv PAT client.

A Personal Access Token is not sent in the legacy WebSocket ``authorize``
request. It is used as a Bearer token for the Options REST API. Deriv then
returns a short-lived, account-specific WebSocket URL (OTP) for market data
and trading requests.

--- REWRITE NOTES (execution-path fix, round 2) ---
The previous rewrite fixed the "one slow heartbeat kills the whole session"
bug (see the advisory-only _heartbeat_loop below, which is kept as-is — it
was correct). But the reported logs show something different: EVERY kind of
request started timing out together (proposal, MTF candles, heartbeat, buy),
clustered in bursts. That pattern is not a dead-socket problem — a dead
socket fails fast with CONNECTION_LOST/NOT_CONNECTED, not a slow drip of
TIMEOUTs across unrelated call types. It is the signature of the *client*
sending more requests than Deriv is answering promptly: the previous
trading_engine.py fired a brand-new one-shot ``{"proposal": 1}`` request on
almost every tick (every ~0.8s) to keep the pre-fetch "fresh", stacked on top
of ticks, the 20s app heartbeat, and periodic MTF candle pulls, all sharing
one connection. Under load or latency, Deriv (or a proxy in front of it)
falls behind, and *every* request type queued behind the backlog times out
together — exactly what the logs show.

The fix here is not a bigger timeout. It's sending far fewer requests:

1. Proposals are now fetched with Deriv's own streaming mode
   (``"proposal": 1, "subscribe": 1``). Sent ONCE per contract
   spec (direction + barrier + stake); Deriv then pushes fresh ask_price
   updates on its own, the same way ticks are pushed, with no repeated
   round trips. This is what "always have a proposal ready" actually means
   on this API — see subscribe_proposal()/forget_proposal() below. This
   directly addresses the request in the user's notes: proposals that are
   "already there" and ready to use the instant a signal fires, without
   hammering the connection to keep them fresh.
2. A small concurrency gate (_request_semaphore) caps how many requests
   this client will have in flight at once, so a reconnect burst (resubscribe
   ticks + both proposal streams + an MTF refresh, all at once) can't itself
   recreate the pile-up.
3. Timeouts are re-tuned: trade-critical calls get a little more headroom
   than the previous 3-4s (Streamlit Cloud's outbound latency to Deriv
   commonly spikes well past 3s under shared-CPU contention), while transport
   ping/pong stays the sole source of truth for "the socket is actually dead".
"""

import asyncio
import json
import time
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

    # Trade-critical calls: a fresh (non-streamed) proposal fetch, or a buy
    # request. These race the market on a 5-tick contract, but the previous
    # 3.0s / 4.0s values were tuned tighter than Streamlit Cloud's real-world
    # outbound latency to Deriv, so ordinary jitter was firing as a false
    # TIMEOUT. With proposal streaming (below) this path is now the rare
    # *fallback* rather than the hot path, so a little extra headroom here
    # buys reliability without costing meaningful speed in the common case.
    TRADE_TIMEOUT = 6.0

    # Non-latency-critical calls (candles, portfolio reconciliation, balance,
    # contract status polling) don't need to race the market.
    DEFAULT_TIMEOUT = 10.0

    # Transport-level ping/pong, handled entirely by the `websockets` library
    # via the parameters passed to `websockets.connect()` below. This is the
    # single authoritative signal for "the socket is actually dead": if a
    # pong doesn't arrive within PING_TIMEOUT_SECONDS, the library itself
    # closes the connection, which _message_listener() detects via
    # ConnectionClosed. Widened slightly from 15/12 to 20/18 so a Streamlit
    # Cloud container that's briefly CPU-starved (shared vCPU) doesn't get its
    # otherwise-healthy socket killed just because a pong was scheduled late.
    PING_INTERVAL_SECONDS = 20
    PING_TIMEOUT_SECONDS = 18

    # Application-level heartbeat. Separate from the transport ping above:
    # some proxies/load balancers only reset idle timers on real application
    # traffic. This is advisory only — a slow or missing reply logs a warning
    # but never by itself marks the connection dead. Interval widened from
    # 20s to 45s: with proposal streaming replacing per-tick polling, request
    # volume on the connection is already much lower, so the heartbeat's job
    # is just "keep intermediary proxies from idling us out", which doesn't
    # need to happen anywhere near every 20s.
    HEARTBEAT_INTERVAL_SECONDS = 45
    HEARTBEAT_TIMEOUT_SECONDS = 10

    # Caps how many requests this client will have awaiting a response at
    # once. Prevents a reconnect (resubscribe ticks + 2 proposal streams +
    # an MTF refresh, all fired back-to-back) from recreating the exact
    # request pile-up that caused the reported timeout storm.
    MAX_CONCURRENT_REQUESTS = 4

    def __init__(self, api_token: str, app_id: str, account_id: str):
        self.api_token = api_token.strip()
        self.app_id = app_id.strip()
        self.account_id = account_id.strip()
        self._ws: Optional[ClientConnection] = None
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._request_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)

        self._tick_callback: Optional[Callable] = None
        self._tick_subscription_id: Optional[str] = None
        self._tick_symbol: Optional[str] = None

        # Streaming proposal subscriptions, keyed by subscription id.
        # _proposal_specs remembers the request payload used for each active
        # subscription tag (e.g. "BUY"/"SELL") so resubscribe_proposals()
        # can re-arm them after a reconnect without the caller having to
        # resend the contract details.
        self._proposal_callbacks: Dict[str, Callable] = {}
        self._proposal_specs: Dict[str, Dict[str, Any]] = {}   # tag -> request payload
        self._proposal_sub_ids: Dict[str, str] = {}            # tag -> subscription id

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
        # Subscription bookkeeping is tied to the connection that created it;
        # a fresh connect() gets fresh subscription IDs from Deriv, so drop
        # the stale ones here. The specs (contract details / tick symbol)
        # are kept so resubscribe_ticks()/resubscribe_proposals() can re-arm
        # them without the caller re-supplying anything.
        self._tick_subscription_id = None
        self._proposal_sub_ids = {}

    async def _message_listener(self) -> None:
        try:
            assert self._ws is not None
            async for raw_message in self._ws:
                message = json.loads(raw_message)
                request_id = message.get("req_id")
                msg_type = message.get("msg_type")

                if request_id in self._pending_requests:
                    # First frame for this request — whether a one-shot reply
                    # or the initial push of a new subscription.
                    future = self._pending_requests.pop(request_id)
                    if not future.done():
                        if "error" in message:
                            error = message["error"]
                            future.set_exception(DerivAPIError(error.get("message", "Deriv request failed."), error.get("code", "API_ERROR")))
                        else:
                            future.set_result(message)
                    continue

                # Subsequent pushes on an existing subscription (ticks or
                # streamed proposals) no longer have a pending future — Deriv
                # keeps sending them under the same req_id/subscription id.
                if msg_type == "tick" and self._tick_callback:
                    await self._maybe_await(self._tick_callback(message.get("tick", {})))
                elif msg_type == "proposal":
                    sub_id = message.get("subscription", {}).get("id")
                    callback = self._proposal_callbacks.get(sub_id) if sub_id else None
                    if callback and "error" not in message:
                        await self._maybe_await(callback(message.get("proposal", {})))
                    elif "error" in message:
                        logger.debug("Streamed proposal error: %s", message["error"])
        except (ConnectionClosed, WebSocketException, json.JSONDecodeError) as exc:
            logger.warning("Deriv WebSocket listener stopped: %s", exc)
            # This is the ONLY place normal request handling marks the
            # connection dead. Timeouts on individual requests (in
            # _send_request) are ambiguous and do not imply the socket is
            # gone; an actual close/protocol error here is unambiguous.
            self._connected = False
            self._fail_all_pending(DerivAPIError("Deriv connection lost.", "CONNECTION_LOST"))

    @staticmethod
    async def _maybe_await(result: Any) -> None:
        if asyncio.iscoroutine(result):
            await result

    async def _heartbeat_loop(self) -> None:
        """Advisory application-level keepalive.

        Deliberately does NOT set self._connected = False on a slow or
        missing reply. The transport-level ping/pong configured on
        websockets.connect() is what actually detects a dead socket,
        surfaced through _message_listener().
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
        # Bound concurrency so a burst of calls (e.g. right after a
        # reconnect) can't itself flood the connection and reproduce a
        # timeout storm. Requests simply queue briefly for a free slot
        # rather than all racing Deriv at once.
        async with self._request_semaphore:
            if not self.connected or self._ws is None:
                raise DerivAPIError("Not connected to Deriv.", "NOT_CONNECTED")
            self._req_id += 1
            request_id = self._req_id
            request = dict(payload, req_id=request_id)
            future = asyncio.get_running_loop().create_future()
            self._pending_requests[request_id] = future
            try:
                await self._ws.send(json.dumps(request))
                return await asyncio.wait_for(future, timeout=effective_timeout)
            except (ConnectionClosed, WebSocketException) as exc:
                # The send itself failed — the frame never left this process,
                # so there is no ambiguity about whether Deriv received it.
                self._pending_requests.pop(request_id, None)
                self._connected = False
                raise DerivAPIError("Deriv connection lost while sending the request.", "CONNECTION_LOST") from exc
            except asyncio.TimeoutError as exc:
                # Ambiguous: the request may still be in flight or may have
                # reached Deriv and been filled. We deliberately do NOT flip
                # self._connected here — a slow reply is not proof of a dead
                # socket.
                self._pending_requests.pop(request_id, None)
                raise DerivAPIError("Deriv did not answer in time.", "TIMEOUT") from exc

    # ------------------------------------------------------------------
    # Ticks
    # ------------------------------------------------------------------

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
        """Re-arm the last known tick subscription after a reconnect."""
        if not self._tick_symbol or not self._tick_callback:
            return None
        callback = self._tick_callback
        symbol = self._tick_symbol
        self._tick_subscription_id = None
        return await self.subscribe_ticks(symbol, callback)

    # ------------------------------------------------------------------
    # Candles / portfolio / balance
    # ------------------------------------------------------------------

    async def get_candles(self, symbol: str, granularity: int, count: int = MTF_CANDLE_COUNT) -> List[Dict[str, Any]]:
        response = await self._send_request(
            {"ticks_history": symbol, "style": "candles", "granularity": granularity, "count": count, "end": "latest"},
            timeout=self.DEFAULT_TIMEOUT,
        )
        return response.get("candles", [])

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

    # ------------------------------------------------------------------
    # Proposals — one-shot (fallback path) and streaming (primary path)
    # ------------------------------------------------------------------

    async def get_proposal(self, symbol: str, contract_type: str, stake: float, duration: int, duration_unit: str, barrier: str, currency: str) -> Dict[str, Any]:
        """One-shot proposal request. Kept for the synchronous fallback path
        used when no fresh streamed proposal is available (e.g. right after
        connecting, or if a stream got dropped). Not used as the steady-state
        way of keeping a proposal ready anymore — see subscribe_proposal()."""
        response = await self._send_request({
            "proposal": 1, "amount": stake, "basis": "stake", "contract_type": contract_type,
            "currency": currency, "duration": duration, "duration_unit": duration_unit,
            "symbol": symbol, "barrier": barrier,
        }, timeout=self.TRADE_TIMEOUT)
        return response.get("proposal", {})

    async def subscribe_proposal(
        self,
        tag: str,
        symbol: str,
        contract_type: str,
        stake: float,
        duration: int,
        duration_unit: str,
        barrier: str,
        currency: str,
        callback: Callable[[Dict[str, Any]], Any],
    ) -> Optional[str]:
        """Open a live-updating proposal stream, tagged (e.g. "BUY"/"SELL")
        so the caller can look it up and forget/replace it later.

        Deriv pushes a fresh ask_price on this same subscription as the
        underlying price moves — no repeated requests needed to keep it
        current. `callback` is invoked with each proposal dict (both the
        initial one and every subsequent push), so the caller can just cache
        "latest proposal we've seen" and trust it's fresh.
        """
        payload = {
            "proposal": 1, "subscribe": 1, "amount": stake, "basis": "stake",
            "contract_type": contract_type, "currency": currency, "duration": duration,
            "duration_unit": duration_unit, "symbol": symbol, "barrier": barrier,
        }
        # Replace any existing subscription under this tag first so we never
        # leak a subscription (e.g. the stake changed after a Martingale step).
        await self.forget_proposal(tag)

        response = await self._send_request(payload, timeout=self.DEFAULT_TIMEOUT)
        sub_id = response.get("subscription", {}).get("id")
        if not sub_id:
            # Deriv accepted the request but didn't hand back a subscription
            # id (shouldn't normally happen with subscribe=1) — nothing to
            # track, but still feed the caller the initial value.
            await self._maybe_await(callback(response.get("proposal", {})))
            return None

        self._proposal_specs[tag] = payload
        self._proposal_sub_ids[tag] = sub_id
        self._proposal_callbacks[sub_id] = callback
        await self._maybe_await(callback(response.get("proposal", {})))
        return sub_id

    async def forget_proposal(self, tag: str) -> None:
        """Cancel the streamed proposal subscription registered under `tag`,
        if any."""
        sub_id = self._proposal_sub_ids.pop(tag, None)
        self._proposal_specs.pop(tag, None)
        if sub_id:
            self._proposal_callbacks.pop(sub_id, None)
            if self.connected:
                try:
                    await self._send_request({"forget": sub_id}, timeout=self.DEFAULT_TIMEOUT)
                except DerivAPIError:
                    pass  # Best-effort — the socket may already be going away.

    async def forget_all_proposals(self) -> None:
        for tag in list(self._proposal_sub_ids.keys()):
            await self.forget_proposal(tag)

    async def resubscribe_proposals(self) -> None:
        """Re-arm every streamed proposal subscription that was active
        before a reconnect, using the specs remembered from the original
        subscribe_proposal() calls. Mirrors resubscribe_ticks()."""
        specs = dict(self._proposal_specs)
        callbacks = {
            tag: self._proposal_callbacks.get(sub_id)
            for tag, sub_id in self._proposal_sub_ids.items()
        }
        # disconnect() already cleared _proposal_sub_ids (stale IDs from the
        # old socket), so rebuild from the remembered specs/callbacks.
        self._proposal_sub_ids = {}
        for tag, payload in specs.items():
            callback = callbacks.get(tag)
            if not callback:
                continue
            try:
                await self.subscribe_proposal(
                    tag=tag,
                    symbol=payload["symbol"],
                    contract_type=payload["contract_type"],
                    stake=payload["amount"],
                    duration=payload["duration"],
                    duration_unit=payload["duration_unit"],
                    barrier=payload["barrier"],
                    currency=payload["currency"],
                    callback=callback,
                )
            except DerivAPIError as exc:
                logger.warning("Failed to resubscribe proposal stream '%s': %s", tag, exc)

    async def buy_contract(self, proposal_id: str, price: float, contract_callback: Optional[Callable] = None) -> Dict[str, Any]:
        response = await self._send_request({"buy": proposal_id, "price": price}, timeout=self.TRADE_TIMEOUT)
        return response.get("buy", {})
