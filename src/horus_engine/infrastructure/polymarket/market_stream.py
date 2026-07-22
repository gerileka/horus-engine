"""Read-only public Polymarket market-data WebSocket adapter."""

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Protocol, cast

from websockets.asyncio.client import connect

from horus_engine.application import (
    MarketDataDisconnected,
    MarketDataEvent,
    MarketId,
    TokenId,
)

from .errors import (
    PolymarketPayloadError,
    PolymarketSubscriptionError,
    PolymarketWebSocketError,
)
from .websocket_parsing import parse_market_data_message

DEFAULT_MARKET_WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class _WebSocketConnection(Protocol):
    """The small subset of a client connection used by this adapter."""

    async def send(self, message: str) -> None:
        """Send one text frame."""

    async def recv(self) -> str | bytes:
        """Receive one text or binary frame."""


ConnectionFactory = Callable[[str], AbstractAsyncContextManager[_WebSocketConnection]]


class PolymarketMarketDataStreamGateway:
    """Stream unauthenticated public market data without local book state.

    Each call opens one caller-independent public connection, makes one token-ID
    subscription, and ends after an unexpected disconnection. It has no account,
    order, authentication, or trading capabilities.
    """

    def __init__(
        self,
        url: str = DEFAULT_MARKET_WEBSOCKET_URL,
        heartbeat_interval: float = 10,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        """Create a gateway with explicit public endpoint and heartbeat settings."""
        if not isinstance(url, str) or not url.strip():
            raise ValueError("url must be a non-blank string")
        if (
            isinstance(heartbeat_interval, bool)
            or not isinstance(heartbeat_interval, int | float)
            or heartbeat_interval <= 0
        ):
            raise ValueError("heartbeat_interval must be strictly positive")
        self._url = url
        self._heartbeat_interval = heartbeat_interval
        self._connection_factory = connection_factory or _default_connection_factory

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        """Return a single-lifecycle normalized stream for selected outcome tokens."""
        if not token_ids:
            raise PolymarketSubscriptionError("at least one token ID is required")
        if len(set(token_ids)) != len(token_ids):
            raise PolymarketSubscriptionError("duplicate token IDs are not allowed")
        return self._stream(market_id, token_ids)

    async def _stream(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        """Connect once, subscribe once, and yield normalized public observations."""
        try:
            connection_context = self._connection_factory(self._url)
            async with connection_context as connection:
                await self._subscribe(connection, token_ids)
                async for event in self._receive_events(
                    connection, market_id, token_ids
                ):
                    yield event
        except asyncio.CancelledError:
            raise
        except PolymarketPayloadError:
            raise
        except PolymarketWebSocketError:
            raise
        except Exception as error:
            raise PolymarketWebSocketError(
                "market WebSocket connection failed"
            ) from error

    async def _subscribe(
        self, connection: _WebSocketConnection, token_ids: tuple[TokenId, ...]
    ) -> None:
        """Send the sole public token subscription immediately after connecting."""
        message = json.dumps(
            {
                "assets_ids": [token_id.value for token_id in token_ids],
                "type": "market",
            },
            separators=(",", ":"),
        )
        try:
            await connection.send(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise PolymarketWebSocketError(
                "market WebSocket subscription failed"
            ) from error

    async def _receive_events(
        self,
        connection: _WebSocketConnection,
        market_id: MarketId,
        token_ids: tuple[TokenId, ...],
    ) -> AsyncIterator[MarketDataEvent]:
        """Receive frames while watching heartbeat failures without leaking tasks."""
        heartbeat_task = asyncio.create_task(self._heartbeat(connection))
        receive_task = asyncio.create_task(connection.recv())
        try:
            while True:
                done, _ = await asyncio.wait(
                    {heartbeat_task, receive_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if heartbeat_task in done:
                    try:
                        heartbeat_task.result()
                    except Exception:
                        yield _disconnected_event()
                        return
                    yield _disconnected_event()
                    return
                if receive_task in done:
                    try:
                        raw_message = receive_task.result()
                    except Exception:
                        yield _disconnected_event()
                        return
                    receive_task = asyncio.create_task(connection.recv())
                    for event in parse_market_data_message(
                        raw_message, market_id, token_ids
                    ):
                        yield event
        finally:
            await _cancel_task(receive_task)
            await _cancel_task(heartbeat_task)

    async def _heartbeat(self, connection: _WebSocketConnection) -> None:
        """Send Polymarket's required application-level heartbeat text."""
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            await connection.send("PING")


def _default_connection_factory(
    url: str,
) -> AbstractAsyncContextManager[_WebSocketConnection]:
    """Open a modern asyncio WebSocket client with protocol pings disabled."""
    return cast(
        AbstractAsyncContextManager[_WebSocketConnection],
        connect(url, ping_interval=None),
    )


def _disconnected_event() -> MarketDataDisconnected:
    """Build a sanitized transport-disconnection event with an aware UTC timestamp."""
    return MarketDataDisconnected("market WebSocket disconnected", datetime.now(UTC))


async def _cancel_task(task: asyncio.Task[object]) -> None:
    """Cancel and await a background task so iteration never leaves it orphaned."""
    if task.done():
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
