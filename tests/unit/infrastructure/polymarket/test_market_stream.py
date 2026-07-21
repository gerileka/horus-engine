"""Deterministic tests for public Polymarket market-data WebSocket streaming."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataStreamGateway,
    MarketId,
    PriceLevelChanged,
    TickSizeChanged,
    TokenId,
    TradeObserved,
)
from horus_engine.domain import Side
from horus_engine.infrastructure.polymarket import (
    PolymarketMarketDataStreamGateway,
    PolymarketPayloadError,
    PolymarketSubscriptionError,
    PolymarketWebSocketError,
)
from horus_engine.infrastructure.polymarket.market_stream import (
    ConnectionFactory,
    _cancel_task,
    _default_connection_factory,
    _WebSocketConnection,
)
from horus_engine.infrastructure.polymarket.websocket_parsing import (
    parse_market_data_message,
)

_MARKET_ID = MarketId("condition-1")
_YES_TOKEN = TokenId("yes-token")
_NO_TOKEN = TokenId("no-token")
_TOKENS = (_YES_TOKEN, _NO_TOKEN)


def _event(event_type: str, **changes: object) -> dict[str, object]:
    """Build a minimal valid public WebSocket event payload."""
    return {
        "event_type": event_type,
        "market": _MARKET_ID.value,
        "asset_id": _YES_TOKEN.value,
        "timestamp": "1720000000123",
        **changes,
    }


def _parse(payload: object) -> tuple[object, ...]:
    """Parse one JSON payload with the stable test subscription identities."""
    return parse_market_data_message(json.dumps(payload), _MARKET_ID, _TOKENS)


def _stream_gateway(gateway: MarketDataStreamGateway) -> MarketDataStreamGateway:
    """Require structural conformance with the stream-only application protocol."""
    return gateway


async def _next_event(iterator: AsyncIterator[MarketDataEvent]) -> MarketDataEvent:
    """Await one event through a coroutine suitable for task creation."""
    return await anext(iterator)


def test_parser_maps_books_without_inventing_tick_validation() -> None:
    """Normalize an unsorted, fractional snapshot and keep observed book states."""
    events = _parse(
        _event(
            "book",
            bids=[{"price": "0.4", "size": "1.25"}, {"price": 0.5, "size": 2}],
            asks=[{"price": "0.5", "size": "3.5"}, {"price": "0.45", "size": 1}],
        )
    )
    event = cast(BookSnapshotReceived, events[0])
    assert tuple(level.price.value for level in event.book.bids) == (
        Decimal("0.5"),
        Decimal("0.4"),
    )
    assert event.book.is_crossed
    assert event.observed_at == datetime(2024, 7, 3, 9, 46, 40, 123000, tzinfo=UTC)


@pytest.mark.parametrize(
    ("bids", "asks", "locked"),
    [
        ([], [], False),
        ([], [{"price": "0.5", "size": "1"}], False),
        ([{"price": "0.5", "size": "1"}], [{"price": "0.5", "size": "1"}], True),
    ],
)
def test_parser_preserves_empty_one_sided_and_locked_books(
    bids: list[object], asks: list[object], locked: bool
) -> None:
    """Do not discard permitted public snapshots just because they look unusual."""
    event = cast(BookSnapshotReceived, _parse(_event("book", bids=bids, asks=asks))[0])
    assert len(event.book.bids) == len(bids)
    assert len(event.book.asks) == len(asks)
    assert event.book.is_locked is locked


@pytest.mark.parametrize(
    "changes",
    [
        {
            "bids": [{"price": "0.5", "size": "1"}, {"price": "0.5", "size": "2"}],
            "asks": [],
        },
        {
            "bids": [],
            "asks": [{"price": "0.5", "size": "1"}, {"price": "0.5", "size": "2"}],
        },
        {"bids": [{"price": "0.5", "size": "0"}], "asks": []},
        {"bids": [{"price": "0.5", "size": "-1"}], "asks": []},
        {"bids": ["bad"], "asks": []},
        {"bids": [{"size": "1"}], "asks": []},
    ],
)
def test_parser_rejects_invalid_book_levels(changes: dict[str, object]) -> None:
    """Reject invalid snapshot liquidity rather than repairing it."""
    with pytest.raises(PolymarketPayloadError, match="price levels|bids|price"):
        _parse(_event("book", **changes))


def test_parser_maps_ordered_price_changes_and_zero_deletions() -> None:
    """Emit one event per supplied change in payload order."""
    events = _parse(
        _event(
            "price_change",
            price_changes=[
                {
                    "asset_id": _NO_TOKEN.value,
                    "side": "SELL",
                    "price": "0.51",
                    "size": "0",
                },
                {
                    "asset_id": _YES_TOKEN.value,
                    "side": "BUY",
                    "price": 0.49,
                    "size": 2.5,
                },
            ],
        )
    )
    first, second = cast(tuple[PriceLevelChanged, PriceLevelChanged], events)
    assert (first.token_id, first.side, first.quantity.value) == (
        _NO_TOKEN,
        Side.SELL,
        Decimal("0"),
    )
    assert (second.token_id, second.side, second.quantity.value) == (
        _YES_TOKEN,
        Side.BUY,
        Decimal("2.5"),
    )


@pytest.mark.parametrize(
    "change",
    [
        {"asset_id": _YES_TOKEN.value, "side": "OTHER", "price": "0.5", "size": "1"},
        {"asset_id": _YES_TOKEN.value, "side": "BUY", "price": "0.5", "size": "-1"},
        {"asset_id": "other-token", "side": "BUY", "price": "0.5", "size": "1"},
        {"asset_id": _YES_TOKEN.value, "side": "BUY", "size": "1"},
    ],
)
def test_parser_rejects_invalid_price_changes(change: dict[str, object]) -> None:
    """Validate every nested asset and every normalized change field."""
    with pytest.raises(PolymarketPayloadError):
        _parse(_event("price_change", price_changes=[change]))


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_parser_maps_trades_with_aggressor_side(side: str) -> None:
    """Retain the venue's BUY/SELL aggressor meaning and fractional size."""
    event = cast(
        TradeObserved,
        _parse(_event("last_trade_price", side=side, price="0.42", size="1.25"))[0],
    )
    assert event.aggressor_side.value == side
    assert event.quantity.value == Decimal("1.25")


@pytest.mark.parametrize(
    "changes",
    [
        {"side": "BAD", "price": "0.5", "size": "1"},
        {"side": "BUY", "price": "1.1", "size": "1"},
        {"side": "BUY", "price": "0.5", "size": "0"},
    ],
)
def test_parser_rejects_invalid_trades(changes: dict[str, object]) -> None:
    """Trade price, side, and size must all have valid application types."""
    with pytest.raises(PolymarketPayloadError):
        _parse(_event("last_trade_price", **changes))


def test_parser_maps_tick_size_changes() -> None:
    """Expose a valid tick-size change as the application event addition."""
    event = cast(
        TickSizeChanged,
        _parse(_event("tick_size_change", old_tick_size="0.01", new_tick_size="0.005"))[
            0
        ],
    )
    assert event.old_tick_size.value == Decimal("0.01")
    assert event.new_tick_size.value == Decimal("0.005")


@pytest.mark.parametrize(
    "changes",
    [
        {"old_tick_size": "0", "new_tick_size": "0.01"},
        {"old_tick_size": "0.01", "new_tick_size": "0.01"},
        {"old_tick_size": "0.01", "new_tick_size": "0.005", "asset_id": "elsewhere"},
    ],
)
def test_parser_rejects_invalid_tick_size_changes(changes: dict[str, object]) -> None:
    """Never accept invalid, unchanged, or unsubscribed tick-size updates."""
    with pytest.raises(PolymarketPayloadError):
        _parse(_event("tick_size_change", **changes))


@pytest.mark.parametrize(
    "raw_message",
    [
        "not JSON",
        "1",
        "[1]",
        json.dumps({"event_type": "unknown"}),
        json.dumps({}),
        json.dumps(_event("book", market="other-market", bids=[], asks=[])),
        json.dumps(_event("book", market="\x00", bids=[], asks=[])),
        json.dumps(_event("book", market="   ", bids=[], asks=[])),
        json.dumps(_event("book", asset_id="other-token", bids=[], asks=[])),
        json.dumps(_event("book", asset_id="\x00", bids=[], asks=[])),
        json.dumps(_event("book", bids=[], asks=[], timestamp="1.5")),
        json.dumps(_event("book", bids=[], asks=[], timestamp=-1)),
        json.dumps(_event("book", bids=[], asks=[], timestamp=True)),
    ],
)
def test_parser_rejects_invalid_envelopes_and_identity(raw_message: str) -> None:
    """Reject unknown, malformed, unrelated, and non-millisecond messages."""
    with pytest.raises(PolymarketPayloadError):
        parse_market_data_message(raw_message, _MARKET_ID, _TOKENS)


def test_parser_handles_pong_lists_and_binary_frames() -> None:
    """Ignore PONG, flatten batches, and reject non-UTF-8 binary input."""
    assert parse_market_data_message("PONG", _MARKET_ID, _TOKENS) == ()
    assert _parse([]) == ()
    assert len(_parse([_event("book", bids=[], asks=[])])) == 1
    assert (
        len(
            parse_market_data_message(
                json.dumps(_event("book", bids=[], asks=[])).encode(),
                _MARKET_ID,
                _TOKENS,
            )
        )
        == 1
    )
    with pytest.raises(PolymarketPayloadError):
        parse_market_data_message(b"\xff", _MARKET_ID, _TOKENS)


class _FakeConnection(AbstractAsyncContextManager["_FakeConnection"]):
    """Small deterministic connection fake with an explicit incoming queue."""

    def __init__(
        self,
        incoming: tuple[object, ...] = (),
        failing_message: str | None = None,
        fail_any_send: bool = False,
    ) -> None:
        self.sent: list[str] = []
        self.closed = False
        self.ping_sent = asyncio.Event()
        self._failing_message = failing_message
        self._fail_any_send = fail_any_send
        self._incoming: asyncio.Queue[object] = asyncio.Queue()
        for item in incoming:
            self._incoming.put_nowait(item)

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def send(self, message: str) -> None:
        if self._fail_any_send or message == self._failing_message:
            raise RuntimeError("send failed")
        self.sent.append(message)
        if message == "PING":
            self.ping_sent.set()

    async def recv(self) -> str | bytes:
        item = await self._incoming.get()
        if isinstance(item, Exception):
            raise item
        return cast(str | bytes, item)


def _factory(connection: _FakeConnection, seen_urls: list[str]) -> ConnectionFactory:
    """Return a narrow connection factory that records its selected endpoint."""

    def factory(url: str) -> AbstractAsyncContextManager[object]:
        seen_urls.append(url)
        return cast(AbstractAsyncContextManager[object], connection)

    return cast(ConnectionFactory, factory)


def test_gateway_subscribes_in_token_order_and_satisfies_protocol() -> None:
    """Subscribe exactly once with no authentication or market fields."""
    connection = _FakeConnection((RuntimeError("closed"),))
    seen_urls: list[str] = []
    gateway = _stream_gateway(
        PolymarketMarketDataStreamGateway(
            connection_factory=_factory(connection, seen_urls)
        )
    )

    async def exercise() -> list[object]:
        return [
            event async for event in gateway.stream_market_data(_MARKET_ID, _TOKENS)
        ]

    events = asyncio.run(exercise())
    assert seen_urls == ["wss://ws-subscriptions-clob.polymarket.com/ws/market"]
    assert json.loads(connection.sent[0]) == {
        "assets_ids": ["yes-token", "no-token"],
        "type": "market",
    }
    assert isinstance(events[0], MarketDataDisconnected)
    assert connection.closed


def test_gateway_yields_received_events_before_disconnection() -> None:
    """Normalize received messages through the live adapter before transport ends."""
    connection = _FakeConnection(
        (
            json.dumps(_event("book", bids=[], asks=[])),
            RuntimeError("closed"),
        )
    )
    gateway = PolymarketMarketDataStreamGateway(
        connection_factory=_factory(connection, [])
    )

    async def exercise() -> list[MarketDataEvent]:
        return [
            event
            async for event in gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN,))
        ]

    events = asyncio.run(exercise())
    assert isinstance(events[0], BookSnapshotReceived)
    assert isinstance(events[1], MarketDataDisconnected)


def test_gateway_honours_custom_url_heartbeat_and_cleanup() -> None:
    """Send text PING through the configured lifecycle and close it cleanly."""
    connection = _FakeConnection()
    seen_urls: list[str] = []
    gateway = PolymarketMarketDataStreamGateway(
        "wss://example.test/market", 0.001, _factory(connection, seen_urls)
    )

    async def exercise() -> None:
        iterator = gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN,))
        next_event: asyncio.Task[MarketDataEvent] = asyncio.create_task(
            _next_event(iterator)
        )
        await asyncio.wait_for(connection.ping_sent.wait(), timeout=1)
        connection._incoming.put_nowait(RuntimeError("closed"))
        assert isinstance(await next_event, MarketDataDisconnected)

    asyncio.run(exercise())
    assert seen_urls == ["wss://example.test/market"]
    assert connection.sent[1] == "PING"
    assert connection.closed


def test_gateway_turns_heartbeat_failures_into_one_disconnection() -> None:
    """Heartbeat send failure ends the one connection lifecycle without leaking."""
    connection = _FakeConnection(failing_message="PING")
    gateway = PolymarketMarketDataStreamGateway(
        heartbeat_interval=0.001,
        connection_factory=_factory(connection, []),
    )

    async def exercise() -> list[MarketDataEvent]:
        return [
            event
            async for event in gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN,))
        ]

    events = asyncio.run(exercise())
    assert len(events) == 1
    assert isinstance(events[0], MarketDataDisconnected)
    assert connection.closed


def test_gateway_ends_when_its_heartbeat_task_ends_normally() -> None:
    """Defensively close the lifecycle if a replacement heartbeat ever returns."""

    class FiniteHeartbeatGateway(PolymarketMarketDataStreamGateway):
        async def _heartbeat(self, connection: _WebSocketConnection) -> None:
            return None

    connection = _FakeConnection()
    gateway = FiniteHeartbeatGateway(connection_factory=_factory(connection, []))

    async def exercise() -> list[MarketDataEvent]:
        return [
            event
            async for event in gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN,))
        ]

    events = asyncio.run(exercise())
    assert len(events) == 1
    assert isinstance(events[0], MarketDataDisconnected)


@pytest.mark.parametrize(
    ("url", "heartbeat_interval"),
    [("", 10), ("   ", 10), ("wss://example", 0), ("wss://example", True)],
)
def test_gateway_rejects_invalid_configuration(
    url: str, heartbeat_interval: object
) -> None:
    """Require a usable public endpoint and a strictly positive heartbeat interval."""
    with pytest.raises(ValueError):
        PolymarketMarketDataStreamGateway(url, cast(float, heartbeat_interval))


def test_gateway_rejects_invalid_subscription_input() -> None:
    """Fail before opening a connection for empty or duplicated token requests."""
    gateway = PolymarketMarketDataStreamGateway(
        connection_factory=lambda _: _FakeConnection()
    )
    with pytest.raises(PolymarketSubscriptionError):
        gateway.stream_market_data(_MARKET_ID, ())
    with pytest.raises(PolymarketSubscriptionError):
        gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN, _YES_TOKEN))


def test_gateway_preserves_payload_errors_and_wraps_connection_failures() -> None:
    """Keep malformed messages distinct while translating connection setup failure."""
    bad_connection = _FakeConnection(("not JSON",))
    bad_gateway = PolymarketMarketDataStreamGateway(
        connection_factory=_factory(bad_connection, [])
    )

    async def parse_failure() -> None:
        async for _ in bad_gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN,)):
            pass

    with pytest.raises(PolymarketPayloadError):
        asyncio.run(parse_failure())

    def broken_factory(_: str) -> AbstractAsyncContextManager[object]:
        raise RuntimeError("unavailable")

    broken_gateway = PolymarketMarketDataStreamGateway(
        connection_factory=cast(ConnectionFactory, broken_factory)
    )

    async def connection_failure() -> None:
        async for _ in broken_gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN,)):
            pass

    with pytest.raises(PolymarketWebSocketError, match="connection failed"):
        asyncio.run(connection_failure())

    subscription_failure = PolymarketMarketDataStreamGateway(
        connection_factory=_factory(_FakeConnection(fail_any_send=True), [])
    )

    async def subscribe_failure() -> None:
        async for _ in subscription_failure.stream_market_data(
            _MARKET_ID, (_YES_TOKEN,)
        ):
            pass

    with pytest.raises(PolymarketWebSocketError, match="subscription failed"):
        asyncio.run(subscribe_failure())


def test_gateway_propagates_cancellation_during_subscription() -> None:
    """Caller cancellation remains cancellation rather than a disconnect event."""
    started = asyncio.Event()

    class BlockingConnection(_FakeConnection):
        async def send(self, message: str) -> None:
            started.set()
            await asyncio.Event().wait()

    connection = BlockingConnection()
    gateway = PolymarketMarketDataStreamGateway(
        connection_factory=_factory(connection, [])
    )

    async def exercise() -> None:
        iterator = gateway.stream_market_data(_MARKET_ID, (_YES_TOKEN,))
        task: asyncio.Task[MarketDataEvent] = asyncio.create_task(_next_event(iterator))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise())
    assert connection.closed


def test_default_connection_factory_and_task_cleanup_helpers_are_covered() -> None:
    """Exercise the private default boundary without making a network connection."""
    assert _default_connection_factory("wss://example.test") is not None

    async def exercise() -> None:
        running = asyncio.create_task(asyncio.sleep(10))
        await _cancel_task(running)
        completed = asyncio.create_task(asyncio.sleep(0))
        await completed
        await _cancel_task(completed)

    asyncio.run(exercise())


@pytest.mark.parametrize("price", ["not-a-decimal", "NaN"])
def test_parser_rejects_invalid_decimal_fields(price: str) -> None:
    """Reject malformed and nonfinite financial fields before domain construction."""
    with pytest.raises(PolymarketPayloadError):
        _parse(_event("book", bids=[{"price": price, "size": "1"}], asks=[]))
