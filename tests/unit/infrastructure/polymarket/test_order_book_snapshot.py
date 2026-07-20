"""Deterministic tests for read-only Polymarket CLOB book snapshots."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import FrozenInstanceError
from decimal import Decimal
from typing import TypeVar

import httpx
import pytest

from horus_engine.application import MarketId, OrderBookSnapshotGateway, TokenId
from horus_engine.domain import OrderBook
from horus_engine.infrastructure.polymarket import (
    PolymarketHttpError,
    PolymarketOrderBookSnapshotGateway,
    PolymarketPayloadError,
)

Handler = Callable[[httpx.Request], httpx.Response]
Result = TypeVar("Result")
_MARKET_ID = MarketId("0xcondition-1")
_TOKEN_ID = TokenId("yes-token")


def _payload(**changes: object) -> dict[str, object]:
    """Build a compact realistic CLOB book response."""
    return {
        "market": _MARKET_ID.value,
        "asset_id": _TOKEN_ID.value,
        "tick_size": "0.0025",
        "bids": [{"price": "0.5000", "size": "12.5"}],
        "asks": [{"price": "0.5050", "size": "4.25"}],
        **changes,
    }


def _response(payload: object, status_code: int = 200) -> Handler:
    """Return a JSON CLOB response for every received request."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, text=json.dumps(payload), request=request)

    return handler


async def _with_gateway(
    handler: Handler,
    operation: Callable[[PolymarketOrderBookSnapshotGateway], Awaitable[Result]],
) -> Result:
    """Run one operation with a caller-owned mock HTTPX client."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = PolymarketOrderBookSnapshotGateway(client)
    try:
        return await operation(gateway)
    finally:
        await client.aclose()


def _snapshot_gateway(gateway: OrderBookSnapshotGateway) -> OrderBookSnapshotGateway:
    """Require snapshot-only structural conformance at mypy type-check time."""
    return gateway


def test_get_order_book_maps_unsorted_levels_and_keeps_client_open() -> None:
    """Request the CLOB endpoint and return a normalized immutable snapshot."""
    observed_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed_requests.append(request)
        return httpx.Response(
            200,
            text=json.dumps(
                _payload(
                    hash="ignored by the domain boundary",
                    bids=[
                        {"price": "0.4950", "size": "1.25"},
                        {"price": "0.5000", "size": "12.5"},
                    ],
                    asks=[
                        {"price": "0.5100", "size": "2"},
                        {"price": "0.5050", "size": "4.25"},
                    ],
                )
            ),
            request=request,
        )

    async def exercise() -> tuple[OrderBook, bool]:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        gateway = PolymarketOrderBookSnapshotGateway(client)
        try:
            book = await gateway.get_order_book(_MARKET_ID, _TOKEN_ID)
            return book, client.is_closed
        finally:
            await client.aclose()

    book, client_was_closed = asyncio.run(exercise())
    assert observed_requests[0].url.path == "/book"
    assert dict(observed_requests[0].url.params) == {"token_id": "yes-token"}
    assert tuple(level.price.value for level in book.bids) == (
        Decimal("0.5000"),
        Decimal("0.4950"),
    )
    assert tuple(level.quantity.value for level in book.asks) == (
        Decimal("4.25"),
        Decimal("2"),
    )
    assert not client_was_closed
    with pytest.raises(FrozenInstanceError):
        book.bids = ()  # type: ignore[misc]


def test_get_order_book_accepts_json_numbers_without_float_constructors() -> None:
    """Decode JSON numeric tokens as Decimal before creating domain values."""
    result = asyncio.run(
        _with_gateway(
            _response(
                _payload(
                    tick_size=0.0025,
                    bids=[{"price": 0.5, "size": 1.25}],
                    asks=[{"price": 0.5025, "size": 2}],
                )
            ),
            lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
        )
    )
    assert result.bids[0].price.value == Decimal("0.5")
    assert result.bids[0].quantity.value == Decimal("1.25")
    assert result.asks[0].price.value == Decimal("0.5025")


@pytest.mark.parametrize(
    ("bids", "asks"),
    [([], [{"price": "0.5050", "size": "1"}]), ([], [])],
)
def test_get_order_book_accepts_one_sided_and_empty_books(
    bids: list[object], asks: list[object]
) -> None:
    """Preserve valid CLOB books even when one or both sides are empty."""
    result = asyncio.run(
        _with_gateway(
            _response(_payload(bids=bids, asks=asks)),
            lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
        )
    )
    assert len(result.bids) == len(bids)
    assert len(result.asks) == len(asks)


@pytest.mark.parametrize(
    ("bid_price", "ask_price", "attribute"),
    [("0.5000", "0.5000", "is_locked"), ("0.5050", "0.5000", "is_crossed")],
)
def test_get_order_book_preserves_locked_and_crossed_observations(
    bid_price: str, ask_price: str, attribute: str
) -> None:
    """Do not discard potentially useful observed exchange state."""
    result = asyncio.run(
        _with_gateway(
            _response(
                _payload(
                    bids=[{"price": bid_price, "size": "1"}],
                    asks=[{"price": ask_price, "size": "1"}],
                )
            ),
            lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
        )
    )
    assert getattr(result, attribute)


def test_gateway_structurally_satisfies_snapshot_protocol() -> None:
    """Keep the concrete adapter scoped to the snapshot-only contract."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(_response(_payload())))
    try:
        gateway = _snapshot_gateway(PolymarketOrderBookSnapshotGateway(client))
        assert isinstance(gateway, PolymarketOrderBookSnapshotGateway)
    finally:
        asyncio.run(client.aclose())


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"market": "different-market"}, "market does not match"),
        ({"asset_id": "different-token"}, "asset_id does not match"),
        ({"market": None}, "market"),
        ({"market": "   "}, "market"),
        ({"market": "invalid\x00market"}, "invalid identity"),
        ({"asset_id": None}, "asset_id"),
        ({"asset_id": "   "}, "asset_id"),
    ],
)
def test_get_order_book_rejects_invalid_or_mismatched_identity(
    changes: dict[str, object], message: str
) -> None:
    """Reject a book that cannot be proven to belong to the request."""
    with pytest.raises(PolymarketPayloadError, match=message):
        asyncio.run(
            _with_gateway(
                _response(_payload(**changes)),
                lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
            )
        )


@pytest.mark.parametrize("field", ["market", "asset_id", "bids", "asks", "tick_size"])
def test_get_order_book_rejects_missing_required_fields(field: str) -> None:
    """Require every response field needed to identify and map a CLOB book."""
    payload = _payload()
    del payload[field]
    with pytest.raises(PolymarketPayloadError, match=field):
        asyncio.run(
            _with_gateway(
                _response(payload),
                lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
            )
        )


@pytest.mark.parametrize(
    "document",
    ["not JSON", json.dumps([])],
)
def test_get_order_book_rejects_malformed_or_nonobject_json(document: str) -> None:
    """Require a valid JSON object at the CLOB boundary."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=document, request=request)

    with pytest.raises(PolymarketPayloadError):
        asyncio.run(
            _with_gateway(
                handler, lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID)
            )
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"bids": None}, "bids"),
        ({"asks": None}, "asks"),
        ({"bids": {}}, "bids"),
        ({"asks": {}}, "asks"),
        ({"tick_size": None}, "tick_size"),
        ({"tick_size": "0"}, "tick_size"),
        ({"tick_size": "NaN"}, "tick_size"),
        ({"tick_size": "not-a-number"}, "tick_size"),
    ],
)
def test_get_order_book_rejects_invalid_top_level_fields(
    changes: dict[str, object], message: str
) -> None:
    """Require CLOB book fields used to construct the domain snapshot."""
    with pytest.raises(PolymarketPayloadError, match=message):
        asyncio.run(
            _with_gateway(
                _response(_payload(**changes)),
                lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
            )
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"bids": ["not an object"]}, "bids"),
        ({"asks": ["not an object"]}, "asks"),
        ({"bids": [{"size": "1"}]}, "price"),
        ({"asks": [{"price": "0.5000"}]}, "size"),
        ({"bids": [{"price": "not-a-price", "size": "1"}]}, "price"),
        ({"bids": [{"price": "1.1", "size": "1"}]}, "price levels"),
        ({"asks": [{"price": "0.5000", "size": "not-a-size"}]}, "size"),
        ({"bids": [{"price": "0.5000", "size": "0"}]}, "price levels"),
        ({"asks": [{"price": "0.5000", "size": "-1"}]}, "price levels"),
        ({"bids": [{"price": True, "size": "1"}]}, "price"),
        ({"asks": [{"price": "0.5000", "size": True}]}, "size"),
        ({"bids": [{"price": "0.5001", "size": "1"}]}, "aligned"),
        (
            {
                "bids": [
                    {"price": "0.5000", "size": "1"},
                    {"price": "0.5000", "size": "2"},
                ]
            },
            "price levels",
        ),
        (
            {
                "asks": [
                    {"price": "0.5050", "size": "1"},
                    {"price": "0.5050", "size": "2"},
                ]
            },
            "price levels",
        ),
    ],
)
def test_get_order_book_rejects_invalid_levels(
    changes: dict[str, object], message: str
) -> None:
    """Translate malformed external levels to payload failures without repair."""
    with pytest.raises(PolymarketPayloadError, match=message):
        asyncio.run(
            _with_gateway(
                _response(_payload(**changes)),
                lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
            )
        )


@pytest.mark.parametrize("status_code", [400, 404, 429, 500])
def test_get_order_book_translates_all_http_failures(status_code: int) -> None:
    """Keep public HTTP failures distinct from malformed successful payloads."""
    with pytest.raises(PolymarketHttpError, match=str(status_code)):
        asyncio.run(
            _with_gateway(
                _response(_payload(), status_code),
                lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID),
            )
        )


def test_get_order_book_translates_transport_failures() -> None:
    """Expose request failures without retries or response-body logging."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    with pytest.raises(PolymarketHttpError, match="request failed"):
        asyncio.run(
            _with_gateway(
                handler, lambda gateway: gateway.get_order_book(_MARKET_ID, _TOKEN_ID)
            )
        )


@pytest.mark.parametrize("base_url", ["", "   "])
def test_constructor_rejects_blank_base_url(base_url: str) -> None:
    """Require an explicit usable CLOB base URL without environment state."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(_response(_payload())))
    try:
        with pytest.raises(ValueError):
            PolymarketOrderBookSnapshotGateway(client, base_url)
    finally:
        asyncio.run(client.aclose())


def test_constructor_normalizes_only_a_trailing_base_url_slash() -> None:
    """Avoid a double slash without otherwise rewriting caller configuration."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text=json.dumps(_payload()), request=request)

    async def exercise() -> OrderBook:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        gateway = PolymarketOrderBookSnapshotGateway(
            client, base_url="https://clob.example/"
        )
        try:
            return await gateway.get_order_book(_MARKET_ID, _TOKEN_ID)
        finally:
            await client.aclose()

    result = asyncio.run(exercise())
    assert result.best_bid is not None
    assert str(requests[0].url) == "https://clob.example/book?token_id=yes-token"
