"""Deterministic tests for the read-only Polymarket Gamma market catalog."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from decimal import Decimal
from pathlib import Path
from typing import TypeVar, cast

import httpx
import pytest

from horus_engine.application import (
    Market,
    MarketCatalogGateway,
    MarketId,
    MarketStatus,
)
from horus_engine.infrastructure.polymarket import (
    PolymarketHttpError,
    PolymarketMarketCatalogGateway,
    PolymarketPayloadError,
)

Handler = Callable[[httpx.Request], httpx.Response]
Result = TypeVar("Result")
_FIXTURES = Path(__file__).parents[3] / "fixtures" / "polymarket"


def _fixture_records() -> list[dict[str, object]]:
    """Load a minimal, realistic public Gamma response fixture."""
    payload = json.loads((_FIXTURES / "active_market.json").read_text())
    assert isinstance(payload, list) and all(isinstance(item, dict) for item in payload)
    return [cast(dict[str, object], item) for item in payload]


def _record(**changes: object) -> dict[str, object]:
    """Build an eligible Gamma record with explicit, compact defaults."""
    record = _fixture_records()[0]
    assert isinstance(record, dict)
    return {**record, **changes}


async def _with_gateway(
    handler: Handler,
    operation: Callable[[PolymarketMarketCatalogGateway], Awaitable[Result]],
) -> Result:
    """Run one operation with a caller-owned mock HTTPX client."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = PolymarketMarketCatalogGateway(client)
    try:
        return await operation(gateway)
    finally:
        await client.aclose()


def _response(records: object) -> Handler:
    """Return one JSON Gamma response for every received request."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=json.dumps(records), request=request)

    return handler


def _catalog(gateway: MarketCatalogGateway) -> MarketCatalogGateway:
    """Require structural conformance through the application protocol."""
    return gateway


def test_gateway_structurally_satisfies_market_catalog_protocol() -> None:
    """Keep the Polymarket adapter behind the exchange-neutral contract."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(_response([])))
    try:
        assert isinstance(_catalog(PolymarketMarketCatalogGateway(client)), object)
    finally:
        asyncio.run(client.aclose())


def test_list_markets_maps_fixture_with_decimal_strings_and_keeps_client_open() -> None:
    """Translate the public fixture without taking ownership of its client."""
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_response(_fixture_records()))
    )
    gateway = PolymarketMarketCatalogGateway(client)
    try:
        markets = asyncio.run(gateway.list_markets())
        assert isinstance(markets, tuple)
        assert markets[0].market_id == MarketId("0xcondition-1")
        assert markets[0].yes_token_id.value == "yes-token"
        assert markets[0].no_token_id.value == "no-token"
        assert markets[0].tick_size.value == Decimal("0.0025")
        assert markets[0].minimum_order_quantity.value == Decimal("5")
        assert markets[0].status is MarketStatus.ACTIVE
        assert not client.is_closed
    finally:
        asyncio.run(client.aclose())


def test_list_markets_maps_reversed_outcomes_and_json_numeric_values() -> None:
    """Match outcome labels rather than assuming Gamma's token ordering."""
    record = _record(
        outcomes='["No", "Yes"]',
        clobTokenIds='["no-token", "yes-token"]',
        orderPriceMinTickSize=0.0025,
        orderMinSize=7,
    )
    result = asyncio.run(
        _with_gateway(_response([record]), lambda gateway: gateway.list_markets())
    )
    market = result[0]
    assert market.yes_token_id.value == "yes-token"
    assert market.no_token_id.value == "no-token"
    assert market.tick_size.value == Decimal("0.0025")
    assert market.minimum_order_quantity.value == Decimal("7")


def test_list_markets_accepts_unencoded_gamma_lists() -> None:
    """Support Gamma responses where already-decoded list fields are supplied."""
    record = _record(outcomes=["Yes", "No"], clobTokenIds=["yes-token", "no-token"])
    result = asyncio.run(
        _with_gateway(_response([record]), lambda gateway: gateway.list_markets())
    )
    assert result[0].market_id.value == "0xcondition-1"


def test_list_markets_skips_non_clob_records_and_maps_suspended_status() -> None:
    """Deliberately ignore clearly ineligible records while retaining suspended ones."""
    records = [
        _record(enableOrderBook=False),
        _record(conditionId="0xcondition-2", enableOrderBook=None),
        _record(conditionId="0xcondition-3", active=True, acceptingOrders=False),
    ]
    result = asyncio.run(
        _with_gateway(_response(records), lambda gateway: gateway.list_markets())
    )
    assert tuple(market.market_id.value for market in result) == ("0xcondition-3",)
    assert result[0].status is MarketStatus.SUSPENDED


def test_list_markets_paginates_full_pages_in_stable_order() -> None:
    """Use limit and offset until a partial Gamma page terminates the traversal."""
    offsets: list[str] = []
    pages = {
        "0": [_record(conditionId="first"), _record(conditionId="second")],
        "2": [_record(conditionId="third")],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        offsets.append(request.url.params["offset"])
        assert request.url.params["active"] == "true"
        assert request.url.params["closed"] == "false"
        assert request.url.params["limit"] == "2"
        return httpx.Response(
            200, text=json.dumps(pages[request.url.params["offset"]]), request=request
        )

    async def exercise() -> tuple[tuple[Market, ...], bool]:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        gateway = PolymarketMarketCatalogGateway(client, page_size=2)
        try:
            markets = await gateway.list_markets()
            return markets, client.is_closed
        finally:
            await client.aclose()

    markets, client_was_closed = asyncio.run(exercise())
    assert offsets == ["0", "2"]
    assert tuple(market.market_id.value for market in markets) == (
        "first",
        "second",
        "third",
    )
    assert not client_was_closed


def test_get_market_uses_condition_id_filter_and_maps_closed_market() -> None:
    """Query condition IDs, not Gamma's unrelated internal market identifier."""
    requested: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(dict(request.url.params))
        return httpx.Response(
            200,
            text=json.dumps(
                [_record(closed=True, active=False, acceptingOrders=False)]
            ),
            request=request,
        )

    result = asyncio.run(
        _with_gateway(
            handler, lambda gateway: gateway.get_market(MarketId("0xcondition-1"))
        )
    )
    assert result is not None
    assert result.status is MarketStatus.CLOSED
    assert requested == [
        {"condition_ids": "0xcondition-1", "limit": "100", "offset": "0"}
    ]


def test_get_market_returns_none_for_no_or_ineligible_match() -> None:
    """Keep unknown and non-CLOB condition lookups out of application results."""
    no_match = asyncio.run(
        _with_gateway(
            _response([]), lambda gateway: gateway.get_market(MarketId("none"))
        )
    )
    non_clob = asyncio.run(
        _with_gateway(
            _response([_record(enableOrderBook=False)]),
            lambda gateway: gateway.get_market(MarketId("0xcondition-1")),
        )
    )
    assert no_match is None
    assert non_clob is None


def test_get_market_rejects_incompatible_duplicate_records() -> None:
    """Fail loudly when one condition ID has incompatible public metadata."""
    records = [_record(), _record(question="Changed question?")]
    with pytest.raises(PolymarketPayloadError, match="incompatible"):
        asyncio.run(
            _with_gateway(
                _response(records),
                lambda gateway: gateway.get_market(MarketId("0xcondition-1")),
            )
        )


def test_get_market_paginates_before_returning_compatible_duplicate() -> None:
    """Continue condition-ID pagination until Gamma returns a partial page."""
    pages = {"0": [_record()], "1": []}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, text=json.dumps(pages[request.url.params["offset"]]), request=request
        )

    async def exercise() -> object:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        gateway = PolymarketMarketCatalogGateway(client, page_size=1)
        try:
            return await gateway.get_market(MarketId("0xcondition-1"))
        finally:
            await client.aclose()

    result = asyncio.run(exercise())
    assert result is not None


@pytest.mark.parametrize(
    ("record", "message"),
    [
        (_record(conditionId=""), "conditionId"),
        (_record(question="   "), "question"),
        (_record(outcomes="not JSON"), "outcomes"),
        (_record(outcomes='"Yes"'), "outcomes"),
        (_record(outcomes='["Yes"]'), "exactly two outcomes"),
        (_record(outcomes='["Yes", "No", "Maybe"]'), "exactly two outcomes"),
        (_record(clobTokenIds="not JSON"), "clobTokenIds"),
        (_record(clobTokenIds='"yes-token"'), "clobTokenIds"),
        (_record(outcomes='["Yes", "No"]', clobTokenIds='["yes-token"]'), "counts"),
        (_record(outcomes='["Yes", "Maybe"]'), "YES and one NO"),
        (_record(clobTokenIds='["same", "same"]'), "duplicate"),
        (_record(orderPriceMinTickSize=None), "orderPriceMinTickSize"),
        (_record(orderPriceMinTickSize="0"), "market metadata"),
        (_record(orderPriceMinTickSize="NaN"), "orderPriceMinTickSize"),
        (_record(orderPriceMinTickSize="not-a-number"), "orderPriceMinTickSize"),
        (_record(orderMinSize=None), "orderMinSize"),
        (_record(orderMinSize="0"), "market metadata"),
    ],
)
def test_list_markets_rejects_malformed_eligible_records(
    record: dict[str, object], message: str
) -> None:
    """Reject bad records that assert order-book eligibility with concise context."""
    with pytest.raises(PolymarketPayloadError, match=message):
        asyncio.run(
            _with_gateway(_response([record]), lambda gateway: gateway.list_markets())
        )


@pytest.mark.parametrize(
    "document",
    [
        "not JSON",
        json.dumps({"markets": []}),
        json.dumps(["not an object"]),
    ],
)
def test_list_markets_rejects_invalid_top_level_gamma_documents(document: str) -> None:
    """Require a JSON list whose members are market objects."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=document, request=request)

    with pytest.raises(PolymarketPayloadError):
        asyncio.run(_with_gateway(handler, lambda gateway: gateway.list_markets()))


@pytest.mark.parametrize("status_code", [400, 429, 500])
def test_http_failures_are_translated_to_infrastructure_errors(
    status_code: int,
) -> None:
    """Keep public HTTP failures distinct from application and domain errors."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, request=request)

    with pytest.raises(PolymarketHttpError, match=str(status_code)):
        asyncio.run(_with_gateway(handler, lambda gateway: gateway.list_markets()))


def test_transport_timeout_is_translated_to_an_infrastructure_error() -> None:
    """Expose request failures without retries or response-body logging."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    with pytest.raises(PolymarketHttpError, match="request failed"):
        asyncio.run(_with_gateway(handler, lambda gateway: gateway.list_markets()))


@pytest.mark.parametrize(
    ("base_url", "page_size"),
    [("", 100), ("https://gamma.example", 0), ("https://gamma.example", True)],
)
def test_constructor_rejects_invalid_configuration(
    base_url: str, page_size: int
) -> None:
    """Require explicit, usable adapter configuration without environment state."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(_response([])))
    try:
        with pytest.raises(ValueError):
            PolymarketMarketCatalogGateway(
                client, base_url=base_url, page_size=page_size
            )
    finally:
        asyncio.run(client.aclose())
