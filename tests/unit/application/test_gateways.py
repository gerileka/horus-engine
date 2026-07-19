"""Static structural-typing checks for application gateway protocols."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime

from horus_engine.application import (
    BookSnapshotReceived,
    ClientOrderId,
    ExchangeOrderId,
    ExecutionGateway,
    Market,
    MarketCatalogGateway,
    MarketDataEvent,
    MarketDataGateway,
    MarketId,
    MarketStatus,
    OrderCancelled,
    OrderEvent,
    TokenId,
)
from horus_engine.domain import (
    NonNegativeQuantity,
    Order,
    OrderBook,
    OrderIdentifier,
    OrderRequest,
    OrderStatus,
    Price,
    Quantity,
    Side,
    TickSize,
)


def _market() -> Market:
    """Build stable metadata returned by the catalog fake."""
    return Market(
        MarketId("market-1"),
        "Will this contract resolve YES?",
        TokenId("yes-token"),
        TokenId("no-token"),
        TickSize("0.01"),
        Quantity("1"),
        MarketStatus.ACTIVE,
    )


class FakeMarketCatalogGateway:
    """A structural implementation with no exchange or network behavior."""

    async def list_markets(self) -> tuple[Market, ...]:
        """Return one fake market."""
        return (_market(),)

    async def get_market(self, market_id: MarketId) -> Market | None:
        """Return the fake market only when its identifier matches."""
        market = _market()
        return market if market.market_id == market_id else None


class FakeMarketDataGateway:
    """A structural implementation that produces local in-memory observations."""

    async def get_order_book(self, market_id: MarketId, token_id: TokenId) -> OrderBook:
        """Return an empty local snapshot."""
        return OrderBook()

    async def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        """Yield one normalized local snapshot for structural typing verification."""
        yield BookSnapshotReceived(
            market_id, token_ids[0], OrderBook(), datetime(2026, 7, 19, tzinfo=UTC)
        )


class FakeExecutionGateway:
    """A structural implementation that tracks no external exchange state."""

    async def submit_order(
        self, client_order_id: ClientOrderId, request: OrderRequest
    ) -> ExchangeOrderId:
        """Return a deterministic local exchange-order identifier."""
        return ExchangeOrderId(f"exchange-{client_order_id.value}")

    async def cancel_order(self, exchange_order_id: ExchangeOrderId) -> None:
        """Accept local cancellation without side effects."""

    async def cancel_all(self) -> None:
        """Accept local bulk cancellation without side effects."""

    async def list_open_orders(self) -> tuple[Order, ...]:
        """Return one local, unfilled tracked order."""
        return (
            Order(
                OrderIdentifier("internal-1"),
                Side.BUY,
                Price("0.40"),
                Quantity("1"),
                NonNegativeQuantity("0"),
                OrderStatus.OPEN,
            ),
        )

    async def stream_order_events(self) -> AsyncIterator[OrderEvent]:
        """Yield no lifecycle events from the local fake."""
        if False:
            yield OrderCancelled(
                ClientOrderId("client-1"),
                ExchangeOrderId("exchange-1"),
                datetime(2026, 7, 19, tzinfo=UTC),
            )


def _accept_catalog(gateway: MarketCatalogGateway) -> MarketCatalogGateway:
    """Require structural conformance at mypy type-check time."""
    return gateway


def _accept_market_data(gateway: MarketDataGateway) -> MarketDataGateway:
    """Require structural conformance at mypy type-check time."""
    return gateway


def _accept_execution(gateway: ExecutionGateway) -> ExecutionGateway:
    """Require structural conformance at mypy type-check time."""
    return gateway


def test_fake_gateways_satisfy_the_protocols_structurally() -> None:
    """Exercise typed assignments without protocol runtime checks or mocks."""
    assert isinstance(
        _accept_catalog(FakeMarketCatalogGateway()), FakeMarketCatalogGateway
    )
    assert isinstance(
        _accept_market_data(FakeMarketDataGateway()), FakeMarketDataGateway
    )
    assert isinstance(_accept_execution(FakeExecutionGateway()), FakeExecutionGateway)
