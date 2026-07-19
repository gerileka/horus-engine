"""Application boundaries implemented by future exchange adapters."""

from collections.abc import AsyncIterator
from typing import Protocol

from horus_engine.domain import Order, OrderBook, OrderRequest

from .events import MarketDataEvent, OrderEvent
from .models import ClientOrderId, ExchangeOrderId, Market, MarketId, TokenId


class MarketCatalogGateway(Protocol):
    """Provide exchange-neutral metadata for available markets."""

    async def list_markets(self) -> tuple[Market, ...]:
        """Return all currently available markets as immutable metadata."""
        ...

    async def get_market(self, market_id: MarketId) -> Market | None:
        """Return one market when it is known to the venue, otherwise ``None``."""
        ...


class MarketDataGateway(Protocol):
    """Provide snapshots and a normalized stream of market-data events."""

    async def get_order_book(self, market_id: MarketId, token_id: TokenId) -> OrderBook:
        """Return the authoritative current order-book snapshot for a token."""
        ...

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        """Return an asynchronous iterator of normalized market-data events."""
        ...


class ExecutionGateway(Protocol):
    """Submit, cancel, inspect, and observe exchange orders through one boundary."""

    async def submit_order(
        self, client_order_id: ClientOrderId, request: OrderRequest
    ) -> ExchangeOrderId:
        """Submit one limit-order request and return its exchange identifier."""
        ...

    async def cancel_order(self, exchange_order_id: ExchangeOrderId) -> None:
        """Request cancellation of one exchange order."""
        ...

    async def cancel_all(self) -> None:
        """Request cancellation of all known exchange orders."""
        ...

    async def list_open_orders(self) -> tuple[Order, ...]:
        """Return currently open orders as an immutable collection."""
        ...

    def stream_order_events(self) -> AsyncIterator[OrderEvent]:
        """Return an asynchronous iterator of normalized order lifecycle events."""
        ...
