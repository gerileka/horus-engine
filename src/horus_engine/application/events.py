"""Normalized immutable events emitted by exchange-facing adapters."""

from dataclasses import dataclass
from datetime import datetime
from typing import TypeAlias

from horus_engine.domain import (
    NonNegativeQuantity,
    OrderBook,
    Price,
    Quantity,
    Side,
)

from .errors import InvalidEventText, InvalidEventTimestamp
from .models import (
    ClientOrderId,
    ExchangeOrderId,
    MarketId,
    TokenId,
    _validate_nonblank_text,
)


def _validate_aware_timestamp(timestamp: datetime) -> None:
    """Reject naive timestamps without silently assigning a timezone.

    Adapters should normalize timestamps to UTC where practical, while event
    contracts preserve any valid timezone-aware ``datetime`` they receive.
    """
    if not isinstance(timestamp, datetime) or (
        timestamp.tzinfo is None or timestamp.utcoffset() is None
    ):
        raise InvalidEventTimestamp("event timestamps must be timezone-aware")


@dataclass(frozen=True)
class BookSnapshotReceived:
    """An authoritative order-book snapshot observed for one outcome token."""

    market_id: MarketId
    token_id: TokenId
    book: OrderBook
    observed_at: datetime

    def __post_init__(self) -> None:
        """Require an observed time with explicit timezone information."""
        _validate_aware_timestamp(self.observed_at)


@dataclass(frozen=True)
class PriceLevelChanged:
    """A change to one order-book level; quantity zero removes that level."""

    market_id: MarketId
    token_id: TokenId
    side: Side
    price: Price
    quantity: NonNegativeQuantity
    observed_at: datetime

    def __post_init__(self) -> None:
        """Require an observed time with explicit timezone information."""
        _validate_aware_timestamp(self.observed_at)


@dataclass(frozen=True)
class TradeObserved:
    """A trade where BUY means the aggressor bought the token and SELL sold it."""

    market_id: MarketId
    token_id: TokenId
    aggressor_side: Side
    price: Price
    quantity: Quantity
    observed_at: datetime

    def __post_init__(self) -> None:
        """Require an observed time with explicit timezone information."""
        _validate_aware_timestamp(self.observed_at)


@dataclass(frozen=True)
class MarketDataDisconnected:
    """Market-data transport became unavailable, optionally with a reason."""

    reason: str | None
    observed_at: datetime

    def __post_init__(self) -> None:
        """Require explicit timestamp timezone and nonblank supplied reason text."""
        _validate_aware_timestamp(self.observed_at)
        if self.reason is not None:
            _validate_nonblank_text(self.reason, InvalidEventText)


@dataclass(frozen=True)
class MarketDataReconnected:
    """Market-data transport became available again."""

    observed_at: datetime

    def __post_init__(self) -> None:
        """Require an observed time with explicit timezone information."""
        _validate_aware_timestamp(self.observed_at)


MarketDataEvent: TypeAlias = (
    BookSnapshotReceived
    | PriceLevelChanged
    | TradeObserved
    | MarketDataDisconnected
    | MarketDataReconnected
)


@dataclass(frozen=True)
class OrderAccepted:
    """An exchange accepted a client order and assigned its identifier."""

    client_order_id: ClientOrderId
    exchange_order_id: ExchangeOrderId
    accepted_at: datetime

    def __post_init__(self) -> None:
        """Require an acceptance time with explicit timezone information."""
        _validate_aware_timestamp(self.accepted_at)


@dataclass(frozen=True)
class OrderRejected:
    """An exchange rejected a client order with a human-readable reason."""

    client_order_id: ClientOrderId
    reason: str
    rejected_at: datetime

    def __post_init__(self) -> None:
        """Require a nonblank reason and explicitly timezone-aware timestamp."""
        _validate_nonblank_text(self.reason, InvalidEventText)
        _validate_aware_timestamp(self.rejected_at)


@dataclass(frozen=True)
class OrderPartiallyFilled:
    """A positive partial fill and its cumulative filled quantity."""

    client_order_id: ClientOrderId
    exchange_order_id: ExchangeOrderId
    fill_quantity: Quantity
    fill_price: Price
    cumulative_filled_quantity: Quantity
    observed_at: datetime

    def __post_init__(self) -> None:
        """Require an observed time with explicit timezone information."""
        _validate_aware_timestamp(self.observed_at)


@dataclass(frozen=True)
class OrderFilled:
    """A positive final fill and the total quantity filled for an order."""

    client_order_id: ClientOrderId
    exchange_order_id: ExchangeOrderId
    fill_quantity: Quantity
    fill_price: Price
    total_filled_quantity: Quantity
    observed_at: datetime

    def __post_init__(self) -> None:
        """Require an observed time with explicit timezone information."""
        _validate_aware_timestamp(self.observed_at)


@dataclass(frozen=True)
class OrderCancelled:
    """An exchange cancelled a known order."""

    client_order_id: ClientOrderId
    exchange_order_id: ExchangeOrderId
    cancelled_at: datetime

    def __post_init__(self) -> None:
        """Require a cancellation time with explicit timezone information."""
        _validate_aware_timestamp(self.cancelled_at)


OrderEvent: TypeAlias = (
    OrderAccepted | OrderRejected | OrderPartiallyFilled | OrderFilled | OrderCancelled
)
