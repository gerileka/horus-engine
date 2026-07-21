"""Tests for normalized exchange-neutral application events."""

from collections.abc import Callable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    ClientOrderId,
    ExchangeOrderId,
    InvalidEventText,
    InvalidEventTimestamp,
    InvalidTickSizeChange,
    MarketDataDisconnected,
    MarketDataReconnected,
    MarketId,
    OrderAccepted,
    OrderCancelled,
    OrderFilled,
    OrderPartiallyFilled,
    OrderRejected,
    PriceLevelChanged,
    TickSizeChanged,
    TokenId,
    TradeObserved,
)
from horus_engine.domain import (
    InvalidQuantity,
    NonNegativeQuantity,
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

EventFactory = Callable[[datetime], object]


def _book() -> OrderBook:
    """Build a concise immutable book for event construction."""
    return OrderBook(bids=[OrderBookLevel(Price("0.40"), Quantity("2"))])


def _market_data_event_factories() -> tuple[EventFactory, ...]:
    """Create every market-data event family from a supplied timestamp."""
    market_id = MarketId("market-1")
    token_id = TokenId("token-yes")
    return (
        lambda timestamp: BookSnapshotReceived(market_id, token_id, _book(), timestamp),
        lambda timestamp: PriceLevelChanged(
            market_id,
            token_id,
            Side.BUY,
            Price("0.40"),
            NonNegativeQuantity("3"),
            timestamp,
        ),
        lambda timestamp: TradeObserved(
            market_id,
            token_id,
            Side.SELL,
            Price("0.41"),
            Quantity("1"),
            timestamp,
        ),
        lambda timestamp: TickSizeChanged(
            market_id,
            token_id,
            TickSize("0.01"),
            TickSize("0.005"),
            timestamp,
        ),
        lambda timestamp: MarketDataDisconnected("temporary disconnect", timestamp),
        lambda timestamp: MarketDataReconnected(timestamp),
    )


def _order_event_factories() -> tuple[EventFactory, ...]:
    """Create every order-lifecycle event family from a supplied timestamp."""
    client_order_id = ClientOrderId("client-1")
    exchange_order_id = ExchangeOrderId("exchange-1")
    return (
        lambda timestamp: OrderAccepted(client_order_id, exchange_order_id, timestamp),
        lambda timestamp: OrderRejected(
            client_order_id, "insufficient liquidity", timestamp
        ),
        lambda timestamp: OrderPartiallyFilled(
            client_order_id,
            exchange_order_id,
            Quantity("1"),
            Price("0.40"),
            Quantity("1"),
            timestamp,
        ),
        lambda timestamp: OrderFilled(
            client_order_id,
            exchange_order_id,
            Quantity("2"),
            Price("0.41"),
            Quantity("3"),
            timestamp,
        ),
        lambda timestamp: OrderCancelled(client_order_id, exchange_order_id, timestamp),
    )


@pytest.mark.parametrize(
    "factory", _market_data_event_factories() + _order_event_factories()
)
def test_events_accept_timezone_aware_timestamps(factory: EventFactory) -> None:
    """Construct every event family from a timestamp with an explicit timezone."""
    assert factory(datetime(2026, 7, 19, 12, 0, tzinfo=UTC)) is not None


@pytest.mark.parametrize(
    "factory", _market_data_event_factories() + _order_event_factories()
)
def test_events_reject_naive_timestamps(factory: EventFactory) -> None:
    """Never silently attach UTC or another timezone to an event timestamp."""
    with pytest.raises(InvalidEventTimestamp):
        factory(datetime(2026, 7, 19, 12, 0))


def test_zero_quantity_price_level_change_explicitly_represents_deletion() -> None:
    """Use the existing zero-capable quantity rather than a deletion event type."""
    event = PriceLevelChanged(
        MarketId("market-1"),
        TokenId("token-yes"),
        Side.SELL,
        Price("0.60"),
        NonNegativeQuantity("0"),
        datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
    )
    assert event.quantity == NonNegativeQuantity("0")


def test_trade_quantity_remains_strictly_positive() -> None:
    """Keep observed trades incompatible with zero quantities."""
    with pytest.raises(InvalidQuantity):
        TradeObserved(
            MarketId("market-1"),
            TokenId("token-yes"),
            Side.BUY,
            Price("0.50"),
            Quantity("0"),
            datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        )


def test_tick_size_changed_is_immutable_and_requires_a_real_change() -> None:
    """Retain only valid observed tick-size transitions as immutable events."""
    timestamp = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)
    event = TickSizeChanged(
        MarketId("market-1"),
        TokenId("token-yes"),
        TickSize("0.01"),
        TickSize("0.005"),
        timestamp,
    )
    with pytest.raises(FrozenInstanceError):
        event.old_tick_size = TickSize("0.02")  # type: ignore[misc]
    with pytest.raises(InvalidTickSizeChange):
        TickSizeChanged(
            MarketId("market-1"),
            TokenId("token-yes"),
            TickSize("0.01"),
            TickSize("0.01"),
            timestamp,
        )


@pytest.mark.parametrize("reason", ["", "   "])
def test_order_rejected_requires_a_nonblank_reason(reason: str) -> None:
    """Keep rejection reasons useful until a stable error-code taxonomy exists."""
    with pytest.raises(InvalidEventText):
        OrderRejected(
            ClientOrderId("client-1"),
            reason,
            datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        )


def test_market_data_disconnect_reason_may_be_absent_but_not_blank() -> None:
    """Allow an unavailable transport to omit a reason without allowing blank text."""
    event = MarketDataDisconnected(None, datetime(2026, 7, 19, 12, 0, tzinfo=UTC))
    assert event.reason is None
    with pytest.raises(InvalidEventText):
        MarketDataDisconnected("   ", datetime(2026, 7, 19, 12, 0, tzinfo=UTC))


def test_events_are_immutable() -> None:
    """Prevent observed normalized events from being mutated after publication."""
    event = OrderAccepted(
        ClientOrderId("client-1"),
        ExchangeOrderId("exchange-1"),
        datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
    )
    with pytest.raises(FrozenInstanceError):
        event.accepted_at = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)  # type: ignore[misc]
