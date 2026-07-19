"""Tests for immutable aggregate order-book snapshots."""

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from horus_engine.domain import (
    DuplicatePriceLevel,
    InvalidPrice,
    InvalidQuantity,
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
)


def level(price: str, quantity: str) -> OrderBookLevel:
    """Build one concise level for snapshot tests."""
    return OrderBookLevel(Price(price), Quantity(quantity))


def test_order_book_level_is_an_immutable_value_object() -> None:
    """Represent aggregate liquidity by price and positive quantity only."""
    order_book_level = level("0.42", "3")
    assert order_book_level == OrderBookLevel(Price("0.42"), Quantity("3"))
    assert repr(order_book_level).startswith("OrderBookLevel(")
    with pytest.raises(FrozenInstanceError):
        order_book_level.quantity = Quantity("4")  # type: ignore[misc]


def test_order_book_level_delegates_invalid_price_to_existing_value_object() -> None:
    """Keep price and quantity validation with their existing value objects."""
    with pytest.raises(InvalidPrice):
        OrderBookLevel(Price("1.01"), Quantity("1"))
    with pytest.raises(InvalidQuantity):
        OrderBookLevel(Price("0.50"), Quantity("0"))


def test_order_book_normalizes_ordering_without_mutating_caller_lists() -> None:
    """Sort copied input into descending bids and ascending asks."""
    bids = [level("0.30", "2"), level("0.70", "1"), level("0.50", "3")]
    asks = [level("0.80", "2"), level("0.60", "1"), level("0.70", "3")]
    book = OrderBook(bids=bids, asks=asks)
    assert [entry.price for entry in book.bids] == [
        Price("0.70"),
        Price("0.50"),
        Price("0.30"),
    ]
    assert [entry.price for entry in book.asks] == [
        Price("0.60"),
        Price("0.70"),
        Price("0.80"),
    ]
    assert bids == [level("0.30", "2"), level("0.70", "1"), level("0.50", "3")]
    assert asks == [level("0.80", "2"), level("0.60", "1"), level("0.70", "3")]
    assert isinstance(book.bids, tuple)
    assert isinstance(book.asks, tuple)


@pytest.mark.parametrize(
    ("bids", "asks"),
    [
        ([level("0.42", "1"), level("0.42", "2")], []),
        ([], [level("0.58", "1"), level("0.58", "2")]),
    ],
)
def test_order_book_rejects_duplicate_prices_on_one_side(
    bids: list[OrderBookLevel], asks: list[OrderBookLevel]
) -> None:
    """Reject duplicate prices per side while allowing the opposite side's price."""
    with pytest.raises(DuplicatePriceLevel):
        OrderBook(bids=bids, asks=asks)


def test_order_book_permits_the_same_price_on_opposite_sides() -> None:
    """Represent a locked book rather than rejecting a shared bid and ask price."""
    book = OrderBook([level("0.50", "1")], [level("0.50", "2")])
    assert book.best_bid == Price("0.50")
    assert book.best_ask == Price("0.50")
    assert book.is_locked is True
    assert book.is_crossed is False


@pytest.mark.parametrize(
    ("bids", "asks", "best_bid", "best_ask"),
    [
        ([level("0.40", "1")], [level("0.60", "1")], Price("0.40"), Price("0.60")),
        ([level("0.40", "1")], [], Price("0.40"), None),
        ([], [level("0.60", "1")], None, Price("0.60")),
        ([], [], None, None),
    ],
)
def test_best_prices_allow_populated_one_sided_and_empty_books(
    bids: list[OrderBookLevel],
    asks: list[OrderBookLevel],
    best_bid: Price | None,
    best_ask: Price | None,
) -> None:
    """Return None only when the requested side has no visible levels."""
    book = OrderBook(bids, asks)
    assert book.best_bid == best_bid
    assert book.best_ask == best_ask


def test_midpoint_and_spread_use_exact_decimal_arithmetic() -> None:
    """Avoid float precision loss when calculating prices from both sides."""
    book = OrderBook([level("0.1", "1")], [level("0.3", "1")])
    assert book.midpoint == Price("0.2")
    assert book.spread == Decimal("0.2")


@pytest.mark.parametrize(
    "book",
    [
        OrderBook([level("0.40", "1")]),
        OrderBook(asks=[level("0.60", "1")]),
        OrderBook(),
    ],
)
def test_midpoint_and_spread_are_unavailable_for_one_sided_or_empty_books(
    book: OrderBook,
) -> None:
    """Require both best prices before calculating price-derived values."""
    assert book.midpoint is None
    assert book.spread is None


def test_crossed_book_is_represented_with_a_negative_spread() -> None:
    """Preserve a crossed observed state instead of rejecting it at construction."""
    book = OrderBook([level("0.70", "1")], [level("0.60", "1")])
    assert book.is_crossed is True
    assert book.is_locked is False
    assert book.spread == Decimal("-0.10")
    assert book.midpoint == Price("0.65")


def test_depth_queries_include_only_levels_in_the_requested_direction() -> None:
    """Calculate totals and cumulative depth without raw floats."""
    book = OrderBook(
        [level("0.70", "1.5"), level("0.50", "2.25"), level("0.30", "3")],
        [level("0.40", "1.25"), level("0.60", "2.5"), level("0.80", "4")],
    )
    assert book.total_bid_quantity.value == Decimal("6.75")
    assert book.total_ask_quantity.value == Decimal("7.75")
    assert book.cumulative_bid_quantity(Price("0.50")).value == Decimal("3.75")
    assert book.cumulative_bid_quantity(Price("0.55")).value == Decimal("1.5")
    assert book.cumulative_bid_quantity(Price("0.90")).value == Decimal("0")
    assert book.cumulative_ask_quantity(Price("0.60")).value == Decimal("3.75")
    assert book.cumulative_ask_quantity(Price("0.55")).value == Decimal("1.25")
    assert book.cumulative_ask_quantity(Price("0.10")).value == Decimal("0")


def test_empty_book_depth_is_zero_and_snapshot_cannot_be_mutated() -> None:
    """Return a zero-capable quantity and prevent changing snapshot fields."""
    book = OrderBook()
    assert book.total_bid_quantity.value == Decimal("0")
    assert book.total_ask_quantity.value == Decimal("0")
    assert book.cumulative_bid_quantity(Price("0.5")).value == Decimal("0")
    assert book.cumulative_ask_quantity(Price("0.5")).value == Decimal("0")
    with pytest.raises(FrozenInstanceError):
        book.bids = ()  # type: ignore[misc]
