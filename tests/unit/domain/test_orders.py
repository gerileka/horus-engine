"""Tests for exchange-independent order models."""

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from horus_engine.domain import (
    InvalidFilledQuantity,
    InvalidNonNegativeQuantity,
    InvalidOrderIdentifier,
    InvalidOrderState,
    NonNegativeQuantity,
    Order,
    OrderIdentifier,
    OrderRequest,
    OrderStatus,
    Price,
    Quantity,
    Side,
    TickAlignmentError,
    TickSize,
)


def test_non_negative_quantity_accepts_zero_and_positive_values() -> None:
    """Represent zero-capable domain quantities without changing Quantity."""
    assert NonNegativeQuantity("0").value == Decimal("0")
    assert NonNegativeQuantity("1.25").value == Decimal("1.25")


@pytest.mark.parametrize("value", ["-0.01", "NaN", "Infinity", 0.5])
def test_non_negative_quantity_rejects_invalid_values(value: object) -> None:
    """Reject negative, non-finite, and float non-negative quantities."""
    with pytest.raises(InvalidNonNegativeQuantity):
        NonNegativeQuantity(value)  # type: ignore[arg-type]


def test_order_identifier_requires_a_non_blank_string() -> None:
    """Accept stable internal identifiers and reject blank or non-string input."""
    identifier = OrderIdentifier("internal-123")
    assert identifier.value == "internal-123"
    assert str(identifier) == "internal-123"
    with pytest.raises(InvalidOrderIdentifier):
        OrderIdentifier("   ")
    with pytest.raises(InvalidOrderIdentifier):
        OrderIdentifier(123)  # type: ignore[arg-type]


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_order_request_factory_creates_aligned_requests(side: Side) -> None:
    """Create both buy and sell requests when the supplied price is aligned."""
    request = OrderRequest.create(
        side=side,
        price=Price("0.42"),
        quantity=Quantity("3"),
        tick_size=TickSize("0.01"),
    )
    assert request.side is side
    assert request.price == Price("0.42")
    assert request.quantity == Quantity("3")


@pytest.mark.parametrize(
    ("price", "tick_size"),
    [
        ("0", "0.01"),
        ("1", "0.01"),
        ("0.375", "0.125"),
        ("0.42", "0.01"),
    ],
)
def test_order_request_accepts_boundary_and_common_tick_sizes(
    price: str, tick_size: str
) -> None:
    """Use existing exact tick validation for common increments and boundaries."""
    request = OrderRequest.create(
        Side.BUY, Price(price), Quantity("1"), TickSize(tick_size)
    )
    assert request.price == Price(price)


def test_order_request_rejects_non_aligned_prices() -> None:
    """Reject a request whose price is not an exact multiple of its tick size."""
    with pytest.raises(TickAlignmentError):
        OrderRequest.create(Side.SELL, Price("0.421"), Quantity("1"), TickSize("0.01"))
    with pytest.raises(TickAlignmentError):
        OrderRequest(Side.SELL, Price("0.421"), Quantity("1"), TickSize("0.01"))


def test_order_request_is_immutable() -> None:
    """Keep a validated request unchanged after construction."""
    request = OrderRequest.create(
        Side.BUY, Price("0.42"), Quantity("1"), TickSize("0.01")
    )
    with pytest.raises(FrozenInstanceError):
        request.price = Price("0.43")  # type: ignore[misc]


@pytest.mark.parametrize(
    ("status", "filled", "remaining"),
    [
        (OrderStatus.PENDING, "0", "5"),
        (OrderStatus.OPEN, "0", "5"),
        (OrderStatus.PARTIALLY_FILLED, "2", "3"),
        (OrderStatus.FILLED, "5", "0"),
        (OrderStatus.CANCELLED, "2", "3"),
        (OrderStatus.REJECTED, "0", "5"),
    ],
)
def test_order_accepts_lifecycle_consistent_quantities(
    status: OrderStatus, filled: str, remaining: str
) -> None:
    """Model each lifecycle state with its permitted quantity combination."""
    order = Order(
        identifier=OrderIdentifier("internal-123"),
        side=Side.BUY,
        price=Price("0.42"),
        quantity=Quantity("5"),
        filled_quantity=NonNegativeQuantity(filled),
        status=status,
    )
    assert order.remaining_quantity == NonNegativeQuantity(remaining)


@pytest.mark.parametrize(
    ("status", "filled", "error"),
    [
        (OrderStatus.FILLED, "2", InvalidOrderState),
        (OrderStatus.OPEN, "5", InvalidOrderState),
        (OrderStatus.PARTIALLY_FILLED, "0", InvalidOrderState),
        (OrderStatus.PARTIALLY_FILLED, "5", InvalidOrderState),
        (OrderStatus.PENDING, "1", InvalidOrderState),
        (OrderStatus.OPEN, "1", InvalidOrderState),
        (OrderStatus.REJECTED, "1", InvalidOrderState),
        (OrderStatus.CANCELLED, "5", InvalidOrderState),
        (OrderStatus.OPEN, "6", InvalidFilledQuantity),
    ],
)
def test_order_rejects_invalid_lifecycle_quantity_combinations(
    status: OrderStatus, filled: str, error: type[ValueError]
) -> None:
    """Reject fills beyond the original quantity and invalid lifecycle states."""
    with pytest.raises(error):
        Order(
            identifier=OrderIdentifier("internal-123"),
            side=Side.SELL,
            price=Price("0.58"),
            quantity=Quantity("5"),
            filled_quantity=NonNegativeQuantity(filled),
            status=status,
        )


def test_order_is_immutable() -> None:
    """Keep tracked order records immutable after construction."""
    order = Order(
        OrderIdentifier("internal-123"),
        Side.BUY,
        Price("0.42"),
        Quantity("5"),
        NonNegativeQuantity("0"),
        OrderStatus.OPEN,
    )
    with pytest.raises(FrozenInstanceError):
        order.status = OrderStatus.CANCELLED  # type: ignore[misc]
