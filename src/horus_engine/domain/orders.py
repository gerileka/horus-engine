"""Immutable, exchange-independent limit-order domain models."""

from dataclasses import dataclass

from .enums import OrderStatus, Side
from .errors import (
    InvalidFilledQuantity,
    InvalidOrderIdentifier,
    InvalidOrderState,
    TickAlignmentError,
)
from .values import NonNegativeQuantity, Price, Quantity, TickSize, is_tick_aligned


@dataclass(frozen=True, init=False)
class OrderIdentifier:
    """A validated internal identifier for a tracked order."""

    value: str

    def __init__(self, value: str) -> None:
        """Require a non-blank string identifier without changing its value."""
        if not isinstance(value, str) or not value.strip():
            raise InvalidOrderIdentifier("order identifier must be a non-blank string")
        object.__setattr__(self, "value", value)

    def __str__(self) -> str:
        """Return the identifier's stable string form."""
        return self.value


@dataclass(frozen=True, init=False)
class OrderRequest:
    """An exchange-neutral request to place one tick-aligned limit order.

    Creation requires a tick size so the price is always validated. The
    :meth:`create` factory is the preferred public entry point. The tick size is
    validation context rather than request state, so it is not retained.
    """

    side: Side
    price: Price
    quantity: Quantity

    def __init__(
        self,
        side: Side,
        price: Price,
        quantity: Quantity,
        tick_size: TickSize,
    ) -> None:
        """Create a request after checking exact price-tick alignment."""
        if not is_tick_aligned(price, tick_size):
            raise TickAlignmentError("order request price must align with tick size")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "quantity", quantity)

    @classmethod
    def create(
        cls,
        side: Side,
        price: Price,
        quantity: Quantity,
        tick_size: TickSize,
    ) -> "OrderRequest":
        """Create a request through the named tick-validation entry point."""
        return cls(side, price, quantity, tick_size)


@dataclass(frozen=True)
class Order:
    """A known order and its lifecycle-consistent fill state.

    Pending, open, and rejected orders must have no fills. Cancelled orders may
    have zero or partial fills, but cannot be fully filled; a fully filled order
    must use ``FILLED``.
    """

    identifier: OrderIdentifier
    side: Side
    price: Price
    quantity: Quantity
    filled_quantity: NonNegativeQuantity
    status: OrderStatus

    def __post_init__(self) -> None:
        """Reject fill quantities and lifecycle combinations that conflict."""
        if self.filled_quantity.value > self.quantity.value:
            raise InvalidFilledQuantity(
                "filled quantity cannot exceed original quantity"
            )
        remaining_quantity = self.remaining_quantity
        if self.status is OrderStatus.FILLED and remaining_quantity.value != 0:
            raise InvalidOrderState("filled orders must have no remaining quantity")
        if self.status is OrderStatus.OPEN and remaining_quantity.value == 0:
            raise InvalidOrderState("open orders must have remaining quantity")
        if self.status is OrderStatus.PARTIALLY_FILLED and (
            self.filled_quantity.value == 0 or remaining_quantity.value == 0
        ):
            raise InvalidOrderState(
                "partially filled orders require both filled and remaining quantity"
            )
        if self.status in {
            OrderStatus.PENDING,
            OrderStatus.OPEN,
            OrderStatus.REJECTED,
        } and (self.filled_quantity.value != 0):
            raise InvalidOrderState(f"{self.status.value} orders cannot have fills")
        if self.status is OrderStatus.CANCELLED and remaining_quantity.value == 0:
            raise InvalidOrderState("cancelled orders cannot be fully filled")

    @property
    def remaining_quantity(self) -> NonNegativeQuantity:
        """Return the unfilled portion of the original quantity."""
        return NonNegativeQuantity(self.quantity.value - self.filled_quantity.value)
