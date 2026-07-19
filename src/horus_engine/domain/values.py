"""Immutable financial value objects and price-tick utilities."""

from dataclasses import dataclass
from decimal import Decimal
from typing import TypeAlias

from .errors import (
    InvalidMoney,
    InvalidNonNegativeQuantity,
    InvalidPrice,
    InvalidQuantity,
    InvalidTickSize,
    TickAlignmentError,
)

DecimalInput: TypeAlias = Decimal | int | str


def _to_decimal(value: DecimalInput, error_type: type[Exception]) -> Decimal:
    """Convert a supported input into a finite Decimal."""
    if isinstance(value, bool) or not isinstance(value, Decimal | int | str):
        raise error_type(
            "value must be a Decimal, int, or str; floats are not accepted"
        )

    try:
        decimal_value = Decimal(value)
    except Exception as error:
        raise error_type("value must be a valid Decimal") from error

    if not decimal_value.is_finite():
        raise error_type("value must be finite")
    return decimal_value


@dataclass(frozen=True, order=True, init=False)
class Price:
    """A finite binary prediction-market price in the inclusive range zero to one."""

    value: Decimal

    def __init__(self, value: DecimalInput) -> None:
        """Validate and normalize the supplied value to Decimal."""
        value = _to_decimal(value, InvalidPrice)
        if not Decimal("0") <= value <= Decimal("1"):
            raise InvalidPrice("price must be between 0 and 1, inclusive")
        object.__setattr__(self, "value", value)

    def __str__(self) -> str:
        """Return the decimal price in a human-readable form."""
        return str(self.value)


@dataclass(frozen=True, order=True, init=False)
class Quantity:
    """A finite, strictly positive contract quantity."""

    value: Decimal

    def __init__(self, value: DecimalInput) -> None:
        """Validate and normalize the supplied value to Decimal."""
        value = _to_decimal(value, InvalidQuantity)
        if value <= Decimal("0"):
            raise InvalidQuantity("quantity must be greater than zero")
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, order=True, init=False)
class NonNegativeQuantity:
    """A finite contract quantity that may be zero.

    This type is used for quantities whose domain naturally includes zero, such
    as an order's filled or remaining quantity and aggregate order-book depth.
    ``Quantity`` remains strictly positive for quantities that must exist.
    """

    value: Decimal

    def __init__(self, value: DecimalInput) -> None:
        """Validate and normalize the supplied value to Decimal."""
        value = _to_decimal(value, InvalidNonNegativeQuantity)
        if value < Decimal("0"):
            raise InvalidNonNegativeQuantity(
                "quantity must be greater than or equal to zero"
            )
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, order=True, init=False)
class Money:
    """A finite monetary amount without an associated currency."""

    value: Decimal

    def __init__(self, value: DecimalInput) -> None:
        """Validate and normalize the supplied value to Decimal."""
        object.__setattr__(self, "value", _to_decimal(value, InvalidMoney))

    def __add__(self, other: "Money") -> "Money":
        """Return the sum of two monetary amounts."""
        if not isinstance(other, Money):
            return NotImplemented
        return Money(self.value + other.value)

    def __sub__(self, other: "Money") -> "Money":
        """Return the difference between two monetary amounts."""
        if not isinstance(other, Money):
            return NotImplemented
        return Money(self.value - other.value)


@dataclass(frozen=True, init=False)
class TickSize:
    """A finite allowed price increment greater than zero and at most one."""

    value: Decimal

    def __init__(self, value: DecimalInput) -> None:
        """Validate and normalize the supplied value to Decimal."""
        value = _to_decimal(value, InvalidTickSize)
        if not Decimal("0") < value <= Decimal("1"):
            raise InvalidTickSize("tick size must be greater than 0 and at most 1")
        object.__setattr__(self, "value", value)


def _decimal_exponent(value: Decimal) -> int:
    """Return the finite Decimal exponent as an integer."""
    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int)
    return exponent


def _scaled_integer(value: Decimal, exponent: int) -> int:
    """Represent a non-negative Decimal exactly at a common exponent."""
    digits = "".join(str(digit) for digit in value.as_tuple().digits) or "0"
    coefficient = int(digits)
    scale = _decimal_exponent(value) - exponent
    multiplier: int = 10**scale
    return coefficient * multiplier


def _decimal_from_scaled(value: int, exponent: int) -> Decimal:
    """Create a Decimal exactly from an integer and decimal exponent."""
    digits = tuple(int(digit) for digit in str(value))
    return Decimal((0, digits, exponent))


def _tick_components(price: Price, tick_size: TickSize) -> tuple[int, int, int]:
    """Return exact scaled integer components for a tick operation."""
    exponent = min(_decimal_exponent(price.value), _decimal_exponent(tick_size.value))
    return (
        _scaled_integer(price.value, exponent),
        _scaled_integer(tick_size.value, exponent),
        exponent,
    )


def is_tick_aligned(price: Price, tick_size: TickSize) -> bool:
    """Return whether price is an exact multiple of tick_size."""
    price_value, tick_value, _ = _tick_components(price, tick_size)
    return price_value % tick_value == 0


def round_price_down_to_tick(price: Price, tick_size: TickSize) -> Price:
    """Round price down to its greatest valid tick-aligned price."""
    price_value, tick_value, exponent = _tick_components(price, tick_size)
    rounded_value = (price_value // tick_value) * tick_value
    return Price(_decimal_from_scaled(rounded_value, exponent))


def round_price_up_to_tick(price: Price, tick_size: TickSize) -> Price:
    """Round price up to its smallest valid tick-aligned price.

    Raises TickAlignmentError when no such price exists within the binary range.
    """
    price_value, tick_value, exponent = _tick_components(price, tick_size)
    quotient, remainder = divmod(price_value, tick_value)
    rounded_value = (quotient + (remainder != 0)) * tick_value
    upper_bound = 10**-exponent
    if rounded_value > upper_bound:
        raise TickAlignmentError("rounding up would exceed the maximum price of 1")
    return Price(_decimal_from_scaled(rounded_value, exponent))
