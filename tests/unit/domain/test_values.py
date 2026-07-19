"""Tests for financial domain values and tick operations."""

from decimal import Decimal, getcontext
from typing import cast

import pytest

from horus_engine.domain import (
    InvalidMoney,
    InvalidPrice,
    InvalidQuantity,
    InvalidTickSize,
    Money,
    Price,
    Quantity,
    TickAlignmentError,
    TickSize,
    is_tick_aligned,
    round_price_down_to_tick,
    round_price_up_to_tick,
)


@pytest.mark.parametrize("value", ["0", 1, Decimal("0.42")])
def test_price_accepts_valid_binary_range(value: Decimal | int | str) -> None:
    """Accept prices at both boundaries and within the binary range."""
    assert isinstance(Price(value).value, Decimal)


@pytest.mark.parametrize(
    "value",
    ["-0.01", "1.01", "not-a-number", "NaN", "Infinity", "-Infinity", 0.5, True],
)
def test_price_rejects_invalid_values(value: object) -> None:
    """Reject prices outside the allowed finite Decimal range."""
    with pytest.raises(InvalidPrice):
        Price(cast(Decimal | int | str, value))


def test_price_supports_equality_ordering_and_string_output() -> None:
    """Compare prices by value and render them clearly."""
    assert Price("0.4") == Price(Decimal("0.4"))
    assert Price("0.4") < Price("0.5")
    assert str(Price("0.40")) == "0.40"


@pytest.mark.parametrize("value", [2, "1.25"])
def test_quantity_accepts_integer_and_fractional_values(value: int | str) -> None:
    """Accept positive whole and fractional quantities."""
    assert Quantity(value).value == Decimal(str(value))


@pytest.mark.parametrize("value", [0, "-0.1", "NaN", "Infinity", "-Infinity", 0.5])
def test_quantity_rejects_invalid_values(value: object) -> None:
    """Reject non-positive, non-finite, and float quantities."""
    with pytest.raises(InvalidQuantity):
        Quantity(cast(Decimal | int | str, value))


@pytest.mark.parametrize("value", ["3.50", 0, "-1.25"])
def test_money_accepts_signed_finite_values(value: int | str) -> None:
    """Accept positive, zero, and negative monetary amounts."""
    assert Money(value).value == Decimal(str(value))


def test_money_addition_subtraction_and_comparison() -> None:
    """Support unambiguous money arithmetic and comparisons."""
    amount = Money("3.50")
    assert amount + Money("1.25") == Money("4.75")
    assert amount - Money("4.00") == Money("-0.50")
    assert Money("-1") < Money("0") < Money("1")


def test_money_rejects_arithmetic_with_non_money_values() -> None:
    """Avoid assigning arithmetic meaning to non-money values."""
    amount = Money("1")
    non_money = cast(Money, Decimal("1"))
    with pytest.raises(TypeError):
        amount + non_money
    with pytest.raises(TypeError):
        amount - non_money


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", 0.5])
def test_money_rejects_invalid_values(value: object) -> None:
    """Reject non-finite and float monetary amounts."""
    with pytest.raises(InvalidMoney):
        Money(cast(Decimal | int | str, value))


@pytest.mark.parametrize("value", ["0.01", Decimal("0.001")])
def test_tick_size_accepts_common_increments(value: Decimal | str) -> None:
    """Accept common finite tick sizes."""
    assert TickSize(value).value == Decimal(value)


@pytest.mark.parametrize(
    "value", [0, "-0.01", "1.01", "NaN", "Infinity", "-Infinity", 0.5]
)
def test_tick_size_rejects_invalid_values(value: object) -> None:
    """Reject invalid or float tick sizes."""
    with pytest.raises(InvalidTickSize):
        TickSize(cast(Decimal | int | str, value))


@pytest.mark.parametrize(
    ("price", "tick_size", "aligned"),
    [
        ("0", "0.01", True),
        ("1", "0.01", True),
        ("0.42", "0.01", True),
        ("0.421", "0.01", False),
        ("1", "0.3", False),
    ],
)
def test_tick_alignment(price: str, tick_size: str, aligned: bool) -> None:
    """Determine exact tick alignment, including both binary boundaries."""
    assert is_tick_aligned(Price(price), TickSize(tick_size)) is aligned


@pytest.mark.parametrize(
    ("price", "tick_size", "rounded_down", "rounded_up"),
    [
        ("0", "0.01", "0.00", "0.00"),
        ("0.42", "0.01", "0.420", "0.420"),
        ("0.421", "0.01", "0.420", "0.430"),
        ("1", "0.3", "0.9", None),
        (
            "0.999999999999999999999",
            "0.000000000000000000001",
            "0.999999999999999999999",
            "0.999999999999999999999",
        ),
    ],
)
def test_tick_rounding(
    price: str, tick_size: str, rounded_down: str, rounded_up: str | None
) -> None:
    """Round prices exactly without relying on the active Decimal precision."""
    domain_price = Price(price)
    domain_tick_size = TickSize(tick_size)
    assert round_price_down_to_tick(domain_price, domain_tick_size) == Price(
        rounded_down
    )
    if rounded_up is None:
        with pytest.raises(TickAlignmentError):
            round_price_up_to_tick(domain_price, domain_tick_size)
    else:
        assert round_price_up_to_tick(domain_price, domain_tick_size) == Price(
            rounded_up
        )


def test_tick_rounding_is_independent_of_active_decimal_precision() -> None:
    """Keep exact tick behavior when callers lower Decimal context precision."""
    previous_precision = getcontext().prec
    getcontext().prec = 4
    try:
        assert round_price_down_to_tick(Price("0.123456"), TickSize("0.0001")) == Price(
            "0.1234"
        )
        assert round_price_up_to_tick(Price("0.123456"), TickSize("0.0001")) == Price(
            "0.1235"
        )
    finally:
        getcontext().prec = previous_precision
