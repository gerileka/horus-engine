"""Tests for domain enumerations."""

from horus_engine.domain import Outcome, Side


def test_side_values() -> None:
    """Expose stable order-side values."""
    assert Side.BUY.value == "BUY"
    assert Side.SELL.value == "SELL"


def test_outcome_values() -> None:
    """Expose stable binary-outcome values."""
    assert Outcome.YES.value == "YES"
    assert Outcome.NO.value == "NO"
