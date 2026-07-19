"""Enumerations shared by exchange-independent domain models."""

from enum import Enum


class Side(str, Enum):
    """The direction of an order or trade."""

    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    """Lifecycle states used by exchange-independent orders."""

    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class Outcome(str, Enum):
    """The possible outcome of a binary prediction-market contract."""

    YES = "YES"
    NO = "NO"
