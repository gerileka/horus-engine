"""Enumerations shared by exchange-independent domain models."""

from enum import Enum


class Side(str, Enum):
    """The direction of an order or trade."""

    BUY = "BUY"
    SELL = "SELL"


class Outcome(str, Enum):
    """The possible outcome of a binary prediction-market contract."""

    YES = "YES"
    NO = "NO"
