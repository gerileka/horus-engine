"""Immutable, exchange-neutral application models."""

from dataclasses import dataclass
from enum import Enum
from unicodedata import category

from horus_engine.domain import Quantity, TickSize

from .errors import (
    InvalidClientOrderId,
    InvalidExchangeOrderId,
    InvalidMarket,
    InvalidMarketId,
    InvalidTokenId,
)


def _validate_identifier(value: str, error_type: type[ValueError]) -> None:
    """Reject identifier values that are blank or contain control characters."""
    if not isinstance(value, str) or not value or value.isspace():
        raise error_type("identifier must be a non-blank string")
    if any(category(character) == "Cc" for character in value):
        raise error_type("identifier must not contain control characters")


def _validate_nonblank_text(value: str, error_type: type[ValueError]) -> None:
    """Reject text that is missing, empty, or composed solely of whitespace."""
    if not isinstance(value, str) or not value or value.isspace():
        raise error_type("text must be a non-blank string")


@dataclass(frozen=True, init=False)
class MarketId:
    """Horus Engine's stable, exchange-neutral market identifier."""

    value: str

    def __init__(self, value: str) -> None:
        """Create an identifier without normalizing its supplied string value."""
        _validate_identifier(value, InvalidMarketId)
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, init=False)
class TokenId:
    """An exchange-neutral identifier for one tradable outcome token."""

    value: str

    def __init__(self, value: str) -> None:
        """Create an identifier without normalizing its supplied string value."""
        _validate_identifier(value, InvalidTokenId)
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, init=False)
class ExchangeOrderId:
    """An identifier assigned to an order by an exchange."""

    value: str

    def __init__(self, value: str) -> None:
        """Create an identifier without normalizing its supplied string value."""
        _validate_identifier(value, InvalidExchangeOrderId)
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, init=False)
class ClientOrderId:
    """An exchange-neutral identifier generated before order submission."""

    value: str

    def __init__(self, value: str) -> None:
        """Create an identifier without normalizing its supplied string value."""
        _validate_identifier(value, InvalidClientOrderId)
        object.__setattr__(self, "value", value)


class MarketStatus(str, Enum):
    """The currently observed availability or terminal state of a market."""

    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"
    RESOLVED = "RESOLVED"


@dataclass(frozen=True)
class Market:
    """Small immutable exchange-neutral market metadata contract."""

    market_id: MarketId
    question: str
    yes_token_id: TokenId
    no_token_id: TokenId
    tick_size: TickSize
    minimum_order_quantity: Quantity
    status: MarketStatus

    def __post_init__(self) -> None:
        """Reject blank questions and markets whose outcome tokens are identical."""
        _validate_nonblank_text(self.question, InvalidMarket)
        if self.yes_token_id == self.no_token_id:
            raise InvalidMarket("YES and NO token identifiers must differ")
