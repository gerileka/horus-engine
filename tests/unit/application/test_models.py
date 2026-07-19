"""Tests for exchange-neutral application models and identifiers."""

from dataclasses import FrozenInstanceError
from typing import TypeAlias

import pytest

from horus_engine.application import (
    ClientOrderId,
    ExchangeOrderId,
    InvalidClientOrderId,
    InvalidExchangeOrderId,
    InvalidMarket,
    InvalidMarketId,
    InvalidTokenId,
    Market,
    MarketId,
    MarketStatus,
    TokenId,
)
from horus_engine.domain import Quantity, TickSize

Identifier: TypeAlias = MarketId | TokenId | ExchangeOrderId | ClientOrderId
IdentifierType: TypeAlias = (
    type[MarketId] | type[TokenId] | type[ExchangeOrderId] | type[ClientOrderId]
)


@pytest.mark.parametrize(
    ("identifier_type", "error_type"),
    [
        (MarketId, InvalidMarketId),
        (TokenId, InvalidTokenId),
        (ExchangeOrderId, InvalidExchangeOrderId),
        (ClientOrderId, InvalidClientOrderId),
    ],
)
def test_identifiers_are_validated_immutable_hashable_value_objects(
    identifier_type: IdentifierType, error_type: type[ValueError]
) -> None:
    """Keep each explicit identifier stable without normalizing its input."""
    identifier = identifier_type(" value-with-spaces ")
    same_identifier = identifier_type(" value-with-spaces ")
    assert identifier.value == " value-with-spaces "
    assert identifier == same_identifier
    assert hash(identifier) == hash(same_identifier)
    with pytest.raises(FrozenInstanceError):
        identifier.value = "changed"  # type: ignore[misc]
    for invalid_value in ("", "   ", "identifier\x00"):
        with pytest.raises(error_type):
            identifier_type(invalid_value)


def test_distinct_identifier_types_do_not_compare_equal() -> None:
    """Prevent matching string values from conflating application identifier roles."""
    identifiers: tuple[Identifier, ...] = (
        MarketId("shared"),
        TokenId("shared"),
        ExchangeOrderId("shared"),
        ClientOrderId("shared"),
    )
    assert len(set(identifiers)) == 4


def test_market_status_exposes_the_supported_exchange_neutral_states() -> None:
    """Keep the public market lifecycle vocabulary deliberately small."""
    assert tuple(status.value for status in MarketStatus) == (
        "ACTIVE",
        "SUSPENDED",
        "CLOSED",
        "RESOLVED",
    )


def test_market_is_immutable_exchange_neutral_metadata() -> None:
    """Represent a valid market without adapter-specific fields or metadata."""
    market = Market(
        market_id=MarketId("market-1"),
        question="Will this contract resolve YES?",
        yes_token_id=TokenId("yes-token"),
        no_token_id=TokenId("no-token"),
        tick_size=TickSize("0.01"),
        minimum_order_quantity=Quantity("1"),
        status=MarketStatus.ACTIVE,
    )
    assert market.status is MarketStatus.ACTIVE
    with pytest.raises(FrozenInstanceError):
        market.question = "Changed"  # type: ignore[misc]


@pytest.mark.parametrize("question", ["", "   "])
def test_market_rejects_empty_or_whitespace_only_questions(question: str) -> None:
    """Require market questions that retain meaningful user-facing text."""
    with pytest.raises(InvalidMarket):
        Market(
            MarketId("market-1"),
            question,
            TokenId("yes-token"),
            TokenId("no-token"),
            TickSize("0.01"),
            Quantity("1"),
            MarketStatus.ACTIVE,
        )


def test_market_rejects_identical_yes_and_no_tokens() -> None:
    """Protect binary market metadata from an impossible token assignment."""
    token_id = TokenId("same-token")
    with pytest.raises(InvalidMarket):
        Market(
            MarketId("market-1"),
            "Question?",
            token_id,
            token_id,
            TickSize("0.01"),
            Quantity("1"),
            MarketStatus.ACTIVE,
        )
