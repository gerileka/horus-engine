"""Strict translation of public Polymarket CLOB order-book payloads."""

import json
from decimal import Decimal, InvalidOperation
from typing import TypeAlias, cast

from horus_engine.application import MarketId, TokenId
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    TickSize,
    is_tick_aligned,
)

from .errors import PolymarketPayloadError

ClobBookPayload: TypeAlias = dict[str, object]
ClobLevel: TypeAlias = dict[str, object]


def parse_order_book(
    document: str, requested_market_id: MarketId, requested_token_id: TokenId
) -> OrderBook:
    """Decode and validate one CLOB book response into an immutable snapshot."""
    try:
        payload = json.loads(document, parse_float=Decimal)
    except (json.JSONDecodeError, TypeError) as error:
        raise PolymarketPayloadError("CLOB returned malformed JSON") from error
    if not isinstance(payload, dict):
        raise PolymarketPayloadError("CLOB book response must be an object")
    return _order_book_from_payload(
        cast(ClobBookPayload, payload), requested_market_id, requested_token_id
    )


def _order_book_from_payload(
    payload: ClobBookPayload,
    requested_market_id: MarketId,
    requested_token_id: TokenId,
) -> OrderBook:
    """Validate CLOB identifiers, tick size, and price levels."""
    context = "CLOB book response"
    try:
        response_market_id = MarketId(_required_text(payload, "market", context))
        response_token_id = TokenId(_required_text(payload, "asset_id", context))
    except ValueError as error:
        raise PolymarketPayloadError(f"{context} has invalid identity") from error
    if response_market_id != requested_market_id:
        raise PolymarketPayloadError("CLOB book response market does not match request")
    if response_token_id != requested_token_id:
        raise PolymarketPayloadError(
            "CLOB book response asset_id does not match request"
        )

    try:
        tick_size = TickSize(_decimal_field(payload, "tick_size", context))
    except ValueError as error:
        raise PolymarketPayloadError(f"{context} has invalid tick_size") from error
    bids = _level_list(payload, "bids", context)
    asks = _level_list(payload, "asks", context)
    try:
        return OrderBook(
            bids=(_level_from_clob(level, tick_size, "bid") for level in bids),
            asks=(_level_from_clob(level, tick_size, "ask") for level in asks),
        )
    except ValueError as error:
        raise PolymarketPayloadError(f"{context} has invalid price levels") from error


def _level_list(
    payload: ClobBookPayload, field: str, context: str
) -> tuple[ClobLevel, ...]:
    """Return a CLOB price-level list whose members are objects."""
    value = payload.get(field)
    if not isinstance(value, list) or not all(
        isinstance(level, dict) for level in value
    ):
        raise PolymarketPayloadError(f"{context} has invalid {field}")
    return tuple(cast(ClobLevel, level) for level in value)


def _level_from_clob(
    level: ClobLevel, tick_size: TickSize, side: str
) -> OrderBookLevel:
    """Map one CLOB level without rounding or altering observed values."""
    context = f"CLOB {side} level"
    price = Price(_decimal_field(level, "price", context))
    quantity = Quantity(_decimal_field(level, "size", context))
    if not is_tick_aligned(price, tick_size):
        raise PolymarketPayloadError(f"{context} price is not aligned with tick_size")
    return OrderBookLevel(price, quantity)


def _required_text(payload: ClobBookPayload, field: str, context: str) -> str:
    """Return a required nonblank CLOB string without normalizing it."""
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PolymarketPayloadError(f"{context} has a missing or blank {field}")
    return value


def _decimal_field(
    payload: ClobBookPayload | ClobLevel, field: str, context: str
) -> Decimal:
    """Read a finite Decimal supplied as a CLOB number or decimal string."""
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, Decimal | int | str):
        raise PolymarketPayloadError(f"{context} has invalid {field}")
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise PolymarketPayloadError(f"{context} has invalid {field}") from error
    if not decimal_value.is_finite():
        raise PolymarketPayloadError(f"{context} has invalid {field}")
    return decimal_value
