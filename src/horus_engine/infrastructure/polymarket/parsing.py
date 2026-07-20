"""Strict translation of public Gamma payloads into application models."""

import json
from decimal import Decimal, InvalidOperation
from typing import TypeAlias, cast

from horus_engine.application import Market, MarketId, MarketStatus, TokenId
from horus_engine.domain import Quantity, TickSize

from .errors import PolymarketPayloadError

GammaRecord: TypeAlias = dict[str, object]


def parse_market_page(document: str) -> tuple[GammaRecord, ...]:
    """Decode one Gamma market page without allowing binary float values."""
    try:
        payload = json.loads(document, parse_float=Decimal)
    except (json.JSONDecodeError, TypeError) as error:
        raise PolymarketPayloadError("Gamma returned malformed JSON") from error
    if not isinstance(payload, list) or not all(
        isinstance(record, dict) for record in payload
    ):
        raise PolymarketPayloadError("Gamma market response must be a list of objects")
    return tuple(cast(GammaRecord, record) for record in payload)


def market_from_gamma(record: GammaRecord, record_index: int) -> Market | None:
    """Map an eligible Gamma market or skip a clearly non-CLOB record."""
    if record.get("enableOrderBook") is not True:
        return None
    context = f"Gamma market record {record_index}"
    condition_id = _required_text(record, "conditionId", context)
    question = _required_text(record, "question", context)
    outcomes = _string_list(record, "outcomes", context)
    token_ids = _string_list(record, "clobTokenIds", context)
    if len(outcomes) != 2:
        raise PolymarketPayloadError(f"{context} must contain exactly two outcomes")
    if len(token_ids) != len(outcomes):
        raise PolymarketPayloadError(f"{context} outcome and token counts differ")
    if len(set(token_ids)) != len(token_ids):
        raise PolymarketPayloadError(f"{context} contains duplicate CLOB token IDs")

    tokens_by_outcome = {
        outcome.casefold(): token_id
        for outcome, token_id in zip(outcomes, token_ids, strict=True)
    }
    if set(tokens_by_outcome) != {"yes", "no"} or len(tokens_by_outcome) != 2:
        raise PolymarketPayloadError(f"{context} outcomes must be one YES and one NO")

    try:
        return Market(
            market_id=MarketId(condition_id),
            question=question,
            yes_token_id=TokenId(tokens_by_outcome["yes"]),
            no_token_id=TokenId(tokens_by_outcome["no"]),
            tick_size=TickSize(
                _decimal_field(record, "orderPriceMinTickSize", context)
            ),
            minimum_order_quantity=Quantity(
                _decimal_field(record, "orderMinSize", context)
            ),
            status=_market_status(record),
        )
    except (ValueError, TypeError) as error:
        raise PolymarketPayloadError(
            f"{context} has invalid market metadata"
        ) from error


def _required_text(record: GammaRecord, field: str, context: str) -> str:
    """Return a nonblank Gamma string field without modifying it."""
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PolymarketPayloadError(f"{context} has a missing or blank {field}")
    return value


def _string_list(record: GammaRecord, field: str, context: str) -> tuple[str, ...]:
    """Decode a Gamma list field that may itself be JSON-encoded text."""
    value = record.get(field)
    if isinstance(value, str):
        try:
            value = json.loads(value, parse_float=Decimal)
        except json.JSONDecodeError as error:
            raise PolymarketPayloadError(f"{context} has malformed {field}") from error
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise PolymarketPayloadError(f"{context} has invalid {field}")
    return tuple(value)


def _decimal_field(record: GammaRecord, field: str, context: str) -> Decimal:
    """Read a finite Decimal supplied as a Gamma number or decimal string."""
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, Decimal | int | str):
        raise PolymarketPayloadError(f"{context} has invalid {field}")
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise PolymarketPayloadError(f"{context} has invalid {field}") from error
    if not decimal_value.is_finite():
        raise PolymarketPayloadError(f"{context} has invalid {field}")
    return decimal_value


def _market_status(record: GammaRecord) -> MarketStatus:
    """Conservatively derive a market status from public Gamma flags."""
    if record.get("closed") is True:
        return MarketStatus.CLOSED
    if record.get("active") is True and record.get("acceptingOrders") is True:
        return MarketStatus.ACTIVE
    return MarketStatus.SUSPENDED
