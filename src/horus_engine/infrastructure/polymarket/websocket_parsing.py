"""Pure parsing of public Polymarket market WebSocket messages."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TypeAlias, cast

from horus_engine.application import (
    BookSnapshotReceived,
    MarketDataEvent,
    MarketId,
    PriceLevelChanged,
    TickSizeChanged,
    TokenId,
    TradeObserved,
)
from horus_engine.domain import (
    NonNegativeQuantity,
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

from .errors import PolymarketPayloadError

WebSocketPayload: TypeAlias = dict[str, object]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def parse_market_data_message(
    raw_message: str | bytes,
    requested_market_id: MarketId,
    requested_token_ids: tuple[TokenId, ...],
) -> tuple[MarketDataEvent, ...]:
    """Decode a WebSocket frame into normalized events without retaining state."""
    if isinstance(raw_message, bytes):
        try:
            raw_message = raw_message.decode("utf-8")
        except UnicodeDecodeError as error:
            raise PolymarketPayloadError(
                "market WebSocket received non-UTF-8 data"
            ) from error
    if raw_message == "PONG":
        return ()
    try:
        decoded = json.loads(raw_message, parse_float=Decimal)
    except (json.JSONDecodeError, TypeError) as error:
        raise PolymarketPayloadError(
            "market WebSocket returned malformed JSON"
        ) from error
    payloads: tuple[WebSocketPayload, ...]
    if isinstance(decoded, dict):
        payloads = (cast(WebSocketPayload, decoded),)
    elif isinstance(decoded, list) and all(isinstance(item, dict) for item in decoded):
        payloads = tuple(cast(WebSocketPayload, item) for item in decoded)
    else:
        raise PolymarketPayloadError(
            "market WebSocket message must be an object or list of objects"
        )
    events: list[MarketDataEvent] = []
    for payload in payloads:
        events.extend(
            _events_from_payload(payload, requested_market_id, requested_token_ids)
        )
    return tuple(events)


def _events_from_payload(
    payload: WebSocketPayload,
    requested_market_id: MarketId,
    requested_token_ids: tuple[TokenId, ...],
) -> tuple[MarketDataEvent, ...]:
    """Translate one typed Polymarket event payload into neutral events."""
    event_type = payload.get("event_type")
    if not isinstance(event_type, str) or not event_type:
        raise PolymarketPayloadError("market WebSocket event has no event_type")
    if event_type == "book":
        return (_book_event(payload, requested_market_id, requested_token_ids),)
    if event_type == "price_change":
        return _price_change_events(payload, requested_market_id, requested_token_ids)
    if event_type == "tick_size_change":
        return (_tick_size_event(payload, requested_market_id, requested_token_ids),)
    if event_type == "last_trade_price":
        return (_trade_event(payload, requested_market_id, requested_token_ids),)
    raise PolymarketPayloadError("market WebSocket event_type is unsupported")


def _book_event(
    payload: WebSocketPayload,
    requested_market_id: MarketId,
    requested_token_ids: tuple[TokenId, ...],
) -> BookSnapshotReceived:
    """Map a complete, unticked public book snapshot."""
    market_id = _market_id(payload, requested_market_id)
    token_id = _token_id(payload, requested_token_ids)
    timestamp = _timestamp(payload)
    try:
        book = OrderBook(
            bids=(_book_level(level, "bid") for level in _level_list(payload, "bids")),
            asks=(_book_level(level, "ask") for level in _level_list(payload, "asks")),
        )
    except ValueError as error:
        raise PolymarketPayloadError(
            "market WebSocket book has invalid price levels"
        ) from error
    return BookSnapshotReceived(market_id, token_id, book, timestamp)


def _price_change_events(
    payload: WebSocketPayload,
    requested_market_id: MarketId,
    requested_token_ids: tuple[TokenId, ...],
) -> tuple[PriceLevelChanged, ...]:
    """Map each level change in the exact order supplied by the venue."""
    market_id = _market_id(payload, requested_market_id)
    timestamp = _timestamp(payload)
    changes = _object_list(payload, "price_changes")
    events: list[PriceLevelChanged] = []
    for change in changes:
        token_id = _token_id(change, requested_token_ids)
        try:
            side = Side(_required_text(change, "side", "price change"))
            price = Price(_decimal_field(change, "price", "price change"))
            quantity = NonNegativeQuantity(
                _decimal_field(change, "size", "price change")
            )
        except ValueError as error:
            raise PolymarketPayloadError(
                "market WebSocket price change has invalid values"
            ) from error
        events.append(
            PriceLevelChanged(market_id, token_id, side, price, quantity, timestamp)
        )
    return tuple(events)


def _tick_size_event(
    payload: WebSocketPayload,
    requested_market_id: MarketId,
    requested_token_ids: tuple[TokenId, ...],
) -> TickSizeChanged:
    """Map a token's observed tick-size update without caching it."""
    market_id = _market_id(payload, requested_market_id)
    token_id = _token_id(payload, requested_token_ids)
    try:
        old_tick_size = TickSize(_decimal_field(payload, "old_tick_size", "tick size"))
        new_tick_size = TickSize(_decimal_field(payload, "new_tick_size", "tick size"))
        return TickSizeChanged(
            market_id, token_id, old_tick_size, new_tick_size, _timestamp(payload)
        )
    except ValueError as error:
        raise PolymarketPayloadError("market WebSocket tick size is invalid") from error


def _trade_event(
    payload: WebSocketPayload,
    requested_market_id: MarketId,
    requested_token_ids: tuple[TokenId, ...],
) -> TradeObserved:
    """Map a public trade while preserving aggressor side semantics."""
    market_id = _market_id(payload, requested_market_id)
    token_id = _token_id(payload, requested_token_ids)
    try:
        return TradeObserved(
            market_id,
            token_id,
            Side(_required_text(payload, "side", "trade")),
            Price(_decimal_field(payload, "price", "trade")),
            Quantity(_decimal_field(payload, "size", "trade")),
            _timestamp(payload),
        )
    except ValueError as error:
        raise PolymarketPayloadError("market WebSocket trade is invalid") from error


def _market_id(payload: WebSocketPayload, requested_market_id: MarketId) -> MarketId:
    """Return a validated message market identity matching the subscription."""
    try:
        market_id = MarketId(_required_text(payload, "market", "event"))
    except ValueError as error:
        raise PolymarketPayloadError(
            "market WebSocket event has invalid market"
        ) from error
    if market_id != requested_market_id:
        raise PolymarketPayloadError(
            "market WebSocket event market does not match request"
        )
    return market_id


def _token_id(
    payload: WebSocketPayload, requested_token_ids: tuple[TokenId, ...]
) -> TokenId:
    """Return a validated subscribed event token identity."""
    try:
        token_id = TokenId(_required_text(payload, "asset_id", "event"))
    except ValueError as error:
        raise PolymarketPayloadError(
            "market WebSocket event has invalid asset_id"
        ) from error
    if token_id not in requested_token_ids:
        raise PolymarketPayloadError(
            "market WebSocket event asset_id is not subscribed"
        )
    return token_id


def _book_level(level: WebSocketPayload, side: str) -> OrderBookLevel:
    """Map one public snapshot level without applying unknown tick alignment."""
    return OrderBookLevel(
        Price(_decimal_field(level, "price", f"{side} level")),
        Quantity(_decimal_field(level, "size", f"{side} level")),
    )


def _level_list(payload: WebSocketPayload, field: str) -> tuple[WebSocketPayload, ...]:
    """Return a list of snapshot level objects."""
    return _object_list(payload, field)


def _object_list(payload: WebSocketPayload, field: str) -> tuple[WebSocketPayload, ...]:
    """Return a named list with object members only."""
    value = payload.get(field)
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise PolymarketPayloadError(f"market WebSocket event has invalid {field}")
    return tuple(cast(WebSocketPayload, item) for item in value)


def _required_text(payload: WebSocketPayload, field: str, context: str) -> str:
    """Return a nonblank text payload field without normalizing its value."""
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PolymarketPayloadError(
            f"market WebSocket {context} has missing or blank {field}"
        )
    return value


def _decimal_field(payload: WebSocketPayload, field: str, context: str) -> Decimal:
    """Read a finite decimal from an integer, Decimal, or decimal string field."""
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, Decimal | int | str):
        raise PolymarketPayloadError(f"market WebSocket {context} has invalid {field}")
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise PolymarketPayloadError(
            f"market WebSocket {context} has invalid {field}"
        ) from error
    if not decimal_value.is_finite():
        raise PolymarketPayloadError(f"market WebSocket {context} has invalid {field}")
    return decimal_value


def _timestamp(payload: WebSocketPayload) -> datetime:
    """Convert a nonnegative Unix-millisecond timestamp to an aware UTC time."""
    value = payload.get("timestamp")
    if isinstance(value, bool):
        raise PolymarketPayloadError("market WebSocket event has invalid timestamp")
    if isinstance(value, int):
        milliseconds = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        milliseconds = int(value)
    else:
        raise PolymarketPayloadError("market WebSocket event has invalid timestamp")
    if milliseconds < 0:
        raise PolymarketPayloadError("market WebSocket event has invalid timestamp")
    seconds, remaining_milliseconds = divmod(milliseconds, 1000)
    return _EPOCH + timedelta(seconds=seconds, milliseconds=remaining_milliseconds)
