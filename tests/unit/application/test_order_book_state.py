"""Tests for snapshot-first local order-book reconstruction."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    InvalidLocalOrderBook,
    LocalBookStatus,
    LocalOrderBookState,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataIdentityMismatch,
    MarketDataReconnected,
    MarketId,
    OutOfOrderMarketDataEvent,
    PriceLevelChanged,
    SnapshotRequired,
    TickSizeChanged,
    TickSizeStateMismatch,
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

_MARKET_ID = MarketId("market-1")
_TOKEN_ID = TokenId("token-yes")
_OTHER_MARKET_ID = MarketId("market-2")
_OTHER_TOKEN_ID = TokenId("token-no")
_TICK_SIZE = TickSize("0.01")
_START = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)


def _at(minutes: int = 0) -> datetime:
    """Return a deterministic increasing observed timestamp."""
    return _START + timedelta(minutes=minutes)


def _book(
    bids: tuple[tuple[str, str], ...] = (("0.40", "2"),),
    asks: tuple[tuple[str, str], ...] = (("0.60", "3"),),
) -> OrderBook:
    """Create a compact immutable domain snapshot from decimal strings."""
    return OrderBook(
        [OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in bids],
        [OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in asks],
    )


def _state() -> LocalOrderBookState:
    """Create the standard one-market local state machine."""
    return LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK_SIZE)


def _snapshot(
    minutes: int = 0,
    book: OrderBook | None = None,
    market_id: MarketId = _MARKET_ID,
    token_id: TokenId = _TOKEN_ID,
) -> BookSnapshotReceived:
    """Create one authoritative snapshot event."""
    return BookSnapshotReceived(market_id, token_id, book or _book(), _at(minutes))


def _change(
    side: Side,
    price: str,
    quantity: str,
    minutes: int,
    market_id: MarketId = _MARKET_ID,
    token_id: TokenId = _TOKEN_ID,
) -> PriceLevelChanged:
    """Create one aggregate price-level update."""
    return PriceLevelChanged(
        market_id,
        token_id,
        side,
        Price(price),
        NonNegativeQuantity(quantity),
        _at(minutes),
    )


def _synchronize(state: LocalOrderBookState) -> None:
    """Apply the standard valid snapshot to a supplied state machine."""
    state.apply(_snapshot())


def test_construction_and_views_are_immutable() -> None:
    """Expose only a frozen snapshot-first view with no mutable mappings."""
    state = _state()
    view = state.view
    assert view.market_id == _MARKET_ID
    assert view.token_id == _TOKEN_ID
    assert view.tick_size == _TICK_SIZE
    assert view.status is LocalBookStatus.AWAITING_SNAPSHOT
    assert view.book is None
    assert view.last_observed_at is None
    assert view.status_reason is None
    assert view is not state.view
    with pytest.raises(FrozenInstanceError):
        view.status = LocalBookStatus.STALE  # type: ignore[misc]


@pytest.mark.parametrize(
    ("book", "expected_bids", "expected_asks"),
    [
        (
            _book((("0.30", "1"), ("0.40", "2")), (("0.70", "1"), ("0.60", "2"))),
            ("0.40", "0.30"),
            ("0.60", "0.70"),
        ),
        (_book((), ()), (), ()),
        (_book((("0.40", "2"),), ()), ("0.40",), ()),
        (_book((), (("0.60", "3"),)), (), ("0.60",)),
        (_book((("0.50", "2"),), (("0.50", "3"),)), ("0.50",), ("0.50",)),
    ],
)
def test_snapshot_preserves_valid_book_shapes(
    book: OrderBook, expected_bids: tuple[str, ...], expected_asks: tuple[str, ...]
) -> None:
    """Accept unsorted, empty, one-sided, and locked snapshots."""
    view = _state().apply(_snapshot(book=book))
    assert view.status is LocalBookStatus.SYNCHRONIZED
    assert view.status_reason is None
    assert view.book is not None
    assert tuple(str(level.price) for level in view.book.bids) == expected_bids
    assert tuple(str(level.price) for level in view.book.asks) == expected_asks


@pytest.mark.parametrize(
    "book",
    [
        _book((("0.61", "1"),), (("0.60", "1"),)),
        _book((("0.405", "1"),), ()),
        _book((), (("0.605", "1"),)),
    ],
)
def test_unsafe_or_misaligned_snapshot_is_rejected_atomically(book: OrderBook) -> None:
    """Never partially replace a synchronized book with an unsafe snapshot."""
    state = _state()
    _synchronize(state)
    previous = state.view
    with pytest.raises(InvalidLocalOrderBook):
        state.apply(_snapshot(1, book))
    assert state.view == previous


def test_snapshot_identity_mismatch_preserves_state() -> None:
    """Reject a snapshot for another configured market or token."""
    state = _state()
    previous = state.view
    with pytest.raises(MarketDataIdentityMismatch):
        state.apply(_snapshot(market_id=_OTHER_MARKET_ID))
    assert state.view == previous
    with pytest.raises(MarketDataIdentityMismatch):
        state.apply(_snapshot(token_id=_OTHER_TOKEN_ID))
    assert state.view == previous


def test_snapshot_replaces_existing_book_and_recovers_from_stale() -> None:
    """A valid snapshot wholly replaces levels and clears a stale reason."""
    state = _state()
    _synchronize(state)
    state.apply(MarketDataDisconnected(None, _at(1)))
    view = state.apply(_snapshot(2, _book((("0.45", "8"),), ())))
    assert view.status is LocalBookStatus.SYNCHRONIZED
    assert view.status_reason is None
    assert view.book == _book((("0.45", "8"),), ())


def test_snapshot_recovers_from_invalid_state() -> None:
    """Only a fresh valid snapshot can restore an invalid local observation."""
    state = _state()
    _synchronize(state)
    state.apply(_change(Side.BUY, "0.70", "1", 1))
    assert state.view.status is LocalBookStatus.INVALID
    view = state.apply(_snapshot(2, _book()))
    assert view.status is LocalBookStatus.SYNCHRONIZED
    assert view.status_reason is None


@pytest.mark.parametrize(
    ("side", "price", "quantity", "expected_book"),
    [
        (
            Side.BUY,
            "0.45",
            "4",
            _book((("0.45", "4"), ("0.40", "2")), (("0.60", "3"),)),
        ),
        (Side.BUY, "0.40", "7", _book((("0.40", "7"),), (("0.60", "3"),))),
        (
            Side.SELL,
            "0.55",
            "4",
            _book((("0.40", "2"),), (("0.55", "4"), ("0.60", "3"))),
        ),
        (Side.SELL, "0.60", "7", _book((("0.40", "2"),), (("0.60", "7"),))),
        (Side.BUY, "0.40", "0", _book((), (("0.60", "3"),))),
        (Side.SELL, "0.60", "0", _book((("0.40", "2"),), ())),
    ],
)
def test_price_level_changes_replace_or_remove_aggregate_levels(
    side: Side, price: str, quantity: str, expected_book: OrderBook
) -> None:
    """Treat incoming size as an aggregate replacement, including deletion."""
    state = _state()
    _synchronize(state)
    view = state.apply(_change(side, price, quantity, 1))
    assert view.status is LocalBookStatus.SYNCHRONIZED
    assert view.book == expected_book


def test_price_change_handles_idempotency_fractional_and_empty_books() -> None:
    """Allow repeated aggregate observations and preserve empty book outcomes."""
    state = _state()
    _synchronize(state)
    state.apply(_change(Side.BUY, "0.40", "2.5", 1))
    repeated = state.apply(_change(Side.BUY, "0.40", "2.5", 2))
    assert repeated.book == _book((("0.40", "2.5"),), (("0.60", "3"),))
    state.apply(_change(Side.BUY, "0.40", "0", 3))
    state.apply(_change(Side.SELL, "0.60", "0", 4))
    empty = state.apply(_change(Side.SELL, "0.70", "0", 5))
    assert empty.book == _book((), ())
    assert empty.status is LocalBookStatus.SYNCHRONIZED


def test_locked_update_stays_synchronized_but_crossed_update_is_retained_invalid() -> (
    None
):
    """Distinguish valid locked observations from unsafe crossed observations."""
    state = _state()
    _synchronize(state)
    locked = state.apply(_change(Side.BUY, "0.60", "1", 1))
    assert locked.book is not None and locked.book.is_locked
    assert locked.status is LocalBookStatus.SYNCHRONIZED
    crossed = state.apply(_change(Side.BUY, "0.61", "1", 2))
    assert crossed.book is not None and crossed.book.is_crossed
    assert crossed.status is LocalBookStatus.INVALID
    assert crossed.status_reason == "crossed local book requires snapshot"
    with pytest.raises(SnapshotRequired):
        state.apply(_change(Side.SELL, "0.62", "1", 3))


@pytest.mark.parametrize(
    "status_event",
    [MarketDataDisconnected(None, _at(0)), MarketDataReconnected(_at(0))],
)
def test_incremental_update_requires_snapshot_when_not_synchronized(
    status_event: MarketDataDisconnected | MarketDataReconnected,
) -> None:
    """Reject updates before initial synchronization and in every stale state."""
    state = _state()
    if isinstance(status_event, MarketDataDisconnected):
        state.apply(status_event)
    else:
        state.apply(status_event)
    previous = state.view
    with pytest.raises(SnapshotRequired):
        state.apply(_change(Side.BUY, "0.40", "1", 1))
    assert state.view == previous


def test_price_change_before_initial_snapshot_is_rejected() -> None:
    """Require an authority before applying any incremental level update."""
    state = _state()
    with pytest.raises(SnapshotRequired):
        state.apply(_change(Side.BUY, "0.40", "1", 0))
    assert state.view.status is LocalBookStatus.AWAITING_SNAPSHOT


def test_invalid_price_change_and_identity_mismatch_are_atomic() -> None:
    """Keep every public view field unchanged after rejected level events."""
    state = _state()
    _synchronize(state)
    previous = state.view
    with pytest.raises(InvalidLocalOrderBook):
        state.apply(_change(Side.BUY, "0.405", "1", 1))
    assert state.view == previous
    with pytest.raises(MarketDataIdentityMismatch):
        state.apply(_change(Side.BUY, "0.40", "1", 1, _OTHER_MARKET_ID))
    assert state.view == previous


def test_tick_size_change_tracks_new_tick_and_forces_resynchronization() -> None:
    """Retain diagnostics but validate the next snapshot against the new tick."""
    state = _state()
    _synchronize(state)
    old_book = state.view.book
    view = state.apply(
        TickSizeChanged(_MARKET_ID, _TOKEN_ID, _TICK_SIZE, TickSize("0.005"), _at(1))
    )
    assert view.tick_size == TickSize("0.005")
    assert view.status is LocalBookStatus.STALE
    assert view.book == old_book
    assert view.status_reason == "tick-size change requires snapshot"
    with pytest.raises(SnapshotRequired):
        state.apply(_change(Side.BUY, "0.405", "1", 2))
    recovered = state.apply(_snapshot(3, _book((("0.405", "1"),), ())))
    assert recovered.status is LocalBookStatus.SYNCHRONIZED


def test_tick_size_mismatch_and_identity_mismatch_are_atomic() -> None:
    """Reject incompatible tick changes without changing tracked tick state."""
    state = _state()
    _synchronize(state)
    previous = state.view
    with pytest.raises(TickSizeStateMismatch):
        state.apply(
            TickSizeChanged(
                _MARKET_ID, _TOKEN_ID, TickSize("0.005"), TickSize("0.01"), _at(1)
            )
        )
    assert state.view == previous
    with pytest.raises(MarketDataIdentityMismatch):
        state.apply(
            TickSizeChanged(
                _OTHER_MARKET_ID, _TOKEN_ID, _TICK_SIZE, TickSize("0.005"), _at(1)
            )
        )
    assert state.view == previous


def test_disconnect_and_reconnect_retain_book_and_require_snapshot() -> None:
    """Connection lifecycle events never restore synchronization themselves."""
    state = _state()
    _synchronize(state)
    disconnected = state.apply(MarketDataDisconnected("raw adapter detail", _at(1)))
    assert disconnected.status is LocalBookStatus.STALE
    assert disconnected.book == _book()
    assert disconnected.status_reason == "market-data disconnection requires snapshot"
    reconnected = state.apply(MarketDataReconnected(_at(2)))
    assert reconnected.status is LocalBookStatus.STALE
    assert reconnected.book == _book()
    assert reconnected.status_reason == "market-data reconnection requires snapshot"
    assert state.apply(_snapshot(3)).status is LocalBookStatus.SYNCHRONIZED


def test_require_snapshot_retains_diagnostics_and_rejects_blank_reasons() -> None:
    """Orchestration can mark a retained book stale without inventing a timestamp."""
    state = _state()
    _synchronize(state)
    synchronized = state.view
    stale = state.require_snapshot(" stream ended ")
    assert stale.status is LocalBookStatus.STALE
    assert stale.status_reason == "stream ended"
    assert stale.book == synchronized.book
    assert stale.tick_size == synchronized.tick_size
    assert stale.last_observed_at == synchronized.last_observed_at
    with pytest.raises(ValueError, match="non-blank"):
        state.require_snapshot(" ")


def test_require_snapshot_keeps_invalid_status() -> None:
    """An invalid observed book remains invalid when resynchronization is needed."""
    state = _state()
    _synchronize(state)
    state.apply(_change(Side.BUY, "0.70", "1", 1))
    view = state.require_snapshot("operator intervention")
    assert view.status is LocalBookStatus.INVALID
    assert view.status_reason == "operator intervention"


def test_ordering_accepts_equal_timestamps_and_rejects_earlier_events_atomically() -> (
    None
):
    """Use timestamps as a strict-earlier safety check, not a reordering buffer."""
    state = _state()
    _synchronize(state)
    same_time = state.apply(_change(Side.BUY, "0.41", "1", 0))
    assert same_time.last_observed_at == _at(0)
    previous = state.view
    earlier_events: tuple[MarketDataEvent, ...] = (
        _snapshot(-1),
        _change(Side.BUY, "0.42", "1", -1),
        MarketDataDisconnected(None, _at(-1)),
        TickSizeChanged(_MARKET_ID, _TOKEN_ID, _TICK_SIZE, TickSize("0.005"), _at(-1)),
    )
    for event in earlier_events:
        with pytest.raises(OutOfOrderMarketDataEvent):
            state.apply(event)
        assert state.view == previous


def test_trade_is_identity_checked_no_op_and_unknown_events_are_rejected() -> None:
    """Ensure trades cannot alter local state and unsupported events are explicit."""
    state = _state()
    _synchronize(state)
    previous = state.view
    trade = TradeObserved(
        _MARKET_ID, _TOKEN_ID, Side.BUY, Price("0.50"), Quantity("1"), _at(1)
    )
    assert state.apply(trade) == previous
    with pytest.raises(MarketDataIdentityMismatch):
        state.apply(
            TradeObserved(
                _OTHER_MARKET_ID,
                _TOKEN_ID,
                Side.BUY,
                Price("0.50"),
                Quantity("1"),
                _at(1),
            )
        )
    assert state.view == previous
    with pytest.raises(TypeError, match="unsupported"):
        state.apply(cast(MarketDataEvent, object()))


def test_caller_owned_snapshot_collections_cannot_mutate_applied_book() -> None:
    """Copy source levels into immutable domain and application state snapshots."""
    bids = [OrderBookLevel(Price("0.40"), Quantity("2"))]
    snapshot = OrderBook(bids, ())
    state = _state()
    state.apply(_snapshot(book=snapshot))
    bids.append(OrderBookLevel(Price("0.50"), Quantity("9")))
    assert state.view.book == _book((("0.40", "2"),), ())
