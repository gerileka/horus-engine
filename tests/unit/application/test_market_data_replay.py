"""Tests for deterministic application-layer market-data journal replay."""

import asyncio
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import cast

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    LocalBookStatus,
    LocalOrderBookState,
    LocalOrderBookView,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataJournalSession,
    MarketDataReconnected,
    MarketDataReplayIntegrityError,
    MarketDataReplayResult,
    MarketDataReplaySequenceError,
    MarketDataReplaySessionNotFound,
    MarketDataReplayStep,
    MarketDataSessionId,
    MarketDataSessionReplayer,
    MarketDataSessionUpdate,
    MarketId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
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

_SESSION_ID = MarketDataSessionId("replay-session")
_MARKET_ID = MarketId("market-1")
_TOKEN_ID = TokenId("token-yes")
_TICK = TickSize("0.01")
_START = datetime(2026, 8, 2, 12, tzinfo=UTC)


class _Journal:
    """Tiny fake journal exposing only the replayer's read operations."""

    def __init__(
        self,
        session: PersistedMarketDataSession | None,
        updates: tuple[PersistedMarketDataUpdate, ...] = (),
    ) -> None:
        self.session = session
        self.updates = updates
        self.list_calls = 0

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession | None:
        """Return configured immutable session metadata."""
        assert session_id == _SESSION_ID
        return self.session

    async def initialize(self) -> None:
        """Satisfy the full journal protocol without mutating fake evidence."""

    async def start_session(self, session: MarketDataJournalSession) -> None:
        """Reject writes because replay only needs the gateway's read side."""
        raise AssertionError(f"unexpected session start: {session.session_id}")

    async def append_update(
        self,
        session_id: MarketDataSessionId,
        sequence_number: int,
        update: MarketDataSessionUpdate,
    ) -> None:
        """Reject writes because replay must be read-only."""
        raise AssertionError(
            f"unexpected update append: {session_id} {sequence_number}"
        )

    async def finish_session(
        self,
        session_id: MarketDataSessionId,
        finished_at: datetime,
        final_view: LocalOrderBookView,
    ) -> None:
        """Reject writes because replay must not finalize journal sessions."""
        raise AssertionError(f"unexpected session finish: {session_id} {finished_at}")

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        """Return configured arrival-ordered normalized updates."""
        assert session_id == _SESSION_ID
        self.list_calls += 1
        return self.updates


def _at(minutes: int) -> datetime:
    """Return one deterministic exchange observation instant."""
    return _START + timedelta(minutes=minutes)


def _book(
    bids: tuple[tuple[str, str], ...] = (("0.40", "2"),),
    asks: tuple[tuple[str, str], ...] = (("0.60", "3"),),
) -> OrderBook:
    """Build a compact immutable book with exact Decimal-backed values."""
    return OrderBook(
        [OrderBookLevel(Price(price), Quantity(size)) for price, size in bids],
        [OrderBookLevel(Price(price), Quantity(size)) for price, size in asks],
    )


def _snapshot(minutes: int = 0, book: OrderBook | None = None) -> BookSnapshotReceived:
    """Build a valid authoritative market snapshot."""
    return BookSnapshotReceived(_MARKET_ID, _TOKEN_ID, book or _book(), _at(minutes))


def _change(minutes: int = 1) -> PriceLevelChanged:
    """Build one valid aggregate bid-level replacement."""
    return PriceLevelChanged(
        _MARKET_ID,
        _TOKEN_ID,
        Side.BUY,
        Price("0.40"),
        NonNegativeQuantity("4.25"),
        _at(minutes),
    )


def _session(
    updates: tuple[PersistedMarketDataUpdate, ...] = (),
    *,
    finished_at: datetime | None = None,
    final_view: LocalOrderBookView | None = None,
) -> PersistedMarketDataSession:
    """Build coherent persisted session metadata from an optional final view."""
    return PersistedMarketDataSession(
        _SESSION_ID,
        _MARKET_ID,
        _TOKEN_ID,
        _TICK,
        _START,
        finished_at,
        None if final_view is None else final_view.status,
        None if final_view is None else final_view.tick_size,
        None if final_view is None else final_view.last_observed_at,
        None if final_view is None else final_view.status_reason,
        len(updates),
    )


def _updates(*events: MarketDataEvent) -> tuple[PersistedMarketDataUpdate, ...]:
    """Record expected metadata with the real local state machine."""
    state = LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK)
    updates: list[PersistedMarketDataUpdate] = []
    for sequence_number, event in enumerate(events, start=1):
        previous_book = state.view.book
        view = state.apply(event)
        updates.append(
            PersistedMarketDataUpdate(
                _SESSION_ID,
                sequence_number,
                event,
                previous_book != view.book,
                view.status,
                view.tick_size,
                view.last_observed_at,
                view.status_reason,
            )
        )
    return tuple(updates)


def _run(journal: _Journal) -> MarketDataReplayResult:
    """Run the async service from synchronous pytest tests."""
    return asyncio.run(MarketDataSessionReplayer(journal).replay(_SESSION_ID))


def test_replays_steps_in_arrival_order_and_preserves_trade() -> None:
    """Reconstruct aggregate changes while trade steps retain their event unchanged."""
    trade = TradeObserved(
        _MARKET_ID, _TOKEN_ID, Side.SELL, Price("0.45"), Quantity("1.5"), _at(2)
    )
    updates = _updates(_snapshot(), _change(), trade)
    result = _run(_Journal(_session(updates), updates))
    assert [step.sequence_number for step in result.steps] == [1, 2, 3]
    assert result.steps[2].event is trade
    assert [step.book_changed for step in result.steps] == [True, True, False]
    assert result.last_event_view.book == _book((("0.40", "4.25"),), (("0.60", "3"),))
    assert result.final_view == result.last_event_view
    assert not result.is_finished


def test_replays_tick_and_connection_events_without_mutating_books() -> None:
    """Keep event order while applying the existing stale-state transitions."""
    events: tuple[MarketDataEvent, ...] = (
        _snapshot(),
        TickSizeChanged(_MARKET_ID, _TOKEN_ID, _TICK, TickSize("0.05"), _at(1)),
        _snapshot(2, _book((("0.45", "2"),), (("0.60", "3"),))),
        MarketDataDisconnected("transport ended", _at(3)),
        _snapshot(4, _book((("0.50", "2"),), (("0.65", "3"),))),
        MarketDataReconnected(_at(5)),
    )
    updates = _updates(*events)
    result = _run(_Journal(_session(updates), updates))
    assert tuple(step.event for step in result.steps) == events
    assert [step.book_changed for step in result.steps] == [
        True,
        False,
        True,
        False,
        True,
        False,
    ]
    assert [step.view.status for step in result.steps] == [
        LocalBookStatus.SYNCHRONIZED,
        LocalBookStatus.STALE,
        LocalBookStatus.SYNCHRONIZED,
        LocalBookStatus.STALE,
        LocalBookStatus.SYNCHRONIZED,
        LocalBookStatus.STALE,
    ]


def test_not_found_does_not_load_updates() -> None:
    """Distinguish an absent session before asking the gateway for history."""
    journal = _Journal(None)
    with pytest.raises(MarketDataReplaySessionNotFound, match="replay-session"):
        _run(journal)
    assert journal.list_calls == 0


def test_finished_session_and_lifecycle_stale_terminal_views() -> None:
    """Use exact terminal state or the narrow non-event stale transition."""
    updates = _updates(_snapshot())
    reconstructed = LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK).apply(_snapshot())
    finished = _session(updates, finished_at=_at(2), final_view=reconstructed)
    result = _run(_Journal(finished, updates))
    assert result.is_finished and result.final_view == result.last_event_view

    stale = LocalOrderBookView(
        _MARKET_ID,
        _TOKEN_ID,
        _TICK,
        LocalBookStatus.STALE,
        reconstructed.book,
        reconstructed.last_observed_at,
        "stream exhausted",
    )
    result = _run(
        _Journal(_session(updates, finished_at=_at(2), final_view=stale), updates)
    )
    assert result.final_view == stale

    initial = LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK).view
    stale_initial = LocalOrderBookView(
        _MARKET_ID,
        _TOKEN_ID,
        _TICK,
        LocalBookStatus.STALE,
        None,
        None,
        "stream exhausted",
    )
    result = _run(_Journal(_session((), finished_at=_at(1), final_view=stale_initial)))
    assert result.last_event_view == initial
    assert result.final_view == stale_initial


@pytest.mark.parametrize(
    ("attribute", "replacement", "field"),
    [
        ("book_changed", False, "book_changed"),
        ("post_status", LocalBookStatus.STALE, "post_status"),
        ("post_tick_size", TickSize("0.05"), "post_tick_size"),
        ("post_last_observed_at", _at(9), "post_last_observed_at"),
        ("post_status_reason", "wrong", "post_status_reason"),
    ],
)
def test_rejects_each_mismatched_post_event_metadata_field(
    attribute: str, replacement: object, field: str
) -> None:
    """Treat persisted post-event metadata as integrity evidence, not state input."""
    update = _updates(_snapshot())[0]
    object.__setattr__(update, attribute, replacement)
    bad_update = update
    session = _session((bad_update,))
    with pytest.raises(MarketDataReplayIntegrityError, match=field) as caught:
        _run(_Journal(session, (bad_update,)))
    assert "replay-session" in str(caught.value) and "sequence 1" in str(caught.value)


@pytest.mark.parametrize("numbers", [(0,), (True,), (2,), (1, 1), (1, 3), (2, 1)])
def test_rejects_malformed_gateway_arrival_sequences(numbers: tuple[int, ...]) -> None:
    """Never sort, coerce, or otherwise repair malformed gateway history."""
    source = _updates(_snapshot(), _change())
    malformed_updates = list(source[: len(numbers)])
    for index, value in enumerate(numbers):
        object.__setattr__(malformed_updates[index], "sequence_number", value)
    updates = tuple(malformed_updates)
    session = _session(updates)
    with pytest.raises(MarketDataReplaySequenceError, match="replay-session"):
        _run(_Journal(session, updates))


def test_rejects_session_header_update_session_and_event_integrity_errors() -> None:
    """Detect incomplete history and wrap local-state validation with its cause."""
    update = _updates(_snapshot())[0]
    header_mismatch = replace(_session((update,)), last_sequence_number=2)
    with pytest.raises(MarketDataReplaySequenceError):
        _run(_Journal(header_mismatch, (update,)))

    wrong_session = replace(update, session_id=MarketDataSessionId("other-session"))
    with pytest.raises(MarketDataReplayIntegrityError, match="session_id"):
        _run(_Journal(_session((wrong_session,)), (wrong_session,)))

    wrong_event = BookSnapshotReceived(
        MarketId("other-market"), _TOKEN_ID, _book(), _at(0)
    )
    invalid = replace(update, event=wrong_event)
    with pytest.raises(
        MarketDataReplayIntegrityError, match="event application"
    ) as caught:
        _run(_Journal(_session((invalid,)), (invalid,)))
    assert caught.value.__cause__ is not None


def test_terminal_metadata_validation_and_timezone_comparison() -> None:
    """Reject corrupted terminal evidence while correctly comparing aware instants."""
    updates = _updates(_snapshot())
    view = LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK).apply(_snapshot())
    missing_reason = LocalOrderBookView(
        _MARKET_ID,
        _TOKEN_ID,
        _TICK,
        LocalBookStatus.STALE,
        view.book,
        view.last_observed_at,
        None,
    )
    with pytest.raises(MarketDataReplayIntegrityError, match="terminal state"):
        _run(
            _Journal(
                _session(updates, finished_at=_at(2), final_view=missing_reason),
                updates,
            )
        )

    bad_terminal = LocalOrderBookView(
        _MARKET_ID,
        _TOKEN_ID,
        TickSize("0.05"),
        LocalBookStatus.STALE,
        view.book,
        view.last_observed_at,
        "stale",
    )
    with pytest.raises(MarketDataReplayIntegrityError, match="terminal state"):
        _run(
            _Journal(
                _session(updates, finished_at=_at(2), final_view=bad_terminal), updates
            )
        )

    valid_offset_finish = _at(3).astimezone(timezone(timedelta(hours=2)))
    assert _run(
        _Journal(
            _session(updates, finished_at=valid_offset_finish, final_view=view), updates
        )
    ).is_finished

    later_updates = _updates(_snapshot(1))
    later_view = LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK).apply(_snapshot(1))
    before_observed = _session(later_updates, finished_at=_at(0), final_view=later_view)
    with pytest.raises(MarketDataReplayIntegrityError, match="finished_at"):
        _run(_Journal(before_observed, later_updates))


def test_replay_models_are_immutable_and_validate_public_values() -> None:
    """Expose frozen immutable steps and results rather than storage rows."""
    view = LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK).view
    with pytest.raises(ValueError):
        MarketDataReplayStep(_SESSION_ID, True, _snapshot(), view, False)
    step = MarketDataReplayStep(_SESSION_ID, 1, _snapshot(), view, True)
    with pytest.raises(FrozenInstanceError):
        step.book_changed = False  # type: ignore[misc]
    with pytest.raises(ValueError):
        MarketDataReplayResult(_session(), cast(object, []), view, view, False)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        MarketDataReplayResult(_session(), (), view, view, 1)  # type: ignore[arg-type]
    result = MarketDataReplayResult(_session(), (step,), view, view, False)
    with pytest.raises(FrozenInstanceError):
        result.steps = ()  # type: ignore[misc]


def test_cancellation_propagates_without_replay_wrapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep cooperative cancellation distinct from corrupted journal evidence."""
    update = _updates(_snapshot())[0]

    def _cancel(
        self: LocalOrderBookState, event: MarketDataEvent
    ) -> LocalOrderBookView:
        raise asyncio.CancelledError

    monkeypatch.setattr(LocalOrderBookState, "apply", _cancel)
    with pytest.raises(asyncio.CancelledError):
        _run(_Journal(_session((update,)), (update,)))


def test_terminal_metadata_corruption_paths_are_rejected() -> None:
    """Exercise gateway-corrupt forms that immutable normal constructors prevent."""
    session = _session()
    object.__setattr__(session, "final_reason", "unexpected")
    with pytest.raises(MarketDataReplayIntegrityError, match="terminal metadata"):
        _run(_Journal(session))

    finished = _session(
        finished_at=_at(1),
        final_view=LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK).view,
    )
    object.__setattr__(finished, "final_status", None)
    with pytest.raises(MarketDataReplayIntegrityError, match="terminal metadata"):
        _run(_Journal(finished))

    before_start = _session(
        finished_at=_at(1),
        final_view=LocalOrderBookState(_MARKET_ID, _TOKEN_ID, _TICK).view,
    )
    object.__setattr__(before_start, "finished_at", _START - timedelta(seconds=1))
    with pytest.raises(MarketDataReplayIntegrityError, match="finished_at"):
        _run(_Journal(before_start))
