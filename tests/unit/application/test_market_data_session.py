"""Tests for the single-lifecycle application market-data session."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    LocalBookStatus,
    MarketDataBootstrapError,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataReconnected,
    MarketDataSession,
    MarketDataSessionAlreadyStarted,
    MarketDataSessionError,
    MarketDataSessionUpdate,
    MarketDataStreamGateway,
    MarketDataSynchronizationLost,
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

_MARKET_ID = MarketId("market-1")
_TOKEN_ID = TokenId("token-yes")
_OTHER_MARKET_ID = MarketId("market-2")
_OTHER_TOKEN_ID = TokenId("token-no")
_TICK_SIZE = TickSize("0.01")
_START = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)


def _at(minutes: int = 0) -> datetime:
    """Return a deterministic aware market-data timestamp."""
    return _START + timedelta(minutes=minutes)


def _book(
    bids: tuple[tuple[str, str], ...] = (("0.40", "2"),),
    asks: tuple[tuple[str, str], ...] = (("0.60", "3"),),
) -> OrderBook:
    """Build a small immutable aggregate book from decimal strings."""
    return OrderBook(
        [OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in bids],
        [OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in asks],
    )


def _snapshot(
    minutes: int = 0,
    book: OrderBook | None = None,
    market_id: MarketId = _MARKET_ID,
    token_id: TokenId = _TOKEN_ID,
) -> BookSnapshotReceived:
    """Build an authoritative snapshot event."""
    return BookSnapshotReceived(market_id, token_id, book or _book(), _at(minutes))


def _change(side: Side, price: str, quantity: str, minutes: int) -> PriceLevelChanged:
    """Build one aggregate-level replacement or deletion event."""
    return PriceLevelChanged(
        _MARKET_ID,
        _TOKEN_ID,
        side,
        Price(price),
        NonNegativeQuantity(quantity),
        _at(minutes),
    )


def _trade(minutes: int = 0) -> TradeObserved:
    """Build a normalized trade event that does not mutate the book."""
    return TradeObserved(
        _MARKET_ID, _TOKEN_ID, Side.BUY, Price("0.50"), Quantity("1"), _at(minutes)
    )


class FakeGateway:
    """A deterministic normalized gateway with recorded subscription arguments."""

    def __init__(
        self,
        events: tuple[MarketDataEvent, ...],
        failure: Exception | None = None,
    ) -> None:
        """Store events to yield in the exact supplied order."""
        self._events = events
        self._failure = failure
        self.calls: list[tuple[MarketId, tuple[TokenId, ...]]] = []

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        """Record a subscription and provide its finite normalized event stream."""
        self.calls.append((market_id, token_ids))
        return self._stream()

    async def _stream(self) -> AsyncIterator[MarketDataEvent]:
        """Yield configured events before optionally failing the transport."""
        for event in self._events:
            yield event
        if self._failure is not None:
            raise self._failure


async def _collect(session: MarketDataSession) -> list[MarketDataSessionUpdate]:
    """Collect the updates from one finite session stream."""
    return [update async for update in session.stream_updates()]


def test_construction_subscription_and_initial_immutable_view() -> None:
    """Expose configured identity while subscribing to exactly the requested token."""
    gateway = FakeGateway((_snapshot(),))
    stream_gateway: MarketDataStreamGateway = gateway
    session = MarketDataSession(stream_gateway, _MARKET_ID, _TOKEN_ID, _TICK_SIZE)
    view = session.view
    assert view.market_id == _MARKET_ID
    assert view.token_id == _TOKEN_ID
    assert view.tick_size == _TICK_SIZE
    assert view.status is LocalBookStatus.AWAITING_SNAPSHOT
    updates = asyncio.run(_collect(session))
    assert gateway.calls == [(_MARKET_ID, (_TOKEN_ID,))]
    assert updates[0].book_changed is True
    assert session.view.status is LocalBookStatus.STALE


@pytest.mark.parametrize(
    "book",
    [
        _book((), ()),
        _book((("0.40", "2"),), ()),
        _book((("0.50", "2"),), (("0.50", "3"),)),
    ],
)
def test_initial_snapshot_synchronizes_valid_book_shapes(book: OrderBook) -> None:
    """Empty, one-sided, and locked snapshots are authoritative bootstraps."""
    session = MarketDataSession(
        FakeGateway((_snapshot(book=book),)), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    updates = asyncio.run(_collect(session))
    assert updates[0].book_view.status is LocalBookStatus.SYNCHRONIZED
    assert updates[0].book_view.book == book


def test_trade_before_snapshot_is_emitted_without_synchronizing() -> None:
    """A trade is observable but cannot prove that the local book is current."""
    session = MarketDataSession(
        FakeGateway((_trade(), _snapshot(1))), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    updates = asyncio.run(_collect(session))
    assert [update.event for update in updates] == [_trade(), _snapshot(1)]
    assert updates[0].book_view.status is LocalBookStatus.AWAITING_SNAPSHOT
    assert updates[0].book_changed is False
    assert updates[1].book_view.status is LocalBookStatus.SYNCHRONIZED


@pytest.mark.parametrize(
    "event",
    [
        _change(Side.BUY, "0.40", "1", 0),
        TickSizeChanged(_MARKET_ID, _TOKEN_ID, _TICK_SIZE, TickSize("0.005"), _at()),
        MarketDataReconnected(_at()),
    ],
)
def test_unsafe_bootstrap_events_fail_closed(event: MarketDataEvent) -> None:
    """Incremental or reconnect observations cannot establish an initial book."""
    session = MarketDataSession(
        FakeGateway((event,)), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    with pytest.raises(MarketDataBootstrapError):
        asyncio.run(_collect(session))
    assert session.view.status is LocalBookStatus.STALE
    assert session.view.last_observed_at is None


def test_disconnection_before_snapshot_yields_one_stale_terminal_update() -> None:
    """A pre-bootstrap disconnection is visible before this lifecycle ends."""
    event = MarketDataDisconnected(None, _at())
    session = MarketDataSession(
        FakeGateway((event, _snapshot(1))), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    updates = asyncio.run(_collect(session))
    assert [update.event for update in updates] == [event]
    assert updates[0].book_changed is False
    assert updates[0].book_view.status is LocalBookStatus.STALE


@pytest.mark.parametrize(
    "event",
    [
        _snapshot(market_id=_OTHER_MARKET_ID),
        _snapshot(token_id=_OTHER_TOKEN_ID),
        _snapshot(book=_book((("0.61", "1"),), (("0.60", "1"),))),
    ],
)
def test_invalid_bootstrap_snapshot_loses_synchronization(
    event: BookSnapshotReceived,
) -> None:
    """State-machine identity and book-validation failures retain diagnostics safely."""
    session = MarketDataSession(
        FakeGateway((event,)), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    with pytest.raises(MarketDataSynchronizationLost) as caught:
        asyncio.run(_collect(session))
    assert caught.value.__cause__ is not None
    assert session.view.status is LocalBookStatus.STALE


def test_synchronized_updates_preserve_arrival_order_and_book_change_semantics() -> (
    None
):
    """The session exposes all events and compares complete immutable books."""
    events: tuple[MarketDataEvent, ...] = (
        _snapshot(),
        _change(Side.BUY, "0.45", "4", 1),
        _change(Side.BUY, "0.45", "4", 2),
        _change(Side.SELL, "0.70", "0", 3),
        _trade(4),
        _snapshot(5, _book((("0.45", "4"), ("0.40", "2")), (("0.60", "3"),))),
    )
    session = MarketDataSession(FakeGateway(events), _MARKET_ID, _TOKEN_ID, _TICK_SIZE)
    updates = asyncio.run(_collect(session))
    assert [update.event for update in updates] == list(events)
    assert [update.book_changed for update in updates] == [
        True,
        True,
        False,
        False,
        False,
        False,
    ]
    assert updates[-1].book_view.book == _book(
        (("0.45", "4"), ("0.40", "2")), (("0.60", "3"),)
    )


@pytest.mark.parametrize(
    "event",
    [
        TickSizeChanged(_MARKET_ID, _TOKEN_ID, _TICK_SIZE, TickSize("0.005"), _at(1)),
        MarketDataDisconnected(None, _at(1)),
        MarketDataReconnected(_at(1)),
    ],
)
def test_stale_events_yield_then_end(event: MarketDataEvent) -> None:
    """Known continuity boundaries are published once before session completion."""
    session = MarketDataSession(
        FakeGateway((_snapshot(), event, _trade(2))), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    updates = asyncio.run(_collect(session))
    assert [update.event for update in updates] == [_snapshot(), event]
    assert updates[-1].book_view.status is LocalBookStatus.STALE
    assert updates[-1].book_changed is False


def test_crossed_incremental_update_is_visible_and_terminal() -> None:
    """Retain the observed crossed book but consume no further events afterward."""
    crossed = _change(Side.BUY, "0.61", "1", 1)
    session = MarketDataSession(
        FakeGateway((_snapshot(), crossed, _trade(2))),
        _MARKET_ID,
        _TOKEN_ID,
        _TICK_SIZE,
    )
    updates = asyncio.run(_collect(session))
    assert [update.event for update in updates] == [_snapshot(), crossed]
    assert updates[-1].book_view.status is LocalBookStatus.INVALID
    assert updates[-1].book_changed is True


def test_completion_marks_retained_book_stale_without_changing_its_timestamp() -> None:
    """Natural stream exhaustion requires a new snapshot without a synthetic event."""
    session = MarketDataSession(
        FakeGateway((_snapshot(),)), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    asyncio.run(_collect(session))
    assert session.view.status is LocalBookStatus.STALE
    assert session.view.status_reason == "market-data stream ended"
    assert session.view.last_observed_at == _at()
    assert session.view.book == _book()


@pytest.mark.parametrize(
    "events",
    [(), (_trade(),)],
)
def test_completion_before_snapshot_leaves_session_stale(
    events: tuple[MarketDataEvent, ...],
) -> None:
    """Natural exhaustion before a snapshot never manufactures synchronization."""
    session = MarketDataSession(FakeGateway(events), _MARKET_ID, _TOKEN_ID, _TICK_SIZE)
    updates = asyncio.run(_collect(session))
    assert len(updates) == len(events)
    assert session.view.status is LocalBookStatus.STALE
    assert session.view.book is None


def test_state_machine_failures_are_chained_as_synchronization_loss() -> None:
    """Out-of-order normalized events end the session with their original cause."""
    session = MarketDataSession(
        FakeGateway((_snapshot(1), _change(Side.BUY, "0.45", "1", 0))),
        _MARKET_ID,
        _TOKEN_ID,
        _TICK_SIZE,
    )
    with pytest.raises(MarketDataSynchronizationLost) as caught:
        asyncio.run(_collect(session))
    assert caught.value.__cause__ is not None
    assert session.view.status is LocalBookStatus.STALE
    assert session.view.last_observed_at == _at(1)


def test_gateway_failure_is_wrapped_and_preserves_its_cause() -> None:
    """Infrastructure failures do not escape through the application contract."""
    failure = RuntimeError("adapter disconnected")
    session = MarketDataSession(
        FakeGateway((_snapshot(),), failure), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    with pytest.raises(MarketDataSessionError) as caught:
        asyncio.run(_collect(session))
    assert caught.value.__cause__ is failure
    assert session.view.status is LocalBookStatus.STALE
    assert session.view.status_reason == "market-data stream failed"


def test_session_is_single_use_before_or_after_completion() -> None:
    """No session object can begin a second connection lifecycle."""
    session = MarketDataSession(
        FakeGateway((_snapshot(),)), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    iterator = session.stream_updates()
    with pytest.raises(MarketDataSessionAlreadyStarted):
        session.stream_updates()
    assert asyncio.run(_consume(iterator))
    with pytest.raises(MarketDataSessionAlreadyStarted):
        session.stream_updates()


async def _consume(iterator: AsyncIterator[MarketDataSessionUpdate]) -> bool:
    """Consume a supplied iterator and confirm it produced an update."""
    return bool([update async for update in iterator])


def test_cancellation_propagates_and_closes_the_gateway_iterator() -> None:
    """Caller cancellation does not become a session error or leave a stream open."""
    gateway = BlockingGateway()
    session = MarketDataSession(gateway, _MARKET_ID, _TOKEN_ID, _TICK_SIZE)
    assert asyncio.run(_cancel_while_waiting(session, gateway))
    assert gateway.iterator.closed is True


class BlockingGateway:
    """Expose an iterator that waits until its consumer is cancelled."""

    def __init__(self) -> None:
        """Create the reusable test iterator holder."""
        self.iterator = BlockingIterator()

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        """Return the iterator that records its eventual close."""
        return self.iterator


class BlockingIterator:
    """An async iterator whose next item blocks indefinitely."""

    def __init__(self) -> None:
        """Initialize its observation and closure flags."""
        self.started = asyncio.Event()
        self.closed = False

    def __aiter__(self) -> AsyncIterator[MarketDataEvent]:
        """Return this iterator for asynchronous iteration."""
        return self

    async def __anext__(self) -> MarketDataEvent:
        """Wait until the task consuming this iterator is cancelled."""
        self.started.set()
        await asyncio.Event().wait()
        raise StopAsyncIteration

    async def aclose(self) -> None:
        """Record that the session closed this iterator on cancellation."""
        self.closed = True


async def _cancel_while_waiting(
    session: MarketDataSession, gateway: BlockingGateway
) -> bool:
    """Cancel active iteration and report that its cancellation propagated."""
    task = asyncio.create_task(_collect(session))
    await gateway.iterator.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    return True


def test_unknown_gateway_event_is_wrapped_as_an_application_failure() -> None:
    """A malformed gateway iterator cannot leak implementation exceptions outward."""
    invalid_event = cast(MarketDataEvent, object())
    session = MarketDataSession(
        FakeGateway((invalid_event,)), _MARKET_ID, _TOKEN_ID, _TICK_SIZE
    )
    with pytest.raises(MarketDataSessionError):
        asyncio.run(_collect(session))
    assert session.view.status is LocalBookStatus.STALE


class FiniteIterator:
    """A finite async iterator intentionally lacking an ``aclose`` method."""

    def __init__(self, events: tuple[MarketDataEvent, ...]) -> None:
        """Keep events and the position of the next event to return."""
        self._events = events
        self._index = 0

    def __aiter__(self) -> AsyncIterator[MarketDataEvent]:
        """Return this iterator for asynchronous iteration."""
        return self

    async def __anext__(self) -> MarketDataEvent:
        """Return the next configured event or complete normally."""
        if self._index == len(self._events):
            raise StopAsyncIteration
        event = self._events[self._index]
        self._index += 1
        return event


class ClosingIterator(FiniteIterator):
    """A finite iterator whose close operation can fail for cleanup coverage."""

    def __init__(
        self, events: tuple[MarketDataEvent, ...], close_error: BaseException
    ) -> None:
        """Store the failure that this iterator's close operation will raise."""
        super().__init__(events)
        self._close_error = close_error

    async def aclose(self) -> None:
        """Raise the configured close failure."""
        raise self._close_error


class IteratorGateway:
    """Return a caller-supplied iterator from the normalized gateway boundary."""

    def __init__(self, iterator: AsyncIterator[MarketDataEvent]) -> None:
        """Store the iterator to return for the sole subscription."""
        self._iterator = iterator

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        """Return the configured iterator without adding transport behavior."""
        return self._iterator


def test_session_allows_iterators_without_aclose() -> None:
    """Cleanup tolerates structural gateway implementations without a close hook."""
    session = MarketDataSession(
        IteratorGateway(FiniteIterator((_snapshot(),))),
        _MARKET_ID,
        _TOKEN_ID,
        _TICK_SIZE,
    )
    assert len(asyncio.run(_collect(session))) == 1


def test_session_ignores_non_cancellation_iterator_close_errors() -> None:
    """A cleanup failure cannot hide a completed normalized market-data stream."""
    session = MarketDataSession(
        IteratorGateway(ClosingIterator((_snapshot(),), RuntimeError("close failed"))),
        _MARKET_ID,
        _TOKEN_ID,
        _TICK_SIZE,
    )
    assert len(asyncio.run(_collect(session))) == 1


def test_cancellation_during_iterator_close_propagates() -> None:
    """Cancellation raised by a close hook stays visible to the session caller."""
    session = MarketDataSession(
        IteratorGateway(ClosingIterator((_snapshot(),), asyncio.CancelledError())),
        _MARKET_ID,
        _TOKEN_ID,
        _TICK_SIZE,
    )
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_collect(session))
