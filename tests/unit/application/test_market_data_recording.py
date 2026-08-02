"""Tests for recording one normalized market-data session into a journal."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    InvalidMarketDataRecording,
    MarketDataJournalSession,
    MarketDataRecorderAlreadyStarted,
    MarketDataRecordingClockError,
    MarketDataRecordingError,
    MarketDataRecordingFinalizationError,
    MarketDataRecordingResult,
    MarketDataSession,
    MarketDataSessionId,
    MarketDataSessionRecorder,
    MarketDataSessionUpdate,
    MarketDataStreamGateway,
    MarketId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
    TokenId,
    TradeObserved,
)
from horus_engine.application.market_data_recording import _close_iterator
from horus_engine.application.order_book_state import LocalOrderBookView
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

_MARKET_ID = MarketId("market-1")
_TOKEN_ID = TokenId("token-1")
_TICK_SIZE = TickSize("0.01")
_START = datetime(2026, 8, 2, 12, tzinfo=UTC)


def _snapshot(minutes: int = 0) -> BookSnapshotReceived:
    """Build an authoritative event for a real market-data session."""
    book = OrderBook(
        (OrderBookLevel(Price("0.40"), Quantity("2")),),
        (OrderBookLevel(Price("0.60"), Quantity("3")),),
    )
    return BookSnapshotReceived(
        _MARKET_ID, _TOKEN_ID, book, _START + timedelta(minutes=minutes)
    )


def _trade(minutes: int = 1) -> TradeObserved:
    """Build a trade event that the recorder must retain without special handling."""
    return TradeObserved(
        _MARKET_ID,
        _TOKEN_ID,
        Side.BUY,
        Price("0.50"),
        Quantity("1"),
        _START + timedelta(minutes=minutes),
    )


class FakeStreamGateway:
    """Yield configured normalized events and expose consumption lifecycle state."""

    def __init__(
        self,
        events: tuple[BookSnapshotReceived | TradeObserved, ...],
        failure: Exception | None = None,
    ) -> None:
        """Configure a finite stream with an optional terminal failure."""
        self._events = events
        self._failure = failure
        self.calls = 0
        self.yielded: list[BookSnapshotReceived | TradeObserved] = []
        self.closed = False

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[BookSnapshotReceived | TradeObserved]:
        """Return one owned iterator for the requested market token pair."""
        assert (market_id, token_ids) == (_MARKET_ID, (_TOKEN_ID,))
        self.calls += 1
        return self._stream()

    async def _stream(self) -> AsyncIterator[BookSnapshotReceived | TradeObserved]:
        """Yield arrival-ordered events while recording iterator closure."""
        try:
            for event in self._events:
                self.yielded.append(event)
                yield event
            if self._failure is not None:
                raise self._failure
        finally:
            self.closed = True


class BlockingStreamGateway:
    """Expose a real session whose upstream iterator blocks until cancellation."""

    def __init__(self) -> None:
        """Create the close-observable blocking iterator."""
        self.iterator = BlockingIterator()

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[BookSnapshotReceived]:
        """Return the blocking upstream iterator for the configured identity."""
        assert (market_id, token_ids) == (_MARKET_ID, (_TOKEN_ID,))
        return self.iterator


class BlockingIterator:
    """Wait for cancellation and record explicit closure."""

    def __init__(self) -> None:
        """Initialize lifecycle signals."""
        self.started = asyncio.Event()
        self.closed = False

    def __aiter__(self) -> AsyncIterator[BookSnapshotReceived]:
        """Return this iterator."""
        return self

    async def __anext__(self) -> BookSnapshotReceived:
        """Block until the consuming task is cancelled."""
        self.started.set()
        await asyncio.Event().wait()
        raise StopAsyncIteration

    async def aclose(self) -> None:
        """Record the caller's close request."""
        self.closed = True


class FakeJournal:
    """In-memory exchange-neutral journal with individually controlled failures."""

    def __init__(
        self,
        *,
        start_failure: Exception | None = None,
        append_failure_at: int | None = None,
        finish_failure: Exception | None = None,
        block_start: bool = False,
        start_cancelled: bool = False,
        finish_cancelled: bool = False,
    ) -> None:
        """Configure journal behavior without an infrastructure adapter."""
        self._start_failure = start_failure
        self._append_failure_at = append_failure_at
        self._finish_failure = finish_failure
        self._block_start = block_start
        self._start_cancelled = start_cancelled
        self._finish_cancelled = finish_cancelled
        self.start_entered = asyncio.Event()
        self.release_start = asyncio.Event()
        self.initialize_calls = 0
        self.started: list[MarketDataJournalSession] = []
        self.appended: list[
            tuple[MarketDataSessionId, int, MarketDataSessionUpdate]
        ] = []
        self.finished: list[
            tuple[MarketDataSessionId, datetime, LocalOrderBookView]
        ] = []

    async def initialize(self) -> None:
        """Record a call that the recorder must never make."""
        self.initialize_calls += 1

    async def start_session(self, session: MarketDataJournalSession) -> None:
        """Record session creation or produce its configured failure."""
        self.started.append(session)
        self.start_entered.set()
        if self._block_start:
            await self.release_start.wait()
        if self._start_cancelled:
            raise asyncio.CancelledError
        if self._start_failure is not None:
            raise self._start_failure

    async def append_update(
        self,
        session_id: MarketDataSessionId,
        sequence_number: int,
        update: MarketDataSessionUpdate,
    ) -> None:
        """Append in observed order unless this exact append is configured to fail."""
        if self._append_failure_at == sequence_number:
            raise RuntimeError("append failed")
        self.appended.append((session_id, sequence_number, update))

    async def finish_session(
        self,
        session_id: MarketDataSessionId,
        finished_at: datetime,
        final_view: LocalOrderBookView,
    ) -> None:
        """Record one terminal view or produce its configured failure."""
        if self._finish_cancelled:
            raise asyncio.CancelledError
        if self._finish_failure is not None:
            raise self._finish_failure
        self.finished.append((session_id, finished_at, final_view))

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession | None:
        """Return no read model because this fake records only write interactions."""
        return None

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        """Return no read models because this fake records only write interactions."""
        return ()


class Clock:
    """Return exact lifecycle instants in call order."""

    def __init__(self, values: list[datetime]) -> None:
        """Store the caller-selected deterministic clock values."""
        self._values = values
        self.calls = 0

    def __call__(self) -> datetime:
        """Return exactly one configured timestamp."""
        self.calls += 1
        return self._values.pop(0)


def _session(gateway: MarketDataStreamGateway) -> MarketDataSession:
    """Construct the actual application session used by every recorder scenario."""
    return MarketDataSession(gateway, _MARKET_ID, _TOKEN_ID, _TICK_SIZE)


def test_recorder_persists_arrival_order_and_returns_final_view() -> None:
    """A normal lifecycle starts once, writes every update, then finalizes once."""
    stream = FakeStreamGateway((_snapshot(), _trade()))
    journal = FakeJournal()
    clock = Clock([_START + timedelta(hours=1), _START + timedelta(hours=2)])
    recorder = MarketDataSessionRecorder(
        _session(stream), journal, MarketDataSessionId("recording-1"), clock
    )

    result = asyncio.run(recorder.run())

    assert result.session_id == MarketDataSessionId("recording-1")
    assert result.started_at == _START + timedelta(hours=1)
    assert result.finished_at == _START + timedelta(hours=2)
    assert result.updates_recorded == 2
    assert result.final_view == journal.finished[0][2]
    assert [number for _, number, _ in journal.appended] == [1, 2]
    assert [update.event for _, _, update in journal.appended] == [
        _snapshot(),
        _trade(),
    ]
    assert journal.started[0].market_id == _MARKET_ID
    assert journal.started[0].token_id == _TOKEN_ID
    assert journal.started[0].initial_tick_size == _TICK_SIZE
    assert journal.initialize_calls == 0
    assert asyncio.run(journal.get_session(result.session_id)) is None
    assert asyncio.run(journal.list_updates(result.session_id)) == ()
    assert stream.closed is True
    assert clock.calls == 2


def test_recorder_finalizes_zero_update_session() -> None:
    """An exhausted session remains a useful completed recording with zero updates."""
    stream = FakeStreamGateway(())
    journal = FakeJournal()
    recorder = MarketDataSessionRecorder(
        _session(stream),
        journal,
        MarketDataSessionId("recording-zero"),
        Clock([_START, _START + timedelta(seconds=1)]),
    )

    result = asyncio.run(recorder.run())

    assert result.updates_recorded == 0
    assert journal.appended == []
    assert len(journal.finished) == 1
    assert result.final_view.status_reason == "market-data stream ended"


def test_recorder_wraps_session_failure_after_finalizing_partial_evidence() -> None:
    """A stream failure keeps prior evidence and its current diagnostic final view."""
    stream_failure = RuntimeError("websocket failed")
    stream = FakeStreamGateway((_snapshot(),), stream_failure)
    journal = FakeJournal()
    recorder = MarketDataSessionRecorder(
        _session(stream),
        journal,
        MarketDataSessionId("recording-stream-failure"),
        Clock([_START, _START + timedelta(seconds=1)]),
    )

    with pytest.raises(MarketDataRecordingError) as caught:
        asyncio.run(recorder.run())

    assert caught.value.__cause__ is not None
    assert [number for _, number, _ in journal.appended] == [1]
    assert len(journal.finished) == 1
    assert journal.finished[0][2].status_reason == "market-data stream failed"


def test_recorder_stops_and_closes_stream_after_append_failure() -> None:
    """A failed append neither consumes later events nor counts the failed update."""
    stream = FakeStreamGateway((_snapshot(), _trade()))
    journal = FakeJournal(append_failure_at=1)
    recorder = MarketDataSessionRecorder(
        _session(stream),
        journal,
        MarketDataSessionId("recording-append-failure"),
        Clock([_START, _START + timedelta(seconds=1)]),
    )

    with pytest.raises(MarketDataRecordingError) as caught:
        asyncio.run(recorder.run())

    assert isinstance(caught.value.__cause__, RuntimeError)
    assert journal.appended == []
    assert stream.yielded == [_snapshot()]
    assert stream.closed is True
    assert len(journal.finished) == 1


def test_recorder_wraps_start_failure_without_consuming_or_finishing() -> None:
    """An unsuccessful journal start leaves the session untouched and unfinished."""
    start_failure = RuntimeError("journal unavailable")
    stream = FakeStreamGateway((_snapshot(),))
    journal = FakeJournal(start_failure=start_failure)
    recorder = MarketDataSessionRecorder(
        _session(stream),
        journal,
        MarketDataSessionId("recording-start-failure"),
        Clock([_START]),
    )

    with pytest.raises(MarketDataRecordingError) as caught:
        asyncio.run(recorder.run())

    assert caught.value.__cause__ is start_failure
    assert stream.calls == 0
    assert journal.finished == []


def test_recorder_propagates_cancellation_while_starting() -> None:
    """Cancellation before a completed journal start never consumes or finishes."""
    stream = FakeStreamGateway((_snapshot(),))
    journal = FakeJournal(start_cancelled=True)
    recorder = MarketDataSessionRecorder(
        _session(stream),
        journal,
        MarketDataSessionId("recording-start-cancelled"),
        Clock([_START]),
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(recorder.run())

    assert stream.calls == 0
    assert journal.finished == []


def test_recorder_wraps_normal_finalization_failure() -> None:
    """A journal failure at normal completion never returns a successful result."""
    finish_failure = RuntimeError("finish failed")
    journal = FakeJournal(finish_failure=finish_failure)
    recorder = MarketDataSessionRecorder(
        _session(FakeStreamGateway(())),
        journal,
        MarketDataSessionId("recording-finish-failure"),
        Clock([_START, _START + timedelta(seconds=1)]),
    )

    with pytest.raises(MarketDataRecordingFinalizationError) as caught:
        asyncio.run(recorder.run())

    assert caught.value.__cause__ is finish_failure
    assert journal.finished == []


def test_recorder_retains_operational_and_finalization_failures() -> None:
    """Keep the operational cause primary and finalization context visible."""
    stream = FakeStreamGateway((), RuntimeError("stream failed"))
    journal = FakeJournal(finish_failure=RuntimeError("finish failed"))
    recorder = MarketDataSessionRecorder(
        _session(stream),
        journal,
        MarketDataSessionId("recording-double-failure"),
        Clock([_START, _START + timedelta(seconds=1)]),
    )

    with pytest.raises(MarketDataRecordingFinalizationError) as caught:
        asyncio.run(recorder.run())

    assert caught.value.__cause__ is not None
    assert caught.value.__context__ is not None


def test_recorder_preserves_clock_error_while_finalizing_an_operational_failure() -> (
    None
):
    """An invalid final clock never turns a partial-session failure into a finish."""
    recorder = MarketDataSessionRecorder(
        _session(FakeStreamGateway((), RuntimeError("stream failed"))),
        FakeJournal(),
        MarketDataSessionId("recording-failure-clock"),
        Clock([_START, _START - timedelta(seconds=1)]),
    )

    with pytest.raises(MarketDataRecordingClockError):
        asyncio.run(recorder.run())


@pytest.mark.parametrize(
    ("clock_values", "expected_starts", "expected_finishes"),
    [
        ([datetime(2026, 8, 2, 12)], 0, 0),
        ([_START, datetime(2026, 8, 2, 13)], 1, 0),
        ([_START, _START - timedelta(seconds=1)], 1, 0),
    ],
)
def test_recorder_rejects_invalid_clock_values_before_invalid_journal_writes(
    clock_values: list[datetime], expected_starts: int, expected_finishes: int
) -> None:
    """Naive and backward times never create invalid journal lifecycle state."""
    journal = FakeJournal()
    recorder = MarketDataSessionRecorder(
        _session(FakeStreamGateway(())),
        journal,
        MarketDataSessionId("recording-clock"),
        Clock(clock_values),
    )

    with pytest.raises(MarketDataRecordingClockError):
        asyncio.run(recorder.run())

    assert len(journal.started) == expected_starts
    assert len(journal.finished) == expected_finishes


def test_recorder_is_single_use_even_while_first_run_is_in_progress() -> None:
    """A second run cannot join or repeat the first recorder lifecycle."""

    async def scenario() -> None:
        journal = FakeJournal(block_start=True)
        recorder = MarketDataSessionRecorder(
            _session(FakeStreamGateway(())),
            journal,
            MarketDataSessionId("recording-single-use"),
            Clock([_START, _START + timedelta(seconds=1)]),
        )
        first = asyncio.create_task(recorder.run())
        await journal.start_entered.wait()
        with pytest.raises(MarketDataRecorderAlreadyStarted):
            await recorder.run()
        journal.release_start.set()
        await first
        with pytest.raises(MarketDataRecorderAlreadyStarted):
            await recorder.run()

    asyncio.run(scenario())


def test_recorder_cancellation_closes_session_and_finalizes_once() -> None:
    """Cancellation stays unwrapped while the started journal gets final state."""

    async def scenario() -> None:
        stream = BlockingStreamGateway()
        journal = FakeJournal()
        recorder = MarketDataSessionRecorder(
            _session(stream),
            journal,
            MarketDataSessionId("recording-cancelled"),
            Clock([_START, _START + timedelta(seconds=1)]),
        )
        task = asyncio.create_task(recorder.run())
        await stream.iterator.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.iterator.closed is True
        assert len(journal.finished) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("finish_cancelled", [False, True])
def test_recorder_cancellation_never_becomes_a_finalization_error(
    finish_cancelled: bool,
) -> None:
    """Cleanup finalization problems never replace task cancellation."""

    async def scenario() -> None:
        stream = BlockingStreamGateway()
        journal = FakeJournal(
            finish_failure=None if finish_cancelled else RuntimeError("finish failed"),
            finish_cancelled=finish_cancelled,
        )
        recorder = MarketDataSessionRecorder(
            _session(stream),
            journal,
            MarketDataSessionId("recording-cancel-cleanup"),
            Clock([_START, _START + timedelta(seconds=1)]),
        )
        task = asyncio.create_task(recorder.run())
        await stream.iterator.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_recorder_propagates_cancellation_from_finalization() -> None:
    """A journal cancellation at normal completion is never wrapped as a failure."""
    recorder = MarketDataSessionRecorder(
        _session(FakeStreamGateway(())),
        FakeJournal(finish_cancelled=True),
        MarketDataSessionId("recording-finish-cancelled"),
        Clock([_START, _START + timedelta(seconds=1)]),
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(recorder.run())


def test_recorder_propagates_cancellation_while_finalizing_a_failure() -> None:
    """Cancellation from partial-evidence finalization remains unwrapped."""
    recorder = MarketDataSessionRecorder(
        _session(FakeStreamGateway((_snapshot(),))),
        FakeJournal(append_failure_at=1, finish_cancelled=True),
        MarketDataSessionId("recording-failure-finish-cancelled"),
        Clock([_START, _START + timedelta(seconds=1)]),
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(recorder.run())


def test_result_rejects_invalid_lifecycle_metadata() -> None:
    """The immutable public result rejects naive, backward, and non-integer counts."""
    view = _session(FakeStreamGateway(())).view
    session_id = MarketDataSessionId("recording-result")
    with pytest.raises(InvalidMarketDataRecording):
        MarketDataRecordingResult(session_id, datetime(2026, 8, 2, 12), _START, 0, view)
    with pytest.raises(InvalidMarketDataRecording):
        MarketDataRecordingResult(session_id, _START, datetime(2026, 8, 2, 12), 0, view)
    with pytest.raises(InvalidMarketDataRecording):
        MarketDataRecordingResult(
            session_id, _START, _START - timedelta(seconds=1), 0, view
        )
    for count in (True, -1, "1"):
        with pytest.raises(InvalidMarketDataRecording):
            MarketDataRecordingResult(session_id, _START, _START, count, view)  # type: ignore[arg-type]


class NoCloseIterator:
    """An async iterator intentionally lacking an ``aclose`` method."""

    def __aiter__(self) -> AsyncIterator[MarketDataSessionUpdate]:
        """Return this iterator."""
        return self

    async def __anext__(self) -> MarketDataSessionUpdate:
        """Finish immediately."""
        raise StopAsyncIteration


class FailingCloseIterator(NoCloseIterator):
    """An iterator whose cleanup failure must not replace a recording failure."""

    async def aclose(self) -> None:
        """Simulate non-cancellation cleanup failure."""
        raise RuntimeError("close failed")


class CancelledCloseIterator(NoCloseIterator):
    """An iterator whose cleanup is itself cancelled."""

    async def aclose(self) -> None:
        """Propagate cancellation from cleanup."""
        raise asyncio.CancelledError


def test_interrupted_iterator_cleanup_is_best_effort_but_propagates_cancellation() -> (
    None
):
    """Only cancellation can replace the original reason for ending iteration."""

    async def scenario() -> None:
        await _close_iterator(None)
        await _close_iterator(NoCloseIterator())
        await _close_iterator(FailingCloseIterator())
        with pytest.raises(asyncio.CancelledError):
            await _close_iterator(CancelledCloseIterator())

    asyncio.run(scenario())
