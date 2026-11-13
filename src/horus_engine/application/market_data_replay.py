"""Deterministic replay of one normalized market-data journal session."""

import asyncio
from dataclasses import dataclass

from .errors import (
    MarketDataReplayIntegrityError,
    MarketDataReplaySequenceError,
    MarketDataReplaySessionNotFound,
)
from .events import MarketDataEvent
from .market_data_journal import (
    MarketDataJournalReader,
    MarketDataSessionId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
)
from .order_book_state import LocalBookStatus, LocalOrderBookState, LocalOrderBookView


@dataclass(frozen=True)
class MarketDataReplayStep:
    """One normalized event and the reconstructed state immediately after it."""

    session_id: MarketDataSessionId
    sequence_number: int
    event: MarketDataEvent
    view: LocalOrderBookView
    book_changed: bool

    def __post_init__(self) -> None:
        """Require an unambiguous positive arrival sequence number."""
        if (
            isinstance(self.sequence_number, bool)
            or not isinstance(self.sequence_number, int)
            or self.sequence_number < 1
        ):
            raise ValueError("replay sequence number must be a positive integer")


@dataclass(frozen=True)
class MarketDataReplayResult:
    """Immutable reconstructed history and terminal state for one session."""

    session: PersistedMarketDataSession
    steps: tuple[MarketDataReplayStep, ...]
    last_event_view: LocalOrderBookView
    final_view: LocalOrderBookView
    is_finished: bool

    def __post_init__(self) -> None:
        """Keep replay output immutable and lifecycle state explicit."""
        if not isinstance(self.steps, tuple):
            raise ValueError("replay steps must be a tuple")
        if not isinstance(self.is_finished, bool):
            raise ValueError("replay finished flag must be a boolean")


class MarketDataSessionReplayer:
    """Read and verify one persisted normalized market-data lifecycle."""

    def __init__(self, journal: MarketDataJournalReader) -> None:
        """Retain an application-level journal without initializing it."""
        self._journal = journal

    async def replay(self, session_id: MarketDataSessionId) -> MarketDataReplayResult:
        """Reconstruct a session in recorded arrival order or fail explicitly."""
        session = await self._journal.get_session(session_id)
        if session is None:
            raise MarketDataReplaySessionNotFound(
                f"market-data replay session not found: {session_id}"
            )
        updates = await self._journal.list_updates(session_id)
        self._validate_session_sequence(session, updates)
        state = LocalOrderBookState(
            session.market_id, session.token_id, session.initial_tick_size
        )
        steps: list[MarketDataReplayStep] = []
        for expected_sequence, update in enumerate(updates, start=1):
            self._validate_update_sequence(session_id, update, expected_sequence)
            self._validate_update_session(session_id, update)
            previous_book = state.view.book
            try:
                view = state.apply(update.event)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise self._integrity_error(
                    session_id, update.sequence_number, "event application"
                ) from error
            book_changed = previous_book != view.book
            self._validate_post_event(session, update, view, book_changed)
            steps.append(
                MarketDataReplayStep(
                    session_id,
                    update.sequence_number,
                    update.event,
                    view,
                    book_changed,
                )
            )
        last_event_view = state.view
        final_view, is_finished = self._final_view(
            session, last_event_view, bool(steps)
        )
        return MarketDataReplayResult(
            session, tuple(steps), last_event_view, final_view, is_finished
        )

    @staticmethod
    def _validate_session_sequence(
        session: PersistedMarketDataSession,
        updates: tuple[PersistedMarketDataUpdate, ...],
    ) -> None:
        """Detect a journal header that disagrees with its returned history."""
        if session.last_sequence_number != len(updates):
            raise MarketDataReplaySequenceError(
                f"market-data replay sequence mismatch for session {session.session_id}"
            )

    @staticmethod
    def _validate_update_sequence(
        session_id: MarketDataSessionId,
        update: PersistedMarketDataUpdate,
        expected_sequence: int,
    ) -> None:
        """Require gateway output to be contiguous without silently sorting it."""
        sequence_number = update.sequence_number
        if (
            isinstance(sequence_number, bool)
            or not isinstance(sequence_number, int)
            or sequence_number != expected_sequence
        ):
            raise MarketDataReplaySequenceError(
                "market-data replay sequence mismatch "
                f"for session {session_id} at position {expected_sequence}"
            )

    @staticmethod
    def _validate_update_session(
        session_id: MarketDataSessionId, update: PersistedMarketDataUpdate
    ) -> None:
        """Require every returned update to belong to the requested session."""
        if update.session_id != session_id:
            raise MarketDataReplayIntegrityError(
                "market-data replay integrity mismatch for "
                f"session {session_id} sequence {update.sequence_number}: session_id"
            )

    def _validate_post_event(
        self,
        session: PersistedMarketDataSession,
        update: PersistedMarketDataUpdate,
        view: LocalOrderBookView,
        book_changed: bool,
    ) -> None:
        """Compare all persisted post-event evidence to reconstructed state."""
        values = (
            ("market_id", view.market_id, session.market_id),
            ("token_id", view.token_id, session.token_id),
            ("book_changed", book_changed, update.book_changed),
            ("post_status", view.status, update.post_status),
            ("post_tick_size", view.tick_size, update.post_tick_size),
            (
                "post_last_observed_at",
                view.last_observed_at,
                update.post_last_observed_at,
            ),
            ("post_status_reason", view.status_reason, update.post_status_reason),
        )
        for field, actual, recorded in values:
            if actual != recorded:
                raise self._integrity_error(
                    session.session_id, update.sequence_number, field
                )

    def _final_view(
        self,
        session: PersistedMarketDataSession,
        last_event_view: LocalOrderBookView,
        has_updates: bool,
    ) -> tuple[LocalOrderBookView, bool]:
        """Validate terminal metadata, allowing only lifecycle-only staleness."""
        terminal_values = (
            session.final_status,
            session.final_tick_size,
            session.final_last_observed_at,
            session.final_reason,
        )
        if session.finished_at is None:
            if any(value is not None for value in terminal_values):
                raise self._integrity_error(
                    session.session_id, None, "terminal metadata"
                )
            return last_event_view, False
        if session.finished_at < session.started_at:
            raise self._integrity_error(session.session_id, None, "finished_at")
        if has_updates and (
            last_event_view.last_observed_at is not None
            and session.finished_at < last_event_view.last_observed_at
        ):
            raise self._integrity_error(session.session_id, None, "finished_at")
        if session.final_status is None or session.final_tick_size is None:
            raise self._integrity_error(session.session_id, None, "terminal metadata")
        if (
            session.final_status == last_event_view.status
            and session.final_tick_size == last_event_view.tick_size
            and session.final_last_observed_at == last_event_view.last_observed_at
            and session.final_reason == last_event_view.status_reason
        ):
            return last_event_view, True
        if (
            last_event_view.status
            in (LocalBookStatus.AWAITING_SNAPSHOT, LocalBookStatus.SYNCHRONIZED)
            and session.final_status is LocalBookStatus.STALE
            and session.final_tick_size == last_event_view.tick_size
            and session.final_last_observed_at == last_event_view.last_observed_at
            and session.final_reason is not None
            and session.final_reason.strip()
        ):
            return (
                LocalOrderBookView(
                    last_event_view.market_id,
                    last_event_view.token_id,
                    last_event_view.tick_size,
                    session.final_status,
                    last_event_view.book,
                    last_event_view.last_observed_at,
                    session.final_reason,
                ),
                True,
            )
        raise self._integrity_error(session.session_id, None, "terminal state")

    @staticmethod
    def _integrity_error(
        session_id: MarketDataSessionId,
        sequence_number: int | None,
        field: str,
    ) -> MarketDataReplayIntegrityError:
        """Create concise corruption errors without exposing event payloads."""
        sequence = "terminal" if sequence_number is None else str(sequence_number)
        return MarketDataReplayIntegrityError(
            "market-data replay integrity mismatch for "
            f"session {session_id} sequence {sequence}: {field}"
        )
