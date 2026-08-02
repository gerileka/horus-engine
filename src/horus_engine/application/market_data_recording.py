"""Application service for recording one normalized market-data lifecycle."""

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime

from .errors import (
    InvalidMarketDataRecording,
    MarketDataRecorderAlreadyStarted,
    MarketDataRecordingClockError,
    MarketDataRecordingError,
    MarketDataRecordingFinalizationError,
)
from .market_data_journal import (
    MarketDataJournalGateway,
    MarketDataJournalSession,
    MarketDataSessionId,
)
from .market_data_session import MarketDataSession, MarketDataSessionUpdate
from .order_book_state import LocalOrderBookView


@dataclass(frozen=True)
class MarketDataRecordingResult:
    """Immutable summary of one successfully finalized recording lifecycle."""

    session_id: MarketDataSessionId
    started_at: datetime
    finished_at: datetime
    updates_recorded: int
    final_view: LocalOrderBookView

    def __post_init__(self) -> None:
        """Require coherent, exchange-neutral lifecycle metadata."""
        _require_aware_timestamp(self.started_at)
        _require_aware_timestamp(self.finished_at)
        if self.finished_at < self.started_at:
            raise InvalidMarketDataRecording("recording finish must not precede start")
        if (
            isinstance(self.updates_recorded, bool)
            or not isinstance(self.updates_recorded, int)
            or self.updates_recorded < 0
        ):
            raise InvalidMarketDataRecording(
                "recorded update count must be a non-negative integer"
            )


class MarketDataSessionRecorder:
    """Record one caller-constructed session into one exchange-neutral journal."""

    def __init__(
        self,
        session: MarketDataSession,
        journal: MarketDataJournalGateway,
        session_id: MarketDataSessionId,
        clock: Callable[[], datetime],
    ) -> None:
        """Retain the caller-owned collaborators for one recording lifecycle."""
        self._session = session
        self._journal = journal
        self._session_id = session_id
        self._clock = clock
        self._started = False

    async def run(self) -> MarketDataRecordingResult:
        """Consume, immediately persist, and finalize one market-data session."""
        if self._started:
            raise MarketDataRecorderAlreadyStarted(
                "market-data recorder already started"
            )
        self._started = True

        started_at = self._now()
        initial_view = self._session.view
        journal_session = MarketDataJournalSession(
            self._session_id,
            initial_view.market_id,
            initial_view.token_id,
            initial_view.tick_size,
            started_at,
        )
        try:
            await self._journal.start_session(journal_session)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise MarketDataRecordingError(
                "could not start market-data recording"
            ) from error

        iterator: AsyncIterator[MarketDataSessionUpdate] | None = None
        updates_recorded = 0
        try:
            iterator = self._session.stream_updates()
            async for update in iterator:
                sequence_number = updates_recorded + 1
                await self._journal.append_update(
                    self._session_id, sequence_number, update
                )
                updates_recorded = sequence_number
        except asyncio.CancelledError:
            await _close_iterator(iterator)
            await self._finalize_after_cancellation(started_at)
            raise
        except Exception as error:
            await _close_iterator(iterator)
            await self._finalize_after_failure(started_at, error)
            raise MarketDataRecordingError("market-data recording failed") from error

        finished_at = self._now()
        await self._finish_or_raise(started_at, finished_at)
        return MarketDataRecordingResult(
            self._session_id,
            started_at,
            finished_at,
            updates_recorded,
            self._session.view,
        )

    def _now(self) -> datetime:
        """Read one timezone-aware lifecycle timestamp from the injected clock."""
        timestamp = self._clock()
        if (
            not isinstance(timestamp, datetime)
            or timestamp.tzinfo is None
            or timestamp.utcoffset() is None
        ):
            raise MarketDataRecordingClockError(
                "market-data recording clock returned a naive timestamp"
            )
        return timestamp

    async def _finalize_after_failure(
        self, started_at: datetime, operational_error: Exception
    ) -> None:
        """Finish partial evidence while retaining operational and finish failures."""
        finished_at = self._now()
        try:
            await self._finish_or_raise(started_at, finished_at)
        except asyncio.CancelledError:
            raise
        except MarketDataRecordingClockError:
            raise
        except Exception:
            raise MarketDataRecordingFinalizationError(
                "could not finalize market-data recording"
            ) from operational_error

    async def _finalize_after_cancellation(self, started_at: datetime) -> None:
        """Best-effort finalize a started journal without replacing cancellation."""
        try:
            await self._finish_or_raise(started_at, self._now())
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def _finish_or_raise(
        self, started_at: datetime, finished_at: datetime
    ) -> None:
        """Finish once, wrapping only journal finalization failures."""
        if finished_at < started_at:
            raise MarketDataRecordingClockError(
                "market-data recording finish precedes start"
            )
        try:
            await self._journal.finish_session(
                self._session_id, finished_at, self._session.view
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise MarketDataRecordingFinalizationError(
                "could not finalize market-data recording"
            ) from error


async def _close_iterator(
    iterator: AsyncIterator[MarketDataSessionUpdate] | None,
) -> None:
    """Close an interrupted owned iterator while preserving its primary failure."""
    if iterator is None:
        return
    close = getattr(iterator, "aclose", None)
    if close is None:
        return
    try:
        await close()
    except asyncio.CancelledError:
        raise
    except Exception:
        return


def _require_aware_timestamp(timestamp: datetime) -> None:
    """Validate result timestamps with the application's focused contract error."""
    if (
        not isinstance(timestamp, datetime)
        or timestamp.tzinfo is None
        or timestamp.utcoffset() is None
    ):
        raise InvalidMarketDataRecording("recording timestamp must be timezone-aware")
