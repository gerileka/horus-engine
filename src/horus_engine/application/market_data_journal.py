"""Exchange-neutral persistence contract for normalized market-data sessions.

Persisted updates intentionally retain post-event state metadata rather than a
second reconstructed ``OrderBook``. A future replay reader can recover books
from authoritative snapshots and subsequent price-level changes without
confusing a partial stored view with ``LocalOrderBookView``.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from horus_engine.domain import TickSize

from .errors import InvalidMarketDataJournal, InvalidMarketDataSessionId
from .events import MarketDataEvent
from .market_data_session import MarketDataSessionUpdate
from .models import MarketId, TokenId, _validate_aware_timestamp, _validate_identifier
from .order_book_state import LocalBookStatus, LocalOrderBookView


@dataclass(frozen=True, init=False)
class MarketDataSessionId:
    """Stable caller-supplied identifier for one recorded market-data session."""

    value: str

    def __init__(self, value: str) -> None:
        """Create a validated identifier without generating or normalizing it."""
        _validate_identifier(value, InvalidMarketDataSessionId)
        object.__setattr__(self, "value", value)

    def __str__(self) -> str:
        """Return the stable string value used by persistence adapters."""
        return self.value


@dataclass(frozen=True)
class MarketDataJournalSession:
    """Metadata required to begin recording one market and outcome token."""

    session_id: MarketDataSessionId
    market_id: MarketId
    token_id: TokenId
    initial_tick_size: TickSize
    started_at: datetime

    def __post_init__(self) -> None:
        """Require an unambiguous start instant."""
        _validate_aware_timestamp(self.started_at, InvalidMarketDataJournal)


@dataclass(frozen=True)
class PersistedMarketDataSession:
    """Immutable recorded session metadata, including optional terminal state."""

    session_id: MarketDataSessionId
    market_id: MarketId
    token_id: TokenId
    initial_tick_size: TickSize
    started_at: datetime
    finished_at: datetime | None
    final_status: LocalBookStatus | None
    final_tick_size: TickSize | None
    final_last_observed_at: datetime | None
    final_reason: str | None
    last_sequence_number: int

    def __post_init__(self) -> None:
        """Keep read models coherent without introducing database-specific state."""
        _validate_aware_timestamp(self.started_at, InvalidMarketDataJournal)
        if (
            isinstance(self.last_sequence_number, bool)
            or not isinstance(self.last_sequence_number, int)
            or self.last_sequence_number < 0
        ):
            raise InvalidMarketDataJournal(
                "last sequence number must be a non-negative integer"
            )
        terminal_values = (self.final_status, self.final_tick_size)
        if self.finished_at is None:
            if any(value is not None for value in terminal_values) or any(
                value is not None
                for value in (self.final_last_observed_at, self.final_reason)
            ):
                raise InvalidMarketDataJournal(
                    "unfinished sessions cannot have terminal state"
                )
            return
        _validate_aware_timestamp(self.finished_at, InvalidMarketDataJournal)
        if self.finished_at < self.started_at:
            raise InvalidMarketDataJournal("session finish must not precede start")
        if any(value is None for value in terminal_values):
            raise InvalidMarketDataJournal(
                "finished sessions require final status and tick size"
            )
        if self.final_last_observed_at is not None:
            _validate_aware_timestamp(
                self.final_last_observed_at, InvalidMarketDataJournal
            )


@dataclass(frozen=True)
class PersistedMarketDataUpdate:
    """One normalized event with state metadata immediately after its processing."""

    session_id: MarketDataSessionId
    sequence_number: int
    event: MarketDataEvent
    book_changed: bool
    post_status: LocalBookStatus
    post_tick_size: TickSize
    post_last_observed_at: datetime | None
    post_status_reason: str | None

    def __post_init__(self) -> None:
        """Validate persistence metadata without reconstructing a complete book."""
        if (
            isinstance(self.sequence_number, bool)
            or not isinstance(self.sequence_number, int)
            or self.sequence_number < 1
        ):
            raise InvalidMarketDataJournal("sequence number must be a positive integer")
        if not isinstance(self.book_changed, bool):
            raise InvalidMarketDataJournal("book_changed must be a boolean")
        if self.post_last_observed_at is not None:
            _validate_aware_timestamp(
                self.post_last_observed_at, InvalidMarketDataJournal
            )


class MarketDataJournalGateway(Protocol):
    """Append-only storage for normalized market-data sessions and updates."""

    async def initialize(self) -> None:
        """Create or validate the adapter's persistence schema."""
        ...

    async def start_session(self, session: MarketDataJournalSession) -> None:
        """Begin one new journal session with no recorded updates."""
        ...

    async def append_update(
        self,
        session_id: MarketDataSessionId,
        sequence_number: int,
        update: MarketDataSessionUpdate,
    ) -> None:
        """Append one arrival-ordered update and post-event state metadata."""
        ...

    async def finish_session(
        self,
        session_id: MarketDataSessionId,
        finished_at: datetime,
        final_view: LocalOrderBookView,
    ) -> None:
        """Record terminal state without adding a synthetic normalized event."""
        ...

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession | None:
        """Return one stored session, or ``None`` for an unknown identifier."""
        ...

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        """Return immutable updates in arrival sequence; unknown sessions fail."""
        ...
