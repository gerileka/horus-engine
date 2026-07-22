"""Asynchronous SQLite implementation of the append-only market-data journal."""

import asyncio
import sqlite3
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TypeAlias, cast

import aiosqlite

from horus_engine.application import (
    BookSnapshotReceived,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataJournalSession,
    MarketDataReconnected,
    MarketDataSessionId,
    MarketDataSessionUpdate,
    MarketId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
    PriceLevelChanged,
    TickSizeChanged,
    TokenId,
    TradeObserved,
)
from horus_engine.application.order_book_state import (
    LocalBookStatus,
    LocalOrderBookView,
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

from .errors import (
    SQLiteJournalPayloadError,
    SQLiteMarketDataJournalError,
    SQLiteSchemaVersionError,
    SQLiteSequenceError,
    SQLiteSessionAlreadyExists,
    SQLiteSessionAlreadyFinished,
    SQLiteSessionNotFound,
)
from .schema import SCHEMA_SQL, SCHEMA_VERSION

_Row: TypeAlias = tuple[object, ...]
_MARKET_EVENT_TYPES = (
    BookSnapshotReceived,
    PriceLevelChanged,
    TradeObserved,
    TickSizeChanged,
)


class SQLiteMarketDataJournal:
    """Store normalized market-data events using a caller-owned connection.

    The adapter never opens, closes, or commits work outside its own SQLite
    savepoints. A savepoint gives every write operation an explicit atomic
    boundary while preserving an enclosing caller transaction when one exists.
    """

    def __init__(self, connection: aiosqlite.Connection) -> None:
        """Retain one non-null caller-managed asynchronous SQLite connection."""
        if connection is None:
            raise ValueError("connection must not be None")
        self._connection = connection
        self._write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Enable foreign keys and create or validate schema version one."""
        async with self._write_lock:
            try:
                await self._connection.execute("PRAGMA foreign_keys = ON")
                version = await self._user_version()
                if version > SCHEMA_VERSION:
                    raise SQLiteSchemaVersionError(
                        "database schema version is unsupported"
                    )
                if version == SCHEMA_VERSION:
                    return
                async with self._savepoint():
                    for statement in _schema_statements():
                        await self._connection.execute(statement)
                    await self._connection.execute(
                        f"PRAGMA user_version = {SCHEMA_VERSION}"
                    )
            except sqlite3.Error as error:
                raise SQLiteMarketDataJournalError(
                    "could not initialize market-data journal"
                ) from error

    async def start_session(self, session: MarketDataJournalSession) -> None:
        """Persist exactly one unfinished session with sequence number zero."""
        async with self._write_lock:
            try:
                async with self._savepoint():
                    if await self._session_row(session.session_id) is not None:
                        raise SQLiteSessionAlreadyExists(
                            "market-data session already exists"
                        )
                    await self._connection.execute(
                        """
                        INSERT INTO market_data_sessions (
                            session_id, market_id, token_id, initial_tick_size,
                            started_at, last_sequence_number
                        ) VALUES (?, ?, ?, ?, ?, 0)
                        """,
                        (
                            session.session_id.value,
                            session.market_id.value,
                            session.token_id.value,
                            _decimal_to_storage(session.initial_tick_size.value),
                            _timestamp_to_storage(session.started_at),
                        ),
                    )
            except sqlite3.IntegrityError as error:
                raise SQLiteSessionAlreadyExists(
                    "market-data session already exists"
                ) from error
            except sqlite3.Error as error:
                raise SQLiteMarketDataJournalError(
                    "could not start market-data session"
                ) from error

    async def append_update(
        self,
        session_id: MarketDataSessionId,
        sequence_number: int,
        update: MarketDataSessionUpdate,
    ) -> None:
        """Atomically append one normalized event at its next arrival sequence."""
        if (
            isinstance(sequence_number, bool)
            or not isinstance(sequence_number, int)
            or sequence_number < 1
        ):
            raise SQLiteSequenceError("sequence number must be a positive integer")
        async with self._write_lock:
            try:
                async with self._savepoint():
                    session = await self._require_active_session(session_id)
                    self._validate_update_identity(session, update)
                    expected_sequence = (
                        _integer(session[10], "last sequence number") + 1
                    )
                    if sequence_number != expected_sequence:
                        raise SQLiteSequenceError(
                            "sequence number is not the expected next value"
                        )
                    event_id = await self._insert_event(
                        session_id, sequence_number, update
                    )
                    await self._insert_event_payload(event_id, update.event)
                    await self._connection.execute(
                        """
                        UPDATE market_data_sessions
                        SET last_sequence_number = ?
                        WHERE session_id = ?
                        """,
                        (sequence_number, session_id.value),
                    )
            except sqlite3.IntegrityError as error:
                raise SQLiteJournalPayloadError(
                    "market-data update violates journal constraints"
                ) from error
            except sqlite3.Error as error:
                raise SQLiteMarketDataJournalError(
                    "could not append market-data update"
                ) from error

    async def finish_session(
        self,
        session_id: MarketDataSessionId,
        finished_at: datetime,
        final_view: LocalOrderBookView,
    ) -> None:
        """Atomically record final local-state metadata without adding an event."""
        finished_text = _timestamp_to_storage(finished_at)
        async with self._write_lock:
            try:
                async with self._savepoint():
                    session = await self._require_session(session_id)
                    if session[5] is not None:
                        raise SQLiteSessionAlreadyFinished(
                            "market-data session is finished"
                        )
                    started_at = _timestamp_from_storage(session[4])
                    if finished_at < started_at:
                        raise SQLiteJournalPayloadError(
                            "session finish must not precede session start"
                        )
                    self._validate_view_identity(session, final_view)
                    await self._connection.execute(
                        """
                        UPDATE market_data_sessions
                        SET finished_at = ?, final_status = ?, final_tick_size = ?,
                            final_last_observed_at = ?, final_reason = ?
                        WHERE session_id = ?
                        """,
                        (
                            finished_text,
                            _status_to_storage(final_view.status),
                            _decimal_to_storage(final_view.tick_size.value),
                            _nullable_timestamp_to_storage(final_view.last_observed_at),
                            final_view.status_reason,
                            session_id.value,
                        ),
                    )
            except sqlite3.IntegrityError as error:
                raise SQLiteJournalPayloadError(
                    "terminal market-data session state is invalid"
                ) from error
            except sqlite3.Error as error:
                raise SQLiteMarketDataJournalError(
                    "could not finish market-data session"
                ) from error

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession | None:
        """Read one session without committing or changing caller connection state."""
        row = await self._session_row(session_id)
        return None if row is None else self._session_from_row(row)

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        """Read all events strictly ordered by persisted arrival sequence."""
        if await self._session_row(session_id) is None:
            raise SQLiteSessionNotFound("market-data session was not found")
        rows = await self._fetchall(
            """
            SELECT event_id, sequence_number, event_type, observed_at, book_changed,
                   post_status, post_tick_size, post_last_observed_at,
                   post_status_reason
            FROM market_data_events
            WHERE session_id = ?
            ORDER BY sequence_number
            """,
            (session_id.value,),
        )
        updates: list[PersistedMarketDataUpdate] = []
        for row in rows:
            updates.append(await self._update_from_row(session_id, row))
        return tuple(updates)

    async def _user_version(self) -> int:
        """Return SQLite's integer schema version with strict payload validation."""
        row = await self._fetchone("PRAGMA user_version")
        assert row is not None
        return _integer(row[0], "schema version")

    async def _session_row(self, session_id: MarketDataSessionId) -> _Row | None:
        """Fetch raw session columns in one stable private order."""
        return await self._fetchone(
            """
            SELECT session_id, market_id, token_id, initial_tick_size, started_at,
                   finished_at, final_status, final_tick_size,
                   final_last_observed_at, final_reason, last_sequence_number
            FROM market_data_sessions WHERE session_id = ?
            """,
            (session_id.value,),
        )

    async def _require_session(self, session_id: MarketDataSessionId) -> _Row:
        """Fetch a required session or raise the adapter's focused not-found error."""
        session = await self._session_row(session_id)
        if session is None:
            raise SQLiteSessionNotFound("market-data session was not found")
        return session

    async def _require_active_session(self, session_id: MarketDataSessionId) -> _Row:
        """Fetch an unfinished session or raise a focused lifecycle error."""
        session = await self._require_session(session_id)
        if session[5] is not None:
            raise SQLiteSessionAlreadyFinished("market-data session is finished")
        return session

    def _validate_update_identity(
        self, session: _Row, update: MarketDataSessionUpdate
    ) -> None:
        """Reject mismatched post views and market-specific event identities."""
        self._validate_view_identity(session, update.book_view)
        event = update.event
        if isinstance(event, _MARKET_EVENT_TYPES) and (
            event.market_id.value != _text(session[1], "market identifier")
            or event.token_id.value != _text(session[2], "token identifier")
        ):
            raise SQLiteJournalPayloadError("event identity does not match session")

    @staticmethod
    def _validate_view_identity(session: _Row, view: LocalOrderBookView) -> None:
        """Require application post-state identity to match its stored session."""
        if view.market_id.value != _text(
            session[1], "market identifier"
        ) or view.token_id.value != _text(session[2], "token identifier"):
            raise SQLiteJournalPayloadError(
                "local book identity does not match session"
            )

    async def _insert_event(
        self,
        session_id: MarketDataSessionId,
        sequence_number: int,
        update: MarketDataSessionUpdate,
    ) -> int:
        """Insert common event and post-state columns and return its generated key."""
        cursor = await self._connection.execute(
            """
            INSERT INTO market_data_events (
                session_id, sequence_number, event_type, observed_at, book_changed,
                post_status, post_tick_size, post_last_observed_at,
                post_status_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id.value,
                sequence_number,
                _event_type(update.event),
                _timestamp_to_storage(update.event.observed_at),
                int(update.book_changed),
                _status_to_storage(update.book_view.status),
                _decimal_to_storage(update.book_view.tick_size.value),
                _nullable_timestamp_to_storage(update.book_view.last_observed_at),
                update.book_view.status_reason,
            ),
        )
        try:
            event_id = cursor.lastrowid
        finally:
            await cursor.close()
        assert isinstance(event_id, int) and not isinstance(event_id, bool)
        return event_id

    async def _insert_event_payload(
        self, event_id: int, event: MarketDataEvent
    ) -> None:
        """Insert exactly one event-specific normalized payload representation."""
        if isinstance(event, BookSnapshotReceived):
            await self._connection.execute(
                "INSERT INTO book_snapshots (event_id) VALUES (?)", (event_id,)
            )
            for side, levels in (
                (Side.BUY, event.book.bids),
                (Side.SELL, event.book.asks),
            ):
                for position, level in enumerate(levels):
                    await self._connection.execute(
                        """
                        INSERT INTO book_snapshot_levels (
                            event_id, side, level_position, price, quantity
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            event_id,
                            side.value,
                            position,
                            _decimal_to_storage(level.price.value),
                            _decimal_to_storage(level.quantity.value),
                        ),
                    )
            return
        if isinstance(event, PriceLevelChanged):
            await self._connection.execute(
                """
                INSERT INTO price_level_changes (event_id, side, price, quantity)
                VALUES (?, ?, ?, ?)
                """,
                (
                    event_id,
                    event.side.value,
                    _decimal_to_storage(event.price.value),
                    _decimal_to_storage(event.quantity.value),
                ),
            )
            return
        if isinstance(event, TradeObserved):
            await self._connection.execute(
                """
                INSERT INTO trades (event_id, aggressor_side, price, quantity)
                VALUES (?, ?, ?, ?)
                """,
                (
                    event_id,
                    event.aggressor_side.value,
                    _decimal_to_storage(event.price.value),
                    _decimal_to_storage(event.quantity.value),
                ),
            )
            return
        if isinstance(event, TickSizeChanged):
            await self._connection.execute(
                """
                INSERT INTO tick_size_changes (event_id, old_tick_size, new_tick_size)
                VALUES (?, ?, ?)
                """,
                (
                    event_id,
                    _decimal_to_storage(event.old_tick_size.value),
                    _decimal_to_storage(event.new_tick_size.value),
                ),
            )
            return
        if isinstance(event, MarketDataDisconnected):
            await self._connection.execute(
                "INSERT INTO connection_events (event_id, reason) VALUES (?, ?)",
                (event_id, event.reason),
            )
            return
        if isinstance(event, MarketDataReconnected):
            await self._connection.execute(
                "INSERT INTO connection_events (event_id, reason) VALUES (?, NULL)",
                (event_id,),
            )
            return
        raise SQLiteJournalPayloadError("market-data event type is unsupported")

    def _session_from_row(self, row: _Row) -> PersistedMarketDataSession:
        """Reconstruct application and domain values from one session result row."""
        finished_at = _nullable_timestamp_from_storage(row[5])
        return PersistedMarketDataSession(
            session_id=MarketDataSessionId(_text(row[0], "session identifier")),
            market_id=MarketId(_text(row[1], "market identifier")),
            token_id=TokenId(_text(row[2], "token identifier")),
            initial_tick_size=TickSize(_decimal_from_storage(row[3], "tick size")),
            started_at=_timestamp_from_storage(row[4]),
            finished_at=finished_at,
            final_status=(None if row[6] is None else _status_from_storage(row[6])),
            final_tick_size=(
                None
                if row[7] is None
                else TickSize(_decimal_from_storage(row[7], "final tick size"))
            ),
            final_last_observed_at=_nullable_timestamp_from_storage(row[8]),
            final_reason=(None if row[9] is None else _text(row[9], "final reason")),
            last_sequence_number=_integer(row[10], "last sequence number"),
        )

    async def _update_from_row(
        self, session_id: MarketDataSessionId, row: _Row
    ) -> PersistedMarketDataUpdate:
        """Reconstruct one fully checked normalized event and its state metadata."""
        event_id = _integer(row[0], "event identifier")
        event_type = _text(row[2], "event type")
        event = await self._event_from_rows(event_id, event_type, row[3])
        book_changed = _integer(row[4], "book_changed")
        if book_changed not in (0, 1):
            raise SQLiteJournalPayloadError("stored book_changed value is invalid")
        return PersistedMarketDataUpdate(
            session_id=session_id,
            sequence_number=_positive_integer(row[1], "sequence number"),
            event=event,
            book_changed=bool(book_changed),
            post_status=_status_from_storage(row[5]),
            post_tick_size=TickSize(_decimal_from_storage(row[6], "post tick size")),
            post_last_observed_at=_nullable_timestamp_from_storage(row[7]),
            post_status_reason=(
                None if row[8] is None else _text(row[8], "post status reason")
            ),
        )

    async def _event_from_rows(
        self, event_id: int, event_type: str, observed_value: object
    ) -> MarketDataEvent:
        """Read one event subtype while rejecting missing or conflicting payloads."""
        snapshot_rows = await self._fetchall(
            "SELECT event_id FROM book_snapshots WHERE event_id = ?", (event_id,)
        )
        price_rows = await self._fetchall(
            "SELECT side, price, quantity FROM price_level_changes WHERE event_id = ?",
            (event_id,),
        )
        trade_rows = await self._fetchall(
            "SELECT aggressor_side, price, quantity FROM trades WHERE event_id = ?",
            (event_id,),
        )
        tick_rows = await self._fetchall(
            """
            SELECT old_tick_size, new_tick_size
            FROM tick_size_changes WHERE event_id = ?
            """,
            (event_id,),
        )
        connection_rows = await self._fetchall(
            "SELECT reason FROM connection_events WHERE event_id = ?", (event_id,)
        )
        subtype_counts = tuple(
            len(rows)
            for rows in (
                snapshot_rows,
                price_rows,
                trade_rows,
                tick_rows,
                connection_rows,
            )
        )
        observed_at = _timestamp_from_storage(observed_value)
        if event_type == "book_snapshot_received":
            _require_only_subtype(subtype_counts, 0)
            return await self._snapshot_event(event_id, observed_at)
        if event_type == "price_level_changed":
            _require_only_subtype(subtype_counts, 1)
            payload = price_rows[0]
            return PriceLevelChanged(
                MarketId(await self._event_market_id(event_id)),
                TokenId(await self._event_token_id(event_id)),
                _side_from_storage(payload[0]),
                Price(_decimal_from_storage(payload[1], "price-level price")),
                NonNegativeQuantity(
                    _decimal_from_storage(payload[2], "price-level quantity")
                ),
                observed_at,
            )
        if event_type == "trade_observed":
            _require_only_subtype(subtype_counts, 2)
            payload = trade_rows[0]
            return TradeObserved(
                MarketId(await self._event_market_id(event_id)),
                TokenId(await self._event_token_id(event_id)),
                _side_from_storage(payload[0]),
                Price(_decimal_from_storage(payload[1], "trade price")),
                Quantity(_decimal_from_storage(payload[2], "trade quantity")),
                observed_at,
            )
        if event_type == "tick_size_changed":
            _require_only_subtype(subtype_counts, 3)
            payload = tick_rows[0]
            return TickSizeChanged(
                MarketId(await self._event_market_id(event_id)),
                TokenId(await self._event_token_id(event_id)),
                TickSize(_decimal_from_storage(payload[0], "old tick size")),
                TickSize(_decimal_from_storage(payload[1], "new tick size")),
                observed_at,
            )
        if event_type == "market_data_disconnected":
            _require_only_subtype(subtype_counts, 4)
            reason = connection_rows[0][0]
            return MarketDataDisconnected(
                None if reason is None else _text(reason, "disconnection reason"),
                observed_at,
            )
        if event_type == "market_data_reconnected":
            _require_only_subtype(subtype_counts, 4)
            if connection_rows[0][0] is not None:
                raise SQLiteJournalPayloadError(
                    "reconnection event must not have a reason"
                )
            return MarketDataReconnected(observed_at)
        raise SQLiteJournalPayloadError("stored market-data event type is unknown")

    async def _snapshot_event(
        self, event_id: int, observed_at: datetime
    ) -> BookSnapshotReceived:
        """Rebuild a complete immutable order book from stored snapshot levels."""
        rows = await self._fetchall(
            """
            SELECT side, level_position, price, quantity
            FROM book_snapshot_levels WHERE event_id = ?
            ORDER BY side, level_position
            """,
            (event_id,),
        )
        levels: dict[Side, list[tuple[int, OrderBookLevel]]] = {
            Side.BUY: [],
            Side.SELL: [],
        }
        for row in rows:
            side = _side_from_storage(row[0])
            position = _integer(row[1], "snapshot level position")
            if position < 0:
                raise SQLiteJournalPayloadError("snapshot level position is invalid")
            levels[side].append(
                (
                    position,
                    OrderBookLevel(
                        Price(_decimal_from_storage(row[2], "snapshot price")),
                        Quantity(_decimal_from_storage(row[3], "snapshot quantity")),
                    ),
                )
            )
        ordered_levels: dict[Side, tuple[OrderBookLevel, ...]] = {}
        for side, side_levels in levels.items():
            positions = [position for position, _ in side_levels]
            if positions != list(range(len(positions))):
                raise SQLiteJournalPayloadError(
                    "snapshot level positions are inconsistent"
                )
            if len({level.price for _, level in side_levels}) != len(side_levels):
                raise SQLiteJournalPayloadError("snapshot contains duplicate prices")
            ordered_levels[side] = tuple(level for _, level in side_levels)
        return BookSnapshotReceived(
            MarketId(await self._event_market_id(event_id)),
            TokenId(await self._event_token_id(event_id)),
            OrderBook(ordered_levels[Side.BUY], ordered_levels[Side.SELL]),
            observed_at,
        )

    async def _event_market_id(self, event_id: int) -> str:
        """Find the fixed session market identity for one event."""
        row = await self._fetchone(
            """
            SELECT sessions.market_id
            FROM market_data_events AS events
            JOIN market_data_sessions AS sessions
                ON sessions.session_id = events.session_id
            WHERE events.event_id = ?
            """,
            (event_id,),
        )
        if row is None:
            raise SQLiteJournalPayloadError("event session identity is missing")
        return _text(row[0], "market identifier")

    async def _event_token_id(self, event_id: int) -> str:
        """Find the fixed session token identity for one event."""
        row = await self._fetchone(
            """
            SELECT sessions.token_id
            FROM market_data_events AS events
            JOIN market_data_sessions AS sessions
                ON sessions.session_id = events.session_id
            WHERE events.event_id = ?
            """,
            (event_id,),
        )
        if row is None:
            raise SQLiteJournalPayloadError("event session identity is missing")
        return _text(row[0], "token identifier")

    async def _fetchone(
        self, query: str, parameters: Sequence[object] = ()
    ) -> _Row | None:
        """Fetch one raw row without altering transaction state."""
        cursor = await self._connection.execute(query, parameters)
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        return None if row is None else cast(_Row, tuple(row))

    async def _fetchall(
        self, query: str, parameters: Sequence[object] = ()
    ) -> tuple[_Row, ...]:
        """Fetch immutable raw rows without altering transaction state."""
        cursor = await self._connection.execute(query, parameters)
        try:
            rows = await cursor.fetchall()
        finally:
            await cursor.close()
        return tuple(cast(_Row, tuple(row)) for row in rows)

    @asynccontextmanager
    async def _savepoint(self) -> AsyncIterator[None]:
        """Run one adapter operation atomically without committing caller work."""
        await self._connection.execute("SAVEPOINT market_data_journal")
        try:
            yield
        except Exception:
            await self._connection.execute("ROLLBACK TO SAVEPOINT market_data_journal")
            await self._connection.execute("RELEASE SAVEPOINT market_data_journal")
            raise
        else:
            await self._connection.execute("RELEASE SAVEPOINT market_data_journal")


def _schema_statements() -> tuple[str, ...]:
    """Return individual schema statements suitable for an atomic savepoint."""
    return tuple(
        statement.strip() for statement in SCHEMA_SQL.split(";") if statement.strip()
    )


def _event_type(event: MarketDataEvent) -> str:
    """Map each normalized event type to its stable persisted discriminator."""
    if isinstance(event, BookSnapshotReceived):
        return "book_snapshot_received"
    if isinstance(event, PriceLevelChanged):
        return "price_level_changed"
    if isinstance(event, TradeObserved):
        return "trade_observed"
    if isinstance(event, TickSizeChanged):
        return "tick_size_changed"
    if isinstance(event, MarketDataDisconnected):
        return "market_data_disconnected"
    if isinstance(event, MarketDataReconnected):
        return "market_data_reconnected"
    raise SQLiteJournalPayloadError("market-data event type is unsupported")


def _decimal_to_storage(value: Decimal) -> str:
    """Serialize an already validated finite Decimal as exact non-float text."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise SQLiteJournalPayloadError("decimal value is invalid")
    return format(value, "f")


def _decimal_from_storage(value: object, context: str) -> Decimal:
    """Read exact finite Decimal text while rejecting SQLite numeric coercions."""
    if not isinstance(value, str):
        raise SQLiteJournalPayloadError(f"stored {context} is invalid")
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise SQLiteJournalPayloadError(f"stored {context} is invalid") from error
    if not decimal_value.is_finite():
        raise SQLiteJournalPayloadError(f"stored {context} is invalid")
    return decimal_value


def _timestamp_to_storage(value: datetime) -> str:
    """Normalize an aware instant to microsecond-precise UTC ISO text."""
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise SQLiteJournalPayloadError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _nullable_timestamp_to_storage(value: datetime | None) -> str | None:
    """Serialize an optional timestamp without changing SQL NULL semantics."""
    return None if value is None else _timestamp_to_storage(value)


def _timestamp_from_storage(value: object) -> datetime:
    """Read timezone-aware timestamp text and normalize it to UTC."""
    if not isinstance(value, str):
        raise SQLiteJournalPayloadError("stored timestamp is invalid")
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError as error:
        raise SQLiteJournalPayloadError("stored timestamp is invalid") from error
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise SQLiteJournalPayloadError("stored timestamp is invalid")
    return timestamp.astimezone(UTC)


def _nullable_timestamp_from_storage(value: object) -> datetime | None:
    """Read optional timestamp text while preserving SQL NULL."""
    return None if value is None else _timestamp_from_storage(value)


def _status_to_storage(status: LocalBookStatus) -> str:
    """Serialize local book status by its stable enum member name."""
    if not isinstance(status, LocalBookStatus):
        raise SQLiteJournalPayloadError("local book status is invalid")
    return status.name


def _status_from_storage(value: object) -> LocalBookStatus:
    """Read a stable local book status name without accepting unknown values."""
    if not isinstance(value, str):
        raise SQLiteJournalPayloadError("stored local book status is invalid")
    try:
        return LocalBookStatus[value]
    except KeyError as error:
        raise SQLiteJournalPayloadError(
            "stored local book status is invalid"
        ) from error


def _side_from_storage(value: object) -> Side:
    """Read an explicit Side string without accepting enum ordinals."""
    if not isinstance(value, str):
        raise SQLiteJournalPayloadError("stored side is invalid")
    try:
        return Side(value)
    except ValueError as error:
        raise SQLiteJournalPayloadError("stored side is invalid") from error


def _text(value: object, context: str) -> str:
    """Return required stored text while rejecting empty or non-text values."""
    if not isinstance(value, str) or not value:
        raise SQLiteJournalPayloadError(f"stored {context} is invalid")
    return value


def _integer(value: object, context: str) -> int:
    """Return a non-boolean SQLite integer with a focused corruption error."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise SQLiteJournalPayloadError(f"stored {context} is invalid")
    return value


def _positive_integer(value: object, context: str) -> int:
    """Return a strictly positive SQLite integer."""
    integer = _integer(value, context)
    if integer < 1:
        raise SQLiteJournalPayloadError(f"stored {context} is invalid")
    return integer


def _require_only_subtype(counts: tuple[int, ...], expected_index: int) -> None:
    """Require exactly one matching subtype marker and no conflicting payload rows."""
    if any(
        count != (1 if index == expected_index else 0)
        for index, count in enumerate(counts)
    ):
        raise SQLiteJournalPayloadError(
            "stored event payload is missing or conflicting"
        )
