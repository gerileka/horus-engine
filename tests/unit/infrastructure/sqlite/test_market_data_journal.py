"""In-memory tests for the asynchronous SQLite market-data journal."""

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import aiosqlite
import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    LocalBookStatus,
    LocalOrderBookView,
    MarketDataDisconnected,
    MarketDataJournalGateway,
    MarketDataJournalSession,
    MarketDataReconnected,
    MarketDataSessionId,
    MarketDataSessionUpdate,
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
from horus_engine.infrastructure.sqlite import (
    SQLiteJournalPayloadError,
    SQLiteMarketDataJournal,
    SQLiteMarketDataJournalError,
    SQLiteSchemaVersionError,
    SQLiteSequenceError,
    SQLiteSessionAlreadyExists,
    SQLiteSessionAlreadyFinished,
    SQLiteSessionNotFound,
)
from horus_engine.infrastructure.sqlite import market_data_journal as journal_module

_STARTED = datetime(2026, 7, 22, 12, 0, 0, 123456, tzinfo=UTC)
_MARKET_ID = MarketId("market-1")
_TOKEN_ID = TokenId("token-1")


def _as_gateway(journal: MarketDataJournalGateway) -> MarketDataJournalGateway:
    """Require structural conformance without a runtime protocol check."""
    return journal


def _session(
    number: int = 1, started_at: datetime = _STARTED
) -> MarketDataJournalSession:
    """Build one stable journal session for local persistence tests."""
    return MarketDataJournalSession(
        MarketDataSessionId(f"session-{number}"),
        _MARKET_ID,
        _TOKEN_ID,
        TickSize("0.0025"),
        started_at,
    )


def _view(
    *,
    status: LocalBookStatus = LocalBookStatus.SYNCHRONIZED,
    tick_size: TickSize = TickSize("0.0025"),
    observed_at: datetime | None = _STARTED,
    reason: str | None = None,
    market_id: MarketId = _MARKET_ID,
    token_id: TokenId = _TOKEN_ID,
) -> LocalOrderBookView:
    """Build post-event metadata without implying a reconstructed persisted book."""
    return LocalOrderBookView(
        market_id, token_id, tick_size, status, OrderBook(), observed_at, reason
    )


def _update(
    event: object,
    *,
    view: LocalOrderBookView | None = None,
    book_changed: bool = False,
) -> MarketDataSessionUpdate:
    """Construct one session update with an explicitly controlled post view."""
    assert isinstance(
        event,
        (
            BookSnapshotReceived,
            PriceLevelChanged,
            TradeObserved,
            TickSizeChanged,
            MarketDataDisconnected,
            MarketDataReconnected,
        ),
    )
    return MarketDataSessionUpdate(
        event, _view() if view is None else view, book_changed
    )


def test_initialize_is_versioned_idempotent_and_preserves_connection_ownership() -> (
    None
):
    """Initialize version one once, enable foreign keys, and leave connection usable."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            assert isinstance(_as_gateway(journal), SQLiteMarketDataJournal)
            await journal.initialize()
            version = await (await connection.execute("PRAGMA user_version")).fetchone()
            foreign_keys = await (
                await connection.execute("PRAGMA foreign_keys")
            ).fetchone()
            assert version == (1,)
            assert foreign_keys == (1,)
            await journal.initialize()
            await journal.start_session(_session())
            assert await (await connection.execute("SELECT 1")).fetchone() == (1,)
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_reading_corruption_and_serialization_helpers_fail_explicitly() -> None:
    """Reject malformed rows and never coerce bad storage values into domain data."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()

            async def corrupted_session(
                number: int,
                event: object,
                statement: str,
                parameters: tuple[object, ...],
            ) -> None:
                session = _session(number)
                await journal.start_session(session)
                await journal.append_update(
                    session.session_id,
                    1,
                    _update(
                        event, book_changed=isinstance(event, BookSnapshotReceived)
                    ),
                )
                event_id_row = await (
                    await connection.execute(
                        """
                        SELECT event_id FROM market_data_events
                        WHERE session_id = ?
                        """,
                        (session.session_id.value,),
                    )
                ).fetchone()
                assert event_id_row is not None
                event_id = event_id_row[0]
                await connection.execute(statement, (*parameters, event_id))
                with pytest.raises(SQLiteJournalPayloadError):
                    await journal.list_updates(session.session_id)

            reconnect = MarketDataReconnected(_STARTED)
            price_change = PriceLevelChanged(
                _MARKET_ID,
                _TOKEN_ID,
                Side.BUY,
                Price("0.1"),
                NonNegativeQuantity("1"),
                _STARTED,
            )
            snapshot = BookSnapshotReceived(
                _MARKET_ID,
                _TOKEN_ID,
                OrderBook((OrderBookLevel(Price("0.1"), Quantity("1")),), ()),
                _STARTED,
            )
            await corrupted_session(
                10,
                reconnect,
                "UPDATE market_data_events SET event_type = ? WHERE event_id = ?",
                ("unknown",),
            )
            await corrupted_session(
                11,
                price_change,
                "DELETE FROM price_level_changes WHERE event_id = ?",
                (),
            )
            await corrupted_session(
                12,
                price_change,
                """
                INSERT INTO trades (event_id, aggressor_side, price, quantity)
                VALUES (?, 'BUY', '0.1', '1')
                """,
                (),
            )
            await corrupted_session(
                13,
                reconnect,
                "UPDATE market_data_events SET post_tick_size = ? WHERE event_id = ?",
                ("not-a-decimal",),
            )
            await corrupted_session(
                14,
                reconnect,
                "UPDATE market_data_events SET post_status = ? WHERE event_id = ?",
                ("unknown",),
            )
            await corrupted_session(
                15,
                reconnect,
                "UPDATE market_data_events SET observed_at = ? WHERE event_id = ?",
                ("not-a-timestamp",),
            )
            await corrupted_session(
                16,
                snapshot,
                "UPDATE book_snapshot_levels SET quantity = ? WHERE event_id = ?",
                ("not-a-quantity",),
            )
            await corrupted_session(
                17,
                reconnect,
                "UPDATE connection_events SET reason = ? WHERE event_id = ?",
                ("unexpected",),
            )
            await connection.execute("PRAGMA ignore_check_constraints = ON")
            await corrupted_session(
                18,
                reconnect,
                "UPDATE market_data_events SET book_changed = ? WHERE event_id = ?",
                (2,),
            )
            await corrupted_session(
                19,
                snapshot,
                "UPDATE book_snapshot_levels SET level_position = ? WHERE event_id = ?",
                (2,),
            )
            await corrupted_session(
                20,
                snapshot,
                "UPDATE book_snapshot_levels SET level_position = ? WHERE event_id = ?",
                (-1,),
            )
            await connection.execute("PRAGMA ignore_check_constraints = OFF")
        finally:
            await connection.close()

    asyncio.run(scenario())
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._event_type(object())  # type: ignore[arg-type]
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._decimal_to_storage(Decimal("NaN"))
    for value in (1, "not-a-decimal", "NaN"):
        with pytest.raises(SQLiteJournalPayloadError):
            journal_module._decimal_from_storage(value, "value")
    for value in ("not-a-timestamp", "2026-07-22T12:00:00", 1):
        with pytest.raises(SQLiteJournalPayloadError):
            journal_module._timestamp_from_storage(value)
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._timestamp_to_storage(datetime(2026, 7, 22, 12))
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._status_to_storage(object())  # type: ignore[arg-type]
    for value in (1, "unknown"):
        with pytest.raises(SQLiteJournalPayloadError):
            journal_module._status_from_storage(value)
    for value in (1, "unknown"):
        with pytest.raises(SQLiteJournalPayloadError):
            journal_module._side_from_storage(value)
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._text("", "text")
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._integer("one", "integer")
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._positive_integer(0, "integer")
    with pytest.raises(SQLiteJournalPayloadError):
        journal_module._require_only_subtype((0, 0, 0, 0, 0), 0)


def test_database_failures_and_impossible_storage_shapes_use_focused_errors() -> None:
    """Convert low-level SQLite failures and impossible corruption into safe errors."""

    async def scenario() -> None:
        with pytest.raises(ValueError):
            SQLiteMarketDataJournal(None)  # type: ignore[arg-type]

        connection = await aiosqlite.connect(":memory:")
        try:
            await connection.execute("CREATE TABLE market_data_sessions (value TEXT)")
            with pytest.raises(SQLiteMarketDataJournalError):
                await SQLiteMarketDataJournal(connection).initialize()
            assert await (
                await connection.execute("PRAGMA user_version")
            ).fetchone() == (0,)
        finally:
            await connection.close()

        async def initialized_connection() -> tuple[
            aiosqlite.Connection, SQLiteMarketDataJournal
        ]:
            current_connection = await aiosqlite.connect(":memory:")
            current_journal = SQLiteMarketDataJournal(current_connection)
            await current_journal.initialize()
            return current_connection, current_journal

        connection, journal = await initialized_connection()
        try:
            await connection.execute(
                """
                CREATE TRIGGER reject_session_insert
                BEFORE INSERT ON market_data_sessions
                BEGIN SELECT RAISE(ABORT, 'reject'); END
                """
            )
            with pytest.raises(SQLiteSessionAlreadyExists):
                await journal.start_session(_session())
        finally:
            await connection.close()

        connection, journal = await initialized_connection()
        try:
            await connection.execute("DROP TABLE market_data_sessions")
            with pytest.raises(SQLiteMarketDataJournalError):
                await journal.start_session(_session())
        finally:
            await connection.close()

        connection, journal = await initialized_connection()
        try:
            session = _session()
            await journal.start_session(session)
            await connection.execute("DROP TABLE market_data_events")
            with pytest.raises(SQLiteMarketDataJournalError):
                await journal.append_update(
                    session.session_id, 1, _update(MarketDataReconnected(_STARTED))
                )
            with pytest.raises(SQLiteJournalPayloadError):
                await journal._insert_event_payload(1, object())  # type: ignore[arg-type]
        finally:
            await connection.close()

        connection, journal = await initialized_connection()
        try:
            session = _session()
            await journal.start_session(session)
            await connection.execute(
                """
                CREATE TRIGGER reject_session_finish
                BEFORE UPDATE ON market_data_sessions
                BEGIN SELECT RAISE(ABORT, 'reject'); END
                """
            )
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.finish_session(session.session_id, _STARTED, _view())
        finally:
            await connection.close()

        connection, journal = await initialized_connection()
        try:
            session = _session()
            await journal.start_session(session)
            await connection.execute("DROP TABLE market_data_sessions")
            with pytest.raises(SQLiteMarketDataJournalError):
                await journal.finish_session(session.session_id, _STARTED, _view())
        finally:
            await connection.close()

        connection = await aiosqlite.connect(":memory:")
        try:
            await connection.executescript(
                """
                CREATE TABLE market_data_events (event_id INTEGER, session_id TEXT);
                CREATE TABLE market_data_sessions (
                    session_id TEXT, market_id TEXT, token_id TEXT
                );
                CREATE TABLE book_snapshot_levels (
                    event_id INTEGER, side TEXT, level_position INTEGER,
                    price TEXT, quantity TEXT
                );
                INSERT INTO market_data_events VALUES (1, 'session');
                INSERT INTO market_data_sessions VALUES ('session', 'market', 'token');
                INSERT INTO book_snapshot_levels VALUES (1, 'BUY', 0, '0.1', '1');
                INSERT INTO book_snapshot_levels VALUES (1, 'BUY', 1, '0.1', '2');
                """
            )
            corrupt_journal = SQLiteMarketDataJournal(connection)
            with pytest.raises(SQLiteJournalPayloadError):
                await corrupt_journal._snapshot_event(1, _STARTED)
            with pytest.raises(SQLiteJournalPayloadError):
                await corrupt_journal._event_market_id(2)
            with pytest.raises(SQLiteJournalPayloadError):
                await corrupt_journal._event_token_id(2)
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_initialize_rejects_future_version_without_recreating_schema() -> None:
    """Refuse unknown schema versions rather than silently changing existing data."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            await connection.execute("PRAGMA user_version = 2")
            with pytest.raises(SQLiteSchemaVersionError):
                await SQLiteMarketDataJournal(connection).initialize()
            assert await (
                await connection.execute("PRAGMA user_version")
            ).fetchone() == (2,)
            assert (
                await (
                    await connection.execute("SELECT name FROM sqlite_master")
                ).fetchall()
                == []
            )
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_start_read_duplicate_and_finish_session_round_trip() -> None:
    """Persist exact Decimal text and complete terminal session state metadata."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            started_at = datetime(
                2026, 7, 22, 14, 0, 0, 123000, tzinfo=timezone(timedelta(hours=2))
            )
            session = _session(started_at=started_at)
            await journal.start_session(session)
            persisted = await journal.get_session(session.session_id)
            assert persisted is not None
            assert persisted.started_at == _STARTED.replace(microsecond=123000)
            assert persisted.initial_tick_size.value == Decimal("0.0025")
            assert persisted.finished_at is None
            assert persisted.last_sequence_number == 0
            with pytest.raises(SQLiteSessionAlreadyExists):
                await journal.start_session(session)
            final_view = _view(
                status=LocalBookStatus.STALE,
                tick_size=TickSize("0.01"),
                observed_at=_STARTED + timedelta(seconds=1),
                reason="transport ended",
            )
            await journal.finish_session(
                session.session_id, _STARTED + timedelta(seconds=2), final_view
            )
            finished = await journal.get_session(session.session_id)
            assert finished is not None
            assert finished.final_status is LocalBookStatus.STALE
            assert finished.final_tick_size == TickSize("0.01")
            assert finished.final_last_observed_at == _STARTED + timedelta(seconds=1)
            assert finished.final_reason == "transport ended"
            with pytest.raises(SQLiteSessionAlreadyFinished):
                await journal.finish_session(
                    session.session_id, _STARTED + timedelta(seconds=3), final_view
                )
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_finish_rejects_unknown_early_and_identity_mismatches() -> None:
    """Protect session lifecycle metadata from impossible finish operations."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            with pytest.raises(SQLiteSessionNotFound):
                await journal.finish_session(
                    MarketDataSessionId("missing"), _STARTED, _view()
                )
            session = _session()
            await journal.start_session(session)
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.finish_session(
                    session.session_id, _STARTED - timedelta(microseconds=1), _view()
                )
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.finish_session(
                    session.session_id,
                    _STARTED,
                    _view(market_id=MarketId("other-market")),
                )
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.finish_session(
                    session.session_id,
                    _STARTED,
                    _view(token_id=TokenId("other-token")),
                )
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_all_event_types_round_trip_with_post_event_metadata() -> None:
    """Persist every normalized event subtype without raw venue payloads or floats."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            session = _session()
            await journal.start_session(session)
            snapshot = BookSnapshotReceived(
                _MARKET_ID,
                _TOKEN_ID,
                OrderBook(
                    (OrderBookLevel(Price("0.1"), Quantity("100000000000.25")),),
                    (OrderBookLevel(Price("0.2"), Quantity("0.0025")),),
                ),
                _STARTED,
            )
            events = (
                (snapshot, _view(observed_at=_STARTED), True),
                (
                    PriceLevelChanged(
                        _MARKET_ID,
                        _TOKEN_ID,
                        Side.BUY,
                        Price("0.1"),
                        NonNegativeQuantity("3"),
                        _STARTED + timedelta(microseconds=1),
                    ),
                    _view(observed_at=_STARTED + timedelta(microseconds=1)),
                    True,
                ),
                (
                    PriceLevelChanged(
                        _MARKET_ID,
                        _TOKEN_ID,
                        Side.SELL,
                        Price("0.3"),
                        NonNegativeQuantity("4"),
                        _STARTED + timedelta(microseconds=2),
                    ),
                    _view(observed_at=_STARTED + timedelta(microseconds=2)),
                    True,
                ),
                (
                    PriceLevelChanged(
                        _MARKET_ID,
                        _TOKEN_ID,
                        Side.BUY,
                        Price("0.1"),
                        NonNegativeQuantity("0"),
                        _STARTED + timedelta(microseconds=3),
                    ),
                    _view(observed_at=_STARTED + timedelta(microseconds=3)),
                    True,
                ),
                (
                    TradeObserved(
                        _MARKET_ID,
                        _TOKEN_ID,
                        Side.BUY,
                        Price("0.1"),
                        Quantity("1"),
                        _STARTED + timedelta(microseconds=4),
                    ),
                    _view(observed_at=_STARTED + timedelta(microseconds=4)),
                    False,
                ),
                (
                    TradeObserved(
                        _MARKET_ID,
                        _TOKEN_ID,
                        Side.SELL,
                        Price("0.0025"),
                        Quantity("1.125"),
                        _STARTED + timedelta(microseconds=5),
                    ),
                    _view(observed_at=_STARTED + timedelta(microseconds=5)),
                    False,
                ),
                (
                    TickSizeChanged(
                        _MARKET_ID,
                        _TOKEN_ID,
                        TickSize("0.0025"),
                        TickSize("0.01"),
                        _STARTED + timedelta(microseconds=6),
                    ),
                    _view(
                        status=LocalBookStatus.STALE,
                        tick_size=TickSize("0.01"),
                        observed_at=_STARTED + timedelta(microseconds=6),
                        reason="tick-size change requires snapshot",
                    ),
                    False,
                ),
                (
                    MarketDataDisconnected(
                        "transport paused", _STARTED + timedelta(microseconds=7)
                    ),
                    _view(
                        status=LocalBookStatus.STALE,
                        tick_size=TickSize("0.01"),
                        observed_at=_STARTED + timedelta(microseconds=7),
                        reason="market-data disconnection requires snapshot",
                    ),
                    False,
                ),
                (
                    MarketDataReconnected(_STARTED + timedelta(microseconds=8)),
                    _view(
                        status=LocalBookStatus.STALE,
                        tick_size=TickSize("0.01"),
                        observed_at=_STARTED + timedelta(microseconds=8),
                        reason="market-data reconnection requires snapshot",
                    ),
                    False,
                ),
                (
                    MarketDataDisconnected(None, _STARTED + timedelta(microseconds=9)),
                    _view(
                        status=LocalBookStatus.STALE,
                        tick_size=TickSize("0.01"),
                        observed_at=None,
                        reason="no timestamp retained",
                    ),
                    False,
                ),
            )
            for sequence, (event, view, changed) in enumerate(events, start=1):
                await journal.append_update(
                    session.session_id,
                    sequence,
                    _update(event, view=view, book_changed=changed),
                )
            updates = await journal.list_updates(session.session_id)
            assert tuple(update.event for update in updates) == tuple(
                item[0] for item in events
            )
            assert tuple(update.sequence_number for update in updates) == tuple(
                range(1, 11)
            )
            assert updates[0].book_changed
            assert updates[-1].post_last_observed_at is None
            assert updates[6].post_tick_size == TickSize("0.01")
            stored_type = await (
                await connection.execute(
                    "SELECT typeof(price), typeof(quantity) FROM trades"
                )
            ).fetchone()
            assert stored_type == ("text", "text")
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_snapshot_variants_and_domain_ordering_round_trip() -> None:
    """Retain empty, one-sided, locked, crossed, and unordered observed snapshots."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            books = (
                OrderBook(),
                OrderBook((OrderBookLevel(Price("0.4"), Quantity("1")),), ()),
                OrderBook(
                    (OrderBookLevel(Price("0.5"), Quantity("1")),),
                    (OrderBookLevel(Price("0.5"), Quantity("2")),),
                ),
                OrderBook(
                    (OrderBookLevel(Price("0.7"), Quantity("1")),),
                    (OrderBookLevel(Price("0.6"), Quantity("2")),),
                ),
                OrderBook(
                    (
                        OrderBookLevel(Price("0.2"), Quantity("1")),
                        OrderBookLevel(Price("0.9"), Quantity("2")),
                    ),
                    (
                        OrderBookLevel(Price("0.8"), Quantity("1")),
                        OrderBookLevel(Price("0.3"), Quantity("2")),
                    ),
                ),
            )
            for index, book in enumerate(books, start=1):
                session = _session(index)
                await journal.start_session(session)
                event = BookSnapshotReceived(_MARKET_ID, _TOKEN_ID, book, _STARTED)
                await journal.append_update(
                    session.session_id, 1, _update(event, book_changed=True)
                )
                update = (await journal.list_updates(session.session_id))[0]
                assert update.event == event
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_sequence_rules_identity_checks_and_atomic_rollback() -> None:
    """Keep per-session arrival order strict and failed writes fully rollback-safe."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            first, second = _session(1), _session(2)
            await journal.start_session(first)
            await journal.start_session(second)
            event = MarketDataReconnected(_STARTED)
            for sequence in (0, -1, 2):
                with pytest.raises(SQLiteSequenceError):
                    await journal.append_update(
                        first.session_id, sequence, _update(event)
                    )
            await journal.append_update(first.session_id, 1, _update(event))
            with pytest.raises(SQLiteSequenceError):
                await journal.append_update(first.session_id, 1, _update(event))
            await journal.append_update(second.session_id, 1, _update(event))
            wrong_event = TradeObserved(
                MarketId("other-market"),
                _TOKEN_ID,
                Side.BUY,
                Price("0.1"),
                Quantity("1"),
                _STARTED,
            )
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.append_update(first.session_id, 2, _update(wrong_event))
            mismatched_events = (
                BookSnapshotReceived(
                    MarketId("other-market"), _TOKEN_ID, OrderBook(), _STARTED
                ),
                PriceLevelChanged(
                    _MARKET_ID,
                    TokenId("other-token"),
                    Side.BUY,
                    Price("0.1"),
                    NonNegativeQuantity("1"),
                    _STARTED,
                ),
                TickSizeChanged(
                    _MARKET_ID,
                    TokenId("other-token"),
                    TickSize("0.0025"),
                    TickSize("0.01"),
                    _STARTED,
                ),
            )
            for mismatched_event in mismatched_events:
                with pytest.raises(SQLiteJournalPayloadError):
                    await journal.append_update(
                        first.session_id, 2, _update(mismatched_event)
                    )
            wrong_view = _view(token_id=TokenId("other-token"))
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.append_update(
                    first.session_id, 2, _update(event, view=wrong_view)
                )
            third = _session(3)
            await journal.start_session(third)
            await connection.execute(
                """
                CREATE TRIGGER abort_snapshot_level
                BEFORE INSERT ON book_snapshot_levels
                BEGIN SELECT RAISE(ABORT, 'intentional test abort'); END
                """
            )
            snapshot = BookSnapshotReceived(
                _MARKET_ID,
                _TOKEN_ID,
                OrderBook((OrderBookLevel(Price("0.1"), Quantity("1")),), ()),
                _STARTED,
            )
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.append_update(
                    third.session_id, 1, _update(snapshot, book_changed=True)
                )
            assert (
                await journal.get_session(third.session_id)
            ).last_sequence_number == 0  # type: ignore[union-attr]
            assert await journal.list_updates(third.session_id) == ()
            await connection.execute(
                """
                CREATE TRIGGER abort_connection_event
                BEFORE INSERT ON connection_events
                BEGIN SELECT RAISE(ABORT, 'intentional test abort'); END
                """
            )
            with pytest.raises(SQLiteJournalPayloadError):
                await journal.append_update(first.session_id, 2, _update(event))
            assert (
                await journal.get_session(first.session_id)
            ).last_sequence_number == 1  # type: ignore[union-attr]
            assert len(await journal.list_updates(first.session_id)) == 1
            assert await (
                await connection.execute(
                    """
                    SELECT COUNT(*) FROM connection_events AS payload
                    JOIN market_data_events AS events
                        ON events.event_id = payload.event_id
                    WHERE events.session_id = ?
                    """,
                    (first.session_id.value,),
                )
            ).fetchone() == (1,)
        finally:
            await connection.close()

    asyncio.run(scenario())


def test_finished_and_unknown_sessions_reject_append_and_reading_unknown_updates() -> (
    None
):
    """Apply lifecycle errors consistently without committing read-only operations."""

    async def scenario() -> None:
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            session = _session()
            await journal.start_session(session)
            await journal.finish_session(session.session_id, _STARTED, _view())
            with pytest.raises(SQLiteSessionAlreadyFinished):
                await journal.append_update(
                    session.session_id, 1, _update(MarketDataReconnected(_STARTED))
                )
            with pytest.raises(SQLiteSessionNotFound):
                await journal.append_update(
                    MarketDataSessionId("missing"),
                    1,
                    _update(MarketDataReconnected(_STARTED)),
                )
            assert await journal.get_session(MarketDataSessionId("missing")) is None
            with pytest.raises(SQLiteSessionNotFound):
                await journal.list_updates(MarketDataSessionId("missing"))
        finally:
            await connection.close()

    asyncio.run(scenario())
