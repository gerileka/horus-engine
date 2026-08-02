"""Focused integration coverage for replay through the SQLite journal."""

import asyncio
from datetime import UTC, datetime, timedelta

import aiosqlite

from horus_engine.application import (
    BookSnapshotReceived,
    LocalBookStatus,
    LocalOrderBookState,
    MarketDataEvent,
    MarketDataSessionId,
    MarketDataSessionReplayer,
    MarketDataSessionUpdate,
    MarketId,
    PriceLevelChanged,
    TokenId,
    TradeObserved,
)
from horus_engine.application.market_data_journal import MarketDataJournalSession
from horus_engine.domain import (
    NonNegativeQuantity,
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


def test_sqlite_journal_replays_authoritative_snapshots_and_updates() -> None:
    """Replay actual persisted normalized rows without a network dependency."""

    async def scenario() -> None:
        session_id = MarketDataSessionId("sqlite-replay")
        market_id = MarketId("market-1")
        token_id = TokenId("token-yes")
        tick_size = TickSize("0.01")
        started_at = datetime(2026, 8, 2, 12, tzinfo=UTC)

        def at(minutes: int) -> datetime:
            return started_at + timedelta(minutes=minutes)

        def book(
            bids: tuple[tuple[str, str], ...], asks: tuple[tuple[str, str], ...]
        ) -> OrderBook:
            return OrderBook(
                [OrderBookLevel(Price(price), Quantity(size)) for price, size in bids],
                [OrderBookLevel(Price(price), Quantity(size)) for price, size in asks],
            )

        events: tuple[MarketDataEvent, ...] = (
            BookSnapshotReceived(
                market_id,
                token_id,
                book((("0.40", "2"),), (("0.60", "3"),)),
                at(1),
            ),
            PriceLevelChanged(
                market_id,
                token_id,
                Side.BUY,
                Price("0.40"),
                NonNegativeQuantity("4.25"),
                at(2),
            ),
            PriceLevelChanged(
                market_id,
                token_id,
                Side.SELL,
                Price("0.55"),
                NonNegativeQuantity("1.5"),
                at(3),
            ),
            TradeObserved(
                market_id, token_id, Side.SELL, Price("0.45"), Quantity("1"), at(4)
            ),
            PriceLevelChanged(
                market_id,
                token_id,
                Side.BUY,
                Price("0.30"),
                NonNegativeQuantity("0"),
                at(5),
            ),
            BookSnapshotReceived(
                market_id,
                token_id,
                book((("0.45", "7"),), (("0.65", "8"),)),
                at(6),
            ),
            PriceLevelChanged(
                market_id,
                token_id,
                Side.BUY,
                Price("0.46"),
                NonNegativeQuantity("2.75"),
                at(7),
            ),
        )
        connection = await aiosqlite.connect(":memory:")
        try:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            await journal.start_session(
                MarketDataJournalSession(
                    session_id, market_id, token_id, tick_size, started_at
                )
            )
            state = LocalOrderBookState(market_id, token_id, tick_size)
            for sequence_number, event in enumerate(events, start=1):
                previous_book = state.view.book
                view = state.apply(event)
                await journal.append_update(
                    session_id,
                    sequence_number,
                    MarketDataSessionUpdate(event, view, previous_book != view.book),
                )
            await journal.finish_session(session_id, at(8), state.view)
            result = await MarketDataSessionReplayer(journal).replay(session_id)
        finally:
            await connection.close()

        assert [step.sequence_number for step in result.steps] == list(range(1, 8))
        assert [step.book_changed for step in result.steps] == [
            True,
            True,
            True,
            False,
            False,
            True,
            True,
        ]
        assert result.final_view == result.last_event_view
        assert result.final_view.book == book(
            (("0.46", "2.75"), ("0.45", "7")), (("0.65", "8"),)
        )
        assert result.final_view.status is LocalBookStatus.SYNCHRONIZED

    asyncio.run(scenario())
