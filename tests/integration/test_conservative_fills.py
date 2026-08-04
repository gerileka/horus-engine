"""End-to-end conservative-fill simulation over a real SQLite market-data journal."""

import asyncio
from datetime import UTC, datetime, timedelta

import aiosqlite

from horus_engine.application import (
    BookSnapshotReceived,
    ConservativeFillSimulator,
    ConservativeFillTrigger,
    LocalOrderBookState,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataSessionId,
    MarketDataSessionReplayer,
    MarketDataSessionUpdate,
    MarketId,
    MidpointFairValueModel,
    ShadowQuoteAnalyzer,
    ShadowQuoteConfig,
    TokenId,
    TradeObserved,
)
from horus_engine.application.market_data_journal import MarketDataJournalSession
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


def test_sqlite_replay_shadow_and_conservative_fills_are_no_lookahead() -> None:
    """Use actual journal/replay composition without a network or journal mutation."""

    async def scenario() -> None:
        session_id = MarketDataSessionId("conservative-fill-integration")
        market_id = MarketId("market")
        token_id = TokenId("token")
        tick_size = TickSize("0.01")
        started_at = datetime(2026, 8, 2, tzinfo=UTC)

        def at(sequence: int) -> datetime:
            return started_at + timedelta(seconds=sequence)

        book = OrderBook(
            (OrderBookLevel(Price("0.48"), Quantity("2")),),
            (OrderBookLevel(Price("0.52"), Quantity("3")),),
        )
        events: tuple[MarketDataEvent, ...] = (
            BookSnapshotReceived(market_id, token_id, book, at(1)),
            TradeObserved(
                market_id, token_id, Side.SELL, Price("0.49"), Quantity("2"), at(2)
            ),
            TradeObserved(
                market_id, token_id, Side.BUY, Price("0.52"), Quantity("1"), at(3)
            ),
            MarketDataDisconnected("transport ended", at(4)),
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
            for sequence, event in enumerate(events, start=1):
                previous_book = state.view.book
                view = state.apply(event)
                await journal.append_update(
                    session_id,
                    sequence,
                    MarketDataSessionUpdate(event, view, previous_book != view.book),
                )
            await journal.finish_session(session_id, at(5), state.view)
            replay = await MarketDataSessionReplayer(journal).replay(session_id)
            analysis = ShadowQuoteAnalyzer(
                MidpointFairValueModel(), ShadowQuoteConfig(1, Quantity("2"))
            ).analyze(replay)
            fills = ConservativeFillSimulator().simulate(analysis)
            persisted = await journal.get_session(session_id)
        finally:
            await connection.close()

        assert [decision.action.value for decision in analysis.decisions] == [
            "place",
            "hold",
            "hold",
            "cancel",
        ]
        assert fills.steps[0].fills == ()
        assert [fill.trigger for fill in fills.fills] == [
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            ConservativeFillTrigger.TRADE_THROUGH,
        ]
        assert [fill.quote_price for fill in fills.fills] == [
            Price("0.49"),
            Price("0.51"),
        ]
        assert fills.buy_filled_quantity == Quantity("2").value
        assert fills.sell_filled_quantity == Quantity("2").value
        assert persisted is not None
        assert persisted.last_sequence_number == 4

    asyncio.run(scenario())
