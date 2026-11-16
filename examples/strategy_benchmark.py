"""Read-only fixed benchmark, or a clearly synthetic three-session oracle."""

import argparse
import asyncio
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite

from horus_engine.application import (
    BookSnapshotReceived,
    LocalOrderBookState,
    MarketDataEvent,
    MarketDataSessionId,
    MarketId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
    TokenId,
    TradeObserved,
)
from horus_engine.application.fill_sensitivity import _Evidence
from horus_engine.application.strategy_benchmark import run_benchmark
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


def synthetic_evidence(number: int) -> _Evidence:
    """Independent hand-oracle histories, NOT empirical train/held-out data."""
    sid = MarketDataSessionId(f"synthetic-benchmark-{number}")
    market = MarketId("synthetic-market")
    token, tick = TokenId("synthetic-token"), TickSize("0.01")
    start = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=number)
    state = LocalOrderBookState(market, token, tick)
    book = OrderBook(
        (OrderBookLevel(Price("0.48"), Quantity("2")),),
        (OrderBookLevel(Price("0.52"), Quantity("2")),),
    )
    # Buy 1 at .49; sell 2 at .51; terminal midpoint .50. Cash .53,
    # short 1 valued at -.50 => marked value .03, not .53 profit.
    events: tuple[MarketDataEvent, ...] = (
        BookSnapshotReceived(market, token, book, start),
        TradeObserved(
            market,
            token,
            Side.SELL,
            Price("0.49"),
            Quantity("1"),
            start + timedelta(seconds=1),
        ),
        TradeObserved(
            market,
            token,
            Side.BUY,
            Price("0.53"),
            Quantity("1"),
            start + timedelta(seconds=2),
        ),
        BookSnapshotReceived(market, token, book, start + timedelta(seconds=6)),
        BookSnapshotReceived(market, token, book, start + timedelta(seconds=7)),
    )
    updates = []
    for seq, event in enumerate(events, 1):
        before = state.view.book
        view = state.apply(event)
        updates.append(
            PersistedMarketDataUpdate(
                sid,
                seq,
                event,
                before != view.book,
                view.status,
                view.tick_size,
                view.last_observed_at,
                view.status_reason,
            )
        )
    view = state.view
    session = PersistedMarketDataSession(
        sid,
        market,
        token,
        tick,
        start,
        start + timedelta(seconds=7),
        view.status,
        tick,
        view.last_observed_at,
        view.status_reason,
        len(updates),
    )
    return _Evidence(session, tuple(updates))


class SyntheticJournal:
    """In-memory synthetic-only input for documented reproducibility."""

    def __init__(self) -> None:
        self.evidence = {
            MarketDataSessionId(f"synthetic-benchmark-{i}"): synthetic_evidence(i)
            for i in range(3)
        }

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession | None:
        """Return a synthetic session header."""
        return self.evidence[session_id].session

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        """Return an immutable synthetic history."""
        return self.evidence[session_id].updates


async def compare(
    database: Path, ids: tuple[MarketDataSessionId, ...]
) -> dict[str, object]:
    """Open existing journals read-only; never initialize or capture data."""
    async with aiosqlite.connect(
        database.resolve().as_uri() + "?mode=ro", uri=True
    ) as connection:
        return await run_benchmark(SQLiteMarketDataJournal(connection), ids)


def main(argv: Sequence[str] | None = None) -> int:
    """Emit canonical JSON; insufficient or rejected inputs exit nonzero."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--database", type=Path)
    parser.add_argument("--session-id", action="append", default=[])
    options = parser.parse_args(argv)
    if options.synthetic:
        if options.database or options.session_id:
            parser.error("synthetic cannot be mixed with database/session IDs")
        journal = SyntheticJournal()
        report = asyncio.run(run_benchmark(journal, tuple(journal.evidence)))
        report["data_kind"] = "synthetic_only_no_empirical_edge_evidence"
    else:
        if options.database is None or not options.session_id:
            parser.error("provide --database and repeated --session-id, or --synthetic")
        report = asyncio.run(
            compare(
                options.database,
                tuple(MarketDataSessionId(s) for s in options.session_id),
            )
        )
        report["data_kind"] = "user_supplied_journal_provenance_not_verified"
    print(json.dumps(report, sort_keys=True))
    return 0 if report["ready"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
