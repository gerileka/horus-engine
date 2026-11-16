"""Real SQLite read-only benchmark, immutable journal and corrupted-history gate."""

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any, cast

import aiosqlite

from examples.strategy_benchmark import SyntheticJournal, compare, main
from horus_engine.application import LocalOrderBookState, MarketDataSessionUpdate
from horus_engine.application.market_data_journal import MarketDataJournalSession
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


def test_read_only_benchmark_and_rejection(tmp_path: Path, capsys: Any) -> None:
    database = tmp_path / "journal with spaces.db"
    synthetic = SyntheticJournal()
    ids = tuple(synthetic.evidence)

    async def write() -> None:
        async with aiosqlite.connect(database) as connection:
            journal = SQLiteMarketDataJournal(connection)
            await journal.initialize()
            for evidence in synthetic.evidence.values():
                session = evidence.session
                assert session is not None and session.finished_at is not None
                await journal.start_session(
                    MarketDataJournalSession(
                        session.session_id,
                        session.market_id,
                        session.token_id,
                        session.initial_tick_size,
                        session.started_at,
                    )
                )
                state = LocalOrderBookState(
                    session.market_id, session.token_id, session.initial_tick_size
                )
                for update in evidence.updates:
                    before = state.view.book
                    view = state.apply(update.event)
                    await journal.append_update(
                        session.session_id,
                        update.sequence_number,
                        MarketDataSessionUpdate(
                            update.event, view, before != view.book
                        ),
                    )
                await journal.finish_session(
                    session.session_id, session.finished_at, state.view
                )

    asyncio.run(write())
    before = database.read_bytes()
    report = asyncio.run(compare(database, ids))
    assert report["ready"] is True
    assert report == asyncio.run(compare(database, ids))
    rows = cast(list[dict[str, Any]], report["sessions"])
    assert (
        rows[0]["variants"][0]["cost_sensitivity"][1][
            "marked_value_after_cost_not_settlement"
        ]
        == "0.02849"
    )
    args = ["--database", str(database)]
    for sid in ids:
        args += ["--session-id", str(sid)]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["ready"] is True
    assert database.read_bytes() == before
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE market_data_events SET sequence_number=99 "
            "WHERE session_id=? AND sequence_number=2",
            (str(ids[-1]),),
        )
    corrupt = database.read_bytes()
    rejected = asyncio.run(compare(database, ids))
    assert rejected["ready"] is False
    rows = cast(list[dict[str, Any]], rejected["sessions"])
    assert rows[-1]["split"] == "held_out" and rows[-1]["variants"] is None
    assert main(args) == 1
    assert database.read_bytes() == corrupt
