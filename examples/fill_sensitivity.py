"""Compare fixed fill assumptions on an eligible existing SQLite session."""

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

import aiosqlite

from horus_engine.application import MarketDataSessionId, ShadowQuoteConfig
from horus_engine.application.fill_sensitivity import compare_fill_scenarios
from horus_engine.domain import Quantity
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


async def compare(database: Path, session_id: str) -> dict[str, object]:
    """Use read-only SQLite and the demo's fixed midpoint/one-tick/size-two quotes."""
    async with aiosqlite.connect(
        database.resolve().as_uri() + "?mode=ro", uri=True
    ) as connection:
        return await compare_fill_scenarios(
            SQLiteMarketDataJournal(connection),
            MarketDataSessionId(session_id),
            ShadowQuoteConfig(1, Quantity("2")),
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Emit the comparison; rejected evidence exits nonzero, not empty success."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--session-id", required=True)
    options = parser.parse_args(argv)
    report = asyncio.run(compare(options.database, options.session_id))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["scenarios"] is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
