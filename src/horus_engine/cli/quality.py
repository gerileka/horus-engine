"""Read-only, fail-closed quality gate for research analysis workflows."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import aiosqlite

from horus_engine.application.market_data_journal import MarketDataSessionId
from horus_engine.application.session_quality import (
    assess_session_quality,
    unavailable_quality_report,
)
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal

from .shadow import _ShadowDependencies


def _open_read_only(path: Path) -> aiosqlite.Connection:
    return aiosqlite.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


_DEFAULT_DEPENDENCIES = _ShadowDependencies(_open_read_only, SQLiteMarketDataJournal)


def add_quality_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register an evidence report with nonzero exit for ineligible sessions."""
    parser = subparsers.add_parser(
        "quality", help="assess recorded-session eligibility"
    )
    parser.add_argument("--database", required=True, help="existing SQLite journal")
    parser.add_argument("--session-id", required=True, help="recorded session ID")


async def quality(
    database: Path,
    session_id: MarketDataSessionId,
    dependencies: _ShadowDependencies = _DEFAULT_DEPENDENCIES,
) -> dict[str, object]:
    """Open a journal without creating, initializing, repairing or finishing it."""
    try:
        async with dependencies.sqlite_connection_factory(database) as connection:
            return await assess_session_quality(
                dependencies.journal_factory(connection), session_id
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        return unavailable_quality_report(session_id, "journal_open_failed")
