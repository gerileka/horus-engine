"""End-to-end local integration test for the public collector composition root."""

import asyncio
import io
import json
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import aiosqlite
import httpx

from horus_engine.application import (
    BookSnapshotReceived,
    Market,
    MarketDataDisconnected,
    MarketDataSessionId,
    MarketDataStreamGateway,
    MarketId,
    MarketStatus,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
    PriceLevelChanged,
    TokenId,
    TradeObserved,
)
from horus_engine.cli.collect import _CollectionDependencies
from horus_engine.cli.main import main
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

_MARKET_ID = MarketId("condition-1")
_TOKEN_ID = TokenId("yes-token")
_OBSERVED_AT = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


class _Catalog:
    """Return one active normalized market without making a network request."""

    async def get_market(self, market_id: MarketId) -> Market | None:
        """Return metadata only for the expected condition ID."""
        if market_id != _MARKET_ID:
            return None
        return Market(
            _MARKET_ID,
            "Will the local collector finish?",
            _TOKEN_ID,
            TokenId("no-token"),
            TickSize("0.01"),
            Quantity("1"),
            MarketStatus.ACTIVE,
        )


class _Stream:
    """Emit a complete normalized lifecycle in the expected arrival order."""

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[object]:
        """Return the fixed stream with no transport or HTTP interaction."""
        assert market_id == _MARKET_ID
        assert token_ids == (_TOKEN_ID,)
        return self._events()

    async def _events(self) -> AsyncIterator[object]:
        yield BookSnapshotReceived(
            _MARKET_ID,
            _TOKEN_ID,
            OrderBook((OrderBookLevel(Price("0.50"), Quantity("2.25")),), ()),
            _OBSERVED_AT,
        )
        yield PriceLevelChanged(
            _MARKET_ID,
            _TOKEN_ID,
            Side.BUY,
            Price("0.50"),
            NonNegativeQuantity("3.75"),
            _OBSERVED_AT + timedelta(seconds=1),
        )
        yield TradeObserved(
            _MARKET_ID,
            _TOKEN_ID,
            Side.BUY,
            Price("0.51"),
            Quantity("1.50"),
            _OBSERVED_AT + timedelta(seconds=2),
        )
        yield MarketDataDisconnected(
            "test disconnect", _OBSERVED_AT + timedelta(seconds=3)
        )


class _Client(AbstractAsyncContextManager[httpx.AsyncClient]):
    """A local resource wrapper used solely to satisfy CLI ownership boundaries."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient()

    async def __aenter__(self) -> httpx.AsyncClient:
        return self._client

    async def __aexit__(self, *args: object) -> None:
        await self._client.aclose()


def test_collect_persists_normalized_events_with_exact_decimals(tmp_path: Path) -> None:
    """Use actual session, recorder, and SQLite persistence below the parser."""
    connection_path = tmp_path / "journal.db"
    connections: list[aiosqlite.Connection] = []
    timestamps = iter((_OBSERVED_AT, _OBSERVED_AT + timedelta(seconds=4)))

    def connect(path: Path) -> aiosqlite.Connection:
        connection = aiosqlite.connect(path)
        connections.append(connection)
        return connection

    dependencies = _CollectionDependencies(
        http_client_factory=_Client,
        sqlite_connection_factory=connect,
        catalog_factory=lambda _client: _Catalog(),
        stream_gateway_factory=lambda: cast(MarketDataStreamGateway, _Stream()),
        journal_factory=SQLiteMarketDataJournal,
        clock=lambda: next(timestamps),
        session_id_factory=lambda: MarketDataSessionId("unused"),
    )
    stdout = io.StringIO()
    stderr = io.StringIO()

    result = main(
        [
            "collect",
            "--market-id",
            _MARKET_ID.value,
            "--outcome",
            "yes",
            "--database",
            str(connection_path),
            "--session-id",
            "integration-session",
        ],
        dependencies=dependencies,
        stdout=stdout,
        stderr=stderr,
    )

    async def read_journal() -> tuple[
        PersistedMarketDataSession | None, tuple[PersistedMarketDataUpdate, ...]
    ]:
        async with aiosqlite.connect(connection_path) as connection:
            journal = SQLiteMarketDataJournal(connection)
            session = await journal.get_session(
                MarketDataSessionId("integration-session")
            )
            updates = await journal.list_updates(
                MarketDataSessionId("integration-session")
            )
            return session, updates

    session, updates = asyncio.run(read_journal())
    payload = json.loads(stdout.getvalue())
    assert result == 0
    assert connection_path.exists()
    assert session is not None and session.finished_at is not None
    assert [update.sequence_number for update in updates] == [1, 2, 3, 4]
    assert isinstance(updates[0].event, BookSnapshotReceived)
    assert isinstance(updates[1].event, PriceLevelChanged)
    assert updates[0].event.book.bids[0].quantity.value == Quantity("2.25").value
    assert updates[1].event.quantity.value == NonNegativeQuantity("3.75").value
    assert payload["updates_recorded"] == 4
    assert payload["final_status"] == "STALE"
    assert "integration-session" in stderr.getvalue()
    assert connections[0]._connection is None  # noqa: SLF001
