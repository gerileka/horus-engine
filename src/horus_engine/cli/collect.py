"""Composition root for one public Polymarket market-data collection run."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, TextIO

import aiosqlite
import httpx

from horus_engine.application import (
    Market,
    MarketDataJournalGateway,
    MarketDataRecordingResult,
    MarketDataSession,
    MarketDataSessionId,
    MarketDataSessionRecorder,
    MarketDataStreamGateway,
    MarketId,
    MarketStatus,
    PersistedMarketDataSession,
    TokenId,
)
from horus_engine.domain import Outcome
from horus_engine.infrastructure.polymarket import (
    PolymarketMarketCatalogGateway,
    PolymarketMarketDataStreamGateway,
)
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


class _MarketCatalog(Protocol):
    """Minimal catalog capability needed by this command."""

    async def get_market(self, market_id: MarketId) -> Market | None:
        """Return normalized metadata for one requested market."""
        ...


class CollectionOperationalError(RuntimeError):
    """An expected operational failure that should produce exit code one."""


@dataclass(frozen=True)
class CollectOptions:
    """Validated, CLI-specific input for a single collector lifecycle."""

    market_id: MarketId
    outcome: Outcome
    database_path: Path
    duration_seconds: int | None
    session_id: MarketDataSessionId

    def __post_init__(self) -> None:
        """Reject invalid local inputs before any external resource is opened."""
        if not isinstance(self.market_id, MarketId):
            raise ValueError("market_id must be a MarketId")
        if not isinstance(self.outcome, Outcome):
            raise ValueError("outcome must be an Outcome")
        if (
            not isinstance(self.database_path, Path)
            or not str(self.database_path).strip()
        ):
            raise ValueError("database path must be non-blank")
        if self.database_path.exists() and self.database_path.is_dir():
            raise ValueError("database path must not be an existing directory")
        if self.duration_seconds is not None and (
            isinstance(self.duration_seconds, bool)
            or not isinstance(self.duration_seconds, int)
            or self.duration_seconds <= 0
        ):
            raise ValueError("duration seconds must be a strictly positive integer")
        if not isinstance(self.session_id, MarketDataSessionId):
            raise ValueError("session_id must be a MarketDataSessionId")


@dataclass(frozen=True)
class CollectionSummary:
    """Serializable, public summary of one finalized collection lifecycle."""

    session_id: MarketDataSessionId
    market_id: MarketId
    token_id: TokenId
    outcome: Outcome
    database_path: Path
    termination: str
    updates_recorded: int
    final_status: str
    started_at: datetime
    finished_at: datetime
    final_reason: str | None

    def as_json(self) -> str:
        """Encode the command's stable stdout object without implementation reprs."""
        return json.dumps(
            {
                "session_id": self.session_id.value,
                "market_id": self.market_id.value,
                "token_id": self.token_id.value,
                "outcome": self.outcome.value,
                "database": str(self.database_path),
                "termination": self.termination,
                "updates_recorded": self.updates_recorded,
                "final_status": self.final_status,
                "started_at": self.started_at.isoformat(),
                "finished_at": self.finished_at.isoformat(),
                "final_reason": self.final_reason,
            }
        )


HttpClientFactory = Callable[[], AbstractAsyncContextManager[httpx.AsyncClient]]
SQLiteConnectionFactory = Callable[
    [Path], AbstractAsyncContextManager[aiosqlite.Connection]
]
CatalogFactory = Callable[[httpx.AsyncClient], _MarketCatalog]
StreamGatewayFactory = Callable[[], MarketDataStreamGateway]
JournalFactory = Callable[[aiosqlite.Connection], MarketDataJournalGateway]
SessionIdFactory = Callable[[], MarketDataSessionId]
Clock = Callable[[], datetime]


@dataclass(frozen=True)
class _CollectionDependencies:
    """Small CLI-only factory seam for deterministic composition tests."""

    http_client_factory: HttpClientFactory
    sqlite_connection_factory: SQLiteConnectionFactory
    catalog_factory: CatalogFactory
    stream_gateway_factory: StreamGatewayFactory
    journal_factory: JournalFactory
    clock: Clock
    session_id_factory: SessionIdFactory


def _new_session_id() -> MarketDataSessionId:
    """Generate a stable CLI-owned session identifier."""
    return MarketDataSessionId(str(uuid.uuid4()))


def _open_sqlite_connection(path: Path) -> aiosqlite.Connection:
    """Open the caller-owned SQLite connection used by one command run."""
    return aiosqlite.connect(path)


_DEFAULT_DEPENDENCIES = _CollectionDependencies(
    http_client_factory=httpx.AsyncClient,
    sqlite_connection_factory=_open_sqlite_connection,
    catalog_factory=PolymarketMarketCatalogGateway,
    stream_gateway_factory=PolymarketMarketDataStreamGateway,
    journal_factory=SQLiteMarketDataJournal,
    clock=lambda: datetime.now(UTC),
    session_id_factory=_new_session_id,
)


def add_collect_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the sole supported public collection command."""
    parser = subparsers.add_parser("collect", help="record one public market outcome")
    parser.add_argument("--market-id", required=True, help="Polymarket condition ID")
    parser.add_argument("--outcome", required=True, help="selected outcome: yes or no")
    parser.add_argument(
        "--database", default="data/horus.db", help="SQLite journal path"
    )
    parser.add_argument(
        "--duration-seconds", type=int, help="strictly positive wall-clock duration"
    )
    parser.add_argument("--session-id", help="optional stable recording session ID")


def options_from_namespace(
    namespace: argparse.Namespace,
    session_id_factory: SessionIdFactory = _new_session_id,
) -> CollectOptions:
    """Convert argparse values to validated CLI options with concise parser errors."""
    outcome = _outcome_from_text(namespace.outcome)
    if outcome is None:
        raise ValueError("outcome must be yes or no")
    database_text = namespace.database
    if not isinstance(database_text, str) or not database_text.strip():
        raise ValueError("database path must be non-blank")
    try:
        session_id = (
            MarketDataSessionId(namespace.session_id)
            if namespace.session_id is not None
            else session_id_factory()
        )
        return CollectOptions(
            market_id=MarketId(namespace.market_id),
            outcome=outcome,
            database_path=Path(database_text),
            duration_seconds=namespace.duration_seconds,
            session_id=session_id,
        )
    except ValueError as error:
        raise ValueError(str(error)) from error


def parse_collect_options(
    parser: argparse.ArgumentParser,
    argv: list[str] | None,
    session_id_factory: SessionIdFactory = _new_session_id,
) -> CollectOptions:
    """Parse the command and delegate invalid input handling to argparse."""
    namespace = parser.parse_args(argv)
    try:
        return options_from_namespace(namespace, session_id_factory)
    except ValueError as error:
        parser.error(str(error))
        raise AssertionError("argparse.error must not return")


async def collect(
    options: CollectOptions,
    dependencies: _CollectionDependencies = _DEFAULT_DEPENDENCIES,
    stderr: TextIO = sys.stderr,
) -> CollectionSummary:
    """Resolve, initialize, record, and summarize one public market token run."""
    async with dependencies.http_client_factory() as client:
        catalog = dependencies.catalog_factory(client)
        market = await _resolve_active_market(catalog, options.market_id)
        token_id = _token_for_outcome(market, options.outcome)
        database_path = _prepare_database_path(options.database_path)
        async with dependencies.sqlite_connection_factory(database_path) as connection:
            journal = dependencies.journal_factory(connection)
            await journal.initialize()
            session = MarketDataSession(
                dependencies.stream_gateway_factory(),
                market.market_id,
                token_id,
                market.tick_size,
            )
            recorder = MarketDataSessionRecorder(
                session, journal, options.session_id, dependencies.clock
            )
            _print_progress(stderr, options, market)
            if options.duration_seconds is None:
                result = await recorder.run()
                return _summary_from_result(options, result)
            try:
                async with asyncio.timeout(options.duration_seconds):
                    result = await recorder.run()
            except TimeoutError:
                persisted = await _finished_persisted_session(
                    journal, options.session_id
                )
                return _summary_from_persisted(options, persisted)
            return _summary_from_result(options, result)


def _prepare_database_path(path: Path) -> Path:
    """Resolve a safe local database target and create only its missing parents."""
    resolved_path = path.expanduser().resolve(strict=False)
    if resolved_path.exists() and resolved_path.is_dir():
        raise CollectionOperationalError("database path is an existing directory")
    try:
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise CollectionOperationalError(
            "could not create database directory"
        ) from error
    return resolved_path


async def _resolve_active_market(
    catalog: _MarketCatalog, market_id: MarketId
) -> Market:
    """Fetch one market before any journal or stream resource is created."""
    try:
        market = await catalog.get_market(market_id)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        raise CollectionOperationalError("could not resolve market") from error
    if market is None:
        raise CollectionOperationalError("market was not found")
    if market.status is not MarketStatus.ACTIVE:
        raise CollectionOperationalError("market is not active")
    return market


def _token_for_outcome(market: Market, outcome: Outcome) -> TokenId:
    """Select the normalized token mapping owned by catalog metadata."""
    return market.yes_token_id if outcome is Outcome.YES else market.no_token_id


async def _finished_persisted_session(
    journal: MarketDataJournalGateway, session_id: MarketDataSessionId
) -> PersistedMarketDataSession:
    """Require a timeout-cancelled recorder to have finalized durable evidence."""
    try:
        session = await journal.get_session(session_id)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        raise CollectionOperationalError("could not read finalized session") from error
    if session is None or session.finished_at is None or session.final_status is None:
        raise CollectionOperationalError("duration ended before journal finalization")
    return session


def _summary_from_result(
    options: CollectOptions, result: MarketDataRecordingResult
) -> CollectionSummary:
    """Translate a normal recorder result into the stable command output shape."""
    return CollectionSummary(
        session_id=result.session_id,
        market_id=result.final_view.market_id,
        token_id=result.final_view.token_id,
        outcome=options.outcome,
        database_path=options.database_path,
        termination="stream_ended",
        updates_recorded=result.updates_recorded,
        final_status=result.final_view.status.name,
        started_at=result.started_at,
        finished_at=result.finished_at,
        final_reason=result.final_view.status_reason,
    )


def _summary_from_persisted(
    options: CollectOptions, session: PersistedMarketDataSession
) -> CollectionSummary:
    """Translate a timeout-finalized persisted session into command output."""
    assert session.finished_at is not None
    assert session.final_status is not None
    return CollectionSummary(
        session_id=session.session_id,
        market_id=session.market_id,
        token_id=session.token_id,
        outcome=options.outcome,
        database_path=options.database_path,
        termination="duration_reached",
        updates_recorded=session.last_sequence_number,
        final_status=session.final_status.name,
        started_at=session.started_at,
        finished_at=session.finished_at,
        final_reason=session.final_reason,
    )


def _print_progress(stderr: TextIO, options: CollectOptions, market: Market) -> None:
    """Print the sole concise progress line while reserving stdout for JSON."""
    duration = (
        " until stream ends"
        if options.duration_seconds is None
        else f" for {options.duration_seconds}s"
    )
    print(
        f"recording session {options.session_id.value}: {market.question} "
        f"[{options.outcome.value}] to {options.database_path}{duration}",
        file=stderr,
    )


def _outcome_from_text(value: object) -> Outcome | None:
    """Normalize the command's intentionally small outcome vocabulary."""
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return Outcome(normalized) if normalized in Outcome.__members__ else None
