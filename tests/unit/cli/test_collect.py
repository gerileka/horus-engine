"""Focused, network-free tests for the market-data collector command."""

from __future__ import annotations

import argparse
import asyncio
import io
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import aiosqlite
import httpx
import pytest

import horus_engine.cli.collect as collect_module
from horus_engine.application import (
    BookSnapshotReceived,
    LocalBookStatus,
    Market,
    MarketDataDisconnected,
    MarketDataJournalGateway,
    MarketDataSessionId,
    MarketDataStreamGateway,
    MarketId,
    MarketStatus,
    TokenId,
)
from horus_engine.cli.collect import (
    CollectionOperationalError,
    CollectOptions,
    _CollectionDependencies,
    _finished_persisted_session,
    _open_sqlite_connection,
    _outcome_from_text,
    _prepare_database_path,
    _resolve_active_market,
    _summary_from_persisted,
    _token_for_outcome,
    options_from_namespace,
)
from horus_engine.cli.main import build_parser, main
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Outcome,
    Price,
    Quantity,
    TickSize,
)
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal

_MARKET_ID = MarketId("condition-1")
_YES_TOKEN = TokenId("yes-token")
_NO_TOKEN = TokenId("no-token")
_STARTED = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


def _market(status: MarketStatus = MarketStatus.ACTIVE) -> Market:
    """Build normalized catalog metadata for one deterministic command run."""
    return Market(
        _MARKET_ID,
        "Will this collector persist normalized public data?",
        _YES_TOKEN,
        _NO_TOKEN,
        TickSize("0.01"),
        Quantity("1"),
        status,
    )


def _snapshot() -> BookSnapshotReceived:
    """Build the authoritative event required to initialize a session."""
    return BookSnapshotReceived(
        _MARKET_ID,
        _YES_TOKEN,
        OrderBook((OrderBookLevel(Price("0.5"), Quantity("2")),), ()),
        _STARTED,
    )


class FakeCatalog:
    """Small catalog fake that records the requested market identifier."""

    def __init__(
        self, result: Market | None = None, error: Exception | None = None
    ) -> None:
        self.result = result
        self.error = error
        self.requests: list[MarketId] = []

    async def get_market(self, market_id: MarketId) -> Market | None:
        """Return the configured normalized metadata or focused failure."""
        self.requests.append(market_id)
        if self.error is not None:
            raise self.error
        return self.result


class FakeStream:
    """A stream gateway fake with explicit events and an optional failure."""

    def __init__(
        self,
        events: tuple[object, ...],
        error: Exception | None = None,
        *,
        block_after_events: bool = False,
    ) -> None:
        self.events = events
        self.error = error
        self.block_after_events = block_after_events
        self.calls: list[tuple[MarketId, tuple[TokenId, ...]]] = []

    def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[object]:
        """Return one simple async iterator over configured normalized events."""
        self.calls.append((market_id, token_ids))
        return self._stream()

    async def _stream(self) -> AsyncIterator[object]:
        for event in self.events:
            yield event
        if self.block_after_events:
            task = asyncio.current_task()
            assert task is not None
            asyncio.get_running_loop().call_soon(task.cancel)
            await asyncio.Event().wait()
        if self.error is not None:
            raise self.error


class RecordingClient(AbstractAsyncContextManager[httpx.AsyncClient]):
    """Wrap a real HTTP client context solely to confirm caller-owned closure."""

    def __init__(self) -> None:
        self.client = httpx.AsyncClient()
        self.closed = False

    async def __aenter__(self) -> httpx.AsyncClient:
        return self.client

    async def __aexit__(self, *args: object) -> None:
        await self.client.aclose()
        self.closed = True


def _dependencies(
    catalog: FakeCatalog,
    stream: FakeStream,
    database_connections: list[aiosqlite.Connection],
    *,
    journal_factory: collect_module.JournalFactory = SQLiteMarketDataJournal,
) -> tuple[_CollectionDependencies, RecordingClient]:
    """Build deterministic composition dependencies using real SQLite connections."""
    client_context = RecordingClient()

    def connect(path: Path) -> aiosqlite.Connection:
        connection = aiosqlite.connect(path)
        database_connections.append(connection)
        return connection

    times = iter((_STARTED, _STARTED + timedelta(seconds=2)))
    dependencies = _CollectionDependencies(
        http_client_factory=lambda: client_context,
        sqlite_connection_factory=connect,
        catalog_factory=lambda _client: catalog,
        stream_gateway_factory=lambda: cast(MarketDataStreamGateway, stream),
        journal_factory=journal_factory,
        clock=lambda: next(times),
        session_id_factory=lambda: MarketDataSessionId("generated-session"),
    )
    return dependencies, client_context


def _options(
    path: Path, outcome: Outcome = Outcome.YES, duration: int | None = None
) -> CollectOptions:
    """Build valid command options with a stable session ID."""
    return CollectOptions(
        _MARKET_ID,
        outcome,
        path,
        duration,
        MarketDataSessionId("session-1"),
    )


def test_parser_accepts_normalized_arguments_defaults_and_generated_id() -> None:
    """Accept the only command and normalize its small explicit vocabulary."""
    options = collect_module.parse_collect_options(
        build_parser(),
        ["collect", "--market-id", "condition-1", "--outcome", "yEs"],
        lambda: MarketDataSessionId("generated"),
    )

    assert options.market_id == _MARKET_ID
    assert options.outcome is Outcome.YES
    assert options.database_path == Path("data/horus.db")
    assert options.duration_seconds is None
    assert options.session_id == MarketDataSessionId("generated")


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["collect", "--outcome", "yes"], "--market-id"),
        (["collect", "--market-id", "condition-1"], "--outcome"),
        (["collect", "--market-id", "condition-1", "--outcome", "maybe"], "outcome"),
        (
            [
                "collect",
                "--market-id",
                "condition-1",
                "--outcome",
                "no",
                "--duration-seconds",
                "0",
            ],
            "duration",
        ),
        (
            [
                "collect",
                "--market-id",
                "condition-1",
                "--outcome",
                "no",
                "--duration-seconds",
                "-1",
            ],
            "duration",
        ),
        (
            ["collect", "--market-id", "condition-1", "--outcome", "no", "--unknown"],
            "unrecognized",
        ),
        (
            [
                "collect",
                "--market-id",
                "condition-1",
                "--outcome",
                "no",
                "--session-id",
                "  ",
            ],
            "identifier",
        ),
    ],
)
def test_parser_rejects_invalid_arguments(
    arguments: list[str], message: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Route all user input validation through argparse's exit code two."""
    with pytest.raises(SystemExit) as raised:
        collect_module.parse_collect_options(build_parser(), arguments)
    assert raised.value.code == 2
    assert message in capsys.readouterr().err


def test_parser_accepts_no_custom_path_duration_and_explicit_session() -> None:
    """Preserve supplied local path and explicit identifier values."""
    options = collect_module.parse_collect_options(
        build_parser(),
        [
            "collect",
            "--market-id",
            "condition-1",
            "--outcome",
            "NO",
            "--database",
            "nested/horus.db",
            "--duration-seconds",
            "3",
            "--session-id",
            "provided-session",
        ],
    )
    assert options.outcome is Outcome.NO
    assert options.database_path == Path("nested/horus.db")
    assert options.duration_seconds == 3
    assert options.session_id == MarketDataSessionId("provided-session")


def test_options_are_immutable_and_reject_invalid_local_values(tmp_path: Path) -> None:
    """Keep options as validated command data, never infrastructure state."""
    options = _options(tmp_path / "journal.db")
    with pytest.raises(AttributeError):
        options.outcome = Outcome.NO  # type: ignore[misc]
    with pytest.raises(ValueError, match="directory"):
        _options(tmp_path)
    with pytest.raises(ValueError, match="strictly positive"):
        _options(tmp_path / "x.db", duration=0)
    with pytest.raises(ValueError, match="MarketId"):
        CollectOptions(
            cast(MarketId, "market"),
            Outcome.YES,
            tmp_path / "x.db",
            None,
            MarketDataSessionId("id"),
        )
    with pytest.raises(ValueError, match="Outcome"):
        CollectOptions(
            _MARKET_ID,
            cast(Outcome, "YES"),
            tmp_path / "x.db",
            None,
            MarketDataSessionId("id"),
        )
    with pytest.raises(ValueError, match="database"):
        CollectOptions(
            _MARKET_ID,
            Outcome.YES,
            cast(Path, "x.db"),
            None,
            MarketDataSessionId("id"),
        )
    with pytest.raises(ValueError, match="session_id"):
        CollectOptions(
            _MARKET_ID,
            Outcome.YES,
            tmp_path / "x.db",
            None,
            cast(MarketDataSessionId, "id"),
        )
    assert _outcome_from_text(" no ") is Outcome.NO
    assert _outcome_from_text(1) is None


def test_namespace_validation_rejects_blank_database_and_invalid_outcome() -> None:
    """Keep values constructed outside argparse subject to the same rules."""
    with pytest.raises(ValueError, match="database"):
        options_from_namespace(
            argparse.Namespace(
                market_id="condition-1",
                outcome="yes",
                database=" ",
                duration_seconds=None,
                session_id=None,
            )
        )
    with pytest.raises(ValueError, match="outcome"):
        options_from_namespace(
            argparse.Namespace(
                market_id="condition-1",
                outcome="maybe",
                database="x.db",
                duration_seconds=None,
                session_id=None,
            )
        )


def test_collect_records_normal_session_and_closes_resources(tmp_path: Path) -> None:
    """Compose actual session, recorder, and SQLite journal below the parser."""
    connections: list[aiosqlite.Connection] = []
    catalog = FakeCatalog(_market())
    stream = FakeStream((_snapshot(), MarketDataDisconnected("closed", _STARTED)))
    dependencies, client = _dependencies(catalog, stream, connections)
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = main(
        [
            "collect",
            "--market-id",
            "condition-1",
            "--outcome",
            "yes",
            "--database",
            str(tmp_path / "nested" / "journal.db"),
        ],
        dependencies=dependencies,
        stdout=stdout,
        stderr=stderr,
    )

    payload = __import__("json").loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["termination"] == "stream_ended"
    assert payload["token_id"] == "yes-token"
    assert payload["final_status"] == "STALE"
    assert payload["final_reason"] == "market-data disconnection requires snapshot"
    assert payload["updates_recorded"] == 2
    assert "recording session generated-session" in stderr.getvalue()
    assert catalog.requests == [_MARKET_ID]
    assert stream.calls == [(_MARKET_ID, (_YES_TOKEN,))]
    assert (tmp_path / "nested" / "journal.db").exists()
    assert client.closed
    assert connections[0]._connection is None  # noqa: SLF001


@pytest.mark.parametrize(
    "status", [MarketStatus.SUSPENDED, MarketStatus.CLOSED, MarketStatus.RESOLVED]
)
def test_collect_rejects_unavailable_markets_before_opening_resources(
    tmp_path: Path, status: MarketStatus
) -> None:
    """Reject every inactive normalized market without opening a stream or journal."""
    connections: list[aiosqlite.Connection] = []
    stream = FakeStream(())
    dependencies, _client = _dependencies(
        FakeCatalog(_market(status)), stream, connections
    )
    with pytest.raises(CollectionOperationalError, match="not active"):
        asyncio.run(
            collect_module.collect(_options(tmp_path / "journal.db"), dependencies)
        )
    assert stream.calls == []
    assert connections == []


def test_collect_handles_unknown_and_catalog_failure_without_stream(
    tmp_path: Path,
) -> None:
    """Return focused operational errors for failed metadata resolution."""
    for catalog in (FakeCatalog(), FakeCatalog(error=RuntimeError("offline"))):
        connections: list[aiosqlite.Connection] = []
        stream = FakeStream(())
        dependencies, _client = _dependencies(catalog, stream, connections)
        with pytest.raises(CollectionOperationalError):
            asyncio.run(
                collect_module.collect(_options(tmp_path / "journal.db"), dependencies)
            )
        assert stream.calls == []
        assert connections == []


def test_collect_selects_no_token_and_reports_runtime_failure(tmp_path: Path) -> None:
    """Use catalog-owned NO mapping and never print JSON after a recorder error."""
    connections: list[aiosqlite.Connection] = []
    stream = FakeStream((), RuntimeError("stream failed"))
    dependencies, _client = _dependencies(FakeCatalog(_market()), stream, connections)
    stdout = io.StringIO()
    stderr = io.StringIO()
    assert (
        main(
            [
                "collect",
                "--market-id",
                "condition-1",
                "--outcome",
                "no",
                "--database",
                str(tmp_path / "journal.db"),
            ],
            dependencies=dependencies,
            stdout=stdout,
            stderr=stderr,
        )
        == 1
    )
    assert stdout.getvalue() == ""
    assert "collection failed" in stderr.getvalue()
    assert stream.calls == [(_MARKET_ID, (_NO_TOKEN,))]
    assert _token_for_outcome(_market(), Outcome.NO) == _NO_TOKEN


def test_duration_that_ends_normally_uses_stream_termination(tmp_path: Path) -> None:
    """A stream ending before its requested wall-clock duration is normal success."""
    connections: list[aiosqlite.Connection] = []
    dependencies, _client = _dependencies(
        FakeCatalog(_market()), FakeStream((_snapshot(),)), connections
    )
    summary = asyncio.run(
        collect_module.collect(
            _options(tmp_path / "journal.db", duration=1), dependencies
        )
    )
    assert summary.termination == "stream_ended"


class _ImmediateTimeout:
    """Deterministically turn recorder cancellation into the timeout boundary error."""

    async def __aenter__(self) -> _ImmediateTimeout:
        return self

    async def __aexit__(self, exc_type: object, *_args: object) -> bool:
        if exc_type is asyncio.CancelledError:
            raise TimeoutError
        return False


def test_duration_cancellation_reads_finalized_persisted_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Timeout cancellation finalizes first, then reports durable session state."""
    monkeypatch.setattr(
        "horus_engine.cli.collect.asyncio.timeout",
        lambda _seconds: _ImmediateTimeout(),
    )
    connections: list[aiosqlite.Connection] = []
    stream = FakeStream((_snapshot(),), block_after_events=True)
    dependencies, _client = _dependencies(FakeCatalog(_market()), stream, connections)

    summary = asyncio.run(
        collect_module.collect(
            _options(tmp_path / "journal.db", duration=1), dependencies
        )
    )

    assert summary.termination == "duration_reached"
    assert summary.updates_recorded == 1
    assert summary.final_status == "SYNCHRONIZED"


class _MissingSessionJournal(SQLiteMarketDataJournal):
    """A journal fake that lets timeout verification exercise missing evidence."""

    async def get_session(self, session_id: MarketDataSessionId) -> None:
        return None


def test_duration_requires_a_finished_persisted_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A timed-out run without durable final evidence is an operational failure."""
    monkeypatch.setattr(
        "horus_engine.cli.collect.asyncio.timeout",
        lambda _seconds: _ImmediateTimeout(),
    )
    connections: list[aiosqlite.Connection] = []
    dependencies, _client = _dependencies(
        FakeCatalog(_market()),
        FakeStream((_snapshot(),), block_after_events=True),
        connections,
        journal_factory=_MissingSessionJournal,
    )
    with pytest.raises(CollectionOperationalError, match="finalization"):
        asyncio.run(
            collect_module.collect(
                _options(tmp_path / "journal.db", duration=1), dependencies
            )
        )


def test_operational_main_errors_path_and_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep expected failures concise and let the synchronous boundary handle Ctrl+C."""
    with pytest.raises(CollectionOperationalError, match="directory"):
        _prepare_database_path(tmp_path)

    stderr = io.StringIO()

    def interrupt(coroutine: object) -> object:
        close = getattr(coroutine, "close")
        close()
        raise KeyboardInterrupt

    monkeypatch.setattr("horus_engine.cli.main.asyncio.run", interrupt)
    assert (
        main(
            ["collect", "--market-id", "condition-1", "--outcome", "yes"], stderr=stderr
        )
        == 130
    )
    assert stderr.getvalue() == "collection interrupted\n"


def test_main_prints_operational_error_for_unknown_market(tmp_path: Path) -> None:
    """Turn expected catalog-resolution failures into exit code one at the boundary."""
    connections: list[aiosqlite.Connection] = []
    dependencies, _client = _dependencies(FakeCatalog(), FakeStream(()), connections)
    stderr = io.StringIO()
    assert (
        main(
            [
                "collect",
                "--market-id",
                "condition-1",
                "--outcome",
                "yes",
                "--database",
                str(tmp_path / "journal.db"),
            ],
            dependencies=dependencies,
            stderr=stderr,
        )
        == 1
    )
    assert stderr.getvalue() == "error: market was not found\n"


def test_finished_session_reader_wraps_journal_errors() -> None:
    """Do not leak persistence implementation failures from duration completion."""

    class Journal:
        async def get_session(self, session_id: MarketDataSessionId) -> object:
            raise RuntimeError("storage error")

    with pytest.raises(CollectionOperationalError, match="could not read"):
        asyncio.run(
            _finished_persisted_session(
                cast(MarketDataJournalGateway, Journal()), MarketDataSessionId("id")
            )
        )


def test_cancellation_and_local_path_error_cases_propagate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Never convert cancellation into an operational error in CLI helpers."""

    class CancelledCatalog:
        async def get_market(self, market_id: MarketId) -> Market | None:
            raise asyncio.CancelledError

    class CancelledJournal:
        async def get_session(self, session_id: MarketDataSessionId) -> object:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_resolve_active_market(CancelledCatalog(), _MARKET_ID))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            _finished_persisted_session(
                cast(MarketDataJournalGateway, CancelledJournal()),
                MarketDataSessionId("id"),
            )
        )
    monkeypatch.setattr(
        Path, "mkdir", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError())
    )
    with pytest.raises(CollectionOperationalError, match="could not create"):
        _prepare_database_path(tmp_path / "new" / "journal.db")


def test_default_sqlite_factory_and_nonreturning_parser_error() -> None:
    """Cover small default factories and argparse's defensive non-return guard."""
    connection = _open_sqlite_connection(Path(":memory:"))

    async def close() -> None:
        async with connection:
            assert await (await connection.execute("SELECT 1")).fetchone() == (1,)

    asyncio.run(close())

    class Parser:
        def parse_args(self, argv: list[str] | None) -> argparse.Namespace:
            return argparse.Namespace(
                market_id="condition-1",
                outcome="invalid",
                database="x.db",
                duration_seconds=None,
                session_id=None,
            )

        def error(self, message: str) -> None:
            return None

    with pytest.raises(AssertionError):
        collect_module.parse_collect_options(
            cast(argparse.ArgumentParser, Parser()), []
        )


def test_persisted_summary_uses_stable_enum_name_and_null_reason() -> None:
    """Serialize timeout output without dataclass reprs or numeric enum values."""
    from horus_engine.application import PersistedMarketDataSession

    session = PersistedMarketDataSession(
        MarketDataSessionId("id"),
        _MARKET_ID,
        _YES_TOKEN,
        TickSize("0.01"),
        _STARTED,
        _STARTED + timedelta(seconds=1),
        LocalBookStatus.SYNCHRONIZED,
        TickSize("0.01"),
        _STARTED,
        None,
        7,
    )
    summary = _summary_from_persisted(_options(Path("journal.db")), session)
    assert '"final_reason": null' in summary.as_json()
    assert summary.final_status == "SYNCHRONIZED"
