"""Focused, read-only composition tests for the shadow quote CLI."""

import argparse
import asyncio
import io
import json
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import aiosqlite
import pytest

import horus_engine.cli.shadow as shadow_module
from horus_engine.application import (
    LocalBookStatus,
    LocalOrderBookView,
    MarketDataJournalGateway,
    MarketDataReplayResult,
    MarketDataReplayStep,
    MarketDataSessionId,
    MarketId,
    MidpointFairValueModel,
    PersistedMarketDataSession,
    ShadowQuoteAnalyzer,
    ShadowQuoteConfig,
    TokenId,
    TradeObserved,
)
from horus_engine.cli.main import build_parser, main
from horus_engine.cli.shadow import (
    ShadowOperationalError,
    ShadowOptions,
    ShadowSummary,
    _open_sqlite_connection,
    _ShadowDependencies,
    model_from_name,
    options_from_namespace,
    parse_shadow_options,
    shadow,
)
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

_SESSION_ID = MarketDataSessionId("stored-session")
_MARKET_ID = MarketId("market-1")
_TOKEN_ID = TokenId("token-1")
_TIME = datetime(2026, 8, 2, tzinfo=UTC)


def _analysis() -> ShadowQuoteAnalyzer:
    """Create an analyzer with the default command model and fixed exact inputs."""
    return ShadowQuoteAnalyzer(
        MidpointFairValueModel(), ShadowQuoteConfig(1, Quantity("2"))
    )


def _replay() -> MarketDataReplayResult:
    """Build an empty persisted replay sufficient to exercise JSON composition."""
    view = LocalOrderBookView(
        _MARKET_ID,
        _TOKEN_ID,
        TickSize("0.01"),
        LocalBookStatus.AWAITING_SNAPSHOT,
        OrderBook((OrderBookLevel(Price("0.4"), Quantity("1")),), ()),
        _TIME,
        None,
    )
    session = PersistedMarketDataSession(
        _SESSION_ID,
        _MARKET_ID,
        _TOKEN_ID,
        TickSize("0.01"),
        _TIME,
        None,
        None,
        None,
        None,
        None,
        0,
    )
    return MarketDataReplayResult(session, (), view, view, False)


def _replay_with_decision() -> MarketDataReplayResult:
    """Build one synchronized two-sided step for JSONL decision output tests."""
    view = LocalOrderBookView(
        _MARKET_ID,
        _TOKEN_ID,
        TickSize("0.01"),
        LocalBookStatus.SYNCHRONIZED,
        OrderBook(
            (OrderBookLevel(Price("0.4"), Quantity("1")),),
            (OrderBookLevel(Price("0.6"), Quantity("1")),),
        ),
        _TIME,
        None,
    )
    session = PersistedMarketDataSession(
        _SESSION_ID,
        _MARKET_ID,
        _TOKEN_ID,
        TickSize("0.01"),
        _TIME,
        None,
        None,
        None,
        None,
        None,
        1,
    )
    step = MarketDataReplayStep(
        _SESSION_ID,
        1,
        TradeObserved(
            _MARKET_ID, _TOKEN_ID, Side.BUY, Price("0.5"), Quantity("1"), _TIME
        ),
        view,
        False,
    )
    return MarketDataReplayResult(session, (step,), view, view, False)


class _Connection(AbstractAsyncContextManager[aiosqlite.Connection]):
    """Tiny async context used to prove the command owns no connection lifecycle."""

    async def __aenter__(self) -> aiosqlite.Connection:
        return cast(aiosqlite.Connection, object())

    async def __aexit__(self, *args: object) -> None:
        return None


class _FailingConnection(AbstractAsyncContextManager[aiosqlite.Connection]):
    """Context that makes replay connection failure observable without SQLite writes."""

    async def __aenter__(self) -> aiosqlite.Connection:
        raise OSError("unavailable")

    async def __aexit__(self, *args: object) -> None:
        return None


def test_shadow_options_and_parser_validate_existing_read_only_input(
    tmp_path: Path,
) -> None:
    """Accept only existing journals and the intentionally small model vocabulary."""
    database = tmp_path / "journal.db"
    database.touch()
    parser = build_parser()
    options = parse_shadow_options(
        parser,
        [
            "shadow",
            "--database",
            str(database),
            "--session-id",
            "stored-session",
            "--half-spread-ticks",
            "2",
            "--quantity",
            "1.25",
        ],
    )
    assert options == ShadowOptions(
        database, _SESSION_ID, "microprice", 2, Quantity("1.25")
    )
    defaults = parse_shadow_options(
        parser,
        ["shadow", "--database", str(database), "--session-id", "stored-session"],
    )
    assert defaults == ShadowOptions(
        database, _SESSION_ID, "microprice", 2, Quantity("10")
    )
    with pytest.raises(ValueError, match="database"):
        ShadowOptions(
            cast(Path, "journal.db"), _SESSION_ID, "midpoint", 1, Quantity("1")
        )
    with pytest.raises(ValueError, match="existing file"):
        ShadowOptions(
            tmp_path / "missing.db", _SESSION_ID, "midpoint", 1, Quantity("1")
        )
    with pytest.raises(ValueError, match="model"):
        ShadowOptions(database, _SESSION_ID, "other", 1, Quantity("1"))
    with pytest.raises(ValueError, match="session_id"):
        ShadowOptions(
            database,
            cast(MarketDataSessionId, "stored-session"),
            "midpoint",
            1,
            Quantity("1"),
        )
    with pytest.raises(ValueError, match="directory"):
        ShadowOptions(
            database,
            _SESSION_ID,
            "midpoint",
            1,
            Quantity("1"),
            tmp_path / "missing" / "decisions.jsonl",
        )
    with pytest.raises(ValueError, match="must not be a directory"):
        ShadowOptions(database, _SESSION_ID, "midpoint", 1, Quantity("1"), tmp_path)
    with pytest.raises(ValueError, match="must be a Path"):
        ShadowOptions(
            database,
            _SESSION_ID,
            "midpoint",
            1,
            Quantity("1"),
            cast(Path, "decisions.jsonl"),
        )
    with pytest.raises(SystemExit) as raised:
        parse_shadow_options(
            parser,
            [
                "shadow",
                "--database",
                str(database),
                "--session-id",
                "stored-session",
                "--half-spread-ticks",
                "0",
                "--quantity",
                "1",
            ],
        )
    assert raised.value.code == 2


def test_shadow_namespace_model_selection_and_json_output(tmp_path: Path) -> None:
    """Expose exactly two fair-value model choices and decimal-safe detailed output."""
    database = tmp_path / "journal.db"
    database.touch()
    options = options_from_namespace(
        argparse.Namespace(
            database=str(database),
            session_id="stored-session",
            model="microprice",
            half_spread_ticks=1,
            quantity="3",
            decisions_output=None,
        )
    )
    assert options.model_name == "microprice"
    assert model_from_name("midpoint").name == "midpoint"
    assert model_from_name("microprice").name == "microprice"
    with pytest.raises(ValueError, match="model"):
        model_from_name("other")
    with pytest.raises(ValueError, match="database"):
        options_from_namespace(
            argparse.Namespace(
                database=" ",
                session_id="stored-session",
                model="midpoint",
                half_spread_ticks=1,
                quantity="1",
                decisions_output=None,
            )
        )

    output = json.loads(
        ShadowSummary(_analysis().analyze(_replay()), database).as_json()
    )
    assert output["quantity"] == "2"
    assert output["total_steps"] == 0
    assert output["quoteable_ratio"] == "0"
    assert "decisions" not in output


def test_shadow_replays_read_only_and_main_emits_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Compose only a replay reader, then return detailed JSON from the subcommand."""
    database = tmp_path / "journal.db"
    database.touch()

    class _Replayer:
        def __init__(self, journal: object) -> None:
            assert journal == "journal"

        async def replay(
            self, session_id: MarketDataSessionId
        ) -> MarketDataReplayResult:
            assert session_id == _SESSION_ID
            return _replay_with_decision()

    monkeypatch.setattr(shadow_module, "MarketDataSessionReplayer", _Replayer)
    dependencies = _ShadowDependencies(
        lambda _path: _Connection(),
        lambda _connection: cast(MarketDataJournalGateway, "journal"),
    )
    options = ShadowOptions(database, _SESSION_ID, "midpoint", 1, Quantity("2"))
    stderr = io.StringIO()
    summary = asyncio.run(shadow(options, dependencies, stderr))
    assert json.loads(summary.as_json())["session_id"] == "stored-session"
    assert "analyzing session" in stderr.getvalue()
    assert not (tmp_path / "omitted.jsonl").exists()

    decisions_path = tmp_path / "decisions.jsonl"
    decisions_path.write_text("old decision evidence\n")
    output_summary = asyncio.run(
        shadow(
            ShadowOptions(
                database,
                _SESSION_ID,
                "midpoint",
                1,
                Quantity("2"),
                decisions_path,
            ),
            dependencies,
            io.StringIO(),
        )
    )
    assert json.loads(output_summary.as_json())["placements"] == 1
    records = [json.loads(line) for line in decisions_path.read_text().splitlines()]
    assert records == [
        {
            "sequence_number": 1,
            "action": "place",
            "suppression_reason": None,
            "status": "SYNCHRONIZED",
            "fair_value": "0.5",
            "desired_bid": "0.49",
            "desired_ask": "0.51",
            "quantity": "2",
        }
    ]
    assert not list(tmp_path.glob(".decisions.jsonl.*.tmp"))

    def fail_replace(_source: Path, _destination: Path) -> Path:
        raise OSError("output replacement unavailable")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(ShadowOperationalError, match="decisions output"):
        asyncio.run(
            shadow(
                ShadowOptions(
                    database,
                    _SESSION_ID,
                    "midpoint",
                    1,
                    Quantity("2"),
                    tmp_path / "failed.jsonl",
                ),
                dependencies,
                io.StringIO(),
            )
        )

    stdout = io.StringIO()
    assert (
        main(
            [
                "shadow",
                "--database",
                str(database),
                "--session-id",
                "stored-session",
                "--half-spread-ticks",
                "1",
                "--quantity",
                "2",
            ],
            shadow_dependencies=dependencies,
            stdout=stdout,
            stderr=io.StringIO(),
        )
        == 0
    )
    main_output = json.loads(stdout.getvalue())
    assert main_output["fair_value_model"] == "microprice"
    assert "decisions" not in main_output


def test_shadow_converts_replay_connection_failures_to_operational_errors(
    tmp_path: Path,
) -> None:
    """Keep ordinary journal failures concise without implying any trading."""
    database = tmp_path / "journal.db"
    database.touch()
    options = ShadowOptions(database, _SESSION_ID, "midpoint", 1, Quantity("1"))
    dependencies = _ShadowDependencies(
        lambda _path: _FailingConnection(),
        lambda _connection: cast(MarketDataJournalGateway, None),
    )
    with pytest.raises(ShadowOperationalError, match="could not replay"):
        asyncio.run(shadow(options, dependencies))


def test_shadow_preserves_cancellation_and_can_open_an_existing_sqlite_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Do not convert task cancellation to an operational replay failure."""
    database = tmp_path / "journal.db"
    database.touch()

    async def close_default_connection() -> None:
        connection = _open_sqlite_connection(database)
        await connection.close()

    asyncio.run(close_default_connection())

    class _CancelledReplayer:
        def __init__(self, journal: object) -> None:
            pass

        async def replay(
            self, session_id: MarketDataSessionId
        ) -> MarketDataReplayResult:
            raise asyncio.CancelledError

    monkeypatch.setattr(shadow_module, "MarketDataSessionReplayer", _CancelledReplayer)
    dependencies = _ShadowDependencies(
        lambda _path: _Connection(),
        lambda _connection: cast(MarketDataJournalGateway, "journal"),
    )
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            shadow(
                ShadowOptions(database, _SESSION_ID, "midpoint", 1, Quantity("1")),
                dependencies,
            )
        )
