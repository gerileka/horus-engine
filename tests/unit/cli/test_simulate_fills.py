"""Read-only CLI tests for conservative historical maker-fill simulation."""

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

import horus_engine.application as application
import horus_engine.cli.simulate_fills as simulate_module
from horus_engine.application import (
    BookSnapshotReceived,
    ConservativeFillAnalysis,
    ConservativeFillSimulator,
    LocalBookStatus,
    LocalOrderBookView,
    MarketDataEvent,
    MarketDataJournalGateway,
    MarketDataReplayResult,
    MarketDataReplayStep,
    MarketDataSessionId,
    MarketId,
    PersistedMarketDataSession,
    ShadowQuote,
    ShadowQuoteAction,
    ShadowQuoteAnalysis,
    ShadowQuoteConfig,
    ShadowQuoteDecision,
    ShadowQuoteSuppressionReason,
    TokenId,
    TradeObserved,
)
from horus_engine.cli.main import build_parser, main
from horus_engine.cli.shadow import _ShadowDependencies
from horus_engine.cli.simulate_fills import (
    SimulateFillsOperationalError,
    SimulateFillsOptions,
    SimulateFillsSummary,
    _fill_record,
    options_from_namespace,
    parse_simulate_fills_options,
    simulate_fills,
    write_fills_jsonl,
)
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

_SESSION = MarketDataSessionId("fill-session")
_MARKET = MarketId("market")
_TOKEN = TokenId("token")
_TIME = datetime(2026, 8, 2, tzinfo=UTC)
_QUOTE = ShadowQuote(Price("0.40"), Price("0.60"), Quantity("5"))


def _book(
    bids: tuple[tuple[str, str], ...] = (("0.40", "10"),),
    asks: tuple[tuple[str, str], ...] = (("0.60", "8"),),
) -> OrderBook:
    """Build compact exact aggregate depth for synthetic replay input."""
    return OrderBook(
        (OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in bids),
        (OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in asks),
    )


def _analysis(
    rows: list[
        tuple[MarketDataEvent, OrderBook, ShadowQuoteAction, ShadowQuote | None]
    ],
) -> ShadowQuoteAnalysis:
    """Build explicit shadow lifecycle input without a database or network."""
    steps = tuple(
        MarketDataReplayStep(
            _SESSION,
            index,
            event,
            LocalOrderBookView(
                _MARKET,
                _TOKEN,
                TickSize("0.01"),
                LocalBookStatus.SYNCHRONIZED,
                book,
                _TIME,
                None,
            ),
            False,
        )
        for index, (event, book, _, _) in enumerate(rows, start=1)
    )
    session = PersistedMarketDataSession(
        _SESSION,
        _MARKET,
        _TOKEN,
        TickSize("0.01"),
        _TIME,
        None,
        None,
        None,
        None,
        None,
        len(steps),
    )
    replay = MarketDataReplayResult(
        session, steps, steps[-1].view, steps[-1].view, False
    )
    decisions = tuple(
        ShadowQuoteDecision(
            step,
            None,
            quote,
            action,
            None
            if quote is not None
            else ShadowQuoteSuppressionReason.BOOK_UNAVAILABLE,
        )
        for step, (_, _, action, quote) in zip(steps, rows, strict=True)
    )
    return ShadowQuoteAnalysis(
        replay, "test", ShadowQuoteConfig(1, Quantity("5")), decisions
    )


def _trade(side: Side, price: str, quantity: str) -> TradeObserved:
    """Build one fixed-identity public trade."""
    return TradeObserved(_MARKET, _TOKEN, side, Price(price), Quantity(quantity), _TIME)


class _Connection(AbstractAsyncContextManager[aiosqlite.Connection]):
    """Tiny context manager used to prove command composition owns no network."""

    async def __aenter__(self) -> aiosqlite.Connection:
        return cast(aiosqlite.Connection, object())

    async def __aexit__(self, *args: object) -> None:
        return None


class _FailingConnection(AbstractAsyncContextManager[aiosqlite.Connection]):
    """Connection that exposes replay operational failure behavior."""

    async def __aenter__(self) -> aiosqlite.Connection:
        raise OSError("unavailable")

    async def __aexit__(self, *args: object) -> None:
        return None


def _filled_analysis() -> ConservativeFillAnalysis:
    """Build a one-fill result suitable for JSONL serialization assertions."""
    book = _book((("0.40", "0.1"),), (("0.60", "1"),))
    shadow = _analysis(
        [
            (
                BookSnapshotReceived(_MARKET, _TOKEN, book, _TIME),
                book,
                ShadowQuoteAction.PLACE,
                _QUOTE,
            ),
            (
                _trade(Side.SELL, "0.40", "2"),
                book,
                ShadowQuoteAction.HOLD,
                _QUOTE,
            ),
        ]
    )
    return ConservativeFillSimulator().simulate(shadow)


def test_options_parser_summary_and_atomic_fill_jsonl(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Validate required arguments and emit exact, atomic JSONL evidence."""
    database = tmp_path / "journal.db"
    database.touch()
    parser = build_parser()
    options = parse_simulate_fills_options(
        parser,
        ["simulate-fills", "--database", str(database), "--session-id", "fill-session"],
    )
    assert options == SimulateFillsOptions(
        database, _SESSION, "microprice", 2, Quantity("10")
    )
    with pytest.raises(SystemExit) as missing_database:
        parse_simulate_fills_options(parser, ["simulate-fills", "--session-id", "x"])
    assert missing_database.value.code == 2
    with pytest.raises(SystemExit) as invalid_quantity:
        parse_simulate_fills_options(
            parser,
            [
                "simulate-fills",
                "--database",
                str(database),
                "--session-id",
                "x",
                "--quantity",
                "0",
            ],
        )
    assert invalid_quantity.value.code == 2
    with pytest.raises(ValueError, match="database"):
        options_from_namespace(
            argparse.Namespace(
                database=" ",
                session_id="x",
                model="microprice",
                half_spread_ticks=2,
                quantity="1",
                fills_output=None,
            )
        )
    with pytest.raises(ValueError, match="directory"):
        SimulateFillsOptions(database, _SESSION, "midpoint", 1, Quantity("1"), tmp_path)
    with pytest.raises(ValueError, match="model"):
        SimulateFillsOptions(database, _SESSION, "invalid", 1, Quantity("1"))
    with pytest.raises(ValueError, match="non-blank"):
        SimulateFillsOptions(
            cast(Path, "journal.db"), _SESSION, "midpoint", 1, Quantity("1")
        )
    with pytest.raises(ValueError, match="existing"):
        SimulateFillsOptions(
            tmp_path / "absent.db", _SESSION, "midpoint", 1, Quantity("1")
        )
    with pytest.raises(ValueError, match="session_id"):
        SimulateFillsOptions(
            database, cast(MarketDataSessionId, "session"), "midpoint", 1, Quantity("1")
        )
    with pytest.raises(ValueError, match="Path"):
        SimulateFillsOptions(
            database, _SESSION, "midpoint", 1, Quantity("1"), cast(Path, "fills.jsonl")
        )

    analysis = _filled_analysis()
    summary = json.loads(SimulateFillsSummary(analysis, database).as_json())
    assert summary["fill_events"] == 1
    assert summary["buy_notional"] == "0.760"
    output = tmp_path / "new" / "fills.jsonl"
    write_fills_jsonl(output, analysis)
    record = json.loads(output.read_text())
    assert record == _fill_record(analysis.fills[0])
    assert record["observed_at"].endswith("+00:00")
    assert record["quote_price"] == "0.40"
    with pytest.raises(TypeError, match="SimulatedMakerFill"):
        _fill_record(object())
    original_replace = Path.replace

    def fail_replace(path: Path, target: Path) -> Path:
        raise OSError("disk unavailable")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(SimulateFillsOperationalError, match="write"):
        write_fills_jsonl(tmp_path / "failure.jsonl", analysis)
    monkeypatch.setattr(Path, "replace", original_replace)


def test_async_composition_and_main_emit_only_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Replay through injected journal dependencies and route failures to stderr."""
    database = tmp_path / "journal.db"
    database.touch()
    replay = _filled_analysis().shadow_analysis.replay

    class _Replayer:
        def __init__(self, journal: object) -> None:
            assert journal == "journal"

        async def replay(
            self, session_id: MarketDataSessionId
        ) -> MarketDataReplayResult:
            assert session_id == _SESSION
            return replay

    monkeypatch.setattr(application, "MarketDataSessionReplayer", _Replayer)
    dependencies = _ShadowDependencies(
        lambda _path: _Connection(),
        lambda _connection: cast(MarketDataJournalGateway, "journal"),
    )
    options = SimulateFillsOptions(database, _SESSION, "midpoint", 1, Quantity("2"))
    stderr = io.StringIO()
    summary = asyncio.run(simulate_fills(options, dependencies, stderr))
    assert json.loads(summary.as_json())["session_id"] == "fill-session"
    assert "simulating fills" in stderr.getvalue()
    fills_output = tmp_path / "nested" / "fills.jsonl"
    asyncio.run(
        simulate_fills(
            SimulateFillsOptions(
                database, _SESSION, "midpoint", 1, Quantity("2"), fills_output
            ),
            dependencies,
        )
    )
    assert fills_output.exists()

    stdout = io.StringIO()
    assert (
        main(
            [
                "simulate-fills",
                "--database",
                str(database),
                "--session-id",
                "fill-session",
                "--model",
                "midpoint",
            ],
            simulate_fills_dependencies=dependencies,
            stdout=stdout,
            stderr=io.StringIO(),
        )
        == 0
    )
    assert json.loads(stdout.getvalue())["fair_value_model"] == "midpoint"

    with pytest.raises(SimulateFillsOperationalError, match="replay"):
        asyncio.run(
            simulate_fills(
                options,
                _ShadowDependencies(
                    lambda _path: _FailingConnection(),
                    lambda _connection: cast(MarketDataJournalGateway, "journal"),
                ),
            )
        )

    monkeypatch.setattr(simulate_module, "ConservativeFillSimulator", lambda: object())
    with pytest.raises(SimulateFillsOperationalError, match="simulate"):
        asyncio.run(simulate_fills(options, dependencies))

    class _CancelledReplayer:
        def __init__(self, journal: object) -> None:
            del journal

        async def replay(
            self, session_id: MarketDataSessionId
        ) -> MarketDataReplayResult:
            del session_id
            raise asyncio.CancelledError()

    monkeypatch.setattr(application, "MarketDataSessionReplayer", _CancelledReplayer)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(simulate_fills(options, dependencies))
