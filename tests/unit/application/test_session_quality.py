"""Adversarial journal evidence and fail-closed CLI quality decisions."""

import asyncio
import io
import json
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import aiosqlite
import pytest

import horus_engine.cli.quality as cli_module
from horus_engine.application import (
    BookSnapshotReceived,
    LocalOrderBookState,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataJournalGateway,
    MarketDataSessionId,
    MarketId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
    PriceLevelChanged,
    TokenId,
    TradeObserved,
)
from horus_engine.application.market_data_replay import MarketDataSessionReplayer
from horus_engine.application.session_quality import assess_session_quality
from horus_engine.cli.main import main
from horus_engine.cli.quality import quality
from horus_engine.cli.shadow import _ShadowDependencies
from horus_engine.domain import (
    NonNegativeQuantity,
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

_ID = MarketDataSessionId("synthetic-quality")
_MARKET, _TOKEN, _TICK = (
    MarketId("synthetic-market"),
    TokenId("synthetic-yes"),
    TickSize("0.01"),
)
_START = datetime(2026, 1, 1, tzinfo=UTC)


def _at(second: int) -> datetime:
    return _START + timedelta(seconds=second)


def _snapshot(second: int = 1) -> BookSnapshotReceived:
    return BookSnapshotReceived(
        _MARKET,
        _TOKEN,
        OrderBook(
            (OrderBookLevel(Price("0.48"), Quantity("2")),),
            (OrderBookLevel(Price("0.52"), Quantity("3")),),
        ),
        _at(second),
    )


def _trade(second: int = 2) -> TradeObserved:
    return TradeObserved(
        _MARKET, _TOKEN, Side.SELL, Price("0.49"), Quantity("1"), _at(second)
    )


class _Reader:
    def __init__(
        self, events: tuple[MarketDataEvent, ...], *, finished: bool = True
    ) -> None:
        state = LocalOrderBookState(_MARKET, _TOKEN, _TICK)
        rows = []
        for number, event in enumerate(events, 1):
            previous = state.view.book
            view = state.apply(event)
            rows.append(
                PersistedMarketDataUpdate(
                    _ID,
                    number,
                    event,
                    previous != view.book,
                    view.status,
                    view.tick_size,
                    view.last_observed_at,
                    view.status_reason,
                )
            )
        self.updates = tuple(rows)
        view = state.view
        self.session: PersistedMarketDataSession | None = PersistedMarketDataSession(
            _ID,
            _MARKET,
            _TOKEN,
            _TICK,
            _START,
            _at(10) if finished else None,
            view.status if finished else None,
            view.tick_size if finished else None,
            view.last_observed_at if finished else None,
            view.status_reason if finished else None,
            len(rows),
        )
        self.reads: list[str] = []

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession | None:
        self.reads.append("header")
        return self.session

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        self.reads.append("updates")
        return self.updates


def _report(reader: _Reader) -> dict[str, Any]:
    return asyncio.run(assess_session_quality(reader, _ID))


def test_fixed_policy_counts_sequence_periods_not_wall_clock_uptime() -> None:
    reader = _Reader((_snapshot(), _trade(), MarketDataDisconnected("end", _at(3))))
    before = reader.updates
    report = _report(reader)
    assert report["eligible"] is True and report["reasons"] == []
    assert reader.reads == ["header", "updates"]  # Replay uses the frozen evidence.
    assert reader.updates == before
    assert report["sequence_consistent"] is True
    assert (
        report["replay_integrity"] == "verified" and report["terminal_complete"] is True
    )
    metrics = report["measurements"]
    assert (metrics["updates"], metrics["snapshots"], metrics["trades"]) == (3, 1, 1)
    assert metrics["post_status_counts"] == {
        "SYNCHRONIZED": 2,
        "STALE": 1,
        "INVALID": 0,
        "AWAITING_SNAPSHOT": 0,
    }
    assert [
        (p["status"], p["first_sequence"], p["last_sequence"])
        for p in metrics["post_status_periods"]
    ] == [("SYNCHRONIZED", 1, 2), ("STALE", 3, 3)]
    assert report["policy"]["minimum_synchronized_fraction"] == "0.5"
    assert (
        "no per-event local receipt clock"
        in report["unavailable_measurements"]["synchronized_wall_clock_uptime"]
    )
    assert metrics["timestamp_regressions"] == 0
    assert metrics["source_timestamps_after_capture_finish"] == 0
    assert "message_loss_rate" in report["unavailable_measurements"]
    json.dumps(report)  # No Decimal or datetime leaks into the JSON schema.


def test_empty_unfinished_and_missing_evidence_are_distinct_from_zero() -> None:
    empty = _report(_Reader(()))
    assert empty["eligible"] is False
    assert empty["measurements"]["updates"] == 0
    assert empty["measurements"]["synchronized_observation_fraction"] is None
    assert "missing_snapshot" in empty["reasons"]
    unfinished = _report(_Reader((_snapshot(), _trade()), finished=False))
    assert unfinished["replay_integrity"] == "verified"
    assert unfinished["terminal_complete"] is False
    assert unfinished["measurements"]["source_timestamps_after_capture_finish"] is None
    assert "terminal_completeness_unverified" in unfinished["reasons"]
    missing = _Reader(())
    missing.session = None
    unavailable = _report(missing)
    assert unavailable["measurements"] is None
    assert unavailable["sequence_consistent"] is None
    assert unavailable["reasons"] == ["session_not_found"]


@pytest.mark.parametrize(
    "corruption", ["gap", "header", "post_status", "session_identity"]
)
def test_corruption_rejects_without_sorting_repair_or_strategy_input(
    corruption: str,
) -> None:
    reader = _Reader((_snapshot(), _trade()))
    assert reader.session is not None
    if corruption == "gap":
        reader.updates = (
            reader.updates[0],
            replace(reader.updates[1], sequence_number=3),
        )
    elif corruption == "header":
        reader.session = replace(reader.session, last_sequence_number=9)
    elif corruption == "post_status":
        reader.updates = (
            reader.updates[0],
            replace(reader.updates[1], book_changed=True),
        )
    else:
        reader.session = replace(
            reader.session, session_id=MarketDataSessionId("wrong")
        )
    original = reader.updates
    report = _report(reader)
    assert report["eligible"] is False
    assert reader.updates == original
    if corruption in {"gap", "header"}:
        assert report["sequence_consistent"] is False
    if corruption == "session_identity":
        assert "session_identity_mismatch" in report["reasons"]
    else:
        assert report["replay_integrity"] == "failed"
        assert report["terminal_complete"] is None


def test_timestamp_anomalies_include_trades_ignored_by_book_clock() -> None:
    # The existing book state ignores trade timestamps; the quality policy must not.
    regression = _report(_Reader((_snapshot(2), _trade(1))))
    assert regression["replay_integrity"] == "verified"
    assert regression["measurements"]["timestamp_regressions"] == 1
    assert "timestamp_regressions" in regression["reasons"]
    outside = _report(_Reader((_snapshot(), _trade(20))))
    assert outside["measurements"]["source_timestamps_after_capture_finish"] == 1
    assert "source_timestamp_outside_capture_bounds" in outside["reasons"]
    before = _report(_Reader((_snapshot(-1), _trade())))
    assert before["measurements"]["source_timestamps_before_capture_start"] == 1
    equal = _report(_Reader((_snapshot(1), _trade(1))))
    assert equal["measurements"]["equal_adjacent_timestamps"] == 1
    assert equal["eligible"] is True  # Timestamp ties do not prove a lost message.


def test_invalid_and_nonterminal_stale_histories_retain_rejection_reasons() -> None:
    crossed = PriceLevelChanged(
        _MARKET, _TOKEN, Side.BUY, Price("0.53"), NonNegativeQuantity("2"), _at(2)
    )
    invalid = _report(_Reader((_snapshot(), crossed)))
    assert invalid["replay_integrity"] == "verified"
    assert "invalid_observations" in invalid["reasons"]
    stale = _report(
        _Reader((_snapshot(), MarketDataDisconnected("lost", _at(2)), _snapshot(3)))
    )
    assert stale["replay_integrity"] == "verified"
    assert "nonterminal_stale_observations" in stale["reasons"]
    low = _report(_Reader((_trade(1), _trade(2), _snapshot(3))))
    assert "insufficient_synchronized_fraction" in low["reasons"]


def test_read_and_replay_errors_fail_closed_but_cancellation_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _Reader((_snapshot(), _trade()))

    async def broken_read(
        session_id: MarketDataSessionId,
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        raise RuntimeError("raw private diagnostic")

    monkeypatch.setattr(reader, "list_updates", broken_read)
    report = _report(reader)
    assert report["measurements"] is None and report["eligible"] is False
    assert report["reasons"] == ["journal_read_failed"]
    assert "raw private" not in json.dumps(report)

    async def cancelled_read(
        session_id: MarketDataSessionId,
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        raise asyncio.CancelledError

    monkeypatch.setattr(reader, "list_updates", cancelled_read)
    with pytest.raises(asyncio.CancelledError):
        _report(reader)

    async def broken_replay(self: object, session_id: MarketDataSessionId) -> None:
        raise RuntimeError("unavailable implementation")

    monkeypatch.setattr(MarketDataSessionReplayer, "replay", broken_replay)
    report = _report(_Reader((_snapshot(), _trade())))
    assert report["eligible"] is False and report["replay_integrity"] == "unavailable"
    assert "replay_verification_unavailable" in report["reasons"]

    async def cancelled_replay(self: object, session_id: MarketDataSessionId) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(MarketDataSessionReplayer, "replay", cancelled_replay)
    with pytest.raises(asyncio.CancelledError):
        _report(_Reader((_snapshot(), _trade())))


class _Connection(AbstractAsyncContextManager[aiosqlite.Connection]):
    async def __aenter__(self) -> aiosqlite.Connection:
        return cast(aiosqlite.Connection, self)

    async def __aexit__(self, *args: object) -> None:
        return None


def test_cli_gate_emits_json_and_blocks_shell_analysis_for_rejected_session(
    tmp_path: Path,
) -> None:
    database = tmp_path / "synthetic.db"
    database.touch()
    for finished, expected in ((True, 0), (False, 1)):
        reader = _Reader((_snapshot(), _trade()), finished=finished)
        deps = _ShadowDependencies(
            lambda _path: _Connection(),
            lambda _connection: cast(MarketDataJournalGateway, reader),
        )
        output = io.StringIO()
        result = main(
            ["quality", "--database", str(database), "--session-id", _ID.value],
            quality_dependencies=deps,
            stdout=output,
            stderr=io.StringIO(),
        )
        assert result == expected
        assert json.loads(output.getvalue())["eligible"] is finished
    with pytest.raises(SystemExit) as error:
        main(
            [
                "quality",
                "--database",
                str(tmp_path / "absent"),
                "--session-id",
                _ID.value,
            ]
        )
    assert error.value.code == 2
    with pytest.raises(SystemExit) as error:
        main(["quality", "--database", str(database), "--session-id", " "])
    assert error.value.code == 2


def test_cli_connection_failure_cancellation_and_readonly_uri(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(path: Path) -> AbstractAsyncContextManager[aiosqlite.Connection]:
        raise OSError("private diagnostics")

    deps = _ShadowDependencies(
        fail, lambda connection: cast(MarketDataJournalGateway, connection)
    )
    report = asyncio.run(quality(tmp_path, _ID, deps))
    assert report["reasons"] == ["journal_open_failed"]
    assert report["measurements"] is None

    def cancel(path: Path) -> AbstractAsyncContextManager[aiosqlite.Connection]:
        raise asyncio.CancelledError

    deps = replace(deps, sqlite_connection_factory=cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(quality(tmp_path, _ID, deps))

    calls: list[tuple[str, bool]] = []

    def connect(database: str, **kwargs: object) -> aiosqlite.Connection:
        calls.append((database, bool(kwargs["uri"])))
        return cast(aiosqlite.Connection, _Connection())

    monkeypatch.setattr(aiosqlite, "connect", connect)
    cli_module._open_read_only(tmp_path / "journal with space.db")
    assert calls == [((tmp_path / "journal with space.db").as_uri() + "?mode=ro", True)]
