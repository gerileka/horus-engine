"""Independent money/time oracles, split leakage and fail-closed evidence."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from typing import Any, cast

import pytest

from examples.strategy_benchmark import SyntheticJournal, main
from horus_engine.application import MarketDataSessionId
from horus_engine.application.conservative_fills import ConservativeFillSimulator
from horus_engine.application.fair_value import MidpointFairValueModel
from horus_engine.application.market_data_journal import MarketDataJournalReader
from horus_engine.application.market_data_replay import MarketDataSessionReplayer
from horus_engine.application.order_book_state import LocalBookStatus
from horus_engine.application.shadow_quotes import (
    ShadowQuoteAnalyzer,
    ShadowQuoteConfig,
)
from horus_engine.application.strategy_benchmark import (
    _future_mark,
    _metrics,
    _midpoint,
    _seconds,
    benchmark_plan,
    run_benchmark,
)
from horus_engine.domain import Quantity


def run(journal: SyntheticJournal) -> dict[str, Any]:
    return asyncio.run(run_benchmark(journal, tuple(reversed(journal.evidence))))


def test_hand_calculated_inventory_cost_markout_and_manifest() -> None:
    journal = SyntheticJournal()
    report = run(journal)
    assert report == run(journal)
    assert report["ready"] is True
    rows = report["sessions"]
    assert [r["split"] for r in rows] == ["train", "validation", "held_out"]
    assert len(rows[0]["variants"]) == 13
    result = rows[0]["variants"][0]
    # -1*.49+2*.51=.53 cash; (1-2)*.50=-.50 inventory; .03 marked.
    assert Decimal(result["hypothetical_cash_flow_not_profit"]) == Decimal(".53")
    assert result["net_position"] == "-1"
    assert Decimal(result["terminal_inventory_marked_value"]) == Decimal("-.50")
    assert result["turnover_notional"] == "1.51"
    assert result["peak_absolute_position"] == "1"
    # |+1| from second1 to2, |-1| from second2 to7 = 6 token seconds.
    assert result["absolute_position_seconds"] == "6"
    assert result["cost_sensitivity"][1]["hypothetical_cost"] == "0.00151"
    assert (
        result["cost_sensitivity"][1]["marked_value_after_cost_not_settlement"]
        == "0.02849"
    )
    assert (
        result["cost_sensitivity"][2]["marked_value_after_cost_not_settlement"]
        == "0.030151"
    )
    assert result["markouts"][0]["available_fills"] == 1
    assert result["markouts"][0]["unavailable_fills"] == 1
    assert result["markouts"][1]["signed_value_sum"] == "0.03"
    assert result["markouts"][1]["quantity_weighted_mean"] == "0.01"
    assert rows[0]["variants"][-1]["model"] == "no_quote"
    assert rows[0]["variants"][-1]["marked_value_after_cost_not_settlement"] == "0"
    assert rows[0]["variants"][2]["fills"] == 0
    # Changing held-out prices cannot change earlier evidence or their arms.
    held = tuple(journal.evidence)[-1]
    original = journal.evidence[held]
    journal.evidence[held] = replace(original, updates=original.updates[:-1])
    changed = run(journal)
    assert changed["sessions"][:2] == rows[:2]
    assert changed["sessions"][-1]["split"] == "held_out"
    assert changed["sessions"][-1]["variants"] is None
    assert changed["unusable_sessions"] == 1 and changed["ready"] is False
    assert changed["plan_sha256"] == report["plan_sha256"]
    fresh = benchmark_plan()
    fresh["version"] = "tampered"
    assert benchmark_plan()["version"] == "horus-benchmark-v1"


@pytest.mark.parametrize(
    "kind", ["missing", "unfinished", "wrong_id", "overlap", "too_few", "duplicate"]
)
def test_invalid_manifest_is_not_silently_filtered(kind: str) -> None:
    journal = SyntheticJournal()
    ids = tuple(journal.evidence)
    first = journal.evidence[ids[0]]
    assert first.session is not None
    if kind == "missing":
        journal.evidence[ids[0]] = replace(first, session=None)
    elif kind == "unfinished":
        journal.evidence[ids[0]] = replace(
            first,
            session=replace(
                first.session,
                finished_at=None,
                final_status=None,
                final_tick_size=None,
                final_last_observed_at=None,
                final_reason=None,
            ),
        )
    elif kind == "wrong_id":
        journal.evidence[ids[0]] = replace(
            first, session=replace(first.session, session_id=ids[1])
        )
    elif kind == "overlap":
        journal.evidence[ids[0]] = replace(
            first,
            session=replace(
                first.session,
                finished_at=first.session.started_at + timedelta(minutes=1),
            ),
        )
    elif kind == "too_few":
        ids = ids[:2]
    else:
        ids = (ids[0], ids[0], ids[1])
    report = asyncio.run(run_benchmark(journal, ids))
    assert report["ready"] is False and report["sessions"] == []
    assert "reason" in report


def test_read_errors_and_cancellation() -> None:
    class Broken:
        async def get_session(self, sid: MarketDataSessionId) -> None:
            raise RuntimeError("private content must not leak")

    journal = SyntheticJournal()
    ids = tuple(journal.evidence)
    broken = cast(MarketDataJournalReader, Broken())
    report = asyncio.run(run_benchmark(broken, ids))
    assert report["reason"] == "journal_read_failed"
    assert "private content" not in str(report)

    class Cancelled:
        async def get_session(self, sid: MarketDataSessionId) -> None:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(run_benchmark(cast(MarketDataJournalReader, Cancelled()), ids))


def test_unavailable_marks_and_terminal_staleness() -> None:
    journal = SyntheticJournal()
    evidence = next(iter(journal.evidence.values()))
    assert evidence.session is not None
    replay = asyncio.run(
        MarketDataSessionReplayer(evidence).replay(evidence.session.session_id)
    )
    step = replay.steps[0]
    assert (
        _future_mark(replay, replay.steps[-1].event.observed_at + timedelta(seconds=1))
        is None
    )
    assert (
        _midpoint(
            replace(
                replay,
                steps=(
                    replace(
                        step, view=replace(step.view, status=LocalBookStatus.STALE)
                    ),
                ),
            ),
            0,
        )
        is None
    )
    assert (
        _midpoint(
            replace(replay, steps=(replace(step, view=replace(step.view, book=None)),)),
            0,
        )
        is None
    )
    assert step.view.book is not None
    from horus_engine.domain import OrderBook

    assert (
        _midpoint(
            replace(
                replay,
                steps=(replace(step, view=replace(step.view, book=OrderBook((), ()))),),
            ),
            0,
        )
        is None
    )
    # A finalized header may legitimately lag the last update; mark then absent.
    assert evidence.session.finished_at is not None
    stale = replace(
        replay,
        session=replace(
            evidence.session,
            finished_at=evidence.session.finished_at + timedelta(seconds=2),
        ),
    )
    result = ConservativeFillSimulator().simulate(
        ShadowQuoteAnalyzer(
            MidpointFairValueModel(), ShadowQuoteConfig(1, Quantity("2"))
        ).analyze(stale)
    )
    metrics = _metrics(result)
    assert metrics["terminal_inventory_marked_value"] is None
    costs = cast(list[dict[str, object]], metrics["cost_sensitivity"])
    assert costs[0]["marked_value_after_cost_not_settlement"] is None
    assert _seconds(
        step.event.observed_at, step.event.observed_at + timedelta(microseconds=1)
    ) == Decimal(".000001")


def test_negative_adverse_selection_not_clipped() -> None:
    journal = SyntheticJournal()
    evidence = next(iter(journal.evidence.values()))
    assert evidence.session is not None
    replay = asyncio.run(
        MarketDataSessionReplayer(evidence).replay(evidence.session.session_id)
    )
    result = ConservativeFillSimulator().simulate(
        ShadowQuoteAnalyzer(
            MidpointFairValueModel(), ShadowQuoteConfig(1, Quantity("2"))
        ).analyze(replay)
    )
    # Keep only the first buy, then mark future book at .40: -.09 per token.
    from horus_engine.domain import OrderBook, OrderBookLevel, Price

    book = OrderBook(
        (OrderBookLevel(Price(".38"), Quantity("2")),),
        (OrderBookLevel(Price(".42"), Quantity("2")),),
    )
    altered = replace(
        replay,
        steps=tuple(replace(s, view=replace(s.view, book=book)) for s in replay.steps),
    )
    # Metrics use immutable fill evidence and changed marks, never changed quotes.
    altered_result = replace(
        result, shadow_analysis=replace(result.shadow_analysis, replay=altered)
    )
    marks = cast(list[dict[str, Any]], _metrics(altered_result)["markouts"])
    assert marks[0]["quantity_weighted_mean"] == "-0.09"


def test_cli_synthetic_and_validation(capsys: pytest.CaptureFixture[str]) -> None:
    import json

    assert main(["--synthetic"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["data_kind"] == "synthetic_only_no_empirical_edge_evidence"
    with pytest.raises(SystemExit):
        main([])
    with pytest.raises(SystemExit):
        main(["--synthetic", "--database", "not-opened.db"])
