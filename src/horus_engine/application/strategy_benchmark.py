"""Predeclared, non-optimizing benchmark over frozen, quality-gated sessions."""

import asyncio
import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal

from horus_engine.domain import Quantity, Side

from .conservative_fills import (
    ConservativeFillAnalysis,
    ConservativeFillSimulator,
    FillAssumptions,
)
from .fair_value import MidpointFairValueModel, TopLevelMicropriceFairValueModel
from .fill_sensitivity import _Evidence
from .market_data_journal import MarketDataJournalReader, MarketDataSessionId
from .market_data_replay import MarketDataReplayResult, MarketDataSessionReplayer
from .order_book_state import LocalBookStatus
from .session_quality import assess_session_quality
from .shadow_quotes import ShadowQuoteAnalyzer, ShadowQuoteConfig


def benchmark_plan() -> dict[str, object]:
    """Return a fresh fixed protocol; no fitted parameters or winner selection."""
    return {
        "version": "horus-benchmark-v1",
        "models": ["midpoint", "microprice", "no_quote"],
        "half_spread_ticks": [1, 2],
        "quantity": "2",
        "execution": ["legacy_baseline", "exact_price_only", "combined_stress"],
        "split": (
            "sort all sessions by started_at; floor(n/3) train, "
            "floor(n/3) validation, remainder held_out; reject overlaps"
        ),
        "markout_seconds": [1, 5],
        "max_mark_age_or_delay_seconds": 1,
        "cost_bps_hypotheses": ["0", "10", "-1"],
        "cost_status": (
            "2026-11-16 Polymarket maker schedule/rebates unverified; not venue rates"
        ),
        "fee_source_attempt": (
            "https://docs.polymarket.com/trading/fees (CERT_HAS_EXPIRED)"
        ),
        "initial_inventory": (
            "0 per session; no cross-session capital or settlement model"
        ),
        "selection": "none; all variants reported separately, no held-out tuning",
    }


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, default=str, sort_keys=True).encode()
    ).hexdigest()


def _midpoint(replay: MarketDataReplayResult, index: int) -> Decimal | None:
    view = replay.steps[index].view
    if view.status is not LocalBookStatus.SYNCHRONIZED or view.book is None:
        return None
    price = MidpointFairValueModel().estimate(view.book)
    return None if price is None else price.value


def _seconds(start: datetime, end: datetime) -> Decimal:
    delta = end - start
    return Decimal(delta.days * 86400 + delta.seconds) + Decimal(
        delta.microseconds
    ) / Decimal(1000000)


def _future_mark(replay: MarketDataReplayResult, at: datetime) -> Decimal | None:
    # First observation at/after target, within one second. Never cross sessions.
    for index, step in enumerate(replay.steps):
        if step.event.observed_at >= at:
            if step.event.observed_at > at + timedelta(seconds=1):
                return None
            return _midpoint(replay, index)
    return None


def _metrics(result: ConservativeFillAnalysis) -> dict[str, object]:
    replay = result.shadow_analysis.replay
    position = peak = exposure = Decimal(0)
    for index, step in enumerate(result.steps):
        for fill in step.fills:
            sign = Decimal(1) if fill.order_side is Side.BUY else Decimal(-1)
            position += sign * fill.fill_quantity.value
        peak = max(peak, abs(position))
        if index + 1 < len(result.steps):
            exposure += abs(position) * _seconds(
                step.decision.replay_step.event.observed_at,
                replay.steps[index + 1].event.observed_at,
            )
    last_time = replay.steps[-1].event.observed_at
    finished = replay.session.finished_at
    assert finished is not None
    exposure += abs(position) * _seconds(last_time, finished)
    terminal = _midpoint(replay, len(replay.steps) - 1)
    if _seconds(last_time, finished) > 1:
        terminal = None
    inventory_value = None if terminal is None else position * terminal
    turnover = result.buy_notional + result.sell_notional
    markouts = []
    for horizon in (1, 5):
        weighted = quantity = Decimal(0)
        missing = 0
        for fill in result.fills:
            mark = _future_mark(replay, fill.observed_at + timedelta(seconds=horizon))
            if mark is None:
                missing += 1
                continue
            sign = Decimal(1) if fill.order_side is Side.BUY else Decimal(-1)
            weighted += (
                sign * (mark - fill.quote_price.value) * fill.fill_quantity.value
            )
            quantity += fill.fill_quantity.value
        markouts.append(
            {
                "horizon_seconds": horizon,
                "available_fills": result.fill_events - missing,
                "unavailable_fills": missing,
                "quantity_weighted_mean": None
                if quantity == 0
                else str(weighted / quantity),
                "signed_value_sum": None if quantity == 0 else str(weighted),
            }
        )
    sensitivities = []
    for bps in (Decimal(0), Decimal(10), Decimal(-1)):
        cost = turnover * bps / Decimal(10000)
        sensitivities.append(
            {
                "assumed_net_cost_bps": str(bps),
                "hypothetical_cost": str(cost),
                "cash_after_cost_not_profit": str(result.net_cash_flow - cost),
                "marked_value_after_cost_not_settlement": None
                if inventory_value is None
                else str(result.net_cash_flow - cost + inventory_value),
            }
        )
    return {
        "quoteable_observation_fraction": str(result.shadow_analysis.quoteable_ratio),
        "fills": result.fill_events,
        "turnover_notional": str(turnover),
        "buy_quantity": str(result.buy_filled_quantity),
        "sell_quantity": str(result.sell_filled_quantity),
        "net_position": str(position),
        "peak_absolute_position": str(peak),
        "absolute_position_seconds": str(exposure),
        "terminal_midpoint": None if terminal is None else str(terminal),
        "terminal_inventory_marked_value": None
        if inventory_value is None
        else str(inventory_value),
        "hypothetical_cash_flow_not_profit": str(result.net_cash_flow),
        "markouts": markouts,
        "cost_sensitivity": sensitivities,
    }


def _variants(replay: MarketDataReplayResult) -> list[dict[str, object]]:
    variants = []
    for model in (MidpointFairValueModel(), TopLevelMicropriceFairValueModel()):
        for width in (1, 2):
            shadow = ShadowQuoteAnalyzer(
                model, ShadowQuoteConfig(width, Quantity("2"))
            ).analyze(replay)
            for name, assumptions in (
                ("legacy_baseline", FillAssumptions()),
                ("exact_price_only", FillAssumptions(allow_trade_through=False)),
                ("combined_stress", FillAssumptions(Decimal(2), 1, False, True, True)),
            ):
                result = ConservativeFillSimulator(assumptions).simulate(shadow)
                variants.append(
                    {
                        "model": model.name,
                        "half_spread_ticks": width,
                        "execution": name,
                        **_metrics(result),
                    }
                )
    variants.append(
        {
            "model": "no_quote",
            "fills": 0,
            "turnover_notional": "0",
            "net_position": "0",
            "hypothetical_cash_flow_not_profit": "0",
            "marked_value_after_cost_not_settlement": "0",
            "quoteable_observation_fraction": "0",
            "peak_absolute_position": "0",
            "absolute_position_seconds": "0",
            "markouts": None,
        }
    )
    return variants


async def run_benchmark(
    journal: MarketDataJournalReader, session_ids: tuple[MarketDataSessionId, ...]
) -> dict[str, object]:
    """Freeze the full manifest before splitting/filtering; never select a winner.

    Unknown/read-failed headers invalidate the split, rather than silently
    dropping a session. Quality-rejected sessions retain their original split.
    """
    plan = benchmark_plan()
    report: dict[str, object] = {
        "plan": plan,
        "plan_sha256": _digest(plan),
        "sessions": [],
        "ready": False,
        "verdict": "insufficient_evidence_no_edge_claim",
    }
    if len(session_ids) < 3 or len(set(session_ids)) != len(session_ids):
        return {**report, "reason": "need_at_least_three_distinct_sessions"}
    evidence = []
    for sid in session_ids:
        try:
            frozen = _Evidence(
                await journal.get_session(sid), await journal.list_updates(sid)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return {**report, "reason": "journal_read_failed", "session_id": str(sid)}
        if (
            frozen.session is None
            or frozen.session.finished_at is None
            or frozen.session.session_id != sid
        ):
            return {
                **report,
                "reason": "missing_or_unfinished_session",
                "session_id": str(sid),
            }
        evidence.append(frozen)
    evidence.sort(
        key=lambda e: (
            (e.session.started_at, str(e.session.session_id)) if e.session else ()
        )
    )
    for before, after in zip(evidence, evidence[1:]):
        assert before.session is not None and after.session is not None
        assert before.session.finished_at is not None
        if before.session.finished_at >= after.session.started_at:
            return {**report, "reason": "overlapping_sessions"}
    rows = []
    third = len(evidence) // 3
    for index, frozen in enumerate(evidence):
        session = frozen.session
        assert session is not None
        split = (
            "train"
            if index < third
            else "validation"
            if index < third * 2
            else "held_out"
        )
        quality = await assess_session_quality(frozen, session.session_id)
        row: dict[str, object] = {
            "session_id": str(session.session_id),
            "split": split,
            "started_at": session.started_at.isoformat(),
            "finished_at": str(session.finished_at),
            "evidence_sha256": _digest(
                [asdict(session), [asdict(u) for u in frozen.updates]]
            ),
            "quality": quality,
            "variants": None,
        }
        if quality["eligible"] is True:
            row["variants"] = _variants(
                await MarketDataSessionReplayer(frozen).replay(session.session_id)
            )
        rows.append(row)
    usable = sum(row["variants"] is not None for row in rows)
    report.update(
        sessions=rows,
        eligible_sessions=usable,
        unusable_sessions=len(rows) - usable,
        ready=usable == len(rows),
        uncertainty=(
            "Session-level descriptive results only; small/dependent "
            "samples, no confidence interval or significance claim. All "
            "arms retained; collect independent real held-out sessions."
        ),
        settlement=(
            "None: midpoint marking is not liquidation, collateral "
            "feasibility, realized P&L or settlement. Short positions "
            "unconstrained; zero initial inventory per session."
        ),
    )
    return report
