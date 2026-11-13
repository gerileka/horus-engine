"""Fixed assumption comparisons over the same quality-gated frozen evidence."""

import asyncio
from dataclasses import asdict, dataclass
from decimal import Decimal

from .conservative_fills import ConservativeFillSimulator, FillAssumptions
from .fair_value import MidpointFairValueModel
from .market_data_journal import (
    MarketDataJournalReader,
    MarketDataSessionId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
)
from .market_data_replay import MarketDataSessionReplayer
from .session_quality import assess_session_quality, unavailable_quality_report
from .shadow_quotes import ShadowQuoteAnalyzer, ShadowQuoteConfig


@dataclass(frozen=True)
class _Evidence:
    session: PersistedMarketDataSession | None
    updates: tuple[PersistedMarketDataUpdate, ...]

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession | None:
        return self.session

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        return self.updates


async def compare_fill_scenarios(
    journal: MarketDataJournalReader,
    session_id: MarketDataSessionId,
    config: ShadowQuoteConfig,
) -> dict[str, object]:
    """Hold midpoint quotes fixed across named, uncalibrated execution stresses.

    No scenario runs on rejected or unavailable evidence. Financial aggregates
    and queue assumptions are JSON decimal strings. No profit is estimated.
    """
    try:
        evidence = _Evidence(
            await journal.get_session(session_id),
            await journal.list_updates(session_id),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        return {
            "quality": unavailable_quality_report(session_id, "journal_read_failed"),
            "scenarios": None,
        }
    quality = await assess_session_quality(evidence, session_id)
    report: dict[str, object] = {"quality": quality, "scenarios": None}
    if quality["eligible"] is not True:
        return report
    replay = await MarketDataSessionReplayer(evidence).replay(session_id)
    shadow = ShadowQuoteAnalyzer(MidpointFairValueModel(), config).analyze(replay)
    policies = {
        "legacy_baseline": FillAssumptions(),
        "exact_price_only": FillAssumptions(allow_trade_through=False),
        "double_displayed_queue": FillAssumptions(queue_multiplier=Decimal("2")),
        "one_event_activation_delay": FillAssumptions(activation_delay_events=1),
        "repeated_trade_abstention": FillAssumptions(suppress_repeated_trades=True),
        "snapshot_abstention": FillAssumptions(suspend_on_snapshot=True),
        "combined_stress": FillAssumptions(Decimal("2"), 1, False, True, True),
    }
    scenarios = []
    for name, policy in policies.items():
        result = ConservativeFillSimulator(policy).simulate(shadow)
        assumptions = asdict(policy)
        assumptions["queue_multiplier"] = str(policy.queue_multiplier)
        scenarios.append(
            {
                "name": name,
                "assumptions": assumptions,
                "fill_events": result.fill_events,
                "buy_quantity": str(result.buy_filled_quantity),
                "sell_quantity": str(result.sell_filled_quantity),
                "buy_notional": str(result.buy_notional),
                "sell_notional": str(result.sell_notional),
                "net_position": str(result.net_token_position),
                "net_cash_flow_not_profit": str(result.net_cash_flow),
            }
        )
    report.update(
        scenarios=scenarios,
        quote_config={
            "model": shadow.model_name,
            "half_spread_ticks": config.half_spread_ticks,
            "quantity": str(config.quantity.value),
        },
        limitations=[
            "Uncalibrated scenarios, not fill bounds or probabilities.",
            "Whole-event delay is not measured wall-clock or cancellation latency.",
            "Repeated payload abstention can discard distinct legitimate trades; "
            "there is no persisted venue trade ID.",
            "Snapshot abstention suspends existing quotes until PLACE/REPLACE; "
            "it does not recover a real queue position.",
            "Quality eligibility does not prove venue completeness or execution.",
            "No acknowledgement, hidden liquidity, fees or inventory valuation.",
        ],
    )
    return report
