"""Read-only CLI composition for conservative historical maker-fill evidence."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from horus_engine.application import (
    ConservativeFillAnalysis,
    ConservativeFillSimulator,
    MarketDataSessionId,
    ShadowQuoteAnalyzer,
    ShadowQuoteConfig,
)
from horus_engine.domain import Quantity

from .shadow import (
    _DEFAULT_DEPENDENCIES as _DEFAULT_SHADOW_DEPENDENCIES,
)
from .shadow import (
    _ShadowDependencies,
    model_from_name,
)

_DEFAULT_DEPENDENCIES = _DEFAULT_SHADOW_DEPENDENCIES


class SimulateFillsOperationalError(RuntimeError):
    """An expected read-only simulation failure with stable CLI exit code one."""


@dataclass(frozen=True)
class SimulateFillsOptions:
    """Validated input for one existing recorded-session fill simulation."""

    database_path: Path
    session_id: MarketDataSessionId
    model_name: str
    half_spread_ticks: int
    quantity: Quantity
    fills_output_path: Path | None = None

    def __post_init__(self) -> None:
        """Reject invalid local input before opening an existing journal."""
        if (
            not isinstance(self.database_path, Path)
            or not str(self.database_path).strip()
        ):
            raise ValueError("database path must be non-blank")
        if not self.database_path.is_file():
            raise ValueError("database path must be an existing file")
        if not isinstance(self.session_id, MarketDataSessionId):
            raise ValueError("session_id must be a MarketDataSessionId")
        if self.model_name not in {"midpoint", "microprice"}:
            raise ValueError("model must be midpoint or microprice")
        if self.fills_output_path is not None:
            if not isinstance(self.fills_output_path, Path):
                raise ValueError("fills output path must be a Path")
            if self.fills_output_path.exists() and self.fills_output_path.is_dir():
                raise ValueError("fills output path must not be a directory")
        ShadowQuoteConfig(self.half_spread_ticks, self.quantity)


@dataclass(frozen=True)
class SimulateFillsSummary:
    """Compact, Decimal-safe JSON summary that does not make a profit claim."""

    analysis: ConservativeFillAnalysis
    database_path: Path

    def as_json(self) -> str:
        """Encode financial Decimal values as JSON strings, never JSON floats."""
        shadow = self.analysis.shadow_analysis
        replay = shadow.replay
        return json.dumps(
            {
                "session_id": replay.session.session_id.value,
                "market_id": replay.session.market_id.value,
                "token_id": replay.session.token_id.value,
                "fair_value_model": shadow.model_name,
                "half_spread_ticks": shadow.config.half_spread_ticks,
                "quantity": str(shadow.config.quantity.value),
                "total_steps": self.analysis.total_steps,
                "trade_events_observed": self.analysis.trade_events_observed,
                "qualifying_trade_events": self.analysis.qualifying_trade_events,
                "fill_events": self.analysis.fill_events,
                "buy_fill_events": self.analysis.buy_fill_events,
                "sell_fill_events": self.analysis.sell_fill_events,
                "buy_filled_quantity": str(self.analysis.buy_filled_quantity),
                "sell_filled_quantity": str(self.analysis.sell_filled_quantity),
                "buy_notional": str(self.analysis.buy_notional),
                "sell_notional": str(self.analysis.sell_notional),
                "net_token_position": str(self.analysis.net_token_position),
                "net_cash_flow": str(self.analysis.net_cash_flow),
                "fully_filled_buy_orders": self.analysis.fully_filled_buy_orders,
                "fully_filled_sell_orders": self.analysis.fully_filled_sell_orders,
                "remaining_bid_quantity": str(self.analysis.remaining_bid_quantity),
                "remaining_ask_quantity": str(self.analysis.remaining_ask_quantity),
            }
        )


def add_simulate_fills_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the read-only conservative maker-fill simulation command."""
    parser = subparsers.add_parser(
        "simulate-fills",
        help="simulate conservative maker fills over a recorded session",
    )
    parser.add_argument(
        "--database", required=True, help="existing SQLite journal path"
    )
    parser.add_argument("--session-id", required=True, help="recorded session ID")
    parser.add_argument(
        "--model", default="microprice", choices=("midpoint", "microprice")
    )
    parser.add_argument(
        "--half-spread-ticks", default=2, type=int, help="positive quote half-width"
    )
    parser.add_argument("--quantity", default="10", help="strictly positive quote size")
    parser.add_argument(
        "--fills-output", help="optional JSONL path for evidence-backed fill events"
    )


def options_from_namespace(namespace: argparse.Namespace) -> SimulateFillsOptions:
    """Convert argparse values to exact typed input without filesystem writes."""
    database_text = namespace.database
    if not isinstance(database_text, str) or not database_text.strip():
        raise ValueError("database path must be non-blank")
    try:
        return SimulateFillsOptions(
            Path(database_text),
            MarketDataSessionId(namespace.session_id),
            namespace.model,
            namespace.half_spread_ticks,
            Quantity(namespace.quantity),
            None if namespace.fills_output is None else Path(namespace.fills_output),
        )
    except ValueError as error:
        raise ValueError(str(error)) from error


def parse_simulate_fills_options(
    parser: argparse.ArgumentParser, argv: list[str] | None
) -> SimulateFillsOptions:
    """Parse command arguments and delegate invalid input handling to argparse."""
    namespace = parser.parse_args(argv)
    try:
        return options_from_namespace(namespace)
    except ValueError as error:
        parser.error(str(error))


def write_fills_jsonl(path: Path, analysis: ConservativeFillAnalysis) -> None:
    """Atomically write ordered fill evidence, creating an absent parent directory."""
    temporary_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            for fill in analysis.fills:
                temporary_file.write(json.dumps(_fill_record(fill)))
                temporary_file.write("\n")
        temporary_path.replace(path)
    except OSError as error:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise SimulateFillsOperationalError("could not write fills output") from error


def _fill_record(fill: object) -> dict[str, str | int]:
    """Translate one typed fill to its intentionally small stable JSONL schema."""
    from horus_engine.application import SimulatedMakerFill

    if not isinstance(fill, SimulatedMakerFill):
        raise TypeError("fill must be SimulatedMakerFill")
    return {
        "sequence_number": fill.sequence_number,
        "observed_at": fill.observed_at.isoformat(),
        "order_side": fill.order_side.value,
        "quote_price": str(fill.quote_price.value),
        "fill_quantity": str(fill.fill_quantity.value),
        "aggressor_side": fill.aggressor_side.value,
        "observed_trade_price": str(fill.observed_trade_price.value),
        "observed_trade_quantity": str(fill.observed_trade_quantity.value),
        "trigger": fill.trigger.value,
        "queue_ahead_before": str(fill.queue_ahead_before.value),
        "queue_ahead_after": str(fill.queue_ahead_after.value),
        "order_remaining_after": str(fill.order_remaining_after.value),
    }


async def simulate_fills(
    options: SimulateFillsOptions,
    dependencies: _ShadowDependencies = _DEFAULT_DEPENDENCIES,
    stderr: TextIO = sys.stderr,
) -> SimulateFillsSummary:
    """Replay, quote, and simulate one journal without modifying it or using network."""
    try:
        async with dependencies.sqlite_connection_factory(
            options.database_path
        ) as connection:
            journal = dependencies.journal_factory(connection)
            from horus_engine.application import MarketDataSessionReplayer

            replay = await MarketDataSessionReplayer(journal).replay(options.session_id)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        raise SimulateFillsOperationalError(
            "could not replay recorded session"
        ) from error
    try:
        shadow = ShadowQuoteAnalyzer(
            model_from_name(options.model_name),
            ShadowQuoteConfig(options.half_spread_ticks, options.quantity),
        ).analyze(replay)
        analysis = ConservativeFillSimulator().simulate(shadow)
    except Exception as error:
        raise SimulateFillsOperationalError(
            "could not simulate conservative fills"
        ) from error
    if options.fills_output_path is not None:
        write_fills_jsonl(options.fills_output_path, analysis)
    print(f"simulating fills for session {options.session_id.value}", file=stderr)
    return SimulateFillsSummary(analysis, options.database_path)
