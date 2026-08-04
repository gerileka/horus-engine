"""Read-only CLI composition for deterministic shadow-quote experiments."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

import aiosqlite

from horus_engine.application import (
    FairValueModel,
    MarketDataJournalGateway,
    MarketDataSessionId,
    MarketDataSessionReplayer,
    MidpointFairValueModel,
    ShadowQuoteAnalysis,
    ShadowQuoteAnalyzer,
    ShadowQuoteConfig,
    ShadowQuoteDecision,
    TopLevelMicropriceFairValueModel,
)
from horus_engine.domain import Quantity
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


class ShadowOperationalError(RuntimeError):
    """An expected read-only experiment failure that should return exit code one."""


@dataclass(frozen=True)
class ShadowOptions:
    """Validated CLI input for one persisted-session shadow experiment."""

    database_path: Path
    session_id: MarketDataSessionId
    model_name: str
    half_spread_ticks: int
    quantity: Quantity
    decisions_output_path: Path | None = None

    def __post_init__(self) -> None:
        """Reject invalid local input before a journal connection is opened."""
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
        if self.decisions_output_path is not None:
            if not isinstance(self.decisions_output_path, Path):
                raise ValueError("decisions output path must be a Path")
            if not self.decisions_output_path.parent.is_dir():
                raise ValueError("decisions output directory must exist")
            if (
                self.decisions_output_path.exists()
                and self.decisions_output_path.is_dir()
            ):
                raise ValueError("decisions output path must not be a directory")
        ShadowQuoteConfig(self.half_spread_ticks, self.quantity)


@dataclass(frozen=True)
class ShadowSummary:
    """Serializable compact experiment output for one recorded session."""

    analysis: ShadowQuoteAnalysis
    database_path: Path

    def as_json(self) -> str:
        """Encode decimal values as strings without introducing JSON floats."""
        replay = self.analysis.replay
        return json.dumps(
            {
                "session_id": replay.session.session_id.value,
                "market_id": replay.session.market_id.value,
                "token_id": replay.session.token_id.value,
                "database": str(self.database_path),
                "fair_value_model": self.analysis.model_name,
                "half_spread_ticks": self.analysis.config.half_spread_ticks,
                "quantity": str(self.analysis.config.quantity.value),
                "total_steps": self.analysis.total_steps,
                "quoteable_steps": self.analysis.quoteable_steps,
                "suppressed_steps": self.analysis.suppressed_steps,
                "quoteable_ratio": str(self.analysis.quoteable_ratio),
                "placements": self.analysis.placements,
                "replacements": self.analysis.replacements,
                "holds": self.analysis.holds,
                "cancellations": self.analysis.cancellations,
                "none_actions": self.analysis.none_actions,
            }
        )


SQLiteConnectionFactory = Callable[
    [Path], AbstractAsyncContextManager[aiosqlite.Connection]
]
JournalFactory = Callable[[aiosqlite.Connection], MarketDataJournalGateway]


@dataclass(frozen=True)
class _ShadowDependencies:
    """Small CLI-only factory seam for deterministic composition tests."""

    sqlite_connection_factory: SQLiteConnectionFactory
    journal_factory: JournalFactory


def _open_sqlite_connection(path: Path) -> aiosqlite.Connection:
    """Open an existing caller-selected journal without initializing its schema."""
    return aiosqlite.connect(path)


_DEFAULT_DEPENDENCIES = _ShadowDependencies(
    sqlite_connection_factory=_open_sqlite_connection,
    journal_factory=SQLiteMarketDataJournal,
)


def add_shadow_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the read-only recorded-session shadow-quote command."""
    parser = subparsers.add_parser(
        "shadow", help="analyze hypothetical maker quotes over a recorded session"
    )
    parser.add_argument(
        "--database", default="data/horus.db", help="SQLite journal path"
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
        "--decisions-output",
        help="optional JSONL path for one complete record per replay decision",
    )


def options_from_namespace(namespace: argparse.Namespace) -> ShadowOptions:
    """Convert argparse values to domain-validated shadow experiment options."""
    database_text = namespace.database
    if not isinstance(database_text, str) or not database_text.strip():
        raise ValueError("database path must be non-blank")
    try:
        return ShadowOptions(
            Path(database_text),
            MarketDataSessionId(namespace.session_id),
            namespace.model,
            namespace.half_spread_ticks,
            Quantity(namespace.quantity),
            (
                None
                if namespace.decisions_output is None
                else Path(namespace.decisions_output)
            ),
        )
    except ValueError as error:
        raise ValueError(str(error)) from error


def parse_shadow_options(
    parser: argparse.ArgumentParser, argv: list[str] | None
) -> ShadowOptions:
    """Parse one shadow command and delegate invalid input handling to argparse."""
    namespace = parser.parse_args(argv)
    try:
        return options_from_namespace(namespace)
    except ValueError as error:
        parser.error(str(error))


def model_from_name(name: str) -> FairValueModel:
    """Construct one of the two deliberately supported fair-value models."""
    if name == "midpoint":
        return MidpointFairValueModel()
    if name == "microprice":
        return TopLevelMicropriceFairValueModel()
    raise ValueError("model must be midpoint or microprice")


def write_decisions_jsonl(
    path: Path, decisions: tuple[ShadowQuoteDecision, ...]
) -> None:
    """Atomically write detailed deterministic decisions outside compact stdout."""
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            for decision in decisions:
                temporary_file.write(json.dumps(_decision_record(decision)))
                temporary_file.write("\n")
        temporary_path.replace(path)
    except OSError as error:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise ShadowOperationalError("could not write decisions output") from error


def _decision_record(decision: ShadowQuoteDecision) -> dict[str, object]:
    """Translate one decision to the stable JSONL schema without Decimal floats."""
    quote = decision.desired_quote
    return {
        "sequence_number": decision.replay_step.sequence_number,
        "action": decision.action.value,
        "suppression_reason": (
            None
            if decision.suppression_reason is None
            else decision.suppression_reason.value
        ),
        "status": decision.replay_step.view.status.name,
        "fair_value": None
        if decision.fair_value is None
        else str(decision.fair_value.value),
        "desired_bid": None if quote is None else str(quote.bid.value),
        "desired_ask": None if quote is None else str(quote.ask.value),
        "quantity": None if quote is None else str(quote.quantity.value),
        "best_bid": (
            None
            if decision.replay_step.view.book is None
            or decision.replay_step.view.book.best_bid is None
            else str(decision.replay_step.view.book.best_bid.value)
        ),
        "best_ask": (
            None
            if decision.replay_step.view.book is None
            or decision.replay_step.view.book.best_ask is None
            else str(decision.replay_step.view.book.best_ask.value)
        ),
    }


async def shadow(
    options: ShadowOptions,
    dependencies: _ShadowDependencies = _DEFAULT_DEPENDENCIES,
    stderr: TextIO = sys.stderr,
) -> ShadowSummary:
    """Replay an existing journal session and analyze it without writing state."""
    try:
        async with dependencies.sqlite_connection_factory(
            options.database_path
        ) as connection:
            journal = dependencies.journal_factory(connection)
            replay = await MarketDataSessionReplayer(journal).replay(options.session_id)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        raise ShadowOperationalError("could not replay recorded session") from error
    analysis = ShadowQuoteAnalyzer(
        model_from_name(options.model_name),
        ShadowQuoteConfig(options.half_spread_ticks, options.quantity),
    ).analyze(replay)
    if options.decisions_output_path is not None:
        write_decisions_jsonl(options.decisions_output_path, analysis.decisions)
    print(
        f"analyzing session {options.session_id.value} with {analysis.model_name}",
        file=stderr,
    )
    return ShadowSummary(analysis, options.database_path)
