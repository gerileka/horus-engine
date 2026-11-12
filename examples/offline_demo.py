"""Collect synthetic events with the real CLI, then replay twice without writes."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite
import httpx

from horus_engine.application import (
    BookSnapshotReceived,
    Market,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataSessionId,
    MarketId,
    MarketStatus,
    TokenId,
    TradeObserved,
)
from horus_engine.cli.collect import _CollectionDependencies
from horus_engine.cli.main import main as horus
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)
from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal

_MARKET = MarketId("synthetic-demo-market")
_TOKEN = TokenId("synthetic-demo-yes")
_START = datetime(2026, 1, 1, tzinfo=UTC)
_SCENARIOS = ("fills", "no-fill")


class _SyntheticCatalog:
    async def get_market(self, market_id: MarketId) -> Market | None:
        if market_id != _MARKET:
            return None
        return Market(
            _MARKET,
            "SYNTHETIC offline demonstration; not venue evidence",
            _TOKEN,
            TokenId("synthetic-demo-no"),
            TickSize("0.01"),
            Quantity("1"),
            MarketStatus.ACTIVE,
        )


def _events(scenario: str) -> tuple[MarketDataEvent, ...]:
    def trade(second: int, side: Side, price: str, size: str) -> TradeObserved:
        return TradeObserved(
            _MARKET,
            _TOKEN,
            side,
            Price(price),
            Quantity(size),
            _START + timedelta(seconds=second),
        )

    snapshot = BookSnapshotReceived(
        _MARKET,
        _TOKEN,
        OrderBook(
            (OrderBookLevel(Price("0.48"), Quantity("2")),),
            (OrderBookLevel(Price("0.52"), Quantity("3")),),
        ),
        _START + timedelta(seconds=2),
    )
    disconnect = MarketDataDisconnected(
        "synthetic disconnect", _START + timedelta(seconds=5)
    )
    if scenario == "fills":
        return (
            snapshot,
            trade(3, Side.SELL, "0.49", "1.25"),
            trade(4, Side.BUY, "0.52", "1"),
            disconnect,
        )
    return (
        trade(1, Side.SELL, "0.49", "2"),  # No snapshot: no active quote.
        snapshot,
        trade(3, Side.SELL, "0.50", "1"),  # Does not reach our 0.49 bid.
        disconnect,
    )


class _SyntheticStream:
    def __init__(self, scenario: str) -> None:
        self._scenario = scenario

    async def stream_market_data(
        self, market_id: MarketId, token_ids: tuple[TokenId, ...]
    ) -> AsyncIterator[MarketDataEvent]:
        if market_id != _MARKET or token_ids != (_TOKEN,):
            raise ValueError("synthetic identity mismatch")
        for event in _events(self._scenario):
            yield event


def _reject_http(request: httpx.Request) -> httpx.Response:
    raise RuntimeError("network requests are forbidden in the offline demo")


def _dependencies(scenario: str) -> _CollectionDependencies:
    timestamps = iter((_START, _START + timedelta(seconds=6)))
    return _CollectionDependencies(
        http_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(_reject_http), trust_env=False
        ),
        sqlite_connection_factory=aiosqlite.connect,
        catalog_factory=lambda _client: _SyntheticCatalog(),
        stream_gateway_factory=lambda: _SyntheticStream(scenario),
        journal_factory=SQLiteMarketDataJournal,
        clock=lambda: next(timestamps),
        session_id_factory=lambda: MarketDataSessionId("synthetic-unused"),
    )


def _invoke(
    arguments: list[str], dependencies: _CollectionDependencies | None = None
) -> dict[str, object]:
    stdout, stderr = io.StringIO(), io.StringIO()
    if dependencies is None:
        result = horus(arguments, stdout=stdout, stderr=stderr)
    else:
        result = horus(
            arguments, dependencies=dependencies, stdout=stdout, stderr=stderr
        )
    if result != 0:
        raise RuntimeError(f"{arguments[0]} failed: {stderr.getvalue().strip()}")
    payload: dict[str, object] = json.loads(stdout.getvalue())
    # Only filesystem location is normalized; preserve every analytical value.
    if "database" in payload:
        payload["database"] = "<synthetic-journal>"
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n")


def run_demo(output: Path) -> dict[str, object]:
    """Create a new synthetic journal and verify repeatable, nonmutating analysis.

    Refuse an existing output directory, including an empty one. Partial results
    are retained for diagnosis if a command or invariant fails.
    """
    output.mkdir(parents=True, exist_ok=False)
    database = output / "synthetic-journal.db"
    for scenario in _SCENARIOS:
        summary = _invoke(
            [
                "collect",
                "--market-id",
                _MARKET.value,
                "--outcome",
                "yes",
                "--database",
                str(database),
                "--session-id",
                f"synthetic-{scenario}",
            ],
            _dependencies(scenario),
        )
        _write_json(output / f"{scenario}-collection.json", summary)

    original = database.read_bytes()
    for attempt in (1, 2):
        destination = output / f"pass-{attempt}"
        destination.mkdir()
        for scenario in _SCENARIOS:
            common = [
                "--database",
                str(database),
                "--session-id",
                f"synthetic-{scenario}",
                "--model",
                "midpoint",
                "--half-spread-ticks",
                "1",
                "--quantity",
                "2",
            ]
            for command, flag, suffix in (
                ("shadow", "--decisions-output", "decisions"),
                ("simulate-fills", "--fills-output", "fills"),
            ):
                summary = _invoke(
                    [
                        command,
                        *common,
                        flag,
                        str(destination / f"{scenario}-{suffix}.jsonl"),
                    ]
                )
                _write_json(destination / f"{scenario}-{command}.json", summary)
                if database.read_bytes() != original:
                    raise RuntimeError(f"{command} modified the input journal")

    for artifact in sorted((output / "pass-1").iterdir()):
        if artifact.read_bytes() != (output / "pass-2" / artifact.name).read_bytes():
            raise RuntimeError(f"analysis is not deterministic: {artifact.name}")
    report: dict[str, object] = {
        "provenance": "SYNTHETIC; not venue evidence, profitability or live trading",
        "scenarios": list(_SCENARIOS),
        "analysis_passes": 2,
        "matching_artifacts": 8,
        "journal_unchanged": True,
        "journal_sha256": hashlib.sha256(original).hexdigest(),
    }
    _write_json(output / "report.json", report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    """Run the repository-local example, refusing to overwrite existing output."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="new output directory"
    )
    options = parser.parse_args(argv)
    print(json.dumps(run_demo(options.output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
