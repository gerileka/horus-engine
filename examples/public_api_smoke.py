"""Opt-in bounded public data capture; private artifacts, sanitized stdout only."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import re
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from horus_engine.application import MarketStatus
from horus_engine.infrastructure.polymarket.catalog import DEFAULT_GAMMA_BASE_URL
from horus_engine.infrastructure.polymarket.clob_parsing import parse_order_book
from horus_engine.infrastructure.polymarket.errors import PolymarketPayloadError
from horus_engine.infrastructure.polymarket.market_stream import (
    DEFAULT_MARKET_WEBSOCKET_URL,
)
from horus_engine.infrastructure.polymarket.order_book import DEFAULT_CLOB_BASE_URL
from horus_engine.infrastructure.polymarket.parsing import (
    market_from_gamma,
    parse_market_page,
)

_ROOT = Path(__file__).resolve().parents[1]
_DISCOVERY_LIMIT = 15
_SOURCE_PATHS = (
    "examples/public_api_smoke.py",
    "src/horus_engine/cli/collect.py",
    "src/horus_engine/infrastructure/polymarket/parsing.py",
    "src/horus_engine/infrastructure/polymarket/clob_parsing.py",
    "src/horus_engine/infrastructure/polymarket/websocket_parsing.py",
    "src/horus_engine/infrastructure/polymarket/market_stream.py",
    "uv.lock",
)


def _utc() -> str:
    return datetime.now(UTC).isoformat()


def _save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def _get(client: httpx.Client, url: str, params: dict[str, str | int]) -> str:
    with client.stream("GET", url, params=params) as response:
        response.raise_for_status()
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_bytes():
            total += len(chunk)
            if total > 2 * 1024 * 1024:
                raise ValueError("response exceeds two MiB")
            chunks.append(chunk)
    return b"".join(chunks).decode("utf-8")


def discover(output: Path) -> None:
    """Read one bounded Gamma page and validate up to three distinct markets.

    Run only in the time-limited child process. Raw replies stay private.
    """
    with httpx.Client(timeout=5, follow_redirects=False) as client:
        document = _get(
            client,
            f"{DEFAULT_GAMMA_BASE_URL}/markets",
            {"active": "true", "closed": "false", "limit": 50, "offset": 0},
        )
        (output / "gamma-page.json").write_text(document)
        selected: list[dict[str, str]] = []
        rejected: list[dict[str, object]] = []
        seen: set[str] = set()
        for index, record in enumerate(parse_market_page(document)):
            try:
                market = market_from_gamma(record, index)
            except (PolymarketPayloadError, ValueError, TypeError) as error:
                rejected.append({"record": index, "error_type": type(error).__name__})
                continue
            if market is None or market.status is not MarketStatus.ACTIVE:
                continue
            if market.market_id.value in seen:
                continue
            seen.add(market.market_id.value)
            outcome = "no" if len(selected) == 1 else "yes"
            token = market.no_token_id if outcome == "no" else market.yes_token_id
            selected.append(
                {
                    "market_id": market.market_id.value,
                    "token_id": token.value,
                    "outcome": outcome,
                }
            )
            if len(selected) == 3:
                break
        _save(output / "discovery.json", {"selected": selected, "rejected": rejected})
        # Snapshot probes are evidence of this parser's compatibility, not stream proof.
        for index, selection in enumerate(selected):
            try:
                book = _get(
                    client,
                    f"{DEFAULT_CLOB_BASE_URL}/book",
                    {"token_id": selection["token_id"]},
                )
                (output / f"book-{index + 1}.json").write_text(book)
                from horus_engine.application import MarketId, TokenId

                parse_order_book(
                    book,
                    MarketId(selection["market_id"]),
                    TokenId(selection["token_id"]),
                )
                selection["snapshot_probe"] = "parsed"
            except Exception as error:
                selection["snapshot_probe"] = type(error).__name__
            _save(
                output / "discovery.json", {"selected": selected, "rejected": rejected}
            )


def _execute(arguments: list[str], limit: int, prefix: Path) -> dict[str, object]:
    started, monotonic = _utc(), time.monotonic_ns()
    code: int | None = None
    terminal = "exited"
    with prefix.with_suffix(".stdout").open("wb") as stdout:
        with prefix.with_suffix(".stderr").open("wb") as stderr:
            try:
                process = subprocess.run(
                    arguments,
                    stdout=stdout,
                    stderr=stderr,
                    timeout=limit,
                    check=False,
                    cwd=_ROOT,
                )
                code = process.returncode
            except subprocess.TimeoutExpired:
                terminal = "wall_timeout_killed"
            except OSError:
                terminal = "process_start_failed"
    return {
        "started_at": started,
        "finished_at": _utc(),
        "elapsed_ms": (time.monotonic_ns() - monotonic) // 1_000_000,
        "wall_limit_seconds": limit,
        "returncode": code,
        "terminal": terminal,
    }


Executor = Callable[[list[str], int, Path], dict[str, object]]


def run_smoke(
    output: Path,
    revision: str,
    duration: int = 10,
    *,
    live: bool = False,
    executor: Executor = _execute,
) -> dict[str, object]:
    """Attempt three distinct short recordings, preserving failures as evidence."""
    if not live:
        raise ValueError("public requests require explicit --live")
    if re.fullmatch("[0-9a-f]{40}", revision) is None:
        raise ValueError("revision must be the full source commit SHA")
    if isinstance(duration, bool) or not 1 <= duration <= 30:
        raise ValueError("duration must be between 1 and 30 seconds")
    output = output.expanduser().resolve()
    if output == _ROOT or _ROOT in output.parents:
        raise ValueError("private recordings must be outside the repository")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    manifest: dict[str, Any] = {
        "revision": revision,
        "started_at": _utc(),
        "python": platform.python_version(),
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in ("aiosqlite", "httpx", "websockets")
        },
        "source_sha256": {
            name: hashlib.sha256((_ROOT / name).read_bytes()).hexdigest()
            for name in _SOURCE_PATHS
        },
        "config": {
            "sessions": 3,
            "duration_seconds": duration,
            "gamma_pages": 1,
            "page_limit": 50,
            "outcomes": ["yes", "no", "yes"],
            "gamma": DEFAULT_GAMMA_BASE_URL,
            "clob": DEFAULT_CLOB_BASE_URL,
            "websocket": DEFAULT_MARKET_WEBSOCKET_URL,
            "retries": 0,
            "authentication": False,
        },
        "sessions": [],
    }
    _save(output / "manifest.json", manifest)
    discovery = executor(
        [
            sys.executable,
            "-m",
            "examples.public_api_smoke",
            "--live",
            "--worker-discover",
            "--output",
            str(output),
        ],
        _DISCOVERY_LIMIT,
        output / "discovery-process",
    )
    manifest["discovery_process"] = discovery
    selections: list[dict[str, str]] = []
    # Partial discovery is retained privately, but never used after worker failure.
    if discovery["returncode"] == 0:
        selections = _load(output / "discovery.json")["selected"]
    for number in range(1, 4):
        row: dict[str, Any] = {
            "session_id": f"public-smoke-{number}",
            "requested_seconds": duration,
            "market_id": None,
            "token_id": None,
            "outcome": "no" if number == 2 else "yes",
            "status": "not_started",
            "reason": "discovery_blocked",
        }
        if number <= len(selections):
            row.update(selections[number - 1])
            row["reason"] = None
            process = executor(
                [
                    sys.executable,
                    "-c",
                    "from horus_engine.cli.main import main; raise SystemExit(main())",
                    "collect",
                    "--market-id",
                    row["market_id"],
                    "--outcome",
                    row["outcome"],
                    "--session-id",
                    row["session_id"],
                    "--database",
                    str(output / f"session-{number}.db"),
                    "--duration-seconds",
                    str(duration),
                ],
                duration + 10,
                output / f"session-{number}",
            )
            row["process"] = process
            row["status"] = "failed"
            row["reason"] = process["terminal"]
            if process["returncode"] == 0:
                try:
                    summary = _load(output / f"session-{number}.stdout")
                    row["collector_summary"] = summary
                    if (
                        summary["session_id"] == row["session_id"]
                        and summary["market_id"] == row["market_id"]
                        and summary["token_id"] == row["token_id"]
                        and summary["updates_recorded"] > 0
                        and summary["finished_at"] is not None
                        and (output / f"session-{number}.db").is_file()
                        and (output / f"session-{number}.db").stat().st_size > 0
                    ):
                        row["status"] = "recorded"
                        row["reason"] = None
                    else:
                        row["reason"] = "empty_or_mismatched_summary"
                except (ValueError, KeyError, OSError, TypeError):
                    row["reason"] = "invalid_collector_summary"
        elif discovery["returncode"] == 0:
            row["reason"] = "insufficient_eligible_markets_in_bounded_page"
        manifest["sessions"].append(row)
        _save(output / "manifest.json", manifest)
    manifest["finished_at"] = _utc()
    for path in output.iterdir():
        if path.is_file():
            path.chmod(0o600)
    manifest["artifacts"] = {
        path.name: {
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": path.stat().st_size,
        }
        for path in sorted(output.iterdir())
        if path.name != "manifest.json"
    }
    _save(output / "manifest.json", manifest)
    rows = manifest["sessions"]
    sanitized: dict[str, object] = {
        "revision": revision,
        "requested_sessions": 3,
        "recorded_sessions": sum(row["status"] == "recorded" for row in rows),
        "failed_sessions": sum(row["status"] == "failed" for row in rows),
        "not_started_sessions": sum(row["status"] == "not_started" for row in rows),
        "requested_seconds_each": duration,
        "discovery_returncode": discovery["returncode"],
        "discovery_terminal": discovery["terminal"],
        "note": (
            "Counts are not schema/quality/profitability certification. "
            "Private artifacts withheld."
        ),
    }
    _save(output / "sanitized-summary.json", sanitized)
    return sanitized


def main(argv: Sequence[str] | None = None) -> int:
    """Require an explicit live flag and print only the safe public summary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", default="")
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument(
        "--worker-discover", action="store_true", help=argparse.SUPPRESS
    )
    options = parser.parse_args(argv)
    if not options.live:
        parser.error("public requests require explicit --live")
    if options.worker_discover:
        discover(options.output)
        return 0
    summary = run_smoke(options.output, options.revision, options.duration, live=True)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["recorded_sessions"] == 3 else 1


if __name__ == "__main__":
    raise SystemExit(main())
