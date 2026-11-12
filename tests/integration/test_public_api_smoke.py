"""Network-free acceptance tests for opt-in capture orchestration and provenance."""

import hashlib
import json
import socket
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from examples import public_api_smoke as smoke

_REVISION = "a" * 40


def _load(path: Path) -> dict[str, Any]:
    return dict(json.loads(path.read_text()))


@pytest.fixture(autouse=True)
def deny_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Normal CI must never connect to public endpoints."""

    def reject(*args: object, **kwargs: object) -> None:
        raise AssertionError("live networking attempted in smoke tests")

    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket, "getaddrinfo", reject)


def test_opt_in_validation_and_existing_output_are_nonmutating(tmp_path: Path) -> None:
    """No directory creation or public request precedes explicit opt-in."""
    target = tmp_path / "new"
    with pytest.raises(ValueError, match="--live"):
        smoke.run_smoke(target, _REVISION)
    with pytest.raises(ValueError, match="full source"):
        smoke.run_smoke(target, "main", live=True)
    with pytest.raises(ValueError, match="between"):
        smoke.run_smoke(target, _REVISION, 31, live=True)
    assert not target.exists()
    with pytest.raises(FileExistsError):
        smoke.run_smoke(tmp_path, _REVISION, live=True)
    with pytest.raises(ValueError, match="outside"):
        smoke.run_smoke(
            Path(smoke.__file__).parents[1] / "private", _REVISION, live=True
        )
    with pytest.raises(SystemExit) as error:
        smoke.main(["--output", str(target), "--revision", _REVISION])
    assert error.value.code == 2


def test_discovery_blocker_never_becomes_three_successful_sessions(
    tmp_path: Path,
) -> None:
    """A failed endpoint leaves explicit not-started slots and private error logs."""
    calls: list[int] = []

    def failed(args: list[str], limit: int, prefix: Path) -> dict[str, object]:
        calls.append(limit)
        prefix.with_suffix(".stderr").write_text("private endpoint failure details")
        return {"returncode": 1, "terminal": "exited"}

    output = tmp_path / "private"
    summary = smoke.run_smoke(output, _REVISION, live=True, executor=failed)
    assert calls == [15]
    assert summary["recorded_sessions"] == 0
    assert summary["not_started_sessions"] == 3
    assert "private endpoint failure details" not in json.dumps(summary)
    manifest = _load(output / "manifest.json")
    assert len(manifest["sessions"]) == 3
    assert all(row["market_id"] is None for row in manifest["sessions"])
    assert all(row["reason"] == "discovery_blocked" for row in manifest["sessions"])
    assert manifest["revision"] == _REVISION
    assert set(manifest["dependencies"]) == {"httpx", "aiosqlite", "websockets"}
    assert "uv.lock" in manifest["source_sha256"]
    artifact = manifest["artifacts"]["discovery-process.stderr"]
    assert (
        artifact["sha256"]
        == hashlib.sha256(
            (output / "discovery-process.stderr").read_bytes()
        ).hexdigest()
    )
    assert output.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in output.iterdir())


def test_three_selected_sessions_preserve_partial_and_timeout_evidence(
    tmp_path: Path,
) -> None:
    """Only a matching nonempty finalized collector summary counts as recorded."""
    calls: list[int] = []

    def execute(args: list[str], limit: int, prefix: Path) -> dict[str, object]:
        calls.append(limit)
        if "--worker-discover" in args:
            selected = [
                {
                    "market_id": f"market-{n}",
                    "token_id": f"token-{n}",
                    "outcome": "no" if n == 2 else "yes",
                }
                for n in (1, 2, 3)
            ]
            (prefix.parent / "discovery.json").write_text(
                json.dumps({"selected": selected})
            )
            return {"returncode": 0, "terminal": "exited"}
        number = int(prefix.name[-1])
        if number == 3:
            return {"returncode": None, "terminal": "wall_timeout_killed"}
        summary = {
            "session_id": f"public-smoke-{number}",
            "market_id": f"market-{number}",
            "token_id": f"token-{number}",
            "updates_recorded": 3 if number == 1 else 0,
            "finished_at": "2026-11-12T19:00:00+00:00",
            "termination": "stream_ended",
        }
        prefix.with_suffix(".stdout").write_text(json.dumps(summary))
        prefix.with_suffix(".db").write_bytes(
            b"synthetic mock journal; not a live capture"
        )
        return {"returncode": 0, "terminal": "exited"}

    output = tmp_path / "private"
    summary = smoke.run_smoke(output, _REVISION, 2, live=True, executor=execute)
    assert calls == [15, 12, 12, 12]
    assert summary["recorded_sessions"] == 1
    assert summary["failed_sessions"] == 2
    rows = _load(output / "manifest.json")["sessions"]
    assert [row["status"] for row in rows] == ["recorded", "failed", "failed"]
    assert rows[0]["collector_summary"]["termination"] == "stream_ended"
    assert rows[1]["reason"] == "empty_or_mismatched_summary"
    assert rows[2]["reason"] == "wall_timeout_killed"
    assert "token-1" not in json.dumps(summary)


def test_real_process_wall_limit_and_nonzero_exit(tmp_path: Path) -> None:
    """OS-level time limits terminate a stuck child independently of asyncio."""
    result = smoke._execute(
        [sys.executable, "-c", "import time; time.sleep(30)"], 1, tmp_path / "slow"
    )
    assert result["terminal"] == "wall_timeout_killed"
    assert result["returncode"] is None
    elapsed = result["elapsed_ms"]
    assert isinstance(elapsed, int) and elapsed < 5000
    result = smoke._execute(
        [sys.executable, "-c", "raise SystemExit(7)"], 2, tmp_path / "failed"
    )
    assert result["returncode"] == 7


def test_one_page_discovery_uses_real_parsers_and_rest_probes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Synthetic schemas exercise adapters without claiming live compatibility."""
    fixture = Path(__file__).parents[1] / "fixtures/polymarket/active_market.json"
    original = json.loads(fixture.read_text())[0]
    records = []
    for n in (1, 1, 2, 3, 4):
        record = dict(original)
        record["conditionId"] = f"market-{n}"
        record["clobTokenIds"] = json.dumps([f"yes-{n}", f"no-{n}"])
        records.append(record)
    requests: list[str] = []

    def reply(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/markets":
            assert request.url.params["limit"] == "50"
            return httpx.Response(200, json=records)
        token = request.url.params["token_id"]
        number = token[-1]
        return httpx.Response(
            200,
            json={
                "market": f"market-{number}",
                "asset_id": token,
                "tick_size": "0.01",
                "bids": [],
                "asks": [],
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(reply))
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client)
    smoke.discover(tmp_path)
    selected = _load(tmp_path / "discovery.json")["selected"]
    assert [row["market_id"] for row in selected] == [
        "market-1",
        "market-2",
        "market-3",
    ]
    assert [row["outcome"] for row in selected] == ["yes", "no", "yes"]
    assert all(row["snapshot_probe"] == "parsed" for row in selected)
    assert requests == ["/markets", "/book", "/book", "/book"]
