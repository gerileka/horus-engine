"""Independent numerical oracle for the runnable synthetic CLI demonstration."""

import json
import socket
from decimal import Decimal
from pathlib import Path

import pytest

from examples.offline_demo import run_demo


def _json(path: Path) -> dict[str, object]:
    return dict(json.loads(path.read_text()))


def _rows(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_offline_demo_cli_artifacts_match_hand_calculated_oracle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cover real collection/replay, fractional fills, suppression and repeatability."""

    def no_network(*args: object, **kwargs: object) -> None:
        raise AssertionError("offline demo attempted a network connection")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    output = tmp_path / "demo"
    report = run_demo(output)
    assert report["journal_unchanged"] is True
    assert report["matching_artifacts"] == 8
    for scenario in ("fills", "no-fill"):
        collection = _json(output / f"{scenario}-collection.json")
        assert collection["updates_recorded"] == 4
        assert collection["final_status"] == "STALE"
        decisions = _rows(output / "pass-1" / f"{scenario}-decisions.jsonl")
        assert [row["action"] for row in decisions] == (
            ["place", "hold", "hold", "cancel"]
            if scenario == "fills"
            else ["none", "place", "hold", "cancel"]
        )
        quote = decisions[0 if scenario == "fills" else 1]
        # (0.48 + 0.52)/2 = 0.50; one 0.01 tick each side, size 2.
        assert Decimal(str(quote["fair_value"])) == Decimal("0.50")
        assert Decimal(str(quote["desired_bid"])) == Decimal("0.49")
        assert Decimal(str(quote["desired_ask"])) == Decimal("0.51")
        assert Decimal(str(quote["quantity"])) == Decimal("2")
        assert decisions[-1]["status"] == "STALE"
        assert decisions[-1]["desired_bid"] is None

    fills = _rows(output / "pass-1" / "fills-fills.jsonl")
    assert [row["sequence_number"] for row in fills] == [2, 3]
    assert [row["fill_quantity"] for row in fills] == ["1.25", "2"]
    assert [row["quote_price"] for row in fills] == ["0.49", "0.51"]
    summary = _json(output / "pass-1" / "fills-simulate-fills.json")
    expected = {
        "buy_filled_quantity": "1.25",
        "sell_filled_quantity": "2",
        "buy_notional": "0.6125",
        "sell_notional": "1.02",
        "net_token_position": "-0.75",
        "net_cash_flow": "0.4075",
    }
    for key, value in expected.items():
        assert isinstance(summary[key], str)  # Never a binary floating-point amount.
        assert Decimal(str(summary[key])) == Decimal(value)
    assert _rows(output / "pass-1" / "no-fill-fills.jsonl") == []
    assert _json(output / "pass-1" / "no-fill-simulate-fills.json")["fill_events"] == 0
    assert _rows(output / "pass-1" / "no-fill-decisions.jsonl")[0]["status"] == (
        "AWAITING_SNAPSHOT"
    )
    for artifact in (output / "pass-1").iterdir():
        assert artifact.read_bytes() == (output / "pass-2" / artifact.name).read_bytes()

    # A fresh collection in a different directory also has identical normalized output.
    second = tmp_path / "second"
    run_demo(second)
    for artifact in (output / "pass-1").iterdir():
        assert artifact.read_bytes() == (second / "pass-1" / artifact.name).read_bytes()
    for scenario in ("fills", "no-fill"):
        name = f"{scenario}-collection.json"
        assert (output / name).read_bytes() == (second / name).read_bytes()


def test_offline_demo_refuses_existing_output(tmp_path: Path) -> None:
    """Existing data must remain untouched rather than being silently replaced."""
    sentinel = tmp_path / "keep.txt"
    sentinel.write_text("keep")
    with pytest.raises(FileExistsError):
        run_demo(tmp_path)
    assert sentinel.read_text() == "keep"
