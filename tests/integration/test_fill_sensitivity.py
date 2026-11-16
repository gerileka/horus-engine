"""Real read-only SQLite comparison, deterministic hand oracle and rejection."""

import asyncio
import json
import sqlite3
from pathlib import Path

from examples.fill_sensitivity import compare, main
from examples.offline_demo import run_demo


def test_eligible_demo_sensitivity_and_gapped_history(tmp_path: Path) -> None:
    """No venue data: one inside-spread partial buy and one assumed through sell."""
    output = tmp_path / "synthetic"
    run_demo(output)
    database = output / "synthetic-journal.db"
    before = database.read_bytes()
    report = asyncio.run(compare(database, "synthetic-fills"))
    assert report == asyncio.run(compare(database, "synthetic-fills"))
    scenarios = report["scenarios"]
    assert isinstance(scenarios, list)
    quantities = {s["name"]: (s["buy_quantity"], s["sell_quantity"]) for s in scenarios}
    assert quantities == {
        "legacy_baseline": ("1.25", "2"),
        "exact_price_only": ("1.25", "0"),
        "double_displayed_queue": ("1.25", "2"),
        "one_event_activation_delay": ("0", "2"),
        "repeated_trade_abstention": ("1.25", "2"),
        "snapshot_abstention": ("1.25", "2"),
        "combined_stress": ("0", "0"),
    }
    baseline = scenarios[0]
    assert baseline["buy_notional"] == "0.6125"
    assert baseline["sell_notional"] == "1.02"
    assert baseline["net_cash_flow_not_profit"] == "0.4075"
    assert baseline["net_position"] == "-0.75"
    no_fill = asyncio.run(compare(database, "synthetic-no-fill"))["scenarios"]
    assert isinstance(no_fill, list)
    assert all(s["buy_quantity"] == s["sell_quantity"] == "0" for s in no_fill)
    assert main(["--database", str(database), "--session-id", "synthetic-fills"]) == 0
    assert database.read_bytes() == before
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE market_data_events SET sequence_number=99 "
            "WHERE session_id=? AND sequence_number=2",
            ("synthetic-fills",),
        )
    corrupt = database.read_bytes()
    rejected = asyncio.run(compare(database, "synthetic-fills"))
    assert rejected["scenarios"] is None
    assert "replay_integrity_failed" in json.dumps(rejected)
    assert main(["--database", str(database), "--session-id", "synthetic-fills"]) == 1
    assert database.read_bytes() == corrupt
