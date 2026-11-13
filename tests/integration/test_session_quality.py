"""Real read-only SQLite quality CLI, including a deliberately gapped copy."""

import io
import json
import shutil
import sqlite3
from pathlib import Path

from examples.offline_demo import run_demo
from horus_engine.cli.main import main


def test_quality_cli_rejects_a_gapped_copy_without_repairing_it(tmp_path: Path) -> None:
    """Use production journal/reader and compare input bytes after assessment."""
    output = tmp_path / "synthetic"
    run_demo(output)
    journal = output / "synthetic-journal.db"
    original = journal.read_bytes()
    for scenario in ("fills", "no-fill"):
        stdout = io.StringIO()
        result = main(
            [
                "quality",
                "--database",
                str(journal),
                "--session-id",
                f"synthetic-{scenario}",
            ],
            stdout=stdout,
        )
        assert result == 0
        report = json.loads(stdout.getvalue())
        assert report["eligible"] is True
        assert report["terminal_complete"] is True
        assert journal.read_bytes() == original
    corrupt = tmp_path / "gapped.db"
    shutil.copyfile(journal, corrupt)
    with sqlite3.connect(corrupt) as connection:
        connection.execute(
            "UPDATE market_data_events SET sequence_number = 9 "
            "WHERE session_id = ? AND sequence_number = 2",
            ("synthetic-fills",),
        )
    before = corrupt.read_bytes()
    stdout = io.StringIO()
    result = main(
        ["quality", "--database", str(corrupt), "--session-id", "synthetic-fills"],
        stdout=stdout,
    )
    assert result == 1
    report = json.loads(stdout.getvalue())
    assert report["eligible"] is False
    assert report["sequence_consistent"] is False
    assert "replay_integrity_failed" in report["reasons"]
    assert corrupt.read_bytes() == before
