"""CLI tests using Typer's CliRunner."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from datapipe.cli import app

runner = CliRunner()


def test_cli_run_writes_output(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    rows = [{"id": i, "name": "BAD-row" if i == 3 else f"row-{i}"} for i in range(6)]
    src.write_text(json.dumps(rows), encoding="utf-8")
    sink = tmp_path / "out.jsonl"

    result = runner.invoke(
        app, ["run", str(src), str(sink), "--workers", "4", "--retries", "1", "--fail-on", "BAD"]
    )

    # one item forced to fail -> exit code 1, error file present
    assert result.exit_code == 1, result.output
    lines = sink.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 5
    assert sink.with_suffix(".errors.jsonl").exists()


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
