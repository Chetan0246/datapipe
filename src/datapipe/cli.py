"""Typer CLI with Rich progress for datapipe."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import typer
from rich.console import Console

# Windows consoles default to cp1252; never crash on unicode spinner/box chars.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(errors="replace")
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from datapipe.pipeline import Pipeline

app = typer.Typer(
    help="Concurrent async data pipeline.", no_args_is_help=True, add_completion=False
)
console = Console()


@app.callback()
def _root() -> None:
    """Concurrent async data pipeline."""


@app.command()
def run(
    source: Path = typer.Argument(
        ..., exists=True, readable=True, help="Input file: .csv / .json / .jsonl"
    ),
    sink: Path = typer.Argument(..., help="Output JSONL file"),
    workers: int = typer.Option(8, "--workers", "-w", min=1, max=64, help="Worker pool size"),
    max_attempts: int = typer.Option(
        3, "--retries", "-r", min=1, max=10, help="Max attempts per item"
    ),
    fail_on: str | None = typer.Option(
        None, "--fail-on", help="Force failure when payload contains this string"
    ),
    resume: bool = typer.Option(
        False, "--resume", help="Resume from last checkpoint, skipping completed items"
    ),
) -> None:
    """Run the pipeline over SOURCE, writing successful rows to SINK."""
    pipeline = Pipeline(
        source=source,
        sink=sink,
        workers=workers,
        max_attempts=max_attempts,
        fail_on=fail_on,
        resume=resume,
    )

    total = pipeline.prepare()
    if resume and pipeline.resumed_count > 0:
        msg = f"Resuming pipeline: {pipeline.resumed_count}/{total} already completed."
        console.print(f"[bold green]{msg}[/bold green]")
    console.print(f"[bold]Loaded {total} item(s) from {source.name}[/bold]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("processing", total=total)
        pipeline.on_progress(lambda _result: progress.advance(task_id))
        stats = asyncio.run(pipeline.run())

    table = Table(title="Pipeline report", show_header=True, header_style="bold magenta")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for key, value in pipeline.report().items():
        table.add_row(key, str(value))
    console.print(table)

    failed = stats.failed
    if failed:
        console.print(
            f"[red]{failed} item(s) failed[/red] — see {sink.with_suffix('.errors.jsonl')}"
        )
    raise typer.Exit(code=1 if failed else 0)


if __name__ == "__main__":
    app()
