"""Pipeline orchestrator: extract -> transform -> load with live stats."""

from __future__ import annotations

import asyncio
import csv
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from datapipe.models import Result, StageStats, WorkItem
from datapipe.worker import WorkerPool


class Pipeline:
    """Three-stage concurrent pipeline.

    stage A: extract rows from a source (CSV/JSON/JSONL)
    stage B: bounded worker pool applying transforms (simulated I/O)
    stage C: sink successful results to JSONL, failures to .errors.jsonl
    """

    def __init__(
        self,
        *,
        source: Path,
        sink: Path,
        workers: int = 8,
        max_attempts: int = 3,
        fail_on: str | None = None,
    ) -> None:
        self.source = source
        self.sink = sink
        self.workers = workers
        self.max_attempts = max_attempts
        self.fail_on = fail_on  # substring that makes an item fail (demos/tests)

        self.stats = StageStats(name="transform")
        self.results: list[Result] = []
        self._progress_cb: Callable[[Result], None] | None = None
        self._items: list[WorkItem] | None = None

    @property
    def total_items(self) -> int:
        """Number of extracted items (valid after prepare()/run())."""
        return len(self._items) if self._items is not None else 0

    def prepare(self) -> int:
        """Extract items up-front; returns the count. Safe to call twice."""
        if self._items is None:
            self._items = self._extract()
        return len(self._items)

    def on_progress(self, cb: Callable[[Result], None]) -> None:
        self._progress_cb = cb

    # ------------------------------------------------------------- extract ---
    def _extract(self) -> list[WorkItem]:
        text = self.source.read_text(encoding="utf-8")
        rows: list[dict[str, Any]]
        suffix = self.source.suffix.lower()
        if suffix == ".csv":
            rows = list(csv.DictReader(text.splitlines()))
        elif suffix == ".jsonl":
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        elif suffix == ".json":
            parsed = json.loads(text)
            rows = parsed if isinstance(parsed, list) else [parsed]
        else:
            raise ValueError(f"unsupported source type: {suffix}")
        return [WorkItem(id=i, payload=json.dumps(row)) for i, row in enumerate(rows)]

    # ----------------------------------------------------------- transform ---
    async def _transform(self, item: WorkItem) -> Result:
        row = json.loads(item.payload)
        await asyncio.sleep(0.02)  # simulated I/O latency of a real call
        if self.fail_on and self.fail_on in json.dumps(row):
            raise RuntimeError(f"forced failure for item {item.id}")
        row["processed"] = True
        return Result(item_id=item.id, ok=True, value=row)

    # ----------------------------------------------------------------- load ---
    def _load(self) -> None:
        self.results.sort(key=lambda r: r.item_id)
        ok = [r for r in self.results if r.ok]
        with self.sink.open("w", encoding="utf-8") as f:
            for r in ok:
                f.write(json.dumps(r.value) + "\n")
        failed = [r for r in self.results if not r.ok]
        if failed:
            err_path = self.sink.with_suffix(".errors.jsonl")
            with err_path.open("w", encoding="utf-8") as f:
                for r in failed:
                    f.write(json.dumps({"item_id": r.item_id, "error": r.error}) + "\n")

    # ----------------------------------------------------------------- main ---
    async def run(self) -> StageStats:
        self.prepare()
        items = self._items or []
        pool = WorkerPool(
            name="transform",
            size=self.workers,
            processor=self._transform,
            max_attempts=self.max_attempts,
        )
        pool.on_result = self._collect
        await pool.run()

        async def feeder() -> None:
            for item in items:
                await pool.submit(item)
            await pool.close()

        await asyncio.gather(feeder(), pool.join())
        await asyncio.sleep(0)  # yield once so trailing callbacks flush

        self.stats.processed = sum(1 for r in self.results if r.ok)
        self.stats.failed = sum(1 for r in self.results if not r.ok)
        self.stats.retries = pool.stats_retries
        self._load()
        return self.stats

    def _collect(self, result: Result) -> None:
        self.results.append(result)
        if self._progress_cb is not None:
            self._progress_cb(result)

    def report(self) -> dict[str, Any]:
        return {
            "source": str(self.source),
            "sink": str(self.sink),
            "workers": self.workers,
            "processed": self.stats.processed,
            "failed": self.stats.failed,
            "retries": self.stats.retries,
            "elapsed_s": round(self.stats.elapsed, 2),
            "throughput": round(self.stats.throughput, 2),
        }
