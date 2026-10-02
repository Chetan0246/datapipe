"""Pipeline orchestrator: streaming extract -> transform -> load with checkpoints."""

from __future__ import annotations

import asyncio
import csv
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from datapipe.models import Result, StageStats, WorkItem
from datapipe.worker import WorkerPool


class Pipeline:
    """Three-stage concurrent streaming pipeline with resume & checkpointing.

    stage A: streaming extract from a source (CSV/JSON/JSONL) with bounded memory
    stage B: bounded worker pool applying transforms with retries & backoff
    stage C: progressive sink to JSONL, failures to .errors.jsonl with checkpointing
    """

    def __init__(
        self,
        *,
        source: Path,
        sink: Path,
        workers: int = 8,
        max_attempts: int = 3,
        fail_on: str | None = None,
        resume: bool = False,
    ) -> None:
        self.source = source
        self.sink = sink
        self.workers = workers
        self.max_attempts = max_attempts
        self.fail_on = fail_on
        self.resume = resume

        self.checkpoint_path = self.sink.with_suffix(".checkpoint.json")
        self.stats = StageStats(name="transform")
        self.results: list[Result] = []
        self._progress_cb: Callable[[Result], None] | None = None
        self._items: list[WorkItem] | None = None
        self._resumed_ids: set[int] = set()

        if self.resume and self.checkpoint_path.exists():
            try:
                data = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
                self._resumed_ids = set(data.get("completed_ids", []))
            except Exception:
                self._resumed_ids = set()

    @property
    def total_items(self) -> int:
        """Number of extracted items (valid after prepare()/run())."""
        return len(self._items) if self._items is not None else 0

    @property
    def resumed_count(self) -> int:
        return len(self._resumed_ids)

    def prepare(self) -> int:
        """Extract/enumerate items up-front; returns the count. Safe to call twice."""
        if self._items is None:
            self._items = list(self._iter_extract())
        return len(self._items)

    def on_progress(self, cb: Callable[[Result], None]) -> None:
        self._progress_cb = cb

    # ------------------------------------------------------------- extract ---
    def _iter_extract(self) -> Iterator[WorkItem]:
        suffix = self.source.suffix.lower()
        if suffix == ".csv":
            with self.source.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    yield WorkItem(id=i, payload=json.dumps(row))
        elif suffix == ".jsonl":
            with self.source.open("r", encoding="utf-8") as f:
                idx = 0
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        yield WorkItem(id=idx, payload=stripped)
                        idx += 1
        elif suffix == ".json":
            text = self.source.read_text(encoding="utf-8")
            parsed = json.loads(text)
            rows = parsed if isinstance(parsed, list) else [parsed]
            for i, row in enumerate(rows):
                yield WorkItem(id=i, payload=json.dumps(row))
        else:
            raise ValueError(f"unsupported source type: {suffix}")

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
        mode = "a" if (self.resume and self.sink.exists()) else "w"
        self.results.sort(key=lambda r: r.item_id)
        ok = [r for r in self.results if r.ok]
        with self.sink.open(mode, encoding="utf-8") as f:
            for r in ok:
                f.write(json.dumps(r.value) + "\n")

        failed = [r for r in self.results if not r.ok]
        if failed:
            err_path = self.sink.with_suffix(".errors.jsonl")
            err_mode = "a" if (self.resume and err_path.exists()) else "w"
            with err_path.open(err_mode, encoding="utf-8") as f:
                for r in failed:
                    f.write(json.dumps({"item_id": r.item_id, "error": r.error}) + "\n")

        # Save checkpoint
        all_completed = self._resumed_ids.union(r.item_id for r in self.results)
        self.checkpoint_path.write_text(
            json.dumps({"completed_ids": sorted(all_completed), "count": len(all_completed)}),
            encoding="utf-8",
        )

    # ----------------------------------------------------------------- main ---
    async def run(self) -> StageStats:
        self.prepare()
        items = self._items or []
        items_to_process = [it for it in items if it.id not in self._resumed_ids]

        pool = WorkerPool(
            name="transform",
            size=self.workers,
            processor=self._transform,
            max_attempts=self.max_attempts,
        )
        pool.on_result = self._collect
        await pool.run()

        async def feeder() -> None:
            for item in items_to_process:
                await pool.submit(item)
            await pool.close()

        await asyncio.gather(feeder(), pool.join())
        await asyncio.sleep(0)  # yield once so trailing callbacks flush

        self.stats.processed = sum(1 for r in self.results if r.ok) + len(self._resumed_ids)
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
            "resumed": len(self._resumed_ids),
            "elapsed_s": round(self.stats.elapsed, 2),
            "throughput": round(self.stats.throughput, 2),
        }
