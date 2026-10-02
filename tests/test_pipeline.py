"""Tests for WorkerPool and Pipeline."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from datapipe.models import Result, WorkItem
from datapipe.pipeline import Pipeline
from datapipe.worker import WorkerPool, exponential_backoff


async def test_worker_pool_processes_all_items() -> None:
    seen: list[int] = []

    async def proc(item: WorkItem) -> Result:
        await asyncio.sleep(0.001)
        seen.append(item.id)
        return Result(item_id=item.id, ok=True, value=item.id * 2)

    pool = WorkerPool(name="t", size=4, processor=proc)
    await pool.run()
    for i in range(20):
        await pool.submit(WorkItem(id=i, payload="x"))
    await pool.close()
    await pool.join()

    assert sorted(seen) == list(range(20))
    assert len(pool.results) == 20
    assert all(r.ok for r in pool.results)


async def test_worker_pool_retries_then_succeeds() -> None:
    calls: dict[int, int] = {}

    async def flaky(item: WorkItem) -> Result:
        calls[item.id] = calls.get(item.id, 0) + 1
        if calls[item.id] < 3:
            raise RuntimeError("transient")
        return Result(item_id=item.id, ok=True, value="ok")

    pool = WorkerPool(name="t", size=1, processor=flaky, max_attempts=5, retry_delay=lambda a: 0.0)
    await pool.run()
    await pool.submit(WorkItem(id=1, payload="x"))
    await pool.close()
    await pool.join()

    (result,) = pool.results
    assert result.ok
    assert result.attempts == 3
    assert pool.stats_retries == 2


async def test_worker_pool_gives_up_after_max_attempts() -> None:
    async def always_fails(item: WorkItem) -> Result:
        raise RuntimeError("nope")

    pool = WorkerPool(
        name="t", size=2, processor=always_fails, max_attempts=2, retry_delay=lambda a: 0.0
    )
    await pool.run()
    await pool.submit(WorkItem(id=1, payload="x"))
    await pool.close()
    await pool.join()

    (result,) = pool.results
    assert not result.ok
    assert "nope" in (result.error or "")
    assert result.attempts == 2


def test_backoff_is_bounded_and_positive() -> None:
    for attempt in range(1, 12):
        delay = exponential_backoff(attempt)
        assert 0 < delay <= 8.0


async def test_pipeline_end_to_end(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    rows = [{"id": i, "name": f"row-{i}"} for i in range(10)]
    src.write_text(json.dumps(rows), encoding="utf-8")
    sink = tmp_path / "out.jsonl"

    pipeline = Pipeline(source=src, sink=sink, workers=4)
    stats = await pipeline.run()

    assert stats.processed == 10
    assert stats.failed == 0
    lines = sink.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 10
    assert all(json.loads(line)["processed"] for line in lines)


async def test_pipeline_handles_failures(tmp_path: Path) -> None:
    src = tmp_path / "in.csv"
    src.write_text("name\nok-row\nBAD-row\n", encoding="utf-8")
    sink = tmp_path / "out.jsonl"

    pipeline = Pipeline(source=src, sink=sink, workers=2, fail_on="BAD", max_attempts=1)
    stats = await pipeline.run()

    assert stats.processed == 1
    assert stats.failed == 1
    errors = sink.with_suffix(".errors.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(errors) == 1


async def test_pipeline_checkpoint_and_resume(tmp_path: Path) -> None:
    src = tmp_path / "stream_in.jsonl"
    lines = [json.dumps({"id": i, "val": f"v-{i}"}) for i in range(8)]
    src.write_text("\n".join(lines), encoding="utf-8")
    sink = tmp_path / "stream_out.jsonl"

    # Run 1: process with a simulated interruption on row 4
    pipe1 = Pipeline(source=src, sink=sink, workers=2, fail_on="v-4", max_attempts=1)
    await pipe1.run()
    assert pipe1.checkpoint_path.exists()

    # Run 2: resume without fail_on
    pipe2 = Pipeline(source=src, sink=sink, workers=2, resume=True)
    assert pipe2.resumed_count > 0
    stats2 = await pipe2.run()
    assert stats2.processed == 8

