"""Bounded async worker pool that drains a queue through a processor function."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable

from datapipe.models import Result, WorkItem

RetryPolicy = Callable[[int], float]


def exponential_backoff(attempt: int, *, base: float = 0.25, cap: float = 8.0) -> float:
    """Jittered exponential backoff for attempt n (1-based)."""
    return min(cap, base * (2 ** (attempt - 1))) * (0.5 + random.random() / 2)


class WorkerPool:
    """Fixed-size pool of async workers consuming from an asyncio.Queue."""

    def __init__(
        self,
        *,
        name: str,
        size: int,
        processor: Callable[[WorkItem], Awaitable[Result]],
        max_attempts: int = 3,
        retry_delay: RetryPolicy = exponential_backoff,
    ) -> None:
        if size < 1:
            raise ValueError("pool size must be >= 1")
        self.name = name
        self.size = size
        self.processor = processor
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self._queue: asyncio.Queue[WorkItem | None] = asyncio.Queue(maxsize=1000)
        self._tasks: list[asyncio.Task[None]] = []
        self.stats_retries = 0
        self.results: list[Result] = []
        self.on_result: Callable[[Result], None] | None = None

    async def submit(self, item: WorkItem) -> None:
        await self._queue.put(item)

    async def close(self) -> None:
        """Signal shutdown: one sentinel per worker."""
        for _ in self._tasks:
            await self._queue.put(None)

    async def run(self) -> list[asyncio.Task[None]]:
        self._tasks = [
            asyncio.create_task(self._worker(i), name=f"{self.name}-w{i}") for i in range(self.size)
        ]
        return self._tasks

    async def _worker(self, worker_idx: int) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            try:
                result = await self._process_with_retries(item)
                self.results.append(result)
                if self.on_result is not None:
                    self.on_result(result)
            finally:
                self._queue.task_done()

    async def _process_with_retries(self, item: WorkItem) -> Result:
        attempt = 0
        while True:
            attempt += 1
            try:
                result = await self.processor(item)
                result.attempts = attempt
                return result
            except Exception as exc:
                if attempt >= self.max_attempts:
                    return Result(
                        item_id=item.id,
                        ok=False,
                        error=f"{type(exc).__name__}: {exc}",
                        attempts=attempt,
                    )
                self.stats_retries += 1
                await asyncio.sleep(self.retry_delay(attempt))

    async def join(self) -> None:
        """Wait until all workers exited (all sentinels consumed)."""
        if self._tasks:
            await asyncio.gather(*self._tasks)

    @property
    def pending(self) -> int:
        return self._queue.qsize()
