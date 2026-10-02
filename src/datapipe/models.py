"""Pipeline models: dataclasses for work items, results and stage configs."""

from __future__ import annotations

import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass(slots=True)
class WorkItem:
    """A unit of work flowing through the pipeline."""

    id: int
    payload: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Result:
    """Outcome of processing a work item."""

    item_id: int
    ok: bool
    value: Any = None
    error: str | None = None
    attempts: int = 0
    duration_ms: float = 0.0


Processor = Callable[[WorkItem], Coroutine[Any, Any, Any]]


@dataclass(slots=True)
class StageStats:
    """Live counters for one pipeline stage."""

    name: str
    processed: int = 0
    failed: int = 0
    retries: int = 0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started_at

    @property
    def throughput(self) -> float:
        """Items/second (processed only)."""
        return self.processed / self.elapsed if self.elapsed > 0 else 0.0
