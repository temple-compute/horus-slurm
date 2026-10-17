#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
A member's allocation goes queued, ready, released on the bus.
"""

from collections.abc import Callable
from pathlib import Path

import pytest
from horus_runtime.context import HorusContext
from horus_runtime.event.base import BaseEvent

from horus_slurm.events import SlurmAllocationEvent
from horus_slurm.target.slurm import SlurmTarget

from .conftest import _launch, poll_until


@pytest.mark.integration
async def test_queued_ready_released(
    shared_target: Callable[..., SlurmTarget],
    itest_dir: Path,
    horus_context: HorusContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each phase is announced once, for generation 1."""
    events: list[SlurmAllocationEvent] = []

    def capture(event: BaseEvent) -> None:
        if isinstance(event, SlurmAllocationEvent):
            events.append(event)

    monkeypatch.setattr(horus_context.bus, "emit", capture)
    target = shared_target(queue_check_interval=5)
    a = await _launch(target, itest_dir, "a", "echo hi", cpus=1)
    assert await poll_until(target, a, 60) == 0
    await target.release("run_end")

    assert [(e.phase, e.generation) for e in events] == [
        ("queued", 1),
        ("ready", 1),
        ("released", 1),
    ]
    assert all(e.member_task_ids == ["a"] for e in events)
    assert events[-1].reason == "run_end"
