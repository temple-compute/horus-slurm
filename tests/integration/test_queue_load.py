#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Fifty members of one shared target share one squeue read per interval.
"""

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from horus_slurm.target.slurm import SlurmTarget

from .conftest import ExecInner, _launch


@pytest.mark.integration
async def test_one_squeue_per_interval(
    shared_target: Callable[..., SlurmTarget],
    itest_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Polling 50 members for 60 s reads the queue about every 10 s."""
    d = itest_dir
    target = shared_target(cpus_per_task=1, queue_check_interval=10)
    members = [target] + [target.idle_copy() for _ in range(49)]
    wait = f"until [ -e {d}/go ]; do sleep 1; done"
    handles = [
        await _launch(m, d, f"m{i}", wait, cpus=1)
        for i, m in enumerate(members)
    ]

    reads = 0
    run = ExecInner.run_command_sync

    async def counting(self: ExecInner, cmd: str, **kw: Any) -> Any:
        nonlocal reads
        reads += cmd.startswith("squeue -h -j ")
        return await run(self, cmd, **kw)

    monkeypatch.setattr(ExecInner, "run_command_sync", counting)
    loop = asyncio.get_running_loop()
    end = loop.time() + 60
    while loop.time() < end:
        codes = await asyncio.gather(
            *(m.poll(h) for m, h in zip(members, handles, strict=True))
        )
        assert codes == [None] * 50
        await asyncio.sleep(5)
    # 600 member polls; per member that would be one read each.
    assert 4 <= reads <= 9, reads
