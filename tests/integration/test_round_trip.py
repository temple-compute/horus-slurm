#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Case 6: a full SlurmTarget round trip in one shared allocation.
"""

import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.record import STDERR_FILE, STDOUT_FILE
from horus_slurm.target.slurm import ALLOC_DIR, SlurmTarget

from .conftest import _launch, job_state, poll_until, wait_for


@pytest.mark.integration
async def test_round_trip(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """Launch, poll, output, keepalive and release against real Slurm."""
    d = itest_dir
    target = shared_target(cpus_per_task=2)
    other = target.idle_copy()
    ok = await _launch(target, d, "ok", "echo out; echo err >&2", cpus=1)
    bad = await _launch(other, d, "bad", "exit 3", cpus=1)

    assert await poll_until(target, ok, 60) == 0
    assert await poll_until(other, bad, 60) == 3
    assert await target.read_output(ok) == (b"out\n", b"err\n")
    side = d / "ok" / "side-artifacts"
    assert (side / STDOUT_FILE).read_bytes() == b"out\n"
    assert (side / STDERR_FILE).read_bytes() == b"err\n"

    assert ok.extra is not None and bad.extra is not None
    job, alloc = ok.extra["job_id"], Path(ok.extra["alloc_dir"])
    assert bad.extra["job_id"] == job
    assert target.generation == 1
    # Every sbatch of the pilot gets its own allocation dir.
    assert len(list(d.glob(f"{ALLOC_DIR}/*"))) == 1

    old = time.time() - 60
    os.utime(alloc / "heartbeat", (old, old))
    await target.keepalive()
    assert (alloc / "heartbeat").stat().st_mtime > old + 30

    await target.release("run_end")

    async def gone() -> bool:
        return not await job_state(target, job)

    await wait_for(gone, 20)
    assert not (alloc / "spool").exists()
    assert not (alloc / "run").exists()
    assert (alloc / "pilot.log").exists()
