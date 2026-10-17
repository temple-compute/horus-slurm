#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
A step spooled in a pilot that is cancelled moves to a new allocation.
"""

from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.target.slurm import SlurmTarget

from .conftest import _launch, job_state, poll_until, sh, wait_for


@pytest.mark.integration
async def test_spooled_step_moves(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """The pending pilot is scancelled; the step runs in generation 2."""
    d = itest_dir
    target = shared_target(queue_check_interval=5)
    rc, out, err = await sh(
        target,
        "sbatch --parsable -N 2 --ntasks-per-node=1 -c 4 --wrap 'sleep 300'",
    )
    assert rc == 0, err
    blocker = out.strip()

    async def blocker_runs() -> bool:
        return await job_state(target, blocker) == "RUNNING"

    await wait_for(blocker_runs, 60)
    b = await _launch(target, d, "b", "echo ran", cpus=1)
    assert b.extra is not None
    extra = b.extra
    old_job = extra["job_id"]
    assert (Path(extra["alloc_dir"]) / "spool" / "horus-b.sh").exists()
    rc, _out, err = await sh(target, f"scancel {old_job}")
    assert rc == 0, err

    async def moved() -> bool:
        assert await target.poll(b) is None
        return extra.get("moves") == "1"

    await wait_for(moved, 60)
    assert target.generation == 2
    assert extra["job_id"] != old_job
    assert extra["alloc_dir"].endswith("-2")

    rc, _out, err = await sh(target, f"scancel {blocker}")
    assert rc == 0, err
    assert await poll_until(target, b, 90) == 0

    # srun writes the step's output after the step wrote exit_code.
    async def output() -> bool:
        stdout, _ = await target.read_output(b)
        return b"ran" in stdout

    await wait_for(output, 15)
