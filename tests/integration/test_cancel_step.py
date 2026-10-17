#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Case 2: cancelling one step leaves the allocation and its other steps alone.
"""

import signal
from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.target.slurm import SlurmTarget

from .conftest import _launch, poll_until, sh, wait_for


class _F1Error(AssertionError):
    """The exit code a cancelled step reports is not 143 yet."""


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    raises=_F1Error,
    reason="F1: srun exits 1 when cancelled pending, the launcher records it",
)
async def test_cancel_step_waiting_for_resources(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """Step b waits for a's CPUs; cancelling b does not touch a."""
    d = itest_dir
    target = shared_target(cpus_per_task=2)
    other = target.idle_copy()
    a = await _launch(
        target, d, "a", f"until [ -e {d}/go ]; do sleep 1; done", cpus=2
    )
    b = await _launch(other, d, "b", "echo ran", cpus=1)
    assert a.extra is not None and b.extra is not None
    job, alloc = a.extra["job_id"], Path(b.extra["alloc_dir"])

    async def a_runs_b_claimed() -> bool:
        _rc, out, _err = await sh(target, f"squeue -s -h -j {job} -o %i")
        steps = [s for s in out.split() if s.split(".")[-1].isdigit()]
        return (
            len(steps) == 1
            and (alloc / "run" / "horus-b.sh").exists()
            and not (alloc / "spool" / "horus-b.sh").exists()
        )

    await wait_for(a_runs_b_claimed, 60)
    await other.send_signal(b, signal.SIGTERM)
    code = await poll_until(other, b, 15)
    stdout, _ = await other.read_output(b)
    assert b"ran" not in stdout

    _rc, state, _err = await sh(target, f"squeue -h -j {job} -o %T")
    assert state.strip() == "RUNNING"
    assert await target.poll(a) is None
    (d / "go").touch()
    assert await poll_until(target, a, 30) == 0

    if code != 143:
        raise _F1Error(f"poll returned {code}")


@pytest.mark.integration
async def test_cancel_spooled_step(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """A step the pending pilot has not claimed is taken back: 143."""
    d = itest_dir
    target = shared_target()
    rc, out, err = await sh(
        target,
        "sbatch --parsable -N 2 --ntasks-per-node=1 -c 4 --wrap 'sleep 300'",
    )
    assert rc == 0, err
    blocker = out.strip()

    async def blocker_runs() -> bool:
        _rc, out, _err = await sh(target, f"squeue -h -j {blocker} -o %T")
        return out.strip() == "RUNNING"

    await wait_for(blocker_runs, 60)
    b = await _launch(target, d, "b", "echo ran", cpus=1)
    assert b.extra is not None
    alloc = Path(b.extra["alloc_dir"])
    assert (alloc / "spool" / "horus-b.sh").exists()

    await target.send_signal(b, signal.SIGTERM)
    assert await poll_until(target, b, 15) == 143
    assert not (alloc / "spool" / "horus-b.sh").exists()
    assert not (alloc / "run" / "horus-b.sh").exists()
