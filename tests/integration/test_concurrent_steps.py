#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Case 1: two ``--exact`` steps run at once in one allocation.
"""

from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.target.slurm import ALLOC_DIR, SlurmTarget

from .conftest import _launch, poll_until, sh, wait_for


def _barrier(d: Path, me: str, other: str) -> str:
    return (
        f"touch {d}/{me}.up; until [ -e {d}/{other}.up ]; do sleep 1; done; "
        f"until [ -e {d}/go ]; do sleep 1; done"
    )


@pytest.mark.integration
async def test_two_steps_run_at_once(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """Each step waits for the other, so both must be running together."""
    d = itest_dir
    target = shared_target()
    other = target.idle_copy()
    a = await _launch(target, d, "a", _barrier(d, "a", "b"), cpus=1)
    b = await _launch(other, d, "b", _barrier(d, "b", "a"), cpus=1)
    await wait_for(lambda: (d / "a.up").exists() and (d / "b.up").exists(), 60)

    assert a.extra is not None and b.extra is not None
    job = a.extra["job_id"]
    assert b.extra["job_id"] == job
    assert target.generation == 1
    # Every sbatch of the pilot gets its own allocation dir.
    assert len(list(d.glob(f"{ALLOC_DIR}/*"))) == 1
    _rc, out, _err = await sh(target, f"squeue -s -h -j {job} -o %i")
    steps = [s for s in out.split() if not s.endswith((".batch", ".extern"))]
    assert sorted(steps) == [f"{job}.0", f"{job}.1"]

    (d / "go").touch()
    assert await poll_until(target, a, 30) == 0
    assert await poll_until(other, b, 30) == 0
