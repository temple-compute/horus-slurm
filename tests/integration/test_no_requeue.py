#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Case 4: the pilot is never requeued, even where the cluster allows it.
"""

import re
from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.target.slurm import SlurmTarget

from .conftest import _launch, poll_until, sh, wait_for


@pytest.mark.integration
async def test_pilot_carries_no_requeue(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """``Requeue=0`` on the job while ``JobRequeue = 1`` on the cluster."""
    d = itest_dir
    target = shared_target()
    handle = await _launch(
        target, d, "a", f"until [ -e {d}/go ]; do sleep 1; done"
    )
    assert handle.extra is not None
    job = handle.extra["job_id"]

    async def running() -> bool:
        _rc, out, _err = await sh(target, f"squeue -h -j {job} -o %T")
        return out.strip() == "RUNNING"

    await wait_for(running, 60)
    _rc, show, _err = await sh(target, f"scontrol show job {job}")
    assert "Requeue=0" in show.split()
    _rc, config, _err = await sh(target, "scontrol show config")
    assert re.search(r"^JobRequeue\s*=\s*1$", config, re.MULTILINE)

    (d / "go").touch()
    assert await poll_until(target, handle, 30) == 0
