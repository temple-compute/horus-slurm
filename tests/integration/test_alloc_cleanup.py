#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
A new allocation removes the target's dirs idle past their retention.
"""

import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.target.slurm import ALLOC_DIR, SlurmTarget

from .conftest import _launch


@pytest.mark.integration
async def test_old_alloc_dir_removed(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """A dir idle for 3 days goes with a 1-day retention; the live stays."""
    target = shared_target(alloc_retention_days=1)
    old = itest_dir / ALLOC_DIR / "hpc.000000000000-1"
    old.mkdir(parents=True)
    heartbeat = old / "heartbeat"
    heartbeat.touch()
    then = time.time() - 3 * 86400
    os.utime(heartbeat, (then, then))

    a = await _launch(target, itest_dir, "a", "echo hi", cpus=1)
    assert a.extra is not None
    live = Path(a.extra["alloc_dir"])
    assert live.name.endswith("-1") and live.name != old.name
    assert not old.exists()
    assert (live / "pilot.sh").exists()
