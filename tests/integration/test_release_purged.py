#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Case 3: releasing an allocation Slurm has already forgotten.
"""

import logging
from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.target.slurm import SlurmTarget

from .conftest import _launch, poll_until, sh, wait_for


@pytest.mark.integration
async def test_scancel_unknown_id(
    shared_target: Callable[..., SlurmTarget],
) -> None:
    """The condition ``release`` relies on holds for an unknown id."""
    rc, out, err = await sh(shared_target(), "scancel 999999")
    logging.getLogger(__name__).info(
        "scancel 999999: rc=%s stdout=%r stderr=%r", rc, out, err
    )
    assert rc == 0 or "invalid job id" in err.lower()


@pytest.mark.integration
async def test_release_purged_allocation(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """Cancelled outside Horus and purged: release still cleans up."""
    target = shared_target()
    handle = await _launch(target, itest_dir, "a", "true")
    assert await poll_until(target, handle, 60) == 0
    assert handle.extra is not None
    job, alloc = handle.extra["job_id"], Path(handle.extra["alloc_dir"])
    rc, _out, err = await sh(target, f"scancel {job}")
    assert rc == 0, err

    async def purged() -> bool:
        rc, _out, err = await sh(target, f"scontrol show job {job}")
        return rc != 0 and "invalid job id" in err.lower()

    await wait_for(purged, 60)
    await target.release("idle")
    assert not target.is_live
    assert not (alloc / "spool").exists()
    assert not (alloc / "run").exists()
    assert (alloc / "pilot.log").exists()
