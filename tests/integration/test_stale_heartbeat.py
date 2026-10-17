#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Case 5: an idle pilot ends its allocation once the heartbeat goes stale.
"""

import asyncio
import contextlib
import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from horus_slurm.target import slurm as slurm_mod
from horus_slurm.target.slurm import SlurmTarget

from .conftest import _launch, poll_until, sh, wait_for


@pytest.fixture(autouse=True)
def _short_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    # PILOT_LOOP is built at import, so patching HEARTBEAT_STALE_S does
    # nothing.
    loop = slurm_mod.PILOT_LOOP.replace("-gt 900", "-gt 10")
    assert loop != slurm_mod.PILOT_LOOP
    monkeypatch.setattr(slurm_mod, "PILOT_LOOP", loop)


async def _state(target: SlurmTarget, job: str) -> str:
    _rc, out, _err = await sh(target, f"squeue -h -j {job} -o %T")
    return out.strip()


@pytest.mark.integration
async def test_stale_heartbeat_ends_allocation(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """Idle with a stale heartbeat: the pilot exits 0."""
    target = shared_target()
    handle = await _launch(target, itest_dir, "a", "true")
    assert await poll_until(target, handle, 60) == 0
    assert handle.extra is not None
    job, alloc = handle.extra["job_id"], Path(handle.extra["alloc_dir"])
    old = time.time() - 60
    os.utime(alloc / "heartbeat", (old, old))

    async def gone() -> bool:
        return not await _state(target, job)

    await wait_for(gone, 20)

    async def accounted() -> bool:
        _rc, out, _err = await sh(
            target, f"sacct -n -X -j {job} -o state,exitcode"
        )
        return out.split() == ["COMPLETED", "0:0"]

    await wait_for(accounted, 15)


@pytest.mark.integration
async def test_keepalive_holds_allocation(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """Idle but kept alive: the allocation outlives the stale window."""
    target = shared_target()
    handle = await _launch(target, itest_dir, "a", "true")

    async def beat() -> None:
        while True:
            await target.keepalive()
            await asyncio.sleep(2)

    # From launch on, so the pilot never sees a heartbeat older than 10 s.
    beating = asyncio.create_task(beat())
    try:
        assert await poll_until(target, handle, 60) == 0
        await asyncio.sleep(15)
        job = target._handle_job_id(handle)
        assert await _state(target, job) == "RUNNING"
    finally:
        beating.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beating
