#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Unit tests for a shared (named) SlurmTarget: one allocation, one job step
per task.
"""

import asyncio
import os
import signal as signal_mod
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
from horus_runtime.core.resources import ResourceRequest
from horus_runtime.core.target.channel import JobHandle
from horus_runtime.core.task.exceptions import TaskExecutionError
from pydantic import ValidationError

from horus_slurm.events import SlurmAllocationEvent
from horus_slurm.target import slurm as slurm_mod
from horus_slurm.target.slurm import ALLOC_DIR, SlurmTarget

from .conftest import FakeInner, FakeProcess

RUNNING = FakeProcess(stdout=b"RUNNING|node01|cpu\n")


class _Task:
    """The only things the target needs from a task."""

    def __init__(
        self,
        working_dir: str,
        resources: ResourceRequest | None = None,
        *,
        id: str = "task-1",
    ) -> None:
        self.id = id
        self.working_dir = working_dir
        self.resources = resources

    @property
    def side_artifacts_dir(self) -> str:
        """Mirrors ``BaseTask.side_artifacts_dir``."""
        return f"{self.working_dir}/side-artifacts"


def _shared(inner: FakeInner, **kwargs: Any) -> SlurmTarget:
    # The file plumbing runs for real; Slurm commands stay canned.
    inner.runs("umask ", "mv ", "touch ", "rm ")
    fields: dict[str, Any] = {
        "cpus_per_task": 8,
        "mem": "16G",
        "time_limit": "02:00:00",
    }
    target = SlurmTarget(
        inner=inner,
        working_directory=inner.working_directory,
        **(fields | kwargs),
    )
    target._ref = "hpc"
    return target


async def _launch(
    target: SlurmTarget,
    tmp_path: Path,
    name: str,
    cmd: str = "echo hi",
    **resources: Any,
) -> JobHandle:
    task = _Task(
        str(tmp_path / name),
        ResourceRequest(**resources) if resources else None,
        id=name,
    )
    target.bind(task)  # type: ignore[arg-type]
    return await target.launch(
        cmd, cwd=task.working_dir, env=None, job_dir=f"{tmp_path}/{name}"
    )


def _alloc_dirs(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / ALLOC_DIR).iterdir())


def _spooled(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / ALLOC_DIR).glob("*/spool/*.sh"))


@pytest.mark.unit
class TestSharing:
    """Which targets share an allocation."""

    def test_only_a_named_target_is_shared(self, inner: FakeInner) -> None:
        """An inline target keeps one sbatch per task."""
        inline = SlurmTarget(inner=inner)
        assert not inline.releasable
        assert inline.hold_timeout_s is None
        named = _shared(inner)
        assert named.releasable
        assert named.hold_timeout_s == 300.0
        assert named.keeps_data_on_release


@pytest.mark.unit
class TestAllocation:
    """One allocation, submitted once, its tasks handed in as steps."""

    async def test_two_tasks_submit_one_allocation(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Copies share the allocation; each task gets its own step."""
        inner.responds(squeue=RUNNING)
        target = _shared(inner)
        a = await _launch(target, tmp_path, "a", cpus=2)
        b = await _launch(target.idle_copy(), tmp_path, "b")

        assert len(inner.commands_starting("sbatch")) == 1
        assert a.extra is not None and b.extra is not None
        assert a.extra["job_id"] == b.extra["job_id"] == "12345"
        assert a.extra["step"] != b.extra["step"]
        assert target.is_live and target.generation == 1
        launchers = [p.read_text() for p in _spooled(tmp_path)]
        assert len(launchers) == 2
        # The task's own request, not the allocation's size.
        assert any("--cpus-per-task=2 --mem=4096M" in t for t in launchers)
        # No request: one CPU and its even share of memory.
        assert any("--cpus-per-task=1 --mem=2048M" in t for t in launchers)

    async def test_the_allocation_dir_is_private(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Only its owner may write where the pilot runs scripts from."""
        await _launch(_shared(inner), tmp_path, "a")
        (alloc,) = _alloc_dirs(tmp_path)
        assert alloc.stat().st_mode & 0o777 == 0o700
        assert (tmp_path / ALLOC_DIR).stat().st_mode & 0o777 == 0o700
        # The job script sits there too, not in the task's job dir.
        assert list((alloc / "run").glob("*.job"))

    async def test_an_existing_allocation_dir_is_refused(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Somebody else's directory is never used as the spool."""
        target = _shared(inner)
        planted = tmp_path / ALLOC_DIR / f"{target.lease_key}-1"
        planted.mkdir(parents=True)
        with pytest.raises(TaskExecutionError, match="allocation directory"):
            await _launch(target, tmp_path, "a")
        assert inner.commands_starting("sbatch") == []

    async def test_pilot_is_sized_by_the_target(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The allocation asks for the target's fields, not a task's."""
        await _launch(_shared(inner, partition="cpu"), tmp_path, "a", cpus=2)
        pilot = (_alloc_dirs(tmp_path)[0] / "pilot.sh").read_text()
        assert "#SBATCH --job-name=horus-hpc" in pilot
        assert "#SBATCH --cpus-per-task=8" in pilot
        assert "#SBATCH --mem=16G" in pilot
        assert "#SBATCH --time=02:00:00" in pilot
        assert "#SBATCH --partition=cpu" in pilot
        assert "#SBATCH --no-requeue" in pilot

    async def test_a_step_takes_no_gpu_it_did_not_ask_for(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Else it would hold every GPU of the allocation."""
        inner.responds(squeue=RUNNING)
        target = _shared(inner, gres="gpu:2")
        await _launch(target, tmp_path, "a")
        await _launch(target, tmp_path, "b", gpus=1)
        launchers = [p.read_text() for p in _spooled(tmp_path)]
        assert any("--gres=none" in t for t in launchers)
        assert any("--gres=gpu:1" in t for t in launchers)

    async def test_no_memory_share_falls_back_to_shared_memory(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """--mem=0: the job's memory, not held from other steps."""
        await _launch(_shared(inner, mem="0"), tmp_path, "a")
        assert "--mem=0 " in _spooled(tmp_path)[0].read_text()

    async def test_an_odd_job_id_is_refused(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """A job id goes into later commands, so it must look like one."""
        inner.responds(sbatch=FakeProcess(stdout=b"12345 && id\n"))
        with pytest.raises(TaskExecutionError, match="unexpected job id"):
            await _launch(_shared(inner), tmp_path, "a")

    async def test_release_cancels_and_the_next_task_resubmits(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """A released target submits a new allocation on demand."""
        inner.responds(squeue=RUNNING)
        target = _shared(inner)
        await _launch(target, tmp_path, "a")
        await target.release("idle_cap")

        assert inner.commands_starting("scancel") == ["scancel 12345"]
        assert not target.is_live
        assert not (_alloc_dirs(tmp_path)[0] / "spool").exists()
        await _launch(target, tmp_path, "b")
        assert len(inner.commands_starting("sbatch")) == 2
        assert target.generation == 2

    async def test_releasing_an_allocation_slurm_forgot_succeeds(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """It ended on its own and was purged: nothing left to cancel."""
        inner.responds(
            scancel=FakeProcess(
                stderr=b"scancel: error: Invalid job id specified\n",
                returncode=1,
            )
        )
        target = _shared(inner)
        await _launch(target, tmp_path, "a")
        await target.release("done")
        assert not target.is_live

    async def test_an_ended_allocation_is_replaced(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """One that left the queue on its own (time limit) is resubmitted."""
        target = _shared(inner)
        await _launch(target, tmp_path, "a")
        await _launch(target, tmp_path, "b")  # squeue: gone
        assert len(inner.commands_starting("sbatch")) == 2

    async def test_ready_callbacks_run_once_per_allocation(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Before any step, once per submitted allocation."""
        inner.responds(squeue=RUNNING)
        target = _shared(inner)
        calls: list[int] = []

        async def ready() -> None:
            calls.append(len(_spooled(tmp_path)))

        target.on_ready(ready)
        await _launch(target, tmp_path, "a")
        await _launch(target, tmp_path, "b")
        assert calls == [0]

    async def test_a_slow_callback_does_not_hold_up_a_release(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The callbacks run outside the lock a release needs."""
        target = _shared(inner)
        restoring = asyncio.Event()
        finish = asyncio.Event()

        async def ready() -> None:
            restoring.set()
            await finish.wait()

        target.on_ready(ready)
        launch = asyncio.create_task(_launch(target, tmp_path, "a"))
        await restoring.wait()
        await asyncio.wait_for(target.release("abort"), timeout=1)
        finish.set()
        await launch

    async def test_keepalive_touches_the_heartbeat(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The pilot's proof that the orchestrator is still there."""
        target = _shared(inner)
        await _launch(target, tmp_path, "a")
        heartbeat = _alloc_dirs(tmp_path)[0] / "heartbeat"
        os.utime(heartbeat, (0, 0))
        await target.keepalive()
        assert heartbeat.stat().st_mtime > 0

    async def test_cancel_takes_a_spooled_step_back(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Not started yet: never starts, and is recorded at once."""
        target = _shared(inner)
        handle = await _launch(target, tmp_path, "b")
        await target.send_signal(handle, signal_mod.SIGTERM)

        assert _spooled(tmp_path) == []
        assert (tmp_path / "b" / "exit_code").read_text().strip() == "143"
        assert list(_alloc_dirs(tmp_path)[0].glob("run/*.cancel"))
        # The allocation the other tasks share is left alone.
        assert inner.commands_starting("scancel") == []

    async def test_other_signals_reach_only_the_step(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Found by name among the allocation's steps."""
        inner.responds(
            **{
                "squeue -s": FakeProcess(
                    stdout=b"12345.0|horus-a\n12345.1|horus-b\n"
                )
            },
        )
        target = _shared(inner)
        handle = await _launch(target, tmp_path, "b")
        await target.send_signal(handle, signal_mod.SIGUSR1)
        assert inner.commands_starting("scancel") == [
            "scancel --signal=USR1 12345.1"
        ]


@pytest.mark.unit
class TestScripts:
    """The launcher and the pilot, run in bash with a fake srun."""

    def _bin(self, tmp_path: Path, srun: str) -> dict[str, str]:
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir(exist_ok=True)
        fake = bin_dir / "srun"
        fake.write_text(f"#!/bin/bash\n{srun}\n")
        fake.chmod(0o755)
        return {"PATH": f"{bin_dir}:/usr/bin:/bin"}

    async def _launched(
        self, inner: FakeInner, tmp_path: Path, cmd: str = "exit 3"
    ) -> tuple[SlurmTarget, JobHandle, Path]:
        target = _shared(inner)
        handle = await _launch(target, tmp_path, "a", cmd=cmd)
        return target, handle, _spooled(tmp_path)[0]

    def _exit_code(self, tmp_path: Path) -> str:
        return (tmp_path / "a" / "exit_code").read_text().strip()

    async def test_the_step_writes_its_own_exit_code(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Srun runs the script, whose exit code is kept."""
        _t, _h, launcher = await self._launched(inner, tmp_path)
        env = self._bin(tmp_path, 'exec bash "${@: -1}"')
        subprocess.run(["bash", str(launcher)], check=True, env=env)
        assert self._exit_code(tmp_path) == "3"

    async def test_a_failed_srun_still_leaves_an_exit_code(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """A step that never ran must not leave poll waiting forever."""
        _t, _h, launcher = await self._launched(inner, tmp_path)
        env = self._bin(tmp_path, "exit 9")
        subprocess.run(["bash", str(launcher)], check=True, env=env)
        assert self._exit_code(tmp_path) == "9"

    async def test_a_cancel_stops_a_waiting_or_running_step(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The launcher watches the mark while srun waits or runs."""
        target, handle, launcher = await self._launched(inner, tmp_path)
        env = self._bin(tmp_path, "trap 'exit 143' TERM; sleep 30 & wait")
        # Claimed, as the pilot would, so the cancel cannot take it back.
        claimed = launcher.parent.parent / "run" / launcher.name
        launcher.rename(claimed)
        proc = subprocess.Popen(["bash", str(claimed)], env=env)
        await asyncio.sleep(0.5)
        await target.send_signal(handle, signal_mod.SIGTERM)
        assert proc.wait(timeout=10) == 0
        assert self._exit_code(tmp_path) == "143"

    async def test_the_pilot_runs_steps_and_ends_when_orphaned(
        self, inner: FakeInner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spooled steps run; a stale heartbeat when idle ends it."""
        monkeypatch.setattr(
            slurm_mod,
            "PILOT_LOOP",
            slurm_mod.PILOT_LOOP.replace("-gt 900", "-gt 1"),
        )
        target, _h, launcher = await self._launched(inner, tmp_path, "true")
        alloc = launcher.parent.parent
        pilot = target._pilot_script(str(alloc))
        old = time.time() - 60
        os.utime(alloc / "heartbeat", (old, old))
        env = self._bin(tmp_path, 'exec bash "${@: -1}"')
        subprocess.run(["bash", "-c", pilot], check=True, env=env, timeout=20)
        assert self._exit_code(tmp_path) == "0"
        assert list((alloc / "run").glob("*.sh"))


@pytest.mark.unit
class TestValidation:
    """A shared allocation is sized by its own fields."""

    def test_named_target_needs_a_size(self, inner: FakeInner) -> None:
        """cpus, mem and time limit are required; one node only."""
        target = _shared(inner, cpus_per_task=None, mem=None, nodes=2)
        errors = target.validate_named()
        assert "cpus_per_task, mem" in errors[0]
        assert "one node" in errors[1]
        assert _shared(inner).validate_named() == []

    def test_a_task_bigger_than_the_allocation_is_rejected(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """CPUs, memory and walltime are checked."""
        target = _shared(inner)
        task = _Task(
            str(tmp_path),
            ResourceRequest(cpus=16, memory_gb=8, walltime="3h"),
        )
        errors = target.validate_member(task)  # type: ignore[arg-type]
        assert len(errors) == 2
        assert "16 CPUs" in errors[0]
        assert "3h walltime" in errors[1]
        fits = _Task(str(tmp_path), ResourceRequest(cpus=8, walltime="1h"))
        assert target.validate_member(fits) == []  # type: ignore[arg-type]

    def test_zero_means_no_limit(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """mem=0 is all of the node's memory to Slurm, not none."""
        target = _shared(inner, mem="0")
        task = _Task(str(tmp_path), ResourceRequest(memory_gb=64))
        assert target.validate_member(task) == []  # type: ignore[arg-type]


@pytest.mark.unit
class TestAllocationEventModel:
    """The wire shape a host reads, like a terraform node event."""

    def test_round_trip(self) -> None:
        """Every field a host reads is in the dump."""
        event = SlurmAllocationEvent(
            phase="queued",
            task_id="a",
            node_key="k",
            target_ref="hpc",
            generation=1,
            member_task_ids=["a"],
            job_id="12345",
        )
        data = event.model_dump(mode="json")
        assert data["event_type"] == "slurm_allocation_event"
        assert {
            k: data[k]
            for k in (
                "phase",
                "task_id",
                "node_key",
                "target_ref",
                "generation",
                "member_task_ids",
                "job_id",
                "reason",
            )
        } == {
            "phase": "queued",
            "task_id": "a",
            "node_key": "k",
            "target_ref": "hpc",
            "generation": 1,
            "member_task_ids": ["a"],
            "job_id": "12345",
            "reason": None,
        }
        assert SlurmAllocationEvent.model_validate(data) == event

    def test_an_unknown_phase_is_refused(self) -> None:
        """A phase the host does not know never leaves the target."""
        with pytest.raises(ValidationError):
            SlurmAllocationEvent(
                phase="releasing",  # type: ignore[arg-type]
                task_id="",
                node_key="k",
                target_ref="hpc",
                generation=1,
                member_task_ids=[],
            )
