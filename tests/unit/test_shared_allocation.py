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

import signal as signal_mod
import subprocess
from pathlib import Path
from typing import Any

import pytest
from horus_runtime.core.resources import ResourceRequest
from horus_runtime.core.target.channel import JobHandle

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
    target: SlurmTarget, tmp_path: Path, name: str, **resources: Any
) -> JobHandle:
    task = _Task(
        str(tmp_path / name),
        ResourceRequest(**resources) if resources else None,
        id=name,
    )
    target.bind(task)  # type: ignore[arg-type]
    return await target.launch(
        "echo hi", cwd=task.working_dir, env=None, job_dir=f"{tmp_path}/{name}"
    )


def _spooled(inner: FakeInner) -> list[Path]:
    root = Path(str(inner.working_directory)) / ALLOC_DIR
    return sorted(root.glob("*/spool/*.tmp"))


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
        launchers = [p.read_text() for p in _spooled(inner)]
        assert len(launchers) == 2
        # The task's own request, not the allocation's size.
        assert any("--cpus-per-task=2 --mem=4096M" in t for t in launchers)
        # No request: one CPU and its even share of memory.
        assert any("--cpus-per-task=1 --mem=2048M" in t for t in launchers)
        assert len(inner.commands_starting("mv ")) == 2

    async def test_pilot_is_sized_by_the_target(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The allocation asks for the target's fields, not a task's."""
        await _launch(_shared(inner, partition="cpu"), tmp_path, "a", cpus=2)
        pilot = next((tmp_path / ALLOC_DIR).glob("*/pilot.sh")).read_text()
        assert "#SBATCH --job-name=horus-hpc" in pilot
        assert "#SBATCH --cpus-per-task=8" in pilot
        assert "#SBATCH --mem=16G" in pilot
        assert "#SBATCH --time=02:00:00" in pilot
        assert "#SBATCH --partition=cpu" in pilot
        assert 'mv "$f" run/' in pilot

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
        await _launch(target, tmp_path, "b")
        assert len(inner.commands_starting("sbatch")) == 2
        assert target.generation == 2

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
            calls.append(len(_spooled(inner)))

        target.on_ready(ready)
        await _launch(target, tmp_path, "a")
        await _launch(target, tmp_path, "b")
        assert calls == [0]

    async def test_cancel_signals_only_the_step(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The allocation the other tasks share is left alone."""
        inner.responds(
            **{
                "squeue -s": FakeProcess(
                    stdout=b"12345.0|horus-a\n12345.1|horus-b\n"
                )
            },
            squeue=RUNNING,
        )
        target = _shared(inner)
        handle = await _launch(target, tmp_path, "b")
        await target.send_signal(handle, signal_mod.SIGTERM)

        assert inner.commands_starting("scancel") == ["scancel 12345.1"]
        assert (tmp_path / "b" / "cancel").exists()


@pytest.mark.unit
class TestLauncher:
    """The launcher always leaves an exit code behind."""

    def _run(self, tmp_path: Path, launcher: str, srun: str) -> str:
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        fake = bin_dir / "srun"
        fake.write_text(f"#!/bin/bash\n{srun}\n")
        fake.chmod(0o755)
        subprocess.run(
            ["bash", "-c", launcher],
            check=True,
            env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
        )
        return (tmp_path / "a" / "exit_code").read_text().strip()

    async def _launcher(
        self, inner: FakeInner, tmp_path: Path, cmd: str = "exit 3"
    ) -> str:
        inner.responds(squeue=RUNNING)
        target = _shared(inner)
        task = _Task(str(tmp_path / "a"), id="a")
        target.bind(task)  # type: ignore[arg-type]
        await target.launch(
            cmd, cwd=task.working_dir, env=None, job_dir=f"{tmp_path}/a"
        )
        return _spooled(inner)[0].read_text()

    async def test_the_step_writes_its_own_exit_code(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The step runs the script, whose exit code is kept."""
        launcher = await self._launcher(inner, tmp_path)
        assert self._run(tmp_path, launcher, 'exec bash "${@: -1}"') == "3"

    async def test_a_failed_srun_still_leaves_an_exit_code(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """A step that never ran must not leave poll waiting forever."""
        launcher = await self._launcher(inner, tmp_path)
        assert self._run(tmp_path, launcher, "exit 9") == "9"

    async def test_a_cancelled_step_never_starts(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Cancelled while spooled: recorded, srun never called."""
        launcher = await self._launcher(inner, tmp_path)
        (tmp_path / "a" / "cancel").touch()
        assert self._run(tmp_path, launcher, "exit 99") == "143"


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
