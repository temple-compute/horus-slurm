#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Unit tests for SlurmTarget: submission, polling, signalling, and the
resource scope it declares.
"""

import signal as signal_mod
from pathlib import Path
from typing import Any

import pytest
from horus_runtime.core.target.channel import JobHandle
from horus_runtime.core.task.exceptions import TaskExecutionError

from horus_slurm.resources import SlurmJobScope
from horus_slurm.target.slurm import JOB_ID_FILE, SlurmTarget

from .conftest import FakeInner, FakeProcess


def _target(inner: FakeInner, **kwargs: Any) -> SlurmTarget:
    return SlurmTarget(
        inner=inner, working_directory=inner.working_directory, **kwargs
    )


class _Task:
    """The only thing the scope needs from a task."""

    def __init__(self, working_dir: str) -> None:
        self.working_dir = working_dir


@pytest.mark.unit
class TestSubmission:
    """What sbatch is told, and what comes back."""

    async def test_launch_returns_the_job_id_without_faking_a_pid(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """
        A Slurm job id is not a process id, so it travels in `extra` and
        `pid` stays None. Anything reading `pid` would be interpreting the
        number on the wrong host entirely.
        """
        handle = await _target(inner).launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        assert handle.pid is None
        assert (handle.extra or {})["job_id"] == "12345"

    async def test_launch_records_the_job_id_for_an_observer(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """
        The scope is declared before submission, so the id has to be left
        somewhere a probe can pick it up once the job exists.
        """
        await _target(inner).launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        assert (tmp_path / JOB_ID_FILE).read_text().strip() == "12345"

    async def test_federated_id_is_reduced_to_the_job(
        self, tmp_path: Path
    ) -> None:
        """``--parsable`` prints ``<jobid>;<cluster>`` when federated."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"777;cluster\n")
        )
        handle = await _target(inner).launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        assert (handle.extra or {})["job_id"] == "777"
        assert (handle.extra or {})["raw_job_id"] == "777;cluster"

    async def test_failed_submission_raises(self, tmp_path: Path) -> None:
        """A rejected submission fails the task rather than hanging."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stderr=b"bad partition", returncode=1)
        )
        with pytest.raises(TaskExecutionError, match="bad partition"):
            await _target(inner).launch(
                "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
            )

    def test_script_carries_the_sbatch_directives(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Every configured option reaches the script."""
        target = _target(
            inner,
            partition="gpu",
            account="acct",
            qos="high",
            mem="8G",
            time_limit="01:00:00",
            gres="gpu:1",
            cpus_per_task=4,
            extra_sbatch_args=["--exclusive"],
        )
        script = target._build_sbatch_script(
            "run me", cwd=str(tmp_path), env={"K": "v"}, job_dir=str(tmp_path)
        )

        for expected in (
            "#SBATCH --partition=gpu",
            "#SBATCH --account=acct",
            "#SBATCH --qos=high",
            "#SBATCH --mem=8G",
            "#SBATCH --time=01:00:00",
            "#SBATCH --gres=gpu:1",
            "#SBATCH --cpus-per-task=4",
            "#SBATCH --exclusive",
            "export K=v",
        ):
            assert expected in script
        # The command runs *inside* the script, which is what puts the
        # resource monitor's injected sampler on the compute node.
        assert "( run me );" in script


@pytest.mark.unit
class TestResourceScope:
    """The target declares that Slurm owns the work."""

    async def test_declares_a_slurm_job_scope(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """
        Without this the task inherits ProcessTreeScope and an observer
        walks a process tree on a machine that never ran the work.
        """
        scope = await _target(inner).resource_scope(
            _Task(str(tmp_path))  # type: ignore[arg-type]
        )

        assert isinstance(scope, SlurmJobScope)
        assert scope.kind == "slurm_job"
        assert scope.job_id_file == str(tmp_path / JOB_ID_FILE)

    async def test_a_plain_target_still_defers(self, tmp_path: Path) -> None:
        """The base target adds nothing, so nothing else changes."""
        target = FakeInner(working_directory=str(tmp_path))

        assert (
            await target.resource_scope(_Task(str(tmp_path)))  # type: ignore[arg-type]
            is None
        )


@pytest.mark.unit
class TestPolling:
    """Finished, still queued, or gone without a trace."""

    async def test_exit_code_is_reported(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The marker file is the authoritative answer."""
        (tmp_path / "exit_code").write_text("0\n")
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        assert await _target(inner).poll(handle) == 0

    async def test_still_queued_reports_nothing_yet(
        self, tmp_path: Path
    ) -> None:
        """A job still in the queue is simply not finished."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"RUNNING\n")
        )
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        assert await _target(inner).poll(handle) is None

    async def test_vanished_without_an_exit_code_fails(
        self, tmp_path: Path
    ) -> None:
        """
        Gone from the queue with nothing written means the scheduler killed
        it (OOM, wall time, node failure) - a failure, not a success.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"")
        )
        target = _target(inner, exit_code_retries=1, exit_code_delay=0.0)
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        assert await target.poll(handle) == 1

    async def test_exit_code_written_late_is_still_honoured(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Slurm can flush the exit code just after the job leaves the queue;
        reading that race as a failure would fail healthy jobs.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"")
        )
        target = _target(inner, exit_code_retries=2, exit_code_delay=0.0)
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        original = target._retry_exit_code

        async def write_then_retry(path: str) -> int | None:
            """The exit code lands while we are already retrying for it."""
            (tmp_path / "exit_code").write_text("3\n")
            return await original(path)

        monkeypatch.setattr(target, "_retry_exit_code", write_then_retry)

        assert await target.poll(handle) == 3

    async def test_a_squeue_error_is_not_read_as_gone(
        self, tmp_path: Path
    ) -> None:
        """
        A scheduler hiccup must not look like a finished job, or a transient
        blip fails jobs that are still perfectly fine.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"", returncode=1)
        )
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        assert await _target(inner).poll(handle) is None


@pytest.mark.unit
class TestSignalling:
    """Cancellation goes through the scheduler, by job id."""

    async def test_scancel_uses_the_job_id(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """``scancel`` takes a job id; a pid would cancel nothing."""
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "999"}
        )
        await _target(inner).send_signal(handle, signal_mod.SIGTERM)

        assert inner.commands_starting("scancel") == [
            "scancel --signal=TERM 999"
        ]

    async def test_unknown_signal_falls_back_to_term(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """An unnameable signal still cancels rather than raising."""
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "5"}
        )
        await _target(inner).send_signal(handle, 424242)

        assert inner.commands_starting("scancel") == [
            "scancel --signal=TERM 5"
        ]


@pytest.mark.unit
class TestOutputAndDelegation:
    """Logs come off the shared filesystem; placement follows the transport."""

    async def test_reads_both_logs(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Stdout and stderr are whatever the job wrote."""
        (tmp_path / "stdout.log").write_text("out")
        (tmp_path / "stderr.log").write_text("err")
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        assert await _target(inner).read_output(handle) == (b"out", b"err")

    async def test_missing_logs_are_empty_not_an_error(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """A job that has not started yet has written nothing."""
        handle = JobHandle(pid=None, job_dir=str(tmp_path / "nope"))

        assert await _target(inner).read_output(handle) == (b"", b"")

    def test_placement_follows_the_transport(self, inner: FakeInner) -> None:
        """
        Compute nodes share the login node's filesystem, so co-location is
        the transport's answer.
        """
        target = _target(inner)

        assert target.location_id == inner.location_id
        assert target.resolved_working_directory == inner.working_directory

    async def test_file_operations_delegate(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Everything filesystem-shaped is the transport's job."""
        target = _target(inner)
        await target.mkdir(str(tmp_path / "d"))
        await target.put_file(b"x", str(tmp_path / "d" / "f"))

        assert await target.get_file(str(tmp_path / "d" / "f")) == b"x"
        assert await target.path_exists(str(tmp_path / "d" / "f"))
        assert [
            e.name for e in await target.list_dir(str(tmp_path / "d"))
        ] == ["f"]

        await target.remove(str(tmp_path / "d" / "f"))
        assert not await target.path_exists(str(tmp_path / "d" / "f"))

    async def test_control_plane_commands_bypass_the_scheduler(
        self, inner: FakeInner
    ) -> None:
        """
        Short commands run on the login node. Queueing a job to ask a
        question would wait in line to get an answer.
        """
        await _target(inner).run_command_sync("hostname")

        assert "hostname" in inner.commands
