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
from horus_runtime.context import HorusContext
from horus_runtime.core.resources import ResourceRequest
from horus_runtime.core.target.channel import JobHandle
from horus_runtime.core.task.exceptions import TaskExecutionError
from horus_runtime.event.base import BaseEvent

from horus_slurm.events import SlurmJobEvent
from horus_slurm.resources import SlurmJobScope, resolve_resources
from horus_slurm.target.slurm import JOB_ID_FILE, SlurmTarget

from .conftest import FakeInner, FakeProcess


def _target(inner: FakeInner, **kwargs: Any) -> SlurmTarget:
    return SlurmTarget(
        inner=inner, working_directory=inner.working_directory, **kwargs
    )


class _Task:
    """The only things the target needs from a task."""

    def __init__(
        self, working_dir: str, resources: ResourceRequest | None = None
    ) -> None:
        self.working_dir = working_dir
        self.resources = resources


def _bind(target: SlurmTarget, task: _Task) -> None:
    """
    Bind the stand-in task. Keeps the one type: ignore the duck type needs
    in a single place rather than at every call site.
    """
    target.bind(task)  # type: ignore[arg-type]


@pytest.fixture
def emitted(
    horus_context: HorusContext, monkeypatch: pytest.MonkeyPatch
) -> list[SlurmJobEvent]:
    """
    Collect the job events published on the bus during a test.

    Intercepting ``emit`` rather than subscribing keeps the assertion about
    what the *target* published, independent of transport or handler
    behaviour.
    """
    events: list[SlurmJobEvent] = []

    def capture(event: BaseEvent) -> None:
        if isinstance(event, SlurmJobEvent):
            events.append(event)

    monkeypatch.setattr(horus_context.bus, "emit", capture)
    return events


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
class TestResolveResources:
    """The portable request -> sbatch translation, on its own."""

    def test_nothing_declared_falls_back_to_one_cpu(self) -> None:
        """A target with no opinion, and no task request, asks the minimum."""
        resolved = resolve_resources(None)
        assert resolved.cpus_per_task == 1
        assert (resolved.mem, resolved.time_limit, resolved.gres) == (
            None,
            None,
            None,
        )

    def test_a_request_supplies_every_directive(self) -> None:
        """Each portable field lands on its sbatch counterpart."""
        resolved = resolve_resources(
            ResourceRequest(cpus=16, gpus=1, memory_gb=64, walltime="24:00:00")
        )
        assert resolved.cpus_per_task == 16
        assert resolved.mem == "64G"
        assert resolved.time_limit == "24:00:00"
        assert resolved.gres == "gpu:1"

    def test_explicit_options_win(self) -> None:
        """The site's own vocabulary is never overridden by a portable hint."""
        resolved = resolve_resources(
            ResourceRequest(
                cpus=16, gpus=1, memory_gb=64, walltime="24:00:00"
            ),
            cpus_per_task=8,
            mem="32G",
            time_limit="01:00:00",
            gres="gpu:a100:2",
        )
        assert resolved.cpus_per_task == 8
        assert resolved.mem == "32G"
        assert resolved.time_limit == "01:00:00"
        assert resolved.gres == "gpu:a100:2"

    def test_a_request_fills_only_the_gaps(self) -> None:
        """Overriding one option leaves the others derived."""
        resolved = resolve_resources(
            ResourceRequest(cpus=16, memory_gb=64), mem="32G"
        )
        assert resolved.mem == "32G"
        assert resolved.cpus_per_task == 16

    def test_no_gpu_asks_for_no_gres(self) -> None:
        """A gpus of 0 (the default) must not become `--gres=gpu:0`."""
        assert resolve_resources(ResourceRequest(cpus=4)).gres is None

    def test_vram_alone_cannot_produce_a_gres(self) -> None:
        """
        vram_gb has no portable sbatch flag, so it is dropped rather than
        guessed at -- a wrong --gres fails the submission outright.
        """
        assert resolve_resources(ResourceRequest(vram_gb=40)).gres is None


@pytest.mark.unit
class TestTaskResources:
    """A bound task's resources reach the submitted script."""

    def test_script_carries_the_tasks_resources(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """The portable request alone is enough to size the job."""
        target = _target(inner, partition="gpu")
        _bind(
            target,
            _Task(
                str(tmp_path),
                ResourceRequest(
                    cpus=16, gpus=1, memory_gb=64, walltime="24:00:00"
                ),
            ),
        )
        script = target._build_sbatch_script(
            "run me", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        for expected in (
            "#SBATCH --cpus-per-task=16",
            "#SBATCH --mem=64G",
            "#SBATCH --time=24:00:00",
            "#SBATCH --gres=gpu:1",
            "#SBATCH --partition=gpu",
        ):
            assert expected in script

    def test_the_target_overrides_the_task(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """
        The site knows things the portable request cannot express (here, that
        the partition's GPUs must be named), so its value is the one submitted.
        """
        target = _target(inner, gres="gpu:RTX6000:1", cpus_per_task=8)
        _bind(target, _Task(str(tmp_path), ResourceRequest(cpus=16, gpus=1)))
        script = target._build_sbatch_script(
            "run me", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        assert "#SBATCH --gres=gpu:RTX6000:1" in script
        assert "#SBATCH --cpus-per-task=8" in script
        assert "gpu:1" not in script

    def test_a_task_without_resources_changes_nothing(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """`resources` is optional, and omitting it is not an error."""
        target = _target(inner, mem="8G")
        _bind(target, _Task(str(tmp_path)))
        script = target._build_sbatch_script(
            "run me", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        assert "#SBATCH --mem=8G" in script
        assert "#SBATCH --cpus-per-task=1" in script

    def test_an_unbound_target_still_builds_a_script(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Nothing here may require a task: control-plane work has none."""
        script = _target(inner)._build_sbatch_script(
            "run me", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )
        assert "#SBATCH --cpus-per-task=1" in script

    def test_resolved_resources_is_inspectable_before_submitting(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """What the job will ask for is answerable without a scheduler."""
        target = _target(inner)
        _bind(target, _Task(str(tmp_path), ResourceRequest(cpus=4, gpus=2)))
        resolved = target.resolved_resources()
        assert (resolved.cpus_per_task, resolved.gres) == (4, "gpu:2")


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
class TestQueueState:
    """What ``squeue`` is understood to be saying."""

    @pytest.mark.parametrize(
        ("stdout", "returncode", "expected"),
        [
            (b"PENDING\n", 0, "PENDING"),
            (b"", 0, ""),
            # A blip must be "don't know", never "gone": the caller treats
            # gone as a failure.
            (b"", 1, None),
            # Array elements report one line each; the first is enough.
            (b"RUNNING\nRUNNING\n", 0, "RUNNING"),
        ],
    )
    async def test_squeue_output_is_read_correctly(
        self,
        tmp_path: Path,
        stdout: bytes,
        returncode: int,
        expected: str | None,
    ) -> None:
        """Present, gone, and unknown are three different answers."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=stdout, returncode=returncode)
        )
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        assert await _target(inner)._queue_state(handle) == expected


@pytest.mark.unit
class TestJobEvents:
    """The job id and its queue state reach the bus."""

    async def test_submission_announces_the_job_id(
        self, inner: FakeInner, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """
        The id is otherwise only on the cluster filesystem, so this event is
        the only way an observer learns which job to look at.
        """
        await _target(inner).launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        assert [(e.job_id, e.state) for e in emitted] == [
            ("12345", "SUBMITTED")
        ]

    async def test_only_transitions_are_announced(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """
        Polling happens every few seconds for the life of a job; a consumer
        that persists events must not get one row per poll.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"PENDING\n")
        )
        target = _target(inner)
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "12345"}
        )

        await target.poll(handle)
        await target.poll(handle)
        inner.responds(squeue=FakeProcess(stdout=b"RUNNING\n"))
        await target.poll(handle)
        await target.poll(handle)

        assert [e.state for e in emitted] == ["PENDING", "RUNNING"]

    async def test_vanishing_without_an_exit_code_is_announced(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """A job killed by the scheduler leaves no other trace."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"")
        )
        target = _target(inner, exit_code_retries=1, exit_code_delay=0.0)
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "12345"}
        )

        assert await target.poll(handle) == 1
        assert [e.state for e in emitted] == ["GONE"]

    async def test_announcing_never_breaks_a_poll(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Outside a Horus context there is no bus at all (the plain CLI), and a
        job must not care.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"PENDING\n")
        )
        monkeypatch.setattr(
            HorusContext,
            "get_context",
            staticmethod(lambda: (_ for _ in ()).throw(LookupError())),
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
