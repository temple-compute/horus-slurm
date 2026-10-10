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

import json
import signal as signal_mod
from pathlib import Path
from typing import Any

import pytest
from horus_builtin.target.local import LocalTarget
from horus_runtime.context import HorusContext
from horus_runtime.core.resources import ResourceRequest
from horus_runtime.core.target.channel import JobHandle
from horus_runtime.core.task.exceptions import TaskExecutionError
from horus_runtime.event.base import BaseEvent

from horus_slurm.events import SlurmJobEvent
from horus_slurm.record import RECORD_FILE, SlurmJobRecord
from horus_slurm.resources import SlurmJobScope, resolve_resources
from horus_slurm.target import slurm as slurm_mod
from horus_slurm.target.slurm import JOB_ID_FILE, QueueState, SlurmTarget

from .conftest import FakeInner, FakeProcess


def _target(inner: FakeInner, **kwargs: Any) -> SlurmTarget:
    return SlurmTarget(
        inner=inner, working_directory=inner.working_directory, **kwargs
    )


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
            (b"PENDING|(Resources)\n", 0, QueueState("PENDING", "Resources")),
            (
                b"PENDING|(ReqNodeNotAvail, UnavailableNodes:gpu[01-02])\n",
                0,
                QueueState(
                    "PENDING", "ReqNodeNotAvail, UnavailableNodes:gpu[01-02]"
                ),
            ),
            # Slurm's "no reason" is not a reason.
            (b"PENDING|(None)\n", 0, QueueState("PENDING")),
            # Unparenthesised %R is where a running job is.
            (b"RUNNING|node01\n", 0, QueueState("RUNNING", nodes="node01")),
            (b"", 0, QueueState("")),
            # A blip must be "don't know", never "gone": the caller treats
            # gone as a failure.
            (b"", 1, None),
            # %P, when present, is the partition the job is queued on.
            (
                b"PENDING|(Resources)|gpu\n",
                0,
                QueueState("PENDING", "Resources", partition="gpu"),
            ),
            # Array elements report one line each; the first is enough.
            (
                b"RUNNING|node01\nRUNNING|node02\n",
                0,
                QueueState("RUNNING", nodes="node01"),
            ),
        ],
    )
    async def test_squeue_output_is_read_correctly(
        self,
        tmp_path: Path,
        stdout: bytes,
        returncode: int,
        expected: QueueState | None,
    ) -> None:
        """Present, gone, and unknown are three different answers."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=stdout, returncode=returncode)
        )
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        assert await _target(inner)._queue_state(handle) == expected


def _read_record(task: _Task) -> SlurmJobRecord:
    """Read back the record a target wrote for *task*."""
    raw = Path(task.side_artifacts_dir, RECORD_FILE).read_bytes()
    return SlurmJobRecord.model_validate(json.loads(raw))


@pytest.mark.unit
class TestJobRecord:
    """
    The durable side-product record, independent of the (truncatable) event
    stream.
    """

    async def test_launch_writes_a_record_with_the_submitted_state(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """
        The script and resolved sbatch options are captured at submit time.
        """
        task = _Task(str(tmp_path))
        target = _target(inner, partition="gpu", mem="8G")
        _bind(target, task)

        await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        record = _read_record(task)
        assert record.job_id == "12345"
        assert record.state == "SUBMITTED"
        assert [s.state for s in record.states] == ["SUBMITTED"]
        assert "echo hi" in record.script
        assert record.sbatch["partition"] == "gpu"
        assert record.sbatch["mem"] == "8G"
        assert record.exit_code is None

    async def test_polling_appends_states_without_duplicating_them(
        self, tmp_path: Path
    ) -> None:
        """
        Mirrors the bus's own dedupe (``TestJobEvents``): a repeated state
        must not grow the record's history.
        """
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n"),
            squeue=FakeProcess(stdout=b"PENDING\n"),
        )
        target = _target(inner)
        _bind(target, task)
        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        await target.poll(handle)
        await target.poll(handle)  # still PENDING: no new entry
        inner.responds(squeue=FakeProcess(stdout=b"RUNNING\n"))
        await target.poll(handle)

        record = _read_record(task)
        assert [s.state for s in record.states] == [
            "SUBMITTED",
            "PENDING",
            "RUNNING",
        ]

    async def test_a_recorded_exit_code_is_persisted(
        self, tmp_path: Path
    ) -> None:
        """An observer reading the record alone can tell the job is done."""
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n"),
            squeue=FakeProcess(stdout=b""),
        )
        target = _target(inner)
        _bind(target, task)
        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )
        (Path(handle.job_dir) / "exit_code").write_text("0")

        exit_code = await target.poll(handle)

        assert exit_code == 0
        assert _read_record(task).exit_code == 0

    async def test_reason_and_nodes_are_recorded(self, tmp_path: Path) -> None:
        """The record says why a job waits, and where it then runs."""
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n"),
            squeue=FakeProcess(stdout=b"PENDING|(Resources)\n"),
        )
        target = _target(inner)
        _bind(target, task)
        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        await target.poll(handle)
        record = _read_record(task)
        assert (record.reason, record.nodes) == ("Resources", None)
        assert record.states[-1].reason == "Resources"

        inner.responds(squeue=FakeProcess(stdout=b"RUNNING|node01\n"))
        await target.poll(handle)
        record = _read_record(task)
        assert (record.state, record.reason, record.nodes) == (
            "RUNNING",
            None,
            "node01",
        )

    def test_an_old_record_without_the_new_fields_still_parses(self) -> None:
        """Records written by horus-slurm 0.4.0 must keep validating."""
        record = SlurmJobRecord.model_validate(
            {
                "job_id": "1",
                "state": "PENDING",
                "states": [{"state": "PENDING", "at": "2026-01-01T00:00:00Z"}],
                "script": "",
                "script_path": "job.sh",
                "working_dir": "/w",
                "job_dir": "/w/j",
                "stdout_path": "/w/j/stdout.log",
                "stderr_path": "/w/j/stderr.log",
            }
        )

        assert record.reason is None
        assert record.nodes is None
        assert record.partition_nodes is None
        assert record.states[0].reason is None

    async def test_a_failed_write_never_breaks_launch_or_poll(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Recording is strictly best-effort, like every other observation this
        target makes: a broken filesystem must not fail the job.
        """
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n"),
            squeue=FakeProcess(stdout=b"PENDING\n"),
        )
        target = _target(inner)
        _bind(target, task)

        real_put_file = LocalTarget.put_file

        async def flaky_put_file(
            self: FakeInner, content: bytes | Path, remote_path: str
        ) -> None:
            # Only the record write is allowed to fail -- everything else
            # (the sbatch script, the job-id marker) must still land, or the
            # test would not be exercising "recording is best-effort" at all.
            if remote_path.endswith(RECORD_FILE):
                raise OSError("disk full")
            await real_put_file(self, content, remote_path)

        # inner is a pydantic model: patch the class, not the instance --
        # pydantic's __setattr__ rejects assigning a non-field attribute.
        monkeypatch.setattr(FakeInner, "put_file", flaky_put_file)

        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )
        assert await target.poll(handle) is None


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

    async def test_a_changed_pending_reason_is_announced(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """
        Still PENDING, but now for a different reason: that is news to a user
        wondering why the job has not started.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b"PENDING|(Priority)\n")
        )
        target = _target(inner)
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "12345"}
        )

        await target.poll(handle)
        await target.poll(handle)
        inner.responds(squeue=FakeProcess(stdout=b"PENDING|(Resources)\n"))
        await target.poll(handle)
        inner.responds(squeue=FakeProcess(stdout=b"RUNNING|node01\n"))
        await target.poll(handle)

        assert [(e.state, e.reason, e.nodes) for e in emitted] == [
            ("PENDING", "Priority", None),
            ("PENDING", "Resources", None),
            ("RUNNING", None, "node01"),
        ]
        assert emitted[0].message == "Slurm job 12345 is PENDING (Priority)"
        assert emitted[-1].message == "Slurm job 12345 is RUNNING"

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

    async def test_vanishing_announces_the_accounted_end_state(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """Accounting knows what killed it; that beats a bare GONE."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b""),
            sacct=FakeProcess(stdout=b"TIMEOUT|0:0|None\n"),
        )
        target = _target(inner, exit_code_retries=1, exit_code_delay=0.0)
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "12345"}
        )

        assert await target.poll(handle) == 1
        assert [(e.state, e.reason) for e in emitted] == [("TIMEOUT", None)]

    async def test_vanishing_keeps_the_accounted_reason(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """The accounted Reason travels with the end state."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b""),
            sacct=FakeProcess(stdout=b"NODE_FAIL|0:0|NodeDown\n"),
        )
        target = _target(inner, exit_code_retries=1, exit_code_delay=0.0)
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "12345"}
        )

        assert await target.poll(handle) == 1
        assert [(e.state, e.reason) for e in emitted] == [
            ("NODE_FAIL", "NodeDown")
        ]

    async def test_vanishing_without_accounting_is_gone(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """No slurmdbd, no answer: all that is known is that it is gone."""
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            squeue=FakeProcess(stdout=b""),
            sacct=FakeProcess(returncode=1),
        )
        target = _target(inner, exit_code_retries=1, exit_code_delay=0.0)
        handle = JobHandle(
            pid=None, job_dir=str(tmp_path), extra={"job_id": "12345"}
        )

        assert await target.poll(handle) == 1
        assert [(e.state, e.reason) for e in emitted] == [("GONE", None)]

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
class TestPartitionSummary:
    """A job waiting on hardware says what state that hardware is in."""

    SINFO = b"idle|0\nmixed|1\ndrained*|1\ndrained|1\n"

    async def _pending(
        self, tmp_path: Path, **kwargs: Any
    ) -> tuple[FakeInner, SlurmTarget, JobHandle, _Task]:
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n"),
            squeue=FakeProcess(stdout=b"PENDING|(Resources)|gpu\n"),
            sinfo=FakeProcess(stdout=self.SINFO),
        )
        target = _target(inner, **kwargs)
        _bind(target, task)
        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )
        return inner, target, handle, task

    async def test_drained_nodes_surface(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """Counts are summed per base state, flags stripped."""
        inner, target, handle, task = await self._pending(tmp_path)

        await target.poll(handle)
        await target.poll(handle)

        expected = {"idle": 0, "mixed": 1, "drained": 2}
        assert emitted[-1].partition_nodes == expected
        assert _read_record(task).partition_nodes == expected
        # squeue's partition, and only once within the interval.
        assert inner.commands_starting("sinfo") == [
            'sinfo -h -p gpu -o "%T|%D"'
        ]

    async def test_the_submitted_partition_wins(self, tmp_path: Path) -> None:
        """What sbatch was told beats what squeue echoes back."""
        inner, target, handle, _task = await self._pending(
            tmp_path, partition="a100"
        )

        await target.poll(handle)

        assert inner.commands_starting("sinfo") == [
            'sinfo -h -p a100 -o "%T|%D"'
        ]

    async def test_only_a_changed_summary_is_announced(
        self,
        tmp_path: Path,
        emitted: list[SlurmJobEvent],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Re-reading the same counts is no news; a node draining is, but it is
        not a new state in the history.
        """
        monkeypatch.setattr(slurm_mod, "PARTITION_SUMMARY_INTERVAL", 0.0)
        inner, target, handle, task = await self._pending(tmp_path)

        await target.poll(handle)
        await target.poll(handle)
        assert len(inner.commands_starting("sinfo")) == 2
        assert [e.state for e in emitted] == ["SUBMITTED", "PENDING"]

        inner.responds(sinfo=FakeProcess(stdout=b"drained|3\n"))
        await target.poll(handle)

        assert [e.partition_nodes for e in emitted[1:]] == [
            {"idle": 0, "mixed": 1, "drained": 2},
            {"drained": 3},
        ]
        record = _read_record(task)
        assert record.partition_nodes == {"drained": 3}
        assert [s.state for s in record.states] == ["SUBMITTED", "PENDING"]

    async def test_a_sinfo_failure_leaves_none(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """Best effort: no summary, and the job carries on."""
        inner, target, handle, _task = await self._pending(tmp_path)
        inner.responds(sinfo=FakeProcess(returncode=1))

        assert await target.poll(handle) is None
        assert emitted[-1].state == "PENDING"
        assert emitted[-1].partition_nodes is None

    async def test_running_clears_it(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """Once the job has nodes, the partition's are beside the point."""
        inner, target, handle, task = await self._pending(tmp_path)
        await target.poll(handle)

        inner.responds(squeue=FakeProcess(stdout=b"RUNNING|gpu01|gpu\n"))
        await target.poll(handle)
        await target.poll(handle)

        assert emitted[-1].state == "RUNNING"
        assert emitted[-1].partition_nodes is None
        assert _read_record(task).partition_nodes is None
        assert len(inner.commands_starting("sinfo")) == 1

    async def test_other_reasons_do_not_ask(self, tmp_path: Path) -> None:
        """Waiting on priority says nothing about the nodes."""
        inner, target, handle, _task = await self._pending(tmp_path)
        inner.responds(squeue=FakeProcess(stdout=b"PENDING|(Priority)|gpu\n"))

        await target.poll(handle)

        assert inner.commands_starting("sinfo") == []


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

        assert inner.commands_starting("scancel") == ["scancel 999"]

    async def test_a_cancel_is_recorded_straight_away(
        self, tmp_path: Path, emitted: list[SlurmJobEvent]
    ) -> None:
        """
        Slurm can take a while to wind a job down; until it reports how the
        job ended, the record says a cancel is under way.
        """
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n")
        )
        target = _target(inner)
        _bind(target, task)
        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )

        await target.send_signal(handle, signal_mod.SIGKILL)

        assert _read_record(task).state == "CANCELLING"
        assert emitted[-1].state == "CANCELLING"

    async def test_a_cancelled_job_ends_with_its_accounted_state(
        self, tmp_path: Path
    ) -> None:
        """
        A cancelled job never writes an exit code: sacct says how it ended,
        without waiting out the exit-code retries.
        """
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n"),
            squeue=FakeProcess(stdout=b""),
            sacct=FakeProcess(stdout=b"CANCELLED by 1000\n"),
        )
        target = _target(inner, exit_code_delay=3600.0)
        _bind(target, task)
        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )
        await target.send_signal(handle, signal_mod.SIGKILL)

        assert await target.poll(handle) == 1
        assert [s.state for s in _read_record(task).states][-2:] == [
            "CANCELLING",
            "CANCELLED",
        ]

    async def test_a_cancel_without_accounting_still_ends_cancelled(
        self, tmp_path: Path
    ) -> None:
        """A cluster without slurmdbd cannot answer sacct; it was cancelled."""
        task = _Task(str(tmp_path))
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sbatch=FakeProcess(stdout=b"12345\n"),
            squeue=FakeProcess(stdout=b""),
            sacct=FakeProcess(returncode=1),
        )
        target = _target(inner, exit_code_delay=3600.0)
        _bind(target, task)
        handle = await target.launch(
            "echo hi", cwd=str(tmp_path), env=None, job_dir=str(tmp_path)
        )
        await target.send_signal(handle, signal_mod.SIGTERM)

        assert await target.poll(handle) == 1
        assert _read_record(task).state == "CANCELLED"

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
        self, inner: FakeInner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        A queued job has not started writing yet, so the logs are not even
        read: an SSH transport surfaces a missing file as an error, and that
        must not spam the UI every poll while the job sits in the queue.
        """
        handle = JobHandle(pid=None, job_dir=str(tmp_path / "nope"))
        reads: list[str] = []

        async def fail_get_file(_self: FakeInner, path: str) -> bytes:
            reads.append(path)
            raise FileNotFoundError(path)

        monkeypatch.setattr(FakeInner, "get_file", fail_get_file)

        assert await _target(inner).read_output(handle) == (b"", b"")
        assert reads == []

    async def test_a_transient_log_read_failure_is_not_an_error(
        self, inner: FakeInner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        A log that exists but cannot be read right now reads as empty; the
        next poll retries it.
        """
        (tmp_path / "stdout.log").write_text("out")
        handle = JobHandle(pid=None, job_dir=str(tmp_path))

        async def fail_get_file(_self: FakeInner, _path: str) -> bytes:
            raise OSError("connection lost")

        monkeypatch.setattr(FakeInner, "get_file", fail_get_file)

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
