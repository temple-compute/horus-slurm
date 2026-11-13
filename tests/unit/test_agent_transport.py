#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
SlurmTarget over a transport with its own Slurm ops (a horus-agent that runs
approved releases only): Slurm runs through those ops, never as a command
line over the transport's shell.
"""

import signal as signal_mod
from pathlib import Path
from typing import Any

import pytest
from horus_runtime.core.target.base import BaseTarget
from horus_runtime.core.target.channel import JobHandle
from horus_runtime.core.task.exceptions import TaskExecutionError
from pydantic import PrivateAttr

from horus_slurm.target.slurm import JOB_ID_FILE, SlurmTarget

from .conftest import FakeInner, FakeProcess
from .test_slurm_target import _bind, _Task


class FakeAgent(FakeInner):
    """A transport with ``slurm_*`` ops, as ``AgentTarget`` has."""

    strict: bool = True

    _calls: list[tuple[str, dict[str, Any]]] = PrivateAttr(
        default_factory=list
    )
    _status: dict[str, Any] = PrivateAttr(default_factory=dict)

    def status(self, **status: Any) -> "FakeAgent":
        """What ``slurm_status`` answers from now on."""
        self._status = {
            "state": "",
            "reason": None,
            "nodes": None,
            "exit_code": None,
            "final": None,
            **status,
        }
        return self

    async def slurm_submit(
        self, *, cwd: str | None, env: dict[str, str] | None, job_dir: str
    ) -> str:
        """Write the script the agent would render; job 777."""
        self._calls.append(
            ("submit", {"cwd": cwd, "env": env, "job_dir": job_dir})
        )
        Path(job_dir, "job.sh").write_text("#!/bin/bash\napproved\n")
        return "777"

    async def slurm_status(self, job_dir: str) -> dict[str, Any]:
        """The canned status."""
        self._calls.append(("status", {"job_dir": job_dir}))
        return dict(self._status)

    async def slurm_cancel(self, job_dir: str) -> None:
        """Record the cancel."""
        self._calls.append(("cancel", {"job_dir": job_dir}))

    async def read_output(self, handle: JobHandle) -> tuple[bytes, bytes]:
        """The job dir's logs, as the agent's reader returns them."""
        self._calls.append(("read_output", {"job_dir": handle.job_dir}))
        return b"out", b"err"

    async def path_digest(self, path: str, mode: str = "deep") -> str:
        """Record the native digest."""
        self._calls.append(("digest", {"path": path, "mode": mode}))
        return "abc"

    async def pack_folder(self, src: str, pkg: str) -> None:
        """Record the native pack."""
        self._calls.append(("pack", {"src": src, "pkg": pkg}))

    async def unpack_folder(self, pkg: str, dest: str) -> None:
        """Record the native unpack."""
        self._calls.append(("unpack", {"pkg": pkg, "dest": dest}))

    async def move_file(self, src: str, dst: str) -> None:
        """Record the native move."""
        self._calls.append(("move", {"src": src, "dst": dst}))

    async def file_size(self, path: str) -> int:
        """Record the native stat."""
        self._calls.append(("size", {"path": path}))
        return 42

    def bind(self, task: Any) -> None:
        """Refuse a task, as an agent refuses some executors."""
        if getattr(task, "id", "") == "refused":
            raise TaskExecutionError("refused")
        super().bind(task)

    def called(self, name: str) -> list[dict[str, Any]]:
        """The arguments of every call to op *name*."""
        return [args for op, args in self._calls if op == name]


@pytest.fixture
def agent(tmp_path: Path) -> FakeAgent:
    """A strict agent whose job is still queued."""
    return FakeAgent(working_directory=str(tmp_path)).status(state="PENDING")


def _target(agent: FakeAgent, **kwargs: Any) -> SlurmTarget:
    return SlurmTarget(
        inner=agent,
        working_directory=agent.working_directory,
        exit_code_retries=0,
        **kwargs,
    )


async def _launch(target: SlurmTarget, tmp_path: Path) -> JobHandle:
    job_dir = tmp_path / "job"
    return await target.launch(
        "rm -rf /", cwd=str(tmp_path), env={"A": "1"}, job_dir=str(job_dir)
    )


@pytest.mark.unit
class TestAgentSubmission:
    """The agent submits; the command line never leaves the orchestrator."""

    async def test_launch_submits_through_the_agent(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """
        No sbatch command and no script upload: the agent renders the
        approved script itself.
        """
        target = _target(agent)
        _bind(target, _Task(str(tmp_path)))

        handle = await _launch(target, tmp_path)

        assert agent.commands == []
        assert agent.called("submit") == [
            {
                "cwd": str(tmp_path),
                "env": {"A": "1"},
                "job_dir": handle.job_dir,
            }
        ]
        assert (handle.extra or {})["job_id"] == "777"
        assert (tmp_path / JOB_ID_FILE).read_text() == "777"
        assert target._record is not None
        assert target._record.script == "#!/bin/bash\napproved\n"
        # The agent's script logs into the job dir.
        assert target._record.stdout_path == f"{handle.job_dir}/stdout.log"

    async def test_a_shared_target_is_refused(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """A shared allocation's pilot is a shell loop the agent never runs."""
        target = _target(agent)
        target._ref = "hpc"

        with pytest.raises(TaskExecutionError):
            await _launch(target, tmp_path)
        assert agent.commands == []
        assert agent.called("submit") == []

    async def test_a_non_strict_agent_keeps_the_shell(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """An agent that runs any command keeps today's sbatch path."""
        agent.strict = False
        agent.responds(sbatch=FakeProcess(stdout=b"12345\n"))
        await _launch(_target(agent), tmp_path)

        assert agent.commands_starting("sbatch")
        assert agent.called("submit") == []

    async def test_no_resource_scope(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """The Slurm probe reads sacct over the shell, so none is declared."""
        scope = await _target(agent).resource_scope(
            _Task(str(tmp_path))  # type: ignore[arg-type]
        )

        assert scope is None


@pytest.mark.unit
class TestAgentPolling:
    """Queue state and accounting come from ``slurm_status``."""

    async def test_queued_job_is_still_running(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """A pending job polls as running, with no squeue or sinfo."""
        agent.status(state="PENDING", reason="Resources")
        target = _target(agent, partition="gpu")
        handle = await _launch(target, tmp_path)

        assert await target.poll(handle) is None
        assert agent.commands == []
        assert agent.called("status") == [{"job_dir": handle.job_dir}]

    async def test_exit_code_file_wins(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """The job's own exit code is read back once it exists."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)
        Path(handle.job_dir, "exit_code").write_text("3\n")

        assert await target.poll(handle) == 3
        assert agent.commands == []

    async def test_out_of_squeue_but_running_in_accounting(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """An empty squeue answer is not gone while sacct says RUNNING."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)
        agent.status(final={"state": "RUNNING", "exit_code": "0:0"})

        assert await target.poll(handle) is None

    @pytest.mark.parametrize(
        ("state", "code", "expected"),
        [("COMPLETED", "0:0", 0), ("TIMEOUT", "0:15", 1)],
    )
    async def test_ended_without_exit_code_uses_accounting(
        self,
        agent: FakeAgent,
        tmp_path: Path,
        state: str,
        code: str,
        expected: int,
    ) -> None:
        """Accounting decides a job whose exit code the agent cannot see."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)
        agent.status(final={"state": state, "exit_code": code})

        assert await target.poll(handle) == expected
        assert agent.commands == []
        assert target._record is not None
        assert target._record.state == state

    async def test_no_accounting_is_gone(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """Out of the queue with nothing recorded fails the task."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)
        agent.status()

        assert await target.poll(handle) == 1


@pytest.mark.unit
class TestAgentSignalsAndOutput:
    """Cancel and logs go through the agent's ops."""

    async def test_terminate_cancels_through_the_agent(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """No scancel command line: the agent cancels the job it submitted."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)

        await target.send_signal(handle, signal_mod.SIGTERM)

        assert agent.called("cancel") == [{"job_dir": handle.job_dir}]
        assert agent.commands == []
        assert target._cancelling

    async def test_other_signals_are_dropped(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """The agent has no op to signal a Slurm job."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)

        await target.send_signal(handle, signal_mod.SIGUSR1)

        assert agent.called("cancel") == []
        assert agent.commands == []

    async def test_a_failed_cancel_is_not_raised(
        self, agent: FakeAgent, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """As with scancel, a failed cancel leaves poll to find the end."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)

        async def refuse(self: FakeAgent, job_dir: str) -> None:
            del self
            raise TaskExecutionError(job_dir)

        monkeypatch.setattr(FakeAgent, "slurm_cancel", refuse)
        await target.send_signal(handle, signal_mod.SIGKILL)

        assert target._cancelling

    async def test_output_is_the_agents_reader(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """Logs come from the agent's reader, which follows its policy."""
        target = _target(agent)
        handle = await _launch(target, tmp_path)

        assert await target.read_output(handle) == (b"out", b"err")
        assert agent.called("read_output") == [{"job_dir": handle.job_dir}]


@pytest.mark.unit
class TestTransportBinding:
    """The transport sees the task, so its own refusals apply."""

    def test_bind_binds_the_transport(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """The transport holds the same task as the Slurm target."""
        target = _target(agent)
        task = _Task(str(tmp_path))
        _bind(target, task)

        assert target.inner._task is task  # type: ignore[comparison-overlap]

    def test_a_transport_refusal_stops_the_bind(
        self, agent: FakeAgent, tmp_path: Path
    ) -> None:
        """A task the transport refuses is not bound to the Slurm target."""
        target = _target(agent)

        with pytest.raises(TaskExecutionError):
            _bind(target, _Task(str(tmp_path), id="refused"))
        assert target._task is None


@pytest.mark.unit
class TestFileHooks:
    """File hooks run on the transport, natively when it has them."""

    async def test_hooks_go_to_the_transport(self, agent: FakeAgent) -> None:
        """Each hook is the transport's own, never a shell command."""
        target = _target(agent)

        assert await target.path_digest("/w/a", "fast") == "abc"
        await target.pack_folder("/w/d", "/w/d.pkg")
        await target.unpack_folder("/w/d.pkg", "/w/e")
        await target.move_file("/w/a", "/w/b")
        assert await target.file_size("/w/a") == 42

        assert agent._calls == [
            ("digest", {"path": "/w/a", "mode": "fast"}),
            ("pack", {"src": "/w/d", "pkg": "/w/d.pkg"}),
            ("unpack", {"pkg": "/w/d.pkg", "dest": "/w/e"}),
            ("move", {"src": "/w/a", "dst": "/w/b"}),
            ("size", {"path": "/w/a"}),
        ]
        assert agent.commands == []

    async def test_a_transport_without_hooks_uses_the_inherited_one(
        self, inner: FakeInner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without the transport's hook, the runtime's default runs."""

        async def default(self: BaseTarget, path: str) -> int:
            del self, path
            return 7

        monkeypatch.setattr(BaseTarget, "file_size", default, raising=False)
        # A transport with no hook of its own.
        monkeypatch.setattr(
            FakeInner, "file_size", property(lambda _: None), raising=False
        )

        target = SlurmTarget(inner=inner, working_directory="/w")

        assert await target.file_size("/w/a") == 7
