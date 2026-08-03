#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#

#
# horus-slurm
#
# SlurmTarget: dispatches Horus tasks to a Slurm cluster via sbatch/squeue/
# scancel, delegating placement + filesystem access to an underlying
# transport target (typically LocalTarget if the orchestrator runs on the
# login node, or SSHTarget if it doesn't).
#
"""
Slurm target for horus-runtime.
"""

from __future__ import annotations

import asyncio
import shlex
import signal
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from horus_builtin.target.local import LocalTarget
from horus_runtime.context import HorusContext
from horus_runtime.core.target.base import BaseTarget
from horus_runtime.core.target.channel import (
    ChannelProcess,
    JobHandle,
    RemoteDirEntry,
)
from horus_runtime.core.task.exceptions import TaskExecutionError
from horus_runtime.logging import horus_logger
from pydantic import Field

from horus_slurm.events import GONE, SUBMITTED, SlurmJobEvent
from horus_slurm.i18n import tr as _
from horus_slurm.resources import SlurmJobScope

if TYPE_CHECKING:
    from horus_runtime.core.artifact.base import BaseArtifact
    from horus_runtime.core.resources import ResourceScope
    from horus_runtime.core.task.base import BaseTask

JOB_ID_FILE = ".horus_slurm_job_id"
"""
File under the task working directory holding the submitted job's id.
"""


class SlurmTarget(BaseTarget):
    """
    Runs commands as Slurm batch jobs.

    Placement (login-node access, filesystem, control-plane commands) is
    delegated to ``transport`` -- a ``LocalTarget`` if the orchestrator runs
    on the login node itself, or an ``SSHTarget`` pointed at one. This class
    only adds the Slurm-specific scheduling primitives on top: submitting via
    ``sbatch``, polling via a marker file (with ``squeue`` as a liveness
    fallback), and signalling via ``scancel``.
    """

    kind: str = "slurm"
    kind_name = "Slurm"
    kind_description = _("Submit jobs to a Slurm cluster via sbatch")

    # A target that can already run commands and move files somewhere with
    # access to `sbatch`/`squeue`/`scancel` -- e.g. LocalTarget() if the
    # orchestrator IS the login node, or SSHTarget(host="login.cluster.edu").
    inner: BaseTarget = LocalTarget()

    # sbatch options. Extend as needed; extra_sbatch_args covers anything
    # not modeled explicitly (e.g. "--constraint=a100", "--exclusive").
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    nodes: int = 1
    ntasks: int = 1
    cpus_per_task: int = 1
    mem: str | None = None
    time_limit: str | None = None  # e.g. "01:00:00"
    gres: str | None = None  # e.g. "gpu:1"
    extra_sbatch_args: list[str] = Field(default_factory=list)

    # Slurm jobs are inherently queued/detached -- there is no "run this
    # right now over a live channel" mode for a real workload, only for
    # short control-plane commands (see run_command_sync below).
    detach_by_default: ClassVar[bool] = True

    # Slurm clusters are usually contacted infrequently; the default 1.0s
    # from BaseTarget is fine, but you may want to raise this to reduce
    # squeue/sacct load on shared login nodes with many concurrent jobs.
    poll_interval: ClassVar[float] = 5.0
    exit_code_retries: int = 5
    exit_code_delay: float = 5.0

    _last_state: str | None = None
    """
    Last queue state announced on the bus, so ``poll`` emits on transitions
    rather than once per poll.
    """

    # --- placement identity -------------------------------------------------

    @property
    def location_id(self) -> str:
        """
        Same location as the transport: compute nodes share the login node's
        filesystem, so artifacts already on the transport's host don't need
        copying, and a plain SSHTarget/LocalTarget on the same host is
        recognized as co-located too.
        """
        return self.inner.location_id

    def access_cost(self, artifact: BaseArtifact) -> float | None:
        """
        Bypass the access cost to the inner target.
        """
        return self.inner.access_cost(artifact)

    @property
    def resolved_working_directory(self) -> str:
        """
        The working dir. Slurm takes precedence.
        """
        return self.working_directory or self.inner.resolved_working_directory

    def path_on_target(self, artifact: BaseArtifact) -> str:
        """
        The path of the inner target.
        """
        # Delegate so this stays correct if the transport itself remaps
        # paths (e.g. SSHTarget copies artifacts to an on-host location).
        return self.inner.path_on_target(artifact)

    # --- control-plane passthrough ------------------------------------------

    async def run_command_sync(
        self,
        cmd: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
    ) -> ChannelProcess:
        """
        Short, blocking commands run directly on the transport, not via sbatch.
        """
        return await self.inner.run_command_sync(cmd, cwd=cwd, env=env)

    async def put_file(self, content: bytes | Path, remote_path: str) -> None:
        """
        Delegate to inner.
        """
        await self.inner.put_file(content, remote_path)

    async def get_file(self, remote_path: str) -> bytes:
        """
        Delegate to inner.
        """
        return await self.inner.get_file(remote_path)

    async def mkdir(self, path: str) -> None:
        """
        Delegate to inner.
        """
        await self.inner.mkdir(path)

    async def list_dir(self, path: str) -> list[RemoteDirEntry]:
        """
        Delegate to inner.
        """
        return await self.inner.list_dir(path)

    async def path_exists(self, path: str) -> bool:
        """
        Delegate to inner.
        """
        return await self.inner.path_exists(path)

    async def remove(self, path: str) -> None:
        """
        Delegate to inner.
        """
        await self.inner.remove(path)

    async def launch(
        self,
        cmd: str,
        *,
        cwd: str | None,
        env: dict[str, str] | None,
        job_dir: str,
    ) -> JobHandle:
        """
        Build the SLURM script and launch the ``sbatch`` command.
        """
        await self.inner.mkdir(job_dir)

        script = self._build_sbatch_script(
            cmd, cwd=cwd, env=env, job_dir=job_dir
        )
        script_path = f"{job_dir}/job.sh"
        await self.inner.put_file(script.encode(), script_path)

        proc = await self.inner.run_command_sync(
            f"sbatch --parsable {shlex.quote(script_path)}"
        )
        out, err = await proc.communicate()
        if proc.returncode != 0:
            raise TaskExecutionError(
                _("sbatch submission failed: %(error)s")
                % {"error": err.decode(errors="replace").strip()}
            )

        # --parsable prints "<jobid>" or "<jobid>;<cluster>" for federated
        # submissions. Array jobs ("<jobid>_<n>") aren't handled here -- see
        # the note at the bottom of this file.
        raw_id = out.decode().strip()
        job_id = raw_id.split(";", 1)[0]
        await self._publish_job_id(job_id, cwd=cwd)
        self._announce(job_id, SUBMITTED)

        # pid stays None: a job id is not a process id.
        return JobHandle(
            pid=None,
            job_dir=job_dir,
            extra={"job_id": job_id, "raw_job_id": raw_id},
        )

    @staticmethod
    def _job_id_path(task: BaseTask) -> str:
        """Path on the target where the submitted job's id is written."""
        return (Path(task.working_dir) / JOB_ID_FILE).as_posix()

    async def _publish_job_id(self, job_id: str, *, cwd: str | None) -> None:
        """
        Record the job id where an observer can find it. Best effort.
        """
        base = (
            self._job_id_path(self._task)
            if self._task is not None
            else f"{(cwd or self.resolved_working_directory)}/{JOB_ID_FILE}"
        )
        try:
            await self.inner.put_file(job_id.encode(), base)
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not record job id for observation: %(err)s")
                % {"err": exc}
            )

    def _announce(self, job_id: str, state: str) -> None:
        """
        Publish a job state on the event bus, if it changed. Best effort.

        Nothing here may fail a job: there is no bus at all outside a Horus
        context (the plain CLI), and a subscriber that raises is the
        subscriber's problem, not this job's.
        """
        if state == self._last_state:
            return
        self._last_state = state
        try:
            HorusContext.get_context().bus.emit(
                SlurmJobEvent(
                    task_id=self._task.id if self._task is not None else "",
                    job_id=job_id,
                    state=state,
                    message=_("Slurm job %(job_id)s is %(state)s")
                    % {"job_id": job_id, "state": state},
                )
            )
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not announce Slurm job state: %(err)s") % {"err": exc}
            )

    async def resource_scope(
        self, task: BaseTask, process: ChannelProcess | None = None
    ) -> ResourceScope | None:
        """
        Report the Slurm job, not whatever the orchestrator spawned.
        """
        del process
        return SlurmJobScope(job_id_file=self._job_id_path(task))

    @staticmethod
    def _handle_job_id(handle: JobHandle) -> str:
        """The Slurm job id carried by *handle*."""
        return (handle.extra or {}).get("job_id", "")

    async def poll(self, handle: JobHandle) -> int | None:
        """
        Poll job status.
        """
        exit_code_path = f"{handle.job_dir}/exit_code"
        job_id = self._handle_job_id(handle)

        # Announce before the exit_code short-circuit: a job that finishes
        # between two polls would otherwise never report having run.
        state = await self._queue_state(handle)
        if state:
            self._announce(job_id, state)

        if await self.inner.path_exists(exit_code_path):
            raw = await self.inner.get_file(exit_code_path)
            return int(raw.decode().strip())

        if state == "":
            # Left the queue without exit_code visible yet. This can be a race:
            # Slurm sometimes flushes exit_code a few seconds after the job
            # disappears from squeue. Retry briefly before concluding it was
            # killed by the scheduler (OOM, node failure, wall-time) or
            # cancelled outside Horus.
            exit_code = await self._retry_exit_code(exit_code_path)
            if exit_code is not None:
                return exit_code

            self._announce(job_id, GONE)
            horus_logger.log.error(
                _("The job vanished from queue without writting an exit code.")
            )
            return 1

        return None

    async def _retry_exit_code(
        self,
        exit_code_path: str,
    ) -> int | None:
        """
        Retry fetching exit_code after the job vanished from the queue,
        to cover the case where Slurm writes it with a slight delay.
        """
        # TODO: Be able to re-attach if workflow failed but job ended.
        for _attempt in range(self.exit_code_retries):
            horus_logger.log.debug(
                _(
                    "Retrying exit_code fetch, "
                    "attempt %(attempt)d/%(attempts)d..."
                )
                % {
                    "attempt": _attempt + 1,
                    "attempts": self.exit_code_retries,
                },
            )
            await asyncio.sleep(self.exit_code_delay)
            if await self.inner.path_exists(exit_code_path):
                raw = await self.inner.get_file(exit_code_path)
                return int(raw.decode().strip())
        return None

    async def read_output(self, handle: JobHandle) -> tuple[bytes, bytes]:
        """
        Read job stderr and stdout.

        ponytail: re-reads both logs whole on every call, and
        ``PollingChannelProcess.stream`` calls this once per poll interval, so
        tailing a long-running chatty job is O(n^2) in log size. Switch to a
        ranged read (``tail -c +offset``) if job logs get large enough to
        matter.
        """
        stdout, stderr = b"", b""
        try:
            stdout = await self._read_log(f"{handle.job_dir}/stdout.log")
        except Exception:
            horus_logger.log.info(_("Could not read stdout: {s}"))

        try:
            stderr = await self._read_log(f"{handle.job_dir}/stderr.log")
        except Exception:
            horus_logger.log.info(_("Could not read stderr: {s}"))

        return stdout, stderr

    async def send_signal(self, handle: JobHandle, sig: int) -> None:
        """
        Send signal to the job.
        """
        try:
            name = signal.Signals(sig).name.removeprefix("SIG")
        except ValueError:
            name = "TERM"
        proc = await self.inner.run_command_sync(
            f"scancel --signal={name} {self._handle_job_id(handle)}"
        )
        await proc.wait()

    # --- helpers -------------------------------------------------------------

    def _build_sbatch_script(
        self,
        cmd: str,
        *,
        cwd: str | None,
        env: dict[str, str] | None,
        job_dir: str,
    ) -> str:
        lines = ["#!/bin/bash"]
        lines.append(f"#SBATCH --job-name=horus-{Path(job_dir).name}")
        lines.append(f"#SBATCH --output={job_dir}/stdout.log")
        lines.append(f"#SBATCH --error={job_dir}/stderr.log")
        lines.append(f"#SBATCH --nodes={self.nodes}")
        lines.append(f"#SBATCH --ntasks={self.ntasks}")
        lines.append(f"#SBATCH --cpus-per-task={self.cpus_per_task}")
        if self.partition:
            lines.append(f"#SBATCH --partition={self.partition}")
        if self.account:
            lines.append(f"#SBATCH --account={self.account}")
        if self.qos:
            lines.append(f"#SBATCH --qos={self.qos}")
        if self.mem:
            lines.append(f"#SBATCH --mem={self.mem}")
        if self.time_limit:
            lines.append(f"#SBATCH --time={self.time_limit}")
        if self.gres:
            lines.append(f"#SBATCH --gres={self.gres}")
        for arg in self.extra_sbatch_args:
            lines.append(f"#SBATCH {arg}")

        lines.append("")
        target_cwd = cwd or self.resolved_working_directory
        lines.append(f"cd {shlex.quote(target_cwd)}")
        for key, value in (env or {}).items():
            lines.append(f"export {key}={shlex.quote(value)}")

        # Subshell so a top-level `exit` in cmd still reaches the write --
        # same reasoning as build_detach_command's `( inner ); echo $? > ...`.
        lines.append(f"( {cmd} ); echo $? > {shlex.quote(job_dir)}/exit_code")
        return "\n".join(lines) + "\n"

    async def _read_log(self, path: str) -> bytes:
        try:
            return await self.inner.get_file(path)
        except FileNotFoundError:
            return b""

    async def _queue_state(self, handle: JobHandle) -> str | None:
        """
        What ``squeue`` says about the job right now.

        Returns the state string (``PENDING``, ``RUNNING``, …), ``""`` when the
        job is no longer in the queue, or ``None`` when squeue could not
        answer. Only an empty *successful* result means gone -- a nonzero exit
        (e.g. a slurmctld hiccup) must NOT be read that way, or a transient
        scheduler blip will fail jobs that are still fine.
        """
        proc = await self.inner.run_command_sync(
            f"squeue -h -j {self._handle_job_id(handle)} -o %T"
        )
        out, _err = await proc.communicate()
        if proc.returncode != 0:
            return None
        text = out.decode(errors="replace").strip()
        if not text:
            return ""
        # A job array reports one line per element; the first is enough for a
        # single job, which is all this target submits.
        return text.splitlines()[0].strip()


# --- Known limitations / next steps -----------------------------------------
#
# 1. Job arrays ("sbatch --array=...") aren't supported here: JobHandle.pid
#    is a plain int, but array elements report as "<jobid>_<n>". If you need
#    arrays, you'd want a JobHandle.extra entry carrying the array spec and
#    a poll() that checks all elements.
#
# 2. squeue/sacct flakiness: shared login nodes can rate-limit or slow down
#    under load. If _vanished_from_queue proves noisy in practice, consider
#    requiring N consecutive empty results before declaring failure.
#
# 3. recover(): BaseTarget's default returns False. Since JobHandle here is
#    just {pid, job_dir}, a real recover() implementation is straightforward
#    if you persist JobHandle in the run-state file -- poll() and
#    read_output() work identically for a freshly-constructed SlurmTarget
#    pointed at the same job_dir, no live connection required.
