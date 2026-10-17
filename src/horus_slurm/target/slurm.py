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
import re
import shlex
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple

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
from pydantic import Field, PrivateAttr, field_validator

from horus_slurm.events import CANCELLING, GONE, SUBMITTED, SlurmJobEvent
from horus_slurm.i18n import tr as _
from horus_slurm.record import (
    RECORD_FILE,
    STDERR_FILE,
    STDOUT_FILE,
    SlurmJobRecord,
)
from horus_slurm.resources import (
    SbatchResources,
    SlurmJobScope,
    resolve_resources,
    slurm_mem_mb,
    slurm_seconds,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from horus_runtime.core.artifact.base import BaseArtifact
    from horus_runtime.core.resources import ResourceScope
    from horus_runtime.core.task.base import BaseTask
    from pydantic import ValidationInfo

JOB_ID_FILE = ".horus_slurm_job_id"
"""
File under the task working directory holding the submitted job's id.
"""

NODE_REASONS = (
    "Resources",
    "ReqNodeNotAvail",
    "PartitionDown",
    "PartitionInactive",
    "PartitionNodeLimit",
    "Nodes_required_for_job_are_DOWN",
)
"""
Pending reasons (matched by prefix) worth a look at the partition's nodes: the
job waits on hardware, and whether that hardware is busy, drained or down is
what the user needs to know next.
"""

ENDED_STATES = frozenset(
    {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "TIMEOUT",
    }
)
"""
States ``squeue`` may still list an ended job in until ``MinJobAge`` passes.
"""

PARTITION_SUMMARY_INTERVAL = 60.0
"""Seconds between ``sinfo`` reads while the pending reason is unchanged."""


class QueueState(NamedTuple):
    """One ``squeue`` reading of a job; ``state`` is ``""`` once it is gone."""

    state: str
    reason: str | None = None
    nodes: str | None = None
    partition: str | None = None

    @property
    def gone(self) -> bool:
        """Whether the job has left the queue or ended."""
        return self.state == "" or self.state in ENDED_STATES


ALLOC_DIR = ".horus_alloc"
"""
Directory under the working directory holding each shared allocation's pilot
script, log and spool.
"""

HEARTBEAT_STALE_S = 900
"""
Seconds without a :meth:`SlurmTarget.keepalive` after which an idle pilot
ends its allocation: the orchestrator is gone, and nobody will release it.
Three of the lease manager's keepalive intervals.
"""

# The pilot runs inside the allocation and starts every launcher dropped into
# spool/ as a job step; a launcher is claimed with `mv`, so it runs once.
# Only the user's own regular files run: the directory is private, and this
# is the second line of defence on a shared filesystem.
PILOT_LOOP = f"""\
umask 077
while true; do
  for f in spool/*.sh; do
    [ -f "$f" ] && [ ! -L "$f" ] && [ -O "$f" ] || continue
    mv "$f" run/ && bash "run/${{f#spool/}}" &
  done
  if [ -z "$(jobs -rp)" ] && \\
    [ $(( $(date +%s) - $(stat -c %Y heartbeat) )) -gt {HEARTBEAT_STALE_S} ]
  then
    exit 0
  fi
  sleep 2
done
"""

CLEAN_INTERVAL_S = 86400
"""Seconds before a process cleans the same allocation root again."""

_ALLOC_NAME = re.compile(r"([a-z][a-z0-9_-]{0,62})\.[0-9a-f]{12}-[0-9]+")
"""An allocation dir name, ``<ref>.<lease_key>-<generation>``."""

_CLEANED: dict[tuple[str, str, str], float] = {}
"""When this process last cleaned each (location, root, target ref)."""

_JOB_ID = re.compile(r"\d+(_\d+)?")
"""A job id as sbatch prints it, before it goes into another command."""

# sbatch splits #SBATCH lines on newlines and options on any whitespace, and
# reads quotes, backslashes and an unquoted `#` too. Values and paths need
# none of these; extra_sbatch_args may hold anything but a new line.
_STRICT_BAD = re.compile(r"[\s#'\"\\\x00]")
_LINE_BAD = re.compile(r"[\n\r\x00]")


@dataclass(eq=False)
class _Allocation:
    """
    The one allocation behind a named target, shared by all its copies.

    ``idle_copy()`` and map clones are shallow copies, so they reference the
    same handle and submit their steps into the same allocation.
    """

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    job_id: str | None = None
    generation: int = 0
    alloc_dir: str = ""
    ready_callbacks: list[Callable[[], Awaitable[None]]] = field(
        default_factory=list
    )
    # Set once the current allocation's ready callbacks have run; steps are
    # handed in only after.
    callbacks_done: asyncio.Event = field(default_factory=asyncio.Event)
    # One squeue reading shared by all copies, keyed by the job id it was
    # read for. Taken after `lock`, never before it.
    refresh_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    queue_job_id: str | None = None
    queue: QueueState | None = None
    queue_at: float = 0.0
    # (job id, monotonic time) of the last failed read, so the copies that
    # waited on it give up too instead of each retrying in turn.
    queue_failed: tuple[str, float] | None = None
    # The shared copies of SlurmTarget's sinfo cache.
    _partition_checked: tuple[str, float] | None = None
    _partition_nodes: dict[str, int] | None = None

    def cached_queue(self, job_id: str, interval: float) -> QueueState | None:
        """The reading for *job_id* if under *interval* seconds old."""
        if (
            self.queue_job_id == job_id
            and time.monotonic() - self.queue_at < interval
        ):
            return self.queue
        return None

    def __deepcopy__(self, memo: dict[int, Any]) -> _Allocation:
        """A deep copy is a separate declared target: no allocation yet."""
        del memo
        return _Allocation()


class SlurmTarget(BaseTarget):
    """
    Runs commands as Slurm batch jobs.

    Placement (login-node access, filesystem, control-plane commands) is
    delegated to ``transport`` -- a ``LocalTarget`` if the orchestrator runs
    on the login node itself, or an ``SSHTarget`` pointed at one. This class
    only adds the Slurm-specific scheduling primitives on top: submitting via
    ``sbatch``, polling via a marker file (with ``squeue`` as a liveness
    fallback), and signalling via ``scancel``.

    Resource requests come from two places and are merged, explicit winning:

    - the task's portable ``resources`` (``cpus``, ``gpus``, ``memory_gb``,
      ``walltime``), which say what the *work* needs in target-agnostic terms
      and let one workflow run on any scheduler;
    - this target's own sbatch fields, which say what to ask *Slurm* for in
      its own vocabulary, and override the portable hint whenever both apply.

    So a task carrying ``resources: {cpus: 16, gpus: 1, memory_gb: 64}``
    submits with ``--cpus-per-task=16 --mem=64G --gres=gpu:1`` on a bare
    ``SlurmTarget(partition="gpu")``, while a target that also sets
    ``gres="gpu:a100:1"`` keeps its own -- the site knows something about the
    hardware that the portable request cannot express.

    See :func:`horus_slurm.resources.resolve_resources` for the full mapping.
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
    #
    # The four that a task's portable `resources` can also determine
    # (cpus_per_task, mem, time_limit, gres) default to None, meaning "derive
    # me". Setting one here is an override that always wins -- see
    # resolve_resources. They are deliberately None rather than carrying a
    # concrete default: "was this set explicitly?" has to survive a round trip
    # through JSON (tc-os stores workflows that way), and pydantic's
    # model_fields_set does not -- every field comes back as explicitly set.
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    nodes: int = 1
    ntasks: int = 1
    cpus_per_task: int | None = Field(default=None, title=_("CPUs per task"))
    mem: str | None = Field(
        default=None,
        title=_("Memory"),
        description=_(
            "Memory per node with a unit, e.g. 4G. "
            "A bare number is megabytes to Slurm."
        ),
    )
    time_limit: str | None = Field(
        default=None,
        title=_("Time limit"),
        description=_("Slurm format, e.g. 02:00:00 or 1-00:00:00"),
    )
    gres: str | None = None  # e.g. "gpu:1"
    extra_sbatch_args: list[str] = Field(default_factory=list)

    # Only for a target named under `targets:`: its tasks run as job steps
    # inside one allocation sized by the fields above.
    hold_timeout_minutes: int = Field(
        default=5,
        ge=0,
        le=120,
        description=_(
            "Minutes a shared allocation is kept while idle and tasks "
            "are still pending"
        ),
    )
    max_parallel: int | None = Field(
        default=None,
        ge=1,
        description=_("Tasks that run at once in a shared allocation"),
    )
    alloc_retention_days: int = Field(
        default=7,
        ge=1,
        le=365,
        title=_("Allocation folder retention (days)"),
    )

    # The allocation's files live on the cluster's shared filesystem, so a
    # released allocation loses no output. A node-local working_directory
    # (/tmp, $TMPDIR) breaks this, as it breaks inline jobs too.
    #
    # A step gets only the task's own `resources`: a task that declares none
    # runs on one CPU, with no GPU even when `gres` is set here.
    keeps_data_on_release: ClassVar[bool] = True

    # Slurm jobs are inherently queued/detached -- there is no "run this
    # right now over a live channel" mode for a real workload, only for
    # short control-plane commands (see run_command_sync below).
    detach_by_default: ClassVar[bool] = True

    poll_seconds: float = Field(
        default=5.0,
        ge=5,
        le=300,
        title=_("Poll interval"),
        description=_("Seconds between checks of each task's exit file"),
    )
    queue_check_interval: float = Field(
        default=30.0,
        # Below Slurm's default MinJobAge (300 s), so an ended job is still
        # listed on the next read.
        ge=5,
        le=120,
        title=_("Queue check interval"),
        description=_("Seconds between squeue checks of a shared allocation"),
    )
    exit_code_retries: int = 5
    exit_code_delay: float = 5.0

    _last_state: tuple[str, str | None, dict[str, int] | None] | None = None
    """
    Last (state, reason, partition_nodes) announced on the bus, so ``poll``
    emits on transitions -- including a pending job's reason or its
    partition's node states changing -- rather than once per poll.
    """

    _partition_checked: tuple[str, float] | None = None
    """
    (reason, monotonic time) of the last ``sinfo`` read, to re-read only when
    the pending reason changes or the summary is stale.
    """

    _partition_nodes: dict[str, int] | None = None
    """The last ``sinfo`` summary, reused until it is due again."""

    _cancelling: bool = False
    """
    Set once ``send_signal`` has cancelled the job, so ``poll`` asks
    accounting how it ended instead of waiting out the exit-code retries.
    """

    _record: SlurmJobRecord | None = None
    """
    The job's durable record, rewritten on every state transition and exit
    code observation. ``None`` until ``launch()`` submits the job.
    """

    _alloc: _Allocation = PrivateAttr(default_factory=_Allocation)

    @field_validator(
        "partition", "account", "qos", "mem", "time_limit", "gres"
    )
    @classmethod
    def _one_sbatch_value(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
        """A value that stays one option on its ``#SBATCH`` line."""
        # A stray space from a form must not stop a stored workflow loading.
        value = value.strip() if value is not None else None
        if value is not None and _STRICT_BAD.search(value):
            raise ValueError(
                _(
                    "%(field)s may not contain whitespace, '#', quotes or "
                    "backslashes."
                )
                % {"field": info.field_name}
            )
        return value

    @field_validator("extra_sbatch_args")
    @classmethod
    def _one_sbatch_line(cls, value: list[str]) -> list[str]:
        """Each argument stays on its own ``#SBATCH`` line."""
        value = [arg.strip() for arg in value]
        if any(_LINE_BAD.search(arg) for arg in value):
            raise ValueError(
                _("extra_sbatch_args may not contain line breaks.")
            )
        return value

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

    async def get_file_to(self, remote_path: str, local_path: Path) -> None:
        """
        Delegate to inner, so a streaming transport is not bypassed.
        """
        await self.inner.get_file_to(remote_path, local_path)

    async def read_range(
        self, remote_path: str, offset: int, length: int
    ) -> AsyncIterator[bytes]:
        """
        Delegate to inner.
        """
        async for chunk in self.inner.read_range(remote_path, offset, length):
            yield chunk

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
        Build the SLURM script and launch the ``sbatch`` command, or, on a
        shared target, hand it to the allocation as a job step.
        """
        await self.inner.mkdir(job_dir)
        stdout_path, stderr_path = self._log_paths(job_dir)
        # Slurm does not create a missing --output directory; it silently
        # drops the output instead.
        await self.inner.mkdir(str(Path(stdout_path).parent))

        script_path = f"{job_dir}/job.sh"
        extra: dict[str, str] = {}
        if self.releasable:
            script = "\n".join(
                ["#!/bin/bash", *self._script_body(cmd, cwd, env, job_dir)]
            )
            step = f"horus-{Path(job_dir).name}"
            job_id, alloc_dir = await self._submit_step(
                step, f"{script}\n", job_dir
            )
            # Kept in the private allocation dir, read when the step starts.
            script_path = f"{alloc_dir}/run/{step}.job"
            raw_id = job_id
            extra = {"step": step, "alloc_dir": alloc_dir}
        else:
            script = self._build_sbatch_script(
                cmd, cwd=cwd, env=env, job_dir=job_dir
            )
            await self.inner.put_file(script.encode(), script_path)
            job_id, raw_id = await self._sbatch(script_path)
        await self._publish_job_id(job_id, cwd=cwd)

        target_cwd = cwd or self.resolved_working_directory
        self._record = SlurmJobRecord(
            job_id=job_id,
            state=SUBMITTED,
            script=script,
            script_path=script_path,
            working_dir=target_cwd,
            job_dir=job_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            sbatch=self._sbatch_summary(),
        )
        await self._announce(job_id, SUBMITTED)

        extra |= {"job_id": job_id, "raw_job_id": raw_id}
        # pid stays None: a job id is not a process id.
        return JobHandle(pid=None, job_dir=job_dir, extra=extra)

    async def _sbatch(self, script_path: str) -> tuple[str, str]:
        """Submit *script_path*; the job id and sbatch's raw answer."""
        proc = await self.inner.run_command_sync(
            f"sbatch --parsable {shlex.quote(script_path)}"
        )
        out, err = await proc.communicate()
        if proc.returncode != 0:
            error = err.decode(errors="replace").strip()
            if "time limit" in error.lower():
                error += await self._time_limit_context()
            raise TaskExecutionError(
                _("sbatch submission failed: %(error)s") % {"error": error}
            )

        # --parsable prints "<jobid>" or "<jobid>;<cluster>" for federated
        # submissions. Array jobs ("<jobid>_<n>") aren't handled here -- see
        # the note at the bottom of this file.
        raw_id = out.decode().strip()
        job_id = raw_id.split(";", 1)[0]
        if not _JOB_ID.fullmatch(job_id):
            raise TaskExecutionError(
                _("sbatch returned an unexpected job id: %(raw)s")
                % {"raw": raw_id}
            )
        return job_id, raw_id

    # --- shared allocation ---------------------------------------------------

    @property
    def releasable(self) -> bool:  # type: ignore[override]
        """A target named under ``targets:`` runs its tasks in one job."""
        return self._ref is not None

    @property
    def poll_interval(self) -> float:  # type: ignore[override]
        """Seconds between polls: :attr:`poll_seconds`."""
        return self.poll_seconds

    @property
    def hold_timeout_s(self) -> float | None:
        """The hold window in seconds."""
        return self.hold_timeout_minutes * 60.0 if self.releasable else None

    @property
    def member_cap(self) -> int | None:
        """Cap on members running at once: :attr:`max_parallel`."""
        return self.max_parallel

    @property
    def is_live(self) -> bool:
        """Whether an allocation is submitted (queued or running)."""
        return self._alloc.job_id is not None

    @property
    def generation(self) -> int:
        """How many allocations this target has submitted."""
        return self._alloc.generation

    def validate_named(self) -> list[str]:
        """A shared allocation is sized by its own fields, never its tasks."""
        missing = [
            name
            for name in ("cpus_per_task", "mem", "time_limit")
            if getattr(self, name) is None
        ]
        errors = []
        if missing:
            errors.append(
                _("Shared Slurm target '%(ref)s' needs %(fields)s.")
                % {"ref": self._ref, "fields": ", ".join(missing)}
            )
        if self.nodes != 1:
            errors.append(
                _("Shared Slurm target '%(ref)s' must use one node.")
                % {"ref": self._ref}
            )
        return errors

    def validate_member(self, task: BaseTask) -> list[str]:
        """Errors that keep *task* from running in this named allocation."""
        request = task.resources
        if self._ref is None or request is None:
            return []
        mem_mb = slurm_mem_mb(self.mem)
        walltime = slurm_seconds(request.walltime)
        limit = slurm_seconds(self.time_limit)
        checks = (
            (request.cpus, self.cpus_per_task, "CPUs", request.cpus),
            (
                request.memory_gb and request.memory_gb * 1024,
                mem_mb,
                "memory",
                f"{request.memory_gb}G",
            ),
            (walltime, limit, "walltime", request.walltime),
        )
        return [
            _(
                "Task '%(task)s' needs %(want)s %(what)s but shared Slurm "
                "target '%(ref)s' has %(have)s."
            )
            % {
                "task": task.id,
                "want": shown,
                "what": what,
                "ref": self._ref,
                "have": {
                    "CPUs": self.cpus_per_task,
                    "memory": self.mem,
                    "walltime": self.time_limit,
                }[what],
            }
            for want, have, what, shown in checks
            # A 0 on the target means "all" or "no limit" to Slurm.
            if want and have and want > have
        ]

    def on_ready(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Run *callback* once per allocation, before any step is handed in."""
        self._alloc.ready_callbacks.append(callback)

    async def release(self, reason: str) -> None:
        """Cancel the allocation (idempotent). Raises when scancel fails."""
        handle = self._alloc
        async with handle.lock:
            if handle.job_id is not None:
                proc = await self.inner.run_command_sync(
                    f"scancel {handle.job_id}"
                )
                _out, err = await proc.communicate()
                error = err.decode(errors="replace").strip()
                # Already gone (time limit, idle exit) and purged: done.
                if proc.returncode != 0 and "invalid job id" not in (
                    error.lower()
                ):
                    raise TaskExecutionError(
                        _("scancel failed: %(error)s") % {"error": error}
                    )
                handle.job_id = None
                # The launchers and job scripts; pilot.log stays.
                alloc = shlex.quote(handle.alloc_dir)
                proc = await self.inner.run_command_sync(
                    f"rm -rf {alloc}/spool {alloc}/run"
                )
                await proc.wait()
            if reason in ("run_end", "abort"):
                # Registered per run; a re-run registers its own.
                handle.ready_callbacks.clear()

    async def keepalive(self) -> None:
        """
        Prove the orchestrator still wants the allocation: an idle pilot
        whose heartbeat goes stale ends it. Never raises.
        """
        handle = self._alloc
        if handle.job_id is None:
            return
        try:
            proc = await self.inner.run_command_sync(
                f"touch {shlex.quote(handle.alloc_dir)}/heartbeat"
            )
            await proc.wait()
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not keep the allocation alive: %(err)s")
                % {"err": exc}
            )

    async def _allocation(self) -> _Allocation:
        """
        The live allocation, submitted first if there is none or it ended
        on its own (time limit, idle exit, cancelled outside Horus).
        """
        handle = self._alloc
        submitted = False
        async with handle.lock:
            if handle.job_id is not None:
                queue = await self._queue_state(
                    JobHandle(
                        pid=None, job_dir="", extra={"job_id": handle.job_id}
                    )
                )
                if queue is not None and queue.gone:
                    handle.job_id = None
            if handle.job_id is None:
                handle.generation += 1
                handle.queue_job_id = handle.queue = None
                handle._partition_checked = None  # noqa: SLF001
                handle._partition_nodes = None  # noqa: SLF001
                root = f"{self.resolved_working_directory}/{ALLOC_DIR}"
                alloc_dir = (
                    f"{root}/{self._ref}.{self.lease_key}-{handle.generation}"
                )
                await self._make_private_dir(alloc_dir)
                script_path = f"{alloc_dir}/pilot.sh"
                await self.inner.put_file(
                    self._pilot_script(alloc_dir).encode(), script_path
                )
                handle.job_id, _raw = await self._sbatch(script_path)
                handle.alloc_dir = alloc_dir
                handle.callbacks_done = asyncio.Event()
                submitted = True
            done = handle.callbacks_done
        if submitted:
            # Outside the lock: a long restore must not hold up a release.
            # The allocation's filesystem is reachable while it queues.
            try:
                for callback in list(handle.ready_callbacks):
                    try:
                        await callback()
                    except Exception:
                        horus_logger.log.exception(
                            _("A machine-ready callback failed.")
                        )
            finally:
                done.set()
            # After sbatch, so the pilot queues while this runs.
            await self._clean_alloc_dirs(root, alloc_dir)
        await done.wait()
        return handle

    async def _make_private_dir(self, alloc_dir: str) -> None:
        """
        Create *alloc_dir* readable and writable by this user only.

        The pilot runs whatever is spooled there, so on a shared filesystem
        nobody else may create, swap or write into it: the parent must be
        ours and not a symlink, and the leaf must not exist yet.
        """
        q = shlex.quote
        root = q(str(Path(alloc_dir).parent))
        leaf = q(alloc_dir)
        proc = await self.inner.run_command_sync(
            f"umask 077 && mkdir -p {root} && [ -O {root} ] "
            f"&& [ ! -L {root} ] && chmod 700 {root} "
            f"&& mkdir -m 700 {leaf} && mkdir {leaf}/spool {leaf}/run "
            f"&& touch {leaf}/heartbeat"
        )
        _out, err = await proc.communicate()
        if proc.returncode != 0:
            raise TaskExecutionError(
                _("Could not create the allocation directory %(dir)s: %(err)s")
                % {
                    "dir": alloc_dir,
                    "err": err.decode(errors="replace").strip(),
                }
            )

    async def _clean_alloc_dirs(self, root: str, keep: str) -> None:
        """
        Remove this target's allocation dirs under *root* idle for longer
        than ``alloc_retention_days``, except *keep*, the newest, and any
        whose pilot Slurm still lists.

        Age is the heartbeat's, which keepalive touches, so a live
        allocation of another run is never old. Other targets' dirs follow
        their own retention. At most once a day per process; best effort,
        never raises.
        """
        key = (self.location_id, root, str(self._ref))
        last = _CLEANED.get(key)
        if last is not None and time.monotonic() - last < CLEAN_INTERVAL_S:
            return
        _CLEANED[key] = time.monotonic()
        q = shlex.quote
        # Relative to the real dir, so later commands name entries in the
        # dir that was listed, and a root starting with `-` is no option.
        cd = f"cd -P -- {q(root)}"
        try:
            proc = await self.inner.run_command_sync(
                f"{cd} && date +%s && find . -mindepth 2 -maxdepth 2 "
                f"-type f -name heartbeat -printf '%T@ %h\\n'"
            )
            out, err = await proc.communicate()
            if proc.returncode != 0:
                # One unreadable entry fails find; the rest still counts.
                horus_logger.log.debug(err.decode(errors="replace").strip())
            now, *lines = out.decode(errors="replace").splitlines()
            # The cluster's clock, as for the heartbeat times.
            cutoff = float(now) - self.alloc_retention_days * 86400
            found: list[tuple[float, str]] = []
            for line in lines:
                mtime, _sep, path = line.partition(" ")
                match = _ALLOC_NAME.fullmatch(path.removeprefix("./"))
                if match is not None and match[1] == self._ref:
                    found.append((float(mtime), match[0]))
            kept = {max(found)[1], Path(keep).name} if found else set()
            old = [n for m, n in found if m < cutoff and n not in kept]
            if not old:
                return
            # A pilot outlives a dead orchestrator while a step runs.
            proc = await self.inner.run_command_sync(
                'squeue -h -u "$(id -un)" -o %o'
            )
            out, err = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(err.decode(errors="replace").strip())
            live = out.decode(errors="replace").split()
            old = [
                n
                for n in old
                if not any(c.endswith(f"/{n}/pilot.sh") for c in live)
            ]
            # In chunks, so a long backlog stays under the argument limit;
            # one chunk that fails does not stop the rest.
            errors = []
            for i in range(0, len(old), 200):
                names = " ".join(q(n) for n in old[i : i + 200])
                proc = await self.inner.run_command_sync(
                    f"{cd} && [ -O . ] && rm -rf -- {names}"
                )
                _out, err = await proc.communicate()
                if proc.returncode != 0:
                    errors.append(err.decode(errors="replace").strip())
            if errors:
                raise RuntimeError("\n".join(errors))
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not clean old allocation dirs in %(dir)s: %(err)s")
                % {"dir": root, "err": exc}
            )

    def _pilot_script(self, alloc_dir: str) -> str:
        """The allocation's batch script: its size, then the pilot loop."""
        resolved = resolve_resources(
            None,
            cpus_per_task=self.cpus_per_task,
            mem=self.mem,
            time_limit=self.time_limit,
            gres=self.gres,
        )
        log = f"{alloc_dir}/pilot.log"
        lines = self._sbatch_header(f"horus-{self._ref}", log, log, resolved)
        # A requeued pilot would not see the steps it had already claimed.
        lines += ["#SBATCH --no-requeue", "", f"cd {shlex.quote(alloc_dir)}"]
        lines.append(PILOT_LOOP)
        return "\n".join(lines)

    async def _submit_step(
        self, step: str, script: str, job_dir: str
    ) -> tuple[str, str]:
        """
        Hand *script* to the allocation as job step *step*; the
        allocation's job id and directory.

        The launcher writes the step's exit code itself when srun fails or
        the step is cancelled (a ``<step>.cancel`` mark, watched while srun
        waits for room or runs), so ``poll`` never waits on a step that will
        not run.
        """
        handle = await self._allocation()
        await self._spool_step(handle, step, script, job_dir)
        return str(handle.job_id), handle.alloc_dir

    async def _spool_step(
        self, handle: _Allocation, step: str, script: str, job_dir: str
    ) -> None:
        """Write the step script and launcher into handle.alloc_dir/spool."""
        request = self._task.resources if self._task is not None else None
        # The task's own request, not the target's (the allocation's) size.
        step_res = resolve_resources(request)
        mem = step_res.mem
        if mem is None:
            # An even share, so one step without a request cannot take the
            # whole allocation's memory and serialise every other step.
            total = slurm_mem_mb(self.mem)
            if total and self.cpus_per_task:
                share = total * step_res.cpus_per_task // self.cpus_per_task
                mem = f"{max(1, share)}M"
            else:
                # The job's memory, without holding it from other steps.
                mem = "0"
        # A step gets every GPU of the job unless it says otherwise.
        gres = step_res.gres or ("none" if self.gres else None)
        stdout_path, stderr_path = self._log_paths(job_dir)
        q = shlex.quote
        run = f"{handle.alloc_dir}/run/{step}"
        exit_path = q(f"{job_dir}/exit_code")
        cancel = q(f"{run}.cancel")
        srun = [
            "srun --exact --ntasks=1",
            f"--job-name={q(step)}",
            f"--cpus-per-task={step_res.cpus_per_task}",
            f"--mem={q(mem)}",
            *([f"--gres={q(gres)}"] if gres else []),
            f"--output={q(stdout_path)}",
            f"--error={q(stderr_path)}",
            f"bash {q(run + '.job')}",
        ]
        launcher = (
            f"[ -e {cancel} ] && {{ echo 143 > {exit_path}; exit; }}\n"
            f"{' '.join(srun)} &\n"
            "p=$!\n"
            "while kill -0 $p 2>/dev/null; do\n"
            f"  [ -e {cancel} ] && kill -TERM $p\n"
            "  sleep 1\n"
            "done\n"
            "wait $p; code=$?\n"
            # srun exits 1 or 137 when cancelled, never 143.
            f"[ -e {cancel} ] && code=143\n"
            f"[ -e {exit_path} ] || echo $code > {exit_path}\n"
        )
        await self.inner.put_file(script.encode(), f"{run}.job")
        spool = f"{handle.alloc_dir}/spool/{step}"
        await self.inner.put_file(launcher.encode(), f"{spool}.tmp")
        # Renamed whole, so the pilot never runs a half-written launcher.
        proc = await self.inner.run_command_sync(
            f"mv {q(spool + '.tmp')} {q(spool + '.sh')}"
        )
        if await proc.wait() == 0:
            return
        # Over SSH a failed mv may still have run: the step name is the
        # key, so a launcher found spooled or claimed has landed.
        try:
            proc = await self.inner.run_command_sync(
                f"[ -e {q(spool + '.sh')} ] || [ -e {q(run + '.sh')} ] "
                f"|| {{ rm -f {q(spool + '.tmp')}; exit 1; }}"
            )
            landed = await proc.wait() == 0
        except Exception:
            landed = False
        if not landed:
            raise TaskExecutionError(
                _("Could not hand the task to allocation %(job_id)s.")
                % {"job_id": handle.job_id}
            )

    async def _step_id(self, job_id: str, step: str) -> str | None:
        """The id (``<job>.<n>``) of the running step named *step*."""
        proc = await self.inner.run_command_sync(
            f'squeue -s -h -j {job_id} -o "%i|%j"'
        )
        out, _err = await proc.communicate()
        for line in out.decode(errors="replace").splitlines():
            step_id, _sep, name = line.strip().partition("|")
            if name == step and re.fullmatch(r"\d+(_\d+)?\.\d+", step_id):
                return step_id
        return None

    async def _time_limit_context(self) -> str:
        """
        What was asked for when sbatch rejects the time limit, and the
        partition's MaxTime when ``sinfo`` is willing to say.

        Slurm's own message ("Requested time limit is invalid (missing or
        exceeds some limit)") names neither, and the usual cause is a
        ``time_limit`` beyond the MaxTime of the partition the job landed in
        -- often the default one, when ``partition`` is unset. Best effort:
        the error is only ever made more specific, never raised harder.
        """
        requested = self.resolved_resources().time_limit
        where = (
            _("partition %(partition)s") % {"partition": self.partition}
            if self.partition
            else _("the default partition")
        )
        context = _(" (requested --time=%(time)s on %(where)s") % {
            "time": requested,
            "where": where,
        }
        max_time = await self._partition_max_time()
        if max_time:
            context += _(", whose MaxTime is %(max_time)s") % {
                "max_time": max_time
            }
        return context + ")"

    async def _partition_max_time(self) -> str | None:
        """
        ``MaxTime`` of this target's partition (the cluster default when none
        is set), or ``None`` when ``sinfo`` cannot say.
        """
        try:
            proc = await self.inner.run_command_sync("sinfo -h -o '%P %l'")
            out, _err = await proc.communicate()
            if proc.returncode != 0:
                return None
            for line in out.decode(errors="replace").splitlines():
                # "%P" marks the default partition with a trailing "*".
                name, _sep, max_time = line.strip().partition(" ")
                matches = (
                    name.rstrip("*") == self.partition
                    if self.partition
                    else name.endswith("*")
                )
                if matches:
                    return max_time or None
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not read the partition's MaxTime: %(err)s")
                % {"err": exc}
            )
        return None

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

    async def _announce(
        self,
        job_id: str,
        state: str,
        reason: str | None = None,
        nodes: str | None = None,
        partition_nodes: dict[str, int] | None = None,
    ) -> None:
        """
        Publish a job state on the event bus and record it, if it changed.

        A change in ``partition_nodes`` alone is announced and recorded too
        (the UI refreshes on events), but is not a new entry in the state
        history.

        Best effort throughout: nothing here may fail a job. There is no bus
        at all outside a Horus context (the plain CLI), and a subscriber that
        raises is the subscriber's problem, not this job's; likewise a
        failure to persist the record costs a stale observation, not the job.
        """
        if (state, reason, partition_nodes) == self._last_state:
            return
        transition = self._last_state is None or self._last_state[:2] != (
            state,
            reason,
        )
        self._last_state = (state, reason, partition_nodes)
        if self._record is not None:
            record = (
                self._record.with_state(state, reason, nodes)
                if transition
                else self._record
            )
            self._record = record.model_copy(
                update={"partition_nodes": partition_nodes}
            )
            await self._write_record()
        if reason:
            message = _("Slurm job %(job_id)s is %(state)s (%(reason)s)") % {
                "job_id": job_id,
                "state": state,
                "reason": reason,
            }
        else:
            message = _("Slurm job %(job_id)s is %(state)s") % {
                "job_id": job_id,
                "state": state,
            }
        try:
            HorusContext.get_context().bus.emit(
                SlurmJobEvent(
                    task_id=self._task.id if self._task is not None else "",
                    job_id=job_id,
                    state=state,
                    reason=reason,
                    nodes=nodes,
                    partition_nodes=partition_nodes,
                    message=message,
                )
            )
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not announce Slurm job state: %(err)s") % {"err": exc}
            )

    async def _write_record(self) -> None:
        """
        Persist the current job record as a side product. Best effort: an
        unbound target (no task) or a write failure just means observers see
        a stale or absent record until the next transition.
        """
        if self._task is None or self._record is None:
            return
        try:
            await self.inner.put_file(
                self._record.model_dump_json().encode(),
                f"{self._task.side_artifacts_dir}/{RECORD_FILE}",
            )
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not record Slurm job state: %(err)s") % {"err": exc}
            )

    def _sbatch_summary(self) -> dict[str, str | int | list[str] | None]:
        """The sbatch options this job was actually submitted with."""
        resolved = self.resolved_resources()
        return {
            "partition": self.partition,
            "account": self.account,
            "qos": self.qos,
            "nodes": self.nodes,
            "ntasks": self.ntasks,
            "cpus_per_task": resolved.cpus_per_task,
            "mem": resolved.mem,
            "time_limit": resolved.time_limit,
            "gres": resolved.gres,
            "extra_sbatch_args": list(self.extra_sbatch_args),
        }

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
        queue = await self._queue_state(handle)
        gone = queue is not None and queue.gone
        if queue is not None and queue.state:
            await self._announce(
                job_id,
                queue.state,
                queue.reason,
                queue.nodes,
                await self._partition_summary(queue),
            )

        if await self.inner.path_exists(exit_code_path):
            raw = await self.inner.get_file(exit_code_path)
            exit_code = int(raw.decode().strip())
            await self._record_exit_code(exit_code)
            return exit_code

        if gone and self._cancelling:
            # A cancelled job never writes exit_code, so waiting for one only
            # outlasts the caller's grace period and loses the final record.
            final = await self._final_state(handle)
            await self._announce(job_id, *(final or ("CANCELLED", None)))
            return 1

        # A shared allocation's job id stays set: the run-end release still
        # cleans it up, and the next launch sees it gone and resubmits.
        if gone:
            # Left the queue without exit_code visible yet. This can be a race:
            # Slurm sometimes flushes exit_code a few seconds after the job
            # disappears from squeue. Retry briefly before concluding it was
            # killed by the scheduler (OOM, node failure, wall-time) or
            # cancelled outside Horus.
            retried_code = await self._retry_exit_code(exit_code_path)
            if retried_code is not None:
                await self._record_exit_code(retried_code)
                return retried_code

            # sacct usually knows what killed it (TIMEOUT, OUT_OF_MEMORY,
            # NODE_FAIL, ...); GONE only when accounting cannot say.
            final_state, reason = await self._final_state(handle) or (
                GONE,
                None,
            )
            await self._announce(job_id, final_state, reason)
            horus_logger.log.error(
                _(
                    "The job vanished from queue without writing an exit "
                    "code (%(state)s)."
                )
                % {"state": final_state}
            )
            return 1

        return None

    async def _record_exit_code(self, exit_code: int) -> None:
        """Persist the job's exit code on its record. Best effort."""
        if self._record is None:
            return
        self._record = self._record.model_copy(update={"exit_code": exit_code})
        await self._write_record()

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

        A queued job has not created its log files yet, so nothing is read
        until the job is actually running (or done); ``_read_log`` returns
        empty until the files exist.

        ponytail: re-reads both logs whole on every call, and
        ``PollingChannelProcess.stream`` calls this once per poll interval, so
        tailing a long-running chatty job is O(n^2) in log size. Switch to a
        ranged read (``tail -c +offset``) if job logs get large enough to
        matter.
        """
        stdout_path, stderr_path = self._log_paths(handle.job_dir)
        return await self._read_log(stdout_path), await self._read_log(
            stderr_path
        )

    async def send_signal(self, handle: JobHandle, sig: int) -> None:
        """
        Send signal to the job.
        """
        job_id = self._handle_job_id(handle)
        step = (handle.extra or {}).get("step")
        if step is not None:
            await self._signal_step(handle, job_id, step, sig)
            return
        if sig in (signal.SIGKILL, signal.SIGTERM):
            # A plain scancel ends the whole job, batch step included, and
            # Slurm accounts for it as CANCELLED. ``--signal`` only reaches
            # the batch step with ``--full``.
            proc = await self.inner.run_command_sync(f"scancel {job_id}")
            await proc.wait()
            self._cancelling = True
            await self._announce(job_id, CANCELLING)
            return
        try:
            name = signal.Signals(sig).name.removeprefix("SIG")
        except ValueError:
            name = "TERM"
        proc = await self.inner.run_command_sync(
            f"scancel --signal={name} {job_id}"
        )
        await proc.wait()

    async def _signal_step(
        self, handle: JobHandle, job_id: str, step: str, sig: int
    ) -> None:
        """
        Signal one task's job step, never the allocation the others share.

        A stop leaves a ``cancel`` mark the launcher watches, whether srun
        is waiting for room or running; a step still spooled is taken back
        and recorded as cancelled at once.
        """
        q = shlex.quote
        alloc = (handle.extra or {}).get("alloc_dir", "")
        if sig not in (signal.SIGKILL, signal.SIGTERM):
            step_id = await self._step_id(job_id, step)
            if step_id is not None:
                try:
                    name = signal.Signals(sig).name.removeprefix("SIG")
                except ValueError:
                    name = "TERM"
                proc = await self.inner.run_command_sync(
                    f"scancel --signal={name} {step_id}"
                )
                await proc.wait()
            return
        exit_path = q(f"{handle.job_dir}/exit_code")
        proc = await self.inner.run_command_sync(
            f"touch {q(f'{alloc}/run/{step}.cancel')}; "
            f"rm {q(f'{alloc}/spool/{step}.sh')} 2>/dev/null "
            f"&& echo 143 > {exit_path}"
        )
        await proc.wait()
        self._cancelling = True
        await self._announce(job_id, CANCELLING)

    # --- helpers -------------------------------------------------------------

    def resolved_resources(self) -> SbatchResources:
        """
        The sbatch options this target will actually submit with.

        Merges the bound task's portable ``resources`` into the explicit
        sbatch fields, explicit winning. ``bind()`` is what makes the task
        reachable here -- it exists precisely "so resource-aware targets can
        read ``task.resources`` while provisioning" -- and it runs ahead of
        dispatch, so the request is available by the time a script is built.

        Public because it is the honest answer to "what will this actually
        ask Slurm for?", which is worth being able to inspect (a dry run, a
        UI preview, a test) without submitting anything.

        Note:
            An unbound target (no task, e.g. a control-plane command) simply
            resolves to its own explicit fields.
        """
        request = self._task.resources if self._task is not None else None
        if request is not None and request.vram_gb is not None:
            # No portable translation exists -- see resolve_resources.
            horus_logger.log.debug(
                _(
                    "Ignoring vram_gb=%(vram)s: Slurm has no portable flag "
                    "for GPU memory. Express it in gres or extra_sbatch_args."
                )
                % {"vram": request.vram_gb}
            )
        return resolve_resources(
            request,
            cpus_per_task=self.cpus_per_task,
            mem=self.mem,
            time_limit=self.time_limit,
            gres=self.gres,
        )

    def _log_paths(self, job_dir: str) -> tuple[str, str]:
        """
        Where the job's stdout and stderr go.

        A bound task gets them in its side-artifacts dir, so the untouched
        files are collected next to the job record; an unbound target keeps
        them in *job_dir*.
        """
        if self._task is None:
            return f"{job_dir}/stdout.log", f"{job_dir}/stderr.log"
        side = self._task.side_artifacts_dir
        return f"{side}/{STDOUT_FILE}", f"{side}/{STDERR_FILE}"

    def _build_sbatch_script(
        self,
        cmd: str,
        *,
        cwd: str | None,
        env: dict[str, str] | None,
        job_dir: str,
    ) -> str:
        stdout_path, stderr_path = self._log_paths(job_dir)
        lines = self._sbatch_header(
            f"horus-{Path(job_dir).name}",
            stdout_path,
            stderr_path,
            self.resolved_resources(),
        )
        lines.append("")
        lines += self._script_body(cmd, cwd, env, job_dir)
        return "\n".join(lines) + "\n"

    def _sbatch_header(
        self,
        job_name: str,
        stdout_path: str,
        stderr_path: str,
        resolved: SbatchResources,
    ) -> list[str]:
        """The shebang and ``#SBATCH`` lines of a batch script."""
        # Checked here too: the validators run on load, not on assignment.
        for option, value in (
            ("--job-name", job_name),
            ("--output", stdout_path),
            ("--error", stderr_path),
            ("--partition", self.partition),
            ("--account", self.account),
            ("--qos", self.qos),
            ("--mem", resolved.mem),
            ("--time", resolved.time_limit),
            ("--gres", resolved.gres),
        ):
            if value and _STRICT_BAD.search(value):
                raise TaskExecutionError(
                    _(
                        "Invalid value for %(option)s: %(value)r may not "
                        "contain whitespace, '#', quotes or backslashes."
                    )
                    % {"option": option, "value": value}
                    + (
                        _(" Check the working directory and the task id.")
                        if option in ("--output", "--error")
                        else ""
                    )
                )
        for arg in self.extra_sbatch_args:
            if _LINE_BAD.search(arg):
                raise TaskExecutionError(
                    _("Invalid extra sbatch argument %(arg)r: a line break.")
                    % {"arg": arg}
                )
        lines = ["#!/bin/bash"]
        lines.append(f"#SBATCH --job-name={job_name}")
        lines.append(f"#SBATCH --output={stdout_path}")
        lines.append(f"#SBATCH --error={stderr_path}")
        lines.append(f"#SBATCH --nodes={self.nodes}")
        lines.append(f"#SBATCH --ntasks={self.ntasks}")
        lines.append(f"#SBATCH --cpus-per-task={resolved.cpus_per_task}")
        if self.partition:
            lines.append(f"#SBATCH --partition={self.partition}")
        if self.account:
            lines.append(f"#SBATCH --account={self.account}")
        if self.qos:
            lines.append(f"#SBATCH --qos={self.qos}")
        if resolved.mem:
            lines.append(f"#SBATCH --mem={resolved.mem}")
        if resolved.time_limit:
            lines.append(f"#SBATCH --time={resolved.time_limit}")
        if resolved.gres:
            lines.append(f"#SBATCH --gres={resolved.gres}")
        for arg in self.extra_sbatch_args:
            lines.append(f"#SBATCH {arg}")
        return lines

    def _script_body(
        self,
        cmd: str,
        cwd: str | None,
        env: dict[str, str] | None,
        job_dir: str,
    ) -> list[str]:
        """Run *cmd* in *cwd* with *env*, then record its exit code."""
        target_cwd = cwd or self.resolved_working_directory
        lines = [f"cd {shlex.quote(target_cwd)}"]
        for key, value in (env or {}).items():
            lines.append(f"export {key}={shlex.quote(value)}")

        # Subshell so a top-level `exit` in cmd still reaches the write --
        # same reasoning as build_detach_command's `( inner ); echo $? > ...`.
        lines.append(f"( {cmd} ); echo $? > {shlex.quote(job_dir)}/exit_code")
        return lines

    async def _read_log(self, path: str) -> bytes:
        """
        Best-effort read of a job log file.

        Slurm only writes the log files once the job starts running, so a
        missing file is the norm for a queued job, not an error. Existence is
        probed before reading because transports do not agree on how a missing
        file surfaces: the SSH transport wraps it in ``TaskExecutionError``
        rather than ``FileNotFoundError``.
        """
        if not await self.inner.path_exists(path):
            return b""
        try:
            return await self.inner.get_file(path)
        except Exception as exc:
            # A transient read failure is not worth a UI error; the next poll
            # retries and, until then, empty is honest.
            horus_logger.log.debug(
                _("Could not read log %(path)s: %(error)s")
                % {"path": path, "error": exc}
            )
            return b""

    async def _queue_state(self, handle: JobHandle) -> QueueState | None:
        """
        :meth:`_read_queue`, shared by a named target's copies: one read per
        :attr:`queue_check_interval`. A failed read is never stored, but the
        copies waiting on it share it rather than each trying again.
        """
        if not self.releasable:
            return await self._read_queue(handle)
        alloc = self._alloc
        job_id = self._handle_job_id(handle)
        cached = alloc.cached_queue(job_id, self.queue_check_interval)
        if cached is not None:
            return cached
        waited_from = time.monotonic()
        async with alloc.refresh_lock:
            # Another copy may have read it while this one waited.
            cached = alloc.cached_queue(job_id, self.queue_check_interval)
            if cached is not None:
                return cached
            failed = alloc.queue_failed
            if (
                failed is not None
                and failed[0] == job_id
                and failed[1] >= waited_from
            ):
                return None
            queue = await self._read_queue(handle)
            if queue is None:
                alloc.queue_failed = (job_id, time.monotonic())
            elif job_id == alloc.job_id:
                # A task of an ended allocation must not evict the
                # current one's reading.
                alloc.queue_job_id = job_id
                alloc.queue = queue
                alloc.queue_at = time.monotonic()
            return queue

    async def _read_queue(self, handle: JobHandle) -> QueueState | None:
        """
        What ``squeue`` says about the job right now.

        Returns the state (``PENDING``, ``RUNNING``, …) with ``%R`` split into
        a reason (parenthesised, e.g. ``(Resources)``) or a node list (a
        running job's ``node01``); a state of ``""`` when the job is no longer
        in the queue; or ``None`` when squeue could not answer. Only an empty
        *successful* result means gone -- a nonzero exit (e.g. a slurmctld
        hiccup) must NOT be read that way, or a transient scheduler blip will
        fail jobs that are still fine.
        """
        proc = await self.inner.run_command_sync(
            f'squeue -h -j {self._handle_job_id(handle)} -o "%T|%R|%P"'
        )
        out, _err = await proc.communicate()
        if proc.returncode != 0:
            return None
        text = out.decode(errors="replace").strip()
        if not text:
            return QueueState("")
        # A job array reports one line per element; the first is enough for a
        # single job, which is all this target submits.
        state, where, partition = [
            *text.splitlines()[0].strip().split("|", 2),
            "",
            "",
        ][:3]
        if where.startswith("(") and where.endswith(")"):
            reason = where[1:-1]
            return QueueState(
                state,
                None if reason == "None" else reason,
                partition=partition or None,
            )
        return QueueState(
            state, nodes=where or None, partition=partition or None
        )

    async def _partition_summary(
        self, queue: QueueState
    ) -> dict[str, int] | None:
        """
        Node state -> count for the job's partition while it is pending on a
        :data:`NODE_REASONS` reason, else ``None``.

        Re-read when the reason changes, and otherwise at most every
        :data:`PARTITION_SUMMARY_INTERVAL` seconds.
        """
        # A named target's copies share one sinfo read.
        cache = self._alloc if self.releasable else self
        reason = queue.reason or ""
        if queue.state != "PENDING" or not reason.startswith(NODE_REASONS):
            cache._partition_checked = None  # noqa: SLF001
            cache._partition_nodes = None  # noqa: SLF001
            return None

        def fresh() -> bool:
            checked = cache._partition_checked  # noqa: SLF001
            return (
                checked is not None
                and checked[0] == reason
                and time.monotonic() - checked[1] < PARTITION_SUMMARY_INTERVAL
            )

        if fresh():
            return cache._partition_nodes  # noqa: SLF001
        # Copies polling meanwhile wait for this read, not the old summary.
        async with self._alloc.refresh_lock:
            if fresh():
                return cache._partition_nodes  # noqa: SLF001
            nodes = await self._sinfo(self.partition or queue.partition)
            cache._partition_nodes = nodes  # noqa: SLF001
            cache._partition_checked = (reason, time.monotonic())  # noqa: SLF001
            return nodes

    async def _sinfo(self, partition: str | None) -> dict[str, int] | None:
        """
        ``sinfo``'s node counts per state for *partition*, with the trailing
        state flags (``*`` not responding, ``~`` powered down, ...) stripped.
        Best effort: any failure is ``None``, never a failed job.
        """
        if not partition:
            return None
        try:
            proc = await self.inner.run_command_sync(
                f'sinfo -h -p {shlex.quote(partition)} -o "%T|%D"'
            )
            out, err = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(err.decode(errors="replace").strip())
            counts: dict[str, int] = {}
            for line in out.decode(errors="replace").splitlines():
                if not line.strip():
                    continue
                state, _sep, count = line.strip().partition("|")
                state = state.rstrip("*~#!%$@^-").lower()
                counts[state] = counts.get(state, 0) + int(count)
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not summarise partition %(partition)s: %(err)s")
                % {"partition": partition, "err": exc}
            )
            return None
        return counts

    async def _final_state(
        self, handle: JobHandle
    ) -> tuple[str, str | None] | None:
        """
        How ``sacct`` says the job ended (``CANCELLED``, ``TIMEOUT``, …), and
        its recorded reason, if any.

        ``None`` when accounting cannot answer: a nonzero exit (no slurmdbd)
        or nothing recorded yet. ``sacct`` appends who cancelled a job
        (``CANCELLED by 1000``), so only the first word is the state.
        """
        proc = await self.inner.run_command_sync(
            f"sacct -n -X -P -j {self._handle_job_id(handle)} "
            "-o State,ExitCode,Reason"
        )
        out, _err = await proc.communicate()
        if proc.returncode != 0:
            return None
        lines = out.decode(errors="replace").strip().splitlines()
        if not lines:
            return None
        state, _code, reason = [*lines[0].split("|"), "", ""][:3]
        words = state.split()
        if not words:
            return None
        reason = reason.strip()
        return words[0], None if reason in ("", "None") else reason


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
