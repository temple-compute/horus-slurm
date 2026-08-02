#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Resource probe for work that Slurm owns.

Two sources, because neither alone is enough:

- The time series comes from the sampler the resource monitor injects into
  the command. Slurm puts that command in the batch script, so the sampler
  runs *on the compute node*, watching the real process tree, and costs the
  scheduler nothing. Inherited wholesale from :class:`ShellSamplerProbe`.
- The totals come from ``sacct`` at the end. Slurm's own accounting is the
  only thing that still reports for a job it killed itself — an OOM or a
  wall-time kill takes the sampler down with the job, so the last thing the
  series shows is a job that was doing fine.
"""

import asyncio
import re
import time
from typing import Any, ClassVar

from horus_resource_monitor.probe.shell import ShellSamplerProbe
from horus_runtime.core.resources import ResourceScope
from horus_runtime.core.target.base import BaseTarget
from horus_runtime.logging import horus_logger
from pydantic import PrivateAttr

from horus_slurm.i18n import tr as _
from horus_slurm.resources import SlurmJobScope

SACCT_FORMAT = "MaxRSS,TotalCPU,Elapsed,State,ExitCode"

#: Read once per task, after the job has finished.
SACCT_COMMAND = (
    f"sacct -n -P -o {SACCT_FORMAT} -j"  # -P: parsable, no aligned padding
)

#: Slurm flushes accounting a moment after a job leaves the queue, the same
#: race ``SlurmTarget.poll`` already works around for the exit code.
SACCT_RETRIES = 3
SACCT_DELAY = 2.0

_KB = 1024.0

#: sacct sizes carry a unit suffix, and default to kilobytes without one.
_UNITS = {"K": 1.0, "M": _KB, "G": _KB * _KB, "T": _KB * _KB * _KB}

_SIZE = re.compile(r"^([0-9.]+)([KMGT])?$", re.IGNORECASE)

#: ``[DD-[HH:]]MM:SS[.mmm]`` — the only shape sacct emits for a duration.
_CPU = re.compile(
    r"^(?:(?P<days>\d+)-)?"
    r"(?:(?P<hours>\d+):)?"
    r"(?P<minutes>\d+):(?P<seconds>\d+(?:\.\d+)?)$"
)

_SECONDS_PER_MINUTE = 60
_MINUTES_PER_HOUR = 60
_HOURS_PER_DAY = 24

#: "<MaxRSS>|<TotalCPU>" is the shortest usable form of an sacct row.
_MIN_FIELDS = 2


def parse_size_kb(text: str) -> float | None:
    """
    Parse a sacct size (``1234K``, ``1.5G``, bare ``1234``) into kilobytes.

    Returns ``None`` for anything unrecognisable, which includes the empty
    field sacct gives for a job step that never reported one.
    """
    match = _SIZE.match(text.strip())
    if match is None:
        return None
    try:
        value = float(match.group(1))
    except ValueError:
        return None
    return value * _UNITS.get((match.group(2) or "K").upper(), 1.0)


def parse_cpu_seconds(text: str) -> float | None:
    """
    Parse a sacct duration into seconds, or ``None`` if it is not one.
    """
    match = _CPU.match(text.strip())
    if match is None:
        return None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes"))
    try:
        seconds = float(match.group("seconds"))
    except ValueError:
        return None
    hours += days * _HOURS_PER_DAY
    minutes += hours * _MINUTES_PER_HOUR
    return seconds + minutes * _SECONDS_PER_MINUTE


def parse_sacct(text: str) -> dict[str, float] | None:
    """
    Pull the peak RSS and total CPU out of ``sacct`` output.

    A job reports several rows (the job, its ``.batch`` step, ``.extern``,
    any ``srun`` steps) and the interesting numbers live on the steps rather
    than the job row, so take the largest of each across every row.
    """
    rss: float | None = None
    cpu: float | None = None
    for line in text.strip().splitlines():
        fields = line.split("|")
        if len(fields) < _MIN_FIELDS:
            continue
        row_rss = parse_size_kb(fields[0])
        if row_rss is not None:
            rss = row_rss if rss is None else max(rss, row_rss)
        row_cpu = parse_cpu_seconds(fields[1])
        if row_cpu is not None:
            cpu = row_cpu if cpu is None else max(cpu, row_cpu)
    if rss is None and cpu is None:
        return None
    return {"rss_kb": rss or 0.0, "cpu_s": cpu or 0.0}


class SlurmJobProbe(ShellSamplerProbe):
    """
    Measures a Slurm job: the in-band series, plus Slurm's own totals.
    """

    kind: str = "slurm_job"
    priority: ClassVar[int] = 100

    _job_id: str | None = PrivateAttr(default=None)
    _job_id_file: str | None = PrivateAttr(default=None)
    _final: list[dict[str, Any]] = PrivateAttr(default_factory=list)

    @classmethod
    def supports(cls, scope: ResourceScope, target: BaseTarget) -> bool:
        """
        Slurm jobs, wherever the login node happens to be.

        Deliberately not conditioned on the target being reachable locally:
        the work is on a compute node either way, so there is nothing the
        orchestrator could look at directly even when it shares a host with
        the scheduler.
        """
        del target
        return isinstance(scope, SlurmJobScope)

    async def start(self, task: Any, scope: ResourceScope) -> None:
        """Note where the job id is, or will be."""
        if isinstance(scope, SlurmJobScope):
            self._job_id = scope.job_id
            self._job_id_file = scope.job_id_file
        await super().start(task, scope)

    async def drain(self) -> list[dict[str, Any]]:
        """The sampler's rows, plus the accounting row once it exists."""
        rows = await super().drain()
        if self._final:
            rows = [*rows, *self._final]
            self._final = []
        return rows

    async def _resolve_id(self) -> str | None:
        """
        The job id, read from the file the target writes once it submits.

        A miss is normal before submission and is retried on the next call.
        """
        if self._job_id or self._job_id_file is None or self._task is None:
            return self._job_id
        try:
            raw = await self._task.target.get_file(self._job_id_file)
        except Exception:
            return None
        self._job_id = raw.decode("utf-8", "replace").strip() or None
        return self._job_id

    async def stop(self) -> None:
        """Ask Slurm what the job actually used, then stop."""
        try:
            await self._collect_accounting()
        except Exception as exc:
            horus_logger.log.debug(
                _("Could not read Slurm accounting: %(err)s") % {"err": exc}
            )
        await super().stop()

    async def _collect_accounting(self) -> None:
        """Record one final row from ``sacct``, if it has anything to say."""
        job_id = await self._resolve_id()
        if job_id is None or self._task is None:
            horus_logger.log.debug(
                _("No Slurm job id for task %(task)s; no accounting read")
                % {"task": self._task.id if self._task else "?"}
            )
            return

        totals = await self._sacct(job_id)
        if totals is None:
            return
        self._final.append({"t": time.time(), **totals, "cpu_pct": 0.0})

    async def _sacct(self, job_id: str) -> dict[str, float] | None:
        """
        Run ``sacct`` for *job_id*, retrying while accounting catches up.

        Goes through ``run_command_sync``: this target submits by default, so
        ``run_command`` would queue a whole Slurm job to read a number.
        """
        target = self._task.target if self._task else None
        if target is None:
            return None

        for attempt in range(SACCT_RETRIES):
            proc = await target.run_command_sync(f"{SACCT_COMMAND} {job_id}")
            stdout, _stderr = await proc.communicate()
            totals = parse_sacct(stdout.decode("utf-8", "replace"))
            if totals is not None:
                return totals
            if attempt + 1 < SACCT_RETRIES:
                await asyncio.sleep(SACCT_DELAY)
        return None
