#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Resource scope for work that Slurm owns, and the translation of a portable
``ResourceRequest`` into sbatch directives.
"""

from dataclasses import dataclass

from horus_runtime.core.resources import ResourceRequest, ResourceScope

DEFAULT_CPUS_PER_TASK = 1
"""
What ``--cpus-per-task`` falls back to when neither the target nor the task
says anything. Slurm's own default, made explicit.
"""

_SUFFIX_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
"""Units of a suffixed duration such as ``"2h"`` or ``"90m"``."""


def to_slurm_time(value: str | None) -> str | None:
    """
    Render a suffixed duration (``"30s"``, ``"90m"``, ``"2h"``, ``"1d"``) in
    Slurm's ``[D-]HH:MM:SS`` syntax.

    ``resources.walltime`` is documented as accepting those suffixes, but
    sbatch does not, and passing ``"2h"`` through verbatim had it rejected
    as an invalid time limit. Anything that is not a suffixed duration --
    ``"01:00:00"``, ``"1-12:00"``, bare minutes, or something this cannot
    read -- is already Slurm's to judge and passes through untouched.
    """
    if value is None:
        return None
    text = value.strip()
    unit = _SUFFIX_SECONDS.get(text[-1:].lower())
    if unit is None:
        return value
    try:
        amount = float(text[:-1])
    except ValueError:
        return value
    total = round(amount * unit)
    days, rest = divmod(total, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock


def slurm_seconds(value: str | None) -> int | None:
    """
    Seconds in a Slurm time (``MM``, ``MM:SS``, ``HH:MM:SS``,
    ``D-HH[:MM[:SS]]``) or a suffixed duration; ``None`` when unreadable.
    """
    text = to_slurm_time(value)
    if not text:
        return None
    days, _sep, clock = text.strip().rpartition("-")
    try:
        parts = [int(p) for p in clock.split(":")]
        day_count = int(days) if days else 0
    except ValueError:
        return None
    if days:
        parts = [*parts, 0, 0][:3]  # HH[:MM[:SS]] after a day count
    elif len(parts) == 1:
        parts = [0, parts[0], 0]  # bare minutes
    else:
        parts = [0, 0, *parts][-3:]
    hours, minutes, seconds = parts
    return ((day_count * 24 + hours) * 60 + minutes) * 60 + seconds


_MEM_MB = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}
"""Megabytes per unit of a Slurm memory size; a bare number is megabytes."""


def slurm_mem_mb(value: str | None) -> int | None:
    """Megabytes in a Slurm memory size (``4G``, ``512M``, ``2048``)."""
    if not value:
        return None
    text = value.strip().lower().removesuffix("b")
    unit = _MEM_MB.get(text[-1:])
    try:
        return int(float(text[:-1] if unit else text) * (unit or 1))
    except ValueError:
        return None


@dataclass(frozen=True)
class SlurmJobScope(ResourceScope):
    """
    The work is a Slurm job, not a process the orchestrator can reach.
    """

    kind: str = "slurm_job"
    job_id: str | None = None
    #: Where the target writes the id, since the scope is declared before
    #: the job is submitted.
    job_id_file: str | None = None


@dataclass(frozen=True)
class SbatchResources:
    """
    The four sbatch options a ``ResourceRequest`` can determine, resolved.
    """

    cpus_per_task: int
    mem: str | None
    time_limit: str | None
    gres: str | None


def resolve_resources(
    request: ResourceRequest | None,
    *,
    cpus_per_task: int | None = None,
    mem: str | None = None,
    time_limit: str | None = None,
    gres: str | None = None,
) -> SbatchResources:
    """
    Merge a task's portable resource request with explicit sbatch options.

    Precedence is *explicit wins*: a value set on the target is what the site
    operator asked for in Slurm's own vocabulary, and must never be silently
    overridden by a portable hint. The request only fills the gaps, and
    :data:`DEFAULT_CPUS_PER_TASK` fills whatever is left.

    This is the whole of the portable -> Slurm translation, kept as a pure
    function so it can be reasoned about (and tested) without a scheduler,
    a target, or a bound task.

    Args:
        request: The task's ``resources``, or ``None`` when it declared none.
        cpus_per_task: Explicit ``--cpus-per-task``, or ``None`` to derive it.
        mem: Explicit ``--mem``, or ``None`` to derive it.
        time_limit: Explicit ``--time``, or ``None`` to derive it. Either
            source may use a suffixed duration (``"2h"``); see
            :func:`to_slurm_time`.
        gres: Explicit ``--gres``, or ``None`` to derive it.

    Returns:
        The resolved directives.

    Note:
        ``ResourceRequest.vram_gb`` has no portable sbatch equivalent -- sites
        express GPU memory as a gres type (``gpu:a100:1``), a ``--constraint``,
        or a dedicated partition, and there is no way to pick between those
        without site knowledge. It is therefore not translated; say it exactly
        once, in ``gres`` or ``extra_sbatch_args``.
    """
    if request is None:
        return SbatchResources(
            cpus_per_task=(
                cpus_per_task
                if cpus_per_task is not None
                else DEFAULT_CPUS_PER_TASK
            ),
            mem=mem,
            time_limit=to_slurm_time(time_limit),
            gres=gres,
        )

    if cpus_per_task is None:
        cpus_per_task = request.cpus
    if mem is None and request.memory_gb is not None:
        mem = f"{request.memory_gb}G"
    if time_limit is None:
        time_limit = request.walltime
    # gpus defaults to 0 (no GPU), so only a positive count asks for one.
    if gres is None and request.gpus > 0:
        gres = f"gpu:{request.gpus}"

    return SbatchResources(
        cpus_per_task=(
            cpus_per_task
            if cpus_per_task is not None
            else DEFAULT_CPUS_PER_TASK
        ),
        mem=mem,
        time_limit=to_slurm_time(time_limit),
        gres=gres,
    )
