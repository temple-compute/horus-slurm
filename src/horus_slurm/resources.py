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
        time_limit: Explicit ``--time``, or ``None`` to derive it.
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
            time_limit=time_limit,
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
        time_limit=time_limit,
        gres=gres,
    )
