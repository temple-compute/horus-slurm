#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
The resource scope for work that Slurm owns.
"""

from dataclasses import dataclass

from horus_runtime.core.resources import ResourceScope


@dataclass(frozen=True)
class SlurmJobScope(ResourceScope):
    """
    The work is a Slurm job, not a process the orchestrator can reach.

    ``sbatch`` returns as soon as the job is queued, so the only handle the
    submitting side holds is a job id that means something to the scheduler
    and nothing to the operating system. The work itself runs later, on a
    compute node the orchestrator has no process view of, so an observer must
    ask Slurm about the job rather than walk a process tree.
    """

    kind: str = "slurm_job"
    job_id: str | None = None
    #: Set when the id is not known yet but the target writes it to this path
    #: once ``sbatch`` accepts the job. The scope is declared before the job
    #: is submitted, so this is normally the only identifier available.
    job_id_file: str | None = None
