#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Resource scope for work that Slurm owns.
"""

from dataclasses import dataclass

from horus_runtime.core.resources import ResourceScope


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
