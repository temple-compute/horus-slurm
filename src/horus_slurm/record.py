#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
The Slurm job record: a side-product JSON that *is* the durable observation of
a job, so an embedding host (tc-os) never has to reconstruct one from the
event bus.

The event bus (see :mod:`horus_slurm.events`) is low-frequency and meant for a
human reading a run's log; it is not a good source of truth for "what is this
job's state right now", because a consumer that fetches or caches only the
tail of a long event stream can miss the ``SUBMITTED`` event this record's
queue timings depend on. The record is rewritten whole on every transition
instead, so reading it once always gives the complete history.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field

RECORD_FILE = "slurm-job.json"
"""
Filename this record is written under, inside ``task.side_artifacts_dir``.

An embedding host that wants to recognise this record by filename (tc-os maps
it to an artifact kind in ``tc_plugins.artifact_kind``) depends on this exact
name -- keep the two in sync if it ever changes.
"""

STDOUT_FILE = "slurm-stdout.log"
"""
Filename the job's raw Slurm stdout is written under, inside
``task.side_artifacts_dir``, so it is collected next to :data:`RECORD_FILE`.
"""

STDERR_FILE = "slurm-stderr.log"
"""Like :data:`STDOUT_FILE`, for the job's raw Slurm stderr."""


class StateChange(BaseModel):
    """One entry in a job's state history."""

    state: str
    at: datetime
    reason: str | None = None


class SlurmJobRecord(BaseModel):
    """
    Everything known about one submitted Slurm job.

    ``state`` mirrors ``states[-1].state`` for a consumer that only wants the
    current state; ``states`` is the full history. Submitted/started/finished
    timestamps are deliberately not duplicated as separate fields -- they are
    derivable from ``states`` and a second copy would only invite the two to
    disagree.
    """

    job_id: str
    state: str
    states: list[StateChange] = Field(default_factory=list)
    script: str
    script_path: str
    working_dir: str
    job_dir: str
    stdout_path: str
    stderr_path: str
    sbatch: dict[str, str | int | list[str] | None] = Field(
        default_factory=dict
    )
    exit_code: int | None = None
    reason: str | None = None
    """Why the job is in its current state, from squeue/sacct; see events."""
    nodes: str | None = None
    """The node list the job is running on, while it runs."""
    partition_nodes: dict[str, int] | None = None
    """Node state -> count for the job's partition, while it waits on it."""

    def with_state(
        self,
        state: str,
        reason: str | None = None,
        nodes: str | None = None,
    ) -> SlurmJobRecord:
        """Return a copy with *state* appended to the history."""
        return self.model_copy(
            update={
                "state": state,
                "reason": reason,
                "nodes": nodes,
                "states": [
                    *self.states,
                    StateChange(
                        state=state, at=datetime.now(UTC), reason=reason
                    ),
                ],
            }
        )
