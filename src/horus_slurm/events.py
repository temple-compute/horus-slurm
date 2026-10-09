#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Events published while a Slurm job is in flight.

A Slurm job spends most of its life somewhere an observer cannot see: queued,
with no process, no output, and no exit code. The only handle on it is the job
id and whatever ``squeue`` says about it, and both are known to
:class:`~horus_slurm.target.slurm.SlurmTarget` on every poll. This event is how
they leave the target — an embedding host (a GUI, a log viewer) subscribes to
the bus and can then show *why* a task that reports "running" has produced
nothing for the last hour.

Deliberately low-frequency: one event on submission and one per distinct state
transition, not one per poll. Consumers that persist events (tc-os writes a row
per event) get a handful per job.
"""

from horus_runtime.event.base import BaseEvent

SUBMITTED = "SUBMITTED"
"""State reported at ``sbatch`` time, before ``squeue`` has anything to say."""

CANCELLING = "CANCELLING"
"""
State reported once Horus has asked Slurm to cancel the job, until the queue
or accounting reports how it actually ended.
"""

GONE = "GONE"
"""
State reported when the job left the queue without writing an exit code —
killed by the scheduler (OOM, node failure, wall-time) or cancelled outside
Horus.
"""


class SlurmJobEvent(BaseEvent):
    """
    A Slurm job was submitted, or changed state in the queue.

    ``state`` is whatever ``squeue -o %T`` reports (``PENDING``, ``RUNNING``,
    ``COMPLETING``, ``SUSPENDED``, …), plus the synthetic states
    :data:`SUBMITTED`, :data:`CANCELLING` and :data:`GONE` that bracket the
    queue's own view.
    It is passed through verbatim rather than mapped onto a Horus enum: Slurm
    grows states between releases, and a consumer showing an unrecognised one
    is strictly better than a target that swallows it.
    """

    event_type: str = "slurm_job"
    task_id: str
    job_id: str
    state: str
