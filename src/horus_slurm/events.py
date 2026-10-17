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

from typing import Literal

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
    reason: str | None = None
    """
    Why the job is in ``state``: squeue's parenthesised ``%R`` with the
    parentheses stripped (``Resources``, ``Priority``,
    ``ReqNodeNotAvail, UnavailableNodes:gpu[01-02]``), or sacct's ``Reason``
    once the job has ended. ``None`` when Slurm gives none.
    """
    nodes: str | None = None
    """The node list (squeue's unparenthesised ``%R``) while the job runs."""
    partition_nodes: dict[str, int] | None = None
    """
    Node state -> count for the job's partition (``{"idle": 0, "drained":
    2}``), only while it is pending on a resource or node reason.
    """


AllocationPhase = Literal[
    "queued",
    "failed",
    "ready",
    "busy",
    "holding",
    "draining",
    "drain_failed",
    "released",
]
"""
Phases of a shared allocation: ``queued``, ``failed``, ``ready`` and
``released`` from the target, the rest passed on from the lease manager.
"""


class SlurmAllocationEvent(BaseEvent):
    """
    The shared allocation behind a named target changed phase.

    Shaped like a terraform node event (``node_key``, ``phase``) so a host
    shows the allocation the way it shows a provisioned machine.
    """

    event_type: str = "slurm_allocation_event"
    phase: AllocationPhase
    task_id: str
    """The task of the copy that sent it; ``""`` without one."""
    node_key: str
    """The target's lease key, the same for all its copies."""
    target_ref: str | None
    generation: int
    member_task_ids: list[str]
    """Tasks bound to this generation of the allocation."""
    job_id: str | None = None
    reason: str | None = None
    """
    For ``released``: ``done``, ``drained``, ``idle_cap``, ``run_end``,
    ``abort`` or ``gone``. For ``failed``: the error.
    """
