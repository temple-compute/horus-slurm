# Slurm allocation badges: backend design

Team 3, architect. Scope: the event horus-slurm emits for a shared
allocation, where it is emitted, and what tc-os has to do with it.
Inputs: `slurm-badges-plan.md`, `slurm-badges-codebase.md`,
`slurm-badges-ux.md`. No code changed.

Paths: horus-slurm is this repo. `TC` is
`/home/cdominguez/orca/workspaces/tc-os/slurm-same-target` (at `c8d71762`).
`RT` is `$TC/horus-runtime/src/horus_runtime`.

## 1. The event

Module: `src/horus_slurm/events.py`, next to `SlurmJobEvent`.

```python
AllocationPhase = Literal[
    "queued", "ready", "busy", "holding", "draining", "drain_failed",
    "released", "failed",
]


class SlurmAllocationEvent(BaseEvent):
    """A shared Slurm allocation changed phase."""

    event_type: str = "slurm_allocation_event"
    phase: AllocationPhase
    task_id: str
    node_key: str
    target_ref: str | None = None
    generation: int
    job_id: str | None = None
    member_task_ids: list[str] = Field(default_factory=list)
    reason: str | None = None
```

| Field | Value |
|---|---|
| `phase` | see section 2 |
| `task_id` | `self._task.id`, `""` without a task (as `_announce`) |
| `node_key` | `self.lease_key`. Named `node_key`, not `lease_key`, so the frontend's `applyNodeEvent` keys it like `terraform_node_event` (`machineStore.ts:39-56`) |
| `target_ref` | `self._ref` |
| `generation` | `self._alloc.generation` |
| `job_id` | `self._alloc.job_id`; on `released`, the id that was cancelled; `None` on `failed` |
| `member_task_ids` | `sorted(self._alloc.members)` (new set, section 3) |
| `reason` | `released`: the release reason (`done`, `drained`, `idle_cap`, `run_end`, `abort`). `failed`: the error text. Else `None` |

Not carried: `outputs` from `draining` (the frontend does not read it),
`phase_detail`, `provider`, instance fields. `emit_node_phase` accepts and
drops extra `**fields`.

### Queued carries no pending reason

Decision: `queued` has `reason=None`. The UX doc takes the reason for the
dot tooltip from the member's `slurm_job` record (`slurmReasonLabel`), which
the card already fetches for the tray. Reasons:

- At submit time squeue has nothing yet; the reason only exists later.
  Carrying it would mean a new `queued` event per reason change, which is
  what `slurm_job` already does per member.
- One fact, one place (UX rule): the allocation event says what, the
  `slurm_job` record says why.
- Cost: a member with no record yet shows `Queued in Slurm` without a reason.
  The UX doc already accepts that.

### Registration

None needed. `pyproject.toml:23-24` already has
`[project.entry-points."horus.event"] slurm_job = "horus_slurm.events"`.
The entry point loads the module, and `BaseEvent` is an `AutoRegistry`
keyed on `event_type` (`$RT/event/base.py:52,66`), so the new class
registers on import. The transport sends `model_dump(mode="json")` and never
looks the type up (`$TC/tc-plugins/src/tc_plugins/event_transport_to_api.py:104`).

## 2. Where it is emitted

One sync helper on `SlurmTarget`, `_emit_allocation(phase, reason=None)`.
It returns at once when `not self.releasable`, and wraps
`HorusContext.get_context().bus.emit(...)` in `try/except Exception` with a
debug log, like `_announce` (`slurm.py:913-927`). It never raises.

| Phase | Where | When |
|---|---|---|
| `queued` | `_allocation` (`slurm.py:632-645`) | right after `handle.job_id, _raw = await self._sbatch(...)` and `handle.alloc_dir` are set, inside the lock. Once per generation |
| `failed` | `_allocation`, same branch | `_make_private_dir`, `put_file` or `_sbatch` raises: emit `failed` with `reason=str(exc)`, then re-raise. `job_id` stays `None`, so the next member submits generation + 1 and its `queued` replaces the failed node |
| `ready` | `poll` (`slurm.py:978`), after `_queue_state` | first `RUNNING` for the current generation: `queue.state == "RUNNING"`, `job_id == self._alloc.job_id`, and `self._alloc.ready_generation != self._alloc.generation`. Set `ready_generation` before emitting. Once per generation however many members poll |
| `busy`, `holding`, `draining`, `drain_failed` | `emit_node_phase` | lease manager (`$RT/core/lease.py:315,375,577,583,620`). Passed through as is |
| `released` | `release` (`slurm.py:569`) | after `handle.job_id = None`, `reason=reason`, `job_id` the cancelled id. Only when it actually had a job id, so a second `release` emits nothing |

`emit_node_phase(phase, **fields)`:

```python
def emit_node_phase(self, phase: str, **fields: Any) -> None:
    del fields
    if phase in ("busy", "holding") and (
        self._alloc.ready_generation != self._alloc.generation
    ):
        # Pending pilot: is_live is already true, keep the queued badge.
        return
    self._emit_allocation(phase)
```

Risk 3 of the codebase notes: `is_live` is true while the pilot is PENDING,
so `reconcile` can send `holding` (or `busy`) before `ready`, and the store
would replace `provisioning` with it. Dropping those two until `ready`
keeps the queued badge. `draining` and `drain_failed` pass, since they only
happen after members ran.

Unknown phases from a future runtime: `_emit_allocation` passes them to the
model, which raises on the `Literal`; the `try/except` drops it with a debug
log. Accepted.

## 3. Handle changes (`_Allocation`, `slurm.py:136`)

Two fields:

- `members: set[str]`: task ids that asked for this generation. Filled in
  `_allocation` under the lock (`self._task.id` when there is a task),
  cleared when a new generation is submitted. `_allocation` is the one call
  every member makes through `_submit_step`, so this avoids touching
  `_submit_step` (Robustness).
- `ready_generation: int = 0`: last generation that emitted `ready`.

Known limit: `queued` lists only the members that called `_allocation`
before it, usually one. Later members join the set and show up on the next
event. `holding`/`draining`/`releasing` show on every card of the target via
`targetRef` anyway (`machineStore.ts:202-224`).

## 4. Ownership and coordination

Badges owns `emit_node_phase`, `events.py`, `_emit_allocation`, the two
handle fields, and these emission calls:

- `_allocation` submit branch: `queued`, `failed`, `members` (Load owns the
  liveness check above it; the calls go after it).
- `poll`: the `ready` check, three lines after `_queue_state` (Load owns
  `poll`). If Load caches `QueueState` on the handle, the check moves to
  the refresh point, still gated by `ready_generation`.
- `release`: `released` (Robustness owns `release`). One line after
  `handle.job_id = None`.

Load plan clears `job_id` in `poll` on `state == ""`. If it does, that site
must call `_emit_allocation("released", reason="gone")` too, or a later
`release` finds `job_id is None`, emits nothing, and the badge stays on
`holding` until the run ends (risk 1). Same for the liveness clear in
`_allocation` (`slurm.py:631`); there a following `queued` with a new
generation replaces the node, so it is optional.

Merge order is load, robustness, badges, so badges rebases onto both and
places the calls in their final code.

## 5. tc-os backend: store and stream only

No backend change. Evidence from `$TC`:

- Ingest has no allowlist. `backend/src/app/api/routes/workflow.py:2018`
  persists every type except `TRANSFER_EVENT_TYPE`, and `:2041-2043`
  publishes each to redis `workflow_run:{id}:events`.
  `backend/src/app/crud/workflow.py:2370-2379` adds the row and calls
  `ingest_event_actuals` in the same transaction.
- Billing ignores it. `backend/src/app/core/cost_ingest.py:517-525`
  `ingest_event_actuals` matches `event_type` exactly against
  `"task_resource_event"` (`:518`) and `"terraform_node_event"` (`:522`).
  Anything else falls through, no ledger write.
- Admin machines ignore it. `backend/src/app/core/machines.py:70-89`
  `_filters` requires `billing_mode == BillingMode.PROVISIONED` (`:78`), and
  `list_machines` (`:153`) uses only those filters. Rows become PROVISIONED
  in only two places:
  - `cost_ingest.py:408,422`, inside `_ingest_terraform_node`, reached only
    from `:522`.
  - `cost_estimate.py:739`, inside `_price_terraform`, called for
    `kind == "terraform"` members (`:493-495`) and for shared node rows,
    which are built only when `shared = ref is not None and
    target.get("kind") == "terraform"` (`:442`, `:540`).
  A Slurm member gets `BillingMode.EXTERNAL` (`cost_estimate.py:497-500`).
  So no Slurm row is ever a machine, with or without the event.
- The reaper (`machines.py:457`) works on EC2 tags; not reachable.

What would break it: emitting under `terraform_node_event`, or a future
`cost_ingest` branch on a prefix. Lock it with tests (section 7).

## 6. Files to touch

horus-slurm:

- `src/horus_slurm/events.py`: `AllocationPhase`, `SlurmAllocationEvent`.
- `src/horus_slurm/target/slurm.py`: `_Allocation.members`,
  `_Allocation.ready_generation`, `emit_node_phase`, `_emit_allocation`,
  the calls in `_allocation`, `poll`, `release`.
- `tests/unit/test_shared_allocation.py`: emission tests.

tc-os (Team 3 frontend and backend workers, not this doc):

- `frontend/src/hooks/workflow/machineStore.ts`,
  `frontend/src/hooks/workflow/WorkflowContext.tsx` (dispatch
  `slurm_allocation_event` to `applyNodeEvent`), plus the UX doc's card
  changes. Phase map: `queued -> provisioning`, `ready -> ready`,
  `released -> destroyed` (clears), `failed -> failed`, the rest as
  terraform.
- `backend/tests/crud/test_cost_ingest.py`,
  `backend/tests/api/routes/test_machines.py`: tests only.

No `pyproject.toml` change.

## 7. Tests

horus-slurm, `tests/unit/test_shared_allocation.py`, capturing with a
`bus.emit` monkeypatch like the `emitted` fixture in
`tests/unit/test_slurm_target.py:80-95`, filtered to
`SlurmAllocationEvent`:

- First `_submit_step` on a shared target emits one `queued` with
  `generation=1`, the sbatch job id, `node_key == lease_key`, `target_ref`,
  and the member in `member_task_ids`. A second member does not emit
  another `queued`.
- `sbatch` failing emits `failed` with the error in `reason`, and the error
  still reaches the caller.
- `poll` with squeue `RUNNING` on two members emits one `ready`. A new
  generation emits `ready` again.
- `emit_node_phase("holding")` before `ready` emits nothing; after `ready`
  emits one `holding` with the member set. `draining` passes either way.
- `release("done")` emits `released` with `reason="done"` and the cancelled
  job id; a second `release` emits nothing.
- A `bus.emit` that raises does not fail `emit_node_phase`, `_allocation`,
  `poll` or `release`.
- An inline (unnamed) target emits no `SlurmAllocationEvent`, including from
  `emit_node_phase`.

tc-os backend:

- `test_cost_ingest.py`: append `slurm_allocation_event` rows
  (`queued`, `ready`, `released`); no `TaskCostEntry` created or changed.
- `test_machines.py`: after the same events, `list_machines` is empty.

Run: `uv run ruff check`, `uv run mypy src`,
`uv run pytest -q -p no:cacheprovider` (horus-slurm); backend `pytest` on
the two files (tc-os).
