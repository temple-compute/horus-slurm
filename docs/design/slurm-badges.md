# Slurm phase badges: tech lead plan

Team 3 of `slurm-badges-plan.md`. Built from `slurm-badges-codebase.md`,
`-ux.md`, `-backend.md`, `-frontend.md`, checked against
`src/horus_slurm/target/slurm.py` at `550027e` and the runtime's
`core/lease.py`. No code changed.

## 1. Work packages

| id | repo / base | scope | files | depends on |
|---|---|---|---|---|
| WP1 | horus-slurm / `feat/shared-allocation` | `SlurmAllocationEvent`, `AllocationPhase` | `src/horus_slurm/events.py`, `tests/unit/test_shared_allocation.py` | - |
| WP2 | horus-slurm / same branch as WP1 | `_Allocation.members`, `_Allocation.ready_generation`, `_emit_allocation`, `emit_node_phase` (with the holding-before-ready rule), `queued` and `failed` in the `_allocation` submit branch, `members` fill in `_allocation` | `src/horus_slurm/target/slurm.py`, `tests/unit/test_shared_allocation.py` | WP1 |
| WP3 | horus-slurm / rebased onto `chdominguez/slurm-load` and `chdominguez/slurm-robust` | `ready` on first RUNNING in `poll`; `released` (reason `gone`) where Load's `poll` clears `job_id` on `state == ""`; `released` in `release` after `handle.job_id = None` | `src/horus_slurm/target/slurm.py`, `tests/unit/test_shared_allocation.py` | WP2, Load branch, Robustness branch |
| WP4 | tc-os / `chdominguez/slurm-same-target` | store accepts `slurm_allocation_event`; socket dispatch; Slurm copy on the card; `SlurmStatusDot` for shared members; `StatusDot` a11y | `frontend/src/hooks/workflow/machineStore.ts`, `frontend/src/hooks/workflow/WorkflowContext.tsx`, `frontend/src/components/Canvas/ReactFlow/TaskCard.tsx`, `frontend/src/hooks/workflow/machineStore.test.ts` | WP1 contract only (no code dependency) |

WP1 and WP2 can be one branch and one PR (`chdominguez/slurm-badges`). WP3
stays a separate commit on top so it can be rebased onto the Load and
Robustness branches without touching WP2. WP4 is its own PR in tc-os.

No tc-os backend change (see 4). Backend tests are open question Q1.

### WP2 details

- `_Allocation` gains `members: set[str] = field(default_factory=set)` and
  `ready_generation: int = 0`.
- `_emit_allocation(phase, reason=None)`: returns at once when
  `not self.releasable`; builds the event from the handle; wraps
  `HorusContext.get_context().bus.emit(...)` in `try/except Exception` with
  a debug log, as `_announce` does (`slurm.py:913-927`). Never raises.
  `bus.emit` is sync (`event/bus.py:96`), so calling it under the lock is
  fine.
- `_allocation`, submit branch only (`slurm.py:632-645`, after the Load-owned
  liveness check):
  - on a new generation: `handle.members = set()` before adding;
  - add `self._task.id` to `handle.members` whenever there is a task, inside
    the lock, on every call (not only on submit);
  - after `handle.job_id` and `handle.alloc_dir` are set: `queued`;
  - if `_make_private_dir`, `put_file` or `_sbatch` raises: `failed` with
    `reason=str(exc)`, then re-raise.
- `emit_node_phase(phase, **fields)`: drops `fields`; applies the rule in
  2.4; else `_emit_allocation(phase)`.

### WP3 details

- `poll`, right after `_queue_state`: when `queue.state == "RUNNING"`,
  the polled job id equals `self._alloc.job_id`, and
  `self._alloc.ready_generation != self._alloc.generation`: set
  `ready_generation = generation`, then emit `ready`. If Load moves the
  `squeue` call to a cached refresh on the handle, the check goes at the
  refresh point, same gate.
- Load's `job_id` clear in `poll` on `state == ""`: emit `released` with
  `reason="gone"` and the id that was cleared. Without it a later `release`
  sees `job_id is None`, emits nothing, and the badge stays until run end.
- `release`: one call after `handle.job_id = None`, `reason=reason`,
  `job_id` the cancelled id. Inside the existing `if handle.job_id is not
  None`, so a second `release` emits nothing.

## 2. Interface contract

`event_type = "slurm_allocation_event"`. The frontend reads `event_data`
as `NodeEventData` (`machineStore.ts:39-56`), the same shape as
`terraform_node_event`.

### 2.1 Fields

| Field | Type | Value | Read by frontend |
|---|---|---|---|
| `phase` | `AllocationPhase` | see 2.2 | yes |
| `task_id` | `str` | `self._task.id`, `""` without a task. On lease phases this is the first member (the lease's copy), not the one that changed | yes, fallback key and added to members |
| `node_key` | `str` | `self.lease_key`. Named `node_key`, not `lease_key` | yes, the store key |
| `target_ref` | `str \| None` | `self._ref` (the `targets:` name; never `None` on a shared target) | yes, matched against `machineOf(task)` |
| `generation` | `int` | `self._alloc.generation` (1 on the first submit) | yes, older ignored, newer replaces |
| `member_task_ids` | `list[str]` | `sorted(self._alloc.members)` | yes, bound members |
| `job_id` | `str \| None` | allocation job id; the cancelled id on `released`; `None` on `failed` | no |
| `reason` | `str \| None` | `released`: `done`, `drained`, `idle_cap`, `run_end`, `abort`, or `gone`. `failed`: error text. Else `None` | no |

Not sent: `outputs`, `phase_detail`, provider or instance fields. `queued`
carries no Slurm pending reason; the card takes it from the member's
`slurm_job` record.

### 2.2 Phases

| phase | sent by | when | frontend stores | card (Slurm copy) |
|---|---|---|---|---|
| `queued` | plugin, `_allocation` | sbatch returned, once per generation | `provisioning` | `queued`, bound members only |
| `failed` | plugin, `_allocation` | submit raised | `failed` | `allocation failed`, bound members, hidden once finished |
| `ready` | plugin, `poll` (WP3) | first RUNNING of the generation | `ready` | nothing |
| `busy` | runtime (`lease.py:315,620`) | member starts while holding; back from drain | `ready` | nothing |
| `holding` | runtime (`lease.py:375,620`) | members pending, none running | `holding` | tooltip only, all cards of the target |
| `draining` | runtime (`lease.py:577`) | before evacuation | `draining` | `saving outputs`, all cards |
| `drain_failed` | runtime (`lease.py:583`) | drain failed, kept | `holding` | as holding |
| `released` | plugin, `release` / `poll` (WP3) | allocation cancelled or gone | ends the node (like `destroyed`) | cleared |

**`releasing` is never sent.** The runtime's `Phase.RELEASING` is internal
(`lease.py:595`) and never passed to `emit_node_phase`; the plugin has no
reason to send it either (scancel is one call). The `releasing` text on
the card comes only from the existing winding-down fallback in
`machinePhase` (stored `ready`/`holding` read as `destroying` while the run
winds down), shown with the Slurm copy `releasing`. WP4 does not add a
`releasing` row to `STORED`.

`AllocationPhase` is exactly the eight values above. An unknown phase from a
future runtime fails model validation inside `_emit_allocation`'s
`try/except` and is dropped with a debug log.

### 2.3 Ordering per generation

`queued` (or `failed`) -> `ready` -> any of `busy` / `holding` / `draining`
/ `drain_failed` -> `released`. A `failed` leaves `job_id` `None`; the next
member submits generation + 1 and its `queued` replaces the failed node.
A `queued` at generation N+1 replaces whatever generation N left.

### 2.4 Holding-before-ready rule

`is_live` is `job_id is not None` (`slurm.py:497`), so it is true while the
pilot is still PENDING, and `reconcile` can send `holding` (and later
`busy`) before the pilot runs. The store has one phase per node, so that
`holding` would replace `provisioning` and hide `queued`.

Rule, in `emit_node_phase`: drop `busy` and `holding` while
`self._alloc.ready_generation != self._alloc.generation`. `draining` and
`drain_failed` always pass (they follow members that ran). Plugin-side
only; the store stays kind-blind.

Until WP3 lands nothing sets `ready_generation`, so WP2 alone drops every
`busy`/`holding`. That is the intended interim state; WP2 tests set
`ready_generation` by hand.

## 3. Frontend (WP4)

As `slurm-badges-frontend.md`, with these changes:

- `STORED` gains `queued: "provisioning"` only. No `releasing` row (2.2).
- `NodeEventData["phase"]` gains `"queued" | "released"`.
- `released` ends a node like `destroyed` (one `ended` check in
  `applyNodeEvent`).
- `MACHINE_EVENT_TYPES` set; `WorkflowContext.tsx` dispatch uses it, keeps
  the fall-through to `appendRunEvent`.
- `TaskCard.tsx`: target kind from `inlineTarget?.kind ??
  getMachines(workflow)[machine]?.kind`; `SLURM_PHASE_TEXT` and
  `PHASE_LABEL` / `SLURM_PHASE_LABEL` lookups; `SlurmStatusDot` for any
  Slurm target, taking `label` and using `slurmReasonLabel`; `StatusDot`
  gets `role="img"`, `aria-label`, `tabIndex={0}`.
- No `lease_key` fallback in `applyNodeEvent`: the plugin sends `node_key`.

## 4. tc-os backend

No change. Ingest has no allowlist (`routes/workflow.py:2018,2041`);
`cost_ingest.ingest_event_actuals` matches `event_type` exactly and only
handles `task_resource_event` and `terraform_node_event` (`:517-525`);
admin machines need `BillingMode.PROVISIONED`, which only terraform paths
set. The one way to break it is to emit under `terraform_node_event`.

## 5. Test plan

Capture with a `bus.emit` monkeypatch like the `emitted` fixture in
`tests/unit/test_slurm_target.py:80-95`, filtered to `SlurmAllocationEvent`.

**WP1**
- Model round trip: `model_dump(mode="json")` has `event_type ==
  "slurm_allocation_event"` and every field in 2.1.
- An unknown phase raises `ValidationError`.

**WP2**
- First `_submit_step` on a shared target emits one `queued`:
  `generation == 1`, sbatch job id, `node_key == lease_key`, `target_ref`,
  member in `member_task_ids`. A second member emits no second `queued`.
- New generation (job id cleared, submit again): `queued` with
  `generation == 2` and members reset.
- `_sbatch` raising emits `failed` with the error in `reason` and
  `job_id is None`; the caller still gets the error.
- `emit_node_phase("holding")` / `("busy")` with `ready_generation` behind:
  nothing. With `ready_generation == generation`: one event with the member
  set. `draining` and `drain_failed` pass either way.
- `bus.emit` raising does not fail `emit_node_phase` or `_allocation`.
- Inline target: no `SlurmAllocationEvent`, including from
  `emit_node_phase`.

**WP3** (written against the rebased Load and Robustness code)
- Two members polling `RUNNING` emit one `ready`; a new generation emits
  `ready` again.
- `PENDING` poll emits no `ready`.
- `poll` seeing `state == ""` on a shared step emits one `released` with
  `reason == "gone"`; a later `release` emits nothing.
- `release("done")` emits `released` with `reason == "done"` and the
  cancelled job id; a second `release` emits nothing.
- `bus.emit` raising does not fail `poll` or `release`.
- 50-copy `squeue` count test from the Load team still passes.

**WP4** (`machineStore.test.ts`, bun)
- `MACHINE_EVENT_TYPES` holds both types.
- `queued` with `member_task_ids: ["a"]`: `a` is `["provisioning"]`, `b`
  (same `targetRef`) is `[]`.
- `ready` then `busy`: `machinePhase` is `null`.
- `holding`, `draining` show on `a` and `b` via `targetRef`, not on a task
  of another ref; `drain_failed` stores `holding`.
- `released` clears the node and both members' `nodeOf` entries.
- `busy` gen 1, then `queued` gen 2: `provisioning`, members reset; a late
  gen-1 `holding` is ignored.
- `failed` shows on bound members only.
- Card copy, dot `aria-label` and Tab focus: canvas E2E with a recorded run
  (no bun DOM harness exists).

Checks: `uv run ruff check`, `uv run mypy src`, `uv run pytest -q -p
no:cacheprovider` (horus-slurm); `biome`, `tsc`, `bun run test:unit`
(tc-os frontend).

## 6. Deviations from the plan

| Plan says | Here | Reason |
|---|---|---|
| Field "lease key" | `node_key` | The store keys on `node_key` (`machineStore.ts:103-144`); same name as terraform, no frontend mapping |
| `ready` emitted from `_allocation` when squeue first says RUNNING | emitted from `poll` (WP3) | `_allocation` only runs `squeue` for liveness on later calls, never tracks the pilot reaching RUNNING; `poll` already reads the allocation's state. Gated once per generation by `ready_generation` |
| Phases: `queued`, `ready`, runtime phases | adds `failed` and `released` | `failed`: a submit error must show. `released`: the runtime emits no end phase, so without it the badge never clears (codebase risk 1) |
| Badges owns `emit_node_phase` and `events.py` | also adds lines in `_allocation` (submit branch), `poll`, `release` and two `_Allocation` fields | Needed for `queued`/`failed`/`ready`/`released`. Lines in Load- and Robustness-owned functions are isolated in WP3 |
| `emit_node_phase` passes runtime phases through | drops `busy`/`holding` until `ready` | `is_live` is true for a PENDING pilot; a `holding` would hide `queued` (2.4) |
| Map phases onto existing `NodePhase` | done; no `releasing` mapping | `releasing` is never sent (2.2); the frontend doc's `STORED` row is dead code |
| tc-os backend: check the event is ignored | checked, no change, no tests by default | Q1 |
| Member set in `_submit_step` (codebase note) | filled in `_allocation` | `_submit_step` is Robustness-owned; every member goes through `_allocation` |

Conflicts resolved between the designs:

- Frontend doc lists `releasing` as a sent phase and asks whether it is;
  backend never sends it. Backend wins; no `STORED` row.
- Frontend doc offers a `lease_key` fallback; backend sends `node_key`. No
  fallback.
- Backend doc makes the `released` in Load's `poll` clear required and the
  one in `_allocation`'s liveness clear optional. Kept: the liveness clear
  is always followed by a new `queued`, which replaces the node.

## 7. Open questions

**Q1. Add tc-os backend tests that lock "no billing, no machine row" for
`slurm_allocation_event`?** Recommended: no. Nothing in the backend changes,
the dispatch is an exact string match, and the coordinator scoped tc-os
work to `frontend/src`. Revisit if `cost_ingest` ever matches by prefix.

**Q2. Should `queued` show on every card of the target, not just bound
members?** Today only members that called `_allocation` before the submit
are bound (usually one), so other queued members show no slot text; their
dot still says `Queued in Slurm: <reason>` from the `slurm_job` record.
Recommended: accept for now. Widening needs a store rule change that also
affects terraform boots.

**Q3. Named-target Slurm queue dot (`TaskCard.tsx:969`) in this WP?**
Recommended: yes, it is in WP4 already (UX doc): shared members otherwise
read as `running` next to a `queued` badge.

## User decisions

- Approved 2026-10-17 with the recommended answers: Q1 no tc-os backend tests; Q2 queued only on bound members, accepted; Q3 SlurmStatusDot for shared members in WP4, yes.
