# Slurm phase badges: codebase notes

Research for Team 3 of `slurm-badges-plan.md`. No code changed.

Paths: horus-slurm is this repo. `TC` is
`/home/cdominguez/orca/workspaces/tc-os/slurm-same-target`. `RT` is
`$TC/horus-runtime/src/horus_runtime`. horus-terraform paths are on
`origin/main` of `/home/cdominguez/Developer/horus-terraform`.

## 1. horus-slurm events today

- `src/horus_slurm/events.py:44` `SlurmJobEvent(BaseEvent)`, `event_type =
  "slurm_job"` (`:58`). Fields: `task_id`, `job_id`, `state`, `reason`,
  `nodes`, `partition_nodes`. Synthetic states `SUBMITTED`, `CANCELLING`,
  `GONE` (`:25-41`). Module docstring says: low frequency, one event per
  transition, tc-os writes a row per event.
- Registration: `pyproject.toml:23-24`
  `[project.entry-points."horus.event"] slurm_job = "horus_slurm.events"`.
  `BaseEvent` is an `AutoRegistry` with `entry_point="event"` and
  `registry_key = "event_type"` (`$RT/event/base.py:52,66`). The entry point
  loads the module, so any new class in `events.py` registers with it. No
  new entry point is needed. Nothing on the tc-os path looks events up in
  the registry: the transport does `event.model_dump(mode="json")` and the
  backend stores the raw dict (see 3).
- Emission: `SlurmTarget._announce` (`src/horus_slurm/target/slurm.py:865`)
  calls `HorusContext.get_context().bus.emit(SlurmJobEvent(...))` at `:914`
  inside `try/except` with a debug log. That is the pattern to copy: no bus
  outside a Horus context, and a failing subscriber must not fail the job.
- `SlurmTarget` does **not** override `emit_node_phase`. The base is a no-op
  (`$RT/core/target/base.py:237-239`), so every lease phase the runtime
  reports for a shared Slurm target is dropped today.
- Shared allocation state: `_Allocation` (`slurm.py:136`) holds `lock`,
  `job_id`, `generation`, `alloc_dir`, `ready_callbacks`, `callbacks_done`.
  No member set (terraform has `members_seen`, see 2). `is_live` is
  `job_id is not None` (`:497`), so it is true while the pilot is still
  PENDING. `generation` (`:502`) counts submitted allocations.
- Where `queued`/`ready` would come from:
  - submit: `_allocation` (`:616`), branch at `:632-645` after `_sbatch`
    sets `handle.job_id`. Emit outside the lock or at least after
    `handle.generation` and `job_id` are set.
  - RUNNING: nothing tracks the pilot's own state today. `_allocation` only
    runs `_queue_state` for liveness (`:620-628`, owned by the Load team).
    `poll` (`:978`) reads the step's queue state via
    `_handle_job_id(handle)`, which for a shared step is the allocation id,
    so the first member poll that sees `RUNNING` is a natural place. Needs a
    once-per-generation flag on `_Allocation` (e.g. `announced_ready`) so N
    members do not each emit.
  - release: `release` (`:569`) cancels and clears `job_id`. The runtime
    does not emit anything for the release itself (see 2), so the plugin
    must emit an end phase here, like terraform's `destroyed`.
- Who the lease target is: `LeaseManager._open_lease` stores the first
  `task.target` it sees (`$RT/core/lease.py:253-267`). That copy is bound
  to one task (`_task`, `base.py:132`) and has `_ref` set (`base.py:135`).
  So `self._task` on `emit_node_phase` is the first member, not the one
  that changed anything.

## 2. horus-terraform's TerraformNodeEvent

- `src/horus_terraform/event.py:17-28` `NodePhase = Literal["provisioning",
  "provisioned", "ready", "holding", "busy", "draining", "drain_failed",
  "failed", "destroying", "destroyed"]`.
- `event.py:31` `TerraformNodeEvent`, `event_type = "terraform_node_event"`
  (`:54`). Fields: `phase`, `task_id`, `provider`, `instance_type`,
  `instance_id`, `region`, `node_key`, `target_ref`, `generation`,
  `member_task_ids`, `reason`, `outputs`, `phase_detail` (`:56-76`).
- Not registered under `horus.event` in its `pyproject.toml` (only
  `horus.target`, `horus.transfer`, `horus.middleware.workflow`). It still
  reaches tc-os, which confirms registration is not needed for transport.
- `terraform_target.py:442-462` `emit_node_phase(phase, *, reason=None,
  outputs=None, **fields)`: casts to `NodePhase`, calls `_emit_node_event`,
  logs and drops any exception. Never raises into the lease manager.
- `terraform_target.py:1126-1160` `_emit_node_event` builds the event from
  the shared handle: `node_key=handle.key`, `target_ref=self._ref`,
  `generation` (+1 while provisioning, `:1140`), `member_task_ids =
  sorted(handle.members_seen)`, `task_id=self._task.id`. Note it is not
  wrapped in try/except for the bus itself; only `emit_node_phase` is.
- Own phases: `provisioning` `:792`, `provisioned` `:822`, `failed` `:845`,
  `ready` `:856`, `destroyed(reason=failed)` `:886`, `destroying` `:1103`,
  `failed(phase_detail=destroy)` `:1108`, `destroyed` `:1121`.
- Phases the runtime emits through `emit_node_phase` (`$RT/core/lease.py`):
  - `busy` `:315`, only when a member starts while the lease is `HOLDING`.
  - `holding` `:375`, in `reconcile` when members are pending, none running.
  - `draining` (`outputs=len(needed)`) `:577`, before evacuation.
  - `drain_failed` `:583`, machine kept.
  - `busy` or `holding` `:620`, after a drain or a failed release.
  - Never `releasing`, `cold`, `queued` or `ready`. Lease `Phase` enum is
    `cold/busy/holding/draining/releasing` (`:68-79`). Start and end of a
    machine's life are the plugin's job.
  - `release(reason)` reasons: `done | drained | idle_cap | run_end | abort`
    (`base.py:221-226`).

## 3. tc-os: runtime event to canvas

- Transport: `$TC/tc-plugins/src/tc_plugins/event_transport_to_api.py:71`
  `EventTransportToAPI.publish` drops `NON_PERSISTED_EVENT_TYPES` (`:75`;
  only resource samples, `resource_events.py:31-34`), else posts
  `event.model_dump(mode="json")` (`:104`) in batches.
- Backend ingest: `$TC/backend/src/app/api/routes/workflow.py:1997`
  `_append_run_events` validates each into
  `CreateWorkflowRunEventFromHorusEvent` (`tc-models/.../workflow/run.py:331`:
  `event_id`, `event_type`, `timestamp`, `raw_event`). All types except
  transfer are persisted (`:2018`) via
  `crud/workflow.py:2339 append_workflow_run_events`, which also calls
  `ingest_event_actuals` per event (`:2379`). Each is published to redis
  `workflow_run:{id}:events` (`:2041`). No allowlist: a new
  `slurm_allocation_event` is stored and streamed with no backend change.
- Reading back: `read_workflow_run_events` `crud/workflow.py:2260` with an
  optional `event_types` filter (route `workflow.py:1367`).
- Frontend live stream: `frontend/src/hooks/workflow/WorkflowContext.tsx`
  - `:2151` `connectWorkflowUpdates("/workflow/runs/{id}/ws", ...)`.
  - `:2192-2199` `message.event_type === NODE_EVENT_TYPE` ->
    `applyNodeEvent(runId, event_data)`. No `return`, so the event also
    joins the events list.
  - `:2203-2207` run terminal status -> `clearRunMachines(runId)`.
- `frontend/src/hooks/workflow/machineStore.ts`
  - `:36` `NODE_EVENT_TYPE = "terraform_node_event"`.
  - `:39-56` `NodeEventData` (`phase`, `task_id`, `node_key`, `target_ref`,
    `generation`, `member_task_ids`).
  - `:59-65` stored `NodePhase`: `provisioning | ready | holding | draining
    | failed | destroying`. `:68-73` `MachinePhase` drops `ready`.
  - `:84-88` `STORED`: `provisioned->provisioning`, `busy->ready`,
    `drain_failed->holding`.
  - `:103-144` `applyNodeEvent`: key = `node_key || task_id`; older
    generation ignored; members accumulate per generation; `destroyed`
    removes the node and its members' index entries.
  - `:180-192` `machinePhase` precedence: failed > provisioning >
    destroying > draining > (winding down: ready/holding -> destroying) >
    holding.
  - `:202-224` `nodePhases`: tasks bound via `nodeOf`, plus every node with
    the same `targetRef` unless it is `provisioning` or `failed`. So a boot
    shows only on bound members; holding/draining/destroying on all members
    of the named target.
  - `:227-240` `useMachinePhase(runId, nodeId, folded, windingDown,
    targetRef)`.
- `frontend/src/components/Canvas/ReactFlow/TaskCard.tsx`
  - `:125-130` `PHASE_TEXT`: `provisioning`, `tearing down`, `saving
    outputs`, `provisioning failed`. `holding` only in the tooltip.
  - `:655-661` `useMachinePhase(currentRunId, data.id, false, windingDown,
    machineOf(data))`; `machineOf` = `target.ref` (`hooks/workflow/
    machines.ts:27`).
  - `:665-668` provisioning/failed hidden on a finished task.
  - `:672-688` tooltip text, terraform-worded ("Provisioning its target",
    "Its target is idle, still billed").
  - `:940-950` badge render.
  - `:969-978` `SlurmStatusDot` (`:165`) only when `inlineTarget?.kind ===
    "slurm"`; `inlineTarget` is undefined for a `{ref}` target (`:851`). So
    members of a named Slurm target get the plain dot, never "Queued in
    Slurm".
- Existing `slurm_job` UI: `runPanels/SlurmIndicator.tsx` (rail peek,
  mounted from `Artifacts/registry.ts:180`), `runPanels/useSlurmJob.ts:56`
  reads run events, `lib/slurmJob.ts:150-175 overlaySlurmEvent` picks the
  newest `slurm_job` event by `task_id`. It keys on the `slurm-job.json`
  side product, which is per task, not per allocation.
- `DeletableEdge.tsx:78` also reads `useMachinePhase(...) ===
  "provisioning"` (edge animation while a node boots).

### Phase mapping proposal (for design)

| slurm_allocation_event phase | stored `NodePhase` | card |
|---|---|---|
| `queued` (sbatch done, PENDING) | `provisioning` | "provisioning", bound tasks only |
| `ready` (first RUNNING) | `ready` | nothing |
| `busy` / `holding` / `draining` / `drain_failed` (runtime) | as terraform | as terraform |
| `released` (scancel / gone) | treat as `destroyed` | cleared |

Copy in `PHASE_TEXT` and the tooltip says "provisioning"; "queued" would be
more accurate for Slurm. That needs either a per-node kind in the store or
a separate phase value, which widens `NodePhase`/`MachinePhase`. Design
call.

## 4. Backend consumers of terraform_node_event

- `backend/src/app/core/cost_ingest.py:504-525` `ingest_event_actuals`
  dispatches on exact `event_type`: `task_resource_event` and
  `terraform_node_event` only. Any other type is a no-op.
  `_ingest_terraform_node` (`:366`) sets `BillingMode.PROVISIONED`
  (`:407-422`) on `provisioned` and closes on `destroyed`.
- `backend/src/app/core/machines.py`: admin list reads `TaskCostEntry` rows
  with `billing_mode == PROVISIONED` (`_filters` `:70-89`, `list_machines`
  `:153`). Rows only become PROVISIONED through `_ingest_terraform_node`.
  Reaper (`:457`) works on EC2 tags.
- So a distinct `event_type = "slurm_allocation_event"` is ignored by
  billing and admin with no tc-os change. The only way to break it is to
  reuse `terraform_node_event`, or to emit a `TerraformNodeEvent`-shaped
  payload under that type. Do not.
- To lock it in: a backend test that appends a `slurm_allocation_event`
  (with `phase: "ready"` and a `released` end) and asserts no
  `TaskCostEntry` change and an empty `list_machines`. Existing tests to
  extend: `backend/tests/crud/test_cost_ingest.py`,
  `backend/tests/api/routes/test_machines.py`.

## 5. Risks, ranked

1. **Badge never clears.** The runtime emits no end phase. If
   `SlurmTarget.release` does not emit `released`/`destroyed`, the store
   keeps `holding` and every member reads "idle" until the run ends (and,
   while winding down, "tearing down" with nothing tearing down). Emit the
   end phase in `release` after `job_id` is cleared, and when `_allocation`
   finds the pilot gone (`:627`).
2. **Duplicate or stale `ready`.** Many members poll the same allocation.
   Gate `ready` with a per-generation flag on `_Allocation`. Interacts with
   the Load team's cached `QueueState` on the handle; coordinate who owns
   the flag.
3. **`holding` while still queued.** `is_live` is true for a PENDING pilot,
   so `reconcile` can emit `holding` before `ready`. In the store a later
   `holding` replaces `provisioning`, which hides the queued state.
   Precedence in `machinePhase` does not help (one phase per node). Either
   suppress lease phases in `emit_node_phase` until `ready` was seen, or
   accept it.
4. **Member list is empty.** `_Allocation` keeps no members; `task_id` on
   lease events is the first member. Terraform's `members_seen` is per
   generation and trimmed on release (`terraform_target.py:1124`). Add
   the same set, filled in `_submit_step` (Robustness owns `_submit_step`:
   coordinate, or fill it in `_allocation` callers). Without it the queued
   badge only shows on one card; holding still shows on all via
   `targetRef`.
5. **Frontend shape mismatch.** `applyNodeEvent` keys on `node_key ||
   task_id`. The plan names the field "lease key"; send it as `node_key`
   (or map it in the frontend). Same for `member_task_ids`, `generation`,
   `target_ref`. Unknown phases fall through `STORED` and get stored raw,
   so `queued`/`ready`/`released` must be mapped explicitly.
6. **Bus failures.** Emit in `try/except` like `_announce`; never raise
   into the lease manager (terraform catches in `emit_node_phase`).
7. **Wording.** "provisioning", "still billed", "provisioning failed" are
   cloud words. A Slurm allocation is queued and uses allocation hours.
8. **Named-target Slurm queue dot missing** (`TaskCard.tsx:969`). Out of
   scope for badges but users will notice next to the new badge.
9. **Event volume.** Fine: a few events per generation, same as
   `slurm_job`.

## Tests to add

horus-slurm (`tests/unit/`, capture pattern: `emitted` fixture in
`tests/unit/test_slurm_target.py:80-95`, monkeypatches `bus.emit`):
- `emit_node_phase("holding")` on a shared target emits one
  `SlurmAllocationEvent` with `phase`, `target_ref`, `node_key`,
  `generation`, `job_id`, `member_task_ids`.
- `_allocation` emits `queued` once per submit; generation matches.
- First RUNNING poll emits `ready` once across several members polling.
- `release` emits the end phase; a second `release` emits nothing.
- A bus that raises does not fail `emit_node_phase`, `_allocation` or
  `poll`.
- Inline (non-shared) target emits no allocation events.

tc-os frontend (`frontend/src/hooks/workflow/machineStore.test.ts`, bun):
- `slurm_allocation_event` `queued` shows `provisioning` on the bound task
  only; `ready` shows nothing; `holding` shows on all members via
  `targetRef`; the end phase clears all members.
- A new generation replaces the old; an old-generation event is ignored.
- `WorkflowContext` dispatch: the new type reaches `applyNodeEvent` (if
  dispatch is tested; else cover the type check in a small pure helper).

tc-os backend:
- `test_cost_ingest.py`: a `slurm_allocation_event` creates or changes no
  ledger row.
- `test_machines.py`: no machine row after such events.
