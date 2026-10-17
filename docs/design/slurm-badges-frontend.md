# Slurm allocation badges: frontend

Team 3, frontend. How tc-os shows `slurm_allocation_event` on member cards.
Inputs: `slurm-badges-plan.md`, `slurm-badges-codebase.md`,
`slurm-badges-ux.md`. No code changed.

Paths below are under `frontend/src/` of tc-os
(`chdominguez/slurm-same-target`).

## 1. Wire contract (from horus-slurm)

`event_type = "slurm_allocation_event"`, `event_data`:

| Field | Used by the store | Note |
|---|---|---|
| `phase` | yes | `queued`, `ready`, `busy`, `holding`, `draining`, `drain_failed`, `releasing`, `released`, `failed` |
| `node_key` | yes, the key | the lease key (`target.lease_key`). Send it as `node_key`, not `lease_key`, so the store needs no mapping |
| `target_ref` | yes | the `targets:` name |
| `generation` | yes | `_Allocation.generation` |
| `member_task_ids` | yes | without it `queued` shows on one card only |
| `task_id` | yes | first member on lease events |
| `job_id` | no | tray already shows it from `slurm_job` |

Same field names as `terraform_node_event`, so one `NodeEventData` covers
both. If the plugin ships `lease_key` instead, map it in one place
(`applyNodeEvent`: `data.node_key || data.lease_key || data.task_id`), but
prefer fixing the plugin.

Lease keys are unique per target in a run, and a target is either terraform
or Slurm, so both kinds can share one `machines[runId]` map without
collisions.

## 2. Delivery

Only the live socket. No change to the backend (`_append_run_events` has no
allowlist; the event is stored and published on
`workflow_run:{id}:events`).

`hooks/workflow/WorkflowContext.tsx`, `connectWorkflowUpdates` callback
(`:2192-2199`): the check `message.event_type === NODE_EVENT_TYPE` becomes
`MACHINE_EVENT_TYPES.has(message.event_type)`. Keep the fall-through (no
`return`): the event is persisted, so it still goes to
`appendRunEvent` (`:2285`) and the run events list, like terraform's.

- `appendRunEvent`: no change. It appends any non-patch event to the
  `["workflowRunEvents", ...]` cache, dedupes by `event_id`, caps at
  `LOG_FETCH_LIMIT`. The event then shows in the run log like any other.
- `followRun` / `useFollowRun`: not involved. They frame canvas nodes from
  task status and transfers; nothing to change.
- Reload and reconnect: the socket does not replay, and the store is live
  only. A page loaded mid-queue shows no slot text until the next event;
  the dot still says `Queued in Slurm: <reason>` from the `slurm_job`
  record. Accepted in the UX doc. Backfilling the store from
  `read_workflow_run_events` is out of scope (terraform has the same gap).
- Run end: `clearRunMachines(runId)` on a terminal status already clears
  Slurm nodes too.

## 3. `hooks/workflow/machineStore.ts`

```ts
export const NODE_EVENT_TYPE = "terraform_node_event"
export const SLURM_ALLOCATION_EVENT_TYPE = "slurm_allocation_event"
/** Event types `applyNodeEvent` takes. */
export const MACHINE_EVENT_TYPES = new Set([
  NODE_EVENT_TYPE,
  SLURM_ALLOCATION_EVENT_TYPE,
])
```

`NodeEventData["phase"]` gains `"queued" | "releasing" | "released"`. Doc
comment: the data of a `terraform_node_event` or `slurm_allocation_event`.

`STORED` gains three rows; `NodePhase` and `MachinePhase` do not change:

```ts
const STORED = {
  provisioned: "provisioning",
  busy: "ready",
  drain_failed: "holding",
  // A Slurm allocation waiting in the queue is a node still booting.
  queued: "provisioning",
  releasing: "destroying",
}
```

`released` ends a node like `destroyed`. In `applyNodeEvent`, replace the two
`data.phase === "destroyed"` checks with one
`const ended = data.phase === "destroyed" || data.phase === "released"`.

Everything else is reused as is:

- Key: `node_key || task_id`, per run.
- Generation: an older one is ignored; a newer one replaces the node and
  resets members (`busy` gen N, then `queued` gen N+1 shows `queued`).
- Members accumulate per generation from `member_task_ids` + `task_id`.
- `nodePhases`: `queued` (stored `provisioning`) and `failed` show on bound
  members only; `holding`, `draining`, `destroying` on every card whose
  `machineOf` is the `target_ref`.
- `machinePhase` precedence unchanged.

Add a line to the module docstring: a shared Slurm allocation sends
`slurm_allocation_event` with the same fields; `queued` is stored as
`provisioning`, `released` ends it like `destroyed`.

Known gap, plugin side: a `holding` sent while the pilot is still PENDING
replaces `provisioning`, hiding `queued` (codebase notes, risk 3). The store
cannot tell; the plugin should hold lease phases until `ready`.

## 4. `TaskCard.tsx`: kind-specific copy

The store stays kind-blind. The card picks the copy by the target's kind:

```ts
// `machine` and `inlineTarget` are computed near :851; move them up.
const machineKind = useWorkflowStore((s) =>
  machine ? getMachines(s.workflow)[machine]?.kind : undefined,
)
const targetKind = inlineTarget?.kind ?? machineKind
const slurm = targetKind === "slurm"
```

Copy tables next to `PHASE_TEXT` (`:125`):

```ts
const SLURM_PHASE_TEXT: Partial<Record<MachinePhase, string>> = {
  provisioning: "queued",
  destroying: "releasing",
  draining: "saving outputs",
  failed: "allocation failed",
}
```

Today's tooltip chain (`:673-688`) becomes a lookup, one table per kind:

```ts
const PHASE_LABEL: Record<MachinePhase, string> = {
  provisioning: "Provisioning its target",
  destroying: "Tearing down its target",
  holding: "Its target is idle, still billed",
  draining: "Saving outputs from its target",
  failed: "Its target failed to provision",
}
const SLURM_PHASE_LABEL: Record<MachinePhase, string> = {
  provisioning: "Queued in Slurm",
  destroying: "Releasing its allocation",
  holding: "Its allocation is idle, still held",
  draining: "Saving outputs from its allocation",
  failed: "Its Slurm allocation failed",
}
```

- `statusLabel`'s first line: `machinePhase && (slurm ? SLURM_PHASE_LABEL :
  PHASE_LABEL)[machinePhase]`. Rest unchanged (OOM, error, skip).
- Slot (`:940-950`): `const phaseText = machinePhase && (slurm ?
  SLURM_PHASE_TEXT : PHASE_TEXT)[machinePhase]`; render as today. Red only
  for `failed`.
- `useMachinePhase` call, finished-task hiding of `provisioning`/`failed`
  (`:655-668`), and the winding-down fallback (`ready`/`holding` read as
  `destroying`, so `releasing`) are unchanged.
- `ready` / `busy` show nothing (`machinePhase` returns `null`).

`DeletableEdge.tsx:78` reads `"provisioning"` for the waiting animation. No
change (UX: an edge rarely sees `queued`).

## 5. Status dot

### `SlurmStatusDot` for shared members (`:969`)

Condition `data.status === "running" && inlineTarget?.kind === "slurm"`
becomes `data.status === "running" && slurm`. The `recordUrl` lookup stays:
a member's `slurm_job` side product is per task and carries the
allocation's job id and state.

`SlurmStatusDot` takes `label?: string` (pass `statusLabel`) and keeps it:

```ts
const reason = record?.state === "PENDING"
  ? `Queued in Slurm${record.reason ? `: ${slurmReasonLabel(record.reason)}` : ""}`
  : null
// The phase line says "Queued in Slurm" too; the record's line replaces it.
const lines = (label ?? "").split("\n").filter((l) => l && l !== "Queued in Slurm")
const full = [reason, ...lines].filter(Boolean).join("\n") || undefined
return <StatusDot status={reason ? "pending" : "running"} label={full} />
```

`slurmReasonLabel` (`lib/slurmJob.ts:227`) replaces the raw reason, as the
UX doc asks. This also changes inline Slurm tooltips (raw `Priority` becomes
`Priority: waiting for higher-priority jobs`), which is wanted.

A member that is not running yet (`pending`) keeps the plain `StatusDot`
with `statusLabel`, whose first line is `Queued in Slurm`.

### `StatusDot` a11y (`:132`)

```tsx
<span
  role="img"
  aria-label={label ?? status}
  tabIndex={0}
  className={`h-2 w-2 shrink-0 rounded-full ${dot}`}
/>
```

Same string as the tooltip, so screen readers get the reason, holding and
error lines. `tabIndex={0}` lets keyboard users open the tooltip (Radix
opens on focus). Applies to every dot, not only Slurm. The dot sits inside
the card; check it does not steal React Flow keyboard handling (node
delete on Backspace acts on the selected node, not the focused element, so
it should be fine). Add `focus-visible:ring-1 ring-ring` only if the
existing design system has no global focus ring.

## 6. Files to touch

| File | Change |
|---|---|
| `hooks/workflow/machineStore.ts` | event type consts, phase union, `STORED` rows, `released` as end, docstring |
| `hooks/workflow/WorkflowContext.tsx` | `MACHINE_EVENT_TYPES.has(...)` in the socket dispatch, import |
| `components/Canvas/ReactFlow/TaskCard.tsx` | target kind, Slurm copy tables, label lookup, `SlurmStatusDot` condition + `label` + `slurmReasonLabel`, `StatusDot` a11y |
| `hooks/workflow/machineStore.test.ts` | new tests (section 8) |

No new component, no new file, no backend change.

## 7. Reused

`useMachineStore` / `applyNodeEvent`, `nodePhases`, `machinePhase`,
`useMachinePhase`, `runWindingDown`, `clearRun`, `machineOf`, `getMachines`,
`isTargetRef`, `StatusDot`, `SlurmStatusDot`, `useSlurmJob`,
`slurmReasonLabel`, `appendRunEvent`, `connectWorkflowUpdates`. The tray
(`SlurmIndicator`) is untouched.

## 8. Tests

`hooks/workflow/machineStore.test.ts` (bun, pure store; same `machines()`
helper and `beforeEach` reset). A `slurm` fixture like `shared`:
`{ node_key: "slurm-lease", target_ref: "cluster", generation: 1 }`.

- `MACHINE_EVENT_TYPES` has both `terraform_node_event` and
  `slurm_allocation_event`.
- `queued` with `member_task_ids: ["a"]`: `nodePhases` of `a` is
  `["provisioning"]`; of `b` (same `targetRef`) is `[]`.
- `ready` then `busy`: stored `ready`, `machinePhase` is `null`.
- `holding`, `draining` show on `a` and `b` via `targetRef`; not on a task
  of another ref.
- `drain_failed` stores `holding`; `releasing` stores `destroying`.
- `released` clears the node and both members' `nodeOf` entries.
- `busy` at generation 1, then `queued` at generation 2: the node is
  `provisioning`, members reset to the new list; a late generation-1
  `holding` is ignored.
- `failed` shows on bound members only.

`hooks/workflow/appendRunEvent.test.ts`: no change needed; it is
type-agnostic.

Component tests: the repo has no bun DOM tests (no `*.test.tsx`), only
Playwright specs in `tests/`. The copy and dot changes are table lookups and
a filter; cover them in the canvas E2E (Team 3 E2E role) with a recorded
run carrying `slurm_allocation_event`s, checking the slot text (`queued`,
`saving outputs`, `releasing`, `allocation failed`), the dot `aria-label`,
and that the dot is reachable with Tab. Adding a DOM test harness only for
this is not worth it.

Checks: `biome`, `tsc`, `bun run test:unit`.

## 9. Open points for the coordinator

- Confirm the plugin sends `node_key` (lease key) and `member_task_ids`.
- Confirm whether the plugin ever sends `releasing`; the `STORED` row costs
  one line either way.
- `holding` before `ready` is a plugin-side fix (section 3).
