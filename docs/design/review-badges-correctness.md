# Review: slurm badges correctness (WP1+WP2, WP4)

Scope: horus-slurm `chdominguez/slurm-badges-wp12` (`chdominguez/slurm-badges...HEAD`)
and tc-os `chdominguez/slurm-badges-wp4` (`chdominguez/slurm-same-target...HEAD`),
against `docs/design/slurm-badges.md` and its User decisions.

Ran: `uv run pytest -q -p no:cacheprovider` (144 passed, 95% coverage);
`bun test src/hooks/workflow/machineStore.test.ts` in tc-os (17 passed).
Read: runtime `horus_runtime/core/lease.py` emit points (315, 375, 573, 579, 616).

## Contract agreement (verified by reading both sides)

- Fields match: `phase`, `task_id`, `node_key`, `target_ref`, `generation`,
  `member_task_ids` are read by `applyNodeEvent`; `job_id`, `reason` sent, not read.
- Phases match: the plugin's eight `AllocationPhase` values are all in
  `NodeEventData["phase"]`; `queued -> provisioning`, `drain_failed -> holding`,
  `busy -> ready`, `released` ends the node like `destroyed`. No `releasing` sent
  or stored, as decided.
- `event_type` string `slurm_allocation_event` is the same on both sides;
  `WorkflowContext` dispatch uses `MACHINE_EVENT_TYPES` and still falls through
  to the events list.
- Event volume: zero events per member; per generation one `queued` (or one
  `failed`), then only lease transitions (the runtime emits on change, not per
  reconcile). OK.

## Findings

| # | Sev | Where | Problem | Fix | Status |
|---|---|---|---|---|---|
| 1 | Medium | horus `slurm.py:611`, tc-os `TaskCard.tsx` | On this branch nothing sets `ready_generation` and nothing emits `ready` or `released` (WP3). If WP4 ships against a runtime with WP2 only, the bound member's card reads `queued` (and its dot tooltip "Queued in Slurm") for the whole time it runs, and the node is never ended. Design calls it the interim state, but the two PRs are independent. | Merge/release WP3 before or with WP4, or hold WP2's `queued` emit until WP3 lands. | Verified by reading; consistent with design 2.4 |
| 2 | Medium | tc-os `TaskCard.tsx` `StatusDot`, `aria-label={label ?? status}` | When a label is passed the accessible name drops the status. A failed task's dot reads only its error text; a running shared member reads "Its allocation is idle, still held" with no "running". Screen readers lose the one thing the dot encodes. | `aria-label={label ? \`${status}: ${label}\` : status}` (status first). | Verified by reading |
| 3 | Low | horus `slurm.py:627` | On lease phases `task_id` is the lease copy's task, which can be from an earlier generation; the store adds `task_id` to members, so a finished gen-1 task gets bound to the gen-2 node. Visible effect is small (holding/draining show on all target cards anyway). Design 2.1 accepts this. | Send `task_id` only when it is in `alloc.members`, else `""`. | Suspected (reasoned, not run) |
| 4 | Low | horus `slurm.py:611` | A `holding` dropped before ready is not re-sent (runtime emits only on entering HOLDING). Mostly unreachable: `ready` comes from a running member's `poll`, by which time `member_started` has moved the lease to BUSY. Worth a WP3 test. | In WP3, after emitting `ready`, nothing needed if the test confirms; else re-emit the lease's current phase. | Suspected |
| 5 | Low | tc-os `TaskCard.tsx` `machineKind` | Kind is looked up in the top-level `workflow.targets`; a subworkflow that declares its own target of the same name with another kind gets the wrong copy. | Resolve the name through the same scope `machineMemberTasks` uses. | Suspected |
| 6 | Low | horus `slurm.py:624` | Unknown phase dropped at debug level (as designed); a runtime rename would vanish silently. | Log at warning once per phase, or keep as designed. | Verified by reading |
| 7 | Nit | horus `slurm.py:677-681` | `if handle.job_id is None` tested twice around `members.add`. | Move `members.add` above one combined block. | Verified by reading |

Checked and fine: `failed` carries `job_id None` and re-raises; a retry bumps
generation and resets members; inline targets emit nothing (`releasable` guard);
`draining`/`drain_failed` always pass; `released` and `destroyed` share the
`ended` path and clear `nodeOf`; `failed`/`provisioning` show only on bound
members and are hidden once finished; `SlurmStatusDot` now covers named
targets (Q3) and drops the duplicate "Queued in Slurm" line.

Not checked: card copy and Tab focus in a browser (no DOM harness; design defers
to E2E); `biome`/`tsc` in tc-os.
