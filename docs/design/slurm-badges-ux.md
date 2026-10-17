# Slurm allocation badges: card UX

Team 3, UX. Scope: what a member task card shows for a shared Slurm
allocation (`slurm_allocation_event`), using the slots the card already has.
Plan: `slurm-badges-plan.md`.

## What the card has today

Header row, right side, left to right (`TaskCard.tsx`):

1. **Phase slot.** One `font-mono text-[10px]` span. Shows the machine phase
   from `useMachinePhase` (`PHASE_TEXT`), else the staging or restoring
   `TransferChip`. The phase wins.
2. **Duration.**
3. **Status dot** (`StatusDot`). Its tooltip (`statusLabel`) carries the
   machine phase line, then OOM, then the task's error or skip reason.
   Inline Slurm tasks use `SlurmStatusDot` instead: a running task whose
   `slurm_job` record is `PENDING` shows a pending dot with
   `Queued in Slurm: <reason>`.
4. **Tray** (`SlurmIndicator`). Hover peek of the task's `slurm_job` record:
   job id, state, reason, partition nodes, timings.

Facts that shape the design:

- A shared member's `slurm_job` record carries the allocation's job id, and
  its state is the allocation's queue state (`poll` runs `squeue` on that id).
  So a member's record already says `PENDING` + reason while the allocation
  queues.
- `SlurmStatusDot` only runs for `inlineTarget?.kind === "slurm"`. Shared
  members (`target: {ref}`) get the plain dot, so today they read as
  `running` while the allocation sits in the queue.
- `nodePhases` shows `provisioning` and `failed` only on the tasks bound to
  the node; `holding`, `draining`, `destroying` show on every card of the
  target, finished ones included.

## Rule: one fact, one place

| Fact | Where | Not here |
|---|---|---|
| Allocation phase (queued, saving outputs, releasing, failed) | Phase slot text | Dot colour, tray |
| Why it is queued (Slurm reason) | Dot tooltip | Phase slot |
| Task's own status | Dot colour | Phase slot |
| Job id, partition nodes, timings, state history | Tray peek | Card face |

The phase slot says *what*, the dot tooltip says *why*. This is the pattern
terraform cards already follow (`provisioning` text, "Provisioning its
target" tooltip).

## Phase mapping

The store maps Slurm phases onto the existing `NodePhase` (plan, Team 3).
The card picks the copy by target kind: `getMachines(workflow)[machineOf(task)]?.kind === "slurm"`
selects a Slurm copy table next to `PHASE_TEXT`. No store field, no new
component.

| Event phase | `NodePhase` | Phase slot | Dot tooltip line | Shown on |
|---|---|---|---|---|
| `queued` | `provisioning` | `queued` | `Queued in Slurm: <reason>` (or `Queued in Slurm` with no reason) | bound members, not finished |
| `ready` | `ready` | none | none | - |
| `busy` | `ready` | none | none | - |
| `holding` | `holding` | none | `Its allocation is idle, still held` | all cards of the target |
| `draining` | `draining` | `saving outputs` | `Saving outputs from its allocation` | all cards of the target |
| `drain_failed` | `holding` | none | `Its allocation is idle, still held` | all cards of the target |
| `releasing` (winding-down fallback, `destroying`) | `destroying` | `releasing` | `Releasing its allocation` | all cards of the target |
| `released` | cleared (as `destroyed`) | none | none | - |
| `failed` | `failed` | `allocation failed` | `Its Slurm allocation failed` | bound members, not finished |

Notes:

- `holding` has no slot text, like terraform. An idle allocation is normal
  between members; text there would be noise on every card.
- `released` clears the node for all members, so the card falls back to its
  own status. No "released" text.
- `failed` follows the existing rule: hidden once the task is terminal, since
  the task's own error then says it.
- The `<reason>` comes from the member's `slurm_job` record through
  `slurmReasonLabel` (e.g. `Priority: waiting for higher-priority jobs`).
  The record is already fetched for the tray, so this costs no request.
  A member with no record yet (not launched) gets `Queued in Slurm`.

## Status dot for shared members

Extend the `SlurmStatusDot` branch from inline Slurm to any Slurm target
(inline, or a ref whose `targets:` entry is `kind: slurm`). Nothing else
changes: a running member whose record is `PENDING` gets the pending dot and
`Queued in Slurm: <reason>`.

When the dot is `SlurmStatusDot`, its label must still include the other
`statusLabel` lines (OOM, error) when present. Pass `statusLabel` through and
join, rather than dropping it.

## Copy

Short, lower case in the slot (matches `provisioning`, `saving outputs`),
sentence case in tooltips, no trailing period, no "please", no "your".

Slot:

- `queued`
- `saving outputs`
- `releasing`
- `allocation failed`

Tooltips:

- `Queued in Slurm: <reason>` / `Queued in Slurm`
- `Its allocation is idle, still held`
- `Saving outputs from its allocation`
- `Releasing its allocation`
- `Its Slurm allocation failed`

## Sketches

Card header, 300px. `( )` dot, `[ ]` tray icon.

Queued, first member launched:

```
+----------------------------------------------------+
| [py] relax_structure            queued  0:42  (o) v|  (o) yellow
|      python_task                                   |
+----------------------------------------------------+
  tooltip on (o): Queued in Slurm
                  Priority: waiting for higher-priority jobs
```

Queued, member not launched yet (pending):

```
| [py] score_pose                 queued        (o) v|  (o) yellow
  tooltip on (o): Queued in Slurm
```

Running inside the allocation:

```
| [py] relax_structure                    3:10  (o) v|  (o) blue
```

Done, allocation idle between members:

```
| [py] relax_structure                    3:10  (o) v|  (o) green
  tooltip on (o): Its allocation is idle, still held
```

Done, allocation saving outputs before release:

```
| [py] relax_structure    saving outputs  3:10  (o) v|  (o) green
  tooltip on (o): Saving outputs from its allocation
```

Allocation failed, task not yet failed:

```
| [py] score_pose      allocation failed        (o) v|  text red
  tooltip on (o): Its Slurm allocation failed
```

Released: same as the plain finished card, no slot text.

## Accessibility

- The slot text is visible text, so screen readers read it as is. Do not
  rely on its colour: `allocation failed` says failed in words.
- `StatusDot` is a bare `span` today, so screen readers get nothing. Give
  it `role="img"` and `aria-label={label ?? status}`, the same string as
  the tooltip. This covers the Slurm reason and holding lines. Small change
  to the existing component, not a new one.
- The tooltip trigger must be focusable for keyboard users. `TooltipTrigger
  asChild` on a span is not. Add `tabIndex={0}` to the dot span.

## What not to show

- Allocation job id, step names, generation on the card face. The tray peek
  already shows the job id from the member's record.
- `ready` / `busy` text. The blue dot already says running.
- `holding` text in the slot.
- `released` text, or any "allocation ended" message.
- Member counts ("2 of 5 running") or other members' states.
- Partition node summary, timings, queue position on the face. Tray only.
- A second Slurm badge or chip next to the phase slot. The slot holds one
  thing.
- Cost or billing. Slurm events are not billed and get no admin machine row
  (backend check, Team 3).
- New fields in the `SlurmIndicator` peek. Its header stays `Slurm job`.

## Edge cases

- Phase precedence is unchanged: phase slot beats `restoring cache`. Restore
  callbacks run while the allocation queues, so a queued card shows `queued`,
  not the restore chip.
- `DeletableEdge` treats `provisioning` as "waiting for machine". Staging
  happens before the allocation is submitted, so an edge rarely sees
  `queued`. Acceptable as is.
- Events can arrive as `busy` (lease, generation N) then `queued`
  (generation N+1). The newer generation replaces the node, so the card shows
  `queued`. The store already handles this.
- A page reloaded mid-queue shows no slot text until the next event (store is
  live only). The dot still shows the reason from the record.
