# Review: slurm badges WP3 (ready / released)

Scope: `965bbf6` as merged into `chdominguez/slurm-badges` (HEAD `988d1b6`,
which also has Robustness's step move `3ad9eea`), against
`docs/design/slurm-badges.md` and the WP3 items in
`review-badges-correctness.md` (#1, #4). Store read-only:
tc-os `slurm-badges-wp4/frontend/src/hooks/workflow/machineStore.ts`.
Runtime read: `horus_runtime/core/lease.py` (installed in `.venv`).

Ran: `uv run pytest -q -p no:cacheprovider`: 252 passed. Two throwaway
repro tests in `/tmp/wp3-review/test_wp3.py` (both reproduce, below).

## Findings

| # | Sev | Where | Problem | Fix | Status |
|---|---|---|---|---|---|
| 1 | High | `slurm.py:729` `emit_node_phase` | After `released` (reason `gone`) from `poll`, `job_id` stays set, so `is_live` is true and the lease keeps going: `reconcile` sends `holding`, `_release` sends `draining` / `drain_failed`, all at the same generation. The store deleted the node on `released`, so there is no `prev` to compare generations with and it re-creates the node (`holding` or `draining`). The later `release()` sends nothing (`released_generation` gate), so the card shows "saving outputs" or holding until the next `queued` or the run-end clear. Same path, smaller: after `release()`, the lease's `finally` sends `busy` at the old generation when a member started meanwhile (replaced by the next `queued`). | In `emit_node_phase`, return first when `alloc.released_generation == alloc.generation`. Nothing valid follows `released` in a generation; the next `queued` bumps it. Add a test: gone poll, then `holding` and `draining`, then `release`: events stay `queued, ready, released`. | Verified: repro gives `queued, ready, released, holding, draining` and no second `released` |
| 2 | Medium | `slurm.py:1290` `poll` | The open item. With the 30 s shared `squeue` cache and 5 s polls, a member shorter than the cache window finishes while the cached reading still says `PENDING`: no `ready`, and the `holding` that `reconcile` sends right after `member_finished` is dropped (ready gate) and never re-sent (runtime sends `holding` only on entering HOLDING). No later poll can fix it: with no running member nobody polls. The card keeps `queued` on the bound member (hidden once finished) and the target's other cards miss the holding tooltip for the whole hold window; if a member starts, its `busy` is dropped but its poll later sends `ready`, so it recovers. Common case: short tasks on an idle cluster. Judgment: not acceptable as is; the fix is one line. | In the exit-code branch, `if job_id == self._alloc.job_id and not gone: self._emit_allocation("ready")` before returning. An exit code proves the pilot ran, and it is emitted before the runtime's `reconcile`, so the following `holding` passes. Test: cached PENDING, exit_code written, poll, then `holding`: events `queued, ready, holding`. | Verified: repro gives only `queued` |
| 3 | Low | `slurm.py:1278` | Prior #4 (dropped `holding` not re-sent): outside finding 2 it is unreachable. A lease is HOLDING only with no running member, and only running members poll, so `ready` after a dropped `holding` comes from a newly started member, whose `busy` maps to the same stored `ready`. | None beyond finding 2. | Reasoned from `lease.py:303-375` |
| 4 | Low | `tests/unit/test_shared_allocation.py` | WP3 tests miss finding 1 and 2 paths and a gone-after-requeue ordering check. Verified by hand that a gone poll whose step is moved gives `queued 1, ready 1, released 1, queued 2` (right order). | Add the two tests from findings 1 and 2. | Verified (repro run) |

## Checked and fine

- `ready` and `released` once per generation, whichever copy sees it first;
  both gates reset when `_allocation` bumps `generation`. `poll` emits right
  after `_queue_state` with no await between, and the cache only ever holds
  the newest reading, so copies cannot emit out of order.
- Stale readings: a straggler from an older generation polls a different job
  id and emits nothing; a task of an ended allocation never evicts the
  current reading (`_queue_state`).
- `release()`: emits only after scancel succeeds (or "invalid job id"),
  with the cancelled `job_id`; a scancel error emits nothing and the lease
  retries; a gone `released` suppresses the run-end one. Prior #1 (WP2 alone
  leaves cards on `queued`) is closed once WP3 ships with WP4.
- Holding before ready: `busy`/`holding` pass only once `ready_generation`
  matches, which is set only by emitting `ready`. A failing bus still sets
  the gate (then `holding` correctly replaces `queued`).
- Gone seen first by `_allocation`'s liveness check: no `released` for N,
  but `queued` N+1 replaces the node, as the design accepts.
- Design said "after `handle.job_id = None`"; the code emits just before it,
  so `job_id` is the cancelled id as the contract wants. Fine.

## Does a card always end?

Not today: finding 1 leaves a re-created node after a gone allocation until
the next `queued` or run end. With fix 1 every generation ends in exactly one
`released` or is replaced by `queued` N+1. With fix 2, `holding` never shows
before `ready` and is no longer lost after a short member.
