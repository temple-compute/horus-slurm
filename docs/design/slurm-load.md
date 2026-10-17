# Slurm load: design

Team 1 of `docs/design/slurm-load-plan.md`. Inputs:
`slurm-load-codebase.md` (code facts) and `slurm-load-competitors.md`
(prior art). Line numbers are for `src/horus_slurm/target/slurm.py` at
`eedb317`.

## Goal

One `squeue` per allocation per refresh interval, shared by every member
of a named target through the `_Allocation` handle. Today each member runs
its own `squeue` on the same allocation id every `poll_interval` (5 s), so
50 members make 10 calls/s for one job (codebase notes, section 1).

Scope (plan ownership): `poll`, `_queue_state`, `_partition_summary` /
`_sinfo`, and the liveness check in `_allocation`. Inline targets
(`releasable` is False) keep today's behaviour exactly.

## Design

### Cache on the handle

The cache is one entry on `_Allocation`: the job id it was read for, the
`QueueState`, and the monotonic time of the read.

- **Keyed by allocation job id.** A member reads the entry only when
  `queue_job_id` equals the job id in its own `JobHandle`. A member of an
  older allocation never reads the state of a newer one, and the reverse.
- **Dropped on submit.** `_allocation` clears the entry when it submits a
  new allocation (under `handle.lock`, next to `generation += 1`). Real
  job ids do not repeat, but the unit fakes always return `12345`, and a
  stale `""` under a reused id would fail the new members.
- **Fresh** means `now - queue_at < QUEUE_STATE_INTERVAL`.

### Single in-flight refresh

`_queue_state(handle)` on a releasable target:

1. If the entry is fresh for this job id, return it. No lock, no call.
2. Otherwise take `handle.refresh_lock` (a new lock, not `handle.lock`).
3. Check again: another member may have refreshed while this one waited.
   If fresh, return it.
4. Run the `squeue` as today. On success, store `(job_id, state, now)`
   and return it.
5. On failure (nonzero exit, `None`) return `None` and **store nothing**.

A failed read is never cached, and never as `""`: one slurmctld hiccup
must not fail every member at once (competitors, pitfalls; Nextflow and
Parsl do the same). `None` keeps today's meaning in `poll`: unknown, so
check the exit file and try again next tick. Members queued on the lock
behind a failed read each try once in turn; at worst that is today's
rate, never more, and only one call is in flight at a time.

`refresh_lock` is separate from `handle.lock` because `handle.lock` is
held across `sbatch` and file writes on a first launch, and across
`scancel` and `rm -rf` in `release`. Pollers must not queue behind those.

**Lock order:** `handle.lock` then `refresh_lock`, never the reverse.
`_allocation` holds `handle.lock` and may refresh (step 2). `poll` never
holds `refresh_lock` when it takes `handle.lock` (it releases it first by
returning from `_queue_state`).

Inline targets skip all of this and run the `squeue` directly, as today.

### `poll`: compare-and-clear on `""`

When a shared member reads `state == ""`:

```python
async with handle.lock:
    if handle.job_id == job_id:
        handle.job_id = None
```

`job_id` is the id in the member's own `JobHandle`. A blind clear could
drop an allocation that `_allocation` already resubmitted, orphan its
pilot until the heartbeat check, and make the next launch submit a third
(codebase notes, risk 2). The rest of the `""` branch is unchanged: the
member still retries its exit file and asks `sacct`.

With the job id cleared, `is_live` is False, so the lease manager stops
holding and pinging a dead allocation (`lease.py:363`, `:698`), and the
next launch submits without a liveness `squeue`.

### Liveness check in `_allocation`

`_allocation` keeps its check, but calls the cached `_queue_state`. When
the entry is fresh it costs nothing and the lock is not held across an
SSH call. When it is stale, the launch refreshes the shared entry for
everyone. A cached `""` means gone, as today; a failed read (`None`)
keeps the allocation, as today.

Accepted window: a launch may spool into an allocation that ended less
than `QUEUE_STATE_INTERVAL` ago. The step is never claimed, the member
reads `""` on its next poll and fails. Today the window is the gap
between the `squeue` and the `mv`, so it is wider now. The usual cause is
the time limit, which fails the running members anyway. Team 2's
orphaned-spool work moves such steps into the next allocation.

### `sinfo` on the handle

`_partition_checked` and `_partition_nodes` move to the handle for shared
targets. The handle gets two fields with the same names, and
`_partition_summary` picks its storage:

```python
cache = self._alloc if self.releasable else self
```

The rest of the method is unchanged. It already stores the check time
before it awaits `_sinfo`, so members that arrive during the call return
the previous summary instead of starting a second `sinfo`. The 60 s
`PARTITION_SUMMARY_INTERVAL` and the re-read on a reason change stay.
Result: one `sinfo` per allocation per 60 s while pending on a node
reason, not one per member.

## Refresh interval

`QUEUE_STATE_INTERVAL = 30.0`, a module constant next to
`PARTITION_SUMMARY_INTERVAL`, separate from `poll_interval`.

Why:
- Step completion comes from the exit file, read every `poll_interval`
  (5 s). `squeue` only answers "is the allocation still there, and is it
  pending or running". Nextflow splits the two the same way
  (`pollInterval` 5 s, `queueStatInterval` 60 s).
- Prior art polls liveness at about 60 s: Nextflow 60 s, Parsl 60 s,
  Snakemake 40 to 180 s, NERSC caps `watch` at 60 s.
- 30 s, not 60 s, because the interval is also the delay before members
  see RUNNING, the delay before they notice a dead allocation (then up to
  25 s of exit-file retries), and the window above in `_allocation`.
  30 s halves those against 60 s for 1 extra call per minute.
- `MinJobAge` defaults to 300 s, well above 30 s, so a job that ends just
  after a refresh is still in `squeue` on the next one.

Effect for 50 members: from 10 `squeue`/s to one per 30 s per
allocation.

## Events and announce

Unchanged per member. `_last_state` and `_record` stay on the copy. Each
member's `poll` passes the shared `QueueState` (and the shared partition
summary) to its own `_announce`, which deduplicates per member. So every
member still emits its own `SlurmJobEvent` with its `task_id` and writes
its own record on each transition, at most one refresh interval late.
The cache never touches `_last_state`.

As today, the reported state is the allocation's, not the step's.

## Work packages

One WP: everything is in `slurm.py` and one test file.

| Id | Scope | Files | Depends on |
|---|---|---|---|
| WP1 | Handle fields and cache; cached `_queue_state` for shared targets; compare-and-clear in `poll`; cache drop on submit in `_allocation`; `_partition_summary` storage switch; tests below | `src/horus_slurm/target/slurm.py`, `tests/unit/test_shared_allocation.py` | none |

Merge order per the plan: after the tests team, before robustness.
Robustness owns `_submit_step` and `release`; WP1 does not touch them.

## Interface contract

All private to the plugin. No runtime API change.

`slurm.py` module:
- `QUEUE_STATE_INTERVAL: float = 30.0`

`_Allocation` new fields:
- `refresh_lock: asyncio.Lock` (default factory, like `lock`)
- `queue_job_id: str | None = None`
- `queue: QueueState | None = None`
- `queue_at: float = 0.0` (monotonic)
- `_partition_checked: tuple[str, float] | None = None`
- `_partition_nodes: dict[str, int] | None = None`

`_Allocation` new method:
- `cached_queue(job_id: str) -> QueueState | None`: the entry if it is for
  `job_id` and fresh, else `None`.

Behaviour:
- `_queue_state(handle)`: same signature and return. Shared targets go
  through the cache; failed reads are not stored.
- `_allocation()`: on submit, sets `queue_job_id = queue = None`.
- `poll(handle)`: on `""` for a shared target, compare-and-clear of
  `handle.job_id` under `handle.lock`.
- `__deepcopy__` already returns a fresh `_Allocation`, so a deep copy
  starts with an empty cache.

## Test plan

All in `tests/unit/test_shared_allocation.py`, with `FakeInner`,
`_shared` and `_launch`. Time is controlled by patching
`slurm_mod.time.monotonic` or `slurm_mod.QUEUE_STATE_INTERVAL`, like the
existing `PARTITION_SUMMARY_INTERVAL` test (`test_slurm_target.py:1022`).

New:
1. **50 copies, one `squeue` per interval.** Launch 50 `idle_copy()`
   members, poll each once (also with `asyncio.gather`, to cover the
   herd), assert one `squeue -h` command. Advance time past the interval,
   poll all again, assert two. This is the plan's done criterion.
2. **Every member announces.** After test 1, each member's
   `_record.state` is RUNNING and the bus has one event per `task_id`.
3. **Failed read is not cached.** `squeue` returns nonzero: the poll
   returns `None`, no member fails, and the next member's poll runs
   `squeue` again. Then `squeue` succeeds and later polls use the cache.
4. **Failed read is not gone.** Same, but assert `handle.job_id` is kept
   and no second `sbatch` happens on the next launch.
5. **Cache dropped on submit.** Allocation gone (`squeue` empty), member
   polls `""`, a new launch resubmits. With `squeue` now RUNNING, the new
   member's first poll runs `squeue` and does not return 1.
6. **Compare-and-clear.** A member holding the old id polls `""` after
   `_allocation` resubmitted: `handle.job_id` stays the new id. A member
   holding the current id clears it, and `is_live` is False.
7. **Launch skips `squeue` when fresh.** Poll (RUNNING), launch within the
   interval: no extra `squeue`. Launch after the interval: one.
8. **`sinfo` shared.** Allocation PENDING (Resources): 50 member polls run
   one `sinfo`, and every member's record has the same `partition_nodes`.
9. **Inline unchanged.** Two inline targets polling run one `squeue`
   each per poll.

Updated:
- `test_an_ended_allocation_is_replaced`: no poll runs before the second
  launch, so the cache is empty, the liveness `squeue` runs and the test
  passes as is. Keep it, and add a stale-cache variant: poll RUNNING,
  switch `squeue` to empty, launch within the interval (no resubmit, the
  accepted window), then after the interval (resubmit).

Must pass unchanged: the inline partition tests in `test_slurm_target.py`
(`:184`, `:1000`, `:1022`) and the rest of `test_shared_allocation.py`.

Run: `uv run ruff check`, `uv run mypy`, `uv run pytest -q -p no:cacheprovider`.

## Deviations from the plan

- **Interval.** The plan says the first member past `poll_interval`
  refreshes. This design uses `QUEUE_STATE_INTERVAL` (30 s). The prior
  art and the exit-file design support a longer interval; see above.
  The done criterion ("≤ 1 per interval") holds either way.
- **Cache keyed by job id and dropped on submit,** not just a timestamp.
  Without it a cached `""` fails the members of the next allocation
  (codebase notes, risk 1).
- **Separate `refresh_lock`.** The plan says "a small refresh lock"; this
  makes explicit that it is not `handle.lock`, and fixes the lock order.
- **Liveness reuses `_queue_state`** instead of a separate freshness
  check, so a stale launch also refreshes the shared entry.

Not in scope, noted for later: the death fan-out (each member runs its
own exit-file retries and `sacct` when the allocation goes; 50 `sacct`
calls). A shared `sacct` result on the handle would cut that to one.
Also the inherited `_last_state` / `_cancelling` from `idle_copy`
(codebase notes, risk 7).

## Open questions

1. **Refresh interval: 30 s, 60 s, or `poll_interval` (5 s)?**
   Recommended: 30 s (see Refresh interval).
2. **Back off after a failed `squeue`?** Prior art suggests it; this
   design only serialises retries and never does worse than today.
   Recommended: no backoff in this PR; revisit if Team 4 sees rate
   limiting.

## User decisions

- The squeue refresh interval is a target field, not a module constant, so it can be set per target from the canvas: `queue_check_interval: float = Field(default=30.0, ge=5, le=600, title=_("Queue check interval"), description=_("Seconds between squeue checks of a shared allocation"))`. It shows up in the schema-driven target form with no frontend change, next to `exit_code_retries` and `exit_code_delay`. Default 30 s.
- The 5 s per-task check is also a target setting (user, 2026-10-17: "implement the target settings so are editable"). Plugin-only, following the `releasable` pattern: a field `poll_seconds: float = Field(default=5.0, ge=1, le=300, title=_("Poll interval"), description=_("Seconds between checks of each task's exit file"))` and `poll_interval` as a property returning it (`# type: ignore[override]`), since `PollingChannelProcess` reads `target.poll_interval` from the instance. No horus-runtime change. Applies to inline targets too. Unit test: a target with `poll_seconds=2` reports `poll_interval == 2`, and the schema exposes both new fields.
- Approved 2026-10-17: no backoff after a failed squeue in this PR.
- After review (2026-10-17): `queue_check_interval` capped at `le=120`, below Slurm's default `MinJobAge` (300 s), and `poll_seconds` floored at `ge=5`. Defaults unchanged.
- After review (2026-10-17): `poll` no longer clears `handle.job_id` on `""`. Clearing it made `is_live` False, so the lease manager skipped the run-end `release()` and its `rm -rf spool run`. The next launch still sees the cached `""` and resubmits. Ended states that `squeue` still lists (`CANCELLED`, `COMPLETED`, `FAILED`, `TIMEOUT`, ...) count as gone, like `""`.
