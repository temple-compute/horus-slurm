# Security review: slurm-load WP1

Scope: `git diff chdominguez/slurm-load...HEAD` (`ff34220`) against
`docs/design/slurm-load.md`, including User decisions. Line numbers are for
`src/horus_slurm/target/slurm.py` at `ff34220`.

Checks run: `uv run pytest -q -p no:cacheprovider` (147 passed), and a
throwaway script that builds `SlurmTarget` with bad values for the new fields.

Counts: 0 critical, 0 high, 3 medium, 4 low.

## Medium

### M1. `poll_seconds` floor of 1 s lets one user flood the login node
- Where: `slurm.py:278-284`, used by `poll` at `:1021` and `:1034`.
- Problem: every poll of every task runs `test -e <exit_code>` over SSH
  (`BaseTarget.path_exists`). Inline targets also run their own `squeue`
  per task per poll (not cached). Before this branch the rate was fixed at
  5 s; now the form allows 1 s. 100 inline tasks at 1 s = about 200 commands
  per second on a shared login node; 50 members of a named target at 1 s =
  50 `test -e` per second. That is the load this work set out to remove.
- Fix: raise the floor to `ge=5` (the old value, and Nextflow's
  `pollInterval`). If a faster check is wanted later, put the inline `squeue`
  behind the same `queue_check_interval` cache first.
- Status: verified by code reading; the bounds script shows 1.0 is accepted.

### M2. `queue_check_interval` up to 600 s widens the stale-allocation window 20x
- Where: `slurm.py:285-291`, `_allocation` at `:656-663`, `cached_queue` at `:164-171`.
- Problem: the design accepts that `_allocation` may spool a step into an
  allocation that ended less than one interval ago, and sized that window at
  30 s. At 600 s, a launch can spool into a cancelled or timed-out allocation
  for up to 10 minutes. The member then sees the cached RUNNING for the rest
  of the interval, reads `""`, waits out the exit-file retries, and fails,
  instead of the step running in a new allocation. 600 s is also above
  Slurm's default `MinJobAge` (300 s), which the design relies on.
- Fix: lower the cap to `le=120` (well under `MinJobAge`). Or keep 600 for
  members but have `_allocation` ignore a cached reading older than 30 s.
- Status: verified by code reading.

### M3. Ended Slurm states count as live
- Where: `_allocation` `:663`, `poll` `:1045`.
- Problem: only `state == ""` means gone. `squeue -j <id>` can still list an
  ended job as `CANCELLED`, `TIMEOUT`, `COMPLETED`, `FAILED`, `NODE_FAIL`,
  `PREEMPTED`, `OUT_OF_MEMORY` until `MinJobAge` passes. Then `_allocation`
  keeps the dead allocation and spools steps into it, `poll` does not
  compare-and-clear, and `is_live` stays True. The check itself was already
  there, but the cache now pins that reading for every member for a whole
  interval.
- Fix: one module set `ENDED = {"CANCELLED", "COMPLETED", "FAILED",
  "TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY", "BOOT_FAIL",
  "DEADLINE"}`, and treat `state == "" or state in ENDED` as gone in both
  places. Add one test with `squeue` returning `CANCELLED|None|cpu`.
- Status: suspected. Depends on `squeue -j` listing ended jobs on the
  cluster's Slurm version; not run against a real Slurm.

## Low

### L1. Bounds are only checked when the model is built
- Where: `slurm.py:278-291`.
- Problem: `target.poll_seconds = 0.001` and
  `model_copy(update={"queue_check_interval": 0})` are accepted (verified by
  script). Nothing in this plugin does that, but the canvas or runtime could
  change a target in place, which turns M1 into a busy loop.
- Fix: `validate_assignment=True` on the model config, or clamp in the
  `poll_interval` property (`max(self.poll_seconds, 1)`).
- Status: verified (script); impact suspected.

### L2. A hung `squeue` blocks every member and the launch path
- Where: `_queue_state` `:1349-1358`.
- Problem: the refresh runs under `refresh_lock`. If the SSH call stalls,
  every member waits on the lock, and `_allocation` waits too while holding
  `handle.lock`, so `release` and new launches stall. Before, only the
  caller hung.
- Fix: `asyncio.wait_for(self._read_queue(handle), timeout=60)` inside the
  lock; on timeout return `None` (stores nothing, same as a failed read).
- Status: suspected; depends on whether `run_command_sync` has its own
  timeout (not checked).

### L3. Limits are per target, not per cluster
- Problem: each named target and each inline task polls on its own. Many
  targets on one cluster add up with no shared cap.
- Fix (later): batch ids into one `squeue -j a,b,c` per cluster per
  interval. Out of scope for WP1.
- Status: design note.

### L4. The fields next to the new ones have no bounds (existed before)
- Where: `slurm.py:292-293` (`exit_code_retries`, `exit_code_delay`).
- Problem: they appear in the same form. `exit_code_delay=0` with a large
  `exit_code_retries` runs back-to-back `test -e` for every member when an
  allocation dies (50 members at once).
- Fix: `Field(ge=0, le=20)` and `Field(ge=1, le=60)`.
- Status: verified by code reading.

## Checked, no finding
- No shell command uses the new values. They only feed the runtime's sleep
  and the `time.monotonic()` comparison. NaN, inf and values below the
  minimum are rejected (verified by script).
- The cache is keyed by job id and cleared on submit (`:667`), so a member
  of an old allocation never reads a newer one, or the other way round.
  Compare-and-clear at `:1045-1050` checks the id under `alloc.lock`.
- A failed `squeue` is never stored (`:1355`); existing test covers it.
- Lock order is `lock` then `refresh_lock` everywhere; `poll` lets go of
  `refresh_lock` before it takes `lock`.
- The job id in `squeue -j` and `scancel` is not quoted, but it is checked
  against `_JOB_ID` at `sbatch` (`:500`). That was there before this branch.
