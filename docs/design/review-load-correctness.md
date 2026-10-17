# Review: slurm-load WP1 correctness

Branch `chdominguez/slurm-load-wp1` against `chdominguez/slurm-load`
(commit ff34220), checked against `docs/design/slurm-load.md` and its User
decisions. Run: `uv run pytest -q -p no:cacheprovider` passes (147 tests,
95% coverage), and `uv run ruff check` is clean. Nothing was run against a
real cluster.

## Checked and correct

- **Lock order.** `_allocation` takes `handle.lock`, then `refresh_lock`
  (inside `_queue_state`). `poll` returns from `_queue_state`, which frees
  `refresh_lock`, before it takes `alloc.lock`. `release` takes only
  `handle.lock`. The locks are never taken in the reverse order.
- **Compare-and-clear** (`slurm.py:1045`) clears `alloc.job_id` only if it
  still equals the polled id. It is skipped for inline targets and when
  cancelling.
- **The cache is keyed by job id and dropped on submit** (`slurm.py:667`,
  under `handle.lock`, next to `generation += 1`). A step handle carries
  the allocation's job id (`slurm.py:477`, `:807`), so the key matches.
- **Failed reads are never stored.** `_read_queue` returns `None` on a
  nonzero exit, and `_queue_state` stores only a result that is not `None`.
- **Shared and per-member state.** The squeue entry and the sinfo entry are
  on `_Allocation`. `_last_state`, `_record` and `_cancelling` stay on each
  member. `__deepcopy__` starts with an empty cache.
- **`poll_interval`.** `PollingChannelProcess` reads
  `self._target.poll_interval` from the instance (`channel.py:250`, `:312`),
  both in tc-os and in the installed runtime, so the property works.
  Nothing reads it at class level.
- **Inline targets.** `_queue_state` goes straight to `_read_queue`, and
  `_partition_summary` uses `self`. The inline tests pass unchanged.

## Findings

| # | Sev | Where | Problem | Fix | Status |
|---|-----|-------|---------|-----|--------|
| 1 | Medium | `slurm.py:1045` | When `poll` sees `""`, it now leaves `alloc.job_id = None` with no resubmit, so `is_live` turns False. The lease manager calls the run-end `release()` only for live targets (`horus_runtime/core/lease.py:746`), so it skips this one. As a result, `rm -rf spool run` never runs: the launchers and job scripts, which hold exported env values, stay on disk. `ready_callbacks.clear()` on `run_end`/`abort` is also skipped, so if the same target object is reused, the old callbacks run again. Before this change, `job_id` stayed set until `release()`. | In `release`, run the `rm -rf` whenever `alloc_dir` is set and clear the callbacks whether or not `job_id` is set. Only the `scancel` should depend on `job_id`. `release` belongs to the robustness WP, so hand the fix to that WP. Alternative: do the `rm -rf` in `poll`'s clear path. | Verified by reading `lease.py`; not run end to end |
| 2 | Low | `slurm.py:1424` | The shared sinfo entry records the check time before awaiting `_sinfo`. Members that poll while that call runs get the old `_partition_nodes` (`None` at first) and write and announce the record twice. | Await `_sinfo` under a lock (for example `refresh_lock`, or a second small lock) and re-check inside it, as `_queue_state` does. Make the fake yield in `test_fifty_copies_share_one_sinfo`. | Verified with a throwaway test where sinfo yields: 5 members, 1 sinfo, partitions `[{…}, None, None, None, None]` |
| 3 | Low | `slurm.py:658` | `_allocation` waits for `refresh_lock` while it holds `handle.lock`. If squeue keeps failing, nothing is cached, so it waits behind every queued member's failed squeue in turn. `release()` waits behind it. | When `refresh_lock` is busy, have `_allocation` call `_read_queue` directly, or add a timeout on the lock. Optional, given the no-backoff decision. | Suspected |
| 4 | Low | `slurm.py:1355` | The cache holds a single entry. After a resubmit, old-generation members that are still polling evict the new allocation's entry, and the two generations keep evicting each other. The squeue rate rises to one per poll until the old members finish. | Skip the store when `job_id != alloc.job_id` and the reading is not `""`, or skip all stores from non-current ids. | Suspected |
| 5 | Low | `slurm.py:1048` | The compare-and-clear takes `alloc.lock` around a check-and-set that contains no await, so the lock is not needed. Taking it makes members that see `""` wait behind `sbatch` or a `release`'s `scancel` and `rm -rf`. The design specifies the lock, so this is not a defect. | Drop the lock, or keep it to match the design. | Suspected |
| 6 | Info | `slurm.py:658` | Liveness accepts a cached RUNNING reading up to `queue_check_interval` old, so new steps can spool into an allocation that has already ended. | The design accepts this window. No change. | Accepted in design |
| 7 | Info | `slurm.py:285` | `queue_check_interval` appears on inline targets' forms but has no effect there. | Leave it, per the User decision. Optionally say so in the field description. | Read |
| 8 | Info | `slurm.py:161`, `:302` | The sinfo fields are declared on both classes, with six `noqa: SLF001` suppressions. | Optional: one small cache object held by both classes. | Read |
