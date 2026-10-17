# Review: slurm-load WP1, performance

Branch `chdominguez/slurm-load-wp1` (`ff34220`) against
`docs/design/slurm-load.md`, including User decisions. Lines are
`src/horus_slurm/target/slurm.py` at `ff34220`.

## Measured counts (50 members)

Run with the unit fakes (`FakeInner` wrapped to also count `mkdir`,
`put_file`, `path_exists`, `get_file`). Over SSH each one is a login-node
round trip (a shell channel, or an SFTP request if the transport has one).
`pytest`: 147 passed.

| Scenario | squeue | sinfo | sacct | other commands | file ops |
|---|---|---|---|---|---|
| 50 launches, first one submits | 1 | 0 | 0 | sbatch 1, umask 1, mv 50 | mkdir 100, put_file 201 |
| Poll round, cache fresh | 0 | 0 | 0 | 0 | path_exists 50 (+50 record writes on RUNNING) |
| Poll round, cache stale (`gather`) | 1 | 0 | 0 | 0 | path_exists 50 |
| Pending on nodes, 50 polls (unit test) | 1 | 1 | 0 | 0 | path_exists 50 |
| Allocation dies, `exit_code_retries=5` | 1 | 0 | **50** | 0 | path_exists **300**, put_file 50 |
| squeue failing, one round | **50** (serial) | 0 | 0 | 0 | path_exists 50 |
| SIGTERM all 50 | 0 | 0 | 0 | touch 50 | put_file 50 |
| 50 inline targets, one round | 50 | 0 | 0 | 0 | path_exists 50 |

Per launch: 7 round trips (2 mkdir, 4 put_file, 1 mv), plus 1 squeue at
most once per `queue_check_interval`. Steady state per poll round:
1/30 squeue (was 50) and 50 exit-file probes. The design goal (one squeue
per allocation per interval) is met, also for the herd.

## Findings

### H1. `queue_check_interval` up to 600 s is past `MinJobAge` (suspected)

`slurm.py:285-291` (`le=600` at `:288`) allows `le=600`. The design justifies the interval with
"`MinJobAge` defaults to 300 s, well above 30 s, so a job that ends just
after a refresh is still in `squeue` on the next one". Above ~300 s that no
longer holds: the pilot is purged before the next read, and
`squeue -h -j <purged id>` exits nonzero ("Invalid job id specified") on
the Slurm versions I know of. `_read_queue` (`:1376`) maps nonzero to
`None`, never `""`. Then members whose steps were killed without an exit
file poll forever, and `_allocation` (`:657-663`) never clears the job id,
so new launches keep spooling into a dead allocation.

Fix: cap at `le=240` (below the default `MinJobAge` with margin), and say
in the description that it must stay below `MinJobAge`. Optionally also
treat "Invalid job id" on stderr as `""`. Not verified against a real
slurmctld (no cluster here); the bound itself is verified in the code.

### M1. A failing squeue serialises the herd and stalls launches under `handle.lock` (verified)

`_queue_state` (`:1348-1358`) stores nothing on failure (by design), so
every member queued on `refresh_lock` runs its own squeue, one at a time.
Measured: 50 squeues per round, run serially. With a 20 ms fake squeue the
round took 1.02 s; a launch arriving meanwhile took 1.04 s, all of it
holding `handle.lock` (`_allocation`, `:656-659`) behind the queue. A real
slurmctld outage usually means a timeout (`MessageTimeout`, 10 s default)
per call: ~500 s per round, during which every launch, `release` and
compare-and-clear waits. Today the same 50 calls run in parallel, so the
rate is not worse (as the design says), but the latency is 50x worse.

Fix, without a backoff (keeps the 2026-10-17 decision): coalesce waiters
on one read. Record the failure time (`alloc.failed_at`) and in the
double-check after taking `refresh_lock` return `None` when a read failed
after this caller started waiting. One squeue per round, members still
retry next tick. Test: 50 polls with `FAILED`, assert 1 squeue.

### M2. Allocation death still fans out: 50 sacct + 250 exit-file retries (verified)

When the pilot goes, the one cached `""` is shared (1 squeue), but each
member then runs `_retry_exit_code` (`:1088-1112`, 5 probes over 25 s) and
its own `sacct -j <alloc id>` (`poll`, `:1065`). Measured: 50 sacct at
once, 300 path_exists. The design lists this as out of scope; it is now
the largest burst this target sends, and 50 simultaneous `sacct` hit
slurmdbd, which is slower than slurmctld.

Fix: cache `_final_state` on `_Allocation` keyed by job id, same pattern
as `queue` (all members ask about the same allocation id). Same lock
pattern; store only a non-`None` answer. Leave the exit-file retries per
member (different files), or skip them when the shared sacct already says
the allocation ended. Follow-up WP, not a blocker.

### M3. Exit-file probes are now the steady load: 50 per `poll_seconds` (verified)

Per round, 50 `path_exists` (one per member, `poll`, `:1032`). At the
default 5 s that is 10 round trips/s, the same rate the design removed for
squeue; only the command is cheaper. With `poll_seconds=1` (allowed,
`:278-284`) it is 50/s. Not a regression, but the plan's "login-node load"
goal is only half met for large groups.

Fix (later): one listing per allocation per tick, e.g. the launcher also
touches `<alloc_dir>/done/<step>` and one shared `list_dir` per
`poll_seconds` answers every member, reading `exit_code` only for steps
listed. Same cache pattern as `queue`.

### L1. `poll_seconds` lower bound 1 s is 5x the old fixed load (verified in code)

`slurm.py:278-284`, `ge=1`. For inline targets each job runs its own
squeue every poll (measured: 50 squeue for 50 inline targets per round),
so `poll_seconds=1` means 1 squeue/s per job, 5x the old fixed 5 s.
Fix: `ge=5`, or mention in the description that low values load the
login node per running job.

### L2. Compare-and-clear waits for a resubmit's sbatch (verified)

`poll`, `:1045-1050` (lock at `:1048`), takes `handle.lock`, which `_allocation` holds
across `_make_private_dir`, `put_file` and `sbatch` (`:666-680`), and
`release` across `scancel` and `rm -rf`. Measured: 50 polls of a dead
allocation waited 0.48 s behind a 0.5 s sbatch. Bounded by one sbatch;
acceptable. Fix if it matters: skip the lock when
`alloc.job_id != job_id` (read without the lock first; it is only ever
cleared or replaced).

### L3. Partition cache not dropped on submit (verified in code)

`_allocation` clears `queue_job_id`/`queue` (`:667`) but not the shared
`_partition_checked`/`_partition_nodes`. A new allocation pending on the
same reason reuses the old summary for up to 60 s. Saves a call, shows a
summary up to 60 s old; harmless. Fix: clear both next to `:667`, if
wanted.

## Checked, no issue

- Herd on a stale cache: one squeue (verified, `gather` of 50).
- Fast path is lock-free; `refresh_lock` is never held while taking
  `handle.lock`, so no lock-order inversion (`poll` returns from
  `_queue_state` before `:1048`).
- `sinfo`: one per allocation per 60 s while pending on nodes (unit test).
- Launch liveness check reuses the cache: 1 squeue for 50 launches.
- `poll_interval` property reads the instance field; runtime picks it up.
