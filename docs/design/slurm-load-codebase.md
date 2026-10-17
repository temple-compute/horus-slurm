# Slurm load: codebase notes

Research for Team 1 of `docs/design/slurm-load-plan.md`. No code changes.

Paths:
- `slurm.py` is `src/horus_slurm/target/slurm.py` in this repo.
- `rt/` is `horus-runtime/src/` in the tc-os checkout
  (`/home/cdominguez/orca/workspaces/tc-os/slurm-same-target`).

## 1. Control commands a shared member issues

Every call goes through `self.inner`. On an SSH transport each one is a
remote exec. `rt/horus_runtime/core/target/base.py:694-702` runs
`path_exists` as `test -e` over a shell by default. `LocalTarget` answers it
from the local filesystem (`rt/horus_builtin/target/local.py:367-371`) and
reads files locally (`local.py:297`).

A shared member's `JobHandle.extra["job_id"]` is the allocation's job id
(`slurm.py:420-426`, `slurm.py:449`, `slurm.py:774`). So all members of one
allocation run `squeue` on the same id.

### Per poll (each member, every `poll_interval`)

| Call | Where | When |
|---|---|---|
| `squeue -h -j <alloc> -o "%T\|%R\|%P"` | `slurm.py:987` -> `slurm.py:1307-1309` | every poll |
| `sinfo -h -p <part> -o "%T\|%D"` | `slurm.py:995` -> `slurm.py:1357`, `slurm.py:1371-1373` | only while PENDING on a `NODE_REASONS` reason; per copy at most once per 60 s or on reason change (`slurm.py:85`, `slurm.py:1349-1355`) |
| `put_file` record | `slurm.py:901` -> `slurm.py:939-942` | only on a state, reason or node-summary change (`slurm.py:885`) |
| `path_exists <job_dir>/exit_code` | `slurm.py:998` | every poll |
| `get_file <job_dir>/exit_code` | `slurm.py:999` | once, at the end |
| `path_exists` + `get_file` stdout log | `read_output` `slurm.py:1088` -> `slurm.py:1282-1285` | every poll (from `stream`, see section 2) |
| `path_exists` + `get_file` stderr log | `slurm.py:1088-1090` -> `slurm.py:1282-1285` | every poll |

When the allocation leaves the queue (`state == ""`), each member also runs:

- `_retry_exit_code`: up to `exit_code_retries` (5) `path_exists` calls, 5 s
  apart (`slurm.py:1017`, `slurm.py:1056-1070`, `slurm.py:264-265`);
- `sacct -n -X -P -j <alloc>` once (`slurm.py:1024`, `slurm.py:1403-1406`),
  or once on the cancelled branch (`slurm.py:1004-1007`).

### Per launch (each member)

| Call | Where |
|---|---|
| `mkdir <job_dir>` | `slurm.py:407` |
| `mkdir <log dir>` | `slurm.py:411` |
| liveness `squeue` on the allocation, **under `handle.lock`** | `slurm.py:623-631` -> `slurm.py:1307` |
| first launch only: `umask 077 && mkdir ...` | `slurm.py:638` -> `slurm.py:675-680` |
| first launch only: `put_file pilot.sh` | `slurm.py:640-642` |
| first launch only: `sbatch --parsable` | `slurm.py:643` -> `slurm.py:455-457` |
| `put_file run/<step>.job` | `slurm.py:762` |
| `put_file spool/<step>.tmp` | `slurm.py:764` |
| `mv spool/<step>.tmp spool/<step>.sh` | `slurm.py:766-768` |
| `put_file .horus_slurm_job_id` | `slurm.py:433` -> `slurm.py:858` |
| `put_file` record (SUBMITTED) | `slurm.py:447` -> `slurm.py:901`, `slurm.py:939` |

### Other paths

- Cancel (SIGTERM/SIGKILL): one `touch ...cancel; rm spool/...` exec
  (`slurm.py:1144-1149`), then a record write (`slurm.py:1151`).
- Other signals: `squeue -s -h -j <alloc>` (`slurm.py:1132` ->
  `slurm.py:778-780`), then `scancel --signal` (`slurm.py:1138-1140`).
- `keepalive`: one `touch heartbeat` per lease every 300 s
  (`slurm.py:606-609`; `rt/horus_runtime/core/lease.py:52`,
  `lease.py:685-711`). Per lease, not per member. Negligible.
- `release`: `scancel` + `rm -rf spool run` under the lock
  (`slurm.py:572-592`). Once per allocation.

### Estimate: 50 members, `poll_interval` 5 s, all running

Per member per poll: 1 `squeue`, 1 `path_exists` (exit_code), 2 log reads
of `path_exists` + `get_file` each (`get_file` only once the log exists).

| Command | Per poll, 50 members | Per second |
|---|---|---|
| `squeue` | 50 | **10/s** |
| `path_exists` | 150 | 30/s |
| `get_file` (logs) | up to 100 | up to 20/s |
| `sinfo` (only while pending on a node reason) | 50 per 60 s | ~0.8/s |
| **Total transport calls** | ~300 | **~60/s** |

On an SSH transport all of these are remote execs on the login node. Only
`squeue` and `sinfo` reach slurmctld, and `squeue` at 10/s for one job id
is the waste the plan targets. A burst of 50 launches adds 50 `squeue`
calls, run one after another because they hold `handle.lock`
(`slurm.py:623-625`). When the allocation dies, there are 50 `sacct` calls
and up to 250 `path_exists` calls over 25 s.

Out of Team 1's scope but worth knowing: the file reads (`path_exists`,
`get_file`) are 5x the `squeue` count and are per member by nature (own
`job_dir`, own logs). `read_output` already notes the whole-file re-read
(`slurm.py:1081-1086`).

## 2. How `PollingChannelProcess` drives `poll` and `read_output`

- `launch` returns a `PollingChannelProcess(self, handle)`, where `self` is
  the member copy (`rt/horus_runtime/core/target/base.py:540-549`).
- Executors iterate `proc.stream()` and then call `proc.wait()`
  (`rt/horus_builtin/executor/shell.py:91-106`;
  `rt/horus_builtin/executor/python_fn_external.py:165`).
- `stream` loop (`rt/horus_runtime/core/target/channel.py:294-312`):
  `poll(handle)`, then `read_output(handle)`, then
  `sleep(target.poll_interval)`. One `poll` and one `read_output` per member
  per interval.
- `wait` after `stream`: `_returncode` is already set, so there is no extra
  `poll` (`channel.py:243-251`). `communicate` adds one `read_output`
  (`channel.py:253-256`).
- Members' loops are not aligned. Each starts when its own `launch` returns,
  so 50 members spread their polls across the 5 s window. A shared cache
  with a TTL of `poll_interval` gives at most one refresh per interval, not
  one per poll tick.
- Copies come from `idle_copy()`, which is `model_copy()` with `_task` and
  `_task_future` cleared (`rt/horus_runtime/core/target/base.py:334-348`).
  It is called from the scheduler (`rt/horus_builtin/workflow/scheduler.py:125`),
  map (`rt/horus_builtin/workflow/map.py:443`) and loop
  (`rt/horus_builtin/workflow/loop.py:468`). Private attrs are copied
  shallowly (`base.py:141-145`), so `_alloc` is the same object for every
  copy. A deep copy gets a fresh `_Allocation` (`slurm.py:155-158`).

What can be shared:

- **`squeue` result**: same allocation id for every member. Safe to share
  if the cache is keyed by the allocation job id (or generation).
- **`sinfo` summary**: the partition is `self.partition or queue.partition`
  (`slurm.py:1357-1359`). Both are the same for all members of one handle.
- **Not shared**: the `exit_code` probe, the log reads, and the record
  writes. They use the member's own `job_dir` and task.

## 3. Private attrs: per member vs. handle

| Attr | Decl. | Verdict | Why |
|---|---|---|---|
| `_last_state` | `slurm.py:267` | **per member** | Dedup for `_announce` (`slurm.py:885-891`). Each member emits its own `SlurmJobEvent` with its `task_id` (`slurm.py:914-924`) and writes its own record. If shared, only the first member would announce RUNNING. Other cards would stay SUBMITTED. |
| `_record` | `slurm.py:289` | **per member** | One record per task, written to the task's side-artifacts dir (`slurm.py:436-446`, `slurm.py:939-942`). |
| `_cancelling` | `slurm.py:283` | **per member** | Set by one step's cancel (`slurm.py:1150`). It picks the cancelled branch in that member's `poll` (`slurm.py:1004`). |
| `_partition_checked` | `slurm.py:274` | **handle** (shared targets) | Inputs are the shared queue state and partition (`slurm.py:1344-1359`). Cleared when not pending (`slurm.py:1345-1348`), which is the same for all members. |
| `_partition_nodes` | `slurm.py:280` | **handle** (shared targets) | Same as above. Each member still passes it to its own `_announce`. |

Inline targets keep all five on the copy. `releasable` is the switch
(`slurm.py:481-484`).

Existing quirk (not new, but it gets worse with more copies): `idle_copy`
clears only `_task` and `_task_future`. So a copy inherits the source
target's `_last_state`, `_cancelling` and `_partition_*`. The scheduler
reuses a target from the idle pool (`scheduler.py:121-125`,
`scheduler.py:132-137`), and `launch` resets only `_record`
(`slurm.py:436`). A reused or cloned target whose earlier task was
cancelled keeps `_cancelling = True` into its next task (`slurm.py:1004`).

## 4. Risks, ranked

1. **Stale `""` across generations.** If the cache holds `QueueState("")`
   for allocation N, a member launched into allocation N+1 must not read
   it. Otherwise it fails with GONE (`slurm.py:1011-1036`). Key the cache by
   job id. `_allocation` bumps `generation` and sets a new `job_id` under
   the lock (`slurm.py:632-646`); the cache must be dropped or keyed there.
2. **Compare-and-clear of `job_id`.** The plan has `poll` clear
   `handle.job_id` on `state == ""`. A member's handle carries the id it
   launched into (`slurm.py:774`, `slurm.py:449`). By the time it polls,
   `_allocation` may already have resubmitted. A blind clear would drop the
   new allocation, leave its pilot orphaned until the heartbeat check
   (`slurm.py:122-125`), and make the next launch submit a third one. Clear
   only when `handle.job_id == polled id`, under `handle.lock`, like
   `slurm.py:630-631`.
3. **Event announce per member.** The cache must not move `_last_state`, or
   only one member would emit and record each transition (section 3). Each
   member keeps calling `_announce` with the shared `QueueState`. Note the
   reported state is the allocation's, not the step's: a step waiting in
   `srun` for room shows RUNNING. This is today's behaviour too.
4. **Lock use across copies.** `handle.lock` is one `asyncio.Lock` shared by
   all copies (`slurm.py:144`). Today it is held across a remote `squeue`
   on every launch (`slurm.py:623-629`) and across `scancel` and `rm` in
   `release` (`slurm.py:572-592`). A refresh lock for the cache should be a
   separate lock, or the cache refresh should not take `handle.lock`.
   Otherwise 50 pollers queue behind a launch's `sbatch` or a `release`. If
   the refresh fails (`None`, `slurm.py:1311-1312`), do not cache it.
   Members fall back to "unknown", as today (`slurm.py:988`). The asyncio
   primitives in `_Allocation` (`slurm.py:144`, `slurm.py:153`) bind to the
   first event loop that waits on them. A target object reused across two
   `asyncio.run` calls could hit a loop mismatch. This is low risk and
   unchanged by the plan.
5. **Freshness in `_allocation`.** Skipping the liveness `squeue` when the
   cache is fresh means a launch may spool into an allocation that ended
   less than `poll_interval` ago. The step is then never claimed, and the
   member sees `""` on its next poll and fails. Today the window is the
   same size (the `squeue` is a point-in-time read), so this is acceptable.
   Team 2's orphaned-spool work covers the rest.
6. **Death fan-out.** On `""`, every member runs its own
   `_retry_exit_code` and `sacct` (`slurm.py:1017-1028`). Not in Team 1's
   brief, but it is the next-largest burst after `squeue`. A shared `sacct`
   result on the handle would cut 50 calls to 1.
7. **Inherited per-copy state** from `idle_copy` (section 3). Existing
   issue; a fix would reset `_last_state` and `_cancelling` in `launch`.

## 5. Tests to add or update

In `tests/unit/test_shared_allocation.py`. Fixtures: `FakeInner`,
`inner.responds`, `inner.commands_starting` (`tests/unit/conftest.py:83-110`),
`_shared` and `_launch` (`test_shared_allocation.py:53-84`).

Add:
- **50 copies poll, at most one `squeue` per interval.** Launch 50
  `idle_copy()` members, poll each once, and assert
  `len(inner.commands_starting("squeue -h"))` is 1. Advance a patched
  `time.monotonic` past `poll_interval`, poll again, and assert 2. This is
  the plan's done criterion.
- **Every member still announces.** After the shared poll, each member's
  `_record.state` is RUNNING, and the bus sees one event per `task_id`.
- **Cache keyed by allocation.** Allocation gone (`squeue` empty), then a
  new launch resubmits. The new member's first poll must run `squeue` on
  the new id and must not return 1.
- **Compare-and-clear.** A member holding the old job id polls `""` after
  `_allocation` resubmitted. `handle.job_id` stays the new id.
- **`_allocation` skips `squeue` when fresh.** Poll, then launch within the
  interval, and assert no extra `squeue`. Launch after the interval and
  assert one.
- **`squeue` failure is not cached.** A nonzero `squeue` returns `None` and
  the next member's poll retries.
- **`sinfo` shared.** Allocation PENDING (Resources): 50 member polls run
  one `sinfo`, and every member's record has the same `partition_nodes`.

Keep unchanged (inline behaviour):
- `tests/unit/test_slurm_target.py` partition tests (e.g. `:184`, `:1000`)
  and the per-target `_partition_*` caching. They must pass as they are.
- `test_an_ended_allocation_is_replaced` (`test_shared_allocation.py:229-236`)
  relies on the liveness `squeue` at launch. With a cache, the test must
  make the cached state stale (or empty) before the second launch, or it
  will stop resubmitting.
