# slurm-robust: codebase notes

Research for Team 2 (robustness). Line numbers are for `src/horus_slurm/target/slurm.py` at `fa387b9` unless a file is named.

horus-ssh is not a dependency of this repo, so it is not in this repo's `.venv` after `uv sync --dev`. The SSH notes below come from horus-ssh 0.0.5 as installed in the tc-os checkout (`tc-os/slurm-same-target/.venv/lib/python3.13/site-packages/horus_ssh/target/ssh_target.py`, cited as `ssh_target.py:N`), with asyncssh from the same venv.

## 1. `_submit_step`: put_file + mv, and where it can fail

Flow (`_submit_step`, 707-774, called from `launch` at 420):

1. `handle = await self._allocation()` (719). The lock is released when this returns. `_submit_step` does not hold it.
2. Build the srun launcher (722-761). The launcher hardcodes `{alloc_dir}/run/{step}.job`, `{alloc_dir}/run/{step}.cancel` and `{job_dir}/exit_code` (738-740).
3. `put_file(script, run/<step>.job)` (762).
4. `put_file(launcher, spool/<step>.tmp)` (764). The pilot only globs `spool/*.sh` (118), so a `.tmp` never runs.
5. `run_command_sync("mv spool/<step>.tmp spool/<step>.sh")` (766). `wait() != 0` raises `TaskExecutionError` (769-773).
6. Return `(job_id, alloc_dir)`. `launch` puts them in `JobHandle.extra` with `step` (426, 449).

The pilot claims a launcher with `mv "$f" run/` (120), so a claimed launcher is `run/<step>.sh`. Do not confuse it with `run/<step>.job`, the step script written in step 3.

### SSH transport behaviour

- `run_command_sync` (`ssh_target.py:376-409`) raises `TaskExecutionError` only if connecting or opening the channel fails. Then the command never ran.
- If the connection drops after the channel opened, asyncssh's `communicate()` and `wait()` return normally. `returncode` is `None` (no exit status, `asyncssh/channel.py:1348`). `_SSHChannelProcess.wait()` maps that to `-1` (`ssh_target.py:81-85`), and `communicate()` leaves `proc.returncode` as `None`. The command may or may not have run. horus-ssh does not reset `_conn` on this path. The next call fails to open a channel, and only then resets it (`ssh_target.py:404`).
- `put_file` (`ssh_target.py:494-534`) calls `sftp.makedirs(parent)` and then writes with a plain `open("wb")`. The write is not atomic. Connection errors are wrapped in `TaskExecutionError`. `asyncssh.SFTPError` subclasses (permission denied, no such file, `SFTPFailure`) are not in the except tuple. `SFTPError` is an `asyncssh.Error` and not an `OSError` (`asyncssh/sftp.py:942`), so it escapes as a raw asyncssh exception.
- `LocalTarget.put_file` also creates parent dirs (`horus_builtin/target/local.py:290-291`). With either transport, writing into an `alloc_dir` whose `spool/` or `run/` was removed silently recreates them with the orchestrator's umask. The `alloc_dir` itself stays 700.

### Failure points

| Step | Failure | Did the launcher land? | What happens today |
|---|---|---|---|
| 1 `_allocation` squeue (625) | rc != 0 or `None` returns `QueueState None` | n/a | Treated as alive (630), so the step can go into a dead allocation (see section 2) |
| 1 `_make_private_dir` (638) | rc != 0 | no | Raises. `generation` was already bumped (633), so the next try uses a new number. |
| 1 `put_file pilot.sh` (640) | raises | no | Raises. The dir is left behind with no pilot. |
| 1 `_sbatch` (643, 453-477) | SSH drop after sbatch ran: `returncode None != 0` | n/a | Raises "sbatch submission failed: " with an empty message. A pilot may be queued with no handle. It idles until `HEARTBEAT_STALE_S` (900 s) after the mkdir touch (679), holding the allocation's size. |
| 3 `put_file .job` (762) | TaskExecutionError or raw SFTPError | no | Raises. A partial `.job` is harmless. |
| 4 `put_file .tmp` (764) | same | no (`.tmp` is never run) | Raises. The `.tmp` stays until `release` removes `spool/` (590). |
| 5 mv: channel open fails | TaskExecutionError | no | Raises. Safe. |
| 5 mv: connection drops mid-command | `wait()` returns -1 | **maybe** | Raises. If mv ran, the pilot runs the step anyway. A task retry relaunches with a new `job_dir` and a new step name (419), so the work runs twice. This is the duplicate the plan targets. |
| 5 mv: real failure (`spool/` gone, ENOSPC) | rc 1 | no | Raises. Correct. |
| after 5 | `_publish_job_id`, `_announce` | yes | Best effort, never raises (857-863, 913-928) |

For the "check before raising" fix:

- The check is another `run_command_sync` over the same, possibly broken, connection. It can raise too. In that case, raising is still the only honest answer.
- One shell command answers it: `[ -e spool/<step>.sh ] || [ -e run/<step>.sh ]`. Exit 0 means it landed: return success. Use `run/<step>.sh`, not `.job`.
- If it did not land, a `rm -f spool/<step>.tmp` keeps `spool/` clean (optional).
- If the check itself is ambiguous (`returncode None`), raising can still duplicate the step. That window is two commands wide instead of one. A full fix would take the launcher back first: `rm spool/<step>.sh`, the same trick as `_signal_step` (1146). Exit 0 means it is now gone, so raising is safe. Otherwise it is in `run/` and already running.
- With `LocalTarget` there is no ambiguity. Only SSH needs this.

## 2. Launchers still in `spool/` when the allocation leaves the queue

### When it happens

The pilot claims every spooled launcher at once with `&` (120). srun `--exact` then waits for room inside the allocation. So "spooled, never claimed" only happens when the pilot never ran or stopped looping:

- The allocation was cancelled or failed while still PENDING (outside Horus, admin, NODE_FAIL before start; `--no-requeue` at 704).
- The pilot exited idle (122-126) after a launcher was moved in. The window is between the `for` loop and the exit check, or longer when keepalive failed for 15 min (SSH outage) and new steps keep arriving.
- `_allocation`'s squeue failed (`None`, treated as alive at 630) while the allocation was really gone.
- A step submitted between `_allocation()` returning and the mv, while the allocation died or was released. `release` holds the lock (572), but `_submit_step` does not, after 719.

### What happens now (member side)

Each member is a shallow copy with its own `_record`, `_last_state` and `_cancelling`, and a shared `_alloc` (135-158, 295).

1. `poll(handle)` (978) calls `_queue_state(handle)` with `extra["job_id"]` = old allocation id (987, 1308). It gets `QueueState("")`.
2. Not cancelling, so 1011: `_retry_exit_code` sleeps `exit_code_retries * exit_code_delay` = 5 x 5 s (264-265, 1047-1071). A spooled launcher never writes `exit_code`, so this always runs out.
3. `_final_state` (sacct) gives CANCELLED, TIMEOUT or similar, else `GONE`. It is announced (1024-1028), and `poll` returns 1. The member fails.

Claimed steps (`run/<step>.sh`) are killed with the batch step. They fail the same way. The plan says these keep failing.

### What happens now (shared handle side)

- `handle.job_id` stays set to the dead id. Only `_allocation` clears it (630-631), and only when a new `_submit_step` calls it. `release` also clears it (586).
- `is_live` (497-499) stays True, so the lease manager keeps it in `reconcile` (`horus_runtime/core/lease.py:363`) and keepalive keeps touching the dead dir (602-614, harmless).
- The next `_allocation` call bumps `generation` (633) and makes `alloc_dir = <wd>/.horus_alloc/<lease_key>-<generation>` (634-637). It submits a new pilot and reruns `ready_callbacks` (651-660).

### What moving them to a fresh allocation needs

Where: the `state == ""` branch of `poll` (1011), before `_retry_exit_code`, for handles with `extra["step"]` and not `_cancelling`. `poll` belongs to the Load team in the plan's ownership split, so this needs a small hook there, agreed with them. One option: a `_requeue_step(handle) -> bool` owned by Robustness, called from one line in `poll`.

Steps:

1. **Take the launcher back atomically:** `rm {old_alloc}/spool/{step}.sh` (no `-f`). Exit 0 means it was unclaimed and is now ours. Non-zero means it was claimed: fall through to today's path. This is the same rule `_signal_step` uses (1146-1148).
2. **Get a live allocation:** `await self._allocation()`. Several members find the dead allocation at once. `_allocation` serialises them on `handle.lock`. The first one sees squeue `""`, clears `job_id` and submits. The rest see the new id alive. Each one still runs a squeue under the lock, which is the Load team's concern.
3. **Rebuild the launcher, do not move the file.** The old launcher hardcodes the old `alloc_dir` paths for `.job` and `.cancel` (738-753). `_signal_step` reads `alloc_dir` from `extra` (1130). Reuse the body of `_submit_step` after line 719, split into a helper that takes `handle`. The step script is in `self._record.script` (436-439, set by `launch`) or at `{old_alloc}/run/{step}.job`. The `step` name and `job_dir` stay the same.
4. **Update the handle in place.** `JobHandle` is `@dataclass(frozen=True)` (`horus_runtime/core/target/channel.py:175`), but `extra` is a plain dict. `PollingChannelProcess` keeps the same handle object for every poll, read and signal (`channel.py:226-301`). So set `extra["job_id"]`, `extra["raw_job_id"]` and `extra["alloc_dir"]`. There is no need to replace the handle.
5. **Update the member's own state.** Set `_record` `job_id` and `script_path` (`model_copy`). Call `_publish_job_id(new_id)` (848) so `SlurmJobScope` follows the new job. Reset `_last_state` so the next `_announce` reports SUBMITTED or PENDING for the new id. Reset `_partition_checked` and `_partition_nodes`. Return `None` from `poll`.
6. **Bound it.** If every new pilot dies before it claims (a bad partition accepted by sbatch, a pilot crash), steps bounce forever. Keep a move counter in `extra` (for example `extra["moves"]`) and fail after 1 or 2 moves.
7. **Cancel during a move.** `_signal_step` touches `.cancel` under `extra["alloc_dir"]`. If a cancel lands between steps 1 and 4, the mark goes to the old dir and the new launcher does not see it. Do the move while no `send_signal` can interleave, or check `_cancelling` again after 4 and remove the new launcher.

`generation` needs no extra work. `lease.member_finished` reads `target.generation` from the shared handle when the member ends (`lease.py:341`), so a moved member records the new generation.

## 3. Fields that reach `#SBATCH` lines

`_sbatch_header` (1223-1252) is used by inline jobs (`_build_sbatch_script`, 1204-1222) and the pilot (`_pilot_script`, 691-705).

| Line | Value | Source | Type | Can carry `\n`? |
|---|---|---|---|---|
| 1232 `--job-name` | `horus-<job_dir name>` (inline), `horus-<_ref>` (pilot, 701) | job dir uuid; `_ref` matches `^[a-z][a-z0-9_-]{0,62}$` (`horus_runtime/core/target/ref.py:30`, checked in `workflow/targets.py:44-50`) | str | no |
| 1233-1234 `--output`/`--error` | stdout/stderr path (inline), `{alloc_dir}/pilot.log` (pilot, 700) | `task.side_artifacts_dir` or `job_dir`; `working_directory` | str | **yes**, through `working_directory` or the task's working dir. No validator except trailing-slash strip (`horus_runtime/core/target/base.py:118-130`). |
| 1235 `--nodes` | `self.nodes` | target | int | no |
| 1236 `--ntasks` | `self.ntasks` | target | int | no |
| 1237 `--cpus-per-task` | `resolved.cpus_per_task` | target or `request.cpus` | int | no |
| 1239 `--partition` | `self.partition` | target | str | **yes** |
| 1241 `--account` | `self.account` | target | str | **yes** |
| 1243 `--qos` | `self.qos` | target | str | **yes** |
| 1245 `--mem` | `resolved.mem` | `self.mem`, or `f"{request.memory_gb}G"` (`resources.py:173-174`) | str | **yes** (target field only) |
| 1247 `--time` | `resolved.time_limit` via `to_slurm_time` | `self.time_limit`, or **the task's `resources.walltime`** (`resources.py:175-176`). `to_slurm_time` returns unknown text unchanged (`resources.py:37-46`). | str | **yes**, from target and from task |
| 1249 `--gres` | `resolved.gres` | `self.gres`, or `gpu:{int}` | str | **yes** (target field only) |
| 1251 `extra_sbatch_args` | each item | target | list[str] | **yes**, per item |

Notes:

- The plan's list covers the target fields. It misses `resources.walltime` from the task, which reaches `--time` on inline jobs. `ResourceRequest` has no validator (`horus_runtime/core/resources.py:36-62`), and a runtime change is out of scope. It also misses `working_directory` and the paths built from it.
- A pydantic `field_validator` on `SlurmTarget` covers the target fields. A guard in `_sbatch_header` covers the rest: reject any value with `\n` or `\r` and raise `TaskExecutionError`. That is one place, and it catches task-derived values and paths too.
- Spaces in `--output` paths are a separate issue (not injection). sbatch reads the line up to whitespace. Not checked here; worth a Team 4 case.
- Outside the header, `_script_body` writes `export {key}=...` with an unquoted `key` (1265). Env keys come from the runtime. This is out of scope for Team 2, noted only.
- The step launcher quotes every srun arg (741-750). `scancel`, `squeue` and `sacct` get ids from sbatch, checked by `_JOB_ID` (131, 472).

Existing validators on `SlurmTarget`:

- `hold_timeout_minutes` `ge=0, le=120` (232-240) and `max_parallel` `ge=1` (241-245).
- `nodes`, `ntasks` and `cpus_per_task` have no bounds.
- `validate_named` (506-524): `cpus_per_task`, `mem` and `time_limit` are required, and `nodes == 1`.
- `validate_member` (526-563): task size checked against the allocation.
- From `BaseTarget`: `_reject_ref` (`base.py:107-116`) and `_strip_trailing_slash` on `working_directory` (`base.py:118-130`).
- There is no `field_validator` in `src/horus_slurm` yet (grep for `validator` finds none). The first one follows the `base.py` style: `@field_validator(...)`, `@classmethod`, and `ValueError` with a `_()` message.

## 4. `.horus_alloc` dirs: created, never removed

- Created by `_allocation` (634-638) through `_make_private_dir` (664-689). That makes `<wd>/.horus_alloc` (700, must be ours and not a symlink) and `<lease_key>-<gen>/` (700) with `spool/`, `run/` and `heartbeat`. Then `pilot.sh` (640), and Slurm writes `pilot.log` (700).
- Partly removed by `release` (588-592): `rm -rf spool run`. `pilot.sh`, `pilot.log` and `heartbeat` stay. Nothing removes the dir itself.
- An allocation that ends on its own (time limit, idle exit) keeps its `spool/` and `run/` too, unless `release` is called later while `job_id` is still set.
- New dirs appear for every generation of every run. `lease_key` is a fresh uuid per declared target per process (`base.py:141-160`), so every run starts a new set. One run with N allocations leaves N dirs.

For the cleanup:

- **Keep the newest `pilot.log` of each target:** dir names hold `lease_key`, not `_ref`, and the key changes every run. So you cannot group by target from the name. Either add `_ref` to the dir name (`{ref}-{lease_key}-{gen}`; `_ref` is a safe charset), or read the target name from `pilot.sh` (`#SBATCH --job-name=horus-<ref>`). The first is simpler.
- **Age:** use the `heartbeat` mtime, not the dir mtime. Keepalive touches the heartbeat while the allocation is used (602-614), so a live allocation of another run in the same working dir is never "old". The dir mtime changes on every spool mv.
- **Where:** in `_allocation`, after `_make_private_dir` has checked that the root is ours and not a symlink. Best effort: log at debug and never raise. One command, for example `find <root> -mindepth 2 -maxdepth 2 -name heartbeat -mtime +N`, then `rm -rf` on each parent. Skip the current `alloc_dir`. `find` does not follow symlinks by default.
- `_allocation` runs under `handle.lock`, so the cleanup adds one SSH round trip to that lock hold. Doing it once per process (a class-level flag) avoids repeating it per generation.

## 5. Risks (ranked) and tests to add

1. **Duplicate step after an ambiguous mv over SSH** (section 1, step 5). The step runs twice and the task is reported failed. SSH only. High.
2. **Spooled steps fail when the allocation dies before claiming them** (section 2). Each one also waits 25 s in `_retry_exit_code` before failing. Medium-high, mostly allocations cancelled while pending.
3. **Header injection**, from target fields and also from the task's `resources.walltime` and the working directory (section 3). The workflow author is the cluster user, so this is mostly about robustness: a stray newline makes a wrong job, not a wrong user. It still matters for UI-stored workflows. Medium.
4. **Ambiguous sbatch over SSH** leaves an untracked pilot that holds resources for up to 15 min (section 1 table). Medium. Not in the plan. A possible fix: on `returncode None`, look for the pilot with `squeue -h -n horus-<ref> -o %i` and adopt it, or at least say in the error that a job may exist. The pilot's idle exit bounds the cost either way.
5. **`release` on a finished but not yet purged allocation.** `scancel` on a completed job may exit non-zero with "already completing or completed", which is not tolerated (580-585). `release` then raises, and the lease retries with backoff until the job is purged (`MinJobAge`). Not verified on a cluster; worth adding to Team 4 case 3.
6. **Raw `asyncssh.SFTPError` from `put_file`** escapes as a non-`TaskExecutionError` (section 1). Fix in horus-ssh, not here. Low.
7. **A release races an in-flight submit.** `put_file` recreates `spool/` and `run/` in the released dir, and the step is orphaned (it lands in risk 2's path). Low: the lease manager does not release while a member is starting.
8. **`.horus_alloc` growth** (section 4). Low.

Tests to add. Unit tests in `tests/unit/test_shared_allocation.py` use `FakeInner`, which runs `mv`, `rm` and `touch` for real and cans the Slurm commands (`_shared`, 53-67):

- mv ambiguity (risk 1). Use a `FakeInner` whose `mv ` performs the move and then returns a `FakeProcess` with `returncode=None` (or -1):
  - The launcher is in `spool/`, so `launch` returns and does not raise.
  - The mv did not run and the check finds nothing, so `launch` raises and leaves no `.sh`.
  - The launcher is already in `run/<step>.sh`, so `launch` succeeds.
  - The check itself raises, so `launch` raises.
- Orphaned spool (risk 2). Launch, then make squeue return empty for the old id:
  - `poll` returns `None`, a second `sbatch` is sent, the launcher is in `<key>-2/spool/`, and `extra` has the new `job_id` and `alloc_dir`.
  - Two members polling at once lead to one sbatch.
  - A claimed launcher (moved to `run/`) still fails with 1.
  - A cancel after the move writes `.cancel` in the new dir.
  - The move cap is hit, so the member fails.
- Validators (risk 3):
  - `\n` and `\r` in each of `partition`, `account`, `qos`, `mem`, `time_limit` and `gres`, and in one item of `extra_sbatch_args`, raise `ValidationError`.
  - A task with `walltime="1:00:00\n#SBATCH --x"` raises at script build.
  - Valid values still round-trip through `model_dump_json`/`model_validate`, because tc-os stores workflows as JSON.
- Cleanup (section 4). Use dirs with old `heartbeat` mtimes (`os.utime`):
  - Old dirs are removed.
  - The current dir and fresh dirs are kept.
  - The newest `pilot.log` per target is kept.
  - A symlink under `.horus_alloc` is not followed.
  - A failing cleanup command does not fail `_allocation`.
- Bash pilot, extending `test_the_pilot_runs_steps_and_ends_when_orphaned` (372-393): the pilot script contains `--no-requeue`, and a launcher dropped after the pilot exits stays in `spool/`. That is the input the requeue path expects.
