# slurm-robust: design

Team 2 of [slurm-robust-plan.md](slurm-robust-plan.md). Facts come from
[slurm-robust-codebase.md](slurm-robust-codebase.md) (cited as "codebase
§N") and [slurm-robust-competitors.md](slurm-robust-competitors.md) (cited as
"prior art"). Line numbers are for `src/horus_slurm/target/slurm.py` at
`116c7dc`.

Branch: `chdominguez/slurm-robust`, from `feat/shared-allocation`. WP1-WP4
do not touch `poll`. WP5 does, in one line, and is rebased onto
`chdominguez/slurm-load` before it lands.

## 1. Ambiguous `mv` after `put_file`

### Problem

`_submit_step` writes `spool/<step>.tmp`, then renames it to
`spool/<step>.sh` with `mv` (764-773). Over SSH, a connection drop after the
channel opened returns `returncode None`, which `wait()` maps to `-1`
(codebase §1). The `mv` may have run. Today this raises, the pilot may run the
step anyway, and a task retry runs it a second time with a new step name.

### Design

On any non-zero `mv` result, run one check before raising:

```sh
[ -e spool/<step>.sh ] || [ -e run/<step>.sh ] || { rm -f spool/<step>.tmp; exit 1; }
```

(paths are `{alloc_dir}/...`, each `shlex.quote`d)

| Check result | Meaning | Action |
|---|---|---|
| 0 | The launcher landed (still spooled, or already claimed) | Return `(job_id, alloc_dir)` as on success |
| 1 | It did not land; the `.tmp` is removed | Raise as today |
| anything else, or the check raises | Unknown | Raise as today |

- The step name is minted by horus per launch, so it works as the
  idempotency key (prior art, recommendation 2). No content dedup.
- Check `run/<step>.sh`, not `run/<step>.job`. The `.job` is written before
  the `mv` and is always there.
- A check that is itself ambiguous still raises. The duplicate window is then
  two commands wide instead of one. The stronger variant (take the launcher
  back with `rm spool/<step>.sh` first) is not used: it turns a step that
  landed into a failure, and the plan asks for an idempotent success.
- `LocalTarget` never gives `-1`. It takes the same path, which keeps one
  code path and makes it testable with `FakeInner`.

### Refactor that WP5 reuses

Split `_submit_step` in two:

- `_submit_step(step, script, job_dir) -> tuple[str, str]`: `handle = await
  self._allocation()`, then `await self._spool_step(handle, step, script,
  job_dir)`, then return as today. Signature and return value do not change.
- `_spool_step(handle: _Allocation, step, script, job_dir) -> None`: the
  current body after line 719 (resources, launcher, both `put_file`, `mv`)
  plus the check above. It reads every path from `handle.alloc_dir`, so it
  builds a correct launcher for any allocation dir.

## 2. Spooled launchers orphaned by a dead allocation

### Problem

When the allocation leaves the queue before the pilot claims a launcher
(cancelled while pending, idle exit race, a liveness `squeue` that failed),
the member's `poll` sees `state == ""`. It waits 25 s in `_retry_exit_code`
and fails with GONE or the sacct state (codebase §2). The step never ran.

### Rule

Parsl HTEX split (prior art, recommendation 3):

- **Unclaimed** (`spool/<step>.sh` still there): move it to a live
  allocation. The member keeps waiting.
- **Claimed** (`run/<step>.sh`): fail as today.

### Design: `_requeue_step(handle: JobHandle) -> bool`

Owned by Robustness. Called from one line in `poll` (contract in §6). It
returns `True` when the step was moved and the member should keep waiting,
`False` when the caller should go on with today's failure path.

1. **Guard.** Return `False` unless `extra` has `step`, `self._cancelling` is
   false, `self._record` is set, and `int(extra.get("moves", "0")) <
   MAX_STEP_MOVES`.
2. **Take the launcher back.** `rm {old_alloc}/spool/{step}.sh` (no `-f`).
   Exit 0: it was unclaimed and is ours now. Any other result, `-1`
   included: return `False`. This is the rule `_signal_step` uses
   (1146-1148). An ambiguous `rm` cannot duplicate: either the launcher is
   gone, or the pilot has it.
3. **Get a live allocation.** `alloc = await self._allocation()`. Members
   that see the death at the same time are serialised on `handle.lock`. The
   first one submits, and the others get the new id (unit test below).
4. **Rebuild the launcher, do not move the file.** `await
   self._spool_step(alloc, step, self._record.script, handle.job_dir)`. The
   old launcher hardcodes the old dir's `.job` and `.cancel` paths (738-753).
   The step name and `job_dir` stay the same, so the `exit_code` path does
   not change. The old `run/<step>.job` is left for cleanup (§4).
5. **Update the handle in place.** `JobHandle` is frozen, but `extra` is a
   plain dict, and `PollingChannelProcess` keeps the same object (codebase
   §2). Set `job_id`, `raw_job_id`, `alloc_dir` and `moves` (as `str`, like
   the other values).
6. **Update the member.** `_record` gets the new `job_id` and `script_path`
   (`{alloc_dir}/run/{step}.job`) through `model_copy`. Call
   `_publish_job_id(new_id, cwd=self._record.working_dir)`. Set
   `_last_state = None`, then `_announce(new_id, SUBMITTED)`, like `launch`
   does. Reset `_partition_checked`/`_partition_nodes` only if they are still
   per member after the Load branch.
7. **Cancel during the move.** `send_signal` can run at any `await` between
   steps 2 and 5. It then touches `.cancel` in the old dir, its `rm` of the
   old spool file fails, and it only sets `_cancelling`. So after step 6, if
   `self._cancelling` is set, call `self._signal_step(handle, new_id, step,
   signal.SIGTERM)` again. That touches the new `.cancel`, takes the new
   launcher back and writes 143. The next poll reads it.
8. **Errors.** If step 3 or 4 raises, log at `error` with the step name and
   return `False`. The member then fails through today's path, as it would
   without this change. `poll` itself never raises from here.

`MAX_STEP_MOVES = 2`, a module constant next to `HEARTBEAT_STALE_S`. It stops
a step from bouncing between pilots that keep dying before they claim (a
partition that sbatch accepts but never starts, a pilot crash). HyperQueue
and Dask bound retries the same way (prior art, (b)).

Why this cannot start an allocation after a release: the lease manager
skips a lease while a member is running (`lease.running`, `lease.py:361`),
and a member that polls is running. `abort` cancels members first, so `_cancelling` is
set.

`generation` needs nothing: `lease.member_finished` reads it from the shared
handle when the member ends (codebase §2).

## 3. `#SBATCH` injection

### How sbatch reads the line

From prior art (`opt.c`): lines split on `\n` only. Tokens split on any
`isspace()` (space, tab, `\r`, `\v`, `\f`). `'` and `"` group, `\` escapes,
and an unquoted `#` starts a comment. So `\n` adds a directive or a shell
command, whitespace adds an option, and `#` or a quote truncates or breaks
the value. Rejecting only `\n`/`\r`, as the plan says, does not stop
`partition="debug --uid=..."`.

### Fields that reach a directive

| Value | Source | Inline jobs | Pilot | Rule | Where |
|---|---|---|---|---|---|
| `partition`, `account`, `qos` | target | yes, pre-existing | yes | strict | `field_validator` |
| `mem`, `time_limit`, `gres` | target | yes, pre-existing | yes | strict | `field_validator` |
| `--time` from task `resources.walltime` | task, via `to_slurm_time`, which passes unknown text through | yes, pre-existing | no (pilot uses target fields only, 691-698) | strict | `_sbatch_header` |
| `--mem` from `memory_gb`, `--gres` from `gpus` | task, numeric | yes | no | none needed (numbers) | covered by the header check anyway |
| `extra_sbatch_args` items | target | yes, pre-existing | yes | line | `field_validator` |
| `--output`/`--error` | `working_directory`, `side_artifacts_dir`, `job_dir` (inline); `{working_directory}/.horus_alloc/...` (pilot) | yes, pre-existing | yes | line | `_sbatch_header` |
| `--job-name` | job dir uuid, or `_ref` (`^[a-z][a-z0-9_-]{0,62}$`) | safe | safe | line | `_sbatch_header` |
| `--nodes`, `--ntasks`, `--cpus-per-task` | int | safe | safe | none | |

- **strict**: reject any whitespace, `#`, `'`, `"`, `\` and `\0`. None is
  valid in these values (prior art, recommendation 1).
- **line**: reject `\n`, `\r` and `\0`. `extra_sbatch_args` is free-form by
  design (Parsl and dask-jobqueue leave it to the user). Paths get the same
  rule. Spaces in paths are a separate correctness issue, not
  injection (codebase §3); a Team 4 case.

All of these except the pilot itself are pre-existing for inline targets:
`_sbatch_header` is shared (1204-1252).

### Design

- One module regex per rule: `_STRICT_BAD = re.compile(r"[\s#'\"\\\x00]")`
  and `_LINE_BAD = re.compile(r"[\n\r\x00]")`.
- `SlurmTarget` gets the first `field_validator`s in this package, in the
  `base.py` style (`@field_validator`, `@classmethod`, `ValueError` with a
  `_()` message naming the field). One for the six strict fields, one for
  `extra_sbatch_args`. They run when tc-os loads a stored workflow, so a bad
  value fails at load, not at submit.
- `_sbatch_header` checks what it renders, because task values and paths do
  not pass through a target validator: `resolved.mem`, `resolved.time_limit`
  and `resolved.gres` with the strict rule, `job_name`, `stdout_path` and
  `stderr_path` with the line rule. On a match it raises
  `TaskExecutionError` naming the option. The target fields are not checked
  again here.
- No runtime change: `ResourceRequest` stays unvalidated.
- Not in scope: `export {key}=` with an unquoted key in `_script_body`
  (1265). Keys come from the runtime (codebase §3).

## 4. Cleanup of old `.horus_alloc` dirs

### Problem

Every generation of every run makes `<wd>/.horus_alloc/<lease_key>-<gen>/`,
and nothing removes it. `release` removes only `spool/` and `run/` (codebase
§4). `lease_key` is a new 12-char hex uuid per declared target per process
(`base.py:141-150`), so the name cannot tell which target a dir belongs to.

### Design

- **Name the target in the dir:** `alloc_dir =
  f"{wd}/{ALLOC_DIR}/{self._ref}.{self.lease_key}-{handle.generation}"`.
  `_ref` cannot contain `.`, and neither can the key, so the name splits
  without ambiguity. The pattern a cleanable dir must match:
  `^([a-z][a-z0-9_-]{0,62})\.[0-9a-f]{12}-[0-9]+$`. Names that do not match
  are never touched.
- **Age** is the `heartbeat` mtime, not the dir mtime. Keepalive touches the
  heartbeat while the allocation is in use, so a live allocation of another
  run in the same working dir is never old (codebase §4).
- **Keep the newest dir of each target** (by heartbeat mtime), whole. It
  holds the newest `pilot.log`. Keeping the dir rather than the file alone is
  one rule instead of two, and the dir is small after `release`.
- **Never remove** the current `alloc_dir`.
- **Commands** (two round trips):
  1. `find {root} -mindepth 2 -maxdepth 2 -type f -name heartbeat -printf '%T@ %h\n'`.
     `find` does not follow symlinks without `-L`. GNU `find`, like the
     `stat -c` the pilot already uses.
  2. Python groups the lines by `_ref`, drops the newest per target, the
     current dir, names that do not match and dirs newer than
     `ALLOC_DIR_MAX_AGE_S`, then runs one `rm -rf` with each path quoted.
     Nothing to remove: no second command.
- **Where:** a new `_clean_alloc_dirs(root, keep)` called from
  `_allocation`'s submit branch, right after `_make_private_dir` succeeds,
  so the root is known to be ours and not a symlink. Once per process per
  root (a module-level `set` of cleaned roots). It adds two round trips to
  the lock hold once per process, not per generation.
- **Best effort:** any error or non-zero exit is logged at debug and
  ignored. `_allocation` never fails because of cleanup.
- `ALLOC_DIR_MAX_AGE_S = 7 * 86400`, a module constant (Q1).
- Dirs without a `heartbeat` (a `mkdir` chain that failed half way) are not
  found and stay. Rare; left as is.
- Dirs in the old `<key>-<gen>` format do not match and stay (Q3).

## 5. Work packages

All in `src/horus_slurm/target/slurm.py` unless stated. WP1-WP4 can land in
any order. They touch different functions; WP3 and WP2 do not overlap.

| WP | Scope | Functions touched | Tests | Depends on |
|---|---|---|---|---|
| WP1 | Header injection (§3) | `SlurmTarget` validators (new), `_sbatch_header` | `tests/unit/test_slurm_target.py` | none |
| WP2 | Ambiguous `mv` (§1), split out `_spool_step` | `_submit_step`, new `_spool_step` | `tests/unit/test_shared_allocation.py` | none |
| WP3 | Cleanup (§4) | new `_clean_alloc_dirs`; `_allocation` submit branch (dir name, one call) | `tests/unit/test_shared_allocation.py` | none |
| WP4 | Bash test of the requeue-free pilot path | none (test only) | `tests/unit/test_shared_allocation.py` | none |
| WP5 | Orphan move (§2) | new `_requeue_step`, `MAX_STEP_MOVES`; one line in `poll` | `tests/unit/test_shared_allocation.py` | WP2, `chdominguez/slurm-load` |

Ownership notes:

- WP3 edits `_allocation`, which the plan does not give to either team in
  full (Load owns its liveness check, 623-631). WP3 changes only lines
  634-637 and adds one call after 638. Expect a small rebase conflict next to
  Load's edit; resolve it by keeping both.
- WP5 is the only change to `poll`. It is written on top of the Load branch
  so its line sits next to Load's compare-and-clear.

## 6. Interface contract

### Robustness provides

```python
async def _spool_step(
    self, handle: _Allocation, step: str, script: str, job_dir: str
) -> None:
    """Write the step script and launcher into handle.alloc_dir/spool."""
    # Raises TaskExecutionError when the launcher did not land.

async def _requeue_step(self, handle: JobHandle) -> bool:
    """Move a never-claimed step into a live allocation."""
    # True: moved, handle.extra updated in place; poll returns None.
    # False: not moved (claimed, cancelled, cap reached, not shared, error).
    # Never raises.
```

### WP5 needs from the Load branch

1. **The call site in `poll`**, in the `state == ""` branch, not cancelling,
   after the compare-and-clear of `handle.job_id` and before
   `_retry_exit_code`:
   ```python
   if await self._requeue_step(handle):
       return None
   ```
2. **Compare-and-clear first.** `poll` clears the shared `job_id` only when it
   equals the polled id, under `handle.lock` (Load codebase notes, risk 2).
   Then the `_allocation()` call in step 3 submits once, and later members
   reuse the new allocation.
3. **The cache is keyed by job id.** A cached `""` for allocation N is never
   read for N+1 (Load risk 1). `_allocation()` treats a fresh cached `""` for
   `handle.job_id` as gone and submits. It never treats it as alive.
4. **`_requeue_step` does not read or write the cache.** It changes
   `handle.extra["job_id"]`, so the member's next poll asks about the new id.

If the Load branch changes any of 1-3, WP5 adapts to it; the Robustness side
of the contract does not change.

## 7. Test plan

Unit tests use `FakeInner` and `_shared` (`test_shared_allocation.py:53-67`),
which run `mv`, `rm` and `touch` for real and fake the Slurm commands.

**WP1** (`test_slurm_target.py`):
- Each strict field with `"a b"`, `"a\tb"`, `"a\nb"`, `"a\rb"`, `"a#b"`,
  `"a'b"`, `'a"b'`, `"a\\b"`, `"a\x00b"` raises `ValidationError`.
- One `extra_sbatch_args` item with `\n`, `\r`, `\0` raises. `"--mail-type=END
  --exclusive"` and `"--comment='x y'"` are accepted.
- Valid values (`"gpu:a100:1"`, `"1-00:00:00"`, `"4G"`, `"a,b"`) round-trip
  through `model_dump_json` / `model_validate`.
- A task with `walltime="1:00:00\n#SBATCH --x"` or `"1:00:00 --x"` raises
  `TaskExecutionError` from `_build_sbatch_script`.
- A `working_directory` with `\n` raises from `_build_sbatch_script` and from
  `_pilot_script`.

**WP2** (`FakeInner` whose `mv ` runs the move or not, then returns
`returncode=None`/`-1`):
- The `mv` ran: `launch` returns, and `spool/<step>.sh` exists.
- The `mv` did not run: `launch` raises, and no `.sh` or `.tmp` is left.
- The launcher is already in `run/<step>.sh`: `launch` returns.
- The check itself raises: `launch` raises.
- Existing shared-allocation tests pass unchanged (the split is pure).

**WP3** (dirs with old heartbeat mtimes via `os.utime`):
- Old dirs of a target are removed. The newest dir of each target, the
  current dir and fresh dirs are kept.
- Two targets: each keeps its own newest dir.
- A name that does not match the pattern, and a symlink under
  `.horus_alloc`, are left alone (the symlink target is untouched).
- A failing `find` or `rm` does not fail `_allocation`.
- A second `_allocation` in the same process does not run `find` again.
- The new dir name is `<ref>.<lease_key>-<gen>` (update existing asserts).

**WP4** (extends `test_the_pilot_runs_steps_and_ends_when_orphaned`,
372-393):
- The pilot script has `--no-requeue`.
- A launcher dropped into `spool/` after the pilot exited stays there. This
  is the input WP5 expects.

**WP5** (launch, then squeue answers `""` for the old id):
- `poll` returns `None`. A second `sbatch` ran. The launcher is in the new
  dir's `spool/` and points at the new dir's `.job` and `.cancel`. `extra`
  has the new `job_id`, `raw_job_id`, `alloc_dir`, and `moves == "1"`.
  `_record.job_id` is the new id. No 25 s wait (`exit_code_retries` not
  used).
- Two members poll at once: one `sbatch`, both moved.
- A claimed launcher (moved to `run/`): `poll` fails with 1, as today.
- A cancel between take-back and re-spool: `exit_code` 143, and the new
  launcher is gone.
- A cancel after the move: `.cancel` lands in the new dir.
- `moves` at `MAX_STEP_MOVES`: the member fails as today.
- `sbatch` fails during the move: `poll` does not raise; the member fails as
  today.
- Inline target: `_requeue_step` returns `False` and runs no command.

Every WP runs `uv run ruff check`, `uv run mypy src` and `uv run pytest -q -p
no:cacheprovider`.

## 8. Deviations from the plan

1. **Header:** the plan rejects `\n`/`\r` in seven target fields. This
   design rejects all whitespace, `#`, quotes, `\` and `\0` in six structured
   fields, and only `\n`/`\r`/`\0` in `extra_sbatch_args`. It also checks the
   task's `walltime` and the log paths in `_sbatch_header`, which the plan
   did not list (codebase §3, prior art).
2. **Orphan move:** the launcher is rebuilt for the new dir, not moved
   (the old one hardcodes old paths). A move cap is added. The `poll` call
   site is a separate WP on top of the Load branch.
3. **Cleanup:** the dir name changes to `<ref>.<lease_key>-<gen>` so dirs
   can be grouped by target. The newest dir of each target is kept whole, not
   only its `pilot.log`. Age uses the heartbeat mtime. It runs once per
   process.
4. **Ambiguous `mv`:** a not-landed result also removes the `.tmp`.

Not done here (codebase §5), reported for other teams:

- Ambiguous `sbatch` over SSH leaves an untracked pilot for up to 15 min
  (risk 4). Could be a later WP.
- `scancel` on a finished, not yet purged job may not say "invalid job id",
  and `release` then raises (risk 5). For Team 4 case 3.
- Raw `asyncssh.SFTPError` from `put_file` (risk 6). A horus-ssh fix.
- Spaces in `--output` paths. A Team 4 case.

## 9. Open questions

- **Q1. Max age of old allocation dirs: constant or a target field?**
  Recommended: a module constant, 7 days, like `HEARTBEAT_STALE_S`. Add a
  field only if someone asks.
- **Q2. How many times may one step move?** Recommended: 2
  (`MAX_STEP_MOVES`). One is enough for an allocation cancelled while
  pending; the second covers a rare idle-exit race. More than that hides a
  broken partition.
- **Q3. Clean the old `<key>-<gen>` dirs too?** Recommended: no. The
  shared target is not released yet (PR #15 is open), so only dev runs have
  them, and they cannot be grouped by target.

## User decisions

- Target settings are editable from the canvas (user, 2026-10-17: "implement the target settings so are editable"), so the Q1/Q2 constants become SlurmTarget fields with the recommended defaults: `alloc_retention_days: int = Field(default=7, ge=1, le=365, title=_("Allocation folder retention (days)"))` and `max_step_moves: int = Field(default=2, ge=0, le=10, title=_("Task moves to a new allocation"))`. They show in the schema-driven form with no frontend change.
- Q3: accepted as recommended (legacy `<key>-<gen>` dirs are not cleaned).
