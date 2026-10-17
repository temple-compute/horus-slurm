# Security review: slurm-robust-wp14

Scope: `git diff chdominguez/slurm-robust...HEAD` (WP1 header injection, WP3
cleanup, the launch retry, and the F1 fix that writes 143 on a cancel mark),
checked against `slurm-robust.md` §3, §4 and User decisions. The suite passes
(206 passed, 95.4% coverage). Probes were throwaway tests run against the
real `_build_sbatch_script` / `_pilot_script` and then deleted.

Counts: High 0, Medium 2, Low 3, Info 3.

## Medium

### M1. Whitespace, `#`, quotes and `\` in paths still reach `#SBATCH`

- **Where:** `src/horus_slurm/target/slurm.py:1351-1353` (line rule on
  `--job-name`, `--output`, `--error`), rendered at 1364-1366.
- **Problem:** §3 itself says sbatch splits options on any whitespace and
  reads quotes, `\` and an unquoted `#`. Paths get only the line rule, so a
  space adds an option. The log paths hold the task id (`side_artifacts_dir`
  = `<wd>/<run scope>/<task.id>/.../side-artifacts`). `BaseTask.id` allows
  anything except a leading `@`. So a task author, whose `walltime` §3 treats
  as untrusted, can still add options. The same is true of the target's
  `working_directory`, through both inline jobs and the pilot.
- **Verified (ran it):** a task dir `a --qos=evil` renders
  `#SBATCH --output=/tmp/.../a --qos=evil/side-artifacts/slurm-stdout.log`.
  A dir `a"b#c\d` renders unchanged. `working_directory="/…/w --qos=evil"`
  reaches the pilot's `--output`/`--error`. How sbatch then splits the line
  comes from §3's reading of `opt.c`. It was not run against a real sbatch.
- **Fix:** check the three paths and the job name with `_STRICT_BAD`, not
  `_LINE_BAD`. Paths with spaces already break the job (§3 says so), so
  rejecting them costs nothing. Add a test with `"a --qos=x"` and `"a#b"` as
  the task dir. If paths with spaces must work, render
  `--output="<path>"` and reject `"` and `\` too. Check that against a real
  sbatch first.

### M2. The target validators can be bypassed by assignment

- **Where:** `slurm.py:316-342` (validators). `SlurmTarget.model_config` has
  no `validate_assignment`, and `_sbatch_header` (1348-1349) skips target
  fields on purpose.
- **Problem:** `target.partition = "debug --uid=0"` or
  `target.extra_sbatch_args = ["--x\n#SBATCH --y"]` after construction (an
  orchestrator that propagates settings, a canvas edit applied in place, or
  `model_construct`) renders straight into the header.
- **Verified (ran it):** both assignments produce
  `#SBATCH --partition=debug --uid=0` and two lines `#SBATCH --x` /
  `#SBATCH --y`.
- **Fix:** check what is rendered, not only what was loaded. In
  `_sbatch_header`, add `partition`/`account`/`qos` with `_STRICT_BAD` to the
  existing loop, plus a `_LINE_BAD` check on each `extra_sbatch_args` item.
  Three lines; the validators stay for early errors. Or set
  `validate_assignment=True` on `SlurmTarget`. That does not cover
  `model_construct`.

## Low

### L1. The cleanup trusts `<wd>/.horus_alloc` between two round trips

- **Where:** `slurm.py:752-779` (`find`, then a separate `rm -rf` of absolute
  paths). The root check in `_make_private_dir` (719-727) checks
  `.horus_alloc` but not `wd`.
- **Problem:** §4 says the root "is known to be ours" because
  `_make_private_dir` checked it. That holds only at that moment. On a
  shared FS where `wd` is group-writable without the sticky bit, another
  user can rename `.horus_alloc` and put a symlink in its place after the
  `find`. The `rm -rf <root>/<ref>.<key>-<n>` then resolves through the
  symlink. The damage is bounded: only names matching `_ALLOC_NAME` that
  `find` listed get removed, and the attacker cannot list the 0700 root to
  learn the keys. The same `wd` access already allows worse attacks that
  existed before this branch, such as reading the `.job` scripts horus writes
  by absolute path, `export`ed env included. So this is Low for the branch.
  The `wd` ownership gap should be tracked separately.
- **Suspected** (not run on a shared FS).
- **Fix:** do the removal in one shell, pinned to the checked dir:
  `cd -P -- {q(root)} && [ -O . ] && [ ! -L {q(root)} ] && rm -rf -- {names}`
  with the bare names (they match `_ALLOC_NAME`, so they hold no `/`). Better
  still, move the `find` into the same command and print `%f` instead of
  `%h`.

### L2. No `--` before paths in `find`/`rm`

- **Where:** `slurm.py:754`, `777`.
- **Problem:** `working_directory` is not required to be absolute. A
  relative one that starts with `-` is read as an option or predicate.
  `shlex.quote` does not prevent that.
- **Verified by reasoning:** both commands then fail, and the error is
  logged at debug. It fails safe today.
- **Fix:** `rm -rf -- …`. For `find`, the L1 `cd` form avoids the
  problem, or prefix `./` to relative roots.

### L3. Cleanup can remove the dir of a pilot that is still running

- **Where:** `slurm.py:740-786`. The pilot loop (`slurm.py:116-130`) exits on
  a stale heartbeat only when no step is running.
- **Problem:** if an orchestrator dies while a long step runs past
  `alloc_retention_days`, the next process removes that dir, including its
  `pilot.log` and `run/*.job`. Data loss, not escalation. The `exit_code`
  lives in `job_dir` and survives.
- **Suspected.**
- **Fix:** before removing a dir, read its `job_id` (if one is recorded) and
  skip dirs whose job `squeue` still lists. Or document that retention must
  exceed the partition's maximum time limit.

## Info

- **I1. Unicode and control characters: blocking is complete for the
  stated rules.** Ran it: `_STRICT_BAD` (Python `\s`, Unicode-aware) rejects
  `\v`, `\f`, `\x1c`-`\x1f`, `\x85`, `\xa0`, U+2028/2029 and U+3000. That is
  stricter than sbatch's byte-level `isspace()`. `_LINE_BAD` lets U+2028,
  U+2029, `\x85`, `\v` and `\f` through. None of these ends a line for
  sbatch or bash, so this is fine. `$`, backtick and `;` are inert in a
  `#SBATCH` comment line.
- **I2. Cleanup parsing fails safe.** `splitlines()` (`slurm.py:763`) also
  splits on `\v`, `\f`, `\x1c`-`\x1e`, `\x85` and U+2028/2029. A `wd` that
  contains one never matches `parent != root`, so cleanup silently never runs
  for it. Nothing wrong is removed. Symlinked entries under the root are not
  descended (`find -P`) and are never listed. `rm` removes a link, not its
  target. The tests cover this (`test_shared_allocation.py`).
- **I3. Cancel mark (F1 fix, `slurm.py:865-866`).** The path is quoted and
  lives in the 0700 `run/`, so nobody else can plant it. When the step was
  killed, 143 now wins, as F1 asks. If the job script itself wrote
  `exit_code`, the `[ -e exit ] ||` guard keeps that value. So a command that
  traps SIGTERM and exits 0 is still recorded as 0 even though it was
  cancelled. That is a correctness gap, not a security one. A cancel that
  lands after a clean exit cannot turn it into 143, because the job script
  wrote `exit_code` first.
