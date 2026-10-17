# slurm-robust WP1-4 + F1: correctness review

Branch `chdominguez/slurm-robust-wp14`, diff `chdominguez/slurm-robust...HEAD`
(5 commits, `src/horus_slurm/target/slurm.py` + tests). Checked against
`docs/design/slurm-robust.md` and its User decisions. Suite: `206 passed`,
coverage 95.4%.

No blocker or high findings (2 medium, 9 low, 1 info). The focus areas
hold up:

- **Validators.** Run against a real `SlurmTarget`, these load fine:
  `partition="a,b"`, `gres="gpu:a100:2"` and `"gpu:a100:2,shard:1"`,
  `mem="4000M"`, `time_limit="1-00:00:00"` and `"2h"`, `qos="high-prio_x"`,
  `account="proj-1.2"`, `extra_sbatch_args=["--constraint=a b"]`. These are
  rejected: `"gpu "`, `"4 GB"`, and an extra arg containing `\n`. Validation
  runs once per field and does not depend on how the target is declared, so
  inline and named targets behave the same. (Verified.)
- **Ambiguous mv.** `ChannelProcess.wait()` returns an int, and a dropped
  SSH connection maps to `-1`. Any non-zero result runs the check. Exit 0
  returns success, which the design asks for. Any other result, or a raise,
  raises the original error. The step name comes from `new_job_dir`, which
  makes a new one for each launch, so a leftover `run/<step>.sh` cannot make
  a failed mv look like it landed. The check looks at `run/<step>.sh`, the
  name the pilot's `mv "$f" run/` produces, not the `.job` file. (Verified
  by reading the code.)
- **`_clean_alloc_dirs`.** It keeps the new dir and the newest dir for each
  ref, whatever their age. Legacy `<key>-<gen>` dirs and anything else that
  fails `_ALLOC_NAME`, which is `TARGET_NAME_PATTERN` plus the lease key,
  are never touched. Dirs with no heartbeat are never listed. Age is the
  heartbeat mtime, which `keepalive` touches while `job_id` is set, queued
  jobs included. On failure it logs at debug level. (Verified by reading the
  code.)
- **Dir rename.** `keepalive`, `release`, `_stop_step` and the handle's
  `extra["alloc_dir"]` all use the absolute `handle.alloc_dir`. Nothing
  rebuilds the path from `lease_key`, so handles that predate the rename
  keep working. (Verified with grep.)
- **143.** `[ -e cancel ] && code=143` runs after `wait $p` and before the
  `exit_code` write that skips an existing file, so it does not override
  the 143 that `_stop_step` writes for a step that is still spooled. Only
  SIGTERM and SIGKILL stops create the mark, so other signals keep srun's
  own code.

## Findings

| # | Sev | Where | Problem | Fix | Status |
|---|---|---|---|---|---|
| 1 | Medium | `slurm.py:316-332` | Validation runs on load, so one stray space from a canvas form (`partition: "gpu "`) makes the whole stored tc-os workflow fail to load, not just that run. sbatch accepted a trailing space before this change. | Add `mode="before"` and `.strip()` the value before the `_STRICT_BAD` check, or strip in the UI form. | Verified (`"gpu "` raises `ValidationError`); impact on tc-os suspected |
| 2 | Low | `slurm.py:1350-1356` | The header check also covers *task* values, and `to_slurm_time` passes `walltime` through untouched unless it has a unit suffix. A walltime of `"01:00:00 "` now fails at submit with `Invalid value for --time`, where sbatch accepted it before. | Strip in `to_slurm_time` (it already strips for suffixed values), or strip in `resolve_resources`. | Suspected (by reading) |
| 3 | Low | `slurm.py:1350-1352` | `--output`/`--error`/`--job-name` only reject line breaks. sbatch also splits `#SBATCH` lines on whitespace, so a working directory with a space still breaks `--output`, and `/a --partition=x/...` would add an option. The comment "free-form ones may hold anything but a new line" is wrong for these. This predates the branch and the paths come from config. | Reject whitespace in these values too, or quote them (`--output="..."`, which sbatch parses). | Suspected; predates the branch |
| 4 | Low | `slurm.py:761-771` | Age compares the cluster's mtime (`%T@`) with the orchestrator's `time.time()`, so clock skew between the two machines shifts the cutoff. | Compute the age on the cluster, e.g. emit `$(date +%s)` in the same command and subtract that, or use `find -mmin +N`. | Suspected; only matters with days of skew |
| 5 | Low | `slurm.py:739-786` | If an orchestrator crashes while a step runs or waits in srun for longer than the retention period, the dir is deleted under the live pilot. A queued step's `run/<step>.job` disappears before srun reads it, and `pilot.log` is lost. The design's rule "a live allocation is never old" assumes keepalive is running. | Accept and document it, or also skip dirs whose pilot job is still in `squeue` (`--name=horus-<ref>`). | Suspected |
| 6 | Low | `slurm.py:865` | `code=143` replaces any srun result once a cancel mark exists. That includes a clean 0, or a real failure such as OOM or a node failure, when the user cancels within the 1 s loop window just as the step ends. The real failure is then hidden as "cancelled". | Override only when the code is 1 or 137 (`case $code in 1\|137) code=143;; esac`), or accept it and document it. | By reading |
| 9 | Medium | `slurm.py:748-754` | `alloc_retention_days` is a per-target field (User decision), but cleaning runs once per root per process, using the retention of whichever target allocates first. A target with 1 day of retention that runs first deletes the idle dirs of a target set to 90 days in the same working directory. | Group by ref and apply each target's own retention only to dirs of its own `self._ref`, keyed `(root, ref)`. Leave other refs alone, or keep them for at least `le=365`. | By reading |
| 10 | Low | `slurm.py:748` | `_CLEANED_ROOTS` is keyed by the path only, so two clusters with the same `working_directory` in one process clean only the first. A long-lived server process also cleans each root only once. | Key by `(self.inner.location_id or host, root)`. For servers, store a timestamp and clean again after a day. | By reading |
| 11 | Low | `slurm.py:757-758` | `find` exits 1 on any unreadable entry, and that throws away all of its valid output, so one bad subdir turns cleaning off for good. | Parse stdout whatever the exit code; just log the stderr. | By reading |
| 12 | Low | `tests/unit/test_shared_allocation.py:540-543` | The "late launchers stay spooled" check is vacuous: it writes `late.sh` after the pilot has exited, then asserts that the file exists. | Run the pilot loop again (or for one more iteration) after writing it, or assert that the pilot exit happens before the write, then check that `run/late.sh` is missing. | Verified by reading the test |
| 7 | Low | slurm-itests `docs/design/slurm-itests.md:253` | Case 2 is `xfail(strict=True)` for F1. With this fix it will XPASS, which fails the run. | Remove the xfail when this lands. | By reading; itests not run |
| 8 | Info | `slurm.py:751-753` | The root goes into `_CLEANED_ROOTS` before the `find` runs, so a transient failure skips cleaning for the rest of the process. | Fine as best effort; add the root only after success if retries are wanted. | By reading |

Considered and dropped: keeping legacy dirs (that is User decision Q3), and
raising when the land check itself fails (the design requires it, and §1
says so). Cleaning runs under `handle.lock` before sbatch, which adds a
`find` and an `rm -rf` to the first submit. That is fine unless the root is
large; moving it after sbatch would be cheap.

Not checked: real Slurm or SSH behaviour (only `FakeInner` and reading the
code), BSD `find` (no `-printf`, so cleaning fails quietly and logs at debug
level).
