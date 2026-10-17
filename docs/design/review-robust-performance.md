# Review: slurm-robust WP14, performance

Branch `chdominguez/slurm-robust-wp14` against `docs/design/slurm-robust.md`
(incl. User decisions) and the F1 exit-code commit. Scope: login-node commands
per launch/allocation and per poll. Suite: 206 passed.

Counts: High 0, Medium 1, Low 1, Info 1.

## P1. Medium: cleanup `rm -rf` fails for good once the list passes ~128 KB

- Where: `src/horus_slurm/target/slurm.py:776-778` (`_clean_alloc_dirs`).
- Problem: all old paths go into one command string. Local runs it with
  `create_subprocess_shell` (`/bin/sh -c <cmd>`), SSH with `$SHELL -c <cmd>`;
  both pass it as one argv string, which Linux caps at 128 KB
  (MAX_ARG_STRLEN). Above that the exec fails with E2BIG, the error is logged
  at debug, and nothing is removed. The next process builds the same, longer
  list and fails again, so the tree never shrinks again and nobody sees it.
  At ~100 bytes per quoted path the limit is ~1300 dirs (one dir per process
  run per target), reachable after a long gap or with sweeps that start many
  processes.
- Verified (local): 2000 dirs at 100 B each (200 KB command) ->
  `OSError [Errno 7] Argument list too long: '/bin/sh'`, 2000 left; 120 KB
  works. SSH path suspected, same kernel limit on the remote exec.
- Fix: cap the work per process. Sort `old` oldest first and remove at most
  500 per call (`old = sorted(old)[:500]` on `(mtime, path)`), or chunk by
  a ~64 KB byte budget and run one `rm -rf` per chunk. The cap also bounds
  P2's lock time. Add a test with a long list.

## P2. Low: cleanup runs inside the allocation lock, before `sbatch`

- Where: `src/horus_slurm/target/slurm.py:686` (call in `_allocation`).
- Problem: `find` + `rm -rf` run under `handle.lock` and before the pilot is
  submitted. Every member waiting for its first launch, and `release`, waits
  for two extra round trips plus the `rm`. On Lustre/GPFS each heartbeat
  stat and each unlink is a metadata RPC, so a few hundred old dirs can add
  seconds. Once per process (as the design accepts), but on the critical path
  of the first launch.
- Suspected (not measured on a cluster; `find` over 3000 dirs on tmpfs took
  0.045 s).
- Fix: call it after `_sbatch` returns, ideally outside the lock in the
  `if submitted:` block before the callbacks. The pilot is queued by then, so
  the cost hides behind the queue wait. The root is still known to be ours
  from `_make_private_dir`.

## I1. Info: checked, no cost added

- `_spool_step` extra check (`slurm.py:876-886`): runs only when `mv` fails;
  the success path keeps the same three commands.
- F1 launcher line (`slurm.py:865`): one `[ -e ]` on the compute node at step
  end; no login-node command.
- `poll` is not touched by this branch; nothing new runs per poll.
- `sbatch` header checks and field validators: in-process regex only.
- `find` predicates: GNU find (default `-O1`) tests `-name` before `-type`,
  and `-maxdepth 2` does not enter `spool/` or `run/`.
- Side note: `_CLEANED_ROOTS` means a long-lived process cleans a root once
  ever; the backlog that P1 hits then grows until restart. Re-running after a
  day per root would fix it if such processes exist.
