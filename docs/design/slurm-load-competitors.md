# Slurm polling load: prior art

How other workflow tools keep scheduler queries off the Slurm controller, and
what that means for shared targets (many srun steps inside one pilot
allocation). Input for Team 1 in `slurm-load-plan.md`.

## Why it matters

- Every `squeue` is an RPC to slurmctld. The man page says not to run it
  from loops in scripts or programs, because enough calls at once can
  degrade slurmctld "possibly resulting in a denial of service"
  ([squeue PERFORMANCE](https://slurm.schedmd.com/squeue.html#SECTION_PERFORMANCE)).
- `sacct` goes to slurmdbd instead of slurmctld; both man pages carry the same
  warning for their own daemon
  ([slurm-users, 2023](https://lists.schedmd.com/pipermail/slurm-users/2023-February/009724.html)).
- NERSC forbids `watch squeue`, and caps any watch at `watch -n 60`
  ([NERSC monitoring](https://docs.nersc.gov/jobs/monitoring/)). Alliance
  Canada asks users not to run squeue from a script "every few seconds"
  ([Alliance monitoring jobs](https://docs.alliancecan.ca/wiki/Monitoring_jobs)).
- The ARCHER2 scheduler docs do not give a polling limit
  ([ARCHER2 scheduler](https://docs.archer2.ac.uk/user-guide/scheduler/)).
  I found no LUMI page on it either. Treat NERSC's 60 s as the strictest
  published number.
- Sites can rate-limit per user. With `SlurmctldParameters=rl_enable`, a token
  bucket (default size 30, refill 2 tokens per 1 s) applies to each user, and
  client commands are told to back off and sleep for a second once it runs
  dry ([slurm.conf](https://slurm.schedmd.com/slurm.conf.html#OPT_rl_enable)).
  A burst of status queries from one user then gets slower, not just counted.
- Admins see who is polling: `sdiag` reports RPC counts and time per user and
  per message type ([sdiag](https://slurm.schedmd.com/sdiag.html)).
- One query for many jobs is cheaper for slurmctld than many queries for one
  job each
  ([slurm-users, 2021](https://lists.schedmd.com/pipermail/slurm-users/2021-February/006832.html)).

## Comparison

| Tool | How it polls | Batching | Interval | Link |
|---|---|---|---|---|
| Nextflow | `squeue --noheader -o '%i %t' -t all -u $USER [-p queue]`, or `squeue --only-job-state` when `onlyJobState` is on (Slurm 24.05+) | One call per executor and queue, result cached and shared by all tasks (`Throttle.cache`) | `queueStatInterval` 1 min; `pollInterval` 5 s for checking the exit file; `exitReadTimeout` 270 s | [SlurmExecutor](https://github.com/nextflow-io/nextflow/blob/master/modules/nextflow/src/main/groovy/nextflow/executor/SlurmExecutor.groovy), [AbstractGridExecutor](https://github.com/nextflow-io/nextflow/blob/master/modules/nextflow/src/main/groovy/nextflow/executor/AbstractGridExecutor.groovy), [executor config](https://docs.seqera.io/nextflow/reference/config/executor) |
| Snakemake slurm plugin | `sacct -X --parsable2 --clusters all --noheader --format=JobIdRaw,State --starttime <now-2d> --endtime now --name <run_uuid>`; `squeue --name <run_uuid>` only when MinJobAge ≥ 120 s | One call per run: every job gets the run UUID as its job name | First check after 40 s, then +10 s per round with no finished job, capped at 180 s; `--max-status-checks-per-second`; 5 attempts | [plugin docs](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html), [job_status_query.py](https://github.com/snakemake/snakemake-executor-plugin-slurm/blob/main/snakemake_executor_plugin_slurm/job_status_query.py), [`__init__.py`](https://github.com/snakemake/snakemake-executor-plugin-slurm/blob/main/snakemake_executor_plugin_slurm/__init__.py) |
| Parsl SlurmProvider | `sacct -X --noheader --format=jobid,state%20 --job '<ids>'`, falling back to `squeue --job` if `sacct` fails | Active job ids joined with commas, `status_batch_size` 50 per call | `status_polling_interval` 60 s | [slurm.py](https://github.com/Parsl/parsl/blob/master/parsl/providers/slurm/slurm.py) |
| dask-jobqueue | Does not poll for tasks; workers start inside the batch job and connect back to the scheduler | Tasks run over the worker connection, not as Slurm jobs | Workers exit 60 s after losing the scheduler | [how it works](https://jobqueue.dask.org/en/latest/clusters-howitworks.html) |
| HyperQueue | Pilot allocations; one worker per node connects to the server, which hands out tasks | Tasks never reach Slurm; `backlog` caps queued allocations, plus an internal rate limit | Not documented; workers stop after 5 min idle | [autoalloc](https://it4innovations.github.io/hyperqueue/stable/deployment/allocation/) |
| Cromwell (HPC backends) | Waits for an `rc` file; `check-alive` (e.g. `squeue -j ${job_id}`) runs per job only if `exit-code-timeout-seconds` is set | None, one call per job | Every `exit-code-timeout-seconds` (example 120); docs warn this "could cause high load" | [SLURM backend](https://cromwell.readthedocs.io/en/stable/backends/SLURM/), [HPC exit code](https://cromwell.readthedocs.io/en/stable/backends/HPC/) |

Patterns that repeat:

1. **Done-ness comes from a file, not from Slurm.** Nextflow (`.exitcode`,
   checked every 5 s) and Cromwell (`rc`) only ask the scheduler whether the
   job is still alive. horus-slurm already does this with the marker file.
2. **One query per user/run, shared by all tasks.** Nextflow caches one
   `squeue -u` per queue; Snakemake makes one `sacct --name` per run; Parsl
   batches 50 ids. Nobody queries per task.
3. **Liveness at about 1 min.** Nextflow 60 s, Parsl 60 s, Snakemake 40 to
   180 s, NERSC's watch limit 60 s.
4. **Pilot tools skip the scheduler for tasks.** dask-jobqueue and HyperQueue
   only talk to Slurm about the allocation, not about the work inside it.

## Recommendation for one allocation with many steps

- **Liveness `squeue` once per allocation, not per member.** Cache the
  `QueueState` on the shared `_Allocation` handle with a timestamp; members
  read the cache and the first one past the interval refreshes it (the Team 1
  plan). This is the Nextflow `Throttle.cache` pattern with the allocation as
  the key.
- **Interval: 30 to 60 s for the allocation query.** 60 s matches Nextflow,
  Parsl and NERSC. Today `SlurmTarget.poll_interval` is 5 s
  (`src/horus_slurm/target/slurm.py:263`). That is fine for reading the
  marker file, but too fast for `squeue` once it is shared. Keep the two
  intervals separate, like Nextflow's `pollInterval` and `queueStatInterval`.
- **Step completion from marker files only.** Members should not call
  `squeue -s` or `sacct` for their own step while the allocation is RUNNING.
  Only ask `sacct` after the allocation or step is gone, to learn why (one
  call per failure, as Snakemake does for reasons).
- **Keep the query small.** `squeue -h -j <id> -o "%T|%R|%P"` asks for one job.
  On Slurm 24.05+ `--only-job-state` is cheaper still
  ([squeue](https://slurm.schedmd.com/squeue.html#OPT_only-job-state)), but
  it only returns id and state, so the pending reason needs the full form.
  Use it only behind a version check, as Nextflow does with `onlyJobState`.
- **`sinfo` on the handle too, and only while PENDING.** Once the allocation
  runs, partition node counts no longer matter.
- **Back off on failures** rather than retrying at once. A rate-limited user
  is already told to sleep for a second per call
  ([slurm.conf rl_enable](https://slurm.schedmd.com/slurm.conf.html#OPT_rl_enable)).

## Pitfalls

- **Jobs drop out of `squeue` after MinJobAge** (default 300 s)
  ([slurm.conf MinJobAge](https://slurm.schedmd.com/slurm.conf.html#OPT_MinJobAge)).
  An empty `squeue` means "gone", not "failed". Snakemake only trusts
  `squeue` when MinJobAge ≥ 120 s and otherwise uses `sacct`
  ([job_status_query.py](https://github.com/snakemake/snakemake-executor-plugin-slurm/blob/main/snakemake_executor_plugin_slurm/job_status_query.py)).
  With a 60 s cache, a job that ends just after a refresh is still visible on
  the next one, as long as MinJobAge > interval.
- **A failed query is not a finished job.** Nextflow treats a `null` queue
  status as still active
  ([AbstractGridExecutor](https://github.com/nextflow-io/nextflow/blob/master/modules/nextflow/src/main/groovy/nextflow/executor/AbstractGridExecutor.groovy));
  Parsl leaves states unchanged when a batch fails
  ([slurm.py](https://github.com/Parsl/parsl/blob/master/parsl/providers/slurm/slurm.py)).
  The shared cache must not store a failed read as `state == ""`, or one
  bad call would fail every member at once.
- **The exit file can lag.** NFS delays are why Nextflow waits up to 270 s
  (`exitReadTimeout`) after the job leaves the queue
  ([executor config](https://docs.seqera.io/nextflow/reference/config/executor)).
  Keep `exit_code_retries` when the allocation goes away.
- **A thundering herd on refresh.** If 50 members wake together and all see
  a stale cache, all 50 query. The refresh needs a lock or a single in-flight
  task that the others await.
- **Per-job `check-alive` scales with task count.** Cromwell's docs say so
  directly ([HPC](https://cromwell.readthedocs.io/en/stable/backends/HPC/));
  that is the current horus-slurm behaviour with shared targets.
- **`sacct` is not free.** It moves load to slurmdbd, and `sacct` without
  `--starttime` scans from midnight
  ([sacct](https://slurm.schedmd.com/sacct.html#OPT_starttime)); Snakemake bounds it to 2 days
  ([job_status_query.py](https://github.com/snakemake/snakemake-executor-plugin-slurm/blob/main/snakemake_executor_plugin_slurm/job_status_query.py)).
- **Bulk `scancel` hurts too.** NERSC warns that many `scancel` calls at once
  degrade service ([NERSC monitoring](https://docs.nersc.gov/jobs/monitoring/));
  Nextflow cancels in batches of 100 (`killBatchSize`)
  ([executor config](https://docs.seqera.io/nextflow/reference/config/executor)).
  Cancelling a shared target should be one `scancel` of the allocation, not
  one per step.
