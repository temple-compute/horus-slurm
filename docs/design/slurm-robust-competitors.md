# slurm-robust: prior art

How other pilot-job systems handle the four Team 2 items in
[slurm-robust-plan.md](slurm-robust-plan.md), and what sbatch does with a
`#SBATCH` line. Source links are pinned to the commit read on 2026-10-17.
"Not found" means the docs and code read did not cover it, not that the
feature is absent.

## Comparison

| System | (a) Lost ack on submit | (b) Tasks on a dead worker/pilot | (c) Per-pilot scratch cleanup | (d) Scheduler directive input |
|---|---|---|---|---|
| Parsl HTEX | Task id is a client-side counter; no dedup on resend [P1] | Tasks already sent to the lost manager fail with `ManagerLost`; tasks still in the interchange queue are not touched [P2]. Retries are off by default and opt-in [P3] | Not found | `scheduler_options` is a raw string placed after the `#SBATCH` block; partition/account/qos are formatted in with no checks [P4][P5] |
| HyperQueue | Server assigns a job id per submit; no dedup documented [H1] | Running tasks are rescheduled to another worker; a crash counter fails the task at 5 (default). Known stops (server stop, worker time limit) do not count [H1][H2] | Keeps script, id and stdout/stderr per allocation in the server dir; cancels allocations on queue removal or server stop [H3] | Extra args are passed after `--` on the `sbatch` command line, not written into the script [H3] |
| RADICAL-Pilot | Not found | When a pilot goes final, the task manager marks its non-final tasks `FAILED` ("pilot died"); restartable tasks are resubmitted as new tasks with new ids [R1]. EnTK resubmits non-final tasks [R2] | `PilotDescription.cleanup` deletes the sandbox on termination, keeping only logs; default `False` [R3] | Not found |
| FireWorks | Duplicate detection by spec (`_dupefinder`): a duplicate copies the earlier launch output instead of running [F1] | Rocket pings hourly; `lpad detect_lostruns` marks runs with no ping for 4 h as FIZZLED, and `--rerun` also reruns them [F2] | Not found | Not covered |
| Balsam | Not found | Early termination goes to `RUN_TIMEOUT`, then "auto retry" to `RESTART_READY`; `RUN_ERROR` without a handler goes to `FAILED` [B1] | Not found | Not found |
| Dask (jobqueue) | Not found | Scheduler reroutes tasks of a dead worker; after `allowed-failures` deaths the task raises `KilledWorker`, which can blame the wrong task [D1] | Not found | `job_extra_directives` lines are added as given; `job_directives_skip` drops generated lines but not the extra ones [D2][D3] |

## How sbatch reads `#SBATCH` lines

From `src/sbatch/opt.c` [S1] and the sbatch man page [S2]:

- The script is split on `\n` only (`next_line`). Each line that starts with
  `#SBATCH` (or the old `#SLURM`) is tokenised.
- Tokens split on `isspace()` (space, tab, `\r`, `\v`, `\f`) unless quoted.
  `'` and `"` group, `\` escapes the next char, an unmatched quote is fatal.
- An unescaped `#` inside the line starts a comment; the rest is dropped.
- The first line that is neither blank nor a comment stops directive
  parsing. The man page says the same: "Once the first non-comment,
  non-whitespace line has been reached, no more #SBATCH directives will be
  processed." [S2]
- Shell syntax is not expanded in directives [S2].

So for `#SBATCH --partition={value}`:

- `\n` in the value ends the line. If the rest starts with `#SBATCH` it is a
  new directive; otherwise it ends directive parsing and runs as a shell
  command in the batch job.
- A space, tab or `\r` splits the value into a second option, for example
  `partition="debug --uid=..."`.
- A `#` truncates the value; a quote changes grouping or kills the submit.

## Recommendations

1. **(d) Header injection.** Rejecting only `\n`/`\r` stops new lines but not
   extra options inside the line. For `partition`, `account`, `qos`, `mem`,
   `time_limit`, `gres`, reject any whitespace, `#`, `'`, `"` and `\`
   (none of these are valid in those values). For `extra_sbatch_args`,
   which are free-form by design, reject `\n`, `\r` and `\0` and leave the
   rest to the user, as Parsl and dask-jobqueue do [P5][D2]. HyperQueue's
   approach of passing options on the sbatch command line [H3] would avoid
   the script parser, but it changes how the pilot is submitted; not needed
   for this PR.
2. **(a) Ambiguous `mv`.** The step name is chosen by horus, so a check for
   `spool/<step>.sh` or `run/<step>.sh` after a failed `mv` is safe: the name
   plays the role of an idempotency key, which none of the systems above
   expose for submit. Keep it to that check; FireWorks-style dedup by content
   [F1] would be more than this needs.
3. **(b) Dead allocation.** The plan's split (unclaimed in `spool/` moves to a
   new allocation, claimed in `run/` fails) is the Parsl HTEX split: queued
   tasks survive, tasks handed to the lost manager fail [P2]. HyperQueue and
   Dask also retry tasks that were running, but bound it with a counter
   [H2][D1]. If claimed steps should retry later, add a counter then, and do
   not count allocation time limits, as HyperQueue does [H2].
4. **(c) Cleanup.** Age-based removal of `.horus_alloc/<key>-<gen>` while
   keeping the newest `pilot.log` matches RADICAL-Pilot keeping logs when it
   deletes a sandbox [R3]. No system above prunes old pilot dirs on the next
   submit, so best effort and never failing the allocation is the right bar.

## Sources

- [P1] Parsl HTEX `submit`, task id from `_task_counter`: https://github.com/Parsl/parsl/blob/e58473bf4a3b6969500016fc17f2afd578e1a06b/parsl/executors/high_throughput/executor.py#L780-L785
- [P2] Parsl interchange `expire_bad_managers`: https://github.com/Parsl/parsl/blob/e58473bf4a3b6969500016fc17f2afd578e1a06b/parsl/executors/high_throughput/interchange.py#L575-L596
- [P3] Parsl retries: https://parsl.readthedocs.io/en/stable/userguide/workflows/exceptions.html
- [P4] Parsl Slurm template: https://github.com/Parsl/parsl/blob/e58473bf4a3b6969500016fc17f2afd578e1a06b/parsl/providers/slurm/template.py
- [P5] Parsl `SlurmProvider` building directives: https://github.com/Parsl/parsl/blob/e58473bf4a3b6969500016fc17f2afd578e1a06b/parsl/providers/slurm/slurm.py#L193-L209
- [H1] HyperQueue jobs and tasks: https://it4innovations.github.io/hyperqueue/stable/jobs/jobs/
- [H2] HyperQueue failure handling: https://it4innovations.github.io/hyperqueue/stable/jobs/failure/
- [H3] HyperQueue automatic allocation: https://it4innovations.github.io/hyperqueue/stable/deployment/allocation/
- [R1] RADICAL-Pilot `TaskManager._pilot_state_cb`: https://github.com/radical-cybertools/radical.pilot/blob/7c540731ff8978208e787f242539a46bd77dd465/src/radical/pilot/task_manager.py#L293-L345
- [R2] EnTK discussion on resubmission: https://github.com/radical-cybertools/radical.entk/discussions/521
- [R3] RADICAL-Pilot `PilotDescription.cleanup`: https://github.com/radical-cybertools/radical.pilot/blob/7c540731ff8978208e787f242539a46bd77dd465/src/radical/pilot/pilot_description.py#L150-L153
- [F1] FireWorks duplicates: https://materialsproject.github.io/fireworks/duplicates_tutorial.html
- [F2] FireWorks failures: https://materialsproject.github.io/fireworks/failures_tutorial.html
- [B1] Balsam job lifecycle: https://balsam.readthedocs.io/en/latest/user-guide/jobs/
- [D1] Dask distributed, KilledWorker: https://distributed.dask.org/en/stable/killed.html
- [D2] dask-jobqueue `HTCondorCluster` parameters: https://dask-jobqueue.readthedocs.io/en/stable/generated/dask_jobqueue.HTCondorCluster.html
- [D3] dask-jobqueue changelog (0.8.1): https://jobqueue.dask.org/en/latest/changelog.html
- [S1] Slurm `src/sbatch/opt.c` (`next_line`, `get_argument`, `_opt_batch_script`): https://github.com/SchedMD/slurm/blob/5bf5f13326d1b9d45863ac69b8e2f8468c2b65c2/src/sbatch/opt.c#L473-L691
- [S2] sbatch man page: https://slurm.schedmd.com/sbatch.html
