# Review: Slurm integration tests, correctness and test quality

Scope: `git diff 116c7dc...HEAD -- tests Makefile .github` on
`chdominguez/slurm-itests`, checked against `docs/design/slurm-itests.md`
(including the User decisions). Method: read by hand plus `/code-review high`.

## What was run

- `uv run pytest -q -p no:cacheprovider` (no `HORUS_SLURM_ITEST`): 136 passed,
  10 skipped, coverage 94.89%. The unit gate is unchanged.
- `HORUS_SLURM_ITEST=1 uv run make integration ITEST_PROJECT=horus-slurm-itest-rev`,
  starting from a cold cluster: 9 passed, 1 xfailed, 47 s. `squeue` was empty
  afterwards. Then `make integration-down`, which left no containers.
  A plain `make integration` without `uv run` fails with
  `pytest: not found` (exit 127). CI uses `uv run make`, so CI is fine.
- Ran case 2 with `--runxfail`. It raises `_F1Error: poll returned 1`,
  which matches F1.
- Opt-in with an unreachable login container
  (`HORUS_SLURM_ITEST_LOGIN=nope-slurmctld-1`): the test errors in the
  `cluster` fixture instead of skipping, as section 4 requires.
- Tried the strict xfail semantics in a throwaway file. A pass with
  `strict=True, raises=E` gives `FAILED [XPASS(strict)]`. Any other
  exception is reported as a plain failure.

## Case-by-case

| Case | Matches design | Would fail if broken |
|---|---|---|
| 1 concurrent steps | yes. "One sbatch" is checked as `generation == 1` plus one alloc dir, not with a recording wrapper. That is equivalent, because each sbatch makes `<lease>-<generation>` | yes: the file barrier deadlocks unless both steps run together |
| 2 cancel pending step | yes, with the strict xfail on 143. The stdout and allocation checks run before the xfail raise, so they are real assertions | yes |
| 2b cancel spooled | yes | yes |
| 3 scancel purged | yes. On 26.05 `rc == 0` makes the raw check nearly vacuous, which F2 accepts | yes for the release part |
| 4 no-requeue | yes. It also checks `JobRequeue = 1` | yes |
| 5 stale heartbeat | yes (`-gt 10` instead of `-gt 5`, which is fine). The patch asserts it changed the string | yes. In the control test, a broken keepalive lets the heartbeat age past 10 s within the 15 s window |
| 6 round trip | level 1 yes. Level 2 is deferred, as allowed | yes |

No bare sleeps act as synchronisation. The one `asyncio.sleep(15)`
(`test_stale_heartbeat.py:84`) is the observation window the design asks for.

## Flipping case 2 once the Robustness 143 fix lands

When `poll` returns 143, `test_cancel_step_waiting_for_resources` reports
`XPASS(strict)` and fails the run (verified above). To flip it, delete the
`@pytest.mark.xfail(...)` decorator and the `_F1Error` class, then replace
`if code != 143: raise _F1Error(...)` (`test_cancel_step.py:67-68`) with
`assert code == 143`.

## Findings

| # | Sev | Where | Problem | Fix | Status |
|---|---|---|---|---|---|
| 1 | Medium | `tests/integration/conftest.py:149-152` | The teardown is not exception-safe. If `release()` raises (it raises `TaskExecutionError` on any scancel failure other than "invalid job id"), the remaining targets are not released and `scancel --me` is skipped. Leaked pilots or the 2-node blocker then hold the nodes for up to 5 min, and every later test fails its 60 s `wait_for`. | Put `scancel --me` in a `finally`, and suppress or collect each target's release error. | suspected (read) |
| 2 | Medium | `tests/integration/conftest.py:151` | `scancel --me` returns before the jobs have left the queue. `test_cluster.py` sorts directly after `test_cancel_step.py` and expects both nodes `idle`, so it can see `completing` or `allocated` from the blocker teardown. It passed in my one run. | In the teardown, `wait_for` until `squeue --me -h` is empty. Or relax the smoke test to "not down" or wait for idle. | suspected; passed once |
| 3 | Low | `test_stale_heartbeat.py:36`, `test_round_trip.py:57`, `test_cancel_step.py:61,86`, `test_no_requeue.py:36` | `squeue -h -j ... -o %T` ignores the return code. A failing `squeue` gives empty stdout, which `gone()` takes as "job left": a false pass. | Assert `rc == 0` inside a shared `job_state` helper, or use `target._queue_state`. | suspected (read) |
| 4 | Low | `tests/integration/conftest.py:62-63` | `read_text()` is strict UTF-8, and logs are attached only when `call.when == "call"`. Non-UTF-8 job output turns the report into an internal error, and setup or teardown failures get no pilot log. | Use `read_text(errors="replace")` and attach on any failed report. | suspected (read) |
| 5 | Low | `tests/integration/conftest.py:83` | `docker exec -i` forwards the host's stdin. With `pytest -s` in a terminal, each Slurm command attaches to the tty. Nothing ever writes stdin. | Drop `-i`. | suspected (read) |
| 6 | Low | `Makefile:55` (`integration-down`) | The per-test dirs under `ITEST_DIR` are never removed. 39 had piled up in `/tmp/horus-slurm-itest-1000` after a few runs. | `rm -rf $(ITEST_DIR)` in `integration-down`, or remove the dir in `itest_dir` teardown when the test passed. | verified (`ls`) |
| 7 | Low | `test_concurrent_steps.py:47`, `test_cancel_step.py:48` | Two different step-id filters (a suffix blacklist and `isdigit`), and neither is the `_step_id` regex. | One helper matching `\d+(_\d+)?\.\d+`. | read |
| 8 | Info | `.github/workflows/python-ci.yaml:5` | `feat/shared-allocation` in the `pull_request` branches is intended (Q1) and must be dropped after #15 merges. It also widens the lint and unit triggers. | Track the revert. | read |
| 9 | Info | `Makefile:10`, `python-ci.yaml:102`, `conftest.py:35` | Project name `horus-slurm-itest` is hardcoded in the CI log dump and the conftest default. This is fine for CI, but a direct `pytest` run against a non-default `ITEST_PROJECT` needs `HORUS_SLURM_ITEST_LOGIN` set by hand. | Use `$ITEST_PROJECT` in the CI dump step. | read |

The skip/opt-in logic, `--no-cov` handling, and the CI `always()` teardown and
`failure()` log dump all match the design. I found no issues there.

Counts: High 0, Medium 2, Low 5, Info 2.
