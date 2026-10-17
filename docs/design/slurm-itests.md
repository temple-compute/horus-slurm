# Slurm integration tests: design

Team 4 of `slurm-itests-plan.md`. Built on `slurm-itests-codebase.md` and
`slurm-itests-competitors.md`. Facts marked **(spike)** come from a throwaway
cluster run on 2026-10-17 with the exact image and config below (docker
29.4.3, amd64, host uid 1000). Nothing from the spike is committed.

## 1. Findings from the spike

Report these to the coordinator; they change what cases 2 and 3 can assert.

- **F1, case 2: a cancelled step never exits 143 through srun.** srun waiting
  for resources and sent SIGTERM prints `srun: Cancelled pending job step
  with signal 15` and exits **1**. srun of a running step sent SIGTERM prints
  `srun: forcing job termination`, the step is `CANCELLED+ 0:9`, and srun
  exits **137** (with `KillWait=5`). The launcher writes `$code` from
  `wait $p`, so `poll` reports 1 or 137, not 143. Only the still-spooled path
  writes 143 (it never runs srun). This is for the robustness team
  (`_submit_step` launcher), not for us to patch.
- **F2, case 3: `scancel` on a purged or unknown id is silent.** On 26.05.2,
  `scancel 999999` and `scancel <purged id>` exit **0** with empty stdout and
  stderr. "Invalid job id specified" only shows with `-v`. `release` still
  works (rc 0 is the success path), but the `invalid job id` branch is never
  taken on this version. Case 3 asserts the tolerance, not the message.
- **F3: workers must be privileged.** slurmd 26.05.2 starts the cgroup plugin
  even with `proctrack/linuxproc` + `task/affinity`, and fails with
  `Unable to create cgroup /sys/fs/cgroup/system: Read-only file system`.
  Upstream runs workers `privileged: true` with `init: true`, and that works.
  A `cgroup.conf` with `CgroupPlugin=disabled` did not help (one try).
- Verified working **(spike)**: static nodes `c[1-2]` with `CPUs=4
  RealMemory=2048` via `config_overrides`; `CR_CORE_MEMORY`; `MinJobAge=5`
  (a finished job is unknown to `scontrol` within seconds); two `--exact`
  1-CPU steps inside `-c 4 --mem=1024` both ran and passed a file barrier;
  a pilot with `--no-requeue` shows `Requeue=0` while `JobRequeue=1`;
  `sacct` works for a user with no association (no
  `AccountingStorageEnforce`); cold `up` to both nodes idle and
  `sacctmgr list cluster` answering: **14 s** (image already pulled).
- The image ships Python 3.12; the project needs `>=3.13`.

## 2. Cluster

All files under `tests/integration/cluster/`.

- **Image:** `giovtorres/slurm-docker-cluster@sha256:8b92cbd3bf7e51f910c7266c1dac13cf0c59059fa8db3c385bec76952a79b47b`
  (tag `26.05.2-3.0.0`, multi-arch index; amd64 and arm64). Slurm 26.05.2,
  Rocky 9.8. The munge key is baked in, identical in every container, so no
  shared `/etc/munge` volume.
- **Database:** `mariadb@sha256:250e1609e93c0e6aecf64bfbab2d84d5e89d2481540fe77447ff6c855c3b1873`
  (`mariadb:11.4`). The service must be named `mysql`: the image's
  `slurmdbd.conf` uses `StorageHost=mysql`, user/password from
  `MYSQL_USER`/`MYSQL_PASSWORD`.
- **`compose.yaml`** (project name `horus-slurm-itest`):
  - `mysql`: `MARIADB_RANDOM_ROOT_PASSWORD=yes`,
    `MARIADB_DATABASE=slurm_acct_db`, `MARIADB_USER=slurm`,
    `MARIADB_PASSWORD=password`.
  - `slurmdbd`, `slurmctld`: the image entrypoint with `command:
    [slurmdbd]` / `[slurmctld]`, hostnames to match. `slurmctld` is the
    login node.
  - `c1`, `c2`: two explicit services (not replicas), `hostname: c1`/`c2`,
    `privileged: true`, `init: true` (F3), `command: [bash, -c, "until
    >/dev/tcp/slurmctld/6817; do sleep 1; done 2>/dev/null; exec
    /usr/sbin/slurmd -D"]`. The entrypoint starts munged first and then
    falls through to `exec "$@"`.
  - Every Slurm service mounts `./slurm.conf:/etc/slurm/slurm.conf:ro` and
    `${ITEST_DIR}:${ITEST_DIR}` (same absolute path as the host).
  - No slurmrestd, no jwt (dropped from our `slurm.conf`), no named volumes.
- **`slurm.conf`** (ours, replaces the image's):

  ```
  ClusterName=itest
  SlurmctldHost=slurmctld
  AuthType=auth/munge
  ProctrackType=proctrack/linuxproc
  TaskPlugin=task/affinity
  ReturnToService=2
  SlurmdParameters=config_overrides
  SlurmUser=slurm
  StateSaveLocation=/var/lib/slurm
  SlurmdSpoolDir=/var/spool/slurm
  SlurmctldPidFile=/var/run/slurm/slurmctld.pid
  SlurmdPidFile=/var/run/slurm/slurmd.pid
  SlurmctldLogFile=/var/log/slurm/slurmctld.log
  SlurmdLogFile=/var/log/slurm/slurmd.log
  InactiveLimit=0
  KillWait=5
  MinJobAge=5
  SlurmctldTimeout=30
  SlurmdTimeout=30
  Waittime=0
  SchedulerType=sched/backfill
  SelectType=select/cons_tres
  SelectTypeParameters=CR_Core_Memory
  AccountingStorageType=accounting_storage/slurmdbd
  AccountingStorageHost=slurmdbd
  NodeName=c[1-2] CPUs=4 RealMemory=2048 State=UNKNOWN
  PartitionName=cpu Nodes=c[1-2] Default=YES MaxTime=INFINITE State=UP
  ```

  Static nodes instead of upstream's `slurmd -Z`: they report the host's
  CPUs (2 on some runners); `config_overrides` lets a node declare 4.
  `CR_Core_Memory` makes memory consumable, as on real clusters, so a wrong
  memory share shows up as steps queueing. `KillWait=5` keeps cancels fast.
  `MinJobAge=5` is for case 3.
- **cgroups:** cases 1 and 2 need none. Step placement under `--exact` is
  done by slurmctld; the spike ran both without cgroup constraints.
  `linuxproc` can miss reparented processes, which does not matter for our
  `sleep`/file-barrier steps. Workers need `privileged` only so slurmd can
  start (F3). **Fallback**, if a case turns out to need real cgroup
  tracking: sind via `GSI-HPC/sind-action@v2` with `sind-node` pinned by
  digest (`proctrack/cgroup`, `task/cgroup`, cgroup v2). Not planned.
- **`up.sh`** (called by make and CI): `mkdir -p "$ITEST_DIR"` **before**
  `compose up` (otherwise docker creates it root-owned and
  `_make_private_dir`'s `[ -O root ]` fails), `docker compose up -d
  --no-build`, `useradd -u $(id -u) -M -d "$ITEST_DIR" itest` in
  `slurmctld`, `c1`, `c2` (skip if the uid exists; containers lose it on
  recreate), then wait up to 120 s for `sinfo -h -N -o %T` to show two
  `idle` and `sacctmgr -n list cluster` to print `itest`. On timeout it
  prints `docker compose logs` and exits 1.
- `ITEST_DIR` defaults to `/tmp/horus-slurm-itest-$(id -u)` (outside the
  repo, so no `.gitignore` change). CI sets `$RUNNER_TEMP/itest`. The CI
  runner uid is 1001; never hardcode it.

## 3. How tests run: host + `docker exec`

Chosen: pytest on the host, Slurm commands sent into `slurmctld`
(codebase notes, option B), for all cases including 6.

- The image has Python 3.12, the project needs 3.13. Running inside would
  mean installing a Python, uv, and the private `horus-resource-monitor`
  token into the container.
- The host reuses the existing venv, `uv sync --group dev`, and the CI setup
  steps unchanged.
- File paths are identical on host and cluster (bind mount at the same
  path), so `LocalTarget` file I/O on the host is exactly what the login
  node would do on a shared FS.
- What it does not test: the Python process living on the login node. That
  difference is only the transport (`docker exec` vs a local shell), which
  `SlurmTarget` does not see.

`tests/integration/conftest.py` defines `ExecInner(LocalTarget)`, the same
pattern as `FakeInner` (`add_to_registry = False`). It overrides only
`run_command_sync`:

```
docker exec -i -u <uid> -w <cwd or working_directory> [-e K=V ...]
    <login container> bash -c <cmd>
```

It passes only the `env` it was given (not the host environment) and builds
the line with `shlex.join`. The login container name comes from
`HORUS_SLURM_ITEST_LOGIN` (default `horus-slurm-itest-slurmctld-1`). File
operations stay local.

Fixtures (sync, session-scoped where noted):

- `cluster` (session): runs `sinfo -h` through `ExecInner`, 10 s timeout.
- `itest_dir` (function): a fresh `$ITEST_DIR/<test name>-<uuid4 hex[:8]>`
  directory, made on the host.
- `shared_target(**fields)` factory: `SlurmTarget(inner=ExecInner(...),
  working_directory=itest_dir, cpus_per_task=4, mem="1G",
  time_limit="00:05:00")`, `_ref = "hpc"`. Teardown: `await
  target.release("run_end")`, then `scancel --me` through the inner, then
  dump `pilot.log` of every `.horus_alloc/*` dir if the test failed.
- A `_Task` stub and a `_launch` helper copied from
  `test_shared_allocation.py` (copy, not import: the unit module is not a
  package API).
- `wait_for(predicate, timeout, interval=1)`: async polling with a
  deadline, never a bare sleep.

## 4. Skip behaviour

- `tests/integration/conftest.py`, `pytest_collection_modifyitems`: if
  `HORUS_SLURM_ITEST != "1"`, add `skip(reason="set HORUS_SLURM_ITEST=1 or
  run make integration")` to every item under `tests/integration/`. So
  `make test` and the unit CI job collect them and skip them, with no docker
  call.
- When `HORUS_SLURM_ITEST=1` and the `cluster` probe fails, the fixture
  **fails** (not skips) with the probe's stderr. Opting in means the cluster
  should be there; a silent skip would turn the CI job green with nothing
  run.
- Every test is `@pytest.mark.integration` (already registered).

## 5. Coverage

- `make integration` runs with `--no-cov`. Checked: `pytest --no-cov` on a
  subset passes without the `--cov-fail-under=90` gate firing.
- It writes `integration-results.xml`, never `coverage.xml`, `htmlcov/` or
  `test-results.xml`.
- The unit gate is untouched: integration tests skip there and run no code.
  Baseline stays 136 passed, 94.89%.

## 6. Makefile

```
INTEGRATION_CMD = pytest tests/integration --no-cov -p no:cacheprovider --junitxml=integration-results.xml
ITEST_DIR ?= /tmp/horus-slurm-itest-$(shell id -u)

integration:
	ITEST_DIR=$(ITEST_DIR) tests/integration/cluster/up.sh
	ITEST_DIR=$(ITEST_DIR) HORUS_SLURM_ITEST=1 PYTHONPATH=. $(INTEGRATION_CMD)

integration-down:
	docker compose -p horus-slurm-itest -f tests/integration/cluster/compose.yaml down -v -t 2
```

Both go in `.PHONY` and `help`. `integration` leaves the cluster up so a
rerun costs seconds; `integration-down` removes it.

## 7. CI job

New job `integration` in `.github/workflows/python-ci.yaml`:

- `runs-on: ubuntu-latest`, `timeout-minutes: 20`, Python 3.13 only.
- Same checkout, setup-python, setup-uv (same pinned SHA), git rewrite and
  `uv sync --group dev` steps as `test`.
- `env: ITEST_DIR: ${{ runner.temp }}/itest`.
- `uv run make integration`.
- On failure: `docker compose ... logs` and `cat $ITEST_DIR/**/pilot.log`.
- `if: always()`: `make integration-down`; upload
  `integration-results.xml`.

**Trigger.** Today: `pull_request: branches: [main]`. This PR targets
`feat/shared-allocation`, so no job runs on it. Smallest change:
`branches: [main, feat/shared-allocation]`, reverted when #15 merges. See
Q1.

## 8. The six cases

One file per case, `tests/integration/test_<name>.py`. Timeouts are per
`wait_for` call. `D` is the test's `itest_dir`.

### Case 1: two `--exact` steps at once (`test_concurrent_steps.py`)

Target `cpus_per_task=4, mem="1G"`. Tasks `a` and `b`, `ResourceRequest(cpus=1)`
each (memory share 256M). Command for `a` (mirror for `b`):
`touch D/a.up; until [ -e D/b.up ]; do sleep 1; done; until [ -e D/go ]; do sleep 1; done`.

- `wait_for` both `.up` files, 60 s.
- `squeue -s -h -j <alloc job id> -o %i` lists exactly two steps
  `<job>.0`, `<job>.1` (plus `.batch`, filtered out).
- Both handles carry the same allocation job id; `target.generation == 1`;
  one `sbatch` was run (count via a recording wrapper on `ExecInner`).
- Touch `D/go`; `poll` returns `0` for both within 30 s.

### Case 2: cancel a step waiting for resources (`test_cancel_step.py`)

Target `cpus_per_task=2, mem="1G"`. Task `a` with `cpus=2`, command
`until [ -e D/go ]; do sleep 1; done`. Task `b` with `cpus=1`, `echo ran`.

- Wait until `a` runs (`squeue -s` shows one step) and `b` is claimed
  (`run/b.sh` exists, `spool/b.sh` gone), 60 s.
- `send_signal(handle_b, SIGTERM)`.
- `poll(handle_b)` returns non-None within 15 s; **assert `== 143`**, marked
  `xfail(strict=True, reason="F1: srun exits 1 when cancelled pending")`.
  Also assert (not xfail) that `b`'s stdout has no `ran`.
- `a` unaffected: allocation still `RUNNING`, `poll(handle_a) is None`.
  Touch `D/go`; `poll(handle_a) == 0` within 30 s.
- Spooled variant (`test_cancel_spooled_step`): first fill the cluster
  with a raw blocker, `sbatch --parsable -N 2 --ntasks-per-node=1 -c 4
  --wrap 'sleep 300'`, and wait for it to run. Launch `b`: the pilot stays
  PENDING, so `spool/b.sh` stays put. `send_signal(handle_b, SIGTERM)`.
  Assert `poll == 143`, `spool/b.sh` gone and no `run/b.sh` (srun never
  ran). Not xfail: the code writes 143 itself. Teardown cancels the
  blocker.

### Case 3: `scancel` on a purged id (`test_release_purged.py`)

- Raw: `scancel 999999` through the inner. Assert
  `rc == 0 or "invalid job id" in stderr.lower()` (the condition `release`
  relies on). Record both in the test output. **(spike: rc 0, empty)**
- Through the target: launch one task `true`, `poll == 0`. `scancel` the
  allocation job outside Horus, `wait_for` `scontrol show job <id>` to fail
  with `Invalid job id`, 60 s (`MinJobAge=5`). Then `await
  target.release("idle")`: does not raise, `is_live` is False,
  `<alloc>/spool` and `<alloc>/run` are gone, `<alloc>/pilot.log` exists.

### Case 4: the pilot carries `--no-requeue` (`test_no_requeue.py`)

Launch one task `until [ -e D/go ]; do sleep 1; done`; wait for RUNNING.
`scontrol show job <alloc id>` contains `Requeue=0`, and `scontrol show
config` contains `JobRequeue = 1` (so the check means something). Touch
`D/go`, release.

### Case 5: the pilot exits on a stale heartbeat (`test_stale_heartbeat.py`)

`monkeypatch.setattr(slurm_mod, "PILOT_LOOP", PILOT_LOOP.replace("-gt 900",
"-gt 5"))`, as the unit test does (the f-string is built at import, so
patching `HEARTBEAT_STALE_S` does nothing). Assert the replace changed the
string, so a renamed constant fails loudly.

- Stale: launch `true`, `poll == 0` (pilot now idle). `os.utime(alloc/heartbeat,
  (now-60, now-60))`. The job leaves `squeue -h -j <id>` within 20 s;
  `sacct -n -X -j <id> -o state,exitcode` is `COMPLETED 0:0` (wait up to
  15 s for accounting).
- Control: same, but call `await target.keepalive()` every 2 s for 15 s.
  The job is still `RUNNING` at the end. Then release.

### Case 6: full `SlurmTarget` round trip (`test_round_trip.py`)

Level 1 (required): target `cpus_per_task=2, mem="1G"`, tasks `ok`
(`echo out; echo err >&2`) and `bad` (`exit 3`), `cpus=1` each.

- `poll` returns `0` and `3`.
- `read_output(handle_ok) == (b"out\n", b"err\n")`; the files sit in the
  task's side-artifacts dir.
- Both handles share one allocation job id; `generation == 1`; one `sbatch`.
- `keepalive()` touches `heartbeat` (mtime moves forward).
- `release("run_end")`: the job leaves `squeue` within 20 s, `spool/` and
  `run/` are gone, `pilot.log` stays.

Level 2 (workflow through the runtime and `LeaseManager`): only if level 1
passes and a workflow can take a target instance with `ExecInner` from
Python. Otherwise deferred (see deviations).

## 9. Work packages

| WP | Scope (write) | Depends on |
|---|---|---|
| WP1 cluster + harness | `tests/integration/cluster/{compose.yaml,slurm.conf,up.sh}`, `tests/integration/__init__.py`, `tests/integration/conftest.py`, `tests/integration/test_cluster.py` (one smoke: `sinfo` shows `c1 c2` idle with 4 CPUs), `Makefile` | none |
| WP2 cases | `tests/integration/test_concurrent_steps.py`, `test_cancel_step.py`, `test_release_purged.py`, `test_no_requeue.py`, `test_stale_heartbeat.py`, `test_round_trip.py` | WP1 merged |
| WP3 CI | `.github/workflows/python-ci.yaml` | WP1 (`make integration` exists); green needs WP2 |

WP2 adds helpers it needs to its own test files, not to `conftest.py`. If
two cases need the same helper, WP2 asks WP1's owner. WP3 can start in
parallel with WP2.

## 10. Test plan

- `uv run make lint` (ruff + mypy cover `tests/`, so integration code is
  typed) and `uv run make test`: 136 passed plus the integration tests
  skipped, coverage still >= 90.
- `make integration` locally: all pass except case 2's strict xfail, which
  must xfail. Run it twice in a row on a live cluster (no leaked
  allocations, rerun under 2 min).
- `make integration-down && make integration` from cold.
- Break it on purpose once: stop `c1` and `c2`, run `make integration`; the
  probe must fail, not skip.
- CI: the `integration` job green on this PR (after Q1).

## 11. Deviations from the plan

- Case 6 runs from the host through `docker exec`, not inside the login
  container (section 3).
- "Skipped when no cluster is reachable" holds for a plain `pytest`; with
  `HORUS_SLURM_ITEST=1` an unreachable cluster fails the run (section 4).
- Case 2 asserts 143 as a strict xfail because of F1, instead of a plain
  pass. Case 3 asserts the tolerance, not the "Invalid job id" message (F2).
- Case 6 level 2 is conditional and may be deferred to a follow-up.
- Compose and config live in `tests/integration/cluster/`, not a top-level
  `docker/`.

## 12. Open questions

- **Q1. CI trigger.** Add `feat/shared-allocation` to `pull_request.branches`
  so the job runs on this stacked PR, and drop it after #15 merges?
  *Recommended: yes.* The alternative (`workflow_dispatch`) needs a manual
  run and does not show on the PR.
- **Q2. Case 2 vs F1.** Keep 143 as the contract (strict xfail until the
  robustness team makes the launcher write 143 when `.cancel` exists), or
  assert today's 1/137? *Recommended: strict xfail on 143*, and send F1 to
  the robustness team.
- **Q3. Privileged workers (F3).** Accept `privileged: true` on `c1`/`c2` in
  CI? *Recommended: yes*; it matches upstream and only runs on a throwaway
  runner. The alternative is sind.

## User decisions

- Approved 2026-10-17 with the recommended answers: Q1 add feat/shared-allocation to the CI pull_request branches; Q2 keep 143 as the contract, strict xfail until the Robustness branch fixes the launcher (coordinator sent F1 to that team: the launcher records 143 whenever the cancel mark exists after srun ends); Q3 privileged workers in CI accepted.
