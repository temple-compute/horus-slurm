# Slurm integration tests: codebase notes

Research for Team 4 of `slurm-itests-plan.md`. Line numbers are for
`src/horus_slurm/target/slurm.py` at `55092f1` unless a file is named.

## 1. What exists today

### Test layout

```
tests/
  conftest.py              registry init, horus_context fixture, markers
  unit/
    conftest.py            FakeProcess, FakeInner, `inner` fixture
    test_probe.py
    test_shared_allocation.py
    test_slurm_target.py
```

There is no `tests/integration/` yet.

- `tests/conftest.py` registers the `unit` and `integration` markers in
  `pytest_configure`, so `@pytest.mark.integration` already works without a
  warning. It also has the session-scoped autouse `init_registry`
  (`AutoRegistry.init_registry()`), which integration tests get for free, and
  a `horus_context` fixture.
- `tests/unit/conftest.py`: `FakeInner(LocalTarget)` keeps real file
  operations and replaces `run_command_sync`. It records every command
  (`commands`, `commands_starting(prefix)`), answers from canned
  `FakeProcess`es keyed by command prefix (`responds(sbatch=...)`), and really
  runs commands whose prefix was passed to `runs(...)`.
  `test_shared_allocation.py::_shared` uses `runs("umask ", "mv ", "touch ",
  "rm ")`, so only the Slurm commands are faked. The `inner` fixture answers
  `sbatch` with job `12345`.
- `test_shared_allocation.py` builds a shared target by setting
  `target._ref = "hpc"` by hand (`_shared`, line 66) and binds a small `_Task`
  stub (id, working_dir, resources, side_artifacts_dir). The bash tests
  (`TestScripts`, from line 318) run the real launcher and pilot with a fake
  `srun` on `PATH`. `test_the_pilot_runs_steps_and_ends_when_orphaned`
  lowers the stale threshold with
  `monkeypatch.setattr(slurm_mod, "PILOT_LOOP", PILOT_LOOP.replace("-gt 900", "-gt 1"))`.
  Integration tests can reuse the `_Task` stub and this patch.

### pytest config (`pyproject.toml`)

- `addopts`: `--verbose --cov=src/horus_slurm --cov-report=html
  --cov-report=term-missing --cov-fail-under=90`. Coverage and the 90% gate
  apply to every pytest run, including a run that selects only one directory.
- `testpaths = ["tests"]`: a plain `pytest` collects `tests/integration/` too.
- `asyncio_mode = "auto"`: `async def test_*` needs no decorator. Fixtures are
  function-scoped by default; a session-scoped cluster fixture should be sync.
- `[tool.coverage.run] source = ["horus_slurm"]`, report omits `tests/*`.
- No pytest-timeout plugin. Use `asyncio.wait_for` or `subprocess` timeouts.
- `horus-runtime` resolves to 0.7.1 from PyPI. It already has
  `BaseTarget.lease_key`, `_ref`, `releasable`, `keepalive`, the
  `LeaseManager` (`horus_runtime/core/lease.py`) and workflow `targets:`.
  `horus_builtin` has only `LocalTarget`; there is no SSH target.
- Baseline: `uv run pytest -q -p no:cacheprovider` gives 136 passed, 94.89%
  coverage.

### Makefile

`PYTEST_CMD = pytest --cov=src/horus_slurm --cov-report=xml --cov-report=html
--cov-report=term --junitxml=test-results.xml`. Targets: `test` (with
`PYTHONPATH=.`), `lint` (ruff check, ruff format check, mypy on `src/ tests/`),
`ruff-check`, `ruff-format-check`, `mypy-check`, `format`, `type-check`,
`babel-*`, `add-license-headers`, `clean`. There is no `integration` target.
New targets go in `.PHONY` and `help`. mypy also checks `tests/`, so
integration tests need type hints.

### CI (`.github/workflows/python-ci.yaml`)

- Trigger: `pull_request` on `branches: [main]` only.
- `lint`: Python 3.13, uv (pinned `setup-uv` SHA), git rewrite with
  `secrets.ACTIONS_TOKEN` (the dev group pulls private
  `horus-resource-monitor` from git), `uv sync --group dev`, `uv run make lint`,
  `uv run make babel-check`.
- `test`: matrix 3.13 and 3.14, same setup, `uv run make test`, uploads
  `htmlcov/` on 3.13.

An `integration` job should copy the setup steps of `test` (one Python
version), start the compose cluster, wait until it is ready, run
`make integration`, and dump `docker compose logs` on failure.

## 2. The six cases

The pilot is built by `_pilot_script` (691): `_sbatch_header` with
`--nodes={nodes}` and `--ntasks={ntasks}` (both default 1) and
`--cpus-per-task`, `--mem` and `--time` from the target, then
`#SBATCH --no-requeue` (703), `cd alloc_dir`, and `PILOT_LOOP` (115). Each
task becomes a launcher built in `_submit_step` (707). The launcher runs
`srun --exact --ntasks=1 --cpus-per-task=<task cpus> --mem=<share>` (742), and
the memory share is `target mem * step cpus / target cpus`.

### Case 1: two `--exact` steps run at the same time

- Path: `_allocation` (616) submits the pilot. `_submit_step` (707) writes
  `run/<step>.job` and moves `spool/<step>.tmp` to `spool/<step>.sh` (767).
  `PILOT_LOOP` claims it with `mv "$f" run/` and runs it in the background
  (120). The launcher runs `srun --exact` (742).
- Setup: target `cpus_per_task=4`, `ntasks=1`, a `mem` that fits the node's
  `RealMemory`, and two tasks with `resources: {cpus: 1}` (or 2).
- Assert overlap with a barrier, not timing: each step touches `<name>.up`,
  then waits up to about 30 s for the other's file, and exits 0 only if it
  saw it. Both `exit_code` files must read `0`. While they run,
  `squeue -s -h -j <job>` should show two steps of one job. Every `sbatch`
  call must return the same job id: there is one allocation.
- Cluster: nodes need `CPUs>=4`. If memory is consumable (`CR_*_Memory`),
  the share must fit, or steps queue one after the other and the barrier
  times out. That would be a real finding.

### Case 2: cancelling a step that waits for resources

- Path: `send_signal` (1092) sees `extra["step"]` and calls `_signal_step`
  (1119). For SIGTERM/SIGKILL it touches `run/<step>.cancel` and tries to
  `rm spool/<step>.sh` (1145). If the step is still spooled, that writes
  `143` at once. If it is claimed, the launcher's loop sends
  `kill -TERM $p` to srun (756), then `wait $p; code=$?` and writes `$code`
  (759-760).
- Setup: allocation `-c 2`. Step A takes 2 CPUs and sleeps (or waits on a
  file the test controls). Step B asks for 1 CPU, so srun waits ("Requested
  nodes are busy"). Wait until B is claimed (`run/B.sh` exists, `spool/B.sh`
  gone), then `send_signal(handle_B, SIGTERM)`.
- Assert: B's `exit_code` is `143` and `poll(handle_B)` returns 143 within a
  few seconds. A is still running (the allocation is `RUNNING`, A has no
  `exit_code`), then finishes with `0` once released.
- Assumption to check: srun killed with SIGTERM while waiting for step
  creation exits 143. If it catches TERM and exits 1 (or anything else),
  `poll` reports that code, not 143. The unit test only fakes srun
  (`test_a_cancel_stops_a_waiting_or_running_step`). Report a mismatch to
  the coordinator; do not patch it.
- Also cover the spooled case: cancel before the pilot claims the launcher
  (for example, cancel while the pilot is still PENDING). Assert `143`
  without srun ever running.

### Case 3: `scancel` on a purged job id (release tolerance)

- Path: `release` (569) runs `scancel <job_id>`. A non-zero exit is ignored
  when stderr contains `invalid job id` (580). It then clears `job_id` (586)
  and removes `spool/` and `run/` (590).
- Raw check: `scancel <id>` where the id never existed or has been purged.
  Assert `"invalid job id" in stderr.lower()`, and record the exit code,
  since the code tolerates either.
- Through the target: start an allocation, end it outside Horus, wait until
  slurmctld forgets it, then `await target.release("idle")`. Assert it does
  not raise, `is_live` is False, `spool/` and `run/` are gone and
  `pilot.log` stays. To end it, use `scancel` or the idle exit. To make
  slurmctld forget it, set `MinJobAge` low in the test `slurm.conf` (the
  default is 300 s), or poll `scontrol show job` until it fails.

### Case 4: the pilot carries `--no-requeue`

- Path: `_pilot_script` (703).
- Submit through `_allocation` (it needs a bound task or a `_ref`), or just
  `sbatch` the text of `_pilot_script(alloc_dir)` after
  `_make_private_dir` (664). Assert that `scontrol show job <id>` contains
  `Requeue=0`. The cluster default is `JobRequeue=1`, so the check means
  something. Cancel the job in teardown.

### Case 5: the pilot exits on a stale heartbeat

- Path: the idle check in `PILOT_LOOP` (122-126). It only fires when
  `jobs -rp` is empty, so the pilot must be idle. `keepalive` (597) is what
  normally touches `heartbeat`.
- `PILOT_LOOP` is an f-string built at import time, so patching
  `HEARTBEAT_STALE_S` has no effect. Patch `slurm_mod.PILOT_LOOP` as the unit
  test does (`"-gt 900"` to `"-gt 5"`). `_pilot_script` reads the module
  global at call time.
- Assert: with `heartbeat` aged (`os.utime` to 60 s ago), the job leaves
  `squeue` within about 15 s, and `sacct` says `COMPLETED` with exit 0.
  Control case: with `keepalive()` called every 2 s, the job is still
  `RUNNING` after 15 s.
- The check compares the compute node's clock with the file's mtime on the
  shared FS. Docker containers share the host clock, so this is safe here.
  See risks.

### Case 6: full `SlurmTarget` round trip with `LocalTarget` inner

- Path: `launch` (395), the shared branch (415), then `_submit_step` (707),
  then `poll` (978). `poll` runs `_queue_state` (1295) and checks for
  `job_dir/exit_code` (998). After that come `read_output` (1073) and
  `release` (569). `keepalive` (597) runs in between.
- Two levels:
  1. Direct, as the unit tests do: `SlurmTarget(inner=LocalTarget(),
     working_directory=<shared dir>, cpus_per_task=2, mem=..., time_limit="00:10:00")`
     with `_ref = "hpc"`. Bind two `_Task`s, launch both, and poll until each
     returns. Assert exit codes `0` and `3` (the command decides), the
     expected stdout in the side-artifacts dir, one allocation job id on
     both handles, and one `sbatch` (`generation == 1`). After
     `release("run_end")`, the job leaves `squeue`, `spool/` and `run/` are
     gone, and `pilot.log` stays.
  2. Workflow level: a YAML or Python workflow with `targets: {hpc: {kind:
     slurm, ...}}` and two tasks on `target: hpc`, run through the runtime.
     This also tests the `LeaseManager`: one allocation, release at run end,
     and the hold timeout. It is slower. Do it only if level 1 passes.

## 3. Running in the login container

### What the code needs from the environment

- `inner=LocalTarget()` runs `sbatch`, `squeue`, `scancel`, `sacct` and
  `sinfo` as local subprocesses, and does file I/O on the local filesystem.
  Everything runs as the current user. So the process that runs
  `SlurmTarget` must be on a host with Slurm client tools and a working munge
  key, which means the login container.
- `resolved_working_directory` (316) is `working_directory` or
  `inner.resolved_working_directory`. The `.horus_alloc/<lease_key>-<gen>`
  directory, every task `job_dir` and the side-artifacts dirs must be on the
  filesystem shared with the compute containers, at the same absolute path.
  The pilot `cd`s there, and the launcher writes `exit_code` into `job_dir`.
- `_make_private_dir` (664) requires the parent to be owned by the caller
  (`[ -O root ]`) and creates the leaf with mode 700. `PILOT_LOOP` runs only
  launchers the job user owns (`[ -O "$f" ]`). So the user that writes files
  on the login node must have the same uid as the user the job runs as on
  the compute nodes, and that user must exist (`getpwuid`) on slurmctld and
  on every slurmd.

### Layout

The usual docker Slurm compose has slurmctld, c1, c2, slurmdbd and mysql. It
mounts one named volume (for example `/data`) on slurmctld and the compute
containers. slurmctld doubles as the login node. Use `/data/itest/<run-id>`
as `working_directory`.

Two ways to run the tests:

- **A. pytest inside the login container.** This is literally what the plan
  asks for. Mount the repo, install Python 3.13 and uv, and point
  `UV_PROJECT_ENVIRONMENT` at a path outside the mount so the host `.venv`
  is not overwritten. Problems: most images ship an older Python, and the
  dev group needs the private `horus-resource-monitor` git dep, so the token
  must reach the container. `uv run --no-dev --with pytest --with
  pytest-asyncio` avoids the private dep. Coverage cannot be merged with the
  host run.
- **B. pytest on the host, commands sent into the container.** Add a
  test-only `ExecInner(LocalTarget)`, the same pattern as `FakeInner`. It
  wraps `run_command_sync` in `docker exec -u <uid> <login> bash -c <cmd>`
  and keeps file I/O local. Bind-mount a host directory into every Slurm
  container at the same absolute path, so local paths and cluster paths are
  the same. Create a user with the host uid (`useradd -u $(id -u)`) on every
  container. The CI runner uid is not 1000, so do not hardcode it. This
  reuses the host venv and the CI setup, and gives cases 1-5 and 6.1 almost
  everything. The one thing it does not test is the process actually living
  on the login node.

Recommendation: B for cases 1-5 and 6.1. A only for 6.2, or for 6 too if the
team wants it to be literal. The coordinator should confirm the choice.

Running everything as root inside the containers is simpler, but it hides
the ownership checks in `_make_private_dir` and `PILOT_LOOP`. Use a normal
user.

### Skipping when there is no cluster

- Put `tests/integration/conftest.py` behind an opt-in env var (for example
  `HORUS_SLURM_ITEST=1`, plus the container name or a connection setting).
  In `pytest_collection_modifyitems`, or a module-level `pytest.skip`, skip
  everything when the variable is unset. When it is set, also skip if a
  probe (`sinfo -h` through the inner, 5 s timeout, return code 0) fails.
- Prefer opt-in over auto-detect. `testpaths = ["tests"]` means `make test`
  collects integration tests. With auto-detect, a developer with the cluster
  up would run the slow suite inside the unit job.
- Mark every test `@pytest.mark.integration` (already registered).
- Readiness: slurmdbd and mysql take a while. `make integration` (or the CI
  step) should wait until `sinfo -h -o %T` shows the nodes `idle` and
  `sacctmgr -n list cluster` answers, before pytest starts.

## 4. Coverage

- `addopts` adds `--cov-fail-under=90` to every run. Integration tests run on
  their own cover only part of the package, so `make integration` would fail
  the gate even when every test passes. Run them with `--no-cov` (pytest-cov),
  or `-o addopts=` with `--verbose` added back.
- The unit job is not affected as long as integration tests skip there:
  skipped tests run no code, and the unit tests alone keep 94.89%. Do not add
  integration coverage to the unit gate. With approach A the numbers come
  from another machine anyway.
- `make integration` should not write `coverage.xml`, `htmlcov/` or
  `test-results.xml` over the unit job's files. Use another `--junitxml`
  name if CI wants one.

## 5. Risks

- **CI will not run on the Team 4 PR as written.** `python-ci.yaml` triggers
  only on PRs into `main`, and the plan bases Team 4 on
  `feat/shared-allocation`. Either add that branch (or `workflow_dispatch`)
  to the trigger for now, or accept that CI only goes green after
  retargeting.
- **Image pinning.** Several docker Slurm cluster projects build the image
  locally and publish no registry image to pin by digest. Check before
  choosing. If the image is built in CI, pin the base image digest and the
  Slurm version, and cache the build. A cold build of Slurm is slow.
- **Default node sizes.** Stock `slurm.conf` files in these projects often
  declare nodes with 1 CPU and small `RealMemory`. Then `-c 4` is rejected
  ("Requested node configuration is not available") and `mem` must be
  small. Ship a test `slurm.conf` with `CPUs=4` (or more),
  `SelectType=select/cons_tres` and a known `RealMemory`, and set
  `MinJobAge` low for case 3.
- **srun exit code on cancel (case 2).** See above. If it is not 143, `poll`
  reports another code. That is a finding for the robustness team.
- **Clock and mtime.** The stale-heartbeat check mixes the compute node's
  clock with the shared FS's mtime. It is fine in docker. On real NFS, skew
  can end a pilot early or late. Note it, but don't test it.
- **uid mapping (approach B).** The host uid must exist with the same number
  in every container, or the `-O` checks fail and the pilot silently runs
  nothing (it skips launchers it does not own). Every launch then hangs
  until a timeout, so tests need short timeouts and should dump
  `pilot.log` on failure.
- **Leaked allocations.** A failing test can leave a pilot running for its
  whole `time_limit` and starve the next test's `-c 4`. Each test should
  call `release` and `scancel -u <user>` in teardown. Use short
  `time_limit`s (`00:05:00`).
- **Flaky timing.** Use barriers and polling with deadlines rather than
  sleeps. `PILOT_LOOP` sleeps 2 s per round and `poll_interval` is 5 s, so
  allow tens of seconds per case, not single seconds.
- **sacct needs slurmdbd.** `_final_state` (1392) returns `None` without it,
  and case 5's `sacct` check needs accounting registered.
- **File ownership.** No source changes are planned. Concurrent teams change
  `_submit_step`, `PILOT_LOOP`, `release` and `poll`, so the line numbers
  here will drift. The cases test behaviour, not lines.
