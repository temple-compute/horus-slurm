# Shared Slurm follow-ups: four agent teams

## Context
The shared Slurm allocation feature is up for review in three PRs: horus-runtime#220, horus-slurm#15 (branch `feat/shared-allocation`, worktree `/home/cdominguez/Developer/horus-slurm-shared`) and tc-os#503 (branch `chdominguez/slurm-same-target`). The five-agent review left four follow-ups, and the user wants one Orca team (the `team-of-agents` skill) per follow-up. Each team runs research → design → implement → review → E2E in its own Orca worktree.

## Shared rules for every team
- **Base branch:** horus-slurm work branches from `feat/shared-allocation`, and each PR targets that branch so the diff stays reviewable. They are retargeted to `main` once #15 merges. tc-os work branches from `chdominguez/slurm-same-target`.
- **Who owns what in `src/horus_slurm/target/slurm.py`**, to avoid conflicts:
  - **Load team:** `poll`, `_queue_state`, `_partition_summary`/`_sinfo`, the liveness check in `_allocation`.
  - **Robustness team:** `_submit_step`, `PILOT_LOOP`, `_pilot_script`, `release`, `_sbatch_header`.
  - **Badges team:** `emit_node_phase`, and a new event in `events.py`.
  - **Tests team:** only `tests/integration/`, CI workflow, docker compose. No source edits unless a test proves a bug; then it reports to the coordinator.
- **House rules:**
  - No emoji, and no Claude attribution in commits or PRs.
  - Short, neutral copy.
  - Plugin fixes rather than runtime API changes.
  - Every change gets a test.
  - Run `ruff`, `mypy` and `pytest` (horus-slurm), or `biome`, `tsc` and `bun test` (frontend).
- **Merge order:** tests → load → robustness → badges. A later team rebases on an earlier one if both touched the same file.

## Team 1: Login-node load (horus-slurm)
**Goal:** one `squeue` per allocation per poll interval, however many tasks there are.
- Add a cached `QueueState` and a timestamp on `_Allocation` (the shared handle). Members' `poll()` reads the cached state; the first member past `poll_interval` refreshes it, under a small refresh lock.
- Move the `sinfo` partition cache (`_partition_checked`/`_partition_nodes`) to the handle for shared targets.
- `_allocation()` skips its liveness `squeue` when the cached state is fresh. `poll()` clears `job_id` on `state == ""`, comparing by id first.
- Inline targets keep today's behaviour.
- **Done when:** a unit test with 50 copies polling counts `squeue` calls at ≤ 1 per interval. Lock-hold time in `_allocation` drops (no SSH call when fresh).

## Team 2: Robustness (horus-slurm)
- **No duplicate after an ambiguous `mv`:** when the `mv` reports failure, check `spool/<step>.sh` / `run/<step>.sh` before raising; if the file landed, return as success.
- **Steps orphaned by a dead allocation:** when the allocation is gone, launchers still in `spool/` (never claimed) move into a fresh allocation instead of failing their members. Claimed ones fail as now.
- **Header injection (existing issue):** reject `\n`/`\r` in `partition`, `account`, `qos`, `mem`, `time_limit`, `gres`, `extra_sbatch_args` with a pydantic validator.
- **Cleanup:** remove `.horus_alloc/<key>-<gen>` dirs older than N days at allocation time (best effort). Keep the newest `pilot.log` of each target.
- **Done when:** a unit test covers each item, plus a bash test of the requeue-free pilot path.

## Team 3: Phase badges (horus-slurm + tc-os)
- **horus-slurm:** `emit_node_phase` emits a new `SlurmAllocationEvent` with event type `slurm_allocation_event`. Fields: phase, `target_ref`, lease key, generation, allocation job id, member task ids. Also emit `queued`/`ready` from `_allocation` when it submits and when `squeue` first says RUNNING.
- **tc-os frontend:** `frontend/src/hooks/workflow/machineStore.ts` (today it only handles `terraform_node_event`, `NODE_EVENT_TYPE`) also applies `slurm_allocation_event`, mapping its phases onto the existing `NodePhase`. The badge appears on member cards through the existing `useMachinePhase`.
- **tc-os backend:** check that the event reaches the run event stream, and that `backend/src/app/core/cost_ingest.py` and `machines.py` ignore it (no billing, no admin machine row).
- **Done when:**
  - frontend tests in `machineStore.test.ts` for the new event;
  - a horus-slurm unit test for emission;
  - E2E on the canvas with a recorded run event, if the team's E2E role can drive the app.

## Team 4: Real Slurm tests (horus-slurm)
- Add `tests/integration/` that runs against a docker Slurm cluster (slurmctld + 2 slurmd + slurmdbd). Pick a maintained image, and pin it with a digest.
- Add a `make integration` target and a CI job in `.github/workflows/python-ci.yaml` (separate, may be slower). Integration tests are skipped when no cluster is reachable.
- **Cases:**
  1. Two `--exact` steps run at the same time inside a `--ntasks=1 -c 4` allocation.
  2. Cancelling a step pending for resources stops it (exit 143) and leaves the others alone.
  3. `scancel` on a purged job id prints "Invalid job id" (release tolerance).
  4. The pilot carries `--no-requeue` (`scontrol show job`).
  5. The pilot self-exits on a stale heartbeat, using a lowered threshold.
  6. A full `SlurmTarget` round trip with `LocalTarget` as the inner target, inside the login container.
- Report any assumption that fails as a finding to the coordinator, rather than changing the source.
- **Done when:** the suite passes locally with docker, and the CI job is green on the PR.

## Launch
1. Invoke the `team-of-agents` skill once per team, with the brief above. Four teams, run in parallel; this session coordinates.
2. Each team opens a PR against its base branch. I relay findings across teams: in particular, Team 4 results may change Teams 1 and 2.
3. Summarise the four PRs and the open findings to the user.

## Verification
- Each PR: its repo's lint, type and unit suites are green, and it gets a reviewer agent sign-off.
- Team 4's integration suite runs once against the combined branch (Teams 1–3 rebased) before the release order resumes: tag horus-runtime → horus-slurm → bump the tc-os pins.
