# Slurm in docker for CI: prior art

Research for Team 4 of `slurm-itests-plan.md`. All facts checked on 2026-10-17 against the linked repos, Docker Hub/GHCR and CI runs.

## Comparison

| Image / tool | Slurm versions | Nodes | Accounting | CI example | Maintenance | Link |
|---|---|---|---|---|---|---|
| giovtorres/slurm-docker-cluster (compose) | 26.05.2, 25.11.x [1] | mariadb, slurmdbd, slurmctld, slurmrestd, 2 `cpu-worker` (scalable) [1][2] | slurmdbd + MariaDB; `sacct` checked in its test script [2][3] | its own `test.yml`: amd64 + `ubuntu-24.04-arm` matrix [4] | 552 stars, release 3.0.0 on 2026-08-09, multi-arch images on Docker Hub [5][6] | [repo](https://github.com/giovtorres/slurm-docker-cluster) |
| giovtorres/slurm-docker (all-in-one) | 26.05.3, 25.11, 25.05.7, 24.11.7; Rocky 8/9/10 [7][8] | one container: MariaDB, slurmdbd, slurmctld, 3 slurmd (`node[1-3]`, 2 CPUs each), slurmrestd [7][9] | slurmdbd, `jobacct_gather/linux` [9] | PySlurm CI [10] | 10 stars, images updated 2026-08-20 [8] | [repo](https://github.com/giovtorres/slurm-docker) |
| GSI-HPC/sind (CLI + action) | 26.05.4, 25.11.8 [11] | controller, db, api, submitter, N workers, systemd per node [12] | db node with MariaDB + slurmdbd, `jobacct_gather/cgroup` [13] | `GSI-HPC/sind-action@v2` [14]; own CI on amd64 + arm64 [15] | v0.12.0 on 2026-10-12, 3 stars, weekly image rebuilds [11][16] | [repo](https://github.com/GSI-HPC/sind) |
| koesterlab/setup-slurm-action | distro package via galaxyproject/ansible-slurm 1.0.1 [17] | single node on the runner itself, no container [17] | slurmdbd against a MySQL service on port 8888 [17][18] | Snakemake SLURM executor plugin [18] | v1.1.0 on 2025-07-20, last push 2025-07-20 [19] | [repo](https://github.com/koesterlab/setup-slurm-action) |
| nathan-hess/docker-slurm | Ubuntu `slurm-wlm` package [20] | single container [20] | not documented [20] | none found | monthly rebuild, amd64 + arm64 tags dated 2026-09-01 [20][21] | [repo](https://github.com/nathan-hess/docker-slurm) |
| ghcr.io/tylern4/slurm-standalone | 24.05.0 (tag) [22] | single container used as the job container [22] | not checked | Parsl `parsl+slurm.yaml` [22] | source repo not found on GitHub (404) | [Parsl CI](https://github.com/Parsl/parsl/blob/master/.github/workflows/parsl%2Bslurm.yaml) |

Nextflow's CI (`build.yml`) has no Slurm job that we could find [23]. The Snakemake plugin uses the runner-native action, not docker [18].

## Questions from the brief

**Accounting and step records.** Both giovtorres images run slurmdbd with `accounting_storage/slurmdbd` [2][9]. The compose cluster sets no `JobAcctGatherType` [2], and Slurm leaves it unset by default [24], so `sacct` shows jobs and steps with states and exit codes but no CPU or memory usage. sind sets `jobacct_gather/cgroup` when there is a db node [13].

**cgroups and `--exact`.** The compose cluster uses `ProctrackType=proctrack/linuxproc` and `TaskPlugin=task/affinity`, with every cgroup constraint off [2]. Slurm warns that `linuxproc` can miss processes that get reparented to init [25]; that matters for case 2 (step cancel) if a step forks daemons. `--exact` limits a step to the resources it asked for and blocks parallel steps until those are free [26]. slurmctld does that step allocation, so cases 1 and 2 should behave the same without cgroups; we have not run them yet. Without `ConstrainCores` nothing pins the processes to their CPUs. sind uses `proctrack/cgroup` and `task/cgroup` with `CgroupPlugin=autodetect`, tracking but not constraining by default [13].

**CPUs for case 1 (`-c 4`).** Compose workers start `slurmd -Z` with no `CPUs=`, so they report the host's CPUs [27]. Public GitHub runners have 4 vCPUs, private ones 2 [28]; horus-slurm is public. The cluster sets `SlurmdParameters=config_overrides` [2], so a worker can also declare more CPUs than it has.

**Release tolerance (case 3).** The compose cluster sets `MinJobAge=300` [2], so a finished job id stays known for 5 minutes. Case 3 needs a lower `MinJobAge` in our own `slurm.conf`.

**Startup time.**
- giovtorres compose, in its own CI: the image build takes 6m39s (we would pull instead), `make up` 38s, workers healthy 11s later [29].
- giovtorres/slurm-docker, in PySlurm CI: pull 20s, `docker compose up -d` 1s [30].
- sind, in its own CI: image build 20s, image pulls 45s [15].
- setup-slurm-action, in Snakemake CI: 1m22s on the runner [31].

**arm64 / amd64.** giovtorres images (both), nathanhess/slurm and sind-node publish amd64 + arm64 [6][8][21][11]. giovtorres compose and sind test both arches in CI [4][15].

**Digest pinning.**
- `giovtorres/slurm-docker-cluster:26.05.2-3.0.0` is the index `sha256:8b92cbd3bf7e51f910c7266c1dac13cf0c59059fa8db3c385bec76952a79b47b` [6].
- `ghcr.io/gsi-hpc/sind-node:26.05` is currently `sha256:ca270c956e1b4c229b7ca9d34b894d5aebcb6a6eb931d9a3e3d3b7cb6bfde277`, from `docker buildx imagetools inspect` on 2026-10-17. sind's own docs say its tags move and to pin by digest [11].

The compose images use a `<slurm>-<release>` tag that the publish workflow does not overwrite [32]. Only `latest` and `26.05.2` move.

## Recommendation

Use the giovtorres/slurm-docker-cluster image, pinned by digest, with a small compose file of our own under `tests/integration/`. Model it on theirs: mariadb, slurmdbd, slurmctld, two `slurmd -Z` workers. It is the only option that matches the plan's slurmctld + 2 slurmd + slurmdbd layout as separate containers, and it is the best maintained. Its CI already runs on `ubuntu-latest` and `ubuntu-24.04-arm` [4].

- Pull, do not build: `docker compose up -d --no-build` with `image: giovtorres/slurm-docker-cluster@sha256:8b92...`. This avoids the 6.5 min build [29].
- Mount our own `slurm.conf` to lower `MinJobAge` for case 3, and keep `config_overrides` [2].
- Wait on the same health checks their CI uses (`scontrol ping`, `pidof slurmd`) [2][4], then `sinfo` until both workers are idle. Skip the suite if that fails, per the plan.
- One job, `ubuntu-latest`, `timeout-minutes: 20`, separate from the unit job. Add arm64 later only if needed.
- Run the client side (case 6, `SlurmTarget` round trip) with `docker exec` into `slurmctld`, which has the Slurm CLIs [3].

If cases 1 or 2 turn out to depend on cgroup tracking, switch to sind via `GSI-HPC/sind-action@v2` with `sind-node` pinned by digest [14][11]. It is the closest to a real cluster (systemd, cgroup v2, no privileged containers) [12], but it is young: v0.x and 3 stars [11][16]. Avoid setup-slurm-action: it changes the runner itself and has not been updated since 2025-07 [17][19].

## Sources

1. giovtorres/slurm-docker-cluster README: https://github.com/giovtorres/slurm-docker-cluster/blob/main/README.md
2. giovtorres/slurm-docker-cluster `docker-compose.yml`, `config/26.05/slurm.conf`, `config/common/cgroup.conf`: https://github.com/giovtorres/slurm-docker-cluster/tree/main/config
3. giovtorres/slurm-docker-cluster `test_cluster.sh` (sacctmgr/sacct/srun checks run through `docker exec slurmctld`): https://github.com/giovtorres/slurm-docker-cluster/blob/main/test_cluster.sh
4. giovtorres/slurm-docker-cluster `.github/workflows/test.yml`: https://github.com/giovtorres/slurm-docker-cluster/blob/main/.github/workflows/test.yml
5. GitHub API `repos/giovtorres/slurm-docker-cluster` and latest release 3.0.0: https://github.com/giovtorres/slurm-docker-cluster/releases/tag/3.0.0
6. Docker Hub tags, giovtorres/slurm-docker-cluster: https://hub.docker.com/r/giovtorres/slurm-docker-cluster/tags
7. giovtorres/slurm-docker README: https://github.com/giovtorres/slurm-docker/blob/main/README.md
8. Docker Hub tags, giovtorres/slurm-docker: https://hub.docker.com/r/giovtorres/slurm-docker/tags
9. giovtorres/slurm-docker `config/26.05/slurm.conf`: https://github.com/giovtorres/slurm-docker/blob/main/config/26.05/slurm.conf
10. PySlurm `.github/workflows/pyslurm.yml` and `docker-compose.yml`: https://github.com/PySlurm/pyslurm/blob/main/.github/workflows/pyslurm.yml
11. sind docs, "Building images" (official tags, multi-arch, digest pinning): https://github.com/GSI-HPC/sind/blob/main/docs/content/container-images/building-images.md ; release v0.12.0: https://github.com/GSI-HPC/sind/releases
12. sind README: https://github.com/GSI-HPC/sind/blob/main/README.md
13. sind docs, "Slurm Configuration": https://github.com/GSI-HPC/sind/blob/main/docs/content/architecture/slurm-config.md
14. GSI-HPC/sind-action README: https://github.com/GSI-HPC/sind-action
15. sind CI run, "Integration Test (slurm-26-05, linux/arm64)": https://github.com/GSI-HPC/sind/actions/runs/38753806751/job/116486919973
16. GitHub API `repos/GSI-HPC/sind` (stars, last push 2026-10-15).
17. koesterlab/setup-slurm-action `action.yml`: https://github.com/koesterlab/setup-slurm-action/blob/main/action.yml
18. snakemake-executor-plugin-slurm `.github/workflows/ci.yml`: https://github.com/snakemake/snakemake-executor-plugin-slurm/blob/main/.github/workflows/ci.yml
19. koesterlab/setup-slurm-action release v1.1.0: https://github.com/koesterlab/setup-slurm-action/releases
20. nathan-hess/docker-slurm README and `dockerfile_base/Dockerfile`: https://github.com/nathan-hess/docker-slurm
21. Docker Hub tags, nathanhess/slurm: https://hub.docker.com/r/nathanhess/slurm/tags
22. Parsl `.github/workflows/parsl+slurm.yaml`: https://github.com/Parsl/parsl/blob/master/.github/workflows/parsl%2Bslurm.yaml
23. Nextflow workflows directory: https://github.com/nextflow-io/nextflow/tree/master/.github/workflows
24. slurm.conf man page, `JobAcctGatherType` ("Unset by default"): https://slurm.schedmd.com/slurm.conf.html#OPT_JobAcctGatherType
25. slurm.conf man page, `ProctrackType`: https://slurm.schedmd.com/slurm.conf.html#OPT_ProctrackType
26. srun man page, `--exact`: https://slurm.schedmd.com/srun.html#OPT_exact
27. giovtorres/slurm-docker-cluster `docker-entrypoint.sh` (`slurmd -Z --conf "Feature=cpu"`): https://github.com/giovtorres/slurm-docker-cluster/blob/main/docker-entrypoint.sh
28. GitHub-hosted runners reference: https://docs.github.com/en/actions/reference/runners/github-hosted-runners
29. giovtorres/slurm-docker-cluster CI run, test-cluster (26.05.2, amd64): https://github.com/giovtorres/slurm-docker-cluster/actions/runs/31336989169/job/93304368708
30. PySlurm CI run, tests (26.05.3-rl10): https://github.com/PySlurm/pyslurm/actions/runs/33923655927/job/101187301144
31. Snakemake plugin CI run, testing: https://github.com/snakemake/snakemake-executor-plugin-slurm/actions/runs/37975665077/job/114443193763
32. giovtorres/slurm-docker-cluster `.github/workflows/publish.yml`: https://github.com/giovtorres/slurm-docker-cluster/blob/main/.github/workflows/publish.yml
