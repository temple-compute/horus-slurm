# Review: integration tests, security and CI safety

Scope: `git diff 116c7dc...HEAD -- tests Makefile .github` on
`chdominguez/slurm-itests`. Read-only review; no code changed.

Counts: High 0, Medium 1, Low 5, Info 3.

## Findings

### M1. Org token in `~/.gitconfig` while PR code runs (Medium, verified by reading)

`.github/workflows/python-ci.yaml:87` writes `secrets.ACTIONS_TOKEN` into the
global git config, and it stays there for `uv sync`, `make integration` and
pytest, all of which run code from the PR. The workflow also has no
`permissions:` block, so `GITHUB_TOKEN` gets the repo default (maybe write),
and `actions/checkout` keeps it in `.git/config`.

Fork PRs are safe: `pull_request` gives them no secrets and a read-only token.
But anyone who can push a branch here can open a PR that reads the token and
sends it out. If `ACTIONS_TOKEN` reaches other private repos, that is more
access than push rights to this repo give. The `lint` and `test` jobs already
do this; the new job adds a third copy.

Fix:
- Add `permissions: { contents: read }` at the top of the workflow.
- Make `ACTIONS_TOKEN` a fine-grained, read-only token (or a GitHub App
  token) limited to the private dependency repos.
- Remove it once dependencies are installed:
  `git config --global --unset-all url."https://${{ secrets.ACTIONS_TOKEN }}@github.com/".insteadOf`.
  This is partial, because `uv sync` can still run build hooks.
- Set `persist-credentials: false` on `actions/checkout`.

### L1. Privileged worker containers (Low, verified by reading and `docker compose config`)

`tests/integration/cluster/compose.yaml:42`: `c1` and `c2` run privileged,
from a third-party image (`giovtorres/slurm-docker-cluster`). On GitHub-hosted
runners this gives a malicious PR nothing new: the runner is a throwaway VM
and the PR's code already has passwordless sudo. The real risk is
**self-hosted runners**, where it allows a container escape onto a long-lived
host, and developers running `make integration` on their own machines.
`slurmctld`, `slurmdbd` and `mysql` are not privileged. No container mounts
the docker socket or uses the host network or PID namespace.

Fix: try dropping `privileged` and give slurmd only what it needs. Either
mount `/sys/fs/cgroup:/sys/fs/cgroup:rw` with `cgroup: host` (add
`cap_add: [SYS_ADMIN]` if needed), or turn the plugin off in `cgroup.conf`
(`CgroupPlugin=disabled`, if Slurm 26.05 accepts it). Both are untested.
Also add a comment that the `integration` job must stay on `ubuntu-latest`
and never run on self-hosted runners.

### L2. Actions pinned by tag, not SHA (Low, verified by reading)

`python-ci.yaml:75,78,110`: `actions/checkout@v6`, `actions/setup-python@v6`
and `actions/upload-artifact@v6` use movable tags. Only `astral-sh/setup-uv`
is pinned by SHA. The new job repeats the existing pattern.
Fix: pin all three by commit SHA with a `# v6.x.y` comment, as is already
done for setup-uv.

### L3. Predictable `/tmp` default for `ITEST_DIR` (Low, suspected; local runs only)

`Makefile:13` and `tests/integration/conftest.py:33` default to
`/tmp/horus-slurm-itest-<uid>`. `up.sh:14` (`mkdir -p`) and the fixture
(`mkdir(parents=True)`) accept the directory even if another user created it
first. The pilot runs whatever launchers it finds below that directory.
`SlurmTarget._make_private_dir` checks only the allocation's direct parent, so
a user who owns `ITEST_DIR` can swap a per-test directory after the check.
This only matters on shared multi-user hosts. CI uses `$RUNNER_TEMP`.
Fix: in `up.sh`, after `mkdir -p`, run
`[ -O "$ITEST_DIR" ] && [ ! -L "$ITEST_DIR" ] && chmod 700 "$ITEST_DIR"`
and fail otherwise. Or default to `${XDG_RUNTIME_DIR:-/tmp}`.

### L4. Shell quoting in the Makefile and `up.sh` (Low, verified by reading)

- `Makefile:52-56`: `$(ITEST_DIR)` and `$(ITEST_PROJECT)` are not quoted, so
  a path with spaces breaks and shell metacharacters run on the host. Only
  the person running make sets these values. Fix: write
  `ITEST_DIR="$(ITEST_DIR)"` and `-p "$(ITEST_PROJECT)"`.
- `up.sh:21`: `$ITEST_DIR` goes inside `'...'` in a `bash -c` string, so a
  `'` in the path can inject commands into the container shell. Fix: pass it
  as an argument:
  `bash -c 'getent passwd "$1" >/dev/null || useradd -u "$1" -M -d "$2" itest' _ "$uid" "$ITEST_DIR"`.
- `compose.yaml:10` bind-mounts `ITEST_DIR` read-write into privileged
  containers. An overly broad value (for example `$HOME`) would expose all of
  it. Consider refusing `/`, `$HOME` and other short paths in `up.sh`.

### L5. Feature-branch trigger (Low, verified by reading)

`python-ci.yaml:5` adds `feat/shared-allocation`. It is still
`pull_request`, not `pull_request_target`, so fork PRs run without secrets
and with a read-only token. This is safe. Remove the branch from the list
when it merges, so stale branch filters do not pile up. There is no `push`
trigger, so the suite does not run again after a merge into that branch.

### I1. Images pinned by digest (Info, verified with `docker compose config`)

Every image is pinned by digest: the Slurm image (all four services) and
`mariadb`. `up.sh` passes `--no-build`, and nothing else pulls images. Add a
`# <tag>` comment next to each digest so upgrades are easy to read. Consider
mirroring the privileged third-party image to an org registry.

### I2. Logs and artifacts (Info, verified with a throwaway pytest under /tmp)

Only `integration-results.xml` is uploaded. The `pilot.log` sections that
`conftest.py:63` attaches do **not** reach the JUnit XML (pytest's default is
`junit_logging=no`). They do appear in the job's stdout, as do the
`Dump cluster logs` output and the `cat` of `pilot.log`. These logs hold Slurm
daemon output and the output of test commands. They contain no host
environment: `ExecInner` passes only the explicit `env` to `docker exec`, and
the compose files contain only a throwaway MariaDB password. No secrets
found. Note that job logs are readable by anyone who can read the repo.

### I3. Network (Info, verified with `docker compose config`)

No `ports:` are published. Everything stays on the compose project network.
The hardcoded `MARIADB_PASSWORD: password` (`compose.yaml:20`) is fine for a
throwaway, unexposed database.
