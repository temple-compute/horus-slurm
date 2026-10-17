#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Fixtures for the integration tests against the docker Slurm cluster.

pytest runs on the host; Slurm commands go into the login container through
``docker exec``. ``ITEST_DIR`` is bind-mounted at the same path in every
container, so file operations stay local. Start the cluster with
``make integration``.
"""

import asyncio
import inspect
import os
import re
import shlex
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from pathlib import Path
from typing import Any, ClassVar

import pytest
from horus_builtin.target.local import LocalTarget
from horus_runtime.core.resources import ResourceRequest
from horus_runtime.core.target.channel import ChannelProcess, JobHandle

from horus_slurm.target.slurm import ALLOC_DIR, SlurmTarget

ITEST_DIR = Path(
    os.environ.get("ITEST_DIR", f"/tmp/horus-slurm-itest-{os.getuid()}")
)
LOGIN = os.environ.get(
    "HORUS_SLURM_ITEST_LOGIN", "horus-slurm-itest-slurmctld-1"
)
_HERE = Path(__file__).parent
_ITEST_DIR_KEY = pytest.StashKey[Path]()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip everything here unless the run opted in."""
    if os.environ.get("HORUS_SLURM_ITEST") == "1":
        return
    skip = pytest.mark.skip(
        reason="set HORUS_SLURM_ITEST=1 or run make integration"
    )
    for item in items:
        if item.path.is_relative_to(_HERE):
            item.add_marker(skip)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """On a failed phase, attach every pilot.log under its itest_dir."""
    del call
    report = yield
    root = item.stash.get(_ITEST_DIR_KEY, None)
    if report.failed and root is not None:
        for log in sorted(root.glob(f"{ALLOC_DIR}/*/pilot.log")):
            report.sections.append((str(log), log.read_text(errors="replace")))
    return report


class ExecInner(LocalTarget):
    """
    A local target whose commands run in the login container as the host
    user. Only the ``env`` it is given is passed, not the host environment.
    """

    add_to_registry: ClassVar[bool] = False

    async def run_command_sync(
        self,
        cmd: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
    ) -> ChannelProcess:
        """Run *cmd* with ``bash -c`` inside the login container."""
        argv = ["docker", "exec", "-u", str(os.getuid())]
        argv += ["-w", cwd or self.resolved_working_directory]
        for key, value in (env or {}).items():
            argv += ["-e", f"{key}={value}"]
        argv += [LOGIN, "bash", "-c", cmd]
        return await super().run_command_sync(shlex.join(argv))


async def _probe() -> tuple[int | None, bytes]:
    proc = await ExecInner(working_directory="/").run_command_sync("sinfo -h")
    try:
        _out, err = await asyncio.wait_for(proc.communicate(), 10)
    except TimeoutError:
        proc.kill()
        return None, b"sinfo -h timed out after 10 s"
    return proc.returncode, err


@pytest.fixture(scope="session")
def cluster() -> None:
    """Fail (not skip) when the run opted in but the cluster is not up."""
    rc, err = asyncio.run(_probe())
    if rc != 0:
        pytest.fail(
            f"cluster not reachable through {LOGIN} (rc={rc}): "
            f"{err.decode(errors='replace').strip()}"
        )


@pytest.fixture
def itest_dir(request: pytest.FixtureRequest, cluster: None) -> Path:
    """A fresh directory on the shared mount, one per test."""
    del cluster
    path = ITEST_DIR / f"{request.node.name}-{uuid.uuid4().hex[:8]}"
    path.mkdir(parents=True)
    request.node.stash[_ITEST_DIR_KEY] = path
    return path


@pytest.fixture
async def shared_target(
    itest_dir: Path,
) -> AsyncGenerator[Callable[..., SlurmTarget]]:
    """
    Build named SlurmTargets on the cluster. Every one is released and the
    user's jobs cancelled afterwards.
    """
    inner = ExecInner(working_directory=str(itest_dir))
    made: list[SlurmTarget] = []

    def make(**fields: Any) -> SlurmTarget:
        defaults: dict[str, Any] = {
            "cpus_per_task": 4,
            "mem": "1G",
            "time_limit": "00:05:00",
        }
        target = SlurmTarget(
            inner=inner,
            working_directory=str(itest_dir),
            **(defaults | fields),
        )
        target._ref = "hpc"
        made.append(target)
        return target

    yield make
    errors: list[Exception] = []
    for target in made:
        try:
            await target.release("run_end")
        except Exception as exc:
            errors.append(exc)
    proc = await inner.run_command_sync("scancel --me")
    await proc.wait()

    # The next test wants idle nodes, and scancel returns before the jobs
    # have left the queue.
    async def queue_empty() -> bool:
        proc = await inner.run_command_sync("squeue --me -h")
        out, _err = await proc.communicate()
        return proc.returncode == 0 and not out.strip()

    await wait_for(queue_empty, 60)
    if errors:
        raise errors[0]


class _Task:
    """The only things the target needs from a task."""

    def __init__(
        self,
        working_dir: str,
        resources: ResourceRequest | None = None,
        *,
        id: str = "task-1",
    ) -> None:
        self.id = id
        self.working_dir = working_dir
        self.resources = resources

    @property
    def side_artifacts_dir(self) -> str:
        """Mirrors ``BaseTask.side_artifacts_dir``."""
        return f"{self.working_dir}/side-artifacts"


async def _launch(
    target: SlurmTarget,
    root: Path,
    name: str,
    cmd: str = "echo hi",
    **resources: Any,
) -> JobHandle:
    task = _Task(
        str(root / name),
        ResourceRequest(**resources) if resources else None,
        id=name,
    )
    target.bind(task)  # type: ignore[arg-type]
    return await target.launch(
        cmd, cwd=task.working_dir, env=None, job_dir=f"{root}/{name}"
    )


async def wait_for(
    predicate: Callable[[], bool | Awaitable[bool]],
    timeout: float,
    interval: float = 1,
) -> None:
    """Poll *predicate* until it holds; fail after *timeout* seconds."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail(f"condition not met within {timeout} s")
        await asyncio.sleep(interval)


async def sh(target: SlurmTarget, cmd: str) -> tuple[int | None, str, str]:
    """Run *cmd* on the login node; its exit code, stdout and stderr."""
    proc = await target.run_command_sync(cmd)
    out, err = await proc.communicate()
    return (
        proc.returncode,
        out.decode(errors="replace"),
        err.decode(errors="replace"),
    )


async def job_state(target: SlurmTarget, job: str) -> str:
    """The job's ``squeue`` state, or "" once it has left the queue."""
    rc, out, err = await sh(target, f"squeue -h -j {job} -o %T")
    # With MinJobAge=5 a finished job is soon unknown to squeue.
    assert rc == 0 or "invalid job id" in err.lower(), err
    return out.strip()


def steps(out: str) -> list[str]:
    """The real step ids in ``squeue -s -o %i`` output."""
    return [s for s in out.split() if re.fullmatch(r"\d+(_\d+)?\.\d+", s)]


async def poll_until(
    target: SlurmTarget, handle: JobHandle, timeout: float
) -> int:
    """The exit code ``poll`` reports within *timeout* seconds."""
    codes: list[int] = []

    async def done() -> bool:
        code = await target.poll(handle)
        if code is not None:
            codes.append(code)
        return code is not None

    await wait_for(done, timeout)
    return codes[0]
