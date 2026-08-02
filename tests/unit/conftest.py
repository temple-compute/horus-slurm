#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Shared fakes for the Slurm unit tests.

A Slurm target is mostly a thing that shells out to ``sbatch``/``squeue``/
``sacct`` on some other target, so the fake here is the *transport*: a real
``LocalTarget`` (so the filesystem operations are genuine) whose command
execution is canned and recorded.
"""

from collections.abc import AsyncGenerator
from typing import ClassVar

import pytest
from horus_builtin.target.local import LocalTarget
from horus_runtime.core.target.channel import ChannelProcess, StreamName
from pydantic import PrivateAttr


class FakeProcess(ChannelProcess):
    """A finished process with canned output."""

    def __init__(
        self, stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0
    ) -> None:
        self._stdout = stdout
        self._stderr = stderr
        self._returncode = returncode

    @property
    def returncode(self) -> int | None:
        """Always finished."""
        return self._returncode

    async def wait(self) -> int:
        """Already done."""
        return self._returncode

    async def communicate(self) -> tuple[bytes, bytes]:
        """The canned output."""
        return self._stdout, self._stderr

    def kill(self) -> None:
        """Nothing to kill."""

    def signal(self, sig: int) -> None:
        """Nothing to signal."""
        del sig

    async def stream(self) -> AsyncGenerator[tuple[StreamName, bytes]]:
        """No streaming in the fake."""
        return
        yield  # pragma: no cover - makes this an async generator


class FakeInner(LocalTarget):
    """
    A real local target for file operations, with canned command execution.

    ``responses`` maps a command prefix (``"sbatch"``, ``"squeue"``, ...) to
    the process that running it should produce; ``commands`` records every
    command that was run, so a test can assert *what* was asked of Slurm.
    Both are private attributes: a ``FakeProcess`` is not something pydantic
    can build a schema for, and neither is part of the target's config.
    """

    add_to_registry: ClassVar[bool] = False

    _responses: dict[str, FakeProcess] = PrivateAttr(default_factory=dict)
    _commands: list[str] = PrivateAttr(default_factory=list)

    @property
    def commands(self) -> list[str]:
        """Every command this transport was asked to run, in order."""
        return self._commands

    def responds(self, **responses: FakeProcess) -> "FakeInner":
        """Canned answers, keyed by command prefix. Chainable."""
        self._responses.update(responses)
        return self

    async def run_command_sync(
        self,
        cmd: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
    ) -> ChannelProcess:
        """Record *cmd* and answer from the canned responses."""
        del cwd, env
        self._commands.append(cmd)
        for prefix, proc in self._responses.items():
            if cmd.startswith(prefix):
                return proc
        return FakeProcess()

    def commands_starting(self, prefix: str) -> list[str]:
        """Every recorded command beginning with *prefix*."""
        return [c for c in self._commands if c.startswith(prefix)]


@pytest.fixture
def inner(tmp_path: object) -> FakeInner:
    """A transport that accepts a submission and reports job 12345."""
    return FakeInner(working_directory=str(tmp_path)).responds(
        sbatch=FakeProcess(stdout=b"12345\n")
    )
