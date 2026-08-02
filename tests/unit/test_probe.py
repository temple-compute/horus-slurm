#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Unit tests for the Slurm resource probe: parsing what sacct says, finding
the job it refers to, and being the probe that gets chosen at all.
"""

from pathlib import Path
from typing import Any

import pytest
from horus_resource_monitor.probe.base import select_probe
from horus_resource_monitor.wrapper import SAMPLES_FILE
from horus_runtime.core.resources import ProcessTreeScope

from horus_slurm.probe import (
    SlurmJobProbe,
    parse_cpu_seconds,
    parse_sacct,
    parse_size_kb,
)
from horus_slurm.resources import SlurmJobScope

from .conftest import FakeInner, FakeProcess


def _task(inner: FakeInner, tmp_path: Path) -> Any:
    """A stand-in task: a probe only needs an id, a target and a directory."""

    class _Task:
        id = "t"
        name = "t"
        side_artifacts_dir = str(tmp_path)
        target = inner

    return _Task()


@pytest.mark.unit
class TestSelection:
    """The bug this probe exists to fix."""

    def test_a_slurm_job_gets_the_slurm_probe(self, inner: FakeInner) -> None:
        """
        Before this, a Slurm task on a login-node orchestrator matched
        ProcessTreeProbe, which walked a *job id* as if it were a pid.
        """
        probe = select_probe(SlurmJobScope(job_id_file="/w/.id"), inner)

        assert isinstance(probe, SlurmJobProbe)

    def test_the_sampler_is_left_switched_on(self, inner: FakeInner) -> None:
        """
        `writes_own_log` is what makes the resource monitor inject its
        sampler into the command, and the command runs on the compute node.
        Without it the series is suppressed before it is ever written.
        """
        probe = select_probe(SlurmJobScope(), inner)

        assert probe is not None
        assert probe.writes_own_log

    def test_ordinary_process_trees_are_left_alone(
        self, inner: FakeInner
    ) -> None:
        """This probe must not capture work that is not a Slurm job."""
        assert not SlurmJobProbe.supports(ProcessTreeScope(), inner)


@pytest.mark.unit
class TestSizeParsing:
    """sacct sizes carry a unit, or default to kilobytes."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("1234K", 1234.0),
            ("1234", 1234.0),
            ("2M", 2048.0),
            ("1.5G", 1.5 * 1024 * 1024),
            ("1T", 1024.0**3),
            ("", None),
            ("garbage", None),
            ("12P", None),
        ],
    )
    def test_parse_size_kb(self, text: str, expected: float | None) -> None:
        """Recognised or refused, never a wrong number."""
        assert parse_size_kb(text) == expected


@pytest.mark.unit
class TestDurationParsing:
    """TotalCPU is `[DD-[HH:]]MM:SS[.mmm]` and nothing else."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("00:30", 30.0),
            ("02:00", 120.0),
            ("01:00:00", 3600.0),
            ("1-00:00:00", 86400.0),
            ("00:01.500", 1.5),
            ("", None),
            ("nonsense", None),
        ],
    )
    def test_parse_cpu_seconds(
        self, text: str, expected: float | None
    ) -> None:
        """Recognised or refused, never a wrong number."""
        assert parse_cpu_seconds(text) == expected


@pytest.mark.unit
class TestSacctParsing:
    """The numbers live on the job's steps, not on the job row."""

    def test_takes_the_peak_across_steps(self) -> None:
        """
        A job reports several rows and only the steps carry MaxRSS, so the
        largest of each column is the job's true peak.
        """
        out = "|00:10:00|\n1024K|00:02:00|\n4096K|00:05:00|\n"

        assert parse_sacct(out) == {"rss_kb": 4096.0, "cpu_s": 600.0}

    @pytest.mark.parametrize("text", ["", "\n", "no-pipes-here"])
    def test_nothing_usable_is_none(self, text: str) -> None:
        """Accounting that says nothing must not become a zero reading."""
        assert parse_sacct(text) is None


@pytest.mark.unit
class TestJobIdResolution:
    """The id appears only once the target has submitted."""

    async def test_reads_the_id_file(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Once written, the id is picked up and remembered."""
        id_file = tmp_path / ".id"
        id_file.write_text("4242\n")
        probe = SlurmJobProbe()
        await probe.start(
            _task(inner, tmp_path), SlurmJobScope(job_id_file=str(id_file))
        )

        assert await probe._resolve_id() == "4242"

    async def test_missing_id_file_is_not_an_error(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """Before submission there is nothing to read, which is normal."""
        probe = SlurmJobProbe()
        await probe.start(
            _task(inner, tmp_path),
            SlurmJobScope(job_id_file=str(tmp_path / "absent")),
        )

        assert await probe._resolve_id() is None

    async def test_an_id_given_up_front_is_used(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """No file lookup when the id is already known."""
        probe = SlurmJobProbe()
        await probe.start(_task(inner, tmp_path), SlurmJobScope(job_id="7"))

        assert await probe._resolve_id() == "7"


@pytest.mark.unit
class TestAccounting:
    """What Slurm charged for, added to what the sampler saw."""

    async def test_totals_are_appended_to_the_series(
        self, tmp_path: Path
    ) -> None:
        """
        Sacct is the only source that still reports for a job Slurm killed
        itself, so its totals land as a final row.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sacct=FakeProcess(stdout=b"8192K|00:01:00|\n")
        )
        probe = SlurmJobProbe()
        await probe.start(_task(inner, tmp_path), SlurmJobScope(job_id="55"))
        await probe.stop()

        rows = await probe.drain()
        assert rows[-1]["rss_kb"] == 8192.0
        assert rows[-1]["cpu_s"] == 60.0

    async def test_sacct_runs_on_the_login_node(self, tmp_path: Path) -> None:
        """
        Through `run_command_sync`: this target submits by default, so
        `run_command` would queue a whole job just to read a number.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sacct=FakeProcess(stdout=b"1K|00:01|\n")
        )
        probe = SlurmJobProbe()
        await probe.start(_task(inner, tmp_path), SlurmJobScope(job_id="55"))
        await probe.stop()

        assert inner.commands_starting("sacct") == [
            "sacct -n -P -o MaxRSS,TotalCPU,Elapsed,State,ExitCode -j 55"
        ]

    async def test_silent_accounting_adds_no_row(self, tmp_path: Path) -> None:
        """
        Plenty of clusters run without accounting configured. That is a
        missing number, not a zero one, and never an error.
        """
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sacct=FakeProcess(stdout=b"")
        )
        probe = SlurmJobProbe()
        await probe.start(_task(inner, tmp_path), SlurmJobScope(job_id="55"))
        await probe.stop()

        assert await probe.drain() == []

    async def test_an_unidentifiable_job_is_survivable(
        self, inner: FakeInner, tmp_path: Path
    ) -> None:
        """
        No id means no accounting, and measurement must never be the reason
        a task fails.
        """
        probe = SlurmJobProbe()
        await probe.start(
            _task(inner, tmp_path),
            SlurmJobScope(job_id_file=str(tmp_path / "absent")),
        )
        await probe.stop()

        assert await probe.drain() == []
        assert inner.commands_starting("sacct") == []

    async def test_the_sampler_series_still_comes_through(
        self, tmp_path: Path
    ) -> None:
        """
        The compute-node series is the point; accounting only supplements
        it. Both must reach the caller from one drain.
        """
        (tmp_path / SAMPLES_FILE).write_text(
            "epoch,rss_kb,cpu_s,gpu_mem_mb,gpu_util_pct,"
            "io_read_b,io_write_b,attribution\n"
            "1,100,0.5,,,,,exact\n"
        )
        inner = FakeInner(working_directory=str(tmp_path)).responds(
            sacct=FakeProcess(stdout=b"9000K|00:02|\n")
        )
        probe = SlurmJobProbe()
        await probe.start(_task(inner, tmp_path), SlurmJobScope(job_id="55"))
        await probe.stop()

        rows = await probe.drain()
        assert [r["rss_kb"] for r in rows] == [100.0, 9000.0]
