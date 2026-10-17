#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Smoke test: the docker Slurm cluster is up and shaped as the tests expect.
"""

import pytest

from .conftest import ExecInner


@pytest.mark.integration
async def test_two_idle_nodes_with_four_cpus(cluster: None) -> None:
    """``sinfo`` shows c1 and c2 idle with 4 CPUs each."""
    del cluster
    proc = await ExecInner(working_directory="/").run_command_sync(
        "sinfo -h -N -o '%N %T %c'"
    )
    out, err = await proc.communicate()
    assert proc.returncode == 0, err
    assert sorted(out.decode().split("\n")[:-1]) == [
        "c1 idle 4",
        "c2 idle 4",
    ]
