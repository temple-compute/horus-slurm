#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
A value that would smuggle an option onto its #SBATCH line is rejected.
"""

from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import ValidationError

from horus_slurm.target.slurm import ALLOC_DIR, SlurmTarget

from .conftest import sh


@pytest.mark.integration
async def test_partition_with_option_rejected(
    shared_target: Callable[..., SlurmTarget], itest_dir: Path
) -> None:
    """The target never exists, so nothing reaches sbatch."""
    with pytest.raises(ValidationError, match="partition"):
        shared_target(partition="debug --qos=x")

    assert not (itest_dir / ALLOC_DIR).exists()
    rc, out, err = await sh(shared_target(), "squeue --me -h")
    assert rc == 0, err
    assert out.strip() == ""
