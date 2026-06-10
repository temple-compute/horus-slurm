#
# horus-slurm
# Copyright (c) 2026 Temple Compute
#
# MIT License
#
"""
Implementation of the SlurmExecutor.
"""

from typing import ClassVar

from horus_runtime.core.target.base import BaseTarget

from horus_slurm.i18n import tr as _


class SlurmExecutor(BaseTarget):
    """
    Executor for running tasks on Slurm clusters.
    """

    kind: str = "slurm"
    kind_name: ClassVar[str] = _("Slurm Target")
    kind_description: ClassVar[str] = _("Run tasks on Slurm clusters.")

    # TODO: Implement
