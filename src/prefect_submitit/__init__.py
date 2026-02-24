"""SLURM TaskRunner implementation package."""

from __future__ import annotations

from prefect_submitit.futures import (
    SlurmArrayPrefectFuture,
    SlurmBatchedItemFuture,
    SlurmJobFailed,
    SlurmPrefectFuture,
)
from prefect_submitit.runner import (
    ExecutionMode,
    SlurmTaskRunner,
)

__all__ = [
    "ExecutionMode",
    "SlurmArrayPrefectFuture",
    "SlurmBatchedItemFuture",
    "SlurmJobFailed",
    "SlurmPrefectFuture",
    "SlurmTaskRunner",
]
