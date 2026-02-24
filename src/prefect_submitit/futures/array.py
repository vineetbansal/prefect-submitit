"""SLURM array-task future wrapper."""

from __future__ import annotations

import subprocess
import uuid
from typing import TYPE_CHECKING, Any

from prefect_submitit.futures.base import SlurmPrefectFuture

if TYPE_CHECKING:
    import submitit


class SlurmArrayPrefectFuture(SlurmPrefectFuture):
    """Future for a task within a SLURM job array."""

    def __init__(
        self,
        job: submitit.Job[Any],
        task_run_id: uuid.UUID,
        poll_interval: float,
        max_poll_time: float,
        array_job_id: str,
        array_task_index: int,
        array_size: int,
        fail_on_error: bool = True,
    ):
        super().__init__(job, task_run_id, poll_interval, max_poll_time)
        self._array_job_id = array_job_id
        self._array_task_index = array_task_index
        self._array_size = array_size
        self._fail_on_error = fail_on_error

    @property
    def array_job_id(self) -> str:
        return self._array_job_id

    @property
    def array_task_index(self) -> int:
        return self._array_task_index

    @property
    def array_size(self) -> int:
        return self._array_size

    @property
    def slurm_job_id(self) -> str:
        return f"{self._array_job_id}_{self._array_task_index}"

    def __repr__(self) -> str:
        return (
            f"SlurmArrayPrefectFuture("
            f"job_id={self.slurm_job_id}, "
            f"index={self._array_task_index}/{self._array_size}, "
            f"state={self.state.type})"
        )

    def cancel(self) -> bool:
        try:
            subprocess.run(
                ["scancel", self._array_job_id],
                check=True,
                capture_output=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def cancel_task(self) -> bool:
        try:
            subprocess.run(
                ["scancel", f"{self._array_job_id}_{self._array_task_index}"],
                check=True,
                capture_output=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def result(
        self,
        timeout: float | None = None,
        raise_on_failure: bool | None = None,
    ) -> Any:
        if raise_on_failure is None:
            raise_on_failure = self._fail_on_error
        return super().result(timeout=timeout, raise_on_failure=raise_on_failure)
