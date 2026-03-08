"""SLURM failure mode integration tests."""

from __future__ import annotations

import subprocess

import pytest
from prefect import flow

from prefect_submitit import SlurmTaskRunner
from prefect_submitit.futures import SlurmJobFailed
from tests.integration.helpers import wait_for_running
from tests.integration.tasks import fail_with, return_unpicklable, sleep_and_return

pytestmark = pytest.mark.slurm


class TestExceptionPropagation:
    """P0: Exception propagation through SLURM."""

    def test_task_exception_propagation(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = fail_with.submit(error_type="ValueError", message="test error")
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        with pytest.raises(ValueError, match="test error"):
            compute()


class TestSlurmFailureModes:
    """P1-P2: SLURM-level failure detection."""

    def test_timeout_detection(self, slurm_config, slurm_jobs):
        """Job exceeding time_limit triggers TIMEOUT state detection.

        Uses a 1-minute time limit with a task that sleeps 120s.
        """
        extra_kwargs = {}
        if slurm_config.account:
            extra_kwargs["slurm_account"] = slurm_config.account
        if slurm_config.qos:
            extra_kwargs["slurm_qos"] = slurm_config.qos

        runner = SlurmTaskRunner(
            partition=slurm_config.partition,
            time_limit="00:01:00",
            mem_gb=slurm_config.mem_gb,
            gpus_per_node=0,
            poll_interval=2.0,
            max_poll_time=slurm_config.max_wait + 120,
            log_folder=str(slurm_config.log_dir / "slurm_logs"),
            **extra_kwargs,
        )

        @flow(task_runner=runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        with pytest.raises(SlurmJobFailed, match="TIMEOUT"):
            compute()

    def test_unpicklable_return_raises_cleanly(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = return_unpicklable.submit()
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        with pytest.raises((TypeError, SlurmJobFailed)):
            compute()

    def test_invalid_partition_fails_cleanly(self, slurm_config, slurm_jobs):
        runner = SlurmTaskRunner(
            partition="nonexistent_partition_xyz",
            time_limit="00:05:00",
            mem_gb=1,
            gpus_per_node=0,
            poll_interval=2.0,
            max_poll_time=60,
            log_folder=str(slurm_config.log_dir / "slurm_logs"),
        )

        @flow(task_runner=runner)
        def compute():
            from tests.integration.tasks import add

            future = add.submit(1, 2)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        with pytest.raises((SlurmJobFailed, RuntimeError, TimeoutError)):
            compute()

    def test_job_no_output_raises_slurm_job_failed(self, slurm_runner, slurm_jobs):
        """SIGKILL prevents result writing -> SlurmJobFailed."""

        @flow(task_runner=slurm_runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            wait_for_running(future, timeout=300)
            # SIGKILL: no chance to write results
            subprocess.run(
                ["scancel", "--signal=SIGKILL", future.slurm_job_id],
                check=True,
                capture_output=True,
            )
            return future.result()

        with pytest.raises(SlurmJobFailed):
            compute()
