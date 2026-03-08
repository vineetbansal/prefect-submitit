"""Cancellation integration tests."""

from __future__ import annotations

import pytest
from prefect import flow

from prefect_submitit.futures import SlurmJobFailed
from tests.integration.helpers import wait_for_running
from tests.integration.tasks import add, sleep_and_return

pytestmark = pytest.mark.slurm


class TestCancelSingleTask:
    """P0: Single task cancellation."""

    def test_cancel_running_task(self, slurm_runner, slurm_jobs):
        """Cancel a RUNNING task and verify SlurmJobFailed with CANCELLED."""

        @flow(task_runner=slurm_runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            wait_for_running(future, timeout=300)
            assert future.cancel() is True
            return future.result()

        with pytest.raises(SlurmJobFailed, match="CANCELLED"):
            compute()


class TestCancelArray:
    """P1: Job array cancellation."""

    def test_cancel_entire_array(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            futures = sleep_and_return.map(seconds=[120.0] * 5)
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            # Wait for at least one to be running
            wait_for_running(futures[0], timeout=300)
            # Cancel the entire array via first future
            futures[0].cancel()
            results = []
            for f in futures:
                try:
                    results.append(f.result())
                except SlurmJobFailed:
                    results.append("CANCELLED")
            return results

        results = compute()
        assert any(r == "CANCELLED" for r in results)

    def test_cancel_single_array_task(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            futures = sleep_and_return.map(seconds=[120.0] * 5)
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            wait_for_running(futures[2], timeout=300)
            # Cancel only task index 2
            futures[2].cancel_task()
            # Task 2 should be cancelled
            with pytest.raises(SlurmJobFailed, match="CANCELLED"):
                futures[2].result()

        compute()


class TestCancelEdgeCases:
    """P2: Cancellation edge cases."""

    def test_cancel_cancelled_by_uid_detection(self, slurm_runner, slurm_jobs):
        """SLURM may report 'CANCELLED by <uid>'; verify detection."""

        @flow(task_runner=slurm_runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            wait_for_running(future, timeout=300)
            future.cancel()
            try:
                future.result()
            except SlurmJobFailed as e:
                return str(e)
            return "no-error"

        error_msg = compute()
        assert "CANCELLED" in error_msg

    def test_cancel_batch_mid_execution(self, make_slurm_runner, slurm_jobs):
        """Cancel a batch job mid-execution; accept multiple valid outcomes."""
        runner = make_slurm_runner(units_per_worker=5)

        @flow(task_runner=runner)
        def compute():
            futures = sleep_and_return.map(seconds=[10.0] * 5)
            for f in futures:
                slurm_jobs.append(f.slurm_job_future.slurm_job_id)
            # Cancel via underlying SLURM future
            import time

            time.sleep(5)
            futures[0].slurm_job_future.cancel()
            results = []
            for f in futures:
                try:
                    r = f.result()
                    results.append(("ok", r))
                except SlurmJobFailed:
                    results.append(("cancelled", None))
            return results

        results = compute()
        # Either all cancelled, all ok, or mixed — all are valid
        assert len(results) == 5

    def test_cancel_race_with_completion(self, slurm_runner, slurm_jobs):
        """Cancel right after submission — no crash regardless of outcome."""

        @flow(task_runner=slurm_runner)
        def compute():
            future = add.submit(1, 2)
            slurm_jobs.append(future.slurm_job_id)
            future.cancel()
            try:
                result = future.result()
                return ("completed", result)
            except SlurmJobFailed:
                return ("cancelled", None)

        outcome, value = compute()
        # Both outcomes are acceptable
        if outcome == "completed":
            assert value == 3
        else:
            assert outcome == "cancelled"
