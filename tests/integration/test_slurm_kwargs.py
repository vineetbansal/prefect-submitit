"""Integration tests verifying per-task slurm_kwargs overrides are applied on SLURM."""

from __future__ import annotations

import pytest
from prefect import flow

from prefect_submitit.task import task as slurm_task
from tests.integration.tasks import add, get_slurm_cpus_per_task

pytestmark = pytest.mark.slurm


# ---------------------------------------------------------------------------
# Module-level tasks with _slurm_kwargs attached
# ---------------------------------------------------------------------------


@slurm_task(slurm_kwargs={"cpus_per_task": 2})
def get_cpus_with_override(x: int = 0) -> str | None:
    """Report SLURM_CPUS_PER_TASK; submitted with cpus_per_task=2 override.

    Args:
        x: Ignored; exists so this task can be used with .map().
    """
    import os

    return os.environ.get("SLURM_CPUS_PER_TASK")


class TestSubmitSlurmKwargsOverride:
    """Verify that per-task slurm_kwargs are applied for submit()."""

    def test_cpus_override_applied(self, make_slurm_runner, slurm_jobs):
        """Task with cpus_per_task=2 override sees SLURM_CPUS_PER_TASK=2."""
        # Runner default is 1 CPU; task override requests 2.
        runner = make_slurm_runner(cpus_per_task=1)

        @flow(task_runner=runner)
        def compute():
            future = get_cpus_with_override.submit()
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        result = compute()
        assert result == "2", f"Expected SLURM_CPUS_PER_TASK=2, got {result!r}"

    def test_override_does_not_affect_subsequent_task(self, make_slurm_runner, slurm_jobs):
        """Runner defaults are restored after a per-task override."""
        runner = make_slurm_runner(cpus_per_task=1)

        @flow(task_runner=runner)
        def compute():
            # Submit overriding task first, then a plain task.
            future_override = get_cpus_with_override.submit()
            slurm_jobs.append(future_override.slurm_job_id)

            future_plain = get_slurm_cpus_per_task.submit()
            slurm_jobs.append(future_plain.slurm_job_id)

            return future_override.result(), future_plain.result()

        cpus_override, cpus_plain = compute()
        assert cpus_override == "2", f"Expected override=2, got {cpus_override!r}"
        # Plain task should inherit the runner default (1 CPU).
        assert cpus_plain == "1", f"Expected plain=1, got {cpus_plain!r}"

    def test_no_override_uses_runner_default(self, make_slurm_runner, slurm_jobs):
        """Task without _slurm_kwargs uses the runner's cpus_per_task."""
        runner = make_slurm_runner(cpus_per_task=1)

        @flow(task_runner=runner)
        def compute():
            future = get_slurm_cpus_per_task.submit()
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        result = compute()
        assert result == "1", f"Expected SLURM_CPUS_PER_TASK=1, got {result!r}"


class TestMapSlurmKwargsOverride:
    """Verify that per-task slurm_kwargs are applied for map()."""

    def test_cpus_override_applied_in_map(self, make_slurm_runner, slurm_jobs):
        """All array tasks see the overridden SLURM_CPUS_PER_TASK."""
        runner = make_slurm_runner(cpus_per_task=1)

        @flow(task_runner=runner)
        def compute():
            # get_cpus_with_override has cpus_per_task=2 override; x is ignored
            futures = get_cpus_with_override.map(x=[1, 2, 3])
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert all(
            r == "2" for r in results
        ), f"Expected all SLURM_CPUS_PER_TASK=2, got {results!r}"

    def test_override_does_not_affect_subsequent_map(self, make_slurm_runner, slurm_jobs):
        """Runner defaults are restored after a per-task override map."""
        runner = make_slurm_runner(cpus_per_task=1)

        @flow(task_runner=runner)
        def compute():
            futures_override = get_cpus_with_override.map(x=[1, 2])
            for f in futures_override:
                slurm_jobs.append(f.slurm_job_id)

            futures_plain = get_slurm_cpus_per_task.map([None, None])
            for f in futures_plain:
                slurm_jobs.append(f.slurm_job_id)

            override_results = [f.result() for f in futures_override]
            plain_results = [f.result() for f in futures_plain]
            return override_results, plain_results

        cpus_override, cpus_plain = compute()
        assert all(
            r == "2" for r in cpus_override
        ), f"Expected override=2, got {cpus_override!r}"
        assert all(
            r == "1" for r in cpus_plain
        ), f"Expected plain=1, got {cpus_plain!r}"
