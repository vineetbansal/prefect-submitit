"""Environment and context propagation integration tests."""

from __future__ import annotations

import os

import pytest
from prefect import flow

from prefect_submitit.utils import get_cluster_max_array_size
from tests.integration.tasks import get_env_var, get_flow_run_id, print_marker

pytestmark = pytest.mark.slurm


class TestEnvironmentPropagation:
    """P2: Environment variable and context propagation to compute nodes."""

    def test_custom_env_var_propagation(self, slurm_runner, slurm_jobs):
        os.environ["MY_TEST_VAR"] = "hello_from_test"
        try:

            @flow(task_runner=slurm_runner)
            def compute():
                future = get_env_var.submit(name="MY_TEST_VAR")
                slurm_jobs.append(future.slurm_job_id)
                return future.result()

            assert compute() == "hello_from_test"
        finally:
            os.environ.pop("MY_TEST_VAR", None)

    def test_flow_run_context_on_compute_node(
        self, slurm_runner, slurm_jobs, prefect_server
    ):
        if not prefect_server:
            pytest.skip("No Prefect server available")

        flow_run_id_holder = {}

        @flow(task_runner=slurm_runner)
        def compute():
            from prefect.context import FlowRunContext

            ctx = FlowRunContext.get()
            flow_run_id_holder["id"] = str(ctx.flow_run.id)
            future = get_flow_run_id.submit()
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        remote_flow_run_id = compute()
        assert remote_flow_run_id == flow_run_id_holder["id"]


class TestLogCapture:
    """P2: Log capture from compute nodes."""

    def test_logs_contain_output(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = print_marker.submit(marker="INTEGRATION_TEST_MARKER_12345")
            slurm_jobs.append(future.slurm_job_id)
            result = future.result()
            stdout, _stderr = future.logs()
            return result, stdout

        result, stdout = compute()
        assert result == "INTEGRATION_TEST_MARKER_12345"
        assert "INTEGRATION_TEST_MARKER_12345" in stdout


class TestClusterInfo:
    """P2: Cluster information queries."""

    def test_cluster_max_array_size(self, slurm_runner):
        """get_cluster_max_array_size() returns a positive integer on real SLURM."""
        max_size = get_cluster_max_array_size(slurm_runner)
        assert isinstance(max_size, int)
        assert max_size > 0
