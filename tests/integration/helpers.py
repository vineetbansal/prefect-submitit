"""Helper utilities for SLURM integration tests."""

from __future__ import annotations

import socket
import time

import pytest


def wait_for_running(future, timeout=300, poll=2.0, *, skip_on_timeout=True):
    """Block until a future's SLURM job is in RUNNING state.

    If skip_on_timeout is True (default), calls pytest.skip() on timeout
    rather than failing — a scheduling timeout is a cluster issue, not a
    code bug.
    """
    from prefect.states import Running

    start = time.time()
    while time.time() - start < timeout:
        state = future.state
        if isinstance(state, Running):
            return
        time.sleep(poll)
    msg = f"Job {future.slurm_job_id} did not reach RUNNING within {timeout}s"
    if skip_on_timeout:
        pytest.skip(msg)
    raise TimeoutError(msg)


def find_free_port() -> int:
    """Find an available TCP port using the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]
