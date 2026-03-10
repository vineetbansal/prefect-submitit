"""Integration tests for server lifecycle."""

from __future__ import annotations

import shutil
import time

import pytest

from prefect_submitit.server.config import ServerConfig
from prefect_submitit.server.discovery import health_check, read_discovery

pytestmark = pytest.mark.skipif(
    shutil.which("prefect") is None,
    reason="prefect not on PATH",
)


@pytest.fixture
def isolated_config(tmp_path):
    """Config with all paths under tmp_path to isolate from real data."""
    # Use a random high port to avoid conflicts
    import socket

    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()

    return ServerConfig(
        port=port,
        host="localhost",
        api_url=f"http://localhost:{port}/api",
        data_dir=tmp_path,
        discovery_file=tmp_path / "server.json",
        pg_data_dir=tmp_path / "postgres",
        pg_port=5433,
        pg_user="prefect",
        pg_database="prefect",
        log_dir=tmp_path / "logs",
    )


class TestStartStopSqlite:
    def test_lifecycle(self, isolated_config):
        """Start a SQLite server in background, verify health, stop it."""
        from prefect_submitit.server import prefect_proc

        pid = prefect_proc.start(isolated_config, background=True, sqlite=True)

        try:
            assert pid > 0
            assert health_check(isolated_config.api_url)

            info = read_discovery(isolated_config.discovery_file)
            assert info is not None
            assert info["backend"] == "sqlite"
            assert info["port"] == isolated_config.port
        finally:
            prefect_proc.stop(isolated_config, force=True)

        # Give process time to die
        time.sleep(1)
        assert not health_check(isolated_config.api_url, timeout=2)
