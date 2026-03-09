"""Tests for Prefect server process management."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from prefect_submitit.server.config import ServerConfig
from prefect_submitit.server.prefect_proc import _build_env, _kill_pid


@pytest.fixture
def tmp_config(tmp_path):
    return ServerConfig(
        port=4242,
        host="test.local",
        api_url="http://test.local:4242/api",
        data_dir=tmp_path,
        discovery_file=tmp_path / "server.json",
        pg_data_dir=tmp_path / "postgres",
        pg_port=5433,
        pg_user="prefect",
        pg_database="prefect",
        log_dir=tmp_path / "logs",
    )


class TestBuildEnv:
    def test_postgres_env(self, tmp_config):
        env = _build_env(tmp_config, sqlite=False)
        assert env["PREFECT_SERVER_API_HOST"] == "0.0.0.0"
        assert env["PREFECT_UI_API_URL"] == "http://localhost:4242/api"
        assert env["PREFECT_SERVER_DATABASE_SQLALCHEMY_POOL_SIZE"] == "50"
        assert env["PREFECT_SERVER_DATABASE_SQLALCHEMY_MAX_OVERFLOW"] == "120"
        assert (
            "postgresql+asyncpg://prefect@localhost:5433/prefect"
            in env["PREFECT_API_DATABASE_CONNECTION_URL"]
        )

    def test_sqlite_env(self, tmp_config):
        env = _build_env(tmp_config, sqlite=True)
        assert env["PREFECT_SERVER_API_HOST"] == "0.0.0.0"
        assert "PREFECT_API_DATABASE_CONNECTION_URL" not in env


class TestKillPid:
    def test_nonexistent_pid(self):
        # Should not raise for a PID that doesn't exist
        _kill_pid(999999999, timeout=0.1)

    def test_graceful_kill(self):
        with (
            patch("prefect_submitit.server.prefect_proc.os.kill") as mock_kill,
        ):
            # First call (SIGTERM) succeeds, second call (signal 0 check) raises
            mock_kill.side_effect = [None, ProcessLookupError]
            _kill_pid(12345, timeout=1.0)

            import signal

            mock_kill.assert_any_call(12345, signal.SIGTERM)
