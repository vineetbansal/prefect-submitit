"""Tests for server configuration."""

from __future__ import annotations

from pathlib import Path

import pytest

from prefect_submitit.server.config import (
    DEFAULT_DATA_DIR,
    DEFAULT_PG_PORT,
    default_host,
    default_port,
    make_config,
    require_binary,
)


class TestDefaultPort:
    def test_range(self):
        port = default_port()
        assert 4200 <= port <= 4999

    def test_deterministic(self):
        assert default_port() == default_port()


class TestDefaultHost:
    def test_returns_string(self):
        host = default_host()
        assert isinstance(host, str)
        assert len(host) > 0


class TestMakeConfig:
    def test_defaults(self):
        config = make_config()
        assert config.port == default_port()
        assert config.pg_port == DEFAULT_PG_PORT
        assert config.data_dir == DEFAULT_DATA_DIR
        assert config.pg_user == "prefect"
        assert config.pg_database == "prefect"
        assert f":{config.port}/api" in config.api_url
        assert config.discovery_file == DEFAULT_DATA_DIR / "server.json"
        assert config.pg_data_dir == DEFAULT_DATA_DIR / "postgres"
        assert config.log_dir == DEFAULT_DATA_DIR / "logs"

    def test_custom_port(self):
        config = make_config(port=9999)
        assert config.port == 9999
        assert ":9999/api" in config.api_url

    def test_custom_pg_port(self):
        config = make_config(pg_port=5555)
        assert config.pg_port == 5555

    def test_frozen(self):
        config = make_config()
        with pytest.raises(AttributeError):
            config.port = 1234  # type: ignore[misc]


class TestRequireBinary:
    def test_found(self):
        path = require_binary("python")
        assert Path(path).exists()

    def test_missing(self):
        with pytest.raises(FileNotFoundError, match="not-a-real-binary-xyz"):
            require_binary("not-a-real-binary-xyz")
