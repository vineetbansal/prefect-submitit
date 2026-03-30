"""Tests for server configuration."""

from __future__ import annotations

import socket
from pathlib import Path
from unittest.mock import patch

import pytest

from prefect_submitit.server.config import (
    _PG_BASE_PORT,
    _PREFECT_BASE_PORT,
    DEFAULT_DATA_DIR,
    _user_port_offset,
    default_host,
    default_pg_port,
    default_port,
    make_config,
    require_binary,
)


class TestUserPortOffset:
    def test_deterministic(self):
        assert _user_port_offset() == _user_port_offset()

    def test_range_for_various_uids(self):
        for uid in [0, 1, 999, 1000, 1001, 5432, 65534]:
            with patch("prefect_submitit.server.config.os.getuid", return_value=uid):
                offset = _user_port_offset()
                assert 0 <= offset < 800, f"uid={uid}, offset={offset}"

    def test_correlated_ports(self):
        for uid in [0, 1, 999, 1000, 1001, 5432, 65534]:
            with patch("prefect_submitit.server.config.os.getuid", return_value=uid):
                offset = _user_port_offset()
                assert default_port() == _PREFECT_BASE_PORT + 2 * offset
                assert default_pg_port() == _PG_BASE_PORT + 2 * offset


class TestDefaultPort:
    def test_range(self):
        port = default_port()
        assert 4200 <= port <= 5798

    def test_deterministic(self):
        assert default_port() == default_port()

    def test_always_even(self):
        assert default_port() % 2 == 0


class TestDefaultPgPort:
    def test_range(self):
        port = default_pg_port()
        assert 5433 <= port <= 7031

    def test_deterministic(self):
        assert default_pg_port() == default_pg_port()

    def test_always_odd(self):
        assert default_pg_port() % 2 == 1


class TestPortParity:
    def test_parity_invariant_across_uids(self):
        for uid in range(1000, 1100):
            with patch("prefect_submitit.server.config.os.getuid", return_value=uid):
                assert default_port() % 2 == 0, f"uid={uid}"
                assert default_pg_port() % 2 == 1, f"uid={uid}"

    def test_no_overlap(self):
        for uid in range(1000, 1800):
            with patch("prefect_submitit.server.config.os.getuid", return_value=uid):
                assert default_port() != default_pg_port()


class TestDefaultHost:
    def test_returns_string(self):
        host = default_host()
        assert isinstance(host, str)
        assert len(host) > 0

    def test_uses_fqdn_when_resolvable(self):
        with (
            patch(
                "prefect_submitit.server.config.socket.getfqdn",
                return_value="node01.cluster.local",
            ),
            patch("prefect_submitit.server.config.socket.getaddrinfo") as mock_gai,
        ):
            result = default_host()
            assert result == "node01.cluster.local"
            mock_gai.assert_called_once_with("node01.cluster.local", None)

    def test_falls_back_on_unresolvable_fqdn(self):
        with (
            patch(
                "prefect_submitit.server.config.socket.getfqdn",
                return_value="bad.ptr.record",
            ),
            patch(
                "prefect_submitit.server.config.socket.getaddrinfo",
                side_effect=socket.gaierror,
            ),
            patch(
                "prefect_submitit.server.config.socket.gethostname",
                return_value="node01",
            ),
            patch(
                "prefect_submitit.server.config.socket.gethostbyname",
                return_value="10.0.0.1",
            ) as mock_ghbn,
        ):
            result = default_host()
            assert result == "10.0.0.1"
            mock_ghbn.assert_called_once_with("node01")


class TestMakeConfig:
    def test_defaults(self):
        config = make_config()
        assert config.port == default_port()
        assert config.pg_port == default_pg_port()
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
