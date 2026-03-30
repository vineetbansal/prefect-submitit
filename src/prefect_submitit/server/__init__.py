"""Prefect server management utilities."""

from __future__ import annotations

from prefect_submitit.server import prefect_proc
from prefect_submitit.server.config import ServerConfig, make_config
from prefect_submitit.server.discovery import (
    health_check,
    resolve_api_url,
    wait_for_healthy,
)

__all__ = [
    "ServerConfig",
    "health_check",
    "make_config",
    "resolve_api_url",
    "start",
    "status",
    "stop",
    "wait_for_healthy",
]


def start(
    *,
    background: bool = True,
    sqlite: bool = False,
    port: int | None = None,
    pg_port: int | None = None,
) -> int:
    """Start the Prefect server.

    Args:
        background: If True, daemonize the server process. Defaults to True.
        sqlite: If True, use SQLite backend instead of PostgreSQL.
        port: Prefect server port. Defaults to UID-based port.
        pg_port: PostgreSQL port. Defaults to UID-based port.

    Returns:
        PID of the server process.
    """
    config = make_config(port=port, pg_port=pg_port)
    return prefect_proc.start(config, background=background, sqlite=sqlite)


def stop(*, force: bool = False) -> None:
    """Stop the Prefect server and PostgreSQL.

    Args:
        force: If True, use SIGKILL as fallback.
    """
    config = make_config()
    prefect_proc.stop(config, force=force)


def status() -> dict | None:
    """Get the current server status.

    Returns:
        Status dict with discovery info and health, or None if not running.
    """
    config = make_config()
    return prefect_proc.status(config)
