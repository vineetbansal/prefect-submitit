"""Server configuration and path resolution."""

from __future__ import annotations

import os
import shutil
import socket
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PG_PORT = 5433
DEFAULT_DATA_DIR = Path.home() / ".prefect-submitit"
PG_USER = "prefect"
PG_DATABASE = "prefect"


def default_port() -> int:
    """Compute a UID-based port to avoid conflicts on shared nodes.

    Returns:
        Port in the range 4200-4999.
    """
    return 4200 + (os.getuid() % 800)


def default_host() -> str:
    """Return the FQDN for cross-node access.

    Returns:
        Fully qualified domain name.
    """
    return socket.getfqdn()


@dataclass(frozen=True)
class ServerConfig:
    """Immutable configuration for a Prefect server instance."""

    port: int
    host: str
    api_url: str
    data_dir: Path
    discovery_file: Path
    pg_data_dir: Path
    pg_port: int
    pg_user: str
    pg_database: str
    log_dir: Path


def make_config(
    port: int | None = None,
    pg_port: int | None = None,
) -> ServerConfig:
    """Build a ServerConfig with computed defaults.

    Args:
        port: Prefect server port. Defaults to UID-based port.
        pg_port: PostgreSQL port. Defaults to 5433.

    Returns:
        Fully populated ServerConfig.
    """
    port = port if port is not None else default_port()
    pg_port = pg_port if pg_port is not None else DEFAULT_PG_PORT
    host = default_host()
    data_dir = DEFAULT_DATA_DIR

    return ServerConfig(
        port=port,
        host=host,
        api_url=f"http://{host}:{port}/api",
        data_dir=data_dir,
        discovery_file=data_dir / "server.json",
        pg_data_dir=data_dir / "postgres",
        pg_port=pg_port,
        pg_user=PG_USER,
        pg_database=PG_DATABASE,
        log_dir=data_dir / "logs",
    )


def require_binary(name: str) -> str:
    """Find a binary on PATH or raise with a clear message.

    Args:
        name: Binary name to locate.

    Returns:
        Absolute path to the binary.

    Raises:
        FileNotFoundError: If the binary is not found on PATH.
    """
    path = shutil.which(name)
    if path is None:
        msg = (
            f"Required binary '{name}' not found on PATH. "
            f"Install it or ensure your environment is activated."
        )
        raise FileNotFoundError(msg)
    return path
