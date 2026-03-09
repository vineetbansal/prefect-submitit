"""PostgreSQL lifecycle management."""

from __future__ import annotations

import contextlib
import os
import shutil as _shutil
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

from prefect_submitit.server.config import require_binary

if TYPE_CHECKING:
    from prefect_submitit.server.config import ServerConfig

_START_SENTINEL = "# --- prefect-submitit custom settings ---"
_END_SENTINEL = "# --- end prefect-submitit custom settings ---"


def _write_custom_config(pg_data_dir: os.PathLike, pg_port: int) -> None:
    """Write sentinel-bounded custom config block to postgresql.conf.

    Idempotent: removes any existing block before writing.

    Args:
        pg_data_dir: Path to the PostgreSQL data directory.
        pg_port: Port for PostgreSQL to listen on.
    """
    conf = Path(pg_data_dir) / "postgresql.conf"
    lines = conf.read_text().splitlines(keepends=True)

    # Remove existing sentinel block
    filtered: list[str] = []
    inside = False
    for line in lines:
        if _START_SENTINEL in line:
            inside = True
            continue
        if _END_SENTINEL in line:
            inside = False
            continue
        if not inside:
            filtered.append(line)

    # Append new config block
    block = (
        f"{_START_SENTINEL}\n"
        f"listen_addresses = 'localhost'\n"
        f"port = {pg_port}\n"
        f"max_connections = 200\n"
        f"shared_buffers = 256MB\n"
        f"log_destination = 'stderr'\n"
        f"logging_collector = off\n"
        f"{_END_SENTINEL}\n"
    )
    filtered.append(block)

    conf.write_text("".join(filtered))


def is_running(config: ServerConfig) -> int | None:
    """Check if PostgreSQL is running.

    Args:
        config: Server configuration.

    Returns:
        PID if running, None otherwise.
    """
    pid_file = config.pg_data_dir / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        pid = int(pid_file.read_text().split("\n")[0])
        os.kill(pid, 0)
        return pid
    except (ValueError, ProcessLookupError, PermissionError, OSError):
        return None


def init_db(config: ServerConfig, *, reset: bool = False) -> None:
    """Initialize the PostgreSQL database cluster.

    Args:
        config: Server configuration.
        reset: If True, delete existing data and reinitialize.

    Raises:
        FileNotFoundError: If initdb binary is not found.
        subprocess.CalledProcessError: If initdb fails.
    """
    initdb_bin = require_binary("initdb")
    pg_ctl_bin = require_binary("pg_ctl")
    createdb_bin = require_binary("createdb")

    if reset and config.pg_data_dir.exists():
        # Stop PG if running before removing data
        pid = is_running(config)
        if pid is not None:
            stop(config, force=True)
        _shutil.rmtree(config.pg_data_dir)

    if config.pg_data_dir.exists() and (config.pg_data_dir / "PG_VERSION").exists():
        # Already initialized — just ensure config is current
        _write_custom_config(config.pg_data_dir, config.pg_port)
        _ensure_database(config, pg_ctl_bin, createdb_bin)
        return

    config.pg_data_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            initdb_bin,
            "-D",
            str(config.pg_data_dir),
            "-U",
            config.pg_user,
            "--auth=trust",
            "--encoding=UTF8",
        ],
        check=True,
        capture_output=True,
    )

    _write_custom_config(config.pg_data_dir, config.pg_port)

    # Temp start to create the database
    subprocess.run(
        [
            pg_ctl_bin,
            "-D",
            str(config.pg_data_dir),
            "-l",
            str(config.pg_data_dir / "postgres.log"),
            "-o",
            f"-p {config.pg_port}",
            "start",
        ],
        check=True,
        capture_output=True,
    )

    _wait_for_pg_ready(config.pg_port)

    subprocess.run(
        [
            createdb_bin,
            "-h",
            "localhost",
            "-p",
            str(config.pg_port),
            "-U",
            config.pg_user,
            config.pg_database,
        ],
        check=True,
        capture_output=True,
    )

    subprocess.run(
        [pg_ctl_bin, "-D", str(config.pg_data_dir), "stop"],
        check=True,
        capture_output=True,
    )


def _ensure_database(
    config: ServerConfig,
    pg_ctl_bin: str,
    createdb_bin: str,
) -> None:
    """Ensure the prefect database exists in an initialized cluster.

    Starts PG temporarily if needed, creates the DB if missing.

    Args:
        config: Server configuration.
        pg_ctl_bin: Path to pg_ctl binary.
        createdb_bin: Path to createdb binary.
    """
    was_stopped = is_running(config) is None

    if was_stopped:
        subprocess.run(
            [
                pg_ctl_bin,
                "-D",
                str(config.pg_data_dir),
                "-l",
                str(config.pg_data_dir / "postgres.log"),
                "-o",
                f"-p {config.pg_port}",
                "start",
                "-w",
                "-t",
                "10",
            ],
            check=True,
            capture_output=True,
        )

    # createdb is idempotent-ish: it fails if DB exists, which we ignore
    subprocess.run(
        [
            createdb_bin,
            "-h",
            "localhost",
            "-p",
            str(config.pg_port),
            "-U",
            config.pg_user,
            config.pg_database,
        ],
        capture_output=True,
        check=False,
    )

    if was_stopped:
        subprocess.run(
            [pg_ctl_bin, "-D", str(config.pg_data_dir), "stop", "-w"],
            capture_output=True,
            check=False,
        )


def _wait_for_pg_ready(
    pg_port: int,
    timeout: float = 10,
    poll: float = 0.5,
) -> None:
    """Wait for PostgreSQL to accept TCP connections.

    Args:
        pg_port: Port to check.
        timeout: Maximum wait time in seconds.
        poll: Time between checks in seconds.

    Raises:
        RuntimeError: If PostgreSQL doesn't become ready.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", pg_port), timeout=1):
                return
        except OSError:
            time.sleep(poll)
    msg = f"PostgreSQL did not become ready on port {pg_port} within {timeout}s"
    raise RuntimeError(msg)


def start(config: ServerConfig) -> int:
    """Start PostgreSQL if not already running.

    Auto-initializes the database if needed.

    Args:
        config: Server configuration.

    Returns:
        PID of the running PostgreSQL process.

    Raises:
        RuntimeError: If PostgreSQL fails to start.
    """
    pid = is_running(config)
    if pid is not None:
        return pid

    # Auto-init if needed
    if not (config.pg_data_dir / "PG_VERSION").exists():
        init_db(config)

    # Sync config
    _write_custom_config(config.pg_data_dir, config.pg_port)

    pg_ctl_bin = require_binary("pg_ctl")

    # Clean stale pid file
    pid_file = config.pg_data_dir / "postmaster.pid"
    if pid_file.exists() and is_running(config) is None:
        pid_file.unlink()

    subprocess.run(
        [
            pg_ctl_bin,
            "-D",
            str(config.pg_data_dir),
            "-l",
            str(config.pg_data_dir / "postgres.log"),
            "-o",
            f"-p {config.pg_port}",
            "start",
        ],
        check=True,
        capture_output=True,
    )

    _wait_for_pg_ready(config.pg_port)

    pid = is_running(config)
    if pid is None:
        msg = "PostgreSQL started but PID could not be determined"
        raise RuntimeError(msg)
    return pid


def stop(config: ServerConfig, *, force: bool = False) -> None:
    """Stop PostgreSQL.

    Args:
        config: Server configuration.
        force: If True, use SIGKILL as fallback after failed graceful stop.
    """
    pid = is_running(config)
    if pid is None:
        return

    pg_ctl_bin = require_binary("pg_ctl")
    result = subprocess.run(
        [pg_ctl_bin, "-D", str(config.pg_data_dir), "stop", "-m", "fast"],
        capture_output=True,
    )

    if result.returncode != 0 and force:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
