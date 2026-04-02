# Design: Docker SLURM Environment for Local Development

**Date:** 2026-03-31
**Status:** Draft
**Scope:** `docker/`, `pyproject.toml`

---

## Context

Development on `prefect-submitit` features that interact with SLURM (the
`srun` execution mode, `sbatch` submission, `squeue`/`scancel` integration)
requires a working SLURM scheduler. Contributors without cluster access
cannot exercise these code paths at all.

This doc defines a minimal containerized SLURM environment for local
prototyping and script execution. It is **not** CI infrastructure or test
harness — just a way to get `sbatch`, `srun`, `squeue`, and `scancel`
working on a laptop.

---

## Goals

1. `pixi run slurm-up` gives you a single-node SLURM cluster.
2. `pixi run slurm-shell` drops you into the container with the project
   installed and editable.
3. Source edits on the host are immediately visible inside the container
   (volume mount; one-time `pip install -e .` on first shell).
4. Works on GitHub Actions `ubuntu-latest` if we ever want CI later.

## Non-Goals

- CI workflows (add later if needed).
- Test fixture changes (`--run-local`, markers, parameterized runners).
- Multi-node clusters, GPU partitions, or job accounting.

---

## Image

**[`nathanhess/slurm:full`](https://github.com/nathan-hess/docker-slurm)**,
pinned to `full-v1.2.0`.

| Property | Value |
|----------|-------|
| Base | Ubuntu 22.04 |
| SLURM version | 21.08.5 (Ubuntu apt) |
| Architecture | **amd64 only** |
| Included | Python 3.10, pip, git, build-essential, curl, wget |
| Services | munge, slurmctld, slurmd (single container) |
| Default partition | `debug` (single node, MaxTime=INFINITE) |
| Accounting | `accounting_storage/none` (`sacct` returns no history) |
| License | MIT |

The startup script `/etc/startup.sh` configures `slurm.conf` with the
container's hostname, CPU count, and memory, then starts munge → slurmctld →
slurmd.

**Python 3.12:** The base image ships Python 3.10 (Ubuntu 22.04 system
Python). The project requires `>=3.12`, so the Dockerfile installs Python
3.12 from the [deadsnakes PPA](https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa)
and sets it as the default `python3`.

**Apple Silicon:** The image is amd64-only. On M-series Macs it runs under
QEMU emulation (~3–5× slower). Fine for prototyping; not for heavy test
suites.

---

## Prerequisite: Populate `[project].dependencies`

The project currently declares `dependencies = []` in `pyproject.toml`.
All runtime deps are managed exclusively by pixi. This means `pip install
-e .` inside the container installs nothing useful.

Before building the container workflow, move runtime deps into
`[project].dependencies`:

```toml
dependencies = [
    "prefect>=3.6,<4.0",
    "submitit",
    "cloudpickle",
    "asyncpg",
]
```

This also fixes `pip install prefect-submitit` for end users who don't use
pixi.

---

## Files

### `docker/Dockerfile`

```dockerfile
FROM nathanhess/slurm:full-v1.2.0

# Base image runs as non-root; switch to root for package installation
USER root

# Python 3.12 (project requires >=3.12; base image has 3.10)
# PostgreSQL server binaries for prefect-server CLI (pg_ctl, initdb)
RUN apt-get update && \
    apt-get install -y --no-install-recommends software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.12 python3.12-venv python3.12-dev \
        postgresql postgresql-client && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    python3 -m ensurepip --upgrade && \
    python3 -m pip install --upgrade pip && \
    rm -rf /var/lib/apt/lists/*

# pg_ctl and initdb live here on Ubuntu 22.04 (not on PATH by default)
ENV PATH="/usr/lib/postgresql/14/bin:${PATH}"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Allow the docker user to create PostgreSQL unix socket lock files
RUN chmod 1777 /var/run/postgresql

# Switch back to the base image's default user
USER docker

WORKDIR /workspace

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
```

### `docker/entrypoint.sh`

```bash
#!/bin/bash
set -e

# Start SLURM daemons (munge + slurmctld + slurmd)
sudo /etc/startup.sh

# Wait for SLURM controller to accept commands
for i in $(seq 1 30); do
    if squeue --noheader 2>/dev/null; then
        echo "SLURM is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: SLURM did not become ready in 30 seconds." >&2
        exit 1
    fi
    sleep 1
done

exec "$@"
```

### `docker/docker-compose.yml`

```yaml
services:
  slurm:
    build: .
    image: prefect-submitit-slurm:dev
    hostname: slurm-node
    volumes:
      - ..:/workspace
    environment:
      SLURM_TEST_PARTITION: debug
      PREFECT_API_URL: http://localhost:4200/api
    ports:
      - "4200:4200"   # Prefect server (if accessed from host)
    tty: true
    stdin_open: true
```

**Privileged mode:** The image uses `ProctrackType=proctrack/linuxproc` and
`TaskPlugin=task/none` — no cgroup management. The upstream repo runs CI
without `--privileged`. Start without it. If `slurmd` fails, add
`privileged: true` to the compose file.

---

## Pixi Tasks

```toml
[tool.pixi.tasks]
slurm-build = "docker compose -f docker/docker-compose.yml build"
slurm-up = "docker compose -f docker/docker-compose.yml up -d"
slurm-down = "docker compose -f docker/docker-compose.yml down -v"
slurm-shell = "docker compose -f docker/docker-compose.yml exec slurm bash"
```

`slurm-build` pulls the base image (~1–2 GB) and builds the project image.
This is slow the first time (several minutes) and should be run explicitly
so the download isn't a surprise. Subsequent builds use Docker layer
caching. (`slurm-up` auto-builds if needed, but run `slurm-build` first
to avoid a surprise download.)

### Workflow

```bash
# One-time setup (pulls base image, builds project image):
pixi run slurm-build

# Start the container (fast — image already built):
pixi run slurm-up
pixi run slurm-shell       # Drop into the container

# Inside the container (first time only):
pip install -e .            # Editable install (uses volume mount)

# Start Prefect server (auto-initializes PostgreSQL):
# Explicit --port 4200 matches the docker-compose port mapping.
# (The default port is UID-based; root/UID 0 happens to get 4200,
# but being explicit avoids surprises.)
prefect-server start --bg --port 4200

# Now you have a working SLURM + prefect-submitit environment:
sinfo                       # Verify SLURM is up
srun hostname               # Run a trivial job
python my_script.py         # Run your code against real SLURM

# When done:
exit
pixi run slurm-down         # Tear down
```

Re-run `slurm-build` only if you change the `Dockerfile`. Source edits on
the host take effect immediately inside the container (editable install +
volume mount). Re-run `pip install -e .` only if you change
`pyproject.toml` metadata. Re-run `prefect-server start --bg` after each
`slurm-up` — PostgreSQL data lives in the container and is lost on
`slurm-down`.

---

## Decisions

**Prefect server runs inside the container.** SLURM jobs execute inside the
container, so the Prefect API must be reachable from there. Running Prefect
on the host would require network bridging. Use the project's
`prefect-server start --bg` CLI inside the container — it auto-initializes
PostgreSQL, starts it, and launches the Prefect server in one command.

**PostgreSQL via apt, not pixi.** Pixi's conda-managed `postgresql` would
conflict with the container's system Python and add ~2 min to build time.
The apt package provides the binaries (`pg_ctl`, `initdb`, `createdb`) that
the `prefect-server` CLI needs. The Dockerfile adds their path
(`/usr/lib/postgresql/14/bin`) to `$PATH`.

**pip, not pixi, inside the container.** Faster for iterative development.
Pixi is unnecessary overhead when the only goal is an editable install of
the project and its deps.

**No `slurm-test` task.** This is for prototyping, not test automation.
Run pytest manually inside the container if needed.

---

## Open Questions

**Mirror image to GHCR?** Docker Hub rate limits can block pulls. Mirroring
`nathanhess/slurm:full-v1.2.0` to GHCR avoids this. Not urgent until
rate limits are hit.

**Accounting storage for `sacct`?** The image has
`AccountingStorageType=accounting_storage/none`. If `sacct` job history is
needed, accounting must be configured (add slurmdbd + MySQL). Not needed
for current `srun` prototyping.
