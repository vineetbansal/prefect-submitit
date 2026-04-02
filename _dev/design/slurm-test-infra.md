# Design: SLURM Testing Infrastructure

**Date:** 2026-03-30
**Status:** Draft
**Scope:** `docker/`, `tests/`, `.github/workflows/`, `pyproject.toml`

---

## Context

All integration tests in `prefect-submitit` require a real SLURM cluster.
The `--run-slurm` flag gates every test in `tests/integration/`, and the
`_check_slurm_available` fixture verifies `squeue` is reachable before any
test runs. Consequences:

- **CI only runs unit tests.** Integration tests are invisible to CI.
- **Contributors without SLURM access cannot run integration tests.**
  Development is limited to unit tests that mock at too high a level to
  catch serialization, subprocess lifecycle, or state-machine bugs.
- **No middle ground.** There is no way to exercise the full
  submit-execute-collect lifecycle without a scheduler.

This doc defines the infrastructure to close these gaps: a containerized
SLURM environment for CI and local prototyping, a LocalExecutor test tier
for fast platform-native development, and the test harness changes that
wire them together.

---

## Design Constraints

**Must not change existing `--run-slurm` behavior.** All current integration
tests pass unchanged when run against a real SLURM cluster.

**Must not add runtime dependencies.** Docker images, compose files, CI
workflows, and test fixtures are dev/test-only. No changes to `src/`.

**Must work on GitHub Actions `ubuntu-latest`.** The container image is
amd64; CI runners are amd64. No self-hosted infrastructure required.

**Must document Apple Silicon limitations.** The SLURM image is amd64-only.
Local dev on M-series Macs works under QEMU emulation but is slower
(~3-5x). The LocalExecutor tier provides a native-speed alternative.

**Must keep Prefect server isolated.** Test sessions use an ephemeral
`PREFECT_HOME` (existing pattern in `tests/integration/conftest.py`).

---

## Architecture Overview

Three test tiers, each covering a different slice of the codebase:

```
┌─────────────────────────────────────────────────────────┐
│  Docker SLURM                                           │
│  Real sbatch/squeue/scancel in a container              │
│  Full fidelity · CI gate · Local prototyping            │
├─────────────────────────────────────────────────────────┤
│  LocalExecutor Integration                              │
│  submitit.LocalExecutor · Real pickle → subprocess      │
│  No scheduler · Fast · Native on all platforms          │
├─────────────────────────────────────────────────────────┤
│  Mock Unit Tests                                        │
│  unittest.mock · State mapping · scancel · scontrol     │
│  No external deps · Runs unconditionally                │
└─────────────────────────────────────────────────────────┘
```

| Tier | Marker | Flag | External deps | Runs by default |
|------|--------|------|---------------|-----------------|
| Mock unit | (none) | — | Nothing | Yes |
| LocalExecutor | `@pytest.mark.local` | `--run-local` | Prefect server | Yes (planned) |
| Docker SLURM | `@pytest.mark.slurm` | `--run-slurm` | Docker or real SLURM | No |

---

## Docker SLURM Container

### Image

**[`nathanhess/slurm:full`](https://github.com/nathan-hess/docker-slurm)**,
pinned to `full-v1.2.0`.

| Property | Value |
|----------|-------|
| Base | Ubuntu 22.04 |
| SLURM version | 21.08.5 (from Ubuntu apt) |
| Architecture | **amd64 only** |
| Included | Python 3, pip, Git, GCC, build-essential, curl, wget |
| Services | munge, slurmctld, slurmd (single container) |
| Default partition | `debug` (single node, MaxTime=INFINITE) |
| Accounting | `accounting_storage/none` (`sacct` returns no history) |
| Tags | `full` (rolling, rebuilt monthly), `full-v1.2.0` (pinned) |

The startup script `/etc/startup.sh` configures `slurm.conf` with the
container's hostname, CPU count, and memory, then starts munge → slurmd →
slurmctld.

Rolling tags break reproducibility. Pin to `full-v1.2.0` in CI and
docker-compose; use `full` for manual testing only.

### Decisions

**Prefect server runs inside the container.** The `prefect_server` test
fixture starts Prefect as a subprocess with isolated `PREFECT_HOME`. This
works identically inside the container. Running it outside would require
network bridging between container (where SLURM jobs execute) and host
(where Prefect serves).

**PostgreSQL via apt.** The Dockerfile installs PostgreSQL with `apt-get`.
Pixi's conda-managed `postgresql` would conflict with the container's
system Python and add ~2 min to build time. The Prefect server fixture
manages PostgreSQL lifecycle regardless of how the binary was installed.

**Python deps via pip.** `pip install -e /workspace` inside the container
leverages the existing Python 3 and lets source changes (via volume mount)
take effect immediately. Pixi inside the container is unnecessary overhead.

**Privileged mode unlikely needed.** The image uses
`ProctrackType=proctrack/linuxproc` and `TaskPlugin=task/none` — no cgroup
management. The upstream repo's own CI runs without `--privileged`. Test
without it first; only add if `slurmd` fails to start.

**Prerequisite: populate `[project].dependencies`.** The project currently
declares `dependencies = []` in `pyproject.toml` — all runtime deps
(prefect, submitit, cloudpickle, asyncpg) are managed exclusively by pixi.
`pip install -e .` inside the container installs nothing useful. Before
implementing Phase 1, move runtime deps into `[project].dependencies` so
pip can resolve them. This also fixes `pip install prefect-submitit` for
end users.

### Files

#### `docker/Dockerfile`

```dockerfile
FROM nathanhess/slurm:full-v1.2.0

# PostgreSQL for Prefect server backend
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Test runner (not part of project runtime deps)
RUN pip install --no-cache-dir pytest pytest-timeout

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /workspace

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
```

#### `docker/entrypoint.sh`

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

#### `docker/docker-compose.yml`

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
      SLURM_TEST_TIME_LIMIT: "00:05:00"
      SLURM_TEST_MEM_GB: "1"
      SLURM_TEST_MAX_WAIT: "120"
    ports:
      - "4200:4200"   # Prefect server (if accessed from host)
    # privileged: true  # Try without first; add only if slurmd fails
```

### Local Dev Workflow

```bash
pixi run slurm-up        # Build and start the SLURM container
pixi run slurm-shell     # Drop into a shell inside the container

# Inside the container:
pip install -e .
pytest --run-slurm tests/integration/

pixi run slurm-test      # Or run tests directly from the host
pixi run slurm-down      # Tear down
```

**Apple Silicon note:** The container runs under QEMU emulation on M-series
Macs (~3-5x slower). For day-to-day development, prefer `--run-local`
which runs natively. Reserve the Docker container for validating
SLURM-specific behavior before pushing.

---

## CI Workflow

### `.github/workflows/test-slurm.yml`

```yaml
name: SLURM Integration Tests

on:
  push:
    branches: [main]
    paths: ['src/**', 'tests/**', 'pyproject.toml', 'pixi.lock']
  pull_request:
    branches: [main]
    paths: ['src/**', 'tests/**', 'pyproject.toml', 'pixi.lock']
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  slurm-integration:
    runs-on: ubuntu-latest
    container:
      image: nathanhess/slurm:full-v1.2.0
      # options: --privileged  # Try without first; add only if slurmd fails
    env:
      SLURM_TEST_PARTITION: debug
      SLURM_TEST_TIME_LIMIT: "00:05:00"
      SLURM_TEST_MEM_GB: "1"
      SLURM_TEST_MAX_WAIT: "120"
    steps:
      - uses: actions/checkout@v6

      # GHA overrides the container entrypoint — start daemons explicitly
      - name: Start SLURM daemons
        run: |
          /etc/startup.sh
          for i in $(seq 1 30); do
            squeue --noheader 2>/dev/null && break
            sleep 1
          done
          sinfo

      - uses: prefix-dev/setup-pixi@v0.9.4
        with:
          pixi-version: v0.39.5
          environments: dev

      - name: Run SLURM integration tests
        run: pixi run -e dev test-slurm
```

**Why `pixi` in CI but `pip` locally?** CI uses `setup-pixi` in the
existing `ci.yml`. Consistency means the same dependency resolution runs
unit tests and SLURM tests. Locally, `pip` is faster for iterative
development inside a volume-mounted container.

**Advisory first.** Start this workflow as an advisory status check.
Promote to required after ~10 stable PR runs. A flaky required check is
worse than no check.

**Partition name.** The `test-slurm` pixi task sets
`SLURM_TEST_PARTITION=cpu` (for real clusters). The Docker image's default
partition is `debug`. The CI workflow's `env:` block overrides the pixi
task value.

---

## LocalExecutor Integration Tests

### What It Tests

`submitit.LocalExecutor` spawns real subprocesses, pickles callables via
cloudpickle, and collects results from files — the same lifecycle as SLURM,
minus the scheduler. This catches serialization bugs, subprocess errors,
and result-collection issues without any external dependencies.

`SlurmTaskRunner` already supports `execution_mode="local"`.

### Compatibility

| Test file | Local-compatible | Notes |
|-----------|:---:|-------|
| `test_submit.py` | Yes | Single task, async tasks, serialization |
| `test_batch.py` | Yes | Batched execution, per-item failures, static params |
| `test_map.py` | Partial | Basic map works; `max_array_size` is synthetic; no real array job IDs |
| `test_polling.py` | Partial | Rapid completion works; timing-sensitive tests may need adjustment |
| `test_failures.py` | Partial | Exception propagation works; SIGKILL and invalid partition are SLURM-only |
| `test_environment.py` | Partial | Env var propagation works; `scontrol` and `SLURM_JOB_ID` are SLURM-only |
| `test_cancel.py` | No | `cancel()` calls `scancel` — not available locally |

### Test Infrastructure Changes

**`tests/conftest.py`** — add `--run-local` option:

```python
def pytest_addoption(parser):
    parser.addoption(
        "--run-slurm",
        action="store_true",
        default=False,
        help="Run tests that require a real SLURM cluster",
    )
    parser.addoption(
        "--run-local",
        action="store_true",
        default=False,
        help="Run tests against submitit.LocalExecutor",
    )
```

**`tests/integration/conftest.py`** — new fixtures:

```python
@pytest.fixture
def make_local_runner(request):
    """Factory: builds a SlurmTaskRunner with LocalExecutor."""
    log_dir = Path(tempfile.mkdtemp(prefix="local-test-logs-"))
    raw_name = request.node.name
    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", raw_name)[:50]

    def _make(**overrides):
        defaults = {
            "execution_mode": "local",
            "time_limit": "00:05:00",
            "poll_interval": 0.5,
            "max_poll_time": 60,
            "log_folder": str(log_dir),
            "slurm_job_name": sanitized,
        }
        defaults.update(overrides)
        return SlurmTaskRunner(**defaults)

    return _make


@pytest.fixture(params=["local", "slurm"])
def runner(request, make_local_runner, make_slurm_runner):
    """Parameterized runner: tests run against both backends."""
    if request.param == "slurm":
        if not request.config.getoption("--run-slurm"):
            pytest.skip("need --run-slurm")
        return make_slurm_runner()
    if not request.config.getoption("--run-local"):
        pytest.skip("need --run-local")
    return make_local_runner()
```

**`tests/integration/conftest.py`** — `prefect_server` starts for either
flag. **Important:** remove the `slurm_config` parameter from the current
signature (`def prefect_server(request, slurm_config)` at line 118).
`slurm_config` calls `pytest.skip()` when `SLURM_TEST_PARTITION` is unset,
which would skip the server for `--run-local`-only runs:

```python
@pytest.fixture(scope="session", autouse=True)
def prefect_server(request):
    run_slurm = request.config.getoption("--run-slurm", default=False)
    run_local = request.config.getoption("--run-local", default=False)
    if not run_slurm and not run_local:
        yield None
        return
    # ... server startup logic unchanged ...
```

**`tests/integration/conftest.py`** — SLURM availability check becomes
conditional:

```python
@pytest.fixture(scope="session", autouse=True)
def _check_slurm_available(request):
    if not request.config.getoption("--run-slurm", default=False):
        return  # LocalExecutor tests don't need SLURM
    # ... squeue check ...
```

**`tests/integration/conftest.py`** — collection hook handles both markers:

```python
def pytest_collection_modifyitems(config, items):
    run_slurm = config.getoption("--run-slurm")
    run_local = config.getoption("--run-local")

    for item in items:
        if "slurm" in item.keywords and not run_slurm:
            item.add_marker(pytest.mark.skip(reason="need --run-slurm"))
        if "local" in item.keywords and not run_local:
            item.add_marker(pytest.mark.skip(reason="need --run-local"))
```

**`tests/integration/conftest.py`** — make `slurm_jobs` cleanup
backend-aware. The existing fixture calls `scancel` unconditionally; guard
it so local-mode runs skip the SLURM cleanup:

```python
@pytest.fixture
def slurm_jobs(request, _session_slurm_job_ids):
    """Track submitted SLURM job IDs for teardown cleanup."""
    job_ids: list[str] = []
    yield job_ids
    if not request.config.getoption("--run-slurm"):
        return  # LocalExecutor — no scancel needed
    _session_slurm_job_ids.update(job_ids)
    if job_ids:
        subprocess.run(
            ["scancel", *job_ids],
            capture_output=True,
            timeout=10,
            check=False,
        )
```

### How Tests Use the Parameterized Fixture

Tests compatible with both backends use the `runner` fixture and carry
both markers. SLURM-only tests use `slurm_runner` directly:

```python
# Runs against both backends
@pytest.mark.slurm
@pytest.mark.local
def test_single_task(runner):
    @flow
    def my_flow():
        return add.submit(1, 2)
    state = my_flow(task_runner=runner, return_state=True)
    assert state.result() == 3


# SLURM-only
@pytest.mark.slurm
def test_cancel(slurm_runner):
    ...
```

---

## `pyproject.toml` Changes

### New Pixi Tasks

```toml
[tool.pixi.feature.dev.tasks]
# ... existing tasks ...
test-local = "pytest --run-local tests/integration/"
slurm-up = "docker compose -f docker/docker-compose.yml up -d --build"
slurm-shell = "docker compose -f docker/docker-compose.yml exec slurm bash"
slurm-test = { cmd = "docker compose -f docker/docker-compose.yml exec slurm bash -c 'pip install -e . && pytest --run-slurm tests/integration/'", env = { SLURM_TEST_PARTITION = "debug" } }
slurm-down = "docker compose -f docker/docker-compose.yml down -v"
```

### New Pytest Marker

```toml
markers = [
    "slurm: tests that submit real SLURM jobs (deselected unless --run-slurm)",
    "slurm_gpu: tests that require a GPU partition",
    "local: tests that run against submitit.LocalExecutor (deselected unless --run-local)",
]
```

---

## Alternatives Considered

1. **Multi-container compose** (`giovtorres/slurm-docker-cluster`, 5
   containers with MySQL + slurmdbd + 2 compute nodes). **Rejected:**
   overkill for single-node test scenarios, slower startup (~2 min),
   more maintenance. Revisit if multi-node tests become necessary.

2. **pytest on host with SLURM in a `services:` container.** **Rejected:**
   SLURM CLI tools (`sbatch`, `squeue`) require munge authentication and
   `slurm.conf` co-located with the calling process. Splitting pytest from
   the SLURM environment requires a remote submission mechanism that
   doesn't exist.

3. **Custom SLURM image from scratch.** **Rejected:** `nathanhess/slurm`
   is maintained, tested, MIT-licensed, and designed for CI. Building from
   scratch is significant ongoing maintenance with no benefit.

4. **Pixi inside the Docker container.** **Rejected:** conda's
   `postgresql` package conflicts with the container's apt-installed
   PostgreSQL. Pixi env creation adds ~2 min to container startup. pip is
   sufficient for the container's dev/test purpose.

5. **Self-hosted runner with real SLURM.** **Rejected:** not accessible to
   external contributors, ongoing infrastructure cost, and the Docker
   approach achieves the same coverage for all testable scenarios.

6. **Ansible-based SLURM install** (`koesterlab/setup-slurm-action`).
   **Rejected:** installs SLURM directly on the GHA runner via Ansible.
   Slower setup, requires MySQL service container, less portable than a
   pre-built Docker image.

---

## Open Questions

**Advisory or required status check?** Recommendation: start advisory.
Promote to required after ~10 stable PR runs. A flaky required check is
worse than no check.

**Mirror image to GHCR?** Docker Hub rate limits can block CI. Mirroring
`nathanhess/slurm:full-v1.2.0` to `ghcr.io/andrewcolinhunt/slurm:full`
avoids this. Low effort, not urgent until rate limits are hit.

**Accounting storage for `sacct` tests?** The image has
`AccountingStorageType=accounting_storage/none`. If tests need `sacct` job
history, accounting must be configured (add slurmdbd + MySQL). Not needed
for current tests.

---

## Implementation Plan

### Phase 1: Docker Infrastructure

**Effort:** Small
**Files:** `docker/Dockerfile`, `docker/docker-compose.yml`,
`docker/entrypoint.sh`, `pyproject.toml`

1. Create Docker files (Dockerfile, compose, entrypoint).
2. Add pixi tasks (`slurm-up`, `slurm-shell`, `slurm-test`, `slurm-down`).
3. Verify existing integration tests pass inside the container with
   `--run-slurm` and `SLURM_TEST_PARTITION=debug`.

### Phase 2: CI Workflow

**Effort:** Small
**Files:** `.github/workflows/test-slurm.yml`

1. Add GitHub Actions workflow.
2. Verify `--run-slurm` tests pass in CI.
3. Add as advisory status check.

### Phase 3: LocalExecutor Integration

**Effort:** Medium
**Files:** `tests/conftest.py`, `tests/integration/conftest.py`,
`pyproject.toml`

1. Add `--run-local` flag and `local` marker.
2. Add `make_local_runner` fixture.
3. Refactor `prefect_server` fixture to support `--run-local`.
4. Refactor `_check_slurm_available` to be conditional.
5. Add parameterized `runner` fixture.
6. Tag compatible tests with `@pytest.mark.local`.
7. Add `test-local` pixi task.
