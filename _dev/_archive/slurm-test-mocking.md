# Design: Testing Without a SLURM Cluster

**Date:** 2026-03-29
**Status:** Draft
**Scope:** `tests/`, `tests/integration/`

---

## Context

All integration tests in `prefect-submitit` require a real SLURM cluster.
The `--run-slurm` flag gates every test in `tests/integration/`, and the
`_check_slurm_available` fixture verifies `squeue` is reachable before any
test runs. This means:

- **Contributors without SLURM access cannot run integration tests.**
  Development is limited to unit tests, which mock at too high a level to
  catch serialization, subprocess lifecycle, or state-machine bugs.
- **CI cannot run integration tests** without dedicated HPC infrastructure
  or a Docker-based SLURM cluster.
- **The gap between unit tests and integration tests is wide.** Unit tests
  mock `submitit.Job`, `submitit.AutoExecutor`, and Prefect internals via
  `MagicMock`. Integration tests submit real SLURM jobs. There is no
  middle ground that exercises the full submit-execute-collect lifecycle
  without a scheduler.

The codebase already supports `ExecutionMode.LOCAL` (using
`submitit.LocalExecutor`), but this mode is not wired into the test suite.

---

## Goals

1. Enable a meaningful subset of integration tests to run **without SLURM**.
2. Test the full pickle → subprocess → result lifecycle locally.
3. Test SLURM-specific code paths (state mapping, cancellation, `scontrol`
   parsing) without a real scheduler.
4. Optionally support a Docker-based SLURM cluster for full-fidelity CI.
5. Preserve the existing `--run-slurm` test path unchanged.

---

## Design Constraints

**No new runtime dependencies.** Test-only dependencies (Docker images,
pytest plugins) are acceptable; changes to `src/` are not required.

**Existing integration tests remain authoritative.** The local test tier
supplements but does not replace real SLURM tests. Tests that depend on
scheduler behavior (queue latency, node allocation, `scancel` races) stay
SLURM-only.

**Minimal test duplication.** Where possible, the same test logic should
run against both the local executor and the real SLURM executor, selected
by fixture parameterization or marker.

---

## SLURM Coupling Points

The codebase touches SLURM in four distinct ways. Each requires a different
testing strategy:

| Layer | Where | What | Mock strategy |
|-------|-------|------|---------------|
| **Executor** | `runner.py:134-159` | `submitit.AutoExecutor` / `LocalExecutor` — submit jobs, create batches | Swap to `LocalExecutor` |
| **Job state** | `futures/base.py:70-81` | `job.state` returns SLURM strings (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, NODE_FAIL, TIMEOUT, OUT_OF_MEMORY) | submitit's `MockedSubprocess` or direct mock |
| **CLI tools** | `futures/base.py`, `futures/array.py`, `utils.py` | `subprocess.run(["scancel", ...])`, `subprocess.run(["scontrol", ...])` | `unittest.mock.patch` on `subprocess.run` |
| **Env vars** | `executors.py` | `SLURM_JOB_ID`, `SLURM_ARRAY_JOB_ID`, `SLURM_ARRAY_TASK_ID` read on compute nodes | `monkeypatch.setenv` (already done in unit tests) |

---

## Design: Three-Tier Test Strategy

### Tier 1 — Local Executor Integration Tests

**What:** Run integration tests against `submitit.LocalExecutor` instead of
`submitit.AutoExecutor`. `LocalExecutor` spawns real subprocesses, pickles
callables via cloudpickle, and collects results from files — the same
lifecycle as SLURM, minus the scheduler.

**Marker:** `@pytest.mark.local` (new), runnable via `--run-local` or by
default (no flag needed — these tests have no external dependencies).

**Fixture:** A `make_local_runner` factory that builds `SlurmTaskRunner`
with `execution_mode="local"`:

```python
@pytest.fixture
def make_local_runner(request, prefect_server_local):
    log_dir = Path(tempfile.mkdtemp(prefix="local-test-logs-"))

    def _make(**overrides):
        defaults = {
            "execution_mode": "local",
            "time_limit": "00:05:00",
            "poll_interval": 0.5,
            "max_poll_time": 60,
            "log_folder": str(log_dir),
        }
        defaults.update(overrides)
        return SlurmTaskRunner(**defaults)

    return _make
```

**Which tests can run locally:**

| Test file | Local-compatible | Notes |
|-----------|-----------------|-------|
| `test_submit.py` | Yes | Single task submit, async tasks, serialization |
| `test_map.py` | Partial | Basic map and parameters work; chunking works but `max_array_size` is synthetic; no real array job IDs |
| `test_batch.py` | Yes | Batched execution, per-item failures, static params |
| `test_polling.py` | Partial | Rapid completion and `max_poll_time` work; `wait(timeout=...)` works; local jobs complete fast so timing-sensitive tests may need adjustment |
| `test_failures.py` | Partial | Exception propagation works; invalid partition detection and SIGKILL are SLURM-only |
| `test_cancel.py` | No | `cancel()` calls `scancel` via subprocess — not available locally |
| `test_environment.py` | Partial | Env var propagation works; `get_cluster_max_array_size` hits `scontrol` (SLURM-only); `SLURM_JOB_ID` not set locally |

**Implementation approach:** Rather than duplicating test files, use
fixture parameterization. Tests that are compatible with both backends
receive either a `slurm_runner` or `local_runner` fixture. Tests that are
SLURM-only keep the `@pytest.mark.slurm` marker exclusively.

```python
# tests/integration/conftest.py

@pytest.fixture(params=["local", "slurm"])
def runner(request, make_local_runner, make_slurm_runner):
    if request.param == "slurm":
        if not request.config.getoption("--run-slurm"):
            pytest.skip("need --run-slurm")
        return make_slurm_runner()
    return make_local_runner()
```

Tests that use the `runner` fixture automatically run against both backends.
Tests that use `slurm_runner` directly remain SLURM-only.

**Prefect server fixture:** The existing `prefect_server` fixture is gated
behind `--run-slurm`. A new `prefect_server_local` fixture (session-scoped)
starts an ephemeral Prefect server for local tests. Alternatively, refactor
the existing fixture to start unconditionally when any integration test
is collected.

---

### Tier 2 — SLURM-Specific Mock Tests

**What:** Test code paths that interact with SLURM CLI tools and state
strings, using mocks instead of a real scheduler. These sit in
`tests/` (not `tests/integration/`) and require no external dependencies.

**Target code paths:**

#### a) Future state mapping (`futures/base.py:70-81`)

The `SlurmPrefectFuture.state` property maps SLURM state strings to Prefect
states. This logic is pure — it reads `self._job.state` and returns a
`State` object. Test it with a mock `submitit.Job`:

```python
def test_state_mapping_completed():
    job = MagicMock()
    job.state = "COMPLETED"
    job.done.return_value = False
    future = SlurmPrefectFuture(job, uuid4(), poll_interval=1, max_poll_time=60)
    assert isinstance(future.state, Completed)

def test_state_mapping_cancelled_with_suffix():
    job = MagicMock()
    job.state = "CANCELLED+"
    job.done.return_value = False
    future = SlurmPrefectFuture(job, uuid4(), poll_interval=1, max_poll_time=60)
    assert isinstance(future.state, Failed)

# Cover: PENDING, RUNNING, COMPLETED, COMPLETING, FAILED, NODE_FAIL,
#        TIMEOUT, CANCELLED, CANCELLED+, OUT_OF_MEMORY
```

#### b) Cancellation via `scancel` (`futures/base.py`, `futures/array.py`)

Mock `subprocess.run` to verify that `cancel()` calls `scancel` with the
correct job ID, and that array cancellation targets the right
`jobid_taskindex` format:

```python
@patch("prefect_submitit.futures.base.subprocess.run")
def test_cancel_calls_scancel(mock_run):
    job = MagicMock()
    job.job_id = "12345"
    future = SlurmPrefectFuture(job, uuid4(), poll_interval=1, max_poll_time=60)
    future.cancel()
    mock_run.assert_called_once_with(
        ["scancel", "12345"], capture_output=True, timeout=10, check=False
    )
```

#### c) `scontrol` parsing (`utils.py:78-112`)

`get_cluster_max_array_size` calls `scontrol show config` and parses
`MaxArraySize`. Mock `subprocess.run` to test parsing, fallback on failure,
and caching behavior:

```python
@patch("prefect_submitit.utils.subprocess.run")
def test_max_array_size_parses_scontrol(mock_run):
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="MaxArraySize          = 4001\n",
    )
    runner = MagicMock(
        max_array_size=None,
        _cached_max_array_size=None,
        execution_mode=ExecutionMode.SLURM,
    )
    assert get_cluster_max_array_size(runner) == 4001
```

#### d) submitit's `MockedSubprocess` (optional, advanced)

submitit's own test infrastructure (`submitit/core/test_core.py`) provides a
`MockedSubprocess` class that intercepts `subprocess.check_output` and
simulates `sbatch` (returns fake job IDs), `sacct` (returns configurable
states), and `scancel`. This could be adopted for more realistic end-to-end
mock tests that exercise the `SlurmExecutor` directly:

```python
from submitit.core.test_core import MockedSubprocess

def test_submit_with_mocked_slurm():
    with MockedSubprocess() as mock:
        executor = submitit.SlurmExecutor(folder=tmp_path)
        job = executor.submit(lambda: 42)
        assert job.job_id == "12"  # MockedSubprocess auto-increments from 12
```

**Caution:** `MockedSubprocess` is internal to submitit and not part of
their public API. Pin the import and expect breakage on submitit upgrades.
Prefer the simpler `unittest.mock` approach for most tests; reserve
`MockedSubprocess` for tests that need to exercise submitit's full
submission pipeline.

---

### Tier 3 — Docker SLURM for CI (Optional)

**What:** Run the full integration test suite against a containerized SLURM
cluster in CI (GitHub Actions).

**Options:**

| Image | Setup | Nodes | Array support | Maintenance |
|-------|-------|-------|---------------|-------------|
| `nathanhess/slurm:full` | Single container | 1 | Yes (local) | Active, Ubuntu-based |
| `giovtorres/slurm-docker-cluster` | Docker Compose | 2 CPU + 1 GPU | Yes (multi-node) | Active, multi-arch |
| `daskdev/dask-jobqueue:slurm` | Docker Compose | 2 | Yes | Maintained by Dask team |

**Recommended: `nathanhess/slurm:full`** for simplicity. Single container,
published to Docker Hub, designed for GitHub Actions:

```yaml
# .github/workflows/test-slurm.yml
jobs:
  slurm-integration:
    runs-on: ubuntu-latest
    container:
      image: nathanhess/slurm:full
    steps:
      - uses: actions/checkout@v4
      - name: Install pixi
        run: curl -fsSL https://pixi.sh/install.sh | bash
      - name: Run SLURM integration tests
        env:
          SLURM_TEST_PARTITION: normal
          SLURM_TEST_TIME_LIMIT: "00:05:00"
        run: ~/.pixi/bin/pixi run -e dev test-slurm
```

**Trade-offs:**
- (+) Full fidelity — real `sbatch`, `scancel`, `scontrol`, job arrays
- (+) Catches regressions that mocks miss (state timing, file-based I/O)
- (-) Slower CI (~2-5 min container startup + test time)
- (-) Single-node container cannot test multi-node scenarios
- (-) Container SLURM config may differ from production clusters

**When to adopt:** After Tier 1 and Tier 2 are in place. Docker SLURM is
most valuable as a CI gate for PRs, not for local development.

---

## Test Marker Summary

| Marker | Flag | What it needs | Runs by default |
|--------|------|---------------|-----------------|
| (none) | — | Nothing | Yes |
| `@pytest.mark.local` | `--run-local` or none | Prefect server | Yes |
| `@pytest.mark.slurm` | `--run-slurm` | SLURM cluster + Prefect server | No |
| `@pytest.mark.slurm_gpu` | `--run-slurm` | SLURM GPU partition + Prefect server | No |

Tier 2 tests (mock-based) have no marker — they are standard unit tests
that run unconditionally.

---

## Implementation Plan

### Phase 1: Tier 2 — SLURM Mock Unit Tests

**Effort:** Small
**Files:** `tests/test_futures.py` (new), extend `tests/test_utils.py`

Add targeted unit tests for:
- `SlurmPrefectFuture.state` — all SLURM state strings → Prefect states
- `SlurmPrefectFuture.cancel()` — `scancel` subprocess call
- `SlurmArrayPrefectFuture.cancel()` — array task `scancel` format
- `get_cluster_max_array_size()` — `scontrol` parsing, caching, fallback

These are fast to write and immediately close coverage gaps on code that is
currently only tested via real SLURM.

### Phase 2: Tier 1 — Local Executor Integration Tests

**Effort:** Medium
**Files:** `tests/integration/conftest.py` (extend), possibly new
`tests/local/` directory

1. Refactor `prefect_server` fixture to start unconditionally when
   integration tests are collected.
2. Add `make_local_runner` factory fixture.
3. Parameterize compatible tests to run against both `local` and `slurm`
   backends (or create a `tests/local/` directory that imports shared test
   logic).
4. Add `@pytest.mark.local` marker and pixi task:
   ```bash
   pixi run -e dev test-local    # Run local integration tests
   ```
5. Determine which tests from each integration file are local-compatible
   and tag accordingly.

**Key decision: shared tests vs. separate directory.**

*Option A — Parameterized fixture:* A single `runner` fixture with
`params=["local", "slurm"]`. Pro: no duplication. Con: every compatible
test runs twice when `--run-slurm` is passed; test output is noisier.

*Option B — Separate `tests/local/` directory:* Copy/adapt compatible tests
into a new directory with local-only fixtures. Pro: clean separation,
independent markers. Con: some duplication of test logic.

*Option C — Shared test functions, imported by both:* Define test logic in
a shared module (e.g., `tests/integration/_shared.py`), imported by both
`tests/integration/` and `tests/local/`. Pro: no duplication, clean
separation. Con: slightly unusual pytest structure.

**Recommendation: Option A** (parameterized fixture) for simplicity. The
doubled test runs are a feature — they verify that both backends produce
the same behavior.

### Phase 3: Tier 3 — Docker SLURM CI (Optional)

**Effort:** Small (workflow file only)
**Files:** `.github/workflows/test-slurm.yml` (new)

Add a GitHub Actions workflow using `nathanhess/slurm:full`. Run on PR
events only (not every push) to limit CI cost. This phase is independent
of Phases 1-2 and can be done at any time.

---

## What This Does Not Solve

- **Multi-node SLURM behavior.** `LocalExecutor` and single-container
  Docker SLURM are both single-node. Tests that depend on cross-node
  scheduling, network topology, or distributed GPU allocation still
  require a real cluster.
- **NFS timing.** `SlurmPrefectFuture` includes NFS cache invalidation
  logic for reading log files. This is only exercisable on a real shared
  filesystem.
- **Scheduler contention.** Queue wait times, preemption, and
  `ReqNodeNotAvail` are inherently untestable without a loaded scheduler.
- **`srun` execution mode.** The planned `ExecutionMode.SRUN` (see
  `_dev/design/srun.md`) dispatches work via `srun` within an existing
  allocation. This cannot be tested with `LocalExecutor` or Docker SLURM
  and will always require a real allocation.
