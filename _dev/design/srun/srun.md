# Design: `srun` Execution Mode for prefect-submitit

**Date:** 2026-03-20
**Status:** Revised
**Repo:** `prefect-submitit`
**Related:** [SLURM Intra-Allocation Backend (artisan)](slurm_intra_backend.md)

---

## Context

Artisan pipelines run on HPC clusters via SLURM. The current `Backend.SLURM`
submits each step's work as independent `sbatch` jobs (or job arrays) to the
SLURM queue. This works well for long-running tasks but creates unnecessary
overhead when a user already has resources allocated:

```bash
salloc --nodes=4 --gpus-per-node=8 --cpus-per-node=64
# 32 GPUs, 256 CPUs are now reserved and idle, waiting for work
```

With the current backend, each pipeline task still goes through the SLURM
queue via `sbatch`, even though the resources are already in hand. This means:

- **Queue latency per task** — seconds to minutes of wait, multiplied across
  hundreds of tasks
- **sbatch overhead** — each submission is a SLURM API call
- **Resource fragmentation** — jobs may land outside the allocation

The intra-allocation backend eliminates this by distributing work directly to
allocated resources using `srun`, SLURM's built-in intra-allocation task
launcher. This doc covers the `prefect-submitit` side: a new `SRUN` execution
mode for `SlurmTaskRunner`.

---

## Design Constraints

**Must support multi-node allocations.** A typical ML workload allocates 2-8
nodes. Single-node-only would leave most allocated resources idle, defeating
the purpose. This constraint eliminates `submitit.LocalExecutor` and Python's
`ProcessPoolExecutor` as dispatch mechanisms — both are single-node only.

**Must use `srun` for task dispatch.** Within a SLURM allocation, `srun` is
the only mechanism that distributes work across multiple allocated nodes
without going through the queue. It also provides GPU binding via
`--gres=gpu:N`, cgroup isolation, and proper SLURM accounting — all for free.

**Follow submitit's serialize-launch-collect pattern.** submitit's approach —
serialize a callable via cloudpickle, launch a worker process, collect the
result from a pickle file on shared storage — is proven and well-understood.
The srun mode follows this same pattern with a thin custom worker script,
avoiding coupling to submitit's internal `_submit` module (whose file-naming
conventions depend on `SLURM_JOB_ID` environment variables that conflict with
intra-allocation usage).

**No new external dependencies.** `srun` is available on any SLURM cluster.
submitit is an existing dependency.

---

## Design Decisions

### `srun` over `submitit.LocalExecutor`

submitit's `LocalExecutor` handles GPU isolation automatically via
`visible_gpus` and has an existing Prefect bridge through `prefect-submitit`'s
local execution mode. This was the original preferred approach.

However, `LocalExecutor` hardcodes single-node execution:
- `start_controller` sets `SUBMITIT_LOCAL_JOB_NUM_NODES="1"` (hardcoded)
- `_internal_update_parameters` raises `ValueError` if `nodes > 1`
- `start_controller` spawns a local subprocess, no cross-node capability

Since multi-node is a hard requirement, `srun` is the necessary dispatch
mechanism. `srun --exclusive` within an allocation runs immediately (no queue),
distributes across all allocated nodes, and SLURM handles GPU binding via
`--gres`.

### New `ExecutionMode.SRUN`

Rather than building a completely new TaskRunner, extend the existing
`SlurmTaskRunner` with a third execution mode. The runner already handles
two modes (`SLURM` for sbatch, `LOCAL` for subprocess). Adding `SRUN` follows
the established pattern and reuses the existing future/submission infrastructure.

The key difference between modes is only how the process is launched:

| Mode    | Launch      | Queue? | Multi-node? | GPU isolation      |
| ------- | ----------- | ------ | ----------- | ------------------ |
| `SLURM` | `sbatch`    | Yes    | Yes         | SLURM scheduler    |
| `LOCAL`  | subprocess  | No     | No          | `CUDA_VISIBLE_DEVICES` env var |
| `SRUN`  | `srun`      | No     | Yes         | `--gres=gpu:N`     |

### Custom worker script over `submitit.core._submit`

submitit's execution pattern is:

```
Orchestrator                          Worker (remote node)
    │                                     │
    ├─ pickle(callable) → NFS             │
    ├─ launch process ──────────────────► │
    │                                     ├─ unpickle callable
    │                                     ├─ execute
    │                                     ├─ pickle(result) → NFS
    │  poll for result file  ◄─────────────┤
    ├─ unpickle result                    │
```

The srun mode follows the same serialize-launch-collect pattern but uses a
custom worker script (`prefect_submitit.srun_worker`) instead of submitit's
internal `_submit` module.

**Why not `submitit.core._submit`?** The `_submit` module constructs file
paths using `SLURM_JOB_ID` from the environment (e.g.,
`<folder>/<SLURM_JOB_ID>_submitted.pkl`). Within a `salloc` allocation, all
`srun` steps share the same `SLURM_JOB_ID` (the allocation's), so concurrent
tasks would collide on the same file paths even with separate folders. The
`_submit` module is also a private API (`_` prefix) with no stability
guarantees.

The custom worker is ~20 lines, uses well-defined paths (`job.pkl` /
`result.pkl`), and is fully under our control:

---

## Changes

### File overview

- `constants.py` — add `SRUN = "srun"` to `ExecutionMode`
- `runner.py` — add srun path in `__enter__`, `__exit__`, `submit`, and `map`
- `futures/srun.py` (new) — `SrunPrefectFuture` wrapping an srun subprocess
- `srun.py` (new) — srun launch helpers (build command, manage subprocess)
- `srun_worker.py` (new) — worker entry point invoked by srun on compute nodes

All new files live under `src/prefect_submitit/`.

**Note:** The architecture section in CLAUDE.md should be updated to reflect
these new files.

### ExecutionMode

```python
class ExecutionMode(StrEnum):
    SLURM = "slurm"
    LOCAL = "local"
    SRUN = "srun"           # NEW: within existing allocation
```

### SlurmTaskRunner — srun mode entry

In `__enter__`, the srun mode doesn't create a submitit executor. Instead it
prepares a working directory for pickle I/O:

```python
elif self.execution_mode == ExecutionMode.SRUN:
    self._srun_folder = Path(self.log_folder) / "srun"
    self._srun_folder.mkdir(parents=True, exist_ok=True)
    self._srun_procs: list[subprocess.Popen] = []
    # Validate we're inside an allocation
    if not os.environ.get("SLURM_JOB_ID"):
        raise RuntimeError(
            "execution_mode='srun' requires an active SLURM allocation "
            "(SLURM_JOB_ID not set). Use salloc or sbatch first."
        )
```

The `poll_interval` default for srun mode matches local mode (`1.0s`) since
srun tasks run immediately without queue wait:

```python
self.poll_interval = (
    1.0
    if poll_interval is None
    and self.execution_mode in (ExecutionMode.LOCAL, ExecutionMode.SRUN)
    else 5.0
    if poll_interval is None
    else poll_interval
)
```

### SlurmTaskRunner.__exit__ — srun cleanup

In srun mode, the orchestrator owns the child processes. `__exit__` must
terminate them explicitly to avoid orphaned srun processes:

```python
def __exit__(self, *args: Any) -> None:
    if self.execution_mode == ExecutionMode.SRUN and self._srun_procs:
        # SIGTERM all running srun processes
        for proc in self._srun_procs:
            if proc.poll() is None:
                proc.terminate()

        # Wait up to 10s for graceful shutdown
        deadline = time.time() + 10
        for proc in self._srun_procs:
            remaining = max(0, deadline - time.time())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.kill()

        self._srun_procs.clear()

    self._executor = None
    super().__exit__(*args)
```

### SlurmTaskRunner.submit — srun path

The submit method, in srun mode, follows the serialize-launch-collect pattern:

```python
# 1. Serialize the callable (same cloudpickle pattern as SLURM mode)
wrapped_call = cloudpickle_wrapped_call(run_task_in_slurm, env=env, **submit_kwargs)

# 2. Write pickle to shared NFS (well-defined path: job.pkl)
job_folder = self._srun_folder / uuid.uuid4().hex
job_folder.mkdir()
pickle_path = job_folder / "job.pkl"
with open(pickle_path, "wb") as f:
    cloudpickle.dump(wrapped_call, f)

# 3. Launch via srun (non-blocking)
cmd = build_srun_command(self, job_folder)
stdout_fh = open(job_folder / "stdout.log", "w")
stderr_fh = open(job_folder / "stderr.log", "w")
proc = subprocess.Popen(cmd, stdout=stdout_fh, stderr=stderr_fh)

# 4. Track subprocess for cleanup
self._srun_procs.append(proc)

# 5. Return future that polls for result pickle
#    File handles are passed to the future for cleanup on completion.
return SrunPrefectFuture(
    proc, job_folder, task_run_id, ...,
    log_fhs=(stdout_fh, stderr_fh),
)
```

### srun command construction

`build_srun_command` lives in `srun.py` (not on the runner class) following
the existing pattern where submission logic lives in dedicated modules:

```python
def build_srun_command(runner: SlurmTaskRunner, job_folder: Path) -> list[str]:
    """Build the srun command for launching a worker within an allocation."""
    cmd = [
        "srun",
        "--exclusive",       # get dedicated resources within allocation
        "-n1",               # one task
        "--kill-on-bad-exit=1",
    ]
    if runner.gpus_per_node > 0:
        cmd.extend(["--gres", f"gpu:{runner.gpus_per_node}"])
    if runner.mem_gb:
        cmd.extend(["--mem", f"{runner.mem_gb}G"])
    if runner.slurm_kwargs.get("cpus_per_task"):
        cmd.extend(["--cpus-per-task", str(runner.slurm_kwargs["cpus_per_task"])])

    # Use our own worker entry point (not submitit._submit)
    cmd.extend([
        sys.executable, "-u", "-m", "prefect_submitit.srun_worker",
        str(job_folder),
    ])
    return cmd
```

`srun --exclusive` within an allocation:
- Runs immediately (no queue)
- SLURM picks a node with available resources
- `--gres=gpu:N` binds specific GPUs and sets `CUDA_VISIBLE_DEVICES`
- Provides cgroup memory/CPU isolation
- Proper SLURM accounting within the allocation

### srun worker script

`srun_worker.py` is a thin, standalone entry point invoked by `srun` on
compute nodes. It follows the same load-execute-save pattern as submitit but
with simple, fixed file paths:

```python
"""Worker entry point for srun tasks.

Invoked as: python -m prefect_submitit.srun_worker <job_folder>

Protocol:
    - Reads callable from <job_folder>/job.pkl
    - Writes ("success", result) or ("error", traceback) to <job_folder>/result.pkl
"""

from __future__ import annotations

import pickle  # standard pickle can deserialize cloudpickle data
import sys
import traceback
from pathlib import Path


def main() -> None:
    """Load a pickled callable, execute it, and write the result."""
    job_folder = Path(sys.argv[1])
    result_path = job_folder / "result.pkl"

    try:
        with open(job_folder / "job.pkl", "rb") as f:
            fn = pickle.load(f)
        result = fn()
        with open(result_path, "wb") as f:
            pickle.dump(("success", result), f)
    except Exception:
        with open(result_path, "wb") as f:
            pickle.dump(("error", traceback.format_exc()), f)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

The result is always a 2-tuple: `("success", <return_value>)` or
`("error", <traceback_string>)`. The return value from `run_task_in_slurm`
is a Prefect `State` (cloudpickle-serialized), so the full chain is:
`worker → ("success", state_bytes)` → future deserializes → extracts result.

### SrunPrefectFuture

Lives in `futures/srun.py`. Wraps an srun subprocess and polls for the
result pickle on NFS. Inherits from `PrefectFuture[Any]` directly (not
`SlurmPrefectFuture`) because its internal mechanism is fundamentally
different — a `subprocess.Popen` handle rather than a `submitit.Job`.
Implements the full `PrefectFuture` protocol including callbacks:

```python
class SrunPrefectFuture(PrefectFuture[Any]):
    """Future for a task launched via srun within an allocation."""

    def __init__(
        self,
        proc: subprocess.Popen[bytes],
        job_folder: Path,
        task_run_id: uuid.UUID,
        poll_interval: float,
        max_poll_time: float,
        log_fhs: tuple[IO[str], IO[str]] | None = None,
    ):
        self._proc = proc
        self._job_folder = job_folder
        self._result_pickle = job_folder / "result.pkl"
        self._task_run_id = task_run_id
        self._poll_interval = poll_interval
        self._max_poll_time = max_poll_time
        self._log_fhs = log_fhs  # stdout/stderr file handles for cleanup
        self._callbacks: list[Callable[[PrefectFuture[Any]], None]] = []
        self._result_cache: Any = None
        self._result_retrieved = False
        self._done = False

    @property
    def task_run_id(self) -> uuid.UUID:
        return self._task_run_id

    @property
    def state(self) -> State:
        if self._done:
            return Completed()
        if self._proc.poll() is not None:
            if self._proc.returncode == 0:
                return Completed()
            return Failed(message=f"srun exited with code {self._proc.returncode}")
        return Running()

    def wait(self, timeout: float | None = None) -> None:
        """Poll for process completion with timeout."""
        effective_timeout = timeout if timeout is not None else self._max_poll_time
        start = time.time()

        while self._proc.poll() is None:
            elapsed = time.time() - start
            if effective_timeout is not None and elapsed > effective_timeout:
                msg = (
                    f"srun task in {self._job_folder.name} did not complete "
                    f"within {effective_timeout:.0f}s"
                )
                raise TimeoutError(msg)
            time.sleep(self._poll_interval)

        if self._proc.returncode != 0:
            stderr = self._read_log("stderr.log")
            msg = (
                f"srun exited with code {self._proc.returncode}\n"
                f"stderr:\n{stderr}"
            )
            raise SlurmJobFailed(msg)

        self._done = True
        self._close_log_fhs()
        self._fire_callbacks()

    def result(
        self,
        timeout: float | None = None,
        raise_on_failure: bool = True,
    ) -> Any:
        """Read result from the worker's output pickle."""
        if self._result_retrieved:
            return self._result_cache

        self.wait(timeout)

        # NFS cache invalidation: the result was written on a compute node
        # and may not be immediately visible on the orchestrator.
        self._nfs_invalidate(self._result_pickle)

        if not self._result_pickle.exists():
            msg = (
                f"srun process completed but result pickle not found "
                f"at {self._result_pickle}"
            )
            if raise_on_failure:
                raise SlurmJobFailed(msg)
            return None

        with open(self._result_pickle, "rb") as f:
            tag, payload = pickle.load(f)

        if tag == "error":
            if raise_on_failure:
                raise SlurmJobFailed(f"srun worker failed:\n{payload}")
            return None

        # payload is a Prefect State from run_task_in_slurm
        state = cloudpickle.loads(payload) if isinstance(payload, bytes) else payload
        self._result_cache = state.result() if hasattr(state, "result") else state
        self._result_retrieved = True
        return self._result_cache

    def add_done_callback(self, fn: Callable[[PrefectFuture[Any]], None]) -> None:
        self._callbacks.append(fn)
        if self._done:
            fn(self)

    def _fire_callbacks(self) -> None:
        for fn in self._callbacks:
            try:
                fn(self)
            except Exception:
                logger.exception("Callback %s failed", getattr(fn, "__name__", fn))

    def cancel(self) -> bool:
        """Terminate the srun subprocess."""
        self._proc.terminate()
        return True

    def logs(self) -> tuple[str, str]:
        """Read captured stdout/stderr logs."""
        return self._read_log("stdout.log"), self._read_log("stderr.log")

    def _read_log(self, filename: str) -> str:
        path = self._job_folder / filename
        if not path.exists():
            return ""
        try:
            return path.read_text()
        except OSError:
            return ""

    def _close_log_fhs(self) -> None:
        """Close stdout/stderr file handles passed from Popen."""
        if self._log_fhs:
            for fh in self._log_fhs:
                with contextlib.suppress(OSError):
                    fh.close()
            self._log_fhs = None

    @staticmethod
    def _nfs_invalidate(path: Path) -> None:
        """Force NFS attribute cache refresh for a file path.

        On NFS, a file written on one node may not be immediately visible
        on another due to attribute caching.  Opening the parent directory
        and stat'ing the file forces the NFS client to refresh.  Mirrors
        the approach in ``SlurmPrefectFuture._read_log_nfs_safe``.
        """
        with contextlib.suppress(OSError):
            list(path.parent.iterdir())
        if path.exists():
            try:
                fd = os.open(path, os.O_RDONLY)
                try:
                    os.fstat(fd)
                finally:
                    os.close(fd)
            except OSError:
                pass
```

### SlurmTaskRunner.map — srun path

The map method launches N concurrent srun processes. Unlike sbatch job arrays
(which are a single submission), srun tasks are launched individually.

Each `Popen` holds open pipes and OS resources, so launching thousands of
subprocesses at once is problematic. An active-process tracker caps the
number of concurrently live srun subprocesses. Before each launch, completed
processes are reaped from the active list. If the limit is reached and none
have completed, the loop polls with a short sleep until a slot opens:

```python
DEFAULT_SRUN_LAUNCH_CONCURRENCY = 128

def _wait_for_slot(active: list[subprocess.Popen]) -> None:
    """Block until fewer than CONCURRENCY subprocesses are alive."""
    while len(active) >= DEFAULT_SRUN_LAUNCH_CONCURRENCY:
        active[:] = [p for p in active if p.poll() is None]
        if len(active) >= DEFAULT_SRUN_LAUNCH_CONCURRENCY:
            time.sleep(0.1)

# In srun mode, map launches concurrent srun processes with a launch limiter
active: list[subprocess.Popen] = []
futures = []
for i, wrapped_call in enumerate(wrapped_calls):
    _wait_for_slot(active)
    job_folder = self._srun_folder / f"task_{i}_{uuid.uuid4().hex[:8]}"
    job_folder.mkdir()
    _serialize(wrapped_call, job_folder)
    stdout_fh = open(job_folder / "stdout.log", "w")
    stderr_fh = open(job_folder / "stderr.log", "w")
    proc = subprocess.Popen(
        build_srun_command(self, job_folder),
        stdout=stdout_fh,
        stderr=stderr_fh,
    )
    active.append(proc)
    self._srun_procs.append(proc)
    futures.append(SrunPrefectFuture(
        proc, job_folder, task_run_ids[i], ...,
        log_fhs=(stdout_fh, stderr_fh),
    ))
```

SLURM internally queues srun requests within the allocation when resources
are fully utilized, so excess tasks block in srun until a slot opens. The
active-process tracker prevents the orchestrator from accumulating thousands
of blocked subprocesses. Unlike a semaphore-with-callback approach, the
inline reaping loop does not depend on `wait()` being called on the futures,
so it cannot deadlock.

---

## Resolved Questions

**submitit `_submit` module compatibility.** Investigation confirmed that
`_submit` accepts an arbitrary folder path, but constructs file paths using
`SLURM_JOB_ID` from the environment (`<folder>/<job_id>_submitted.pkl`,
`<folder>/<job_id>_<task_id>_result.pkl`). Within a `salloc` allocation,
all srun steps share the same `SLURM_JOB_ID`, which would cause file naming
collisions even with separate folders. Additionally, `_submit` is a private
API. **Decision:** use a custom worker script (`srun_worker.py`) with
fixed, well-defined paths (`job.pkl` / `result.pkl`).

**Concurrency control.** **Decision:** use an active-process tracker with
inline reaping (default cap 128) in the map path. Before each launch,
completed processes are reaped; if the cap is reached the loop polls until
a slot opens. See the map section above.

**Log capture.** **Decision:** redirect `Popen` stdout/stderr to files in
the job folder (`stdout.log`, `stderr.log`). `SrunPrefectFuture.logs()`
reads these directly — no dependency on submitit's log path conventions.

**Cleanup.** **Decision:** pickle files are left in the srun working
directory for post-mortem debugging. The entire `<log_folder>/srun/`
directory can be deleted by the user after the flow completes. `__exit__`
handles subprocess cleanup only, not file cleanup.

---

## Open Questions

**Batching (units_per_worker > 1).** The batched submission path in
`prefect-submitit` groups multiple items into one job. This should work
with srun mode (one srun per batch), but needs verification that
`run_batch_in_slurm` works correctly when invoked via the custom worker.

**`SLURM_STEP_ID` for folder naming.** Each `srun` invocation gets a unique
`SLURM_STEP_ID`. Using this instead of (or alongside) UUIDs for folder
naming would simplify correlation with SLURM accounting
(`sacct -j <jobid>.<stepid>`). Needs investigation into whether the step ID
is available before srun blocks (it may only be assigned at execution time).

---

## Testing

### Test files

- `tests/test_srun_worker.py` — unit tests for the worker script
- `tests/test_srun.py` — unit tests for srun command construction and helpers
- `tests/futures/test_srun.py` — unit tests for `SrunPrefectFuture`
- `tests/integration/test_srun_mode.py` — integration tests requiring a SLURM
  allocation

### Scenarios

**Worker (`test_srun_worker.py`):**
- `test_worker_success` — callable returns value, result.pkl contains
  `("success", value)`
- `test_worker_exception` — callable raises, result.pkl contains
  `("error", traceback_string)`, exit code 1
- `test_worker_missing_pickle` — job.pkl doesn't exist, exits non-zero

**Future (`tests/futures/test_srun.py`):**
- `test_result_success` — process exits 0, result.pkl exists, returns value
- `test_result_failure` — process exits non-zero, raises `SlurmJobFailed`
- `test_result_missing_pickle` — process exits 0 but no result.pkl, raises
  `SlurmJobFailed`
- `test_result_error_tag` — result.pkl contains `("error", ...)`, raises
  `SlurmJobFailed` or returns None based on `raise_on_failure`
- `test_wait_timeout` — process doesn't finish within timeout, raises
  `TimeoutError`
- `test_cancel` — terminates the subprocess
- `test_callbacks_fire_on_done` — `add_done_callback` fires after `wait()`
- `test_logs` — reads stdout.log and stderr.log from job folder

**Command construction (`test_srun.py`):**
- `test_build_command_basic` — minimal args
- `test_build_command_gpu` — includes `--gres` when `gpus_per_node > 0`
- `test_build_command_mem` — includes `--mem` when `mem_gb` is set
- `test_build_command_cpus` — includes `--cpus-per-task` from `slurm_kwargs`

**Runner (`test_srun.py` or integration):**
- `test_enter_validates_slurm_job_id` — raises `RuntimeError` without
  `SLURM_JOB_ID`
- `test_exit_terminates_running_procs` — `__exit__` kills child processes

**Integration (`test_srun_mode.py`):**
- `@pytest.mark.slurm` — tests requiring an active SLURM allocation
- `test_submit_single_task` — submit and collect result via srun
- `test_map_multiple_tasks` — map N tasks, verify all results
- `test_map_with_failure` — one task raises, verify error handling
