# Design: Revised `srun` Execution Mode for prefect-submitit

**Date:** 2026-03-30
**Status:** Draft
**Repo:** `prefect-submitit`
**Supersedes:** [Original `srun` design](srun.md)
**Related:** [SLURM Intra-Allocation Backend (artisan)](slurm-intra.md)

---

## Context

`prefect-submitit` currently has two execution modes under one public
`SlurmTaskRunner`:

- `SLURM` submits work through `submitit.AutoExecutor`
- `LOCAL` runs work through `submitit.LocalExecutor`

Both modes share the same high-level flow:

1. Build a cloudpickle-wrapped callable for Prefect task execution
2. Launch that callable through a mode-specific transport
3. Return a Prefect future that polls for completion and decodes the result

An `srun` mode should fit into this same model. The original design treated
`SRUN` as a set of special-case branches in `runner.py`. That would work, but
it does not line up well with the existing codebase shape:

- `runner.py` is the public Prefect-facing entry point
- `submission.py` already owns callable construction and array submission logic
- `futures/` already owns mode-specific future behavior
- `executors.py` already owns worker-side task execution and naming

The revised design keeps that split intact. `SRUN` becomes a third internal
backend under `SlurmTaskRunner`, not a separate runner and not an ad hoc bypass
around the existing architecture.

---

## Design Goals

**Preserve the public runner API.** `SlurmTaskRunner` remains the only public
TaskRunner. `SRUN` is selected through `ExecutionMode`.

**Preserve existing feature coverage.** `submit()`, `map()`, and
`units_per_worker > 1` batching should work in `SRUN` mode unless explicitly
documented otherwise. Since batching is already part of current behavior, it
should not be left unspecified.

**Support multi-node allocations.** `SRUN` is for dispatching many one-node
task launches across a multi-node allocation. It is not a design for
multi-node single-task execution.

**Stay aligned with current module responsibilities.** Shared callable
construction should live in `submission.py`. Worker-side task naming should
live in `executors.py`. Mode-specific polling and log behavior should live in
`futures/`.

**Avoid private submitit internals.** The design must not depend on
`submitit.core._submit`.

**Fail fast on unsupported configuration.** `SRUN` runs inside an existing
allocation, so many allocation-level SLURM options do not make sense. The
design must say exactly which settings are supported, ignored with warning, or
rejected.

---

## Non-Goals

**No new public TaskRunner class.** `SRUN` is not a separate runner type.

**No long-lived worker pool in v1.** This design launches one `srun` step per
task or per batch. It does not introduce a daemon, RPC layer, or step-local
queue.

**No broad passthrough of arbitrary `srun` flags.** The design supports a
small explicit subset of step-level resource options. Additional flags can be
added later if needed.

---

## Design Decision: Internal `SRUN` Backend Under `SlurmTaskRunner`

`SlurmTaskRunner` gains a private `_backend` object created in `__enter__`.
That backend owns launch and cleanup behavior for the chosen execution mode.

Two backend families exist:

- `SubmititBackend` for `SLURM` and `LOCAL`
- `SrunBackend` for `SRUN`

This is an internal implementation detail, not a public abstraction.

### Why introduce `_backend`?

Today, `runner.py` uses `_executor is None` as both:

- the “not entered” sentinel
- the transport handle

That works for `SLURM` and `LOCAL`, but not for `SRUN`, which has no submitit
executor. A private backend object cleanly separates “runner has been entered”
from “submitit executor exists”.

### Private backend contract

The backend only needs three operations:

```python
class _RunnerBackend(Protocol):
    def submit_one(
        self,
        wrapped_call: Callable[..., Any],
        task_run_id: uuid.UUID,
        fail_on_error: bool = True,
    ) -> PrefectFuture[Any]: ...

    def submit_many(
        self,
        wrapped_calls: list[Callable[..., Any]],
        task_run_ids: list[uuid.UUID],
        fail_on_error: bool,
    ) -> list[PrefectFuture[Any]]: ...

    def close(self) -> None: ...
```

`submit()` and `map()` in `runner.py` build the work and delegate the launch.
They no longer care whether the transport is `sbatch`, local submitit, or
`srun`.

---

## Architecture

### High-level flow

```
SlurmTaskRunner
    │
    ├─ build_task_callable / build_array_callable / build_batch_callable
    │        (submission.py)
    │
    ├─ hand wrapped call(s) to backend
    │     ├─ SubmititBackend -> submitit.Job
    │     └─ SrunBackend     -> subprocess.Popen
    │
    └─ return Prefect futures
          ├─ SlurmPrefectFuture / SlurmArrayPrefectFuture
          └─ SrunPrefectFuture
```

### Shared callable construction

The current single-submit path builds its wrapped callable directly in
`runner.py`, while map/batch callable builders already live in
`submission.py`. The revised design removes that split.

`submission.py` becomes the canonical home for:

- `build_task_callable(...)`
- `build_array_callable(...)`
- `build_batch_callable(...)`

This gives all execution modes one shared source of truth for:

- resolved parameters
- Prefect context serialization
- environment propagation
- `return_type="state"` semantics

### Worker-side execution

`executors.py` continues to own worker-side task execution functions. The
revised design adds a small helper there for task-run naming so `SLURM`,
`LOCAL`, and `SRUN` all use the same logic.

---

## `SRUN` Execution Model

### What `SRUN` means

`SRUN` is a step-launch mode for work inside an already-allocated SLURM job.
It does not submit new jobs to the queue.

Each Prefect task launch becomes one `srun` step:

- one task submitted via `submit()` -> one `srun` step
- one item in non-batched `map()` -> one `srun` step
- one batch in `units_per_worker > 1` mode -> one `srun` step

This allows a head process to fan work out across a multi-node allocation while
keeping Prefect’s existing task orchestration flow intact.

### Folder layout

Each `srun` launch gets a dedicated working folder on shared storage:

```text
<log_folder>/<allocation_job_id>/srun/<task_run_id>/
    job.pkl
    result.pkl
    stdout.log
    stderr.log
    metadata.json        # optional, written by worker when available
```

Using the allocation job id in the path keeps `SRUN` logs aligned with the
existing `slurm_logs/%j` convention used by `submitit`.

`task_run_id` is used for pre-launch uniqueness. If desired, the worker may
record `SLURM_STEP_ID` into `metadata.json` after the step starts.

### Supported configuration

`SRUN` only supports step-level options plus runner-local polling settings.

| Setting | `SRUN` behavior |
| --- | --- |
| `time_limit` | Supported, mapped to `srun --time` |
| `mem_gb` | Supported, mapped to `srun --mem` |
| `gpus_per_node` | Supported, mapped to `--gres=gpu:N` |
| `slurm_gres` | Supported, mapped to `--gres=<value>` |
| `cpus_per_task` in `slurm_kwargs` | Supported, mapped to `--cpus-per-task` |
| `log_folder` | Supported |
| `poll_interval` | Supported |
| `max_poll_time` | Supported |
| `fail_on_error` | Supported |
| `units_per_worker` | Supported |
| `srun_launch_concurrency` | Supported, runner-local cap on concurrent live `srun` client processes |
| `partition` | Ignored with warning |
| `slurm_array_parallelism` | Ignored with warning |
| `max_array_size` | Ignored with warning |
| other `slurm_kwargs` | Rejected or ignored with explicit warning unless mapped by the implementation |

### `gpus_per_node` vs `slurm_gres`

The same mutual-exclusion rule used in `SLURM` mode applies here:

- if `slurm_gres` is provided, it wins
- if not, `gpus_per_node > 0` maps to `--gres=gpu:N`

This keeps `SRUN` consistent with the current `SLURM` behavior and tests.

### Why `srun_launch_concurrency`?

`slurm_array_parallelism` is specific to job arrays. `SRUN` does not use job
arrays, so reusing that field would blur two different controls.

`srun_launch_concurrency` caps the number of concurrently live `srun` client
processes owned by the orchestrator. This protects the head process from
unbounded subprocess growth during large `map()` calls.

---

## Worker Result Protocol

The worker protocol must distinguish between:

- task execution success/failure encoded as Prefect state bytes
- worker/bootstrap failure outside Prefect task execution

The revised design uses a structured result envelope written to `result.pkl`:

```python
{
    "status": "ok",
    "payload": <bytes>,
}

{
    "status": "error",
    "message": <str>,
    "traceback": <str>,
}
```

### Worker behavior

`srun_worker.py` performs:

1. load `job.pkl`
2. execute the wrapped callable
3. write a result envelope to `result.pkl`

If the worker successfully writes either envelope, it exits with code `0`.

The worker exits non-zero only if it cannot complete the protocol itself, for
example:

- `job.pkl` cannot be read
- the result file cannot be written
- the worker crashes before envelope creation

### Why exit `0` on a serialized error?

This removes the contradiction in the original design where:

- the worker wrote an error payload
- but the future raised on non-zero exit before reading it

With the revised protocol:

- `wait()` means “the step finished and the worker protocol completed”
- `result()` means “decode the envelope and surface task or worker failure”

This matches the existing `SlurmPrefectFuture` pattern, where completion and
result decoding are separate concerns.

---

## Futures

### `SrunPrefectFuture`

A new `SrunPrefectFuture` lives in `src/prefect_submitit/futures/srun.py`.

It wraps:

- the `Popen` handle for the `srun` client process
- the task run id
- the step folder paths
- polling configuration
- `fail_on_error` behavior for map-style launches

Responsibilities:

- wait for process exit
- inspect `result.pkl`
- decode Prefect state bytes using the same logic as submitit-based futures
- read logs from file paths
- terminate the process on cancellation

### Log handling

The future stores log file paths, not open file handles.

The orchestrator opens log files for `Popen`, launches the process, and closes
its own file descriptors immediately. This keeps descriptor growth bounded even
for large `map()` calls.

### Shared future behavior

If useful, small helpers may be extracted from `futures/base.py` for:

- callback registration
- result caching
- state-byte decoding

The goal is behavioral consistency, not a deep inheritance tree.

### Batched item futures

The current batched-item future in `futures/batched.py` should be generalized
to wrap any parent future that provides:

- `wait()`
- `result()`
- `state`

That allows batching to work in `SRUN` mode without duplicating the batched
future implementation.

---

## Task Naming

Worker-side task naming remains in `executors.py`, but is routed through a
shared helper instead of inline environment probing.

Priority order for task-run names:

1. explicit override set by launcher env
2. `SLURM_ARRAY_JOB_ID` + `SLURM_ARRAY_TASK_ID`
3. `SLURM_JOB_ID` + `SLURM_STEP_ID`
4. `SLURM_JOB_ID`
5. no override

This preserves current array naming while fixing the `SRUN` case where every
step inside one allocation would otherwise appear as the same
`slurm-<jobid>`.

---

## Runner Behavior

### `__enter__`

`SlurmTaskRunner.__enter__` constructs the private backend object and stores it
on `self._backend`.

For `SRUN`, `__enter__` also:

- validates `SLURM_JOB_ID` is present
- computes the allocation-scoped log root
- validates/warns on unsupported settings

### `__exit__`

`__exit__` delegates to `self._backend.close()`.

For `SRUN`, that means:

- terminate still-running `srun` client processes
- wait briefly for exit
- kill if needed

No file cleanup is performed automatically. Step folders are preserved for
debugging.

### `submit()`

`submit()` becomes:

1. build a task callable via `submission.build_task_callable(...)`
2. call `self._backend.submit_one(...)`
3. return the backend-provided future

This removes mode-specific callable-building duplication from `runner.py`.

### `map()`

Non-batched `map()`:

1. resolve iterable/static params
2. build one task callable per item
3. call `self._backend.submit_many(...)`
4. return a `PrefectFutureList`

Batched `map()`:

1. batch the items
2. build one batch callable per batch
3. call `self._backend.submit_many(...)`
4. wrap each parent future with batched item futures

This keeps `units_per_worker > 1` fully specified in all modes.

### `fail_on_error`

`SRUN` map-created futures should honor `runner.fail_on_error` the same way
`SlurmArrayPrefectFuture` does today. This keeps error-handling behavior
consistent across transports.

---

## File Changes

### Source files

- `src/prefect_submitit/constants.py`
  - add `ExecutionMode.SRUN`
- `src/prefect_submitit/runner.py`
  - introduce `_backend`
  - validate `SRUN` mode in `__enter__`
  - route `submit()` and `map()` through shared callable builders and backend methods
- `src/prefect_submitit/submission.py`
  - add `build_task_callable(...)`
  - keep callable construction centralized
- `src/prefect_submitit/srun.py` (new)
  - `SrunBackend`
  - command construction
  - folder creation
  - `srun` launch concurrency limiting
- `src/prefect_submitit/srun_worker.py` (new)
  - worker entry point and result envelope protocol
- `src/prefect_submitit/futures/srun.py` (new)
  - `SrunPrefectFuture`
- `src/prefect_submitit/futures/batched.py`
  - generalize parent future type to support `SRUN`
- `src/prefect_submitit/futures/__init__.py`
  - export `SrunPrefectFuture`
- `src/prefect_submitit/__init__.py`
  - export `SrunPrefectFuture`
- `src/prefect_submitit/executors.py`
  - extract shared task-run naming helper with `SLURM_STEP_ID` support

### Tests

- `tests/test_constants.py`
  - add `ExecutionMode.SRUN`
- `tests/test_runner.py`
  - `SRUN` mode init, warnings, enter/exit, submit, map, batching
- `tests/test_executors.py`
  - task naming with `SLURM_STEP_ID`
- `tests/test_submission.py`
  - `build_task_callable(...)`
- `tests/test_srun.py` (new)
  - command construction, folder layout, launch concurrency helpers
- `tests/test_srun_worker.py` (new)
  - worker result envelope behavior
- `tests/futures/test_srun.py` (new)
  - `SrunPrefectFuture`
- `tests/integration/test_srun_mode.py` (new)
  - active-allocation integration coverage

### Documentation

- `CLAUDE.md`
  - update architecture section for new `srun` modules

---

## Testing Plan

### Unit tests

`tests/test_runner.py`

- `test_srun_uses_backend`
- `test_srun_requires_slurm_job_id`
- `test_srun_warns_on_ignored_settings`
- `test_srun_submit_builds_task_callable`
- `test_srun_map_non_batched`
- `test_srun_map_batched`
- `test_srun_exit_terminates_live_processes`

`tests/test_submission.py`

- `test_build_task_callable_returns_wrapped_call`
- `test_build_task_callable_uses_return_type_state`

`tests/test_executors.py`

- `test_task_run_name_from_job_and_step_id`
- `test_explicit_task_run_name_override_wins`

`tests/test_srun_worker.py`

- `test_worker_writes_ok_envelope`
- `test_worker_writes_error_envelope_and_exits_zero`
- `test_worker_missing_job_pickle_exits_nonzero`

`tests/futures/test_srun.py`

- `test_wait_success`
- `test_wait_nonzero_exit_without_result_raises`
- `test_result_ok_envelope_returns_value`
- `test_result_error_envelope_raises_or_returns_none`
- `test_logs_read_from_paths`
- `test_cancel_terminates_process`
- `test_callbacks_fire_on_completion`

### Integration tests

`tests/integration/test_srun_mode.py`

- `@pytest.mark.slurm`
- `test_submit_single_task_inside_allocation`
- `test_map_multiple_tasks_inside_allocation`
- `test_batched_map_inside_allocation`
- `test_task_names_include_step_id`
- `test_failure_surfaces_from_result_envelope`

---

## Migration and Compatibility

Public API remains:

- `SlurmTaskRunner`
- `ExecutionMode`

New public surface:

- `ExecutionMode.SRUN`
- `SrunPrefectFuture`
- `srun_launch_concurrency` runner argument

Behavioral notes:

- `SRUN` warns on ignored allocation-level settings instead of silently
  pretending to support them.
- `SRUN` task naming includes step identity when available.
- `SRUN` batching is fully supported in this design.

---

## Alternatives Considered

### 1. Keep all `SRUN` branching inline in `runner.py`

**Rejected:** Too much transport-specific behavior would leak into the public
runner path, and the `_executor` sentinel problem would remain awkward.

### 2. Create a brand-new `SrunTaskRunner`

**Rejected:** Would duplicate the existing public API and break the current
mental model where execution mode is a runner configuration choice.

### 3. Treat `units_per_worker > 1` as unsupported in `SRUN`

**Rejected for this design:** It would simplify initial implementation, but it
would also leave an existing public feature path inconsistent across modes.
If implementation pressure requires this cut, it should be a deliberate v1
scope reduction with explicit validation and documentation, not an open
question.

---

## Implementation Order

1. Extract no-behavior-change refactors:
   - `build_task_callable(...)`
   - task-run naming helper
   - `_backend` sentinel
2. Add `SrunBackend`, `srun_worker.py`, and `SrunPrefectFuture`
3. Enable `submit()` in `SRUN`
4. Enable non-batched `map()` in `SRUN`
5. Enable batched `map()` in `SRUN`
6. Add integration tests inside an allocation

This keeps the diff staged and makes each phase testable.
