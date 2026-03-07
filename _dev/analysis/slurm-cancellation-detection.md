# Analysis: SLURM Job Cancellation Detection

**Date:** 2026-03-07
**Status:** Draft
**Scope:** `prefect_submitit.futures`, `prefect_submitit.executors`, `prefect_submitit.runner`

## Summary

This document analyzes how `prefect-submitit` detects and responds to SLURM job cancellations. The analysis identifies several weaknesses ranging from a critical race condition that can cause cancelled jobs to be treated as successful, to missing cancellation APIs, missing signal handling on compute nodes, and orphaned Prefect task-run states.

---

## Background

### How cancellation currently works

The package monitors SLURM jobs through submitit's `Job` object, which exposes two key APIs:

- **`job.done()`** — returns `True` when the job has reached any terminal state (completed, failed, cancelled, etc.)
- **`job.state`** — returns a string such as `"RUNNING"`, `"COMPLETED"`, `"CANCELLED"`, `"FAILED"`, etc.

The client-side polling loop lives in `SlurmPrefectFuture.wait()` (`futures/base.py:76-101`). It calls `done()` to decide whether to keep looping, and checks `state` against a set of known failure states on each iteration. The SLURM-side execution code (`executors.py`) has no cancellation awareness at all.

### Terminal failure states

Defined in `futures/base.py:33-35`:

```python
TERMINAL_FAILURE_STATES = frozenset(
    {"FAILED", "NODE_FAIL", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY"}
)
```

---

## Issues

### 1. Race condition: `done()` returning `True` bypasses failure detection in `wait()`

**Severity:** Critical
**File:** `futures/base.py:76-101`

This is the most impactful problem. The `wait()` method structure is:

```python
def wait(self, timeout: float | None = None) -> None:
    effective_timeout = timeout or self._max_poll_time
    start = time.time()

    while not self._job.done():          # loop exits when done() is True
        elapsed = time.time() - start
        if effective_timeout and elapsed > effective_timeout:
            raise TimeoutError(...)

        slurm_state = self._job.state    # failure check is INSIDE the loop
        if slurm_state in self.TERMINAL_FAILURE_STATES:
            raise SlurmJobFailed(...)

        time.sleep(self._poll_interval)

    self._done = True                    # no state validation here
    self._fire_callbacks()               # callbacks fire as if successful
```

When a job is cancelled between polls:

1. Poll N: `done()` returns `False`, `state` is `"RUNNING"` — normal.
2. The job is cancelled externally (e.g., `scancel`, admin preemption, resource limit).
3. Poll N+1: `done()` returns `True` because the job reached a terminal state.
4. The `while` loop exits **without ever checking `state`** on this iteration.
5. `self._done` is set to `True` and callbacks fire as though the job succeeded.
6. Only later, when `result()` calls `self._job.result()`, does submitit raise `FailedJobError` or `UncompletedJobError`.

**Consequences:**

- `wait()` completes without error for cancelled jobs.
- Done-callbacks receive a future that appears successful.
- Any code path that calls `wait()` without subsequently calling `result()` will never learn the job was cancelled.
- The actual failure is deferred and may surface in an unexpected context.

**Recommended fix:** Add a post-loop state check before marking the future as done:

```python
while not self._job.done():
    # ... existing loop body ...

# Validate final state after loop exits
slurm_state = self._job.state
if slurm_state in self.TERMINAL_FAILURE_STATES:
    try:
        stderr = self._job.stderr()
    except Exception:
        stderr = "(stderr unavailable)"
    msg = f"Job {self.slurm_job_id}: {slurm_state}\nstderr:\n{stderr}"
    raise SlurmJobFailed(msg)

self._done = True
self._fire_callbacks()
```

---

### 2. Base `SlurmPrefectFuture` has no `cancel()` method

**Severity:** High
**File:** `futures/base.py`

Only `SlurmArrayPrefectFuture` implements `cancel()` and `cancel_task()` (via `scancel` subprocess calls in `futures/array.py:59-79`). The base `SlurmPrefectFuture` — returned by `SlurmTaskRunner.submit()` for every single-task submission — provides no cancellation API.

This means users cannot programmatically cancel a single submitted task. It also means any framework-level cancellation (e.g., Prefect flow cancellation propagating to task runners) has no mechanism to reach single-task jobs.

**Recommended fix:** Add a `cancel()` method to `SlurmPrefectFuture`:

```python
def cancel(self) -> bool:
    try:
        subprocess.run(
            ["scancel", self.slurm_job_id],
            check=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False
```

---

### 3. `CANCELLED by <uid>` state variant may not be recognized

**Severity:** High
**File:** `futures/base.py:33-35, 70, 90`

SLURM's `sacct` output can report cancellation as `CANCELLED by <uid>` (e.g., `CANCELLED by 1000`), indicating who cancelled the job. The current detection uses exact set membership:

```python
if slurm_state in self.TERMINAL_FAILURE_STATES:
```

If submitit passes through the raw `sacct` state without normalizing it, the string `"CANCELLED by 1000"` will **not** match `"CANCELLED"` in the frozenset, and the cancellation will go undetected.

Whether this actually occurs depends on submitit's internal normalization behavior, which is not guaranteed by its API contract and could change between versions.

**Recommended fix:** Use prefix matching for `CANCELLED` in addition to exact matching:

```python
if slurm_state in self.TERMINAL_FAILURE_STATES or slurm_state.startswith("CANCELLED"):
```

Alternatively, define a helper method so the logic is centralized:

```python
def _is_terminal_failure(self, slurm_state: str) -> bool:
    return (
        slurm_state in self.TERMINAL_FAILURE_STATES
        or slurm_state.startswith("CANCELLED")
    )
```

---

### 4. No Prefect backend state update when cancellation is detected client-side

**Severity:** High
**File:** `futures/base.py:89-96`

When the client-side polling detects a `CANCELLED` (or other failure) state, it raises `SlurmJobFailed`. It does **not** update the Prefect task run state in the backend. Since the SLURM-side process was killed by SLURM (and therefore never had a chance to report its final state), the task run can remain as `Running` in the Prefect UI indefinitely.

This affects two code paths:

- **`wait()` failure detection** (line 90-96): raises `SlurmJobFailed` but doesn't call `set_task_run_state`.
- **`result()` exception handlers** (lines 124-136): catches `FailedJobError`/`UncompletedJobError` and either raises or returns `None`, but never updates the backend.

**Recommended fix:** Before raising `SlurmJobFailed`, make a best-effort attempt to update the Prefect backend:

```python
try:
    from prefect.client.orchestration import get_client
    client = get_client(sync_client=True)
    client.set_task_run_state(
        task_run_id=self._task_run_id,
        state=Failed(message=f"SLURM: {slurm_state}"),
    )
except Exception:
    logger.debug("Could not update Prefect task run state for %s", self.slurm_job_id)
```

This should be centralized in a private method to avoid duplication across `wait()` and `result()`.

---

### 5. No SLURM-side signal handling for graceful shutdown

**Severity:** Medium
**File:** `executors.py`

When SLURM cancels a job it sends `SIGTERM`, waits a configurable grace period, then sends `SIGKILL`. Neither `run_task_in_slurm()` nor `run_batch_in_slurm()` register signal handlers. This means:

- **Batch processing** (`run_batch_in_slurm`): If cancelled mid-batch, all partial results are lost. The function has a `try/except` safety net (line 211-233) but that only catches Python exceptions, not signals.
- **Prefect task run state**: The SLURM-side code created the task run with `state=Running()` (line 144). If killed, it never transitions to `Failed` or `Cancelled`. The Prefect backend retains the `Running` state.
- **Resource cleanup**: Any open file handles, database connections, or temporary files are abandoned.

**Recommended fix:** Register a `SIGTERM` handler at the start of the executor functions:

```python
import signal

def _sigterm_handler(signum, frame):
    raise SystemExit("SLURM job cancelled (SIGTERM)")

signal.signal(signal.SIGTERM, _sigterm_handler)
```

By converting `SIGTERM` to `SystemExit`, the existing `try/except` safety nets in `run_batch_in_slurm()` will execute, and context managers will unwind normally.

For batch processing specifically, the handler could save partial results before exiting.

---

### 6. `SlurmTaskRunner.__exit__()` does not cancel running jobs

**Severity:** Medium
**File:** `runner.py:162-164`

```python
def __exit__(self, *args: Any) -> None:
    self._executor = None
    super().__exit__(*args)
```

If a flow exits early (exception, user interrupt, etc.), any SLURM jobs that were submitted but haven't completed continue running on the cluster, consuming resources. There is no tracking of submitted futures and no cleanup on exit.

**Recommended fix:** Track submitted futures and cancel on exit:

```python
def __init__(self, ...):
    ...
    self._submitted_futures: list[SlurmPrefectFuture] = []

def submit(self, ...) -> SlurmPrefectFuture:
    ...
    future = SlurmPrefectFuture(...)
    self._submitted_futures.append(future)
    return future

def __exit__(self, *args: Any) -> None:
    for future in self._submitted_futures:
        if not future.is_done:
            try:
                future.cancel()
            except Exception:
                pass
    self._submitted_futures.clear()
    self._executor = None
    super().__exit__(*args)
```

This should likely be gated behind a configuration flag (e.g., `cancel_on_exit=True`) since some users may want SLURM jobs to outlive the submitting process.

---

### 7. Timeout errors mask the actual job state

**Severity:** Low
**File:** `futures/base.py:81-87`

When `max_poll_time` expires, the error message does not include the current SLURM state:

```python
if effective_timeout and elapsed > effective_timeout:
    msg = (
        f"Job {self.slurm_job_id} did not complete "
        f"within {effective_timeout:.0f}s"
    )
    raise TimeoutError(msg)
```

If a job was cancelled but the cancellation hasn't been detected yet (because it occurred moments before the timeout), the user sees a `TimeoutError` with no indication that the job was actually cancelled. This sends users on the wrong debugging path.

**Recommended fix:** Include the current state in the timeout message:

```python
if effective_timeout and elapsed > effective_timeout:
    current_state = self._job.state
    msg = (
        f"Job {self.slurm_job_id} did not complete "
        f"within {effective_timeout:.0f}s "
        f"(last observed state: {current_state})"
    )
    raise TimeoutError(msg)
```

---

### 8. Default 30-second poll interval delays cancellation detection

**Severity:** Low
**File:** `runner.py:98-104`

The default `poll_interval` for SLURM mode is 30 seconds. A cancelled job may not be detected for up to 30 seconds after cancellation. While this is a reasonable default to avoid overloading the SLURM scheduler, it can be surprising for interactive use cases.

**Possible improvements (non-breaking):**

- **Adaptive polling:** Start with short intervals (e.g., 5s) and increase to the configured maximum over time. This gives faster feedback for quick failures while still being gentle on the scheduler for long-running jobs.
- **Documentation:** Clarify in the README that `poll_interval` directly affects cancellation detection latency.

---

### 9. Batched item futures don't propagate cancellation to siblings

**Severity:** Low
**File:** `futures/batched.py`

When a batched SLURM job is cancelled, each `SlurmBatchedItemFuture` sharing that job will independently discover the failure when its `wait()` or `result()` is called. There is no mechanism to eagerly notify sibling item futures that their underlying job was cancelled.

In practice this means that if a caller iterates over batched item futures sequentially, the first one will raise on `wait()`/`result()`, and the remaining futures will each redundantly poll the same cancelled job.

This is a minor efficiency concern rather than a correctness issue, but it could be improved by having the underlying `SlurmArrayPrefectFuture` cache and propagate its failure state to all associated item futures.

---

## Priority Summary

| Priority | Issue | Impact | Effort |
|----------|-------|--------|--------|
| P0 | 1. Race condition: `done()=True` bypasses failure check | Cancelled jobs treated as successful | Low |
| P1 | 2. No `cancel()` on base `SlurmPrefectFuture` | Cannot cancel single-task submissions | Low |
| P1 | 3. `CANCELLED by <uid>` variant not recognized | Some cancellations missed entirely | Low |
| P1 | 4. No Prefect backend state update on cancellation | Task runs stuck as Running in UI | Medium |
| P2 | 5. No SIGTERM handler on SLURM compute nodes | Partial results lost, orphaned states | Medium |
| P2 | 6. `__exit__` doesn't cancel running jobs | Resource waste on early exit | Medium |
| P3 | 7. Timeout masks actual job state | Confusing error messages | Low |
| P3 | 8. 30s poll interval delays detection | Slow feedback | Low |
| P3 | 9. Batched siblings don't share cancellation | Redundant polling | Low |

---

## Recommended Implementation Order

1. **Fix the `wait()` race condition** (Issue 1) — this is a correctness bug with a small, targeted fix.
2. **Add `cancel()` to base future** (Issue 2) — straightforward addition, enables all downstream improvements.
3. **Handle `CANCELLED by` variant** (Issue 3) — one-line defensive fix.
4. **Update Prefect backend on failure detection** (Issue 4) — requires plumbing a Prefect client call into the futures module.
5. **Add SIGTERM handler** (Issue 5) — improves SLURM-side resilience, pairs well with Issue 4.
6. **Cancel-on-exit in task runner** (Issue 6) — requires tracking submitted futures, should be opt-in.
7. **Improve error messages** (Issues 7, 8, 9) — quality-of-life improvements.
