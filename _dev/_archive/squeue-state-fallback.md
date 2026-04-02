# Design: squeue Fallback for Job State Detection

**Date:** 2026-04-01
**Status:** Draft
**Scope:** `src/prefect_submitit/futures/base.py`

---

## Context

Submitit uses `sacct` to query SLURM job states. When `sacct` is unavailable
(e.g. `AccountingStorageType=accounting_storage/none`), submitit returns
`UNKNOWN` for all state queries. Our `SlurmPrefectFuture.state` property
maps `UNKNOWN` to `Pending()`, which means:

- `wait_for_running()` never detects RUNNING state → cancel tests skip
- Any code that branches on job state is blind
- Job *completion* still works (submitit detects it via output files)

This affects the Docker SLURM dev environment and any cluster with broken
or missing accounting storage.

---

## Goals

1. Accurate job state reporting when `sacct` is unavailable.
2. No behavior change when `sacct` works normally.

## Non-Goals

- Fixing submitit itself (upstream library).
- Replacing submitit's state detection entirely.

---

## Proposed Solution

When submitit returns `UNKNOWN`, fall back to `squeue` to query the job
state directly. `squeue` queries the scheduler (slurmctld) and works
without accounting storage.

### Implementation

In `SlurmPrefectFuture.state` (`src/prefect_submitit/futures/base.py`):

```python
@property
def state(self) -> State:
    if self.is_done:
        return Completed()
    slurm_state = self._job.state
    if slurm_state == "UNKNOWN":
        slurm_state = self._query_squeue()
    normalized = slurm_state.rstrip("+")
    if normalized in ("COMPLETED", "COMPLETING"):
        return Completed()
    if self._is_terminal_failure(slurm_state):
        return Failed(message=f"SLURM: {slurm_state}")
    if normalized == "RUNNING":
        return Running()
    return Pending()
```

New private method:

```python
def _query_squeue(self) -> str:
    """Query squeue for job state when sacct is unavailable."""
    try:
        result = subprocess.run(
            ["squeue", "-j", self.slurm_job_id, "-h", "-o", "%T"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        state = result.stdout.strip()
        if state:
            return state
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return "UNKNOWN"
```

### Behavior

| `sacct` works | `squeue` works | Result |
|---|---|---|
| Yes | — | Normal path (submitit returns real state) |
| No | Yes | Fallback to squeue → accurate state |
| No | No | Returns UNKNOWN → maps to Pending (current behavior) |

### Edge case: job already completed

`squeue` only shows active jobs. If a job finishes between the submitit
`sacct` call and our `squeue` fallback, `squeue` returns empty output.
In that case `_query_squeue` returns `UNKNOWN`, which maps to `Pending()`.
This is a transient mis-report — the next poll cycle will detect
completion via submitit's file-based check. No action needed.

---

## Files Changed

| File | Change |
|------|--------|
| `src/prefect_submitit/futures/base.py` | Add `_query_squeue`, modify `state` property |
| `tests/futures/test_base.py` | Test UNKNOWN → squeue fallback |

---

## Verification

1. Existing tests pass (`pixi run -e dev test`).
2. Inside Docker container: submit a long-running job, verify `future.state`
   transitions from Pending → Running → Completed.
3. Cancel integration tests pass inside Docker (`pixi run -e dev test-slurm-docker`).
