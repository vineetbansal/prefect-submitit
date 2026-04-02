# Plan: `srun` Execution Mode for prefect-submitit

**Date:** 2026-03-31
**Status:** Final synthesis
**Inputs:** Original design, readiness review, revised design, landscape
analysis, gap analysis

---

## What We're Building

A third execution mode (`SRUN`) for `SlurmTaskRunner` that dispatches Prefect
tasks across a multi-node SLURM allocation using `srun`, without going through
the scheduler queue. Users who already hold resources via `salloc` or `sbatch`
get immediate task execution with full SLURM accounting — no queue latency, no
resource fragmentation, no new dependencies.

The user-facing change is minimal: set `execution_mode="srun"` on the same
`SlurmTaskRunner` they already use.

```python
runner = SlurmTaskRunner(execution_mode="srun", gpus_per_node=1)

@flow(task_runner=runner)
def train_pipeline():
    results = preprocess.map(shards)    # fans across allocated nodes via srun
    model = train(results)
```

---

## Why srun-per-task

The landscape analysis surveyed four dispatch patterns: srun-per-task,
persistent worker pools (Parsl/Dask/Ray), MPI-based (mpi4py.futures), and
hierarchical sub-schedulers (Flux). For our target workload — 10–1000
independent tasks with minute-plus runtimes inside ML allocations —
srun-per-task is the right tradeoff:

- **No new dependencies.** `srun` is on every SLURM cluster.
- **SLURM-native accounting.** Each task is a SLURM step visible in `sacct`.
- **No persistent infrastructure.** No worker daemons, ZMQ brokers, or MPI
  processes to manage or debug.
- **Simple failure model.** A crashed task is a dead subprocess; nothing else
  to clean up.
- **~100ms per-task overhead** is negligible against minute-plus runtimes.

Inspiration is drawn from executorlib (dual-executor pattern, per-task srun),
Snakemake's jobstep plugin (same engine, different dispatch), and submitit's
proven serialize-launch-collect pattern via cloudpickle.

---

## Core Design

### Architecture: internal backend, not inline branching

`SlurmTaskRunner` gains a private `_backend` object created in `__enter__`.
This cleanly separates "runner has been entered" from "submitit executor
exists" — necessary because `SRUN` has no submitit executor.

```
SlurmTaskRunner
    ├── build callable        (submission.py — shared across all modes)
    ├── hand to backend
    │     ├── SubmititBackend  (SLURM, LOCAL — wraps submitit executor)
    │     └── SrunBackend      (SRUN — manages srun subprocesses)
    └── return future
          ├── SlurmPrefectFuture / SlurmArrayPrefectFuture
          └── SrunPrefectFuture
```

The backend protocol is three methods: `submit_one`, `submit_many`, `close`.
This is a private implementation detail, not a public abstraction.

### Worker protocol: serialize → launch → collect

Each task follows submitit's proven pattern with a custom worker script:

1. **Serialize** — cloudpickle the wrapped Prefect callable to `job.pkl`
2. **Launch** — `srun --exact --mpi=none -n1 python -m prefect_submitit.srun_worker <folder>`
3. **Collect** — worker writes structured result envelope to `result.pkl`;
   future polls for process exit, then reads the envelope

The worker exits 0 when the protocol completes (even if the task itself
failed — the error is in the envelope). Non-zero exit means the worker
itself broke (can't read pickle, can't write result). This avoids the
original design's contradiction where `wait()` raised before `result()`
could read the error payload.

Result writes use atomic temp-file-then-rename to prevent NFS partial reads.
On the read side, process exit acts as the synchronization point: the future
waits for the srun subprocess to exit before reading `result.pkl`, so no
additional NFS cache invalidation is needed (unlike log reads, which can race
with still-running processes).

### srun command flags

```
srun --exact --mpi=none -n1 [--gres=gpu:N] [--mem=XG] [--cpus-per-task=C] [--time=T] \
     python -u -m prefect_submitit.srun_worker <job_folder>
```

- `--exact` — request only what's specified, enabling concurrent steps on
  one node (replaces `--exclusive`, which claims the entire node)
- `--mpi=none` — skip PMI initialization to prevent hangs on clusters with
  `MpiDefault=pmix`
- No `--kill-on-bad-exit` — only relevant for multi-task steps (`-n > 1`)

### Concurrency control

`map()` launches one srun per item (or per batch when `units_per_worker > 1`).
An active-process tracker caps concurrent live `Popen` handles (default 128)
with a minimum 50ms inter-launch interval to avoid overwhelming `slurmctld`.
SLURM itself queues steps that exceed available resources, so the cap protects
the orchestrator's file descriptors, not the cluster.

### Task naming

A shared naming helper in `executors.py` uses a priority chain:
`SLURM_ARRAY_JOB_ID+TASK_ID` → `SLURM_JOB_ID+STEP_ID` → `SLURM_JOB_ID`.
Both `run_task_in_slurm` (existing) and `srun_worker.py` (new) call this
helper. This fixes the original problem where every step in one allocation
appeared as the same `slurm-<jobid>` and enables `sacct -j <jobid>.<stepid>`
correlation from the Prefect UI.

### Allocation validation

`__enter__` validates that `SLURM_JOB_ID` is set when `execution_mode="srun"`,
raising `RuntimeError` if absent. Without an existing allocation, `srun`
silently falls back to scheduler allocation — per-task queue latency that
defeats the purpose of this mode.

### Error handling on non-zero exit

When the srun process exits non-zero (no result envelope exists),
`SrunPrefectFuture.wait()` raises `SlurmJobFailed` with the exit code and any
captured stderr. If the exit code indicates a signal (128 + N convention, e.g.
137 for SIGKILL/OOM), the error message notes this explicitly so users can
distinguish infrastructure kills (OOM, wall-time, `scancel`) from worker bugs
(bad pickle, missing module).

### Cleanup and signal handling

`__exit__` terminates live srun subprocesses (SIGTERM → wait 10s → SIGKILL).
A SIGTERM handler registered in `__enter__` ensures cleanup runs even when
SLURM cancels the job or hits wall time, since `SystemExit` may not unwind
the context manager in all execution contexts.

---

## Supported Configuration

| Setting | Behavior |
|---------|----------|
| `gpus_per_node` / `slurm_gres` | Mapped to `--gres` (mutual exclusion: `slurm_gres` wins) |
| `mem_gb` | `--mem` |
| `cpus_per_task` | `--cpus-per-task` (promoted to named parameter in step 1) |
| `time_limit` | `--time` |
| `units_per_worker` | Supported — one srun per batch |
| `log_folder`, `poll_interval`, `max_poll_time`, `fail_on_error` | Supported |
| `srun_launch_concurrency` | New. Caps concurrent srun processes (default 128) |
| `partition`, `slurm_array_parallelism`, `max_array_size` | Ignored with warning in `__enter__` (same pattern as LOCAL mode) |
| Other `slurm_kwargs` | Rejected with warning in `__enter__` |

---

## What We Are NOT Doing

- **No new public TaskRunner.** `SRUN` is a mode on `SlurmTaskRunner`, not a
  separate class. One runner, three modes.
- **No persistent worker pool.** No daemons, no ZMQ, no RPC. One srun
  process per task (or batch). This trades throughput ceiling for simplicity.
  If sub-second tasks at scale become a need, a block-allocation mode is the
  natural next step.
- **No per-task resource specification.** All tasks in a flow get the same
  resources (runner-level `gpus_per_node`, etc.). Heterogeneous pipelines
  require separate runners. Per-task `resource_dict` is a clean future
  extension.
- **No arbitrary srun flag passthrough.** A small, explicit set of step-level
  options is supported. More can be added as needed.
- **No ZMQ/TCP result exchange.** Results go through NFS pickle files with
  atomic writes and cache invalidation. Simpler, proven, sufficient.
- **No sacct integration (yet).** Per-step metrics are available via
  `sacct -j <jobid>.<stepid>` but we don't query them automatically. A
  `future.resource_usage()` method is a natural v2 feature.
- **No Flux or MPI backends.** srun is universally available. Flux is the
  right evolution for extreme scale but isn't widely deployed yet.
- **No automatic file cleanup.** Step folders (pickles, logs) are preserved
  for debugging. Users delete `<log_folder>/srun/` when done.

---

## New Files

| File | Purpose |
|------|---------|
| `src/prefect_submitit/srun.py` | `SrunBackend`, command construction, concurrency control |
| `src/prefect_submitit/srun_worker.py` | Worker entry point — load pickle, execute, write result envelope |
| `src/prefect_submitit/futures/srun.py` | `SrunPrefectFuture` — wraps Popen, reads result pickle |

Plus updates to `constants.py` (enum), `runner.py` (`cpus_per_task` promotion, `_backend` attribute, backend routing),
`submission.py` (shared callable builder), `executors.py` (step-aware naming),
`futures/batched.py` (generalize parent future type), and exports.

---

## Implementation Order

1. **No-behavior-change refactors** — promote `cpus_per_task` to a named
   `__init__` parameter (default `1`, backwards-compatible — currently flows
   through `**slurm_kwargs`), create `build_task_callable` in `submission.py`
   (factoring out the inline callable construction currently in `runner.py`
   `submit()`), add step-aware naming helper to `executors.py`, add `_backend`
   attribute to runner alongside the existing `_executor` (both coexist:
   `_executor` remains for SLURM/LOCAL, `_backend` is the new dispatch
   interface used by all modes)
2. **Core srun plumbing** — `SrunBackend`, `srun_worker.py`, `SrunPrefectFuture`
3. **Enable `submit()`** in SRUN mode
4. **Enable non-batched `map()`** with concurrency control
5. **Enable batched `map()`** (`units_per_worker > 1`)
6. **Tests** — unit tests (`tests/test_srun.py`, `tests/test_srun_worker.py`,
   `tests/test_futures_srun.py`) plus additions to `tests/test_constants.py`
   (new enum value) and `tests/test_runner.py` (backend routing). Integration
   tests (`@pytest.mark.slurm`) inside a real SLURM allocation.
