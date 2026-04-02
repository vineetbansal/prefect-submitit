# Gap Analysis: srun Design vs Prior Art

**Date:** 2026-03-30
**Input:** [Landscape analysis](srun_landscape.md), deep dives into executorlib,
Parsl, and SLURM srun edge cases.
**Purpose:** Identify concrete gaps in the [srun design doc](srun.md) by
comparing against implementation details of executorlib, Parsl, mpi4py.futures,
and documented SLURM pitfalls.

---

## Bugs: Must Fix Before Implementation

### 1. Missing `--exact` flag

**Severity:** Correctness bug -- concurrent steps will not work as intended.

**What we have:**
```python
cmd = ["srun", "--exclusive", "-n1", "--kill-on-bad-exit=1", ...]
```

**The problem:** Within a SLURM allocation, `--exclusive` without `--exact`
causes each step to claim **all resources on its assigned node**, even if
it only requests `-n1 --gres=gpu:1`. On a 4-GPU node, the first srun step
blocks the other three GPUs entirely.

This is a well-documented SLURM behavior (post-20.11.3). The `--exact` flag
restricts a step to only the resources explicitly requested. Without it,
a `-n1 -c4 --gres=gpu:1` step still reserves the entire node.

**What executorlib and QCG-PilotJob do:** Both use the equivalent of
`--exact` to restrict steps to their requested resources, enabling
concurrent steps on the same node.

**Fix:**
```python
cmd = ["srun", "--exact", "-n1", ...]
```

Drop `--exclusive` entirely. `--exact` is what we actually want: "give me
exactly what I asked for, no more, no less."

**Sources:**
- [srun documentation](https://slurm.schedmd.com/srun.html)
- [NERSC running jobs guide](https://docs.nersc.gov/systems/perlmutter/running-jobs/)

---

### 2. Missing `--mpi=none`

**Severity:** Causes silent hangs on some clusters.

**The problem:** If a cluster sets `MpiDefault=pmix` (or `pmi2`) in
`slurm.conf`, srun attempts PMI initialization for every step, even
non-MPI Python tasks. This can cause steps to hang indefinitely waiting
for PMI handshake that will never come.

This is a known issue flagged in Parsl's SLURM documentation and in SLURM
mailing lists. The workaround is explicit:

**Fix:**
```python
cmd = ["srun", "--mpi=none", "--exact", "-n1", ...]
```

`--mpi=none` tells srun to skip PMI initialization entirely. No downside
for non-MPI tasks.

**Sources:**
- [SchedMD Bug 6727 -- MPI programs hang with srun](https://support.schedmd.com/show_bug.cgi?id=6727)
- [SLURM troubleshooting guide](https://slurm.schedmd.com/troubleshoot.html)

---

### 3. Non-atomic result pickle writes

**Severity:** Race condition -- corrupted pickle reads on NFS.

**What we have:** The worker writes directly to `result.pkl`:
```python
with open(result_path, "wb") as f:
    pickle.dump(("success", result), f)
```

**The problem:** On NFS, close-to-open consistency only guarantees
visibility *after* the writer calls `close()` and the reader opens
*after* that close returns. But NFS attribute caching means the
orchestrator may see a non-zero file size before the write is fully
committed to the server. Reading a partially-written pickle file produces
`UnpicklingError` or silently corrupted data.

submitit internally uses a write-to-temp-then-rename pattern for exactly
this reason. `os.rename()` is atomic on POSIX -- the file either exists
with full content or doesn't exist at all.

**Fix** (in `srun_worker.py`):
```python
tmp_path = result_path.with_suffix(".tmp")
with open(tmp_path, "wb") as f:
    pickle.dump(("success", result), f)
    f.flush()
    os.fsync(f.fileno())
os.rename(tmp_path, result_path)
```

The same pattern should apply to the error path.

**Sources:**
- [NFS close-to-open consistency semantics](https://docs.kernel.org/admin-guide/nfs/nfs-client.html)
- [submitit pickle path issues -- GitHub #1592](https://github.com/facebookincubator/submitit/issues/1592)

---

## Robustness: Should Fix Before Implementation

### 4. No SIGTERM handler for job cancellation

**What we have:** `__exit__` terminates child processes during normal
context-manager cleanup.

**The problem:** When the SLURM job is cancelled (`scancel`) or hits wall
time, the orchestrator receives SIGTERM. Python's default handler raises
`SystemExit`, but if the context manager isn't properly unwound (e.g.,
running inside a thread, or the exception is swallowed), `__exit__` never
runs. Srun children become orphans until SLURM sends SIGKILL after
`KillWait` seconds (default 30).

Parsl installs signal handlers for graceful shutdown. The srun edge cases
research confirms that SLURM sends SIGCONT then SIGTERM on `scancel`,
followed by SIGKILL after `KillWait`.

**Fix:** Register a SIGTERM handler in `__enter__` that triggers the same
cleanup logic as `__exit__`:

```python
def __enter__(self):
    ...
    if self.execution_mode == ExecutionMode.SRUN:
        self._prev_sigterm = signal.signal(
            signal.SIGTERM, self._handle_sigterm
        )

def _handle_sigterm(self, signum, frame):
    """Graceful shutdown on SLURM job cancellation or wall-time."""
    self.__exit__(None, None, None)
    # Restore previous handler and re-raise
    signal.signal(signal.SIGTERM, self._prev_sigterm)
    os.kill(os.getpid(), signal.SIGTERM)

def __exit__(self, *args):
    if self.execution_mode == ExecutionMode.SRUN:
        # Restore signal handler
        if hasattr(self, "_prev_sigterm"):
            signal.signal(signal.SIGTERM, self._prev_sigterm)
        ...  # existing cleanup
```

**Caveat:** Python signal handlers only run in the main thread. If the
runner is used from a worker thread, this won't fire. Document this
limitation.

**Sources:**
- [Signal propagation on SLURM -- Dhruvesh Patel](https://dhruveshp.com/blog/2021/signal-propagation-on-slurm/)
- [Handling Python job cancellation in SLURM](https://dmitrykabanov.com/blog/2020/04-job-cancelation-in-slurm/)

---

### 5. No inter-launch delay in map loop

**The problem:** Rapid-fire srun launches can overwhelm the SLURM
controller, producing `"srun: Job step creation temporarily disabled,
retrying"`. Each step launch is an RPC to `slurmctld`. The SLURM High
Throughput Computing guide recommends keeping query rates reasonable.

Our `_wait_for_slot` already sleeps when at concurrency cap, but fires
srun launches as fast as possible when below the cap. With 100 tasks and
128 slots, all 100 launch in a burst.

**Fix:** Add a minimum inter-launch interval:

```python
MIN_LAUNCH_INTERVAL = 0.05  # 50ms between srun launches

last_launch = 0.0
for i, wrapped_call in enumerate(wrapped_calls):
    _wait_for_slot(active)
    now = time.monotonic()
    if now - last_launch < MIN_LAUNCH_INTERVAL:
        time.sleep(MIN_LAUNCH_INTERVAL - (now - last_launch))
    ...  # launch srun
    last_launch = time.monotonic()
```

50ms per launch = 20 launches/sec, which is well within SLURM's
throughput ceiling (~500 steps/sec sustained) while avoiding bursts.

**Sources:**
- [SLURM High Throughput Computing Guide](https://slurm.schedmd.com/high_throughput.html)
- [NERSC best practices](https://docs.nersc.gov/jobs/best-practices/)

---

### 6. Remove `--kill-on-bad-exit=1`

**What we have:**
```python
cmd = ["srun", "--exclusive", "-n1", "--kill-on-bad-exit=1", ...]
```

**The problem:** `--kill-on-bad-exit` only matters for multi-task steps
(`-n > 1`). With `-n1`, there is exactly one task -- if it fails, the step
ends regardless. The flag is a no-op that signals to readers we expect
multi-task steps when we don't.

**Fix:** Remove it from `build_srun_command`. If we later add multi-task
steps (e.g., for distributed training), add it back.

---

## Resolved Open Question

### 7. SLURM_STEP_ID for task naming -- confirmed viable

**Our open question:** "Each srun invocation gets a unique SLURM_STEP_ID.
Needs investigation into whether the step ID is available before srun
blocks."

**Answer:** `SLURM_STEP_ID` is set as an environment variable **inside the
srun step** (in the worker process), not on the orchestrator side. It
auto-increments: step 0, 1, 2, ... for each job. The worker can read it
and use it for the Prefect task_run_name:

```python
# In srun_worker.py or run_task_in_slurm
step_id = os.environ.get("SLURM_STEP_ID")
job_id = os.environ.get("SLURM_JOB_ID")
task_run_name = f"srun-{job_id}.{step_id}" if step_id else None
```

This enables direct `sacct -j 12345.7` correlation from the Prefect UI.

Steps also appear in `sacct` as `<jobid>.<stepid>` with per-step MaxRSS,
CPU time, and exit code.

**Sources:**
- [NASA NCCS -- srun environment variables](https://www.nccs.nasa.gov/nccs-users/instructional/using-slurm/submit-jobs/srun-env)
- [sacct documentation](https://slurm.schedmd.com/sacct.html)

---

## Future Extensions (Not Blockers)

### 8. Per-task resource specification

**From:** executorlib

executorlib's most distinctive feature: each `submit()` call accepts a
`resource_dict` specifying cores, GPUs, and memory for that specific task:

```python
with SlurmJobExecutor() as exe:
    f1 = exe.submit(preprocess, data, resource_dict={"cores": 4})
    f2 = exe.submit(train, model, resource_dict={"cores": 16, "gpus_per_core": 1})
```

Our design uses runner-level `gpus_per_node`, `mem_gb`, etc. -- all tasks
get the same resources. For heterogeneous pipelines (CPU preprocessing ->
GPU training -> CPU postprocessing), users must create separate runners.

**How to add later:** Accept an optional `resource_dict` on `submit()` and
`map()` that overrides runner-level defaults in `build_srun_command`. The
srun flags already support per-step resource specification.

---

### 9. Block allocation / persistent worker

**From:** executorlib (`block_allocation=True`)

Instead of spawning a new srun process per task, keep a persistent Python
process alive on each node and dispatch functions to it via ZMQ. This
amortizes srun spawn overhead (~100ms) across many tasks.

**When it matters:** For 1000 sub-second tasks, spawn overhead is 100s
total. For 100 minute-long tasks, it is 10s (negligible).

**Our target workload (10-1000 tasks, minute+ runtimes):** Spawn overhead
is negligible. Block allocation would be the right evolution for high-
throughput workloads with many short tasks.

---

### 10. ZMQ-based result exchange

**From:** executorlib

executorlib avoids NFS entirely: the parent opens a ZMQ socket, passes the
address to the srun child via command-line arg, and exchanges
cloudpickle-serialized objects over TCP.

**Advantages:** No NFS caching issues, no fsync, no attribute
invalidation, lower latency. **Disadvantage:** adds `pyzmq` dependency,
more complex process coordination (what if the socket dies?).

Our NFS approach with atomic writes (gap #3) and cache invalidation
(already addressed in the design) is simpler and sufficient. If NFS
proves to be a pain point in production, ZMQ is the proven alternative.

---

### 11. Retry with infrastructure-aware cost function

**From:** Parsl

Parsl distinguishes infrastructure failures from application errors:
```python
def retry_handler(exception, task_record):
    if isinstance(exception, (WorkerLost, ManagerLost)):
        return 0.1   # cheap -- retry aggressively
    return 1.0        # expensive -- count against retry budget
```

Prefect has its own retry mechanism (`@task(retries=3)`), which may
suffice. However, distinguishing "srun process OOM-killed" (retry-worthy)
from "user code raised ValueError" (not retry-worthy) and surfacing that
in the exception type would help users configure Prefect retries.

**How to add later:** `SrunPrefectFuture` could inspect the srun exit code
and stderr to classify failures:
- Exit 137 (SIGKILL) -> `SlurmOOMKilled`
- Exit 143 (SIGTERM) -> `SlurmJobCancelled`
- result.pkl contains `("error", ...)` -> `SlurmTaskFailed` (user code)

---

### 12. sacct integration for resource reporting

Since we use srun-native steps, `sacct` gives us per-task resource
metrics for free. No other Prefect integration can provide this.

```bash
sacct -j 12345.7 --format=Elapsed,MaxRSS,MaxVMSize,AveCPU,ExitCode
```

A `future.resource_usage()` method that queries sacct after completion
would be a differentiating feature. This enables users to right-size
their resource requests based on actual usage.

---

### 13. MaxStepCount awareness

SLURM's `MaxStepCount` (default 40,000) limits total steps per job.
Long-running orchestrators recycling many short tasks could hit this.
Worth documenting as a known limit and potentially checking
`scontrol show config | grep MaxStepCount` in `__enter__` to warn users.

---

## Summary

| # | Gap | Severity | Action |
|---|-----|----------|--------|
| 1 | `--exact` instead of `--exclusive` | **Bug** | Fix `build_srun_command` |
| 2 | `--mpi=none` for non-MPI tasks | **Bug** | Add to `build_srun_command` |
| 3 | Atomic result writes (temp+rename) | **Bug** | Fix `srun_worker.py` |
| 4 | SIGTERM handler | Should fix | Add in `__enter__`/`__exit__` |
| 5 | Inter-launch delay in map | Should fix | Add to map loop |
| 6 | Remove `--kill-on-bad-exit=1` | Cleanup | Remove from command |
| 7 | SLURM_STEP_ID for task naming | Resolves open Q | Confirmed viable |
| 8 | Per-task resource_dict | Future | Extension point |
| 9 | Block allocation mode | Future | For high-throughput |
| 10 | ZMQ result exchange | Future | If NFS is insufficient |
| 11 | Infrastructure-aware retries | Future | Classify exit codes |
| 12 | sacct resource reporting | Future | Differentiating feature |
| 13 | MaxStepCount awareness | Future | Document / warn |
