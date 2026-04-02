# Landscape Analysis: Intra-Allocation Task Dispatch on SLURM

**Date:** 2026-03-30
**Purpose:** Survey existing packages that distribute Python tasks across SLURM
nodes within an allocation, to inform the `srun` execution mode design for
prefect-submitit.

---

## The Gap We're Filling

No existing package provides all three of:

1. **`concurrent.futures`-style API** (submit callable, get future, collect result)
2. **`srun`-based intra-allocation dispatch** (run within existing `salloc`/`sbatch`)
3. **Prefect integration** (TaskRunner protocol, state management, UI visibility)

The closest packages each cover one or two of these:

| Package | Futures API | Intra-alloc | Prefect |
|---------|:-----------:|:-----------:|:-------:|
| submitit | yes | no | via prefect-submitit |
| executorlib | yes | yes | no |
| Parsl | yes | yes | no |
| mpi4py.futures | yes | yes | no |
| Dask + jobqueue | yes | partial | via prefect-dask |
| RADICAL-Pilot | no | yes | no |
| HyperQueue | no | yes | no |

---

## Packages With Intra-Allocation Dispatch

### executorlib (pyiron) -- most directly comparable

The closest existing design to what we're building. Extends
`concurrent.futures.Executor` for HPC with explicit srun support.

- **`SlurmJobExecutor`**: distributes Python functions within an existing
  allocation via `srun`. Each `submit()` carves out a resource subset
  (CPUs, GPUs) from the allocation.
- **`SlurmClusterExecutor`**: submits new `sbatch` jobs (like submitit).
- **Serialization**: pickle-based, file exchange on shared storage.
- **Resource control**: per-function resource dicts
  (`{"cores": 4, "gpus_per_core": 1}`).
- **Maturity**: published in JOSS. Active development.
- **GitHub**: [pyiron/executorlib](https://github.com/pyiron/executorlib)
- **Paper**: [JOSS 2024](https://joss.theoj.org/papers/10.21105/joss.07782.pdf)

**Lessons for us:**
- The dual-executor pattern (`ClusterExecutor` for sbatch,
  `JobExecutor` for srun) validates our `ExecutionMode.SLURM` vs
  `ExecutionMode.SRUN` split.
- Per-function resource specification is a natural extension we could add
  later.
- They also found that srun overhead per task is non-trivial at scale --
  Flux backend achieves lower overhead for very high task counts.

### Parsl (HighThroughputExecutor + SrunLauncher)

A full parallel scripting library from Argonne National Lab. The only
mature package with both a futures API and srun-based execution.

- **Architecture**: three-tier. User script -> Interchange (ZeroMQ load
  balancer) -> worker pools on compute nodes.
- **SLURM integration**: `SlurmProvider` acquires allocations via `sbatch`.
  `SrunLauncher` uses `srun` within those allocations to start worker
  pools. Tasks flow through ZeroMQ, not through srun per-task.
- **API**: `@python_app` / `@bash_app` decorators returning `AppFuture`
  (subclass of `concurrent.futures.Future`).
- **Serialization**: cloudpickle/pickle over ZeroMQ.
- **Maturity**: ~563 stars, 100+ contributors, weekly releases. Backed by
  DOE/NSF.
- **GitHub**: [Parsl/parsl](https://github.com/Parsl/parsl)
- **Docs**: [parsl.readthedocs.io](https://parsl.readthedocs.io/)

**Lessons for us:**
- Parsl uses srun only to *launch worker pools*, not per-task. Tasks are
  dispatched over ZeroMQ within those pools. This avoids srun-per-task
  overhead but requires running a persistent Interchange process.
- Our approach (srun per task) is simpler but higher overhead at scale.
  For the 10-1000 task range typical of ML pipelines, the overhead is
  acceptable.
- Parsl's configuration complexity (Provider + Executor + Launcher +
  Channel) is a cautionary tale -- keep our API surface minimal.

### mpi4py.futures (MPIPoolExecutor)

A `concurrent.futures.Executor` backed by MPI. The most lightweight
intra-allocation dispatch option.

- **Architecture**: 1 manager rank + N worker ranks. Manager distributes
  callables via MPI send/recv.
- **Launch**: `srun python -m mpi4py.futures script.py` -- srun starts
  the MPI processes, then MPI handles all task dispatch internally.
- **API**: `MPIPoolExecutor` -- direct `concurrent.futures.Executor` subclass.
- **Serialization**: standard pickle over MPI.
- **Maturity**: very mature, part of the established mpi4py ecosystem.
- **Docs**: [mpi4py.readthedocs.io](https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html)

**Lessons for us:**
- MPI gives the lowest per-task overhead (no subprocess spawn per task),
  but requires MPI installation and a persistent process model.
- Our srun-per-task approach trades dispatch overhead for simplicity: no
  persistent worker processes, no MPI dependency, no ZeroMQ.
- mpi4py.futures can only serialize pickle-able objects (no cloudpickle).
  Our cloudpickle approach is more flexible for closures and lambdas.

### RADICAL-Pilot

A pilot-job system for large-scale HPC. Acquires allocations then
schedules tasks internally via its own agent.

- **Architecture**: PilotManager submits batch job -> Agent starts on
  allocated nodes -> Agent's Scheduler assigns tasks to cores/GPUs ->
  Executor launches via srun/mpirun/prrte/etc.
- **16+ launch methods**: srun, OpenMPI, MPICH, PRRTE, JSRUN, Flux,
  Dragon, SSH, and more.
- **API**: Custom (Session, PilotManager, TaskManager, TaskDescription).
  NOT futures-based.
- **Requires MongoDB** for coordination between client and agent.
- **Maturity**: deployed at OLCF, NERSC. Active development.
- **GitHub**: [radical-cybertools/radical.pilot](https://github.com/radical-cybertools/radical.pilot)

**Lessons for us:**
- RP's pluggable launcher architecture shows the space of dispatch
  mechanisms. We're choosing the simplest (srun) which covers the common
  case.
- MongoDB dependency and complex API are exactly what we want to avoid.
- RP's approach of running an agent process on node-0 that manages task
  scheduling is an interesting alternative to our srun-per-task model,
  but far heavier.

### HyperQueue

A Rust-based meta-scheduler: "SLURM within SLURM." Runs a
server+worker architecture within SLURM allocations and distributes
tasks without additional srun/sbatch calls.

- **Architecture**: HQ server (on login/head node) + HQ workers (on
  compute nodes). Tasks submitted to server, distributed to workers.
- **API**: primarily CLI. Python API is experimental and limited to task
  graph construction -- tasks are shell commands, not Python functions.
- **No function serialization**: tasks are command-line programs.
- **Maturity**: ~400-500 stars. Developed by IT4Innovations (Czech
  national supercomputing center).
- **GitHub**: [It4innovations/hyperqueue](https://github.com/It4innovations/hyperqueue)
- **Comparison page**: [hyperqueue/other-tools](https://it4innovations.github.io/hyperqueue/stable/other-tools/)

**Lessons for us:**
- HyperQueue's [comparison page](https://it4innovations.github.io/hyperqueue/stable/other-tools/)
  is excellent prior art for understanding the tradeoffs in this space.
- Their approach avoids srun entirely (user-space dispatch via TCP),
  trading SLURM accounting for lower overhead. We deliberately choose
  srun for SLURM-native accounting.

### Flux Framework (in SLURM)

A next-generation hierarchical resource manager from LLNL. Can run
inside a SLURM allocation as a sub-scheduler.

- **Launch**: `srun flux start ...` within a SLURM allocation creates a
  full Flux scheduler instance across the allocated nodes.
- **Python bindings**: `FluxClusterExecutor` (submits Python functions
  as Flux jobs) and `FluxJobExecutor` (distributes within a Flux job).
- **Key advantage**: hierarchical scheduling. Each node can have its own
  child scheduler, achieving much higher task throughput than SLURM's
  native step management.
- **Maturity**: backed by LLNL. Growing HPC adoption. Not yet
  ubiquitous.
- **GitHub**: [flux-framework/flux-core](https://github.com/flux-framework/flux-core)

**Lessons for us:**
- Flux is the highest-throughput option for intra-allocation dispatch,
  but requires Flux to be installed. srun is universally available.
- executorlib supports both srun and Flux backends -- the Flux backend
  achieves lower per-task overhead. If we ever need higher throughput, a
  Flux execution mode is a natural extension.

### QCG-PilotJob

A pilot-job manager for SLURM that uses srun internally for task
dispatch within allocations.

- **Architecture**: starts inside a SLURM allocation, manages its own
  task queue. Internal scheduler determines execution order and resource
  assignment.
- **Dispatch**: uses `srun` to launch tasks with resource subsets.
- **Maturity**: research-grade. Part of the VECMA project (EU-funded).
- **GitHub**: [vecma-project/QCG-PilotJob](https://github.com/vecma-project/QCG-PilotJob)

**Lessons for us:**
- Validates the srun-per-task approach for pilot-style execution.
- Requires whole-node allocations for affinity binding to work correctly
  -- a constraint worth noting in our docs.

---

## Packages Without Intra-Allocation Dispatch

### submitit (Meta/Facebook)

The foundation our project builds on. `concurrent.futures`-compatible
SLURM executor.

- **Dispatch**: every `submit()` generates a new `sbatch` call. No srun
  standalone support.
- **srun usage**: srun wraps the command *inside* the generated sbatch
  script (controllable via `use_srun` parameter). This is for signal
  handling and output routing, not intra-allocation dispatch.
- **`LocalExecutor` limits**: hardcoded `nodes=1`
  (`ValueError` if `nodes > 1`), no concurrency control, minimal
  parameter support.
- **File path conflict**: `_submit` module constructs paths using
  `SLURM_JOB_ID` from env. Within `salloc`, all srun steps share the
  same ID -- collisions. This is why our design uses a custom worker.
- **GitHub**: [facebookincubator/submitit](https://github.com/facebookincubator/submitit)
  (~1.6k stars)

**Relevant issues:**
- [#1597](https://github.com/facebookincubator/submitit/issues/1597) --
  `LocalExecutor` single-node error
- [#1685](https://github.com/facebookincubator/submitit/issues/1685) --
  `tasks_per_node=1` doesn't limit spawned processes
- [#1718](https://github.com/facebookincubator/submitit/issues/1718) --
  Feature request for `array_parallelism` on LocalExecutor
- No issues, PRs, or forks adding srun-standalone execution.

### clusterfutures

The original `concurrent.futures` for SLURM. Minimal serialize-launch-
collect implementation.

- **API**: `SlurmExecutor` -- direct `concurrent.futures.Executor`
  subclass with `submit()` and `map()`.
- **Serialization**: cloudpickle to files on shared filesystem.
- **Each submit = one sbatch job**. No intra-allocation support.
- **GitHub**: [sampsyo/clusterfutures](https://github.com/sampsyo/clusterfutures)

### dask-jobqueue

Deploys Dask clusters on SLURM. Two patterns:

- **Dynamic Cluster** (`SLURMCluster`): each Dask worker = one sbatch
  job. Workers autoscale. sbatch overhead per worker.
- **Batch Runner** (`SLURMRunner`): entire Dask cluster in a single
  allocation. All processes coordinate during startup. True intra-
  allocation, but only for Dask workers -- not for individual tasks.
- **GitHub**: [dask/dask-jobqueue](https://github.com/dask/dask-jobqueue)
  (~217 stars)

### Ray on SLURM

Deploys a full Ray cluster within a SLURM allocation.

- **Architecture**: head-worker model. sbatch script uses multiple srun
  commands to start Ray head + workers on different nodes.
- **Intra-allocation**: yes -- Ray operates entirely within the
  allocation. Once running, Ray's internal scheduler handles dispatch.
- **Heavy**: requires running a full Ray runtime. Port conflicts in
  multi-tenant environments.
- **Docs**: [ray.io/slurm](https://docs.ray.io/en/latest/cluster/vms/user-guides/community/slurm.html)

---

## Workflow Orchestrators and SLURM

### Snakemake -- the dual-plugin model

Snakemake has the cleanest architectural separation:

- **`snakemake-executor-plugin-slurm`**: submits rules as sbatch jobs.
- **`snakemake-executor-plugin-slurm-jobstep`**: runs rules as srun
  steps within an existing allocation.

Two separate plugins, same Snakemake DAG engine. Users choose based on
whether they want many small queue jobs or one big allocation.

- **GitHub**: [snakemake-executor-plugin-slurm-jobstep](https://github.com/snakemake/snakemake-executor-plugin-slurm-jobstep)

**Lessons for us:** Validates the `ExecutionMode` enum approach --
same runner, different dispatch modes. Snakemake's jobstep plugin
specifically uses `srun -n1 --cpu-bind=q`.

### Nextflow

Has a native `slurm` executor (sbatch per process). For intra-
allocation, the recommended pattern is: submit the Nextflow manager as a
batch job, use `local` executor, call `srun` from process definitions.
Two-mode approach matches ours.

### Airflow

Community operators support srun dispatch via SSH to SLURM login nodes.
Not production-grade for this use case.

### Prefect -- current state

No official SLURM TaskRunner. Community options:

- **prefect-submitit** (this project): sbatch via submitit. Adding srun.
- **prefect-dask + SLURMCluster**: Dask workers as sbatch jobs.
- **prefect-slurm-worker**: submits flow runs (not tasks) as SLURM jobs.
- **SNIFS-science/prefect-hpc-worker**: research prototype.

None support intra-allocation execution.

---

## Architectural Patterns

The research reveals four distinct approaches to intra-allocation
dispatch:

### 1. srun-per-task (our approach)

Each Python callable gets its own `srun` subprocess. Simple,
SLURM-native, no persistent infrastructure.

**Used by:** executorlib (SlurmJobExecutor), QCG-PilotJob, our design

```
Orchestrator ──srun──► Worker 1 (unpickle, run, pickle result)
             ──srun──► Worker 2
             ──srun──► Worker 3
```

| Pros | Cons |
|------|------|
| Simplest implementation | srun overhead per task (~100ms) |
| Full SLURM accounting per step | File descriptor pressure at scale |
| GPU binding via --gres | Throughput ceiling ~500 steps/sec |
| No persistent infrastructure | NFS latency for pickle exchange |
| Familiar subprocess model | |

### 2. Persistent worker pool (Parsl, Dask, Ray)

Start long-running worker processes on allocated nodes. Dispatch tasks
via TCP/ZeroMQ/internal protocol.

**Used by:** Parsl HTEX, Dask, Ray, HyperQueue

```
Orchestrator ──ZeroMQ──► Worker Pool (node 0) ──► task, task, task
             ──ZeroMQ──► Worker Pool (node 1) ──► task, task, task
```

| Pros | Cons |
|------|------|
| Lowest per-task overhead | Persistent processes consume resources |
| Highest throughput | Additional infrastructure (scheduler, interchange) |
| Efficient for many small tasks | Bypass SLURM step accounting |
| In-memory data passing possible | Port management / multi-tenancy |

### 3. MPI-based dispatch (mpi4py.futures)

Use MPI send/recv to distribute callables across ranks.

**Used by:** mpi4py.futures, some RP configurations

```
srun ──► Rank 0 (manager) ──MPI──► Rank 1 (worker)
                           ──MPI──► Rank 2 (worker)
                           ──MPI──► Rank 3 (worker)
```

| Pros | Cons |
|------|------|
| Very low per-task overhead | Requires MPI installation |
| No shared filesystem needed | All ranks must start together |
| Standard HPC pattern | Only pickle-able objects (no cloudpickle) |
| | Fixed worker count for lifetime |

### 4. Hierarchical sub-scheduler (Flux)

Run a full scheduler within the allocation that manages its own job
queue.

**Used by:** Flux, Dragon

```
SLURM alloc ──► Flux instance ──► Flux job 1
                               ──► Flux job 2
                               ──► Flux job N
```

| Pros | Cons |
|------|------|
| Lowest overhead at extreme scale | Requires Flux installation |
| Full scheduling policies | Not universally available |
| Hierarchical resource management | Additional complexity |
| Best for 10k+ tasks | |

### Why srun-per-task is right for us

Our target workload is **10-1000 independent tasks** within a
multi-node ML allocation. At this scale:

- srun overhead (~100ms/task) is negligible vs task runtime (minutes+)
- SLURM-native accounting is valuable (sacct, job steps visible)
- No persistent infrastructure to manage or debug
- No new dependencies (srun is always available)
- Subprocess model is easy to reason about

The persistent-worker-pool approach (Parsl, Dask, Ray) would be
overkill and would require users to manage additional infrastructure.
The MPI approach requires an MPI installation and doesn't support
cloudpickle. Flux isn't widely deployed yet.

---

## Design Inspiration Summary

| Source | What to adopt |
|--------|--------------|
| **executorlib** | Dual-executor pattern validates our `ExecutionMode` split. Per-function resource specification is a future extension. |
| **Snakemake** | Dual-plugin (sbatch vs srun-jobstep) validates same-engine, different-dispatch. |
| **submitit** | Serialize-launch-collect via cloudpickle on shared storage. Our custom worker follows this proven pattern. |
| **Parsl** | srun for worker launch, ZeroMQ for task dispatch. We simplify by using srun for both. SrunLauncher flags are reference material. |
| **mpi4py.futures** | Clean `concurrent.futures.Executor` for HPC. Aspirational API simplicity. |
| **HyperQueue** | [Comparison page](https://it4innovations.github.io/hyperqueue/stable/other-tools/) is excellent analysis of the tradeoff space. Active-process tracking pattern for concurrency control. |
| **dask-jobqueue** | Dual-mode (Dynamic Cluster vs Batch Runner) validates offering both queue-submission and intra-allocation modes. |
| **RADICAL-Pilot** | Pluggable launcher architecture shows the full design space. Confirms srun is the right default. |

---

## References

### Packages (GitHub)
- [facebookincubator/submitit](https://github.com/facebookincubator/submitit)
- [pyiron/executorlib](https://github.com/pyiron/executorlib)
- [Parsl/parsl](https://github.com/Parsl/parsl)
- [mpi4py/mpi4py](https://github.com/mpi4py/mpi4py)
- [dask/dask-jobqueue](https://github.com/dask/dask-jobqueue)
- [radical-cybertools/radical.pilot](https://github.com/radical-cybertools/radical.pilot)
- [It4innovations/hyperqueue](https://github.com/It4innovations/hyperqueue)
- [flux-framework/flux-core](https://github.com/flux-framework/flux-core)
- [vecma-project/QCG-PilotJob](https://github.com/vecma-project/QCG-PilotJob)
- [sampsyo/clusterfutures](https://github.com/sampsyo/clusterfutures)
- [CrayLabs/SmartSim](https://github.com/CrayLabs/SmartSim)
- [PySlurm/pyslurm](https://github.com/PySlurm/pyslurm)
- [amq92/simple_slurm](https://github.com/amq92/simple_slurm)
- [snakemake/snakemake-executor-plugin-slurm-jobstep](https://github.com/snakemake/snakemake-executor-plugin-slurm-jobstep)
- [ExaWorks/psij-python](https://github.com/ExaWorks/psij-python)

### Prefect SLURM Integrations
- [PrefectHQ/prefect-dask](https://github.com/PrefectHQ/prefect-dask)
- [PrefectHQ/prefect-ray](https://github.com/PrefectHQ/prefect-ray)
- [SNIFS-science/prefect-hpc-worker](https://github.com/SNIFS-science/prefect-hpc-worker)
- [prefect-slurm-worker (PyPI)](https://pypi.org/project/prefect-slurm-worker/)
- [Prefect issue #10136: SLURM tutorial request](https://github.com/PrefectHQ/prefect/issues/10136)

### Documentation
- [SLURM srun manual](https://slurm.schedmd.com/srun.html)
- [SLURM Job Launch Design](https://slurm.schedmd.com/job_launch.html)
- [SLURM High Throughput Guide](https://slurm.schedmd.com/high_throughput.html)
- [SLURM GRES Scheduling](https://slurm.schedmd.com/gres.html)
- [executorlib JOSS paper](https://joss.theoj.org/papers/10.21105/joss.07782.pdf)
- [Parsl SrunLauncher](https://parsl.readthedocs.io/en/stable/stubs/parsl.launchers.SrunLauncher.html)
- [HyperQueue tool comparison](https://it4innovations.github.io/hyperqueue/stable/other-tools/)
- [Flux in SLURM (SLUG23)](https://slurm.schedmd.com/SLUG23/flux_in_slurm.pdf)
- [Ray on SLURM](https://docs.ray.io/en/latest/cluster/vms/user-guides/community/slurm.html)
- [Nextflow executor docs](https://www.nextflow.io/docs/latest/executor.html)
- [Snakemake SLURM plugin catalog](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html)

### Papers
- [Executorlib -- Up-scaling Python workflows (JOSS 2024)](https://joss.theoj.org/papers/10.21105/joss.07782.pdf)
- [Flux: Overcoming Scheduling Challenges for Exascale Workflows (FGCS 2020)](https://flux-framework.org/publications/Flux-FGCS-2020.pdf)
- [Parsl: Pervasive Parallel Programming in Python (2019)](https://web.cels.anl.gov/~woz/papers/Parsl_2019.pdf)
- [Using Pilot Systems to Execute Many Task Workloads (2018)](https://ar5iv.labs.arxiv.org/html/1512.08194)
- [Integrating HPC Task Runtime Systems for Hybrid AI-HPC (2025)](https://arxiv.org/html/2509.20819v1)
