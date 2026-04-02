# Review: `srun` Execution Mode for prefect-submitit

Reviewed design doc: [`_dev/design/srun.md`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md)

## Clarity

**Verdict:** WARN

**Findings**

- `SRUN` only specifies how `gpus_per_node`, `mem_gb`, and `cpus_per_task` map to `srun` flags in [`_dev/design/srun.md:250`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L250), [`_dev/design/srun.md:258`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L258), and [`_dev/design/srun.md:262`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L262).
- The current runner has more surface area than that, including special handling for `slurm_gres` in [`src/prefect_submitit/runner.py:147`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/runner.py#L147) and ignored-parameter warnings for `LOCAL` in [`src/prefect_submitit/runner.py:114`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/runner.py#L114).
- The doc does not clearly state which existing runner args are supported, ignored, translated, or rejected in `SRUN` mode.

**Suggestions**

- Add an explicit `SRUN` parameter support matrix for runner args and `slurm_kwargs`.
- Define fail-fast or warning behavior for unsupported settings so implementation does not invent policy ad hoc.

## Internal Consistency

**Verdict:** FAIL

**Findings**

- The worker/future failure protocol conflicts with itself.
- The worker writes `("error", traceback)` and exits non-zero in [`_dev/design/srun.md:315`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L315) and [`_dev/design/srun.md:318`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L318).
- `SrunPrefectFuture.wait()` raises immediately on any non-zero exit in [`_dev/design/srun.md:392`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L392), so `result()` never reaches the `("error", ...)` branch described in [`_dev/design/srun.md:428`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L428).
- The test plan still expects `test_result_error_tag` behavior in [`_dev/design/srun.md:621`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L621), which does not match the current control flow.

**Suggestions**

- Pick one failure protocol and document it consistently.
- Option 1: always inspect `result.pkl`, even after non-zero exit.
- Option 2: treat a serialized worker error as a normal worker exit and let the future convert it into `SlurmJobFailed`.

## Codebase Convention Compliance

**Verdict:** WARN

**Findings**

- The repo expects tests to mirror existing source and module structure per [`CLAUDE.md:66`](/Users/andrewhunt/git/prefect-submitit-dev/CLAUDE.md#L66).
- Existing enum and runner behavior already live in [`tests/test_constants.py:14`](/Users/andrewhunt/git/prefect-submitit-dev/tests/test_constants.py#L14) and [`tests/test_runner.py:520`](/Users/andrewhunt/git/prefect-submitit-dev/tests/test_runner.py#L520).
- The design instead places runner checks in `tests/test_srun.py or integration` in [`_dev/design/srun.md:635`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L635), which diverges from current test organization.

**Suggestions**

- Keep new unit files for `srun.py`, `srun_worker.py`, and `futures/srun.py`.
- Add `ExecutionMode` and `SlurmTaskRunner` coverage to the existing constants and runner test files rather than creating a new catch-all runner test module.

## Codebase Accuracy

**Verdict:** WARN

**Findings**

- Current task naming uses only array ids or `SLURM_JOB_ID` in [`src/prefect_submitit/executors.py:37`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/executors.py#L37) and [`src/prefect_submitit/executors.py:45`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/executors.py#L45), with tests in [`tests/test_executors.py:87`](/Users/andrewhunt/git/prefect-submitit-dev/tests/test_executors.py#L87) and [`tests/test_executors.py:104`](/Users/andrewhunt/git/prefect-submitit-dev/tests/test_executors.py#L104).
- In `SRUN` mode, all steps within one allocation share the same `SLURM_JOB_ID`, so task names will collapse to the same `slurm-<jobid>` unless the design changes naming.
- The doc only mentions `SLURM_STEP_ID` for folder naming in [`_dev/design/srun.md:589`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L589), not task-run naming or UI correlation.
- The file overview is incomplete relative to the actual package structure. The runner imports futures from [`src/prefect_submitit/runner.py:27`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/runner.py#L27), and exports are defined in [`src/prefect_submitit/futures/__init__.py:5`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/futures/__init__.py#L5) and [`src/prefect_submitit/__init__.py:5`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/__init__.py#L5), but the design only lists the new future module in [`_dev/design/srun.md:132`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L132).

**Suggestions**

- Define the `SRUN` task naming strategy explicitly, ideally using step-level identity if available.
- Expand the file-change list to include any required export/import updates.

## Solution Effectiveness

**Verdict:** FAIL

**Findings**

- The design leaves batching unresolved even though batching is already part of the runner’s public behavior.
- `SlurmTaskRunner.map()` has a dedicated `units_per_worker > 1` path in [`src/prefect_submitit/runner.py:366`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/runner.py#L366), backed by batched submission logic in [`src/prefect_submitit/submission.py:211`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/submission.py#L211).
- `SRUN` batching is still an open question in [`_dev/design/srun.md:584`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L584), which means the proposal is not implementation-ready for an existing feature path.
- The launch limiter only partially addresses the resource problem it identifies. The design caps live `Popen`s in [`_dev/design/srun.md:507`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L507) and [`_dev/design/srun.md:523`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L523), but it also keeps parent-side log handles on each future in [`_dev/design/srun.md:349`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L349), [`_dev/design/srun.md:357`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L357), [`_dev/design/srun.md:531`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L531), and [`_dev/design/srun.md:542`](/Users/andrewhunt/git/prefect-submitit-dev/_dev/design/srun.md#L542).
- Existing future log reading is path-based in [`src/prefect_submitit/futures/base.py:187`](/Users/andrewhunt/git/prefect-submitit-dev/src/prefect_submitit/futures/base.py#L187), so those handles do not need to stay open for the lifetime of the future.

**Suggestions**

- Either implement and test batched `SRUN` mode now, or explicitly reject `units_per_worker > 1` in `SRUN` mode with a clear error.
- Close parent log handles immediately after `Popen` and keep only the log paths.

## Overall Assessment

**Needs significant revision**

Items to address before implementation begins:

1. Resolve the worker/future failure protocol.
2. Decide and document `units_per_worker > 1` behavior.
3. Define supported vs ignored `SRUN` parameters, including `slurm_gres` and other `slurm_kwargs`.
4. Fix `SRUN` task naming and operator-facing correlation, ideally with step-level identity.
5. Align the test plan and file-change list with the current repo structure.

## Notes

- Review used project conventions from [`CLAUDE.md`](/Users/andrewhunt/git/prefect-submitit-dev/CLAUDE.md).
- No `CLAUDE.local.md` was present in the repo during review.
