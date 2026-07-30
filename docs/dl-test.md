# DL Test

The DL test validates that a node can run representative deep learning layers,
functional kernels, and collective operations with numerically consistent output
and reasonable CPU/GPU timing behavior.

## c-val Role

c-val runs the DL test as the third validation phase, after storage and NCCL:

```bash
bash /workspace/c-val/validation-tests/dltest/setup.sh
bash /workspace/c-val/validation-tests/dltest/run-test.sh 8
```

`dltest.sh` remains a thin compatibility wrapper for pinned jobs and older
operator commands.

The wrapper launches the installed DL unit-test harness with `torchrun` across
8 local GPU ranks and writes raw output to `DLTEST_LOG_FILE`. A zero exit code
marks the DL phase as `pass`; any non-zero exit marks it `fail` and is reflected
in the structured `cval.results.v2` JSON artifact.

The enable switch, GPU count, test plan, and iteration count come from
the composed registry: `enabled` and `config_path` are under `[tests.dltest]`
in `config/cval.toml`, while GPU count, test plan, and iterations live in
`validation-tests/dltest/test_config.toml`. During the compatibility stage they
are injected as `RUN_DLTEST`, `CVAL_DL_GPU_COUNT`, `CVAL_DL_TEST_PLAN`, and
`CVAL_DL_ITERATIONS`.

## What It Exercises

- `nn_tasks`: modules from `torch.nn`, such as large linear layers.
- `f_tasks`: functional kernels, such as scaled dot-product attention.
- `coll_tasks`: distributed collectives, such as reduce-scatter or all-reduce.
- `overlap_tasks`: generated communication/compute overlap cases.

## Output Signals

Look for these fields or table columns in the DL log:

- `status`: task completion state.
- `norm_output`: final output norm used for numerical consistency checks.
- `weight` and `bias`: gradient norms for layer tasks.
- `fp_cpu_time` and `fp_gpu_time`: forward-pass timings.
- `bp_cpu_time` and `bp_gpu_time`: backward-pass timings.
- `cpu_time` and `gpu_time`: collective timing fields.
- `coll_mean`, `coll_stdev`, `layer_mean`, `layer_stdev`: overlap statistics.

## Pass/Fail Interpretation

A healthy DL phase should show:

- final `DL Test completed successfully` line
- task rows with completed statuses
- stable finite `norm_output` values
- no `Traceback`, `Exception`, `FAILED`, `NaN`, `Inf`, or mismatch markers

If the DL phase fails, keep the classification specific: DL correctness,
PyTorch/runtime bootstrap, GPU execution, or distributed collective behavior.
Do not fold it into generic node failure without checking the storage and NCCL
phase results in the same structured JSON file.