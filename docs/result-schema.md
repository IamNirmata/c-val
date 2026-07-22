# Result Schema

c-val 2.0 writes one structured result JSON per node validation run.

Path:

```text
/data/continuous_validation/results/<node>/cval-results-<node>-<timestamp>.json
```

Schema version:

```text
cval.results.v1
```

## Example

```json
{
  "schema_version": "cval.results.v1",
  "node": "slc01-cl02-hgx-0204",
  "image_name": "pytorch:26.05-py3",
  "pytorch_version": "2.8.0a0+gitabc123",
  "cuda_version": "12.9",
  "timestamp": "1781134840",
  "generated_at": "2026-06-10T23:47:42.103216Z",
  "overall": "pass",
  "tests": {
    "storage": {
      "status": "pass",
      "log": "/data/continuous_validation/storage/.../storage.log",
      "summary": "/data/continuous_validation/storage/.../storage-summary.txt"
    },
    "nccl": {
      "status": "pass",
      "log": "/data/continuous_validation/nccl/.../nccl.log",
      "summary": "/data/continuous_validation/nccl/.../nccl-summary.json"
    },
    "dltest": {
      "status": "pass",
      "log": "/data/continuous_validation/dltest/.../dltest.log",
      "summary": "/data/continuous_validation/dltest/.../dltest-summary.json"
    }
  }
}
```

## Rules

- Valid statuses: `pass`, `fail`, `incomplete`.
- `image_name` records the validation image identity used for the run.
- `pytorch_version` and `cuda_version` are detected from the running image in
  `0-env.sh` (`torch.__version__` and `torch.version.cuda`) and are best-effort:
  they are empty strings when torch is unavailable.
- `overall` is `pass` only when all test statuses are `pass`.
- `db-update.sh` prefers JSON and falls back to the legacy env file if JSON is missing.
- DB writes use package-native `cval db-add-*` commands inside the validation pod and store `image_name`, `pytorch_version`, and `cuda_version` with the `runs` validation rows.
- `cval.validation.results` validates schema version, required tests, valid statuses, and aggregate consistency.

## DB Rows

Each run should write four latest-status rows in `validation.db` `runs`, each
carrying `image_name`, `pytorch_version`, and `cuda_version`:

```text
<node> storage <timestamp> <status>  <image_name> <pytorch_version> <cuda_version>
<node> nccl    <timestamp> <status>  <image_name> <pytorch_version> <cuda_version>
<node> dltest  <timestamp> <status>  <image_name> <pytorch_version> <cuda_version>
<node> all     <timestamp> <overall> <image_name> <pytorch_version> <cuda_version>
```

## Baseline Classification

These latest-status rows and the storage/NCCL/DL metric DBs are the inputs to
dynamic baseline building and node classification. See
[Baselines and Node Classification](baselines.md).

## DL metric DB iterations

Every metric table in the four DL metadata DBs contains an `iterations`
column:

```text
dltest_numerical_correctness.db → numerical_correctness
dltest_compute_performance.db → compute_performance
dltest_collective_performance.db → collective_performance
dltest_overlap_performance.db → overlap_performance
```

The value is repeated on every metric row for its run so results produced with
different workload iteration counts can be filtered and stratified. Ingestion
reads the value from `dltest-summary-*.json`. Artifacts without a summary are
historical and use `20`, the prior c-val default. The additive
`db-migrate-dltest-iterations` command adds/backfills this column on existing
DBs without rebuilding millions of metric rows.

## NCCL and IB health (`test-nccl.db`)

The NCCL phase runs `ibbw.sh`, which **auto-detects every IB device and port**
under `/sys/class/infiniband/` (an optional numeric range still restricts it to
`mlx5_<start>..mlx5_<end>`). It samples each port's `port_xmit_data` counter
during the all-reduce. `single-node-allreduce.py` summarizes those samples per
port under `GCR_IB_PORT_BW_GBPS` and records `GCR_ITERATIONS`, aggregate
`GCR_BUSBW`, and aggregate `GCR_LATENCY`:

```json
"GCR_IB_PORT_BW_GBPS": {
  "mlx5_4":   {"avg_gbps": 20.285, "max_gbps": 46.236, "last_gbps": 46.1, "samples": 26},
  "mlx5_13":  {"avg_gbps": 20.330, "max_gbps": 46.308, "last_gbps": 46.2, "samples": 26},
  "mlx5_5.2": {"avg_gbps": 12.000, "max_gbps": 24.000, "last_gbps": 23.0, "samples": 26}
}
```

Port labels use the bare device name for port 1 (`mlx5_4`). `db-update.sh`
ingests the summary via `cval db-add-nccl-health` into `IB_HEALTH`, with exactly
**one row per node/test run**. Only each port's `max_gbps` is retained:

```text
Node, timestamp, la_timestamp, iterations, image_name, cuda, pytorch, samples,
BUS_BW, LATENCY, mlx5_0, mlx5_1, ... mlx5_13
```

- `BUS_BW` (GB/s) and `LATENCY` (ms) are the aggregate 8-GPU all-reduce values.
- `mlx5_0` ... `mlx5_13` are maximum observed transmit bandwidths in GB/s.
- `samples` is the number of HCA counter samples collected during the run.
- `la_timestamp` is ISO-8601 in `America/Los_Angeles`.
- `cuda` / `pytorch` are the versions detected inside the validation image.

Dynamic NCCL baselines and `cval results --test nccl` read `IB_HEALTH`.
The export mirrors the same wide one-row-per-node shape and appends
classification columns. Legacy tables are renamed to
`OLD_nccl_performance` and `OLD_nccl_ib_port_performance` for rollback and are
not read by normal operations. `cval db-migrate-nccl-health` performs the
consolidation, table renames, and view creation; it is safe to rerun.

### NCCL operational views

`LATEST_NODE_STATUS` contains the complete latest `IB_HEALTH` record for each
node (maximum `timestamp` per `Node`).

`NODE_RANKING` averages each node's latest five `IB_HEALTH` records. If fewer
than five exist, all available records are used. Its columns are:

```text
node, bus_bw, bus_bw_pctl, latency, latency_pctl, mlx5_0, ... mlx5_13
```

- `bus_bw`, `latency`, and every `mlx5_*` value are rolling averages.
- `bus_bw_pctl` is `PERCENT_RANK() × 100` ordered by `bus_bw`: a low value is a
  low fleet bandwidth percentile (rounded to two decimals).
- `latency_pctl` is `PERCENT_RANK() × 100` ordered by `latency`: a low value is
  a low (better) fleet latency percentile (rounded to two decimals).
- Rows are ordered by `bus_bw` ascending, then node name.
- SQLite `AVG` ignores missing historical port values; real zero values remain
  part of the average.