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

## NCCL per-HCA-port IB bandwidth

The NCCL phase runs `ibbw.sh`, which **auto-detects every IB device and port**
under `/sys/class/infiniband/` (an optional numeric range still restricts it to
`mlx5_<start>..mlx5_<end>`). It samples each port's `port_xmit_data` counter
during the all-reduce. `single-node-allreduce.py` averages those samples per
port into the NCCL summary JSON under `GCR_IB_PORT_BW_GBPS`:

```json
"GCR_IB_PORT_BW_GBPS": {
  "mlx5_4":   {"avg_gbps": 20.285, "max_gbps": 46.236, "last_gbps": 46.1, "samples": 26},
  "mlx5_13":  {"avg_gbps": 20.330, "max_gbps": 46.308, "last_gbps": 46.2, "samples": 26},
  "mlx5_5.2": {"avg_gbps": 12.000, "max_gbps": 24.000, "last_gbps": 23.0, "samples": 26}
}
```

Port labels use the bare device name for port 1 (`mlx5_4`) and `device.port`
for additional ports (`mlx5_5.2`). `db-update.sh` ingests this block via
`cval db-add-nccl-ports` into an **additive** `nccl_ib_port_performance` table in
`test-nccl.db`, one row per port:

```text
node  timestamp  device    image_name  avg_gbps  max_gbps  last_gbps  samples
```

The aggregate all-reduce `busbw`/`latency` stay in the separate
`nccl_performance` table (the input baselines classify against). `cval results
--test nccl` emits one row per (node, IB port) with `port_avg_gbps`/`port_max_gbps`
plus the node's aggregate `node_allreduce_busbw`/`node_allreduce_latency_ms`;
`--active-ports-only` drops idle (zero-bandwidth) ports.