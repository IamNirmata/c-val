# Raw SQLite Database Design

## Scope

c-val writes seven authoritative raw SQLite databases under
`/data/continuous_validation/metadata/`. Paths are configured in
[`config/cval.toml`](../../config/cval.toml). Logs and retained test artifacts
are outside this design.

`(node, timestamp)` is the shared run identity. DL metrics also use `run_key`.
The current writer creates no evaluator/classification tables or cross-database
foreign keys. Retained historical objects are listed below.

| Database | Main table | Row grain | Purpose |
|---|---|---|---|
| `validation.db` | `runs` | test status per run | Final storage, NCCL, DL, and aggregate status |
| `test-storage.db` | `storage_performance` | one row per run | FIO IOPS and bandwidth metrics |
| `test-nccl.db` | `IB_HEALTH` | one row per run | NCCL bandwidth/latency and HCA-port maxima |
| `dltest_numerical_correctness.db` | `numerical_correctness` | rank/task/metric | DL correctness metrics |
| `dltest_compute_performance.db` | `compute_performance` | rank/task/metric | DL operator timings |
| `dltest_collective_performance.db` | `collective_performance` | rank/task/metric | Collective timings |
| `dltest_overlap_performance.db` | `overlap_performance` | rank/task/metric | Compute/collective overlap timings |

## Write Contract

One run is ingested in this order:

```text
immutable result -> storage -> NCCL -> four DL DBs -> validation status
```

`validation.db` receives four rows (`storage`, `nccl`, `dltest`, `all`) in one
transaction only after required metric writes succeed. It is the completion
marker. Earlier metric rows can exist without final status after a late
ingestion failure; audits must correlate exact timestamps.

The four DL writes are serialized by one metadata-directory lock and publish
the same completed generation ID. Schema owners are
[`ingest.py`](../../cval/storage/ingest.py) and
[`dltest_ingest.py`](../../cval/storage/dltest_ingest.py).

## Table Shapes

```sql
-- validation.db; logical key: (node, test, timestamp)
runs(node, test, timestamp, result,
     image_name, pytorch_version, cuda_version)

-- test-storage.db; primary key: (node, timestamp)
storage_performance(node, timestamp, image_name,
  iodepth_read_1file_iops, iodepth_read_1file_bw,
  iodepth_write_1file_iops, iodepth_write_1file_bw,
  numjobs_read_nfiles_iops, numjobs_read_nfiles_bw,
  numjobs_write_nfiles_iops, numjobs_write_nfiles_bw,
  randread_iops, randread_bw, randwrite_iops, randwrite_bw)

-- test-nccl.db; primary key: (Node, timestamp)
IB_HEALTH(Node, timestamp, iterations, data_size_gb,
  image_name, cuda, pytorch, samples, BUS_BW, LATENCY,
  mlx5_0, ..., mlx5_13)

-- Standard DL tables; composite primary key shown below
metric_table(run_key, node, cval_timestamp, iterations,
  sample_dir, test_plan, dltest_run_id, rank,
  task_group, task_name, status, metric_name, metric_value, source_file)
PRIMARY KEY (run_key, rank, task_group, task_name, metric_name)
```

`overlap_performance` additionally stores `coll_name` and `layer_name`.
Every DL DB also has:

- `cval_ingested_runs`: one receipt per ingested run.
- `cval_ingest_metadata`: current generation ID, state, and update time.
- `cval_ingest_migrations`: completed additive migrations.

## Evaluator Read Contract

Use explicit table names, never the first table in a database. Join DL rows by
`node, cval_timestamp` to `runs.node, runs.timestamp`; read the stored `run_key`
for receipts. Current keys are `<node>-<timestamp>`; older keys may start with
`dltest-`. Do not reconstruct them from a hard-coded prefix.

Read only committed status sets and their exact-run metrics, not independent
latest-per-node rows. Deduplicate identical historical status rows; conflicting
duplicates are indeterminate. DL generation IDs/state must agree before and
after a bounded read; retain per-run receipts and rank-coverage checks. A global
generation is a publication marker, not a run ID or an ingestion cursor.

Run timestamps identify submissions, not arrival order: a delayed run can
commit after a newer one. Do not process new results using only `MAX(timestamp)`.
The DBs do not provide a complete hardware/test-config cohort; verified run
provenance is needed before pooling measurements across environments.

## Lightweight Review (2026-09-08 UTC)

Read through `gcr-admin/gcr-admin-pvc-access` using SQLite `mode=ro` and
`query_only=ON`: schema metadata, four status rows, one storage/NCCL row, and
two rows per DL table. No full counts or integrity scans were repeated.

- Latest aggregate sample: `slc01-cl02-hgx-0368`, timestamp `1788680072`;
  all four statuses passed, and matching metrics existed in all seven DBs.
- Four DL DBs shared completed generation
  `1788818284360325-f35f4c9a11ca48d6b15003cbdc3ab65d`.
- DL files were approximately 3.75 / 5.29 / 0.66 / 3.18 GB
  (numerical / compute / collective / overlap).
- Historical NCCL tables/views, including `NODE_RANKING`, remain present;
  they are not evaluation inputs. This sampled review is not a full integrity
  or freshness guarantee for every node.

The proposed independent evaluator is described in
[evaluation-engine-design.md](evaluation-engine-design.md); it must not update
these raw databases.

## Accepted-Run Example

Run `slc01-cl02-hgx-0106-1788475963`, commit
`9678f1756c60e2b3b82f32f070712332ce8604e6`:

```text
validation: storage=pass, nccl=pass, dltest=pass, all=pass
storage:    randread_iops=32683.598307, randwrite_iops=20242.567658
NCCL:       BUS_BW=44.530298, LATENCY=628.785377, samples=29
DL sample:  norm_output=82.0, bp_cpu_time=0.062,
            collective cpu_time=0.102, overlap coll_mean=6.581
```

Exact-run status query:

```sql
SELECT test, result
FROM runs
WHERE node = 'slc01-cl02-hgx-0106' AND timestamp = 1788475963
ORDER BY test;
```

## Live PVC Snapshot (2026-09-04)

| Database | Size | Rows/runs |
|---|---:|---:|
| `validation.db` | 954,368 B | 6,984 status rows; 477 nodes |
| `test-storage.db` | 446,464 B | 2,008 rows; 477 nodes |
| `test-nccl.db` | 1,318,912 B | 1,834 current rows; 477 nodes |
| numerical DL | 3.57 GB | 3,861,208 metrics; 1,781 runs |
| compute DL | 5.03 GB | 5,414,240 metrics; 1,781 runs |
| collective DL | 632 MB | 683,904 metrics; 1,781 runs |
| overlap DL | 3.04 GB | 2,735,616 metrics; 1,781 runs |

The seven files total about 12 GiB; the PVC had about 80 TiB free. Read-only
checks found:

- All four DL DBs have identical run and receipt sets, no missing/orphan
  receipts, no null node/timestamp keys, and one shared `complete` generation.
- Full SQLite `quick_check` passed for validation, storage, NCCL, and collective
  DL. Other large DL scans were intentionally skipped; logical checks passed.
- `validation.db` has one retained January 2026 duplicate group: three
  identical `all=pass` rows, not conflicting evidence.
- `IB_HEALTH` has 800 older rows missing only `samples`; none are from the last
  30 days. Current accepted rows are complete.
- `test-nccl.db` and `test-storage.db` retain old tables/ranking views. Current
  raw-only writers do not use them; preserve them as historical evidence.