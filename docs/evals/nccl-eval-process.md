# NCCL evaluation process: SQLite proposal

> **Status: proposal for review.** This document describes the desired lean
> design. The repository still contains PostgreSQL NCCL code and deployment
> resources; approving this document does not change or deploy them.

## 1. Decision

Use SQLite for NCCL evaluation, consistent with the rest of c-val.

Keep raw measurements and derived evaluator data separate:

```text
/data/continuous_validation/
├── metadata/
│   ├── validation.db                authoritative run/pass/fail status
│   └── test-nccl.db                 authoritative NCCL measurements
└── baselines/
    ├── test-nccl-baselines.db       versioned baseline records
    └── nccl-classifications.db      derived node verdicts
```

Use one resident evaluator process as the only writer to the two derived NCCL
files. Validation jobs remain the only writers to `test-nccl.db`.

There is no PostgreSQL server, database Secret, RWO PostgreSQL PVC, result
outbox, ingestion queue, worker claim, retry queue, schema owner, or runtime
role.

## 2. Goals

- Retain `metadata/test-nccl.db` as authoritative raw evidence.
- Compare only results produced by materially compatible NCCL environments.
- Build robust BUS_BW and LATENCY baselines from reviewed, successful results.
- Keep baseline history immutable and require deliberate activation.
- Classify nodes as `improved`, `normal`, `underperforming`, `degraded`, or
    `dnr`.
- Preserve the exact baseline identity and metric details used by each verdict.
- Make every write idempotent and recoverable from the raw DB.
- Keep ordinary reads and exports nonmutating.

## 3. Non-goals

This design does not provide:

- multiple evaluator writers;
- a distributed work queue;
- automatic node remediation;
- automatic baseline activation;
- deletion or rewriting of raw results;
- a second normalized copy of NCCL raw measurements;
- a separate health-class lookup table.

If c-val later needs multiple active evaluator replicas writing concurrently,
SQLite should be reconsidered. That is not a current requirement.

## 4. Raw evidence

### 4.1 Run status in `validation.db`

`metadata/validation.db` answers whether NCCL ran successfully. The evaluator
reads the existing `runs` row for `(node, timestamp, test = 'nccl')`.

Made-up `runs` rows:

| node | timestamp | test | result | image_name |
|---|---:|---|---|---|
| `node-a` | 1790000100 | `nccl` | `pass` | `pytorch:26.05-py3` |
| `node-b` | 1790000200 | `nccl` | `fail` | `pytorch:26.05-py3` |
| `node-c` | 1790000300 | `nccl` | `pass` | `pytorch:26.05-py3` |
| `node-d` | 1790000400 | `nccl` | `pass` | `pytorch:26.05-py3` |
| `node-e` | 1790000500 | `nccl` | `pass` | `pytorch:26.05-py3` |

`node-b` becomes `dnr` because the run failed and no usable NCCL measurement
was produced. The evaluator does not invent BUS_BW or LATENCY values.

### 4.2 Measurements in `test-nccl.db`

`metadata/test-nccl.db` remains authoritative. Its `IB_HEALTH` table already
contains one consolidated row per node and timestamp, including:

- `Node`;
- `timestamp` and `la_timestamp`;
- `image_name`, CUDA, and PyTorch versions;
- iterations, samples, and data size when available;
- `BUS_BW`;
- `LATENCY`;
- `mlx5_*` diagnostic values.

The evaluator opens this DB read-only. It never adds a `classified` flag and
never updates raw rows.

Made-up `IB_HEALTH` rows:

| Node | timestamp | iterations | BUS_BW GB/s | LATENCY ms | image_name | gpu_model | nccl_version |
|---|---:|---:|---:|---:|---|---|---|
| `node-a` | 1790000100 | 20 | 47.4 | 590.0 | `pytorch:26.05-py3` | `NVIDIA B200` | `2.29.2` |
| `node-c` | 1790000300 | 20 | 44.4 | 635.0 | `pytorch:26.05-py3` | `NVIDIA B200` | `2.29.2` |
| `node-d` | 1790000400 | 20 | 40.8 | 680.0 | `pytorch:26.05-py3` | `NVIDIA B200` | `2.29.2` |
| `node-e` | 1790000500 | 20 | 35.2 | 760.0 | `pytorch:26.05-py3` | `NVIDIA B200` | `2.29.2` |

There is intentionally no `node-b` row because its NCCL workload crashed.

### 4.3 Additive provenance for new rows

New native rows should add these nullable columns through the existing additive
migration pattern:

- `run_id`;
- `source_commit`;
- `test_definition_version`;
- `gpu_model`;
- `nccl_version`;
- `driver_group`;
- `topology_class`.

New ingestion should populate every field. Historical rows remain readable but
are excluded from automatic calibration when required profile facts are blank.
An operator may explicitly select historical rows for a one-time candidate.

Latency remains stored in the raw DB's existing unit. The baseline record must
state that unit. A unit conversion, if needed, happens in Python before
comparison and is recorded in baseline provenance.

## 5. Compatibility profile

A baseline profile prevents unlike workloads from sharing a reference.

Build `profile_key` as SHA-256 over canonical JSON containing:

- test definition version;
- image identity;
- GPU model and GPU count;
- CUDA, PyTorch, and NCCL versions;
- driver compatibility group;
- topology class;
- iterations, samples, data size, datatype, and collective settings;
- latency unit.

No fuzzy matching is allowed. A missing required field means the result cannot
join an automatic calibration cohort.

Store the canonical profile JSON and `profile_key` in every baseline record.
Classifications refer to that same key.

## 6. Baseline database

Reuse the established c-val baseline lifecycle and SQLite storage pattern.

`baselines/test-nccl-baselines.db` contains the existing `baselines` table shape:

```text
baseline_id
test_type = nccl
status = candidate | active | superseded
stratum_key = profile_key
n_samples
window_days
method
schema_version
created_at
supersedes
metrics_json
```

`metrics_json` contains:

- canonical profile JSON and `profile_key`;
- exact raw sample identities `(node, timestamp)`;
- included and excluded sample identities with reasons;
- BUS_BW and LATENCY count, median, MAD, p05, p50, and p95;
- directional acceptance bands;
- derivation method version;
- raw unit and conversion evidence;
- source DB identity and build timestamp.

Baseline content and identity are immutable. Lifecycle changes update only
`status` and `supersedes` under one `BEGIN IMMEDIATE` transaction.

Exactly one active baseline is allowed per profile.

### 6.1 What `candidate → active → superseded` means

- **candidate**: a newly calculated baseline waiting for operator review. It is
    not used to classify production results.
- **active**: the reviewed baseline currently used for classification. There is
    exactly one active baseline per compatibility profile.
- **superseded**: an older baseline replaced by a newer active one. It remains
    immutable so historical verdicts still show which baseline they used.

Example lifecycle:

1. `nccl-b200-v1` is activated from 40 reviewed results.
2. Ten more reviewed results arrive. `nccl-b200-v2` is built as a candidate.
3. Operators compare v2 with v1 and approve v2.
4. One transaction marks v2 active and v1 superseded.
5. Existing classifications keep pointing to v1; new classifications use v2.

Made-up `baselines` rows:

| baseline_id | test_type | status | stratum_key | n_samples | created_at | supersedes | metrics_json summary |
|---|---|---|---|---:|---:|---|---|
| `nccl-b200-v1` | `nccl` | `superseded` | `sha256:b200-profile` | 40 | 1790001000 |  | BW median `44.5`, latency median `629` |
| `nccl-b200-v2` | `nccl` | `active` | `sha256:b200-profile` | 50 | 1790501000 | `nccl-b200-v1` | BW median `44.6`, latency median `628` |
| `nccl-b200-v3` | `nccl` | `candidate` | `sha256:b200-profile` | 60 | 1791001000 | `nccl-b200-v2` | awaiting operator review |

In this example, v2 is used now. V3 does nothing until explicitly activated.

## 7. Baseline policy

### 7.1 Eligibility

A raw result is eligible only when:

- the corresponding NCCL test passed;
- BUS_BW and LATENCY are finite and greater than zero;
- required profile fields are present and match exactly;
- the result is not duplicated;
- the node/result belongs to the reviewed calibration cohort;
- configured sanity checks pass.

Do not silently train on every fleet result. Known degraded or maintenance nodes
must not pull the reference downward.

### 7.2 Build and activation

Recommended defaults:

- minimum candidate size: 40 reviewed results;
- create a new candidate after 10 additional eligible results;
- never auto-activate a candidate;
- activation supersedes the previous active baseline for that profile.

Candidate creation is idempotent. Rebuilding the same sample set and profile
must produce the same baseline ID and byte-equivalent record.

### 7.3 Statistics

Use the existing c-val robust baseline method:

- center: median;
- robust scale: $1.4826 \times \mathrm{MAD}$;
- engineering tolerance floor;
- BUS_BW direction: lower is bad;
- LATENCY direction: higher is bad.

The derivation version is part of the baseline identity. Changing formulas
creates a new candidate; it never edits an active record.

### 7.4 Five verdict bands

The active baseline stores exact boundaries for both metrics:

| Stored code | Display label | Meaning |
|---|---|---|
| `improved` | Improved | Better than the active baseline's good-side limit |
| `normal` | Normal | Within the active baseline limits |
| `underperforming` | Underperforming | Outside normal limits but not yet severe |
| `degraded` | Degraded | Beyond the severe performance limit |
| `dnr` | DNR | Did not run, crashed, timed out, or produced no usable result |

| Verdict | BUS_BW, where higher is better | LATENCY, where lower is better |
|---|---|---|
| `improved` | at or beyond the reviewed good-side threshold | at or beyond the reviewed good-side threshold |
| `normal` | within the baseline's normal limits | within the baseline's normal limits |
| `underperforming` | outside normal, but not beyond the severe limit | outside normal, but not beyond the severe limit |
| `degraded` | beyond the severe low limit | beyond the severe high limit |
| `dnr` | no usable measurement because the test did not run, crashed, timed out, or produced invalid/missing metrics | same |

For a made-up profile with BUS_BW median `44.5 GB/s` and LATENCY median
`629 ms`, the stored boundaries could be:

| Metric | Improved | Normal | Underperforming | Degraded |
|---|---|---|---|---|
| BUS_BW | `>= 46.7` | `42.3 <= x < 46.7` | `37.8 <= x < 42.3` | `< 37.8` |
| LATENCY | `<= 597.6` | `597.6 < x <= 660.5` | `660.5 < x <= 723.4` | `> 723.4` |

These example numbers use approximately 5% normal/good-side limits and a 15%
severe boundary. Production boundaries come from the versioned median/MAD
derivation plus configured engineering floors. They are stored in
`metrics_json`, not recomputed differently by each reader.

## 8. Classification database

`baselines/nccl-classifications.db` uses the established
`classification_results` schema with one additive column:

```text
raw_timestamp INTEGER NOT NULL
n_underperforming INTEGER NOT NULL DEFAULT 0
```

Each row records:

- classification timestamp;
- node;
- `test_type = nccl`;
- baseline ID;
- overall status;
- compared/improved/underperforming/degraded counts;
- worst percentage difference;
- `metrics_json` with BUS_BW and LATENCY details.

Protect the natural immutable identity with a unique index:

```text
(node, raw_timestamp, baseline_id)
```

An exact retry is a no-op; conflicting content fails closed. `metrics_json`
also records `raw_timestamp` so exports remain self-describing.

Made-up `classification_results` rows showing all five verdicts:

| classified_at | node | raw_timestamp | baseline_id | status | n_compared | n_improved | n_underperforming | n_degraded | metrics_json summary |
|---:|---|---:|---|---|---:|---:|---:|---:|---|
| 1791002000 | `node-a` | 1790000100 | `nccl-b200-v2` | `improved` | 2 | 2 | 0 | 0 | BW `47.4`, latency `590.0` |
| 1791002000 | `node-c` | 1790000300 | `nccl-b200-v2` | `normal` | 2 | 0 | 0 | 0 | BW `44.4`, latency `635.0` |
| 1791002000 | `node-d` | 1790000400 | `nccl-b200-v2` | `underperforming` | 2 | 0 | 2 | 0 | BW `40.8`, latency `680.0` |
| 1791002000 | `node-e` | 1790000500 | `nccl-b200-v2` | `degraded` | 2 | 0 | 0 | 2 | BW `35.2`, latency `760.0` |
| 1791002000 | `node-b` | 1790000200 | `nccl-b200-v2` | `dnr` | 0 | 0 | 0 | 0 | test crashed; no measurement |

### Verdict rules

For one raw NCCL result:

1. if the NCCL run did not complete with two usable metrics, set overall `dnr`;
2. otherwise classify BUS_BW and LATENCY against the active baseline;
3. set overall `degraded` if either metric is degraded;
4. otherwise set overall `underperforming` if either metric is underperforming;
5. otherwise set overall `normal` if both metrics are normal, or one is improved
    and the other is normal;
6. set overall `improved` only when both metrics are improved.

Overall precedence from worst to best is:

```text
dnr → degraded → underperforming → normal → improved
```

`dnr` is operationally worst because there is no trustworthy performance
measurement, but it is not a numeric performance class. Keep the run's failure
reason in `metrics_json`.

Never average metrics to hide one degraded dimension. `mlx5_*` values remain
diagnostic evidence and do not affect the initial overall verdict.

Raw pass/fail and derived performance health remain separate. A test can pass
functionally while its performance classification is underperforming or
degraded. A failed or incomplete NCCL run produces `dnr`.

## 9. Resident evaluator cycle

One resident evaluator process runs the following loop:

1. open `test-nccl.db` read-only;
2. enumerate exact compatibility profiles;
3. build due candidates from reviewed samples;
4. read the active baseline for each profile;
5. scan recent raw rows for that profile;
6. calculate missing classifications;
7. write one classification transaction with `BEGIN IMMEDIATE`;
8. emit a structured cycle receipt;
9. sleep for the configured interval.

There is no durable queue. Recovery is deterministic: after a crash, the next
cycle scans raw rows again and idempotently fills missing classifications.

The evaluator is the only writer to NCCL baseline/classification DBs. Readers
use SQLite read-only URIs. Use a bounded busy timeout and the default rollback
journal; do not enable WAL on the shared NFS PVC.

## 10. Commands

Proposed operator surface, reusing existing baseline commands:

```text
# inspect/build candidate
cval baseline build --test-type nccl --store

# deliberate promotion
cval baseline activate <baseline-id> nccl

# classify one node or all recent nodes
cval baseline classify --test-type nccl --node <node> --store-results
cval baseline classify --test-type nccl --store-results

# read/export
cval baseline list --test-type nccl
cval classifications --test nccl --type csv
cval results --test nccl --type csv
```

These commands are proposed, not currently available. Implementation should
restore NCCL through the registry adapter and normal `cval.baselines` APIs,
without introducing a second evaluator framework.

Candidate build, activation, and classification writes must keep their existing
explicit mutation gates. List/show/results/classification export remain
read-only.

## 11. Backup and concurrency safety

- Whole-root backup still requires writer quiescence and exact confirmation.
- Stop the resident evaluator before backup.
- Ensure no validation job is writing `test-nccl.db`.
- Reject backup while `-wal`, `-shm`, or `-journal` sidecars exist.
- Verify the backup manifest before any cutover.
- Never place derived tables inside `test-nccl.db`.
- Never run two resident evaluator writers.

## 12. Transition from the PostgreSQL implementation

Approval of this document should lead to a separate implementation change:

1. keep `test-nccl.db` and all historical artifacts unchanged;
2. add native provenance columns to raw ingestion additively;
3. restore an NCCL adapter in `cval.baselines`;
4. add the two derived SQLite paths and schemas;
5. implement candidate build, activation, classification, and exports;
6. run one copied-DB rehearsal;
7. run one exact-commit on-cluster NCCL validation;
8. compare raw metrics and derived verdicts;
9. only then remove PostgreSQL code, dependencies, Secrets, manifests, and docs;
10. preserve any PostgreSQL data created before cutover as read-only evidence.

Do not run a live migration or delete PostgreSQL/PVC data as part of document
approval.

## 13. Acceptance criteria

The SQLite implementation is ready when:

1. new NCCL runs still write authoritative `test-nccl.db` rows;
2. the evaluator never writes the raw DB;
3. incompatible profiles cannot share a baseline;
4. no candidate is created below 40 reviewed samples;
5. candidates never auto-activate;
6. active baseline history is preserved through supersession;
7. BUS_BW and LATENCY classifications are deterministic at all three numeric
    boundaries; DNR is handled independently from numeric bands;
8. overall status implements all five verdicts and uses the worse metric;
9. exact reruns create no duplicate baseline or classification rows;
10. restart recovery requires no queue repair;
11. read/export commands are nonmutating;
12. backup rejects active writers and SQLite sidecars;
13. one real exact-commit cluster run produces raw and derived evidence;
14. PostgreSQL is no longer required by the active evaluator path.

## 14. Review decisions

Please approve or change these defaults before implementation:

| Decision | Recommended default |
|---|---|
| Verdict model | `improved` / `normal` / `underperforming` / `degraded` / `dnr` |
| Initial calibration size | 40 explicitly reviewed results |
| Candidate refresh | every additional 10 eligible results |
| Activation | manual only |
| Historical rows with incomplete profile facts | excluded unless explicitly selected |
| Derived files | separate baseline and classification SQLite DBs |
| Evaluator writers | exactly one resident writer |
| Journal mode on shared PVC | rollback journal, not WAL |

The main simplification is intentional: raw SQLite evidence plus two derived
SQLite files and one resident writer. Everything else should use existing c-val
baseline, classification, backup, and export machinery.
