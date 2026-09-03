# c-val

c-val is a deterministic continuous-validation framework for GPU clusters. It
prioritizes stale coverage across the GPU-node inventory, checks availability
one node at a time in priority order, runs real Volcano validation jobs from
exact commits, supervises registered checks, ingests current result databases,
and classifies nodes with robust baselines.

## Safety defaults

- Development validation runs directly on one eligible cluster node from an
  exact published commit and requires `--submit --confirm submit`.
- `plan --git-ref <40-hex-commit>` remains a read-only queue-inspection command;
  it is not a local test gate.
- The source-controlled Git ref is an all-zero fail-closed placeholder; every
  real submission supplies an exact published nonzero commit.
- Status, overview, results, classifications, baseline reads, and job monitoring
  are read-only.
- A degraded verdict never mutates Kubernetes or a node.
- No credentials, kubeconfigs, or tokens are embedded in manifests.
- Backup and classification split helpers provide nonwriting inspection and
  explicit apply confirmations; they never delete source data.

## Current data model

Raw DBs under `/data/continuous_validation/metadata/`:

- `validation.db` — authoritative pass/fail and latest status;
- `test-storage.db`;
- `test-nccl.db`;
- four `dltest_*.db` component metric files.

The median/MAD baseline engine is the sole storage/DL evaluator. Baselines are stored per
storage/DL component and move through
`candidate → active → superseded`. Classification is stored in deterministic
per-target DBs and reported as `normal`, `degraded`, or `improved`.

## Quick start

```text
python -m cval.cli status
python -m cval.cli validate --node <node> --git-ref <40-hex-commit> --submit --confirm submit
python -m cval.cli baseline classify --test-type storage --store-results
python -m cval.cli classifications --test all --type csv
```

## Validation tests

The generic runner loads repository-local `cval.test.v1` descriptors. A new
test can remain pass/fail-only or declare baseline/export hooks. Unique
persistence is designed and implemented by that test; c-val does not create a
generic per-test common DB.

New runs emit canonical `cval.results`. Historical `cval.results.v1` and
`cval.results.v2` artifacts remain readable.

## Evaluator workload

`deploy/cval-evaluator/` contains one fail-closed always-on CPU Deployment named
`cval-evaluator`. The pod runs storage/DL baseline and classification loops plus
NCCL outbox ingestion, PostgreSQL baseline building, recovery, and evaluation.
It uses `python:3.12-slim`, clones/fetches configurable
`CVAL_GIT_REPO` and a reviewed commit in `CVAL_GIT_REF`, verify exact checkout,
install c-val, and validate the registry. They request no GPU/RDMA and contain
no `kubectl`. Source manifests retain fail-closed commit/storage placeholders;
reviewed NCCL runtime images and Python wheels are pinned by digest/hash.

The base Deployment and PostgreSQL StatefulSet both have zero replicas. No
manifest is applied or scaled automatically.

## Optional NCCL PostgreSQL evaluator

NCCL is the explicit exception to the storage/DL `cval.baselines` evaluator
rule. Its opt-in PostgreSQL implementation follows the checked-in NCCL spec;
`metadata/test-nccl.db` remains authoritative raw evidence, not an evaluator.

The unpublished `cval.nccl_eval` subsystem is isolated from the current raw
SQLite path and remains disabled by the NCCL descriptor. It uses one
PostgreSQL database with append-only raw rows, immutable median-centered
baseline versions, and fenced durable queue claims. Its database mutations
remain separately exact-confirmation gated.

Validation jobs have no PostgreSQL credentials. When explicitly enabled they
write immutable `pending/<run>.json` before current SQLite writes, then expose
it with a digest-bound `committed/<run>.json` marker only after those writes
commit. The credentialed NCCL process in the resident evaluator ingests each
file idempotently and retains it. PostgreSQL and evaluator source replicas are
zero. NCCL images are pinned to
reviewed digests; Git commits and the RWO storage class remain fail-closed
placeholders. The complete Python 3.12 dependency lock is hash-pinned and its
exact bootstrap has been verified in the pinned image. Reviewed phased
overlays do not grant live apply approval.

Evaluator latency is canonicalized to microseconds. Native outbox ingestion
converts the raw `IB_HEALTH.LATENCY` summary value from milliseconds to
microseconds (`628.2` becomes `628200.0 us`). Iterations, nullable samples, and
warmup count are part of the type-aware profile
fingerprint, so materially different workloads never share a baseline.

See [docs/evals/nccl-eval-process.md](docs/evals/nccl-eval-process.md) for the
schema, claim fencing, health-band derivation, role grants, and test contract.

## Documentation

The maintained references are:

- [architecture](docs/architecture.md);
- [configuration](docs/configuration.md);
- [operations](docs/operations-runbook.md);
- [result schema](docs/result-schema.md);
- [NCCL evaluator specification](docs/evals/nccl-eval-process.md).
