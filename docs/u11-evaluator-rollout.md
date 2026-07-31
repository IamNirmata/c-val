# U11 evaluator rollout preparation

U11 remains **IN PROGRESS / BLOCKED for live criteria**. This document covers
locally audited and validated preparation only. It does not authorize Kubernetes access,
PVC reads or writes, database creation/migration, image publication, CronJob
activation, compatibility cutover, or deployment.

## Local architecture

`cval-evaluator` is a one-shot process, not a daemon. Every invocation performs:

1. verify the embedded 40-character release commit equals
   `CVAL_EXPECTED_COMMIT` and reject the all-zero placeholder;
2. require the declared `CVAL_IMAGE_REF` to be digest-pinned,
   non-placeholder, and exactly equal to the rendered container `image` field;
3. run read-only `cval.evaluator-preflight.v1` checks;
4. run exactly one U9 evaluation cycle; and
5. emit exactly one `cval.evaluator-cycle.v1` JSON object to stdout.

The envelope includes release commit, image reference, effective config digest,
preflight report, duration, U9 report, process exit code, bounded output-count
diagnostics, and an explicit `stdout-only` logging policy. Dependency stdout and
stderr are suppressed without copying their contents into the envelope. SIGINT
and SIGTERM interrupt the in-process evaluator and produce the same final JSON
envelope with exit code `128 + signal`. Unexpected `SystemExit` from preflight
or evaluator dependencies is contained as a redacted dependency failure with
exit code 2 rather than escaping the envelope. It never writes PVC log files.

The checked-in Kustomize base and both variants are under
`deploy/cval-evaluator/`:

- `base/`: `batch/v1` CronJob, tokenless ServiceAccount with no Role or binding,
  and evaluator-scoped deny-all ingress/egress NetworkPolicy;
- `overlays/shadow/`: read-only PVC, no `--write-enabled`, and no `--apply`;
- `overlays/apply/`: read-write PVC, explicit `--write-enabled`, and exact
  `--apply --confirm evaluate` arguments.

Every source-controlled CronJob has `suspend: true`, `concurrencyPolicy: Forbid`,
start/runtime deadlines, zero retries, bounded history/TTL/CPU/memory/
ephemeral-storage resources, non-root
restricted security contexts, read-only root filesystem, and a bounded `/tmp`
`emptyDir`. There are no ports, host namespaces, host paths, service-account
tokens, GPU/RDMA resources, init containers, runtime package installation,
Git checkout, Kubernetes client, or network requirement.

The rendered `CVAL_IMAGE_REF` declaration is tested to equal the rendered image
digest exactly. Runtime code can verify only that declaration and the embedded
commit marker; it cannot prove which image the container runtime actually
started. Admission policy plus verified image signatures and provenance must
bind the admitted digest to the running workload, and this remains a live
blocker.

The image and commit use syntactically valid all-zero placeholders. Placeholder
manifests are deliberately non-runnable: startup release verification fails
closed. The PVC claim and both pinned base-image defaults in the checked-in
`deploy/cval-evaluator/Dockerfile` are also reviewed replacement markers. The
recipe installs only a prebuilt `cval` wheel and an exact `PyYAML==6.0.2` wheel
from a local wheelhouse with `--no-index`, verifies the dependency wheel against
the supplied SHA-256, injects the exact non-placeholder `BUILD_COMMIT`, sets the
OCI revision label, and copies only the installed tree, config, and three
descriptor/plugin pairs into a distroless non-root UID/GID 65532 final image.
The final stage has no shell, Git, pip, package manager, Kubernetes client,
compiler, GPU/RDMA runtime, or runtime network requirement. The manifest reads
`/app/config/cval.toml`; the image contains that exact path, while the installed
package and test descriptors/plugins are read-only beneath `/workspace/c-val`.
No image is claimed until approved base digests, registry, signing identity,
SBOM/provenance publication, and vulnerability/license review are supplied.

## Offline commands

All examples use local/disposable copies only and produce strict JSON.

```bash
# Config/registry/path/ownership/schema preflight; no writes or locks.
python -m cval.cli --config <local-copy-config.toml> \
  evaluator-preflight --validation-root <local-copy-root> --access ro

# Deterministic, non-authoritative direction parity from copied inputs.
python -m cval.cli evaluator-parity \
  --u8-json <u8-labels.json> \
  --compatibility-db <copied-classification-results.db>

# Backup plan only. Source and destination must both be outside the configured
# runtime root, and the destination parent must already exist.
python -m cval.cli evaluator-backup \
  --source-root <disposable-copy-root> \
  --destination <new-backup-directory>

# Local-copy execution: separate --apply plus exact backup confirmation.
python -m cval.cli evaluator-backup \
  --source-root <disposable-copy-root> \
  --destination <new-backup-directory> \
  --apply --confirm backup

# Render manifests locally. Rendering must still contain suspend=true.
kustomize build deploy/cval-evaluator/overlays/shadow > /tmp/cval-evaluator-shadow.yaml
kustomize build deploy/cval-evaluator/overlays/apply > /tmp/cval-evaluator-apply.yaml
```

The backup API takes and continuously revalidates each U7 evaluator lock only
during apply. It rejects rollback-journal, WAL, and SHM sidecars immediately
around each immutable snapshot, revalidates source identity, preserves a U8 DB
and owner-only activation key as one unit, and compares source/destination
schema, logical table inventories, row identities, row counts, and content
digests. It never reports key bytes or a standalone key hash. Source DB/key
files must be current-UID, single-link regular files with exact safe modes.

The source must be neither `runtime.validation_root` nor any of its descendants.
That rejection occurs before destination reservation or evaluator-lock creation.
The destination must be outside `runtime.validation_root`, disjoint from the
source, traversal-free, and beneath a pre-existing current-UID safe-mode parent.
Every lexical ancestor is checked for symlinks before resolution. Apply reserves
the final destination atomically with `mkdir`, never overwrites it, fsyncs files
and directories, and removes a partial destination only while its original
device/inode identity is still bound. Dry-run validates only and creates no
directory or lock. A separately approved live backup procedure is still
required.

Parity accepts copied JSON and SQLite inputs. Every JSON input must have exactly
a top-level array of record objects; duplicate object keys and the non-standard
`NaN`, `Infinity`, and `-Infinity` constants are rejected during parsing. U8 JSON requires exact non-empty
TEXT node/test/run identities, both matching stable class name and exact integer
code, an `hb1:<64 lowercase hex>` baseline or null according to class/DNR
semantics, and exactly one stable `DnrReason` only for class 5. Boolean/float
codes and Boolean/float/text timestamps are rejected; optional timestamps must
be exact non-negative integers no greater than the SQLite signed 64-bit maximum
of $2^{63}-1$. Compatibility JSON similarly forbids label
lowercasing/coercion and requires non-empty text baseline identity. U8 labels
and compatibility labels remain unchanged in every
comparison. The report separately projects
`improved`, `normal`, `degraded`, and `dnr` direction buckets, rejects unknown
or invalid labels, reports coverage, matrices, divergences, and unpaired
records, and explicitly marks the projection non-authoritative. DNR never
becomes a compatibility label.

Copied U8 databases are read through one immutable snapshot. Before checking
whether history even exists, parity requires exactly one owner from the
effective registered test catalog and invokes the shared strict U7 owner
validator. Every `test_results` row, adapter schema owner, durable receipt, and
receipt parent must belong to that registered test. It then runs the bounded
streamed classification-history integrity audit against the same snapshot
before selecting latest node/test rows. Malformed typed history,
missing/mismatched owners, mixed ownership even without history, or receipt
owner/parent drift fail closed. Compatibility SQLite rows are not coerced: node, test, status, and
baseline must use non-empty TEXT storage; `classified_at` and `rowid` must use
valid INTEGER storage; and status must already be one of the stable lowercase
labels.

Preflight validates every existing component from `runtime.validation_root`
through each U7 owner and U8 parent, not only the nearest child. The root must
match the exact safe `health_evaluator.validation_root_mode`; descendants must
be current-UID, no-symlink, exact `0700`, owner-readable/searchable, and never
group/world writable. Required write parents or the nearest ancestor for a
missing health path must also be owner-writable. Captured device/inode/mode
identities are revalidated after DB/key reads, so an intermediate replacement
or symlink swap fails closed. Preflight invokes the same U7 owner/receipt helper
with the registered test ID even for schema v1 databases with no history.
Nested missing parents are inspected without being created.

## Reproducible image release procedure (not executed)

Run only in an approved release environment. Tool versions, builder/runtime
base digests, registry destination, KMS identity, and dependency-wheel digest
must be reviewed release inputs. The source tree must be clean and `COMMIT` must
be the exact checked-out 40-character commit.

```bash
export COMMIT="$(git rev-parse HEAD)"
export BUILDER_IMAGE='python:3.11-slim-bookworm@sha256:<reviewed-builder-digest>'
export RUNTIME_IMAGE='gcr.io/distroless/python3-debian12:nonroot@sha256:<reviewed-runtime-digest>'
export IMAGE_TAG='<approved-registry>/cval-evaluator:'"${COMMIT}"

mkdir -p -m 0700 reports

# Build wheel and sdist from local build dependencies only.
PIP_NO_INDEX=1 python -m build --no-isolation --wheel --sdist

# Acquire PyYAML outside the Docker build from an approved package mirror,
# retain the downloaded wheel as release evidence, and review its exact hash.
python -m pip download --only-binary=:all: --no-deps 'PyYAML==6.0.2' \
  --dest deploy/cval-evaluator/wheelhouse
export PYYAML_WHEEL_SHA256="$(sha256sum deploy/cval-evaluator/wheelhouse/PyYAML-6.0.2-*.whl | awk '{print $1}')"

# RUN steps have no network. BuildKit publishes maximal provenance and an SBOM
# with the pushed immutable result; metadata records the resulting digest.
docker buildx build --platform linux/amd64 --network=none --no-cache \
  --file deploy/cval-evaluator/Dockerfile \
  --build-arg BUILD_COMMIT="${COMMIT}" \
  --build-arg PYYAML_WHEEL_SHA256="${PYYAML_WHEEL_SHA256}" \
  --build-arg BUILDER_IMAGE="${BUILDER_IMAGE}" \
  --build-arg RUNTIME_IMAGE="${RUNTIME_IMAGE}" \
  --provenance=mode=max --sbom=true \
  --metadata-file reports/cval-evaluator-build-metadata.json \
  --tag "${IMAGE_TAG}" --push .

# Set IMAGE_REF from containerimage.digest in reviewed build metadata.
export IMAGE_REF='<approved-registry>/cval-evaluator@sha256:<built-digest>'
syft "${IMAGE_REF}" -o spdx-json=reports/cval-evaluator.spdx.json
cosign sign --key '<approved-kms-uri>' "${IMAGE_REF}"
cosign verify --key '<approved-verification-key-or-kms-uri>' "${IMAGE_REF}"
cosign tree "${IMAGE_REF}"
```

Review the wheel/sdist marker contents, OCI revision label, image user, SBOM,
BuildKit provenance, signature verification, vulnerability scan, license
inventory, and embedded marker equality before replacing the source-controlled
image/commit placeholders. Keep source-controlled CronJobs suspended. The
manifest image digest remains all-zero until that evidence and a separate
manifest change are approved.

## Reviewed live phases (not executed or approved)

### Phase 0 — evidence and backups

Block until U7 exists for every enabled health test, ownership permits
`O_NOATIME|O_NOFOLLOW`, WAL/SHM/rollback-journal sidecars are absent at snapshot
time, U8/key units are
inventoried, and an operator-approved live backup and restore rehearsal exists.
Do not use the local-copy backup command directly on the configured live root.

### Phase 1 — image and manifest review

Replace all three manifest placeholders with reviewed facts: namespace/PVC
claim, non-zero image digest, and the exact commit embedded in that image.
Generate and review SBOM, signature, provenance, vulnerability results, and
license inventory. Enforce admission/signature/provenance policy that binds the
admitted digest to the runtime image; environment declarations alone are not
runtime attestation. Re-run rendered-manifest/static tests and verify no runtime
network or toolchain dependency.

### Phase 2 — suspended shadow object

After separate approval, the exact production command will be of this form:

```bash
kubectl apply -n <reviewed-namespace> -k deploy/cval-evaluator/overlays/shadow
```

Risk: this creates Kubernetes objects and a deny-all policy selecting evaluator
pods. The object remains suspended, but applying an incorrect selector, PVC, or
namespace can affect production. Verify the rendered object and server-side dry
run/diff before requesting approval. Source control must remain suspended.

### Phase 3 — bounded manual shadow

Create one Job from the reviewed suspended CronJob, then inspect its single JSON
stdout envelope. This is read-only but accesses live PVC data and therefore
requires approval and ownership validation. Compare copied U8 and compatibility
outputs with the parity reporter. Establish an acceptance window and explicit
coverage/divergence thresholds; direction agreement alone is not class equality.

### Phase 4 — scheduled shadow

Only after shadow acceptance, a separately approved patch may set the shadow
CronJob `suspend=false`. Kubernetes CronJobs are approximately once-per-schedule,
so the evaluator remains idempotent and uses `Forbid` plus U9 locks. Monitor
missed schedules, deadlines, preflight failures, duration, and JSON exit codes.

### Phase 5 — apply variant and cutover

Do not unsuspend the apply variant until the live backup/restore evidence is
accepted, `[health_evaluator]` ownership/write facts are verified, and exact
apply/cutover commands receive approval. Apply may additively migrate U7 v1 to
v2, create U8/key units, store candidates, and append classification history.
It does not auto-activate candidates or replace compatibility readers. Any
reader/loop cutover is a later, explicit compatibility decision.

## Rollback

Rollback is non-destructive and starts by suspending both CronJobs:

```bash
kubectl patch cronjob -n <reviewed-namespace> cval-evaluator-shadow \
  --type=merge -p '{"spec":{"suspend":true}}'
kubectl patch cronjob -n <reviewed-namespace> cval-evaluator-apply \
  --type=merge -p '{"spec":{"suspend":true}}'
```

These commands mutate production and require explicit approval. Suspension does
not stop an already running Job; decide separately whether to let it finish.
Do not delete DBs, history, candidates, keys, Jobs, or logs. Keep compatibility
loops/readers unchanged until cutover acceptance. Restore only a complete
operator-approved backup unit; never restore or rotate an activation key alone.
Because U7 and U8 commits are cross-database non-atomic, use the cycle's stage
and partial-write report, validate immutable evidence, and retry idempotently
before considering restore.

Offline tests rehearse destination cleanup after forced termination, SQLite
transaction/integrity preservation, exact backup retry refusal, paired-key
restore validation, deterministic parity, and no-side-effect dry-runs.

## Live blockers

U11 cannot be marked DONE until all of the following are independently verified
and approved:

1. canonical U7 availability and schema/receipt completeness for every enabled
   health test;
2. evaluator UID/GID ownership, modes, mount behavior, WAL/SHM/journal state, and
   `O_NOATIME` capability on the real PVC;
3. namespace, PVC claim/access modes, CNI NetworkPolicy enforcement, admission
   policy, Kubernetes version/features, scheduler, and image-pull facts;
4. an approved non-placeholder image digest containing the matching verified
   commit plus reviewed SBOM/signature/provenance;
5. a live backup command, destination, retention policy, approval, and complete
   U8 DB/key restore rehearsal;
6. accepted shadow coverage/parity/divergence evidence over an agreed period;
7. explicit approvals and exact commands for apply activation, CronJob
   unsuspension, reader/loop cutover, and rollback.
