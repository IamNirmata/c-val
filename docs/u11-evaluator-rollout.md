# U11 evaluator rollout preparation

U11 remains **IN PROGRESS / BLOCKED for live criteria**. Final local
publication, rollback, backup-binding, cleanup, and lock-order hardening is
under recertification on 2026-07-31. This document covers local preparation only;
local certification is not live READY/closed status. It does not authorize Kubernetes access,
PVC reads or writes, database creation/migration, image publication, CronJob
activation, compatibility cutover, or deployment.

U12A changes only local image catalog assembly: `.dockerignore` admits the
complete `validation-tests/**` tree so future registrations and declared
support files cannot be omitted by a fixed built-in allowlist. The Dockerfile
copies that tree only into its builder stage, sets `PYTHONPATH=/workspace/c-val`
for the already installed `pip --target` package, strictly loads the copied
registry and every declared plugin API/config hook from a bounded
descriptor-anchored source snapshot, then publishes only each registered
descriptor, adapter, declared support file, setup, and entrypoint. Unregistered
and unrelated workload assets never reach the final stage. Destination
assembly uses no-follow lexical ancestors, same-parent staging, fsync, and
atomic no-replace publication. Assembly is offline and performs no runtime
discovery. A malformed descriptor or adapter, source/destination ancestor race,
or copy failure fails the image build and transactional cleanup leaves no
partial catalog. This closes no U11
live blocker and authorizes no image publication or deployment.

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
- `overlays/shadow/`: only the pre-existing
  `continuous_validation/evaluator_state` PVC subPath mounted read-only at the
  configured state root, no `--write-enabled`, and no `--apply`;
- `overlays/apply/`: only that subPath mounted read-write, explicit `--write-enabled`, and exact
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
OCI revision label, and copies only the installed tree, config, and the exact
registry-selected descriptor/plugin catalog into a distroless non-root UID/GID
65532 final image. The builder command is tested from outside the checkout
against a staged `pip --target` tree, including synthetic-registry success and
malformed-plugin failure.
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
  evaluator-preflight --state-root <local-copy-root> --access ro

# Deterministic, non-authoritative direction parity from copied inputs.
python -m cval.cli evaluator-parity \
  --u8-json <u8-labels.json> \
  --compatibility-db <copied-classification-results.db>

# Backup plan only. Source and destination must both be outside the configured
# live shared/state roots, and the destination parent must already exist.
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

The backup API takes the same descriptor-relative fixed-owner per-test lock used
by U7 ingestion and U9 apply/activation, and continuously revalidates it only
during backup apply. It rejects rollback-journal, WAL, and SHM sidecars immediately
around each immutable snapshot, revalidates source identity, preserves a U8 DB
and owner-only activation key as one unit, and compares source/destination
schema, logical table inventories, row identities, row counts, and content
digests. It never reports key bytes or a standalone key hash. Source DB/key
files must be current-UID, single-link regular files with exact safe modes.
Snapshot bytes and activation keys are read with `pread` from the retained file
descriptors; sidecars are inspected through the retained parent. A moved or
substituted parent therefore either fails before the read or reads only the
original inode and then rejects binding drift without touching the substitute.

The source and destination must not equal, contain, or be contained by configured
`runtime.validation_root` or configured `health_evaluator.state_root`; siblings
remain allowed.
That rejection occurs before destination reservation or evaluator-lock creation.
The destination must be outside both configured live roots, disjoint from the
source, traversal-free, and beneath a pre-existing current-UID safe-mode parent.
Every lexical ancestor is opened no-follow and its identity is retained from
destination validation through reservation. Apply reserves the final
destination and every persistent nested directory through a private same-parent
128-bit random staging name, immediate no-follow retained binding, and Linux
`renameat2(RENAME_NOREPLACE)` publication. It retains the parent, higher
ancestry, root, and every created nested directory identity, never overwrites it,
creates/writes/fsyncs files through retained descriptors, and removes a partial
destination only through identity-checked `dir_fd` operations. Racer
replacements are preserved, unpublished/published staged inodes are cleaned by
their exact retained identity on every `BaseException`, and the operation fails
closed. Evaluator U7 ancestry uses the same primitive; the shared-evidence job
supervisor remains deliberately separate. The threat model closes framework
concurrency and final-path races. It does not claim safety against an arbitrary
malicious same-UID process that can guess a cryptographically random private
staging name. After retained
descriptors close, success finalization freshly traverses from `/` and compares
every original ancestry identity before opening the exact destination root.
If that fresh check raises, post-close cleanup also reopens from `/` and removes
only a completely unchanged framework-created ancestry/root/tree with no
unknown entry; relocation, replacement, or ownership ambiguity preserves the
observed content and reports a chained cleanup failure. This behavior is part
of the current local recertification and is not a live-readiness claim. Dry-run
validates only and creates no
directory or lock. A separately approved live backup procedure is still
required.

The U7/U9/backup per-test lock is one persistent owner-only `0600` inode. Once
its canonical pathname has been exclusively created and identity-registered it
is never unlinked on timeout, failed acquisition, or normal release. Every
process opens the same no-follow pathname and revalidates pathname, retained
descriptor, owner, mode, link count, device, and inode while held. This avoids
the split-lock failure where one waiter removes an inode already locked by
another process and a third process creates a second lock inode. Malformed or
raced lock entries remain untouched and fail closed.

U8 production activation is internal to the evaluator. `_activate_candidate`
requires the held shared-lock guard plus retained U8 database and activation-key
bindings as mandatory arguments; there is no public path-only activation
function or production fallback. Isolated storage tests use an explicitly
internal test helper only.

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

Preflight requires the running effective UID/GID to exactly match
`state_owner_uid`/`state_owner_gid`. It descriptor-traverses foreign outer mount
ancestors without requiring their ownership or changing them, then requires the
pre-provisioned state root and every existing descendant to be no-symlink,
fixed-owner, exact `0700`; files are exact `0600` single-link regular files.
The compatibility key `validation_root_mode` means the exact state-root mode and
must remain `0700`. Read-only shadow does not require any writable directory;
apply additionally requires a writable mount. Captured owner/GID/mode/device/
inode identities remain descriptor-bound through DB/key reads and are
revalidated after them. Immutable snapshots receive the captured source inode,
so a transient substitute fails even if the original pathname is restored.
Replacement, movement, metadata drift, hard links, or symlink swaps fail
closed. Missing U8 is valid in shadow; U7 remains required. Preflight invokes the same U7 owner/receipt helper
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

### Bounded read-only fact audit (2026-07-31)

A bounded operator audit queried Kubernetes object metadata and used only
`lstat`, bounded directory-name inventory, and mount metadata in the existing
PVC access pod. It did not open a SQLite database, read an activation key or
file payload, run evaluator preflight, create a lock/path, apply an object, or
write the PVC.

Confirmed facts:

- the API server reports Kubernetes `v1.32.5+rke2r1`, namespace `gcr-admin` is
  active, and the running `gcr-admin-pvc-access` pod mounts the claim name
  `pvc-vast-gcr-admin` at `/data`;
- `/data/continuous_validation` is a shared NFSv4 mount path owned by UID/GID
  `0:0` with mode `0755`; this is expected shared-root state and must never be
  chmod/chown'ed by the evaluator;
- `/data/continuous_validation/validation_tests` is absent, so every enabled
  test's canonical U7 results DB, U8 health DB, and activation-key path is also
  absent; and
- the existing access pod uses its unrelated default ServiceAccount and does
  not establish the future evaluator UID/GID, tokenless, Restricted, or
  admission-policy facts.

The audit identity was not authorized to read the PVC object, StorageClass,
namespace NetworkPolicies, validating admission configuration, or evaluator
CronJob/ServiceAccount/NetworkPolicy objects. API-resource discovery alone does
not prove CNI or admission enforcement. Those facts therefore remain unknown,
not negative evidence.

Phase 0 cannot start from this state. The dedicated
`/data/continuous_validation/evaluator_state` subroot has not been approved or
provisioned. The current validation workload execution UID/GID is unspecified,
so U7 activation is blocked until a fixed UID/GID ingestion path is deployed;
enabling the gate before then fails closed. Creating/activating U7, provisioning
the state subroot, or reading live DB
contents are separate production-write/read approvals with backup and
coexistence review. No such action was performed.

U11 cannot be marked DONE until all of the following are independently verified
and approved:

1. canonical U7 availability and schema/receipt completeness for every enabled
   health test;
2. approved state-subroot provisioning plus fixed-UID/GID ingestion/evaluator
  ownership, modes, mount behavior, WAL/SHM/journal state, and
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
