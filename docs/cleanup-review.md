# Framework Simplification Review

Review date: 2026-09-22. Baseline:
`7464e4fc55ec88691d015afdb3ec08ffa246f6c1`.

**Batches A and B were approved and removed on 2026-09-22.** Batches C-D remain
pending separate approval; E is consolidation guidance. **Planning finding 1
and ingestion finding 2 were approved and implemented locally on 2026-09-23.** No commit, push or
deployment was performed. Absence from the continuous loop alone is not proof
that a file is unused.

The 2026-09-23 settings cleanup also removes single-choice format switches,
legacy TSV, optional validation ZIP export and duplicate public CLI overrides
for config-owned targets, job settings and policy limits. The
[remaining control inventory](components_design/cli-design.md#mode-inventory)
records replacements. TOML settings, raw writers, canonical evidence and the
independent evaluator are retained; the live loop has not been restarted.

## Maintainability Re-audit

The A-B removals reduce stale material, not active framework complexity. The
next goal is fewer places to understand or change one behavior, not merely fewer
files. Findings 1-2 are implemented locally; findings 3-5 remain proposals, not
approved runtime refactors or deletions.
The earlier retain inventory protected responsibilities and evidence; it did
not mean every current implementation or abstraction should remain unchanged.

### 1. Separate Candidate Selection From Job Rendering

**Implemented locally; cluster acceptance pending.** Previously the loop rendered
every due candidate before selecting an available node, then rendered its Job
again at submission. Preflight repeated that rendering as well.

Plain `plan` now returns ordered candidates with an exact commit and counts,
using the existing pure priority function. Both
[live-loop](../scripts/cval-live.sh) inspection paths use this single default;
only confirmed submission invokes the [Job planner](../cval/orchestrator/workflow.py).
The unshipped `--candidates-only` flag and old `--include-yaml` mode are removed,
along with rendering-only plan arguments and the unused rendered-plan serializer.
No internal render toggle, new command, scheduler or configuration was added.

Local regressions verify zero renders during candidate inspection, identical
priority order, one render for selected-node submission, and failure before
creation for invalid templates. Existing preflight, fresh eligibility, cooldown,
resume, pruning and worktree/source guards remain. Malformed candidate JSON and
a returned commit differing from the requested commit fail closed. See the
[CLI contract](components_design/cli-design.md) for the default candidate JSON
shape and remaining mode inventory. Callers must migrate from `planned_jobs`
to `candidates`; this is an intentional planning-interface change.

### 2. Give Ingestion One Orchestration Path

**Implemented locally; cluster acceptance pending.** The former 537-line
[db-update.sh](../validation-tests/db-update.sh) is now a six-line launcher for
[one Python coordinator](../cval/storage/run_ingest.py). JSON-to-shell projection,
argument reconstruction and separate writer CLI processes are gone. The existing
raw writers and schemas are unchanged; no generic DB or new plugin layer was added.

The coordinator authorizes the result/config snapshot, binds the immutable
digest, and preserves storage -> NCCL -> locked DL -> atomic status ordering.
The provenance owner now owns digest binding; each low-level writer still
revalidates its authorization. The existing POSIX lock helper exposes its secure
acquisition checks for direct use, while its command wrapper retains child
cleanup behavior. Events and SQLite retries use the existing implementations.

Local tests verify seven actual temporary stores, unchanged receipts and rows
after a lock retry, suppressed status on missing evidence, failure/interruption
events, released locks and descriptor-bound supervisor ingestion. Historical
structured v1/v2 readers remain; environment-only results cannot authorize writes.
Individual CLI writer/rebuild hooks remain compatibility/maintenance interfaces,
not part of normal orchestration. No live job or raw evidence was modified.

### 3. Maintain One Job Bootstrap Program

**Medium impact, bounded change.** The pinned and scheduler-selected Job
templates contain the same 34 nonblank, noncomment bootstrap lines. A source
transport or supervisor-startup change must currently be maintained twice;
[renderer validation](../cval/jobs/renderer.py#L213) also depends on their shape.

Keep both requested YAML entry points and their placement differences. Have the
renderer insert one reviewed bootstrap payload into both, without introducing a
template engine or weakening semantic/duplicate-key validation. Rendered output
must remain self-contained: a bootstrap cannot rely on source not fetched yet.

Gate: [placement and bootstrap parity](../tests/test_renderer.py#L34), exact
commit/hash rejection, shell syntax, and the existing hostile-template tests.
This is consolidation of shared code, not removal of scheduler-selected tests.

### 4. Share DL Transaction Bookkeeping

**Medium impact, narrow implementation.**
[write_standard_db](../cval/storage/dltest_ingest.py#L738) and
[write_overlap_db](../cval/storage/dltest_ingest.py#L804) occupy 64 and 67 lines.
They repeat receipt setup, scoped replacement, insertion, generation writes and
commit handling; their columns and indexes genuinely differ.

Extract only common transaction mechanics into a private helper, leaving fixed
schemas/columns explicit. Keep the existing databases and public writer APIs;
do not replace them with a configurable database framework or combine the four
stores. Moving historical rebuild code to another file alone is not a reduction.

Gate: unchanged rows, schemas, receipts and idempotency;
[generation stays incomplete until finalization](../tests/test_dltest_ingest.py#L1089)
and divergent receipts still block publication. Retain symlink/identity checks.

### 5. Give Current Behavior One Documentation Home

**Low runtime risk, direct readability benefit.** The
[evaluator design note](components_design/evaluation-engine-design.md#L55)
still places a future Deployment/block-PVC/WAL design below the implemented
fixed-Pod/shared-storage/DELETE description. It is labeled as a proposal, but
readers must continually separate the two. Do not treat its target as deployed.

Use [architecture](architecture.md) for the runtime map,
[configuration](configuration.md) for settings, [operations](operations-runbook.md)
for procedures, and [result schema](result-schema.md) for evidence contracts.
Keep evaluator operations in its [deployment README](../deploy/evaluation-engine/README.md).
Shorten design notes to rationale and explicitly deferred decisions; link to
current contracts instead of repeating them. Keep research/roadmap separate.
This review itself is consolidated rather than adding another design document.

Gate: link/reference checks and comparison with effective code/config. Do not
refresh dated operating claims without fresh evidence. This is the previous
batch E guidance, not approval to delete current reference documentation.

## Simple Ownership Model

Keep the existing responsibilities; simplify the hand-offs, not the safety model:

| Owner | Responsibility |
| --- | --- |
| Live controller and existing scheduler/job APIs | Exact source, candidate priority, fresh eligibility, bounded slots, submission/resume state |
| Supervisor and registered test runner | Descriptor-owned paths, test setup/execution, deadlines, canonical results and events |
| Test-owned raw writers | Validated metrics first, final status last, unchanged raw schemas and history |
| Independent consumers and maintenance | Read-only audit/export, separate evaluator, backup and recovery outside the per-cycle path |

No new orchestration framework, generic persistence layer, or wholesale directory
reorganization is proposed. A refactor should remove a duplicate implementation
or conversion path, not just rename, relocate or compress it.

## File Cleanup Status

| Batch | Status | Baseline reduction |
| --- | --- | --- |
| A: retired evaluation prototypes | Approved and removed locally | 3 files, 554 text lines, 23,924 bytes |
| B: historical planning bundle | Approved and removed locally | 7 files, 427 text lines plus PNG, 147,067 bytes |
| C: all-files publish wrapper | Pending approval | 1 file, 61 lines, 1,422 bytes |
| D: shell compatibility aliases | Deferred unless explicitly retired | 4 files, only 26 lines total |

A-B total: 10 files, 981 text lines plus one PNG, 170,991 bytes. Gross source
removals exclude documentation edits. Exact removed paths remain recoverable at
the baseline commit; no duplicate archive was created:

```text
docs/evals/README.md
docs/evals/classify.py
docs/evals/threshold.py
docs/project_managemnt_docs/README.md
docs/project_managemnt_docs/cval-components.md
docs/project_managemnt_docs/cval-versions.md
docs/project_managemnt_docs/cval2.0-audited.md
docs/project_managemnt_docs/cval2.0-initial-scope.md
docs/project_managemnt_docs/cval3.0.md
docs/project_managemnt_docs/image.png
```

C is [push.sh](../push.sh): it stages everything and pushes the current branch
to origin, not an isolated reviewed change to an explicitly chosen target.
Retirement must also update the workspace shipping-skill warning; do not create
another wrapper solely to replace it. C is useful safety cleanup, not the main
runtime simplification.

D's exact paths remain:

- [validation-tests/run-test.sh](../validation-tests/run-test.sh)
- [validation-tests/storage/storage.sh](../validation-tests/storage/storage.sh)
- [validation-tests/nccl/run-nccl-allreduce.sh](../validation-tests/nccl/run-nccl-allreduce.sh)
- [validation-tests/dltest/dltest.sh](../validation-tests/dltest/dltest.sh)

These are tested operator interfaces, not unreferenced code. The supervisor
already bypasses the top-level alias, and descriptors select canonical per-test
runners. Removal needs an external-usage decision and retained behavior coverage.

## Keep These Guarantees

- Retain exact-commit/mutation gates, cooldown/resume records, bounded commands,
	descriptor/path/symlink protections, immutable result/config checks, locking,
	status-last publication, and historical result/path readers. Reuse their code
	where justified; do not remove checks solely to lower line counts.
- Keep registered tests/plugins, their regression suites, backup/layout/journal
	recovery, and the supported evaluator. Their independent entry points or
	dynamic loading do not make them dead files. Splitting services/packages is
	a separate product decision, not necessary to obtain the reductions above.
- Protect raw DBs, PVC contents, backups, run logs, active source/runner worktrees,
	governance and user-owned untracked files. Conversation archives and the
	protected `dl_unit_test_evaluation` directory were not read.

## Verification and Next Approval

This re-audit uses current code, direct call sites, descriptors and neighboring
tests. Logical line counts include a final unterminated line; they identify
review surfaces, not promised deletion amounts.
Static references do not prove external non-use. This is not a comprehensive
security review or a fresh live-state audit. No live commands or additional
source deletions were performed for the re-audit.

Findings 1-2 have local regression coverage for planning, ingestion and evidence
safety. Bootstrap and DL transaction consolidation remain separate proposals;
their baseline tests do not mean those refactors are implemented. The active
loop has not been restarted with the local changes.

Next: publish an isolated exact commit, then perform separately approved one-node
acceptance before runtime rollout. Keep public APIs and evidence formats stable unless
explicitly approved otherwise. Source cleanup never authorizes live deletion or
evidence removal.