# c-val Improvement Plan

Review: 2026-09-23, source `fe4d405`. Proposed order; implement one item at a time.
Keep raw history, test-owned DBs, separate evaluation and explicit mutation gates.

1. [ ] **P0 | Configuration and discovery.**
   [Bulk discovery](../cval/k8s/discovery.py) reloads defaults and omits explicit
   readiness. Pass one effective config; derive eligibility from structured data.
   **Done:** custom resources/tolerations apply; missing/false readiness blocks.

2. [ ] **P0 | Submission.**
   Recheck eligibility at the [create boundary](../cval/jobs/manager.py) for every
   caller. Retain partial-batch receipts; reconcile uncertain creates before retry.
   **Done:** busy nodes block; second-create failure preserves the first receipt.

3. [ ] **P0 | Results and acceptance.**
   Bind [exports](../cval/storage/results_export.py) to exact runs or show metric age.
   Make [acceptance](../cval/orchestrator/validate.py) require canonical results and
   metric receipts. **Done:** stale/missing evidence cannot appear current or pass.

4. [ ] **P1 | Live controller and monitoring.**
   Merge duplicated resume/active observation in [the loop](../scripts/cval-live.sh)
   into existing Python orchestration; batch reads and share deadlines.
   **Done:** unchanged cooldown/resume/prune behavior, distinct forbidden/unavailable/missing
   states, fewer subprocesses measured per cycle.

5. [ ] **P1 | Runner and registry.**
   Consolidate [dual evidence-setup paths](../cval/validation/runner.py) around
   supervisor reservation and one compatibility-environment mapping.
   **Done:** dynamic tests, signal handling, historical readers, duplicate-run
   refusal and write-boundary/path protections remain intact.

6. [ ] **P1 | Raw ingestion.**
   Finish the [deferred DL transaction cleanup](cleanup-review.md#4-share-dl-transaction-bookkeeping):
   share receipt/transaction mechanics, keeping fixed schemas explicit.
   **Done:** unchanged seven-store rows, receipts, generations, locking and
   status-last publication under retry/failure injection; no generic per-test DB.

7. [ ] **P1 | Test workloads.**
   Preinstall FIO instead of [installing during GPU allocation](../validation-tests/storage/setup.sh).
   Pin image digest and [external DL source/plan](../validation-tests/dltest/run-test.sh).
   **Done:** changed dependencies change recorded identity; startup cost measured.

8. [ ] **P1 | Evaluator.**
   Improve [history pagination and repeated reads](../evaluation_engine/runtime.py):
   indexed incremental selection, generation-aware per-pass reuse, bounded replay.
   **Done:** fewer reads, identical results, late-arrival recovery, policy
   invalidation and unchanged raw databases.

9. [ ] **P2 | CLI compatibility.**
   Check external use before retiring [writer hooks or aliases](components_design/cli-design.md).
   **Done:** approved migration notes and behavior tests; inspection stays separate
   from mutation. Cosmetic file splitting is not simplification.

10. [ ] **P2 | Recovery tools.**
    Extend [existing recovery tests](../tests/test_pvc_layout.py) through journal
    migration and restore. Keep backup/layout/journal tools outside the controller.
    **Done:** restored rows/artifacts/metadata verified; failed preconditions preserve evidence.

11. [ ] **P2 | Delivery.**
    Separate runtime and host-test contents in the [source bundle](../deploy/evaluation-engine/render.py).
    Retain source-checkout installation, bootstrap, descriptors and required helpers.
    **Done:** smaller bundle, offline startup, exact commit/hash verification,
    preserved rollback bundles.

12. [ ] **P2 | Tests and CI.**
    Gate CI on offline unittest, shell syntax and documentation checks; reuse
    table-driven fixtures. **Done:** reproducible checks without duplicated fake
    scripts; separately approved exact-commit cluster acceptance remains required.

13. [ ] **P2 | Documentation.**
    Enforce the [existing ownership map](architecture.md#documentation-map).
    Replace repeated current contracts in overview/progress with links; retain
    dated research/history. **Done:** one home per contract, checked CLI/config
    examples, this plan as the active improvement backlog.
