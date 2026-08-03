# c-val Architecture Reference

## Current Package Surface

- `cval.cli`: operator and Hermes command surface.
- `cval.k8s.discovery`: read-only pod/node parsing and free GPU node discovery.
- `cval.storage.status`: read-only latest validation status through the PVC access pod.
- `cval.scheduler.priority`: stale and never-tested node prioritization.
- `cval.jobs.renderer`: Volcano validation job manifest rendering.
- `cval.orchestrator.workflow`: read-only queue planning.
- `cval.jobs.manager`: policy-gated real cluster submission.
- `cval.jobs.monitor`: read-only Volcano job phase polling and timeout classification.
- `cval.validation.results`: structured result schema parsing.
- `cval.storage.ingest`: package-native SQLite writes from validation pods.

## Flow

1. `status` reads latest validation history.
2. `nodes` reads live GPU availability.
3. `plan` inspects the priority queue and rendered jobs.
4. `validate --git-ref <exact-commit> --submit --confirm submit` runs targeted
	development validation; `run --submit --confirm submit` operates batches.
5. `jobs --watch` reads phases until terminal or timeout.
6. In-pod scripts write structured JSON result artifacts.
7. `db-update.sh` ingests per-test results and metrics.

## Repository Notes

- The repository checkout is `c-val`; the importable Python package is `cval` because Python imports cannot contain hyphens.
- Legacy notebook and `utils/functions.py` helper paths are removed from the active tree.
- Existing Volcano YAML remains under `ymls/specific-node-job.yml`.