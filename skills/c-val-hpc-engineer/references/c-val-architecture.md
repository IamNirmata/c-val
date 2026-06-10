# c-val Architecture Reference

## Current Package Surface

- `cval.cli`: operator and Hermes command surface.
- `cval.k8s.discovery`: read-only pod/node parsing and free GPU node discovery.
- `cval.storage.status`: read-only latest validation status through the PVC access pod.
- `cval.scheduler.priority`: stale and never-tested node prioritization.
- `cval.jobs.renderer`: Volcano validation job manifest rendering.
- `cval.orchestrator.workflow`: dry-run workflow planning.
- `cval.jobs.manager`: dry-run by default, policy-gated submission.
- `cval.jobs.monitor`: read-only Volcano job phase polling and timeout classification.
- `cval.validation.results`: structured result schema parsing.

## Flow

1. `status` reads latest validation history.
2. `discover-free-nodes` reads live GPU availability.
3. `plan` builds the priority queue and renders planned jobs.
4. `submit-plan` previews or explicitly submits the plan.
5. `monitor-jobs` reads phases until terminal or timeout.
6. In-pod scripts write structured JSON result artifacts.
7. `db-update.sh` ingests per-test results and metrics.

## Compatibility Notes

- `utils/functions.py` remains available for legacy DB updates and helper commands.
- `job-runner.ipynb` remains available but should migrate toward package calls.
- Existing Volcano YAML remains under `ymls/specific-node-job.yml`.