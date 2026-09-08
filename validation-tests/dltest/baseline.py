"""DL-owned inputs for all four raw metric categories."""

from evaluation_engine.runtime import EvidenceError
from evaluation_engine.config import digest
from pathlib import Path


SOURCES = {
    "numerical_correctness": "dl_numerical_db_path",
    "compute_performance": "dl_compute_db_path",
    "collective_performance": "dl_collective_db_path",
    "overlap_performance": "dl_overlap_db_path",
}


def read(context, run, test):
    before = context.dl_generation()
    observations = []
    metadata = {}
    for component, storage_field in SOURCES.items():
        direction = "reference" if component == "numerical_correctness" else "lower"
        directions = {name: direction for name in test.settings["metrics"][component]}
        rows, plans = context.read_dl(run, component, storage_field, directions, test.settings["expected_ranks"])
        observations.extend(rows)
        metadata[component] = plans
    plans = {plan for component_plans in metadata.values() for plan, _iterations in component_plans}
    plan_digests = {}
    for plan in plans:
        if Path(plan).name != plan or plan in {".", ".."}:
            raise EvidenceError("invalid_test_plan_name")
        path = Path(context.settings.raw.runtime.validation_root) / "validation_tests/dltest/runs" / run["node"] / f"{run['node']}-{run['timestamp']}" / "artifacts/workdir/test_plans" / plan / "test_plan.json"
        plan_digests[plan] = digest(context.json_file(path))
    metadata["plan_digests"] = plan_digests
    if context.dl_generation() != before:
        raise EvidenceError("dl_generation_changed_during_read")
    return observations, metadata


def build(context, test, runs):
    return context.build_baselines(test, runs)


if __name__ == "__main__":
    from evaluation_engine.__main__ import main
    raise SystemExit(main("dltest", "baseline"))