"""DL-owned inputs for all four raw metric categories."""

import ast

from evaluation_engine.runtime import EvidenceError
from evaluation_engine.config import digest
from pathlib import Path


SOURCES = {
    "numerical_correctness": "dl_numerical_db_path",
    "compute_performance": "dl_compute_db_path",
    "collective_performance": "dl_collective_db_path",
    "overlap_performance": "dl_overlap_db_path",
}


def layer_numerical_metrics(layer):
    try:
        expression = ast.parse(layer, mode="eval").body
    except (SyntaxError, TypeError) as error:
        raise EvidenceError("invalid_dl_layer_contract") from error
    if not isinstance(expression, ast.Call) or not isinstance(expression.func, ast.Attribute) or not isinstance(expression.func.value, ast.Name) or expression.func.value.id != "nn":
        raise EvidenceError("unsupported_dl_layer_contract")
    if any(isinstance(argument, ast.Starred) for argument in expression.args) or any(keyword.arg is None for keyword in expression.keywords):
        raise EvidenceError("unsupported_dl_layer_arguments")
    keywords = {keyword.arg: keyword.value for keyword in expression.keywords}
    if len(keywords) != len(expression.keywords):
        raise EvidenceError("duplicate_dl_layer_argument")

    def argument(name, position, default):
        if name in keywords and len(expression.args) > position:
            raise EvidenceError("duplicate_dl_layer_argument")
        value = keywords.get(name, expression.args[position] if len(expression.args) > position else None)
        try:
            result = default if value is None else ast.literal_eval(value)
        except (ValueError, TypeError) as error:
            raise EvidenceError("unsupported_dl_layer_arguments") from error
        if type(result) is not type(default):
            raise EvidenceError("invalid_dl_layer_argument_type")
        return result

    name = expression.func.attr
    parameters = set()
    if name in {"Linear", "Bilinear", "Conv1d", "Conv2d", "Conv3d", "ConvTranspose1d", "ConvTranspose2d", "ConvTranspose3d"}:
        parameters.add("weight")
        position = 2 if name == "Linear" else 3 if name == "Bilinear" else 7
        if argument("bias", position, True):
            parameters.add("bias")
    elif name in {"GroupNorm", "LayerNorm", "RMSNorm", "BatchNorm1d", "BatchNorm2d", "BatchNorm3d", "InstanceNorm1d", "InstanceNorm2d", "InstanceNorm3d"}:
        elementwise = name in {"LayerNorm", "RMSNorm"}
        if argument("elementwise_affine" if elementwise else "affine", 2 if elementwise else 3, not name.startswith("InstanceNorm")):
            parameters.add("weight")
            if name != "RMSNorm" and (name != "LayerNorm" or argument("bias", 3, True)):
                parameters.add("bias")
    elif name in {"Embedding", "EmbeddingBag"}:
        parameters.add("weight")
    elif name in {"GRU", "LSTM"}:
        layers = argument("num_layers", 2, 1)
        if not 1 <= layers <= 64:
            raise EvidenceError("unsupported_dl_recurrent_layer_count")
        bias = argument("bias", 3, True)
        directions = ("", "_reverse") if argument("bidirectional", 6, False) else ("",)
        projection = argument("proj_size", 7, 0) if name == "LSTM" else 0
        for index in range(layers):
            for suffix in directions:
                parameters.update(f"{kind}_{side}_l{index}{suffix}" for kind in (("weight", "bias") if bias else ("weight",)) for side in ("ih", "hh"))
                if projection:
                    parameters.add(f"weight_hr_l{index}{suffix}")
    elif name != "LocalResponseNorm":
        raise EvidenceError("unsupported_dl_layer_contract:" + name)
    return parameters | {"norm_output"}


def required_task_metrics(plan, policy):
    if not isinstance(plan, dict):
        raise EvidenceError("invalid_dl_test_plan")
    groups = {}
    for group in ("nn_tasks", "f_tasks", "coll_tasks"):
        tasks = plan.get(group, [])
        if not isinstance(tasks, list) or any(not isinstance(task, dict) or not isinstance(task.get("name"), str) or not task["name"] for task in tasks):
            raise EvidenceError("invalid_dl_plan_tasks")
        if len({task["name"] for task in tasks}) != len(tasks):
            raise EvidenceError("duplicate_dl_plan_task")
        if any(type(task.get("overlap", False)) is not bool for task in tasks):
            raise EvidenceError("invalid_dl_overlap_selection")
        groups[group] = tasks
    required = set()

    def add(component, group, task, metrics):
        selected = set(metrics) & set(policy[component])
        if not selected:
            raise EvidenceError("dl_task_has_no_selected_metrics")
        for metric in selected:
            identity = component, group, task, metric
            if identity in required:
                raise EvidenceError("duplicate_dl_required_metric")
            required.add(identity)

    for group, tasks in groups.items():
        for task in tasks:
            numerical = layer_numerical_metrics(task.get("layer")) if group == "nn_tasks" else {"norm_output", "weight"} if group == "f_tasks" else {"norm_output"}
            add("numerical_correctness", group, task["name"], numerical)
            if group == "coll_tasks":
                add("collective_performance", group, task["name"], {"cpu_time", "gpu_time"})
            else:
                add("compute_performance", group, task["name"], {"fp_cpu_time", "fp_gpu_time", "bp_cpu_time", "bp_gpu_time"})
    for collective in groups["coll_tasks"]:
        for layer in groups["nn_tasks"]:
            if collective.get("overlap", False) and layer.get("overlap", False):
                add("overlap_performance", "overlap_tasks", collective["name"] + "+" + layer["name"], {"coll_mean", "coll_stdev", "layer_mean", "layer_stdev"})
    if not required:
        raise EvidenceError("empty_dl_required_metrics")
    return required


def read(context, run, test):
    before = context.dl_generation()
    root = Path(context.settings.raw.runtime.validation_root) / "validation_tests/dltest/runs" / run["node"] / f"{run['node']}-{run['timestamp']}"
    summary = context.json_file(root / "summary.json")
    plan_name, iterations = summary.get("test_plan"), summary.get("iterations")
    if not isinstance(plan_name, str) or Path(plan_name).name != plan_name or plan_name in {"", ".", ".."} or type(iterations) is not int or iterations <= 0:
        raise EvidenceError("missing_dl_plan_provenance")
    plan = context.json_file(root / "artifacts/workdir/test_plans" / plan_name / "test_plan.json")
    required = required_task_metrics(plan, test.settings["metrics"])
    if len(required) * test.settings["expected_ranks"] > context.settings.max_metric_rows_per_run * len(SOURCES):
        raise EvidenceError("dl_required_metric_limit_exceeded")
    expected = {
        context.observation(component, plan_name, group, task, metric, rank, None, "reference" if component == "numerical_correctness" else "lower")["key"]
        for component, group, task, metric in required for rank in range(test.settings["expected_ranks"])
    }
    observations = []
    metadata = {"plan_digests": {plan_name: digest(plan)}, "required_metrics_digest": digest(sorted(expected))}
    for component, storage_field in SOURCES.items():
        direction = "reference" if component == "numerical_correctness" else "lower"
        directions = {name: direction for name in test.settings["metrics"][component]}
        rows, plans = context.read_dl(run, component, storage_field, directions, test.settings["expected_ranks"])
        if plans != [(plan_name, iterations)]:
            raise EvidenceError("dl_metric_plan_provenance_mismatch")
        observations.extend(rows)
        metadata[component] = plans
    if {item["key"] for item in observations} != expected:
        raise EvidenceError("dl_required_metric_coverage_mismatch")
    if context.dl_generation() != before:
        raise EvidenceError("dl_generation_changed_during_read")
    return observations, metadata


def build(context, test, runs):
    return context.build_baselines(test, runs)


if __name__ == "__main__":
    from evaluation_engine.__main__ import main
    raise SystemExit(main("dltest", "baseline"))