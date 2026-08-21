"""Command-line interface for c-val.

This file is the main human/Hermes entry point. It exposes read-only status and
discovery commands, nonmutating queue inspection, approval-gated cluster
validation, read-only monitoring, and structured result inspection. Handlers are intentionally thin:
they parse arguments, call package modules, and format output.

Public commands: config, tests, nodes, validate, status, plan, run, jobs, result,
results, classifications, baseline, nccl-eval, and overview.
The db-add-* commands are in-pod ingestion hooks and stay out of --help.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

from cval.config import (
    CvalConfig,
    REPO_ROOT,
    config_to_dict,
    encode_config_snapshot,
    is_exact_commit,
    load_config,
    load_config_snapshot,
)
from cval.jobs.manager import submission_result_to_dict, submit_workflow_plan
from cval.jobs.monitor import get_job_phases, monitored_jobs_to_dict, monitor_jobs_until_terminal
from cval.k8s.discovery import (
    describe_node,
    discover_free_nodes,
    discover_gpu_node_names,
    fully_free_node_names,
)
from cval.policy import ExecutionPolicy, PolicyViolation
from cval.storage.status import (
    get_latest_status_rows,
    latest_status_rows_to_node_map,
    latest_status_rows_to_tsv,
    parse_latest_status_tsv,
)
from cval.storage.ingest import (
    add_nccl_health_from_summary,
    add_storage_result,
    add_validation_run_results,
    add_validation_result,
    parse_timestamp,
)
from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    validation_result_to_env,
    validation_result_to_env_lines,
)
from cval.validation.operational_targets import (
    BASELINE_ACTIVATE,
    BASELINE_BUILD,
    BASELINE_CLASSIFY,
    BASELINE_LIST,
    BASELINE_SHOW,
    CLASSIFICATIONS_EXPORT,
    OPERATION_ORDER,
    RESULTS_EXPORT,
    build_operational_target_catalog,
)


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments, dispatch to a handler, and return a process code."""

    raw_argv = sys.argv[1:] if argv is None else argv
    try:
        _preflight_nccl_mutation_gate(raw_argv)
    except PolicyViolation as exc:
        print(f"Policy violation: {exc}", file=sys.stderr)
        return 2
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", type=Path)
    config_args, _ = bootstrap.parse_known_args(raw_argv)
    try:
        snapshot = os.environ.get("CVAL_CONFIG_SNAPSHOT_B64")
        runtime_repo_root = os.environ.get("CVAL_TEST_REPO_ROOT") or os.environ.get(
            "CVAL_REPO_DIR"
        )
        descriptor_only = _tests_descriptor_only_command(raw_argv)
        config = (
            load_config(
                config_args.config,
                validate_plugins=not descriptor_only,
            )
            if config_args.config is not None or not snapshot
            else load_config_snapshot(
                snapshot,
                repo_root=Path(runtime_repo_root) if runtime_repo_root else None,
            )
        )
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    parser = build_parser(config)
    args = parser.parse_args(raw_argv)
    args.cval_config = config
    try:
        return args.handler(args)
    except BrokenPipeError:
        # Make `cval ... | head` behave like a normal Unix CLI instead of tracing.
        try:
            sys.stdout.close()
        except OSError:
            pass
        return 0
    except PolicyViolation as exc:
        # Policy violations are expected operator errors, not Python stack traces.
        print(f"Policy violation: {exc}", file=sys.stderr)
        return 2


def _tests_descriptor_only_command(argv: list[str]) -> bool:
    """Keep tests list/describe descriptor-only and free of adapter imports."""

    try:
        index = argv.index("tests")
    except ValueError:
        return False
    return index + 1 < len(argv) and argv[index + 1] in {"list", "describe"}


def _preflight_nccl_mutation_gate(argv: list[str]) -> None:
    """Reject malformed NCCL apply confirmation before config/plugin loading."""

    try:
        index = argv.index("nccl-eval")
    except ValueError:
        return
    if index + 1 >= len(argv) or "--apply" not in argv[index + 1 :]:
        return
    command = argv[index + 1]
    expected = {
        "schema": "schema",
        "grant-runtime": "grant-runtime",
        "ingest": "ingest",
        "emit-outbox": "emit-outbox",
        "commit-outbox": "commit-outbox",
        "ingest-outbox": "ingest-outbox",
        "calibration": "calibration",
        "build-baselines": "build-baselines",
        "evaluate": "evaluate",
        "worker": "worker",
        "recover": "recover",
    }
    if command not in expected:
        return
    confirmation = None
    for position, value in enumerate(argv[index + 2 :], start=index + 2):
        if value == "--confirm" and position + 1 < len(argv):
            confirmation = argv[position + 1]
            break
        if value.startswith("--confirm="):
            confirmation = value.split("=", 1)[1]
            break
    if confirmation != expected[command]:
        raise PolicyViolation(
            f"NCCL mutation requires --apply --confirm {expected[command]}"
        )


def build_parser(config: CvalConfig | None = None) -> argparse.ArgumentParser:
    """Build the top-level parser and subcommands."""

    active_config = config or load_config()
    target_catalog = build_operational_target_catalog(active_config.tests.registry)
    from cval.baselines.storage import validate_default_baseline_db_paths

    validate_default_baseline_db_paths(active_config)
    parser = argparse.ArgumentParser(prog="cval", description="c-val orchestration CLI")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to c-val TOML config; defaults to config/cval.toml or CVAL_CONFIG",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar=(
            "{config,tests,nodes,validate,status,plan,run,jobs,result,results,"
            "classifications,baseline,nccl-eval,overview}"
        ),
    )

    show_config = subparsers.add_parser("config", help="Print the effective c-val config")
    show_config.add_argument("--output", choices=["json"], default="json")
    show_config.set_defaults(handler=handle_config)

    tests_command = subparsers.add_parser(
        "tests", help="Inspect and validate the registered validation tests"
    )
    tests_sub = tests_command.add_subparsers(dest="tests_command", required=True)

    tests_list = tests_sub.add_parser("list", help="List registered validation tests")
    tests_list.add_argument(
        "--enabled-only", action="store_true", help="Show only enabled tests"
    )
    tests_list.add_argument("--output", choices=["table", "json"], default="table")
    tests_list.set_defaults(handler=handle_tests_list)

    tests_describe = tests_sub.add_parser(
        "describe", help="Show one effective test descriptor"
    )
    tests_describe.add_argument("test_id")
    tests_describe.add_argument("--output", choices=["table", "json"], default="json")
    tests_describe.set_defaults(handler=handle_tests_describe)

    tests_validate = tests_sub.add_parser(
        "validate", help="Validate all registered test descriptors and shared resources"
    )
    tests_validate.add_argument("--output", choices=["table", "json"], default="table")
    tests_validate.set_defaults(handler=handle_tests_validate)

    tests_scaffold = tests_sub.add_parser(
        "scaffold",
        help="Inspect or create a disabled pass/fail-only test scaffold",
    )
    tests_scaffold.add_argument("test_id")
    tests_scaffold.add_argument("--order", type=int, required=True)
    tests_scaffold.add_argument(
        "--apply", action="store_true", help="Create files after the exact confirmation"
    )
    tests_scaffold.add_argument("--confirm")
    tests_scaffold.add_argument("--output", choices=["table", "json"], default="table")
    tests_scaffold.set_defaults(handler=handle_tests_scaffold)

    # Machine-only catalog used by background loops. It is intentionally
    # omitted from public help and emits data, never shell assignments.
    operational_targets = subparsers.add_parser("operational-targets")
    operational_targets.add_argument("--operation", choices=OPERATION_ORDER, required=True)
    operational_targets.add_argument("--output", choices=["tsv", "json"], default="tsv")
    operational_targets.set_defaults(handler=handle_operational_targets)

    nodes = subparsers.add_parser("nodes", help="List schedulable GPU nodes and free capacity")
    nodes.add_argument(
        "--node-filter",
        default=active_config.cluster.node_filter,
        help="Substring filter for GPU nodes",
    )
    nodes_mode = nodes.add_mutually_exclusive_group()
    nodes_mode.add_argument(
        "--inventory-only",
        action="store_true",
        help="List matching GPU node names without reading pods",
    )
    nodes_mode.add_argument(
        "--check-node",
        help="Read schedulability and resources for one prioritized node",
    )
    nodes.add_argument("--output", choices=["table", "json"], default="table")
    nodes.set_defaults(handler=handle_nodes)

    validate = subparsers.add_parser(
        "validate",
        help="Run a confirmed real-cluster validation for one node",
    )
    validate.add_argument("--node", required=True, help="Target node name to validate")
    validate.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    validate.add_argument(
        "--git-ref",
        required=True,
        help="Exact lowercase 40-hex commit published to the configured repository",
    )
    validate.add_argument(
        "--submit",
        action="store_true",
        help="Authorize creation of the single validation job",
    )
    validate.add_argument("--confirm")
    validate.add_argument(
        "--timestamp", type=int, help="Override the run timestamp (defaults to now)"
    )
    validate.add_argument(
        "--poll-interval", type=float, default=3.0, help="Live status poll seconds"
    )
    validate.add_argument(
        "--timeout-seconds",
        type=float,
        default=active_config.monitoring.timeout_seconds,
        help="Overall live-tracking timeout",
    )
    validate.add_argument(
        "--pending-timeout",
        type=float,
        default=600.0,
        help="Warn if the job is still Pending after N seconds (it stays queued)",
    )
    validate.add_argument(
        "--pvc-pod",
        default=active_config.cluster.pvc_access_pod,
        help="PVC access pod used to verify fresh raw status",
    )
    validate.add_argument(
        "--download",
        action="store_true",
        help="Save this run's logs, results, and raw report as a local zip",
    )
    validate.add_argument(
        "--download-dir",
        type=Path,
        default=Path.cwd(),
        help="Directory to write the artifact zip into (default: current directory)",
    )
    validate.add_argument("--output", choices=["table", "json"], default="table")
    validate.set_defaults(handler=handle_validate)

    status = subparsers.add_parser(
        "status",
        help="Read latest validation status from the PVC access pod",
    )
    status.add_argument("--pod", default=active_config.cluster.pvc_access_pod)
    status.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    status.add_argument("--db-path", default=active_config.storage.validation_db_path)
    status.add_argument("--output", choices=["table", "json", "tsv"], default="table")
    status.set_defaults(handler=handle_status)

    plan = subparsers.add_parser(
        "plan", help="Inspect the detailed queue and rendered job plan"
    )
    _add_plan_inputs(plan, active_config)
    plan.add_argument(
        "--include-yaml",
        action="store_true",
        help="Include rendered YAML in JSON output",
    )
    plan.add_argument("--output", choices=["table", "json"], default="table")
    plan.set_defaults(handler=handle_plan)

    run = subparsers.add_parser(
        "run",
        help="Submit a validation batch after exact confirmation",
    )
    _add_plan_inputs(run, active_config)
    run.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    run.add_argument("--allowed-namespace", action="append")
    run.add_argument("--max-batch-size", type=int, default=active_config.policy.max_batch_size)
    run.add_argument("--submit", action="store_true")
    run.add_argument("--confirm")
    run.add_argument("--output", choices=["table", "json"], default="table")
    run.set_defaults(handler=handle_run)

    jobs = subparsers.add_parser("jobs", help="Read or watch Volcano job phases")
    jobs.add_argument("--jobs", required=True, help="Comma-separated vcjob names")
    jobs.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    jobs.add_argument("--watch", action="store_true", help="Poll until terminal or timeout")
    jobs.add_argument(
        "--timeout-seconds",
        type=float,
        default=active_config.monitoring.timeout_seconds,
    )
    jobs.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=active_config.monitoring.poll_interval_seconds,
    )
    jobs.add_argument("--output", choices=["table", "json"], default="table")
    jobs.set_defaults(handler=handle_jobs)

    result = subparsers.add_parser("result", help="Inspect a structured validation result")
    result.add_argument("--result-json", type=Path, required=True)
    result.add_argument("--output", choices=["env", "json"], default="env")
    result.set_defaults(handler=handle_result)

    results = subparsers.add_parser(
        "results",
        help="Export latest per-node results for one test to a local file",
    )
    results.add_argument(
        "--test",
        choices=["overall", "all", *target_catalog.names_for(RESULTS_EXPORT)],
        required=True,
    )
    results.add_argument("--type", choices=["csv"], default="csv", dest="result_type")
    results.add_argument("--output-dir", type=Path, default=Path.cwd())
    results.add_argument("--pod", default=active_config.cluster.pvc_access_pod)
    results.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    results.add_argument("--db-path", default=active_config.storage.validation_db_path)
    results.add_argument(
        "--classification-db-path",
        help="Override one classification DB; defaults to per-target DBs",
    )
    results.add_argument(
        "--no-classification",
        action="store_true",
        help="Do not join latest baseline classification columns into the CSV",
    )
    results.add_argument(
        "--nccl-db-path",
        default=active_config.storage.nccl_db_path,
        help="Override path to test-nccl.db on the PVC pod",
    )
    results.add_argument(
        "--storage-db-path",
        default=active_config.storage.storage_db_path,
        help="Override path to test-storage.db on the PVC pod",
    )
    results.add_argument(
        "--no-metrics",
        action="store_true",
        help="Do not join NCCL/storage metric columns into the CSV",
    )
    results.set_defaults(handler=handle_results)

    classifications = subparsers.add_parser(
        "classifications",
        help="Export latest baseline classification verdicts to a local CSV",
    )
    classifications.add_argument(
        "--test",
        choices=["all", *target_catalog.names_for(CLASSIFICATIONS_EXPORT)],
        default="all",
    )
    classifications.add_argument("--type", choices=["csv"], default="csv", dest="result_type")
    classifications.add_argument("--output-dir", type=Path, default=Path.cwd())
    classifications.add_argument("--pod", default=active_config.cluster.pvc_access_pod)
    classifications.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    classifications.add_argument(
        "--db-path",
        default=None,
        help="Override one classification DB; defaults to per-target DBs",
    )
    classifications.set_defaults(handler=handle_classifications)

    # In-pod ingestion commands; added without `help` so they stay out of --help.
    db_add_result = subparsers.add_parser("db-add-result")
    db_add_result.add_argument("node")
    db_add_result.add_argument("test")
    db_add_result.add_argument("result", choices=["pass", "fail", "incomplete"])
    db_add_result.add_argument("timestamp")
    db_add_result.add_argument("--image-name", default="")
    db_add_result.add_argument("--pytorch-version", default="")
    db_add_result.add_argument("--cuda-version", default="")
    db_add_result.add_argument("--result-json", type=Path, required=True)
    db_add_result.add_argument("--result-digest", default="")
    db_add_result.add_argument("--db-path", default=active_config.storage.validation_db_path)
    db_add_result.set_defaults(handler=handle_db_add_result)

    db_add_run = subparsers.add_parser("db-add-run-results")
    db_add_run.add_argument("node")
    db_add_run.add_argument("timestamp")
    db_add_run.add_argument("--storage-result", required=True, choices=["pass", "fail", "incomplete"])
    db_add_run.add_argument("--nccl-result", required=True, choices=["pass", "fail", "incomplete"])
    db_add_run.add_argument("--dltest-result", required=True, choices=["pass", "fail", "incomplete"])
    db_add_run.add_argument("--overall-result", required=True, choices=["pass", "fail", "incomplete"])
    db_add_run.add_argument("--image-name", default="")
    db_add_run.add_argument("--pytorch-version", default="")
    db_add_run.add_argument("--cuda-version", default="")
    db_add_run.add_argument("--result-json", type=Path, required=True)
    db_add_run.add_argument("--result-digest", default="")
    db_add_run.add_argument("--db-path", default=active_config.storage.validation_db_path)
    db_add_run.set_defaults(handler=handle_db_add_run_results)

    db_preflight_compat = subparsers.add_parser("db-preflight-result")
    db_preflight_compat.add_argument("--result-json", type=Path, required=True)
    db_preflight_compat.add_argument("--result-digest", required=True)
    db_preflight_compat.set_defaults(handler=handle_db_preflight_result)

    db_add_storage = subparsers.add_parser("db-add-storage-result")
    db_add_storage.add_argument("node")
    db_add_storage.add_argument("timestamp")
    db_add_storage.add_argument("results_dir", type=Path)
    db_add_storage.add_argument("--image-name", default="")
    db_add_storage.add_argument("--run-id", default="")
    db_add_storage.add_argument("--immutable", action="store_true")
    db_add_storage.add_argument("--result-json", type=Path, required=True)
    db_add_storage.add_argument("--result-digest", default="")
    db_add_storage.add_argument("--db-path", default=active_config.storage.storage_db_path)
    db_add_storage.set_defaults(handler=handle_db_add_storage_result)

    db_add_nccl_health = subparsers.add_parser("db-add-nccl-health")
    db_add_nccl_health.add_argument("node")
    db_add_nccl_health.add_argument("timestamp")
    db_add_nccl_health.add_argument("summary_json", type=Path)
    db_add_nccl_health.add_argument("--iterations", type=int)
    db_add_nccl_health.add_argument("--image-name", default="")
    db_add_nccl_health.add_argument("--cuda-version", default="")
    db_add_nccl_health.add_argument("--pytorch-version", default="")
    db_add_nccl_health.add_argument("--run-id", default="")
    db_add_nccl_health.add_argument("--immutable", action="store_true")
    db_add_nccl_health.add_argument("--ibbw-log", type=Path)
    db_add_nccl_health.add_argument("--require-hca-samples", action="store_true")
    db_add_nccl_health.add_argument("--result-json", type=Path, required=True)
    db_add_nccl_health.add_argument("--result-digest", default="")
    db_add_nccl_health.add_argument("--db-path", default=active_config.storage.nccl_db_path)
    db_add_nccl_health.set_defaults(handler=handle_db_add_nccl_health)

    db_rebuild_dltest = subparsers.add_parser("db-rebuild-dltest-metrics")
    db_rebuild_dltest.add_argument(
        "--results-root",
        type=Path,
        default=Path(active_config.runtime.dl_results_root_path),
        help="Root containing canonical or legacy DL run directories with rank JSON",
    )
    db_rebuild_dltest.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Override directory for standard DL DB filenames; defaults to each "
            "exact configured storage.dl_*_db_path"
        ),
    )
    db_rebuild_dltest.add_argument("--output", choices=["table", "json"], default="table")
    db_rebuild_dltest.set_defaults(handler=handle_db_rebuild_dltest_metrics)

    # Baseline commands (read-only and ingestion)
    baseline = subparsers.add_parser("baseline", help="Manage baselines and peer comparison")
    baseline_sub = baseline.add_subparsers(dest="baseline_command", required=True)

    baseline_list = baseline_sub.add_parser("list", help="List stored baselines")
    baseline_list.add_argument(
        "--test-type", choices=target_catalog.names_for(BASELINE_LIST)
    )
    baseline_list.add_argument(
        "--db-path", help="Override baseline DB; defaults to baseline_root_path DBs"
    )
    baseline_list.add_argument("--output", choices=["table", "json"], default="table")
    baseline_list.set_defaults(handler=handle_baseline_list)

    baseline_build = baseline_sub.add_parser(
        "build", help="Build a dynamic baseline from result DBs (robust stats)"
    )
    baseline_build.add_argument(
        "--test-type", choices=target_catalog.names_for(BASELINE_BUILD), required=True
    )
    baseline_build.add_argument(
        "--window-days", type=int, default=active_config.baseline.window_days
    )
    baseline_build.add_argument(
        "--min-samples", type=int, default=active_config.baseline.min_samples
    )
    baseline_build.add_argument("--image-name", help="Stratify storage by image")
    baseline_build.add_argument("--node", help="Restrict storage to one node")
    baseline_build.add_argument("--test-plan", help="Stratify DL by test plan")
    baseline_build.add_argument("--source-db", help="Override source result DB")
    baseline_build.add_argument("--baseline-id", help="Explicit baseline id")
    baseline_build.add_argument(
        "--db-path",
        help="Override DB to store the baseline in; defaults to baseline_root_path DBs",
    )
    baseline_build.add_argument(
        "--store", action="store_true", help="Persist as a candidate baseline"
    )
    baseline_build.add_argument(
        "--activate", action="store_true", help="Persist and promote to active"
    )
    baseline_build.add_argument("--output", choices=["table", "json"], default="table")
    baseline_build.set_defaults(handler=handle_baseline_build)

    baseline_activate = baseline_sub.add_parser(
        "activate", help="Promote a stored baseline to active"
    )
    baseline_activate.add_argument("baseline_id")
    baseline_activate.add_argument(
        "test_type", choices=target_catalog.names_for(BASELINE_ACTIVATE)
    )
    baseline_activate.add_argument(
        "--db-path", help="Override baseline DB; defaults to baseline_root_path DBs"
    )
    baseline_activate.set_defaults(handler=handle_baseline_activate)

    baseline_show = baseline_sub.add_parser("show", help="Show a stored baseline record")
    baseline_show.add_argument("baseline_id")
    baseline_show.add_argument(
        "test_type", choices=target_catalog.names_for(BASELINE_SHOW)
    )
    baseline_show.add_argument(
        "--db-path", help="Override baseline DB; defaults to baseline_root_path DBs"
    )
    baseline_show.add_argument("--output", choices=["table", "json"], default="table")
    baseline_show.set_defaults(handler=handle_baseline_show)

    baseline_classify = baseline_sub.add_parser(
        "classify", help="Classify nodes against the active baseline"
    )
    baseline_classify.add_argument(
        "--test-type", choices=target_catalog.names_for(BASELINE_CLASSIFY), required=True
    )
    baseline_classify.add_argument(
        "--node", help="Classify one node; omit to classify all nodes in the window"
    )
    baseline_classify.add_argument(
        "--baseline-id",
        help="Baseline id to compare against; default is the active baseline",
    )
    baseline_classify.add_argument(
        "--window-days", type=int, default=active_config.baseline.window_days
    )
    baseline_classify.add_argument(
        "--source-db", help="Override source result DB"
    )
    baseline_classify.add_argument(
        "--db-path",
        help="Override baseline DB; defaults to baseline_root_path DBs",
    )
    baseline_classify.add_argument(
        "--store-results",
        action="store_true",
        help="Persist decisions to the target classification DB",
    )
    baseline_classify.add_argument(
        "--classification-db-path",
        help="Override the target classification DB path",
    )
    baseline_classify.add_argument("--output", choices=["table", "json"], default="table")
    baseline_classify.set_defaults(handler=handle_baseline_classify)

    nccl_eval = subparsers.add_parser(
        "nccl-eval",
        help="Operate the focused PostgreSQL NCCL evaluation subsystem",
    )
    nccl_sub = nccl_eval.add_subparsers(dest="nccl_eval_command", required=True)

    nccl_schema = nccl_sub.add_parser(
        "schema", help="Plan or apply packaged PostgreSQL schema migrations"
    )
    _add_nccl_mutation_gate(nccl_schema)
    nccl_schema.add_argument("--output", choices=["table", "json"], default="table")
    nccl_schema.set_defaults(handler=handle_nccl_eval_schema)

    nccl_grant_runtime = nccl_sub.add_parser(
        "grant-runtime", help="Provision the least-privilege PostgreSQL runtime role"
    )
    _add_nccl_mutation_gate(nccl_grant_runtime)
    nccl_grant_runtime.add_argument("--output", choices=["table", "json"], default="table")
    nccl_grant_runtime.set_defaults(handler=handle_nccl_eval_grant_runtime)

    nccl_ingest = nccl_sub.add_parser(
        "ingest", help="Validate or atomically ingest one normalized JSON batch"
    )
    nccl_ingest.add_argument("--input", type=Path, required=True)
    _add_nccl_mutation_gate(nccl_ingest)
    nccl_ingest.add_argument("--output", choices=["table", "json"], default="table")
    nccl_ingest.set_defaults(handler=handle_nccl_eval_ingest)

    nccl_emit_outbox = nccl_sub.add_parser(
        "emit-outbox",
        help="Validate or atomically emit one native NCCL PVC outbox file",
    )
    nccl_emit_outbox.add_argument("--result-json", type=Path, required=True)
    nccl_emit_outbox.add_argument("--summary", type=Path, required=True)
    nccl_emit_outbox.add_argument("--runtime-evidence", type=Path, required=True)
    nccl_emit_outbox.add_argument("--result-digest", required=True)
    nccl_emit_outbox.add_argument("--outbox-root", type=Path, required=True)
    _add_nccl_mutation_gate(nccl_emit_outbox)
    nccl_emit_outbox.add_argument(
        "--output", choices=["table", "json"], default="table"
    )
    nccl_emit_outbox.set_defaults(handler=handle_nccl_eval_emit_outbox)

    nccl_commit_outbox = nccl_sub.add_parser(
        "commit-outbox", help="Create the final retryable marker after SQLite commits"
    )
    nccl_commit_outbox.add_argument("--outbox-root", type=Path, required=True)
    nccl_commit_outbox.add_argument("--pending", type=Path, required=True)
    nccl_commit_outbox.add_argument("--result-digest", required=True)
    _add_nccl_mutation_gate(nccl_commit_outbox)
    nccl_commit_outbox.add_argument("--output", choices=["table", "json"], default="table")
    nccl_commit_outbox.set_defaults(handler=handle_nccl_eval_commit_outbox)

    nccl_ingest_outbox = nccl_sub.add_parser(
        "ingest-outbox",
        help="Scan or durably ingest immutable NCCL PVC outbox files",
    )
    nccl_ingest_outbox.add_argument("--outbox-root", type=Path, required=True)
    nccl_ingest_outbox.add_argument("--limit", type=int, default=100)
    nccl_ingest_outbox.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue after a per-file database error; fail closed by default",
    )
    _add_nccl_mutation_gate(nccl_ingest_outbox)
    nccl_ingest_outbox.add_argument(
        "--output", choices=["table", "json"], default="table"
    )
    nccl_ingest_outbox.set_defaults(handler=handle_nccl_eval_ingest_outbox)

    nccl_calibration = nccl_sub.add_parser(
        "calibration", help="Plan, append, or inspect immutable calibration decisions"
    )
    calibration_sub = nccl_calibration.add_subparsers(
        dest="nccl_calibration_command", required=True
    )
    calibration_plan = calibration_sub.add_parser("plan")
    calibration_plan.add_argument("--input", type=Path, required=True)
    calibration_plan.add_argument("--output", choices=["table", "json"], default="table")
    calibration_plan.set_defaults(handler=handle_nccl_eval_calibration)
    calibration_apply = calibration_sub.add_parser("apply")
    calibration_apply.add_argument("--input", type=Path, required=True)
    _add_nccl_mutation_gate(calibration_apply)
    calibration_apply.add_argument("--output", choices=["table", "json"], default="table")
    calibration_apply.set_defaults(handler=handle_nccl_eval_calibration)
    for calibration_command in ("list", "status"):
        calibration_read = calibration_sub.add_parser(calibration_command)
        calibration_read.add_argument("--limit", type=int, default=100)
        calibration_read.add_argument("--output", choices=["table", "json"], default="table")
        calibration_read.set_defaults(handler=handle_nccl_eval_calibration)

    nccl_build = nccl_sub.add_parser(
        "build-baselines", help="Report eligibility or build due immutable baselines"
    )
    _add_nccl_mutation_gate(nccl_build)
    nccl_build.add_argument("--output", choices=["table", "json"], default="table")
    nccl_build.set_defaults(handler=handle_nccl_eval_build_baselines)

    nccl_evaluate = nccl_sub.add_parser(
        "evaluate", help="Report the queue or evaluate one claimed batch"
    )
    from cval.nccl_eval.repository import default_worker_id

    nccl_evaluate.add_argument("--worker-id", default=default_worker_id())
    nccl_evaluate.add_argument("--batch-size", type=int)
    _add_nccl_mutation_gate(nccl_evaluate)
    nccl_evaluate.add_argument("--output", choices=["table", "json"], default="table")
    nccl_evaluate.set_defaults(handler=handle_nccl_eval_evaluate)

    nccl_worker = nccl_sub.add_parser(
        "worker", help="Run the graceful continuous NCCL evaluator worker"
    )
    nccl_worker.add_argument("--worker-id", default=default_worker_id())
    nccl_worker.add_argument("--recover-every-cycles", type=int, default=12)
    _add_nccl_mutation_gate(nccl_worker)
    nccl_worker.add_argument("--output", choices=["table", "json"], default="json")
    nccl_worker.set_defaults(handler=handle_nccl_eval_worker)

    nccl_resident = nccl_sub.add_parser(
        "resident", help="Run ingestion, baseline, recovery, and evaluation continuously"
    )
    nccl_resident.add_argument("--worker-id", default=default_worker_id())
    nccl_resident.add_argument("--outbox-root", type=Path, required=True)
    nccl_resident.add_argument("--ingest-limit", type=int, default=5000)
    nccl_resident.add_argument("--recover-every-cycles", type=int, default=12)
    _add_nccl_mutation_gate(nccl_resident)
    nccl_resident.add_argument("--output", choices=["json"], default="json")
    nccl_resident.set_defaults(handler=handle_nccl_eval_resident)

    nccl_recover = nccl_sub.add_parser(
        "recover", help="Report or recover expired evaluator claims"
    )
    _add_nccl_mutation_gate(nccl_recover)
    nccl_recover.add_argument("--output", choices=["table", "json"], default="table")
    nccl_recover.set_defaults(handler=handle_nccl_eval_recover)

    nccl_status = nccl_sub.add_parser(
        "status", help="Read NCCL queue, profile, and latest evaluation summaries"
    )
    nccl_status.add_argument("--latest-limit", type=int, default=20)
    nccl_status.add_argument("--output", choices=["table", "json"], default="table")
    nccl_status.set_defaults(handler=handle_nccl_eval_status)

    overview = subparsers.add_parser(
        "overview", help="One-screen status: free nodes, freshness, queue, and jobs"
    )
    overview.add_argument("--node-filter", default=active_config.cluster.node_filter)
    overview.add_argument(
        "--threshold-days", type=float, default=active_config.scheduling.days_threshold
    )
    overview.add_argument("--queue-limit", type=int, default=10)
    overview.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    overview.add_argument("--no-jobs", action="store_true", help="Skip listing Volcano jobs")
    overview.add_argument("--watch", action="store_true", help="Refresh until interrupted")
    overview.add_argument(
        "--interval", type=float, default=15.0, help="Watch refresh seconds"
    )
    overview.add_argument("--output", choices=["table", "json"], default="table")
    overview.set_defaults(handler=handle_overview)

    return parser


def handle_nodes(args: argparse.Namespace) -> int:
    """Run read-only GPU node discovery and print table or JSON output."""

    if args.inventory_only:
        names = discover_gpu_node_names(node_name_filter=args.node_filter)
        if args.output == "json":
            print(json.dumps({"nodes": names, "node_count": len(names)}, indent=2))
        else:
            print("\n".join(names))
            print(f"GPU nodes: {len(names)}")
        return 0

    if args.check_node:
        status = describe_node(args.check_node, config=args.cval_config)
        eligible = (
            status.found
            and status.is_gpu_node
            and status.ready
            and status.resource_ready
            and status.allocatable > 0
            and status.free == status.allocatable
            and status.schedulable
        )
        payload = asdict(status) | {"eligible": eligible}
        if args.output == "json":
            print(json.dumps(payload, indent=2))
        else:
            print(
                f"{status.name}: {status.status_label} eligible={str(eligible).lower()} "
                f"free={status.free}/{status.allocatable} - {status.reason}"
            )
        return 0

    nodes, totals = discover_free_nodes(node_name_filter=args.node_filter)
    if args.output == "json":
        # JSON output is meant for Hermes, scripts, and tests.
        print(
            json.dumps(
                {"nodes": [asdict(node) | {"free": node.free} for node in nodes], "totals": totals},
                indent=2,
            )
        )
        return 0

    # Table output is optimized for quick operator scanning.
    print(f"{'NODE':<32} {'CAP':>4} {'ALLOC':>5} {'USED':>5} {'FREE':>5}")
    for node in nodes:
        marker = "*" if node.is_fully_free else " "
        print(
            f"{marker} {node.name:<30} {node.capacity:>4} {node.allocatable:>5} "
            f"{node.used:>5} {node.free:>5}"
        )
    print(
        f"{'TOTAL':<32} {totals['capacity']:>4} {totals['allocatable']:>5} "
        f"{totals['used']:>5} {totals['free']:>5}"
    )
    print(f"Fully free nodes: {len(fully_free_node_names(nodes))}")
    return 0


def handle_config(args: argparse.Namespace) -> int:
    """Print effective c-val config for operators and automation."""

    print(json.dumps(config_to_dict(args.cval_config), indent=2))
    return 0


def handle_tests_list(args: argparse.Namespace) -> int:
    """List registered tests without importing or executing their adapters."""

    registry = args.cval_config.tests.registry
    tests = registry.enabled if args.enabled_only else registry.tests
    if args.output == "json":
        print(
            json.dumps(
                [
                    {
                        "id": test.id,
                        "display_name": test.definition.metadata.display_name,
                        "enabled": test.enabled,
                        "order": test.definition.metadata.order,
                        "config_path": test.config_path,
                        "schema_version": test.definition.schema_version,
                    }
                    for test in tests
                ],
                indent=2,
            )
        )
        return 0

    print(f"Registered validation tests: {len(tests)}")
    print(f"{'ID':<18} {'ENABLED':<8} {'ORDER':>5} {'SCHEMA':<16} CONFIG")
    for test in tests:
        print(
            f"{test.id:<18} {str(test.enabled).lower():<8} "
            f"{test.definition.metadata.order:>5} "
            f"{test.definition.schema_version:<16} {test.config_path}"
        )
    return 0


def handle_tests_describe(args: argparse.Namespace) -> int:
    """Show one registered test's effective composed configuration."""

    test = args.cval_config.tests.registry.get(args.test_id)
    if test is None:
        print(f"Validation test is not registered: {args.test_id}", file=sys.stderr)
        return 1
    data = test.to_dict()
    if args.output == "json":
        print(json.dumps(data, indent=2))
        return 0

    metadata = test.definition.metadata
    requirements = test.definition.requirements
    print(f"Validation test: {test.id} ({metadata.display_name})")
    print(f"Enabled: {str(test.enabled).lower()} | order: {metadata.order}")
    print(f"Config: {test.config_path} | schema: {test.definition.schema_version}")
    print(
        f"Entrypoint: {metadata.entrypoint} | setup: {metadata.setup} | "
        f"timeout: {metadata.timeout_seconds}s"
    )
    print(
        f"Requirements: cpu={requirements.cpu} memory={requirements.memory} "
        f"gpu={requirements.gpu_count} rdma={requirements.rdma_count} "
        f"shared_memory={requirements.shared_memory}"
    )
    return 0


def handle_tests_validate(args: argparse.Namespace) -> int:
    """Report successful registry validation performed during config load."""

    from cval.validation.plugins import PluginLoadError, validate_registry_plugins

    registry = args.cval_config.tests.registry
    try:
        loaded_plugins = validate_registry_plugins(registry.tests)
    except PluginLoadError as exc:
        print(f"Validation adapter error: {exc}", file=sys.stderr)
        return 1
    payload = {
        "valid": True,
        "registered_count": len(registry.tests),
        "enabled_count": len(registry.enabled),
        "plugin_count": len(loaded_plugins),
        "plugins": list(loaded_plugins),
        "tests": [test.id for test in registry.tests],
    }
    if args.output == "json":
        print(json.dumps(payload, indent=2))
        return 0
    print(
        f"Validation test registry is valid: {payload['registered_count']} registered, "
        f"{payload['enabled_count']} enabled"
    )
    for test in registry.tests:
        state = "enabled" if test.enabled else "disabled"
        print(
            f"  {test.definition.metadata.order:>3} {test.id:<18} "
            f"{state:<8} {test.config_path}"
        )
    return 0


def handle_tests_scaffold(args: argparse.Namespace) -> int:
    """Inspect or safely create one disabled pass/fail-only test directory."""

    from cval.validation.scaffold import scaffold_validation_test

    try:
        payload = scaffold_validation_test(
            args.test_id,
            args.order,
            repo_root=REPO_ROOT,
            apply=args.apply,
            confirmation=args.confirm,
        )
    except (FileExistsError, OSError, ValueError) as exc:
        print(f"Scaffold error: {exc}", file=sys.stderr)
        return 2
    if args.output == "json":
        print(json.dumps(payload, indent=2))
        return 0
    print(f"Validation test scaffold ({payload['mode']}): {payload['test_id']}")
    print(f"Target: {payload['target_dir']}")
    print("Files: " + ", ".join(payload["files"]))
    print("\nDisabled registry stanza:\n" + str(payload["registry_stanza"]))
    print("\nNext steps:")
    for step in payload["next_commands"]:
        print(f"  - {step}")
    return 0



def handle_operational_targets(args: argparse.Namespace) -> int:
    """Emit enabled capability-derived targets for trusted local loops."""

    catalog = build_operational_target_catalog(args.cval_config.tests.registry)
    targets = catalog.for_operation(args.operation)
    if args.output == "json":
        print(json.dumps([target.to_dict() for target in targets], indent=2))
        return 0
    for target in targets:
        print(
            "\t".join(
                (
                    "cval.operational-target.v1",
                    target.name,
                    target.owner_test_id,
                    target.baseline_test_type,
                    target.status_test,
                    str(target.alias).lower(),
                    target.refresh_group or "-",
                )
            )
        )
    return 0


def handle_status(args: argparse.Namespace) -> int:
    """Read latest validation status without mutating SQLite metadata."""

    rows = get_latest_status_rows(
        pod=args.pod,
        namespace=args.namespace,
        db_path=args.db_path,
        config=args.cval_config,
    )
    if args.output == "json":
        print(json.dumps([asdict(row) for row in rows], indent=2))
        return 0
    if args.output == "tsv":
        # TSV keeps compatibility with older status/parsing workflows.
        print(latest_status_rows_to_tsv(rows))
        return 0

    node_map = latest_status_rows_to_node_map(rows)
    print(f"Latest validation status rows: {len(rows)} | nodes: {len(node_map)}")
    print(f"{'NODE':<32} {'TEST':<18} {'TIMESTAMP':>12} RESULT")
    for row in rows:
        timestamp = "" if row.latest_timestamp is None else str(row.latest_timestamp)
        print(f"{row.node:<32} {row.test:<18} {timestamp:>12} {row.result}")
    return 0



def handle_plan(args: argparse.Namespace) -> int:
    """Build and print a nonmutating workflow plan."""

    from cval.orchestrator.workflow import workflow_plan_to_dict

    if not is_exact_commit(args.git_ref):
        raise PolicyViolation(
            "Plan inspection requires --git-ref to be an exact lowercase "
            "40-hex commit"
        )

    plan = _build_plan_from_args(args)

    if args.output == "json":
        print(
            json.dumps(
                workflow_plan_to_dict(plan, include_yaml=args.include_yaml),
                indent=2,
            )
        )
        return 0

    print("Validation workflow plan (read-only)")
    print(
        f"Free nodes: {len(plan.free_nodes)} | Queue: {len(plan.queue)} | "
        f"Batch: {len(plan.planned_jobs)}"
    )
    print(f"Threshold days: {plan.days_threshold} | Batch size: {plan.batch_size}")
    print(f"{'PRI':>3} {'NODE':<32} {'REASON':<13} JOB")
    for planned in plan.planned_jobs:
        print(
            f"{planned.candidate.priority:>3} {planned.candidate.node:<32} "
            f"{planned.candidate.reason:<13} {planned.rendered_job.job_name}"
        )
    return 0


def handle_run(args: argparse.Namespace) -> int:
    """Explicitly submit a planned validation batch."""

    if not args.submit:
        raise PolicyViolation(
            "run performs real cluster validation and requires --submit --confirm submit; "
            "use plan for read-only queue inspection"
        )
    if not is_exact_commit(args.git_ref):
        raise PolicyViolation(
            "Real cluster submission requires --git-ref to be an exact lowercase "
            "40-hex commit"
        )

    plan = _build_plan_from_args(args)
    config: CvalConfig = args.cval_config
    policy = ExecutionPolicy(
        namespace_allowlist=tuple(args.allowed_namespace or config.policy.namespace_allowlist),
        max_batch_size=args.max_batch_size,
        confirmation_phrase=config.policy.confirmation_phrase,
    )
    result = submit_workflow_plan(
        plan,
        namespace=args.namespace,
        policy=policy,
        submit=args.submit,
        confirmation=args.confirm,
    )

    if args.output == "json":
        print(json.dumps(submission_result_to_dict(result), indent=2))
        return 0

    print("Validation job submission (submitted)")
    print(f"Namespace: {result.namespace} | Jobs: {len(result.records)}")
    for record in result.records:
        print(f"  {record.node} -> {record.job_name} [{record.action}]")
    return 0


def handle_jobs(args: argparse.Namespace) -> int:
    """Read Volcano job phases once, or watch until terminal/timeout."""

    if args.watch:
        jobs = monitor_jobs_until_terminal(
            _parse_csv(args.jobs),
            namespace=args.namespace,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        if args.output == "json":
            print(json.dumps(monitored_jobs_to_dict(jobs), indent=2))
            return 0

        print(f"{'JOB':<64} {'PHASE':<12} TERMINAL TIMED_OUT ELAPSED")
        for job in jobs:
            print(
                f"{job.job_name:<64} {job.phase:<12} "
                f"{str(job.terminal):<8} {str(job.timed_out):<9} {job.elapsed_seconds:.1f}s"
            )
        return 0

    phases = get_job_phases(_parse_csv(args.jobs), namespace=args.namespace)
    if args.output == "json":
        print(json.dumps([asdict(phase) for phase in phases], indent=2))
        return 0

    print(f"{'JOB':<64} PHASE")
    for phase in phases:
        print(f"{phase.job_name:<64} {phase.phase}")
    return 0


def handle_result(args: argparse.Namespace) -> int:
    """Inspect a structured validation result JSON file."""

    result = load_validation_result(args.result_json)
    if args.output == "json":
        print(json.dumps(asdict(result), indent=2))
        return 0

    for line in validation_result_to_env_lines(result):
        print(line)
    return 0


def handle_results(args: argparse.Namespace) -> int:
    """Export latest per-node results for one selected test to a local CSV."""
    from cval.storage.results_export import (
        latest_result_rows,
        write_latest_results_csv,
        write_export_rows_csv,
    )
    from cval.storage.classification_status import get_latest_classification_rows
    from cval.validation.operations import (
        export_evaluator_rows,
        resolve_operational_target,
    )
    from cval.validation.plugins import ExportContext

    if args.result_type != "csv":
        raise ValueError("Only --type csv is currently supported")

    rows = get_latest_status_rows(
        pod=args.pod,
        namespace=args.namespace,
        db_path=args.db_path,
        config=args.cval_config,
    )
    test = args.test.lower()
    classifications = []
    if not args.no_classification and test != "nccl":
        selected_test = test
        classifications = get_latest_classification_rows(
            pod=args.pod,
            namespace=args.namespace,
            db_path=args.classification_db_path,
            test_type=(
                None if selected_test in {"all", "overall"} else selected_test
            ),
            config=args.cval_config,
        )

    if test in {"overall", "all"}:
        from cval.storage.metrics import get_latest_nccl_metrics, get_latest_storage_metrics

        nccl_metrics = None
        storage_metrics = None
        if not args.no_metrics:
            nccl_metrics = get_latest_nccl_metrics(
                pod=args.pod,
                namespace=args.namespace,
                db_path=args.nccl_db_path,
                config=args.cval_config,
            )
            storage_metrics = get_latest_storage_metrics(
                pod=args.pod,
                namespace=args.namespace,
                db_path=args.storage_db_path,
                config=args.cval_config,
            )
        selected = latest_result_rows(rows, args.test)
        output_path = write_latest_results_csv(
            rows,
            args.test,
            output_dir=args.output_dir,
            classifications=classifications,
            nccl_metrics=nccl_metrics,
            storage_metrics=storage_metrics,
        )
        output_label = f"{args.test} latest result"
    else:
        target = resolve_operational_target(args.cval_config, test, RESULTS_EXPORT)
        registered = args.cval_config.tests.registry.require(target.owner_test_id)
        context = ExportContext(
            target=target,
            definition=registered.definition,
            config=args.cval_config,
            status_rows=tuple(rows),
            classification_rows=tuple(classifications),
            pod=args.pod,
            namespace=args.namespace,
            source_db_paths=(
                ("nccl", args.nccl_db_path),
                ("storage", args.storage_db_path),
            ),
            include_metrics=not args.no_metrics,
        )
        export = export_evaluator_rows(args.cval_config, test, context)
        output_path = write_export_rows_csv(
            export,
            target.name,
            output_dir=args.output_dir,
        )
        selected = export.rows
        output_label = export.row_label or f"{target.name} latest result"
    print(f"Wrote {len(selected)} {output_label} row(s) to {output_path}")
    return 0


def handle_classifications(args: argparse.Namespace) -> int:
    """Export latest baseline classification verdicts to a local CSV."""
    from cval.storage.classification_status import (
        filter_classification_rows,
        get_latest_classification_rows,
        write_classifications_csv,
    )

    if args.result_type != "csv":
        raise ValueError("Only --type csv is currently supported")

    catalog = build_operational_target_catalog(args.cval_config.tests.registry)
    if args.test != "all":
        from cval.validation.operations import resolve_operational_target

        resolve_operational_target(
            args.cval_config, args.test, CLASSIFICATIONS_EXPORT
        )

    rows = get_latest_classification_rows(
        pod=args.pod,
        namespace=args.namespace,
        db_path=args.db_path,
        test_type=(None if args.test == "all" else args.test),
        config=args.cval_config,
    )
    if args.test == "all":
        enabled_targets = set(catalog.names_for(CLASSIFICATIONS_EXPORT))
        rows = [row for row in rows if row.test_type in enabled_targets]
    selected = filter_classification_rows(rows, args.test)
    output_path = write_classifications_csv(rows, args.test, output_dir=args.output_dir)
    print(f"Wrote {len(selected)} {args.test} classification row(s) to {output_path}")
    return 0



def handle_validate(args: argparse.Namespace) -> int:
    """Submit one exact-commit cluster job, live-track it, and report raw evidence."""
    if not args.submit:
        raise PolicyViolation(
            "validate performs real cluster validation and requires explicit "
            "--submit --confirm submit"
        )
    if not is_exact_commit(args.git_ref):
        raise PolicyViolation(
            "Cluster validation requires --git-ref to be an exact lowercase "
            "40-hex commit"
        )
    from cval.orchestrator.validate import run_node_validation

    report = run_node_validation(
        args.node,
        config=args.cval_config,
        namespace=args.namespace,
        git_ref=args.git_ref,
        timestamp=args.timestamp,
        poll_interval=args.poll_interval,
        overall_timeout=args.timeout_seconds,
        pending_timeout=args.pending_timeout,
        pod=args.pvc_pod,
        submit=args.submit,
        confirmation=args.confirm,
        download=args.download,
        download_dir=args.download_dir,
        verbose=(args.output == "table"),
    )
    if args.output == "json":
        print(json.dumps(report, indent=2))
    return 0 if report.get("ok", False) else 1


def _raw_write_authorization(args: argparse.Namespace):
    from cval.storage.write_provenance import authorize_result_write

    authorization = authorize_result_write(
        args.result_json,
        result_digest=args.result_digest,
        config_snapshot_b64=os.environ.get("CVAL_CONFIG_SNAPSHOT_B64", ""),
        config=args.cval_config,
    )
    result = authorization.result
    if args.node != result.node or parse_timestamp(args.timestamp) != parse_timestamp(
        result.timestamp
    ):
        raise ValueError("Raw DB writer identity does not match validated result")
    for argument_name, result_value in (
        ("image_name", result.image_name),
        ("pytorch_version", result.pytorch_version),
        ("cuda_version", result.cuda_version),
    ):
        if hasattr(args, argument_name) and getattr(args, argument_name) != result_value:
            raise ValueError(
                f"Raw DB writer {argument_name} does not match validated result"
            )
    if (
        isinstance(result, ValidationResultV2)
        and hasattr(args, "run_id")
        and args.run_id
        and args.run_id != result.run_id
    ):
        raise ValueError("Raw DB writer run_id does not match validated result")
    return authorization


def _require_v2_db_target(authorization, db_path: str | Path, config_field: str) -> None:
    if not isinstance(authorization.result, ValidationResultV2):
        return
    expected = Path(getattr(authorization.config.storage, config_field)).expanduser()
    if Path(db_path).expanduser() != expected:
        raise ValueError(
            f"Raw DB target does not match snapshot storage.{config_field}"
        )


def handle_db_add_result(args: argparse.Namespace) -> int:
    """Append one validation result row to the main SQLite DB."""

    authorization = _raw_write_authorization(args)
    values = validation_result_to_env(authorization.result)
    expected_result = (
        values["overall_result"]
        if args.test == "all"
        else authorization.result.tests.get(args.test).status
        if args.test in authorization.result.tests
        else None
    )
    if expected_result != args.result:
        raise ValueError("Raw DB status row does not match validated result")
    _require_v2_db_target(authorization, args.db_path, "validation_db_path")
    timestamp = add_validation_result(
        args.node,
        args.test,
        args.result,
        args.timestamp,
        image_name=args.image_name,
        pytorch_version=args.pytorch_version,
        cuda_version=args.cuda_version,
        db_path=args.db_path,
        _authorization=authorization,
    )
    print(f"Added validation result: {args.node} {args.test} {args.result} {timestamp}")
    return 0


def handle_db_add_run_results(args: argparse.Namespace) -> int:
    """Atomically append fixed built-in status rows for one run."""

    authorization = _raw_write_authorization(args)
    projected = validation_result_to_env(authorization.result)
    from cval.validation.builtins import (
        BUILTIN_AGGREGATE_TEST_ID,
        BUILTIN_TEST_PROJECTIONS,
        project_builtin_statuses,
    )

    expected_results = project_builtin_statuses(projected)
    actual_results = {
        item.test_id: getattr(args, f"{item.test_id}_result")
        for item in BUILTIN_TEST_PROJECTIONS
    } | {
        BUILTIN_AGGREGATE_TEST_ID: args.overall_result,
    }
    if actual_results != expected_results:
        raise ValueError("Raw DB status set does not match validated result")
    _require_v2_db_target(authorization, args.db_path, "validation_db_path")
    timestamp = add_validation_run_results(
        args.node,
        args.timestamp,
        actual_results,
        image_name=args.image_name,
        pytorch_version=args.pytorch_version,
        cuda_version=args.cuda_version,
        db_path=args.db_path,
        _authorization=authorization,
    )
    print(f"Added atomic validation run results: {args.node} {timestamp}")
    return 0



def handle_db_preflight_result(args: argparse.Namespace) -> int:
    """Validate result provenance and current raw DB targets without writing."""

    from cval.storage.write_provenance import authorize_result_write

    try:
        authorization = authorize_result_write(
            args.result_json,
            result_digest=args.result_digest,
            config_snapshot_b64=os.environ.get("CVAL_CONFIG_SNAPSHOT_B64", ""),
            config=args.cval_config,
        )
    except Exception as exc:  # noqa: BLE001 - hidden in-pod boundary
        print(f"Raw DB write preflight failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"Raw DB write preflight passed: "
        f"{authorization.result.node}-{authorization.result.timestamp}"
    )
    return 0


def handle_db_add_storage_result(args: argparse.Namespace) -> int:
    """Parse storage artifacts and write one storage metrics row."""

    authorization = _raw_write_authorization(args)
    if isinstance(authorization.result, ValidationResultV2):
        test = authorization.result.tests.get("storage")
        if test is None or test.status != "pass" or Path(test.artifacts) != args.results_dir:
            raise ValueError("Storage command does not match validated v2 evidence")
    _require_v2_db_target(authorization, args.db_path, "storage_db_path")
    timestamp = add_storage_result(
        args.node,
        args.timestamp,
        args.results_dir,
        image_name=args.image_name,
        run_id=args.run_id,
        immutable=args.immutable,
        db_path=args.db_path,
        _authorization=authorization,
    )
    print(f"Added storage result: {args.node} {timestamp}")
    return 0


def handle_db_add_nccl_health(args: argparse.Namespace) -> int:
    """Ingest one consolidated NCCL/IB health row from a summary JSON."""

    authorization = _raw_write_authorization(args)
    if not args.summary_json.exists():
        print(f"NCCL summary JSON not found: {args.summary_json}", file=sys.stderr)
        return 1
    if isinstance(authorization.result, ValidationResultV2):
        test = authorization.result.tests.get("nccl")
        if test is None or test.status != "pass" or Path(test.summary) != args.summary_json:
            raise ValueError("NCCL command does not match validated v2 evidence")
    _require_v2_db_target(authorization, args.db_path, "nccl_db_path")
    timestamp = add_nccl_health_from_summary(
        args.node,
        args.timestamp,
        args.summary_json,
        iterations=args.iterations,
        image_name=args.image_name,
        cuda_version=args.cuda_version,
        pytorch_version=args.pytorch_version,
        run_id=args.run_id,
        immutable=args.immutable,
        ibbw_log_path=args.ibbw_log,
        require_hca_samples=args.require_hca_samples,
        db_path=args.db_path,
        _authorization=authorization,
    )
    print(f"Added consolidated IB_HEALTH result: {args.node} {timestamp}")
    return 0


def handle_db_rebuild_dltest_metrics(args: argparse.Namespace) -> int:
    """Rebuild the four DL metric DBs from rank JSON artifacts."""
    from cval.storage.dltest_ingest import ingest_dltest_results
    from cval.storage.write_provenance import authorize_dl_rebuild

    try:
        authorization = authorize_dl_rebuild(
            args.results_root,
            args.output_dir,
            config=args.cval_config,
            config_snapshot_b64=os.environ.get(
                "CVAL_CONFIG_SNAPSHOT_B64",
                encode_config_snapshot(args.cval_config),
            ),
        )
        summary = ingest_dltest_results(
            args.results_root,
            args.output_dir,
            config=args.cval_config,
            _authorization=authorization,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.output == "json":
        print(json.dumps(summary, indent=2))
        return 0

    print(
        f"Rebuilt DL metric DBs from {summary['rank_files']} rank file(s) "
        f"across {summary['runs']} run(s)"
    )
    print(f"Results root: {summary['results_root']}")
    print(f"Output dir: {summary['output_dir']}")
    print(f"numerical_correctness: {summary['numerical_correctness_rows']} rows")
    print(f"compute_performance: {summary['compute_performance_rows']} rows")
    print(f"collective_performance: {summary['collective_performance_rows']} rows")
    print(f"overlap_performance: {summary['overlap_performance_rows']} rows")
    return 0


def handle_baseline_list(args: argparse.Namespace) -> int:
    """List stored baselines in the validation DB."""
    from cval.baselines.storage import list_dynamic_baselines
    from cval.validation.operations import resolve_operational_target

    if args.test_type:
        resolve_operational_target(args.cval_config, args.test_type, BASELINE_LIST)

    baselines = list_dynamic_baselines(
        test_type=args.test_type,
        db_path=args.db_path,
        config=args.cval_config,
    )
    if args.output == "json":
        print(
            json.dumps(
                [
                    {
                        "baseline_id": b[0],
                        "test_type": b[1],
                        "status": b[2],
                        "stratum_key": b[3],
                        "n_samples": b[4],
                        "created_at": b[5],
                    }
                    for b in baselines
                ],
                indent=2,
            )
        )
        return 0

    print(f"Stored baselines: {len(baselines)}")
    print(f"{'BASELINE_ID':<40} {'TEST_TYPE':<10} {'STATUS':<10} {'N':>5} STRATUM")
    for baseline_id, test_type, status, stratum_key, n_samples, _created_at in baselines:
        print(f"{baseline_id:<40} {test_type:<10} {status:<10} {n_samples:>5} {stratum_key}")
    return 0


def handle_baseline_build(args: argparse.Namespace) -> int:
    """Build a dynamic baseline from result DBs and optionally store/activate it."""
    from cval.baselines.storage import (
        activate_baseline,
        default_dynamic_baseline_db_paths,
        store_dynamic_baseline,
    )
    from cval.validation.operations import build_evaluator_baseline

    config: CvalConfig = args.cval_config
    try:
        record = build_evaluator_baseline(
            config,
            args.test_type,
            window_days=args.window_days,
            min_samples=args.min_samples,
            source_db=args.source_db,
            image_name=args.image_name,
            node=args.node,
            test_plan=args.test_plan,
            baseline_id=args.baseline_id,
        )
    except (FileNotFoundError, TypeError, ValueError) as exc:
        print(f"Baseline build failed: {exc}", file=sys.stderr)
        return 1

    stored = False
    if args.store or args.activate:
        store_dynamic_baseline(record, db_path=args.db_path, status="candidate", config=config)
        stored = True
        if args.activate:
            activate_baseline(record["baseline_id"], args.test_type, db_path=args.db_path, config=config)

    metrics = record["metrics"]
    if args.output == "json":
        payload = dict(record)
        payload["stored"] = stored
        payload["activated"] = bool(args.activate)
        print(json.dumps(payload, indent=2))
        return 0

    print(f"Built {args.test_type} baseline: {record['baseline_id']}")
    print(
        f"Stratum: {record['stratum_key'] or '(all)'} | window: {record['window_days']}d | "
        f"runs: {record['n_samples']} | metrics: {len(metrics)}"
    )
    if stored:
        state = "active" if args.activate else "candidate"
        if args.db_path:
            print(f"Stored in {args.db_path} as {state}")
        else:
            paths = ", ".join(str(path) for path in default_dynamic_baseline_db_paths(args.test_type, config=config))
            print(f"Stored in default baseline DB(s) as {state}: {paths}")
    if not metrics:
        print("No metrics met --min-samples; lower it or widen --window-days.")
        return 0
    print(f"{'METRIC':<48} {'MEDIAN':>14} {'MAD_SIGMA':>12} {'N':>4} DIR")
    for key, stat in metrics.items():
        print(
            f"{key:<48} {stat['median']:>14.4g} {stat['mad_sigma']:>12.4g} "
            f"{stat['n']:>4} {stat['direction']}"
        )
    return 0


def handle_baseline_activate(args: argparse.Namespace) -> int:
    """Promote a stored baseline to active and supersede the previous one."""
    from cval.baselines.storage import activate_baseline
    from cval.validation.operations import resolve_operational_target

    resolve_operational_target(
        args.cval_config, args.test_type, BASELINE_ACTIVATE
    )

    try:
        activated = activate_baseline(
            args.baseline_id,
            args.test_type,
            db_path=args.db_path,
            config=args.cval_config,
        )
    except (TypeError, ValueError) as exc:
        print(f"Baseline activation failed: {exc}", file=sys.stderr)
        return 1
    if not activated:
        print(
            f"Baseline not found: {args.baseline_id} ({args.test_type})",
            file=sys.stderr,
        )
        return 1
    print(f"Activated baseline: {args.baseline_id} ({args.test_type})")
    return 0


def handle_baseline_show(args: argparse.Namespace) -> int:
    """Print a stored baseline record and its per-metric acceptance bands."""
    from cval.baselines.storage import load_dynamic_baseline
    from cval.validation.operations import resolve_operational_target

    resolve_operational_target(args.cval_config, args.test_type, BASELINE_SHOW)

    record = load_dynamic_baseline(
        args.baseline_id,
        args.test_type,
        db_path=args.db_path,
        config=args.cval_config,
    )
    if record is None:
        print(
            f"Baseline not found: {args.baseline_id} ({args.test_type})",
            file=sys.stderr,
        )
        return 1

    if args.output == "json":
        print(json.dumps(record, indent=2))
        return 0

    metrics = record.get("metrics", {})
    print(f"Baseline: {record.get('baseline_id')} ({record.get('test_type')})")
    print(
        f"Stratum: {record.get('stratum_key') or '(all)'} | "
        f"window: {record.get('window_days')}d | runs: {record.get('n_samples')} | "
        f"metrics: {len(metrics)}"
    )
    print(f"{'METRIC':<48} {'MEDIAN':>14} {'LOWER':>14} {'UPPER':>14} DIR")
    for key, stat in metrics.items():
        lower = stat.get("lower_bound")
        upper = stat.get("upper_bound")
        lower_s = "-inf" if lower is None else f"{lower:.4g}"
        upper_s = "+inf" if upper is None else f"{upper:.4g}"
        print(
            f"{key:<48} {stat['median']:>14.4g} {lower_s:>14} {upper_s:>14} "
            f"{stat['direction']}"
        )
    return 0


def handle_baseline_classify(args: argparse.Namespace) -> int:
    """Classify one or all nodes against the active (or named) baseline."""
    from cval.baselines.storage import (
        default_classification_db_path,
        get_active_baseline,
        load_dynamic_baseline,
        store_classification_results,
    )
    from cval.validation.operations import (
        classify_evaluator_target,
        resolve_operational_target,
    )

    config: CvalConfig = args.cval_config
    resolve_operational_target(config, args.test_type, BASELINE_CLASSIFY)
    if args.baseline_id:
        baseline = load_dynamic_baseline(
            args.baseline_id,
            args.test_type,
            db_path=args.db_path,
            config=config,
        )
    else:
        baseline = get_active_baseline(args.test_type, db_path=args.db_path, config=config)
    if not baseline:
        target = args.baseline_id or "active"
        print(
            f"No {target} baseline for {args.test_type}; build and activate one first.",
            file=sys.stderr,
        )
        return 1

    try:
        verdicts = classify_evaluator_target(
            config,
            args.test_type,
            baseline,
            node=args.node,
            source_db=args.source_db,
            window_days=args.window_days,
        )
    except (TypeError, ValueError) as exc:
        print(f"Classification failed: {exc}", file=sys.stderr)
        return 1

    stored_count = 0
    if args.store_results:
        try:
            stored_count = store_classification_results(
                verdicts,
                db_path=args.classification_db_path,
                config=config,
            )
        except (TypeError, ValueError) as exc:
            print(f"Classification storage failed: {exc}", file=sys.stderr)
            return 1

    if args.output == "json":
        payload = {
            "verdicts": verdicts,
            "stored_count": stored_count,
            "classification_db_path": str(
                args.classification_db_path
                or default_classification_db_path(args.test_type, config)
            ) if args.store_results else "",
        }
        print(json.dumps(payload if args.store_results else verdicts, indent=2))
        return 0

    print(f"Classification vs baseline {baseline.get('baseline_id')} ({args.test_type})")
    if args.store_results:
        target_db = args.classification_db_path or default_classification_db_path(
            args.test_type, config
        )
        print(f"Stored {stored_count} classification row(s) in {target_db}")
    if not verdicts:
        print("No nodes found in the window.")
        return 0
    print(
        f"{'NODE':<32} {'STATUS':<9} {'BAD':>8} {'BAND_BAD':>8} "
        f"{'BAD%':>8} {'WORST%':>8} {'COMPARED':>8}"
    )
    for verdict in verdicts:
        print(
            f"{verdict['node']:<32} {verdict['status']:<9} {verdict['n_degraded']:>8} "
            f"{verdict.get('n_band_degraded', verdict['n_degraded']):>8} "
            f"{verdict.get('degraded_metric_percent', 0.0):>7.2f}% "
            f"{verdict.get('worst_pct_diff', 0.0):>7.2f}% {verdict['n_compared']:>8}"
        )
    degraded = [v["node"] for v in verdicts if v["status"] == "degraded"]
    if degraded:
        print(f"Degraded nodes: {', '.join(degraded)}")
    return 0


def handle_nccl_eval_schema(args: argparse.Namespace) -> int:
    """Plan packaged migrations or apply them after the exact schema gate."""

    if args.apply:
        _require_nccl_apply(args, "schema")
    else:
        from cval.nccl_eval.schema import migration_plan

        _print_nccl_payload(migration_plan(), args.output)
        return 0
    return _run_nccl_db_action(args, "apply_schema")


def handle_nccl_eval_grant_runtime(args: argparse.Namespace) -> int:
    """Provision the runtime login from Secret-backed environment only."""

    if args.apply:
        _require_nccl_apply(args, "grant-runtime")
    else:
        _print_nccl_payload(
            {
                "mode": "dry-run",
                "required_environment": [
                    "DATABASE_URL",
                    "CVAL_POSTGRES_RUNTIME_USERNAME",
                    "CVAL_POSTGRES_RUNTIME_PASSWORD",
                ],
                "ownership_granted": False,
            },
            args.output,
        )
        return 0
    try:
        from cval.nccl_eval.config import NcclEvaluationConfig
        from cval.nccl_eval.service import grant_runtime

        username = os.environ.get("CVAL_POSTGRES_RUNTIME_USERNAME", "")
        password = os.environ.get("CVAL_POSTGRES_RUNTIME_PASSWORD", "")
        config = NcclEvaluationConfig.from_env(require_database=True)
        _print_nccl_payload(
            grant_runtime(config, username=username, password=password), args.output
        )
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_ingest(args: argparse.Namespace) -> int:
    """Validate normalized input without a DB, or ingest it atomically."""

    if args.apply:
        _require_nccl_apply(args, "ingest")
    try:
        from cval.nccl_eval.models import IngestionBatch
        from cval.nccl_eval.profile import build_profile_identity

        raw = json.loads(args.input.read_text(encoding="utf-8"))
        batch = IngestionBatch.from_dict(raw)
        if not args.apply:
            profile = build_profile_identity(batch.test_run)
            _print_nccl_payload(
                {
                    "mode": "dry-run",
                    "valid": True,
                    "run_id": str(batch.test_run.run_id),
                    "profile_id": str(profile.profile_id),
                    "profile_key": profile.profile_key,
                    "node_count": len(batch.node_results),
                    "nic_count": sum(len(node.nics) for node in batch.node_results),
                },
                args.output,
            )
            return 0
        from cval.nccl_eval.config import NcclEvaluationConfig
        from cval.nccl_eval.service import ingest

        config = NcclEvaluationConfig.from_env(require_database=True)
        _print_nccl_payload(ingest(config, batch), args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_emit_outbox(args: argparse.Namespace) -> int:
    """Build a descriptor-bound native batch and optionally emit it once."""

    if args.apply:
        _require_nccl_apply(args, "emit-outbox")
    try:
        from cval.nccl_eval.outbox import (
            build_ingestion_batch,
            emission_plan,
            emit_outbox,
        )

        batch = build_ingestion_batch(
            result_json=args.result_json,
            summary=args.summary,
            runtime_evidence=args.runtime_evidence,
            result_digest=args.result_digest,
            config=args.cval_config,
        )
        payload = (
            emit_outbox(args.outbox_root, batch.test_run.cval_run_id, batch)
            if args.apply
            else emission_plan(args.outbox_root, batch.test_run.cval_run_id, batch)
        )
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - immutable-file CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_commit_outbox(args: argparse.Namespace) -> int:
    """Validate or write the final committed marker for one pending payload."""

    if args.apply:
        _require_nccl_apply(args, "commit-outbox")
    try:
        from cval.nccl_eval.outbox import commit_outbox, commit_outbox_plan

        payload = (
            commit_outbox(
                args.outbox_root,
                pending=args.pending,
                result_digest=args.result_digest,
            )
            if args.apply
            else commit_outbox_plan(
                args.outbox_root,
                pending=args.pending,
                result_digest=args.result_digest,
            )
        )
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - immutable-file CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_ingest_outbox(args: argparse.Namespace) -> int:
    """Scan before any database setup, then ingest each exact valid file."""

    if args.apply:
        _require_nccl_apply(args, "ingest-outbox")
    try:
        from cval.nccl_eval.outbox import ingest_outbox_progression, scan_outbox

        scan = scan_outbox(args.outbox_root, limit=args.limit)
        if not args.apply:
            _print_nccl_payload(scan.public_dict(), args.output)
            return 0
        from cval.nccl_eval.config import NcclEvaluationConfig

        config = NcclEvaluationConfig.from_env(require_database=True)
        payload = ingest_outbox_progression(
            config,
            args.outbox_root,
            limit=args.limit,
            continue_on_error=args.continue_on_error,
        )
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_calibration(args: argparse.Namespace) -> int:
    """Plan/apply exact decision IDs or inspect the append-only ledger."""

    try:
        from cval.nccl_eval.config import NcclEvaluationConfig
        from cval.nccl_eval.repository import parse_calibration_input
        from cval.nccl_eval.service import (
            apply_calibration,
            calibration_plan,
            calibration_report,
        )

        config = NcclEvaluationConfig.from_env(require_database=True)
        command = args.nccl_calibration_command
        if command in {"list", "status"}:
            payload = calibration_report(config, limit=args.limit)
        else:
            raw = json.loads(args.input.read_text(encoding="utf-8"))
            decisions = parse_calibration_input(raw)
            if command == "apply":
                _require_nccl_apply(args, "calibration")
                payload = apply_calibration(config, decisions)
            else:
                payload = calibration_plan(config, decisions)
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_build_baselines(args: argparse.Namespace) -> int:
    """Report baseline eligibility or run the approval-gated builder once."""

    if args.apply:
        _require_nccl_apply(args, "build-baselines")
    return _run_nccl_db_action(
        args, "build_baselines" if args.apply else "baseline_report"
    )


def handle_nccl_eval_evaluate(args: argparse.Namespace) -> int:
    """Report ready jobs or claim and evaluate a short batch."""

    if args.apply:
        _require_nccl_apply(args, "evaluate")
    try:
        from cval.nccl_eval.config import NcclEvaluationConfig
        from cval.nccl_eval.service import evaluate_once, queue_report

        config = NcclEvaluationConfig.from_env(require_database=True)
        payload = (
            evaluate_once(
                config, worker_id=args.worker_id, batch_size=args.batch_size
            )
            if args.apply
            else queue_report(config)
        )
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_worker(args: argparse.Namespace) -> int:
    """Run the signal-aware evaluator service until graceful shutdown."""

    if args.apply:
        _require_nccl_apply(args, "worker")
    try:
        from cval.nccl_eval.config import NcclEvaluationConfig
        from cval.nccl_eval.service import queue_report, worker

        config = NcclEvaluationConfig.from_env(require_database=True)
        payload = (
            worker(
                config,
                worker_id=args.worker_id,
                recover_every_cycles=args.recover_every_cycles,
            )
            if args.apply
            else queue_report(config)
        )
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_resident(args: argparse.Namespace) -> int:
    """Run all recurring NCCL evaluator tasks in one resident process."""

    if args.apply:
        _require_nccl_apply(args, "resident")
    try:
        from cval.nccl_eval.config import NcclEvaluationConfig
        from cval.nccl_eval.service import queue_report, resident

        config = NcclEvaluationConfig.from_env(require_database=True)
        if not args.apply:
            _print_nccl_payload(queue_report(config), args.output)
            return 0

        def emit(payload: dict[str, object]) -> None:
            print(json.dumps(payload, sort_keys=True), flush=True)

        payload = resident(
            config,
            worker_id=args.worker_id,
            outbox_root=args.outbox_root,
            ingest_limit=args.ingest_limit,
            recover_every_cycles=args.recover_every_cycles,
            event_sink=emit,
        )
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def handle_nccl_eval_recover(args: argparse.Namespace) -> int:
    """Report or approval-gate stale processing claim recovery."""

    if args.apply:
        _require_nccl_apply(args, "recover")
    return _run_nccl_db_action(args, "recover" if args.apply else "stale_report")


def handle_nccl_eval_status(args: argparse.Namespace) -> int:
    """Read subsystem status without mutations."""

    try:
        from cval.nccl_eval.config import NcclEvaluationConfig
        from cval.nccl_eval.service import status

        config = NcclEvaluationConfig.from_env(require_database=True)
        _print_nccl_payload(
            status(config, latest_limit=args.latest_limit), args.output
        )
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def _add_nccl_mutation_gate(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")


def _require_nccl_apply(args: argparse.Namespace, phrase: str) -> None:
    if not args.apply or args.confirm != phrase:
        raise PolicyViolation(
            f"NCCL mutation requires --apply --confirm {phrase}"
        )


def _run_nccl_db_action(args: argparse.Namespace, action: str) -> int:
    try:
        from cval.nccl_eval import service
        from cval.nccl_eval.config import NcclEvaluationConfig

        config = NcclEvaluationConfig.from_env(require_database=True)
        payload = getattr(service, action)(config)
        _print_nccl_payload(payload, args.output)
        return 0
    except Exception as exc:  # noqa: BLE001 - secret-redacting CLI boundary
        return _report_nccl_error(exc)


def _print_nccl_payload(payload: dict[str, object], output: str) -> None:
    if output == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print("NCCL PostgreSQL evaluation")
    for key, value in payload.items():
        if isinstance(value, list | dict):
            print(f"{key}: {json.dumps(value, sort_keys=True)}")
        else:
            print(f"{key}: {value}")


def _report_nccl_error(exc: BaseException) -> int:
    import re

    message = str(exc).replace("\n", " ").replace("\r", " ")
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        message = message.replace(database_url, "[DATABASE_URL REDACTED]")
    message = re.sub(
        r"(postgres(?:ql)?://)[^\s/@:]+(?::[^\s/@]*)?@",
        r"\1[REDACTED]@",
        message,
        flags=re.IGNORECASE,
    )
    message = re.sub(
        r"\b(user|password|passfile|sslkey)=('(?:[^']|'')*'|\S+)",
        r"\1=[REDACTED]",
        message,
        flags=re.IGNORECASE,
    )
    print(f"NCCL evaluation error: {message[:1000]}", file=sys.stderr)
    return 2


def handle_overview(args: argparse.Namespace) -> int:
    """Print a one-screen operational overview, optionally auto-refreshing."""
    import time

    from cval.orchestrator.overview import build_overview, render_overview

    def render_once() -> None:
        overview = build_overview(
            config=args.cval_config,
            node_filter=args.node_filter,
            days_threshold=args.threshold_days,
            queue_limit=args.queue_limit,
            namespace=args.namespace,
            include_jobs=not args.no_jobs,
        )
        if args.output == "json":
            print(json.dumps(overview, indent=2))
        else:
            print(render_overview(overview))

    if not args.watch:
        render_once()
        return 0

    try:
        while True:
            # Clear screen + home cursor, then redraw.
            print("\033[2J\033[H", end="")
            render_once()
            print(f"\n(refreshing every {args.interval:.0f}s - Ctrl-C to stop)")
            time.sleep(max(1.0, args.interval))
    except KeyboardInterrupt:
        print()
        return 0


def _build_plan_from_args(args: argparse.Namespace):
    """Resolve status inputs, discover nodes if needed, and build a workflow plan."""

    from cval.orchestrator.workflow import build_workflow_plan

    db_status = _load_db_status(args)
    if args.free_nodes is not None:
        # Explicit node lists are useful for controlled one-node submissions.
        free_nodes = _parse_csv(args.free_nodes)
    else:
        # No explicit nodes means live read-only discovery is part of the plan.
        nodes, _ = discover_free_nodes(node_name_filter=args.node_filter)
        free_nodes = fully_free_node_names(nodes)

    return build_workflow_plan(
        free_nodes,
        db_status,
        days_threshold=args.threshold_days,
        batch_size=args.batch_size,
        template_path=args.template,
        timestamp=args.timestamp,
        job_prefix=args.job_prefix,
        git_repo=args.git_repo,
        git_ref=args.git_ref,
    )


def _add_plan_inputs(parser: argparse.ArgumentParser, config: CvalConfig) -> None:
    """Attach shared plan-building arguments to plan-like commands."""

    parser.add_argument(
        "--free-nodes",
        help="Comma-separated free node names; discovers live nodes if omitted",
    )
    parser.add_argument("--db-status-json", type=Path, help="JSON object mapping node to timestamp")
    parser.add_argument(
        "--db-status-tsv",
        type=Path,
        help="TSV output from existing latest-status command",
    )
    parser.add_argument(
        "--live-status",
        action="store_true",
        help="Read latest status from the PVC access pod in read-only mode",
    )
    parser.add_argument("--status-pod", default=config.cluster.pvc_access_pod)
    parser.add_argument("--status-namespace", default=config.cluster.namespace)
    parser.add_argument("--status-db-path", default=config.storage.validation_db_path)
    parser.add_argument("--threshold-days", type=float, default=config.scheduling.days_threshold)
    parser.add_argument("--batch-size", type=int, default=config.scheduling.batch_size)
    parser.add_argument("--timestamp", type=int)
    parser.add_argument("--template", type=Path, default=config.job.template_path)
    parser.add_argument("--job-prefix", default=config.job.job_prefix)
    parser.add_argument("--git-repo", default=config.job.git_repo)
    parser.add_argument("--git-ref", default=config.job.git_ref)
    parser.add_argument("--node-filter", default=config.cluster.node_filter, help="Live discovery substring filter")


def _parse_csv(value: str) -> list[str]:
    """Parse comma-separated CLI values while ignoring empty items."""

    return [item.strip() for item in value.split(",") if item.strip()]



def _load_db_status(args: argparse.Namespace) -> dict[str, int]:
    """Load validation history from JSON, TSV, live status, or empty defaults."""

    selected = sum(1 for source in (args.db_status_json, args.db_status_tsv, args.live_status) if source)
    if selected > 1:
        raise ValueError("Use only one of --db-status-json, --db-status-tsv, or --live-status")
    if args.live_status:
        # Live status is read-only, but still reaches the cluster through the PVC access pod.
        return latest_status_rows_to_node_map(
            get_latest_status_rows(
                pod=args.status_pod,
                namespace=args.status_namespace,
                db_path=args.status_db_path,
                config=args.cval_config,
            )
        )
    if args.db_status_tsv:
        return parse_latest_status_tsv(args.db_status_tsv.read_text(encoding="utf-8"))
    if args.db_status_json is None:
        # Missing history makes every free node appear never-tested.
        return {}
    data = json.loads(args.db_status_json.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("DB status JSON must be an object mapping node names to timestamps")
    return {str(node): int(timestamp) for node, timestamp in data.items()}


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
