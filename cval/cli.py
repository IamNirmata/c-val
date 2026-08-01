"""Command-line interface for c-val 2.0.

This file is the main human/Hermes entry point. It exposes read-only status and
discovery commands, dry-run planning, approval-gated submission, read-only
monitoring, and structured result inspection. Handlers are intentionally thin:
they parse arguments, call package modules, and format output.

Public commands: config, tests, compatibility, nodes, validate, status, history,
plan, run, jobs, result, results, classifications, health, baseline, and overview.
The db-add-* commands are in-pod ingestion hooks and stay out of --help.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, replace
from pathlib import Path

from cval.config import (
    CvalConfig,
    REPO_ROOT,
    config_to_dict,
    encode_config_snapshot,
    load_config,
    load_config_snapshot,
)
from cval.jobs.manager import submission_result_to_dict, submit_workflow_plan
from cval.jobs.monitor import get_job_phases, monitored_jobs_to_dict, monitor_jobs_until_terminal
from cval.k8s.discovery import discover_free_nodes, fully_free_node_names
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


_STRICT_JSON_COMMANDS = frozenset(
    {
        "evaluator-preflight",
        "evaluator-parity",
        "evaluator-backup",
        "evaluator-service",
    }
)


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments, dispatch to a handler, and return a process code."""

    raw_argv = sys.argv[1:] if argv is None else argv
    if _is_compatibility_command(raw_argv):
        return _dispatch_compatibility_command(raw_argv)
    strict_json = any(command in raw_argv for command in _STRICT_JSON_COMMANDS)
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", type=Path)
    parsed_bootstrap = _parse_cli_arguments(
        lambda: bootstrap.parse_known_args(raw_argv),
        strict_json=strict_json,
    )
    if parsed_bootstrap is None:
        return 2
    config_args, _ = parsed_bootstrap
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
        if strict_json:
            _print_strict_json({"ok": False, "error": _single_line_error(exc)})
            return 2
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    parser = build_parser(config)
    args = _parse_cli_arguments(
        lambda: parser.parse_args(raw_argv),
        strict_json=strict_json,
    )
    if args is None:
        return 2
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


def _is_compatibility_command(argv: list[str]) -> bool:
    """Recognize the config-independent command at the top-level position."""

    index = 0
    if index < len(argv) and argv[index] == "--config":
        index += 2
    elif index < len(argv) and argv[index].startswith("--config="):
        index += 1
    return index < len(argv) and argv[index] == "compatibility"


def _dispatch_compatibility_command(argv: list[str]) -> int:
    """Parse and run offline compatibility tools without loading configuration."""

    parser = argparse.ArgumentParser(prog="cval")
    parser.add_argument("--config", type=Path, help=argparse.SUPPRESS)
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_compatibility_parser(subparsers)
    args = parser.parse_args(argv)
    return args.handler(args)


def _add_compatibility_parser(
    subparsers: argparse._SubParsersAction,
) -> None:
    compatibility = subparsers.add_parser(
        "compatibility",
        help="Inventory or offline-audit retained compatibility surfaces",
    )
    compatibility_sub = compatibility.add_subparsers(
        dest="compatibility_command", required=True
    )
    compatibility_inventory = compatibility_sub.add_parser(
        "inventory", help="Print the immutable compatibility surface catalog"
    )
    compatibility_inventory.add_argument(
        "--output", choices=["table", "json"], default="table"
    )
    compatibility_inventory.set_defaults(handler=handle_compatibility_inventory)

    compatibility_audit = compatibility_sub.add_parser(
        "audit", help="Scan only explicitly named local copied files under fixed bounds"
    )
    compatibility_audit.add_argument(
        "--input", type=Path, action="append", required=True, dest="inputs"
    )
    compatibility_audit.add_argument(
        "--output", choices=["table", "json"], default="table"
    )
    compatibility_audit.set_defaults(handler=handle_compatibility_audit)


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
            "{config,tests,compatibility,nodes,validate,status,history,plan,run,jobs,result,"
            "results,classifications,health,baseline,overview}"
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
        help="Plan or create a disabled pass/fail-only test scaffold",
    )
    tests_scaffold.add_argument("test_id")
    tests_scaffold.add_argument("--order", type=int, required=True)
    tests_scaffold.add_argument(
        "--apply", action="store_true", help="Create files after the exact confirmation"
    )
    tests_scaffold.add_argument("--confirm")
    tests_scaffold.add_argument("--output", choices=["table", "json"], default="table")
    tests_scaffold.set_defaults(handler=handle_tests_scaffold)

    _add_compatibility_parser(subparsers)

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
    nodes.add_argument("--output", choices=["table", "json"], default="table")
    nodes.set_defaults(handler=handle_nodes)

    validate = subparsers.add_parser(
        "validate",
        help="Submit, live-track, classify, and report validation for one node",
    )
    validate.add_argument("--node", required=True, help="Target node name to validate")
    validate.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    validate.add_argument("--git-ref", default=active_config.job.git_ref)
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
        "--window-days", type=int, default=active_config.baseline.window_days
    )
    validate.add_argument(
        "--pvc-pod",
        default=active_config.cluster.pvc_access_pod,
        help="PVC access pod used to run classification",
    )
    validate.add_argument(
        "--pod-repo-dir",
        default="/tmp/c-val",
        help="c-val checkout directory inside the PVC pod",
    )
    validate.add_argument(
        "--pod-config",
        help="Config path inside the PVC pod; defaults to <pod-repo-dir>/config/cval.toml",
    )
    validate.add_argument(
        "--skip-dl-rebuild",
        action="store_true",
        help="Do not refresh DL metric DBs for the node before classifying",
    )
    validate.add_argument(
        "--dl-rebuild-timeout",
        type=float,
        default=300.0,
        help="Dedicated timeout (s) for the scoped DL metric refresh",
    )
    validate.add_argument(
        "--dl-lock-wait",
        type=float,
        default=120.0,
        help="Max seconds to wait for the shared DL metric lock before skipping refresh",
    )
    validate.add_argument(
        "--dry-run", action="store_true", help="Render only; do not submit a job"
    )
    validate.add_argument(
        "--download",
        action="store_true",
        help="Save this run's logs, results, and baseline comparison as a local zip",
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

    history = subparsers.add_parser(
        "history", help="Read normalized node run history from the PVC access pod"
    )
    history.add_argument("--pod", default=active_config.cluster.pvc_access_pod)
    history.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    history.add_argument("--db-path", default=active_config.storage.run_history_db_path)
    history.add_argument("--run-id")
    history.add_argument("--node")
    history.add_argument("--test")
    history.add_argument(
        "--status", choices=["pass", "fail", "incomplete"]
    )
    history.add_argument("--limit", type=int, default=100)
    history.add_argument("--output", choices=["table", "json"], default="table")
    history.set_defaults(handler=handle_history)

    plan = subparsers.add_parser(
        "plan", help="Build a detailed dry-run queue and rendered job plan"
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
        help="Plan a validation batch and optionally submit it",
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
        help="Override classification-results DB path; defaults to baseline_root_path DB",
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
        help="Override classification-results DB path; defaults to baseline_root_path DB",
    )
    classifications.set_defaults(handler=handle_classifications)

    health = subparsers.add_parser(
        "health", help="Dry-run or apply registry-driven U8 health evaluation"
    )
    health_sub = health.add_subparsers(dest="health_command", required=True)

    health_evaluate = health_sub.add_parser(
        "evaluate", help="Evaluate enabled health+ingest tests (dry-run by default)"
    )
    health_evaluate.add_argument(
        "--apply", action="store_true", help="Write candidates/history after safety gates"
    )
    health_evaluate.add_argument("--confirm")
    health_evaluate.add_argument("--output", choices=["table", "json"], default="table")
    health_evaluate.set_defaults(handler=handle_health_evaluate)

    health_activate = health_sub.add_parser(
        "activate", help="Preflight or deliberately activate one named candidate"
    )
    health_activate.add_argument("test_id")
    health_activate.add_argument("baseline_id")
    health_activate.add_argument(
        "--apply", action="store_true", help="Perform activation after safety gates"
    )
    health_activate.add_argument("--confirm")
    health_activate.add_argument("--output", choices=["table", "json"], default="table")
    health_activate.set_defaults(handler=handle_health_activate)

    # U11 machine-only service/deployment preparation commands. They are
    # intentionally absent from public help and emit exactly one JSON value.
    evaluator_preflight = subparsers.add_parser("evaluator-preflight")
    evaluator_preflight.add_argument("--access", choices=["ro", "rw"], default="ro")
    evaluator_preflight.add_argument(
        "--state-root",
        "--validation-root",
        dest="state_root",
        type=Path,
    )
    evaluator_preflight.set_defaults(handler=handle_evaluator_preflight)

    evaluator_parity = subparsers.add_parser("evaluator-parity")
    evaluator_parity.add_argument("--u8-json", type=Path, action="append", default=[])
    evaluator_parity.add_argument("--u8-db", type=Path, action="append", default=[])
    evaluator_parity.add_argument(
        "--compatibility-json", type=Path, action="append", default=[]
    )
    evaluator_parity.add_argument(
        "--compatibility-db", type=Path, action="append", default=[]
    )
    evaluator_parity.set_defaults(handler=handle_evaluator_parity)

    evaluator_backup = subparsers.add_parser("evaluator-backup")
    evaluator_backup.add_argument("--source-root", type=Path, required=True)
    evaluator_backup.add_argument("--destination", type=Path, required=True)
    evaluator_backup.add_argument("--apply", action="store_true")
    evaluator_backup.add_argument("--confirm")
    evaluator_backup.set_defaults(handler=handle_evaluator_backup)

    evaluator_service = subparsers.add_parser("evaluator-service")
    evaluator_service.add_argument("--apply", action="store_true")
    evaluator_service.add_argument("--confirm")
    evaluator_service.add_argument("--write-enabled", action="store_true")
    evaluator_service.add_argument("--expected-commit")
    evaluator_service.add_argument("--image-ref")
    evaluator_service.set_defaults(handler=handle_evaluator_service)

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

    db_upsert_history = subparsers.add_parser("db-upsert-run-history")
    db_upsert_history.add_argument("--result-json", type=Path, required=True)
    db_upsert_history.add_argument("--result-digest", required=True)
    db_upsert_history.add_argument(
        "--db-path", default=active_config.storage.run_history_db_path
    )
    db_upsert_history.set_defaults(handler=handle_db_upsert_run_history)

    db_ingest_tests = subparsers.add_parser("db-ingest-test-results")
    db_ingest_tests.add_argument("--result-json", type=Path, required=True)
    db_ingest_tests.add_argument("--result-digest", required=True)
    db_ingest_tests.set_defaults(handler=handle_db_ingest_test_results)

    db_preflight_tests = subparsers.add_parser("db-preflight-test-results")
    db_preflight_tests.add_argument("--result-json", type=Path, required=True)
    db_preflight_tests.add_argument("--result-digest", required=True)
    db_preflight_tests.set_defaults(handler=handle_db_preflight_test_results)

    db_preflight_compat = subparsers.add_parser("db-preflight-compatibility-result")
    db_preflight_compat.add_argument("--result-json", type=Path, required=True)
    db_preflight_compat.add_argument("--result-digest", required=True)
    db_preflight_compat.set_defaults(handler=handle_db_preflight_compatibility_result)

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
    baseline_build.add_argument("--image-name", help="Stratify storage/NCCL by image")
    baseline_build.add_argument("--node", help="Restrict storage/NCCL to one node")
    baseline_build.add_argument("--test-plan", help="Stratify DL by test plan")
    baseline_build.add_argument("--source-db", help="Override source DB (storage/NCCL)")
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
        "--source-db", help="Override source result DB (storage/NCCL)"
    )
    baseline_classify.add_argument(
        "--db-path",
        help="Override baseline DB; defaults to baseline_root_path DBs",
    )
    baseline_classify.add_argument(
        "--store-results",
        action="store_true",
        help="Persist classification decisions to classification-results.db",
    )
    baseline_classify.add_argument(
        "--classification-db-path",
        help="Override classification-results DB path",
    )
    baseline_classify.add_argument("--output", choices=["table", "json"], default="table")
    baseline_classify.set_defaults(handler=handle_baseline_classify)

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
    """Plan or safely create one disabled pass/fail-only test directory."""

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


def handle_compatibility_inventory(args: argparse.Namespace) -> int:
    """Print the central immutable compatibility inventory without I/O."""

    from cval.validation.compatibility import compatibility_inventory

    payload = compatibility_inventory()
    if args.output == "json":
        print(json.dumps(payload, indent=2))
        return 0
    print(
        f"Compatibility surfaces: {len(payload['surfaces'])} | "
        "removal eligible: false"
    )
    print(f"{'SURFACE':<34} {'CATEGORY':<18} BLOCKERS")
    for surface in payload["surfaces"]:
        print(
            f"{surface['surface_id']:<34} {surface['category']:<18} "
            + ",".join(surface["blockers"])
        )
    return 0


def handle_compatibility_audit(args: argparse.Namespace) -> int:
    """Run the bounded, explicit-input-only offline compatibility audit."""

    from cval.validation.compatibility import audit_compatibility_inputs

    try:
        payload = audit_compatibility_inputs(args.inputs)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"Compatibility audit error: {exc}", file=sys.stderr)
        return 2
    if args.output == "json":
        print(json.dumps(payload, indent=2))
        return 0
    observed = sum(1 for surface in payload["surfaces"] if surface["observed"])
    print(
        f"Offline compatibility audit: {len(payload['inputs'])} explicit inputs, "
        f"{payload['total_bytes']} bytes, {observed} observed surfaces"
    )
    print("Removal eligible: false")
    print("Global blockers: " + ", ".join(payload["global_blockers"]))
    for surface in payload["surfaces"]:
        marker = "observed" if surface["observed"] else "not-observed"
        print(f"  {surface['surface_id']}: {marker}; " + ", ".join(surface["blockers"]))
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


def handle_history(args: argparse.Namespace) -> int:
    """Read normalized node run history without mutating SQLite."""

    from cval.storage.run_history import (
        get_run_history_rows,
        run_history_rows_to_dicts,
    )

    rows = get_run_history_rows(
        pod=args.pod,
        namespace=args.namespace,
        db_path=args.db_path,
        run_id=args.run_id,
        node=args.node,
        test_id=args.test,
        status=args.status,
        limit=args.limit,
        config=args.cval_config,
    )
    if args.output == "json":
        print(json.dumps(run_history_rows_to_dicts(rows), indent=2))
        return 0

    print(f"Node run history rows: {len(rows)}")
    print(f"{'STARTED':>12} {'NODE':<30} {'STATUS':<10} {'TESTS':<28} RUN_ID")
    for row in rows:
        print(
            f"{row.started_timestamp:>12} {row.node:<30} "
            f"{row.overall_status:<10} {row.tests_ran:<28} {row.run_id}"
        )
    return 0


def handle_plan(args: argparse.Namespace) -> int:
    """Build and print a dry-run workflow plan."""

    from cval.orchestrator.workflow import workflow_plan_to_dict

    plan = _build_plan_from_args(args)

    if args.output == "json":
        print(
            json.dumps(
                workflow_plan_to_dict(plan, include_yaml=args.include_yaml),
                indent=2,
            )
        )
        return 0

    print("Dry-run workflow plan")
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
    """Dry-run or explicitly submit a planned validation batch."""

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

    mode = "submitted" if args.submit else "dry-run"
    print(f"Validation job submission plan ({mode})")
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
        export_compatibility_rows,
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
    classifications = []
    if not args.no_classification:
        classifications = get_latest_classification_rows(
            pod=args.pod,
            namespace=args.namespace,
            db_path=args.classification_db_path,
            config=args.cval_config,
        )

    test = args.test.lower()
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
        export = export_compatibility_rows(args.cval_config, test, context)
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
        config=args.cval_config,
    )
    if args.test == "all":
        enabled_targets = set(catalog.names_for(CLASSIFICATIONS_EXPORT))
        rows = [row for row in rows if row.test_type in enabled_targets]
    selected = filter_classification_rows(rows, args.test)
    output_path = write_classifications_csv(rows, args.test, output_dir=args.output_dir)
    print(f"Wrote {len(selected)} {args.test} classification row(s) to {output_path}")
    return 0


def handle_health_evaluate(args: argparse.Namespace) -> int:
    """Run one local/PVC-only evaluator cycle without Kubernetes access."""

    from cval.health.evaluator import evaluate_health_cycle

    try:
        report = evaluate_health_cycle(
            args.cval_config,
            apply=args.apply,
            confirmation=args.confirm,
        )
    except Exception as exc:  # noqa: BLE001 - public CLI safety boundary
        message = " ".join((str(exc).strip() or exc.__class__.__name__).splitlines())
        if args.output == "json":
            print(json.dumps({"ok": False, "error": message}, indent=2))
        else:
            print(f"Health evaluator error: {message}", file=sys.stderr)
        return 2
    if args.output == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(
            f"Health evaluator cycle: {report.mode} | tests={len(report.tests)} | "
            f"ok={str(report.ok).lower()}"
        )
        for test in report.tests:
            print(
                f"  {test.test_id:<18} {test.status:<9} results={test.result_count:<4} "
                f"candidate_sources={test.candidate_source_count:<4} "
                f"classifications={test.classification_selected_count:<3} "
                f"deferred={test.deferred_count:<4} "
                f"backlog={test.classification_backlog:<4} "
                f"remaining={test.classification_remaining:<4} "
                f"truncated={str(test.classification_truncated).lower()}"
            )
            print(
                f"    migrated_to_v2={str(test.migrated_to_v2).lower()} "
                f"candidates={len(test.candidates)} "
                f"candidate_inserted={test.candidates_inserted} "
                f"candidate_idempotent={test.candidates_idempotent} "
                f"history_inserted={test.history_inserted} "
                f"history_idempotent={test.history_idempotent} "
                f"partial_durable_writes={str(test.partial_writes).lower()}"
            )
            if test.error:
                print(
                    f"    stage={test.error_stage or 'unknown'} "
                    f"atomicity={test.write_atomicity}: {test.error}"
                )
    return 0 if report.ok else 1


def handle_health_activate(args: argparse.Namespace) -> int:
    """Preflight or explicitly activate one U8 candidate."""

    from cval.health.evaluator import activate_health_candidate

    try:
        report = activate_health_candidate(
            args.cval_config,
            args.test_id,
            args.baseline_id,
            apply=args.apply,
            confirmation=args.confirm,
        )
    except Exception as exc:  # noqa: BLE001 - public CLI safety boundary
        message = " ".join((str(exc).strip() or exc.__class__.__name__).splitlines())
        if args.output == "json":
            print(json.dumps({"ok": False, "error": message}, indent=2))
        else:
            print(f"Health activation error: {message}", file=sys.stderr)
        return 2
    if args.output == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(
            f"Health activation {report.mode}: {report.baseline_id} | "
            f"state={report.lifecycle} ready={str(report.activation_ready).lower()} "
            f"activated={str(report.activated).lower()}"
        )
    return 0


def handle_evaluator_preflight(args: argparse.Namespace) -> int:
    """Emit one read-only U11 deployment-preflight JSON object."""

    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            from cval.evaluator.preflight import run_deployment_preflight

            config = args.cval_config
            if args.state_root is not None:
                config = replace(
                    config,
                    health_evaluator=replace(
                        config.health_evaluator,
                        state_root=str(args.state_root),
                    ),
                )
            report = run_deployment_preflight(config, access=args.access)
        _print_strict_json(report)
        return 0 if report["ok"] else 1
    except KeyboardInterrupt:
        raise
    except BaseException as exc:  # noqa: BLE001 - strict machine boundary
        _print_strict_evaluator_error("evaluator preflight", exc)
        return 2


def handle_evaluator_parity(args: argparse.Namespace) -> int:
    """Emit one deterministic shadow-parity JSON object from copied inputs."""

    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            from cval.evaluator.parity import build_shadow_parity_report

            report = build_shadow_parity_report(
                u8_json_paths=args.u8_json,
                u8_db_paths=args.u8_db,
                compatibility_json_paths=args.compatibility_json,
                compatibility_db_paths=args.compatibility_db,
                registered_test_ids=(
                    registered.id
                    for registered in args.cval_config.tests.registry.tests
                ),
            )
        _print_strict_json(report)
        return 0
    except KeyboardInterrupt:
        raise
    except BaseException as exc:  # noqa: BLE001 - strict machine boundary
        _print_strict_evaluator_error("evaluator parity", exc)
        return 2


def handle_evaluator_backup(args: argparse.Namespace) -> int:
    """Plan or execute a separately gated backup of a disposable local copy."""

    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            from cval.evaluator.backup import backup_local_evaluator_state

            report = backup_local_evaluator_state(
                args.cval_config,
                source_root=args.source_root,
                destination=args.destination,
                apply=args.apply,
                confirmation=args.confirm,
            )
        _print_strict_json(report)
        return 0 if report["ok"] else 1
    except KeyboardInterrupt:
        raise
    except BaseException as exc:  # noqa: BLE001 - strict machine boundary
        _print_strict_evaluator_error("evaluator backup", exc)
        return 2


def handle_evaluator_service(args: argparse.Namespace) -> int:
    """Run one startup-verified evaluator cycle and emit one stdout envelope."""

    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            from cval.evaluator.service import run_evaluator_service

            report = run_evaluator_service(
                args.cval_config,
                apply=args.apply,
                confirmation=args.confirm,
                write_enabled=args.write_enabled,
                expected_commit=args.expected_commit,
                image_ref=args.image_ref,
            )
            exit_code = int(report["exit_code"])
        _print_strict_json(report)
        return exit_code
    except KeyboardInterrupt:
        raise
    except BaseException as exc:  # noqa: BLE001 - strict machine boundary
        category = "SystemExit" if isinstance(exc, SystemExit) else exc.__class__.__name__
        _print_strict_json(
            {"ok": False, "error": f"evaluator service failed ({category})"}
        )
        return 2


def handle_validate(args: argparse.Namespace) -> int:
    """Submit one node, live-track it, classify the fresh result, and report."""
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
        skip_dl_rebuild=args.skip_dl_rebuild,
        dl_rebuild_timeout=args.dl_rebuild_timeout,
        dl_lock_wait=args.dl_lock_wait,
        pod=args.pvc_pod,
        pod_repo_dir=args.pod_repo_dir,
        pod_config_path=args.pod_config,
        window_days=args.window_days,
        dry_run=args.dry_run,
        download=args.download,
        download_dir=args.download_dir,
        verbose=(args.output == "table"),
    )
    if args.output == "json":
        print(json.dumps(report, indent=2))
    if report.get("dry_run"):
        return 0
    return 0 if report.get("ok", False) else 1


def _compatibility_write_authorization(args: argparse.Namespace):
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
        raise ValueError("Compatibility writer identity does not match validated result")
    for argument_name, result_value in (
        ("image_name", result.image_name),
        ("pytorch_version", result.pytorch_version),
        ("cuda_version", result.cuda_version),
    ):
        if hasattr(args, argument_name) and getattr(args, argument_name) != result_value:
            raise ValueError(
                f"Compatibility writer {argument_name} does not match validated result"
            )
    if (
        isinstance(result, ValidationResultV2)
        and hasattr(args, "run_id")
        and args.run_id
        and args.run_id != result.run_id
    ):
        raise ValueError("Compatibility writer run_id does not match validated result")
    return authorization


def _require_v2_db_target(authorization, db_path: str | Path, config_field: str) -> None:
    if not isinstance(authorization.result, ValidationResultV2):
        return
    expected = Path(getattr(authorization.config.storage, config_field)).expanduser()
    if Path(db_path).expanduser() != expected:
        raise ValueError(
            f"Compatibility DB target does not match snapshot storage.{config_field}"
        )


def handle_db_add_result(args: argparse.Namespace) -> int:
    """Append one validation result row to the main SQLite DB."""

    authorization = _compatibility_write_authorization(args)
    values = validation_result_to_env(authorization.result)
    expected_result = (
        values["overall_result"]
        if args.test == "all"
        else authorization.result.tests.get(args.test).status
        if args.test in authorization.result.tests
        else None
    )
    if expected_result != args.result:
        raise ValueError("Compatibility status row does not match validated result")
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
    """Atomically append fixed compatibility status rows for one run."""

    authorization = _compatibility_write_authorization(args)
    projected = validation_result_to_env(authorization.result)
    from cval.validation.compatibility import (
        LEGACY_AGGREGATE_TEST_ID,
        LEGACY_TEST_PROJECTIONS,
        project_legacy_statuses,
    )

    expected_results = project_legacy_statuses(projected)
    actual_results = {
        item.test_id: getattr(args, f"{item.test_id}_result")
        for item in LEGACY_TEST_PROJECTIONS
    } | {
        LEGACY_AGGREGATE_TEST_ID: args.overall_result,
    }
    if actual_results != expected_results:
        raise ValueError("Compatibility status set does not match validated result")
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


def handle_db_upsert_run_history(args: argparse.Namespace) -> int:
    """Persist one validated v2 result into normalized run history."""

    from cval.storage.run_history import ingest_run_history_file

    run_id = ingest_run_history_file(
        args.result_json,
        db_path=args.db_path,
        config=args.cval_config,
        result_digest=args.result_digest,
        config_snapshot_b64=os.environ.get("CVAL_CONFIG_SNAPSHOT_B64", ""),
    )
    print(f"Upserted node run history: {run_id}")
    return 0


def handle_db_ingest_test_results(args: argparse.Namespace) -> int:
    """Persist common raw rows and declared metrics from one finalized v2 run."""

    from cval.validation.ingestion import ingest_test_results_file

    try:
        report = ingest_test_results_file(
            args.result_json,
            config=args.cval_config,
            result_digest=args.result_digest,
            config_snapshot_b64=os.environ.get("CVAL_CONFIG_SNAPSHOT_B64", ""),
        )
    except Exception as exc:  # noqa: BLE001 - hidden in-pod command boundary
        print(f"Modular per-test ingestion failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 0 if report.ok else 1


def handle_db_preflight_test_results(args: argparse.Namespace) -> int:
    """Validate modular evidence/targets without creating or changing files."""

    from cval.validation.ingestion import preflight_test_results_file

    try:
        run_id = preflight_test_results_file(
            args.result_json,
            config=args.cval_config,
            result_digest=args.result_digest,
            config_snapshot_b64=os.environ.get("CVAL_CONFIG_SNAPSHOT_B64", ""),
        )
    except Exception as exc:  # noqa: BLE001 - hidden in-pod command boundary
        print(f"Modular per-test preflight failed: {exc}", file=sys.stderr)
        return 1
    print(f"Modular per-test preflight passed: {run_id}")
    return 0


def handle_db_preflight_compatibility_result(args: argparse.Namespace) -> int:
    """Validate v1/v2 compatibility provenance and targets without writing."""

    from cval.storage.write_provenance import authorize_result_write

    try:
        authorization = authorize_result_write(
            args.result_json,
            result_digest=args.result_digest,
            config_snapshot_b64=os.environ.get("CVAL_CONFIG_SNAPSHOT_B64", ""),
            config=args.cval_config,
        )
    except Exception as exc:  # noqa: BLE001 - hidden in-pod boundary
        print(f"Compatibility write preflight failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"Compatibility write preflight passed: "
        f"{authorization.result.node}-{authorization.result.timestamp}"
    )
    return 0


def handle_db_add_storage_result(args: argparse.Namespace) -> int:
    """Parse storage artifacts and write one storage metrics row."""

    authorization = _compatibility_write_authorization(args)
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

    authorization = _compatibility_write_authorization(args)
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
    from cval.validation.operations import build_compatibility_baseline

    config: CvalConfig = args.cval_config
    try:
        record = build_compatibility_baseline(
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
    except FileNotFoundError as exc:
        print(f"Source DB not found: {exc}", file=sys.stderr)
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

    if not activate_baseline(
        args.baseline_id,
        args.test_type,
        db_path=args.db_path,
        config=args.cval_config,
    ):
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
        classify_compatibility_target,
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

    verdicts = classify_compatibility_target(
        config,
        args.test_type,
        baseline,
        node=args.node,
        source_db=args.source_db,
        window_days=args.window_days,
    )

    stored_count = 0
    if args.store_results:
        stored_count = store_classification_results(
            verdicts,
            db_path=args.classification_db_path,
            config=config,
        )

    if args.output == "json":
        payload = {
            "verdicts": verdicts,
            "stored_count": stored_count,
            "classification_db_path": str(
                args.classification_db_path or default_classification_db_path(config)
            ) if args.store_results else "",
        }
        print(json.dumps(payload if args.store_results else verdicts, indent=2))
        return 0

    print(f"Classification vs baseline {baseline.get('baseline_id')} ({args.test_type})")
    if args.store_results:
        target_db = args.classification_db_path or default_classification_db_path(config)
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


def _single_line_error(exc: BaseException) -> str:
    return " ".join((str(exc).strip() or exc.__class__.__name__).splitlines())


def _print_strict_json(value: object) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))


def _print_strict_evaluator_error(operation: str, exc: BaseException) -> None:
    if isinstance(exc, Exception):
        message = _single_line_error(exc)
    else:
        category = "SystemExit" if isinstance(exc, SystemExit) else "BaseException"
        message = f"{operation} failed ({category})"
    _print_strict_json({"ok": False, "error": message})


def _parse_cli_arguments(parser_call, *, strict_json: bool):
    if not strict_json:
        return parser_call()
    errors = io.StringIO()
    try:
        with redirect_stderr(errors):
            return parser_call()
    except SystemExit as exc:
        if exc.code == 0:
            raise
        lines = [line.strip() for line in errors.getvalue().splitlines() if line.strip()]
        message = lines[-1] if lines else "invalid evaluator command arguments"
        if message.startswith("cval: error: "):
            message = message.removeprefix("cval: error: ")
        _print_strict_json({"ok": False, "error": message})
        return None


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
