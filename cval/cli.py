"""Command-line interface for c-val.

This file is the main human/Hermes entry point. It exposes read-only status and
discovery commands, nonmutating queue inspection, approval-gated cluster
validation, read-only monitoring, and structured result inspection. Handlers are intentionally thin:
they parse arguments, call package modules, and format output.

Public commands: config, tests, nodes, validate, status, plan, run, jobs, and results.
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
from cval.jobs.manager import JobSubmissionError, submission_result_to_dict, submit_workflow_plan
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
)
from cval.storage.ingest import (
    add_nccl_health_from_summary,
    add_storage_result,
    add_validation_run_results,
    parse_timestamp,
)
from cval.validation.results import (
    ValidationResultV2,
    validation_result_to_env,
)
from cval.validation.operational_targets import (
    RESULTS_EXPORT,
    build_operational_target_catalog,
)


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments, dispatch to a handler, and return a process code."""

    raw_argv = sys.argv[1:] if argv is None else argv
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
            if not snapshot
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
    settings = {
        "node_filter": config.cluster.node_filter,
        "threshold_days": config.scheduling.days_threshold,
        "timeout_seconds": config.monitoring.timeout_seconds,
        "poll_interval": config.monitoring.validation_poll_seconds,
        "poll_interval_seconds": config.monitoring.poll_interval_seconds,
        "pending_timeout": config.monitoring.validation_pending_seconds,
    }
    if args.command == "run":
        settings["batch_size"] = config.scheduling.batch_size
    for name, expected in settings.items():
        if hasattr(args, name) and getattr(args, name) != expected:
            parser.error(f"--{name.replace('_', '-')} disagrees with TOML; change the reviewed config")
    os.environ["CVAL_CONFIG_SNAPSHOT_B64"] = encode_config_snapshot(config)
    try:
        if args.command.startswith("db-"):
            from cval.storage.retry import retry_sqlite

            return retry_sqlite(
                lambda: args.handler(args), config.sqlite_retry
            )
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


def build_parser(config: CvalConfig | None = None) -> argparse.ArgumentParser:
    """Build the top-level parser and subcommands."""

    active_config = config or load_config()
    target_catalog = build_operational_target_catalog(active_config.tests.registry)
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
            "{config,tests,nodes,validate,status,plan,run,jobs,results}"
        ),
    )

    show_config = subparsers.add_parser("config", help="Print the effective c-val config")
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
        "--poll-interval", type=float, default=active_config.monitoring.validation_poll_seconds, help="Live status poll seconds"
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
        default=active_config.monitoring.validation_pending_seconds,
        help="Warn if the job is still Pending after N seconds (it stays queued)",
    )
    validate.add_argument("--output", choices=["table", "json"], default="table")
    validate.set_defaults(handler=handle_validate)

    status = subparsers.add_parser(
        "status",
        help="Read latest validation status from the PVC access pod",
    )
    status.add_argument("--output", choices=["table", "json"], default="table")
    status.add_argument("--coverage", action="store_true", help="Read complete-run scheduling coverage and provenance limits")
    status.set_defaults(handler=handle_status)

    plan = subparsers.add_parser(
        "plan", help="Inspect prioritized candidates without rendering jobs"
    )
    _add_plan_inputs(plan, active_config)
    plan.add_argument("--output", choices=["table", "json"], default="table")
    plan.set_defaults(handler=handle_plan)

    run = subparsers.add_parser(
        "run",
        help="Submit a validation batch after exact confirmation",
    )
    _add_plan_inputs(run, active_config)
    run.add_argument("--timestamp", type=int)
    run.add_argument("--submit", action="store_true")
    run.add_argument("--confirm")
    run.add_argument("--output", choices=["table", "json"], default="table")
    run.set_defaults(handler=handle_run)

    jobs = subparsers.add_parser("jobs", help="Read or watch Volcano job phases")
    jobs.add_argument("--jobs", required=True, help="Comma-separated vcjob names")
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

    results = subparsers.add_parser(
        "results",
        help="Export latest per-node results for one test to a local file",
    )
    results.add_argument(
        "--test",
        choices=[
            "overall",
            "all",
            *target_catalog.names_for(RESULTS_EXPORT),
        ],
        required=True,
    )
    results.add_argument("--output-dir", type=Path, default=Path.cwd())
    results.add_argument("--node", help="Exact run node; requires --timestamp")
    results.add_argument("--timestamp", type=int, help="Exact run timestamp; requires --node")
    results.add_argument(
        "--no-metrics",
        action="store_true",
        help="Do not join NCCL/storage metric columns into the CSV",
    )
    results.set_defaults(handler=handle_results)

    # In-pod ingestion commands; added without `help` so they stay out of --help.
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
    db_add_nccl_health.add_argument("--recover-descriptor-ibbw-log", action="store_true")
    db_add_nccl_health.add_argument("--confirm-recovery")
    db_add_nccl_health.add_argument("--require-hca-samples", action="store_true")
    db_add_nccl_health.add_argument("--result-json", type=Path, required=True)
    db_add_nccl_health.add_argument("--result-digest", default="")
    db_add_nccl_health.add_argument("--db-path", default=active_config.storage.nccl_db_path)
    db_add_nccl_health.set_defaults(handler=handle_db_add_nccl_health)

    db_add_dltest = subparsers.add_parser("db-add-dltest-run")
    db_add_dltest.add_argument("node")
    db_add_dltest.add_argument("timestamp")
    db_add_dltest.add_argument("results_root", type=Path)
    db_add_dltest.add_argument("--result-json", type=Path, required=True)
    db_add_dltest.add_argument("--result-digest", required=True)
    db_add_dltest.add_argument("--output", choices=["table", "json"], default="table")
    db_add_dltest.set_defaults(handler=handle_db_add_dltest_run)

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
    db_rebuild_dltest.add_argument(
        "--only-missing",
        action="store_true",
        help="Append or repair evidence absent from any DL metric DB; never purge rows",
    )
    db_rebuild_dltest.add_argument("--output", choices=["table", "json"], default="table")
    db_rebuild_dltest.set_defaults(handler=handle_db_rebuild_dltest_metrics)

    return parser


def handle_nodes(args: argparse.Namespace) -> int:
    """Run read-only GPU node discovery and print table or JSON output."""

    if args.inventory_only:
        names = discover_gpu_node_names(node_name_filter=args.node_filter, config=args.cval_config)
        if args.output == "json":
            print(json.dumps({"nodes": names, "node_count": len(names)}, indent=2))
        else:
            print("\n".join(names))
            print(f"GPU nodes: {len(names)}")
        return 0

    if args.check_node:
        status = describe_node(args.check_node, config=args.cval_config)
        eligible = status.eligible
        payload = asdict(status) | {"eligible": eligible}
        if args.output == "json":
            print(json.dumps(payload, indent=2))
        else:
            print(
                f"{status.name}: {status.status_label} eligible={str(eligible).lower()} "
                f"free={status.free}/{status.allocatable} - {status.reason}"
            )
        return 0

    nodes, totals = discover_free_nodes(node_name_filter=args.node_filter, config=args.cval_config)
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


def handle_status(args: argparse.Namespace) -> int:
    """Read latest validation status without mutating SQLite metadata."""

    if args.coverage:
        from cval.storage.status import get_coverage_report

        report = get_coverage_report(config=args.cval_config)
        if args.output == "json":
            print(json.dumps(report, indent=2))
        else:
            print(f"Coverage: {report['state']} policy={report.get('policy', 'unknown')}")
            for item in report.get("nodes", []):
                print(f"{item['node']:<32} {str(item['coverage_timestamp']):>12} {item['reason']}")
            if report.get("reason"):
                print(report["reason"])
        return 0 if report["state"] in {"queried", "empty"} else 1
    rows = get_latest_status_rows(config=args.cval_config)
    if args.output == "json":
        print(json.dumps([asdict(row) for row in rows], indent=2))
        return 0
    node_map = latest_status_rows_to_node_map(rows)
    print(f"Latest validation status rows: {len(rows)} | nodes: {len(node_map)}")
    print(f"{'NODE':<32} {'TEST':<18} {'TIMESTAMP':>12} RESULT")
    for row in rows:
        timestamp = "" if row.latest_timestamp is None else str(row.latest_timestamp)
        print(f"{row.node:<32} {row.test:<18} {timestamp:>12} {row.result}")
    return 0



def handle_plan(args: argparse.Namespace) -> int:
    """Inspect prioritized candidates without rendering or submitting jobs."""

    from cval.scheduler.priority import build_priority_queue

    if not is_exact_commit(args.git_ref):
        raise PolicyViolation(
            "Plan inspection requires --git-ref to be an exact lowercase "
            "40-hex commit"
        )

    if args.batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    free_nodes, db_status = _load_plan_inputs(args)
    queue = build_priority_queue(
        free_nodes, db_status, days_threshold=args.threshold_days,
        coverage_reasons=getattr(args, "coverage_reasons", None),
    )
    candidates = queue[:args.batch_size]
    if args.output == "json":
        print(
            json.dumps(
                {
                    "batch_size": args.batch_size,
                    "days_threshold": args.threshold_days,
                    "free_nodes_count": len(free_nodes),
                    "queue_count": len(queue),
                    "git_ref": args.git_ref,
                    "candidates": [asdict(candidate) for candidate in candidates],
                },
                indent=2,
            )
        )
        return 0

    print("Validation candidates (read-only; jobs not rendered)")
    print(
        f"Input nodes: {len(free_nodes)} | Queue: {len(queue)} | "
        f"Candidates: {len(candidates)}"
    )
    print(f"Threshold days: {args.threshold_days} | Batch size: {args.batch_size}")
    print(f"{'PRI':>3} {'NODE':<32} REASON")
    for candidate in candidates:
        print(f"{candidate.priority:>3} {candidate.node:<32} {candidate.reason}")
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
        namespace_allowlist=tuple(config.policy.namespace_allowlist),
        max_batch_size=config.policy.max_batch_size,
        confirmation_phrase=config.policy.confirmation_phrase,
    )
    exit_code = 0
    try:
        result = submit_workflow_plan(
            plan,
            config=config,
            namespace=config.cluster.namespace,
            policy=policy,
            submit=args.submit,
            confirmation=args.confirm,
        )
    except JobSubmissionError as exc:
        result = exc.result
        exit_code = 1

    if args.output == "json":
        print(json.dumps(submission_result_to_dict(result), indent=2))
        return exit_code

    print("Validation job submission (stopped)" if exit_code else "Validation job submission (submitted)")
    print(f"Namespace: {result.namespace} | Jobs: {len(result.records)}")
    for record in result.records:
        print(f"  {record.node} -> {record.job_name} [{record.action}]")
        if exit_code and record.action != "submitted":
            print(f"    {record.stdout}")
    return exit_code


def handle_jobs(args: argparse.Namespace) -> int:
    """Read Volcano job phases once, or watch until terminal/timeout."""

    if args.watch:
        jobs = monitor_jobs_until_terminal(
            _parse_csv(args.jobs),
            config=args.cval_config,
            namespace=args.cval_config.cluster.namespace,
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

    phases = get_job_phases(
        _parse_csv(args.jobs), namespace=args.cval_config.cluster.namespace, config=args.cval_config
    )
    if args.output == "json":
        print(json.dumps([asdict(phase) for phase in phases], indent=2))
        return 0

    print(f"{'JOB':<64} PHASE")
    for phase in phases:
        print(f"{phase.job_name:<64} {phase.phase}")
    return 0


def handle_results(args: argparse.Namespace) -> int:
    """Export latest per-node results for one selected test to a local CSV."""
    from cval.storage.results_export import (
        latest_result_rows,
        write_latest_results_csv,
        write_export_rows_csv,
    )
    from cval.validation.operations import (
        export_result_rows,
        resolve_operational_target,
    )
    from cval.validation.plugins import ExportContext

    config: CvalConfig = args.cval_config
    test = args.test.lower()
    rows = get_latest_status_rows(config=config, node=args.node, timestamp=args.timestamp)
    if args.node and not rows:
        raise ValueError(f"empty: no exact status for {args.node} at {args.timestamp}; no CSV written")

    if test in {"overall", "all"}:
        from cval.storage.metrics import get_latest_nccl_metrics, get_latest_storage_metrics

        nccl_metrics = None
        storage_metrics = None
        selected = latest_result_rows(rows, args.test)
        timestamps = {row.node: row.latest_timestamp for row in selected}
        if not args.no_metrics:
            nccl_metrics = get_latest_nccl_metrics(config=config, timestamps=timestamps)
            storage_metrics = get_latest_storage_metrics(config=config, timestamps=timestamps)
        output_path = write_latest_results_csv(
            rows,
            args.test,
            output_dir=args.output_dir,
            nccl_metrics=nccl_metrics,
            storage_metrics=storage_metrics,
        )
        output_label = f"{args.test} {'exact-run' if args.node else 'latest'} result"
    else:
        target = resolve_operational_target(args.cval_config, test, RESULTS_EXPORT)
        registered = args.cval_config.tests.registry.require(target.owner_test_id)
        context = ExportContext(
            target=target,
            definition=registered.definition,
            config=args.cval_config,
            status_rows=tuple(rows),
            pod=config.cluster.pvc_access_pod,
            namespace=config.cluster.namespace,
            source_db_paths=(
                ("nccl", config.storage.nccl_db_path),
                ("storage", config.storage.storage_db_path),
            ),
            include_metrics=not args.no_metrics,
        )
        export = export_result_rows(args.cval_config, test, context)
        output_path = write_export_rows_csv(
            export,
            target.name,
            output_dir=args.output_dir,
        )
        selected = export.rows
        output_label = export.row_label or f"{target.name} latest result"
    print(f"Wrote {len(selected)} {output_label} row(s) to {output_path}")
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

    try:
        report = run_node_validation(
            args.node,
            config=args.cval_config,
            git_ref=args.git_ref,
            timestamp=args.timestamp,
            poll_interval=args.poll_interval,
            overall_timeout=args.timeout_seconds,
            pending_timeout=args.pending_timeout,
            submit=args.submit,
            confirmation=args.confirm,
            verbose=(args.output == "table"),
        )
    except JobSubmissionError as exc:
        report = {"ok": False, "node": args.node, "stage": "submission",
                  "submission": submission_result_to_dict(exc.result), "notes": [str(exc)]}
        if args.output == "table":
            print(json.dumps(report, indent=2))
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

    if args.recover_descriptor_ibbw_log:
        if args.confirm_recovery != "recover-descriptor-ibbw":
            raise ValueError(
                "descriptor IBBW recovery requires exact --confirm-recovery "
                "recover-descriptor-ibbw"
            )
    elif args.confirm_recovery is not None:
        raise ValueError(
            "--confirm-recovery is valid only with --recover-descriptor-ibbw-log"
        )
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
        recover_descriptor_ibbw_log=args.recover_descriptor_ibbw_log,
        require_hca_samples=args.require_hca_samples,
        db_path=args.db_path,
        _authorization=authorization,
    )
    print(f"Added consolidated IB_HEALTH result: {args.node} {timestamp}")
    return 0


def handle_db_add_dltest_run(args: argparse.Namespace) -> int:
    """Ingest one complete passing DL run into all four metric DBs."""

    from cval.storage.dltest_ingest import ingest_dltest_run

    authorization = _raw_write_authorization(args)
    summary = ingest_dltest_run(
        args.results_root,
        node=args.node,
        timestamp=args.timestamp,
        config=args.cval_config,
        _authorization=authorization,
    )
    if args.output == "json":
        print(json.dumps(summary, indent=2))
    else:
        print(
            f"Ingested current DL run from {summary['rank_files']} rank file(s): "
            f"{summary['results_root']}"
        )
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
            only_missing=args.only_missing,
            _authorization=authorization,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.output == "json":
        print(json.dumps(summary, indent=2))
        return 0

    if summary["only_missing"]:
        print(
            f"Scanned {summary['discovered_runs']} DL run(s): "
            f"ingested {summary['runs']}, "
            f"skipped {summary['skipped_existing_runs']} already complete"
        )
    else:
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


def _load_plan_inputs(args: argparse.Namespace) -> tuple[list[str], dict[str, int]]:
    """Load validation history and the node input shared by plan and run."""

    db_status = _load_db_status(args)
    if args.free_nodes is not None:
        # Explicit node lists are useful for controlled one-node submissions.
        free_nodes = _parse_csv(args.free_nodes)
    else:
        # No explicit nodes means live read-only discovery is part of the plan.
        nodes, _ = discover_free_nodes(node_name_filter=args.node_filter, config=args.cval_config)
        free_nodes = fully_free_node_names(nodes)

    return free_nodes, db_status


def _build_plan_from_args(args: argparse.Namespace):
    """Render the submission batch from the resolved node and history inputs."""

    from cval.orchestrator.workflow import build_workflow_plan

    free_nodes, db_status = _load_plan_inputs(args)
    return build_workflow_plan(
        free_nodes,
        db_status,
        days_threshold=args.threshold_days,
        batch_size=args.batch_size,
        timestamp=args.timestamp,
        git_ref=args.git_ref,
        config=args.cval_config,
        coverage_reasons=getattr(args, "coverage_reasons", None),
    )


def _add_plan_inputs(parser: argparse.ArgumentParser, config: CvalConfig) -> None:
    """Attach shared plan-building arguments to plan-like commands."""

    parser.add_argument(
        "--free-nodes",
        help="Comma-separated free node names; discovers live nodes if omitted",
    )
    parser.add_argument("--db-status-json", type=Path, help="JSON object mapping node to timestamp")
    parser.add_argument(
        "--live-status",
        action="store_true",
        help="Read latest status from the PVC access pod in read-only mode",
    )
    parser.add_argument("--threshold-days", type=float, default=config.scheduling.days_threshold)
    parser.add_argument("--batch-size", type=int, default=config.scheduling.batch_size)
    parser.add_argument("--git-ref", default=config.job.git_ref)
    parser.add_argument("--node-filter", default=config.cluster.node_filter, help="Live discovery substring filter")


def _parse_csv(value: str) -> list[str]:
    """Parse comma-separated CLI values while ignoring empty items."""

    return [item.strip() for item in value.split(",") if item.strip()]



def _load_db_status(args: argparse.Namespace) -> dict[str, int]:
    """Load validation history from JSON, live status, or empty defaults."""

    def from_coverage(report):
        from cval.storage.status import coverage_timestamps

        timestamps = coverage_timestamps(report, args.cval_config)
        args.coverage_reasons = {item["node"]: "no-complete-coverage" for item in report["nodes"]
                                 if item["coverage_timestamp"] is None and item["state"] == "queried"}
        return timestamps

    selected = sum(1 for source in (args.db_status_json, args.live_status) if source)
    if selected > 1:
        raise ValueError("Use only one of --db-status-json or --live-status")
    if args.live_status:
        from cval.storage.status import get_coverage_report

        return from_coverage(get_coverage_report(config=args.cval_config))
    if args.db_status_json is None:
        # Missing history makes every free node appear never-tested.
        return {}
    data = json.loads(args.db_status_json.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "schema_version" in data:
        return from_coverage(data)
    if not isinstance(data, dict):
        raise ValueError("DB status JSON must be an object mapping node names to timestamps")
    if any(not isinstance(node, str) or not node or type(timestamp) is not int or timestamp < 0 for node, timestamp in data.items()):
        raise ValueError("Status map must contain exact node names and nonnegative integer timestamps")
    return data


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
