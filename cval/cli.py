"""Command-line interface for c-val 2.0.

This file is the main human/Hermes entry point. It exposes read-only status and
discovery commands, dry-run planning, approval-gated submission, read-only
monitoring, and structured result inspection. Handlers are intentionally thin:
they parse arguments, call package modules, and format output.

Public commands: config, status, nodes, plan, run, jobs, result.
The db-add-* commands are in-pod ingestion hooks and stay out of --help.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from cval.config import CvalConfig, config_to_dict, load_config
from cval.jobs.manager import submission_result_to_dict, submit_workflow_plan
from cval.jobs.monitor import get_job_phases, monitored_jobs_to_dict, monitor_jobs_until_terminal
from cval.k8s.discovery import discover_free_nodes, fully_free_node_names
from cval.orchestrator.workflow import build_workflow_plan, workflow_plan_to_dict
from cval.policy import ExecutionPolicy, PolicyViolation
from cval.storage.status import (
    get_latest_status_rows,
    latest_status_rows_to_node_map,
    latest_status_rows_to_tsv,
    parse_latest_status_tsv,
)
from cval.storage.ingest import (
    add_nccl_result,
    add_storage_result,
    add_validation_result,
)
from cval.validation.results import load_validation_result, validation_result_to_env_lines


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments, dispatch to a handler, and return a process code."""

    raw_argv = sys.argv[1:] if argv is None else argv
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", type=Path)
    config_args, _ = bootstrap.parse_known_args(raw_argv)
    config = load_config(config_args.config)

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


def build_parser(config: CvalConfig | None = None) -> argparse.ArgumentParser:
    """Build the top-level parser and subcommands."""

    active_config = config or load_config()
    parser = argparse.ArgumentParser(prog="cval", description="c-val orchestration CLI")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to c-val TOML config; defaults to config/cval.toml or CVAL_CONFIG",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar="{config,status,nodes,plan,run,jobs,result}",
    )

    show_config = subparsers.add_parser("config", help="Print the effective c-val config")
    show_config.add_argument("--output", choices=["json"], default="json")
    show_config.set_defaults(handler=handle_config)

    nodes = subparsers.add_parser("nodes", help="List schedulable GPU nodes and free capacity")
    nodes.add_argument(
        "--node-filter",
        default=active_config.cluster.node_filter,
        help="Substring filter for GPU nodes",
    )
    nodes.add_argument("--output", choices=["table", "json"], default="table")
    nodes.set_defaults(handler=handle_nodes)

    status = subparsers.add_parser(
        "status",
        help="Read latest validation status from the PVC access pod",
    )
    status.add_argument("--pod", default=active_config.cluster.pvc_access_pod)
    status.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    status.add_argument("--db-path", default=active_config.storage.validation_db_path)
    status.add_argument("--output", choices=["table", "json", "tsv"], default="table")
    status.set_defaults(handler=handle_status)

    plan = subparsers.add_parser("plan", help="Build and print a dry-run validation plan")
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

    # In-pod ingestion commands; added without `help` so they stay out of --help.
    db_add_result = subparsers.add_parser("db-add-result")
    db_add_result.add_argument("node")
    db_add_result.add_argument("test")
    db_add_result.add_argument("result", choices=["pass", "fail", "incomplete"])
    db_add_result.add_argument("timestamp")
    db_add_result.add_argument("--db-path", default=active_config.storage.validation_db_path)
    db_add_result.set_defaults(handler=handle_db_add_result)

    db_add_storage = subparsers.add_parser("db-add-storage-result")
    db_add_storage.add_argument("node")
    db_add_storage.add_argument("timestamp")
    db_add_storage.add_argument("results_dir", type=Path)
    db_add_storage.add_argument("--db-path", default=active_config.storage.storage_db_path)
    db_add_storage.set_defaults(handler=handle_db_add_storage_result)

    db_add_nccl = subparsers.add_parser("db-add-nccl-result")
    db_add_nccl.add_argument("node")
    db_add_nccl.add_argument("timestamp")
    db_add_nccl.add_argument("busbw")
    db_add_nccl.add_argument("latency")
    db_add_nccl.add_argument("--db-path", default=active_config.storage.nccl_db_path)
    db_add_nccl.set_defaults(handler=handle_db_add_nccl_result)

    # Baseline commands (read-only and ingestion)
    baseline = subparsers.add_parser("baseline", help="Manage baselines and peer comparison")
    baseline_sub = baseline.add_subparsers(dest="baseline_command", required=True)

    baseline_list = baseline_sub.add_parser("list", help="List stored baselines")
    baseline_list.add_argument("--test-type", choices=["nccl", "storage", "dltest"])
    baseline_list.add_argument("--db-path", default=active_config.storage.validation_db_path)
    baseline_list.add_argument("--output", choices=["table", "json"], default="table")
    baseline_list.set_defaults(handler=handle_baseline_list)

    baseline_load = baseline_sub.add_parser("load", help="Load baseline summary from directory")
    baseline_load.add_argument("baseline_dir", type=Path, help="Baseline directory path")
    baseline_load.add_argument("test_type", choices=["nccl", "storage", "dltest"])
    baseline_load.add_argument("--output", choices=["json"], default="json")
    baseline_load.set_defaults(handler=handle_baseline_load)

    baseline_ingest = baseline_sub.add_parser("ingest", help="Store baseline in DB")
    baseline_ingest.add_argument("baseline_dir", type=Path, help="Baseline directory path")
    baseline_ingest.add_argument("test_type", choices=["nccl", "storage", "dltest"])
    baseline_ingest.add_argument("--db-path", default=active_config.storage.validation_db_path)
    baseline_ingest.set_defaults(handler=handle_baseline_ingest)

    baseline_compare = baseline_sub.add_parser("compare", help="Compare result vs. baseline")
    baseline_compare.add_argument("baseline_id", help="Baseline ID to compare against")
    baseline_compare.add_argument("test_type", choices=["nccl", "storage", "dltest"])
    baseline_compare.add_argument("--result-json", type=Path, help="Result JSON to compare")
    baseline_compare.add_argument("--db-path", default=active_config.storage.validation_db_path)
    baseline_compare.add_argument("--output", choices=["json", "table"], default="table")
    baseline_compare.set_defaults(handler=handle_baseline_compare)

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


def handle_status(args: argparse.Namespace) -> int:
    """Read latest validation status without mutating SQLite metadata."""

    rows = get_latest_status_rows(
        pod=args.pod,
        namespace=args.namespace,
        db_path=args.db_path,
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
    """Build and print a dry-run workflow plan."""

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


def handle_db_add_result(args: argparse.Namespace) -> int:
    """Append one validation result row to the main SQLite DB."""

    timestamp = add_validation_result(
        args.node,
        args.test,
        args.result,
        args.timestamp,
        db_path=args.db_path,
    )
    print(f"Added validation result: {args.node} {args.test} {args.result} {timestamp}")
    return 0


def handle_db_add_storage_result(args: argparse.Namespace) -> int:
    """Parse storage artifacts and write one storage metrics row."""

    timestamp = add_storage_result(
        args.node,
        args.timestamp,
        args.results_dir,
        db_path=args.db_path,
    )
    print(f"Added storage result: {args.node} {timestamp}")
    return 0


def handle_db_add_nccl_result(args: argparse.Namespace) -> int:
    """Write one NCCL metric row."""

    timestamp = add_nccl_result(
        args.node,
        args.timestamp,
        args.busbw,
        args.latency,
        db_path=args.db_path,
    )
    print(f"Added NCCL result: {args.node} {timestamp}")
    return 0


def handle_baseline_list(args: argparse.Namespace) -> int:
    """List stored baselines in the validation DB."""
    from cval.baselines.storage import list_baselines

    baselines = list_baselines(test_type=args.test_type, db_path=args.db_path)
    if args.output == "json":
        print(
            json.dumps(
                [{"baseline_id": b[0], "test_type": b[1], "timestamp": b[2]} for b in baselines],
                indent=2,
            )
        )
        return 0

    print(f"Stored baselines: {len(baselines)}")
    print(f"{'BASELINE_ID':<40} {'TEST_TYPE':<12} TIMESTAMP")
    for baseline_id, test_type, timestamp in baselines:
        print(f"{baseline_id:<40} {test_type:<12} {timestamp}")
    return 0


def handle_baseline_load(args: argparse.Namespace) -> int:
    """Load and display a baseline summary from a directory."""
    from cval.baselines.ingest import load_baseline_summary

    baseline = load_baseline_summary(args.baseline_dir, args.test_type)
    if not baseline:
        print(f"Baseline not found: {args.baseline_dir}")
        return 1

    print(json.dumps(asdict(baseline), indent=2))
    return 0


def handle_baseline_ingest(args: argparse.Namespace) -> int:
    """Ingest a baseline from a directory into the validation DB."""
    from cval.baselines.ingest import load_baseline_summary
    from cval.baselines.storage import store_baseline

    baseline = load_baseline_summary(args.baseline_dir, args.test_type)
    if not baseline:
        print(f"Baseline not found: {args.baseline_dir}")
        return 1

    store_baseline(baseline, db_path=args.db_path, test_type=args.test_type)
    print(f"Ingested baseline: {baseline.baseline_id} ({args.test_type}) at {args.baseline_dir}")
    return 0


def handle_baseline_compare(args: argparse.Namespace) -> int:
    """Compare a result against a baseline and output classification."""
    from cval.baselines.ingest import classify_result_vs_baseline
    from cval.baselines.storage import load_baseline_from_db

    baseline = load_baseline_from_db(args.baseline_id, args.test_type, db_path=args.db_path)
    if not baseline:
        print(f"Baseline not found: {args.baseline_id} ({args.test_type})")
        return 1

    result_dict = {}
    if args.result_json:
        result = load_validation_result(args.result_json)
        # Convert result to dict based on test type
        if args.test_type == "dltest":
            result_dict = asdict(result) if hasattr(result, "__dataclass_fields__") else result
        else:
            result_dict = asdict(result) if hasattr(result, "__dataclass_fields__") else result

    classification = classify_result_vs_baseline(result_dict, baseline)

    if args.output == "json":
        print(json.dumps(classification, indent=2))
        return 0

    print(f"Classification: {classification['status'].upper()}")
    print(f"Test type: {classification['test_type']}")
    print(f"Violations: {len(classification['violations'])}")
    for violation in classification['violations']:
        print(
            f"  {violation['metric']}: expected={violation['expected']}, "
            f"actual={violation['actual']}, diff={violation['pct_diff']:.2f}%"
        )
    return 0


def _build_plan_from_args(args: argparse.Namespace):
    """Resolve status inputs, discover nodes if needed, and build a workflow plan."""

    db_status = _load_db_status(args)
    if args.free_nodes:
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
