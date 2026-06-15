"""Command-line interface for c-val 2.0.

This file is the main human/Hermes entry point. It exposes read-only status and
discovery commands, dry-run planning, approval-gated submission, read-only
monitoring, and structured result inspection. Handlers are intentionally thin:
they parse arguments, call package modules, and format output.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from cval.config import CvalConfig, config_to_dict, load_config
from cval.jobs.renderer import render_validation_job_from_file
from cval.jobs.manager import submission_result_to_dict, submit_workflow_plan
from cval.jobs.monitor import get_job_phases, monitored_jobs_to_dict, monitor_jobs_until_terminal
from cval.k8s.discovery import discover_free_nodes, fully_free_node_names
from cval.orchestrator.workflow import build_workflow_plan, workflow_plan_to_dict
from cval.policy import ExecutionPolicy, PolicyViolation
from cval.scheduler.priority import build_priority_queue
from cval.storage.status import (
    DEFAULT_DB_PATH,
    DEFAULT_NAMESPACE,
    DEFAULT_PVC_ACCESS_POD,
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
        metavar="{config,status,nodes,run,jobs,result}",
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

    discover = subparsers.add_parser("discover-free-nodes", help=argparse.SUPPRESS)
    discover.add_argument(
        "--node-filter",
        default=active_config.cluster.node_filter,
        help="Substring filter for GPU nodes",
    )
    discover.add_argument("--output", choices=["table", "json"], default="table")
    discover.set_defaults(handler=handle_nodes)

    status = subparsers.add_parser(
        "status",
        help="Read latest validation status from the PVC access pod",
    )
    status.add_argument("--pod", default=active_config.cluster.pvc_access_pod)
    status.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    status.add_argument("--db-path", default=active_config.storage.validation_db_path)
    status.add_argument("--output", choices=["table", "json", "tsv"], default="table")
    status.set_defaults(handler=handle_status)

    prioritize = subparsers.add_parser("prioritize", help=argparse.SUPPRESS)
    prioritize.add_argument("--free-nodes", required=True, help="Comma-separated free node names")
    prioritize.add_argument(
        "--db-status-json",
        type=Path,
        help="JSON object mapping node to timestamp",
    )
    prioritize.add_argument(
        "--db-status-tsv",
        type=Path,
        help="TSV output from existing latest-status command",
    )
    prioritize.add_argument("--threshold-days", type=float, default=active_config.scheduling.days_threshold)
    prioritize.add_argument("--output", choices=["table", "json"], default="table")
    prioritize.set_defaults(handler=handle_prioritize)

    render = subparsers.add_parser("render-job", help=argparse.SUPPRESS)
    render.add_argument("--node", required=True)
    render.add_argument("--timestamp", type=int)
    render.add_argument("--template", type=Path, default=active_config.job.template_path)
    render.add_argument("--job-prefix", default=active_config.job.job_prefix)
    render.add_argument("--git-repo", default=active_config.job.git_repo)
    render.add_argument("--git-ref", default=active_config.job.git_ref)
    render.add_argument("--output", type=Path, help="Write rendered YAML to this path")
    render.set_defaults(handler=handle_render_job)

    run_batch = subparsers.add_parser("run-batch", help=argparse.SUPPRESS)
    run_batch.add_argument("--nodes", required=True, help="Comma-separated target nodes")
    run_batch.add_argument("--batch-size", type=int, default=active_config.scheduling.batch_size)
    run_batch.add_argument("--timestamp", type=int)
    run_batch.add_argument("--template", type=Path, default=active_config.job.template_path)
    run_batch.add_argument("--job-prefix", default=active_config.job.job_prefix)
    run_batch.add_argument("--git-repo", default=active_config.job.git_repo)
    run_batch.add_argument("--git-ref", default=active_config.job.git_ref)
    run_batch.add_argument("--output", choices=["table", "json"], default="table")
    run_batch.set_defaults(handler=handle_run_batch)

    plan = subparsers.add_parser("plan", help=argparse.SUPPRESS)
    plan.add_argument(
        "--free-nodes",
        help="Comma-separated free node names; discovers live nodes if omitted",
    )
    plan.add_argument("--db-status-json", type=Path, help="JSON object mapping node to timestamp")
    plan.add_argument(
        "--db-status-tsv",
        type=Path,
        help="TSV output from existing latest-status command",
    )
    plan.add_argument(
        "--live-status",
        action="store_true",
        help="Read latest status from the PVC access pod in read-only mode",
    )
    plan.add_argument("--status-pod", default=active_config.cluster.pvc_access_pod)
    plan.add_argument("--status-namespace", default=active_config.cluster.namespace)
    plan.add_argument("--status-db-path", default=active_config.storage.validation_db_path)
    plan.add_argument("--threshold-days", type=float, default=active_config.scheduling.days_threshold)
    plan.add_argument("--batch-size", type=int, default=active_config.scheduling.batch_size)
    plan.add_argument("--timestamp", type=int)
    plan.add_argument("--template", type=Path, default=active_config.job.template_path)
    plan.add_argument("--job-prefix", default=active_config.job.job_prefix)
    plan.add_argument("--git-repo", default=active_config.job.git_repo)
    plan.add_argument("--git-ref", default=active_config.job.git_ref)
    plan.add_argument(
        "--node-filter",
        default=active_config.cluster.node_filter,
        help="Live discovery substring filter",
    )
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

    submit_plan = subparsers.add_parser("submit-plan", help=argparse.SUPPRESS)
    _add_plan_inputs(submit_plan, active_config)
    submit_plan.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    submit_plan.add_argument("--allowed-namespace", action="append")
    submit_plan.add_argument("--max-batch-size", type=int, default=active_config.policy.max_batch_size)
    submit_plan.add_argument("--submit", action="store_true")
    submit_plan.add_argument("--confirm")
    submit_plan.add_argument("--output", choices=["table", "json"], default="table")
    submit_plan.set_defaults(handler=handle_run)

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

    job_status = subparsers.add_parser("job-status", help=argparse.SUPPRESS)
    job_status.add_argument("--jobs", required=True, help="Comma-separated vcjob names")
    job_status.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    job_status.add_argument("--output", choices=["table", "json"], default="table")
    job_status.set_defaults(handler=handle_jobs, watch=False)

    monitor_jobs = subparsers.add_parser("monitor-jobs", help=argparse.SUPPRESS)
    monitor_jobs.add_argument("--jobs", required=True, help="Comma-separated vcjob names")
    monitor_jobs.add_argument("--namespace", "-n", default=active_config.cluster.namespace)
    monitor_jobs.add_argument(
        "--timeout-seconds",
        type=float,
        default=active_config.monitoring.timeout_seconds,
    )
    monitor_jobs.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=active_config.monitoring.poll_interval_seconds,
    )
    monitor_jobs.add_argument("--output", choices=["table", "json"], default="table")
    monitor_jobs.set_defaults(handler=handle_jobs, watch=True)

    result = subparsers.add_parser("result", help="Inspect a structured validation result")
    result.add_argument("--result-json", type=Path, required=True)
    result.add_argument("--output", choices=["env", "json"], default="env")
    result.set_defaults(handler=handle_result)

    result_env = subparsers.add_parser(
        "result-env",
        help=argparse.SUPPRESS,
    )
    result_env.add_argument("--result-json", type=Path, required=True)
    result_env.set_defaults(handler=handle_result, output="env")

    db_add_result = subparsers.add_parser(
        "db-add-result",
        help=argparse.SUPPRESS,
    )
    db_add_result.add_argument("node")
    db_add_result.add_argument("test")
    db_add_result.add_argument("result", choices=["pass", "fail", "incomplete"])
    db_add_result.add_argument("timestamp")
    db_add_result.add_argument("--image-name", default="")
    db_add_result.add_argument("--db-path", default=active_config.storage.validation_db_path)
    db_add_result.set_defaults(handler=handle_db_add_result)

    db_add_storage = subparsers.add_parser(
        "db-add-storage-result",
        help=argparse.SUPPRESS,
    )
    db_add_storage.add_argument("node")
    db_add_storage.add_argument("timestamp")
    db_add_storage.add_argument("results_dir", type=Path)
    db_add_storage.add_argument("--image-name", default="")
    db_add_storage.add_argument("--db-path", default=active_config.storage.storage_db_path)
    db_add_storage.set_defaults(handler=handle_db_add_storage_result)

    db_add_nccl = subparsers.add_parser(
        "db-add-nccl-result",
        help=argparse.SUPPRESS,
    )
    db_add_nccl.add_argument("node")
    db_add_nccl.add_argument("timestamp")
    db_add_nccl.add_argument("busbw")
    db_add_nccl.add_argument("latency")
    db_add_nccl.add_argument("--image-name", default="")
    db_add_nccl.add_argument("--db-path", default=active_config.storage.nccl_db_path)
    db_add_nccl.set_defaults(handler=handle_db_add_nccl_result)

    _hide_subcommands(
        subparsers,
        {
            "discover-free-nodes",
            "prioritize",
            "render-job",
            "run-batch",
            "plan",
            "submit-plan",
            "job-status",
            "monitor-jobs",
            "result-env",
            "db-add-result",
            "db-add-storage-result",
            "db-add-nccl-result",
        },
    )
    return parser


def _hide_subcommands(subparsers: argparse._SubParsersAction, names: set[str]) -> None:
    """Hide compatibility/internal commands from help without disabling them."""

    subparsers._choices_actions = [
        action for action in subparsers._choices_actions if action.dest not in names
    ]


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


def handle_discover_free_nodes(args: argparse.Namespace) -> int:
    """Compatibility wrapper for the old `discover-free-nodes` command."""

    return handle_nodes(args)


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


def handle_prioritize(args: argparse.Namespace) -> int:
    """Build a priority queue from explicit free nodes and status history."""

    db_status = _load_db_status(args.db_status_json, args.db_status_tsv)
    queue = build_priority_queue(
        _parse_csv(args.free_nodes),
        db_status,
        days_threshold=args.threshold_days,
    )
    if args.output == "json":
        print(json.dumps([asdict(candidate) for candidate in queue], indent=2))
        return 0

    print(f"{'PRI':>3} {'NODE':<32} {'LAST_TS':>12} {'AGE_DAYS':>9} REASON")
    for candidate in queue:
        age = "" if candidate.age_days is None else f"{candidate.age_days:.2f}"
        print(
            f"{candidate.priority:>3} {candidate.node:<32} "
            f"{candidate.last_tested_timestamp:>12} {age:>9} {candidate.reason}"
        )
    return 0


def handle_render_job(args: argparse.Namespace) -> int:
    """Render one job manifest locally without submitting it."""

    rendered = render_validation_job_from_file(
        args.template,
        node_name=args.node,
        timestamp=args.timestamp,
        job_prefix=args.job_prefix,
        git_repo=args.git_repo,
        git_ref=args.git_ref,
        cval_config=args.cval_config,
    )
    if args.output:
        # Local file output is useful for manual inspection or `kubectl diff` workflows.
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered.yaml_text, encoding="utf-8")
        print(str(args.output))
    else:
        print(rendered.yaml_text)
    return 0


def handle_run_batch(args: argparse.Namespace) -> int:
    """Render a local dry-run batch from explicit node names."""

    nodes = _parse_csv(args.nodes)[: args.batch_size]
    rendered_jobs = [
        render_validation_job_from_file(
            args.template,
            node_name=node,
            timestamp=args.timestamp,
            job_prefix=args.job_prefix,
            git_repo=args.git_repo,
            git_ref=args.git_ref,
            cval_config=args.cval_config,
        )
        for node in nodes
    ]
    if args.output == "json":
        print(json.dumps([asdict(job) | {"dry_run": True} for job in rendered_jobs], indent=2))
        return 0

    print(f"Dry run: {len(rendered_jobs)} job(s) would be submitted")
    for job in rendered_jobs:
        print(f"  {job.node_name} -> {job.job_name}")
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

    _print_plan_table(plan)
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


def handle_submit_plan(args: argparse.Namespace) -> int:
    """Compatibility wrapper for the old `submit-plan` command."""

    return handle_run(args)


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


def handle_job_status(args: argparse.Namespace) -> int:
    """Compatibility wrapper for the old `job-status` command."""

    args.watch = False
    return handle_jobs(args)


def handle_monitor_jobs(args: argparse.Namespace) -> int:
    """Compatibility wrapper for the old `monitor-jobs` command."""

    args.watch = True
    return handle_jobs(args)


def handle_result(args: argparse.Namespace) -> int:
    """Inspect a structured validation result JSON file."""

    result = load_validation_result(args.result_json)
    if args.output == "json":
        print(json.dumps(asdict(result), indent=2))
        return 0

    for line in validation_result_to_env_lines(result):
        print(line)
    return 0


def handle_result_env(args: argparse.Namespace) -> int:
    """Compatibility wrapper for the old `result-env` command."""

    args.output = "env"
    return handle_result(args)


def handle_db_add_result(args: argparse.Namespace) -> int:
    """Append one validation result row to the main SQLite DB."""

    timestamp = add_validation_result(
        args.node,
        args.test,
        args.result,
        args.timestamp,
        image_name=args.image_name,
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
        image_name=args.image_name,
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
        image_name=args.image_name,
        db_path=args.db_path,
    )
    print(f"Added NCCL result: {args.node} {timestamp}")
    return 0


def _build_plan_from_args(args: argparse.Namespace):
    """Resolve status inputs, discover nodes if needed, and build a workflow plan."""

    db_status = _load_db_status(
        args.db_status_json,
        args.db_status_tsv,
        live_status=args.live_status,
        status_pod=args.status_pod,
        status_namespace=args.status_namespace,
        status_db_path=args.status_db_path,
    )
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


def _print_plan_table(plan) -> None:
    """Print a human-readable workflow plan summary."""

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


def _load_db_status(
    json_path: Path | None,
    tsv_path: Path | None = None,
    live_status: bool = False,
    status_pod: str = DEFAULT_PVC_ACCESS_POD,
    status_namespace: str = DEFAULT_NAMESPACE,
    status_db_path: str = DEFAULT_DB_PATH,
) -> dict[str, int]:
    """Load validation history from JSON, TSV, live status, or empty defaults."""

    selected_sources = sum(1 for selected in (json_path, tsv_path, live_status) if selected)
    if selected_sources > 1:
        raise ValueError("Use only one of --db-status-json, --db-status-tsv, or --live-status")
    if live_status:
        # Live status is read-only, but still reaches the cluster through the PVC access pod.
        return latest_status_rows_to_node_map(
            get_latest_status_rows(
                pod=status_pod,
                namespace=status_namespace,
                db_path=status_db_path,
            )
        )
    if tsv_path:
        return parse_latest_status_tsv(tsv_path.read_text(encoding="utf-8"))
    if json_path is None:
        # Missing history makes every free node appear never-tested.
        return {}
    data = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("DB status JSON must be an object mapping node names to timestamps")
    return {str(node): int(timestamp) for node, timestamp in data.items()}


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))