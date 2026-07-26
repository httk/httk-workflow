"""Command-line interface installed as :command:`httk-taskmanager`."""

import argparse
import json
import logging
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path

from ._logging import LOG_LEVELS, add_log_file, configure_logging
from ._util import utc_now
from .errors import WorkflowError
from .manager import DEFAULT_TAKEOVER_GRACE_FACTOR, TaskManager
from .models import STATE_KINDS
from .workspace import WorkflowWorkspace

_LOGGER = logging.getLogger(__name__)


def _parser(program: str = "httk-taskmanager") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=program, description="Manage httk filesystem workflows")
    parser.add_argument(
        "--durable",
        action="store_true",
        help="fsync protocol publications (the default; accepted for compatibility)",
    )
    parser.add_argument(
        "--no-durable",
        action="store_true",
        help="do not fsync protocol publications; a crashed node may then strand markers",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    initialize = subparsers.add_parser("init", help="initialize a workflow workspace")
    initialize.add_argument("workspace")
    initialize.add_argument(
        "--extension",
        action="append",
        default=[],
        choices=("transactional-data-v1", "detached-transfer-v1"),
    )

    submit = subparsers.add_parser("submit", help="submit a complete payload directory")
    submit.add_argument("workspace")
    submit.add_argument("source")
    submit.add_argument("--placement", required=True)
    submit.add_argument("--move", action="store_true", help="rename rather than copy the source")

    run = subparsers.add_parser("run", help="run the task manager")
    run.add_argument("workspace")
    run.add_argument("--pool", action="append", default=[])
    run.add_argument("--capability", action="append", default=[])
    run.add_argument("--workers", type=int, default=1)
    run.add_argument(
        "--lease-seconds",
        type=float,
        help="lease length for this manager (default: the workspace policy's lease_seconds)",
    )
    run.add_argument("--heartbeat-interval", type=float, default=30.0)
    run.add_argument("--poll-interval", type=float, default=1.0)
    run.add_argument("--until-idle", action="store_true")
    run.add_argument("--timeout", type=float, default=3600.0)
    run.add_argument(
        "--unsafe-persistent-takeover",
        action="store_true",
        help="take over a persistent workdir on lease expiry alone, without proving the old writer stopped",
    )
    run.add_argument(
        "--unsafe-isolated-takeover",
        action="store_true",
        help="relaunch an isolated-workdir attempt on lease expiry alone, without waiting out the takeover grace",
    )
    run.add_argument(
        "--takeover-grace-factor",
        type=float,
        default=DEFAULT_TAKEOVER_GRACE_FACTOR,
        metavar="FACTOR",
        help="multiples of the lease a silent attempt is left alone before it may be taken over (default: 2.0)",
    )
    run.add_argument(
        "--runner-search-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="ordered root for jobs whose runner.source is installed (repeatable)",
    )
    run.add_argument(
        "--drain-timeout",
        type=float,
        default=30.0,
        help="seconds to keep committing outcomes after a stop signal",
    )
    run.add_argument(
        "--gc-interval",
        type=float,
        metavar="SECONDS",
        help=(
            "also collect garbage from this manager, at most once per SECONDS "
            "(default: no background collection; use 'httk workflow workspace gc' instead)"
        ),
    )
    run.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        help="log level for the manager log file, and for the console when given (default: info)",
    )
    run.add_argument(
        "--log-file",
        help="manager log file (default: WORKSPACE/.httk-workflow/managers/MANAGER_ID/log)",
    )
    run.add_argument("--json-logs", action="store_true", help="log one JSON object per line")

    status = subparsers.add_parser("status", help="summarize authoritative markers")
    status.add_argument("workspace")
    status.add_argument("--json", action="store_true")

    request = subparsers.add_parser("request", help="publish an operator request")
    request.add_argument("workspace")
    request.add_argument("job_id")
    request.add_argument(
        "action",
        choices=("continue", "override_step", "cancel", "set_priority", "pause"),
    )
    request.add_argument("--operator", required=True)
    request.add_argument("--reason", required=True)
    request.add_argument("--priority", type=int)
    request.add_argument("--step")
    request.add_argument(
        "--force",
        action="store_true",
        help=(
            "accept the hazard of reviving a job a decided join already consumed; "
            "the hazard is journalled in the resulting state frame"
        ),
    )
    return parser


def _status(workspace: WorkflowWorkspace, *, as_json: bool) -> None:
    counts: dict[str, int] = {}
    rows: list[dict[str, object]] = []
    for marker in workspace.scan_markers(STATE_KINDS):
        counts[marker.kind] = counts.get(marker.kind, 0) + 1
        rows.append(
            {
                "job_id": marker.job_id,
                "job_key": marker.job_key,
                "state": marker.kind,
                "placement": marker.placement.as_posix(),
                "priority": marker.priority,
                "generation": marker.generation,
            }
        )
    if as_json:
        print(
            json.dumps(
                {
                    "format": "httk-workflow-status",
                    "format_version": 1,
                    "workspace_id": workspace.workspace_id,
                    "workspace_format_version": workspace.format["format_version"],
                    "core_profile": workspace.format["core_profile"],
                    "extensions": sorted(workspace.extensions),
                    "counts": counts,
                    "jobs": rows,
                },
                indent=2,
            )
        )
        return
    print(f"workspace {workspace.workspace_id}")
    for kind in sorted(counts):
        print(f"{kind:12s} {counts[kind]}")


def _publish_request(workspace: WorkflowWorkspace, arguments: argparse.Namespace) -> None:
    marker = workspace.find_marker_by_id(arguments.job_id)
    if marker is None:
        raise ValueError(f"job does not exist: {arguments.job_id}")
    request: dict[str, object] = {
        "format": "httk-workflow-request",
        "format_version": 1,
        "request_id": str(uuid.uuid4()),
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "expected_generation": marker.generation,
        "expected_record_ref": marker.record_ref,
        "action": arguments.action,
        "operator": arguments.operator,
        "reason": arguments.reason,
        "created_at": utc_now(),
    }
    if arguments.priority is not None:
        request["priority"] = arguments.priority
    if arguments.step is not None:
        request["step"] = arguments.step
    if arguments.force:
        request["force"] = True
    path = workspace.publish_request(request)
    print(path)


def _run(workspace: WorkflowWorkspace, arguments: argparse.Namespace) -> int:
    """Run one task manager with its own log file."""

    # Without an explicit level the console stays quiet about normal lifecycle
    # events while the manager log file keeps the complete info-level record.
    configure_logging(level=arguments.log_level or "warning", json_logs=arguments.json_logs)
    with TaskManager(
        workspace,
        pools=arguments.pool or ["default"],
        capabilities=arguments.capability,
        maximum_workers=arguments.workers,
        lease_seconds=arguments.lease_seconds,
        heartbeat_interval=arguments.heartbeat_interval,
        unsafe_persistent_takeover=arguments.unsafe_persistent_takeover,
        unsafe_isolated_takeover=arguments.unsafe_isolated_takeover,
        takeover_grace_factor=arguments.takeover_grace_factor,
        runner_search_paths=arguments.runner_search_path,
        gc_interval=arguments.gc_interval,
    ) as manager:
        log_file = Path(arguments.log_file) if arguments.log_file else manager.manager_directory / "log"
        add_log_file(log_file, level=arguments.log_level or "info", json_logs=arguments.json_logs)
        _LOGGER.info(
            "manager %s serving workspace %s; logging to %s",
            manager.manager_id,
            workspace.root,
            log_file,
        )
        if arguments.until_idle:
            manager.run_until_idle(timeout=arguments.timeout, poll_interval=arguments.poll_interval)
        else:
            manager.serve(poll_interval=arguments.poll_interval, drain_timeout=arguments.drain_timeout)
    return 0


def main(argv: Sequence[str] | None = None, *, program: str = "httk-taskmanager") -> int:
    """Run the command-line interface."""

    parser = _parser(program)
    arguments = parser.parse_args(argv)
    # Durability is the default: a manager that survives a node crash must not
    # be left holding markers that reference journal frames the page cache lost.
    durable = not arguments.no_durable
    try:
        if arguments.command == "init":
            workspace = WorkflowWorkspace.initialize(
                arguments.workspace,
                extensions=arguments.extension,
                durable=durable,
            )
            print(workspace.root)
            return 0
        workspace = WorkflowWorkspace(
            arguments.workspace,
            mutable=arguments.command != "status",
            durable=durable,
        )
        if arguments.command == "submit":
            marker = workspace.submit(arguments.source, arguments.placement, move=arguments.move)
            print(marker.path)
            return 0
        if arguments.command == "status":
            _status(workspace, as_json=arguments.json)
            return 0
        if arguments.command == "request":
            _publish_request(workspace, arguments)
            return 0
        if arguments.command == "run":
            return _run(workspace, arguments)
    except (WorkflowError, OSError, ValueError) as exc:
        print(f"{program}: {exc}", file=sys.stderr)
        return 2
    parser.error(f"unknown command: {arguments.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
