"""Command-line interface installed as :command:`httk-taskmanager`."""

import argparse
import json
import sys
import uuid
from collections.abc import Sequence

from ._util import utc_now
from .errors import WorkflowError
from .manager import TaskManager
from .models import STATE_KINDS
from .store import WorkflowStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="httk-taskmanager", description="Manage httk filesystem workflows")
    parser.add_argument("--durable", action="store_true", help="fsync protocol publications")
    subparsers = parser.add_subparsers(dest="command", required=True)

    initialize = subparsers.add_parser("init", help="initialize a workflow store")
    initialize.add_argument("store")
    initialize.add_argument(
        "--extension",
        action="append",
        default=[],
        choices=("transactional-data-v1", "priority-bands-v1"),
    )

    submit = subparsers.add_parser("submit", help="submit a complete payload directory")
    submit.add_argument("store")
    submit.add_argument("source")
    submit.add_argument("--placement", required=True)
    submit.add_argument("--move", action="store_true", help="rename rather than copy the source")

    run = subparsers.add_parser("run", help="run the task manager")
    run.add_argument("store")
    run.add_argument("--pool", action="append", default=[])
    run.add_argument("--capability", action="append", default=[])
    run.add_argument("--workers", type=int, default=1)
    run.add_argument("--lease-seconds", type=float, default=900.0)
    run.add_argument("--heartbeat-interval", type=float, default=30.0)
    run.add_argument("--poll-interval", type=float, default=1.0)
    run.add_argument("--until-idle", action="store_true")
    run.add_argument("--timeout", type=float, default=3600.0)
    run.add_argument("--unsafe-persistent-takeover", action="store_true")

    status = subparsers.add_parser("status", help="summarize authoritative markers")
    status.add_argument("store")
    status.add_argument("--json", action="store_true")

    request = subparsers.add_parser("request", help="publish an operator request")
    request.add_argument("store")
    request.add_argument("job_id")
    request.add_argument(
        "action",
        choices=("continue", "override_step", "cancel", "set_priority", "pause"),
    )
    request.add_argument("--operator", required=True)
    request.add_argument("--reason", required=True)
    request.add_argument("--priority", type=int)
    request.add_argument("--step")
    return parser


def _status(store: WorkflowStore, *, as_json: bool) -> None:
    counts: dict[str, int] = {}
    rows: list[dict[str, object]] = []
    for marker in store.scan_markers(STATE_KINDS):
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
        print(json.dumps({"store_id": store.store_id, "counts": counts, "jobs": rows}, indent=2))
        return
    print(f"store {store.store_id}")
    for kind in sorted(counts):
        print(f"{kind:12s} {counts[kind]}")


def _publish_request(store: WorkflowStore, arguments: argparse.Namespace) -> None:
    marker = store.find_marker_by_id(arguments.job_id)
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
    path = store.publish_request(request)
    print(path)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface."""

    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "init":
            store = WorkflowStore.initialize(
                arguments.store,
                extensions=arguments.extension,
                durable=arguments.durable,
            )
            print(store.root)
            return 0
        store = WorkflowStore(
            arguments.store,
            mutable=arguments.command != "status",
            durable=arguments.durable,
        )
        if arguments.command == "submit":
            marker = store.submit(arguments.source, arguments.placement, move=arguments.move)
            print(marker.path)
            return 0
        if arguments.command == "status":
            _status(store, as_json=arguments.json)
            return 0
        if arguments.command == "request":
            _publish_request(store, arguments)
            return 0
        if arguments.command == "run":
            pools = arguments.pool or ["default"]
            with TaskManager(
                store,
                pools=pools,
                capabilities=arguments.capability,
                maximum_workers=arguments.workers,
                lease_seconds=arguments.lease_seconds,
                heartbeat_interval=arguments.heartbeat_interval,
                unsafe_persistent_takeover=arguments.unsafe_persistent_takeover,
            ) as manager:
                if arguments.until_idle:
                    manager.run_until_idle(timeout=arguments.timeout, poll_interval=arguments.poll_interval)
                else:
                    manager.serve(poll_interval=arguments.poll_interval)
            return 0
    except (WorkflowError, OSError, ValueError) as exc:
        print(f"httk-taskmanager: {exc}", file=sys.stderr)
        return 2
    parser.error(f"unknown command: {arguments.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
