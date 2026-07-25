"""Command-line interface installed as :command:`httk-v1-taskmanager`."""

import argparse
import sys
from collections.abc import Sequence

from .errors import WorkflowError
from .store import WorkflowStore
from .v1 import V1TaskManager, prepare_v1_payload, submit_v1_task


def _job_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--id", dest="job_id")
    parser.add_argument("--tag", default="v1-task")
    parser.add_argument("--name", default="httk v1 task")
    parser.add_argument("--step", default="start")
    parser.add_argument("--set", dest="taskset", default="default")
    parser.add_argument("--priority", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--attempts", "-a", type=int, default=10)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="httk-v1-taskmanager",
        description="Prepare and execute httk v1 task templates through the v2 workflow engine",
    )
    parser.add_argument("--durable", action="store_true", help="fsync protocol publications")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="turn an instantiated v1 task directory into a v2 payload")
    prepare.add_argument("source")
    prepare.add_argument("destination")
    _job_options(prepare)

    submit = subparsers.add_parser("submit", help="prepare and submit an instantiated v1 task")
    submit.add_argument("store")
    submit.add_argument("source")
    submit.add_argument("--placement", required=True)
    _job_options(submit)

    run = subparsers.add_parser("run", help="run only httk-v1 jobs in a v2 store")
    run.add_argument("store")
    run.add_argument("--set", "-s", dest="taskset", default="any")
    run.add_argument("--wrap", "-w")
    run.add_argument("--task-timeout", "-t", type=float, default=21600.0)
    run.add_argument("--attempts", "-a", type=int, default=10)
    run.add_argument("--workers", type=int, default=1)
    run.add_argument("--lease-seconds", type=float, default=900.0)
    run.add_argument("--heartbeat-interval", type=float, default=30.0)
    run.add_argument("--poll-interval", type=float, default=1.0)
    run.add_argument("--until-idle", action="store_true")
    run.add_argument("--idle-timeout", type=float, default=3600.0)
    run.add_argument("--unsafe-persistent-takeover", action="store_true")
    run.add_argument("--httk-v1-root")
    compression = run.add_mutually_exclusive_group()
    compression.add_argument("--no-bzip2log", "-b", action="store_true")
    compression.add_argument("--zstdlog", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the httk v1 compatibility command."""

    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "prepare":
            job = prepare_v1_payload(
                arguments.source,
                arguments.destination,
                job_id=arguments.job_id,
                tag=arguments.tag,
                name=arguments.name,
                initial_step=arguments.step,
                pool="default" if arguments.taskset == "any" else arguments.taskset,
                priority=arguments.priority,
                attempts=arguments.attempts,
            )
            print(job.job_key)
            return 0
        store = WorkflowStore(arguments.store, durable=arguments.durable)
        if arguments.command == "submit":
            marker = submit_v1_task(
                store,
                arguments.source,
                arguments.placement,
                job_id=arguments.job_id,
                tag=arguments.tag,
                name=arguments.name,
                initial_step=arguments.step,
                pool="default" if arguments.taskset == "any" else arguments.taskset,
                priority=arguments.priority,
                attempts=arguments.attempts,
            )
            print(marker.path)
            return 0
        if arguments.command == "run":
            compression = "zstd" if arguments.zstdlog else "none" if arguments.no_bzip2log else "bzip2"
            with V1TaskManager(
                store,
                taskset=arguments.taskset,
                maximum_workers=arguments.workers,
                lease_seconds=arguments.lease_seconds,
                heartbeat_interval=arguments.heartbeat_interval,
                unsafe_persistent_takeover=arguments.unsafe_persistent_takeover,
                runtime_root=arguments.httk_v1_root,
                timeout=arguments.task_timeout,
                wrapper=arguments.wrap,
                log_compression=compression,
                attempts=arguments.attempts,
            ) as manager:
                if arguments.until_idle:
                    manager.run_until_idle(
                        timeout=arguments.idle_timeout,
                        poll_interval=arguments.poll_interval,
                    )
                else:
                    manager.serve(poll_interval=arguments.poll_interval)
            return 0
    except (WorkflowError, OSError, ValueError) as exc:
        print(f"httk-v1-taskmanager: {exc}", file=sys.stderr)
        return 2
    parser.error(f"unknown command: {arguments.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
