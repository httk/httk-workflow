"""Import and v1 compatibility command groups."""

import json

from httk.workflow.compat.v1 import collect_finished_tree

from ._collect import _collected_mapping, _store_collected
from ._common import *
from ._common import (
    _durable,
    _group,
    _leaf,
    _local_root,
)

# ---------------------------------------------------------------------------
# v1
# ---------------------------------------------------------------------------


def _v1_pool(taskset: str) -> str:
    """Return the pool one *httk* v1 task set assigns work to."""

    return "default" if taskset == "any" else taskset


def add_v1_job_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the job description shared by :command:`v1 prepare` and ``v1 submit``."""

    parser.add_argument(
        "--id",
        dest="job_id",
        metavar="UUID",
        help="the job UUID (default: a fresh one)",
    )
    parser.add_argument(
        "--tag",
        metavar="TAG",
        default="v1-task",
        help="the readable half of the job key",
    )
    parser.add_argument(
        "--name",
        metavar="NAME",
        default="httk v1 task",
        help="the human-readable job name",
    )
    parser.add_argument("--step", metavar="STEP", default="start", help="the step the task starts at")
    # prepare and submit *assign* a task set, so their default is the task set
    # an unconfigured v1 installation calls its own; `run` *filters* by one, so
    # its default is the filter that accepts every set. See build_v1_parser.
    parser.add_argument(
        "--taskset",
        "-s",
        dest="taskset",
        metavar="TASKSET",
        default="default",
        help="the httk v1 task set this task belongs to (default: default)",
    )
    parser.add_argument(
        "--priority",
        type=int,
        choices=range(1, 6),
        default=3,
        metavar="PRIORITY",
        help="httk v1 priority, 1 to 5 (default: 3)",
    )
    parser.add_argument(
        "--attempts",
        "-a",
        type=int,
        default=10,
        metavar="COUNT",
        help="how many times the task may be attempted (default: 10)",
    )


def handle_v1_prepare(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Turn one instantiated *httk* v1 task directory into a v2 payload."""

    job = prepare_v1_payload(
        arguments.source,
        arguments.destination,
        job_id=arguments.job_id,
        tag=arguments.tag,
        name=arguments.name,
        initial_step=arguments.step,
        pool=_v1_pool(arguments.taskset),
        priority=arguments.priority,
        attempts=arguments.attempts,
    )
    print(job.job_key)
    return 0


def handle_v1_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Prepare and submit one instantiated *httk* v1 task."""

    workspace = Workspace(
        _local_root(arguments, context, action="submit into it"),
        durable=_durable(arguments),
    )
    marker = submit_v1_task(
        workspace,
        arguments.source,
        arguments.placement,
        job_id=arguments.job_id,
        tag=arguments.tag,
        name=arguments.name,
        initial_step=arguments.step,
        pool=_v1_pool(arguments.taskset),
        priority=arguments.priority,
        attempts=arguments.attempts,
    )
    print(marker.path)
    return 0


def handle_v1_run(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run only *httk* v1 jobs of one v2 workspace."""

    workspace = Workspace(
        _local_root(arguments, context, action="run its managers"),
        durable=_durable(arguments),
    )
    compression = "zstd" if arguments.zstdlog else "none" if arguments.no_bzip2log else "bzip2"
    with V1TaskManager(
        workspace,
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
        if arguments.idle:
            manager.serve(poll_interval=arguments.poll_interval)
        else:
            try:
                manager.run_until_idle(timeout=arguments.idle_timeout, poll_interval=arguments.poll_interval)
            except TimeoutError:
                print(
                    f"workspace is not idle after {arguments.idle_timeout:.0f}s; jobs are still running or "
                    "claimable — rerun, raise --idle-timeout, or pass --idle to keep serving",
                    file=sys.stderr,
                )
                return 2
    return 0


def handle_v1_collect(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Collect finished directories from a pre-existing httk v1 tree."""

    del context
    if arguments.into is not None and arguments.json:
        raise ValueError("--into cannot be combined with --json")
    items = list(collect_finished_tree(arguments.root, workflow_dir=arguments.workflow_dir))
    reports = (
        _store_collected(items, arguments.into)
        if arguments.into is not None
        else [_collected_mapping(item) for item in items]
    )
    for report in reports:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


def add_v1_prepare_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 prepare`, shared with ``httk-v1-taskmanager prepare``."""

    parser.add_argument("source", metavar="SOURCE", help="the instantiated httk v1 task directory")
    parser.add_argument("destination", metavar="DESTINATION", help="the payload directory to write")
    add_v1_job_arguments(parser)
    add_durability_arguments(parser)


def add_v1_submit_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 submit`, shared with ``httk-v1-taskmanager submit``."""

    add_workspace_argument(parser, help_text="the workspace to submit into")
    parser.add_argument("source", metavar="SOURCE", help="the instantiated httk v1 task directory")
    parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        required=True,
        help="where the job lands in the tree",
    )
    add_v1_job_arguments(parser)
    add_durability_arguments(parser)


def add_v1_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 run`, shared with ``httk-v1-taskmanager run``."""

    add_workspace_argument(parser, help_text="the workspace this manager serves")
    # `run` filters rather than assigns, so "any" — accept every task set — is
    # the only default that runs the work a v1 operator already submitted.
    parser.add_argument(
        "--taskset",
        "-s",
        dest="taskset",
        metavar="TASKSET",
        default="any",
        help="run only the jobs of this httk v1 task set (default: any = accept all)",
    )
    parser.add_argument("--wrap", "-w", metavar="COMMAND", help="wrap each task launch in this command")
    parser.add_argument(
        "--task-timeout",
        "-t",
        type=float,
        default=21600.0,
        metavar="SECONDS",
        help="give up on one task after this long (default: 21600)",
    )
    parser.add_argument(
        "--attempts",
        "-a",
        type=int,
        default=10,
        metavar="COUNT",
        help="how many times a task may be attempted (default: 10)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="COUNT",
        help="tasks to run at once (default: 1)",
    )
    parser.add_argument(
        "--lease-seconds",
        type=float,
        metavar="SECONDS",
        help="lease length for this manager (default: the workspace policy's lease_seconds)",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="how often this manager refreshes its lease (default: 30)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="how often this manager looks for work (default: 1)",
    )
    parser.add_argument(
        "--idle",
        action="store_true",
        help="keep serving the workspace when nothing is left to do",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="without --idle, give up after this long if the workspace never becomes idle (default: 3600)",
    )
    parser.add_argument(
        "--unsafe-persistent-takeover",
        action="store_true",
        help="take over a persistent workdir on lease expiry alone, without proving the old writer stopped",
    )
    parser.add_argument(
        "--httk-v1-root",
        metavar="DIRECTORY",
        help="the httk v1 runtime to execute tasks with",
    )
    compression = parser.add_mutually_exclusive_group()
    compression.add_argument("--no-bzip2log", "-b", action="store_true", help="leave task logs uncompressed")
    compression.add_argument(
        "--zstdlog",
        action="store_true",
        help="compress task logs with zstd rather than bzip2",
    )
    add_durability_arguments(parser)


def add_v1_collect_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare v1 collect; it is intentionally not an alias command."""

    parser.add_argument("root", metavar="ROOT", help="the root of a finished httk v1 result tree")
    parser.add_argument(
        "--workflow-dir",
        required=True,
        metavar="PKG",
        help="the directory workflow package providing the postprocess hook",
    )
    parser.add_argument("--into", metavar="PATH", help="save collected entries, runs, and products to SQLite")
    parser.add_argument("--json", action="store_true", help="print collected summaries as JSON lines")


def build_v1_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``v1`` group: *httk* v1 task templates on the v2 engine.

    ``--taskset`` deliberately defaults differently between siblings, because
    the sibling commands mean different things by it. ``prepare`` and ``submit``
    *assign* a task set to the job they create, so their default is the ordinary
    ``default`` set; ``run`` *filters* the jobs it will claim, so its default is
    ``any``, which accepts every set. Unifying them would either strand every
    submitted job under a manager that filters for one set, or quietly file
    every prepared task under a set named ``any``.
    """

    _, group = _group(
        subparsers,
        "v1",
        summary="prepare, submit, run, and collect httk v1 jobs",
        description="Prepare and execute v1 templates, or collect an old v1 result tree",
    )
    add_v1_prepare_arguments(
        _leaf(
            group,
            "prepare",
            summary="turn an instantiated v1 task directory into a v2 payload",
            description="Turn one instantiated httk v1 task directory into a v2 payload",
            handler=handle_v1_prepare,
        )
    )
    add_v1_submit_arguments(
        _leaf(
            group,
            "submit",
            summary="prepare and submit an instantiated v1 task",
            description="Prepare and submit one instantiated httk v1 task",
            handler=handle_v1_submit,
        )
    )
    add_v1_run_arguments(
        _leaf(
            group,
            "run",
            summary="run only httk-v1 jobs in a v2 workspace",
            description="Run only the httk v1 jobs of one v2 workflow workspace",
            handler=handle_v1_run,
        )
    )
    add_v1_collect_arguments(
        _leaf(
            group,
            "collect",
            summary="collect a finished v1 result tree",
            description="Collect finished tasks from a pre-existing httk v1 result tree",
            handler=handle_v1_collect,
        )
    )
