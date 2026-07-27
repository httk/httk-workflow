"""Import and v1 compatibility command groups."""

# ruff: noqa: F405
from ._common import *  # noqa: F401,F403
from ._common import (
    _durable,
    _group,
    _json_value,
    _leaf,
    _local_root,
    _pairs,
)

# ---------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------


def _print_imported(job: ScaffoldedJob, *, as_json: bool) -> int:
    """Report one imported job the way ``job new`` reports a scaffolded one."""

    if as_json:
        print(json.dumps(job.as_mapping(), indent=2))
    else:
        print(f"{job.job_key}\t{job.payload}")
    return 0


def handle_import_pwd(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Import one Python Workflow Definition document as one job."""

    workspace = Workspace(_local_root(arguments, context, action="import into it"))
    overrides = {
        name: _json_value(text, f"workflow input {name!r}")
        for name, text in _pairs(arguments.inputs, "a workflow input")
    }
    job = import_pwd(
        workspace,
        arguments.document,
        placement=arguments.placement,
        tag=arguments.tag,
        name=arguments.name,
        priority=arguments.priority,
        modules=arguments.modules,
        module_path=arguments.module_path,
        workflow_inputs=overrides,
        allowed_modules=arguments.allow_module,
        data_mode=arguments.data_mode,
        maximum_attempts=arguments.attempts,
        allow_unknown_version=arguments.allow_unknown_version,
    )
    return _print_imported(job, as_json=arguments.json)


def handle_import_cwl(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Import one CWL workflow or command-line tool as one job."""

    workspace = Workspace(_local_root(arguments, context, action="import into it"))
    job = import_cwl(
        workspace,
        arguments.workflow,
        arguments.inputs,
        placement=arguments.placement,
        tag=arguments.tag,
        name=arguments.name,
        priority=arguments.priority,
        data_mode=arguments.data_mode,
    )
    for warning in job.warnings:
        print(f"warning: {warning}", file=sys.stderr)
    return _print_imported(job.job, as_json=arguments.json)


def build_import_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``import`` group: workflows written in another language."""

    _, group = _group(
        subparsers,
        "import",
        summary="import a workflow written in another language as one job",
        description=(
            "Import a workflow written in another language as one httk job. Importing is one way: "
            "the imported job carries the document it came from, and nothing translates a job back"
        ),
    )

    pwd = _leaf(
        group,
        "pwd",
        summary="import one Python Workflow Definition document",
        description="Import one Python Workflow Definition (PWD) JSON document as one job",
        handler=handle_import_pwd,
    )
    pwd.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
    pwd.add_argument("document", metavar="DOCUMENT", help="the PWD JSON document to import")
    pwd.add_argument(
        "--module",
        dest="modules",
        action="append",
        default=[],
        metavar="FILE",
        help="a Python file staged into the payload and put first on the runner's import path (repeatable)",
    )
    pwd.add_argument(
        "--module-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="a further import root, as it will exist on the machine that runs the job (repeatable)",
    )
    pwd.add_argument(
        "--input",
        dest="inputs",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="override one input node by name; @FILE reads a JSON value from a file (repeatable)",
    )
    pwd.add_argument(
        "--allow-module",
        action="append",
        default=[],
        metavar="PREFIX",
        help="restrict the function nodes to modules at or below this prefix (repeatable)",
    )
    pwd.add_argument(
        "--attempts",
        type=int,
        default=3,
        metavar="COUNT",
        help="how many attempts one activation may take before the job fails (default: 3)",
    )
    pwd.add_argument(
        "--allow-unknown-version",
        action="store_true",
        help="import a document declaring a format version this importer was not written against",
    )
    _add_import_arguments(pwd)

    cwl = _leaf(
        group,
        "cwl",
        summary="import one CWL workflow or command-line tool",
        description=(
            "Import one Common Workflow Language document as one job. CWL is supported as a workflow "
            "language: the document is executed on httk's own runner and manager, never by cwltool"
        ),
        handler=handle_import_cwl,
    )
    cwl.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
    cwl.add_argument(
        "workflow",
        metavar="WORKFLOW",
        help="the .cwl workflow or command-line tool to import",
    )
    cwl.add_argument("inputs", metavar="INPUTS", help="the CWL input object, as YAML or JSON")
    _add_import_arguments(cwl)


def _add_import_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare what every import command shares with every other one."""

    parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default=DEFAULT_PLACEMENT,
        help=f"where the job lands in the tree (default: {DEFAULT_PLACEMENT})",
    )
    parser.add_argument("--tag", metavar="TAG", help="the job tag, which prefixes its job key")
    parser.add_argument("--name", metavar="NAME", help="the human-readable job name")
    parser.add_argument(
        "--priority",
        type=int,
        metavar="PRIORITY",
        help="the job priority (default: 500)",
    )
    parser.add_argument(
        "--data-mode",
        choices=("none", "transactional"),
        default="none",
        help="publish the workflow outputs as transactional data (default: none)",
    )
    parser.add_argument("--json", action="store_true", help="print the submitted job as one JSON report")


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
        if arguments.until_idle:
            manager.run_until_idle(timeout=arguments.idle_timeout, poll_interval=arguments.poll_interval)
        else:
            manager.serve(poll_interval=arguments.poll_interval)
    return 0


def add_v1_prepare_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 prepare`, shared with ``httk-v1-taskmanager prepare``."""

    parser.add_argument("source", metavar="SOURCE", help="the instantiated httk v1 task directory")
    parser.add_argument("destination", metavar="DESTINATION", help="the payload directory to write")
    add_v1_job_arguments(parser)
    add_durability_arguments(parser)


def add_v1_submit_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 submit`, shared with ``httk-v1-taskmanager submit``."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
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

    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace this manager serves")
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
    parser.add_argument("--until-idle", action="store_true", help="stop once no claimable task is left")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="with --until-idle, give up waiting for work after this long (default: 3600)",
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
        summary="prepare, submit, and run httk v1 task templates",
        description="Prepare and execute httk v1 task templates through the v2 workflow engine",
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
