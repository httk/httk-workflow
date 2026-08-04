"""Manager command group."""

from ._common import *
from ._common import (
    _LOGGER,
    _add_adapter_timeout,
    _add_by_path_argument,
    _durable,
    _group,
    _leaf,
    _resolve_binding,
)
from ._common import _run_adapter as run_adapter

# ---------------------------------------------------------------------------
# manager
# ---------------------------------------------------------------------------


def add_manager_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`manager run`, shared with ``httk-taskmanager run``."""

    add_workspace_argument(parser, help_text="the workspace this manager serves")
    parser.add_argument(
        "--pool",
        action="append",
        default=[],
        metavar="POOL",
        help="claim only jobs of this pool (repeatable, default: default)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        metavar="COUNT",
        help="for a remote workspace, how many managers to submit to its scheduler (default: 1)",
    )
    _add_adapter_timeout(parser)
    _add_by_path_argument(parser)
    parser.add_argument(
        "--capability",
        action="append",
        default=[],
        metavar="CAPABILITY",
        help="advertise this capability to the scheduler (repeatable)",
    )
    parser.add_argument(
        "--placement-prefix",
        action="append",
        default=[],
        metavar="PREFIX",
        help=(
            "restrict every scheduling scan to jobs at or below this placement subtree "
            "(repeatable, default: the whole workspace)"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        metavar="COUNT",
        help="attempts to run at once, locally, or workers per submitted remote manager (default: 1)",
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
        "--unsafe-isolated-takeover",
        action="store_true",
        help="relaunch an isolated-workdir attempt on lease expiry alone, without waiting out the takeover grace",
    )
    parser.add_argument(
        "--takeover-grace-factor",
        type=float,
        default=DEFAULT_TAKEOVER_GRACE_FACTOR,
        metavar="FACTOR",
        help="multiples of the lease a silent attempt is left alone before it may be taken over (default: 2.0)",
    )
    parser.add_argument(
        "--runner-search-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="ordered root for jobs whose runner.source is installed (repeatable)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="seconds to keep committing outcomes after a stop signal (default: 30)",
    )
    parser.add_argument(
        "--gc-interval",
        type=float,
        metavar="SECONDS",
        help=(
            "also collect garbage from this manager, at most once per SECONDS "
            "(default: no background collection; use 'httk workflow workspace gc' instead)"
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        help="log level for the manager log file, and for the console when given (default: info)",
    )
    parser.add_argument(
        "--log-file",
        metavar="PATH",
        help="manager log file (default: WORKSPACE/.httk-workflow/managers/MANAGER_ID/log)",
    )
    parser.add_argument("--json-logs", action="store_true", help="log one JSON object per line")
    add_durability_arguments(parser)


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare the streamlined top-level :command:`run` leaf."""

    add_workspace_argument(parser, help_text="the workspace this manager serves")
    parser.add_argument(
        "--pool",
        action="append",
        default=[],
        metavar="POOL",
        help="claim only jobs of this pool (repeatable, default: default)",
    )
    parser.add_argument("--idle", action="store_true", help="keep serving the workspace when nothing is left to do")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="without --idle, give up after this long if the workspace never becomes idle (default: 3600)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        metavar="COUNT",
        help="attempts to run at once, locally, or workers per submitted remote manager (default: 1)",
    )
    parser.add_argument("--log-level", choices=LOG_LEVELS, help="log level for the manager log and console")
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        metavar="COUNT",
        help="for a remote workspace, how many managers to submit to its scheduler (default: 1)",
    )
    _add_adapter_timeout(parser)
    add_durability_arguments(parser)
    parser.set_defaults(
        handler=handle_manager_run,
        by_path=False,
        capability=[],
        placement_prefix=[],
        lease_seconds=None,
        heartbeat_interval=30.0,
        poll_interval=1.0,
        unsafe_persistent_takeover=False,
        unsafe_isolated_takeover=False,
        takeover_grace_factor=DEFAULT_TAKEOVER_GRACE_FACTOR,
        runner_search_path=[],
        drain_timeout=30.0,
        log_file=None,
        json_logs=False,
        gc_interval=None,
    )


def _submit_remote_manager(binding: WorkspaceBinding, arguments: argparse.Namespace, context: CLIContext) -> int:
    """Submit one or more managers to a remote binding's scheduler."""

    if arguments.count < 1:
        raise ValueError("--count must be a positive integer")
    target = resolve_remote(binding.remote, project=context.cwd)
    manager_argv = [*REMOTE_MANAGER_COMMAND, binding.path, "--by-path"]
    # Left off unless asked for, so a remote configured with workers=N is not
    # permanently shadowed by a command-line default.
    if arguments.workers is not None:
        if arguments.workers < 1:
            raise ValueError("--workers must be a positive integer")
        manager_argv += ["--workers", str(arguments.workers)]
    if arguments.idle:
        manager_argv.append("--idle")
    elif arguments.idle_timeout != 3600.0:
        manager_argv += ["--idle-timeout", str(arguments.idle_timeout)]
    request: dict[str, object] = {
        "remote_settings": {},
        "argv": manager_argv,
        "workspace": binding.path,
        "count": arguments.count,
    }
    print(
        json.dumps(
            run_adapter(
                target.bundle,
                "start-manager",
                request,
                timeout=arguments.adapter_timeout,
            ),
            indent=2,
        )
    )
    return 0


def handle_manager_run(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run one task manager with its own log file.

    A local binding runs the manager in this process. A remote binding submits
    managers through the remote's scheduler over its adapter. ``--idle`` keeps
    a local manager serving after the workspace becomes idle; otherwise the
    command exits when it becomes idle.
    """

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _submit_remote_manager(binding, arguments, context)
    workspace = Workspace(root, durable=_durable(arguments))
    # Without an explicit level the console stays quiet about normal lifecycle
    # events while the manager log file keeps the complete info-level record.
    configure_logging(level=arguments.log_level or "warning", json_logs=arguments.json_logs)
    with TaskManager(
        workspace,
        pools=arguments.pool or ["default"],
        capabilities=arguments.capability,
        placement_prefixes=arguments.placement_prefix,
        maximum_workers=arguments.workers if arguments.workers is not None else 1,
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
        if arguments.idle:
            manager.serve(
                poll_interval=arguments.poll_interval,
                drain_timeout=arguments.drain_timeout,
            )
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


def build_manager_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``manager`` group: the process that runs the jobs."""

    _, group = _group(
        subparsers,
        "manager",
        summary="run a task manager against a workspace",
        description="Run the task manager that claims and executes the jobs of a workspace",
    )
    add_manager_run_arguments(
        _leaf(
            group,
            "run",
            summary="run the task manager",
            description="Run one task manager against one workflow workspace",
            handler=handle_manager_run,
        )
    )


def build_run_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the top-level manager runner leaf."""

    add_run_arguments(
        _leaf(
            subparsers,
            "run",
            summary="run a task manager until the workspace is idle",
            description="Run one task manager against one workflow workspace",
            handler=handle_manager_run,
        )
    )
