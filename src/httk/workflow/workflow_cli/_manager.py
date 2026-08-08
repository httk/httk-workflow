"""Manager command group."""

from ..adapters import submit_remote_managers
from ..manager import NotIdleError
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
        "--join-grace-seconds",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help=(
            "how long a waiting job tolerates an unresolvable join child before it fails; measured from when a "
            "manager first records it and persisted in the state frame, so it survives a restart (default: 3600)"
        ),
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
    parser.add_argument(
        "--capability",
        action="append",
        default=[],
        metavar="CAPABILITY",
        help="advertise this capability to the scheduler, so capability-gated jobs become claimable (repeatable)",
    )
    parser.add_argument(
        "--placement-prefix",
        action="append",
        default=[],
        metavar="PREFIX",
        help="restrict every scheduling scan to jobs at or below this placement subtree (repeatable)",
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
        lease_seconds=None,
        heartbeat_interval=30.0,
        poll_interval=1.0,
        join_grace_seconds=3600.0,
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
    if arguments.runner_search_path:
        raise ValueError("--runner-search-path cannot be used with a remote workspace binding")
    if arguments.gc_interval is not None:
        raise ValueError("--gc-interval cannot be used with a remote workspace binding")
    target = resolve_remote(binding.remote, project=context.cwd)
    from ._transfer import _remote_workspace_probe

    name = binding.name.split(":", 1)[1]
    _workspace_id, root = _remote_workspace_probe(target, name, timeout=arguments.adapter_timeout)
    manager_argv: list[str] = []
    for pool in arguments.pool:
        manager_argv += ["--pool", pool]
    for capability in arguments.capability:
        manager_argv += ["--capability", capability]
    for prefix in arguments.placement_prefix:
        manager_argv += ["--placement-prefix", prefix]
    if arguments.lease_seconds is not None:
        manager_argv += ["--lease-seconds", str(arguments.lease_seconds)]
    if arguments.heartbeat_interval != 30.0:
        manager_argv += ["--heartbeat-interval", str(arguments.heartbeat_interval)]
    if arguments.poll_interval != 1.0:
        manager_argv += ["--poll-interval", str(arguments.poll_interval)]
    if arguments.join_grace_seconds != 3600.0:
        manager_argv += ["--join-grace-seconds", str(arguments.join_grace_seconds)]
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
    if not _durable(arguments):
        manager_argv.append("--no-durable")
    if arguments.log_level is not None:
        manager_argv += ["--log-level", arguments.log_level]
    if arguments.json_logs:
        manager_argv.append("--json-logs")
    print(
        json.dumps(
            submit_remote_managers(
                target,
                name,
                root,
                count=arguments.count,
                argv_tail=manager_argv,
                timeout=arguments.adapter_timeout,
                adapter=run_adapter,
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
        join_grace_seconds=arguments.join_grace_seconds,
        unsafe_persistent_takeover=arguments.unsafe_persistent_takeover,
        unsafe_isolated_takeover=arguments.unsafe_isolated_takeover,
        takeover_grace_factor=arguments.takeover_grace_factor,
        runner_search_paths=arguments.runner_search_path,
        gc_interval=arguments.gc_interval,
    ) as manager:
        log_file = Path(arguments.log_file) if arguments.log_file else manager.manager_directory / "log"
        add_log_file(log_file, level=arguments.log_level or "info", json_logs=arguments.json_logs)
        # One unconditional line, whatever the console log level: which manager
        # is serving what, where its log is, and what it serves. Without it a
        # normal run prints nothing at all, and a job that no configured pool,
        # capability, or executor matches looks identical to an idle workspace.
        print(
            f"manager {manager.manager_id} serving {workspace.root} "
            f"(pools={','.join(sorted(manager.pools)) or '-'}, "
            f"capabilities={','.join(sorted(manager.capabilities)) or '-'}, "
            f"executors={','.join(sorted(manager.allowed_executors))}); log {log_file}",
            file=sys.stderr,
        )
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
                census = manager.run_until_idle(timeout=arguments.idle_timeout, poll_interval=arguments.poll_interval)
            except NotIdleError as exc:
                print(exc.census.timeout_message(arguments.idle_timeout), file=sys.stderr)
                return 2
            print(census.summary_line(), file=sys.stderr)
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
