"""Manager command group."""

import os
import signal
import subprocess
from collections.abc import Mapping, Sequence
from typing import cast

from ..errors import FormatError
from ..launchers import PROCESS_LAUNCHER, launch_processes, resolve_launcher, split_capacity, start_managers
from ..manager import NotIdleError
from ..models import WORKSPACE_DIRECTORY, validate_resources
from ._common import *
from ._common import (
    _LOGGER,
    _add_adapter_timeout,
    _add_by_path_argument,
    _durable,
    _group,
    _leaf,
    _resolve_binding,
    remote_workspace_output,
)

_WORKER_RESOURCE_HELP = "advertise COUNT units of resource NAME to the scheduler (repeatable; procs and mem are shared fairly among --workers)"


class _LauncherOption(argparse.Action):
    """Reject the mutually exclusive ``--inline``/``--launcher`` pair."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        if self.dest == "launcher":
            if getattr(namespace, "inline", False):
                parser.error("argument --launcher: not allowed with argument --inline")
            setattr(namespace, self.dest, values)
        else:
            if getattr(namespace, "launcher", None) is not None:
                parser.error("argument --inline: not allowed with argument --launcher")
            setattr(namespace, self.dest, True)


def _add_launcher_argument(parser: argparse.ArgumentParser) -> None:
    """Declare the invocation-specific launcher selector."""

    parser.add_argument(
        "--launcher",
        metavar="NAME",
        action=_LauncherOption,
        help="use launcher NAME for this invocation",
    )


def _worker_resources(pairs: Sequence[Sequence[str]]) -> dict[str, int]:
    """Parse and validate repeatable worker-resource option pairs.

    :param pairs: Resource name and capacity pairs from ``argparse``.
    :return: The advertised resource capacities.
    :raises ValueError: If a pair, name, or capacity is invalid.
    """

    resources: dict[str, int] = {}
    for pair in pairs:
        name, raw_count = pair
        if name in resources:
            raise ValueError(f"duplicate --worker-resource name: {name}")
        try:
            count = int(raw_count)
        except ValueError as exc:
            raise ValueError(f"--worker-resource {name} COUNT must be a non-negative integer") from exc
        if count < 0:
            raise ValueError(f"--worker-resource {name} COUNT must be a non-negative integer")
        resources[name] = count
    try:
        return validate_resources(resources, "worker resources")
    except FormatError as exc:
        raise ValueError(str(exc)) from exc


def _slurm_resources(environ: Mapping[str, str]) -> dict[str, int]:
    """Read manager resource capacities from an active SLURM allocation.

    :param environ: Environment mapping to inspect.
    :return: Resource capacities advertised by SLURM, or an empty mapping.
    """

    if "SLURM_JOB_ID" not in environ:
        return {}

    def integer(name: str, *, memory: bool = False) -> int | None:
        raw = environ.get(name)
        if raw is None:
            return None
        value = raw.strip()
        multiplier = 1
        divide_kibibytes = False
        if memory and value:
            suffix = value[-1].upper()
            if suffix in {"M", "G", "K"}:
                value = value[:-1]
                if suffix == "G":
                    multiplier = 1024
                elif suffix == "K":
                    divide_kibibytes = True
        try:
            parsed = int(value)
        except ValueError:
            _LOGGER.warning("ignoring unparsable SLURM resource variable %s=%r", name, raw)
            return None
        if parsed < 0:
            _LOGGER.warning("ignoring negative SLURM resource variable %s=%r", name, raw)
            return None
        if divide_kibibytes:
            return parsed // 1024
        return parsed * multiplier

    resources: dict[str, int] = {}
    ntasks = integer("SLURM_NTASKS")
    if ntasks is not None:
        resources["procs"] = ntasks
    gpus = integer("SLURM_GPUS")
    if gpus is not None:
        resources["gpus"] = gpus
    nodes = integer("SLURM_JOB_NUM_NODES")
    if nodes is not None:
        resources["nodes"] = nodes

    if "SLURM_MEM_PER_CPU" in environ:
        mem_per_cpu = integer("SLURM_MEM_PER_CPU", memory=True)
        cpus = integer("SLURM_CPUS_PER_TASK") if "SLURM_CPUS_PER_TASK" in environ else 1
        if mem_per_cpu is not None and cpus is not None and ntasks is not None:
            resources["mem"] = mem_per_cpu * cpus * ntasks
    elif "SLURM_MEM_PER_NODE" in environ:
        mem_per_node = integer("SLURM_MEM_PER_NODE", memory=True)
        if mem_per_node is not None and nodes is not None:
            resources["mem"] = mem_per_node * nodes
    return resources


def _add_worker_resource_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--worker-resource",
        nargs=2,
        action="append",
        default=[],
        metavar=("NAME", "COUNT"),
        help=_WORKER_RESOURCE_HELP,
    )


# ---------------------------------------------------------------------------
# manager
# ---------------------------------------------------------------------------


def add_manager_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`manager run`."""

    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace this manager serves (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )
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
        default=None,
        metavar="COUNT",
        help="managers to start (default: the workspace's manager.count, or 1)",
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
    _add_worker_resource_argument(parser)
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
            "(default: no background collection; use 'httk workspace gc' instead)"
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
        help=f"manager log file (default: WORKSPACE/{WORKSPACE_DIRECTORY}/managers.log)",
    )
    parser.add_argument("--json-logs", action="store_true", help="log one JSON object per line")
    inline_detach = parser.add_mutually_exclusive_group()
    inline_detach.add_argument(
        "--inline",
        action=_LauncherOption,
        nargs=0,
        help="run one manager in this process regardless of the workspace's launcher",
    )
    inline_detach.add_argument(
        "--detach", action="store_true", help="start the managers and return; the default when invoked from a remote"
    )
    _add_launcher_argument(parser)
    add_durability_arguments(parser)


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare the streamlined top-level :command:`run` leaf."""

    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace this manager serves (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )
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
    _add_worker_resource_argument(parser)
    parser.add_argument("--log-level", choices=LOG_LEVELS, help="log level for the manager log and console")
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        metavar="COUNT",
        help="managers to start (default: the workspace's manager.count, or 1)",
    )
    _add_adapter_timeout(parser)
    inline_detach = parser.add_mutually_exclusive_group()
    inline_detach.add_argument(
        "--inline",
        action=_LauncherOption,
        nargs=0,
        help="run one manager in this process regardless of the workspace's launcher",
    )
    inline_detach.add_argument(
        "--detach", action="store_true", help="start the managers and return; the default when invoked from a remote"
    )
    _add_launcher_argument(parser)
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
        "--join-grace-seconds",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="how long a waiting job tolerates an unresolvable join child (default: 3600)",
    )
    parser.add_argument(
        "--unsafe-persistent-takeover", action="store_true", help="take over a persistent workdir on lease expiry alone"
    )
    parser.add_argument(
        "--unsafe-isolated-takeover",
        action="store_true",
        help="relaunch an isolated-workdir attempt on lease expiry alone",
    )
    parser.add_argument(
        "--takeover-grace-factor",
        type=float,
        default=DEFAULT_TAKEOVER_GRACE_FACTOR,
        metavar="FACTOR",
        help="multiples of the lease before a silent attempt may be taken over (default: 2.0)",
    )
    parser.add_argument(
        "--runner-search-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="ordered root for installed runners (repeatable)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="seconds to keep committing outcomes after a stop signal (default: 30)",
    )
    parser.add_argument("--gc-interval", type=float, metavar="SECONDS", help="background garbage collection interval")
    parser.add_argument("--log-file", metavar="PATH", help="manager log file")
    parser.add_argument("--json-logs", action="store_true", help="log one JSON object per line")
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


def manager_argv_tail(arguments: argparse.Namespace) -> list[str]:
    """Serialize the non-default manager options shared by every launch path."""

    argv: list[str] = []
    for pool in getattr(arguments, "pool", []):
        argv += ["--pool", pool]
    for capability in getattr(arguments, "capability", []):
        argv += ["--capability", capability]
    for prefix in getattr(arguments, "placement_prefix", []):
        argv += ["--placement-prefix", prefix]
    if getattr(arguments, "lease_seconds", None) is not None:
        argv += ["--lease-seconds", str(arguments.lease_seconds)]
    if getattr(arguments, "heartbeat_interval", 30.0) != 30.0:
        argv += ["--heartbeat-interval", str(arguments.heartbeat_interval)]
    if getattr(arguments, "poll_interval", 1.0) != 1.0:
        argv += ["--poll-interval", str(arguments.poll_interval)]
    if getattr(arguments, "join_grace_seconds", 3600.0) != 3600.0:
        argv += ["--join-grace-seconds", str(arguments.join_grace_seconds)]
    workers = getattr(arguments, "workers", None)
    if workers is not None:
        if workers < 1:
            raise ValueError("--workers must be a positive integer")
        argv += ["--workers", str(workers)]
    for name, count in _worker_resources(getattr(arguments, "worker_resource", [])).items():
        argv += ["--worker-resource", name, str(count)]
    if getattr(arguments, "idle", False):
        argv.append("--idle")
    elif getattr(arguments, "idle_timeout", 3600.0) != 3600.0:
        argv += ["--idle-timeout", str(arguments.idle_timeout)]
    if getattr(arguments, "unsafe_persistent_takeover", False):
        argv.append("--unsafe-persistent-takeover")
    if getattr(arguments, "unsafe_isolated_takeover", False):
        argv.append("--unsafe-isolated-takeover")
    if getattr(arguments, "takeover_grace_factor", DEFAULT_TAKEOVER_GRACE_FACTOR) != DEFAULT_TAKEOVER_GRACE_FACTOR:
        argv += ["--takeover-grace-factor", str(arguments.takeover_grace_factor)]
    for path in getattr(arguments, "runner_search_path", []):
        argv += ["--runner-search-path", path]
    if getattr(arguments, "drain_timeout", 30.0) != 30.0:
        argv += ["--drain-timeout", str(arguments.drain_timeout)]
    if getattr(arguments, "gc_interval", None) is not None:
        argv += ["--gc-interval", str(arguments.gc_interval)]
    if getattr(arguments, "log_level", None) is not None:
        argv += ["--log-level", arguments.log_level]
    if getattr(arguments, "log_file", None) is not None:
        argv += ["--log-file", arguments.log_file]
    if getattr(arguments, "json_logs", False):
        argv.append("--json-logs")
    if getattr(arguments, "no_durable", False):
        argv.append("--no-durable")
    elif getattr(arguments, "durable", False):
        argv.append("--durable")
    return argv


def _remote_manager_argv(arguments: argparse.Namespace, name: str) -> list[str]:
    """Build the complete far-side manager invocation for one workspace name."""

    count = getattr(arguments, "count", None)
    if count is not None and count < 1:
        raise ValueError("--count must be a positive integer")
    argv = [
        *REMOTE_MANAGER_COMMAND,
        "--workspace",
        name,
        "--detach",
        *(["--count", str(count)] if count is not None else []),
        *manager_argv_tail(arguments),
    ]
    if getattr(arguments, "launcher", None) is not None:
        argv += ["--launcher", arguments.launcher]
    return argv


def _run_local_manager_children(
    arguments: argparse.Namespace, root: Path, context: CLIContext, count: int | None = None
) -> int:
    """Run multiple local manager children and return their maximum status."""

    children: list[subprocess.Popen[bytes]] = []
    stopping = False

    def terminate(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        for child in children:
            if child.poll() is None:
                try:
                    child.terminate()
                except ProcessLookupError:
                    pass

    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)
    try:
        cli_resources = _worker_resources(arguments.worker_resource)
        slurm_resources = _slurm_resources(os.environ)
        capacity = {**slurm_resources, **cli_resources}
        actual_count = arguments.count if count is None else count
        assert actual_count is not None
        base_tail = manager_argv_tail(arguments)
        explicit = set(_worker_resources(getattr(arguments, "worker_resource", [])))
        for index in range(actual_count):
            if stopping:
                break
            resources = split_capacity(capacity, actual_count, explicit)[index]
            child_argv = [
                sys.executable,
                "-m",
                "httk.core.cli",
                "workflow",
                "manager",
                "run",
                "--by-path",
                "--workspace",
                str(root.resolve()),
                *base_tail,
            ]
            for name, value in resources.items():
                if name not in explicit:
                    child_argv += ["--worker-resource", name, str(value)]
            children.append(subprocess.Popen(child_argv, cwd=context.cwd))
        if stopping:
            terminate(0, None)
        return max((child.wait() for child in children), default=130)
    except BaseException:
        terminate(0, None)
        for child in children:
            child.wait()
        raise
    finally:
        signal.signal(signal.SIGINT, previous[signal.SIGINT])
        signal.signal(signal.SIGTERM, previous[signal.SIGTERM])


def _positive_setting(settings: Mapping[str, object], key: str, default: int) -> int:
    """Read a positive integer manager setting."""

    value = settings.get(key, str(default))
    text = str(value)
    if isinstance(value, bool) or not text.isdigit() or int(text) < 1:
        raise ValueError(f"workspace setting {key} must be a positive integer: {value!r}")
    return int(text)


def _run_in_process_manager(
    arguments: argparse.Namespace, root: Path, context: CLIContext, settings: Mapping[str, object]
) -> int:
    """Run one manager in this process using resolved workspace defaults."""

    cli_resources = _worker_resources(getattr(arguments, "worker_resource", []))
    capacity = {**_slurm_resources(os.environ), **cli_resources}
    workspace = Workspace(root, durable=_durable(arguments))
    configure_logging(
        level=getattr(arguments, "log_level", None) or "warning", json_logs=getattr(arguments, "json_logs", False)
    )
    log_file = Path(arguments.log_file) if getattr(arguments, "log_file", None) else workspace.control / "managers.log"

    def install_manager_log(manager_id: str) -> None:
        add_log_file(
            log_file,
            level=getattr(arguments, "log_level", None) or "info",
            json_logs=getattr(arguments, "json_logs", False),
            manager_id=manager_id,
        )

    configured_workers = cast(int | None, getattr(arguments, "workers", None))
    maximum_workers = (
        configured_workers if configured_workers is not None else _positive_setting(settings, "manager.workers", 1)
    )
    with TaskManager(
        workspace,
        pools=getattr(arguments, "pool", []) or ["default"],
        capabilities=getattr(arguments, "capability", []),
        resources=capacity,
        placement_prefixes=getattr(arguments, "placement_prefix", []),
        maximum_workers=maximum_workers,
        lease_seconds=getattr(arguments, "lease_seconds", None),
        heartbeat_interval=getattr(arguments, "heartbeat_interval", 30.0),
        join_grace_seconds=getattr(arguments, "join_grace_seconds", 3600.0),
        unsafe_persistent_takeover=getattr(arguments, "unsafe_persistent_takeover", False),
        unsafe_isolated_takeover=getattr(arguments, "unsafe_isolated_takeover", False),
        takeover_grace_factor=getattr(arguments, "takeover_grace_factor", DEFAULT_TAKEOVER_GRACE_FACTOR),
        runner_search_paths=getattr(arguments, "runner_search_path", []),
        gc_interval=getattr(arguments, "gc_interval", None),
        on_attached=install_manager_log,
    ) as manager:
        serving_line = (
            f"manager {manager.manager_id} serving {workspace.root} "
            f"(pools={','.join(sorted(manager.pools)) or '-'}, "
            f"capabilities={','.join(sorted(manager.capabilities)) or '-'}, "
            f"executors={','.join(sorted(manager.allowed_executors))}, "
            f"resources={','.join(f'{name}={value}' for name, value in manager.resources.items()) or '-'}); log {log_file}"
        )
        print(serving_line, file=sys.stderr)
        _LOGGER.info("%s", serving_line)
        if getattr(arguments, "idle", False):
            manager.serve(
                poll_interval=getattr(arguments, "poll_interval", 1.0),
                drain_timeout=getattr(arguments, "drain_timeout", 30.0),
            )
        else:
            try:
                census = manager.run_until_idle(
                    timeout=getattr(arguments, "idle_timeout", 3600.0),
                    poll_interval=getattr(arguments, "poll_interval", 1.0),
                )
            except NotIdleError as exc:
                print(exc.census.timeout_message(getattr(arguments, "idle_timeout", 3600.0)), file=sys.stderr)
                return 2
            print(census.summary_line(), file=sys.stderr)
    return 0


def launch_workspace_managers(root: Path, arguments: argparse.Namespace, context: CLIContext) -> tuple[str, object]:
    """Dispatch managers for a local workspace through its configured launcher."""

    root = Path(root)
    settings = Workspace(root).settings
    inline = getattr(arguments, "inline", False)
    requested_launcher = getattr(arguments, "launcher", None)
    if inline and requested_launcher is not None:
        raise ValueError("--launcher cannot be combined with --inline")
    forced_process = inline or getattr(arguments, "by_path", False)
    profile = (
        PROCESS_LAUNCHER
        if forced_process
        else requested_launcher
        if requested_launcher is not None
        else settings.get("manager.launch", PROCESS_LAUNCHER)
    )
    if not isinstance(profile, str) or not profile:
        raise ValueError(f"workspace {root} setting manager.launch must be a nonempty string")
    requested_count = getattr(arguments, "count", None)
    if requested_count is not None and requested_count < 1:
        raise ValueError("--count must be a positive integer")
    if getattr(arguments, "inline", False) and requested_count not in (None, 1):
        raise ValueError("--inline can only be combined with --count 1")
    count = 1 if forced_process else requested_count or _positive_setting(settings, "manager.count", 1)
    tail = manager_argv_tail(arguments)
    if profile == PROCESS_LAUNCHER and getattr(arguments, "workers", None) is None and "manager.workers" in settings:
        tail += ["--workers", str(_positive_setting(settings, "manager.workers", 1))]
    argv = [
        sys.executable,
        "-m",
        "httk.core.cli",
        "workflow",
        "manager",
        "run",
        "--by-path",
        "--workspace",
        str(root.resolve()),
        *tail,
    ]
    if profile != PROCESS_LAUNCHER:
        try:
            target = resolve_launcher(profile, project=context.cwd)
        except (OSError, ValueError) as exc:
            label = "--launcher" if requested_launcher is not None else "setting manager.launch"
            raise RuntimeError(f"workspace {root} {label}={profile!r}: {exc}") from exc
        try:
            result = start_managers(
                target,
                workspace_root=root,
                argv=argv,
                count=count,
                settings=settings,
                timeout=getattr(arguments, "adapter_timeout", None),
            )
        except (OSError, RuntimeError, ValueError) as exc:
            label = "--launcher" if requested_launcher is not None else "setting manager.launch"
            raise RuntimeError(f"workspace {root} {label}={profile!r}: {exc}") from exc
        return target.name, result
    if getattr(arguments, "detach", False):
        return PROCESS_LAUNCHER, launch_processes(workspace_root=root, argv=argv, count=count, settings=settings)
    if count > 1:
        if getattr(arguments, "log_file", None) is not None:
            raise ValueError("--log-file cannot be used with --count > 1: all managers share the workspace log")
        return PROCESS_LAUNCHER, _run_local_manager_children(arguments, root, context, count)
    return PROCESS_LAUNCHER, _run_in_process_manager(arguments, root, context, settings)


def _submit_remote_manager(binding: WorkspaceBinding, arguments: argparse.Namespace, context: CLIContext) -> int:
    """Invoke the manager command on the far side of a remote binding."""

    status, stdout, stderr = submit_remote_manager_result(binding, arguments, context)
    if stdout:
        sys.stdout.write(stdout if stdout.endswith("\n") else stdout + "\n")
    if stderr:
        sys.stderr.write(stderr)
    return status


def submit_remote_manager_result(
    binding: WorkspaceBinding, arguments: argparse.Namespace, context: CLIContext
) -> tuple[int, str, str]:
    """Invoke a remote manager command without writing to process streams."""

    name = binding.name.split(":", 1)[1]
    return remote_workspace_output(
        binding,
        context,
        _remote_manager_argv(arguments, name),
        timeout=arguments.adapter_timeout,
    )


def handle_manager_run(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run one task manager with the workspace manager log.

    A local binding runs the manager in this process. A remote binding submits
    managers through the remote's scheduler over its adapter. ``--idle`` keeps
    a local manager serving after the workspace becomes idle; otherwise the
    command exits when it becomes idle.
    """

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _submit_remote_manager(binding, arguments, context)
    try:
        _mode, result = launch_workspace_managers(root, arguments, context)
    except RuntimeError as exc:
        print(f"{context.program} workflow: {exc}", file=sys.stderr)
        return 1
    if isinstance(result, Mapping):
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("ok", True) else 1
    return cast(int, result)


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
            description="Run one task manager against one execution workspace",
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
            description="Run one task manager against one execution workspace",
            handler=handle_manager_run,
        )
    )
