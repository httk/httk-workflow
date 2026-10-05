"""Manager command group."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from httk.core.cli import CLIContext

from .._allocation import (
    Allocation,
    argv_allocation,
    bind_cpus_setting,
    parse_allocation_spec,
    probe_allocation,
    split_allocation,
)
from .._confine import CONFINE_PREFIX, confine_settings, is_override_key
from .._durations import format_duration, parse_slurm_duration
from .._logging import LOG_LEVELS, add_log_file, configure_logging
from .._scheduler import detect_scheduler
from ..adapters import REMOTE_MANAGER_COMMAND
from ..errors import FormatError
from ..launchers import PROCESS_LAUNCHER, launch_processes, resolve_launcher, split_capacity, start_managers
from ..manager import DEFAULT_TAKEOVER_GRACE_FACTOR, NotIdleError, TaskManager
from ..models import WORKSPACE_DIRECTORY, validate_capacity
from ..registry import WorkspaceBinding
from ..workspace import Workspace, _validate_setting_key, _validate_setting_value
from ._common import (
    _LOGGER,
    _add_adapter_timeout,
    _add_by_path_argument,
    _durable,
    _group,
    _leaf,
    _resolve_binding,
    add_durability_arguments,
    remote_workspace_output,
)

_TIME_LIMIT_HELP = (
    "this manager's allocation ends after DURATION (Slurm --time syntax such as 12:00:00); "
    "default: the enclosing scheduler allocation's end time when available"
)
_DEADLINE_MARGIN_HELP = "start draining this many seconds before the allocation ends (default: 120)"
_ALLOCATION_HELP = (
    "where this manager learns its nodes, processors and devices: auto, none, slurm, host, or exec:PATH (default: auto)"
)
_WORKER_RESOURCE_HELP = "advertise COUNT units of resource NAME to the scheduler (repeatable; procs and mem are shared fairly among --workers)"
_SETTING_KEYS = "manager.confine, manager.launch_template, manager.bind_cpus or confine.*"


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
        return validate_capacity(resources, "worker resources")
    except FormatError as exc:
        raise ValueError(str(exc)) from exc


def _scheduler_resources(environ: Mapping[str, str]) -> dict[str, int]:
    """Read manager resource capacities from an active scheduler allocation.

    :param environ: Environment mapping to inspect.
    :return: Resource capacities advertised by the scheduler, or an empty mapping.
    """

    scheduler = detect_scheduler(environ)
    return {} if scheduler is None else scheduler.counts(environ)


def _time_limit_seconds(arguments: argparse.Namespace) -> int | None:
    """Return the parsed ``--time-limit``, or ``None`` when it is not given.

    :param arguments: The parsed manager options.
    :return: The allocation length in seconds.
    :raises ValueError: If the duration is not a Slurm ``--time`` duration.
    """

    text = getattr(arguments, "time_limit", None)
    if text is None:
        return None
    try:
        return parse_slurm_duration(text)
    except ValueError as exc:
        raise ValueError(f"--time-limit: {exc}") from exc


def _effective_margin(arguments: argparse.Namespace) -> float:
    """Return ``--deadline-margin`` raised to at least ``--drain-timeout``."""

    return max(getattr(arguments, "deadline_margin", 120.0), getattr(arguments, "drain_timeout", 30.0))


def _manager_end_time(arguments: argparse.Namespace, allocation_end: float | None) -> tuple[float | None, float]:
    """Return when this manager's allocation ends and the deadline margin it drains with.

    ``--time-limit`` counts from now; with the probed allocation's end time also
    known, the earlier of the two wins. The margin is ``--deadline-margin`` raised
    to ``--drain-timeout``, so draining can finish before the allocation ends.

    :param arguments: The parsed manager options.
    :param allocation_end: The probed allocation's end time, or ``None`` when unknown.
    :return: The allocation end time, or ``None`` when unknown, and the margin.
    :raises ValueError: If ``--time-limit`` is invalid.
    """

    seconds = _time_limit_seconds(arguments)
    limit = None if seconds is None else time.time() + seconds
    if limit is not None and allocation_end is not None and limit > allocation_end:
        _LOGGER.warning(
            "--time-limit %s ends after the allocation does; using the allocation's end time",
            arguments.time_limit,
        )
    ends = [value for value in (limit, allocation_end) if value is not None]
    margin = _effective_margin(arguments)
    if ends and margin > getattr(arguments, "deadline_margin", 120.0):
        _LOGGER.warning(
            "raising --deadline-margin to the --drain-timeout of %g s so draining finishes before the allocation ends",
            margin,
        )
    return min(ends, default=None), margin


def _add_time_limit_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--time-limit", metavar="DURATION", help=_TIME_LIMIT_HELP)
    parser.add_argument("--deadline-margin", type=float, metavar="SECONDS", help=_DEADLINE_MARGIN_HELP)


def _add_worker_resource_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--allocation", metavar="SPEC", help=_ALLOCATION_HELP)
    parser.add_argument(
        "--worker-resource",
        nargs=2,
        action="append",
        default=[],
        metavar=("NAME", "COUNT"),
        help=_WORKER_RESOURCE_HELP,
    )


def _setting_override(text: str) -> str:
    """Validate one ``--setting KEY=VALUE`` pinned override and return it unchanged."""

    key, separator, value = text.partition("=")
    if not separator or not is_override_key(key):
        raise argparse.ArgumentTypeError(f"KEY=VALUE needs KEY {_SETTING_KEYS}: {text!r}")
    try:
        _validate_setting_key(key)
        _validate_setting_value(key, value)
        if key == "manager.confine" or key.startswith(CONFINE_PREFIX):
            confine_settings({key: value})
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return text


def _add_setting_argument(parser: argparse.ArgumentParser) -> None:
    # Hidden: launchers pin their confinement settings on the managers they start with it.
    parser.add_argument("--setting", type=_setting_override, action="append", help=argparse.SUPPRESS)


def _pinned_settings(arguments: argparse.Namespace) -> dict[str, str]:
    """Return the manager's pinned ``--setting`` overrides; the last occurrence of a key wins.

    :param arguments: The parsed manager arguments.
    :return: The pinned settings, which override workspace settings for the manager's lifetime.
    """

    pinned: dict[str, str] = {}
    for item in getattr(arguments, "setting", []):
        key, _separator, value = item.partition("=")
        pinned[key] = value
    return pinned


# ---------------------------------------------------------------------------
# manager
# ---------------------------------------------------------------------------


def manager_option_defaults() -> dict[str, object]:
    """Return the default value of every manager option, freshly built.

    The manager parsers, :func:`manager_argv_tail` and every caller that builds
    a manager namespace by hand take their defaults from here. The durability
    switches are absent on purpose: they default to ``argparse.SUPPRESS`` (see
    :func:`add_durability_arguments`), and an absent switch reads as false.

    :return: The option defaults keyed by ``argparse`` destination.
    """

    return {
        "pool": [],
        "capability": [],
        "placement_prefix": [],
        "workers": None,
        "worker_resource": [],
        "setting": [],
        "allocation": "auto",
        "count": None,
        "launcher": None,
        "inline": False,
        "detach": False,
        "by_path": False,
        "adapter_timeout": None,
        "lease_seconds": None,
        "heartbeat_interval": 30.0,
        "poll_interval": 1.0,
        "idle": False,
        "idle_timeout": 3600.0,
        "join_grace_seconds": 3600.0,
        "unsafe_persistent_takeover": False,
        "unsafe_isolated_takeover": False,
        "takeover_grace_factor": DEFAULT_TAKEOVER_GRACE_FACTOR,
        "runner_search_path": [],
        "drain_timeout": 30.0,
        "time_limit": None,
        "deadline_margin": 120.0,
        "gc_interval": None,
        "exchange": False,
        "log_level": None,
        "log_file": None,
        "json_logs": False,
    }


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
        metavar="POOL",
        help="claim only jobs of this pool (repeatable, default: default)",
    )
    parser.add_argument(
        "--count",
        type=int,
        metavar="COUNT",
        help="managers to start (default: the workspace's manager.count, or 1)",
    )
    _add_adapter_timeout(parser)
    _add_by_path_argument(parser)
    parser.add_argument(
        "--capability",
        action="append",
        metavar="CAPABILITY",
        help="advertise this capability to the scheduler (repeatable)",
    )
    parser.add_argument(
        "--placement-prefix",
        action="append",
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
    _add_setting_argument(parser)
    parser.add_argument(
        "--lease-seconds",
        type=float,
        metavar="SECONDS",
        help="lease length for this manager (default: the workspace policy's lease_seconds)",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        metavar="SECONDS",
        help="how often this manager refreshes its lease (default: 30)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
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
        metavar="SECONDS",
        help="without --idle, give up after this long if the workspace never becomes idle (default: 3600)",
    )
    parser.add_argument(
        "--join-grace-seconds",
        type=float,
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
        metavar="FACTOR",
        help="multiples of the lease a silent attempt is left alone before it may be taken over (default: 2.0)",
    )
    parser.add_argument(
        "--runner-search-path",
        action="append",
        metavar="DIRECTORY",
        help="ordered root for jobs whose runner.source is installed (repeatable)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        metavar="SECONDS",
        help=(
            "seconds to keep committing outcomes after a drain starts "
            "(stop signal or the allocation's drain point) (default: 30)"
        ),
    )
    _add_time_limit_arguments(parser)
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
        "--exchange",
        action="store_true",
        help="adopt staged exchange bundles, eject finished jobs and publish status (used by the workspace daemon)",
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
    parser.set_defaults(**manager_option_defaults())


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
        metavar="POOL",
        help="claim only jobs of this pool (repeatable, default: default)",
    )
    parser.add_argument(
        "--capability",
        action="append",
        metavar="CAPABILITY",
        help="advertise this capability to the scheduler, so capability-gated jobs become claimable (repeatable)",
    )
    parser.add_argument(
        "--placement-prefix",
        action="append",
        metavar="PREFIX",
        help="restrict every scheduling scan to jobs at or below this placement subtree (repeatable)",
    )
    parser.add_argument("--idle", action="store_true", help="keep serving the workspace when nothing is left to do")
    parser.add_argument(
        "--idle-timeout",
        type=float,
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
    _add_setting_argument(parser)
    parser.add_argument("--log-level", choices=LOG_LEVELS, help="log level for the manager log and console")
    parser.add_argument(
        "--count",
        type=int,
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
        metavar="SECONDS",
        help="how often this manager refreshes its lease (default: 30)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        metavar="SECONDS",
        help="how often this manager looks for work (default: 1)",
    )
    parser.add_argument(
        "--join-grace-seconds",
        type=float,
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
        metavar="FACTOR",
        help="multiples of the lease before a silent attempt may be taken over (default: 2.0)",
    )
    parser.add_argument(
        "--runner-search-path",
        action="append",
        metavar="DIRECTORY",
        help="ordered root for installed runners (repeatable)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        metavar="SECONDS",
        help=(
            "seconds to keep committing outcomes after a drain starts "
            "(stop signal or the allocation's drain point) (default: 30)"
        ),
    )
    _add_time_limit_arguments(parser)
    parser.add_argument("--gc-interval", type=float, metavar="SECONDS", help="background garbage collection interval")
    parser.add_argument("--log-file", metavar="PATH", help="manager log file")
    parser.add_argument("--json-logs", action="store_true", help="log one JSON object per line")
    add_durability_arguments(parser)
    parser.set_defaults(handler=handle_manager_run, **manager_option_defaults())


def manager_argv_tail(arguments: argparse.Namespace) -> list[str]:
    """Serialize the non-default manager options shared by every launch path."""

    defaults = manager_option_defaults()

    def changed(name: str) -> bool:
        return getattr(arguments, name, defaults[name]) != defaults[name]

    argv: list[str] = []
    for pool in getattr(arguments, "pool", []):
        argv += ["--pool", pool]
    for capability in getattr(arguments, "capability", []):
        argv += ["--capability", capability]
    for prefix in getattr(arguments, "placement_prefix", []):
        argv += ["--placement-prefix", prefix]
    if getattr(arguments, "lease_seconds", None) is not None:
        argv += ["--lease-seconds", str(arguments.lease_seconds)]
    if changed("heartbeat_interval"):
        argv += ["--heartbeat-interval", str(arguments.heartbeat_interval)]
    if changed("poll_interval"):
        argv += ["--poll-interval", str(arguments.poll_interval)]
    if changed("join_grace_seconds"):
        argv += ["--join-grace-seconds", str(arguments.join_grace_seconds)]
    workers = getattr(arguments, "workers", None)
    if workers is not None:
        if workers < 1:
            raise ValueError("--workers must be a positive integer")
        argv += ["--workers", str(workers)]
    for name, count in _worker_resources(getattr(arguments, "worker_resource", [])).items():
        argv += ["--worker-resource", name, str(count)]
    if changed("allocation"):
        # The path of exec:PATH may only exist where the manager runs.
        argv += ["--allocation", parse_allocation_spec(arguments.allocation)]
    if getattr(arguments, "idle", False):
        argv.append("--idle")
    elif changed("idle_timeout"):
        argv += ["--idle-timeout", str(arguments.idle_timeout)]
    if getattr(arguments, "unsafe_persistent_takeover", False):
        argv.append("--unsafe-persistent-takeover")
    if getattr(arguments, "unsafe_isolated_takeover", False):
        argv.append("--unsafe-isolated-takeover")
    if changed("takeover_grace_factor"):
        argv += ["--takeover-grace-factor", str(arguments.takeover_grace_factor)]
    for path in getattr(arguments, "runner_search_path", []):
        argv += ["--runner-search-path", path]
    if changed("drain_timeout"):
        argv += ["--drain-timeout", str(arguments.drain_timeout)]
    # Parsed here because every launch path builds this tail first: a bad
    # duration fails before any manager is started.
    seconds = _time_limit_seconds(arguments)
    if seconds is not None:
        if seconds <= _effective_margin(arguments):
            raise ValueError(
                f"--time-limit {format_duration(seconds)} does not exceed the "
                f"{_effective_margin(arguments):g} s deadline margin"
            )
        argv += ["--time-limit", arguments.time_limit]
    if changed("deadline_margin"):
        if arguments.deadline_margin < 0:
            raise ValueError("--deadline-margin cannot be negative")
        argv += ["--deadline-margin", str(arguments.deadline_margin)]
    if getattr(arguments, "gc_interval", None) is not None:
        argv += ["--gc-interval", str(arguments.gc_interval)]
    for key, value in _pinned_settings(arguments).items():
        argv += ["--setting", f"{key}={value}"]
    if getattr(arguments, "exchange", False):
        argv.append("--exchange")
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
        scheduler_resources = _scheduler_resources(os.environ)
        capacity = {**scheduler_resources, **cli_resources}
        actual_count = arguments.count if count is None else count
        assert actual_count is not None
        base_tail = manager_argv_tail(arguments)
        base_tail += split_allocation(argv_allocation(base_tail), actual_count)
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


def _manager_capacity(arguments: argparse.Namespace, allocation: Allocation | None) -> dict[str, int]:
    """Return the allocation's capacity overridden by ``--worker-resource``."""

    probed = {} if allocation is None else allocation.capacity()
    return {**probed, **_worker_resources(getattr(arguments, "worker_resource", []))}


def _run_in_process_manager(
    arguments: argparse.Namespace, root: Path, context: CLIContext, settings: Mapping[str, object]
) -> int:
    """Run one manager in this process using resolved workspace defaults."""

    workspace = Workspace(root, durable=_durable(arguments))
    # Pinned overrides win over workspace settings for this manager's lifetime; a malformed
    # confinement setting refuses the start.
    pinned = _pinned_settings(arguments)
    effective = {**settings, **pinned}
    confine_settings(effective)
    configure_logging(
        level=getattr(arguments, "log_level", None) or "warning", json_logs=getattr(arguments, "json_logs", False)
    )
    # After configure_logging so its warnings are formatted; managers.log only
    # attaches inside TaskManager, which needs the end time first.
    allocation = probe_allocation(
        getattr(arguments, "allocation", "auto"), os.environ, cpu_slots=bind_cpus_setting(effective)
    )
    capacity = _manager_capacity(arguments, allocation)
    # A probe without an end still honours the enclosing scheduler allocation.
    allocation_end = None if allocation is None else allocation.end_time
    if allocation_end is None:
        scheduler = detect_scheduler(os.environ)
        allocation_end = None if scheduler is None else scheduler.end_time(os.environ)
    end_time, deadline_margin = _manager_end_time(arguments, allocation_end)
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
        exchange=getattr(arguments, "exchange", False),
        on_attached=install_manager_log,
        end_time=end_time,
        deadline_margin=deadline_margin,
        allocation=allocation,
        setting_overrides=pinned,
    ) as manager:
        ends = "" if end_time is None else f", ends={format_duration(max(0, int(end_time - time.time())))}"
        if allocation is not None:
            nodes = f" nodes={len(allocation.nodes)}" if allocation.nodes else ""
            ends = f", allocation={allocation.kind}{nodes}{ends}"
        serving_line = (
            f"manager {manager.manager_id} serving {workspace.root} "
            f"(pools={','.join(sorted(manager.pools)) or '-'}, "
            f"capabilities={','.join(sorted(manager.capabilities)) or '-'}, "
            f"executors={','.join(sorted(manager.allowed_executors))}, "
            f"resources={','.join(f'{name}={value}' for name, value in manager.resources.items()) or '-'}{ends}); "
            f"log {log_file}"
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
                    drain_timeout=getattr(arguments, "drain_timeout", 30.0),
                )
            except NotIdleError as exc:
                print(exc.census.timeout_message(getattr(arguments, "idle_timeout", 3600.0)), file=sys.stderr)
                return 2
            if manager.drained is not None:
                print(
                    f"drained ({manager.drained}): {manager.running_attempts} attempt(s) left to lease recovery",
                    file=sys.stderr,
                )
            else:
                print(census.summary_line(), file=sys.stderr)
            advice = census.time_advice()
            if advice is not None:
                print(advice, file=sys.stderr)
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
