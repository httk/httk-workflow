"""Settings schema of ``daemon`` launchers, parsed into one frozen record."""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from ._daemon_policy import _HARD_LIMIT, _SLURM_NAME, _environment

DAEMON_KIND = "daemon"
DAEMON_LAUNCHER_NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")

_CANONICAL_INTEGER = re.compile(r"[1-9][0-9]*\Z")
_MEMORY = re.compile(r"([1-9][0-9]*)([KMGT]?)\Z")
_CPU_LIMIT = 1024
_MEMORY_LIMIT = 1_048_576
_TIME_LIMIT = 10_080
_FORCE_HINT = "; pass --force to approve it"
_GRES = re.compile(r"[A-Za-z0-9_.:,=+-]{1,255}\Z")
_CLUSTER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_MPI_ENVIRONMENT = re.compile(r"daemon\.mpi\.environment\.[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_FIXED_KEYS = frozenset(
    {
        "slurm.partition",
        "slurm.account",
        "slurm.gres",
        "slurm.reservation",
        "slurm.cpus_per_task",
        "slurm.mem",
        "slurm.time_limit",
        "slurm.mpi",
        "slurm.nodes",
        "slurm.ntasks",
        "slurm.ntasks_per_node",
        "manager.workers",
        "manager.command",
        "environment.prelude",
        "daemon.readonly_paths",
        "daemon.bwrap",
        "daemon.python",
        "daemon.sbatch",
        "daemon.squeue",
        "daemon.scancel",
        "daemon.sacct",
        "daemon.scontrol",
        "daemon.cluster",
        "daemon.slurm_conf",
        "daemon.max_submissions",
        "daemon.isolate_network",
        "daemon.mpi.control_root",
        "daemon.mpi.srun",
        "daemon.mpi.pmix_roots",
        "daemon.mpi.shm_root",
        "daemon.mpi.devices",
        "daemon.mpi.max_steps",
        "daemon.mpi.termination_grace",
    }
)
_PATH_LISTS = frozenset({"daemon.readonly_paths", "daemon.mpi.pmix_roots", "daemon.mpi.devices"})
_OTHER_SITE = frozenset(
    {
        "daemon.cluster",
        "daemon.max_submissions",
        "daemon.isolate_network",
        "daemon.mpi.max_steps",
        "daemon.mpi.termination_grace",
    }
)
_SINGLE_PATHS = frozenset(
    {
        "daemon.bwrap",
        "daemon.python",
        "daemon.sbatch",
        "daemon.squeue",
        "daemon.scancel",
        "daemon.sacct",
        "daemon.scontrol",
        "daemon.slurm_conf",
        "daemon.mpi.control_root",
        "daemon.mpi.srun",
        "daemon.mpi.shm_root",
    }
)


@dataclass(frozen=True, slots=True)
class DaemonSettings:
    """Parsed settings of one ``daemon`` launcher.

    :param cpus: CPUs per task, if set.
    :param memory_mb: Memory in MiB, if set.
    :param time_minutes: Time limit in minutes, if set.
    :param partition: Fixed Slurm partition, if set.
    :param account: Fixed Slurm account, if set.
    :param gres: Fixed Slurm generic resources, if set.
    :param reservation: Fixed Slurm reservation, if set.
    :param mpi: Whether the launcher selects PMIx MPI.
    :param nodes: Node count (1 when serial).
    :param ranks: Task count (1 when serial).
    :param ntasks_per_node: Tasks per node, MPI only.
    :param workers: Manager workers per job.
    :param manager_command: Manager command after the prelude, if set.
    :param prelude: Shell prelude text.
    :param site: Normalized ``daemon.*`` site values keyed by setting name, only those set.
    :param mpi_environment: ``daemon.mpi.environment.<NAME>`` values keyed by NAME.
    """

    cpus: int | None = None
    memory_mb: int | None = None
    time_minutes: int | None = None
    partition: str | None = None
    account: str | None = None
    gres: str | None = None
    reservation: str | None = None
    mpi: bool = False
    nodes: int = 1
    ranks: int = 1
    ntasks_per_node: int | None = None
    workers: int = 1
    manager_command: str | None = None
    prelude: str = ""
    site: Mapping[str, object] = field(default_factory=dict)
    mpi_environment: Mapping[str, str] = field(default_factory=dict)


def _positive_integer(value: object, name: str, maximum: int, hint: str = "") -> int:
    if type(value) is int:
        result = value
    elif type(value) is str and _CANONICAL_INTEGER.fullmatch(value) is not None:
        result = int(value)
    else:
        raise ValueError(f"{name} must be a positive integer or canonical decimal string")
    if not 1 <= result <= maximum:
        raise ValueError(f"{name} must be from 1 through {maximum}{hint}")
    return result


def _memory_mb(value: object, *, force: bool = False) -> int:
    if type(value) is int:
        result = value
    elif type(value) is str:
        match = _MEMORY.fullmatch(value)
        if match is None:
            raise ValueError("slurm.mem must be a positive integer with an optional K, M, G, or T suffix")
        amount, suffix = int(match.group(1)), match.group(2)
        if suffix == "K":
            result = (amount + 1023) // 1024
        elif suffix == "M" or not suffix:
            result = amount
        elif suffix == "G":
            result = amount * 1024
        else:
            result = amount * 1024 * 1024
    else:
        raise ValueError("slurm.mem must be a positive integer with an optional K, M, G, or T suffix")
    if not 1 <= result <= (_HARD_LIMIT if force else _MEMORY_LIMIT):
        if result >= 1:
            if force:
                raise ValueError(f"slurm.mem exceeds the {_HARD_LIMIT} MiB ceiling")
            raise ValueError(f"slurm.mem exceeds the {_MEMORY_LIMIT} MiB sanity limit{_FORCE_HINT}")
        raise ValueError("slurm.mem must normalize to at least 1 MiB")
    return result


def _time_minutes(value: object, *, force: bool = False) -> int:
    if type(value) is int:
        result = value
    elif type(value) is str and _CANONICAL_INTEGER.fullmatch(value) is not None:
        result = int(value)
    elif type(value) is str:
        days = 0
        clock = value
        if "-" in value:
            pieces = value.split("-")
            if len(pieces) != 2 or _CANONICAL_INTEGER.fullmatch(pieces[0]) is None:
                raise ValueError("invalid slurm.time_limit")
            days, clock = int(pieces[0]), pieces[1]
        fields = clock.split(":")
        if len(fields) not in (1, 2, 3) or any(not field.isdigit() or not field for field in fields):
            raise ValueError("invalid slurm.time_limit")
        numbers = [int(field) for field in fields]
        if days:
            if len(numbers) == 1:
                hours, minutes, seconds = numbers[0], 0, 0
            elif len(numbers) == 2:
                hours, minutes, seconds = numbers[0], numbers[1], 0
            else:
                hours, minutes, seconds = numbers
            if hours > 23 or minutes > 59 or seconds > 59:
                raise ValueError("invalid slurm.time_limit")
            total_seconds = ((days * 24 + hours) * 60 + minutes) * 60 + seconds
        elif len(numbers) == 2:
            minutes, seconds = numbers
            if seconds > 59:
                raise ValueError("invalid slurm.time_limit")
            total_seconds = minutes * 60 + seconds
        elif len(numbers) == 3:
            hours, minutes, seconds = numbers
            if minutes > 59 or seconds > 59:
                raise ValueError("invalid slurm.time_limit")
            total_seconds = (hours * 60 + minutes) * 60 + seconds
        else:
            raise ValueError("invalid slurm.time_limit")
        result = (total_seconds + 59) // 60
    else:
        raise ValueError("invalid slurm.time_limit")
    if not 1 <= result <= (_HARD_LIMIT if force else _TIME_LIMIT):
        if result >= 1:
            if force:
                raise ValueError(f"slurm.time_limit exceeds the {_HARD_LIMIT} minute ceiling")
            raise ValueError(f"slurm.time_limit exceeds the {_TIME_LIMIT} minute sanity limit{_FORCE_HINT}")
        raise ValueError("slurm.time_limit must normalize to at least 1 minute")
    return result


def _text(value: object, key: str) -> str:
    if type(value) is not str or "\0" in value:
        raise ValueError(f"{key} must be a string without NUL")
    return value


def _pattern(value: object, key: str, pattern: re.Pattern[str]) -> str:
    text = _text(value, key)
    if pattern.fullmatch(text) is None:
        raise ValueError(f"{key} has an invalid value")
    return text


def _path(text: str, key: str) -> Path:
    path = Path(text)
    if not text or not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{key} must be an absolute path without '..': {text!r}")
    return path


def _grace(value: object) -> float:
    if type(value) in (int, float):
        number = float(value)  # type: ignore[arg-type]
    elif type(value) is str:
        try:
            number = float(value)
        except ValueError:
            number = float("nan")
    else:
        number = float("nan")
    if not 0.1 <= number <= 60.0:
        raise ValueError("daemon.mpi.termination_grace must be a number from 0.1 through 60.0")
    return number


def _flag(value: object, key: str) -> bool:
    # Launcher settings are JSON scalars without booleans, so a flag is spelled as text or 1/0.
    if type(value) is str and value.lower() in ("true", "false"):
        return value.lower() == "true"
    if type(value) is int and value in (0, 1):
        return value == 1
    raise ValueError(f"{key} must be 'true', 'false', 1 or 0")


def _site_value(key: str, value: object) -> object:
    if key in _PATH_LISTS:
        return tuple(_path(entry, key) for entry in _text(value, key).split(":"))
    if key in _SINGLE_PATHS:
        return _path(_text(value, key), key)
    if key == "daemon.cluster":
        return _pattern(value, key, _CLUSTER)
    if key == "daemon.max_submissions":
        return _positive_integer(value, key, 4096)
    if key == "daemon.isolate_network":
        return _flag(value, key)
    if key == "daemon.mpi.max_steps":
        return _positive_integer(value, key, 65_536)
    return _grace(value)


def parse_daemon_settings(settings: Mapping[str, object], *, force: bool) -> DaemonSettings:
    """Validate and normalize the settings of one ``daemon`` launcher.

    :param settings: The launcher's ``settings`` object.
    :param force: Whether operator-approved sanity limits are lifted to the hard ceiling.
    :return: The parsed settings.
    :raises ValueError: If a key is unsupported or a value is invalid.
    """

    unknown = sorted(k for k in settings if k not in _FIXED_KEYS and _MPI_ENVIRONMENT.fullmatch(k) is None)
    if unknown:
        raise ValueError(f"unsupported daemon launcher setting: {', '.join(unknown)}")
    cpus = memory_mb = time_minutes = None
    if "slurm.cpus_per_task" in settings:
        cpus = _positive_integer(
            settings["slurm.cpus_per_task"],
            "slurm.cpus_per_task",
            _HARD_LIMIT if force else _CPU_LIMIT,
            "" if force else _FORCE_HINT,
        )
    if "slurm.mem" in settings:
        memory_mb = _memory_mb(settings["slurm.mem"], force=force)
    if "slurm.time_limit" in settings:
        time_minutes = _time_minutes(settings["slurm.time_limit"], force=force)
    names = {
        key: _pattern(settings[key], key, _SLURM_NAME)
        for key in ("slurm.partition", "slurm.account", "slurm.reservation")
        if key in settings
    }
    gres = _pattern(settings["slurm.gres"], "slurm.gres", _GRES) if "slurm.gres" in settings else None
    workers = _positive_integer(settings.get("manager.workers", 1), "manager.workers", 1024)
    prelude = _text(settings.get("environment.prelude", ""), "environment.prelude")
    command = settings.get("manager.command")
    manager_command: str | None = None
    if command is not None:
        if type(command) is not str or not command.strip() or "\0" in command:
            raise ValueError("manager.command must be one nonempty executable string")
        manager_command = command.strip()
    mpi_mode = settings.get("slurm.mpi")
    if mpi_mode is not None and mpi_mode != "pmix":
        raise ValueError("slurm.mpi must be 'pmix' when set")
    nodes, ranks, ntasks_per_node = 1, 1, None
    if mpi_mode is None:
        for key in ("slurm.nodes", "slurm.ntasks", "slurm.ntasks_per_node"):
            if key in settings and settings[key] is None:
                raise ValueError(f"{key} must be a positive integer")
            if key in settings and not (type(settings[key]) is int and settings[key] == 1 or settings[key] == "1"):
                raise ValueError(
                    f"{key}={settings[key]} requires slurm.mpi=pmix; serial daemon launchers use one node and one task"
                )
    else:
        if workers != 1:
            raise ValueError("MPI approved configurations require manager.workers=1")
        nodes = _positive_integer(settings.get("slurm.nodes", 1), "slurm.nodes", 4096)
        if "slurm.ntasks_per_node" in settings:
            ntasks_per_node = _positive_integer(settings["slurm.ntasks_per_node"], "slurm.ntasks_per_node", 65_536)
        if "slurm.ntasks" in settings:
            ranks = _positive_integer(settings["slurm.ntasks"], "slurm.ntasks", 65_536)
        else:
            ranks = nodes * ntasks_per_node if ntasks_per_node is not None else nodes
        if ranks < nodes or ranks > 65_536:
            raise ValueError("slurm.ntasks must be from slurm.nodes through 65536")
        if ntasks_per_node is not None and ranks > nodes * ntasks_per_node:
            raise ValueError("slurm.ntasks exceeds the approved node placement")
    prefix = "daemon.mpi.environment."
    return DaemonSettings(
        cpus,
        memory_mb,
        time_minutes,
        names.get("slurm.partition"),
        names.get("slurm.account"),
        gres,
        names.get("slurm.reservation"),
        mpi_mode is not None,
        nodes,
        ranks,
        ntasks_per_node,
        workers,
        manager_command,
        prelude,
        {
            key: _site_value(key, value)
            for key, value in settings.items()
            if key in _PATH_LISTS | _SINGLE_PATHS | _OTHER_SITE
        },
        dict(
            _environment(
                tuple(
                    (key.removeprefix(prefix), _text(value, key))
                    for key, value in settings.items()
                    if key.startswith(prefix)
                )
            )
        ),
    )
