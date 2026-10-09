"""Attempt confinement: the ``manager.confine`` and ``confine.*`` settings and the Bubblewrap attempt sandbox.

The manager is trusted and runs unconfined; with ``manager.confine=bwrap`` it starts every attempt inside
the sandbox built here. The workspace is visible read-only at its real path and only the attempt's own job
directory is writable, so ``HTTK_WORKFLOW_*_DIR`` paths stay valid inside the sandbox unchanged.
"""

import logging
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httk

from ._sandbox import (
    BWRAP_USERNS_BLOCK,
    O_PATH,
    PreparedSandbox,
    check_bwrap,
    device_parent_dirs,
    make_inheritable,
    merged_usr_symlinks,
    open_device_nofollow,
    open_directory_nofollow,
)
from .errors import ConfinementUnavailableError

_LOGGER = logging.getLogger(__name__)

#: The non-``confine.*`` keys a manager accepts as pinned ``--setting`` overrides.
CONFINE_OVERRIDE_KEYS = frozenset(
    {
        "manager.confine",
        "manager.confine.block_mpi_spawn",
        "manager.launch_template",
        "manager.launch_mpi",
        "manager.bind_cpus",
    }
)
#: Every key with this prefix is a confinement setting, and may also be pinned.
CONFINE_PREFIX = "confine."
_ENVIRONMENT_PREFIX = "confine.environment."
_KNOWN_KEYS = (
    "confine.bwrap",
    "confine.devices",
    "confine.isolate_network",
    "confine.pmix_roots",
    "confine.readonly_paths",
    "confine.shm_root",
)
_MODES: dict[str, Literal["none", "bwrap"]] = {"none": "none", "bwrap": "bwrap"}
_SPAWN_MODES: dict[str, Literal["on", "off", "auto"]] = {"on": "on", "off": "off", "auto": "auto"}
_BOOLEANS = {"true": True, "false": False, "1": True, "0": False}
_LAUNCH_MPI = re.compile(r"[a-z0-9_]{1,32}\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_ENVIRONMENT_VALUE_BYTES = 4096
_DEFAULT_READONLY = ("/usr", "/bin", "/lib", "/lib64", "/etc")
_DEFAULT_SHM_ROOT = Path("/dev/shm")
# Hiding the scheduler and process-manager environment keeps MPI started inside an attempt on the local node.
_DROPPED_PREFIXES = ("SLURM_", "SRUN_", "SBATCH_", "SALLOC_", "PMI_", "PMIX_")
_CONFINED_ENVIRONMENT = {"HOME": "/tmp/home", "TMPDIR": "/tmp", "HTTK_WORKFLOW_CONFINED": "1"}
_PROBE_ENVIRONMENT = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
_PROBE_TIMEOUT = 10.0
_STDERR_EXCERPT = 400


@dataclass(frozen=True, slots=True)
class ConfineSettings:
    """Validated confinement settings of one manager.

    :param mode: ``none`` runs attempts unconfined; ``bwrap`` confines each attempt with Bubblewrap.
    :param readonly_paths: Paths bound read-only at their own paths.
    :param isolate_network: Whether attempts get a private network namespace.
    :param bwrap: The Bubblewrap executable, or ``None`` when none was configured or found.
    :param devices: Device nodes bound into attempts and ranks.
    :param pmix_roots: Approved parents of a per-step PMIx directory exposed to ranks.
    :param shm_root: The node-local parent of per-launch shared-memory directories.
    :param environment: Extra variables set in rank sandboxes, in name order.
    :param block_mpi_spawn: ``manager.confine.block_mpi_spawn``: whether confined ranks may not spawn MPI
        processes (``on``, ``off`` or ``auto``); it only applies in ``bwrap`` mode.
    """

    mode: Literal["none", "bwrap"]
    readonly_paths: tuple[Path, ...]
    isolate_network: bool
    bwrap: Path | None
    devices: tuple[Path, ...]
    pmix_roots: tuple[Path, ...]
    shm_root: Path
    environment: tuple[tuple[str, str], ...]
    block_mpi_spawn: Literal["on", "off", "auto"] = "on"


def is_override_key(key: str) -> bool:
    """Return whether a manager accepts *key* as a pinned ``--setting`` override.

    :param key: The setting key.
    :return: Whether the key is one of :data:`CONFINE_OVERRIDE_KEYS` or a ``confine.*`` key.
    """

    return key in CONFINE_OVERRIDE_KEYS or key.startswith(CONFINE_PREFIX)


def _nested_dropped(paths: set[Path]) -> tuple[Path, ...]:
    return tuple(
        sorted(path for path in paths if not any(path != other and path.is_relative_to(other) for other in paths))
    )


def _editable_finder_roots() -> set[Path]:
    # Setuptools finder-hook editable installs appear in httk.__path__ only as hook sentinels; their
    # MAPPING gives each package's source directory, whose import root is above the dotted name.
    roots: set[Path] = set()
    for name, module in list(sys.modules.items()):
        mapping = getattr(module, "MAPPING", None) if name.startswith("__editable___") else None
        if not isinstance(mapping, dict):
            continue
        for package, location in mapping.items():
            if isinstance(package, str) and isinstance(location, str) and package.split(".")[0] == "httk":
                root = Path(location)
                for _part in package.split("."):
                    root = root.parent
                roots.add(root.resolve())
    return roots


def default_readonly_paths() -> tuple[Path, ...]:
    """Return the read-only paths used when ``confine.readonly_paths`` is unset.

    :return: The system directories, *httk* import roots and Python prefixes of this interpreter, nested entries dropped.
    """

    defaults = {Path(path).resolve() for path in _DEFAULT_READONLY if os.path.exists(path)}
    # Jobs import the manager's own code, which an editable install keeps outside the prefix.
    imports = {Path(entry).resolve().parent for entry in httk.__path__ if os.path.isabs(entry)}
    imports |= _editable_finder_roots()
    prefixes = {Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve()}
    return _nested_dropped(defaults | imports | prefixes)


def _string(settings: Mapping[str, object], key: str) -> str | None:
    value = settings.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"setting {key} must be a string, not {type(value).__name__}")
    return value


def _absolute(key: str, text: str) -> Path:
    path = Path(text)
    if not text or "\0" in text or not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"setting {key} needs absolute paths without '..', NUL or empty entries: {text!r}")
    return path


def _paths(settings: Mapping[str, object], key: str) -> tuple[Path, ...] | None:
    text = _string(settings, key)
    if text == "":
        return ()
    return None if text is None else tuple(_absolute(key, item) for item in text.split(":"))


def _boolean(settings: Mapping[str, object], key: str, default: bool) -> bool:
    value = settings.get(key)
    if value is None:
        return default
    if isinstance(value, str) and value.lower() in _BOOLEANS:
        return _BOOLEANS[value.lower()]
    if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
        return value == 1
    raise ValueError(f"setting {key} must be true or false: {value!r}")


def _readonly_paths(settings: Mapping[str, object]) -> tuple[Path, ...]:
    paths = _paths(settings, "confine.readonly_paths")
    if paths is None:
        return default_readonly_paths()
    for path in paths:
        # The private /tmp, its home and the sandbox's own /proc and /dev must stay what the sandbox mounts.
        if (
            path in (Path("/"), Path("/tmp"))
            or path.is_relative_to("/tmp/home")
            or path.is_relative_to("/proc")
            or path.is_relative_to("/dev")
        ):
            raise ValueError(f"setting confine.readonly_paths must not cover /, /tmp, /tmp/home, /proc or /dev: {path}")
    return paths


def _devices(settings: Mapping[str, object]) -> tuple[Path, ...]:
    devices = _paths(settings, "confine.devices") or ()
    for device in devices:
        if device == Path("/dev") or not device.is_relative_to("/dev"):
            raise ValueError(f"setting confine.devices entries must be device nodes below /dev: {device}")
    return devices


def _environment(settings: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    environment: list[tuple[str, str]] = []
    for key in sorted(settings):
        if not key.startswith(_ENVIRONMENT_PREFIX):
            continue
        name = key.removeprefix(_ENVIRONMENT_PREFIX)
        if _ENVIRONMENT_NAME.fullmatch(name) is None or name.startswith("HTTK_"):
            raise ValueError(f"setting {key} must name a variable matching {_ENVIRONMENT_NAME.pattern} outside HTTK_*")
        value = settings[key]
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError(f"setting {key} must be a string or number")
        text = str(value)
        if "\0" in text or len(text.encode("utf-8")) > _ENVIRONMENT_VALUE_BYTES:
            raise ValueError(f"setting {key} must be NUL-free and at most {_ENVIRONMENT_VALUE_BYTES} bytes")
        environment.append((name, text))
    return tuple(environment)


def _default_bwrap() -> Path | None:
    found = shutil.which("bwrap")
    return None if found is None else Path(found).resolve()


def confine_settings(settings: Mapping[str, object]) -> ConfineSettings:
    """Validate and return the confinement settings of an effective settings mapping.

    Keys other than ``manager.confine``, ``manager.confine.block_mpi_spawn`` and ``confine.*`` are ignored; a ``null`` value reads as unset.

    :param settings: Effective settings: workspace settings with the manager's pinned overrides applied.
    :return: The validated settings with defaults filled in.
    :raises ValueError: If a value is malformed or a ``confine.*`` key is unknown.
    """

    for key in settings:
        if key.startswith(CONFINE_PREFIX) and key not in _KNOWN_KEYS and not key.startswith(_ENVIRONMENT_PREFIX):
            raise ValueError(
                f"unknown confinement setting {key!r}; known: {', '.join(_KNOWN_KEYS)}, confine.environment.<NAME>"
            )
    raw_mode = settings.get("manager.confine")
    if raw_mode is None:
        raw_mode = "none"
    mode = _MODES.get(raw_mode) if isinstance(raw_mode, str) else None
    if mode is None:
        raise ValueError(f"setting manager.confine must be none or bwrap: {raw_mode!r}")
    raw_spawn = settings.get("manager.confine.block_mpi_spawn")
    if raw_spawn is None:
        raw_spawn = "on"
    block_mpi_spawn = _SPAWN_MODES.get(raw_spawn) if isinstance(raw_spawn, str) else None
    if block_mpi_spawn is None:
        raise ValueError(f"setting manager.confine.block_mpi_spawn must be on, off or auto: {raw_spawn!r}")
    bwrap_text = _string(settings, "confine.bwrap")
    shm_root = _string(settings, "confine.shm_root")
    return ConfineSettings(
        mode=mode,
        readonly_paths=_readonly_paths(settings),
        isolate_network=_boolean(settings, "confine.isolate_network", True),
        bwrap=_default_bwrap() if bwrap_text is None else _absolute("confine.bwrap", bwrap_text),
        devices=_devices(settings),
        pmix_roots=_paths(settings, "confine.pmix_roots") or (),
        shm_root=_DEFAULT_SHM_ROOT if shm_root is None else _absolute("confine.shm_root", shm_root),
        environment=_environment(settings),
        block_mpi_spawn=block_mpi_spawn,
    )


def launch_mpi_setting(settings: Mapping[str, object]) -> str | None:
    """Return the validated ``manager.launch_mpi`` setting, the Slurm MPI plugin of the built-in step, or ``None``.

    :param settings: Effective settings: workspace settings with the manager's pinned overrides applied.
    :return: The plugin name, or ``None`` when the setting is unset.
    :raises ValueError: If the value is not a plugin name of 1-32 lowercase letters, digits or underscores.
    """

    value = _string(settings, "manager.launch_mpi")
    if value is not None and not _LAUNCH_MPI.match(value):
        raise ValueError(f"setting manager.launch_mpi must be 1-32 lowercase letters, digits or underscores: {value!r}")
    return value


def filtered_attempt_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """Return the environment of a confined attempt.

    The scheduler and process-manager variables (``SLURM_*``, ``SRUN_*``, ``SBATCH_*``, ``SALLOC_*``,
    ``PMI_*``, ``PMIX_*``) are removed, and ``HOME``, ``TMPDIR`` and ``HTTK_WORKFLOW_CONFINED`` are set to
    the sandbox's private values.

    :param environment: The attempt environment the manager built.
    :return: A new filtered environment.
    """

    kept = {name: value for name, value in environment.items() if not name.startswith(_DROPPED_PREFIXES)}
    return kept | _CONFINED_ENVIRONMENT


def _directory_descriptor(descriptor: int, path: Path) -> int:
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        raise ValueError(f"sandbox source is not a directory: {path}")
    return os.dup(descriptor)


def _namespace_options(settings: ConfineSettings, *, block_userns: bool) -> list[str]:
    assert settings.bwrap is not None
    argv = [str(settings.bwrap), "--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts"]
    if settings.isolate_network:
        argv.append("--unshare-net")
    if block_userns:
        argv += BWRAP_USERNS_BLOCK
    return [*argv, "--cap-drop", "ALL"]


def prepare_attempt_sandbox(
    settings: ConfineSettings,
    *,
    workspace_root: Path,
    workspace_fd: int,
    job_path: Path,
    job_fd: int,
    workdir: Path,
    environment: Mapping[str, str],
    block_userns: bool,
) -> PreparedSandbox:
    """Build the Bubblewrap argument vector and descriptor set of one confined attempt.

    The sandbox starts from Bubblewrap's empty root with a private ``/tmp`` (and ``/tmp/home``), then binds
    ``settings.readonly_paths`` read-only, the workspace read-only at its real path and the job directory
    writable at its real path, in that order: a read-only path may be an ancestor of the workspace (``/home``
    for a workspace below it), because the later workspace and job binds overlay it. A read-only path at or
    inside the workspace is refused. ``/proc``, a private ``/dev`` and the device binds follow. There is no
    ``--new-session`` and no ``--die-with-parent``: the sandbox stays in its launcher's process group, so
    ``killpg`` reaches the sandboxed command, and an attempt survives a manager exit as unconfined ones do.

    No environment value enters the argument vector, where ``/proc/<pid>/cmdline`` would show it to every
    user: Bubblewrap passes its own environment through, so the caller starts the launcher that execs
    Bubblewrap with ``env=`` the filtered environment it also passes here.

    :param settings: The validated confinement settings.
    :param workspace_root: The workspace root's real path.
    :param workspace_fd: A directory descriptor of the workspace root; duplicated, not consumed.
    :param job_path: The job directory's real path, inside the workspace.
    :param job_fd: A directory descriptor of the job directory; duplicated, not consumed.
    :param workdir: The attempt's working directory, inside the job directory.
    :param environment: The launcher's environment, already filtered with :func:`filtered_attempt_environment`.
    :param block_userns: Whether to block nested user namespaces (the result of :func:`probe_bwrap`).
    :return: The argument vector ending in ``--``, to which the caller appends the command, and the
        inheritable descriptors to pass with ``pass_fds``; the caller closes them after the spawn.
    :raises ValueError: If a path is not absolute, misplaced, unavailable or overlaps the workspace, or the
        environment is not a filtered attempt environment.
    :raises ConfinementUnavailableError: If no Bubblewrap executable is configured.
    """

    if settings.bwrap is None:
        raise ConfinementUnavailableError("manager.confine=bwrap needs Bubblewrap: none on PATH; set confine.bwrap")
    for path in (workspace_root, job_path, workdir):
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError(f"sandbox paths must be absolute without '..': {path}")
    if job_path == workspace_root or not job_path.is_relative_to(workspace_root):
        raise ValueError(f"job directory {job_path} is not inside the workspace {workspace_root}")
    if not workdir.is_relative_to(job_path):
        raise ValueError(f"attempt working directory {workdir} is not inside the job directory {job_path}")
    if dict(environment) != filtered_attempt_environment(environment):
        raise ValueError("a confined attempt needs its environment filtered with filtered_attempt_environment")

    argv = [*_namespace_options(settings, block_userns=block_userns), "--tmpfs", "/tmp", "--dir", "/tmp/home"]
    descriptors: list[int] = []
    try:
        resolved: list[Path] = []
        for path in settings.readonly_paths:
            try:
                real = path.resolve(strict=True)
            except (OSError, RuntimeError) as exc:
                raise ValueError(f"confine.readonly_paths entry is unavailable: {path}") from exc
            if any(item.is_relative_to(workspace_root) for item in (path, real)):
                raise ValueError(f"confine.readonly_paths entry {path} lies inside the workspace {workspace_root}")
            descriptors.append(os.open(real, O_PATH | os.O_CLOEXEC))
            argv += ["--ro-bind-fd", str(descriptors[-1]), str(path)]
            resolved.append(real)
        argv += merged_usr_symlinks(tuple(resolved), set(settings.readonly_paths))
        descriptors.append(_directory_descriptor(workspace_fd, workspace_root))
        argv += ["--ro-bind-fd", str(descriptors[-1]), str(workspace_root)]
        descriptors.append(_directory_descriptor(job_fd, job_path))
        argv += ["--bind-fd", str(descriptors[-1]), str(job_path)]
        argv += ["--proc", "/proc", "--dev", "/dev"]
        created: set[Path] = set()
        for device in settings.devices:
            argv += device_parent_dirs(device, created)
            descriptors.append(open_device_nofollow(device))
            argv += ["--bind-fd", str(descriptors[-1]), str(device)]
        argv += ["--chdir", str(workdir), "--"]
        make_inheritable(descriptors)
        return PreparedSandbox(argv, tuple(descriptors))
    except BaseException:
        PreparedSandbox([], tuple(descriptors)).close()
        raise


def _probe(settings: ConfineSettings, true: str, *, block_userns: bool) -> subprocess.CompletedProcess[str] | None:
    argv = [
        *_namespace_options(settings, block_userns=block_userns),
        "--ro-bind",
        "/",
        "/",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--",
        true,
    ]
    try:
        return subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT,
            check=False,
            env=dict(_PROBE_ENVIRONMENT),
        )
    except subprocess.TimeoutExpired:
        return None
    except OSError as exc:
        raise ConfinementUnavailableError(f"cannot run Bubblewrap {settings.bwrap} (confine.bwrap): {exc}") from exc


def _excerpt(result: subprocess.CompletedProcess[str] | None) -> str:
    if result is None:
        return f"timed out after {_PROBE_TIMEOUT:g} s"
    text = (result.stderr or result.stdout or "").strip()
    return text[-_STDERR_EXCERPT:] or f"exit status {result.returncode}"


def probe_bwrap(settings: ConfineSettings) -> bool:
    """Run a trivial sandbox with the attempt namespace set to prove Bubblewrap works here.

    Called once at manager start under ``manager.confine=bwrap``. Nested user namespaces are blocked when
    Bubblewrap lists ``--disable-userns`` and the probe passes with it; otherwise the manager continues
    without that block and logs a warning.

    :param settings: The validated confinement settings.
    :return: Whether attempts block nested user namespaces.
    :raises ConfinementUnavailableError: If Bubblewrap is missing or cannot create the sandbox, or
        ``confine.shm_root`` is not a usable tmpfs directory.
    """

    if settings.bwrap is None:
        raise ConfinementUnavailableError("manager.confine=bwrap needs Bubblewrap: none on PATH; set confine.bwrap")
    # Imported here: the rank helper runs as ``python -m httk.workflow._confine_rank``, and importing it while
    # the package loads would make runpy warn about the module already being in ``sys.modules``.
    from ._confine_rank import _check_shm_root

    try:
        root_fd = open_directory_nofollow(settings.shm_root)
    except OSError as exc:
        raise ConfinementUnavailableError(f"cannot open confine.shm_root {settings.shm_root}: {exc}") from exc
    try:
        _check_shm_root(root_fd, settings.shm_root)
    except ValueError as exc:
        raise ConfinementUnavailableError(str(exc)) from exc
    finally:
        os.close(root_fd)
    try:
        listed = check_bwrap(settings.bwrap)
    except ValueError as exc:
        raise ConfinementUnavailableError(f"Bubblewrap {settings.bwrap} (confine.bwrap) is unusable: {exc}") from exc
    true = shutil.which("true", path=_PROBE_ENVIRONMENT["PATH"]) or "/bin/true"
    blocked = None
    if listed:
        blocked = _probe(settings, true, block_userns=True)
        if blocked is not None and blocked.returncode == 0:
            return True
    plain = _probe(settings, true, block_userns=False)
    if plain is None or plain.returncode != 0:
        raise ConfinementUnavailableError(
            f"Bubblewrap {settings.bwrap} cannot create the attempt sandbox; manager.confine=bwrap needs "
            f"unprivileged user namespaces and a PID namespace with its own /proc: {_excerpt(plain)}"
        )
    if listed:
        _LOGGER.warning(
            "Bubblewrap %s refuses --disable-userns here (%s); confined attempts can create nested user namespaces",
            settings.bwrap,
            _excerpt(blocked),
        )
    else:
        _LOGGER.warning(
            "Bubblewrap %s lacks --disable-userns (0.8.0+); confined attempts can create nested user namespaces",
            settings.bwrap,
        )
    return False
