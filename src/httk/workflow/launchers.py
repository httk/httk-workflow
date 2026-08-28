"""Versioned bundles that start workflow managers."""

import importlib.resources
import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .configuration import launchers_home
from .errors import ResolutionMiss
from .projects import PROJECT_DIRECTORY, discover_project

__all__ = [
    "LAUNCHER_EXECUTABLE",
    "LAUNCHER_FILE",
    "LAUNCHER_FORMAT",
    "LAUNCHER_METADATA",
    "LAUNCHER_OPERATIONS",
    "PROCESS_LAUNCHER",
    "RESULT_FORMAT",
    "LauncherTarget",
    "add_launcher",
    "check_launcher",
    "describe_launcher",
    "host_capacity",
    "launch_processes",
    "list_launchers",
    "project_launcher_roots",
    "remove_launcher",
    "resolve_launcher",
    "run_launcher",
    "split_capacity",
    "start_managers",
    "valid_launcher_name",
    "validate_launcher_bundle",
]

LAUNCHER_FILE = "launcher"
LAUNCHER_EXECUTABLE = LAUNCHER_FILE
LAUNCHER_METADATA = "launcher.json"
LAUNCHER_FORMAT = "httk-manager-launcher"
LAUNCHER_OPERATIONS = ("check", "start")
PROCESS_LAUNCHER = "process"
REQUEST_FORMAT = "httk-manager-launcher-request"
RESULT_FORMAT = "httk-manager-launcher-result"


@dataclass(frozen=True)
class LauncherTarget:
    """Resolved manager launcher bundle.

    :param name: The launcher name.
    :param bundle: The resolved launcher bundle path.
    :param project_local: Whether the bundle came from project data.
    """

    name: str
    bundle: Path
    project_local: bool


def _metadata_path(bundle: str | os.PathLike[str]) -> Path:
    return Path(bundle).expanduser() / LAUNCHER_METADATA


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read launcher JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"launcher JSON must be an object: {path}")
    return value


def validate_launcher_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate launcher metadata, executable, and local binary requirements.

    :param bundle: The launcher bundle path.
    :return: The validated metadata document.
    :raises ValueError: If the bundle is malformed or unavailable locally.
    """

    root = Path(bundle).expanduser().resolve()
    metadata = _read_object(_metadata_path(root))
    if metadata.get("format") != LAUNCHER_FORMAT or metadata.get("format_version") != 2:
        raise ValueError(f"{LAUNCHER_METADATA} must use {LAUNCHER_FORMAT} format version 2")
    if metadata.get("launcher_version") != 2:
        raise ValueError(f"unsupported manager launcher version: {metadata.get('launcher_version')!r}")
    executable = root / LAUNCHER_EXECUTABLE
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError(f"launcher executable is not runnable: {executable}")
    settings = metadata.get("settings", {})
    if not isinstance(settings, Mapping):
        raise ValueError("manager launcher settings must be an object")
    timeout = metadata.get("timeout_seconds", 60.0)
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("launcher timeout_seconds must be positive")
    binaries = metadata.get("required_binaries", [])
    if not isinstance(binaries, Sequence) or isinstance(binaries, (str, bytes)):
        raise ValueError("required_binaries must be an array")
    for binary in binaries:
        if not isinstance(binary, str) or not binary:
            raise ValueError("required_binaries entries must be nonempty strings")
        if shutil.which(binary) is None:
            raise ValueError(f"required launcher binary is unavailable: {binary}")
    return metadata


def valid_launcher_name(name: str) -> str:
    """Validate and return one launcher name.

    :param name: The launcher name.
    :return: The validated launcher name.
    :raises ResolutionMiss: If the name cannot identify a bundle.
    """

    if not name or "/" in name or ":" in name or name in {".", ".."}:
        raise ResolutionMiss(f"invalid launcher name: {name!r}")
    return name


def project_launcher_roots(project: Path) -> tuple[Path, ...]:
    """Return where a project keeps its manager launchers.

    :param project: The project root.
    :return: Project launcher roots in precedence order.
    """

    return (project / PROJECT_DIRECTORY / "launchers",)


def resolve_launcher(
    name: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> LauncherTarget:
    """Resolve a project launcher before a global launcher of the same name.

    :param name: The launcher name.
    :param project: The project path, or the discovered project when omitted.
    :return: The resolved launcher target.
    :raises ResolutionMiss: If no launcher bundle is found.
    :raises ValueError: If the found bundle is invalid.
    """

    name = valid_launcher_name(name)
    project_root = discover_project(project)
    candidates: list[tuple[Path, bool]] = []
    if project_root is not None:
        candidates.extend((root / name, True) for root in project_launcher_roots(project_root))
    candidates.append((launchers_home() / name, False))
    for bundle, local in candidates:
        if bundle.is_dir():
            validate_launcher_bundle(bundle)
            return LauncherTarget(name, bundle, local)
    raise ResolutionMiss(f"unknown launcher: {name}")


def list_launchers(project: str | os.PathLike[str] | None = None) -> list[dict[str, object]]:
    """List project and global launchers with project shadowing global entries.

    :param project: The project path, or the discovered project when omitted.
    :return: Launcher names, scopes, and bundle paths.
    """

    rows: dict[str, dict[str, object]] = {}
    global_root = launchers_home()
    if global_root.is_dir():
        for path in sorted(global_root.iterdir()):
            if path.is_dir():
                rows[path.name] = {"name": path.name, "scope": "global", "path": str(path)}
    project_root = discover_project(project)
    if project_root is not None:
        for local_root in reversed(project_launcher_roots(project_root)):
            if local_root.is_dir():
                for path in sorted(local_root.iterdir()):
                    if path.is_dir():
                        rows[path.name] = {"name": path.name, "scope": "project", "path": str(path)}
    return [rows[name] for name in sorted(rows)]


def add_launcher(
    name: str,
    *,
    template: str,
    project: str | os.PathLike[str] | None = None,
    global_: bool = False,
) -> Path:
    """Copy one maintained launcher template into project or global data.

    :param name: The launcher name to create.
    :param template: The maintained launcher template name.
    :param project: The project path for a project-local launcher.
    :param global_: Whether to create the launcher in global data.
    :return: The newly created launcher bundle path.
    :raises ValueError: If the name, template, or scope is invalid.
    :raises FileExistsError: If the destination already exists.
    """

    valid_launcher_name(name)
    if name == PROCESS_LAUNCHER:
        raise ValueError("the launcher name 'process' is reserved for the built-in process launcher")
    if template != "slurm":
        raise ValueError(f"unknown maintained launcher template: {template}")
    if global_:
        destination = launchers_home() / name
    else:
        project_root = discover_project(project)
        if project_root is None:
            raise ValueError("a project is required unless --global is used")
        destination = project_launcher_roots(project_root)[0] / name
    if destination.exists():
        raise FileExistsError(destination)
    source = importlib.resources.files("httk.workflow").joinpath("launch_templates", template)
    with importlib.resources.as_file(source) as template_path:
        validate_launcher_bundle(template_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(template_path, destination)
    os.chmod(destination / LAUNCHER_EXECUTABLE, 0o755)
    validate_launcher_bundle(destination)
    return destination


def _find_launcher_bundle(name: str, project: str | os.PathLike[str] | None) -> tuple[Path, str]:
    valid_launcher_name(name)
    project_root = discover_project(project)
    if project_root is not None:
        for root in project_launcher_roots(project_root):
            bundle = root / name
            if bundle.is_dir():
                return bundle, "project"
    bundle = launchers_home() / name
    if bundle.is_dir():
        return bundle, "global"
    raise ValueError(f"unknown launcher: {name}")


def describe_launcher(
    name: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> dict[str, object]:
    """Describe one launcher without exposing any private bundle contents.

    :param name: Launcher bundle name.
    :param project: Project directory used for project-local lookup.
    :return: JSON-compatible launcher description.
    """

    bundle, scope = _find_launcher_bundle(name, project)
    try:
        metadata = validate_launcher_bundle(bundle)
        valid, problem = True, None
    except (OSError, ValueError) as exc:
        metadata = _read_object(_metadata_path(bundle)) if _metadata_path(bundle).is_file() else {}
        valid, problem = False, str(exc)
    return {
        "format": "httk-launcher-description",
        "format_version": 2,
        "name": name,
        "scope": scope,
        "bundle": str(bundle),
        "valid": valid,
        "problem": problem,
        "kind": metadata.get("kind"),
        "launcher_version": metadata.get("launcher_version"),
        "timeout_seconds": metadata.get("timeout_seconds", 60.0),
        "required_binaries": list(metadata.get("required_binaries", [])),
        "launcher": str(bundle / LAUNCHER_EXECUTABLE),
        "settings": dict(metadata.get("settings", {})) if isinstance(metadata.get("settings", {}), Mapping) else {},
    }


def remove_launcher(
    name: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> dict[str, object]:
    """Remove one project-local or global launcher bundle.

    :param name: Launcher bundle name.
    :param project: Project directory used for project-local lookup.
    :return: JSON-compatible removal result.
    """

    bundle, scope = _find_launcher_bundle(name, project)
    shutil.rmtree(bundle)
    return {"name": name, "scope": scope, "bundle": str(bundle), "removed": True}


def run_launcher(
    bundle: str | os.PathLike[str],
    operation: str,
    request: Mapping[str, object],
    *,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Execute one launcher operation without invoking a shell.

    :param bundle: The launcher bundle path.
    :param operation: The launcher operation name.
    :param request: Operation-specific request members.
    :param timeout: Operation timeout, or the bundle default when omitted.
    :return: The launcher result document.
    :raises TimeoutError: If the launcher exceeds its timeout.
    :raises ValueError: If the request or result violates the protocol.
    :raises RuntimeError: If the launcher refuses or fails.
    """

    if operation not in LAUNCHER_OPERATIONS:
        raise ValueError(f"unknown launcher operation: {operation}")
    root = Path(bundle).expanduser().resolve()
    metadata = validate_launcher_bundle(root)
    descriptor, temporary_name = tempfile.mkstemp(prefix="httk-launcher-", suffix=".json")
    request_path = Path(temporary_name)
    payload = {
        "format": REQUEST_FORMAT,
        "format_version": 2,
        "operation": operation,
        "launcher_dir": str(root),
        **dict(request),
    }
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
        limit = float(metadata.get("timeout_seconds", 60.0) if timeout is None else timeout)
        try:
            completed = subprocess.run(
                [str(root / LAUNCHER_EXECUTABLE), str(request_path)],
                text=True,
                capture_output=True,
                timeout=limit,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"launcher {operation} exceeded {limit:g} seconds") from exc
    finally:
        request_path.unlink(missing_ok=True)
    if completed.returncode != 0:
        raise RuntimeError(f"launcher {operation} failed ({completed.returncode}): {completed.stderr.strip()}")
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError(f"launcher {operation} did not emit exactly one JSON result") from exc
    if not isinstance(result, dict):
        raise ValueError(f"launcher {operation} result must be a JSON object")
    if result.get("format") != RESULT_FORMAT or result.get("format_version") != 2:
        raise ValueError(f"launcher {operation} returned an unsupported result")
    if result.get("operation") != operation:
        raise ValueError(f"launcher result operation disagrees with request: {operation}")
    if result.get("ok") is not True:
        raise RuntimeError(str(result.get("error", f"launcher {operation} reported failure")))
    if completed.stderr:
        result["diagnostics"] = completed.stderr
    return result


def check_launcher(target: LauncherTarget, *, timeout: float | None = None) -> dict[str, Any]:
    """Run a launcher's environment check operation.

    :param target: The resolved launcher target.
    :param timeout: Optional caller-side operation timeout.
    :return: The launcher check result.
    """

    return run_launcher(target.bundle, "check", {}, timeout=timeout)


def start_managers(
    target: LauncherTarget,
    *,
    workspace_root: str | os.PathLike[str],
    argv: Sequence[str],
    count: int,
    settings: Mapping[str, object],
    timeout: float | None,
) -> dict[str, Any]:
    """Start manager processes through one resolved launcher.

    :param target: The resolved launcher target.
    :param workspace_root: Absolute workspace root for the managers.
    :param argv: Full manager command argument vector.
    :param count: Number of managers to start.
    :param settings: Workspace settings passed to the launcher.
    :param timeout: Optional caller-side operation timeout.
    :return: The launcher start result.
    """

    if count < 1:
        raise ValueError("manager count must be a positive integer")
    if not argv or not all(isinstance(item, str) for item in argv):
        raise ValueError("manager argv must be a nonempty string array")
    root = Path(workspace_root).expanduser().resolve()
    return run_launcher(
        target.bundle,
        "start",
        {"workspace": str(root), "argv": list(argv), "count": count, "settings": dict(settings)},
        timeout=timeout,
    )


def split_capacity(
    capacity: Mapping[str, int],
    count: int,
    explicit: Collection[str],
) -> list[dict[str, int]]:
    """Split implicit capacities across children and preserve explicit ones.

    :param capacity: Resource capacities to distribute.
    :param count: Number of children.
    :param explicit: Resource names whose values are passed to every child.
    :return: One resource mapping for each child.
    :raises ValueError: If the count or capacities are invalid.
    """

    if count < 1:
        raise ValueError("child count must be a positive integer")
    for name, value in capacity.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"capacity for {name!r} must be a non-negative integer")
    explicit_names = set(explicit)
    return [
        {
            name: value if name in explicit_names else value // count + (1 if index < value % count else 0)
            for name, value in capacity.items()
        }
        for index in range(count)
    ]


def host_capacity() -> dict[str, int]:
    """Return the process and half-physical-memory capacity of this host.

    :return: Host capacities in manager resource units.
    """

    capacity = {"procs": os.cpu_count() or 1}
    try:
        capacity["mem"] = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // 2**20 // 2
    except (ValueError, OSError, AttributeError):
        pass
    return capacity


def _explicit_resources(argv: Sequence[str]) -> set[str]:
    """Return resource names already supplied in a manager argument vector."""

    return {argv[index + 1] for index, argument in enumerate(argv[:-1]) if argument == "--worker-resource"}


def launch_processes(
    *,
    workspace_root: str | os.PathLike[str],
    argv: Sequence[str],
    count: int,
    settings: Mapping[str, object],
    capacity: Mapping[str, int] | None = None,
) -> dict[str, object]:
    """Launch detached local manager processes with split host capacity.

    :param workspace_root: Workspace directory used as each child's cwd.
    :param argv: Full manager command argument vector.
    :param count: Number of manager children.
    :param settings: Workspace settings, including an optional environment prelude.
    :param capacity: Capacity to split, or :func:`host_capacity` when omitted.
    :return: Process launcher result document.
    """

    if count < 1:
        raise ValueError("manager count must be a positive integer")
    if not argv or not all(isinstance(item, str) for item in argv):
        raise ValueError("manager argv must be a nonempty string array")
    root = Path(workspace_root).expanduser().resolve()
    effective_capacity = host_capacity() if capacity is None else dict(capacity)
    resources = split_capacity(effective_capacity, count, _explicit_resources(argv))
    environment = {key: value for key, value in os.environ.items() if not key.startswith("SLURM_")}
    prelude = settings.get("environment.prelude")
    prelude_text = prelude.strip() if isinstance(prelude, str) else ""
    pids: list[int] = []
    for child_resources in resources:
        child_argv = list(argv)
        supplied = _explicit_resources(child_argv)
        for name, value in child_resources.items():
            if name not in supplied:
                child_argv += ["--worker-resource", name, str(value)]
        launch_argv: list[str]
        if prelude_text:
            launch_argv = ["bash", "-lc", "set -e\n" + prelude_text + '\nexec "$@"', "bash", *child_argv]
        else:
            launch_argv = child_argv
        child = subprocess.Popen(
            launch_argv,
            cwd=str(root),
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        pids.append(child.pid)
    return {"ok": True, "kind": PROCESS_LAUNCHER, "count": count, "pids": pids}
