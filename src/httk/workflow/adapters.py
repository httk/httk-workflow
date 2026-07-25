"""Versioned JSON computer-adapter bundles."""

import importlib.resources
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .configuration import data_home
from .projects import discover_project, read_project

ADAPTER_OPERATIONS = (
    "configure",
    "install",
    "invoke",
    "push",
    "pull",
    "start-manager",
    "status",
)


@dataclass(frozen=True)
class ComputerTarget:
    """Resolved computer bundle and queue."""

    name: str
    queue: str
    bundle: Path
    project_local: bool


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read adapter JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"adapter JSON must be an object: {path}")
    return value


def validate_adapter_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate static adapter metadata and executable operation files."""

    root = Path(bundle).expanduser().resolve()
    metadata = _read_object(root / "computer.json")
    if metadata.get("format") != "httk-computer-adapter" or metadata.get("format_version") != 1:
        raise ValueError("computer.json must use httk-computer-adapter format version 1")
    if metadata.get("adapter_version") != 1:
        raise ValueError(f"unsupported computer adapter version: {metadata.get('adapter_version')!r}")
    operations = metadata.get("operations")
    if not isinstance(operations, Mapping):
        raise ValueError("computer adapter operations must be an object")
    for operation in ADAPTER_OPERATIONS:
        relative = operations.get(operation)
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or ".." in Path(relative).parts
        ):
            raise ValueError(f"invalid adapter operation path for {operation}")
        executable = root / relative
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise ValueError(f"adapter operation is not executable: {executable}")
    queues = metadata.get("queues", {"default": {}})
    if not isinstance(queues, Mapping) or not queues:
        raise ValueError("computer adapter must configure at least one queue")
    if not all(isinstance(name, str) and isinstance(value, Mapping) for name, value in queues.items()):
        raise ValueError("computer adapter queues must map names to objects")
    timeout = metadata.get("timeout_seconds", 60.0)
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("adapter timeout_seconds must be positive")
    binaries = metadata.get("required_binaries", [])
    if not isinstance(binaries, Sequence) or isinstance(binaries, (str, bytes)):
        raise ValueError("required_binaries must be an array")
    for binary in binaries:
        if not isinstance(binary, str) or not binary:
            raise ValueError("required_binaries entries must be nonempty strings")
        if shutil.which(binary) is None:
            raise ValueError(f"required adapter binary is unavailable: {binary}")
    return metadata


def split_computer(value: str) -> tuple[str, str | None]:
    name, separator, queue = value.partition(":")
    if not name:
        raise ValueError("computer name cannot be empty")
    if separator and not queue:
        raise ValueError("queue name cannot be empty")
    return name, queue if separator else None


def resolve_computer(
    value: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> ComputerTarget:
    """Resolve project-local before global computer definitions."""

    name, explicit_queue = split_computer(value)
    project_root = discover_project(project)
    candidates: list[tuple[Path, bool]] = []
    default_queue: str | None = None
    if project_root is not None:
        candidates.append((project_root / ".httk-project" / "computers" / name, True))
        raw_default = read_project(project_root).get("default_queue")
        default_queue = raw_default if isinstance(raw_default, str) and raw_default else None
    candidates.append((data_home() / "computers" / name, False))
    for bundle, local in candidates:
        if bundle.is_dir():
            metadata = validate_adapter_bundle(bundle)
            queue = explicit_queue or default_queue or "default"
            queues = metadata.get("queues", {})
            assert isinstance(queues, Mapping)
            if queue not in queues:
                raise ValueError(f"computer {name!r} does not configure queue {queue!r}")
            return ComputerTarget(name, queue, bundle, local)
    raise ValueError(f"unknown computer: {name}")


def list_computers(project: str | os.PathLike[str] | None = None) -> list[dict[str, object]]:
    """List definitions with project entries shadowing global entries."""

    rows: dict[str, dict[str, object]] = {}
    global_root = data_home() / "computers"
    if global_root.is_dir():
        for path in sorted(global_root.iterdir()):
            if path.is_dir():
                rows[path.name] = {"name": path.name, "scope": "global", "path": str(path)}
    project_root = discover_project(project)
    if project_root is not None:
        local_root = project_root / ".httk-project" / "computers"
        if local_root.is_dir():
            for path in sorted(local_root.iterdir()):
                if path.is_dir():
                    rows[path.name] = {"name": path.name, "scope": "project", "path": str(path)}
    return [rows[name] for name in sorted(rows)]


def add_computer(
    name: str,
    *,
    template: str,
    global_scope: bool = False,
    project: str | os.PathLike[str] | None = None,
) -> Path:
    """Copy one maintained adapter template into user/project data."""

    if not name or "/" in name or name in {".", ".."}:
        raise ValueError("invalid computer name")
    if template not in {"local", "local-slurm", "ssh-slurm"}:
        raise ValueError(f"unknown maintained computer template: {template}")
    if global_scope:
        destination = data_home() / "computers" / name
    else:
        project_root = discover_project(project)
        if project_root is None:
            raise ValueError("a project is required unless --global is used")
        destination = project_root / ".httk-project" / "computers" / name
    if destination.exists():
        raise FileExistsError(destination)
    source = importlib.resources.files("httk.workflow").joinpath("adapter_templates", template)
    with importlib.resources.as_file(source) as template_path:
        validate_adapter_bundle(template_path)
        shutil.copytree(template_path, destination)
    validate_adapter_bundle(destination)
    return destination


_LEGACY_SETTING = re.compile(r"[A-Z][A-Z0-9_]*")


def _legacy_settings(path: Path) -> dict[str, str]:
    settings: dict[str, str] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        try:
            parts = shlex.split(stripped, comments=True, posix=True)
        except ValueError as exc:
            raise ValueError(f"unsafe legacy adapter configuration at {path}:{number}") from exc
        if len(parts) != 1:
            raise ValueError(f"legacy adapter configuration is not a simple assignment at {path}:{number}")
        name, separator, value = parts[0].partition("=")
        if not separator or _LEGACY_SETTING.fullmatch(name) is None:
            raise ValueError(f"legacy adapter configuration is not a simple assignment at {path}:{number}")
        settings[name] = value
    return settings


def import_v1_computer(
    source: str | os.PathLike[str],
    *,
    name: str | None = None,
    global_scope: bool = False,
    project: str | os.PathLike[str] | None = None,
) -> Path:
    """Map a recognizable legacy executable bundle to the versioned contract.

    Legacy shell programs are never copied or executed. Only their simple
    assignment-only ``config`` files are read, and the result uses a maintained
    v2 adapter implementation.
    """

    legacy = Path(source).expanduser().resolve()
    required = {"command", "install", "push", "pull", "start-taskmgr", "config"}
    if not legacy.is_dir() or not all((legacy / item).is_file() for item in required):
        raise ValueError("legacy computer definition does not contain the recognized executable set")
    base = _legacy_settings(legacy / "config")
    if "REMOTE_HOST" in base:
        template = "ssh-slurm"
    elif any(key.startswith("SLURM_") for key in base):
        template = "local-slurm"
    elif "LOCAL_HTTK_DIR" in base:
        template = "local"
    else:
        raise ValueError("legacy computer definition cannot be mapped to a maintained adapter")

    queue_settings: dict[str, dict[str, str]] = {"default": dict(base)}
    for queue_file in sorted(legacy.glob("config.*")):
        queue = queue_file.name.removeprefix("config.")
        if not queue or "/" in queue or queue in {".", ".."}:
            raise ValueError(f"invalid legacy queue name: {queue!r}")
        queue_settings[queue] = {**base, **_legacy_settings(queue_file)}

    computer_name = legacy.name if name is None else name
    destination = add_computer(
        computer_name,
        template=template,
        global_scope=global_scope,
        project=project,
    )
    metadata = _read_object(destination / "computer.json")
    queues: dict[str, dict[str, object]] = {}
    for queue, settings in queue_settings.items():
        row: dict[str, object] = {"legacy_settings": settings}
        if template in {"local", "local-slurm"} and "LOCAL_HTTK_DIR" in settings:
            root = Path(settings["LOCAL_HTTK_DIR"]).expanduser()
            if not root.is_absolute():
                root = (legacy / root).resolve()
            row["workspace"] = str(root / "Runs" / queue)
        elif template == "ssh-slurm" and "REMOTE_HTTK_DIR" in settings:
            row["workspace"] = f"{settings['REMOTE_HTTK_DIR'].rstrip('/')}/Runs/{queue}"
            row["host"] = settings.get("REMOTE_HOST", "")
            row["username"] = settings.get("USERNAME", "")
        queues[queue] = row
    metadata["queues"] = queues
    metadata["legacy_import"] = {
        "format": "httk-v1-computer-import",
        "source": str(legacy),
        "legacy_executables_copied": False,
    }
    write_path = destination / "computer.json"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".computer.json.", dir=destination)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(metadata, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
        os.replace(temporary, write_path)
    finally:
        temporary.unlink(missing_ok=True)
    validate_adapter_bundle(destination)
    return destination


def run_adapter(
    bundle: str | os.PathLike[str],
    operation: str,
    request: Mapping[str, object],
    *,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Execute one JSON adapter operation without invoking a shell."""

    if operation not in ADAPTER_OPERATIONS:
        raise ValueError(f"unknown adapter operation: {operation}")
    root = Path(bundle).expanduser().resolve()
    metadata = validate_adapter_bundle(root)
    operations = metadata["operations"]
    assert isinstance(operations, Mapping)
    executable = root / str(operations[operation])
    payload = {
        "format": "httk-computer-request",
        "format_version": 1,
        "operation": operation,
        "adapter_dir": str(root),
        **dict(request),
    }
    descriptor, temporary_name = tempfile.mkstemp(prefix="httk-adapter-", suffix=".json")
    request_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
        limit = float(metadata.get("timeout_seconds", 60.0) if timeout is None else timeout)
        try:
            completed = subprocess.run(
                [str(executable), str(request_path)],
                text=True,
                capture_output=True,
                timeout=limit,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"adapter {operation} exceeded {limit:g} seconds") from exc
    finally:
        request_path.unlink(missing_ok=True)
    if completed.returncode != 0:
        detail = completed.stderr.strip()
        raise RuntimeError(f"adapter {operation} failed ({completed.returncode}): {detail}")
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError(f"adapter {operation} did not emit exactly one JSON result") from exc
    if not isinstance(result, dict):
        raise ValueError(f"adapter {operation} result must be a JSON object")
    if result.get("format") != "httk-computer-result" or result.get("format_version") != 1:
        raise ValueError(f"adapter {operation} returned an unsupported result")
    if result.get("operation") != operation:
        raise ValueError(f"adapter result operation disagrees with request: {operation}")
    if result.get("ok") is not True:
        raise RuntimeError(str(result.get("error", f"adapter {operation} reported failure")))
    if completed.stderr:
        result["diagnostics"] = completed.stderr
    return result
