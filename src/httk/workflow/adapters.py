"""Versioned JSON remote-adapter bundles."""

import importlib.resources
import json
import logging
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

from ._util import write_json_atomic
from .configuration import remotes_home
from .projects import PROJECT_DIRECTORY, discover_project, read_project

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "ADAPTER_EXECUTABLE",
    "ADAPTER_FORMAT",
    "ADAPTER_OPERATIONS",
    "CREDENTIALS_FILE",
    "METADATA_FILE",
    "PERSISTABLE_QUEUE_SETTINGS",
    "REQUEST_FORMAT",
    "RESULT_FORMAT",
    "SEED_SETTING_MAP",
    "RemoteTarget",
    "add_remote",
    "import_v1_remote",
    "list_remotes",
    "metadata_path",
    "project_remote_roots",
    "queue_settings",
    "read_credentials",
    "read_metadata",
    "resolve_remote",
    "run_adapter",
    "seed_application_settings",
    "split_remote",
    "split_settings",
    "store_credentials",
    "validate_adapter_bundle",
]

ADAPTER_OPERATIONS = (
    "configure",
    "install",
    "invoke",
    "push",
    "pull",
    "start-manager",
    "status",
)

#: The single dispatcher executable every bundle carries. One program serves
#: every operation; the operation name travels inside the request JSON, so the
#: seven historical per-operation wrappers collapsed into this one file.
ADAPTER_EXECUTABLE = "adapter"

CREDENTIALS_FILE = "credentials.json"

#: The metadata file of one adapter bundle.
METADATA_FILE = "remote.json"

#: The format of the metadata file, and of the request and result documents that
#: cross the adapter boundary. These three names are *protocol* and deliberately
#: keep their historical spelling: a bundle or an adapter implementation written
#: against an earlier release is still a valid one, and renaming the identifier
#: would refuse it for no reason. Only the file *name* changed.
ADAPTER_FORMAT = "httk-computer-adapter"
REQUEST_FORMAT = "httk-computer-request"
RESULT_FORMAT = "httk-computer-result"

#: Queue settings that may be persisted in the signed, shareable
#: ``remote.json``. Everything else is treated as a credential and is written
#: to the manifest-excluded ``credentials.json`` instead.
PERSISTABLE_QUEUE_SETTINGS = frozenset(
    {
        "account",
        "bootstrap",
        "check_connectivity",
        "cpus_per_task",
        "host",
        "httk_command",
        "legacy_settings",
        "nodes",
        "partition",
        "port",
        "reservation",
        "time_limit",
        "username",
        "vasp_command",
        "vasp_pseudo_library",
        "workers",
        "workspace",
    }
)

#: How a remote definition's queue settings seed the application settings of a
#: workspace created against it. A workspace bound to a remote should not make
#: every operator restate the remote's VASP command, so the whitelisted queue
#: settings on the left become the dotted application settings on the right when
#: the workspace is created. The map is deliberately small and explicit: a queue
#: setting only becomes an application setting when it is named here.
SEED_SETTING_MAP: Mapping[str, str] = {
    "vasp_command": "vasp.command",
    "vasp_pseudo_library": "vasp.pseudo_library",
}


def seed_application_settings(bundle: str | os.PathLike[str], queue: str) -> dict[str, object]:
    """Return the application settings a remote queue seeds a workspace with."""

    configured = queue_settings(bundle, queue)
    seeds: dict[str, object] = {}
    for source, target in SEED_SETTING_MAP.items():
        value = configured.get(source)
        # Only a JSON scalar seeds a setting; a container queue setting is never
        # an application setting, which is a flat scalar map by construction.
        if isinstance(value, (str, int, float)):
            seeds[target] = value
    return seeds


@dataclass(frozen=True)
class RemoteTarget:
    """Resolved remote bundle and queue."""

    name: str
    queue: str
    bundle: Path
    project_local: bool


def metadata_path(bundle: str | os.PathLike[str]) -> Path:
    """Return the metadata file of one adapter bundle."""

    return Path(bundle).expanduser() / METADATA_FILE


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read adapter JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"adapter JSON must be an object: {path}")
    return value


def read_metadata(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Read the metadata of one adapter bundle."""

    return _read_object(metadata_path(bundle))


def validate_adapter_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate static adapter metadata and the single dispatcher executable."""

    root = Path(bundle).expanduser().resolve()
    metadata = read_metadata(root)
    if metadata.get("format") != ADAPTER_FORMAT or metadata.get("format_version") != 1:
        raise ValueError(f"{METADATA_FILE} must use {ADAPTER_FORMAT} format version 1")
    if metadata.get("adapter_version") != 1:
        raise ValueError(f"unsupported remote adapter version: {metadata.get('adapter_version')!r}")
    # One executable dispatches every operation; the operation name travels in the
    # request JSON, so there is nothing per-operation to resolve or validate here.
    executable = root / ADAPTER_EXECUTABLE
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError(f"adapter executable is not runnable: {executable}")
    queues = metadata.get("queues", {"default": {}})
    if not isinstance(queues, Mapping) or not queues:
        raise ValueError("remote adapter must configure at least one queue")
    if not all(isinstance(name, str) and isinstance(value, Mapping) for name, value in queues.items()):
        raise ValueError("remote adapter queues must map names to objects")
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


def split_remote(value: str) -> tuple[str, str | None]:
    name, separator, queue = value.partition(":")
    if not name:
        raise ValueError("remote name cannot be empty")
    if separator and not queue:
        raise ValueError("queue name cannot be empty")
    return name, queue if separator else None


def resolve_remote(
    value: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> RemoteTarget:
    """Resolve project-local before global remote definitions."""

    name, explicit_queue = split_remote(value)
    project_root = discover_project(project)
    candidates: list[tuple[Path, bool]] = []
    default_queue: str | None = None
    if project_root is not None:
        candidates.extend((root / name, True) for root in project_remote_roots(project_root))
        raw_default = read_project(project_root).get("default_queue")
        default_queue = raw_default if isinstance(raw_default, str) and raw_default else None
    candidates.append((remotes_home() / name, False))
    for bundle, local in candidates:
        if bundle.is_dir():
            metadata = validate_adapter_bundle(bundle)
            queue = explicit_queue or default_queue or "default"
            queues = metadata.get("queues", {})
            assert isinstance(queues, Mapping)
            if queue not in queues:
                raise ValueError(f"remote {name!r} does not configure queue {queue!r}")
            return RemoteTarget(name, queue, bundle, local)
    raise ValueError(f"unknown remote: {name}")


def project_remote_roots(project_root: Path) -> tuple[Path, ...]:
    """Return where one project keeps its remotes."""

    return (project_root / PROJECT_DIRECTORY / "remotes",)


def list_remotes(project: str | os.PathLike[str] | None = None) -> list[dict[str, object]]:
    """List definitions with project entries shadowing global entries."""

    rows: dict[str, dict[str, object]] = {}
    global_root = remotes_home()
    if global_root.is_dir():
        for path in sorted(global_root.iterdir()):
            if path.is_dir():
                rows[path.name] = {"name": path.name, "scope": "global", "path": str(path)}
    project_root = discover_project(project)
    if project_root is not None:
        for local_root in reversed(project_remote_roots(project_root)):
            if local_root.is_dir():
                for path in sorted(local_root.iterdir()):
                    if path.is_dir():
                        rows[path.name] = {"name": path.name, "scope": "project", "path": str(path)}
    return [rows[name] for name in sorted(rows)]


def add_remote(
    name: str,
    *,
    template: str,
    global_scope: bool = False,
    project: str | os.PathLike[str] | None = None,
) -> Path:
    """Copy one maintained adapter template into user/project data."""

    if not name or "/" in name or name in {".", ".."}:
        raise ValueError("invalid remote name")
    if name == "local":
        # `local` is the built-in remote every workspace registry resolves as
        # "this machine". Defining one would make a binding to `local` ambiguous,
        # so the name is reserved.
        raise ValueError("the remote name 'local' is reserved for the built-in local remote")
    if template not in {"local", "local-slurm", "ssh-slurm"}:
        raise ValueError(f"unknown maintained remote template: {template}")
    if global_scope:
        destination = remotes_home() / name
    else:
        project_root = discover_project(project)
        if project_root is None:
            raise ValueError("a project is required unless --global is used")
        destination = project_remote_roots(project_root)[0] / name
    if destination.exists():
        raise FileExistsError(destination)
    source = importlib.resources.files("httk.workflow").joinpath("adapter_templates", template)
    with importlib.resources.as_file(source) as template_path:
        validate_adapter_bundle(template_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(template_path, destination)
    validate_adapter_bundle(destination)
    return destination


def split_settings(settings: Mapping[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """Partition ``--set`` values into persistable settings and credentials."""

    persistable = {key: value for key, value in settings.items() if key in PERSISTABLE_QUEUE_SETTINGS}
    credentials = {key: value for key, value in settings.items() if key not in PERSISTABLE_QUEUE_SETTINGS}
    return persistable, credentials


def read_credentials(bundle: str | os.PathLike[str], queue: str) -> dict[str, Any]:
    """Read queue-scoped credentials that never enter a project manifest."""

    path = Path(bundle).expanduser().resolve() / CREDENTIALS_FILE
    if not path.is_file():
        return {}
    scoped = _read_object(path).get(queue)
    return dict(scoped) if isinstance(scoped, Mapping) else {}


def store_credentials(
    bundle: str | os.PathLike[str],
    queue: str,
    settings: Mapping[str, str],
) -> Path:
    """Merge *settings* into the queue-scoped, manifest-excluded credentials."""

    root = Path(bundle).expanduser().resolve()
    path = root / CREDENTIALS_FILE
    document = _read_object(path) if path.is_file() else {}
    scoped = document.get(queue)
    merged: dict[str, Any] = dict(scoped) if isinstance(scoped, Mapping) else {}
    merged.update(settings)
    document[queue] = merged
    write_json_atomic(path, document)
    os.chmod(path, 0o600)
    return path


def queue_settings(bundle: str | os.PathLike[str], queue: str) -> dict[str, Any]:
    """Return persisted queue settings with credentials merged back in."""

    root = Path(bundle).expanduser().resolve()
    queues = read_metadata(root).get("queues", {})
    scoped = queues.get(queue) if isinstance(queues, Mapping) else None
    settings: dict[str, Any] = dict(scoped) if isinstance(scoped, Mapping) else {}
    settings.update(read_credentials(root, queue))
    return settings


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


def import_v1_remote(
    source: str | os.PathLike[str],
    *,
    name: str | None = None,
    global_scope: bool = False,
    project: str | os.PathLike[str] | None = None,
) -> Path:
    """Map a recognizable legacy executable bundle to the versioned contract.

    Legacy shell programs are never copied or executed. Only their simple
    assignment-only ``config`` files are read, and the result uses a maintained
    v2 adapter implementation. What is read is an *httk v1* computer definition,
    so the legacy names below are the names that tree really uses.
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

    remote_name = legacy.name if name is None else name
    destination = add_remote(
        remote_name,
        template=template,
        global_scope=global_scope,
        project=project,
    )
    metadata = read_metadata(destination)
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
    # The provenance format keeps its historical name: it records that this
    # bundle was mapped from an httk v1 *computer* definition, which is what
    # that tree calls the thing.
    metadata["legacy_import"] = {
        "format": "httk-v1-computer-import",
        "source": str(legacy),
        "legacy_executables_copied": False,
    }
    write_path = metadata_path(destination)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{write_path.name}.", dir=destination)
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
    executable = root / ADAPTER_EXECUTABLE
    requested_queue = request.get("queue")
    payload = {
        "format": REQUEST_FORMAT,
        "format_version": 1,
        "operation": operation,
        "adapter_dir": str(root),
        # Persisted settings and their manifest-excluded credentials reach the
        # adapter together, so splitting the two storage locations is invisible.
        **({"queue_settings": queue_settings(root, requested_queue)} if isinstance(requested_queue, str) else {}),
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
    if result.get("format") != RESULT_FORMAT or result.get("format_version") != 1:
        raise ValueError(f"adapter {operation} returned an unsupported result")
    if result.get("operation") != operation:
        raise ValueError(f"adapter result operation disagrees with request: {operation}")
    if result.get("ok") is not True:
        raise RuntimeError(str(result.get("error", f"adapter {operation} reported failure")))
    if completed.stderr:
        result["diagnostics"] = completed.stderr
    return result
