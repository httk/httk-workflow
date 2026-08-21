"""Versioned JSON remote-adapter bundles."""

import importlib.resources
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._util import write_json_atomic
from .configuration import remotes_home
from .errors import ResolutionMiss
from .models import CORE_PROFILE
from .projects import PROJECT_DIRECTORY, discover_project

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "ADAPTER_EXECUTABLE",
    "ADAPTER_FORMAT",
    "ADAPTER_OPERATIONS",
    "CREDENTIALS_FILE",
    "METADATA_FILE",
    "PERSISTABLE_REMOTE_SETTINGS",
    "REMOTE_JOB_PUBLISH_REQUESTS_COMMAND",
    "REMOTE_JOB_REQUEST_ENVELOPES_COMMAND",
    "REMOTE_MANAGER_COMMAND",
    "REMOTE_OFFER_COMMAND",
    "REMOTE_RECEIVE_COMMAND",
    "REMOTE_RETIRE_COMMAND",
    "REMOTE_STATUS_COMMAND",
    "REMOTE_WORKSPACE_DELETE_COMMAND",
    "REMOTE_WORKSPACE_FSCK_COMMAND",
    "REMOTE_WORKSPACE_GC_COMMAND",
    "REMOTE_WORKSPACE_INIT_COMMAND",
    "REMOTE_WORKSPACE_LIST_COMMAND",
    "REMOTE_WORKSPACE_MOVE_COMMAND",
    "REMOTE_WORKSPACE_SETTINGS_COMMAND",
    "REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND",
    "REQUEST_FORMAT",
    "RESULT_FORMAT",
    "SEED_SETTING_MAP",
    "RemoteTarget",
    "add_remote",
    "import_v1_remote",
    "list_remotes",
    "metadata_path",
    "probe_remote_workspace",
    "project_remote_roots",
    "read_credentials",
    "read_metadata",
    "remote_settings",
    "resolve_remote",
    "run_adapter",
    "seed_application_settings",
    "split_settings",
    "store_credentials",
    "submit_remote_managers",
    "valid_remote_name",
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

#: Remote settings that may be persisted in the signed, shareable
#: ``remote.json``. Everything else is treated as a credential and is written
#: to the manifest-excluded ``credentials.json`` instead.
PERSISTABLE_REMOTE_SETTINGS = frozenset(
    {
        "check_connectivity",
        "host",
        "httk_command",
        "legacy_settings",
        "port",
        "username",
        "vasp_command",
        "vasp_pseudo_library",
    }
)

_RETIRED_REMOTE_SETTINGS = frozenset(
    {
        "account",
        "bootstrap",
        "constraint",
        "cpus_per_task",
        "nodes",
        "partition",
        "reservation",
        "time_limit",
        "workers",
        "workspace_root",
    }
)

REMOTE_WORKSPACE_INIT_COMMAND = ("httk", "workflow", "workspace", "init")
REMOTE_WORKSPACE_DELETE_COMMAND = ("httk", "workflow", "workspace", "delete")
REMOTE_WORKSPACE_FSCK_COMMAND = ("httk", "workflow", "workspace", "fsck")
REMOTE_WORKSPACE_GC_COMMAND = ("httk", "workflow", "workspace", "gc")
REMOTE_WORKSPACE_LIST_COMMAND = ("httk", "workflow", "workspace", "list")
REMOTE_WORKSPACE_MOVE_COMMAND = ("httk", "workflow", "workspace", "move")
REMOTE_RECEIVE_COMMAND = ("httk", "workflow", "transfer", "receive")
REMOTE_OFFER_COMMAND = ("httk", "workflow", "transfer", "offer")
REMOTE_RETIRE_COMMAND = ("httk", "workflow", "transfer", "retire")
REMOTE_STATUS_COMMAND = ("httk", "workflow", "workspace", "status")
REMOTE_WORKSPACE_SETTINGS_COMMAND = ("httk", "workflow", "workspace", "settings")
REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND = ("httk", "workflow", "workspace", "workflow-prelude")
REMOTE_MANAGER_COMMAND = ("httk", "workflow", "manager", "run")
REMOTE_JOB_REQUEST_ENVELOPES_COMMAND = ("httk", "workflow", "job", "request-envelopes")
REMOTE_JOB_PUBLISH_REQUESTS_COMMAND = ("httk", "workflow", "job", "publish-requests")


def submit_remote_managers(
    target: "RemoteTarget",
    name: str,
    root: str,
    *,
    count: int,
    argv_tail: Sequence[str],
    timeout: float | None,
    adapter: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Submit managers for a far-side workspace name using its probed root.

    :param target: The resolved remote adapter bundle.
    :param name: The remote workspace name.
    :param root: The remote workspace root.
    :param count: The number of managers to submit.
    :param argv_tail: Additional manager arguments.
    :param timeout: The adapter operation timeout.
    :param adapter: The adapter callable, or :func:`run_adapter` when omitted.
    :return: The adapter result document.
    """

    if count < 1:
        raise ValueError("manager count must be a positive integer")
    runner = run_adapter if adapter is None else adapter
    return runner(
        target.bundle,
        "start-manager",
        {
            "argv": [*REMOTE_MANAGER_COMMAND, name, *argv_tail],
            "workspace": root,
            "count": count,
        },
        timeout=timeout,
    )


def probe_remote_workspace(
    target: "RemoteTarget",
    name: str,
    *,
    timeout: float | None,
    noun: str = "workspace",
    adapter: Callable[..., dict[str, Any]] | None = None,
) -> tuple[str, str]:
    """Return the UUID and current root reported for a remote workspace name.

    :param target: The resolved remote adapter bundle.
    :param name: The remote workspace name.
    :param timeout: The adapter operation timeout.
    :param noun: The noun used in diagnostic messages.
    :param adapter: The adapter callable, or :func:`run_adapter` when omitted.
    :return: The remote workspace identifier and root.
    :raises RuntimeError: If the remote status operation fails.
    :raises ValueError: If the remote returns an incompatible status document.
    """

    runner = run_adapter if adapter is None else adapter
    status = runner(
        target.bundle,
        "status",
        {"argv": [*REMOTE_STATUS_COMMAND, name, "--json"]},
        timeout=timeout,
    )
    if status.get("returncode") != 0:
        raise RuntimeError(f"{noun} workspace compatibility check failed: {status.get('stderr', '')}")
    try:
        document = json.loads(str(status.get("stdout", "")))
        if (
            document.get("format") != "httk-workflow-status"
            or document.get("format_version") != 2
            or document.get("core_profile") != CORE_PROFILE
        ):
            raise ValueError
        workspace_id = document["workspace_id"]
        root = document["root"]
        if not isinstance(workspace_id, str) or not isinstance(root, str) or not root:
            raise ValueError
        uuid.UUID(workspace_id)
    except (AttributeError, json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        raise ValueError(f"{noun} did not return a compatible workflow workspace status") from exc
    return workspace_id, root


#: How a remote definition's settings seed the application settings of a
#: workspace created against it. A workspace bound to a remote should not make
#: every operator restate the remote's VASP command, so the whitelisted remote
#: settings on the left become the dotted application settings on the right when
#: the workspace is created. The map is deliberately small and explicit: a
#: remote setting only becomes an application setting when it is named here.
SEED_SETTING_MAP: Mapping[str, str] = {
    "vasp_command": "vasp.command",
    "vasp_pseudo_library": "vasp.pseudo_library",
}


def seed_application_settings(bundle: str | os.PathLike[str]) -> dict[str, object]:
    """Return the application settings a remote seeds a workspace with.

    :param bundle: The adapter bundle path.
    :return: The whitelisted application settings derived from the bundle.
    """

    configured = remote_settings(bundle)
    seeds: dict[str, object] = {}
    for source, target in SEED_SETTING_MAP.items():
        value = configured.get(source)
        # Only a JSON scalar seeds a setting; a container remote setting is never
        # an application setting, which is a flat scalar map by construction.
        if isinstance(value, (str, int, float)):
            seeds[target] = value
    return seeds


@dataclass(frozen=True)
class RemoteTarget:
    """Resolved remote bundle.

    :param name: The remote name.
    :param bundle: The resolved adapter bundle path.
    :param project_local: Whether the bundle came from project data.
    """

    name: str
    bundle: Path
    project_local: bool


def metadata_path(bundle: str | os.PathLike[str]) -> Path:
    """Return the metadata file of one adapter bundle.

    :param bundle: The adapter bundle path.
    :return: The bundle metadata path.
    """

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
    """Read the metadata of one adapter bundle.

    :param bundle: The adapter bundle path.
    :return: The decoded metadata document.
    :raises ValueError: If the metadata cannot be read as an object.
    """

    return _read_object(metadata_path(bundle))


def validate_adapter_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate static adapter metadata and the single dispatcher executable.

    :param bundle: The adapter bundle path.
    :return: The validated metadata document.
    :raises ValueError: If metadata, the dispatcher, or a required binary is invalid or unavailable.
    """

    root = Path(bundle).expanduser().resolve()
    metadata = read_metadata(root)
    if metadata.get("format") != ADAPTER_FORMAT or metadata.get("format_version") != 2:
        raise ValueError(f"{METADATA_FILE} must use {ADAPTER_FORMAT} format version 2")
    if metadata.get("adapter_version") != 2:
        raise ValueError(f"unsupported remote adapter version: {metadata.get('adapter_version')!r}")
    # One executable dispatches every operation; the operation name travels in the
    # request JSON, so there is nothing per-operation to resolve or validate here.
    executable = root / ADAPTER_EXECUTABLE
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError(f"adapter executable is not runnable: {executable}")
    settings = metadata.get("settings", {})
    if not isinstance(settings, Mapping):
        raise ValueError("remote adapter settings must be an object")
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


def valid_remote_name(name: str) -> str:
    """Validate and return one remote name.

    :param name: The remote name to validate.
    :return: The validated remote name.
    :raises httk.workflow.errors.ResolutionMiss: If the name is not valid for remote resolution.
    """
    if not name or "/" in name or ":" in name or name in {".", ".."}:
        raise ResolutionMiss(f"invalid remote name: {name!r}; ':' is reserved for workspace bindings")
    return name


def resolve_remote(
    value: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> RemoteTarget:
    """Resolve project-local before global remote definitions.

    :param value: The remote name to resolve.
    :param project: The project path, or the discovered project when omitted.
    :return: The resolved remote target.
    :raises httk.workflow.errors.ResolutionMiss: If the name is invalid or no bundle is found.
    """

    name = valid_remote_name(value)
    project_root = discover_project(project)
    candidates: list[tuple[Path, bool]] = []
    if project_root is not None:
        candidates.extend((root / name, True) for root in project_remote_roots(project_root))
    candidates.append((remotes_home() / name, False))
    for bundle, local in candidates:
        if bundle.is_dir():
            validate_adapter_bundle(bundle)
            return RemoteTarget(name, bundle, local)
    raise ResolutionMiss(f"unknown remote: {name}")


def project_remote_roots(project_root: Path) -> tuple[Path, ...]:
    """Return where one project keeps its remotes.

    :param project_root: The project root.
    :return: The project remote roots in precedence order.
    """

    return (project_root / PROJECT_DIRECTORY / "remotes",)


def list_remotes(project: str | os.PathLike[str] | None = None) -> list[dict[str, object]]:
    """List definitions with project entries shadowing global entries.

    :param project: The project path, or the discovered project when omitted.
    :return: Remote definitions with scope and bundle path.
    """

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
    """Copy one maintained adapter template into user/project data.

    :param name: The remote name to create.
    :param template: The maintained adapter template name.
    :param global_scope: Whether to create the remote in global data.
    :param project: The project path for a project-local remote.
    :return: The newly created adapter bundle path.
    :raises ValueError: If the name, template, or scope is invalid.
    :raises FileExistsError: If the destination already exists.
    """

    valid_remote_name(name)
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
    """Partition ``--set`` values into persistable settings and credentials.

    :param settings: The settings supplied by the caller.
    :return: Persistable settings followed by manifest-excluded credentials.
    :raises ValueError: If a setting name is retired or unsupported.
    """

    retired = sorted(set(settings) & _RETIRED_REMOTE_SETTINGS)
    if retired:
        names = ", ".join(retired)
        raise ValueError(f"unknown remote setting {names!r}; set scheduler values in the workspace settings")
    persistable = {key: value for key, value in settings.items() if key in PERSISTABLE_REMOTE_SETTINGS}
    credentials = {key: value for key, value in settings.items() if key not in PERSISTABLE_REMOTE_SETTINGS}
    return persistable, credentials


def read_credentials(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Read remote credentials that never enter a project manifest.

    :param bundle: The adapter bundle path.
    :return: The stored credential document, or an empty mapping when absent.
    :raises ValueError: If the credentials file is not a JSON object.
    """

    path = Path(bundle).expanduser().resolve() / CREDENTIALS_FILE
    if not path.is_file():
        return {}
    return _read_object(path)


def store_credentials(
    bundle: str | os.PathLike[str],
    settings: Mapping[str, str],
) -> Path:
    """Merge *settings* into the manifest-excluded remote credentials.

    :param bundle: The adapter bundle path.
    :param settings: Credential members to merge.
    :return: The credentials file path.
    :raises ValueError: If the existing credentials file is invalid.
    """

    root = Path(bundle).expanduser().resolve()
    path = root / CREDENTIALS_FILE
    document = _read_object(path) if path.is_file() else {}
    document.update(settings)
    write_json_atomic(path, document)
    os.chmod(path, 0o600)
    return path


def remote_settings(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    """Return persisted remote settings with credentials merged back in.

    :param bundle: The adapter bundle path.
    :return: Persisted settings and manifest-excluded credentials.
    :raises ValueError: If adapter metadata or credentials are invalid.
    """

    root = Path(bundle).expanduser().resolve()
    configured = read_metadata(root).get("settings", {})
    settings: dict[str, Any] = dict(configured) if isinstance(configured, Mapping) else {}
    settings.update(read_credentials(root))
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

    :param source: The legacy executable bundle path.
    :param name: The imported remote name, or the source directory name when omitted.
    :param global_scope: Whether to create the imported remote in global data.
    :param project: The project path for a project-local import.
    :return: The created versioned adapter bundle path.
    :raises ValueError: If the legacy bundle cannot be recognized or imported.
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

    legacy_profiles: dict[str, dict[str, str]] = {}
    for queue_file in sorted(legacy.glob("config.*")):
        queue = queue_file.name.removeprefix("config.")
        if not queue or "/" in queue or queue in {".", ".."}:
            raise ValueError(f"invalid legacy queue name: {queue!r}")
        legacy_profiles[queue] = {**base, **_legacy_settings(queue_file)}

    selected = next(iter(legacy_profiles.values()), base)
    if len(legacy_profiles) > 1:
        skipped = ", ".join(sorted(legacy_profiles)[1:])
        print(
            f"warning: imported the first v1 submission profile; skipped {skipped}. "
            "Per-queue submission profiles are now per-workspace settings.",
            file=sys.stderr,
        )

    remote_name = legacy.name if name is None else name
    destination = add_remote(
        remote_name,
        template=template,
        global_scope=global_scope,
        project=project,
    )
    metadata = read_metadata(destination)
    legacy_settings = dict(selected)
    settings: dict[str, object] = {"legacy_settings": legacy_settings}
    if template in {"local", "local-slurm"} and "LOCAL_HTTK_DIR" in selected:
        root = Path(selected["LOCAL_HTTK_DIR"]).expanduser()
        if not root.is_absolute():
            root = (legacy / root).resolve()
        legacy_settings["workspace_root"] = str(root / "Runs")
    elif template == "ssh-slurm" and "REMOTE_HTTK_DIR" in selected:
        legacy_settings["workspace_root"] = f"{selected['REMOTE_HTTK_DIR'].rstrip('/')}/Runs"
        settings["host"] = selected.get("REMOTE_HOST", "")
        settings["username"] = selected.get("USERNAME", "")
    metadata["settings"] = settings
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
    """Execute one JSON adapter operation without invoking a shell.

    :param bundle: The adapter bundle path.
    :param operation: The adapter operation name.
    :param request: The operation request members.
    :param timeout: The operation timeout, or the bundle default when omitted.
    :return: The adapter result document.
    :raises TimeoutError: If the adapter exceeds its timeout.
    :raises ValueError: If the request or result violates the adapter protocol.
    :raises RuntimeError: If the adapter refuses or fails the operation.
    """

    if operation not in ADAPTER_OPERATIONS:
        raise ValueError(f"unknown adapter operation: {operation}")
    root = Path(bundle).expanduser().resolve()
    metadata = validate_adapter_bundle(root)
    executable = root / ADAPTER_EXECUTABLE
    payload = {
        "format": REQUEST_FORMAT,
        "format_version": 2,
        "operation": operation,
        "adapter_dir": str(root),
        # Persisted settings and their manifest-excluded credentials reach the
        # adapter together, so splitting the two storage locations is invisible.
        **dict(request),
        "remote_settings": remote_settings(root),
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
    if result.get("format") != RESULT_FORMAT or result.get("format_version") != 2:
        raise ValueError(f"adapter {operation} returned an unsupported result")
    if result.get("operation") != operation:
        raise ValueError(f"adapter result operation disagrees with request: {operation}")
    if result.get("ok") is not True:
        raise RuntimeError(str(result.get("error", f"adapter {operation} reported failure")))
    if completed.stderr:
        result["diagnostics"] = completed.stderr
    return result
