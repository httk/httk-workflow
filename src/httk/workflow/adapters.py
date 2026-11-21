"""Versioned JSON remote-adapter bundles."""

import contextlib
import importlib.resources
import io
import json
import logging
import os
import queue
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import deque
from collections.abc import Buffer, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, BinaryIO, Self, cast

from . import _frames
from ._frames import (
    DATA,
    DATA_LIMIT,
    END,
    JSON_LIMIT,
    PROGRESS,
    REQUEST,
    RESPONSE,
    Frame,
    FrameReader,
    StreamReader,
    encode_header,
    encode_json,
)
from ._util import write_json_atomic
from .adapter_runtime import _PRELUDE_RENAMED
from .configuration import connection_reuse_default, remotes_home
from .errors import ResolutionMiss, StreamAborted, TransferProtocolError
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
    "REMOTE_JOB_ADOPT_COMMAND",
    "REMOTE_JOB_DELETE_COMMAND",
    "REMOTE_JOB_EJECT_COMMAND",
    "REMOTE_JOB_LIST_COMMAND",
    "REMOTE_JOB_LOG_COMMAND",
    "REMOTE_JOB_MOVE_COMMAND",
    "REMOTE_JOB_PUBLISH_REQUESTS_COMMAND",
    "REMOTE_JOB_REQUEST_ENVELOPES_COMMAND",
    "REMOTE_JOB_SHOW_COMMAND",
    "REMOTE_JOB_WHY_COMMAND",
    "REMOTE_MANAGER_COMMAND",
    "REMOTE_SESSION_COMMAND",
    "REMOTE_STATUS_COMMAND",
    "REMOTE_TRANSFER_SESSION_COMMAND",
    "REMOTE_WORKSPACE_DELETE_COMMAND",
    "REMOTE_WORKSPACE_FSCK_COMMAND",
    "REMOTE_WORKSPACE_GC_COMMAND",
    "REMOTE_WORKSPACE_INIT_COMMAND",
    "REMOTE_WORKSPACE_LIST_COMMAND",
    "REMOTE_WORKSPACE_MOVE_COMMAND",
    "REMOTE_WORKSPACE_SET_COMMAND",
    "REMOTE_WORKSPACE_SHOW_COMMAND",
    "REMOTE_WORKSPACE_UNSET_COMMAND",
    "REQUEST_FORMAT",
    "RESULT_FORMAT",
    "SEED_SETTING_MAP",
    "SESSION_FORMAT",
    "SESSION_FORMAT_VERSION",
    "TRANSFER_PROTOCOL",
    "TRANSFER_SESSION_FORMAT",
    "AdapterSession",
    "RemoteTarget",
    "RequestAborted",
    "SessionFailed",
    "SessionUnsupported",
    "TransferSession",
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
    "valid_remote_name",
    "validate_adapter_bundle",
]

ADAPTER_OPERATIONS = (
    "configure",
    "daemon",
    "install",
    "invoke",
    "push",
    "pull",
    "session",
    "status",
)

#: The single dispatcher executable every bundle carries. One program serves
#: every operation; the operation name travels inside the request JSON, so the
#: six maintained per-operation wrappers collapsed into this one file.
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
        "check_mount",
        "cli_prelude",
        "connection_reuse",
        "daemon_enrollment_id",
        "daemon_public_key",
        "daemon_workspace_id",
        "exchange",
        "exec_command",
        "host",
        "httk_command",
        "interactive_auth",
        "legacy_settings",
        "mount_root",
        "port",
        "remote_root",
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

#: Removed remote settings, refused as unknown: transfers always use a transfer session (plan
#: WORKFLOW_TRANSFER_SESSION §7.12), so ``transfer_session`` has no value left to choose.
_REMOVED_REMOTE_SETTINGS = frozenset({"transfer_session"})

#: Renamed remote settings and the refusal naming the fix; never read under the old key.
_RENAMED_REMOTE_SETTINGS = {"prelude": _PRELUDE_RENAMED}

REMOTE_WORKSPACE_INIT_COMMAND = ("httk", "workspace", "init")
REMOTE_WORKSPACE_DELETE_COMMAND = ("httk", "workspace", "delete")
REMOTE_WORKSPACE_FSCK_COMMAND = ("httk", "workspace", "fsck")
REMOTE_WORKSPACE_GC_COMMAND = ("httk", "workspace", "gc")
REMOTE_WORKSPACE_LIST_COMMAND = ("httk", "workspace", "list")
REMOTE_WORKSPACE_MOVE_COMMAND = ("httk", "workspace", "move")
REMOTE_JOB_EJECT_COMMAND = ("httk", "job", "eject")
REMOTE_JOB_ADOPT_COMMAND = ("httk", "job", "adopt")
REMOTE_JOB_MOVE_COMMAND = ("httk", "job", "move")
REMOTE_STATUS_COMMAND = ("httk", "workspace", "status")
REMOTE_WORKSPACE_SHOW_COMMAND = ("httk", "workspace", "show")
REMOTE_WORKSPACE_SET_COMMAND = ("httk", "workspace", "set")
REMOTE_WORKSPACE_UNSET_COMMAND = ("httk", "workspace", "unset")
REMOTE_MANAGER_COMMAND = ("httk", "workflow", "manager", "run")
REMOTE_JOB_REQUEST_ENVELOPES_COMMAND = ("httk", "job", "request-envelopes")
REMOTE_JOB_PUBLISH_REQUESTS_COMMAND = ("httk", "job", "publish-requests")
REMOTE_JOB_LIST_COMMAND = ("httk", "job", "list")
REMOTE_JOB_DELETE_COMMAND = ("httk", "job", "delete")
REMOTE_JOB_SHOW_COMMAND = ("httk", "job", "show")
REMOTE_JOB_LOG_COMMAND = ("httk", "job", "log")
REMOTE_JOB_WHY_COMMAND = ("httk", "job", "why")
REMOTE_SESSION_COMMAND = ("httk", "workflow", "session")
#: The ``format`` the session's first (hello) line names.
SESSION_FORMAT = "httk-workflow-session"
#: The ``format_version`` the session's hello line names.
SESSION_FORMAT_VERSION = 1
REMOTE_TRANSFER_SESSION_COMMAND = ("httk", "workflow", "transfer-session")
#: The ``format`` the transfer session's hello line names.
TRANSFER_SESSION_FORMAT = "httk-workflow-transfer-session"
#: The transfer protocol: the ``format_version`` the transfer session's hello line names, and the ``protocol`` the
#: client's first request carries. Both ends must speak exactly this one; there is no range and no optional op.
#: Version 3 (plan WORKFLOW_TRANSFER_SESSION §7.11, T6c-lite) added a push's deferred commit (``stage`` with
#: ``defer_commit``, committed by its ``adopt_staged``) and the retry slot of a late ``sync_failed`` (``retry_of``).
TRANSFER_PROTOCOL = 3


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
        {"argv": [*REMOTE_STATUS_COMMAND, "--json", name]},
        timeout=timeout,
    )
    if status.get("returncode") != 0:
        raise RuntimeError(f"{noun} workspace compatibility check failed: {status.get('stderr', '')}")
    try:
        documents = json.loads(str(status.get("stdout", "")))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{noun} did not return a compatible workspace status") from exc
    try:
        if not isinstance(documents, list) or len(documents) != 1:
            raise ValueError
        document = documents[0]
        if document.get("format") != "httk-workflow-status":
            raise ValueError
        workspace_id = document["workspace_id"]
        root = document["root"]
        if not isinstance(workspace_id, str) or not isinstance(root, str) or not root:
            raise ValueError
        uuid.UUID(workspace_id)
    except (AttributeError, json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        raise ValueError(f"{noun} did not return a compatible workspace status") from exc
    if document.get("format_version") != 3 or document.get("core_profile") != CORE_PROFILE:
        raise ValueError(
            f"{noun} runs an incompatible httk-workflow (status format {document.get('format_version')!r}, "
            f"profile {document.get('core_profile')!r}); this client reads only {CORE_PROFILE}. "
            "Upgrade httk-workflow on the remote and recreate its workspace; there is no migration."
        )
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
    if template not in {"local", "ssh", "mount", "mount-daemon"}:
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
    """Partition ``remote set`` values into persistable settings and credentials.

    :param settings: The settings supplied by the caller.
    :return: Persistable settings followed by manifest-excluded credentials.
    :raises ValueError: If a setting name is retired, renamed or unsupported.
    """

    renamed = sorted(set(settings) & _RENAMED_REMOTE_SETTINGS.keys())
    if renamed:
        raise ValueError(_RENAMED_REMOTE_SETTINGS[renamed[0]])
    retired = sorted(set(settings) & _RETIRED_REMOTE_SETTINGS)
    if retired:
        names = ", ".join(retired)
        raise ValueError(f"unknown remote setting {names!r}; set scheduler values in the workspace settings")
    removed = sorted(set(settings) & _REMOVED_REMOTE_SETTINGS)
    if removed:
        raise ValueError(f"unknown remote setting {', '.join(removed)!r}")
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
    *,
    unset: Sequence[str] = (),
) -> Path:
    """Merge *settings* into the manifest-excluded remote credentials.

    :param bundle: The adapter bundle path.
    :param settings: Credential members to merge.
    :param unset: Credential members to remove before applying ``settings``.
    :return: The credentials file path.
    :raises ValueError: If the existing credentials file is invalid.
    """

    root = Path(bundle).expanduser().resolve()
    path = root / CREDENTIALS_FILE
    document = _read_object(path) if path.is_file() else {}
    for key in unset:
        document.pop(key, None)
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
        template = "ssh"
    else:
        template = "local"

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
    if template == "local" and "LOCAL_HTTK_DIR" in selected:
        root = Path(selected["LOCAL_HTTK_DIR"]).expanduser()
        if not root.is_absolute():
            root = (legacy / root).resolve()
        legacy_settings["workspace_root"] = str(root / "Runs")
    elif template == "ssh" and "REMOTE_HTTK_DIR" in selected:
        legacy_settings["workspace_root"] = f"{selected['REMOTE_HTTK_DIR'].rstrip('/')}/Runs"
        settings["host"] = selected.get("REMOTE_HOST", "")
        settings["username"] = selected.get("USERNAME", "")
    if any(key.startswith("SLURM_") for key in selected):
        print(
            "notice: scheduler settings belong to the workspace: `httk launcher add NAME --kind slurm` "
            "and `httk workspace set manager.launch=NAME`",
            file=sys.stderr,
        )
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


def _request_file(
    root: Path, operation: str, request: Mapping[str, object], unset_settings: Sequence[str] = ()
) -> Path:
    """Write one adapter request document to a private temporary file.

    The file carries credentials: the caller deletes it once the adapter has read it.

    :param root: The resolved adapter bundle path.
    :param operation: The adapter operation name.
    :param request: The operation request members.
    :param unset_settings: Stored settings to omit.
    :return: The request file path.
    """

    settings = {key: value for key, value in remote_settings(root).items() if key not in unset_settings}
    pending = request.get("settings")
    if "connection_reuse" not in settings and not (isinstance(pending, Mapping) and "connection_reuse" in pending):
        # The per-user default reaches the adapter as an ordinary setting, so adapters never read user config.
        default = connection_reuse_default()
        if default is not None:
            settings["connection_reuse"] = default
    payload = {
        "format": REQUEST_FORMAT,
        "format_version": 2,
        "operation": operation,
        "adapter_dir": str(root),
        # Persisted settings and their manifest-excluded credentials reach the
        # adapter together, so splitting the two storage locations is invisible.
        **dict(request),
        "remote_settings": settings,
    }
    descriptor, temporary_name = tempfile.mkstemp(prefix="httk-adapter-", suffix=".json")
    request_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
    except BaseException:
        request_path.unlink(missing_ok=True)
        raise
    return request_path


def run_adapter(
    bundle: str | os.PathLike[str],
    operation: str,
    request: Mapping[str, object],
    *,
    timeout: float | None = None,
    unset_settings: Sequence[str] = (),
    interactive: bool = True,
) -> dict[str, Any]:
    """Execute one JSON adapter operation without invoking a shell.

    :param bundle: The adapter bundle path.
    :param operation: The adapter operation name.
    :param request: The operation request members.
    :param timeout: The operation timeout, or the bundle default when omitted.
    :param unset_settings: Stored settings to omit when validating a configuration change.
    :param interactive: Whether the adapter may see this process's terminal (and
        so let ssh prompt); False gives it no stdin and no controlling terminal,
        e.g. under a curses display.
    :return: The adapter result document.
    :raises TimeoutError: If the adapter exceeds its timeout.
    :raises ValueError: If the request or result violates the adapter protocol.
    :raises RuntimeError: If the adapter refuses or fails the operation.
    """

    if operation not in ADAPTER_OPERATIONS:
        raise ValueError(f"unknown adapter operation: {operation}")
    if operation == "session":
        raise ValueError("the session operation keeps its pipes open: use AdapterSession")
    root = Path(bundle).expanduser().resolve()
    metadata = validate_adapter_bundle(root)
    executable = root / ADAPTER_EXECUTABLE
    request_path = _request_file(root, operation, request, unset_settings)
    try:
        limit = float(metadata.get("timeout_seconds", 60.0) if timeout is None else timeout)
        try:
            # stdin is inherited deliberately so an adapter can detect an attached
            # terminal; interactive=False withholds it and the controlling
            # terminal (a new session), so nothing can prompt.
            completed = subprocess.run(
                [str(executable), str(request_path)],
                text=True,
                capture_output=True,
                stdin=None if interactive else subprocess.DEVNULL,
                start_new_session=not interactive,
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


class SessionUnsupported(RuntimeError):
    """The adapter refused the ``session`` operation, or its far side cannot serve a session.

    The monitor's :class:`AdapterSession` callers fall back to one ``invoke`` per read; a transfer stops before
    anything is held or changed (:class:`TransferSession`).

    :param message: What failed.
    :param kind: ``adapter`` when the adapter cannot open a session (it refuses the operation, answers with its own
        result document, or its channel does not carry binary data intact), ``version`` when the far side's ``httk``
        speaks another protocol (or has no such command), ``missing`` when the far side's shell found no ``httk`` or
        a command in its ``cli_prelude`` was not found (exit status 127), ``other`` for any other failure before the session was open (the far side crashed or refused
        for a reason of its own, ...).
    """

    def __init__(self, message: str, *, kind: str = "adapter") -> None:
        super().__init__(message)
        self.kind = kind


#: How much of a session's standard error is kept for error messages.
_SESSION_STDERR_TAIL = 16384
#: How many lines a session skips before its hello (a login banner, a printing ``cli_prelude``).
_SESSION_PREAMBLE_LINES = 1000


def _queue_lines(stream: IO[str], lines: "queue.Queue[str | None]") -> None:
    """Move *stream*'s lines into *lines*, then ``None`` at its end."""

    try:
        with stream:
            for line in stream:
                lines.put(line)
    finally:
        lines.put(None)


def _keep_tail(stream: IO[str], tail: deque[str]) -> None:
    """Drain *stream* into the bounded *tail*, so the far side never blocks on a full pipe."""

    with stream:
        for line in stream:
            tail.extend(line)


class AdapterSession:
    """A long-lived far-side ``httk workflow session`` reached through the adapter ``session`` operation.

    Each :meth:`run` is one request/response round trip over the session's pipes, so a caller that polls
    logs in once instead of once per read. A failed round trip closes the session; the next :meth:`run`
    opens a new one. Use it as a context manager, or call :meth:`close`.

    :param bundle: The adapter bundle path.
    :param timeout: The bound in seconds on opening the session and on each round trip, or the bundle's
        ``timeout_seconds`` when omitted.
    """

    def __init__(self, bundle: str | os.PathLike[str], *, timeout: float | None = None) -> None:
        self._root = Path(bundle).expanduser().resolve()
        self._timeout = timeout
        self._limit = 60.0
        # ponytail: one lock serializes all round trips; open one session per worker if reads must overlap
        self._lock = threading.RLock()
        self._process: subprocess.Popen[str] | None = None
        self._group = False
        self._lines: queue.Queue[str | None] = queue.Queue()
        self._errors: deque[str] = deque(maxlen=_SESSION_STDERR_TAIL)
        self._error_reader: threading.Thread | None = None
        self._next_id = 0
        self._closing = False

    def __enter__(self) -> Self:
        """Return this session; it opens on first use."""

        return self

    def __exit__(self, *_exc_info: object) -> None:
        """Close the session."""

        self.close()

    def open(self, *, interactive: bool = False) -> None:
        """Start the session unless it is open, and wait for the far side's hello line.

        :param interactive: Whether the adapter keeps this process's controlling terminal (and so lets ssh
            prompt); False starts it in a new session without one.
        :raises SessionUnsupported: If the adapter refuses the operation or the far side cannot serve a
            session; the monitor then falls back to ``invoke``.
        :raises TimeoutError: If no hello arrives within the timeout.
        :raises RuntimeError: If ssh itself fails (status 255), such as a refused login.
        """

        with self._lock:
            if self._process is not None:
                return
            self._closing = False
            metadata = validate_adapter_bundle(self._root)
            self._limit = float(metadata.get("timeout_seconds", 60.0) if self._timeout is None else self._timeout)
            request_path = _request_file(self._root, "session", {"argv": list(REMOTE_SESSION_COMMAND)})
            try:
                process = subprocess.Popen(
                    [str(self._root / ADAPTER_EXECUTABLE), str(request_path)],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    start_new_session=not interactive,
                )
                self._process, self._group = process, not interactive
                self._lines, self._errors = queue.Queue(), deque(maxlen=_SESSION_STDERR_TAIL)
                threading.Thread(target=_queue_lines, args=(process.stdout, self._lines), daemon=True).start()
                self._error_reader = threading.Thread(
                    target=_keep_tail, args=(process.stderr, self._errors), daemon=True
                )
                self._error_reader.start()
                deadline = time.monotonic() + self._limit
                for _ in range(_SESSION_PREAMBLE_LINES):
                    try:
                        line = self._lines.get(timeout=max(0.0, deadline - time.monotonic()))
                    except queue.Empty:
                        raise TimeoutError(f"adapter session did not start within {self._limit:g} seconds") from None
                    if line is None:
                        try:
                            status = process.wait(timeout=5.0)
                        except subprocess.TimeoutExpired:
                            status = None
                        message = f"adapter session failed ({status}): {self._discard()}"
                        if self._closing:  # close() killed it: not a reason to fall back and log in again
                            raise RuntimeError("adapter session was closed")
                        # 255 is ssh's own failure (connection or login refused): falling back would log in again.
                        raise RuntimeError(message) if status == 255 else SessionUnsupported(message)
                    try:
                        document = json.loads(line)
                    except ValueError:
                        continue
                    if not isinstance(document, dict):
                        continue
                    if document.get("format") == RESULT_FORMAT and document.get("ok") is False:
                        raise SessionUnsupported(str(document.get("error", "the adapter refused the session")))
                    if document.get("format") == SESSION_FORMAT:
                        if document.get("format_version") != SESSION_FORMAT_VERSION:
                            version = document.get("format_version")
                            raise SessionUnsupported(f"unsupported remote session version: {version!r}")
                        return
                raise SessionUnsupported(f"no remote session hello within {_SESSION_PREAMBLE_LINES} lines")
            except BaseException:
                self._discard()
                raise
            finally:
                request_path.unlink(missing_ok=True)

    def run(self, argv: Sequence[str], *, cwd: str | None = None) -> tuple[int, str, str]:
        """Run one pinned remote command in the session, opening it non-interactively first if needed.

        :param argv: The pinned ``httk`` command vector.
        :param cwd: The far-side working directory, or the session's own when omitted.
        :return: The command's exit status, standard output and standard error.
        :raises TimeoutError: If the round trip exceeds the timeout; the session is closed.
        :raises RuntimeError: If the session ends during the round trip or answers out of protocol; it is
            closed, and the next call opens a new one.
        """

        with self._lock:
            if self._process is not None and self._process.poll() is not None:
                self._discard()  # it ended between calls (an idle disconnect, say): open a new one
            self.open()
            process = self._process
            assert process is not None and process.stdin is not None
            self._next_id += 1
            request: dict[str, object] = {"id": self._next_id, "argv": list(argv)}
            if cwd is not None:
                request["cwd"] = cwd
            line = None
            try:
                process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
                process.stdin.flush()
                line = self._lines.get(timeout=self._limit)
            except queue.Empty:
                self._discard()
                raise TimeoutError(f"remote session request exceeded {self._limit:g} seconds") from None
            except OSError:
                pass  # a broken pipe: the session ended, reported below with its stderr
            try:
                response = None if line is None else json.loads(line)
            except ValueError:
                response = None
            if (
                not isinstance(response, dict)
                or response.get("id") != request["id"]
                or not isinstance(response.get("returncode"), int)
                or not isinstance(response.get("stdout"), str)
                or not isinstance(response.get("stderr"), str)
            ):
                detail = "the remote session ended" if line is None else f"bad remote session response {line[:200]!r}"
                tail = self._discard()
                raise RuntimeError(f"{detail}: {tail}" if tail else detail)
            return response["returncode"], response["stdout"], response["stderr"]

    def close(self) -> None:
        """End the session: close its input so the far side exits, and kill it if it lingers.

        A round trip in flight is not waited for: its process is killed, and that round trip fails, as
        does a session still opening. A later :meth:`open` or :meth:`run` opens a new session.
        """

        if not self._lock.acquire(timeout=1.0):
            self._closing = True  # set after losing the lock race, so an open() in flight cannot clear it
            process = self._process
            if process is not None and process.poll() is None:
                with contextlib.suppress(OSError):
                    if self._group:
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
            return
        try:
            self._closing = True
            process = self._process
            if process is None:
                return
            with contextlib.suppress(OSError):
                if process.stdin is not None:
                    process.stdin.close()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=2.0)
            self._discard()
        finally:
            self._lock.release()

    def _discard(self) -> str:
        """Kill the session process if it still runs, reap it, and return its stderr tail."""

        process, self._process = self._process, None
        if process is None:
            return ""
        if process.poll() is None:
            if self._group:
                # Started in its own session, the adapter leads a process group: kill what it started too.
                with contextlib.suppress(OSError):
                    os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        process.wait()
        with contextlib.suppress(OSError):
            if process.stdin is not None:
                process.stdin.close()
        if self._error_reader is not None:
            self._error_reader.join(timeout=1.0)
        return "".join(self._errors).strip()


class SessionFailed(RuntimeError):
    """A transfer-session request failed with an unknown outcome; the session is closed and never reopened.

    :param message: What failed.
    :param response: The far side's ``fatal`` response document, when one arrived.
    """

    def __init__(self, message: str, *, response: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.response = None if response is None else dict(response)


class RequestAborted(RuntimeError):
    """A transfer-session request whose stream was aborted; the far side answered, and the session stays usable.

    Raised once the response arrived, from (``__cause__``) what aborted the stream: the upload source or the download
    consumer raising, or :class:`~httk.workflow.errors.StreamAborted` for a download that ended without a successful
    terminal. The far side saw an upload abort as such, never as a complete upload.

    :param message: What was aborted.
    :param response: The far side's response document.
    """

    def __init__(self, message: str, *, response: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.response = dict(response)


#: What every queued event counts for beyond its payload, so that empty frames are bounded too.
_FRAME_OVERHEAD = 64
#: How many bytes of far-side events a transfer session queues before its reader thread stops reading: the largest
#: JSON frame (512 MiB) with its overhead, so that every event fits an empty queue.
_TRANSFER_QUEUE_BYTES = JSON_LIMIT + _FRAME_OVERHEAD
#: The phases in which a far side's progress may stand still: a running ``syncfs`` or fsync, the wait of the
#: visibility deadline before a landing's absent jobs are settled, a build.
_UNCOUNTED_PHASES = frozenset({"sync", "settle", "build"})
#: What :meth:`TransferSession.open` sends through ``echo``: every byte value, so a text-mangling channel shows.
_ECHO_PAYLOAD = bytes(range(256)) * 4
#: The most seconds the opening echo, which reports no progress, may take.
_ECHO_BOUND = 30.0
#: The most seconds the ``bye`` of :meth:`TransferSession.close` may take, progress or not.
_BYE_BOUND = 5.0
#: How long a far side whose output ended may take to exit on its own before its failure is classified.
_EXIT_GRACE = 5.0
#: The longest preamble line read at once; a longer one is read in pieces, none of them a hello.
_PREAMBLE_LINE_BYTES = 1 << 16


@dataclass(frozen=True)
class _Event:
    """One thing the reader thread saw on a transfer session's stdout.

    :param tag: ``hello``, ``foreign`` (another format's hello), ``refused`` (the adapter's result document: a refusal, or a result instead of a session), ``nohello``,
        ``frame``, ``error`` (a protocol error), ``cut`` (the output ended inside a frame) or ``eof``.
    :param value: The document, frame or error.
    :param released: When the reader was last released from a full queue before it read this event.
    :param arrival: When it was complete.
    :param begun: When its first byte was read (a frame's), else its arrival: a frame in transit is no silence.
    """

    tag: str
    value: Any
    released: float
    arrival: float
    begun: float | None = None

    @property
    def started(self) -> float:
        """When the far side began to send it."""

        return self.arrival if self.begun is None else self.begun


class _ByteQueue:
    """A FIFO of reader events bounded by the frame bytes it holds: :meth:`put` blocks while it is full.

    Each event counts its payload plus :data:`_FRAME_OVERHEAD`; the bound is hard. While :meth:`put` is blocked,
    this side holds the far side back, which :meth:`held_back` reports to the bounds at once.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._items: deque[tuple[_Event, int]] = deque()
        self._bytes = 0
        self._closed = False
        self._condition = threading.Condition()
        #: The most bytes the queue ever held.
        self.high_water = 0
        #: When a blocked :meth:`put` last got room: until then this side, not the far side, held the reader back.
        self.released = 0.0
        #: When the reader last read a byte of the far side's output (a frame may take a while to arrive whole).
        self.arrived = 0.0
        self._blocked_since: float | None = None

    def put(self, event: _Event, size: int) -> bool:
        """Append *event*, waiting for room.

        :param event: The event.
        :param size: What it counts for: its payload plus :data:`_FRAME_OVERHEAD`.
        :return: False once the queue was closed (the event is dropped), else true.
        :raises ValueError: If the event alone exceeds the bound.
        """

        with self._condition:
            if size > self._limit:
                raise ValueError(f"an event of {size} bytes exceeds the queue bound of {self._limit}")
            while not self._closed and self._bytes + size > self._limit:
                if self._blocked_since is None:
                    self._blocked_since = time.monotonic()
                self._condition.wait()
            if self._blocked_since is not None:
                self._blocked_since = None
                self.released = time.monotonic()
            if self._closed:
                return False
            self._items.append((event, size))
            self._bytes += size
            self.high_water = max(self.high_water, self._bytes)
            self._condition.notify_all()
            return True

    def get(self, timeout: float) -> _Event:
        """Remove and return the oldest event, waiting for one.

        :param timeout: The most seconds to wait.
        :return: The event.
        :raises queue.Empty: If no event arrives in time.
        """

        with self._condition:
            if not self._condition.wait_for(lambda: bool(self._items), max(0.0, timeout)):
                raise queue.Empty
            return self._pop()

    def held_back(self) -> float:
        """Return when this side last stopped holding the reader back: now, while a :meth:`put` is blocked.

        Read under the queue's lock, so a consumer that just made room never judges the far side before the blocked
        reader got to run.
        """

        with self._condition:
            return time.monotonic() if self._blocked_since is not None else self.released

    def take(self) -> _Event | None:
        """Remove and return the oldest event, or ``None`` when there is none."""

        with self._condition:
            return self._pop() if self._items else None

    def _pop(self) -> _Event:
        event, size = self._items.popleft()
        self._bytes -= size
        self._condition.notify_all()
        return event

    def close(self) -> None:
        """Drop every event and release a blocked :meth:`put` for good."""

        with self._condition:
            self._closed = True
            self._items.clear()
            self._bytes = 0
            self._condition.notify_all()


class _Arrivals:
    """The far side's output as the reader reads it: each read takes what is there (``read1``) and records when a
    byte last arrived (:attr:`_ByteQueue.arrived`), and when the frame being read began (:attr:`begun`)."""

    def __init__(self, stream: IO[bytes], events: "_ByteQueue") -> None:
        self._stream = stream
        self._events = events
        #: When the first byte since :meth:`mark` arrived, if any did.
        self.begun: float | None = None

    def mark(self) -> None:
        """Start a frame: its first byte is the next one read."""

        self.begun = None

    def read(self, size: int = -1) -> bytes:
        """Read at most *size* bytes, those already there when any are.

        :param size: The most bytes to read.
        :return: The bytes; empty at the end of the output.
        """

        read1 = getattr(self._stream, "read1", None)
        data = read1(size) if read1 is not None else self._stream.read(size)
        if data:
            now = time.monotonic()
            self.begun = now if self.begun is None else self.begun
            self._events.arrived = now
        return data


def _read_transfer_session(stream: IO[bytes], events: _ByteQueue) -> None:
    """Turn a transfer session's stdout into events: its hello (or why there is none), its frames, then ``eof``.

    Up to :data:`_SESSION_PREAMBLE_LINES` lines ahead of the hello (a login banner, a printing ``cli_prelude``)
    are skipped. The stream is closed at the end. Every byte read after the hello is recorded as it arrives
    (:class:`_Arrivals`), so that a large frame still in transit is no silence of the far side.
    """

    def emit(tag: str, value: object, size: int = 0, begun: float | None = None) -> bool:
        return events.put(_Event(tag, value, events.released, time.monotonic(), begun), size + _FRAME_OVERHEAD)

    frames: FrameReader | None = None
    try:
        with stream:
            for _ in range(_SESSION_PREAMBLE_LINES):
                line = stream.readline(_PREAMBLE_LINE_BYTES)
                if not line:
                    return
                try:
                    document = json.loads(line.decode("utf-8"))
                except (ValueError, RecursionError):
                    continue
                if not isinstance(document, dict):
                    continue
                kind = document.get("format")
                if kind == RESULT_FORMAT:
                    # The adapter's own result document (a refusal, or a result where a session was asked for):
                    # the adapter's answer, never the far side's httk.
                    emit("refused", document)
                    return
                if kind == TRANSFER_SESSION_FORMAT:
                    emit("hello", document)
                    break
                if isinstance(kind, str) and "format_version" in document:
                    emit("foreign", document)
                    return
            else:
                emit("nohello", None)
                return
            arrivals = _Arrivals(stream, events)
            frames = FrameReader(cast(IO[bytes], arrivals))
            while (frame := frames.read_frame()) is not None:
                if not emit("frame", frame, frame.size, arrivals.begun):
                    return
                arrivals.mark()
    except TransferProtocolError as exc:
        emit("cut" if frames is not None and frames.ended else "error", exc)
    except (OSError, ValueError) as exc:
        emit("error", exc)
    finally:
        emit("eof", None)


#: What an ``httk workflow`` without the ``transfer-session`` command prints, exit status 2: argparse's refusal of
#: the subcommand, ``PROG workflow: error: argument GROUP: invalid choice: 'transfer-session' (choose from ...)``,
#: PROG being ``httk`` (or the script name ``httk_command`` runs), ``GROUP`` the metavar of every released version.
_NO_SUCH_COMMAND = re.compile(r"(?m)^\S+ workflow: error: argument GROUP: invalid choice: 'transfer-session'(?: \(|$)")


def _exit_status(process: "subprocess.Popen[bytes]", wait: float) -> int | None:
    """Return *process*'s exit status once it exited, waiting up to *wait* seconds, without reaping it.

    An unreaped process keeps its pid, and so its process group id, from being reused before the group is killed.

    :param process: The far-side process.
    :param wait: The most seconds to wait.
    :return: The exit status (negative for a signal), or ``None`` while it runs.
    """

    waitid = getattr(os, "waitid", None)
    deadline = time.monotonic() + wait
    while True:
        if process.returncode is not None:
            return process.returncode
        if waitid is None:
            if process.poll() is not None:
                return process.returncode
        else:
            try:
                info = waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            except ChildProcessError:
                return process.poll()
            if info is not None:
                return info.si_status if info.si_code == os.CLD_EXITED else -info.si_status
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.02)


class _Watch:
    """The bounds of one request, judged by when frames arrived rather than when they were consumed.

    Idle: no inbound byte (of a frame, whole or still in transit) and no accepted upload bytes for the bound. Stall:
    neither the far side's progress (``phase``, ``count``) nor any data (accepted upload bytes, arriving download
    bytes, the bytes of a frame in transit) moved for the bound, outside the ``sync``, ``settle`` and ``build``
    phases; an empty ``D`` frame shows the far side alive but moves nothing. The bytes of a progress tick count as
    arriving, so while ticks keep coming a stall is noticed with the first tick that began to arrive after its
    deadline (:meth:`late`): at the granularity of the ticks, not at the deadline itself.

    A frame is judged by when it began to arrive: a queued frame that began after its deadline fails the request
    all the same, and one that began in time is accepted however long the rest of it took. A pause in the middle of
    a frame is therefore noticed only while this side waits for it (:meth:`remaining`, with the time of the last
    byte read); one that ended while this side was busy elsewhere leaves a whole frame, which is accepted, since
    failing a complete answer would turn its success into an unknown outcome.

    The bounds measure the far side's liveness only, so time in which this side offered it nothing is never charged
    to it: the reader blocked on a full queue (this side's back-pressure), and an upload source preparing its next
    write. Each upload write, and the terminal, re-arm both clocks first (:meth:`rearm`); a write after a source pause
    longer than the bound therefore revives a request whose far side sent nothing meanwhile, by design. An optional
    overall deadline bounds the request regardless of progress.

    :param bound: The bound in seconds.
    :param deadline: The overall deadline (``time.monotonic()``), if any.
    """

    def __init__(self, bound: float, deadline: float | None = None) -> None:
        now = time.monotonic()
        self.bound = bound
        self._deadline = deadline
        self._activity = now
        self._moved = now
        self._progress: tuple[str, int | None] | None = None

    def _due(self, released: float, arrived: float = 0.0) -> tuple[float, str]:
        """Return the deadline and what expires then; *released* is when this side last stopped holding back the
        reader, before which the far side could not send, and *arrived* when a byte of the far side's output last
        arrived (part of a frame still in transit counts: the far side is sending)."""

        due = max(self._activity, released, arrived) + self.bound
        reason = f"the far side sent nothing and took no data for {self.bound:g} seconds"
        if self._progress is not None and self._progress[0] not in _UNCOUNTED_PHASES:
            stalled = max(self._moved, released, arrived) + self.bound
            if stalled < due:
                phase, count = self._progress
                due = stalled
                reason = (
                    f"the far side's {phase} progress stood at {count} and no data moved for {self.bound:g} seconds"
                )
        if self._deadline is not None and self._deadline < due:
            due, reason = self._deadline, "the far side did not answer in time"
        return due, reason

    def remaining(self, released: float, arrived: float = 0.0) -> tuple[float, str]:
        """Return the seconds left and what expires then, given when the reader was last released from a full queue
        and when a byte of the far side's output last arrived."""

        due, reason = self._due(released, arrived)
        return due - time.monotonic(), reason

    def late(self, event: _Event) -> str | None:
        """Return what expired before *event* began to arrive, or ``None`` when it was in time (a frame whose first
        byte came in time was being sent, however long the rest took)."""

        due, reason = self._due(event.released)
        return reason if event.started > due else None

    def saw(self, frame: Frame, at: float) -> None:
        """Note a frame that arrived.

        :param frame: The frame.
        :param at: When it arrived.
        :raises TransferProtocolError: If it is a malformed progress frame.
        """

        self._activity = max(self._activity, at)
        if frame.kind == DATA and frame.data:
            self._moved = max(self._moved, at)
        elif frame.kind == PROGRESS:
            assert frame.document is not None
            phase, count = frame.document.get("phase"), frame.document.get("count")
            if not isinstance(phase, str) or (
                count is not None and (isinstance(count, bool) or not isinstance(count, int))
            ):
                raise TransferProtocolError(f"malformed progress frame {frame.document!r}")
            if (phase, count) != self._progress:
                self._progress, self._moved = (phase, count), max(self._moved, at)

    def sent(self) -> None:
        """Note upload bytes the far side accepted just now."""

        self._activity = self._moved = time.monotonic()

    def rearm(self) -> None:
        """Restart both clocks: this side offers the far side data now, after a pause that was its own."""

        self._activity = self._moved = time.monotonic()


class _UploadStream(io.RawIOBase, BinaryIO):
    """The writable upload of one transfer-session request: each write goes out at once as whole ``D`` frames.

    Writes run in the calling thread; while one blocks, the far side's progress is serviced and the bounds apply.
    The terminal is the session's to send, not this stream's.
    """

    def __init__(self, session: "TransferSession", request_id: int, watch: _Watch) -> None:
        super().__init__()
        self._session = session
        self._id = request_id
        self._watch = watch

    def writable(self) -> bool:
        """Return true: the stream is writable."""

        return True

    def write(self, data: Buffer, /) -> int:
        """Send *data* as ``D`` frames of up to 1 MiB, sliced from it without copying.

        :param data: The bytes.
        :return: How many bytes were sent (all of them).
        :raises ValueError: If the request is over.
        """

        if self.closed:
            raise ValueError("write to a finished upload")
        self._watch.rearm()  # the source's pause before this write was this side's, not the far side's
        view = memoryview(data).cast("B")
        for offset in range(0, len(view), DATA_LIMIT):
            piece = view[offset : offset + DATA_LIMIT]
            self._session._write(encode_header(DATA, self._id, len(piece)), self._id, self._watch)
            self._session._write(piece, self._id, self._watch)
        return len(view)


class TransferSession:
    """A far-side ``httk workflow transfer-session`` reached through the adapter ``session`` operation.

    One connection and one far-side ``httk`` start carry every request of a transfer, each a :meth:`request` in binary
    frames (the grammar is in ``httk.workflow._frames``). Whether the session can be used is decided by :meth:`open`
    alone, before anything is changed: :class:`SessionUnsupported` means that it cannot be (the adapter cannot open
    one, or the far side speaks another :data:`TRANSFER_PROTOCOL`), and the transfer stops there. After
    that, any failure (a bound expiring, the far side ending, a protocol error, a fatal op) is :class:`SessionFailed`,
    an unknown outcome of the request in flight: the session is closed and every later request raises; it never
    reconnects. Use it as a context manager, or call :meth:`close`.

    The far side's frames are read by a thread into a queue of 512 MiB (each frame counted with 64 bytes of overhead;
    a JSON frame is at most 512 MiB, so every frame fits). The reader stops reading while the queue is full, so a slow
    download consumer holds back the far side instead of growing this process. Upload and download run in the
    calling thread.

    A failed or closed non-interactive session kills the far side's whole process group. An interactive one (started
    with the terminal, so ssh can prompt) shares this process's group, so only the adapter process itself is killed:
    an accepted limitation, under which a descendant that keeps the pipes open can delay the end of :meth:`close` (by
    the reader threads' joins, 2 seconds each) and survive it.

    :param bundle: The adapter bundle path.
    :param timeout: The bound in seconds, or the bundle's ``timeout_seconds`` when omitted: on the far side's hello,
        and, per request, on the far side being idle (no frame, no upload bytes taken) or stalled (no progress, no data
        moving outside the ``sync``, ``settle`` and ``build`` phases). A request's bound is at least three progress intervals
        (30 seconds), since the far side reports progress only every 10 seconds.
    """

    def __init__(self, bundle: str | os.PathLike[str], *, timeout: float | None = None) -> None:
        self._root = Path(bundle).expanduser().resolve()
        self._timeout = timeout
        self._limit = 60.0
        self._lock = threading.Lock()
        self._owner: int | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._group = False
        self._ssh = False
        self._selector: selectors.BaseSelector | None = None
        self._events = _ByteQueue(_TRANSFER_QUEUE_BYTES)
        self._reader: threading.Thread | None = None
        self._errors: deque[str] = deque(maxlen=_SESSION_STDERR_TAIL)
        self._error_reader: threading.Thread | None = None
        self._next_id = 0
        self._status: int | None = None
        self._closing = False
        # new -> open -> closed, or -> unsupported (at open), or -> failed (at open or a request)
        self._state = "new"
        self._reason = ""
        self._kind = "adapter"

    @property
    def is_open(self) -> bool:
        """Return whether the session is open (opened, and neither failed nor closed since)."""

        return self._state == "open"

    def __enter__(self) -> Self:
        """Return this session; :meth:`open` it before the first request."""

        return self

    def __exit__(self, *_exc_info: object) -> None:
        """Close the session."""

        self.close()

    @contextlib.contextmanager
    def _held(self) -> Iterator[None]:
        """Hold the session for one call; a call nested in a request's own callback raises at once."""

        if self._owner == threading.get_ident():
            raise RuntimeError("a transfer session request is already in flight in this thread")
        with self._lock:
            self._owner = threading.get_ident()
            try:
                yield
            finally:
                self._owner = None

    def _bound(self) -> float:
        return max(self._limit, 3 * _frames.TICK)

    def open(self, *, interactive: bool = False) -> None:
        """Start the far side and check it: its hello, which must name :data:`TRANSFER_PROTOCOL`, and a binary
        ``echo``, whose header carries that protocol for the far side to check in turn.

        :param interactive: Whether the adapter keeps this process's controlling terminal (and so lets ssh
            prompt); False starts it in a new session without one.
        :raises SessionUnsupported: Of kind ``adapter`` if the adapter refuses the operation (or prints its own
            result document instead of a session) or the echo does not come back intact (a channel that mangles
            binary data); of kind ``version`` if the far side prints another format's hello, its hello names another
            protocol, it refuses this one (naming its own), or it ends with exit status 2 before its hello after
            ``httk workflow``'s argparse refused ``invalid choice: 'transfer-session'`` (an older ``httk workflow``
            without the command); of kind ``missing`` if it ends before its hello with exit
            status 127 (no ``httk`` there, or a command of its ``cli_prelude`` not found); of kind ``other`` for any
            other end before the session was open (a crash, a refusal for a reason of its own). The caller stops;
            nothing was changed. Raised again by a later call, without starting anything.
        :raises SessionFailed: If the session was opened, failed or closed before.
        :raises TimeoutError: If no hello arrives within the bound.
        :raises RuntimeError: If ssh itself fails (status 255, an ``ssh`` remote only), such as a refused login, at
            any stage; or if :meth:`close` ended the opening.
        """

        with self._held():
            if self._state == "unsupported":
                raise SessionUnsupported(self._reason, kind=self._kind)
            if self._state != "new":
                raise SessionFailed(f"the transfer session was {self._state} already: {self._reason}")
            metadata = validate_adapter_bundle(self._root)
            self._limit = float(metadata.get("timeout_seconds", 60.0) if self._timeout is None else self._timeout)
            self._ssh = metadata.get("kind") == "ssh"
            request_path = _request_file(self._root, "session", {"argv": list(REMOTE_TRANSFER_SESSION_COMMAND)})
            try:
                self._start(request_path, interactive)
                self._hello()
                self._echo()
                self._state = "open"
            except SessionUnsupported as exc:
                self._discard()
                self._state, self._reason, self._kind = "unsupported", str(exc), exc.kind
                raise
            except BaseException as exc:
                self._discard()
                if self._state != "closed":
                    self._state, self._reason = "failed", str(exc)
                raise
            finally:
                request_path.unlink(missing_ok=True)

    def _start(self, request_path: Path, interactive: bool) -> None:
        process = subprocess.Popen(
            [str(self._root / ADAPTER_EXECUTABLE), str(request_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=not interactive,
        )
        self._process, self._group = process, not interactive
        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        # Writes never block past the bound: a far side that stops reading cannot hang this process.
        os.set_blocking(process.stdin.fileno(), False)
        self._selector = selectors.DefaultSelector()
        self._selector.register(process.stdin.fileno(), selectors.EVENT_WRITE)
        self._reader = threading.Thread(
            target=_read_transfer_session, args=(process.stdout, self._events), name="httk-transfer-reader", daemon=True
        )
        self._reader.start()
        errors = io.TextIOWrapper(process.stderr, encoding="utf-8", errors="replace")
        self._error_reader = threading.Thread(target=_keep_tail, args=(errors, self._errors), daemon=True)
        self._error_reader.start()

    def _classify(self, detail: str, kind: str) -> BaseException:
        """Return what a failure before the session was open means; the one place that decides on a stop of kind
        *kind* (:class:`SessionUnsupported`).

        The far side's input is closed and its exit status waited for (:data:`_EXIT_GRACE`) first. A :meth:`close`
        from another thread is no such stop, and neither is ssh's own failure (status 255 of an ``ssh`` remote): a
        connection failure, whatever the far side runs. Everything else is :class:`SessionUnsupported`; with
        *kind* ``ended`` (the far side ended before its hello) its kind is ``missing`` for exit status 127,
        ``version`` when the far side's ``httk workflow`` has no such command (:data:`_NO_SUCH_COMMAND`), else
        ``other``.

        :param detail: What failed.
        :param kind: ``adapter``, ``version``, ``missing``, ``other`` (see :class:`SessionUnsupported`) or ``ended``.
        :return: The exception to raise.
        """

        status, tail = self._discard(wait=_EXIT_GRACE)
        message = f"{detail} (exit status {status}): {tail}" if tail else f"{detail} (exit status {status})"
        if self._closing:
            self._state = "closed"
            return RuntimeError(f"the transfer session was closed while opening: {message}")
        if self._ssh and status == 255:
            return RuntimeError(f"the transfer session's ssh connection failed: {message}")
        if kind == "ended":
            if status == 127:
                kind = "missing"
            else:
                kind = "version" if status == 2 and _NO_SUCH_COMMAND.search(tail) is not None else "other"
        return SessionUnsupported(message, kind=kind)

    def _hello(self) -> None:
        try:
            event = self._events.get(self._limit)
        except queue.Empty:
            if self._closing:
                raise self._classify("no hello", "other") from None
            raise TimeoutError(f"the transfer session did not start within {self._limit:g} seconds") from None
        document = event.value
        if event.tag == "refused":
            if document.get("ok") is False:
                raise self._classify(str(document.get("error", "the adapter refused the session")), "adapter")
            raise self._classify("the adapter answered with a result document instead of a transfer session", "adapter")
        if event.tag == "foreign":
            raise self._classify(
                f"the far side speaks {document.get('format')!r} version {document.get('format_version')!r}, "
                "not a transfer session",
                "version",
            )
        if event.tag == "nohello":
            raise self._classify(f"no transfer session hello within {_SESSION_PREAMBLE_LINES} lines", "other")
        if event.tag != "hello":
            raise self._classify("the transfer session ended before its hello", "ended")
        if (version := document.get("format_version")) != TRANSFER_PROTOCOL:
            raise self._classify(f"its transfer protocol is {version!r}, this side's {TRANSFER_PROTOCOL}", "version")

    def _echo(self) -> None:
        received = bytearray()
        bound = min(self._limit, _ECHO_BOUND)  # absolute: the far side's progress cannot extend it
        try:
            response = self._request(
                "echo",
                {"protocol": TRANSFER_PROTOCOL},  # the far side refuses another one, before any op
                upload=lambda stream: stream.write(_ECHO_PAYLOAD),
                download=lambda stream: received.extend(stream.read()),
                bound=bound,
                deadline=time.monotonic() + bound,
            )
        except (SessionFailed, RequestAborted) as exc:
            # Frames that do not decode came through a channel that changes bytes: the adapter's fault.
            mangled = isinstance(exc, SessionFailed) and "broke its protocol" in str(exc)
            raise self._classify(
                f"the transfer session echo failed: {exc}", "adapter" if mangled else "other"
            ) from None
        if response.get("ok") is not True:
            protocol = response.get("protocol")
            if protocol is not None and protocol != TRANSFER_PROTOCOL:
                detail = f"it refused this side's transfer protocol {TRANSFER_PROTOCOL}, speaking {protocol!r}"
                raise self._classify(f"{detail}: {response.get('error')}", "version")
            raise self._classify(f"it refused the transfer session: {response.get('error')}", "other")
        if bytes(received) != _ECHO_PAYLOAD:
            raise self._classify("the transfer session does not carry binary data intact", "adapter")

    def request(
        self,
        op: str,
        header: Mapping[str, Any] | None = None,
        *,
        upload: Callable[[BinaryIO], object] | None = None,
        download: Callable[[BinaryIO], object] | None = None,
    ) -> dict[str, Any]:
        """Run one request: send it and its upload, hand its download to *download*, and return its response.

        :param op: The op name.
        :param header: The op's own request members.
        :param upload: Writes the upload to the writable stream it is given (in this thread; each write goes out as
            ``D`` frames). If it raises, the far side gets an aborting terminal and the request raises
            :class:`RequestAborted` once the response arrived.
        :param download: Reads the download from the readable stream it is given (in this thread), which reads as
            ended at a successful terminal and raises :class:`~httk.workflow.errors.StreamAborted` at an aborting
            one. It is called only when a download arrives; a refusal may come without one, but an ``ok`` response
            without a download breaks the protocol. What it leaves unread is dropped, provided the download ends
            successfully.
        :return: The response document (``ok`` and the op's members).
        :raises SessionFailed: If the session is not open, or the request failed with an unknown outcome (a
            bound expired, the far side ended or broke the protocol, the op was fatal); the session is closed.
        :raises RequestAborted: If the upload or the download was aborted; the session stays usable.
        :raises RuntimeError: If called from within a request's own callback.
        """

        with self._held():
            if self._state != "open":
                raise SessionFailed(f"the transfer session is {self._state}: {self._reason}".rstrip(": "))
            return self._request(op, header or {}, upload=upload, download=download, bound=self._bound())

    def _request(
        self,
        op: str,
        header: Mapping[str, Any],
        *,
        upload: Callable[[BinaryIO], object] | None,
        download: Callable[[BinaryIO], object] | None,
        bound: float,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        document: dict[str, Any] = {**header, "op": op}
        document.pop("upload", None)
        if upload is not None:
            document["upload"] = True
        request_id = self._next_id + 1
        encoded = encode_json(REQUEST, request_id, document)  # an unsendable header raises before anything changed
        self._next_id = request_id
        watch = _Watch(bound, deadline)
        aborted: BaseException | None = None
        streamed = False
        try:
            self._expect_nothing()
            self._write(encoded, request_id, watch)
            if upload is not None:
                aborted = self._upload(request_id, upload, watch)
            while True:
                frame = self._receive(request_id, watch)
                if frame.kind == PROGRESS:
                    continue
                if frame.kind == RESPONSE:
                    break
                if frame.kind not in {DATA, END} or download is None or streamed:
                    raise TransferProtocolError(f"an unexpected {frame.kind} frame in the answer to {op}")
                streamed = True
                aborted = self._download(request_id, download, frame, watch, aborted)
            assert frame.document is not None
            response = frame.document
            if download is not None and response["ok"] is True and not streamed:
                raise TransferProtocolError(f"{op} answered ok without its download")
        except TransferProtocolError as exc:
            raise self._fail(f"the transfer session broke its protocol: {exc}") from None
        except SessionFailed:
            raise
        except BaseException:
            # Interrupted between frames of the request: what follows on the pipe is no longer known.
            self._fail("the transfer session request was interrupted")
            raise
        if aborted is not None:
            raise RequestAborted(f"{op} was aborted: {aborted}", response=response) from aborted
        return response

    def _upload(self, request_id: int, upload: Callable[[BinaryIO], object], watch: _Watch) -> BaseException | None:
        """Run the upload callable, then send exactly one terminal; return what aborted the upload, if anything."""

        stream = _UploadStream(self, request_id, watch)
        try:
            upload(stream)
        except (SessionFailed, TransferProtocolError):
            raise
        except Exception as exc:
            stream.close()
            abort = f"{type(exc).__name__}: {exc}"
            watch.rearm()
            self._write(encode_json(END, request_id, {"ok": False, "abort": abort}), request_id, watch)
            return exc
        stream.close()
        watch.rearm()
        self._write(encode_json(END, request_id, {"ok": True}), request_id, watch)
        return None

    def _download(
        self,
        request_id: int,
        download: Callable[[BinaryIO], object],
        first: Frame,
        watch: _Watch,
        aborted: BaseException | None,
    ) -> BaseException | None:
        """Hand the download starting at *first* to *download*; return what aborted the request, if anything."""

        stream = StreamReader(lambda: self._stream_frame(request_id, watch), request_id, first)
        try:
            if aborted is None:
                try:
                    download(stream)
                except (SessionFailed, TransferProtocolError):
                    raise
                except Exception as exc:
                    aborted = exc
            if aborted is None:
                try:
                    stream.finish()
                except StreamAborted as exc:
                    aborted = exc
            else:
                stream.discard()
        finally:
            stream.close()
        return aborted

    def _stream_frame(self, request_id: int, watch: _Watch) -> Frame:
        """Return the next ``D`` or ``E`` frame of the request's download, servicing its progress."""

        while True:
            frame = self._receive(request_id, watch)
            if frame.kind in {DATA, END}:
                return frame
            if frame.kind != PROGRESS:
                raise self._fail(f"the transfer session broke its protocol: a {frame.kind} frame inside a download")

    def _receive(self, request_id: int, watch: _Watch) -> Frame:
        if self._process is None:
            raise SessionFailed(f"the transfer session is {self._state}: {self._reason}")
        # A queued event is judged by when it arrived (``_frame``), however long it waited for this side.
        event = self._events.take()
        while event is None:
            # A frame in transit (its bytes arriving) moves the deadline on: waited for again, not failed.
            remaining, reason = watch.remaining(self._events.held_back(), self._events.arrived)
            if remaining <= 0:
                raise self._fail(reason)
            with contextlib.suppress(queue.Empty):
                event = self._events.get(remaining)
        return self._frame(event, request_id, watch)

    def _frame(self, event: _Event, request_id: int, watch: _Watch) -> Frame:
        """Return the frame of *event* for the active request, or fail the session for anything else it means."""

        if event.tag in {"eof", "cut"}:
            raise self._fail(f"the transfer session ended: {event.value or 'end of output'}", wait=_EXIT_GRACE)
        if event.tag != "frame":
            raise self._fail(f"the transfer session broke its protocol: {event.value}")
        frame: Frame = event.value
        if frame.kind == RESPONSE and frame.id == request_id:
            assert frame.document is not None
            if frame.document.get("fatal") is True:
                raise self._fail(f"the far side failed: {frame.document.get('error')}", frame.document)
            if not isinstance(frame.document.get("ok"), bool):
                raise self._fail("the transfer session broke its protocol: a response needs a boolean ok")
        late = watch.late(event)
        if late is not None:
            raise self._fail(late)
        if frame.id != request_id or frame.kind == REQUEST:
            raise self._fail(
                f"the transfer session broke its protocol: a {frame.kind} frame of request {frame.id} "
                f"during request {request_id}"
            )
        try:
            watch.saw(frame, event.arrival)
        except TransferProtocolError as exc:
            raise self._fail(f"the transfer session broke its protocol: {exc}") from None
        return frame

    def _expect_nothing(self) -> None:
        """Fail the session for anything the far side sent between requests, before a new request is sent."""

        event = self._events.take()
        if event is None:
            return
        if event.tag in {"eof", "cut"}:
            raise self._fail("the transfer session ended between requests", wait=_EXIT_GRACE)
        frame = event.value
        detail = f"an unsolicited {frame.kind} frame of request {frame.id}" if event.tag == "frame" else event.value
        raise self._fail(f"the transfer session broke its protocol between requests: {detail}")

    def _write(self, data: Buffer, request_id: int, watch: _Watch) -> None:
        """Write all of *data*, servicing progress; fail the session when a bound expires while it blocks."""

        process, selector = self._process, self._selector
        if process is None or process.stdin is None or selector is None:
            raise SessionFailed(f"the transfer session is {self._state}: {self._reason}")
        descriptor = process.stdin.fileno()
        view = memoryview(data).cast("B")
        while view:
            while (event := self._events.take()) is not None:
                frame = self._frame(event, request_id, watch)
                if frame.kind != PROGRESS:
                    raise self._fail(
                        f"the transfer session broke its protocol: a {frame.kind} frame before the upload ended"
                    )
            try:
                written = os.write(descriptor, view)
            except BlockingIOError:
                written = 0
            except OSError as exc:
                raise self._fail(
                    f"the transfer session ended: {exc}", self._fatal_response(request_id), wait=_EXIT_GRACE
                ) from None
            if written:
                view = view[written:]
                watch.sent()
                continue
            remaining, reason = watch.remaining(self._events.held_back(), self._events.arrived)
            if remaining <= 0:
                raise self._fail(reason)
            selector.select(min(remaining, 0.1))

    def _fatal_response(self, request_id: int) -> dict[str, Any] | None:
        """After a refused write, look briefly (2 seconds at most) for the fatal response the far side sent."""

        deadline = time.monotonic() + 2.0
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                event = self._events.get(remaining)
            except queue.Empty:
                return None
            if event.tag != "frame":
                return None
            frame: Frame = event.value
            if frame.kind == RESPONSE and frame.id == request_id and frame.document is not None:
                return dict(frame.document) if frame.document.get("fatal") is True else None
        return None

    def _fail(self, message: str, response: Mapping[str, Any] | None = None, *, wait: float = 0.0) -> SessionFailed:
        """Close the session for good and return the :class:`SessionFailed` to raise.

        :param message: What failed.
        :param response: The far side's fatal response, if any.
        :param wait: How many seconds the far side may take to exit on its own (its output ended); while the session
            is opening, always :data:`_EXIT_GRACE`, so that the classification sees its exit status.
        :return: The exception.
        """

        _status, tail = self._discard(wait=max(wait, _EXIT_GRACE) if self._state == "new" else wait)
        if self._state in {"new", "open"}:
            self._state, self._reason = "failed", message
        return SessionFailed(f"{message}: {tail}" if tail else message, response=response)

    def close(self) -> None:
        """End the session: say ``bye`` (5 seconds at most) when it is idle and healthy, then stop the far side.

        A request in flight (in another thread, or this one's callback) is not waited for: the far side is killed
        and that request fails. A closed session is never reopened.
        """

        self._closing = True
        if self._owner == threading.get_ident() or not self._lock.acquire(timeout=1.0):
            self._kill()
            return
        try:
            self._owner = threading.get_ident()
            if self._state == "open":
                with contextlib.suppress(SessionFailed, RequestAborted):
                    deadline = time.monotonic() + _BYE_BOUND
                    self._request("bye", {}, upload=None, download=None, bound=self._bound(), deadline=deadline)
            if self._state in {"new", "open"}:
                self._state, self._reason = "closed", "closed by its owner"
            process = self._process
            if process is not None and process.stdin is not None:
                with contextlib.suppress(OSError):
                    process.stdin.close()
            self._discard(wait=2.0)
        finally:
            self._owner = None
            self._lock.release()

    def _kill(self) -> None:
        """Kill the far side (its process group when it has one) without reaping it."""

        process = self._process
        if process is None:
            return
        with contextlib.suppress(OSError):
            if self._group:
                os.killpg(process.pid, signal.SIGKILL)
            elif process.returncode is None:
                process.kill()

    def _discard(self, *, wait: float = 0.0) -> tuple[int | None, str]:
        """Stop the far side and everything around it: kill, reap, and join the reader threads.

        The process group of a non-interactive session is killed whether or not its leader still runs, so nothing
        the far side started survives it.

        :param wait: How many seconds the far side may take to exit on its own first, its input closed.
        :return: The exit status (negative when killed) and the stderr tail.
        """

        process, self._process = self._process, None
        if process is not None:
            if wait:
                with contextlib.suppress(OSError):  # a far side waiting for input ends at its end
                    if process.stdin is not None:
                        process.stdin.close()
            status = _exit_status(process, wait)
            with contextlib.suppress(OSError):
                if self._group:
                    os.killpg(process.pid, signal.SIGKILL)
                elif status is None:
                    process.kill()
            process.wait()
            self._status = status if status is not None else process.returncode
            with contextlib.suppress(OSError):
                if process.stdin is not None:
                    process.stdin.close()
            self._events.close()
            if self._selector is not None:
                self._selector.close()
                self._selector = None
            for thread in (self._reader, self._error_reader):
                if thread is not None:
                    thread.join(timeout=2.0)
        return self._status, "".join(self._errors).strip()
