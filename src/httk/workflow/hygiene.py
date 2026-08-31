"""Describe and repair a project's remotes, and check its workspace health.

Everything here answers an operator question about state that already exists:
*what is this remote configured to do*, *what is wrong with this workspace and
can it be fixed*. Nothing here is on the execution path of a job. The individual
workspace checks are the ones the project-member ``repair`` and ``scan_project``
surface through core's ``httk project repair``; each repair is explicit.
"""

import logging
import os
import shutil
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._util import read_json
from .adapters import (
    ADAPTER_EXECUTABLE,
    CREDENTIALS_FILE,
    metadata_path,
    project_remote_roots,
    read_credentials,
    valid_remote_name,
    validate_adapter_bundle,
)
from .configuration import remotes_home
from .errors import WorkflowError
from .manifests import (
    read_maintenance_lock,
    release_maintenance_lock,
)
from .models import STATE_KINDS, WORKSPACE_DIRECTORY
from .projects import (
    discover_project,
    key_fingerprint,
    read_project,
    read_project_section,
)
from .registry import list_workspaces, resolve_workspace
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "REMOTE_DESCRIPTION_FORMAT",
    "TMP_MAXIMUM_AGE_SECONDS",
    "Finding",
    "describe_remote",
    "remove_remote",
]

REMOTE_DESCRIPTION_FORMAT = "httk-remote-description"

#: How long a staging entry may sit below ``.httk-workspace/tmp`` before the
#: check calls it a leftover. Every publication renames its staging entry out
#: within one operation, so a day is far beyond any honest in-flight window.
TMP_MAXIMUM_AGE_SECONDS = 24 * 60 * 60


def _key_record(value: str) -> dict[str, object]:
    return {"public_key": value, "fingerprint": key_fingerprint(value)}


def _workspace_of(project: Path) -> Workspace | None:
    """Attach the project's workspace read-only, or report that there is none."""

    try:
        return Workspace(project, mutable=False)
    except (WorkflowError, OSError, ValueError):
        return None


def _workspace_summary(project: Path, metadata: Mapping[str, object]) -> dict[str, object]:
    """Summarize the project workspace without mutating anything in it."""

    summary: dict[str, object] = {
        "present": (project / WORKSPACE_DIRECTORY / "format.json").is_file(),
        "initialization_failed": metadata.get("workspace_initialization_failed") is True,
    }
    section = read_project_section(project, "workspace")
    recorded = section.get("default")
    default: dict[str, object] = {"name": recorded if isinstance(recorded, str) else None, "resolves": False}
    if isinstance(recorded, str):
        try:
            binding = resolve_workspace(recorded, project=project)
            if binding.remote == "local" and (
                binding.path is None or not (Path(binding.path) / WORKSPACE_DIRECTORY / "format.json").is_file()
            ):
                raise ValueError("registered workspace path is not reachable")
        except (OSError, ValueError, WorkflowError):
            pass
        else:
            default["resolves"] = True
    summary["default"] = default
    workspace = _workspace_of(project)
    if workspace is None:
        return summary
    counts: dict[str, int] = {}
    for marker in workspace.scan_markers(STATE_KINDS):
        counts[marker.kind] = counts.get(marker.kind, 0) + 1
    holder = read_maintenance_lock(workspace)
    summary.update(
        {
            "workspace_id": workspace.workspace_id,
            "core_profile": workspace.core_profile,
            "extensions": sorted(workspace.extensions),
            "counts": counts,
            "jobs": sum(counts.values()),
            "maintenance_lock": (
                None
                if holder is None
                else {"holder": holder.describe(), "stale": holder.is_stale(), "path": str(holder.path)}
            ),
        }
    )
    return summary


def _project_remotes(project: Path) -> list[str]:
    """Return the names of the remotes defined in *project*, sorted."""

    return sorted(
        {
            path.name
            for root in project_remote_roots(project)
            if root.is_dir()
            for path in root.iterdir()
            if path.is_dir()
        }
    )


def _remote_bundle(name: str, *, project: str | os.PathLike[str] | None) -> tuple[Path, str]:
    """Locate one remote bundle directory, project-local before global.

    The bundle is located by name rather than resolved through the adapter
    contract, because describing or removing a remote must still work when the
    bundle is exactly what is broken about it.
    """

    valid_remote_name(name)
    project_root = discover_project(project)
    if project_root is not None:
        for root in project_remote_roots(project_root):
            local = root / name
            if local.is_dir():
                return local, "project"
    shared = remotes_home() / name
    if shared.is_dir():
        return shared, "global"
    raise ValueError(f"unknown remote: {name}")


def describe_remote(
    name: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> dict[str, object]:
    """Describe one remote: where it lives, what it is, how it is configured.

    Credential values never appear. A remote's settings are reported with the
    file each one came from, and for a setting stored in the manifest-excluded
    ``credentials.json`` only its *name* is reported: a description an operator
    can paste into a bug report must never carry a password.

    :param name: Remote bundle name.
    :param project: Project directory used for project-local lookup.
    :return: JSON-compatible remote description.
    :raises ValueError: If the remote name is invalid or unknown.
    """

    bundle, scope = _remote_bundle(name, project=project)
    try:
        metadata: dict[str, Any] = dict(validate_adapter_bundle(bundle))
        valid, problem = True, None
    except (OSError, ValueError) as exc:
        recorded = metadata_path(bundle)
        metadata = read_json(recorded) if recorded.is_file() else {}
        valid, problem = False, str(exc)
    persisted = metadata.get("settings", {})
    persisted = dict(persisted) if isinstance(persisted, Mapping) else {}
    credentials = sorted(read_credentials(bundle))
    return {
        "format": REMOTE_DESCRIPTION_FORMAT,
        "format_version": 2,
        "name": name,
        "scope": scope,
        "bundle": str(bundle),
        "valid": valid,
        "problem": problem,
        "kind": metadata.get("kind"),
        "adapter_version": metadata.get("adapter_version"),
        "timeout_seconds": metadata.get("timeout_seconds", 60.0),
        "required_binaries": list(metadata.get("required_binaries", [])),
        # One dispatcher serves every operation; the operation name travels in
        # the request JSON, so a single executable path is all there is to report.
        "adapter": str(bundle / ADAPTER_EXECUTABLE),
        "settings": persisted,
        "settings_source": {
            **{key: CREDENTIALS_FILE for key in credentials},
            **{key: metadata_path(bundle).name for key in sorted(persisted)},
        },
        "credential_keys": credentials,
        "credentials_file": str(bundle / CREDENTIALS_FILE) if (bundle / CREDENTIALS_FILE).is_file() else None,
    }


def _pending_remote_transfers(remote: str) -> list[str]:
    """Return registered local workspace names with live transfers to *remote*."""

    names: list[str] = []
    for binding in list_workspaces():
        assert binding.path is not None
        directory = Path(binding.path) / WORKSPACE_DIRECTORY / "transfers"
        for path in sorted(directory.glob("*.json")) if directory.is_dir() else ():
            try:
                ledger = read_json(path)
            except (WorkflowError, OSError, ValueError):
                continue
            if ledger.get("status") != "retired" and ledger.get("destination_remote") == remote:
                names.append(binding.name)
                break
    return names


def remove_remote(
    name: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> dict[str, object]:
    """Remove one remote bundle, refusing while a transfer still needs it.

    A sealed bundle that has not been acknowledged is work this remote still
    owes an answer about, and the adapter is how that answer is fetched.
    Removing the remote would leave the transfer with no way home, so it is
    refused by name — retire or fetch the transfer first.

    :param name: Remote bundle name.
    :param project: Project directory used for project-local lookup.
    :return: JSON-compatible removal result.
    :raises ValueError: If the remote is invalid, unknown, or still has transfers.
    """

    bundle, scope = _remote_bundle(name, project=project)
    pending = _pending_remote_transfers(name)
    if pending:
        raise ValueError(
            f"remote {name!r} still has unretired transfers from workspace {', '.join(pending)}; "
            "fetch or retire them first"
        )
    shutil.rmtree(bundle)
    _LOGGER.info(
        "removed the %s remote %s at %s",
        scope,
        name,
        bundle,
        extra={"event": "remote_removed", "remote": name, "bundle": str(bundle)},
    )
    return {"name": name, "scope": scope, "bundle": str(bundle), "removed": True}


@dataclass
class Finding:
    """Describe one thing the check looked at and what it found.

    :param check: Check name.
    :param status: Check result status.
    :param message: Human-readable result.
    :param repairable: Whether the finding can be repaired automatically.
    :param repaired: Whether this run repaired the finding.
    :param action: Repair action, when one was taken.
    :param details: Structured result details.
    """

    check: str
    status: str
    message: str
    repairable: bool = False
    repaired: bool = False
    action: str | None = None
    details: dict[str, object] = field(default_factory=dict)

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this finding.

        :return: JSON-compatible finding members.
        """

        return {
            "check": self.check,
            "status": self.status,
            "message": self.message,
            "repairable": self.repairable,
            "repaired": self.repaired,
            "action": self.action,
            "details": dict(self.details),
        }


def _check_maintenance_lock(project: Path, repair: bool) -> Finding:
    """A stale maintenance lock fences every manager for nothing."""

    workspace = _workspace_of(project)
    if workspace is None:
        return Finding("maintenance_lock", "ok", "there is no workspace to hold a maintenance lock")
    holder = read_maintenance_lock(workspace)
    if holder is None:
        return Finding("maintenance_lock", "ok", "no maintenance lock is held")
    if not holder.is_stale():
        return Finding(
            "maintenance_lock",
            "warning",
            f"a live maintenance lock is held by {holder.describe()}",
            details={"path": str(holder.path)},
        )
    finding = Finding(
        "maintenance_lock",
        "error",
        f"a stale maintenance lock is held by {holder.describe()}",
        repairable=True,
        details={"path": str(holder.path)},
    )
    if repair:
        finding.action = release_maintenance_lock(Workspace(project))
        finding.repaired = True
        finding.status = "ok"
    return finding


def _check_workspace_default(project: Path) -> Finding | None:
    """Report the recorded workspace name without creating a fallback."""

    default = _workspace_summary(project, read_project(project))["default"]
    if not isinstance(default, Mapping) or not isinstance(default.get("name"), str):
        return None
    name = str(default["name"])
    if default.get("resolves"):
        return Finding("workspace_default", "ok", f"recorded default workspace {name!r} resolves")
    return Finding(
        "workspace_default",
        "error",
        f"recorded default workspace {name!r} does not resolve",
        details={"name": name, "resolves": False},
    )


def _check_tmp_leftovers(project: Path, repair: bool) -> Finding:
    """Staging entries nothing renamed out are pure leftovers."""

    tmp = project / WORKSPACE_DIRECTORY / "tmp"
    if not tmp.is_dir():
        return Finding("tmp_leftovers", "ok", "there is no workspace staging directory")
    deadline = time.time() - TMP_MAXIMUM_AGE_SECONDS
    try:
        stale = [entry for entry in sorted(tmp.iterdir()) if entry.lstat().st_mtime < deadline]
    except PermissionError as exc:
        return Finding(
            "tmp_leftovers",
            "warning",
            f"cannot inspect workspace staging directory {tmp}: {exc}",
            details={"path": str(tmp), "error": str(exc)},
        )
    if not stale:
        return Finding("tmp_leftovers", "ok", "the workspace staging directory holds nothing abandoned")
    finding = Finding(
        "tmp_leftovers",
        "warning",
        f"{len(stale)} abandoned staging entr{'y' if len(stale) == 1 else 'ies'} below {tmp}",
        repairable=True,
        details={"entries": [entry.name for entry in stale]},
    )
    if repair:
        try:
            for entry in stale:
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry)
                else:
                    entry.unlink(missing_ok=True)
        except PermissionError as exc:
            finding.message = f"cannot remove abandoned staging entries below {tmp}: {exc}"
            finding.details["error"] = str(exc)
            return finding
        finding.action = f"removed {len(stale)} staging entries"
        finding.repaired = True
        finding.status = "ok"
    return finding
