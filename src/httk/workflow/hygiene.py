"""Describing and repairing a project, its remotes, and its workspace.

Everything here answers an operator question about state that already exists:
*what is this project*, *what is this remote configured to do*, *what is wrong
with this project and can it be fixed*. Nothing here is on the execution path of
a job, and every repair is explicit — :func:`project_doctor` reports by default
and only changes something when it is asked to.
"""

import logging
import os
import shutil
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from httk.project.cli import ProjectShowSection

from ._util import read_json, utc_now, write_json_atomic
from .adapters import (
    ADAPTER_EXECUTABLE,
    CREDENTIALS_FILE,
    metadata_path,
    project_remote_roots,
    read_credentials,
    read_metadata,
    validate_adapter_bundle,
)
from .configuration import keys_home, remotes_home
from .errors import WorkflowError
from .manifests import (
    read_maintenance_lock,
    release_maintenance_lock,
    verify_manifest,
)
from .models import STATE_KINDS
from .projects import (
    PROJECT_DIRECTORY,
    PROJECT_FILE,
    discover_project,
    key_fingerprint,
    pin_project_key,
    pinned_project_key,
    read_project,
    read_public_key_file,
    require_project,
    trusted_project_keys,
)
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "PROJECT_DESCRIPTION_FORMAT",
    "REMOTE_DESCRIPTION_FORMAT",
    "DOCTOR_REPORT_FORMAT",
    "DOCTOR_FRAME_FORMAT",
    "TMP_MAXIMUM_AGE_SECONDS",
    "describe_project",
    "workflow_show_section",
    "describe_remote",
    "remove_remote",
    "Finding",
    "project_doctor",
]

PROJECT_DESCRIPTION_FORMAT = "httk-project-description"
REMOTE_DESCRIPTION_FORMAT = "httk-remote-description"
DOCTOR_REPORT_FORMAT = "httk-project-doctor"
#: The journal frame one repairing doctor run appends. It is not a state frame,
#: so every reader that walks the journal for job history ignores it.
DOCTOR_FRAME_FORMAT = "httk-workflow-doctor"

#: How long a staging entry may sit below ``.httk-workflow/tmp`` before the
#: doctor calls it a leftover. Every publication renames its staging entry out
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
        "present": (project / ".httk-workflow" / "format.json").is_file(),
        "initialization_failed": metadata.get("workspace_initialization_failed") is True,
    }
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


def _manifest_summary(project: Path, *, verify: bool) -> dict[str, object]:
    """Report which manifest a project has and, on request, what it verifies as."""

    path = project / PROJECT_DIRECTORY / "manifest.jsonl.bz2"
    legacy = project / "ht.project" / "manifest.bz2"
    present = path.is_file() or legacy.is_file()
    summary: dict[str, object] = {
        "path": str(path if path.is_file() else legacy if legacy.is_file() else path),
        "present": present,
        "verdict": None,
        "reason": None if present else "this project has no manifest yet",
    }
    if present:
        summary["modified_at"] = int(Path(str(summary["path"])).stat().st_mtime)
    if not present or not verify:
        return summary
    try:
        verification = verify_manifest(project)
    except (WorkflowError, OSError, ValueError) as exc:
        summary["reason"] = f"the manifest could not be verified: {exc}"
        return summary
    summary.update({"verdict": verification.verdict, "reason": verification.reason})
    return summary


def describe_project(
    project_root: str | os.PathLike[str] | None = None,
    *,
    verify: bool = True,
) -> dict[str, object]:
    """Describe one project: its metadata, its keys, its workspace, its manifest.

    *verify* walks the tree to classify the manifest, which is what makes the
    description say whether the project is *currently* described by what it
    signed; pass ``False`` for a cheap answer on a very large project.
    """

    project = require_project(project_root)
    metadata = read_project(project)
    own = pinned_project_key(metadata)
    trusted = trusted_project_keys(metadata)
    seed = project / PROJECT_DIRECTORY / "keys" / "project.seed"
    return {
        "format": PROJECT_DESCRIPTION_FORMAT,
        "format_version": 1,
        "root": str(project),
        "project": {
            "project_id": metadata.get("project_id"),
            "name": metadata.get("name"),
            "description": metadata.get("description"),
            "default_queue": metadata.get("default_queue"),
            "imported_from": metadata.get("imported_from"),
            "manifest_exclusions": metadata.get("manifest_exclusions", []),
        },
        "keys": {
            "pinned": own is not None,
            "public_key": None if own is None else _key_record(own),
            "seed_present": seed.is_file(),
            "trusted_keys": [_key_record(key) for key in trusted],
        },
        "workspace": _workspace_summary(project, metadata),
        "manifest": _manifest_summary(project, verify=verify),
        "remotes": _project_remotes(project),
    }


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


def workflow_show_section(project: Path, *, verify: bool) -> ProjectShowSection:
    """Contribute the workflow rows of ``httk project show``.

    This is the workflow half of what :func:`describe_project` reports — the
    workspace, the manifest, and the remotes — registered into the umbrella
    ``httk project`` command as a show section. The anchor half (metadata and
    keys) is rendered by *httk-core*; here we only add what a workflow
    installation knows about a project.
    """

    metadata = read_project(project)
    workspace = _workspace_summary(project, metadata)
    manifest = _manifest_summary(project, verify=verify)
    remotes = _project_remotes(project)
    rows: list[tuple[str, str]] = [
        ("workspace", str(workspace.get("workspace_id") or ("present" if workspace.get("present") else "-"))),
        ("jobs", str(workspace.get("jobs", 0))),
        ("manifest", f"{manifest.get('verdict') or 'none'}: {manifest.get('reason') or '-'}"),
        ("remotes", ", ".join(remotes) or "-"),
    ]
    lock = workspace.get("maintenance_lock")
    if isinstance(lock, dict):
        rows.append(("maintenance_lock", f"{lock.get('holder')} ({'stale' if lock.get('stale') else 'live'})"))
    return ProjectShowSection(
        json={"workspace": workspace, "manifest": manifest, "remotes": remotes},
        rows=rows,
    )


def _remote_bundle(name: str, *, project: str | os.PathLike[str] | None) -> tuple[Path, str]:
    """Locate one remote bundle directory, project-local before global.

    The bundle is located by name rather than resolved through the adapter
    contract, because describing or removing a remote must still work when the
    bundle is exactly what is broken about it.
    """

    if not name or "/" in name or name in {".", ".."}:
        raise ValueError(f"invalid remote name: {name!r}")
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

    Credential values never appear. A queue's settings are reported with the
    file each one came from, and for a setting stored in the manifest-excluded
    ``credentials.json`` only its *name* is reported: a description an operator
    can paste into a bug report must never carry a password.
    """

    bundle, scope = _remote_bundle(name, project=project)
    project_root = discover_project(project)
    default_queue = str(read_project(project_root).get("default_queue") or "") if project_root is not None else ""
    try:
        metadata: dict[str, Any] = dict(validate_adapter_bundle(bundle))
        valid, problem = True, None
    except (OSError, ValueError) as exc:
        recorded = metadata_path(bundle)
        metadata = read_json(recorded) if recorded.is_file() else {}
        valid, problem = False, str(exc)
    raw_queues = metadata.get("queues", {"default": {}})
    queues: dict[str, object] = {}
    for queue, settings in sorted(raw_queues.items()) if isinstance(raw_queues, Mapping) else ():
        persisted = dict(settings) if isinstance(settings, Mapping) else {}
        credentials = sorted(read_credentials(bundle, queue))
        queues[queue] = {
            "settings": persisted,
            # Every setting the adapter will see, with the file it comes from.
            # Only the names of the credential settings appear; their values
            # stay in credentials.json, which is also what manifests exclude.
            "settings_source": {
                **{key: CREDENTIALS_FILE for key in credentials},
                **{key: metadata_path(bundle).name for key in sorted(persisted)},
            },
            "credential_keys": credentials,
        }
    return {
        "format": REMOTE_DESCRIPTION_FORMAT,
        "format_version": 1,
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
        "queues": queues,
        "credentials_file": str(bundle / CREDENTIALS_FILE) if (bundle / CREDENTIALS_FILE).is_file() else None,
        "default_queue": default_queue or "default",
    }


def _configured_workspace_ids(bundle: Path) -> set[str]:
    """Return the workspace UUIDs this remote's queues point at, if readable."""

    try:
        metadata = read_metadata(bundle)
    except (WorkflowError, OSError, ValueError):
        return set()
    queues = metadata.get("queues", {})
    identifiers: set[str] = set()
    for settings in queues.values() if isinstance(queues, Mapping) else ():
        merged: dict[str, Any] = dict(settings) if isinstance(settings, Mapping) else {}
        location = merged.get("workspace")
        if not isinstance(location, str) or not location:
            continue
        format_path = Path(location).expanduser() / ".httk-workflow" / "format.json"
        if not format_path.is_file():
            continue
        try:
            identifiers.add(str(read_json(format_path).get("workspace_id")))
        except (WorkflowError, OSError, ValueError):
            continue
    return identifiers


def _pending_transfers(project: Path, workspace_ids: set[str]) -> list[dict[str, object]]:
    """Return every sealed, unretired transfer of *project* bound for *workspace_ids*."""

    directory = project / ".httk-workflow" / "transfers"
    pending: list[dict[str, object]] = []
    for path in sorted(directory.glob("*.json")) if directory.is_dir() else ():
        try:
            ledger = read_json(path)
        except (WorkflowError, OSError, ValueError):
            continue
        if ledger.get("status") == "retired":
            continue
        if workspace_ids and ledger.get("destination_workspace_id") not in workspace_ids:
            continue
        pending.append(
            {
                "transfer_id": ledger.get("transfer_id"),
                "job_key": ledger.get("job_key"),
                "status": ledger.get("status"),
                "ledger": str(path),
            }
        )
    return pending


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
    """

    bundle, scope = _remote_bundle(name, project=project)
    project_root = discover_project(project)
    if project_root is not None:
        pending = _pending_transfers(project_root, _configured_workspace_ids(bundle))
        if pending:
            jobs = ", ".join(str(item["job_key"]) for item in pending)
            raise ValueError(
                f"remote {name!r} is still referenced by {len(pending)} unretired transfer(s): {jobs}; "
                "fetch or retire them before removing the remote"
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
    """One thing the doctor looked at, and what it found."""

    check: str
    status: str
    message: str
    repairable: bool = False
    repaired: bool = False
    action: str | None = None
    details: dict[str, object] = field(default_factory=dict)

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this finding."""

        return {
            "check": self.check,
            "status": self.status,
            "message": self.message,
            "repairable": self.repairable,
            "repaired": self.repaired,
            "action": self.action,
            "details": dict(self.details),
        }


def _check_workspace_initialization(project: Path, metadata: dict[str, object], repair: bool) -> Finding:
    """A project whose workspace creation failed is left recognizable, not fixed."""

    if metadata.get("workspace_initialization_failed") is not True:
        return Finding("workspace_initialization", "ok", "the project workspace was initialized")
    finding = Finding(
        "workspace_initialization",
        "error",
        "project.json records workspace_initialization_failed: the project has no usable workspace",
        repairable=True,
    )
    if not repair:
        return finding
    if not (project / ".httk-workflow" / "format.json").is_file():
        Workspace.initialize(project, extensions=("detached-transfer-v1",))
        finding.action = "initialized a detached-transfer workspace"
    else:
        finding.action = "the workspace already exists; cleared the failure flag"
    del metadata["workspace_initialization_failed"]
    write_json_atomic(project / PROJECT_DIRECTORY / PROJECT_FILE, metadata)
    finding.repaired = True
    finding.status = "ok"
    return finding


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


def _check_key_pin(project: Path, metadata: dict[str, object], repair: bool) -> Finding:
    """Without a pin, manifest verification has no anchor but the manifest itself."""

    if pinned_project_key(metadata) is not None:
        return Finding("key_pin", "ok", "project.json pins the project's public key")
    finding = Finding(
        "key_pin",
        "warning",
        "project.json pins no public key, so every manifest verifies as an unknown key",
        repairable=(project / PROJECT_DIRECTORY / "keys" / "project.pub").is_file(),
    )
    if repair and finding.repairable:
        pinned = pin_project_key(project)
        finding.action = f"pinned {key_fingerprint(str(pinned['public_key']))}"
        finding.repaired = True
        finding.status = "ok"
        metadata.update(pinned)
    return finding


def _check_tmp_leftovers(project: Path, repair: bool) -> Finding:
    """Staging entries nothing renamed out are pure leftovers."""

    tmp = project / ".httk-workflow" / "tmp"
    if not tmp.is_dir():
        return Finding("tmp_leftovers", "ok", "there is no workspace staging directory")
    deadline = time.time() - TMP_MAXIMUM_AGE_SECONDS
    stale = [entry for entry in sorted(tmp.iterdir()) if entry.lstat().st_mtime < deadline]
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
        for entry in stale:
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry)
            else:
                entry.unlink(missing_ok=True)
        finding.action = f"removed {len(stale)} staging entries"
        finding.repaired = True
        finding.status = "ok"
    return finding


def _check_legacy_identity(project: Path, metadata: dict[str, object]) -> Finding:
    """An imported legacy identity that nothing trusts verifies nothing."""

    trusted = set(trusted_project_keys(metadata))
    legacy_dir = project / PROJECT_DIRECTORY / "keys" / "legacy-public"
    unpinned: list[str] = []
    for path in sorted(legacy_dir.glob("*.pub")) if legacy_dir.is_dir() else ():
        try:
            recorded = read_public_key_file(path)
        except ValueError:
            unpinned.append(path.name)
            continue
        if recorded not in trusted:
            unpinned.append(path.name)
    user_legacy = keys_home() / "legacy-identity.pub"
    if user_legacy.is_file():
        try:
            if read_public_key_file(user_legacy) not in trusted:
                unpinned.append(str(user_legacy))
        except ValueError:
            unpinned.append(str(user_legacy))
    if not unpinned:
        return Finding("legacy_identity", "ok", "every imported legacy identity is pinned or absent")
    return Finding(
        "legacy_identity",
        "warning",
        f"{len(unpinned)} imported legacy public key(s) are present but not trusted by this project; "
        "adopt one deliberately with trust_project_key() if its old manifests must verify",
        details={"keys": unpinned},
    )


def _check_manifest(project: Path) -> Finding:
    """Manifest staleness is reported and never repaired behind an operator."""

    summary = _manifest_summary(project, verify=True)
    if not summary["present"]:
        return Finding("manifest", "warning", "this project has no manifest", details=dict(summary))
    verdict = str(summary["verdict"] or "unknown")
    status = {"valid_trusted": "ok", "valid_unknown_key": "warning"}.get(verdict, "error")
    return Finding("manifest", status, f"{verdict}: {summary['reason']}", details=dict(summary))


def project_doctor(
    project_root: str | os.PathLike[str] | None = None,
    *,
    repair: bool = False,
) -> dict[str, object]:
    """Check one project for the conditions that quietly break it later.

    Every check reports; a check that can be fixed automatically is fixed only
    when *repair* is asked for, and a run that repaired anything says exactly
    what it did — in the log and, when the project has a workspace, in that
    workspace's journal, so the repair is part of its durable history.
    """

    project = require_project(project_root)
    metadata = read_project(project)
    findings: list[Finding] = [
        _check_workspace_initialization(project, metadata, repair),
        _check_maintenance_lock(project, repair),
        _check_key_pin(project, metadata, repair),
        _check_tmp_leftovers(project, repair),
        _check_legacy_identity(project, metadata),
        _check_manifest(project),
    ]
    report: dict[str, object] = {
        "format": DOCTOR_REPORT_FORMAT,
        "format_version": 1,
        "root": str(project),
        "project_id": metadata.get("project_id"),
        "checked_at": utc_now(),
        "repair": repair,
        "findings": [finding.as_mapping() for finding in findings],
        "problems": sum(finding.status != "ok" for finding in findings),
        "repaired": sum(finding.repaired for finding in findings),
    }
    _journal_repairs(project, report, findings)
    return report


def _journal_repairs(project: Path, report: Mapping[str, object], findings: Sequence[Finding]) -> None:
    """Record what a repairing run changed, in the log and in the journal.

    A run that changed nothing writes nothing: opening a journal writer creates
    a writer directory, and a read-only check that left one behind would be
    making work for the collector it came to report on.
    """

    repairs = [finding for finding in findings if finding.repaired]
    if not repairs:
        return
    for finding in repairs:
        _LOGGER.warning(
            "project doctor repaired %s in %s: %s",
            finding.check,
            project,
            finding.action,
            extra={"event": "project_doctor_repair", "check": finding.check, "project": str(project)},
        )
    workspace = _workspace_of(project)
    if workspace is None:
        return
    frame = {
        "format": DOCTOR_FRAME_FORMAT,
        "format_version": 1,
        "workspace_id": workspace.workspace_id,
        "project_id": report.get("project_id"),
        "checked_at": report.get("checked_at"),
        "repairs": [
            {"check": finding.check, "action": finding.action, "message": finding.message} for finding in repairs
        ],
    }
    try:
        with Workspace(project).open_journal_writer() as writer:
            writer.append(frame)
    except (WorkflowError, OSError, ValueError) as exc:  # pragma: no cover - a broken journal is its own finding
        _LOGGER.warning("cannot journal the doctor repairs of %s: %s", project, exc)
