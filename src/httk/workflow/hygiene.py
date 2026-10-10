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

from . import _death, _kernel, gc
from ._state import UNOWNED_STATES
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
from .models import WORKSPACE_DIRECTORY
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
    "Finding",
    "describe_remote",
    "remove_remote",
]

REMOTE_DESCRIPTION_FORMAT = "httk-remote-description"


def _key_record(value: str) -> dict[str, object]:
    return {"public_key": value, "fingerprint": key_fingerprint(value)}


def _workspace_root(project: Path) -> Path | None:
    """Return the root of the project's recorded default local workspace, if it resolves."""

    recorded = read_project_section(project, "workspace").get("default")
    if not isinstance(recorded, str):
        return None
    try:
        binding = resolve_workspace(recorded, project=project)
    except (OSError, ValueError, WorkflowError):
        return None
    if binding.remote != "local" or binding.path is None:
        return None
    root = Path(binding.path)
    return root if (root / WORKSPACE_DIRECTORY / "format.json").is_file() else None


def _workspace_at(root: Path | None) -> Workspace | None:
    """Attach the workspace at *root* read-only, or report that there is none."""

    if root is None:
        return None
    try:
        return Workspace(root, mutable=False)
    except (WorkflowError, OSError, ValueError):
        return None


def _workspace_summary(project: Path, metadata: Mapping[str, object]) -> dict[str, object]:
    """Summarize the project workspace without mutating anything in it."""

    summary: dict[str, object] = {
        "present": _workspace_root(project) is not None,
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
    workspace = _workspace_at(_workspace_root(project))
    if workspace is None:
        return summary
    counts = {state: sum(1 for _ref in _kernel.list_jobs(workspace, state)) for state in UNOWNED_STATES}
    owned = workspace.jobs / _kernel.OWNED
    counts[_kernel.OWNED] = sum(len(_names(owned / owner_id)) for owner_id in _names(owned))
    summary.update(
        {
            "workspace_id": workspace.workspace_id,
            "core_profile": workspace.core_profile,
            "extensions": sorted(workspace.extensions),
            "counts": counts,
            "jobs": sum(counts.values()),
            "owners": len(_kernel.list_owners(workspace)),
        }
    )
    return summary


def _names(directory: Path) -> list[str]:
    try:
        return sorted(os.listdir(directory))
    except (FileNotFoundError, NotADirectoryError):
        return []


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
    """Return registered local workspace names with unacknowledged transfers to *remote*.

    A transfer is pending while its root job is ``transferring`` under an
    addressed transaction naming the remote.
    """

    from ._sealing import pending_outgoing

    names: list[str] = []
    for binding in list_workspaces():
        assert binding.path is not None
        try:
            workspace = Workspace(binding.path, mutable=False)
            pending = pending_outgoing(workspace)
        except (WorkflowError, OSError, ValueError):
            continue
        if any(txn.destination_remote == remote for _marker, _frame, txn in pending):
            names.append(binding.name)
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


def _check_owners(workspace_root: Path, repair: bool) -> Finding:
    """Owners proven dead but not yet recovered (their jobs stay owned), and scratch nobody owns.

    The repair is the ``dead_owners`` category of ``httk workspace gc``.
    """

    workspace = _workspace_at(workspace_root)
    if workspace is None:
        return Finding("owners", "ok", "there is no workspace to hold owners")
    scheduler, here = _death.SchedulerQueries(), _death.process_identity()
    records = _kernel.list_owners(workspace)
    dead = [
        record.owner_id
        for record in records
        if record.record is not None
        and _death.probe(
            record.path, visibility_deadline=workspace.visibility_deadline, scheduler=scheduler, here=here
        )[0]
        is _death.Liveness.DEAD
    ]
    known = {record.owner_id for record in records}
    # tmp/<owner-id>.<purpose>.<token>: recovery takes a dead owner's scratch; an unknown owner's is left over.
    orphaned = [name for name in _names(workspace.control / "tmp") if name.split(".")[0] not in known | {"trash"}]
    details: dict[str, object] = {"dead_owners": dead, "orphaned_scratch": orphaned}
    if not dead and not orphaned:
        return Finding("owners", "ok", "every owner is alive or recovered", details=details)
    finding = Finding(
        "owners",
        "warning",
        f"{len(dead)} dead owner(s) not yet recovered, {len(orphaned)} scratch entr(ies) of unknown owners",
        repairable=bool(dead),
        details=details,
    )
    if repair and dead:
        report = gc.collect_garbage(Workspace(workspace_root), categories=("dead_owners",), sizes=False)
        finding.action = f"recovered {report.removed} dead owner(s)"
        finding.repaired = report.removed == len(dead)
        finding.status = "ok" if finding.repaired and not orphaned else "warning"
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


def _check_transfers(workspace_root: Path) -> Finding:
    """Report transfer work that waits for an operator (plan 4.7); this check never repairs anything.

    The legacy transfer machinery it inspects is replaced in phase D (held bundles of eject + copy + adopt).

    It names exports held for a copy-out (``httk job eject --resume``), exports
    whose interrupted copy-out may already have delivered them (in doubt: remove
    the held copy, or take it back with ``httk job adopt``), addressed bundles
    still unacknowledged past their freshness window (in doubt: retire them if
    the destination has the job, reclaim them if not), and per-job adoption
    claims whose lineage directory is gone.
    """

    from ._adoption import stale_claims
    from ._receipts import FRESHNESS_WINDOW_NS
    from ._sealing import exports_in_doubt, held_exports, pending_outgoing

    try:
        workspace = Workspace(workspace_root, mutable=False)
    except (WorkflowError, OSError) as exc:
        return Finding("transfers", "ok", f"there is no readable workspace to check transfers in: {exc}")
    doubtful = exports_in_doubt(workspace)
    held = held_exports(workspace)
    now = time.time_ns()
    in_doubt = [
        {"transfer_id": txn.transfer_id, "job_key": marker.job_key, "sealed_at": txn.sealed_at}
        for marker, _frame, txn in pending_outgoing(workspace)
        if now > txn.sealed_at + FRESHNESS_WINDOW_NS
    ]
    orphaned = stale_claims(workspace)
    details: dict[str, object] = {
        "held_exports": held,
        "exports_in_doubt": doubtful,
        "outgoing_in_doubt": in_doubt,
        "stale_claims": orphaned,
    }
    if not (held or doubtful or in_doubt or orphaned):
        return Finding("transfers", "ok", "no transfer waits for an operator", details=details)
    parts = []
    if held:
        parts.append(f"{len(held)} export(s) held for copy-out (`httk job eject --resume`)")
    if doubtful:
        parts.append(
            f"{len(doubtful)} export(s) in doubt, perhaps already delivered "
            "(remove the held copy, or take it back with `httk job adopt HELD`)"
        )
    if in_doubt:
        parts.append(
            f"{len(in_doubt)} outgoing transfer(s) unacknowledged past their window "
            "(`httk workflow transfer retire|reclaim JOB_ID`)"
        )
    if orphaned:
        parts.append(f"{len(orphaned)} adoption claim(s) without their lineage")
    return Finding("transfers", "warning", "; ".join(parts), details=details)


def _check_tmp_leftovers(workspace_root: Path, repair: bool) -> Finding:
    """Write temporaries and ``tmp/trash.<token>`` entries older than a day: the ``tmp_entries`` gc category."""

    workspace = _workspace_at(workspace_root)
    if workspace is None:
        return Finding("tmp_leftovers", "ok", "there is no workspace to hold leftovers")
    found = gc.collect_garbage(workspace, dry_run=True, categories=("tmp_entries",), sizes=False)
    entries = list(found.category("tmp_entries").entries)
    if not entries:
        return Finding("tmp_leftovers", "ok", "the workspace holds no abandoned temporaries")
    finding = Finding(
        "tmp_leftovers",
        "warning",
        f"{len(entries)} abandoned temporar{'y' if len(entries) == 1 else 'ies'}",
        repairable=True,
        details={"entries": entries},
    )
    if repair:
        report = gc.collect_garbage(Workspace(workspace_root), categories=("tmp_entries",), sizes=False)
        finding.action = f"removed {report.removed} temporaries"
        finding.repaired = True
        finding.status = "ok"
    return finding
