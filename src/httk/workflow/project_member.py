"""The workflow-workspace project-member handler.

Core owns the project verbs — seal, manifest, doctor, verify — and delegates a
member's internals to the handler its kind registers (see
:mod:`httk.core.project.members`). A *workflow workspace* is one member kind;
this module is what core calls to seal it, exclude its scratch from a project
manifest, verify it, and check its health, all through
:func:`handler`.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Sequence
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from .seals import SealVerification

__all__ = ["handler"]


def handler() -> WorkspaceMemberHandler:
    """Return the workspace project-member handler.

    Core resolves the registered ``"module:callable"`` reference and calls it
    with no arguments to build the handler, so importing this module is deferred
    until a workspace member is actually acted on.

    :return: The workspace member handler.
    """

    return WorkspaceMemberHandler()


def _entry(level: str, subject: str, verification: SealVerification) -> dict[str, object]:
    """Render one workflow seal verdict as a core whole-tree report entry."""

    return {
        "level": level,
        "subject": subject,
        "valid": verification.valid,
        "verdict": verification.verdict,
        "reason": verification.reason,
        "signers": list(verification.signers),
        "missing_signers": list(verification.missing_signers),
        "discrepancies": [{"kind": item.kind, "path": item.path} for item in verification.discrepancies],
    }


class WorkspaceMemberHandler:
    """Implement :class:`httk.core.project.members.ProjectMemberHandler` for a workspace."""

    def manifest_exclusions(self, project_root: Path, member_relpath: str) -> tuple[str, ...]:
        """Return this workspace's control-dir, payload, and postprocess exclusions.

        The patterns are posix relpaths below the *project* root. The control
        directory and every job payload are left out of a project manifest and
        seal — a payload is covered through the workspace seal chain rather than
        re-hashed loose — as is the workspace's postprocess output tree.

        :param project_root: The project root the patterns are relative to.
        :param member_relpath: This workspace's relpath below the project root.
        :return: The exclusion patterns.
        """

        from .models import WORKSPACE_DIRECTORY
        from .workspace import Workspace

        root = Path(project_root)
        prefix = "" if member_relpath in {".", ""} else member_relpath.rstrip("/") + "/"
        patterns: list[str] = [f"{prefix}{WORKSPACE_DIRECTORY}", f"{prefix}{WORKSPACE_DIRECTORY}/**"]
        ws_root = root if prefix == "" else root / member_relpath
        try:
            workspace = Workspace(ws_root)
        except Exception:
            return tuple(patterns)
        for marker in sorted(workspace.scan_markers(), key=lambda item: item.job_key):
            payload = workspace.payload_path(marker.placement, marker.job_key)
            try:
                rel = payload.relative_to(root).as_posix()
            except ValueError:
                continue
            patterns.extend((rel, f"{rel}/**"))
        from .postprocessing import postprocess_root

        try:
            output_root = postprocess_root(workspace)
        except (OSError, ValueError):
            output_root = None
        if output_root is not None and output_root.is_relative_to(root):
            rel = output_root.relative_to(root).as_posix()
            patterns.extend((rel, f"{rel}/**"))
        return tuple(patterns)

    def seal(self, member_root: Path, keys: object) -> Path:
        """Seal this workspace and return its seal path.

        :param member_root: This workspace's root directory.
        :param keys: The resolved signing keys, or ``None`` for the default.
        :return: The written workspace seal path.
        """

        from . import seals
        from .workspace import Workspace

        return seals.seal_workspace(Workspace(member_root), keys=cast(Any, keys) or None)

    def unseal(self, member_root: Path) -> None:
        """Remove this workspace's seal.

        :param member_root: This workspace's root directory.
        """

        from . import seals
        from .workspace import Workspace

        seals.unseal_workspace(Workspace(member_root))

    def seal_digest(self, member_root: Path) -> tuple[str, str]:
        """Return this workspace's id and the SHA-256 of its seal bytes.

        :param member_root: This workspace's root directory.
        :return: The workspace id and the hex SHA-256 of its seal file.
        :raises httk.core.project.sealing.SealError: If the workspace is unsealed.
        """

        from httk.core.project.sealing import SealError

        from . import seals
        from .workspace import Workspace

        workspace = Workspace(member_root)
        path = seals.workspace_seal_path(workspace)
        if not path.is_file():
            raise SealError(f"workspace {workspace.workspace_id} is not sealed")
        return workspace.workspace_id, hashlib.sha256(path.read_bytes()).hexdigest()

    def verify(
        self,
        member_root: Path,
        *,
        trusted_keys: Sequence[str],
        deep: bool,
    ) -> tuple[dict[str, object], ...]:
        """Verify this workspace and, when deep, every job it seals.

        :param member_root: This workspace's root directory.
        :param trusted_keys: Trust anchors to classify the signers against.
        :param deep: Whether to verify every referenced job seal.
        :return: The workspace entry followed by any job entries.
        """

        from . import seals
        from .workspace import Workspace

        workspace = Workspace(member_root)
        verification = seals.verify_workspace_seal(workspace, trusted_keys=trusted_keys)
        entries: list[dict[str, object]] = [_entry("workspace", workspace.workspace_id, verification)]
        if not deep or not seals.is_workspace_sealed(workspace):
            return tuple(entries)
        from pathlib import PurePosixPath

        seal = seals.read_seal(seals.workspace_seal_path(workspace))
        for record in seal.records:
            job_key = str(record["job_key"])
            placement = PurePosixPath(str(record["placement"]))
            job = seals._verify_job(workspace, job_key, placement, trusted_keys=trusted_keys, expected_roles=())
            entries.append(_entry("job", job_key, job))
        return tuple(entries)

    def doctor(self, member_root: Path, *, repair: bool) -> tuple[dict[str, object], ...]:
        """Check this workspace's health and, when a member is the project root,
        that no sibling workspace is unregistered.

        :param member_root: This workspace's root directory.
        :param repair: Whether to apply automatic repairs.
        :return: The workspace's doctor findings.
        """

        from .hygiene import _check_maintenance_lock, _check_tmp_leftovers, _check_workspace_default
        from .projects import discover_project

        findings = [
            _check_maintenance_lock(member_root, repair).as_mapping(),
            _check_tmp_leftovers(member_root, repair).as_mapping(),
        ]
        # The recorded default-workspace binding is a project-registry concern, so
        # only the member that is itself the project root reports on it.
        project = discover_project(member_root)
        if project is not None and project.resolve() == member_root.resolve():
            default = _check_workspace_default(member_root)
            if default is not None:
                findings.append(default.as_mapping())
        unregistered = _unregistered_workspaces(member_root, repair)
        if unregistered is not None:
            findings.append(unregistered)
        return tuple(findings)

    def describe(self, member_root: Path) -> dict[str, object]:
        """Describe this workspace for an operator diagnostic.

        :param member_root: This workspace's root directory.
        :return: A JSON-compatible workspace description.
        """

        from . import seals
        from .workspace import Workspace

        workspace = Workspace(member_root)
        markers = list(workspace.scan_markers())
        return {
            "kind": "workspace",
            "workspace_id": workspace.workspace_id,
            "root": str(workspace.root),
            "sealed": seals.is_workspace_sealed(workspace),
            "jobs": len(markers),
            "unsealed_jobs": [marker.job_key for marker in seals.unsealed_jobs(workspace)],
        }

    def guard(self, member_root: Path) -> AbstractContextManager[object]:
        """Fence this workspace against maintenance while it is snapshotted.

        :param member_root: This workspace's root directory.
        :return: The workspace maintenance guard.
        """

        from .manifests import workspace_maintenance_guard
        from .workspace import Workspace

        return workspace_maintenance_guard(Workspace(member_root))


def _unregistered_workspaces(member_root: Path, repair: bool) -> dict[str, object] | None:
    """Report sibling workspaces under the project that are not registered members.

    Only a member that is itself the project root scans the tree, so a project
    whose root is not a workspace reaches this through its lone nested member and
    a project whose root *is* a workspace never double-reports.
    """

    from httk.core.project.members import project_members, register_project_member

    from .hygiene import Finding
    from .models import WORKSPACE_DIRECTORY
    from .projects import discover_project

    project = discover_project(member_root)
    if project is None or project.resolve() == member_root.resolve():
        return None
    registered = {member.path for member in project_members(project)}
    found: list[str] = []
    for dirpath, dirnames, _filenames in os.walk(project):
        directory = Path(dirpath)
        if (directory / WORKSPACE_DIRECTORY / "format.json").is_file():
            relpath = directory.relative_to(project).as_posix()
            if relpath not in registered:
                found.append(relpath)
            dirnames[:] = []
    if not found:
        return Finding(
            "workspace_members", "ok", "every workspace under the project is a registered member"
        ).as_mapping()
    finding = Finding(
        "workspace_members",
        "error",
        f"{len(found)} workspace(s) under the project are not registered members: {', '.join(found)}",
        repairable=True,
        details={"workspaces": found},
    )
    if repair:
        for relpath in found:
            register_project_member(project, project if relpath == "." else project / relpath, "workspace")
        finding.action = f"registered {len(found)} workspace(s)"
        finding.repaired = True
        finding.status = "ok"
    return finding.as_mapping()
