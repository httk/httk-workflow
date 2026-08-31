"""The workflow-workspace project-member handler.

Core owns the project verbs — seal, manifest, repair, verify — and delegates a
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
from typing import TYPE_CHECKING

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

    def repair(self, member_root: Path, *, apply: bool) -> tuple[dict[str, object], ...]:
        """Repair this workspace member's own health, or report only.

        Project-scope checks — the recorded default binding and any workspace on
        disk that is not a registered member — live in :meth:`scan_project`.

        :param member_root: This workspace's root directory.
        :param apply: Whether to apply repairs; ``False`` reports only.
        :return: The workspace's repair findings.
        """

        from .hygiene import _check_maintenance_lock, _check_tmp_leftovers

        return (
            _check_maintenance_lock(member_root, apply).as_mapping(),
            _check_tmp_leftovers(member_root, apply).as_mapping(),
        )

    def scan_project(self, project_root: Path, *, apply: bool, adopt: bool) -> tuple[dict[str, object], ...]:
        """Report project-scope workspace findings, even with no registered member.

        Core calls this once for the workspace kind whether or not members.json
        holds any workspace. Every mutation this scan makes — re-registering a
        workspace as a member and linking its name into this machine's registry —
        is part of adoption, so it mutates only when both *apply* and *adopt* are
        set; otherwise it reports and touches nothing.

        :param project_root: The project root to scan.
        :param apply: Whether to apply repairs; ``False`` reports only.
        :param adopt: Whether to adopt members on this machine.
        :return: The project-scope findings.
        """

        from .hygiene import _check_workspace_default

        mutate = apply and adopt
        findings: list[dict[str, object]] = [_unregistered_workspaces(project_root, mutate)]
        default = _check_workspace_default(project_root)
        if default is not None:
            if mutate and default.status == "error":
                findings.extend(_adopt_default_workspace(project_root))
                default = _check_workspace_default(project_root)
            if default is not None:
                findings.append(default.as_mapping())
        return tuple(findings)

    def adopt(self, member_root: Path, *, name: str | None) -> tuple[dict[str, object], ...]:
        """Re-establish this workspace's local links on this machine.

        :param member_root: This workspace's root directory.
        :param name: The member's recorded name, or ``None`` when it has none.
        :return: The adoption findings.
        """

        from .registry import adopt_workspace

        return adopt_workspace(member_root, name=name)

    def guard(self, member_root: Path) -> AbstractContextManager[object]:
        """Fence this workspace against maintenance while it is snapshotted.

        :param member_root: This workspace's root directory.
        :return: The workspace maintenance guard.
        """

        from .manifests import workspace_maintenance_guard
        from .workspace import Workspace

        return workspace_maintenance_guard(Workspace(member_root))


def _unregistered_workspaces(project_root: Path, adopt: bool) -> dict[str, object]:
    """Report every workspace on disk under a project that is not a member.

    The walk prunes at each workspace, so a workspace nested inside another is
    covered by that outer workspace rather than reported separately.
    """

    from httk.core.project.members import project_members

    from .hygiene import Finding
    from .models import WORKSPACE_DIRECTORY
    from .registry import _read_global, adopt_workspace

    project = Path(project_root)
    members = list(project_members(project))
    registered = {member.path for member in members}
    on_disk: list[str] = []
    for dirpath, dirnames, _filenames in os.walk(project):
        directory = Path(dirpath)
        if (directory / WORKSPACE_DIRECTORY / "format.json").is_file():
            on_disk.append(directory.relative_to(project).as_posix())
            dirnames[:] = []

    if adopt:
        # Idempotent: adopt every workspace under the project, whether or not it is
        # already a member or already centrally registered. This fully wires a
        # cleanly-copied tree in one pass.
        for relpath in on_disk:
            adopt_workspace(project if relpath == "." else project / relpath)
        return Finding(
            "workspace_members",
            "ok",
            f"adopted {len(on_disk)} workspace(s) under the project",
            details={"workspaces": on_disk},
        ).as_mapping()

    # Detection only: a workspace missing from members.json, or a member whose
    # recorded name is not registered centrally on this machine (a freshly copied
    # tree), both need `httk workspace adopt` / `httk project adopt --repair`.
    central = {str(Path(record["path"]).resolve()) for record in _read_global().values()}
    unregistered = [relpath for relpath in on_disk if relpath not in registered]
    # Only a member with a recorded *name* is adoptable by name; an unnamed local
    # init has nothing to adopt and is left alone.
    unadopted = [
        member.path
        for member in members
        if member.name is not None and str((project / member.path).resolve()) not in central
    ]
    problems = sorted(set(unregistered) | set(unadopted))
    if not problems:
        return Finding(
            "workspace_members", "ok", "every workspace under the project is a registered, adopted member"
        ).as_mapping()
    return Finding(
        "workspace_members",
        "error",
        f"{len(problems)} workspace(s) are not registered members or not adopted on this machine "
        f"({', '.join(problems)}); run `httk project adopt`",
        repairable=True,
        details={"workspaces": problems},
    ).as_mapping()


def _adopt_default_workspace(project_root: Path) -> list[dict[str, object]]:
    """Adopt the member that carries the project's recorded default-workspace name."""

    from httk.core.project.members import project_members

    from .projects import read_project_section
    from .registry import adopt_workspace

    default = read_project_section(project_root, "workspace").get("default")
    if not isinstance(default, str) or not default:
        return []
    for member in project_members(project_root):
        if member.kind == "workspace" and member.name == default:
            root = project_root if member.path == "." else project_root / member.path
            return list(adopt_workspace(root, name=default))
    return []
