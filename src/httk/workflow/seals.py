"""Write, read, sign, and verify seal documents for jobs, workspaces, and projects.

A *seal* is a signed statement about what a level of the workflow tree contained
at one moment. A job seal records the files of one payload; a workspace seal
records, for every job, the digest of that job's seal; a project seal records the
project's loose files and, for every workspace nested below it, the digest of
that workspace's seal. Each level therefore binds the level below it, so a
project seal transitively pins whole payloads without re-hashing them, and a
change to any covered byte becomes a discrepancy the moment the seal is verified.

The signature is over a domain-separated digest of the document body, exactly as
:mod:`httk.workflow.manifests` signs a manifest, so a digest signed as a seal can
never be replayed as anything else. Verification answers the two independent
questions a signature always raises separately — does the seal still describe
this tree, and was it made by a key this project trusts — and reports both.
"""

import hashlib
import logging
import os
from collections.abc import Iterable, Sequence
from dataclasses import replace
from pathlib import Path, PurePosixPath

from httk.core.project.sealing import (
    INVALID,
    VALID_TRUSTED,
    VALID_UNKNOWN_KEY,
    Discrepancy,
    Seal,
    SealKey,
    SealKeys,
    SealReport,
    SealVerification,
    build_seal_body,
    default_project_keys,
    diff_records,
    read_seal,
    resolve_seal_keys,
    verify_seal,
    write_seal,
)

from .errors import FormatError, SealedError, SealError
from .manifests import payload_file_records
from .models import WORKSPACE_DIRECTORY, Marker
from .projects import PROJECT_DIRECTORY, discover_project
from .workspace import Workspace

__all__ = [
    "INVALID",
    "VALID_TRUSTED",
    "VALID_UNKNOWN_KEY",
    "Discrepancy",
    "Seal",
    "SealKey",
    "SealKeys",
    "SealReport",
    "SealVerification",
    "default_project_keys",
    "default_workspace_keys",
    "is_job_sealed",
    "is_project_sealed",
    "is_workspace_sealed",
    "job_seal_path",
    "job_seal_path_at",
    "project_seal_path",
    "read_seal",
    "resolve_seal_keys",
    "seal_job",
    "seal_workspace",
    "unseal_job",
    "unseal_workspace",
    "unsealed_jobs",
    "verify_job_seal",
    "verify_seal",
    "verify_tree",
    "verify_workspace_seal",
    "workspace_seal_path",
]

_LOGGER = logging.getLogger(__name__)

# -- locations ---------------------------------------------------------------


def job_seal_path_at(root: str | os.PathLike[str], job_key: str) -> Path:
    """Return where one job's seal lives, from a workspace root path alone.

    :param root: The workspace root directory.
    :param job_key: The job key whose seal path to build.
    :return: The job seal path.
    """

    return Path(root) / WORKSPACE_DIRECTORY / "seals" / "jobs" / f"{job_key}.json"


def job_seal_path(workspace: Workspace, job_key: str) -> Path:
    """Return where one job's seal lives.

    :param workspace: The workspace holding the job.
    :param job_key: The job key whose seal path to build.
    :return: The job seal path.
    """

    return job_seal_path_at(workspace.root, job_key)


def workspace_seal_path(workspace: Workspace) -> Path:
    """Return where one workspace's seal lives.

    :param workspace: The workspace whose seal path to build.
    :return: The workspace seal path.
    """

    return workspace.control / "seal.json"


def project_seal_path(project_root: str | os.PathLike[str]) -> Path:
    """Return where one project's seal lives.

    :param project_root: The project root whose seal path to build.
    :return: The project seal path.
    """

    return Path(project_root) / PROJECT_DIRECTORY / "seal.json"


def is_job_sealed(workspace: Workspace, job_key: str) -> bool:
    """Return whether one job carries a seal.

    :param workspace: The workspace holding the job.
    :param job_key: The job key to check.
    :return: Whether the job seal file exists.
    """

    return job_seal_path(workspace, job_key).is_file()


def is_workspace_sealed(workspace: Workspace) -> bool:
    """Return whether one workspace carries a seal.

    :param workspace: The workspace to check.
    :return: Whether the workspace seal file exists.
    """

    return workspace_seal_path(workspace).is_file()


def is_project_sealed(project_root: str | os.PathLike[str]) -> bool:
    """Return whether one project carries a seal.

    :param project_root: The project root to check.
    :return: Whether the project seal file exists.
    """

    return project_seal_path(project_root).is_file()


def default_workspace_keys(workspace: Workspace, refs: Sequence[str] | None = None) -> SealKeys:
    """Resolve a workspace's signing keys from ``seal.keys`` or explicit *refs*.

    :param workspace: The workspace whose ``seal.keys`` setting is read.
    :param refs: Key refs to use instead of the setting, when given.
    :return: The resolved signing keys and the roles that could not be resolved.
    :raises httk.workflow.errors.SealError: If no key at all could be resolved.
    """

    if refs is None:
        setting = workspace.read_settings().get("seal.keys", "project,identity")
        refs = [item.strip() for item in str(setting).split(",") if item.strip()]
    return resolve_seal_keys(refs, project_root=workspace.root)


def _job_subject(workspace: Workspace, marker: Marker) -> dict[str, object]:
    return {
        "workspace_id": workspace.workspace_id,
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "placement": marker.placement.as_posix(),
    }


def seal_job(workspace: Workspace, marker: Marker, *, keys: SealKeys | None = None) -> Path:
    """Seal one job's payload, or keep an identical existing seal.

    :param workspace: The workspace holding the job.
    :param marker: The marker locating the job payload.
    :param keys: The signing keys, or ``None`` to use the workspace default.
    :return: The job seal path.
    :raises httk.workflow.errors.SealError: If no signing key is available.
    :raises httk.workflow.errors.SealedError: If a seal with different records already exists.
    """

    payload = workspace.payload_path(marker.placement, marker.job_key)
    records = payload_file_records(payload)
    path = job_seal_path(workspace, marker.job_key)
    if path.is_file():
        existing = read_seal(path)
        if list(existing.records) == records:
            return path
        raise SealedError(f"job {marker.job_key} is already sealed with different contents; unseal it first")
    resolved = keys if keys is not None else default_workspace_keys(workspace)
    body = build_seal_body("job", _job_subject(workspace, marker), records)
    return write_seal(path, body, resolved.keys)


def unseal_job(workspace: Workspace, marker: Marker) -> None:
    """Remove one job's seal, refusing while its workspace is sealed.

    :param workspace: The workspace holding the job.
    :param marker: The marker locating the job.
    :raises httk.workflow.errors.SealedError: If the enclosing workspace is sealed.
    """

    if is_workspace_sealed(workspace):
        raise SealedError("cannot unseal a job while its workspace is sealed; unseal the workspace first")
    job_seal_path(workspace, marker.job_key).unlink(missing_ok=True)


# -- workspace seals ---------------------------------------------------------


def _workspace_job_markers(workspace: Workspace) -> list[Marker]:
    """Return every current job marker in a workspace, ordered by job key."""

    return sorted(workspace.scan_markers(), key=lambda marker: marker.job_key)


def unsealed_jobs(workspace: Workspace) -> list[Marker]:
    """Return the markers of jobs in a workspace that carry no seal.

    :param workspace: The workspace to inspect.
    :return: The markers of unsealed jobs, ordered by job key.
    """

    return [marker for marker in _workspace_job_markers(workspace) if not is_job_sealed(workspace, marker.job_key)]


def seal_workspace(workspace: Workspace, *, keys: SealKeys | None = None) -> Path:
    """Seal a workspace by recording every job's seal digest.

    :param workspace: The workspace to seal.
    :param keys: The signing keys, or ``None`` to use the workspace default.
    :return: The workspace seal path.
    :raises httk.workflow.errors.SealError: If a job is unsealed or no key is available.
    """

    unsealed = unsealed_jobs(workspace)
    if unsealed:
        listing = ", ".join(marker.job_id for marker in unsealed)
        raise SealError(f"cannot seal the workspace while these jobs are unsealed: {listing}")
    records: list[dict[str, object]] = []
    for marker in _workspace_job_markers(workspace):
        digest = hashlib.sha256(job_seal_path(workspace, marker.job_key).read_bytes()).hexdigest()
        records.append(
            {
                "job_id": marker.job_id,
                "job_key": marker.job_key,
                "placement": marker.placement.as_posix(),
                "kind": marker.kind,
                "seal_sha256": digest,
            }
        )
    resolved = keys if keys is not None else default_workspace_keys(workspace)
    body = build_seal_body("workspace", {"workspace_id": workspace.workspace_id}, records)
    return write_seal(workspace_seal_path(workspace), body, resolved.keys)


def unseal_workspace(workspace: Workspace) -> None:
    """Remove a workspace's seal, refusing while its project is sealed.

    :param workspace: The workspace to unseal.
    :raises httk.workflow.errors.SealedError: If the enclosing project is sealed.
    """

    project = discover_project(workspace.root)
    if project is not None and is_project_sealed(project):
        raise SealedError("cannot unseal a workspace while its project is sealed; unseal the project first")
    workspace_seal_path(workspace).unlink(missing_ok=True)


def _combine(base: SealVerification, discrepancies: Sequence[Discrepancy]) -> SealVerification:
    """Fold record discrepancies into a signature verdict."""

    if not discrepancies:
        return base
    reason = base.reason if base.verdict == INVALID else "the sealed subject no longer matches the seal"
    return replace(base, valid=False, verdict=INVALID, reason=reason, discrepancies=tuple(discrepancies))


def _verify_job(
    workspace: Workspace,
    job_key: str,
    placement: PurePosixPath,
    *,
    trusted_keys: Iterable[str],
    expected_roles: Iterable[str],
) -> SealVerification:
    """Verify one job seal's signature and re-walk its payload."""

    path = job_seal_path(workspace, job_key)
    if not path.is_file():
        return SealVerification(
            False, INVALID, f"job seal is absent: {path}", (), tuple(expected_roles), (Discrepancy(job_key, "missing"),)
        )
    base = verify_seal(path, trusted_keys=trusted_keys, expected_roles=expected_roles)
    seal = read_seal(path)
    actual = payload_file_records(workspace.payload_path(placement, job_key))
    return _combine(base, diff_records(list(seal.records), actual))


def verify_job_seal(
    workspace: Workspace,
    marker: Marker,
    *,
    trusted_keys: Iterable[str] = (),
    expected_roles: Iterable[str] = (),
) -> SealVerification:
    """Verify a job seal's signature and that it still describes the payload.

    :param workspace: The workspace holding the job.
    :param marker: The marker locating the job.
    :param trusted_keys: Trust anchors to classify the signers against.
    :param expected_roles: Signing roles the seal is expected to carry.
    :return: The verdict, including any payload discrepancies.
    """

    return _verify_job(
        workspace, marker.job_key, marker.placement, trusted_keys=trusted_keys, expected_roles=expected_roles
    )


def verify_workspace_seal(
    workspace: Workspace,
    *,
    trusted_keys: Iterable[str] = (),
    expected_roles: Iterable[str] = (),
) -> SealVerification:
    """Verify a workspace seal's signature and every job's seal digest.

    :param workspace: The workspace to verify.
    :param trusted_keys: Trust anchors to classify the signers against.
    :param expected_roles: Signing roles the seal is expected to carry.
    :return: The verdict, including jobs that disagree with the seal.
    """

    if not is_workspace_sealed(workspace):
        return SealVerification(False, INVALID, "not sealed", (), tuple(expected_roles), ())
    base = verify_seal(workspace_seal_path(workspace), trusted_keys=trusted_keys, expected_roles=expected_roles)
    seal = read_seal(workspace_seal_path(workspace))
    recorded = {str(record["job_key"]): record for record in seal.records}
    present = {marker.job_key for marker in workspace.scan_markers()}
    discrepancies: list[Discrepancy] = []
    for job_key in sorted(set(recorded) | present):
        if job_key not in present:
            discrepancies.append(Discrepancy(job_key, "missing_job"))
        elif job_key not in recorded:
            discrepancies.append(Discrepancy(job_key, "unsealed"))
        else:
            path = job_seal_path(workspace, job_key)
            if not path.is_file():
                discrepancies.append(Discrepancy(job_key, "missing"))
            elif hashlib.sha256(path.read_bytes()).hexdigest() != recorded[job_key]["seal_sha256"]:
                discrepancies.append(Discrepancy(job_key, "mismatch"))
    return _combine(base, discrepancies)


def verify_tree(
    path: str | os.PathLike[str],
    *,
    trusted_keys: Iterable[str] = (),
    deep: bool = True,
) -> SealReport:
    """Verify the seal at *path* and, when deep, every seal it references.

    *path* is a project root (holds ``httk_project/``), a workspace root (holds
    ``.httk-workspace/``), or a job payload directory (anything else, whose
    workspace is discovered upward). Discrepancies are never raised; only
    missing or malformed seal files are.

    :param path: The project root, workspace root, or job payload to verify.
    :param trusted_keys: Trust anchors to classify signers against.
    :param deep: Whether to recurse into every referenced child seal.
    :return: The flat report of every verdict, with an overall ``ok``.
    :raises httk.workflow.errors.FormatError: If *path* is not inside any seal-able subject.
    :raises OSError: If a seal file cannot be read.
    """

    location = Path(path).expanduser().resolve()
    if (location / PROJECT_DIRECTORY).is_dir():
        # The project level is core-owned: core seals a project's members and
        # verifies them through their registered handlers.
        from httk.core.project.sealing import verify_project

        return verify_project(location, trusted_keys=list(trusted_keys), deep=deep)
    entries: list[dict[str, object]] = []
    if (location / WORKSPACE_DIRECTORY).is_dir():
        workspace = Workspace(location)
        verification = verify_workspace_seal(workspace, trusted_keys=trusted_keys)
        entries.append(verification.as_entry("workspace", workspace.workspace_id))
        if deep and is_workspace_sealed(workspace):
            seal = read_seal(workspace_seal_path(workspace))
            for record in seal.records:
                job_key = str(record["job_key"])
                placement = PurePosixPath(str(record["placement"]))
                job = _verify_job(workspace, job_key, placement, trusted_keys=trusted_keys, expected_roles=())
                entries.append(job.as_entry("job", job_key))
    else:
        workspace_root = Workspace.discover(location)
        if workspace_root is None:
            raise FormatError(f"{location} is not a project, workspace, or job payload")
        workspace = Workspace(workspace_root)
        markers = workspace.find_markers(location.name)
        if not markers:
            raise FormatError(f"no job marker names the payload at {location}")
        marker = markers[0]
        verification = verify_job_seal(workspace, marker, trusted_keys=trusted_keys)
        entries.append(verification.as_entry("job", marker.job_key))
    ok = bool(entries) and all(bool(entry["valid"]) for entry in entries)
    return SealReport(tuple(entries), ok)
