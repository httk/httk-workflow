"""Write, read, sign, and verify seal documents for jobs, workspaces, and projects.

A *seal* is a signed statement about what a level of the workflow tree contained
at one moment. A job seal records the files of one payload and lives inside it,
in the payload-private ``.httk-job/seal.json``, so it travels with the job
directory wherever the directory goes and names nothing about the workspace
that held it when it was made; a workspace seal
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
import stat
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
    sign_seal_body,
    verify_seal,
    verify_signed_body,
    write_seal,
)

from ._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectory, JobDirectoryError
from ._util import json_bytes
from .errors import FormatError, SealedError, SealError
from .manifests import payload_file_records
from .models import (
    JOB_STATE_DIRECTORY,
    WORKSPACE_DIRECTORY,
    JobDefinition,
    Marker,
    placement_text,
)
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
    "project_seal_path",
    "read_seal",
    "resolve_seal_keys",
    "seal_job",
    "seal_workspace",
    "tree_ledger_keys",
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
_SEAL_NAME = "seal.json"
#: The job seal as a no-follow control path below the payload.
_JOB_SEAL = PurePosixPath(JOB_STATE_DIRECTORY, _SEAL_NAME)

# -- locations ---------------------------------------------------------------


def job_seal_path(payload: str | os.PathLike[str]) -> Path:
    """Return where one job's seal lives: inside its own payload.

    The seal sits in the payload-private ``.httk-job/`` directory, which every
    payload digest and seal record excludes, so the seal never covers itself
    and moves with the payload directory.

    :param payload: The job payload directory.
    :return: The job seal path.
    """

    return Path(payload) / JOB_STATE_DIRECTORY / _SEAL_NAME


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


def is_job_sealed(payload: str | os.PathLike[str]) -> bool:
    """Return whether one job payload carries a seal.

    Only a regular ``seal.json`` in a real ``.httk-job`` directory is a seal.
    The check never follows a link: a symlinked ``.httk-job`` or ``seal.json``,
    or a special file there, is not a seal anybody wrote, so it reads as no
    seal rather than as an error. The job could equally delete its own seal,
    and raising here would let it fail every transition of its own marker.

    :param payload: The job payload directory.
    :return: Whether a regular job seal file exists.
    """

    try:
        with JobDirectory.at(payload) as job_dir:
            information = job_dir.stat(_JOB_SEAL)
    except (FileNotFoundError, NotADirectoryError, JobDirectoryError) as exc:
        _LOGGER.debug("no usable job seal below %s: %s", payload, exc)
        return False
    return information is not None and stat.S_ISREG(information.st_mode)


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


def tree_ledger_keys(root: str | os.PathLike[str]) -> tuple[SealKey, ...]:
    """Resolve the id-ledger signing keys for collecting a calculation tree.

    A tree has no workspace settings, so the default ``seal.keys`` refs of a
    workspace (``project,identity``) are used, with the ``project`` ref
    discovered from *root*. When no key resolves, the sweep runs without a
    ledger: this logs a warning and returns no keys.

    :param root: The swept tree.
    :return: The resolved signing keys, or ``()`` when none is available.
    """

    try:
        return resolve_seal_keys(("project", "identity"), project_root=root).keys
    except SealError as exc:
        _LOGGER.warning(
            "no signing key is available to seal an id ledger (%s); entry ids collected from %s are store-minted "
            "and will NOT be stable across rebuilds. Configure seal.keys, or pass --no-id-ledger to silence this.",
            exc,
            root,
        )
        return ()


def _job_subject(marker: Marker) -> dict[str, object]:
    # Only the job's own identity: a seal names no workspace or placement, so a
    # job directory stays truthfully sealed wherever it is moved.
    return {"job_id": marker.job_id, "job_key": marker.job_key}


def _job_seal_present(job_dir: JobDirectory) -> bool:
    """Report whether a pinned payload holds a seal, refusing a non-regular one."""

    information = job_dir.stat(_JOB_SEAL)
    if information is None:
        return False
    if not stat.S_ISREG(information.st_mode):
        raise JobDirectoryError(f"{job_dir.path / _JOB_SEAL} is not a regular file")
    return True


def _read_job_seal(job_dir: JobDirectory) -> Seal:
    """Read and validate the seal of a pinned payload through a no-follow descriptor.

    The seal is opened without following a link, checked to be a regular file
    of bounded size, and parsed by :func:`read_seal` through the descriptor's
    ``/dev/fd`` alias, so a swapped entry is never read.
    """

    shown = job_dir.path / _JOB_SEAL
    descriptor = job_dir.open_read(_JOB_SEAL, limit=CONTROL_DOCUMENT_LIMIT)
    alias = f"/dev/fd/{descriptor}"
    try:
        seal = read_seal(alias)
    except ValueError as exc:
        raise ValueError(str(exc).replace(alias, str(shown))) from exc
    finally:
        os.close(descriptor)
    return replace(seal, path=shown)


def _job_seal_digest(workspace: Workspace, placement: PurePosixPath, job_key: str) -> str | None:
    """Return the SHA-256 of one job's seal document, or ``None`` when it has none."""

    try:
        with JobDirectory.open(jobs=workspace.jobs, placement=placement, job_key=job_key) as job_dir:
            if not _job_seal_present(job_dir):
                return None
            return hashlib.sha256(job_dir.read(_JOB_SEAL, CONTROL_DOCUMENT_LIMIT)).hexdigest()
    except FileNotFoundError:
        return None


def seal_job(workspace: Workspace, marker: Marker, *, keys: SealKeys | None = None) -> Path:
    """Seal one job's payload, or keep an identical existing seal.

    The seal is read and written through the job directory without following
    a symlinked ``.httk-job`` or ``seal.json``.

    :param workspace: The workspace holding the job.
    :param marker: The marker locating the job payload.
    :param keys: The signing keys, or ``None`` to use the workspace default.
    :return: The job seal path.
    :raises httk.workflow.errors.SealError: If no signing key is available.
    :raises httk.workflow.errors.SealedError: If a seal with different records already exists.
    :raises httk.workflow.errors.FormatError: If the seal path is a symlink or special file.
    """

    payload = workspace.payload_path(marker.placement, marker.job_key)
    records = payload_file_records(payload)
    path = job_seal_path(payload)
    with JobDirectory.open(jobs=workspace.jobs, placement=marker.placement, job_key=marker.job_key) as job_dir:
        if _job_seal_present(job_dir):
            existing = _read_job_seal(job_dir)
            if list(existing.records) == records:
                return path
            raise SealedError(f"job {marker.job_key} is already sealed with different contents; unseal it first")
        resolved = keys if keys is not None else default_workspace_keys(workspace)
        body = build_seal_body("job", _job_subject(marker), records)
        if not resolved.keys:
            raise SealError(f"no signing key is available to seal the {body.get('kind')}")
        # The document write_seal produces, written through the pinned payload.
        body_sha256, signatures = sign_seal_body(body, resolved.keys)
        document = {**body, "body_sha256": body_sha256, "signatures": signatures}
        with job_dir.directory(JOB_STATE_DIRECTORY, create=True) as state:
            state.write_atomic(_SEAL_NAME, json_bytes(document) + b"\n", durable=True)
    return path


def unseal_job(workspace: Workspace, marker: Marker) -> None:
    """Remove one job's seal, refusing while its workspace is sealed.

    :param workspace: The workspace holding the job.
    :param marker: The marker locating the job.
    :raises httk.workflow.errors.SealedError: If the enclosing workspace is sealed.
    :raises httk.workflow.errors.FormatError: If ``.httk-job`` is a symlink or not a directory.
    """

    if is_workspace_sealed(workspace):
        raise SealedError("cannot unseal a job while its workspace is sealed; unseal the workspace first")
    try:
        with JobDirectory.open(jobs=workspace.jobs, placement=marker.placement, job_key=marker.job_key) as job_dir:
            job_dir.unlink(_JOB_SEAL, missing_ok=True)
    except FileNotFoundError:
        return


# -- workspace seals ---------------------------------------------------------


def _workspace_job_markers(workspace: Workspace) -> list[Marker]:
    """Return every current job marker in a workspace, ordered by job key."""

    return sorted(workspace.scan_markers(), key=lambda marker: marker.job_key)


def unsealed_jobs(workspace: Workspace) -> list[Marker]:
    """Return the markers of jobs in a workspace that carry no seal.

    :param workspace: The workspace to inspect.
    :return: The markers of unsealed jobs, ordered by job key.
    """

    return [
        marker
        for marker in _workspace_job_markers(workspace)
        if not is_job_sealed(workspace.payload_path(marker.placement, marker.job_key))
    ]


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
        digest = _job_seal_digest(workspace, marker.placement, marker.job_key)
        if digest is None:
            raise SealError(f"cannot seal the workspace while job {marker.job_id} is unsealed")
        records.append(
            {
                "job_id": marker.job_id,
                "job_key": marker.job_key,
                "placement": placement_text(marker.placement),
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


def verify_job_seal(
    payload: str | os.PathLike[str],
    *,
    trusted_keys: Iterable[str] = (),
    expected_roles: Iterable[str] = (),
) -> SealVerification:
    """Verify a job seal's signature and that it still describes the payload.

    Verification needs only the payload directory, so it works the same for a
    job inside a workspace and for a free-standing job directory.

    :param payload: The job payload directory.
    :param trusted_keys: Trust anchors to classify the signers against.
    :param expected_roles: Signing roles the seal is expected to carry.
    :return: The verdict, including any payload discrepancies; a symlinked or
        special ``.httk-job`` or ``seal.json`` is an ``invalid`` discrepancy.
    """

    payload = Path(payload)
    path = job_seal_path(payload)
    seal: Seal | None = None
    try:
        with JobDirectory.at(payload) as job_dir:
            if _job_seal_present(job_dir):
                seal = _read_job_seal(job_dir)
    except FileNotFoundError:
        seal = None
    except JobDirectoryError as exc:
        # A symlinked or special .httk-job or seal.json is never followed, and
        # is no seal (as for is_job_sealed); here it is reported, not raised.
        return SealVerification(
            False,
            INVALID,
            f"job seal is not a regular file in a real directory: {path} ({exc})",
            (),
            tuple(expected_roles),
            (Discrepancy(payload.name, "invalid"),),
        )
    if seal is None:
        return SealVerification(
            False,
            INVALID,
            f"job seal is absent: {path}",
            (),
            tuple(expected_roles),
            (Discrepancy(payload.name, "missing"),),
        )
    # verify_seal's signature check, on the seal read once through the descriptor.
    base = verify_signed_body(
        seal.body_bytes, seal.body_sha256, seal.signatures, trusted_keys=trusted_keys, expected_roles=expected_roles
    )
    return _combine(base, diff_records(list(seal.records), payload_file_records(payload)))


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
            placement = PurePosixPath(str(recorded[job_key]["placement"]))
            digest = _job_seal_digest(workspace, placement, job_key)
            if digest is None:
                discrepancies.append(Discrepancy(job_key, "missing"))
            elif digest != recorded[job_key]["seal_sha256"]:
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
    ``.httk-workspace/``), or a job payload directory (holds ``job.json``),
    which need not be inside any workspace. Discrepancies are never raised;
    only missing or malformed seal files are.

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
                job = verify_job_seal(workspace.payload_path(placement, job_key), trusted_keys=trusted_keys)
                entries.append(job.as_entry("job", job_key))
    elif (location / "job.json").is_file():
        # A job seal lives in its payload, so a job verifies from its own
        # directory alone, inside a workspace or free-standing.
        definition = JobDefinition.from_path(location / "job.json")
        verification = verify_job_seal(location, trusted_keys=trusted_keys)
        entries.append(verification.as_entry("job", definition.job_key))
    else:
        raise FormatError(f"{location} is not a project, workspace, or job payload")
    ok = bool(entries) and all(bool(entry["valid"]) for entry in entries)
    return SealReport(tuple(entries), ok)
