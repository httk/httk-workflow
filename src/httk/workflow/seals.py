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

import base64
import hashlib
import json
import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath

from httk.core.crypto import ed25519_public_key, ed25519_sign, ed25519_verify
from httk.core.identity import identity_key_paths, identity_seed

from ._util import json_bytes, utc_now, write_json_atomic
from .errors import FormatError, SealedError, SealError
from .manifests import _records, _seed
from .models import WORKSPACE_DIRECTORY, Marker
from .projects import (
    PROJECT_DIRECTORY,
    canonical_public_key,
    discover_project,
    format_public_key,
    key_fingerprint,
    parse_public_key,
    read_project,
)
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

#: Domain separation of every seal signature, so a seal digest is never a valid
#: signature of a manifest, an identity document, or any other httk artifact.
_DOMAIN = b"httk-seal-v1\0"

_FORMAT = "httk-seal"
_FORMAT_VERSION = 1

#: The signing roles a seal understands.
ROLE_PROJECT = "project"
ROLE_IDENTITY = "identity"
ROLE_FILE = "file"

#: The seal signature verified and a signer is a pinned trust anchor.
VALID_TRUSTED = "valid_trusted"
#: The seal signature verified, but nothing pins the key that made it.
VALID_UNKNOWN_KEY = "valid_unknown_key"
#: The seal does not describe this tree, or a signature does not verify.
INVALID = "invalid"

#: One resolved signing key: its role and its raw 32-byte Ed25519 seed.
SealKey = tuple[str, bytes]

#: The members of a seal document that make up the signed body.
_BODY_MEMBERS = ("format", "format_version", "kind", "subject", "created_at", "records")


@dataclass(frozen=True)
class SealKeys:
    """The signing keys resolved for one seal, and the roles that were missing.

    :param keys: The available signing keys, in the order their refs resolved.
    :param missing_roles: The roles that were requested but could not be resolved.
    """

    keys: tuple[SealKey, ...]
    missing_roles: tuple[str, ...]


@dataclass(frozen=True)
class Discrepancy:
    """One way a sealed subject no longer matches its seal.

    :param path: The record path, job key, or workspace path that disagrees.
    :param kind: The disagreement: ``missing``, ``extra``, ``mismatch``,
        ``unsealed`` (present but not recorded), or ``missing_job`` (recorded but
        absent).
    """

    path: str
    kind: str


@dataclass(frozen=True)
class Seal:
    """One parsed seal document.

    :param kind: The sealed level: ``job``, ``workspace``, or ``project``.
    :param subject: The identifiers of the sealed subject.
    :param created_at: When the seal was written.
    :param records: The recorded contents of the sealed subject.
    :param body_sha256: The recorded digest of the signed body.
    :param signatures: The detached signatures over the body digest.
    :param body_bytes: The canonical bytes the recorded digest is taken over.
    :param path: Where the seal was read from.
    """

    kind: str
    subject: dict[str, object]
    created_at: str
    records: tuple[dict[str, object], ...]
    body_sha256: str
    signatures: tuple[dict[str, object], ...]
    body_bytes: bytes
    path: Path


@dataclass(frozen=True)
class SealVerification:
    """What verifying one seal established.

    :param valid: Whether the seal describes its subject and a signature verified.
    :param verdict: One of :data:`VALID_TRUSTED`, :data:`VALID_UNKNOWN_KEY`, or
        :data:`INVALID`.
    :param reason: A human-readable explanation of the verdict.
    :param signers: The fingerprints of the signatures that verified.
    :param missing_signers: The expected roles that the seal did not carry.
    :param discrepancies: How the subject diverges from the seal, if at all.
    """

    valid: bool
    verdict: str
    reason: str
    signers: tuple[str, ...]
    missing_signers: tuple[str, ...]
    discrepancies: tuple[Discrepancy, ...]


@dataclass(frozen=True)
class SealReport:
    """The verdicts of verifying a seal and, when deep, every seal below it.

    :param entries: A flat sequence of ``(level, subject, verification)`` tuples,
        parent before child.
    :param ok: Whether every entry is valid and at least one entry exists.
    """

    entries: tuple[tuple[str, str, SealVerification], ...]
    ok: bool


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


# -- keys --------------------------------------------------------------------


def _role_of(ref: str) -> str:
    """Classify one key ref into the role its signature carries."""

    if ref == ROLE_PROJECT:
        return ROLE_PROJECT
    if ref == ROLE_IDENTITY or ref.startswith("identity:"):
        return ROLE_IDENTITY
    return ROLE_FILE


def _seed_for_ref(ref: str, root: Path) -> bytes | None:
    """Resolve one key ref to a seed, or ``None`` when it is unavailable."""

    if ref == ROLE_PROJECT:
        project = discover_project(root)
        if project is None:
            return None
        try:
            return _seed(project)
        except ValueError:
            return None
    if ref == ROLE_IDENTITY:
        return identity_seed()
    if ref.startswith("identity:"):
        short = ref[len("identity:") :]
        try:
            seed_path = identity_key_paths(short)[0]
        except ValueError:
            return None
        return identity_seed(seed_path)
    path = Path(ref).expanduser()
    try:
        seed = base64.b64decode(path.read_text(encoding="ascii").strip(), validate=True)
    except (OSError, ValueError):
        return None
    return seed if len(seed) == 32 else None


def resolve_seal_keys(refs: Sequence[str], *, root: Path) -> SealKeys:
    """Resolve seal-key refs to the signing keys that are actually available.

    A ref is ``project`` (the project's own signing seed, discovered from
    *root*), ``identity`` (the default operator identity), ``identity:<short>``
    (a named identity), or the path of a base64 Ed25519 seed file. An
    unavailable ref is skipped and logged rather than fatal; only resolving no
    key at all is an error.

    :param refs: The key refs to resolve, in order.
    :param root: The tree the ``project`` ref is discovered from.
    :return: The resolved keys and the roles that could not be resolved.
    :raises httk.workflow.errors.SealError: If no key at all could be resolved.
    """

    keys: list[SealKey] = []
    missing: list[str] = []
    for ref in refs:
        role = _role_of(ref)
        seed = _seed_for_ref(ref, root)
        if seed is None:
            missing.append(role)
            # Skipping one recoverable ref while others still sign is not itself a
            # problem: resolving no key at all raises below, and the caller gets
            # the skipped roles in ``missing_roles``. Logged at info so auto-seal
            # of a projectless workspace does not warn on every succeeded job.
            _LOGGER.info(
                "seal key %r is unavailable and will not sign", ref, extra={"event": "seal_key_unavailable", "ref": ref}
            )
            continue
        keys.append((role, seed))
    if not keys:
        raise SealError(f"no seal signing key could be resolved from {list(refs)}")
    return SealKeys(tuple(keys), tuple(missing))


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
    return resolve_seal_keys(refs, root=workspace.root)


def default_project_keys(root: Path, refs: Sequence[str] | None = None) -> SealKeys:
    """Resolve a project's signing keys from its ``seal_keys`` member or *refs*.

    :param root: The project root whose ``seal_keys`` member is read.
    :param refs: Key refs to use instead of the project member, when given.
    :return: The resolved signing keys and the roles that could not be resolved.
    :raises httk.workflow.errors.SealError: If the member is malformed or no key resolves.
    """

    if refs is None:
        raw = read_project(root).get("seal_keys", [ROLE_PROJECT, ROLE_IDENTITY])
        if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
            raise SealError("project member 'seal_keys' must be an array of strings")
        refs = [str(item) for item in raw]
    return resolve_seal_keys(refs, root=root)


# -- writing -----------------------------------------------------------------


def _build_body(kind: str, subject: Mapping[str, object], records: Sequence[dict[str, object]]) -> dict[str, object]:
    """Assemble the signed body of one seal document."""

    return {
        "format": _FORMAT,
        "format_version": _FORMAT_VERSION,
        "kind": kind,
        "subject": subject,
        "created_at": utc_now(),
        "records": list(records),
    }


def _sign(body: dict[str, object], keys: Sequence[SealKey]) -> tuple[str, list[dict[str, object]]]:
    """Digest the body and produce one detached signature per key."""

    body_digest = hashlib.sha256(_DOMAIN + json_bytes(body)).digest()
    message = _DOMAIN + body_digest
    signatures: list[dict[str, object]] = []
    for role, seed in keys:
        key = format_public_key(ed25519_public_key(seed))
        signatures.append(
            {
                "role": role,
                "key": key,
                "fingerprint": key_fingerprint(key),
                "signature": base64.b64encode(ed25519_sign(seed, message)).decode("ascii"),
            }
        )
    return body_digest.hex(), signatures


def _write_seal(
    path: Path,
    kind: str,
    subject: Mapping[str, object],
    records: Sequence[dict[str, object]],
    keys: Sequence[SealKey],
) -> Path:
    """Write one signed seal document atomically and return its path."""

    if not keys:
        raise SealError(f"no signing key is available to seal the {kind}")
    body = _build_body(kind, subject, records)
    body_sha256, signatures = _sign(body, keys)
    document = {**body, "body_sha256": body_sha256, "signatures": signatures}
    write_json_atomic(path, document, durable=True)
    return path


# -- job seals ---------------------------------------------------------------


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
    records = list(_records(payload, ()))
    path = job_seal_path(workspace, marker.job_key)
    if path.is_file():
        existing = read_seal(path)
        if list(existing.records) == records:
            return path
        raise SealedError(f"job {marker.job_key} is already sealed with different contents; unseal it first")
    resolved = keys if keys is not None else default_workspace_keys(workspace)
    return _write_seal(path, "job", _job_subject(workspace, marker), records, resolved.keys)


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
    return _write_seal(
        workspace_seal_path(workspace), "workspace", {"workspace_id": workspace.workspace_id}, records, resolved.keys
    )


def unseal_workspace(workspace: Workspace) -> None:
    """Remove a workspace's seal, refusing while its project is sealed.

    :param workspace: The workspace to unseal.
    :raises httk.workflow.errors.SealedError: If the enclosing project is sealed.
    """

    project = discover_project(workspace.root)
    if project is not None and is_project_sealed(project):
        raise SealedError("cannot unseal a workspace while its project is sealed; unseal the project first")
    workspace_seal_path(workspace).unlink(missing_ok=True)


def read_seal(path: str | os.PathLike[str]) -> Seal:
    """Read and structurally validate one seal document.

    :param path: The seal file to read.
    :return: The parsed seal.
    :raises httk.workflow.errors.FormatError: If the file is not a valid seal document.
    :raises OSError: If the file cannot be read.
    """

    location = Path(path)
    try:
        document = json.loads(location.read_bytes())
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FormatError(f"seal is not valid JSON: {location}") from exc
    if not isinstance(document, dict):
        raise FormatError(f"seal is not a JSON object: {location}")
    if document.get("format") != _FORMAT or document.get("format_version") != _FORMAT_VERSION:
        raise FormatError(f"not an {_FORMAT} version {_FORMAT_VERSION} document: {location}")
    try:
        body = {member: document[member] for member in _BODY_MEMBERS}
    except KeyError as exc:
        raise FormatError(f"seal is missing member {exc.args[0]!r}: {location}") from exc
    subject = document["subject"]
    records = document["records"]
    signatures = document["signatures"]
    body_sha256 = document["body_sha256"]
    if (
        not isinstance(subject, dict)
        or not isinstance(records, list)
        or not isinstance(signatures, list)
        or not isinstance(body_sha256, str)
        or not all(isinstance(item, dict) for item in records)
        or not all(isinstance(item, dict) for item in signatures)
    ):
        raise FormatError(f"seal has a malformed body: {location}")
    return Seal(
        kind=str(document["kind"]),
        subject=subject,
        created_at=str(document["created_at"]),
        records=tuple(records),
        body_sha256=body_sha256,
        signatures=tuple(signatures),
        body_bytes=json_bytes(body),
        path=location,
    )


def _normalize_trusted(trusted_keys: Iterable[str]) -> set[str]:
    """Return trust anchors as canonical keys and fingerprints, skipping junk."""

    result: set[str] = set()
    for entry in trusted_keys:
        text = str(entry).strip()
        if text.startswith("sha256:"):
            result.add(text)
            continue
        try:
            result.add(canonical_public_key(text))
        except ValueError:
            continue
    return result


def verify_seal(
    path: str | os.PathLike[str],
    *,
    trusted_keys: Iterable[str] = (),
    expected_roles: Iterable[str] = (),
) -> SealVerification:
    """Verify one seal's body digest and signatures, and classify the signers.

    This checks the seal against itself, not against the tree: at least one
    signature must verify, any signature that does not makes the seal invalid,
    and the seal is trusted when a verifying signer is a pinned key or
    fingerprint. The record-level checks are done by the ``verify_*_seal``
    functions.

    :param path: The seal file to verify.
    :param trusted_keys: Trust anchors as ``ed25519:`` keys or ``sha256:`` fingerprints.
    :param expected_roles: Signing roles the seal is expected to carry.
    :return: The signature verdict.
    :raises httk.workflow.errors.FormatError: If the file is not a valid seal document.
    :raises OSError: If the file cannot be read.
    """

    seal = read_seal(path)
    expected = tuple(expected_roles)
    present_roles = {str(signature.get("role")) for signature in seal.signatures}
    missing = tuple(role for role in expected if role not in present_roles)
    if hashlib.sha256(_DOMAIN + seal.body_bytes).hexdigest() != seal.body_sha256:
        return SealVerification(False, INVALID, "the seal body does not match its recorded digest", (), missing, ())
    try:
        message = _DOMAIN + bytes.fromhex(seal.body_sha256)
    except ValueError:
        return SealVerification(False, INVALID, "the seal's recorded digest is not hex", (), missing, ())
    trusted = _normalize_trusted(trusted_keys)
    signers: list[str] = []
    any_invalid = False
    any_trusted = False
    for signature in seal.signatures:
        key = signature.get("key")
        raw = signature.get("signature")
        if not isinstance(key, str) or not isinstance(raw, str):
            any_invalid = True
            continue
        try:
            public_key = parse_public_key(key)
            signature_bytes = base64.b64decode(raw, validate=True)
        except ValueError:
            any_invalid = True
            continue
        if len(public_key) != 32 or not ed25519_verify(public_key, message, signature_bytes):
            any_invalid = True
            continue
        canonical = format_public_key(public_key)
        fingerprint = key_fingerprint(canonical)
        signers.append(fingerprint)
        if canonical in trusted or fingerprint in trusted:
            any_trusted = True
    if any_invalid:
        return SealVerification(False, INVALID, "a seal signature does not verify", tuple(signers), missing, ())
    if not signers:
        return SealVerification(False, INVALID, "the seal carries no signature", (), missing, ())
    if any_trusted:
        return SealVerification(True, VALID_TRUSTED, "signed by a trusted key", tuple(signers), missing, ())
    return SealVerification(
        True, VALID_UNKNOWN_KEY, "the signature verifies but no signer is trusted", tuple(signers), missing, ()
    )


# -- record checks -----------------------------------------------------------


def _combine(base: SealVerification, discrepancies: Sequence[Discrepancy]) -> SealVerification:
    """Fold record discrepancies into a signature verdict."""

    if not discrepancies:
        return base
    reason = base.reason if base.verdict == INVALID else "the sealed subject no longer matches the seal"
    return replace(base, valid=False, verdict=INVALID, reason=reason, discrepancies=tuple(discrepancies))


def _diff_records(recorded: Sequence[dict[str, object]], actual: Sequence[dict[str, object]]) -> list[Discrepancy]:
    """Diff two path-keyed record lists into discrepancies."""

    recorded_by_path = {str(record["path"]): record for record in recorded}
    actual_by_path = {str(record["path"]): record for record in actual}
    discrepancies: list[Discrepancy] = []
    for relpath in sorted(set(recorded_by_path) | set(actual_by_path)):
        if relpath not in actual_by_path:
            discrepancies.append(Discrepancy(relpath, "missing"))
        elif relpath not in recorded_by_path:
            discrepancies.append(Discrepancy(relpath, "extra"))
        elif recorded_by_path[relpath] != actual_by_path[relpath]:
            discrepancies.append(Discrepancy(relpath, "mismatch"))
    return discrepancies


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
    actual = list(_records(workspace.payload_path(placement, job_key), ()))
    return _combine(base, _diff_records(list(seal.records), actual))


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


def _verify_workspace_into(
    workspace: Workspace,
    entries: list[tuple[str, str, SealVerification]],
    trusted_keys: Iterable[str],
    deep: bool,
) -> None:
    """Append a workspace verdict and, when deep, every referenced job."""

    verification = verify_workspace_seal(workspace, trusted_keys=trusted_keys)
    entries.append(("workspace", workspace.workspace_id, verification))
    if not deep or not is_workspace_sealed(workspace):
        return
    seal = read_seal(workspace_seal_path(workspace))
    for record in seal.records:
        job_key = str(record["job_key"])
        placement = PurePosixPath(str(record["placement"]))
        job = _verify_job(workspace, job_key, placement, trusted_keys=trusted_keys, expected_roles=())
        entries.append(("job", job_key, job))


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
    entries: list[tuple[str, str, SealVerification]] = []
    if (location / PROJECT_DIRECTORY).is_dir():
        # The project level is core-owned: core seals a project's members and
        # verifies them through their registered handlers. Delegate and adapt
        # core's report entries back to this module's tuple shape.
        from httk.core.project.sealing import verify_project as _core_verify_project

        core_report = _core_verify_project(location, trusted_keys=list(trusted_keys), deep=deep)
        for entry in core_report.entries:
            discrepancies = tuple(
                Discrepancy(str(item["path"]), str(item["kind"]))
                for item in entry["discrepancies"]  # type: ignore[union-attr]
            )
            verification = SealVerification(
                bool(entry["valid"]),
                str(entry["verdict"]),
                str(entry["reason"]),
                tuple(str(value) for value in entry["signers"]),  # type: ignore[union-attr]
                tuple(str(value) for value in entry["missing_signers"]),  # type: ignore[union-attr]
                discrepancies,
            )
            entries.append((str(entry["level"]), str(entry["subject"]), verification))
        return SealReport(tuple(entries), core_report.ok)
    if (location / WORKSPACE_DIRECTORY).is_dir():
        _verify_workspace_into(Workspace(location), entries, trusted_keys, deep)
    else:
        workspace_root = Workspace.discover(location)
        if workspace_root is None:
            raise FormatError(f"{location} is not a project, workspace, or job payload")
        workspace = Workspace(workspace_root)
        markers = workspace.find_markers(location.name)
        if not markers:
            raise FormatError(f"no job marker names the payload at {location}")
        marker = markers[0]
        entries.append(("job", marker.job_key, verify_job_seal(workspace, marker, trusted_keys=trusted_keys)))
    ok = bool(entries) and all(verification.valid for _level, _subject, verification in entries)
    return SealReport(tuple(entries), ok)
