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

A job seal is never rewritten. A sealed file moved out to another job (``job
new --input-move-from-job``) is recorded by a signed *release record* in the
payload's ``.httk-job/releases/`` directory (one record per path), which amends the seal: verification
accepts the file's absence while a valid record covers it.

The signature is over a domain-separated digest of the document body, exactly as
:mod:`httk.workflow.manifests` signs a manifest, so a digest signed as a seal can
never be replayed as anything else. Verification answers the two independent
questions a signature always raises separately — does the seal still describe
this tree, and was it made by a key this project trusts — and reports both.
"""

import errno
import hashlib
import json
import logging
import os
import re
import stat
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from itertools import islice
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
)

from . import _fs, _kernel
from ._job import JobDefinition
from ._kernel import JobRef
from ._util import json_bytes, utc_now
from .errors import FormatError, SealedError, SealError, WorkflowError
from .manifests import payload_file_records
from .models import JOB_STATE_DIRECTORY, WORKSPACE_DIRECTORY, placement_text
from .projects import PROJECT_DIRECTORY, discover_project
from .workspace import Workspace

__all__ = [
    "INVALID",
    "VALID_TRUSTED",
    "VALID_UNKNOWN_KEY",
    "Discrepancy",
    "Release",
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
    "job_releases_path",
    "job_seal_digest",
    "job_seal_path",
    "project_seal_path",
    "read_job_release",
    "read_job_releases",
    "read_seal",
    "record_job_release",
    "recorded_job_path",
    "require_cli_modifiable",
    "resolve_seal_keys",
    "seal_job",
    "seal_payload",
    "seal_workspace",
    "tree_ledger_keys",
    "unseal_job",
    "unseal_workspace",
    "unsealed_jobs",
    "valid_job_releases",
    "verify_job_seal",
    "verify_seal",
    "verify_tree",
    "verify_workspace_seal",
    "workspace_jobs",
    "workspace_seal_path",
]

_LOGGER = logging.getLogger(__name__)
_SEAL_NAME = "seal.json"
#: The largest job seal read from a (job-written) payload.
_SEAL_LIMIT = 16 * 1024 * 1024
_RELEASES_DIRECTORY = "releases"
_RELEASE_FORMAT = "httk-job-release"
#: The largest release record, and the most records one job's release directory is read with.
_RELEASE_LIMIT = 1024 * 1024
_RELEASES_LIMIT = 65536
_RELEASE_MEMBERS = frozenset({"kind", "amends", "path", "size", "sha256", "released_to", "at"})
_HEX64 = re.compile(r"[0-9a-f]{64}")

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
        state = _fs.open_dir_under(Path(payload), JOB_STATE_DIRECTORY)
    except (FileNotFoundError, NotADirectoryError, _fs.UnsafePath) as exc:
        _LOGGER.debug("no usable job seal below %s: %s", payload, exc)
        return False
    try:
        return stat.S_ISREG(os.stat(_SEAL_NAME, dir_fd=state, follow_symlinks=False).st_mode)
    except FileNotFoundError:
        return False
    finally:
        os.close(state)


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


def _job_seal_bytes(payload: str | os.PathLike[str]) -> bytes | None:
    """Read a payload's seal document without following a link or blocking.

    :param payload: The job payload directory.
    :return: The seal bytes, or ``None`` when the payload has no seal.
    :raises httk.workflow.errors.FormatError: If ``.httk-job`` or the seal is a symlink or special file,
        or the seal is oversized.
    """

    return _job_state_bytes(payload, _SEAL_NAME)


def _job_state_bytes(payload: str | os.PathLike[str], name: str) -> bytes | None:
    """Read one ``.httk-job`` document (the seal or the releases) without following a link or blocking."""

    try:
        state = _fs.open_dir_under(Path(payload), JOB_STATE_DIRECTORY)
    except FileNotFoundError:
        return None
    except _fs.UnsafePath as exc:
        raise FormatError(str(exc)) from exc
    try:
        return _fs.read_bounded(_fs.anchored(state, name), _SEAL_LIMIT, nonblock=True)
    except _fs.UnsafePath as exc:
        raise FormatError(str(exc)) from exc
    except OSError as exc:
        if exc.errno == errno.ENXIO:  # a socket: opening one fails rather than reaching the regular-file check
            raise FormatError(f"{Path(payload) / JOB_STATE_DIRECTORY / name} is not a regular file") from exc
        raise
    finally:
        os.close(state)


def _parse_job_seal(data: bytes, shown: Path) -> Seal:
    """Validate seal bytes read once with :func:`read_seal`, which only reads a path."""

    # ponytail: read_seal takes only a path, so the bytes go through an anonymous temporary; a bytes parser in
    # httk-core would drop it.
    with tempfile.TemporaryFile() as handle:
        handle.write(data)
        handle.flush()
        handle.seek(0)
        alias = f"/dev/fd/{handle.fileno()}"
        try:
            seal = read_seal(alias)
        except ValueError as exc:
            raise ValueError(str(exc).replace(alias, str(shown))) from exc
    return replace(seal, path=shown)


def job_seal_digest(payload: str | os.PathLike[str]) -> str | None:
    """Return the SHA-256 of a job's seal document, or ``None`` when it has none.

    :param payload: The job directory.
    :return: The hex digest, which ``state.json`` records as ``seal.sha256``.
    :raises httk.workflow.errors.FormatError: If the seal or ``.httk-job`` is a symlink or special file.
    """

    data = _job_seal_bytes(payload)
    return None if data is None else hashlib.sha256(data).hexdigest()


def seal_payload(
    payload: str | os.PathLike[str], *, job_id: str, job_key: str, keys: Sequence[SealKey], durable: bool
) -> tuple[str, bool]:
    """Compute and write the job seal of a quiescent payload directory, replacing any earlier copy.

    The records are :func:`httk.workflow.manifests.payload_file_records`.
    Without *keys* the seal is unsigned: the records and ``body_sha256`` with
    an empty ``signatures``.

    :param payload: The job directory (its processes are gone).
    :param job_id: The job UUID, the seal's subject.
    :param job_key: The job key, the seal's subject.
    :param keys: The signing keys; empty for an unsigned seal.
    :param durable: Fsync the written seal.
    :return: The SHA-256 of the written seal document and whether it is signed.
    :raises httk.workflow.errors.FormatError: If ``.httk-job`` is a symlink or not a directory.
    """

    base = Path(payload)
    records = payload_file_records(base)
    state = base / JOB_STATE_DIRECTORY
    try:
        if not stat.S_ISDIR(os.lstat(state).st_mode):
            raise FormatError(f"{state} is a symlink or not a directory")
    except FileNotFoundError:
        _fs.make_dirs(state.absolute(), durable=durable)
    # Only the job's own identity: a seal names no workspace or placement, so it stays true wherever the job moves.
    body = build_seal_body("job", {"job_id": job_id, "job_key": job_key}, records)
    body_sha256, signatures = sign_seal_body(body, keys)
    data = json_bytes({**body, "body_sha256": body_sha256, "signatures": signatures}) + b"\n"
    _fs.write_file(_fs.loc(state / _SEAL_NAME), data, durable=durable)
    return hashlib.sha256(data).hexdigest(), bool(signatures)


# -- release records ---------------------------------------------------------


@dataclass(frozen=True)
class Release:
    """One release record: a sealed file moved out of its job, which amends the job seal without rewriting it.

    The record is its own signed body (``kind`` ``"release"``), signed like a
    seal, stored as one file per released path. It covers the file only while
    it is valid against the seal it amends (see :func:`valid_job_releases`).

    :param path: The payload-relative path the file had in the released job.
    :param size: The file size in bytes, as the seal records it.
    :param sha256: The file's SHA-256, as the seal records it.
    :param to_job_key: The job key of the job the file was moved into.
    :param to_path: The payload-relative path of the file in that job.
    :param at: When the file was released (UTC ISO 8601).
    :param amends: The ``body_sha256`` of the job seal the record amends.
    :param body_sha256: The recorded digest of the signed body.
    :param signatures: The detached signatures over the body digest; empty when unsigned.
    :param body: The signed body.
    """

    path: str
    size: int
    sha256: str
    to_job_key: str
    to_path: str
    at: str
    amends: str
    body_sha256: str
    signatures: tuple[dict[str, object], ...]
    body: Mapping[str, object]

    def as_mapping(self) -> dict[str, object]:
        """Return the record document as it is stored.

        :return: ``{format, version, body, body_sha256, signatures}``.
        """

        return {
            "format": _RELEASE_FORMAT,
            "version": 1,
            "body": dict(self.body),
            "body_sha256": self.body_sha256,
            "signatures": list(self.signatures),
        }


def job_releases_path(payload: str | os.PathLike[str]) -> Path:
    """Return where one job's release records live: beside its seal, in the ``.httk-job/releases/`` directory.

    Each released path has one record, ``<SHA-256 of the payload-relative path>.json``.

    :param payload: The job payload directory.
    :return: The release record directory.
    """

    return Path(payload) / JOB_STATE_DIRECTORY / _RELEASES_DIRECTORY


def _release_name(path: str) -> str:
    """Return the record file name of one released payload-relative path."""

    return hashlib.sha256(path.encode("utf-8")).hexdigest() + ".json"


def _parse_release(document: object) -> Release:
    """Structurally validate one stored release record document."""

    if (
        not isinstance(document, dict)
        or document.get("format") != _RELEASE_FORMAT
        or document.get("version") != 1
        or not isinstance(body := document.get("body"), dict)
    ):
        raise FormatError(f"a release record is not an {_RELEASE_FORMAT} version 1 document")
    target = body.get("released_to")
    body_sha256, signatures = document.get("body_sha256"), document.get("signatures")
    fields = (body.get("path"), body.get("amends"), body.get("at"), body_sha256)
    if (
        set(body) != _RELEASE_MEMBERS
        or body.get("kind") != "release"
        or not all(isinstance(field, str) for field in fields)
        or not isinstance(size := body.get("size"), int)
        or isinstance(size, bool)
        or not isinstance(sha256 := body.get("sha256"), str)
        or not _HEX64.fullmatch(sha256)
        or not isinstance(target, dict)
        or set(target) != {"job_key", "path"}
        or not all(isinstance(value, str) for value in target.values())
        or not isinstance(signatures, list)
        or not all(isinstance(signature, dict) for signature in signatures)
    ):
        raise FormatError("a release record has a malformed body")
    return Release(
        path=str(body["path"]),
        size=size,
        sha256=sha256,
        to_job_key=str(target["job_key"]),
        to_path=str(target["path"]),
        at=str(body["at"]),
        amends=str(body["amends"]),
        body_sha256=str(body_sha256),
        signatures=tuple(signatures),
        body=body,
    )


def _read_release(directory: int, name: str, payload: str | os.PathLike[str]) -> Release | None:
    """Read one record file below the open release directory; ``None`` when absent, unsafe or malformed (warned)."""

    try:
        data = _fs.read_bounded(_fs.anchored(directory, name), _RELEASE_LIMIT, nonblock=True)
        if data is None:
            return None
        release = _parse_release(json.loads(data))
        if _release_name(release.path) != name:
            raise FormatError(f"it is not named for its path {release.path!r}")
    except (WorkflowError, OSError, ValueError) as exc:
        _LOGGER.warning("skipping the release record %s of %s: %s", name, payload, exc)
        return None
    return release


def _open_releases_readonly(payload: str | os.PathLike[str]) -> int | None:
    """Open a job's release directory without following a link; ``None`` when absent or not a real directory."""

    try:
        return _fs.open_dir_under(Path(payload), f"{JOB_STATE_DIRECTORY}/{_RELEASES_DIRECTORY}")
    except (FileNotFoundError, NotADirectoryError, _fs.UnsafePath) as exc:
        _LOGGER.debug("no usable release records below %s: %s", payload, exc)
        return None


def read_job_releases(payload: str | os.PathLike[str]) -> tuple[Release, ...]:
    """List a job's release records (for diagnostics) without following a link or blocking.

    Validation never lists the directory (see :func:`valid_job_releases`).
    The listing is bounded; a dot-named temporary is skipped silently, and an
    entry that is not a regular file named for its record, or that does not
    parse, is skipped with a warning. A ``releases`` entry that is not a real
    directory (a job could plant one) is no records.

    :param payload: The job payload directory.
    :return: The records, ordered by record file name.
    :raises httk.workflow.errors.FormatError: If the directory holds more than the bounded number of entries.
    """

    directory = _open_releases_readonly(payload)
    if directory is None:
        return ()
    try:
        with os.scandir(directory) as entries:
            names = sorted(entry.name for entry in islice(entries, _RELEASES_LIMIT + 1))
        if len(names) > _RELEASES_LIMIT:
            raise FormatError(f"{job_releases_path(payload)} holds more than {_RELEASES_LIMIT} entries")
        releases = (_read_release(directory, name, payload) for name in names if not name.startswith("."))
        return tuple(release for release in releases if release is not None)
    finally:
        os.close(directory)


def read_job_release(payload: str | os.PathLike[str], path: str) -> Release | None:
    """Read the release record of one payload-relative path, opening only its own record file.

    :param payload: The job payload directory.
    :param path: The payload-relative path of the released file.
    :return: The record, or ``None`` when there is none (or it is unsafe or malformed, with a warning).
    """

    directory = _open_releases_readonly(payload)
    if directory is None:
        return None
    try:
        return _read_release(directory, _release_name(path), payload)
    finally:
        os.close(directory)


def _release_document(
    seal: Seal, *, path: str, size: int, sha256: str, released_to: Mapping[str, str], keys: Sequence[SealKey]
) -> tuple[Release, bytes]:
    """Sign one release body amending *seal* and return the record with its stored bytes."""

    body: dict[str, object] = {
        "kind": "release",
        "amends": seal.body_sha256,
        "path": path,
        "size": size,
        "sha256": sha256,
        "released_to": {"job_key": released_to["job_key"], "path": released_to["path"]},
        "at": utc_now(),
    }
    body_sha256, signatures = sign_seal_body(body, keys)
    document = {
        "format": _RELEASE_FORMAT,
        "version": 1,
        "body": body,
        "body_sha256": body_sha256,
        "signatures": signatures,
    }
    return _parse_release(document), json_bytes(document) + b"\n"


def _open_releases(payload: str | os.PathLike[str], durable: bool) -> int:
    """Open (creating) a job's release directory without following a link, as a ``FormatError`` when unsafe."""

    try:
        return _fs.open_dir_under(
            Path(payload), f"{JOB_STATE_DIRECTORY}/{_RELEASES_DIRECTORY}", create=True, mode=0o755, durable=durable
        )
    except _fs.UnsafePath as exc:
        raise FormatError(str(exc)) from exc


def record_job_release(
    workspace: Workspace,
    payload: str | os.PathLike[str],
    *,
    path: str,
    size: int,
    sha256: str,
    released_to: Mapping[str, str],
    keys: Sequence[SealKey],
    seal: Seal | None = None,
    replace: bool = False,
) -> Release:
    """Create the release record of one file of a sealed job, amending its seal for a file moved out of it.

    The record is created exclusively before the file moves (it is the intent
    and the claim): a second mover of the same path finds it and is refused,
    and a record whose file is still in place changes nothing. The seal itself
    is never rewritten, so workspace and project seals binding its digest stay
    valid.

    :param workspace: The workspace holding the job (its durability setting is used).
    :param payload: The sealed job's payload directory.
    :param path: The payload-relative path of the released file.
    :param size: The file size, as the seal records it.
    :param sha256: The file's SHA-256, as the seal records it.
    :param released_to: ``{"job_key", "path"}``: the receiving job and the file's payload-relative path there.
    :param keys: The signing keys; empty for an unsigned record, exactly as for a seal.
    :param seal: The job's seal, already validated by the caller; read from the payload when not given.
    :param replace: Replace the path's record (a stale one, or one naming another key) in one rename.
    :return: The created record.
    :raises FileExistsError: If the path already has a release record (it is released or being moved).
    :raises httk.workflow.errors.FormatError: If the job has no seal, or its seal or release directory is unusable.
    """

    if seal is None:
        data = _job_seal_bytes(payload)
        if data is None:
            raise FormatError(f"{payload} has no seal to amend")
        try:
            seal = _parse_job_seal(data, job_seal_path(payload))
        except ValueError as exc:
            raise FormatError(str(exc)) from exc
    release, data = _release_document(seal, path=path, size=size, sha256=sha256, released_to=released_to, keys=keys)
    directory = _open_releases(payload, workspace.durable)
    try:
        target = _fs.anchored(directory, _release_name(path))
        if replace:
            _fs.write_file(target, data, durable=workspace.durable)
        else:
            os.close(_fs.create_exclusive(target, data, durable=workspace.durable))
    finally:
        os.close(directory)
    return release


def _withdraw_job_release(workspace: Workspace, payload: Path, release: Release) -> None:
    """Remove one record again (a move rolled back): one unlink, only while the record is still *release*."""

    directory = _open_releases(payload, workspace.durable)
    try:
        # ponytail: read-compare-unlink is not atomic; a mover replacing this record in between loses it.
        current = _read_release(directory, _release_name(release.path), payload)
        if current is not None and current.body_sha256 == release.body_sha256:
            _fs.remove_file(_fs.anchored(directory, _release_name(release.path)), durable=workspace.durable)
    finally:
        os.close(directory)


def _signed_body_problem(
    body: Mapping[str, object], body_sha256: str, signatures: Sequence[Mapping[str, object]]
) -> str | None:
    """Return why a signed body is unusable, or ``None``: an unsigned body only needs its digest to match.

    Any signature that is present must verify, as for a seal; trust in the signer is not required.
    """

    if signatures:
        verification = verify_signed_body(json_bytes(body), body_sha256, signatures)
        return None if verification.valid else verification.reason
    # Signing with no keys is the body's own (domain-separated) digest.
    return None if sign_seal_body(dict(body), ())[0] == body_sha256 else "the body does not match its recorded digest"


def valid_job_releases(payload: str | os.PathLike[str], seal: Seal, paths: Iterable[str]) -> dict[str, Release]:
    """Return the release records that validly cover the missing files *paths* of *seal*, by path.

    Only the record files of *paths* are opened; the release directory is never
    listed, so nothing else in it affects validation. A record is valid when
    its body digest checks, it amends exactly *seal* (``amends`` is its
    ``body_sha256``), and its ``sha256`` and ``size`` are those *seal* records
    for the path. Against a signed seal it must carry a signature that
    verifies and was made by a key that also signed the seal, and every other
    signature it carries must verify; against an unsigned seal an unsigned
    record is accepted. A record whose file is present in the payload covers
    nothing, so a stale record (a move that never happened) changes nothing.

    :param payload: The job payload directory.
    :param seal: The job's seal.
    :param paths: The payload-relative paths to look up, normally the sealed files that are missing.
    :return: The valid records by released path.
    """

    recorded = {str(record.get("path")): record for record in seal.records if record.get("type") == "file"}
    wanted = [
        path
        for path in dict.fromkeys(paths)
        if path in recorded and not os.path.lexists(Path(payload).joinpath(*PurePosixPath(path).parts))
    ]
    if not wanted:
        return {}
    sealed_by = set(verify_signed_body(seal.body_bytes, seal.body_sha256, seal.signatures).signers)
    valid: dict[str, Release] = {}
    for path in wanted:
        release = read_job_release(payload, path)
        if release is None:
            continue
        record = recorded[path]
        problem = _signed_body_problem(release.body, release.body_sha256, release.signatures)
        if problem is None and seal.signatures:
            signers = verify_signed_body(json_bytes(release.body), release.body_sha256, release.signatures).signers
            if not set(signers) & sealed_by:
                problem = "no key that signed the seal signed it"
        if problem is None and release.amends != seal.body_sha256:
            problem = "it amends another seal"
        elif problem is None and (record.get("sha256"), record.get("size")) != (release.sha256, release.size):
            problem = "it does not match the sealed file"
        if problem is not None:
            _LOGGER.info("%s: the release record of %s is not valid: %s", payload, path, problem)
            continue
        valid[path] = release
    return valid


def _request(workspace: Workspace, ref: JobRef, action: str) -> str | None:
    from .removal import request_now  # removal executes requests with this module's seal_payload

    with _kernel.register_owner(workspace, kind="cli", label=f"job {action}", allocation=None, advertised={}) as owner:
        return request_now(workspace, owner, ref, action, f"job {action}")


def seal_job(workspace: Workspace, ref: JobRef) -> str | None:
    """Seal a succeeded job that has no seal (a repair verb): a ``seal`` request, applied now when unowned.

    :param workspace: The workspace.
    :param ref: The job.
    :return: ``None`` when the job is sealed now, otherwise why not (yet).
    """

    return _request(workspace, ref, "seal")


def unseal_job(workspace: Workspace, ref: JobRef) -> str | None:
    """Release a succeeded job from its protection (removing its seal), so ``job delete`` applies.

    :param workspace: The workspace.
    :param ref: The job.
    :return: ``None`` when the job is released now, otherwise why not (yet).
    """

    return _request(workspace, ref, "unseal")


# -- workspace seals ---------------------------------------------------------


def workspace_jobs(workspace: Workspace) -> list[tuple[JobRef, PurePosixPath]]:
    """List every job of a workspace (owned ones included) with its placement, ordered by job key.

    Interactive use: the walk visits every placement and every owner.

    :param workspace: The workspace.
    :return: The references and placements.
    """

    found: list[tuple[JobRef, PurePosixPath]] = []
    for state in _kernel.UNOWNED_STATES:
        found += [(ref, ref.placement or PurePosixPath()) for ref in _kernel.list_jobs(workspace, state)]
    owned = workspace.jobs / _kernel.OWNED
    for owner_id in sorted(os.listdir(owned)) if owned.is_dir() else []:
        for name in sorted(os.listdir(owned / owner_id)) if (owned / owner_id).is_dir() else []:
            try:
                ref = JobRef.from_path(owned / owner_id / name, state=_kernel.OWNED, owner_id=owner_id)
                found.append((ref, JobDefinition.from_path(ref.path / "job.json").placement))
            except (FormatError, OSError):
                continue
    return sorted(found, key=lambda item: item[0].job_key)


def unsealed_jobs(workspace: Workspace) -> list[JobRef]:
    """Return the jobs of a workspace that carry no seal, ordered by job key.

    :param workspace: The workspace to inspect.
    :return: The references.
    """

    unsealed: list[JobRef] = []
    for ref, _placement in workspace_jobs(workspace):
        try:
            if job_seal_digest(ref.path) is None:
                unsealed.append(ref)
        except FormatError:
            unsealed.append(ref)
    return unsealed


def require_cli_modifiable(workspace: Workspace) -> None:
    """The CLI guard: refuse a modifying command while the workspace or its project is sealed.

    Managers and jobs never check it: a workspace seal is a snapshot, not a freeze.

    :param workspace: The workspace.
    :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
    """

    if is_workspace_sealed(workspace):
        raise SealedError("the workspace is sealed; `httk workspace unseal` it first")
    project = discover_project(workspace.root)
    if project is not None and is_project_sealed(project):
        raise SealedError(f"project at {project} is sealed; unseal it first")


def seal_workspace(workspace: Workspace, *, keys: SealKeys | None = None) -> tuple[Path, list[JobRef]]:
    """Seal a workspace: a snapshot of the job seals present now (no waiting, no sealing of other jobs).

    Every job is recorded with the digest of its seal; a job without one
    (unfinished, failed, cancelled, opted out) is recorded with ``None`` and
    returned so the caller can list it. Unsigned when no key resolves.

    :param workspace: The workspace.
    :param keys: The signing keys, or ``None`` for the workspace's ``seal.keys``.
    :return: The workspace seal path and the jobs without a seal.
    :raises httk.workflow.errors.SealedError: If the workspace is already sealed.
    """

    if is_workspace_sealed(workspace):
        raise SealedError("the workspace is already sealed")
    records: list[dict[str, object]] = []
    unsealed: list[JobRef] = []
    for ref, placement in workspace_jobs(workspace):
        try:
            digest = job_seal_digest(ref.path)
        except FormatError:
            digest = None
        if digest is None:
            unsealed.append(ref)
        records.append(
            {
                "job_id": ref.job_id,
                "job_key": ref.job_key,
                "placement": placement_text(placement),
                "seal_sha256": digest,
            }
        )
    if keys is None:
        try:
            keys = default_workspace_keys(workspace)
        except SealError:
            keys = None
    subject = {"workspace_id": workspace.workspace_id, "unsealed_jobs": len(unsealed)}
    body = build_seal_body("workspace", subject, records)
    body_sha256, signatures = sign_seal_body(body, () if keys is None else keys.keys)
    data = json_bytes({**body, "body_sha256": body_sha256, "signatures": signatures}) + b"\n"
    path = workspace_seal_path(workspace)
    _fs.write_file(_fs.loc(path), data, durable=workspace.durable)
    return path, unsealed


def unseal_workspace(workspace: Workspace) -> None:
    """Remove a workspace's seal, refusing while its project is sealed.

    :param workspace: The workspace to unseal.
    :raises httk.workflow.errors.SealedError: If the enclosing project is sealed.
    """

    project = discover_project(workspace.root)
    if project is not None and is_project_sealed(project):
        raise SealedError("cannot unseal a workspace while its project is sealed; unseal the project first")
    _fs.remove_file(_fs.loc(workspace_seal_path(workspace)), durable=workspace.durable)


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
    job inside a workspace and for a free-standing job directory. A sealed
    file that is missing because it was moved to another job is not a
    discrepancy while a valid release record covers it (:func:`valid_job_releases`).

    :param payload: The job payload directory.
    :param trusted_keys: Trust anchors to classify the signers against.
    :param expected_roles: Signing roles the seal is expected to carry.
    :return: The verdict, including any payload discrepancies; a symlinked or
        special ``.httk-job`` or ``seal.json`` is an ``invalid`` discrepancy.
    """

    payload = Path(payload)
    path = job_seal_path(payload)
    try:
        data = _job_seal_bytes(payload)
    except FormatError as exc:
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
    if data is None:
        return SealVerification(
            False,
            INVALID,
            f"job seal is absent: {path}",
            (),
            tuple(expected_roles),
            (Discrepancy(payload.name, "missing"),),
        )
    seal = _parse_job_seal(data, path)
    # verify_seal's signature check, on the seal read once through the descriptor.
    base = verify_signed_body(
        seal.body_bytes, seal.body_sha256, seal.signatures, trusted_keys=trusted_keys, expected_roles=expected_roles
    )
    discrepancies = diff_records(list(seal.records), payload_file_records(payload))
    if any(item.kind == "missing" for item in discrepancies):
        # A file moved out to another job is covered by a valid release record, not missing.
        missing = [item.path for item in discrepancies if item.kind == "missing"]
        released = valid_job_releases(payload, seal, missing)
        discrepancies = [item for item in discrepancies if not (item.kind == "missing" and item.path in released)]
    return _combine(base, discrepancies)


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
    present = {ref.job_key: ref for ref, _placement in workspace_jobs(workspace)}
    discrepancies: list[Discrepancy] = []
    # Drift since the snapshot: recorded jobs gone or changed, and jobs present but not recorded.
    for job_key in sorted(set(recorded) | set(present)):
        if job_key not in present:
            discrepancies.append(Discrepancy(job_key, "missing_job"))
        elif job_key not in recorded:
            discrepancies.append(Discrepancy(job_key, "unsealed"))
        else:
            try:
                digest = job_seal_digest(present[job_key].path)
            except FormatError:
                digest = None
            expected = recorded[job_key]["seal_sha256"]
            if digest is None and expected is not None:
                discrepancies.append(Discrepancy(job_key, "missing"))
            elif digest != expected:
                discrepancies.append(Discrepancy(job_key, "mismatch"))
    return _combine(base, discrepancies)


def recorded_job_path(workspace: Workspace, record: Mapping[str, object]) -> Path | None:
    """Return where a job a workspace seal records is now, or ``None`` when it is gone.

    :param workspace: The workspace.
    :param record: One record of the workspace seal.
    :return: The job directory, or ``None``.
    """

    placement = PurePosixPath(str(record["placement"]))
    ref = _kernel.locate(workspace, str(record["job_id"]), placement_hint=placement)
    return None if ref is None else ref.path


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
                located = recorded_job_path(workspace, record)
                if located is None or record["seal_sha256"] is None:
                    continue  # a missing job is a discrepancy of the workspace verdict; an unsealed one has no seal
                entries.append(
                    verify_job_seal(located, trusted_keys=trusted_keys).as_entry("job", str(record["job_key"]))
                )
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
