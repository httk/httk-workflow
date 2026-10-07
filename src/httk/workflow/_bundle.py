"""Transfer bundle format v3 and the one function every import trusts it through.

A bundle is the root job's payload directory carrying a transfer envelope::

    <root payload>/.httk-transfer/
    ├── manifest.json
    ├── markers/<job_id>            one empty regular file per job in the bundle
    ├── runners/<runner_path>       the workspace runners the jobs pin
    └── tree/<placement>/<job_key>/ member payloads; the empty placement is tree/<job_key>/

:func:`verify_bundle` is the only trust gate: every import (the exchange inbox,
``httk job adopt``, an addressed receive, taking back a held export) runs it
before the bundle's envelope moves. It refuses *content* only; an ``OSError``
propagates and leaves the bundle where it is for a retry.

The payload digest, runner digest and seal digest helpers, and the adoption
walk, live here and are shared with :mod:`httk.workflow.transfers`.
"""

import copy
import errno
import hashlib
import json
import os
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

from httk.core.digests import sha256_file, tree_digest

from .errors import FormatError
from .models import (
    _MAXIMUM_JOB_DOCUMENT_BYTES,
    CARRIED_STATE_MEMBERS,
    CORE_PROFILE,
    QUIESCENT_KINDS,
    STATE_KINDS,
    TRANSFER_DIRECTORY,
    JobDefinition,
    _read_regular_file,
    canonical_uuid,
    check_job_placement,
    is_payload_private,
    parse_job_key,
    parse_placement_text,
    placement_text,
    validate_runner_path,
    validate_sha256,
)
from .seals import job_seal_path

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .workspace import Workspace

__all__ = [
    "BUNDLE_SOURCES",
    "EXCHANGE_PRIOR_STATE_MEMBERS",
    "EXCHANGE_SOURCES",
    "TRANSFER_CORE_PROFILE",
    "TRANSFER_DIRECTORY",
    "TRANSFER_FORMAT",
    "TRANSFER_FORMAT_VERSION",
    "TRANSFER_MANIFEST",
    "TRANSFER_MARKERS",
    "TRANSFER_RUNNERS",
    "TRANSFER_TREE",
    "BundleManifest",
    "BundleMember",
    "BundleRunner",
    "BundleSource",
    "VerifiedBundle",
    "VerifiedJob",
    "check_bundle",
    "verified_jobs",
    "verify_bundle",
]

TRANSFER_MANIFEST = "manifest.json"
#: Where the envelope carries one empty marker file per job, named by job id.
TRANSFER_MARKERS = "markers"
#: Where the envelope carries the workspace runners its jobs pin.
TRANSFER_RUNNERS = "runners"
#: Where the envelope carries the member payloads of a tree.
TRANSFER_TREE = "tree"
TRANSFER_FORMAT = "httk-workflow-detached-transfer"
#: Version 2 widened the payload digest: it now also pins the executable bit of
#: every regular file and the target of every symlink. Version 3 belongs to the
#: v3 workspace format; earlier bundles are refused.
TRANSFER_FORMAT_VERSION = 3
#: The core profile a version 3 bundle declares.
TRANSFER_CORE_PROFILE = CORE_PROFILE
#: Domain separation of the payload digest, so a digest computed by an older
#: rule can never collide with one computed by the current rule.
_PAYLOAD_DIGEST_DOMAIN = b"httk-workflow-transfer-payload-v2\0"

# The largest transfer manifest a reader accepts; a manifest is small.
_MANIFEST_BYTES = 1024 * 1024
# The bounds of the walk that vets a job directory before it is adopted.
_ADOPT_MAX_ENTRIES = 1_000_000
_ADOPT_MAX_DEPTH = 256
#: How many entries a walk or digest visits between two calls of its pacer.
PACE_STRIDE = 1000

#: Where a bundle being verified comes from. ``"incoming"`` bundles are
#: addressed to this workspace; the others are ejected bundles addressed to none.
#: ``"exchange_inbox"`` (adopted by the exchange pass) and ``"exchange_outbox"``
#: (an operator's ``job adopt`` of this workspace's outbox entry) are
#: client-written content: both get the exchange checks.
type BundleSource = Literal["exchange_inbox", "exchange_outbox", "adopt", "incoming", "export"]
BUNDLE_SOURCES: frozenset[str] = frozenset({"exchange_inbox", "exchange_outbox", "adopt", "incoming", "export"})
#: The sources whose bundles a client of the exchange may have written.
EXCHANGE_SOURCES: frozenset[str] = frozenset({"exchange_inbox", "exchange_outbox"})
_ADDRESSED_SOURCES = frozenset({"incoming"})

#: The ``prior_state`` members an exchange-inbox bundle may carry into this
#: workspace; every other member is dropped. Derived (October 6 2026, plan 4.5
#: "Allowlist") by reading every ``Workspace.transition`` call that can target a
#: quiescent kind (``submitted``, ``ready``, ``waiting``, ``paused``, ``failed``,
#: ``succeeded``, ``cancelled``) and listing the members it writes:
#: ``manager._transition`` (deferred pause: ``operator``, ``operator_reason``,
#: ``request_id``, ``reason``, ``process``); ``_manager_scheduling.register_submissions``
#: (``job_digest``, ``failure``); ``manager._claim`` / ``_release`` /
#: ``_fail_unloadable_job``; ``_manager_commit`` outcomes, ``advance`` and ``retry``
#: (``next_step``, ``join``, ``pause``, ``failure``, ``previous_attempt_id``,
#: ``unclean_restart``, ``unsafe_persistent_takeover``, ``takeover_evidence``,
#: ``process``); ``manager._join_child_unresolved`` (``join_unresolved``);
#: ``_manager_joins`` (``failure``); ``_manager_requests`` (``operator_key``,
#: ``revival_hazard``, the preserved ``pause_requested``); and the cancellation
#: paths to ``cancelled`` (``cancellation`` and ``manager._CANCELLING_MEMBERS``:
#: ``attempt_control``, ``manager_id``, ``writer_id``, ``lease_seconds``,
#: ``workdir``, ``started_at``), plus :data:`~httk.workflow.models.CARRIED_STATE_MEMBERS`.
#: ``transfer``, ``origin``, ``outgoing``, ``prior_kind`` and ``prior_state`` are
#: never accepted from a client. A new member written into a quiescent frame
#: must be added here, or it is dropped at exchange adoption.
EXCHANGE_PRIOR_STATE_MEMBERS: frozenset[str] = frozenset(
    {
        *CARRIED_STATE_MEMBERS,
        "attempt_control",
        "cancellation",
        "failure",
        "job_digest",
        "join",
        "join_unresolved",
        "lease_seconds",
        "manager_id",
        "next_step",
        "operator",
        "operator_key",
        "operator_reason",
        "pause",
        "previous_attempt_id",
        "process",
        "reason",
        "request_id",
        "revival_hazard",
        "started_at",
        "takeover_evidence",
        "unclean_restart",
        "unsafe_persistent_takeover",
        "workdir",
        "writer_id",
    }
) - {"transfer", "origin", "outgoing", "prior_kind", "prior_state"}

_ROOT_KEYS = frozenset(
    {
        "format",
        "format_version",
        "core_profile",
        "transfer_id",
        "source_workspace_id",
        "destination_workspace_id",
        "destination_remote",
        "destination_placement",
        "sealed_at",
        "job_id",
        "job_key",
        "source_placement",
        "payload_sha256",
        "seal_sha256",
        "runners",
        "prior_kind",
        "prior_state",
        "priority",
        "source_generation",
        "members",
    }
)
_MEMBER_KEYS = frozenset(
    {
        "job_id",
        "job_key",
        "placement",
        "parent_job_id",
        "payload_sha256",
        "seal_sha256",
        "prior_kind",
        "prior_state",
        "priority",
        "source_generation",
    }
)
_ENVELOPE_ENTRIES = frozenset({TRANSFER_MANIFEST, TRANSFER_MARKERS, TRANSFER_RUNNERS, TRANSFER_TREE})
_MAXIMUM_GENERATION = (1 << 64) - 1
_MAXIMUM_NS = (1 << 63) - 1


# ---------------------------------------------------------------------------
# The adoption walk and the digests (shared with transfers.py)
# ---------------------------------------------------------------------------


def _excluded_from_bundle(name: str) -> bool:
    """Report whether one top-level payload entry stays out of the digest.

    The transfer directory describes the bundle rather than the job, and the
    runner-private entries of a payload — attempt control directories and job
    state — are excluded from every payload digest, so a job that ran before it
    was detached digests exactly like one that never did.
    """

    return name == TRANSFER_DIRECTORY or is_payload_private(name)


def _contained_symlink_target(payload: Path, entry: Path) -> str:
    """Return the target of one payload symlink, refusing any that escapes.

    A symlink is transferred as its literal target string, exactly as a signed
    project manifest records one, because that is what makes the link mean the
    same thing at the destination. That only holds for a link that stays inside
    the payload: an absolute target names a path of the source machine, and a
    relative target climbing out of the payload resolves against whatever
    happens to sit beside the payload at the destination. Both are refused by
    name rather than transferred into a different meaning.
    """

    target = os.readlink(entry)
    if PurePosixPath(target).is_absolute():
        raise FormatError(
            f"transfer payload rejects the absolute symlink {entry.name} -> {target}: "
            f"an absolute target names a path of the source machine ({entry})"
        )
    parts = list(entry.parent.relative_to(payload).parts)
    for part in PurePosixPath(target).parts:
        if part in {"", "."}:
            continue
        if part != "..":
            parts.append(part)
            continue
        if not parts:
            raise FormatError(
                f"transfer payload rejects the escaping symlink {entry.name} -> {target}: "
                f"the target resolves outside the payload ({entry})"
            )
        parts.pop()
    return target


def _refuse_unsafe_entries(bundle: Path, *, pace: Callable[[], None] | None = None) -> None:
    """Refuse a job directory holding anything but files, directories and contained symlinks.

    The walk only uses ``lstat``/``readlink``, before any file of the directory
    is opened, so a planted FIFO or device cannot hang or affect an adoption and
    a symlink cannot lead outside it, also below the undigested ``logs/``,
    ``attempts/`` and ``.httk-transfer/``. A relative symlink must stay inside
    the directory both lexically and once every link on its way is resolved.
    A regular file with more than one link is refused too: its other name may
    sit anywhere on the filesystem, so adopting it would hand the job a file
    that something outside the directory still writes or reads. The walk is
    bounded to 1,000,000 entries and a nesting depth of 256.

    :param bundle: The job directory, which must itself be a real directory.
    :param pace: Called every :data:`PACE_STRIDE` entries (a manager's heartbeat pacer).
    :raises httk.workflow.errors.FormatError: Naming the first refused entry, or if the directory
        exceeds the walk bounds.
    :raises FileNotFoundError: If the directory does not exist.
    """

    if not stat.S_ISDIR(os.lstat(bundle).st_mode):
        raise FormatError(f"job directory {bundle} is not a directory (a symlink or special file)")
    real = os.path.realpath(bundle)
    pending = [(bundle, 1)]
    visited = 0
    while pending:
        directory, depth = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                visited += 1
                if visited > _ADOPT_MAX_ENTRIES:
                    raise FormatError(f"job directory {bundle} holds more than {_ADOPT_MAX_ENTRIES} entries")
                if pace is not None and visited % PACE_STRIDE == 0:
                    pace()
                path = Path(entry.path)
                if entry.is_symlink():
                    _contained_symlink_target(bundle, path)
                    if os.path.commonpath([real, os.path.realpath(path)]) != real:
                        raise FormatError(
                            f"job directory rejects the symlink {path.relative_to(bundle)}: it resolves outside it"
                        )
                elif entry.is_dir(follow_symlinks=False):
                    if depth >= _ADOPT_MAX_DEPTH:
                        raise FormatError(
                            f"job directory rejects {path.relative_to(bundle)}: "
                            f"it nests deeper than {_ADOPT_MAX_DEPTH} levels"
                        )
                    pending.append((path, depth + 1))
                elif not entry.is_file(follow_symlinks=False):
                    raise FormatError(f"job directory rejects the special entry {path.relative_to(bundle)}")
                elif entry.stat(follow_symlinks=False).st_nlink > 1:
                    raise FormatError(
                        f"job directory rejects the hard-linked file {path.relative_to(bundle)}: "
                        "hard links are not accepted in bundles"
                    )


def _payload_digest(payload: Path, *, pace: Callable[[], None] | None = None) -> str:
    """Digest one payload tree: names, kinds, content, exec bits, and link targets.

    The executable bit is part of the digest because it is part of what a
    payload *is*: a runner or helper script that arrives without it does not
    run, so a transfer that dropped it would be a silent corruption rather than
    a detected one.
    """

    digest = hashlib.sha256()
    digest.update(_PAYLOAD_DIGEST_DOMAIN)
    entries = [
        item
        for item in payload.rglob("*")
        if item.relative_to(payload).parts and not _excluded_from_bundle(item.relative_to(payload).parts[0])
    ]
    for index, entry in enumerate(sorted(entries, key=lambda item: item.relative_to(payload).as_posix()), 1):
        if pace is not None and index % PACE_STRIDE == 0:
            pace()
        relative = entry.relative_to(payload).as_posix().encode("utf-8")
        mode = entry.lstat().st_mode
        if stat.S_ISLNK(mode):
            target = _contained_symlink_target(payload, entry).encode("utf-8")
            digest.update(b"L\0" + relative + b"\0" + target + b"\0")
        elif stat.S_ISDIR(mode):
            digest.update(b"D\0" + relative + b"\0")
        elif stat.S_ISREG(mode):
            executable = b"x" if mode & 0o111 else b"-"
            digest.update(b"F\0" + relative + b"\0" + executable + b"\0" + sha256_file(entry).encode("ascii") + b"\0")
        else:
            raise FormatError(f"transfer payload rejects special entry: {entry}")
    return digest.hexdigest()


def _runner_digest(path: Path) -> str:
    """Digest one published runner file or tree with the workspace rule."""

    if path.is_symlink() or not (path.is_file() or path.is_dir()):
        raise FormatError(f"referenced workspace runner is not a regular file or tree: {path}")
    return sha256_file(path) if path.is_file() else tree_digest(path)


def _manifest_runners(manifest: Mapping[str, Any]) -> list[dict[str, str]]:
    """Validate and return the ``runners`` list of one transfer manifest."""

    raw = manifest.get("runners", [])
    if not isinstance(raw, list):
        raise FormatError("transfer manifest runners must be an array")
    result: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise FormatError("transfer manifest runner must be an object")
        relative = validate_runner_path(item.get("path"), "workspace")
        result.append({"path": relative.as_posix(), "sha256": validate_sha256(item.get("sha256"), "runner.sha256")})
    return result


def _payload_seal_sha256(payload: Path) -> str | None:
    """Return the digest of the seal a payload carries, or ``None`` when unsealed.

    The seal lives inside the payload's ``.httk-job/``, which the payload digest
    excludes, so the manifest pins it separately: a bundle must arrive exactly as
    sealed, or exactly as unsealed, as it left.
    """

    path = job_seal_path(payload)
    return sha256_file(path) if path.is_file() else None


# ---------------------------------------------------------------------------
# The manifest model
# ---------------------------------------------------------------------------


def _exact_keys(value: object, keys: frozenset[str], name: str) -> Mapping[str, Any]:
    """Require a JSON object with exactly *keys*."""

    if not isinstance(value, Mapping):
        raise FormatError(f"{name} must be an object")
    present = set(value)
    missing = sorted(keys - present)
    unknown = sorted(str(key) for key in present - keys)
    if missing:
        raise FormatError(f"{name} lacks {', '.join(missing)}")
    if unknown:
        raise FormatError(f"{name} has unknown members {', '.join(unknown)}")
    return value


def _integer(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise FormatError(f"{name} must be an integer from 0 through {maximum}")
    return value


def _optional_sha256(value: object, name: str) -> str | None:
    return None if value is None else validate_sha256(value, name)


def _job_key_of(value: object, job_id: str, name: str) -> str:
    """Require a job key that parses and carries *job_id*."""

    if not isinstance(value, str):
        raise FormatError(f"{name} must be a string")
    if parse_job_key(value)[1] != job_id:
        raise FormatError(f"{name} {value!r} does not carry the job id {job_id}")
    return value


def _job_placement(value: object, name: str) -> PurePosixPath:
    placement = parse_placement_text(value, name)
    check_job_placement(placement)
    return placement


def _prior_kind(value: object, name: str) -> str:
    # The type first: an array or object is unhashable, and must be refused, not raise TypeError.
    if not isinstance(value, str) or value not in QUIESCENT_KINDS:
        raise FormatError(f"{name} must be a quiescent state kind, not {value!r}")
    return value


def _prior_state(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise FormatError(f"{name} must be an object")
    return MappingProxyType(copy.deepcopy(dict(value)))


@dataclass(frozen=True)
class BundleRunner:
    """One workspace runner a bundle carries below ``runners/``.

    :param path: The runner's store path.
    :param sha256: The runner's digest.
    """

    path: PurePosixPath
    sha256: str

    @classmethod
    def from_mapping(cls, value: object) -> "BundleRunner":
        """Parse one ``runners`` entry strictly.

        :param value: The JSON object.
        :return: The runner entry.
        :raises httk.workflow.errors.FormatError: If the entry is malformed.
        """

        mapping = _exact_keys(value, frozenset({"path", "sha256"}), "transfer manifest runner")
        return cls(
            validate_runner_path(mapping["path"], "workspace"), validate_sha256(mapping["sha256"], "runner.sha256")
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the manifest JSON form.

        :return: The ``runners`` entry.
        """

        return {"path": self.path.as_posix(), "sha256": self.sha256}


@dataclass(frozen=True)
class BundleMember:
    """One member job of a tree bundle, as its manifest records it.

    :param job_id: The member's job id.
    :param job_key: The member's job key.
    :param placement: The member's placement, here and in the source.
    :param parent_job_id: The job id of the member's parent: the root or an earlier member.
    :param payload_sha256: The member payload digest.
    :param seal_sha256: The digest of the member's seal, or ``None`` when unsealed.
    :param prior_kind: The quiescent kind the member had before it was fenced.
    :param prior_state: The member's state frame before it was fenced (read-only).
    :param priority: The member's marker priority.
    :param source_generation: The member's state generation in the source.
    """

    job_id: str
    job_key: str
    placement: PurePosixPath
    parent_job_id: str
    payload_sha256: str
    seal_sha256: str | None
    prior_kind: str
    prior_state: Mapping[str, Any]
    priority: int
    source_generation: int

    @classmethod
    def from_mapping(cls, value: object, name: str = "transfer manifest member") -> "BundleMember":
        """Parse one ``members`` entry strictly.

        :param value: The JSON object.
        :param name: The name used in error messages.
        :return: The member.
        :raises httk.workflow.errors.FormatError: If the entry is malformed.
        """

        mapping = _exact_keys(value, _MEMBER_KEYS, name)
        job_id = canonical_uuid(mapping["job_id"], f"{name}.job_id")
        return cls(
            job_id=job_id,
            job_key=_job_key_of(mapping["job_key"], job_id, f"{name}.job_key"),
            placement=_job_placement(mapping["placement"], f"{name}.placement"),
            parent_job_id=canonical_uuid(mapping["parent_job_id"], f"{name}.parent_job_id"),
            payload_sha256=validate_sha256(mapping["payload_sha256"], f"{name}.payload_sha256"),
            seal_sha256=_optional_sha256(mapping["seal_sha256"], f"{name}.seal_sha256"),
            prior_kind=_prior_kind(mapping["prior_kind"], f"{name}.prior_kind"),
            prior_state=_prior_state(mapping["prior_state"], f"{name}.prior_state"),
            priority=_integer(mapping["priority"], f"{name}.priority", 999),
            source_generation=_integer(mapping["source_generation"], f"{name}.source_generation", _MAXIMUM_GENERATION),
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the manifest JSON form.

        :return: The ``members`` entry.
        """

        return {
            "job_id": self.job_id,
            "job_key": self.job_key,
            "placement": placement_text(self.placement),
            "parent_job_id": self.parent_job_id,
            "payload_sha256": self.payload_sha256,
            "seal_sha256": self.seal_sha256,
            "prior_kind": self.prior_kind,
            "prior_state": copy.deepcopy(dict(self.prior_state)),
            "priority": self.priority,
            "source_generation": self.source_generation,
        }


@dataclass(frozen=True)
class BundleManifest:
    """The manifest of one version 3 bundle (``.httk-transfer/manifest.json``).

    :param transfer_id: The one transfer id of the bundle (plan decision 9).
    :param source_workspace_id: The workspace that sealed it.
    :param destination_workspace_id: The addressed workspace, or ``None`` for an ejected bundle.
    :param destination_remote: The destination's remote name, when addressed through one.
    :param destination_placement: Where the root is published.
    :param sealed_at: When the bundle was sealed, in integer UTC nanoseconds.
    :param job_id: The root job's id.
    :param job_key: The root job's key.
    :param source_placement: The root's placement in the source workspace.
    :param payload_sha256: The root payload digest.
    :param seal_sha256: The digest of the root's seal, or ``None`` when unsealed.
    :param runners: The workspace runners the bundle carries.
    :param prior_kind: The quiescent kind the root had before it was fenced.
    :param prior_state: The root's state frame before it was fenced (read-only).
    :param priority: The root's marker priority.
    :param source_generation: The root's state generation in the source.
    :param members: The tree members, top-down: each member's parent precedes it.
    """

    transfer_id: str
    source_workspace_id: str
    destination_workspace_id: str | None
    destination_remote: str | None
    destination_placement: PurePosixPath
    sealed_at: int
    job_id: str
    job_key: str
    source_placement: PurePosixPath
    payload_sha256: str
    seal_sha256: str | None
    runners: tuple[BundleRunner, ...]
    prior_kind: str
    prior_state: Mapping[str, Any]
    priority: int
    source_generation: int
    members: tuple[BundleMember, ...]

    @classmethod
    def from_mapping(cls, value: object) -> "BundleManifest":
        """Parse a manifest strictly: exact members, canonical ids, valid placements, a well-formed tree.

        :param value: The decoded ``manifest.json`` object.
        :return: The manifest.
        :raises httk.workflow.errors.FormatError: If the manifest is not a valid version 3 manifest.
        """

        mapping = _exact_keys(value, _ROOT_KEYS, "transfer manifest")
        if (
            mapping["format"] != TRANSFER_FORMAT
            or mapping["format_version"] != TRANSFER_FORMAT_VERSION
            or mapping["core_profile"] != TRANSFER_CORE_PROFILE
        ):
            raise FormatError(
                f"unsupported transfer bundle: expected {TRANSFER_FORMAT} version {TRANSFER_FORMAT_VERSION} "
                f"({TRANSFER_CORE_PROFILE})"
            )
        job_id = canonical_uuid(mapping["job_id"], "transfer manifest job_id")
        destination = mapping["destination_workspace_id"]
        remote = mapping["destination_remote"]
        if remote is not None and (not isinstance(remote, str) or not remote):
            raise FormatError("transfer manifest destination_remote must be a nonempty string or null")
        runners_raw = mapping["runners"]
        if not isinstance(runners_raw, list):
            raise FormatError("transfer manifest runners must be an array")
        runners: dict[PurePosixPath, BundleRunner] = {}
        for item in runners_raw:
            runner = BundleRunner.from_mapping(item)
            if runners.setdefault(runner.path, runner) != runner:
                raise FormatError(f"transfer manifest names runner {runner.path.as_posix()} with two digests")
        members_raw = mapping["members"]
        if not isinstance(members_raw, list):
            raise FormatError("transfer manifest members must be an array")
        members = tuple(
            BundleMember.from_mapping(item, f"transfer manifest member {index}")
            for index, item in enumerate(members_raw)
        )
        known = {job_id}
        for member in members:
            if member.job_id in known:
                raise FormatError(f"transfer manifest names job {member.job_id} more than once")
            if member.parent_job_id not in known:
                raise FormatError(
                    f"transfer manifest member {member.job_id} names parent {member.parent_job_id}, "
                    "which is neither the root nor an earlier member"
                )
            known.add(member.job_id)
        return cls(
            transfer_id=canonical_uuid(mapping["transfer_id"], "transfer manifest transfer_id"),
            source_workspace_id=canonical_uuid(mapping["source_workspace_id"], "transfer manifest source_workspace_id"),
            destination_workspace_id=(
                None
                if destination is None
                else canonical_uuid(destination, "transfer manifest destination_workspace_id")
            ),
            destination_remote=remote,
            destination_placement=_job_placement(
                mapping["destination_placement"], "transfer manifest destination_placement"
            ),
            sealed_at=_integer(mapping["sealed_at"], "transfer manifest sealed_at", _MAXIMUM_NS),
            job_id=job_id,
            job_key=_job_key_of(mapping["job_key"], job_id, "transfer manifest job_key"),
            source_placement=_job_placement(mapping["source_placement"], "transfer manifest source_placement"),
            payload_sha256=validate_sha256(mapping["payload_sha256"], "transfer manifest payload_sha256"),
            seal_sha256=_optional_sha256(mapping["seal_sha256"], "transfer manifest seal_sha256"),
            runners=tuple(runners.values()),
            prior_kind=_prior_kind(mapping["prior_kind"], "transfer manifest prior_kind"),
            prior_state=_prior_state(mapping["prior_state"], "transfer manifest prior_state"),
            priority=_integer(mapping["priority"], "transfer manifest priority", 999),
            source_generation=_integer(
                mapping["source_generation"], "transfer manifest source_generation", _MAXIMUM_GENERATION
            ),
            members=members,
        )

    def as_mapping(self) -> dict[str, object]:
        """Return the manifest JSON form, the exact document :meth:`from_mapping` accepts.

        :return: The ``manifest.json`` object.
        """

        return {
            "format": TRANSFER_FORMAT,
            "format_version": TRANSFER_FORMAT_VERSION,
            "core_profile": TRANSFER_CORE_PROFILE,
            "transfer_id": self.transfer_id,
            "source_workspace_id": self.source_workspace_id,
            "destination_workspace_id": self.destination_workspace_id,
            "destination_remote": self.destination_remote,
            "destination_placement": placement_text(self.destination_placement),
            "sealed_at": self.sealed_at,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "source_placement": placement_text(self.source_placement),
            "payload_sha256": self.payload_sha256,
            "seal_sha256": self.seal_sha256,
            "runners": [runner.as_mapping() for runner in self.runners],
            "prior_kind": self.prior_kind,
            "prior_state": copy.deepcopy(dict(self.prior_state)),
            "priority": self.priority,
            "source_generation": self.source_generation,
            "members": [member.as_mapping() for member in self.members],
        }

    @property
    def job_ids(self) -> tuple[str, ...]:
        """Return the job ids of the root and every member, top-down."""

        return (self.job_id, *(member.job_id for member in self.members))


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VerifiedJob:
    """The facts about one job of a verified bundle that an import acts on.

    :param job_id: The job id.
    :param job_key: The job key.
    :param placement: Where the job is published: the manifest's
        ``destination_placement`` for the root, the member's own placement otherwise.
    :param payload: The job's payload directory inside the verified bundle.
    :param envelope_relative: The member payload's path relative to the envelope
        (``tree/<placement>/<job_key>``), or ``None`` for the root.
    :param parent_job_id: The member's parent, or ``None`` for the root.
    :param payload_sha256: The verified payload digest.
    :param seal_sha256: The verified seal digest, or ``None`` when unsealed.
    :param prior_kind: The quiescent kind to publish the job in.
    :param prior_state: The prior state members to carry (filtered by
        :data:`EXCHANGE_PRIOR_STATE_MEMBERS` for an exchange-inbox bundle; read-only).
    :param priority: The marker priority.
    :param source_generation: The state generation in the source.
    """

    job_id: str
    job_key: str
    placement: PurePosixPath
    payload: Path
    envelope_relative: PurePosixPath | None
    parent_job_id: str | None
    payload_sha256: str
    seal_sha256: str | None
    prior_kind: str
    prior_state: Mapping[str, Any]
    priority: int
    source_generation: int

    @property
    def is_root(self) -> bool:
        """Return whether this is the bundle's root job."""

        return self.parent_job_id is None


@dataclass(frozen=True)
class VerifiedBundle:
    """A bundle that passed :func:`verify_bundle`, with the facts each import step needs.

    :param path: The verified bundle (root payload) directory.
    :param source: Where the bundle came from.
    :param manifest: The parsed manifest, exactly as the bundle carries it.
    :param jobs: Every job of the bundle, in job-id order (the order of per-job claims).
    """

    path: Path
    source: BundleSource
    manifest: BundleManifest
    jobs: tuple[VerifiedJob, ...]

    @property
    def transfer_id(self) -> str:
        """Return the bundle's transfer id."""

        return self.manifest.transfer_id

    @property
    def root(self) -> VerifiedJob:
        """Return the root job."""

        return self.job(self.manifest.job_id)

    @property
    def top_down(self) -> tuple[VerifiedJob, ...]:
        """Return the jobs root first, each member after its parent (the manifest order)."""

        return tuple(self.job(job_id) for job_id in self.manifest.job_ids)

    def job(self, job_id: str) -> VerifiedJob:
        """Return one job of the bundle.

        :param job_id: The job id.
        :return: The job's verified facts.
        :raises KeyError: If the bundle holds no such job.
        """

        for job in self.jobs:
            if job.job_id == job_id:
                return job
        raise KeyError(job_id)


def _real_directory(path: Path, name: str) -> None:
    """Require *path* to be a real directory by ``lstat``; an absent one is content, too."""

    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        raise FormatError(f"transfer bundle lacks {name}") from None
    if not stat.S_ISDIR(mode):
        raise FormatError(f"transfer bundle {name} is not a real directory")


def _read_bundle_manifest(envelope: Path) -> BundleManifest:
    """Read ``manifest.json`` bounded, without following a symlink, and parse it strictly."""

    path = envelope / TRANSFER_MANIFEST
    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        raise FormatError(f"transfer bundle lacks {TRANSFER_DIRECTORY}/{TRANSFER_MANIFEST}") from None
    if not stat.S_ISREG(mode):
        raise FormatError(f"transfer bundle {TRANSFER_DIRECTORY}/{TRANSFER_MANIFEST} is not a regular file")
    data = _read_regular_file(path, _MANIFEST_BYTES)
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise FormatError(f"transfer manifest {path} is not JSON: {exc}") from exc
    return BundleManifest.from_mapping(value)


def _check_exact_tree(root: Path, leaves: Mapping[PurePosixPath, bool], name: str) -> None:
    """Require *root* to hold exactly the given leaves and the real directories leading to them.

    :param root: The directory to check.
    :param leaves: Each expected leaf, mapped to whether it must be a real directory
        (``False``: a regular file or a real directory).
    :param name: The name used in error messages.
    """

    prefixes = {leaf.parents[index] for leaf in leaves for index in range(len(leaf.parts) - 1)}
    seen: set[PurePosixPath] = set()
    pending = [(root, PurePosixPath())]
    while pending:
        directory, relative = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                child = relative / entry.name
                is_dir = entry.is_dir(follow_symlinks=False)
                if child in leaves:
                    if not is_dir and (leaves[child] or not entry.is_file(follow_symlinks=False)):
                        raise FormatError(f"transfer bundle {name}/{child.as_posix()} has the wrong kind")
                    seen.add(child)
                elif child in prefixes and is_dir:
                    pending.append((Path(entry.path), child))
                else:
                    raise FormatError(f"transfer bundle holds the unexpected entry {name}/{child.as_posix()}")
    missing = sorted(leaf.as_posix() for leaf in set(leaves) - seen)
    if missing:
        raise FormatError(f"transfer bundle lacks {name}/{missing[0]}")


def _check_markers(markers: Path, job_ids: Sequence[str]) -> None:
    """Require exactly one empty regular file per job, named by its job id."""

    expected = set(job_ids)
    seen: set[str] = set()
    with os.scandir(markers) as entries:
        for entry in entries:
            if entry.name not in expected:
                raise FormatError(f"transfer bundle holds the unexpected marker {entry.name!r}")
            if not entry.is_file(follow_symlinks=False):
                raise FormatError(f"transfer bundle marker {entry.name} is not a regular file")
            if entry.stat(follow_symlinks=False).st_size != 0:
                raise FormatError(f"transfer bundle marker {entry.name} is not empty")
            seen.add(entry.name)
    missing = sorted(expected - seen)
    if missing:
        raise FormatError(f"transfer bundle lacks the marker of job {missing[0]}")


def _filtered_prior_state(prior_state: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(
        {name: copy.deepcopy(value) for name, value in prior_state.items() if name in EXCHANGE_PRIOR_STATE_MEMBERS}
    )


def _check_exchange_joins(manifest: BundleManifest) -> None:
    """Refuse a waiting job whose join names a job outside the bundle (plan 4.5 "Allowlist").

    A forged join naming local jobs would otherwise pin them as children of an
    unresolved join.
    """

    inside = set(manifest.job_ids)
    states = [(manifest.job_id, manifest.prior_kind, manifest.prior_state)]
    states += [(member.job_id, member.prior_kind, member.prior_state) for member in manifest.members]
    for job_id, kind, state in states:
        if kind != "waiting":
            continue
        join = state.get("join")
        children = join.get("children") if isinstance(join, Mapping) else None
        if not isinstance(children, list) or not children:
            raise FormatError(f"waiting job {job_id} carries no join with children")
        for child in children:
            if not isinstance(child, Mapping):
                raise FormatError(f"waiting job {job_id} has a malformed join child")
            child_id = canonical_uuid(child.get("job_id"), "join child job_id")
            if child_id not in inside:
                raise FormatError(f"waiting job {job_id} joins job {child_id}, which is not part of the bundle")
            child_key = child.get("job_key")
            if child_key is not None:
                _job_key_of(child_key, child_id, "join child job_key")


def _job_document(payload: Path) -> JobDefinition:
    """Read one bundled ``job.json``; an ``OSError`` other than a planted symlink propagates."""

    try:
        data = _read_regular_file(payload / "job.json", _MAXIMUM_JOB_DOCUMENT_BYTES)
    except FileNotFoundError:
        raise FormatError(f"transfer bundle payload {payload} has no job.json") from None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise FormatError(f"transfer bundle payload {payload} has a symlinked job.json") from exc
        raise
    return JobDefinition.from_bytes(data, name=str(payload / "job.json"))


def _check_exchange_parents(workspace: "Workspace", jobs: Sequence[VerifiedJob]) -> None:
    """Refuse a root whose parent is present here, and a member that is not its manifest parent's child.

    A forged parent would keep a client tree from ever returning, or bind a
    member to a local job.
    """

    for job in jobs:
        parent = _job_document(job.payload).parent
        if job.is_root:
            if parent is None:
                continue
            parent_id = canonical_uuid(parent.get("job_id"), "parent.job_id")
            if workspace.find_marker_by_id(parent_id, kinds=STATE_KINDS) is not None:
                raise FormatError(
                    f"transfer bundle root {job.job_key} names the parent {parent_id}, a job of this workspace"
                )
        elif parent is None or parent.get("job_id") != job.parent_job_id:
            raise FormatError(
                f"transfer bundle member {job.job_key} is not a child of its manifest parent {job.parent_job_id}"
            )


def _check_exchange_spawns(workspace: "Workspace", jobs: Sequence[VerifiedJob]) -> None:
    """Refuse a bundle whose spawn records name a job present here that is not part of the bundle.

    A tree's members are found from its jobs' spawn records, so a forged record
    naming a local job (an orphan whose absent parent the client claims to be)
    would otherwise sweep that job into the client's tree when it returns.
    """

    from ._job_tree import spawned_children

    inside = {job.job_id for job in jobs}
    for job in jobs:
        try:
            entries = spawned_children(job.payload)
        except FormatError as exc:
            raise FormatError(f"transfer bundle payload of {job.job_key} has unreadable spawn records: {exc}") from exc
        for entry in entries:
            child_id = entry.get("job_id")
            if not isinstance(child_id, str) or child_id in inside:
                continue
            if workspace.find_marker_by_id(child_id, kinds=STATE_KINDS) is not None:
                raise FormatError(
                    f"transfer bundle job {job.job_key} records the spawned child {child_id}, a job of this workspace"
                )


def _verified_structure(
    root: Path, *, exchange: bool, pace: Callable[[], None] | None
) -> tuple[BundleManifest, list[VerifiedJob]]:
    """Steps 1-5 of :func:`verify_bundle`: everything that needs no importing workspace."""

    # Steps 1 and 2: the root and the walk, before anything is opened.
    _refuse_unsafe_entries(root, pace=pace)
    # Step 3: the manifest.
    envelope = root / TRANSFER_DIRECTORY
    _real_directory(envelope, TRANSFER_DIRECTORY)
    manifest = _read_bundle_manifest(envelope)
    if exchange:
        _check_exchange_joins(manifest)
    # Step 4: the envelope's exact shape.
    with os.scandir(envelope) as entries:
        names = {entry.name for entry in entries}
    unexpected = sorted(names - _ENVELOPE_ENTRIES)
    if unexpected:
        raise FormatError(f"transfer bundle holds the unexpected entry {TRANSFER_DIRECTORY}/{unexpected[0]}")
    for name in (TRANSFER_MARKERS, TRANSFER_RUNNERS, TRANSFER_TREE):
        _real_directory(envelope / name, f"{TRANSFER_DIRECTORY}/{name}")
    _check_markers(envelope / TRANSFER_MARKERS, manifest.job_ids)
    member_paths = {
        member.job_id: PurePosixPath(TRANSFER_TREE, *member.placement.parts, member.job_key)
        for member in manifest.members
    }
    _check_exact_tree(
        envelope / TRANSFER_TREE,
        {relative.relative_to(TRANSFER_TREE): True for relative in member_paths.values()},
        f"{TRANSFER_DIRECTORY}/{TRANSFER_TREE}",
    )
    _check_exact_tree(
        envelope / TRANSFER_RUNNERS,
        {runner.path: False for runner in manifest.runners},
        f"{TRANSFER_DIRECTORY}/{TRANSFER_RUNNERS}",
    )
    jobs = verified_jobs(manifest, root, exchange=exchange)
    # Step 5: digests.
    for job in jobs:
        if _payload_digest(job.payload, pace=pace) != job.payload_sha256:
            raise FormatError(f"transfer bundle payload digest mismatch for job {job.job_key}")
        if _payload_seal_sha256(job.payload) != job.seal_sha256:
            raise FormatError(f"transfer bundle seal does not match the manifest for job {job.job_key}")
        # The payload's own identity agrees with the key it is published under.
        definition = _job_document(job.payload)
        if definition.job_key != job.job_key:
            raise FormatError(f"transfer bundle payload of {job.job_key} holds the job.json of {definition.job_key}")
    for runner in manifest.runners:
        if _runner_digest(envelope.joinpath(TRANSFER_RUNNERS, *runner.path.parts)) != runner.sha256:
            raise FormatError(f"bundled runner digest mismatch: {runner.path.as_posix()}")
    return manifest, jobs


def verified_jobs(manifest: BundleManifest, root: Path, *, exchange: bool) -> list[VerifiedJob]:
    """Return the per-job facts of one manifest, the root first, then the members top-down.

    :param manifest: The bundle's manifest.
    :param root: Where the root payload is (members are below its ``.httk-transfer/tree``).
    :param exchange: Filter every ``prior_state`` to :data:`EXCHANGE_PRIOR_STATE_MEMBERS`.
    :return: The jobs.
    """

    envelope = root / TRANSFER_DIRECTORY
    member_paths = {
        member.job_id: PurePosixPath(TRANSFER_TREE, *member.placement.parts, member.job_key)
        for member in manifest.members
    }
    jobs = [
        VerifiedJob(
            job_id=manifest.job_id,
            job_key=manifest.job_key,
            placement=manifest.destination_placement,
            payload=root,
            envelope_relative=None,
            parent_job_id=None,
            payload_sha256=manifest.payload_sha256,
            seal_sha256=manifest.seal_sha256,
            prior_kind=manifest.prior_kind,
            prior_state=_filtered_prior_state(manifest.prior_state) if exchange else manifest.prior_state,
            priority=manifest.priority,
            source_generation=manifest.source_generation,
        )
    ]
    jobs += [
        VerifiedJob(
            job_id=member.job_id,
            job_key=member.job_key,
            placement=member.placement,
            payload=envelope.joinpath(*member_paths[member.job_id].parts),
            envelope_relative=member_paths[member.job_id],
            parent_job_id=member.parent_job_id,
            payload_sha256=member.payload_sha256,
            seal_sha256=member.seal_sha256,
            prior_kind=member.prior_kind,
            prior_state=_filtered_prior_state(member.prior_state) if exchange else member.prior_state,
            priority=member.priority,
            source_generation=member.source_generation,
        )
        for member in manifest.members
    ]
    return jobs


def check_bundle(path: Path, *, pace: Callable[[], None] | None = None) -> BundleManifest:
    """Check one bundle's structure, digests and seals, without an importing workspace.

    These are steps 1-5 of :func:`verify_bundle`; only an import, which knows its
    workspace and source, checks the destination and the exchange rules.

    :param path: The bundle (root payload) directory.
    :param pace: Called regularly during the walk and the digests.
    :return: The bundle's manifest.
    :raises httk.workflow.errors.FormatError: If the bundle's content is refused.
    :raises OSError: If the bundle cannot be read.
    """

    return _verified_structure(Path(path), exchange=False, pace=pace)[0]


def verify_bundle(
    path: Path,
    *,
    workspace: "Workspace",
    source: BundleSource,
    pace: Callable[[], None] | None = None,
) -> VerifiedBundle:
    """Verify one bundle completely before any part of it moves (plan 4.2).

    1. ``lstat`` the root: a real directory.
    2. The adoption walk over the whole bundle, ``.httk-transfer`` included (no
       special files, no escaping symlinks, no multiply linked files, the
       entry and depth bounds), calling *pace* every :data:`PACE_STRIDE`
       entries. Nothing is opened before the walk.
    3. The manifest: bounded, strict schema (:class:`BundleManifest`); for an
       exchange bundle (:data:`EXCHANGE_SOURCES`), ``prior_state`` filtered to
       :data:`EXCHANGE_PRIOR_STATE_MEMBERS` and joins checked (parents and the
       children the spawn records name are checked once the payload digests
       are verified: neither may be a job of this workspace outside the bundle).
    4. ``.httk-transfer`` holds exactly ``manifest.json``, ``markers/`` (one
       empty regular file per job, named by job id), ``runners/`` (exactly the
       declared runners) and ``tree/`` (exactly the members' payload
       directories at their placements).
    5. Digests: the root and member payloads, the runners, the seals; each
       payload's ``job.json`` names the job key it is published under.
    6. The destination: this workspace for an ``"incoming"`` bundle, none otherwise.

    :param path: The bundle (root payload) directory.
    :param workspace: The importing workspace.
    :param source: Where the bundle came from.
    :param pace: Called regularly during the walk and the digests (a manager's heartbeat pacer).
    :return: The verified bundle.
    :raises ValueError: If *source* is not a :data:`BUNDLE_SOURCES` value.
    :raises httk.workflow.errors.FormatError: If the bundle's content is refused.
    :raises OSError: If the bundle cannot be read; it is left for a retry.
    """

    if source not in BUNDLE_SOURCES:
        raise ValueError(f"unknown bundle source {source!r}")
    root = Path(path)
    exchange = source in EXCHANGE_SOURCES
    manifest, jobs = _verified_structure(root, exchange=exchange, pace=pace)
    if exchange:
        _check_exchange_parents(workspace, jobs)
        _check_exchange_spawns(workspace, jobs)
    # Step 6: the destination.
    if source in _ADDRESSED_SOURCES:
        if manifest.destination_workspace_id != workspace.workspace_id:
            raise FormatError(
                f"transfer bundle is addressed to workspace {manifest.destination_workspace_id}, "
                f"not to {workspace.workspace_id}"
            )
    elif manifest.destination_workspace_id is not None:
        raise FormatError(
            f"transfer bundle is addressed to workspace {manifest.destination_workspace_id}; "
            "only an ejected bundle (addressed to none) can be adopted this way"
        )
    return VerifiedBundle(
        path=root,
        source=source,
        manifest=manifest,
        jobs=tuple(sorted(jobs, key=lambda job: job.job_id)),
    )
