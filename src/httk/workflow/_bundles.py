"""Job bundles: build one from owned jobs, validate one at the trust boundary, and rekey an exchange bundle.

A bundle is a plain directory (plan §3.8)::

    <bundle>/
    ├── bundle.json              BundleManifest: members listed top-down, members[0] is the root
    └── jobs/<placement>/<job-key>/   one complete payload per member

:func:`build_bundle` writes ``bundle.json`` first and then extracts each member
out of ``owned/`` into the bundle inside the ejecting owner's scratch.
:func:`validate_bundle` is the trust boundary of adoption: it reads only, and
accepts a tree that matches its manifest exactly. :func:`rekey_untrusted` gives
the members of an exchange bundle fresh job UUIDs (O11), derived
deterministically from the adopting workspace and the client's UUID.

**Crash-resume contract of rekeying** (for the ``adopt`` reconciler). The
rekey records its complete result in ``<scratch>/rekey.json`` before it renames
anything; that file lives in the adopter's private scratch, outside the
client-controlled bundle tree, so a client can never supply it.

- If ``<scratch>/rekey.json`` exists, call :func:`rekey_untrusted` again to
  finish; afterwards ``validate_bundle(bundle, untrusted=False)`` accepts the
  bundle and equals :func:`read_rekeyed`. The manifest passed on resumption may
  be the original one or the rekeyed one, whichever ``bundle.json`` holds.
- If it does not exist, nothing was renamed: the untrusted validation runs
  again from the start.

Rekeying writes no ``state.json``; adoption records ``origin: exchange`` and
the client's UUID as ``exchange_name`` (from :func:`exchange_names`) when it
publishes.
"""

import contextlib
import dataclasses
import json
import os
import re
import stat
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Self

from httk.workflow import _fs
from httk.workflow._job import MAX_JOB_BYTES, JobDefinition
from httk.workflow._kernel import UNOWNED_STATES, OwnedJob, Owner
from httk.workflow._util import json_bytes, utc_now
from httk.workflow.errors import FormatError
from httk.workflow.models import (
    canonical_uuid,
    check_job_placement,
    make_job_key,
    normalize_placement,
    parse_job_key,
    parse_placement_text,
    placement_text,
)

BUNDLE_FORMAT = "httk-workflow-bundle"
BUNDLE_VERSION = 1
REKEY_FORMAT = "httk-workflow-rekey"
REKEY_VERSION = 1
#: The largest accepted ``bundle.json``, in bytes.
MAX_MANIFEST_BYTES = 1 << 20
#: The largest accepted ``rekey.json`` (a manifest plus one name pair per member), in bytes.
MAX_REKEY_BYTES = 4 << 20

# The fixed namespace of rekeyed exchange job UUIDs: uuid5(namespace, "<workspace-id>/<client-uuid>").
_EXCHANGE_NAMESPACE = uuid.UUID("b480bb06-dc60-4d16-a78b-cc6b3af734b4")
_TRANSFER_ID = re.compile(r"[0-9a-f]{32}")
_TIMESTAMP_LIMIT = 64
_LOCATOR_LIMIT = 4096
_MANIFEST_MEMBERS = frozenset(
    {"format", "format_version", "transfer_id", "source_workspace_id", "created_at", "destination", "members"}
)
_DESTINATION_MEMBERS = frozenset({"workspace_id", "locator"})
_MEMBER_MEMBERS = frozenset({"job_id", "job_key", "placement", "state", "priority", "parent_job_id"})
_REKEY_MEMBERS = frozenset({"format", "format_version", "manifest", "exchange_names"})
# Entries that only an owner writes: a fresh job from an untrusted client carries none of them.
_NOT_FRESH = frozenset({"state.json", "seal.json", "logs", "attempts", ".httk-job"})
_MANIFEST_NAME = PurePosixPath("bundle.json")
_JOBS = PurePosixPath("jobs")


class BundleError(FormatError):
    """A bundle, its manifest or a rekey record is refused; the message names the precise reason."""


# -- the manifest ---------------------------------------------------------------------------------------------------


def _uuid(value: object, name: str) -> str:
    try:
        return canonical_uuid(value, name)
    except FormatError as exc:
        raise BundleError(str(exc)) from exc


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise BundleError(f"{name} must be a string")
    return value


def _optional(value: object, name: str) -> str | None:
    return None if value is None else _string(value, name)


def _placement_text(value: object, name: str) -> PurePosixPath:
    try:
        placement = parse_placement_text(value, name)
    except FormatError as exc:
        raise BundleError(f"{name}: {exc}") from exc
    if placement_text(placement) != value:
        raise BundleError(f"{name} is not canonical: {value!r}")
    return placement


@dataclass(frozen=True)
class BundleMember:
    """One job of a bundle, as listed in ``bundle.json``.

    :param job_id: The job UUID.
    :param job_key: The job key, ``[<tag>--]<job_id>``.
    :param placement: The job's authoritative placement (that of its ``job.json``).
    :param state: The unowned state the job was taken from, and into which adoption publishes it.
    :param priority: The priority it was taken with, ``0``-``999``.
    :param parent_job_id: The parent's UUID; ``None`` for the root.
    :raises BundleError: If a member is malformed.
    """

    job_id: str
    job_key: str
    placement: PurePosixPath
    state: str
    priority: int
    parent_job_id: str | None

    def __post_init__(self) -> None:
        """Validate the members."""

        _uuid(self.job_id, "member job_id")
        if not isinstance(self.job_key, str):
            raise BundleError(f"member {self.job_id}: job_key must be a string")
        try:
            _, key_id = parse_job_key(self.job_key)
        except FormatError as exc:
            raise BundleError(f"member {self.job_id}: {exc}") from exc
        if key_id != self.job_id:
            raise BundleError(f"member {self.job_id}: job_key {self.job_key!r} names another job")
        if not isinstance(self.placement, PurePosixPath):
            raise BundleError(f"member {self.job_id}: placement must be a PurePosixPath")
        try:
            normalized = normalize_placement(self.placement)
            check_job_placement(normalized)
        except FormatError as exc:
            raise BundleError(f"member {self.job_id}: {exc}") from exc
        # Listings tell job directories from placement directories by "~", so a placement may not use it.
        if normalized != self.placement or "~" in placement_text(normalized):
            raise BundleError(f"member {self.job_id}: placement is not canonical: {self.placement!r}")
        if not isinstance(self.state, str) or self.state not in UNOWNED_STATES:
            raise BundleError(f"member {self.job_id}: state must be one of {UNOWNED_STATES}: {self.state!r}")
        if type(self.priority) is not int or not 0 <= self.priority <= 999:
            raise BundleError(f"member {self.job_id}: priority must be an integer 0-999: {self.priority!r}")
        parent = self.parent_job_id
        if parent is not None and _uuid(parent, f"member {self.job_id}: parent_job_id") == self.job_id:
            raise BundleError(f"member {self.job_id} names itself as its parent")

    def as_mapping(self) -> dict[str, object]:
        """Return the member as written to ``bundle.json``.

        :return: A fresh mapping.
        """

        return {
            "job_id": self.job_id,
            "job_key": self.job_key,
            "placement": placement_text(self.placement),
            "state": self.state,
            "priority": self.priority,
            "parent_job_id": self.parent_job_id,
        }

    @classmethod
    def from_mapping(cls, value: object, index: int) -> Self:
        """Parse one decoded member strictly: exact members, exact types, canonical formats.

        :param value: The decoded member.
        :param index: Its position in the list (for messages).
        :return: The member.
        :raises BundleError: If the member is malformed.
        """

        if not isinstance(value, dict):
            raise BundleError(f"members[{index}] must be an object")
        if set(value) != _MEMBER_MEMBERS:
            raise BundleError(f"members[{index}] members differ: {sorted(set(value) ^ _MEMBER_MEMBERS)}")
        priority = value["priority"]
        if type(priority) is not int:
            raise BundleError(f"members[{index}].priority must be an integer")
        return cls(
            job_id=_uuid(value["job_id"], f"members[{index}].job_id"),
            job_key=_string(value["job_key"], f"members[{index}].job_key"),
            placement=_placement_text(value["placement"], f"members[{index}].placement"),
            state=_string(value["state"], f"members[{index}].state"),
            priority=priority,
            parent_job_id=_optional(value["parent_job_id"], f"members[{index}].parent_job_id"),
        )


def _check_tree(members: tuple[BundleMember, ...]) -> None:
    # Top-down: the root first, and every other member's parent earlier in the list.
    ids = {member.job_id for member in members}
    if len(ids) != len(members):
        raise BundleError("the members repeat a job id")
    if len({(member.placement, member.job_key) for member in members}) != len(members):
        raise BundleError("the members repeat a placement and job key")
    if members[0].parent_job_id in ids:
        raise BundleError(f"the root {members[0].job_id} has its parent inside the bundle")
    seen = {members[0].job_id}
    for member in members[1:]:
        if member.parent_job_id is None or member.parent_job_id not in ids:
            raise BundleError(f"member {member.job_id} is a second root: its parent is not in the bundle")
        if member.parent_job_id not in seen:
            raise BundleError(f"member {member.job_id} is listed before its parent {member.parent_job_id}")
        seen.add(member.job_id)


@dataclass(frozen=True)
class BundleManifest:
    """The ``bundle.json`` of a bundle (format ``httk-workflow-bundle``, version 1).

    :param transfer_id: The bundle's fresh id, 32 lowercase hex digits.
    :param source_workspace_id: The ejecting workspace's id.
    :param created_at: When the bundle was built, an ISO 8601 timestamp with a time zone.
    :param destination_workspace_id: The intended destination workspace, or ``None``.
    :param destination_locator: The intended destination path or address, or ``None``.
    :param members: The jobs, top-down: every parent before its children; ``members[0]`` is the root.
    :raises BundleError: If a member is malformed or the members do not form a top-down tree.
    """

    transfer_id: str
    source_workspace_id: str
    created_at: str
    destination_workspace_id: str | None
    destination_locator: str | None
    members: tuple[BundleMember, ...]

    def __post_init__(self) -> None:
        """Validate the members."""

        if not isinstance(self.transfer_id, str) or not _TRANSFER_ID.fullmatch(self.transfer_id):
            raise BundleError(f"transfer_id must be 32 lowercase hex digits: {self.transfer_id!r}")
        _uuid(self.source_workspace_id, "source_workspace_id")
        if not isinstance(self.created_at, str) or len(self.created_at) > _TIMESTAMP_LIMIT:
            raise BundleError("created_at must be a timestamp string")
        try:
            created = datetime.fromisoformat(self.created_at)
        except ValueError as exc:
            raise BundleError(f"created_at is not an ISO 8601 timestamp: {self.created_at!r}") from exc
        if created.tzinfo is None:
            raise BundleError(f"created_at has no time zone: {self.created_at!r}")
        if self.destination_workspace_id is not None:
            _uuid(self.destination_workspace_id, "destination.workspace_id")
        locator = self.destination_locator
        if locator is not None and (
            not isinstance(locator, str) or not locator or len(locator) > _LOCATOR_LIMIT or "\0" in locator
        ):
            raise BundleError(f"destination.locator must be a nonempty string of at most {_LOCATOR_LIMIT} characters")
        if not isinstance(self.members, tuple) or not self.members:
            raise BundleError("members must be a nonempty tuple")
        if not all(isinstance(member, BundleMember) for member in self.members):
            raise BundleError("members must be BundleMember values")
        _check_tree(self.members)

    def as_mapping(self) -> dict[str, object]:
        """Return the document as written to ``bundle.json``.

        :return: A fresh mapping.
        """

        return {
            "format": BUNDLE_FORMAT,
            "format_version": BUNDLE_VERSION,
            "transfer_id": self.transfer_id,
            "source_workspace_id": self.source_workspace_id,
            "created_at": self.created_at,
            "destination": {"workspace_id": self.destination_workspace_id, "locator": self.destination_locator},
            "members": [member.as_mapping() for member in self.members],
        }

    def to_json(self) -> bytes:
        """Return the canonical bytes of ``bundle.json``: compact, sorted UTF-8 JSON and a newline.

        Delivery compares exactly these bytes, so they depend on the manifest alone.

        :return: The bytes.
        """

        return json_bytes(self.as_mapping()) + b"\n"

    @classmethod
    def from_mapping(cls, value: object) -> Self:
        """Parse a decoded document strictly: exact members, exact types, canonical formats.

        :param value: The decoded document.
        :return: The manifest.
        :raises BundleError: If the document is malformed.
        """

        if not isinstance(value, dict):
            raise BundleError("bundle.json must be a JSON object")
        if set(value) != _MANIFEST_MEMBERS:
            raise BundleError(f"bundle.json members differ: {sorted(set(value) ^ _MANIFEST_MEMBERS)}")
        if value["format"] != BUNDLE_FORMAT:
            raise BundleError(f"bundle.json format must be {BUNDLE_FORMAT!r}: {value['format']!r}")
        if type(value["format_version"]) is not int or value["format_version"] != BUNDLE_VERSION:
            raise BundleError(f"bundle.json format_version must be {BUNDLE_VERSION}: {value['format_version']!r}")
        destination = value["destination"]
        if not isinstance(destination, dict) or set(destination) != _DESTINATION_MEMBERS:
            raise BundleError("bundle.json destination must be an object of exactly workspace_id and locator")
        members = value["members"]
        if not isinstance(members, list):
            raise BundleError("bundle.json members must be an array")
        return cls(
            transfer_id=_string(value["transfer_id"], "transfer_id"),
            source_workspace_id=_string(value["source_workspace_id"], "source_workspace_id"),
            created_at=_string(value["created_at"], "created_at"),
            destination_workspace_id=_optional(destination["workspace_id"], "destination.workspace_id"),
            destination_locator=_optional(destination["locator"], "destination.locator"),
            members=tuple(BundleMember.from_mapping(member, index) for index, member in enumerate(members)),
        )

    @classmethod
    def from_json(cls, data: bytes) -> Self:
        """Parse ``bundle.json`` bytes strictly; duplicate object keys and non-finite numbers are refused.

        :param data: The bytes.
        :return: The manifest.
        :raises BundleError: On any deviation from the format.
        """

        return cls.from_mapping(_decode(data, "bundle.json"))

    def member_dir(self, bundle: Path, member: BundleMember) -> Path:
        """Return the payload directory of *member*: ``<bundle>/jobs/<placement>/<job-key>``.

        :param bundle: The bundle directory.
        :param member: One of :attr:`members`.
        :return: The directory.
        """

        return bundle / _JOBS / member.placement / member.job_key


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, item in pairs:
        if key in result:
            raise BundleError(f"duplicate JSON object key {key!r}")
        result[key] = item
    return result


def _refuse_constant(name: str) -> object:
    raise BundleError(f"non-finite JSON number {name}")


def _decode(data: bytes, name: str) -> object:
    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_refuse_constant)
    except BundleError:
        raise
    except (ValueError, RecursionError) as exc:
        # ValueError covers undecodable UTF-8, malformed JSON and integers beyond the conversion limit.
        raise BundleError(f"{name} is not valid UTF-8 JSON: {exc}") from exc


def _read(path: Path, limit: int) -> bytes:
    # Bounded, regular files only, never following a symlink, and never blocking on a planted FIFO.
    try:
        data = _fs.read_bounded(_fs.loc(path), limit, nonblock=True)
    except (_fs.UnsafePath, _fs.TooLarge) as exc:
        raise BundleError(str(exc)) from exc
    if data is None:
        raise BundleError(f"{path} does not exist")
    return data


# -- building -------------------------------------------------------------------------------------------------------


def build_bundle(
    owner: Owner,
    members: Sequence[OwnedJob],
    *,
    source_workspace_id: str,
    destination_workspace_id: str | None = None,
    destination_locator: str | None = None,
) -> Path:
    """Build a bundle of quiescent owned jobs in a fresh ``eject`` scratch of *owner*.

    ``bundle.json`` is written first, recording each member's ``from`` state and
    priority, and then each member is extracted out of ``owned/``. A crash leaves an
    ``eject`` scratch for its reconciler.

    :param owner: The owner holding every member.
    :param members: The jobs, top-down: the root first, every parent before its children.
    :param source_workspace_id: This workspace's id.
    :param destination_workspace_id: The intended destination workspace, or ``None``.
    :param destination_locator: The intended destination path or address, or ``None``.
    :return: The bundle directory, ``tmp/<owner-id>.eject.<token>/bundle``.
    :raises ValueError: For no members, a member held by another owner or listed out of order.
    :raises httk.workflow.errors.FormatError: For a malformed id or locator, or a ``job.json`` that is
        unreadable or disagrees with its handle (:class:`BundleError` for all but an unreadable one).
    :raises httk.workflow.errors.WorkflowError: While a member's attempt runs or after it was released.
    """

    if not members:
        raise ValueError("a bundle needs at least one member")
    entries: list[BundleMember] = []
    definitions: list[JobDefinition] = []
    listed: set[str] = set()
    for index, job in enumerate(members):
        if job.owner is not owner:
            raise ValueError(f"{job.job_key} is not held by owner {owner.owner_id}")
        job.require_quiescent()
        definition = JobDefinition.from_path(job.path / "job.json")
        if definition.job_key != job.job_key or definition.placement != job.placement():
            raise BundleError(f"{job.path}/job.json disagrees with its directory name or claimed placement")
        parent = None if index == 0 or definition.parent is None else definition.parent["job_id"]
        if index and parent not in listed:
            raise ValueError(f"{job.job_key} is not listed after its parent")
        assert parent is None or isinstance(parent, str)
        entries.append(
            BundleMember(job.job_id, job.job_key, job.placement(), job.from_state, job.from_priority, parent)
        )
        definitions.append(definition)
        listed.add(job.job_id)
    manifest = BundleManifest(
        uuid.uuid4().hex,
        _uuid(source_workspace_id, "source_workspace_id"),
        utc_now(),
        destination_workspace_id,
        destination_locator,
        tuple(entries),
    )
    # Refuse here what adoption would refuse, before anything moves.
    by_id = {member.job_id: member for member in manifest.members}
    for index, definition in enumerate(definitions):
        _check_job(manifest, index, definition, by_id, untrusted=False)
    durable = owner.workspace.durable
    scratch = owner.scratch("eject")
    bundle = scratch / "bundle"
    _fs.make_dirs(bundle, durable=durable)
    _fs.write_file(_fs.loc(bundle / _MANIFEST_NAME), manifest.to_json(), durable=durable)
    for job, member in zip(members, manifest.members, strict=True):
        target = manifest.member_dir(bundle, member)
        _fs.make_dirs(target.parent, durable=durable)
        job.extract(target)
    return bundle


# -- validating -----------------------------------------------------------------------------------------------------


def _check_job(
    manifest: BundleManifest, index: int, job: JobDefinition, by_id: Mapping[str, BundleMember], *, untrusted: bool
) -> None:
    member = manifest.members[index]
    if job.id != member.job_id:
        raise BundleError(f"member {member.job_id}: job.json names job {job.id}")
    if job.job_key != member.job_key:
        raise BundleError(f"member {member.job_id}: job.json gives the key {job.job_key!r}, not {member.job_key!r}")
    if job.placement != member.placement:
        raise BundleError(
            f"member {member.job_id}: job.json gives another placement: {placement_text(job.placement)!r}"
        )
    if index == 0 and member.parent_job_id is None:
        # A trusted root may keep a parent outside the bundle; an untrusted root may name none at all.
        if untrusted and job.parent is not None:
            raise BundleError(f"the root {member.job_id} of an untrusted bundle names a parent in job.json")
        return
    if job.parent is None or job.parent["job_id"] != member.parent_job_id:
        raise BundleError(f"member {member.job_id}: job.json names another parent than {member.parent_job_id}")
    # A parent outside the bundle (a trusted root's) is not checked further.
    parent = None if member.parent_job_id is None else by_id.get(member.parent_job_id)
    if parent is not None and (
        job.parent["job_key"] != parent.job_key or job.parent["placement"] != placement_text(parent.placement)
    ):
        raise BundleError(f"member {member.job_id}: job.json's parent key or placement disagrees with the bundle")


def _parse_job(data: bytes, job_id: str) -> JobDefinition:
    try:
        return JobDefinition.from_bytes(data)
    except (ValueError, TypeError, RecursionError) as exc:
        # Any failure to parse untrusted bytes is a refusal (FormatError is a ValueError).
        raise BundleError(f"member {job_id}: job.json is invalid: {exc}") from exc


def _check_structure(manifest: BundleManifest, kinds: Mapping[PurePosixPath, str], *, untrusted: bool) -> None:
    # The tree must be exactly bundle.json, jobs/, the listed placement directories and member directories, and
    # the member payloads; every structural entry must be a real directory or regular file.
    files = {_MANIFEST_NAME}
    directories = {_JOBS}
    members: set[PurePosixPath] = set()
    for member in manifest.members:
        relative = _JOBS / member.placement / member.job_key
        members.add(relative)
        files.add(relative / "job.json")
        directories.update(parent for parent in relative.parents if parent != PurePosixPath("."))
    for relative, kind in kinds.items():
        if relative in files:
            if kind != "file":
                raise BundleError(f"{relative} is a {kind}, not a regular file")
        elif relative in directories or relative in members:
            if kind != "dir":
                raise BundleError(f"{relative} is a {kind}, not a directory")
        elif any(parent in members for parent in relative.parents):
            if untrusted and kind == "symlink":
                raise BundleError(f"{relative} is a symlink, which an untrusted bundle may not carry")
            if untrusted and relative.parent in members and relative.name in _NOT_FRESH:
                raise BundleError(f"{relative}: an untrusted bundle carries only fresh jobs, without {relative.name}")
        else:
            raise BundleError(f"{relative} is not listed in bundle.json")
    if missing := sorted((files | directories | members) - kinds.keys()):
        raise BundleError(f"{missing[0]} is listed in bundle.json but missing")


def _kind(mode: int) -> str:
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISDIR(mode):
        return "dir"
    return "file" if stat.S_ISREG(mode) else "special"


def _structural_kinds(path: Path, manifest: BundleManifest) -> dict[PurePosixPath, str]:
    # Trusted bundles: the payloads are job content, so only the structural positions are listed: the bundle root,
    # jobs/ and the placement directories (never descending into anything else), and each member's job.json.
    members = {_JOBS / member.placement / member.job_key for member in manifest.members}
    directories = {_JOBS} | {parent for member in members for parent in member.parents if parent != PurePosixPath(".")}
    kinds: dict[PurePosixPath, str] = {}
    pending = [PurePosixPath()]
    while pending:
        relative = pending.pop()
        with os.scandir(path / relative) as listing:
            for item in listing:
                child = relative / item.name
                kinds[child] = _kind(item.stat(follow_symlinks=False).st_mode)
                if kinds[child] != "dir":
                    continue
                if child in directories:
                    pending.append(child)
                elif child in members:
                    with contextlib.suppress(FileNotFoundError):
                        kinds[child / "job.json"] = _kind(os.lstat(path / child / "job.json").st_mode)
    return kinds


def _read_manifest(path: Path, kind: str | None) -> BundleManifest:
    if kind != "file":
        raise BundleError(f"{path}/bundle.json is {'missing' if kind is None else f'a {kind}'}, not a regular file")
    return BundleManifest.from_json(_read(path / _MANIFEST_NAME, MAX_MANIFEST_BYTES))


def validate_bundle(path: Path, *, untrusted: bool, limits: _fs.WalkLimits = _fs.DEFAULT_LIMITS) -> BundleManifest:
    """Validate a bundle against its manifest; read-only, nothing moves.

    Every bundle: ``bundle.json`` is strict; the tree holds exactly ``bundle.json``, ``jobs/``, the listed
    placement and member directories and the member payloads, none of the structural entries a symlink or a
    special file; each member's ``job.json`` agrees with the manifest (id, key and tag, placement, parent); the
    members form one top-down tree. A trusted bundle's payloads are job content and are not examined. An
    untrusted bundle's whole tree further passes :func:`~httk.workflow._fs.walk_untrusted` (no special or
    hard-linked files, within *limits*), and it carries no symlink at all, only fresh jobs (no ``state.json``,
    ``seal.json``, ``logs``, ``attempts`` or ``.httk-job``) in state ``ready``, and a root without a parent.

    :param path: The bundle directory.
    :param untrusted: Apply the rules for a bundle from an untrusted client (the exchange).
    :param limits: The walk's entry-count and depth limits (untrusted bundles only).
    :return: The manifest.
    :raises BundleError: With the precise reason, for any refusal, including an entry that vanishes or cannot be
        read during validation (a live client may still be writing the tree).
    """

    try:
        return _validate(path, untrusted=untrusted, limits=limits)
    except OSError as exc:
        raise BundleError(f"{path} cannot be validated: {exc}") from exc


def _validate(path: Path, *, untrusted: bool, limits: _fs.WalkLimits) -> BundleManifest:
    if untrusted:
        try:
            entries = _fs.walk_untrusted(path, limits=limits)
        except _fs.UntrustedContentError as exc:
            raise BundleError(f"{path}: {exc}") from exc
        kinds = {entry.relative: str(entry.kind) for entry in entries}
        manifest = _read_manifest(path, kinds.get(_MANIFEST_NAME))
        if manifest.members[0].parent_job_id is not None:
            raise BundleError("the root of an untrusted bundle may not have a parent")
        if stale := [member.job_id for member in manifest.members if member.state != "ready"]:
            raise BundleError(f"an untrusted bundle carries only ready jobs; {stale[0]} is not")
    else:
        if (root := _kind(os.lstat(path).st_mode)) != "dir":
            raise BundleError(f"{path} is a {root}, not a bundle directory")
        try:
            kind: str | None = _kind(os.lstat(path / _MANIFEST_NAME).st_mode)
        except FileNotFoundError:
            kind = None
        manifest = _read_manifest(path, kind)
        kinds = _structural_kinds(path, manifest)
    _check_structure(manifest, kinds, untrusted=untrusted)
    by_id = {member.job_id: member for member in manifest.members}
    for index, member in enumerate(manifest.members):
        job = _parse_job(_read(manifest.member_dir(path, member) / "job.json", MAX_JOB_BYTES), member.job_id)
        _check_job(manifest, index, job, by_id, untrusted=untrusted)
    return manifest


# -- rekeying -------------------------------------------------------------------------------------------------------


def _rekeyed_id(workspace_id: str, job_id: str) -> str:
    return canonical_uuid(str(uuid.uuid5(_EXCHANGE_NAMESPACE, f"{workspace_id}/{job_id}")))


def _rekey(manifest: BundleManifest, names: Mapping[str, str]) -> BundleManifest:
    # Map ids, keys (keeping each tag) and parent links through *names* (old id -> new id).
    members = []
    for member in manifest.members:
        tag, _ = parse_job_key(member.job_key)
        new_id = names[member.job_id]
        parent = None if member.parent_job_id is None else names[member.parent_job_id]
        members.append(
            dataclasses.replace(member, job_id=new_id, job_key=make_job_key(new_id, tag), parent_job_id=parent)
        )
    return dataclasses.replace(manifest, members=tuple(members))


def _load_rekey(scratch: Path) -> tuple[BundleManifest, dict[str, str]] | None:
    try:
        data = _fs.read_bounded(_fs.loc(scratch / "rekey.json"), MAX_REKEY_BYTES, nonblock=True)
    except (_fs.UnsafePath, _fs.TooLarge) as exc:
        raise BundleError(str(exc)) from exc
    if data is None:
        return None
    value = _decode(data, "rekey.json")
    if not isinstance(value, dict) or set(value) != _REKEY_MEMBERS:
        raise BundleError(f"rekey.json must be an object of exactly {sorted(_REKEY_MEMBERS)}")
    version = value["format_version"]
    if value["format"] != REKEY_FORMAT or type(version) is not int or version != REKEY_VERSION:
        raise BundleError(f"rekey.json is not {REKEY_FORMAT} version {REKEY_VERSION}")
    manifest = BundleManifest.from_mapping(value["manifest"])
    raw = value["exchange_names"]
    if not isinstance(raw, dict):
        raise BundleError("rekey.json exchange_names must be an object")
    names = {_uuid(new, "rekeyed id"): _uuid(old, "exchange name") for new, old in raw.items()}
    if set(names) != {member.job_id for member in manifest.members} or len(set(names.values())) != len(names):
        raise BundleError("rekey.json exchange_names do not map exactly the rekeyed members to distinct names")
    return manifest, names


def rekey_untrusted(scratch: Path, bundle: Path, manifest: BundleManifest, *, workspace_id: str) -> BundleManifest:
    """Give the members of a validated untrusted bundle fresh job UUIDs; deterministic and crash-idempotent.

    The new id of a member is ``uuid5(namespace, "<workspace_id>/<client id>")``; its key keeps the tag, and
    parent links (``job_id``, ``job_key``, ``placement`` and ``workspace_id``, which becomes *workspace_id*)
    are mapped likewise. The result is recorded in ``<scratch>/rekey.json`` before anything is renamed; when
    that record exists it is loaded, never recomputed. Then each member's ``job.json`` is rewritten and its
    directory renamed, and finally ``bundle.json`` is rewritten. See the module docstring for the
    crash-resume contract.

    :param scratch: The adopter's private scratch directory.
    :param bundle: The bundle directory, ``<scratch>/<name>``.
    :param manifest: The validated manifest (on resumption, the original or the rekeyed one).
    :param workspace_id: The adopting workspace's id.
    :return: The rekeyed manifest.
    :raises ValueError: When *bundle* is not directly inside *scratch*.
    :raises BundleError: When a client id collides with a rekeyed id, when ``rekey.json`` is malformed or
        belongs to another bundle or workspace, or when a member directory is missing.
    """

    if bundle.parent != scratch:
        raise ValueError(f"{bundle} is not a bundle directly inside {scratch}")
    if bundle.name == "rekey.json":
        # The bundle's name comes from the client (an inbox entry name): it may not shadow the record.
        raise BundleError(f"a bundle named rekey.json cannot be rekeyed in {scratch}")
    workspace_id = _uuid(workspace_id, "workspace_id")
    loaded = _load_rekey(scratch)
    if loaded is None:
        forward = {member.job_id: _rekeyed_id(workspace_id, member.job_id) for member in manifest.members}
        if collisions := sorted(forward.keys() & set(forward.values())):
            # A client may pick ids that a rekey would produce; renaming one member onto another must not happen.
            raise BundleError(f"client job id {collisions[0]} collides with a rekeyed id")
        rekeyed, names = _rekey(manifest, forward), {new: old for old, new in forward.items()}
        record = {"format": REKEY_FORMAT, "format_version": REKEY_VERSION, "manifest": rekeyed.as_mapping()}
        _fs.write_file(_fs.loc(scratch / "rekey.json"), json_bytes(record | {"exchange_names": names}), durable=True)
    else:
        rekeyed, names = loaded
        forward = {old: new for new, old in names.items()}
        if any(_rekeyed_id(workspace_id, old) != new for old, new in forward.items()):
            raise BundleError(f"rekey.json was derived for another workspace than {workspace_id}")
    original = _rekey(rekeyed, names)
    if manifest not in (original, rekeyed):
        raise BundleError("rekey.json belongs to another bundle")
    by_id = {member.job_id: member for member in rekeyed.members}
    for old, new in zip(original.members, rekeyed.members, strict=True):
        old_dir, new_dir = original.member_dir(bundle, old), rekeyed.member_dir(bundle, new)
        if _fs.exists(_fs.loc(old_dir)):
            job = _parse_job(_read(old_dir / "job.json", MAX_JOB_BYTES), old.job_id)
            parent = job.parent
            if new.parent_job_id is not None:
                if parent is None:
                    raise BundleError(f"member {old.job_id}: job.json lost its parent after validation")
                link = by_id[new.parent_job_id]
                parent = {
                    **parent,
                    "workspace_id": workspace_id,
                    "job_id": link.job_id,
                    "job_key": link.job_key,
                    "placement": placement_text(link.placement),
                }
            # Derived from the record, not from job.json, so rewriting an already rewritten job.json is a no-op.
            rekeyed_job = dataclasses.replace(job, id=new.job_id, parent=parent, stored_digest=None)
            _fs.write_file(_fs.loc(old_dir / "job.json"), rekeyed_job.encode(), durable=True)
            # The new directory shares the old one's placement directory, so its parent exists.
            _fs.move_owned(_fs.loc(old_dir), _fs.loc(new_dir), durable=True)
        elif not _fs.exists(_fs.loc(new_dir)):
            raise BundleError(f"member {old.job_id} is missing from {bundle} under both its old and its new key")
    _fs.write_file(_fs.loc(bundle / _MANIFEST_NAME), rekeyed.to_json(), durable=True)
    # A resumed rekey may find the temporaries of a write that died; untrusted validation passed before rekey.json
    # existed, so at these positions such names are ours.
    _fs.remove_write_temporaries(bundle, _MANIFEST_NAME.name, durable=True)
    for member in rekeyed.members:
        _fs.remove_write_temporaries(rekeyed.member_dir(bundle, member), "job.json", durable=True)
    return rekeyed


def read_rekeyed(scratch: Path) -> BundleManifest | None:
    """Return the rekeyed manifest recorded in ``<scratch>/rekey.json``.

    :param scratch: The adopter's scratch directory.
    :return: The manifest, or ``None`` when nothing was rekeyed.
    :raises BundleError: When ``rekey.json`` is malformed.
    """

    loaded = _load_rekey(scratch)
    return None if loaded is None else loaded[0]


def exchange_names(scratch: Path) -> Mapping[str, str]:
    """Return the client's original job id (its ``exchange_name``) of each rekeyed job, from ``rekey.json``.

    :param scratch: The adopter's scratch directory.
    :return: New job id to the client's job id; empty when nothing was rekeyed.
    :raises BundleError: When ``rekey.json`` is malformed.
    """

    loaded = _load_rekey(scratch)
    return {} if loaded is None else loaded[1]
