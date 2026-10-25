"""Children a committing job spawned: validation at commit step 1 and replay-safe publication (note §7.3).

A runner proposes children as complete payloads in
``attempts/<A>/outcome.ready/children/jobs/<key>/`` plus ``children/spawn.json``.
:func:`validate_children` checks them once, while the parent is quiescent, and
returns the publication plan (the ``commit.children`` entries of plan §3.3).
:func:`publish_children` works from that plan alone, so a replay never
re-validates: a replay finds owner-written ``state.json`` files in the staged
children, which validation would refuse.
"""

import dataclasses
import errno
import json
import os
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Self, cast

from httk.workflow import _fs, _kernel
from httk.workflow._bundles import EXCHANGE_POOL_PREFIX
from httk.workflow._embedded import embedded_digest
from httk.workflow._job import _SHA256, JobDefinition
from httk.workflow._state import StateDoc, _thaw, encode_state
from httk.workflow._util import json_bytes, require_mapping, require_string
from httk.workflow.errors import FormatError, UnsupportedExtensionError
from httk.workflow.models import (
    JOB_WORKFLOW_DIRECTORY,
    canonical_uuid,
    normalize_placement,
    parse_job_key,
    placement_text,
    validate_label,
)

__all__ = [
    "SPAWN_FORMAT",
    "SPAWN_FORMAT_VERSION",
    "ChildPlan",
    "labeled_join",
    "newest_labeled",
    "publish_children",
    "validate_children",
]

SPAWN_FORMAT = "httk-workflow-spawn"
SPAWN_FORMAT_VERSION = 2
#: The largest accepted ``spawn.json``, in bytes (the legacy control-document bound).
MAX_SPAWN_BYTES = 16 << 20
#: Path bytes kept free below a child's job directory: room for the protocol files read inside an unowned job.
_PATH_HEADROOM = 256
#: Where publication assembles an embedded-workflow child's copy of its parent's ``workflow/`` before renaming it
#: into place.
_PARTIAL_WORKFLOW = "workflow.partial"
#: Names only owners write at a job's root (``workflow`` is an unlocked job's runner copy, placed by an
#: operator, or an embedded workflow, copied by publication); a proposed child may not carry them.
TRUSTED_NAMES = frozenset({"state.json", "seal.json", "logs", "attempts", JOB_WORKFLOW_DIRECTORY, _PARTIAL_WORKFLOW})

_ENTRY_REQUIRED = frozenset({"job_key", "label", "placement"})
_ENTRY_MEMBERS = _ENTRY_REQUIRED | {"workspace_id", "job_id", "spawn_id"}
_PLAN_MEMBERS = frozenset(
    {"job_id", "job_key", "label", "spawn_id", "placement", "priority", "staged", "initial_state", "embedded_sha256"}
)


@dataclass(frozen=True)
class ChildPlan:
    """One child to publish: exactly a ``commit.children`` entry of plan §3.3.

    :param job_id: The child UUID.
    :param job_key: The child key, also its staged directory name.
    :param label: The child's label, unique within its spawn set.
    :param spawn_id: The spawn id ``spawn.json`` recorded, or ``None``.
    :param placement: The child's placement text.
    :param priority: The child's priority.
    :param staged: The payload-relative ``attempts/<A>/outcome.ready/children/jobs/<key>``.
    :param initial_state: The child ``state.json`` mapping written before its submit.
    :param embedded_sha256: For a child embedding its parent's workflow, the embedded digest of the parent's
        ``workflow/`` at its launch, which the child's copy must have; ``None`` otherwise.
    """

    job_id: str
    job_key: str
    label: str
    spawn_id: str | None
    placement: str
    priority: int
    staged: str
    initial_state: Mapping[str, object]
    embedded_sha256: str | None = None

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON-ready ``commit.children`` entry.

        :return: A fresh mutable mapping.
        """

        return {
            "job_id": self.job_id,
            "job_key": self.job_key,
            "label": self.label,
            "spawn_id": self.spawn_id,
            "placement": self.placement,
            "priority": self.priority,
            "staged": self.staged,
            "initial_state": _thaw(self.initial_state),
            "embedded_sha256": self.embedded_sha256,
        }

    @classmethod
    def from_mapping(cls, value: object) -> Self:
        """Read one ``commit.children`` entry back from the owner's ``state.json``.

        :param value: The decoded entry.
        :return: The plan.
        :raises httk.workflow.errors.FormatError: If the entry is malformed.
        """

        entry = require_mapping(value, "commit.children entry")
        if set(entry) != _PLAN_MEMBERS:
            raise FormatError(f"commit.children entry members differ: {sorted(set(entry) ^ _PLAN_MEMBERS)}")
        job_key = require_string(entry["job_key"], "commit.children job_key")
        job_id = canonical_uuid(entry["job_id"], "commit.children job_id")
        if parse_job_key(job_key)[1] != job_id:
            raise FormatError("commit.children job_key does not carry its job_id")
        spawn_id = entry["spawn_id"]
        priority = entry["priority"]
        if type(priority) is not int or not 0 <= priority <= 999:
            raise FormatError("commit.children priority must be an integer 0-999")
        staged = require_string(entry["staged"], "commit.children staged")
        # The plan is owner-written, yet a malformed path would make publication move something else.
        parts = PurePosixPath(staged).parts
        if (
            len(parts) != 6
            or (parts[0], *parts[2:5]) != ("attempts", "outcome.ready", "children", "jobs")
            or parts[1] != canonical_uuid(parts[1], "commit.children attempt")
            or parts[5] != job_key
        ):
            raise FormatError(f"commit.children staged is not attempts/<A>/outcome.ready/children/jobs/<key>: {staged}")
        placement = entry["placement"]
        if not isinstance(placement, str) or placement_text(normalize_placement(placement)) != placement:
            raise FormatError(f"commit.children placement is not canonical: {placement!r}")
        initial = _thaw(require_mapping(entry["initial_state"], "commit.children initial_state"))
        assert isinstance(initial, dict)
        if StateDoc.from_mapping(initial).job_id != job_id:
            raise FormatError("commit.children initial_state names another job")
        embedded_sha256 = entry["embedded_sha256"]
        if embedded_sha256 is not None and (
            not isinstance(embedded_sha256, str) or not _SHA256.fullmatch(embedded_sha256)
        ):
            raise FormatError("commit.children embedded_sha256 must be 64 lowercase hex digits or null")
        return cls(
            job_id=job_id,
            job_key=job_key,
            label=validate_label(entry["label"], "commit.children label"),
            spawn_id=None if spawn_id is None else canonical_uuid(spawn_id, "commit.children spawn_id"),
            placement=placement,
            priority=priority,
            staged=staged,
            initial_state=initial,
            embedded_sha256=embedded_sha256,
        )


def _real_dir(path: Path, name: str) -> bool:
    """Report whether *path* is a real directory (``False`` when absent); refuse anything else."""

    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        return False
    if not stat.S_ISDIR(mode):
        raise FormatError(f"{name} is a symlink or not a directory")
    return True


def _launched_tree(job: _kernel.OwnedJob, parent_doc: StateDoc) -> str:
    """Return the digest of the parent's ``workflow/`` its embedded-workflow children get.

    A locked parent's tree must still be the one its last attempt launched (verified against ``job.json`` then);
    an unlocked parent's children are unlocked too and launch unverified, so they get its tree as it is now.
    """

    try:
        actual = embedded_digest(job.path / JOB_WORKFLOW_DIRECTORY)
    except (FormatError, OSError) as exc:
        raise FormatError(f"the parent's workflow/ cannot be copied into its children: {exc}") from exc
    if parent_doc.unlocked is not None:
        return actual
    pin = parent_doc.workflow_pin or {}
    pinned = pin.get("tree_sha256")
    if pin.get("source") != "embedded" or not isinstance(pinned, str):
        raise FormatError("the parent's last launch pinned no workflow/ tree to copy into its children")
    if actual != pinned:
        raise FormatError(
            f"the parent's workflow/ changed since its attempt launched (digest {actual}, pinned {pinned})"
        )
    return pinned


def _spawn_entries(document: object) -> list[Mapping[str, object]]:
    spawn = require_mapping(document, "spawn.json")
    if set(spawn) != {"format", "format_version", "children"}:
        raise FormatError("spawn.json members must be format, format_version and children")
    if spawn["format"] != SPAWN_FORMAT or spawn["format_version"] != SPAWN_FORMAT_VERSION:
        raise FormatError(f"spawn.json is not {SPAWN_FORMAT} version {SPAWN_FORMAT_VERSION}")
    entries = spawn["children"]
    if not isinstance(entries, list):
        raise FormatError("spawn children must be an array")
    for raw in entries:
        entry = require_mapping(raw, "spawn child")
        if not _ENTRY_REQUIRED <= set(entry) <= _ENTRY_MEMBERS:
            raise FormatError(f"spawn child members must be {sorted(_ENTRY_REQUIRED)} plus optional ones")
    return entries


def validate_children(
    job: _kernel.OwnedJob,
    parent: JobDefinition,
    parent_doc: StateDoc,
    outcome_dir: Path,
    *,
    workspace_id: str,
) -> list[ChildPlan]:
    """Validate the children an outcome proposes and return their publication plan (commit step 1 only).

    :param job: The committing parent, owned and quiescent.
    :param parent: The parent's ``job.json``. A child of an exchange job (``origin`` ``exchange``, or a job in an
        exchange pool) is rewritten into exactly the parent's pool, whatever its runner asked for; a child of any
        other job may not name an exchange pool.
    :param parent_doc: The parent's ``state.json``; children inherit its ``origin`` and ``exchange_name``.
    :param outcome_dir: ``<job>/attempts/<A>/outcome.ready``.
    :param workspace_id: This workspace's id; children must name it.
    :return: The plans in ``spawn.json`` order; empty when the outcome spawns nothing.
    :raises httk.workflow.errors.FormatError: If ``spawn.json`` or a child payload is malformed or unsafe, a
        child id names an existing job, a child outside the exchange names an exchange pool, or a child placement is
        too long for the filesystem.
    :raises httk.workflow.errors.UnsupportedExtensionError: If a child names another workspace.
    :raises httk.workflow.errors.WorkflowError: If an attempt of *job* is still live.
    :raises ValueError: If *outcome_dir* is not an outcome directory of *job*.
    """

    job.require_quiescent()
    relative = outcome_dir.relative_to(job.path)
    if len(relative.parts) != 3 or relative.parts[0] != "attempts" or relative.parts[2] != "outcome.ready":
        raise ValueError(f"{outcome_dir} is not attempts/<A>/outcome.ready of {job.path}")
    canonical_uuid(relative.parts[1], "attempt id")
    # The job is dead (R5), so these checks cannot be raced; each real directory keeps every
    # later path-based read and rename below the job.
    current = job.path
    for part in (*relative.parts, "children"):
        current = current / part
        if not _real_dir(current, current.relative_to(job.path).as_posix()):
            return []
    try:
        data = _fs.read_bounded(_fs.loc(outcome_dir / "children" / "spawn.json"), MAX_SPAWN_BYTES, nonblock=True)
    except (_fs.UnsafePath, OSError) as exc:
        raise FormatError(f"cannot read spawn.json: {exc}") from exc
    if data is None:
        return []
    try:
        document = json.loads(data.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FormatError(f"spawn.json is not JSON: {exc}") from exc
    entries = _spawn_entries(document)
    jobs_dir = outcome_dir / "children" / "jobs"
    if entries and not _real_dir(jobs_dir, "children/jobs"):
        raise FormatError("spawn.json names children but children/jobs is missing")
    labels: set[str] = set()
    keys: set[str] = set()
    plans: list[ChildPlan] = []
    launched: str | None = (
        None  # the parent's launched workflow/ digest, checked once for its embedded-workflow children
    )
    for entry in entries:
        job_key = require_string(entry["job_key"], "spawn child job_key")
        _, child_id = parse_job_key(job_key)
        label = validate_label(entry["label"], "spawn child label")
        if label in labels:
            raise FormatError(f"spawn child label is not unique within the spawn set: {label}")
        if job_key in keys:
            raise FormatError(f"spawn child job_key is repeated: {job_key}")
        labels.add(label)
        keys.add(job_key)
        if entry.get("workspace_id", workspace_id) != workspace_id:
            raise UnsupportedExtensionError("cross-workspace spawn children are not supported")
        if entry.get("job_id", child_id) != child_id:
            raise FormatError(f"spawn child job_id disagrees with its job_key {job_key}")
        spawn_id = entry.get("spawn_id")
        spawn_id = None if spawn_id is None else canonical_uuid(spawn_id, "spawn child spawn_id")
        child_dir = jobs_dir / job_key
        if not _real_dir(child_dir, f"children/jobs/{job_key}"):
            raise FormatError(f"spawned child {job_key} has no payload directory")
        entries_found = _fs.walk_untrusted(child_dir)  # UntrustedContentError is a FormatError
        planted = TRUSTED_NAMES.intersection(e.relative.name for e in entries_found if len(e.relative.parts) == 1)
        if planted:
            raise FormatError(f"spawned child {job_key} carries trusted entries: {sorted(planted)}")
        child = JobDefinition.from_path(child_dir / "job.json")
        if child.job_key != job_key:
            raise FormatError(f"spawned child job.json names {child.job_key}, not {job_key}")
        embedded_sha256 = None
        if child.embedded is not None:
            if (
                parent.embedded is None
                or (child.workflow_id, child.workflow_name) != (parent.workflow_id, parent.workflow_name)
                or json_bytes(_thaw(child.embedded)) != json_bytes(_thaw(parent.embedded))
            ):
                raise FormatError(f"spawned child {job_key} names an embedded workflow its parent does not embed")
            launched = launched or _launched_tree(job, parent_doc)
            embedded_sha256 = launched
        placement = placement_text(child.placement)
        if entry["placement"] != placement:
            raise FormatError(f"spawned child {job_key} placement disagrees with its job.json")
        expected_parent: dict[str, object] = {
            "workspace_id": workspace_id,
            "job_id": parent.id,
            "job_key": parent.job_key,
            "placement": placement_text(parent.placement),
        }
        if spawn_id is not None:
            expected_parent["spawn_id"] = spawn_id
        recorded = child.parent or {}
        if any(recorded.get(name) != value for name, value in expected_parent.items()):
            raise FormatError(f"spawned child {job_key} does not name this parent")
        # An exchange job's runner is untrusted: its children stay in its pool (its key's, or default for a keyless
        # one), and no other job may place work in a key's pool. The parent's job.json and state.json are trusted
        # (read-only to its attempts); the rewrite is idempotent, so validating again after a crash finds the same
        # child.
        if parent_doc.origin == "exchange" or parent.claim_pool.startswith(EXCHANGE_POOL_PREFIX):
            if child.claim_pool != parent.claim_pool:
                forced = dataclasses.replace(child, claim_pool=parent.claim_pool, stored_digest=None)
                _fs.write_file(_fs.loc(child_dir / "job.json"), forced.encode(), durable=job.owner.workspace.durable)
        elif child.claim_pool.startswith(EXCHANGE_POOL_PREFIX):
            raise FormatError(f"spawned child {job_key} names the exchange pool {child.claim_pool}")
        state = StateDoc.empty(child_id).updated(origin=parent_doc.origin, exchange_name=parent_doc.exchange_name)
        if parent_doc.unlocked is not None:
            # Debug mode propagates along data flow, owner-side: an unlocked parent's children are unlocked.
            state = state.updated(unlocked={"at": state.updated_at, "inherited_from": parent.job_key})
        plans.append(
            ChildPlan(
                job_id=child_id,
                job_key=job_key,
                label=label,
                spawn_id=spawn_id,
                placement=placement,
                priority=child.priority,
                staged=(PurePosixPath(*relative.parts) / "children" / "jobs" / job_key).as_posix(),
                initial_state=state.as_mapping(),
                embedded_sha256=embedded_sha256,
            )
        )
    if plans:
        # A runner may name any placement: a child whose job directory the filesystem cannot name in any unowned
        # state fails the outcome here, not the publication (which would retry the commit forever).
        jobs = job.owner.workspace.jobs
        try:
            path_max = os.pathconf(jobs, "PC_PATH_MAX")
        except (OSError, ValueError):
            path_max = 4096
        longest_state = max(_kernel.UNOWNED_STATES, key=len)
        for plan in plans:
            name = _kernel.format_job_name(plan.job_key, 999, _fs.fresh_token())
            if (size := len(os.fsencode(jobs / longest_state / plan.placement / name))) + _PATH_HEADROOM >= path_max:
                raise FormatError(
                    f"spawned child placement is too long for the filesystem: {plan.job_key} needs a {size}-byte path"
                )
        # The runner chose the ids: one already taken (owned jobs included) would give two jobs one id.
        placements = [normalize_placement(plan.placement) for plan in plans] + [parent.placement]
        ids = [plan.job_id for plan in plans]
        try:
            taken = _kernel.locate_many(job.owner.workspace, ids, placements=placements, settle=False)
        except OSError as exc:
            # A runner may name any placement; one the filesystem cannot hold fails the outcome, not the manager.
            if exc.errno != errno.ENAMETOOLONG:
                raise
            raise FormatError(f"spawned child placement is too long for the filesystem: {exc}") from exc
        if taken:
            ref = taken[min(taken)]
            raise FormatError(f"spawned child id {ref.job_id} is already the job {ref.job_key} ({ref.state})")
    return plans


def publish_children(job: _kernel.OwnedJob, plans: Sequence[ChildPlan]) -> list[_kernel.JobRef]:
    """Publish planned children into ``jobs/ready/<placement>/``; replay-safe, never re-validating.

    A staged directory that is gone was published by an earlier run of this commit.

    :param job: The committing parent, owned.
    :param plans: The plans :func:`validate_children` returned.
    :return: The references of the children this call published.
    :raises httk.workflow._kernel.OwnerLost: When the parent was taken from this owner.
    """

    published: list[_kernel.JobRef] = []
    durable = job.owner.workspace.durable
    for plan in plans:
        staged = job.path / plan.staged
        if not _fs.exists(_fs.loc(staged)):
            continue
        if plan.embedded_sha256 is not None:
            _place_workflow(job, plan, durable=durable)
        _fs.write_file(
            _fs.loc(staged / "state.json"), encode_state(StateDoc.from_mapping(plan.initial_state)), durable=durable
        )
        published.append(_kernel.submit(job.owner.workspace, job.owner, staged, state="ready"))
    return published


def _place_workflow(job: _kernel.OwnedJob, plan: ChildPlan, *, durable: bool) -> None:
    """Copy the parent's ``workflow/`` into a staged embedded-workflow child, verified; replay-safe.

    The copy is assembled at :data:`_PARTIAL_WORKFLOW` (a name validation refuses in a proposed child) and renamed
    to ``workflow``, so a ``workflow/`` with the planned digest is complete and kept; anything else is recopied.
    """

    assert plan.embedded_sha256 is not None
    target = job.path / plan.staged / JOB_WORKFLOW_DIRECTORY
    job.discard_subtree(f"{plan.staged}/{_PARTIAL_WORKFLOW}")  # what a crash during a copy left
    if os.path.lexists(target):
        try:
            if embedded_digest(target) == plan.embedded_sha256:
                return
        except (FormatError, OSError):
            pass
        job.discard_subtree(f"{plan.staged}/{JOB_WORKFLOW_DIRECTORY}")
    partial = job.path / plan.staged / _PARTIAL_WORKFLOW
    _fs.copy_tree(job.path / JOB_WORKFLOW_DIRECTORY, partial, durable=durable, limits=_fs.DEFAULT_LIMITS)
    if (actual := embedded_digest(partial)) != plan.embedded_sha256:
        # ponytail: the parent is owned and quiescent from validation on, so only an operator changes its tree in
        # between; the commit then fails here on every pass (reported) instead of failing the job.
        raise FormatError(f"the parent's workflow/ changed since its children were validated (digest {actual})")
    _fs.move_owned(_fs.loc(partial), _fs.loc(target), durable=durable)


def labeled_join(join: Mapping[str, object], plans: Sequence[ChildPlan]) -> dict[str, object]:
    """Return a ``wait`` outcome's join whose child references carry the labels of the spawn set.

    :param join: The outcome's join object.
    :param plans: The validated plans of the same outcome.
    :return: The join with missing labels filled in from the spawn set.
    """

    labels = {plan.job_key: plan.label for plan in plans}
    children = join.get("children")
    if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
        return dict(join)
    referenced: list[object] = []
    for raw in children:
        if isinstance(raw, Mapping) and raw.get("label") is None and raw.get("job_key") in labels:
            raw = {**raw, "label": labels[raw["job_key"]]}
        referenced.append(raw)
    return {**join, "children": referenced}


def newest_labeled(children: Sequence[Mapping[str, object]]) -> dict[str, dict[str, object]]:
    """Return the newest published child entry per label: the children a spawn or call of that label reuses.

    :param children: A parent's ``state.json`` ``children`` records, oldest first; label-less entries name none.
    :return: The entries by label.
    """

    newest: dict[str, dict[str, object]] = {}
    for item in children:
        entry = cast(dict[str, object], _thaw(item))
        if entry.get("label") is not None:
            newest[str(entry["label"])] = entry
    return newest
