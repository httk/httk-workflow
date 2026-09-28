"""The parent/child tree metadata kept inside job payloads.

A parent's spawned children and a child's detachment from its parent are facts
about the job tree rather than scheduling state, so they live in the reserved
``.httk-job/tree/`` directory of the payload (the same payload-private area
as ``.httk-job/declarations/``). There they travel with every transfer bundle,
stay outside payload digests and seals, and survive every state transition
without being carried forward frame by frame.

``spawns/<attempt-id>.json`` records the children one committed outcome
registered. Each fragment is written once, before any of its children is moved
into place, and a replayed commit must find it byte-identical, so a torn or
diverging record is refused rather than merged. ``detached.json`` marks a child
an operator has made independent of its parent.
"""

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

from ._util import fsync_directory, fsync_file, json_bytes, read_json, utc_now, write_json_atomic
from .errors import FormatError, WorkflowError, WorkspaceCorruptionError
from .models import (
    CORE_STATE_KINDS,
    JOB_STATE_DIRECTORY,
    JobDefinition,
    Marker,
    normalize_placement,
    parse_job_key,
)
from .workspace import Workspace

TREE_DIRECTORY = "tree"
SPAWNS_DIRECTORY = "spawns"
DETACHED_FILE = "detached.json"
SPAWNS_FORMAT = "httk-workflow-spawns"
DETACHED_FORMAT = "httk-workflow-detached"
_FRAGMENT_MEMBERS = ("job_id", "job_key", "label", "placement", "spawn_id")
# What the protocol requires of every spawn entry; the job id follows from the
# key, and a spawn id, which a hand-written outcome may omit, is recorded as null.
_REQUIRED_MEMBERS = ("job_key", "label", "placement")


def tree_directory(payload: Path) -> Path:
    """Return the reserved tree-metadata directory of one payload."""

    return payload / JOB_STATE_DIRECTORY / TREE_DIRECTORY


def record_spawns(payload: Path, attempt_id: str, entries: Sequence[Mapping[str, object]], *, durable: bool) -> None:
    """Publish the children one outcome spawns, before any child is registered.

    :param payload: The spawning job's payload directory.
    :param attempt_id: The attempt whose outcome spawned the children.
    :param entries: The outcome's ``spawn.json`` child entries.
    :param durable: Whether to synchronize the fragment and its directories.
    :raises httk.workflow.errors.FormatError: If an entry lacks its key, label, or placement.
    :raises httk.workflow.errors.WorkspaceCorruptionError: If a replay finds a different fragment.
    """

    children: list[dict[str, object]] = []
    for entry in entries:
        if not all(isinstance(entry.get(name), str) for name in _REQUIRED_MEMBERS):
            raise FormatError(f"spawn child entry must carry string {', '.join(_REQUIRED_MEMBERS)}")
        spawn_id = entry.get("spawn_id")
        children.append(
            {
                "job_id": parse_job_key(str(entry["job_key"]))[1],
                "job_key": entry["job_key"],
                "label": entry["label"],
                "placement": entry["placement"],
                "spawn_id": spawn_id if isinstance(spawn_id, str) else None,
            }
        )
    document = {"format": SPAWNS_FORMAT, "format_version": 1, "children": children}
    content = json_bytes(document) + b"\n"
    directory = tree_directory(payload) / SPAWNS_DIRECTORY
    path = directory / f"{attempt_id}.json"
    if path.is_file():
        existing = path.read_bytes()
        if existing == content:
            if durable:
                # An earlier writer may have died between its rename and its syncs.
                fsync_file(path)
                fsync_directory(directory)
            return
        try:
            json.loads(existing)
        except ValueError:
            # A torn write of the non-durable profile: nothing was published from
            # it, so the deterministic content simply replaces it.
            pass
        else:
            raise WorkspaceCorruptionError(f"spawn record {path} disagrees with the outcome being committed")
    created = _make_directories(payload, directory)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.tmp.", dir=directory)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            if durable:
                os.fsync(handle.fileno())
        # A concurrent replay publishes the same bytes, so either rename wins.
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    if durable:
        for synchronized in (directory, *created):
            fsync_directory(synchronized)


def _make_directories(payload: Path, directory: Path) -> list[Path]:
    """Create *directory* below *payload*; return the parents whose entries changed."""

    changed: list[Path] = []
    current = payload
    for part in directory.relative_to(payload).parts:
        current = current / part
        if not current.is_dir():
            current.mkdir(exist_ok=True)
            changed.append(current.parent)
    return changed


def spawned_children(payload: Path) -> list[dict[str, str | None]]:
    """Return every child the recorded outcomes of *payload* spawned, once each.

    :param payload: The parent job's payload directory.
    :return: Child entries, grouped by fragment (ordered by attempt id) in spawn order.
    :raises httk.workflow.errors.FormatError: If a spawn record is malformed.
    """

    directory = tree_directory(payload) / SPAWNS_DIRECTORY
    if not directory.is_dir():
        return []
    seen: set[str] = set()
    children: list[dict[str, str | None]] = []
    for path in sorted(directory.glob("*.json")):
        document = read_json(path)
        if document.get("format") != SPAWNS_FORMAT or document.get("format_version") != 1:
            raise FormatError(f"unsupported spawn record {path}")
        entries = document.get("children")
        if not isinstance(entries, list):
            raise FormatError(f"spawn record {path} has no children array")
        for entry in entries:
            if (
                not isinstance(entry, Mapping)
                or not all(isinstance(entry.get(name), str) for name in ("job_id", *_REQUIRED_MEMBERS))
                or not isinstance(entry.get("spawn_id"), str | None)
            ):
                raise FormatError(f"malformed child entry in spawn record {path}")
            if entry["job_id"] not in seen:
                seen.add(entry["job_id"])
                children.append({name: entry[name] for name in _FRAGMENT_MEMBERS})
    return children


def is_detached(payload: Path) -> bool:
    """Report whether the job at *payload* was detached from its parent."""

    return (tree_directory(payload) / DETACHED_FILE).is_file()


def mark_detached(payload: Path, *, operator: str | None, durable: bool) -> bool:
    """Detach the job at *payload* from its parent, permanently.

    :param payload: The child job's payload directory.
    :param operator: The operator identity recorded with the detachment.
    :param durable: Whether to synchronize the record and its directories.
    :return: Whether this call detached the job (``False`` when it already was).
    """

    path = tree_directory(payload) / DETACHED_FILE
    if path.is_file():
        return False
    created = _make_directories(payload, path.parent)
    write_json_atomic(
        path,
        {"format": DETACHED_FORMAT, "format_version": 1, "detached_at": utc_now(), "operator": operator},
        durable=durable,
    )
    if durable:
        for synchronized in created:
            fsync_directory(synchronized)
    return True


def live_parent(workspace: Workspace, parent: Mapping[str, object] | None) -> Marker | None:
    """Return the current marker of a recorded parent that is still in *workspace*.

    Only the finite state set at the parent's recorded placement is probed, so
    this never scans. A parent that is transferring or already transferred has
    no live marker here, which is what frees its children to follow it.

    :param workspace: The workspace to look in.
    :param parent: The ``parent`` member of a child's ``job.json``.
    :return: The parent's live marker, or ``None``.
    """

    if parent is None:
        return None
    job_key, placement, job_id = parent.get("job_key"), parent.get("placement"), parent.get("job_id")
    if not isinstance(job_key, str) or not isinstance(placement, str):
        return None
    try:
        if parse_job_key(job_key)[1] != job_id:
            return None
        marker = workspace.find_marker_at(job_key, PurePosixPath(placement))
    except (FormatError, ValueError):
        return None
    if marker is None or marker.kind == "transferring":
        return None
    return marker


def bound_parent(workspace: Workspace, payload: Path, job: JobDefinition) -> Marker | None:
    """Return the live parent marker a job is bound to, or ``None`` when it is free.

    A spawned child is bound to its parent while it is not detached and its
    parent is still live in the same workspace; a bound child leaves only with
    its parent's tree.

    :param workspace: The workspace holding the job.
    :param payload: The job's payload directory.
    :param job: The job's immutable definition.
    :return: The parent's live marker, or ``None``.
    """

    if job.parent is None or is_detached(payload):
        return None
    return live_parent(workspace, job.parent)


def tree_children(workspace: Workspace, payload: Path, job: JobDefinition) -> list[tuple[Marker, JobDefinition]]:
    """Return the live children bound to one job, in spawn order.

    Children come from the parent's spawn records and are confirmed from the
    child side: each must have a live marker at its recorded placement, name this
    job and the recorded spawn in its own ``job.json``, and not be detached. A
    record naming anything else is ignored, so a stale or foreign entry can
    never pull an unrelated job into a tree. Only the recorded placements are
    probed; nothing is scanned.

    :param workspace: The workspace holding the parent.
    :param payload: The parent's payload directory.
    :param job: The parent's immutable definition.
    :return: The bound children with their definitions.
    :raises httk.workflow.errors.FormatError: If a spawn record is malformed.
    """

    entries = spawned_children(payload)
    # Siblings usually share one placement: list each placement once rather than
    # probing it per child, which would be quadratic in a large fan-out.
    live: dict[tuple[str, str], Marker] = {}
    for placement in dict.fromkeys(str(entry["placement"]) for entry in entries):
        try:
            live.update(((placement, marker.job_key), marker) for marker in _markers_at(workspace, placement))
        except (FormatError, ValueError):
            continue
    children: list[tuple[Marker, JobDefinition]] = []
    for entry in entries:
        child_marker = live.get((str(entry["placement"]), str(entry["job_key"])))
        if child_marker is None or child_marker.job_id != entry["job_id"]:
            continue
        child_payload = workspace.payload_path(child_marker.placement, child_marker.job_key)
        try:
            child = JobDefinition.from_path(child_payload / "job.json")
        except (FormatError, OSError):
            continue
        parent = child.parent
        if parent is None or parent.get("job_id") != job.id:
            continue
        if entry["spawn_id"] is not None and parent.get("spawn_id") != entry["spawn_id"]:
            continue
        if is_detached(child_payload):
            continue
        children.append((child_marker, child))
    return children


def _markers_at(workspace: Workspace, placement: str) -> list[Marker]:
    """Return every live marker directly at one placement, one listing per state kind."""

    normalized = normalize_placement(placement)
    state_root = workspace.control / "state"
    markers: list[Marker] = []
    for kind in CORE_STATE_KINDS:
        directory = workspace.state_directory(kind, normalized)
        if not directory.is_dir():
            continue
        for path in directory.iterdir():
            if not path.is_file():
                continue
            try:
                markers.append(Marker.from_path(state_root, path))
            except (WorkflowError, ValueError):
                continue
    return markers


def descendant_ids(workspace: Workspace, payload: Path, job: JobDefinition) -> set[str]:
    """Return the ids of every non-detached descendant recorded below one job.

    Unlike :func:`tree_children` this follows payloads rather than live markers,
    so it also reaches members that are already sealed for transfer (a sealed
    payload stays in place until it is retired). That is what lets a resumed
    tree transfer find the members an interruption left in any state.

    :param workspace: The workspace holding the tree.
    :param payload: The root's payload directory.
    :param job: The root's immutable definition.
    :return: The descendant job ids.
    :raises httk.workflow.errors.FormatError: If a spawn record is malformed.
    """

    found: set[str] = set()
    queue = [(payload, job)]
    while queue:
        parent_payload, parent_job = queue.pop()
        for entry in spawned_children(parent_payload):
            try:
                child_payload = workspace.payload_path(PurePosixPath(str(entry["placement"])), str(entry["job_key"]))
                child = JobDefinition.from_path(child_payload / "job.json")
            except (FormatError, OSError, ValueError):
                continue
            recorded = child.parent
            if recorded is None or recorded.get("job_id") != parent_job.id or child.id in found:
                continue
            if entry["spawn_id"] is not None and recorded.get("spawn_id") != entry["spawn_id"]:
                continue
            if is_detached(child_payload):
                continue
            found.add(child.id)
            queue.append((child_payload, child))
    return found
