"""Workflow workspace creation, submission, marker discovery, and transitions."""

import errno
import logging
import os
import re
import shutil
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from ._util import (
    fsync_directory,
    read_json,
    retry_delay,
    sha256_file,
    tree_digest,
    utc_now,
    write_json_atomic,
)
from .errors import (
    FormatError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
    WorkspaceCorruptionError,
    WorkspaceUnavailableError,
)
from .journal import JournalWriter, read_record
from .models import (
    CORE_PROFILE,
    CORE_STATE_KINDS,
    READABLE_CORE_PROFILES,
    STATE_KINDS,
    SUPPORTED_EXTENSIONS,
    JobDefinition,
    Marker,
    is_payload_private,
    marker_basename,
    normalize_placement,
    validate_runner_path,
)

_LOGGER = logging.getLogger(__name__)
# Anything below state/ that cannot possibly be a marker basename is ignored
# silently: NFS silly-renames, editor droppings, and other foreign files are
# not protocol entries and must never stop a scan or reach quarantine.
_MARKER_SHAPE_PATTERN = re.compile(r"\.p[0-9]{3}\.g[0-9a-z]+\.")


@dataclass(frozen=True)
class MarkerFault:
    """One state entry shaped like a marker that cannot be interpreted."""

    path: Path
    reason: str


def _marker_shaped(name: str) -> bool:
    """Report whether *name* is shaped enough like a marker to be validated."""

    return not name.startswith(".") and _MARKER_SHAPE_PATTERN.search(name) is not None


class WorkflowWorkspace:
    """One self-contained httk workflow filesystem workspace."""

    def __init__(self, root: str | os.PathLike[str], *, mutable: bool = True, durable: bool = False) -> None:
        self.root = Path(root).resolve()
        self.control = self.root / ".httk-workflow"
        self.runners = self.control / "runners"
        self.durable = durable
        self._reported_faults: set[Path] = set()
        self.format = read_json(self.control / "format.json")
        if self.format.get("format") != "httk-workflow-filesystem" or self.format.get("format_version") != 1:
            raise FormatError("workspace must use httk-workflow-filesystem format version 1")
        self.core_profile = self.format.get("core_profile")
        if self.core_profile not in READABLE_CORE_PROFILES:
            raise UnsupportedExtensionError(f"unsupported core profile: {self.core_profile!r}")
        if mutable and self.core_profile != CORE_PROFILE:
            # An older profile can be read and exported, never written: its
            # spawn sets, join summaries, and runner references predate the
            # shapes this implementation now publishes.
            raise UnsupportedExtensionError(
                f"workspace core profile {self.core_profile!r} cannot be mutated by this "
                f"implementation, which writes {CORE_PROFILE!r}; attach read-only to inspect it"
            )
        extensions_raw = self.format.get("extensions", [])
        if not isinstance(extensions_raw, list) or not all(isinstance(item, str) for item in extensions_raw):
            raise FormatError("workspace extensions must be an array of strings")
        self.extensions = frozenset(extensions_raw)
        unsupported = self.extensions - SUPPORTED_EXTENSIONS
        if mutable and unsupported:
            raise UnsupportedExtensionError(f"unsupported enabled extensions: {', '.join(sorted(unsupported))}")
        if self.format.get("record_ref_encoding") != "hwref-v1":
            raise UnsupportedExtensionError("unsupported record reference encoding")
        self.workspace_id = str(self.format.get("workspace_id"))
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except ValueError as exc:
            raise FormatError("workspace_id must be a canonical UUID") from exc
        self.priority_bands = "priority-bands-v1" in self.extensions

    @classmethod
    def initialize(
        cls,
        root: str | os.PathLike[str],
        *,
        extensions: Iterable[str] = (),
        durable: bool = False,
    ) -> "WorkflowWorkspace":
        """Create and return a new workspace."""

        root_path = Path(root).resolve()
        root_path.mkdir(parents=True, exist_ok=True)
        extension_set = frozenset(extensions)
        unsupported = extension_set - SUPPORTED_EXTENSIONS
        if unsupported:
            raise UnsupportedExtensionError(f"unsupported extensions: {', '.join(sorted(unsupported))}")
        name_max = os.pathconf(root_path, "PC_NAME_MAX")
        if name_max < 213:
            raise FormatError(f"filesystem NAME_MAX {name_max} is below the {CORE_PROFILE} requirement of 213")
        control = root_path / ".httk-workflow"
        control.mkdir(exist_ok=False)
        for relative in (
            "tmp",
            "quarantine",
            "journal",
            "managers",
            "runners",
            "requests/tmp",
            "requests/ready",
            "requests/claimed",
            "state/submitted",
        ):
            (control / relative).mkdir(parents=True, exist_ok=True)
        if "detached-transfer-v1" in extension_set:
            for relative in ("transfers/acks", "transfers/imported", "transfers/incoming", "transfers/retired"):
                (control / relative).mkdir(parents=True, exist_ok=True)
        write_json_atomic(
            control / "format.json",
            {
                "format": "httk-workflow-filesystem",
                "format_version": 1,
                "core_profile": CORE_PROFILE,
                "extensions": sorted(extension_set),
                "record_ref_encoding": "hwref-v1",
                "workspace_id": str(uuid.uuid4()),
                "created_at": utc_now(),
            },
            durable=durable,
        )
        return cls(root_path, durable=durable)

    def upgrade(self, extensions: Iterable[str]) -> frozenset[str]:
        """Enable extensions that have an implemented in-place migration."""

        requested = frozenset(extensions)
        unknown = requested - SUPPORTED_EXTENSIONS
        if unknown:
            raise UnsupportedExtensionError(f"no implemented migration for extensions: {', '.join(sorted(unknown))}")
        additions = requested - self.extensions
        unsupported_migrations = additions - {"detached-transfer-v1"}
        if unsupported_migrations:
            raise UnsupportedExtensionError(
                "existing workspaces can only enable detached-transfer-v1; "
                f"no implemented migration for: {', '.join(sorted(unsupported_migrations))}"
            )
        if "detached-transfer-v1" in additions:
            for relative in ("transfers/acks", "transfers/imported", "transfers/incoming", "transfers/retired"):
                (self.control / relative).mkdir(parents=True, exist_ok=True)
            updated = dict(self.format)
            updated["extensions"] = sorted(self.extensions | additions)
            write_json_atomic(self.control / "format.json", updated, durable=self.durable)
            self.format = updated
            self.extensions = frozenset(updated["extensions"])
        return self.extensions

    def runner_store_path(self, path: str | PurePosixPath) -> Path:
        """Return the store location of one workspace runner.

        The store is flat and name-keyed below ``.httk-workflow/runners/``.
        Relative subdirectories are permitted so a campaign can group runners,
        but a name can never escape the store.
        """

        relative = validate_runner_path(str(PurePosixPath(path)), "workspace")
        resolved = self.runners.joinpath(*relative.parts)
        root = self.runners.resolve()
        if not Path(os.path.normpath(resolved)).is_relative_to(root):
            raise FormatError(f"runner name must remain below the workspace runner store: {relative}")
        return resolved

    def publish_runner(
        self,
        source: str | os.PathLike[str],
        *,
        name: str | PurePosixPath | None = None,
        replace: bool = False,
    ) -> dict[str, object]:
        """Install one runner in the workspace store and describe the reference.

        Publication is content addressed: republishing identical bytes is an
        idempotent no-op, and replacing a name whose content differs requires
        *replace* so a live campaign referring to the old digest can never be
        changed underneath by accident.
        """

        source_path = Path(source).expanduser()
        if source_path.is_symlink() or not source_path.is_file():
            raise FormatError(f"a published runner must be a regular file: {source_path}")
        digest = sha256_file(source_path)
        target = self.runner_store_path(name if name is not None else source_path.name)
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise FormatError(f"workspace runner store entry is not a regular file: {target}")
            existing = sha256_file(target)
            if existing == digest:
                _LOGGER.debug("workspace runner %s already holds digest %s", target, digest)
            elif not replace:
                raise FileExistsError(
                    f"workspace runner {target.relative_to(self.runners).as_posix()} already holds a "
                    f"different digest {existing}; pass replace to overwrite it"
                )
            else:
                self._install_runner_file(source_path, target)
        else:
            self._install_runner_file(source_path, target)
        relative = target.relative_to(self.runners)
        _LOGGER.info(
            "published workspace runner %s with digest %s",
            relative.as_posix(),
            digest,
            extra={"event": "runner_published", "runner": relative.as_posix(), "sha256": digest},
        )
        return {"source": "workspace", "path": relative.as_posix(), "sha256": digest}

    def _install_runner_file(self, source: Path, target: Path) -> None:
        """Atomically replace one store entry with the bytes of *source*."""

        target.parent.mkdir(parents=True, exist_ok=True)
        staging = self.control / "tmp" / f"runner.{uuid.uuid4()}"
        staging.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, staging)
        staging.chmod(0o555)
        try:
            os.replace(staging, target)
        finally:
            staging.unlink(missing_ok=True)

    def detach(
        self,
        job_id: str,
        *,
        destination_workspace_id: str,
        destination_placement: str | PurePosixPath | None = None,
        transfer_id: str | None = None,
    ) -> Path:
        """Seal one quiescent job as a detached transfer bundle."""

        from .transfers import detach_job

        return detach_job(
            self,
            job_id,
            destination_workspace_id=destination_workspace_id,
            destination_placement=destination_placement,
            transfer_id=transfer_id,
        )

    def import_bundle(self, bundle: str | os.PathLike[str]) -> dict[str, object]:
        """Import a validated detached transfer bundle."""

        from .transfers import import_bundle

        return import_bundle(self, bundle)

    def acknowledge_transfer(self, acknowledgement: Mapping[str, object]) -> Path:
        """Retire a source bundle after destination acknowledgement."""

        from .transfers import acknowledge_transfer

        return acknowledge_transfer(self, acknowledgement)

    def recover_transfers(self) -> list[dict[str, object]]:
        """Recover or report interrupted detached-transfer publications."""

        from .transfers import recover_transfers

        return recover_transfers(self)

    def state_directory(self, kind: str, placement: PurePosixPath, *, priority: int) -> Path:
        if kind not in STATE_KINDS:
            raise ValueError(f"unknown state kind: {kind}")
        result = self.control / "state" / kind
        if self.priority_bands and kind == "ready":
            result = result / f"p{priority // 100}xx"
        return result.joinpath(*placement.parts)

    def marker_path(
        self,
        kind: str,
        placement: PurePosixPath,
        job_key: str,
        priority: int,
        generation: int,
        record_ref: str,
    ) -> Path:
        return self.state_directory(kind, placement, priority=priority) / marker_basename(
            job_key, priority, generation, record_ref
        )

    def payload_path(self, placement: PurePosixPath, job_key: str) -> Path:
        return self.root.joinpath(*placement.parts, job_key)

    def scan_marker_entries(self, kinds: Iterable[str] | None = None) -> Iterator[Marker | MarkerFault]:
        """Yield every marker below ``state/``, reporting damage per entry.

        One unusable entry must never hide the rest of the workspace, so a
        marker-shaped basename that fails validation is reported as a
        :class:`MarkerFault` instead of aborting the scan.
        """

        selected = tuple(kinds or CORE_STATE_KINDS)
        state_root = self.control / "state"
        for kind in selected:
            directory = state_root / kind
            if not directory.exists():
                continue
            for path in directory.rglob("*"):
                if not path.is_file():
                    continue
                if not _marker_shaped(path.name):
                    _LOGGER.debug("ignoring foreign file below state: %s", path)
                    continue
                try:
                    yield Marker.from_path(state_root, path, priority_bands=self.priority_bands)
                except (WorkflowError, ValueError) as exc:
                    yield MarkerFault(path=path, reason=str(exc))

    def scan_markers(self, kinds: Iterable[str] | None = None) -> Iterable[Marker]:
        for entry in self.scan_marker_entries(kinds):
            if isinstance(entry, Marker):
                yield entry
            else:
                self.report_marker_fault(entry)

    def report_marker_fault(self, fault: MarkerFault) -> None:
        """Report an uninterpretable state entry loudly once, then quietly.

        A marker whose basename or placement cannot be parsed is workspace
        corruption rather than a job state: the core profile leaves its repair
        to an explicit workspace tool, so a manager only reports it and never
        schedules or relocates it.
        """

        message = "unusable state entry %s: %s (repair it with a workspace tool)"
        if fault.path in self._reported_faults:
            _LOGGER.debug(message, fault.path, fault.reason)
            return
        self._reported_faults.add(fault.path)
        _LOGGER.error(message, fault.path, fault.reason, extra={"event": "marker_fault", "entry": str(fault.path)})

    def find_markers(self, job_key: str, kinds: Iterable[str] | None = None) -> list[Marker]:
        return [marker for marker in self.scan_markers(kinds) if marker.job_key == job_key]

    def find_marker_by_id(self, job_id: str) -> Marker | None:
        matches = [marker for marker in self.scan_markers() if marker.job_id == job_id]
        if len(matches) > 1:
            raise WorkspaceCorruptionError(f"job {job_id} has more than one state marker")
        return matches[0] if matches else None

    def find_marker_at(self, job_key: str, placement: PurePosixPath) -> Marker | None:
        """Find *job_key* by checking the finite state set at a placement."""

        matches: list[Marker] = []
        state_root = self.control / "state"
        for kind in CORE_STATE_KINDS:
            directory = self.state_directory(kind, placement, priority=0)
            directories = [directory]
            if self.priority_bands and kind == "ready":
                directories = [
                    self.control / "state" / "ready" / f"p{band}xx" / Path(*placement.parts) for band in range(10)
                ]
            for candidate_dir in directories:
                if not candidate_dir.is_dir():
                    continue
                for path in candidate_dir.glob(f"{job_key}.p???.g*.*"):
                    if not path.is_file():
                        continue
                    try:
                        matches.append(Marker.from_path(state_root, path, priority_bands=self.priority_bands))
                    except (WorkflowError, ValueError) as exc:
                        self.report_marker_fault(MarkerFault(path=path, reason=str(exc)))
        if len(matches) > 1:
            raise WorkspaceCorruptionError(f"job {job_key} has multiple markers at {placement}")
        return matches[0] if matches else None

    def load_job(self, marker: Marker) -> JobDefinition:
        path = self.payload_path(marker.placement, marker.job_key) / "job.json"
        job = JobDefinition.from_path(path)
        if job.job_key != marker.job_key:
            raise FormatError("job.json identity disagrees with marker")
        if job.priority != marker.priority and marker.generation == 0:
            raise FormatError("submitted marker priority disagrees with job.json")
        return job

    def read_state(self, marker: Marker) -> dict[str, Any]:
        if marker.record_ref == "init":
            return {
                "format": "httk-workflow-state",
                "format_version": 1,
                "workspace_id": self.workspace_id,
                "job_id": marker.job_id,
                "job_key": marker.job_key,
                "placement": marker.placement.as_posix(),
                "state_generation": 0,
                "kind": "submitted",
                "previous_record_ref": None,
                "created_at": None,
                "priority": marker.priority,
            }
        frame = read_record(self.control, marker.record_ref)
        if (
            frame.get("format") != "httk-workflow-state"
            or frame.get("format_version") != 1
            or frame.get("workspace_id") != self.workspace_id
            or frame.get("job_key") != marker.job_key
            or frame.get("state_generation") != marker.generation
            or frame.get("kind") != marker.kind
        ):
            raise WorkspaceCorruptionError(f"state frame disagrees with marker {marker.path}")
        return frame

    def transition(
        self,
        writer: JournalWriter,
        marker: Marker,
        kind: str,
        updates: Mapping[str, object],
        *,
        priority: int | None = None,
    ) -> Marker:
        """Append a state frame and atomically move *marker* to it."""

        next_priority = marker.priority if priority is None else priority
        generation = marker.generation + 1
        if generation > (1 << 64) - 1:
            raise WorkspaceCorruptionError("state generation exhausted")
        frame: dict[str, object] = {
            "format": "httk-workflow-state",
            "format_version": 1,
            "workspace_id": self.workspace_id,
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": marker.placement.as_posix(),
            "state_generation": generation,
            "kind": kind,
            "previous_record_ref": None if marker.record_ref == "init" else marker.record_ref,
            "created_at": utc_now(),
            "priority": next_priority,
        }
        frame.update(updates)
        record_ref = writer.append(frame)
        destination = self.marker_path(kind, marker.placement, marker.job_key, next_priority, generation, record_ref)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _LOGGER.debug(
            "moving marker %s from %s to %s at generation %d",
            marker.job_key,
            marker.kind,
            kind,
            generation,
        )
        return self._verified_marker_rename(marker, destination)

    def _verified_marker_rename(self, marker: Marker, destination: Path, *, attempts: int = 7) -> Marker:
        source = marker.path
        state_root = self.control / "state"
        last_error: OSError | None = None
        for attempt in range(attempts):
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.rename(source, destination)
            except OSError as exc:
                last_error = exc
                _LOGGER.warning(
                    "marker rename %s -> %s reported %s on attempt %d; verifying the destination",
                    source,
                    destination,
                    exc,
                    attempt + 1,
                )
            if destination.is_file():
                return Marker.from_path(state_root, destination, priority_bands=self.priority_bands)
            if source.is_file():
                _LOGGER.debug("marker %s is not yet visible at %s; retrying", source, destination)
                time.sleep(retry_delay(attempt))
                continue
            current = self.find_markers(marker.job_key)
            if len(current) == 1:
                raise TransitionLostError(f"another transition moved {source} to {current[0].path}")
            if len(current) > 1:
                raise WorkspaceCorruptionError(f"job {marker.job_key} has multiple markers")
            time.sleep(retry_delay(attempt))
        detail = f": {last_error}" if last_error is not None else ""
        raise WorkspaceUnavailableError(f"cannot resolve marker rename {source} -> {destination}{detail}")

    def _publish_path(self, source: Path, destination: Path, *, attempts: int = 7) -> None:
        last_error: OSError | None = None
        for attempt in range(attempts):
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.rename(source, destination)
            except OSError as exc:
                last_error = exc
                _LOGGER.warning(
                    "publication %s -> %s reported %s on attempt %d; verifying the destination",
                    source,
                    destination,
                    exc,
                    attempt + 1,
                )
            if destination.exists():
                if not source.exists():
                    return
                try:
                    if source.samefile(destination):
                        time.sleep(retry_delay(attempt))
                        continue
                except OSError:
                    pass
                raise FileExistsError(f"publication destination already exists: {destination}")
            if source.exists():
                time.sleep(retry_delay(attempt))
                continue
            time.sleep(retry_delay(attempt))
        detail = f": {last_error}" if last_error else ""
        raise WorkspaceUnavailableError(f"cannot resolve publication {source} -> {destination}{detail}")

    def submit(
        self,
        source: str | os.PathLike[str],
        placement: str | PurePosixPath,
        *,
        move: bool = False,
    ) -> Marker:
        """Copy or move a complete payload into the workspace and publish it."""

        source_path = Path(source).resolve()
        job = JobDefinition.from_path(source_path / "job.json")
        if job.data_mode == "transactional" and "transactional-data-v1" not in self.extensions:
            raise UnsupportedExtensionError("job requires transactional-data-v1")
        normalized_placement = normalize_placement(placement)
        target = self.payload_path(normalized_placement, job.job_key)
        if target.exists():
            raise FileExistsError(target)
        staging = self.control / "tmp" / f"submit.{uuid.uuid4()}"
        if move:
            try:
                os.rename(source_path, staging)
            except OSError as exc:
                if exc.errno == errno.EXDEV:
                    raise WorkflowWorkspaceError("move submission must remain on one filesystem") from exc
                raise
        else:
            shutil.copytree(source_path, staging, symlinks=False)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._publish_path(staging, target)
        temporary_marker = self.control / "tmp" / f"marker.{uuid.uuid4()}"
        temporary_marker.touch(exist_ok=False)
        if self.durable:
            fsync_directory(temporary_marker.parent)
        destination = self.marker_path("submitted", normalized_placement, job.job_key, job.priority, 0, "init")
        self._publish_path(temporary_marker, destination)
        return Marker.from_path(self.control / "state", destination, priority_bands=self.priority_bands)

    def validate_job_payload(self, marker: Marker) -> JobDefinition:
        """Perform manager-side immutable submission validation."""

        job = self.load_job(marker)
        if job.data_mode == "transactional" and "transactional-data-v1" not in self.extensions:
            raise UnsupportedExtensionError("job requires transactional-data-v1")
        return job

    def quarantine(self, path: Path, *, reason: str) -> Path:
        """Move a malformed protocol entry into the canonical quarantine."""

        identifier = f"{int(time.time())}-{uuid.uuid4()}"
        destination = self.control / "quarantine" / identifier
        destination.mkdir(parents=True)
        moved = destination / "entry"
        os.rename(path, moved)
        write_json_atomic(
            destination / "report.json",
            {"original_path": str(path), "reason": reason, "quarantined_at": utc_now()},
            durable=self.durable,
        )
        _LOGGER.warning(
            "quarantined %s as %s: %s",
            path,
            destination,
            reason,
            extra={"event": "quarantine", "entry": str(path), "quarantine": str(destination)},
        )
        return destination

    def payload_digest(self, marker: Marker) -> str:
        """Return the digest of one payload, ignoring runner-private entries."""

        return tree_digest(
            self.payload_path(marker.placement, marker.job_key),
            skip=is_payload_private,
        )

    def publish_request(self, request: Mapping[str, object]) -> Path:
        """Atomically publish an operator request."""

        temporary = self.control / "requests" / "tmp" / f"{uuid.uuid4()}.json"
        ready = self.control / "requests" / "ready" / temporary.name
        write_json_atomic(temporary, dict(request), durable=self.durable)
        self._publish_path(temporary, ready)
        return ready


class WorkflowWorkspaceError(WorkspaceUnavailableError):
    """A workspace operation could not be completed."""
