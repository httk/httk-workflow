"""Bounded readers used by the workflow monitor."""

import json
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any

from ..adapters import (
    REMOTE_JOB_LIST_COMMAND,
    REMOTE_JOB_LOG_COMMAND,
    REMOTE_JOB_SHOW_COMMAND,
    REMOTE_JOB_WHY_COMMAND,
)
from ..introspection import (
    STATE_KINDS,
    JobListPage,
    count_markers,
    describe_job,
    explain_job,
    job_frames,
    list_jobs,
    read_managers,
)
from ..models import Marker
from ..registry import LOCAL_REMOTE, WorkspaceBinding, resolve_workspace
from ..workspace import Workspace


def _first_mapping(value: object) -> dict[str, Any]:
    """Return the first mapping from a remote JSON object or array."""

    if isinstance(value, list):
        value = value[0] if value else {}
    return dict(value) if isinstance(value, Mapping) else {}


def remote_workspace_output(
    binding: WorkspaceBinding,
    context: Any,
    argv_tail: Sequence[str],
    *,
    timeout: float | None = None,
) -> tuple[int, str, str]:
    """Relay one remote read without importing the CLI package eagerly.

    :param binding: Remote-qualified workspace binding.
    :param context: CLI context used to resolve the adapter.
    :param argv_tail: Complete command vector for the remote read.
    :param timeout: Optional adapter timeout.
    :return: Remote return code, standard output, and standard error.
    """

    from ..workflow_cli import remote_workspace_output as relay

    return relay(binding, context, argv_tail, timeout=timeout)


class WorkspaceView:
    """Read one workspace while retaining only visible monitor state.

    :param workspace: A local :class:`Workspace`, binding, registered name, or
        remote-qualified name.
    :param context: CLI context needed for registry and remote adapter reads.
    :param refresh_interval: Seconds for count, manager, and detail cache expiry.
    :param detail_frames: Maximum number of history frames loaded by default detail.
    :param adapter_timeout: Optional timeout for one remote adapter read.
    """

    def __init__(
        self,
        workspace: Workspace | WorkspaceBinding | str,
        context: Any | None = None,
        *,
        refresh_interval: float = 5.0,
        detail_frames: int = 20,
        adapter_timeout: float | None = None,
    ) -> None:
        self.workspace: Workspace | None
        if isinstance(workspace, Workspace):
            self.binding = WorkspaceBinding(str(workspace.root), LOCAL_REMOTE, str(workspace.root))
            self.workspace = workspace
        else:
            self.binding = (
                resolve_workspace(workspace, project=getattr(context, "cwd", None))
                if isinstance(workspace, str)
                else workspace
            )
            self.workspace = (
                None
                if self.binding.remote != LOCAL_REMOTE or self.binding.path is None
                else Workspace(self.binding.path, mutable=False)
            )
        self.context = context
        self.refresh_interval = max(0.0, float(refresh_interval))
        self.detail_frames = max(1, int(detail_frames))
        self.adapter_timeout = adapter_timeout
        self._lock = threading.RLock()
        self._counts: dict[tuple[tuple[str, ...], str | None], tuple[float, dict[str, int]]] = {}
        self._managers: tuple[float, list[Mapping[str, Any]]] | None = None
        self._generation = 0
        self._details: dict[str, tuple[int, dict[str, Any]]] = {}
        self._whys: dict[str, tuple[int, dict[str, Any]]] = {}
        self._logs: dict[str, tuple[int, list[dict[str, Any]]]] = {}
        self._rows: dict[str, dict[str, Any]] = {}
        self._tail_offsets: dict[str, int] = {}

    @property
    def name(self) -> str:
        """Return the display name of this workspace."""

        return self.binding.name

    @property
    def remote(self) -> bool:
        """Return whether this view addresses a remote workspace."""

        return self.binding.remote != LOCAL_REMOTE

    def _remote_json(
        self,
        command: Sequence[str],
        *,
        flags: Sequence[str] = ("--json",),
        tail: Sequence[str] = (),
    ) -> object:
        """Run one JSON read through the non-printing remote relay."""

        if self.context is None:
            raise ValueError("a CLI context is required to read a remote workspace")
        argv = [*command, *flags, *tail, self.binding.name.split(":", 1)[1]]
        status, stdout, stderr = remote_workspace_output(
            self.binding,
            self.context,
            argv,
            timeout=self.adapter_timeout,
        )
        if status:
            detail = stderr.strip() or f"exit status {status}"
            raise RuntimeError(f"remote read failed: {detail}")
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise ValueError("remote read did not return JSON") from exc

    def _remember_counts(self, selected: tuple[str, ...], prefix: str | None, result: dict[str, int]) -> None:
        """Cache one count result under its active filter key."""

        with self._lock:
            self._counts[(selected, prefix)] = (time.monotonic(), result)

    def counts(
        self,
        kinds: Sequence[str] | None = None,
        placement_prefix: str | None = None,
    ) -> dict[str, int]:
        """Return counts for the active kind/prefix filter within the TTL.

        :param kinds: State kinds shown by the current page, or all kinds.
        :param placement_prefix: Active placement subtree, if any.
        :return: Counts keyed by selected state kind.
        """

        selected = tuple(kinds or STATE_KINDS)
        prefix = placement_prefix
        key = (selected, prefix)
        now = time.monotonic()
        with self._lock:
            cached = self._counts.get(key)
            if cached is not None and now - cached[0] < self.refresh_interval:
                return dict(cached[1])
        if self.remote:
            document = self._remote_json(
                REMOTE_JOB_LIST_COMMAND,
                flags=("--json", "--counts"),
                tail=self._list_tail(selected, prefix, None, None, 1),
            )
            raw = _first_mapping(document).get("counts", {})
            result = {kind: int(raw.get(kind, 0)) for kind in selected} if isinstance(raw, Mapping) else {}
        else:
            assert self.workspace is not None
            result = {kind: count_markers(self.workspace, kind, prefix) for kind in selected}
        self._remember_counts(selected, prefix, result)
        return dict(result)

    @staticmethod
    def _list_tail(
        kinds: Sequence[str],
        placement_prefix: str | None,
        tag_contains: str | None,
        cursor: str | None,
        limit: int,
    ) -> list[str]:
        """Compose remote job-list options before ``--workspace``."""

        tail: list[str] = ["--limit", str(limit)]
        for kind in kinds:
            tail.extend(("--kind", kind))
        if placement_prefix:
            tail.extend(("--placement", placement_prefix))
        if tag_contains:
            tail.extend(("--tag-contains", tag_contains))
        if cursor:
            tail.extend(("--after", cursor))
        tail.append("--workspace")
        return tail

    def page(
        self,
        kind_filter: Sequence[str] | None = None,
        placement_prefix: str | None = None,
        tag_contains: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> JobListPage:
        """Return one visible page without mutating the accepted-row cache."""

        if limit < 1:
            raise ValueError("page limit must be positive")
        kinds = tuple(kind_filter or STATE_KINDS)
        if self.remote:
            document = _first_mapping(
                self._remote_json(
                    REMOTE_JOB_LIST_COMMAND,
                    flags=("--json", "--counts"),
                    tail=self._list_tail(kinds, placement_prefix, tag_contains, cursor, limit),
                )
            )
            rows = document.get("jobs", [])
            raw_counts = document.get("counts", {})
            page = JobListPage(
                [dict(row) for row in rows] if isinstance(rows, list) else [],
                document.get("next_after") if isinstance(document.get("next_after"), str) else None,
                {kind: int(raw_counts.get(kind, 0)) for kind in kinds} if isinstance(raw_counts, Mapping) else None,
            )
        else:
            assert self.workspace is not None
            page = list_jobs(
                self.workspace,
                kinds=kinds,
                placement_prefix=placement_prefix,
                after=cursor,
                limit=limit,
                tag_contains=tag_contains,
            )
        return page

    def accept_page(self, page: JobListPage) -> None:
        """Commit a page after the caller accepts its generation."""

        with self._lock:
            self._rows = {
                str(row["job_id"]): dict(row)
                for row in page.jobs
                if isinstance(row, Mapping) and isinstance(row.get("job_id"), str)
            }

    def marker_for(self, job_id: str) -> Marker:
        """Resolve a selected row using its known placement."""

        if self.workspace is None:
            raise ValueError("remote jobs have no local marker")
        row = self._rows.get(job_id)
        marker = None
        if row is not None:
            marker = self.workspace.find_marker_at(str(row["job_key"]), PurePosixPath(str(row["placement"])))
        if marker is None:
            marker = self.workspace.find_marker_by_id(job_id)
        if marker is None:
            raise ValueError(f"job {job_id} is no longer present")
        return marker

    def _read_stdio(self, marker: Marker, *, follow: bool = False, size: int = 8192) -> str:
        """Read a bounded tail of a local job's stdio log."""

        assert self.workspace is not None
        path = self.workspace.payload_path(marker.placement, marker.job_key) / "logs" / "stdio.out"
        try:
            with path.open("rb") as handle:
                if follow:
                    offset = self._tail_offsets.get(marker.job_id, 0)
                    length = path.stat().st_size
                    if offset > length:
                        offset = 0
                    handle.seek(offset)
                else:
                    handle.seek(0, 2)
                    handle.seek(max(0, handle.tell() - size))
                data = handle.read(min(size, 256 * 1024))
                self._tail_offsets[marker.job_id] = handle.tell()
        except OSError:
            return ""
        return data.decode("utf-8", "replace")

    def detail(self, job_id: str, *, generation: int | None = None) -> dict[str, Any]:
        """Load and cache the bounded default detail, excluding diagnosis/log."""

        with self._lock:
            accepted_generation = self._generation if generation is None else generation
            cached = self._details.get(job_id)
            if cached is not None and cached[0] == accepted_generation:
                return dict(cached[1])
        if self.remote:
            result = _first_mapping(
                self._remote_json(REMOTE_JOB_SHOW_COMMAND, tail=(job_id, "--no-children", "--workspace"))
            )
            result.setdefault("frames", [])
            result.setdefault("stdio_tail", "")
        else:
            marker = self.marker_for(job_id)
            assert self.workspace is not None
            report = describe_job(self.workspace, marker, include_children=False)
            result = {
                **report,
                "frames": job_frames(self.workspace, marker, limit=self.detail_frames),
                "stdio_tail": self._read_stdio(marker),
            }
        with self._lock:
            if generation is None or generation == self._generation:
                self._details[job_id] = (self._generation, result)
        return dict(result)

    def why(self, job_id: str, *, generation: int | None = None) -> dict[str, Any]:
        """Load and cache the full diagnosis for one selected job."""

        with self._lock:
            accepted_generation = self._generation if generation is None else generation
            cached = self._whys.get(job_id)
            if cached is not None and cached[0] == accepted_generation:
                return dict(cached[1])
        if self.remote:
            result = _first_mapping(self._remote_json(REMOTE_JOB_WHY_COMMAND, tail=(job_id, "--workspace")))
        else:
            marker = self.marker_for(job_id)
            assert self.workspace is not None
            diagnosis = explain_job(self.workspace, marker)
            if hasattr(diagnosis, "as_mapping"):
                result = diagnosis.as_mapping()
            elif isinstance(diagnosis, Mapping):
                result = dict(diagnosis)
            else:
                raise TypeError("job diagnosis is not a mapping")
        with self._lock:
            if generation is None or generation == self._generation:
                self._whys[job_id] = (self._generation, result)
        return dict(result)

    def log(self, job_id: str, *, generation: int | None = None) -> list[dict[str, Any]]:
        """Load and cache the full transition history for one selected job."""

        with self._lock:
            accepted_generation = self._generation if generation is None else generation
            cached = self._logs.get(job_id)
            if cached is not None and cached[0] == accepted_generation:
                return list(cached[1])
        if self.remote:
            document = _first_mapping(
                self._remote_json(
                    REMOTE_JOB_LOG_COMMAND,
                    tail=(job_id, "--workspace"),
                )
            )
            frames = document.get("frames", [])
            result = [dict(item) for item in frames if isinstance(item, Mapping)] if isinstance(frames, list) else []
        else:
            marker = self.marker_for(job_id)
            assert self.workspace is not None
            result = job_frames(self.workspace, marker, limit=None)
        with self._lock:
            if generation is None or generation == self._generation:
                self._logs[job_id] = (self._generation, result)
        return list(result)

    def tail(self, job_id: str) -> str:
        """Read at most 256 KiB of the next local stdio chunk."""

        if self.remote:
            return "remote stdio tail unavailable (remote payload is not exposed)"
        return self._read_stdio(self.marker_for(job_id), follow=True, size=256 * 1024)

    def managers(self) -> list[Mapping[str, Any]]:
        """Return manager records, cached until the refresh interval expires."""

        if self.remote:
            return []
        now = time.monotonic()
        with self._lock:
            if self._managers is not None and now - self._managers[0] < self.refresh_interval:
                return list(self._managers[1])
        assert self.workspace is not None
        result: list[Mapping[str, Any]] = [manager.as_mapping() for manager in read_managers(self.workspace)]
        with self._lock:
            self._managers = (time.monotonic(), result)
        return list(result)

    def refresh(self) -> None:
        """Invalidate all cached reads and remembered visible rows."""

        with self._lock:
            self._generation += 1
            self._counts.clear()
            self._managers = None
            self._details.clear()
            self._whys.clear()
            self._logs.clear()
            self._rows.clear()
            self._tail_offsets.clear()


__all__ = ["WorkspaceView"]
