"""Asynchronous-friendly monitor actions backed by workflow CLI helpers."""

import os
import shutil
from argparse import Namespace
from collections.abc import Mapping, Sequence
from pathlib import Path

from ..configuration import resolve_operator_identity
from ..models import TERMINAL_KINDS
from ..workflow_cli import (
    ensure_identity_key,
    launch_workspace_managers,
    publish_job_requests,
    request_remote_job_result,
    run_transfer_verb_result,
    submit_remote_manager_result,
)
from ..workspace import Workspace
from .data import WorkspaceView


def _request_arguments(view: WorkspaceView, action: str, job_ids: Sequence[str], reason: str) -> Namespace:
    """Build the common argument object consumed by the existing request code."""

    return Namespace(
        action=action,
        job_id=list(job_ids),
        reason=reason,
        operator=None,
        priority=None,
        step=None,
        force=False,
        wait=False,
        timeout=None,
        adapter_timeout=view.adapter_timeout,
        durable=False,
        no_durable=False,
        workspace=view.name,
        by_path=False,
    )


def _mutable_workspace(view: WorkspaceView) -> Workspace:
    """Open a mutable local handle for an action without changing the reader."""

    if view.remote or view.binding.path is None:
        raise ValueError("this action requires a local workspace")
    return Workspace(view.binding.path)


def request(view: WorkspaceView, action: str, job_ids: Sequence[str], reason: str) -> str:
    """Publish a cancel, pause, or continue request using CLI request logic.

    :param view: Workspace receiving the request.
    :param action: Existing job request action.
    :param job_ids: Canonical ids or selectors of selected jobs.
    :param reason: Operator explanation recorded with each request.
    :return: A short result message.
    """

    if action not in {"cancel", "pause", "continue"}:
        raise ValueError(f"monitor does not support request action {action!r}")
    if not job_ids:
        raise ValueError("select at least one job")
    arguments = _request_arguments(view, action, job_ids, reason)
    context = view.context
    if context is None:
        raise ValueError("a CLI context is required for monitor actions")
    if view.remote:
        identity = resolve_operator_identity(None)
        ensure_identity_key(identity)
        status, _stdout, _stderr = request_remote_job_result(view.binding, context, arguments, identity)
        if status:
            raise RuntimeError(f"job request failed with exit status {status}")
        return f"requested {action} for {len(job_ids)} job(s)"

    workspace = _mutable_workspace(view)
    markers = [view.marker_for(job_id) for job_id in job_ids]
    published = publish_job_requests(workspace, markers, action=action, reason=reason)
    return f"requested {action} for {len(published)} job(s)"


def start_managers(view: WorkspaceView, count: int, launcher: str | None = None) -> str:
    """Start managers through the same manager command dispatcher as the CLI."""

    if count < 1:
        raise ValueError("manager count must be positive")
    if view.context is None:
        raise ValueError("a CLI context is required for monitor actions")
    arguments = Namespace(
        workspace=view.name,
        count=count,
        launcher=launcher,
        detach=True,
        inline=False,
        by_path=False,
        pool=[],
        capability=[],
        placement_prefix=[],
        workers=None,
        worker_resource=[],
        lease_seconds=None,
        heartbeat_interval=30.0,
        poll_interval=1.0,
        idle=False,
        idle_timeout=3600.0,
        drain_timeout=30.0,
        join_grace_seconds=3600.0,
        unsafe_persistent_takeover=False,
        unsafe_isolated_takeover=False,
        runner_search_path=[],
        gc_interval=None,
        log_level=None,
        log_file=None,
        json_logs=False,
        adapter_timeout=view.adapter_timeout,
        durable=False,
        no_durable=False,
    )
    if view.remote:
        status, _stdout, _stderr = submit_remote_manager_result(view.binding, arguments, view.context)
    else:
        assert view.binding.path is not None
        _mode, result = launch_workspace_managers(Path(view.binding.path), arguments, view.context)
        if isinstance(result, int):
            status = int(result)
        elif isinstance(result, Mapping):
            status = 0 if result.get("ok", True) else 1
        else:
            raise RuntimeError("manager start returned an invalid result")
    if status:
        raise RuntimeError(f"manager start failed with exit status {status}")
    return f"started {count} manager(s)"


def transfer(view: WorkspaceView, job_ids: Sequence[str], destination: str) -> str:
    """Transfer selected jobs through the existing transfer verb."""

    if not job_ids:
        raise ValueError("select at least one job")
    if not destination:
        raise ValueError("destination workspace cannot be empty")
    if view.context is None:
        raise ValueError("a CLI context is required for monitor actions")
    arguments = Namespace(
        source=view.name,
        destination=destination,
        jobs=list(job_ids),
        state=None,
        placement=None,
        destination_placement=None,
        adapter_timeout=view.adapter_timeout,
        strict_environment=False,
        json=True,
    )
    markers = None
    if not view.remote:
        markers = [view.marker_for(job_id) for job_id in job_ids]
    run_transfer_verb_result(arguments, view.context, True, markers)
    return f"transferred {len(job_ids)} job(s) to {destination}"


def remove(view: WorkspaceView, job_ids: Sequence[str]) -> str:
    """Remove selected terminal payloads and their markers.

    This explicit operator action intentionally skips the parent-join guard
    applied by garbage collection; callers should remove children only after
    their non-terminal parents are terminal.
    """

    if view.remote:
        raise ValueError("removing remote payloads is not supported by the monitor")
    if not job_ids:
        raise ValueError("select at least one job")
    workspace = _mutable_workspace(view)
    markers = [(job_id, view.marker_for(job_id)) for job_id in job_ids]
    non_terminal = [(job_id, marker.kind) for job_id, marker in markers if marker.kind not in TERMINAL_KINDS]
    if non_terminal:
        job_id, kind = non_terminal[0]
        raise ValueError(f"job {job_id} is {kind}, not terminal; no jobs were removed")
    removed = 0
    for _job_id, marker in markers:
        if marker.kind not in TERMINAL_KINDS:
            raise ValueError(f"job {_job_id} is {marker.kind}, not terminal")
        payload = workspace.payload_path(marker.placement, marker.job_key)
        if payload.exists():
            shutil.rmtree(payload)
            removed += 1
        os.unlink(marker.path)
    return f"removed {removed} job payload(s)"


class Actions:
    """Small object facade convenient for the curses application and tests."""

    def __init__(self, view: WorkspaceView) -> None:
        self.view = view

    def request(self, action: str, job_ids: Sequence[str], reason: str) -> str:
        """Publish a selected job request."""

        return request(self.view, action, job_ids, reason)

    def start_managers(self, count: int, launcher: str | None = None) -> str:
        """Start managers for this view's workspace."""

        return start_managers(self.view, count, launcher)

    def transfer(self, job_ids: Sequence[str], destination: str) -> str:
        """Transfer selected jobs."""

        return transfer(self.view, job_ids, destination)

    def remove(self, job_ids: Sequence[str]) -> str:
        """Remove selected terminal jobs."""

        return remove(self.view, job_ids)


__all__ = ["Actions", "remove", "request", "start_managers", "transfer"]
