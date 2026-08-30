"""Monitor data, rendering, action delegation, and terminal guards."""

import os
from argparse import Namespace
from concurrent.futures import Future
from contextlib import nullcontext
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, Self

import pytest
from httk.core.cli import CLIContext

from conftest import fake_remote, register_ws
from httk.workflow import Workspace, transfers
from httk.workflow.introspection import JobListPage, _reading
from httk.workflow.models import STATE_KINDS, Marker
from httk.workflow.monitor import actions as monitor_actions
from httk.workflow.monitor import data as monitor_data
from httk.workflow.monitor.data import WorkspaceView
from httk.workflow.monitor.ui import (
    ActionOutcome,
    MonitorApp,
    MonitorState,
    handle_key,
    render_detail_pane,
    render_job_pane,
    render_workspace_pane,
)
from httk.workflow.projects import initialize_project
from httk.workflow.registry import WorkspaceBinding
from httk.workflow.workflow_cli import _monitor as monitor_cli
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command
from test_job_paging import _marker


def test_monitor_view_uses_marker_counts_and_one_visible_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A large workspace is counted and paged without state reads beyond the page."""

    workspace = Workspace.initialize(tmp_path / "large")
    for index in range(100_000):
        _marker(workspace, STATE_KINDS[index % 3], f"p{index % 50:02d}")
    reads: list[str] = []
    scanned_entries = 0
    real_scandir = os.scandir

    class CountedScan:
        def __init__(self, iterator: Any) -> None:
            self.iterator = iterator

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            close = getattr(self.iterator, "close", None)
            if close is not None:
                close()

        def __iter__(self) -> "CountedScan":
            return self

        def __next__(self) -> object:
            nonlocal scanned_entries
            scanned_entries += 1
            return next(self.iterator)

    def counted_scandir(path: object) -> CountedScan:
        return CountedScan(real_scandir(path))

    monkeypatch.setattr(os, "scandir", counted_scandir)

    def fake_state(_workspace: Workspace, marker: object) -> tuple[dict[str, object], None]:
        reads.append(str(marker))
        return {}, None

    monkeypatch.setattr(_reading, "_state_of", fake_state)
    view = WorkspaceView(workspace, refresh_interval=60)
    counts = view.counts()
    page = view.page(limit=25)

    assert sum(counts.values()) == 100_000
    assert len(page.jobs) == 25
    assert len(reads) == 25
    assert scanned_entries <= 103_000


def test_monitor_detail_is_lazy_and_cached(tmp_path: Path) -> None:
    """Selecting no job performs no detail work, and one selection is cached."""

    workspace = Workspace.initialize(tmp_path / "detail")
    _marker(workspace, "ready", "jobs")
    view = WorkspaceView(workspace, refresh_interval=60)
    page = view.page(limit=1)
    view.accept_page(page)
    job_id = str(page.jobs[0]["job_id"])

    assert view._details == {}
    first = view.detail(job_id)
    second = view.detail(job_id)
    assert first == second
    assert first["job_id"] == job_id


def test_monitor_detail_does_not_explain_until_requested(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The default detail reads one report, twenty frames, and no diagnosis."""

    workspace = Workspace.initialize(tmp_path / "detail-bounded")
    _marker(workspace, "ready", "jobs")
    view = WorkspaceView(workspace, refresh_interval=60)
    job_id = str(view.page(limit=1).jobs[0]["job_id"])
    calls: list[str] = []
    describe_modes: list[bool] = []

    def fake_describe(*_args: object, **kwargs: object) -> dict[str, object]:
        describe_modes.append(bool(kwargs["include_children"]))
        return {"job_id": job_id}

    monkeypatch.setattr(monitor_data, "describe_job", fake_describe)

    def fake_frames(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
        calls.append("frames")
        return []

    monkeypatch.setattr(monitor_data, "job_frames", fake_frames)

    def unexpected_why(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("why must be explicit")

    monkeypatch.setattr(monitor_data, "explain_job", unexpected_why)
    view.detail(job_id)
    view.detail(job_id)
    assert calls == ["frames"]
    assert describe_modes == [False]

    class Diagnosis:
        def as_mapping(self) -> dict[str, object]:
            calls.append("why")
            return {}

    monkeypatch.setattr(monitor_data, "explain_job", lambda *_: Diagnosis())
    view.why(job_id)
    view.why(job_id)
    assert calls == ["frames", "why"]


def test_monitor_remote_page_uses_json_relay(tmp_path: Path, remote: object) -> None:
    """A remote view parses the existing JSON job-list protocol."""

    project = tmp_path / "project"
    initialize_project(project, name="monitor-remote")
    fake_remote(project)
    root = remote.root / "runs" / "workspace"  # type: ignore[attr-defined]
    workspace = Workspace.initialize(root)
    _marker(workspace, "ready", "jobs")
    context = CLIContext("httk", project)
    register_ws(context, root, "station")
    view = WorkspaceView("cluster:station", context)

    page = view.page(limit=5)
    assert len(page.jobs) == 1
    assert page.jobs[0]["state"] == "ready"


def test_monitor_remote_page_forwards_counts_and_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    """A remote page carries counts and every active list filter together."""

    binding = WorkspaceBinding("cluster:station", "cluster", None)
    context = CLIContext("httk", Path.cwd())
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_output(*args: object, **kwargs: object) -> tuple[int, str, str]:
        calls.append((args, dict(kwargs)))
        return 0, '{"jobs": [], "next_after": null, "counts": {"ready": 3}}', ""

    monkeypatch.setattr(monitor_data, "remote_workspace_output", fake_output)
    view = WorkspaceView(binding, context)
    view.page(("ready",), "jobs/sub", "needle", "after-cursor", 7)

    assert len(calls) == 1
    args = calls[0][0]
    assert args[2] == [
        "httk",
        "job",
        "list",
        "--json",
        "--counts",
        "--limit",
        "7",
        "--kind",
        "ready",
        "--placement",
        "jobs/sub",
        "--tag-contains",
        "needle",
        "--after",
        "after-cursor",
        "--workspace",
        "station",
    ]


def test_monitor_remote_default_detail_forwards_no_children(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remote default detail asks the owning CLI for its compact report."""

    binding = WorkspaceBinding("cluster:station", "cluster", None)
    context = CLIContext("httk", Path.cwd())
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_output(*args: object, **kwargs: object) -> tuple[int, str, str]:
        calls.append((args, dict(kwargs)))
        return 0, '[{"job_id": "job"}]', ""

    monkeypatch.setattr(monitor_data, "remote_workspace_output", fake_output)
    WorkspaceView(binding, context).detail("job")

    assert calls[0][0][2] == [
        "httk",
        "job",
        "show",
        "--json",
        "job",
        "--no-children",
        "--workspace",
        "station",
    ]


def test_monitor_page_does_not_select_the_counts_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An old page request cannot change the explicit count key of a new one."""

    workspace = Workspace.initialize(tmp_path / "overlapping-pages")
    _marker(workspace, "ready", "accepted")
    _marker(workspace, "failed", "stale")
    view = WorkspaceView(workspace, refresh_interval=60)
    view.page(("ready",), "accepted", limit=1)
    view.page(("failed",), "stale", limit=1)
    calls: list[tuple[str, str | None]] = []

    def fake_count(_workspace: Workspace, kind: str, prefix: str | None) -> int:
        calls.append((kind, prefix))
        return 1

    monkeypatch.setattr(monitor_data, "count_markers", fake_count)
    assert view.counts(("ready",), "accepted") == {"ready": 1}
    assert calls == [("ready", "accepted")]


def test_monitor_actions_delegate_to_cli_implementations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Request, manager, and transfer actions use the existing command code."""

    workspace = Workspace.initialize(tmp_path / "actions")
    context = CLIContext("httk", tmp_path)
    view = WorkspaceView(workspace, context)
    calls: list[str] = []

    class MutableWorkspace:
        def publish_request(self, _document: object) -> None:
            calls.append("publish")

    monkeypatch.setattr(monitor_actions, "_mutable_workspace", lambda _view: MutableWorkspace())
    monkeypatch.setattr(monitor_actions, "ensure_identity_key", lambda _identity: None)
    monkeypatch.setattr(monitor_actions, "resolve_operator_identity", lambda _operator: object())
    monkeypatch.setattr(view, "marker_for", lambda _job_id: object())

    def fake_publish(*_args: object, **_kwargs: object) -> list[tuple[str, Any, Any]]:
        calls.append("publish")
        return [("id", object(), object())]

    monkeypatch.setattr(monitor_actions, "publish_job_requests", fake_publish)

    def fake_manager(*_args: object) -> tuple[str, int]:
        calls.append("manager")
        return "process", 0

    transfer_markers: list[Any] = []

    def fake_transfer(*args: object) -> int:
        calls.append("transfer")
        transfer_markers.append(args[3])
        return 0

    monkeypatch.setattr(monitor_actions, "launch_workspace_managers", fake_manager)
    monkeypatch.setattr(monitor_actions, "run_transfer_verb_result", fake_transfer)

    assert monitor_actions.request(view, "cancel", ["id"], "because") == "requested cancel for 1 job(s)"
    assert monitor_actions.start_managers(view, 1) == "started 1 manager(s)"
    assert monitor_actions.transfer(view, ["id"], "destination") == "transferred 1 job(s) to destination"
    assert calls == ["publish", "manager", "transfer"]
    assert len(transfer_markers) == 1 and len(transfer_markers[0]) == 1


def test_monitor_actions_forward_adapter_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """All monitor action argument objects retain the configured adapter timeout."""

    workspace = Workspace.initialize(tmp_path / "action-timeout")
    context = CLIContext("httk", tmp_path)
    view = WorkspaceView(workspace, context, adapter_timeout=12.5)
    monkeypatch.setattr(view, "marker_for", lambda _job_id: object())
    seen: dict[str, float | None] = {}

    remote_view = WorkspaceView(
        WorkspaceBinding("cluster:station", "cluster", None),
        context,
        adapter_timeout=12.5,
    )
    monkeypatch.setattr(monitor_actions, "resolve_operator_identity", lambda _operator: SimpleNamespace())
    monkeypatch.setattr(monitor_actions, "ensure_identity_key", lambda _identity: None)

    def fake_request(_binding: object, _context: object, arguments: Any, _identity: object) -> tuple[int, str, str]:
        seen["request"] = arguments.adapter_timeout
        return 0, "", ""

    monkeypatch.setattr(monitor_actions, "request_remote_job_result", fake_request)
    monitor_actions.request(remote_view, "cancel", ["id"], "because")

    def fake_manager(_root: Path, arguments: Any, _context: object) -> tuple[str, int]:
        seen["manager"] = arguments.adapter_timeout
        return "process", 0

    monkeypatch.setattr(monitor_actions, "launch_workspace_managers", fake_manager)
    monitor_actions.start_managers(view, 1)

    def fake_transfer(arguments: Any, _context: object, *_args: object) -> dict[str, object]:
        seen["transfer"] = arguments.adapter_timeout
        return {}

    monkeypatch.setattr(monitor_actions, "run_transfer_verb_result", fake_transfer)
    monitor_actions.transfer(view, ["id"], "destination")
    assert seen == {"request": 12.5, "manager": 12.5, "transfer": 12.5}


def test_known_marker_detach_skips_full_marker_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Known-marker detaches inspect only waiting parents for join references."""

    workspace = Workspace.initialize(tmp_path / "known-marker")
    job_id = "00000000-0000-0000-0000-000000000001"
    marker = Marker(
        "succeeded",
        PurePosixPath("jobs"),
        job_id,
        500,
        0,
        "init",
        tmp_path / "marker",
    )
    scanned: list[tuple[str, ...]] = []
    monkeypatch.setattr(transfers, "_all_markers", lambda _workspace: pytest.fail("full marker scan"))

    def scan_waiting(kinds: tuple[str, ...]) -> list[Marker]:
        scanned.append(kinds)
        return []

    monkeypatch.setattr(
        workspace,
        "scan_markers",
        scan_waiting,
    )
    monkeypatch.setattr(workspace, "read_state", lambda _marker: {})
    monkeypatch.setattr(workspace, "open_journal_writer", nullcontext)
    monkeypatch.setattr(workspace, "transition", lambda *_args, **_kwargs: marker)
    monkeypatch.setattr(transfers, "_seal_transferring", lambda *_args: tmp_path / "bundle")

    assert (
        transfers.detach_job(
            workspace,
            job_id,
            marker=marker,
            destination_workspace_id="00000000-0000-0000-0000-000000000002",
        )
        == tmp_path / "bundle"
    )
    assert scanned == [("waiting",)]


def test_quiet_remote_relay_does_not_write_worker_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The returning remote-to-remote transfer helper honors quiet workers."""

    source = WorkspaceBinding("source:jobs", "source", None)
    destination = WorkspaceBinding("destination:jobs", "destination", None)
    context = CLIContext("httk", tmp_path)
    monkeypatch.setattr(
        transfer_cli,
        "resolve_workspace",
        lambda name, project: source if name == source.name else destination,
    )
    monkeypatch.setattr(transfer_cli, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(transfer_cli, "_remote_workspace_settings", lambda *_args, **_kwargs: None)
    seen: dict[str, bool] = {}

    def fake_relay(*_args: object, **kwargs: object) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        seen["quiet"] = bool(kwargs["quiet"])
        return [], []

    monkeypatch.setattr(transfer_cli, "_transfer_remote_to_remote", fake_relay)
    arguments = Namespace(
        source=source.name,
        destination=destination.name,
        jobs=[],
        state=None,
        placement=None,
        adapter_timeout=None,
        strict_environment=False,
    )
    assert transfer_cli.run_transfer_verb_result(arguments, context, quiet=True) == {"moved": [], "retired": []}
    assert seen == {"quiet": True}
    assert capsys.readouterr().out == ""


def test_monitor_renderers_and_key_state_machine(tmp_path: Path) -> None:
    """Pure rendering and key handling do not require curses screen calls."""

    view = WorkspaceView(Workspace.initialize(tmp_path / "render"))
    state = MonitorState(
        [view],
        rows=[
            {"job_id": "id", "job_key": "tag--id", "state": "ready", "step": "run", "placement": "jobs", "priority": 1}
        ],
    )
    state.counts = {"ready": 1}
    workspace_lines = render_workspace_pane(state)
    assert any("ready" in line and "1" in line for line in workspace_lines)
    assert "tag--id" in "\n".join(render_job_pane(state.rows))
    assert "DETAIL" in render_detail_pane(None)
    handle_key(state, "j")
    handle_key(state, "TAB")
    handle_key(state, "?")
    assert state.pane == 2
    assert "pages" in state.status


def test_monitor_page_navigation_ignores_second_key_while_loading(tmp_path: Path) -> None:
    """A pending page replacement owns the cursor until it is accepted."""

    state = MonitorState([WorkspaceView(Workspace.initialize(tmp_path / "navigation"))])
    state.next_after = "ready:jobs/job-1"
    handle_key(state, "n")
    assert state.page_cursor == "ready:jobs/job-1"
    assert state.next_after is None
    state.loading = True
    generation = state.generation
    handle_key(state, "n")
    assert state.generation == generation
    assert state.page_cursor == "ready:jobs/job-1"
    assert state.status == "page still loading"


def test_monitor_remove_confirmation_captures_only_highlighted_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D then confirmation dispatches the id selected when D was pressed."""

    view = WorkspaceView(Workspace.initialize(tmp_path / "remove-key"))
    state = MonitorState(
        [view],
        rows=[
            {"job_id": "first", "state": "succeeded"},
            {"job_id": "second", "state": "succeeded"},
        ],
    )
    app = MonitorApp(object(), [view], 5.0)
    app.state = state
    dispatched: list[tuple[object, tuple[object, ...]]] = []

    def dispatch(function: object, *args: object) -> None:
        dispatched.append((function, args))

    monkeypatch.setattr(app, "_dispatch_action", dispatch)
    handle_key(state, "D")
    app._pending_remove_ids = [state.selected_row["job_id"]]  # type: ignore[index]
    app._handle_confirmation("y")
    assert state.confirmation is None
    assert len(dispatched) == 1
    assert dispatched[0][1] == (["first"],)
    app.executor.shutdown(wait=False, cancel_futures=True)


def test_monitor_remove_decline_and_nonremovable_refusal_use_real_key_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Declining D dispatches nothing, and a live row is refused before confirmation."""

    view = WorkspaceView(Workspace.initialize(tmp_path / "remove-keys"))
    app = MonitorApp(object(), [view], 5.0)
    app_any: Any = app
    app_any._schedule_load = lambda **_kwargs: None
    app_any._collect = lambda: None
    app_any._load_detail = lambda: None
    app_any._draw = lambda: None
    dispatched: list[tuple[object, tuple[object, ...]]] = []
    monkeypatch.setattr(app, "_dispatch_action", lambda function, *args: dispatched.append((function, args)))

    keys = iter((ord("D"), ord("n"), ord("q")))
    screen = SimpleNamespace(
        keypad=lambda _value: None,
        timeout=lambda _value: None,
        getch=lambda: next(keys),
    )
    app.stdscr = screen
    app.state.rows = [{"job_id": "terminal", "state": "succeeded"}]
    assert app.run() == 0
    assert dispatched == []

    app = MonitorApp(object(), [view], 5.0)
    app_any = app
    app_any._schedule_load = lambda **_kwargs: None
    app_any._collect = lambda: None
    app_any._load_detail = lambda: None
    app_any._draw = lambda: None
    keys = iter((ord("D"), ord("q")))
    app.stdscr = SimpleNamespace(
        keypad=lambda _value: None,
        timeout=lambda _value: None,
        getch=lambda: next(keys),
    )
    app.state.rows = [{"job_id": "live", "state": "running"}]
    assert app.run() == 0
    assert app.state.confirmation is None
    assert "not removable" in app.state.status


def test_monitor_action_completion_invalidates_originating_view_after_switch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An action survives a workspace switch and reports its captured result."""

    first = WorkspaceView(Workspace.initialize(tmp_path / "first"))
    second = WorkspaceView(Workspace.initialize(tmp_path / "second"))
    app = MonitorApp(object(), [first, second], 5.0)
    refreshed: list[str] = []
    monkeypatch.setattr(first, "refresh", lambda: refreshed.append("first"))
    pending: Future[ActionOutcome] = Future()
    pending.set_result(ActionOutcome(0, first, result="removed"))
    app.action_future = pending
    app.state.active_workspace = 1
    app.state.generation = 1
    app.state.rows = [{"job_id": "second"}]
    app._collect()
    assert refreshed == ["first"]
    assert app.state.rows == [{"job_id": "second"}]
    assert "removed" in app.state.status
    app.executor.shutdown(wait=False, cancel_futures=True)


def test_monitor_failed_action_invalidates_active_view(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A partially completed action still invalidates and reloads its view."""

    view = WorkspaceView(Workspace.initialize(tmp_path / "failed-action"))
    app = MonitorApp(object(), [view], 5.0)
    refreshed: list[str] = []
    monkeypatch.setattr(view, "refresh", lambda: refreshed.append("view"))
    app.state.rows = [{"job_id": "possibly-removed"}]
    pending: Future[ActionOutcome] = Future()
    pending.set_result(ActionOutcome(0, view, error="second job was already absent"))
    app.action_future = pending

    app._collect()

    assert refreshed == ["view"]
    assert app.state.rows == []
    assert "second job was already absent" in app.state.status
    app.executor.shutdown(wait=False, cancel_futures=True)


def test_monitor_late_page_and_detail_completions_are_ignored(tmp_path: Path) -> None:
    """Read results from an old generation cannot repopulate the visible model."""

    view = WorkspaceView(Workspace.initialize(tmp_path / "late-reads"))
    app = MonitorApp(object(), [view], 5.0)
    app.state.generation = 1
    app.state.rows = [{"job_id": "current"}]
    view.accept_page(JobListPage([{"job_id": "current"}], None))
    page_future: Future[Any] = Future()
    page_future.set_result((0, view, {"ready": 1}, SimpleNamespace(jobs=[{"job_id": "old"}], next_after="old"), []))
    app.future = page_future
    app._collect()
    assert app.state.rows == [{"job_id": "current"}]
    assert set(view._rows) == {"current"}

    app.state.detail_job = "current"
    detail_future: Future[Any] = Future()
    detail_future.set_result((0, view, "old", {"job_id": "old"}))
    app.detail_future = detail_future
    app._collect()
    assert app.state.detail is None
    app.executor.shutdown(wait=False, cancel_futures=True)


def test_monitor_keeps_only_one_current_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Paging replaces the selection cache instead of retaining all pages."""

    workspace = Workspace.initialize(tmp_path / "pages")
    ids = [_marker(workspace, "ready", "jobs", tag=f"job{index:02d}") for index in range(100)]
    view = WorkspaceView(workspace, refresh_interval=60)
    cursor = None
    for _ in range(10):
        page = view.page(("ready",), cursor=cursor, limit=10)
        view.accept_page(page)
        cursor = page.next_after
    assert len(view._rows) == 10
    assert set(view._rows) == set(ids[-10:])


def test_monitor_stdio_tail_restarts_after_truncation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A follow read resets its offset when stdio is truncated."""

    workspace = Workspace.initialize(tmp_path / "tail")
    payload = workspace.root / "payload"
    (payload / "logs").mkdir(parents=True)
    (payload / "logs" / "stdio.out").write_text("new", encoding="utf-8")
    marker = SimpleNamespace(job_id="job", placement=SimpleNamespace(), job_key="job")
    monkeypatch.setattr(view := WorkspaceView(workspace), "marker_for", lambda _job_id: marker)
    monkeypatch.setattr(workspace, "payload_path", lambda *_args: payload)
    view._tail_offsets["job"] = 99
    assert view.tail("job") == "new"
    assert view._tail_offsets["job"] == 3


def test_monitor_remove_mixed_batch_preflights_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mixed terminal/non-terminal removal leaves every target untouched."""

    view = WorkspaceView(Workspace.initialize(tmp_path / "remove-mixed"))
    terminal = SimpleNamespace(kind="succeeded", placement=SimpleNamespace(), job_key="terminal", path=Path("t"))
    live = SimpleNamespace(kind="running", placement=SimpleNamespace(), job_key="live", path=Path("l"))
    monkeypatch.setattr(view, "marker_for", lambda job_id: terminal if job_id == "terminal" else live)
    monkeypatch.setattr(monitor_actions, "_mutable_workspace", lambda _view: SimpleNamespace())
    with pytest.raises(ValueError, match=r"removed 0 of 2 job\(s\)"):
        monitor_actions.remove(view, ["terminal", "live"])


def test_monitor_requires_a_tty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The command gives a clear error before importing or invoking curses."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    register_ws(context, workspace.root, "monitor")
    monkeypatch.setattr(monitor_cli.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(monitor_cli.sys.stdout, "isatty", lambda: False)

    assert command(["monitor", "--workspace", "monitor"], context) == 2
