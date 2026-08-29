"""Pure monitor model/renderers and the small curses adapter."""

import curses
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from ..models import STATE_KINDS
from .actions import Actions
from .data import WorkspaceView


@dataclass(frozen=True)
class ActionOutcome:
    """Structured result of one background action.

    :param generation: UI generation captured when the action was requested.
    :param view: Workspace view on which the action operated.
    :param result: Human-readable success message, if successful.
    :param error: Human-readable failure, if the action raised.
    """

    generation: int
    view: WorkspaceView
    result: str | None = None
    error: str | None = None


@dataclass
class MonitorState:
    """Mutable, terminal-independent state of one monitor session."""

    views: list[WorkspaceView]
    active_workspace: int = 0
    pane: int = 1
    rows: list[dict[str, Any]] = field(default_factory=list)
    selected: int = 0
    page_cursor: str | None = None
    page_history: list[str | None] = field(default_factory=lambda: [None])
    page_number: int = 1
    next_after: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    managers: list[dict[str, Any]] = field(default_factory=list)
    detail: dict[str, Any] | None = None
    detail_job: str | None = None
    follow: bool = False
    kind_filter: tuple[str, ...] = ()
    placement_prefix: str | None = None
    tag_contains: str | None = None
    status: str = ""
    generation: int = 0
    confirmation: str | None = None
    loading: bool = False

    @property
    def view(self) -> WorkspaceView:
        """Return the currently selected workspace view."""

        return self.views[self.active_workspace]

    @property
    def selected_row(self) -> dict[str, Any] | None:
        """Return the currently highlighted row."""

        return self.rows[self.selected] if self.rows and 0 <= self.selected < len(self.rows) else None


def _clip(value: object, width: int) -> str:
    """Make one display cell fit its column."""

    text = "-" if value is None else str(value)
    return text if len(text) <= width else text[: max(0, width - 1)] + "…"


def render_workspace_pane(state: MonitorState, *, width: int = 28, height: int | None = None) -> list[str]:
    """Render workspace names, counts, and live-manager summaries."""

    lines = ["WORKSPACES"]
    for index, view in enumerate(state.views):
        marker = ">" if index == state.active_workspace else " "
        lines.append(f"{marker} {_clip(view.name, width - 3)}")
        counts = state.counts if index == state.active_workspace else {}
        if counts:
            for kind in STATE_KINDS:
                if kind in counts:
                    lines.append(f"    {kind:<12} {counts[kind]}")
        if index == state.active_workspace and view.remote:
            lines.append("    managers     unavailable (remote)")
        elif index == state.active_workspace:
            live = sum(1 for manager in state.managers if manager.get("alive"))
            lines.append(f"    managers     {live} live")
    return lines if height is None else lines[:height]


def render_job_pane(
    rows: list[dict[str, Any]],
    *,
    selected: int = 0,
    width: int = 70,
    height: int | None = None,
) -> list[str]:
    """Render the visible job page without performing any I/O."""

    lines = ["JOBS", "  key                         state       step        placement       priority"]
    for index, row in enumerate(rows):
        prefix = ">" if index == selected else " "
        lines.append(
            f"{prefix} {_clip(row.get('job_key'), 27):<27} "
            f"{_clip(row.get('state'), 10):<10} "
            f"{_clip(row.get('step'), 10):<10} "
            f"{_clip(row.get('placement'), 15):<15} "
            f"{_clip(row.get('priority'), 8):>8}"
        )
    if len(lines) == 2:
        lines.append("  (no jobs on this page)")
    return [line[:width] for line in lines] if height is None else [line[:width] for line in lines[:height]]


def render_detail_pane(detail: dict[str, Any] | None, *, width: int = 60, height: int | None = None) -> list[str]:
    """Render a selected job detail report and its bounded tail."""

    if not detail:
        lines = ["DETAIL", "  press Enter to inspect the selected job"]
    else:
        lines = ["DETAIL"]
        for key in ("job_id", "job_key", "state", "step", "placement", "reason"):
            if key in detail:
                lines.append(f"  {key}: {detail[key]}")
        why = detail.get("why")
        if isinstance(why, dict):
            summary = why.get("summary") or why.get("message")
            if summary:
                lines.append(f"  why: {summary}")
        lines.append("  frames:")
        frames = detail.get("frames", [])
        lines.extend(f"    {item}" for item in frames[-5:] if item)
        lines.append("  stdio tail:")
        lines.extend(f"    {line}" for line in str(detail.get("stdio_tail", "")).splitlines()[-12:])
    result = [line[:width] for line in lines]
    return result if height is None else result[:height]


def render_status(state: MonitorState, *, width: int = 120) -> str:
    """Render the one-line status and key hint area."""

    hint = "j/k move  n/p page  Tab pane  Enter detail  ? help  q quit"
    text = state.status or hint
    return text[:width]


def handle_key(state: MonitorState, key: str) -> MonitorState:
    """Apply one normalized key to the pure monitor state machine."""

    if state.confirmation is not None:
        if key.lower() in {"y", "yes"}:
            state.status = f"confirmed {state.confirmation}"
            state.confirmation = None
        elif key.lower() in {"n", "no", "escape"}:
            state.status = "cancelled"
            state.confirmation = None
        return state
    old_selected = state.selected
    changed = False
    if key in {"j", "KEY_DOWN"}:
        if state.pane == 0:
            state.active_workspace = min(state.active_workspace + 1, max(0, len(state.views) - 1))
            changed = True
        else:
            state.selected = min(state.selected + 1, max(0, len(state.rows) - 1))
    elif key in {"k", "KEY_UP"}:
        if state.pane == 0:
            state.active_workspace = max(0, state.active_workspace - 1)
            changed = True
        else:
            state.selected = max(0, state.selected - 1)
    elif key == "TAB":
        state.pane = (state.pane + 1) % 3
    elif key == "n":
        if state.loading:
            state.status = "page still loading"
        elif state.next_after is not None:
            state.page_number += 1
            state.page_cursor = state.next_after
            state.next_after = None
            state.page_history.append(state.page_cursor)
            state.selected = 0
            state.status = "loading next page"
            changed = True
    elif key == "p":
        if state.loading:
            state.status = "page still loading"
        elif len(state.page_history) > 1:
            state.page_history.pop()
            state.page_number -= 1
            state.page_cursor = state.page_history[-1]
            state.next_after = None
            state.selected = 0
            state.status = "loading previous page"
            changed = True
    elif key == "r":
        state.status = "refreshing"
        changed = True
    elif key == "t":
        state.follow = not state.follow
        state.status = "stdio follow " + ("on" if state.follow else "off")
    elif key == "ENTER":
        row = state.selected_row
        if row is not None:
            state.detail_job = str(row["job_id"])
            state.detail = None
            state.status = "loading detail"
    elif key == "f":
        state.status = "filter prompt: kind, placement prefix, tag substring"
    elif key == "m":
        state.status = "manager prompt: count and launcher"
    elif key == "x":
        state.status = "transfer prompt: destination workspace"
    elif key == "D":
        state.confirmation = "remove"
        state.status = "remove prompt: confirm terminal job removal"
    elif key == "KEY_RESIZE":
        state.status = "resized"
    elif key == "?":
        state.status = "j/k arrows move | n/p pages | Tab panes | f filters | Enter detail | t tail | c/P/C requests | m managers | x transfer | D remove | r refresh | q quit"
    if state.selected != old_selected:
        state.detail = None
        state.detail_job = None
    if changed:
        state.generation += 1
        state.detail = None
        state.detail_job = None
        if key in {"r", "KEY_DOWN", "KEY_UP"} and state.pane == 0:
            state.rows = []
            state.page_cursor = None
            state.page_history = [None]
            state.counts = {}
            state.managers = []
    return state


class MonitorApp:
    """Curses adapter; all filesystem, adapter, and action calls are workers."""

    def __init__(self, stdscr: Any, views: list[WorkspaceView], refresh: float) -> None:
        self.stdscr = stdscr
        self.state = MonitorState(views)
        self.refresh_interval = refresh
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="httk-monitor")
        self.future: Future[tuple[int, WorkspaceView, dict[str, int], Any, list[dict[str, Any]]]] | None = None
        self.detail_future: Future[tuple[int, WorkspaceView, str, dict[str, Any]]] | None = None
        self.extra_future: Future[tuple[int, WorkspaceView, str, str, object]] | None = None
        self.tail_future: Future[tuple[int, WorkspaceView, str, str]] | None = None
        self.action_future: Future[ActionOutcome] | None = None
        self._pending_remove_ids: list[str] = []
        self.last_load = 0.0

    def _load(
        self,
        generation: int,
        view: WorkspaceView,
        kinds: tuple[str, ...],
        prefix: str | None,
        tag: str | None,
        cursor: str | None,
    ) -> tuple[int, WorkspaceView, dict[str, int], Any, list[dict[str, Any]]]:
        """Read only the selected view's counts, page, and managers."""

        page = view.page(
            kinds,
            prefix,
            tag,
            cursor,
            50,
        )
        counts = page.counts if page.counts is not None else view.counts(kinds, prefix)
        return generation, view, counts, page, [dict(item) for item in view.managers()]

    def _schedule_load(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if self.future is None and (force or now - self.last_load >= self.refresh_interval):
            if self.last_load and not force:
                self.state.view.refresh()
                self.state.generation += 1
                self._drop_read_inflight()
                self.state.rows = []
                self.state.next_after = None
                self.state.counts = {}
                self.state.managers = []
                self.state.detail = None
            generation = self.state.generation
            view = self.state.view
            kinds = self.state.kind_filter or tuple(STATE_KINDS)
            self.future = self.executor.submit(
                self._load,
                generation,
                view,
                kinds,
                self.state.placement_prefix,
                self.state.tag_contains,
                self.state.page_cursor,
            )
            self.state.loading = True
            self.state.next_after = None
            self.last_load = now

    def _collect(self) -> None:
        if self.future is not None and self.future.done():
            self.state.loading = False
            try:
                generation, view, counts, page, managers = self.future.result()
                if generation == self.state.generation and view is self.state.view:
                    view.accept_page(page)
                    self.state.counts = counts
                    self.state.rows = list(page.jobs)
                    self.state.next_after = page.next_after
                    self.state.managers = managers
                    self.state.selected = min(self.state.selected, max(0, len(self.state.rows) - 1))
                    self.state.status = ""
            except Exception as exc:  # UI must survive an unavailable workspace
                self.state.status = f"read error: {exc}"
            self.future = None
        if self.detail_future is not None and self.detail_future.done():
            try:
                generation, view, job_id, detail = self.detail_future.result()
                if generation == self.state.generation and view is self.state.view and job_id == self.state.detail_job:
                    self.state.detail = detail
            except Exception as exc:
                self.state.status = f"detail error: {exc}"
            self.detail_future = None
        if self.tail_future is not None and self.tail_future.done():
            try:
                generation, view, job_id, chunk = self.tail_future.result()
                if (
                    generation == self.state.generation
                    and view is self.state.view
                    and job_id == self.state.detail_job
                    and self.state.detail is not None
                    and chunk
                ):
                    if chunk.startswith("remote stdio tail"):
                        self.state.status = chunk
                    else:
                        self.state.detail["stdio_tail"] = (str(self.state.detail.get("stdio_tail", "")) + chunk)[-8192:]
            except Exception as exc:
                self.state.status = f"tail error: {exc}"
            self.tail_future = None
        if self.extra_future is not None and self.extra_future.done():
            try:
                generation, view, job_id, kind, value = self.extra_future.result()
                if generation == self.state.generation and view is self.state.view and job_id == self.state.detail_job:
                    if kind == "why":
                        self.state.detail = {**(self.state.detail or {}), "why": value}
                    else:
                        self.state.detail = {**(self.state.detail or {}), "frames": value}
            except Exception as exc:
                self.state.status = f"detail error: {exc}"
            self.extra_future = None
        if self.action_future is not None and self.action_future.done():
            action_future = self.action_future
            self.action_future = None
            outcome = action_future.result()
            view = outcome.view
            view.refresh()
            message = outcome.result if outcome.error is None else f"action error: {outcome.error}"
            if outcome.generation == self.state.generation and view is self.state.view:
                self.state.generation += 1
                self.state.rows = []
                self.state.counts = {}
                self.state.managers = []
                self.state.detail = None
                self.state.detail_job = None
                self.state.page_cursor = None
                self.state.page_history = [None]
                self.state.status = message or "action completed"
                self.last_load = 0.0
                self._drop_read_inflight()
            else:
                self.state.status = f"{view.name}: {message or 'action completed'}"

    def _draw(self) -> None:
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()
        left = min(30, max(20, width // 5))
        centre = max(35, (width - left) // 2)
        right = max(1, width - left - centre)

        def add(row: int, column: int, text: str, cells: int) -> None:
            try:
                self.stdscr.addnstr(row, column, text, max(1, cells))
            except curses.error:
                pass

        for line_no, line in enumerate(render_workspace_pane(self.state, width=left - 1, height=height - 2)):
            add(line_no, 0, line, left - 1)
        for line_no, line in enumerate(
            render_job_pane(self.state.rows, selected=self.state.selected, width=centre - 1, height=height - 2)
        ):
            add(line_no, left, line, centre - 1)
        for line_no, line in enumerate(render_detail_pane(self.state.detail, width=right - 1, height=height - 2)):
            add(line_no, left + centre, line, right - 1)
        add(height - 1, 0, render_status(self.state, width=max(1, width - 1)), max(1, width - 1))
        try:
            self.stdscr.refresh()
        except curses.error:
            pass

    def _load_detail(self) -> None:
        if self.state.detail_job is None or self.state.detail is not None or self.detail_future is not None:
            return
        job_id = self.state.detail_job
        generation = self.state.generation
        view = self.state.view
        self.detail_future = self.executor.submit(self._detail_load, generation, view, job_id)

    @staticmethod
    def _detail_load(
        generation: int, view: WorkspaceView, job_id: str
    ) -> tuple[int, WorkspaceView, str, dict[str, Any]]:
        """Load a detail report with its generation identity."""

        return generation, view, job_id, view.detail(job_id, generation=generation)

    @staticmethod
    def _extra_load(
        generation: int, view: WorkspaceView, job_id: str, kind: str
    ) -> tuple[int, WorkspaceView, str, str, object]:
        """Load one explicit diagnosis or history request."""

        value = view.why(job_id, generation=generation) if kind == "why" else view.log(job_id, generation=generation)
        return generation, view, job_id, kind, value

    @staticmethod
    def _tail_load(generation: int, view: WorkspaceView, job_id: str) -> tuple[int, WorkspaceView, str, str]:
        """Load one bounded stdio follow chunk with its task identity."""

        return generation, view, job_id, view.tail(job_id)

    def _prompt(self, text: str) -> str:
        """Read a short value after putting its prompt on the status line."""

        height, width = self.stdscr.getmaxyx()
        self.state.status = text
        self._draw()
        curses.echo()
        try:
            value = self.stdscr.getstr(
                height - 1,
                min(len(text) + 1, max(0, width - 2)),
                max(1, width - len(text) - 2),
            )
        finally:
            curses.noecho()
        return value.decode("utf-8", "replace").strip()

    def _selected_ids(self) -> list[str]:
        row = self.state.selected_row
        return [] if row is None else [str(row["job_id"])]

    def _handle_confirmation(self, normalized: str) -> None:
        """Resolve a status-line confirmation without losing its selected ids."""

        confirmation = self.state.confirmation
        pending_ids = list(self._pending_remove_ids)
        handle_key(self.state, normalized)
        if confirmation == "remove" and normalized.lower() in {"y", "yes"}:
            self._pending_remove_ids = []
            self._dispatch_action(Actions(self.state.view).remove, pending_ids)
        elif normalized.lower() in {"n", "no", "escape"}:
            self._pending_remove_ids = []

    def _dispatch_action(self, function: Any, *args: Any) -> None:
        if self.action_future is None:
            self.state.status = "working…"
            generation = self.state.generation
            view = self.state.view
            self.action_future = self.executor.submit(
                self._action_load,
                generation,
                view,
                function,
                args,
            )
        else:
            self.state.status = "action already running"

    @staticmethod
    def _action_load(generation: int, view: WorkspaceView, function: Any, args: tuple[Any, ...]) -> ActionOutcome:
        """Run one action with the view and generation captured at keypress."""

        try:
            return ActionOutcome(generation, view, result=function(*args))
        except Exception as exc:
            return ActionOutcome(generation, view, error=f"{type(exc).__name__}: {exc}")

    def _drop_read_inflight(self) -> None:
        """Cancel queued reads and forget completions from old generations."""

        for future in (self.future, self.detail_future, self.extra_future, self.tail_future):
            if future is not None:
                future.cancel()
        self.future = None
        self.detail_future = None
        self.extra_future = None
        self.tail_future = None
        self.state.loading = False

    def run(self) -> int:
        """Run until q or a curses error ends the session."""

        self.stdscr.keypad(True)
        self.stdscr.timeout(200)
        try:
            while True:
                self._schedule_load(force=self.last_load == 0.0)
                self._collect()
                self._load_detail()
                if self.state.follow and self.state.detail_job is not None and self.tail_future is None:
                    self.tail_future = self.executor.submit(
                        self._tail_load, self.state.generation, self.state.view, self.state.detail_job
                    )
                self._draw()
                key = self.stdscr.getch()
                if key < 0:
                    continue
                if key in (ord("q"), 27):
                    return 0
                normalized = {
                    curses.KEY_DOWN: "KEY_DOWN",
                    curses.KEY_UP: "KEY_UP",
                    curses.KEY_RESIZE: "KEY_RESIZE",
                    9: "TAB",
                    10: "ENTER",
                    13: "ENTER",
                }.get(key, chr(key) if 0 <= key < 256 else "")
                if self.state.confirmation is not None:
                    self._handle_confirmation(normalized)
                    continue
                if normalized == "f":
                    value = self._prompt("filter kind placement tag (blank clears): ")
                    parts = value.split()
                    self.state.kind_filter = tuple(part for part in parts if part in STATE_KINDS)
                    self.state.placement_prefix = next((part[5:] for part in parts if part.startswith("path=")), None)
                    self.state.tag_contains = next((part[4:] for part in parts if part.startswith("tag=")), None)
                    self.state.page_cursor = None
                    self.state.page_history = [None]
                    self.state.detail = None
                    self.state.generation += 1
                    self._drop_read_inflight()
                    self.state.rows = []
                    self.state.counts = {}
                    self.state.managers = []
                    self._schedule_load(force=True)
                elif normalized in {"c", "P", "C"}:
                    self._dispatch_action(
                        Actions(self.state.view).request,
                        {"c": "cancel", "P": "pause", "C": "continue"}[normalized],
                        self._selected_ids(),
                        "requested from workflow monitor",
                    )
                elif normalized in {"w", "l"}:
                    row = self.state.selected_row
                    if row is not None and self.extra_future is None:
                        job_id = str(row["job_id"])
                        self.state.detail_job = job_id
                        self.state.status = "loading diagnosis" if normalized == "w" else "loading history"
                        self.extra_future = self.executor.submit(
                            self._extra_load,
                            self.state.generation,
                            self.state.view,
                            job_id,
                            "why" if normalized == "w" else "log",
                        )
                elif normalized == "m":
                    count = self._prompt("manager count [1]: ") or "1"
                    launcher = self._prompt("launcher (blank for workspace setting): ") or None
                    try:
                        self._dispatch_action(Actions(self.state.view).start_managers, int(count), launcher)
                    except ValueError as exc:
                        self.state.status = str(exc)
                elif normalized == "x":
                    destination = self._prompt("destination workspace: ")
                    if destination:
                        self._dispatch_action(Actions(self.state.view).transfer, self._selected_ids(), destination)
                elif normalized == "D":
                    row = self.state.selected_row
                    if self.state.view.remote:
                        self.state.status = "remove unavailable (remote)"
                    elif row is not None and row.get("state") not in {"succeeded", "failed", "cancelled"}:
                        self.state.status = "remove refused: selected job is not terminal"
                    else:
                        self._pending_remove_ids = self._selected_ids()
                        self.state.confirmation = "remove"
                        self.state.status = "remove selected terminal payload? [y/N]"
                elif normalized == "r":
                    self.state.view.refresh()
                    self.state.detail = None
                    self.state.generation += 1
                    self._drop_read_inflight()
                    self.state.rows = []
                    self.state.counts = {}
                    self.state.managers = []
                    self.state.page_cursor = None
                    self.state.page_history = [None]
                    self.last_load = 0.0
                    self._schedule_load(force=True)
                else:
                    old_generation = self.state.generation
                    handle_key(self.state, normalized)
                    if old_generation != self.state.generation:
                        self._drop_read_inflight()
                        self.state.rows = []
                        self.state.detail = None
                        if self.state.pane == 0 or normalized in {"n", "p", "r"}:
                            self.state.page_cursor = self.state.page_cursor if normalized in {"n", "p"} else None
                            if normalized in {"r"} or self.state.pane == 0:
                                self.state.page_history = [None]
                        self.last_load = 0.0
                        self._schedule_load(force=True)
        finally:
            self.executor.shutdown(wait=False, cancel_futures=True)


def run_curses(stdscr: Any, views: list[WorkspaceView], refresh: float) -> int:
    """Entry point passed to :func:`curses.wrapper`."""

    return MonitorApp(stdscr, views, refresh).run()


__all__ = [
    "ActionOutcome",
    "MonitorApp",
    "MonitorState",
    "handle_key",
    "render_detail_pane",
    "render_job_pane",
    "render_status",
    "render_workspace_pane",
    "run_curses",
]
