"""Bounded, resumable scheduling discovery on the filesystem kernel.

The manager never materializes or globally sorts a state tree: each claim pass
reads one window of ``ready`` (``discovery_budget`` jobs, in placement order)
through :func:`httk.workflow._kernel.list_jobs`, resumes after the window's
cursor on the next pass, and claims the best-priority jobs within the window.
These tests pin that a listing resumes without dropping a job, that a tick never
opens a terminal state tree, that priority is best-within-window with rotation
reaching a starved subtree, and that a placement-prefixed manager schedules only
its subtree.
"""

import os
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as v3
from httk.workflow import TaskManager, _kernel
from httk.workflow._state import TERMINAL_STATES


class _ListingSpy:
    """Record every directory ``os.scandir`` and ``os.listdir`` open while installed."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.paths: list[str] = []
        for name in ("scandir", "listdir"):
            real = getattr(os, name)

            def spy(*args: Any, _real: Any = real, **kwargs: Any) -> Any:
                if args:
                    self.paths.append(str(args[0]))
                return _real(*args, **kwargs)

            monkeypatch.setattr(os, name, spy)


def test_a_listing_resumes_from_a_cursor_and_drops_nothing(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    submitted = {
        v3.submit(ws, ("local:demo", "demo"), {"start": "succeed"}, placement=f"{root}/{index:02d}").job_id
        for root in ("a", "b", "c")
        for index in range(3)
    }

    seen: list[str] = []
    cursor: str | None = None
    while window := list(_kernel.list_jobs(ws, "ready", limit=2, start=cursor)):
        # No window ever reads more than its budget.
        assert len(window) <= 2
        seen += [ref.job_id for ref in window]
        cursor = window[-1].cursor
    # The bound defers work, it never drops or repeats it.
    assert sorted(seen) == sorted(submitted)


def test_a_tick_never_opens_the_terminal_state_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    installed = v3.install(ws, tmp_path / "package")
    for script in ({"start": "succeed"}, {"start": "fail"}):
        for index in range(3):
            v3.submit(ws, installed, script, placement=f"project/done/{index:02d}")
    v3.run(ws)
    assert all(list(_kernel.list_jobs(ws, state)) for state in ("succeeded", "failed"))
    v3.submit(ws, installed, {"start": "succeed"}, placement="project/active", pool="nothing-serves-this")

    spy = _ListingSpy(monkeypatch)
    with TaskManager(ws, heartbeat_interval=600.0) as manager:
        spy.paths.clear()
        manager.tick()
        tick_paths = tuple(spy.paths)

    for state in TERMINAL_STATES:
        needle = f"{os.sep}jobs{os.sep}{state}"
        assert not any(needle in path for path in tick_paths), f"a tick opened the {state} tree"
    # The spy is wired: the tick did list the ready tree.
    assert any(f"{os.sep}jobs{os.sep}ready" in path for path in tick_paths)


def test_windowed_priority_is_best_in_window_and_rotation_reaches_a_starved_subtree(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    installed = v3.install(ws, tmp_path / "package")
    for index in range(3):
        v3.submit(ws, installed, {"start": "sleep"}, tag=f"a{index}", placement=f"a/{index:02d}")
    # The globally most urgent job — the lowest priority number — sits in a
    # subtree the first window does not reach.
    v3.submit(ws, installed, {"start": "sleep"}, tag="b0", placement="b/00", priority=100)
    for index in range(1, 3):
        v3.submit(ws, installed, {"start": "sleep"}, tag=f"b{index}", placement=f"b/{index:02d}")

    def owned() -> set[str]:
        return {ref.job_key.split("--")[0] for ref in manager.owner.owned()}

    with TaskManager(ws, maximum_workers=1, discovery_budget=3, cancel_grace_seconds=0.5) as manager:
        manager.tick()
        # The first window covered subtree ``a`` only, so its claim is best-within-window, not global.
        assert owned() == {"a0"}
        manager.maximum_workers = 2
        manager.tick()
        # The cursor rotated to the starved subtree, where the best candidate is the global best.
        assert owned() == {"a0", "b0"}


def test_a_placement_prefixed_manager_schedules_only_within_its_prefix(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    installed = v3.install(ws, tmp_path / "package")
    a_ids = {v3.submit(ws, installed, {"start": "succeed"}, placement=f"a/{index:02d}").job_id for index in range(2)}
    b_ids = {v3.submit(ws, installed, {"start": "succeed"}, placement=f"b/{index:02d}").job_id for index in range(2)}

    v3.run(ws, placement_prefixes=("a",))
    # The manager scheduled only its assigned subtree; the other half is untouched.
    assert {ref.job_id for ref in _kernel.list_jobs(ws, "succeeded")} == a_ids
    assert {ref.job_id for ref in _kernel.list_jobs(ws, "ready")} == b_ids
    v3.run(ws, placement_prefixes=("b",))
    assert {ref.job_id for ref in _kernel.list_jobs(ws, "succeeded")} == a_ids | b_ids
