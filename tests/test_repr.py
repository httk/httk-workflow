"""Repr policy: runner-author-facing handles render an informative class-named repr."""

from pathlib import Path
from types import SimpleNamespace

from httk.workflow.manager import RunningAttempt, TaskManager
from httk.workflow.sdk import Attempt, Runner
from httk.workflow.workspace import Workspace


def _assert_informative(text: str, class_name: str) -> None:
    assert text.startswith(f"{class_name}("), text
    assert " object at 0x" not in text, text


def test_runner_repr() -> None:
    _assert_informative(repr(Runner("tests.campaign")), "Runner")


def test_workspace_and_manager_repr(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _assert_informative(repr(workspace), "Workspace")
    with TaskManager(workspace) as manager:
        _assert_informative(repr(manager), "TaskManager")


def test_attempt_repr() -> None:
    # A live Attempt needs a manager-written context tree; the repr logic reads
    # only these three fields, so exercise it directly.
    stub = SimpleNamespace(
        context=SimpleNamespace(job_id="single--abc", attempt_id="att-1"),
        step="relax",
    )
    _assert_informative(Attempt.__repr__(stub), "Attempt")  # type: ignore[arg-type]


def test_running_attempt_repr() -> None:
    stub = SimpleNamespace(attempt_id="att-1", process=SimpleNamespace(pid=4321))
    _assert_informative(RunningAttempt.__repr__(stub), "RunningAttempt")  # type: ignore[arg-type]
