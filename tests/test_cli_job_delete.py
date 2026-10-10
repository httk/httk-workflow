"""Explicit job deletion, safety guards, and remote dispatch."""

import json
import shutil
import uuid
from collections.abc import Sequence
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace, _kernel
from httk.workflow._state import Release, StateDoc
from httk.workflow.registry import WorkspaceBinding, create_workspace
from httk.workflow.removal import remove_jobs
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command
from v3_helpers import cli_owner, find, job_mapping, state_of, submit_mapping
from v3_helpers import workspace as v3_workspace

_WORKFLOW = ("demo--0123456789abcdef", "demo")


def _workspace(tmp_path: Path) -> tuple[Workspace, str]:
    """Create and register a workspace for a CLI test."""

    root = tmp_path / "workspace"
    name = f"delete-{uuid.uuid4()}"
    create_workspace(name, root)
    return Workspace(root, durable=False), name


def _context(cwd: Path) -> CLIContext:
    """Build a CLI context rooted at *cwd*."""

    return CLIContext("httk", cwd)


def _at(workspace: Workspace, state: str, placement: str, **mapping: object) -> _kernel.JobRef:
    """A job moved to *state* by a CLI owner; *mapping* overrides ``job.json`` members."""

    ref = submit_mapping(workspace, {**job_mapping(_WORKFLOW, {"start": "succeed"}, placement=placement), **mapping})
    if state == "ready":
        return ref
    with cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        return owned.release(StateDoc.empty(owned.job_id).next_activation("start", "initial"), Release(state, 500))


def test_delete_applies_inline_to_unowned_terminal_and_paused_jobs_only(tmp_path: Path) -> None:
    workspace = v3_workspace(tmp_path / "workspace")
    failed, paused, ready = (_at(workspace, state, f"p/{state}") for state in ("failed", "paused", "ready"))
    report = remove_jobs(workspace, [failed, paused, ready])
    assert [(outcome.job_key, outcome.removed) for outcome in report.outcomes] == [
        (failed.job_key, True),
        (paused.job_key, True),
        (ready.job_key, False),
    ]
    assert report.refused[0].reason == "only terminal or paused jobs are deleted, not ready"
    assert find(workspace, ready.job_id).state == "ready"
    assert _kernel.locate(workspace, failed.job_id, placement_hint=None, exhaustive=True) is None
    # The dropped request is recorded in the job's history and its file is gone.
    assert state_of(find(workspace, ready.job_id)).history_tail[-2]["event"] == "request_dropped"
    assert not list((workspace.control / "requests").iterdir())


def test_delete_of_a_held_job_waits_for_its_owner(tmp_path: Path) -> None:
    workspace = v3_workspace(tmp_path / "workspace")
    failed = _at(workspace, "failed", "p/0")
    with cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, failed)
        assert owned is not None
        report = remove_jobs(workspace, [failed])
        assert report.refused[0].reason == "a manager holds the job; the request applies at its next boundary"
        owned.release(owned.read_state() or StateDoc.empty(owned.job_id), Release("failed", 500))
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
    assert _kernel.locate(workspace, failed.job_id, placement_hint=None, exhaustive=True) is None


@pytest.mark.parametrize("directory", ("attempts", "logs", "run"))
def test_delete_removes_a_symlink_in_the_job_without_following_it(tmp_path: Path, directory: str) -> None:
    workspace = v3_workspace(tmp_path / "workspace")
    failed = _at(workspace, "failed", "p/0")
    outside = tmp_path / f"outside-{directory}"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep", encoding="utf-8")
    target = failed.path / directory
    if target.exists():
        shutil.rmtree(target)
    target.symlink_to(outside, target_is_directory=True)
    assert remove_jobs(workspace, [failed]).removed_count == 1
    assert (outside / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_cli_delete_removes_unowned_failed_and_paused_jobs_and_refuses_the_others(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    failed, paused, ready, succeeded = (
        _at(workspace, state, f"p/{state}") for state in ("failed", "paused", "ready", "succeeded")
    )
    selectors = [failed.job_id, paused.job_id, ready.job_id, succeeded.job_id]

    assert command(["job", "delete", "--force", "--workspace", name, *selectors], _context(tmp_path)) == 1
    out = capsys.readouterr().out
    assert f"{failed.job_key}\tfailed\tremoved" in out and f"{paused.job_key}\tpaused\tremoved" in out
    assert f"{ready.job_key}\tready\trefused\tonly terminal or paused jobs are deleted, not ready" in out
    assert "release it with `job unseal` first" in out and "removed 2 of 4 job(s)" in out
    assert [
        _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True) for ref in (failed, paused)
    ] == [None, None]
    assert find(workspace, ready.job_id).state == "ready" and find(workspace, succeeded.job_id).state == "succeeded"


def test_cli_delete_of_a_held_job_leaves_the_request_to_its_owner(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    failed = _at(workspace, "failed", "p/held")
    with cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, failed)
        assert owned is not None
        assert command(["job", "delete", "--force", "--workspace", name, failed.job_id], _context(tmp_path)) == 1
        assert "a manager holds the job; the request applies at its next boundary" in capsys.readouterr().out
        assert owned.path.is_dir()
        owned.release(owned.read_state() or StateDoc.empty(owned.job_id), Release("failed", 500))


def test_delete_prompt_decline_and_non_tty_refusal_leave_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    workspace, name = _workspace(tmp_path)
    failed = _at(workspace, "failed", "prompt")

    monkeypatch.setattr(job_cli.sys.stdin, "isatty", lambda: False)
    assert command(["job", "delete", "--workspace", name, failed.job_id], _context(tmp_path)) == 1
    assert "without a terminal requires --force" in capsys.readouterr().err
    monkeypatch.setattr(job_cli.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")
    assert command(["job", "delete", "--workspace", name, failed.job_id], _context(tmp_path)) == 1
    assert "not removed" in capsys.readouterr().out
    assert find(workspace, failed.job_id).state == "failed"
    assert not list(workspace.control.glob("requests/*.json"))


def test_delete_path_glob_and_batch_resolution_are_safe(tmp_path: Path) -> None:
    workspace, name = _workspace(tmp_path)
    first, second = (_at(workspace, "failed", f"silicon-{index}") for index in ("one", "two"))
    context = _context(workspace.root)

    assert command(["job", "delete", "--force", "--workspace", name, "jobs/failed/silicon*"], context) == 0
    assert all(
        _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True) is None for ref in (first, second)
    )

    # Every selector resolves before anything is requested.
    third = _at(workspace, "failed", "protected")
    assert command(["job", "delete", "--force", "--workspace", name, "missing-selector", third.job_id], context) != 0
    assert find(workspace, third.job_id).state == "failed"


def test_delete_of_a_join_child_is_refused_while_its_parent_waits(tmp_path: Path) -> None:
    workspace = v3_workspace(tmp_path / "workspace")
    parent = _at(workspace, "ready", "p/parent")
    parent_link = {
        "workspace_id": workspace.workspace_id,
        "job_id": parent.job_id,
        "job_key": parent.job_key,
        "placement": "p/parent",
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }
    child = _at(workspace, "failed", "p/child", parent=parent_link)
    with cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, parent)
        assert owned is not None
        join = {"children": [{"job_id": child.job_id, "job_key": child.job_key, "placement_hint": "p/child"}]}
        doc = StateDoc.empty(owned.job_id).next_activation("start", "initial").updated(join=join)
        parent = owned.release(doc, Release("waiting", 500))
    guarded = remove_jobs(workspace, [child])
    assert guarded.refused and "is waiting on it" in (guarded.refused[0].reason or "")
    assert find(workspace, child.job_id).state == "failed"


def test_remote_delete_confirms_locally_and_forwards_confirmed(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    binding = WorkspaceBinding("cluster:station", "cluster", None)
    calls: list[list[str]] = []
    job_id = str(uuid.uuid4())
    show_output = json.dumps([{"job_id": job_id, "job_key": f"remote--{job_id}", "state": "ready"}])
    monkeypatch.setattr(job_cli, "_resolve_binding", lambda *_args: (binding, None))

    def fake_output(_binding: object, _context: object, argv: Sequence[str], **_kwargs: object) -> tuple[int, str, str]:
        calls.append(list(argv))
        return 0, show_output if argv[2] == "show" else "", ""

    monkeypatch.setattr(
        job_cli,
        "remote_workspace_output",
        fake_output,
    )

    monkeypatch.setattr(job_cli.sys.stdin, "isatty", lambda: False)
    assert command(["job", "delete", "--workspace", binding.name, job_id], _context(Path.cwd())) == 1
    assert "requires --force" in capsys.readouterr().err
    assert calls == []

    monkeypatch.setattr(job_cli.sys.stdin, "isatty", lambda: True)
    answers = iter(("n", "y"))
    prompts: list[str] = []

    def answer(prompt: str) -> str:
        prompts.append(prompt)
        return next(answers)

    monkeypatch.setattr("builtins.input", answer)
    assert command(["job", "delete", "--workspace", binding.name, job_id], _context(Path.cwd())) == 1
    assert command(["job", "delete", "--workspace", binding.name, job_id], _context(Path.cwd())) == 0
    assert prompts == ["Delete 1 jobs? [y/N] ", "Delete 1 jobs? [y/N] "]
    assert f"remote--{job_id}\tready" in capsys.readouterr().out
    assert calls[-2:] == [
        ["httk", "job", "show", "--json", "--no-children", job_id, "--workspace", "station"],
        ["httk", "job", "delete", "--confirmed", job_id, "--workspace", "station"],
    ]

    assert command(["job", "delete", "--force", "--workspace", binding.name, job_id], _context(Path.cwd())) == 0
    assert calls[-2:] == [
        ["httk", "job", "show", "--json", "--no-children", job_id, "--workspace", "station"],
        ["httk", "job", "delete", "--force", job_id, "--workspace", "station"],
    ]


def test_remote_delete_rejects_path_selectors_before_confirmation(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    binding = WorkspaceBinding("cluster:station", "cluster", None)
    calls: list[list[str]] = []
    unknown_id = str(uuid.uuid4())
    monkeypatch.setattr(job_cli, "_resolve_binding", lambda *_args: (binding, None))

    def fake_output(_binding: object, _context: object, argv: Sequence[str], **_kwargs: object) -> tuple[int, str, str]:
        calls.append(list(argv))
        return 0, "[]", ""

    monkeypatch.setattr(
        job_cli,
        "remote_workspace_output",
        fake_output,
    )

    assert command(["job", "delete", "--force", "--workspace", binding.name, unknown_id], _context(Path.cwd())) == 1
    assert "not found" in capsys.readouterr().err
    assert calls == [["httk", "job", "show", "--json", "--no-children", unknown_id, "--workspace", "station"]]

    calls.clear()
    assert command(["job", "delete", "--workspace", binding.name, "jobs/silicon*"], _context(Path.cwd())) == 2
    assert "canonical job ids" in capsys.readouterr().err
    assert calls == []
