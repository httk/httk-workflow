"""Explicit job deletion, safety guards, and remote dispatch."""

import json
import os
import shutil
import uuid
from collections.abc import Sequence
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace
from httk.workflow import removal as removal_module
from httk.workflow.registry import WorkspaceBinding, create_workspace
from httk.workflow.removal import remove_jobs
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


def _payload(root: Path, tag: str, runner: str = _SUCCEED_RUNNER) -> tuple[Path, str]:
    """Create a minimal complete payload."""

    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    runner_path = files / "runner"
    runner_path.write_text(runner, encoding="utf-8")
    runner_path.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": tag,
                "name": tag,
                "workflow": "tests.delete",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "only",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
                "parent": None,
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def _workspace(tmp_path: Path) -> tuple[Workspace, str]:
    """Create and register a workspace for a CLI test."""

    root = tmp_path / "workspace"
    name = f"delete-{uuid.uuid4()}"
    create_workspace(name, root)
    return Workspace(root, durable=False), name


def _context(cwd: Path) -> CLIContext:
    """Build a CLI context rooted at *cwd*."""

    return CLIContext("httk", cwd)


def test_delete_succeeded_and_submitted_jobs_without_gc(tmp_path: Path) -> None:
    workspace, name = _workspace(tmp_path)
    succeeded_payload, succeeded_id = _payload(tmp_path / "source", "succeeded")
    workspace.submit(succeeded_payload, "jobs/succeeded")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30)
    submitted_payload, submitted_id = _payload(tmp_path / "source", "submitted")
    workspace.submit(submitted_payload, "jobs/submitted")

    assert (
        command(["job", "delete", "--force", "--workspace", name, succeeded_id, submitted_id], _context(tmp_path)) == 0
    )
    assert workspace.find_marker_by_id(succeeded_id) is None
    assert workspace.find_marker_by_id(submitted_id) is None
    assert not (workspace.root / "jobs" / "succeeded" / f"succeeded--{succeeded_id}").exists()
    assert not (workspace.root / "jobs" / "submitted" / f"submitted--{submitted_id}").exists()


def test_delete_ready_job_after_manual_payload_removal(tmp_path: Path) -> None:
    workspace, name = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "ready")
    marker = workspace.submit(payload, "jobs/ready")
    with workspace.open_journal_writer() as writer:
        marker = workspace.transition(writer, marker, "ready", {})
    installed_payload = workspace.payload_path(marker.placement, marker.job_key)
    shutil.rmtree(installed_payload)
    assert payload.exists()

    assert command(["job", "delete", "--force", "--workspace", name, job_id], _context(tmp_path)) == 0
    assert workspace.find_marker_by_id(job_id) is None


def test_delete_refuses_symlinked_payload_and_placement(tmp_path: Path) -> None:
    workspace, _name = _workspace(tmp_path)
    payload, _job_id = _payload(tmp_path / "source", "symlink-payload")
    marker = workspace.submit(payload, "jobs/symlink-payload")
    installed_payload = workspace.payload_path(marker.placement, marker.job_key)
    outside = tmp_path / "outside"
    outside.mkdir()
    shutil.rmtree(installed_payload)
    installed_payload.symlink_to(outside, target_is_directory=True)

    report = remove_jobs(workspace, [marker], force=True)
    assert report.refused and "not a real directory" in (report.refused[0].reason or "")
    assert marker.path.exists() and installed_payload.is_symlink() and outside.exists()

    payload2, job_id2 = _payload(tmp_path / "source", "symlink-placement")
    marker2 = workspace.submit(payload2, "jobs/symlink-placement")
    installed_root = workspace.root / "jobs"
    moved_root = tmp_path / "moved-jobs"
    shutil.move(installed_root, moved_root)
    installed_root.symlink_to(moved_root, target_is_directory=True)

    report = remove_jobs(workspace, [marker2], force=True)
    assert report.refused and "placement" in (report.refused[0].reason or "")
    assert marker2.path.exists() and job_id2 == marker2.job_id


@pytest.mark.parametrize("directory", ("attempts", "logs"))
def test_delete_refuses_symlinked_attempt_or_log_directory(tmp_path: Path, directory: str) -> None:
    workspace, _name = _workspace(tmp_path)
    payload, _job_id = _payload(tmp_path / "source", directory)
    marker = workspace.submit(payload, f"jobs/{directory}")
    installed_payload = workspace.payload_path(marker.placement, marker.job_key)
    outside = tmp_path / f"outside-{directory}"
    outside.mkdir()
    (installed_payload / directory).symlink_to(outside, target_is_directory=True)

    report = remove_jobs(workspace, [marker], force=True)
    assert report.refused and directory in (report.refused[0].reason or "")
    assert marker.path.exists() and installed_payload.exists() and outside.exists()


def test_delete_marker_move_during_unlink_leaves_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    workspace, name = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "raced")
    marker = workspace.submit(payload, "jobs/raced")
    installed_payload = workspace.payload_path(marker.placement, marker.job_key)
    claimed_path = workspace.control / "state" / "claimed" / marker.placement / marker.path.name

    def move_then_disappear(path: Path) -> None:
        claimed_path.parent.mkdir(parents=True)
        os.rename(path, claimed_path)
        raise FileNotFoundError(path)

    monkeypatch.setattr(removal_module.os, "unlink", move_then_disappear)
    assert command(["job", "delete", "--force", "--workspace", name, job_id], _context(tmp_path)) == 1
    assert "state changed; not removed" in capsys.readouterr().out
    assert installed_payload.exists()
    assert workspace.find_marker_by_id(job_id) is not None


def test_delete_refuses_running_job_through_cli_without_mutation(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "running")
    marker = workspace.submit(payload, "jobs/running")
    with workspace.open_journal_writer() as writer:
        workspace.transition(writer, marker, "running", {})

    assert command(["job", "delete", "--force", "--workspace", name, job_id], _context(tmp_path)) == 1
    assert "cancel it first" in capsys.readouterr().out
    assert workspace.find_marker_by_id(job_id) is not None
    assert workspace.payload_path(marker.placement, marker.job_key).exists()


def test_delete_prompt_decline_and_non_tty_refusal_leave_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    workspace, name = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "prompt")
    marker = workspace.submit(payload, "jobs/prompt")
    installed_payload = workspace.payload_path(marker.placement, marker.job_key)

    monkeypatch.setattr(job_cli.sys.stdin, "isatty", lambda: False)
    assert command(["job", "delete", "--workspace", name, job_id], _context(tmp_path)) == 1
    assert "without a terminal requires --force" in capsys.readouterr().err
    assert installed_payload.exists() and marker.path.exists()

    monkeypatch.setattr(job_cli.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")
    assert command(["job", "delete", "--workspace", name, job_id], _context(tmp_path)) == 1
    assert "not removed" in capsys.readouterr().out
    assert installed_payload.exists() and marker.path.exists()


def test_delete_path_glob_and_batch_resolution_are_safe(tmp_path: Path) -> None:
    workspace, name = _workspace(tmp_path)
    first, first_id = _payload(tmp_path / "source", "silicon-one")
    second, second_id = _payload(tmp_path / "source", "silicon-two")
    workspace.submit(first, "jobs/silicon-one")
    workspace.submit(second, "jobs/silicon-two")
    context = _context(workspace.root)

    assert command(["job", "delete", "--force", "--workspace", name, "jobs/silicon*"], context) == 0
    assert workspace.find_marker_by_id(first_id) is None
    assert workspace.find_marker_by_id(second_id) is None

    third, third_id = _payload(tmp_path / "source", "protected")
    workspace.submit(third, "jobs/protected")
    assert command(["job", "delete", "--force", "--workspace", name, "missing-selector", third_id], context) != 0
    assert workspace.find_marker_by_id(third_id) is not None


def test_delete_join_child_guard_and_force(tmp_path: Path) -> None:
    workspace, name = _workspace(tmp_path)
    child_payload, child_id = _payload(tmp_path / "source", "child")
    child = workspace.submit(child_payload, "jobs/child")
    parent_payload, _parent_id = _payload(tmp_path / "source", "parent")
    parent = workspace.submit(parent_payload, "jobs/parent")
    with workspace.open_journal_writer() as writer:
        workspace.transition(
            writer,
            parent,
            "waiting",
            {
                "join": {
                    "children": [
                        {
                            "workspace_id": workspace.workspace_id,
                            "job_id": child_id,
                            "job_key": child.job_key,
                            "placement_hint": child.placement.as_posix(),
                        }
                    ],
                    "condition": "all_terminal",
                }
            },
        )

    guarded = remove_jobs(workspace, [child])
    assert guarded.refused and "non-terminal parent" in (guarded.refused[0].reason or "")
    assert child_payload.exists() and child.path.exists()

    assert command(["job", "delete", "--workspace", name, "--force", child_id], _context(tmp_path)) == 0
    assert workspace.find_marker_by_id(child_id) is None


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
