"""The workspace settings verbs: one key view over settings, policy and preludes, preserving format sections and seals."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import fake_remote
from httk.workflow import workspace as workspace_module
from httk.workflow.adapters import (
    REMOTE_WORKSPACE_INIT_COMMAND,
    REMOTE_WORKSPACE_SET_COMMAND,
    REMOTE_WORKSPACE_SHOW_COMMAND,
    REMOTE_WORKSPACE_UNSET_COMMAND,
)
from httk.workflow.errors import SealedError
from httk.workflow.models import WorkspacePolicy
from httk.workflow.projects import initialize_project
from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import _common, _workspace, command
from httk.workflow.workspace import Workspace


def _format(root: Path) -> bytes:
    return (Workspace(root).control / "format.json").read_bytes()


def test_set_show_unset_round_trip_preserves_the_document(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "set", "-w", "ws", "app.command=run", "app.count=2"], context) == 0
    assert json.loads(capsys.readouterr().out) == {"app.command": "run", "app.count": 2}
    before = _format(tmp_path / "ws")
    assert command(["workspace", "set", "-w", "ws", "app.count=3", "bad key=2"], context) == 2
    assert _format(tmp_path / "ws") == before
    assert command(["workspace", "unset", "-w", "ws", "app.command"], context) == 0
    assert json.loads(capsys.readouterr().out) == ["app.command"]
    assert command(["workspace", "show", "-w", "ws", "--json", "app.count"], context) == 0
    assert json.loads(capsys.readouterr().out) == {"app.count": 2}
    assert command(["workspace", "unset", "-w", "ws", "app.count"], context) == 0
    document = json.loads(_format(tmp_path / "ws"))
    assert document["settings"] == {}
    assert "policy" in document and "workflow_preludes" in document


def test_one_set_writes_all_three_stores_and_show_reads_them(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    workspace = Workspace(tmp_path / "ws")
    workspace.set_policy({"retention": {"owner_tombstone_days": 7}})
    script = tmp_path / "prelude.sh"
    script.write_text("module purge\nmodule load VASP\n", encoding="utf-8")
    context = CLIContext("httk", tmp_path)
    argv = ["workspace", "set", "-w", "ws", "vasp.command=mock", "policy.retention.trash_days=14"]
    assert command([*argv, f"prelude.relax-vasp=@{script}"], context) == 0
    assert json.loads(capsys.readouterr().out) == {
        "vasp.command": "mock",
        "policy.retention.trash_days": 14,
        "prelude.relax-vasp": "module purge\nmodule load VASP\n",
    }
    workspace = Workspace(tmp_path / "ws")
    assert workspace.settings == {"vasp.command": "mock"}
    assert workspace.policy.retention.trash_days == 14 and workspace.policy.retention.owner_tombstone_days == 7
    assert workspace.read_workflow_preludes() == {"relax-vasp": "module purge\nmodule load VASP\n"}

    # One KEY prints its bare value: JSON for a setting or policy member, the text for a prelude.
    for key, printed in (
        ("vasp.command", '"mock"\n'),
        ("policy.retention.trash_days", "14.0\n"),
        ("policy.visibility_deadline_seconds", f"{WorkspacePolicy().visibility_deadline_seconds}\n"),
        ("prelude.relax-vasp", "module purge\nmodule load VASP\n\n"),
    ):
        assert command(["workspace", "show", "-w", "ws", key], context) == 0
        assert capsys.readouterr().out == printed
    assert command(["workspace", "show", "-w", "ws"], context) == 0
    rows = {row[0]: row[1:] for row in (line.split("\t") for line in capsys.readouterr().out.splitlines())}
    assert rows["vasp.command"][0] == '"mock"' and len(rows["vasp.command"]) == 2
    assert rows["policy.retention.owner_tombstone_days"] == ["7.0"]
    assert rows["prelude.relax-vasp"] == [json.dumps("module purge\nmodule load VASP\n")]
    assert command(["workspace", "show", "-w", "ws", "--json"], context) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["policy.retention.trash_days"] == 14 and shown["prelude.relax-vasp"].startswith("module purge")
    assert command(["workspace", "show", "-w", "ws", "prelude.other"], context) == 2
    assert "workspace key is not set: prelude.other" in capsys.readouterr().err


@pytest.mark.parametrize(
    "bad",
    [
        "policy.retention.typo=1",
        "policy.visibility_deadline_seconds=not-json",
        "policy.visibility_deadline_seconds=-1",
        "policy.retention.journal_days=1",
        "prelude.x=@missing.sh",
        "prelude.bad id=x",
        "io.workers=0",
        "bad key=1",
    ],
)
def test_a_failing_pair_leaves_all_three_stores_untouched(tmp_path: Path, capsys, bad: str) -> None:
    create_workspace("ws", tmp_path / "ws")
    before = _format(tmp_path / "ws")
    argv = ["workspace", "set", "-w", "ws", "app.x=1", "policy.retention.trash_days=3", "prelude.wf=echo", bad]
    assert command(argv, CLIContext("httk", tmp_path)) == 2
    assert capsys.readouterr().err
    assert _format(tmp_path / "ws") == before


@pytest.mark.parametrize("key", ["policy", "prelude", "policy.lease_seconds", "policy.retention.foo"])
def test_reserved_namespaces_are_never_application_settings(tmp_path: Path, capsys, key: str) -> None:
    create_workspace("ws", tmp_path / "ws")
    assert command(["workspace", "set", "-w", "ws", f"{key}=1"], CLIContext("httk", tmp_path)) == 2
    capsys.readouterr()
    assert Workspace(tmp_path / "ws").settings == {}
    with pytest.raises(ValueError, match="reserved"):
        Workspace(tmp_path / "ws").configure_settings({"prelude.x": 1})


def test_policy_unset_restores_defaults_and_rejects_unknown_keys(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    workspace = Workspace(tmp_path / "ws")
    workspace.set_policy({"visibility_deadline_seconds": 72, "retention": {"owner_tombstone_days": 7, "trash_days": 9}})
    keys = ["policy.visibility_deadline_seconds", "policy.retention.owner_tombstone_days"]
    assert command(["workspace", "unset", "-w", "ws", *keys], context) == 0
    assert json.loads(capsys.readouterr().out) == keys
    policy = Workspace(workspace.root).policy
    assert policy.visibility_deadline_seconds == WorkspacePolicy().visibility_deadline_seconds
    assert policy.retention.owner_tombstone_days == WorkspacePolicy().retention.owner_tombstone_days
    assert policy.retention.trash_days == 9
    for key in ("policy.retention.typo", "policy.typo", "prelude.unset"):
        before = _format(workspace.root)
        assert command(["workspace", "unset", "-w", "ws", key], context) == 2
        assert _format(workspace.root) == before
    assert command(["workspace", "unset", "-w", "ws", "policy.retention"], context) == 0
    assert Workspace(workspace.root).policy.retention == WorkspacePolicy().retention


def _count_writes(monkeypatch) -> list[int]:
    """Count the ``format.json`` read-modify-writes a command makes."""

    writes: list[int] = []
    original = Workspace._update_format

    def spy(self, mutate):
        writes.append(1)
        return original(self, mutate)

    monkeypatch.setattr(Workspace, "_update_format", spy)
    return writes


def test_a_mixed_set_and_a_mixed_unset_are_one_write_each(tmp_path: Path, monkeypatch, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    Workspace(tmp_path / "ws").set_policy({"retention": {"owner_tombstone_days": 7}})
    writes = _count_writes(monkeypatch)
    argv = ["workspace", "set", "-w", "ws", "a=1", "b=2", "policy.visibility_deadline_seconds=61"]
    argv += ["policy.retention.trash_days=14", "policy.retention.daemon_history_days=keep", "prelude.wf=true"]
    assert command(argv, context) == 0
    assert json.loads(capsys.readouterr().out)["policy.visibility_deadline_seconds"] == 61
    assert len(writes) == 1
    workspace = Workspace(tmp_path / "ws")
    assert workspace.settings == {"a": 1, "b": 2} and workspace.read_workflow_preludes() == {"wf": "true"}
    policy = workspace.policy
    assert policy.visibility_deadline_seconds == 61
    assert policy.retention.trash_days == 14 and policy.retention.daemon_history_days is None
    assert policy.retention.owner_tombstone_days == 7

    writes.clear()
    keys = ["a", "prelude.wf", "policy.visibility_deadline_seconds", "b"]
    assert command(["workspace", "unset", "-w", "ws", *keys], context) == 0
    assert json.loads(capsys.readouterr().out) == keys
    assert len(writes) == 1
    workspace = Workspace(tmp_path / "ws")
    assert workspace.settings == {} and workspace.read_workflow_preludes() == {}
    assert workspace.policy.visibility_deadline_seconds == WorkspacePolicy().visibility_deadline_seconds
    assert command(["workspace", "set", "-w", "ws", "novalue"], context) == 2


def test_a_failure_during_the_one_write_leaves_the_document_unchanged(tmp_path: Path, monkeypatch, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    Workspace(tmp_path / "ws").configure_settings({"a": 1, "c": 3})
    context = CLIContext("httk", tmp_path)
    before = _format(tmp_path / "ws")
    # A change refused inside the write (a prelude that is not set) stores none of the others.
    assert command(["workspace", "unset", "-w", "ws", "a", "policy.retention", "prelude.unset"], context) == 2
    assert command(["workspace", "set", "-w", "ws", "b=2", "policy.retention=[]", "prelude.wf=x"], context) == 2
    assert _format(tmp_path / "ws") == before

    def fail(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(workspace_module._fs, "write_file", fail)
    argv = ["workspace", "set", "-w", "ws", "b=2", "policy.retention.trash_days=3", "prelude.wf=x"]
    assert command(argv, context) == 2
    assert "disk full" in capsys.readouterr().err
    assert _format(tmp_path / "ws") == before


def test_set_collects_pairs_after_the_workspace_option(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    assert command(["workspace", "set", "a=3", "-w", "ws", "b=4"], CLIContext("httk", tmp_path)) == 0
    assert json.loads(capsys.readouterr().out) == {"a": 3, "b": 4}
    assert Workspace(tmp_path / "ws").settings == {"a": 3, "b": 4}


@pytest.mark.parametrize("member_first", [False, True])
def test_set_merges_retention_members_with_a_whole_retention_pair_in_order(
    tmp_path: Path, capsys, member_first: bool
) -> None:
    create_workspace("ws", tmp_path / "ws")
    workspace = Workspace(tmp_path / "ws")
    workspace.set_policy({"retention": {"owner_tombstone_days": 7, "trash_days": 9}})
    whole = 'policy.retention={"trash_days": 3}'
    member = "policy.retention.daemon_history_days=11"
    pairs = [member, whole] if member_first else [whole, member]
    assert command(["workspace", "set", "-w", "ws", *pairs], CLIContext("httk", tmp_path)) == 0
    capsys.readouterr()
    retention = Workspace(workspace.root).policy.retention
    defaults = WorkspacePolicy().retention
    assert retention.trash_days == 3
    # A later whole-object pair replaces an earlier member merge; a later member merges into it.
    assert retention.daemon_history_days == (defaults.daemon_history_days if member_first else 11)
    assert retention.owner_tombstone_days == defaults.owner_tombstone_days


def test_configuration_and_prelude_mutations_obey_workspace_seals(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    workspace.set_setting("app.command", "run")
    workspace.set_workflow_prelude("test.flow", "module load test")
    (workspace.control / "seal.json").write_text("{}")
    before = (workspace.control / "format.json").read_bytes()
    with pytest.raises(SealedError):
        workspace.configure_settings({"app.command": "other"})
    with pytest.raises(SealedError):
        workspace.unset_setting("app.command")
    with pytest.raises(SealedError):
        workspace.set_workflow_prelude("test.flow", "other")
    with pytest.raises(SealedError):
        workspace.unset_workflow_prelude("test.flow")
    assert (workspace.control / "format.json").read_bytes() == before


def _far_workspace(tmp_path: Path, monkeypatch, settings: dict[str, object]) -> tuple[Path, list[list[str]]]:
    """Bind ``cluster:far`` to a loopback remote and record every far-side command."""

    project = tmp_path / "project"
    initialize_project(project, name="one-call")
    fake_remote(project, template="local", name="cluster")
    create_workspace("far", tmp_path / "far")
    if settings:
        Workspace(tmp_path / "far").configure_settings(settings)
    calls: list[list[str]] = []
    original = _common.remote_workspace_output

    def spy(binding, context, argv, **kwargs):
        calls.append(list(argv))
        return original(binding, context, argv, **kwargs)

    monkeypatch.setattr(_common, "remote_workspace_output", spy)
    return project, calls


def test_remote_verbs_are_one_forwarded_command_each(tmp_path: Path, monkeypatch, capsys) -> None:
    project, calls = _far_workspace(tmp_path, monkeypatch, {"app.old": 1})
    context = CLIContext("httk", project)
    script = tmp_path / "far-prelude.sh"
    script.write_text("module load far\n", encoding="utf-8")
    pairs = ["a=1", "policy.retention.trash_days=9", f"prelude.wf=@{script}"]
    assert command(["workspace", "set", "-w", "cluster:far", "--no-durable", *pairs], context) == 0
    assert json.loads(capsys.readouterr().out) == {
        "a": 1,
        "policy.retention.trash_days": 9,
        "prelude.wf": "module load far\n",
    }
    far = Workspace(tmp_path / "far")
    assert far.settings == {"app.old": 1, "a": 1} and far.policy.retention.trash_days == 9
    assert command(["workspace", "show", "-w", "cluster:far", "--json", "a", "prelude.wf"], context) == 0
    assert json.loads(capsys.readouterr().out) == {"a": 1, "prelude.wf": "module load far\n"}
    assert command(["workspace", "unset", "-w", "cluster:far", "app.old", "policy.retention.trash_days"], context) == 0
    assert json.loads(capsys.readouterr().out) == ["app.old", "policy.retention.trash_days"]
    assert Workspace(tmp_path / "far").policy.retention.trash_days == WorkspacePolicy().retention.trash_days
    # The pairs travel verbatim, so the far side reads @FILE; the far workspace is --workspace NAME.
    assert calls == [
        [*REMOTE_WORKSPACE_SET_COMMAND, "--no-durable", "--workspace", "far", *pairs],
        [*REMOTE_WORKSPACE_SHOW_COMMAND, "--json", "--workspace", "far", "a", "prelude.wf"],
        [*REMOTE_WORKSPACE_UNSET_COMMAND, "--workspace", "far", "app.old", "policy.retention.trash_days"],
    ]
    calls.clear()
    # Local key validation refuses before anything is sent.
    assert command(["workspace", "set", "-w", "cluster:far", "a=2", "bad key=new"], context) == 2
    assert command(["workspace", "unset", "-w", "cluster:far", "policy.typo"], context) == 2
    assert not calls


def test_remote_set_failure_is_relayed_with_a_trailing_newline(tmp_path: Path, monkeypatch, capsys) -> None:
    project, _calls = _far_workspace(tmp_path, monkeypatch, {})
    monkeypatch.setattr(_common, "remote_workspace_output", lambda *_args, **_kwargs: (3, "partial", "remote error\n"))
    assert command(["workspace", "set", "-w", "cluster:far", "a=1"], CLIContext("httk", project)) == 3
    captured = capsys.readouterr()
    assert captured.out == "partial\n" and captured.err == "remote error\n"


def test_init_takes_pairs_after_an_option_and_forwards_them_positionally(tmp_path: Path, monkeypatch, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "init", str(tmp_path / "ws"), "--name", "named", "a=1", "b=x"], context) == 0
    assert Workspace(tmp_path / "ws").settings == {"a": 1, "b": "x"}
    for pair, message in (
        ("policy.visibility_deadline_seconds=1", "application settings only"),
        ("policy.typo=1", "application settings only"),
        ("prelude.wf=x", "application settings only"),
        ("bad key=1", "dotted identifier"),
    ):
        assert command(["workspace", "init", str(tmp_path / "other"), pair], context) == 2
        assert message in capsys.readouterr().err
        assert not (tmp_path / "other").exists()

    project = tmp_path / "project"
    initialize_project(project, name="remote-init")
    fake_remote(project, template="local", name="cluster")
    sent: list[list[str]] = []

    def adapter(_bundle, _operation: str, request: dict[str, object], **_kwargs):
        sent.append(list(request["argv"]))  # type: ignore[call-overload]
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(_workspace, "run_adapter", adapter)
    assert command(["workspace", "init", "cluster:runs", "--name", "runs", "a=1"], CLIContext("httk", project)) == 0
    (argv,) = sent
    assert argv[:6] == [*REMOTE_WORKSPACE_INIT_COMMAND, "--name", "runs", "runs"] and argv[-1] == "a=1"
