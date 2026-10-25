"""Workspace configuration edits preserve policy defaults, format sections, and seals."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import fake_remote
from httk.workflow.adapters import (
    REMOTE_WORKSPACE_CONFIGURE_COMMAND,
    REMOTE_WORKSPACE_POLICY_COMMAND,
    REMOTE_WORKSPACE_SETTINGS_COMMAND,
)
from httk.workflow.errors import SealedError
from httk.workflow.models import WorkspacePolicy
from httk.workflow.projects import initialize_project
from httk.workflow.registry import WorkspaceBinding, create_workspace
from httk.workflow.workflow_cli import _common, _workspace, command
from httk.workflow.workspace import Workspace


def test_configuration_round_trip_and_invalid_batch_preserves_document(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "configure", "ws", "--set", "app.command=run", "--set", "app.count=2"], context) == 0
    capsys.readouterr()
    workspace = Workspace(tmp_path / "ws")
    path = workspace.control / "format.json"
    before = path.read_bytes()
    assert command(["workspace", "configure", "ws", "--unset", "app.count", "--set", "bad key=2"], context) == 1
    assert path.read_bytes() == before
    capsys.readouterr()
    assert (
        command(["workspace", "configure", "ws", "--unset", "app.command", "--set", "app.count=3", "--json"], context)
        == 0
    )
    assert json.loads(capsys.readouterr().out) == [{"app.count": 3}]
    assert command(["workspace", "show", "ws", "--json"], context) == 0
    assert json.loads(capsys.readouterr().out) == [{"app.count": 3}]
    assert command(["workspace", "configure", "ws", "--unset", "app.count"], context) == 0
    document = json.loads(path.read_bytes())
    assert document["settings"] == {}
    assert "policy" in document and "workflow_preludes" in document


def test_policy_unset_restores_defaults_and_rejects_unknown_keys(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    workspace = Workspace(tmp_path / "ws")
    workspace.set_policy({"visibility_deadline_seconds": 72, "retention": {"owner_tombstone_days": 7, "trash_days": 9}})
    for key in ("visibility_deadline_seconds", "retention.owner_tombstone_days"):
        assert command(["workspace", "policy", "unset", "ws", "--key", key], context) == 0
    policy = Workspace(workspace.root).policy
    assert policy.visibility_deadline_seconds == WorkspacePolicy().visibility_deadline_seconds
    assert policy.retention.owner_tombstone_days == WorkspacePolicy().retention.owner_tombstone_days
    assert policy.retention.trash_days == 9
    for key in ("retention.typo", "typo"):
        before = (workspace.control / "format.json").read_bytes()
        assert command(["workspace", "policy", "unset", "ws", "--key", key], context) == 1
        assert (workspace.control / "format.json").read_bytes() == before
    assert command(["workspace", "policy", "unset", "ws", "--key", "retention"], context) == 0
    assert Workspace(workspace.root).policy.retention == WorkspacePolicy().retention


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


def test_remote_configuration_is_one_forwarded_command(tmp_path: Path, monkeypatch, capsys) -> None:
    project, calls = _far_workspace(tmp_path, monkeypatch, {"app.old": 1, "app.gone": 2})
    argv = ["workspace", "configure", "cluster:far", "--set", "a=1", "--set", "b=x", "--unset", "app.old"]
    assert command([*argv, "--unset", "app.gone", "--json"], CLIContext("httk", project)) == 0
    assert json.loads(capsys.readouterr().out) == [{"a": 1, "b": "x"}]
    assert calls == [
        [*REMOTE_WORKSPACE_CONFIGURE_COMMAND, "--json"]
        + ["--set", "a=1", "--set", "b=x", "--unset", "app.old", "--unset", "app.gone", "far"]
    ]
    calls.clear()
    # Local validation refuses before anything is sent.
    bad = ["workspace", "configure", "cluster:far", "--unset", "app.old", "--set", "bad key=new"]
    assert command(bad, CLIContext("httk", project)) == 1
    assert not calls


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


def test_settings_unset_removes_every_key_in_one_write(tmp_path: Path, monkeypatch, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    Workspace(tmp_path / "ws").configure_settings({"a": 1, "b": 2, "c": 3})
    writes = []
    original = Workspace.configure_settings

    def spy(self, changes, **kwargs):
        writes.append((dict(changes), tuple(kwargs.get("unset", ()))))
        return original(self, changes, **kwargs)

    monkeypatch.setattr(Workspace, "configure_settings", spy)
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "settings", "unset", "--key", "a", "--key", "b", "ws"], context) == 0
    assert json.loads(capsys.readouterr().out) == ["a", "b"]
    assert writes == [({}, ("a", "b"))]
    assert Workspace(tmp_path / "ws").settings == {"c": 3}
    # A key that is not set refuses the whole removal.
    assert command(["workspace", "settings", "unset", "--key", "c", "--key", "a", "ws"], context) == 1
    assert Workspace(tmp_path / "ws").settings == {"c": 3}


def test_remote_settings_unset_forwards_every_key_in_one_call(tmp_path: Path, monkeypatch, capsys) -> None:
    project, calls = _far_workspace(tmp_path, monkeypatch, {"a": 1, "b": 2, "c": 3})
    argv = ["workspace", "settings", "unset", "--key", "a", "--key", "b", "cluster:far"]
    assert command(argv, CLIContext("httk", project)) == 0
    assert json.loads(capsys.readouterr().out) == ["a", "b"]
    assert calls == [[*REMOTE_WORKSPACE_SETTINGS_COMMAND, "unset", "--key", "a", "--key", "b", "far"]]
    assert Workspace(tmp_path / "far").settings == {"c": 3}


def test_remote_settings_unset_detects_an_older_remote(tmp_path: Path, monkeypatch, capsys) -> None:
    project, _calls = _far_workspace(tmp_path, monkeypatch, {"a": 1, "b": 2})
    # An older far side keeps only the last --key and prints nothing.
    monkeypatch.setattr(_common, "remote_workspace_output", lambda *_args, **_kwargs: (0, "", ""))
    context = CLIContext("httk", project)
    assert command(["workspace", "settings", "unset", "--key", "a", "--key", "b", "cluster:far"], context) == 1
    assert "predates repeatable --key" in capsys.readouterr().err
    assert command(["workspace", "settings", "unset", "--key", "a", "cluster:far"], context) == 0
    assert json.loads(capsys.readouterr().out) == ["a"]


def test_remote_policy_verbs_are_one_forwarded_command_each(tmp_path: Path, monkeypatch, capsys) -> None:
    project, calls = _far_workspace(tmp_path, monkeypatch, {})
    context = CLIContext("httk", project)
    argv = ["workspace", "policy", "set", "--key", "visibility_deadline_seconds", "--value", "72"]
    assert command([*argv, "--key", "retention.trash_days", "--value", "9", "cluster:far"], context) == 0
    policy = Workspace(tmp_path / "far").policy
    assert policy.visibility_deadline_seconds == 72 and policy.retention.trash_days == 9
    capsys.readouterr()
    assert command(["workspace", "policy", "show", "--json", "cluster:far"], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["visibility_deadline_seconds"] == 72
    assert (
        command(["workspace", "policy", "unset", "--key", "visibility_deadline_seconds", "cluster:far"], context) == 0
    )
    default = WorkspacePolicy().visibility_deadline_seconds
    assert Workspace(tmp_path / "far").policy.visibility_deadline_seconds == default
    assert calls == [
        [*REMOTE_WORKSPACE_POLICY_COMMAND, "set"]
        + ["--key", "visibility_deadline_seconds", "--value", "72", "--key", "retention.trash_days", "--value", "9"]
        + ["far"],
        [*REMOTE_WORKSPACE_POLICY_COMMAND, "show", "--json", "far"],
        [*REMOTE_WORKSPACE_POLICY_COMMAND, "unset", "--key", "visibility_deadline_seconds", "far"],
    ]


def test_settings_set_applies_every_pair_in_one_write(tmp_path: Path, monkeypatch, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    writes = []
    original = Workspace.configure_settings

    def spy(self, changes, **kwargs):
        writes.append(dict(changes))
        return original(self, changes, **kwargs)

    monkeypatch.setattr(Workspace, "configure_settings", spy)
    argv = ["workspace", "settings", "set", "--key", "a", "--value", "1", "--key", "b", "--value", "2", "ws"]
    assert command(argv, context) == 0
    assert capsys.readouterr().out == '{"a": 1, "b": 2}\n'
    assert writes == [{"a": 1, "b": 2}]
    assert Workspace(tmp_path / "ws").settings == {"a": 1, "b": 2}
    # Any invalid pair applies nothing; unequal counts are refused naming both.
    path = Workspace(tmp_path / "ws").control / "format.json"
    before = path.read_bytes()
    assert (
        command(
            ["workspace", "settings", "set", "--key", "c", "--value", "3", "--key", "bad key", "--value", "4", "ws"],
            context,
        )
        == 1
    )
    assert command(["workspace", "settings", "set", "--key", "c", "--key", "d", "--value", "3", "ws"], context) == 2
    assert "got 2 --key and 1 --value" in capsys.readouterr().err
    assert path.read_bytes() == before


def test_remote_settings_set_forwards_every_pair_in_one_call(tmp_path: Path, monkeypatch, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="multi-pair")
    fake_remote(project, template="local", name="cluster")
    create_workspace("far", tmp_path / "far")
    calls = []
    original = _common.remote_workspace_output

    def spy(binding, context, argv, **kwargs):
        calls.append(list(argv))
        return original(binding, context, argv, **kwargs)

    monkeypatch.setattr(_common, "remote_workspace_output", spy)
    argv = ["workspace", "settings", "set", "--key", "a", "--value", "1", "--key", "b", "--value", "x", "cluster:far"]
    assert command(argv, CLIContext("httk", project)) == 0
    assert json.loads(capsys.readouterr().out) == {"a": 1, "b": "x"}
    assert calls == [
        [*REMOTE_WORKSPACE_SETTINGS_COMMAND, "set", "--key", "a", "--value", "1", "--key", "b", "--value", "x", "far"]
    ]
    assert Workspace(tmp_path / "far").settings == {"a": 1, "b": "x"}


def _fake_remote_reply(monkeypatch, reply: tuple[int, str, str]) -> None:
    binding = WorkspaceBinding("remote:ws", "remote", None)
    monkeypatch.setattr(_workspace, "_resolve_binding", lambda *_args: (binding, None))
    monkeypatch.setattr(_common, "remote_workspace_output", lambda *_args, **_kwargs: reply)


_TWO_PAIRS = ["workspace", "settings", "set", "--key", "a", "--value", "1", "--key", "b", "--value", "x", "remote:ws"]


@pytest.mark.parametrize("reply", ['"x"', '{"b": "x"}', "[]"])
def test_remote_settings_set_refuses_an_answer_missing_requested_keys(
    tmp_path: Path, monkeypatch, capsys, reply: str
) -> None:
    _fake_remote_reply(monkeypatch, (0, reply, ""))
    assert command(_TWO_PAIRS, CLIContext("httk", tmp_path)) == 1
    assert "the remote httk-workflow predates repeatable --key; set one pair per call or upgrade it" in (
        capsys.readouterr().err
    )


def test_remote_settings_set_reports_a_reply_that_is_not_json(tmp_path: Path, monkeypatch, capsys) -> None:
    _fake_remote_reply(monkeypatch, (0, "Traceback: boom", ""))
    assert command(_TWO_PAIRS, CLIContext("httk", tmp_path)) == 1
    err = capsys.readouterr().err
    assert "did not answer settings set with JSON: 'Traceback: boom'" in err and "predates" not in err


@pytest.mark.parametrize(("reply", "printed"), [('"x"', {"a": "x"}), ('{"a": "x"}', {"a": "x"})])
def test_remote_single_pair_settings_set_accepts_a_legacy_bare_value(
    tmp_path: Path, monkeypatch, capsys, reply: str, printed: dict[str, object]
) -> None:
    _fake_remote_reply(monkeypatch, (0, reply, ""))
    assert (
        command(
            ["workspace", "settings", "set", "--key", "a", "--value", "x", "remote:ws"], CLIContext("httk", tmp_path)
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == printed


def test_remote_settings_set_failure_is_relayed_with_a_trailing_newline(tmp_path: Path, monkeypatch, capsys) -> None:
    _fake_remote_reply(monkeypatch, (3, "partial", "remote error\n"))
    assert command(_TWO_PAIRS, CLIContext("httk", tmp_path)) == 1  # the workspace batch folds failures to 1
    captured = capsys.readouterr()
    assert captured.out == "partial\n" and captured.err == "remote error\n"


def test_policy_set_applies_every_pair_in_one_write(tmp_path: Path, monkeypatch, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    workspace = Workspace(tmp_path / "ws")
    workspace.set_policy({"retention": {"owner_tombstone_days": 7}})
    writes = []
    original = Workspace.set_policy

    def spy(self, changes):
        writes.append(dict(changes))
        return original(self, changes)

    monkeypatch.setattr(Workspace, "set_policy", spy)
    argv = ["workspace", "policy", "set", "ws", "--json"]
    pairs = {"visibility_deadline_seconds": "61", "retention.trash_days": "14", "retention.daemon_history_days": "keep"}
    for key, value in pairs.items():
        argv += ["--key", key, "--value", value]
    assert command(argv, context) == 0
    assert json.loads(capsys.readouterr().out)[0]["visibility_deadline_seconds"] == 61
    assert len(writes) == 1
    policy = Workspace(workspace.root).policy
    assert policy.visibility_deadline_seconds == 61
    assert policy.retention.trash_days == 14 and policy.retention.daemon_history_days is None
    assert policy.retention.owner_tombstone_days == 7
    # An invalid pair, whether it fails coercion or validation, applies nothing.
    before = (workspace.control / "format.json").read_bytes()
    valid = ["workspace", "policy", "set", "ws", "--key", "visibility_deadline_seconds", "--value", "5"]
    assert command([*valid, "--key", "retention.trash_days", "--value", "not-json"], context) == 1
    assert command([*valid, "--key", "retention.journal_days", "--value", "1"], context) == 1
    assert command([*valid, "--key", "retention.trash_days"], context) == 2
    assert (
        command(
            [*valid, "--key", "retention", "--value", "[]", "--key", "retention.trash_days", "--value", "1"], context
        )
        == 1
    )
    assert "got 2 --key and 1 --value" in capsys.readouterr().err
    assert (workspace.control / "format.json").read_bytes() == before


@pytest.mark.parametrize("member_first", [False, True])
def test_policy_set_merges_members_with_a_whole_retention_pair_in_order(
    tmp_path: Path, capsys, member_first: bool
) -> None:
    create_workspace("ws", tmp_path / "ws")
    workspace = Workspace(tmp_path / "ws")
    workspace.set_policy({"retention": {"owner_tombstone_days": 7, "trash_days": 9}})
    whole = ["--key", "retention", "--value", '{"trash_days": 3}']
    member = ["--key", "retention.daemon_history_days", "--value", "11"]
    pairs = [*member, *whole] if member_first else [*whole, *member]
    assert command(["workspace", "policy", "set", "ws", *pairs], CLIContext("httk", tmp_path)) == 0
    capsys.readouterr()
    retention = Workspace(workspace.root).policy.retention
    defaults = WorkspacePolicy().retention
    assert retention.trash_days == 3
    # A later whole-object pair replaces an earlier member merge; a later member merges into it.
    assert retention.daemon_history_days == (defaults.daemon_history_days if member_first else 11)
    assert retention.owner_tombstone_days == defaults.owner_tombstone_days
