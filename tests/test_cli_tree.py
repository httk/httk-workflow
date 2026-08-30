"""The shape of the command tree itself.

Everything here is about the command line as an interface rather than about
what any one command does: that every group answers ``--help``, that a mistyped
action is reported at the level it was mistyped at, and that the spellings
other machines depend on are stable.
"""

import argparse
from pathlib import Path

import pytest
from httk.core.cli import CLIContext, main

from httk.workflow import Workspace, workflow_cli
from httk.workflow.projects import initialize_project
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload
from httk.workflow.workflow_cli import _campaign, command

#: Every group of the canonical tree, with the subcommands its help must name.
#: ``run`` and ``transfer`` are deliberately absent: they are single verbs
#: rather than groups, so they are checked on their own below.
GROUPS: dict[str, tuple[str, ...]] = {
    "workspace": (
        "init",
        "status",
        "managers",
        "list",
        "default",
        "move",
        "forget",
        "delete",
        "settings",
        "workflow-prelude",
        "policy",
        "fsck",
        "gc",
        "unlock",
    ),
    "runner": ("publish", "describe"),
    "job": ("new", "submit", "request", "delete", "list", "show", "log", "why", "debug"),
    "manager": ("run",),
    "v1": ("collect",),
    "config": ("init", "show", "set", "unset", "import-v1"),
    "project": ("init", "import-v1", "show", "doctor", "manifest"),
    "remote": ("list", "add", "configure", "check", "import-v1", "show", "remove"),
    "campaign": ("init", "show", "submit", "collect", "start-managers"),
}

#: Superseded group spellings that were removed: ``tasks`` was the transfer
#: group and ``computer`` the remote group, both before the renames; ``internal``
#: was the hidden home of ``receive``. None of them parses any more.
REMOVED_GROUPS = ("tasks", "computer", "internal", "import")


def _context(tmp_path: Path) -> CLIContext:
    return CLIContext("httk", tmp_path)


def _namespace(parsed: argparse.Namespace) -> dict[str, object]:
    """The parsed values, without the plumbing that names the parser itself."""

    return {
        key: value
        for key, value in vars(parsed).items()
        if key not in {"handler", "help_parser", "command", "durable", "no_durable"}
    }


# ---------------------------------------------------------------------------
# Help, at every level
# ---------------------------------------------------------------------------


def test_the_tree_itself_answers_help_and_names_every_group(tmp_path: Path, capsys) -> None:
    assert command(["--help"], _context(tmp_path)) == 0
    printed = capsys.readouterr().out
    for group in GROUPS:
        assert group in printed
    # The single-verb commands are leaves, but the tree still names them.
    assert "run" in printed and "transfer" in printed
    # A bare invocation is somebody exploring, not somebody making a mistake.
    assert command([], _context(tmp_path)) == 0
    assert "usage:" in capsys.readouterr().out


@pytest.mark.parametrize("group", sorted(GROUPS))
def test_every_group_help_exits_zero_and_names_its_subcommands(group: str, tmp_path: Path, capsys) -> None:
    assert command([group, "--help"], _context(tmp_path)) == 0
    printed = capsys.readouterr().out
    assert printed.startswith("usage:")
    for action in GROUPS[group]:
        assert action in printed


@pytest.mark.parametrize("group", sorted(GROUPS))
def test_every_group_prints_its_own_help_when_given_no_action(group: str, tmp_path: Path, capsys) -> None:
    assert command([group], _context(tmp_path)) == 0
    expected = f"usage: httk {group}" if group in {"workspace", "job"} else f"usage: httk workflow {group}"
    assert expected in capsys.readouterr().out


def test_every_leaf_documents_every_argument_it_takes(tmp_path: Path) -> None:
    """No argument of any command is left without a help string."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    undocumented: list[str] = []

    def walk(current: argparse.ArgumentParser) -> None:
        for action in current._actions:  # pyright: ignore[reportPrivateUsage]
            if isinstance(action, argparse._SubParsersAction):  # pyright: ignore[reportPrivateUsage]
                for name, child in action.choices.items():
                    assert child.prog.endswith(name)
                    walk(child)
            elif action.help is None:
                undocumented.append(f"{current.prog}: {action.dest}")

    walk(parser)
    assert undocumented == []


def test_the_removed_spellings_are_absent_from_the_help(tmp_path: Path, capsys) -> None:
    assert command(["--help"], _context(tmp_path)) == 0
    printed = capsys.readouterr().out
    for group in REMOVED_GROUPS:
        assert group not in printed

    # transfer is a verb, not a group: its help shows the SRC/DST usage rather
    # than a subcommand listing (receive/offer/retire are hidden protocol).
    assert command(["transfer", "--help"], _context(tmp_path)) == 0
    assert "SRC DST" in capsys.readouterr().out


def test_postprocess_is_a_single_verb(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    parsed = parser.parse_args(["postprocess", "--workspace", "WS", "--script", "report"])
    assert parsed.handler is workflow_cli.handle_postprocess
    assert parsed.workspace == "WS"
    assert parsed.script == "report"


# ---------------------------------------------------------------------------
# Errors, at the level they happened at
# ---------------------------------------------------------------------------


def test_an_unknown_action_under_a_known_group_names_that_group(tmp_path: Path, capsys) -> None:
    """The old dispatcher fell through to an *unknown group* error instead."""

    assert command(["workspace", "frobnicate"], _context(tmp_path)) == 2
    captured = capsys.readouterr().err
    assert "httk workspace" in captured
    assert "invalid choice: 'frobnicate'" in captured
    # The group's own actions are what it offers instead, not the tree's groups.
    assert "'fsck'" in captured and "'remote'" not in captured


def test_an_unknown_action_under_a_nested_group_names_that_nested_group(tmp_path: Path, capsys) -> None:
    assert command(["project", "manifest", "sign"], _context(tmp_path)) == 2
    captured = capsys.readouterr().err
    assert "httk workflow project manifest" in captured and "invalid choice: 'sign'" in captured


def test_an_unknown_group_names_the_tree(tmp_path: Path, capsys) -> None:
    assert command(["frobnicate"], _context(tmp_path)) == 2
    captured = capsys.readouterr().err
    assert "usage: httk workflow" in captured and "invalid choice: 'frobnicate'" in captured


def test_a_missing_required_argument_names_the_leaf(tmp_path: Path, capsys) -> None:
    assert command(["workspace", "policy", "set"], _context(tmp_path)) == 2
    assert "httk workspace policy set" in capsys.readouterr().err


def test_workflow_rejects_the_standalone_groups(tmp_path: Path, capsys) -> None:
    assert workflow_cli.workflow_command(["workspace", "status"], _context(tmp_path)) == 2
    captured = capsys.readouterr().err
    assert "httk workflow" in captured
    assert "invalid choice: 'workspace'" in captured


def test_core_dispatches_the_standalone_groups(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["workspace", "--help"]) == 0
    assert capsys.readouterr().out.startswith("usage: httk workspace")
    assert main(["job", "list"]) == 0


# ---------------------------------------------------------------------------
# Removed spellings and canonical scope options
# ---------------------------------------------------------------------------


def test_the_superseded_option_spellings_are_removed(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))

    # But --set on `remote configure` is, and stays, KEY=VALUE settings.
    assert parser.parse_args(["remote", "configure", "--set", "host=a", "cluster"]).set == ["host=a"]
    with pytest.raises(SystemExit):
        parser.parse_args(["project", "init", "--default-queue", "batch"])

    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "check", "--timeout", "5", "c"])
    # The manager's idle wait is bounded by --idle-timeout.
    assert parser.parse_args(["manager", "run", "--workspace", "WS", "--idle-timeout", "5"]).idle_timeout == 5.0
    assert parser.parse_args(["manager", "run", "--workspace", "WS"]).idle_timeout == 3600.0
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "--workspace", "WS", "--until-" + "idle"])
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "--workspace", "WS", "--fore" + "ground"])
    assert parser.parse_args(["run"]).workspace is None
    assert parser.parse_args(["run", "--workspace", "WS", "--idle"]).idle is True
    assert parser.parse_args(["run", "--launcher", "gpu"]).launcher == "gpu"
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--inline", "--launcher", "gpu"])
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "--workspace", "WS", "--timeout", "5"])
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "WS"])


def test_top_level_run_is_manager_run_with_pinned_defaults(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    top_level = _namespace(parser.parse_args(["run", "--workspace", "WS", "--workers", "4", "--idle"]))
    manager = _namespace(parser.parse_args(["manager", "run", "--workspace", "WS", "--workers", "4", "--idle"]))
    assert top_level == manager


def test_top_level_run_defaults_to_until_idle_for_the_project_workspace(tmp_path: Path) -> None:
    initialize_project(tmp_path, name="run-default")
    assert command(["run"], _context(tmp_path)) == 0


def test_top_level_run_reports_an_idle_timeout_without_a_traceback(tmp_path: Path, capsys) -> None:
    initialize_project(tmp_path, name="run-timeout")
    Workspace.initialize(tmp_path)
    source = tmp_path / "source" / "files"
    source.mkdir(parents=True)
    (source / "runner").write_text("#!/bin/sh\n", encoding="utf-8")
    payload = tmp_path / "source"
    prepare_job_payload(
        payload,
        JobSpec(
            name="unserved",
            workflow="tests.cli_tree",
            runner_path="files/runner",
            initial_step="only",
            claim_pool="unserved",
        ),
    )
    Workspace(tmp_path).submit(payload, "project/unserved")

    assert command(["run", "--idle-timeout", "0.05"], _context(tmp_path)) == 2
    error = capsys.readouterr().err
    assert "workspace is not idle after 0s" in error
    assert "does not serve pool(s) unserved" in error
    assert "Traceback" not in error


def test_campaign_start_managers_forwards_idle_timeout(tmp_path: Path, monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_campaign_managers(**arguments: object) -> list[dict[str, object]]:
        seen.update(arguments)
        return []

    monkeypatch.setattr(_campaign, "campaign_managers", fake_campaign_managers)
    assert (
        command(
            [
                "campaign",
                "start-managers",
                "--idle-timeout",
                "10",
                "--worker-resource",
                "procs",
                "4",
            ],
            _context(tmp_path),
        )
        == 0
    )
    assert seen["idle_timeout"] == 10.0
    assert seen["resources"] == {"procs": 4}
    assert seen["count"] is None

    assert command(["campaign", "start-managers", "--count", "3"], _context(tmp_path)) == 0
    assert seen["count"] == 3


def test_campaign_start_managers_returns_nonzero_for_a_failed_partition(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        _campaign,
        "campaign_managers",
        lambda **_arguments: [{"partition": "north", "result": 2}],
    )
    assert command(["campaign", "start-managers"], _context(tmp_path)) == 1


def test_transfer_is_a_single_verb_not_a_group(tmp_path: Path) -> None:
    """`transfer SRC DST` replaced the old send/fetch manager-submission subcommands."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    parsed = parser.parse_args(["transfer", "--job", "J", "a", "b"])
    assert parsed.handler is workflow_cli.handle_transfer
    assert (parsed.source, parsed.destination, parsed.jobs) == ("a", "b", ["J"])
    with pytest.raises(SystemExit):
        parser.parse_args(["transfer", "send", "c", "J"])


# ---------------------------------------------------------------------------
# Spellings other machines depend on
# ---------------------------------------------------------------------------


def test_the_remote_protocol_spellings_are_stable(tmp_path: Path) -> None:
    """The private cross-machine protocol retains its declared command vectors."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    assert callable(workflow_cli.handle_transfer_receive)
    with pytest.raises(SystemExit):
        parser.parse_args(["tasks", "receive", "--workspace", "/w", "--bundle", "/b"])
    with pytest.raises(SystemExit):
        parser.parse_args(["internal", "receive", "--workspace", "/w", "--bundle", "/b"])
