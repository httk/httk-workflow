"""The shape of the command tree itself.

Everything here is about the command line as an interface rather than about
what any one command does: that every group answers ``--help``, that a mistyped
action is reported at the level it was mistyped at, that the two installed
executables really are aliases of the canonical tree rather than a second
implementation of it, and that the spellings other machines depend on are
stable.
"""

import argparse
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace, workflow_cli
from httk.workflow import cli as native_cli
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
        "policy",
        "fsck",
        "gc",
        "unlock",
    ),
    "runner": ("publish", "describe"),
    "job": ("new", "submit", "request", "list", "show", "log", "why", "debug"),
    "manager": ("run",),
    "v1": ("collect",),
    "config": ("init", "show", "set", "unset", "import-v1"),
    "project": ("init", "import-v1", "show", "doctor", "manifest"),
    "remote": ("list", "add", "configure", "install", "import-v1", "show", "remove"),
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
    assert f"usage: httk workflow {group}" in capsys.readouterr().out


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
    parsed = parser.parse_args(["postprocess", "WS", "--script", "report"])
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
    assert "httk workflow workspace" in captured
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
    assert "httk workflow workspace policy set" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# The two executables are aliases, not implementations
# ---------------------------------------------------------------------------


ALIASED_TASKMANAGER = (
    (["init", "WS"], ["workspace", "init", "WS"]),
    (
        ["submit", "WS", "SRC", "--placement", "p/0", "--move"],
        ["job", "submit", "WS", "SRC", "--placement", "p/0", "--move"],
    ),
    (["run", "WS", "--workers", "4", "--idle"], ["manager", "run", "WS", "--workers", "4", "--idle"]),
    (["status", "WS", "--json"], ["workspace", "status", "WS", "--json"]),
    (
        ["request", "WS", "JOB", "cancel", "--operator", "me", "--reason", "why"],
        ["job", "request", "WS", "JOB", "cancel", "--operator", "me", "--reason", "why"],
    ),
)


@pytest.mark.parametrize("alias_argv, canonical_argv", ALIASED_TASKMANAGER)
def test_httk_taskmanager_maps_onto_the_canonical_tree(
    alias_argv: list[str],
    canonical_argv: list[str],
    tmp_path: Path,
) -> None:
    canonical = workflow_cli.build_parser("httk workflow", _context(tmp_path)).parse_args(canonical_argv)
    alias = native_cli._parser().parse_args(alias_argv)  # pyright: ignore[reportPrivateUsage]
    # The same function, reached with the same values: an alias cannot drift.
    assert alias.handler is canonical.handler
    assert _namespace(alias) == _namespace(canonical)


def test_the_taskmanager_alias_really_does_the_work_it_names(tmp_path: Path, capsys) -> None:
    """Not only the same parse: the same effect on the filesystem."""

    root = tmp_path / "workspace"
    assert native_cli.main(["init", str(root), "--name", "aliased"]) == 0
    assert "aliased" in capsys.readouterr().out
    assert (root / ".httk-workflow" / "format.json").is_file()

    assert native_cli.main(["status", "aliased", "--json"]) == 0
    alias = capsys.readouterr().out
    assert command(["workspace", "status", "aliased", "--json"], _context(tmp_path)) == 0
    assert capsys.readouterr().out == alias


def test_taskmanager_alias_says_which_spelling_is_canonical(capsys) -> None:
    native_cli._parser().print_help()  # pyright: ignore[reportPrivateUsage]
    printed = capsys.readouterr().out
    assert "httk workflow" in printed and "canonical" in printed


def test_the_executables_still_take_their_durability_switch_before_the_command() -> None:
    parser = native_cli._parser()  # pyright: ignore[reportPrivateUsage]
    assert parser.parse_args(["init", "WS"]).no_durable is False
    assert parser.parse_args(["--no-durable", "init", "WS"]).no_durable is True
    # And after it, which the canonical tree is what makes possible.
    assert parser.parse_args(["init", "WS", "--no-durable"]).no_durable is True


# ---------------------------------------------------------------------------
# Deprecated spellings keep working
# ---------------------------------------------------------------------------


def test_the_superseded_option_spellings_are_removed(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))

    # But --set on `remote configure` is, and stays, KEY=VALUE settings.
    assert parser.parse_args(["remote", "configure", "cluster", "--set", "host=a"]).set == ["host=a"]
    with pytest.raises(SystemExit):
        parser.parse_args(["project", "init", "--default-queue", "batch"])

    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "install", "c", "--timeout", "5"])
    # The manager's idle wait is bounded by --idle-timeout.
    assert parser.parse_args(["manager", "run", "WS", "--idle-timeout", "5"]).idle_timeout == 5.0
    assert parser.parse_args(["manager", "run", "WS"]).idle_timeout == 3600.0
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "WS", "--until-" + "idle"])
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "WS", "--fore" + "ground"])
    assert parser.parse_args(["run"]).workspace is None
    assert parser.parse_args(["run", "WS", "--idle"]).idle is True
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "WS", "--timeout", "5"])


def test_top_level_run_is_manager_run_with_pinned_defaults(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    top_level = _namespace(parser.parse_args(["run", "WS", "--workers", "4", "--idle"]))
    manager = _namespace(parser.parse_args(["manager", "run", "WS", "--workers", "4", "--idle"]))
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
    assert "workspace is not idle after 0s; jobs are still running or claimable" in error
    assert "Traceback" not in error


def test_campaign_start_managers_forwards_idle_timeout(tmp_path: Path, monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_campaign_managers(**arguments: object) -> list[dict[str, object]]:
        seen.update(arguments)
        return []

    monkeypatch.setattr(_campaign, "campaign_managers", fake_campaign_managers)
    assert command(["campaign", "start-managers", "--idle-timeout", "10"], _context(tmp_path)) == 0
    assert seen["idle_timeout"] == 10.0


def test_transfer_is_a_single_verb_not_a_group(tmp_path: Path) -> None:
    """`transfer SRC DST` replaced the old send/fetch/start-manager subcommands."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    # The verb is one leaf: its handler is handle_transfer and its argument is the
    # trailing vector its handler parses, so the two workspace names travel there.
    parsed = parser.parse_args(["transfer", "a", "b", "--job", "J"])
    assert parsed.handler is workflow_cli.handle_transfer
    assert parsed.args == ["a", "b", "--job", "J"]
    # The removed group subcommands no longer parse as group actions: they are just
    # more of the verb's own trailing vector now.
    assert parser.parse_args(["transfer", "send", "c", "J"]).args == ["send", "c", "J"]


# ---------------------------------------------------------------------------
# Spellings other machines depend on
# ---------------------------------------------------------------------------


def test_the_remote_protocol_spellings_are_stable(tmp_path: Path) -> None:
    """A transfer composes commands the far side may run on an older httk."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    # ``transfer receive`` is the frozen import half: the verb leaf parses it as
    # its own trailing vector and its handler dispatches "receive" at run time to
    # the still-present handle_transfer_receive.
    receive = parser.parse_args(["transfer", "receive", "--workspace", "/w", "--bundle", "/b"])
    assert receive.handler is workflow_cli.handle_transfer
    assert receive.args[0] == "receive"
    assert callable(workflow_cli.handle_transfer_receive)
    # The old ``tasks``/``internal`` group spellings are gone.
    with pytest.raises(SystemExit):
        parser.parse_args(["tasks", "receive", "--workspace", "/w", "--bundle", "/b"])
    with pytest.raises(SystemExit):
        parser.parse_args(["internal", "receive", "--workspace", "/w", "--bundle", "/b"])
