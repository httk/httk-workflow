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

import v3_helpers as v3
from httk.workflow import Workspace, adapters, workflow_cli
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _campaign, _transfer, command

#: Every group of the canonical tree, with the subcommands its help must name.
#: ``run``, ``monitor`` and ``postprocess`` are deliberately absent: they are single verbs rather than groups,
#: so they are checked on their own below. ``move`` is a leaf of the ``job`` group; ``push`` and ``pull`` are top-level verbs.
GROUPS: dict[str, tuple[str, ...]] = {
    "workspace": (
        "init",
        "status",
        "owners",
        "attest-dead",
        "list",
        "default",
        "register",
        "move",
        "forget",
        "delete",
        "show",
        "set",
        "unset",
        "check",
        "repair",
        "gc",
        "seal",
        "unseal",
        "verify",
    ),
    "job": (
        "new",
        "submit",
        "cancel",
        "pause",
        "resume",
        "restart",
        "set",
        "delete",
        "seal",
        "unseal",
        "detach",
        "eject",
        "adopt",
        "list",
        "show",
        "log",
        "why",
        "precheck",
        "debug",
        "verify",
        "move",
    ),
    "v1": ("collect",),
    "config": ("show", "set", "unset", "import-v1"),
    "remote": ("list", "add", "set", "unset", "check", "import-v1", "show", "remove"),
    "launcher": ("list", "add", "set", "unset", "show", "check", "remove"),
    "campaign": ("init", "show", "set", "unset", "submit", "collect", "start-managers"),
}

#: Superseded group spellings that were removed: ``tasks`` was the transfer
#: group and ``computer`` the remote group, both before the renames; ``internal``
#: was the hidden home of ``receive``. None of them parses any more.
REMOVED_GROUPS = ("tasks", "computer", "internal", "import", "runner")


def _init_workspace(project: Path) -> Workspace:
    """Create ``project/workspace``, register it as ``default``, and record it as the project default."""

    from httk.workflow.projects import write_project_section
    from httk.workflow.registry import create_workspace

    create_workspace("default", project / "workspace")
    write_project_section(project, "workspace", {"default": "default"})
    return Workspace(project / "workspace")


def _context(tmp_path: Path) -> CLIContext:
    return CLIContext("httk", tmp_path)


def _namespace(parsed: argparse.Namespace) -> dict[str, object]:
    """The parsed values, without the plumbing that names the parser itself."""

    return {
        key: value
        for key, value in vars(parsed).items()
        if key not in {"handler", "help_parser", "command", "no_durable"}
    }


# ---------------------------------------------------------------------------
# Help, at every level
# ---------------------------------------------------------------------------


def test_the_tree_itself_answers_help_and_names_every_group(tmp_path: Path, capsys) -> None:
    assert command(["--help"], _context(tmp_path)) == 0
    printed = capsys.readouterr().out
    for group in GROUPS:
        assert group in printed
    # The single-verb ``run`` command is a leaf, but the tree still names it.
    # ``move`` is a leaf of ``job`` and is checked in that group's help.
    assert "run" in printed
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
    expected = f"usage: httk {group}"
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

    # move is a verb, not a group: its help shows the SRC/DST usage rather
    # than a subcommand listing.
    assert command(["job", "move", "--help"], _context(tmp_path)) == 0
    assert "SRC DST" in capsys.readouterr().out
    # The verbs this tree reorganized are gone from their old places.
    for removed in (
        ["job", "request", "pause", "--reason", "r", "J"],
        ["job", "transfer", "a", "b"],
        ["job", "push", "b"],
        ["job", "pull", "a"],
        ["precheck"],
        ["seal", "verify"],
        ["transfer", "status"],
    ):
        assert command(removed, _context(tmp_path)) == 2
        assert "invalid choice" in capsys.readouterr().err


def test_postprocess_is_a_single_verb(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    parsed = parser.parse_args(["postprocess", "report", "J1", "J2", "--workspace", "WS"])
    assert parsed.handler is workflow_cli.handle_postprocess
    assert (parsed.workspace, parsed.script, parsed.jobs) == ("WS", "report", ["J1", "J2"])
    with pytest.raises(SystemExit):
        parser.parse_args(["postprocess", "--script", "report"])
    assert parser.parse_args(["postprocess", "report"]).jobs == []


def test_postprocess_requires_only_its_script(tmp_path: Path, capsys) -> None:
    assert workflow_cli.postprocess_command([], _context(tmp_path)) == 2
    error = capsys.readouterr().err
    assert "the following arguments are required: SCRIPT" in error and "JOB" not in error.splitlines()[-1]


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
    # (argparse dropped the quotes around each choice in 3.13+, so match the bare name.)
    assert "repair" in captured and "remote" not in captured


def test_an_unknown_action_under_a_nested_group_names_that_nested_group(capsys) -> None:
    # `workspace exchange` is a nested group of a tree httk-workflow mounts on the core
    # `httk` command; a mistyped action still names the nested group it is under.
    assert main(["workspace", "exchange", "frobnicate"]) == 2
    captured = capsys.readouterr().err
    assert "httk workspace exchange" in captured and "invalid choice: 'frobnicate'" in captured


def test_an_unknown_group_names_the_tree(tmp_path: Path, capsys) -> None:
    assert command(["frobnicate"], _context(tmp_path)) == 2
    captured = capsys.readouterr().err
    assert "usage: httk" in captured and "invalid choice: 'frobnicate'" in captured


def test_a_missing_required_argument_names_the_leaf(tmp_path: Path, capsys) -> None:
    assert command(["workspace", "set"], _context(tmp_path)) == 2
    assert "httk workspace set" in capsys.readouterr().err


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


def test_workflow_project_verbs_are_mounted_on_the_core_project_command(capsys) -> None:
    # repair/manifest/seal/unseal live under the core `httk project` command,
    # mounted there by httk-workflow via register_cli_extension.
    assert main(["project", "--help"]) == 0
    printed = capsys.readouterr().out
    for verb in ("repair", "manifest", "seal", "unseal"):
        assert verb in printed


# ---------------------------------------------------------------------------
# Removed spellings and canonical scope options
# ---------------------------------------------------------------------------


def test_the_job_options_of_the_runner_store_and_per_job_modes_are_removed(tmp_path: Path, capsys) -> None:
    """Workflows are installed (no runner publication), and a job has no workdir or data mode."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    new = ["job", "new", "--from-command", "true"]
    for removed in (
        [*new, "--publish", "workspace"],
        [*new, "--workdir-mode", "persistent"],
        [*new, "--data-mode", "none"],
        [*new, "--runner-name", "x"],
        [*new, "--workflow-id", "x"],
        ["job", "submit", "--placement", "p", "payload"],
        ["job", "seal", "--keys", "k", "job"],
        ["job", "eject", "--resume"],
        ["job", "confirm-launches-ended", "job"],
        ["runner", "publish", "file"],
        ["job", "precheck", "--runner-search-path", "d"],
        ["build", "--store", "s"],
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(removed)
    capsys.readouterr()
    assert parser.parse_args([*new, "--install"]).install is True
    assert parser.parse_args(["install", "--workspace", "cluster:ws", "--no-calls", "pkg"]).no_calls is True


def test_the_superseded_option_spellings_are_removed(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))

    # Settings are positional KEY=VALUE pairs of the set verbs; configure and --template are gone.
    assert parser.parse_args(["remote", "set", "cluster", "host=a"]).pairs == ["host=a"]
    for removed in (
        ["remote", "configure", "--set", "host=a", "cluster"],
        ["remote", "add", "--template", "ssh", "cluster"],
        ["remote", "check", "cluster", "--set", "host=a"],
        ["launcher", "configure", "--set", "a=b", "cluster"],
        ["launcher", "add", "--template", "slurm", "cluster"],
        ["launcher", "set", "cluster", "--add-path", "confine.devices=/dev/a"],
        ["config", "configure", "--set", "machine_names=a"],
        ["config", "set", "machine_names", "a"],
        ["campaign", "configure", "--set", "assignment=hash"],
        ["workspace", "configure", "--set", "a=1"],
        ["workspace", "settings", "set", "--key", "a", "--value", "1"],
        ["workspace", "policy", "show"],
        ["workspace", "workflow-prelude", "show"],
        ["workspace", "show", "--key", "a"],
        ["workspace", "init", "--setting", "a=1", "path"],
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(removed)

    with pytest.raises(SystemExit):
        parser.parse_args(["remote", "check", "--timeout", "5", "c"])
    # The manager's idle wait is bounded by --idle-timeout.
    assert parser.parse_args(["run", "--workspace", "WS", "--idle-timeout", "5"]).idle_timeout == 5.0
    assert parser.parse_args(["run", "--workspace", "WS"]).idle_timeout == 3600.0
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--workspace", "WS", "--until-" + "idle"])
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--workspace", "WS", "--fore" + "ground"])
    assert parser.parse_args(["run"]).workspace is None
    assert parser.parse_args(["run", "--workspace", "WS", "--keep-running"]).idle is True
    assert parser.parse_args(["run", "--launcher", "gpu"]).launcher == "gpu"
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--inline", "--launcher", "gpu"])
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--workspace", "WS", "--timeout", "5"])
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "WS"])


def test_run_monitor_and_postprocess_dispatch_at_the_top_level(tmp_path: Path, monkeypatch, capsys) -> None:
    from httk.workflow.workflow_cli import _manager, _monitor, _postprocess

    seen: list[tuple[str, argparse.Namespace]] = []

    def recorder(name: str):
        def handle(arguments: argparse.Namespace, _context: CLIContext) -> int:
            seen.append((name, arguments))
            return 0

        return handle

    for module, name in (
        (_manager, "handle_manager_run"),
        (_monitor, "handle_monitor"),
        (_postprocess, "handle_postprocess"),
    ):
        monkeypatch.setattr(module, name, recorder(name))
    assert workflow_cli.run_command(["-w", "WS", "--keep-running"], _context(tmp_path)) == 0
    assert workflow_cli.monitor_command(["--refresh", "2"], _context(tmp_path)) == 0
    # A job after an option is collected too (argparse before 3.13 strands it).
    assert workflow_cli.postprocess_command(["report", "J", "-w", "WS", "K"], _context(tmp_path)) == 0
    (_, ran), (_, monitored), (_, postprocessed) = seen
    assert [name for name, _ in seen] == ["handle_manager_run", "handle_monitor", "handle_postprocess"]
    assert (ran.workspace, ran.idle, monitored.refresh) == ("WS", True, 2.0)
    assert (postprocessed.script, postprocessed.jobs, postprocessed.workspace) == ("report", ["J", "K"], "WS")
    # The old spellings are gone, with no alias left behind.
    for removed in (["run"], ["monitor"], ["postprocess", "--script", "report"], ["manager", "run"]):
        assert workflow_cli.workflow_command(removed, _context(tmp_path)) == 2
        assert "invalid choice" in capsys.readouterr().err


def test_spawned_and_remote_managers_run_httk_run(tmp_path: Path, monkeypatch) -> None:
    """Every manager another process starts parses as ``httk run`` on the far side."""

    from httk.workflow import _daemon_slurm
    from httk.workflow.workflow_cli import _manager

    assert adapters.REMOTE_MANAGER_COMMAND == ("httk", "run")
    parser = workflow_cli.build_parser("httk", _context(tmp_path))
    remote = _manager._remote_manager_argv(parser.parse_args(["run", "--workers", "2"]), "station")
    assert remote == ["httk", "run", "--workspace", "station", "--detach", "--workers", "2"]
    assert parser.parse_args(remote[1:]).handler is workflow_cli.handle_manager_run

    # Local children re-invoke this interpreter as `-m httk.core.cli run --by-path`.
    launched: list[list[str]] = []

    class _Child:
        def __init__(self, argv: list[str], **_options: object) -> None:
            launched.append(argv)

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(_manager.subprocess, "Popen", _Child)
    arguments = parser.parse_args(["run", "--count", "2"])
    assert _manager._run_local_manager_children(arguments, tmp_path, _context(tmp_path)) == 0
    assert [argv[1:5] for argv in launched] == [["-m", "httk.core.cli", "run", "--by-path"]] * 2
    assert parser.parse_args(launched[0][3:]).by_path is True

    # A daemon-started Slurm manager runs the same verb.
    policy = argparse.Namespace(python=Path("/usr/bin/python3"), workspace=Path("/data"))
    argv = _daemon_slurm.manager_argv(policy, "a" * 64)  # type: ignore[arg-type]
    assert argv[4:6] == ["run", "--by-path"] and parser.parse_args(argv[4:]).pool


def test_top_level_run_defaults_to_until_idle_for_the_project_workspace(tmp_path: Path) -> None:
    initialize_project(tmp_path, name="run-default")
    assert command(["run"], _context(tmp_path)) == 0


def test_top_level_run_reports_an_idle_timeout_without_a_traceback(tmp_path: Path, capsys) -> None:
    initialize_project(tmp_path, name="run-timeout")
    workspace = _init_workspace(tmp_path)
    v3.submit(workspace, v3.install(workspace, tmp_path / "package"), {"start": "succeed"}, pool="unserved")

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
    """`job move SRC DST` replaced the old send/fetch manager-submission subcommands."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    parsed = parser.parse_args(["job", "move", "--job", "J", "--tree", "a", "b"])
    assert parsed.handler is workflow_cli.handle_transfer
    assert (parsed.source, parsed.destination, parsed.jobs, parsed.tree) == ("a", "b", ["J"], True)
    parsed = parser.parse_args(["job", "move", "--resume"])
    assert (parsed.resume, parsed.source, parsed.destination) == (True, None, None)
    parsed = parser.parse_args(["job", "move", "--release", "T", "--release", "U", "a"])
    assert (parsed.release, parsed.source) == (["T", "U"], "a")
    with pytest.raises(SystemExit):
        parser.parse_args(["job", "move", "send", "c", "J"])
    parsed = parser.parse_args(["job", "eject", "--hold", "J"])
    assert (parsed.hold, parsed.destination) == (True, None)
    # Selection by state, as `job list --state`, optionally under a placement prefix; owned jobs cannot be held.
    argv = ["job", "move", "--state", "succeeded", "--state", "failed", "--placement", "c1", "a", "b"]
    parsed = parser.parse_args(argv)
    assert (parsed.state, parsed.placement, parsed.jobs) == (["succeeded", "failed"], "c1", [])
    with pytest.raises(SystemExit):
        parser.parse_args(["job", "move", "--state", "owned", "a", "b"])
    # Selection by step repeats; a value that is no step name is refused while parsing.
    parsed = parser.parse_args(["job", "move", "--step", "relax", "--step", "scf", "a", "b"])
    assert parsed.step == ["relax", "scf"]
    for invalid in ("a/b", ""):
        with pytest.raises(SystemExit):
            parser.parse_args(["job", "move", "--step", invalid, "a", "b"])


# ---------------------------------------------------------------------------
# Spellings other machines depend on
# ---------------------------------------------------------------------------


def test_the_remote_protocol_spellings_are_stable(tmp_path: Path) -> None:
    """Transfers reach other machines through the ordinary verbs; the legacy protocol spellings are gone."""

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    assert adapters.REMOTE_JOB_EJECT_COMMAND == ("httk", "job", "eject")
    assert adapters.REMOTE_JOB_ADOPT_COMMAND == ("httk", "job", "adopt")
    assert adapters.REMOTE_JOB_MOVE_COMMAND == ("httk", "job", "move")
    assert not hasattr(adapters, "REMOTE_JOB_TRANSFER_COMMAND") and not hasattr(
        adapters, "REMOTE_TRANSFER_STATUS_COMMAND"
    )
    # A remote release runs `job move --release` on the far side.
    release = _transfer._release_argv(["T"], True, "D")
    assert release == ["httk", "job", "move", "--release", "T", "--keep", "--destination-id", "D"]
    assert parser.parse_args(release[1:]).release == ["T"]
    for name in ("handle_transfer_receive", "handle_transfer_offer", "handle_transfer_retire"):
        assert not hasattr(workflow_cli, name)
    with pytest.raises(SystemExit):
        parser.parse_args(["tasks", "receive", "--workspace", "/w", "--bundle", "/b"])
    with pytest.raises(SystemExit):
        parser.parse_args(["internal", "receive", "--workspace", "/w", "--bundle", "/b"])


@pytest.mark.parametrize("group", ["launcher", "remote", "config", "campaign", "v1"])
def test_promoted_groups_are_discoverable_at_root(group, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main([group, "--help"]) == 0
    assert f"usage: httk {group}" in capsys.readouterr().out
    assert main([group]) == 0
    assert f"usage: httk {group}" in capsys.readouterr().out
    assert main([group, "frobnicate"]) == 2
    assert f"httk {group}" in capsys.readouterr().err
    assert main(["workflow", group, "--help"]) == 2
    assert "invalid choice" in capsys.readouterr().err


def test_public_workflow_help_only_names_workflow_operations(capsys):
    assert main(["workflow", "--help"]) == 0
    help_text = capsys.readouterr().out
    for name in ("launcher", "remote", "config", "campaign", "seal", "manager", "run", "monitor", "postprocess"):
        assert f"    {name} " not in help_text
    for name in ("list", "show", "install", "uninstall", "build", "fetch", "forget"):
        assert f"    {name} " in help_text


def test_run_is_top_level_and_hides_its_protocol_switch(capsys):
    assert main(["run", "--help"]) == 0
    assert "--by-path" not in capsys.readouterr().out
    for removed in (["workflow", "manager", "run", "--help"], ["workflow", "run", "--help"], ["manager", "run"]):
        assert main(removed) == 2
        capsys.readouterr()


@pytest.mark.parametrize(("verb", "keep"), [("push", True), ("pull", False)])
def test_push_and_pull_are_top_level_verbs(verb, keep, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main([verb, "--help"]) == 0
    assert capsys.readouterr().out.startswith(f"usage: httk {verb} ")
    parser = workflow_cli.build_parser("httk", _context(tmp_path))
    parsed = parser.parse_args([verb, "cluster:ws", "--state", "failed"])
    assert parsed.handler is workflow_cli.handle_transfer and parsed.transfer_verb == verb
    assert (parsed.source, parsed.state, parsed.keep, parsed.tree) == ("cluster:ws", ["failed"], keep, True)
    # The registered handler reaches the transfer: a missing other end is refused under the verb's own name.
    dispatch = {"push": workflow_cli.push_command, "pull": workflow_cli.pull_command}[verb]
    assert dispatch([], _context(tmp_path)) == 2
    end = "DST" if verb == "push" else "SRC"
    assert f"httk {verb}: name the other workspace: httk {verb} {end} (e.g. REMOTE:WS)" in capsys.readouterr().err
    assert main(["workflow", verb, "--help"]) == 2
    assert "invalid choice" in capsys.readouterr().err
    assert main(["transfer", "status"]) == 2


def test_renamed_options_and_short_flags_parse_to_their_destinations(tmp_path: Path, monkeypatch) -> None:
    from httk.workflow.registry import WorkspaceBinding
    from httk.workflow.workflow_cli import _describe

    parser = workflow_cli.build_parser("httk workflow", _context(tmp_path))
    parsed = parser.parse_args(["job", "new", "--workflow", "W", "--start-step", "s", "-p", "a=1", "-i", "b=2"])
    assert (parsed.step, parsed.parameters, parsed.inputs) == ("s", ["a=1"], ["b=2"])
    assert parser.parse_args(["job", "debug", "J", "--start-step", "s"]).step == "s"
    assert parser.parse_args(["show", "T", "--language", "cwl"]).format == "cwl"
    assert parser.parse_args(["install", "S", "--language", "cwl"]).format == "cwl"
    assert parser.parse_args(["launcher", "check", "N", "--adapter-timeout", "5"]).launcher_timeout == 5.0
    listed = parser.parse_args(["job", "list", "-w", "WS", "-s", "ready"])
    assert (listed.workspace, listed.state) == ("WS", ["ready"])
    assert parser.parse_args(["workspace", "gc", "-n"]).dry_run is True

    # The far side of a remote show and install receives --language.
    binding = WorkspaceBinding("cluster:far", "cluster", None)
    relayed: list[tuple[str, ...]] = []
    monkeypatch.setattr(_describe, "_resolve_binding", lambda *_args: (binding, None))

    def relay(_binding: object, _context: object, _arguments: object, *argv: str) -> int:
        relayed.append(argv)
        return 0

    monkeypatch.setattr(_describe, "_remote_relay", relay)
    described = parser.parse_args(["show", "T", "--language", "cwl", "--workspace", "cluster:far"])
    assert _describe.handle_workflow_show(described, _context(tmp_path)) == 0
    assert relayed == [("show", "--language", "cwl", "T")]

    invoked: list[list[str]] = []

    def adapter(
        _bundle: object, operation: str, payload: dict[str, list[str]], **_options: object
    ) -> dict[str, object]:
        invoked.append(payload["argv"])
        return {"returncode": 0}

    monkeypatch.setattr(_describe, "resolve_remote", lambda *_args, **_options: argparse.Namespace(bundle=None))
    monkeypatch.setattr(_describe, "probe_remote_workspace", lambda *_args, **_options: (None, "/far"))
    monkeypatch.setattr(_describe, "_push_source", lambda source: source)
    monkeypatch.setattr(_describe, "run_adapter", adapter)
    installing = parser.parse_args(["install", "git+https://x/r", "--language", "cwl", "--workspace", "cluster:far"])
    assert _describe._install_remote(binding, _context(tmp_path), installing) == 0
    assert invoked == [
        [*_describe._WORKFLOW_COMMAND, "install", "--language", "cwl", "--workspace", "far", "git+https://x/r"]
    ]
