"""The top-level ``httk collect`` command: workspace and calculation-tree dispatch."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.register import codes

from conftest import configure_identity, register_ws
from httk.workflow import Workspace
from httk.workflow.workflow_cli import collect_command, command, job_command, workflow_command
from test_calculations import _calculation, _collector


@pytest.fixture(autouse=True)
def _no_registered_collectors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(codes, "_collectors", {})


def _lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def test_a_tree_is_collected_and_stored_with_a_ledger(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("httk.store")
    configure_identity()
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "a")
    _calculation(tree / "b", "other", DECLINE="")
    context = CLIContext("httk", tmp_path)

    assert command(["collect", "tree", "--collector", str(package)], context) == 0
    *items, summary = _lines(capsys)
    assert [(item["workflow"], item["directory"]) for item in items] == [("tests.calc", "a")]
    assert (summary["collected"], summary["unclaimed"]) == (1, 1) and "revised" not in summary

    store = tmp_path / "into.sqlite"
    argv = ["collect", "tree", "--collector", str(package), "--into", str(store), "--id-base", "httk.probe"]
    assert command(argv, context) == 0
    *reports, summary = _lines(capsys)
    assert (tmp_path / "into.sqlite.ids.sqlite").exists()
    assert reports[0]["revised"] is False and summary["revised"] == 0
    assert reports[0]["directory"] == "a"
    (tree / "a" / "ENERGY").write_text("-4.0", encoding="utf-8")
    assert command(argv, context) == 0
    *reports, summary = _lines(capsys)
    assert reports[0]["revised"] is True and summary["revised"] == 1


def test_a_dry_run_prints_claim_lines_only(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "a")
    _calculation(tree / "b", DECLINE="")
    Workspace.initialize(tree / "ws")

    assert command(["collect", str(tree), "--dry-run", "--collector", str(package)], CLIContext("httk", tmp_path)) == 0

    lines = _lines(capsys)
    assert [(line["directory"], line["kind"]) for line in lines] == [
        ("a", "claimed"),
        ("b", "unclaimed"),
        ("ws", "workspace"),
    ]
    assert lines[0]["format"] == "httk-collect-claim" and lines[0]["format_version"] == 1
    assert lines[0]["collector"] == "tests.calc" and lines[0]["also_matched"] == []
    assert lines[1]["reason"] == "declined on request"
    assert lines[0]["consumes"] == []


def test_a_dry_run_names_consumed_directories(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from test_calculations import _parent_tree

    tree, (child, parent) = _parent_tree(tmp_path)
    argv = ["collect", str(tree), "--dry-run", "--collector", str(child), "--collector", str(parent)]

    assert command(argv, CLIContext("httk", tmp_path)) == 0

    lines = {line["directory"]: line for line in _lines(capsys)}
    assert lines["p"]["consumes"] == ["p/d1", "p/d2"] and lines["p"]["collector"] == "tests.parent"


def test_workspace_paths_dispatch_to_workspace_collection(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    (workspace.root / "inner").mkdir()
    context = CLIContext("httk", tmp_path)

    assert command(["collect", "ws"], context) == 0
    assert "unclaimed" not in _lines(capsys)[-1]

    name = register_ws(context, workspace.root, "named")
    assert command(["collect", name], context) == 0
    assert _lines(capsys)[-1]["collected"] == 0

    assert command(["collect", "ws/inner"], context) == 2
    error = capsys.readouterr().err
    assert f"is inside workspace {workspace.root}; collect {workspace.root} (use --placement to narrow)" in error
    assert command(["collect", "ws", "--workspace", name], context) == 2
    assert "either PATH or --workspace" in capsys.readouterr().err
    assert command(["collect", "nowhere"], context) == 2
    assert "PATH is neither a directory nor a registered workspace name: nowhere" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("target", "option", "message"),
    [
        ("ws", ["--dry-run"], "--dry-run only applies to a calculation tree"),
        ("ws", ["--prefer", "x"], "--prefer only applies to a calculation tree"),
        ("ws", ["--exclude", "x"], "--exclude only applies to a calculation tree"),
        ("ws", ["--collector", "x"], "--collector only applies to a calculation tree"),
        ("tree", ["--state", "failed"], "--state only applies to a workspace"),
        ("tree", ["--placement", "x"], "--placement only applies to a workspace"),
        ("tree", ["--raw"], "--raw only applies to a workspace"),
        ("tree", ["--allow-job-collector"], "--allow-job-collector only applies to a workspace"),
        ("tree", ["--dry-run", "--into", "x.sqlite"], "--dry-run cannot be combined with --into"),
        ("tree", ["--dry-run", "--fail-fast"], "--dry-run cannot be combined with --fail-fast"),
        ("tree", ["--dry-run", "--degraded"], "--dry-run cannot be combined with --degraded"),
        ("tree", ["--dry-run", "--batch-size", "64"], "--dry-run cannot be combined with --batch-size"),
        ("tree", ["--dry-run", "--no-bare-runs"], "--dry-run cannot be combined with --no-bare-runs"),
        ("tree", ["--upgrade"], "--upgrade only applies with --into"),
        ("ws", ["--upgrade"], "--upgrade only applies with --into"),
    ],
)
def test_options_are_refused_for_the_other_kind_of_target(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], target: str, option: list[str], message: str
) -> None:
    Workspace.initialize(tmp_path / "ws")
    (tmp_path / "tree").mkdir()

    assert command(["collect", target, *option], CLIContext("httk", tmp_path)) == 2
    assert message in capsys.readouterr().err


def test_an_ambiguous_claim_exits_2_naming_prefer(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    one = _collector(tmp_path / "one", "tests.one", priority=3)
    two = _collector(tmp_path / "two", "tests.two", priority=3)
    _calculation(tmp_path / "tree" / "calc")
    argv = ["collect", "tree", "--collector", str(one), "--collector", str(two)]
    context = CLIContext("httk", tmp_path)

    assert command(argv, context) == 2
    assert "--prefer NAME" in capsys.readouterr().err
    assert command([*argv, "--prefer", "tests.two"], context) == 0
    assert _lines(capsys)[0]["workflow"] == "tests.two"


def test_collect_is_no_longer_a_workflow_subcommand(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert workflow_command(["collect", "--help"], CLIContext("httk", tmp_path)) == 2
    assert "invalid choice: 'collect'" in capsys.readouterr().err


def test_standalone_commands_report_errors_under_their_own_name(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "tree").mkdir()
    context = CLIContext("httk", tmp_path)

    assert collect_command(["tree", "--into", "x.sqlite"], context) == 2
    assert capsys.readouterr().err.startswith("httk collect: --id-base is required with --into")
    assert job_command(["list", "--workspace", "no-such-workspace"], context) == 2
    assert capsys.readouterr().err.startswith("httk job: ")
