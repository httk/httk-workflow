"""Classification of application settings as recognized, unrecognized or undeclared, and its CLI surfaces."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.register import codes as code_registry

from conftest import Remote, fake_remote
from httk.workflow import _setting_catalog, _store
from httk.workflow._setting_catalog import classify_setting
from httk.workflow.projects import initialize_project
from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import command
from httk.workflow.workspace import Workspace
from v3_helpers import install


@pytest.fixture
def zz_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register a fake code ``zz`` declaring ``zz.command``, in an isolated copy of the code registry."""

    monkeypatch.setattr(code_registry, "_codes", dict(code_registry._codes))
    code_registry.register_code("zz", bridge="zz.bridge", settings=("zz.command",))


def test_engine_keys_are_recognized_at_their_layers() -> None:
    for key in ("slurm.account", "manager.workers", "confine.bwrap", "environment.prelude", "confine.environment.FOO"):
        for layer in ("workspace", "launcher"):
            status = classify_setting(key, "x", layer=layer)
            assert (status.state, status.readers) == ("recognized", ("httk-workflow",)), (key, layer)
    for key in ("manager.launch", "seal.keys", "children.placement", "httk_v1.root"):
        assert classify_setting(key, "x").state == "recognized"
        assert classify_setting(key, "x", layer="launcher").state == "unrecognized"
    status = classify_setting("manager.launch", "slurm", layer="launcher")
    assert "workspace setting" in status.note
    assert classify_setting("slurm.export", "NONE", layer="launcher").state == "recognized"
    assert classify_setting("slurm.export", "NONE").state == "unrecognized"


def test_jobflow_runner_keys_are_recognized_only_as_workspace_settings() -> None:
    for key in ("pymatgen.x", "atomate2.y.z", "vasp.command"):
        status = classify_setting(key, "x")
        assert status.state == "recognized"
        assert "httk-workflow jobflow runner" in status.readers
    assert classify_setting("vasp.command", "x", layer="launcher").state == "unrecognized"


def test_unrecognized_slurm_keys_suggest_a_close_key_or_sbatch_options() -> None:
    status = classify_setting("slurm.qos", "high")
    assert status.state == "unrecognized"
    assert "slurm.sbatch_options" in status.note and "--qos=high" in status.note
    status = classify_setting("slurm.acount", "proj")
    assert status.state == "unrecognized"
    assert "slurm.account" in status.note and "sbatch_options" not in status.note
    assert "slurm.sbatch_options" in classify_setting("slurm.dot.ted", "x").note
    status = classify_setting("slurm.qos", "high", layer="launcher")
    assert status.state == "unrecognized" and "--qos=high" in status.note
    assert "slurm.account" in classify_setting("slurm.acount", "p", layer="launcher").note
    assert classify_setting("foo.bar", 1, layer="launcher").state == "unrecognized"
    # Options slurm.sbatch_options refuses name the refusal's reason instead.
    assert "slurm.time_limit" in classify_setting("slurm.time", "1:00:00").note
    assert "reserved by httk" in classify_setting("slurm.job_name", "x").note


def test_unknown_confine_keys_are_refused_rather_than_inert() -> None:
    for layer in ("workspace", "launcher"):
        status = classify_setting("confine.isolate_networks", "true", layer=layer)
        assert status.state == "unrecognized"
        assert "confine.isolate_network" in status.note and "refuse to start" in status.note


def test_custom_launcher_kinds_read_their_own_settings() -> None:
    for key in ("pbs.queue", "slurm.partition", "manager.launch"):
        status = classify_setting(key, "x", layer="launcher", kind="pbs")
        assert status.state == "undeclared" and "own executable" in status.note


def test_code_declared_keys(zz_code: None) -> None:
    status = classify_setting("zz.command", "run")
    assert (status.state, status.readers) == ("recognized", ("zz code support",))
    status = classify_setting("zz.other", "run")
    assert status.state == "undeclared" and "zz code support" in status.note
    assert classify_setting("zz.command", "run", layer="launcher").state == "unrecognized"
    status = classify_setting("foo.bar", 1)
    assert status.state == "undeclared" and "code support" not in status.note


def test_installed_workflow_declarations_recognize_workspace_keys(zz_code: None) -> None:
    workflows = {"b": ("zz.other", "mywf.x"), "a": ("mywf.x",), "c": ()}
    status = classify_setting("mywf.x", 1, workflows=workflows)
    assert (status.state, status.readers) == ("recognized", ("workflow a", "workflow b"))
    status = classify_setting("zz.other", 1, workflows=workflows)
    assert status.annotation == "recognized by workflow b"
    many = {name: ("zz.command",) for name in "edcba"}
    status = classify_setting("zz.command", 1, workflows=many)
    assert status.readers == ("zz code support", *(f"workflow {name}" for name in "abcde"))
    assert status.annotation == "recognized by zz code support, workflow a, workflow b and 3 more"
    # A workflow's declaration never makes an ignored engine key read.
    engine = {"a": ("confine.foo", "slurm.qos")}
    assert classify_setting("confine.foo", 1, workflows=engine).state == "unrecognized"
    assert classify_setting("slurm.qos", "x", workflows=engine).state == "unrecognized"
    # Workflows never read launcher settings.
    assert classify_setting("mywf.x", 1, layer="launcher", workflows=workflows).state == "unrecognized"
    status = classify_setting("mywf.y", 1, workflows=workflows)
    assert status.state == "undeclared" and "future workflow" in status.note


def test_settings_show_annotates_and_settings_set_warns(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    settings = {"slurm.account": "p", "slurm.qos": "high", "foo.bar": 1}
    create_workspace("ws", tmp_path / "ws", settings=settings)
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "settings", "show", "ws"], context) == 0
    rows = [line.split("\t") for line in capsys.readouterr().out.splitlines()]
    assert [(row[0], row[2].split(" ")[0]) for row in rows] == [
        ("foo.bar", "undeclared:"),
        ("slurm.account", "recognized"),
        ("slurm.qos", "unrecognized:"),
    ]
    assert command(["workspace", "settings", "show", "--json", "ws"], context) == 0
    assert json.loads(capsys.readouterr().out) == [settings]

    assert command(["workspace", "settings", "set", "--key", "slurm.qos", "--value", "low", "ws"], context) == 0
    captured = capsys.readouterr()
    assert captured.out == '{"slurm.qos": "low"}\n'
    warnings = captured.err.splitlines()
    assert len(warnings) == 1 and "slurm.qos" in warnings[0] and "slurm.sbatch_options" in warnings[0]
    assert Workspace(tmp_path / "ws").settings["slurm.qos"] == "low"
    assert command(["workspace", "settings", "set", "--key", "vasp.command", "--value", "v", "ws"], context) == 0
    assert capsys.readouterr().err == ""
    assert command(["workspace", "settings", "set", "--key", "foo.bar", "--value", "2", "ws"], context) == 0
    captured = capsys.readouterr()
    assert captured.out == '{"foo.bar": 2}\n'
    (notice,) = captured.err.splitlines()
    assert notice.startswith("notice: setting foo.bar is undeclared:")
    assert command(["workspace", "configure", "--set", "manager.workerz=2", "ws"], context) == 0
    assert "manager.workers" in capsys.readouterr().err
    assert command(["workspace", "init", "--setting", "slurm.qos=x", str(tmp_path / "other")], context) == 0
    assert "slurm.qos" in capsys.readouterr().err


def test_remote_init_relays_the_far_side_warning(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-warn")
    fake_remote(project)
    context = CLIContext("httk", project)
    assert command(["workspace", "init", "--setting", "slurm.qos=x", "cluster:runs"], context) == 0
    assert "warning: setting slurm.qos" in capsys.readouterr().err


def test_launcher_show_annotates_and_configure_warns(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="catalog")
    context = CLIContext("httk", project)
    assert command(["launcher", "add", "--template", "slurm", "--set", "slurm.partition=p", "cluster"], context) == 0
    assert capsys.readouterr().err == ""
    assert command(["launcher", "add", "--template", "slurm", "--set", "slurm.qos=x", "other"], context) == 0
    assert "slurm.qos" in capsys.readouterr().err
    assert command(["launcher", "configure", "--set", "vasp.command=x", "cluster"], context) == 0
    assert "vasp.command" in capsys.readouterr().err
    assert command(["launcher", "show", "cluster"], context) == 0
    out = capsys.readouterr().out
    assert "slurm.partition=p  # recognized by httk-workflow" in out
    assert "vasp.command=x  # unrecognized:" in out


def test_color_follows_tty_and_no_color(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    create_workspace("ws", tmp_path / "ws", settings={"slurm.qos": "high"})
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "settings", "show", "ws"], context) == 0
    assert "\x1b[" not in capsys.readouterr().out
    monkeypatch.setattr(_setting_catalog.sys.stdout, "isatty", lambda: True)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("TERM", raising=False)
    assert command(["workspace", "settings", "show", "ws"], context) == 0
    assert "\x1b[" in capsys.readouterr().out
    monkeypatch.setenv("NO_COLOR", "1")
    assert command(["workspace", "settings", "show", "ws"], context) == 0
    assert "\x1b[" not in capsys.readouterr().out


def test_custom_kind_launcher_settings_are_undeclared_without_warnings(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="custom-kind")
    context = CLIContext("httk", project)
    assert command(["launcher", "add", "--template", "slurm", "pbs"], context) == 0
    metadata_path = project / "httk_project" / "launchers" / "pbs" / "launcher.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["kind"] = "pbs"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    capsys.readouterr()
    assert command(["launcher", "configure", "--set", "pbs.queue=gpu", "pbs"], context) == 0
    assert capsys.readouterr().err == ""
    assert command(["launcher", "show", "pbs"], context) == 0
    assert "pbs.queue=gpu  # undeclared:" in capsys.readouterr().out


def test_settings_show_counts_installed_workflow_declarations(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    create_workspace("ws", tmp_path / "ws", settings={"mywf.threshold": 1, "other.key": 2})
    install(Workspace(tmp_path / "ws"), tmp_path / "package", name="mywf", workflow='settings = ["mywf.threshold"]')
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "settings", "show", "ws"], context) == 0
    rows = {row[0]: row[2] for row in (line.split("\t") for line in capsys.readouterr().out.splitlines())}
    assert rows["mywf.threshold"] == "recognized by workflow mywf"
    assert rows["other.key"].startswith("undeclared:")
    assert command(["workspace", "settings", "set", "--key", "mywf.threshold", "--value", "3", "ws"], context) == 0
    assert capsys.readouterr().err == ""
    assert command(["workspace", "configure", "--set", "other.key=3", "ws"], context) == 0
    assert capsys.readouterr().err.startswith("notice: setting other.key is undeclared:")


def test_environment_entries_declare_and_engine_keys_still_warn(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    create_workspace("ws", tmp_path / "ws", settings={"tool.command": "x"})
    manifest = (
        '[workflow.environment.command]\nsetting = "tool.command"\n\n'
        '[workflow.environment.mode]\nsetting = "confine.foo"\n'
    )
    install(Workspace(tmp_path / "ws"), tmp_path / "package", name="wfa", manifest=manifest)
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "settings", "show", "ws"], context) == 0
    assert "tool.command\t\"x\"\trecognized by workflow wfa" in capsys.readouterr().out
    assert command(["workspace", "settings", "set", "--key", "confine.foo", "--value", "1", "ws"], context) == 0
    assert capsys.readouterr().err.startswith("warning: setting confine.foo is unrecognized:")


def test_broken_or_unreadable_installations_leave_keys_undeclared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    create_workspace("ws", tmp_path / "ws")
    installed = install(Workspace(tmp_path / "ws"), tmp_path / "package", name="mywf", workflow='settings = ["mywf.x"]')
    assert _setting_catalog.installed_workflow_settings(Workspace(tmp_path / "ws")) == {"mywf": ("mywf.x",)}
    (installed.package / "httk_workflow.toml").write_text("[workflow\n", encoding="utf-8")
    assert _setting_catalog.installed_workflow_settings(Workspace(tmp_path / "ws")) == {}

    def unreadable(workspace: object) -> list[object]:
        raise PermissionError("workflows/")

    monkeypatch.setattr(_store, "list_installed", unreadable)
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "settings", "set", "--key", "mywf.x", "--value", "1", "ws"], context) == 0
    assert capsys.readouterr().err.startswith("notice: setting mywf.x is undeclared:")
