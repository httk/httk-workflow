"""Git URI workflow references: fetching, installation, resolution and the install verbs."""

import json
import logging
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core import git_sources
from httk.core.cli import CLIContext
from httk.core.plugins.install import install_plugin
from httk.core.userdirs import data_home

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, collect, git_workflows, scaffold
from httk.workflow.git_workflows import fetch_workflow, fetched_workflows
from httk.workflow.packages import _reset_plugin_workflow_cache, parse_workflow_manifest
from httk.workflow.provenance import _definition_uri
from httk.workflow.scaffold import (
    WorkflowProvider,
    new_job,
    register_workflow,
    registered_workflow_labels,
    registered_workflows,
    resolve_workflow,
    workflow_provider,
)
from httk.workflow.workflow_cli import command
from httk.workflow.workflow_cli._collect import _collected_mapping
from test_call import _in_process_attempt
from test_campaigns import _campaign_project
from test_workflow_cli_packages import _SUCCESS_RUNNER
from test_workflow_packages import _package

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is not available")

_COLLECT = """from httk.core import DataRecord


def collect(record):
    return {"total": DataRecord.from_value("https://example.test/energy", "total", 1.5)}
"""


def _manifest(name: str, alias: str | None = None) -> str:
    alias_line = f'alias = "{alias}"\n' if alias else ""
    return (
        f'[workflow]\nname = "{name}"\n{alias_line}description = "Git workflow {name}."\n\n'
        '[workflow.runner]\nsteps = ["start"]\n\n[workflow.collect]\nfile = "collect.py"\n\n'
        '[workflow.outputs.total]\nentry_type = "records"\nref = "https://example.test/records"\n'
    )


def _git(cwd: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.test", *arguments],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write_package(directory: Path, name: str, alias: str | None = None) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "httk_workflow.toml").write_text(_manifest(name, alias), encoding="utf-8")
    (directory / "run").write_text(_SUCCESS_RUNNER, encoding="utf-8")
    (directory / "run").chmod(0o755)
    (directory / "collect.py").write_text(_COLLECT, encoding="utf-8")


def _repository(root: Path, packages: dict[str, str]) -> tuple[Path, str]:
    """Create a repository whose *packages* map subdir ("" = root) to short name."""

    root.mkdir(parents=True)
    _git(root, "init", "-q", "-b", "main")
    for subdir, name in packages.items():
        _write_package(root / subdir if subdir else root, name)
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "one")
    return root, _git(root, "rev-parse", "HEAD")


def _commit(root: Path, message: str) -> str:
    (root / "CHANGES").write_text(message, encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", message)
    return _git(root, "rev-parse", "HEAD")


@pytest.fixture(autouse=True)
def _caches() -> Iterator[None]:
    git_workflows._reset_fetched_workflow_cache()
    yield
    scaffold._WORKFLOW_PROVIDERS.pop("tests.git.relax", None)
    git_workflows._reset_fetched_workflow_cache()


def test_malformed_git_reference_never_falls_through_to_paths() -> None:
    with pytest.raises(ValueError, match="userinfo"):
        resolve_workflow("git+https://user@example.test/org/repo")


def test_every_ref_form_yields_the_canonical_full_hash_uri(tmp_path: Path) -> None:
    root, first = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    _git(root, "tag", "v1")
    second = _commit(root, "two")
    base = f"git+file://{root}"
    expected = {
        "": second,
        "@main": second,
        "@v1": first,
        f"@{first[:8]}": first,
        f"@{first.upper()}": first,
    }
    for ref, commit in expected.items():
        provider = fetch_workflow(f"{base}{ref}#relax")
        assert provider.workflow_id == f"{base}@{commit}#relax"
        assert provider.name == "tests.git.relax"
        assert provider.directory is not None and (provider.directory / "httk_workflow.toml").is_file()
        assert not (provider.directory.parent / ".git").exists()
    checkout = json.loads(next((data_home() / "git").glob("*/*.json")).read_text(encoding="utf-8"))
    assert checkout["repository"] == base
    assert set(fetched_workflows()) == {f"{base}@{first}#relax", f"{base}@{second}#relax"}


def test_two_subdirectories_and_a_top_level_package(tmp_path: Path) -> None:
    root, _ = _repository(tmp_path / "multi", {"a": "tests.git.a", "b": "tests.git.b"})
    base = f"git+file://{root}"
    assert fetch_workflow(f"{base}#a").name == "tests.git.a"
    assert fetch_workflow(f"{base}#b").name == "tests.git.b"
    top, top_commit = _repository(tmp_path / "top", {"": "tests.git.top"})
    provider = fetch_workflow(f"git+file://{top}")
    assert provider.workflow_id == f"git+file://{top}@{top_commit}"
    assert provider.name == "tests.git.top"


def test_missing_manifest_errors_hint_at_the_subdirectory(tmp_path: Path) -> None:
    root, commit = _repository(tmp_path / "multi", {"a": "tests.git.a", "b": "tests.git.b"})
    with pytest.raises(ValueError, match=r"no top-level httk_workflow.toml.*#subdir.*a, b"):
        fetch_workflow(f"git+file://{root}")
    with pytest.raises(ValueError, match=rf"'c' is not a directory in git\+file://{root} at {commit}"):
        fetch_workflow(f"git+file://{root}#c")
    (root / "empty").mkdir()
    (root / "empty" / "README").write_text("x", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "empty")
    with pytest.raises(ValueError, match="'empty' has no httk_workflow.toml"):
        fetch_workflow(f"git+file://{root}#empty")


def test_pinned_cached_reference_runs_no_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    uri = f"git+file://{root}@{commit}#relax"
    fetch_workflow(uri)
    shutil.rmtree(root)

    def refuse(*arguments: object, **options: object) -> None:
        raise AssertionError("git must not run for a cached pinned reference")

    monkeypatch.setattr(git_sources, "_git", refuse)
    entry = next((data_home() / "workflows" / "installed").glob("*.json"))
    before = json.loads(entry.read_text(encoding="utf-8"))["referenced_at"]
    assert fetch_workflow(uri).workflow_id == uri
    after = json.loads(entry.read_text(encoding="utf-8"))
    assert after["referenced_at"] > before
    assert after["uri"] == uri and after["subdir"] == "relax" and after["names"] == ["tests.git.relax"]


def test_job_new_records_the_canonical_uri_and_publishes_the_tree(tmp_path: Path) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, f"git+file://{root}@main#relax")
    uri = f"git+file://{root}@{commit}#relax"
    assert job.workflow == uri
    document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    assert document["workflow"] == uri
    assert document["runner"]["source"] == "workspace"
    assert (workspace.runner_store_path(str(job.runner["path"])) / "httk_workflow.toml").is_file()
    resolved = resolve_workflow(uri)
    assert resolved.definition_uri == uri
    assert "$id" not in resolved.declarations["workflow"]  # the URI names the definition, not the declaration


def test_short_name_follows_the_latest_reference_within_a_lineage(tmp_path: Path) -> None:
    root, first = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    second = _commit(root, "two")
    base = f"git+file://{root}"
    fetch_workflow(f"{base}@{first}#relax")
    fetch_workflow(f"{base}@{second}#relax")
    git_workflows._reset_fetched_workflow_cache()
    provider = workflow_provider("tests.git.relax")
    assert provider is not None and provider.workflow_id == f"{base}@{second}#relax"
    fetch_workflow(f"{base}@{first}#relax")
    provider = workflow_provider("tests.git.relax")
    assert provider is not None and provider.workflow_id == f"{base}@{first}#relax"
    assert resolve_workflow("tests.git.relax").workflow_id == f"{base}@{first}#relax"
    assert "tests.git.relax" in registered_workflows()
    assert f"tests.git.relax [{base}@{first}#relax]" in registered_workflow_labels()
    assert "did you mean 'tests.git.relax'" in _unknown("tests.git.relx")


def _unknown(name: str) -> str:
    with pytest.raises(ValueError) as caught:
        resolve_workflow(name)
    return str(caught.value)


def test_two_lineages_claiming_one_name_must_use_the_uri(tmp_path: Path) -> None:
    one, _ = _repository(tmp_path / "one", {"relax": "tests.git.relax"})
    two, _ = _repository(tmp_path / "two", {"relax": "tests.git.relax"})
    fetch_workflow(f"git+file://{one}#relax")
    fetch_workflow(f"git+file://{two}#relax")
    with pytest.raises(ValueError, match="several installed workflows.*by its URI"):
        workflow_provider("tests.git.relax")
    assert "tests.git.relax" not in registered_workflows()


def test_shadowed_short_name_warns_and_the_uri_still_resolves(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    local = tmp_path / "local"
    _write_package(local, "tests.git.relax")
    register_workflow(WorkflowProvider(workflow_id="tests.git.relax", directory=local, steps=("start",)))
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    with caplog.at_level(logging.WARNING, logger="httk.workflow.git_workflows"):
        fetch_workflow(f"git+file://{root}#relax")
    assert "shadowed" in caplog.text
    provider = workflow_provider("tests.git.relax")
    assert provider is not None and provider.directory == local
    uri = f"git+file://{root}@{commit}#relax"
    assert resolve_workflow(uri).workflow_id == uri
    assert registered_workflows().count("tests.git.relax") == 1


def test_provider_lookup_of_an_uri_is_cache_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})

    def refuse(*arguments: object, **options: object) -> None:
        raise AssertionError("workflow_provider must never run git")

    monkeypatch.setattr(git_sources, "_git", refuse)
    for text in (
        f"git+file://{root}#relax",
        f"git+file://{root}@main#relax",
        f"git+file://{root}@{commit}#relax",
        "git+https://user@example.test/bad",
    ):
        assert workflow_provider(text) is None
    assert not (data_home() / "workflows").exists()


def test_malformed_installed_entry_is_skipped(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    fetch_workflow(f"git+file://{root}#relax")
    (data_home() / "workflows" / "installed" / "bogus.json").write_text("{}", encoding="utf-8")
    git_workflows._reset_fetched_workflow_cache()
    with caplog.at_level(logging.WARNING, logger="httk.core.git_sources"):
        assert list(fetched_workflows()) == [f"git+file://{root}@{commit}#relax"]
    assert "Skipping installed git workflows entry" in caplog.text


def test_collect_dispatches_to_the_installed_provider_without_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    workspace = Workspace.initialize(tmp_path / "workspace")
    new_job(workspace, f"git+file://{root}#relax")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    def refuse(*arguments: object, **options: object) -> None:
        raise AssertionError("collection must never run git")

    monkeypatch.setattr(git_sources, "_git", refuse)
    git_workflows._reset_fetched_workflow_cache()
    item = next(collect(workspace))
    assert item.missing_collector is None
    assert set(item.outputs) == {"total"}
    assert item.run.workflow_definition_uri == f"git+file://{root}@{commit}#relax"
    assert item.run.workflow_declaration_uri is None
    run = _collected_mapping(item)["run"]
    assert isinstance(run, dict) and run["workflow_definition_uri"] == item.run.workflow_definition_uri


def test_collect_of_an_uninstalled_uri_uses_only_the_pinned_tree(tmp_path: Path) -> None:
    root, _ = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    workspace = Workspace.initialize(tmp_path / "workspace")
    new_job(workspace, f"git+file://{root}#relax")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    shutil.rmtree(data_home() / "workflows")
    git_workflows._reset_fetched_workflow_cache()

    without = next(collect(workspace))
    assert without.outputs == {} and without.missing_collector is not None
    with_fallback = next(collect(workspace, allow_job_collector=True))
    assert with_fallback.missing_collector is None and set(with_fallback.outputs) == {"total"}
    assert not (data_home() / "workflows").exists()


def test_cli_list_and_describe_report_fetched_workflows(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    uri = f"git+file://{root}@{commit}#relax"
    context = CLIContext("httk", tmp_path)
    assert command(["describe", "--json", f"git+file://{root}#relax"], context) == 0
    described = json.loads(capsys.readouterr().out)[0]
    assert described["workflow"] == uri
    assert described["source"]["kind"] == "installed"
    assert described["source"]["uri"] == uri and described["source"]["commit"] == commit
    assert described["declaration"]["id"] is None

    assert command(["describe", "tests.git.relax"], context) == 0
    assert f"source: installed {uri}" in capsys.readouterr().out

    assert command(["list", "--json"], context) == 0
    rows = [row for row in json.loads(capsys.readouterr().out) if row["workflow"] == "tests.git.relax"]
    assert rows[0]["source"] == {"kind": "installed", "plugin": None, "uri": uri}
    assert command(["list"], context) == 0
    assert f"installed {uri}" in capsys.readouterr().out


def test_cli_job_new_accepts_a_git_uri(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    context = CLIContext("httk", tmp_path)
    Workspace.initialize(tmp_path / "workspace")
    workspace = register_ws(context, tmp_path / "workspace", "git")
    assert command(["job", "new", "--workspace", workspace, "--workflow", f"git+file://{root}#relax"], context) == 0
    documents = [json.loads(path.read_text(encoding="utf-8")) for path in (tmp_path / "workspace").glob("**/job.json")]
    assert [document["workflow"] for document in documents] == [f"git+file://{root}@{commit}#relax"]


def test_uri_parse_keeps_the_manifest_name_as_short_name_and_an_explicit_declaration_uri(tmp_path: Path) -> None:
    package = _package(tmp_path / "package")
    uri = "git+https://example.test/org/repo@" + "a" * 40 + "#package"
    provider = parse_workflow_manifest(package, _uri=uri)
    assert (provider.workflow_id, provider.name, provider.alias) == (uri, "tests.package", "test-package")
    assert provider.declarations["workflow"]["$id"] == "https://example.test/workflows/package"


def test_campaign_submit_surfaces_the_git_error_of_an_uri(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root, _ = _campaign_project(tmp_path, "hash")
    missing = f"git+file://{tmp_path}/no-such-repo#relax"
    assert command(["campaign", "submit", "--workflow", missing, "--key", "k"], CLIContext("httk", root)) == 2
    error = capsys.readouterr().err
    assert "git clone" in error and "workflow names only" not in error


def test_fetch_refuses_an_unpublishable_tree_before_installing(tmp_path: Path) -> None:
    root, _ = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    (root / "relax" / "link").symlink_to("run")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "symlink")
    with pytest.raises(ValueError, match="symlink"):
        fetch_workflow(f"git+file://{root}#relax")
    assert not list((data_home() / "workflows").glob("installed/*.json"))


def test_attempt_call_fetches_an_unpinned_uri_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    workspace = Workspace.initialize(tmp_path / "workspace")
    attempt = _in_process_attempt(tmp_path, workspace.root)
    calls: list[tuple[str, ...]] = []
    original = git_sources._git

    def counting(cwd: Path, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        return original(cwd, *arguments, check=check)

    monkeypatch.setattr(git_sources, "_git", counting)
    attempt.call(f"git+file://{root}@main#relax", label="relax")
    assert [arguments[0] for arguments in calls].count("clone") == 1
    child = json.loads(next(attempt.control.glob("outcome.tmp.*/children/jobs/*/job.json")).read_text())
    assert child["workflow"] == f"git+file://{root}@{commit}#relax"


def test_describe_ignores_step_order(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    package = tmp_path / "ordered"
    _write_package(package, "tests.git.ordered")
    manifest = package / "httk_workflow.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace('steps = ["start"]', 'steps = ["start", "finish"]'),
        encoding="utf-8",
    )
    src = str(Path(__file__).parents[1] / "src")
    (package / "run").write_text(
        f"#!/usr/bin/env python3\nimport sys\nsys.path.insert(0, {src!r})\nfrom httk.workflow import Runner\n"
        "run = Runner('tests.git.ordered')\n@run.step\ndef start(a):\n    a.succeed()\n"
        "@run.step\ndef finish(a):\n    a.succeed()\nraise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    assert command(["describe", "--json", str(package)], CLIContext("httk", tmp_path)) == 0
    assert json.loads(capsys.readouterr().out)[0]["manifest_step_drift"] is None


def test_provider_lookup_never_raises_for_malformed_uri_text() -> None:
    for text in ("git+ssh://example.test/a", "git+https://example.test/a@", "git+https://example.test/a#../b"):
        assert workflow_provider(text) is None
    assert not (data_home() / "git").exists()


def test_cli_install_and_uninstall(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root, commit = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    uri = f"git+file://{root}@{commit}#relax"
    context = CLIContext("httk", tmp_path)
    assert command(["install", "--json", f"git+file://{root}#relax", "not-a-uri"], context) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == [{"uri": uri, "name": "tests.git.relax", "alias": None}]
    assert "not-a-uri" in captured.err
    assert workflow_provider("tests.git.relax") is not None
    assert not list(tmp_path.glob("**/job.json"))

    local = tmp_path / "local"
    _write_package(local, "tests.git.local")
    register_workflow(WorkflowProvider(workflow_id="tests.git.local", directory=local, steps=("start",)))
    try:
        assert command(["uninstall", "tests.git.local"], context) == 1
        assert "registered in-process" in capsys.readouterr().err
    finally:
        scaffold._WORKFLOW_PROVIDERS.pop("tests.git.local", None)
    assert command(["uninstall", "--json", "tests.git.relax"], context) == 0
    assert json.loads(capsys.readouterr().out) == [{"uri": uri, "names": ["tests.git.relax"]}]
    assert workflow_provider("tests.git.relax") is None
    assert workflow_provider(uri) is None
    assert command(["uninstall", "tests.git.relax"], context) == 1


def test_uninstall_by_pinned_uri_falls_back_and_unpinned_uri_removes_the_lineage(tmp_path: Path) -> None:
    root, first = _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    second = _commit(root, "two")
    base = f"git+file://{root}"
    fetch_workflow(f"{base}@{first}#relax")
    fetch_workflow(f"{base}@{second}#relax")
    context = CLIContext("httk", tmp_path)
    assert command(["uninstall", f"{base}@{second}#relax"], context) == 0
    provider = workflow_provider("tests.git.relax")
    assert provider is not None and provider.workflow_id == f"{base}@{first}#relax"
    fetch_workflow(f"{base}@{second}#relax")
    assert command(["uninstall", "--json", f"{base}#relax"], context) == 0
    assert workflow_provider("tests.git.relax") is None and not fetched_workflows()


def test_uninstall_of_a_plugin_workflow_points_to_plugin_uninstall(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    plugin = tmp_path / "plugin"
    _write_package(plugin / "flow", "tests.git.plugin")
    (plugin / "httk_plugin.toml").write_text('[plugin]\nname = "gitplug"\nworkflows = ["flow"]\n', encoding="utf-8")
    install_plugin(plugin)
    _reset_plugin_workflow_cache()
    try:
        assert command(["uninstall", "tests.git.plugin"], CLIContext("httk", tmp_path)) == 1
        assert "httk plugin uninstall" in capsys.readouterr().err
    finally:
        _reset_plugin_workflow_cache()


@pytest.mark.parametrize(
    ("workflow", "expected"),
    [
        ("tests.plain", None),
        ("git+https://example.test/a#b", None),
        ("git+https://example.test/a@main#b", None),
        ("git+https://example.test/a@abcdef1#b", None),
        ("git+https://example.test/a@" + "A" * 40 + "#b", "git+https://example.test/a@" + "a" * 40 + "#b"),
        ("git+ssh://example.test/a@" + "a" * 40, None),
        (None, None),
    ],
)
def test_definition_uri_is_only_a_pinned_canonical_git_uri(workflow: object, expected: str | None) -> None:
    assert _definition_uri(workflow) == expected
