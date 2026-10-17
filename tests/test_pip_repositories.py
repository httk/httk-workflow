"""Workflow repositories pip-installed into the Python environment: discovery, resolution and the CLI."""

import json
import logging
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core import git_sources
from httk.core.cli import CLIContext
from httk.core.git_sources import cached_git_checkout, parse_git_uri
from httk.core.userdirs import data_home

from httk.workflow import git_workflows, scaffold
from httk.workflow.git_workflows import fetch_workflow
from httk.workflow.packages import _reset_plugin_workflow_cache
from httk.workflow.scaffold import WorkflowProvider, register_workflow, registered_workflows, workflow_provider
from httk.workflow.workflow_cli import command
from test_git_workflows import _commit, _git, _write_package
from test_job_creation import install, workspace_at

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is not available")

_REPOSITORY = "git+https://example.invalid/org/repo"  # unreachable: any network attempt fails
_NAME = "tests.pip.relax"


@pytest.fixture(autouse=True)
def _caches() -> Iterator[None]:
    git_workflows._reset_fetched_workflow_cache()
    _reset_plugin_workflow_cache()
    yield
    scaffold._WORKFLOW_PROVIDERS.pop(_NAME, None)
    git_workflows._reset_fetched_workflow_cache()
    _reset_plugin_workflow_cache()


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail at once on any git call that names the remote, instead of trusting a caught failure."""

    original = git_sources._git

    def guarded(cwd: Path, *arguments: str, check: bool = True) -> object:
        if any("example.invalid" in argument.lower() for argument in arguments):
            pytest.fail(f"git was asked to reach the remote: {arguments}")
        return original(cwd, *arguments, check=check)

    monkeypatch.setattr(git_sources, "_git", guarded)


def _uri(commit: str, subdir: str = "pkg") -> str:
    return str(parse_git_uri(f"{_REPOSITORY}@{commit}#{subdir}"))


def _distribution(site: Path, name: str, record: dict[str, object], bundle: Path | None = None) -> None:
    """Write a ``<name>-1.0.dist-info`` holding the record (and the bundle) below *site*."""

    info = site / f"{name.replace('-', '_')}-1.0.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.0\n", encoding="utf-8")
    (info / "httk_workflow_repository.json").write_text(json.dumps(record), encoding="utf-8")
    files = ["METADATA", "httk_workflow_repository.json", "RECORD"]
    if bundle is not None:
        shutil.copy(bundle, info / "httk_workflow_repository.bundle")
        files.append("httk_workflow_repository.bundle")
    (info / "RECORD").write_text("".join(f"{info.name}/{file},,\n" for file in files), encoding="utf-8")


def _record(**fields: object) -> dict[str, object]:
    return {"format": "httk-workflow-repository", "format_version": 1, "repository": _REPOSITORY, **fields}


def _clone(root: Path, packages: dict[str, str]) -> Path:
    root.mkdir(parents=True)
    _git(root, "init", "-q", "-b", "main")
    for subdir, name in packages.items():
        _write_package(root / subdir, name)
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "one")
    return root


def _bundled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, str, str]:
    """Install a wheel-form repository: tag wf-v1.0.0, then an intermediate commit, then HEAD."""

    root = _clone(tmp_path / "clone", {"pkg": _NAME})
    tag = _git(root, "rev-parse", "HEAD")
    _git(root, "tag", "wf-v1.0.0")
    middle = _commit(root, "two")
    head = _commit(root, "three")
    bundle = tmp_path / "repo.bundle"
    _git(root, "bundle", "create", str(bundle), "HEAD", "refs/tags/wf-v1.0.0")
    site = tmp_path / "site"
    record = _record(source="bundle", bundle="httk_workflow_repository.bundle", head=head, tags={"wf-v1.0.0": tag})
    _distribution(site, "example-repo", record, bundle)
    monkeypatch.syspath_prepend(str(site))
    git_workflows._reset_fetched_workflow_cache()
    return tag, middle, head


def test_bundle_source_offers_head_by_name_and_tags_by_uri_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tag, _, head = _bundled(tmp_path, monkeypatch)
    provider = workflow_provider(_NAME)
    assert provider is not None and provider.workflow_id == _uri(head) == f"{_REPOSITORY}@{head}#pkg"
    assert _NAME in registered_workflows()
    assert cached_git_checkout(f"{_REPOSITORY}@{tag}") is None  # tag versions are lazy
    tagged = workflow_provider(_uri(tag))
    assert tagged is not None and tagged.workflow_id == _uri(tag)
    assert cached_git_checkout(f"{_REPOSITORY}@{tag}") is not None
    assert workflow_provider(_uri(head, "missing")) is None
    assert fetch_workflow(f"{_REPOSITORY}@wf-v1.0.0#pkg").workflow_id == _uri(tag)
    assert fetch_workflow(_uri(head)).workflow_id == _uri(head)
    installed = data_home() / "workflows" / "installed"
    assert not installed.exists() or not any(installed.iterdir())
    workspace = workspace_at(tmp_path / "workspace")
    assert install(workspace, _NAME).id == _uri(head)


def test_an_intermediate_commit_is_fetched_from_the_bundle_and_installed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, middle, _ = _bundled(tmp_path, monkeypatch)
    assert workflow_provider(_uri(middle)) is None  # neither HEAD nor a tag: not offered
    assert fetch_workflow(_uri(middle)).workflow_id == _uri(middle)
    assert any((data_home() / "workflows" / "installed").iterdir())
    assert workflow_provider(_uri(middle)) is not None


def test_clone_source_follows_head_and_tags_and_only_committed_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _clone(tmp_path / "clone", {"pkg": _NAME})
    first = _git(root, "rev-parse", "HEAD")
    _git(root, "tag", "wf-v1.0.0")
    site = tmp_path / "site"
    _distribution(site, "example-repo", _record(source="clone", clone=str(root)))
    monkeypatch.syspath_prepend(str(site))
    provider = workflow_provider(_NAME)
    assert provider is not None and provider.workflow_id == _uri(first)

    second = _commit(root, "two")
    _git(root, "tag", "wf-v2.0.0")
    committed = (root / "pkg" / "collect.py").read_text(encoding="utf-8")
    (root / "pkg" / "collect.py").write_text("raise SystemExit('uncommitted')\n", encoding="utf-8")
    git_workflows._reset_fetched_workflow_cache()
    provider = workflow_provider(_NAME)
    assert provider is not None and provider.workflow_id == _uri(second)
    assert provider.directory is not None
    assert (provider.directory / "collect.py").read_text(encoding="utf-8") == committed
    assert workflow_provider(_uri(first)) is not None
    assert fetch_workflow(f"{_REPOSITORY}@wf-v2.0.0#pkg").workflow_id == _uri(second)


def test_in_process_names_win_over_pip_names_and_pip_names_over_fetched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _, _, head = _bundled(tmp_path, monkeypatch)
    other = _clone(tmp_path / "other", {"pkg": _NAME})
    with caplog.at_level(logging.WARNING, logger="httk.workflow.git_workflows"):
        fetched = fetch_workflow(f"git+file://{other}#pkg")
    assert "shadowed" in caplog.text
    provider = workflow_provider(_NAME)
    assert provider is not None and provider.workflow_id == _uri(head)
    assert workflow_provider(fetched.workflow_id) is not None
    assert registered_workflows().count(_NAME) == 1

    local = tmp_path / "local"
    _write_package(local, _NAME)
    register_workflow(WorkflowProvider(workflow_id=_NAME, directory=local, steps=("start",)))
    provider = workflow_provider(_NAME)
    assert provider is not None and provider.directory == local
    assert workflow_provider(_uri(head)) is not None
    assert registered_workflows().count(_NAME) == 1


def test_a_name_two_packages_claim_must_use_the_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _clone(tmp_path / "clone", {"a": _NAME, "b": _NAME})
    head = _git(root, "rev-parse", "HEAD")
    site = tmp_path / "site"
    _distribution(site, "example-repo", _record(source="clone", clone=str(root)))
    monkeypatch.syspath_prepend(str(site))
    with pytest.raises(ValueError, match="several pip-installed workflows") as caught:
        workflow_provider(_NAME)
    assert _uri(head, "a") in str(caught.value) and _uri(head, "b") in str(caught.value)
    assert _NAME not in registered_workflows()


def test_broken_sources_warn_and_never_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root = _clone(tmp_path / "clone", {"pkg": _NAME})
    gone = _clone(tmp_path / "gone", {"pkg": "tests.pip.gone"})
    site = tmp_path / "site"
    _distribution(site, "example-repo", _record(source="clone", clone=str(root)))
    _distribution(site, "broken-json", {})
    (site / "broken_json-1.0.dist-info" / "httk_workflow_repository.json").write_text("{", encoding="utf-8")
    _distribution(
        site,
        "broken-repo",
        _record(repository="git+https://Example.invalid/org/repo/", source="clone", clone=str(root)),
    )
    _distribution(site, "broken-bundle", _record(source="bundle", bundle="missing.bundle", head="a" * 40, tags={}))
    _distribution(site, "broken-clone", _record(source="clone", clone=str(gone)))
    _distribution(site, "broken-metadata", {})
    (site / "broken_metadata-1.0.dist-info" / "METADATA").write_bytes(b"Metadata-Version: 2.1\nName: \xff\n")
    shutil.rmtree(gone)
    monkeypatch.syspath_prepend(str(site))
    with caplog.at_level(logging.WARNING, logger="httk.workflow._pip_repositories"):
        provider = workflow_provider(_NAME)
        assert provider is not None
        assert _NAME in registered_workflows()
    for name in ("broken-json", "broken-repo", "broken-bundle", "broken-clone"):
        assert f"Python distribution '{name}'" in caplog.text
    assert "broken_metadata-1.0.dist-info" in caplog.text


def test_packages_the_git_route_refuses_are_not_offered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    outside = tmp_path / "outside.toml"
    outside.write_text('[workflow]\nname = "tests.pip.outside"\n', encoding="utf-8")
    root = tmp_path / "clone"
    _write_package(root / "pkg", _NAME)
    _write_package(root / "linked", "tests.pip.linked")
    (root / "linked" / "alias").symlink_to("run")
    (root / "manifest").mkdir(parents=True)
    (root / "manifest" / "httk_workflow.toml").symlink_to(outside)
    _git(root, "init", "-q", "-b", "main")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "one")
    head = _git(root, "rev-parse", "HEAD")
    site = tmp_path / "site"
    _distribution(site, "example-repo", _record(source="clone", clone=str(root)))
    monkeypatch.syspath_prepend(str(site))
    with caplog.at_level(logging.WARNING, logger="httk.workflow._pip_repositories"):
        assert workflow_provider(_NAME) is not None
    assert workflow_provider(_uri(head, "linked")) is None and workflow_provider(_uri(head, "manifest")) is None
    assert not {"tests.pip.linked", "tests.pip.outside"} & set(registered_workflows())
    assert _uri(head, "linked") in caplog.text and "is a symlink" in caplog.text


def test_cli_lists_pip_workflows_and_points_uninstall_to_pip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    tag, _, head = _bundled(tmp_path, monkeypatch)
    context = CLIContext("httk", tmp_path)
    assert command(["list", "--json"], context) == 0
    (row,) = [row for row in json.loads(capsys.readouterr().out) if row["workflow"] == _NAME]
    assert row["source"] == {
        "kind": "pip",
        "distribution": "example-repo",
        "uri": _uri(head),
        "tags": {"wf-v1.0.0": _uri(tag)},
    }
    assert command(["list"], context) == 0
    assert f"pip example-repo {_uri(head)}" in capsys.readouterr().out
    assert command(["describe", "--json", _NAME], context) == 0
    source = json.loads(capsys.readouterr().out)[0]["source"]
    assert (source["kind"], source["distribution"], source["uri"]) == ("pip", "example-repo", _uri(head))
    assert command(["describe", f"{_REPOSITORY}@wf-v1.0.0#pkg"], context) == 0
    assert f"source: pip example-repo {_uri(tag)}" in capsys.readouterr().out
    for selector in (_NAME, _uri(tag)):
        assert command(["uninstall", selector], context) == 1
        assert "pip uninstall example-repo" in capsys.readouterr().err
