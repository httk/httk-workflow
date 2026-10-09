"""Unit tests of :mod:`httk.workflow._store`, the workspace store of installed workflows."""

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _kernel, _store, git_workflows, scaffold
from httk.workflow._kernel import Owner, register_owner
from httk.workflow.packages import parse_workflow_manifest, source_tree_digest
from httk.workflow.scaffold import WorkflowProvider

GIT_ID = "git+https://example.test/workflows.git@" + "a" * 40 + "#sub"


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


@pytest.fixture()
def ws(tmp_path: Path) -> FakeWorkspace:
    root = tmp_path / "ws"
    workspace = FakeWorkspace(root, root / ".httk-workspace", root / "jobs", False, 0.2)
    (workspace.control / "tmp").mkdir(parents=True)
    (workspace.control / "owners").mkdir()
    return workspace


@pytest.fixture()
def owner(ws: FakeWorkspace, monkeypatch: pytest.MonkeyPatch) -> Iterator[Owner]:
    monkeypatch.setattr(_kernel, "_RECONCILERS", {})
    with register_owner(ws, kind="cli", label=None, allocation=None, advertised={}) as registered:
        yield registered


def package(parent: Path, name: str, *, calls: dict[str, str] | None = None, build: str = "") -> Path:
    directory = parent / name
    directory.mkdir(parents=True)
    manifest = f'[workflow]\nname = "{name}"\n\n[workflow.runner]\nsteps = ["start"]\n'
    if calls:
        manifest += "\n[workflow.calls]\n" + "".join(f'{alias} = "{ref}"\n' for alias, ref in calls.items())
    (directory / "httk_workflow.toml").write_text(manifest + build, encoding="utf-8")
    (directory / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (directory / "run").chmod(0o755)
    (directory / "lib").mkdir()
    (directory / "lib" / "data.txt").write_text("support\n", encoding="utf-8")
    return directory


def known(monkeypatch: pytest.MonkeyPatch, providers: dict[str, WorkflowProvider]) -> None:
    monkeypatch.setattr(scaffold, "workflow_provider", providers.get)


def test_install_from_directory(ws: FakeWorkspace, owner: Owner, tmp_path: Path) -> None:
    source = package(tmp_path, "Relax.Demo+1")
    (source / ".git").mkdir()
    (source / ".git" / "HEAD").write_text("ref\n", encoding="utf-8")
    installed = _store.install(ws, owner, source)
    assert installed.id == "local:Relax.Demo+1"
    assert installed.directory.parent == ws.root / "workflows"
    assert installed.directory.name.startswith("relax.demo-1--") and len(installed.directory.name) == 30
    assert sorted(path.name for path in installed.directory.iterdir()) == ["install.json", "package"]
    assert not (installed.package / ".git").exists() and os.access(installed.package / "run", os.X_OK)
    record = json.loads((installed.directory / "install.json").read_text(encoding="utf-8"))
    assert record == dict(installed.record)
    assert record["format"] == "httk-workflow-install" and record["format_version"] == 1
    assert record["name"] == "Relax.Demo+1" and record["source"] == str(source.resolve())
    assert record["runner"] == {"command": None, "entry": "run", "builtin": None}
    assert record["steps"] == ["start"] and record["initial_step"] == "start"
    assert record["calls"] == {} and record["requires"] == [] and record["build"] is False
    assert record["tree_sha256"] == source_tree_digest(installed.package)
    provider = installed.provider()
    assert provider.workflow_id == installed.id and provider.name == "Relax.Demo+1"
    assert installed.provider() is provider
    # A reinstall replaces the installation in place; the digest is stable.
    (source / "lib" / "extra.txt").write_text("new\n", encoding="utf-8")
    again = _store.install(ws, owner, source)
    assert again.directory == installed.directory and (again.package / "lib" / "extra.txt").is_file()
    assert again.record["tree_sha256"] != record["tree_sha256"]
    assert _store.install(ws, owner, source).record["tree_sha256"] == again.record["tree_sha256"]
    assert [entry.name for entry in (ws.control / "tmp").iterdir() if ".build." in entry.name] == []


def test_lookup_list_ambiguity_and_uninstall(ws: FakeWorkspace, owner: Owner, tmp_path: Path) -> None:
    first = _store.install(ws, owner, package(tmp_path / "a", "demo"))
    assert _store.lookup(ws, "local:demo") == first and _store.lookup(ws, "demo") == first
    assert _store.lookup(ws, "nothing") is None
    second = _store.install(ws, owner, package(tmp_path / "b", "other"))
    assert {found.id for found in _store.list_installed(ws)} == {first.id, second.id}
    # A git install claiming the same short name makes the name ambiguous.
    git_provider = parse_workflow_manifest(package(tmp_path / "c", "demo"), _uri=GIT_ID)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(git_workflows, "fetch_workflow", lambda uri: git_provider)
        third = _store.install(ws, owner, GIT_ID)
    assert third.id == GIT_ID and third.name == "demo" and third.record["source"] == GIT_ID
    with pytest.raises(ValueError, match="ambiguous.*local:demo"):
        _store.lookup(ws, "demo")
    assert _store.lookup(ws, GIT_ID) == third
    removed = _store.uninstall(ws, owner, "local:demo")
    assert removed.id == "local:demo" and not removed.directory.exists()
    assert _store.lookup(ws, "demo") == third
    with pytest.raises(ValueError, match="not installed"):
        _store.uninstall(ws, owner, "local:demo")


@pytest.mark.slow
def test_adhoc_runner_file(ws: FakeWorkspace, owner: Owner, tmp_path: Path) -> None:
    runner = tmp_path / "quick.py"
    runner.write_text(
        "#!/usr/bin/env python3\nfrom httk.workflow import Attempt, Runner\n\nrun = Runner('adhoc.quick')\n\n\n"
        "@run.step\ndef start(a: Attempt) -> None:\n    a.succeed()\n\n\n"
        "if __name__ == '__main__':\n    raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    runner.chmod(0o755)
    installed = _store.install(ws, owner, runner)
    assert installed.id.startswith("adhoc:quick@") and len(installed.id) == len("adhoc:quick@") + 12
    assert installed.name == "quick" and installed.record["steps"] == ["start"]
    assert (installed.package / "run").read_bytes() == runner.read_bytes()
    assert os.access(installed.package / "run", os.X_OK)
    assert installed.provider().initial_step == "start"


def test_calls_closure(ws: FakeWorkspace, owner: Owner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    callee = parse_workflow_manifest(package(tmp_path, "callee"))
    known(monkeypatch, {"callee": callee})
    caller = _store.install(ws, owner, package(tmp_path, "caller", calls={"sub": "callee"}))
    assert caller.record["calls"] == {"sub": "local:callee"}
    assert _store.lookup(ws, "local:callee") is not None
    assert _store.closure(ws, caller.id, check_builds=False).ok
    _store.uninstall(ws, owner, "callee")
    report = _store.closure(ws, caller.id, check_builds=False)
    assert report.missing == ("local:callee",) and not report.ok
    assert _store.closure(ws, "local:absent", check_builds=False).missing == ("local:absent",)
    # Without calls, the reference is recorded as written and reported missing.
    _store.install(ws, owner, package(tmp_path / "x", "caller", calls={"sub": "callee"}), calls=False)
    assert _store.closure(ws, caller.id, check_builds=False).missing == ("callee",)


def test_call_cycle_is_refused(
    ws: FakeWorkspace, owner: Owner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    one = parse_workflow_manifest(package(tmp_path, "one", calls={"next": "two"}))
    two = parse_workflow_manifest(package(tmp_path, "two", calls={"back": "one"}))
    known(monkeypatch, {"one": one, "two": two})
    with pytest.raises(ValueError, match="cycle: local:one -> local:two -> local:one"):
        _store.install(ws, owner, "one")


def test_build_for_platform(ws: FakeWorkspace, owner: Owner, tmp_path: Path) -> None:
    build = '\n[workflow.build]\ncommand = "sh -c \'echo built > out.bin\'"\nplatform = "echo probe-os"\nartifacts = ["out.bin"]\n'
    source = package(tmp_path, "compiled", build=build)
    (source / "out.bin").write_text("stale\n", encoding="utf-8")
    unbuilt = _store.install(ws, owner, source, build=False)
    assert not (unbuilt.package / "out.bin").exists() and unbuilt.record["build"] is True
    spec = unbuilt.provider().build
    assert spec is not None
    tag = _store.platform_tag(spec)
    assert tag.startswith("probe-os.") and unbuilt.build_dir(tag) is None
    assert _store.closure(ws, unbuilt.id, check_builds=True).unbuilt == (unbuilt.id,)
    assert _store.closure(ws, unbuilt.id, check_builds=False).ok
    built = _store.build(ws, owner, "compiled")
    assert built == unbuilt.directory / "builds" / tag
    assert (built / "artifacts" / "out.bin").read_text(encoding="utf-8") == "built\n"
    stamp = json.loads((built / "build.json").read_text(encoding="utf-8"))
    assert stamp["id"] == unbuilt.id and stamp["tree_sha256"] == unbuilt.record["tree_sha256"]
    assert _store.closure(ws, unbuilt.id, check_builds=True).ok
    assert _store.build(ws, owner, "compiled") == built  # a rebuild replaces it
    installed = _store.install(ws, owner, source)  # install builds by default
    assert installed.build_dir(tag) == built and not (installed.package / "out.bin").exists()


def test_symlink_in_package_is_refused(ws: FakeWorkspace, owner: Owner, tmp_path: Path) -> None:
    source = package(tmp_path, "linked")
    (source / "lib" / "escape").symlink_to("/etc/passwd")
    with pytest.raises(ValueError, match="symlink or special file: lib/escape"):
        _store.install(ws, owner, source)
    assert _store.list_installed(ws) == []
    assert [entry.name for entry in (ws.control / "tmp").iterdir() if ".build." in entry.name] == []


def test_in_process_provider_is_refused(ws: FakeWorkspace, owner: Owner, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = WorkflowProvider(workflow_id="mem", runner_package="workflow_fixtures", runner_file="relax.py")
    known(monkeypatch, {"mem": provider})
    with pytest.raises(ValueError, match="registered in-process only"):
        _store.install(ws, owner, "mem")
    with pytest.raises(ValueError, match="no workflow package"):
        _store.install(ws, owner, "unknown-name")
