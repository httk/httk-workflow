"""Workflows travel with ordinary transfers: what DST lacks and SRC has is installed there under the same id."""

import dataclasses
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _moving, _store
from httk.workflow.gc import collect_garbage
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from test_job_transfer import adapter_calls, outcomes, outgoing, project, registered, run, starts  # noqa: F401
from test_moving import client_bundle, find, place, single
from v3_helpers import cli_owner, job_mapping

GIT_ID = "git+https://example.test/workflows.git@" + "a" * 40 + "#callee"
BUILD = '\n[workflow.build]\ncommand = "sh -c \'echo built > out.bin\'"\nartifacts = ["out.bin"]\n'


def package(parent: Path, name: str, extra: str = "") -> Path:
    directory = parent / name
    directory.mkdir(parents=True)
    manifest = f'[workflow]\nname = "{name}"\n\n[workflow.runner]\nsteps = ["start"]\n{extra}'
    (directory / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (directory / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (directory / "run").chmod(0o755)
    return directory


def workflows(workspace: Workspace, tmp_path: Path, *, build: str = BUILD, built: bool = True) -> _store.Installed:
    """Install a git callee (with a build section) and a ``local:caller`` calling it; return the caller."""

    callee = parse_workflow_manifest(package(tmp_path / "callee", "callee", build), _uri=GIT_ID)
    caller = package(tmp_path / "caller", "caller", f'\n[workflow.calls]\nsub = "{GIT_ID}"\n')
    with cli_owner(workspace) as owner:
        _store.install(workspace, owner, GIT_ID, siblings={GIT_ID: callee}, build=built)
        return _store.install(workspace, owner, caller)


def job(workspace: Workspace, workflow: _store.Installed, state: str = "succeeded") -> str:
    mapping = job_mapping(workflow, {"start": "succeed"}, tag="solo", placement="proj/solo")
    place(workspace, mapping, state)
    return str(mapping["id"])


def shipped(outcome: dict[str, Any]) -> dict[str, str]:
    return {entry["id"]: entry["status"] for entry in outcome["workflows"]}


def landings(workspace: Workspace) -> list[str]:
    return [path.name for path in (workspace.control / "tmp").iterdir() if path.name.startswith("push.")]


def test_a_transfer_ships_the_workflow_a_call_slot_is_bound_to(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    caller = package(tmp_path / "caller", "caller", '\n[workflow.calls.forces]\ndeclaration_file = "forces.json"\n')
    interface = {
        "inputs": [{"name": "structure", "entry_type": "structures", "ref": "https://example.test/structure"}],
        "outputs": [
            {"name": "forces", "entry_type": "records", "ref": "https://example.test/f", "product_of": "structure"}
        ],
    }
    (caller / "forces.json").write_text(json.dumps(interface), encoding="utf-8")
    with cli_owner(source) as owner:
        installed = _store.install(source, owner, caller)
        bound = _store.install(source, owner, package(tmp_path / "bound", "bound"))
    mapping = job_mapping(installed, {"start": "succeed"}, tag="solo", placement="proj/solo")
    mapping["calls"] = {"forces": {"workflow": bound.id, "parameters": {}}}
    place(source, mapping, "succeeded")
    assert run(tmp_path, "--json", "--job", str(mapping["id"]), source_name, target_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {"local:caller": "installed", "local:bound": "installed"}
    assert _store.lookup(target, "local:bound") is not None


def test_local_to_local_ships_the_workflows_and_their_calls(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    job_id = job(source, caller)
    assert run(tmp_path, "--json", "--job", job_id, source_name, target_name) == 0
    captured = capsys.readouterr()
    (outcome,) = json.loads(captured.out)["transfers"]
    assert outcome["status"] == "adopted" and outcome["missing_workflows"] == []
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    assert f"installed these workflows in the destination: {GIT_ID}, local:caller" in captured.err
    for workflow_id in (GIT_ID, "local:caller"):
        copy = _store.lookup(target, workflow_id)
        assert copy is not None and copy.record["source"] == f"transfer from {source.root}"
    # Built for this platform after the adoption: the job can start.
    assert _store.closure(target, "local:caller", check_builds=True).ok
    # Back again: the source still has them with the same trees, so they stay.
    assert run(tmp_path, "--json", "--job", job_id, target_name, source_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "present", "local:caller": "present"}


def test_an_installation_is_never_replaced_and_a_different_tree_is_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path / "src")
    other = package(tmp_path / "other", "caller", f'\n[workflow.calls]\nsub = "{GIT_ID}"\n# another tree\n')
    with cli_owner(target) as owner:
        theirs = _store.install(target, owner, other, calls=False)
    assert run(tmp_path, "--json", "--job", job(source, caller), source_name, target_name) == 0
    captured = capsys.readouterr()
    (outcome,) = json.loads(captured.out)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "mismatch"}
    assert "another tree, left as they are (the jobs run it): local:caller" in captured.err
    assert _store.lookup(target, "local:caller") == theirs


def test_a_tampered_entry_is_refused_and_reported(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    (caller.package / "run").write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    assert run(tmp_path, "--json", "--job", job(source, caller), source_name, target_name) == 0
    captured = capsys.readouterr()
    (outcome,) = json.loads(captured.out)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "refused"}
    assert outcome["missing_workflows"] == ["local:caller"]
    assert "warning: workflow local:caller refused: " in captured.err and "tree digest mismatch" in captured.err
    assert "these workflows are not installed, so their jobs wait: local:caller" in captured.err


def test_a_failed_build_is_reported_and_the_installation_stays(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    failing = '\n[workflow.build]\ncommand = "sh -c \'exit 3\'"\nartifacts = ["out.bin"]\n'
    caller = workflows(source, tmp_path, build=failing, built=False)
    assert run(tmp_path, "--json", "--job", job(source, caller), source_name, target_name) == 0
    captured = capsys.readouterr()
    (outcome,) = json.loads(captured.out)["transfers"]
    (callee,) = [entry for entry in outcome["workflows"] if entry["id"] == GIT_ID]
    assert callee["status"] == "unbuilt"
    assert f"httk workflow build --workspace {target.root} {GIT_ID}" in callee["reason"]
    assert _store.lookup(target, GIT_ID) is not None
    assert _store.closure(target, "local:caller", check_builds=True).unbuilt == (GIT_ID,)


def test_no_workflows_opts_out_and_is_refused_with_hold_and_release(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    assert run(tmp_path, "--json", "--no-workflows", "--job", job(source, caller), source_name, target_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert "workflows" not in outcome and outcome["missing_workflows"] == ["local:caller"]
    assert _store.list_installed(target) == []
    assert run(tmp_path, "--no-workflows", "--hold", "--job", "x", source_name, target_name) == 2
    assert "--no-workflows applies to a transfer" in capsys.readouterr().err
    assert run(tmp_path, "--no-workflows", "--release", "0" * 32, source_name) == 2
    assert "--release takes transfer ids" in capsys.readouterr().err


def test_an_older_source_ships_nothing_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    real = transfer_cli._holds

    def older(*arguments: Any, **options: Any) -> list[_moving.Hold]:  # a transfer status without workflows
        return [dataclasses.replace(hold, workflows=None) for hold in real(*arguments, **options)]

    monkeypatch.setattr(transfer_cli, "_holds", older)
    assert run(tmp_path, "--json", "--job", job(source, caller), source_name, target_name) == 0
    captured = capsys.readouterr()
    assert f"warning: {source.root} reported no usable workflows for its holds" in captured.err
    assert "workflows were not shipped" in captured.err
    (outcome,) = json.loads(captured.out)["transfers"]
    assert "workflows" not in outcome and _store.list_installed(target) == []


def test_a_held_transfer_reports_its_workflows_and_resume_re_queries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    job_id = job(source, caller)
    assert run(tmp_path, "--hold", "--json", "--job", job_id, source_name, target_name) == 0
    (hold,) = outcomes(capsys)["holds"]
    assert {entry["id"]: entry["directory"] for entry in hold["workflows"]} == {
        GIT_ID: _store.lookup(source, GIT_ID).directory.name,  # type: ignore[union-attr]
        "local:caller": caller.directory.name,
    }
    assert run(tmp_path, "--json", "--resume", source_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    assert find(target, job_id) is not None


def test_local_to_remote_and_pull_back_ship_through_the_adapter(
    project: Path,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    home, home_name = registered(tmp_path, "home")
    calls = adapter_calls(monkeypatch)
    job_id = job(near, workflows(near, tmp_path))
    assert run(project, "--json", "--job", job_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}, outcome["workflows"]
    # The adoption reports the workflows missing; one more start installs and builds them.
    assert calls == ["deliver job adopt", "deliver workflow install"]
    assert {found.id for found in _store.list_installed(far)} == {GIT_ID, "local:caller"}
    assert _store.closure(far, "local:caller", check_builds=True).ok and landings(far) == []
    # Pulled back into a third workspace: copied into a landing scratch there, which goes.
    calls.clear()
    assert run(project, "--json", "--job", job_id, f"cluster:{far_name}", home_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    assert calls == ["job transfer", "pull", "pull", "job transfer"]
    assert not list((home.control / "tmp").iterdir())
    assert _store.lookup(home, GIT_ID).record["source"] == f"transfer from cluster:{far_name}"  # type: ignore[union-attr]
    # Remote to remote is relayed through this client.
    assert run(project, "--json", "--job", job_id, home_name, f"cluster:{near_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "present", "local:caller": "present"}
    with cli_owner(far) as owner:
        for workflow_id in ("local:caller", GIT_ID):
            _store.uninstall(far, owner, workflow_id)
    calls.clear()
    assert run(project, "--json", "--job", job_id, f"cluster:{near_name}", f"other:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    relayed = ["job transfer", "pull", "deliver job adopt", "pull", "deliver workflow install", "job transfer"]
    assert calls == relayed
    assert outgoing(near) == [] and landings(far) == [] and find(far, job_id) is not None


def test_ssh_ships_store_entries_in_one_push_and_one_install(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project_root = tmp_path / "project"
    initialize_project(project_root, name="ssh-workflows")
    fake_remote(project_root)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    calls = adapter_calls(monkeypatch)
    job_id = job(near, workflows(near, tmp_path), state="ready")
    assert run(project_root, "--json", "--job", job_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    assert calls == ["deliver job adopt", "deliver workflow install"]
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (2, 2)
    assert _store.closure(far, "local:caller", check_builds=True).ok
    assert {found.id for found in _store.list_installed(far)} == {GIT_ID, "local:caller"}
    assert landings(far) == []
    assert find(far, job_id) is not None


def test_the_exchange_adoption_never_installs_workflows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from v3_helpers import workspace as v3_workspace

    def refuse(*_arguments: object, **_options: object) -> object:
        raise AssertionError("the untrusted adoption reached the store")

    monkeypatch.setattr(_store, "install_copy", refuse)
    monkeypatch.setattr(_store, "install", refuse)
    workspace = v3_workspace(tmp_path / "ws")
    client_bundle(tmp_path / "inbox" / "entry")
    with cli_owner(workspace) as owner:
        report = _moving.adopt(
            workspace, owner, tmp_path / "inbox" / "entry", untrusted=True, refused_to=tmp_path / "rejected"
        )
    assert report is not None and report.published and report.missing_workflows


def test_transfer_status_reports_the_workflows_of_each_hold(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    caller = workflows(source, tmp_path)
    assert run(tmp_path, "--hold", "--job", job(source, caller), source_name, "far:somewhere") == 0
    capsys.readouterr()
    from httk.workflow.workflow_cli import transfer_command

    assert transfer_command(["status", "--json", source_name], CLIContext("httk", tmp_path)) == 0
    (hold,) = json.loads(capsys.readouterr().out)["holds"]
    assert sorted(entry["id"] for entry in hold["workflows"]) == [GIT_ID, "local:caller"]
    assert _moving.Hold.from_mapping(hold).workflows is not None


def test_an_older_destination_s_install_refusal_is_reported_and_gc_sweeps_landings(
    project: Path,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    real = transfer_cli._deliver_call

    def older(end: Any, context: Any, arguments: Any, source: str, files: list[str], argv: list[str]) -> Any:
        if "workflow" in argv:
            return 2, "", "httk: error: unrecognized arguments: --stdin-tar"
        return real(end, context, arguments, source, files, argv)

    monkeypatch.setattr(transfer_cli, "_deliver_call", older)
    assert run(project, "--json", "--job", job(near, workflows(near, tmp_path)), near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "adopted"
    assert set(shipped(outcome).values()) == {"failed"}
    assert all("older httk-workflow" in entry["reason"] for entry in outcome["workflows"])
    assert landings(far) == []
    # gc removes a pushed copy (an interrupted install's landing) after a day, and nothing else in tmp/.
    landing = far.control / "tmp" / ("push." + "3" * 32)
    landing.mkdir()
    young = far.control / "tmp" / ("push." + "0" * 32)
    young.mkdir()
    other = far.control / "tmp" / "push.not-hex"
    other.mkdir()
    old = time.time() - 2 * 86400
    os.utime(landing, (old, old))
    os.utime(other, (old, old))
    collect_garbage(far, categories=["tmp_entries"])
    assert sorted(landings(far)) == sorted([young.name, other.name])


def _pushed(workspace: Workspace, entry: _store.Installed) -> Path:
    landing = workspace.control / "tmp" / ("push." + "1" * 32)
    shutil.copytree(entry.directory, landing / entry.directory.name)
    return landing / entry.directory.name


def test_install_from_store_consumes_the_landing_even_when_refused_early(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.workflow.workflow_cli import _describe, command

    source, _ = registered(tmp_path, "a")
    target, _ = registered(tmp_path, "b")
    entry = _pushed(target, workflows(source, tmp_path))
    context = CLIContext("httk", tmp_path)
    argv = ["install", "--by-path", "--workspace", str(target.root), "--consume", "--from-store", str(entry)]

    def sealed(_workspace: object) -> None:
        raise RuntimeError("the workspace is sealed")

    with monkeypatch.context() as patch:
        patch.setattr(_describe, "require_cli_modifiable", sealed)
        assert command(argv, context) == 2
    assert "sealed" in capsys.readouterr().err and not entry.parent.exists()
    # A pushed copy that cannot be discarded is reported, leaving the document and the exit status alone.
    entry = _pushed(target, workflows(source, tmp_path / "again"))
    stray = tmp_path / "stray" / entry.name
    shutil.copytree(entry, stray)
    argv[-1:] = [str(entry), str(stray)]
    assert command(argv, context) == 0
    captured = capsys.readouterr()
    assert [row["status"] for row in json.loads(captured.out)["workflows"]] == ["installed", "present"]
    assert "the pushed copy was not discarded" in captured.err and not entry.parent.exists()


def test_a_forward_incompatible_workflows_report_reads_as_none() -> None:
    hold = {
        "transfer_id": "0" * 32,
        "path": "/w/.httk-workspace/transfers/outgoing/" + "0" * 32,
        "members": ["k"],
        "destination": None,
        "destination_workspace_id": None,
        "created_at": "2026-10-23T00:00:00+00:00",
    }
    entry = {"id": "local:x", "directory": "x--" + "a" * 16, "tree_sha256": "b" * 64}
    assert _moving.Hold.from_mapping({**hold, "workflows": [entry]}).workflows == (
        ("local:x", entry["directory"], "b" * 64),
    )
    for workflows in ([{**entry, "directory": "../escape"}], [{"id": 7, "directory": None}], {"a": 1}):
        assert _moving.Hold.from_mapping({**hold, "workflows": workflows}).workflows is None


def test_a_name_starting_with_a_dash_is_shipped(
    project: Path,  # noqa: F811
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    with cli_owner(near) as owner:
        dashed = _store.install(near, owner, package(tmp_path, "-dash"))
    assert dashed.directory.name.startswith("-dash--")
    assert run(project, "--json", "--job", job(near, dashed), near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {"local:-dash": "installed"} and landings(far) == []


def test_an_unbuilt_installation_is_built_by_a_later_transfer(
    project: Path,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    flag = tmp_path / "toolchain"
    build = (
        f"\n[workflow.build]\ncommand = \"sh -c 'test -f {flag} && echo built > out.bin'\"\nartifacts = [\"out.bin\"]\n"
    )
    job_id = job(near, workflows(near, tmp_path, build=build, built=False))
    assert run(project, "--json", "--job", job_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "unbuilt", "local:caller": "installed"}
    flag.write_text("", encoding="utf-8")
    # Back (near builds its own unbuilt copy) and out again: far's installation is present and now built.
    assert run(project, "--json", "--job", job_id, f"cluster:{far_name}", near_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "present", "local:caller": "present"}
    assert _store.closure(near, "local:caller", check_builds=True).ok
    calls = adapter_calls(monkeypatch)
    assert run(project, "--json", "--job", job_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "present", "local:caller": "present"}
    # Nothing to install, one unbuilt: one more start, which only builds.
    assert calls == ["deliver job adopt", "workflow install"]
    assert _store.closure(far, "local:caller", check_builds=True).ok


def test_workflows_installed_nowhere_are_named(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    caller = package(tmp_path, "caller", '\n[workflow.calls]\nsub = "local:absent"\n')
    with cli_owner(source) as owner:
        installed = _store.install(source, owner, caller, calls=False)
    assert run(tmp_path, "--job", job(source, installed), source_name, target_name) == 0
    err = capsys.readouterr().err
    assert "these workflows are installed in neither workspace, so their jobs wait: local:absent" in err


def test_an_installation_under_another_id_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    real = _store.install_copy

    def elsewhere(*arguments: Any, **options: Any) -> tuple[str, _store.Installed]:
        status, installed = real(*arguments, **options)
        return status, dataclasses.replace(installed, id="local:other")

    monkeypatch.setattr(_store, "install_copy", elsewhere)
    assert run(tmp_path, "--json", "--job", job(source, caller), source_name, target_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "refused", "local:caller": "refused"}


def test_a_lost_install_race_never_rebuilds_the_winner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path / "src")
    # Another actor installs the callee (unbuilt) after the destination was queried.
    workflows(target, tmp_path / "dst", built=False)
    monkeypatch.setattr(transfer_cli, "_dst_workflows", lambda *arguments: {})
    assert run(tmp_path, "--json", "--job", job(source, caller), source_name, target_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "present", "local:caller": "present"}
    assert _store.closure(target, "local:caller", check_builds=True).unbuilt == (GIT_ID,)


def test_workflow_list_probes_builds_only_for_the_ids_asked(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from httk.workflow.workflow_cli import command

    workspace, name = registered(tmp_path, "a")
    workflows(workspace, tmp_path, built=False)
    context = CLIContext("httk", tmp_path)
    assert command(["list", "--json", "--workspace", name], context) == 0
    assert all("built" not in row for row in json.loads(capsys.readouterr().out))
    assert command(["list", "--json", "--workspace", name, "--built-for", GIT_ID], context) == 0
    rows = {row["id"]: row for row in json.loads(capsys.readouterr().out)}
    assert rows[GIT_ID]["built"] is False and "built" not in rows["local:caller"]


def test_a_missing_root_workflow_is_warned_about_once(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    mapping, _, _ = single(source)
    assert run(tmp_path, "--job", str(mapping["id"]), source_name, target_name) == 0
    err = capsys.readouterr().err
    assert "not installed, so their jobs wait: demo--0123456789abcdef" in err
    assert "installed in neither workspace" not in err


def test_install_from_store_builds_in_the_same_call(tmp_path: Path, capfd: pytest.CaptureFixture[str]) -> None:
    from httk.workflow.workflow_cli import command

    source, _ = registered(tmp_path, "a")
    target, _ = registered(tmp_path, "b")
    noisy = '\n[workflow.build]\ncommand = "sh -c \'echo noisy; echo built > out.bin\'"\nartifacts = ["out.bin"]\n'
    workflows(source, tmp_path, build=noisy)
    callee, caller = _store.lookup(source, GIT_ID), _store.lookup(source, "local:caller")
    assert callee is not None and caller is not None
    entry = _pushed(target, callee)
    shutil.copytree(caller.directory, entry.parent / caller.directory.name)
    with cli_owner(target) as owner:
        here = _store.install(target, owner, package(tmp_path / "here", "here", noisy), build=False)
    assert _store.built(here) is False
    capfd.readouterr()  # the source's own build printed here
    argv = ["install", "--by-path", "--workspace", str(target.root), "--consume", "--build"]
    argv += ["--also-build", here.id, "--also-build", "local:absent"]
    argv += ["--from-store", str(entry), str(entry.parent / caller.directory.name)]
    # One call installs both, builds the one that declares [workflow.build], and builds the one already here.
    assert command(argv, CLIContext("httk", tmp_path)) == 1  # local:absent cannot be built
    captured = capfd.readouterr()
    document = json.loads(captured.out)  # the builds' own output went to stderr
    assert "noisy" in captured.err
    rows = {row["id"]: row for row in document["workflows"]}
    assert rows[GIT_ID]["status"] == "installed" and rows[GIT_ID]["build"] is True
    assert rows[GIT_ID]["build_status"] == "built" and "build_reason" not in rows[GIT_ID]
    assert rows["local:caller"]["build"] is False and rows["local:caller"]["build_status"] is None
    (built, absent) = document["built"]
    assert built == {"id": here.id, "status": "built"}
    assert absent["id"] == "local:absent" and absent["status"] == "failed" and "not installed" in absent["reason"]
    assert _store.closure(target, "local:caller", check_builds=True).ok
    assert _store.built(_store.lookup(target, here.id)) is True  # type: ignore[arg-type]
    assert not entry.parent.exists()


def test_install_from_store_reports_a_failed_build_and_keeps_the_installation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.workflow.workflow_cli import command

    source, _ = registered(tmp_path, "a")
    target, _ = registered(tmp_path, "b")
    failing = '\n[workflow.build]\ncommand = "sh -c \'exit 3\'"\nartifacts = ["out.bin"]\n'
    workflows(source, tmp_path, build=failing, built=False)
    callee = _store.lookup(source, GIT_ID)
    assert callee is not None
    entry = _pushed(target, callee)
    argv = ["install", "--by-path", "--workspace", str(target.root), "--consume", "--build", "--from-store", str(entry)]
    context = CLIContext("httk", tmp_path)
    assert command(argv, context) == 1
    (row,) = json.loads(capsys.readouterr().out)["workflows"]
    assert row["status"] == "installed" and row["build_status"] == "failed" and row["build_reason"]
    installed = _store.lookup(target, GIT_ID)
    assert installed is not None and _store.built(installed) is False
    # The build options belong to --from-store.
    assert command(["install", "--workspace", str(target.root), "--build", "x"], context) == 2
    assert "--from-store" in capsys.readouterr().err


def _entries_tar(entries: list[_store.Installed]) -> bytes:
    """The store entries ``<dir>/{install.json,package}`` as the tar stream a push delivers."""

    import io
    import tarfile

    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for entry in entries:
            for name in ("install.json", "package"):
                archive.add(entry.directory / name, arcname=f"{entry.directory.name}/{name}")
    return data.getvalue()


def test_install_stdin_tar_installs_builds_and_discards_its_landing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import io
    import sys

    from httk.workflow.workflow_cli import command

    source, _ = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    workflows(source, tmp_path)
    capsys.readouterr()
    entries = [_store.lookup(source, GIT_ID), _store.lookup(source, "local:caller")]
    assert all(entries)
    data = _entries_tar(entries)  # type: ignore[arg-type]
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    token = "2" * 32
    argv = ["install", "--json", "--stdin-tar", "--token", token, "--workspace", target_name, "--build"]
    assert command([*argv, "--source-label", "transfer from a"], CLIContext("httk", tmp_path)) == 0
    document = json.loads(capsys.readouterr().out)
    rows = {row["id"]: row for row in document["workflows"]}
    assert rows[GIT_ID]["status"] == "installed" and rows[GIT_ID]["build_status"] == "built"
    assert rows["local:caller"]["status"] == "installed" and "built" not in document
    assert _store.closure(target, "local:caller", check_builds=True).ok
    assert landings(target) == []
    copy = _store.lookup(target, GIT_ID)
    assert copy is not None and copy.record["source"] == "transfer from a"


@pytest.mark.parametrize("bad", ["sources", "from-store", "token", "stream"])
def test_install_stdin_tar_refusals(
    tmp_path: Path, bad: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import io
    import sys

    from httk.workflow.workflow_cli import command

    target, target_name = registered(tmp_path, "b")
    data = b"not a tar stream" * 64 if bad == "stream" else b""
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    argv = ["install", "--json", "--stdin-tar", "--workspace", target_name]
    argv += {"sources": ["./x"], "from-store": ["--from-store", "x"], "token": ["--token", "../x"], "stream": []}[bad]
    assert command(argv, CLIContext("httk", tmp_path)) == 2
    assert capsys.readouterr().err
    assert landings(target) == []


def test_also_build_alone_builds_installed_workflows_in_one_call(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.workflow.workflow_cli import command

    target, target_name = registered(tmp_path, "b")
    workflows(target, tmp_path, built=False)
    argv = ["install", "--json", "--workspace", target_name, "--also-build", GIT_ID, "--also-build", "local:absent"]
    assert command(argv, CLIContext("httk", tmp_path)) == 1
    document = json.loads(capsys.readouterr().out)
    assert document["workflows"] == []
    assert document["built"][0] == {"id": GIT_ID, "status": "built"}
    assert document["built"][1]["status"] == "failed"
    assert _store.closure(target, "local:caller", check_builds=True).ok


def test_a_failed_build_at_a_remote_destination_is_reported_with_its_remedy(
    project: Path,  # noqa: F811
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    failing = '\n[workflow.build]\ncommand = "sh -c \'exit 3\'"\nartifacts = ["out.bin"]\n'
    job_id = job(near, workflows(near, tmp_path, build=failing, built=False))
    assert run(project, "--json", "--job", job_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    (callee,) = [entry for entry in outcome["workflows"] if entry["id"] == GIT_ID]
    assert callee["status"] == "unbuilt"
    assert f"httk workflow build --workspace cluster:{far_name} {GIT_ID}" in callee["reason"]
    assert _store.lookup(far, GIT_ID) is not None and landings(far) == []
