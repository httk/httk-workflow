"""Embedded workflows after the run: collection from snapshots, set_workflow, transfers and inspection."""

import json
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from httk.core import DataRecord
from httk.core.cli import CLIContext

import v3_helpers as v3
from attempt_fixtures import package
from conftest import register_ws
from httk.workflow import Workspace, _bundles, _kernel, _requests, _store, collect, job_records, removal, scaffold
from httk.workflow._embedded import embedded_digest
from httk.workflow._job import JobDefinition
from httk.workflow.introspection import describe_job, explain_job, render_job
from httk.workflow.packages import load_workflow_package
from httk.workflow.precheck import precheck_jobs
from httk.workflow.provenance import _record_definition_uri
from httk.workflow.scaffold import JobItem
from httk.workflow.workflow_cli import command
from httk.workflow.workflow_cli._describe import _users
from test_bundles import family, write_bundle
from test_cli_job_eject_adopt import registered
from test_set_workflow import _identity  # noqa: F401  (the fixture)

_NAME = "tests.embedded.collect"
_MANIFEST = """
[workflow.collect]
file = "collect.py"

[workflow.outputs.energy]
entry_type = "records"
ref = "https://example.test/records"
role = "energy"
"""
#: Counts its calls per module (a module shared between two jobs would answer 2.0 for the second), and adds 100
#: when it runs from anywhere but a snapshot (``EMBEDDED_SNAPSHOTS``, when set).
_HOOK = """import os

from httk.core import DataRecord

CALLS = []


def collect(record):
    CALLS.append(record.job_id)
    elsewhere = not __file__.startswith(os.environ.get("EMBEDDED_SNAPSHOTS", ""))
    value = float(len(CALLS)) + (100.0 if elsewhere else 0.0)
    return {"energy": DataRecord.from_value("https://example.test/energy", "total", value)}
"""
#: An executable collector that answers only when it runs in a snapshot (its working directory, the tree).
_EXEC_HOOK = f"""#!/usr/bin/env python3
import os, sys
sys.path.insert(0, {str(Path(__file__).parents[1] / "src")!r})
from httk.workflow.hookapi import collect_main
if not os.getcwd().startswith(os.environ["EMBEDDED_SNAPSHOTS"]) or not os.path.isfile("collect"):
    sys.exit(3)
collect_main(lambda record: {{"energy": {{"value": 1.0}}}})
"""
PINNED = "git+https://example.org/w.git@" + "c" * 40


def _energy(value: float) -> DataRecord:
    return DataRecord.from_value("https://example.test/energy", "total", value)


def _package(root: Path, hook: str = _HOOK, name: str = _NAME) -> Path:
    source = package(root, name, list(v3.STEPS), runner=v3.RUNNER, extra=_MANIFEST)
    (source / "collect.py").write_text(hook, encoding="utf-8")
    return source


@pytest.fixture()
def snapshots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Where collection snapshots go: a private temporary directory, empty afterwards."""

    directory = tmp_path / "tmp"
    directory.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(directory))
    monkeypatch.setenv("EMBEDDED_SNAPSHOTS", str(directory))
    return directory


def _from_snapshots(directory: Path) -> list[str]:
    """The modules still loaded from below *directory*."""

    return [
        name for name, module in sys.modules.items() if str(getattr(module, "__file__", "")).startswith(str(directory))
    ]


def _finished(tmp_path: Path, count: int = 1, source: Path | None = None) -> tuple[Workspace, list[_kernel.JobRef]]:
    workspace = v3.workspace(tmp_path / "workspace")
    items: list[JobItem] = [{"tag": f"j{index}", "placement": f"project/{index}"} for index in range(count)]
    source = source or _package(tmp_path / "pkg")
    jobs = list(
        scaffold.new_jobs(workspace, source, items, embed_workflow=True, parameters={"script": {"start": "succeed"}})
    )
    v3.run(workspace)
    refs = [v3.find(workspace, job.job_id) for job in jobs]
    assert [ref.state for ref in refs] == ["succeeded"] * count
    return workspace, refs


def test_embedded_jobs_collect_each_from_its_own_verified_snapshot(tmp_path: Path, snapshots: Path) -> None:
    workspace, refs = _finished(tmp_path, 2)
    digests = [embedded_digest(ref.path / "workflow") for ref in refs]
    collected = list(collect(workspace, allow_job_collector=True))
    assert [item.missing_collector for item in collected] == [None, None]
    # Identical trees, yet each job's hook ran as a module of its own snapshot.
    assert [item.outputs["energy"] for item in collected] == [_energy(1.0), _energy(1.0)]
    assert {item.record.job["embedded"]["tree_sha256"] for item in collected} == set(digests)  # type: ignore[index]
    assert [embedded_digest(ref.path / "workflow") for ref in refs] == digests
    assert not list((refs[0].path / "workflow").rglob("__pycache__"))
    assert list(snapshots.iterdir()) == [] and _from_snapshots(snapshots) == []


def test_an_executable_collector_runs_in_its_snapshot(tmp_path: Path, snapshots: Path) -> None:
    source = package(
        tmp_path / "exec",
        _NAME,
        list(v3.STEPS),
        runner=v3.RUNNER,
        extra=_MANIFEST.replace("collect.py", "collect").replace('ref = "https://example.test/records"\n', ""),
    )
    (source / "collect").write_text(_EXEC_HOOK, encoding="utf-8")
    (source / "collect").chmod(0o755)
    workspace, (ref,) = _finished(tmp_path, source=source)
    digest = embedded_digest(ref.path / "workflow")
    (item,) = collect(workspace, allow_job_collector=True)
    assert item.missing_collector is None and item.collector_exit_status is None and "energy" in item.outputs
    assert embedded_digest(ref.path / "workflow") == digest and list(snapshots.iterdir()) == []


def test_an_unlocked_job_whose_tree_could_not_be_digested_is_not_collected(tmp_path: Path, snapshots: Path) -> None:
    workspace, (ref,) = _finished(tmp_path)
    with v3.cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, v3.find(workspace, ref.job_id), "unseal", "t") is None
        assert removal.request_now(workspace, owner, v3.find(workspace, ref.job_id), "unlock", "t") is None
    tree = v3.find(workspace, ref.job_id).path / "workflow"
    (tree / "__pycache__").mkdir()  # undigestable: the unlocked job launches it anyway, pinned null
    with v3.cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, v3.find(workspace, ref.job_id), "restart", "t") is None
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "succeeded" and v3.state_of(done).workflow_pin["tree_sha256"] is None  # type: ignore[index]
    (item,) = collect(workspace, allow_job_collector=True, force=True)
    assert item.missing_collector == (
        "the job's workflow/ could not be digested at launch (its launch pinned no digest); "
        "fix it and restart before collecting"
    )


def test_a_tree_edited_after_the_run_is_never_collected_from(tmp_path: Path, snapshots: Path) -> None:
    workspace, (ref,) = _finished(tmp_path)
    pinned = v3.state_of(ref).workflow_pin
    assert pinned is not None
    (ref.path / "workflow" / "collect.py").write_text(_HOOK.replace("len(CALLS)", "7"), encoding="utf-8")
    actual = embedded_digest(ref.path / "workflow")
    (item,) = collect(workspace, allow_job_collector=True)
    assert item.missing_collector == (
        f"embedded workflow modified since the job was created: digest {actual} does not match pinned "
        f"{pinned['tree_sha256']}"
    )
    assert list(snapshots.iterdir()) == []


def test_an_embedded_job_needs_the_job_collector_and_never_a_registered_namesake(tmp_path: Path) -> None:
    workspace, (ref,) = _finished(tmp_path)
    workflow_id = JobDefinition.from_path(ref.path / "job.json").workflow_id
    (item,) = collect(workspace)
    assert item.missing_collector == (
        f"no provider for workflow {workflow_id!r}; pass allow_job_collector=True to use the job's installed "
        "workflow package"
    )
    namesake = _package(tmp_path / "namesake", _HOOK.replace("len(CALLS)", "9"))
    provider = load_workflow_package(namesake)
    try:
        (item,) = collect(workspace)
        assert item.missing_collector is not None  # the registered namesake answers nothing for it
        (item,) = collect(workspace, allow_job_collector=True)
        assert item.outputs["energy"] == _energy(1.0)
    finally:
        scaffold._WORKFLOW_PROVIDERS.pop(provider.workflow_id, None)


def test_the_definition_uri_is_the_embedded_source(tmp_path: Path) -> None:
    workspace, _refs = _finished(tmp_path)
    (record,) = job_records(workspace, states=("succeeded",))
    assert record.job["embedded"]["source"] is None  # type: ignore[index]
    assert _record_definition_uri(record) is None
    embedded = {**record.job["embedded"], "source": PINNED}  # type: ignore[dict-item]
    assert _record_definition_uri(replace(record, job={**record.job, "embedded": embedded})) == PINNED


@pytest.mark.usefixtures("_identity")
def test_set_workflow_refuses_an_embedded_job(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    job = scaffold.new_job(workspace, _package(tmp_path / "pkg"), embed_workflow=True, parameters={"script": {}})
    ref = v3.find(workspace, job.job_id)  # ready: set_workflow applies to it
    other = v3.install(workspace, tmp_path / "other", name="other")
    message = "an embedded workflow cannot be switched; unlock the job and edit workflow/ instead"
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, f"embedded-{os.getpid()}")
    argv = ["job", "request", "set_workflow", "--workspace", name, "--reason", "r", "--workflow", other.id, ref.job_id]
    assert command(argv, context) == 2
    assert message in capsys.readouterr().err
    path = _requests.post(
        workspace,
        action="set_workflow",
        job_id=ref.job_id,
        placement=JobDefinition.from_path(ref.path / "job.json").placement,
        operator="cli",
        reason="r",
        workflow=other.id,
    )
    with v3.cli_owner(workspace) as owner:
        (outcome,) = removal.serve_requests(workspace, [(v3.find(workspace, ref.job_id), path)], owner=owner)
    assert outcome.status != "applied" and outcome.explanation == message


@pytest.mark.parametrize("index", [0, 1])
def test_an_untrusted_bundle_never_holds_an_embedded_workflow(tmp_path: Path, index: int) -> None:
    embedded = scaffold.new_job(
        v3.workspace(tmp_path / "elsewhere"), _package(tmp_path / "pkg"), embed_workflow=True, parameters={"script": {}}
    )
    foreign = JobDefinition.from_path(embedded.payload / "job.json").as_mapping()
    jobs = family()
    jobs[index] = {**jobs[index], "workflow": foreign["workflow"], "embedded": foreign["embedded"]}
    write_bundle(tmp_path / "bundle", jobs)
    with pytest.raises(_bundles.BundleError, match="an untrusted bundle may not hold an embedded workflow"):
        _bundles.validate_bundle(tmp_path / "bundle", untrusted=True)
    assert _bundles.validate_bundle(tmp_path / "bundle", untrusted=False) is not None


def test_an_embedded_job_travels_with_its_workflow(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    job = scaffold.new_job(
        source, _package(tmp_path / "pkg"), embed_workflow=True, parameters={"script": {"start": "succeed"}}
    )
    context = CLIContext("httk", tmp_path)
    assert command(["job", "eject", "--workspace", source_name, job.job_id, "out", "--json"], context) == 0
    bundle = Path(json.loads(capsys.readouterr().out)["destination"])
    assert command(["job", "adopt", "--workspace", target_name, str(bundle)], context) == 0
    assert "not installed" not in capsys.readouterr().err
    v3.run(target)
    done = v3.find(target, job.job_id)
    assert done.state == "succeeded" and (v3.state_of(done).workflow_pin or {}).get("source") == "embedded"
    assert _store.list_installed(target) == []


def test_inspection_of_an_embedded_job(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    callee = v3.install(workspace, tmp_path / "callee", name="tests.callee")
    source = _package(tmp_path / "pkg")
    manifest = source / "httk_workflow.toml"
    manifest.write_text(manifest.read_text(encoding="utf-8") + '\n[workflow.calls]\nhelper = "tests.callee"\n')
    job = scaffold.new_job(workspace, source, embed_workflow=True, parameters={"script": {"start": "succeed"}})
    ref = v3.find(workspace, job.job_id)
    definition = JobDefinition.from_path(ref.path / "job.json")
    assert definition.embedded is not None
    sha12 = str(definition.embedded["tree_sha256"])[:12]
    report = describe_job(workspace, ref)
    assert report["embedded"]["calls"] == {"helper": callee.id}
    assert f"; embedded, digest {sha12}" in render_job(report)
    checks = {check.name: check for check in explain_job(workspace, ref).checks}
    assert checks["embedded workflow"].satisfied is True
    assert checks["installed workflow"].detail == "its calls are installed and built here"
    (finding,) = precheck_jobs(workspace)
    assert finding["runner"] == {"status": "ok", "ok": True}
    # The uninstall check counts the embedded workflow's callee as used.
    assert _users(workspace, callee.id) == [ref.job_key]

    (ref.path / "workflow" / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    checks = {check.name: check for check in explain_job(workspace, ref).checks}
    assert checks["embedded workflow"].satisfied is False
    assert str(checks["embedded workflow"].detail).startswith("workflow/ changed since the job was created")
    (finding,) = precheck_jobs(workspace)
    runner: Any = finding["runner"]
    assert runner["status"] == "problem" and runner["problem"].startswith("workflow/ changed since the job was created")
