"""Job-pinned collector fallback coverage."""

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, collect, job_records
from httk.workflow import collecting as collecting_module
from httk.workflow import scaffold as scaffold_module
from httk.workflow.collecting import JobRecord
from httk.workflow.scaffold import new_job
from httk.workflow.workflow_cli import command
from test_workflow_cli_packages import _SUCCESS_RUNNER
from test_workflow_packages import _MANIFEST, _package

_DATA_MANIFEST = _MANIFEST.replace("tests.package", "tests.fallback.package").replace(
    'entry_type = "structures"\nref = "https://example.test/structures"\ndescription = "The output structure."',
    'entry_type = "records"\nref = "https://example.test/records"\ndescription = "The output record."',
)
_DATA_POSTPROCESS = """from httk.core import DataRecord


def collect(record):
    return {"relaxed_structure": DataRecord.from_value("https://example.test/energy", "total", 1.5)}
"""
_CHAIN_POSTPROCESS = """from httk.core import DataRecord


def collect(record):
    return {
        "relaxed_structure": DataRecord.from_value("https://example.test/structure", "relaxed", 1.5),
        "total_energy": DataRecord.from_value("https://example.test/energy", "energy", -2.5),
    }
"""
_FAKE_POSTPROCESS = """class FakeEntry:
    type = "fake_entries"
    id = "fake-1"


def collect(record):
    return {"relaxed_structure": FakeEntry()}
"""
_POSCAR = "silicon\n1.0\n"


def _finished(
    tmp_path: Path, *, manifest: str = _DATA_MANIFEST, collect: str = _DATA_POSTPROCESS
) -> tuple[Workspace, Path]:
    package = _package(tmp_path / "package", manifest)
    (package / "run").write_text(_SUCCESS_RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "collect.py").write_text(collect, encoding="utf-8")
    structure = tmp_path / "POSCAR"
    structure.write_text(_POSCAR, encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    new_job(workspace, package, inputs={"structure": structure})
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    return workspace, package


def _runner_path(workspace: Workspace, record: JobRecord) -> Path:
    job = record.job
    assert isinstance(job, Mapping)
    runner = job["runner"]
    assert isinstance(runner, Mapping)
    return workspace.runner_store_path(str(runner["path"]))


def test_collect_degraded_filters_to_the_degraded_lines_only(tmp_path: Path, capsys) -> None:
    """--degraded prints only degraded lines while the summary counts the whole sweep (item 9)."""

    workspace, _ = _finished(tmp_path)
    context = CLIContext("httk", workspace.root)
    name = register_ws(context, workspace.root, "degraded")

    # Without --allow-job-collector the pinned-tree collector is not run, so the
    # one job degrades: --degraded therefore prints its single line.
    assert command(["collect", name, "--degraded"], context) == 1
    lines = capsys.readouterr().out.splitlines()
    summary = json.loads(lines[-1])
    assert summary["format"] == "httk-workflow-collect-summary" and summary["degraded"] == 1
    assert len(lines) == 2 and json.loads(lines[0])["missing_collector"] is not None

    # Allowed, the job collects cleanly, so --degraded filters it out entirely
    # while the summary still reports the whole sweep.
    assert command(["collect", name, "--degraded", "--allow-job-collector"], context) == 0
    lines = capsys.readouterr().out.splitlines()
    summary = json.loads(lines[-1])
    assert summary["collected"] == 1 and summary["degraded"] == 0
    assert len(lines) == 1


def test_collect_uses_a_pinned_tree_collector_when_allowed(tmp_path: Path) -> None:
    workspace, package = _finished(tmp_path)
    record = next(job_records(workspace))

    without = next(collect(workspace))
    assert without.outputs == {} and without.missing_collector is not None
    assert "allow_job_collector=True" in without.missing_collector

    with_fallback = next(collect(workspace, allow_job_collector=True))
    assert with_fallback.missing_collector is None
    assert set(with_fallback.outputs) == {"relaxed_structure"}
    assert getattr(with_fallback.outputs["relaxed_structure"], "id", None)
    runner = record.job["runner"]
    assert isinstance(runner, Mapping)
    assert runner["sha256"] == tree_digest(package)


def test_fallback_uses_pinned_manifest_curation_for_output_products(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = (
        _DATA_MANIFEST
        + '''

[workflow.outputs.total_energy]
entry_type = "records"
product_of = "relaxed_structure"
role = "total_energy"
'''
    )
    workspace, _ = _finished(tmp_path, manifest=manifest, collect=_CHAIN_POSTPROCESS)
    scaffold_module._WORKFLOW_PROVIDERS.pop("tests.fallback.package", None)

    item = next(collect(workspace, allow_job_collector=True))

    assert item.missing_collector is None
    assert len(item.products) == 1
    product = item.products[0]
    assert product.source_type == "_httk_records"
    assert product.source_id == cast(Any, item.outputs["relaxed_structure"]).id
    assert product.target_type == "_httk_records"
    assert product.target_id == cast(Any, item.outputs["total_energy"]).id
    assert product.label == "total_energy"


def test_collect_degrades_a_tampered_pinned_tree_loudly(tmp_path: Path) -> None:
    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    runner = _runner_path(workspace, record)
    runner.chmod(0o755)
    (runner / "collect.py").chmod(0o644)
    (runner / "collect.py").write_text(_DATA_POSTPROCESS + "\n# tampered\n", encoding="utf-8")

    item = next(collect(workspace, allow_job_collector=True))
    assert item.outputs == {}
    assert item.missing_collector is not None
    assert "pinned runner tree was modified" in item.missing_collector


def test_collect_degrades_a_pinned_tree_with_the_wrong_manifest_id(tmp_path: Path) -> None:
    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    runner = _runner_path(workspace, record)
    runner.chmod(0o755)
    (runner / "httk_workflow.toml").chmod(0o644)
    manifest = (
        (runner / "httk_workflow.toml")
        .read_text(encoding="utf-8")
        .replace("tests.fallback.package", "tests.other.package")
    )
    (runner / "httk_workflow.toml").write_text(manifest, encoding="utf-8")

    item = next(collect(workspace, allow_job_collector=True))
    assert item.outputs == {}
    assert item.missing_collector is not None
    # Digest verification deliberately precedes manifest parsing: a modified
    # manifest is tampering, not a trusted wrong-id diagnostic.
    assert "pinned runner tree was modified" in item.missing_collector


def test_collect_refuses_a_file_runner_for_the_job_fallback(tmp_path: Path) -> None:
    runner = tmp_path / "runner.py"
    runner.write_text(
        """#!/usr/bin/env python3
from httk.workflow import Runner
run = Runner("tests.fallback.file")
@run.step
def start(attempt):
    attempt.succeed()
if __name__ == "__main__":
    raise SystemExit(run.main())
""",
        encoding="utf-8",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    new_job(workspace, runner)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    item = next(collect(workspace, allow_job_collector=True))
    assert item.missing_collector is not None
    assert "requires a directory runner tree" in item.missing_collector


def test_collect_degrades_an_unknown_language_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    scaffold_module._WORKFLOW_PROVIDERS.pop(str(record.job["workflow"]), None)
    parameters = record.job["parameters"]
    assert isinstance(parameters, Mapping)
    record = replace(
        record,
        job={
            **record.job,
            "parameters": {**parameters, "workflow_language": "missing", "workflow_realization": "language"},
        },
    )
    monkeypatch.setattr(collecting_module, "job_records", lambda *_args, **_kwargs: iter((record,)))

    item = next(collecting_module.collect(workspace))
    assert item.outputs == {}
    assert item.missing_collector is not None
    assert "missing" in item.missing_collector


@pytest.mark.parametrize("parameter", ("workflow_language", "workflow_collect"))
def test_native_parameters_do_not_trigger_language_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, parameter: str
) -> None:
    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    workflow_id = str(record.job["workflow"])
    monkeypatch.delitem(scaffold_module._WORKFLOW_PROVIDERS, workflow_id, raising=False)
    parameters = record.job["parameters"]
    assert isinstance(parameters, Mapping)
    record = replace(record, job={**record.job, "parameters": {parameter: "cwl"}})
    monkeypatch.setattr(collecting_module, "job_records", lambda *_args, **_kwargs: iter((record,)))

    without = next(collecting_module.collect(workspace))
    assert without.outputs == {}
    assert without.missing_collector is not None
    assert "no provider" in without.missing_collector

    with_fallback = next(collecting_module.collect(workspace, allow_job_collector=True))
    assert with_fallback.missing_collector is None
    assert set(with_fallback.outputs) == {"relaxed_structure"}


def test_collect_into_round_trips_records_and_runs_when_data_is_available(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.data")
    pytest.importorskip("httk.atomistic")
    from httk.core import DataRecord, DataRecordEntry, RunEntry
    from httk.data.db import Database, SqlStore

    workspace, _ = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "collect-into")
    store_path = tmp_path / "results.sqlite"
    assert (
        command(
            ["collect", workspace_name, "--allow-job-collector", "--into", str(store_path)],
            context,
        )
        == 0
    )
    lines = capsys.readouterr().out.splitlines()
    report = json.loads(lines[0])
    summary = json.loads(lines[-1])
    assert summary["format"] == "httk-workflow-collect-summary"
    assert summary["collected"] == 1 and summary["degraded"] == 0 and summary["storage_errors"] == 0
    assert report["stored"]["entries"]
    assert report["stored"]["run"]
    with Database.sqlite(store_path) as database:
        store = SqlStore(database)
        entry = store.fetch_entry(DataRecordEntry, report["stored"]["entries"][0])
        run = store.fetch_entry(RunEntry, report["stored"]["run"])
    assert isinstance(entry, DataRecord)
    assert run is not None


def test_collect_into_twice_is_idempotent(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.data")
    pytest.importorskip("httk.atomistic")
    from httk.core import RunEntry
    from httk.data.db import Database, SqlStore

    workspace, _ = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "collect-twice")
    store_path = tmp_path / "results.sqlite"
    argv = ["collect", workspace_name, "--allow-job-collector", "--into", str(store_path)]

    assert command(argv, context) == 0
    first = json.loads(capsys.readouterr().out.splitlines()[0])["stored"]
    # Re-collection is stateless and de-duplicated on the stable ids, so a second
    # store into the same file exits clean and stores the very same entries.
    assert command(argv, context) == 0
    second = json.loads(capsys.readouterr().out.splitlines()[0])["stored"]
    assert first == second

    # The run is present exactly once — the re-store neither errored nor forked it.
    with Database.sqlite(store_path) as database:
        run = SqlStore(database).fetch_entry(RunEntry, first["run"])
    assert run is not None


def test_collect_into_a_store_with_a_different_layout_teaches(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.data")
    pytest.importorskip("httk.atomistic")
    from httk.core.register import resolve_entry_family, resolve_entry_record
    from httk.data.db import Database, SqlStore

    workspace, _ = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "collect-mismatch")
    store_path = tmp_path / "results.sqlite"
    # A store created for an unrelated entry-type layout cannot absorb this sweep.
    with Database.sqlite(store_path) as database:
        SqlStore(database, entry_records={resolve_entry_family("runs"): (resolve_entry_record("core-run"),)})

    assert command(["collect", workspace_name, "--allow-job-collector", "--into", str(store_path)], context) == 2
    err = capsys.readouterr().err
    assert str(store_path) in err
    assert "different set of entry types" in err
    assert "Collect into a new store file" in err


def test_collect_into_reports_an_unknown_entry_type_per_job(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.data")
    pytest.importorskip("httk.atomistic")

    manifest = _DATA_MANIFEST.replace('entry_type = "records"', 'entry_type = "fake_entries"')
    workspace, _ = _finished(tmp_path, manifest=manifest, collect=_FAKE_POSTPROCESS)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "collect-unknown")
    # A storage failure is a nonzero exit under the honest exit-code policy: the
    # per-job storage_error is reported, and the sweep no longer claims success.
    assert (
        command(
            ["collect", workspace_name, "--allow-job-collector", "--into", str(tmp_path / "unknown.sqlite")],
            context,
        )
        == 1
    )
    lines = capsys.readouterr().out.splitlines()
    report = json.loads(lines[0])
    assert "fake_entries" in report["storage_error"]
    summary = json.loads(lines[-1])
    assert summary["storage_errors"] == 1 and summary["collected"] == 1


def test_malformed_provenance_document_degrades_only_its_job(tmp_path: Path) -> None:
    from httk.workflow.models import JOB_STATE_DIRECTORY

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    declarations = record.payload / JOB_STATE_DIRECTORY / "declarations"
    declarations.mkdir(parents=True, exist_ok=True)
    # An observed provenance edge missing its 'id' makes run_record raise; the
    # sweep must degrade this job rather than abort on runner-written data.
    (declarations / "provenance.json").write_text(json.dumps({"inputs": {"x": {"type": "a"}}}), encoding="utf-8")

    items = list(collect(workspace, allow_job_collector=True))

    assert len(items) == 1
    assert items[0].missing_collector is not None
    assert "provenance declaration is unusable" in items[0].missing_collector
    assert "declarations/provenance.json" in items[0].missing_collector


def test_degraded_job_reports_all_declared_roles_as_unfulfilled(tmp_path: Path) -> None:
    workspace, _ = _finished(tmp_path)
    without = next(collect(workspace))
    assert without.missing_collector is not None
    # A degraded job produced nothing, so every declared role is unfulfilled and
    # can be told apart from a complete collection that declared no outputs.
    assert without.unfulfilled == ("relaxed_structure",)


def test_unlinked_product_of_is_reported_but_unfulfilled_roles_are_not(tmp_path: Path) -> None:
    # relaxed_structure is a product of the input role initial_structure, which
    # has no edge in the observed provenance, so its skipped link surfaces.
    # total_energy also declares a product but is never produced: it belongs in
    # unfulfilled, and must NOT be double-reported as an unlinked product.
    manifest = (
        _DATA_MANIFEST
        + '''

[workflow.outputs.total_energy]
entry_type = "records"
product_of = "relaxed_structure"
role = "total_energy"
'''
    )
    workspace, _ = _finished(tmp_path, manifest=manifest)
    scaffold_module._WORKFLOW_PROVIDERS.pop("tests.fallback.package", None)

    item = next(collect(workspace, allow_job_collector=True))

    assert item.missing_collector is None
    assert item.products == ()
    assert item.unfulfilled == ("total_energy",)
    assert item.products_unlinked == (
        "relaxed_structure -> initial_structure (source edge absent in observed provenance)",
    )
