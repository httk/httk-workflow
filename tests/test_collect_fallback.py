"""Job-pinned collector fallback coverage."""

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, collect, job_records
from httk.workflow import collecting as collecting_module
from httk.workflow import scaffold as scaffold_module
from httk.workflow._util import tree_digest
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
    report = json.loads(capsys.readouterr().out)
    assert report["stored"]["entries"]
    assert report["stored"]["run"]
    with Database.sqlite(store_path) as database:
        store = SqlStore(database)
        entry = store.fetch_entry(DataRecordEntry, report["stored"]["entries"][0])
        run = store.fetch_entry(RunEntry, report["stored"]["run"])
    assert isinstance(entry, DataRecord)
    assert run is not None


def test_collect_into_reports_an_unknown_entry_type_per_job(tmp_path: Path, capsys) -> None:
    pytest.importorskip("httk.data")
    pytest.importorskip("httk.atomistic")

    manifest = _DATA_MANIFEST.replace('entry_type = "records"', 'entry_type = "fake_entries"')
    workspace, _ = _finished(tmp_path, manifest=manifest, collect=_FAKE_POSTPROCESS)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "collect-unknown")
    assert (
        command(
            ["collect", workspace_name, "--allow-job-collector", "--into", str(tmp_path / "unknown.sqlite")],
            context,
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert "fake_entries" in report["storage_error"]
