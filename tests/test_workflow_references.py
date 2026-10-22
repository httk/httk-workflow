"""``[workflow.references.<LABEL>]``: declared literature references, collected and stored as cited entries."""

import json
import logging
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core import DataRecord, ReferenceRecord, Run
from httk.core.cli import CLIContext

from conftest import configure_identity
from httk.workflow import job_records
from httk.workflow.collecting import _assemble_collected, _degraded_job
from httk.workflow.packages import load_workflow_package, parse_workflow_manifest
from httk.workflow.provenance import run_record
from httk.workflow.storing import _collected_mapping
from httk.workflow.workflow_cli import command
from test_postprocess_store import _collect, _lines, _setup
from test_recognize_packages import _MANIFEST as _RECOGNIZE_MANIFEST
from test_recognize_packages import _package as _recognize_package
from test_workflow_packages import _MANIFEST, _package

_PAW = """
[workflow.references.paw]
doi = "10.1103/PhysRevB.59.1758"
title = "From ultrasoft pseudopotentials to the projector augmented-wave method"
authors = [{name = "G. Kresse"}, {name = "D. Joubert", firstname = "D.", lastname = "Joubert"}]
journal = "Physical Review B"
volume = "59"
pages = "1758"
year = "1999"
"""
_PAW_TABLE: dict[str, Any] = {
    "doi": "10.1103/PhysRevB.59.1758",
    "title": "From ultrasoft pseudopotentials to the projector augmented-wave method",
    "authors": [{"name": "G. Kresse"}, {"name": "D. Joubert", "firstname": "D.", "lastname": "Joubert"}],
    "journal": "Physical Review B",
    "volume": "59",
    "pages": "1758",
    "year": "1999",
}


def test_references_parse_onto_the_provider_and_describe(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    package = _package(tmp_path / "package", _MANIFEST + _PAW)
    provider = parse_workflow_manifest(package)
    assert provider.references == {"paw": _PAW_TABLE}
    assert parse_workflow_manifest(_package(tmp_path / "plain")).references == {}

    assert command(["describe", "--json", str(package)], CLIContext("httk", tmp_path)) == 0
    assert json.loads(capsys.readouterr().out)[0]["references"] == {"paw": _PAW_TABLE}
    assert command(["describe", str(package)], CLIContext("httk", tmp_path)) == 0
    text = capsys.readouterr().out
    assert f"references:\n  paw: {_PAW_TABLE['doi']} — {_PAW_TABLE['title']}\n" in text


def test_recognize_packages_declare_references(tmp_path: Path) -> None:
    provider = load_workflow_package(_recognize_package(tmp_path / "p", _RECOGNIZE_MANIFEST + _PAW), register=False)
    assert provider.references == {"paw": _PAW_TABLE}


@pytest.mark.parametrize(
    ("table", "message"),
    [
        ('doi = "10.1/x"\nbogus = "y"', r"unknown key \[workflow\.references\.paw\.bogus\]"),
        ('doi = "10.1/x"\nimmutable_id = "y"', r"unknown key \[workflow\.references\.paw\.immutable_id\]"),
        ('doi = "10.1/x"\nlast_modified = "y"', r"unknown key \[workflow\.references\.paw\.last_modified\]"),
        ('doi = "10.1/x"\nid = "y"', r"unknown key \[workflow\.references\.paw\.id\]"),
        ('doi = "10.1/x"\ntype = "references"', r"unknown key \[workflow\.references\.paw\.type\]"),
        (
            'doi = "10.1/x"\nyear = 1999',
            r'\[workflow\.references\.paw\]\.year must be a nonempty string \(quote numbers, e\.g\. "1999"\)',
        ),
        ('doi = ""', r"\[workflow\.references\.paw\]\.doi must be a nonempty string\Z"),
        ('doi = "10.1/x"\nauthors = []', r"\[workflow\.references\.paw\]\.authors must be a nonempty list of tables"),
        ('doi = "10.1/x"\neditors = "A. B."', r"\[workflow\.references\.paw\]\.editors must be a nonempty list"),
        ('doi = "10.1/x"\nauthors = ["A. B."]', r"\[workflow\.references\.paw\]\.authors\[0\] must be a table"),
        ('doi = "10.1/x"\nauthors = [{firstname = "A."}]', r"\.authors\[0\]\.name is required"),
        ('doi = "10.1/x"\nauthors = [{name = ""}]', r"\.authors\[0\]\.name must be a nonempty string"),
        ('doi = "10.1/x"\nauthors = [{name = "A", lastname = 1}]', r"\.authors\[0\]\.lastname must be a string"),
        (
            'doi = "10.1/x"\nauthors = [{name = "A", email = "a@b"}]',
            r"unknown key \[workflow\.references\.paw\.authors\[0\]\.email\]",
        ),
        ('journal = "J"', r"\[workflow\.references\.paw\] needs at least one of doi, url or title"),
    ],
)
def test_invalid_references_are_rejected(tmp_path: Path, table: str, message: str) -> None:
    manifest = _MANIFEST + f"\n[workflow.references.paw]\n{table}\n"
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(_package(tmp_path / "package", manifest))


def test_references_must_be_tables(tmp_path: Path) -> None:
    manifest = _MANIFEST.replace("[workflow.runner]", 'references = "paw"\n\n[workflow.runner]')
    with pytest.raises(ValueError, match=r"\[workflow\.references\] must be a table"):
        parse_workflow_manifest(_package(tmp_path / "a", manifest))
    manifest = _MANIFEST.replace("[workflow.runner]", 'references = {paw = "10.1/x"}\n\n[workflow.runner]')
    with pytest.raises(ValueError, match=r"\[workflow\.references\.paw\] must be a table"):
        parse_workflow_manifest(_package(tmp_path / "b", manifest))


def test_collected_jobs_carry_references_and_degraded_jobs_none(tmp_path: Path) -> None:
    _package_dir, provider, ws = _setup(tmp_path, manifest=_manifest_with_references())
    (record,) = job_records(ws)
    run = run_record(record)
    collected = _assemble_collected(
        "job", record, provider, run, {"relaxed_structure": DataRecord.from_value("https://example.test/e", "e", 1.0)}
    )
    assert collected.references == {"paw": ReferenceRecord(**_PAW_TABLE)}
    assert _collected_mapping(collected)["references"] == ["paw"]
    degraded = _degraded_job(record, provider, run, "no collector")
    assert degraded.references == {}
    assert "references" not in _collected_mapping(degraded)


def _manifest_with_references() -> str:
    from test_postprocess_store import _MANIFEST as _STORE_MANIFEST

    return _STORE_MANIFEST + _PAW


def _by_id(store: Any, cls: type, entry_id: str) -> Any:
    searcher = store.searcher(only_latest=True)
    variable = searcher.variable(cls)
    searcher.add(variable.id == entry_id)
    return searcher.results(entry=variable).first().entry


@pytest.mark.parametrize("product_of", [True, False], ids=["deferred-output", "plain-output"])
def test_collect_into_stores_and_links_references(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], product_of: bool
) -> None:
    configure_identity()
    manifest = _manifest_with_references()
    if not product_of:
        manifest = manifest.replace('product_of = "initial_structure"\n', "")
    _package_dir, _provider, ws = _setup(tmp_path, jobs=2, manifest=manifest, unique=True)
    store_path = tmp_path / "store.sqlite"
    assert _collect(ws, store_path) == 0
    reports = [line for line in _lines(capsys) if line.get("format") == "httk-workflow-collected"]
    assert len(reports) == 2
    assert all(report["references"] == ["paw"] for report in reports)
    # One reference entry: the second job dedups onto the first job's.
    (reference_id,) = {reference for report in reports for reference in report["stored"]["references"]}

    # Collecting again adds no reference and no link.
    assert _collect(ws, store_path) == 0
    capsys.readouterr()

    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(store_path) as database:
        store = SqlStore(database)
        searcher = store.searcher(only_latest=True)
        (reference,) = [row.ref for row in searcher.results(ref=searcher.variable(ReferenceRecord))]
        assert reference.id == reference_id
        assert reference.authors[1].lastname == "Joubert"
        for report in reports:
            run = _by_id(store, Run, report["stored"]["run"])
            (record_id,) = report["stored"]["entries"]
            record = _by_id(store, DataRecord, record_id)
            for source in (run, record):
                assert [linked.id for linked in store.linked(source, "references")] == [reference_id]
    from httk.atomistic.storage.records import UnitcellStructureRecord
    from httk.store.backend.schema import resolve_schema  # pyright: ignore[reportMissingImports]

    link_tables = [
        spec.table_name
        for cls in (Run, DataRecord, UnitcellStructureRecord)
        for spec in resolve_schema(cls).links
        if spec.name == "references"
    ]
    with sqlite3.connect(store_path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        rows = {
            name: connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            for name in link_tables
            if name in tables
        }
    # Each stored run and record cites the reference by one link row: the staged
    # structure input does not cite, and collecting again added nothing.
    citing = {report["stored"]["run"] for report in reports} | {report["stored"]["entries"][0] for report in reports}
    assert sum(rows.values()) == len(citing), rows


def test_stored_references_are_served_as_relationships(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("starlette")
    serve = pytest.importorskip("httk.serve.optimade")
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]
    from starlette.testclient import TestClient

    _package_dir, _provider, ws = _setup(tmp_path, manifest=_manifest_with_references())
    store_path = tmp_path / "store.sqlite"
    assert _collect(ws, store_path, "--no-id-ledger") == 0
    (report,) = [line for line in _lines(capsys) if line.get("format") == "httk-workflow-collected"]
    (reference_id,) = report["stored"]["references"]
    (record_id,) = report["stored"]["entries"]

    with Backend.sqlite(store_path) as database:
        app = serve.create_asgi_app(serve.adapter_from_store(SqlStore(database)), baseurl="http://testserver")
        with TestClient(app, base_url="http://testserver") as client:
            for path in (f"/v1/_httk_runs/{report['stored']['run']}", f"/v1/_httk_records/{record_id}"):
                response = client.get(path)
                assert response.status_code == 200, response.text
                cited = response.json()["data"]["relationships"]["references"]["data"]
                assert {"type": "references", "id": reference_id} in [
                    {"type": item["type"], "id": item["id"]} for item in cited
                ]
            served = client.get(f"/v1/references/{reference_id}")
            assert served.status_code == 200, served.text
            assert served.json()["data"]["attributes"]["doi"] == "10.1103/PhysRevB.59.1758"


def test_package_without_references_stores_none(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _package_dir, _provider, ws = _setup(tmp_path)
    store_path = tmp_path / "store.sqlite"
    assert _collect(ws, store_path, "--no-id-ledger") == 0
    (report,) = [line for line in _lines(capsys) if line.get("format") == "httk-workflow-collected"]
    assert "references" not in report
    assert "references" not in report["stored"]
    from httk.store.backend.schema import resolve_schema  # pyright: ignore[reportMissingImports]

    with sqlite3.connect(store_path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert resolve_schema(ReferenceRecord).table_name not in tables


def test_an_output_without_the_link_is_stored_uncited_and_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from httk.core.register import entries, register_entry_record
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    from httk.workflow.collecting import collect
    from httk.workflow.storing import store_collected
    from test_calculations import XRecord

    monkeypatch.setattr(entries, "_entry_records", dict(entries._entry_records))
    register_entry_record(
        name="test-x-record",
        family="records",
        record="test_calculations:XRecord",
        definition_id="https://schemas.httk.org/defs/v0.1/entrytypes/records",
    )
    _package_dir, _provider, ws = _setup(tmp_path, manifest=_manifest_with_references())
    (item,) = collect(ws, allow_job_collector=True)
    item = replace(item, outputs={**item.outputs, "extra": XRecord("third-party")})
    store_path = tmp_path / "store.sqlite"
    with caplog.at_level(logging.WARNING, logger="httk.workflow.storing"):
        (report,) = store_collected([item], str(store_path), id_base="httk.probe")

    stored = cast(dict[str, Any], report["stored"])
    assert stored["references_uncited"] == ["extra"]
    assert "'extra' (XRecord)" in caplog.text and "declares no 'references' weak link" in caplog.text
    (reference_id,) = stored["references"]
    with Backend.sqlite(store_path) as database:
        store = SqlStore(database)
        run = _by_id(store, Run, stored["run"])
        assert [linked.id for linked in store.linked(run, "references")] == [reference_id]
