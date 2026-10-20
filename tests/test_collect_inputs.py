"""Workspace collection of a job's inputs and of file-entry URLs.

A declared input staged as a file (``destination``) is loaded by the framework
into an entry with a run input edge; a collect hook may hand over the inputs it
read instead (a :class:`~httk.workflow.hookapi.Collected` in process, or the
``"inputs"`` member of an executable response). File entries get
``data/<workspace name>/<workspace-relative path>`` URLs.
"""

import io
import json
import logging
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from httk.core import DataRecord, FileRecord
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _store, collect
from httk.workflow import scaffold as scaffold_module
from httk.workflow.collecting import JobRecord, _prefixed_url
from httk.workflow.workflow_cli import command
from test_collect_fallback import _DATA_MANIFEST
from test_workflow_packages import _package
from v3_helpers import RUNNER, cli_owner, run, submit, workspace

_WORKFLOW = "tests.inputs.package"
_MANIFEST = (
    _DATA_MANIFEST.replace("tests.fallback.package", _WORKFLOW)
    + """
[workflow.outputs.report]
entry_type = "files"
role = "report"
"""
)
_POSCAR = "Si\n5.43\n0 0.5 0.5\n0.5 0 0.5\n0.5 0.5 0\nSi\n2\nDirect\n0 0 0\n0.25 0.25 0.25\n"
_DECLARED = {
    "inputs": {
        "structure": {
            "destination": "POSCAR",
            "entry_type": "structures",
            "required": True,
            "role": "initial_structure",
        }
    }
}
# Built with the bare workspace-relative path, as workflow packages' hooks do.
_COLLECT = """from httk.core import DataRecord, FileRecord
from httk.core.digests import sha256_file


def outputs(record):
    path = record.workdir / "report.txt"
    return {
        "relaxed_structure": DataRecord.from_value("https://example.test/energy", "total", 1.5),
        "report": FileRecord(
            url=(record.payload_path / "run" / "report.txt").as_posix(),
            name="report.txt",
            size=path.stat().st_size,
            sha256=sha256_file(path),
        ),
    }


def collect(record):
    return outputs(record)
"""
_COLLECT_HANDING_OVER = (
    _COLLECT
    + """

from httk.core import load
from httk.workflow.hookapi import Collected


def collect(record):
    return Collected(outputs(record), {"initial_structure": load(str(record.payload / "files" / "POSCAR"))})
"""
)


@pytest.fixture(autouse=True)
def _unregister() -> Any:
    yield
    scaffold_module._WORKFLOW_PROVIDERS.pop(_WORKFLOW, None)


def _finished(
    tmp_path: Path,
    *,
    collect_hook: str = _COLLECT,
    manifest: str = _MANIFEST,
    declared: Mapping[str, object] = _DECLARED,
    members: Mapping[str, str] | None = None,
) -> Workspace:
    """Install the package and run one job with a staged POSCAR and a report file to success."""

    pytest.importorskip("httk.atomistic")
    package = _package(tmp_path / "package", manifest)
    (package / "run").write_text(RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "collect.py").write_text(collect_hook, encoding="utf-8")
    ws = workspace(tmp_path / "workspace")
    with cli_owner(ws) as owner:
        installed = _store.install(ws, owner, package)
    declarations = {"workflow": installed.provider().declarations["workflow"]}
    submit(
        ws,
        installed,
        {"start": "succeed"},
        members={"files/POSCAR": _POSCAR} if members is None else members,
        declarations=declarations,
        declared=declared,
        parameters={"files": {"start": {"report.txt": "hello"}}},
    )
    run(ws)
    scaffold_module._WORKFLOW_PROVIDERS.pop(_WORKFLOW, None)
    return ws


def test_a_staged_input_becomes_an_entry_that_the_product_links_to(tmp_path: Path) -> None:
    ws = _finished(tmp_path)

    item = next(collect(ws, allow_job_collector=True))

    assert item.missing_collector is None and item.products_unlinked == ()
    # Assembling the run keeps every run_record field, the job's name and times among them.
    assert item.run.name == "demo job"
    assert item.run.started_at is not None and item.run.finished_at is not None
    structure = item.inputs["initial_structure"]
    assert getattr(structure, "type", None) == "structures"
    assert [(edge.label, edge.entry_type) for edge in item.run.inputs] == [("initial_structure", "structures")]
    (product,) = item.products
    assert (product.source_type, product.source_id) == ("structures", item.run.inputs[0].entry_id)
    assert product.label == "relaxed_structure"
    # The file entry's URL is data/<workspace name>/<workspace-relative path>; unregistered, the name is the root's.
    report = item.outputs["report"]
    assert isinstance(report, FileRecord)
    relative = f"{item.record.payload_path.as_posix()}/run/report.txt"
    assert report.url == f"data/workspace/{relative}"

    register_ws(CLIContext("httk", tmp_path), ws.root, "named")
    named = next(collect(ws, allow_job_collector=True))
    assert getattr(named.outputs["report"], "url", None) == f"data/named/{relative}"


def test_only_normalized_relative_urls_are_prefixed(caplog: pytest.LogCaptureFixture) -> None:
    for url in ("https://example.org/a.txt", "/srv/files/a.txt"):
        value = FileRecord(url=url, name="a.txt")
        assert _prefixed_url("ws:job", "report", value, "data/ws") is value
    record = DataRecord.from_value("https://example.test/energy", "total", 1.5)
    assert _prefixed_url("ws:job", "energy", record, "data/ws") is record
    prefixed = _prefixed_url("ws:job", "report", FileRecord(url="jobs/a.txt", name="a.txt"), "data/ws")
    assert getattr(prefixed, "url", None) == "data/ws/jobs/a.txt"
    assert not caplog.records
    for url in ("jobs/../a.txt", "./a.txt", "jobs//a.txt", "data/ws/jobs/a.txt"):
        value = FileRecord(url=url, name="a.txt")
        assert _prefixed_url("ws:job", "report", value, "data/ws") is value
    assert len(caplog.records) == 4 and "ws:job" in caplog.text and "'report'" in caplog.text


def test_a_destination_with_a_directory_is_read_where_it_was_staged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    manifest = _MANIFEST.replace('destination = "POSCAR"', 'destination = "inputs/POSCAR"')
    declared = {"inputs": {"structure": {**_DECLARED["inputs"]["structure"], "destination": "inputs/POSCAR"}}}
    ws = _finished(tmp_path, manifest=manifest, declared=declared, members={"inputs/POSCAR": _POSCAR})

    item = next(collect(ws, allow_job_collector=True))

    assert [edge.label for edge in item.run.inputs] == ["initial_structure"] and item.products_unlinked == ()

    # A declared input that was not staged is reported, not silently dropped.
    absent = _finished(tmp_path / "absent", manifest=manifest, declared=declared, members={})
    item = next(collect(absent, allow_job_collector=True))
    assert item.run.inputs == () and len(item.products_unlinked) == 1
    assert "not collecting input 'initial_structure'" in caplog.text and "inputs/POSCAR is not staged" in caplog.text


def test_a_staged_input_that_is_a_symlink_is_not_loaded(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    ws = _finished(tmp_path, members={})
    payload = next(iter(collect(ws, allow_job_collector=True))).record.payload
    (payload / "files").mkdir(exist_ok=True)
    (tmp_path / "outside").write_text(_POSCAR, encoding="utf-8")
    (payload / "files" / "POSCAR").symlink_to(tmp_path / "outside")

    item = next(collect(ws, allow_job_collector=True))

    assert item.run.inputs == () and "cannot read files/POSCAR" in caplog.text


def test_a_staged_input_is_loaded_from_the_bytes_read_file_returned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.core.loading import load_source
    from httk.core.storage import content_id

    ws = _finished(tmp_path)
    germanium = _POSCAR.replace("Si", "Ge").replace("5.43", "5.66")
    read: list[str] = []

    def patched(self: JobRecord, relative: str, *, limit: int = 0) -> bytes:
        read.append(relative)
        return germanium.encode()

    monkeypatch.setattr(JobRecord, "read_file", patched)
    item = next(collect(ws, allow_job_collector=True))

    assert read == ["files/POSCAR"]
    expected = load_source(io.BytesIO(germanium.encode()), "POSCAR")
    assert content_id(expected) != content_id(load_source(io.BytesIO(_POSCAR.encode()), "POSCAR"))
    assert content_id(item.inputs["initial_structure"]) == content_id(expected)
    assert item.run.inputs[0].entry_id == content_id(expected)


def test_optional_unstaged_and_compressed_inputs_are_skipped(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    optional = {"inputs": {"structure": {**_DECLARED["inputs"]["structure"], "required": False}}}
    ws = _finished(tmp_path, declared=optional, members={})
    caplog.set_level(logging.INFO)
    item = next(collect(ws, allow_job_collector=True))
    assert item.run.inputs == ()
    (message,) = [record for record in caplog.records if "is not staged" in record.getMessage()]
    assert message.levelno == logging.INFO

    manifest = _MANIFEST.replace('destination = "POSCAR"', 'destination = "POSCAR.gz"')
    compressed = {"inputs": {"structure": {**_DECLARED["inputs"]["structure"], "destination": "POSCAR.gz"}}}
    ws = _finished(tmp_path / "gz", manifest=manifest, declared=compressed, members={"files/POSCAR.gz": "x"})
    item = next(collect(ws, allow_job_collector=True))
    assert item.run.inputs == () and "a compressed staged file (POSCAR.gz) is not loaded" in caplog.text


def test_read_file_reads_job_files_safely(tmp_path: Path) -> None:
    ws = _finished(tmp_path)
    record = next(iter(collect(ws, allow_job_collector=True))).record
    assert record.read_file("files/POSCAR") == _POSCAR.encode()
    assert record.read_file("run/report.txt") == b"hello"
    (record.payload / "run" / "link.txt").symlink_to(record.payload / "run" / "report.txt")
    for relative, limit, message in (
        ("files/absent", 1 << 20, "has no file files/absent"),
        ("run/link.txt", 1 << 20, "cannot read run/link.txt"),
        ("files/POSCAR", 4, "cannot read files/POSCAR"),
        ("../job.json", 1 << 20, "cannot read ../job.json"),
    ):
        with pytest.raises(ValueError, match=message):
            record.read_file(relative, limit=limit)


def _stored(store: Any, kind: type, entry_id: str) -> Any:
    searcher = store.searcher()
    variable = searcher.variable(kind)
    searcher.add(variable.id == entry_id)
    return next(iter(searcher.results(entry=variable))).entry


def test_into_stores_the_input_and_the_prefixed_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("httk.store")
    from httk.core import Run
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    ws = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "stored")
    store_path = tmp_path / "results.sqlite"
    argv = ["collect", "--workspace", name, "--allow-job-collector", "--into", str(store_path)]
    argv += ["--id-base", "httk.inputs"]
    # The URL prefix is fixed at collect time; mapping it is a serving option.
    assert command([*argv, "--data-path", "https://example.org/files"], context) == 2
    assert "unrecognized arguments: --data-path" in capsys.readouterr().err
    assert command(argv, context) == 0
    report = json.loads(capsys.readouterr().out.splitlines()[0])
    assert "products_unlinked" not in report and len(report["products"]) == 1
    stored = report["stored"]
    with Backend.sqlite(store_path) as database:
        store = SqlStore(database)
        stored_run = _stored(store, Run, stored["run"])
        edges = {edge.label: edge for edge in (*stored_run.inputs, *stored_run.outputs)}
        assert stored_run.name == "demo job" and stored_run.finished_at is not None
        assert edges["initial_structure"].entry_type == "structures"
        assert edges["initial_structure"].entry_id in stored["entries"]
        assert edges["report"].entry_id in stored["entries"]
        file_entry = _stored(store, FileRecord, edges["report"].entry_id)
        product = _stored(store, DataRecord, edges["relaxed_structure"].entry_id)
    # The product names the stored input structure.
    assert [(edge.label, edge.entry_id) for edge in getattr(product, "product_of", ())] == [
        ("initial_structure", edges["initial_structure"].entry_id)
    ]
    assert file_entry.url.startswith("data/stored/jobs/succeeded/")


def test_a_python_hook_hands_over_its_inputs(tmp_path: Path) -> None:
    ws = _finished(tmp_path, collect_hook=_COLLECT_HANDING_OVER)

    item = next(collect(ws, allow_job_collector=True))

    assert item.missing_collector is None and item.products_unlinked == ()
    # The hook's input wins; the framework does not load the staged file a second time.
    assert [edge.label for edge in item.run.inputs] == ["initial_structure"]
    assert len(item.products) == 1


def test_an_executable_hook_hands_over_inputs_in_its_response(tmp_path: Path) -> None:
    source = Path(__file__).parents[1] / "src"
    hook = (
        "#!/usr/bin/env python3\n"
        f"import sys; sys.path.insert(0, {str(source)!r})\n"
        "from httk.workflow.hookapi import Collected, collect_main\n"
        "collect_main(lambda record: Collected(\n"
        "    {'relaxed_structure': {'value': 1.5}},\n"
        "    {'initial_structure': {'entry': {\n"
        "        'type': 'files', 'url': record['payload_path'] + '/files/POSCAR', 'name': 'POSCAR'}}},\n"
        "))\n"
    )
    manifest = _MANIFEST.replace('[workflow.collect]\nfile = "collect.py"', '[workflow.collect]\nfile = "collect"')
    # Without a ref, a value output needs no property definition.
    manifest = manifest.replace('ref = "https://example.test/records"\n', "")
    manifest = manifest.replace(
        '[workflow.inputs.structure]\ndestination = "POSCAR"\nentry_type = "structures"',
        '[workflow.inputs.structure]\ndestination = "POSCAR"\nentry_type = "files"',
    )
    package = _package(tmp_path / "package", manifest)
    (package / "collect").write_text(hook, encoding="utf-8")
    (package / "collect").chmod(0o755)
    ws = _finished(tmp_path, collect_hook=_COLLECT, manifest=manifest)

    item = next(collect(ws, allow_job_collector=True))

    assert item.missing_collector is None, item.missing_collector
    value = item.inputs["initial_structure"]
    assert isinstance(value, FileRecord)
    assert value.url == f"data/workspace/{item.record.payload_path.as_posix()}/files/POSCAR"
    assert [(edge.label, edge.entry_type) for edge in item.run.inputs] == [("initial_structure", "files")]
    assert item.products_unlinked == () and len(item.products) == 1


def test_an_input_role_the_declaration_does_not_know_degrades_the_job(tmp_path: Path) -> None:
    hook = _COLLECT + (
        "\n\nfrom httk.workflow.hookapi import Collected\n\n\n"
        "def collect(record):\n"
        "    return Collected(outputs(record), {'mystery': DataRecord.from_value('https://example.test/x', 'x', 1)})\n"
    )
    ws = _finished(tmp_path, collect_hook=hook)

    item = next(collect(ws, allow_job_collector=True))

    assert item.missing_collector is not None and "unknown input role 'mystery'" in item.missing_collector


def test_a_run_stored_before_run_names_and_times_teaches_a_rebuild(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.workflow import store_collected

    ws = _finished(tmp_path)
    item = next(collect(ws, allow_job_collector=True))
    path = str(tmp_path / "store.sqlite")
    # A store written before runs carried a name and times holds the same run content without them.
    untimed = replace(item, run=replace(item.run, name=None, started_at=None, finished_at=None))
    assert "storage_error" not in store_collected([untimed], path, id_base="httk.t")[0]

    (report,) = store_collected([item], path, id_base="httk.t")
    assert report["storage_error"] == (
        f"could not store job {item.record.job_id}: run stored before run names and times existed; "
        "rebuild the store from the workspace (collect into a new file) to name and time it"
    )

    # Any other metadata change is a storage error that says what happened.
    other = str(tmp_path / "other.sqlite")
    store_collected([item], other, id_base="httk.t")
    moved = replace(item, run=replace(item.run, finished_at=datetime(2030, 1, 1, tzinfo=UTC)))
    (report,) = store_collected([moved], other, id_base="httk.t")
    assert "run metadata changed since it was stored: " in str(report["storage_error"])
