"""The original file of a structure input: recorded by ``job new`` in ``job.json``, stored by ``collect --into``.

A file given for a declared ``structures`` input is recorded by name, size and SHA-256 (``staged_files[].original``),
and by its path only relative to an explicit ``source_root``; collection stores it as a bytes-less file entry and an
import record of the structure it was read as. No absolute path of this machine is recorded anywhere.
"""

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core import FileRecord

from httk.workflow import collect, new_job
from httk.workflow import scaffold as scaffold_module
from httk.workflow._job import JobDefinition
from httk.workflow.collecting import StructureInput
from httk.workflow.errors import FormatError
from httk.workflow.storing import _persisted_record_order
from httk.workflow.workflow_cli import command
from test_collect_inputs import _COLLECT, _FILES_MANIFEST, _MANIFEST, _WORKFLOW, _finished
from test_collect_inputs import _POSCAR as _STAGED
from test_workflow_cli_packages import _CIF, _POSCAR, _cli_package, _context, _workspace
from test_workflow_packages import _package
from v3_helpers import RUNNER, run, workspace


@pytest.fixture(autouse=True)
def _unregister() -> Any:
    yield
    scaffold_module._WORKFLOW_PROVIDERS.pop(_WORKFLOW, None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _job_json(jobs: Path, tag: str) -> tuple[bytes, dict[str, dict[str, Any]]]:
    """Return a job's ``job.json`` bytes and its ``staged_files`` entries by path."""

    data = (next(jobs.rglob(f"{tag}--*")) / "job.json").read_bytes()
    return data, {entry["path"]: entry for entry in json.loads(data).get("staged_files", [])}


def test_job_new_records_the_original_of_a_structure_input(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("httk.atomistic")
    context = _context(tmp_path)
    name = _workspace(tmp_path, context)
    argv = ["job", "new", "--workspace", name, "--install", str(_cli_package(tmp_path / "package"))]
    sources = tmp_path / "sources" / "sub"
    sources.mkdir(parents=True)
    cif, poscar = sources / "NaCl.cif", sources / "Si.vasp"
    cif.write_text(_CIF, encoding="utf-8")
    poscar.write_text(_POSCAR, encoding="utf-8")
    (tmp_path / "extra.txt").write_text("extra\n", encoding="utf-8")
    jobs = tmp_path / "workspace" / "jobs"

    # Converted to the POSCAR destination: the CIF is recorded by name, size and digest, and no path.
    extra = ["--file", f"extra.txt={tmp_path / 'extra.txt'}", "--tag", "nacl"]
    assert command([*argv, "--input", f"structure={cif}", *extra], context) == 0
    data, staged = _job_json(jobs, "nacl")
    assert staged["files/POSCAR"]["original"] == {"name": "NaCl.cif", "size": len(_CIF), "sha256": _sha256(cif)}
    # A file staged with --file is not an input's original.
    assert "original" not in staged["files/extra.txt"]
    assert str(tmp_path).encode() not in data

    # With --source-root, its path relative to the root; a verbatim copy is recorded too.
    root = ["--source-root", str(tmp_path / "sources")]
    assert command([*argv, *root, "--input", f"structure={cif}", "--tag", "rooted"], context) == 0
    data, staged = _job_json(jobs, "rooted")
    assert staged["files/POSCAR"]["original"]["path"] == "sub/NaCl.cif"
    assert str(tmp_path).encode() not in data
    assert command([*argv, *root, "--input", f"structure={poscar}", "--tag", "verbatim"], context) == 0
    data, staged = _job_json(jobs, "verbatim")
    entry = staged["files/POSCAR"]
    assert entry["original"] == {
        "name": "Si.vasp",
        "size": entry["size"],
        "sha256": entry["sha256"],
        "path": "sub/Si.vasp",
    }
    assert str(tmp_path).encode() not in data

    # A file outside the root refuses the job.
    capsys.readouterr()
    narrow = ["--source-root", str(tmp_path / "sources" / "elsewhere")]
    assert command([*argv, *narrow, "--input", f"structure={cif}", "--tag", "outside"], context) == 2
    assert "is not under the source root" in capsys.readouterr().err
    assert not list(jobs.rglob("outside--*"))


def test_a_files_input_records_no_original(tmp_path: Path) -> None:
    pytest.importorskip("httk.atomistic")
    ws = workspace(tmp_path / "workspace")
    given = tmp_path / "POSCAR"
    given.write_text(_POSCAR, encoding="utf-8")
    job = new_job(
        ws,
        str(_package(tmp_path / "package", _FILES_MANIFEST)),
        install=True,
        inputs={"structure": given},
        source_root=tmp_path,
        tag="plain",
    )
    _, staged = _job_json(tmp_path / "workspace" / "jobs", job.job_key.split("--")[0])
    assert staged and all("original" not in entry for entry in staged.values())


def _definition(original: object | None = None) -> dict[str, Any]:
    """Return a minimal job.json mapping with one staged file, carrying *original* when given."""

    entry: dict[str, object] = {"path": "files/POSCAR", "size": 3, "sha256": "a" * 64}
    if original is not None:
        entry["original"] = original
    return {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": "00000000-0000-4000-8000-000000000001",
        "tag": None,
        "name": "job",
        "placement": "",
        "workflow": {"id": "local:tests.x", "name": "tests.x"},
        "initial_step": "start",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "step_resources": {},
        "parameters": {},
        "declarations": {},
        "declared": {},
        "environment": {},
        "parent": None,
        "seal_succeeded": None,
        "staged_files": [entry],
    }


def test_job_json_validates_the_original_strictly() -> None:
    # A job.json written before the member existed loads unchanged, its digest that of its bytes.
    old = JobDefinition.from_mapping(_definition())
    assert JobDefinition.from_bytes(old.encode()).digest == old.digest
    good = {"name": "x.cif", "size": 0, "sha256": "b" * 64}
    assert JobDefinition.from_mapping(_definition(good)).staged_files[0]["original"] == good
    JobDefinition.from_mapping(_definition({**good, "path": "a/x.cif"}))
    for bad in (
        {"name": "x.cif", "size": 0},
        {**good, "url": "x"},
        {**good, "sha256": "B" * 64},
        {**good, "size": -1},
        {**good, "name": "a/x.cif"},
        {**good, "path": "/abs/x.cif"},
        {**good, "path": "../x.cif"},
        {**good, "path": "a//x.cif"},
    ):
        with pytest.raises(FormatError):
            JobDefinition.from_mapping(_definition(bad))


def _collected(tmp_path: Path, given: Path, source_root: Path | None) -> Any:
    """Create one job of the collecting package from *given*, run it to success and collect it."""

    package = _package(tmp_path / "package", _MANIFEST)
    (package / "run").write_text(RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "collect.py").write_text(_COLLECT, encoding="utf-8")
    ws = workspace(tmp_path / "workspace")
    new_job(
        ws,
        str(package),
        install=True,
        inputs={"structure": given},
        parameters={"script": {"start": "succeed"}, "files": {"start": {"report.txt": "hello"}}},
        source_root=source_root,
    )
    run(ws)
    scaffold_module._WORKFLOW_PROVIDERS.pop(_WORKFLOW, None)
    return _only(collect(ws, allow_job_collector=True))


def _only(items: Any) -> Any:
    (item,) = items
    return item


def _imports(path: str) -> list[Any]:
    from httk.atomistic import UnitcellStructureImportRecord
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(path) as database:
        searcher = SqlStore(database).searcher()
        variable = searcher.variable(UnitcellStructureImportRecord)
        return [row.entry for row in searcher.results(entry=variable)]


@pytest.mark.parametrize("rooted", [True, False])
def test_collect_into_stores_the_original_and_its_import(tmp_path: Path, rooted: bool) -> None:
    pytest.importorskip("httk.store")
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    from httk.workflow import store_collected

    sources = tmp_path / "sources" / "sub"
    sources.mkdir(parents=True)
    given = sources / "NaCl.cif"
    given.write_text(_CIF, encoding="utf-8")
    item = _collected(tmp_path, given, tmp_path / "sources" if rooted else None)
    value = item.inputs["initial_structure"]
    assert isinstance(value, StructureInput) and value.imported is not None

    path = str(tmp_path / "store.sqlite")
    (report,) = store_collected([item], path, id_base="httk.t")
    assert "storage_error" not in report, report
    entries = cast(Any, report["stored"])["entries"]
    (alternative_id,) = [entry for entry in entries if "~" in entry]
    main_id = alternative_id.split("~")[0]
    (imported,) = _imports(path)
    assert imported.id in entries
    assert imported.source_uri == ("sub/NaCl.cif" if rooted else "NaCl.cif")
    assert imported.status == "imported" and imported.importer.startswith("httk-workflow ")
    # The import names the input's main structure, the one whose alternative the run's edge names.
    assert imported.structure.id == main_id
    original = imported.source_file
    assert (original.url, original.name, original.size) == (None, "NaCl.cif", len(_CIF))
    assert (original.sha256, original.format) == (_sha256(given), "cif")
    with Backend.sqlite(path) as database:
        searcher = SqlStore(database).searcher()
        variable = searcher.variable(FileRecord)
        searcher.add(variable.sha256 == _sha256(given))
        (stored_file,) = [row.entry for row in searcher.results(entry=variable)]
    assert stored_file.url is None
    assert str(tmp_path) not in repr(imported) + repr(stored_file)

    # Collecting the job again stores nothing new.
    (again,) = store_collected([item], path, id_base="httk.t")
    assert again["stored"] == report["stored"] and len(_imports(path)) == 1


def test_a_job_without_an_original_stores_no_import(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.workflow import store_collected

    item = _only(collect(_finished(tmp_path), allow_job_collector=True))
    value = item.inputs["initial_structure"]
    assert isinstance(value, StructureInput) and value.imported is None
    path = tmp_path / "store.sqlite"
    (report,) = store_collected([item], str(path), id_base="httk.t")
    assert "storage_error" not in report
    # The store's layout is as before: it declares no imports family.
    families = _persisted_record_order(path)
    assert families is not None and "structures" in families and "imports" not in families


def test_an_original_in_job_json_becomes_the_inputs_import(tmp_path: Path) -> None:
    pytest.importorskip("httk.atomistic")
    from httk.atomistic import UnitcellStructureImportRecord

    staged = {"path": "files/POSCAR", "size": len(_STAGED), "sha256": hashlib.sha256(_STAGED.encode()).hexdigest()}
    original = {"name": "given.vasp", "size": 7, "sha256": "c" * 64}
    item = _only(
        collect(_finished(tmp_path, staged_files=[{**staged, "original": original}]), allow_job_collector=True)
    )
    value = item.inputs["initial_structure"]
    assert isinstance(value, StructureInput)
    imported = value.imported
    assert isinstance(imported, UnitcellStructureImportRecord)
    assert imported.source_uri == "given.vasp" and imported.structure == replace(value.alternative, source_file=None)
    assert imported.source_file == FileRecord(None, "given.vasp", size=7, sha256="c" * 64, format="vasp-poscar")


def test_an_existing_store_takes_the_imports_family_as_an_additive_upgrade(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.workflow import store_collected

    path = str(tmp_path / "store.sqlite")
    (before,) = store_collected(
        [_only(collect(_finished(tmp_path / "a"), allow_job_collector=True))], path, id_base="httk.t"
    )
    assert "storage_error" not in before
    given = tmp_path / "NaCl.cif"
    given.write_text(_CIF, encoding="utf-8")
    item = _collected(tmp_path / "b", given, None)
    with pytest.raises(ValueError, match="upgrade"):
        store_collected([item], path, id_base="httk.t")
    (report,) = store_collected([item], path, id_base="httk.t", upgrade=True)
    assert "storage_error" not in report, report
    assert [imported.source_uri for imported in _imports(path)] == ["NaCl.cif"]


def test_a_record_selection_the_import_cannot_embed_stores_no_import(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.workflow import store_collected

    given = tmp_path / "NaCl.cif"
    given.write_text(_CIF, encoding="utf-8")
    item = _collected(tmp_path, given, None)
    path = str(tmp_path / "store.sqlite")
    selection = {"structures": ["atomistic-numeric-unitcell-structure"]}
    (report,) = store_collected([item], path, id_base="httk.t", entry_records=selection)
    assert "storage_error" not in report, report
    # The store's layout is that of a store without imports.
    families = _persisted_record_order(Path(path))
    assert families is not None and "imports" not in families


def test_input_from_records_originals_and_honours_the_source_root(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("httk.atomistic")
    context = _context(tmp_path)
    name = _workspace(tmp_path, context)
    argv = ["job", "new", "--workspace", name, "--install", str(_cli_package(tmp_path / "package"))]
    batch = tmp_path / "sources" / "batch"
    batch.mkdir(parents=True)
    for stem in ("one", "two"):
        (batch / f"{stem}.cif").write_text(_CIF.replace("data_NaCl", f"data_{stem}"), encoding="utf-8")
    single = tmp_path / "sources" / "Si.vasp"
    single.write_text(_POSCAR, encoding="utf-8")
    jobs = tmp_path / "workspace" / "jobs"
    root = ["--source-root", str(tmp_path / "sources")]

    # A directory batch: each job records its own CIF, by its path under the root.
    assert command([*argv, *root, "--tag", "run", "--input-from", "structure", str(batch)], context) == 0
    for stem in ("one", "two"):
        data, staged = _job_json(jobs, f"run-{stem}")
        original = staged["files/POSCAR"]["original"]
        assert original == {
            "name": f"{stem}.cif",
            "size": (batch / f"{stem}.cif").stat().st_size,
            "sha256": _sha256(batch / f"{stem}.cif"),
            "path": f"batch/{stem}.cif",
        }
        assert str(tmp_path).encode() not in data
    # A single file in the destination's format is copied byte for byte, as with --input.
    assert command([*argv, "--input-from", "structure", str(single), "--tag", "single"], context) == 0
    _, staged = _job_json(jobs, "single")
    assert (next(jobs.rglob("single--*")) / "files" / "POSCAR").read_text(encoding="utf-8") == _POSCAR
    assert staged["files/POSCAR"]["original"] == {"name": "Si.vasp", "size": len(_POSCAR), "sha256": _sha256(single)}
    # The root is enforced for --input-from too.
    capsys.readouterr()
    narrow = ["--source-root", str(batch)]
    assert command([*argv, *narrow, "--input-from", "structure", str(single), "--tag", "outside"], context) == 2
    assert "is not under the source root" in capsys.readouterr().err
    assert not list(jobs.rglob("outside--*"))


def test_a_symlinked_input_records_the_file_it_points_to(tmp_path: Path) -> None:
    pytest.importorskip("httk.atomistic")
    ws = workspace(tmp_path / "workspace")
    sources = tmp_path / "sources" / "sub"
    sources.mkdir(parents=True)
    (sources / "NaCl.cif").write_text(_CIF, encoding="utf-8")
    link = tmp_path / "sources" / "link.cif"
    link.symlink_to(sources / "NaCl.cif")
    job = new_job(
        ws,
        str(_cli_package(tmp_path / "package")),
        install=True,
        inputs={"structure": link},
        source_root=tmp_path / "sources",
        tag="linked",
    )
    _, staged = _job_json(tmp_path / "workspace" / "jobs", job.job_key.split("--")[0])
    original = staged["files/POSCAR"]["original"]
    assert (original["name"], original["path"]) == ("NaCl.cif", "sub/NaCl.cif")


@pytest.mark.parametrize("changed", ["source", "copy"])
def test_a_file_changed_while_staged_refuses_the_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    pytest.importorskip("httk.atomistic")
    ws = workspace(tmp_path / "workspace")
    given = tmp_path / "Si.vasp"
    given.write_text(_POSCAR, encoding="utf-8")
    stage_inputs = scaffold_module._stage_inputs

    def staged_then_changed(payload: Path, *args: Any, **kwargs: Any) -> set[str]:
        verbatim = stage_inputs(payload, *args, **kwargs)
        target = given if changed == "source" else payload / "files" / "POSCAR"
        target.write_text(_POSCAR.replace("silicon", "changed"), encoding="utf-8")
        return verbatim

    monkeypatch.setattr(scaffold_module, "_stage_inputs", staged_then_changed)
    package = str(_cli_package(tmp_path / "package"))
    with pytest.raises(ValueError, match="changed while it was staged"):
        new_job(ws, package, install=True, inputs={"structure": given}, tag="raced")
    assert not list((tmp_path / "workspace" / "jobs").rglob("raced--*"))


def test_an_import_that_cannot_be_built_leaves_the_input_without_one(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    pytest.importorskip("httk.atomistic")
    staged = {"path": "files/POSCAR", "size": len(_STAGED), "sha256": hashlib.sha256(_STAGED.encode()).hexdigest()}
    # A name with surrounding whitespace is no valid source_uri; the name is kept as recorded.
    original = {"name": "given.vasp ", "size": 7, "sha256": "c" * 64}
    ws = _finished(tmp_path, staged_files=[{**staged, "original": original}])
    with caplog.at_level("WARNING"):
        item = _only(collect(ws, allow_job_collector=True))
    assert item.missing_collector is None and not item.record.gaps
    value = item.inputs["initial_structure"]
    assert isinstance(value, StructureInput) and value.imported is None
    assert "its import is not recorded" in caplog.text and item.record.job_key in caplog.text
