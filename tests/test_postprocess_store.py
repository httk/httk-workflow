"""``httk collect --postprocess``: postprocess executions stored and served as provenance runs."""

import json
import sqlite3
from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from httk.core import DataRecord, FileRecord, Run, RunEdge
from httk.core.cli import CLIContext

from conftest import configure_identity
from httk.workflow import Workspace, _store, job_records
from httk.workflow.packages import load_workflow_package
from httk.workflow.postprocessing import run_postprocess_script
from httk.workflow.workflow_cli import collect_command, command
from test_collect_fallback import _DATA_MANIFEST, _DATA_POSTPROCESS, _POSCAR
from test_workflow_packages import _package
from v3_helpers import RUNNER, cli_owner, run, submit, workspace

pytest.importorskip("httk.store")
pytest.importorskip("httk.atomistic")

_MANIFEST = (
    _DATA_MANIFEST
    + '\n[workflow.postprocess.report]\nfile = "scripts/report.sh"\noutputs = ["report.json", "figures/*.png"]\n'
)


def _script(package: Path, text: str = "one") -> None:
    script = package / "scripts" / "report.sh"
    script.write_text(
        f"#!/bin/sh\nmkdir -p figures\nprintf '{text}' > report.json\nprintf plot > figures/a.png\n", encoding="utf-8"
    )
    script.chmod(0o755)


def _setup(
    tmp_path: Path,
    jobs: int = 1,
    *,
    manifest: str = _MANIFEST,
    collect: str = _DATA_POSTPROCESS,
    unique: bool = False,
) -> tuple[Path, Any, Workspace]:
    """Run *jobs* jobs of a package with a records collector and a ``report`` script declaring outputs.

    Each job writes ``report.txt`` (``plot``, or ``plot <n>`` when *unique*) into its workdir, which
    *collect* may return as a file.
    """

    package = _package(tmp_path / "package", manifest)
    (package / "run").write_text(RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "collect.py").write_text(collect, encoding="utf-8")
    (package / "scripts").mkdir()
    _script(package)
    ws = workspace(tmp_path / "workspace")
    with cli_owner(ws) as owner:
        installed = _store.install(ws, owner, package)
    declarations = {"workflow": installed.provider().declarations["workflow"]}
    for index in range(jobs):
        submit(
            ws,
            installed,
            {"start": "succeed"},
            members={"POSCAR": _POSCAR},
            declarations=declarations,
            parameters={"files": {"start": {"report.txt": f"plot {index}" if unique else "plot"}}},
        )
    run(ws)
    return package, load_workflow_package(package, register=False), ws


def _postprocess(provider: Any, ws: Workspace) -> None:
    for record in job_records(ws):
        assert run_postprocess_script(provider, "report", record).returncode == 0


def _lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def _into(ws: Workspace, store: Path, *extra: str) -> int:
    argv = [str(ws.root), "--into", str(store), "--id-base", "httk.probe", *extra]
    return collect_command(argv, CLIContext("httk", ws.root.parent))


def _collect(ws: Workspace, store: Path, *extra: str) -> int:
    """Collect the jobs (with the installed package's collector) into *store*."""

    return _into(ws, store, "--allow-job-collector", *extra)


def _collect_postprocess(ws: Workspace, store: Path, *extra: str) -> int:
    """Collect the ``report`` executions into *store*; its jobs stored no files, so it gains the files family."""

    return _into(ws, store, "--postprocess", "report", "--upgrade", *extra)


def _runs(store_path: Path) -> list[Run]:
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(store_path) as database:
        store = SqlStore(database)
        searcher = store.searcher()
        return [row.run for row in searcher.results(run=searcher.variable(Run))]


def _file(store_path: Path, file_id: str) -> FileRecord:
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(store_path) as database:
        searcher = SqlStore(database).searcher(only_latest=True)
        variable = searcher.variable(FileRecord)
        searcher.add(variable.id == file_id)
        return searcher.results(file=variable).first().file


@pytest.mark.parametrize("ledger", [True, False], ids=["ledger", "no-ledger"])
def test_postprocess_runs_are_stored_unchanged_and_revised(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], ledger: bool
) -> None:
    configure_identity()
    package, provider, ws = _setup(tmp_path)
    store = tmp_path / "store.sqlite"
    flags = () if ledger else ("--no-id-ledger",)
    assert _collect(ws, store, *flags) == 0
    capsys.readouterr()
    _postprocess(provider, ws)

    assert _collect_postprocess(ws, store, *flags) == 0
    report, summary = _lines(capsys)
    assert summary["format"] == "httk-workflow-postprocess-collect-summary"
    assert (summary["stored"], summary["unchanged"], summary["revised"], summary["errors"]) == (1, 0, 0, 0)
    assert (tmp_path / "store.sqlite.ids.sqlite").exists() is ledger
    run_id = report["stored"]["run"]
    (job_run,) = [candidate for candidate in _runs(store) if candidate.id != run_id]
    (stored,) = [candidate for candidate in _runs(store) if candidate.id == run_id]
    record = next(job_records(ws))
    assert stored.source_id == f"{record.workspace_id}:{record.job_id}:postprocess:report"
    if ledger:
        with sqlite3.connect(tmp_path / "store.sqlite.ids.sqlite") as connection:
            allocated = dict(connection.execute("SELECT key, id FROM records").fetchall())
        assert allocated[stored.source_id] == run_id
    assert job_run.id is not None
    assert stored.inputs == (*job_run.outputs, RunEdge("job", "runs", job_run.id))
    assert job_run.outputs, "the job run feeds its outputs to the postprocess run"
    assert [(edge.label, edge.entry_type) for edge in stored.outputs] == [
        ("figures/a.png", "files"),
        ("report.json", "files"),
    ]
    assert [edge.entry_id for edge in stored.outputs] == report["stored"]["files"]
    expected = PurePosixPath(
        "data", ws.root.name, "postprocess", *record.placement.parts, record.job_key, "report", "report.json"
    )
    assert _file(store, stored.outputs[1].entry_id).url == expected.as_posix()
    assert stored.started_at is not None and stored.finished_at is not None
    assert stored.workflow_definition_uri == job_run.workflow_definition_uri

    # Collecting the same execution again changes nothing.
    assert _collect_postprocess(ws, store, *flags) == 0
    report, summary = _lines(capsys)
    assert report["unchanged"] is True and report["stored"]["run"] == run_id
    assert (summary["stored"], summary["unchanged"]) == (0, 1)
    assert len(_runs(store)) == 2

    # A rerun writing different bytes revises the same run with a new file.
    _script(package, "two")
    _postprocess(provider, ws)
    assert _collect_postprocess(ws, store, *flags) == 0
    revised, summary = _lines(capsys)
    assert revised["revised"] is True and revised["stored"]["run"] == run_id and summary["revised"] == 1
    assert revised["stored"]["files"][1] != report["stored"]["files"][1]
    assert revised["stored"]["files"][0] == report["stored"]["files"][0]
    assert len([candidate for candidate in _runs(store) if candidate.id == run_id]) == 2


def test_identical_outputs_of_two_jobs_deduplicate_and_still_revise(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_identity()
    package, provider, ws = _setup(tmp_path, jobs=2)
    store = tmp_path / "store.sqlite"
    assert _collect(ws, store) == 0
    _postprocess(provider, ws)
    capsys.readouterr()

    assert _collect_postprocess(ws, store) == 0
    *reports, summary = _lines(capsys)
    assert summary["stored"] == 2 and summary["errors"] == 0
    first, second = reports
    assert first["stored"]["files"] == second["stored"]["files"]
    assert first["stored"]["run"] != second["stored"]["run"]

    _script(package, "changed")
    record = next(job_records(ws, jobs=[first["job"].split(":")[1]]))
    run_postprocess_script(provider, "report", record)
    assert _collect_postprocess(ws, store) == 0
    *reports, summary = _lines(capsys)
    by_job = {report["job"]: report for report in reports}
    assert by_job[first["job"]]["revised"] is True and by_job[first["job"]]["stored"]["run"] == first["stored"]["run"]
    assert by_job[second["job"]]["unchanged"] is True
    assert (summary["revised"], summary["unchanged"]) == (1, 1)


def test_postprocess_collection_errors_exit_one(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    package, provider, ws = _setup(tmp_path)
    store = tmp_path / "store.sqlite"

    # No store yet: refused up front.
    assert _collect_postprocess(ws, store, "--no-id-ledger") == 2
    assert "collect the jobs into this store first" in capsys.readouterr().err

    # A store without this job's run: a storage error, nothing stored.
    _other_package, _other_provider, other = _setup(tmp_path / "other")
    assert _collect(other, store, "--no-id-ledger") == 0
    _postprocess(provider, ws)
    capsys.readouterr()
    # The jobs stored no files, so storing postprocess files is an explicit additive upgrade.
    assert _into(ws, store, "--postprocess", "report", "--no-id-ledger") == 2
    assert "--upgrade" in capsys.readouterr().err
    assert _collect_postprocess(ws, store, "--no-id-ledger") == 1
    report, summary = _lines(capsys)
    assert "has no run in this store" in report["storage_error"] and report["stored"] is None
    assert summary["errors"] == 1
    assert len(_runs(store)) == 1

    # A missing literal output is an error for that job, and nothing is stored.
    assert _collect(ws, store, "--no-id-ledger") == 0
    (package / "scripts" / "report.sh").write_text("#!/bin/sh\nrm -f report.json\n", encoding="utf-8")
    _postprocess(provider, ws)
    capsys.readouterr()
    assert command(["collect", str(ws.root), "--postprocess", "report"], CLIContext("httk", tmp_path)) == 1
    report, summary = _lines(capsys)
    assert "report.json" in report["error"] and summary["errors"] == 1 and "stored" not in summary
    assert _collect_postprocess(ws, store, "--no-id-ledger") == 1
    assert len(_runs(store)) == 2


@pytest.mark.parametrize(
    "extra",
    [
        ["--raw"],
        ["--allow-job-collector"],
        ["--no-bare-runs"],
        ["--fail-fast"],
        ["--batch-size", "2"],
        ["--degraded"],
        ["--dry-run"],
        ["--prefer", "x"],
        ["--exclude", "x"],
        ["--collector", "x"],
    ],
)
def test_postprocess_refuses_sweep_options(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], extra: list[str]
) -> None:
    ws = Workspace.initialize(tmp_path / "ws")
    assert collect_command([str(ws.root), "--postprocess", "report", *extra], CLIContext("httk", tmp_path)) == 2
    assert f"--postprocess cannot be combined with {extra[0]}" in capsys.readouterr().err


def test_postprocess_refuses_a_calculation_tree(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "tree").mkdir()
    assert collect_command([str(tmp_path / "tree"), "--postprocess", "report"], CLIContext("httk", tmp_path)) == 2
    assert "--postprocess only applies to a workspace" in capsys.readouterr().err


def test_stored_postprocess_runs_are_served_with_their_relationships(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("starlette")
    serve = pytest.importorskip("httk.serve.optimade")
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]
    from starlette.testclient import TestClient

    _package_dir, provider, ws = _setup(tmp_path)
    store_path = tmp_path / "store.sqlite"
    assert _collect(ws, store_path, "--no-id-ledger") == 0
    _postprocess(provider, ws)
    capsys.readouterr()
    assert _collect_postprocess(ws, store_path, "--no-id-ledger") == 0
    report, _summary = _lines(capsys)
    run_id = report["stored"]["run"]
    (job_run,) = [candidate for candidate in _runs(store_path) if candidate.id != run_id]
    source = f"{report['job']}:postprocess:report"

    with Backend.sqlite(store_path) as database:
        app = serve.create_asgi_app(serve.adapter_from_store(SqlStore(database)), baseurl="http://testserver")
        with TestClient(app, base_url="http://testserver") as client:
            response = client.get("/v1/_httk_runs", params={"filter": f'_httk_source_id="{source}"'})
            assert response.status_code == 200, response.text
            (served,) = response.json()["data"]
            assert served["id"] == run_id
            relationships = served["relationships"]
            assert {"type": "_httk_runs", "id": job_run.id} in [
                {"type": item["type"], "id": item["id"]} for item in relationships["_httk_has_input"]["data"]
            ]
            outputs = relationships["_httk_has_output"]["data"]
            assert {item["type"] for item in outputs} == {"files"}
            assert sorted(item["id"] for item in outputs) == sorted(report["stored"]["files"])

            job = client.get(f"/v1/_httk_runs/{job_run.id}")
            assert job.status_code == 200, job.text
            reverse = job.json()["data"]["relationships"]["_httk_is_input"]["data"]
            assert run_id in [item["id"] for item in reverse]

            file_id = report["stored"]["files"][1]
            served_file = client.get(f"/v1/files/{file_id}")
            assert served_file.status_code == 200, served_file.text
            assert served_file.json()["data"]["attributes"]["url"].startswith(f"data/{ws.root.name}/postprocess/")


def test_output_equal_to_an_earlier_revision_is_a_storage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package, provider, ws = _setup(tmp_path)
    store = tmp_path / "store.sqlite"
    assert _collect(ws, store, "--no-id-ledger") == 0
    run_ids = []
    for text in ("A", "B"):
        _script(package, text)
        _postprocess(provider, ws)
        capsys.readouterr()
        assert _collect_postprocess(ws, store, "--no-id-ledger") == 0
        report, _summary = _lines(capsys)
        run_ids.append(report["stored"]["run"])
    assert run_ids[0] == run_ids[1] and report["revised"] is True

    _script(package, "A")
    _postprocess(provider, ws)
    capsys.readouterr()
    assert _collect_postprocess(ws, store, "--no-id-ledger") == 1
    report, summary = _lines(capsys)
    assert report["storage_error"].startswith("postprocess output equals an earlier revision (")
    assert f"of run {run_ids[0]};" in report["storage_error"]
    assert report["stored"] is None and report["revised"] is False and summary["errors"] == 1
    assert len([candidate for candidate in _runs(store) if candidate.id == run_ids[0]]) == 2


def test_the_id_ledger_is_opened_only_for_a_new_run_and_then_shared(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_identity()
    per_job = _FILE_COLLECT.replace('"total", 1.5', "record.job_id, 1.5")
    _package_dir, provider, ws = _setup(tmp_path, 2, manifest=_FILE_MANIFEST, collect=per_job, unique=True)
    first, second = (f"{record.workspace_id}:{record.job_id}" for record in job_records(ws))
    store = tmp_path / "store.sqlite"
    ledger = tmp_path / "store.sqlite.ids.sqlite"
    assert _collect(ws, store, "--no-id-ledger", "--job", first.split(":")[1]) == 0

    # Nothing postprocessed: every job is skipped and no ledger is created.
    assert _collect_postprocess(ws, store, "--job", first.split(":")[1]) == 0
    assert _lines(capsys)[-1]["skipped"] == 1
    assert not ledger.exists()

    _postprocess(provider, ws)
    assert _collect_postprocess(ws, store, "--job", first.split(":")[1]) == 0
    report, _summary = _lines(capsys)

    def allocated() -> dict[str, str]:
        with sqlite3.connect(ledger) as connection:
            return dict(connection.execute("SELECT key, id FROM records").fetchall())

    assert allocated() == {f"{first}:postprocess:report": report["stored"]["run"]}
    # A later collect of the other job shares that ledger: its record, file and run get ledger ids, no fallback.
    assert _collect(ws, store, "--job", second.split(":")[1]) == 0
    job_report, summary = _lines(capsys)
    assert summary["storage_errors"] == 0
    keys = allocated()
    assert keys[second] == job_report["stored"]["run"]
    assert {keys[f"{second}:relaxed_structure"], keys[f"{second}:report"]} == set(job_report["stored"]["entries"])
    assert all(entry.startswith("httk.probe.") for entry in job_report["stored"]["entries"])


def test_a_revised_job_run_revises_the_postprocess_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.scaffold import _WORKFLOW_PROVIDERS

    _package_dir, provider, ws = _setup(tmp_path)
    store = tmp_path / "store.sqlite"
    monkeypatch.setitem(_WORKFLOW_PROVIDERS, provider.workflow_id, provider)
    assert _into(ws, store, "--no-id-ledger") == 0
    _postprocess(provider, ws)
    capsys.readouterr()
    assert _collect_postprocess(ws, store, "--no-id-ledger") == 0
    first, _summary = _lines(capsys)

    # The job's collector now reports another value, so the job run is revised.
    value = DataRecord.from_value("https://example.test/energy", "total", 2.5)
    revised_provider = replace(provider, collector=lambda _record: {"relaxed_structure": value})
    monkeypatch.setitem(_WORKFLOW_PROVIDERS, provider.workflow_id, revised_provider)
    assert _into(ws, store, "--no-id-ledger") == 0
    assert _lines(capsys)[0]["revised"] is True
    assert _collect_postprocess(ws, store, "--no-id-ledger") == 0
    second, _summary = _lines(capsys)
    assert second["revised"] is True and second["stored"]["run"] == first["stored"]["run"]

    runs = _runs(store)
    job_runs = sorted((run for run in runs if run.source_id == first["job"]), key=lambda run: str(run.immutable_id))
    postprocess_runs = sorted(
        (run for run in runs if run.id == first["stored"]["run"]), key=lambda run: str(run.immutable_id)
    )
    assert len(job_runs) == 2 and len(postprocess_runs) == 2
    assert job_runs[0].outputs != job_runs[1].outputs
    for job_run, postprocess_run in zip(job_runs, postprocess_runs):
        assert postprocess_run.inputs[:-1] == job_run.outputs


def test_postprocess_collect_selects_jobs_and_counts_unreadable_ones(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _package_dir, provider, ws = _setup(tmp_path, jobs=2)
    _postprocess(provider, ws)
    chosen = next(job_records(ws))
    context = CLIContext("httk", tmp_path)

    assert collect_command([str(ws.root), "--postprocess", "report", "--job", chosen.job_id], context) == 0
    (report, summary) = _lines(capsys)
    assert report["job"] == f"{chosen.workspace_id}:{chosen.job_id}" and summary["collected"] == 1
    assert report["workflow_id"] == provider.workflow_id
    # --step narrows the selection; a job left out for its step is not counted.
    step = str(chosen.job["initial_step"])
    assert collect_command([str(ws.root), "--postprocess", "report", "--step", step], context) == 0
    assert _lines(capsys)[-1]["collected"] == 2
    assert collect_command([str(ws.root), "--postprocess", "report", "--step", "elsewhere"], context) == 0
    (summary,) = _lines(capsys)
    assert summary["collected"] == 0 and summary["unreadable"] == 0

    other = next(record for record in job_records(ws) if record.job_id != chosen.job_id)
    (other.payload / "job.json").write_text("{", encoding="utf-8")
    assert collect_command([str(ws.root), "--postprocess", "report"], context) == 1
    *reports, summary = _lines(capsys)
    assert len(reports) == 1 and summary["unreadable"] == 1 and summary["errors"] == 0


_FILE_MANIFEST = _MANIFEST + '\n[workflow.outputs.report]\nentry_type = "files"\nrole = "report"\n'
_FILE_COLLECT = """from httk.core import DataRecord, FileRecord
from httk.core.digests import sha256_file


def collect(record):
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
"""


def test_a_postprocess_file_identical_to_a_job_file_shares_its_row(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _package_dir, provider, ws = _setup(tmp_path, manifest=_FILE_MANIFEST, collect=_FILE_COLLECT)
    store = tmp_path / "store.sqlite"
    assert _collect(ws, store, "--no-id-ledger") == 0
    capsys.readouterr()
    (job_run,) = _runs(store)
    (job_file,) = [edge.entry_id for edge in job_run.outputs if edge.entry_type == "files"]
    _postprocess(provider, ws)

    assert _collect_postprocess(ws, store, "--no-id-ledger") == 0
    report, _summary = _lines(capsys)
    # figures/a.png holds the same bytes ("plot") as the job's report.txt: R2 dedups onto the job file's row.
    plot_id = report["stored"]["files"][0]
    assert plot_id == job_file
    assert "/postprocess/" not in str(_file(store, plot_id).url)


def test_an_unavailable_id_ledger_fails_the_whole_call(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from httk.core.crypto import ed25519_generate_seed
    from httk.store import IdLedger  # pyright: ignore[reportMissingImports]

    configure_identity()
    _package_dir, provider, ws = _setup(tmp_path, 2)
    store = tmp_path / "store.sqlite"
    assert _collect(ws, store, "--no-id-ledger") == 0
    _postprocess(provider, ws)
    capsys.readouterr()

    ledger = tmp_path / "store.sqlite.ids.sqlite"
    with IdLedger.create(
        ledger, bases={"runs": "httk.probe.runs"}, series="1", keys=[("test", ed25519_generate_seed())]
    ):
        assert _collect_postprocess(ws, store) == 2
    captured = capsys.readouterr()
    assert "id ledger lock is already held" in captured.err
    assert captured.out == ""
