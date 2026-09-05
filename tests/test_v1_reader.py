"""Finished-tree reader and collector coverage."""

import bz2
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httk.core
import pytest
from httk.core.cli import CLIContext

from httk.workflow import JobRecord
from httk.workflow.collecting import _CollectEnvironmentError
from httk.workflow.compat.v1 import code_of, collect_finished_tree, finished_tasks, task_file
from httk.workflow.workflow_cli import command


def _task(root: Path, name: str, dates: tuple[str, ...], *, code: str | None = "code") -> Path:
    directory = root / name
    directory.mkdir()
    if code is not None:
        (directory / "ht_steps").write_text(f"#!/bin/sh\n# {code}\n# 1.2\n", encoding="utf-8")
    for date in dates:
        run = directory / f"ht.run.{date}"
        run.mkdir()
        (run / "result.txt").write_text(date, encoding="utf-8")
    return directory


def _package(root: Path) -> Path:
    root.mkdir()
    (root / "httk_workflow.toml").write_text(
        '[workflow]\nid = "tests.v1.finished"\ndeclaration_uri = "urn:finished"\n'
        '[workflow.runner]\nlanguage = "httk-v1"\n'
        '[workflow.collect]\nfile = "collect.py"\n'
        '[workflow.outputs.result]\nentry_type = "records"\n',
        encoding="utf-8",
    )
    (root / "collect.py").write_text(
        "from httk.core import DataRecord\n"
        "from httk.workflow.compat.v1 import run_directory, task_file\n"
        "def collect(record):\n"
        "    path = task_file(run_directory(record), 'result.txt')\n"
        "    return {'result': DataRecord.from_value('urn:result', 'result', path.read_text())}\n",
        encoding="utf-8",
    )
    return root


def test_finished_tasks_prunes_nested_tasks_and_selects_latest_run(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "Runs"
    root.mkdir()
    task = _task(
        root,
        "ht.task.default.t1.cleanup.0.unclaimed.3.finished",
        ("2020-01-02_03.04.05", "2021-06-07_08.09.10"),
    )
    (task / "ht.run.current").mkdir()
    (task / "ht.tmp.something").mkdir()
    (task / "ht.run.2021-06-07_08.09.10" / "data.json.bz2").write_bytes(bz2.compress(b"{}"))
    (task / "ht.config").write_text("[main]\ndescription = old result\n", encoding="utf-8")
    nested = task / "nested" / "ht.task.default.nested.cleanup.0.unclaimed.3.finished"
    nested.mkdir(parents=True)
    _task(root, "ht.task.default.t2.cleanup.0.unclaimed.3.finished", ("2021-01-01_00.00.00",), code=None)
    _task(root, "ht.task.default.bad.cleanup.0.unclaimed.3.broken", ("2021-01-01_00.00.00",))
    _task(root, "ht.task.default.empty.cleanup.0.unclaimed.3.finished", ())

    tasks = list(finished_tasks(root))
    assert [task.task_id for task in tasks] == ["t1", "t2"]
    assert tasks[0].rundir.name == "ht.run.2021-06-07_08.09.10"
    assert tasks[0].computation_date == datetime(2021, 6, 7, 8, 9, 10, tzinfo=UTC)
    assert (tasks[0].code_name, tasks[0].code_version) == ("code", "1.2")
    assert tasks[0].description == "old result"
    assert (tasks[1].code_name, tasks[1].code_version) == ("unknown", "0")
    assert next(iter(finished_tasks(root))).immutable_id == tasks[0].immutable_id
    assert "without a dated run directory" in caplog.text


def test_reader_helpers_and_manifest_identity(tmp_path: Path) -> None:
    program = tmp_path / "ht_run"
    program.write_text("#!/bin/sh\n# name\n# version\n", encoding="utf-8")
    assert code_of(program) == ("name", "version")
    assert code_of(tmp_path / "missing") == ("unknown", "0")
    compressed = tmp_path / "data.json.bz2"
    compressed.write_bytes(bz2.compress(b"{}"))
    assert task_file(tmp_path, "data.json") == compressed

    task = _task(tmp_path, "ht.task.default.t.cleanup.0.unclaimed.3.finished", ("2021-01-01_00.00.00",))
    body = b"project-key\n\nmanifest-body\nsignature\n"
    (task / "ht.manifest.bz2").write_bytes(bz2.compress(body))
    item = next(finished_tasks(tmp_path))
    assert item.manifest_hash == hashlib.sha256(b"project-key\n\n").hexdigest()


def test_collect_finished_tree_package_extract_and_degradation(tmp_path: Path) -> None:
    root = tmp_path / "Runs"
    root.mkdir()
    _task(root, "ht.task.default.good.cleanup.0.unclaimed.3.finished", ("2021-06-07_08.09.10",))
    _task(root, "ht.task.default.bad.cleanup.0.unclaimed.3.finished", ("2020-01-02_03.04.05",))
    (root / "ht.task.default.bad.cleanup.0.unclaimed.3.finished" / "ht.run.2020-01-02_03.04.05" / "result.txt").unlink()
    package = _package(tmp_path / "package")

    collected = list(collect_finished_tree(root, workflow_dir=package))
    assert len(collected) == 2
    good = next(item for item in collected if item.missing_collector is None)
    assert cast(Any, good.outputs["result"]).value == "2021-06-07_08.09.10"
    assert isinstance(good.run.source_id, str)
    assert good.run.source_id.startswith("httk-v1:")
    assert good.run.source_id != good.record.job_id
    assert good.run.immutable_id is None
    assert JobRecord.from_mapping(good.record.as_mapping()) == good.record
    assert good.run.last_modified == datetime(2021, 6, 7, 8, 9, 10, tzinfo=UTC)
    assert good.run.workflow_declaration_uri == "urn:finished"
    assert [edge.label for edge in good.run.outputs] == ["result"]
    assert next(item for item in collected if item.missing_collector is not None)

    direct = list(
        collect_finished_tree(
            root,
            extract=lambda task: {"result": httk.core.DataRecord.from_value("urn:result", "result", task.task_id)},
        )
    )
    assert all(item.outputs for item in direct)
    with pytest.raises(ValueError, match="exactly one"):
        list(collect_finished_tree(root))
    with pytest.raises(ValueError, match="exactly one"):
        list(collect_finished_tree(root, workflow_dir=package, extract=lambda _task: {}))


def test_v1_collect_cli_json_lines(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "Runs"
    root.mkdir()
    _task(root, "ht.task.default.cli.cleanup.0.unclaimed.3.finished", ("2021-01-01_00.00.00",))
    package = _package(tmp_path / "package")
    assert (
        command(
            ["v1", "collect", "--workflow-dir", str(package), str(root)],
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    lines = capsys.readouterr().out.splitlines()
    report = json.loads(lines[0])
    assert report["workflow"] == "tests.v1.finished"
    assert report["outputs"]["result"]["type"] == "records"
    assert report["identity_stable"] is False
    summary = json.loads(lines[-1])
    assert summary["format"] == "httk-workflow-v1-collect-summary"
    assert summary["finished"] == 1 and summary["skipped_no_rundir"] == 0


def test_collection_batching_options(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'batch_size'"):
        cast(Any, collect_finished_tree)(tmp_path, extract=lambda _task: {}, batch_size=64)
    context = CLIContext("httk", tmp_path)
    assert command(["v1", "collect", "--workflow-dir", "package", "root", "--batch-size", "64"], context) == 2
    assert "unrecognized arguments: --batch-size 64" in capsys.readouterr().err
    for prefix in ([], ["campaign"]):
        assert command([*prefix, "collect", "--help"], context) == 0
        help_text = " ".join(capsys.readouterr().out.split())
        assert "disables executable batching (one collector process per job)" in help_text
        assert "ignored with --fail-fast" in help_text


@pytest.mark.parametrize("error", [ImportError, _CollectEnvironmentError])
def test_v1_collector_environment_failure_is_fatal(tmp_path: Path, error: type[Exception]) -> None:
    _task(tmp_path, "ht.task.default.bad.cleanup.0.unclaimed.3.finished", ("2021-01-01_00.00.00",))

    def extract(_task):
        raise error("dependency unavailable")

    with pytest.raises(error, match="dependency unavailable"):
        list(collect_finished_tree(tmp_path, extract=extract))


@pytest.mark.parametrize("fail_fast", [False, True])
def test_v1_cli_fail_fast_stops_before_later_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_fast: bool
) -> None:
    from httk.workflow.workflow_cli import _compat

    visited = []

    def collect_root(root, **_kwargs):
        visited.append(root)
        raise ValueError("broken result")

    monkeypatch.setattr(_compat, "collect_finished_tree", collect_root)
    flags = ["--fail-fast"] if fail_fast else []
    assert (
        command(["v1", "collect", "--workflow-dir", "package", *flags, "broken", "later"], CLIContext("httk", tmp_path))
        == 1
    )
    assert visited == (["broken"] if fail_fast else ["broken", "later"])


def test_synthesized_ids_are_canonical_and_path_distinct(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    for branch in ("left", "right"):
        branch_root = root / branch
        branch_root.mkdir(parents=True)
        _task(branch_root, "ht.task.default.same.cleanup.0.unclaimed.3.finished", ("2021-01-01_00.00.00",))

    tasks = list(finished_tasks(root))
    assert len(tasks) == 2
    assert tasks[0].immutable_id != tasks[1].immutable_id
    collected = list(
        collect_finished_tree(
            root,
            extract=lambda task: {"result": httk.core.DataRecord.from_value("urn:result", "result", task.task_id)},
        )
    )
    assert len({item.record.job_id for item in collected}) == 2
    assert all("--" in item.record.job_key for item in collected)
    assert all(JobRecord.from_mapping(item.record.as_mapping()) == item.record for item in collected)


def test_manifest_ids_survive_relocation_but_path_ids_do_not(tmp_path: Path) -> None:
    body = b"project-key\n\nmanifest-body\nsignature\n"
    roots = (tmp_path / "one", tmp_path / "two")
    tasks = []
    for root in roots:
        root.mkdir()
        task = _task(root, "ht.task.default.same.cleanup.0.unclaimed.3.finished", ("2021-01-01_00.00.00",))
        (task / "ht.manifest.bz2").write_bytes(bz2.compress(body))
        tasks.append(task)

    with_manifest = [next(finished_tasks(root)) for root in roots]
    assert with_manifest[0].immutable_id == with_manifest[1].immutable_id

    for task in tasks:
        (task / "ht.manifest.bz2").unlink()
    without_manifest = [next(finished_tasks(root)) for root in roots]
    assert without_manifest[0].immutable_id != without_manifest[1].immutable_id


def test_collect_finished_tree_reports_harvest_stats_and_stable_identity(tmp_path: Path) -> None:
    root = tmp_path / "Runs"
    root.mkdir()
    good = _task(root, "ht.task.default.good.cleanup.0.unclaimed.3.finished", ("2021-06-07_08.09.10",))
    (good / "ht.manifest.bz2").write_bytes(bz2.compress(b"project-key\n\nbody\nsignature\n"))
    _task(root, "ht.task.default.bad.cleanup.0.unclaimed.3.broken", ("2021-01-01_00.00.00",))
    _task(root, "ht.task.default.gone.cleanup.0.unclaimed.3.timeout", ("2021-01-01_00.00.00",))
    _task(root, "ht.task.default.empty.cleanup.0.unclaimed.3.finished", ())
    package = _package(tmp_path / "package")

    stats: dict[str, object] = {}
    collected = list(collect_finished_tree(root, workflow_dir=package, stats=stats))

    assert len(collected) == 1
    assert collected[0].identity_stable is True
    unfinished = stats["unfinished_by_status"]
    assert isinstance(unfinished, Mapping)
    assert dict(unfinished) == {"broken": 1, "timeout": 1}
    assert stats["skipped_no_rundir"] == 1
