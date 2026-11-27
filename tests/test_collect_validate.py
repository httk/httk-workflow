"""``httk collect --into --validate`` and ``httk v1 collect --into --validate``: opt-in record validation."""

import json
from pathlib import Path
from typing import Any

import pytest
from httk.core import DataRecord, FileRecord
from httk.core.cli import CLIContext

from conftest import configure_identity, register_ws
from httk.workflow.workflow_cli import command
from test_collect_inputs import _finished
from test_postprocess_store import _collect, _collect_postprocess, _lines, _postprocess, _setup
from test_v1_reader import _package, _task

pytest.importorskip("httk.store")


def _refuse(monkeypatch: pytest.MonkeyPatch, record_type: type) -> None:
    """Give *record_type* a validator that refuses every instance."""

    def refuse(cls: type, record: Any) -> None:
        raise ValueError(f"refused {cls.__name__} for the test")

    monkeypatch.setattr(record_type, "__httk_validate__", classmethod(refuse), raising=False)


@pytest.mark.parametrize("validate", [False, True], ids=["trusting", "validating"])
def test_collect_into_validates_only_when_asked(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, validate: bool
) -> None:
    _refuse(monkeypatch, DataRecord)
    ws = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "stored")
    argv = ["collect", "--workspace", name, "--allow-job-collector", "--into", str(tmp_path / "s.sqlite")]
    argv += ["--id-base", "httk.t", *(["--validate"] if validate else [])]
    status = command(argv, context)
    report = json.loads(capsys.readouterr().out.splitlines()[0])
    if validate:
        assert status != 0
        assert "refused DataRecord for the test" in report["storage_error"]
    else:
        assert status == 0 and "storage_error" not in report and report["stored"] is not None


@pytest.mark.parametrize("validate", [False, True], ids=["trusting", "validating"])
def test_collect_postprocess_validates_only_when_asked(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, validate: bool
) -> None:
    configure_identity()
    _package_dir, provider, ws = _setup(tmp_path)
    store = tmp_path / "store.sqlite"
    assert _collect(ws, store) == 0
    capsys.readouterr()
    _postprocess(provider, ws)
    _refuse(monkeypatch, FileRecord)
    status = _collect_postprocess(ws, store, *(["--validate"] if validate else []))
    report, summary = _lines(capsys)
    if validate:
        assert status != 0 and summary["errors"] == 1
        assert "refused FileRecord for the test" in report["storage_error"]
    else:
        assert status == 0 and report["stored"] is not None and "storage_error" not in report


@pytest.mark.parametrize("validate", [False, True], ids=["trusting", "validating"])
def test_v1_collect_into_validates_only_when_asked(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, validate: bool
) -> None:
    _refuse(monkeypatch, DataRecord)
    root = tmp_path / "Runs"
    root.mkdir()
    _task(root, "ht.task.default.cli.cleanup.0.unclaimed.3.finished", ("2021-01-01_00.00.00",))
    argv = ["v1", "collect", "--workflow-dir", str(_package(tmp_path / "package")), str(root)]
    argv += ["--into", str(tmp_path / "s.sqlite"), "--id-base", "httk.v1", *(["--validate"] if validate else [])]
    status = command(argv, CLIContext("httk", tmp_path))
    report = json.loads(capsys.readouterr().out.splitlines()[0])
    if validate:
        assert status == 1 and "refused DataRecord for the test" in report["storage_error"]
    else:
        assert status == 0 and "storage_error" not in report and report["stored"] is not None


def test_validate_needs_into(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    context = CLIContext("httk", tmp_path)
    for argv in (["collect", str(tmp_path)], ["v1", "collect", str(tmp_path), "--workflow-dir", "p"]):
        assert command([*argv, "--validate"], context) == 2, argv
        assert "--validate only applies with --into" in capsys.readouterr().err, argv
