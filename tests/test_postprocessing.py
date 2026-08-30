"""Postprocess execution and CLI coverage."""

import json
import time
from dataclasses import replace
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, job_records
from httk.workflow.configuration import ensure_identity_key
from httk.workflow.packages import load_workflow_package
from httk.workflow.postprocessing import postprocess_root, run_postprocess_script
from httk.workflow.scaffold import new_job
from httk.workflow.seals import job_seal_path, seal_job, verify_job_seal
from httk.workflow.workflow_cli import command
from test_workflow_cli_packages import _POSCAR, _SUCCESS_RUNNER, _cli_package

_OBSERVING_SCRIPT = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

names = (
    "HTTK_WORKFLOW_WORKSPACE_DIR",
    "HTTK_WORKFLOW_JOB_DIR",
    "HTTK_WORKFLOW_WORKDIR",
    "HTTK_WORKFLOW_DATA_DIR",
    "HTTK_WORKFLOW_POSTPROCESS_DIR",
    "HTTK_WORKFLOW_CONTEXT",
    "HTTK_WORKFLOW_CONTROL_DIR",
    "HTTK_WORKFLOW_STEP",
)
Path("report.json").write_text(json.dumps({
    **{name: os.environ.get(name) for name in names},
    "reserved": sorted(name for name in os.environ if name.startswith("HTTK_WORKFLOW_")),
    "user": os.environ.get("POSTPROCESS_USER"),
}))
print("postprocessed")
"""


def _finished(tmp_path: Path, *, register: bool = False):
    package = _cli_package(tmp_path / "package")
    (package / "run").write_text(_SUCCESS_RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8")
    (package / "httk_workflow.toml").write_text(
        manifest.replace('steps = ["start"]', 'steps = ["start"]\ndata_mode = "transactional"'),
        encoding="utf-8",
    )
    script = package / "scripts" / "report.sh"
    script.write_text(_OBSERVING_SCRIPT, encoding="utf-8")
    script.chmod(0o755)
    provider = load_workflow_package(package, register=register)
    workspace = Workspace.initialize(tmp_path / "workspace")
    structure = tmp_path / "POSCAR"
    structure.write_text(_POSCAR, encoding="utf-8")
    job = new_job(workspace, package, inputs={"structure": structure})
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    return package, provider, workspace, job, next(iter(job_records(workspace)))


def test_postprocess_script_observes_the_record_contract(tmp_path: Path, monkeypatch) -> None:
    _package, provider, workspace, job, record = _finished(tmp_path)
    monkeypatch.setenv("HTTK_WORKFLOW_STEP", "leak")
    monkeypatch.setenv("HTTK_WORKFLOW_IS_RESTART", "1")
    monkeypatch.setenv("POSTPROCESS_USER", "kept")

    result = run_postprocess_script(provider, "report", record)

    assert result.script == "report"
    assert result.workspace_id == workspace.workspace_id
    assert result.job_id == job.job_id
    assert result.returncode == 0
    assert result.stdout == "postprocessed\n"
    # Output lands under the workspace-level postprocess tree, not the payload.
    expected = workspace.root.joinpath("postprocess", *record.placement.parts, record.job_key, "report")
    assert result.output_dir == expected
    assert not result.output_dir.is_relative_to(record.payload)
    report = json.loads((result.output_dir / "report.json").read_text(encoding="utf-8"))
    assert report["HTTK_WORKFLOW_WORKSPACE_DIR"] == str(record.workspace_root)
    assert report["HTTK_WORKFLOW_JOB_DIR"] == str(record.payload)
    assert report["HTTK_WORKFLOW_WORKDIR"] == str(record.workdir)
    assert report["HTTK_WORKFLOW_DATA_DIR"] == str(record.data)
    assert report["HTTK_WORKFLOW_POSTPROCESS_DIR"] == str(result.output_dir)
    assert report["reserved"] == [
        "HTTK_WORKFLOW_DATA_DIR",
        "HTTK_WORKFLOW_JOB_DIR",
        "HTTK_WORKFLOW_POSTPROCESS_DIR",
        "HTTK_WORKFLOW_WORKDIR",
        "HTTK_WORKFLOW_WORKSPACE_DIR",
    ]
    assert report["user"] == "kept"


def test_postprocess_script_rejects_untrusted_members_and_output_paths(tmp_path: Path) -> None:
    _package, provider, _workspace, _job, record = _finished(tmp_path)
    escape = tmp_path / "escape.sh"
    escape.write_text("#!/bin/sh\n", encoding="utf-8")
    escape.chmod(0o755)

    with pytest.raises(ValueError, match="relative"):
        run_postprocess_script(
            replace(provider, postprocess_scripts={"report": {"file": "../escape.sh"}}),
            "report",
            record,
        )
    with pytest.raises(ValueError, match="relative"):
        run_postprocess_script(replace(provider, postprocess_scripts={"report": {"file": "/tmp/x"}}), "report", record)
    with pytest.raises(ValueError, match="single path component"):
        run_postprocess_script(provider, "a/b", record)

    outside = tmp_path / "outside"
    outside.mkdir()
    output_dir = _workspace.root.joinpath("postprocess", *record.placement.parts, record.job_key, "report")
    output_dir.parent.mkdir(parents=True)
    output_dir.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="output directory.*unsafe"):
        run_postprocess_script(provider, "report", record)


def test_postprocess_script_validates_selection_and_source(tmp_path: Path) -> None:
    package, provider, _workspace, _job, record = _finished(tmp_path)

    with pytest.raises(ValueError, match="report"):
        run_postprocess_script(provider, "missing", record)
    script = package / "scripts" / "report.sh"
    script.chmod(0o644)
    with pytest.raises(ValueError, match=r"not executable.*chmod \+x.*shebang"):
        run_postprocess_script(provider, "report", record)

    script.chmod(0o755)
    # A job without a workdir is still postprocessable from its payload.
    result = run_postprocess_script(provider, "report", replace(record, workdir_path=None))
    assert result.returncode == 0 and result.output_dir.is_dir()


@pytest.mark.timing
def test_postprocess_script_wraps_timeout(tmp_path: Path) -> None:
    package, provider, _workspace, _job, record = _finished(tmp_path)
    script = package / "scripts" / "report.sh"
    script.write_text("#!/bin/sh\nsleep 1\n", encoding="utf-8")
    script.chmod(0o755)

    started = time.monotonic()
    with pytest.raises(ValueError, match="report.*job"):
        run_postprocess_script(provider, "report", record, timeout=0.05)
    assert time.monotonic() - started < 1


def test_postprocess_cli_streams_results_errors_and_workflow_dir(tmp_path: Path, capsys) -> None:
    package, _provider, workspace, _job, record = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "postprocess")

    assert (
        command(
            ["postprocess", "--workspace", workspace_name, "--script", "report", "--workflow-dir", str(package)],
            context,
        )
        == 0
    )
    line = capsys.readouterr().out.strip().split("\t")
    assert line[:3] == [record.job_key, "report", "0"]
    assert Path(line[3]).is_dir()

    assert command(["postprocess", "--workspace", workspace_name, "--script", "report", "--json"], context) == 1
    error = json.loads(capsys.readouterr().out)
    assert error["job_key"] == record.job_key
    assert "not registered" in error["error"]

    script = package / "scripts" / "report.sh"
    script.write_text("#!/bin/sh\nexit 4\n", encoding="utf-8")
    script.chmod(0o755)
    assert (
        command(
            [
                "postprocess",
                "--workspace",
                workspace_name,
                "--script",
                "report",
                "--workflow-dir",
                str(package),
                "--json",
            ],
            context,
        )
        == 1
    )
    failed = json.loads(capsys.readouterr().out)
    assert failed["returncode"] == 4


def test_postprocess_cli_surfaces_failing_script_stderr(tmp_path: Path, capsys) -> None:
    package, _provider, workspace, _job, _record = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "postprocess-stderr")
    script = package / "scripts" / "report.sh"
    script.write_text("#!/bin/sh\necho 'boom: it failed' 1>&2\nexit 5\n", encoding="utf-8")
    script.chmod(0o755)

    # The JSON report carries the failing script's stderr tail.
    assert (
        command(
            [
                "postprocess",
                "--workspace",
                workspace_name,
                "--script",
                "report",
                "--workflow-dir",
                str(package),
                "--json",
            ],
            context,
        )
        == 1
    )
    failed = json.loads(capsys.readouterr().out)
    assert failed["returncode"] == 5
    assert "boom: it failed" in failed["stderr"]

    # The human form appends the last stderr line to the row.
    assert (
        command(
            ["postprocess", "--workspace", workspace_name, "--script", "report", "--workflow-dir", str(package)],
            context,
        )
        == 1
    )
    assert "boom: it failed" in capsys.readouterr().out


def test_postprocess_of_a_sealed_job_works_and_leaves_the_seal_valid(tmp_path: Path) -> None:
    ensure_identity_key()
    _package, provider, workspace, _job, record = _finished(tmp_path)
    marker = workspace.find_marker_by_id(record.job_id)
    assert marker is not None
    seal_job(workspace, marker)
    seal_before = job_seal_path(workspace, marker.job_key).read_bytes()

    result = run_postprocess_script(provider, "report", record)

    assert result.returncode == 0
    # Output is outside the payload, so the seal is untouched and still verifies.
    assert not result.output_dir.is_relative_to(record.payload)
    assert job_seal_path(workspace, marker.job_key).read_bytes() == seal_before
    assert verify_job_seal(workspace, marker).valid


def test_postprocess_directory_setting_overrides_the_root(tmp_path: Path) -> None:
    _package, provider, workspace, _job, record = _finished(tmp_path)
    workspace.set_setting("postprocess.directory", "analysis")
    output_root = postprocess_root(workspace)
    assert output_root == workspace.root / "analysis"
    result = run_postprocess_script(provider, "report", record, output_root=output_root)
    expected = workspace.root.joinpath("analysis", *record.placement.parts, record.job_key, "report")
    assert result.output_dir == expected
    assert (result.output_dir / "report.json").is_file()


def test_postprocess_output_dir_override_wins(tmp_path: Path) -> None:
    _package, provider, workspace, _job, record = _finished(tmp_path)
    workspace.set_setting("postprocess.directory", "ignored")
    absolute = tmp_path / "elsewhere"
    output_root = postprocess_root(workspace, str(absolute))
    assert output_root == absolute
    result = run_postprocess_script(provider, "report", record, output_root=output_root)
    assert result.output_dir == absolute.joinpath(*record.placement.parts, record.job_key, "report")
    assert (result.output_dir / "report.json").is_file()


def test_postprocess_cli_output_dir_flag_places_output(tmp_path: Path, capsys) -> None:
    package, _provider, workspace, _job, _record = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "postprocess-outdir")
    target = tmp_path / "cli-out"
    assert (
        command(
            [
                "postprocess",
                "--workspace",
                workspace_name,
                "--script",
                "report",
                "--workflow-dir",
                str(package),
                "--output-dir",
                str(target),
                "--json",
            ],
            context,
        )
        == 0
    )
    line = json.loads(capsys.readouterr().out)
    assert Path(line["output_dir"]).is_relative_to(target)
    assert (Path(line["output_dir"]) / "report.json").is_file()
