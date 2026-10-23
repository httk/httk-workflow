"""Postprocess execution and CLI coverage."""

import json
import os
import sys
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

import v3_helpers as v3
from conftest import configure_identity, register_ws
from httk.workflow import _store, job_records
from httk.workflow.packages import MANIFEST_NAME, load_workflow_package
from httk.workflow.postprocessing import postprocess_root, run_postprocess_script
from httk.workflow.seals import job_seal_path
from httk.workflow.workflow_cli import command
from test_workflow_cli_packages import _POSCAR, _cli_package

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
    "path": os.environ.get("PATH"),
}))
print("postprocessed")
"""


def _finished(tmp_path: Path, *, register: bool = False):
    """Install the package, run one job that commits data to success, and return its record."""

    package = _cli_package(tmp_path / "package")
    (package / "run").write_text(v3.RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    script = package / "scripts" / "report.sh"
    script.write_text(_OBSERVING_SCRIPT, encoding="utf-8")
    script.chmod(0o755)
    provider = load_workflow_package(package, register=register)
    workspace = v3.workspace(tmp_path / "workspace")
    with v3.cli_owner(workspace) as owner:
        installed = _store.install(workspace, owner, package)
    parameters = {"put": {"start": {"data/result.txt": "committed"}}}
    job = v3.submit(workspace, installed, {"start": "succeed"}, members={"POSCAR": _POSCAR}, parameters=parameters)
    v3.run(workspace)
    return package, provider, workspace, job, next(iter(job_records(workspace)))


def test_postprocess_script_observes_the_record_contract(tmp_path: Path, monkeypatch) -> None:
    _package, provider, workspace, job, record = _finished(tmp_path)
    monkeypatch.setenv("HTTK_WORKFLOW_STEP", "leak")
    monkeypatch.setenv("HTTK_WORKFLOW_IS_RESTART", "1")
    monkeypatch.setenv("POSTPROCESS_USER", "kept")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

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
    assert record.data is not None and (record.data / "result.txt").read_text(encoding="utf-8") == "committed"
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
    # The script gets the interpreter-first PATH a runner gets.
    assert report["path"] == os.pathsep.join([os.path.dirname(sys.executable), "/usr/bin", "/bin"])


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
    configure_identity()
    _package, provider, _workspace, _job, record = _finished(tmp_path)
    # A succeeded job is sealed by its commit.
    seal_before = job_seal_path(record.payload).read_bytes()
    payload_before = tree_digest(record.payload)

    result = run_postprocess_script(provider, "report", record)

    assert result.returncode == 0
    # Output is outside the payload, so the seal is untouched and still verifies.
    assert not result.output_dir.is_relative_to(record.payload)
    assert job_seal_path(record.payload).read_bytes() == seal_before
    assert tree_digest(record.payload) == payload_before


def test_postprocess_directory_setting_overrides_the_root(tmp_path: Path) -> None:
    _package, provider, workspace, _job, record = _finished(tmp_path)
    analysis = tmp_path / "analysis"
    workspace.set_setting("postprocess.directory", str(analysis))
    output_root = postprocess_root(workspace)
    assert output_root == analysis
    result = run_postprocess_script(provider, "report", record, output_root=output_root)
    expected = analysis.joinpath(*record.placement.parts, record.job_key, "report")
    assert result.output_dir == expected
    assert (result.output_dir / "report.json").is_file()


def test_postprocess_output_dir_override_wins(tmp_path: Path) -> None:
    _package, provider, workspace, _job, record = _finished(tmp_path)
    workspace.set_setting("postprocess.directory", str(tmp_path / "ignored"))
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


def test_postprocess_directory_setting_must_be_absolute_and_outside(tmp_path: Path) -> None:
    _package, _provider, workspace, _job, _record = _finished(tmp_path)
    workspace.set_setting("postprocess.directory", "analysis")
    with pytest.raises(ValueError, match="absolute path outside the workspace"):
        postprocess_root(workspace)
    workspace.set_setting("postprocess.directory", str(workspace.root / "analysis"))
    with pytest.raises(ValueError, match="outside the workspace"):
        postprocess_root(workspace)


def test_postprocess_output_dir_override_must_be_outside_the_workspace(tmp_path: Path, monkeypatch) -> None:
    _package, _provider, workspace, _job, _record = _finished(tmp_path)
    with pytest.raises(ValueError, match="outside the workspace"):
        postprocess_root(workspace, str(workspace.root / "analysis"))
    monkeypatch.chdir(workspace.root)
    with pytest.raises(ValueError, match="outside the workspace"):
        postprocess_root(workspace, "analysis")  # relative: resolves against the current directory
    monkeypatch.chdir(tmp_path)
    assert postprocess_root(workspace, "rel-out") == tmp_path / "rel-out"


def test_postprocess_root_refuses_the_control_directory(tmp_path: Path) -> None:
    _package, _provider, workspace, _job, _record = _finished(tmp_path)
    with pytest.raises(ValueError, match="control directory"):
        postprocess_root(workspace, str(workspace.control / "seals"))


def test_postprocess_root_refuses_a_job_payload(tmp_path: Path) -> None:
    _package, _provider, workspace, _job, record = _finished(tmp_path)
    with pytest.raises(ValueError, match="job payload"):
        postprocess_root(workspace, str(record.payload / "sub"))


def test_postprocess_cli_unknown_job_id_exits_one(tmp_path: Path, capsys) -> None:
    package, _provider, workspace, _job, _record = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "postprocess-unknown")
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
                "nope",
            ],
            context,
        )
        == 1
    )
    assert "nope" in capsys.readouterr().err


def test_postprocess_cli_selector_with_state_is_a_usage_error(tmp_path: Path, capsys) -> None:
    package, _provider, workspace, _job, record = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "postprocess-both")
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
                "--state",
                "succeeded",
                record.job_id,
            ],
            context,
        )
        == 2
    )
    assert "state" in capsys.readouterr().err
    base = ["postprocess", "--workspace", workspace_name, "--script", "report", "--workflow-dir", str(package)]
    assert command([*base, "--step", "start", record.job_id], context) == 2
    assert "--step" in capsys.readouterr().err
    # Without job selectors --step selects: a job at another step is left out.
    assert command([*base, "--step", "elsewhere"], context) == 0
    assert capsys.readouterr().out == ""
    assert command([*base, "--step", str(record.job["initial_step"])], context) == 0
    assert capsys.readouterr().out.split("\t")[:3] == [record.job_key, "report", "0"]


# -- declared outputs and the postprocess receipt -----------------------------

_RECEIPT_KEYS = {
    "format",
    "format_version",
    "script",
    "workflow_id",
    "workspace_id",
    "job_id",
    "started_at",
    "finished_at",
    "returncode",
    "outputs",
}


def _declaring(tmp_path: Path, outputs: str) -> Path:
    """Return the CLI package with ``outputs = <outputs>`` appended to its (last) postprocess table."""

    package = _cli_package(tmp_path / "package")
    manifest = package / MANIFEST_NAME
    manifest.write_text(manifest.read_text(encoding="utf-8") + f"outputs = {outputs}\n", encoding="utf-8")
    return package


def test_postprocess_outputs_are_validated_and_carried_by_the_provider(tmp_path: Path) -> None:
    package = _declaring(tmp_path, '["summary.json", "figures/**/*.png", "data/*.csv"]')
    provider = load_workflow_package(package, register=False)
    assert provider.postprocess_scripts["report"]["outputs"] == ("summary.json", "figures/**/*.png", "data/*.csv")
    assert (
        load_workflow_package(_cli_package(tmp_path / "plain"), register=False).postprocess_scripts["report"]["outputs"]
        is None
    )


@pytest.mark.parametrize(
    "outputs",
    [
        '"summary.json"',
        "[]",
        '["a\\tb.json"]',
        '["a\\u0000b.json"]',
        "[1]",
        '[""]',
        '["a.json", "a.json"]',
        '["/abs.json"]',
        '["a\\\\b.json"]',
        '["a//b.json"]',
        '["./a.json"]',
        '["../a.json"]',
        '["a/../b.json"]',
        '[".hidden"]',
        '["a/.git/x"]',
        '["a/"]',
        '["a**/b"]',
    ],
)
def test_postprocess_outputs_reject_malformed_patterns(tmp_path: Path, outputs: str) -> None:
    with pytest.raises(ValueError, match=r"\[workflow\.postprocess\.report\]\.outputs"):
        load_workflow_package(_declaring(tmp_path, outputs), register=False)


def test_postprocess_tables_still_reject_unknown_keys(tmp_path: Path) -> None:
    package = _cli_package(tmp_path / "package")
    manifest = package / MANIFEST_NAME
    manifest.write_text(manifest.read_text(encoding="utf-8") + 'inputs = ["energy"]\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"unknown key \[workflow\.postprocess\.report\.inputs\]"):
        load_workflow_package(package, register=False)


def test_describe_shows_declared_outputs(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    package = _declaring(tmp_path, '["summary.json", "*.png"]')
    assert command(["describe", str(package)], context) == 0
    assert "report: scripts/report.sh — write a report (outputs: summary.json, *.png)" in capsys.readouterr().out
    assert command(["describe", "--json", str(package)], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["postprocess"]["report"]["outputs"] == ["summary.json", "*.png"]


def test_postprocess_writes_a_receipt_on_success_and_on_failure(tmp_path: Path) -> None:
    package, provider, workspace, job, record = _finished(tmp_path)
    declared = replace(
        provider, postprocess_scripts={"report": {**provider.postprocess_scripts["report"], "outputs": ("a.json",)}}
    )

    result = run_postprocess_script(declared, "report", record)

    assert result.receipt == result.output_dir / ".httk-postprocess.json"
    receipt = json.loads(result.receipt.read_text(encoding="utf-8"))
    assert set(receipt) == _RECEIPT_KEYS
    assert receipt["format"] == "httk-workflow-postprocess-receipt" and receipt["format_version"] == 1
    assert receipt["script"] == "report"
    assert receipt["workflow_id"] == provider.workflow_id
    assert (receipt["workspace_id"], receipt["job_id"]) == (workspace.workspace_id, job.job_id)
    assert receipt["returncode"] == 0 and receipt["outputs"] == ["a.json"]
    started = datetime.fromisoformat(receipt["started_at"])
    finished = datetime.fromisoformat(receipt["finished_at"])
    assert receipt["started_at"].endswith("Z") and receipt["finished_at"].endswith("Z")
    assert started.tzinfo is not None and started <= finished

    script = package / "scripts" / "report.sh"
    script.write_text("#!/bin/sh\nexit 4\n", encoding="utf-8")
    script.chmod(0o755)
    failed = run_postprocess_script(provider, "report", record)
    receipt = json.loads(failed.receipt.read_text(encoding="utf-8"))
    assert set(receipt) == _RECEIPT_KEYS
    assert receipt["returncode"] == 4 and receipt["outputs"] is None


@pytest.mark.timing
def test_postprocess_removes_a_stale_receipt_and_leaves_none_on_timeout(tmp_path: Path) -> None:
    package, provider, _workspace, _job, record = _finished(tmp_path)
    stale = run_postprocess_script(provider, "report", record).receipt
    assert stale.is_file()
    script = package / "scripts" / "report.sh"
    script.write_text("#!/bin/sh\nsleep 1\n", encoding="utf-8")
    script.chmod(0o755)

    with pytest.raises(ValueError, match="report.*job"):
        run_postprocess_script(provider, "report", record, timeout=0.05)
    assert not stale.exists()
