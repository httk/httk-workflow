"""Workflow ``requires``: manifest parsing, resolution-time refusal, claim-time re-check, and the runner interpreter."""

import json
import os
import re
import sys
from pathlib import Path

import pytest
from httk.core.requirements import RequirementError

from httk.workflow import TaskManager, _store
from httk.workflow import precheck as precheck_module
from httk.workflow.introspection import explain_job, resolve_job
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.scaffold import describe_runner, resolve_workflow
from test_manager_scheduling import _SUCCEED, _install, _submit
from v3_helpers import cli_owner, find
from v3_helpers import workspace as initialize_workspace

UNMET = "httk-no-such-distribution>=1"
UNMET_MESSAGE = f"{UNMET} (not installed)"


def _package(root: Path, requires: str, *, name: str = "tests.requires") -> Path:
    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "{name}"\nrequires = {requires}\n\n[workflow.runner]\nsteps = ["start"]\n',
        encoding="utf-8",
    )
    (root / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (root / "run").chmod(0o755)
    return root


def test_manifest_requires_parse_and_reject_malformed_entries(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_package(tmp_path / "ok", '["httk-core >= 0", "httk_workflow>=2.0"]'))
    assert provider.requires == ("httk-core>=0", "httk_workflow>=2.0")
    for index, bad in enumerate(('["httk-core<3"]', '"httk-core>=0"', '["httk-core>=0", "httk_core>=1"]', "[1]")):
        with pytest.raises(ValueError, match=r"\[workflow\]\.requires"):
            parse_workflow_manifest(_package(tmp_path / f"bad{index}", bad))


def test_unmet_requires_refuse_resolution_and_met_requires_reach_the_installed_workflow(tmp_path: Path) -> None:
    expected = re.escape(f"workflow 'tests.requires' has unmet requirements: {UNMET_MESSAGE}")
    with pytest.raises(RequirementError, match=expected):
        resolve_workflow(_package(tmp_path / "unmet", f'["httk-core>=0", "{UNMET}"]'))

    # The manager reads requires from the installed workflow's record, which the install copies from the manifest.
    workspace = initialize_workspace(tmp_path / "workspace")
    with cli_owner(workspace) as owner:
        met = _store.install(workspace, owner, _package(tmp_path / "met", '["httk-core>=0"]', name="tests.met"))
        plain = _store.install(workspace, owner, _package(tmp_path / "none", "[]", name="tests.plain"))
    assert met.record["requires"] == ["httk-core>=0"]
    assert plain.record["requires"] == []


def test_manager_skips_an_unmet_job_reports_it_and_claims_a_met_one(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    unmet = _install(workspace, tmp_path / "unmet", _SUCCEED, name="tests.unmet", workflow=f'requires = ["{UNMET}"]')
    met = _install(workspace, tmp_path / "met", _SUCCEED, name="tests.met", workflow='requires = ["httk-core>=0"]')
    unmet_id = _submit(workspace, unmet, tag="unmet")
    met_id = _submit(workspace, met, tag="met")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        census = manager.run_until_idle(timeout=60.0)
        assert census.ready_blocked["requirements"] == {UNMET_MESSAGE: 1}
        assert f"requires {UNMET_MESSAGE}: 1" in census.summary_line()
        advice = census.mismatch_advice()
        assert advice is not None and UNMET_MESSAGE in advice

        # precheck and job why run in the operator's process; a live manager makes a miss indeterminate.
        findings = {item["job_id"]: item for item in precheck_module.precheck_jobs(workspace)}
        requirements = findings[unmet_id]["requirements"]
        assert isinstance(requirements, dict) and requirements["status"] == "indeterminate"
        assert UNMET_MESSAGE in requirements["problem"]
        check = next(
            item
            for item in explain_job(workspace, resolve_job(workspace, unmet_id)).checks
            if item.name == "required distributions"
        )
        assert check.satisfied is False and UNMET_MESSAGE in check.detail

    assert find(workspace, unmet_id).state == "ready"
    assert find(workspace, met_id).state == "succeeded"
    finding = next(item for item in precheck_module.precheck_jobs(workspace) if item["job_id"] == unmet_id)
    assert precheck_module.has_requirements_problem(finding)


_WHICH = (
    "import shutil\n"
    'Path("which.json").write_text(json.dumps([shutil.which("python3"), sys.executable]))\n'
    'publish("succeed")\n'
)


def test_runners_and_describe_run_under_the_managers_interpreter(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    interpreter = os.path.dirname(sys.executable)
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _submit(workspace, _install(workspace, tmp_path / "package", _WHICH), tag="which")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    ref = find(workspace, job_id)
    assert ref.state == "succeeded"
    which, executable = json.loads((ref.path / "run" / "which.json").read_text())
    assert os.path.dirname(which) == interpreter and os.path.dirname(executable) == interpreter

    runner = tmp_path / "describe.py"
    runner.write_text(
        '#!/usr/bin/env python3\nfrom httk.workflow import Runner\nrun = Runner("tests.describe")\n\n\n'
        '@run.step\ndef start(a):\n    a.succeed()\n\n\nraise SystemExit(run.main())\n',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    assert describe_runner(runner)["workflow"] == "tests.describe"


def test_a_prelude_wrapped_runner_still_runs_under_the_managers_interpreter(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    workspace = initialize_workspace(tmp_path / "workspace")
    workspace.set_workflow_prelude("tests.ux", "true")
    job_id = _submit(workspace, _install(workspace, tmp_path / "package", _WHICH, name="tests.ux"), tag="prelude")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    ref = find(workspace, job_id)
    assert ref.state == "succeeded"
    which, _ = json.loads((ref.path / "run" / "which.json").read_text())
    assert os.path.dirname(which) == os.path.dirname(sys.executable)
