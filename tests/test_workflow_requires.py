"""Workflow ``requires``: manifest parsing, creation-time refusal, claim-time re-check, and the runner interpreter."""

import json
import os
import re
import sys
from pathlib import Path

import pytest
from httk.core.requirements import RequirementError

from httk.workflow import TaskManager, Workspace
from httk.workflow import precheck as precheck_module
from httk.workflow.errors import FormatError
from httk.workflow.introspection import explain_job, resolve_job
from httk.workflow.models import JobDefinition
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.scaffold import describe_runner, new_job, resolve_workflow
from test_ux_tier_b import _SUCCEED_RUNNER, _payload

UNMET = "httk-no-such-distribution>=1"
UNMET_MESSAGE = f"{UNMET} (not installed)"


def _package(root: Path, requires: str) -> Path:
    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "tests.requires"\nrequires = {requires}\n\n[workflow.runner]\nsteps = ["start"]\n',
        encoding="utf-8",
    )
    (root / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (root / "run").chmod(0o755)
    return root


def _requiring_payload(root: Path, tag: str, requires: list[str], runner: str = _SUCCEED_RUNNER) -> tuple[Path, str]:
    payload, job_id = _payload(root, runner, tag=tag)
    document = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    document["requires"] = requires
    (payload / "job.json").write_text(json.dumps(document), encoding="utf-8")
    return payload, job_id


def test_manifest_requires_parse_and_reject_malformed_entries(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_package(tmp_path / "ok", '["httk-core >= 0", "httk_workflow>=2.0"]'))
    assert provider.requires == ("httk-core>=0", "httk_workflow>=2.0")
    for index, bad in enumerate(('["httk-core<3"]', '"httk-core>=0"', '["httk-core>=0", "httk_core>=1"]', "[1]")):
        with pytest.raises(ValueError, match=r"\[workflow\]\.requires"):
            parse_workflow_manifest(_package(tmp_path / f"bad{index}", bad))


def test_unmet_requires_refuse_resolution_and_met_requires_reach_job_json(tmp_path: Path) -> None:
    expected = re.escape(f"workflow 'tests.requires' has unmet requirements: {UNMET_MESSAGE}")
    with pytest.raises(RequirementError, match=expected):
        resolve_workflow(_package(tmp_path / "unmet", f'["httk-core>=0", "{UNMET}"]'))

    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, _package(tmp_path / "met", '["httk-core>=0"]'))
    document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    assert document["requires"] == ["httk-core>=0"]
    assert JobDefinition.from_mapping(document).requires == ("httk-core>=0",)

    plain = new_job(workspace, _package(tmp_path / "none", "[]"))
    assert "requires" not in json.loads((plain.payload / "job.json").read_text(encoding="utf-8"))
    document["requires"] = ["httk-core"]
    with pytest.raises(FormatError, match="NAME>=VERSION"):
        JobDefinition.from_mapping(document)


def test_manager_skips_an_unmet_job_reports_it_and_claims_a_met_one(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    unmet_payload, unmet_id = _requiring_payload(tmp_path / "source", "unmet", [UNMET])
    met_payload, met_id = _requiring_payload(tmp_path / "source", "met", ["httk-core>=0"])
    workspace.submit(unmet_payload, "project/unmet")
    workspace.submit(met_payload, "project/met")

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

    unmet_marker = workspace.find_marker_by_id(unmet_id)
    met_marker = workspace.find_marker_by_id(met_id)
    assert unmet_marker is not None and unmet_marker.kind == "ready"
    assert met_marker is not None and met_marker.kind == "succeeded"
    finding = next(item for item in precheck_module.precheck_jobs(workspace) if item["job_id"] == unmet_id)
    assert precheck_module.has_requirements_problem(finding)


_WHICH_RUNNER = (
    _SUCCEED_RUNNER.replace("import json, os", "import json, os, shutil, sys")
    + 'Path("which.json").write_text(json.dumps([shutil.which("python3"), sys.executable]))\n'
)


def test_runners_and_describe_run_under_the_managers_interpreter(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    interpreter = os.path.dirname(sys.executable)
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _WHICH_RUNNER, tag="which")
    workspace.submit(payload, "project/which")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    which, executable = json.loads(
        (workspace.payload_path(marker.placement, marker.job_key) / "run" / "which.json").read_text()
    )
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
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_workflow_prelude("tests.ux", "true")
    payload, job_id = _payload(tmp_path / "source", _WHICH_RUNNER, tag="prelude")
    workspace.submit(payload, "project/prelude")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    payload_dir = workspace.payload_path(marker.placement, marker.job_key)
    which, _ = json.loads((payload_dir / "run" / "which.json").read_text())
    assert os.path.dirname(which) == os.path.dirname(sys.executable)
