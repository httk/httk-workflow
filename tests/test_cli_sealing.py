"""The seal/unseal CLI surface: writing, removing, and verifying seals."""

import base64
import json
import sys
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.crypto import ed25519_generate_seed
from httk.core.project.cli import command as project_command

import v3_helpers as v3
from conftest import configure_identity
from httk.workflow import TaskManager, Workspace, _kernel
from httk.workflow._state import Release, StateDoc
from httk.workflow.projects import initialize_project
from httk.workflow.seals import is_job_sealed, is_project_sealed, is_workspace_sealed, job_seal_path
from httk.workflow.workflow_cli import command

# The library side (seal and unseal requests, the workspace snapshot, verify) is covered by test_seals and
# test_sealing_runtime; these drive the CLI handlers. The workspace and verify verbs wait for C5b-2.
_C5B2 = pytest.mark.skip(reason="C5b-2: the workspace seal and seal verify CLI verbs are rewritten on the kernel")

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


def _init_workspace(project: Path) -> Workspace:
    """Create ``project/workspace``, register it as ``default``, and record it as the project default."""

    from httk.workflow.projects import write_project_section
    from httk.workflow.registry import create_workspace

    create_workspace("default", project / "workspace")
    write_project_section(project, "workspace", {"default": "default"})
    return Workspace(project / "workspace")


def _payload(root: Path, tag: str) -> tuple[Path, str]:
    """Create a minimal complete payload that succeeds when run."""

    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    runner_path = files / "runner"
    runner_path.write_text(_SUCCEED_RUNNER, encoding="utf-8")
    runner_path.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": tag,
                "name": tag,
                "workflow": "tests.seal",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "only",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
                "parent": None,
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def _setup(tmp_path: Path) -> tuple[Path, Workspace, str]:
    """Build a project whose root workspace holds one succeeded job."""

    project_root = tmp_path / "project"
    project_root.mkdir()
    initialize_project(project_root, name="sealing")
    configure_identity()
    workspace = _init_workspace(project_root)
    # Drive the job to succeeded without the manager auto-sealing it, so these
    # tests exercise the explicit seal commands from a known unsealed start.
    workspace.set_setting("seal.succeeded", "false")
    payload, job_id = _payload(tmp_path / "source", "silicon")
    workspace.submit(payload, "jobs/silicon")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30)
    assert workspace.find_marker_by_id(job_id) is not None
    return project_root, workspace, job_id


def _context(cwd: Path) -> CLIContext:
    return CLIContext("httk", cwd)


def _succeeded(tmp_path: Path) -> tuple[Path, Workspace, _kernel.JobRef]:
    """A project whose default workspace holds one succeeded job without a seal."""

    project_root = tmp_path / "project"
    project_root.mkdir()
    initialize_project(project_root, name="sealing")
    configure_identity()
    workspace = _init_workspace(project_root)
    ref = v3.submit(workspace, ("demo--0123456789abcdef", "demo"), {"start": "succeed"}, placement="jobs/silicon")
    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        ref = owned.release(StateDoc.empty(owned.job_id).next_activation("start", "initial"), Release("succeeded", 500))
    return project_root, workspace, ref


def test_job_seal_writes_a_seal_and_refuses_twice(tmp_path: Path, capsys) -> None:
    project_root, workspace, ref = _succeeded(tmp_path)

    assert command(["job", "seal", ref.job_id], _context(project_root)) == 0
    assert capsys.readouterr().out == f"{ref.job_id}\tsealed\n"
    payload = v3.find(workspace, ref.job_id).path
    assert is_job_sealed(payload) and job_seal_path(payload).is_file()
    assert command(["job", "seal", ref.job_id], _context(project_root)) == 1
    assert "the job is already sealed" in capsys.readouterr().out


def test_job_seal_refuses_a_job_that_did_not_succeed(tmp_path: Path, capsys) -> None:
    project_root, workspace, _ref = _succeeded(tmp_path)
    ready = v3.submit(workspace, ("demo--0123456789abcdef", "demo"), {"start": "succeed"}, placement="jobs/ready")

    assert command(["job", "seal", ready.job_id], _context(project_root)) == 1
    assert "only succeeded jobs are sealed, not ready" in capsys.readouterr().out


def test_job_unseal_declined_then_forced_and_then_deletable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    project_root, workspace, ref = _succeeded(tmp_path)
    assert command(["job", "seal", ref.job_id], _context(project_root)) == 0
    assert command(["job", "delete", "--force", ref.job_id], _context(project_root)) == 1
    assert "release it with `job unseal` first" in capsys.readouterr().out

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")
    assert command(["job", "unseal", ref.job_id], _context(project_root)) == 1
    assert "not removed" in capsys.readouterr().out
    assert is_job_sealed(v3.find(workspace, ref.job_id).path)

    assert command(["job", "unseal", "--force", ref.job_id], _context(project_root)) == 0
    assert capsys.readouterr().out == f"{ref.job_id}\tunsealed\n"
    assert not is_job_sealed(v3.find(workspace, ref.job_id).path)
    assert command(["job", "delete", "--force", ref.job_id], _context(project_root)) == 0
    assert _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True) is None


@_C5B2
def test_workspace_seal_refuses_unsealed_jobs_then_forces(tmp_path: Path, capsys) -> None:
    project_root, workspace, job_id = _setup(tmp_path)

    assert command(["workspace", "seal"], _context(project_root)) == 1
    err = capsys.readouterr().err
    assert job_id in err
    assert not is_workspace_sealed(workspace)

    assert command(["workspace", "seal", "--force"], _context(project_root)) == 0
    out = capsys.readouterr().out
    assert "\tsealed\t" in out
    assert is_workspace_sealed(workspace)


@_C5B2
def test_project_seal_then_verify_ok_and_tamper_fails(tmp_path: Path, capsys) -> None:
    project_root, workspace, job_id = _setup(tmp_path)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None

    assert command(["workspace", "seal", "--force"], _context(project_root)) == 0
    capsys.readouterr()
    assert project_command(["seal"], _context(project_root)) == 0
    assert "\tsealed\t" in capsys.readouterr().out
    assert is_project_sealed(project_root)

    assert command(["seal", "verify"], _context(project_root)) == 0
    verified = capsys.readouterr().out
    assert verified.strip().splitlines()[-1] == "ok"
    assert "valid_trusted" in verified

    payload = workspace.payload_path(marker.placement, marker.job_key)
    (payload / "files" / "runner").write_text("#!/bin/sh\necho tampered\n", encoding="utf-8")

    assert command(["seal", "verify"], _context(project_root)) == 1
    tampered = capsys.readouterr().out
    assert tampered.strip().splitlines()[-1] == "FAILED"
    assert "  mismatch\tfiles/runner" in tampered


@_C5B2
def test_seal_verify_json_shape(tmp_path: Path, capsys) -> None:
    project_root, _workspace, _job_id = _setup(tmp_path)
    assert command(["workspace", "seal", "--force"], _context(project_root)) == 0
    assert project_command(["seal"], _context(project_root)) == 0
    capsys.readouterr()

    assert command(["seal", "verify", "--json"], _context(project_root)) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["ok"] is True and document["trusted"] is True
    assert isinstance(document["entries"], list) and document["entries"]
    first = document["entries"][0]
    assert first["level"] == "project"
    assert set(first) >= {"level", "subject", "valid", "verdict", "reason", "signers", "discrepancies"}


def test_confirm_non_tty_refuses_without_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    project_root, workspace, ref = _succeeded(tmp_path)
    assert command(["job", "seal", ref.job_id], _context(project_root)) == 0
    capsys.readouterr()

    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    assert command(["job", "unseal", ref.job_id], _context(project_root)) == 1
    assert "requires --force" in capsys.readouterr().err
    assert is_job_sealed(v3.find(workspace, ref.job_id).path)


@_C5B2
def test_job_show_and_workspace_status_report_sealed(tmp_path: Path, capsys) -> None:
    project_root, _workspace, job_id = _setup(tmp_path)

    assert command(["job", "show", job_id], _context(project_root)) == 0
    assert "sealed: no" in capsys.readouterr().out
    assert command(["workspace", "status"], _context(project_root)) == 0
    assert "sealed: no" in capsys.readouterr().out

    assert command(["job", "seal", job_id], _context(project_root)) == 0
    assert command(["workspace", "seal", "--force"], _context(project_root)) == 0
    capsys.readouterr()

    assert command(["job", "show", job_id], _context(project_root)) == 0
    show_out = capsys.readouterr().out
    assert "sealed: yes" in show_out and "identity" in show_out

    assert command(["job", "show", "--json", job_id], _context(project_root)) == 0
    report = json.loads(capsys.readouterr().out)[0]
    assert report["sealed"] is True and "seal_roles" in report

    assert command(["workspace", "status"], _context(project_root)) == 0
    assert "sealed: yes" in capsys.readouterr().out

    assert command(["workspace", "status", "--json"], _context(project_root)) == 0
    assert json.loads(capsys.readouterr().out)[0]["sealed"] is True


@_C5B2
def test_seal_verify_untrusted_signer_exits_three(tmp_path: Path, capsys) -> None:
    project_root, workspace, job_id = _setup(tmp_path)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    seed_file = tmp_path / "foreign.seed"
    seed_file.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")

    assert command(["job", "seal", "--keys", str(seed_file), job_id], _context(project_root)) == 0
    capsys.readouterr()

    payload = workspace.payload_path(marker.placement, marker.job_key)
    assert command(["seal", "verify", str(payload)], _context(project_root)) == 3
    out = capsys.readouterr().out
    assert out.strip().splitlines()[-1] == "UNTRUSTED"
    assert "valid_unknown_key" in out


@_C5B2
def test_seal_verify_unsealed_subject_renders_not_sealed(tmp_path: Path, capsys) -> None:
    project_root, _workspace, _job_id = _setup(tmp_path)

    assert command(["seal", "verify"], _context(project_root)) == 1
    out = capsys.readouterr().out
    assert "not sealed" in out
    assert out.strip().splitlines()[-1] == "FAILED"
