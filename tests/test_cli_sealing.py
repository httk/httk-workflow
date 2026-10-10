"""The seal/unseal CLI surface: writing, removing, and verifying seals."""

import base64
import json
import sys
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.crypto import ed25519_generate_seed
from httk.core.project.cli import command as project_command

import v3_helpers as v3
from conftest import configure_identity
from httk.workflow import Workspace, _kernel
from httk.workflow._state import Release, StateDoc
from httk.workflow.projects import initialize_project
from httk.workflow.seals import is_job_sealed, is_project_sealed, is_workspace_sealed, job_seal_path
from httk.workflow.workflow_cli import command

# The library side (seal and unseal requests, the workspace snapshot, verify) is covered by test_seals and
# test_sealing_runtime; these drive the CLI handlers.


def _init_workspace(project: Path) -> Workspace:
    """Create ``project/workspace``, register it as ``default``, and record it as the project default."""

    from httk.workflow.projects import write_project_section
    from httk.workflow.registry import create_workspace

    create_workspace("default", project / "workspace")
    write_project_section(project, "workspace", {"default": "default"})
    return Workspace(project / "workspace")


def _setup(tmp_path: Path) -> tuple[Path, Workspace, _kernel.JobRef]:
    """A project whose default workspace holds one succeeded, unsealed job with a ``files/runner`` member."""

    project_root = tmp_path / "project"
    project_root.mkdir()
    initialize_project(project_root, name="sealing")
    configure_identity()
    workspace = _init_workspace(project_root)
    ref = v3.submit(
        workspace,
        ("demo--0123456789abcdef", "demo"),
        {"start": "succeed"},
        placement="jobs/silicon",
        members={"files/runner": "#!/bin/sh\nexit 0\n"},
    )
    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        return (
            project_root,
            workspace,
            owned.release(StateDoc.empty(owned.job_id).next_activation("start", "initial"), Release("succeeded", 500)),
        )


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


def test_workspace_seal_lists_unsealed_jobs_and_refuses_a_second_seal(tmp_path: Path, capsys) -> None:
    # Unsealed jobs are recorded and listed for information; the removed --force sealed them first.
    project_root, workspace, ref = _setup(tmp_path)

    assert command(["workspace", "seal"], _context(project_root)) == 0
    captured = capsys.readouterr()
    assert f"unsealed job\t{ref.job_id}\tsucceeded" in captured.err
    assert f"{workspace.root}\tsealed\t" in captured.out
    assert is_workspace_sealed(workspace)
    assert command(["workspace", "seal"], _context(project_root)) == 1
    assert "already sealed" in capsys.readouterr().err
    assert command(["workspace", "seal", "--force"], _context(project_root)) == 2
    capsys.readouterr()

    # The workspace seal blocks modifying CLI commands until it is removed.
    settings = ["workspace", "settings", "set", "--key", "answer", "--value", "1"]
    assert command(settings, _context(project_root)) == 1
    assert "sealed" in capsys.readouterr().err
    assert command(["workspace", "unseal", "--force"], _context(project_root)) == 0
    assert command(settings, _context(project_root)) == 0
    capsys.readouterr()


def test_workspace_verify_reports_drift_and_exits_one(tmp_path: Path, capsys) -> None:
    project_root, workspace, ref = _setup(tmp_path)
    assert command(["job", "seal", ref.job_id], _context(project_root)) == 0
    assert command(["workspace", "seal"], _context(project_root)) == 0
    capsys.readouterr()
    assert command(["workspace", "verify"], _context(project_root)) == 0
    assert f"{workspace.root}\tvalid_trusted\t" in capsys.readouterr().out

    added = v3.submit(workspace, ("demo--0123456789abcdef", "demo"), {"start": "succeed"}, placement="jobs/added")
    assert command(["workspace", "verify"], _context(project_root)) == 1
    assert f"unsealed\t{added.job_key}" in capsys.readouterr().out
    assert command(["workspace", "verify", "--json"], _context(project_root)) == 1
    (entry,) = json.loads(capsys.readouterr().out)
    assert entry["level"] == "workspace" and entry["valid"] is False
    assert {"path": added.job_key, "kind": "unsealed"} in entry["discrepancies"]


def test_project_seal_then_verify_ok_and_tamper_fails(tmp_path: Path, capsys) -> None:
    project_root, workspace, ref = _setup(tmp_path)

    assert command(["job", "seal", ref.job_id], _context(project_root)) == 0
    assert command(["workspace", "seal"], _context(project_root)) == 0
    capsys.readouterr()
    assert project_command(["seal"], _context(project_root)) == 0
    assert "\tsealed\t" in capsys.readouterr().out
    assert is_project_sealed(project_root)

    assert command(["seal", "verify"], _context(project_root)) == 0
    verified = capsys.readouterr().out
    assert verified.strip().splitlines()[-1] == "ok"
    assert "valid_trusted" in verified

    (v3.find(workspace, ref.job_id).path / "files" / "runner").write_text(
        "#!/bin/sh\necho tampered\n", encoding="utf-8"
    )

    assert command(["seal", "verify"], _context(project_root)) == 1
    tampered = capsys.readouterr().out
    assert tampered.strip().splitlines()[-1] == "FAILED"
    assert "  mismatch\tfiles/runner" in tampered


def test_seal_verify_json_shape(tmp_path: Path, capsys) -> None:
    project_root, _workspace, _ref = _setup(tmp_path)
    assert command(["workspace", "seal"], _context(project_root)) == 0
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


def test_workspace_status_reports_the_seal(tmp_path: Path, capsys) -> None:
    # The job-show half moved with job show (C5b-1): it reports the state's seal digest, not "sealed: yes".
    project_root, _workspace, ref = _setup(tmp_path)

    assert command(["workspace", "status"], _context(project_root)) == 0
    assert "sealed: no" in capsys.readouterr().out

    assert command(["job", "seal", ref.job_id], _context(project_root)) == 0
    assert command(["workspace", "seal"], _context(project_root)) == 0
    capsys.readouterr()

    assert command(["workspace", "status"], _context(project_root)) == 0
    assert "sealed: yes" in capsys.readouterr().out
    assert command(["workspace", "status", "--json"], _context(project_root)) == 0
    assert json.loads(capsys.readouterr().out)[0]["sealed"] is True


def test_seal_verify_untrusted_signer_exits_three(tmp_path: Path, capsys) -> None:
    # job seal --keys is gone: a job seal is signed with the workspace seal.keys setting.
    project_root, workspace, ref = _setup(tmp_path)
    seed_file = tmp_path / "foreign.seed"
    seed_file.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")
    workspace.set_setting("seal.keys", str(seed_file))

    assert command(["job", "seal", ref.job_id], _context(project_root)) == 0
    capsys.readouterr()

    assert command(["seal", "verify", str(v3.find(workspace, ref.job_id).path)], _context(project_root)) == 3
    out = capsys.readouterr().out
    assert out.strip().splitlines()[-1] == "UNTRUSTED"
    assert "valid_unknown_key" in out


def test_seal_verify_unsealed_subject_renders_not_sealed(tmp_path: Path, capsys) -> None:
    project_root, _workspace, _ref = _setup(tmp_path)

    assert command(["seal", "verify"], _context(project_root)) == 1
    out = capsys.readouterr().out
    assert "not sealed" in out
    assert out.strip().splitlines()[-1] == "FAILED"
