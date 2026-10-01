"""In-sandbox identity, cwd, environment and exec tests for MPI ranks."""

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import httk.workflow._daemon_mpi_rank as rank_module
from httk.workflow._daemon_mpi_protocol import Manifest, encode_manifest

REQUEST_ID = "1" * 32
HANDLE = "2" * 32
WORKSPACE_ID = "12345678-1234-4234-8234-123456789abc"


def _policy(*, workspace_id: str = WORKSPACE_ID, mpi_profile: bool = True, mpi_settings: bool = True):
    profile = SimpleNamespace(name="parallel", mpi=SimpleNamespace(nodes=2, ranks=4) if mpi_profile else None)
    return SimpleNamespace(
        workspace_id=workspace_id,
        mpi=SimpleNamespace(environment=(("OMPI_MCA_btl", "self,vader,tcp"),)) if mpi_settings else None,
        profile=lambda name: profile if name == "parallel" else (_ for _ in ()).throw(ValueError("unknown profile")),
    )


def _manifest(**changes: object) -> Manifest:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "manager_handle": HANDLE,
        "workspace_id": WORKSPACE_ID,
        "argv": ("solver", "--flag"),
        "cwd": "jobs/example/run",
        "environment": (("PATH", "/application/bin"), ("USER_VALUE", "yes")),
    }
    values.update(changes)
    return Manifest(**values)  # type: ignore[arg-type]


def _publish(workspace: Path, manifest: Manifest) -> None:
    directory = workspace / ".httk-workspace" / "mpi" / HANDLE
    directory.mkdir(parents=True)
    (directory / f"{REQUEST_ID}.json").write_bytes(encode_manifest(manifest))


def test_rank_reads_manifest_after_entry_and_executes_with_merged_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "workspace"
    workdir = workspace / "jobs" / "example" / "run"
    workdir.mkdir(parents=True)
    _publish(workspace, _manifest())
    observed: dict[str, object] = {}
    monkeypatch.setattr(rank_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(rank_module, "_POLICY_PATH", tmp_path / "policy.json")
    monkeypatch.setattr(rank_module, "load_policy", lambda _path: _policy())
    monkeypatch.setattr(
        rank_module.os,
        "environ",
        {"PMIX_RANK": "0", "SLURM_PROCID": "0", "PATH": "/sandbox/bin", "HOME": "/tmp/home"},
    )
    monkeypatch.setattr(rank_module.os, "chdir", lambda path: observed.update(cwd=path))

    def execvpe(executable, argv, environment) -> None:
        observed.update(executable=executable, argv=argv, environment=environment)

    monkeypatch.setattr(rank_module.os, "execvpe", execvpe)

    assert rank_module.main(["--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID]) == 0
    assert observed["cwd"] == workdir
    assert observed["executable"] == "solver"
    assert observed["argv"] == ["solver", "--flag"]
    environment = cast(dict[str, str], observed["environment"])
    assert environment["PMIX_RANK"] == "0"
    assert environment["SLURM_PROCID"] == "0"
    assert environment["OMPI_MCA_btl"] == "self,vader,tcp"
    assert environment["PATH"] == "/application/bin"
    assert environment["USER_VALUE"] == "yes"


@pytest.mark.parametrize(
    ("policy", "manifest", "message"),
    [
        (_policy(workspace_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"), _manifest(), "workspace identity"),
        (_policy(mpi_profile=False), _manifest(), "not configured for MPI"),
        (_policy(mpi_settings=False), _manifest(), "not configured for MPI"),
    ],
)
def test_rank_refuses_policy_and_manifest_identity_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy, manifest: Manifest, message: str, capsys
) -> None:
    workspace = tmp_path / "workspace"
    (workspace / "jobs" / "example" / "run").mkdir(parents=True)
    _publish(workspace, manifest)
    monkeypatch.setattr(rank_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(rank_module, "_POLICY_PATH", tmp_path / "policy.json")
    monkeypatch.setattr(rank_module, "load_policy", lambda _path: policy)
    monkeypatch.setattr(rank_module.os, "execvpe", lambda *_args: pytest.fail("application must not execute"))

    assert rank_module.main(["--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID]) == 2
    assert message in capsys.readouterr().err


def test_rank_refuses_manifest_path_identity_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    workspace = tmp_path / "workspace"
    (workspace / "jobs" / "example" / "run").mkdir(parents=True)
    _publish(workspace, _manifest(request_id="3" * 32))
    monkeypatch.setattr(rank_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(rank_module, "load_policy", lambda _path: _policy())
    monkeypatch.setattr(rank_module.os, "execvpe", lambda *_args: pytest.fail("application must not execute"))

    assert rank_module.main(["--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID]) == 2
    assert "identity" in capsys.readouterr().err


def test_rank_refuses_cwd_symlink_escape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (workspace / "escape").symlink_to(outside, target_is_directory=True)
    _publish(workspace, _manifest(cwd="escape"))
    monkeypatch.setattr(rank_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(rank_module, "load_policy", lambda _path: _policy())
    monkeypatch.setattr(rank_module.os, "execvpe", lambda *_args: pytest.fail("application must not execute"))

    assert rank_module.main(["--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID]) == 2
    assert "escapes" in capsys.readouterr().err


def test_rank_refuses_missing_or_non_directory_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _publish(workspace, _manifest(cwd="missing"))
    monkeypatch.setattr(rank_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(rank_module, "load_policy", lambda _path: _policy())

    assert rank_module.main(["--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID]) == 2
    assert "No such file" in capsys.readouterr().err


def test_rank_exec_failure_returns_127_inside_sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    workspace = tmp_path / "workspace"
    (workspace / "jobs" / "example" / "run").mkdir(parents=True)
    _publish(workspace, _manifest())
    monkeypatch.setattr(rank_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(rank_module, "load_policy", lambda _path: _policy())
    monkeypatch.setattr(rank_module.os, "chdir", lambda _path: None)
    monkeypatch.setattr(
        rank_module.os,
        "execvpe",
        lambda *_args: (_ for _ in ()).throw(FileNotFoundError("solver unavailable")),
    )

    assert rank_module.main(["--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID]) == 127
    assert "exec failed" in capsys.readouterr().err


@pytest.mark.parametrize(("handle", "request_id"), [("bad", REQUEST_ID), (HANDLE, "bad")])
def test_rank_rejects_invalid_ids_before_application_exec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, handle: str, request_id: str
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(rank_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(rank_module, "load_policy", lambda _path: _policy())
    monkeypatch.setattr(rank_module.os, "execvpe", lambda *_args: pytest.fail("application must not execute"))

    assert rank_module.main(["--profile", "parallel", "--handle", handle, "--request-id", request_id]) == 2
    assert "invalid" in capsys.readouterr().err
