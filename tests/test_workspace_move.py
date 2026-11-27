"""``httk workspace move``: the rename, its refusals, and every local record of the workspace's path."""

import base64
import errno
import json
import os
import threading
from contextlib import ExitStack
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import identity_public_key
from httk.core.project.members import project_members, register_project_member

import test_daemon_setup
from conftest import fake_remote
from httk.workflow import Workspace, _daemon_cli, _daemon_setup, _kernel
from httk.workflow._daemon_activation import read_active_snapshot
from httk.workflow._daemon_auth import sign_request
from httk.workflow._daemon_policy import load_policy
from httk.workflow._daemon_protocol import Request, decode_response, encode_request
from httk.workflow._daemon_slurm import SchedulerError
from httk.workflow._daemon_state import Ledger
from httk.workflow.adapters import add_remote, metadata_path
from httk.workflow.projects import initialize_project
from httk.workflow.registry import register_workspace, resolve_workspace
from httk.workflow.workflow_cli import _workspace as workspace_cli
from httk.workflow.workflow_cli import command
from test_daemon_setup import _broker, _host, _initialize, _layout  # noqa: F401  (_host: the autouse fixture)
from v3_helpers import submit, workspace

_WORKFLOW = ("demo--0123456789abcdef", "demo")


def _context(cwd: Path) -> CLIContext:
    return CLIContext("httk", cwd)


def _daemon(root: Path) -> Path:
    return root / ".httk-workspace" / "daemon"


def _enrolled(tmp_path: Path, *keys: str) -> tuple[Path, CLIContext]:
    """A registered workspace ``site`` with a configured, stopped daemon."""

    layout = _layout(tmp_path)
    _initialize(layout, "small", changes=[("add", f"authorized_keys={key}") for key in keys])
    register_workspace("site", layout.workspace.root)
    return layout.workspace.root, _context(tmp_path)


def _registered_path(name: str) -> Path:
    path = resolve_workspace(name).path
    assert path is not None
    return Path(path)


def _assert_relocated(root: Path) -> None:
    """The active enrollment, its snapshot and the published daemon document all name *root*."""

    snapshot, _digest = read_active_snapshot(_daemon(root))
    policy = load_policy(snapshot)
    assert snapshot.parent == _daemon(root) / "snapshots" and policy.workspace == root
    published = json.loads((root / "exchange" / "daemon.json").read_text(encoding="utf-8"))
    assert published["configurations"]["small"]["digest"] == policy.configuration_digest("small")
    assert _daemon_setup.describe(root)["workspace"] == str(root)


def test_move_updates_the_registry_and_the_project_members(tmp_path: Path, capsys) -> None:
    project, other = tmp_path / "project", tmp_path / "other"
    initialize_project(project, name="moves")
    initialize_project(other, name="elsewhere")
    context = _context(project)
    assert command(["workspace", "init", "--name", "runs", "runs"], context) == 0
    capsys.readouterr()

    assert command(["workspace", "move", "runs", "renamed"], context) == 0
    out = capsys.readouterr().out
    assert f"moved runs: {project / 'runs'} -> {project / 'renamed'}" in out
    assert "updated: registry entry runs" in out and f"updated: project members of {project}" in out
    assert "updated: daemon" not in out and "note: remotes reach this workspace by name" in out
    assert _registered_path("runs") == project / "renamed"
    assert [(m.path, m.name) for m in project_members(project) if m.kind == "workspace"] == [("renamed", "runs")]

    assert command(["workspace", "move", "runs", str(other / "runs")], context) == 0
    capsys.readouterr()
    assert not [m for m in project_members(project) if m.kind == "workspace"]
    assert [(m.path, m.name) for m in project_members(other) if m.kind == "workspace"] == [("runs", "runs")]
    assert command(["workspace", "status", "runs"], context) == 0


def test_move_relocates_a_stopped_daemon_and_it_serves_from_the_new_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    seed = tmp_path / "operator.seed"
    seed.write_bytes(base64.b64encode(bytes([7]) * 32) + b"\n")
    seed.chmod(0o600)
    operator_key = identity_public_key(seed)
    assert operator_key is not None
    old, context = _enrolled(tmp_path, operator_key)
    before = load_policy(read_active_snapshot(_daemon(old))[0])
    new = tmp_path / "moved"
    # A client remote on this machine that mounts the exchange by the old path is named with its fix.
    bundle = add_remote("client", template="mount-daemon", global_scope=True)
    metadata = json.loads(metadata_path(bundle).read_text(encoding="utf-8"))
    metadata["settings"]["exchange"] = str(old / "exchange")
    metadata_path(bundle).write_text(json.dumps(metadata), encoding="utf-8")

    assert command(["workspace", "move", "site", str(new)], context) == 0
    out = capsys.readouterr().out
    assert "updated: daemon enrollment (active snapshot" in out and "exchange/daemon.json" in out
    assert f"note: remote client mounts the old exchange: httk remote set client exchange={new / 'exchange'}" in out
    assert _registered_path("site") == new and not old.exists()
    _assert_relocated(new)
    after = load_policy(read_active_snapshot(_daemon(new))[0])
    assert (after.enrollment_id, after.workspace_id, after.launchers) == (
        before.enrollment_id,
        before.workspace_id,
        before.launchers,
    )

    # The daemon commands work from the new path: show, reload (nothing changed), check and run.
    assert _daemon_cli.command(["show", str(new)], program="httk") == 0
    assert f"workspace: {new}" in capsys.readouterr().out
    assert _daemon_cli.command(["reload", str(new)], program="httk") == 0
    assert "unchanged" in capsys.readouterr().out
    observed: list[list[str]] = []
    monkeypatch.setattr(_daemon_cli, "_close_inherited", lambda: None)
    monkeypatch.setattr(_daemon_cli.os, "chdir", lambda _path: None)
    monkeypatch.setattr(_daemon_cli.os, "dup2", lambda *_args, **_kwargs: None)

    def refuse_exec(_executable: str, argv: list[str], _environment: dict[str, str]) -> None:
        observed.append(argv)
        raise OSError("inspection stop")

    monkeypatch.setattr(_daemon_cli.os, "execve", refuse_exec)
    assert _daemon_cli.command(["run", str(new), "--once"], program="httk") == 2
    (argv,) = observed
    assert argv[argv.index("--workspace") + 1] == str(new)
    assert Path(argv[argv.index("--policy") + 1]).is_relative_to(_daemon(new))

    # A daemon instance on the relocated enrollment admits a start signed for the new configuration.
    request = Request(
        "1" * 32,
        after.workspace_id,
        "start_manager",
        profile="small",
        enrollment_id=after.enrollment_id,
        configuration_digest=after.configuration_digest("small"),
    )
    (after.requests / f"{request.request_id}.json").write_bytes(encode_request(sign_request(request, seed_path=seed)))
    with ExitStack() as stack:
        _broker(after, read_active_snapshot(_daemon(new))[0], stack).process_once(threading.Event())
    response = decode_response((after.responses / f"{request.request_id}.json").read_bytes())
    assert response.outcome == "submitted"


def test_move_refuses_while_the_daemon_runs(tmp_path: Path, capsys) -> None:
    old, context = _enrolled(tmp_path)
    with _kernel.register_owner(
        Workspace(old), kind="daemon", label="workspace daemon", allocation=None, advertised={}
    ):
        assert command(["workspace", "move", "site", str(tmp_path / "moved")], context) == 2
    error = capsys.readouterr().err
    assert "the workspace daemon is running" in error and "httk daemon run" in error
    assert old.is_dir() and not (tmp_path / "moved").exists() and _registered_path("site") == old


def test_move_refuses_while_a_job_is_held_by_a_live_owner(tmp_path: Path, capsys) -> None:
    ws = workspace(tmp_path / "ws")
    register_workspace("ws", ws.root)
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    with _kernel.register_owner(ws, kind="manager", label="test", allocation=None, advertised={}) as owner:
        claimed = _kernel.claim(ws, owner, ref)
        assert claimed is not None
        assert command(["workspace", "move", "ws", "moved"], _context(tmp_path)) == 2
        error = capsys.readouterr().err
        assert (
            f"owners not proven dead: {owner.owner_id} (manager)" in error and f"jobs they hold: {ref.job_key}" in error
        )
        claimed.give_back()
    assert ws.root.is_dir() and not (tmp_path / "moved").exists()


def test_move_refuses_while_a_daemon_manager_job_may_still_run(tmp_path: Path, capsys) -> None:
    seed = tmp_path / "operator.seed"
    seed.write_bytes(base64.b64encode(bytes([7]) * 32) + b"\n")
    seed.chmod(0o600)
    operator_key = identity_public_key(seed)
    assert operator_key is not None
    old, context = _enrolled(tmp_path, operator_key)
    snapshot = read_active_snapshot(_daemon(old))[0]
    policy = load_policy(snapshot)
    request = Request(
        "2" * 32,
        policy.workspace_id,
        "start_manager",
        profile="small",
        enrollment_id=policy.enrollment_id,
        configuration_digest=policy.configuration_digest("small"),
    )
    (policy.requests / f"{request.request_id}.json").write_bytes(encode_request(sign_request(request, seed_path=seed)))
    with ExitStack() as stack:
        _broker(policy, snapshot, stack).process_once(threading.Event())

    assert command(["workspace", "move", "site", str(tmp_path / "moved")], context) == 2
    error = capsys.readouterr().err
    assert "daemon manager jobs may still be queued or running" in error and "(Slurm job 42)" in error
    with Ledger(policy.broker, policy.workspace_id, policy.enrollment_id) as ledger:
        (row,) = ledger.managers()
        final = {
            "job_id": "42",
            "cluster": policy.cluster,
            "scheduler_state": "COMPLETED",
            "exit_code": "0:0",
            "started_at": None,
            "ended_at": None,
            "final": True,
            "log_published": True,
            "checked_at": 0,
            "unknown_since": None,
        }
        ledger.record_observation(str(row["handle"]), json.dumps(final).encode())
    assert command(["workspace", "move", "site", str(tmp_path / "moved")], context) == 0
    _assert_relocated(tmp_path / "moved")


def test_move_refuses_across_filesystems_and_record_only_completes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    old, context = _enrolled(tmp_path)
    new = tmp_path / "moved"
    real_rename = os.rename

    def cross_filesystem(source: str, destination: str, **kwargs: int) -> None:
        if Path(source).resolve() == old:
            raise OSError(errno.EXDEV, "cross-device link")
        real_rename(source, destination, **kwargs)

    monkeypatch.setattr(os, "rename", cross_filesystem)
    assert command(["workspace", "move", "site", str(new)], context) == 2
    error = capsys.readouterr().err
    assert "one filesystem" in error and f"httk workspace move --record-only site {new}" in error
    assert old.is_dir() and not new.exists()

    # --record-only refuses while the old path is still the workspace, and a destination that is none.
    assert command(["workspace", "move", "--record-only", "site", str(new)], context) == 2
    assert "is not an httk execution workspace" in capsys.readouterr().err
    real_rename(old, new)  # the operator moves the directory by other means
    (old / ".httk-workspace").mkdir(parents=True)
    (old / ".httk-workspace" / "format.json").write_text("{}", encoding="utf-8")
    assert command(["workspace", "move", "--record-only", "site", str(new)], context) == 2
    assert "is still the workspace" in capsys.readouterr().err
    (old / ".httk-workspace" / "format.json").unlink()

    assert command(["workspace", "move", "--record-only", "site", str(new)], context) == 0
    out = capsys.readouterr().out
    assert "moved site" not in out and "updated: registry entry site" in out
    assert _registered_path("site") == new
    _assert_relocated(new)


def test_an_interrupted_move_completes_with_record_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    old, context = _enrolled(tmp_path)
    new = tmp_path / "moved"
    real_document = _daemon_setup._write_daemon_document

    def interrupted(_policy: object) -> None:
        raise RuntimeError("interrupted")

    # Interrupted between the new snapshot's publication and the daemon document: the directory has moved.
    monkeypatch.setattr(_daemon_setup, "_write_daemon_document", interrupted)
    assert command(["workspace", "move", "site", str(new)], context) == 2
    capsys.readouterr()
    assert new.is_dir() and not old.exists() and _registered_path("site") == old
    monkeypatch.setattr(_daemon_setup, "_write_daemon_document", real_document)

    # Running the plain move again points at --record-only.
    assert command(["workspace", "move", "site", str(new)], context) == 2
    assert "workspace move --record-only" in capsys.readouterr().err

    # Interrupted again after the registry update: the registry already names the new path.
    def late(*_args: object) -> list[str]:
        raise RuntimeError("interrupted")

    real_references = workspace_cli._external_references
    monkeypatch.setattr(workspace_cli, "_external_references", late)
    assert command(["workspace", "move", "--record-only", "site", str(new)], context) == 2
    capsys.readouterr()
    assert _registered_path("site") == new
    monkeypatch.setattr(workspace_cli, "_external_references", real_references)

    assert command(["workspace", "move", "--record-only", "site", str(new)], context) == 0
    assert "updated: daemon enrollment" in capsys.readouterr().out
    _assert_relocated(new)


def _signed_start(tmp_path: Path, root: Path, request_id: str) -> None:
    """Have a daemon instance on *root*'s enrollment handle one signed manager start."""

    seed = tmp_path / "operator.seed"
    snapshot = read_active_snapshot(_daemon(root))[0]
    policy = load_policy(snapshot)
    request = Request(
        request_id,
        policy.workspace_id,
        "start_manager",
        profile="small",
        enrollment_id=policy.enrollment_id,
        configuration_digest=policy.configuration_digest("small"),
    )
    (policy.requests / f"{request_id}.json").write_bytes(encode_request(sign_request(request, seed_path=seed)))
    with ExitStack() as stack:
        _broker(policy, snapshot, stack).process_once(threading.Event())


def test_an_uncertain_daemon_start_warns_without_blocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    seed = tmp_path / "operator.seed"
    seed.write_bytes(base64.b64encode(bytes([7]) * 32) + b"\n")
    seed.chmod(0o600)
    operator_key = identity_public_key(seed)
    assert operator_key is not None
    old, context = _enrolled(tmp_path, operator_key)

    def unconfirmed(*_args: object, **_kwargs: object) -> None:
        raise SchedulerError("sbatch timed out")

    monkeypatch.setattr(test_daemon_setup._Gateway, "submit", unconfirmed)
    _signed_start(tmp_path, old, "3" * 32)
    assert command(["workspace", "move", "site", str(tmp_path / "moved")], context) == 0
    out = capsys.readouterr().out
    assert "warning: a daemon manager start " in out and "is uncertain" in out and "squeue --name=httk-" in out
    _assert_relocated(tmp_path / "moved")


def test_an_incomplete_daemon_directory_is_refused_before_the_rename(tmp_path: Path, capsys) -> None:
    old, context = _enrolled(tmp_path)
    (_daemon(old) / "active.json").unlink()
    assert command(["workspace", "move", "site", str(tmp_path / "moved")], context) == 2
    error = capsys.readouterr().err
    assert "has no active.json" in error and "httk daemon init" in error
    assert old.is_dir() and not (tmp_path / "moved").exists()


def test_a_destination_registered_under_another_name_is_refused(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    for name in ("a", "b"):
        assert command(["workspace", "init", name], context) == 0
    capsys.readouterr()
    for argv in (["a", "b"], ["--record-only", "a", "b"]):
        assert command(["workspace", "move", *argv], context) == 2
        assert "is registered as 'b'" in capsys.readouterr().err
    assert _registered_path("a") == tmp_path / "a"


def test_a_destination_project_that_cannot_record_the_member_is_reported(tmp_path: Path, capsys) -> None:
    other = tmp_path / "other"
    initialize_project(other, name="elsewhere")
    register_project_member(other, other / "taken", "workspace", name="runs")
    context = _context(tmp_path)
    assert command(["workspace", "init", "runs"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "move", "runs", str(other / "runs")], context) == 0
    out = capsys.readouterr().out
    assert f"warning: not recorded in {other / 'httk_project' / 'members.json'}" in out and "'runs'" in out
    assert f"updated: project members of {other}" not in out
    assert _registered_path("runs") == other / "runs"


def test_a_remote_move_forwards_record_only_and_no_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-move")
    fake_remote(project)
    captured: list[list[str]] = []

    def adapter(
        _bundle: object, _operation: str, request: dict[str, list[str]], **_kwargs: object
    ) -> dict[str, object]:
        captured.append(request["argv"])
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workspace_cli, "run_adapter", adapter)
    argv = ["workspace", "move", "--record-only", "--no-durable", "cluster:runs", "/far/new"]
    assert command(argv, _context(project)) == 0
    assert captured == [["httk", "workspace", "move", "--record-only", "--no-durable", "runs", "/far/new"]]
