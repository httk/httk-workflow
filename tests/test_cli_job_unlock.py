"""The ``httk job unlock`` verb: the marker, the runner copy, and the marker-only unlock of every ancestor."""

import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from httk.core.cli import CLIContext

import v3_helpers as v3
from conftest import configure_identity, register_ws
from httk.workflow import Workspace, _kernel, _store, collect, removal
from httk.workflow.collecting import _UNLOCKED_REASON
from httk.workflow.registry import WorkspaceBinding
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command

COPY = _store.RUNNER_COPY


def _unlock(workspace: Workspace, *arguments: str) -> int:
    return command(["job", "unlock", *arguments], CLIContext("httk", workspace.root))


def _failed(tmp_path: Path) -> tuple[Workspace, _store.Installed, _kernel.JobRef]:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = v3.submit(workspace, installed, {"start": "fail"}, placement="project/failed")
    v3.run(workspace)
    return workspace, installed, ref


def test_unlock_copies_the_runner_and_refuses_a_second_unlock(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace, installed, ref = _failed(tmp_path)
    assert _unlock(workspace, "--reason", "debugging", ref.job_id) == 0
    assert capsys.readouterr().out == f"{ref.job_id}\tunlocked\n"
    job = v3.find(workspace, ref.job_id)
    unlocked = v3.state_of(job).unlocked
    assert unlocked is not None and unlocked["reason"] == "debugging" and unlocked["operator"] == "cli"
    assert (job.path / COPY / "run").read_bytes() == (installed.package / "run").read_bytes()

    assert _unlock(workspace, ref.job_id) == 1
    assert capsys.readouterr().out == f"{ref.job_id}\trefused\talready unlocked\n"


def test_unlock_without_a_copy(tmp_path: Path) -> None:
    workspace, _installed, ref = _failed(tmp_path)
    assert _unlock(workspace, "--no-copy", ref.job_id) == 0
    job = v3.find(workspace, ref.job_id)
    assert v3.state_of(job).unlocked is not None and not (job.path / COPY).exists()


def test_a_sealed_succeeded_job_is_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, placement="project/sealed")
    v3.run(workspace)
    assert _unlock(workspace, ref.job_id) == 1
    assert "run `job unseal` first" in capsys.readouterr().out
    assert v3.state_of(v3.find(workspace, ref.job_id)).unlocked is None


def test_unlock_reaches_every_ancestor_marker_only(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    # grandparent -> parent -> child; the child fails, so both ancestors fail at their joins (unsealed).
    child = {"label": "child", "script": {"start": "fail"}}
    parent = {
        "label": "parent",
        "script": {"start": "spawn", "gather": "succeed"},
        "parameters": {"spawn": {"children": [child]}},
    }
    top = v3.submit(
        workspace,
        installed,
        {"start": "spawn", "gather": "succeed"},
        placement="project/top",
        parameters={"spawn": {"children": [parent]}},
    )
    v3.run(workspace)
    (middle,) = v3.state_of(v3.find(workspace, top.job_id)).children
    (leaf,) = v3.state_of(v3.find(workspace, str(middle["job_id"]))).children
    chain = [str(leaf["job_id"]), str(middle["job_id"]), top.job_id]

    assert _unlock(workspace, chain[0]) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"{chain[0]}\tunlocked",
        f"{chain[1]}\tunlocked (ancestor)",
        f"{chain[2]}\tunlocked (ancestor)",
    ]
    jobs = [v3.find(workspace, job_id) for job_id in chain]
    assert all(v3.state_of(job).unlocked is not None for job in jobs)
    assert [(job.path / COPY).is_dir() for job in jobs] == [True, False, False]

    # Selecting the middle job now gives it its copy; its already unlocked ancestor is walked past.
    assert _unlock(workspace, chain[1]) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"{chain[1]}\tunlocked",
        f"{chain[2]}\talready unlocked (ancestor)",
    ]
    assert (v3.find(workspace, chain[1]).path / COPY).is_dir()


def test_job_request_unlock_is_the_same_action(tmp_path: Path) -> None:
    workspace, _installed, ref = _failed(tmp_path)
    configure_identity()
    arguments = ["job", "request", "unlock", "--reason", "debugging", ref.job_id]
    assert command(arguments, CLIContext("httk", workspace.root)) == 0
    job = v3.find(workspace, ref.job_id)
    assert v3.state_of(job).unlocked is not None and (job.path / COPY / "run").is_file()


def test_mount_daemon_remotes_refuse_unlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    bundle = tmp_path / "remote"
    bundle.mkdir()
    (bundle / "remote.json").write_text(json.dumps({"kind": "mount-daemon", "settings": {}}), encoding="utf-8")
    binding = WorkspaceBinding("cluster:runs", "cluster", None)
    monkeypatch.setattr(job_cli, "_resolve_binding", lambda *_args, **_kwargs: (binding, None))
    monkeypatch.setattr(job_cli, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle=bundle))
    context = CLIContext("httk", tmp_path)
    assert command(["job", "unlock", "--workspace", "cluster:runs", str(uuid.uuid4())], context) == 2
    assert "the workspace daemon does not carry unlock" in capsys.readouterr().err
    configure_identity()
    request = ["job", "request", "unlock", "--workspace", "cluster:runs", "--reason", "r", str(uuid.uuid4())]
    assert command(request, context) == 2
    assert "unlock is not carried by the workspace daemon protocol" in capsys.readouterr().err


def test_a_parent_not_in_the_workspace_is_reported_and_not_unlocked(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    mapping = v3.job_mapping(installed, {"start": "fail"}, placement="project/orphan")
    gone = str(uuid.uuid4())
    mapping["parent"] = {
        "workspace_id": str(uuid.uuid4()),
        "job_id": gone,
        "job_key": f"gone--{gone}",
        "placement": "project",
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }
    ref = v3.submit_mapping(workspace, mapping)
    assert _unlock(workspace, "--no-copy", ref.job_id) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"{ref.job_id}\tunlocked",
        f"{ref.job_id}\tparent gone--{gone} not found; not unlocked",
    ]


def test_job_request_unlock_unlocks_the_ancestors_too(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # A parent failed at its wait on a failed child; the child is debugged through `job request unlock`.
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    spawn = {"children": [{"label": "child", "script": {"start": "fail"}}]}
    parent = v3.submit(
        workspace,
        installed,
        {"start": "spawn", "gather": "succeed"},
        placement="project/p",
        parameters={"spawn": spawn},
    )
    v3.run(workspace)
    (child,) = v3.state_of(v3.find(workspace, parent.job_id)).children
    child_id = str(child["job_id"])
    configure_identity()
    request = ["job", "request", "unlock", "--reason", "debugging", child_id]
    assert command(request, CLIContext("httk", workspace.root)) == 0
    assert capsys.readouterr().out.splitlines()[-1] == f"{parent.job_id}\tunlocked (ancestor)"

    runner = v3.find(workspace, child_id).path / COPY / "run"
    runner.write_text(
        runner.read_text(encoding="utf-8").replace('behavior = parameters["script"][step]', 'behavior = "succeed"'),
        encoding="utf-8",
    )
    with v3.cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, v3.find(workspace, child_id), "restart", "fixed") is None
        assert removal.request_now(workspace, owner, v3.find(workspace, parent.job_id), "continue", "fixed") is None
    v3.run(workspace)
    done = v3.find(workspace, parent.job_id)
    assert done.state == "succeeded" and v3.state_of(done).unlocked is not None
    # Its result came from the debugged child: never collected unforced.
    assert {item.record.job_id: item.missing_collector for item in collect(workspace)} == {
        parent.job_id: _UNLOCKED_REASON,
        child_id: _UNLOCKED_REASON,
    }


def test_remote_envelopes_refuse_unlocking_a_spawned_job(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    spawn = {"children": [{"label": "child", "script": {"start": "fail"}}]}
    parent = v3.submit(
        workspace,
        installed,
        {"start": "spawn", "gather": "succeed"},
        placement="project/p",
        parameters={"spawn": spawn},
    )
    v3.run(workspace)
    (child,) = v3.state_of(v3.find(workspace, parent.job_id)).children
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "envelopes")
    argv = ["job", "request-envelopes", "unlock", "--workspace", name, "--operator=Test User <t@example.test>"]
    argv += ["--reason=debug", "--json"]
    assert command([*argv, str(child["job_id"])], context) == 2
    assert "unlock spawned jobs on the workspace host with `httk job unlock`" in capsys.readouterr().err
    assert command([*argv, parent.job_id], context) == 0


def test_set_workflow_of_a_job_running_its_copy_is_refused_before_posting(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace, _installed, ref = _failed(tmp_path)
    other = v3.install(workspace, tmp_path / "other", name="other")
    assert _unlock(workspace, ref.job_id) == 0
    configure_identity()
    request = ["job", "request", "set_workflow", "--workflow", other.id, "--reason", "r", ref.job_id]
    assert command(request, CLIContext("httk", workspace.root)) == 2
    assert f"the job runs its own workflow copy at {COPY}; edit or remove it" in capsys.readouterr().err
    assert not list(workspace.control.glob("requests/*.json"))
