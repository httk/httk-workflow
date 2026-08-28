"""Real ``ssh`` adapter behaviour and launcher-profile forwarding.

No test here needs a network or a batch system: the stand-in cluster of
``conftest`` provides an ``ssh`` that runs the remote command locally and an
``sbatch`` that spools the script it was handed, so genuine ``rsync``
transfers, genuine quoting and genuine exit statuses are exercised.
"""

import json
import os
import stat
import sys
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import FAKE_HOST, Remote, fake_remote, register_ws
from httk.workflow import Workspace, adapter_runtime, launchers
from httk.workflow.adapters import RemoteTarget, add_remote, probe_remote_workspace, run_adapter
from httk.workflow.launchers import add_launcher
from httk.workflow.manager import TaskManager
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _manager, command

_FAKE_PYTHON = '''#!{python}
"""Stand-in for python3 that records ``-m pip`` instead of running it."""

import json
import os
import subprocess
import sys

argv = sys.argv[1:]
if argv[:2] == ["-m", "pip"]:
    with open(os.environ["HTTK_FAKE_PIP_LOG"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps(argv) + "\\n")
    sys.exit(int(os.environ.get("HTTK_FAKE_PIP_STATUS", "1")))
sys.exit(subprocess.run([{python!r}, *argv]).returncode)
'''

_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


def _tree(root: Path) -> Path:
    (root / "files").mkdir(parents=True)
    (root / "files" / "runner").write_text(_RUNNER, encoding="utf-8")
    (root / "files" / "runner").chmod(0o750)
    (root / "files" / "data with spaces.txt").write_text("payload\n", encoding="utf-8")
    (root / "link").symlink_to("files/runner")
    return root


def _payload(root: Path) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / "payload"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text(_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": "test",
                "name": "test",
                "workflow": "tests",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "start",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def test_ssh_push_and_pull_round_trip_a_real_tree(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="push-pull")
    bundle = fake_remote(project)
    source = _tree(tmp_path / "source")
    destination = remote.root / "runs" / "incoming"

    pushed = run_adapter(
        bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)}
    )

    assert pushed["path"] == str(destination)
    assert (destination / "files" / "data with spaces.txt").read_text(encoding="utf-8") == "payload\n"
    assert stat.S_IMODE((destination / "files" / "runner").stat().st_mode) == 0o750
    assert (destination / "link").is_symlink()
    assert os.readlink(destination / "link") == "files/runner"

    back = tmp_path / "back"
    pulled = run_adapter(bundle, "pull", {"remote_settings": {}, "source": str(destination), "destination": str(back)})

    assert pulled["path"] == str(back)
    assert (back / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert stat.S_IMODE((back / "files" / "runner").stat().st_mode) == 0o750
    assert os.readlink(back / "link") == "files/runner"
    # Repeating the push is the resume path the detached transfer relies on.
    assert run_adapter(bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)})


def test_ssh_push_transfers_only_the_requested_files(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="batched")
    bundle = fake_remote(project)
    source = _tree(tmp_path / "source")
    destination = remote.root / "runs" / "batched"

    run_adapter(
        bundle,
        "push",
        {
            "remote_settings": {},
            "source": str(source),
            "destination": str(destination),
            "files": ["files/data with spaces.txt"],
        },
    )

    assert (destination / "files" / "data with spaces.txt").read_text(encoding="utf-8") == "payload\n"
    assert not (destination / "files" / "runner").exists()


def test_ssh_push_refuses_a_path_that_escapes_the_transfer(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="escape")
    bundle = fake_remote(project)
    source = _tree(tmp_path / "source")
    with pytest.raises(RuntimeError, match="not workspace relative"):
        run_adapter(
            bundle,
            "push",
            {
                "remote_settings": {},
                "source": str(source),
                "destination": str(remote.root / "runs" / "escape"),
                "files": ["../secret"],
            },
        )


def test_ssh_push_creates_the_destination_without_rsync_mkpath(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Older rsync releases lack --mkpath, so the parent is made over the
    # transport first; the fallback is exercised in this process.
    monkeypatch.setattr(adapter_runtime, "_rsync_help", lambda: "")
    source = _tree(tmp_path / "source")
    destination = remote.root / "deep" / "nested" / "target"

    adapter_runtime._rsync(
        {"host": FAKE_HOST, "username": "someone"},
        source=str(source),
        destination=str(destination),
        directory=True,
        files=None,
        push=True,
    )

    assert (destination / "files" / "runner").read_text(encoding="utf-8") == _RUNNER


def test_remote_invoke_keeps_quoting_hostile_arguments_intact(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="quoting")
    bundle = fake_remote(project)
    hostile = ["a b", "$HOME", "`id`", "'single'", '"double"', "semi;colon", "star*", "new\nline", "back\\slash"]
    sentinel = remote.root / "must-not-exist"

    invoked = run_adapter(
        bundle,
        "invoke",
        {
            "remote_settings": {},
            "argv": [
                "python3",
                "-c",
                "import json, sys; print(json.dumps(sys.argv[1:]))",
                *hostile,
                f"value; touch {sentinel}",
            ],
        },
    )

    assert invoked["returncode"] == 0
    assert json.loads(str(invoked["stdout"])) == [*hostile, f"value; touch {sentinel}"]
    assert not sentinel.exists()


def test_remote_invoke_honours_the_requested_directory(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="cwd")
    bundle = fake_remote(project)
    directory = remote.root / "a directory"
    directory.mkdir()

    invoked = run_adapter(
        bundle,
        "invoke",
        {"remote_settings": {}, "argv": ["pwd"], "cwd": str(directory)},
    )

    assert invoked["returncode"] == 0
    assert str(invoked["stdout"]).strip() == str(directory)


def test_remote_invoke_reports_a_failing_remote_command(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="failing")
    bundle = fake_remote(project)

    invoked = run_adapter(bundle, "invoke", {"remote_settings": {}, "argv": ["false"]})

    assert invoked["returncode"] == 1


def test_remote_status_returns_the_remote_workspace_json(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="status")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    bundle = fake_remote(project, workspace=str(workspace.root))

    result = run_adapter(
        bundle,
        "status",
        {
            "remote_settings": {},
            "argv": ["httk", "workspace", "status", "--by-path", "--json", str(workspace.root)],
        },
    )

    assert result["returncode"] == 0
    reported = json.loads(str(result["stdout"]))
    assert reported[0]["format"] == "httk-workflow-status"
    assert reported[0]["workspace_id"] == workspace.workspace_id


def test_workspace_launcher_profile_dispatches_run_to_slurm(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="workspace-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    for key, value in {
        "manager.launch": "cluster",
        "manager.count": "2",
        "manager.workers": "4",
        "slurm.account": "p2026-1",
        "slurm.partition": "main",
        "slurm.time_limit": "04:00:00",
        "slurm.nodes": "2",
        "slurm.cpus_per_task": "16",
    }.items():
        workspace.set_setting(key, value)
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    assert command(["run", "--workspace", "station"], context) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["job_ids"] == ["4201", "4202"]
    script = next(workspace.root.glob(".httk-workspace/batch/*.sbatch")).read_text(encoding="utf-8")
    for directive in (
        "--account=p2026-1",
        "--partition=main",
        "--time=04:00:00",
        "--nodes=2",
        "--cpus-per-task=16",
    ):
        assert f"#SBATCH {directive}" in script
    assert f"#SBATCH --chdir={workspace.root}" in script
    assert (
        f"exec {sys.executable} -m httk.core.cli workflow manager run --by-path --workspace {workspace.root} --workers 4"
        in script
    )


def test_unresolved_workspace_launcher_names_workspace_and_setting(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="unknown-workspace-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.launch", "missing-cluster")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    assert command(["run", "--workspace", "station"], context) == 1
    captured = capsys.readouterr()
    assert str(workspace.root) in captured.err
    assert "manager.launch='missing-cluster'" in captured.err
    assert "Traceback" not in captured.err


def test_inline_overrides_workspace_launcher(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="inline-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    workspace.set_setting("manager.launch", "cluster")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")
    assert command(["run", "--workspace", "station", "--inline", "--detach"], context) == 2
    assert "not allowed with argument" in capsys.readouterr().err
    assert command(["run", "--workspace", "station", "--inline", "--count", "2"], context) == 2
    assert "--inline can only be combined" in capsys.readouterr().err
    workspace.set_setting("manager.count", "4")
    called: list[Path] = []

    def fake_in_process(_arguments, root, _context, _settings):
        called.append(root)
        return 0

    monkeypatch.setattr(_manager, "_run_in_process_manager", fake_in_process)

    assert command(["run", "--workspace", "station", "--inline"], context) == 0
    assert called == [workspace.root]
    assert not list(remote.spool.glob("*.json"))


def test_missing_sbatch_refusal_is_returned_by_workspace_launcher(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="missing-sbatch")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    workspace.set_setting("manager.launch", "cluster")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")
    no_sbatch_path = tmp_path / "no-sbatch-bin"
    no_sbatch_path.mkdir()
    (no_sbatch_path / "python3").symlink_to(sys.executable)
    monkeypatch.setenv("PATH", str(no_sbatch_path))
    monkeypatch.setattr(launchers.shutil, "which", lambda name: "/fake/sbatch" if name == "sbatch" else None)

    assert command(["run", "--workspace", "station"], context) == 1
    captured = capsys.readouterr()
    assert "manager.launch='cluster'" in captured.err
    assert "sbatch" in captured.err


def test_remote_manager_from_the_command_line_invokes_once(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="cli-manager")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    fake_remote(project, workspace=str(workspace.root))
    workspace.set_setting("manager.workers", "4")
    context = CLIContext("httk", project)
    # `manager run` on a remote-bound workspace submits through the remote's
    # The remote command is invoked directly on the owning machine.
    register_ws(context, workspace.root, "station", remote="cluster")

    assert command(["manager", "run", "--workspace", "cluster:station", "--count", "2"], context) == 0
    capsys.readouterr()
    manager_commands = [item for item in remote.commands() if "workflow manager run" in item]
    assert len(manager_commands) == 1
    assert "httk workflow manager run --workspace station --detach --count 2" in manager_commands[0]


def test_remote_manager_forwards_execution_options(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-manager-options")
    workspace = Workspace.initialize(remote.root / "runs" / "options")
    fake_remote(project, workspace=str(workspace.root))
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "options", remote="cluster")

    assert (
        command(
            [
                "manager",
                "run",
                "--workspace",
                "cluster:options",
                "--pool",
                "gpu",
                "--pool",
                "cpu",
                "--capability",
                "cuda",
                "--placement-prefix",
                "volume/a",
                "--lease-seconds",
                "12",
                "--heartbeat-interval",
                "3",
                "--poll-interval",
                "4",
                "--idle-timeout",
                "9",
                "--workers",
                "2",
                "--no-durable",
                "--log-level",
                "debug",
                "--json-logs",
            ],
            context,
        )
        == 0
    )
    capsys.readouterr()
    command_line = remote.commands()[-1]
    for option in (
        "--pool gpu",
        "--pool cpu",
        "--capability cuda",
        "--placement-prefix volume/a",
        "--lease-seconds 12.0",
        "--heartbeat-interval 3.0",
        "--poll-interval 4.0",
        "--idle-timeout 9.0",
        "--workers 2",
        "--no-durable",
        "--log-level debug",
        "--json-logs",
    ):
        assert option in command_line

    assert (
        command(["manager", "run", "--workspace", "cluster:options", "--runner-search-path", "/tmp/runners"], context)
        == 0
    )
    assert "--runner-search-path /tmp/runners" in remote.commands()[-1]


def test_local_transfers_stay_in_this_filesystem(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="local-copy")
    bundle = fake_remote(project, template="local", name="batch")
    source = _tree(tmp_path / "source")

    pushed = run_adapter(
        bundle,
        "push",
        {"remote_settings": {}, "source": str(source), "destination": str(tmp_path / "copy")},
    )

    assert (tmp_path / "copy" / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert pushed["path"] == str(tmp_path / "copy")
    assert not remote.log.exists()


def test_check_reports_a_missing_remote_httk_with_the_remedy(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="check-missing")
    bundle = fake_remote(project, httk_command=str(tmp_path / "nowhere" / "httk"))

    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(bundle, "install", {"remote_settings": {}})


def test_check_finds_the_remote_httk_without_creating_a_workspace(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="check-found")
    workspace = remote.root / "runs" / "fresh"
    bundle = fake_remote(project, workspace=str(workspace))

    result = run_adapter(bundle, "install", {"remote_settings": {}})

    assert result["installed"] is True
    assert result["httk_command"] == ["httk"]
    assert str(result["httk_version"]).strip()
    assert "workspace_created" not in result
    assert not workspace.exists()


def test_check_refuses_an_unreachable_host(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="check-unreachable")
    bundle = fake_remote(project)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")

    with pytest.raises(RuntimeError, match=f"cannot reach someone@{FAKE_HOST}"):
        run_adapter(bundle, "install", {"remote_settings": {}})


def test_check_never_installs_software_on_the_target(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retired bootstrap=pip setting is inert: no pip attempt, ever."""

    project = tmp_path / "project"
    initialize_project(project, name="bootstrap")
    remote.install("python3", _FAKE_PYTHON)
    log = tmp_path / "pip.log"
    monkeypatch.setenv("HTTK_FAKE_PIP_LOG", str(log))
    absent = str(tmp_path / "nowhere" / "httk")

    bundle = fake_remote(project, name="opted-in", httk_command=absent, bootstrap="pip")
    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(bundle, "install", {"remote_settings": {}})
    assert not log.exists()


def test_configure_verifies_connectivity_for_ssh_remotes(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="configure")
    bundle = fake_remote(project)

    assert run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})["connectivity"] == "ok"

    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")
    with pytest.raises(RuntimeError, match="cannot reach"):
        run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})
    skipped = run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {"check_connectivity": "no"}})
    assert skipped["connectivity"] == "skipped"


def test_configure_checks_the_host_the_command_line_is_about_to_store(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="pending")
    bundle = add_remote("cluster", template="ssh", project=project)

    assert run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})["connectivity"] == "skipped"
    pending = {"host": FAKE_HOST, "username": "someone"}
    assert run_adapter(bundle, "configure", {"remote_settings": {}, "settings": pending})["connectivity"] == "ok"


def test_an_unrecognized_adapter_kind_still_refuses(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="unknown")
    bundle = fake_remote(project)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["kind"] = "torque"
    (bundle / "remote.json").write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")

    with pytest.raises(RuntimeError, match="'torque' is not implemented"):
        run_adapter(bundle, "status", {"remote_settings": {}, "argv": ["true"]})


def test_a_job_reaches_a_remote_workspace_and_runs_there(tmp_path: Path, remote: Remote) -> None:
    source_root = tmp_path / "project"
    initialize_project(source_root, name="end-to-end")
    Workspace.initialize(source_root)
    destination = Workspace.initialize(remote.root / "runs" / "workspace")
    fake_remote(source_root, workspace=str(destination.root))
    payload, job_id = _payload(tmp_path / "incoming")
    Workspace(source_root).submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root, "home")
    register_ws(context, destination.root, "station", remote="cluster")

    assert command(["transfer", "--job", job_id, "home", "cluster:station"], context) == 0

    assert Workspace(source_root).find_marker_by_id(job_id) is None
    marker = destination.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "submitted"
    with TaskManager(destination, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    finished = destination.find_marker_by_id(job_id)
    assert finished is not None and finished.kind == "succeeded"
    # Every step of the flow really crossed the stand-in transport.
    commands = [json.loads(line)["command"] for line in remote.log.read_text(encoding="utf-8").splitlines()]
    assert any("workspace status --json station" in item for item in commands)
    assert any("transfer receive --workspace station --bundle" in item for item in commands)
    assert any(item.startswith("rsync ") or " rsync " in item for item in commands)


def test_probe_remote_workspace_reports_older_remote_returning_single_document(tmp_path: Path) -> None:
    target = RemoteTarget("far", tmp_path, False)

    def _old_release_adapter(bundle, verb, payload, *, timeout):
        # A remote on the previous release answers ``status --json`` with a
        # single status object rather than the current one-element list.
        return {
            "returncode": 0,
            "stdout": json.dumps(
                {
                    "format": "httk-workflow-status",
                    "format_version": 2,
                    "workspace_id": str(uuid.uuid4()),
                    "root": "/data/ws",
                }
            ),
        }

    with pytest.raises(ValueError, match="older than this client"):
        probe_remote_workspace(target, "ws", timeout=None, adapter=_old_release_adapter)
