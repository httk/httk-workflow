"""Real ``ssh-slurm`` and ``local-slurm`` adapter behaviour.

No test here needs a network or a batch system: the stand-in cluster of
``conftest`` provides an ``ssh`` that runs the remote command locally and an
``sbatch`` that spools the script it was handed, so genuine ``rsync``
transfers, genuine quoting and genuine exit statuses are exercised.
"""

import json
import os
import stat
import uuid
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]
from conftest import FAKE_HOST, Remote, fake_remote
from httk.core import CLIContext

from httk.workflow import Workspace, adapter_runtime
from httk.workflow.adapters import add_remote, run_adapter
from httk.workflow.manager import TaskManager
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command

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

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 1,
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
                "format_version": 1,
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

    pushed = run_adapter(bundle, "push", {"queue": "default", "source": str(source), "destination": str(destination)})

    assert pushed["path"] == str(destination)
    assert (destination / "files" / "data with spaces.txt").read_text(encoding="utf-8") == "payload\n"
    assert stat.S_IMODE((destination / "files" / "runner").stat().st_mode) == 0o750
    assert (destination / "link").is_symlink()
    assert os.readlink(destination / "link") == "files/runner"

    back = tmp_path / "back"
    pulled = run_adapter(bundle, "pull", {"queue": "default", "source": str(destination), "destination": str(back)})

    assert pulled["path"] == str(back)
    assert (back / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert stat.S_IMODE((back / "files" / "runner").stat().st_mode) == 0o750
    assert os.readlink(back / "link") == "files/runner"
    # Repeating the push is the resume path the detached transfer relies on.
    assert run_adapter(bundle, "push", {"queue": "default", "source": str(source), "destination": str(destination)})


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
            "queue": "default",
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
                "queue": "default",
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
            "queue": "default",
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
        {"queue": "default", "argv": ["pwd"], "cwd": str(directory)},
    )

    assert invoked["returncode"] == 0
    assert str(invoked["stdout"]).strip() == str(directory)


def test_remote_invoke_reports_a_failing_remote_command(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="failing")
    bundle = fake_remote(project)

    invoked = run_adapter(bundle, "invoke", {"queue": "default", "argv": ["false"]})

    assert invoked["returncode"] == 1


def test_remote_status_returns_the_remote_workspace_json(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="status")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    bundle = fake_remote(project, workspace=str(workspace.root))

    result = run_adapter(
        bundle,
        "status",
        {"queue": "default", "argv": ["httk", "workflow", "workspace", "status", str(workspace.root), "--json"]},
    )

    assert result["returncode"] == 0
    reported = json.loads(str(result["stdout"]))
    assert reported["format"] == "httk-workflow-status"
    assert reported["workspace_id"] == workspace.workspace_id


def test_ssh_start_manager_generates_and_submits_the_batch_script(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="submit")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    bundle = fake_remote(
        project,
        workspace=str(workspace.root),
        account="p2026-1",
        partition="main",
        time_limit="04:00:00",
        nodes="2",
        cpus_per_task="16",
        workers="4",
    )

    result = run_adapter(
        bundle,
        "start-manager",
        {
            "queue": "default",
            "argv": ["httk", "workflow", "manager", "run", str(workspace.root)],
            "count": 3,
        },
    )

    assert result["count"] == 3
    assert result["job_ids"] == ["4201", "4202", "4203"]
    script_path = Path(str(result["script"]))
    assert script_path.parent == workspace.root / ".httk-workflow" / "batch"
    script = script_path.read_text(encoding="utf-8")
    assert script.startswith("#!/bin/bash\n")
    assert "#SBATCH --account=p2026-1" in script
    assert "#SBATCH --partition=main" in script
    assert "#SBATCH --time=04:00:00" in script
    assert "#SBATCH --nodes=2" in script
    assert "#SBATCH --cpus-per-task=16" in script
    assert f"#SBATCH --chdir={workspace.root}" in script
    assert f"exec httk workflow manager run {workspace.root} --workers 4" in script
    submissions = sorted(remote.spool.glob("*.json"))
    assert len(submissions) == 3
    for submission in submissions:
        recorded = json.loads(submission.read_text(encoding="utf-8"))
        assert recorded["argv"] == [str(script_path)]
        assert recorded["cwd"] == str(workspace.root)
        assert (submission.parent / f"{submission.stem}.sbatch").read_text(encoding="utf-8") == script


def test_start_manager_keeps_an_explicit_worker_count(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="explicit-workers")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    bundle = fake_remote(project, workspace=str(workspace.root), workers="4")

    result = run_adapter(
        bundle,
        "start-manager",
        {
            "queue": "default",
            "argv": ["httk", "workflow", "manager", "run", str(workspace.root), "--workers", "1"],
        },
    )

    script = Path(str(result["script"])).read_text(encoding="utf-8")
    assert "--workers 1" in script and "--workers 4" not in script


def test_start_manager_prefers_the_stated_workspace_over_the_argv_heuristic(
    tmp_path: Path,
    remote: Remote,
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="stated-workspace")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    bundle = fake_remote(project, workspace=str(remote.root / "runs" / "configured"))

    result = run_adapter(
        bundle,
        "start-manager",
        {
            "queue": "default",
            "workspace": str(workspace.root),
            # Reading the workspace back out of this vector is only the
            # documented fallback, so the stated field has to win over it.
            "argv": ["httk", "workflow", "manager", "run", str(remote.root / "runs" / "argv")],
        },
    )

    script_path = Path(str(result["script"]))
    assert script_path.parent == workspace.root / ".httk-workflow" / "batch"
    assert f"#SBATCH --chdir={workspace.root}" in script_path.read_text(encoding="utf-8")


def test_start_manager_from_the_command_line_counts_managers_and_defers_workers(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="cli-start-manager")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    fake_remote(project, workspace=str(workspace.root), workers="4")
    context = CLIContext("httk", project)

    assert command(["transfer", "start-manager", "cluster", "--count", "2"], context) == 0
    submitted = json.loads(capsys.readouterr().out)
    assert submitted["count"] == 2 and len(submitted["job_ids"]) == 2
    # No --workers on the command line, so the queue's setting is what runs.
    script = Path(str(submitted["script"])).read_text(encoding="utf-8")
    assert f"exec httk workflow manager run {workspace.root} --workers 4" in script

    assert command(["transfer", "start-manager", "cluster", "--workers", "1"], context) == 0
    explicit = json.loads(capsys.readouterr().out)
    assert explicit["count"] == 1 and len(explicit["job_ids"]) == 1
    later = Path(str(explicit["script"])).read_text(encoding="utf-8")
    assert "--workers 1" in later and "--workers 4" not in later
    assert len(list(remote.spool.glob("*.json"))) == 3


def test_local_slurm_start_manager_submits_with_the_local_sbatch(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="local-batch")
    workspace = Workspace.initialize(tmp_path / "workspace")
    bundle = fake_remote(
        project,
        template="local-slurm",
        name="batch",
        workspace=str(workspace.root),
        account="p2026-1",
        partition="devel",
    )

    result = run_adapter(
        bundle,
        "start-manager",
        {"queue": "default", "argv": ["httk", "workflow", "manager", "run", str(workspace.root)], "count": 2},
    )

    script_path = Path(str(result["script"]))
    assert script_path.parent == workspace.root / ".httk-workflow" / "batch"
    assert "#SBATCH --partition=devel" in script_path.read_text(encoding="utf-8")
    assert result["job_ids"] == ["4201", "4202"]
    assert len(list(remote.spool.glob("*.json"))) == 2
    # Nothing went over the transport for a remote that is not remote at all.
    assert not remote.log.exists()


def test_local_slurm_transfers_stay_in_this_filesystem(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="local-copy")
    bundle = fake_remote(project, template="local-slurm", name="batch")
    source = _tree(tmp_path / "source")

    pushed = run_adapter(
        bundle,
        "push",
        {"queue": "default", "source": str(source), "destination": str(tmp_path / "copy")},
    )

    assert (tmp_path / "copy" / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert pushed["path"] == str(tmp_path / "copy")
    assert not remote.log.exists()


def test_install_reports_a_missing_remote_httk_with_the_packaging_hint(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="install-missing")
    bundle = fake_remote(project, httk_command=str(tmp_path / "nowhere" / "httk"))

    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(bundle, "install", {"queue": "default"})


def test_install_finds_the_remote_httk_and_creates_the_workspace(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="install-found")
    workspace = remote.root / "runs" / "fresh"
    bundle = fake_remote(project, workspace=str(workspace))

    result = run_adapter(bundle, "install", {"queue": "default"})

    assert result["installed"] is True
    assert result["bootstrapped"] is False
    assert result["httk_command"] == ["httk"]
    assert str(result["httk_version"]).strip()
    assert result["workspace_created"] is True
    assert workspace.is_dir()
    assert run_adapter(bundle, "install", {"queue": "default"})["workspace_created"] is False


def test_install_refuses_an_unreachable_host(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="install-unreachable")
    bundle = fake_remote(project)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")

    with pytest.raises(RuntimeError, match=f"cannot reach someone@{FAKE_HOST}"):
        run_adapter(bundle, "install", {"queue": "default"})


def test_install_attempts_pip_only_when_bootstrap_opts_in(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="bootstrap")
    remote.install("python3", _FAKE_PYTHON)
    log = tmp_path / "pip.log"
    monkeypatch.setenv("HTTK_FAKE_PIP_LOG", str(log))
    absent = str(tmp_path / "nowhere" / "httk")

    without = fake_remote(project, name="plain", httk_command=absent)
    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(without, "install", {"queue": "default"})
    assert not log.exists()

    opted_in = fake_remote(project, name="opted-in", httk_command=absent, bootstrap="pip")
    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(opted_in, "install", {"queue": "default"})
    assert json.loads(log.read_text(encoding="utf-8").splitlines()[0]) == [
        "-m",
        "pip",
        "install",
        "--user",
        "httk-workflow",
    ]


def test_configure_verifies_connectivity_for_ssh_remotes(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="configure")
    bundle = fake_remote(project)

    assert run_adapter(bundle, "configure", {"queue": "default", "settings": {}})["connectivity"] == "ok"

    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")
    with pytest.raises(RuntimeError, match="cannot reach"):
        run_adapter(bundle, "configure", {"queue": "default", "settings": {}})
    skipped = run_adapter(bundle, "configure", {"queue": "default", "settings": {"check_connectivity": "no"}})
    assert skipped["connectivity"] == "skipped"


def test_configure_checks_the_host_the_command_line_is_about_to_store(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="pending")
    bundle = add_remote("cluster", template="ssh-slurm", project=project)

    assert run_adapter(bundle, "configure", {"queue": "default", "settings": {}})["connectivity"] == "skipped"
    pending = {"host": FAKE_HOST, "username": "someone"}
    assert run_adapter(bundle, "configure", {"queue": "default", "settings": pending})["connectivity"] == "ok"


def test_an_unrecognized_adapter_kind_still_refuses(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="unknown")
    bundle = fake_remote(project)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["kind"] = "torque"
    (bundle / "remote.json").write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")

    with pytest.raises(RuntimeError, match="'torque' is not implemented"):
        run_adapter(bundle, "status", {"queue": "default", "argv": ["true"]})


def test_a_job_reaches_a_remote_workspace_and_runs_there(tmp_path: Path, remote: Remote) -> None:
    source_root = tmp_path / "project"
    initialize_project(source_root, name="end-to-end")
    Workspace(source_root).upgrade(["detached-transfer-v1"])
    destination = Workspace.initialize(
        remote.root / "runs" / "workspace",
        extensions=["detached-transfer-v1"],
    )
    fake_remote(source_root, workspace=str(destination.root))
    payload, job_id = _payload(tmp_path / "incoming")
    Workspace(source_root).submit(payload, "jobs")

    assert (
        command(
            ["transfer", "send", "cluster", job_id, "--source-workspace", str(source_root)],
            CLIContext("httk", source_root),
        )
        == 0
    )

    assert Workspace(source_root).find_marker_by_id(job_id) is None
    marker = destination.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "submitted"
    with TaskManager(destination, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    finished = destination.find_marker_by_id(job_id)
    assert finished is not None and finished.kind == "succeeded"
    # Every step of the flow really crossed the stand-in transport.
    commands = [json.loads(line)["command"] for line in remote.log.read_text(encoding="utf-8").splitlines()]
    assert any("workspace status" in item for item in commands)
    assert any("tasks receive" in item for item in commands)
    assert any(item.startswith("rsync ") or " rsync " in item for item in commands)
