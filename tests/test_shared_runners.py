"""Shared runners: one published runner file referenced by many jobs."""

import json
import stat
import uuid
from pathlib import Path

import pytest
from httk.core import CLIContext

from httk.workflow import TaskManager, Workspace
from httk.workflow._util import tree_digest
from httk.workflow.errors import UnsupportedExtensionError, WorkspaceCorruptionError
from httk.workflow.workflow_cli import command as workflow_command

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
(Path(os.environ["HTTK_WORKFLOW_WORKDIR"]) / "ran.txt").write_text(context["step"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""

_OTHER_RUNNER = _SUCCEED_RUNNER.replace('"ran.txt"', '"also-ran.txt"')


def _runner_file(root: Path, source: str = _SUCCEED_RUNNER, *, name: str = "succeed.py") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)
    return path


def _payload(root: Path, runner: dict[str, object], *, tag: str = "shared") -> tuple[Path, str]:
    """Create one payload whose job.json carries the given runner reference."""

    job_id = str(uuid.uuid4())
    payload = root / tag
    payload.mkdir(parents=True)
    job = {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": job_id,
        "tag": tag,
        "name": f"Shared runner job {tag}",
        "workflow": "tests.shared",
        "runner": runner,
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "only",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"maximum_attempts_per_activation": 1, "retry_on": []},
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _failure(workspace: Workspace, job_id: str) -> dict[str, object]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert isinstance(failure, dict)
    return failure


def test_workspace_runner_is_published_resolved_staged_and_verified(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_file(tmp_path / "source"))
    assert reference["source"] == "workspace"
    assert reference["path"] == "succeed.py"
    stored = workspace.runners / "succeed.py"
    assert stored.is_file()

    # Two jobs referencing exactly one stored runner file, which is the whole
    # point of the feature: a campaign does not copy its runner per job.
    identifiers = []
    for tag in ("first", "second"):
        payload, job_id = _payload(tmp_path / "payloads" / tag, dict(reference), tag=tag)
        workspace.submit(payload, f"project/{tag}")
        identifiers.append(job_id)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    for job_id in identifiers:
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "succeeded"
        job_root = workspace.payload_path(marker.placement, marker.job_key)
        assert (job_root / "run" / "ran.txt").read_text(encoding="utf-8") == "only"
        # The verified copy below the attempt control directory is what ran.
        staged = sorted(job_root.glob(".httk-attempt.*/runner"))
        assert len(staged) == 1
        assert staged[0].read_text(encoding="utf-8") == _SUCCEED_RUNNER
        assert stat.S_IMODE(staged[0].stat().st_mode) == 0o500


def test_installed_runner_resolves_from_a_manager_search_path(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    installed = _runner_file(tmp_path / "opt" / "runners", name="tool.py")
    digest = workspace.publish_runner(installed)["sha256"]
    (workspace.runners / "tool.py").unlink()
    payload, job_id = _payload(
        tmp_path / "payloads",
        {"source": "installed", "path": "tool.py", "sha256": digest},
        tag="installed",
    )
    workspace.submit(payload, "project/installed")
    with TaskManager(
        workspace,
        heartbeat_interval=0.01,
        runner_search_paths=(tmp_path / "opt" / "runners",),
    ) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_an_installed_runner_tree_is_pinned_and_staged_whole(tmp_path: Path) -> None:
    bundle = tmp_path / "opt" / "toolbox"
    bundle.mkdir(parents=True)
    (bundle / "outcome.py").write_text(_SUCCEED_RUNNER, encoding="utf-8")
    entry = bundle / "run"
    entry.write_text(
        "#!/usr/bin/env bash\nset -eu\nexec python3 \"$(dirname \"$0\")/outcome.py\"\n",
        encoding="utf-8",
    )
    entry.chmod(0o755)

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(
        tmp_path / "payloads",
        {"source": "installed", "path": "toolbox", "sha256": tree_digest(bundle)},
        tag="tree",
    )
    workspace.submit(payload, "project/tree")
    with TaskManager(workspace, heartbeat_interval=0.01, runner_search_paths=(tmp_path / "opt",)) as manager:
        manager.run_until_idle()

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    job_root = workspace.payload_path(marker.placement, marker.job_key)
    staged = sorted(job_root.glob(".httk-attempt.*/runner"))
    assert len(staged) == 1 and staged[0].is_dir()
    assert {item.name for item in staged[0].iterdir()} == {"run", "outcome.py"}


def test_a_replaced_runner_fails_the_job_with_runner_mismatch(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_file(tmp_path / "source"))
    payload, job_id = _payload(tmp_path / "payloads", dict(reference), tag="pinned")
    workspace.submit(payload, "project/pinned")
    # The stored runner changes after the job pinned its digest.
    workspace.publish_runner(_runner_file(tmp_path / "other", _OTHER_RUNNER, name="succeed.py"), replace=True)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    failure = _failure(workspace, job_id)
    assert failure["code"] == "runner_mismatch"
    assert str(reference["sha256"]) in str(failure["message"])


def test_an_unpublished_runner_fails_the_job_with_runner_unavailable(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    digest = "0" * 64
    absent, absent_id = _payload(
        tmp_path / "payloads" / "absent",
        {"source": "workspace", "path": "never-published.py", "sha256": digest},
        tag="absent",
    )
    workspace.submit(absent, "project/absent")
    uninstalled, uninstalled_id = _payload(
        tmp_path / "payloads" / "uninstalled",
        {"source": "installed", "path": "nowhere.py", "sha256": digest},
        tag="uninstalled",
    )
    workspace.submit(uninstalled, "project/uninstalled")
    forbidden, forbidden_id = _payload(
        tmp_path / "payloads" / "forbidden",
        {"source": "installed", "path": "pkg:tests.evil/runner.py", "sha256": digest},
        tag="forbidden",
    )
    workspace.submit(forbidden, "project/forbidden")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    for job_id in (absent_id, uninstalled_id, forbidden_id):
        assert _failure(workspace, job_id)["code"] == "runner_unavailable"
    assert "allowlist" in str(_failure(workspace, forbidden_id)["message"])


def test_publish_refuses_a_different_digest_without_replace(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    first = workspace.publish_runner(_runner_file(tmp_path / "source"), name="tools/step.py")
    assert first["path"] == "tools/step.py"
    # Republishing identical bytes is an idempotent no-op.
    assert workspace.publish_runner(_runner_file(tmp_path / "source"), name="tools/step.py") == first
    changed = _runner_file(tmp_path / "other", _OTHER_RUNNER, name="step.py")
    with pytest.raises(FileExistsError, match="already holds a different digest"):
        workspace.publish_runner(changed, name="tools/step.py")
    assert (workspace.runners / "tools" / "step.py").read_text(encoding="utf-8") == _SUCCEED_RUNNER
    replaced = workspace.publish_runner(changed, name="tools/step.py", replace=True)
    assert replaced["sha256"] != first["sha256"]
    assert (workspace.runners / "tools" / "step.py").read_text(encoding="utf-8") == _OTHER_RUNNER


def test_runner_publish_command_prints_the_job_reference(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    runner = _runner_file(tmp_path / "source")
    code = workflow_command(
        ["runner", "publish", str(runner), "--workspace", str(workspace.root), "--name", "campaign/step.py"],
        context,
    )
    assert code == 0
    reference = json.loads(capsys.readouterr().out)
    assert reference == {
        "source": "workspace",
        "path": "campaign/step.py",
        "sha256": workspace.publish_runner(runner, name="campaign/step.py")["sha256"],
    }
    changed = _runner_file(tmp_path / "other", _OTHER_RUNNER, name="step.py")
    conflict = workflow_command(
        ["runner", "publish", str(changed), "--workspace", str(workspace.root), "--name", "campaign/step.py"],
        context,
    )
    assert conflict == 2
    assert "pass replace to overwrite it" in capsys.readouterr().err
    assert (
        workflow_command(
            [
                "runner",
                "publish",
                str(changed),
                "--workspace",
                str(workspace.root),
                "--name",
                "campaign/step.py",
                "--replace",
            ],
            context,
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["sha256"] != reference["sha256"]


def test_detached_transfer_carries_the_workspace_runner_it_references(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source", extensions=["detached-transfer-v1"])
    destination = Workspace.initialize(tmp_path / "destination", extensions=["detached-transfer-v1"])
    reference = source.publish_runner(_runner_file(tmp_path / "runners"))
    payload, job_id = _payload(tmp_path / "payloads", dict(reference), tag="detached")
    source.submit(payload, "project/detached")

    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = json.loads((bundle / ".httk-transfer" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["runners"] == [{"path": "succeed.py", "sha256": reference["sha256"]}]
    assert (bundle / ".httk-transfer" / "runners" / "succeed.py").is_file()

    acknowledgement = destination.import_bundle(bundle)
    assert acknowledgement["job_id"] == job_id
    installed = destination.runners / "succeed.py"
    assert installed.read_text(encoding="utf-8") == _SUCCEED_RUNNER
    # A second import of the same bundle content is idempotent, not a conflict.
    assert destination.import_bundle(bundle)["transfer_id"] == acknowledgement["transfer_id"]

    with TaskManager(destination, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = destination.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_import_refuses_a_runner_name_holding_different_content(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source", extensions=["detached-transfer-v1"])
    destination = Workspace.initialize(tmp_path / "destination", extensions=["detached-transfer-v1"])
    reference = source.publish_runner(_runner_file(tmp_path / "runners"))
    destination.publish_runner(_runner_file(tmp_path / "other", _OTHER_RUNNER, name="succeed.py"))
    payload, job_id = _payload(tmp_path / "payloads", dict(reference), tag="conflicting")
    source.submit(payload, "project/conflicting")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    with pytest.raises(WorkspaceCorruptionError, match="holds digest"):
        destination.import_bundle(bundle)


def test_core_v1_workspaces_are_readable_but_never_mutated(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    format_path = workspace.control / "format.json"
    assert json.loads(format_path.read_text(encoding="utf-8"))["core_profile"] == "core-v2"
    stored = json.loads(format_path.read_text(encoding="utf-8"))
    stored["core_profile"] = "core-v1"
    format_path.write_text(json.dumps(stored), encoding="utf-8")

    with pytest.raises(UnsupportedExtensionError, match="core-v1"):
        Workspace(workspace.root)
    inspected = Workspace(workspace.root, mutable=False)
    assert inspected.core_profile == "core-v1"
    with pytest.raises(UnsupportedExtensionError, match="cannot serve"):
        TaskManager(inspected)


def test_runner_describe_reports_every_published_reference(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    first = workspace.publish_runner(_runner_file(tmp_path / "source"))
    second = workspace.publish_runner(
        _runner_file(tmp_path / "other", _OTHER_RUNNER, name="step.py"),
        name="campaign/step.py",
    )

    assert workflow_command(["runner", "describe", "--workspace", str(workspace.root), "--json"], context) == 0
    described = json.loads(capsys.readouterr().out)
    assert described == sorted([first, second], key=lambda reference: str(reference["path"]))

    assert workflow_command(["runner", "describe", "--workspace", str(workspace.root)], context) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines == [f"{reference['path']}\t{reference['sha256']}" for reference in described]

    # One name at a time, and a name that was never published is an error.
    argv = ["runner", "describe", "campaign/step.py", "--workspace", str(workspace.root), "--json"]
    assert workflow_command(argv, context) == 0
    assert json.loads(capsys.readouterr().out) == [second]
    assert workflow_command(["runner", "describe", "absent.py", "--workspace", str(workspace.root)], context) == 2
    assert "no such workspace runner" in capsys.readouterr().err
