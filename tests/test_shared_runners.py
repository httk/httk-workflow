"""Shared runners: one published runner file referenced by many jobs."""

import json
import os
import stat
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, _manager_runners
from httk.workflow.errors import FormatError, RunnerResolutionError, WorkspaceCorruptionError
from httk.workflow.protocol import JobSpec, prepare_job_payload
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
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""

_OTHER_RUNNER = _SUCCEED_RUNNER.replace('"ran.txt"', '"also-ran.txt"')
_RESERVED_CHECK_RUNNER = _SUCCEED_RUNNER.replace(
    'control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])',
    'control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])\n'
    '(Path(os.environ["HTTK_WORKFLOW_WORKDIR"]) / "reserved.txt").write_text(\n'
    '    ",".join(os.environ.get(name, "absent") for name in '
    '["HTTK_WORKFLOW_RUNNER_ROOT", "HTTK_WORKFLOW_RUNNER_ARTIFACTS"])\n'
    ')',
)


def _runner_file(root: Path, source: str = _SUCCEED_RUNNER, *, name: str = "succeed.py") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)
    return path


def _runner_tree(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "outcome.py").write_text(_SUCCEED_RUNNER, encoding="utf-8")
    entry = root / "run"
    entry.write_text(
        "#!/usr/bin/env bash\nset -eu\nexec python3 \"$(dirname \"$0\")/outcome.py\"\n",
        encoding="utf-8",
    )
    entry.chmod(0o755)
    return root


def _payload(root: Path, runner: dict[str, object], *, tag: str = "shared") -> tuple[Path, str]:
    """Create one payload whose job.json carries the given runner reference."""

    job_id = str(uuid.uuid4())
    payload = root / tag
    payload.mkdir(parents=True)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
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


def test_workspace_runner_is_published_resolved_and_verified_in_place(tmp_path: Path) -> None:
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
        attempt = next(job_root.joinpath("attempts").iterdir())
        assert not (attempt / "runner").exists()
        assert {entry.name for entry in attempt.iterdir()} <= {"context.json", "outcome.ready"}
        events = [
            json.loads(line) for line in (job_root / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        event = next(item for item in events if item["kind"] == "attempt")
        assert event["runner_path"] == str(stored)
        assert event["runner_sha256"] == reference["sha256"]


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


def test_a_non_executable_installed_runner_is_unavailable(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    installed = _runner_file(tmp_path / "opt" / "runners", name="tool.py")
    digest = workspace.publish_runner(installed)["sha256"]
    (workspace.runners / "tool.py").unlink()
    installed.chmod(0o644)
    payload, job_id = _payload(
        tmp_path / "payloads",
        {"source": "installed", "path": "tool.py", "sha256": digest},
        tag="not-executable",
    )
    workspace.submit(payload, "project/not-executable")
    with TaskManager(workspace, heartbeat_interval=0.01, runner_search_paths=(installed.parent,)) as manager:
        manager.run_until_idle()
    assert _failure(workspace, job_id)["code"] == "runner_unavailable"


def test_an_installed_runner_tree_is_pinned_and_executed_in_place(tmp_path: Path) -> None:
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
    attempt = next(job_root.joinpath("attempts").iterdir())
    assert not (attempt / "runner").exists()
    assert (bundle / "run").is_file()


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


def test_verifying_a_file_does_not_advance_its_launch_fd(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_file(tmp_path / "source"))
    payload, job_id = _payload(tmp_path / "payloads", dict(reference), tag="offset")
    workspace.submit(payload, "project/offset")
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    job = workspace.load_job(marker)
    manager = type(
        "Manager",
        (),
        {"workspace": workspace, "runner_search_paths": (), "runner_modules": ("httk.workflow",)},
    )()
    verified = _manager_runners.verify_runner(manager, job)
    assert verified.fd is not None
    try:
        assert os.lseek(verified.fd, 0, os.SEEK_CUR) == 0
    finally:
        os.close(verified.fd)


def test_a_file_replaced_during_verification_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_file(tmp_path / "source"))
    payload, job_id = _payload(tmp_path / "payloads", dict(reference), tag="replaced-during-verification")
    workspace.submit(payload, "project/replaced-during-verification")
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    job = workspace.load_job(marker)
    stored = workspace.runner_store_path("succeed.py")
    replacement = _runner_file(tmp_path / "replacement", _OTHER_RUNNER, name="replacement.py")
    original_hash = _manager_runners._hash_fd

    def replace_after_hash(fd: int) -> str:
        digest = original_hash(fd)
        os.replace(replacement, stored)
        return digest

    monkeypatch.setattr(_manager_runners, "_hash_fd", replace_after_hash)
    manager = type(
        "Manager", (), {"workspace": workspace, "runner_search_paths": (), "runner_modules": ("httk.workflow",)}
    )()
    with pytest.raises(RunnerResolutionError, match="replaced during verification") as failure:
        _manager_runners.verify_runner(manager, job)
    assert failure.value.code == "runner_unavailable"


def test_a_file_replaced_after_verification_executes_the_verified_inode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_file(tmp_path / "source"))
    payload, job_id = _payload(tmp_path / "payloads", dict(reference), tag="inode-pinned")
    workspace.submit(payload, "project/inode-pinned")
    stored = workspace.runner_store_path("succeed.py")
    replacement = _runner_file(tmp_path / "replacement", _OTHER_RUNNER, name="replacement.py")
    original_write = os.write
    replaced = False

    def replace_before_gate(fd: int, data: bytes) -> int:
        nonlocal replaced
        if data == b"R" and not replaced:
            os.replace(replacement, stored)
            replaced = True
        return original_write(fd, data)

    monkeypatch.setattr(os, "write", replace_before_gate)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    job_root = workspace.payload_path(marker.placement, marker.job_key)
    assert (job_root / "run" / "ran.txt").is_file()
    assert not (job_root / "run" / "also-ran.txt").exists()


def test_reserved_runner_variables_are_not_inherited_by_payload_or_buildless_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTK_WORKFLOW_RUNNER_ROOT", "poison-root")
    monkeypatch.setenv("HTTK_WORKFLOW_RUNNER_ARTIFACTS", "poison-artifacts")
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, payload_id = _payload(
        tmp_path / "payloads" / "payload",
        {"source": "payload", "path": "runner.py"},
        tag="payload",
    )
    payload_runner = payload / "runner.py"
    payload_runner.write_text(_RESERVED_CHECK_RUNNER, encoding="utf-8")
    payload_runner.chmod(0o755)
    tree = _runner_tree(tmp_path / "tree" / "toolbox")
    (tree / "outcome.py").write_text(_RESERVED_CHECK_RUNNER, encoding="utf-8")
    tree_reference = workspace.publish_runner(tree, name="toolbox")
    tree_payload, tree_id = _payload(tmp_path / "payloads" / "tree", dict(tree_reference), tag="tree")
    workspace.submit(payload, "project/payload")
    workspace.submit(tree_payload, "project/tree")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    payload_marker = workspace.find_marker_by_id(payload_id)
    tree_marker = workspace.find_marker_by_id(tree_id)
    assert payload_marker is not None and payload_marker.kind == "succeeded"
    assert tree_marker is not None and tree_marker.kind == "succeeded"
    payload_root = workspace.payload_path(payload_marker.placement, payload_marker.job_key)
    tree_root = workspace.payload_path(tree_marker.placement, tree_marker.job_key)
    assert (payload_root / "run" / "reserved.txt").read_text(encoding="utf-8") == "absent,absent"
    assert (tree_root / "run" / "reserved.txt").read_text(encoding="utf-8") == (
        f"{workspace.runners / 'toolbox'},absent"
    )


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


def test_workspace_runner_tree_is_content_addressed_read_only_and_replaceable(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source = _runner_tree(tmp_path / "source" / "toolbox")
    first = workspace.publish_runner(source, name="tools/toolbox")
    stored = workspace.runners / "tools" / "toolbox"
    assert first == {
        "source": "workspace",
        "path": "tools/toolbox",
        "sha256": tree_digest(source),
    }
    assert stored.is_dir()
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o555 for path in (stored, *stored.rglob("*")))
    assert workspace.publish_runner(source, name="tools/toolbox") == first

    (source / "outcome.py").write_text(_OTHER_RUNNER, encoding="utf-8")
    changed_digest = tree_digest(source)
    with pytest.raises(FileExistsError, match="already holds a different digest"):
        workspace.publish_runner(source, name="tools/toolbox")
    replaced = workspace.publish_runner(source, name="tools/toolbox", replace=True)
    assert replaced["sha256"] == changed_digest != first["sha256"]
    assert (stored / "outcome.py").read_text(encoding="utf-8") == _OTHER_RUNNER
    assert not any(path.name.startswith("runner-old.") for path in workspace.control.joinpath("tmp").iterdir())


def test_failed_tree_install_cleans_nested_read_only_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source = _runner_tree(tmp_path / "source" / "toolbox")
    (source / "support" / "nested").mkdir(parents=True)
    (source / "support" / "nested" / "member.txt").write_text("support", encoding="utf-8")
    target = workspace.runner_store_path("toolbox")
    real_replace = os.replace

    def fail_new_tree(source_path: str | os.PathLike[str], target_path: str | os.PathLike[str]) -> None:
        if Path(source_path).name.startswith("runner.") and Path(target_path) == target:
            raise OSError("injected install failure")
        real_replace(source_path, target_path)

    monkeypatch.setattr(os, "replace", fail_new_tree)
    with pytest.raises(OSError, match="injected install failure"):
        workspace.publish_runner(source, name="toolbox")
    assert not target.exists()
    assert not any(path.name.startswith("runner.") for path in workspace.control.joinpath("tmp").iterdir())


def test_failed_tree_replacement_restores_a_read_only_old_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    old_source = _runner_tree(tmp_path / "old" / "toolbox")
    workspace.publish_runner(old_source, name="toolbox")
    new_source = _runner_tree(tmp_path / "new" / "toolbox")
    (new_source / "outcome.py").write_text(_OTHER_RUNNER, encoding="utf-8")
    target = workspace.runner_store_path("toolbox")
    real_replace = os.replace

    def fail_new_tree(source_path: str | os.PathLike[str], target_path: str | os.PathLike[str]) -> None:
        if Path(source_path).name.startswith("runner.") and Path(target_path) == target:
            raise OSError("injected replacement failure")
        real_replace(source_path, target_path)

    monkeypatch.setattr(os, "replace", fail_new_tree)
    with pytest.raises(OSError, match="injected replacement failure"):
        workspace.publish_runner(new_source, name="toolbox", replace=True)
    assert (target / "outcome.py").read_text(encoding="utf-8") == _SUCCEED_RUNNER
    assert stat.S_IMODE(target.stat().st_mode) == 0o555


def test_publish_runner_tree_rejects_symlinks_and_type_mismatches(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source = _runner_tree(tmp_path / "source" / "toolbox")
    with pytest.raises(FormatError, match="symlink"):
        (source / "link").symlink_to(source / "run")
        workspace.publish_runner(source)
    (source / "link").unlink()

    file_source = _runner_file(tmp_path / "file-source", name="entry")
    workspace.publish_runner(file_source, name="same")
    with pytest.raises(FormatError, match="type does not match"):
        workspace.publish_runner(source, name="same")
    workspace.publish_runner(source, name="tree")
    with pytest.raises(FormatError, match="type does not match"):
        workspace.publish_runner(file_source, name="tree")


def test_workspace_runner_tree_is_verified_and_executed_in_place(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_tree(tmp_path / "source" / "toolbox"), name="toolbox")
    payload = tmp_path / "payload"
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="workspace tree",
            workflow="tests.shared",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            initial_step="only",
        ),
    )
    workspace.submit(payload, "project/workspace-tree")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    job_root = workspace.payload_path(marker.placement, marker.job_key)
    attempt = next(job_root.joinpath("attempts").iterdir())
    assert not (attempt / "runner").exists()
    assert (workspace.runners / "toolbox" / "run").is_file()


def test_tampering_with_a_pinned_tree_support_member_fails_at_execution(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_tree(tmp_path / "source" / "toolbox"), name="toolbox")
    payload = tmp_path / "payload"
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="tampered workspace tree",
            workflow="tests.shared",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            initial_step="only",
        ),
    )
    stored = workspace.runners / "toolbox"
    stored.chmod(0o755)
    (stored / "outcome.py").chmod(0o644)
    (stored / "outcome.py").write_text(_OTHER_RUNNER, encoding="utf-8")
    workspace.submit(payload, "project/tampered-tree")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    assert _failure(workspace, job.id)["code"] == "runner_mismatch"


def test_published_runner_tree_tampering_changes_its_pinned_digest(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(_runner_tree(tmp_path / "source" / "toolbox"), name="toolbox")
    stored = workspace.runners / "toolbox"
    stored.chmod(0o755)
    (stored / "outcome.py").chmod(0o644)
    (stored / "outcome.py").write_text(_OTHER_RUNNER, encoding="utf-8")
    assert tree_digest(stored) != reference["sha256"]


def test_runner_publish_command_prints_the_job_reference(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)
    runner = _runner_file(tmp_path / "source")
    code = workflow_command(
        ["runner", "publish", "--workspace", ws, "--name", "campaign/step.py", "--json", str(runner)],
        context,
    )
    assert code == 0
    reference = json.loads(capsys.readouterr().out)
    assert reference == [
        {
            "source": "workspace",
            "path": "campaign/step.py",
            "sha256": workspace.publish_runner(runner, name="campaign/step.py")["sha256"],
        }
    ]
    changed = _runner_file(tmp_path / "other", _OTHER_RUNNER, name="step.py")
    conflict = workflow_command(
        ["runner", "publish", "--workspace", ws, "--name", "campaign/step.py", str(changed)],
        context,
    )
    assert conflict == 1
    assert "pass replace to overwrite it" in capsys.readouterr().err
    assert (
        workflow_command(
            [
                "runner",
                "publish",
                str(changed),
                "--workspace",
                ws,
                "--name",
                "campaign/step.py",
                "--replace",
                "--json",
            ],
            context,
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)[0]["sha256"] != reference[0]["sha256"]


def test_detached_transfer_carries_the_workspace_runner_it_references(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
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
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    reference = source.publish_runner(_runner_file(tmp_path / "runners"))
    destination.publish_runner(_runner_file(tmp_path / "other", _OTHER_RUNNER, name="succeed.py"))
    payload, job_id = _payload(tmp_path / "payloads", dict(reference), tag="conflicting")
    source.submit(payload, "project/conflicting")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    with pytest.raises(WorkspaceCorruptionError, match="holds digest"):
        destination.import_bundle(bundle)


def test_runner_describe_reports_every_published_reference(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)
    first = workspace.publish_runner(_runner_file(tmp_path / "source"))
    second = workspace.publish_runner(
        _runner_file(tmp_path / "other", _OTHER_RUNNER, name="step.py"),
        name="campaign/step.py",
    )
    tree = workspace.publish_runner(_runner_tree(tmp_path / "tree" / "toolbox"), name="campaign/toolbox")

    assert workflow_command(["runner", "describe", "--workspace", ws, "--json"], context) == 0
    described = json.loads(capsys.readouterr().out)
    expected = [
        {**first, "kind": "file", "inferred": False},
        {**second, "kind": "file", "inferred": False},
        {**tree, "kind": "tree", "inferred": True},
    ]
    assert described == sorted(expected, key=lambda reference: str(reference["path"]))

    assert workflow_command(["runner", "describe", "--workspace", ws], context) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines == [
        f"{reference['path']}\t{reference['sha256']}"
        + ("\ttree (inferred)" if workspace.runner_store_path(str(reference["path"])).is_dir() else "")
        for reference in described
    ]

    # One name at a time, and a name that was never published is an error.
    argv = ["runner", "describe", "--workspace", ws, "--json", "campaign/step.py"]
    assert workflow_command(argv, context) == 0
    assert json.loads(capsys.readouterr().out) == [{**second, "kind": "file", "inferred": False}]
    assert workflow_command(["runner", "describe", "--workspace", ws, "--json", "campaign/toolbox"], context) == 0
    assert json.loads(capsys.readouterr().out) == [{**tree, "kind": "tree", "inferred": True}]
    assert workflow_command(["runner", "describe", "--workspace", ws, "absent.py"], context) == 1
    assert "no such workspace runner" in capsys.readouterr().err
