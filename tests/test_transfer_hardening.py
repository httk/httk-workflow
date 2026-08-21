"""What a detached transfer bundle pins, refuses, and recovers from."""

import argparse
import ast
import json
import os
import shutil
import sys
import uuid
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest
from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

from conftest import register_ws
from httk.workflow import FormatError, TaskManager, Workspace
from httk.workflow import transfers as transfers_module
from httk.workflow._runner_builds import register_build
from httk.workflow.packages import source_tree_digest
from httk.workflow.scaffold import BuildSpec, new_job
from httk.workflow.transfers import (
    TRANSFER_DIRECTORY,
    TRANSFER_FORMAT_VERSION,
    TRANSFER_MANIFEST,
    _payload_digest,
    offer_transfers,
    validate_bundle,
)
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command
from test_native_java_api import _build_example


def _payload(root: Path, *, tag: str = "test") -> tuple[Path, str]:
    """Write one minimal, valid payload and return it with its job UUID."""

    job_id = str(uuid.uuid4())
    payload = root / "payload"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": tag,
                "name": tag,
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


def _pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    return source, destination


def test_resuming_a_transfer_requires_the_destination_remote_to_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = Workspace.initialize(tmp_path / "source")
    transfer_dir = source.control / "transfers"
    transfer_dir.mkdir(parents=True, exist_ok=True)
    foreign_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    (transfer_dir / f"{foreign_id}.json").write_text(
        json.dumps(
            {
                "transfer_id": foreign_id,
                "job_id": job_id,
                "destination_workspace_id": "destination-id",
                "destination_remote": "other-cluster",
                "status": "sealed",
            }
        ),
        encoding="utf-8",
    )
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    target = SimpleNamespace(name="cluster", bundle=tmp_path / "adapter")
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(transfer_cli, "_remote_workspace_probe", lambda *_args, **_kwargs: ("destination-id", "/dest"))

    def detach(_job_id: str, **kwargs: object) -> Path:
        calls.append(kwargs)
        return bundle

    monkeypatch.setattr(source, "detach", detach)
    monkeypatch.setattr(source, "acknowledge_transfer", lambda _acknowledgement: None)

    def adapter(
        _bundle: Path, operation: str, _request: dict[str, object], *, timeout: float | None
    ) -> dict[str, object]:
        if operation == "push":
            return {"path": "/dest/incoming"}
        return {"returncode": 0, "stdout": json.dumps({"transfer_id": str(uuid.uuid4())})}

    monkeypatch.setattr(transfer_cli, "run_adapter", adapter)
    transfer_cli._send_jobs_to_remote(source, target, "destination", [job_id], destination_placement=None, timeout=None)
    assert calls and calls[0]["destination_remote"] == "cluster"
    assert calls[0]["transfer_id"] != foreign_id


def test_a_sealed_ledger_without_destination_remote_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = Workspace.initialize(tmp_path / "source")
    transfer_dir = source.control / "transfers"
    transfer_dir.mkdir(parents=True, exist_ok=True)
    transfer_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    ledger_path = transfer_dir / f"{transfer_id}.json"
    ledger_path.write_text(
        json.dumps(
            {
                "transfer_id": transfer_id,
                "job_id": job_id,
                "destination_workspace_id": "destination-id",
                "status": "sealed",
            }
        ),
        encoding="utf-8",
    )
    target = SimpleNamespace(name="cluster", bundle=tmp_path / "adapter")
    monkeypatch.setattr(transfer_cli, "_remote_workspace_probe", lambda *_args, **_kwargs: ("destination-id", "/dest"))

    with pytest.raises(ValueError, match="has no destination_remote"):
        transfer_cli._send_jobs_to_remote(
            source, target, "destination", [job_id], destination_placement=None, timeout=None
        )


def test_transfer_adapter_requests_are_exact_argv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = Workspace.initialize(tmp_path / "source")
    target = SimpleNamespace(name="cluster", bundle=tmp_path / "adapter")
    captured: list[tuple[str, ...]] = []
    detached: list[dict[str, object]] = []
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    job_id = str(uuid.uuid4())

    def adapter(
        _bundle: Path, operation: str, request: dict[str, object], *, timeout: float | None
    ) -> dict[str, object]:
        if operation == "invoke":
            captured.append(tuple(request["argv"]))  # type: ignore[arg-type]
            if captured[-1][3] == "offer":
                return {
                    "returncode": 0,
                    "stdout": json.dumps({"format": "httk-workflow-transfer-offer", "format_version": 2, "offers": []}),
                }
            if captured[-1][3] == "retire":
                return {"returncode": 0, "stdout": json.dumps({"retired": []})}
            return {"returncode": 0, "stdout": json.dumps({"transfer_id": detached[0]["transfer_id"]})}
        return {"path": f"/dest/incoming/{detached[0]['transfer_id']}"}

    monkeypatch.setattr(transfer_cli, "run_adapter", adapter)
    monkeypatch.setattr(transfer_cli, "_remote_workspace_probe", lambda *_args, **_kwargs: ("destination-id", "/dest"))

    def detach(_job_id: str, **kwargs: object) -> Path:
        detached.append(kwargs)
        return bundle

    monkeypatch.setattr(source, "detach", detach)
    monkeypatch.setattr(source, "acknowledge_transfer", lambda _acknowledgement: None)

    transfer_cli._remote_offer(
        target,
        "station",
        "destination-id",
        states=("succeeded",),
        placement=None,
        timeout=None,
    )
    transfer_cli._remote_retire(target, "station", [job_id], "destination-id", timeout=None)
    transfer_cli._send_jobs_to_remote(source, target, "destination", [job_id], destination_placement=None, timeout=None)

    transfer_id = str(detached[0]["transfer_id"])
    assert captured == [
        (
            "httk",
            "workflow",
            "transfer",
            "offer",
            "station",
            "--destination-workspace-id",
            "destination-id",
            "--json",
            "--state",
            "succeeded",
        ),
        (
            "httk",
            "workflow",
            "transfer",
            "retire",
            "station",
            job_id,
            "--destination-workspace-id",
            "destination-id",
            "--json",
        ),
        (
            "httk",
            "workflow",
            "transfer",
            "receive",
            "--workspace",
            "destination",
            "--bundle",
            f"/dest/incoming/{transfer_id}",
        ),
    ]


# ---------------------------------------------------------------------------
# What the payload digest pins
# ---------------------------------------------------------------------------


def test_the_payload_digest_pins_the_executable_bit(tmp_path: Path) -> None:
    payload, _ = _payload(tmp_path)
    executable = _payload_digest(payload)
    (payload / "files" / "runner").chmod(0o644)
    plain = _payload_digest(payload)
    # A runner that arrives without its executable bit does not run, so losing
    # the bit has to be a digest mismatch rather than a silent success.
    assert plain != executable
    (payload / "files" / "runner").chmod(0o755)
    assert _payload_digest(payload) == executable


def test_the_payload_digest_pins_the_target_of_a_symlink(tmp_path: Path) -> None:
    payload, _ = _payload(tmp_path)
    (payload / "run").mkdir()
    link = payload / "run" / "inputs"
    link.symlink_to("../files")
    first = _payload_digest(payload)
    link.unlink()
    link.symlink_to("../job.json")
    # The link is hashed as its literal target, exactly as a signed project
    # manifest records one, so retargeting it moves the digest.
    assert _payload_digest(payload) != first
    link.unlink()
    link.symlink_to("../files")
    assert _payload_digest(payload) == first


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("/etc/passwd", "absolute symlink"),
        ("../../elsewhere", "escaping symlink"),
        ("../files/../../elsewhere", "escaping symlink"),
    ],
)
def test_a_symlink_that_leaves_the_payload_is_refused_by_name(tmp_path: Path, target: str, message: str) -> None:
    payload, _ = _payload(tmp_path)
    (payload / "files" / "outside").symlink_to(target)
    with pytest.raises(FormatError, match=message):
        _payload_digest(payload)


def test_a_bundle_sealed_under_the_previous_digest_rule_is_refused_by_version(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest_path = bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["format_version"] == TRANSFER_FORMAT_VERSION == 2
    manifest["format_version"] = 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    # A manifest at any other version is refused by the strict format gate.
    with pytest.raises(FormatError, match="unsupported detached transfer manifest"):
        validate_bundle(bundle)


# ---------------------------------------------------------------------------
# What survives a transfer
# ---------------------------------------------------------------------------


def test_a_v1_style_relative_symlink_transfers_and_survives_the_import(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    marker = source.submit(payload, "jobs")
    live = source.payload_path(marker.placement, marker.job_key)
    # Exactly the shape the v1 compatibility executor plants in a live payload.
    legacy = live / "ht.task"
    legacy.mkdir()
    (legacy / "inputs").symlink_to("../files")

    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = json.loads((bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST).read_text(encoding="utf-8"))
    destination.import_bundle(bundle)

    imported = destination.find_marker_by_id(job_id)
    assert imported is not None
    arrived = destination.payload_path(imported.placement, imported.job_key)
    link = arrived / "ht.task" / "inputs"
    assert link.is_symlink() and os.readlink(link) == "../files"
    assert (link / "runner").is_file()
    assert (arrived / "files" / "runner").stat().st_mode & 0o111
    assert _payload_digest(arrived) == manifest["payload_sha256"]


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


def test_a_detach_interrupted_before_sealing_is_completed_by_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")

    def interrupt(*arguments: object, **keywords: object) -> Path:
        raise RuntimeError("simulated interruption between the transition and the seal")

    monkeypatch.setattr(transfers_module, "_seal_transferring", interrupt)
    with pytest.raises(RuntimeError):
        source.detach(job_id, destination_workspace_id=destination.workspace_id)
    # The job is fenced: it is no longer schedulable, and the only marker left
    # is the transferring one no bundle has been sealed around yet.
    assert source.find_marker_by_id(job_id) is None
    fenced = [marker for marker in source.scan_markers(("transferring",)) if marker.job_id == job_id]
    assert len(fenced) == 1
    monkeypatch.undo()

    recovered = source.recover_transfers()
    assert len(recovered) == 1 and recovered[0]["status"] == "sealed"
    bundle = Path(str(recovered[0]["bundle"]))
    assert validate_bundle(bundle)["job_id"] == job_id
    assert destination.import_bundle(bundle)["job_id"] == job_id


def test_a_sealed_but_unsent_bundle_is_offered_again(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = validate_bundle(bundle)

    # A crash between the seal and the copy loses nothing: the sealed bundle is
    # what a later offer reports, under the transfer UUID it was sealed with.
    offers = offer_transfers(
        source,
        destination_workspace_id=destination.workspace_id,
        states=("submitted",),
    )
    assert len(offers) == 1
    assert offers[0]["transfer_id"] == manifest["transfer_id"]
    assert offers[0]["bundle_path"] == str(bundle)
    assert offers[0]["job_id"] == job_id

    # Even a lost ledger is rebuilt from the bundle the workspace still holds.
    ledger = source.control / "transfers" / f"{manifest['transfer_id']}.json"
    ledger.unlink()
    assert offer_transfers(source, destination_workspace_id=destination.workspace_id, states=("submitted",)) == offers


def test_explicit_offer_reports_a_late_sealing_failure_and_resumes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, destination = _pair(tmp_path)
    first_payload, first_id = _payload(tmp_path / "first", tag="first")
    second_payload, second_id = _payload(tmp_path / "second", tag="second")
    source.submit(first_payload, "jobs")
    source.submit(second_payload, "jobs")
    real_detach = transfers_module.detach_job

    def fail_second(
        workspace: Workspace,
        job_id: str,
        *,
        destination_workspace_id: str,
        destination_remote: str | None = None,
        destination_placement: str | PurePosixPath | None = None,
        transfer_id: str | None = None,
    ) -> Path:
        if job_id == second_id:
            raise ValueError("job became active")
        return real_detach(
            workspace,
            job_id,
            destination_workspace_id=destination_workspace_id,
            destination_remote=destination_remote,
            destination_placement=destination_placement,
            transfer_id=transfer_id,
        )

    monkeypatch.setattr(transfers_module, "detach_job", fail_second)
    with pytest.raises(ValueError, match=f"{second_id}.*already-sealed jobs remain sealed"):
        offer_transfers(
            source,
            destination_workspace_id=destination.workspace_id,
            states=("submitted",),
            job_ids=(first_id, second_id),
        )
    assert source.find_marker_by_id(second_id) is not None
    assert source.find_marker_by_id(first_id) is None

    monkeypatch.setattr(transfers_module, "detach_job", real_detach)
    resumed = offer_transfers(
        source,
        destination_workspace_id=destination.workspace_id,
        states=("submitted",),
        job_ids=(first_id, second_id),
    )
    assert {str(offer["job_id"]) for offer in resumed} == {first_id, second_id}


def test_a_source_bundle_already_moved_aside_is_retired_without_a_second_move(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    acknowledgement = destination.import_bundle(bundle)
    transfer_id = str(acknowledgement["transfer_id"])

    # Simulate a crash between the rename and the ledger write.
    retired = source.control / "transfers" / "retired" / transfer_id / "bundle"
    retired.parent.mkdir(parents=True, exist_ok=True)
    os.rename(bundle, retired)
    ledger = source.control / "transfers" / f"{transfer_id}.json"
    assert json.loads(ledger.read_text(encoding="utf-8"))["status"] == "sealed"

    assert source.acknowledge_transfer(acknowledgement) == retired
    assert json.loads(ledger.read_text(encoding="utf-8"))["status"] == "retired"
    assert (retired / TRANSFER_DIRECTORY / TRANSFER_MANIFEST).is_file()


@pytest.mark.skipif(shutil.which("javac") is None or shutil.which("java") is None, reason="javac and java are required")
def test_a_directory_runner_survives_detach_bundle_import_and_execution(tmp_path: Path) -> None:
    package = _build_example(tmp_path / "java-build")
    poscar = tmp_path / "POSCAR"
    poscar.write_text(
        "silicon\n1.0\n2.0 0.0 0.0\n0.0 2.0 0.0\n0.0 0.0 2.0\nSi\n2\nDirect\n"
        "0.0000000000 0.0000000000 0.0000000000\n0.5000000000 0.5000000000 0.5000000000\n",
        encoding="utf-8",
    )
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    destination.set_setting(
        "vasp.command", f"{sys.executable} {Path(__file__).parents[1] / 'examples' / 'mock_vasp.py'}"
    )
    job = new_job(source, package, files={"POSCAR": poscar}, step="prepare", data_mode="transactional")
    job_document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    runner_path = str(job_document["runner"]["path"])
    expected_digest = source_tree_digest(package)

    bundle = source.detach(job.job_id, destination_workspace_id=destination.workspace_id)
    manifest = validate_bundle(bundle)
    assert manifest["runners"] == [{"path": runner_path, "sha256": expected_digest}]
    bundled_runner = bundle / TRANSFER_DIRECTORY / "runners" / Path(*runner_path.split("/"))
    assert not (bundled_runner / "classes").exists()
    destination.import_bundle(bundle)

    stored = destination.runner_store_path(runner_path)
    assert stored.is_dir()
    assert tree_digest(stored) == expected_digest
    register_build(
        destination,
        stored,
        PurePosixPath(runner_path),
        BuildSpec("make", ("classes",)),
        source_sha256=expected_digest,
    )
    with TaskManager(destination, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)
    marker = destination.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_receive_reminds_about_build_registration_for_bundled_runner_trees(tmp_path: Path, capsys) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    package = tmp_path / "compiled"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        "[workflow]\nid = 'compiled.transfer'\n[workflow.runner]\nsteps = ['start']\n"
        "[workflow.build]\ncommand = './build.sh'\nartifacts = ['out']\n",
        encoding="utf-8",
    )
    (package / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "build.sh").write_text("#!/bin/sh\nmkdir out\n", encoding="utf-8")
    (package / "build.sh").chmod(0o755)
    job = new_job(source, package)
    bundle = source.detach(job.job_id, destination_workspace_id=destination.workspace_id)

    assert (
        transfer_cli.handle_transfer_receive(
            argparse.Namespace(workspace=str(destination.root), bundle=str(bundle)),
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    output = capsys.readouterr().err
    assert "workflow compiled.transfer declares a build" in output
    assert "httk workflow build" in output


def test_local_transfer_command_reminds_after_importing_a_build_declaring_runner(tmp_path: Path, capsys) -> None:
    source = Workspace.initialize(tmp_path / "source-command")
    destination = Workspace.initialize(tmp_path / "destination-command")
    package = tmp_path / "compiled-command"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        "[workflow]\nid = 'compiled.transfer.command'\n[workflow.runner]\nsteps = ['start']\n"
        "[workflow.build]\ncommand = './build.sh'\nartifacts = ['out']\n",
        encoding="utf-8",
    )
    (package / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "build.sh").write_text("#!/bin/sh\nmkdir out\n", encoding="utf-8")
    (package / "build.sh").chmod(0o755)
    job = new_job(source, package)
    context = CLIContext("httk", tmp_path)
    source_name = register_ws(context, source.root, "transfer-source")
    destination_name = register_ws(context, destination.root, "transfer-destination")

    assert command(["transfer", source_name, destination_name, "--job", job.job_id], context) == 0
    output = capsys.readouterr().err
    assert "workflow compiled.transfer.command declares a build" in output
    assert "httk workflow build transfer-destination --store" in output


def test_receive_does_not_remind_for_buildless_runner_trees(tmp_path: Path, capsys) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    runner = tmp_path / "runner"
    runner.mkdir()
    (runner / "httk_workflow.toml").write_text(
        "[workflow]\nid = 'buildless.transfer'\n[workflow.runner]\nsteps = ['start']\n",
        encoding="utf-8",
    )
    (runner / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (runner / "run").chmod(0o755)
    job = new_job(source, runner)
    bundle = source.detach(job.job_id, destination_workspace_id=destination.workspace_id)

    assert (
        transfer_cli.handle_transfer_receive(
            argparse.Namespace(workspace=str(destination.root), bundle=str(bundle)),
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    assert "declares a build" not in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


def test_every_transfer_ledger_write_is_durability_aware() -> None:
    """No write of the transfer protocol may quietly ignore workspace durability."""

    tree = ast.parse(Path(str(transfers_module.__file__)).read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "write_json_atomic"
    ]
    assert calls
    for call in calls:
        assert any(keyword.arg == "durable" for keyword in call.keywords), ast.unparse(call)
