"""Transfer-time environment advisories (legacy until the transfer port, D3; split out of ``test_precheck.py``)."""

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace
from httk.workflow._bundle import _payload_digest
from httk.workflow.models import QUIESCENT_KINDS
from httk.workflow.projects import initialize_project
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload
from httk.workflow.transfers import offer_transfers, select_transfer_jobs
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command


def _job(root: Path, name: str, environment: Mapping[str, object], *, runner: str = "payload") -> Path:
    """Prepare one minimal job payload."""

    payload = root / name
    (payload / "files").mkdir(parents=True)
    run = payload / "files" / "run"
    run.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    run.chmod(0o755)
    prepare_job_payload(
        payload,
        JobSpec(
            name=name,
            workflow="tests.precheck",
            runner_path="files/run" if runner == "payload" else runner,
            environment=environment,
        ),
    )
    return payload


@contextmanager
def _interrupted_after_the_fence() -> Iterator[None]:
    """Interrupt a sealing transaction right after its root is fenced (the job is ``transferring``)."""

    from httk.workflow import _txn

    def interrupt(step: str) -> None:
        if step == "S3.fenced":
            raise RuntimeError("interrupt")

    _txn._HOOK = interrupt
    try:
        yield
    finally:
        _txn._HOOK = None


def test_local_transfer_warns_and_strict_mode_moves_nothing(tmp_path: Path, capsys) -> None:
    """The destination environment is checked before local transfer detach."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    context = CLIContext("httk", tmp_path)
    source_name = register_ws(context, source.root, "source")
    destination_name = register_ws(context, destination.root, "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "transfer-job",
            {"declared": {"missing": {"type": "string"}}, "overrides": {}},
        ),
        "ready",
    )

    assert (
        command(
            ["job", "transfer", "--job", marker.job_id, "--strict-environment", source_name, destination_name],
            context,
        )
        == 2
    )
    capsys.readouterr()
    assert source.find_marker_by_id(marker.job_id) is not None
    assert destination.find_marker_by_id(marker.job_id) is None

    assert command(["job", "transfer", "--job", marker.job_id, source_name, destination_name], context) == 0
    warning = capsys.readouterr().err
    assert "destination environment unresolved" in warning
    assert destination.find_marker_by_id(marker.job_id) is not None


def test_transfer_does_not_use_the_client_environment_for_destination_resolution(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """A client override cannot hide a missing destination setting."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    context = CLIContext("httk", tmp_path)
    source_name = register_ws(context, source.root, "source")
    destination_name = register_ws(context, destination.root, "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "client-environment-job",
            {"declared": {"command": {"type": "string", "setting": "tool.command"}}, "overrides": {}},
        ),
        "ready",
    )
    monkeypatch.setenv("HTTK_TOOL_COMMAND", "client-only")

    assert (
        command(
            ["job", "transfer", "--job", marker.job_id, "--strict-environment", source_name, destination_name],
            context,
        )
        == 2
    )
    assert "destination environment unresolved" in capsys.readouterr().err
    assert destination.find_marker_by_id(marker.job_id) is None


def test_remote_offer_forwards_successful_stderr(tmp_path: Path, capsys, monkeypatch) -> None:
    """Warnings emitted by the far-side offer remain visible to the caller."""

    target = SimpleNamespace(bundle=tmp_path / "adapter")
    monkeypatch.setattr(
        transfer_cli,
        "run_adapter",
        lambda *_args, **_kwargs: {
            "returncode": 0,
            "stdout": json.dumps({"format": "httk-workflow-transfer-offer", "format_version": 2, "offers": []}),
            "stderr": "warning: destination environment unresolved\n",
        },
    )
    assert (
        transfer_cli._remote_offer(
            target,
            "remote",
            "destination",
            states=None,
            placement=None,
            timeout=None,
        )
        == []
    )
    assert "destination environment unresolved" in capsys.readouterr().err


def test_strict_transfer_checks_an_interrupted_marker_before_recovery(tmp_path: Path, monkeypatch) -> None:
    """Strict advisory leaves an interrupted source marker untouched."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "interrupted",
            {"declared": {"missing": {"type": "string"}}, "overrides": {}},
        ),
        "ready",
    )
    with _interrupted_after_the_fence(), pytest.raises(RuntimeError):
        source.detach(
            marker.job_id,
            destination_workspace_id=destination.workspace_id,
            destination_remote="cluster",
        )
    fenced = source.find_marker_by_id(marker.job_id)
    assert fenced is None
    assert [item.job_id for item in source.scan_markers(("transferring",))] == [marker.job_id]

    target = SimpleNamespace(name="cluster", bundle=tmp_path / "adapter")
    monkeypatch.setattr(
        transfer_cli,
        "_remote_workspace_probe",
        lambda *_args, **_kwargs: (destination.workspace_id, str(destination.root)),
    )
    with pytest.raises(ValueError, match="strict environment"):
        transfer_cli._send_jobs_to_remote(
            source,
            target,
            "destination",
            [marker.job_id],
            destination_placement=None,
            timeout=None,
            destination_settings={},
            strict_environment=True,
        )
    assert [item.job_id for item in source.scan_markers(("transferring",))] == [marker.job_id]
    assert not list((source.control / "transfers").glob("*.json"))


def test_other_destination_interrupted_marker_does_not_block_strict_advisory(tmp_path: Path, monkeypatch) -> None:
    """An interrupted transfer for another destination is not selected."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "other-destination",
            {"declared": {"missing": {"type": "string"}}, "overrides": {}},
        ),
        "ready",
    )
    with _interrupted_after_the_fence(), pytest.raises(RuntimeError):
        source.detach(
            marker.job_id,
            destination_workspace_id=destination.workspace_id,
            destination_remote="remote-a",
        )

    candidates = select_transfer_jobs(
        source,
        destination_workspace_id=destination.workspace_id,
        states=(*QUIESCENT_KINDS, "transferring"),
        job_ids=(marker.job_id,),
        destination_remote="remote-b",
        include_transferring=True,
    )
    assert candidates == []
    transfer_cli._environment_advisory(
        source,
        [marker.job_id],
        {},
        strict=True,
        candidates=candidates,
    )


def test_invalid_sealed_job_is_advisory_problem_but_remains_offerable(tmp_path: Path, capsys) -> None:
    """Strict mode blocks an invalid sealed job; non-strict mode reports it."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(_job(tmp_path, "invalid-sealed", {"declared": {}, "overrides": {}}), "ready")
    bundle = source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)
    (bundle / "job.json").write_text("{\"not\": \"a job\"}", encoding="utf-8")
    from httk.workflow import transfers as transfers_module

    manifest_path = bundle / transfers_module.TRANSFER_DIRECTORY / transfers_module.TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["payload_sha256"] = _payload_digest(bundle)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    candidates = select_transfer_jobs(
        source,
        destination_workspace_id=destination.workspace_id,
        states=("submitted",),
    )
    assert candidates and candidates[0].problem
    with pytest.raises(ValueError, match="strict environment"):
        transfer_cli._environment_advisory(
            source,
            [marker.job_id],
            {},
            strict=True,
            candidates=candidates,
        )
    transfer_cli._environment_advisory(
        source,
        [marker.job_id],
        {},
        strict=False,
        candidates=candidates,
    )
    assert "destination environment unresolved" in capsys.readouterr().err
    assert offer_transfers(source, destination_workspace_id=destination.workspace_id, states=("submitted",))


def test_local_transfer_strict_checks_recovering_marker_before_recovery(tmp_path: Path, monkeypatch) -> None:
    """Local-to-local advisory uses the same transferring-marker selector."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(
        _job(tmp_path, "local-interrupted", {"declared": {"missing": {"type": "string"}}, "overrides": {}}),
        "ready",
    )
    with _interrupted_after_the_fence(), pytest.raises(RuntimeError):
        source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)
    with pytest.raises(ValueError, match="strict environment"):
        transfer_cli._transfer_local_to_local(
            source,
            destination,
            [marker.job_id],
            strict_environment=True,
        )
    assert [item.job_id for item in source.scan_markers(("transferring",))] == [marker.job_id]


def test_full_local_to_remote_unreachable_notice_is_printed_once(tmp_path: Path, capsys, monkeypatch) -> None:
    """The command-level unavailable destination path emits one notice."""

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    initialize_project(source_root, name="source")
    initialize_project(destination_root, name="destination")
    source = Workspace.initialize(source_root / "workspace")
    destination = Workspace.initialize(destination_root / "workspace")
    from httk.workflow.adapters import add_remote

    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = str(destination.root)
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload = _job(tmp_path, "notice-job", {"declared": {}, "overrides": {}})
    marker = source.submit(payload, "ready")
    context = CLIContext("httk", source_root)
    register_ws(context, source.root, "home")
    register_ws(context, destination.root, "station")

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("settings unavailable")

    monkeypatch.setattr(transfer_cli, "_remote_workspace_settings", unavailable)
    monkeypatch.setattr(transfer_cli, "_send_jobs_to_remote", lambda *args, **kwargs: [])

    assert command(["job", "transfer", "--job", marker.job_id, "home", "cluster:station"], context) == 0
    assert capsys.readouterr().err.count("could not be prechecked remotely") == 1


def test_unreachable_destination_advisory_is_one_warning(tmp_path: Path, capsys) -> None:
    """The unit seam covers the adapter-unreachable branch without a network."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(
        _job(tmp_path, "unreachable-job", {"declared": {}, "overrides": {}}),
        "ready",
    )

    transfer_cli._environment_advisory(workspace, [marker.job_id], None, strict=False)
    assert capsys.readouterr().err.count("could not be prechecked remotely") == 1
