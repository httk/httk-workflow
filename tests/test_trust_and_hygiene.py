"""The trust model of signed manifests, operator identity, and project hygiene."""

import base64
import bz2
import json
import os
import time
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pytest  # pyright: ignore[reportMissingImports]
from httk.core import CLIContext, ed25519_generate_seed, ed25519_public_key

from httk.workflow import TaskManager, WorkflowWorkspace
from httk.workflow import cli as native_cli
from httk.workflow.adapters import add_computer, store_credentials
from httk.workflow.configuration import (
    CONFIG_KEYS,
    config_path,
    ensure_identity_key,
    identity_public_key,
    read_config,
    set_config_key,
    settable_config_keys,
    sign_document,
    unset_config_key,
    verify_document,
    write_config,
)
from httk.workflow.errors import FormatError
from httk.workflow.hygiene import (
    describe_computer,
    describe_project,
    project_doctor,
    remove_computer,
)
from httk.workflow.manifests import (
    INVALID,
    VALID_TRUSTED,
    VALID_UNKNOWN_KEY,
    create_manifest,
    verify_manifest,
)
from httk.workflow.projects import (
    PROJECT_DIRECTORY,
    PROJECT_FILE,
    initialize_project,
    key_fingerprint,
    pin_project_key,
    read_project,
    trust_project_key,
)
from httk.workflow.workflow_cli import command


def _fields(value: Mapping[str, object]) -> dict[str, Any]:
    """Read one JSON report in a test without restating every member type."""

    return cast(dict[str, Any], dict(value))


def _by_check(report: Mapping[str, object]) -> dict[str, Any]:
    """Index the findings of one doctor report by the check that made them."""

    return {str(finding["check"]): finding for finding in cast(Sequence[Any], report["findings"])}


def _isolate(tmp_path: Path, monkeypatch) -> None:
    """Keep every test out of the invoking user's real configuration."""

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)


def _project(tmp_path: Path, monkeypatch, name: str = "trust") -> Path:
    _isolate(tmp_path, monkeypatch)
    project = tmp_path / name
    initialize_project(project, name=name)
    (project / "content.txt").write_text("original\n", encoding="utf-8")
    return project


def _rekey(project: Path) -> str:
    """Replace the project's signing key, as an attacker holding the tree would."""

    seed = ed25519_generate_seed()
    keys = project / PROJECT_DIRECTORY / "keys"
    (keys / "project.seed").write_text(base64.b64encode(seed).decode("ascii") + "\n", encoding="ascii")
    (keys / "project.pub").write_text(
        base64.b64encode(ed25519_public_key(seed)).decode("ascii") + "\n",
        encoding="ascii",
    )
    return "ed25519:" + base64.b64encode(ed25519_public_key(seed)).decode("ascii")


def _write_project(project: Path, metadata: dict[str, object]) -> None:
    (project / PROJECT_DIRECTORY / PROJECT_FILE).write_text(json.dumps(metadata), encoding="utf-8")


def _payload(root: Path, *, pool: str = "default") -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / f"payload-{job_id[:8]}"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
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
                "claim": {"pool": pool, "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


# ---------------------------------------------------------------------------
# Task 1: what a verified manifest proves, and against which key
# ---------------------------------------------------------------------------


def test_fresh_project_manifest_is_valid_and_trusted(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch)
    create_manifest(project)
    verification = verify_manifest(project)
    assert verification.verdict == VALID_TRUSTED
    assert verification.valid and verification.trusted and bool(verification)
    assert verification.exit_code == 0
    assert verification.public_key == read_project(project)["public_key"]
    assert key_fingerprint(str(verification.public_key)) in verification.reason

    assert command(["project", "manifest", "verify", str(project)], CLIContext("httk", project)) == 0
    printed = capsys.readouterr().out
    assert printed.splitlines()[0] == "valid"
    assert VALID_TRUSTED in printed


def test_resigning_with_a_fresh_key_is_valid_but_not_trusted(tmp_path: Path, monkeypatch, capsys) -> None:
    """The seed lives in the tree, so a holder of the tree can always re-sign."""

    project = _project(tmp_path, monkeypatch)
    create_manifest(project)
    forged_key = _rekey(project)
    (project / "content.txt").write_text("tampered\n", encoding="utf-8")
    create_manifest(project)

    verification = verify_manifest(project)
    assert verification.verdict == VALID_UNKNOWN_KEY
    assert verification.valid and not verification.trusted and not bool(verification)
    assert verification.exit_code == 3
    assert verification.public_key == forged_key
    assert "not among this project's trusted keys" in verification.reason

    context = CLIContext("httk", project)
    assert command(["project", "manifest", "verify", str(project)], context) == 3
    assert VALID_UNKNOWN_KEY in capsys.readouterr().out

    # Naming the key explicitly is the one-off way to accept it.
    public = project / PROJECT_DIRECTORY / "keys" / "project.pub"
    assert command(["project", "manifest", "verify", str(project), "--trusted-key", str(public)], context) == 0
    assert command(["project", "manifest", "verify", str(project), "--trusted-key", forged_key], context) == 0
    assert verify_manifest(project, trusted_keys=[forged_key]).verdict == VALID_TRUSTED


def test_tampered_tree_is_invalid(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch)
    create_manifest(project)
    (project / "content.txt").write_text("tampered\n", encoding="utf-8")

    verification = verify_manifest(project)
    assert verification.verdict == INVALID
    assert not verification.valid and verification.exit_code == 1
    assert "does not match" in verification.reason

    assert command(["project", "manifest", "verify", str(project)], CLIContext("httk", project)) == 1
    assert capsys.readouterr().out.splitlines()[0] == "invalid"


def test_manifest_of_another_project_is_invalid(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch)
    create_manifest(project)
    metadata = read_project(project)
    metadata["project_id"] = str(uuid.uuid4())
    _write_project(project, metadata)

    verification = verify_manifest(project)
    assert verification.verdict == INVALID
    assert "names project" in verification.reason


def test_unpinned_project_reports_unknown_key_until_it_is_pinned(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch)
    metadata = read_project(project)
    del metadata["public_key"]
    _write_project(project, metadata)
    create_manifest(project)

    unpinned = verify_manifest(project)
    assert unpinned.verdict == VALID_UNKNOWN_KEY
    assert "pins no key" in unpinned.reason and "pin_project_key" in unpinned.reason

    adopted = pin_project_key(project)
    assert str(adopted["public_key"]).startswith("ed25519:")
    assert verify_manifest(project).verdict == VALID_TRUSTED


def test_an_adopted_third_party_key_becomes_a_trust_anchor(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch)
    collaborator = _rekey(project)
    create_manifest(project)
    assert verify_manifest(project).verdict == VALID_UNKNOWN_KEY

    metadata = _fields(trust_project_key(project, collaborator))
    assert collaborator in metadata["trusted_keys"]
    assert verify_manifest(project).verdict == VALID_TRUSTED
    # Adopting the same key twice records it once.
    assert _fields(trust_project_key(project, collaborator))["trusted_keys"] == [collaborator]


def test_root_level_attempt_and_job_entries_stay_out_of_the_manifest(tmp_path: Path, monkeypatch) -> None:
    """`fnmatch` `*` spans `/`, so `**/x` alone never matches a root-level entry."""

    project = _project(tmp_path, monkeypatch)
    attempt = project / ".httk-attempt.x"
    attempt.mkdir()
    (attempt / "stdout.log").write_text("private\n", encoding="utf-8")
    (project / ".httk-job").mkdir()
    (project / ".httk-job" / "state.json").write_text("{}", encoding="utf-8")
    nested = project / "jobs" / "one"
    nested.mkdir(parents=True)
    (nested / ".httk-attempt.y").mkdir()

    manifest = create_manifest(project)
    records = bz2.decompress(manifest.read_bytes()).decode("utf-8").splitlines()[1:]
    body = "\n".join(records)
    assert '"path":".httk-attempt.x"' not in body and '"path":".httk-job"' not in body
    assert ".httk-attempt.y" not in body
    assert '"path":"content.txt"' in body
    assert verify_manifest(project).verdict == VALID_TRUSTED

    # A runner writing inside its own attempt directory never invalidates it.
    (attempt / "stdout.log").write_text("more private output\n", encoding="utf-8")
    assert verify_manifest(project).verdict == VALID_TRUSTED


# ---------------------------------------------------------------------------
# Task 2: the identity key is real
# ---------------------------------------------------------------------------


def test_document_signing_is_optional_and_verifiable(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    document = {"format": "test", "value": 1}

    # No identity key: the document is returned unchanged and reads as unsigned.
    assert identity_public_key() is None
    assert sign_document(document) == document
    absent = verify_document(document)
    assert not absent.present and not absent.valid

    ensure_identity_key()
    signed = sign_document(document)
    assert signed["operator_key"] == identity_public_key()
    checked = verify_document(signed)
    assert checked.present and checked.valid and checked.operator_key == identity_public_key()

    forged = {**signed, "value": 2}
    assert verify_document(forged).present and not verify_document(forged).valid
    truncated = {key: value for key, value in signed.items() if key != "operator_key"}
    assert not verify_document(truncated).valid


def _request_workspace(tmp_path: Path) -> tuple[WorkflowWorkspace, str]:
    workspace = WorkflowWorkspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path, pool="unserved")
    workspace.submit(payload, "jobs")
    return workspace, job_id


def _handle_requests(workspace: WorkflowWorkspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()


def test_signed_operator_request_round_trips_and_is_attributed(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    ensure_identity_key()
    workspace, job_id = _request_workspace(tmp_path)
    assert (
        native_cli.main(
            [
                "request",
                str(workspace.root),
                job_id,
                "cancel",
                "--operator",
                "a-user",
                "--reason",
                "signed request test",
            ]
        )
        == 0
    )
    published = next((workspace.control / "requests" / "ready").iterdir())
    document = json.loads(published.read_text(encoding="utf-8"))
    assert document["operator_key"] == identity_public_key()
    assert verify_document(document).valid

    _handle_requests(workspace)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "cancelled"
    state = workspace.read_state(marker)
    assert state["operator"] == "a-user"
    assert state["operator_key"] == identity_public_key()


def test_unsigned_operator_request_is_still_accepted(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    workspace, job_id = _request_workspace(tmp_path)
    assert identity_public_key() is None
    arguments = ["request", str(workspace.root), job_id, "cancel", "--operator", "nobody", "--reason", "no key"]
    assert native_cli.main(arguments) == 0
    document = json.loads(next((workspace.control / "requests" / "ready").iterdir()).read_text(encoding="utf-8"))
    assert "signature" not in document and "operator_key" not in document

    _handle_requests(workspace)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "cancelled"
    assert "operator_key" not in workspace.read_state(marker)


def test_forged_operator_request_is_quarantined(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    ensure_identity_key()
    workspace, job_id = _request_workspace(tmp_path)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    request = sign_document(
        {
            "format": "httk-workflow-request",
            "format_version": 1,
            "request_id": str(uuid.uuid4()),
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "expected_generation": marker.generation,
            "expected_record_ref": marker.record_ref,
            "action": "cancel",
            "operator": "an-impostor",
            "reason": "forged",
        }
    )
    # Everything about the request is honest except who is claimed to have said it.
    workspace.publish_request({**request, "operator": "somebody-else"})

    _handle_requests(workspace)
    unchanged = workspace.find_marker_by_id(job_id)
    # The request never applied: the job is where the same tick's ordinary
    # submission pass left it, not cancelled.
    assert unchanged is not None and unchanged.kind in {"submitted", "ready"}
    quarantined = list((workspace.control / "quarantine").iterdir())
    assert len(quarantined) == 1
    report = json.loads((quarantined[0] / "report.json").read_text(encoding="utf-8"))
    assert "signature" in report["reason"]


def test_transfer_acknowledgement_is_signed_and_a_forged_one_is_refused(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    ensure_identity_key()
    source = WorkflowWorkspace.initialize(tmp_path / "source", extensions=["detached-transfer-v1"])
    destination = WorkflowWorkspace.initialize(tmp_path / "destination", extensions=["detached-transfer-v1"])
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)

    acknowledgement = destination.import_bundle(bundle)
    assert acknowledgement["operator_key"] == identity_public_key()
    assert verify_document(acknowledgement).valid

    forged = {**acknowledgement, "signature": base64.b64encode(b"\0" * 64).decode("ascii")}
    with pytest.raises(FormatError, match="signature is invalid"):
        source.acknowledge_transfer(forged)

    retired = source.acknowledge_transfer(acknowledgement)
    assert retired.is_dir()


# ---------------------------------------------------------------------------
# Task 3: configuration hygiene
# ---------------------------------------------------------------------------


def test_read_config_refuses_a_foreign_format_and_accepts_a_legacy_version(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(json.dumps({"format": "something-else", "name": "A"}), encoding="utf-8")
    with pytest.raises(ValueError, match="not a httk-config document"):
        read_config()

    path.write_text(json.dumps({"format": "httk-config", "format_version": 99}), encoding="utf-8")
    with pytest.raises(ValueError, match="version 99"):
        read_config()

    # No version at all is what every configuration written before versioning
    # looks like, and it is read rather than refused.
    path.write_text(json.dumps({"name": "A User"}), encoding="utf-8")
    assert read_config() == {"name": "A User"}


def test_config_set_is_restricted_to_the_registry_and_unset_removes(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    assert settable_config_keys() == ("email", "name")
    assert not CONFIG_KEYS["format"].settable

    set_config_key("name", "A User")
    set_config_key("email", "a@example.test")
    assert read_config()["name"] == "A User"

    with pytest.raises(ValueError, match="unknown configuration key 'nmae'.*email, name"):
        set_config_key("nmae", "A User")
    with pytest.raises(ValueError, match="cannot be set"):
        set_config_key("format_version", "2")

    unset_config_key("email")
    assert "email" not in read_config()
    with pytest.raises(ValueError, match="not set"):
        unset_config_key("email")

    context = CLIContext("httk", tmp_path)
    assert command(["config", "set", "email", "b@example.test"], context) == 0
    assert read_config()["email"] == "b@example.test"
    assert command(["config", "unset", "email"], context) == 0
    assert "email" not in read_config()
    assert command(["config", "set", "nickname", "x"], context) == 2


# ---------------------------------------------------------------------------
# Task 3: describing and repairing
# ---------------------------------------------------------------------------


def test_describe_project_reports_keys_workspace_and_manifest(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="described")
    add_computer("local", template="local", project=project)
    create_manifest(project)

    description = _fields(describe_project(project))
    assert description["format"] == "httk-project-description"
    assert description["project"]["name"] == "described"
    assert description["keys"]["pinned"] is True
    assert description["keys"]["public_key"]["fingerprint"].startswith("sha256:")
    assert description["keys"]["seed_present"] is True
    assert description["workspace"]["present"] is True
    assert "detached-transfer-v1" in description["workspace"]["extensions"]
    assert description["workspace"]["counts"] == {} and description["workspace"]["jobs"] == 0
    assert description["manifest"]["verdict"] == VALID_TRUSTED
    assert description["computers"] == ["local"]

    cheap = _fields(describe_project(project, verify=False))
    assert cheap["manifest"]["present"] is True and cheap["manifest"]["verdict"] is None


def test_describe_computer_never_reports_a_credential_value(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="computers")
    bundle = add_computer("cluster", template="local", project=project)
    store_credentials(bundle, "default", {"password": "hunter2", "token": "abc"})
    metadata = json.loads((bundle / "computer.json").read_text(encoding="utf-8"))
    metadata["queues"]["default"]["workspace"] = str(tmp_path / "remote")
    (bundle / "computer.json").write_text(json.dumps(metadata), encoding="utf-8")

    description = _fields(describe_computer("cluster", project=project))
    assert description["format"] == "httk-computer-description"
    assert description["scope"] == "project" and description["kind"] == "local"
    assert description["valid"] is True
    assert set(description["operations"]) >= {"configure", "install", "invoke", "status"}
    queue = description["queues"]["default"]
    assert queue["settings"] == {"workspace": str(tmp_path / "remote")}
    assert queue["credential_keys"] == ["password", "token"]
    assert queue["settings_source"] == {
        "workspace": "computer.json",
        "password": "credentials.json",
        "token": "credentials.json",
    }
    assert "hunter2" not in json.dumps(description) and "abc" not in json.dumps(description)


def test_remove_computer_refuses_while_a_transfer_still_needs_it(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="removal")
    destination = WorkflowWorkspace.initialize(tmp_path / "remote", extensions=["detached-transfer-v1"])
    bundle = add_computer("cluster", template="local", project=project)
    metadata = json.loads((bundle / "computer.json").read_text(encoding="utf-8"))
    metadata["queues"]["default"]["workspace"] = str(destination.root)
    (bundle / "computer.json").write_text(json.dumps(metadata), encoding="utf-8")

    workspace = WorkflowWorkspace(project)
    payload, job_id = _payload(tmp_path)
    workspace.submit(payload, "jobs")
    sealed = workspace.detach(job_id, destination_workspace_id=destination.workspace_id)

    with pytest.raises(ValueError, match="unretired transfer"):
        remove_computer("cluster", project=project)
    assert bundle.is_dir()

    workspace.acknowledge_transfer(destination.import_bundle(sealed))
    assert remove_computer("cluster", project=project)["removed"] is True
    assert not bundle.exists()
    with pytest.raises(ValueError, match="unknown computer"):
        remove_computer("cluster", project=project)


def test_project_doctor_reports_then_repairs(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="doctored")
    metadata = read_project(project)
    metadata["workspace_initialization_failed"] = True
    del metadata["public_key"]
    _write_project(project, metadata)
    lock = project / ".httk-workflow" / "maintenance.lock"
    lock.write_text(json.dumps({"pid": 1, "hostname": "gone.example.test", "created": "2000-01-01T00:00:00.000000Z"}))
    leftover = project / ".httk-workflow" / "tmp" / "import.abandoned"
    leftover.mkdir(parents=True, exist_ok=True)
    old = time.time() - 3 * 24 * 60 * 60
    os.utime(leftover, (old, old))

    report = _fields(project_doctor(project))
    findings = _by_check(report)
    assert report["format"] == "httk-project-doctor" and report["repaired"] == 0
    assert findings["workspace_initialization"]["status"] == "error"
    assert findings["maintenance_lock"]["status"] == "error"
    assert findings["key_pin"]["status"] == "warning"
    assert findings["tmp_leftovers"]["status"] == "warning"
    assert findings["manifest"]["status"] == "warning"
    assert report["problems"] == 5
    assert lock.is_file() and leftover.is_dir()

    repaired = _fields(project_doctor(project, repair=True))
    fixed = _by_check(repaired)
    assert repaired["repaired"] == 4
    assert all(fixed[name]["repaired"] for name in ("workspace_initialization", "maintenance_lock", "key_pin"))
    assert not lock.exists() and not leftover.exists()
    assert "workspace_initialization_failed" not in read_project(project)
    assert str(read_project(project)["public_key"]).startswith("ed25519:")
    # The manifest check is reported and never repaired behind an operator.
    assert fixed["manifest"]["repaired"] is False

    create_manifest(project)
    final = _by_check(project_doctor(project))
    assert final["manifest"]["status"] == "ok"
    assert project_doctor(project)["problems"] == 0


def test_project_doctor_journals_what_it_repaired(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="journalled")
    metadata = read_project(project)
    del metadata["public_key"]
    _write_project(project, metadata)

    journal = project / ".httk-workflow" / "journal"
    assert project_doctor(project)["repaired"] == 0
    # A read-only check opens no writer, so it creates no writer directory.
    assert not list(journal.glob("*/*.hwj"))

    assert project_doctor(project, repair=True)["repaired"] == 1
    segments = list(journal.glob("*/*.hwj"))
    assert len(segments) == 1
    assert b'"httk-workflow-doctor"' in segments[0].read_bytes()
    assert b'"key_pin"' in segments[0].read_bytes()


def test_legacy_identity_import_is_pinned_and_reported(tmp_path: Path, monkeypatch) -> None:
    from httk.workflow.projects import import_v1_project

    _isolate(tmp_path, monkeypatch)
    project = tmp_path / "legacy"
    legacy = project / "ht.project"
    (legacy / "keys").mkdir(parents=True)
    seed = ed25519_generate_seed()
    recorded = "ed25519:" + base64.b64encode(ed25519_public_key(seed)).decode("ascii")
    (legacy / "keys" / "key1.pub").write_text(
        base64.b64encode(ed25519_public_key(seed)).decode("ascii") + "\n",
        encoding="ascii",
    )
    (legacy / "config").write_text("[main]\nproject_name = legacy\n", encoding="utf-8")

    metadata = _fields(import_v1_project(project))
    assert metadata["trusted_keys"] == [recorded]
    described = _fields(describe_project(project, verify=False))
    assert [key["public_key"] for key in described["keys"]["trusted_keys"]][1:] == [recorded]

    findings = _by_check(project_doctor(project))
    assert findings["legacy_identity"]["status"] == "ok"

    # A legacy key that was copied but never adopted is reported, not adopted.
    metadata["trusted_keys"] = []
    _write_project(project, metadata)
    findings = _by_check(project_doctor(project))
    assert findings["legacy_identity"]["status"] == "warning"
    assert findings["legacy_identity"]["details"]["keys"] == ["key1.pub"]


def test_written_configuration_keeps_its_format_members(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    write_config({"name": "A User"})
    stored = read_config()
    assert stored["format"] == "httk-config" and stored["format_version"] == 1


# ---------------------------------------------------------------------------
# The commands that put those descriptions and repairs on the command line
# ---------------------------------------------------------------------------


def test_project_show_and_doctor_are_reachable_from_the_command_line(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    project = _project(tmp_path, monkeypatch, name="described-cli")
    context = CLIContext("httk", project)

    assert command(["project", "show", "--json"], context) == 0
    description = _fields(json.loads(capsys.readouterr().out))
    assert description["format"] == "httk-project-description"
    assert description["project"]["name"] == "described-cli"

    assert command(["project", "show"], context) == 0
    rendered = capsys.readouterr().out
    assert "described-cli" in rendered and "key_pinned" in rendered

    # A project with no manifest yet is a warning, and a warning is not a failure.
    assert command(["project", "doctor"], context) == 0
    report = capsys.readouterr().out
    assert "manifest" in report and "problem(s)" in report

    assert command(["project", "doctor", "--repair", "--json"], context) == 0
    repaired = _fields(json.loads(capsys.readouterr().out))
    assert repaired["format"] == "httk-project-doctor" and repaired["repair"] is True


def test_computer_show_and_remove_are_reachable_from_the_command_line(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    project = _project(tmp_path, monkeypatch, name="computers-cli")
    bundle = add_computer("cluster", template="local", project=project)
    store_credentials(bundle, "default", {"password": "hunter2"})
    context = CLIContext("httk", project)

    assert command(["computer", "show", "cluster", "--json"], context) == 0
    description = _fields(json.loads(capsys.readouterr().out))
    assert description["name"] == "cluster" and description["scope"] == "project"

    assert command(["computer", "show", "cluster"], context) == 0
    rendered = capsys.readouterr().out
    # The name of a credential is reported; its value never is.
    assert "password=<credential>" in rendered and "hunter2" not in rendered

    assert command(["computer", "show", "nowhere"], context) == 2
    assert "unknown computer" in capsys.readouterr().err

    # --force skips the confirmation this non-interactive test cannot answer.
    assert command(["computer", "remove", "cluster"], context) == 2
    assert "requires --force" in capsys.readouterr().err
    assert bundle.is_dir()
    assert command(["computer", "remove", "cluster", "--force"], context) == 0
    assert not bundle.exists()


def test_computer_remove_force_does_not_skip_the_transfer_refusal(tmp_path: Path, monkeypatch, capsys) -> None:
    """``--force`` answers the prompt; it does not overrule the refusal."""

    project = _project(tmp_path, monkeypatch, name="forced-removal")
    destination = WorkflowWorkspace.initialize(tmp_path / "remote", extensions=["detached-transfer-v1"])
    bundle = add_computer("cluster", template="local", project=project)
    metadata = json.loads((bundle / "computer.json").read_text(encoding="utf-8"))
    metadata["queues"]["default"]["workspace"] = str(destination.root)
    (bundle / "computer.json").write_text(json.dumps(metadata), encoding="utf-8")
    workspace = WorkflowWorkspace(project)
    payload, job_id = _payload(tmp_path)
    workspace.submit(payload, "jobs")
    workspace.detach(job_id, destination_workspace_id=destination.workspace_id)

    context = CLIContext("httk", project)
    assert command(["computer", "remove", "cluster", "--force"], context) == 2
    assert "unretired transfer" in capsys.readouterr().err
    assert bundle.is_dir()
