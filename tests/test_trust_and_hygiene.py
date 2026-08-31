"""The trust model of signed manifests, operator identity, and project hygiene."""

import base64
import bz2
import json
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core.cli import CLIContext
from httk.core.crypto import ed25519_generate_seed, ed25519_public_key, ed25519_sign
from httk.core.identity import (
    ensure_identity_key,
    identity_public_key,
    initialize_identity,
    sign_document,
    verify_document,
)
from httk.core.project.cli import command as project_command
from httk.core.project.manifests import (
    INVALID,
    VALID_TRUSTED,
    VALID_UNKNOWN_KEY,
    create_manifest,
)

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.adapters import add_remote, store_credentials
from httk.workflow.configuration import (
    CONFIG_KEYS,
    config_path,
    read_config,
    set_config_key,
    settable_config_keys,
    unset_config_key,
    write_config,
)
from httk.workflow.errors import FormatError
from httk.workflow.hygiene import describe_remote
from httk.workflow.manifests import verify_manifest
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
    Workspace.initialize(project)
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

    assert project_command(["manifest", "verify", str(project)], CLIContext("httk", project)) == 0
    printed = capsys.readouterr().out
    assert printed.splitlines()[0] == "valid"
    assert VALID_TRUSTED in printed


def test_legacy_manifest_verification_handles_v1_discovery_refusal(tmp_path: Path) -> None:
    project = tmp_path / "legacy"
    legacy = project / "ht.project"
    legacy.mkdir(parents=True)
    seed = ed25519_generate_seed()
    public = base64.b64encode(ed25519_public_key(seed))
    signed = public + b"\n\n"
    manifest = signed + b"\n" + base64.b64encode(ed25519_sign(seed, signed)) + b"\n"
    path = legacy / "manifest.bz2"
    path.write_bytes(bz2.compress(manifest))

    verification = verify_manifest(project)

    assert verification.valid
    assert verification.manifest_format == "legacy"
    assert verification.manifest == path


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
    assert project_command(["manifest", "verify", str(project)], context) == 3
    assert VALID_UNKNOWN_KEY in capsys.readouterr().out

    # Naming the key explicitly is the one-off way to accept it.
    public = project / PROJECT_DIRECTORY / "keys" / "project.pub"
    assert project_command(["manifest", "verify", "--trusted-key", str(public), str(project)], context) == 0
    assert project_command(["manifest", "verify", "--trusted-key", forged_key, str(project)], context) == 0
    assert verify_manifest(project, trusted_keys=[forged_key]).verdict == VALID_TRUSTED


def test_tampered_tree_is_invalid(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch)
    create_manifest(project)
    (project / "content.txt").write_text("tampered\n", encoding="utf-8")

    verification = verify_manifest(project)
    assert verification.verdict == INVALID
    assert not verification.valid and verification.exit_code == 1
    assert "does not match" in verification.reason

    assert project_command(["manifest", "verify", str(project)], CLIContext("httk", project)) == 1
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


def test_workspace_payloads_are_excluded_from_the_project_manifest(tmp_path: Path, monkeypatch) -> None:
    """A workspace member's payloads are excluded; loose project files are covered."""

    project = _project(tmp_path, monkeypatch)  # root-as-workspace, a registered member
    workspace = Workspace(project)
    source = tmp_path / "src" / "job"
    (source / "files").mkdir(parents=True)
    (source / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (source / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": str(uuid.uuid4()),
                "tag": "priv",
                "name": "priv",
                "workflow": "tests.priv",
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
    marker = workspace.submit(source, "jobs")
    payload = workspace.payload_path(marker.placement, marker.job_key)
    (payload / "attempts" / "a").mkdir(parents=True)
    (payload / "attempts" / "a" / "f").write_text("private\n", encoding="utf-8")

    plain_docs = project / "docs"
    plain_docs.mkdir()
    (plain_docs / "notes.txt").write_text("user content\n", encoding="utf-8")

    manifest = create_manifest(project)
    body = "\n".join(bz2.decompress(manifest.read_bytes()).decode("utf-8").splitlines()[1:])
    # The workspace subtree is excluded whole — payload and its private trees alike;
    # it is covered through the workspace seal chain, not the loose manifest records.
    assert marker.job_key not in body
    assert "attempts" not in body
    assert '"path":"docs/notes.txt"' in body
    assert '"path":"content.txt"' in body
    assert verify_manifest(project).verdict == VALID_TRUSTED

    # A runner writing inside its own payload never invalidates the project manifest.
    (payload / "attempts" / "a" / "f").write_text("more private output\n", encoding="utf-8")
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


def _request_workspace(tmp_path: Path) -> tuple[Workspace, str]:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path, pool="unserved")
    workspace.submit(payload, "jobs")
    return workspace, job_id


def _handle_requests(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()


def test_signed_operator_request_round_trips_and_is_attributed(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    initialize_identity("Me", "me@example.org")
    workspace, job_id = _request_workspace(tmp_path)
    ws = register_ws(None, workspace.root)
    assert (
        command(
            [
                "job",
                "request",
                "cancel",
                "--workspace",
                ws,
                job_id,
                "--operator",
                "Me <me@example.org>",
                "--reason",
                "signed request test",
            ],
            CLIContext("httk", tmp_path),
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
    assert state["operator"] == "Me <me@example.org>"
    assert state["operator_key"] == identity_public_key()


def test_unsigned_operator_request_is_still_accepted(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    workspace, job_id = _request_workspace(tmp_path)
    ws = register_ws(None, workspace.root)
    assert identity_public_key() is None
    arguments = [
        "job",
        "request",
        "cancel",
        "--workspace",
        ws,
        job_id,
        "--operator",
        "Nobody <nobody@example.org>",
        "--reason",
        "no key",
    ]
    assert command(arguments, CLIContext("httk", tmp_path)) == 0
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
            "format_version": 2,
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
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
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


def test_read_config_refuses_a_foreign_format_or_version(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(json.dumps({"format": "something-else", "name": "A"}), encoding="utf-8")
    with pytest.raises(ValueError, match="not a httk-config document"):
        read_config()

    path.write_text(json.dumps({"format": "httk-config", "format_version": 99}), encoding="utf-8")
    with pytest.raises(ValueError, match="version 99"):
        read_config()

    # A document with no format or version at all is refused, not read as legacy:
    # the format and format_version are both required.
    path.write_text(json.dumps({"name": "A User"}), encoding="utf-8")
    with pytest.raises(ValueError, match="not a httk-config document"):
        read_config()


def test_config_set_is_restricted_to_the_registry_and_unset_removes(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    assert settable_config_keys() == ("machine_names",)
    assert not CONFIG_KEYS["format"].settable

    set_config_key("machine_names", "node-a")
    assert read_config()["machine_names"] == "node-a"

    with pytest.raises(ValueError, match="unknown configuration key 'mahcine'.*machine_names"):
        set_config_key("mahcine", "node-a")
    with pytest.raises(ValueError, match="cannot be set"):
        set_config_key("format_version", "2")

    unset_config_key("machine_names")
    assert "machine_names" not in read_config()
    with pytest.raises(ValueError, match="not set"):
        unset_config_key("machine_names")

    context = CLIContext("httk", tmp_path)
    assert command(["config", "set", "machine_names", "node-b"], context) == 0
    assert read_config()["machine_names"] == "node-b"
    assert command(["config", "unset", "machine_names"], context) == 0
    assert "machine_names" not in read_config()
    assert command(["config", "set", "nickname", "x"], context) == 2


def test_describe_remote_never_reports_a_credential_value(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="remotes")
    bundle = add_remote("cluster", template="local", project=project)
    store_credentials(bundle, {"password": "hunter2", "token": "abc"})
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["legacy_settings"] = {"workspace_root": str(tmp_path / "remote")}
    (bundle / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")

    description = _fields(describe_remote("cluster", project=project))
    assert description["format"] == "httk-remote-description"
    assert description["scope"] == "project" and description["kind"] == "local"
    assert description["valid"] is True
    assert description["adapter"] == str(bundle / "adapter")
    remote_settings = description
    assert remote_settings["settings"] == {"legacy_settings": {"workspace_root": str(tmp_path / "remote")}}
    assert remote_settings["credential_keys"] == ["password", "token"]
    assert remote_settings["settings_source"] == {
        "legacy_settings": "remote.json",
        "password": "credentials.json",
        "token": "credentials.json",
    }
    assert "hunter2" not in json.dumps(description) and "abc" not in json.dumps(description)


def test_written_configuration_keeps_its_format_members(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    write_config({"name": "A User"})
    stored = read_config()
    assert stored["format"] == "httk-config" and stored["format_version"] == 2


# ---------------------------------------------------------------------------
# The commands that put those descriptions and repairs on the command line
# ---------------------------------------------------------------------------


def test_project_repair_is_reachable_from_the_command_line(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    project = _project(tmp_path, monkeypatch, name="described-cli")
    context = CLIContext("httk", project)

    # A project with no manifest yet is a warning, and a warning is not a failure.
    assert project_command(["repair", "--dry-run"], context) == 0
    report = capsys.readouterr().out
    assert "manifest" in report and "problem(s)" in report

    assert project_command(["repair", "--json", str(project)], context) == 0
    repaired = _fields(json.loads(capsys.readouterr().out))
    assert repaired["format"] == "httk-project-repair" and repaired["apply"] is True


def test_remote_show_and_remove_are_reachable_from_the_command_line(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    project = _project(tmp_path, monkeypatch, name="remotes-cli")
    bundle = add_remote("cluster", template="local", project=project)
    store_credentials(bundle, {"password": "hunter2"})
    context = CLIContext("httk", project)

    assert command(["remote", "show", "--json", "cluster"], context) == 0
    description = _fields(json.loads(capsys.readouterr().out)[0])
    assert description["name"] == "cluster" and description["scope"] == "project"

    assert command(["remote", "show", "cluster"], context) == 0
    rendered = capsys.readouterr().out
    # The name of a credential is reported; its value never is.
    assert "password=<credential>" in rendered and "hunter2" not in rendered

    assert command(["remote", "show", "nowhere"], context) == 1
    assert "unknown remote" in capsys.readouterr().err

    # --force skips the confirmation this non-interactive test cannot answer.
    assert command(["remote", "remove", "cluster"], context) == 1
    assert "requires --force" in capsys.readouterr().err
    assert bundle.is_dir()
    assert command(["remote", "remove", "--force", "cluster"], context) == 0
    assert not bundle.exists()
