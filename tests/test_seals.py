"""The sealing feature: writing, signing, reading, and verifying seal documents."""

import base64
import hashlib
import json
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from httk.core.crypto import ed25519_generate_seed
from httk.core.identity import ensure_identity_key, identity_public_key

from httk.workflow import Workspace
from httk.workflow.errors import SealedError, SealError
from httk.workflow.manifests import _records
from httk.workflow.models import Marker
from httk.workflow.projects import initialize_project, key_fingerprint, read_project, trusted_project_keys
from httk.workflow.seals import (
    INVALID,
    VALID_TRUSTED,
    VALID_UNKNOWN_KEY,
    Seal,
    is_job_sealed,
    is_project_sealed,
    is_workspace_sealed,
    job_seal_path,
    read_seal,
    resolve_seal_keys,
    seal_job,
    seal_project,
    seal_workspace,
    unseal_job,
    unseal_project,
    unseal_workspace,
    unsealed_jobs,
    verify_job_seal,
    verify_project_seal,
    verify_seal,
    verify_tree,
    verify_workspace_seal,
    workspace_seal_path,
)

_DOMAIN = b"httk-seal-v1\0"


def _payload(root: Path, name: str) -> Path:
    """Write one minimal, valid job payload whose directory is *name*."""

    job_id = str(uuid.uuid4())
    payload = root / name
    (payload / "files").mkdir(parents=True)
    (payload / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": "seal",
                "name": "seal test",
                "workflow": "tests.seal",
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
    return payload


@dataclass
class Env:
    """One project with a nested workspace and its submitted job markers."""

    project: Path
    workspace: Workspace
    markers: list[Marker]


def _setup(tmp_path: Path, *, identity: bool = True, jobs: int = 1) -> Env:
    """Build a project, a nested workspace, a loose project file, and *jobs* jobs."""

    if identity:
        ensure_identity_key()
    project = tmp_path / "project"
    initialize_project(project, name="sealed")
    (project / "content.txt").write_text("loose project file\n", encoding="utf-8")
    workspace = Workspace.initialize(project / "work")
    source = tmp_path / "source"
    markers = [workspace.submit(_payload(source, f"job{index}"), "jobs") for index in range(jobs)]
    return Env(project, workspace, markers)


def _project_trust(project: Path) -> tuple[str, ...]:
    return trusted_project_keys(read_project(project))


# -- digest and signature ----------------------------------------------------


def test_body_digest_is_deterministic_and_self_describing(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    payload = env.workspace.payload_path(marker.placement, marker.job_key)
    assert list(_records(payload, ())) == list(_records(payload, ()))
    seal_job(env.workspace, marker)
    seal = read_seal(job_seal_path(env.workspace, marker.job_key))
    assert hashlib.sha256(_DOMAIN + seal.body_bytes).hexdigest() == seal.body_sha256
    assert isinstance(seal, Seal) and seal.kind == "job"


def test_two_key_seal_is_trusted_by_project_key_alone(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    path = seal_job(env.workspace, marker)
    seal = read_seal(path)
    assert {str(signature["role"]) for signature in seal.signatures} == {"project", "identity"}
    verification = verify_seal(path, trusted_keys=_project_trust(env.project))
    assert verification.verdict == VALID_TRUSTED and verification.valid
    assert len(verification.signers) == 2


def test_unknown_key_verifies_but_is_untrusted(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = seal_job(env.workspace, env.markers[0])
    verification = verify_seal(path)
    assert verification.verdict == VALID_UNKNOWN_KEY and verification.valid


def test_tampered_body_is_invalid(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = seal_job(env.workspace, env.markers[0])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["created_at"] = "1999-01-01T00:00:00.000000Z"
    path.write_text(json.dumps(document), encoding="utf-8")
    verification = verify_seal(path, trusted_keys=_project_trust(env.project))
    assert verification.verdict == INVALID and not verification.valid


def test_tampered_signature_is_invalid(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = seal_job(env.workspace, env.markers[0])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["signatures"][0]["signature"] = "AAAA"
    path.write_text(json.dumps(document), encoding="utf-8")
    assert verify_seal(path).verdict == INVALID


# -- key resolution ----------------------------------------------------------


def test_missing_identity_key_leaves_project_only_signature(tmp_path: Path) -> None:
    env = _setup(tmp_path, identity=False)
    keys = resolve_seal_keys(["project", "identity"], root=env.workspace.root)
    assert keys.missing_roles == ("identity",)
    assert [role for role, _seed in keys.keys] == ["project"]
    path = seal_job(env.workspace, env.markers[0], keys=keys)
    verification = verify_seal(path, trusted_keys=_project_trust(env.project), expected_roles=("project", "identity"))
    assert verification.missing_signers == ("identity",)
    assert verification.verdict == VALID_TRUSTED


def test_no_available_key_is_an_error(tmp_path: Path) -> None:
    env = _setup(tmp_path, identity=False)
    with pytest.raises(SealError):
        resolve_seal_keys(["identity"], root=env.workspace.root)
    with pytest.raises(SealError):
        resolve_seal_keys([], root=env.workspace.root)


def test_seal_key_file_ref(tmp_path: Path) -> None:
    env = _setup(tmp_path, identity=False)
    seed_file = tmp_path / "extra.seed"
    seed_file.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")
    keys = resolve_seal_keys([str(seed_file)], root=env.workspace.root)
    assert [role for role, _seed in keys.keys] == ["file"]


# -- idempotency and conflict ------------------------------------------------


def test_reseal_is_idempotent_but_conflict_is_refused(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    path = seal_job(env.workspace, marker)
    assert seal_job(env.workspace, marker) == path
    (env.workspace.payload_path(marker.placement, marker.job_key) / "extra.txt").write_text("new", encoding="utf-8")
    with pytest.raises(SealedError):
        seal_job(env.workspace, marker)


# -- record checks -----------------------------------------------------------


def test_job_seal_detects_mismatch_extra_and_missing(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    seal_job(env.workspace, marker)
    payload = env.workspace.payload_path(marker.placement, marker.job_key)
    (payload / "files" / "runner").write_text("changed\n", encoding="utf-8")
    (payload / "added.txt").write_text("added\n", encoding="utf-8")
    kinds = {
        (discrepancy.path, discrepancy.kind) for discrepancy in verify_job_seal(env.workspace, marker).discrepancies
    }
    assert ("files/runner", "mismatch") in kinds
    assert ("added.txt", "extra") in kinds

    other = _setup(tmp_path / "second")
    seal_job(other.workspace, other.markers[0])
    op = other.workspace.payload_path(other.markers[0].placement, other.markers[0].job_key)
    (op / "files" / "runner").unlink()
    missing = {d.kind for d in verify_job_seal(other.workspace, other.markers[0]).discrepancies}
    assert "missing" in missing


def test_job_seal_detects_an_executable_bit_flip(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    seal_job(env.workspace, marker)
    runner = env.workspace.payload_path(marker.placement, marker.job_key) / "files" / "runner"
    runner.chmod(runner.stat().st_mode | 0o100)
    kinds = {(d.path, d.kind) for d in verify_job_seal(env.workspace, marker).discrepancies}
    assert ("files/runner", "mismatch") in kinds


# -- workspace and project seals ---------------------------------------------


def test_workspace_seal_requires_every_job_sealed(tmp_path: Path) -> None:
    env = _setup(tmp_path, jobs=2)
    seal_job(env.workspace, env.markers[0])
    assert {marker.job_key for marker in unsealed_jobs(env.workspace)} == {env.markers[1].job_key}
    with pytest.raises(SealError):
        seal_workspace(env.workspace)
    seal_job(env.workspace, env.markers[1])
    path = seal_workspace(env.workspace)
    assert is_workspace_sealed(env.workspace)
    verification = verify_workspace_seal(env.workspace, trusted_keys=_project_trust(env.project))
    assert verification.valid and not verification.discrepancies
    assert len(read_seal(path).records) == 2


def test_project_seal_covers_loose_files_and_workspace_but_not_payloads(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    seal_job(env.workspace, env.markers[0])
    seal_workspace(env.workspace)
    path = seal_project(env.project)
    records = read_seal(path).records
    paths = {str(record["path"]) for record in records if "type" in record}
    assert "content.txt" in paths
    assert not any(name.startswith("work/") for name in paths)
    workspace_records = [record for record in records if "workspace" in record]
    assert len(workspace_records) == 1
    assert workspace_records[0]["workspace"] == "work"
    expected = hashlib.sha256(workspace_seal_path(env.workspace).read_bytes()).hexdigest()
    assert workspace_records[0]["seal_sha256"] == expected
    assert verify_project_seal(env.project, trusted_keys=_project_trust(env.project)).valid


# -- whole-tree verification -------------------------------------------------


def test_verify_tree_is_ok_and_pinpoints_a_tampered_job_file(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    seal_job(env.workspace, marker)
    seal_workspace(env.workspace)
    seal_project(env.project)
    trust = _project_trust(env.project)
    report = verify_tree(env.project, trusted_keys=trust, deep=True)
    assert report.ok
    assert [level for level, _subject, _v in report.entries] == ["project", "workspace", "job"]
    assert all(verification.verdict == VALID_TRUSTED for _l, _s, verification in report.entries)

    (env.workspace.payload_path(marker.placement, marker.job_key) / "files" / "runner").write_text(
        "evil\n", encoding="utf-8"
    )
    after = verify_tree(env.project, trusted_keys=trust, deep=True)
    assert not after.ok
    faults = {level: verification for level, _subject, verification in after.entries}
    assert faults["project"].valid and faults["workspace"].valid
    assert not faults["job"].valid
    assert [(d.path, d.kind) for d in faults["job"].discrepancies] == [("files/runner", "mismatch")]


def test_verify_tree_from_a_job_payload(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    seal_job(env.workspace, marker)
    payload = env.workspace.payload_path(marker.placement, marker.job_key)
    report = verify_tree(payload, trusted_keys=_project_trust(env.project))
    assert report.ok
    assert [level for level, _s, _v in report.entries] == ["job"]


# -- unseal ordering ---------------------------------------------------------


def test_unseal_refuses_out_of_order(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    marker = env.markers[0]
    seal_job(env.workspace, marker)
    seal_workspace(env.workspace)
    seal_project(env.project)
    with pytest.raises(SealedError):
        unseal_job(env.workspace, marker)
    with pytest.raises(SealedError):
        unseal_workspace(env.workspace)
    unseal_project(env.project)
    assert not is_project_sealed(env.project)
    unseal_workspace(env.workspace)
    assert not is_workspace_sealed(env.workspace)
    unseal_job(env.workspace, marker)
    assert not is_job_sealed(env.workspace, marker.job_key)


def test_identity_public_key_is_a_signer(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = seal_job(env.workspace, env.markers[0])
    identity = identity_public_key()
    assert identity is not None
    verification = verify_seal(path, trusted_keys=(identity,))
    assert key_fingerprint(identity) in verification.signers
    assert verification.verdict == VALID_TRUSTED


def _setup_root_workspace(tmp_path: Path, *, jobs: int = 1) -> Env:
    """Build the single-directory layout: the project root is itself a workspace."""

    ensure_identity_key()
    project = tmp_path / "project"
    initialize_project(project, name="sealed-root")
    (project / "content.txt").write_text("loose project file\n", encoding="utf-8")
    workspace = Workspace.initialize(project)
    source = tmp_path / "source"
    markers = [workspace.submit(_payload(source, f"job{index}"), "jobs") for index in range(jobs)]
    return Env(project, workspace, markers)


def test_project_seal_of_root_workspace_layout(tmp_path: Path) -> None:
    env = _setup_root_workspace(tmp_path)
    marker = env.markers[0]
    seal_job(env.workspace, marker)
    seal_workspace(env.workspace)
    path = seal_project(env.project)
    records = read_seal(path).records
    paths = {str(record["path"]) for record in records if "type" in record}
    assert "content.txt" in paths
    payload_rel = env.workspace.payload_path(marker.placement, marker.job_key).relative_to(env.project).as_posix()
    assert not any(name == payload_rel or name.startswith(f"{payload_rel}/") for name in paths)
    workspace_records = [record for record in records if "workspace" in record]
    assert [record["workspace"] for record in workspace_records] == ["."]
    expected = hashlib.sha256(workspace_seal_path(env.workspace).read_bytes()).hexdigest()
    assert workspace_records[0]["seal_sha256"] == expected

    trust = _project_trust(env.project)
    report = verify_tree(env.project, trusted_keys=trust, deep=True)
    assert report.ok
    assert [level for level, _s, _v in report.entries] == ["project", "workspace", "job"]

    (env.workspace.payload_path(marker.placement, marker.job_key) / "files" / "runner").write_text(
        "evil\n", encoding="utf-8"
    )
    after = verify_tree(env.project, trusted_keys=trust, deep=True)
    assert not after.ok
    faults = {level: verification for level, _s, verification in after.entries}
    assert faults["project"].valid and faults["workspace"].valid
    assert [(d.path, d.kind) for d in faults["job"].discrepancies] == [("files/runner", "mismatch")]
