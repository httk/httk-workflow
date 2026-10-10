"""The sealing feature: writing, signing, reading, and verifying seal documents."""

import base64
import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest
from httk.core.crypto import ed25519_generate_seed
from httk.core.identity import identity_public_key
from httk.core.project.sealing import seal_project, unseal_project, verify_project

from conftest import configure_identity
from httk.workflow import Workspace, _kernel
from httk.workflow._state import Release, StateDoc
from httk.workflow.errors import SealedError, SealError
from httk.workflow.manifests import payload_file_records
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
    require_cli_modifiable,
    resolve_seal_keys,
    seal_job,
    seal_workspace,
    unseal_job,
    unseal_workspace,
    unsealed_jobs,
    verify_job_seal,
    verify_seal,
    verify_tree,
    verify_workspace_seal,
    workspace_seal_path,
)
from v3_helpers import cli_owner, find, state_of, submit, workspace

_DOMAIN = b"httk-seal-v1\0"
_RUNNER = "#!/bin/sh\nexit 0\n"


def _succeeded(ws: Workspace, placement: str) -> _kernel.JobRef:
    """Submit one job with a ``files/runner`` member and move it to ``succeeded`` without a seal."""

    ref = submit(
        ws,
        ("demo--0123456789abcdef", "demo"),
        {"start": "succeed"},
        tag="seal",
        placement=placement,
        members={"files/runner": _RUNNER},
    )
    with cli_owner(ws) as owner:
        owned = _kernel.claim(ws, owner, ref)
        assert owned is not None
        doc = StateDoc.empty(owned.job_id).next_activation("start", "initial")
        return owned.release(doc, Release("succeeded", 500))


@dataclass
class Env:
    """One project with a nested workspace and its succeeded, unsealed jobs."""

    project: Path
    workspace: Workspace
    jobs: list[_kernel.JobRef]

    def seal(self, index: int = 0) -> Path:
        """Seal one job through its ``seal`` request and return its payload."""

        assert seal_job(self.workspace, self.jobs[index]) is None
        return self.payload(index)

    def payload(self, index: int = 0) -> Path:
        return find(self.workspace, self.jobs[index].job_id).path


def _setup(tmp_path: Path, *, identity: bool = True, jobs: int = 1, member: str = "work") -> Env:
    """Build a project, a member workspace, a loose project file, and *jobs* succeeded jobs."""

    if identity:
        configure_identity()
    project = tmp_path / "project"
    initialize_project(project, name="sealed")
    (project / "content.txt").write_text("loose project file\n", encoding="utf-8")
    ws = workspace(project / member)
    return Env(project, ws, [_succeeded(ws, f"jobs/{index}") for index in range(jobs)])


def _project_trust(project: Path) -> tuple[str, ...]:
    return trusted_project_keys(read_project(project))


# -- digest and signature ----------------------------------------------------


def test_body_digest_is_deterministic_and_self_describing(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.payload()
    assert payload_file_records(payload) == payload_file_records(payload)
    seal = read_seal(job_seal_path(env.seal()))
    assert hashlib.sha256(_DOMAIN + seal.body_bytes).hexdigest() == seal.body_sha256
    assert isinstance(seal, Seal) and seal.kind == "job"


def test_seal_request_records_the_digest_and_excludes_owner_documents(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.seal()
    doc = state_of(find(env.workspace, env.jobs[0].job_id))
    assert doc.seal is not None
    assert doc.seal["sha256"] == hashlib.sha256(job_seal_path(payload).read_bytes()).hexdigest()
    paths = {str(record["path"]) for record in read_seal(job_seal_path(payload)).records}
    assert "files/runner" in paths and "job.json" in paths
    assert not paths & {"state.json", "seal.json"} and not any(
        path.startswith(("logs/", ".httk-job/")) for path in paths
    )


def test_two_key_seal_is_trusted_by_project_key_alone(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = job_seal_path(env.seal())
    seal = read_seal(path)
    assert {str(signature["role"]) for signature in seal.signatures} == {"project", "identity"}
    verification = verify_seal(path, trusted_keys=_project_trust(env.project))
    assert verification.verdict == VALID_TRUSTED and verification.valid
    assert len(verification.signers) == 2


def test_unknown_key_verifies_but_is_untrusted(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    verification = verify_seal(job_seal_path(env.seal()))
    assert verification.verdict == VALID_UNKNOWN_KEY and verification.valid


def test_tampered_body_is_invalid(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = job_seal_path(env.seal())
    document = json.loads(path.read_text(encoding="utf-8"))
    document["created_at"] = "1999-01-01T00:00:00.000000Z"
    path.write_text(json.dumps(document), encoding="utf-8")
    verification = verify_seal(path, trusted_keys=_project_trust(env.project))
    assert verification.verdict == INVALID and not verification.valid


def test_tampered_signature_is_invalid(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = job_seal_path(env.seal())
    document = json.loads(path.read_text(encoding="utf-8"))
    document["signatures"][0]["signature"] = "AAAA"
    path.write_text(json.dumps(document), encoding="utf-8")
    assert verify_seal(path).verdict == INVALID


# -- key resolution ----------------------------------------------------------


def test_missing_identity_key_leaves_project_only_signature(tmp_path: Path) -> None:
    env = _setup(tmp_path, identity=False)
    keys = resolve_seal_keys(["project", "identity"], project_root=env.workspace.root)
    assert keys.missing_roles == ("identity",)
    assert [role for role, _seed in keys.keys] == ["project"]
    path = job_seal_path(env.seal())
    verification = verify_seal(path, trusted_keys=_project_trust(env.project), expected_roles=("project", "identity"))
    assert verification.missing_signers == ("identity",)
    assert verification.verdict == VALID_TRUSTED


def test_no_available_key_is_an_error(tmp_path: Path) -> None:
    env = _setup(tmp_path, identity=False)
    with pytest.raises(SealError):
        resolve_seal_keys(["identity"], project_root=env.workspace.root)
    with pytest.raises(SealError):
        resolve_seal_keys([], project_root=env.workspace.root)


def test_seal_key_file_ref(tmp_path: Path) -> None:
    env = _setup(tmp_path, identity=False)
    seed_file = tmp_path / "extra.seed"
    seed_file.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")
    keys = resolve_seal_keys([str(seed_file)], project_root=env.workspace.root)
    assert [role for role, _seed in keys.keys] == ["file"]


def test_a_seal_without_any_key_is_unsigned(tmp_path: Path) -> None:
    project = tmp_path / "loose"
    ws = workspace(project)
    ref = _succeeded(ws, "jobs/0")
    assert seal_job(ws, ref) is None
    seal = read_seal(job_seal_path(find(ws, ref.job_id).path))
    assert seal.signatures == () or list(seal.signatures) == []
    assert state_of(find(ws, ref.job_id)).seal == {
        "sha256": hashlib.sha256(job_seal_path(find(ws, ref.job_id).path).read_bytes()).hexdigest(),
        "signed": False,
    }


# -- the requests ------------------------------------------------------------


def test_seal_and_unseal_apply_only_to_succeeded_jobs_and_never_twice(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    env.seal()
    assert seal_job(env.workspace, env.jobs[0]) == "the job is already sealed"
    ready = submit(env.workspace, ("demo--0123456789abcdef", "demo"), {"start": "succeed"}, placement="jobs/r")
    assert seal_job(env.workspace, ready) == "only succeeded jobs are sealed, not ready"
    assert unseal_job(env.workspace, ready) == "only succeeded jobs are unsealed, not ready"
    assert unseal_job(env.workspace, env.jobs[0]) is None
    assert unseal_job(env.workspace, env.jobs[0]) == "the job is already unsealed"


def test_unseal_removes_the_seal_and_releases_the_job_for_delete(tmp_path: Path) -> None:
    from httk.workflow.removal import remove_jobs

    env = _setup(tmp_path)
    payload = env.seal()
    refused = remove_jobs(env.workspace, [find(env.workspace, env.jobs[0].job_id)])
    assert refused.removed_count == 0 and "job unseal" in str(refused.refused[0].reason)
    assert unseal_job(env.workspace, env.jobs[0]) is None
    payload = env.payload()
    assert not is_job_sealed(payload)
    doc = state_of(find(env.workspace, env.jobs[0].job_id))
    assert doc.seal is not None and doc.seal["released"] is True
    assert any(entry["event"] == "unsealed" and entry["reason"] == "unseal" for entry in doc.history_tail)
    assert remove_jobs(env.workspace, [find(env.workspace, env.jobs[0].job_id)]).removed_count == 1
    assert _kernel.locate(env.workspace, env.jobs[0].job_id, placement_hint=None, exhaustive=True) is None


def test_a_job_held_by_a_manager_gets_the_request_at_its_next_boundary(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    with cli_owner(env.workspace) as owner:
        owned = _kernel.claim(env.workspace, owner, env.jobs[0])
        assert owned is not None
        assert (
            seal_job(env.workspace, env.jobs[0]) == "a manager holds the job; the request applies at its next boundary"
        )
        owned.release(owned.read_state() or StateDoc.empty(owned.job_id), Release("succeeded", 500))
    assert not is_job_sealed(env.payload())
    assert len(list((env.workspace.control / "requests").iterdir())) == 1


# -- record checks -----------------------------------------------------------


def test_job_seal_detects_mismatch_extra_and_missing(tmp_path: Path) -> None:
    env = _setup(tmp_path, jobs=2)
    payload = env.seal(0)
    (payload / "files" / "runner").write_text("changed\n", encoding="utf-8")
    (payload / "added.txt").write_text("added\n", encoding="utf-8")
    kinds = {(discrepancy.path, discrepancy.kind) for discrepancy in verify_job_seal(payload).discrepancies}
    assert ("files/runner", "mismatch") in kinds
    assert ("added.txt", "extra") in kinds

    other = env.seal(1)
    (other / "files" / "runner").unlink()
    assert "missing" in {d.kind for d in verify_job_seal(other).discrepancies}


def test_job_seal_detects_an_executable_bit_flip(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    runner = env.seal() / "files" / "runner"
    runner.chmod(runner.stat().st_mode | 0o100)
    kinds = {(d.path, d.kind) for d in verify_job_seal(env.payload()).discrepancies}
    assert ("files/runner", "mismatch") in kinds


# -- workspace and project seals ---------------------------------------------


def test_workspace_seal_is_a_snapshot_that_lists_unsealed_jobs(tmp_path: Path) -> None:
    env = _setup(tmp_path, jobs=2)
    env.seal(0)
    assert [ref.job_key for ref in unsealed_jobs(env.workspace)] == [env.jobs[1].job_key]
    path, unsealed = seal_workspace(env.workspace)
    assert [ref.job_key for ref in unsealed] == [env.jobs[1].job_key]
    assert is_workspace_sealed(env.workspace)
    seal = read_seal(path)
    assert seal.subject["unsealed_jobs"] == 1
    assert {str(record["job_key"]): record["seal_sha256"] is None for record in seal.records} == {
        env.jobs[0].job_key: False,
        env.jobs[1].job_key: True,
    }
    verification = verify_workspace_seal(env.workspace, trusted_keys=_project_trust(env.project))
    assert verification.valid and not verification.discrepancies
    with pytest.raises(SealedError, match="already sealed"):
        seal_workspace(env.workspace)


def test_workspace_seal_verify_reports_drift(tmp_path: Path) -> None:
    from httk.workflow.removal import remove_jobs

    env = _setup(tmp_path, jobs=3)
    env.seal(0)
    env.seal(1)
    seal_workspace(env.workspace)
    # The seal blocks modifying CLI commands only: a request still applies.
    assert unseal_job(env.workspace, env.jobs[0]) is None
    assert unseal_job(env.workspace, env.jobs[1]) is None
    assert remove_jobs(env.workspace, [find(env.workspace, env.jobs[1].job_id)]).removed_count == 1
    env.seal(2)
    new = _succeeded(env.workspace, "jobs/new")
    verification = verify_workspace_seal(env.workspace)
    assert not verification.valid
    assert {(d.path, d.kind) for d in verification.discrepancies} == {
        (env.jobs[0].job_key, "missing"),
        (env.jobs[1].job_key, "missing_job"),
        (env.jobs[2].job_key, "mismatch"),
        (new.job_key, "unsealed"),
    }


def test_require_cli_modifiable_refuses_a_sealed_workspace_or_project(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    require_cli_modifiable(env.workspace)
    seal_workspace(env.workspace)
    with pytest.raises(SealedError, match="workspace is sealed"):
        require_cli_modifiable(env.workspace)
    seal_project(env.project)
    unseal_project(env.project)
    unseal_workspace(env.workspace)
    require_cli_modifiable(env.workspace)
    assert not (env.workspace.control / "seal.json").exists()


def test_project_seal_covers_loose_files_and_workspace_but_not_payloads(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    env.seal()
    seal_workspace(env.workspace)
    path = seal_project(env.project)
    records = read_seal(path).records
    paths = {str(record["path"]) for record in records if "type" in record}
    assert "content.txt" in paths
    assert not any(name.startswith("work/") for name in paths)
    workspace_records = [record for record in records if "member" in record]
    assert len(workspace_records) == 1
    assert workspace_records[0]["member"] == "work"
    expected = hashlib.sha256(workspace_seal_path(env.workspace).read_bytes()).hexdigest()
    assert workspace_records[0]["seal_sha256"] == expected
    assert verify_project(env.project, trusted_keys=_project_trust(env.project)).ok


# -- whole-tree verification -------------------------------------------------


def _check_tree(env: Env) -> None:
    payload = env.seal()
    seal_workspace(env.workspace)
    seal_project(env.project)
    trust = _project_trust(env.project)
    report = verify_tree(env.project, trusted_keys=trust, deep=True)
    assert report.ok
    assert [entry["level"] for entry in report.entries] == ["project", "workspace", "job"]
    assert all(entry["verdict"] == VALID_TRUSTED for entry in report.entries)

    (payload / "files" / "runner").write_text("evil\n", encoding="utf-8")
    after = verify_tree(env.project, trusted_keys=trust, deep=True)
    assert not after.ok
    faults = {entry["level"]: entry for entry in after.entries}
    assert faults["project"]["valid"] and faults["workspace"]["valid"]
    assert not faults["job"]["valid"]
    job_discrepancies = faults["job"]["discrepancies"]
    assert isinstance(job_discrepancies, list)
    assert [(d["path"], d["kind"]) for d in job_discrepancies] == [("files/runner", "mismatch")]


def test_verify_tree_is_ok_and_pinpoints_a_tampered_job_file(tmp_path: Path) -> None:
    _check_tree(_setup(tmp_path))


def test_project_seal_of_root_workspace_layout(tmp_path: Path) -> None:
    env = _setup(tmp_path, member="workspace")
    _check_tree(env)
    records = read_seal(env.project / "httk_project" / "seal.json").records
    paths = {str(record["path"]) for record in records if "type" in record}
    # The member workspace subtree is excluded wholesale.
    assert "content.txt" in paths
    assert not any(name == "workspace" or name.startswith("workspace/") for name in paths)
    assert [record["member"] for record in records if "member" in record] == ["workspace"]


def test_verify_tree_from_a_job_payload(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    report = verify_tree(env.seal(), trusted_keys=_project_trust(env.project))
    assert report.ok
    assert [entry["level"] for entry in report.entries] == ["job"]


def test_job_seal_lives_in_its_payload_and_names_only_the_job(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.seal()
    path = job_seal_path(payload)
    assert path == payload / ".httk-job" / "seal.json"
    assert is_job_sealed(payload)
    assert read_seal(path).subject == {"job_id": env.jobs[0].job_id, "job_key": env.jobs[0].job_key}


def test_a_job_directory_moved_out_of_its_workspace_still_verifies(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.seal()
    loose = tmp_path / "elsewhere" / env.jobs[0].job_key
    loose.parent.mkdir()
    shutil.copytree(payload, loose)
    trust = _project_trust(env.project)
    assert verify_job_seal(loose, trusted_keys=trust).verdict == VALID_TRUSTED
    report = verify_tree(loose, trusted_keys=trust)
    assert report.ok
    assert [(entry["level"], entry["subject"]) for entry in report.entries] == [("job", env.jobs[0].job_key)]
    (loose / "files" / "runner").write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    assert not verify_tree(loose, trusted_keys=trust).ok


# -- unseal ordering ---------------------------------------------------------


def test_unseal_refuses_out_of_order(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    env.seal()
    seal_workspace(env.workspace)
    seal_project(env.project)
    with pytest.raises(SealedError):
        unseal_workspace(env.workspace)
    unseal_project(env.project)
    assert not is_project_sealed(env.project)
    unseal_workspace(env.workspace)
    assert not is_workspace_sealed(env.workspace)
    assert unseal_job(env.workspace, env.jobs[0]) is None
    assert not is_job_sealed(env.payload())


def test_identity_public_key_is_a_signer(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    path = job_seal_path(env.seal())
    identity = identity_public_key()
    assert identity is not None
    verification = verify_seal(path, trusted_keys=(identity,))
    assert key_fingerprint(identity) in verification.signers
    assert verification.verdict == VALID_TRUSTED


def test_a_special_or_oversized_job_seal_is_refused_without_blocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket

    from httk.workflow import seals
    from httk.workflow.errors import FormatError
    from httk.workflow.seals import INVALID, is_job_sealed, job_seal_digest, verify_job_seal

    state = tmp_path / "payload" / ".httk-job"
    state.mkdir(parents=True)
    monkeypatch.chdir(state)  # a short relative name: AF_UNIX paths are length-limited
    with socket.socket(socket.AF_UNIX) as server:
        server.bind("seal.json")
        assert not is_job_sealed(state.parent)
        with pytest.raises(FormatError):
            job_seal_digest(state.parent)
        assert verify_job_seal(state.parent).verdict == INVALID
    (state / "seal.json").unlink()
    (state / "seal.json").write_bytes(b"x" * 11)
    monkeypatch.setattr(seals, "_SEAL_LIMIT", 10)
    with pytest.raises(FormatError):
        job_seal_digest(state.parent)
    (state / "seal.json").write_bytes(b"x" * 10)
    assert job_seal_digest(state.parent) == hashlib.sha256(b"x" * 10).hexdigest()
