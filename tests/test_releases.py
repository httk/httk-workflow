"""Release records: a sealed file moved out of its job amends the seal without rewriting it."""

import json
from pathlib import Path

import pytest
from httk.core.project.sealing import seal_project, sign_seal_body

from httk.workflow.errors import FormatError
from httk.workflow.seals import (
    VALID_TRUSTED,
    default_workspace_keys,
    job_releases_path,
    job_seal_path,
    read_job_releases,
    read_seal,
    record_job_release,
    seal_job,
    seal_payload,
    seal_workspace,
    valid_job_releases,
    verify_job_seal,
    verify_tree,
    verify_workspace_seal,
)
from test_seals import Env, _project_trust, _setup, _succeeded
from v3_helpers import find, workspace

_TARGET = {"job_key": "next--00000000-0000-4000-8000-000000000000", "path": "files/runner"}


def _record(env: Env, payload: Path, *, signed: bool = True, keys: object = None) -> None:
    (sealed,) = [item for item in read_seal(job_seal_path(payload)).records if item["path"] == "files/runner"]
    record_job_release(
        env.workspace,
        payload,
        path="files/runner",
        size=int(sealed["size"]),  # type: ignore[call-overload]
        sha256=str(sealed["sha256"]),
        released_to=_TARGET,
        keys=(default_workspace_keys(env.workspace).keys if signed else ()) if keys is None else keys,  # type: ignore[arg-type]
    )


def _released(tmp_path: Path, *, signed: bool = True) -> tuple[Env, Path]:
    """Seal one job (signed), move its ``files/runner`` away and record the release; return the env and payload."""

    env = _setup(tmp_path)
    payload = env.seal()
    (payload / "files" / "runner").rename(tmp_path / "moved")
    _record(env, payload, signed=signed)
    return env, payload


def _kinds(payload: Path) -> set[tuple[str, str]]:
    return {(item.path, item.kind) for item in verify_job_seal(payload).discrepancies}


def _record_path(payload: Path) -> Path:
    (path,) = job_releases_path(payload).iterdir()
    return path


def test_a_signed_release_covers_a_moved_file_of_a_signed_seal(tmp_path: Path) -> None:
    env, payload = _released(tmp_path)

    verification = verify_job_seal(payload, trusted_keys=_project_trust(env.project))

    assert verification.discrepancies == ()
    assert verification.verdict == VALID_TRUSTED
    (release,) = read_job_releases(payload)
    assert (release.path, release.to_job_key, release.to_path) == ("files/runner", *_TARGET.values())
    assert release.signatures
    assert set(valid_job_releases(payload, read_seal(job_seal_path(payload)), ["files/runner"])) == {"files/runner"}
    document = json.loads(_record_path(payload).read_text(encoding="utf-8"))
    assert (document["format"], document["version"], document["body"]["kind"]) == ("httk-job-release", 1, "release")


def test_an_unsigned_release_covers_a_file_of_an_unsigned_seal(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "loose")
    ref = _succeeded(ws, "jobs/0")
    assert seal_job(ws, ref) is None
    payload = find(ws, ref.job_id).path
    assert not read_seal(job_seal_path(payload)).signatures
    (payload / "files" / "runner").rename(tmp_path / "moved")
    _record(Env(tmp_path, ws, [ref]), payload, signed=False)

    assert verify_job_seal(payload).discrepancies == ()


def test_an_unsigned_release_does_not_cover_a_file_of_a_signed_seal(tmp_path: Path) -> None:
    _env, payload = _released(tmp_path, signed=False)

    assert _kinds(payload) == {("files/runner", "missing")}


def test_a_missing_sealed_file_without_a_release_is_a_discrepancy(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.seal()
    (payload / "files" / "runner").rename(tmp_path / "moved")

    assert _kinds(payload) == {("files/runner", "missing")}


def _rewrite(payload: Path, change: str, keys: object) -> None:
    """Replace the stored record with a broken variant."""

    path = _record_path(payload)
    record = json.loads(path.read_text(encoding="utf-8"))
    body = record["body"]
    if change == "amends":
        body["amends"] = "0" * 64
    elif change == "sha256":
        body["sha256"] = "1" * 64
    if change in ("amends", "sha256"):
        # Properly signed, but about another seal or other bytes.
        record["body_sha256"], record["signatures"] = sign_seal_body(body, keys)  # type: ignore[arg-type]
    elif change == "body":
        body["size"] += 1
    elif change == "signature":
        record["signatures"][0]["signature"] = "AAAA"
    path.write_text(json.dumps(record), encoding="utf-8")


@pytest.mark.parametrize("change", ["amends", "sha256", "body", "signature"])
def test_an_invalid_release_does_not_cover_the_file(tmp_path: Path, change: str) -> None:
    env, payload = _released(tmp_path)

    _rewrite(payload, change, default_workspace_keys(env.workspace).keys)

    assert _kinds(payload) == {("files/runner", "missing")}
    assert valid_job_releases(payload, read_seal(job_seal_path(payload)), ["files/runner"]) == {}


def test_a_path_is_released_once(tmp_path: Path) -> None:
    env, payload = _released(tmp_path)

    with pytest.raises(FileExistsError):
        _record(env, payload)

    assert len(read_job_releases(payload)) == 1


def test_a_release_whose_file_is_present_covers_nothing(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.seal()
    _record(env, payload)

    assert len(read_job_releases(payload)) == 1
    assert valid_job_releases(payload, read_seal(job_seal_path(payload)), ["files/runner"]) == {}


def test_workspace_and_project_seals_stay_valid_after_a_release(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.seal()
    seal_bytes = job_seal_path(payload).read_bytes()
    seal_workspace(env.workspace)
    seal_project(env.project)
    (payload / "files" / "runner").rename(tmp_path / "moved")
    _record(env, payload)

    trust = _project_trust(env.project)
    assert job_seal_path(payload).read_bytes() == seal_bytes
    assert verify_workspace_seal(env.workspace, trusted_keys=trust).valid
    report = verify_tree(env.project, trusted_keys=trust, deep=True)
    assert report.ok, report.entries


def test_resealing_keeps_release_records_that_amend_the_old_seal(tmp_path: Path) -> None:
    env, payload = _released(tmp_path)
    ref = env.jobs[0]

    seal_payload(payload, job_id=ref.job_id, job_key=ref.job_key, keys=(), durable=False)

    assert len(read_job_releases(payload)) == 1
    assert valid_job_releases(payload, read_seal(job_seal_path(payload)), ["files/runner"]) == {}


def test_a_planted_releases_file_is_no_records_and_breaks_nothing(tmp_path: Path) -> None:
    env = _setup(tmp_path)
    payload = env.seal()
    job_releases_path(payload).write_text("planted\n", encoding="utf-8")
    ref = env.jobs[0]

    assert read_job_releases(payload) == ()
    assert verify_job_seal(payload).discrepancies == ()
    seal_payload(payload, job_id=ref.job_id, job_key=ref.job_key, keys=(), durable=False)
    with pytest.raises(FormatError):
        _record(env, payload)


def test_a_release_signed_only_by_a_key_that_did_not_sign_the_seal_is_invalid(tmp_path: Path) -> None:
    from httk.core.crypto import ed25519_generate_seed

    env = _setup(tmp_path)
    payload = env.seal()
    (payload / "files" / "runner").rename(tmp_path / "moved")
    _record(env, payload, keys=[("rogue", ed25519_generate_seed())])

    assert _kinds(payload) == {("files/runner", "missing")}


def test_validation_opens_only_the_records_of_missing_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow import seals

    env, payload = _released(tmp_path)
    for index in range(50):
        (job_releases_path(payload) / f"junk{index}").write_bytes(b"junk")
    (job_releases_path(payload) / ".partial.tmp").write_bytes(b"")
    monkeypatch.setattr(seals, "_RELEASES_LIMIT", 10)

    assert verify_job_seal(payload, trusted_keys=_project_trust(env.project)).discrepancies == ()
    with pytest.raises(FormatError):
        read_job_releases(payload)
