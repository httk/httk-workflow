"""``verify_job_seal(content=True)``: sealed digest manifests checked against decompressed content."""

import gzip
import hashlib
import sys
from pathlib import Path

import pytest
from httk.core.crypto import ed25519_generate_seed

from httk.workflow.errors import FormatError
from httk.workflow.models import WORKSPACE_DIRECTORY
from httk.workflow.runtime_utils import DIGEST_MANIFEST_NAME, compress_files
from httk.workflow.seals import seal_payload, verify_job_seal, verify_tree

zstd = pytest.importorskip("compression.zstd" if sys.version_info >= (3, 14) else "backports.zstd")

_X = b"".join(f"band {n} {n * n}\n".encode() for n in range(5000))
_Y = b"other content\n" * 100


def _sealed(tmp_path: Path, manifest: str | None = None, *, keep_plain: bool = False) -> Path:
    """A signed job payload whose ``run/X`` and ``run/Y`` are zst-compressed with a digest manifest."""

    payload = tmp_path / "job"
    run = payload / "run"
    run.mkdir(parents=True)
    (run / "X").write_bytes(_X)
    (run / "Y").write_bytes(_Y)
    compress_files([run / "X", run / "Y"], method="zst", record_digests=True, remove_source=not keep_plain)
    if manifest is not None:
        (run / DIGEST_MANIFEST_NAME).write_text(manifest)
    seal_payload(
        payload, job_id="job-id", job_key="job-key", keys=[("operator", ed25519_generate_seed())], durable=False
    )
    return payload


def _verify(payload: Path, content: bool) -> tuple[bool, set[tuple[str, str]]]:
    verification = verify_job_seal(payload, content=content)
    return verification.valid, {(item.path, item.kind) for item in verification.discrepancies}


def test_an_untouched_payload_verifies_with_and_without_content(tmp_path: Path) -> None:
    payload = _sealed(tmp_path)
    assert _verify(payload, False) == (True, set())
    assert _verify(payload, True) == (True, set())


def test_a_lying_manifest_is_caught_only_by_content(tmp_path: Path) -> None:
    manifest = f"{'0' * 64}  X\n{hashlib.sha256(_Y).hexdigest()}  Y\n"
    payload = _sealed(tmp_path, manifest)
    assert _verify(payload, False) == (True, set())
    assert _verify(payload, True) == (False, {("run/X.zst", "content_mismatch")})


def test_a_recompression_at_another_level_is_reencoded(tmp_path: Path) -> None:
    payload = _sealed(tmp_path)
    stored = payload / "run" / "X.zst"
    before = stored.read_bytes()
    stored.write_bytes(zstd.compress(_X, level=19))
    assert stored.read_bytes() != before
    assert _verify(payload, False) == (False, {("run/X.zst", "mismatch")})
    assert _verify(payload, True) == (True, {("run/X.zst", "reencoded")})


def test_a_decompressed_file_is_reencoded(tmp_path: Path) -> None:
    payload = _sealed(tmp_path)
    (payload / "run" / "X.zst").unlink()
    (payload / "run" / "X").write_bytes(_X)
    assert _verify(payload, False) == (False, {("run/X.zst", "missing"), ("run/X", "extra")})
    assert _verify(payload, True) == (True, {("run/X", "reencoded")})


def test_another_codec_is_reencoded(tmp_path: Path) -> None:
    payload = _sealed(tmp_path)
    (payload / "run" / "X.zst").unlink()
    (payload / "run" / "X.gz").write_bytes(gzip.compress(_X))
    assert _verify(payload, True) == (True, {("run/X.gz", "reencoded")})


@pytest.mark.parametrize("stored", [zstd.compress(b"changed\n"), b"not a zstd stream"])
def test_changed_or_undecodable_content_is_a_content_mismatch(tmp_path: Path, stored: bytes) -> None:
    payload = _sealed(tmp_path)
    (payload / "run" / "X.zst").write_bytes(stored)
    assert _verify(payload, True) == (False, {("run/X.zst", "mismatch"), ("run/X.zst", "content_mismatch")})


def test_a_deleted_file_is_content_missing(tmp_path: Path) -> None:
    payload = _sealed(tmp_path)
    (payload / "run" / "X.zst").unlink()
    assert _verify(payload, True) == (False, {("run/X.zst", "missing"), ("run/X", "content_missing")})


def test_a_manifest_changed_after_sealing_is_not_used(tmp_path: Path) -> None:
    payload = _sealed(tmp_path)
    (payload / "run" / DIGEST_MANIFEST_NAME).write_text(f"{'0' * 64}  X\n")
    assert _verify(payload, True) == (False, {(f"run/{DIGEST_MANIFEST_NAME}", "mismatch")})


def test_a_sealed_malformed_manifest_is_invalid(tmp_path: Path) -> None:
    payload = _sealed(tmp_path, "garbage\n")
    assert _verify(payload, False) == (True, set())
    assert _verify(payload, True) == (False, {(f"run/{DIGEST_MANIFEST_NAME}", "invalid")})


@pytest.mark.parametrize("form", ["X", "X.gz"])
def test_a_matching_form_does_not_excuse_a_changed_one(tmp_path: Path, form: str) -> None:
    payload = _sealed(tmp_path)
    (payload / "run" / "X.zst").write_bytes(zstd.compress(b"evil\n"))
    (payload / "run" / form).write_bytes(_X if form == "X" else gzip.compress(_X))
    valid, found = _verify(payload, True)
    assert not valid
    assert found == {("run/X.zst", "mismatch"), ("run/X.zst", "content_mismatch"), (f"run/{form}", "extra")}


def test_a_changed_plain_form_beside_a_sealed_compressed_one_is_a_content_mismatch(tmp_path: Path) -> None:
    payload = _sealed(tmp_path, keep_plain=True)
    (payload / "run" / "X").write_bytes(b"other bytes\n")
    assert _verify(payload, True) == (False, {("run/X", "mismatch"), ("run/X", "content_mismatch")})


def test_content_refuses_a_shallow_workspace_verification(tmp_path: Path) -> None:
    (tmp_path / WORKSPACE_DIRECTORY).mkdir()
    with pytest.raises(FormatError, match="--content needs a deep verification"):
        verify_tree(tmp_path, deep=False, content=True)
