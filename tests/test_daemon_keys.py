"""Protected workspace-daemon response key lifecycle tests."""

import base64
import os
from pathlib import Path

import pytest
from httk.core.crypto import ed25519_public_key

from httk.workflow._daemon_keys import (
    initialize_response_seed,
    read_response_seed,
    response_public_key,
    response_seed_path,
)


def test_response_seed_is_exclusive_private_and_stable(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    path = initialize_response_seed(state)
    seed = read_response_seed(path)
    assert path == state / "response.seed"
    assert len(seed) == 32
    assert path.stat().st_mode & 0o777 == 0o600
    assert response_public_key(path) == "ed25519:" + base64.b64encode(ed25519_public_key(seed)).decode("ascii")

    with pytest.raises(FileExistsError):
        initialize_response_seed(state)
    assert read_response_seed(path) == seed


def test_missing_corrupt_permissive_and_symlinked_seeds_fail_closed(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    path = response_seed_path(state)
    with pytest.raises(FileNotFoundError):
        read_response_seed(path)

    path.write_text("not-base64\n", encoding="ascii")
    path.chmod(0o600)
    with pytest.raises(ValueError, match="canonical base64"):
        read_response_seed(path)

    path.write_bytes(base64.b64encode(b"x" * 32) + b"\n")
    path.chmod(0o640)
    with pytest.raises(ValueError, match="0600"):
        read_response_seed(path)

    path.unlink()
    target = tmp_path / "target.seed"
    target.write_bytes(base64.b64encode(b"y" * 32) + b"\n")
    target.chmod(0o600)
    path.symlink_to(target)
    with pytest.raises(OSError):
        read_response_seed(path)


def test_response_seed_refuses_symlinked_parent_and_partial_file_is_not_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    with pytest.raises(OSError):
        initialize_response_seed(alias)

    state = tmp_path / "state"
    state.mkdir()
    original_write = os.write
    calls = 0

    def fail_after_prefix(descriptor: int, data: bytes) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return original_write(descriptor, data[:4])
        raise OSError("injected seed write failure")

    monkeypatch.setattr("httk.workflow._daemon_keys.os.write", fail_after_prefix)
    with pytest.raises(OSError, match="injected"):
        initialize_response_seed(state)
    partial = response_seed_path(state)
    assert partial.exists()
    with pytest.raises(FileExistsError):
        initialize_response_seed(state)
    with pytest.raises(ValueError):
        read_response_seed(partial)
