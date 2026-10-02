"""Protected active snapshot pointer tests."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from httk.workflow._daemon_activation import activation_document, read_active_snapshot, verify_active_snapshot
from httk.workflow._daemon_policy import Policy, Profile


def _policy(tmp_path: Path) -> Policy:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    return Policy(
        workspace=tmp_path / "workspace",
        workspace_id="12345678-1234-1234-1234-123456789abc",
        enrollment_id="1" * 32,
        requests=tmp_path / "requests",
        responses=tmp_path / "responses",
        state=tmp_path / "state",
        bwrap=broker / "bwrap",
        python=runtime / "python",
        sbatch=broker / "sbatch",
        squeue=broker / "squeue",
        scancel=broker / "scancel",
        cluster="cluster",
        readonly_paths=(runtime,),
        broker_paths=(broker,),
        profiles=(Profile("cpu", 2, 1024, 10, workers=2, prelude="module load approved"),),
    )


def _write(state: Path, document: object) -> None:
    path = state / "active.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    path.chmod(0o600)


def test_activation_document_round_trips_and_verifies_exact_policy(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    snapshot = tmp_path / "snapshots" / ("a" * 64 + ".json")
    policy = _policy(tmp_path)
    document = activation_document(snapshot, policy)
    assert document == {
        "format": "httk-workspace-daemon-activation",
        "format_version": 1,
        "snapshot": str(snapshot),
        "digest": document["digest"],
    }
    assert isinstance(document["digest"], str) and len(document["digest"]) == 64
    _write(state, document)
    assert read_active_snapshot(state) == (snapshot, document["digest"])
    verify_active_snapshot(state, snapshot, policy)

    with pytest.raises(ValueError, match="stale or mismatched"):
        verify_active_snapshot(state, tmp_path / "snapshots/other.json", policy)
    with pytest.raises(ValueError, match="stale or mismatched"):
        verify_active_snapshot(state, snapshot, replace(policy, profiles=(replace(policy.profiles[0], cpus=3),)))


@pytest.mark.parametrize(
    "document",
    [
        {},
        {
            "format": "wrong",
            "format_version": 1,
            "snapshot": "/snapshot.json",
            "digest": "0" * 64,
        },
        {
            "format": "httk-workspace-daemon-activation",
            "format_version": True,
            "snapshot": "/snapshot.json",
            "digest": "0" * 64,
        },
        {
            "format": "httk-workspace-daemon-activation",
            "format_version": 1,
            "snapshot": "relative.json",
            "digest": "0" * 64,
        },
        {
            "format": "httk-workspace-daemon-activation",
            "format_version": 1,
            "snapshot": "/snapshot.json",
            "digest": "A" * 64,
        },
    ],
)
def test_active_pointer_schema_is_exact(tmp_path: Path, document: object) -> None:
    state = tmp_path / "state"
    state.mkdir()
    _write(state, document)
    with pytest.raises(ValueError):
        read_active_snapshot(state)


def test_active_pointer_is_bounded_private_regular_and_nofollow(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    active = state / "active.json"
    active.write_bytes(b"x" * 4097)
    active.chmod(0o600)
    with pytest.raises(ValueError, match="too large"):
        read_active_snapshot(state)

    active.write_text("{}", encoding="utf-8")
    active.chmod(0o666)
    with pytest.raises(ValueError, match="writable"):
        read_active_snapshot(state)

    active.unlink()
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    active.symlink_to(target)
    with pytest.raises(OSError):
        read_active_snapshot(state)

    alias = tmp_path / "alias"
    alias.symlink_to(state, target_is_directory=True)
    with pytest.raises(OSError):
        read_active_snapshot(alias)


def test_duplicate_pointer_fields_are_refused(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    active = state / "active.json"
    active.write_text(
        '{"format":"httk-workspace-daemon-activation","format":"httk-workspace-daemon-activation",'
        '"format_version":1,"snapshot":"/snapshot.json","digest":"' + "0" * 64 + '"}',
        encoding="utf-8",
    )
    active.chmod(0o600)
    with pytest.raises(ValueError, match="duplicate"):
        read_active_snapshot(state)
