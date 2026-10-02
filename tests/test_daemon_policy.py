"""Strict validation for the confined workspace daemon policy."""

import base64
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from httk.workflow._daemon_policy import Policy, Profile, load_policy

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


def _document(tmp_path: Path) -> dict[str, object]:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    return {
        "format": "httk-workspace-daemon-policy",
        "format_version": 1,
        "workspace": str(tmp_path / "workspace"),
        "workspace_id": "12345678-1234-1234-1234-123456789abc",
        "enrollment_id": "0123456789abcdef0123456789abcdef",
        "requests": str(tmp_path / "requests"),
        "responses": str(tmp_path / "responses"),
        "state": str(tmp_path / "state"),
        "bwrap": str(runtime / "bin/bwrap"),
        "python": str(runtime / "bin/python"),
        "sbatch": str(broker / "bin/sbatch"),
        "squeue": str(broker / "bin/squeue"),
        "scancel": str(broker / "bin/scancel"),
        "cluster": "cluster-1",
        "readonly_paths": [str(runtime)],
        "broker_paths": [str(broker)],
        "authorized_keys": [AUTHORIZED_KEY],
        "profiles": {
            "cpu": {"cpus": 8, "memory_mb": 16384, "time_minutes": 60},
            "long-1": {
                "cpus": 2,
                "memory_mb": 4096,
                "time_minutes": 1440,
                "partition": "compute.1",
                "account": "project_1",
            },
        },
    }


def _write(tmp_path: Path, document: object) -> Path:
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_policy_loads_exact_fields_and_defaults(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, _document(tmp_path)))

    assert policy.workspace == tmp_path / "workspace"
    assert policy.max_records == 4096
    assert policy.max_submissions == 128
    assert policy.poll_seconds == 1.0
    assert policy.command_timeout == 30.0
    assert policy.max_output_bytes == 65536
    assert policy.request_max_age == 3600
    assert policy.authorized_keys == (AUTHORIZED_KEY,)
    assert policy.profile("cpu") == Profile("cpu", 8, 16384, 60)
    assert policy.profile("long-1").partition == "compute.1"
    with pytest.raises(ValueError, match="unknown daemon profile"):
        policy.profile("missing")


@pytest.mark.parametrize("field", ["workspace", "workspace_id", "profiles", "bwrap", "cluster"])
def test_required_policy_fields_cannot_be_omitted(tmp_path: Path, field: str) -> None:
    document = _document(tmp_path)
    del document[field]
    with pytest.raises(ValueError, match="missing or unknown"):
        load_policy(_write(tmp_path, document))


def test_authorized_keys_must_be_explicit_and_nonempty(tmp_path: Path) -> None:
    missing = _document(tmp_path)
    del missing["authorized_keys"]
    with pytest.raises(ValueError, match="authorized_keys.*required.*nonempty"):
        load_policy(_write(tmp_path, missing))

    empty = _document(tmp_path)
    empty["authorized_keys"] = []
    with pytest.raises(ValueError, match="authorized_keys.*nonempty"):
        load_policy(_write(tmp_path, empty))


def test_unknown_and_duplicate_keys_are_refused(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document["argv"] = ["sh"]
    with pytest.raises(ValueError, match="missing or unknown"):
        load_policy(_write(tmp_path, document))

    raw = json.dumps(_document(tmp_path))[:-1] + ',"cluster":"other"}'
    path = tmp_path / "duplicate.json"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(ValueError, match="invalid policy document"):
        load_policy(path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("format_version", True),
        ("workspace_id", 7),
        ("readonly_paths", "runtime"),
        ("profiles", []),
        ("poll_seconds", True),
        ("max_records", 1.0),
    ],
)
def test_policy_field_types_are_exact(tmp_path: Path, field: str, value: object) -> None:
    document = _document(tmp_path)
    document[field] = value
    with pytest.raises(ValueError):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("number", ["NaN", "Infinity", "1e9999"])
def test_nonfinite_json_numbers_are_refused(tmp_path: Path, number: str) -> None:
    raw = json.dumps(_document(tmp_path)).replace(
        '"cluster": "cluster-1"', f'"poll_seconds": {number}, "cluster": "cluster-1"'
    )
    path = tmp_path / "nonfinite.json"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(ValueError, match="invalid policy document"):
        load_policy(path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_records", 0),
        ("max_records", 100001),
        ("max_submissions", 0),
        ("poll_seconds", 0.049),
        ("poll_seconds", 60.1),
        ("poll_seconds", 10**400),
        ("command_timeout", 0.09),
        ("command_timeout", 601),
        ("max_output_bytes", 1023),
        ("max_output_bytes", 1048577),
        ("request_max_age", 0),
        ("request_max_age", 86401),
        ("request_max_age", True),
    ],
)
def test_policy_numeric_bounds_are_enforced(tmp_path: Path, field: str, value: object) -> None:
    document = _document(tmp_path)
    document[field] = value
    with pytest.raises(ValueError):
        load_policy(_write(tmp_path, document))


def test_submission_quota_cannot_exceed_record_quota(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document.update(max_records=5, max_submissions=6)
    with pytest.raises(ValueError, match="max_submissions"):
        load_policy(_write(tmp_path, document))


def test_authorized_keys_are_canonical_unique_ed25519_values(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document["request_max_age"] = 7200
    policy = load_policy(_write(tmp_path, document))
    assert policy.authorized_keys == (AUTHORIZED_KEY,)
    assert policy.request_max_age == 7200

    for invalid in (
        AUTHORIZED_KEY.removeprefix("ed25519:"),
        "rsa:" + AUTHORIZED_KEY.removeprefix("ed25519:"),
        "ed25519:AAAA",
        "ed25519:not-base64",
    ):
        document = _document(tmp_path)
        document["authorized_keys"] = [invalid]
        with pytest.raises(ValueError, match="canonical Ed25519"):
            load_policy(_write(tmp_path, document))

    document = _document(tmp_path)
    document["authorized_keys"] = [AUTHORIZED_KEY, AUTHORIZED_KEY]
    with pytest.raises(ValueError, match="unique"):
        load_policy(_write(tmp_path, document))

    document = _document(tmp_path)
    document["authorized_keys"] = AUTHORIZED_KEY
    with pytest.raises(ValueError, match="array"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cpus", 0),
        ("cpus", 1025),
        ("memory_mb", 0),
        ("memory_mb", 1048577),
        ("time_minutes", 0),
        ("time_minutes", 10081),
        ("partition", "bad value"),
        ("account", True),
    ],
)
def test_profile_types_names_and_bounds_are_enforced(tmp_path: Path, field: str, value: object) -> None:
    document = _document(tmp_path)
    profiles = document["profiles"]
    assert isinstance(profiles, dict)
    cpu = profiles["cpu"]
    assert isinstance(cpu, dict)
    cpu[field] = value
    with pytest.raises(ValueError):
        load_policy(_write(tmp_path, document))


def test_profile_names_and_members_are_strict(tmp_path: Path) -> None:
    document = _document(tmp_path)
    profiles = document["profiles"]
    assert isinstance(profiles, dict)
    profiles["Bad Name"] = profiles.pop("cpu")
    with pytest.raises(ValueError, match="profile name"):
        load_policy(_write(tmp_path, document))

    document = _document(tmp_path)
    profiles = document["profiles"]
    assert isinstance(profiles, dict) and isinstance(profiles["cpu"], dict)
    profiles["cpu"]["command"] = "sh"
    with pytest.raises(ValueError, match="profile fields"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("field", ["workspace", "requests", "responses", "state"])
def test_mutable_roots_must_be_disjoint(tmp_path: Path, field: str) -> None:
    document = _document(tmp_path)
    document[field] = str(tmp_path / "workspace/inside") if field != "workspace" else str(tmp_path / "requests/inside")
    with pytest.raises(ValueError, match="pairwise disjoint"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("kind", ["readonly_paths", "broker_paths"])
def test_runtime_roots_cannot_overlap_mutable_roots_or_be_root(tmp_path: Path, kind: str) -> None:
    document = _document(tmp_path)
    document[kind] = [str(tmp_path / "workspace/escape")]
    with pytest.raises(ValueError, match="disjoint"):
        load_policy(_write(tmp_path, document))

    document = _document(tmp_path)
    document[kind] = ["/"]
    with pytest.raises(ValueError, match="filesystem root"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("readonly_inside_broker", [False, True])
def test_readonly_and_broker_roots_must_be_lexically_disjoint(tmp_path: Path, readonly_inside_broker: bool) -> None:
    document = _document(tmp_path)
    if readonly_inside_broker:
        broker = tmp_path / "broker"
        document["readonly_paths"] = [str(broker / "shared")]
        document["python"] = str(broker / "shared/python")
    else:
        readonly = tmp_path / "runtime"
        document["broker_paths"] = [str(readonly / "private")]
        for command in ("sbatch", "squeue", "scancel"):
            document[command] = str(readonly / "private" / command)
    with pytest.raises(ValueError, match="pairwise disjoint"):
        load_policy(_write(tmp_path, document))


def test_overlapping_roots_within_shared_readonly_role_are_allowed(tmp_path: Path) -> None:
    document = _document(tmp_path)
    readonly = tmp_path / "runtime"
    document["readonly_paths"] = [str(readonly), str(readonly / "bin")]
    policy = load_policy(_write(tmp_path, document))
    assert policy.readonly_paths == (readonly, readonly / "bin")


@pytest.mark.parametrize("reserved", ["/tmp", "/workspace/child", "/proc/self", "/tmp/home/cache"])
def test_runtime_roots_cannot_overlap_reserved_sandbox_destinations(tmp_path: Path, reserved: str) -> None:
    document = _document(tmp_path)
    document["readonly_paths"] = [reserved]
    document["python"] = str(Path(reserved) / "python")
    with pytest.raises(ValueError, match="reserved sandbox destinations"):
        load_policy(_write(tmp_path, document))


def test_commands_must_be_covered_by_their_approved_roots(tmp_path: Path) -> None:
    for field in ("python", "sbatch", "squeue", "scancel", "bwrap"):
        document = _document(tmp_path)
        document[field] = str(tmp_path / "unapproved" / field)
        with pytest.raises(ValueError, match="within"):
            load_policy(_write(tmp_path, document))


def test_slurm_commands_may_use_a_shared_readonly_runtime(tmp_path: Path) -> None:
    document = _document(tmp_path)
    readonly_paths = document["readonly_paths"]
    assert isinstance(readonly_paths, list) and isinstance(readonly_paths[0], str)
    runtime = Path(readonly_paths[0])
    document["sbatch"] = str(runtime / "bin/sbatch")
    document["squeue"] = str(runtime / "bin/squeue")
    document["scancel"] = str(runtime / "bin/scancel")
    policy = load_policy(_write(tmp_path, document))
    assert policy.sbatch == runtime / "bin/sbatch"


@pytest.mark.parametrize("field", ["workspace", "python", "readonly_paths"])
def test_paths_must_be_absolute_strings_without_parent_segments(tmp_path: Path, field: str) -> None:
    document = _document(tmp_path)
    document[field] = ["/safe/../bad"] if field == "readonly_paths" else "/safe/../bad"
    with pytest.raises(ValueError, match="absolute path"):
        load_policy(_write(tmp_path, document))

    document = _document(tmp_path)
    document[field] = [7] if field == "readonly_paths" else 7
    with pytest.raises(ValueError, match="string"):
        load_policy(_write(tmp_path, document))


def test_policy_source_is_bounded_regular_and_nofollow(tmp_path: Path) -> None:
    target = _write(tmp_path, _document(tmp_path))
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(OSError):
        load_policy(link)

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(ValueError, match="regular"):
        load_policy(directory)

    large = tmp_path / "large.json"
    large.write_bytes(b"x" * (64 * 1024 + 1))
    with pytest.raises(ValueError, match="too large"):
        load_policy(large)


def test_policy_walk_closes_new_descriptor_after_old_close_error(monkeypatch: pytest.MonkeyPatch) -> None:
    module: Any = importlib.import_module("httk.workflow._daemon_policy")
    opened = iter((10, 11))
    closed: list[int] = []

    def fake_open(*_args: object, **_kwargs: object) -> int:
        return next(opened)

    def fake_close(descriptor: int) -> None:
        closed.append(descriptor)
        if descriptor == 10:
            raise OSError("injected close failure")

    fake_os = SimpleNamespace(
        open=fake_open,
        close=fake_close,
        O_RDONLY=module.os.O_RDONLY,
        O_DIRECTORY=module.os.O_DIRECTORY,
        O_CLOEXEC=module.os.O_CLOEXEC,
        O_NOFOLLOW=module.os.O_NOFOLLOW,
        O_NONBLOCK=module.os.O_NONBLOCK,
    )
    monkeypatch.setattr(module, "os", fake_os)
    with pytest.raises(OSError, match="injected"):
        module._open_nofollow(Path("/parent/policy.json"))
    assert closed == [10, 11]


def test_policy_walk_closes_file_after_final_parent_close_error(monkeypatch: pytest.MonkeyPatch) -> None:
    module: Any = importlib.import_module("httk.workflow._daemon_policy")
    opened = iter((10, 11))
    closed: list[int] = []

    def fake_open(*_args: object, **_kwargs: object) -> int:
        return next(opened)

    def fake_close(descriptor: int) -> None:
        closed.append(descriptor)
        if descriptor == 10:
            raise OSError("injected close failure")

    fake_os = SimpleNamespace(
        open=fake_open,
        close=fake_close,
        O_RDONLY=module.os.O_RDONLY,
        O_DIRECTORY=module.os.O_DIRECTORY,
        O_CLOEXEC=module.os.O_CLOEXEC,
        O_NOFOLLOW=module.os.O_NOFOLLOW,
        O_NONBLOCK=module.os.O_NONBLOCK,
    )
    monkeypatch.setattr(module, "os", fake_os)
    with pytest.raises(OSError, match="injected"):
        module._open_nofollow(Path("/policy.json"))
    assert closed == [10, 11]


def test_direct_dataclasses_enforce_the_same_invariants(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        Profile("bad name", 1, 1, 1)
    with pytest.raises(ValueError, match="pairwise disjoint"):
        Policy(
            workspace=tmp_path / "workspace",
            workspace_id="12345678-1234-1234-1234-123456789abc",
            enrollment_id="0" * 32,
            requests=tmp_path / "workspace/requests",
            responses=tmp_path / "responses",
            state=tmp_path / "state",
            bwrap=tmp_path / "runtime/bwrap",
            python=tmp_path / "runtime/python",
            sbatch=tmp_path / "broker/sbatch",
            squeue=tmp_path / "broker/squeue",
            scancel=tmp_path / "broker/scancel",
            cluster="cluster",
            readonly_paths=(tmp_path / "runtime",),
            broker_paths=(tmp_path / "broker",),
            profiles=(),
        )
