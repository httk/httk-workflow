"""Strict validation for the confined workspace daemon policy."""

import base64
import errno
import importlib
import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from httk.workflow._daemon_policy import Policy, Profile, check_layout, load_policy, policy_document

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


def _document(tmp_path: Path) -> dict[str, object]:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    return {
        "format": "httk-workspace-daemon-policy",
        "format_version": 3,
        "workspace": str(tmp_path / "site/workspace"),
        "workspace_id": "12345678-1234-1234-1234-123456789abc",
        "enrollment_id": "0123456789abcdef0123456789abcdef",
        "exchange": str(tmp_path / "site/exchange"),
        "state": str(tmp_path / "state"),
        "snapshots": str(tmp_path / "snapshots"),
        "bwrap": str(runtime / "bin/bwrap"),
        "python": str(runtime / "bin/python"),
        "sbatch": str(broker / "bin/sbatch"),
        "squeue": str(broker / "bin/squeue"),
        "scancel": str(broker / "bin/scancel"),
        "cluster": "cluster-1",
        "readonly_paths": [str(runtime)],
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

    assert policy.workspace == tmp_path / "site/workspace"
    assert policy.root == tmp_path / "site"
    assert policy.requests == tmp_path / "site/exchange/requests"
    assert policy.responses == tmp_path / "site/exchange/responses"
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


def test_runtime_policy_document_round_trips_frozen_configuration(tmp_path: Path) -> None:
    document = _document(tmp_path)
    profiles = document["profiles"]
    assert isinstance(profiles, dict) and isinstance(profiles["cpu"], dict)
    profiles["cpu"].update(
        workers=4,
        prelude="module load approved\nexport SITE=yes",
        manager_command="approved-httk",
    )
    policy = load_policy(_write(tmp_path, document))
    serialized = policy_document(policy)
    round_trip = tmp_path / "round-trip.json"
    round_trip.write_text(json.dumps(serialized), encoding="utf-8")
    assert load_policy(round_trip) == policy
    assert policy.profile("cpu").workers == 4
    assert policy.profile("cpu").prelude == "module load approved\nexport SITE=yes"
    assert policy.profile("cpu").manager_command == "approved-httk"


def test_configuration_digest_covers_execution_policy_and_ignores_operations(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, _document(tmp_path)))
    digest = policy.configuration_digest("cpu")
    assert len(digest) == 64
    assert (
        replace(
            policy,
            authorized_keys=("ed25519:" + base64.b64encode(bytes(reversed(range(32)))).decode("ascii"),),
            max_records=2048,
            max_submissions=64,
            poll_seconds=2.0,
            command_timeout=15.0,
            max_output_bytes=32768,
            request_max_age=7200,
        ).configuration_digest("cpu")
        == digest
    )
    for changed in (
        replace(policy, workspace_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        replace(policy, bwrap=policy.bwrap.with_name("other-bwrap")),
        replace(policy, readonly_paths=(policy.readonly_paths[0], tmp_path / "extra-runtime")),
        replace(policy, profiles=(replace(policy.profile("cpu"), workers=2), *policy.profiles[1:])),
        replace(policy, profiles=(replace(policy.profile("cpu"), prelude="module load other"), *policy.profiles[1:])),
        replace(
            policy, profiles=(replace(policy.profile("cpu"), manager_command="approved-httk"), *policy.profiles[1:])
        ),
    ):
        assert changed.configuration_digest("cpu") != digest


def test_resources_are_optional_and_sanity_limits_are_not_a_load_concern(tmp_path: Path) -> None:
    document = _document(tmp_path)
    profiles = document["profiles"]
    assert isinstance(profiles, dict)
    profiles["bare"] = {}
    profiles["huge"] = {"cpus": 4096, "memory_mb": 2 * 1024 * 1024, "time_minutes": 20_000}
    policy = load_policy(_write(tmp_path, document))
    bare = policy.profile("bare")
    assert bare.cpus is bare.memory_mb is bare.time_minutes is None
    assert policy.profile("huge").cpus == 4096
    serialized = policy_document(policy)
    serialized_profiles = serialized["profiles"]
    assert isinstance(serialized_profiles, dict)
    assert not {"cpus", "memory_mb", "time_minutes"} & set(serialized_profiles["bare"])
    round_trip = tmp_path / "round-trip.json"
    round_trip.write_text(json.dumps(serialized), encoding="utf-8")
    assert load_policy(round_trip) == policy
    assert policy.configuration_digest("bare") != policy.configuration_digest("cpu")


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
        ("format_version", 1),
        ("format_version", 2),
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
        ("cpus", 2**31),
        ("memory_mb", 0),
        ("memory_mb", 2**31),
        ("time_minutes", 0),
        ("time_minutes", 2**31),
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("workers", 0),
        ("workers", True),
        ("workers", 1025),
        ("prelude", 1),
        ("prelude", "bad\0prelude"),
        ("manager_command", ""),
        ("manager_command", "  \t"),
        ("manager_command", "bad\0command"),
    ],
)
def test_compiled_manager_fields_are_strict(tmp_path: Path, field: str, value: object) -> None:
    document = _document(tmp_path)
    profiles = document["profiles"]
    assert isinstance(profiles, dict) and isinstance(profiles["cpu"], dict)
    profiles["cpu"][field] = value
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


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("exchange", "elsewhere/exchange", "siblings in a dedicated directory"),
        ("exchange", "site/workspace", "siblings in a dedicated directory"),
        ("state", "site/workspace/state", "disjoint"),
        ("state", "site", "disjoint"),
        ("snapshots", "site/snapshots", "disjoint"),
        ("snapshots", "state/inside", "state and snapshots must be disjoint"),
    ],
)
def test_layout_rules_require_a_dedicated_disjoint_parent(tmp_path: Path, field: str, value: str, message: str) -> None:
    document = _document(tmp_path)
    document[field] = str(tmp_path / value)
    with pytest.raises(ValueError, match=message):
        load_policy(_write(tmp_path, document))


def test_daemon_parent_cannot_be_the_filesystem_root(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document["workspace"], document["exchange"] = "/httk-test-workspace", "/httk-test-exchange"
    with pytest.raises(ValueError, match="filesystem root"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("runtime", ["site", "site/lib", "site/workspace/lib"])
def test_daemon_parent_must_be_disjoint_from_runtime_paths(tmp_path: Path, runtime: str) -> None:
    document = _document(tmp_path)
    document["readonly_paths"] = [str(tmp_path / runtime), *document["readonly_paths"]]  # type: ignore[misc]
    with pytest.raises(ValueError, match="daemon parent .* runtime path"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize(
    ("runtime", "message"),
    [
        ("site/workspace/escape", "daemon parent .* runtime path"),
        ("state/escape", "runtime path .* must be disjoint from state"),
        ("snapshots/escape", "runtime path .* must be disjoint from snapshots"),
        ("/", "filesystem root"),
    ],
)
def test_runtime_roots_cannot_overlap_mutable_roots_or_be_root(tmp_path: Path, runtime: str, message: str) -> None:
    document = _document(tmp_path)
    document["readonly_paths"] = [*document["readonly_paths"], str(tmp_path / runtime)]  # type: ignore[misc]
    with pytest.raises(ValueError, match=message):
        load_policy(_write(tmp_path, document))


def test_policy_has_no_broker_paths(tmp_path: Path) -> None:
    document = _document(tmp_path)
    policy = load_policy(_write(tmp_path, document))
    assert "broker_paths" not in policy_document(policy)
    assert policy_document(policy)["format_version"] == 3
    assert not hasattr(policy, "broker_paths")
    document["broker_paths"] = [str(tmp_path / "broker")]
    with pytest.raises(ValueError, match="fields are missing or unknown"):
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


def test_only_python_must_be_covered_by_readonly_paths(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document["python"] = str(tmp_path / "unapproved/python")
    with pytest.raises(ValueError, match="python must be within readonly_paths"):
        load_policy(_write(tmp_path, document))
    # The broker and allocation service see the host read-only; bwrap runs on the host.
    document = _document(tmp_path)
    for field in ("sbatch", "squeue", "scancel", "bwrap"):
        document[field] = str(tmp_path / "unapproved" / field)
    document["slurm_conf"] = str(tmp_path / "unapproved/slurm.conf")
    assert load_policy(_write(tmp_path, document)).sbatch == tmp_path / "unapproved/sbatch"


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
    with pytest.raises(ValueError, match="siblings"):
        Policy(
            workspace=tmp_path / "site/workspace",
            workspace_id="12345678-1234-1234-1234-123456789abc",
            enrollment_id="0" * 32,
            exchange=tmp_path / "site/workspace/exchange",
            state=tmp_path / "state",
            snapshots=tmp_path / "snapshots",
            bwrap=tmp_path / "runtime/bwrap",
            python=tmp_path / "runtime/python",
            sbatch=tmp_path / "broker/sbatch",
            squeue=tmp_path / "broker/squeue",
            scancel=tmp_path / "broker/scancel",
            cluster="cluster",
            readonly_paths=(tmp_path / "runtime",),
            profiles=(),
        )


def test_gres_and_reservation_round_trip_only_when_set(tmp_path: Path) -> None:
    document = _document(tmp_path)
    profiles = document["profiles"]
    assert isinstance(profiles, dict)
    profiles["gpu"] = {"gres": "gpu:a100=2", "reservation": "maint.1"}
    policy = load_policy(_write(tmp_path, document))
    assert (policy.profile("gpu").gres, policy.profile("gpu").reservation) == ("gpu:a100=2", "maint.1")
    rendered = policy_document(policy)["profiles"]
    assert isinstance(rendered, dict)
    assert "gres" not in rendered["cpu"] and "reservation" not in rendered["cpu"]
    assert load_policy(_write(tmp_path, policy_document(policy))) == policy
    with pytest.raises(ValueError, match="invalid gres"):
        Profile("gpu", gres="gpu a100")
    with pytest.raises(ValueError, match="invalid reservation"):
        Profile("gpu", reservation="-bad")


def _layout_policy(tmp_path: Path) -> Policy:
    (tmp_path / "site/workspace/.httk-workspace/exchange").mkdir(parents=True)
    (tmp_path / "site/exchange").mkdir()
    return load_policy(_write(tmp_path, _document(tmp_path)))


def test_check_layout_accepts_the_dedicated_parent_and_leaves_no_probe(tmp_path: Path) -> None:
    policy = _layout_policy(tmp_path)
    check_layout(policy)
    assert list((tmp_path / "site/exchange").iterdir()) == []
    assert list((tmp_path / "site/workspace/.httk-workspace/exchange").iterdir()) == []


def test_check_layout_refuses_an_extra_parent_entry(tmp_path: Path) -> None:
    policy = _layout_policy(tmp_path)
    (tmp_path / "site/.hidden").touch()
    with pytest.raises(ValueError, match="must contain only") as refusal:
        check_layout(policy)
    assert ".hidden" in str(refusal.value)


def test_check_layout_refuses_symlinked_components(tmp_path: Path) -> None:
    policy = _layout_policy(tmp_path)
    staging = tmp_path / "site/workspace/.httk-workspace/exchange"
    staging.rmdir()
    staging.symlink_to(tmp_path / "site/exchange")
    with pytest.raises(OSError) as refusal:
        check_layout(policy)
    assert refusal.value.errno in (errno.ELOOP, errno.ENOTDIR)


def _failing_rename(code: int) -> Any:
    def rename(*_args: object, **_kwargs: object) -> None:
        raise OSError(code, os.strerror(code))

    return rename


@pytest.mark.parametrize(
    ("code", "message"),
    [
        (errno.EXDEV, "renameable into each other"),
        (errno.EACCES, "cannot rename the layout probe .*Permission denied"),
    ],
)
def test_check_layout_refuses_failed_renames_and_cleans_the_probe(
    code: int, message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module: Any = importlib.import_module("httk.workflow._daemon_policy")
    policy = _layout_policy(tmp_path)
    monkeypatch.setattr(module.os, "rename", _failing_rename(code))
    with pytest.raises(ValueError, match=message):
        check_layout(policy)
    assert list((tmp_path / "site/exchange").iterdir()) == []


def test_check_layout_probe_renames_once_and_leaves_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module: Any = importlib.import_module("httk.workflow._daemon_policy")
    policy = _layout_policy(tmp_path)
    real = module.os.rename
    calls: list[str] = []

    def recording(source: str, destination: str, **kwargs: object) -> None:
        calls.append(source)
        real(source, destination, **kwargs)

    monkeypatch.setattr(module.os, "rename", recording)
    check_layout(policy)
    assert len(calls) == 1 and calls[0].startswith(".probe-")
    assert list((tmp_path / "site/exchange").iterdir()) == []
    assert list((tmp_path / "site/workspace/.httk-workspace/exchange").iterdir()) == []


@pytest.mark.parametrize("tamper", ["replaced", "removed"])
def test_check_layout_detects_a_tampered_probe_and_never_moves_foreign_entries(
    tamper: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module: Any = importlib.import_module("httk.workflow._daemon_policy")
    policy = _layout_policy(tmp_path)
    staging = tmp_path / "site/workspace/.httk-workspace/exchange"
    real = module.os.rename
    renames: list[str] = []

    def rename_then_tamper(source: str, destination: str, **kwargs: object) -> None:
        real(source, destination, **kwargs)
        renames.append(destination)
        (staging / destination).unlink()
        if tamper == "replaced":
            (staging / destination).mkdir()
            (staging / destination / "payload").touch()

    monkeypatch.setattr(module.os, "rename", rename_then_tamper)
    with pytest.raises(ValueError, match="layout probe was tampered with"):
        check_layout(policy)
    assert len(renames) == 1
    assert list((tmp_path / "site/exchange").iterdir()) == []
    if tamper == "replaced":
        assert (staging / renames[0] / "payload").is_file()


def test_check_layout_without_probe_still_checks_the_parent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module: Any = importlib.import_module("httk.workflow._daemon_policy")
    policy = _layout_policy(tmp_path)
    monkeypatch.setattr(module.os, "rename", lambda *_args, **_kwargs: pytest.fail("probe must not run"))
    check_layout(policy, probe=False)
    (tmp_path / "site/extra").mkdir()
    with pytest.raises(ValueError, match="must contain only"):
        check_layout(policy, probe=False)


@pytest.mark.parametrize("destination", ["/daemon-policy.json", "/workspace/lib", "/tmp", "/"])
def test_runtime_paths_cannot_overlap_payload_destinations(tmp_path: Path, destination: str) -> None:
    document = _document(tmp_path)
    document["readonly_paths"] = [*document["readonly_paths"], destination]  # type: ignore[misc]
    with pytest.raises(ValueError, match="reserved sandbox destinations|filesystem root"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("destination", ["/daemon-root", "/control/lib"])
def test_former_broker_destinations_are_no_longer_reserved(tmp_path: Path, destination: str) -> None:
    document = _document(tmp_path)
    document["readonly_paths"] = [*document["readonly_paths"], destination]  # type: ignore[misc]
    assert Path(destination) in load_policy(_write(tmp_path, document)).readonly_paths


def test_check_layout_reports_a_missing_staging_directory(tmp_path: Path) -> None:
    policy = _layout_policy(tmp_path)
    (tmp_path / "site/workspace/.httk-workspace/exchange").rmdir()
    with pytest.raises(ValueError, match="staging directory .* is missing; .*--reload"):
        check_layout(policy)


def test_isolate_network_is_written_only_when_disabled_and_binds_the_digest(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, _document(tmp_path)))
    assert policy.isolate_network is True
    # Default documents and digests are those of enrollments made before the setting existed.
    assert "isolate_network" not in policy_document(policy)
    assert replace(policy, isolate_network=True).configuration_digest("cpu") == policy.configuration_digest("cpu")
    disabled = replace(policy, isolate_network=False)
    assert policy_document(disabled)["isolate_network"] is False
    assert disabled.configuration_digest("cpu") != policy.configuration_digest("cpu")
    assert load_policy(_write(tmp_path, policy_document(disabled))) == disabled
    for value in (True, 0, "false", None):
        with pytest.raises(ValueError, match="isolate_network may only be present as false"):
            load_policy(_write(tmp_path, {**_document(tmp_path), "isolate_network": value}))
    with pytest.raises(ValueError, match="isolate_network must be a boolean"):
        replace(policy, isolate_network=0)  # type: ignore[arg-type]
