"""Strict validation for the workspace daemon policy."""

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

from httk.workflow._daemon_policy import ApprovedLauncher, Policy, check_layout, load_policy, policy_document

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")
DIGEST = "0123456789abcdef" * 4


def _document(tmp_path: Path) -> dict[str, object]:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    return {
        "format": "httk-workspace-daemon-policy",
        "format_version": 4,
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
        "authorized_keys": [AUTHORIZED_KEY],
        "launchers": {
            "cpu": {
                "settings": {"manager.confine": "bwrap", "slurm.cpus_per_task": "8", "slurm.mem": "16G"},
                "digest": DIGEST,
            },
            "long-1": {
                "settings": {
                    "manager.confine": "bwrap",
                    "slurm.partition": "compute.1",
                    "slurm.time_limit": 1440,
                    "environment.prelude": "module load approved\nexport SITE=yes",
                    "manager.workers": None,
                    "confine.isolate_network": 0.0,
                },
                "digest": "f" * 64,
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
    assert policy.jobs == tmp_path / "snapshots/jobs"
    assert policy.max_records == 4096
    assert policy.max_submissions == 128
    assert policy.poll_seconds == 1.0
    assert policy.command_timeout == 30.0
    assert policy.max_output_bytes == 65536
    assert policy.request_max_age == 3600
    assert policy.authorized_keys == (AUTHORIZED_KEY,)
    assert policy.launcher("cpu") == ApprovedLauncher(
        "cpu", (("manager.confine", "bwrap"), ("slurm.cpus_per_task", "8"), ("slurm.mem", "16G")), DIGEST
    )
    assert dict(policy.launcher("long-1").settings)["manager.workers"] is None
    with pytest.raises(ValueError, match="unknown daemon launcher"):
        policy.launcher("missing")


def test_runtime_policy_document_round_trips_frozen_launchers(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, _document(tmp_path)))
    serialized = policy_document(policy)
    assert serialized["format_version"] == 4
    round_trip = tmp_path / "round-trip.json"
    round_trip.write_text(json.dumps(serialized), encoding="utf-8")
    assert load_policy(round_trip) == policy
    settings = dict(policy.launcher("long-1").settings)
    assert settings["environment.prelude"] == "module load approved\nexport SITE=yes"
    assert settings["slurm.time_limit"] == 1440 and settings["confine.isolate_network"] == 0.0


def test_configuration_digest_binds_the_launcher_and_the_submission_identity(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, _document(tmp_path)))
    digest = policy.configuration_digest("cpu")
    assert len(digest) == 64 and digest != policy.configuration_digest("long-1")
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
            bwrap=policy.bwrap.with_name("other-bwrap"),
            squeue=policy.squeue.with_name("other-squeue"),
            scancel=policy.scancel.with_name("other-scancel"),
            sacct=Path("/usr/bin/sacct"),
            launchers=(policy.launcher("cpu"),),
        ).configuration_digest("cpu")
        == digest
    )
    other_launcher = replace(policy.launcher("cpu"), digest="e" * 64)
    for changed in (
        replace(policy, workspace_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        replace(policy, python=policy.python.with_name("other-python")),
        replace(policy, sbatch=policy.sbatch.with_name("other-sbatch")),
        replace(policy, cluster="cluster-2"),
        replace(policy, slurm_conf=Path("/etc/slurm/slurm.conf")),
        replace(policy, launchers=(other_launcher, policy.launcher("long-1"))),
    ):
        assert changed.configuration_digest("cpu") != digest
    renamed = replace(policy, launchers=(replace(policy.launcher("cpu"), name="cpu2"),))
    assert renamed.configuration_digest("cpu2") != digest


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"settings": {}}, "exactly settings and digest"),
        ({"settings": {}, "digest": DIGEST, "extra": 1}, "exactly settings and digest"),
        ({"settings": [], "digest": DIGEST}, "settings must be an object"),
        ({"settings": {}, "digest": "ABC"}, "invalid launcher digest"),
        ({"settings": {"bad key": "x"}, "digest": DIGEST}, "invalid launcher setting name"),
        ({"settings": {"slurm.mem": True}, "digest": DIGEST}, "JSON scalar"),
        ({"settings": {"slurm.mem": ["4G"]}, "digest": DIGEST}, "JSON scalar"),
        ({"settings": {"slurm.mem": "4\u0000G"}, "digest": DIGEST}, "JSON scalar"),
    ],
)
def test_launcher_entries_are_strict(tmp_path: Path, entry: object, message: str) -> None:
    document = _document(tmp_path)
    document["launchers"] = {"cpu": entry}
    with pytest.raises(ValueError, match=message):
        load_policy(_write(tmp_path, document))


def test_launcher_names_are_daemon_configuration_names(tmp_path: Path) -> None:
    document = _document(tmp_path)
    launchers = document["launchers"]
    assert isinstance(launchers, dict)
    launchers["Bad Name"] = launchers.pop("cpu")
    with pytest.raises(ValueError, match="invalid launcher name"):
        load_policy(_write(tmp_path, document))
    with pytest.raises(ValueError, match="sorted with unique keys"):
        ApprovedLauncher("cpu", (("slurm.mem", "1G"), ("manager.confine", "bwrap")), DIGEST)
    with pytest.raises(ValueError, match="sorted with unique keys"):
        ApprovedLauncher("cpu", (("slurm.mem", "1G"), ("slurm.mem", "2G")), DIGEST)
    with pytest.raises(ValueError, match="key/value pairs"):
        ApprovedLauncher("cpu", (("slurm.mem",),), DIGEST)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["workspace", "workspace_id", "launchers", "bwrap", "cluster"])
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
        ("format_version", 3),
        ("workspace_id", 7),
        ("launchers", []),
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


def test_policy_has_no_broker_paths_or_job_sandbox_settings(tmp_path: Path) -> None:
    document = _document(tmp_path)
    policy = load_policy(_write(tmp_path, document))
    for field in ("broker_paths", "readonly_paths", "isolate_network", "mpi", "profiles"):
        assert field not in policy_document(policy)
        assert not hasattr(policy, field)
        with pytest.raises(ValueError, match="fields are missing or unknown"):
            load_policy(_write(tmp_path, {**document, field: []}))


def test_broker_executables_need_no_approved_runtime_roots(tmp_path: Path) -> None:
    # The broker sees the host read-only and managers run unconfined, so no tool is tied to a mount list.
    document = _document(tmp_path)
    for field in ("python", "sbatch", "squeue", "scancel", "bwrap"):
        document[field] = str(tmp_path / "unapproved" / field)
    document["slurm_conf"] = str(tmp_path / "unapproved/slurm.conf")
    assert load_policy(_write(tmp_path, document)).python == tmp_path / "unapproved/python"


@pytest.mark.parametrize("field", ["workspace", "python", "slurm_conf"])
def test_paths_must_be_absolute_strings_without_parent_segments(tmp_path: Path, field: str) -> None:
    document = _document(tmp_path)
    document[field] = "/safe/../bad"
    with pytest.raises(ValueError, match="absolute path"):
        load_policy(_write(tmp_path, document))

    document = _document(tmp_path)
    document[field] = 7
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
        ApprovedLauncher("bad name", (), DIGEST)
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
            launchers=(),
        )
    policy = load_policy(_write(tmp_path, _document(tmp_path)))
    with pytest.raises(ValueError, match="launcher names must be unique"):
        replace(policy, launchers=(policy.launcher("cpu"), policy.launcher("cpu")))


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


def test_check_layout_reports_a_missing_staging_directory(tmp_path: Path) -> None:
    policy = _layout_policy(tmp_path)
    (tmp_path / "site/workspace/.httk-workspace/exchange").rmdir()
    with pytest.raises(ValueError, match="staging directory .* is missing; .*--reload"):
        check_layout(policy)


def test_sacct_is_written_only_when_set_and_never_binds_the_digest(tmp_path: Path) -> None:
    document = _document(tmp_path)
    policy = load_policy(_write(tmp_path, document))
    assert policy.sacct is None
    # Documents and digests of policies without sacct are those of enrollments made before the setting existed.
    assert "sacct" not in policy_document(policy)
    accounting = replace(policy, sacct=Path("/usr/bin/sacct"))
    assert policy_document(accounting) == {**policy_document(policy), "sacct": "/usr/bin/sacct"}
    assert accounting.configuration_digest("cpu") == policy.configuration_digest("cpu")
    assert load_policy(_write(tmp_path, policy_document(accounting))) == accounting
    for value in ("relative/sacct", None, 1):
        with pytest.raises(ValueError, match="sacct"):
            load_policy(_write(tmp_path, {**document, "sacct": value}))
