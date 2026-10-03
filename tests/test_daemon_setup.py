"""Local approval, publication, and reload for the workspace daemon."""

import base64
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _daemon_cli, _daemon_policy, _daemon_setup
from httk.workflow._daemon_policy import load_policy
from httk.workflow._daemon_state import Ledger
from httk.workflow.launchers import add_launcher
from httk.workflow.projects import initialize_project
from httk.workflow.workspace import Workspace

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


@dataclass(slots=True)
class Layout:
    project: Path
    workspace: Workspace
    policy: Path
    document: dict[str, object]
    runtime: Path
    broker: Path


def _executable(path: Path, body: str = "#!/bin/sh\nexit 0\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def _write_policy(layout: Layout) -> None:
    layout.policy.write_text(json.dumps(layout.document), encoding="utf-8")
    layout.policy.chmod(0o600)


def _layout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Layout:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    _executable(runtime / "bwrap")
    for name in ("sbatch", "squeue", "scancel"):
        _executable(broker / name)
    monkeypatch.setenv("PATH", f"{broker}:{os.environ['PATH']}")
    project = tmp_path / "project"
    initialize_project(project, name="daemon-setup")
    workspace = Workspace.initialize(project / "workspace")
    workspace.set_setting("slurm.cpus_per_task", "2")
    workspace.set_setting("slurm.mem", "1025K")
    workspace.set_setting("slurm.time_limit", "0:01")
    workspace.set_setting("manager.workers", "2")
    workspace.set_setting("environment.prelude", "module load workspace")
    add_launcher(
        "small",
        template="slurm",
        settings={"slurm.partition": "short", "manager.workers": "3"},
        project=project,
    )
    add_launcher(
        "large",
        template="slurm",
        settings={"slurm.cpus_per_task": "8", "slurm.mem": "2G", "slurm.time_limit": "1:00:01"},
        project=project,
    )
    policy = tmp_path / "operator.json"
    document: dict[str, object] = {
        "format": "httk-workspace-daemon-policy",
        "format_version": 2,
        "workspace": str(workspace.root),
        "readonly_paths": [str(runtime), str(Path(sys.executable).absolute().parent.parent)],
        "broker_paths": [str(broker)],
        "authorized_keys": [AUTHORIZED_KEY],
        "allowed_launchers": ["small", "large"],
        "bwrap": str(runtime / "bwrap"),
        "sbatch": str(broker / "sbatch"),
        "squeue": str(broker / "squeue"),
        "scancel": str(broker / "scancel"),
        "cluster": "test-cluster",
    }
    layout = Layout(project, workspace, policy, document, runtime, broker)
    _write_policy(layout)
    return layout


def test_initialize_compiles_named_launchers_and_freezes_workspace_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    snapshot = _daemon_setup.initialize(layout.workspace.root, layout.policy)
    policy = load_policy(snapshot)
    small = policy.profile("small")
    large = policy.profile("large")
    assert (small.cpus, small.memory_mb, small.time_minutes, small.workers) == (2, 2, 1, 3)
    assert (large.cpus, large.memory_mb, large.time_minutes, large.workers) == (8, 2048, 61, 2)
    assert small.partition == "short"
    assert small.prelude == "module load workspace"
    assert policy.python == Path(sys.executable).absolute()
    assert policy.requests == layout.workspace.root.with_name("workspace.daemon-requests")
    assert policy.responses == layout.workspace.root.with_name("workspace.daemon-responses")
    assert snapshot.parent == layout.policy.with_suffix(".daemon")
    assert policy.state.parent.name == "workspace-daemons"
    assert policy.state.stat().st_mode & 0o777 == 0o700
    assert snapshot.parent.stat().st_mode & 0o777 == 0o700

    layout.workspace.set_setting("slurm.cpus_per_task", "16")
    assert load_policy(_daemon_setup.active_policy_path(layout.workspace.root, layout.policy)) == policy


def test_workspace_alias_initializes_and_hands_canonical_path_to_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    alias = tmp_path / "workspace-alias"
    alias.symlink_to(layout.workspace.root, target_is_directory=True)
    snapshot = _daemon_setup.initialize(alias, layout.policy)
    observed: list[object] = []

    monkeypatch.setattr(_daemon_cli, "_close_inherited", lambda: None)
    monkeypatch.setattr(_daemon_cli.os, "chdir", lambda _path: None)
    monkeypatch.setattr(_daemon_cli.os, "dup2", lambda *_args, **_kwargs: None)

    def refuse_exec(executable: str, argv: list[str], environment: dict[str, str]) -> None:
        observed.extend([executable, argv, environment])
        raise OSError("inspection stop")

    monkeypatch.setattr(_daemon_cli.os, "execve", refuse_exec)
    assert _daemon_cli.command([str(alias), "--policy", str(layout.policy), "--once"], program="httk") == 2
    argv = observed[1]
    assert isinstance(argv, list)
    assert argv[argv.index("--workspace") + 1] == str(layout.workspace.root)
    assert argv[argv.index("--policy") + 1] == str(snapshot)


def test_relative_cli_paths_initialize_and_export_from_inside_the_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    monkeypatch.chdir(layout.workspace.root)
    relative_policy = os.path.relpath(layout.policy)
    assert relative_policy.startswith("..")
    assert _daemon_cli.command([".", "--policy", relative_policy, "--initialize"], program="httk") == 0
    assert _daemon_cli.command([".", "--policy", relative_policy, "--export-endpoint"], program="httk") == 0
    assert json.loads(capsys.readouterr().out) == _daemon_setup.export_endpoint(layout.workspace.root, layout.policy)


def test_project_launcher_shadows_global_and_approval_never_executes_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    add_launcher(
        "shadow",
        template="slurm",
        global_=True,
        settings={"slurm.cpus_per_task": "32", "slurm.mem": "4G", "slurm.time_limit": "60"},
    )
    local = add_launcher(
        "shadow",
        template="slurm",
        project=layout.project,
        settings={"slurm.cpus_per_task": "4", "slurm.mem": "1G", "slurm.time_limit": "30"},
    )
    marker = tmp_path / "executed"
    _executable(local / "launcher", f"#!/bin/sh\ntouch {marker}\nexit 99\n")
    layout.document["allowed_launchers"] = ["shadow"]
    _write_policy(layout)
    policy = load_policy(_daemon_setup.initialize(layout.workspace.root, layout.policy))
    assert policy.profile("shadow").cpus == 4
    assert not marker.exists()


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2", 2),
        ("2:01", 3),
        ("1:02:01", 63),
        ("1-2", 1560),
        ("1-2:03", 1563),
        ("1-2:03:01", 1564),
    ],
)
def test_slurm_time_forms_normalize_to_bounded_minutes(text: str, expected: int) -> None:
    assert _daemon_setup._time_minutes(text) == expected


@pytest.mark.parametrize("value", [True, 1.0, 0, "0", "UNLIMITED", "1:60", "1-24", float("inf")])
def test_slurm_time_refuses_noncanonical_or_unbounded_values(value: object) -> None:
    with pytest.raises(ValueError):
        _daemon_setup._time_minutes(value)


def test_mpi_geometry_defaults_and_explicit_placement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path, monkeypatch)
    _executable(layout.broker / "srun")
    control = tmp_path / "mpi-control"
    control.mkdir()
    add_launcher(
        "mpi",
        template="slurm",
        project=layout.project,
        settings={
            "slurm.cpus_per_task": "4",
            "slurm.mem": "8G",
            "slurm.time_limit": "30",
            "slurm.nodes": "2",
            "slurm.ntasks_per_node": "3",
            "slurm.mpi": "pmix",
            "manager.workers": "1",
        },
    )
    layout.document["allowed_launchers"] = ["mpi"]
    layout.document["mpi"] = {"srun": str(layout.broker / "srun"), "control_root": str(control)}
    _write_policy(layout)
    profile = load_policy(_daemon_setup.initialize(layout.workspace.root, layout.policy)).profile("mpi")
    assert profile.mpi is not None
    assert (profile.mpi.nodes, profile.mpi.ranks, profile.mpi.ntasks_per_node) == (2, 6, 3)
    assert profile.workers == 1


def test_unsupported_or_incomplete_launcher_settings_fail_before_state_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    settings = layout.workspace.read_settings()
    layout.workspace.unset_setting("slurm.mem")
    layout.workspace.set_setting("slurm.gres", "gpu:1")
    layout.document["allowed_launchers"] = ["small"]
    _write_policy(layout)
    with pytest.raises(ValueError, match="unsupported Slurm"):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)
    state = _daemon_setup._decode_operator(layout.policy).state
    assert not state.exists()
    assert settings["slurm.mem"] == "1025K"


def test_duplicate_and_oversized_launcher_metadata_are_bounded_before_construction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    metadata = layout.project / "httk_project" / "launchers" / "small" / "launcher.json"
    metadata.write_text('{"format":"httk-manager-launcher","format":"duplicate"}', encoding="utf-8")
    layout.document["allowed_launchers"] = ["small"]
    _write_policy(layout)
    with pytest.raises(ValueError, match="invalid launcher metadata JSON"):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)
    metadata.write_text("{" + '"padding":"' + "x" * (64 * 1024) + '"}', encoding="utf-8")
    with pytest.raises(ValueError, match="too large"):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)


def test_oversized_compiled_policy_fails_without_enrollment_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    layout.workspace.set_setting("environment.prelude", "x" * (64 * 1024 - 1024))
    layout.document["allowed_launchers"] = ["small"]
    _write_policy(layout)
    operator = _daemon_setup._decode_operator(layout.policy)
    with pytest.raises(ValueError, match="compiled runtime policy"):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)
    assert not operator.state.exists()
    assert not operator.snapshot_root.exists()


def test_exact_runtime_policy_size_limit_round_trips_without_an_extra_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    operator = _daemon_setup._decode_operator(layout.policy)
    compiled = _daemon_setup._compile(operator, "0" * 32)
    canonical = _daemon_setup._canonical_bytes(_daemon_setup.policy_document(compiled))
    monkeypatch.setattr(_daemon_setup, "_MAX_RUNTIME_POLICY_BYTES", len(canonical))
    monkeypatch.setattr(_daemon_policy, "MAX_POLICY_BYTES", len(canonical))
    snapshot = _daemon_setup.initialize(layout.workspace.root, layout.policy)
    assert snapshot.stat().st_size == len(canonical)
    assert load_policy(snapshot).workspace_id == compiled.workspace_id


def test_partial_initialization_is_preserved_and_never_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path, monkeypatch)

    def fail_snapshot(_path: Path, _data: bytes) -> None:
        raise OSError("injected snapshot failure")

    monkeypatch.setattr(_daemon_setup, "_write_exclusive", fail_snapshot)
    with pytest.raises(OSError, match="injected"):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)
    operator = _daemon_setup._decode_operator(layout.policy)
    assert (operator.state / "ledger.sqlite3").is_file()
    assert (operator.state / "response.seed").is_file()
    assert not (operator.state / "active.json").exists()
    with pytest.raises(FileExistsError):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)


def test_interrupted_snapshot_write_never_leaves_a_partial_hash_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshots"
    root.mkdir(mode=0o700)
    destination = root / "snapshot.json"
    original_write = _daemon_setup.os.write
    calls = 0

    def fail_after_prefix(descriptor: int, data: bytes) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return original_write(descriptor, data[:3])
        raise OSError("injected snapshot write failure")

    monkeypatch.setattr(_daemon_setup.os, "write", fail_after_prefix)
    with pytest.raises(OSError, match="injected"):
        _daemon_setup._write_exclusive(destination, b"complete-policy")
    assert not destination.exists()
    assert list(root.iterdir()) == []


@pytest.mark.parametrize("failure", ["before_install", "after_install"])
def test_reload_snapshot_failure_keeps_old_active_and_retry_recovers(
    failure: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    old_snapshot = _daemon_setup.initialize(layout.workspace.root, layout.policy)
    layout.workspace.set_setting("slurm.cpus_per_task", "16")
    original = _daemon_setup._write_exclusive

    def interrupted(path: Path, data: bytes) -> None:
        if failure == "after_install":
            original(path, data)
        raise OSError(f"injected {failure}")

    monkeypatch.setattr(_daemon_setup, "_write_exclusive", interrupted)
    with pytest.raises(OSError, match=failure):
        _daemon_setup.reload(layout.workspace.root, layout.policy)
    assert _daemon_setup.active_policy_path(layout.workspace.root, layout.policy) == old_snapshot
    monkeypatch.setattr(_daemon_setup, "_write_exclusive", original)
    new_snapshot = _daemon_setup.reload(layout.workspace.root, layout.policy)
    assert new_snapshot != old_snapshot
    assert load_policy(new_snapshot).profile("small").cpus == 16


def test_reload_preserves_history_updates_catalog_and_refuses_running_or_connection_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    first_path = _daemon_setup.initialize(layout.workspace.root, layout.policy)
    first = load_policy(first_path)
    layout.workspace.set_setting("slurm.cpus_per_task", "16")
    layout.document["allowed_launchers"] = ["small"]
    layout.document["authorized_keys"] = ["ed25519:" + base64.b64encode(bytes(reversed(range(32)))).decode("ascii")]
    _write_policy(layout)
    second_path = _daemon_setup.reload(layout.workspace.root, layout.policy)
    second = load_policy(second_path)
    assert first_path.is_file() and second_path != first_path
    assert second.enrollment_id == first.enrollment_id
    assert second.authorized_keys != first.authorized_keys
    assert {profile.name for profile in second.profiles} == {"small"}

    with (
        Ledger(
            second.state,
            second.workspace_id,
            second.enrollment_id,
            max_records=second.max_records,
            max_submissions=second.max_submissions,
        ),
        pytest.raises(OSError, match="locked"),
    ):
        _daemon_setup.reload(layout.workspace.root, layout.policy)

    active_before = _daemon_setup.active_policy_path(layout.workspace.root, layout.policy)
    layout.document["cluster"] = "other-cluster"
    _write_policy(layout)
    with pytest.raises(ValueError, match="scheduler connection"):
        _daemon_setup.reload(layout.workspace.root, layout.policy)
    assert _daemon_setup.active_policy_path(layout.workspace.root, layout.policy) == active_before


def test_export_is_exact_and_remains_readable_while_broker_lock_is_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    policy = load_policy(_daemon_setup.initialize(layout.workspace.root, layout.policy))
    with Ledger(
        policy.state,
        policy.workspace_id,
        policy.enrollment_id,
        max_records=policy.max_records,
        max_submissions=policy.max_submissions,
    ):
        exported = _daemon_setup.export_endpoint(layout.workspace.root, layout.policy)
    assert set(exported) == {
        "format",
        "format_version",
        "workspace_id",
        "enrollment_id",
        "daemon_public_key",
        "configurations",
        "request_max_age",
    }
    assert exported["format"] == "httk-workspace-daemon-endpoint"
    assert exported["configurations"] == {name: policy.configuration_digest(name) for name in ("small", "large")}
    assert str(exported["daemon_public_key"]).startswith("ed25519:")


def test_policy_inside_writable_workspace_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path, monkeypatch)
    inside = layout.workspace.root / "operator.json"
    layout.policy = inside
    _write_policy(layout)
    with pytest.raises(ValueError, match="outside workspace"):
        _daemon_setup.initialize(layout.workspace.root, inside)


def test_operator_policy_version_requires_an_integer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path, monkeypatch)
    layout.document["format_version"] = 2.0
    _write_policy(layout)
    with pytest.raises(ValueError, match="format or version"):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)


@pytest.mark.parametrize("field", ["poll_seconds", "command_timeout", "mpi.termination_grace"])
def test_huge_numeric_limits_are_clean_cli_refusals_before_artifacts(
    field: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    huge = 10**400
    if field.startswith("mpi."):
        _executable(layout.broker / "srun")
        control = tmp_path / "mpi-control"
        control.mkdir()
        layout.document["mpi"] = {
            "srun": str(layout.broker / "srun"),
            "control_root": str(control),
            "termination_grace": huge,
        }
    else:
        layout.document[field] = huge
    _write_policy(layout)
    state = _daemon_setup._state_default(layout.workspace.root)
    assert (
        _daemon_cli.command(
            [str(layout.workspace.root), "--policy", str(layout.policy), "--initialize"], program="httk"
        )
        == 2
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert field.rsplit(".", 1)[-1] in captured.err
    assert not state.exists()


def test_slurm_conf_without_cluster_uses_bounded_scontrol_with_fixed_conf(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    slurm_conf = layout.broker / "slurm.conf"
    slurm_conf.write_text("Include extra.conf\n", encoding="utf-8")
    log = tmp_path / "scontrol.log"
    _executable(
        layout.broker / "scontrol",
        f"#!/bin/sh\nprintf '%s' \"$SLURM_CONF\" > {log}\nprintf 'ClusterName = discovered-cluster\\n'\n",
    )
    layout.document.pop("cluster")
    layout.document["slurm_conf"] = str(slurm_conf)
    layout.document["scontrol"] = str(layout.broker / "scontrol")
    _write_policy(layout)
    policy = load_policy(_daemon_setup.initialize(layout.workspace.root, layout.policy))
    assert policy.cluster == "discovered-cluster"
    assert log.read_text(encoding="utf-8") == str(slurm_conf)


def test_missing_path_discovered_tool_and_manager_command_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path, monkeypatch)
    layout.document.pop("bwrap")
    _write_policy(layout)
    monkeypatch.setenv("PATH", str(layout.broker))
    with pytest.raises(ValueError, match="bwrap"):
        _daemon_setup.initialize(layout.workspace.root, layout.policy)

    layout.document["bwrap"] = str(layout.runtime / "bwrap")
    metadata = layout.project / "httk_project" / "launchers" / "small" / "launcher.json"
    value = json.loads(metadata.read_text(encoding="utf-8"))
    value["settings"]["manager.command"] = "module-provided-httk"
    metadata.write_text(json.dumps(value), encoding="utf-8")
    _write_policy(layout)
    monkeypatch.setenv("PATH", f"{layout.broker}:{os.environ['PATH']}")
    policy = load_policy(_daemon_setup.initialize(layout.workspace.root, layout.policy))
    assert policy.profile("small").manager_command == "module-provided-httk"
