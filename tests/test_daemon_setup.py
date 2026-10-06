"""Enrollment, configuration, approval of slurm launchers, and activation for the workspace daemon."""

import base64
import errno
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _daemon_cli, _daemon_policy, _daemon_setup, launchers
from httk.workflow._daemon_activation import read_active_snapshot
from httk.workflow._daemon_keys import response_public_key, response_seed_path
from httk.workflow._daemon_policy import Policy, load_policy
from httk.workflow._daemon_protocol import Request
from httk.workflow._daemon_state import CapacityError, Ledger
from httk.workflow.configuration import launchers_home
from httk.workflow.launchers import add_launcher
from httk.workflow.projects import initialize_project
from httk.workflow.workspace import Workspace

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")
OTHER_KEY = "ed25519:" + base64.b64encode(bytes(reversed(range(32)))).decode("ascii")
CONFINED = {"manager.confine": "bwrap"}


@dataclass(slots=True)
class Layout:
    workspace: Workspace
    exchange: Path
    runtime: Path
    broker: Path
    options: list[tuple[str, str]]


def _executable(path: Path, body: str = "#!/bin/sh\nexit 0\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


@pytest.fixture(autouse=True)
def _host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLURM_CONF", raising=False)
    monkeypatch.delenv("SLURM_CLUSTER_NAME", raising=False)
    monkeypatch.setattr(_daemon_setup, "_DEFAULT_SLURM_CONF", tmp_path / "absent-etc-slurm" / "slurm.conf")
    # The slurm template requires sbatch on PATH when a bundle is added.
    for name in ("sbatch", "squeue", "scancel"):
        _executable(tmp_path / "broker" / name)
    monkeypatch.setenv("PATH", f"{tmp_path / 'broker'}:/usr/bin:/bin")


def _launcher(name: str, **settings: str) -> Path:
    return add_launcher(name, template="slurm", global_=True, settings={**CONFINED, **settings})


def _metadata_path(name: str) -> Path:
    return launchers_home() / name / "launcher.json"


def _rewrite_launcher(name: str, **settings: object) -> None:
    path = _metadata_path(name)
    value = json.loads(path.read_text(encoding="utf-8"))
    for key, setting in settings.items():
        if setting is None:
            value["settings"].pop(key, None)
        else:
            value["settings"][key] = setting
    path.write_text(json.dumps(value), encoding="utf-8")


def _layout(tmp_path: Path) -> Layout:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    _executable(runtime / "bwrap")
    workspace = Workspace.initialize(tmp_path / "site" / "workspace")
    options = [
        ("set", f"bwrap={runtime / 'bwrap'}"),
        ("set", f"sbatch={broker / 'sbatch'}"),
        ("set", f"squeue={broker / 'squeue'}"),
        ("set", f"scancel={broker / 'scancel'}"),
        ("set", "cluster=test-cluster"),
    ]
    layout = Layout(workspace, tmp_path / "site" / "exchange", runtime, broker, options)
    _launcher("small", **{"slurm.partition": "short", "manager.workers": "3"})
    _launcher(
        "large",
        **{"slurm.cpus_per_task": "8", "slurm.mem": "2G", "slurm.time_limit": "1:00:01", "slurm.gres": "gpu:2"},
    )
    return layout


def _initialize(
    layout: Layout,
    *names: str,
    changes: list[tuple[str, str]] | None = None,
    broker: list[tuple[str, str]] | None = None,
    state: Path | None = None,
    snapshots: Path | None = None,
) -> Path:
    added = [("add", f"launchers={name}") for name in names or ("small", "large")]
    return _daemon_setup.initialize(
        layout.workspace.root,
        exchange=layout.exchange,
        changes=[
            *(layout.options if broker is None else broker),
            *added,
            ("add", f"authorized_keys={AUTHORIZED_KEY}"),
            *(changes or []),
        ],
        state=state,
        snapshots=snapshots,
    )


def _state(layout: Layout) -> Path:
    return _daemon_setup._state_default(layout.workspace.root)


def _active(layout: Layout, state: Path | None = None) -> Path:
    return read_active_snapshot(_state(layout) if state is None else state)[0]


def _activate(layout: Layout, **options: Path) -> Path:
    return _daemon_setup.activate(layout.workspace.root, **options)[0]


def _configure(layout: Layout, *changes: tuple[str, str]) -> dict[str, object]:
    return _daemon_setup.configure(layout.workspace.root, changes)


def _stored_configuration(layout: Layout) -> dict[str, object]:
    return json.loads((_state(layout) / "configuration.json").read_bytes())


def _bundle_digest(name: str) -> str:
    metadata = json.loads((launchers_home() / name / "launcher.json").read_text(encoding="utf-8"))
    return hashlib.sha256(json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def test_initialize_freezes_slurm_launchers_and_publishes_the_endpoint(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    snapshot = _initialize(layout)
    policy = load_policy(snapshot)
    assert dict(policy.launcher("small").settings) == {**CONFINED, "slurm.partition": "short", "manager.workers": "3"}
    assert dict(policy.launcher("large").settings)["slurm.time_limit"] == "1:00:01"
    assert [launcher.name for launcher in policy.launchers] == ["large", "small"]
    assert [launcher.digest for launcher in policy.launchers] == [_bundle_digest("large"), _bundle_digest("small")]
    assert policy.exchange == layout.exchange and policy.root == tmp_path / "site"
    assert policy.state == _state(layout) and policy.state.parent.name == "workspace-daemons"
    assert policy.snapshots == policy.state.with_name(policy.state.name + ".snapshots") == snapshot.parent
    assert policy.authorized_keys == (AUTHORIZED_KEY,)
    assert (policy.bwrap, policy.sbatch, policy.cluster) == (
        layout.runtime / "bwrap",
        layout.broker / "sbatch",
        "test-cluster",
    )
    assert policy.max_submissions == 128
    assert policy.python.is_relative_to(Path(sys.prefix).resolve())
    for directory in (policy.state, policy.snapshots, policy.jobs, layout.exchange):
        assert directory.stat().st_mode & 0o777 == 0o700
    assert policy.jobs == policy.snapshots / "jobs"
    for name in ("requests", "responses", "inbox", "outbox", "outbox/rejected"):
        assert (layout.exchange / name).is_dir()
    for name in ("inbox", "outbox", "outbox/rejected", "records"):
        assert (layout.workspace.root / ".httk-workspace/exchange" / name).is_dir()
    # Setup, and only setup, enrolls the workspace: every manager on it must then be confined.
    marker = layout.workspace.root / ".httk-workspace/exchange/enrollment.json"
    assert json.loads(marker.read_bytes()) == {
        "format": "httk-workspace-daemon-enrollment",
        "format_version": 1,
        "enrollment_id": policy.enrollment_id,
        "workspace_id": layout.workspace.workspace_id,
    }
    assert marker.stat().st_mode & 0o777 == 0o600
    assert sorted(os.listdir(tmp_path / "site")) == ["exchange", "workspace"]

    raw = (layout.exchange / "endpoint.json").read_bytes()
    endpoint = json.loads(raw)
    assert raw.rstrip(b"\n") == json.dumps(endpoint, sort_keys=True, separators=(",", ":")).encode()
    assert endpoint == {
        "format": "httk-workspace-daemon-endpoint",
        "format_version": 2,
        "workspace_id": layout.workspace.workspace_id,
        "enrollment_id": policy.enrollment_id,
        "daemon_public_key": response_public_key(response_seed_path(policy.state)),
        "configurations": {name: policy.configuration_digest(name) for name in ("small", "large")},
        "request_max_age": 3600,
    }
    assert _active(layout) == snapshot
    configuration = _state(layout) / "configuration.json"
    assert configuration.stat().st_mode & 0o777 == 0o600
    assert _stored_configuration(layout) == {
        "format": "httk-workspace-daemon-configuration",
        "format_version": 1,
        "launchers": ["small", "large"],
        "authorized_keys": [AUTHORIZED_KEY],
        "bwrap": str(layout.runtime / "bwrap"),
        "python": str(policy.python),
        "sbatch": str(layout.broker / "sbatch"),
        "squeue": str(layout.broker / "squeue"),
        "scancel": str(layout.broker / "scancel"),
        "sacct": None,
        "slurm_conf": None,
        "max_submissions": 128,
        "force": False,
    }


def test_bundle_edits_take_effect_at_the_next_activation_without_another_command(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    first_snapshot = _initialize(layout)
    first = load_policy(first_snapshot)
    endpoint = (layout.exchange / "endpoint.json").read_bytes()
    _rewrite_launcher("small", **{"slurm.cpus_per_task": "16"})
    layout.workspace.set_setting("slurm.cpus_per_task", "32")
    assert _active(layout) == first_snapshot
    assert (layout.exchange / "endpoint.json").read_bytes() == endpoint
    snapshot, changed = _daemon_setup.activate(layout.workspace.root)
    assert snapshot != first_snapshot and _active(layout) == snapshot and changed == ("small",)
    second = load_policy(snapshot)
    assert dict(second.launcher("small").settings)["slurm.cpus_per_task"] == "16"
    assert second.launcher("small").digest != first.launcher("small").digest
    assert second.launcher("large") == first.launcher("large")
    configurations = json.loads((layout.exchange / "endpoint.json").read_text(encoding="utf-8"))["configurations"]
    assert configurations["small"] == second.configuration_digest("small") != first.configuration_digest("small")
    assert configurations["large"] == first.configuration_digest("large")
    # Nothing changed since: the next activation publishes nothing.
    assert _daemon_setup.activate(layout.workspace.root) == (snapshot, None)
    assert sorted(path.name for path in snapshot.parent.glob("*.json")) == sorted([first_snapshot.name, snapshot.name])


@pytest.mark.parametrize("kind", ["daemon", "pbs", None])
def test_only_slurm_launchers_are_approved(kind: str | None, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    path = _metadata_path("small")
    value = json.loads(path.read_text(encoding="utf-8"))
    if kind is None:
        del value["kind"]
    else:
        value["kind"] = kind
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match=f"is a {kind!r} launcher; .*only global slurm launchers.*--template slurm"):
        _initialize(layout, "small")
    assert not _state(layout).exists() and not layout.exchange.exists()


@pytest.mark.parametrize("confine", [None, "none"])
def test_approved_launchers_must_confine_their_managers(confine: str | None, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"manager.confine": confine})
    with pytest.raises(ValueError, match="'small' must set manager.confine=bwrap"):
        _initialize(layout, "small")


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        ({"confine.readonly_paths": "relative"}, "absolute paths"),
        ({"confine.unknown": "x"}, "unknown confinement setting"),
        ({"slurm.partition": "short\nexport X=1"}, "control characters"),
        ({"manager.workers": "many"}, "manager.workers must be a positive integer"),
        ({"manager.allocation": "pbs"}, "allocation"),
    ],
)
def test_launcher_settings_the_dispatcher_refuses_fail_at_approval(
    settings: dict[str, str], message: str, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **settings)
    with pytest.raises(ValueError, match=message):
        _initialize(layout, "small")
    assert not _state(layout).exists()


def test_other_launcher_settings_are_frozen_as_ordinary_slurm_settings(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"slurm.qos": "high", "slurm.ntasks": 4, "environment.prelude": "module load x"})
    settings = dict(load_policy(_initialize(layout, "small")).launcher("small").settings)
    assert (settings["slurm.qos"], settings["slurm.ntasks"], settings["environment.prelude"]) == (
        "high",
        4,
        "module load x",
    )


def test_slurm_export_default_keeps_the_digest_and_nil_changes_it(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    # Without slurm.export the frozen content, and so the digest, is what it was before the setting existed.
    default_digest = _bundle_digest("small")
    policy = load_policy(_initialize(layout, "small"))
    assert policy.launcher("small").digest == default_digest
    assert "slurm.export" not in dict(policy.launcher("small").settings)
    # NIL is a different frozen configuration, so both the bundle digest and the configuration digest change.
    _rewrite_launcher("small", **{"slurm.export": "NIL"})
    reloaded = load_policy(_activate(layout))
    assert dict(reloaded.launcher("small").settings)["slurm.export"] == "NIL"
    assert reloaded.launcher("small").digest == _bundle_digest("small") != default_digest
    assert reloaded.configuration_digest("small") != policy.configuration_digest("small")


@pytest.mark.parametrize("value", ["ALL", "none", " NONE", "NONE\n", ""])
def test_slurm_export_invalid_value_fails_at_approval(value: str, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"slurm.export": value})
    with pytest.raises(ValueError, match="slurm.export must be exactly"):
        _initialize(layout, "small")
    assert not _state(layout).exists()


def test_a_launcher_executable_differing_from_the_packaged_template_is_accepted(tmp_path: Path) -> None:
    # The broker submits through the installed launch runtime and never runs the bundle's executable.
    layout = _layout(tmp_path)
    _executable(launchers_home() / "small" / "launcher", "#!/bin/sh\nexec site-launcher \"$@\"\n")
    snapshot = _initialize(layout, "small")
    assert load_policy(snapshot).launcher("small").digest == _bundle_digest("small")
    _executable(launchers_home() / "small" / "launcher", "#!/bin/sh\nexec other-launcher \"$@\"\n")
    assert _daemon_setup.activate(layout.workspace.root) == (snapshot, None)


def test_setup_never_runs_a_launcher_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)

    def refuse(*_args: object, **_kwargs: object) -> None:
        pytest.fail("setup must not run a launcher operation")

    monkeypatch.setattr(launchers, "run_launcher", refuse)
    monkeypatch.setattr(launchers.subprocess, "run", refuse)
    _initialize(layout)
    _configure(layout, ("remove", "launchers=small"), ("set", "force=true"))
    _activate(layout)


def test_project_launchers_are_never_consulted(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    project = tmp_path / "project"
    initialize_project(project, name="daemon-setup")
    add_launcher("local", template="slurm", project=project, settings=CONFINED)
    with pytest.raises(ValueError, match="unknown global launcher: 'local'"):
        _initialize(layout, "local")


def test_launcher_names_must_be_daemon_configuration_names(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _launcher("Upper")
    with pytest.raises(ValueError, match="daemon launcher names must match"):
        _initialize(layout, "Upper")
    with pytest.raises(ValueError, match="names must be unique"):
        _initialize(layout, "small", changes=[("set", "launchers=small,small")])


def test_workspace_alias_initializes_and_hands_canonical_path_to_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    alias = tmp_path / "workspace-alias"
    alias.symlink_to(layout.workspace.root, target_is_directory=True)
    changes = [*layout.options, ("add", "launchers=small"), ("add", f"authorized_keys={AUTHORIZED_KEY}")]
    snapshot = _daemon_setup.initialize(alias, exchange=layout.exchange, changes=changes)
    observed: list[object] = []

    monkeypatch.setattr(_daemon_cli, "_close_inherited", lambda: None)
    monkeypatch.setattr(_daemon_cli.os, "chdir", lambda _path: None)
    monkeypatch.setattr(_daemon_cli.os, "dup2", lambda *_args, **_kwargs: None)

    def refuse_exec(executable: str, argv: list[str], environment: dict[str, str]) -> None:
        observed.extend([executable, argv, environment])
        raise OSError("inspection stop")

    monkeypatch.setattr(_daemon_cli.os, "execve", refuse_exec)
    assert _daemon_cli.command(["run", str(alias), "--once"], program="httk") == 2
    argv = observed[1]
    assert isinstance(argv, list)
    assert "--mode" not in argv
    assert argv[argv.index("--workspace") + 1] == str(layout.workspace.root)
    assert argv[argv.index("--policy") + 1] == str(snapshot)


def _pass_sandbox_check(monkeypatch: pytest.MonkeyPatch) -> None:
    # Setup ends with the real sandboxed check, which needs user namespaces this host lacks.
    monkeypatch.setattr(_daemon_cli.subprocess, "run", lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0))


def _broker_arguments(layout: Layout) -> list[str]:
    return [argument for operation, item in layout.options for argument in (f"--{operation}", item)]


def test_relative_cli_paths_initialize_from_inside_the_workspace_and_print_the_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    layout = _layout(tmp_path)
    _pass_sandbox_check(monkeypatch)
    monkeypatch.chdir(layout.workspace.root)
    arguments = ["init", ".", "--exchange", "../exchange", "--add", "launchers=large"]
    arguments += ["--add", f"authorized_keys={AUTHORIZED_KEY}"]
    broker = _broker_arguments(layout)
    broker[broker.index(f"sbatch={layout.broker / 'sbatch'}")] = "sbatch=../../broker/sbatch"
    assert _daemon_cli.command([*arguments, *broker, "--set", "max_submissions=7"], program="httk") == 0
    output = capsys.readouterr().out.splitlines()
    assert output[-1] == "sandbox check passed"
    assert {
        "launchers=large",
        f"authorized_keys={AUTHORIZED_KEY}",
        "max_submissions=7",
        "cluster: test-cluster",
    } <= set(output)
    policy = load_policy(_active(layout))
    assert (policy.sbatch, policy.max_submissions) == (layout.broker / "sbatch", 7)
    assert (layout.exchange / "endpoint.json").is_file()


def test_old_daemon_flags_are_gone(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    layout = _layout(tmp_path)
    workspace = str(layout.workspace.root)
    for arguments in (
        [workspace, "--reload"],
        [workspace, "--initialize"],
        ["run", workspace, "--set", "cluster=c"],
        ["check", workspace, "--force"],
        ["configure", workspace, "--launcher", "small"],
    ):
        assert _daemon_cli.command(arguments, program="httk") == 2
        assert "error:" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("not_sibling", "siblings in a dedicated directory"),
        ("extra_entry", "must contain only workspace and exchange; found notes.txt"),
        ("state_inside", "must be disjoint from state"),
        ("snapshots_inside", "must be disjoint from snapshots"),
        ("data_home", "must be disjoint from the httk data home"),
        ("nonempty_exchange", "must not exist or must be an empty directory"),
        ("exchange_file", "must not exist or must be an empty directory"),
    ],
)
def test_layout_refusals_happen_before_any_enrollment_artifact(
    change: str, message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    options: dict[str, Path] = {}
    if change == "not_sibling":
        layout.exchange = tmp_path / "exchange"
    elif change == "extra_entry":
        (tmp_path / "site" / "notes.txt").touch()
    elif change == "state_inside":
        options["state"] = tmp_path / "site" / "workspace" / "state"
    elif change == "snapshots_inside":
        options["snapshots"] = tmp_path / "site" / "snapshots"
    elif change == "data_home":
        monkeypatch.setattr(_daemon_setup, "data_home", lambda: tmp_path / "site" / "workspace" / "home")
        options["state"] = tmp_path / "state"
    elif change == "nonempty_exchange":
        layout.exchange.mkdir()
        (layout.exchange / "left-over").touch()
    else:
        layout.exchange.touch()
    with pytest.raises(ValueError, match=message):
        _initialize(layout, "small", state=options.get("state"), snapshots=options.get("snapshots"))
    assert not _state(layout).exists()
    assert not (tmp_path / "state").exists()
    assert not (layout.workspace.root / ".httk-workspace" / "exchange").exists()
    assert not (layout.workspace.root / ".httk-workspace" / "exchange" / "enrollment.json").exists()


def test_existing_empty_exchange_is_adopted_with_private_mode(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.exchange.mkdir(mode=0o755)
    _initialize(layout, "small")
    assert layout.exchange.stat().st_mode & 0o777 == 0o700


def test_cross_mount_rename_probe_failure_refuses_initialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)

    def cross_device(*_args: object, **_kwargs: object) -> None:
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    with monkeypatch.context() as patch:
        patch.setattr(_daemon_policy.os, "rename", cross_device)
        with pytest.raises(ValueError, match="renameable into each other"):
            _initialize(layout, "small")
    assert list(layout.exchange.iterdir()) == []
    assert not _state(layout).exists()
    _initialize(layout, "small")
    assert (layout.exchange / "endpoint.json").is_file()


def test_bootstrap_refuses_an_entry_added_to_the_parent_after_initialize(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    snapshot = _initialize(layout, "small")
    (tmp_path / "site" / "intruder").mkdir()
    bootstrap = Path(_daemon_setup.__file__).with_name("_daemon_bootstrap.py")
    arguments = ["--policy", str(snapshot), "--workspace", str(layout.workspace.root), "--once"]
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(bootstrap), *arguments], capture_output=True, text=True, check=False
    )
    assert result.returncode == 2
    assert "must contain only workspace and exchange; found intruder" in result.stderr


@pytest.mark.parametrize(
    ("key", "value"),
    [("slurm.cpus_per_task", "2048"), ("slurm.mem", "2T"), ("slurm.time_limit", "8-0")],
)
def test_sanity_limits_apply_at_setup_unless_forced(key: str, value: str, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{key: value})
    with pytest.raises(ValueError, match=key.replace(".", r"\.") + ".*set force=true"):
        _initialize(layout, "small")
    assert not _state(layout).exists()
    assert not layout.exchange.exists()
    snapshot = _initialize(layout, "small", changes=[("set", "force=true")])
    assert _stored_configuration(layout)["force"] is True
    assert dict(load_policy(snapshot).launcher("small").settings)[key] == value


@pytest.mark.parametrize("key", ["slurm.mem", "slurm.time_limit", "slurm.cpus_per_task"])
def test_force_does_not_lift_the_hard_ceiling(key: str, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{key: str(2**31)})
    with pytest.raises(ValueError, match=key.replace(".", r"\.")) as refusal:
        _initialize(layout, "small", changes=[("set", "force=true")])
    assert "force=true" not in str(refusal.value)


def test_force_is_persisted_and_honoured_at_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    layout = _layout(tmp_path)
    _pass_sandbox_check(monkeypatch)
    monkeypatch.setattr(_daemon_cli.os, "execve", lambda *_args: pytest.fail("a refused activation must not exec"))
    workspace = str(layout.workspace.root)
    setup = ["--add", "launchers=small", "--add", f"authorized_keys={AUTHORIZED_KEY}", *_broker_arguments(layout)]
    assert _daemon_cli.command(["init", workspace, "--exchange", str(layout.exchange), *setup], program="httk") == 0
    old = _active(layout)
    _rewrite_launcher("small", **{"slurm.mem": "2T"})
    capsys.readouterr()
    for mode in ("check", "run"):
        assert _daemon_cli.command([mode, workspace], program="httk") == 2
        assert "slurm.mem" in capsys.readouterr().err
    assert _active(layout) == old
    assert _daemon_cli.command(["configure", workspace, "--set", "force=true"], program="httk") == 0
    assert "force=true" in capsys.readouterr().out.splitlines()
    assert _stored_configuration(layout)["force"] is True
    active = load_policy(_activate(layout))
    assert dict(active.launcher("small").settings)["slurm.mem"] == "2T"


@pytest.mark.parametrize(
    ("text", "expected"),
    [("2", 2), ("2:01", 3), ("1:02:01", 63), ("1-2", 1560), ("1-2:03", 1563), ("1-2:03:01", 1564), (90, 90)],
)
def test_slurm_time_forms_normalize_to_bounded_minutes(text: object, expected: int) -> None:
    assert _daemon_setup._time_minutes(text) == expected


@pytest.mark.parametrize("value", [True, 1.0, 0, "0", "UNLIMITED", "1:60", "1-24", float("inf")])
def test_slurm_time_refuses_noncanonical_or_unbounded_values(value: object) -> None:
    with pytest.raises(ValueError):
        _daemon_setup._time_minutes(value)


@pytest.mark.parametrize(("text", "expected"), [("1", 1), ("1K", 1), ("1025K", 2), ("3M", 3), ("2G", 2048), (64, 64)])
def test_slurm_memory_forms_normalize_to_mebibytes(text: object, expected: int) -> None:
    assert _daemon_setup._memory_mb(text) == expected


@pytest.mark.parametrize("value", [True, 1.5, 0, "0", "2P", "-1", " 1G"])
def test_slurm_memory_refuses_malformed_values(value: object) -> None:
    with pytest.raises(ValueError, match="slurm.mem"):
        _daemon_setup._memory_mb(value)


def test_activation_keeps_the_saved_configuration_without_rediscovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    first_path = _initialize(layout, changes=[("set", "max_submissions=9")])
    first = load_policy(first_path)
    _rewrite_launcher("small", **{"slurm.cpus_per_task": "16"})
    other = tmp_path / "other-broker"
    _executable(other / "sbatch")
    monkeypatch.setenv("PATH", f"{other}:{os.environ['PATH']}")
    second_path = _activate(layout)
    second = load_policy(second_path)
    assert first_path.is_file() and second_path != first_path
    assert second.enrollment_id == first.enrollment_id
    assert [launcher.name for launcher in second.launchers] == ["large", "small"]
    assert second.authorized_keys == first.authorized_keys
    for field in ("bwrap", "python", "sbatch", "squeue", "scancel", "sacct", "cluster", "slurm_conf"):
        assert getattr(second, field) == getattr(first, field)
    assert second.max_submissions == 9
    assert dict(second.launcher("small").settings)["slurm.cpus_per_task"] == "16"


def test_a_failed_initialize_does_not_enroll_the_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)

    def fail(_policy: object) -> None:
        raise OSError("endpoint write failed")

    monkeypatch.setattr(_daemon_setup, "_write_endpoint", fail)
    with pytest.raises(OSError, match="endpoint write failed"):
        _initialize(layout)
    assert (layout.workspace.root / ".httk-workspace/exchange/inbox").is_dir()
    assert not os.path.lexists(layout.workspace.root / ".httk-workspace/exchange/enrollment.json")


def test_activation_restores_a_missing_enrollment_marker_and_keeps_an_existing_one(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    policy = load_policy(_initialize(layout))
    marker = layout.workspace.root / ".httk-workspace/exchange/enrollment.json"
    marker.unlink()
    _activate(layout)
    assert json.loads(marker.read_bytes())["enrollment_id"] == policy.enrollment_id
    marker.write_text("kept", encoding="utf-8")
    _activate(layout)
    assert marker.read_text(encoding="utf-8") == "kept"


def test_configure_changes_lists_and_scalars_and_activation_refuses_a_running_daemon(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    first = _initialize(layout)
    description = _configure(
        layout,
        ("remove", "launchers=small"),
        ("add", f"authorized_keys={OTHER_KEY}"),
        ("remove", f"authorized_keys={AUTHORIZED_KEY}"),
        ("add", "launchers=large"),
        ("set", "max_submissions=5"),
    )
    configuration = description["configuration"]
    assert isinstance(configuration, dict)
    assert (configuration["launchers"], configuration["authorized_keys"]) == (["large"], [OTHER_KEY])
    assert configuration["max_submissions"] == 5
    assert _stored_configuration(layout) == {
        "format": "httk-workspace-daemon-configuration",
        "format_version": 1,
        **configuration,
    }
    # configure publishes nothing; the change waits for the next activation.
    assert _active(layout) == first
    second = load_policy(_activate(layout))
    assert [launcher.name for launcher in second.launchers] == ["large"]
    assert (second.authorized_keys, second.max_submissions) == ((OTHER_KEY,), 5)
    endpoint = json.loads((layout.exchange / "endpoint.json").read_text(encoding="utf-8"))
    assert set(endpoint["configurations"]) == {"large"}
    assert _configure(layout, ("set", "launchers=small,large"))["configuration"]["launchers"] == ["small", "large"]  # type: ignore[index]
    with Ledger(
        second.state,
        second.workspace_id,
        second.enrollment_id,
        max_records=second.max_records,
        max_submissions=second.max_submissions,
    ):
        with pytest.raises(ValueError, match="the daemon is running; stop it"):
            _activate(layout)
        # configure works while the daemon runs, and an unchanged activation takes no lock.
        _configure(layout, ("set", "launchers=large"))
        assert _daemon_setup.activate(layout.workspace.root) == (_active(layout), None)


def test_configure_accepts_slurm_client_changes_but_refuses_fixed_connection_changes(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    first = load_policy(_initialize(layout))
    other = tmp_path / "other-broker"
    for name in ("sbatch", "squeue", "scancel", "python3"):
        _executable(other / name)
    _configure(
        layout,
        ("set", f"sbatch={other / 'sbatch'}"),
        ("set", f"python={other / 'python3'}"),
        ("set", "max_submissions=3"),
    )
    active, changed_launchers = _daemon_setup.activate(layout.workspace.root)
    assert changed_launchers == ()
    changed = load_policy(active)
    assert (changed.sbatch, changed.python, changed.max_submissions) == (other / "sbatch", other / "python3", 3)
    assert changed.squeue == first.squeue
    assert changed.configuration_digest("small") != first.configuration_digest("small")
    with pytest.raises(ValueError, match="snapshots"):
        _activate(layout, snapshots=tmp_path / "elsewhere")
    for key in ("cluster", "scontrol"):
        with pytest.raises(ValueError, match=f"{key} is fixed by the enrollment; .*new enrollment"):
            _configure(layout, ("set", f"{key}=another-cluster"))
    assert _active(layout) == active


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (("set", "nope=1"), "unknown daemon configuration key 'nope'; valid keys: launchers, authorized_keys, bwrap"),
        (("set", "launchers"), "expects KEY=VALUE"),
        (("add", "max_submissions=3"), "--add applies only to the list keys"),
        (("remove", "launchers=absent"), "does not contain 'absent'"),
        (("set", "force=yes"), "force must be true or false"),
        (("set", "max_submissions=0"), "max_submissions"),
        (("set", "max_submissions=5000"), "max_submissions"),
        (("set", "bwrap="), "bwrap requires a value"),
        (("set", "sbatch=/absent/sbatch"), "sbatch"),
        (("add", "launchers=unknown"), "unknown global launcher: 'unknown'"),
        (("set", "launchers="), "at least one daemon launcher"),
        (("set", "authorized_keys=ed25519:bad"), "canonical Ed25519"),
    ],
)
def test_configure_refuses_an_invalid_change_or_result_without_saving(
    change: tuple[str, str], message: str, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _initialize(layout)
    saved = (_state(layout) / "configuration.json").read_bytes()
    with pytest.raises(ValueError, match=message):
        _configure(layout, ("set", "max_submissions=7"), change)
    assert (_state(layout) / "configuration.json").read_bytes() == saved


def test_a_missing_configuration_is_seeded_once_from_the_active_snapshot(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    snapshot = _initialize(layout, changes=[("set", "max_submissions=9")])
    policy = load_policy(snapshot)
    expected = _stored_configuration(layout)
    (_state(layout) / "configuration.json").unlink()
    assert _daemon_setup.activate(layout.workspace.root) == (snapshot, None)
    # The snapshot keeps the launchers in name order; force is never inferred.
    assert _stored_configuration(layout) == {**expected, "launchers": ["large", "small"]}
    assert _daemon_setup.describe(layout.workspace.root)["cluster"] == policy.cluster


def test_show_prints_the_enrollment_and_the_configuration(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    layout = _layout(tmp_path)
    policy = load_policy(_initialize(layout))
    workspace = str(layout.workspace.root)
    assert _daemon_cli.command(["show", workspace], program="httk") == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[:7] == [
        f"workspace: {layout.workspace.root}",
        f"workspace_id: {policy.workspace_id}",
        f"enrollment_id: {policy.enrollment_id}",
        f"exchange: {layout.exchange}",
        f"state: {policy.state}",
        f"snapshots: {policy.snapshots}",
        "cluster: test-cluster",
    ]
    assert lines[7:9] == ["launchers=small,large", f"authorized_keys={AUTHORIZED_KEY}"]
    assert lines[-4:] == ["sacct=", "slurm_conf=", "max_submissions=128", "force=false"]
    assert _daemon_cli.command(["show", workspace, "--json"], program="httk") == 0
    document = json.loads(capsys.readouterr().out)
    assert document == _daemon_setup.describe(layout.workspace.root)
    assert document["configuration"]["launchers"] == ["small", "large"]
    assert document["configuration"]["force"] is False and document["configuration"]["sacct"] is None


def test_configure_through_the_cli_prints_the_configuration_and_when_it_applies(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    layout = _layout(tmp_path)
    _initialize(layout)
    workspace = str(layout.workspace.root)
    arguments = ["configure", workspace, "--remove", "launchers=large", "--set", "slurm_conf="]
    arguments += ["--add", f"authorized_keys={OTHER_KEY}"]
    assert _daemon_cli.command(arguments, program="httk") == 0
    lines = capsys.readouterr().out.splitlines()
    assert "launchers=small" in lines and f"authorized_keys={AUTHORIZED_KEY},{OTHER_KEY}" in lines
    assert (
        lines[-1]
        == f"the configuration takes effect when the daemon next starts: httk workspace daemon run {workspace}"
    )
    assert _daemon_cli.command(["configure", workspace, "--set", "cluster=x"], program="httk") == 2
    assert "cluster is fixed by the enrollment" in capsys.readouterr().err


@pytest.mark.parametrize("source", ["declared", "environment", "default", "none"])
def test_effective_slurm_configuration_precedence_drives_policy(
    source: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    configurations = {}
    for name in ("declared", "environment", "default"):
        directory = tmp_path / f"slurm-{name}"
        directory.mkdir()
        configurations[name] = directory / "slurm.conf"
        configurations[name].write_text(f"ClusterName={name}\n", encoding="utf-8")
    monkeypatch.setenv("SLURM_CONF", str(configurations["environment"]))
    monkeypatch.setattr(_daemon_setup, "_DEFAULT_SLURM_CONF", configurations["default"])
    options = layout.options[:-1]
    if source == "declared":
        options.append(("set", f"slurm_conf={configurations['declared']}"))
    elif source == "default":
        monkeypatch.setenv("SLURM_CONF", "relative/slurm.conf")
    elif source == "none":
        monkeypatch.delenv("SLURM_CONF")
        configurations["default"].unlink()
        options.append(("set", "cluster=test-cluster"))
    policy = load_policy(_initialize(layout, broker=options))
    if source == "none":
        assert policy.slurm_conf is None
        assert policy.cluster == "test-cluster"
    else:
        assert policy.slurm_conf == configurations[source]
        assert policy.cluster == source
    digests = {name: policy.configuration_digest(name) for name in ("small", "large")}
    assert json.loads((layout.exchange / "endpoint.json").read_text(encoding="utf-8"))["configurations"] == digests
    # Activation keeps the saved file; a configured one never changes the enrollment's fixed cluster.
    other = tmp_path / "slurm-other"
    other.mkdir()
    (other / "slurm.conf").write_text(f"ClusterName=other-{source}\n", encoding="utf-8")
    monkeypatch.setenv("SLURM_CONF", str(other / "slurm.conf"))
    assert load_policy(_activate(layout)).slurm_conf == policy.slurm_conf
    _configure(layout, ("set", f"slurm_conf={other / 'slurm.conf'}"))
    changed = load_policy(_activate(layout))
    assert (changed.slurm_conf, changed.cluster) == (other / "slurm.conf", policy.cluster)


def test_slurm_conf_without_cluster_uses_bounded_scontrol_with_fixed_conf(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    slurm_conf = layout.broker / "slurm.conf"
    slurm_conf.write_text("Include extra.conf\n", encoding="utf-8")
    log = tmp_path / "scontrol.log"
    scontrol = _executable(
        layout.broker / "scontrol",
        f"#!/bin/sh\nprintf '%s' \"$SLURM_CONF\" > {log}\nprintf 'ClusterName = discovered-cluster\\n'\n",
    )
    options = [*layout.options[:-1], ("set", f"slurm_conf={slurm_conf}"), ("set", f"scontrol={scontrol}")]
    policy = load_policy(_initialize(layout, "small", broker=options))
    assert policy.cluster == "discovered-cluster"
    assert log.read_text(encoding="utf-8") == str(slurm_conf)


def test_unset_broker_tools_are_discovered_on_the_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    monkeypatch.setenv("PATH", str(layout.broker))
    with pytest.raises(ValueError, match="bwrap"):
        _initialize(layout, "small", broker=layout.options[1:])
    monkeypatch.setenv("PATH", f"{layout.runtime}:{layout.broker}")
    policy = load_policy(_initialize(layout, "small", broker=[("set", "cluster=test-cluster")]))
    assert (policy.bwrap, policy.sbatch, policy.squeue) == (
        layout.runtime / "bwrap",
        layout.broker / "sbatch",
        layout.broker / "squeue",
    )


def test_a_custom_state_must_be_repeated_and_custom_snapshots_are_remembered(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    state, snapshots = tmp_path / "custom-state", tmp_path / "custom-snapshots"
    snapshot = _initialize(layout, "small", state=state, snapshots=snapshots)
    assert snapshot.parent == snapshots
    assert _daemon_setup.activate(layout.workspace.root, state=state, snapshots=snapshots) == (snapshot, None)
    assert _daemon_setup.activate(layout.workspace.root, state=state) == (snapshot, None)
    with pytest.raises(FileNotFoundError):
        _activate(layout)
    with pytest.raises(ValueError, match="keeps its snapshots"):
        _activate(layout, state=state, snapshots=tmp_path / "other")


def test_duplicate_and_oversized_launcher_metadata_are_bounded(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    metadata = _metadata_path("small")
    metadata.write_text('{"format":"httk-manager-launcher","format":"duplicate"}', encoding="utf-8")
    with pytest.raises(ValueError, match="invalid launcher metadata JSON"):
        _initialize(layout, "small")
    metadata.write_text("{" + '"padding":"' + "x" * (64 * 1024) + '"}', encoding="utf-8")
    with pytest.raises(ValueError, match="too large"):
        _initialize(layout, "small")


def test_oversized_compiled_policy_fails_without_enrollment_artifacts(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"environment.prelude": "x" * (32 * 1024)})
    _rewrite_launcher("large", **{"environment.prelude": "x" * (32 * 1024)})
    with pytest.raises(ValueError, match="compiled runtime policy"):
        _initialize(layout)
    assert not _state(layout).exists()
    assert not layout.exchange.exists()


def test_exact_runtime_policy_size_limit_round_trips_without_an_extra_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    state = _state(layout)
    configuration = _daemon_setup._Configuration(
        launchers=("small",),
        authorized_keys=(AUTHORIZED_KEY,),
        bwrap=layout.runtime / "bwrap",
        python=_daemon_setup._running_python(),
        sbatch=layout.broker / "sbatch",
        squeue=layout.broker / "squeue",
        scancel=layout.broker / "scancel",
        sacct=None,
        slurm_conf=None,
        max_submissions=128,
        force=False,
    )
    compiled: Policy = _daemon_setup._compile(
        layout.workspace.root,
        layout.exchange,
        state,
        state.with_name(state.name + ".snapshots"),
        "0" * 32,
        "test-cluster",
        configuration,
    )
    canonical = _daemon_setup._canonical_bytes(_daemon_setup.policy_document(compiled))
    monkeypatch.setattr(_daemon_setup, "_MAX_RUNTIME_POLICY_BYTES", len(canonical))
    monkeypatch.setattr(_daemon_policy, "MAX_POLICY_BYTES", len(canonical))
    snapshot = _initialize(layout, "small")
    assert snapshot.stat().st_size == len(canonical)
    assert load_policy(snapshot).workspace_id == compiled.workspace_id


def test_partial_initialization_is_preserved_and_never_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)

    def fail_snapshot(_path: Path, _data: bytes) -> None:
        raise OSError("injected snapshot failure")

    monkeypatch.setattr(_daemon_setup, "_write_exclusive", fail_snapshot)
    with pytest.raises(OSError, match="injected"):
        _initialize(layout, "small")
    state = _state(layout)
    assert (state / "ledger.sqlite3").is_file()
    assert (state / "response.seed").is_file()
    assert not (state / "active.json").exists()
    assert not (layout.exchange / "endpoint.json").exists()
    with pytest.raises(ValueError, match="empty directory"):
        _initialize(layout, "small")


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
def test_activation_snapshot_failure_keeps_old_active_and_retry_recovers(
    failure: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    old_snapshot = _initialize(layout, "small")
    old_endpoint = (layout.exchange / "endpoint.json").read_bytes()
    _rewrite_launcher("small", **{"slurm.cpus_per_task": "16"})
    original = _daemon_setup._write_exclusive

    def interrupted(path: Path, data: bytes) -> None:
        if failure == "after_install":
            original(path, data)
        raise OSError(f"injected {failure}")

    monkeypatch.setattr(_daemon_setup, "_write_exclusive", interrupted)
    with pytest.raises(OSError, match=failure):
        _activate(layout)
    assert _active(layout) == old_snapshot
    assert (layout.exchange / "endpoint.json").read_bytes() == old_endpoint
    monkeypatch.setattr(_daemon_setup, "_write_exclusive", original)
    new_snapshot = _activate(layout)
    assert new_snapshot != old_snapshot
    assert dict(load_policy(new_snapshot).launcher("small").settings)["slurm.cpus_per_task"] == "16"


def test_world_writable_ancestry_is_refused_without_artifacts_and_group_writable_accepted(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    (tmp_path / "site").chmod(0o777)
    with pytest.raises(ValueError, match="world-writable"):
        _initialize(layout, "small")
    assert not layout.exchange.exists() and not _state(layout).exists()
    (tmp_path / "site").chmod(0o770)
    _initialize(layout, "small")


def test_at_least_one_launcher_and_key_are_required(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    launcher, key = ("add", "launchers=small"), ("add", f"authorized_keys={AUTHORIZED_KEY}")
    for changes, message in (
        ([launcher], "at least one authorized key"),
        ([key], "at least one daemon launcher"),
        ([launcher, ("add", "authorized_keys=ed25519:bad")], "canonical Ed25519"),
        ([launcher, key, ("remove", "launchers=small")], "at least one daemon launcher"),
    ):
        with pytest.raises(ValueError, match=message):
            _daemon_setup.initialize(
                layout.workspace.root, exchange=layout.exchange, changes=[*layout.options, *changes]
            )
    assert not _state(layout).exists()


def test_leftover_state_is_refused_before_the_exchange_is_touched(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _state(layout).mkdir(parents=True)
    with pytest.raises(ValueError, match="daemon state already exists: .*different --state"):
        _initialize(layout, "small")
    assert not layout.exchange.exists()
    fresh = tmp_path / "fresh-state"
    assert load_policy(_initialize(layout, "small", state=fresh)).state == fresh


def test_activation_recreates_deleted_staging_and_job_output_directories(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _initialize(layout, "small")
    staging = layout.workspace.root / ".httk-workspace" / "exchange"
    shutil.rmtree(staging)
    jobs = _state(layout).with_name(_state(layout).name + ".snapshots") / "jobs"
    jobs.rmdir()
    _activate(layout)
    for name in ("inbox", "outbox", "outbox/rejected", "records"):
        assert (staging / name).is_dir()
    assert jobs.stat().st_mode & 0o777 == 0o700


def test_max_submissions_flows_into_the_policy_and_the_ledger_quota(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    policy = load_policy(_initialize(layout, "small", changes=[("set", "max_submissions=2")]))
    assert policy.max_submissions == 2
    digest = policy.configuration_digest("small")
    with Ledger(
        policy.state,
        policy.workspace_id,
        policy.enrollment_id,
        max_records=policy.max_records,
        max_submissions=policy.max_submissions,
    ) as ledger:
        for number in (1, 2):
            ledger.admit(
                Request(
                    f"{number:032x}",
                    policy.workspace_id,
                    "start_manager",
                    profile="small",
                    enrollment_id=policy.enrollment_id,
                    configuration_digest=digest,
                )
            )
        with pytest.raises(CapacityError, match="submission"):
            ledger.admit(
                Request(
                    f"{3:032x}",
                    policy.workspace_id,
                    "start_manager",
                    profile="small",
                    enrollment_id=policy.enrollment_id,
                    configuration_digest=digest,
                )
            )


@pytest.mark.parametrize("problem", ["world_writable", "inside_state"])
def test_launcher_bundle_must_be_protected_and_outside_daemon_roots(
    problem: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    if problem == "world_writable":
        _metadata_path("small").chmod(0o666)
        with pytest.raises(ValueError, match="world-writable"):
            _initialize(layout, "small")
    else:
        state = tmp_path / "custom-state"
        home = state / "launchers"
        home.mkdir(parents=True)
        (launchers_home() / "small").rename(home / "small")
        monkeypatch.setattr(_daemon_setup, "launchers_home", lambda: home)
        with pytest.raises(ValueError, match="'small' must not lie inside the daemon parent, state or snapshots"):
            _initialize(layout, "small", state=state)


def test_report_directories_are_created_at_initialize_and_recreated_at_activation(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _initialize(layout, "small")
    for name in ("outbox/managers", "outbox/withdrawn"):
        assert (layout.exchange / name).stat().st_mode & 0o777 == 0o700
    (layout.exchange / "outbox/managers").rmdir()
    _activate(layout)
    for name in ("outbox/managers", "outbox/withdrawn"):
        assert (layout.exchange / name).stat().st_mode & 0o777 == 0o700


def test_sacct_is_optional_discovered_at_initialize_and_kept_or_configured_later(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    policy = load_policy(_initialize(layout, "small"))
    assert policy.sacct is None and "sacct" not in _daemon_policy.policy_document(policy)
    endpoint = (layout.exchange / "endpoint.json").read_bytes()

    sacct = _executable(layout.broker / "sacct")
    assert load_policy(_activate(layout)).sacct is None
    _configure(layout, ("set", f"sacct={sacct}"))
    given = load_policy(_activate(layout))
    assert given.sacct == sacct
    # Reporting only: the approved configuration digests the client pins do not change.
    assert (layout.exchange / "endpoint.json").read_bytes() == endpoint
    with pytest.raises(ValueError, match="sacct"):
        _configure(layout, ("set", f"sacct={tmp_path / 'absent/sacct'}"))
    _configure(layout, ("set", "sacct="))
    assert load_policy(_activate(layout)).sacct is None
    monkeypatch.setattr(_daemon_setup, "_state_default", lambda _workspace: tmp_path / "second-state")
    shutil.rmtree(layout.exchange)
    assert load_policy(_initialize(layout, "small")).sacct == sacct
