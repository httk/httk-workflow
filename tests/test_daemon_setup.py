"""Policy-free enrollment, publication, and reload for the workspace daemon."""

import base64
import errno
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _daemon_cli, _daemon_launcher, _daemon_policy, _daemon_setup
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


@dataclass(slots=True)
class Layout:
    workspace: Workspace
    exchange: Path
    runtime: Path
    broker: Path
    site: dict[str, str]


@pytest.fixture(autouse=True)
def _no_host_slurm_configuration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLURM_CONF", raising=False)
    monkeypatch.setattr(_daemon_setup, "_DEFAULT_SLURM_CONF", tmp_path / "absent-etc-slurm" / "slurm.conf")


def _executable(path: Path, body: str = "#!/bin/sh\nexit 0\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def _launcher(name: str, layout: Layout, **settings: str) -> Path:
    return add_launcher(name, template="daemon", global_=True, settings={**layout.site, **settings})


def _rewrite_launcher(name: str, **settings: str | None) -> None:
    path = launchers_home() / name / "launcher.json"
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
    for name in ("sbatch", "squeue", "scancel"):
        _executable(broker / name)
    workspace = Workspace.initialize(tmp_path / "site" / "workspace")
    site = {
        "daemon.readonly_paths": f"{runtime}:{Path(sys.prefix).resolve()}",
        "daemon.broker_paths": str(broker),
        "daemon.bwrap": str(runtime / "bwrap"),
        "daemon.sbatch": str(broker / "sbatch"),
        "daemon.squeue": str(broker / "squeue"),
        "daemon.scancel": str(broker / "scancel"),
        "daemon.cluster": "test-cluster",
    }
    layout = Layout(workspace, tmp_path / "site" / "exchange", runtime, broker, site)
    _launcher("small", layout, **{"slurm.partition": "short", "manager.workers": "3"})
    _launcher(
        "large",
        layout,
        **{"slurm.cpus_per_task": "8", "slurm.mem": "2G", "slurm.time_limit": "1:00:01", "slurm.gres": "gpu:2"},
    )
    return layout


def _initialize(layout: Layout, *launchers: str, **options: object) -> Path:
    return _daemon_setup.initialize(
        layout.workspace.root,
        exchange=layout.exchange,
        launchers=launchers or ("small", "large"),
        authorized_keys=(AUTHORIZED_KEY,),
        **options,  # type: ignore[arg-type]
    )


def _state(layout: Layout) -> Path:
    return _daemon_setup._state_default(layout.workspace.root)


def test_initialize_compiles_daemon_launchers_and_publishes_the_endpoint(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    snapshot = _initialize(layout)
    policy = load_policy(snapshot)
    small, large = policy.profile("small"), policy.profile("large")
    assert (small.cpus, small.memory_mb, small.time_minutes, small.workers, small.partition) == (
        None,
        None,
        None,
        3,
        "short",
    )
    assert (large.cpus, large.memory_mb, large.time_minutes, large.gres) == (8, 2048, 61, "gpu:2")
    assert policy.exchange == layout.exchange and policy.root == tmp_path / "site"
    assert policy.state == _state(layout) and policy.state.parent.name == "workspace-daemons"
    assert policy.snapshots == policy.state.with_name(policy.state.name + ".snapshots") == snapshot.parent
    assert policy.authorized_keys == (AUTHORIZED_KEY,)
    assert policy.max_submissions == 128
    assert policy.python.is_relative_to(Path(sys.prefix).resolve())
    for directory in (policy.state, policy.snapshots, layout.exchange):
        assert directory.stat().st_mode & 0o777 == 0o700
    for name in ("requests", "responses", "inbox", "outbox", "outbox/rejected"):
        assert (layout.exchange / name).is_dir()
    for name in ("inbox", "outbox", "outbox/rejected", "records"):
        assert (layout.workspace.root / ".httk-workspace/exchange" / name).is_dir()
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
    assert _daemon_setup.active_policy_path(layout.workspace.root) == snapshot


def test_workspace_settings_are_never_read(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.workspace.set_setting("slurm.ntasks", "4")
    layout.workspace.set_setting("slurm.cpus_per_task", "16")
    layout.workspace.set_setting("environment.prelude", "module load workspace")
    policy = load_policy(_initialize(layout, "small"))
    assert policy.profile("small") == _daemon_policy.Profile("small", workers=3, partition="short")


def test_default_runtime_paths_follow_the_interpreter_and_the_slurm_configuration(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    slurm_conf = layout.broker / "slurm.conf"
    slurm_conf.write_text("ClusterName=test-cluster\n", encoding="utf-8")
    _executable(layout.broker / "bwrap")
    _rewrite_launcher(
        "small",
        **{
            "daemon.readonly_paths": None,
            "daemon.broker_paths": None,
            "daemon.bwrap": str(layout.broker / "bwrap"),
            "daemon.slurm_conf": str(slurm_conf),
        },
    )
    policy = load_policy(_initialize(layout, "small"))
    candidates = {Path(path).resolve() for path in ("/usr", "/bin", "/lib", "/lib64") if os.path.exists(path)}
    candidates |= {Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve()}
    expected = {
        path for path in candidates if not any(path != other and path.is_relative_to(other) for other in candidates)
    }
    assert set(policy.readonly_paths) == expected
    munge = (Path("/run/munge"),) if Path("/run/munge").exists() else ()
    assert policy.broker_paths == (layout.broker, *munge)
    assert policy.slurm_conf == slurm_conf


@pytest.mark.parametrize("source", ["declared", "environment", "default", "none"])
def test_effective_slurm_configuration_precedence_drives_policy_and_broker_default(
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
    settings: dict[str, str | None] = {"daemon.broker_paths": None, "daemon.cluster": None}
    if source == "declared":
        settings["daemon.slurm_conf"] = str(configurations["declared"])
    elif source == "default":
        monkeypatch.setenv("SLURM_CONF", "relative/slurm.conf")
    elif source == "none":
        monkeypatch.delenv("SLURM_CONF")
        configurations["default"].unlink()
        settings["daemon.cluster"] = "test-cluster"
    _rewrite_launcher("small", **settings)
    _rewrite_launcher("large", **settings)
    # The test Slurm clients live in the declared broker root, so keep it as a readonly root here.
    _rewrite_launcher(
        "small", **{"daemon.readonly_paths": f"{layout.runtime}:{layout.broker}:{Path(sys.prefix).resolve()}"}
    )
    _rewrite_launcher(
        "large", **{"daemon.readonly_paths": f"{layout.runtime}:{layout.broker}:{Path(sys.prefix).resolve()}"}
    )
    policy = load_policy(_initialize(layout))
    munge = (Path("/run/munge"),) if Path("/run/munge").exists() else ()
    if source == "none":
        assert policy.slurm_conf is None
        assert policy.broker_paths == munge
        assert policy.cluster == "test-cluster"
    else:
        assert policy.slurm_conf == configurations[source]
        assert policy.broker_paths == (configurations[source].parent, *munge)
        assert policy.cluster == source
    digests = {name: policy.configuration_digest(name) for name in ("small", "large")}
    assert json.loads((layout.exchange / "endpoint.json").read_text(encoding="utf-8"))["configurations"] == digests
    if source != "none":
        other = tmp_path / "slurm-other"
        other.mkdir()
        (other / "slurm.conf").write_text(f"ClusterName=other-{source}\n", encoding="utf-8")
        monkeypatch.setenv("SLURM_CONF", str(other / "slurm.conf"))
        monkeypatch.setattr(_daemon_setup, "_DEFAULT_SLURM_CONF", other / "slurm.conf")
        if source == "declared":
            _rewrite_launcher("small", **{"daemon.slurm_conf": str(other / "slurm.conf")})
            _rewrite_launcher("large", **{"daemon.slurm_conf": str(other / "slurm.conf")})
        with pytest.raises(ValueError, match="a new enrollment is required") as refusal:
            _daemon_setup.reload(layout.workspace.root)
        assert "cluster" in set(str(refusal.value).split(": ", 1)[1].split(";")[0].split(", "))


def test_workspace_alias_initializes_and_hands_canonical_path_to_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    alias = tmp_path / "workspace-alias"
    alias.symlink_to(layout.workspace.root, target_is_directory=True)
    snapshot = _daemon_setup.initialize(
        alias, exchange=layout.exchange, launchers=("small",), authorized_keys=(AUTHORIZED_KEY,)
    )
    observed: list[object] = []

    monkeypatch.setattr(_daemon_cli, "_close_inherited", lambda: None)
    monkeypatch.setattr(_daemon_cli.os, "chdir", lambda _path: None)
    monkeypatch.setattr(_daemon_cli.os, "dup2", lambda *_args, **_kwargs: None)

    def refuse_exec(executable: str, argv: list[str], environment: dict[str, str]) -> None:
        observed.extend([executable, argv, environment])
        raise OSError("inspection stop")

    monkeypatch.setattr(_daemon_cli.os, "execve", refuse_exec)
    assert _daemon_cli.command([str(alias), "--once"], program="httk") == 2
    argv = observed[1]
    assert isinstance(argv, list)
    assert argv[argv.index("--workspace") + 1] == str(layout.workspace.root)
    assert argv[argv.index("--policy") + 1] == str(snapshot)


def test_relative_cli_paths_initialize_from_inside_the_workspace_and_print_the_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    layout = _layout(tmp_path)
    monkeypatch.chdir(layout.workspace.root)
    arguments = [".", "--initialize", "--exchange", "../exchange", "--launcher", "large", "--authorize", AUTHORIZED_KEY]
    assert _daemon_cli.command(arguments, program="httk") == 0
    assert capsys.readouterr().out == f"launcher large\nauthorized {AUTHORIZED_KEY}\n"
    assert (layout.exchange / "endpoint.json").is_file()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("not_sibling", "siblings in a dedicated directory"),
        ("extra_entry", "must contain only workspace and exchange; found notes.txt"),
        ("state_inside", "must be disjoint from state"),
        ("snapshots_inside", "must be disjoint from snapshots"),
        ("data_home", "must be disjoint from the httk data home"),
        ("readonly_inside", "must be disjoint from runtime path"),
        ("nonempty_exchange", "must not exist or must be an empty directory"),
        ("exchange_file", "must not exist or must be an empty directory"),
    ],
)
def test_layout_refusals_happen_before_any_enrollment_artifact(
    change: str, message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    options: dict[str, object] = {}
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
    elif change == "readonly_inside":
        _rewrite_launcher("small", **{"daemon.readonly_paths": f"{layout.runtime}:{tmp_path / 'site/lib'}"})
    elif change == "nonempty_exchange":
        layout.exchange.mkdir()
        (layout.exchange / "left-over").touch()
    else:
        layout.exchange.touch()
    with pytest.raises(ValueError, match=message):
        _initialize(layout, "small", **options)
    assert not _state(layout).exists()
    assert not (tmp_path / "state").exists()
    assert not (layout.workspace.root / ".httk-workspace" / "exchange").exists()


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
    arguments = ["--policy", str(snapshot), "--mode", "broker", "--workspace", str(layout.workspace.root), "--once"]
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(bootstrap), *arguments], capture_output=True, text=True, check=False
    )
    assert result.returncode == 2
    assert "must contain only workspace and exchange; found intruder" in result.stderr


def test_slurm_kind_launcher_is_refused_with_the_daemon_template_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    monkeypatch.setenv("PATH", f"{layout.broker}:{os.environ['PATH']}")
    add_launcher("cluster", template="slurm", global_=True, settings={"slurm.cpus_per_task": "2"})
    with pytest.raises(ValueError, match="'cluster' is a 'slurm' launcher; .*--template daemon --global"):
        _initialize(layout, "cluster")


def test_project_launchers_are_never_consulted(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    project = tmp_path / "project"
    initialize_project(project, name="daemon-setup")
    add_launcher("local", template="daemon", project=project, settings=layout.site)
    with pytest.raises(ValueError, match="unknown global daemon launcher: 'local'"):
        _initialize(layout, "local")


def test_conflicting_site_settings_across_launchers_are_refused(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("large", **{"daemon.cluster": "other-cluster"})
    with pytest.raises(ValueError, match="set daemon.cluster to different values") as refusal:
        _initialize(layout)
    assert "'small'" in str(refusal.value) and "'large'" in str(refusal.value)
    _rewrite_launcher("large", **{"daemon.cluster": "test-cluster", "daemon.mpi.environment.OMPI_MCA_btl": "self"})
    _rewrite_launcher("small", **{"daemon.mpi.environment.OMPI_MCA_btl": "tcp"})
    with pytest.raises(ValueError, match=r"daemon\.mpi\.environment\.OMPI_MCA_btl"):
        _initialize(layout)


def test_mpi_launchers_require_and_use_the_site_mpi_settings(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _executable(layout.broker / "srun")
    control = tmp_path / "mpi-control"
    control.mkdir()
    geometry = {
        "slurm.cpus_per_task": "4",
        "slurm.nodes": "2",
        "slurm.ntasks_per_node": "3",
        "slurm.mpi": "pmix",
        "daemon.mpi.srun": str(layout.broker / "srun"),
        "daemon.mpi.environment.OMPI_MCA_btl": "self,tcp",
    }
    _launcher("mpi", layout, **geometry)
    with pytest.raises(ValueError, match="require daemon.mpi.control_root"):
        _initialize(layout, "mpi")
    _rewrite_launcher("mpi", **{"daemon.mpi.control_root": str(control)})
    policy = load_policy(_initialize(layout, "mpi", "small"))
    profile = policy.profile("mpi")
    assert profile.mpi is not None
    assert (profile.mpi.nodes, profile.mpi.ranks, profile.mpi.ntasks_per_node) == (2, 6, 3)
    assert policy.profile("small").mpi is None
    assert policy.mpi is not None
    assert (policy.mpi.control_root, policy.mpi.environment) == (control, (("OMPI_MCA_btl", "self,tcp"),))


def test_serial_enrollment_ignores_unused_mpi_site_settings(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"daemon.mpi.control_root": str(tmp_path / "absent")})
    assert load_policy(_initialize(layout, "small")).mpi is None


@pytest.mark.parametrize(
    ("key", "value", "field", "expected"),
    [
        ("slurm.cpus_per_task", "2048", "cpus", 2048),
        ("slurm.mem", "2T", "memory_mb", 2 * 1024 * 1024),
        ("slurm.time_limit", "8-0", "time_minutes", 8 * 24 * 60),
    ],
)
def test_sanity_limits_apply_at_setup_unless_forced(
    key: str, value: str, field: str, expected: int, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{key: value})
    with pytest.raises(ValueError, match=key.replace(".", r"\.")):
        _initialize(layout, "small")
    assert not _state(layout).exists()
    assert not layout.exchange.exists()
    snapshot = _initialize(layout, "small", force=True)
    assert getattr(load_policy(snapshot).profile("small"), field) == expected


@pytest.mark.parametrize("key", ["slurm.mem", "slurm.time_limit", "slurm.cpus_per_task"])
def test_force_does_not_lift_the_hard_ceiling(key: str, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{key: str(2**31)})
    with pytest.raises(ValueError, match=key.replace(".", r"\.")) as refusal:
        _initialize(layout, "small", force=True)
    assert "--force" not in str(refusal.value)


def test_force_on_reload_and_through_the_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    layout = _layout(tmp_path)
    workspace = str(layout.workspace.root)
    setup = ["--launcher", "small", "--authorize", AUTHORIZED_KEY]
    initialize = [workspace, "--initialize", "--exchange", str(layout.exchange), *setup]
    assert _daemon_cli.command(initialize, program="httk") == 0
    _rewrite_launcher("small", **{"slurm.mem": "2T"})
    capsys.readouterr()
    assert _daemon_cli.command([workspace, "--reload"], program="httk") == 2
    assert "slurm.mem" in capsys.readouterr().err
    assert _daemon_cli.command([workspace, "--reload", "--force"], program="httk") == 0
    active = load_policy(_daemon_setup.active_policy_path(layout.workspace.root))
    assert active.profile("small").memory_mb == 2 * 1024 * 1024


def test_reload_keeps_the_stored_launchers_and_keys_and_rewrites_the_endpoint(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    first_path = _initialize(layout)
    first = load_policy(first_path)
    _rewrite_launcher("small", **{"slurm.cpus_per_task": "16"})
    second_path = _daemon_setup.reload(layout.workspace.root)
    second = load_policy(second_path)
    assert first_path.is_file() and second_path != first_path
    assert second.enrollment_id == first.enrollment_id
    assert sorted(profile.name for profile in second.profiles) == ["large", "small"]
    assert second.authorized_keys == first.authorized_keys
    assert second.profile("small").cpus == 16
    endpoint = json.loads((layout.exchange / "endpoint.json").read_text(encoding="utf-8"))
    assert endpoint["configurations"]["small"] == second.configuration_digest("small")
    assert endpoint["configurations"]["small"] != first.configuration_digest("small")


def test_reload_replaces_launchers_and_keys_and_refuses_a_held_ledger(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _initialize(layout)
    second = load_policy(_daemon_setup.reload(layout.workspace.root, launchers=["large"], authorized_keys=[OTHER_KEY]))
    assert [profile.name for profile in second.profiles] == ["large"]
    assert second.authorized_keys == (OTHER_KEY,)
    endpoint = json.loads((layout.exchange / "endpoint.json").read_text(encoding="utf-8"))
    assert set(endpoint["configurations"]) == {"large"}
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
        _daemon_setup.reload(layout.workspace.root)


def test_reload_accepts_broker_path_changes_but_refuses_fixed_connection_changes(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _initialize(layout)
    other = tmp_path / "other-broker"
    for name in ("sbatch", "squeue", "scancel"):
        _executable(other / name)
    _rewrite_launcher("small", **{"daemon.broker_paths": f"{layout.broker}:{other}"})
    _rewrite_launcher("large", **{"daemon.broker_paths": f"{layout.broker}:{other}"})
    active = _daemon_setup.reload(layout.workspace.root)
    assert other in load_policy(active).broker_paths
    with pytest.raises(ValueError, match="snapshots"):
        _daemon_setup.reload(layout.workspace.root, snapshots=tmp_path / "elsewhere")
    _rewrite_launcher("small", **{"daemon.cluster": "another-cluster"})
    _rewrite_launcher("large", **{"daemon.cluster": "another-cluster"})
    with pytest.raises(ValueError, match="a new enrollment is required") as refusal:
        _daemon_setup.reload(layout.workspace.root)
    assert "cluster" in str(refusal.value)
    assert _daemon_setup.active_policy_path(layout.workspace.root) == active


def test_custom_state_and_snapshots_must_be_repeated(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    state, snapshots = tmp_path / "custom-state", tmp_path / "custom-snapshots"
    snapshot = _initialize(layout, "small", state=state, snapshots=snapshots)
    assert snapshot.parent == snapshots
    assert _daemon_setup.active_policy_path(layout.workspace.root, state=state, snapshots=snapshots) == snapshot
    with pytest.raises(FileNotFoundError):
        _daemon_setup.active_policy_path(layout.workspace.root)
    with pytest.raises(ValueError, match="keeps its snapshots"):
        _daemon_setup.active_policy_path(layout.workspace.root, state=state, snapshots=tmp_path / "other")


def test_duplicate_and_oversized_launcher_metadata_are_bounded(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    metadata = launchers_home() / "small" / "launcher.json"
    metadata.write_text('{"format":"httk-manager-launcher","format":"duplicate"}', encoding="utf-8")
    with pytest.raises(ValueError, match="invalid launcher metadata JSON"):
        _initialize(layout, "small")
    metadata.write_text("{" + '"padding":"' + "x" * (64 * 1024) + '"}', encoding="utf-8")
    with pytest.raises(ValueError, match="too large"):
        _initialize(layout, "small")


def test_oversized_compiled_policy_fails_without_enrollment_artifacts(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"environment.prelude": "x" * (64 * 1024 - 1024)})
    with pytest.raises(ValueError, match="compiled runtime policy"):
        _initialize(layout, "small")
    assert not _state(layout).exists()
    assert not layout.exchange.exists()


def test_exact_runtime_policy_size_limit_round_trips_without_an_extra_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    state = _state(layout)
    compiled: Policy = _daemon_setup._compile(
        layout.workspace.root,
        layout.exchange,
        state,
        state.with_name(state.name + ".snapshots"),
        "0" * 32,
        ["small"],
        [AUTHORIZED_KEY],
        force=False,
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
def test_reload_snapshot_failure_keeps_old_active_and_retry_recovers(
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
        _daemon_setup.reload(layout.workspace.root)
    assert _daemon_setup.active_policy_path(layout.workspace.root) == old_snapshot
    assert (layout.exchange / "endpoint.json").read_bytes() == old_endpoint
    monkeypatch.setattr(_daemon_setup, "_write_exclusive", original)
    new_snapshot = _daemon_setup.reload(layout.workspace.root)
    assert new_snapshot != old_snapshot
    assert load_policy(new_snapshot).profile("small").cpus == 16


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
    with pytest.raises(ValueError, match="at least one authorized key"):
        _daemon_setup.initialize(
            layout.workspace.root, exchange=layout.exchange, launchers=["small"], authorized_keys=[]
        )
    with pytest.raises(ValueError, match="at least one daemon launcher"):
        _daemon_setup.initialize(
            layout.workspace.root, exchange=layout.exchange, launchers=[], authorized_keys=[AUTHORIZED_KEY]
        )
    with pytest.raises(ValueError, match="canonical Ed25519"):
        _daemon_setup.initialize(
            layout.workspace.root, exchange=layout.exchange, launchers=["small"], authorized_keys=["ed25519:bad"]
        )


def test_slurm_conf_without_cluster_uses_bounded_scontrol_with_fixed_conf(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    slurm_conf = layout.broker / "slurm.conf"
    slurm_conf.write_text("Include extra.conf\n", encoding="utf-8")
    log = tmp_path / "scontrol.log"
    _executable(
        layout.broker / "scontrol",
        f"#!/bin/sh\nprintf '%s' \"$SLURM_CONF\" > {log}\nprintf 'ClusterName = discovered-cluster\\n'\n",
    )
    _rewrite_launcher(
        "small",
        **{
            "daemon.cluster": None,
            "daemon.slurm_conf": str(slurm_conf),
            "daemon.scontrol": str(layout.broker / "scontrol"),
        },
    )
    policy = load_policy(_initialize(layout, "small"))
    assert policy.cluster == "discovered-cluster"
    assert log.read_text(encoding="utf-8") == str(slurm_conf)


def test_missing_path_discovered_tool_and_manager_command_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"daemon.bwrap": None})
    monkeypatch.setenv("PATH", str(layout.broker))
    with pytest.raises(ValueError, match="bwrap"):
        _initialize(layout, "small")

    _rewrite_launcher("small", **{"daemon.bwrap": str(layout.runtime / "bwrap"), "manager.command": "module-httk"})
    policy = load_policy(_initialize(layout, "small"))
    assert policy.profile("small").manager_command == "module-httk"


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
    assert _daemon_launcher._time_minutes(text) == expected


@pytest.mark.parametrize("value", [True, 1.0, 0, "0", "UNLIMITED", "1:60", "1-24", float("inf")])
def test_slurm_time_refuses_noncanonical_or_unbounded_values(value: object) -> None:
    with pytest.raises(ValueError):
        _daemon_launcher._time_minutes(value)


def test_leftover_state_is_refused_before_the_exchange_is_touched(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _state(layout).mkdir(parents=True)
    with pytest.raises(ValueError, match="daemon state already exists: .*different --state"):
        _initialize(layout, "small")
    assert not layout.exchange.exists()
    fresh = tmp_path / "fresh-state"
    assert load_policy(_initialize(layout, "small", state=fresh)).state == fresh


def test_reload_recreates_deleted_workspace_staging_directories(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _initialize(layout, "small")
    staging = layout.workspace.root / ".httk-workspace" / "exchange"
    shutil.rmtree(staging)
    _daemon_setup.reload(layout.workspace.root)
    for name in ("inbox", "outbox", "outbox/rejected", "records"):
        assert (staging / name).is_dir()


def test_max_submissions_flows_into_the_policy_and_the_ledger_quota(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _rewrite_launcher("small", **{"daemon.max_submissions": "2"})
    policy = load_policy(_initialize(layout, "small"))
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


def test_setup_never_executes_a_launcher_bundle(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    marker = tmp_path / "launcher-executed"
    for name in ("small", "large"):
        _executable(launchers_home() / name / "launcher", f"#!/bin/sh\ntouch {marker}\nexit 99\n")
    _initialize(layout)
    _daemon_setup.reload(layout.workspace.root)
    _daemon_setup.reload(layout.workspace.root, launchers=["large"], force=True)
    assert not marker.exists()


@pytest.mark.parametrize("problem", ["world_writable", "inside_state"])
def test_launcher_metadata_must_be_protected_and_outside_daemon_roots(
    problem: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    if problem == "world_writable":
        (launchers_home() / "small" / "launcher.json").chmod(0o666)
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
