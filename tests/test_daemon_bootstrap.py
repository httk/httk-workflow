"""Test bootstrap argument and descriptor boundaries with a non-confining recorder."""

import base64
import json
import os
import runpy
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

BOOTSTRAP = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "_daemon_bootstrap.py"
AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")
REQUIRED_BWRAP_OPTIONS = (
    "--assert-userns-disabled",
    "--bind-fd",
    "--clearenv",
    "--disable-userns",
    "--new-session",
    "--ro-bind-data",
    "--ro-bind-fd",
    "--unshare-ipc",
    "--unshare-net",
    "--unshare-pid",
    "--unshare-user",
    "--unshare-uts",
)


def _write_executable(path: Path, body: str = "#!/bin/sh\nexit 0\n") -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def _layout(
    tmp_path: Path, help_options: tuple[str, ...] = REQUIRED_BWRAP_OPTIONS
) -> tuple[Path, dict[str, Any], Path]:
    roots = {name: tmp_path / name for name in ("site/workspace", "site/exchange", "state", "runtime", "broker")}
    for directory in (
        roots["site/workspace"] / ".httk-workspace/exchange",
        roots["site/exchange"] / "requests",
        roots["site/exchange"] / "responses",
        roots["state"],
        roots["runtime"],
        roots["broker"],
    ):
        directory.mkdir(parents=True)
    record = tmp_path / "bwrap-record.json"
    bwrap = roots["broker"] / "bwrap"
    options = " ".join(help_options)
    _write_executable(
        bwrap,
        "#!/usr/bin/python3\n"
        "import json, os, sys\n"
        "if sys.argv[1:] == ['--help']:\n"
        f"    print({options!r})\n"
        "else:\n"
        "    fds = {}\n"
        "    for name in os.listdir('/proc/self/fd'):\n"
        "        try: fds[name] = os.readlink('/proc/self/fd/' + name)\n"
        "        except OSError: pass\n"
        f"    open({str(record)!r}, 'w', encoding='utf-8').write(json.dumps({{'argv': sys.argv, 'env': dict(os.environ), 'fds': fds, 'ppid': os.getppid()}}))\n",
    )
    python = roots["runtime"] / "python"
    _write_executable(python)
    for name in ("sbatch", "squeue", "scancel"):
        _write_executable(roots["broker"] / name)
    policy: dict[str, Any] = {
        "format": "httk-workspace-daemon-policy",
        "format_version": 3,
        "workspace": str(roots["site/workspace"]),
        "workspace_id": str(uuid.uuid4()),
        "enrollment_id": "1" * 32,
        "exchange": str(roots["site/exchange"]),
        "state": str(roots["state"]),
        "snapshots": str(tmp_path / "snapshots"),
        "bwrap": str(bwrap),
        "python": str(python),
        "sbatch": str(roots["broker"] / "sbatch"),
        "squeue": str(roots["broker"] / "squeue"),
        "scancel": str(roots["broker"] / "scancel"),
        "cluster": "test-cluster",
        "readonly_paths": [str(roots["runtime"])],
        "authorized_keys": [AUTHORIZED_KEY],
        "profiles": {"small": {"cpus": 2, "memory_mb": 1024, "time_minutes": 10}},
    }
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    policy_path.chmod(0o600)
    return policy_path, policy, record


def _rewrite_policy(path: Path, policy: dict[str, Any]) -> None:
    path.write_text(json.dumps(policy), encoding="utf-8")
    path.chmod(0o600)


def _run(
    tmp_path: Path,
    policy_path: Path,
    arguments: list[str],
    *,
    close_fds: bool = True,
    slurm: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    hostile = tmp_path / "hostile"
    hostile.mkdir(exist_ok=True)
    marker = tmp_path / "hostile-imported"
    (hostile / "sitecustomize.py").write_text(f"open({str(marker)!r}, 'w').write('site')\n", encoding="utf-8")
    package = hostile / "httk"
    package.mkdir(exist_ok=True)
    (package / "__init__.py").write_text(f"open({str(marker)!r}, 'w').write('httk')\n", encoding="utf-8")
    environment = {name: value for name, value in os.environ.items() if not name.startswith("SLURM_")}
    environment.update(slurm or {})
    environment.update(
        {"PYTHONPATH": str(hostile), "PYTHONSTARTUP": str(hostile / "sitecustomize.py"), "POISON": "yes"}
    )
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(BOOTSTRAP), "--policy", str(policy_path), *arguments],
        cwd=hostile,
        env=environment,
        close_fds=close_fds,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert not marker.exists()
    return result


def _pairs(argv: list[str], option: str) -> list[tuple[str, str]]:
    return [(argv[index + 1], argv[index + 2]) for index, item in enumerate(argv) if item == option]


def _set_environment(argv: list[str]) -> dict[str, str]:
    return {argv[index + 1]: argv[index + 2] for index, item in enumerate(argv) if item == "--setenv"}


def test_descriptor_cleanup_is_idempotent_and_closes_new_fd_after_close_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    prepared_type = api["_PreparedSandbox"]
    read_descriptor, write_descriptor = os.pipe()
    prepared = prepared_type([], (read_descriptor, write_descriptor))
    prepared.close()
    prepared.close()
    assert prepared.descriptors == ()
    with pytest.raises(OSError):
        os.fstat(read_descriptor)
    with pytest.raises(OSError):
        os.fstat(write_descriptor)

    opened = iter((10, 11))
    closed: list[int] = []

    def fake_open(*_args: object, **_kwargs: object) -> int:
        return next(opened)

    def fake_close(descriptor: int) -> None:
        closed.append(descriptor)
        if descriptor == 10:
            raise OSError("injected close failure")

    module_os = api["os"]
    fake_os = SimpleNamespace(
        open=fake_open,
        close=fake_close,
        O_RDONLY=module_os.O_RDONLY,
        O_DIRECTORY=module_os.O_DIRECTORY,
        O_CLOEXEC=module_os.O_CLOEXEC,
        O_NOFOLLOW=module_os.O_NOFOLLOW,
    )
    monkeypatch.setitem(api["_open_directory_nofollow"].__globals__, "os", fake_os)
    with pytest.raises(OSError, match="injected"):
        api["_open_directory_nofollow"](Path("/child"))
    assert closed == [10, 11]


def test_broker_boundary_has_exact_roles_and_clean_launch(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    leak = tmp_path / "inherited-secret"
    with leak.open("w", encoding="utf-8") as stream:
        os.set_inheritable(stream.fileno(), True)
        result = _run(
            tmp_path,
            policy_path,
            ["--workspace", str(policy["workspace"]), "--mode", "broker", "--once"],
            close_fds=False,
        )
    assert result.returncode == 0, result.stderr
    observed = json.loads(record.read_text(encoding="utf-8"))
    argv = observed["argv"]
    assert "--unshare-net" not in argv
    assert "--clearenv" in argv
    for option in (
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--disable-userns",
        "--assert-userns-disabled",
        "--new-session",
        "--die-with-parent",
    ):
        assert option in argv
    assert argv[argv.index("--cap-drop") + 1] == "ALL"
    assert _set_environment(argv) == {
        "PATH": "/usr/bin:/bin",
        "HOME": "/tmp/home",
        "TMPDIR": "/tmp",
        "LANG": "C.UTF-8",
    }
    # The broker sees the host read-only; every directory it adds lies on its private /tmp.
    mounts = argv.index("--ro-bind")
    assert argv[mounts : mounts + 10] == [
        "--ro-bind",
        *("/", "/"),
        *("--proc", "/proc"),
        *("--dev", "/dev"),
        *("--tmpfs", "/tmp"),
        "--dir",
    ]
    assert argv.count("--ro-bind") == 1 and "--ro-bind-fd" not in argv
    assert all(argv[index + 1].startswith("/tmp/") for index, item in enumerate(argv) if item == "--dir")
    assert not {"/workspace", "/run", "/daemon-root", "/control", "/daemon-policy.json"} & set(argv)
    assert [destination for _, destination in _pairs(argv, "--ro-bind-data")] == ["/tmp/daemon-policy.json"]
    assert "POISON" not in observed["env"]
    assert str(leak) not in observed["fds"].values()
    writable = _pairs(argv, "--bind-fd")
    assert sorted(destination for _, destination in writable) == ["/tmp/control", "/tmp/daemon-root"]
    root_fd = next(source for source, destination in writable if destination == "/tmp/daemon-root")
    assert observed["fds"][root_fd] == str(tmp_path / "site")
    state_fd = next(source for source, destination in writable if destination == "/tmp/control")
    assert observed["fds"][state_fd] == policy["state"]
    separator = argv.index("--")
    assert argv[separator + 1 :] == [
        policy["python"],
        "-I",
        "-m",
        "httk.workflow._daemon_service",
        "--policy",
        "/tmp/daemon-policy.json",
        "--policy-source",
        str(policy_path),
        "--once",
    ]


def test_bootstrap_has_no_runtime_initialization_route(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    result = _run(
        tmp_path,
        policy_path,
        ["--workspace", str(policy["workspace"]), "--mode", "broker", "--initialize"],
    )
    assert result.returncode == 2
    assert "unrecognized arguments: --initialize" in result.stderr
    assert not record.exists()


def test_payload_excludes_broker_mounts_and_network(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "payload", "--profile", "small", "--handle", "a" * 32],
    )
    assert result.returncode == 0, result.stderr
    observed = json.loads(record.read_text(encoding="utf-8"))
    argv = observed["argv"]
    assert "--unshare-net" in argv
    assert "--ro-bind" not in argv
    assert {destination for _, destination in _pairs(argv, "--bind-fd")} == {"/workspace"}
    assert {destination for _, destination in _pairs(argv, "--ro-bind-fd")} == set(policy["readonly_paths"])
    assert [destination for _, destination in _pairs(argv, "--ro-bind-data")] == ["/daemon-policy.json"]
    assert "/tmp/daemon-root" not in argv and "/tmp/control" not in argv
    # Bubblewrap runs as a child that still sees the prepared descriptor numbers.
    assert observed["ppid"] != os.getpid()
    workspace_fd = next(source for source, destination in _pairs(argv, "--bind-fd") if destination == "/workspace")
    assert observed["fds"][workspace_fd] == policy["workspace"]
    # Without SLURM_JOB_ID there is no job log to copy, and no complaint about it.
    assert "job log" not in result.stderr
    assert not list((Path(policy["workspace"]) / ".httk-workspace").glob("*daemon-job*"))
    separator = argv.index("--")
    assert argv[separator + 1 :] == [
        policy["python"],
        "-I",
        "-m",
        "httk.workflow._daemon_payload",
        "--profile",
        "small",
        "--handle",
        "a" * 32,
        "--procs",
        "2",
        "--mem-mb",
        "1024",
    ]


NETWORK_NOTE = "daemon bootstrap: note: job network isolation is disabled (daemon.isolate_network=false)"


@pytest.mark.parametrize("isolate", [True, False])
@pytest.mark.parametrize("mode", ["broker", "payload"])
def test_payload_network_isolation_follows_the_policy(tmp_path: Path, mode: str, isolate: bool) -> None:
    policy_path, policy, record = _layout(tmp_path)
    if not isolate:
        _rewrite_policy(policy_path, {**policy, "isolate_network": False})
    arguments = (
        ["--workspace", str(policy["workspace"]), "--mode", "broker", "--check"]
        if mode == "broker"
        else ["--mode", "payload", "--profile", "small", "--handle", "d" * 32]
    )
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 0, result.stderr
    argv = json.loads(record.read_text(encoding="utf-8"))["argv"]
    # Only the job sandbox ever unshares the network; every other namespace stays private.
    assert ("--unshare-net" in argv) == (mode == "payload" and isolate)
    assert {"--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts"} <= set(argv)
    assert (NETWORK_NOTE in result.stderr) == (mode == "payload" and not isolate)


@pytest.mark.parametrize(
    ("slurm", "memory_mb", "expected"),
    [
        ({"SLURM_CPUS_ON_NODE": "6", "SLURM_MEM_PER_NODE": "3000"}, 1024, ["--procs", "6", "--mem-mb", "3000"]),
        ({"SLURM_CPUS_ON_NODE": "4", "SLURM_MEM_PER_CPU": "500"}, 1024, ["--procs", "4", "--mem-mb", "2000"]),
        ({"SLURM_CPUS_ON_NODE": "4", "SLURM_MEM_PER_NODE": "0"}, 1024, ["--procs", "4", "--mem-mb", "1024"]),
        ({}, 1024, ["--procs", "2", "--mem-mb", "1024"]),
        ({"SLURM_CPUS_ON_NODE": "3"}, None, ["--procs", "3"]),
    ],
)
def test_serial_payload_capacity_comes_from_allocation(
    tmp_path: Path, slurm: dict[str, str], memory_mb: int | None, expected: list[str]
) -> None:
    policy_path, policy, record = _layout(tmp_path)
    policy["profiles"]["small"] = {"cpus": 2, "memory_mb": memory_mb}
    _rewrite_policy(policy_path, policy)
    result = _run(tmp_path, policy_path, ["--mode", "payload", "--profile", "small", "--handle", "a" * 32], slurm=slurm)
    assert result.returncode == 0, result.stderr
    argv = json.loads(record.read_text(encoding="utf-8"))["argv"]
    assert argv[argv.index("httk.workflow._daemon_payload") + 5 :] == expected


@pytest.mark.parametrize(
    ("slurm", "cpus", "message"),
    [
        ({"SLURM_CPUS_ON_NODE": "4x"}, 2, "SLURM_CPUS_ON_NODE"),
        ({"SLURM_CPUS_ON_NODE": "04"}, 2, "SLURM_CPUS_ON_NODE"),
        ({"SLURM_CPUS_ON_NODE": "4", "SLURM_MEM_PER_CPU": "-1"}, 2, "SLURM_MEM_PER_CPU"),
        ({"SLURM_CPUS_ON_NODE": str(2**63)}, 2, "SLURM_CPUS_ON_NODE"),
        ({"SLURM_CPUS_ON_NODE": "2", "SLURM_MEM_PER_CPU": str(2**63 - 1)}, 2, "exceeds"),
        ({"SLURM_CPUS_ON_NODE": "0"}, 2, "did not report"),
        ({}, None, "did not report"),
    ],
)
def test_serial_payload_refuses_unusable_allocation_capacity(
    tmp_path: Path, slurm: dict[str, str], cpus: int | None, message: str
) -> None:
    policy_path, policy, record = _layout(tmp_path)
    policy["profiles"]["small"] = {"cpus": cpus}
    _rewrite_policy(policy_path, policy)
    result = _run(tmp_path, policy_path, ["--mode", "payload", "--profile", "small", "--handle", "a" * 32], slurm=slurm)
    assert result.returncode == 2
    assert message in result.stderr
    assert not record.exists()


def test_payload_does_not_require_broker_local_paths(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    for directory in ("requests", "responses"):
        (Path(policy["exchange"]) / directory).rmdir()
    Path(policy["state"]).rmdir()
    for field in ("sbatch", "squeue", "scancel"):
        Path(policy[field]).unlink()
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "payload", "--profile", "small", "--handle", "b" * 32],
    )
    assert result.returncode == 0, result.stderr
    assert record.exists()


def test_payload_does_not_require_slurm_clients(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    runtime = Path(policy["readonly_paths"][0])
    original_broker = Path(policy["bwrap"]).parent
    bwrap = runtime / "bwrap"
    bwrap.write_bytes(Path(policy["bwrap"]).read_bytes())
    bwrap.chmod(0o755)
    policy["bwrap"] = str(bwrap)
    missing_broker = tmp_path / "compute-node-absent-broker"
    for field in ("sbatch", "squeue", "scancel"):
        policy[field] = str(missing_broker / field)
    for child in original_broker.iterdir():
        child.unlink()
    original_broker.rmdir()
    _rewrite_policy(policy_path, policy)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "payload", "--profile", "small", "--handle", "c" * 32],
    )
    assert result.returncode == 0, result.stderr
    assert record.exists()


def test_aliases_within_shared_readonly_roots_are_allowed(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    runtime = Path(policy["readonly_paths"][0])
    alias = tmp_path / "runtime-alias"
    alias.symlink_to(runtime, target_is_directory=True)
    policy["readonly_paths"] = [str(runtime), str(alias)]
    _rewrite_policy(policy_path, policy)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "payload", "--profile", "small", "--handle", "d" * 32],
    )
    assert result.returncode == 0, result.stderr
    assert record.exists()


@pytest.mark.parametrize("alias_target", ["root", "workspace_ancestor"])
def test_resolved_runtime_roots_cannot_escape_role_boundaries(tmp_path: Path, alias_target: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    alias = tmp_path / "runtime-alias"
    if alias_target == "root":
        target = Path("/")
        python = alias / "usr/bin/python3"
    else:
        target = tmp_path
        python = alias / "runtime/python"
    alias.symlink_to(target, target_is_directory=True)
    policy["readonly_paths"] = [str(alias)]
    policy["python"] = str(python)
    _rewrite_policy(policy_path, policy)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "payload", "--profile", "small", "--handle", "e" * 32],
    )
    assert result.returncode == 2
    assert not record.exists()


def test_runtime_path_overlapping_payload_destination_is_refused(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    policy["readonly_paths"].append("/workspace/runtime")
    _rewrite_policy(policy_path, policy)
    result = _run(tmp_path, policy_path, ["--workspace", str(policy["workspace"]), "--mode", "broker", "--once"])
    assert result.returncode == 2
    assert "reserved destination /workspace" in result.stderr
    assert not record.exists()


@pytest.mark.parametrize("defect", ["world_writable", "foreign_owner", "mutable_root"])
def test_slurm_client_outside_readonly_paths_keeps_ownership_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, defect: str
) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    _, policy, _ = _layout(tmp_path)
    sbatch = Path(policy["sbatch"])
    mutable = (Path(policy["state"]), tmp_path / "site")
    # The fixture's Slurm clients lie outside every readonly path; the broker sees them through its host view.
    api["_check_command"](sbatch, mutable)
    if defect == "world_writable":
        sbatch.chmod(0o757)
    elif defect == "foreign_owner":
        monkeypatch.setattr(api["os"], "geteuid", lambda: sbatch.stat().st_uid + 1)
    else:
        sbatch = Path(policy["state"]) / "sbatch"
        _write_executable(sbatch)
    with pytest.raises(ValueError, match="unprotected ownership or mode|across a mutable root"):
        api["_check_command"](sbatch, mutable)


@pytest.mark.parametrize("mode", ["root", "workspace", "exchange", "state"])
def test_mutable_root_symlink_is_refused(tmp_path: Path, mode: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    original = Path(policy[mode]) if mode in policy else Path(policy["exchange"]).parent
    target = tmp_path / f"{mode}-target"
    original.rename(target)
    original.symlink_to(target, target_is_directory=True)
    arguments = ["--workspace", str(policy["workspace"]), "--mode", "broker"]
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
    assert not record.exists()


@pytest.mark.parametrize("mode", ["payload", "broker"])
def test_layout_is_rechecked_before_every_sandbox_entry(tmp_path: Path, mode: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    (tmp_path / "site" / "intruder").mkdir()
    arguments = (
        ["--workspace", str(policy["workspace"]), "--mode", "broker", "--once"]
        if mode == "broker"
        else ["--mode", "payload", "--profile", "small", "--handle", "a" * 32]
    )
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
    assert "must contain only workspace and exchange; found intruder" in result.stderr
    assert not record.exists()
    (tmp_path / "site" / "intruder").rmdir()
    (Path(policy["workspace"]) / ".httk-workspace/exchange").rmdir()
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
    assert "workspace staging directory" in result.stderr and "--reload" in result.stderr
    assert not record.exists()


def test_policy_file_must_be_protected(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    policy_path.chmod(0o602)
    result = _run(tmp_path, policy_path, ["--workspace", str(policy["workspace"]), "--mode", "broker"])
    assert result.returncode == 2
    assert "world-writable" in result.stderr
    assert not record.exists()


@pytest.mark.parametrize("target", ["policy", "bwrap"])
def test_group_writable_sources_are_accepted_but_world_writable_refused(target: str, tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    path = policy_path if target == "policy" else Path(policy["bwrap"])
    base = path.stat().st_mode & 0o7777
    arguments = ["--workspace", str(policy["workspace"]), "--mode", "broker", "--once"]
    path.chmod(base | 0o020)
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 0, result.stderr
    record.unlink()
    path.chmod(base | 0o002)
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
    assert not record.exists()


@pytest.mark.parametrize(
    "arguments",
    [
        ["--mode", "broker"],
        ["--workspace", "/wrong", "--mode", "broker"],
        ["--workspace", "relative", "--mode", "broker"],
        ["--mode", "payload", "--profile", "unknown", "--handle", "a" * 32],
        ["--mode", "payload", "--profile", "small", "--handle", "../bad"],
        ["--workspace", "/tmp", "--mode", "payload", "--profile", "small", "--handle", "a" * 32],
        ["--workspace", "/tmp", "--mode", "broker", "--", "sh"],
        ["--mode", "payload", "--profile", "small", "--handle", "a" * 32, "--procs", "2"],
        ["--mode", "payload", "--profile", "small", "--handle", "a" * 32, "--mem-mb", "1024"],
        ["--workspace", "/tmp", "--mode", "broker", "--procs", "2"],
    ],
)
def test_role_specific_arguments_are_strict(tmp_path: Path, arguments: list[str]) -> None:
    policy_path, _, record = _layout(tmp_path)
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
    assert not record.exists()


def test_missing_bwrap_feature_is_refused(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    bwrap = Path(policy["bwrap"])
    options = " ".join(option for option in REQUIRED_BWRAP_OPTIONS if option not in ("--bind-fd", "--clearenv"))
    _write_executable(bwrap, f"#!/bin/sh\necho 'usage: bwrap [OPTIONS...]' {options}\n")
    result = _run(tmp_path, policy_path, ["--workspace", str(policy["workspace"]), "--mode", "broker"])
    assert result.returncode == 2
    assert "lacks required confinement features: --bind-fd, --clearenv" in result.stderr
    assert not record.exists()


@pytest.mark.parametrize("userns", [True, False])
@pytest.mark.parametrize("mode", ["broker", "payload"])
def test_userns_block_follows_bwrap_capability(tmp_path: Path, mode: str, userns: bool) -> None:
    blocking = ("--disable-userns", "--assert-userns-disabled")
    help_options = tuple(option for option in REQUIRED_BWRAP_OPTIONS if userns or option not in blocking)
    policy_path, policy, record = _layout(tmp_path, help_options)
    arguments = (
        ["--workspace", str(policy["workspace"]), "--mode", "broker", "--check"]
        if mode == "broker"
        else ["--mode", "payload", "--profile", "small", "--handle", "c" * 32]
    )
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 0, result.stderr
    argv = json.loads(record.read_text(encoding="utf-8"))["argv"]
    assert all((option in argv) == userns for option in blocking)
    warning = (
        "daemon bootstrap: warning: this Bubblewrap lacks --disable-userns (0.8.0+); "
        "sandboxed code can create nested user namespaces"
    )
    assert (warning in result.stderr) == (mode == "broker" and not userns)


def test_o_path_fallback_matches_the_platform_constant() -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    if hasattr(os, "O_PATH"):
        assert api["_O_PATH"] == os.O_PATH
    assert api["_O_PATH"] == 0o10000000


def test_linux_constant_fallbacks_match_the_platform() -> None:
    import fcntl

    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    assert (api["_MFD_CLOEXEC"], api["_MFD_ALLOW_SEALING"], api["_F_ADD_SEALS"], api["_SEALS"]) == (1, 2, 1033, 15)
    if hasattr(fcntl, "F_ADD_SEALS"):
        assert fcntl.F_ADD_SEALS == 1033
        assert fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE == 15


def test_policy_snapshot_without_the_os_memfd_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    module_os = api["os"]
    monkeypatch.delattr(module_os, "memfd_create", raising=False)
    descriptor = api["_policy_snapshot"](b"policy-bytes")
    try:
        assert os.pread(descriptor, 64, 0) == b"policy-bytes"
        with pytest.raises(OSError):
            os.write(descriptor, b"x")
    finally:
        os.close(descriptor)


def test_exec_marks_o_path_descriptors_inheritable_without_os_set_inheritable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    module_os = api["os"]
    descriptor = os.open(tmp_path, api["_O_PATH"] | os.O_CLOEXEC)
    observed: list[bool] = []

    def refuse(*_args: object) -> None:
        raise OSError(9, "Bad file descriptor")

    def execve(*_args: object) -> None:
        observed.append(os.get_inheritable(descriptor))
        raise OSError("exec boundary")

    monkeypatch.setattr(module_os, "set_inheritable", refuse)
    monkeypatch.setattr(module_os, "execve", execve)
    monkeypatch.setitem(api["_exec_prepared"].__globals__, "_open_descriptor_numbers", lambda: set())
    prepared = api["_PreparedSandbox"](["/bin/true"], (descriptor,))
    with pytest.raises(OSError, match="exec boundary"):
        api["_exec_prepared"](prepared)
    assert observed == [True]


def test_merged_usr_links_are_recreated_inside_the_sandbox(tmp_path: Path) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    usr = tmp_path / "usr"
    (usr / "lib64").mkdir(parents=True)
    (usr / "bin").mkdir()
    lib64 = tmp_path / "lib64"
    lib64.symlink_to("usr/lib64")
    sbin = tmp_path / "sbin"
    sbin.symlink_to("/nowhere/sbin")
    plain = tmp_path / "bin"
    plain.mkdir()
    argv = api["_merged_usr_symlinks"]((usr.resolve(),), set(), (lib64, sbin, plain))
    assert argv == ["--symlink", "usr/lib64", str(lib64)]
    assert api["_merged_usr_symlinks"]((usr.resolve(),), {lib64}, (lib64,)) == []


def _job_output(policy: dict[str, Any], job_id: str = "123") -> Path:
    jobs = Path(policy["snapshots"]) / "jobs"
    jobs.mkdir(parents=True, mode=0o700, exist_ok=True)
    return jobs / f"httk-{job_id}.out"


def test_serial_payload_returns_bwrap_status_and_replaces_planted_job_log(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    bwrap = Path(policy["bwrap"])
    bwrap.write_text(bwrap.read_text(encoding="utf-8") + "    sys.exit(3)\n", encoding="utf-8")
    content = b"head" + b"x" * (1024 * 1024) + b"slurm said: failed\n"
    _job_output(policy).write_bytes(content)
    outside = tmp_path / "outside.txt"
    outside.write_text("untouched", encoding="utf-8")
    log = Path(policy["workspace"]) / ".httk-workspace" / f"daemon-job-{'a' * 32}.log"
    log.symlink_to(outside)
    arguments = ["--mode", "payload", "--profile", "small", "--handle", "a" * 32]
    result = _run(tmp_path, policy_path, arguments, slurm={"SLURM_JOB_ID": "123"})
    assert result.returncode == 3, result.stderr
    assert json.loads(record.read_text(encoding="utf-8"))["ppid"] != os.getpid()
    assert not log.is_symlink() and log.stat().st_mode & 0o777 == 0o600
    assert log.read_bytes() == content[-1024 * 1024 :]
    assert outside.read_text(encoding="utf-8") == "untouched"
    assert [path.name for path in log.parent.glob(".daemon-job-*")] == []


def test_refused_serial_payload_still_copies_the_job_log(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    _job_output(policy).write_text("refusal context\n", encoding="utf-8")
    arguments = ["--mode", "payload", "--profile", "small", "--handle", "a" * 32]
    result = _run(tmp_path, policy_path, arguments, slurm={"SLURM_JOB_ID": "123", "SLURM_CPUS_ON_NODE": "4x"})
    assert result.returncode == 2
    assert "SLURM_CPUS_ON_NODE" in result.stderr
    assert not record.exists()
    log = Path(policy["workspace"]) / ".httk-workspace" / f"daemon-job-{'a' * 32}.log"
    assert log.read_text(encoding="utf-8") == "refusal context\n"


def test_job_log_source_must_be_a_regular_file(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    os.mkfifo(_job_output(policy))
    arguments = ["--mode", "payload", "--profile", "small", "--handle", "a" * 32]
    result = _run(tmp_path, policy_path, arguments, slurm={"SLURM_JOB_ID": "123"})
    assert result.returncode == 0, result.stderr
    assert record.exists()
    assert "could not copy the job log" in result.stderr and "not a regular file" in result.stderr
    assert not list((Path(policy["workspace"]) / ".httk-workspace").glob("*daemon-job*"))


def test_serial_payload_forwards_sigterm_to_bwrap(tmp_path: Path) -> None:
    policy_path, policy, _ = _layout(tmp_path)
    ready, received = tmp_path / "bwrap-ready", tmp_path / "bwrap-received"
    _write_executable(
        Path(policy["bwrap"]),
        "#!/usr/bin/python3\n"
        "import signal, sys, time\n"
        "if sys.argv[1:] == ['--help']:\n"
        f"    print({' '.join(REQUIRED_BWRAP_OPTIONS)!r})\n"
        "    sys.exit()\n"
        "def handler(signum, _frame):\n"
        f"    open({str(received)!r}, 'w').write(str(signum))\n"
        "    sys.exit(7)\n"
        "signal.signal(signal.SIGTERM, handler)\n"
        f"open({str(ready)!r}, 'w').close()\n"
        "time.sleep(60)\n"
        "sys.exit(9)\n",
    )
    environment = {name: value for name, value in os.environ.items() if not name.startswith("SLURM_")}
    process = subprocess.Popen(
        [sys.executable, "-I", "-S", str(BOOTSTRAP), "--policy", str(policy_path)]
        + ["--mode", "payload", "--profile", "small", "--handle", "a" * 32],
        env=environment,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 30
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=30) == 7
    finally:
        process.kill()
        process.wait()
        if process.stderr is not None:
            process.stderr.close()
    assert received.read_text(encoding="utf-8") == str(int(signal.SIGTERM))
