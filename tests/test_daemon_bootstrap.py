"""Test bootstrap argument and descriptor boundaries with a non-confining recorder."""

import base64
import json
import os
import runpy
import subprocess
import sys
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


def _layout(tmp_path: Path) -> tuple[Path, dict[str, Any], Path]:
    roots = {name: tmp_path / name for name in ("workspace", "requests", "responses", "state", "runtime", "broker")}
    for root in roots.values():
        root.mkdir()
    record = tmp_path / "bwrap-record.json"
    bwrap = roots["broker"] / "bwrap"
    options = " ".join(REQUIRED_BWRAP_OPTIONS)
    _write_executable(
        bwrap,
        "#!/usr/bin/python3\n"
        "import json, os, sys\n"
        "if sys.argv[1:] == ['--version']:\n"
        "    print('bubblewrap 0.9.0')\n"
        "elif sys.argv[1:] == ['--help']:\n"
        f"    print({options!r})\n"
        "else:\n"
        "    fds = {}\n"
        "    for name in os.listdir('/proc/self/fd'):\n"
        "        try: fds[name] = os.readlink('/proc/self/fd/' + name)\n"
        "        except OSError: pass\n"
        f"    open({str(record)!r}, 'w', encoding='utf-8').write(json.dumps({{'argv': sys.argv, 'env': dict(os.environ), 'fds': fds}}))\n",
    )
    python = roots["runtime"] / "python"
    _write_executable(python)
    for name in ("sbatch", "squeue", "scancel"):
        _write_executable(roots["broker"] / name)
    policy: dict[str, Any] = {
        "format": "httk-workspace-daemon-policy",
        "format_version": 1,
        "workspace": str(roots["workspace"]),
        "workspace_id": str(uuid.uuid4()),
        "enrollment_id": "1" * 32,
        "requests": str(roots["requests"]),
        "responses": str(roots["responses"]),
        "state": str(roots["state"]),
        "bwrap": str(bwrap),
        "python": str(python),
        "sbatch": str(roots["broker"] / "sbatch"),
        "squeue": str(roots["broker"] / "squeue"),
        "scancel": str(roots["broker"] / "scancel"),
        "cluster": "test-cluster",
        "readonly_paths": [str(roots["runtime"])],
        "broker_paths": [str(roots["broker"])],
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
) -> subprocess.CompletedProcess[str]:
    hostile = tmp_path / "hostile"
    hostile.mkdir(exist_ok=True)
    marker = tmp_path / "hostile-imported"
    (hostile / "sitecustomize.py").write_text(f"open({str(marker)!r}, 'w').write('site')\n", encoding="utf-8")
    package = hostile / "httk"
    package.mkdir(exist_ok=True)
    (package / "__init__.py").write_text(f"open({str(marker)!r}, 'w').write('httk')\n", encoding="utf-8")
    environment = dict(os.environ)
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
    assert argv[argv.index("--tmpfs") + 1] == "/tmp"
    assert "/proc" in argv and "/dev" in argv
    assert "/daemon-policy.json" in argv
    assert "POISON" not in observed["env"]
    assert str(leak) not in observed["fds"].values()
    readonly_destinations = {destination for _, destination in _pairs(argv, "--ro-bind-fd")}
    assert readonly_destinations == {"/workspace", *policy["readonly_paths"], *policy["broker_paths"]}
    writable_destinations = {destination for _, destination in _pairs(argv, "--bind-fd")}
    assert writable_destinations == {"/requests", "/responses", "/control"}
    separator = argv.index("--")
    assert argv[separator + 1 :] == [
        policy["python"],
        "-I",
        "-m",
        "httk.workflow._daemon_service",
        "--policy",
        "/daemon-policy.json",
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
    assert {destination for _, destination in _pairs(argv, "--bind-fd")} == {"/workspace"}
    destinations = {destination for _, destination in _pairs(argv, "--bind-fd") + _pairs(argv, "--ro-bind-fd")}
    assert not {"/requests", "/responses", "/control", str(Path(policy["broker_paths"][0]))} & destinations
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
    ]


def test_payload_does_not_require_broker_local_paths(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    for field in ("requests", "responses", "state"):
        Path(policy[field]).rmdir()
    for field in ("sbatch", "squeue", "scancel"):
        Path(policy[field]).unlink()
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "payload", "--profile", "small", "--handle", "b" * 32],
    )
    assert result.returncode == 0, result.stderr
    assert record.exists()


def test_payload_does_not_require_declared_broker_root_or_slurm_clients(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    runtime = Path(policy["readonly_paths"][0])
    original_broker = Path(policy["broker_paths"][0])
    bwrap = runtime / "bwrap"
    bwrap.write_bytes(Path(policy["bwrap"]).read_bytes())
    bwrap.chmod(0o755)
    policy["bwrap"] = str(bwrap)
    missing_broker = tmp_path / "compute-node-absent-broker"
    policy["broker_paths"] = [str(missing_broker)]
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


@pytest.mark.parametrize("alias_target", ["root", "workspace_ancestor", "broker"])
def test_resolved_runtime_roots_cannot_escape_role_boundaries(tmp_path: Path, alias_target: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    alias = tmp_path / "runtime-alias"
    if alias_target == "root":
        target = Path("/")
        python = alias / "usr/bin/python3"
    elif alias_target == "workspace_ancestor":
        target = tmp_path
        python = alias / "runtime/python"
    else:
        target = Path(policy["broker_paths"][0])
        python = alias / "python"
        _write_executable(target / "python")
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


@pytest.mark.parametrize("mode", ["workspace", "requests", "responses", "state"])
def test_mutable_root_symlink_is_refused(tmp_path: Path, mode: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    original = Path(policy[mode])
    original.rmdir()
    target = tmp_path / f"{mode}-target"
    target.mkdir()
    original.symlink_to(target, target_is_directory=True)
    arguments = ["--workspace", str(policy["workspace"]), "--mode", "broker"]
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
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
    _write_executable(
        bwrap,
        "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'bubblewrap 0.8.0'; else echo --bind-fd; fi\n",
    )
    result = _run(tmp_path, policy_path, ["--workspace", str(policy["workspace"]), "--mode", "broker"])
    assert result.returncode == 2
    assert "0.9.0 or newer" in result.stderr
    assert not record.exists()
