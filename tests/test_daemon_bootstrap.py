"""Test broker bootstrap argument and descriptor boundaries with a non-confining recorder."""

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
SANDBOX = BOOTSTRAP.with_name("_sandbox.py")
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
        "format_version": 4,
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
        "authorized_keys": [AUTHORIZED_KEY],
        "launchers": {"small": {"settings": {"manager.confine": "bwrap"}, "digest": "a" * 64}},
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
    environment = {name: value for name, value in os.environ.items() if not name.startswith("SLURM_")}
    environment.update(
        {
            "PYTHONPATH": str(hostile),
            "PYTHONSTARTUP": str(hostile / "sitecustomize.py"),
            "POISON": "yes",
            "NSC_RESOURCE_NAME": "tetralith",
            "SBATCH_ACCOUNT": "other",
            "XDG_RUNTIME_DIR": "/run/user/1",
        }
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


def test_operator_environment_drops_submission_interpreter_and_malformed_variables(
    capsys: pytest.CaptureFixture[str],
) -> None:
    operator_environment = runpy.run_path(str(BOOTSTRAP))["_operator_environment"]
    dropped = {
        **{f"{prefix}X": "1" for prefix in ("SBATCH_", "SALLOC_", "SRUN_", "SLURM_", "PYTHON")},
        **dict.fromkeys(("BASH_ENV", "ENV", "LD_PRELOAD", "LD_LIBRARY_PATH", "1BAD", "BAD-NAME", "A=B", ""), "1"),
        "NUL": "a\0b",
    }
    kept = {"NSC_RESOURCE_NAME": "tetralith", "MODULEPATH": "/software/modules", "SLURM_CONF": "/etc/s.conf"}
    assert operator_environment({**dropped, **kept}) == dict(sorted(kept.items()))
    assert capsys.readouterr().err == ""

    many = {f"V{index:04d}": "x" for index in range(600)}
    assert list(operator_environment(many)) == sorted(many)[:512]
    assert capsys.readouterr().err.count("was dropped from V0512") == 1
    large = {f"V{index}": "x" * 100 * 1024 for index in range(3)}
    assert list(operator_environment(large)) == ["V0", "V1"]
    assert "was dropped from V2" in capsys.readouterr().err


def test_broker_environment_is_the_fixed_one_plus_the_filtered_operator_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    monkeypatch.setenv("NSC_RESOURCE_NAME", "tetralith")
    monkeypatch.setenv("SBATCH_ACCOUNT", "other")
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/1")
    policy = SimpleNamespace(bwrap=Path("/usr/bin/bwrap"), slurm_conf=tmp_path / "slurm.conf")
    argv = api["_base_bwrap_argv"](policy, block_userns=True)
    environment = _set_environment(argv)
    assert api["_FIXED_ENVIRONMENT"] == {
        "PATH": "/usr/bin:/bin",
        "HOME": "/tmp/home",
        "TMPDIR": "/tmp",
        "LANG": "C.UTF-8",
    }
    assert environment["NSC_RESOURCE_NAME"] == "tetralith" and environment["SLURM_CONF"] == str(policy.slurm_conf)
    assert (environment["HOME"], environment["XDG_CACHE_HOME"]) == ("/tmp/home", "/tmp/home/.cache")
    assert not {"SBATCH_ACCOUNT", "XDG_RUNTIME_DIR"} & set(environment)
    assert "--unshare-net" not in argv and "/tmp/home/.cache" in argv


def test_broker_boundary_has_exact_roles_and_clean_launch(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    leak = tmp_path / "inherited-secret"
    with leak.open("w", encoding="utf-8") as stream:
        os.set_inheritable(stream.fileno(), True)
        result = _run(
            tmp_path,
            policy_path,
            ["--workspace", str(policy["workspace"]), "--once"],
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
    environment = _set_environment(argv)
    assert environment["NSC_RESOURCE_NAME"] == "tetralith" and environment["POISON"] == "yes"
    assert environment["HOME"] == "/tmp/home" and environment["XDG_CACHE_HOME"] == "/tmp/home/.cache"
    assert environment["TMPDIR"] == "/tmp" and environment["PATH"] == os.environ["PATH"]
    assert not {"SBATCH_ACCOUNT", "XDG_RUNTIME_DIR", "PYTHONPATH", "PYTHONSTARTUP"} & set(environment)
    assert not any(name.startswith("SBATCH_") for name in environment)
    assert "/tmp/home/.cache" in [argv[index + 1] for index, item in enumerate(argv) if item == "--dir"]
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
        ["--workspace", str(policy["workspace"]), "--initialize"],
    )
    assert result.returncode == 2
    assert "unrecognized arguments: --initialize" in result.stderr
    assert not record.exists()


@pytest.mark.parametrize("defect", ["world_writable", "foreign_owner", "mutable_root"])
def test_broker_commands_keep_ownership_checks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, defect: str) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    _, policy, _ = _layout(tmp_path)
    sbatch = Path(policy["sbatch"])
    mutable = (Path(policy["state"]), tmp_path / "site")
    # The broker sees the Slurm clients through its read-only host view, so only ownership and roots matter.
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
    arguments = ["--workspace", str(policy["workspace"])]
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
    assert not record.exists()


def test_layout_is_rechecked_before_every_sandbox_entry(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    (tmp_path / "site" / "intruder").mkdir()
    arguments = ["--workspace", str(policy["workspace"]), "--once"]
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
    result = _run(tmp_path, policy_path, ["--workspace", str(policy["workspace"])])
    assert result.returncode == 2
    assert "world-writable" in result.stderr
    assert not record.exists()


@pytest.mark.parametrize("target", ["policy", "bwrap"])
def test_group_writable_sources_are_accepted_but_world_writable_refused(target: str, tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    path = policy_path if target == "policy" else Path(policy["bwrap"])
    base = path.stat().st_mode & 0o7777
    arguments = ["--workspace", str(policy["workspace"]), "--once"]
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
        [],
        ["--workspace", "/wrong"],
        ["--workspace", "relative"],
        ["--workspace", "/tmp", "--", "sh"],
        ["--workspace", "/tmp", "--mode", "broker"],
        ["--workspace", "/tmp", "--mode", "payload", "--profile", "small", "--handle", "a" * 32],
        ["--workspace", "/tmp", "--procs", "2"],
        ["--workspace", "/tmp", "--control-source", "/tmp/x"],
        ["--workspace", "/tmp", "--check", "--once"],
    ],
)
def test_bootstrap_arguments_are_strict(tmp_path: Path, arguments: list[str]) -> None:
    policy_path, _, record = _layout(tmp_path)
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 2
    assert not record.exists()


def test_missing_bwrap_feature_is_refused(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    bwrap = Path(policy["bwrap"])
    options = " ".join(option for option in REQUIRED_BWRAP_OPTIONS if option not in ("--bind-fd", "--clearenv"))
    _write_executable(bwrap, f"#!/bin/sh\necho 'usage: bwrap [OPTIONS...]' {options}\n")
    result = _run(tmp_path, policy_path, ["--workspace", str(policy["workspace"])])
    assert result.returncode == 2
    assert "lacks required confinement features: --bind-fd, --clearenv" in result.stderr
    assert not record.exists()


@pytest.mark.parametrize("userns", [True, False])
def test_userns_block_follows_bwrap_capability(tmp_path: Path, userns: bool) -> None:
    blocking = ("--disable-userns", "--assert-userns-disabled")
    help_options = tuple(option for option in REQUIRED_BWRAP_OPTIONS if userns or option not in blocking)
    policy_path, policy, record = _layout(tmp_path, help_options)
    arguments = ["--workspace", str(policy["workspace"]), "--check"]
    result = _run(tmp_path, policy_path, arguments)
    assert result.returncode == 0, result.stderr
    argv = json.loads(record.read_text(encoding="utf-8"))["argv"]
    assert all((option in argv) == userns for option in blocking)
    warning = (
        "daemon bootstrap: warning: this Bubblewrap lacks --disable-userns (0.8.0+); "
        "sandboxed code can create nested user namespaces"
    )
    assert (warning in result.stderr) == (not userns)


def test_o_path_fallback_matches_the_platform_constant() -> None:
    api: dict[str, Any] = runpy.run_path(str(SANDBOX))
    if hasattr(os, "O_PATH"):
        assert api["O_PATH"] == os.O_PATH
    assert api["O_PATH"] == 0o10000000


def test_linux_constant_fallbacks_match_the_platform() -> None:
    import fcntl

    api: dict[str, Any] = runpy.run_path(str(SANDBOX))
    assert (api["MFD_CLOEXEC"], api["MFD_ALLOW_SEALING"], api["F_ADD_SEALS"], api["SEALS"]) == (1, 2, 1033, 15)
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
    descriptor = os.open(tmp_path, runpy.run_path(str(SANDBOX))["O_PATH"] | os.O_CLOEXEC)
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
    api: dict[str, Any] = runpy.run_path(str(SANDBOX))
    usr = tmp_path / "usr"
    (usr / "lib64").mkdir(parents=True)
    (usr / "bin").mkdir()
    lib64 = tmp_path / "lib64"
    lib64.symlink_to("usr/lib64")
    sbin = tmp_path / "sbin"
    sbin.symlink_to("/nowhere/sbin")
    plain = tmp_path / "bin"
    plain.mkdir()
    argv = api["merged_usr_symlinks"]((usr.resolve(),), set(), (lib64, sbin, plain))
    assert argv == ["--symlink", "usr/lib64", str(lib64)]
    assert api["merged_usr_symlinks"]((usr.resolve(),), {lib64}, (lib64,)) == []
