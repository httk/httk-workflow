"""Test MPI bootstrap trust boundaries with a non-confining Bubblewrap recorder."""

import base64
import json
import os
import runpy
import stat
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

BOOTSTRAP = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "_daemon_bootstrap.py"
AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")
HANDLE = "a" * 32
REQUEST_ID = "b" * 32
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
    roots = {
        name: tmp_path / name
        for name in (
            "site/workspace/.httk-workspace/exchange",
            "site/exchange",
            "state",
            "runtime",
            "broker",
            "mpi-control",
            "pmix",
            "shm",
        )
    }
    for root in roots.values():
        root.mkdir(parents=True)
    roots["workspace"] = tmp_path / "site/workspace"
    record = tmp_path / "bwrap-record.json"
    bwrap = roots["broker"] / "bwrap"
    options = " ".join(REQUIRED_BWRAP_OPTIONS)
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
    for name in ("sbatch", "squeue", "scancel", "srun"):
        _write_executable(roots["broker"] / name)
    policy: dict[str, Any] = {
        "format": "httk-workspace-daemon-policy",
        "format_version": 3,
        "workspace": str(roots["workspace"]),
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
        "profiles": {
            "serial": {"cpus": 2, "memory_mb": 1024, "time_minutes": 10},
            "parallel": {
                "cpus": 4,
                "memory_mb": 8192,
                "time_minutes": 60,
                "mpi": {"nodes": 2, "ranks": 8},
            },
        },
        "mpi": {
            "srun": str(roots["broker"] / "srun"),
            "control_root": str(roots["mpi-control"]),
            "pmix_roots": [str(roots["pmix"])],
            "shm_root": str(roots["shm"]),
            "devices": ["/dev/null"],
            "environment": {"OMPI_MCA_btl": "self,vader,tcp", "SITE_SETTING": "protected"},
            "max_steps": 8,
            "termination_grace": 2.0,
        },
    }
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    policy_path.chmod(0o600)
    return policy_path, policy, record


def _rewrite_policy(path: Path, policy: dict[str, Any]) -> None:
    path.write_text(json.dumps(policy), encoding="utf-8")
    path.chmod(0o600)


def _allocation_environment(**updates: str) -> dict[str, str]:
    environment = {name: value for name, value in os.environ.items() if not name.startswith("SLURM_")}
    environment.update(
        {
            "SLURM_RESTART_COUNT": "0",
            "SLURM_JOB_ID": "1234",
            "SLURMD_NODENAME": "node001",
            "SLURM_CLUSTER_NAME": "test-cluster",
        }
    )
    environment.update(updates)
    return environment


def _rank_environment(pmix: Path | None = None, **updates: str) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "SLURM_PROCID": "3",
            "SLURM_LOCALID": "1",
            "SLURM_NODEID": "0",
            "SLURM_NTASKS": "8",
            "SLURM_JOB_ID": "1234",
            "SLURMD_NODENAME": "node001",
            "SLURM_CLUSTER_NAME": "test-cluster",
            "PMI_RANK": "3",
            "PMIX_RANK": "3",
            "OMPI_COMM_WORLD_RANK": "3",
            "OPAL_PREFIX": "/trusted/openmpi",
            "UNRELATED_SECRET": "absent",
        }
    )
    if pmix is not None:
        environment["PMIX_SERVER_TMPDIR"] = str(pmix)
    environment.update(updates)
    return environment


def _run(
    tmp_path: Path,
    policy_path: Path,
    arguments: list[str],
    environment: dict[str, str],
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
    environment = dict(environment)
    environment.update({"PYTHONPATH": str(hostile), "PYTHONSTARTUP": str(hostile / "sitecustomize.py")})
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


def test_root_owned_sticky_shared_memory_parent_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    api: dict[str, Any] = runpy.run_path(str(BOOTSTRAP))
    monkeypatch.setattr(
        api["os"],
        "fstat",
        lambda _descriptor: SimpleNamespace(st_mode=stat.S_IFDIR | 0o1777, st_uid=0),
    )
    api["_validate_directory"](
        7,
        Path("/dev/shm"),
        allow_root_sticky_parent=True,
    )


def test_allocation_creates_private_control_and_enters_launcher_sandbox(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE],
        _allocation_environment(),
    )
    assert result.returncode == 0, result.stderr
    observed = json.loads(record.read_text(encoding="utf-8"))
    argv = observed["argv"]
    assert "--unshare-net" not in argv
    for option in (
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--disable-userns",
        "--assert-userns-disabled",
        "--new-session",
    ):
        assert option in argv
    # The allocation service sees the host read-only, like the broker, and writes only its control directory.
    mounts = argv.index("--ro-bind")
    assert argv[mounts : mounts + 9] == ["--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]
    assert "--ro-bind-fd" not in argv
    assert all(argv[index + 1].startswith("/tmp/") for index, item in enumerate(argv) if item == "--dir")
    assert not {"/workspace", "/run", "/run/httk-mpi", "/daemon-policy.json"} & set(argv)
    assert [destination for _, destination in _pairs(argv, "--ro-bind-data")] == ["/tmp/daemon-policy.json"]
    writable = _pairs(argv, "--bind-fd")
    assert [destination for _, destination in writable] == ["/tmp/httk-mpi"]
    control_fd = writable[0][0]
    control_source = Path(observed["fds"][control_fd])
    assert control_source.parent == Path(policy["mpi"]["control_root"])
    assert stat.S_IMODE(control_source.stat().st_mode) == 0o700
    separator = argv.index("--")
    assert argv[separator + 1 :] == [
        policy["python"],
        "-I",
        "-m",
        "httk.workflow._daemon_mpi_service",
        "--policy-source",
        str(policy_path),
        "--profile",
        "parallel",
        "--handle",
        HANDLE,
        "--job-id",
        "1234",
        "--node",
        "node001",
        "--control-source",
        str(control_source),
        "--procs",
        "32",
        "--mem-mb",
        "16384",
    ]


@pytest.mark.parametrize(
    ("slurm", "resources", "expected"),
    [
        ({"SLURM_MEM_PER_NODE": "3000"}, {}, ["--procs", "32", "--mem-mb", "6000"]),
        ({"SLURM_MEM_PER_CPU": "100", "SLURM_CPUS_ON_NODE": "16"}, {}, ["--procs", "32", "--mem-mb", "3200"]),
        ({"SLURM_MEM_PER_NODE": "0"}, {}, ["--procs", "32", "--mem-mb", "16384"]),
        ({}, {"cpus": None, "memory_mb": None}, ["--procs", "8"]),
        ({"SLURM_MEM_PER_CPU": "100"}, {"cpus": None}, ["--procs", "8", "--mem-mb", "800"]),
        ({"SLURM_CPUS_ON_NODE": "4x"}, {}, ["--procs", "32", "--mem-mb", "16384"]),
        (
            {},
            {"memory_mb": 1048576, "mpi": {"nodes": 4096, "ranks": 4096}},
            ["--procs", "16384", "--mem-mb", str(2**32)],
        ),
    ],
)
def test_allocation_passes_trusted_capacity_to_service(
    tmp_path: Path, slurm: dict[str, str], resources: dict[str, Any], expected: list[str]
) -> None:
    policy_path, policy, record = _layout(tmp_path)
    policy["profiles"]["parallel"].update(resources)
    _rewrite_policy(policy_path, policy)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE],
        _allocation_environment(**slurm),
    )
    assert result.returncode == 0, result.stderr
    argv = json.loads(record.read_text(encoding="utf-8"))["argv"]
    assert argv[argv.index("--control-source") + 2 :] == expected


def test_allocation_refuses_malformed_slurm_memory_before_creating_scratch(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE],
        _allocation_environment(SLURM_MEM_PER_NODE="3000M"),
    )
    assert result.returncode == 2
    assert "SLURM_MEM_PER_NODE" in result.stderr
    assert list(Path(policy["mpi"]["control_root"]).iterdir()) == []
    assert not record.exists()


@pytest.mark.parametrize("restart", ["1", "00", "-1", "x", "1.0", ""])
def test_allocation_refuses_restart_before_creating_scratch(tmp_path: Path, restart: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE],
        _allocation_environment(SLURM_RESTART_COUNT=restart),
    )
    assert result.returncode == 2
    assert "SLURM_RESTART_COUNT" in result.stderr
    assert list(Path(policy["mpi"]["control_root"]).iterdir()) == []
    assert not record.exists()


def test_allocation_validates_current_cluster_before_creating_scratch(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE],
        _allocation_environment(SLURM_CLUSTER_NAME="other"),
    )
    assert result.returncode == 2
    assert list(Path(policy["mpi"]["control_root"]).iterdir()) == []
    assert not record.exists()


def test_allocation_refuses_writable_srun_before_creating_scratch(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    Path(policy["mpi"]["srun"]).chmod(0o777)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE],
        _allocation_environment(),
    )
    assert result.returncode == 2
    assert "ownership or mode" in result.stderr
    assert list(Path(policy["mpi"]["control_root"]).iterdir()) == []
    assert not record.exists()


def test_broker_validates_configured_srun_as_protected_command(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    Path(policy["mpi"]["srun"]).chmod(0o777)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "broker", "--workspace", policy["workspace"], "--check"],
        dict(os.environ),
    )
    assert result.returncode == 2
    assert "ownership or mode" in result.stderr
    assert not record.exists()


def test_allocation_validates_resolved_slurm_configuration(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    mutable = Path(policy["state"]) / "slurm.conf"
    mutable.write_text("ClusterName=test-cluster\n", encoding="utf-8")
    slurm_conf = tmp_path / "broker" / "slurm.conf"
    slurm_conf.symlink_to(mutable)
    policy["slurm_conf"] = str(slurm_conf)
    _rewrite_policy(policy_path, policy)
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE],
        _allocation_environment(),
    )
    assert result.returncode == 2
    assert "configuration resolves across a mutable root" in result.stderr
    assert list(Path(policy["mpi"]["control_root"]).iterdir()) == []
    assert not record.exists()


def test_mpi_manager_receives_readonly_direct_control_child_and_markers(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    control_source = Path(policy["mpi"]["control_root"]) / "private"
    control_source.mkdir(mode=0o700)
    result = _run(
        tmp_path,
        policy_path,
        [
            "--mode",
            "payload",
            "--profile",
            "parallel",
            "--handle",
            HANDLE,
            "--control-source",
            str(control_source),
            "--procs",
            "32",
            "--mem-mb",
            "16384",
        ],
        dict(os.environ),
    )
    assert result.returncode == 0, result.stderr
    argv = json.loads(record.read_text(encoding="utf-8"))["argv"]
    assert argv[argv.index("httk.workflow._daemon_payload") + 5 :] == ["--procs", "32", "--mem-mb", "16384"]
    assert any(destination == "/run/httk-mpi" for _, destination in _pairs(argv, "--ro-bind-fd"))
    assert all(destination != "/run/httk-mpi" for _, destination in _pairs(argv, "--bind-fd"))
    environment = _set_environment(argv)
    assert environment["HTTK_DAEMON_MPI_HANDLE"] == HANDLE
    assert environment["HTTK_DAEMON_MPI_PROFILE"] == "parallel"
    assert "--unshare-net" in argv


def test_serial_payload_ignores_mpi_control_root_compute_locality(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    Path(policy["mpi"]["control_root"]).rmdir()
    result = _run(
        tmp_path,
        policy_path,
        ["--mode", "payload", "--profile", "serial", "--handle", HANDLE],
        dict(os.environ),
    )
    assert result.returncode == 0, result.stderr
    argv = json.loads(record.read_text(encoding="utf-8"))["argv"]
    assert all(destination != "/run/httk-mpi" for _, destination in _pairs(argv, "--ro-bind-fd"))


@pytest.mark.parametrize("capacity", [[], ["--mem-mb", "1024"], ["--procs", "0"], ["--procs", "32", "--mem-mb", "1x"]])
def test_mpi_manager_requires_canonical_capacity(tmp_path: Path, capacity: list[str]) -> None:
    policy_path, policy, record = _layout(tmp_path)
    control_source = Path(policy["mpi"]["control_root"]) / "private"
    control_source.mkdir(mode=0o700)
    arguments = ["--mode", "payload", "--profile", "parallel", "--handle", HANDLE]
    arguments += ["--control-source", str(control_source), *capacity]
    result = _run(tmp_path, policy_path, arguments, dict(os.environ))
    assert result.returncode == 2
    assert "--procs" in result.stderr or "--mem-mb" in result.stderr
    assert not record.exists()


@pytest.mark.parametrize("kind", ["indirect", "symlink", "permissive"])
def test_mpi_manager_refuses_unprotected_control_source(tmp_path: Path, kind: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    control_root = Path(policy["mpi"]["control_root"])
    if kind == "indirect":
        parent = control_root / "nested"
        parent.mkdir()
        source = parent / "private"
        source.mkdir(mode=0o700)
    elif kind == "symlink":
        target = control_root / "target"
        target.mkdir(mode=0o700)
        source = control_root / "private"
        source.symlink_to(target, target_is_directory=True)
    else:
        source = control_root / "private"
        source.mkdir(mode=0o755)
    result = _run(
        tmp_path,
        policy_path,
        [
            "--mode",
            "payload",
            "--profile",
            "parallel",
            "--handle",
            HANDLE,
            "--control-source",
            str(source),
        ],
        dict(os.environ),
    )
    assert result.returncode == 2
    assert not record.exists()


def test_rank_mounts_only_pinned_pmix_shared_shm_and_explicit_devices(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    pmix = Path(policy["mpi"]["pmix_roots"][0]) / "step-1234"
    pmix.mkdir(mode=0o700)
    leak = tmp_path / "inherited-secret"
    with leak.open("w", encoding="utf-8") as stream:
        os.set_inheritable(stream.fileno(), True)
        result = _run(
            tmp_path,
            policy_path,
            [
                "--mode",
                "mpi-rank",
                "--profile",
                "parallel",
                "--handle",
                HANDLE,
                "--request-id",
                REQUEST_ID,
            ],
            _rank_environment(pmix),
            close_fds=False,
        )
    assert result.returncode == 0, result.stderr
    observed = json.loads(record.read_text(encoding="utf-8"))
    argv = observed["argv"]
    assert "--unshare-net" not in argv
    assert "/proc" in argv and "/dev" in argv
    assert any(destination == "/workspace" for _, destination in _pairs(argv, "--bind-fd"))
    assert any(destination == str(pmix) for _, destination in _pairs(argv, "--bind-fd"))
    assert all(destination != str(pmix.parent) for _, destination in _pairs(argv, "--bind-fd"))
    assert any(destination == "/dev/null" for _, destination in _pairs(argv, "--bind-fd"))
    shm_fd = next(source for source, destination in _pairs(argv, "--bind-fd") if destination == "/dev/shm")
    shm_source = Path(observed["fds"][shm_fd])
    assert shm_source == Path(policy["mpi"]["shm_root"]) / f"httk-{policy['enrollment_id']}-{HANDLE}"
    assert stat.S_IMODE(shm_source.stat().st_mode) == 0o700
    environment = _set_environment(argv)
    assert environment["SLURM_PROCID"] == "3"
    assert environment["PMIX_SERVER_TMPDIR"] == str(pmix)
    assert environment["OMPI_MCA_btl"] == "self,vader,tcp"
    assert environment["SITE_SETTING"] == "protected"
    assert "UNRELATED_SECRET" not in environment
    assert str(leak) not in observed["fds"].values()
    separator = argv.index("--")
    assert argv[separator + 1 :] == [
        policy["python"],
        "-I",
        "-m",
        "httk.workflow._daemon_mpi_rank",
        "--profile",
        "parallel",
        "--handle",
        HANDLE,
        "--request-id",
        REQUEST_ID,
    ]


def test_rank_reuses_same_private_shared_memory_directory(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    arguments = [
        "--mode",
        "mpi-rank",
        "--profile",
        "parallel",
        "--handle",
        HANDLE,
        "--request-id",
        REQUEST_ID,
    ]
    assert _run(tmp_path, policy_path, arguments, _rank_environment()).returncode == 0
    first = Path(policy["mpi"]["shm_root"]) / f"httk-{policy['enrollment_id']}-{HANDLE}"
    inode = first.stat().st_ino
    record.unlink()
    assert _run(tmp_path, policy_path, arguments, _rank_environment(SLURM_PROCID="4")).returncode == 0
    assert first.stat().st_ino == inode


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SLURM_NTASKS", "7"),
        ("SLURM_PROCID", "8"),
        ("SLURM_NODEID", "2"),
        ("SLURM_LOCALID", "-1"),
        ("SLURM_JOB_ID", "bad"),
        ("SLURM_JOB_ID", "01"),
    ],
)
def test_rank_refuses_identity_outside_profile_before_shared_scratch(tmp_path: Path, name: str, value: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    environment = _rank_environment()
    environment[name] = value
    result = _run(
        tmp_path,
        policy_path,
        [
            "--mode",
            "mpi-rank",
            "--profile",
            "parallel",
            "--handle",
            HANDLE,
            "--request-id",
            REQUEST_ID,
        ],
        environment,
    )
    assert result.returncode == 2
    assert list(Path(policy["mpi"]["shm_root"]).iterdir()) == []
    assert not record.exists()


def test_rank_refuses_missing_exact_identity(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    environment = _rank_environment()
    environment.pop("SLURM_PROCID")
    result = _run(
        tmp_path,
        policy_path,
        [
            "--mode",
            "mpi-rank",
            "--profile",
            "parallel",
            "--handle",
            HANDLE,
            "--request-id",
            REQUEST_ID,
        ],
        environment,
    )
    assert result.returncode == 2
    assert list(Path(policy["mpi"]["shm_root"]).iterdir()) == []
    assert not record.exists()


@pytest.mark.parametrize("kind", ["outside", "root", "symlink", "writable"])
def test_rank_refuses_unprotected_pmix_directory(tmp_path: Path, kind: str) -> None:
    policy_path, policy, record = _layout(tmp_path)
    pmix_root = Path(policy["mpi"]["pmix_roots"][0])
    if kind == "outside":
        pmix = tmp_path / "outside-pmix"
        pmix.mkdir(mode=0o700)
    elif kind == "root":
        pmix = pmix_root
    elif kind == "symlink":
        target = pmix_root / "target"
        target.mkdir(mode=0o700)
        pmix = pmix_root / "step"
        pmix.symlink_to(target, target_is_directory=True)
    else:
        pmix = pmix_root / "step"
        pmix.mkdir()
        pmix.chmod(0o777)
    result = _run(
        tmp_path,
        policy_path,
        [
            "--mode",
            "mpi-rank",
            "--profile",
            "parallel",
            "--handle",
            HANDLE,
            "--request-id",
            REQUEST_ID,
        ],
        _rank_environment(pmix),
    )
    assert result.returncode == 2
    assert not record.exists()


@pytest.mark.parametrize(
    "arguments",
    [
        ["--mode", "allocation", "--profile", "serial", "--handle", HANDLE],
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID],
        ["--mode", "payload", "--profile", "parallel", "--handle", HANDLE],
        ["--mode", "payload", "--profile", "serial", "--handle", HANDLE, "--control-source", "/tmp/private"],
        ["--mode", "mpi-rank", "--profile", "serial", "--handle", HANDLE, "--request-id", REQUEST_ID],
        ["--mode", "mpi-rank", "--profile", "parallel", "--handle", HANDLE],
        ["--mode", "mpi-rank", "--profile", "parallel", "--handle", HANDLE, "--request-id", REQUEST_ID, "--procs", "8"],
        ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE, "--procs", "8"],
    ],
)
def test_mpi_mode_arguments_are_role_specific(tmp_path: Path, arguments: list[str]) -> None:
    policy_path, _, record = _layout(tmp_path)
    environment = _allocation_environment()
    environment.update(_rank_environment())
    result = _run(tmp_path, policy_path, arguments, environment)
    assert result.returncode == 2
    assert not record.exists()


def test_rank_rejects_resolved_mpi_root_alias_into_runtime(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    pmix_root = Path(policy["mpi"]["pmix_roots"][0])
    pmix_root.rmdir()
    pmix_root.symlink_to(Path(policy["readonly_paths"][0]), target_is_directory=True)
    result = _run(
        tmp_path,
        policy_path,
        [
            "--mode",
            "mpi-rank",
            "--profile",
            "parallel",
            "--handle",
            HANDLE,
            "--request-id",
            REQUEST_ID,
        ],
        _rank_environment(),
    )
    assert result.returncode == 2
    assert not record.exists()


def test_rank_rejects_non_device_approved_path(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    mpi = policy["mpi"]
    assert isinstance(mpi, dict)
    mpi["devices"] = ["/dev/does-not-exist"]
    _rewrite_policy(policy_path, policy)
    result = _run(
        tmp_path,
        policy_path,
        [
            "--mode",
            "mpi-rank",
            "--profile",
            "parallel",
            "--handle",
            HANDLE,
            "--request-id",
            REQUEST_ID,
        ],
        _rank_environment(),
    )
    assert result.returncode == 2
    assert not record.exists()


def _job_output(policy: dict[str, Any]) -> Path:
    jobs = Path(policy["snapshots"]) / "jobs"
    jobs.mkdir(parents=True, mode=0o700)
    output = jobs / "httk-1234.out"
    output.write_text("allocation output\n", encoding="utf-8")
    return Path(policy["workspace"]) / ".httk-workspace" / f"daemon-job-{HANDLE}.log"


def test_allocation_runs_bwrap_as_a_child_and_copies_the_job_log(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    log = _job_output(policy)
    arguments = ["--mode", "allocation", "--profile", "parallel", "--handle", HANDLE]
    result = _run(tmp_path, policy_path, arguments, _allocation_environment())
    assert result.returncode == 0, result.stderr
    assert json.loads(record.read_text(encoding="utf-8"))["ppid"] != os.getpid()
    assert log.read_text(encoding="utf-8") == "allocation output\n"


def test_mpi_manager_step_still_execs_and_copies_nothing(tmp_path: Path) -> None:
    policy_path, policy, record = _layout(tmp_path)
    log = _job_output(policy)
    control_source = Path(policy["mpi"]["control_root"]) / "private"
    control_source.mkdir(mode=0o700)
    arguments = ["--mode", "payload", "--profile", "parallel", "--handle", HANDLE]
    arguments += ["--control-source", str(control_source), "--procs", "32"]
    result = _run(tmp_path, policy_path, arguments, _allocation_environment())
    assert result.returncode == 0, result.stderr
    assert json.loads(record.read_text(encoding="utf-8"))["ppid"] == os.getpid()
    assert not log.exists()
