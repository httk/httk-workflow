"""Exercise the daemon boundary in a real Bubblewrap namespace."""

import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

BOOTSTRAP = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "_daemon_bootstrap.py"
AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


def _unsupported_namespace_failure(result: subprocess.CompletedProcess[str]) -> None:
    detail = (result.stderr or result.stdout).strip()
    lowered = detail.lower()
    expected = any(
        phrase in lowered
        for phrase in (
            "operation not permitted",
            "permission denied",
            "no permissions to create",
            "user namespace",
        )
    )
    if not expected:
        pytest.fail(f"Bubblewrap sandbox failed unexpectedly: {detail}")
    if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
        pytest.fail(f"required Bubblewrap sandbox is unavailable: {detail}")
    pytest.skip(f"Bubblewrap user namespaces are unavailable: {detail}")


def _write_executable(path: Path, body: str = "#!/bin/sh\nexit 0\n") -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def test_real_payload_confinement(tmp_path: Path) -> None:
    bwrap_text = shutil.which("bwrap")
    if bwrap_text is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    bwrap = Path(bwrap_text).resolve()

    workspace = tmp_path / "site/workspace"
    exchange = tmp_path / "site/exchange"
    state = tmp_path / "state"
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    for root in (workspace / ".httk-workspace/exchange", exchange / "requests", exchange / "responses"):
        root.mkdir(parents=True)
    for root in (state, runtime, broker):
        root.mkdir()

    # Container root-owned binaries can appear with an unmapped UID. Use an
    # owned copy without weakening the bootstrap's executable ownership check.
    trusted_bwrap = broker / "bwrap"
    shutil.copyfile(bwrap, trusted_bwrap)
    trusted_bwrap.chmod(0o755)
    bwrap = trusted_bwrap

    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("host-secret", encoding="utf-8")
    escape = workspace / "escape"
    escape.symlink_to(sentinel)

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        host_namespaces = {name: os.readlink(f"/proc/self/ns/{name}") for name in ("user", "pid", "ipc", "uts", "net")}
        payload = runtime / "python"
        _write_executable(
            payload,
            "#!/usr/bin/python3\n"
            "import json, os, socket, subprocess\n"
            "def attempt(action):\n"
            "    try:\n"
            "        action()\n"
            "    except Exception:\n"
            "        return False\n"
            "    return True\n"
            "def descriptor_visible(target):\n"
            "    for name in os.listdir('/proc/self/fd'):\n"
            "        try:\n"
            "            if os.readlink('/proc/self/fd/' + name) == target:\n"
            "                return True\n"
            "        except OSError:\n"
            "            pass\n"
            "    return False\n"
            f"outside = {str(sentinel)!r}\n"
            f"port = {port!r}\n"
            "report = {}\n"
            "report['outside_read'] = attempt(lambda: open(outside, encoding='utf-8').read())\n"
            "report['outside_write'] = attempt(lambda: open(outside, 'w', encoding='utf-8').write('changed'))\n"
            "report['symlink_read'] = attempt(lambda: open('/workspace/escape', encoding='utf-8').read())\n"
            "report['workspace_write'] = attempt(lambda: open('/workspace/created', 'w', encoding='utf-8').write('inside'))\n"
            "report['sentinel_fd_visible'] = descriptor_visible(outside)\n"
            "report['nested_userns'] = subprocess.run(\n"
            "    ['/usr/bin/unshare', '--user', '--map-root-user', '/usr/bin/true'],\n"
            "    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,\n"
            ").returncode == 0\n"
            "report['network_connect'] = attempt(lambda: socket.create_connection(('127.0.0.1', port), timeout=0.2))\n"
            "report['namespaces'] = {name: os.readlink('/proc/self/ns/' + name) for name in ('user','pid','ipc','uts','net')}\n"
            "print(json.dumps(report))\n",
        )
        for command in ("sbatch", "squeue", "scancel"):
            _write_executable(broker / command)

        readonly_paths = [Path("/usr"), runtime]
        for candidate in (Path("/lib"), Path("/lib64")):
            if candidate.exists():
                readonly_paths.append(candidate)
        policy = {
            "format": "httk-workspace-daemon-policy",
            "format_version": 2,
            "workspace": str(workspace),
            "workspace_id": str(uuid.uuid4()),
            "enrollment_id": "2" * 32,
            "exchange": str(exchange),
            "state": str(state),
            "snapshots": str(tmp_path / "snapshots"),
            "bwrap": str(bwrap),
            "python": str(payload),
            "sbatch": str(broker / "sbatch"),
            "squeue": str(broker / "squeue"),
            "scancel": str(broker / "scancel"),
            "cluster": "sandbox-test",
            "readonly_paths": [str(path) for path in readonly_paths],
            "broker_paths": [str(broker)],
            "authorized_keys": [AUTHORIZED_KEY],
            "profiles": {"small": {"cpus": 1, "memory_mb": 128, "time_minutes": 1}},
        }
        policy_path = tmp_path / "policy.json"
        policy_path.write_text(json.dumps(policy), encoding="utf-8")
        policy_path.chmod(0o600)
        with sentinel.open("r", encoding="utf-8") as inherited:
            os.set_inheritable(inherited.fileno(), True)
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    str(BOOTSTRAP),
                    "--policy",
                    str(policy_path),
                    "--mode",
                    "payload",
                    "--profile",
                    "small",
                    "--handle",
                    "a" * 32,
                ],
                close_fds=False,
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
    if result.returncode != 0:
        _unsupported_namespace_failure(result)
    report = json.loads(result.stdout)
    assert report["outside_read"] is False
    assert report["outside_write"] is False
    assert report["symlink_read"] is False
    assert report["workspace_write"] is True
    assert report["sentinel_fd_visible"] is False
    assert report["nested_userns"] is False
    assert report["network_connect"] is False
    for name, host_namespace in host_namespaces.items():
        assert report["namespaces"][name] != host_namespace
    assert (workspace / "created").read_text(encoding="utf-8") == "inside"
    assert sentinel.read_text(encoding="utf-8") == "host-secret"


def test_real_mpi_ranks_share_only_allocation_shm(tmp_path: Path) -> None:
    """Use real rank namespaces without claiming an installed MPI transport test."""

    bwrap_text = shutil.which("bwrap")
    if bwrap_text is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    roots = {name: tmp_path / name for name in ("site/workspace", "runtime", "broker", "control", "shm")}
    for root in roots.values():
        root.mkdir(parents=True)
    roots["workspace"] = roots["site/workspace"]
    (roots["workspace"] / ".httk-workspace/exchange").mkdir(parents=True)
    (tmp_path / "site/exchange").mkdir()
    trusted_bwrap = roots["broker"] / "bwrap"
    shutil.copyfile(Path(bwrap_text).resolve(), trusted_bwrap)
    trusted_bwrap.chmod(0o755)
    outside = tmp_path / "host-canary"
    outside.write_text("untouched")
    host_shm = roots["shm"] / "unrelated"
    host_shm.write_text("not visible")
    (roots["workspace"] / "escape").symlink_to(outside)
    payload = roots["runtime"] / "python"
    _write_executable(
        payload,
        "#!/usr/bin/python3\n"
        "import json, os, pathlib, time\n"
        "rank = int(os.environ['SLURM_PROCID'])\n"
        "path = pathlib.Path('/dev/shm/shared')\n"
        "if rank == 0: path.write_text('rank zero')\n"
        "deadline = time.monotonic() + 5\n"
        "while not path.exists() and time.monotonic() < deadline: time.sleep(0.01)\n"
        "report = {'shared': path.read_text(), 'unrelated': pathlib.Path('/dev/shm/unrelated').exists(),\n"
        "          'symlink_target': pathlib.Path('/workspace/escape').exists(),\n"
        "          'namespaces': {n: os.readlink('/proc/self/ns/' + n) for n in ('pid','user','ipc','net')}}\n"
        "print(json.dumps(report))\n",
    )
    for command_name in ("sbatch", "squeue", "scancel", "srun"):
        _write_executable(roots["broker"] / command_name)
    readonly = ["/usr", str(roots["runtime"])]
    readonly += [str(path) for path in (Path("/lib"), Path("/lib64")) if path.exists()]
    policy = {
        "format": "httk-workspace-daemon-policy",
        "format_version": 2,
        "workspace": str(roots["workspace"]),
        "workspace_id": str(uuid.uuid4()),
        "enrollment_id": "e" * 32,
        "exchange": str(tmp_path / "site/exchange"),
        "state": str(tmp_path / "state"),
        "snapshots": str(tmp_path / "snapshots"),
        "bwrap": str(trusted_bwrap),
        "python": str(payload),
        "sbatch": str(roots["broker"] / "sbatch"),
        "squeue": str(roots["broker"] / "squeue"),
        "scancel": str(roots["broker"] / "scancel"),
        "cluster": "sandbox-test",
        "readonly_paths": readonly,
        "broker_paths": [str(roots["broker"])],
        "authorized_keys": [AUTHORIZED_KEY],
        "profiles": {"mpi": {"cpus": 1, "memory_mb": 128, "time_minutes": 1, "mpi": {"nodes": 1, "ranks": 2}}},
        "mpi": {
            "srun": str(roots["broker"] / "srun"),
            "control_root": str(roots["control"]),
            "shm_root": str(roots["shm"]),
        },
    }
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy))
    policy_path.chmod(0o600)
    command = [
        sys.executable,
        "-I",
        "-S",
        str(BOOTSTRAP),
        "--policy",
        str(policy_path),
        "--mode",
        "mpi-rank",
        "--profile",
        "mpi",
        "--handle",
        "a" * 32,
        "--request-id",
        "b" * 32,
    ]
    processes: list[subprocess.Popen[str]] = []
    results: list[subprocess.CompletedProcess[str]] = []
    try:
        for rank in range(2):
            environment = {
                "SLURM_PROCID": str(rank),
                "SLURM_LOCALID": str(rank),
                "SLURM_NODEID": "0",
                "SLURM_NTASKS": "2",
                "SLURM_JOB_ID": "123",
                "SLURMD_NODENAME": "node",
            }
            processes.append(
                subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            )
        for process in processes:
            stdout, stderr = process.communicate(timeout=20)
            results.append(subprocess.CompletedProcess(command, process.returncode, stdout, stderr))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
    for result in results:
        if result.returncode != 0:
            _unsupported_namespace_failure(result)
    reports = [json.loads(result.stdout) for result in results]
    for report in reports:
        assert report["shared"] == "rank zero"
        assert report["unrelated"] is False
        assert report["symlink_target"] is False
        assert report["namespaces"]["net"] == os.readlink("/proc/self/ns/net")
        for namespace in ("pid", "user", "ipc"):
            assert report["namespaces"][namespace] != os.readlink("/proc/self/ns/" + namespace)
    for namespace in ("pid", "user", "ipc"):
        assert reports[0]["namespaces"][namespace] != reports[1]["namespaces"][namespace]
    assert outside.read_text() == "untouched"
    assert host_shm.read_text() == "not visible"
