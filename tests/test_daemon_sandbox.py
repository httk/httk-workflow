"""Exercise the daemon boundary in a real Bubblewrap namespace."""

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

    workspace = tmp_path / "workspace"
    requests = tmp_path / "requests"
    responses = tmp_path / "responses"
    state = tmp_path / "state"
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    for root in (workspace, requests, responses, state, runtime, broker):
        root.mkdir()

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
            "format_version": 1,
            "workspace": str(workspace),
            "workspace_id": str(uuid.uuid4()),
            "enrollment_id": "2" * 32,
            "requests": str(requests),
            "responses": str(responses),
            "state": str(state),
            "bwrap": str(bwrap),
            "python": str(payload),
            "sbatch": str(broker / "sbatch"),
            "squeue": str(broker / "squeue"),
            "scancel": str(broker / "scancel"),
            "cluster": "sandbox-test",
            "readonly_paths": [str(path) for path in readonly_paths],
            "broker_paths": [str(broker)],
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
