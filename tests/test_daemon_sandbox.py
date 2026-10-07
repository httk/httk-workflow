"""Exercise the daemon broker boundary in a real Bubblewrap namespace."""

import base64
import json
import os
import shutil
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


def test_real_broker_confinement(tmp_path: Path) -> None:
    bwrap_text = shutil.which("bwrap")
    if bwrap_text is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    bwrap = Path(bwrap_text).resolve()

    workspace = tmp_path / "site/workspace"
    exchange = workspace / "exchange"
    state = tmp_path / "state"
    broker = tmp_path / "broker"
    for root in (exchange / "requests", exchange / "responses"):
        root.mkdir(parents=True)
    for root in (state, broker):
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

    host_namespaces = {name: os.readlink(f"/proc/self/ns/{name}") for name in ("user", "pid", "ipc", "uts", "net")}
    # The broker command is replaced by a reporter: it runs where the broker would, with the same mounts.
    service = broker / "python"
    _write_executable(
        service,
        "#!/usr/bin/python3\n"
        "import json, os, subprocess\n"
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
        "report = {}\n"
        "report['outside_read'] = attempt(lambda: open(outside, encoding='utf-8').read())\n"
        "report['outside_write'] = attempt(lambda: open(outside, 'w', encoding='utf-8').write('changed'))\n"
        "report['exchange_write'] = attempt(\n"
        "    lambda: open('/tmp/daemon-exchange/created', 'w', encoding='utf-8').write('inside'))\n"
        f"report['workspace_write'] = attempt(lambda: open({str(workspace / 'created')!r}, 'w').write('x'))\n"
        f"report['workspace_exchange_host_write'] = attempt(lambda: open({str(exchange / 'host')!r}, 'w').write('x'))\n"
        "report['state_write'] = attempt(lambda: open('/tmp/control/created', 'w', encoding='utf-8').write('state'))\n"
        "report['policy'] = json.load(open('/tmp/daemon-policy.json', encoding='utf-8'))['enrollment_id']\n"
        "report['sentinel_fd_visible'] = descriptor_visible(outside)\n"
        "report['nested_userns'] = subprocess.run(\n"
        "    ['/usr/bin/unshare', '--user', '--map-root-user', '/usr/bin/true'],\n"
        "    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,\n"
        ").returncode == 0\n"
        "report['namespaces'] = {name: os.readlink('/proc/self/ns/' + name) for name in ('user','pid','ipc','uts','net')}\n"
        "print(json.dumps(report))\n",
    )
    for command in ("sbatch", "squeue", "scancel"):
        _write_executable(broker / command)
    policy = {
        "format": "httk-workspace-daemon-policy",
        "format_version": 5,
        "workspace": str(workspace),
        "workspace_id": str(uuid.uuid4()),
        "enrollment_id": "2" * 32,
        "state": str(state),
        "snapshots": str(tmp_path / "snapshots"),
        "bwrap": str(bwrap),
        "python": str(service),
        "sbatch": str(broker / "sbatch"),
        "squeue": str(broker / "squeue"),
        "scancel": str(broker / "scancel"),
        "cluster": "sandbox-test",
        "authorized_keys": [AUTHORIZED_KEY],
        "launchers": {"small": {"settings": {"manager.confine": "bwrap"}, "digest": "a" * 64}},
    }
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    policy_path.chmod(0o600)
    with sentinel.open("r", encoding="utf-8") as inherited:
        os.set_inheritable(inherited.fileno(), True)
        result = subprocess.run(
            [sys.executable, "-I", "-S", str(BOOTSTRAP), "--policy", str(policy_path), "--workspace", str(workspace)],
            close_fds=False,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    if result.returncode != 0:
        _unsupported_namespace_failure(result)
    report = json.loads(result.stdout)
    # The broker runs trusted code with a read-only host view: it reads the host but writes only its roots.
    assert report["outside_read"] is True
    assert report["outside_write"] is False
    # Writable: the bound exchange and the state. Not writable: the rest of the workspace, and the exchange
    # through the host's read-only view (the bind is the only way in).
    assert report["exchange_write"] is True and report["state_write"] is True
    assert report["workspace_write"] is False and report["workspace_exchange_host_write"] is False
    assert report["policy"] == "2" * 32
    assert report["sentinel_fd_visible"] is False
    assert report["nested_userns"] is False
    for name, host_namespace in host_namespaces.items():
        # Slurm clients need the host network; every other namespace is private.
        assert (report["namespaces"][name] == host_namespace) == (name == "net")
    assert (exchange / "created").read_text(encoding="utf-8") == "inside"
    assert not (workspace / "created").exists() and not (exchange / "host").exists()
    assert (state / "created").read_text(encoding="utf-8") == "state"
    assert sentinel.read_text(encoding="utf-8") == "host-secret"
