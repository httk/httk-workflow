"""Exercise the daemon broker boundary in a real Bubblewrap namespace."""

import base64
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from httk.workflow._death import ProcessIdentity, process_gone, process_identity

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


@pytest.mark.parametrize("termination", ["normal", "startup-error", "term", "kill"])
def test_real_broker_confinement(tmp_path: Path, termination: str) -> None:
    bwrap_text = shutil.which("bwrap")
    if bwrap_text is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    bwrap = Path(bwrap_text).resolve()

    workspace = tmp_path / "site/workspace"
    project_metadata = workspace.parent / "httk_project"
    project_metadata.mkdir(parents=True)
    (project_metadata / "project.json").write_text('{"format":"httk-project"}', encoding="utf-8")
    operator_config = tmp_path / "operator-config"
    (operator_config / "keys").mkdir(parents=True)
    (operator_config / "keys" / "identity.marker").write_text("present", encoding="utf-8")
    exchange = workspace / "exchange"
    daemon = workspace / ".httk-workspace" / "daemon"
    daemon_broker = daemon / "broker"
    broker = tmp_path / "broker"
    for root in (exchange / "requests", exchange / "responses"):
        root.mkdir(parents=True)
    daemon.mkdir(parents=True)
    (workspace / "jobs").mkdir()
    (workspace / "workflows").mkdir()
    for name in ("owners", "requests", "tmp", "quarantine", "exchange-jobs"):
        (workspace / ".httk-workspace" / name).mkdir(parents=True)
    for root in (daemon / "snapshots", daemon / "jobs", daemon_broker, broker):
        root.mkdir()
    (daemon / "response.seed").write_bytes(b"test-seed")

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
        "import json, os, subprocess, sys, time\n"
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
        "if 'httk.workflow._daemon_jobs' in sys.argv:\n"
        "    # The bootstrap's host-side recovery of dead daemon owners, before it enters the sandbox.\n"
        f"    open({str(broker / 'recovery.json')!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
        "    raise SystemExit(0)\n"
        "report = {}\n"
        "report['identity'] = json.loads(sys.argv[sys.argv.index('--execution-scope') + 1])\n"
        f"open({str(daemon_broker / 'scope.json')!r}, 'w').write(json.dumps(report['identity']))\n"
        f"release = {str(daemon_broker / 'release')!r}\n"
        "if '--check' in sys.argv:\n"
        "    while not os.path.exists(release): time.sleep(0.01)\n"
        "    raise SystemExit(3)\n"
        "while not os.path.exists(release): time.sleep(0.01)\n"
        "report['outside_read'] = attempt(lambda: open(outside, encoding='utf-8').read())\n"
        "report['outside_write'] = attempt(lambda: open(outside, 'w', encoding='utf-8').write('changed'))\n"
        "report['exchange_write'] = attempt(\n"
        "    lambda: open('/tmp/daemon-exchange/created', 'w', encoding='utf-8').write('inside'))\n"
        f"report['workspace_write'] = attempt(lambda: open({str(workspace / 'created')!r}, 'w').write('x'))\n"
        f"report['workspace_exchange_host_write'] = attempt(lambda: open({str(exchange / 'host')!r}, 'w').write('x'))\n"
        "report['state_write'] = attempt(lambda: open('/tmp/control/created', 'w', encoding='utf-8').write('state'))\n"
        f"report['broker_real_write'] = attempt(lambda: open({str(daemon_broker / 'real-canary')!r}, 'w').write('broker'))\n"
        f"report['owners_write'] = attempt(lambda: open({str(workspace / '.httk-workspace/owners/owner-canary')!r}, 'w').write('owner'))\n"
        f"report['jobs_write'] = attempt(lambda: open({str(workspace / 'jobs/job-canary')!r}, 'w').write('job'))\n"
        f"report['workspace_root_write'] = attempt(lambda: open({str(workspace / 'root-canary')!r}, 'w').write('root'))\n"
        f"report['workflow_config_write'] = attempt(lambda: open({str(workspace / 'workflows/config.toml')!r}, 'w').write('x'))\n"
        "report['daemon_read'] = attempt(lambda: open('/tmp/daemon/response.seed', encoding='utf-8').read())\n"
        "report['daemon_write'] = attempt(lambda: open('/tmp/daemon/x', 'w', encoding='utf-8').write('x'))\n"
        f"report['project_metadata_read'] = open({str(project_metadata / 'project.json')!r}).read() == '{{\"format\":\"httk-project\"}}'\n"
        f"report['project_metadata_write'] = attempt(lambda: open({str(project_metadata / 'generated')!r}, 'w').write('x'))\n"
        f"report['operator_key_read'] = open({str(operator_config / 'keys/identity.marker')!r}).read() == 'present'\n"
        f"report['operator_config_write'] = attempt(lambda: open({str(operator_config / 'generated')!r}, 'w').write('x'))\n"
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
        "format_version": 6,
        "workspace": str(workspace),
        "workspace_id": str(uuid.uuid4()),
        "enrollment_id": "2" * 32,
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
    here = process_identity()
    with sentinel.open("r", encoding="utf-8") as inherited:
        os.set_inheritable(inherited.fileno(), True)
        bootstrap_args = [
            sys.executable,
            "-I",
            "-S",
            str(BOOTSTRAP),
            "--policy",
            str(policy_path),
            "--workspace",
            str(workspace),
        ]
        if termination == "startup-error":
            bootstrap_args.append("--check")
        process = subprocess.Popen(
            bootstrap_args,
            close_fds=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "HTTK_CONFIG_HOME": str(operator_config)},
        )
        deadline = time.monotonic() + 20
        scope_path = daemon_broker / "scope.json"
        while not scope_path.exists() and time.monotonic() < deadline:
            if process.poll() is not None:
                break
            time.sleep(0.01)
        scope_started = scope_path.exists()
        if not scope_started:
            process.kill()
            stdout, stderr = process.communicate(timeout=10)
            result = subprocess.CompletedProcess(process.args, process.returncode or 2, stdout, stderr)
        else:
            identity_data = json.loads(scope_path.read_text(encoding="utf-8"))
            identity = ProcessIdentity(**identity_data)
            # The bootstrap execs Bubblewrap, whose monitor keeps its pid for the sandbox's lifetime.
            assert identity.pid == process.pid
            host_stat = Path(f"/proc/{identity.pid}/stat").read_text(encoding="ascii")
            host_ticks = int(host_stat[host_stat.rindex(")") + 2 :].split()[19])
            assert identity.start_ticks == host_ticks
            assert process_gone(identity, here=here) is False
            if termination == "term":
                process.send_signal(signal.SIGTERM)
            elif termination == "kill":
                process.kill()
            else:
                (daemon_broker / "release").touch()
            stdout, stderr = process.communicate(timeout=20)
            result = subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)
            assert process_gone(identity, here=process_identity()) is True
    if not scope_started:
        _unsupported_namespace_failure(result)
    recovery = broker / "recovery.json"
    if termination == "startup-error":
        assert result.returncode == 3
        assert not recovery.exists()
        return
    assert json.loads(recovery.read_text(encoding="utf-8"))[-4:] == [
        "--workspace",
        str(workspace),
        "--workspace-id",
        policy["workspace_id"],
    ]
    if termination in ("term", "kill"):
        signal_number = signal.SIGTERM if termination == "term" else signal.SIGKILL
        assert result.returncode in (-signal_number, 128 + signal_number)
        return
    if result.returncode != 0:
        _unsupported_namespace_failure(result)
    report = json.loads(result.stdout)
    identity = report["identity"]
    assert identity["hostname"] == os.uname().nodename
    assert identity["boot_id"] == Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    assert report["project_metadata_read"] and report["project_metadata_write"] is False
    assert report["operator_key_read"] and report["operator_config_write"] is False
    # The broker runs trusted code with a read-only host view: it reads the host but writes only its roots.
    assert report["outside_read"] is True
    assert report["outside_write"] is False
    # Both the real workspace overlays and the service aliases expose their permitted writes.
    assert report["exchange_write"] is True and report["state_write"] is True
    assert report["owners_write"] is True and report["jobs_write"] is True
    assert report["broker_real_write"] is True
    assert report["workspace_root_write"] is False and report["workflow_config_write"] is False
    assert report["daemon_read"] is True and report["daemon_write"] is False
    assert report["workspace_write"] is False and report["workspace_exchange_host_write"] is True
    assert report["policy"] == "2" * 32
    assert report["sentinel_fd_visible"] is False
    assert report["nested_userns"] is False
    for name, host_namespace in host_namespaces.items():
        # Slurm clients need the host network; every other namespace is private.
        assert (report["namespaces"][name] == host_namespace) == (name == "net")
    assert (exchange / "created").read_text(encoding="utf-8") == "inside"
    assert not (workspace / "created").exists() and (exchange / "host").read_text() == "x"
    assert (daemon_broker / "created").read_text(encoding="utf-8") == "state"
    assert (workspace / ".httk-workspace" / "owners" / "owner-canary").read_text() == "owner"
    assert (workspace / "jobs" / "job-canary").read_text() == "job"
    assert not (workspace / "root-canary").exists()
    assert not (workspace / "workflows" / "config.toml").exists()
    assert not (project_metadata / "generated").exists()
    assert not (operator_config / "generated").exists()
    assert sentinel.read_text(encoding="utf-8") == "host-secret"
