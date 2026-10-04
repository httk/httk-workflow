"""Compose MPI client, launcher and rank with a non-confining scheduler stand-in."""

import argparse
import base64
import json
import sys
import threading
import time
from pathlib import Path

import pytest

from httk.workflow import _daemon_mpi_client as client
from httk.workflow import _daemon_mpi_service as service
from httk.workflow._daemon_policy import load_policy

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


def test_client_service_rank_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / ".httk-workspace").mkdir()
    (workspace / "input.txt").write_text("application input")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    finished = tmp_path / "manager-stop"
    manager_ready = tmp_path / "manager-ready"
    launched = tmp_path / "launches.jsonl"
    srun = runtime / "srun"
    policy_path = tmp_path / "policy.json"
    handle = "a" * 32
    rank_script = (
        "from pathlib import Path; import sys; "
        "from httk.workflow import _daemon_mpi_rank as rank; "
        f"rank._POLICY_PATH=Path({str(policy_path)!r}); rank._WORKSPACE=Path({str(workspace)!r}); "
        f"raise SystemExit(rank.main(['--profile','mpi','--handle',{handle!r},'--request-id',sys.argv[1]]))"
    )
    srun.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys, time\n"
        f"with open({str(launched)!r}, 'a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if '--mpi=none' in sys.argv:\n"
        f"    open({str(manager_ready)!r}, 'w').close()\n"
        f"    while not os.path.exists({str(finished)!r}): time.sleep(0.01)\n"
        "else:\n"
        "    request_id = sys.argv[sys.argv.index('--request-id') + 1]\n"
        f"    os.execv({sys.executable!r}, [{sys.executable!r}, '-I', '-c', {rank_script!r}, request_id])\n"
    )
    srun.chmod(0o700)
    policy_path.write_text(
        json.dumps(
            {
                "format": "httk-workspace-daemon-policy",
                "format_version": 1,
                "workspace": str(workspace),
                "workspace_id": "12345678-1234-4234-8234-123456789abc",
                "enrollment_id": "e" * 32,
                "requests": str(tmp_path / "requests"),
                "responses": str(tmp_path / "responses"),
                "state": str(tmp_path / "state"),
                "bwrap": "/usr/bin/bwrap",
                "python": sys.executable,
                "sbatch": str(runtime / "sbatch"),
                "squeue": str(runtime / "squeue"),
                "scancel": str(runtime / "scancel"),
                "cluster": "site",
                "readonly_paths": ["/usr", sys.prefix, str(runtime)],
                "broker_paths": [],
                "authorized_keys": [AUTHORIZED_KEY],
                "profiles": {"mpi": {"cpus": 1, "memory_mb": 128, "time_minutes": 1, "mpi": {"nodes": 1, "ranks": 2}}},
                "mpi": {"srun": str(srun), "control_root": str(tmp_path / "control"), "termination_grace": 0.1},
            }
        )
    )
    policy_path.chmod(0o600)
    policy = load_policy(policy_path)
    socket_path = tmp_path / "control.sock"
    monkeypatch.setattr(service, "_SOCKET", socket_path)
    monkeypatch.setattr(client, "_CONTROL_SOCKET", socket_path)
    monkeypatch.setattr(client, "_POLICY_PATH", policy_path)
    monkeypatch.setattr(client, "_WORKSPACE", workspace)
    monkeypatch.setenv("HTTK_DAEMON_MPI_HANDLE", handle)
    monkeypatch.setenv("HTTK_DAEMON_MPI_PROFILE", "mpi")
    monkeypatch.setenv("APPLICATION_SETTING", "inside app")
    monkeypatch.chdir(workspace)
    arguments = argparse.Namespace(
        policy_source=str(policy_path),
        handle=handle,
        job_id="123",
        node="node",
        control_source=str(tmp_path / "control" / "private"),
        procs="2",
        mem_mb=None,
    )
    launcher = service._Service(policy, policy.profile("mpi"), arguments)
    outcomes: list[int | BaseException] = []

    def supervise() -> None:
        try:
            outcomes.append(launcher.run())
        except BaseException as exc:
            outcomes.append(exc)

    thread = threading.Thread(target=supervise)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not manager_ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert manager_ready.exists()
        assert socket_path.exists()
        application = [
            sys.executable,
            "-c",
            "import os, pathlib, sys; print(pathlib.Path('input.txt').read_text()); print(os.environ['APPLICATION_SETTING']); print(sys.argv[1], file=sys.stderr); sys.exit(7)",
            "$(touch NOT_EXECUTED)",
        ]
        assert client.run(application) == 7
        output = capfd.readouterr()
        assert "application input\ninside app\n" in output.out
        assert "$(touch NOT_EXECUTED)" in output.err
        assert not (workspace / "NOT_EXECUTED").exists()
        assert list((workspace / ".httk-workspace" / "mpi" / handle).glob("*.json")) == []
        commands = [json.loads(line) for line in launched.read_text().splitlines()]
        assert len(commands) == 2
        assert len([command for command in commands if "--mpi=none" in command]) == 1
        application_commands = [command for command in commands if "--mpi=pmix" in command]
        assert len(application_commands) == 1
        assert application[-1] not in application_commands[0] and "APPLICATION_SETTING" not in str(
            application_commands[0]
        )
        finished.write_text("done")
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert outcomes == [0]
    finally:
        launcher.stop = True
        finished.touch()
        thread.join(timeout=5)
        assert not thread.is_alive()
