"""Compose native transfer, the real adapter subprocess and a restarted broker.

Local directories and scheduler executables stand in for SSHFS and Slurm. This
checks protocol composition; it does not execute a confined compute payload.
"""

import json
import os
import sys
import threading
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_policy import Policy, Profile
from httk.workflow._daemon_service import Broker
from httk.workflow._daemon_slurm import SlurmGateway
from httk.workflow._daemon_state import Ledger
from httk.workflow.adapters import add_remote
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload
from httk.workflow.workflow_cli import command


def _executable(path: Path, body: str) -> None:
    path.write_text(f"#!{sys.executable}\nimport json, os, sys\n" + body)
    path.chmod(0o700)


@contextmanager
def _broker(policy: Policy, *, initialize: bool) -> Iterator[None]:
    stop = threading.Event()
    ready = threading.Event()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            with ExitStack() as stack:
                ledger = stack.enter_context(
                    Ledger(policy.state, policy.workspace_id, policy.enrollment_id, initialize=initialize)
                )
                ledger.recover()
                requests = stack.enter_context(MailboxDirectory(policy.requests))
                responses = stack.enter_context(MailboxDirectory(policy.responses))
                gateway = SlurmGateway(policy, policy.state / "policy.json")
                gateway.check()
                ready.set()
                Broker(policy, gateway, ledger, requests, responses).run(stop)
        except BaseException as exc:
            errors.append(exc)
            ready.set()

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert ready.wait(10), "broker did not start"
        assert not errors, errors
        yield
    finally:
        stop.set()
        worker.join(10)
        assert not worker.is_alive(), "broker did not stop"
        assert not errors, errors


@pytest.mark.timing
def test_mounted_transfer_typed_control_timeout_and_restart_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    source = Workspace.initialize(tmp_path / "source")
    mounted = Workspace.initialize(tmp_path / "mounted-data")
    payload = tmp_path / "payload"
    (payload / "files").mkdir(parents=True)
    canary = tmp_path / "runner-must-not-execute"
    runner = payload / "files" / "run"
    runner.write_text(f"#!{sys.executable}\nfrom pathlib import Path\nPath({str(canary)!r}).touch()\n")
    runner.chmod(0o700)
    prepare_job_payload(payload, JobSpec(name="job", workflow="tests.daemon", runner_path="files/run"))
    marker = source.submit(payload, "jobs")
    context = CLIContext("httk", tmp_path)
    for name in ("requests", "responses", "state", "scheduler"):
        (tmp_path / name).mkdir()
    scheduler = tmp_path / "scheduler"
    submitted = tmp_path / "submitted.jsonl"
    cancelled = tmp_path / "cancelled.json"
    version = "if sys.argv[1:] == ['--version']:\n    print('slurm 26.05.4')\n    sys.exit(0)\n"
    _executable(
        scheduler / "sbatch",
        version
        + "script = sys.stdin.read()\n"
        + f"with open({str(submitted)!r}, 'a') as record: record.write(json.dumps([sys.argv[1:], script]) + '\\n')\n"
        + "print('123;cluster')\n",
    )
    _executable(
        scheduler / "squeue",
        version
        + f"calls = [json.loads(line) for line in open({str(submitted)!r})]\n"
        + "name = next(arg.split('=', 1)[1] for arg in calls[-1][0] if arg.startswith('--job-name='))\n"
        + "print(f'123|{name}|{os.getuid()}|RUNNING')\n",
    )
    _executable(
        scheduler / "scancel",
        version + f"open({str(cancelled)!r}, 'w').write(json.dumps(sys.argv[1:]))\n",
    )
    policy = Policy(
        workspace=mounted.root,
        workspace_id=mounted.workspace_id,
        enrollment_id="b" * 32,
        requests=tmp_path / "requests",
        responses=tmp_path / "responses",
        state=tmp_path / "state",
        bwrap=Path("/usr/bin/bwrap"),
        python=Path(sys.executable),
        sbatch=scheduler / "sbatch",
        squeue=scheduler / "squeue",
        scancel=scheduler / "scancel",
        cluster="cluster",
        readonly_paths=(Path("/usr"), Path(sys.prefix)),
        broker_paths=(scheduler,),
        profiles=(Profile("cpu", 2, 512, 5),),
        poll_seconds=0.05,
    )
    add_remote("mounted", template="mount-daemon", global_scope=True)
    settings = {
        "mount_root": str(mounted.root),
        "daemon_requests": str(policy.requests),
        "daemon_responses": str(policy.responses),
        "daemon_workspace_id": policy.workspace_id,
        "daemon_enrollment_id": policy.enrollment_id,
    }
    configure = ["remote", "configure", "mounted"]
    for key, value in settings.items():
        configure.extend(["--set", f"{key}={value}"])
    assert command(configure, context) == 0
    assert not list(policy.requests.iterdir())

    # Generic remote transfers refuse before detaching the source job.
    assert command(["job", "transfer", str(source.root), "mounted:workspace", "--job", marker.job_id], context) != 0
    assert source.find_marker_by_id(marker.job_id) is not None
    assert command(["job", "transfer", str(source.root), str(mounted.root), "--job", marker.job_id], context) == 0
    assert source.find_marker_by_id(marker.job_id) is None
    assert mounted.find_marker_by_id(marker.job_id) is not None
    assert not canary.exists()
    capsys.readouterr()

    def control(verb: str, *args: str) -> dict[str, object]:
        assert command(["remote", "daemon", verb, "mounted", *args], context) == 0
        captured = capsys.readouterr()
        response = json.loads(captured.out)
        assert response["request_id"] in captured.err
        return response

    start = ["--profile", "cpu", "--request-id", "1" * 32]
    # The stopped destination leaves a live request; the same identity completes later.
    assert command(["remote", "daemon", "start", "mounted", *start, "--wait-seconds", "0.05"], context) == 2
    assert (policy.requests / ("1" * 32 + ".json")).exists()
    assert "1" * 32 in capsys.readouterr().err
    with _broker(policy, initialize=True):
        assert command(["remote", "check", "mounted"], context) == 0
        capsys.readouterr()
        assert control("health")["outcome"] == "ready"
        first = control("start", *start)
        assert first["outcome"] == "submitted"
    with _broker(policy, initialize=False):
        assert control("start", *start) == first
        handle = str(first["handle"])
        assert control("status", "--handle", handle)["scheduler_state"] == "RUNNING"
        refused = command(
            ["remote", "daemon", "start", "mounted", "--profile", "missing", "--request-id", "4" * 32], context
        )
        assert refused == 2
        assert json.loads(capsys.readouterr().out)["reason"] == "invalid_profile"
        assert control("cancel", "--handle", handle, "--request-id", "3" * 32)["outcome"] == "cancel_requested"
    calls = [json.loads(line) for line in submitted.read_text().splitlines()]
    assert len(calls) == 1
    assert "--mode payload" in calls[0][1]
    assert json.loads(cancelled.read_text()) == [
        "--ctld",
        "--clusters=cluster",
        f"--name=httk-{handle}",
        f"--user={os.getuid()}",
        "123",
    ]
    assert command(["job", "transfer", str(mounted.root), str(source.root), "--job", marker.job_id], context) == 0
    assert source.find_marker_by_id(marker.job_id) is not None
    assert mounted.find_marker_by_id(marker.job_id) is None
    assert not canary.exists()
