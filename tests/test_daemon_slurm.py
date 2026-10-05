"""Exercise fixed scheduler operations with real bounded client processes."""

import json
import os
import sys
import threading
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path

import pytest

from httk.workflow._daemon_auth import sign_request
from httk.workflow._daemon_keys import initialize_response_seed, response_public_key
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_policy import Policy, Profile
from httk.workflow._daemon_protocol import Request, decode_response, encode_request
from httk.workflow._daemon_service import Broker
from httk.workflow._daemon_slurm import SchedulerError, SlurmGateway, UncertainSubmission, _run
from httk.workflow._daemon_state import Ledger

_HANDLE = "a" * 32


def _client(path: Path, body: str) -> None:
    path.write_text(f"#!{sys.executable}\nimport json, os, sys, time\n" + body)
    path.chmod(0o700)


@pytest.fixture
def policy(tmp_path: Path) -> Policy:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    for name in ("site/data", "site/exchange/requests", "site/exchange/responses", "state"):
        (tmp_path / name).mkdir(parents=True)
    return Policy(
        workspace=tmp_path / "site/data",
        workspace_id="12345678-1234-1234-1234-123456789abc",
        enrollment_id="b" * 32,
        exchange=tmp_path / "site/exchange",
        state=tmp_path / "state",
        snapshots=tmp_path / "snapshots",
        bwrap=Path("/usr/bin/bwrap"),
        python=Path(sys.executable),
        sbatch=runtime / "sbatch",
        squeue=runtime / "squeue",
        scancel=runtime / "scancel",
        cluster="cluster",
        readonly_paths=(runtime, Path(sys.prefix), Path("/usr")),
        broker_paths=(),
        profiles=(Profile("cpu", 2, 512, 5, partition="batch", account="science"),),
    )


def test_submission_uses_fixed_script_stdin_and_clean_environment(
    policy: Policy, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = tmp_path / "call.json"
    _client(
        policy.sbatch,
        f"open({str(record)!r}, 'w').write(json.dumps([sys.argv, dict(os.environ), sys.stdin.read()]))\n"
        "print('123;cluster')\n",
    )
    for name in ("SBATCH_WRAP", "SLURM_CLUSTERS", "PYTHONPATH", "BASH_ENV", "SSH_AUTH_SOCK", "LD_PRELOAD"):
        monkeypatch.setenv(name, "untrusted")
    gateway = SlurmGateway(policy, tmp_path / "policy with spaces.json")
    result = gateway.submit(policy.profile("cpu"), _HANDLE)
    assert (result.job_id, result.cluster) == ("123", "cluster")
    argv, environment, script = json.loads(record.read_text())
    assert "--export=NIL" in argv
    assert "--chdir=/" in argv
    assert {"--input=/dev/null", "--output=/dev/null", "--error=/dev/null"} <= set(argv)
    assert {"--nodes=1", "--ntasks=1", "--cpus-per-task=2", "--mem=512M", "--time=5"} <= set(argv)
    assert "--job-name=httk-" + _HANDLE in argv
    assert "--partition=batch" in argv and "--account=science" in argv
    assert all(not item.endswith(".sbatch") for item in argv)
    assert environment == {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"}
    assert script.startswith("#!/bin/sh\nexec ")
    assert " -I -S " in script and "_daemon_bootstrap.py" in script
    assert "--mode payload --profile cpu --handle " + _HANDLE in script
    assert "policy with spaces.json'" in script
    assert "prelude" not in script and "/workspace" not in script


def test_gres_and_reservation_follow_the_account_flag(policy: Policy, tmp_path: Path) -> None:
    record = tmp_path / "call.json"
    _client(
        policy.sbatch, f"open({str(record)!r}, 'w').write(json.dumps(sys.argv))\nsys.stdin.read()\nprint('1;cluster')\n"
    )
    gateway = SlurmGateway(policy, tmp_path / "policy.json")
    gateway.submit(Profile("gpu", account="science", gres="gpu:a100=2", reservation="maint.1"), _HANDLE)
    argv = json.loads(record.read_text())
    account = argv.index("--account=science")
    assert argv[account + 1 : account + 3] == ["--gres=gpu:a100=2", "--reservation=maint.1"]
    gateway.submit(Profile("plain"), _HANDLE)
    assert not any(item.startswith(("--gres=", "--reservation=")) for item in json.loads(record.read_text()))


def test_unset_resources_add_no_sbatch_flags(policy: Policy, tmp_path: Path) -> None:
    record = tmp_path / "call.json"
    _client(
        policy.sbatch, f"open({str(record)!r}, 'w').write(json.dumps(sys.argv))\nsys.stdin.read()\nprint('1;cluster')\n"
    )
    gateway = SlurmGateway(policy, tmp_path / "policy.json")
    gateway.submit(Profile("bare", partition="batch"), _HANDLE)
    argv = json.loads(record.read_text())
    assert not any(item.startswith(("--cpus-per-task=", "--mem=", "--time=")) for item in argv)
    assert {"--nodes=1", "--ntasks=1", "--partition=batch"} <= set(argv)
    gateway.submit(Profile("some", memory_mb=64), _HANDLE)
    argv = json.loads(record.read_text())
    assert "--mem=64M" in argv
    assert not any(item.startswith(("--cpus-per-task=", "--time=")) for item in argv)


@pytest.mark.parametrize("output", ["bad", "123;other", "123;cluster\n124;cluster", "--help", "0", "123;cluster;extra"])
def test_unconfirmed_submission_is_uncertain(policy: Policy, output: str) -> None:
    _client(policy.sbatch, f"sys.stdin.read()\nprint({output!r})\n")
    with pytest.raises(UncertainSubmission):
        SlurmGateway(policy, Path("/trusted/policy.json")).submit(policy.profile("cpu"), _HANDLE)


def test_nonzero_submission_does_not_echo_client_errors(policy: Policy) -> None:
    _client(policy.sbatch, "sys.stdin.read()\nprint('private-token',file=sys.stderr)\nsys.exit(1)\n")
    with pytest.raises(UncertainSubmission, match="^submission acceptance is unknown$"):
        SlurmGateway(policy, Path("/trusted/policy.json")).submit(policy.profile("cpu"), _HANDLE)


@pytest.mark.parametrize(
    "version,valid", [("23.11.5", False), ("23.11.6", True), ("26.05.4", True), ("unknown", False)]
)
def test_controller_filter_version_requirement(policy: Policy, version: str, valid: bool) -> None:
    for client in (policy.sbatch, policy.squeue, policy.scancel):
        _client(client, f"assert sys.argv[1:]==['--version']\nprint('slurm {version}')\n")
    gateway = SlurmGateway(policy, Path("/trusted/policy.json"))
    if valid:
        gateway.check()
    else:
        with pytest.raises(SchedulerError):
            gateway.check()


def test_status_matches_identity_and_absence_is_unknown(policy: Policy) -> None:
    gateway = SlurmGateway(policy, Path("/trusted/policy.json"))
    row = f"123|httk-{_HANDLE}|{os.getuid()}|RUNNING"
    _client(policy.squeue, f"print('CLUSTER: cluster')\nprint({row!r})\n")
    assert gateway.status("123", "cluster", _HANDLE) == "RUNNING"
    assert gateway.status("456", "cluster", _HANDLE) == "UNKNOWN"
    assert gateway.status("123", "cluster", "c" * 32) == "UNKNOWN"
    _client(policy.squeue, "print('CLUSTER: cluster')\n")
    assert gateway.status("123", "cluster", _HANDLE) == "UNKNOWN"


def test_cancel_sends_identity_filters_to_controller_without_lookup(policy: Policy, tmp_path: Path) -> None:
    record = tmp_path / "cancel.json"
    _client(policy.scancel, f"open({str(record)!r},'w').write(json.dumps(sys.argv))\n")
    SlurmGateway(policy, Path("/trusted/policy.json")).cancel("123", "cluster", _HANDLE)
    assert json.loads(record.read_text())[1:] == [
        "--ctld",
        "--clusters=cluster",
        f"--name=httk-{_HANDLE}",
        f"--user={os.getuid()}",
        "123",
    ]
    assert not policy.squeue.exists()


def test_combined_output_is_bounded(policy: Policy) -> None:
    _client(policy.squeue, "sys.stdout.write('a'*800)\nsys.stdout.flush()\nsys.stderr.write('b'*800)\n")
    with pytest.raises(SchedulerError, match="output exceeded"):
        _run([str(policy.squeue)], replace(policy, max_output_bytes=1024))


@pytest.mark.timing
def test_client_timeout_kills_process(policy: Policy, tmp_path: Path) -> None:
    record = tmp_path / "pid"
    _client(policy.squeue, f"open({str(record)!r},'w').write(str(os.getpid()))\ntime.sleep(10)\n")
    with pytest.raises(SchedulerError, match="timed out"):
        _run([str(policy.squeue)], replace(policy, command_timeout=1.0))
    with pytest.raises(ProcessLookupError):
        os.kill(int(record.read_text()), 0)


def test_protected_slurm_conf_is_the_only_extra_environment(policy: Policy, tmp_path: Path) -> None:
    _client(policy.squeue, "print(os.environ['SLURM_CONF'])\n")
    broker_root = tmp_path / "broker"
    policy = replace(policy, broker_paths=(broker_root,), slurm_conf=broker_root / "slurm.conf")
    code, output = _run([str(policy.squeue)], policy)
    assert code == 0 and output.decode().strip() == str(policy.slurm_conf)


def test_mailbox_to_scheduler_replays_after_reopening_private_state(policy: Policy, tmp_path: Path) -> None:
    client_keys = tmp_path / "client-keys"
    client_keys.mkdir()
    client_seed = initialize_response_seed(client_keys)
    response_seed = initialize_response_seed(policy.state)
    policy = replace(policy, authorized_keys=(response_public_key(client_seed),))
    submitted_calls = tmp_path / "submitted"
    cancelled_calls = tmp_path / "cancelled"
    _client(
        policy.sbatch,
        "sys.stdin.read()\n"
        f"with open({str(submitted_calls)!r}, 'a') as record: record.write('submitted\\n')\n"
        "print('123;cluster')\n",
    )
    start = sign_request(
        Request(
            "1" * 32,
            policy.workspace_id,
            "start_manager",
            profile="cpu",
            enrollment_id=policy.enrollment_id,
            configuration_digest=policy.configuration_digest("cpu"),
        ),
        seed_path=client_seed,
    )
    with ExitStack() as stack:
        requests = stack.enter_context(MailboxDirectory(policy.requests))
        responses = stack.enter_context(MailboxDirectory(policy.responses))
        with Ledger(policy.state, policy.workspace_id, policy.enrollment_id, initialize=True) as ledger:
            requests.replace(start.request_id + ".json", encode_request(start))
            Broker(
                policy,
                SlurmGateway(policy, tmp_path / "policy.json"),
                ledger,
                requests,
                responses,
                response_seed=response_seed,
            ).run(threading.Event(), once=True)
            submitted = decode_response(responses.read(start.request_id + ".json"))
            assert submitted.outcome == "submitted" and submitted.handle is not None
            assert requests.names() == ()
        handle = submitted.handle
        _client(policy.squeue, f"print('123|httk-{handle}|{os.getuid()}|RUNNING')\n")
        _client(policy.scancel, f"open({str(cancelled_calls)!r}, 'w').write(json.dumps(sys.argv[1:]))\n")
        with Ledger(policy.state, policy.workspace_id, policy.enrollment_id) as ledger:
            ledger.recover()
            broker = Broker(
                policy,
                SlurmGateway(policy, tmp_path / "policy.json"),
                ledger,
                requests,
                responses,
                response_seed=response_seed,
            )
            requests.replace(start.request_id + ".json", encode_request(start))
            status = Request(
                "2" * 32, policy.workspace_id, "manager_status", handle=handle, enrollment_id=policy.enrollment_id
            )
            cancel = Request(
                "3" * 32, policy.workspace_id, "cancel_manager", handle=handle, enrollment_id=policy.enrollment_id
            )
            for request in (status, cancel):
                requests.replace(
                    request.request_id + ".json", encode_request(sign_request(request, seed_path=client_seed))
                )
            broker.run(threading.Event(), once=True)
            assert decode_response(responses.read(start.request_id + ".json")) == submitted
            assert decode_response(responses.read(status.request_id + ".json")).scheduler_state == "RUNNING"
            assert decode_response(responses.read(cancel.request_id + ".json")).outcome == "cancel_requested"
            assert requests.names() == ()
    assert submitted_calls.read_text() == "submitted\n"
    assert json.loads(cancelled_calls.read_text()) == [
        "--ctld",
        "--clusters=cluster",
        f"--name=httk-{handle}",
        f"--user={os.getuid()}",
        "123",
    ]


def _mpi_policy(policy: Policy, tmp_path: Path) -> Policy:
    from httk.workflow._daemon_policy import MPIProfile, MPISettings

    settings = MPISettings(
        srun=policy.sbatch.with_name("srun"),
        control_root=tmp_path / "mpi-control",
        pmix_roots=(tmp_path / "pmix",),
    )
    profile = replace(policy.profiles[0], mpi=MPIProfile(nodes=2, ranks=4, ntasks_per_node=2))
    return replace(policy, profiles=(profile,), mpi=settings)


def test_mpi_submission_has_fixed_geometry_and_forbids_requeue(policy: Policy, tmp_path: Path) -> None:
    policy = _mpi_policy(policy, tmp_path)
    record = tmp_path / "mpi-submit.json"
    _client(
        policy.sbatch,
        f"open({str(record)!r}, 'w').write(json.dumps([sys.argv, sys.stdin.read()]))\nprint('123;cluster')\n",
    )
    SlurmGateway(policy, tmp_path / "protected.json").submit(policy.profiles[0], _HANDLE)
    argv, script = json.loads(record.read_text())
    assert {
        "--nodes=2",
        "--ntasks=4",
        "--ntasks-per-node=2",
        "--cpus-per-task=2",
        "--mem=512M",
        "--no-requeue",
    } <= set(argv)
    assert "--mode allocation --profile cpu --handle " + _HANDLE in script
    assert " -I -S " in script


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_mpi_check_accepts_pmix_listing_on_either_stream(policy: Policy, tmp_path: Path, stream: str) -> None:
    policy = _mpi_policy(policy, tmp_path)
    assert policy.mpi is not None
    for client in (policy.sbatch, policy.squeue, policy.scancel):
        _client(client, "print('slurm 23.11.6')\n")
    _client(
        policy.mpi.srun,
        "if sys.argv[1:] == ['--version']: print('slurm 23.11.6')\n"
        f"else: print('MPI plugin types are...\\n    none\\n    pmix', file=sys.{stream})\n",
    )
    SlurmGateway(policy, tmp_path / "protected.json").check()


def test_mpi_check_rejects_missing_direct_plugin(policy: Policy, tmp_path: Path) -> None:
    policy = _mpi_policy(policy, tmp_path)
    assert policy.mpi is not None
    for client in (policy.sbatch, policy.squeue, policy.scancel):
        _client(client, "print('slurm 23.11.6')\n")
    _client(
        policy.mpi.srun,
        "print('slurm 23.11.6' if sys.argv[1:] == ['--version'] else 'none\\npmi2')\n",
    )
    with pytest.raises(SchedulerError, match="pmix"):
        SlurmGateway(policy, tmp_path / "protected.json").check()


def test_check_failure_names_the_client_and_its_output(policy: Policy) -> None:
    for client in (policy.sbatch, policy.scancel):
        _client(client, "print('slurm 24.05.1')\n")
    _client(policy.squeue, "print('wrapper says no', file=sys.stderr)\nsys.exit(3)\n")
    with pytest.raises(SchedulerError, match=r"squeue --version exited 3 and printed 'wrapper says no'"):
        SlurmGateway(policy, Path("/trusted/policy.json")).check()
