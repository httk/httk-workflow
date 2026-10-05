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
from httk.workflow._daemon_slurm import Observation, SchedulerError, SlurmGateway, UncertainSubmission, _run
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
        profiles=(Profile("cpu", 2, 512, 5, partition="batch", account="science"),),
    )


def test_submission_uses_fixed_script_stdin_and_filtered_environment(
    policy: Policy, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = tmp_path / "call.json"
    _client(
        policy.sbatch,
        f"open({str(record)!r}, 'w').write(json.dumps([sys.argv, dict(os.environ), sys.stdin.read()]))\n"
        "print('123;cluster')\n",
    )
    for name in ("SBATCH_WRAP", "SBATCH_PARTITION", "SLURM_CLUSTERS", "PYTHONPATH", "BASH_ENV", "LD_PRELOAD"):
        monkeypatch.setenv(name, "untrusted")
    monkeypatch.setenv("NSC_RESOURCE_NAME", "tetralith")
    monkeypatch.setenv("LANG", "sv_SE.UTF-8")
    gateway = SlurmGateway(policy, tmp_path / "policy with spaces.json")
    result = gateway.submit(policy.profile("cpu"), _HANDLE)
    assert (result.job_id, result.cluster) == ("123", "cluster")
    argv, environment, script = json.loads(record.read_text())
    assert "--export=NIL" in argv
    assert "--chdir=/" in argv
    assert {"--input=/dev/null", f"--output={tmp_path / 'snapshots/jobs/httk-%j.out'}"} <= set(argv)
    assert not any(item.startswith("--error") for item in argv)
    assert {"--nodes=1", "--ntasks=1", "--cpus-per-task=2", "--mem=512M", "--time=5"} <= set(argv)
    assert "--job-name=httk-" + _HANDLE in argv
    assert "--partition=batch" in argv and "--account=science" in argv
    assert all(not item.endswith(".sbatch") for item in argv)
    assert environment["NSC_RESOURCE_NAME"] == "tetralith"
    assert environment["LANG"] == environment["LC_ALL"] == "C"
    assert not {"SBATCH_WRAP", "SBATCH_PARTITION", "SLURM_CLUSTERS", "PYTHONPATH", "BASH_ENV", "LD_PRELOAD"} & set(
        environment
    )
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


def test_nonzero_submission_reports_the_client_error(policy: Policy) -> None:
    _client(policy.sbatch, "sys.stdin.read()\nprint('sbatch: error: Invalid account',file=sys.stderr)\nsys.exit(1)\n")
    with pytest.raises(UncertainSubmission, match="^sbatch exited 1: sbatch: error: Invalid account$"):
        SlurmGateway(policy, Path("/trusted/policy.json")).submit(policy.profile("cpu"), _HANDLE)


def test_unconfirmed_submission_reports_its_stdout(policy: Policy) -> None:
    _client(policy.sbatch, "sys.stdin.read()\nprint('Submitted batch job 12')\n")
    with pytest.raises(UncertainSubmission, match="^sbatch exited 0 without a confirmed job: Submitted batch job 12$"):
        SlurmGateway(policy, Path("/trusted/policy.json")).submit(policy.profile("cpu"), _HANDLE)


def test_client_error_excerpt_is_printable_and_bounded(policy: Policy) -> None:
    _client(
        policy.squeue,
        "sys.stderr.buffer.write(b'bad\\x1b[31m\\tline\\r\\n\\xc3\\xa5\\xff\\x00 ' + b'x' * 2000)\nsys.exit(2)\n",
    )
    with pytest.raises(SchedulerError) as error:
        SlurmGateway(policy, Path("/trusted/policy.json")).status("1", "cluster", _HANDLE)
    message = str(error.value)
    assert message.startswith("squeue exited 2: bad [31m line ?? x")
    assert len(message) == len("squeue exited 2: ") + 500
    assert all(" " <= character <= "~" for character in message)


def test_unstartable_client_is_named(policy: Policy) -> None:
    with pytest.raises(SchedulerError, match="^scancel could not start: .*No such file"):
        SlurmGateway(policy, Path("/trusted/policy.json")).cancel("1", "cluster", _HANDLE)


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
    with pytest.raises(SchedulerError, match="^squeue output exceeded its limit"):
        _run([str(policy.squeue)], replace(policy, max_output_bytes=1024))


@pytest.mark.timing
def test_client_timeout_kills_process(policy: Policy, tmp_path: Path) -> None:
    record = tmp_path / "pid"
    _client(policy.squeue, f"open({str(record)!r},'w').write(str(os.getpid()))\ntime.sleep(10)\n")
    with pytest.raises(SchedulerError, match="^squeue timed out$"):
        _run([str(policy.squeue)], replace(policy, command_timeout=1.0))
    with pytest.raises(ProcessLookupError):
        os.kill(int(record.read_text()), 0)


def test_protected_slurm_conf_overrides_the_operator_environment(
    policy: Policy, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client(policy.squeue, "print(json.dumps(dict(os.environ)))\n")
    monkeypatch.setenv("SLURM_CONF", "/operator/slurm.conf")
    monkeypatch.setenv("SBATCH_PARTITION", "other")
    monkeypatch.setenv("NSC_RESOURCE_NAME", "tetralith")
    policy = replace(policy, slurm_conf=tmp_path / "broker" / "slurm.conf")
    code, output, errors = _run([str(policy.squeue)], policy)
    assert code == 0 and errors == b""
    environment = json.loads(output)
    assert environment["SLURM_CONF"] == str(policy.slurm_conf) and environment["NSC_RESOURCE_NAME"] == "tetralith"
    assert environment["LANG"] == environment["LC_ALL"] == "C" and "SBATCH_PARTITION" not in environment


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


def _sacct(policy: Policy, tmp_path: Path, *rows: str) -> Policy:
    accounting = replace(policy, sacct=tmp_path / "runtime" / "sacct")
    assert accounting.sacct is not None
    record = tmp_path / "sacct.json"
    printed = "".join(f"print({row!r})\n" for row in rows)
    _client(accounting.sacct, f"open({str(record)!r},'w').write(json.dumps([sys.argv, dict(os.environ)]))\n{printed}")
    return accounting


def test_accounting_reads_the_matching_row_in_the_c_locale(policy: Policy, tmp_path: Path) -> None:
    user = str(os.getuid())
    name = f"httk-{_HANDLE}"
    accounting = _sacct(
        policy,
        tmp_path,
        f"123|{name}|{os.getuid() + 1}|COMPLETED|0:0|2026-10-05T10:00:00|2026-10-05T10:05:00",
        f"123|httk-{'c' * 32}|{user}|COMPLETED|0:0|Unknown|Unknown",
        f"456|{name}|{user}|COMPLETED|0:0|Unknown|Unknown",
        f"123|{name}|{user}|FAILED|1:0|2026-10-05T10:00:00|2026-10-05T10:01:02",
    )
    gateway = SlurmGateway(accounting, Path("/trusted/policy.json"))
    assert gateway.accounting("123", "cluster", _HANDLE) == Observation(
        "FAILED", "1:0", "2026-10-05T10:00:00", "2026-10-05T10:01:02"
    )
    argv, environment = json.loads((tmp_path / "sacct.json").read_text())
    assert argv[1:] == [
        "--clusters=cluster",
        "-j",
        "123",
        "-X",
        "-n",
        "-P",
        "--format=JobID,JobName,UID,State,ExitCode,Start,End",
    ]
    assert environment["LANG"] == environment["LC_ALL"] == "C" and "SLURM_CONF" not in environment


def test_accounting_normalizes_cancellation_and_unset_times(policy: Policy, tmp_path: Path) -> None:
    user = str(os.getuid())
    accounting = _sacct(policy, tmp_path, f"123|httk-{_HANDLE}|{user}|CANCELLED by 1234|0:15|None|Unknown")
    assert SlurmGateway(accounting, Path("/p.json")).accounting("123", "cluster", _HANDLE) == Observation(
        "CANCELLED", "0:15", None, None
    )


def test_accounting_absent_row_or_client_is_none_and_failures_raise(policy: Policy, tmp_path: Path) -> None:
    assert SlurmGateway(policy, Path("/p.json")).accounting("123", "cluster", _HANDLE) is None
    user = str(os.getuid())
    gateway = SlurmGateway(_sacct(policy, tmp_path), Path("/p.json"))
    assert gateway.accounting("123", "cluster", _HANDLE) is None
    for row in (f"123|httk-{_HANDLE}|{user}|lower|0:0|None|None", f"123|httk-{_HANDLE}|{user}|FAILED|x|None|None"):
        gateway = SlurmGateway(_sacct(policy, tmp_path, row), Path("/p.json"))
        with pytest.raises(SchedulerError, match="accounting is malformed"):
            gateway.accounting("123", "cluster", _HANDLE)
    row = f"123|httk-{_HANDLE}|{user}|FAILED|1:0|None|None"
    with pytest.raises(SchedulerError, match="ambiguous"):
        SlurmGateway(_sacct(policy, tmp_path, row, row), Path("/p.json")).accounting("123", "cluster", _HANDLE)
    accounting = _sacct(policy, tmp_path)
    assert accounting.sacct is not None
    _client(accounting.sacct, "sys.stderr.write('sacct: error: Slurm accounting storage is disabled')\nsys.exit(1)\n")
    with pytest.raises(SchedulerError, match="^sacct exited 1: sacct: error: Slurm accounting storage is disabled$"):
        SlurmGateway(accounting, Path("/p.json")).accounting("123", "cluster", _HANDLE)
    with pytest.raises(SchedulerError, match="outside the current policy"):
        SlurmGateway(accounting, Path("/p.json")).accounting("123", "other", _HANDLE)
