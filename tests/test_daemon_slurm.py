"""Exercise fixed scheduler operations with real bounded client processes."""

import json
import os
import shlex
import sys
import threading
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path

import pytest

from httk.workflow._daemon_auth import sign_request
from httk.workflow._daemon_keys import initialize_response_seed, response_public_key
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_policy import ApprovedLauncher, Policy
from httk.workflow._daemon_protocol import Request, decode_response, encode_request
from httk.workflow._daemon_service import Broker
from httk.workflow._daemon_slurm import (
    Observation,
    SchedulerError,
    SlurmGateway,
    UncertainSubmission,
    _run,
    manager_argv,
)
from httk.workflow._daemon_state import Ledger
from httk.workflow.launch_runtime import SubmissionIdentity, slurm_submission

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
        launchers=(_launcher("cpu", **CPU),),
    )


CPU: dict[str, str | int | None] = {
    "manager.confine": "bwrap",
    "slurm.cpus_per_task": "2",
    "slurm.mem": "512M",
    "slurm.time_limit": 5,
    "slurm.partition": "batch",
    "slurm.account": "science",
}


def _launcher(name: str, **settings: str | int | None) -> ApprovedLauncher:
    return ApprovedLauncher(name, tuple(sorted(settings.items())), "c" * 64)


def test_submission_pipes_the_slurm_launcher_script_with_the_trusted_identity(
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
    result = SlurmGateway(policy).submit(policy.launcher("cpu"), _HANDLE)
    assert (result.job_id, result.cluster) == ("123", "cluster")
    argv, environment, script = json.loads(record.read_text())
    assert argv[1:] == [
        "--parsable",
        "--export=NONE",
        "--no-requeue",
        "--input=/dev/null",
        "--clusters=cluster",
        f"--job-name=httk-{_HANDLE}",
        f"--output={tmp_path / 'snapshots/jobs/httk-%j.out'}",
    ]
    assert environment["NSC_RESOURCE_NAME"] == "tetralith"
    assert environment["LANG"] == environment["LC_ALL"] == "C"
    assert not {"SBATCH_WRAP", "SBATCH_PARTITION", "SLURM_CLUSTERS", "PYTHONPATH", "BASH_ENV", "LD_PRELOAD"} & set(
        environment
    )
    lines = script.splitlines()
    assert lines[0] == "#!/bin/bash -l"
    directives = [line for line in lines if line.startswith("#SBATCH")]
    assert directives == [
        "#SBATCH --account=science",
        "#SBATCH --partition=batch",
        "#SBATCH --time=5",
        "#SBATCH --cpus-per-task=2",
        "#SBATCH --mem=512M",
        f"#SBATCH --chdir={policy.workspace}",
    ]
    # Nothing in the script names the job or writes its output into the workspace.
    assert "--job-name" not in script and "--output" not in script and "--error" not in script
    exec_line = lines[-1]
    assert shlex.split(exec_line) == [
        "exec",
        *manager_argv(policy),
        "--allocation",
        "slurm",
        "--setting",
        "manager.confine=bwrap",
    ]
    assert manager_argv(policy) == [
        sys.executable,
        "-I",
        "-m",
        "httk.core.cli",
        "workflow",
        "manager",
        "run",
        "--by-path",
        "--workspace",
        str(policy.workspace),
        "--exchange",
        "--idle",
    ]
    assert not (policy.workspace / ".httk-workspace").exists()


def _submission_identity(policy: Policy) -> SubmissionIdentity:
    return SubmissionIdentity(
        job_name=f"httk-{_HANDLE}",
        sbatch=policy.sbatch,
        cluster=policy.cluster,
        output=str(policy.jobs / "httk-%j.out"),
    )


def test_submission_emits_one_export_none_by_default(policy: Policy) -> None:
    argv, _script = slurm_submission(
        settings=dict(policy.launcher("cpu").settings),
        argv=manager_argv(policy),
        workspace=str(policy.workspace),
        identity=_submission_identity(policy),
    )
    assert [item for item in argv if item.startswith("--export=")] == ["--export=NONE"]


def test_submission_honours_slurm_export_nil(policy: Policy) -> None:
    launcher = _launcher("cpu", **{**CPU, "slurm.export": "NIL"})
    argv, _script = slurm_submission(
        settings=dict(launcher.settings),
        argv=manager_argv(policy),
        workspace=str(policy.workspace),
        identity=_submission_identity(policy),
    )
    assert [item for item in argv if item.startswith("--export=")] == ["--export=NIL"]


@pytest.mark.parametrize("value", ["ALL", "NONE,FOO", "", "NONE --wrap x", "none", "nil", " NONE", "NONE\n", 1])
def test_slurm_submission_defensively_refuses_an_unvalidated_export(policy: Policy, value: object) -> None:
    # The frozen path validates first, so reach the sbatch-argv guard directly with a raw settings map.
    with pytest.raises(ValueError, match="slurm.export must be exactly"):
        slurm_submission(
            settings={**CPU, "slurm.export": value},
            argv=manager_argv(policy),
            workspace=str(policy.workspace),
            identity=_submission_identity(policy),
        )


def test_submission_uses_only_the_frozen_launcher_settings(policy: Policy, tmp_path: Path) -> None:
    record = tmp_path / "call.json"
    _client(policy.sbatch, f"open({str(record)!r}, 'w').write(sys.stdin.read())\nprint('1')\n")
    launcher = _launcher(
        "site",
        **{
            "manager.confine": "bwrap",
            "manager.workers": "4",
            "manager.allocation": "host",
            "manager.command": "site-httk",
            "environment.prelude": "module load httk",
            "confine.readonly_paths": "/usr:/software",
            "manager.launch_mpi": "pmi2",
            "slurm.gres": "gpu:a100=2",
            "slurm.reservation": "maint.1",
            "slurm.nodes": "2",
        },
    )
    assert SlurmGateway(policy).submit(launcher, _HANDLE).job_id == "1"
    script = record.read_text()
    assert "#SBATCH --gres=gpu:a100=2" in script and "#SBATCH --reservation=maint.1" in script
    assert "#SBATCH --nodes=2" in script
    assert script.splitlines()[-2] == "module load httk"
    command = shlex.split(script.splitlines()[-1])
    # After a prelude, the launcher's manager command replaces the isolated interpreter.
    assert command[:2] == ["exec", "site-httk"]
    assert command[2:10] == manager_argv(policy)[4:]
    assert command[10:] == [
        "--workers",
        "4",
        "--allocation",
        "host",
        "--setting",
        "confine.readonly_paths=/usr:/software",
        "--setting",
        "manager.confine=bwrap",
        "--setting",
        "manager.launch_mpi=pmi2",
    ]


def test_unexpressible_frozen_setting_fails_before_sbatch_runs(policy: Policy, tmp_path: Path) -> None:
    marker = tmp_path / "ran"
    _client(policy.sbatch, f"open({str(marker)!r}, 'w').close()\nprint('1')\n")
    with pytest.raises(SchedulerError, match="cannot build the submission: .*control characters"):
        SlurmGateway(policy).submit(_launcher("bad", **{"slurm.partition": "a\nb"}), _HANDLE)
    assert not marker.exists()


@pytest.mark.parametrize("output", ["bad", "123;other", "123;cluster\n124;cluster", "--help", "0", "123;cluster;extra"])
def test_unconfirmed_submission_is_uncertain(policy: Policy, output: str) -> None:
    _client(policy.sbatch, f"sys.stdin.read()\nprint({output!r})\n")
    with pytest.raises(UncertainSubmission):
        SlurmGateway(policy).submit(policy.launcher("cpu"), _HANDLE)


def test_nonzero_submission_reports_the_client_error(policy: Policy) -> None:
    _client(policy.sbatch, "sys.stdin.read()\nprint('sbatch: error: Invalid account',file=sys.stderr)\nsys.exit(1)\n")
    with pytest.raises(UncertainSubmission, match="^sbatch exited 1: sbatch: error: Invalid account$"):
        SlurmGateway(policy).submit(policy.launcher("cpu"), _HANDLE)


def test_unconfirmed_submission_reports_its_stdout(policy: Policy) -> None:
    _client(policy.sbatch, "sys.stdin.read()\nprint('Submitted batch job 12')\n")
    with pytest.raises(UncertainSubmission, match="^sbatch exited 0 without a confirmed job: Submitted batch job 12$"):
        SlurmGateway(policy).submit(policy.launcher("cpu"), _HANDLE)


def test_client_error_excerpt_is_printable_and_bounded(policy: Policy) -> None:
    _client(
        policy.squeue,
        "sys.stderr.buffer.write(b'bad\\x1b[31m\\tline\\r\\n\\xc3\\xa5\\xff\\x00 ' + b'x' * 2000)\nsys.exit(2)\n",
    )
    with pytest.raises(SchedulerError) as error:
        SlurmGateway(policy).status("1", "cluster", _HANDLE)
    message = str(error.value)
    assert message.startswith("squeue exited 2: bad [31m line ?? x")
    assert len(message) == len("squeue exited 2: ") + 500
    assert all(" " <= character <= "~" for character in message)


def test_unstartable_client_is_named(policy: Policy) -> None:
    with pytest.raises(SchedulerError, match="^scancel could not start: .*No such file"):
        SlurmGateway(policy).cancel("1", "cluster", _HANDLE)


@pytest.mark.parametrize(
    "version,valid", [("23.11.5", False), ("23.11.6", True), ("26.05.4", True), ("unknown", False)]
)
def test_controller_filter_version_requirement(policy: Policy, version: str, valid: bool) -> None:
    for client in (policy.sbatch, policy.squeue, policy.scancel):
        _client(client, f"assert sys.argv[1:]==['--version']\nprint('slurm {version}')\n")
    gateway = SlurmGateway(policy)
    if valid:
        gateway.check()
    else:
        with pytest.raises(SchedulerError):
            gateway.check()


def test_status_matches_identity_and_absence_is_unknown(policy: Policy) -> None:
    gateway = SlurmGateway(policy)
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
    SlurmGateway(policy).cancel("123", "cluster", _HANDLE)
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
                SlurmGateway(policy),
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
                SlurmGateway(policy),
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


def test_check_failure_names_the_client_and_its_output(policy: Policy) -> None:
    for client in (policy.sbatch, policy.scancel):
        _client(client, "print('slurm 24.05.1')\n")
    _client(policy.squeue, "print('wrapper says no', file=sys.stderr)\nsys.exit(3)\n")
    with pytest.raises(SchedulerError, match=r"squeue --version exited 3 and printed 'wrapper says no'"):
        SlurmGateway(policy).check()


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
    gateway = SlurmGateway(accounting)
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
    assert SlurmGateway(accounting).accounting("123", "cluster", _HANDLE) == Observation(
        "CANCELLED", "0:15", None, None
    )


def test_accounting_absent_row_or_client_is_none_and_failures_raise(policy: Policy, tmp_path: Path) -> None:
    assert SlurmGateway(policy).accounting("123", "cluster", _HANDLE) is None
    user = str(os.getuid())
    gateway = SlurmGateway(_sacct(policy, tmp_path))
    assert gateway.accounting("123", "cluster", _HANDLE) is None
    for row in (f"123|httk-{_HANDLE}|{user}|lower|0:0|None|None", f"123|httk-{_HANDLE}|{user}|FAILED|x|None|None"):
        gateway = SlurmGateway(_sacct(policy, tmp_path, row))
        with pytest.raises(SchedulerError, match="accounting is malformed"):
            gateway.accounting("123", "cluster", _HANDLE)
    row = f"123|httk-{_HANDLE}|{user}|FAILED|1:0|None|None"
    with pytest.raises(SchedulerError, match="ambiguous"):
        SlurmGateway(_sacct(policy, tmp_path, row, row)).accounting("123", "cluster", _HANDLE)
    accounting = _sacct(policy, tmp_path)
    assert accounting.sacct is not None
    _client(accounting.sacct, "sys.stderr.write('sacct: error: Slurm accounting storage is disabled')\nsys.exit(1)\n")
    with pytest.raises(SchedulerError, match="^sacct exited 1: sacct: error: Slurm accounting storage is disabled$"):
        SlurmGateway(accounting).accounting("123", "cluster", _HANDLE)
    with pytest.raises(SchedulerError, match="outside the current policy"):
        SlurmGateway(accounting).accounting("123", "other", _HANDLE)
