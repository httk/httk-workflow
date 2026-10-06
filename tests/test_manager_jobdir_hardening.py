"""A job that tampers with its own control paths fails alone, never redirecting or stalling the manager.

Each scenario runs a real :class:`~httk.workflow.TaskManager` over a job whose
runner plants a symlink, FIFO, or oversized document on one of the manager's
control paths in its own directory and then exits, so the manager meets the
tampering while ending that attempt and launching the next one. A healthy job
in the same workspace must still succeed, nothing outside the job directory
may change, and nothing may block.
"""

import json
import os
import signal
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import FrameType
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectoryError
from httk.workflow._logging import reset_logging
from httk.workflow.models import Marker

_RUNNER = """#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
OUTSIDE = Path({outside!r})
LIMIT = {limit!r}


def outcome(action="succeed"):
    return {{
        "format": "httk-workflow-outcome",
        "format_version": 2,
        "job_id": context["job_id"],
        "activation_id": context["activation_id"],
        "attempt_id": context["attempt_id"],
        "action": action,
    }}


def publish(document=None):
    temporary = control / "outcome.tmp.test"
    temporary.mkdir()
    (temporary / "outcome.json").write_text(json.dumps(document or outcome()))
    os.rename(temporary, control / "outcome.ready")


if context["attempt_ordinal"] == 1:
{tamper}
publish()
"""

#: What the first attempt of each scenario does to its own job directory, and the
#: failure code the job must end with.
_SCENARIOS: dict[str, tuple[str, str]] = {
    # (a) the stdio chronicle is a symlink to a file outside the job directory.
    "stdio_symlink": (
        """
    (job / "logs" / "stdio.out").unlink()
    (job / "logs" / "stdio.out").symlink_to(OUTSIDE / "target")
    sys.exit(1)
""",
        "protocol_error",
    ),
    # (b) the whole log directory is a symlink to a directory outside.
    "logs_symlink": (
        """
    (job / "logs").rename(job / "logs.moved")
    (job / "logs").symlink_to(OUTSIDE, target_is_directory=True)
    sys.exit(1)
""",
        "protocol_error",
    ),
    # (c) the stdio chronicle is a FIFO nobody reads.
    "stdio_fifo": (
        """
    (job / "logs" / "stdio.out").unlink()
    os.mkfifo(job / "logs" / "stdio.out")
    sys.exit(1)
""",
        "protocol_error",
    ),
    # (d) the attempt container is a symlink to a directory outside.
    "attempts_symlink": (
        """
    os.rename(job / "attempts", job / "attempts.moved")
    (job / "attempts").symlink_to(OUTSIDE, target_is_directory=True)
    sys.exit(1)
""",
        "protocol_error",
    ),
    # (e) outcome.ready is a symlink to an outside directory holding a valid outcome.
    "outcome_symlink": (
        """
    (OUTSIDE / "published").mkdir()
    (OUTSIDE / "published" / "outcome.json").write_text(json.dumps(outcome()))
    (control / "outcome.ready").symlink_to(OUTSIDE / "published", target_is_directory=True)
    sys.exit(0)
""",
        "protocol_error",
    ),
    # (f) .httk-job is a symlink to an outside directory holding a "seal": following
    # it would make every transition of the job raise SealedError. The job gets a
    # single attempt, so the failure transition itself is what must not be refused.
    "state_symlink": (
        """
    (OUTSIDE / "state").mkdir()
    (OUTSIDE / "state" / "seal.json").write_text("{}")
    if (job / ".httk-job").exists():
        (job / ".httk-job").rename(job / ".httk-job.moved")
    (job / ".httk-job").symlink_to(OUTSIDE / "state", target_is_directory=True)
    sys.exit(1)
""",
        "process_failure",
    ),
    # (g) the persistent workdir is a symlink to a directory outside.
    "workdir_symlink": (
        """
    os.chdir(job)
    (job / "run").rename(job / "run.moved")
    (job / "run").symlink_to(OUTSIDE, target_is_directory=True)
    sys.exit(1)
""",
        "protocol_error",
    ),
    # An oversized outcome document.
    "outcome_oversized": (
        """
    temporary = control / "outcome.tmp.test"
    temporary.mkdir()
    with (temporary / "outcome.json").open("wb") as handle:
        handle.truncate(LIMIT + 1)
    os.rename(temporary, control / "outcome.ready")
    sys.exit(0)
""",
        "protocol_error",
    ),
    # A FIFO as the environment-resolution marker next to a valid outcome.
    "environment_marker_fifo": (
        """
    os.mkfifo(control / ".httk-environment-resolution.json")
    publish()
    sys.exit(0)
""",
        "protocol_error",
    ),
}
#: Scenarios whose job gets no second attempt.
_SINGLE_ATTEMPT = {"state_symlink"}


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    reset_logging()
    yield
    reset_logging()


@pytest.fixture(autouse=True)
def _watchdog() -> Iterator[None]:
    """Turn a blocked manager into a test failure instead of a hung test run."""

    def expire(number: int, frame: FrameType | None) -> None:
        raise TimeoutError("the manager blocked")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.alarm(90)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _payload(root: Path, tag: str, runner_source: str, *, attempts: int) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": tag,
        "name": f"Job directory hardening {tag}",
        "workflow": "tests.jobdir",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "only",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {
            "maximum_attempts_per_activation": attempts,
            "maximum_total_attempts": attempts,
            "maximum_activations": 1,
            "retry_on": ["process_failure"] if attempts > 1 else [],
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _runner(outside: Path, tamper: str = "    pass\n") -> str:
    return _RUNNER.format(outside=str(outside), limit=CONTROL_DOCUMENT_LIMIT, tamper=tamper.strip("\n") + "\n")


def _snapshot(directory: Path) -> dict[str, Any]:
    """Record every entry below *directory*, following nothing."""

    state: dict[str, Any] = {}
    for current, directories, files in os.walk(directory):
        for name in (*directories, *files):
            path = Path(current, name)
            information = path.lstat()
            content = path.read_bytes() if path.is_file() and not path.is_symlink() else None
            state[str(path.relative_to(directory))] = (information.st_mode, information.st_size, content)
    return state


def _failure_code(workspace: Workspace, job_id: str) -> tuple[str, str | None]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    if marker.kind != "failed":
        return marker.kind, None
    return marker.kind, workspace.read_state(marker)["failure"]["code"]


@pytest.mark.parametrize("scenario", sorted(_SCENARIOS))
def test_tampering_with_a_control_path_fails_only_that_job(tmp_path: Path, scenario: str) -> None:
    tamper, expected = _SCENARIOS[scenario]
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "target").write_text("outside\n", encoding="utf-8")
    attempts = 1 if scenario in _SINGLE_ATTEMPT else 2
    hostile_payload, hostile_id = _payload(tmp_path / "source", "hostile", _runner(outside, tamper), attempts=attempts)
    healthy_payload, healthy_id = _payload(tmp_path / "source", "healthy", _runner(outside), attempts=1)
    workspace.submit(hostile_payload, "project/hostile")
    workspace.submit(healthy_payload, "project/healthy")

    started = time.monotonic()
    with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=1.0) as manager:
        manager.run_until_idle(timeout=60.0)
        # The tampering only makes the manager record a failure, so it is still serving.
        manager.tick()
    elapsed = time.monotonic() - started

    # Nothing the manager did was written through the planted links: the only
    # entries outside are the target and what the job itself put there.
    planted = _snapshot(outside)
    assert planted.pop("target")[2] == b"outside\n"
    assert set(planted) <= {"published", "published/outcome.json", "state", "state/seal.json"}
    assert _failure_code(workspace, hostile_id) == ("failed", expected)
    assert _failure_code(workspace, healthy_id) == ("succeeded", None)
    assert elapsed < 60.0


@pytest.mark.timing
def test_a_fenced_attempt_ignoring_sigterm_is_killed_after_the_grace(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    runner = _runner(outside).replace(
        "\npublish()\n",
        "\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\npublish()\n"
        '(OUTSIDE / "pid").write_text(str(os.getpid()))\ntime.sleep(120)\n',
    )
    payload, job_id = _payload(tmp_path / "source", "lingering", runner, attempts=1)
    workspace.submit(payload, "project/lingering")

    started = time.monotonic()
    with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5) as manager:
        manager.run_until_idle(timeout=60.0)
        assert not manager._running
    elapsed = time.monotonic() - started

    assert _failure_code(workspace, job_id) == ("succeeded", None)
    pid = int((outside / "pid").read_text(encoding="utf-8"))
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert elapsed < 30.0


def test_an_undescribable_launch_digest_is_logged_and_the_launch_proceeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    linked_payload, linked_id = _payload(tmp_path / "source", "linked", _runner(outside), attempts=1)
    plain_payload, plain_id = _payload(tmp_path / "source", "plain", _runner(outside), attempts=1)
    linked = workspace.submit(linked_payload, "project/linked")
    workspace.submit(plain_payload, "project/plain")
    # A contained relative symlink a previous attempt left in the persistent
    # workdir is legal payload content that the payload digest cannot describe.
    workdir = workspace.payload_path(linked.placement, linked.job_key) / "run"
    workdir.mkdir()
    (workdir / "alias").symlink_to("../files/runner")

    with (
        caplog.at_level("WARNING", logger="httk.workflow.manager"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
        manager.run_until_idle(timeout=60.0)

    assert _failure_code(workspace, linked_id) == ("succeeded", None)
    assert _failure_code(workspace, plain_id) == ("succeeded", None)
    assert any("cannot record the launch payload digest" in record.getMessage() for record in caplog.records)
    launches = [getattr(record, "event", None) for record in caplog.records]
    assert "payload_digest_unavailable" in launches


def test_a_job_directory_refusal_while_polling_fails_only_that_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    bad_payload, bad_id = _payload(tmp_path / "source", "bad", _runner(outside), attempts=1)
    good_payload, good_id = _payload(tmp_path / "source", "good", _runner(outside), attempts=1)
    workspace.submit(bad_payload, "project/bad")
    workspace.submit(good_payload, "project/good")
    real_poll = TaskManager._poll_one_running

    def poll(self: TaskManager, marker: Marker, *args: Any) -> bool:
        if marker.job_id == bad_id:
            # What a digest of a tree holding a special file reports.
            raise JobDirectoryError("special file is forbidden in immutable bundle")
        return real_poll(self, marker, *args)

    monkeypatch.setattr(TaskManager, "_poll_one_running", poll)
    with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=1.0) as manager:
        manager.run_until_idle(timeout=60.0)

    assert _failure_code(workspace, bad_id) == ("failed", "protocol_error")
    assert _failure_code(workspace, good_id) == ("succeeded", None)


def test_a_payload_runner_is_hashed_by_descriptor_and_executed_by_its_path(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    runner = _runner(outside).replace("\npublish()\n", '\n(OUTSIDE / "file").write_text(__file__)\npublish()\n')
    payload, job_id = _payload(tmp_path / "source", "runner", runner, attempts=1)
    marker = workspace.submit(payload, "project/runner")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _failure_code(workspace, job_id) == ("succeeded", None)
    installed = workspace.payload_path(marker.placement, marker.job_key)
    # The runner sees its real path, so it can locate the files beside it.
    assert (outside / "file").read_text(encoding="utf-8") == str(installed / "files" / "runner")
    events = [
        json.loads(line)
        for line in (installed / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    launched = [event for event in events if event.get("kind") == "attempt"]
    assert launched and launched[0]["runner_path"] == str(installed / "files" / "runner")
    assert isinstance(launched[0]["runner_sha256"], str) and len(launched[0]["runner_sha256"]) == 64


def test_an_execute_only_payload_runner_still_launches_without_a_digest(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    payload, job_id = _payload(tmp_path / "source", "exec-only", _runner(outside), attempts=1)
    # A binary the manager may execute but not read: it exits 0 without an outcome.
    true = Path("/bin/true")
    (payload / "files" / "runner").write_bytes(true.read_bytes())
    (payload / "files" / "runner").chmod(0o755)
    marker = workspace.submit(payload, "project/exec-only")
    installed = workspace.payload_path(marker.placement, marker.job_key)
    (installed / "files" / "runner").chmod(0o111)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    state = workspace.read_state(workspace.find_marker_by_id(job_id))  # type: ignore[arg-type]
    assert state["failure"]["code"] == "protocol_error"
    assert "without an outcome" in state["failure"]["message"]
    events = [
        json.loads(line)
        for line in (installed / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    launched = [event for event in events if event.get("kind") == "attempt"]
    assert launched and launched[0]["runner_sha256"] is None


def test_a_symlinked_payload_runner_planted_after_registration_fails_the_launch(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    # Were the planted symlink followed, the target would leave a sentinel.
    (outside / "target").write_text(f"#!/bin/sh\ntouch {outside / 'ran'}\nexit 0\n", encoding="utf-8")
    (outside / "target").chmod(0o755)
    payload, job_id = _payload(tmp_path / "source", "linked-runner", _runner(outside), attempts=1)
    marker = workspace.submit(payload, "project/linked-runner")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        installed = workspace.payload_path(marker.placement, marker.job_key)
        (installed / "files" / "runner").unlink()
        (installed / "files" / "runner").symlink_to(outside / "target")
        manager.run_until_idle(timeout=60.0)
    assert _failure_code(workspace, job_id) == ("failed", "protocol_error")
    assert not (outside / "ran").exists()
    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None
    message = workspace.read_state(failed)["failure"]["message"]
    assert "cannot launch runner" in message and "not a regular file" in message, message


def _tick_until(manager: TaskManager, workspace: Workspace, job_id: str, kind: str) -> None:
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        manager.tick()
        marker = workspace.find_marker_by_id(job_id)
        if marker is not None and marker.kind == kind:
            return
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} never reached {kind}")


@pytest.mark.parametrize("state", ["ready", "running"])
def test_a_fifo_job_definition_fails_the_job_without_stalling_the_manager(tmp_path: Path, state: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    sleeper = _runner(outside).replace("\npublish()\n", "\ntime.sleep(120)\n")
    hostile_payload, hostile_id = _payload(
        tmp_path / "source", "hostile", sleeper if state == "running" else _runner(outside), attempts=1
    )
    healthy_payload, healthy_id = _payload(tmp_path / "source", "healthy", _runner(outside), attempts=1)
    hostile = workspace.submit(hostile_payload, "project/hostile")
    installed = workspace.payload_path(hostile.placement, hostile.job_key)

    with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5) as manager:
        if state == "ready":
            manager._register_submissions()
        else:
            _tick_until(manager, workspace, hostile_id, "running")
        # The job replaces its own definition with a FIFO nobody writes.
        (installed / "job.json").unlink()
        os.mkfifo(installed / "job.json")
        workspace.submit(healthy_payload, "project/healthy")
        manager.run_until_idle(timeout=60.0)
        assert not manager._running

    kind, code = _failure_code(workspace, hostile_id)
    assert (kind, code) == ("failed", "protocol_error")
    failed = workspace.find_marker_by_id(hostile_id)
    assert failed is not None
    assert "job.json is not a regular file" in workspace.read_state(failed)["failure"]["message"]
    assert _failure_code(workspace, healthy_id) == ("succeeded", None)


def test_a_job_under_a_symlinked_placement_directory_runs(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    # Placement directories are operator layout: project -> /scratch/project.
    scratch = tmp_path / "scratch" / "project"
    scratch.mkdir(parents=True)
    (workspace.jobs / "project").symlink_to(scratch, target_is_directory=True)
    payload, job_id = _payload(tmp_path / "source", "placed", _runner(outside), attempts=1)
    marker = workspace.submit(payload, "project/placed")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _failure_code(workspace, job_id) == ("succeeded", None)
    assert (scratch / "placed" / marker.job_key / "logs" / "stdio.out").is_file()


def test_a_tampered_job_seal_path_is_a_discrepancy_not_an_error(tmp_path: Path) -> None:
    from httk.workflow.seals import INVALID, is_job_sealed, verify_job_seal

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "seal.json").write_text("{}", encoding="utf-8")
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / ".httk-job").symlink_to(outside, target_is_directory=True)
    verdict = verify_job_seal(payload)
    assert not verdict.valid and verdict.verdict == INVALID
    assert [(item.path, item.kind) for item in verdict.discrepancies] == [("payload", "invalid")]
    assert not is_job_sealed(payload)

    (payload / ".httk-job").unlink()
    (payload / ".httk-job").mkdir()
    os.mkfifo(payload / ".httk-job" / "seal.json")
    verdict = verify_job_seal(payload)
    assert [(item.path, item.kind) for item in verdict.discrepancies] == [("payload", "invalid")]
    assert not is_job_sealed(payload)
