"""Manager-enforced ``maxtime``: timeouts, their escalation, and the attempt deadline."""

import json
import signal
import time
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.runtime import AttemptContext
from test_manager_scheduling import _doc, _failure, _job, _state
from v3_helpers import find
from v3_helpers import workspace as initialize_workspace

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("heartbeat-timing")]


def _run(tmp_path: Path, body: str, **job: object) -> tuple[Workspace, str, float]:
    """Run one job with *body* as its runner and return the workspace, job id, and wall time."""

    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(workspace, tmp_path, body, tag="timed", **job)
    started = time.monotonic()
    with TaskManager(workspace, cancel_grace_seconds=0.5, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=20.0)
    return workspace, job_id, time.monotonic() - started


def test_attempt_over_maxtime_fails_with_timeout(tmp_path: Path) -> None:
    workspace, job_id, seconds = _run(tmp_path, "time.sleep(30)\n", resources={"maxtime": 1})
    assert _state(workspace, job_id) == "failed" and seconds < 10
    failure = _failure(workspace, job_id)
    assert failure["code"] == "timeout"
    assert failure["message"] == "attempt exceeded its maxtime 00:00:01"
    assert failure["details"]["exit_status"] == -signal.SIGTERM


def test_attempt_ignoring_sigterm_is_killed_after_the_grace(tmp_path: Path) -> None:
    body = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(30)\n"
    workspace, job_id, seconds = _run(tmp_path, body, resources={"maxtime": 1})
    assert _state(workspace, job_id) == "failed" and seconds < 10
    failure = _failure(workspace, job_id)
    assert failure["code"] == "timeout"
    assert failure["details"]["exit_status"] == -signal.SIGKILL


def test_outcome_published_on_sigterm_wins_over_the_timeout(tmp_path: Path) -> None:
    body = """
def on_term(signum, frame):
    publish("succeed")
    sys.exit(0)

signal.signal(signal.SIGTERM, on_term)
time.sleep(30)
"""
    workspace, job_id, seconds = _run(tmp_path, body, resources={"maxtime": 1})
    assert _state(workspace, job_id) == "succeeded" and seconds < 10


def test_timeout_is_retried_when_listed_in_retry_on(tmp_path: Path) -> None:
    body = 'if context["attempt_ordinal"] == 1:\n    time.sleep(30)\npublish("succeed")\n'
    workspace, job_id, seconds = _run(tmp_path, body, resources={"maxtime": 1}, retry_on=("timeout",))
    assert _state(workspace, job_id) == "succeeded" and seconds < 10
    attempt = _doc(workspace, job_id).attempt
    assert attempt is not None and attempt["ordinal"] == 2


@pytest.mark.parametrize(("status", "code"), [(7, "process_failure"), (0, "protocol_error")])
def test_already_exited_attempt_is_not_retried_as_a_timeout(tmp_path: Path, status: int, code: str) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    job_id = _job(
        workspace, tmp_path, f"sys.exit({status})\n", tag="exited", resources={"maxtime": 1}, retry_on=("timeout",)
    )
    with TaskManager(workspace) as manager:
        assert manager._claim_and_launch(find(workspace, job_id))
        (attempt,) = manager._running.values()
        assert attempt.process.wait(timeout=10) == status
        # Model a delayed next tick after a known exit, without a timed sleep.
        attempt.started -= 2
        manager.run_until_idle(timeout=10)
    assert _state(workspace, job_id) == "failed"
    failure = _failure(workspace, job_id)
    assert failure["code"] == code
    assert failure["details"]["exit_status"] == status
    assert _doc(workspace, job_id).counters["attempts_total"] == 1


@pytest.mark.parametrize("maxtime", [600, None])
def test_deadline_is_present_exactly_with_maxtime(tmp_path: Path, maxtime: int | None) -> None:
    body = """
Path(os.environ["HTTK_WORKFLOW_WORKDIR"], "seen.json").write_text(json.dumps(
    {"context": context.get("deadline", "absent"), "env": os.environ.get("HTTK_WORKFLOW_DEADLINE", "absent")}
))
publish("succeed")
"""
    launched = time.time()
    resources = {} if maxtime is None else {"maxtime": maxtime}
    workspace, job_id, _ = _run(tmp_path, body, resources=resources)
    finished = time.time()
    assert _state(workspace, job_id) == "succeeded"
    seen = json.loads((find(workspace, job_id).path / "run" / "seen.json").read_text())
    if maxtime is None:
        assert seen == {"context": None, "env": "absent"}
    else:
        assert launched + maxtime - 1 <= seen["context"] <= finished + maxtime
        assert seen["env"] == str(seen["context"])


def test_attempt_context_deadline_is_optional_and_validated() -> None:
    base: dict[str, object] = {
        "format": "httk-workflow-attempt-context",
        "durable": False,
        "deadline": None,
        "settings": {},
        "format_version": 2,
        **{
            name: name
            for name in ("workspace_id", "job_id", "job_key", "placement", "step", "activation_id", "attempt_id")
        },
        "payload": "/payload",
    }
    assert AttemptContext.from_mapping(base).deadline is None
    assert AttemptContext.from_mapping({**base, "deadline": 1_900_000_000}).deadline == 1_900_000_000
    for bad in ("x", True, -1, 1.5):
        with pytest.raises(ValueError, match="deadline"):
            AttemptContext.from_mapping({**base, "deadline": bad})
