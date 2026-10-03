"""Manager-enforced ``maxtime``: timeouts, their escalation, and the attempt deadline."""

import json
import signal
import time
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.runtime import AttemptContext
from test_manager_scheduling import _payload

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("heartbeat-timing")]

_PUBLISH = """
def publish(action):
    temporary = control / "outcome.tmp.test"
    temporary.mkdir()
    (temporary / "outcome.json").write_text(json.dumps({
        "format": "httk-workflow-outcome",
        "format_version": 2,
        "job_id": context["job_id"],
        "activation_id": context["activation_id"],
        "attempt_id": context["attempt_id"],
        "action": action,
    }))
    os.rename(temporary, control / "outcome.ready")
"""

_HEADER = (
    """#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
"""
    + _PUBLISH
)


def _run(tmp_path: Path, body: str, **job: object) -> tuple[Workspace, str, float]:
    """Run one job with *body* as its runner and return the workspace, job id, and wall time."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _HEADER + body, tag="timed", **job)  # type: ignore[arg-type]
    workspace.submit(payload, "project/timed")
    started = time.monotonic()
    with TaskManager(workspace, cancel_grace_seconds=0.5, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=20.0)
    return workspace, job_id, time.monotonic() - started


def _final(workspace: Workspace, job_id: str) -> tuple[str, dict[str, object]]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return marker.kind, workspace.read_state(marker)


def test_attempt_over_maxtime_fails_with_timeout(tmp_path: Path) -> None:
    workspace, job_id, seconds = _run(tmp_path, "time.sleep(30)\n", resources={"maxtime": 1})
    kind, state = _final(workspace, job_id)
    assert kind == "failed" and seconds < 10
    failure = state["failure"]
    assert isinstance(failure, dict)
    assert failure["code"] == "timeout"
    assert failure["message"] == "attempt exceeded its maxtime 00:00:01"
    assert failure["details"]["exit_status"] == -signal.SIGTERM


def test_attempt_ignoring_sigterm_is_killed_after_the_grace(tmp_path: Path) -> None:
    body = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(30)\n"
    workspace, job_id, seconds = _run(tmp_path, body, resources={"maxtime": 1})
    kind, state = _final(workspace, job_id)
    assert kind == "failed" and seconds < 10
    failure = state["failure"]
    assert isinstance(failure, dict)
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
    kind, _ = _final(workspace, job_id)
    assert kind == "succeeded" and seconds < 10


def test_timeout_is_retried_when_listed_in_retry_on(tmp_path: Path) -> None:
    body = 'if context["attempt_ordinal"] == 1:\n    time.sleep(30)\npublish("succeed")\n'
    workspace, job_id, seconds = _run(tmp_path, body, resources={"maxtime": 1}, retry_on=("timeout",))
    kind, state = _final(workspace, job_id)
    assert kind == "succeeded" and seconds < 10
    assert state["attempt_ordinal"] == 2


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
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    seen = json.loads((workspace.payload_path(marker.placement, marker.job_key) / "run" / "seen.json").read_text())
    if maxtime is None:
        assert seen == {"context": "absent", "env": "absent"}
    else:
        assert launched + maxtime - 1 <= seen["context"] <= finished + maxtime
        assert seen["env"] == str(seen["context"])


def test_attempt_context_deadline_is_optional_and_validated() -> None:
    base = {
        "format": "httk-workflow-attempt-context",
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
