"""The manager's allocation end: the ``mintime`` start gate, the drain-point deadline, and their CLI."""

import argparse
import json
import logging
import time
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace, workflow_cli
from httk.workflow._slurm import slurm_end_time
from httk.workflow.introspection import read_managers
from httk.workflow.introspection._diagnosis import claim_requirements, manager_refusals
from httk.workflow.workflow_cli import _manager
from test_manager_scheduling import _SUCCEED_RUNNER, _payload
from test_maxtime import _HEADER

pytestmark = [pytest.mark.timing, pytest.mark.xdist_group("heartbeat-timing")]


def test_slurm_end_time_reads_only_a_positive_epoch_inside_a_job(caplog: pytest.LogCaptureFixture) -> None:
    assert slurm_end_time({"SLURM_JOB_END_TIME": "1900000000"}) is None
    assert slurm_end_time({"SLURM_JOB_ID": "7"}) is None
    assert slurm_end_time({"SLURM_JOB_ID": "7", "SLURM_JOB_END_TIME": "1900000000"}) == 1_900_000_000.0
    with caplog.at_level(logging.WARNING, logger="httk.workflow._slurm"):
        assert slurm_end_time({"SLURM_JOB_ID": "7", "SLURM_JOB_END_TIME": "soon"}) is None
    assert "SLURM_JOB_END_TIME" in caplog.text


def _submit(workspace: Workspace, tmp_path: Path, tag: str, resources: dict[str, int]) -> str:
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag=tag, resources=resources)
    workspace.submit(payload, f"project/{tag}")
    return job_id


def _kind(workspace: Workspace, job_id: str) -> str:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return marker.kind


def test_mintime_beyond_the_time_left_is_not_started(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    long_job = _submit(workspace, tmp_path, "long", {"mintime": 3600})
    short_job = _submit(workspace, tmp_path, "short", {"mintime": 30})
    plain_job = _submit(workspace, tmp_path, "plain", {})
    end_time = time.time() + 60
    with TaskManager(workspace, end_time=end_time, deadline_margin=10, heartbeat_interval=0.01) as manager:
        census = manager.run_until_idle(timeout=20.0)
        assert manager.drain_start == end_time - 10
    assert (_kind(workspace, long_job), _kind(workspace, short_job), _kind(workspace, plain_job)) == (
        "ready",
        "succeeded",
        "succeeded",
    )
    assert census.ready_blocked == {"time": {"mintime": 1}}
    assert "mintime" in census.summary_line()
    advice = census.mismatch_advice()
    assert advice is not None and "--time-limit" in advice and advice == census.time_advice()


def test_a_manager_started_past_its_drain_point_warns_and_claims_nothing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit(workspace, tmp_path, "plain", {})
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow.manager"),
        TaskManager(workspace, end_time=time.time() + 1, deadline_margin=100, heartbeat_interval=0.01) as manager,
    ):
        census = manager.run_until_idle(timeout=5.0)
    assert "drain point passed" in caplog.text
    assert _kind(workspace, job_id) == "ready"
    assert census.ready_blocked == {"time": {"drain_point": 1}}
    assert "past the drain point: 1" in census.summary_line()
    advice = census.time_advice()
    assert advice is not None and "mintime" not in advice


def test_mintime_does_not_gate_without_an_end_time(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit(workspace, tmp_path, "long", {"mintime": 3600})
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=20.0)
    assert _kind(workspace, job_id) == "succeeded"


def test_attempt_deadline_is_the_earlier_of_maxtime_and_the_drain_start(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    now = time.time()
    with TaskManager(workspace, end_time=now + 600, deadline_margin=100) as manager:
        drain = int(now + 500)
        assert manager._attempt_deadline({"maxtime": 3600}) == drain
        short = manager._attempt_deadline({"maxtime": 60})
        assert short is not None and abs(short - (now + 60)) <= 2
        assert manager._attempt_deadline({}) == drain
    with TaskManager(workspace) as manager:
        assert manager._attempt_deadline({}) is None


def test_drain_start_reaches_the_attempt_context_without_maxtime(tmp_path: Path) -> None:
    body = """
Path(os.environ["HTTK_WORKFLOW_WORKDIR"], "seen.json").write_text(json.dumps(
    {"context": context.get("deadline"), "env": os.environ.get("HTTK_WORKFLOW_DEADLINE")}
))
publish("succeed")
"""
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _HEADER + body, tag="timed")
    workspace.submit(payload, "project/timed")
    end_time = time.time() + 600
    with TaskManager(workspace, end_time=end_time, deadline_margin=100, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=20.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    seen = json.loads((workspace.payload_path(marker.placement, marker.job_key) / "run" / "seen.json").read_text())
    assert seen == {"context": int(end_time - 100), "env": str(int(end_time - 100))}


def test_manager_json_records_end_time_and_resources_and_introspection_shows_them(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    end_time = time.time() + 3600
    with TaskManager(workspace, end_time=end_time, resources={"procs": 4}) as manager:
        record = json.loads((workspace.control / "managers" / manager.manager_id / "manager.json").read_text())
        assert record["end_time"] == end_time and record["resources"] == {"procs": 4}
        (managed,) = read_managers(workspace)
        assert managed.end_time == end_time and managed.as_mapping()["end_time"] == end_time
        assert "ends in 00:59:" in managed.describe()
        payload, _ = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="long", resources={"mintime": 7200})
        job = workspace.load_job(workspace.submit(payload, "project/long"))
        (reason,) = manager_refusals(managed, claim_requirements(job), job=job)
        assert reason.startswith("its allocation leaves 00:5") and reason.endswith("needs mintime 02:00:00")
        del record["end_time"]
        del record["drain_start"]
        (workspace.control / "managers" / manager.manager_id / "manager.json").write_text(json.dumps(record))
        (older,) = read_managers(workspace)
        assert older.end_time is None and older.ends() is None
        assert manager_refusals(older, claim_requirements(job), job=job) == []
    with pytest.raises(ValueError, match="deadline_margin"):
        TaskManager(workspace, deadline_margin=-1)
    with pytest.raises(ValueError, match="end_time"):
        TaskManager(workspace, end_time=float("inf"))


def _arguments(**options: object) -> argparse.Namespace:
    return argparse.Namespace(**{**_manager.manager_option_defaults(), **options})


def test_end_time_is_the_earlier_of_time_limit_and_the_allocation(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(_manager.time, "time", lambda: 1000.0)
    slurm = slurm_end_time({"SLURM_JOB_ID": "7", "SLURM_JOB_END_TIME": "5000"})
    with caplog.at_level(logging.WARNING):
        assert _manager._manager_end_time(_arguments(time_limit="1:00:00"), slurm) == (4600.0, 120.0)
        assert _manager._manager_end_time(_arguments(time_limit="1:00:00"), None) == (4600.0, 120.0)
        assert _manager._manager_end_time(_arguments(), slurm) == (5000.0, 120.0)
        assert _manager._manager_end_time(_arguments(), None) == (None, 120.0)
        assert not caplog.records
        assert _manager._manager_end_time(_arguments(time_limit="2:00:00"), slurm) == (5000.0, 120.0)
        assert "ends after the allocation does" in caplog.text
        caplog.clear()
        # The margin is raised to the drain timeout, warning only when an end time makes it matter.
        assert _manager._manager_end_time(_arguments(deadline_margin=10.0), None) == (None, 30.0)
        assert not caplog.records
        assert _manager._manager_end_time(_arguments(deadline_margin=10.0), slurm) == (5000.0, 30.0)
        assert "raising --deadline-margin" in caplog.text
    with pytest.raises(ValueError, match="--time-limit"):
        _manager._manager_end_time(_arguments(time_limit="abc"), None)


def test_time_options_parse_and_forward(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    for leaf in (["manager", "run"], ["run"]):
        parsed = parser.parse_args([*leaf, "--time-limit", "1:00:00", "--deadline-margin", "300"])
        assert _manager.manager_argv_tail(parsed) == ["--time-limit", "1:00:00", "--deadline-margin", "300.0"]
    with pytest.raises(ValueError, match="--time-limit 00:01:00 does not exceed the 120 s deadline margin"):
        _manager.manager_argv_tail(_arguments(time_limit="1"))
    with pytest.raises(ValueError, match="does not exceed the 90 s"):
        _manager.manager_argv_tail(_arguments(time_limit="1:30", deadline_margin=10.0, drain_timeout=90.0))


def test_invalid_time_limit_is_refused_before_any_manager_starts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Workspace.initialize(tmp_path / "workspace").root

    def never(*_args: object, **_kwargs: object) -> int:
        raise AssertionError("a manager was launched")

    monkeypatch.setattr(_manager, "_run_in_process_manager", never)
    monkeypatch.setattr(_manager, "launch_processes", never)
    for options in ({}, {"detach": True}):
        with pytest.raises(ValueError, match="--time-limit"):
            _manager.launch_workspace_managers(
                root, _arguments(time_limit="abc", **options), CLIContext("httk", tmp_path)
            )
