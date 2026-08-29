"""The Python authoring SDK: dynamic step graphs, job state, and outcomes."""

import json
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import (
    Attempt,
    ChildSpec,
    FormatError,
    Runner,
    RunnerRef,
    TaskManager,
    Workspace,
)
from httk.workflow.models import Marker
from httk.workflow.protocol import JobDefinition, JobSpec, prepare_job_payload

_SRC = str(Path(__file__).parents[1] / "src")

# One published runner implements the whole campaign, and every child job is
# synthesized from a step name and its inputs: nothing declares the graph.
_CAMPAIGN_RUNNER = f'''#!/usr/bin/env python3
import json
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.campaign")


@run.step
def characterize(a):
    a.state["sites"] = a.parameter("sites")
    failing = a.parameter("failing", [])
    for site in range(a.parameter("sites")):
        a.spawn(
            ChildSpec(
                step="relax",
                parameters={{"site": site, "diverge": site in failing}},
                maximum_attempts_per_activation=1,
            ),
            label="site-%d" % site,
            placement="project/children",
        )
    a.gather("aggregate", when=a.parameter("when", "all_terminal"), on_impossible="triage")


@run.step
def relax(a):
    (a.workdir / "site.txt").write_text(str(a.parameter("site")), encoding="utf-8")
    if a.parameter("diverge"):
        a.fail("relax.diverged", "site %s did not relax" % a.parameter("site"))
    else:
        a.succeed()


@run.step
def aggregate(a):
    failed = [child.label for child in a.children.failed]
    (a.workdir / "report.json").write_text(
        json.dumps(
            {{
                "sites": a.state["sites"],
                "labels": [child.label for child in a.children.succeeded],
                "relaxed": [
                    (child.workdir / "site.txt").read_text(encoding="utf-8") for child in a.children.succeeded
                ],
                "keys": [a.children[child.label].job_key for child in a.children.all],
                "failed": failed,
                "codes": [child.failure.code for child in a.children.failed],
            }},
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    if failed:
        a.advance("triage", state={{"failed": failed}})
    else:
        a.succeed()


@run.step
def triage(a):
    failed = a.state.get("failed") or [child.label for child in a.children.failed]
    (a.workdir / "triage.json").write_text(
        json.dumps(
            {{"failed": failed, "observed": len(a.children), "sites": a.state["sites"]}},
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    a.fail("campaign_incomplete", "not every site relaxed", details={{"failed": len(failed)}})


raise SystemExit(run.main())
'''

_CAMPAIGN_STEPS = ["aggregate", "characterize", "relax", "triage"]


def _publish_campaign(workspace: Workspace, root: Path) -> dict[str, object]:
    """Publish the campaign runner and return its job runner reference."""

    source = root / "campaign.py"
    root.mkdir(parents=True, exist_ok=True)
    source.write_text(_CAMPAIGN_RUNNER, encoding="utf-8")
    return workspace.publish_runner(source, name="campaign/run.py")


def _submit_campaign(
    workspace: Workspace,
    root: Path,
    parameters: dict[str, object],
    *,
    initial_step: str = "characterize",
) -> str:
    """Submit one campaign parent job built entirely through the SDK."""

    reference = _publish_campaign(workspace, root / "runners")
    payload = root / "parent"
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Defect campaign",
            workflow="tests.campaign",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="campaign",
            initial_step=initial_step,
            maximum_attempts_per_activation=1,
            parameters=parameters,
        ),
    )
    workspace.submit(payload, "project/campaign")
    return job.id


def _run(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)


def _state(workspace: Workspace, job_id: str) -> dict[str, Any]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return dict(workspace.read_state(marker))


def _workdir(workspace: Workspace, job_id: str) -> Path:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return workspace.payload_path(marker.placement, marker.job_key) / "run"


def test_a_dynamic_campaign_spawns_gathers_and_aggregates(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit_campaign(workspace, tmp_path / "source", {"sites": 3})
    _run(workspace)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    report = json.loads((_workdir(workspace, job_id) / "report.json").read_text(encoding="utf-8"))
    assert report["labels"] == ["site-0", "site-1", "site-2"]
    assert report["relaxed"] == ["0", "1", "2"]
    # Every child is a complete job with no payload of its own, keyed by the tag
    # its spawn label supplied.
    assert [key.split("--")[0] for key in report["keys"]] == ["site-0", "site-1", "site-2"]
    assert report["sites"] == 3

    children = [found for found in workspace.scan_markers() if found.job_key != marker.job_key]
    assert len(children) == 3 and all(child.kind == "succeeded" for child in children)
    for child in children:
        child_job = workspace.load_job(child)
        assert child_job.runner_source == "workspace" and child_job.runner_path.as_posix() == "campaign/run.py"
        assert child_job.workflow == "tests.campaign"
        assert not (workspace.payload_path(child.placement, child.job_key) / "campaign").exists()

    # The step set of the runner is recorded once, from the first outcome the job
    # published, and carried forward by later ones.
    assert _state(workspace, job_id)["runner_steps"] == _CAMPAIGN_STEPS


def test_a_failing_child_is_observed_and_triaged_by_a_later_step(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit_campaign(workspace, tmp_path / "source", {"sites": 3, "failing": [1]})
    _run(workspace)

    state = _state(workspace, job_id)
    assert state["reason"] == "declared_failure"
    assert state["failure"] == {
        "code": "campaign_incomplete",
        "message": "not every site relaxed",
        "details": {"failed": 1},
    }
    workdir = _workdir(workspace, job_id)
    report = json.loads((workdir / "report.json").read_text(encoding="utf-8"))
    assert report["labels"] == ["site-0", "site-2"]
    assert report["relaxed"] == ["0", "2"]
    assert report["failed"] == ["site-1"] and report["codes"] == ["relax.diverged"]
    # The gathering step decided at run time to branch into triage, and carried
    # what it learned in job state: a new activation observes no children.
    triage = json.loads((workdir / "triage.json").read_text(encoding="utf-8"))
    assert triage == {"failed": ["site-1"], "observed": 0, "sites": 3}


def test_an_impossible_join_advances_to_the_step_it_names(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit_campaign(
        workspace,
        tmp_path / "source",
        {"sites": 3, "failing": [1], "when": "all_succeeded"},
    )
    _run(workspace)

    state = _state(workspace, job_id)
    assert state["reason"] == "declared_failure"
    assert state["failure"]["code"] == "campaign_incomplete"
    workdir = _workdir(workspace, job_id)
    # The join became impossible the moment one child failed, so triage ran
    # without an aggregate step and observed the children as they then were.
    assert not (workdir / "report.json").exists()
    triage = json.loads((workdir / "triage.json").read_text(encoding="utf-8"))
    assert triage["failed"] == ["site-1"] and triage["observed"] == 3 and triage["sites"] == 3


def test_an_unregistered_step_fails_the_job_with_unknown_step(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit_campaign(workspace, tmp_path / "source", {"sites": 1}, initial_step="charaterize")
    _run(workspace)

    state = _state(workspace, job_id)
    assert state["failure"]["code"] == "unknown_step"
    assert "registered steps: aggregate, characterize, relax, triage" in state["failure"]["message"]
    assert state["failure"].get("retryable", False) is False


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    environment: dict[str, object] | None = None,
    settings: dict[str, object] | None = None,
    data_generation: int | None = None,
    children: list[dict[str, object]] | None = None,
    runner: Runner | None = None,
    name: str = "payload",
    resources: dict[str, int] | None = None,
    step_resources: dict[str, dict[str, int]] | None = None,
) -> Attempt:
    """Bind one attempt of a fabricated job, without a manager."""

    payload = tmp_path / name
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.sdk",
            runner_path="files/runner",
            initial_step=step,
            data_mode="none" if data_generation is None else "transactional",
            parameters=parameters or {},
            environment=environment or {},
            resources=resources or {},
            step_resources=step_resources or {},
        ),
    )
    control = payload / f"attempts/{uuid.uuid4()}"
    control.mkdir(parents=True)
    workdir = payload / "run"
    workdir.mkdir()
    context_json = json.dumps(
        {
            "format": "httk-workflow-attempt-context",
            "format_version": 2,
            "workspace_id": str(uuid.uuid4()),
            "job_id": str(uuid.uuid4()),
            "job_key": f"fabricated--{uuid.uuid4()}",
            "placement": "project/fabricated",
            "payload": str(payload),
            "step": step,
            "activation_id": str(uuid.uuid4()),
            "attempt_id": str(uuid.uuid4()),
            "data_generation": data_generation,
            "children": children or [],
            "settings": settings or {},
        }
    )
    attempt_environment = {
        "HTTK_WORKFLOW_CONTEXT": context_json,
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(payload),
        "HTTK_WORKFLOW_WORKDIR": str(workdir),
        "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
        "HTTK_WORKFLOW_STEP": step,
    }
    if data_generation is not None:
        attempt_environment["HTTK_WORKFLOW_DATA_DIR"] = str(payload / "data")
    return Attempt.initialize(attempt_environment, runner=runner)


def test_child_spec_inherits_job_resource_requirements(tmp_path: Path) -> None:
    attempt = _attempt(
        tmp_path,
        step="start",
        resources={"procs": 2},
        step_resources={"start": {"mem": 1024}},
    )
    child = ChildSpec(step="start", runner=RunnerRef.workspace("runner", "a" * 64))
    spec = child._job_spec(attempt.job, "child")
    assert spec.resources == {"procs": 2}
    assert spec.step_resources == {"start": {"mem": 1024}}


def test_outcome_resource_requirements_are_action_scoped(tmp_path: Path) -> None:
    advance = _attempt(tmp_path / "advance", step="start")
    advance._require_draft().publish("advance", next_step="next", resources={"procs": 4})
    assert _published(advance)["resources"] == {"procs": 4}
    for action in ("succeed", "fail", "retry", "pause"):
        fresh = _attempt(tmp_path / action, step="start")
        with pytest.raises(ValueError, match="does not accept resources"):
            fresh._require_draft().publish(action, resources={"procs": 1})


def test_attempt_environment_resolves_declared_layers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    attempt = _attempt(
        tmp_path,
        step="start",
        environment={
            "declared": {
                "command": {"type": "string", "setting": "tool.command", "default": "manifest"},
                "retries": {"type": "integer"},
                "path": {"type": "string", "setting": "tool.path"},
            },
            "overrides": {"command": "override"},
        },
        settings={"tool.command": "workspace", "retries": 2},
    )
    monkeypatch.setenv("HTTK_TOOL_COMMAND", "env")
    assert attempt.environment("command") == "override"
    assert attempt.environment("retries") == 2
    monkeypatch.delenv("HTTK_TOOL_COMMAND")
    monkeypatch.setenv("HTTK_TOOL_PATH", "env")
    assert attempt.environment("path") == "env"
    with pytest.raises(KeyError, match="not declared"):
        attempt.environment("missing")


def test_runner_gates_all_declared_environment_and_records_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Runner("tests.environment.gate")

    @run.step
    def start(a: Attempt) -> None:
        (a.workdir / "ran").write_text("yes", encoding="utf-8")
        a.succeed()

    monkeypatch.setenv("HTTK_TOOL_VARIABLE", "env")
    attempt = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={
            "declared": {
                "override": {"type": "string", "setting": "tool.override"},
                "variable": {"type": "string", "setting": "tool.variable"},
                "setting": {"type": "integer", "setting": "tool.setting"},
                "defaulted": {"type": "string", "default": "manifest"},
            },
            "overrides": {"override": "job"},
        },
        settings={"tool.setting": 7},
    )

    assert _main(run, attempt) == 0
    assert (attempt.workdir / "ran").read_text(encoding="utf-8") == "yes"
    observed = attempt.declaration("environment")
    assert observed == {
        "format": "httk-workflow-environment-resolution",
        "format_version": 2,
        "values": {
            "defaulted": {"value": "manifest", "source": "default"},
            "override": {"value": "job", "source": "override"},
            "setting": {"value": 7, "source": "workspace-setting"},
            "variable": {"value": "env", "source": "environment-variable"},
        },
    }
    log = (attempt.payload / "logs" / "runlog.jsonl").read_text(encoding="utf-8")
    assert log.count("parameters are in job.json; environment resolved as") == 1


def test_environment_gate_does_not_create_a_fresh_workdir_before_the_step(tmp_path: Path) -> None:
    run = Runner("tests.environment.fresh")
    seen: list[bool] = []

    @run.step
    def start(a: Attempt) -> None:
        seen.append(a.workdir.exists())
        a.workdir.mkdir()
        a.succeed()

    attempt = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={"declared": {"value": {"type": "string", "default": "manifest"}}, "overrides": {}},
    )
    attempt.workdir.rmdir()

    assert _main(run, attempt) == 0
    assert seen == [False]


def test_unresolved_environment_fails_before_python_step(tmp_path: Path) -> None:
    run = Runner("tests.environment.unresolved")
    run.step(name="start")(lambda a: (a.workdir / "ran").write_text("yes", encoding="utf-8"))
    attempt = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={"declared": {"needed": {"type": "string", "setting": "tool.needed"}}, "overrides": {}},
    )

    assert _main(run, attempt) == 0
    failure = _published(attempt)["failure"]
    assert failure["code"] == "environment_unresolved"
    assert failure.get("retryable", False) is False
    assert failure["details"] == {"unresolved": ["needed"]}
    assert not (attempt.workdir / "ran").exists()


def test_environment_resolution_snapshot_and_unchanged_activation_are_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Runner("tests.environment.snapshot")
    run.step(name="start")(lambda a: a.succeed())
    monkeypatch.setenv("HTTK_TOOL_VALUE", "first")
    first = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={"declared": {"value": {"type": "string", "setting": "tool.value"}}, "overrides": {}},
        name="first",
    )
    assert _main(run, first) == 0
    monkeypatch.setenv("HTTK_TOOL_VALUE", "first")
    assert first.environment("value") == "first"

    second = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={"declared": {"value": {"type": "string", "setting": "tool.value"}}, "overrides": {}},
        name="second",
    )
    # Model the same payload's previously recorded declaration on a fresh
    # activation: the unchanged result must not append another declaration note.
    document = first.declaration("environment")
    assert document is not None
    second.declare("environment", document)
    assert _main(run, second) == 0
    assert second.declaration("environment") == first.declaration("environment")
    assert not (second.payload / "logs" / "runlog.jsonl").exists()


def test_environment_resolution_distinguishes_json_number_and_boolean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("HTTK_TOOL_VALUE", raising=False)
    run = Runner("tests.environment.json-types")
    run.step(name="start")(lambda a: a.succeed())
    first = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={"declared": {"value": {"setting": "tool.value"}}, "overrides": {}},
        settings={"tool.value": 1},
        name="number",
    )
    assert _main(run, first) == 0
    document = first.declaration("environment")
    assert document is not None

    second = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={"declared": {"value": {"setting": "tool.value"}}, "overrides": {}},
        settings={"tool.value": True},
        name="boolean",
    )
    second.declare("environment", document)
    assert _main(run, second) == 0
    updated = second.declaration("environment")
    assert updated is not None
    values = updated["values"]
    assert isinstance(values, Mapping)
    value = values["value"]
    assert isinstance(value, Mapping)
    assert value["value"] is True
    assert (second.payload / "logs" / "runlog.jsonl").is_file()


@pytest.mark.timing
def test_manager_waits_for_deferred_environment_log_before_commit(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = tmp_path / "payload"
    runner = payload / "runner.py"
    runner.parent.mkdir(parents=True)
    runner.write_text(
        f'''#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, {_SRC!r})
from httk.workflow import Runner

run = Runner("tests.environment.race")

@run.step
def start(a):
    a.succeed()
    time.sleep(0.2)

raise SystemExit(run.main())
''',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Environment race",
            workflow="tests.environment.race",
            runner_path="runner.py",
            environment={
                "declared": {"value": {"type": "string", "default": "manifest"}},
                "overrides": {},
            },
        ),
    )
    workspace.submit(payload, "project/environment-race")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30)

    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "run"
    runlog = (workdir.parent / "logs" / "runlog.jsonl").read_text(encoding="utf-8")
    assert "parameters are in job.json; environment resolved as" in runlog


@pytest.mark.timing
def test_peer_manager_defers_a_live_environment_log_writer(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = tmp_path / "payload"
    runner = payload / "runner.py"
    runner.parent.mkdir(parents=True)
    runner.write_text(
        f'''#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, {_SRC!r})
from httk.workflow import Runner

run = Runner("tests.environment.peer")

@run.step
def start(a):
    a.succeed()
    time.sleep(0.4)

raise SystemExit(run.main())
''',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Environment peer race",
            workflow="tests.environment.peer",
            runner_path="runner.py",
            environment={"declared": {"value": {"type": "string", "default": "manifest"}}, "overrides": {}},
        ),
    )
    workspace.submit(payload, "project/environment-peer")
    with (
        TaskManager(workspace, heartbeat_interval=0.01) as owner,
        TaskManager(workspace, heartbeat_interval=0.01) as peer,
    ):
        deadline = time.monotonic() + 30.0
        while not owner._running and time.monotonic() < deadline:
            owner.tick()
        assert owner._running
        running = workspace.find_marker_by_id(job.id)
        assert running is not None and running.kind == "running"
        state = workspace.read_state(running)
        control = workspace.payload_path(running.placement, running.job_key) / str(state["attempt_control"])
        while not (control / "outcome.ready").is_dir() and time.monotonic() < deadline:
            owner.heartbeat()
            time.sleep(0.01)
        assert (control / "outcome.ready").is_dir()

        peer.tick()
        recorded = json.loads((control / ".httk-environment-resolution.json").read_text(encoding="utf-8"))
        assert recorded["log_pending"] is True
        assert isinstance(recorded["log_deadline"], (int, float))
        assert "log_absent" not in recorded
        still_running = workspace.find_marker_by_id(job.id)
        assert still_running is not None and still_running.kind == "running"

        final: Marker | None = None
        while time.monotonic() < deadline:
            owner.tick()
            final = workspace.find_marker_by_id(job.id)
            if final is not None and final.kind == "succeeded":
                break
            time.sleep(0.01)
        assert final is not None and final.kind == "succeeded"

    workdir = workspace.payload_path(final.placement, final.job_key) / "run"
    assert "parameters are in job.json; environment resolved as" in (
        workdir.parent / "logs" / "runlog.jsonl"
    ).read_text(encoding="utf-8")


@pytest.mark.timing
def test_environment_log_grace_starts_when_outcome_is_published(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = tmp_path / "payload"
    runner = payload / "runner.py"
    runner.parent.mkdir(parents=True)
    runner.write_text(
        f'''#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, {_SRC!r})
from httk.workflow import Runner

run = Runner("tests.environment.late-publish")

@run.step
def start(a):
    time.sleep(1.2)
    a.succeed()
    time.sleep(0.2)

raise SystemExit(run.main())
''',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Late environment publish",
            workflow="tests.environment.late-publish",
            runner_path="runner.py",
            environment={"declared": {"value": {"type": "string", "default": "manifest"}}, "overrides": {}},
        ),
    )
    workspace.submit(payload, "project/environment-late-publish")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30)

    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "run"
    assert "parameters are in job.json; environment resolved as" in (
        workdir.parent / "logs" / "runlog.jsonl"
    ).read_text(encoding="utf-8")


def test_bad_workspace_environment_type_fails_at_attempt_start(tmp_path: Path) -> None:
    run = Runner("tests.environment.type")
    run.step(name="start")(lambda a: a.succeed())
    attempt = _attempt(
        tmp_path,
        step="start",
        runner=run,
        environment={"declared": {"count": {"type": "integer", "setting": "tool.count"}}, "overrides": {}},
        settings={"tool.count": "not-an-integer"},
    )

    assert _main(run, attempt) == 0
    failure = _published(attempt)["failure"]
    assert failure["code"] == "environment_unresolved"
    assert failure["details"] == {"unresolved": ["count"]}
    assert "workspace setting 'tool.count'" in failure["message"]


def _run_manager_environment_job(
    root: Path,
    *,
    name: str = "timeout",
    setting: str = "httk_v1.timeout",
    environment_type: str = "integer",
    setting_value: object = 60,
    workspace_settings: dict[str, object] | None = None,
    environment_default: object | None = None,
) -> str:
    workspace = Workspace.initialize(root / "workspace")
    for workspace_setting, value in (workspace_settings or {setting: setting_value}).items():
        workspace.set_setting(workspace_setting, value)
    payload = root / "payload"
    runner = payload / "runner.py"
    runner.parent.mkdir(parents=True)
    runner.write_text(
        f'''#!/usr/bin/env python3
import sys
sys.path.insert(0, {_SRC!r})
from httk.workflow import Attempt

attempt = Attempt.initialize()
try:
    value = attempt.environment({name!r})
except ValueError as exc:
    (attempt.workdir / "error").write_text(str(exc), encoding="utf-8")
else:
    (attempt.workdir / "value").write_text(f"{{type(value).__name__}}:{{value}}", encoding="utf-8")
attempt.succeed()
''',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    metadata: dict[str, object] = {"type": environment_type, "setting": setting}
    if environment_default is not None:
        metadata["default"] = environment_default
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Environment manager job",
            workflow="tests.environment",
            runner_path="runner.py",
            environment={
                "declared": {name: metadata},
                "overrides": {},
            },
        ),
    )
    workspace.submit(payload, "project/environment")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30)
    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    run = workspace.payload_path(marker.placement, marker.job_key) / "run"
    if (run / "error").is_file():
        return (run / "error").read_text()
    return (run / "value").read_text()


def test_attempt_environment_decodes_real_manager_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HTTK_HTTK_V1_TIMEOUT", raising=False)
    assert _run_manager_environment_job(tmp_path / "workspace-setting") == "int:60"
    monkeypatch.setenv("HTTK_HTTK_V1_TIMEOUT", "60")
    assert _run_manager_environment_job(tmp_path / "environment-variable") == "int:60"


def test_manager_does_not_mask_string_type_errors_with_synthetic_exports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("HTTK_TOOL_LABEL", raising=False)
    error = _run_manager_environment_job(
        tmp_path / "string-setting",
        name="label",
        setting="tool.label",
        environment_type="string",
    )
    assert "workflow environment 'label' from workspace setting 'tool.label'" in error
    assert "declared type 'string'" in error


def test_manager_filters_colliding_derived_setting_exports(tmp_path: Path) -> None:
    value = _run_manager_environment_job(
        tmp_path,
        name="label",
        setting="tool_value",
        environment_type="string",
        workspace_settings={"tool.value": 7},
        environment_default="fallback",
    )
    assert value == "str:fallback"


def test_attempt_environment_rejects_bad_typed_external_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    attempt = _attempt(
        tmp_path,
        step="start",
        environment={"declared": {"timeout": {"type": "integer"}}, "overrides": {}},
    )
    monkeypatch.setenv("HTTK_TIMEOUT", "not-an-integer")
    with pytest.raises(ValueError, match="timeout.*environment variable.*integer"):
        attempt.environment("timeout")

    string_attempt = _attempt(
        tmp_path / "string",
        step="start",
        environment={"declared": {"label": {"type": "string"}}, "overrides": {}},
    )
    monkeypatch.setenv("HTTK_LABEL", "60")
    assert string_attempt.environment("label") == "60"


def _published(attempt: Attempt) -> dict[str, Any]:
    return json.loads((attempt.control / "outcome.ready" / "outcome.json").read_text(encoding="utf-8"))


def test_fail_can_set_terminal_priority(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="start")
    attempt.fail("tests.failed", "failed at elevated priority", priority=900)
    assert _published(attempt)["priority"] == 900


def test_gather_can_set_join_priority(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="start")
    child = tmp_path / "child"
    (child / "files").mkdir(parents=True)
    (child / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        child, JobSpec(name="Child", workflow="tests.sdk", runner_path="files/runner", initial_step="start")
    )
    attempt.spawn(child, label="child")
    attempt.gather("start", priority=900)
    assert _published(attempt)["priority"] == 900


def _main(runner: Runner, attempt: Attempt) -> int:
    """Dispatch one prepared attempt exactly as :meth:`Runner.main` does."""

    if not attempt._prepare_environment():
        return 0
    handler = runner._steps.get(attempt.step)
    if handler is None:
        registered = ", ".join(sorted(runner.steps)) or "none"
        attempt.fail(
            "unknown_step",
            f"step {attempt.step!r} is not implemented by the {runner.workflow} runner; registered steps: {registered}",
        )
        return 0
    try:
        handler(attempt)
    except BaseException as exception:
        attempt._abort(exception)
        raise
    attempt._finish_environment_log()
    if not attempt.published:
        attempt.fail("no_outcome", f"step {attempt.step!r} finished without publishing an outcome")
    return 0


def test_describe_mode_prints_the_step_set_and_touches_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run = Runner("tests.describe")
    run.step(name="collect")(lambda a: a.succeed())
    run.step(name="prepare")(lambda a: a.succeed())
    monkeypatch.chdir(tmp_path)
    for name in ("HTTK_WORKFLOW_CONTEXT", "HTTK_WORKFLOW_CONTROL_DIR", "HTTK_WORKFLOW_WORKDIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HTTK_WORKFLOW_DESCRIBE", "1")

    assert run.main([]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "format": "httk-workflow-runner-description",
        "format_version": 2,
        "workflow": "tests.describe",
        "steps": ["collect", "prepare"],
    }
    # Describing a runner is a pure read: no attempt is bound and nothing is
    # created, which is why it needs no attempt context at all.
    assert list(tmp_path.iterdir()) == []

    monkeypatch.delenv("HTTK_WORKFLOW_DESCRIBE")
    assert run.main(["--describe"]) == 0
    assert json.loads(capsys.readouterr().out)["steps"] == ["collect", "prepare"]


def test_runner_creation_parameters_are_immutable_and_optional() -> None:
    plain = Runner("tests.plain")
    assert plain.inputs == {}
    assert "inputs" not in plain.description()

    run = Runner("tests.parameters", inputs={"structure": "POSCAR", "future": None})
    assert run.inputs == {"structure": "POSCAR", "future": None}
    with pytest.raises(TypeError):
        run.inputs["other"] = "x"  # type: ignore[index]
    assert run.description()["inputs"] == {"structure": "POSCAR", "future": None}
    with pytest.raises(ValueError, match="nonempty"):
        Runner("tests.invalid", inputs={"": "POSCAR"})
    with pytest.raises(ValueError, match="nonempty string or null"):
        Runner("tests.invalid", inputs={"x": 7})  # type: ignore[dict-item]


def test_registration_refuses_a_duplicate_step_name() -> None:
    run = Runner("tests.duplicates")

    @run.step
    def relax(a: Attempt) -> None:
        a.succeed()

    with pytest.raises(ValueError, match="already registered"):
        run.step(name="relax")(lambda a: a.succeed())
    assert run.steps == frozenset({"relax"})


def test_registration_refuses_a_duplicate_instantiate_hook() -> None:
    run = Runner("tests.instantiate")

    @run.instantiate
    def hook(context: object) -> None:
        del context

    assert run.has_instantiate
    assert run.description()["instantiate"] is True
    with pytest.raises(ValueError, match="tests.instantiate"):
        run.instantiate(lambda context: None)


def test_a_step_that_publishes_nothing_is_reported_as_no_outcome(tmp_path: Path) -> None:
    run = Runner("tests.sdk")
    run.step(name="silent")(lambda a: None)
    attempt = _attempt(tmp_path, step="silent", runner=run)

    assert _main(run, attempt) == 0
    outcome = _published(attempt)
    assert outcome["action"] == "fail"
    assert outcome["failure"] == {
        "code": "no_outcome",
        "message": "step 'silent' finished without publishing an outcome",
    }
    assert outcome["runner_steps"] == ["silent"]


def test_an_unknown_step_names_the_registered_steps(tmp_path: Path) -> None:
    run = Runner("tests.sdk")
    run.step(name="relax")(lambda a: a.succeed())
    run.step(name="collect")(lambda a: a.succeed())
    attempt = _attempt(tmp_path, step="realx", runner=run)

    assert _main(run, attempt) == 0
    failure = _published(attempt)["failure"]
    assert failure["code"] == "unknown_step"
    assert "registered steps: collect, relax" in str(failure["message"])
    assert "retryable" not in failure


def test_a_second_terminal_publication_raises_and_keeps_the_first(tmp_path: Path) -> None:
    run = Runner("tests.sdk")

    @run.step(name="double")
    def double(a: Attempt) -> None:
        a.succeed()
        a.advance("double")

    attempt = _attempt(tmp_path, step="double", runner=run)
    with pytest.raises(RuntimeError, match="already published its succeed outcome"):
        _main(run, attempt)

    assert _published(attempt)["action"] == "succeed"
    assert not list(attempt.control.glob("outcome.tmp.*"))
    breadcrumb = json.loads((attempt.control / "error.json").read_text(encoding="utf-8"))
    assert breadcrumb["step"] == "double" and breadcrumb["exception"] == "RuntimeError"


def test_a_step_target_that_is_not_registered_raises_in_the_step(tmp_path: Path) -> None:
    run = Runner("tests.sdk")
    run.step(name="relax")(lambda a: a.succeed())
    run.step(name="collect")(lambda a: a.succeed())
    attempt = _attempt(tmp_path, step="relax", runner=run)

    with pytest.raises(ValueError, match="registered steps: collect, relax"):
        attempt.advance("colect")
    with pytest.raises(ValueError, match="gather target 'aggregate'"):
        attempt.gather("aggregate")
    with pytest.raises(ValueError, match="spawned child step 'relx'"):
        attempt.spawn(ChildSpec(step="relx"), label="one")
    # Nothing was published, and no draft leaked from the refused calls: a spawn
    # that names an unknown step is refused before the draft is created.
    assert not (attempt.control / "outcome.ready").exists()
    assert not list(attempt.control.glob("outcome.tmp.*"))
    assert attempt._draft is None
    assert attempt.advance("collect", state={"decided": True}).name == "outcome.ready"
    assert attempt.state["decided"] is True


def test_an_uncaught_exception_leaves_a_breadcrumb_and_no_draft(tmp_path: Path) -> None:
    run = Runner("tests.sdk")

    @run.step(name="explode")
    def explode(a: Attempt) -> None:
        a.put(a.workdir / "energy.json", "results/energy.json")
        raise KeyError("missing parameter")

    attempt = _attempt(tmp_path, step="explode", data_generation=0, runner=run)
    (attempt.workdir / "energy.json").write_text("{}", encoding="utf-8")

    with pytest.raises(KeyError):
        _main(run, attempt)
    assert not (attempt.control / "outcome.ready").exists()
    assert not list(attempt.control.glob("outcome.tmp.*"))
    breadcrumb = json.loads((attempt.control / "error.json").read_text(encoding="utf-8"))
    assert breadcrumb["format"] == "httk-workflow-runner-error"
    assert breadcrumb["exception"] == "KeyError" and breadcrumb["step"] == "explode"
    assert "raise KeyError" in breadcrumb["traceback"]


def test_data_operation_identifiers_are_generated_in_call_order(tmp_path: Path) -> None:
    def publish(attempt: Attempt) -> list[dict[str, object]]:
        (attempt.workdir / "energy.json").write_text("{}", encoding="utf-8")
        (attempt.workdir / "bundle").mkdir()
        (attempt.workdir / "bundle" / "log.txt").write_text("done\n", encoding="utf-8")
        assert attempt.put(attempt.workdir / "energy.json", "results/energy.json") == "op-0001"
        assert attempt.put(attempt.workdir / "bundle", "results/bundle") == "op-0002"
        assert attempt.remove("scratch", missing_ok=True) == "op-0003"
        attempt.succeed()
        manifest = json.loads(
            (attempt.control / "outcome.ready" / "transaction" / "manifest.json").read_text(encoding="utf-8")
        )
        return list(manifest["operations"])

    first = publish(_attempt(tmp_path, step="collect", data_generation=0, name="first"))
    second = publish(_attempt(tmp_path, step="collect", data_generation=0, name="second"))
    assert [item["id"] for item in first] == ["op-0001", "op-0002", "op-0003"]
    assert [item["op"] for item in first] == ["put-file", "put-tree", "remove"]
    # Replaying the same step produces exactly the same manifest, which is what
    # makes an interrupted attempt safe to repeat.
    assert first == second


def test_data_operations_refuse_a_job_without_transactional_data(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="collect")
    with pytest.raises(ValueError, match="data.mode none"):
        attempt.remove("results")
    assert not list(attempt.control.glob("outcome.tmp.*"))


def test_job_inputs_round_trip_and_are_bounded(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="relax", parameters={"encut": 520, "species": ["Si", "O"], "spin": None})
    assert attempt.parameter("encut") == 520
    assert attempt.parameter("species") == ["Si", "O"]
    assert attempt.parameter("spin") is None
    assert attempt.parameter("missing", "fallback") == "fallback"
    with pytest.raises(KeyError, match="defined parameters: encut, species, spin"):
        attempt.parameter("missing")
    assert attempt.parameters == {"encut": 520, "species": ["Si", "O"], "spin": None}

    # The inputs of a job are part of job.json and therefore of its digest.
    stored = JobDefinition.from_path(attempt.payload / "job.json")
    assert stored.parameters == attempt.parameters
    assert stored.digest == JobDefinition.from_path(attempt.payload / "job.json").digest

    oversized = JobSpec(
        name="Too much",
        workflow="tests.sdk",
        runner_path="files/runner",
        parameters={"blob": "x" * 300000},
    )
    with pytest.raises(FormatError, match="exceeds the 262144-byte limit"):
        oversized.as_mapping()
    with pytest.raises(FormatError, match="keys must be nonempty strings"):
        JobDefinition.from_mapping({**stored.raw, "parameters": {"": 1}})


def test_a_prepared_payload_child_can_be_spawned_by_path(tmp_path: Path) -> None:
    run = Runner("tests.sdk")
    run.step(name="branch")(lambda a: a.succeed())
    attempt = _attempt(tmp_path, step="branch", runner=run)
    child = attempt.workdir / "child"
    (child / "files").mkdir(parents=True)
    (child / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        child,
        JobSpec(name="Child", workflow="tests.sdk", runner_path="files/runner", tag="child", initial_step="branch"),
    )

    reference = attempt.spawn(child, label="prepared")
    assert reference.placement_hint == attempt.context.placement
    draft = next(iter(attempt.control.glob("outcome.tmp.*")))
    spawn = json.loads((draft / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert [entry["label"] for entry in spawn["children"]] == ["prepared"]
    assert (draft / "children" / "jobs" / reference.job_key / "files" / "runner").is_file()


def test_inheriting_a_payload_runner_is_refused_with_a_usable_message(tmp_path: Path) -> None:
    run = Runner("tests.sdk")
    run.step(name="branch")(lambda a: a.succeed())
    attempt = _attempt(tmp_path, step="branch", runner=run)
    with pytest.raises(ValueError, match="publish_runner"):
        attempt.spawn(ChildSpec(step="branch"), label="child")
    # A child that names a shared runner explicitly needs no payload at all.
    reference = attempt.spawn(
        ChildSpec(step="branch", runner=RunnerRef.workspace("campaign/run.py", "a" * 64)),
        label="shared",
    )
    draft = next(iter(attempt.control.glob("outcome.tmp.*")))
    child = JobDefinition.from_path(draft / "children" / "jobs" / reference.job_key / "job.json")
    assert child.runner_source == "workspace" and child.runner_sha256 == "a" * 64
    assert list((draft / "children" / "jobs" / reference.job_key).iterdir()) == [
        draft / "children" / "jobs" / reference.job_key / "job.json"
    ]


_STATE_RUNNER = f"""#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.state")


@run.step
def count(a):
    visits = int(a.state.get("visits", 0)) + 1
    a.state["visits"] = visits
    (a.workdir / "visit.txt").write_text(str(visits), encoding="utf-8")
    if visits < 3:
        a.retry("counting again")
    else:
        a.advance("finish", state={{"counted": True}})


@run.step
def finish(a):
    (a.workdir / "final.json").write_text(
        '{{"visits": %d, "counted": %s, "keys": "%s"}}'
        % (a.state["visits"], str(a.state["counted"]).lower(), ",".join(sorted(a.state))),
        encoding="utf-8",
    )
    a.succeed()


raise SystemExit(run.main())
"""


def test_job_state_survives_retries_advances_and_isolated_workdirs(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = tmp_path / "source" / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(_STATE_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Stateful",
            workflow="tests.state",
            runner_path="files/runner",
            tag="stateful",
            initial_step="count",
            workdir_mode="isolated",
            maximum_attempts_per_activation=3,
        ),
    )
    workspace.submit(payload, "project/stateful")
    _run(workspace)

    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    root = workspace.payload_path(marker.placement, marker.job_key)
    # Each attempt ran in its own workdir, so the count can only have survived in
    # the job state stored beside them.
    workdirs = sorted(item for item in root.glob("run.*") if item.is_dir())
    assert len(workdirs) == 4
    visits = [(item / "visit.txt").read_text(encoding="utf-8") for item in workdirs if (item / "visit.txt").is_file()]
    assert sorted(visits) == ["1", "2", "3"]
    finals = [item / "final.json" for item in workdirs if (item / "final.json").is_file()]
    assert len(finals) == 1
    final = json.loads(finals[0].read_text(encoding="utf-8"))
    assert final == {"visits": 3, "counted": True, "keys": "counted,visits"}
    assert json.loads((root / ".httk-job" / "state.json").read_text(encoding="utf-8")) == {
        "visits": 3,
        "counted": True,
    }


def test_job_state_is_a_dict_like_mapping(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="relax")
    assert dict(attempt.state) == {} and len(attempt.state) == 0
    attempt.state["energy"] = -12.5
    attempt.state.merge({"converged": True, "steps": [1, 2]})
    assert attempt.state["energy"] == -12.5
    assert sorted(attempt.state) == ["converged", "energy", "steps"]
    assert attempt.state.get("missing", "default") == "default"
    assert "converged" in attempt.state
    del attempt.state["converged"]
    assert attempt.state.delete("converged") is False
    with pytest.raises(KeyError):
        del attempt.state["converged"]
    with pytest.raises(ValueError, match="must be JSON"):
        attempt.state["bad"] = object()
    with pytest.raises(ValueError, match="nonempty strings"):
        attempt.state[""] = 1
    assert dict(attempt.state) == {"energy": -12.5, "steps": [1, 2]}
