"""Failure objects and joins: what a declared or malformed failure records, and how waiting parents resume.

A runner fails a job with a canonical failure object; anything else is the
runner's protocol error. A ``wait`` outcome records a join over children (new
ones, and children of earlier activations rejoined by reference); the parent
resumes when the join's condition is decided, with the observations of every
child, or fails with ``dependency_failure`` when it cannot be satisfied or a
child cannot be found.
"""

import json
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as h
from httk.workflow import Attempt, FormatError, Workspace, _kernel, _store
from httk.workflow.protocol import Failure, validate_failure
from httk.workflow.runtime_builders import ChildReference
from test_crash_injection import _all_jobs

#: A scriptable join runner. A job with ``parameters.child`` is a child: it succeeds, or fails with
#: ``child.broken`` for ``"fail"``. Any other job runs ``parameters.steps[<step>]``: it records the children its
#: context names (into ``run/events.json``), spawns ``spawn`` children (``{label, child, placement?}``), and then
#: waits with ``wait`` (``{condition, count?, next_step, on_impossible?, rejoin?: [labels]}``) over the new
#: children plus the rejoined ones, or else succeeds.
_JOIN_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, uuid

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job = json.loads((pathlib.Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / "job.json").read_text())
parameters = job["parameters"]
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="succeed")
draft = control / "outcome.tmp.x"
draft.mkdir()
if "child" in parameters:
    if parameters["child"] == "fail":
        outcome.update(action="fail", failure={"code": "child.broken", "message": "the child declared a failure"})
else:
    plan = parameters["steps"][context["step"]]
    events = json.loads(pathlib.Path("events.json").read_text()) if pathlib.Path("events.json").exists() else []
    events.append({"step": context["step"],
                   "children": sorted(({"label": c["label"], "kind": c["kind"]} for c in context["children"]),
                                      key=lambda c: c["label"])})
    pathlib.Path("events.json").write_text(json.dumps(events))
    entries, references = [], []
    for spec in plan.get("spawn", []):
        child_id, spawn_id, label = str(uuid.uuid4()), str(uuid.uuid4()), spec["label"]
        key = label + "--" + child_id
        placement = spec.get("placement", job["placement"] + "/" + label)
        child = dict(job, id=child_id, tag=label, name=label, placement=placement, parameters={"child": spec["child"]})
        child["parent"] = {
            "workspace_id": context["workspace_id"], "job_id": job["id"], "job_key": context["job_key"],
            "placement": job["placement"], "activation_id": context["activation_id"], "spawn_id": spawn_id,
        }
        (draft / "children" / "jobs" / key).mkdir(parents=True)
        (draft / "children" / "jobs" / key / "job.json").write_text(json.dumps(child))
        entries.append({"job_key": key, "label": label, "placement": placement, "spawn_id": spawn_id})
        references.append({"workspace_id": context["workspace_id"], "job_id": child_id, "job_key": key,
                           "placement_hint": placement})
    if entries:
        (draft / "children" / "spawn.json").write_text(
            json.dumps({"format": "httk-workflow-spawn", "format_version": 2, "children": entries})
        )
    wait = plan.get("wait")
    if wait:
        for label in wait.get("rejoin", []):
            (known,) = [c for c in context["children"] if c["label"] == label]
            references.append({"workspace_id": context["workspace_id"], "job_id": known["job_id"],
                               "job_key": known["job_key"], "placement_hint": known["placement"], "label": label})
        join = {"children": references, "condition": wait["condition"]}
        if "count" in wait:
            join["count"] = wait["count"]
        if "on_impossible" in wait:
            join["on_impossible"] = {"action": "advance", "next_step": wait["on_impossible"]}
        outcome.update(action="wait", next_step=wait["next_step"], join=join)
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def joins(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(
        ws,
        tmp_path / "joins",
        name="joins",
        manifest="",
        executables={"run": _JOIN_RUNNER},
    )


def _parent(ws: Workspace, installed: _store.Installed, steps: Mapping[str, object], **options: Any) -> _kernel.JobRef:
    first = next(iter(steps))
    return h.submit(
        ws, installed, {name: "join" for name in steps}, initial_step=first, parameters={"steps": steps}, **options
    )


def _failure_code(observation: Mapping[str, object]) -> object:
    failure = observation["failure"]
    assert isinstance(failure, Mapping)
    return failure["code"]


def _events(ref: _kernel.JobRef) -> list[dict[str, Any]]:
    return json.loads((ref.path / "run" / "events.json").read_text(encoding="utf-8"))


def test_validate_failure_accepts_only_the_canonical_shape() -> None:
    failure = validate_failure(
        {
            "code": "vasp.nonconvergent",
            "message": "electronic minimization did not converge",
            "details": {"iterations": 61},
            "retryable": True,
        }
    )
    assert failure == Failure(
        "vasp.nonconvergent", "electronic minimization did not converge", {"iterations": 61}, True
    )
    assert failure.as_mapping() == {
        "code": "vasp.nonconvergent",
        "message": "electronic minimization did not converge",
        "details": {"iterations": 61},
        "retryable": True,
    }
    assert Failure("timeout", "ran out of time").as_mapping() == {"code": "timeout", "message": "ran out of time"}
    for rejected in (
        {"class": "process_failure", "summary": "old python spelling"},
        {"code": "process_failure", "summary": "old bash spelling"},
        {"code": "process_failure", "message": "extra member", "exit_status": 9},
        {"code": "process failure", "message": "whitespace in code"},
        {"code": "", "message": "empty code"},
        {"code": "process_failure"},
        {"code": "process_failure", "message": "bad details", "details": "text"},
        {"code": "process_failure", "message": "bad retryable", "retryable": "yes"},
        "not an object",
    ):
        with pytest.raises(FormatError):
            validate_failure(rejected)


@pytest.mark.slow
def test_a_declared_failure_reaches_the_failed_state(ws: Workspace, tmp_path: Path) -> None:
    installed = h.install(ws, tmp_path / "demo")
    submitted = h.submit(ws, installed, {"start": "fail:vasp.nonconvergent"})
    h.run(ws)
    failed = h.find(ws, submitted.job_id)
    assert failed.state == "failed"
    doc = h.state_of(failed)
    # The runner's failure object is recorded exactly, details and all.
    assert doc.failure == {"code": "vasp.nonconvergent", "message": "did not converge", "details": {"cycles": 3}}
    assert [item["code"] for item in doc.failure_history] == ["vasp.nonconvergent"]
    assert h.events(failed)[-2:] == ["failed", "released"]


_LEGACY_SHAPE_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
draft = control / "outcome.tmp.x"
draft.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="fail",
               failure={"class": "declared_failure", "summary": "an outdated failure spelling"})
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.mark.slow
def test_malformed_published_failure_becomes_a_protocol_error(ws: Workspace, tmp_path: Path) -> None:
    installed = h.install(ws, tmp_path / "legacy", executables={"run": _LEGACY_SHAPE_RUNNER})
    submitted = h.submit(ws, installed, {"start": "fail"})
    h.run(ws)
    failed = h.find(ws, submitted.job_id)
    assert failed.state == "failed"
    failure = h.state_of(failed).failure
    assert failure is not None and failure["code"] == "protocol_error"
    assert "malformed failure object" in str(failure["message"])


@pytest.mark.slow
def test_gather_registers_the_children_it_joins_on(ws: Workspace, joins: _store.Installed) -> None:
    steps = {
        "start": {
            "spawn": [{"label": "only", "child": "succeed", "placement": "project/children"}],
            "wait": {"condition": "all_succeeded", "next_step": "gather"},
        },
        "gather": {},
    }
    parent = _parent(ws, joins, steps)
    h.run(ws)
    done = h.find(ws, parent.job_id)
    assert done.state == "succeeded"
    (child,) = [ref for ref in _all_jobs(ws) if ref.job_id != parent.job_id]
    assert child.state == "succeeded" and child.placement is not None
    assert child.placement.as_posix() == "project/children"
    doc = h.state_of(done)
    assert [(entry["label"], entry["job_id"]) for entry in doc.children] == [("only", child.job_id)]
    assert _events(done)[1] == {"step": "gather", "children": [{"label": "only", "kind": "succeeded"}]}


def _in_process_attempt(tmp_path: Path, *, label: str | None = None) -> Attempt:
    control = tmp_path / "control"
    control.mkdir()
    child_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    context = {
        "format": "httk-workflow-attempt-context",
        "durable": False,
        "deadline": None,
        "settings": {},
        "format_version": 2,
        "workspace_id": str(uuid.uuid4()),
        "job_id": job_id,
        "job_key": f"job--{job_id}",
        "placement": "project/a",
        "payload": str(tmp_path / "job"),
        "step": "branch",
        "activation_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "children": []
        if label is None
        else [
            {
                "label": label,
                "job_id": child_id,
                "job_key": f"child--{child_id}",
                "kind": "succeeded",
                "placement": "project/a",
                "payload_path": f"project/a/child--{child_id}",
            }
        ],
    }
    for directory in ("run", "job", "job/data"):
        (tmp_path / directory).mkdir()
    return Attempt.initialize(
        {
            "HTTK_WORKFLOW_CONTEXT": json.dumps(context),
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(tmp_path / "job"),
            "HTTK_WORKFLOW_WORKDIR": str(tmp_path / "run"),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
            "HTTK_WORKFLOW_DATA_DIR": str(tmp_path / "job" / "data"),
        }
    )


def test_gather_refuses_a_join_over_no_children(tmp_path: Path) -> None:
    # A join is resolvable only for children the publishing bundle registers or rejoins, so a gather without
    # either can never become work.
    attempt = _in_process_attempt(tmp_path)
    with pytest.raises(ValueError, match="neither was provided"):
        attempt.gather("aggregate")
    assert not (tmp_path / "control" / "outcome.ready").exists()


def test_gather_rejoin_unknown_label_raises_in_process(tmp_path: Path) -> None:
    attempt = _in_process_attempt(tmp_path)
    with pytest.raises(ValueError, match="missing.*known labels: none"):
        attempt.gather("aggregate", rejoin=("missing",))


def test_gather_rejects_duplicate_rejoin_label_in_process(tmp_path: Path) -> None:
    attempt = _in_process_attempt(tmp_path, label="old-child")
    with pytest.raises(ValueError, match="duplicate join child label.*old-child"):
        attempt.gather("aggregate", rejoin=("old-child", "old-child"))


def test_gather_rejects_rejoin_label_used_by_a_new_child_in_process(tmp_path: Path) -> None:
    attempt = _in_process_attempt(tmp_path, label="old-child")
    draft = attempt._require_draft()
    draft._children.append(
        (
            ChildReference(
                attempt.context.workspace_id,
                str(uuid.uuid4()),
                f"new-child--{uuid.uuid4()}",
                "project/a",
            ),
            {"label": "old-child"},
        )
    )
    with pytest.raises(ValueError, match="duplicate join child label.*old-child"):
        attempt.gather("aggregate", rejoin=("old-child",))


@pytest.mark.slow
def test_any_terminal_wakes_for_a_failed_child(ws: Workspace, joins: _store.Installed) -> None:
    steps = {
        "start": {
            "spawn": [
                {"label": "a-child", "child": "fail"},
                # Placed where this manager does not serve, so it stays ready while the parent wakes.
                {"label": "b-child", "child": "succeed", "placement": "elsewhere/b"},
            ],
            "wait": {"condition": "any_terminal", "next_step": "gather"},
        },
        "gather": {},
    }
    parent = _parent(ws, joins, steps)
    h.run(ws, placement_prefixes=("project",))
    done = h.find(ws, parent.job_id)
    assert done.state == "succeeded"
    woken = _events(done)[1]
    assert woken["step"] == "gather"
    assert {item["label"]: item["kind"] for item in woken["children"]} == {"a-child": "failed", "b-child": "ready"}
    observations = {item["label"]: item for item in h.state_of(done).observations}
    assert _failure_code(observations["a-child"]) == "child.broken"
    assert observations["b-child"]["state"] == "ready"


@pytest.mark.slow
def test_gather_rejoins_children_from_an_earlier_activation(ws: Workspace, joins: _store.Installed) -> None:
    steps = {
        "start": {
            "spawn": [{"label": "a-child", "child": "succeed"}, {"label": "b-child", "child": "succeed"}],
            "wait": {"condition": "any_terminal", "next_step": "gather"},
        },
        # The first wake rejoins a-child (of the first activation) beside a new c-child.
        "gather": {
            "spawn": [{"label": "c-child", "child": "succeed"}],
            "wait": {"condition": "all_terminal", "next_step": "finish", "rejoin": ["a-child"]},
        },
        "finish": {},
    }
    parent = _parent(ws, joins, steps)
    h.run(ws)
    done = h.find(ws, parent.job_id)
    assert done.state == "succeeded"
    events = _events(done)
    assert [item["step"] for item in events] == ["start", "gather", "finish"]
    assert {item["label"] for item in events[2]["children"]} == {"a-child", "c-child"}
    assert all(item["kind"] == "succeeded" for item in events[2]["children"])
    # Only the new child was published by the second wait; three children in all.
    assert len(_all_jobs(ws)) == 4
    assert sorted(str(entry["label"]) for entry in h.state_of(done).children) == ["a-child", "b-child", "c-child"]


@pytest.mark.slow
def test_gather_rejoins_an_already_terminal_child_without_new_children(ws: Workspace, joins: _store.Installed) -> None:
    steps = {
        "start": {
            "spawn": [{"label": "a-child", "child": "succeed"}],
            "wait": {"condition": "all_terminal", "next_step": "gather"},
        },
        "gather": {"wait": {"condition": "any_terminal", "next_step": "finish", "rejoin": ["a-child"]}},
        "finish": {},
    }
    parent = _parent(ws, joins, steps)
    h.run(ws)
    done = h.find(ws, parent.job_id)
    assert done.state == "succeeded"
    events = _events(done)
    assert [item["step"] for item in events] == ["start", "gather", "finish"]
    assert events[2]["children"] == [{"label": "a-child", "kind": "succeeded"}]
    assert len(_all_jobs(ws)) == 2


@pytest.mark.slow
@pytest.mark.parametrize(
    ("condition", "count", "decided"),
    [
        ("all_succeeded", None, "impossible"),
        ("all_terminal", None, "satisfied"),
        ("any_succeeded", None, "satisfied"),
        ("any_terminal", None, "satisfied"),
        ("at_least", 1, "satisfied"),
        ("at_least", 2, "impossible"),
    ],
)
def test_a_join_condition_is_decided_over_a_succeeded_and_a_failed_child(
    ws: Workspace, joins: _store.Installed, condition: str, count: int | None, decided: str
) -> None:
    wait: dict[str, object] = {"condition": condition, "next_step": "gather"}
    if count is not None:
        wait["count"] = count
    steps = {
        "start": {"spawn": [{"label": "ok", "child": "succeed"}, {"label": "bad", "child": "fail"}], "wait": wait},
        "gather": {},
    }
    parent = _parent(ws, joins, steps)
    h.run(ws)
    ref = h.find(ws, parent.job_id)
    doc = h.state_of(ref)
    # Either way the children are observed and the join is cleared.
    assert doc.join is None and len(doc.observations) == 2
    if decided == "satisfied":
        assert ref.state == "succeeded"
        assert [item["step"] for item in _events(ref)] == ["start", "gather"]
        assert doc.activation is not None and doc.activation["reason"] == "join"
    else:
        assert ref.state == "failed"
        assert doc.failure is not None and doc.failure["code"] == "dependency_failure"
        assert condition in str(doc.failure["message"])


@pytest.mark.slow
def test_dependency_failure_names_the_failed_child(ws: Workspace, joins: _store.Installed) -> None:
    steps = {
        "start": {
            "spawn": [{"label": "doomed", "child": "fail"}],
            "wait": {"condition": "all_succeeded", "next_step": "gather"},
        },
        "gather": {},
    }
    parent = _parent(ws, joins, steps)
    h.run(ws)
    failed = h.find(ws, parent.job_id)
    assert failed.state == "failed"
    doc = h.state_of(failed)
    assert doc.failure is not None and doc.failure["code"] == "dependency_failure"
    # The observations name the failed child and carry its own failure, for diagnosis.
    (observation,) = doc.observations
    assert observation["label"] == "doomed" and observation["state"] == "failed"
    assert _failure_code(observation) == "child.broken"
    assert [entry["event"] for entry in doc.history_tail][-2:] == ["join_decided", "released"]


_GHOST_JOIN_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, uuid

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
child_id = str(uuid.uuid5(uuid.UUID(context["job_id"]), "ghost"))
draft = control / "outcome.tmp.x"
draft.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="wait", next_step="gather", join={
    "children": [{"workspace_id": context["workspace_id"], "job_id": child_id, "job_key": "child--" + child_id,
                  "placement_hint": "project/ghosts"}],
    "condition": "all_succeeded",
})
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.mark.slow
def test_unresolvable_join_child_fails_instead_of_waiting_forever(ws: Workspace, tmp_path: Path) -> None:
    installed = h.install(ws, tmp_path / "ghost", executables={"run": _GHOST_JOIN_RUNNER})
    submitted = h.submit(ws, installed, {"start": "wait"})
    h.run(ws, join_grace_seconds=0.0)
    failed = h.find(ws, submitted.job_id)
    assert failed.state == "failed"
    doc = h.state_of(failed)
    ghost_id = str(uuid.uuid5(uuid.UUID(submitted.job_id), "ghost"))
    assert doc.failure is not None and doc.failure["code"] == "dependency_failure"
    assert ghost_id in str(doc.failure["message"])
    assert [item["state"] for item in doc.observations] == ["pending"]


@pytest.mark.slow
def test_unresolvable_join_child_is_tolerated_within_the_grace(ws: Workspace, tmp_path: Path) -> None:
    installed = h.install(ws, tmp_path / "ghost", executables={"run": _GHOST_JOIN_RUNNER})
    submitted = h.submit(ws, installed, {"start": "wait"})
    h.run(ws, join_grace_seconds=3600.0)
    waiting = h.find(ws, submitted.job_id)
    assert waiting.state == "waiting"
    assert h.state_of(waiting).join is not None
