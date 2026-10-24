"""Unit tests of :mod:`httk.workflow._requests`: the v3 request file and the pure application table."""

import base64
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any

import pytest

from httk.workflow._job import JobDefinition
from httk.workflow._joins import JoinDecision
from httk.workflow._manager_commit import _outcome_intent, attempt_budget_failure, decide_join, retry_budget_available
from httk.workflow._requests import (
    ACTIONS,
    Defer,
    Discard,
    Drop,
    Effect,
    Eject,
    Request,
    Seal,
    Unlock,
    Unseal,
    apply,
    parse,
    post,
    validate_envelope,
)
from httk.workflow._state import TERMINAL_STATES, UNOWNED_STATES, Release, StateDoc
from httk.workflow.errors import FormatError

JOB_ID = "0b6f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e01"
PARENT_ID = "1c7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e02"


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


@pytest.fixture()
def ws(tmp_path: Path) -> FakeWorkspace:
    return FakeWorkspace(tmp_path, tmp_path / ".httk-workspace", tmp_path / "jobs", False, 0.1)


def _seed(tmp_path: Path) -> Path:
    path = tmp_path / "operator.seed"
    path.write_text(base64.b64encode(bytes([7]) * 32).decode("ascii") + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _job(
    *,
    parent: bool = True,
    maximum_attempts: int | None = None,
    maximum_activations: int | None = None,
    maximum_total_attempts: int | None = None,
) -> JobDefinition:
    retry: dict[str, object] = {"retry_on": []}
    if maximum_attempts is not None:
        retry["maximum_attempts_per_activation"] = maximum_attempts
    if maximum_activations is not None:
        retry["maximum_activations"] = maximum_activations
    if maximum_total_attempts is not None:
        retry["maximum_total_attempts"] = maximum_total_attempts
    return JobDefinition.from_mapping(
        {
            "format": "httk-workflow-job",
            "format_version": 3,
            "id": JOB_ID,
            "tag": "t",
            "name": "n",
            "placement": "p/q",
            "workflow": {"id": "local:w", "name": "w"},
            "initial_step": "start",
            "priority": 500,
            "claim": {"pool": "default", "required_capabilities": []},
            "retry_policy": retry,
            "resources": {},
            "step_resources": {},
            "parameters": {},
            "declarations": {},
            "declared": {},
            "environment": {},
            "parent": {
                "workspace_id": "2d7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e03",
                "job_id": PARENT_ID,
                "job_key": PARENT_ID,
                "placement": "p",
                "activation_id": "3e7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e04",
                "spawn_id": "4f7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e05",
            }
            if parent
            else None,
            "seal_succeeded": None,
        }
    )


def _doc(*, running: bool = False) -> StateDoc:
    doc = StateDoc.empty(JOB_ID).next_activation("start", "initial").next_attempt("launch", unclean=False)
    assert doc.attempt is not None
    doc = doc.with_phase("launching", str(doc.attempt["id"]))
    if not running:
        doc = doc.with_phase("idle", None).with_failure({"code": "process_failed", "message": "exit 1"})
    return doc


def _document(action: str, **extra: object) -> dict[str, object]:
    options: dict[str, dict[str, object]] = {
        "set_priority": {"priority": 7},
        "set_budget": {"budget": {"maximum_attempts_per_activation": 3}},
        "set_workflow": {"workflow": "local:w2"},
        "override_step": {"step": "relax"},
        "eject": {"destination": "out"},
    }
    return {
        "format": "httk-workflow-request",
        "format_version": 3,
        "request_id": str(uuid.uuid4()),
        "job_id": JOB_ID,
        "placement": "p/q",
        "action": action,
        "operator": "Op <op@example.org>",
        "reason": "because",
        "created_at": "2026-10-09T00:00:00+00:00",
        **options.get(action, {}),
        **extra,
    }


def _request(action: str, **extra: object) -> Request:
    document = _document(action, **extra)
    request_id = str(document["request_id"])
    path = Path(f"{JOB_ID}.{request_id}.json")
    return Request(request_id, JOB_ID, PurePosixPath("p/q"), action, MappingProxyType(document), path)


# -- the file ---------------------------------------------------------------------------------------------------------


def test_post_and_parse_round_trip(ws: FakeWorkspace) -> None:
    path = post(
        ws, action="set_priority", job_id=JOB_ID, placement=PurePosixPath("p/q"), operator="op", reason="r", priority=9
    )
    assert path.parent == ws.control / "requests"
    request = parse(path)
    assert (request.job_id, request.action, request.placement, request.path) == (
        JOB_ID,
        "set_priority",
        PurePosixPath("p/q"),
        path,
    )
    assert path.name == f"{JOB_ID}.{request.request_id}.json"
    assert request.document["priority"] == 9 and "operator_key" not in request.document
    # Unset options are left out; a deterministic request id is kept.
    request_id = str(uuid.uuid4())
    path = post(
        ws, action="cancel", job_id=JOB_ID, placement="", operator="op", reason="", force=False, request_id=request_id
    )
    assert parse(path).request_id == request_id and "force" not in parse(path).document


def test_post_refuses_invalid_documents(ws: FakeWorkspace) -> None:
    cases: tuple[tuple[str, dict[str, Any]], ...] = (
        ("explode", {}),
        ("set_priority", {}),
        ("cancel", {"step": "x"}),
        ("pause", {"bogus": 1}),
    )
    for action, extra in cases:
        with pytest.raises(FormatError):
            post(ws, action=action, job_id=JOB_ID, placement="p", operator="op", reason="r", **extra)
    assert not (ws.control / "requests").exists()


def test_signed_requests_verify_and_tampering_is_refused(ws: FakeWorkspace, tmp_path: Path) -> None:
    path = post(
        ws, action="pause", job_id=JOB_ID, placement="p/q", operator="op", reason="r", seed_path=_seed(tmp_path)
    )
    request = parse(path)
    assert str(request.document["operator_key"]).startswith("ed25519:")
    document = json.loads(path.read_bytes())
    document["reason"] = "forged"
    path.write_text(json.dumps(document))
    with pytest.raises(FormatError, match="signature"):
        parse(path)
    del document["signature"]
    path.write_text(json.dumps(document))
    with pytest.raises(FormatError, match="both"):
        parse(path)


@pytest.mark.parametrize(
    "change",
    [
        {"format_version": 2},
        {"job_key": f"t--{JOB_ID}"},
        {"expected_generation": 3},
        {"priority": 7},
        {"force": False},
        {"request_id": "x"},
        {"job_id": JOB_ID.upper()},
        {"placement": "/abs"},
        {"operator": ""},
        {"reason": None},
    ],
)
def test_envelope_is_strict(change: dict[str, object]) -> None:
    validate_envelope(_document("continue"))
    validate_envelope(_document("continue", force=True))
    with pytest.raises(FormatError):
        validate_envelope({**_document("continue"), **change})


def test_restart_envelope_takes_only_force() -> None:
    validate_envelope(_document("restart"))
    validate_envelope(_document("restart", force=True))
    for extra in ({"step": "relax"}, {"priority": 7}, {"destination": "out"}, {"tree": True}):
        with pytest.raises(FormatError):
            validate_envelope(_document("restart", **extra))


@pytest.mark.parametrize(
    ("action", "missing"),
    [
        ("set_priority", "priority"),
        ("set_budget", "budget"),
        ("set_workflow", "workflow"),
        ("override_step", "step"),
        ("eject", "destination"),
    ],
)
def test_action_options_are_required(action: str, missing: str) -> None:
    document = _document(action)
    del document[missing]
    with pytest.raises(FormatError):
        validate_envelope(document)


def test_parse_refusals(tmp_path: Path) -> None:
    document = _document("cancel")
    with pytest.raises(FileNotFoundError):
        parse(tmp_path / f"{JOB_ID}.{document['request_id']}.json")
    misnamed = tmp_path / f"{JOB_ID}.{uuid.uuid4()}.json"
    misnamed.write_text(json.dumps(document))
    with pytest.raises(FormatError, match="name"):
        parse(misnamed)
    real = tmp_path / "real.json"
    real.write_text(json.dumps(document))
    link = tmp_path / f"{JOB_ID}.{document['request_id']}.json"
    os.symlink(real, link)
    with pytest.raises(FormatError):
        parse(link)
    link.unlink()
    link.write_bytes(b"[]")
    with pytest.raises(FormatError):
        parse(link)
    link.write_bytes(b" " * (1 << 17))
    with pytest.raises(FormatError):
        parse(link)


# -- application ------------------------------------------------------------------------------------------------------


def _expected(action: str, state: str) -> Effect:
    terminal = state in TERMINAL_STATES
    if action == "cancel":
        return Drop("") if terminal else Release("cancelled", 500)
    if action == "pause":
        # A waiting parent is never paused: a later continue would rerun its spawning step.
        return Drop("") if terminal or state in ("paused", "waiting") else Release("paused", 500)
    if action in ("continue", "restart", "override_step"):
        return Release("ready", 500) if state in ("failed", "paused") else Drop("")
    if action == "set_priority":
        return Release(state, 7)
    if action in ("set_budget", "set_workflow"):
        return Drop("") if state in ("succeeded", "cancelled") else Release(state, 500)
    if action == "detach":
        return Release(state, 500)
    if action == "eject":
        return Eject("out")
    if action == "delete":
        # A succeeded job is deleted only after `job unseal`.
        return Discard() if (terminal and state != "succeeded") or state == "paused" else Drop("")
    if action == "seal":
        return Seal(Release(state, 500)) if state == "succeeded" else Drop("")
    if action == "unseal":
        return Unseal(Release(state, 500)) if state == "succeeded" else Drop("")
    if action == "unlock":
        return Drop("") if state == "cancelled" else Unlock(Release(state, 500))
    return Defer()


@pytest.mark.parametrize("state", UNOWNED_STATES)
@pytest.mark.parametrize("action", ACTIONS)
def test_application_table(action: str, state: str) -> None:
    doc, request = _doc(), _request(action)
    new, effect = apply(_job(), doc, state, 500, request, workflow_name=lambda _id: "w2")
    expected = _expected(action, state)
    assert type(effect) is type(expected)
    if isinstance(expected, (Release, Eject)):
        assert effect == (
            Release(expected.state, expected.priority, (request.request_id,))
            if isinstance(expected, Release)
            else expected
        )
    if isinstance(expected, (Seal, Unseal, Unlock)):
        assert isinstance(effect, (Seal, Unseal, Unlock))
        assert effect.release == Release(expected.release.state, 500, (request.request_id,))
    if isinstance(expected, Defer):
        assert new is doc
        return
    # Every decided request is recorded as applied, with a history note.
    assert new.applied_requests == (request.request_id,)
    entry = new.history_tail[-1]
    assert (entry["request_id"], entry["action"], entry["from"]) == (request.request_id, action, state)
    assert entry["event"] == ("request_dropped" if isinstance(effect, Drop) else "request_applied")
    if isinstance(effect, Drop) and action in ("continue", "restart", "override_step"):
        assert effect.reason == f"{action} applies to failed or paused jobs, not {state}"


@pytest.mark.parametrize("action", ACTIONS)
def test_live_attempt_defers_what_applies(action: str) -> None:
    doc = _doc(running=True)
    new, effect = apply(_job(), doc, "ready", 500, _request(action), workflow_name=lambda _id: "w2")
    if isinstance(_expected(action, "ready"), Drop):
        assert isinstance(effect, Drop)
    else:
        assert effect == Defer() and new is doc


def test_continue_and_override_step_revive() -> None:
    doc = _doc()
    assert doc.attempt is not None and doc.activation is not None
    new, _ = apply(_job(), doc, "failed", 500, _request("continue"))
    assert new.failure is None and new.activation == doc.activation
    assert new.attempt is not None and new.attempt["ordinal"] == 2 and new.attempt["reason"] == "manual_continue"
    assert new.attempt["previous_attempt_id"] == doc.attempt["id"]
    new, _ = apply(_job(), doc, "paused", 500, _request("override_step"))
    assert new.activation is not None and new.activation["step"] == "relax" and new.activation["ordinal"] == 2
    assert new.activation["reason"] == "override_step" and new.attempt is None and new.failure is None
    # A job paused before its first launch, or before its attempt started, returns to ready without a new attempt.
    new, effect = apply(_job(), StateDoc.empty(JOB_ID), "paused", 3, _request("continue"))
    assert effect == Release("ready", 3, effect.applied_requests) and new.activation is None  # type: ignore[union-attr]
    unstarted = doc.next_attempt("retry", unclean=False)
    new, _ = apply(_job(), unstarted, "paused", 500, _request("continue"))
    assert new.attempt == unstarted.attempt and new.counters == unstarted.counters


def _failed_at_wait() -> StateDoc:
    """A parent that failed at its wait: dependency_failure with the join kept."""

    child = {"label": None, "job_id": PARENT_ID, "job_key": PARENT_ID, "placement_hint": "p"}
    join = {"condition": "all_succeeded", "count": 0, "next_step": "gather", "children": [child]}
    return _doc().with_failure({"code": "dependency_failure", "message": "child failed"}).updated(join=join)


def test_continue_reopens_a_kept_wait() -> None:
    doc = _failed_at_wait()
    # A retry would exhaust the attempt budget: reopening the wait reruns nothing, so no budget binds it.
    request = _request("continue")
    new, effect = apply(_job(maximum_attempts=1), doc, "failed", 500, request)
    assert effect == Release("waiting", 500, (request.request_id,))
    assert new.failure is None and new.join == doc.join
    assert (new.attempt, new.activation, new.counters) == (doc.attempt, doc.activation, doc.counters)
    assert new.history_tail[-1]["to"] == "waiting"
    # A paused job never carries a join to reopen; continue from paused still retries.
    _, effect = apply(_job(), _doc(), "paused", 500, _request("continue"))
    assert isinstance(effect, Release) and effect.state == "ready"


def test_restart_and_override_step_drop_a_kept_wait() -> None:
    doc = _failed_at_wait()
    assert doc.attempt is not None
    new, effect = apply(_job(), doc, "failed", 500, _request("restart"))
    assert isinstance(effect, Release) and effect.state == "ready"
    assert new.join is None and new.failure is None and new.activation == doc.activation
    assert new.attempt is not None and new.attempt["reason"] == "manual_continue" and new.attempt["ordinal"] == 2
    new, effect = apply(_job(), doc, "failed", 500, _request("override_step"))
    assert isinstance(effect, Release) and effect.state == "ready" and new.join is None
    # With keep_budget, restart is bound by the budgets, like the legacy continue.
    new, effect = apply(_job(maximum_attempts=1), doc, "failed", 500, _request("restart", keep_budget=True))
    assert isinstance(effect, Release) and effect.state == "failed"
    assert new.failure is not None and new.failure["code"] == "retry_exhausted" and new.attempt == doc.attempt
    assert new.budget_override is None and "budget" not in new.history_tail[-1]
    # Without it, the restart restores the declared allowance: the spawning attempt plus one more.
    new, effect = apply(_job(maximum_attempts=1), doc, "failed", 500, _request("restart"))
    assert isinstance(effect, Release) and effect.state == "ready"
    assert dict(new.budget_override or {}) == {"maximum_attempts_per_activation": 2}


def test_refusal_and_force() -> None:
    seen: list[str] = []

    def refuse(request: Request) -> str:
        seen.append(request.action)
        return "parent p decided its join on this job"

    doc = _doc()
    new, effect = apply(_job(), doc, "failed", 500, _request("continue"), refusal=refuse)
    assert isinstance(effect, Drop) and "parent p" in effect.reason and new.attempt == doc.attempt
    request = _request("override_step", force=True)
    new, effect = apply(_job(), _doc(), "failed", 500, request, refusal=refuse)
    assert effect == Release("ready", 500, (request.request_id,))
    assert new.history_tail[-1]["revival_hazard"] == "parent p decided its join on this job"
    _, effect = apply(_job(), _doc(), "failed", 500, _request("continue"), refusal=lambda _request: None)
    assert isinstance(effect, Release)
    # A refused delete is always dropped (force is not allowed on delete).
    _, effect = apply(_job(), _doc(), "failed", 500, _request("delete"), refusal=refuse)
    assert isinstance(effect, Drop) and "parent p" in effect.reason
    # A refused eject (an unusable destination) is dropped too, without a force hint.
    _, effect = apply(_job(), _doc(), "failed", 500, _request("eject"), refusal=refuse)
    assert isinstance(effect, Drop) and effect.reason == "parent p decided its join on this job"
    request = _request("restart", force=True)
    _, effect = apply(_job(), _doc(), "failed", 500, request, refusal=refuse)
    assert effect == Release("ready", 500, (request.request_id,))
    _, effect = apply(_job(), _doc(), "failed", 500, _request("restart"), refusal=refuse)
    assert isinstance(effect, Drop) and effect.reason.endswith("republish with force to accept the hazard")
    # Only continue, restart, override_step, delete and eject consult the check.
    seen.clear()
    for action in ("cancel", "pause", "set_priority", "set_budget", "detach", "seal", "unseal"):
        apply(_job(), _doc(), "paused", 500, _request(action), refusal=refuse)
    assert seen == []


def test_revival_within_exhausted_budget_fails() -> None:
    doc = _doc()
    new, effect = apply(_job(maximum_attempts=1), doc, "paused", 500, _request("continue", keep_budget=True))
    assert effect == Release("failed", 500, effect.applied_requests)  # type: ignore[union-attr]
    assert new.failure is not None and new.failure["code"] == "retry_exhausted"
    assert new.attempt == doc.attempt


@pytest.mark.parametrize(
    "budget",
    [
        None,
        {},
        [],
        {"maximum_attempts": 3},
        {"maximum_activations": 0},
        {"maximum_total_attempts": True},
        {"maximum_total_attempts": "3"},
    ],
)
def test_set_budget_envelope(budget: object) -> None:
    validate_envelope(_document("set_budget", budget={"maximum_total_attempts": None, "maximum_activations": 2}))
    with pytest.raises(FormatError):
        validate_envelope(_document("set_budget", budget=budget))
    # Only set_budget carries a budget.
    with pytest.raises(FormatError):
        validate_envelope(_document("restart", budget={"maximum_activations": 2}))


def test_set_budget_merges_and_keeps_state_and_priority() -> None:
    doc = _doc().updated(budget_override={"maximum_total_attempts": 5, "maximum_activations": 2})
    request = _request("set_budget", budget={"maximum_activations": None, "maximum_attempts_per_activation": 4})
    new, effect = apply(_job(), doc, "failed", 321, request)
    assert effect == Release("failed", 321, (request.request_id,))
    assert dict(new.budget_override or {}) == {
        "maximum_total_attempts": 5,
        "maximum_activations": None,
        "maximum_attempts_per_activation": 4,
    }
    assert new.failure == doc.failure and new.attempt == doc.attempt
    assert new.history_tail[-1]["budget"] == {"maximum_activations": None, "maximum_attempts_per_activation": 4}
    for state in ("succeeded", "cancelled"):
        new, effect = apply(_job(), doc, state, 500, _request("set_budget"))
        assert effect == Drop(f"the job is already {state}") and new.budget_override == doc.budget_override


@pytest.mark.parametrize("workflow", ["w2", "", 3, None, "demo@v2", "local:", "git+", "embedded:w@0123456789ab"])
def test_set_workflow_envelope_names_an_installed_id(workflow: object) -> None:
    for valid in ("local:w2", "adhoc:run@0123456789ab", "git+https://example.org/r.git@" + "a" * 40):
        validate_envelope(_document("set_workflow", workflow=valid))
    with pytest.raises(FormatError):
        validate_envelope(_document("set_workflow", workflow=workflow))
    with pytest.raises(FormatError):
        validate_envelope(_document("set_workflow", workflow="local:w2", force=True))
    with pytest.raises(FormatError):
        validate_envelope(_document("restart", workflow="local:w2"))


def test_set_workflow_overrides_resets_runner_steps_and_records_both_ids() -> None:
    doc = _doc().updated(runner_steps=["start", "relax"])
    names: list[str] = []

    def name_of(workflow_id: str) -> str:
        names.append(workflow_id)
        return "w2"

    request = _request("set_workflow")
    new, effect = apply(_job(), doc, "failed", 321, request, workflow_name=name_of)
    assert effect == Release("failed", 321, (request.request_id,)) and names == ["local:w2"]
    assert dict(new.workflow_override or {}) == {"id": "local:w2", "name": "w2"} and new.runner_steps is None
    assert new.failure == doc.failure and new.attempt == doc.attempt and new.budget_override == doc.budget_override
    assert new.history_tail[-1]["workflow"] == {"from": "local:w", "to": "local:w2"}
    # Back to job.json's workflow: the override is cleared, not recorded as a no-op.
    back = _request("set_workflow", workflow="local:w")
    cleared, _ = apply(_job(), new, "failed", 321, back, workflow_name=lambda _id: "w")
    assert cleared.workflow_override is None and cleared.history_tail[-1]["workflow"] == {
        "from": "local:w2",
        "to": "local:w",
    }
    # A second switch records the override it replaces.
    again = _request("set_workflow", workflow="local:w3")
    newer, _ = apply(_job(), new, "failed", 321, again, workflow_name=lambda _id: "w3")
    assert dict(newer.workflow_override or {}) == {"id": "local:w3", "name": "w3"}
    assert newer.history_tail[-1]["workflow"] == {"from": "local:w2", "to": "local:w3"}
    # Gone from the workspace (or nobody to ask): dropped, nothing recorded.
    for lookup in (None, lambda _id: None):
        dropped, effect = apply(_job(), doc, "ready", 500, _request("set_workflow"), workflow_name=lookup)
        assert isinstance(effect, Drop) and "not installed" in effect.reason and dropped.workflow_override is None
        assert dropped.history_tail[-1]["workflow"] == {"from": "local:w", "to": "local:w2"}
    # The owner's compatibility check refuses, never forceable; succeeded and cancelled jobs are never switched.
    _, effect = apply(
        _job(), doc, "ready", 500, _request("set_workflow"), refusal=lambda _r: "rule 7", workflow_name=lambda _id: "w2"
    )
    assert effect == Drop("rule 7")
    for state in ("succeeded", "cancelled"):
        _, effect = apply(_job(), doc, state, 500, _request("set_workflow"), workflow_name=lambda _id: "w2")
        assert effect == Drop(f"the job is already {state}")


def test_set_budget_lets_an_exhausted_restart_run() -> None:
    job = _job(maximum_attempts=1)
    doc, effect = apply(job, _doc(), "failed", 500, _request("restart", keep_budget=True))
    assert effect == Release("failed", 500, effect.applied_requests)  # type: ignore[union-attr]
    assert doc.failure is not None and doc.failure["code"] == "retry_exhausted"
    doc, effect = apply(job, doc, "failed", 500, _request("set_budget"))
    assert isinstance(effect, Release) and effect.state == "failed"
    doc, effect = apply(job, doc, "failed", 500, _request("restart", keep_budget=True))
    assert isinstance(effect, Release) and effect.state == "ready"
    assert doc.failure is None and doc.attempt is not None and doc.attempt["ordinal"] == 2


def test_manager_budget_checks_honour_the_override() -> None:
    job, doc = _job(maximum_attempts=1), _doc()
    assert not retry_budget_available(job, doc)
    assert attempt_budget_failure(job, doc.next_attempt("retry", unclean=False)) is not None
    raised = doc.updated(budget_override={"maximum_attempts_per_activation": 2})
    assert retry_budget_available(job, raised)
    assert attempt_budget_failure(job, raised.next_attempt("retry", unclean=False)) is None
    lifted = doc.updated(budget_override={"maximum_attempts_per_activation": None})
    assert retry_budget_available(job, lifted)


def test_activation_budget_sites_honour_the_override() -> None:
    job, doc = _job(maximum_activations=1), _doc()
    raised = doc.updated(budget_override={"maximum_activations": 2})
    assert doc.attempt is not None
    outcome = {"action": "advance", "attempt_id": doc.attempt["id"], "next_step": "finish"}
    assert _outcome_intent(job, doc, outcome, 500, (), JOB_ID)["target_state"] == "failed"
    assert _outcome_intent(job, raised, outcome, 500, (), JOB_ID)["target_state"] == "ready"
    # override_step opens a second activation: refused by the declared limit, allowed once it is lifted.
    _, effect = apply(job, doc, "failed", 500, _request("override_step", keep_budget=True))
    assert isinstance(effect, Release) and effect.state == "failed"
    lifted = doc.updated(budget_override={"maximum_activations": None})
    new, effect = apply(job, lifted, "failed", 500, _request("override_step", keep_budget=True))
    assert isinstance(effect, Release) and effect.state == "ready"
    assert new.activation is not None and new.activation["ordinal"] == 2


def test_set_budget_then_continue_redecides_a_join_kept_by_maximum_activations() -> None:
    job = _job(maximum_activations=1)
    join = {"condition": "all_succeeded", "count": 0, "next_step": "gather", "children": []}
    waiting = _doc().with_failure(None).updated(join=join)
    doc, target = decide_join(job, waiting, JoinDecision("satisfied", (), ()))
    assert target == "failed" and doc.join == waiting.join
    assert doc.failure is not None and doc.failure["code"] == "budget_exhausted"
    doc, effect = apply(job, doc, "failed", 500, _request("set_budget", budget={"maximum_activations": 2}))
    assert isinstance(effect, Release) and effect.state == "failed"
    doc, effect = apply(job, doc, "failed", 500, _request("continue"))
    assert isinstance(effect, Release) and effect.state == "waiting" and doc.join == waiting.join
    assert doc.failure is None
    doc, target = decide_join(job, doc, JoinDecision("satisfied", (), ()))
    assert target == "ready" and doc.join is None and doc.activation is not None
    assert (doc.activation["step"], doc.activation["reason"], doc.activation["ordinal"]) == ("gather", "join", 2)


def test_detach() -> None:
    new, effect = apply(_job(), _doc(), "ready", 500, _request("detach"))
    assert (
        isinstance(effect, Release) and new.detached is not None and new.detached["operator"] == "Op <op@example.org>"
    )
    _, effect = apply(_job(), new, "ready", 500, _request("detach"))
    assert isinstance(effect, Drop)
    _, effect = apply(_job(parent=False), _doc(), "ready", 500, _request("detach"))
    assert isinstance(effect, Drop)


def _unlocked(doc: StateDoc, state: str = "failed") -> StateDoc:
    new, effect = apply(_job(), doc, state, 500, _request("unlock"))
    assert isinstance(effect, Unlock) and effect.copy and effect.release.state == state
    return new


def test_unlock_sets_a_one_way_marker() -> None:
    doc = _doc()
    new = _unlocked(doc)
    assert new.unlocked is not None and dict(new.unlocked).keys() == {"at", "operator", "reason"}
    assert new.unlocked["operator"] == "Op <op@example.org>" and new.unlocked["reason"] == "because"
    assert new.history_tail[-1]["unlocked"] is True and new.history_tail[-1]["to"] == "failed"
    # With no_copy a second unlock is dropped; a plain one (it copies) is left to the owner, keeping the marker.
    dropped, effect = apply(_job(), new, "failed", 500, _request("unlock", no_copy=True))
    assert isinstance(effect, Drop) and effect.reason == "already unlocked"
    assert dropped.unlocked == new.unlocked
    again, effect = apply(_job(), new, "failed", 500, _request("unlock", reason="other"))
    assert isinstance(effect, Unlock) and again.unlocked == new.unlocked
    _, effect = apply(_job(), doc, "failed", 500, _request("unlock", no_copy=True))
    assert isinstance(effect, Unlock) and not effect.copy
    assert apply(_job(), _doc(running=True), "ready", 500, _request("unlock"))[1] == Defer()


def test_unlock_refusals() -> None:
    _, effect = apply(_job(), _doc(), "cancelled", 500, _request("unlock"))
    assert isinstance(effect, Drop) and effect.reason == "the job is already cancelled"
    sealed = _doc().updated(seal={"sha256": "0" * 64, "signed": False})
    _, effect = apply(_job(), sealed, "succeeded", 500, _request("unlock"))
    assert isinstance(effect, Drop) and "run `job unseal` first" in effect.reason
    for seal in ({"disabled": True}, {"released": True, "at": "now", "request_id": str(uuid.uuid4())}, None):
        _, effect = apply(_job(), _doc().updated(seal=seal), "succeeded", 500, _request("unlock"))
        assert isinstance(effect, Unlock)
    with pytest.raises(FormatError):
        validate_envelope(_document("unlock", no_copy=False))
    with pytest.raises(FormatError):
        validate_envelope(_document("cancel", no_copy=True))


def test_unlocked_job_relaxations() -> None:
    locked = _doc().updated(seal={"disabled": True})
    unlocked = _unlocked(locked, "succeeded")
    for action in ("continue", "restart", "override_step", "set_budget", "set_workflow"):
        _, effect = apply(_job(), locked, "succeeded", 500, _request(action), workflow_name=lambda _id: "w2")
        assert isinstance(effect, Drop), action
        _, effect = apply(_job(), unlocked, "succeeded", 500, _request(action), workflow_name=lambda _id: "w2")
        expected = "succeeded" if action.startswith("set_") else "ready"
        assert isinstance(effect, Release) and effect.state == expected, action
        _, effect = apply(_job(), unlocked, "cancelled", 500, _request(action), workflow_name=lambda _id: "w2")
        assert isinstance(effect, Drop), action
    _, effect = apply(_job(), locked, "succeeded", 500, _request("delete"))
    assert isinstance(effect, Drop)
    _, effect = apply(_job(), unlocked, "succeeded", 500, _request("delete"))
    assert isinstance(effect, Discard)
    _, effect = apply(_job(), locked.updated(seal=None), "succeeded", 500, _request("seal"))
    assert isinstance(effect, Seal)
    _, effect = apply(_job(), unlocked.updated(seal=None), "succeeded", 500, _request("seal"))
    assert isinstance(effect, Drop) and effect.reason == "an unlocked job cannot be sealed"


def test_mismatched_job_is_refused() -> None:
    other = StateDoc.empty(str(uuid.uuid4()))
    with pytest.raises(ValueError):
        apply(_job(), other, "ready", 500, _request("cancel"))


def test_unseal_releases_a_succeeded_job_for_delete() -> None:
    doc = _doc()
    new, effect = apply(_job(), doc, "succeeded", 500, _request("unseal"))
    assert isinstance(effect, Unseal) and new.seal is not None and new.seal["released"] is True
    assert any(entry["event"] == "unsealed" and entry["reason"] == "unseal" for entry in new.history_tail)
    _, effect = apply(_job(), new, "succeeded", 500, _request("delete"))
    assert isinstance(effect, Discard)
    _, effect = apply(_job(), new, "succeeded", 500, _request("unseal"))
    assert isinstance(effect, Drop) and "already unsealed" in effect.reason
    sealed = doc.updated(seal={"sha256": "0" * 64, "signed": False})
    _, effect = apply(_job(), sealed, "succeeded", 500, _request("seal"))
    assert isinstance(effect, Drop) and "already sealed" in effect.reason
    _, effect = apply(_job(), sealed, "succeeded", 500, _request("delete"))
    assert isinstance(effect, Drop) and "job unseal" in effect.reason


def _started(doc: StateDoc) -> StateDoc:
    assert doc.attempt is not None
    return doc.with_phase("launching", str(doc.attempt["id"])).with_phase("idle", None)


def _failed_after(attempts: int, code: str = "process_failed") -> StateDoc:
    """A job whose first activation ran *attempts* attempts and failed."""

    doc = _doc()
    for _ in range(attempts - 1):
        doc = _started(doc.next_attempt("retry", unclean=False))
    return doc.with_failure({"code": code, "message": "failed"})


def _attempts_run(job: JobDefinition, doc: StateDoc) -> list[int]:
    """Drive the manager's budget checks: the ordinals that launch, retrying every failure until refused."""

    ran: list[int] = []
    while doc.attempt is not None and attempt_budget_failure(job, doc) is None:
        ordinal = doc.attempt["ordinal"]
        assert isinstance(ordinal, int)
        ran.append(ordinal)
        doc = _started(doc)
        if not retry_budget_available(job, doc):
            break
        doc = doc.next_attempt("retry", unclean=False)
    return ran


@pytest.mark.parametrize(("failed_at", "expected"), [(3, [4, 5, 6]), (1, [2, 3, 4])])
def test_restart_restores_the_declared_attempts(failed_at: int, expected: list[int]) -> None:
    job = _job(maximum_attempts=3)
    doc = _failed_after(failed_at, "retry_exhausted" if failed_at == 3 else "process_failed")
    request = _request("restart")
    new, effect = apply(job, doc, "failed", 500, request)
    assert effect == Release("ready", 500, (request.request_id,))
    assert _attempts_run(job, new) == expected
    assert dict(new.budget_override or {}) == {"maximum_attempts_per_activation": failed_at + 3}
    assert new.history_tail[-1]["budget"] == {"maximum_attempts_per_activation": failed_at + 3}
    # keep_budget changes nothing: the exhausted job stays failed, the other runs out its declared three.
    kept, effect = apply(job, doc, "failed", 500, _request("restart", keep_budget=True))
    assert kept.budget_override is None and "budget" not in kept.history_tail[-1]
    assert isinstance(effect, Release) and effect.state == ("failed" if failed_at == 3 else "ready")
    if failed_at == 1:
        assert _attempts_run(job, kept) == [2, 3]


def test_continue_of_an_unstarted_retry_counts_it_as_the_next_attempt() -> None:
    # Settle-then-pause: attempt 1 failed, the retry (attempt 2) was recorded but never started.
    job = _job(maximum_attempts=3, maximum_total_attempts=3, maximum_activations=1)
    paused = _failed_after(1).next_attempt("retry", unclean=False)
    new, _ = apply(job, paused, "paused", 500, _request("continue"))
    assert new.attempt == paused.attempt and _attempts_run(job, new) == [2, 3, 4]
    # The activation has a started attempt, so it counts as used: maximum_activations 1 + 1.
    raised = {"maximum_attempts_per_activation": 4, "maximum_total_attempts": 4, "maximum_activations": 2}
    assert new.history_tail[-1]["budget"] == raised
    # An activation whose first attempt never started is the next unit itself: nothing to raise.
    fresh = StateDoc.empty(JOB_ID).next_activation("start", "initial").next_attempt("launch", unclean=False)
    new, _ = apply(job, fresh, "paused", 500, _request("continue"))
    assert new.budget_override is None and "budget" not in new.history_tail[-1]
    assert _attempts_run(job, new) == [1, 2, 3]


def test_continue_at_a_budget_exhausted_wait_restores_one_activation() -> None:
    job = _job(maximum_activations=1)
    join = {"condition": "all_succeeded", "count": 0, "next_step": "gather", "children": []}
    doc, target = decide_join(job, _doc().with_failure(None).updated(join=join), JoinDecision("satisfied", (), ()))
    assert target == "failed" and doc.failure is not None and doc.failure["code"] == "budget_exhausted"
    doc, effect = apply(job, doc, "failed", 500, _request("continue"))
    assert isinstance(effect, Release) and effect.state == "waiting"
    assert dict(doc.budget_override or {}) == {"maximum_activations": 2}
    doc, target = decide_join(job, doc, JoinDecision("satisfied", (), ()))
    assert target == "ready" and doc.activation is not None and doc.activation["ordinal"] == 2
    # Exactly one more: the next activation is refused again.
    assert doc.attempt is None
    doc = _started(doc.next_attempt("launch", unclean=False))
    assert doc.attempt is not None
    outcome = {"action": "advance", "attempt_id": doc.attempt["id"], "next_step": "finish"}
    assert _outcome_intent(job, doc, outcome, 500, (), JOB_ID)["target_state"] == "failed"


def test_override_step_restores_attempts_and_activations() -> None:
    job = _job(maximum_attempts=3, maximum_activations=2)
    doc = _failed_after(3, "retry_exhausted")
    new, effect = apply(job, doc, "failed", 500, _request("override_step"))
    assert isinstance(effect, Release) and effect.state == "ready"
    # Per activation: the new activation starts at 0, so the declared 3 already fit; activations: 1 used + 2.
    assert dict(new.budget_override or {}) == {"maximum_activations": 3}
    assert new.history_tail[-1]["budget"] == {"maximum_activations": 3}
    assert _attempts_run(job, new.next_attempt("launch", unclean=False)) == [1, 2, 3]
    # Paused before the new activation's first attempt, continue counts the opened activation as the next unit.
    paused, _ = apply(job, new, "ready", 500, _request("pause"))
    resumed, _ = apply(job, paused, "paused", 500, _request("continue"))
    assert resumed.budget_override == new.budget_override and "budget" not in resumed.history_tail[-1]


def test_revival_restores_a_declared_unlimited_limit() -> None:
    # A stop-by-lowering holds only until a revival without keep_budget.
    doc = _failed_after(1).updated(budget_override={"maximum_attempts_per_activation": 1, "maximum_activations": 4})
    new, effect = apply(_job(maximum_activations=1), doc, "failed", 500, _request("restart"))
    assert isinstance(effect, Release) and effect.state == "ready"
    assert dict(new.budget_override or {}) == {"maximum_activations": 4}
    assert new.history_tail[-1]["budget"] == {"maximum_attempts_per_activation": None}
    only = _failed_after(1).updated(budget_override={"maximum_attempts_per_activation": 1})
    new, _ = apply(_job(), only, "failed", 500, _request("restart"))
    assert new.budget_override is None
    kept, effect = apply(_job(), only, "failed", 500, _request("restart", keep_budget=True))
    assert isinstance(effect, Release) and effect.state == "failed" and kept.budget_override == only.budget_override


def test_revival_never_lowers_or_limits_an_override() -> None:
    job = _job(maximum_attempts=1, maximum_activations=1)
    for override in (
        {"maximum_attempts_per_activation": 10, "maximum_activations": 10},
        {
            "maximum_attempts_per_activation": None,
            "maximum_activations": None,
        },
    ):
        doc = _failed_after(1).updated(budget_override=override)
        new, effect = apply(job, doc, "failed", 500, _request("restart"))
        assert isinstance(effect, Release) and effect.state == "ready"
        assert new.budget_override == doc.budget_override and "budget" not in new.history_tail[-1]


def test_keep_budget_envelope() -> None:
    for action in ("continue", "restart", "override_step"):
        validate_envelope(_document(action, keep_budget=True))
        with pytest.raises(FormatError):
            validate_envelope(_document(action, keep_budget=False))
    for action in ("pause", "set_budget", "cancel"):
        with pytest.raises(FormatError):
            validate_envelope(_document(action, keep_budget=True))


@pytest.mark.parametrize("maximum", [1, 2])
def test_override_step_counts_an_abandoned_never_launched_activation(maximum: int) -> None:
    job = _job(maximum_activations=maximum)
    paused = StateDoc.empty(JOB_ID).next_activation("start", "initial")
    new, effect = apply(job, paused, "paused", 500, _request("override_step"))
    assert isinstance(effect, Release) and effect.state == "ready"
    assert new.activation is not None and new.activation["ordinal"] == 2
    assert dict(new.budget_override or {}) == {"maximum_activations": 1 + maximum}
    # Exactly the declared activations from the opened one: each advance until the limit is allowed.
    for _ in range(maximum):
        new = _started(new.next_attempt("launch", unclean=False))
        assert new.attempt is not None
        outcome = {"action": "advance", "attempt_id": new.attempt["id"], "next_step": "finish"}
        intent = _outcome_intent(job, new, outcome, 500, (), JOB_ID)
        if new.counters["activations"] == 1 + maximum:
            assert intent["target_state"] == "failed"
        else:
            assert intent["target_state"] == "ready"
            new = new.next_activation("finish", "advance")


def test_override_step_counts_an_abandoned_unstarted_retry() -> None:
    # Settle-then-pause: attempt 2 was recorded (attempts_total 2) but never started.
    job = _job(maximum_attempts=2, maximum_total_attempts=2)
    paused = _failed_after(1).next_attempt("retry", unclean=False)
    new, _ = apply(job, paused, "paused", 500, _request("override_step"))
    assert dict(new.budget_override or {}) == {"maximum_total_attempts": 4}
    assert _attempts_run(job, new.next_attempt("launch", unclean=False)) == [1, 2]
