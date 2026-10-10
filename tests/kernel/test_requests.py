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
from httk.workflow._requests import (
    ACTIONS,
    Defer,
    Discard,
    Drop,
    Effect,
    Eject,
    Request,
    Seal,
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


def _job(*, parent: bool = True, maximum_attempts: int | None = None) -> JobDefinition:
    retry: dict[str, object] = {"retry_on": []}
    if maximum_attempts is not None:
        retry["maximum_attempts_per_activation"] = maximum_attempts
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


@pytest.mark.parametrize(
    ("action", "missing"), [("set_priority", "priority"), ("override_step", "step"), ("eject", "destination")]
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
    if action in ("continue", "override_step"):
        return Release("ready", 500) if state in ("failed", "paused") else Drop("")
    if action == "set_priority":
        return Release(state, 7)
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
    return Defer()


@pytest.mark.parametrize("state", UNOWNED_STATES)
@pytest.mark.parametrize("action", ACTIONS)
def test_application_table(action: str, state: str) -> None:
    doc, request = _doc(), _request(action)
    new, effect = apply(_job(), doc, state, 500, request)
    expected = _expected(action, state)
    assert type(effect) is type(expected)
    if isinstance(expected, (Release, Eject)):
        assert effect == (
            Release(expected.state, expected.priority, (request.request_id,))
            if isinstance(expected, Release)
            else expected
        )
    if isinstance(expected, (Seal, Unseal)):
        assert isinstance(effect, (Seal, Unseal))
        assert effect.release == Release(expected.release.state, 500, (request.request_id,))
    if isinstance(expected, Defer):
        assert new is doc
        return
    # Every decided request is recorded as applied, with a history note.
    assert new.applied_requests == (request.request_id,)
    entry = new.history_tail[-1]
    assert (entry["request_id"], entry["action"], entry["from"]) == (request.request_id, action, state)
    assert entry["event"] == ("request_dropped" if isinstance(effect, Drop) else "request_applied")


@pytest.mark.parametrize("action", ACTIONS)
def test_live_attempt_defers_what_applies(action: str) -> None:
    doc = _doc(running=True)
    new, effect = apply(_job(), doc, "ready", 500, _request(action))
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
    # Only continue, override_step, delete and eject consult the check.
    seen.clear()
    for action in ("cancel", "pause", "set_priority", "detach", "seal", "unseal"):
        apply(_job(), _doc(), "paused", 500, _request(action), refusal=refuse)
    assert seen == []


def test_revival_within_exhausted_budget_fails() -> None:
    doc = _doc()
    new, effect = apply(_job(maximum_attempts=1), doc, "paused", 500, _request("continue"))
    assert effect == Release("failed", 500, effect.applied_requests)  # type: ignore[union-attr]
    assert new.failure is not None and new.failure["code"] == "retry_exhausted"
    assert new.attempt == doc.attempt


def test_detach() -> None:
    new, effect = apply(_job(), _doc(), "ready", 500, _request("detach"))
    assert (
        isinstance(effect, Release) and new.detached is not None and new.detached["operator"] == "Op <op@example.org>"
    )
    _, effect = apply(_job(), new, "ready", 500, _request("detach"))
    assert isinstance(effect, Drop)
    _, effect = apply(_job(parent=False), _doc(), "ready", 500, _request("detach"))
    assert isinstance(effect, Drop)


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
