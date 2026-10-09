"""Unit tests of :mod:`httk.workflow._state`: round trips, strictness, bounds and the release record."""

import json
import os
import signal
import uuid
from pathlib import Path

import pytest

from httk.workflow._state import (
    MAX_STATE_BYTES,
    Release,
    StateDoc,
    decode_state,
    encode_state,
    read_state_unowned,
)
from httk.workflow.errors import FormatError

JOB_ID = "0b6f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e01"


def _request_id() -> str:
    return str(uuid.uuid4())


def _full_mapping() -> dict[str, object]:
    """A document with every member populated."""

    mapping = StateDoc.empty(JOB_ID).as_mapping()
    mapping.update(
        owner_id="a" * 32,
        phase={"kind": "running", "attempt_id": str(uuid.uuid4())},
        activation={"id": str(uuid.uuid4()), "step": "relax", "ordinal": 3, "reason": "advance"},
        attempt={"id": str(uuid.uuid4()), "ordinal": 2, "reason": "owner_lost", "previous_attempt_id": None},
        counters={"activations": 3, "attempts_total": 5},
        resources={"cores": 4, "memory": "4G"},
        runner_steps=["prepare", {"name": "relax"}],
        failure={"code": "timeout", "detail": "too slow"},
        failure_history=[{"code": "x"}] * 16,
        join={"children": [], "condition": "all_succeeded", "on_impossible": None, "next_step": "aggregate"},
        observations=[{"job_id": JOB_ID, "state": "succeeded"}],
        children=[{"job_id": JOB_ID, "label": "a"}],
        commit={"attempt_id": "a", "action": "advance", "children": [{"staged": "x"}]},
        release_to={"state": "ready", "priority": 7},
        applied_requests=[_request_id(), _request_id()],
        origin="exchange",
        exchange_name=str(uuid.uuid4()),
        detached={"at": "now", "operator": "op"},
        seal={"sha256": "00", "signed": True},
        workflow_pin={"id": "local:x", "tree_sha256": "ff"},
        history_tail=[{"at": "t", "event": "claimed"}] * 32,
    )
    return mapping


def test_empty_round_trips() -> None:
    doc = StateDoc.empty(JOB_ID)
    assert doc.phase == {"kind": "idle", "attempt_id": None}
    assert doc.release_to is None and doc.applied_requests == () and doc.origin == "local"
    assert StateDoc.from_mapping(doc.as_mapping()) == doc
    assert decode_state(encode_state(doc)) == doc


def test_full_document_round_trips_through_bytes() -> None:
    mapping = _full_mapping()
    doc = StateDoc.from_mapping(mapping)
    assert doc.release_to == Release("ready", 7)
    assert doc.as_mapping() == mapping
    assert decode_state(encode_state(doc)) == doc
    # Canonical encoding: sorted keys, so equal documents encode to equal bytes.
    assert encode_state(doc) == encode_state(StateDoc.from_mapping(json.loads(encode_state(doc))))


def test_document_is_immutable_all_the_way_down() -> None:
    doc = StateDoc.from_mapping(_full_mapping())
    with pytest.raises(TypeError):
        doc.resources["cores"] = 8  # type: ignore[index]
    assert isinstance(doc.runner_steps, tuple)
    exported = doc.as_mapping()
    exported["resources"]["cores"] = 99  # type: ignore[index]
    assert doc.resources is not None and doc.resources["cores"] == 4


@pytest.mark.parametrize(
    ("member", "value"),
    [
        ("format", "httk-workflow-job"),
        ("format_version", 2),
        ("job_id", "not-a-uuid"),
        ("job_id", JOB_ID.upper()),
        ("updated_at", ""),
        ("owner_id", "A" * 32),
        ("owner_id", "a" * 31),
        ("phase", {"kind": "sleeping", "attempt_id": None}),
        ("phase", {"kind": "idle"}),
        ("phase", None),
        ("counters", None),
        ("activation", []),
        ("runner_steps", {"a": 1}),
        ("failure_history", [1]),
        ("observations", {"a": 1}),
        ("release_to", {"state": "owned", "priority": 1}),
        ("release_to", {"state": "ready", "priority": 1000}),
        ("release_to", {"state": "ready", "priority": True}),
        ("release_to", {"state": "ready", "priority": 1, "applied_requests": []}),
        ("release_to", {"state": 3, "priority": 1}),
        ("applied_requests", ["not-a-uuid"]),
        ("applied_requests", "x"),
        ("origin", "remote"),
        ("exchange_name", "client-1"),
    ],
)
def test_malformed_members_are_refused(member: str, value: object) -> None:
    mapping = _full_mapping()
    mapping[member] = value
    with pytest.raises(FormatError):
        StateDoc.from_mapping(mapping)


def test_unknown_and_missing_members_are_refused() -> None:
    mapping = _full_mapping()
    mapping["pause_requested"] = True
    with pytest.raises(FormatError, match="pause_requested"):
        StateDoc.from_mapping(mapping)
    mapping = _full_mapping()
    del mapping["history_tail"]
    with pytest.raises(FormatError, match="history_tail"):
        StateDoc.from_mapping(mapping)


def test_non_json_values_are_refused() -> None:
    mapping = _full_mapping()
    mapping["resources"] = {1: "x"}
    with pytest.raises(FormatError):
        StateDoc.from_mapping(mapping)
    mapping["resources"] = {"x": object()}
    with pytest.raises(FormatError):
        StateDoc.from_mapping(mapping)
    mapping["resources"] = {"x": float("nan")}
    with pytest.raises(FormatError):
        StateDoc.from_mapping(mapping)
    with pytest.raises(FormatError):
        decode_state(b"[1, 2]")
    with pytest.raises(FormatError):
        decode_state(b"\xff\xfe")


def test_bounded_lists() -> None:
    mapping = _full_mapping()
    mapping["failure_history"] = [{"code": "x"}] * 17
    with pytest.raises(FormatError, match="failure_history"):
        StateDoc.from_mapping(mapping)
    mapping = _full_mapping()
    mapping["history_tail"] = [{"event": "x"}] * 33
    with pytest.raises(FormatError, match="history_tail"):
        StateDoc.from_mapping(mapping)


def test_document_size_is_bounded() -> None:
    mapping = _full_mapping()
    mapping["resources"] = {"blob": "x" * MAX_STATE_BYTES}
    with pytest.raises(FormatError, match="larger"):
        StateDoc.from_mapping(mapping)
    with pytest.raises(FormatError, match="larger"):
        decode_state(b" " * (MAX_STATE_BYTES + 1))
    # Just under the bound passes.
    mapping["resources"] = {"blob": "x" * (MAX_STATE_BYTES - 4096)}
    assert len(encode_state(StateDoc.from_mapping(mapping))) <= MAX_STATE_BYTES


def test_with_release_records_release_unions_requests_and_clears_commit() -> None:
    first, second, third = _request_id(), _request_id(), _request_id()
    mapping = _full_mapping()
    mapping["applied_requests"] = [first, second]
    doc = StateDoc.from_mapping(mapping)
    released = doc.with_release(Release("failed", 12, (second, third)))
    assert released.release_to == Release("failed", 12)
    assert released.release_to is not None and released.release_to.applied_requests == ()
    assert released.applied_requests == (first, second, third)
    assert released.commit is None
    assert released.as_mapping()["release_to"] == {"state": "failed", "priority": 12}
    # The original is unchanged, and the result is a valid document.
    assert doc.commit is not None and doc.applied_requests == (first, second)
    assert decode_state(encode_state(released)) == released


@pytest.mark.parametrize(
    ("state", "priority", "requests"),
    [("owned", 1, ()), ("ready", -1, ()), ("ready", 1000, ()), ("ready", True, ()), ("ready", 1, ("x",))],
)
def test_release_validates(state: str, priority: int, requests: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        Release(state, priority, requests)


def test_read_state_unowned(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    assert read_state_unowned(path) == (None, False)
    assert read_state_unowned(tmp_path / "gone" / "state.json") == (None, False)
    doc = StateDoc.empty(JOB_ID)
    path.write_bytes(encode_state(doc))
    assert read_state_unowned(path) == (doc, False)
    path.write_bytes(encode_state(doc)[:-7])
    assert read_state_unowned(path) == (None, True)
    path.write_bytes(b'{"format": "httk-workflow-state"}')
    assert read_state_unowned(path) == (None, True)
    path.write_bytes(b"x" * (MAX_STATE_BYTES + 1))
    assert read_state_unowned(path) == (None, True)
    path.unlink()
    target = tmp_path / "elsewhere.json"
    target.write_bytes(encode_state(doc))
    os.symlink(target, path)
    assert read_state_unowned(path) == (None, True)
    path.unlink()
    path.mkdir()
    assert read_state_unowned(path) == (None, True)


# -- helpers (phase C) ----------------------------------------------------------------------------------------------


def test_activation_and_attempt_lifecycle() -> None:
    doc = StateDoc.empty(JOB_ID)
    with pytest.raises(ValueError):
        doc.next_attempt("launch", unclean=False)
    first = doc.next_activation("prepare", "initial")
    assert first.activation is not None and first.activation["ordinal"] == 1 and first.activation["step"] == "prepare"
    assert first.attempt is None and first.counters["activations"] == 1
    one = first.next_attempt("launch", unclean=False)
    two = one.next_attempt("owner_lost", unclean=True)
    assert one.attempt is not None and two.attempt is not None
    assert (one.attempt["ordinal"], two.attempt["ordinal"]) == (1, 2)
    assert two.attempt["previous_attempt_id"] == one.attempt["id"] and two.attempt["unclean"] is True
    assert two.attempt["started_at"] is None and two.counters["attempts_total"] == 2
    advanced = two.next_activation("relax", "advance")
    assert advanced.activation is not None and advanced.activation["ordinal"] == 2
    assert advanced.attempt is None and advanced.counters == {"activations": 2, "attempts_total": 2}
    assert advanced.activation["id"] != first.activation["id"]
    assert decode_state(encode_state(advanced)) == advanced
    with pytest.raises(ValueError):
        doc.next_activation("relax", "whim")
    with pytest.raises(FormatError):
        doc.next_activation("a/b", "advance")


def test_phase_names_the_current_attempt() -> None:
    doc = StateDoc.empty(JOB_ID).next_activation("s", "initial").next_attempt("launch", unclean=False)
    assert doc.attempt is not None
    attempt_id = str(doc.attempt["id"])
    launching = doc.with_phase("launching", attempt_id)
    assert launching.phase == {"kind": "launching", "attempt_id": attempt_id}
    assert launching.attempt is not None and launching.attempt["started_at"] is not None
    running = launching.with_phase("running", attempt_id)
    assert running.attempt == launching.attempt
    assert running.with_phase("idle", None).phase == {"kind": "idle", "attempt_id": None}
    for kind, named in (("idle", attempt_id), ("running", None), ("running", str(uuid.uuid4())), ("asleep", None)):
        with pytest.raises(ValueError):
            doc.with_phase(kind, named)


def test_failure_history_is_bounded() -> None:
    doc = StateDoc.empty(JOB_ID)
    for index in range(20):
        doc = doc.with_failure({"code": f"c{index}", "message": "m"})
    assert doc.failure == {"code": "c19", "message": "m"}
    assert [item["code"] for item in doc.failure_history] == [f"c{index}" for index in range(4, 20)]
    cleared = doc.with_failure(None)
    assert cleared.failure is None and cleared.failure_history == doc.failure_history
    assert len(doc.with_failure({"code": "x"}, history_limit=2).failure_history) == 2


def test_history_tail_is_bounded_and_members_replace() -> None:
    doc = StateDoc.empty(JOB_ID)
    for index in range(40):
        doc = doc.with_history("released", **{"from": "ready", "to": "paused", "n": index})
    assert len(doc.history_tail) == 32 and doc.history_tail[-1]["n"] == 39 and doc.history_tail[0]["n"] == 8
    assert doc.history_tail[-1]["event"] == "released" and "at" in doc.history_tail[-1]
    commit = {"attempt_id": "a", "action": "advance", "children": [{"staged": "x"}]}
    doc = doc.with_commit(commit).with_observations([{"job_id": JOB_ID}]).with_children([{"label": "a"}])
    assert doc.commit is not None and doc.commit["children"] == ({"staged": "x"},)
    assert doc.observations == ({"job_id": JOB_ID},) and doc.children == ({"label": "a"},)
    assert doc.with_commit(None).commit is None
    assert decode_state(encode_state(doc)) == doc
    with pytest.raises(FormatError):
        doc.updated(origin="remote")


def test_read_state_unowned_never_blocks_on_a_fifo(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    os.mkfifo(path)

    def expire(signum: int, frame: object) -> None:
        raise TimeoutError("read_state_unowned blocked on a FIFO")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.alarm(5)
    try:
        assert read_state_unowned(path) == (None, True)
        # A FIFO with a writer attached is refused the same way, without reading from it.
        writer = os.open(path, os.O_RDWR | os.O_NONBLOCK)
        try:
            assert read_state_unowned(path) == (None, True)
        finally:
            os.close(writer)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
