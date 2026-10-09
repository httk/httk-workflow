"""Unit tests of :mod:`httk.workflow._joins` (read-only join evaluation, note §7.4) on a real filesystem."""

import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow._job import JobDefinition
from httk.workflow._joins import CONDITIONS, consumed_by_decided_join, evaluate, impossible, satisfied
from httk.workflow._kernel import JobRef, ListingCache, Owner, claim, register_owner, submit
from httk.workflow._state import StateDoc, encode_state
from httk.workflow.errors import FormatError

FAILURE: dict[str, object] = {"code": "runner_failed", "message": "boom"}


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


def job_mapping(job_id: str, tag: str, placement: str, parent: object = None) -> dict[str, object]:
    return {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": job_id,
        "tag": tag,
        "name": tag,
        "placement": placement,
        "workflow": {"id": "w", "name": "w"},
        "initial_step": "start",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {},
        "resources": {},
        "step_resources": {},
        "parameters": {},
        "declarations": {},
        "declared": {},
        "environment": {},
        "parent": parent,
        "seal_succeeded": None,
    }


@dataclass
class World:
    ws: FakeWorkspace
    owner: Owner

    def put(self, job: JobDefinition, state: str, doc: StateDoc | None = None) -> JobRef:
        staging = self.owner.scratch("build") / "job"
        staging.mkdir(parents=True)
        (staging / "job.json").write_bytes(job.encode())
        if doc is not None:
            (staging / "state.json").write_bytes(encode_state(doc))
        return submit(self.ws, self.owner, staging, state=state)

    def child(self, index: int, state: str | None, *, failure: dict[str, object] | None = None) -> JobDefinition:
        """A child at ``proj/k<index>`` in *state*; ``None`` never publishes it, ``"owned"`` claims it."""

        child = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), f"c{index}", f"proj/k{index}"))
        if state is None:
            return child
        doc = StateDoc.empty(child.id)
        if failure is not None:
            doc = doc.with_failure(failure)
        ref = self.put(child, "ready" if state == "owned" else state, doc)
        if state == "owned":
            assert claim(self.ws, self.owner, ref) is not None
        return child

    def parent(self, children: list[JobDefinition], condition: str, **join: object) -> JobRef:
        parent = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "parent", "proj"))
        references = [
            {"job_id": child.id, "job_key": child.job_key, "placement_hint": str(child.placement), "label": f"l{index}"}
            for index, child in enumerate(children)
        ]
        doc = StateDoc.empty(parent.id).updated(
            join={"children": references, "condition": condition, "on_impossible": None, "next_step": "next", **join}
        )
        return self.put(parent, "waiting", doc)

    def evaluate(self, parent: JobRef, since: dict[str, float] | None = None, *, now: float = 0.0, grace: float = 10):
        return evaluate(
            self.ws, parent, cache=ListingCache(), unresolved_since={} if since is None else since, grace=grace, now=now
        )


@pytest.fixture()
def world(tmp_path: Path) -> World:
    root = tmp_path / "ws"
    ws = FakeWorkspace(root, root / ".httk-workspace", root / "jobs", False, 0.05)
    return World(ws, register_owner(ws, kind="manager", label=None, allocation=None, advertised={}))


# (condition, count, kinds, satisfied, impossible)
TABLE = [
    ("all_succeeded", 0, ["succeeded", "succeeded"], True, False),
    ("all_succeeded", 0, ["succeeded", "ready"], False, False),
    ("all_succeeded", 0, ["succeeded", "cancelled"], False, True),
    ("all_terminal", 0, ["failed", "cancelled"], True, False),
    ("all_terminal", 0, ["failed", "pending"], False, False),
    ("any_succeeded", 0, ["failed", "succeeded"], True, False),
    ("any_succeeded", 0, ["failed", "waiting"], False, False),
    ("any_succeeded", 0, ["failed", "cancelled"], False, True),
    ("any_terminal", 0, ["pending", "failed"], True, False),
    ("any_terminal", 0, ["pending", "paused"], False, False),
    ("at_least", 2, ["succeeded", "succeeded", "failed"], True, False),
    ("at_least", 2, ["succeeded", "ready", "failed"], False, False),
    ("at_least", 2, ["succeeded", "failed", "failed"], False, True),
]


@pytest.mark.parametrize(("condition", "count", "kinds", "is_satisfied", "is_impossible"), TABLE)
def test_condition_logic(condition: str, count: int, kinds: list[str], is_satisfied: bool, is_impossible: bool) -> None:
    assert satisfied(condition, count, kinds) is is_satisfied
    assert impossible(condition, count, kinds) is is_impossible


def test_unknown_condition() -> None:
    assert len(CONDITIONS) == 5
    with pytest.raises(FormatError):
        satisfied("most", 0, [])
    with pytest.raises(FormatError):
        impossible("most", 0, [])


@pytest.mark.parametrize(("condition", "count", "kinds", "is_satisfied", "is_impossible"), TABLE)
def test_evaluate_on_the_filesystem(
    world: World, condition: str, count: int, kinds: list[str], is_satisfied: bool, is_impossible: bool
) -> None:
    children = [world.child(index, "owned" if kind == "pending" else kind) for index, kind in enumerate(kinds)]
    parent = world.parent(children, condition, count=count)
    decision = world.evaluate(parent)
    if not (is_satisfied or is_impossible):
        assert decision is None
        return
    assert decision is not None
    assert decision.kind == ("satisfied" if is_satisfied else "impossible")
    assert [o.state for o in decision.observations] == kinds
    assert [o.job_id for o in decision.observations] == [child.id for child in children]
    assert [o.label for o in decision.observations] == [f"l{index}" for index in range(len(kinds))]


def test_owned_child_is_pending_and_observations_carry_failures(world: World) -> None:
    failed = world.child(0, "failed", failure=FAILURE)
    owned = world.child(1, "owned")
    parent = world.parent([failed, owned], "any_terminal")
    decision = world.evaluate(parent)
    assert decision is not None and decision.kind == "satisfied"
    first, second = decision.observations
    assert first.as_mapping() == {
        "label": "l0",
        "job_id": failed.id,
        "job_key": failed.job_key,
        "placement": "proj/k0",
        "state": "failed",
        "token": first.token,
        "failure": FAILURE,
    }
    assert first.token is not None
    assert (second.state, second.token, second.failure) == ("pending", None, None)
    assert decision.unresolved == (owned.id,)
    # all_succeeded with the failed child is impossible however the owned one ends.
    decision = world.evaluate(world.parent([owned, failed], "all_succeeded"))
    assert decision is not None and decision.kind == "impossible"


def test_unresolved_child_needs_grace_and_an_exhaustive_miss(world: World) -> None:
    done = world.child(0, "succeeded")
    vanished = world.child(1, None)
    parent = world.parent([done, vanished], "all_succeeded")
    since: dict[str, float] = {}
    assert world.evaluate(parent, since, now=100.0) is None
    assert since == {vanished.id: 100.0}
    assert world.evaluate(parent, since, now=110.0) is None  # within the grace
    decision = world.evaluate(parent, since, now=110.5)
    assert decision is not None and decision.kind == "unresolvable"
    assert decision.unresolved == (vanished.id,)
    assert [o.state for o in decision.observations] == ["succeeded", "pending"]


def test_long_running_owned_child_restarts_the_grace(world: World) -> None:
    owned = world.child(0, "owned")
    parent = world.parent([owned], "all_succeeded")
    since = {owned.id: 0.0}
    assert world.evaluate(parent, since, now=50.0) is None
    assert since == {owned.id: 50.0}


def test_found_and_decided_children_leave_the_unresolved_map(world: World) -> None:
    child = world.child(0, "succeeded")
    since = {child.id: 0.0}
    decision = world.evaluate(world.parent([child], "all_succeeded"), since)
    assert decision is not None and decision.kind == "satisfied" and since == {}


def test_parent_without_a_join_or_state(world: World) -> None:
    plain = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "plain", "proj"))
    assert world.evaluate(world.put(plain, "waiting")) is None
    other = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "other", "proj"))
    assert world.evaluate(world.put(other, "waiting", StateDoc.empty(other.id))) is None


@pytest.mark.parametrize(
    "join",
    [
        {"condition": "most"},
        {"condition": "at_least", "count": -1},
        {"condition": "at_least", "count": True},
        {"children": []},
        {"children": [{"job_id": str(uuid.uuid4()), "job_key": "x"}]},
    ],
)
def test_malformed_joins_are_refused(world: World, join: dict[str, object]) -> None:
    child = world.child(0, "succeeded")
    parent = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "parent", "proj"))
    reference = {"job_id": child.id, "job_key": child.job_key, "placement_hint": "proj/k0"}
    doc = StateDoc.empty(parent.id).updated(join={"children": [reference], "condition": "all_succeeded", **join})
    with pytest.raises(FormatError):
        world.evaluate(world.put(parent, "waiting", doc))


def test_a_child_with_another_key_at_its_placement_is_refused(world: World) -> None:
    child = world.child(0, "succeeded")
    parent = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "parent", "proj"))
    wrong = {"job_id": child.id, "job_key": child.id, "placement_hint": "proj/k0"}
    doc = StateDoc.empty(parent.id).updated(join={"children": [wrong], "condition": "all_succeeded"})
    with pytest.raises(FormatError, match="not"):
        world.evaluate(world.put(parent, "waiting", doc))


def _consumer(world: World, parent_state: str, *, observed: bool) -> JobDefinition:
    parent = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "parent", "proj"))
    child = JobDefinition.from_mapping(
        job_mapping(
            str(uuid.uuid4()),
            "child",
            "proj/k0",
            parent={
                "workspace_id": str(uuid.uuid4()),
                "job_id": parent.id,
                "job_key": parent.job_key,
                "placement": "proj",
                "activation_id": str(uuid.uuid4()),
                "spawn_id": str(uuid.uuid4()),
            },
        )
    )
    observations = [{"job_id": child.id if observed else str(uuid.uuid4()), "state": "succeeded"}]
    ref = world.put(parent, "ready" if parent_state == "owned" else parent_state, StateDoc.empty(parent.id))
    if parent_state == "owned":
        owned = claim(world.ws, world.owner, ref)
        assert owned is not None
        owned.write_state(StateDoc.empty(parent.id).with_observations(observations))
    else:
        (ref.path / "state.json").write_bytes(encode_state(StateDoc.empty(parent.id).with_observations(observations)))
    return child


@pytest.mark.parametrize("parent_state", ["ready", "owned", "succeeded"])
def test_revival_guard(world: World, parent_state: str) -> None:
    child = _consumer(world, parent_state, observed=True)
    described = consumed_by_decided_join(world.ws, child)
    assert described is not None and described.startswith("parent--") and parent_state in described


def test_revival_guard_negatives(world: World) -> None:
    assert consumed_by_decided_join(world.ws, _consumer(world, "ready", observed=False)) is None
    orphan = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "orphan", "proj"))
    assert consumed_by_decided_join(world.ws, orphan) is None
    # A parent that is nowhere to be found consumed nothing.
    child = _consumer(world, "ready", observed=True)
    assert child.parent is not None
    elsewhere = JobDefinition.from_mapping({**child.as_mapping(), "parent": {**child.parent, "placement": "other"}})
    assert consumed_by_decided_join(world.ws, elsewhere, cache=ListingCache()) is None
