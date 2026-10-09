"""Unit tests of :mod:`httk.workflow._children` (child validation and publication, note §7.3) on a real filesystem."""

import json
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow._children import ChildPlan, labeled_join, publish_children, validate_children
from httk.workflow._job import JobDefinition
from httk.workflow._kernel import OwnedJob, claim, register_owner, submit
from httk.workflow._state import StateDoc, read_state_unowned
from httk.workflow.errors import FormatError, UnsupportedExtensionError, WorkflowError

WORKSPACE_ID = "2d7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e03"
ATTEMPT = "0b5c1f8e-4d2a-4c55-9a43-0d8f7e6a1b23"
EXCHANGE_NAME = "5a7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e06"


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


def job_mapping(job_id: str, tag: str, placement: str, **changes: object) -> dict[str, object]:
    mapping: dict[str, object] = {
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
        "parent": None,
        "seal_succeeded": None,
    }
    mapping.update(changes)
    return mapping


@dataclass
class Setup:
    ws: FakeWorkspace
    job: OwnedJob
    parent: JobDefinition
    doc: StateDoc
    outcome: Path

    def spawn(self, count: int = 2, **child_changes: object) -> list[dict[str, object]]:
        """Stage *count* valid children and their spawn.json; return the spawn entries."""

        entries: list[dict[str, object]] = []
        for index in range(count):
            child_id, spawn_id = str(uuid.uuid4()), str(uuid.uuid4())
            parent = {
                "workspace_id": WORKSPACE_ID,
                "job_id": self.parent.id,
                "job_key": self.parent.job_key,
                "placement": "proj",
                "activation_id": str(uuid.uuid4()),
                "spawn_id": spawn_id,
            }
            child = JobDefinition.from_mapping(
                job_mapping(child_id, f"c{index}", f"proj/k{index}", parent=parent, **child_changes)
            )
            directory = self.outcome / "children" / "jobs" / child.job_key
            directory.mkdir(parents=True)
            (directory / "job.json").write_bytes(child.encode())
            (directory / "input.txt").write_text(f"input {index}")
            entries.append(
                {
                    "workspace_id": WORKSPACE_ID,
                    "job_id": child_id,
                    "job_key": child.job_key,
                    "label": f"branch-{index}",
                    "placement": f"proj/k{index}",
                    "spawn_id": spawn_id,
                }
            )
        self.write_spawn(entries)
        return entries

    def write_spawn(self, entries: list[dict[str, object]], **document: object) -> None:
        spawn = {"format": "httk-workflow-spawn", "format_version": 2, "children": entries, **document}
        (self.outcome / "children").mkdir(parents=True, exist_ok=True)
        (self.outcome / "children" / "spawn.json").write_text(json.dumps(spawn))

    def child_dir(self, entry: dict[str, object]) -> Path:
        return self.outcome / "children" / "jobs" / str(entry["job_key"])

    def validate(self) -> list[ChildPlan]:
        return validate_children(self.job, self.parent, self.doc, self.outcome, workspace_id=WORKSPACE_ID)


@pytest.fixture()
def setup(tmp_path: Path) -> Setup:
    root = tmp_path / "ws"
    ws = FakeWorkspace(root, root / ".httk-workspace", root / "jobs", False, 0.2)
    owner = register_owner(ws, kind="manager", label=None, allocation=None, advertised={})
    parent = JobDefinition.from_mapping(job_mapping(str(uuid.uuid4()), "parent", "proj"))
    staging = owner.scratch("build") / "job"
    staging.mkdir(parents=True)
    (staging / "job.json").write_bytes(parent.encode())
    job = claim(ws, owner, submit(ws, owner, staging))
    assert job is not None
    doc = StateDoc.empty(parent.id).updated(origin="exchange", exchange_name=EXCHANGE_NAME)
    outcome = job.path / "attempts" / ATTEMPT / "outcome.ready"
    outcome.mkdir(parents=True)
    return Setup(ws, job, parent, doc, outcome)


def test_valid_spawn_set_gives_the_plan(setup: Setup) -> None:
    entries = setup.spawn()
    plans = setup.validate()
    assert [plan.job_key for plan in plans] == [entry["job_key"] for entry in entries]
    first = plans[0]
    assert (first.label, first.placement, first.priority, first.spawn_id) == (
        "branch-0",
        "proj/k0",
        500,
        entries[0]["spawn_id"],
    )
    assert first.staged == f"attempts/{ATTEMPT}/outcome.ready/children/jobs/{first.job_key}"
    state = StateDoc.from_mapping(first.initial_state)
    assert (state.job_id, state.origin, state.exchange_name, state.owner_id) == (
        first.job_id,
        "exchange",
        EXCHANGE_NAME,
        None,
    )
    # The plan round-trips through the commit intent in state.json, frozen there.
    committed = setup.doc.with_commit({"children": [plan.as_mapping() for plan in plans]})
    assert committed.commit is not None
    children = committed.commit["children"]
    assert isinstance(children, tuple)
    assert [ChildPlan.from_mapping(item) for item in children] == plans


def test_no_children(setup: Setup) -> None:
    assert setup.validate() == []
    (setup.outcome / "children").mkdir()
    assert setup.validate() == []


def test_requires_quiescence(setup: Setup) -> None:
    setup.spawn()
    setup.job.begin_attempt(str(uuid.uuid4()))
    with pytest.raises(WorkflowError, match="still live"):
        setup.validate()


def _refuse(setup: Setup, change: Callable[[Setup, list[dict[str, object]]], object], match: str) -> None:
    entries = setup.spawn()
    change(setup, entries)
    with pytest.raises(FormatError, match=match):
        setup.validate()


def _rewrite_child(setup: Setup, entry: dict[str, object], **changes: object) -> None:
    path = setup.child_dir(entry) / "job.json"
    mapping = json.loads(path.read_text())
    mapping.update(changes)
    path.write_text(json.dumps(mapping))


def _symlink_in_place(path: Path) -> None:
    """Move *path* aside and leave a symlink to it in its place."""

    path.rename(path.with_name(path.name + ".real"))
    path.symlink_to(path.with_name(path.name + ".real"))


def _parent_change(member: str, value: object) -> Callable[[Setup, list[dict[str, object]]], object]:
    def change(setup: Setup, entries: list[dict[str, object]]) -> None:
        mapping = json.loads((setup.child_dir(entries[1]) / "job.json").read_text())
        _rewrite_child(setup, entries[1], parent={**mapping["parent"], member: value})

    return change


REFUSALS: dict[str, tuple[Callable[[Setup, list[dict[str, object]]], object], str]] = {
    "missing child directory": (lambda s, e: os.rename(s.child_dir(e[1]), s.outcome / "gone"), "no payload"),
    "symlinked child directory": (lambda s, e: _symlink_in_place(s.child_dir(e[0])), "symlink"),
    "fifo": (lambda s, e: os.mkfifo(s.child_dir(e[0]) / "pipe"), "special file"),
    "hard link": (lambda s, e: os.link(s.child_dir(e[0]) / "input.txt", s.outcome / "link"), "hard-linked"),
    "trusted state.json": (lambda s, e: (s.child_dir(e[0]) / "state.json").write_text("{}"), "trusted"),
    "trusted logs": (lambda s, e: (s.child_dir(e[1]) / "logs").mkdir(), "trusted"),
    "trusted seal.json": (lambda s, e: (s.child_dir(e[1]) / "seal.json").write_text("{}"), "trusted"),
    "trusted attempts": (lambda s, e: (s.child_dir(e[1]) / "attempts").mkdir(), "trusted"),
    "key mismatch": (lambda s, e: _rewrite_child(s, e[0], tag="other"), "names"),
    "placement mismatch": (lambda s, e: _rewrite_child(s, e[0], placement="proj/elsewhere"), "placement"),
    "no parent": (lambda s, e: _rewrite_child(s, e[0], parent=None), "parent"),
    "parent job_id": (_parent_change("job_id", str(uuid.uuid4())), "job_key does not carry"),
    "parent placement": (_parent_change("placement", "other"), "parent"),
    "parent workspace": (_parent_change("workspace_id", str(uuid.uuid4())), "parent"),
    "parent spawn id": (_parent_change("spawn_id", str(uuid.uuid4())), "parent"),
    "duplicate label": (lambda s, e: s.write_spawn([e[0], {**e[1], "label": "branch-0"}]), "not unique"),
    "duplicate key": (lambda s, e: s.write_spawn([e[0], {**e[0], "label": "x"}]), "repeated"),
    "entry placement": (lambda s, e: s.write_spawn([{**e[0], "placement": "proj/k1"}]), "placement"),
    "entry job_id": (lambda s, e: s.write_spawn([{**e[0], "job_id": e[1]["job_id"]}]), "job_id"),
    "bad label": (lambda s, e: s.write_spawn([{**e[0], "label": "Bad Label"}]), "label"),
    "no label": (lambda s, e: s.write_spawn([{k: v for k, v in e[0].items() if k != "label"}]), "members"),
    "unknown entry member": (lambda s, e: s.write_spawn([{**e[0], "extra": 1}]), "members"),
    "spawn not json": (lambda s, e: (s.outcome / "children" / "spawn.json").write_text("{"), "not JSON"),
    "spawn not an object": (lambda s, e: (s.outcome / "children" / "spawn.json").write_text("[]"), "object"),
    "spawn version": (lambda s, e: s.write_spawn(e, format_version=1), "version"),
    "spawn children": (lambda s, e: s.write_spawn(e, children={}), "array"),
    "spawn symlink": (lambda s, e: _symlink_in_place(s.outcome / "children" / "spawn.json"), "symlink"),
    "children symlink": (lambda s, e: _symlink_in_place(s.outcome / "children"), "symlink"),
}


@pytest.mark.parametrize("case", sorted(REFUSALS))
def test_refusals(setup: Setup, case: str) -> None:
    change, match = REFUSALS[case]
    _refuse(setup, change, match)


def test_cross_workspace_child_is_unsupported(setup: Setup) -> None:
    entries = setup.spawn()
    setup.write_spawn([{**entries[0], "workspace_id": str(uuid.uuid4())}])
    with pytest.raises(UnsupportedExtensionError):
        setup.validate()


def test_publish_and_replay_never_revalidates(setup: Setup) -> None:
    entries = setup.spawn(3)
    plans = setup.validate()
    # A staged child that already carries an owner-written state.json (as a replay finds it) fails
    # validation, so publication must never re-run it.
    (setup.child_dir(entries[1]) / "state.json").write_text("planted")
    with pytest.raises(FormatError, match="trusted"):
        setup.validate()
    # A crash after the first child: the replay reads the plans back from the intent.
    first = publish_children(setup.job, plans[:1])
    assert [ref.job_key for ref in first] == [entries[0]["job_key"]]
    replayed = [ChildPlan.from_mapping(plan.as_mapping()) for plan in plans]
    rest = publish_children(setup.job, replayed)
    assert [ref.job_key for ref in rest] == [entries[1]["job_key"], entries[2]["job_key"]]
    assert publish_children(setup.job, replayed) == []
    for index, ref in enumerate((*first, *rest)):
        assert ref.state == "ready" and ref.path.parent == setup.ws.jobs / "ready" / "proj" / f"k{index}"
        assert (ref.path / "input.txt").read_text() == f"input {index}"
        state, damaged = read_state_unowned(ref.path / "state.json")
        assert state is not None and not damaged
        assert (state.job_id, state.origin, state.exchange_name) == (ref.job_id, "exchange", EXCHANGE_NAME)
    assert not (setup.outcome / "children" / "jobs").exists() or not any(
        (setup.outcome / "children" / "jobs").iterdir()
    )


@pytest.mark.parametrize(
    ("member", "value"),
    [
        ("staged", "../escape"),
        ("staged", f"attempts/{ATTEMPT}/outcome.ready/children/jobs/other"),
        ("priority", 1000),
        ("placement", "a//b"),
        ("job_id", str(uuid.uuid4())),
        ("initial_state", {}),
    ],
)
def test_plan_from_mapping_refuses_malformed_entries(setup: Setup, member: str, value: object) -> None:
    setup.spawn(1)
    (plan,) = setup.validate()
    with pytest.raises(FormatError):
        ChildPlan.from_mapping({**plan.as_mapping(), member: value})
    with pytest.raises(FormatError):
        ChildPlan.from_mapping({**plan.as_mapping(), "extra": 1})


def test_labeled_join_fills_missing_labels(setup: Setup) -> None:
    entries = setup.spawn()
    plans = setup.validate()
    join = {
        "condition": "all_succeeded",
        "children": [{"job_key": entries[0]["job_key"]}, {"job_key": entries[1]["job_key"], "label": "own"}, 7],
    }
    labeled = labeled_join(join, plans)
    assert labeled["children"] == [
        {"job_key": entries[0]["job_key"], "label": "branch-0"},
        {"job_key": entries[1]["job_key"], "label": "own"},
        7,
    ]
    assert labeled_join({"children": None}, plans) == {"children": None}
