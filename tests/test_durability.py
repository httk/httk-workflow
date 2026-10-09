"""What a durable workspace synchronizes, and when.

The storage-durability contract promises that a durable workspace synchronizes
not only the owner-written ``state.json`` and the launch records, but the
runner-side artifacts that publish work: an outcome and its child bundles, and
the data a commit moves into place. These tests hold the implementation to that
promise two ways. An AST audit fixes every runner-side publication write to
name its durability rather than inherit the default, so a future call site that
forgets fails here rather than silently losing an outcome. Behavioural tests
then wrap ``os.fsync``, ``os.rename`` and ``os.replace`` to prove the
synchronizations happen, and happen *before* the rename or the launch gate that
makes the artifact authoritative — and that a non-durable workspace performs
none of them.
"""

import ast
import json
import os
import uuid
from pathlib import Path

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _kernel, _store
from httk.workflow._state import decode_state
from httk.workflow.runtime import AttemptContext
from httk.workflow.runtime_builders import OutcomeDraft

# ---------------------------------------------------------------------------
# The AST durability audit
# ---------------------------------------------------------------------------


def test_every_publication_write_names_its_durability() -> None:
    """No runner-side publication write may inherit the ``durable`` default.

    This is the runner-side counterpart of the transfer-ledger audit: every
    ``write_json_atomic`` in the builders and the authoring SDK must pass
    ``durable=`` explicitly, so a new outcome, child, or state write that forgets
    it fails this test rather than quietly reverting to process-interruption-only
    safety on a durable workspace.
    """

    from httk.workflow import registry, runtime_builders, sdk

    for module in (registry, runtime_builders, sdk):
        tree = ast.parse(Path(str(module.__file__)).read_text(encoding="utf-8"))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "write_json_atomic"
        ]
        assert calls, f"{module.__name__} publishes no protocol JSON"
        for call in calls:
            assert any(keyword.arg == "durable" for keyword in call.keywords), f"{module.__name__}: {ast.unparse(call)}"


def test_durable_global_workspace_registration_syncs_its_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow import registry

    monkeypatch.setattr(registry, "workspaces_path", lambda: tmp_path / "config" / "workspaces.json")
    events = _install_spies(monkeypatch)
    registry.register_workspace("durable", tmp_path / "workspace", durable=True)
    assert sum(event[0] == "fsync" for event in events) >= 2


def test_transaction_replay_accepts_a_durability_argument() -> None:
    """The runner-side workdir replay carries no ``write_json_atomic``; it fsyncs instead.

    Its durability is therefore a ``durable`` parameter its callers must be
    able to pass. Pinning the keyword-only parameter keeps that contract from
    being dropped in a refactor.
    """

    from httk.workflow import transactions

    tree = ast.parse(Path(str(transactions.__file__)).read_text(encoding="utf-8"))
    replay = next(
        node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "replay_transaction"
    )
    assert any(argument.arg == "durable" for argument in replay.args.kwonlyargs)


# ---------------------------------------------------------------------------
# Attempt-context round-trip
# ---------------------------------------------------------------------------


def _context(directory: Path, *, durable: bool | None) -> AttemptContext:
    """Build one attempt context, optionally omitting ``durable``."""

    directory.mkdir(parents=True, exist_ok=True)
    document: dict[str, object] = {
        "format": "httk-workflow-attempt-context",
        "deadline": None,
        "settings": {},
        "format_version": 2,
        "workspace_id": str(uuid.uuid4()),
        "job_id": str(uuid.uuid4()),
        "job_key": "durable--" + str(uuid.uuid4()),
        "placement": "project/jobs",
        "payload": str(directory.parent / "payload"),
        "step": "start",
        "activation_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "resources": {},
    }
    if durable is not None:
        document["durable"] = durable
    return AttemptContext.from_mapping(document)


def test_attempt_context_round_trips_durable_and_requires_it(tmp_path: Path) -> None:
    assert _context(tmp_path / "on", durable=True).durable is True
    assert _context(tmp_path / "off", durable=False).durable is False
    # The manager always records the durability; a context without it is malformed, never silently non-durable.
    with pytest.raises(ValueError, match="durable"):
        _context(tmp_path / "absent", durable=None)


# ---------------------------------------------------------------------------
# The fsync/rename spy
# ---------------------------------------------------------------------------


def _install_spies(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Record every ``fsync``, ``rename`` and ``replace`` by resolved path, and every launch-gate write.

    ``os.fsync`` is given a descriptor, so the target is resolved through
    ``/proc/self/fd``; a durable synchronization thus appears in the timeline as
    the very file or directory it flushed, which is what lets a test assert one
    happened before a later rename.
    """

    events: list[tuple[str, ...]] = []
    real_fsync = os.fsync
    real_rename = os.rename
    real_replace = os.replace
    real_write = os.write

    def spy_fsync(fd: int) -> None:
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:  # pragma: no cover - defensive; /proc is present on Linux
            target = ""
        events.append(("fsync", target))
        return real_fsync(fd)

    def spy_rename(src: str | os.PathLike[str], dst: str | os.PathLike[str], *args: object, **keywords: object) -> None:
        events.append(("rename", os.fspath(src), os.fspath(dst)))
        return real_rename(src, dst, *args, **keywords)  # type: ignore[arg-type]

    def spy_replace(
        src: str | os.PathLike[str], dst: str | os.PathLike[str], *args: object, **keywords: object
    ) -> None:
        events.append(("replace", os.fspath(src), os.fspath(dst)))
        return real_replace(src, dst, *args, **keywords)  # type: ignore[arg-type]

    def spy_write(fd: int, data: bytes) -> int:
        if data == b"R":
            events.append(("gate", str(fd)))
        return real_write(fd, data)

    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(os, "rename", spy_rename)
    monkeypatch.setattr(os, "replace", spy_replace)
    monkeypatch.setattr(os, "write", spy_write)
    return events


def _indices(events: list[tuple[str, ...]], kind: str, predicate: object = None) -> list[int]:
    return [
        index
        for index, event in enumerate(events)
        if event[0] == kind and (predicate is None or predicate(event))  # type: ignore[operator]
    ]


# ---------------------------------------------------------------------------
# Outcome publication
# ---------------------------------------------------------------------------


def _draft(tmp_path: Path, *, durable: bool) -> tuple[OutcomeDraft, Path]:
    """Return an outcome draft staging one extra file, ready to publish, and its control directory."""

    control = tmp_path / "control"
    control.mkdir()
    draft = OutcomeDraft(_context(tmp_path / "attempt", durable=durable), control, durable=durable)
    (draft.root / "staged").mkdir()
    (draft.root / "staged" / "energy.json").write_text('{"energy": 1}', encoding="utf-8")
    return draft, control


def test_a_durable_outcome_publish_syncs_the_draft_before_the_ready_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft, control = _draft(tmp_path, durable=True)

    events = _install_spies(monkeypatch)
    draft.publish("succeed")

    publications = _indices(events, "rename", lambda event: event[2].endswith("outcome.ready"))
    assert len(publications) == 1, "the outcome is published by exactly one directory rename"
    published_at = publications[0]

    # Every file the draft staged — the outcome and anything staged beside it — is flushed before the rename
    # that makes the outcome authoritative.
    draft_syncs = _indices(events, "fsync", lambda event: "outcome.tmp" in event[1])
    assert draft_syncs, "the draft tree must be synchronized"
    assert max(draft_syncs) < published_at
    staged = _indices(events, "fsync", lambda event: event[1].endswith(f"staged{os.sep}energy.json"))
    assert staged, "a staged plain file must be synchronized, not only the JSON"

    # The rename installed the outcome.ready name; the control directory is flushed afterwards so that name
    # itself survives a crash.
    control_real = os.path.realpath(control)
    control_syncs = _indices(events, "fsync", lambda event: event[1] == control_real)
    assert control_syncs and min(control_syncs) > published_at


def test_a_nondurable_outcome_publish_performs_no_draft_syncs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    draft, control = _draft(tmp_path, durable=False)

    events = _install_spies(monkeypatch)
    draft.publish("succeed")

    assert _indices(events, "rename", lambda event: event[2].endswith("outcome.ready"))
    assert not _indices(events, "fsync", lambda event: "outcome.tmp" in event[1])
    control_real = os.path.realpath(control)
    assert not _indices(events, "fsync", lambda event: event[1] == control_real)


# ---------------------------------------------------------------------------
# The manager: launch gate and commit
# ---------------------------------------------------------------------------


def _workspace(root: Path, *, durable: bool) -> Workspace:
    return Workspace.initialize(root, durable=durable, policy={"visibility_deadline_seconds": 0.05})


def _run(ws: Workspace) -> None:
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)


@pytest.mark.slow
def test_a_durable_running_phase_and_launch_record_are_synced_before_the_launch_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path / "ws", durable=True)
    installed = h.install(ws, tmp_path / "demo")
    submitted = h.submit(ws, installed, {"start": "succeed"})

    events = _install_spies(monkeypatch)
    phases: list[object] = []
    real_write = os.write

    def at_the_gate(fd: int, data: bytes) -> int:
        if data == b"R":
            # What a crash right at the gate would leave on disk.
            (owned,) = (ws.jobs / "owned").glob("*/*")
            phases.append(decode_state((owned / "state.json").read_bytes()).phase["kind"])
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", at_the_gate)
    _run(ws)
    assert h.find(ws, submitted.job_id).state == "succeeded"
    assert phases == ["running"]

    gates = _indices(events, "gate")
    assert len(gates) == 1
    gate = gates[0]
    # The running phase reached state.json by a synchronized write: the temporary is flushed, replaces
    # state.json, and the job directory is flushed, all before the gate opens.
    state_replaces = _indices(events, "replace", lambda event: event[2].endswith(f"{os.sep}state.json"))
    running = max(index for index in state_replaces if index < gate)
    owned_job = os.path.dirname(events[running][2])
    assert _indices(events, "fsync", lambda event: os.path.basename(event[1]).startswith(".state.json."))
    assert [index for index in _indices(events, "fsync", lambda event: event[1] == owned_job) if running < index < gate]
    # So did the launch record a probe of a dead owner relies on.
    records = _indices(events, "replace", lambda event: event[2].endswith(f"{os.sep}process.json"))
    assert records and max(records) < gate
    launches = os.path.dirname(events[records[0]][2])
    assert [index for index in _indices(events, "fsync", lambda event: event[1] == launches) if index < gate]


@pytest.mark.slow
def test_a_durable_commit_syncs_the_applied_data_before_the_release_moves_the_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path / "ws", durable=True)
    installed = h.install(ws, tmp_path / "demo")
    puts = {"data/results/energy.json": '{"energy": 1}'}
    submitted = h.submit(
        ws, installed, {"start": "succeed"}, members={"data/README": "inputs\n"}, parameters={"put": {"start": puts}}
    )

    events = _install_spies(monkeypatch)
    _run(ws)

    done = h.find(ws, submitted.job_id)
    assert done.state == "succeeded"
    assert (done.path / "data" / "results" / "energy.json").read_text(encoding="utf-8") == '{"energy": 1}'
    # The committed entry was moved into data/ by the manager, and both directories it changed were flushed...
    (applied,) = _indices(events, "rename", lambda event: event[2].endswith(f"{os.sep}data{os.sep}results"))
    owned_data = os.path.dirname(events[applied][2])
    data_syncs = [index for index in _indices(events, "fsync", lambda event: event[1] == owned_data) if index > applied]
    assert data_syncs, "a durable commit must synchronize the directory the committed data entered"
    # ...before the exact rename that carries the job out of owned/.
    released = _indices(
        events,
        "rename",
        lambda event: (
            f"{os.sep}jobs{os.sep}owned{os.sep}" in event[1] and f"{os.sep}jobs{os.sep}succeeded{os.sep}" in event[2]
        ),
    )
    assert len(released) == 1
    assert max(data_syncs) < released[0]
    # The decision point came first: the commit intent was synchronized before any committed entry moved.
    intents = [
        index
        for index in _indices(events, "replace", lambda event: event[2].endswith(f"{os.sep}state.json"))
        if index < applied
    ]
    assert intents


@pytest.mark.slow
def test_reputting_a_tree_onto_committed_data_replaces_it_instead_of_failing(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws", durable=False)
    installed = h.install(ws, tmp_path / "demo")
    submitted = h.submit(
        ws,
        installed,
        {"start": "advance:finish", "finish": "succeed"},
        parameters={
            "put": {
                "start": {"data/results/bundle/value.txt": "v1", "data/results/bundle/first.txt": "kept"},
                "finish": {"data/results/bundle/value.txt": "v2"},
            }
        },
    )
    _run(ws)

    done = h.find(ws, submitted.job_id)
    assert done.state == "succeeded"
    bundle = done.path / "data" / "results" / "bundle"
    # The second tree merged into the first: its file replaced the committed one, the rest stayed.
    assert (bundle / "value.txt").read_text(encoding="utf-8") == "v2"
    assert (bundle / "first.txt").read_text(encoding="utf-8") == "kept"


@pytest.mark.slow
def test_a_nondurable_commit_does_not_sync_the_committed_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path / "ws", durable=False)
    installed = h.install(ws, tmp_path / "demo")
    puts = {"data/results/energy.json": '{"energy": 1}'}
    submitted = h.submit(ws, installed, {"start": "succeed"}, parameters={"put": {"start": puts}})

    events = _install_spies(monkeypatch)
    _run(ws)

    done = h.find(ws, submitted.job_id)
    assert done.state == "succeeded"
    # The transaction is still applied; only its synchronization is skipped, as is every other job write's.
    assert (done.path / "data" / "results" / "energy.json").read_text(encoding="utf-8") == '{"energy": 1}'
    jobs = os.path.realpath(ws.jobs)
    assert not _indices(events, "fsync", lambda event: event[1].startswith(jobs + os.sep) or event[1] == jobs)


#: Records the durability the attempt was launched with: the language-neutral variable and the context member.
_DURABLE_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
pathlib.Path("durable.json").write_text(
    json.dumps({"env": os.environ.get("HTTK_WORKFLOW_DURABLE"), "context": context["durable"]})
)
draft = control / "outcome.tmp.x"
draft.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="succeed")
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.mark.slow
@pytest.mark.parametrize("durable", [True, False])
def test_the_runner_environment_carries_the_workspace_durability(tmp_path: Path, durable: bool) -> None:
    ws = _workspace(tmp_path / "ws", durable=durable)
    installed: _store.Installed = h.install(ws, tmp_path / "durable", executables={"run": _DURABLE_RUNNER})
    submitted = h.submit(ws, installed, {"start": "succeed"})
    _run(ws)

    done: _kernel.JobRef = h.find(ws, submitted.job_id)
    assert done.state == "succeeded"
    recorded = json.loads((done.path / "run" / "durable.json").read_text(encoding="utf-8"))
    # HTTK_WORKFLOW_DURABLE is the language-neutral contract; the context is the source of truth. Both must agree
    # with the workspace they were launched by.
    assert recorded["env"] == ("1" if durable else "0")
    assert recorded["context"] is durable
