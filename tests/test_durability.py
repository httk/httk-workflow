"""What a durable workspace synchronizes on the runner side, and when.

The storage-durability contract promises that a durable workspace synchronizes
not only its journal, markers, and protocol JSON, but the runner-side artifacts
that publish work: an outcome, its transaction, and its child bundles. These
tests hold the implementation to that promise two ways. An AST audit fixes every
publication write to name its durability rather than inherit the default, so a
future call site that forgets fails here rather than silently losing an outcome.
Behavioural tests then wrap ``os.fsync``, ``os.rename``, and ``os.replace`` to
prove the synchronizations happen, and happen *before* the rename that makes the
artifact authoritative — and that a non-durable workspace performs none of them.
"""

import ast
import json
import os
import uuid
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.runtime import AttemptContext
from httk.workflow.runtime_builders import OutcomeDraft

_SRC = str(Path(__file__).parents[1] / "src")


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
    """Transaction replay carries no ``write_json_atomic``; it fsyncs instead.

    Its durability is therefore a ``durable`` parameter the manager must be able
    to pass, so the committed data is synchronized before the marker rename that
    claims it. Pinning the keyword-only parameter keeps that contract from being
    dropped in a refactor.
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


def _write_context(
    directory: Path,
    *,
    durable: bool | None,
    data_generation: int | None = None,
) -> AttemptContext:
    """Write and read one attempt context, optionally omitting ``durable``."""

    directory.mkdir(parents=True, exist_ok=True)
    document: dict[str, object] = {
        "format": "httk-workflow-attempt-context",
        "format_version": 2,
        "workspace_id": str(uuid.uuid4()),
        "job_id": str(uuid.uuid4()),
        "job_key": "durable--" + str(uuid.uuid4()),
        "placement": "project/jobs",
        "payload": str(directory.parent / "payload"),
        "step": "start",
        "activation_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "data_generation": data_generation,
        "resources": {},
    }
    if durable is not None:
        document["durable"] = durable
    path = directory / "context.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return AttemptContext.from_path(path)


def test_attempt_context_round_trips_durable_and_tolerates_absence(tmp_path: Path) -> None:
    assert _write_context(tmp_path / "on", durable=True).durable is True
    assert _write_context(tmp_path / "off", durable=False).durable is False
    # A context written before the member existed reads as non-durable rather
    # than failing, so an old attempt-control directory still loads.
    assert _write_context(tmp_path / "absent", durable=None).durable is False


# ---------------------------------------------------------------------------
# The fsync/rename spy
# ---------------------------------------------------------------------------


def _install_spies(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Record every ``fsync``, ``rename``, and ``replace`` by resolved path.

    ``os.fsync`` is given a descriptor, so the target is resolved through
    ``/proc/self/fd``; a durable synchronization thus appears in the timeline as
    the very file or directory it flushed, which is what lets a test assert one
    happened before a later rename.
    """

    events: list[tuple[str, ...]] = []
    real_fsync = os.fsync
    real_rename = os.rename
    real_replace = os.replace

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

    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(os, "rename", spy_rename)
    monkeypatch.setattr(os, "replace", spy_replace)
    return events


# ---------------------------------------------------------------------------
# Outcome publication
# ---------------------------------------------------------------------------


def _draft_with_transaction(context: AttemptContext, control: Path, workdir: Path, *, durable: bool) -> OutcomeDraft:
    """Return an outcome draft staging one transactional file, ready to publish."""

    draft = OutcomeDraft(context, control, durable=durable)
    transaction = draft.transaction()
    source = workdir / "energy.json"
    source.write_text('{"energy": 1}', encoding="utf-8")
    transaction.put_file("op-0001", str(source), "results/energy.json")
    return draft


def test_a_durable_outcome_publish_syncs_the_draft_before_the_ready_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _write_context(tmp_path / "attempt", durable=True, data_generation=0)
    control = tmp_path / "control"
    control.mkdir()
    draft = _draft_with_transaction(context, control, tmp_path, durable=True)

    events = _install_spies(monkeypatch)
    draft.publish("succeed")

    publications = [
        index for index, event in enumerate(events) if event[0] == "rename" and event[2].endswith("outcome.ready")
    ]
    assert len(publications) == 1, "the outcome is published by exactly one directory rename"
    published_at = publications[0]

    # Every file the draft staged — the outcome, the sealed manifest, the copied
    # payload — is flushed before the rename that makes the outcome authoritative.
    draft_syncs = [index for index, event in enumerate(events) if event[0] == "fsync" and "outcome.tmp" in event[1]]
    assert draft_syncs, "the draft tree must be synchronized"
    assert max(draft_syncs) < published_at
    # The staged payload copied by ``put_file`` is a plain file, not JSON, and it
    # is flushed too — durability is the whole draft tree, not only its JSON.
    staged_payload = [
        event for event in events if event[0] == "fsync" and f"transaction{os.sep}payload{os.sep}" in event[1]
    ]
    assert staged_payload, "the staged transaction payload must be synchronized, not only the JSON"

    # The rename installed the outcome.ready name; the control directory is
    # flushed afterwards so that name itself survives a crash.
    control_real = os.path.realpath(control)
    control_syncs = [index for index, event in enumerate(events) if event[0] == "fsync" and event[1] == control_real]
    assert control_syncs and min(control_syncs) > published_at


def test_a_nondurable_outcome_publish_performs_no_draft_syncs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context = _write_context(tmp_path / "attempt", durable=False, data_generation=0)
    control = tmp_path / "control"
    control.mkdir()
    draft = _draft_with_transaction(context, control, tmp_path, durable=False)

    events = _install_spies(monkeypatch)
    draft.publish("succeed")

    assert any(event[0] == "rename" and event[2].endswith("outcome.ready") for event in events)
    assert not any(event[0] == "fsync" and "outcome.tmp" in event[1] for event in events)
    control_real = os.path.realpath(control)
    assert not any(event[0] == "fsync" and event[1] == control_real for event in events)


# ---------------------------------------------------------------------------
# End-to-end: transaction commit and the runner environment
# ---------------------------------------------------------------------------


_TRANSACTION_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.durable")


@run.step
def start(a):
    source = a.workdir / "energy.json"
    source.write_text('{{"energy": 1}}', encoding="utf-8")
    a.put(str(source), "results/energy.json")
    a.succeed()


raise SystemExit(run.main())
'''


_REPUT_TREE_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.durable")


@run.step
def start(a):
    bundle = a.workdir / "bundle"
    bundle.mkdir(exist_ok=True)
    (bundle / "value.txt").write_text("v1", encoding="utf-8")
    a.put(str(bundle), "results/bundle")
    if a.state.get("again"):
        a.succeed()
    else:
        a.advance("start", state={{"again": True}})


raise SystemExit(run.main())
'''


_DURABLE_RUNNER = f'''#!/usr/bin/env python3
import json
import os
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.durable")


@run.step
def start(a):
    (a.workdir / "durable.json").write_text(
        json.dumps({{"env": os.environ.get("HTTK_WORKFLOW_DURABLE"), "context": a.context.durable}}),
        encoding="utf-8",
    )
    a.succeed()


raise SystemExit(run.main())
'''


def _prepare(root: Path, runner_source: str, *, data_mode: str = "none") -> tuple[Path, str]:
    """Prepare a payload whose own runner implements one ``start`` step."""

    payload = root / "payload"
    payload.mkdir(parents=True)
    runner = payload / "runner.py"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Durable job",
            workflow="tests.durable",
            runner_path="runner.py",
            initial_step="start",
            data_mode=data_mode,  # type: ignore[arg-type]
            maximum_attempts_per_activation=1,
        ),
    )
    return payload, job.id


def _run(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)


def test_a_durable_commit_syncs_the_transaction_before_the_committing_marker_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=True)
    payload, job_id = _prepare(tmp_path / "source", _TRANSACTION_RUNNER, data_mode="transactional")
    workspace.submit(payload, "project/jobs")

    events = _install_spies(monkeypatch)
    _run(workspace)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    data = workspace.payload_path(marker.placement, marker.job_key) / "data"
    assert (data / "results" / "energy.json").read_text(encoding="utf-8") == '{"energy": 1}'

    # The committed data file is flushed by the manager during replay...
    committed = [
        index
        for index, event in enumerate(events)
        if event[0] == "fsync" and event[1].endswith("data/results/energy.json")
    ]
    assert committed, "a durable commit must synchronize the committed data file"
    # ...before the exact rename that carries the marker out of committing.
    left_committing = [
        index
        for index, event in enumerate(events)
        if event[0] == "rename" and f"{os.sep}state{os.sep}committing{os.sep}" in event[1]
    ]
    assert left_committing, "the marker must leave committing by a rename"
    assert max(committed) < min(left_committing)


def test_reputting_a_tree_onto_committed_data_replaces_it_instead_of_failing(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _prepare(tmp_path / "source", _REPUT_TREE_RUNNER, data_mode="transactional")
    workspace.submit(payload, "project/jobs")

    _run(workspace)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    data = workspace.payload_path(marker.placement, marker.job_key) / "data"
    assert (data / "results" / "bundle" / "value.txt").read_text(encoding="utf-8") == "v1"


def test_a_nondurable_commit_does_not_sync_the_committed_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _prepare(tmp_path / "source", _TRANSACTION_RUNNER, data_mode="transactional")
    workspace.submit(payload, "project/jobs")

    events = _install_spies(monkeypatch)
    _run(workspace)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    data = workspace.payload_path(marker.placement, marker.job_key) / "data"
    # The transaction is still applied; only its synchronization is skipped.
    assert (data / "results" / "energy.json").read_text(encoding="utf-8") == '{"energy": 1}'
    assert not any(event[0] == "fsync" and event[1].endswith("data/results/energy.json") for event in events)


@pytest.mark.parametrize("durable", [True, False])
def test_the_runner_environment_carries_the_workspace_durability(tmp_path: Path, durable: bool) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=durable)
    payload, job_id = _prepare(tmp_path / "source", _DURABLE_RUNNER)
    workspace.submit(payload, "project/jobs")
    _run(workspace)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    recorded = json.loads(
        (workspace.payload_path(marker.placement, marker.job_key) / "run" / "durable.json").read_text(encoding="utf-8")
    )
    # HTTK_WORKFLOW_DURABLE is the language-neutral contract; the context is the
    # source of truth. Both must agree with the workspace they were launched by.
    assert recorded["env"] == ("1" if durable else "0")
    assert recorded["context"] is durable
