"""Unlocked jobs (debug mode): the ``unlock`` request, its runner copy, and the relaxed policies it carries."""

import json
import os
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as v3
from httk.workflow import Workspace, _kernel, _moving, _requests, _store, removal
from httk.workflow._job import JobDefinition
from httk.workflow.packages import source_tree_digest
from test_runner_builds import _compiled_package

COPY = _store.RUNNER_COPY


def _request(workspace: Workspace, job_id: str, action: str, **options: Any) -> str | None:
    """Post one request and apply it now as a CLI owner (as ``request_now``); ``None`` when it applied."""

    ref = v3.find(workspace, job_id)
    path = _requests.post(
        workspace,
        action=action,
        job_id=job_id,
        placement=JobDefinition.from_path(ref.path / "job.json").placement,
        operator="cli",
        reason="debug",
        **options,
    )
    with v3.cli_owner(workspace) as owner:
        (outcome,) = removal.serve_requests(workspace, [(ref, path)], owner=owner)
    return None if outcome.status == "applied" else outcome.explanation


def _failed(workspace: Workspace, installed: _store.Installed, placement: str) -> _kernel.JobRef:
    ref = v3.submit(workspace, installed, {"start": "fail"}, placement=placement)
    v3.run(workspace)
    assert v3.find(workspace, ref.job_id).state == "failed"
    return ref


def _edit_job(ref: _kernel.JobRef, script: dict[str, str]) -> str:
    """Rewrite the job's ``job.json`` with another script; return the new digest."""

    path = ref.path / "job.json"
    mapping = json.loads(path.read_text(encoding="utf-8"))
    mapping["parameters"]["script"] = script
    path.write_text(json.dumps(mapping), encoding="utf-8")
    return JobDefinition.from_path(path).digest


def test_unlock_copies_the_runner_once(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = _failed(workspace, installed, "project/copied")

    with v3.cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, v3.find(workspace, ref.job_id), "unlock", "t") is None
    job = v3.find(workspace, ref.job_id)
    doc = v3.state_of(job)
    assert job.state == "failed" and doc.unlocked is not None and doc.unlocked["operator"] == "cli"
    runner = job.path / COPY / "run"
    assert runner.read_bytes() == (installed.package / "run").read_bytes() and os.access(runner, os.X_OK)
    assert not [name for name in os.listdir(job.path / ".httk-job") if name.startswith(f"{COPY}.")]
    assert _request(workspace, ref.job_id, "unlock") == "already unlocked"
    assert _request(workspace, ref.job_id, "unlock", no_copy=True) == "already unlocked"
    assert v3.state_of(v3.find(workspace, ref.job_id)).unlocked == doc.unlocked

    # With no_copy, only the marker; a later plain unlock request (no flag: it copies) adds the copy.
    bare = _failed(workspace, installed, "project/bare")
    with v3.cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, v3.find(workspace, bare.job_id), "unlock", "t", copy=False) is None
    marked = v3.find(workspace, bare.job_id)
    assert v3.state_of(marked).unlocked is not None and not (marked.path / COPY).exists()
    assert _request(workspace, bare.job_id, "unlock") is None
    assert (v3.find(workspace, bare.job_id).path / COPY / "run").is_file()


def test_unlock_without_an_installation_or_of_a_language_realization(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    gone = v3.submit(workspace, ("local:gone", "gone"), {"start": "fail"}, placement="project/gone")
    reason = _request(workspace, gone.job_id, "unlock")
    assert reason == "workflow local:gone is not installed in the job's workspace; nothing to copy"
    doc = v3.state_of(v3.find(workspace, gone.job_id))
    assert doc.unlocked is None and "request_dropped" in [entry["event"] for entry in doc.history_tail]
    # Without copy nothing needs the installation.
    assert _request(workspace, gone.job_id, "unlock", no_copy=True) is None

    language = v3.submit(
        workspace,
        installed,
        {"start": "fail"},
        placement="project/lang",
        parameters={"workflow_realization": "language"},
    )
    assert _request(workspace, language.job_id, "unlock") is None
    job = v3.find(workspace, language.job_id)
    assert v3.state_of(job).unlocked is not None and not (job.path / COPY).exists()


def test_an_edited_job_definition_is_repinned_only_when_unlocked(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    unlocked, locked = (_failed(workspace, installed, f"project/{name}") for name in ("unlocked", "locked"))
    assert _request(workspace, unlocked.job_id, "unlock", no_copy=True) is None
    old = v3.state_of(v3.find(workspace, unlocked.job_id)).job_digest
    digests = {
        ref.job_id: _edit_job(v3.find(workspace, ref.job_id), {"start": "succeed"}) for ref in (unlocked, locked)
    }
    for ref in (unlocked, locked):
        assert _request(workspace, ref.job_id, "restart") is None
    v3.run(workspace)

    done = v3.find(workspace, unlocked.job_id)
    doc = v3.state_of(done)
    assert done.state == "succeeded" and doc.job_digest == digests[unlocked.job_id] != old
    assert {"from": old, "to": digests[unlocked.job_id]} in [
        {"from": entry["from"], "to": entry["to"]}
        for entry in doc.history_tail
        if entry["event"] == "job_digest_repinned"
    ]
    failed = v3.state_of(v3.find(workspace, locked.job_id))
    assert failed.failure is not None and failed.failure["code"] == "protocol_error"
    assert "changed since the job's first activation" in str(failed.failure["message"])


def test_an_unlocked_attempt_may_rewrite_its_job_definition(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    mapping = v3.job_mapping(installed, {"start": "succeed"}, placement="project/rewriter")
    rewritten = json.dumps(mapping)
    mapping["parameters"] = {**mapping["parameters"], "files": {"start": {"../job.json": rewritten}}}  # type: ignore[dict-item]
    ref = v3.submit_mapping(workspace, mapping)
    assert _request(workspace, ref.job_id, "unlock", no_copy=True) is None
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    doc = v3.state_of(done)
    assert done.state == "succeeded" and doc.job_digest == JobDefinition.from_path(done.path / "job.json").digest
    assert any(entry["event"] == "job_digest_repinned" for entry in doc.history_tail)


def test_a_succeeded_unlocked_job_reruns_and_is_never_sealed(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, placement="project/rerun")
    v3.run(workspace)
    sealed = v3.state_of(v3.find(workspace, ref.job_id)).seal
    assert sealed is not None and "sha256" in sealed
    assert "run `job unseal` first" in str(_request(workspace, ref.job_id, "unlock"))
    assert _request(workspace, ref.job_id, "unseal") is None
    assert _request(workspace, ref.job_id, "unlock", no_copy=True) is None
    assert _request(workspace, ref.job_id, "restart") is None
    assert v3.find(workspace, ref.job_id).state == "ready"
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    doc = v3.state_of(done)
    assert done.state == "succeeded" and dict(doc.seal or {}) == {"disabled": True}
    assert v3.events(done).count("attempt_started") == 2
    assert _request(workspace, ref.job_id, "seal") == "an unlocked job cannot be sealed"
    assert _request(workspace, ref.job_id, "delete") is None
    assert _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True) is None


def test_an_unlocked_job_launches_from_its_runner_copy(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = _failed(workspace, installed, "project/edited")
    assert _request(workspace, ref.job_id, "unlock") is None
    runner = v3.find(workspace, ref.job_id).path / COPY / "run"
    text = runner.read_text(encoding="utf-8").replace(
        'behavior = parameters["script"][step]\n',
        'behavior = "succeed"\npathlib.Path("sentinel").write_text(os.environ["HTTK_WORKFLOW_RUNNER_ROOT"])\n',
    )
    runner.write_text(text, encoding="utf-8")
    assert _request(workspace, ref.job_id, "restart") is None
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "succeeded"
    # The attempt ran from the job's own copy (the job then sat below owned/).
    root = (done.path / "run" / "sentinel").read_text(encoding="utf-8")
    assert root.endswith(f"/{COPY}") and done.job_key in root
    pin = v3.state_of(done).workflow_pin
    assert pin is not None and pin["id"] == installed.id
    assert pin["tree_sha256"] == source_tree_digest(done.path / COPY) != installed.record["tree_sha256"]


def test_a_locked_job_never_launches_from_a_planted_copy(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = _failed(workspace, installed, "project/planted")
    planted = v3.find(workspace, ref.job_id).path / COPY
    planted.mkdir(parents=True)
    (planted / "run").write_text("#!/bin/sh\nexit 7\n", encoding="utf-8")
    (planted / "run").chmod(0o755)
    assert _request(workspace, ref.job_id, "restart") is None
    v3.run(workspace)
    doc = v3.state_of(v3.find(workspace, ref.job_id))
    assert doc.failure is not None and doc.failure["code"] == "converge"
    assert doc.workflow_pin is not None and doc.workflow_pin["tree_sha256"] == installed.record["tree_sha256"]


def test_set_workflow_of_an_unlocked_job(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    other = v3.install(workspace, tmp_path / "other", name="other")
    copied, bare = (_failed(workspace, installed, f"project/{name}") for name in ("copied", "bare"))
    # A copy planted in a locked job is not the copy refusal's business: the compatibility rules decide.
    planted = v3.find(workspace, bare.job_id).path / COPY
    planted.mkdir(parents=True)
    assert str(_request(workspace, bare.job_id, "set_workflow", workflow=other.id)).startswith("rule 2 (short name)")
    (v3.find(workspace, bare.job_id).path / COPY).rmdir()
    assert _request(workspace, copied.job_id, "unlock") is None
    assert _request(workspace, copied.job_id, "set_workflow", workflow=other.id) == (
        f"the job runs its own workflow copy at {COPY}/; edit or remove it instead of switching"
    )
    assert _request(workspace, bare.job_id, "unlock", no_copy=True) is None
    assert _request(workspace, bare.job_id, "set_workflow", workflow=other.id) is None
    override = v3.state_of(v3.find(workspace, bare.job_id)).workflow_override
    assert dict(override or {}) == {"id": other.id, "name": "other"}
    assert str(_request(workspace, bare.job_id, "set_workflow", workflow="local:gone")).startswith("rule 1")


def test_children_of_an_unlocked_parent_inherit_the_marker(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    spawn = {"children": [{"label": "a", "script": {"start": "succeed"}}]}
    parent = v3.submit(
        workspace,
        installed,
        {"start": "spawn", "gather": "succeed"},
        placement="project/p",
        parameters={"spawn": spawn},
    )
    assert _request(workspace, parent.job_id, "unlock") is None
    v3.run(workspace)
    done = v3.find(workspace, parent.job_id)
    assert done.state == "succeeded"
    (child,) = v3.state_of(done).children
    child_doc = v3.state_of(v3.find(workspace, str(child["job_id"])))
    assert child_doc.unlocked is not None and child_doc.unlocked["inherited_from"] == done.job_key
    assert dict(child_doc.seal or {}) == {"disabled": True}
    assert not (v3.find(workspace, str(child["job_id"])).path / COPY).exists()


def test_an_unlocked_job_is_never_ejected_into_an_exchange(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = v3.submit(workspace, installed, {"start": "fail"}, placement="project/eject")
    assert _request(workspace, ref.job_id, "unlock", no_copy=True) is None
    exchange = tmp_path / "exchange"
    (exchange / "inbox").mkdir(parents=True)
    (exchange / "exchange.json").write_text("{}", encoding="utf-8")
    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, v3.find(workspace, ref.job_id))
        assert owned is not None
        with pytest.raises(ValueError, match="is unlocked"):
            _moving.eject(workspace, owner, owned, destination=exchange / "inbox", tree=False)
    assert v3.find(workspace, ref.job_id).state == "ready" and not list((exchange / "inbox").iterdir())
    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, v3.find(workspace, ref.job_id))
        assert owned is not None
        _moving.eject(workspace, owner, owned, destination=tmp_path / "out", tree=False)


def test_a_failed_copy_leaves_neither_marker_nor_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = _failed(workspace, installed, "project/broken")
    original = _store._copy_package

    def torn(source: Path, destination: Path, spec: object) -> None:
        original(source, destination, None)
        raise ValueError("the package holds a special file")

    monkeypatch.setattr(_store, "_copy_package", torn)
    reason = _request(workspace, ref.job_id, "unlock")
    assert reason == f"cannot copy the runner of workflow {installed.id}: the package holds a special file"
    job = v3.find(workspace, ref.job_id)
    assert v3.state_of(job).unlocked is None and os.listdir(job.path / ".httk-job") == []
    assert not (job.path / COPY).exists()


def test_a_built_runner_copy_runs_with_the_store_build_and_its_manifest_is_never_read(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    with v3.cli_owner(workspace) as owner:
        installed = _store.install(workspace, owner, _compiled_package(tmp_path))
    ref = v3.submit(workspace, installed, {"start": "succeed"}, placement="project/built")
    assert _request(workspace, ref.job_id, "unlock") is None
    copy = v3.find(workspace, ref.job_id).path / COPY
    assert not (copy / "build").exists()  # build artifacts are never copied
    run = copy / "run"
    run.write_text(
        run.read_text(encoding="utf-8").replace("exec ", 'echo copy > "$HTTK_WORKFLOW_WORKDIR/from-copy"\nexec ')
    )
    sentinel = tmp_path / "probed"
    manifests = (
        # A platform probe the manager would run outside the sandbox, and a malformed build section.
        f"[workflow.build]\nplatform = 'touch {sentinel}'\n",
        "[workflow.build]\nartifacts = 3\n",
    )
    for manifest in manifests:
        text = (installed.package / "httk_workflow.toml").read_text(encoding="utf-8")
        (copy / "httk_workflow.toml").write_text(text.replace("[workflow.build]\n", manifest), encoding="utf-8")
        v3.run(workspace)
        done = v3.find(workspace, ref.job_id)
        assert done.state == "succeeded" and not sentinel.exists()
        assert (done.path / "run" / "from-copy").is_file() and (done.path / "run" / "used-artifact").is_file()
        pin = v3.state_of(done).workflow_pin
        assert pin is not None and pin["tree_sha256"] == _store.source_digest(
            done.path / COPY, declared_by=installed.package
        )
        (done.path / "run" / "from-copy").unlink()
        assert _request(workspace, ref.job_id, "restart") is None
        copy = v3.find(workspace, ref.job_id).path / COPY


def test_a_first_unlock_keeps_a_placed_copy_and_clears_crash_leftovers(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    placed, crashed = (_failed(workspace, installed, f"project/{name}") for name in ("placed", "crashed"))
    own = v3.find(workspace, placed.job_id).path / COPY
    own.mkdir(parents=True)
    (own / "run").write_text("mine", encoding="utf-8")
    observed: list[removal.RequestOutcome] = []
    with v3.cli_owner(workspace) as owner:
        assert (
            removal.request_now(workspace, owner, v3.find(workspace, placed.job_id), "unlock", "t", outcomes=observed)
            is None
        )
    assert observed[0].explanation == "job unlocked; existing runner copy kept"
    job = v3.find(workspace, placed.job_id)
    assert (job.path / COPY / "run").read_text(encoding="utf-8") == "mine" and "runner_copy_kept" in v3.events(job)

    leftover = v3.find(workspace, crashed.job_id).path / ".httk-job" / f"{COPY}.abcdefghijklmnop"
    # A job file of the same shape at the payload root is the job's own, never a leftover.
    (leftover.parent.parent / f"{COPY}.abcdefghijklmnop").write_text("job data", encoding="utf-8")
    leftover.mkdir(parents=True)
    (leftover / "run").write_text("half", encoding="utf-8")
    assert _request(workspace, crashed.job_id, "unlock") is None
    crashed_path = v3.find(workspace, crashed.job_id).path
    assert os.listdir(crashed_path / ".httk-job") == [] and (crashed_path / COPY / "run").is_file()
    assert (crashed_path / f"{COPY}.abcdefghijklmnop").read_text(encoding="utf-8") == "job data"


def test_a_damaged_state_keeps_the_unlocked_marker(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    ref = _failed(workspace, installed, "project/damaged")
    assert _request(workspace, ref.job_id, "unlock", no_copy=True) is None
    job = v3.find(workspace, ref.job_id)
    marker = v3.state_of(job).unlocked
    state = json.loads((job.path / "state.json").read_text(encoding="utf-8"))
    (job.path / "state.json").write_text(json.dumps({**state, "phase": "broken"}), encoding="utf-8")
    _requests.post(
        workspace,
        action="set_priority",
        job_id=ref.job_id,
        placement="project/damaged",
        operator="t",
        reason="t",
        priority=7,
    )
    v3.run(workspace)
    doc = v3.state_of(v3.find(workspace, ref.job_id))
    assert doc.unlocked == marker and doc.failure is not None and doc.failure["code"] == "protocol_error"
