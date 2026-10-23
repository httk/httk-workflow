"""A child label is unique per job: a spawn or call naming an existing child of the job reuses it.

The attempts are fabricated (no manager): the job's ``state.json`` records published
children, each given a job directory in the workspace where the test wants it found.
"""

import json
import logging
import uuid
from pathlib import Path
from typing import Any

import pytest

from attempt_fixtures import FabricatedAttempt, child_observation, fabricate
from httk.workflow import Attempt, ChildSpec, FormatError, _kernel
from httk.workflow._state import encode_state, read_state_unowned
from httk.workflow.runtime_builders import ChildReference

_TOKEN = "aaaaaaaaaaaaaaaa"


def _entry(attempt: FabricatedAttempt, label: str | None, *, kind: str | None = "succeeded") -> dict[str, Any]:
    """A published child entry at ``project/children``, given a job directory in state *kind* (none for None)."""

    job_id = str(uuid.uuid4())
    job_key = f"{label or 'child'}--{job_id}"
    if kind is not None:
        (attempt.workspace.root / "jobs" / kind / "project" / "children" / f"{job_key}~p500~{_TOKEN}").mkdir(
            parents=True
        )
    entry = {
        "workspace_id": attempt.workspace.workspace_id,
        "job_id": job_id,
        "job_key": job_key,
        "placement": "project/children",
    }
    return entry if label is None else {**entry, "label": label}


def _record(attempt: FabricatedAttempt, *entries: dict[str, Any]) -> None:
    """Record *entries* as the job's published children."""

    path = attempt.payload / "state.json"
    doc, _damaged = read_state_unowned(path)
    assert doc is not None
    path.write_bytes(encode_state(doc.with_children([*doc.children, *entries])))


def _spawn_json(attempt: FabricatedAttempt) -> list[dict[str, Any]]:
    path = attempt.control / "outcome.ready" / "children" / "spawn.json"
    return json.loads(path.read_text(encoding="utf-8"))["children"] if path.exists() else []


def _leaf() -> ChildSpec:
    return ChildSpec(step="leaf")


def test_a_spawn_of_an_existing_label_reuses_the_child_and_the_join_names_it(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch")
    x = _entry(attempt, "x")
    _record(attempt, x)
    a = Attempt.initialize(attempt.environment)
    with caplog.at_level(logging.INFO, logger="httk.workflow.sdk"):
        first = a.spawn(_leaf(), label="y")
        reused = a.spawn(ChildSpec(step="other", parameters={"ignored": True}), label="x", placement="else/where")
        last = a.spawn(_leaf(), label="z")
    assert reused == ChildReference(attempt.workspace.workspace_id, x["job_id"], x["job_key"], "project/children")
    assert any(r.levelno == logging.INFO and x["job_key"] in r.getMessage() for r in caplog.records)
    a.gather("next")

    # Nothing new is published for x; the join names it, with its label, in the order of the calls.
    assert [entry["label"] for entry in _spawn_json(attempt)] == ["y", "z"]
    join = attempt.outcome()["join"]
    assert [child["job_id"] for child in join["children"]] == [first.job_id, x["job_id"], last.job_id]
    assert join["children"][1] == {
        "workspace_id": attempt.workspace.workspace_id,
        "job_id": x["job_id"],
        "job_key": x["job_key"],
        "placement_hint": "project/children",
        "label": "x",
    }
    assert not (attempt.control / "outcome.ready" / ".reused-children.json").exists()


def test_an_owned_child_is_reused_at_its_recorded_placement(tmp_path: Path) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch")
    x = _entry(attempt, "x", kind=None)
    _record(attempt, x)
    # A manager holds x: its directory is below owned/<owner-id>/, which has no placement.
    owned = attempt.workspace.root / "jobs" / "owned" / uuid.uuid4().hex / f"{x['job_key']}~p500~{_TOKEN}~ready"
    owned.mkdir(parents=True)
    reference = Attempt.initialize(attempt.environment).spawn(_leaf(), label="x")
    assert reference.job_id == x["job_id"] and reference.placement_hint == "project/children"


def test_only_the_newest_recorded_child_counts_and_label_less_entries_are_skipped(tmp_path: Path) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch")
    older, newer = _entry(attempt, "x"), _entry(attempt, "x", kind=None)
    _record(attempt, older, newer, _entry(attempt, None), {**_entry(attempt, None), "label": None})
    a = Attempt.initialize(attempt.environment)
    # The newest x is gone, so x is created anew; the older located one is not resurrected.
    fresh = a.spawn(_leaf(), label="x")
    assert fresh.job_id not in {older["job_id"], newer["job_id"]}
    # The label-less entries match nothing.
    a.spawn(_leaf(), label="child")
    a.gather("next")
    assert [entry["label"] for entry in _spawn_json(attempt)] == ["x", "child"]


def test_an_absent_state_records_no_children_and_a_damaged_one_is_refused(tmp_path: Path) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch")
    (attempt.payload / "state.json").unlink()
    Attempt.initialize(attempt.environment).spawn(_leaf(), label="x")
    (attempt.payload / "state.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(FormatError, match="damaged"):
        Attempt.initialize(attempt.environment).spawn(_leaf(), label="x")


def test_a_label_is_still_unique_within_one_attempt(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch", children=[child_observation("x", "succeeded")])
    doc, _damaged = read_state_unowned(attempt.payload / "state.json")
    assert doc is not None
    (observation,) = doc.observations
    _record(attempt, {key: observation[key] for key in ("job_id", "job_key", "label", "placement")})

    fresh = Attempt.initialize(attempt.environment)
    fresh.spawn(_leaf(), label="new")
    with pytest.raises(ValueError, match="not unique"):
        fresh.spawn(_leaf(), label="new")

    reusing = Attempt.initialize(attempt.environment)
    assert reusing.spawn(_leaf(), label="x").job_id == observation["job_id"]
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="httk.workflow.sdk"), pytest.raises(ValueError, match="not unique"):
        reusing.spawn(_leaf(), label="x")
    # A refused duplicate is not reported as a reuse.
    assert not [record for record in caplog.records if "reusing" in record.getMessage()]
    with pytest.raises(ValueError, match="not unique"):
        reusing.call("sub", label="x")
    with pytest.raises(ValueError, match="duplicate join child label: x"):
        reusing.gather("next", rejoin=["x"])


def test_a_gather_of_only_reused_children_publishes_a_wait(tmp_path: Path) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch")
    x = _entry(attempt, "x")
    _record(attempt, x)
    a = Attempt.initialize(attempt.environment)
    a.spawn(_leaf(), label="x")
    a.gather("next", when="at_least", count=1)
    outcome = attempt.outcome()
    assert outcome["action"] == "wait" and outcome["join"]["count"] == 1
    assert [child["label"] for child in outcome["join"]["children"]] == ["x"]
    assert _spawn_json(attempt) == []


def test_a_call_of_an_existing_label_stages_nothing_and_needs_no_callee(tmp_path: Path) -> None:
    # No workflow is installed at all: a reusing call resolves nothing.
    attempt = fabricate(tmp_path / "attempt", step="branch")
    x = _entry(attempt, "x")
    _record(attempt, x)
    a = Attempt.initialize(attempt.environment)
    assert a.call("sub", label="x", inputs={"anything": 1}).job_id == x["job_id"]
    assert not list(attempt.control.glob("call.*"))
    a.gather("next")
    assert [child["job_id"] for child in attempt.outcome()["join"]["children"]] == [x["job_id"]]


@pytest.mark.parametrize("kind", ["failed", "cancelled", "owned"])
def test_reusing_a_failed_or_cancelled_child_warns_with_the_remedy(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, kind: str
) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch")
    x = _entry(attempt, "x", kind=None if kind == "owned" else kind)
    _record(attempt, x)
    if kind == "owned":
        # A failed child a manager holds (claimed from failed) warns too.
        owned = attempt.workspace.root / "jobs" / "owned" / uuid.uuid4().hex / f"{x['job_key']}~p500~{_TOKEN}~failed"
        owned.mkdir(parents=True)
        kind = "failed"
    with caplog.at_level(logging.INFO, logger="httk.workflow.sdk"):
        assert Attempt.initialize(attempt.environment).spawn(_leaf(), label="x").job_id == x["job_id"]
    (warning,) = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert x["job_key"] in warning.getMessage() and kind in warning.getMessage()
    assert "httk job delete" in warning.getMessage()


def test_a_child_missed_by_the_unsettled_listing_is_found_by_the_settled_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch")
    x, y = _entry(attempt, "x"), _entry(attempt, "y")
    _record(attempt, x, y)
    real = _kernel.locate_many
    settled: list[list[str]] = []

    def flaky(workspace: Any, job_ids: Any, **options: Any) -> dict[str, _kernel.JobRef]:
        found = real(workspace, job_ids, **options)
        if options["settle"]:
            settled.append(list(job_ids))
            return found
        # x is mid-rename during the first listing.
        return {job_id: ref for job_id, ref in found.items() if job_id != x["job_id"]}

    monkeypatch.setattr(_kernel, "locate_many", flaky)
    a = Attempt.initialize(attempt.environment)
    assert a.spawn(_leaf(), label="x").job_id == x["job_id"]
    assert a.spawn(_leaf(), label="y").job_id == y["job_id"]
    # Only the miss was settled, once for the attempt.
    assert settled == [[x["job_id"]]]


def test_the_bash_bridge_keeps_a_reused_child_across_processes(tmp_path: Path) -> None:
    attempt = fabricate(tmp_path / "attempt", step="branch", workflow="tests.bash")
    x = _entry(attempt, "x")
    _record(attempt, x)
    runner = tmp_path / "runner.sh"
    # Every bridge verb is its own process: the gather resumes the draft the spawns left behind.
    runner.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.bash branch leaf
step_branch() {
    httk_workflow_spawn x --step leaf
    httk_workflow_spawn y --step leaf
    httk_workflow_gather leaf
}
step_leaf() { httk_workflow_succeed; }
httk_workflow_main
""",
        encoding="utf-8",
    )
    completed = attempt.run("bash", str(runner))
    assert completed.returncode == 0, completed.stderr
    # The reusing spawn printed the existing child's key.
    assert completed.stdout.splitlines()[0] == x["job_key"]
    assert [entry["label"] for entry in _spawn_json(attempt)] == ["y"]
    join = attempt.outcome()["join"]["children"]
    assert join[0] == {
        "workspace_id": attempt.workspace.workspace_id,
        "job_id": x["job_id"],
        "job_key": x["job_key"],
        "placement_hint": "project/children",
        "label": "x",
    }
    assert len(join) == 2
