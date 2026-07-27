"""Labeled children, typed join observations, and runner-declared retries."""

import json
import uuid
from pathlib import Path

from httk.workflow import TaskManager, Workspace

_CHILD_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
mode = "@MODE@"
if mode == "succeed":
    body = {"action": "succeed"}
else:
    body = {
        "action": "fail",
        "failure": {"code": "child.broken", "message": "the child declared a failure"},
    }
temporary = control / "outcome.tmp.child"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    **body,
}))
os.rename(temporary, control / "outcome.ready")
"""

_PARENT_RUNNER = """#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

CHILD = {child!r}
LABELS = {labels!r}
context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
workdir = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {{
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}}
if context["step"] == "gather":
    (workdir / "children.json").write_text(json.dumps(context["children"], sort_keys=True))
    outcome = {{**base, "action": "succeed"}}
else:
    entries = []
    for label, mode in LABELS:
        child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), label + mode))
        child_key = "child--" + child_id
        child_dir = temporary / "children" / "jobs" / child_key
        (child_dir / "files").mkdir(parents=True)
        runner = child_dir / "files" / "runner"
        runner.write_text(CHILD.replace("@MODE@", mode))
        runner.chmod(0o755)
        (child_dir / "job.json").write_text(json.dumps({{
            "format": "httk-workflow-job",
            "format_version": 1,
            "id": child_id,
            "tag": "child",
            "name": "Child " + label,
            "workflow": "tests.child",
            "runner": {{"path": "files/runner", "arguments": []}},
            "workdir": {{"mode": "persistent", "path": "run"}},
            "data": {{"mode": "none"}},
            "initial_step": "run",
            "priority": 500,
            "claim": {{"pool": "default", "required_capabilities": []}},
            "retry_policy": {{"retry_on": []}},
            "resources": {{}},
            "parent": {{
                "workspace_id": context["workspace_id"],
                "job_id": context["job_id"],
                "activation_id": context["activation_id"],
            }},
        }}))
        entry = {{
            "workspace_id": context["workspace_id"],
            "job_id": child_id,
            "job_key": child_key,
            "placement": "project/children",
        }}
        if label:
            entry["label"] = label
        entries.append(entry)
    (temporary / "children" / "spawn.json").write_text(json.dumps({{"children": entries}}))
    outcome = {{
        **base,
        "action": "wait",
        "next_step": "gather",
        "join": {{
            # The join names no labels: the manager must carry them over from the
            # spawn set that registered these children.
            "children": [
                {{
                    "workspace_id": entry["workspace_id"],
                    "job_id": entry["job_id"],
                    "job_key": entry["job_key"],
                    "placement_hint": entry["placement"],
                }}
                for entry in entries
            ],
            "condition": "all_terminal",
        }},
    }}
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""

_RETRYABLE_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
run = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
count_path = run / "attempts"
count = int(count_path.read_text()) + 1 if count_path.exists() else 1
count_path.write_text(str(count))
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "fail",
    "failure": {
        "code": "vasp.nonconvergent",
        "message": "electronic minimization did not converge",
        "retryable": True,
    },
}))
os.rename(temporary, control / "outcome.ready")
"""


def _parent_runner(labels: list[tuple[str, str]]) -> str:
    return _PARENT_RUNNER.format(child=_CHILD_RUNNER, labels=labels)


def _payload(
    root: Path,
    runner_source: str,
    *,
    initial_step: str = "branch",
    attempts_per_activation: int = 1,
) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": job_id,
        "tag": "parent",
        "name": "Labeled children parent",
        "workflow": "tests.labels",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": initial_step,
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {
            "maximum_attempts_per_activation": attempts_per_activation,
            "maximum_total_attempts": 10,
            "retry_on": [],
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _run(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)


def test_a_spawn_child_without_a_label_is_a_protocol_error(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _parent_runner([("", "succeed")]))
    workspace.submit(payload, "project/unlabeled")
    _run(workspace)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "protocol_error"
    assert "spawn child label" in failure["message"]
    # Nothing was registered: an unusable spawn set never becomes work.
    assert [found.job_key for found in workspace.scan_markers()] == [marker.job_key]


def test_duplicate_spawn_labels_are_a_protocol_error(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _parent_runner([("alpha", "succeed"), ("alpha", "fail")]))
    workspace.submit(payload, "project/duplicated")
    _run(workspace)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "protocol_error"
    assert "not unique" in failure["message"]


def test_gather_step_reads_labeled_child_observations_from_its_context(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", _parent_runner([("alpha", "succeed"), ("beta", "fail")]))
    workspace.submit(payload, "project/gathering")
    _run(workspace)

    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    parent_payload = workspace.payload_path(parent.placement, parent.job_key)
    observations = json.loads((parent_payload / "run" / "children.json").read_text(encoding="utf-8"))
    assert [item["label"] for item in observations] == ["alpha", "beta"]
    by_label = {str(item["label"]): item for item in observations}

    succeeded = by_label["alpha"]
    assert succeeded["kind"] == "succeeded"
    assert succeeded["failure"] is None
    assert succeeded["data_generation"] is None
    assert succeeded["payload_path"] == f"project/children/{succeeded['job_key']}"
    assert succeeded["workdir_path"] == f"project/children/{succeeded['job_key']}/run"
    assert (workspace.root / str(succeeded["workdir_path"])).is_dir()

    failed = by_label["beta"]
    assert failed["kind"] == "failed"
    assert failed["failure"] == {"code": "child.broken", "message": "the child declared a failure"}

    # The enriched observations are exactly the join summary earlier profiles
    # published, so a runner reading either member sees one consistent record.
    assert workspace.read_state(parent)["join_summary"] == observations
    assert all(item["record_ref"] for item in observations)


def test_a_retryable_declared_failure_retries_until_the_budget_is_exhausted(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(
        tmp_path / "source",
        _RETRYABLE_RUNNER,
        initial_step="only",
        attempts_per_activation=3,
    )
    workspace.submit(payload, "project/retryable")
    _run(workspace)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    # The runner-declared failure is what an operator finally sees, and the
    # activation was repeated exactly as often as the budget permitted.
    assert state["reason"] == "declared_failure"
    assert state["failure"] == {
        "code": "vasp.nonconvergent",
        "message": "electronic minimization did not converge",
        "retryable": True,
    }
    assert state["attempt_ordinal"] == 3
    attempts = workspace.payload_path(marker.placement, marker.job_key) / "run" / "attempts"
    assert attempts.read_text(encoding="utf-8") == "3"


def test_a_retryable_failure_is_not_retried_without_a_remaining_attempt(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(
        tmp_path / "source",
        _RETRYABLE_RUNNER,
        initial_step="only",
        attempts_per_activation=1,
    )
    workspace.submit(payload, "project/single-attempt")
    _run(workspace)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    assert state["reason"] == "declared_failure"
    assert state["failure"]["code"] == "vasp.nonconvergent"
    attempts = workspace.payload_path(marker.placement, marker.job_key) / "run" / "attempts"
    assert attempts.read_text(encoding="utf-8") == "1"
