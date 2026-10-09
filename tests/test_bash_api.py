"""Protocol-level outcome bundles and supervised processes.

The Bash authoring SDK that publishes through these bundles is tested in
:mod:`tests.test_bash_sdk`, and its parity with the Python SDK in
:mod:`tests.test_parity`.
"""

import json
import sys
import uuid
from pathlib import Path

from httk.workflow.protocol import (
    AttemptContext,
    JobSpec,
    OutcomeDraft,
    ReplayableWorkdirBatch,
    prepare_job_payload,
)
from httk.workflow.supervision import ProcessSupervisor


def _draft(tmp_path: Path) -> OutcomeDraft:
    """Return one unpublished outcome draft of a fabricated attempt."""

    control = tmp_path / "control"
    control.mkdir()
    context = {
        "format": "httk-workflow-attempt-context",
        "settings": {},
        "durable": False,
        "deadline": None,
        "format_version": 2,
        "workspace_id": str(uuid.uuid4()),
        "job_id": (_jid := str(uuid.uuid4())),
        "job_key": f"job--{_jid}",
        "placement": "project/a",
        "payload": str(tmp_path / "payload"),
        "step": "prepare",
        "activation_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "activation_ordinal": 2,
        "attempt_ordinal": 3,
        "total_attempts": 4,
        "is_restart": True,
        "is_unclean_restart": False,
        "attempt_reason": "requested_retry",
        "join": None,
    }
    (tmp_path / "run").mkdir()
    return OutcomeDraft(AttemptContext.from_mapping(context), control)


def test_composed_outcome_contains_children_with_v3_job_definitions(tmp_path: Path) -> None:
    outcome = _draft(tmp_path)
    child = tmp_path / "child"
    (child / "files").mkdir(parents=True)
    (child / "files" / "input").write_text("input\n", encoding="utf-8")
    prepare_job_payload(
        child,
        JobSpec(name="child", workflow_id="local:tests.child", workflow_name="tests.child", tag="child"),
    )

    reference = outcome.add_child(child, "children/a", label="first")
    ready = outcome.publish("wait", next_step="collect")

    body = json.loads((ready / "outcome.json").read_text(encoding="utf-8"))
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert body["action"] == "wait" and "expected_data_generation" not in body
    assert body["join"]["children"][0]["job_id"] == reference.job_id
    assert spawn["format_version"] == 2
    assert spawn["children"][0]["placement"] == "children/a"
    assert spawn["children"][0]["label"] == "first"
    staged = ready / "children" / "jobs" / reference.job_key
    assert (staged / "files" / "input").is_file() and (child / "files" / "input").is_file()
    job = json.loads((staged / "job.json").read_text(encoding="utf-8"))
    assert job["format_version"] == 3 and job["placement"] == "children/a"
    assert job["parent"]["job_id"] == body["job_id"] and job["parent"]["spawn_id"] == spawn["children"][0]["spawn_id"]


def test_a_moved_child_payload_leaves_its_source(tmp_path: Path) -> None:
    outcome = _draft(tmp_path)
    child = tmp_path / "control" / "prepared"
    prepare_job_payload(child, JobSpec(name="child", workflow_id="local:tests.child", workflow_name="tests.child"))
    reference = outcome.add_child(child, "children/a", label="moved", move=True)
    assert not child.exists()
    assert (outcome.root / "children" / "jobs" / reference.job_key / "job.json").is_file()


def test_workdir_batch_replays_after_seal(tmp_path: Path) -> None:
    workdir = tmp_path / "run"
    workdir.mkdir()
    source = tmp_path / "new"
    source.write_text("new\n", encoding="utf-8")
    batch = ReplayableWorkdirBatch.initialize(workdir)
    batch.transaction.put_file("value", source, "results/value.txt")
    batch.seal()
    recovered = ReplayableWorkdirBatch.recover(workdir)
    assert len(recovered) == 1
    assert (workdir / "results" / "value.txt").read_text(encoding="utf-8") == "new\n"
    assert ReplayableWorkdirBatch.recover(workdir) == ()


def test_supervisor_external_checker_and_literal_argv(tmp_path: Path) -> None:
    checker = tmp_path / "checker.py"
    checker.write_text(
        """import json, sys
for line in sys.stdin:
    event = json.loads(line)
    if event.get("event") == "line" and "STOP" in event.get("line", ""):
        print(json.dumps({"format":"httk-workflow-checker-result","format_version":2,
                          "code":"found_stop","severity":"fatal","summary":"stop",
                          "source":event["source"],"stop":True}), flush=True)
""",
        encoding="utf-8",
    )
    from httk.workflow.supervision import CheckerSpec

    literal = f"literal;touch {tmp_path / 'unsafe'}"
    report = ProcessSupervisor(
        checkers=(CheckerSpec((sys.executable, str(checker))),),
    ).run([sys.executable, "-c", "import sys; print(sys.argv[1]); print('STOP')", literal])
    assert literal.encode() in report.stdout
    assert not (tmp_path / "unsafe").exists()
    assert any(item.code == "found_stop" for item in report.diagnostics)
