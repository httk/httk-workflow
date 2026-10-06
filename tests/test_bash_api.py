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


def _draft(tmp_path: Path, *, data_generation: int | None = None) -> OutcomeDraft:
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
        "workdir_mode": "persistent",
        "workdir_reused": True,
        "data_generation": data_generation,
        "join": None,
    }
    (tmp_path / "run").mkdir()
    return OutcomeDraft(AttemptContext.from_mapping(context), control)


def test_composed_outcome_contains_transaction_and_children(tmp_path: Path) -> None:
    outcome = _draft(tmp_path, data_generation=2)
    source = tmp_path / "result.txt"
    source.write_text("result\n", encoding="utf-8")
    child = tmp_path / "child"
    files = child / "files"
    files.mkdir(parents=True)
    runner = files / "run"
    runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runner.chmod(0o755)
    prepare_job_payload(
        child,
        JobSpec(
            name="child",
            workflow="tests.child",
            runner_path="files/run",
            tag="child",
            job_id=str(uuid.uuid4()),
        ),
    )

    transaction = outcome.transaction()
    transaction.make_dir("results-dir", "results")
    transaction.put_file("result", source, "results/value.txt")
    reference = outcome.add_child(child, "children/a", label="first")
    ready = outcome.publish("wait", next_step="collect")

    body = json.loads((ready / "outcome.json").read_text(encoding="utf-8"))
    manifest = json.loads((ready / "transaction" / "manifest.json").read_text(encoding="utf-8"))
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert body["action"] == "wait"
    assert body["expected_data_generation"] == 2
    assert body["join"]["children"][0]["job_id"] == reference.job_id
    assert [item["op"] for item in manifest["operations"]] == ["make-dir", "put-file"]
    assert spawn["children"][0]["placement"] == "children/a"
    assert spawn["children"][0]["label"] == "first"


def test_outcome_rejects_stale_explicit_generation(tmp_path: Path) -> None:
    outcome = _draft(tmp_path, data_generation=2)
    outcome.transaction().make_dir("results", "results")
    try:
        outcome.publish("advance", next_step="collect", expected_data_generation=1)
    except ValueError as exc:
        assert "does not match" in str(exc)
    else:
        raise AssertionError("stale explicit generation was accepted")


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
