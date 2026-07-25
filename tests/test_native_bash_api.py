import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

from httk.workflow import (
    AttemptRuntime,
    Diagnostic,
    JobSpec,
    ProcessSupervisor,
    ReplayableWorkspaceBatch,
    TaskManager,
    VaspRemedyDecision,
    WorkflowStore,
    apply_vasp_remedy,
    clean_vasp_outputs,
    diagnose_vasp_files,
    plan_vasp_remedy,
    prepare_job_payload,
    run_vasp,
)


def _runtime(tmp_path: Path, *, data_generation: int | None = None) -> AttemptRuntime:
    control = tmp_path / "control"
    control.mkdir()
    context = control / "context.json"
    context.write_text(
        json.dumps(
            {
                "format": "httk-workflow-attempt-context",
                "format_version": 1,
                "store_id": str(uuid.uuid4()),
                "job_id": str(uuid.uuid4()),
                "job_key": f"job--{uuid.uuid4()}",
                "placement": "project/a",
                "step": "prepare",
                "activation_id": str(uuid.uuid4()),
                "attempt_id": str(uuid.uuid4()),
                "activation_ordinal": 2,
                "attempt_ordinal": 3,
                "total_attempts": 4,
                "is_restart": True,
                "is_unclean_restart": False,
                "attempt_reason": "requested_retry",
                "workspace_mode": "persistent",
                "workspace_reused": True,
                "data_generation": data_generation,
                "join": None,
            }
        ),
        encoding="utf-8",
    )
    environment = {
        "HTTK_WORKFLOW_CONTEXT": str(context),
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(tmp_path / "job"),
        "HTTK_WORKFLOW_RUN_DIR": str(tmp_path / "run"),
        "HTTK_WORKFLOW_STORE_DIR": str(tmp_path / "store"),
    }
    (tmp_path / "run").mkdir()
    return AttemptRuntime.from_environment(environment)


def test_composed_outcome_contains_transaction_and_children(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, data_generation=2)
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

    outcome = runtime.outcome()
    transaction = outcome.transaction()
    transaction.make_dir("results-dir", "results")
    transaction.put_file("result", source, "results/value.txt")
    reference = outcome.add_child(child, "children/a")
    ready = outcome.publish("wait", next_step="collect")

    body = json.loads((ready / "outcome.json").read_text(encoding="utf-8"))
    manifest = json.loads((ready / "transaction" / "manifest.json").read_text(encoding="utf-8"))
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert body["action"] == "wait"
    assert body["expected_data_generation"] == 2
    assert body["join"]["children"][0]["job_id"] == reference.job_id
    assert [item["op"] for item in manifest["operations"]] == ["make-dir", "put-file"]
    assert spawn["children"][0]["placement"] == "children/a"


def test_outcome_rejects_stale_explicit_generation(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, data_generation=2)
    outcome = runtime.outcome()
    outcome.transaction().make_dir("results", "results")
    try:
        outcome.publish("advance", next_step="collect", expected_data_generation=1)
    except ValueError as exc:
        assert "does not match" in str(exc)
    else:
        raise AssertionError("stale explicit generation was accepted")


def test_workspace_batch_replays_after_seal(tmp_path: Path) -> None:
    workspace = tmp_path / "run"
    workspace.mkdir()
    source = tmp_path / "new"
    source.write_text("new\n", encoding="utf-8")
    batch = ReplayableWorkspaceBatch.create(workspace)
    batch.transaction.put_file("value", source, "results/value.txt")
    batch.seal()
    recovered = ReplayableWorkspaceBatch.recover(workspace)
    assert len(recovered) == 1
    assert (workspace / "results" / "value.txt").read_text(encoding="utf-8") == "new\n"
    assert ReplayableWorkspaceBatch.recover(workspace) == ()


def test_supervisor_external_checker_and_literal_argv(tmp_path: Path) -> None:
    checker = tmp_path / "checker.py"
    checker.write_text(
        """import json, sys
for line in sys.stdin:
    event = json.loads(line)
    if event.get("event") == "line" and "STOP" in event.get("line", ""):
        print(json.dumps({"format":"httk-workflow-checker-result","format_version":1,
                          "code":"found_stop","severity":"fatal","summary":"stop",
                          "source":event["source"],"stop":True}), flush=True)
""",
        encoding="utf-8",
    )
    from httk.workflow import CheckerSpec

    literal = f"literal;touch {tmp_path / 'unsafe'}"
    report = ProcessSupervisor(
        checkers=(CheckerSpec((sys.executable, str(checker))),),
    ).run([sys.executable, "-c", "import sys; print(sys.argv[1]); print('STOP')", literal])
    assert literal.encode() in report.stdout
    assert not (tmp_path / "unsafe").exists()
    assert any(item.code == "found_stop" for item in report.diagnostics)


def _poscar(path: Path) -> None:
    path.write_text(
        """test
1
2 0 0
0 2 0
0 0 2
Si
1
Direct
0 0 0
""",
        encoding="utf-8",
    )


def test_vasp_remedy_is_planned_then_explicitly_applied(tmp_path: Path) -> None:
    _poscar(tmp_path / "POSCAR")
    (tmp_path / "INCAR").write_text("ISPIN = 2\nISYM = 2\n", encoding="utf-8")
    (tmp_path / "KPOINTS").write_text("mesh\n0\nMonkhorst-Pack\n3 4 5\n0 0 0\n", encoding="utf-8")
    diagnostic = Diagnostic("kpoints_class", "error", "mismatch", "stdout")
    decision = plan_vasp_remedy((diagnostic,), history_path=tmp_path / ".httk-vasp" / "remedies.json")
    assert not decision.give_up
    assert decision.changes == (("bump_kpoints", 1),)
    apply_vasp_remedy(decision, directory=tmp_path)
    assert (tmp_path / "KPOINTS").read_text(encoding="utf-8").splitlines()[3] == "4 5 6"
    history = json.loads((tmp_path / ".httk-vasp" / "remedies.json").read_text(encoding="utf-8"))
    assert history["attempts"]["kpoints_class"] == 1
    assert history["events"][0]["files"][0]["before_sha256"]
    assert history["events"][0]["files"][0]["after_sha256"]


def test_vasp_zpotrf_remedy_is_bounded_and_rounds_bands(tmp_path: Path) -> None:
    _poscar(tmp_path / "POSCAR")
    (tmp_path / "INCAR").write_text("NBANDS = 10\nNPAR = 4\n", encoding="utf-8")
    decision = plan_vasp_remedy(
        (Diagnostic("zpotrf", "fatal", "factorization failed", "stdout"),),
        history_path=tmp_path / ".httk-vasp" / "remedies.json",
    )
    assert not decision.give_up
    assert decision.changes == (("scale_lattice", 1.05),)
    apply_vasp_remedy(decision, directory=tmp_path)
    assert (tmp_path / "POSCAR").read_text(encoding="utf-8").splitlines()[1] == "1.05"

    second = plan_vasp_remedy(
        (Diagnostic("zpotrf", "fatal", "factorization failed", "stdout"),),
        history_path=tmp_path / ".httk-vasp" / "remedies.json",
    )
    assert second.changes == (("bump_kpoints", 1),)

    bands = VaspRemedyDecision(
        "reviewed-v1",
        "zpotrf",
        2,
        (("bump_bands", 2),),
        False,
        "reviewed remedy available",
    )
    apply_vasp_remedy(bands, directory=tmp_path)
    assert "NBANDS = 12" in (tmp_path / "INCAR").read_text(encoding="utf-8")


def test_vasp_preclean_preserves_declared_outputs(tmp_path: Path) -> None:
    for name in ("OUTCAR", "WAVECAR", "keep.txt"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    removed = clean_vasp_outputs(tmp_path, keep=("WAVECAR",))
    assert {path.name for path in removed} == {"OUTCAR"}
    assert (tmp_path / "WAVECAR").is_file()
    assert (tmp_path / "keep.txt").is_file()


def test_vasp_5_and_6_diagnostics_are_structured(tmp_path: Path) -> None:
    program = tmp_path / "fake-vasp.py"
    program.write_text(
        """from pathlib import Path
print("Fatal error: unable to match k-point")
print("ERROR FEXCF: supplied exchange-correlation table")
Path("OUTCAR").write_text("vasp.6.4\\n")
Path("OSZICAR").write_text("")
""",
        encoding="utf-8",
    )
    report = run_vasp([sys.executable, str(program)], directory=tmp_path, termination_grace=0.1)
    assert report.classification == "diagnosed_stop"
    assert {item.code for item in report.diagnostics} >= {
        "tetrahedron_kpoints",
        "fexcf",
        "incomplete_outcar",
    }
    saved = json.loads((tmp_path / "vasp-run-report.json").read_text(encoding="utf-8"))
    assert saved["format"] == "httk-vasp-run-report"

    for noun in ("information", "informations"):
        (tmp_path / "OUTCAR").write_text(
            f"General timing and accounting {noun} for this job:\n",
            encoding="utf-8",
        )
        assert "incomplete_outcar" not in {item.code for item in diagnose_vasp_files(tmp_path)}


def test_installed_style_bash_runner_uses_manager_paths(tmp_path: Path) -> None:
    store = WorkflowStore.initialize(tmp_path / "store", extensions=["transactional-data-v1"])
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
source "$HTTK_WORKFLOW_VASP_BASH_API"
httk_workflow_init
case "$HTTK_WORKFLOW_STEP" in
  start)
    printf '%s\n' result > result.txt
    httk_workflow_outcome_begin >/dev/null
    httk_workflow_transaction_put_file result "$PWD/result.txt" result.txt
    httk_workflow_advance collect
    ;;
  collect)
    test "$(cat "$HTTK_WORKFLOW_DATA_DIR/result.txt")" = result
    httk_workflow_state_set answer 42
    httk_workflow_succeed
    ;;
esac
""",
        encoding="utf-8",
    )
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="bash",
            workflow="tests.bash",
            runner_path="files/runner",
            tag="bash",
            initial_step="start",
            job_id=str(uuid.uuid4()),
            data_mode="transactional",
        ),
    )
    store.submit(payload, "bash/jobs")
    with TaskManager(store, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = store.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    state = store.payload_path(marker.placement, marker.job_key) / "run" / ".httk-runner" / "state.json"
    assert json.loads(state.read_text(encoding="utf-8")) == {"answer": 42}
    data = store.payload_path(marker.placement, marker.job_key) / "data" / "result.txt"
    assert data.read_text(encoding="utf-8") == "result\n"


def test_shell_library_is_safe_with_set_u(tmp_path: Path) -> None:
    shell = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "shell" / "httk-workflow.sh"
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'set -euo pipefail; source "$1"; test "$HTTK_WORKFLOW_BASH_API_VERSION" = 1',
            "bash",
            str(shell),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_bash_composes_transaction_without_jq_or_eval(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, data_generation=0)
    shell = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "shell" / "httk-workflow.sh"
    environment = os.environ.copy()
    environment.update(
        {
            "HTTK_WORKFLOW_CONTEXT": str(runtime.control / "context.json"),
            "HTTK_WORKFLOW_CONTROL_DIR": str(runtime.control),
            "HTTK_WORKFLOW_JOB_DIR": str(runtime.job),
            "HTTK_WORKFLOW_RUN_DIR": str(runtime.workspace),
            "HTTK_WORKFLOW_STORE_DIR": str(runtime.store),
            "HTTK_WORKFLOW_PYTHON": sys.executable,
            "HTTK_WORKFLOW_BASH_API": str(shell),
            "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
        }
    )
    completed = subprocess.run(
        [
            "bash",
            "-c",
            """set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_init
printf '%s\\n' 'literal; $(touch unsafe)' > result.txt
httk_workflow_outcome_begin >/dev/null
httk_workflow_transaction_put_file result "$PWD/result.txt" "results/result.txt"
httk_workflow_advance collect
""",
        ],
        cwd=runtime.workspace,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert not (runtime.workspace / "unsafe").exists()
    ready = runtime.control / "outcome.ready"
    body = json.loads((ready / "outcome.json").read_text(encoding="utf-8"))
    assert body["action"] == "advance"
    assert body["expected_data_generation"] == 0
    assert (ready / "transaction" / "payload" / "result").read_text(encoding="utf-8").startswith("literal;")
