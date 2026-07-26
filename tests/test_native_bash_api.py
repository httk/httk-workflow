"""Protocol-level outcome bundles, supervised processes, and the VASP helpers.

The Bash authoring SDK that publishes through these bundles is tested in
:mod:`tests.test_bash_sdk`, and its parity with the Python SDK in
:mod:`tests.test_parity`.
"""

import json
import sys
import uuid
from pathlib import Path

from httk.workflow import (
    AttemptContext,
    Diagnostic,
    JobSpec,
    OutcomeDraft,
    ProcessSupervisor,
    ReplayableWorkdirBatch,
    VaspRemedyDecision,
    apply_vasp_remedy,
    clean_vasp_outputs,
    diagnose_vasp_files,
    plan_vasp_remedy,
    prepare_job_payload,
    run_vasp,
)


def _draft(tmp_path: Path, *, data_generation: int | None = None) -> OutcomeDraft:
    """Return one unpublished outcome draft of a fabricated attempt."""

    control = tmp_path / "control"
    control.mkdir()
    context = control / "context.json"
    context.write_text(
        json.dumps(
            {
                "format": "httk-workflow-attempt-context",
                "format_version": 1,
                "workspace_id": str(uuid.uuid4()),
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
                "workdir_mode": "persistent",
                "workdir_reused": True,
                "data_generation": data_generation,
                "join": None,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "run").mkdir()
    return OutcomeDraft(AttemptContext.read(context), control)


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
    batch = ReplayableWorkdirBatch.create(workdir)
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
