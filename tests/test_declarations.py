"""Workflow declarations: carried verbatim, declared statically, observed at run time.

A declaration says what a workflow *is* — its inputs, its method, its outputs —
without a graph. *httk-workflow* carries the document and never interprets it, so
what is tested here is carriage and honesty: that a document survives `job.json`
and a spawned child unchanged, that a runner may refine it at run time in either
language without disturbing the payload digest, and that a collect reports the
declared and the observed document side by side, including when one of them is
damaged.
"""

import json
import os
import subprocess
import sys
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from httk.workflow import (
    Attempt,
    ChildSpec,
    FormatError,
    JobRecord,
    RunnerRef,
    TaskManager,
    Workspace,
    job_records,
)
from httk.workflow.models import MAXIMUM_DECLARATIONS_BYTES, validate_declarations
from httk.workflow.protocol import JobDefinition, JobSpec, prepare_job_payload
from httk.workflow.transfers import _payload_digest

_SRC = str(Path(__file__).parents[1] / "src")
_SHELL = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "shell" / "httk-workflow.sh"

# One plausible declaration document. Nothing here reads any of it: the members
# that say which vocabulary and which version it follows live inside the document
# itself, which is exactly the point of carrying it verbatim.
_DECLARED: dict[str, Any] = {
    "$id": "https://example.org/workflows/relax/v1.0.0",
    "$schema": "https://schemas.optimade.org/defs/v1.2/workflow_declaration.json",
    "title": "Relaxation",
    "x-optimade-definition": {"kind": "workflow", "version": "1.0.0", "name": "relax"},
    "inputs": {"structure": {"$ref": "https://schemas.optimade.org/defs/v1.2/types/structure"}},
    "method": {"code": "example", "convergence": {"ediffg": -0.01}, "restarts": None},
    "outputs": {"structure": {"$ref": "https://schemas.optimade.org/defs/v1.2/types/structure"}},
    "unicode": "Ångström ≤ 1e-3",
}
_OBSERVED: dict[str, Any] = {**_DECLARED, "outputs": {"structures": 3, "labels": ["site-0", "site-1"]}}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_a_declaration_must_be_a_json_object() -> None:
    # Only the shape is checked, and only at the top level: whatever is inside a
    # document belongs to the vocabulary it names, not to this engine.
    assert validate_declarations({"workflow": _DECLARED}) == {"workflow": _DECLARED}
    assert validate_declarations({}) == {}
    for refused in (5, "a string", [1, 2], None, True):
        with pytest.raises(FormatError, match="declarations.workflow must be an object"):
            validate_declarations({"workflow": refused})
    with pytest.raises(FormatError, match="declarations must be an object"):
        validate_declarations([{"workflow": {}}])
    with pytest.raises(FormatError, match="must contain only JSON values"):
        validate_declarations({"workflow": {"structure": object()}})


def test_a_declaration_name_is_one_safe_path_component() -> None:
    for name in ("workflow", "_optimade_workflow", "relax.v1", "a-b_c.0", "Z" * 64):
        assert validate_declarations({name: {}}) == {name: {}}
    for refused in ("", ".", "..", "../escape", "a/b", ".hidden", "-leading", "x" * 65, "spaced name", "n\x00l"):
        with pytest.raises(FormatError):
            validate_declarations({refused: {}})
    with pytest.raises(FormatError, match="declarations key must be a nonempty string"):
        validate_declarations({7: {}})


def test_declarations_have_their_own_size_budget() -> None:
    filling = "x" * (MAXIMUM_DECLARATIONS_BYTES // 2)
    assert validate_declarations({"workflow": {"blob": filling}})["workflow"]["blob"] == filling
    with pytest.raises(FormatError, match=f"exceeds the {MAXIMUM_DECLARATIONS_BYTES}-byte limit"):
        validate_declarations({"workflow": {"blob": filling}, "second": {"blob": filling}})
    # The budget is the inputs budget again, not shared with it: a job may spend
    # both, because they bound two independent members of one job.json.
    spec = JobSpec(
        name="Both",
        workflow="tests.declarations",
        runner_path="files/runner",
        parameters={"blob": filling},
        declarations={"workflow": {"blob": filling}},
    )
    definition = JobDefinition.from_mapping(spec.as_mapping())
    assert definition.parameters["blob"] == filling
    assert definition.declarations["workflow"]["blob"] == filling


# ---------------------------------------------------------------------------
# job.json and spawned children
# ---------------------------------------------------------------------------


def _payload(root: Path, **spec: Any) -> Path:
    """Prepare one payload whose runner never has to run."""

    payload = root
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Declaring job",
            workflow="tests.declarations",
            runner_path="files/runner",
            initial_step="only",
            **spec,
        ),
    )
    return payload


def test_a_job_carries_its_declarations_verbatim(tmp_path: Path) -> None:
    payload = _payload(tmp_path / "payload", tag="declaring", declarations={"workflow": _DECLARED})

    stored = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    assert stored["declarations"] == {"workflow": _DECLARED}
    job = JobDefinition.from_path(payload / "job.json")
    assert job.declarations == {"workflow": _DECLARED}
    # A job that declares nothing says so with an empty mapping, and its job.json
    # stays exactly as small as it was: no member is written for it at all.
    plain_payload = _payload(tmp_path / "plain")
    assert JobDefinition.from_path(plain_payload / "job.json").declarations == {}
    assert "declarations" not in json.loads((plain_payload / "job.json").read_text(encoding="utf-8"))


def test_a_spawned_child_declares_for_itself_and_inherits_nothing(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="characterize", declarations={"workflow": _DECLARED})
    child = {"$id": "https://example.org/workflows/relax-site/v1.0.0", "method": {"code": "example"}}

    attempt.spawn(
        ChildSpec(
            step="relax",
            parameters={"site": 0},
            declarations={"workflow": child},
            runner=RunnerRef.workspace("campaign/run.py", "a" * 64),
        ),
        label="declaring-child",
    )
    attempt.spawn(
        ChildSpec(step="relax", runner=RunnerRef.workspace("campaign/run.py", "a" * 64)),
        label="silent-child",
    )

    jobs = {
        path.parent.name.split("--")[0]: json.loads(path.read_text(encoding="utf-8")) for path in _child_jobs(attempt)
    }
    assert jobs["declaring-child"]["declarations"] == {"workflow": child}
    # The parent's own declaration describes the parent, so nothing of it leaks
    # into a child: a child that declares nothing carries no declarations member.
    assert "declarations" not in jobs["silent-child"]


def _child_jobs(attempt: Attempt) -> list[Path]:
    return sorted(attempt.control.glob("outcome.tmp.*/children/jobs/*/job.json"))


# ---------------------------------------------------------------------------
# Declaring at run time, in both languages
# ---------------------------------------------------------------------------


def _fabricate(tmp_path: Path, *, step: str, declarations: dict[str, Any] | None = None) -> dict[str, str]:
    """Fabricate one attempt of one job and return its runner environment."""

    payload = _payload(tmp_path / "payload", declarations=declarations or {})
    control = payload / f".httk-attempt.{uuid.uuid4()}"
    control.mkdir()
    workdir = payload / "run"
    workdir.mkdir()
    (control / "context.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-attempt-context",
                "format_version": 2,
                "workspace_id": str(uuid.uuid4()),
                "job_id": str(uuid.uuid4()),
                "job_key": f"fabricated--{uuid.uuid4()}",
                "placement": "project/fabricated",
                "step": step,
                "activation_id": str(uuid.uuid4()),
                "attempt_id": str(uuid.uuid4()),
                "data_generation": None,
                "children": [],
            }
        ),
        encoding="utf-8",
    )
    return {
        "HTTK_WORKFLOW_CONTEXT": str(control / "context.json"),
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(payload),
        "HTTK_WORKFLOW_WORKDIR": str(workdir),
        "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
        "HTTK_WORKFLOW_STEP": step,
    }


def _attempt(tmp_path: Path, *, step: str, declarations: dict[str, Any] | None = None) -> Attempt:
    """Bind one fabricated attempt to this process, without a manager."""

    return Attempt.initialize(_fabricate(tmp_path, step=step, declarations=declarations))


def _bash(environment: dict[str, str], root: Path, body: str) -> "subprocess.CompletedProcess[str]":
    """Run one Bash runner whose single step is *body*."""

    runner = root / "runner.sh"
    runner.write_text(
        "#!/usr/bin/env bash\n"
        'set -euo pipefail\nsource "$HTTK_WORKFLOW_BASH_API"\n'
        "httk_workflow_runner tests.declarations only\n"
        f"step_only() {{\n{body}\n}}\n"
        "httk_workflow_main\n",
        encoding="utf-8",
    )
    full = os.environ.copy()
    full.update(environment)
    full["HTTK_WORKFLOW_PYTHON"] = sys.executable
    full["HTTK_WORKFLOW_BASH_API"] = str(_SHELL)
    for dropped in ("HTTK_WORKFLOW_DESCRIBE", "HTTK_WORKFLOW_RUNNER_WORKFLOW", "HTTK_WORKFLOW_RUNNER_STEPS"):
        full.pop(dropped, None)
    return subprocess.run(
        ["bash", str(runner)],
        cwd=environment["HTTK_WORKFLOW_WORKDIR"],
        env=full,
        text=True,
        capture_output=True,
        check=False,
    )


def _bridge(environment: dict[str, str], *arguments: str) -> "subprocess.CompletedProcess[str]":
    """Call one bridge subcommand exactly as the Bash library calls it."""

    full = os.environ.copy()
    full.update(environment)
    full["PYTHONPATH"] = os.pathsep.join(filter(None, (_SRC, os.environ.get("PYTHONPATH", ""))))
    return subprocess.run(
        [sys.executable, "-m", "httk.workflow._shell_bridge", *arguments],
        cwd=environment["HTTK_WORKFLOW_WORKDIR"],
        env=full,
        text=True,
        capture_output=True,
        check=False,
    )


def test_declaring_records_the_observed_document_and_reading_prefers_it(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="only", declarations={"workflow": _DECLARED})
    stored = attempt.payload / ".httk-job" / "declarations" / "workflow.json"

    # Nothing observed yet: the declared document of job.json is the answer.
    assert attempt.declaration("workflow") == _DECLARED
    assert attempt.declaration("absent") is None
    assert not stored.exists()

    assert attempt.declare("workflow", _OBSERVED) == stored
    assert attempt.declaration("workflow") == _OBSERVED
    assert json.loads(stored.read_text(encoding="utf-8")) == _OBSERVED

    # A campaign refines what it observed as it learns it, so the last word wins
    # and the declared document is left exactly as job.json carried it.
    attempt.declare("workflow", {**_OBSERVED, "outputs": {"structures": 4}})
    assert attempt.declaration("workflow") == {**_OBSERVED, "outputs": {"structures": 4}}
    assert attempt.job.declarations == {"workflow": _DECLARED}

    # An observed name job.json never declared is still recorded and read back.
    attempt.declare("observed_only", {"$id": "https://example.org/hints/v1"})
    assert attempt.declaration("observed_only") == {"$id": "https://example.org/hints/v1"}

    for refused in ("../escape", "a/b", ""):
        with pytest.raises(FormatError):
            attempt.declare(refused, _OBSERVED)
    with pytest.raises(FormatError):
        invalid_document = cast(Mapping[str, object], ["not an object"])
        attempt.declare("workflow", invalid_document)


def test_a_bash_step_and_a_python_step_declare_the_same_bytes(tmp_path: Path) -> None:
    scenario = {"workflow": _DECLARED}

    python_environment = _fabricate(tmp_path / "python", step="only", declarations=scenario)
    attempt = Attempt.initialize(python_environment)
    assert attempt.declaration("workflow") == _DECLARED
    attempt.declare("workflow", _OBSERVED)
    attempt.declare("observed_only", {"note": "discovered at run time"})
    attempt.succeed()

    shell_environment = _fabricate(tmp_path / "bash", step="only", declarations=scenario)
    workdir = Path(shell_environment["HTTK_WORKFLOW_WORKDIR"])
    (workdir / "observed.json").write_text(json.dumps(_OBSERVED), encoding="utf-8")
    (workdir / "extra.json").write_text(json.dumps({"note": "discovered at run time"}), encoding="utf-8")
    completed = _bash(
        shell_environment,
        tmp_path / "bash",
        "    httk_workflow_declaration workflow >declared.json\n"
        "    httk_workflow_declare workflow observed.json\n"
        "    httk_workflow_declare observed_only extra.json\n"
        "    httk_workflow_succeed",
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads((workdir / "declared.json").read_text(encoding="utf-8")) == _DECLARED

    # The same scenario in two languages leaves byte-identical documents behind,
    # because both publish through exactly one implementation.
    python_files = sorted((Path(python_environment["HTTK_WORKFLOW_JOB_DIR"]) / ".httk-job" / "declarations").iterdir())
    shell_files = sorted((Path(shell_environment["HTTK_WORKFLOW_JOB_DIR"]) / ".httk-job" / "declarations").iterdir())
    assert [path.name for path in python_files] == ["observed_only.json", "workflow.json"]
    assert [path.name for path in shell_files] == [path.name for path in python_files]
    assert [path.read_bytes() for path in shell_files] == [path.read_bytes() for path in python_files]


def test_the_bridge_keeps_the_absent_and_refused_exit_codes(tmp_path: Path) -> None:
    environment = _fabricate(tmp_path, step="only", declarations={"workflow": _DECLARED})
    document = Path(environment["HTTK_WORKFLOW_WORKDIR"]) / "document.json"
    document.write_text(json.dumps(_OBSERVED), encoding="utf-8")

    declared = _bridge(environment, "declaration", "workflow")
    assert declared.returncode == 0
    assert json.loads(declared.stdout) == _DECLARED

    absent = _bridge(environment, "declaration", "nothing_declared_this")
    assert absent.returncode == 1 and not absent.stdout

    assert _bridge(environment, "declare", "workflow", str(document)).returncode == 0
    observed = _bridge(environment, "declaration", "workflow")
    assert observed.returncode == 0 and json.loads(observed.stdout) == _OBSERVED

    refused = _bridge(environment, "declare", "../escape", str(document))
    assert refused.returncode == 2 and "invalid component syntax" in refused.stderr
    unreadable = _bridge(environment, "declare", "workflow", str(document) + ".missing")
    assert unreadable.returncode == 2
    array = Path(environment["HTTK_WORKFLOW_WORKDIR"]) / "array.json"
    array.write_text("[1, 2, 3]", encoding="utf-8")
    assert _bridge(environment, "declare", "workflow", str(array)).returncode == 2


# ---------------------------------------------------------------------------
# Collecting and the payload digest
# ---------------------------------------------------------------------------


_DECLARING_RUNNER = f'''#!/usr/bin/env python3
"""A job that refines its own declaration once it knows what it produced."""

import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.declarations")


@run.step
def only(a):
    declared = a.declaration("workflow")
    a.declare("workflow", dict(declared, outputs={{"structures": 3}}))
    a.declare("observed_only", {{"$id": "https://example.org/hints/v1", "note": "runtime"}})
    (a.workdir / "done.txt").write_text("done\\n", encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
'''


def _campaign(root: Path) -> tuple[Workspace, str]:
    """Run one declaring job to completion in its own workspace."""

    root.mkdir(parents=True)
    workspace = Workspace.initialize(root / "workspace")
    payload = root / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(_DECLARING_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Declaring job",
            workflow="tests.declarations",
            runner_path="files/runner",
            tag="declaring",
            initial_step="only",
            maximum_attempts_per_activation=1,
            declarations={"workflow": _DECLARED, "declared_only": {"$id": "https://example.org/static/v1"}},
        ),
    )
    workspace.submit(payload, "project/declaring")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    return workspace, job.id


def test_a_collect_reports_declared_and_observed_side_by_side(tmp_path: Path) -> None:
    workspace, _ = _campaign(tmp_path / "run")

    records = list(job_records(workspace))
    assert len(records) == 1
    record = records[0]
    assert not record.gaps
    # Every name either source knows appears once, and the two sides are exactly
    # what each source said — never merged into one document.
    assert sorted(record.declarations) == ["declared_only", "observed_only", "workflow"]
    assert record.declarations["workflow"] == {
        "declared": _DECLARED,
        "observed": {**_DECLARED, "outputs": {"structures": 3}},
    }
    assert record.declarations["declared_only"] == {
        "declared": {"$id": "https://example.org/static/v1"},
        "observed": None,
    }
    assert record.declarations["observed_only"] == {
        "declared": None,
        "observed": {"$id": "https://example.org/hints/v1", "note": "runtime"},
    }

    # A record survives being written, shipped, and read back unchanged.
    shipped = json.loads(json.dumps(record.as_mapping()))
    assert JobRecord.from_mapping(shipped).declarations == record.declarations
    assert JobRecord.from_mapping(shipped).as_mapping() == record.as_mapping()


def test_an_unreadable_observed_declaration_is_a_gap_and_not_a_silence(tmp_path: Path) -> None:
    workspace, job_id = _campaign(tmp_path / "run")
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    declarations = workspace.payload_path(marker.placement, marker.job_key) / ".httk-job" / "declarations"
    (declarations / "workflow.json").write_text("{ this is not JSON", encoding="utf-8")

    record = next(iter(job_records(workspace)))
    # The result still exists, so it is still collected: the damaged side is
    # reported as absent, the intact side is untouched, and the record says its
    # history is not complete.
    assert record.gaps
    assert record.declarations["workflow"] == {"declared": _DECLARED, "observed": None}
    assert record.declarations["observed_only"]["observed"] == {
        "$id": "https://example.org/hints/v1",
        "note": "runtime",
    }
    assert JobRecord.from_mapping(record.as_mapping()).declarations == record.declarations


def test_declaring_does_not_move_the_payload_digest(tmp_path: Path) -> None:
    source, job_id = _campaign(tmp_path / "run")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.find_marker_by_id(job_id)
    assert marker is not None
    payload = source.payload_path(marker.placement, marker.job_key)
    assert (payload / ".httk-job" / "declarations" / "workflow.json").is_file()
    digest = source.payload_digest(marker)

    # Observed declarations live below .httk-job/, so writing one more cannot
    # move the digest the manager, a registration check, or a transfer verifies.
    (payload / ".httk-job" / "declarations" / "late.json").write_text('{"late": true}', encoding="utf-8")
    assert source.payload_digest(marker) == digest

    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = json.loads((bundle / ".httk-transfer" / "manifest.json").read_text(encoding="utf-8"))
    assert destination.import_bundle(bundle)["job_id"] == job_id
    imported = destination.find_marker_by_id(job_id)
    assert imported is not None
    imported_payload = destination.payload_path(imported.placement, imported.job_key)
    assert destination.payload_digest(imported) == digest
    # The bundle digest is stable across the transfer too, even though the
    # payload carries declarations the digest deliberately ignores.
    assert _payload_digest(imported_payload) == manifest["payload_sha256"]

    # And the declarations travelled with the payload, so the job collects the
    # same way at home as it did where it ran.
    record = next(iter(job_records(destination)))
    assert record.declarations["workflow"]["observed"] == {**_DECLARED, "outputs": {"structures": 3}}
    assert record.declarations["late"] == {"declared": None, "observed": {"late": True}}
