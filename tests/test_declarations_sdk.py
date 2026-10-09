"""Workflow declarations in the runner SDK and the Bash bridge.

Split out of ``test_declarations.py``; the description below is the original module's.

Workflow declarations: carried verbatim, declared statically, observed at run time.

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

from httk.workflow import Attempt, ChildSpec, FormatError
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload

_SRC = str(Path(__file__).parents[1] / "src")
_SHELL = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "languages" / "bash" / "httk-workflow.sh"

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


def _payload(root: Path, **spec: Any) -> Path:
    """Prepare one payload whose runner never has to run."""

    prepare_job_payload(
        root,
        JobSpec(
            name="Declaring job",
            workflow_id="local:tests.declarations",
            workflow_name="tests.declarations",
            initial_step="only",
            **spec,
        ),
    )
    return root


def test_a_spawned_child_declares_for_itself_and_inherits_nothing(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path, step="characterize", declarations={"workflow": _DECLARED})
    child = {"$id": "https://example.org/workflows/relax-site/v1.0.0", "method": {"code": "example"}}

    attempt.spawn(
        ChildSpec(
            step="relax",
            parameters={"site": 0},
            declarations={"workflow": child},
        ),
        label="declaring-child",
    )
    attempt.spawn(ChildSpec(step="relax"), label="silent-child")

    jobs = {
        path.parent.name.split("--")[0]: json.loads(path.read_text(encoding="utf-8")) for path in _child_jobs(attempt)
    }
    assert jobs["declaring-child"]["declarations"] == {"workflow": child}
    # The parent's own declaration describes the parent, so nothing of it leaks
    # into a child: a child that declares nothing carries empty declarations.
    assert jobs["silent-child"]["declarations"] == {}


def _child_jobs(attempt: Attempt) -> list[Path]:
    return sorted(attempt.control.glob("outcome.tmp.*/children/jobs/*/job.json"))


# ---------------------------------------------------------------------------
# Declaring at run time, in both languages
# ---------------------------------------------------------------------------


def _fabricate(tmp_path: Path, *, step: str, declarations: dict[str, Any] | None = None) -> dict[str, str]:
    """Fabricate one attempt of one job and return its runner environment."""

    payload = _payload(tmp_path / "payload", declarations=declarations or {})
    control = payload / f"attempts/{uuid.uuid4()}"
    control.mkdir(parents=True)
    workdir = payload / "run"
    workdir.mkdir()
    context_json = json.dumps(
        {
            "format": "httk-workflow-attempt-context",
            "durable": False,
            "deadline": None,
            "settings": {},
            "format_version": 2,
            "workspace_id": str(uuid.uuid4()),
            "job_id": (_jid := str(uuid.uuid4())),
            "job_key": f"fabricated--{_jid}",
            "placement": "project/fabricated",
            "payload": str(payload),
            "step": step,
            "activation_id": str(uuid.uuid4()),
            "attempt_id": str(uuid.uuid4()),
            "children": [],
        }
    )
    return {
        "HTTK_WORKFLOW_CONTEXT": context_json,
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(payload),
        "HTTK_WORKFLOW_WORKDIR": str(workdir),
        "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
        "HTTK_WORKFLOW_DATA_DIR": str(payload / "data"),
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
