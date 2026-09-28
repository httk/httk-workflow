"""The Perl authoring SDK: describe parity and runner dispatch.

The Perl SDK is a bridge client, exactly like the Bash, C, Fortran, and Rust
ones: every bridge-backed verb spawns ``$HTTK_WORKFLOW_PYTHON -m
httk.workflow._shell_bridge``, and only ``--describe`` is native. What is
tested here is what only the Perl half can get wrong: warning-clean syntax,
byte-identical description output, dispatch, and outcome handling.
"""

import json
import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import httk.workflow
from httk.workflow import Workspace
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.scaffold import describe_runner
from test_bash_sdk import _CALL_SUB_RUNNER

pytestmark = pytest.mark.skipif(shutil.which("perl") is None, reason="no Perl interpreter is available")

_PERL = shutil.which("perl")
_PERL_SDK = Path(httk.workflow.__file__).parent / "languages" / "perl"
_SHELL = Path(httk.workflow.__file__).parent / "languages" / "bash" / "httk-workflow.sh"


def _runner(
    tmp_path: Path,
    workflow: str = "tests.perl",
    steps: tuple[str, ...] = ("prepare",),
    handlers: dict[str, str] | None = None,
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    script = tmp_path / "runner.pl"
    quoted_steps = ", ".join("'" + step + "'" for step in steps)
    handlers = handlers or {}
    registrations = "\n".join(
        f"$runner->step('{step}' => sub {{ my ($attempt) = @_; {handlers.get(step, '$attempt->succeed();')} return 0; }});"
        for step in steps
    )
    script.write_text(
        "#!/usr/bin/env perl\n"
        f"use lib '{_PERL_SDK}';\n"
        "use HttkWorkflow;\n"
        f"my $runner = HttkWorkflow::Runner->new(workflow => '{workflow}', steps => [{quoted_steps}]);\n"
        f"{registrations}\n"
        "$runner->main();\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


@dataclass(frozen=True)
class _Attempt:
    """One fabricated attempt a Perl runner can be dispatched into."""

    payload: Path
    control: Path
    workdir: Path
    environment: dict[str, str]

    def run(self, runner: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(runner), *arguments],
            cwd=self.workdir,
            env=self.environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def outcome(self) -> dict[str, Any]:
        return json.loads((self.control / "outcome.ready" / "outcome.json").read_text(encoding="utf-8"))

    def breadcrumb(self) -> dict[str, Any]:
        return json.loads((self.control / "error.json").read_text(encoding="utf-8"))


def _parent_job(tmp_path: Path) -> dict[str, str]:
    """Fabricate a persistent-workdir parent payload in the workspace; return a child's ``parent`` block."""

    staging = tmp_path / "workspace" / "project" / "parent" / "staging"
    (staging / "files").mkdir(parents=True)
    (staging / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    spec = JobSpec(
        name="Parent", workflow="tests.parent", runner_path="files/runner", initial_step="start", workdir_path="calc"
    )
    job_id = prepare_job_payload(staging, spec).id
    staging.rename(staging.with_name(f"parent--{job_id}"))
    return {
        "workspace_id": str(uuid.uuid4()),
        "job_id": job_id,
        "job_key": f"parent--{job_id}",
        "placement": "project/parent",
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    data_generation: int | None = None,
    parent: dict[str, str] | None = None,
) -> _Attempt:
    """Fabricate one attempt of one job, without a manager (mirrors test_rust_api)."""

    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.perl",
            runner_path="files/runner",
            initial_step=step,
            data_mode="none" if data_generation is None else "transactional",
            parameters=parameters or {},
        ),
        parent=parent,
    )
    control = payload / f"attempts/{uuid.uuid4()}"
    control.mkdir(parents=True)
    workdir = payload / "run"
    workdir.mkdir()
    context_json = json.dumps(
        {
            "format": "httk-workflow-attempt-context",
            "format_version": 2,
            "workspace_id": parent["workspace_id"] if parent else str(uuid.uuid4()),
            "job_id": str(uuid.uuid4()),
            "job_key": f"fabricated--{uuid.uuid4()}",
            "placement": "project/fabricated",
            "payload": str(payload),
            "step": step,
            "activation_id": str(uuid.uuid4()),
            "attempt_id": str(uuid.uuid4()),
            "data_generation": data_generation,
            "children": [],
            "settings": {},
        }
    )
    environment = os.environ.copy()
    environment.update(
        {
            "HTTK_WORKFLOW_CONTEXT": context_json,
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(payload),
            "HTTK_WORKFLOW_WORKDIR": str(workdir),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
            "HTTK_WORKFLOW_STEP": step,
            "HTTK_WORKFLOW_PYTHON": sys.executable,
        }
    )
    if data_generation is not None:
        environment["HTTK_WORKFLOW_DATA_DIR"] = str(payload / "data")
    for name in ("HTTK_WORKFLOW_DESCRIBE", "HTTK_WORKFLOW_RUNNER_WORKFLOW", "HTTK_WORKFLOW_RUNNER_STEPS"):
        environment.pop(name, None)
    return _Attempt(payload, control, workdir, environment)


def _call_target(tmp_path: Path) -> tuple[Path, Path]:
    """Initialize the attempt's workspace; write a callable Python runner file and a file to stage."""

    Workspace.initialize(tmp_path / "workspace")
    sub = tmp_path / "sub_runner.py"
    sub.write_text(_CALL_SUB_RUNNER.format(src=Path(__file__).parents[1] / "src"), encoding="utf-8")
    sub.chmod(0o755)
    input_file = tmp_path / "input.txt"
    input_file.write_text("staged-by-call\n", encoding="utf-8")
    return sub, input_file


def _assert_called(attempt: _Attempt, job_key: str) -> None:
    """The outcome registered one ``sub`` child running the called workflow, with the staged file."""

    ready = attempt.control / "outcome.ready"
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert [(entry["label"], entry["job_key"]) for entry in spawn["children"]] == [("sub", job_key)]
    child_dir = ready / "children" / "jobs" / job_key
    child = json.loads((child_dir / "job.json").read_text(encoding="utf-8"))
    assert child["workflow"] == "tests.sub"
    assert child["runner"]["source"] == "workspace"
    assert (child_dir / "files" / "input.txt").read_text(encoding="utf-8") == "staged-by-call\n"
    assert attempt.outcome()["join"]["condition"] == "all_succeeded"


def test_the_module_is_warning_clean() -> None:
    assert _PERL is not None
    completed = subprocess.run(
        [_PERL, "-cw", str(_PERL_SDK / "HttkWorkflow.pm")],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stderr.endswith("HttkWorkflow.pm syntax OK\n")
    assert "warning" not in completed.stderr.lower()


def test_a_published_copy_uses_the_manager_sdk_path(tmp_path: Path) -> None:
    """A runner outside any source tree finds the SDK only through the described environment."""

    published = tmp_path / "runner.pl"
    published.write_text(
        "#!/usr/bin/env perl\n"
        "use lib $ENV{HTTK_WORKFLOW_PERL_API};\n"
        "use HttkWorkflow;\n"
        "my $runner = HttkWorkflow::Runner->new(workflow => 'tests.perl.published', steps => ['run', 'prepare']);\n"
        "$runner->step($_ => sub { return 0; }) for ('run', 'prepare');\n"
        "$runner->main();\n",
        encoding="utf-8",
    )
    published.chmod(0o555)
    assert describe_runner(published) == {"workflow": "tests.perl.published", "steps": ["prepare", "run"]}


def test_describe_is_byte_identical_to_the_bash_sdk(tmp_path: Path) -> None:
    workflow = "tests.perl.describe"
    order = ("relax", "collect", "prepare")
    script = _runner(tmp_path, workflow, order)

    bash_environment = dict(os.environ)
    bash_environment["HTTK_WORKFLOW_DESCRIBE"] = "1"
    bash_environment["HTTK_WORKFLOW_BASH_API"] = str(_SHELL)
    bash = subprocess.run(
        ["bash", "-c", 'source "$1"; httk_workflow_runner "$2" "${@:3}"', "bash", str(_SHELL), workflow, *order],
        text=True,
        capture_output=True,
        check=False,
        env=bash_environment,
    )
    assert bash.returncode == 0, bash.stderr
    perl = subprocess.run([str(script), "--describe"], text=True, capture_output=True, check=False)
    assert perl.returncode == 0, perl.stderr
    expected = (
        '{"format": "httk-workflow-runner-description", "format_version": 2, '
        '"steps": ["collect", "prepare", "relax"], "workflow": "tests.perl.describe"}\n'
    )
    assert bash.stdout == expected
    assert perl.stdout == expected
    assert perl.stdout == bash.stdout


def test_invalid_workflow_id_refuses_before_describing(tmp_path: Path) -> None:
    script = _runner(tmp_path, "bad id")
    completed = subprocess.run([str(script), "--describe"], text=True, capture_output=True, check=False)
    assert completed.returncode == 2
    assert completed.stdout == ""


def test_workflow_id_with_trailing_newline_refuses_before_describing(tmp_path: Path) -> None:
    script = _runner(tmp_path, "bad\n")
    completed = subprocess.run([str(script), "--describe"], text=True, capture_output=True, check=False)
    assert completed.returncode == 2
    assert completed.stdout == ""


def test_unknown_step_publishes_a_structured_failure_and_exits_zero(tmp_path: Path) -> None:
    runner = _runner(tmp_path, steps=("relax", "collect"))
    attempt = _attempt(tmp_path, step="realx")
    completed = attempt.run(runner)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"]["code"] == "unknown_step"
    assert "registered steps: collect, relax" in outcome["failure"]["message"]


def test_a_step_handler_is_dispatched(tmp_path: Path) -> None:
    runner = _runner(
        tmp_path,
        steps=("go",),
        handlers={
            "go": "open my $fh, '>', 'handler-ran.txt' or return 3; print $fh 'yes'; close $fh; $attempt->succeed();"
        },
    )
    attempt = _attempt(tmp_path, step="go")
    completed = attempt.run(runner)
    assert completed.returncode == 0, completed.stderr
    assert (attempt.workdir / "handler-ran.txt").read_text(encoding="utf-8") == "yes"
    assert attempt.outcome()["action"] == "succeed"


def test_log_writes_timestamped_stderr_from_a_handler(tmp_path: Path) -> None:
    runner = _runner(
        tmp_path, steps=("log",), handlers={"log": "$attempt->log('info', 'hello from Perl'); $attempt->succeed();"}
    )
    attempt = _attempt(tmp_path, step="log")
    completed = attempt.run(runner)
    assert completed.returncode == 0, completed.stderr
    assert "[info] hello from Perl" in completed.stderr


def test_a_missing_read_is_absent_but_a_refused_read_dies(tmp_path: Path) -> None:
    absent_runner = _runner(
        tmp_path / "absent",
        steps=("read",),
        handlers={"read": "die 'unexpected value' if defined $attempt->state_get('missing'); $attempt->succeed();"},
    )
    absent = _attempt(tmp_path / "absent", step="read")
    completed = absent.run(absent_runner)
    assert completed.returncode == 0, completed.stderr
    assert absent.outcome()["action"] == "succeed"

    refused_runner = _runner(tmp_path / "refused", steps=("read",), handlers={"read": "$attempt->children('--bogus');"})
    refused = _attempt(tmp_path / "refused", step="read")
    completed = refused.run(refused_runner)
    assert completed.returncode == 2
    assert not (refused.control / "outcome.ready").exists()
    assert refused.breadcrumb()["exception"] == "PerlError"


def test_a_handler_can_publish_a_structured_failure(tmp_path: Path) -> None:
    runner = _runner(
        tmp_path,
        steps=("start",),
        handlers={"start": "$attempt->fail('tests.broken', 'it broke', 0);"},
    )
    attempt = _attempt(tmp_path, step="start")
    completed = attempt.run(runner)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["failure"] == {"code": "tests.broken", "message": "it broke"}


def test_a_handler_returning_nonzero_leaves_a_perl_error_breadcrumb(tmp_path: Path) -> None:
    runner = _runner(tmp_path, steps=("explode",), handlers={"explode": "return 3;"})
    attempt = _attempt(tmp_path, step="explode")
    completed = attempt.run(runner)
    assert completed.returncode == 3
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["format"] == "httk-workflow-runner-error"
    assert breadcrumb["step"] == "explode"
    assert breadcrumb["exception"] == "PerlError"
    assert breadcrumb["message"] == "explode exited with status 3"


def test_stage_input_copies_a_payload_file_or_answers_false(tmp_path: Path) -> None:
    body = (
        "print $attempt->stage_input('poscar', 'POSCAR', 'files/POSCAR'), "
        "$attempt->stage_input('incar', 'INCAR', 'files/INCAR'), "
        "$attempt->stage_input('missing', 'X'), \"\\n\"; "
        "eval { $attempt->stage_input('encut', 'X'); 1 } or print ref($@), ' ', $@->kind(), \"\\n\"; "
        "$attempt->succeed();"
    )
    runner = _runner(tmp_path, steps=("start",), handlers={"start": body})
    attempt = _attempt(tmp_path, step="start", parameters={"encut": 520})
    (attempt.payload / "files" / "POSCAR").write_bytes(b"Si\n1.0\n")

    completed = attempt.run(runner)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "100\nHttkWorkflow::BridgeError Refused\n"
    assert (attempt.workdir / "POSCAR").read_bytes() == b"Si\n1.0\n"
    assert attempt.outcome()["action"] == "succeed"


def test_parent_reads_the_parent_location_or_answers_undef(tmp_path: Path) -> None:
    body = (
        "print join(\"\\n\", map { $attempt->parent($_) } qw(payload workdir job_id)), \"\\n\", "
        "$attempt->parent(), \"\\n\"; "
        "$attempt->succeed();"
    )
    runner = _runner(tmp_path, steps=("start",), handlers={"start": body})
    parent = _parent_job(tmp_path)
    completed = _attempt(tmp_path, step="start", parent=parent).run(runner)
    assert completed.returncode == 0, completed.stderr
    payload = tmp_path / "workspace" / "project" / "parent" / parent["job_key"]
    lines = completed.stdout.splitlines()
    assert lines[:3] == [str(payload), str(payload / "calc"), parent["job_id"]]
    assert json.loads(lines[3])["spawn_id"] == parent["spawn_id"]

    orphan = _runner(
        tmp_path / "orphan", steps=("start",), handlers={"start": "print defined($attempt->parent()) ? 1 : 0;"}
    )
    completed = _attempt(tmp_path / "orphan", step="start").run(orphan)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "0"


def test_call_spawns_another_workflow_as_a_child(tmp_path: Path) -> None:
    sub, input_file = _call_target(tmp_path)
    body = (
        f"print $attempt->call('sub', '{sub}', ['--file', 'input.txt={input_file}']), \"\\n\"; "
        "$attempt->gather('finish');"
    )
    runner = _runner(tmp_path, steps=("start", "finish"), handlers={"start": body, "finish": ""})
    attempt = _attempt(tmp_path, step="start")
    completed = attempt.run(runner)
    assert completed.returncode == 0, completed.stderr
    _assert_called(attempt, completed.stdout.strip())
