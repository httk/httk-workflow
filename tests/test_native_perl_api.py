"""The native Perl authoring SDK: describe parity and runner dispatch.

The Perl SDK is a bridge client, exactly like the Bash, C, Fortran, and Rust
ones: every bridge-backed verb spawns ``$HTTK_WORKFLOW_PYTHON -m
httk.workflow._shell_bridge``, and only ``--describe`` is native. What is
tested here is what only the Perl half can get wrong: warning-clean syntax,
byte-identical description output, dispatch, outcome handling, and one real
VASP relaxation through a real manager.
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
from httk.workflow import TaskManager, Workspace
from httk.workflow.protocol import JobSpec, prepare_job_payload

pytestmark = pytest.mark.skipif(shutil.which("perl") is None, reason="no Perl interpreter is available")

_PERL = shutil.which("perl")
_PERL_SDK = Path(httk.workflow.__file__).parent / "native" / "perl"
_SHELL = Path(httk.workflow.__file__).parent / "shell" / "httk-workflow.sh"
_RELAX_PERL = Path(__file__).parents[1] / "examples" / "relax_perl" / "relax.pl"


_MOCK_VASP = Path(__file__).parents[1] / "examples" / "mock_vasp.py"
_RELAX_PERL_DIR = Path(__file__).parents[1] / "examples" / "relax_perl"
_POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
"""


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


def _attempt(tmp_path: Path, *, step: str, data_generation: int | None = None) -> _Attempt:
    """Fabricate one attempt of one job, without a manager (mirrors test_native_rust_api)."""

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
        ),
    )
    control = payload / f".httk-attempt.{uuid.uuid4()}"
    control.mkdir()
    workdir = payload / "run"
    workdir.mkdir()
    (control / "context.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-attempt-context",
                "format_version": 1,
                "workspace_id": str(uuid.uuid4()),
                "job_id": str(uuid.uuid4()),
                "job_key": f"fabricated--{uuid.uuid4()}",
                "placement": "project/fabricated",
                "step": step,
                "activation_id": str(uuid.uuid4()),
                "attempt_id": str(uuid.uuid4()),
                "data_generation": data_generation,
                "children": [],
                "settings": {},
            }
        ),
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "HTTK_WORKFLOW_CONTEXT": str(control / "context.json"),
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


def test_the_sdk_and_example_are_warning_clean() -> None:
    assert _PERL is not None
    for source in (_PERL_SDK / "HttkWorkflow.pm", _RELAX_PERL):
        completed = subprocess.run([_PERL, "-cw", str(source)], text=True, capture_output=True, check=False)
        assert completed.returncode == 0, completed.stderr
        assert completed.stderr.endswith(f"{source.name} syntax OK\n")
        assert "warning" not in completed.stderr.lower()


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
        '{"format": "httk-workflow-runner-description", "format_version": 1, '
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


def test_the_relax_runner_prepares_runs_and_publishes(tmp_path: Path) -> None:
    """The examples/relax_perl runner, driven end to end through a real manager."""

    # A workspace runner keeps the example's documented, script-relative `use lib`
    # wiring intact while giving the installed script its sibling source tree.
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "POSCAR").write_text(_POSCAR, encoding="utf-8")

    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("vasp.command", f"{sys.executable} {_MOCK_VASP}")
    reference = dict(workspace.publish_runner(_RELAX_PERL_DIR / "relax.pl", name="relax"))
    # The manager launches a workspace-file runner from the attempt control
    # directory; preserve the example's source-relative layout at that level.
    module = workspace.root / "project" / "vasp" / "src" / "httk" / "workflow" / "native" / "perl"
    module.mkdir(parents=True)
    shutil.copyfile(_PERL_SDK / "HttkWorkflow.pm", module / "HttkWorkflow.pm")
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Perl relaxation",
            workflow="httk.vasp.relax-perl",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="silicon",
            initial_step="prepare",
            data_mode="transactional",
            maximum_total_attempts=8,
        ),
    )
    workspace.submit(payload, "project/vasp")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)

    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    root = workspace.payload_path(marker.placement, marker.job_key)
    state = json.loads((root / ".httk-job" / "state.json").read_text(encoding="utf-8"))
    assert state["classification"] == "completed"
    published = root / "data" / "vasp"
    assert (published / "OUTCAR").is_file()
    contcar = (published / "CONTCAR").read_text(encoding="utf-8").splitlines()
    assert contcar[-1].startswith("0.51")
