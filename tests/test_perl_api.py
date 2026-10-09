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
from pathlib import Path

import pytest

import httk.workflow
from attempt_fixtures import FabricatedAttempt, assert_called, every_job, fabricate, sub_package
from httk.workflow.scaffold import describe_runner

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


_Attempt = FabricatedAttempt


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    parent: bool = False,
    calls: bool = False,
) -> FabricatedAttempt:
    """Fabricate one attempt of one job of ``tests.perl``, without a manager."""

    return fabricate(
        tmp_path,
        step=step,
        workflow="tests.perl",
        parameters=parameters,
        parent=parent,
        calls={"sub": sub_package(tmp_path / "sub")} if calls else None,
    )


def _parent_of(attempt: FabricatedAttempt) -> tuple[Path, str, str]:
    """Return the waiting parent's job directory, id and the child's spawn id."""

    (parent,) = every_job(attempt.workspace)
    recorded = json.loads((attempt.payload / "job.json").read_text(encoding="utf-8"))["parent"]
    return parent.path, parent.job_id, str(recorded["spawn_id"])


def _call_input(tmp_path: Path) -> Path:
    """Write the file a call stages into the child."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "input.txt"
    path.write_text("staged-by-call\n", encoding="utf-8")
    return path


def _assert_called(attempt: FabricatedAttempt, job_key: str | None = None) -> None:
    """The outcome registered one ``sub`` child of the installed ``tests.sub``, with the staged file."""

    key = assert_called(attempt, job_key)
    staged = attempt.control / "outcome.ready" / "children" / "jobs" / key / "files" / "input.txt"
    assert staged.read_text(encoding="utf-8") == "staged-by-call\n"
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
    child = _attempt(tmp_path, step="start", parent=True)
    completed = child.run(runner)
    assert completed.returncode == 0, completed.stderr
    payload, job_id, spawn_id = _parent_of(child)
    lines = completed.stdout.splitlines()
    assert lines[:3] == [str(payload), str(payload / "run"), job_id]
    assert json.loads(lines[3])["spawn_id"] == spawn_id

    orphan = _runner(
        tmp_path / "orphan", steps=("start",), handlers={"start": "print defined($attempt->parent()) ? 1 : 0;"}
    )
    completed = _attempt(tmp_path / "orphan", step="start").run(orphan)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "0"


def test_call_spawns_an_installed_workflow_as_a_child(tmp_path: Path) -> None:
    input_file = _call_input(tmp_path)
    body = (
        f"print $attempt->call('sub', 'sub', ['--file', 'input.txt={input_file}']), \"\\n\"; "
        "$attempt->gather('finish');"
    )
    runner = _runner(tmp_path, steps=("start", "finish"), handlers={"start": body, "finish": ""})
    attempt = _attempt(tmp_path, step="start", calls=True)
    completed = attempt.run(runner)
    assert completed.returncode == 0, completed.stderr
    _assert_called(attempt, completed.stdout.strip())
