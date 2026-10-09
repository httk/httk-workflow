"""The Java authoring SDK: compile cleanliness, parity, dispatch, and outcomes."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

import httk.workflow
from attempt_fixtures import FabricatedAttempt, assert_called, every_job, fabricate, sub_package

_JAVAC = shutil.which("javac")
_JAVA = shutil.which("java")
pytestmark = pytest.mark.skipif(_JAVAC is None or _JAVA is None, reason="javac and java are required")

_JAVA_SDK = Path(httk.workflow.__file__).parent / "languages" / "java" / "HttkWorkflow.java"
_SHELL = Path(httk.workflow.__file__).parent / "languages" / "bash" / "httk-workflow.sh"


def _compile(output: Path, *sources: Path) -> subprocess.CompletedProcess[str]:
    assert _JAVAC is not None
    output.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [_JAVAC, "--release", "17", "-Werror", "-Xlint:all", "-d", str(output), *(str(source) for source in sources)],
        text=True,
        capture_output=True,
        check=False,
    )


def _runner_source(workflow: str, steps: dict[str, str]) -> str:
    names = ", ".join(json.dumps(name) for name in steps)
    registrations = "\n".join(
        f"                .step({json.dumps(name)}, attempt -> {{ {body} }})" for name, body in steps.items()
    )
    return f"""public final class RunnerMain {{
    private RunnerMain() {{}}

    public static void main(String[] args) {{
        new HttkWorkflow.Runner({json.dumps(workflow)}, new String[]{{{names}}})
{registrations}
                .main(args);
    }}
}}
"""


def _write_runner(tmp_path: Path, workflow: str, steps: dict[str, str], name: str = "runner") -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "RunnerMain.java"
    source.write_text(_runner_source(workflow, steps), encoding="utf-8")
    classes = tmp_path / f"{name}-classes"
    result = _compile(classes, _JAVA_SDK, source)
    assert result.returncode == 0, result.stderr
    return classes


_Attempt = FabricatedAttempt


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    parent: bool = False,
    calls: bool = False,
) -> FabricatedAttempt:
    """Fabricate one attempt of one job of ``tests.java``, without a manager."""

    return fabricate(
        tmp_path,
        step=step,
        workflow="tests.java",
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


def test_sdk_and_a_runner_compile_warning_clean(tmp_path: Path) -> None:
    source = tmp_path / "RunnerMain.java"
    source.write_text(_runner_source("tests.java.clean", {"only": "return 0;"}), encoding="utf-8")
    result = _compile(tmp_path / "classes", _JAVA_SDK, source)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_describe_is_byte_identical_to_the_bash_sdk(tmp_path: Path) -> None:
    assert _JAVA is not None
    workflow = "tests.java.describe"
    order = ["relax", "collect", "prepare"]
    classes = _write_runner(tmp_path, workflow, {name: "return 0;" for name in order})
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
    expected = (
        '{"format": "httk-workflow-runner-description", "format_version": 2, '
        '"steps": ["collect", "prepare", "relax"], "workflow": "tests.java.describe"}\n'
    )
    for invocation in (
        subprocess.run(
            [_JAVA, "-cp", str(classes), "RunnerMain", "--describe"], text=True, capture_output=True, check=False
        ),
        subprocess.run(
            [_JAVA, "-cp", str(classes), "RunnerMain"],
            text=True,
            capture_output=True,
            check=False,
            env={**os.environ, "HTTK_WORKFLOW_DESCRIBE": "1"},
        ),
    ):
        assert invocation.returncode == 0, invocation.stderr
        assert invocation.stdout == expected == bash.stdout


def test_invalid_workflow_ids_refuse_before_describing(tmp_path: Path) -> None:
    assert _JAVA is not None
    for workflow in ("bad id", "bad\n"):
        classes = _write_runner(tmp_path, workflow, {"only": "return 0;"}, name="bad" + str(len(workflow)))
        completed = subprocess.run(
            [_JAVA, "-cp", str(classes), "RunnerMain", "--describe"], text=True, capture_output=True, check=False
        )
        assert completed.returncode == 2
        assert completed.stdout == ""


def test_unknown_step_is_published_and_exits_zero(tmp_path: Path) -> None:
    classes = _write_runner(tmp_path, "tests.java", {"relax": "return 0;", "collect": "return 0;"})
    attempt = _attempt(tmp_path, step="realx")
    completed = attempt.run(classes)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"]["code"] == "unknown_step"
    assert "registered steps: collect, relax" in outcome["failure"]["message"]


def test_dispatch_log_and_present_empty_are_native_java_paths(tmp_path: Path) -> None:
    classes = _write_runner(
        tmp_path,
        "tests.java",
        {
            "log": 'attempt.log("info", "hello from Java"); attempt.succeed(); return 0;',
            "read": (
                'if (!attempt.parameter("empty").isPresent() || !attempt.parameter("empty").get().isEmpty()) '
                'throw new RuntimeException("empty value was lost"); '
                'if (attempt.parameter("missing").isPresent()) throw new RuntimeException("missing value was present"); '
                "attempt.succeed(); return 0;"
            ),
        },
    )
    logged = _attempt(tmp_path / "logged", step="log")
    completed = logged.run(classes)
    assert completed.returncode == 0, completed.stderr
    assert "[info] hello from Java" in completed.stderr
    read = _attempt(tmp_path / "read", step="read", parameters={"empty": ""})
    completed = read.run(classes)
    assert completed.returncode == 0, completed.stderr
    assert read.outcome()["action"] == "succeed"


def test_absent_read_refused_read_and_java_error_abort(tmp_path: Path) -> None:
    absent_classes = _write_runner(
        tmp_path / "absent",
        "tests.java",
        {
            "read": 'if (attempt.stateGet("missing").isPresent()) throw new RuntimeException(); attempt.succeed(); return 0;'
        },
    )
    absent = _attempt(tmp_path / "absent", step="read")
    completed = absent.run(absent_classes)
    assert completed.returncode == 0, completed.stderr
    assert absent.outcome()["action"] == "succeed"

    refused_classes = _write_runner(
        tmp_path / "refused", "tests.java", {"read": 'attempt.children("--bogus"); return 0;'}
    )
    refused = _attempt(tmp_path / "refused", step="read")
    completed = refused.run(refused_classes)
    assert completed.returncode == 2
    assert not (refused.control / "outcome.ready").exists()
    assert refused.breadcrumb()["exception"] == "JavaError"

    error_classes = _write_runner(tmp_path / "error", "tests.java", {"explode": 'throw new RuntimeException("boom");'})
    error = _attempt(tmp_path / "error", step="explode")
    completed = error.run(error_classes)
    assert completed.returncode == 2
    assert not (error.control / "outcome.ready").exists()
    breadcrumb = error.breadcrumb()
    assert breadcrumb["exception"] == "JavaError"
    assert breadcrumb["message"] == "boom"


def test_handler_endings_map_to_no_outcome_and_structured_failure(tmp_path: Path) -> None:
    classes = _write_runner(
        tmp_path,
        "tests.java",
        {
            "silent": 'attempt.log("info", "silent"); return 0;',
            "fail": 'attempt.fail("tests.broken", "it broke", false); return 0;',
        },
    )
    silent = _attempt(tmp_path / "silent", step="silent")
    completed = silent.run(classes)
    assert completed.returncode == 0, completed.stderr
    assert silent.outcome()["failure"] == {
        "code": "no_outcome",
        "message": "step 'silent' finished without publishing an outcome",
    }

    failed = _attempt(tmp_path / "failed", step="fail")
    completed = failed.run(classes)
    assert completed.returncode == 0, completed.stderr
    assert failed.outcome()["failure"] == {"code": "tests.broken", "message": "it broke"}


def test_stage_input_copies_a_payload_file_or_answers_false(tmp_path: Path) -> None:
    body = (
        'System.out.println(attempt.stageInput("poscar", "POSCAR", "files/POSCAR")); '
        'System.out.println(attempt.stageInput("incar", "INCAR", "files/INCAR")); '
        'System.out.println(attempt.stageInput("missing", "X")); '
        'try { attempt.stageInput("encut", "X"); } '
        "catch (HttkWorkflow.BridgeError error) { System.out.println(error.kind()); } "
        "attempt.succeed(); return 0;"
    )
    classes = _write_runner(tmp_path, "tests.java", {"start": body})
    attempt = _attempt(tmp_path, step="start", parameters={"encut": 520})
    (attempt.payload / "files" / "POSCAR").write_bytes(b"Si\n1.0\n")

    completed = attempt.run(classes)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == ["true", "false", "false", "Refused"]
    assert (attempt.workdir / "POSCAR").read_bytes() == b"Si\n1.0\n"
    assert attempt.outcome()["action"] == "succeed"


def test_parent_reads_the_parent_location_or_answers_empty(tmp_path: Path) -> None:
    body = (
        'System.out.println(attempt.parent("payload").orElseThrow()); '
        'System.out.println(attempt.parent("workdir").orElseThrow()); '
        'System.out.println(attempt.parent("job_id").orElseThrow()); '
        "System.out.println(attempt.parent().orElseThrow()); "
        "attempt.succeed(); return 0;"
    )
    classes = _write_runner(tmp_path, "tests.java", {"start": body})
    child = _attempt(tmp_path, step="start", parent=True)
    completed = child.run(classes)
    assert completed.returncode == 0, completed.stderr
    payload, job_id, spawn_id = _parent_of(child)
    lines = completed.stdout.splitlines()
    assert lines[:3] == [str(payload), str(payload / "run"), job_id]
    assert json.loads(lines[3])["spawn_id"] == spawn_id

    orphan = _write_runner(
        tmp_path, "tests.java", {"start": "System.out.println(attempt.parent().isPresent()); return 0;"}, "orphan"
    )
    completed = _attempt(tmp_path / "orphan", step="start").run(orphan)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "false\n"


def test_call_spawns_an_installed_workflow_as_a_child(tmp_path: Path) -> None:
    input_file = _call_input(tmp_path)
    body = (
        f"String key = attempt.call(\"sub\", \"sub\", "
        f"\"--file\", {json.dumps(f'input.txt={input_file}')}).orElseThrow(); "
        "System.out.println(key); "
        'attempt.gather("finish", new HttkWorkflow.Gather()); return 0;'
    )
    classes = _write_runner(tmp_path, "tests.java", {"start": body, "finish": "return 0;"})
    attempt = _attempt(tmp_path, step="start", calls=True)
    completed = attempt.run(classes)
    assert completed.returncode == 0, completed.stderr
    _assert_called(attempt, completed.stdout.strip())
