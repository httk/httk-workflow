"""The native Java authoring SDK: compile cleanliness, parity, dispatch, and relaxation."""

import json
import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

import httk.workflow
from httk.workflow import TaskManager, Workspace
from httk.workflow._runner_builds import register_build
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.scaffold import BuildSpec, new_job

_JAVAC = shutil.which("javac")
_JAVA = shutil.which("java")
pytestmark = pytest.mark.skipif(_JAVAC is None or _JAVA is None, reason="javac and java are required")

_JAVA_SDK = Path(httk.workflow.__file__).parent / "native" / "java" / "HttkWorkflow.java"
_SHELL = Path(httk.workflow.__file__).parent / "shell" / "httk-workflow.sh"
_RELAX_JAVA = Path(__file__).parents[1] / "examples" / "relax_java"
_MOCK_VASP = Path(__file__).parents[1] / "examples" / "mock_vasp.py"

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


@dataclass(frozen=True)
class _Attempt:
    payload: Path
    control: Path
    workdir: Path
    environment: dict[str, str]

    def run(self, classes: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        assert _JAVA is not None
        return subprocess.run(
            [_JAVA, "-cp", str(classes), "RunnerMain", *arguments],
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


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    data_generation: int | None = None,
) -> _Attempt:
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.java",
            runner_path="files/runner",
            initial_step=step,
            data_mode="none" if data_generation is None else "transactional",
            parameters=parameters or {},
        ),
    )
    control = payload / f"attempts/{uuid.uuid4()}"
    control.mkdir(parents=True)
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
                "payload": str(payload),
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


def _build_example(tmp_path: Path) -> Path:
    package = tmp_path / "relax_java"
    shutil.copytree(_RELAX_JAVA, package, ignore=shutil.ignore_patterns("classes"))
    classes = package / "classes"
    result = _compile(classes, package / "HttkWorkflow.java", package / "Relax.java")
    assert result.returncode == 0, result.stderr
    return package


def test_sdk_and_example_compile_warning_clean(tmp_path: Path) -> None:
    result = _compile(tmp_path / "classes", _JAVA_SDK, _RELAX_JAVA / "Relax.java")
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_vendored_java_sdk_is_byte_identical() -> None:
    assert (_RELAX_JAVA / "HttkWorkflow.java").read_bytes() == _JAVA_SDK.read_bytes()


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


def test_relax_runner_prepares_runs_and_publishes(tmp_path: Path) -> None:
    package = _build_example(tmp_path)
    poscar = tmp_path / "POSCAR"
    poscar.write_text(_POSCAR, encoding="utf-8")

    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("vasp.command", f"{sys.executable} {_MOCK_VASP}")
    job = new_job(
        workspace,
        package,
        files={"POSCAR": poscar},
        tag="silicon",
        data_mode="transactional",
        step="prepare",
    )
    source = workspace.runner_store_path(str(job.runner["path"]))
    register_build(
        workspace,
        source,
        PurePosixPath(str(job.runner["path"])),
        BuildSpec("make", ("classes",)),
        source_sha256=str(job.runner["sha256"]),
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    root = workspace.payload_path(marker.placement, marker.job_key)
    state = json.loads((root / ".httk-job" / "state.json").read_text(encoding="utf-8"))
    assert state["classification"] == "completed"
    published = root / "data" / "vasp"
    assert (published / "OUTCAR").is_file()
    assert (published / "CONTCAR").read_text(encoding="utf-8").splitlines()[-1].startswith("0.51")


def test_relax_package_needs_foreground_build_registration(tmp_path: Path) -> None:
    package = tmp_path / "relax_java"
    shutil.copytree(_RELAX_JAVA, package, ignore=shutil.ignore_patterns("classes"))
    assert not (package / "classes").exists()
    poscar = tmp_path / "POSCAR"
    poscar.write_text(_POSCAR, encoding="utf-8")

    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("vasp.command", f"{sys.executable} {_MOCK_VASP}")
    first = new_job(
        workspace,
        package,
        files={"POSCAR": poscar},
        tag="unbuilt",
        data_mode="transactional",
        step="prepare",
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)
    marker = workspace.find_marker_by_id(first.job_id)
    assert marker is not None and marker.kind == "failed"
    assert workspace.read_state(marker)["failure"]["code"] == "runner_not_built"

    source = workspace.runner_store_path(str(first.runner["path"]))
    register_build(
        workspace,
        source,
        PurePosixPath(str(first.runner["path"])),
        BuildSpec("make", ("classes",)),
        source_sha256=str(first.runner["sha256"]),
    )
    second = new_job(
        workspace,
        package,
        files={"POSCAR": poscar},
        tag="built",
        data_mode="transactional",
        step="prepare",
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)
    marker = workspace.find_marker_by_id(second.job_id)
    assert marker is not None and marker.kind == "succeeded"
