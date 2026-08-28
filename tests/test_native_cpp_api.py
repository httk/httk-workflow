"""The native C++ authoring SDK: C++17/C ABI parity and one relax."""

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
from httk.workflow.scaffold import BuildSpec, describe_runner, new_job

_CXX = shutil.which("g++") or shutil.which("c++")
_CC = shutil.which("cc")
_MAKE = shutil.which("make")
_READELF = shutil.which("readelf")
pytestmark = pytest.mark.skipif(
    _CXX is None or _CC is None or _MAKE is None,
    reason="g++/c++, a C compiler, and make are required",
)

_C_SDK = Path(httk.workflow.__file__).parent / "native" / "c"
_CPP_SDK = Path(httk.workflow.__file__).parent / "native" / "cpp"
_SHELL = Path(httk.workflow.__file__).parent / "shell" / "httk-workflow.sh"
_MOCK_VASP = Path(__file__).parents[1] / "examples" / "mock_vasp.py"
_RELAX_CPP = Path(__file__).parents[1] / "examples" / "relax_cpp" / "relax.cpp"

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


def _compile(tmp_path: Path, source: Path, *, name: str = "runner") -> subprocess.CompletedProcess[str]:
    """Compile a C++ runner against both SDK halves with warnings as errors."""

    assert _CXX is not None and _CC is not None
    c_object = tmp_path / "httk_workflow_c.o"
    c_result = subprocess.run(
        [
            _CC,
            "-std=c99",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-c",
            str(_C_SDK / "httk_workflow.c"),
            "-o",
            str(c_object),
            f"-I{_C_SDK}",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if c_result.returncode != 0:
        return c_result
    return subprocess.run(
        [
            _CXX,
            "-std=c++17",
            "-Wall",
            "-Wextra",
            "-Werror",
            f"-I{_CPP_SDK}",
            "-o",
            str(tmp_path / name),
            str(source),
            str(c_object),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_runner(tmp_path: Path, workflow: str, handlers: dict[str, str], name: str = "runner") -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    functions = "\n".join(f"int step_{step}() {{\n{body}\n}}" for step, body in handlers.items())
    registrations = "\n".join(f'  runner.add_step("{step}", guarded<&step_{step}>);' for step in handlers)
    source = tmp_path / "runner.cpp"
    source.write_text(
        f'''#include "httk_workflow.hpp"
#include <cstdlib>
#include <string>

using httk::workflow::Attempt;
using httk::workflow::BridgeError;
using httk::workflow::Runner;
using httk::workflow::guarded;

{functions}

int main(int argc, char** argv) {{
  Runner runner("{workflow}");
{registrations}
  return runner.main(argc, argv);
}}
''',
        encoding="utf-8",
    )
    result = _compile(tmp_path, source, name=name)
    assert result.returncode == 0, result.stderr
    return tmp_path / name


@dataclass(frozen=True)
class _Attempt:
    payload: Path
    control: Path
    workdir: Path
    environment: dict[str, str]

    def run(self, binary: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(binary), *arguments],
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
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.cpp",
            runner_path="files/runner",
            initial_step=step,
            data_mode="none" if data_generation is None else "transactional",
        ),
    )
    control = payload / f"attempts/{uuid.uuid4()}"
    control.mkdir(parents=True)
    workdir = payload / "run"
    workdir.mkdir()
    context_json = json.dumps(
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


def test_the_sdk_and_a_runner_compile_warning_clean(tmp_path: Path) -> None:
    result = _compile(tmp_path, _RELAX_CPP, name="relax")
    output = result.stdout + result.stderr
    assert result.returncode == 0, result.stderr
    assert "warning" not in output.lower()
    assert "execstack" not in (Path(__file__).parents[1] / "examples" / "relax_cpp" / "Makefile").read_text()
    if _READELF is None:
        pytest.skip("readelf is required for the PT_GNU_STACK NX assertion")
    headers = subprocess.run([_READELF, "-lW", str(tmp_path / "relax")], text=True, capture_output=True, check=False)
    assert headers.returncode == 0, headers.stderr
    stack_headers = [line for line in headers.stdout.splitlines() if "GNU_STACK" in line]
    assert len(stack_headers) == 1
    assert "E" not in stack_headers[0].split()[-2]


def test_vendored_cpp_sdk_is_byte_identical() -> None:
    assert (_RELAX_CPP.parent / "cpp" / "httk_workflow.hpp").read_bytes() == (
        _CPP_SDK / "httk_workflow.hpp"
    ).read_bytes()
    for name in ("httk_workflow.c", "httk_workflow.h"):
        assert (_RELAX_CPP.parent / "c" / name).read_bytes() == (_C_SDK / name).read_bytes()


def test_describe_is_byte_identical_to_the_bash_sdk(tmp_path: Path) -> None:
    workflow = "tests.cpp.describe"
    order = ["relax", "collect", "prepare"]
    binary = _write_runner(tmp_path, workflow, {name: "return 0;" for name in order})
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
    invocation = subprocess.run([str(binary), "--describe"], text=True, capture_output=True, check=False)
    assert invocation.returncode == 0, invocation.stderr
    assert invocation.stdout == bash.stdout
    assert invocation.stdout == (
        '{"format": "httk-workflow-runner-description", "format_version": 2, '
        '"steps": ["collect", "prepare", "relax"], "workflow": "tests.cpp.describe"}\n'
    )
    assert describe_runner(binary) == {"workflow": workflow, "steps": sorted(order)}


def test_invalid_workflow_id_is_refused(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.cpp invalid", {"start": "return 0;"})
    completed = subprocess.run([str(binary), "--describe"], text=True, capture_output=True, check=False)
    assert completed.returncode == 2


def test_a_cpp_handler_dispatches_and_publishes(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.cpp", {"start": "return Attempt::succeed();"})
    attempt = _attempt(tmp_path, step="start")
    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["action"] == "succeed"


def test_outcome_mapping_matches_the_c_sdk(tmp_path: Path) -> None:
    unknown = _write_runner(tmp_path / "unknown", "tests.cpp", {"known": "return 0;"})
    attempt = _attempt(tmp_path / "unknown", step="missing")
    assert attempt.run(unknown).returncode == 0
    assert attempt.outcome()["failure"]["code"] == "unknown_step"

    silent = _write_runner(tmp_path / "silent", "tests.cpp", {"silent": "return 0;"})
    attempt = _attempt(tmp_path / "silent", step="silent")
    assert attempt.run(silent).returncode == 0
    assert attempt.outcome()["failure"]["code"] == "no_outcome"

    failed = _write_runner(
        tmp_path / "failed",
        "tests.cpp",
        {"start": 'Attempt::fail("tests.broken", "it broke"); return 0;'},
    )
    attempt = _attempt(tmp_path / "failed", step="start")
    assert attempt.run(failed).returncode == 0
    assert attempt.outcome()["failure"] == {"code": "tests.broken", "message": "it broke"}


def test_a_nonzero_handler_leaves_the_inherited_cerror_breadcrumb(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.cpp", {"explode": "return 3;"})
    attempt = _attempt(tmp_path, step="explode")
    completed = attempt.run(binary)
    assert completed.returncode == 3
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["exception"] == "CError"
    assert breadcrumb["message"] == "explode exited with status 3"


def test_guarded_exception_leaves_the_inherited_cerror_breadcrumb(tmp_path: Path) -> None:
    body = """
  (void)Attempt::invoke_capture({"state-get"});
  return 0;
"""
    binary = _write_runner(tmp_path, "tests.cpp.guarded", {"explode": body})
    attempt = _attempt(tmp_path, step="explode")
    completed = attempt.run(binary)
    assert completed.returncode == 1
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["exception"] == "CError"
    assert breadcrumb["message"] == "explode exited with status 1"
    assert "C++ handler exception" in completed.stderr


def test_optional_reads_distinguish_absent_refused_empty_and_nonempty(tmp_path: Path) -> None:
    body = """
  if (Attempt::state_set("empty", "") != 0) return 10;
  const auto empty = Attempt::state_get("empty");
  if (!empty || !empty->empty()) return 11;
  if (Attempt::state_set("present", "value") != 0) return 12;
  const auto present = Attempt::state_get("present");
  if (!present || *present != "value") return 13;
  if (Attempt::state_get("missing")) return 14;
  std::string python = std::getenv("HTTK_WORKFLOW_PYTHON");
  unsetenv("HTTK_WORKFLOW_PYTHON");
  try {
    (void)Attempt::state_get("refused");
    return 15;
  } catch (const BridgeError& error) {
    if (error.status() != 2) return 16;
  }
  setenv("HTTK_WORKFLOW_PYTHON", python.c_str(), 1);
  return Attempt::succeed();
"""
    binary = _write_runner(tmp_path, "tests.cpp.reads", {"probe": body})
    attempt = _attempt(tmp_path, step="probe")
    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["action"] == "succeed"


def test_the_relax_runner_prepares_runs_and_publishes(tmp_path: Path) -> None:
    result = _compile(tmp_path, _RELAX_CPP, name="relax")
    assert result.returncode == 0, result.stderr
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("vasp.command", f"{sys.executable} {_MOCK_VASP}")
    reference = dict(workspace.publish_runner(tmp_path / "relax", name="relax"))
    payload = tmp_path / "payload"
    (payload / "files").mkdir(parents=True)
    (payload / "files" / "POSCAR").write_text(_POSCAR, encoding="utf-8")
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="C++ relaxation",
            workflow="httk.vasp.relax-cpp",
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
    assert json.loads((root / ".httk-job" / "state.json").read_text(encoding="utf-8"))["classification"] == "completed"
    published = root / "data" / "vasp"
    assert (published / "OUTCAR").is_file()
    assert (published / "CONTCAR").read_text(encoding="utf-8").splitlines()[-1].startswith("0.51")


def test_relax_package_needs_foreground_build_registration(tmp_path: Path) -> None:
    package = tmp_path / "relax_cpp"
    shutil.copytree(_RELAX_CPP.parent, package, ignore=shutil.ignore_patterns("relax", "*.o"))
    assert not (package / "relax").exists()
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
        BuildSpec("make", ("relax", "*.o"), "uname -sm"),
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
    root = workspace.payload_path(marker.placement, marker.job_key)
    assert (root / "data" / "vasp" / "OUTCAR").is_file()
