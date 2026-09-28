"""The C++ authoring SDK: C++17/C ABI parity, dispatch, and outcomes."""

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
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.scaffold import describe_runner

_CXX = shutil.which("g++") or shutil.which("c++")
_CC = shutil.which("cc")
_MAKE = shutil.which("make")
_READELF = shutil.which("readelf")
pytestmark = pytest.mark.skipif(
    _CXX is None or _CC is None or _MAKE is None,
    reason="g++/c++, a C compiler, and make are required",
)

_C_SDK = Path(httk.workflow.__file__).parent / "languages" / "c"
_CPP_SDK = Path(httk.workflow.__file__).parent / "languages" / "cpp"
_SHELL = Path(httk.workflow.__file__).parent / "languages" / "bash" / "httk-workflow.sh"


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


def _attempt(tmp_path: Path, *, step: str, data_generation: int | None = None, parent: bool = False) -> _Attempt:
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    workspace_id = str(uuid.uuid4())
    parent_block = _fabricate_parent(tmp_path / "workspace", workspace_id) if parent else None
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.cpp",
            runner_path="files/runner",
            initial_step=step,
            data_mode="none" if data_generation is None else "transactional",
        ),
        parent=parent_block,
    )
    control = payload / f"attempts/{uuid.uuid4()}"
    control.mkdir(parents=True)
    workdir = payload / "run"
    workdir.mkdir()
    context_json = json.dumps(
        {
            "format": "httk-workflow-attempt-context",
            "format_version": 2,
            "workspace_id": workspace_id,
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


def _fabricate_parent(workspace: Path, workspace_id: str) -> dict[str, object]:
    """Fabricate a parent payload with a persistent ``calc`` workdir; return the child's ``parent`` block."""

    staging = workspace / "project/parent/staging"
    (staging / "files").mkdir(parents=True)
    (staging / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    spec = JobSpec(
        name="Parent",
        workflow="tests.parent",
        runner_path="files/runner",
        initial_step="start",
        workdir_mode="persistent",
        workdir_path="calc",
    )
    job_id = prepare_job_payload(staging, spec).id
    job_key = staging.rename(staging.with_name(f"parent--{job_id}")).name
    return {
        "workspace_id": workspace_id,
        "job_id": job_id,
        "job_key": job_key,
        "placement": "project/parent",
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }


def test_the_sdk_and_a_runner_compile_warning_clean(tmp_path: Path) -> None:
    _write_runner(tmp_path, "tests.cpp.clean", {"only": "  return 0;"}, name="relax")
    result = _compile(tmp_path, tmp_path / "runner.cpp", name="relax")
    output = result.stdout + result.stderr
    assert result.returncode == 0, result.stderr
    assert "warning" not in output.lower()
    if _READELF is None:
        pytest.skip("readelf is required for the PT_GNU_STACK NX assertion")
    headers = subprocess.run([_READELF, "-lW", str(tmp_path / "relax")], text=True, capture_output=True, check=False)
    assert headers.returncode == 0, headers.stderr
    stack_headers = [line for line in headers.stdout.splitlines() if "GNU_STACK" in line]
    assert len(stack_headers) == 1
    assert "E" not in stack_headers[0].split()[-2]


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


def test_stage_input_is_true_staged_false_absent_and_throws_refused(tmp_path: Path) -> None:
    body = """
  if (!Attempt::stage_input("poscar", "POSCAR", "files/POSCAR")) return 10;
  if (Attempt::stage_input("incar", "INCAR", "files/INCAR")) return 11;
  if (Attempt::stage_input("nothing", "X")) return 12;
  std::string python = std::getenv("HTTK_WORKFLOW_PYTHON");
  unsetenv("HTTK_WORKFLOW_PYTHON");
  try {
    (void)Attempt::stage_input("poscar", "AGAIN", "files/POSCAR");
    return 13;
  } catch (const BridgeError& error) {
    if (error.status() != 2) return 14;
  }
  setenv("HTTK_WORKFLOW_PYTHON", python.c_str(), 1);
  return Attempt::succeed();
"""
    binary = _write_runner(tmp_path, "tests.cpp.stage", {"probe": body})
    attempt = _attempt(tmp_path, step="probe")
    (attempt.payload / "files" / "POSCAR").write_bytes(b"Si\n1.0\n")
    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["action"] == "succeed"
    assert (attempt.workdir / "POSCAR").read_bytes() == b"Si\n1.0\n"


def test_parent_reads_the_spawning_job_or_answers_absent(tmp_path: Path) -> None:
    """A child reads its parent's payload, workdir and id; a job without a parent reads absent (1)."""

    body = """
  auto show = [](const std::string& label, const std::optional<std::string>& value) {
    (void)Attempt::log("probe", label + (value ? " 0 " + *value : " 1"));
  };
  show("WHOLE", Attempt::parent());
  show("PAYLOAD", Attempt::parent("payload"));
  show("WORKDIR", Attempt::parent("workdir"));
  show("JOB", Attempt::parent("job_id"));
  return Attempt::succeed();
"""
    binary = _write_runner(tmp_path / "build", "tests.cpp.parent", {"probe": body})
    child = _attempt(tmp_path / "child", step="probe", parent=True)
    parent_payload = next((tmp_path / "child/workspace/project/parent").iterdir())
    completed = child.run(binary)
    assert completed.returncode == 0, completed.stderr
    job_id = parent_payload.name.removeprefix("parent--")
    for expected in (
        f"PAYLOAD 0 {parent_payload}",
        f"WORKDIR 0 {parent_payload / 'calc'}",
        f"JOB 0 {job_id}",
        '"spawn_id"',
    ):
        assert expected in completed.stderr, completed.stderr
    assert child.outcome()["action"] == "succeed"

    orphan = _attempt(tmp_path / "orphan", step="probe")
    completed = orphan.run(binary)
    assert completed.returncode == 0, completed.stderr
    for expected in ("WHOLE 1", "PAYLOAD 1", "WORKDIR 1", "JOB 1"):
        assert expected in completed.stderr, completed.stderr
