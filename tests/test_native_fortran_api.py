"""The native modern-Fortran authoring SDK: describe parity, dispatch, one relax.

The Fortran SDK is `iso_c_binding` bindings over the native C SDK plus an
idiomatic Fortran module: every verb is a `bind(c)` call into the same C library
a C runner links, and the C `httk_workflow_main` owns registration and dispatch,
calling back into Fortran step handlers through `c_funloc` function pointers. So
what is tested here is what only the Fortran half can get wrong -- compiling
warning-clean under the modern-Fortran standard, describing itself byte-for-byte
the way the C and Bash SDKs do, dispatching into a Fortran handler, and turning a
handler's ending into exactly one outcome -- plus one real VASP relaxation driven
end to end through a real manager.

Every test gates on both a Fortran compiler (``gfortran``) and a C compiler
(``cc``), and skips cleanly without either.
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
from httk.workflow.scaffold import describe_runner

_FC = shutil.which("gfortran")
_CC = shutil.which("cc")
pytestmark = pytest.mark.skipif(
    _FC is None or _CC is None, reason="both a Fortran (gfortran) and a C (cc) compiler are required"
)

_C_SDK = Path(httk.workflow.__file__).parent / "native" / "c"
_F_SDK = Path(httk.workflow.__file__).parent / "native" / "fortran"
_SHELL = Path(httk.workflow.__file__).parent / "shell" / "httk-workflow.sh"
_MOCK_VASP = Path(__file__).parents[1] / "examples" / "mock_vasp.py"
_RELAX_F = Path(__file__).parents[1] / "examples" / "relax_fortran" / "relax.f90"

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


def _compile(
    tmp_path: Path, *fortran_sources: Path, name: str = "runner", werror: bool = True
) -> subprocess.CompletedProcess[str]:
    """Compile one Fortran runner against both SDK halves, warning-clean by default.

    The C half is compiled with ``cc`` (its ``-std=c99`` is not valid for Fortran,
    and the strict Fortran flags are not valid for C), then the Fortran SDK module,
    the runner sources, and the C object are compiled and linked with ``gfortran``
    under the modern-Fortran standard. The SDK module is listed first so its
    ``.mod`` exists before the runner that ``use``\\ s it is compiled.
    """

    assert _FC is not None and _CC is not None
    c_object = tmp_path / "httk_workflow_c.o"
    c_result = subprocess.run(
        [_CC, "-std=c99", "-c", str(_C_SDK / "httk_workflow.c"), "-o", str(c_object), f"-I{_C_SDK}"],
        text=True,
        capture_output=True,
        check=False,
    )
    if c_result.returncode != 0:
        return c_result

    flags = ["-std=f2008", "-Wall", "-Wextra"]
    if werror:
        flags.append("-Werror")
    return subprocess.run(
        [
            _FC,
            *flags,
            "-o",
            str(tmp_path / name),
            str(_F_SDK / "httk_workflow.f90"),
            *(str(source) for source in fortran_sources),
            str(c_object),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )


def _runner_source(workflow: str, steps: dict[str, str]) -> str:
    """Assemble one Fortran runner registering the given ``name -> body`` handlers.

    Each body is Fortran that owns ``code``: it publishes (or not) and sets the
    handler's return value, exactly as a C body would ``... ; return N;``.
    """

    width = max(len(name) for name in steps)
    bodies = "\n".join(
        f"""  function step_{name}() result(code) bind(c)
    integer(c_int) :: code
    {body}
  end function"""
        for name, body in steps.items()
    )
    names = ", ".join(f'"{name}"' for name in steps)
    handlers = ", ".join(f"c_funloc(step_{name})" for name in steps)
    return f"""module gen_steps
  use, intrinsic :: iso_c_binding, only: c_int
  use httk_workflow
  implicit none
contains
{bodies}
end module gen_steps

program gen
  use, intrinsic :: iso_c_binding, only: c_funloc
  use httk_workflow
  use gen_steps
  implicit none
  if (httk_workflow_runner("{workflow}", &
        [character(len={width}) :: {names}], &
        [{handlers}]) /= HTTK_WORKFLOW_OK) call httk_workflow_exit(2)
  call httk_workflow_exit(httk_workflow_main())
end program gen
"""


def _write_runner(tmp_path: Path, workflow: str, steps: dict[str, str], name: str = "runner") -> Path:
    source = tmp_path / f"{name}.f90"
    source.write_text(_runner_source(workflow, steps), encoding="utf-8")
    result = _compile(tmp_path, source, name=name)
    assert result.returncode == 0, result.stderr
    return tmp_path / name


@dataclass(frozen=True)
class _Attempt:
    """One fabricated attempt a compiled Fortran runner can be dispatched into."""

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
    """Fabricate one attempt of one job, without a manager (mirrors test_native_c_api)."""

    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.fortran",
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
    for name_to_drop in ("HTTK_WORKFLOW_DESCRIBE", "HTTK_WORKFLOW_RUNNER_WORKFLOW", "HTTK_WORKFLOW_RUNNER_STEPS"):
        environment.pop(name_to_drop, None)
    return _Attempt(payload, control, workdir, environment)


def test_the_sdk_and_a_runner_compile_warning_clean(tmp_path: Path) -> None:
    """-std=f2008 -Wall -Wextra -Werror is the contract for the packaged module."""

    source = tmp_path / "runner.f90"
    source.write_text(_runner_source("tests.fortran.clean", {"only": "code = 0"}), encoding="utf-8")
    result = _compile(tmp_path, source)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


def test_describe_is_byte_identical_to_the_bash_sdk(tmp_path: Path) -> None:
    """The native handshake prints exactly what the Bash and C SDKs print."""

    workflow = "tests.fortran.describe"
    order = ["relax", "collect", "prepare"]
    binary = _write_runner(tmp_path, workflow, {name: "code = 0" for name in order})

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

    for invocation in (
        subprocess.run([str(binary), "--describe"], text=True, capture_output=True, check=False),
        subprocess.run(
            [str(binary)],
            text=True,
            capture_output=True,
            check=False,
            env={**os.environ, "HTTK_WORKFLOW_DESCRIBE": "1"},
        ),
    ):
        assert invocation.returncode == 0, invocation.stderr
        assert invocation.stdout == bash.stdout

    # And the scaffolder that resolves `job new --workflow ./relax` reads it back.
    described = describe_runner(binary)
    assert described == {"workflow": workflow, "steps": sorted(order)}


def test_an_unknown_step_is_reported_with_the_registered_steps(tmp_path: Path) -> None:
    binary = _write_runner(
        tmp_path,
        "tests.fortran",
        {"relax": "call ignore(httk_workflow_succeed()); code = 0", "collect": "code = 0"},
    )
    attempt = _attempt(tmp_path, step="realx")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"]["code"] == "unknown_step"
    assert "registered steps: collect, relax" in outcome["failure"]["message"]


def test_a_step_that_publishes_nothing_is_reported_as_no_outcome(tmp_path: Path) -> None:
    binary = _write_runner(
        tmp_path, "tests.fortran", {"silent": 'call ignore(httk_workflow_log("info", "nothing")); code = 0'}
    )
    attempt = _attempt(tmp_path, step="silent")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["failure"] == {
        "code": "no_outcome",
        "message": "step 'silent' finished without publishing an outcome",
    }


def test_a_step_can_publish_a_structured_failure(tmp_path: Path) -> None:
    binary = _write_runner(
        tmp_path,
        "tests.fortran",
        {"start": 'call ignore(httk_workflow_fail("tests.broken", "it broke")); code = 0'},
    )
    attempt = _attempt(tmp_path, step="start")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"] == {"code": "tests.broken", "message": "it broke"}


def test_a_handler_that_returns_nonzero_leaves_a_breadcrumb_and_no_outcome(tmp_path: Path) -> None:
    """A `bind(c)` handler that returns nonzero is the aborted-attempt path."""

    binary = _write_runner(tmp_path, "tests.fortran", {"explode": "code = 3"})
    attempt = _attempt(tmp_path, step="explode")

    completed = attempt.run(binary)
    assert completed.returncode == 3
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["format"] == "httk-workflow-runner-error"
    assert breadcrumb["step"] == "explode"
    assert breadcrumb["exception"] == "CError"
    assert breadcrumb["message"] == "explode exited with status 3"


_PROBE_RUNNER = """module probe_steps
  use, intrinsic :: iso_c_binding, only: c_int
  use httk_workflow
  implicit none
contains
  subroutine reportv(tag, is_alloc, ln, st)
    character(len=*), intent(in) :: tag
    logical, intent(in) :: is_alloc
    integer, intent(in) :: ln, st
    character(len=64) :: line
    write (line, '(A,1X,L1,1X,I0,1X,I0)') tag, is_alloc, ln, st
    call ignore(httk_workflow_log("probe", trim(line)))
  end subroutine
  function step_probe() result(code) bind(c)
    integer(c_int) :: code
    character(len=:), allocatable :: empty_value, absent_value
    integer :: empty_status, absent_status
    call ignore(httk_workflow_state_set("empty", ""))
    call httk_workflow_state_get("empty", empty_value, empty_status)
    call httk_workflow_state_get("absent_key", absent_value, absent_status)
    if (allocated(empty_value)) then
      call reportv("EMPTY", .true., len(empty_value), empty_status)
    else
      call reportv("EMPTY", .false., 0, empty_status)
    end if
    if (allocated(absent_value)) then
      call reportv("ABSENT", .true., 0, absent_status)
    else
      call reportv("ABSENT", .false., 0, absent_status)
    end if
    call ignore(httk_workflow_succeed())
    code = 0
  end function
end module probe_steps

program probe
  use, intrinsic :: iso_c_binding, only: c_funloc
  use httk_workflow
  use probe_steps
  implicit none
  if (httk_workflow_runner("tests.fortran.probe", [character(len=5) :: "probe"], &
        [c_funloc(step_probe)]) /= HTTK_WORKFLOW_OK) call httk_workflow_exit(2)
  call httk_workflow_exit(httk_workflow_main())
end program probe
"""

_REREGISTER_RUNNER = """module rr_steps
  use, intrinsic :: iso_c_binding, only: c_int
  use httk_workflow
  implicit none
contains
  function s() result(code) bind(c)
    integer(c_int) :: code
    code = 0
  end function
end module rr_steps

program rr
  use, intrinsic :: iso_c_binding, only: c_funloc
  use httk_workflow
  use rr_steps
  implicit none
  integer :: rc
  ! Register once, then a second time with a different set: the second call must
  ! replace the first cleanly (Bash and C do), not abort on "already allocated".
  rc = httk_workflow_runner("w.first", [character(len=5) :: "alpha"], [c_funloc(s)])
  rc = httk_workflow_runner("w.second", [character(len=5) :: "beta", "gamma"], &
        [c_funloc(s), c_funloc(s)])
  if (rc /= HTTK_WORKFLOW_OK) call httk_workflow_exit(2)
  call httk_workflow_exit(httk_workflow_main())
end program rr
"""


def _compile_source(tmp_path: Path, source: str, name: str = "runner") -> Path:
    path = tmp_path / f"{name}.f90"
    path.write_text(source, encoding="utf-8")
    result = _compile(tmp_path, path, name=name)
    assert result.returncode == 0, result.stderr
    return tmp_path / name


def test_an_absent_read_is_unallocated_and_an_empty_read_is_allocated(tmp_path: Path) -> None:
    """The read contract: absent -> unallocated + status 1; empty -> allocated, len 0."""

    binary = _compile_source(tmp_path, _PROBE_RUNNER, name="probe")
    attempt = _attempt(tmp_path, step="probe")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    # A key set to "" reads back allocated with length 0 and status OK ...
    assert "EMPTY T 0 0" in completed.stderr, completed.stderr
    # ... while an unset key reads back unallocated with the absent status (1).
    assert "ABSENT F 0 1" in completed.stderr, completed.stderr


def test_a_runner_can_be_registered_twice(tmp_path: Path) -> None:
    """Re-registration replaces the set cleanly instead of aborting."""

    binary = _compile_source(tmp_path, _REREGISTER_RUNNER, name="rereg")
    completed = subprocess.run([str(binary), "--describe"], text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "format": "httk-workflow-runner-description",
        "format_version": 1,
        "steps": ["beta", "gamma"],
        "workflow": "w.second",
    }


def test_the_relax_runner_prepares_runs_and_publishes(tmp_path: Path) -> None:
    """The examples/relax_fortran runner, driven end to end through a real manager."""

    result = _compile(tmp_path, _RELAX_F, name="relax")
    assert result.returncode == 0, result.stderr

    workspace = Workspace.initialize(tmp_path / "workspace")
    # The documented flow: name the mock VASP as the workspace vasp.command.
    workspace.set_setting("vasp.command", f"{sys.executable} {_MOCK_VASP}")

    reference = dict(workspace.publish_runner(tmp_path / "relax", name="relax"))
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "POSCAR").write_text(_POSCAR, encoding="utf-8")

    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Fortran relaxation",
            workflow="httk.vasp.relax-fortran",
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


def test_a_missing_vasp_command_fails_by_name(tmp_path: Path) -> None:
    result = _compile(tmp_path, _RELAX_F, name="relax")
    assert result.returncode == 0, result.stderr

    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = dict(workspace.publish_runner(tmp_path / "relax", name="relax"))
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "POSCAR").write_text(_POSCAR, encoding="utf-8")

    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Fortran relaxation",
            workflow="httk.vasp.relax-fortran",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            initial_step="prepare",
            data_mode="transactional",
        ),
    )
    workspace.submit(payload, "project/vasp")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker).get("failure")
    assert isinstance(failure, dict)
    assert failure["code"] == "vasp.command_missing"
