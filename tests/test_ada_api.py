"""The Ada authoring SDK: describe parity, dispatch, reads, and outcomes.

The Ada SDK is ``Interfaces.C`` bindings over the C SDK. Every bridge
verb reaches the same ``httk.workflow._shell_bridge`` implementation, while
the C ``httk_workflow_main`` owns registration, dispatch, and exit status. This
tests the Ada-specific boundary: a warning-clean GNAT build, byte-identical
description, C-convention handler dispatch, absent-versus-refused reads,
and the ``CError`` breadcrumb.
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

_GNATMAKE = shutil.which("gnatmake")
_CC = shutil.which("cc")
_READELF = shutil.which("readelf")
pytestmark = pytest.mark.skipif(
    _GNATMAKE is None or _CC is None or _READELF is None,
    reason="gnatmake, a C compiler, and readelf are required",
)

_C_SDK = Path(httk.workflow.__file__).parent / "languages" / "c"
_ADA_SDK = Path(httk.workflow.__file__).parent / "languages" / "ada"
_SHELL = Path(httk.workflow.__file__).parent / "languages" / "bash" / "httk-workflow.sh"


def _compile(tmp_path: Path, source: Path, *, name: str = "runner") -> subprocess.CompletedProcess[str]:
    """Compile one Ada runner against both SDK halves with GNAT warnings as errors."""

    assert _GNATMAKE is not None and _CC is not None
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
            _GNATMAKE,
            "-gnat2012",
            "-gnatwa",
            "-gnatwe",
            f"-I{_ADA_SDK}",
            f"-D{tmp_path}",
            "-o",
            str(tmp_path / name),
            str(source),
            "-largs",
            str(c_object),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )


def _runner_sources(workflow: str, steps: dict[str, str], handler_first: int = 1) -> tuple[str, str, str]:
    declarations = "\n".join(f"   function Step_{name} return Interfaces.C.int with Convention => C;" for name in steps)
    bodies = "\n".join(
        f"""   function Step_{name} return Interfaces.C.int is
    begin
      {body}
    end Step_{name};"""
        for name, body in steps.items()
    )
    names = ", ".join(f'{index} => U.To_Unbounded_String ("{name}")' for index, name in enumerate(steps, 1))
    handlers = ", ".join(
        f"{handler_first + index - 1} => Generated_Steps.Step_{name}'Access" for index, name in enumerate(steps, 1)
    )
    handler_last = handler_first + len(steps) - 1
    needs_env = any("Ada.Environment_Variables" in body for body in steps.values())
    needs_unbounded = any("U." in body for body in steps.values())
    needs_hhtk = any("Httk_Workflow" in body for body in steps.values())
    needs_c = any("C." in body or "/=" in body for body in steps.values())
    extra_with = ""
    if needs_env:
        extra_with += "with Ada.Environment_Variables;\n"
    if needs_unbounded:
        extra_with += "with Ada.Strings.Unbounded;\n"
    if needs_hhtk:
        extra_with += "with Httk_Workflow;\n"
    spec = f"""with Interfaces.C;
package Generated_Steps is
{declarations}
end Generated_Steps;
"""
    c_declarations = "  package C renames Interfaces.C;\n  use type C.int;\n" if needs_c else ""
    u_declaration = "  package U renames Ada.Strings.Unbounded;\n" if needs_unbounded else ""
    body = f"""{extra_with}
package body Generated_Steps is
{c_declarations}
{u_declaration}
{bodies}
end Generated_Steps;
"""
    main = f"""with Ada.Strings.Unbounded;
with Interfaces.C;
with Httk_Workflow;
with Generated_Steps;
procedure Generated is
  package U renames Ada.Strings.Unbounded;
  package C renames Interfaces.C;
  use type C.int;
  Names : constant Httk_Workflow.Step_Names := ({names});
  Handlers : constant Httk_Workflow.Step_Handlers ({handler_first} .. {handler_last}) := ({handlers});
begin
  if Httk_Workflow.Httk_Workflow_Runner ("{workflow}", Names, Handlers) /= Httk_Workflow.HTTK_WORKFLOW_OK then
    Httk_Workflow.Httk_Workflow_Exit (2);
  end if;
  Httk_Workflow.Httk_Workflow_Exit (Httk_Workflow.Httk_Workflow_Main);
end Generated;
"""
    return spec, body, main


def _write_runner(
    tmp_path: Path,
    workflow: str,
    steps: dict[str, str],
    name: str = "runner",
    handler_first: int = 1,
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "generated.adb"
    spec, body, main = _runner_sources(workflow, steps, handler_first)
    (tmp_path / "generated_steps.ads").write_text(spec, encoding="utf-8")
    (tmp_path / "generated_steps.adb").write_text(body, encoding="utf-8")
    source.write_text(main, encoding="utf-8")
    result = _compile(tmp_path, source, name=name)
    assert result.returncode == 0, result.stderr
    return tmp_path / name


_Attempt = FabricatedAttempt


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    parent: bool = False,
    calls: bool = False,
) -> FabricatedAttempt:
    """Fabricate one attempt of one job of ``tests.ada``, without a manager."""

    return fabricate(
        tmp_path,
        step=step,
        workflow="tests.ada",
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


def _stage_call_target(attempt: FabricatedAttempt) -> None:
    """Write the file the call stages, ``input.txt`` in the attempt's workdir."""

    (attempt.workdir / "input.txt").write_text("staged-by-call\n", encoding="utf-8")


def test_the_sdk_and_a_runner_compile_warning_clean(tmp_path: Path) -> None:
    """-gnat2012 -gnatwa -gnatwe is the warning-clean contract for the Ada package."""

    spec, body, main = _runner_sources("tests.ada.clean", {"only": "return 0;"})
    (tmp_path / "generated_steps.ads").write_text(spec, encoding="utf-8")
    (tmp_path / "generated_steps.adb").write_text(body, encoding="utf-8")
    (tmp_path / "generated.adb").write_text(main, encoding="utf-8")
    result = _compile(tmp_path, tmp_path / "generated.adb", name="relax")
    output = result.stdout + result.stderr
    assert result.returncode == 0, result.stderr
    assert "warning" not in output.lower()
    assert _READELF is not None
    headers = subprocess.run([_READELF, "-lW", str(tmp_path / "relax")], text=True, capture_output=True, check=False)
    assert headers.returncode == 0, headers.stderr
    stack_headers = [line for line in headers.stdout.splitlines() if "GNU_STACK" in line]
    assert len(stack_headers) == 1
    assert "E" not in stack_headers[0].split()[-2]


def test_describe_is_byte_identical_to_the_bash_sdk(tmp_path: Path) -> None:
    """The native handshake prints exactly what the Bash SDK prints."""

    workflow = "tests.ada.describe"
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
        '"steps": ["collect", "prepare", "relax"], "workflow": "tests.ada.describe"}\n'
    )
    assert describe_runner(binary) == {"workflow": workflow, "steps": sorted(order)}


def test_an_ada_handler_dispatches_and_publishes(tmp_path: Path) -> None:
    """A C-convention Ada function is dispatched by the C main."""

    binary = _write_runner(
        tmp_path,
        "tests.ada",
        {"start": "if Httk_Workflow.Httk_Workflow_Succeed /= 0 then null; end if; return 0;"},
        handler_first=7,
    )
    attempt = _attempt(tmp_path, step="start")
    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["action"] == "succeed"


def test_outcome_mapping_matches_the_c_sdk(tmp_path: Path) -> None:
    """Unknown, silent, and structured-failure endings retain C semantics."""

    unknown = _write_runner(tmp_path / "unknown", "tests.ada", {"known": "return 0;"})
    attempt = _attempt(tmp_path / "unknown", step="missing")
    assert attempt.run(unknown).returncode == 0
    assert attempt.outcome()["failure"]["code"] == "unknown_step"

    silent = _write_runner(tmp_path / "silent", "tests.ada", {"silent": "return 0;"})
    attempt = _attempt(tmp_path / "silent", step="silent")
    assert attempt.run(silent).returncode == 0
    assert attempt.outcome()["failure"]["code"] == "no_outcome"

    failed = _write_runner(
        tmp_path / "failed",
        "tests.ada",
        {"start": 'if Httk_Workflow.Httk_Workflow_Fail ("tests.broken", "it broke") /= 0 then null; end if; return 0;'},
    )
    attempt = _attempt(tmp_path / "failed", step="start")
    assert attempt.run(failed).returncode == 0
    assert attempt.outcome()["failure"] == {"code": "tests.broken", "message": "it broke"}


def test_a_nonzero_handler_leaves_the_inherited_cerror_breadcrumb(tmp_path: Path) -> None:
    """A nonzero C-convention Ada handler is reported by the C dispatcher."""

    binary = _write_runner(tmp_path, "tests.ada", {"explode": "return 3;"})
    attempt = _attempt(tmp_path, step="explode")
    completed = attempt.run(binary)
    assert completed.returncode == 3
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["exception"] == "CError"
    assert breadcrumb["message"] == "explode exited with status 3"


def test_absent_and_refused_reads_stay_distinct(tmp_path: Path) -> None:
    """NULL reads report status 1, while an unset bridge reports refused status 2."""

    source = tmp_path / "generated.adb"
    spec, body, main = _runner_sources(
        "tests.ada.reads",
        {
            "probe": """declare
        Value : U.Unbounded_String;
        Present : Boolean;
        Status : C.int;
        Python : constant String := Ada.Environment_Variables.Value ("HTTK_WORKFLOW_PYTHON");
      begin
        if Httk_Workflow.Httk_Workflow_State_Set ("empty", "") /= 0 then return 10; end if;
        Httk_Workflow.Httk_Workflow_State_Get ("empty", Value, Present, Status);
        if Status /= 0 or else not Present or else U.Length (Value) /= 0 then return 11; end if;
        if Httk_Workflow.Httk_Workflow_State_Set ("present", "value") /= 0 then return 12; end if;
        Httk_Workflow.Httk_Workflow_State_Get ("present", Value, Present, Status);
        if Status /= 0 or else not Present or else U.To_String (Value) /= "value" then return 13; end if;
        Httk_Workflow.Httk_Workflow_State_Get ("missing", Value, Present, Status);
        if Status /= 1 or else Present then return 14; end if;
        if Httk_Workflow.Httk_Workflow_Log ("probe", "ABSENT " & C.int'Image (Status)) /= 0 then null; end if;
        Ada.Environment_Variables.Clear ("HTTK_WORKFLOW_PYTHON");
        Httk_Workflow.Httk_Workflow_State_Get ("refused", Value, Present, Status);
        if Status /= 2 or else Present then return 15; end if;
        if Httk_Workflow.Httk_Workflow_Log ("probe", "REFUSED " & C.int'Image (Status)) /= 0 then null; end if;
        Ada.Environment_Variables.Set ("HTTK_WORKFLOW_PYTHON", Python);
        if Httk_Workflow.Httk_Workflow_Succeed /= 0 then null; end if;
        return 0;
      end;"""
        },
    )
    (tmp_path / "generated_steps.ads").write_text(spec, encoding="utf-8")
    (tmp_path / "generated_steps.adb").write_text(body, encoding="utf-8")
    source.write_text(main, encoding="utf-8")
    binary = _compile(tmp_path, source)
    assert binary.returncode == 0, binary.stderr
    attempt = _attempt(tmp_path, step="probe")
    completed = attempt.run(tmp_path / "runner")
    assert completed.returncode == 0, completed.stderr
    assert "ABSENT  1" in completed.stderr
    assert "REFUSED  2" in completed.stderr


def test_stage_input_copies_a_payload_file_or_answers_absent(tmp_path: Path) -> None:
    """Staged is 0; no such payload file or parameter is 1; a non-string parameter is refused."""

    body = """if Httk_Workflow.Httk_Workflow_Stage_Input ("poscar", "POSCAR", "files/POSCAR") /= 0 then return 10; end if;
      if Httk_Workflow.Httk_Workflow_Stage_Input ("incar", "INCAR", "files/INCAR") /= 1 then return 11; end if;
      if Httk_Workflow.Httk_Workflow_Stage_Input ("missing", "X") /= 1 then return 12; end if;
      if Httk_Workflow.Httk_Workflow_Stage_Input ("encut", "X") /= Httk_Workflow.HTTK_WORKFLOW_REFUSED then
        return 13;
      end if;
      if Httk_Workflow.Httk_Workflow_Succeed /= 0 then null; end if;
      return 0;"""
    binary = _write_runner(tmp_path, "tests.ada", {"start": body})
    attempt = _attempt(tmp_path, step="start", parameters={"encut": 520})
    (attempt.payload / "files" / "POSCAR").write_bytes(b"Si\n1.0\n")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert (attempt.workdir / "POSCAR").read_bytes() == b"Si\n1.0\n"
    assert attempt.outcome()["action"] == "succeed"


def test_parent_reads_the_spawning_job_or_answers_absent(tmp_path: Path) -> None:
    """A child reads its parent's payload, workdir and id; a job without a parent reads absent (1)."""

    body = """declare
        function Show (Label : String; Field : String := "") return C.int is
          Value : U.Unbounded_String;
          Present : Boolean;
          Status : C.int;
        begin
          if Field = "" then
            Httk_Workflow.Httk_Workflow_Parent (Value, Present, Status);
          else
            Httk_Workflow.Httk_Workflow_Parent (Value, Present, Status, Field);
          end if;
          return Httk_Workflow.Httk_Workflow_Log ("probe", Label & C.int'Image (Status) & " " & U.To_String (Value));
        end Show;
      begin
        if Show ("WHOLE") /= 0 then return 10; end if;
        if Show ("PAYLOAD", "payload") /= 0 then return 11; end if;
        if Show ("WORKDIR", "workdir") /= 0 then return 12; end if;
        if Show ("JOB", "job_id") /= 0 then return 13; end if;
        if Httk_Workflow.Httk_Workflow_Succeed /= 0 then null; end if;
        return 0;
      end;"""
    binary = _write_runner(tmp_path / "build", "tests.ada.parent", {"probe": body})
    child = _attempt(tmp_path / "child", step="probe", parent=True)
    parent_payload, job_id, _spawn_id = _parent_of(child)
    completed = child.run(binary)
    assert completed.returncode == 0, completed.stderr
    for expected in (
        f"PAYLOAD 0 {parent_payload}",
        f"WORKDIR 0 {parent_payload / 'run'}",
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


def test_call_spawns_an_installed_workflow_as_a_child(tmp_path: Path) -> None:
    """Httk_Workflow_Call registers a job of another installed workflow as a labelled child."""

    body = """declare
        Key : U.Unbounded_String;
        Present : Boolean;
        Status : C.int;
      begin
        Httk_Workflow.Httk_Workflow_Call
          ("sub", "sub", Key, Present, Status,
           (U.To_Unbounded_String ("--file"), U.To_Unbounded_String ("input.txt=input.txt")));
        if Httk_Workflow.Httk_Workflow_Log ("probe", "KEY" & C.int'Image (Status) & " " & U.To_String (Key)) /= 0
        then
          return 10;
        end if;
        return Httk_Workflow.Httk_Workflow_Gather ("finish");
      end;"""
    binary = _write_runner(
        tmp_path / "build",
        "tests.ada.call",
        {"start": body, "finish": "return Httk_Workflow.Httk_Workflow_Succeed;"},
    )
    attempt = _attempt(tmp_path / "attempt", step="start", calls=True)
    _stage_call_target(attempt)
    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    _assert_called(attempt)
