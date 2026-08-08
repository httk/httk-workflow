"""The native C authoring SDK: describe parity, dispatch, and one relaxation.

The C SDK is a bridge client, exactly like the Bash one: every verb execs
``$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge``, and only ``--describe``
is native. What is tested here is what only the C half can get wrong — compiling
warning-clean, describing itself byte-for-byte the way the Bash SDK does,
dispatching into a step handler, and turning a handler's ending into exactly one
outcome — plus one real VASP relaxation driven end to end through a real manager.

Every test gates on a C compiler and skips cleanly without one.
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
from httk.core.cli import CLIContext

import httk.workflow
from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.scaffold import describe_runner
from httk.workflow.workflow_cli import command as workflow_command

_CC = shutil.which("cc")
pytestmark = pytest.mark.skipif(_CC is None, reason="no C compiler (cc) is available")

_SDK = Path(httk.workflow.__file__).parent / "native" / "c"
_SHELL = Path(httk.workflow.__file__).parent / "shell" / "httk-workflow.sh"
_MOCK_VASP = Path(__file__).parents[1] / "examples" / "mock_vasp.py"
_RELAX_C = Path(__file__).parents[1] / "examples" / "relax_c" / "relax.c"

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
    tmp_path: Path, *sources: Path, name: str = "runner", werror: bool = True
) -> subprocess.CompletedProcess[str]:
    """Compile one C runner against the SDK, warning-clean by default."""

    assert _CC is not None
    flags = ["-std=c99", "-Wall", "-Wextra"]
    if werror:
        flags.append("-Werror")
    return subprocess.run(
        [
            _CC,
            *flags,
            f"-I{_SDK}",
            "-o",
            str(tmp_path / name),
            *(str(source) for source in sources),
            str(_SDK / "httk_workflow.c"),
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def _runner_source(workflow: str, steps: dict[str, str]) -> str:
    """Assemble one C runner registering the given ``name -> body`` step handlers."""

    bodies = "\n".join(f"static int step_{name}(void) {{ {body} }}" for name, body in steps.items())
    table = ", ".join(f'{{"{name}", step_{name}}}' for name in steps)
    return f"""#include "httk_workflow.h"
{bodies}
int main(int argc, char **argv) {{
    static const httk_workflow_step steps[] = {{ {table} }};
    if (httk_workflow_runner("{workflow}", steps, {len(steps)}) != 0) return 2;
    return httk_workflow_main(argc, argv);
}}
"""


def _write_runner(tmp_path: Path, workflow: str, steps: dict[str, str], name: str = "runner") -> Path:
    source = tmp_path / f"{name}.c"
    source.write_text(_runner_source(workflow, steps), encoding="utf-8")
    result = _compile(tmp_path, source, name=name)
    assert result.returncode == 0, result.stderr
    return tmp_path / name


@dataclass(frozen=True)
class _Attempt:
    """One fabricated attempt a compiled C runner can be dispatched into."""

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
    """Fabricate one attempt of one job, without a manager (mirrors test_bash_sdk)."""

    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.c",
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
    """-Wall -Wextra -Werror is the contract for the packaged pair."""

    source = tmp_path / "runner.c"
    source.write_text(_runner_source("tests.c.clean", {"only": "return 0;"}), encoding="utf-8")
    result = _compile(tmp_path, source)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


def test_describe_is_byte_identical_to_the_bash_sdk(tmp_path: Path) -> None:
    """The native handshake prints exactly what the Bash and Python SDKs print."""

    workflow = "tests.c.describe"
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


def test_a_workflow_name_outside_the_charset_is_refused(tmp_path: Path) -> None:
    """A stray character in the id would print invalid describe JSON."""

    binary = _write_runner(tmp_path, "bad name", {"go": "return 0;"})
    completed = subprocess.run([str(binary)], text=True, capture_output=True, check=False)
    assert completed.returncode == 2
    assert "cannot name a runner" in completed.stderr


def test_an_unknown_step_is_reported_with_the_registered_steps(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.c", {"relax": "httk_workflow_succeed(); return 0;", "collect": "return 0;"})
    attempt = _attempt(tmp_path, step="realx")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"]["code"] == "unknown_step"
    assert "registered steps: collect, relax" in outcome["failure"]["message"]


def test_a_step_that_publishes_nothing_is_reported_as_no_outcome(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.c", {"silent": 'httk_workflow_log("info", "nothing"); return 0;'})
    attempt = _attempt(tmp_path, step="silent")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["failure"] == {
        "code": "no_outcome",
        "message": "step 'silent' finished without publishing an outcome",
    }


def test_a_step_can_publish_a_structured_failure(tmp_path: Path) -> None:
    binary = _write_runner(
        tmp_path, "tests.c", {"start": 'httk_workflow_fail("tests.broken", "it broke", 0); return 0;'}
    )
    attempt = _attempt(tmp_path, step="start")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"] == {"code": "tests.broken", "message": "it broke"}


def test_a_handler_that_returns_nonzero_leaves_a_breadcrumb_and_no_outcome(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.c", {"explode": "return 3;"})
    attempt = _attempt(tmp_path, step="explode")

    completed = attempt.run(binary)
    assert completed.returncode == 3
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["format"] == "httk-workflow-runner-error"
    assert breadcrumb["step"] == "explode"
    assert breadcrumb["exception"] == "CError"
    assert breadcrumb["message"] == "explode exited with status 3"


def test_the_relax_runner_prepares_runs_and_publishes(tmp_path: Path) -> None:
    """The examples/relax_c runner, driven end to end through a real manager."""

    result = _compile(tmp_path, _RELAX_C, name="relax")
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
            name="C relaxation",
            workflow="httk.vasp.relax-c",
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
    result = _compile(tmp_path, _RELAX_C, name="relax")
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
            name="C relaxation",
            workflow="httk.vasp.relax-c",
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


def _cli_relax_context(tmp_path: Path) -> tuple[Workspace, CLIContext, str, Path]:
    """Compile relax, register a workspace with a mock VASP, and stage a POSCAR."""

    result = _compile(tmp_path, _RELAX_C, name="relax")
    assert result.returncode == 0, result.stderr
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("vasp.command", f"{sys.executable} {_MOCK_VASP}")
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)
    (tmp_path / "POSCAR").write_text(_POSCAR, encoding="utf-8")
    return workspace, context, ws, tmp_path / "POSCAR"


def _job_new(ws: str, workflow: str, poscar: Path, context: CLIContext) -> int:
    argv = [
        "job",
        "new",
        ws,
        "--workflow",
        workflow,
        "--step",
        "prepare",
        "--file",
        f"POSCAR={poscar}",
        "--data-mode",
        "transactional",
        "--tag",
        "silicon",
    ]
    return workflow_command(argv, context)


def _assert_relax_succeeded(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)
    markers = list(workspace.walk_markers(("succeeded",)))
    assert len(markers) == 1
    root = workspace.payload_path(markers[0].placement, markers[0].job_key)
    assert (root / "data" / "vasp" / "OUTCAR").is_file()
    assert (root / "data" / "vasp" / "CONTCAR").is_file()


def test_the_cli_scaffolds_and_runs_the_relax_binary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """An absolute `--workflow` path scaffolds, describes by running, and submits."""

    workspace, context, ws, poscar = _cli_relax_context(tmp_path)
    assert _job_new(ws, str(tmp_path / "relax"), poscar, context) == 0, capsys.readouterr().err
    _assert_relax_succeeded(workspace)


def test_the_cli_scaffolds_from_a_relative_runner_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The documented `--workflow ./relax` flow: a relative path must not PATH-exec."""

    workspace, context, ws, poscar = _cli_relax_context(tmp_path)
    # From the runner's own directory, `./relax` normalizes to bare `relax`; only
    # the resolve() in describe_runner keeps exec from doing a PATH lookup.
    monkeypatch.chdir(tmp_path)
    assert _job_new(ws, "./relax", poscar, context) == 0, capsys.readouterr().err
    _assert_relax_succeeded(workspace)


def test_a_bare_runner_name_resolves_to_the_cwd_file_not_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """`--workflow relax` (no ./, file present in cwd) runs the cwd file, never a PATH exec.

    The compiled `relax` exists only in the cwd, never on PATH, so a successful
    describe-and-run proves the resolve() in describe_runner turned the bare name
    into the cwd file explicitly. Pre-fix, `[str(Path("relax"))]` == `["relax"]`
    would have done a PATH lookup — a FileNotFoundError or a same-named-program
    hijack.
    """

    workspace, context, ws, poscar = _cli_relax_context(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert _job_new(ws, "relax", poscar, context) == 0, capsys.readouterr().err
    _assert_relax_succeeded(workspace)


# A runner that ignores SIGCHLD — as a daemonized launcher or MPI harness leaves
# it — auto-reaps the bridge child, so waitpid returns ECHILD. The verb must not
# spin at 100% CPU; the step below sets it after `begin`, so the outcome is still
# published and the process still terminates promptly.
_SIGCHLD_RUNNER = """#include "httk_workflow.h"
#include <signal.h>
static int step_go(void) {
    signal(SIGCHLD, SIG_IGN);
    httk_workflow_succeed();
    return 0;
}
int main(int argc, char **argv) {
    static const httk_workflow_step steps[] = {{"go", step_go}};
    if (httk_workflow_runner("tests.c.sigchld", steps, 1) != 0) return 2;
    return httk_workflow_main(argc, argv);
}
"""

# A cluster walltime warning delivers SIGUSR1 to the runner mid-verb. A watcher
# child spams it throughout, so the bridge reads and waits are interrupted; the
# EINTR retries must keep `begin` from capturing an empty step and dispatching a
# spurious unknown_step for a healthy attempt.
_SIGUSR1_RUNNER = """#define _POSIX_C_SOURCE 200809L
#include "httk_workflow.h"
#include <signal.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
static void on_usr1(int signal_number) { (void)signal_number; }
static int step_go(void) {
    httk_workflow_succeed();
    return 0;
}
int main(int argc, char **argv) {
    pid_t self = getpid();
    struct sigaction action;
    action.sa_handler = on_usr1;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0; /* no SA_RESTART: reads and waits get EINTR */
    sigaction(SIGUSR1, &action, (struct sigaction *)0);
    pid_t watcher = fork();
    if (watcher == 0) {
        struct timespec nap = {0, 300000};
        for (;;) {
            nanosleep(&nap, (struct timespec *)0);
            kill(self, SIGUSR1);
        }
    }
    static const httk_workflow_step steps[] = {{"go", step_go}};
    int status = 2;
    if (httk_workflow_runner("tests.c.usr1", steps, 1) == 0) {
        status = httk_workflow_main(argc, argv);
    }
    kill(watcher, SIGKILL);
    return status;
}
"""


def _compile_source(tmp_path: Path, source: str, name: str) -> Path:
    path = tmp_path / f"{name}.c"
    path.write_text(source, encoding="utf-8")
    result = _compile(tmp_path, path, name=name)
    assert result.returncode == 0, result.stderr
    return tmp_path / name


def test_ignoring_sigchld_does_not_spin_and_still_publishes(tmp_path: Path) -> None:
    binary = _compile_source(tmp_path, _SIGCHLD_RUNNER, "sigchld")
    attempt = _attempt(tmp_path, step="go")

    # A short timeout is the regression guard: the pre-fix waitpid retried ECHILD
    # forever and would hang here.
    completed = subprocess.run(
        [str(binary)],
        cwd=attempt.workdir,
        env=attempt.environment,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode in (0, 2)  # environment-log cannot read status under SIG_IGN
    assert attempt.outcome()["action"] == "succeed"


def test_a_signal_storm_mid_verb_does_not_corrupt_dispatch(tmp_path: Path) -> None:
    binary = _compile_source(tmp_path, _SIGUSR1_RUNNER, "usr1")
    attempt = _attempt(tmp_path, step="go")

    completed = subprocess.run(
        [str(binary)],
        cwd=attempt.workdir,
        env=attempt.environment,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    # begin's capture survived the EINTR storm: the real step ran, no spurious fail.
    assert attempt.outcome()["action"] == "succeed"
