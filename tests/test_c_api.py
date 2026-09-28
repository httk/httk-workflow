"""The C authoring SDK: describe parity, dispatch, and CLI scaffolding.

The C SDK is a bridge client, exactly like the Bash one: every verb execs
``$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge``, and only ``--describe``
is native. What is tested here is what only the C half can get wrong — compiling
warning-clean, describing itself byte-for-byte the way the Bash SDK does,
dispatching into a step handler, and turning a handler's ending into exactly one
outcome — plus `job new --from-runner` resolving and running a compiled runner.

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

_SDK = Path(httk.workflow.__file__).parent / "languages" / "c"
_SHELL = Path(httk.workflow.__file__).parent / "languages" / "bash" / "httk-workflow.sh"


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


def _attempt(tmp_path: Path, *, step: str, data_generation: int | None = None, parent: bool = False) -> _Attempt:
    """Fabricate one attempt of one job, without a manager (mirrors test_bash_sdk)."""

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
            workflow="tests.c",
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
    for name_to_drop in ("HTTK_WORKFLOW_DESCRIBE", "HTTK_WORKFLOW_RUNNER_WORKFLOW", "HTTK_WORKFLOW_RUNNER_STEPS"):
        environment.pop(name_to_drop, None)
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

    # And the scaffolder that resolves `job new --from-runner ./relax` reads it back.
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


def _cli_context(tmp_path: Path) -> tuple[Workspace, CLIContext, str]:
    """Compile a one-step ``relax`` runner and register a workspace for it."""

    _write_runner(tmp_path, "tests.c.cli", {"start": "httk_workflow_succeed(); return 0;"}, name="relax")
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    return workspace, context, register_ws(context, workspace.root)


def _job_new(ws: str, runner: str, context: CLIContext) -> int:
    return workflow_command(["job", "new", "--workspace", ws, "--from-runner", runner, "--step", "start"], context)


def _assert_succeeded(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    assert len(list(workspace.walk_markers(("succeeded",)))) == 1


def test_the_cli_scaffolds_and_runs_a_c_binary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """An absolute `--from-runner` path scaffolds, describes by running, and submits."""

    workspace, context, ws = _cli_context(tmp_path)
    assert _job_new(ws, str(tmp_path / "relax"), context) == 0, capsys.readouterr().err
    _assert_succeeded(workspace)


def test_the_cli_scaffolds_from_a_relative_runner_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The documented `--from-runner ./relax` flow: a relative path must not PATH-exec."""

    workspace, context, ws = _cli_context(tmp_path)
    # From the runner's own directory, `./relax` normalizes to bare `relax`; only
    # the resolve() in describe_runner keeps exec from doing a PATH lookup.
    monkeypatch.chdir(tmp_path)
    assert _job_new(ws, "./relax", context) == 0, capsys.readouterr().err
    _assert_succeeded(workspace)


def test_a_bare_runner_name_resolves_to_the_cwd_file_not_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """`--from-runner relax` runs the cwd file, never a PATH exec.

    The compiled `relax` exists only in the cwd, never on PATH, so a successful
    describe-and-run proves the resolve() in describe_runner turned the bare name
    into the cwd file explicitly. Pre-fix, `[str(Path("relax"))]` == `["relax"]`
    would have done a PATH lookup — a FileNotFoundError or a same-named-program
    hijack.
    """

    workspace, context, ws = _cli_context(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert _job_new(ws, "relax", context) == 0, capsys.readouterr().err
    _assert_succeeded(workspace)


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


@pytest.mark.timing
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


@pytest.mark.timing
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


_STAGE_RUNNER = r"""#include <stdio.h>
#include "httk_workflow.h"
static int step_go(void) {
    int staged = httk_workflow_stage_input("poscar", "POSCAR", "files/POSCAR");
    int missing = httk_workflow_stage_input("incar", "INCAR", "files/INCAR");
    int absent = httk_workflow_stage_input("nothing", "X", NULL);
    FILE *record = fopen("statuses", "w");
    if (record == NULL) return 1;
    fprintf(record, "%d %d %d", staged, missing, absent);
    fclose(record);
    return httk_workflow_succeed();
}
int main(int argc, char **argv) {
    static const httk_workflow_step steps[] = {{"go", step_go}};
    if (httk_workflow_runner("tests.c.stage", steps, 1) != 0) return 2;
    return httk_workflow_main(argc, argv);
}
"""


def test_stage_input_forwards_to_the_bridge(tmp_path: Path) -> None:
    binary = _compile_source(tmp_path, _STAGE_RUNNER, "stage")
    attempt = _attempt(tmp_path, step="go")
    (attempt.payload / "files" / "POSCAR").write_bytes(b"Si\n1.0\n")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert (attempt.workdir / "statuses").read_text(encoding="utf-8") == "0 1 1"
    assert (attempt.workdir / "POSCAR").read_bytes() == b"Si\n1.0\n"
    assert not (attempt.workdir / "INCAR").exists()
    assert attempt.outcome()["action"] == "succeed"


_FILES_PROGRAM = r"""#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "httk_workflow.h"
static int joined(const char *a, const char *b, const char *want) {
    char *got = httk_join_path(a, b);
    int ok = got != NULL && strcmp(got, want) == 0;
    free(got);
    return ok;
}
int main(void) {
    FILE *f = fopen("source", "wb");
    if (f == NULL) return 1;
    fputs("payload\n", f);
    fclose(f);
    if (httk_copy_file("source", "copy") != HTTK_WORKFLOW_OK) return 2;
    if (httk_copy_file("missing", "never") != HTTK_WORKFLOW_REFUSED) return 3;
    if (httk_copy_file("source", "./source") != HTTK_WORKFLOW_REFUSED) return 6;
    if (httk_copy_file(".", "never") != HTTK_WORKFLOW_REFUSED) return 7;
    if (httk_file_exists("copy") != 1 || httk_file_exists(".") != 0 || httk_file_exists("missing") != 0) return 4;
    if (!joined("a", "b", "a/b") || !joined("a/", "b", "a/b") || !joined("a", "/abs", "/abs")) return 5;
    return 0;
}
"""


def test_the_local_file_helpers(tmp_path: Path) -> None:
    binary = _compile_source(tmp_path, _FILES_PROGRAM, "files")
    completed = subprocess.run([str(binary)], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed
    assert (tmp_path / "copy").read_bytes() == b"payload\n"
    assert (tmp_path / "source").read_bytes() == b"payload\n"  # the same-file copy kept it
    assert not (tmp_path / "never").exists()


_PARENT_RUNNER = r"""#include <stdio.h>
#include <stdlib.h>
#include "httk_workflow.h"
static void show(const char *label, const char *field) {
    int status = -1;
    char *value = httk_workflow_parent(field, &status);
    char line[4096];
    snprintf(line, sizeof line, "%s %d %s", label, status, value != NULL ? value : "");
    httk_workflow_log("probe", line);
    free(value);
}
static int step_probe(void) {
    show("WHOLE", NULL);
    show("PAYLOAD", "payload");
    show("WORKDIR", "workdir");
    show("JOB", "job_id");
    return httk_workflow_succeed();
}
int main(int argc, char **argv) {
    static const httk_workflow_step steps[] = {{"probe", step_probe}};
    if (httk_workflow_runner("tests.c.parent", steps, 1) != 0) return 2;
    return httk_workflow_main(argc, argv);
}
"""


def test_parent_reads_the_spawning_job_or_answers_absent(tmp_path: Path) -> None:
    """A child reads its parent's payload, workdir and id; a job without a parent reads absent (1)."""

    binary = _compile_source(tmp_path, _PARENT_RUNNER, "parent")
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


_CALL_SUB_RUNNER = """#!{python}
from httk.workflow import Runner

run = Runner("tests.sub")


@run.step
def run_sub(a):
    a.succeed()


raise SystemExit(run.main())
"""


def _stage_call_target(attempt: _Attempt) -> None:
    """Write a runner file of another workflow and a file to stage into the attempt's workdir."""

    Workspace.initialize(attempt.payload.parent / "workspace")
    sub = attempt.workdir / "sub_runner.py"
    sub.write_text(_CALL_SUB_RUNNER.format(python=sys.executable), encoding="utf-8")
    sub.chmod(0o755)
    (attempt.workdir / "input.txt").write_text("staged-by-call\n", encoding="utf-8")


def _assert_called(attempt: _Attempt, stderr: str) -> None:
    """The call registered one child running the other workflow, with the staged file."""

    ready = attempt.control / "outcome.ready"
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert [entry["label"] for entry in spawn["children"]] == ["sub"]
    job_key = spawn["children"][0]["job_key"]
    assert f"KEY 0 {job_key}" in stderr, stderr
    child_dir = ready / "children" / "jobs" / job_key
    child = json.loads((child_dir / "job.json").read_text(encoding="utf-8"))
    assert child["workflow"] == "tests.sub"
    assert (child_dir / "files" / "input.txt").read_text(encoding="utf-8") == "staged-by-call\n"
    assert attempt.outcome()["join"]["condition"] == "all_succeeded"


def test_call_spawns_another_workflow_as_a_child(tmp_path: Path) -> None:
    """httk_workflow_call registers a runner file of another workflow as a labelled child."""

    body = r"""int status = -1;
    const char *args[] = {"--file", "input.txt=input.txt", NULL};
    char *key = httk_workflow_call("sub", "./sub_runner.py", args, &status);
    char line[512];
    snprintf(line, sizeof line, "KEY %d %s", status, key != NULL ? key : "");
    httk_workflow_log("probe", line);
    free(key);
    return httk_workflow_gather("finish", NULL);"""
    source = tmp_path / "call.c"
    source.write_text(
        "#include <stdio.h>\n#include <stdlib.h>\n"
        + _runner_source("tests.c.call", {"start": body, "finish": "return httk_workflow_succeed();"}),
        encoding="utf-8",
    )
    result = _compile(tmp_path, source, name="call")
    assert result.returncode == 0, result.stderr
    attempt = _attempt(tmp_path / "attempt", step="start")
    _stage_call_target(attempt)
    completed = attempt.run(tmp_path / "call")
    assert completed.returncode == 0, completed.stderr
    _assert_called(attempt, completed.stderr)
