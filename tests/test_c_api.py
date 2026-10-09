"""The C authoring SDK: describe parity, dispatch, and CLI scaffolding.

The C SDK is a bridge client, exactly like the Bash one: every verb execs
``$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge``, and only ``--describe``
is native. What is tested here is what only the C half can get wrong — compiling
warning-clean, describing itself byte-for-byte the way the Bash SDK does,
dispatching into a step handler, and turning a handler's ending into exactly one
outcome — plus a compiled runner file installed ad hoc and run.

Every test gates on a C compiler and skips cleanly without one.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

import httk.workflow
from attempt_fixtures import FabricatedAttempt, assert_called, every_job, fabricate, run_manager, sub_package
from httk.workflow import Workspace
from httk.workflow.scaffold import describe_runner, new_job

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


_Attempt = FabricatedAttempt


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    parent: bool = False,
    calls: bool = False,
) -> FabricatedAttempt:
    """Fabricate one attempt of one job of ``tests.c``, without a manager."""

    return fabricate(
        tmp_path,
        step=step,
        workflow="tests.c",
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


def _workspace(tmp_path: Path) -> Workspace:
    """Compile a one-step ``relax`` runner and initialize a workspace for it."""

    _write_runner(tmp_path, "tests.c.cli", {"start": "httk_workflow_succeed(); return 0;"}, name="relax")
    return Workspace.initialize(tmp_path / "workspace", durable=False)


def _assert_succeeded(workspace: Workspace) -> None:
    run_manager(workspace)
    assert [ref.state for ref in every_job(workspace)] == ["succeeded"]


def test_a_c_binary_is_installed_ad_hoc_and_runs(tmp_path: Path) -> None:
    """An absolute runner path is described by running it, installed and submitted."""

    workspace = _workspace(tmp_path)
    new_job(workspace, tmp_path / "relax", step="start")
    _assert_succeeded(workspace)


def test_a_relative_runner_path_is_installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The documented `./relax` runner path: a relative path must not PATH-exec."""

    workspace = _workspace(tmp_path)
    # From the runner's own directory, `./relax` normalizes to bare `relax`; only
    # the resolve() in describe_runner keeps exec from doing a PATH lookup.
    monkeypatch.chdir(tmp_path)
    new_job(workspace, "./relax", step="start")
    _assert_succeeded(workspace)


def test_a_bare_runner_name_resolves_to_the_cwd_file_not_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A runner named `relax` is the cwd file, never a PATH exec.

    The compiled `relax` exists only in the cwd, never on PATH, so a successful
    describe-and-run proves the resolve() in describe_runner turned the bare name
    into the cwd file explicitly. Pre-fix, `[str(Path("relax"))]` == `["relax"]`
    would have done a PATH lookup — a FileNotFoundError or a same-named-program
    hijack.
    """

    workspace = _workspace(tmp_path)
    monkeypatch.chdir(tmp_path)
    new_job(workspace, "relax", step="start")
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
    """httk_workflow_call registers a job of another installed workflow as a labelled child."""

    body = r"""int status = -1;
    const char *args[] = {"--file", "input.txt=input.txt", NULL};
    char *key = httk_workflow_call("sub", "sub", args, &status);
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
    attempt = _attempt(tmp_path / "attempt", step="start", calls=True)
    _stage_call_target(attempt)
    completed = attempt.run(tmp_path / "call")
    assert completed.returncode == 0, completed.stderr
    _assert_called(attempt)
