"""A workflow package's declared ``[workflow.runner].command`` replaces its ``run`` bridge."""

import json
import shutil
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.errors import FormatError
from httk.workflow.models import JobDefinition
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.precheck import precheck_jobs
from httk.workflow.scaffold import describe_package_runner, new_job
from httk.workflow.workflow_cli import command

_CC = shutil.which("cc")

_PYTHON_RUNNER = """from httk.workflow import Runner

run = Runner("tests.command.python")


@run.step
def start(a):
    (a.workdir / "done.txt").write_text("done\\n", encoding="utf-8")
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
"""

_C_RUNNER = """#include "httk_workflow.h"
static int step_start(void) { httk_workflow_succeed(); return 0; }
int main(int argc, char **argv) {
    static const httk_workflow_step steps[] = { {"start", step_start} };
    if (httk_workflow_runner("tests.command.c", steps, 1) != 0) return 2;
    return httk_workflow_main(argc, argv);
}
"""


def _manifest(root: Path, runner: str, extra: str = "") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "httk_workflow.toml").write_text(
        f"[workflow]\nname = 'tests.command'\n[workflow.runner]\nsteps = ['start']\n{runner}\n{extra}",
        encoding="utf-8",
    )
    return root


def _python_package(root: Path) -> Path:
    _manifest(root, "command = ['python3', '{package}/runner.py']")
    (root / "runner.py").write_text(_PYTHON_RUNNER, encoding="utf-8")
    return root


def _c_package(root: Path) -> Path:
    _manifest(
        root,
        "command = ['{artifacts}/relax']",
        "[workflow.build]\ncommand = './build.sh'\nartifacts = ['relax']\n",
    )
    (root / "relax.c").write_text(_C_RUNNER, encoding="utf-8")
    (root / "build.sh").write_text(
        '#!/bin/sh\nset -e\ncc -std=c99 -I"$HTTK_WORKFLOW_LANGUAGES_DIR/c" -o relax relax.c '
        '"$HTTK_WORKFLOW_LANGUAGES_DIR/c/httk_workflow.c"\n',
        encoding="utf-8",
    )
    (root / "build.sh").chmod(0o755)
    return root


def test_a_command_parses_with_placeholders_and_no_run_member(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_python_package(tmp_path / "python"))
    assert provider.command == ("python3", "{package}/runner.py")
    compiled = parse_workflow_manifest(_c_package(tmp_path / "c"))
    assert compiled.command == ("{artifacts}/relax",)
    java = _manifest(
        tmp_path / "java",
        "command = ['java', '-cp', '{artifacts}/classes', 'Relax']",
        "[workflow.build]\ncommand = 'make'\nartifacts = ['classes']\n",
    )
    assert parse_workflow_manifest(java).command == ("java", "-cp", "{artifacts}/classes", "Relax")


@pytest.mark.parametrize(
    ("runner", "extra", "message"),
    [
        ("command = []", "", "nonempty array"),
        ("command = 'python3'", "", "nonempty array"),
        ("command = ['python3', '{home}/x.py']", "", "must place"),
        ("command = ['python3', '{package']", "", "must place"),
        ("command = ['python3', 'x{package}/runner.py']", "", "must place"),
        ("command = ['python3', '{package}/../runner.py']", "", "must continue its placeholder"),
        ("command = ['{artifacts}/relax']", "", "requires [workflow.build]"),
        ("command = ['python3', '{package}/missing.py']", "", "member does not exist"),
        ("command = ['/usr/bin/python3', '{package}/runner.py']", "", "bare name"),
        ("command = ['./runner.py']", "", "bare name"),
        ("command = ['{package}/runner.py']", "", "must be executable"),
        ("entry = 'run'\ncommand = ['python3', '{package}/runner.py']", "", "not both"),
        (
            "command = ['{artifacts}/other']",
            "[workflow.build]\ncommand = 'make'\nartifacts = ['relax']\n",
            "not covered by [workflow.build].artifacts",
        ),
        (
            "command = ['{artifacts}/../relax']",
            "[workflow.build]\ncommand = 'make'\nartifacts = ['relax']\n",
            "must continue its placeholder",
        ),
        (
            "command = ['python3', '{package}/relax']",
            "[workflow.build]\ncommand = 'make'\nartifacts = ['relax']\n",
            "is a build artifact",
        ),
    ],
)
def test_a_malformed_command_is_refused(tmp_path: Path, runner: str, extra: str, message: str) -> None:
    package = _manifest(tmp_path / "package", runner, extra)
    (package / "runner.py").write_text(_PYTHON_RUNNER, encoding="utf-8")
    (package / "relax").write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match=message.replace("[", r"\[").replace("]", r"\]")):
        parse_workflow_manifest(package)


def test_a_command_package_may_not_also_carry_a_run_member(tmp_path: Path) -> None:
    package = _python_package(tmp_path / "package")
    (package / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="remove the package member 'run'"):
        parse_workflow_manifest(package)


def test_the_command_is_recorded_in_job_json_and_parsed_strictly(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, _python_package(tmp_path / "package"))
    document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    assert document["runner"]["command"] == ["python3", "{package}/runner.py"]
    assert JobDefinition.from_path(job.payload / "job.json").runner_command == ("python3", "{package}/runner.py")
    for bad in (
        [],
        ["{nope}"],
        ["/bin/sh"],
        "python3",
        ["{package}/../../usr/bin/env"],
        ["{artifacts}/../../evil"],
        ["{package}//runner.py"],
        ["{package}/./runner.py"],
        ["{package}/"],
        ["python3", "a{package}/runner.py"],
        ["python3", "{package}/runner.py{artifacts}"],
        ["-Dx={package}/runner.py"],
        ["{package}"],
        ["{artifacts}"],
        ["."],
        [".."],
    ):
        document["runner"]["command"] = bad
        with pytest.raises(FormatError):
            JobDefinition.from_mapping(document)
    document["runner"]["command"] = ["python3", "-Dhome={package}", "{package}/runner.py"]
    assert JobDefinition.from_mapping(document).runner_command == ("python3", "-Dhome={package}", "{package}/runner.py")
    payload = dict(document, runner={"source": "payload", "path": "runner.py", "command": ["python3"]})
    with pytest.raises(FormatError, match="payload runner"):
        JobDefinition.from_mapping(payload)


def test_the_manager_runs_an_interpreter_command_without_a_run_entry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    package = _python_package(tmp_path / "package")
    assert describe_package_runner(package) == {"workflow": "tests.command.python", "steps": ["start"]}
    job = new_job(workspace, package)
    (finding,) = precheck_jobs(workspace)
    assert finding["runner"] == {"status": "ok", "ok": True}, finding
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None
    assert marker.kind == "succeeded", workspace.read_state(marker).get("failure")
    assert (job.payload / "run" / "done.txt").is_file()
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "command-listing")
    capsys.readouterr()
    assert command(["runner", "describe", "--workspace", name, "--json"], context) == 0
    (listed,) = json.loads(capsys.readouterr().out)
    assert listed["kind"] == "tree" and listed["path"] == job.runner["path"]


@pytest.mark.skipif(_CC is None, reason="no C compiler (cc) is available")
def test_the_manager_runs_a_compiled_command_after_the_build_is_registered(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "command-c")
    package = _c_package(tmp_path / "package")
    assert not (package / "run").exists()

    first = new_job(workspace, package)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(first.job_id)
    assert marker is not None and marker.kind == "failed"
    assert workspace.read_state(marker)["failure"]["code"] == "runner_not_built"

    assert command(["build", "--workspace", name, str(package)], context) == 0
    second = new_job(workspace, package)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(second.job_id)
    assert marker is not None
    assert marker.kind == "succeeded", workspace.read_state(marker).get("failure")


@pytest.mark.skipif(_CC is None, reason="no C compiler (cc) is available")
def test_describe_runs_the_command_only_with_artifacts(tmp_path: Path) -> None:
    import os
    import subprocess

    import httk.workflow

    package = _c_package(tmp_path / "package")
    with pytest.raises(ValueError, match="no build artifacts"):
        describe_package_runner(package)
    languages = str(Path(httk.workflow.__file__).parent / "languages")
    subprocess.run(
        ["./build.sh"], cwd=package, env={**os.environ, "HTTK_WORKFLOW_LANGUAGES_DIR": languages}, check=True
    )
    assert describe_package_runner(package, artifacts=package) == {"workflow": "tests.command.c", "steps": ["start"]}


def _rewrite_job(job, change) -> None:
    path = job.payload / "job.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    change(document["runner"])
    path.write_text(json.dumps(document), encoding="utf-8")


def _run(workspace: Workspace, job):
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None
    return marker


def test_an_escaping_command_in_job_json_never_runs(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, _python_package(tmp_path / "package"))
    _rewrite_job(job, lambda runner: runner.update(command=["{package}/../../../../usr/bin/env", "touch", "x"]))
    marker = _run(workspace, job)
    assert marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "protocol_error" and "must continue its placeholder" in failure["message"], failure
    assert not (job.payload / "run" / "x").exists()


def test_a_placeholder_program_resolving_outside_its_root_is_refused(tmp_path: Path) -> None:
    from httk.workflow._manager_runners import runner_command_problem

    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, _python_package(tmp_path / "package"))
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "link").symlink_to(shutil.which("env") or "/usr/bin/env")
    _rewrite_job(job, lambda runner: runner.update(command=["{package}/link"]))
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert "escapes its root" in str(runner_command_problem(definition, tree, None))


def test_a_command_reference_missing_from_the_tree_is_runner_unavailable(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, _python_package(tmp_path / "package"))
    _rewrite_job(job, lambda runner: runner.update(command=["python3", "{package}/missing.py"]))
    (finding,) = precheck_jobs(workspace)
    assert finding["runner"] == {
        "status": "problem",
        "problem": f"runner {job.runner['path']} command reference {{package}}/missing.py does not exist",
    }
    marker = _run(workspace, job)
    assert marker.kind == "failed"
    assert workspace.read_state(marker)["failure"]["code"] == "runner_unavailable"


def test_runner_arguments_follow_the_command_and_the_runlog_records_it(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    package = _python_package(tmp_path / "package")
    (package / "runner.py").write_text(
        _PYTHON_RUNNER.replace(
            '(a.workdir / "done.txt").write_text("done\\n", encoding="utf-8")',
            'import sys; (a.workdir / "done.txt").write_text(" ".join(sys.argv[1:]), encoding="utf-8")',
        ),
        encoding="utf-8",
    )
    job = new_job(workspace, package)
    _rewrite_job(job, lambda runner: runner.update(arguments=["--extra", "value"]))
    marker = _run(workspace, job)
    assert marker.kind == "succeeded", workspace.read_state(marker).get("failure")
    assert (job.payload / "run" / "done.txt").read_text(encoding="utf-8") == "--extra value"
    events = [
        json.loads(line) for line in (job.payload / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    tree = workspace.runner_store_path(str(job.runner["path"]))
    (attempt,) = [event for event in events if event["kind"] == "attempt"]
    assert attempt["runner_command"] == ["python3", f"{tree}/runner.py"]


def test_an_inherited_child_runs_the_parents_command(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    package = _manifest(tmp_path / "package", "command = ['python3', '{package}/runner.py']\ninitial_step = 'parent'")
    (package / "httk_workflow.toml").write_text(
        (package / "httk_workflow.toml")
        .read_text(encoding="utf-8")
        .replace("['start']", "['parent', 'child', 'finish']"),
        encoding="utf-8",
    )
    (package / "runner.py").write_text(
        """from httk.workflow import ChildSpec, Runner

run = Runner("tests.command.spawn")


@run.step
def parent(a):
    a.spawn(ChildSpec(step="child", maximum_attempts_per_activation=1), label="kid")
    a.gather("finish", when="all_terminal")


@run.step
def child(a):
    (a.workdir / "child.txt").write_text("child", encoding="utf-8")
    a.succeed()


@run.step
def finish(a):
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
""",
        encoding="utf-8",
    )
    job = new_job(workspace, package)
    marker = _run(workspace, job)
    assert marker.kind == "succeeded", workspace.read_state(marker).get("failure")
    children = [found for found in workspace.walk_markers(("succeeded",)) if found.job_key != marker.job_key]
    (child,) = children
    child_job = JobDefinition.from_path(workspace.payload_path(child.placement, child.job_key) / "job.json")
    assert child_job.runner_command == ("python3", "{package}/runner.py")
    assert (workspace.payload_path(child.placement, child.job_key) / "run" / "child.txt").is_file()


_BASH_ENTRY = """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.command.bash start

step_start() {
    printf 'done\\n' >done.txt
    httk_workflow_succeed
}

httk_workflow_main
"""


def _entry_package(root: Path, entry: str, source: str) -> Path:
    _manifest(root, f"entry = '{entry}'")
    (root / entry).write_text(source, encoding="utf-8")
    (root / entry).chmod(0o755)
    return root


@pytest.mark.parametrize(
    ("entry", "source", "workflow"),
    [
        ("run.py", "#!/usr/bin/env python3\n" + _PYTHON_RUNNER, "tests.command.python"),
        ("run.sh", _BASH_ENTRY, "tests.command.bash"),
    ],
)
def test_a_named_entry_runs_end_to_end_as_its_command(tmp_path: Path, entry: str, source: str, workflow: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    package = _entry_package(tmp_path / "package", entry, source)
    assert parse_workflow_manifest(package).command == (f"{{package}}/{entry}",)
    assert describe_package_runner(package) == {"workflow": workflow, "steps": ["start"]}
    job = new_job(workspace, package)
    document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    assert document["runner"]["command"] == [f"{{package}}/{entry}"]
    (finding,) = precheck_jobs(workspace)
    assert finding["runner"] == {"status": "ok", "ok": True}, finding
    marker = _run(workspace, job)
    assert marker.kind == "succeeded", workspace.read_state(marker).get("failure")
    assert (job.payload / "run" / "done.txt").is_file()


def test_a_named_entry_follows_the_command_rules(tmp_path: Path) -> None:
    package = _entry_package(tmp_path / "stray", "run.py", "#!/usr/bin/env python3\n" + _PYTHON_RUNNER)
    (package / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="entry replaces the run entry point"):
        parse_workflow_manifest(package)
    plain = _entry_package(tmp_path / "plain", "run.py", _PYTHON_RUNNER)
    (plain / "run.py").chmod(0o644)
    with pytest.raises(ValueError, match=r"\[workflow.runner\].entry program \{package\}/run.py must be executable"):
        parse_workflow_manifest(plain)


def test_a_named_entry_that_is_a_build_artifact_points_at_command(tmp_path: Path) -> None:
    package = _manifest(
        tmp_path / "package", "entry = 'relax'", "[workflow.build]\ncommand = 'make'\nartifacts = ['relax']\n"
    )
    (package / "relax").write_text("#!/bin/sh\n", encoding="utf-8")
    (package / "relax").chmod(0o755)
    with pytest.raises(ValueError, match=r'is a build artifact; use command = \["\{artifacts\}/relax"\]'):
        parse_workflow_manifest(package)
