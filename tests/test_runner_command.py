"""A workflow package's declared ``[workflow.runner].command`` replaces its ``run`` bridge."""

import json
import shutil
import uuid
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace, _kernel, _store
from httk.workflow._job import JobDefinition
from httk.workflow.errors import FormatError
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.scaffold import describe_package_runner, new_job
from test_job_creation import install, workspace_at
from test_runner_builds import _build

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


def test_the_command_lives_in_the_installation_not_in_job_json(tmp_path: Path) -> None:
    workspace = workspace_at(tmp_path / "workspace")
    job = new_job(workspace, _python_package(tmp_path / "package"), install=True)
    document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    assert "runner" not in document and document["workflow"]["id"] == "local:tests.command"
    (installed,) = _store.list_installed(workspace)
    assert installed.record["runner"] == {"command": ["python3", "{package}/runner.py"], "entry": None, "builtin": None}


def _run(workspace: Workspace) -> _kernel.JobRef:
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=120.0)
    (done,) = _kernel.list_jobs(workspace, "succeeded")
    return done


@pytest.mark.slow
def test_the_manager_runs_an_interpreter_command_without_a_run_entry(tmp_path: Path) -> None:
    workspace = workspace_at(tmp_path / "workspace")
    package = _python_package(tmp_path / "package")
    assert describe_package_runner(package) == {"workflow": "tests.command.python", "steps": ["start"]}
    job = new_job(workspace, package, install=True)
    done = _run(workspace)
    assert done.job_id == job.job_id
    assert (done.path / "run" / "done.txt").is_file()


@pytest.mark.slow
@pytest.mark.skipif(_CC is None, reason="no C compiler (cc) is available")
def test_the_manager_runs_a_compiled_command_after_the_build_is_registered(tmp_path: Path) -> None:
    workspace = workspace_at(tmp_path / "workspace")
    package = _c_package(tmp_path / "package")
    assert not (package / "run").exists()
    installed = install(workspace, package, build=False)
    job = new_job(workspace, installed.id)
    with TaskManager(workspace) as manager:
        assert manager.run_until_idle(timeout=120.0).ready_claimable == 0
    _build(workspace, installed.id)
    assert _run(workspace).job_id == job.job_id


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


@pytest.mark.slow
def test_an_inherited_child_runs_the_parents_command(tmp_path: Path) -> None:
    workspace = workspace_at(tmp_path / "workspace")
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
    job = new_job(workspace, package, install=True)
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=120.0)
    done = {ref.job_id: ref for ref in _kernel.list_jobs(workspace, "succeeded")}
    assert job.job_id in done and len(done) == 2
    (child,) = [ref for job_id, ref in done.items() if job_id != job.job_id]
    assert JobDefinition.from_path(child.path / "job.json").workflow_id == job.workflow
    assert (child.path / "run" / "child.txt").is_file()


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


@pytest.mark.slow
@pytest.mark.parametrize(
    ("entry", "source", "workflow"),
    [
        ("run.py", "#!/usr/bin/env python3\n" + _PYTHON_RUNNER, "tests.command.python"),
        ("run.sh", _BASH_ENTRY, "tests.command.bash"),
    ],
)
def test_a_named_entry_runs_end_to_end_as_its_command(tmp_path: Path, entry: str, source: str, workflow: str) -> None:
    workspace = workspace_at(tmp_path / "workspace")
    package = _entry_package(tmp_path / "package", entry, source)
    assert parse_workflow_manifest(package).command == (f"{{package}}/{entry}",)
    assert describe_package_runner(package) == {"workflow": workflow, "steps": ["start"]}
    job = new_job(workspace, package, install=True)
    (installed,) = _store.list_installed(workspace)
    assert installed.record["runner"]["command"] == [f"{{package}}/{entry}"]  # type: ignore[index]
    done = _run(workspace)
    assert done.job_id == job.job_id and (done.path / "run" / "done.txt").is_file()


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


def test_a_parent_member_must_locate_its_parent(tmp_path: Path) -> None:
    workspace = workspace_at(tmp_path / "workspace")
    job = new_job(workspace, _python_package(tmp_path / "package"), install=True)
    document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    parent_id = str(uuid.uuid4())
    good = {
        "workspace_id": str(uuid.uuid4()),
        "job_id": parent_id,
        "job_key": f"p--{parent_id}",
        "placement": "jobs",
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }
    assert JobDefinition.from_mapping(dict(document, parent=good)).parent == good
    for bad in (
        {"job_id": parent_id},
        {**good, "placement": 3},
        {**good, "placement": "a~b"},
        {**good, "job_key": f"p--{uuid.uuid4()}"},
        {**good, "job_id": "nope"},
        {**good, "spawn_id": "nope"},
        "parent",
    ):
        with pytest.raises(FormatError):
            JobDefinition.from_mapping(dict(document, parent=bad))
