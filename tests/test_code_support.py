"""Simulation-code support reaches the shell bridge and attempts only through the registry.

A fake ``fakecode`` support package is written to a temporary directory in the
layout a real ``httk-workflow-<code>`` distribution installs: a bridge module and
a Bash API file in its own package, and a registry package
``httk.registry.codes.fakecode`` that calls :func:`httk.core.register.register_code`.
"""

import io
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core.register import codes as core_codes
from httk.core.register import register_code

from httk.workflow import TaskManager, _kernel, _shell_bridge
from httk.workflow._state import read_state_unowned
from httk.workflow.codes import code_environment, installed_codes
from httk.workflow.errors import RunnerResolutionError
from httk.workflow.scaffold import new_job
from test_job_creation import workspace_at

SRC = Path(__file__).parents[1] / "src"

_BRIDGE = '''
def add_commands(commands):
    commands.add_parser("fakecode-hello").add_argument("name", nargs="?")


def run_command(namespace):
    if namespace.name is None:
        return 1
    if namespace.name == "refuse":
        raise ValueError("fakecode-hello refuses")
    print(f"hello {namespace.name}")
    return 0
'''

_BASH_API = """fakecode_hello() {
    _httk_workflow_bridge fakecode-hello "$@"
}
"""

_REGISTRATION = """from httk.core.register import register_code

register_code("fakecode", bridge="fakecode_support._bridge", bash_api="fakecode_support:fake.sh")
"""

_RUNNER = """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.codes.fake start

step_start() {
    compgen -e | grep '_BASH_API$' | sort >"$HTTK_WORKFLOW_JOB_DIR/variables.txt"
    if [ -n "${HTTK_WORKFLOW_FAKECODE_BASH_API:-}" ]; then
        source "$HTTK_WORKFLOW_FAKECODE_BASH_API"
        fakecode_hello world >"$HTTK_WORKFLOW_JOB_DIR/hello.txt"
    fi
    httk_workflow_succeed
}

httk_workflow_main
"""


@pytest.fixture(autouse=True)
def _isolated_codes(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Give each test an empty code registry and a fresh bridge table."""

    monkeypatch.setattr(core_codes, "_codes", {})
    _shell_bridge._code_bridges.cache_clear()
    yield
    _shell_bridge._code_bridges.cache_clear()
    sys.modules.pop("fakecode_support._bridge", None)
    sys.modules.pop("fakecode_support", None)


@pytest.fixture
def fake_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Install the fake code package, register it here, and return its root."""

    root = tmp_path / "site"
    package = root / "fakecode_support"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "_bridge.py").write_text(_BRIDGE, encoding="utf-8")
    (package / "fake.sh").write_text(_BASH_API, encoding="utf-8")
    registry = root / "httk" / "registry" / "codes" / "fakecode"
    registry.mkdir(parents=True)
    (registry / "__init__.py").write_text(_REGISTRATION, encoding="utf-8")
    monkeypatch.syspath_prepend(str(root))
    # The manager's runner and every bridge process it spawns find the package
    # and its registration through the inherited PYTHONPATH.
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join((str(root), str(SRC))))
    register_code("fakecode", bridge="fakecode_support._bridge", bash_api="fakecode_support:fake.sh")
    return root


def _run_job(tmp_path: Path, *, failure: str | None = None) -> Path:
    """Run one Bash job of an installed package and return its job directory.

    The job must succeed, or fail with the protocol failure code *failure*.
    """

    workspace = workspace_at(tmp_path / "workspace")
    package = tmp_path / "package"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        '[workflow]\nname = "tests.codes.fake"\n\n[workflow.runner]\nsteps = ["start"]\n', encoding="utf-8"
    )
    (package / "run").write_text(_RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    job = new_job(workspace, package, placement="codes/jobs", install=True)
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=120.0)
    (done,) = _kernel.list_jobs(workspace, "succeeded" if failure is None else "failed")
    assert done.job_id == job.job_id
    if failure is not None:
        state, _damaged = read_state_unowned(done.path / "state.json")
        assert state is not None and state.failure is not None and state.failure["code"] == failure
    return done.path


def _bridge(*arguments: str, stdin: str = "") -> int:
    """Run the shell bridge in this process, as ``python -m`` would."""

    saved = sys.stdin
    sys.stdin = io.StringIO(stdin)
    try:
        return _shell_bridge.main(list(arguments))
    finally:
        sys.stdin = saved


def test_a_registered_code_mounts_its_bridge_commands(fake_code: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert [code.name for code in installed_codes()] == ["fakecode"]
    assert _bridge("fakecode-hello", "world") == 0
    assert capsys.readouterr().out == "hello world\n"
    # The bridge's uniform exit codes hold for a code's commands, batched or not.
    assert _bridge("fakecode-hello") == 1
    assert _bridge("fakecode-hello", "refuse") == 2
    assert "fakecode-hello refuses" in capsys.readouterr().err
    assert _bridge("batch", stdin="fakecode-hello a\nfakecode-hello\n") == 1
    captured = capsys.readouterr()
    assert captured.out == "hello a\n"
    assert "batch line 2 failed: fakecode-hello" in captured.err


def test_a_code_is_discovered_through_its_registry_package(fake_code: Path) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "httk.workflow._shell_bridge", "fakecode-hello", "discovered"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert (completed.returncode, completed.stdout) == (0, "hello discovered\n"), completed.stderr


def test_an_attempt_carries_the_code_bash_api(fake_code: Path, tmp_path: Path) -> None:
    variable = "HTTK_WORKFLOW_FAKECODE_BASH_API"
    assert code_environment() == {variable: str(fake_code / "fakecode_support" / "fake.sh")}
    payload = _run_job(tmp_path)
    variables = (payload / "variables.txt").read_text(encoding="utf-8").split()
    assert variables == ["HTTK_WORKFLOW_BASH_API", variable]
    assert (payload / "hello.txt").read_text(encoding="utf-8") == "hello world\n"


def test_without_codes_there_are_no_code_commands_or_variables(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert installed_codes() == ()
    assert code_environment() == {}
    assert _shell_bridge._code_bridges() == {}
    with pytest.raises(SystemExit):
        _bridge("fakecode-hello", "world")
    assert "invalid choice: 'fakecode-hello'" in capsys.readouterr().err

    payload = _run_job(tmp_path)
    assert (payload / "variables.txt").read_text(encoding="utf-8").split() == ["HTTK_WORKFLOW_BASH_API"]
    assert not (payload / "hello.txt").exists()


def test_a_broken_code_bridge_breaks_only_its_own_commands(fake_code: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (fake_code / "fakecode_support" / "broken.py").write_text(
        "raise ImportError('no such library')\n", encoding="utf-8"
    )
    register_code("brokencode", bridge="fakecode_support.broken")
    assert _bridge("calc", "1+1") == 0
    captured = capsys.readouterr()
    assert captured.out == "2\n"
    assert "code 'brokencode' bridge 'fakecode_support.broken' is unavailable: no such library" in captured.err
    with pytest.raises(SystemExit) as refused:
        _bridge("brokencode-hello")
    assert refused.value.code == 2
    assert _bridge("fakecode-hello", "world") == 0


@pytest.mark.parametrize("bash_api", ["fakecode_support:absent.sh", "no_such_code_package:fake.sh"])
def test_an_unavailable_bash_api_fails_the_attempt_not_the_manager(
    fake_code: Path, tmp_path: Path, bash_api: str
) -> None:
    register_code("gone", bridge="fakecode_support._bridge", bash_api=bash_api)
    with pytest.raises(RunnerResolutionError, match=f"code 'gone' Bash API '{bash_api}'") as unavailable:
        code_environment()
    assert unavailable.value.code == "code_support_unavailable"
    _run_job(tmp_path, failure="code_support_unavailable")
