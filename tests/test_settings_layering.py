"""Layered application settings: one setting resolved through four layers.

An application setting such as ``vasp.command`` is resolved most-specific first —
the job's own ``inputs``, then the environment, then the workspace's configured
settings, then a default — so an operator configures a machine's VASP command
once per workspace instead of exporting it for every job, while a single job or a
single shell can still override it. These tests prove that order holds in both
SDKs identically, that a remote definition seeds a new workspace's settings, and
that the packaged VASP runner reads the workspace setting with no environment at
all.
"""

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import fake_remote
from httk.workflow import Workspace
from httk.workflow.adapters import SEED_SETTING_MAP, seed_application_settings
from httk.workflow.manager import TaskManager
from httk.workflow.projects import initialize_project
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.sdk import Attempt
from httk.workflow.workflow_cli import command

_BASH_API = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "shell" / "httk-workflow.sh"

_EXPORT_RUNNER = """#!/usr/bin/env python3
import json
import os
from httk.workflow import Runner

run = Runner("tests.settings_export")


@run.step
def only(a):
    (a.workdir / "environment.json").write_text(
        json.dumps({{name: os.environ.get(name) for name in {names!r}}}),
        encoding="utf-8",
    )
    a.succeed()


raise SystemExit(run.main())
"""


def _manager_environment(
    tmp_path: Path,
    settings: dict[str, object],
    names: tuple[str, ...],
    before_run=None,
) -> dict[str, object]:
    """Run one tiny job and return the selected variables from its environment."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    for key, value in settings.items():
        workspace.set_setting(key, value)
    payload = tmp_path / "payload"
    payload.mkdir()
    runner = payload / "runner.py"
    runner.write_text(_EXPORT_RUNNER.format(names=names), encoding="utf-8")
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Settings export",
            workflow="tests.settings_export",
            runner_path="runner.py",
            initial_step="only",
            data_mode="none",
        ),
    )
    workspace.submit(payload, "project/settings")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        if before_run is not None:
            before_run(workspace)
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    result = workspace.payload_path(marker.placement, marker.job_key) / "run" / "environment.json"
    return json.loads(result.read_text(encoding="utf-8"))


def _attempt_environment(
    tmp_path: Path, *, parameters: dict[str, object], settings: dict[str, object]
) -> dict[str, str]:
    """Fabricate one attempt whose job carries *parameters* and whose context carries
    *settings*, returning the environment both SDKs bind to.

    The ``settings`` member of ``context.json`` is exactly the workspace layer a
    manager snapshots at claim time, so writing it here reproduces what a runner
    sees without a manager in the loop.
    """

    payload = tmp_path / "payload"
    (payload / "files").mkdir(parents=True)
    (payload / "files" / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.settings",
            runner_path="files/runner",
            initial_step="only",
            data_mode="none",
            parameters=parameters,
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
                "step": "only",
                "activation_id": str(uuid.uuid4()),
                "attempt_id": str(uuid.uuid4()),
                "data_generation": None,
                "children": [],
                "settings": settings,
            }
        ),
        encoding="utf-8",
    )
    return {
        "HTTK_WORKFLOW_CONTEXT": str(control / "context.json"),
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(payload),
        "HTTK_WORKFLOW_WORKDIR": str(workdir),
        "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
        "HTTK_WORKFLOW_STEP": "only",
    }


def _python_setting(environment: dict[str, str], overlay: dict[str, str], name: str, default: object) -> object:
    """Resolve one setting through the Python SDK, under exactly *overlay* env."""

    saved = dict(os.environ)
    try:
        os.environ.clear()
        os.environ.update({**environment, **overlay})
        return Attempt.initialize(environment).setting(name, default)
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _bash_setting(
    environment: dict[str, str],
    overlay: dict[str, str],
    name: str,
    default: str | None,
) -> subprocess.CompletedProcess[str]:
    """Resolve one setting through the Bash SDK, under exactly *overlay* env."""

    call = f'httk_workflow_setting {name}' if default is None else f'httk_workflow_setting {name} {default!r}'
    script = f'set -eu\nsource "$HTTK_WORKFLOW_BASH_API"\n{call}\n'
    child = {
        **environment,
        **overlay,
        "HTTK_WORKFLOW_BASH_API": str(_BASH_API),
        "HTTK_WORKFLOW_PYTHON": sys.executable,
        "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
        "PATH": os.environ.get("PATH", ""),
        # The bash SDK boots a Python that imports NumPy through httk-core; cap
        # its thread pools exactly as conftest does for the test process itself,
        # or parallel runs exhaust the pid limit on constrained machines.
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
    }
    return subprocess.run(["bash", "-c", script], env=child, text=True, capture_output=True, check=False)


_ENV = {"HTTK_VASP_COMMAND": "srun -n 32 vasp_std"}


@pytest.mark.parametrize(
    ("parameters", "settings", "overlay", "expected"),
    [
        ({}, {}, {}, "the-default"),
        ({}, {"vasp.command": "workspace vasp"}, {}, "workspace vasp"),
        ({}, {"vasp.command": "workspace vasp"}, _ENV, "srun -n 32 vasp_std"),
        ({"vasp.command": "job vasp"}, {"vasp.command": "workspace vasp"}, _ENV, "job vasp"),
    ],
    ids=["only-default", "workspace-only", "env-beats-workspace", "parameter-beats-env"],
)
def test_the_two_sdks_resolve_the_same_layers_in_the_same_order(
    tmp_path: Path,
    parameters: dict[str, object],
    settings: dict[str, object],
    overlay: dict[str, str],
    expected: str,
) -> None:
    """parameters beat environment beat workspace settings beat the default, and Bash
    resolves the very same scenario to the very same bytes."""

    environment = _attempt_environment(tmp_path, parameters=parameters, settings=settings)

    assert _python_setting(environment, overlay, "vasp.command", "the-default") == expected

    completed = _bash_setting(environment, overlay, "vasp.command", "the-default")
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == expected


def test_an_absent_setting_is_none_in_python_and_exit_one_in_bash(tmp_path: Path) -> None:
    """With no layer supplying it and no default, the answer is legitimately
    absent: ``None`` in Python and a bare exit 1 in Bash."""

    environment = _attempt_environment(tmp_path, parameters={}, settings={})

    assert _python_setting(environment, {}, "vasp.command", None) is None

    completed = _bash_setting(environment, {}, "vasp.command", None)
    assert completed.returncode == 1
    assert completed.stdout == ""


def test_a_remote_definition_seeds_the_new_workspaces_settings(tmp_path: Path) -> None:
    """A workspace bound to a remote starts with the remote's whitelisted
    settings as its application settings, so no operator restates them."""

    assert SEED_SETTING_MAP["vasp_command"] == "vasp.command"
    assert SEED_SETTING_MAP["vasp_pseudo_library"] == "vasp.pseudo_library"

    project = tmp_path / "project"
    initialize_project(project, name="seeding")
    bundle = fake_remote(
        project,
        template="local",
        name="cluster",
        vasp_command="srun vasp_std",
        vasp_pseudo_library="/data/potpaw",
    )

    seeds = seed_application_settings(bundle)
    assert seeds == {"vasp.command": "srun vasp_std", "vasp.pseudo_library": "/data/potpaw"}

    destination = tmp_path / "runs"
    assert (
        command(["workspace", "init", f"cluster:{destination}", "--name", "station"], CLIContext("httk", project)) == 0
    )
    # A `local`-template remote runs the adapter here, so the seeded workspace is
    # the one on disk and its settings carry the remote's command.
    assert Workspace(destination).settings == {
        "vasp.command": "srun vasp_std",
        "vasp.pseudo_library": "/data/potpaw",
    }


def test_the_manager_exports_scalar_settings_and_preserves_real_environment_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTK_EXAMPLE_SCALAR", "machine value")
    names = (
        "HTTK_EXAMPLE_SCALAR",
        "HTTK_EXAMPLE_INTEGER",
        "HTTK_EXAMPLE_FLOAT",
        "HTTK_EXAMPLE_BOOL",
        "HTTK_EXAMPLE_NONE",
        "HTTK_EXAMPLE_STRUCTURED",
        "HTTK_WORKFLOW_SECRET",
    )
    settings = {
        "example.scalar": "workspace value",
        "example.integer": 7,
        "example.float": 1.5,
        "example.bool": True,
        "example.none": None,
        "example.structured": {"value": 1},
        "workflow.secret": "reserved",
    }
    # Workspace writes intentionally validate scalar settings. This override
    # also checks the manager's defensive filtering for values from an older or
    # externally-written format document.
    monkeypatch.setattr(Workspace, "read_settings", lambda _workspace: settings)
    observed = _manager_environment(tmp_path, {}, names)
    assert observed == {
        "HTTK_EXAMPLE_SCALAR": "machine value",
        "HTTK_EXAMPLE_INTEGER": "7",
        "HTTK_EXAMPLE_FLOAT": "1.5",
        "HTTK_EXAMPLE_BOOL": None,
        "HTTK_EXAMPLE_NONE": None,
        "HTTK_EXAMPLE_STRUCTURED": None,
        "HTTK_WORKFLOW_SECRET": None,
    }


def test_reserved_setting_exports_are_warned_about(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow import manager as manager_module

    warnings: list[str] = []
    monkeypatch.setattr(manager_module._LOGGER, "warning", lambda message, *args: warnings.append(message % args))
    _manager_environment(tmp_path, {"workflow.secret": "reserved"}, ("HTTK_WORKFLOW_SECRET",))
    assert "setting workflow.secret shadows the reserved HTTK_WORKFLOW_ namespace; not exported" in warnings


def test_manager_snapshots_settings_at_claim_time(tmp_path: Path) -> None:
    observed = _manager_environment(
        tmp_path,
        {},
        ("HTTK_EXAMPLE_SCALAR",),
        before_run=lambda workspace: Workspace(workspace.root).set_setting("example.scalar", "changed"),
    )
    assert observed["HTTK_EXAMPLE_SCALAR"] == "changed"


def test_seed_settings_skips_a_derived_name_collision(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "settings")
    workspace.set_setting("a.b", "operator")
    assert workspace.seed_settings({"a_b": "seed"}) == {"a.b": "operator"}


def test_a_workflow_prelude_initializes_the_runner_environment(tmp_path: Path) -> None:
    """Layer 2: the workspace prelude for the job's workflow runs in a login shell
    before the runner, so a variable it exports reaches the runner's environment."""

    observed = _manager_environment(
        tmp_path,
        {},
        ("HTTK_PRELUDE_MARKER",),
        before_run=lambda workspace: workspace.set_workflow_prelude(
            "tests.settings_export", "export HTTK_PRELUDE_MARKER=hello"
        ),
    )
    assert observed["HTTK_PRELUDE_MARKER"] == "hello"


def test_a_failing_workflow_prelude_aborts_before_the_runner(tmp_path: Path) -> None:
    """``set -e`` in the prelude aborts the login shell before it execs the runner,
    so a job whose prelude fails never succeeds."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = tmp_path / "payload"
    payload.mkdir()
    runner = payload / "runner.py"
    runner.write_text(_EXPORT_RUNNER.format(names=("HTTK_PRELUDE_MARKER",)), encoding="utf-8")
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Settings export",
            workflow="tests.settings_export",
            runner_path="runner.py",
            initial_step="only",
            data_mode="none",
        ),
    )
    workspace.submit(payload, "project/settings")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        workspace.set_workflow_prelude("tests.settings_export", "false")
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind != "succeeded"


def test_the_vasp_runner_reads_the_workspace_setting_without_the_environment(tmp_path: Path, monkeypatch) -> None:
    """The packaged relaxation runner takes its command from ``vasp.command``, so a
    workspace configured with it needs no ``HTTK_VASP_COMMAND`` in the run's env."""

    from httk.workflow.vasp.runners.vasp_relax import vasp_argv

    monkeypatch.delenv("HTTK_VASP_COMMAND", raising=False)
    environment = _attempt_environment(
        tmp_path,
        parameters={},
        settings={"vasp.command": "srun -n 8 vasp_std"},
    )
    attempt = Attempt.initialize(environment)
    assert vasp_argv(attempt) == ("srun", "-n", "8", "vasp_std")
