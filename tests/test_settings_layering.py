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
import logging
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]

from conftest import fake_remote
from httk.workflow import Workspace
from httk.workflow.adapters import SEED_SETTING_MAP, seed_application_settings
from httk.workflow.manager import TaskManager
from httk.workflow.projects import initialize_project
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.registry import create_workspace
from httk.workflow.sdk import Attempt

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


def _manager_environment(tmp_path: Path, settings: dict[str, object], names: tuple[str, ...]) -> dict[str, object]:
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
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    result = workspace.payload_path(marker.placement, marker.job_key) / "run" / "environment.json"
    return json.loads(result.read_text(encoding="utf-8"))


def _attempt_environment(tmp_path: Path, *, inputs: dict[str, object], settings: dict[str, object]) -> dict[str, str]:
    """Fabricate one attempt whose job carries *inputs* and whose context carries
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
            inputs=inputs,
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
    }
    return subprocess.run(["bash", "-c", script], env=child, text=True, capture_output=True, check=False)


_ENV = {"HTTK_VASP_COMMAND": "srun -n 32 vasp_std"}


@pytest.mark.parametrize(
    ("inputs", "settings", "overlay", "expected"),
    [
        ({}, {}, {}, "the-default"),
        ({}, {"vasp.command": "workspace vasp"}, {}, "workspace vasp"),
        ({}, {"vasp.command": "workspace vasp"}, _ENV, "srun -n 32 vasp_std"),
        ({"vasp.command": "job vasp"}, {"vasp.command": "workspace vasp"}, _ENV, "job vasp"),
    ],
    ids=["only-default", "workspace-only", "env-beats-workspace", "input-beats-env"],
)
def test_the_two_sdks_resolve_the_same_layers_in_the_same_order(
    tmp_path: Path,
    inputs: dict[str, object],
    settings: dict[str, object],
    overlay: dict[str, str],
    expected: str,
) -> None:
    """inputs beat environment beat workspace settings beat the default, and Bash
    resolves the very same scenario to the very same bytes."""

    environment = _attempt_environment(tmp_path, inputs=inputs, settings=settings)

    assert _python_setting(environment, overlay, "vasp.command", "the-default") == expected

    completed = _bash_setting(environment, overlay, "vasp.command", "the-default")
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == expected


def test_an_absent_setting_is_none_in_python_and_exit_one_in_bash(tmp_path: Path) -> None:
    """With no layer supplying it and no default, the answer is legitimately
    absent: ``None`` in Python and a bare exit 1 in Bash."""

    environment = _attempt_environment(tmp_path, inputs={}, settings={})

    assert _python_setting(environment, {}, "vasp.command", None) is None

    completed = _bash_setting(environment, {}, "vasp.command", None)
    assert completed.returncode == 1
    assert completed.stdout == ""


def test_a_remote_definition_seeds_the_new_workspaces_settings(tmp_path: Path) -> None:
    """A workspace bound to a remote starts with the remote's whitelisted queue
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

    seeds = seed_application_settings(bundle, "default")
    assert seeds == {"vasp.command": "srun vasp_std", "vasp.pseudo_library": "/data/potpaw"}

    binding = create_workspace(
        "station",
        remote="cluster",
        path=tmp_path / "runs",
        scope="project",
        project=project,
    )
    # A `local`-template remote runs the adapter here, so the seeded workspace is
    # the one on disk and its settings carry the remote's command.
    assert Workspace(binding.path).settings == {
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
    monkeypatch.setattr(Workspace, "settings", property(lambda _workspace: settings))
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


def test_reserved_setting_exports_are_warned_about(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="httk.workflow"):
        _manager_environment(tmp_path, {"workflow.secret": "reserved"}, ("HTTK_WORKFLOW_SECRET",))
    assert any(
        record.getMessage() == "setting workflow.secret shadows the reserved HTTK_WORKFLOW_ namespace; not exported"
        for record in caplog.records
    )


def test_the_vasp_runner_reads_the_workspace_setting_without_the_environment(tmp_path: Path, monkeypatch) -> None:
    """The packaged relaxation runner takes its command from ``vasp.command``, so a
    workspace configured with it needs no ``HTTK_VASP_COMMAND`` in the run's env."""

    from httk.workflow.vasp.runners.vasp_relax import vasp_argv

    monkeypatch.delenv("HTTK_VASP_COMMAND", raising=False)
    environment = _attempt_environment(
        tmp_path,
        inputs={},
        settings={"vasp.command": "srun -n 8 vasp_std"},
    )
    attempt = Attempt.initialize(environment)
    assert vasp_argv(attempt) == ("srun", "-n", "8", "vasp_std")
