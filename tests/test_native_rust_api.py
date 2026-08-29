"""The native Rust authoring SDK: describe parity, dispatch, and one relaxation.

The Rust SDK is a bridge client, exactly like the Bash, C, and Fortran ones:
every verb spawns ``$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge``, and
only ``--describe`` is native. Unlike the Fortran SDK it is *not* FFI over the C
library -- it is a std-only, dependency-free reimplementation of the same thin
pattern in safe Rust. What is tested here is what only the Rust half can get
wrong: building warning-clean with no crates.io dependency and no network,
describing itself byte-for-byte the way the Bash SDK does, dispatching into a
step handler, and turning a handler's ending into exactly one outcome -- plus one
real VASP relaxation driven end to end through a real manager.

Every test gates on ``cargo`` and skips cleanly without it; the whole build runs
``--offline`` and depends only on path crates, so it never touches the network.
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

_CARGO = shutil.which("cargo")
_CLIPPY = shutil.which("cargo-clippy")
pytestmark = pytest.mark.skipif(_CARGO is None, reason="no Rust toolchain (cargo) is available")

_RUST_SDK = Path(httk.workflow.__file__).parent / "native" / "rust"
_SHELL = Path(httk.workflow.__file__).parent / "shell" / "httk-workflow.sh"
_MOCK_VASP = Path(__file__).parents[1] / "examples" / "mock_vasp.py"
_RELAX_RUST = Path(__file__).parents[1] / "examples" / "relax_rust"

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


def _cargo_env(tmp_path: Path) -> dict[str, str]:
    """A hermetic cargo environment: its own CARGO_HOME, and the workspace PYTHONPATH."""

    environment = os.environ.copy()
    environment["CARGO_HOME"] = str(tmp_path / "cargo-home")
    return environment


def _stage_sdk(tmp_path: Path) -> Path:
    """Copy the packaged SDK crate into the tmp tree, so no build ever lands in src/."""

    staged = tmp_path / "sdk"
    if not staged.exists():
        shutil.copytree(_RUST_SDK, staged, ignore=shutil.ignore_patterns("target", "Cargo.lock"))
    return staged


def _cargo_build(tmp_path: Path, manifest: Path, *, release: bool = False) -> subprocess.CompletedProcess[str]:
    """Build one crate offline into the shared tmp target dir, with a hermetic CARGO_HOME."""

    assert _CARGO is not None
    command = [_CARGO, "build", "--offline", "--manifest-path", str(manifest), "--target-dir", str(tmp_path / "target")]
    if release:
        command.append("--release")
    return subprocess.run(command, text=True, capture_output=True, check=False, env=_cargo_env(tmp_path))


def _runner_source(workflow: str, steps: dict[str, str]) -> str:
    """Assemble one Rust runner registering the given ``name -> body`` step handlers.

    Each body is Rust that owns the closure result: it publishes (or not) and
    returns ``Ok(())`` or ``Err(...)``, exactly as a C body would ``... ; return N;``.
    """

    names = ", ".join(f'"{name}"' for name in steps)
    registrations = "\n".join(
        f'        .step("{name}", |attempt: &Attempt| -> Result<(), StepError> {{ {body} }})'
        for name, body in steps.items()
    )
    return f"""#![allow(unused_variables, unused_imports)]
use httk_workflow::{{Attempt, Runner, StepError}};

fn main() {{
    Runner::new("{workflow}", &[{names}])
{registrations}
        .main();
}}
"""


def _write_runner(tmp_path: Path, workflow: str, steps: dict[str, str], name: str = "runner") -> Path:
    """Build one generated runner crate against the staged SDK; return its binary."""

    sdk = _stage_sdk(tmp_path)
    crate = tmp_path / name
    (crate / "src").mkdir(parents=True, exist_ok=True)
    (crate / "src" / "main.rs").write_text(_runner_source(workflow, steps), encoding="utf-8")
    (crate / "Cargo.toml").write_text(
        f"""[package]
name = "{name}"
version = "0.0.0"
edition = "2021"
publish = false

[[bin]]
name = "{name}"
path = "src/main.rs"

[dependencies]
httk_workflow = {{ path = "{sdk}" }}
""",
        encoding="utf-8",
    )
    result = _cargo_build(tmp_path, crate / "Cargo.toml")
    assert result.returncode == 0, result.stderr
    return tmp_path / "target" / "debug" / name


def _build_example(tmp_path: Path) -> Path:
    """Build examples/relax_rust against the staged SDK, out of tree; return its binary."""

    sdk = _stage_sdk(tmp_path)
    crate = tmp_path / "relax_example"
    (crate / "src").mkdir(parents=True, exist_ok=True)
    (crate / "src" / "main.rs").write_text(
        (_RELAX_RUST / "src" / "main.rs").read_text(encoding="utf-8"), encoding="utf-8"
    )
    (crate / "Cargo.toml").write_text(
        (_RELAX_RUST / "Cargo.toml")
        .read_text(encoding="utf-8")
        .replace('path = "../../src/httk/workflow/native/rust"', f'path = "{sdk}"'),
        encoding="utf-8",
    )
    result = _cargo_build(tmp_path, crate / "Cargo.toml", release=True)
    assert result.returncode == 0, result.stderr
    return tmp_path / "target" / "release" / "relax"


@dataclass(frozen=True)
class _Attempt:
    """One fabricated attempt a compiled Rust runner can be dispatched into."""

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
            workflow="tests.rust",
            runner_path="files/runner",
            initial_step=step,
            data_mode="none" if data_generation is None else "transactional",
        ),
    )
    control = payload / f"attempts/{uuid.uuid4()}"
    control.mkdir(parents=True)
    workdir = payload / "run"
    workdir.mkdir()
    context_json = json.dumps(
        {
            "format": "httk-workflow-attempt-context",
            "format_version": 2,
            "workspace_id": str(uuid.uuid4()),
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


def test_the_sdk_and_the_example_build_warning_clean(tmp_path: Path) -> None:
    """A std-only offline build is the contract; the crate and example are warning-clean."""

    sdk = _stage_sdk(tmp_path)
    sdk_build = _cargo_build(tmp_path, sdk / "Cargo.toml")
    assert sdk_build.returncode == 0, sdk_build.stderr
    assert "warning" not in sdk_build.stderr

    # Building the example proves the path-dependency wiring compiles the same way.
    binary = _build_example(tmp_path)
    assert binary.is_file()

    # A path-deps-only build must have fetched nothing: no vendored registry cache.
    assert not (tmp_path / "cargo-home" / "registry" / "cache").exists()

    if _CLIPPY is not None:
        assert _CARGO is not None
        clippy = subprocess.run(
            [
                _CARGO,
                "clippy",
                "--offline",
                "--manifest-path",
                str(sdk / "Cargo.toml"),
                "--target-dir",
                str(tmp_path / "target-clippy"),
                "--",
                "-D",
                "warnings",
            ],
            text=True,
            capture_output=True,
            check=False,
            env=_cargo_env(tmp_path),
        )
        assert clippy.returncode == 0, clippy.stderr


def test_describe_is_byte_identical_to_the_bash_sdk(tmp_path: Path) -> None:
    """The native handshake prints exactly what the Bash, C, and Fortran SDKs print."""

    workflow = "tests.rust.describe"
    order = ["relax", "collect", "prepare"]
    binary = _write_runner(tmp_path, workflow, {name: "Ok(())" for name in order})

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


def test_an_invalid_registration_refuses_before_describing(tmp_path: Path) -> None:
    """Validation precedes --describe: a bad workflow id exits 2 and emits no JSON."""

    binary = _write_runner(tmp_path, "bad id", {"only": "Ok(())"})
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
        assert invocation.returncode == 2
        assert invocation.stdout == ""


def test_an_unknown_step_is_reported_with_the_registered_steps(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.rust", {"relax": "let _ = attempt.succeed(); Ok(())", "collect": "Ok(())"})
    attempt = _attempt(tmp_path, step="realx")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"]["code"] == "unknown_step"
    assert "registered steps: collect, relax" in outcome["failure"]["message"]


def test_a_step_that_publishes_nothing_is_reported_as_no_outcome(tmp_path: Path) -> None:
    binary = _write_runner(tmp_path, "tests.rust", {"silent": 'attempt.log("info", "nothing"); Ok(())'})
    attempt = _attempt(tmp_path, step="silent")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert attempt.outcome()["failure"] == {
        "code": "no_outcome",
        "message": "step 'silent' finished without publishing an outcome",
    }


def test_a_step_can_publish_a_structured_failure(tmp_path: Path) -> None:
    binary = _write_runner(
        tmp_path, "tests.rust", {"start": 'let _ = attempt.fail("tests.broken", "it broke", false); Ok(())'}
    )
    attempt = _attempt(tmp_path, step="start")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    outcome = attempt.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"] == {"code": "tests.broken", "message": "it broke"}


def test_a_handler_that_returns_an_error_leaves_a_breadcrumb_and_no_outcome(tmp_path: Path) -> None:
    """A handler that returns ``Err(StepError::new(3))`` is the aborted-attempt path."""

    binary = _write_runner(tmp_path, "tests.rust", {"explode": "Err(StepError::new(3))"})
    attempt = _attempt(tmp_path, step="explode")

    completed = attempt.run(binary)
    assert completed.returncode == 3
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["format"] == "httk-workflow-runner-error"
    assert breadcrumb["step"] == "explode"
    assert breadcrumb["exception"] == "RustError"
    assert breadcrumb["message"] == "explode exited with status 3"


def test_the_relax_runner_prepares_runs_and_publishes(tmp_path: Path) -> None:
    """The examples/relax_rust runner, driven end to end through a real manager."""

    binary = _build_example(tmp_path)

    workspace = Workspace.initialize(tmp_path / "workspace")
    # The documented flow: name the mock VASP as the workspace vasp.command.
    workspace.set_setting("vasp.command", f"{sys.executable} {_MOCK_VASP}")

    reference = dict(workspace.publish_runner(binary, name="relax"))
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "POSCAR").write_text(_POSCAR, encoding="utf-8")

    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Rust relaxation",
            workflow="httk.vasp.relax-rust",
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
    binary = _build_example(tmp_path)

    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = dict(workspace.publish_runner(binary, name="relax"))
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "POSCAR").write_text(_POSCAR, encoding="utf-8")

    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Rust relaxation",
            workflow="httk.vasp.relax-rust",
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
