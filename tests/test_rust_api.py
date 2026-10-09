"""The Rust authoring SDK: describe parity, dispatch, and outcomes.

The Rust SDK is a bridge client, exactly like the Bash, C, and Fortran ones:
every verb spawns ``$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge``, and
only ``--describe`` is native. Unlike the Fortran SDK it is *not* FFI over the C
library -- it is a std-only, dependency-free reimplementation of the same thin
pattern in safe Rust. What is tested here is what only the Rust half can get
wrong: building warning-clean with no crates.io dependency and no network,
describing itself byte-for-byte the way the Bash SDK does, dispatching into a
step handler, and turning a handler's ending into exactly one outcome.

Every test gates on ``cargo`` and skips cleanly without it; the whole build runs
``--offline`` and depends only on path crates, so it never touches the network.
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

_CARGO = shutil.which("cargo")
_CLIPPY = shutil.which("cargo-clippy")
pytestmark = pytest.mark.skipif(_CARGO is None, reason="no Rust toolchain (cargo) is available")

_RUST_SDK = Path(httk.workflow.__file__).parent / "languages" / "rust"
_SHELL = Path(httk.workflow.__file__).parent / "languages" / "bash" / "httk-workflow.sh"


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


_Attempt = FabricatedAttempt


def _attempt(
    tmp_path: Path,
    *,
    step: str,
    parameters: dict[str, object] | None = None,
    parent: bool = False,
    calls: bool = False,
) -> FabricatedAttempt:
    """Fabricate one attempt of one job of ``tests.rust``, without a manager."""

    return fabricate(
        tmp_path,
        step=step,
        workflow="tests.rust",
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


def test_the_sdk_and_a_runner_build_warning_clean(tmp_path: Path) -> None:
    """A std-only offline build is the contract; the crate and a runner are warning-clean."""

    sdk = _stage_sdk(tmp_path)
    sdk_build = _cargo_build(tmp_path, sdk / "Cargo.toml")
    assert sdk_build.returncode == 0, sdk_build.stderr
    assert "warning" not in sdk_build.stderr

    # Building a runner proves the path-dependency wiring compiles the same way.
    binary = _write_runner(tmp_path, "tests.rust.clean", {"only": "Ok(())"})
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


def test_a_host_error_propagated_with_question_mark_aborts_with_its_text(tmp_path: Path) -> None:
    """``?`` on a failing ``std::io`` or parse call aborts with status 1 and the error's own text."""

    binary = _write_runner(
        tmp_path,
        "tests.rust",
        {
            "read": 'let text = std::fs::read_to_string("missing.txt")?; attempt.runlog_note(&text)?; Ok(())',
            "parse": 'let count: i64 = "four".parse()?; attempt.runlog_note(&count.to_string())?; Ok(())',
        },
    )

    attempt = _attempt(tmp_path, step="read")
    completed = attempt.run(binary)
    assert completed.returncode == 1
    assert not (attempt.control / "outcome.ready").exists()
    breadcrumb = attempt.breadcrumb()
    assert breadcrumb["step"] == "read"
    assert breadcrumb["exception"] == "RustError"
    assert breadcrumb["message"] == "I/O error: No such file or directory (os error 2)"

    attempt = _attempt(tmp_path / "parse", step="parse")
    completed = attempt.run(binary)
    assert completed.returncode == 1
    assert attempt.breadcrumb()["message"] == "invalid integer: invalid digit found in string"


def test_stage_input_copies_a_payload_file_or_answers_false(tmp_path: Path) -> None:
    body = (
        'println!("{:?}", attempt.stage_input("poscar", "POSCAR", Some("files/POSCAR"))); '
        'println!("{:?}", attempt.stage_input("incar", "INCAR", Some("files/INCAR"))); '
        'println!("{:?}", attempt.stage_input("missing", "X", None)); '
        'println!("{:?}", attempt.stage_input("encut", "X", None)); '
        "let _ = attempt.succeed(); Ok(())"
    )
    binary = _write_runner(tmp_path, "tests.rust", {"start": body})
    attempt = _attempt(tmp_path, step="start", parameters={"encut": 520})
    (attempt.payload / "files" / "POSCAR").write_bytes(b"Si\n1.0\n")

    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == ["Ok(true)", "Ok(false)", "Ok(false)", "Err(Refused)"]
    assert (attempt.workdir / "POSCAR").read_bytes() == b"Si\n1.0\n"
    assert attempt.outcome()["action"] == "succeed"


def test_parent_reads_the_parent_location_or_answers_none(tmp_path: Path) -> None:
    body = (
        'println!("{}", attempt.parent(Some("payload")).unwrap().unwrap()); '
        'println!("{}", attempt.parent(Some("workdir")).unwrap().unwrap()); '
        'println!("{}", attempt.parent(Some("job_id")).unwrap().unwrap()); '
        'println!("{}", attempt.parent(None).unwrap().unwrap()); '
        "let _ = attempt.succeed(); Ok(())"
    )
    binary = _write_runner(tmp_path, "tests.rust", {"start": body})
    child = _attempt(tmp_path, step="start", parent=True)
    completed = child.run(binary)
    assert completed.returncode == 0, completed.stderr
    payload, job_id, spawn_id = _parent_of(child)
    lines = completed.stdout.splitlines()
    assert lines[:3] == [str(payload), str(payload / "run"), job_id]
    assert json.loads(lines[3])["spawn_id"] == spawn_id

    orphan = _write_runner(
        tmp_path, "tests.rust", {"start": 'println!("{:?}", attempt.parent(None)); Ok(())'}, "orphan"
    )
    completed = _attempt(tmp_path / "orphan", step="start").run(orphan)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "Ok(None)\n"


def test_call_spawns_an_installed_workflow_as_a_child(tmp_path: Path) -> None:
    input_file = _call_input(tmp_path)
    body = (
        f'let key = attempt.call("sub", "sub", &["--file", "input.txt={input_file}"]).unwrap().unwrap(); '
        'println!("{key}"); '
        'attempt.gather("finish", &httk_workflow::Gather::default()).unwrap(); Ok(())'
    )
    binary = _write_runner(tmp_path, "tests.rust", {"start": body, "finish": "Ok(())"})
    attempt = _attempt(tmp_path, step="start", calls=True)
    completed = attempt.run(binary)
    assert completed.returncode == 0, completed.stderr
    _assert_called(attempt, completed.stdout.strip())
