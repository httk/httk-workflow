"""Builds of installed compiled workflow packages and how the manager uses them."""

import hashlib
import json
from pathlib import Path

import pytest
from httk.core.building import BuildError

from httk.workflow import TaskManager, Workspace, _kernel, _store
from httk.workflow.errors import RunnerResolutionError
from httk.workflow.scaffold import BuildSpec, new_job
from test_job_creation import install, workspace_at


@pytest.fixture(autouse=True)
def _fresh_probe_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_store, "_PLATFORM_CACHE", {})


def _script(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\nset -eu\n{body}\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _tool_package(root: Path, *, artifacts: str = "out") -> Path:
    """A package whose build copies ``payload`` to an executable ``out/tool``."""

    root.mkdir()
    (root / "httk_workflow.toml").write_text(
        "[workflow]\nname = 'tests.tool'\n[workflow.runner]\nsteps = ['start']\n"
        f"[workflow.build]\ncommand = './build.sh'\nartifacts = ['{artifacts}']\n",
        encoding="utf-8",
    )
    (root / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (root / "run").chmod(0o755)
    _script(
        root / "build.sh",
        'if [ "${BUILD_FAIL:-}" = "1" ]; then exit 17; fi; mkdir -p out; cp payload out/tool; chmod +x out/tool',
    )
    (root / "payload").write_text("payload\n", encoding="utf-8")
    return root


def _compiled_package(root: Path) -> Path:
    """A package whose ``run`` executes the built ``build/runner.py``, an SDK-free runner."""

    package = root / "package"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        "[workflow]\nname = 'compiled.test'\n[workflow.runner]\nsteps = ['start']\n"
        "[workflow.build]\ncommand = './build.sh'\nartifacts = ['build']\n",
        encoding="utf-8",
    )
    (package / "run").write_text(
        "#!/bin/sh\nexec \"${HTTK_WORKFLOW_RUNNER_ARTIFACTS:?}/build/runner.py\"\n", encoding="utf-8"
    )
    (package / "run").chmod(0o755)
    (package / "runner.py").write_text(
        "#!/usr/bin/env python3\n"
        "import json, os\n"
        "from pathlib import Path\n"
        "context = json.loads(os.environ['HTTK_WORKFLOW_CONTEXT'])\n"
        "workdir = Path(os.environ['HTTK_WORKFLOW_WORKDIR'])\n"
        "(workdir / 'used-artifact').write_text('yes')\n"
        "(workdir / 'languages-dir').write_text(os.environ['HTTK_WORKFLOW_LANGUAGES_DIR'])\n"
        "control = Path(os.environ['HTTK_WORKFLOW_CONTROL_DIR'])\n"
        "draft = control / 'outcome.tmp'\n"
        "draft.mkdir()\n"
        "(draft / 'outcome.json').write_text(json.dumps({'format': 'httk-workflow-outcome', 'format_version': 2,"
        "'job_id': context['job_id'], 'activation_id': context['activation_id'], "
        "'attempt_id': context['attempt_id'], 'action': 'succeed'}))\n"
        "draft.rename(control / 'outcome.ready')\n",
        encoding="utf-8",
    )
    _script(package / "build.sh", "mkdir -p build; cp runner.py build/runner.py; chmod +x build/runner.py")
    return package


def _build(ws: Workspace, workflow_id: str) -> Path:
    owner = _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})
    try:
        return _store.build(ws, owner, workflow_id)
    finally:
        owner.close()


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return workspace_at(tmp_path / "workspace")


def test_install_builds_registers_a_stamp_and_preserves_the_exec_bit(ws: Workspace, tmp_path: Path) -> None:
    installed = install(ws, _tool_package(tmp_path / "package"))
    built = installed.build_dir("any")
    assert built is not None
    tool = built / "artifacts" / "out" / "tool"
    assert tool.read_text(encoding="utf-8") == "payload\n" and tool.stat().st_mode & 0o111
    stamp = json.loads((built / "build.json").read_text(encoding="utf-8"))
    assert stamp["id"] == installed.id and stamp["tree_sha256"] == installed.record["tree_sha256"]
    assert stamp["platform_tag"] == "any" and stamp["command"] == "./build.sh"
    # The installed package keeps sources only.
    assert not (installed.package / "out").exists()


def test_a_rebuild_replaces_the_platform_registration(ws: Workspace, tmp_path: Path) -> None:
    installed = install(ws, _tool_package(tmp_path / "package"))
    first = json.loads((installed.directory / "builds" / "any" / "build.json").read_bytes())
    target = _build(ws, installed.id)
    assert target == installed.directory / "builds" / "any"
    assert json.loads((target / "build.json").read_bytes())["built_at"] >= first["built_at"]
    assert sorted(path.name for path in (installed.directory / "builds").iterdir()) == ["any"]


def test_a_failed_rebuild_keeps_the_previous_build(
    ws: Workspace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed = install(ws, _tool_package(tmp_path / "package"))
    stamp = (installed.directory / "builds" / "any" / "build.json").read_bytes()
    monkeypatch.setenv("BUILD_FAIL", "1")
    with pytest.raises(BuildError, match="exit code 17") as failed:
        _build(ws, installed.id)
    assert failed.value.code == "runner_build_failed"
    assert (installed.directory / "builds" / "any" / "build.json").read_bytes() == stamp


def test_build_errors_for_zero_matches_and_nonzero_exit(
    ws: Workspace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(BuildError, match="no artifacts") as empty:
        install(ws, _tool_package(tmp_path / "missing", artifacts="missing"))
    assert empty.value.code == "runner_build_failed"
    monkeypatch.setenv("BUILD_FAIL", "1")
    with pytest.raises(BuildError, match="exit code 17"):
        install(ws, _tool_package(tmp_path / "failing"))
    assert _store.list_installed(ws) == []
    # An install without the build leaves the package installed and unbuilt.
    installed = install(ws, _tool_package(tmp_path / "unbuilt"), build=False)
    assert installed.build_dir("any") is None
    assert _store.closure(ws, installed.id, check_builds=True).unbuilt == (installed.id,)


@pytest.mark.parametrize(
    ("output", "expected"),
    [("linux x86_64\n", "linux-x86_64"), ("", None), (".", None), ("..", None)],
)
def test_platform_tag_sanitizes_probe_output(tmp_path: Path, output: str, expected: str | None) -> None:
    body = "printf 'linux x86_64\\n'" if output == "linux x86_64\n" else f"printf '%s' {output!r}"
    probe = _script(tmp_path / "probe.sh", body)
    tag = _store.platform_tag(BuildSpec("true", (), platform=probe.as_posix()))
    if expected is not None:
        assert tag.startswith(expected + ".")
        assert len(tag.rsplit(".", 1)[1]) == 8
    else:
        assert tag.startswith("h") and len(tag) == 17


def test_platform_tag_hashes_long_output_and_memoizes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    counter = tmp_path / "counter"
    probe = _script(tmp_path / "probe.sh", f"printf x >> \"$COUNTER\"; printf '%s' {'x' * 80!r}")
    monkeypatch.setenv("COUNTER", str(counter))
    spec = BuildSpec("true", (), platform=probe.as_posix())
    tag = _store.platform_tag(spec)
    assert tag == "h" + hashlib.sha256(("x" * 80).encode()).hexdigest()[:16]
    assert _store.platform_tag(spec) == tag
    assert counter.read_text(encoding="utf-8") == "x"


def test_platform_probe_failure_is_structured(tmp_path: Path) -> None:
    probe = _script(tmp_path / "probe.sh", "printf failure >&2; exit 9")
    with pytest.raises(RunnerResolutionError, match="platform probe.*exit code 9") as failure:
        _store.platform_tag(BuildSpec("true", (), platform=probe.as_posix()))
    assert failure.value.code == "runner_build_failed"


def test_distinct_platform_outputs_get_distinct_registration_directories(tmp_path: Path) -> None:
    first_probe = _script(tmp_path / "probe-one.sh", "printf 'gpu/a'")
    second_probe = _script(tmp_path / "probe-two.sh", "printf 'gpu a'")
    tags = {
        _store.platform_tag(BuildSpec("true", (), platform=probe.as_posix())) for probe in (first_probe, second_probe)
    }
    assert len(tags) == 2
    assert all(tag.startswith("gpu-a.") for tag in tags)


@pytest.mark.slow
def test_an_unbuilt_workflow_waits_unclaimed_until_it_is_built(ws: Workspace, tmp_path: Path) -> None:
    installed = install(ws, _compiled_package(tmp_path), build=False)
    job = new_job(ws, installed.id)
    with TaskManager(ws) as manager:
        census = manager.run_until_idle(timeout=60)
    assert census.ready_claimable == 0
    (waiting,) = _kernel.list_jobs(ws, "ready")
    assert waiting.job_id == job.job_id and not (waiting.path / "state.json").exists()

    _build(ws, installed.id)
    with TaskManager(ws) as manager:
        manager.run_until_idle(timeout=60)
    (done,) = _kernel.list_jobs(ws, "succeeded")
    assert done.job_id == job.job_id
    assert (done.path / "run" / "used-artifact").read_text() == "yes"


@pytest.mark.slow
def test_build_and_attempt_environments_carry_only_the_languages_dir(
    ws: Workspace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An external package finds the installed language SDKs at build and at run time."""

    monkeypatch.setenv("HTTK_WORKFLOW_JOB_DIR", "/stray")
    package = _compiled_package(tmp_path)
    _script(
        package / "build.sh",
        "mkdir -p build; cp runner.py build/runner.py; chmod +x build/runner.py\n"
        'printf %s "$HTTK_WORKFLOW_LANGUAGES_DIR" > build/languages-dir\n'
        'printf %s "${HTTK_WORKFLOW_JOB_DIR-unset}" > build/stray',
    )
    installed = install(ws, package)
    built = installed.build_dir("any")
    assert built is not None
    artifacts = built / "artifacts"
    assert (artifacts / "build" / "stray").read_text(encoding="utf-8") == "unset"
    languages = Path((artifacts / "build" / "languages-dir").read_text(encoding="utf-8"))
    assert languages.is_absolute() and (languages / "c" / "httk_workflow.h").is_file()

    new_job(ws, installed.id)
    with TaskManager(ws) as manager:
        manager.run_until_idle(timeout=60)
    (done,) = _kernel.list_jobs(ws, "succeeded")
    assert Path((done.path / "run" / "languages-dir").read_text(encoding="utf-8")) == languages
