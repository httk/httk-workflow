"""Registration and manager integration for compiled workflow runners."""

import hashlib
import json
import shlex
import shutil
from pathlib import Path, PurePosixPath

import pytest
from httk.core.cli import CLIContext
from httk.core.plugins.install import install_plugin

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, git_workflows
from httk.workflow._runner_builds import (
    platform_tag,
    register_build,
    registered_artifacts,
)
from httk.workflow.errors import RunnerResolutionError
from httk.workflow.packages import _reset_plugin_workflow_cache, load_workflow_package
from httk.workflow.scaffold import _WORKFLOW_PROVIDERS, BuildSpec, new_job
from httk.workflow.workflow_cli import command
from test_workflow_packages import _package


def _script(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\nset -eu\n{body}\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _runner(tmp_path: Path, workspace: Workspace, *, artifacts: str = "out") -> tuple[Path, PurePosixPath, str]:
    source = tmp_path / "runner"
    source.mkdir()
    (source / "httk_workflow.toml").write_text(
        f"[workflow.build]\ncommand = './build.sh'\nartifacts = ['{artifacts}']\n", encoding="utf-8"
    )
    (source / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (source / "run").chmod(0o755)
    build = _script(
        source / "build.sh",
        'if [ "${BUILD_FAIL:-}" = "1" ]; then exit 17; fi; mkdir -p out; cp payload out/tool; chmod +x out/tool',
    )
    _script(source / "build-again.sh", "mkdir -p out; printf replaced > out/tool")
    _script(source / "fail.sh", "exit 17")
    (source / "payload").write_text("payload\n", encoding="utf-8")
    reference = workspace.publish_runner(source, name="compiled")
    return build, PurePosixPath(str(reference["path"])), str(reference["sha256"])


def test_register_build_writes_stamp_log_and_preserves_exec_bit(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    build, relative, source_sha256 = _runner(tmp_path, workspace)
    artifacts = register_build(
        workspace,
        workspace.runner_store_path(relative),
        relative,
        BuildSpec(f"./{build.name}", ("out",)),
        source_sha256=source_sha256,
    )
    assert (artifacts / "out" / "tool").read_text(encoding="utf-8") == "payload\n"
    assert (artifacts / "out" / "tool").stat().st_mode & 0o111
    stamp = json.loads((artifacts.parent / "build.json").read_text(encoding="utf-8"))
    assert stamp["source_sha256"] == source_sha256
    assert (artifacts.parent.parent.parent / "any.log").is_file()


def test_reregister_build_replaces_the_platform_registration(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    build, relative, source_sha256 = _runner(tmp_path, workspace)
    first = register_build(
        workspace,
        workspace.runner_store_path(relative),
        relative,
        BuildSpec(f"./{build.name}", ("out",)),
        source_sha256=source_sha256,
    )
    second = register_build(
        workspace,
        workspace.runner_store_path(relative),
        relative,
        BuildSpec("./build-again.sh", ("out",)),
        source_sha256=source_sha256,
    )
    assert first != second
    assert (second / "out" / "tool").read_text(encoding="utf-8") == "payload\n"
    assert registered_artifacts(workspace, relative, "any", expected_source_sha256=source_sha256) == second
    assert first.parent.is_dir()


def test_failed_reregistration_keeps_the_previous_generation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    build, relative, source_sha256 = _runner(tmp_path, workspace)
    first = register_build(
        workspace,
        workspace.runner_store_path(relative),
        relative,
        BuildSpec(f"./{build.name}", ("out",)),
        source_sha256=source_sha256,
    )
    monkeypatch.setenv("BUILD_FAIL", "1")
    with pytest.raises(RunnerResolutionError, match="exit code 17"):
        register_build(
            workspace,
            workspace.runner_store_path(relative),
            relative,
            BuildSpec("./fail.sh", ("out",)),
            source_sha256=source_sha256,
        )
    assert registered_artifacts(workspace, relative, "any", expected_source_sha256=source_sha256) == first
    pointer = json.loads((first.parent.parent / "current.json").read_text(encoding="utf-8"))
    assert pointer == {"generation": first.parent.name}


def test_register_build_uses_the_verified_tree_manifest(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _, relative, source_sha256 = _runner(tmp_path, workspace)
    artifacts = register_build(
        workspace,
        workspace.runner_store_path(relative),
        relative,
        BuildSpec("./fail.sh", ("missing",)),
        source_sha256=source_sha256,
    )
    stamp = json.loads((artifacts.parent / "build.json").read_text(encoding="utf-8"))
    assert stamp["command"] == "./build.sh"
    assert (artifacts / "out" / "tool").read_text(encoding="utf-8") == "payload\n"


def test_register_build_rejects_a_claimed_source_digest_mismatch(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    build, relative, _ = _runner(tmp_path, workspace)
    with pytest.raises(RunnerResolutionError, match="does not match claimed digest") as failure:
        register_build(
            workspace,
            workspace.runner_store_path(relative),
            relative,
            BuildSpec(f"./{build.name}", ("out",)),
            source_sha256="0" * 64,
        )
    assert failure.value.code == "runner_build_failed"


def test_build_errors_for_zero_matches_and_nonzero_exit(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    build, relative, source_sha256 = _runner(tmp_path, workspace, artifacts="missing")
    with pytest.raises(RunnerResolutionError, match="no artifacts") as empty:
        register_build(
            workspace,
            workspace.runner_store_path(relative),
            relative,
            BuildSpec(f"./{build.name}", ("missing",)),
            source_sha256=source_sha256,
        )
    assert empty.value.code == "runner_build_failed"
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("BUILD_FAIL", "1")
        with pytest.raises(RunnerResolutionError, match="exit code 17") as failed:
            register_build(
                workspace,
                workspace.runner_store_path(relative),
                relative,
                BuildSpec("./fail.sh", ("out",)),
                source_sha256=source_sha256,
            )
    assert failed.value.code == "runner_build_failed"


@pytest.mark.parametrize(
    ("output", "expected"),
    [("linux x86_64\n", "linux-x86_64"), ("", None), (".", None), ("..", None)],
)
def test_platform_tag_sanitizes_probe_output(tmp_path: Path, output: str, expected: str | None) -> None:
    body = "printf 'linux x86_64\\n'" if output == "linux x86_64\n" else f"printf '%s' {output!r}"
    probe = _script(tmp_path / "probe.sh", body)
    tag = platform_tag(BuildSpec("true", (), platform=probe.as_posix()))
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
    tag = platform_tag(spec)
    assert tag == "h" + hashlib.sha256(("x" * 80).encode()).hexdigest()[:16]
    assert platform_tag(spec) == tag
    assert counter.read_text(encoding="utf-8") == "x"


def test_platform_probe_failure_is_structured(tmp_path: Path) -> None:
    probe = _script(tmp_path / "probe.sh", "printf failure >&2; exit 9")
    with pytest.raises(RunnerResolutionError, match="platform probe.*exit code 9") as failure:
        platform_tag(BuildSpec("true", (), platform=probe.as_posix()))
    assert failure.value.code == "runner_build_failed"


def test_distinct_platform_outputs_get_distinct_registration_directories(tmp_path: Path) -> None:
    first_probe = _script(tmp_path / "probe-one.sh", "printf 'gpu/a'")
    second_probe = _script(tmp_path / "probe-two.sh", "printf 'gpu a'")
    tags = {platform_tag(BuildSpec("true", (), platform=probe.as_posix())) for probe in (first_probe, second_probe)}
    assert len(tags) == 2
    assert all(tag.startswith("gpu-a.") for tag in tags)


def test_registered_artifacts_rejects_a_source_stamp_mismatch(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    build, relative, source_sha256 = _runner(tmp_path, workspace)
    register_build(
        workspace,
        workspace.runner_store_path(relative),
        relative,
        BuildSpec(f"./{build.name}", ("out",)),
        source_sha256=source_sha256,
    )
    assert registered_artifacts(workspace, relative, "any", expected_source_sha256="wrong") is None


def _compiled_package(root: Path) -> Path:
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


def test_manager_reports_runner_not_built_then_uses_registered_artifact(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "runner-manager")
    package = _compiled_package(tmp_path)
    nested = workspace.publish_runner(package, name="group/compiled")

    def pin_nested(job) -> None:
        document = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
        document["runner"]["path"] = nested["path"]
        document["runner"]["sha256"] = nested["sha256"]
        (job.payload / "job.json").write_text(json.dumps(document), encoding="utf-8")

    first = new_job(workspace, package)
    pin_nested(first)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(first.job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "runner_not_built"
    recovery = str(failure["message"]).split("run: ", 1)[1]
    recovery_argv = shlex.split(recovery)
    assert recovery_argv[:7] == [
        "httk",
        "workflow",
        "build",
        "--workspace",
        workspace_name,
        "--store",
        "group/compiled",
    ]
    assert command(["build", *recovery_argv[3:]], context) == 0
    second = new_job(workspace, package)
    pin_nested(second)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(second.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert (workspace.payload_path(marker.placement, marker.job_key) / "run" / "used-artifact").read_text() == "yes"


def test_build_and_attempt_environments_carry_only_the_languages_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An external package finds the installed language SDKs at build and at run time."""

    monkeypatch.setenv("HTTK_WORKFLOW_JOB_DIR", "/stray")
    workspace = Workspace.initialize(tmp_path / "workspace")
    package = _compiled_package(tmp_path)
    _script(
        package / "build.sh",
        "mkdir -p build; cp runner.py build/runner.py; chmod +x build/runner.py\n"
        'printf %s "$HTTK_WORKFLOW_LANGUAGES_DIR" > build/languages-dir\n'
        'printf %s "${HTTK_WORKFLOW_JOB_DIR-unset}" > build/stray',
    )
    job = new_job(workspace, package)
    relative = PurePosixPath(str(job.runner["path"]))
    artifacts = register_build(
        workspace,
        workspace.runner_store_path(relative),
        relative,
        BuildSpec("./build.sh", ("build",)),
        source_sha256=str(job.runner["sha256"]),
    )
    assert (artifacts / "build" / "stray").read_text(encoding="utf-8") == "unset"
    languages = Path((artifacts / "build" / "languages-dir").read_text(encoding="utf-8"))
    assert languages.is_absolute() and (languages / "c" / "httk_workflow.h").is_file()

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "run"
    assert Path((workdir / "languages-dir").read_text(encoding="utf-8")) == languages


def test_workflow_build_prefers_in_workspace_packages_and_resolves_job_globs(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-selector")
    package = _package(
        workspace.root / "package",
        '[workflow]\nname = "tests.build.selector"\n[workflow.runner]\nsteps = ["start"]\n'
        '[workflow.build]\ncommand = "./build.sh"\nartifacts = ["out"]\n',
    )
    (package / "build.sh").write_text("#!/bin/sh\nmkdir -p out\nprintf artifact > out/result\n", encoding="utf-8")
    (package / "build.sh").chmod(0o755)

    assert command(["build", "--workspace", name, str(package)], context) == 0
    capsys.readouterr()
    first = new_job(workspace, package, tag="silicon-one")
    assert command(["build", "--workspace", name, f"{workspace.root}/jobs/silicon-one*"], context) == 0
    capsys.readouterr()
    second = new_job(workspace, package, tag="silicon-two")
    assert first.job_id != second.job_id
    assert command(["build", "--workspace", name, f"{workspace.root}/jobs/silicon*"], context) == 1
    assert "one job" in capsys.readouterr().err


def test_in_workspace_package_errors_remain_package_specific(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-package-errors")
    malformed = _package(workspace.root / "malformed")
    (malformed / "httk_workflow.toml").write_text("[workflow\n", encoding="utf-8")
    buildless = _package(workspace.root / "buildless")

    assert command(["build", "--workspace", name, str(malformed)], context) == 1
    assert "target package is malformed" in capsys.readouterr().err
    assert command(["build", "--workspace", name, str(buildless)], context) == 1
    assert "has no [workflow.build] section" in capsys.readouterr().err


def _named_package(root: Path, workflow_id: str, *, build: bool = True) -> Path:
    build_section = '[workflow.build]\ncommand = "./build.sh"\nartifacts = ["out"]\n' if build else ""
    package = _package(
        root,
        f'[workflow]\nname = "{workflow_id}"\n[workflow.runner]\nsteps = ["start"]\n{build_section}',
    )
    _script(package / "build.sh", "mkdir -p out; printf artifact > out/result")
    return package


def test_workflow_build_accepts_a_registered_workflow_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capfd
) -> None:
    """Building by name registers exactly the tree a job selecting that name pins."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-by-name")
    package = _named_package(tmp_path / "named", "tests.build.named")
    provider = load_workflow_package(package, register=False)
    monkeypatch.setitem(_WORKFLOW_PROVIDERS, provider.workflow_id, provider)

    assert command(["build", "--workspace", name, "tests.build.named"], context) == 0
    output = capfd.readouterr().out
    assert "tests.build.named:" in output and f"package: {package.resolve()}" in output
    assert "registered:" in output

    job = new_job(workspace, "tests.build.named")
    relative = PurePosixPath(str(job.runner["path"]))
    tag = platform_tag(BuildSpec("./build.sh", ("out",)))
    artifacts = registered_artifacts(workspace, relative, tag, expected_source_sha256=str(job.runner["sha256"]))
    assert artifacts is not None and (artifacts / "out" / "result").read_text(encoding="utf-8") == "artifact"

    assert command(["build", "--workspace", name, "--json", "tests.build.named"], context) == 0
    (row,) = json.loads(capfd.readouterr().out)
    assert row["target"] == "tests.build.named" and row["workflow"] == "tests.build.named"
    assert row["package"] == str(package.resolve()) and row["status"] == "registered"
    assert row["store"] == relative.as_posix()


def test_workflow_build_accepts_an_installed_plugin_workflow_name(tmp_path: Path, capfd) -> None:
    from test_plugin_workflows import _plugin

    install_plugin(_plugin(tmp_path / "plugin", "plugin-build-name", [("test.plugin.buildname", "pbn", True)]))
    _reset_plugin_workflow_cache()
    try:
        workspace = Workspace.initialize(tmp_path / "workspace")
        context = CLIContext("httk", tmp_path)
        name = register_ws(context, workspace.root, "build-plugin-name")
        assert command(["build", "--workspace", name, "--json", "pbn"], context) == 0
        (row,) = json.loads(capfd.readouterr().out)
        assert row["workflow"] == "test.plugin.buildname" and row["status"] == "registered"
        assert (Path(str(row["artifacts"])) / "build" / "run").is_file()
        job = new_job(workspace, "test.plugin.buildname")
        assert job.runner["path"] == row["store"]
    finally:
        _reset_plugin_workflow_cache()


def test_workflow_build_by_name_reports_unknown_buildless_and_packaged_workflows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relax_workflow: object, capfd
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-name-errors")

    assert command(["build", "--workspace", name, "tests.build.nosuch"], context) == 1
    err = capfd.readouterr().err
    assert "no workflow package, workflow name, or workspace runner matches 'tests.build.nosuch'" in err

    buildless = load_workflow_package(
        _named_package(tmp_path / "plain", "tests.build.plain", build=False), register=False
    )
    monkeypatch.setitem(_WORKFLOW_PROVIDERS, buildless.workflow_id, buildless)
    assert command(["build", "--workspace", name, "tests.build.plain"], context) == 0
    assert "nothing to build" in capfd.readouterr().out
    assert command(["build", "--workspace", name, "--json", "tests.build.plain"], context) == 0
    (row,) = json.loads(capfd.readouterr().out)
    assert row["status"] == "nothing-to-build" and "artifacts" not in row

    assert command(["build", "--workspace", name, "test-relax"], context) == 1
    assert "is not a directory workflow package" in capfd.readouterr().err


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not available")
def test_workflow_build_accepts_a_git_uri_and_its_installed_short_name(tmp_path: Path, capfd) -> None:
    from test_git_workflows import _git

    repository = _named_package(tmp_path / "repo", "tests.build.gitflow")
    _git(repository, "init", "-q", "-b", "main")
    _git(repository, "add", "-A")
    _git(repository, "commit", "-q", "-m", "one")
    git_workflows._reset_fetched_workflow_cache()
    try:
        workspace = Workspace.initialize(tmp_path / "workspace")
        context = CLIContext("httk", tmp_path)
        name = register_ws(context, workspace.root, "build-git")
        uri = f"git+file://{repository}"
        assert command(["build", "--workspace", name, "--json", uri], context) == 0
        (by_uri,) = json.loads(capfd.readouterr().out)
        assert by_uri["target"] == uri and by_uri["status"] == "registered"
        assert str(by_uri["workflow"]).startswith(f"{uri}@")
        assert command(["build", "--workspace", name, "--json", "tests.build.gitflow"], context) == 0
        (by_name,) = json.loads(capfd.readouterr().out)
        assert by_name["store"] == by_uri["store"] and by_name["package"] == by_uri["package"]
    finally:
        git_workflows._reset_fetched_workflow_cache()
