"""Registration and manager integration for compiled workflow runners."""

import hashlib
import json
import shlex
from pathlib import Path, PurePosixPath

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow._runner_builds import (
    platform_tag,
    register_build,
    registered_artifacts,
)
from httk.workflow.errors import RunnerResolutionError
from httk.workflow.scaffold import BuildSpec, new_job
from httk.workflow.workflow_cli import command


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
        "[workflow]\nid = 'compiled.test'\n[workflow.runner]\nsteps = ['start']\n"
        "[workflow.build]\ncommand = './build.sh'\nartifacts = ['build']\n",
        encoding="utf-8",
    )
    (package / "run").write_text("#!/bin/sh\nexec \"$(dirname \"$0\")/build/runner.py\"\n", encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "runner.py").write_text(
        "#!/usr/bin/env python3\n"
        "import json, os\n"
        "from pathlib import Path\n"
        "context = json.loads(Path(os.environ['HTTK_WORKFLOW_CONTEXT']).read_text())\n"
        "workdir = Path(os.environ['HTTK_WORKFLOW_WORKDIR'])\n"
        "(workdir / 'used-artifact').write_text('yes')\n"
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
    assert recovery_argv[:6] == ["httk", "workflow", "build", workspace_name, "--store", "group/compiled"]
    assert command(["build", *recovery_argv[3:]], context) == 0
    second = new_job(workspace, package)
    pin_nested(second)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(second.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert (workspace.payload_path(marker.placement, marker.job_key) / "run" / "used-artifact").read_text() == "yes"
