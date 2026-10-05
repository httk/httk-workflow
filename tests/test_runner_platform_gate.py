"""The manager never runs a runner tree's platform probe that no operator build already ran."""

import json
import shlex
import time
from pathlib import Path, PurePosixPath

import pytest
from httk.core import building
from httk.core.cli import CLIContext

from conftest import configure_identity, register_ws
from httk.workflow import TaskManager, Workspace, _runner_builds
from httk.workflow.errors import RunnerResolutionError
from httk.workflow.models import Marker
from httk.workflow.scaffold import new_job
from httk.workflow.workflow_cli import command
from test_runner_builds import _compiled_package


@pytest.fixture(autouse=True)
def _fresh_probe_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_runner_builds, "_PLATFORM_CACHE", {})


def _with_platform(tmp_path: Path, platform: str) -> Path:
    """A compiled workflow package whose build declares *platform* as its probe."""

    package = _compiled_package(tmp_path)
    manifest = package / "httk_workflow.toml"
    text = manifest.read_text(encoding="utf-8")
    assert "'" not in platform
    manifest.write_text(
        text.replace("[workflow.build]\n", f"[workflow.build]\nplatform = '{platform}'\n"), encoding="utf-8"
    )
    return package


def _run(workspace: Workspace, job_id: str) -> tuple[Marker, dict[str, object]]:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return marker, workspace.read_state(marker)


def test_an_imported_tree_with_a_hostile_probe_is_not_built_and_the_probe_never_runs(tmp_path: Path) -> None:
    configure_identity()
    evidence = tmp_path / "probe-ran"
    package = _with_platform(tmp_path, f"sh -c \"touch {evidence}\"")
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    job = new_job(source, package)
    bundle = source.detach(job.job_id, destination_workspace_id=destination.workspace_id)
    destination.import_bundle(bundle)

    marker, state = _run(destination, job.job_id)
    assert marker.kind == "failed"
    failure = state["failure"]
    assert isinstance(failure, dict) and failure["code"] == "runner_not_built"
    assert "httk workflow build" in str(failure["message"])
    assert not evidence.exists()


def test_a_registration_that_ran_the_same_probe_lets_resolution_proceed(tmp_path: Path) -> None:
    counter = tmp_path / "probe-count"
    package = _with_platform(tmp_path, f"sh -c \"printf x >> {counter}; echo gate-test\"")
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    register_ws(context, workspace.root, "platform-gate")

    first = new_job(workspace, package)
    marker, state = _run(workspace, first.job_id)
    failure = state["failure"]
    assert marker.kind == "failed" and isinstance(failure, dict) and failure["code"] == "runner_not_built"
    assert not counter.exists()

    # The operator's build runs the probe and records it in the stamp.
    recovery = shlex.split(str(failure["message"]).split("run: ", 1)[1])
    assert recovery[:3] == ["httk", "workflow", "build"]
    assert command(["build", *recovery[3:]], context) == 0
    assert counter.is_file()
    (stamp,) = _runner_builds.registered_platforms(workspace, PurePosixPath(str(first.runner["path"])))
    assert stamp["platform"] == f"sh -c \"printf x >> {counter}; echo gate-test\""
    assert stamp["source_sha256"] == first.runner["sha256"]

    second = new_job(workspace, package)
    marker, _state = _run(workspace, second.job_id)
    assert marker.kind == "succeeded"


def test_a_stamp_recording_another_probe_does_not_open_the_gate(tmp_path: Path) -> None:
    counter = tmp_path / "probe-count"
    package = _with_platform(tmp_path, f"sh -c \"printf x >> {counter}; echo gate-test\"")
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    register_ws(context, workspace.root, "platform-gate-other")
    job = new_job(workspace, package)
    relative = PurePosixPath(str(job.runner["path"]))
    assert command(["build", "--workspace", "platform-gate-other", "--store", relative.as_posix()], context) == 0
    ran = counter.read_text(encoding="utf-8")

    # The source digest pins the manifest, so only a stamp naming another
    # command can disagree with it; such a registration never ran this probe.
    (stamp_path,) = workspace.runner_builds.joinpath(*relative.parts).glob("*/gen-*/build.json")
    stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
    stamp["platform"] = "echo gate-test"
    stamp_path.write_text(json.dumps(stamp), encoding="utf-8")
    marker, state = _run(workspace, job.job_id)
    failure = state["failure"]
    assert marker.kind == "failed" and isinstance(failure, dict) and failure["code"] == "runner_not_built"
    assert counter.read_text(encoding="utf-8") == ran


def test_a_probe_that_outlives_the_timeout_fails_the_job_instead_of_hanging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    slow = tmp_path / "slow"
    platform = f"sh -c \"if [ -e {slow} ]; then sleep 60; fi; echo timed\""
    package = _with_platform(tmp_path, platform)
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    register_ws(context, workspace.root, "platform-gate-slow")
    job = new_job(workspace, package)
    relative = PurePosixPath(str(job.runner["path"]))
    assert command(["build", "--workspace", "platform-gate-slow", "--store", relative.as_posix()], context) == 0

    slow.touch()
    monkeypatch.setattr(_runner_builds, "_PLATFORM_CACHE", {})
    monkeypatch.setattr(_runner_builds, "_PLATFORM_PROBE_TIMEOUT", 0.5)
    started = time.monotonic()
    marker, state = _run(workspace, job.job_id)
    assert time.monotonic() - started < 30.0
    failure = state["failure"]
    assert marker.kind == "failed" and isinstance(failure, dict) and failure["code"] == "runner_not_built"
    assert "did not finish within 0.5 s" in str(failure["message"])


@pytest.mark.parametrize(
    "probe",
    [
        "echo x86_64",
        "echo 'Linux x86_64 glibc 2.39'",
        "printf 'gpu/a\\nsecond line\\n'",
        "printf ''",
        "printf .",
        "printf '%s' " + "x" * 80,
        "printf 'ünïcode tag'",
        "true",
    ],
)
def test_the_bounded_probe_derives_the_tag_core_derives(probe: str) -> None:
    """The local probe must name exactly the registration directory a build made."""

    expected = building.platform_tag(probe, env=_runner_builds._environment())[0]
    assert _runner_builds.platform_tag(building.BuildSpec("true", (), platform=probe)) == expected


@pytest.mark.parametrize("probe", ["sh -c 'echo broken >&2; exit 3'", "/nonexistent/platform-probe", "''"])
def test_the_bounded_probe_fails_as_core_fails(probe: str) -> None:
    with pytest.raises(building.BuildError) as core:
        building.platform_tag(probe, env=_runner_builds._environment())
    with pytest.raises(RunnerResolutionError) as local:
        _runner_builds.platform_tag(building.BuildSpec("true", (), platform=probe))
    assert (local.value.code, str(local.value)) == (core.value.code, core.value.message or str(core.value))
