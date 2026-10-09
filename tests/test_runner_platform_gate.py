"""The bounded platform probe of installed compiled packages.

Workflows never travel with jobs, so a probe only ever comes from a package an
operator installed; what remains to hold is that the manager's probe names
exactly the build directory a build registered, and that a hanging probe cannot
hang the manager.
"""

import time
from pathlib import Path

import pytest
from httk.core import building

from httk.workflow import TaskManager, _kernel, _store
from httk.workflow.errors import RunnerResolutionError
from httk.workflow.scaffold import new_job
from test_job_creation import install, workspace_at
from test_runner_builds import _compiled_package


@pytest.fixture(autouse=True)
def _fresh_probe_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_store, "_PLATFORM_CACHE", {})


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


@pytest.mark.slow
def test_a_job_runs_with_the_build_its_probe_names(tmp_path: Path) -> None:
    counter = tmp_path / "probe-count"
    ws = workspace_at(tmp_path / "workspace")
    installed = install(ws, _with_platform(tmp_path, f'sh -c "printf x >> {counter}; echo gate-test"'))
    assert len(list((installed.directory / "builds").iterdir())) == 1
    job = new_job(ws, installed.id)
    with TaskManager(ws) as manager:
        manager.run_until_idle(timeout=60)
    (done,) = _kernel.list_jobs(ws, "succeeded")
    assert done.job_id == job.job_id
    assert (done.path / "run" / "used-artifact").read_text() == "yes"


@pytest.mark.slow
def test_a_probe_that_outlives_the_timeout_leaves_the_job_waiting_instead_of_hanging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    slow = tmp_path / "slow"
    ws = workspace_at(tmp_path / "workspace")
    installed = install(ws, _with_platform(tmp_path, f'sh -c "if [ -e {slow} ]; then sleep 60; fi; echo timed"'))
    job = new_job(ws, installed.id)
    slow.touch()
    monkeypatch.setattr(_store, "_PLATFORM_CACHE", {})
    monkeypatch.setattr(_store, "_PLATFORM_PROBE_TIMEOUT", 0.5)
    started = time.monotonic()
    with TaskManager(ws) as manager:
        census = manager.run_until_idle(timeout=60)
    assert time.monotonic() - started < 30.0
    assert census.ready_claimable == 0
    (waiting,) = _kernel.list_jobs(ws, "ready")
    assert waiting.job_id == job.job_id


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

    expected = building.platform_tag(probe, env=_store._environment())[0]
    assert _store.platform_tag(building.BuildSpec("true", (), platform=probe)) == expected


@pytest.mark.parametrize("probe", ["sh -c 'echo broken >&2; exit 3'", "/nonexistent/platform-probe", "''"])
def test_the_bounded_probe_fails_as_core_fails(probe: str) -> None:
    with pytest.raises(building.BuildError) as core:
        building.platform_tag(probe, env=_store._environment())
    with pytest.raises(RunnerResolutionError) as local:
        _store.platform_tag(building.BuildSpec("true", (), platform=probe))
    assert (local.value.code, str(local.value)) == (core.value.code, core.value.message or str(core.value))
