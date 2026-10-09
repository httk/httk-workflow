"""Read-only readiness findings for pending jobs on the filesystem kernel.

A precheck asks, without running anything, whether a job can make progress
here: is its workflow (and every workflow it calls) installed and built, does
its environment resolve, could a registered manager claim it, are its declared
inputs present and is its next step one the runner implements. Transfer-time
advisories are tested in ``test_precheck_transfer.py``.
"""

from pathlib import Path
from typing import cast

import pytest

import v3_helpers as v3
from httk.workflow import TaskManager, _store
from httk.workflow import precheck as precheck_module
from test_workflow_packages import _jobflow_package

_BUILD = '\n[workflow.build]\ncommand = "./build.sh"\nartifacts = ["out"]\n'
_BUILD_SCRIPT = {"build.sh": "#!/bin/sh\nmkdir out\nprintf artifact > out/result\n"}


def _one(workspace: v3.Workspace, **options: object) -> dict[str, object]:
    findings = list(precheck_module.precheck_jobs(workspace, **options))  # type: ignore[arg-type]
    assert len(findings) == 1
    return findings[0]


def test_precheck_flags_a_step_outside_the_recorded_runner_steps(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    installed = v3.install(ws, tmp_path / "package")
    v3.submit(ws, installed, {"start": "fail"}, parameters={"runner_steps": ["only", "other"]})
    v3.run(ws)

    finding = _one(ws, states=("failed",))
    assert isinstance(finding["step"], str)
    assert "'start'" in finding["step"] and "only, other" in finding["step"]
    assert precheck_module.has_step_problem(finding)
    # A job whose runner recorded no steps is never faulted.
    v3.submit(ws, installed, {"start": "succeed"})
    assert _one(ws)["step"] is None


def test_precheck_reports_an_uninstalled_workflow(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    v3.submit(ws, ("local:absent", "absent"), {"start": "succeed"})

    finding = _one(ws)
    assert precheck_module.has_runner_problem(finding)
    runner = finding["runner"]
    assert isinstance(runner, dict) and "local:absent is not installed" in str(runner["problem"])


def test_precheck_reports_an_unbuilt_package_and_clears_after_building(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    installed = v3.install(ws, tmp_path / "package", manifest=_BUILD, executables=_BUILD_SCRIPT, build=False)
    v3.submit(ws, installed, {"start": "succeed"})

    finding = _one(ws)
    runner = finding["runner"]
    assert isinstance(runner, dict) and runner["status"] == "problem"
    assert "not built for this platform" in str(runner["problem"])
    with v3.cli_owner(ws) as owner:
        _store.build(ws, owner, installed.id)
    assert _one(ws)["runner"] == {"status": "ok", "ok": True}


def test_precheck_reports_an_uninstalled_called_workflow(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    package = tmp_path / "package"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        '[workflow]\nname = "caller"\n\n[workflow.runner]\nsteps = ["start"]\n\n'
        '[workflow.calls]\nhelper = "local:helper"\n',
        encoding="utf-8",
    )
    (package / "run").write_text(v3.RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    with v3.cli_owner(ws) as owner:
        installed = _store.install(ws, owner, package, calls=False)
    v3.submit(ws, installed, {"start": "succeed"})

    finding = _one(ws)
    assert finding["runner"] == {"status": "ok", "ok": True}
    assert precheck_module.has_call_problem(finding)
    assert finding["calls"] == [
        "called workflow local:helper is not installed in this workspace; run 'httk workflow install'"
    ]


def test_precheck_reports_environment_resolution_sources(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    ws.set_setting("tool.value", "from workspace")
    environments = (
        {"declared": {"value": {"type": "string", "setting": "tool.value"}}, "overrides": {}},
        {"declared": {"fallback": {"type": "string", "default": "default"}}, "overrides": {}},
        {"declared": {"missing": {"type": "string", "setting": "tool.missing"}}, "overrides": {}},
    )
    for environment in environments:
        v3.submit(ws, ("local:demo", "demo"), {"start": "succeed"}, environment=environment)

    findings = list(precheck_module.precheck_jobs(ws))
    entries = [cast(list[dict[str, object]], finding["environment"])[0] for finding in findings]
    statuses = sorted((str(entry["status"]), str(entry["source"])) for entry in entries)
    assert statuses == [("default", "default"), ("resolved", "workspace-setting"), ("unresolved", "None")]
    assert sum(precheck_module.has_environment_problem(finding) for finding in findings) == 1


def test_precheck_flags_a_job_no_live_manager_can_claim(tmp_path: Path) -> None:
    """A capability no registered manager offers is a claim problem naming it."""

    ws = v3.workspace(tmp_path / "workspace")
    v3.submit(ws, ("local:demo", "demo"), {"start": "succeed"}, capabilities=("docker",))

    # With no manager registered, claimability is one workspace notice, not a per-job problem.
    assert _one(ws)["claim"] is None
    notice = precheck_module.manager_availability_notice(ws)
    assert notice is not None and "no manager" in notice
    with TaskManager(ws, capabilities=("gpu",)):
        finding = _one(ws)
        assert precheck_module.manager_availability_notice(ws) is None
    assert precheck_module.has_claim_problem(finding)
    claim = finding["claim"]
    assert isinstance(claim, dict) and "lacks capabilities docker" in str(claim["problem"])


def test_precheck_flags_a_missing_language_engine_unless_a_manager_may_have_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A built-in realization whose engine this process lacks is a problem, or indeterminate under a manager."""

    ws = v3.workspace(tmp_path / "workspace")
    with v3.cli_owner(ws) as owner:
        package = _jobflow_package(tmp_path / "package", 'maker = "atomate2.vasp.flows.core:DoubleRelaxMaker"')
        installed = _store.install(ws, owner, package)
    v3.submit(ws, installed, {"start": "succeed"})
    monkeypatch.setattr(
        precheck_module, "_find_module_spec_without_import", lambda module: None if module == "maggma" else object()
    )

    language = _one(ws)["language"]
    assert isinstance(language, dict) and language["status"] == "problem"
    assert "maggma" in str(language["problem"]) and "pip install httk-workflow[jobflow]" in str(language["problem"])
    with TaskManager(ws):
        language = _one(ws)["language"]
    assert isinstance(language, dict) and language["status"] == "indeterminate"
    assert "verified only at run time" in str(language["problem"])


def test_a_native_workflow_has_no_language_finding(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    installed = v3.install(ws, tmp_path / "package")
    # The open parameters channel alone does not make a language job.
    v3.submit(ws, installed, {"start": "succeed"}, parameters={"workflow_language": "jobflow"})
    assert _one(ws)["language"] is None


def test_precheck_flags_unmet_installed_requirements(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    installed = v3.install(ws, tmp_path / "package", workflow='requires = ["httk-no-such-distribution>=1"]')
    v3.submit(ws, installed, {"start": "succeed"})

    requirements = _one(ws)["requirements"]
    assert isinstance(requirements, dict) and requirements["status"] == "problem"
    assert "httk-no-such-distribution" in str(requirements["problem"])
    with TaskManager(ws):
        requirements = _one(ws)["requirements"]
    assert isinstance(requirements, dict) and requirements["status"] == "indeterminate"


def test_precheck_flags_a_missing_required_input_destination(tmp_path: Path) -> None:
    """A declared required input absent from the payload is an input problem."""

    ws = v3.workspace(tmp_path / "workspace")
    declared = {"inputs": {"structure": {"required": True, "destination": "files/POSCAR"}}}
    v3.submit(ws, ("local:demo", "demo"), {"start": "succeed"}, declared=declared)
    v3.submit(ws, ("local:demo", "demo"), {"start": "succeed"}, declared=declared, members={"files/POSCAR": "Si\n"})

    problems = [cast(list[str], finding["inputs"]) for finding in precheck_module.precheck_jobs(ws)]
    assert sorted(problems, key=len) == [
        [],
        ["required input 'structure' is missing its staged destination files/POSCAR"],
    ]


def test_precheck_selects_states_and_placements(tmp_path: Path) -> None:
    ws = v3.workspace(tmp_path / "workspace")
    v3.submit(ws, ("local:demo", "demo"), {"start": "succeed"}, placement="a/b")
    v3.submit(ws, ("local:demo", "demo"), {"start": "succeed"}, placement="c")

    assert [finding["placement"] for finding in precheck_module.precheck_jobs(ws, placement="a")] == ["a/b"]
    assert list(precheck_module.precheck_jobs(ws, states=("failed",))) == []
    with pytest.raises(ValueError, match="unknown precheck state: submitted"):
        list(precheck_module.precheck_jobs(ws, states=("submitted",)))


def test_precheck_finds_package_module_without_executing_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Package discovery does not import a parent package."""

    package = tmp_path / "sentinel_parent"
    package.mkdir()
    sentinel = tmp_path / "executed"
    (package / "__init__.py").write_text(
        f"from pathlib import Path\nPath({str(sentinel)!r}).touch()\n", encoding="utf-8"
    )
    (package / "child.py").write_text("runner = True\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    spec = precheck_module._find_module_spec_without_import("sentinel_parent.child")
    assert spec is not None and spec.origin is not None
    assert not sentinel.exists()
