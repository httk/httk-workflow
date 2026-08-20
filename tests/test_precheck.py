"""Readiness reports and transfer-time environment advisories."""

import json
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest
from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow import precheck as precheck_module
from httk.workflow._runner_builds import register_build
from httk.workflow.models import QUIESCENT_KINDS
from httk.workflow.projects import initialize_project
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload
from httk.workflow.scaffold import BuildSpec
from httk.workflow.transfers import offer_transfers, select_transfer_jobs
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command


def _job(root: Path, name: str, environment: Mapping[str, object], *, runner: str = "payload") -> Path:
    """Prepare one minimal job payload."""

    payload = root / name
    (payload / "files").mkdir(parents=True)
    run = payload / "files" / "run"
    run.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    run.chmod(0o755)
    prepare_job_payload(
        payload,
        JobSpec(
            name=name,
            workflow="tests.precheck",
            runner_path="files/run" if runner == "payload" else runner,
            environment=environment,
        ),
    )
    return payload


def _compiled_runner_job(
    tmp_path: Path, *, platform: str | None = None, malformed: bool = False
) -> tuple[Workspace, Path, str]:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source = tmp_path / "runner"
    source.mkdir()
    (source / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (source / "run").chmod(0o755)
    manifest = (
        "[workflow]\nid = 'compiled'\n[workflow.runner]\nsteps = ['start']\n"
        "[workflow.build]\ncommand = './build.sh'\nartifacts = ['out']\n"
    )
    if platform is not None:
        manifest = manifest.replace("artifacts = ['out']", f"platform = {platform!r}\nartifacts = ['out']")
    if malformed:
        manifest = "[workflow\n"
    (source / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (source / "build.sh").write_text("#!/bin/sh\nmkdir out\nprintf artifact > out/result\n", encoding="utf-8")
    (source / "build.sh").chmod(0o755)
    target = workspace.runners / "compiled"
    target.parent.mkdir(parents=True, exist_ok=True)
    import shutil

    shutil.copytree(source, target)
    for entry in (target, *target.rglob("*")):
        entry.chmod(0o555)
    digest = tree_digest(target)
    payload = _job(tmp_path / "payload", "compiled-job", {})
    definition = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    definition["runner"] = {"source": "workspace", "path": "compiled", "sha256": digest}
    (payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
    workspace.submit(payload, "ready")
    return workspace, target, digest


def _runner_finding(finding: dict[str, object]) -> Mapping[str, object]:
    runner = finding["runner"]
    assert isinstance(runner, Mapping)
    return runner


def test_precheck_flags_a_step_outside_the_recorded_runner_steps(tmp_path: Path) -> None:
    from httk.workflow.models import StateFrame

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _job(tmp_path / "src", "job", {})
    submitted = workspace.submit(payload, "project/steps")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        state = StateFrame.from_mapping(workspace.read_state(submitted))
        manager._transition(
            submitted,
            "paused",
            StateFrame.replace(state.carried(), step="bogus", runner_steps=["only", "other"], reason="paused"),
        )
    findings = list(precheck_module.precheck_jobs(workspace))
    step_problems = [str(finding["step"]) for finding in findings if finding["step"]]
    assert step_problems and "bogus" in step_problems[0]
    assert "only, other" in step_problems[0]


def test_precheck_reports_an_unbuilt_platformless_package_and_clears_after_registration(tmp_path: Path) -> None:
    workspace, runner, digest = _compiled_runner_job(tmp_path)
    finding = next(precheck_module.precheck_jobs(workspace))
    runner_finding = _runner_finding(finding)
    assert runner_finding["status"] == "problem"
    assert isinstance(runner_finding["problem"], str)
    assert "httk workflow build --by-path" in runner_finding["problem"]
    assert "--store compiled" in runner_finding["problem"]
    register_build(
        workspace, runner, PurePosixPath("compiled"), BuildSpec("./build.sh", ("out",)), source_sha256=digest
    )
    finding = next(precheck_module.precheck_jobs(workspace))
    assert _runner_finding(finding)["status"] == "ok"


def test_precheck_does_not_probe_platform_specific_packages(tmp_path: Path) -> None:
    counter = tmp_path / "probed"
    probe = tmp_path / "probe.sh"
    probe.write_text(f"#!/bin/sh\ntouch {counter}\nprintf linux\n", encoding="utf-8")
    probe.chmod(0o755)
    workspace, _, _ = _compiled_runner_job(tmp_path, platform=probe.as_posix())
    finding = next(precheck_module.precheck_jobs(workspace))
    assert _runner_finding(finding) == {
        "status": "indeterminate",
        "problem": "declares platform-specific builds; registration is checked at manager start — run: "
        f"httk workflow build --by-path {workspace.root} --store compiled",
    }
    assert not counter.exists()


def test_precheck_reports_malformed_build_manifest(tmp_path: Path) -> None:
    workspace, _, _ = _compiled_runner_job(tmp_path, malformed=True)
    finding = next(precheck_module.precheck_jobs(workspace))
    runner_finding = _runner_finding(finding)
    assert runner_finding["status"] == "problem"
    assert isinstance(runner_finding["problem"], str)
    assert "manifest is malformed" in runner_finding["problem"]


def test_precheck_existing_runner_problem_wins_over_build_lookup(tmp_path: Path) -> None:
    workspace, runner, _ = _compiled_runner_job(tmp_path)
    runner.chmod(0o755)
    (runner / "run").chmod(0o755)
    (runner / "run").unlink()
    finding = next(precheck_module.precheck_jobs(workspace))
    runner_finding = _runner_finding(finding)
    assert runner_finding["status"] == "problem"
    assert isinstance(runner_finding["problem"], str)
    assert "has no run entry point" in runner_finding["problem"]


def test_precheck_reports_resolution_sources_and_exit_code(tmp_path: Path, capsys) -> None:
    """The CLI reports setting, default, and unresolved entries in JSON."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "precheck")
    workspace.set_setting("tool.value", "from workspace")
    environments = (
        {"declared": {"value": {"type": "string", "setting": "tool.value"}}, "overrides": {}},
        {"declared": {"fallback": {"type": "string", "default": "default"}}, "overrides": {}},
        {"declared": {"missing": {"type": "string", "setting": "tool.missing"}}, "overrides": {}},
    )
    for index, environment in enumerate(environments):
        workspace.submit(_job(tmp_path, f"job-{index}", environment), "ready")

    assert command(["precheck", name, "--json"], context) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["format"] == "httk-workflow-precheck"
    assert report["summary"] == {
        "checked": 3,
        "runner_indeterminate": 0,
        "runner_problems": 0,
        "unresolved": 1,
        "claim_problems": 0,
        "language_problems": 0,
        "language_indeterminate": 0,
        "input_problems": 0,
        "step_problems": 0,
    }
    statuses = sorted(
        (entry["environment"][0]["status"], entry["environment"][0]["source"]) for entry in report["jobs"]
    )
    assert statuses == [("default", "default"), ("resolved", "workspace-setting"), ("unresolved", None)]
    assert "environment_variable_caveat" in report


def _rewrite_job(payload: Path, **members: object) -> None:
    """Overwrite named members of a prepared job.json."""

    definition = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    definition.update(members)
    (payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")


def test_precheck_flags_a_job_no_live_manager_can_claim(tmp_path: Path, capsys) -> None:
    """A capability no live manager offers is a claim problem naming it (item 1)."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _job(tmp_path, "gated", {"declared": {}, "overrides": {}})
    _rewrite_job(payload, claim={"pool": "default", "required_capabilities": ["docker"]})
    workspace.submit(payload, "ready")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "claim")

    with TaskManager(workspace, heartbeat_interval=0.01):
        assert command(["precheck", name, "--json"], context) == 1
        report = json.loads(capsys.readouterr().out)
    assert report["summary"]["claim_problems"] == 1
    assert "lacks capabilities docker" in report["jobs"][0]["claim"]["problem"]


def test_precheck_uses_the_actual_manager_runner_module_allowlist(tmp_path: Path, capsys) -> None:
    """A pkg module the live manager's real allowlist excludes is refused (item 2)."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _job(tmp_path, "modules", {"declared": {}, "overrides": {}})
    # httk.workflow would pass the DEFAULT precheck allowlist; the actual manager
    # publishes ('acme.runners',), so it is the manager's allowlist that decides.
    _rewrite_job(
        payload,
        runner={
            "executor": "path",
            "source": "installed",
            "path": "pkg:httk.workflow/precheck.py",
            "sha256": "0" * 64,
            "arguments": [],
        },
    )
    workspace.submit(payload, "ready")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "modules")

    with TaskManager(workspace, runner_modules=("acme.runners",), heartbeat_interval=0.01):
        assert command(["precheck", name, "--json"], context) == 1
        report = json.loads(capsys.readouterr().out)
    assert report["summary"]["claim_problems"] == 1
    problem = report["jobs"][0]["claim"]["problem"]
    assert "does not allow runner module httk.workflow" in problem and "acme.runners" in problem


def _language_job(tmp_path: Path, name: str) -> Path:
    """Prepare a jobflow language job (the pair the collect gate recognizes)."""

    payload = _job(tmp_path, name, {"declared": {}, "overrides": {}})
    _rewrite_job(payload, parameters={"workflow_realization": "language", "workflow_language": "jobflow"})
    return payload


def test_precheck_flags_a_missing_language_engine_when_nothing_serves_it(tmp_path: Path, capsys, monkeypatch) -> None:
    """A jobflow job whose engine is absent and unserved is a problem (item 3)."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.submit(_language_job(tmp_path, "jobflow"), "ready")
    monkeypatch.setattr(
        precheck_module,
        "_find_module_spec_without_import",
        lambda module: None if module == "maggma" else object(),
    )
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "jobflow")

    assert command(["precheck", name, "--json"], context) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["summary"]["language_problems"] == 1
    problem = report["jobs"][0]["language"]["problem"]
    assert "maggma" in problem
    assert "pip install httk-workflow[jobflow]" in problem
    assert "pymatgen" in problem


def test_precheck_language_engine_is_indeterminate_when_a_manager_serves_it(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """The check reads this process, so a serving manager's env makes it indeterminate, not a failure."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.submit(_language_job(tmp_path, "jobflow-served"), "ready")
    monkeypatch.setattr(
        precheck_module,
        "_find_module_spec_without_import",
        lambda module: None if module == "maggma" else object(),
    )
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "jobflow-served")

    with TaskManager(workspace, heartbeat_interval=0.01):
        assert command(["precheck", name, "--json"], context) == 0
        report = json.loads(capsys.readouterr().out)
    assert report["summary"]["language_problems"] == 0
    assert report["summary"]["language_indeterminate"] == 1
    assert report["jobs"][0]["language"]["status"] == "indeterminate"
    assert "verified only at run time" in report["jobs"][0]["language"]["problem"]


def test_precheck_ignores_a_bare_workflow_language_parameter(tmp_path: Path, capsys) -> None:
    """The open parameters channel alone is not a language job; the realization must say so."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _job(tmp_path, "not-language", {"declared": {}, "overrides": {}})
    _rewrite_job(payload, parameters={"workflow_language": "jobflow"})
    workspace.submit(payload, "ready")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "not-language")

    assert command(["precheck", name, "--json"], context) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["jobs"][0]["language"] is None


def test_precheck_treats_an_empty_manager_allowlist_as_empty(tmp_path: Path, capsys) -> None:
    """An explicit empty runner_modules allows nothing, not the default (finding 7)."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _job(tmp_path, "empty-allowlist", {"declared": {}, "overrides": {}})
    _rewrite_job(
        payload,
        runner={
            "executor": "path",
            "source": "installed",
            "path": "pkg:httk.workflow/precheck.py",
            "sha256": "0" * 64,
            "arguments": [],
        },
    )
    workspace.submit(payload, "ready")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "empty-allowlist")

    with TaskManager(workspace, runner_modules=[], heartbeat_interval=0.01):
        assert command(["precheck", name, "--json"], context) == 1
        report = json.loads(capsys.readouterr().out)
    assert report["summary"]["claim_problems"] == 1
    problem = report["jobs"][0]["claim"]["problem"]
    assert "does not allow runner module httk.workflow" in problem and "allows none" in problem


def test_precheck_flags_a_missing_required_input_destination(tmp_path: Path, capsys) -> None:
    """A declared required input absent from the payload is an input problem (item 4)."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _job(tmp_path, "inputs", {"declared": {}, "overrides": {}})
    _rewrite_job(payload, declared={"inputs": {"structure": {"required": True, "destination": "files/POSCAR"}}})
    workspace.submit(payload, "ready")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "inputs")

    assert command(["precheck", name, "--json"], context) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["summary"]["input_problems"] == 1
    assert "files/POSCAR" in report["jobs"][0]["inputs"][0]


def test_precheck_notice_when_no_manager_has_registered(tmp_path: Path, capsys) -> None:
    """With no manager registered, claimability is one workspace notice, not per-job spam."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(2):
        workspace.submit(_job(tmp_path, f"orphan-{index}", {"declared": {}, "overrides": {}}), "ready")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "orphan")

    assert command(["precheck", name, "--json"], context) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["summary"]["claim_problems"] == 0
    assert report["manager_notice"] is not None and "no manager" in report["manager_notice"]
    assert all(finding["claim"] is None for finding in report["jobs"])


def test_precheck_flags_a_deleted_workspace_runner(tmp_path: Path, capsys) -> None:
    """A missing pinned workspace runner is a runner problem."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    source = tmp_path / "runner.py"
    source.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    reference = workspace.publish_runner(source, name="test/runner")
    payload = _job(tmp_path, "job", {"declared": {}, "overrides": {}})
    # Rebuild the payload's runner member with the shared reference.
    definition = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    definition["runner"] = {**reference, "executor": "path", "arguments": []}
    (payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
    workspace.submit(payload, "ready")
    workspace.runner_store_path("test/runner").unlink()
    name = register_ws(CLIContext("httk", tmp_path), workspace.root, "runner-check")

    assert command(["precheck", name, "--json"], CLIContext("httk", tmp_path)) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["summary"]["runner_problems"] == 1
    assert "not published" in report["jobs"][0]["runner"]["problem"]


def test_local_transfer_warns_and_strict_mode_moves_nothing(tmp_path: Path, capsys) -> None:
    """The destination environment is checked before local transfer detach."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    context = CLIContext("httk", tmp_path)
    source_name = register_ws(context, source.root, "source")
    destination_name = register_ws(context, destination.root, "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "transfer-job",
            {"declared": {"missing": {"type": "string"}}, "overrides": {}},
        ),
        "ready",
    )

    assert (
        command(
            ["transfer", source_name, destination_name, "--job", marker.job_id, "--strict-environment"],
            context,
        )
        == 2
    )
    capsys.readouterr()
    assert source.find_marker_by_id(marker.job_id) is not None
    assert destination.find_marker_by_id(marker.job_id) is None

    assert command(["transfer", source_name, destination_name, "--job", marker.job_id], context) == 0
    warning = capsys.readouterr().err
    assert "destination environment unresolved" in warning
    assert destination.find_marker_by_id(marker.job_id) is not None


def test_transfer_does_not_use_the_client_environment_for_destination_resolution(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """A client override cannot hide a missing destination setting."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    context = CLIContext("httk", tmp_path)
    source_name = register_ws(context, source.root, "source")
    destination_name = register_ws(context, destination.root, "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "client-environment-job",
            {"declared": {"command": {"type": "string", "setting": "tool.command"}}, "overrides": {}},
        ),
        "ready",
    )
    monkeypatch.setenv("HTTK_TOOL_COMMAND", "client-only")

    assert (
        command(
            ["transfer", source_name, destination_name, "--job", marker.job_id, "--strict-environment"],
            context,
        )
        == 2
    )
    assert "destination environment unresolved" in capsys.readouterr().err
    assert destination.find_marker_by_id(marker.job_id) is None


def test_precheck_plain_installed_runner_without_path_is_indeterminate(tmp_path: Path, capsys) -> None:
    """An unconfigured plain installed runner is not falsely called broken."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(
        _job(
            tmp_path,
            "installed-job",
            {"declared": {}, "overrides": {}},
        ),
        "ready",
    )
    # The helper creates a payload runner; replace only the reference with a
    # valid installed-runner shape for this indeterminate-path check.
    definition_path = workspace.payload_path(marker.placement, marker.job_key) / "job.json"
    definition = json.loads(definition_path.read_text(encoding="utf-8"))
    definition["runner"] = {
        "executor": "path",
        "source": "installed",
        "path": "plain-installed-runner",
        "sha256": "0" * 64,
        "arguments": [],
    }
    definition_path.write_text(json.dumps(definition), encoding="utf-8")
    name = register_ws(CLIContext("httk", tmp_path), workspace.root, "installed")
    context = CLIContext("httk", tmp_path)
    assert command(["precheck", name, "--json"], context) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["jobs"][0]["runner"]["status"] == "indeterminate"
    assert report["summary"]["runner_indeterminate"] == 1


def test_remote_offer_forwards_successful_stderr(tmp_path: Path, capsys, monkeypatch) -> None:
    """Warnings emitted by the far-side offer remain visible to the caller."""

    target = SimpleNamespace(bundle=tmp_path / "adapter")
    monkeypatch.setattr(
        transfer_cli,
        "run_adapter",
        lambda *_args, **_kwargs: {
            "returncode": 0,
            "stdout": json.dumps({"format": "httk-workflow-transfer-offer", "format_version": 2, "offers": []}),
            "stderr": "warning: destination environment unresolved\n",
        },
    )
    assert (
        transfer_cli._remote_offer(
            target,
            "remote",
            "destination",
            states=None,
            placement=None,
            timeout=None,
        )
        == []
    )
    assert "destination environment unresolved" in capsys.readouterr().err


def test_precheck_finds_package_module_without_executing_parent(tmp_path: Path, monkeypatch) -> None:
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


def test_precheck_rejects_a_prefix_collision_package_runner(tmp_path: Path, capsys) -> None:
    """A module sharing the allowlist prefix is still rejected."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(
        _job(tmp_path, "prefix-collision", {"declared": {}, "overrides": {}}),
        "ready",
    )
    definition_path = workspace.payload_path(marker.placement, marker.job_key) / "job.json"
    definition = json.loads(definition_path.read_text(encoding="utf-8"))
    definition["runner"] = {
        "executor": "path",
        "source": "installed",
        "path": "pkg:httk.workflowevil/run",
        "sha256": "0" * 64,
        "arguments": [],
    }
    definition_path.write_text(json.dumps(definition), encoding="utf-8")
    name = register_ws(CLIContext("httk", tmp_path), workspace.root, "prefix-collision")

    assert command(["precheck", name, "--json"], CLIContext("httk", tmp_path)) == 1
    report = json.loads(capsys.readouterr().out)
    assert "allowlist" in report["jobs"][0]["runner"]["problem"]


def test_strict_transfer_checks_an_interrupted_marker_before_recovery(tmp_path: Path, monkeypatch) -> None:
    """Strict advisory leaves an interrupted source marker untouched."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "interrupted",
            {"declared": {"missing": {"type": "string"}}, "overrides": {}},
        ),
        "ready",
    )
    from httk.workflow import transfers as transfers_module

    real_seal = transfers_module._seal_transferring
    monkeypatch.setattr(
        transfers_module,
        "_seal_transferring",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupt")),
    )
    try:
        with pytest.raises(RuntimeError):
            source.detach(
                marker.job_id,
                destination_workspace_id=destination.workspace_id,
                destination_remote="cluster",
            )
    finally:
        monkeypatch.setattr(transfers_module, "_seal_transferring", real_seal)
    fenced = source.find_marker_by_id(marker.job_id)
    assert fenced is None
    assert [item.job_id for item in source.scan_markers(("transferring",))] == [marker.job_id]

    target = SimpleNamespace(name="cluster", bundle=tmp_path / "adapter")
    monkeypatch.setattr(
        transfer_cli,
        "_remote_workspace_probe",
        lambda *_args, **_kwargs: (destination.workspace_id, str(destination.root)),
    )
    with pytest.raises(ValueError, match="strict environment"):
        transfer_cli._send_jobs_to_remote(
            source,
            target,
            "destination",
            [marker.job_id],
            destination_placement=None,
            timeout=None,
            destination_settings={},
            strict_environment=True,
        )
    assert [item.job_id for item in source.scan_markers(("transferring",))] == [marker.job_id]
    assert not list((source.control / "transfers").glob("*.json"))


def test_other_destination_interrupted_marker_does_not_block_strict_advisory(tmp_path: Path, monkeypatch) -> None:
    """An interrupted transfer for another destination is not selected."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(
        _job(
            tmp_path,
            "other-destination",
            {"declared": {"missing": {"type": "string"}}, "overrides": {}},
        ),
        "ready",
    )
    from httk.workflow import transfers as transfers_module

    real_seal = transfers_module._seal_transferring
    monkeypatch.setattr(
        transfers_module,
        "_seal_transferring",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupt")),
    )
    try:
        with pytest.raises(RuntimeError):
            source.detach(
                marker.job_id,
                destination_workspace_id=destination.workspace_id,
                destination_remote="remote-a",
            )
    finally:
        monkeypatch.setattr(transfers_module, "_seal_transferring", real_seal)

    candidates = select_transfer_jobs(
        source,
        destination_workspace_id=destination.workspace_id,
        states=(*QUIESCENT_KINDS, "transferring"),
        job_ids=(marker.job_id,),
        destination_remote="remote-b",
        include_transferring=True,
    )
    assert candidates == []
    transfer_cli._environment_advisory(
        source,
        [marker.job_id],
        {},
        strict=True,
        candidates=candidates,
    )


def test_invalid_sealed_job_is_advisory_problem_but_remains_offerable(tmp_path: Path, capsys) -> None:
    """Strict mode blocks an invalid sealed job; non-strict mode reports it."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(_job(tmp_path, "invalid-sealed", {"declared": {}, "overrides": {}}), "ready")
    bundle = source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)
    (bundle / "job.json").write_text("{\"not\": \"a job\"}", encoding="utf-8")
    from httk.workflow import transfers as transfers_module

    manifest_path = bundle / transfers_module.TRANSFER_DIRECTORY / transfers_module.TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["payload_sha256"] = transfers_module._payload_digest(bundle)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    candidates = select_transfer_jobs(
        source,
        destination_workspace_id=destination.workspace_id,
        states=("submitted",),
    )
    assert candidates and candidates[0].problem
    with pytest.raises(ValueError, match="strict environment"):
        transfer_cli._environment_advisory(
            source,
            [marker.job_id],
            {},
            strict=True,
            candidates=candidates,
        )
    transfer_cli._environment_advisory(
        source,
        [marker.job_id],
        {},
        strict=False,
        candidates=candidates,
    )
    assert "destination environment unresolved" in capsys.readouterr().err
    assert offer_transfers(source, destination_workspace_id=destination.workspace_id, states=("submitted",))


def test_local_transfer_strict_checks_recovering_marker_before_recovery(tmp_path: Path, monkeypatch) -> None:
    """Local-to-local advisory uses the same transferring-marker selector."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(
        _job(tmp_path, "local-interrupted", {"declared": {"missing": {"type": "string"}}, "overrides": {}}),
        "ready",
    )
    from httk.workflow import transfers as transfers_module

    real_seal = transfers_module._seal_transferring
    monkeypatch.setattr(
        transfers_module,
        "_seal_transferring",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupt")),
    )
    try:
        with pytest.raises(RuntimeError):
            source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)
    finally:
        monkeypatch.setattr(transfers_module, "_seal_transferring", real_seal)
    with pytest.raises(ValueError, match="strict environment"):
        transfer_cli._transfer_local_to_local(
            source,
            destination,
            [marker.job_id],
            strict_environment=True,
        )
    assert [item.job_id for item in source.scan_markers(("transferring",))] == [marker.job_id]


def test_full_local_to_remote_unreachable_notice_is_printed_once(tmp_path: Path, capsys, monkeypatch) -> None:
    """The command-level unavailable destination path emits one notice."""

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    initialize_project(source_root, name="source")
    initialize_project(destination_root, name="destination")
    Workspace.initialize(source_root)
    Workspace.initialize(destination_root)
    from httk.workflow.adapters import add_remote

    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = str(destination_root)
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload = _job(tmp_path, "notice-job", {"declared": {}, "overrides": {}})
    marker = Workspace(source_root).submit(payload, "ready")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root, "home")
    register_ws(context, destination_root, "station")

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("settings unavailable")

    monkeypatch.setattr(transfer_cli, "_remote_workspace_settings", unavailable)
    monkeypatch.setattr(transfer_cli, "_send_jobs_to_remote", lambda *args, **kwargs: [])

    assert command(["transfer", "home", "cluster:station", "--job", marker.job_id], context) == 0
    assert capsys.readouterr().err.count("could not be prechecked remotely") == 1


def test_unreachable_destination_advisory_is_one_warning(tmp_path: Path, capsys) -> None:
    """The unit seam covers the adapter-unreachable branch without a network."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(
        _job(tmp_path, "unreachable-job", {"declared": {}, "overrides": {}}),
        "ready",
    )

    transfer_cli._environment_advisory(workspace, [marker.job_id], None, strict=False)
    assert capsys.readouterr().err.count("could not be prechecked remotely") == 1
