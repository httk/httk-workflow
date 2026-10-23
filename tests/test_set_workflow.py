"""Switching the workflow existing jobs run: ``job request set_workflow`` and ``state.json`` ``workflow_override``."""

import json
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import ensure_identity_key, write_identity_config

import v3_helpers as v3
from conftest import fake_remote
from httk.workflow import Workspace, _kernel, _moving, _requests, _store, collect, removal
from httk.workflow._job import JobDefinition
from httk.workflow._state import StateDoc, _thaw
from httk.workflow.introspection import debug_job, describe_job, explain_job, render_job
from httk.workflow.precheck import precheck_jobs
from httk.workflow.projects import initialize_project
from httk.workflow.registry import WorkspaceBinding, create_workspace
from httk.workflow.removal import workflow_switch_problem
from httk.workflow.scaffold import new_job
from httk.workflow.sdk import Attempt
from httk.workflow.workflow_cli import _describe, command
from httk.workflow.workflow_cli import _job as job_cli

URI = "https://example.test/workflows/demo"
DECLARED = f'declaration_uri = "{URI}"'
#: The scriptable runner, also leaving the installation it was launched as (from the attempt context) in ``ran_by``.
MARKED = v3.RUNNER.replace(
    'step = context["step"]\n', 'step = context["step"]\npathlib.Path("ran_by").write_text(context["workflow"]["id"])\n'
)
#: A newer runner of the same workflow that fixes every step: it always succeeds.
FIXED = MARKED.replace('behavior = parameters["script"][step]', 'behavior = "succeed"')


@pytest.fixture(autouse=True)
def _identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    write_identity_config(
        {"identities": {"tester": {"name": "Test User", "email": "tester@example.test"}}, "default_identity": "tester"}
    )
    ensure_identity_key("tester")


def _workspace(tmp_path: Path) -> tuple[Workspace, str]:
    name = f"switch-{uuid.uuid4().hex}"
    create_workspace(name, tmp_path / "workspace", durable=False, policy={"visibility_deadline_seconds": 0.05})
    return Workspace(tmp_path / "workspace"), name


def _versions(tmp_path: Path, workspace: Workspace) -> tuple[_store.Installed, _store.Installed]:
    """Install the job's workflow ``demo`` and ``demo-next``, a newer one of the same declaration."""

    old = v3.install(workspace, tmp_path / "v1", workflow=DECLARED, executables={"run": MARKED})
    new = v3.install(workspace, tmp_path / "v2", name="demo-next", workflow=DECLARED, executables={"run": FIXED})
    return old, new


def _new(workspace: Workspace, workflow: str, script: str, placement: str, **parameters: object) -> _kernel.JobRef:
    return new_job(workspace, workflow, parameters={"script": {"start": script}, **parameters}, placement=placement).ref


def _request(name: str, action: str, *selectors: str) -> list[str]:
    return ["job", "request", action, "--workspace", name, "--reason", "bump", *selectors]


def _ran_by(workspace: Workspace, job_id: str) -> str:
    return (v3.find(workspace, job_id).path / "run" / "ran_by").read_text(encoding="utf-8")


def test_ready_jobs_switch_now_by_state_and_run_the_new_installation(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    old, new = _versions(tmp_path, workspace)
    jobs = [_new(workspace, old.id, "fail", f"project/{index}") for index in range(3)]
    other = _new(workspace, old.id, "fail", "elsewhere/0")
    before = {ref.job_id: (ref.path / "job.json").read_bytes() for ref in jobs}

    selection = ["--workflow", "demo-next", "--state", "ready", "--placement", "project"]
    assert command(_request(name, "set_workflow", *selection), CLIContext("httk", tmp_path)) == 0
    assert capsys.readouterr().out.count("\tapplied\t") == 3

    for ref in jobs:
        current = v3.find(workspace, ref.job_id)
        assert current.state == "ready" and (current.path / "job.json").read_bytes() == before[ref.job_id]
        doc = v3.state_of(current)
        assert dict(doc.workflow_override or {}) == {"id": new.id, "name": "demo-next"} and doc.runner_steps is None
        (entry,) = (entry for entry in doc.history_tail if entry["event"] == "request_applied")
        assert entry["action"] == "set_workflow" and entry["workflow"] == {"from": old.id, "to": new.id}
    assert not (v3.find(workspace, other.job_id).path / "state.json").exists()  # not selected: never claimed
    checked = {finding["job_id"]: finding["workflow"] for finding in precheck_jobs(workspace)}
    assert checked == {**{ref.job_id: new.id for ref in jobs}, other.job_id: old.id}

    v3.run(workspace)
    for ref in jobs:
        assert v3.find(workspace, ref.job_id).state == "succeeded" and _ran_by(workspace, ref.job_id) == new.id
        pin = v3.state_of(v3.find(workspace, ref.job_id)).workflow_pin
        assert pin is not None and pin["id"] == new.id
    assert v3.find(workspace, other.job_id).state == "failed" and _ran_by(workspace, other.job_id) == old.id


def test_a_failed_job_switches_then_restarts_on_the_new_installation(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    old, new = _versions(tmp_path, workspace)
    ref = _new(workspace, "demo", "fail", "project/failed")
    v3.run(workspace)
    failed = v3.find(workspace, ref.job_id)
    assert failed.state == "failed" and _ran_by(workspace, ref.job_id) == old.id
    pinned, stored = v3.state_of(failed).job_digest, (failed.path / "job.json").read_bytes()
    assert pinned is not None

    assert command(_request(name, "set_workflow", "--workflow", new.id, ref.job_id), CLIContext("httk", tmp_path)) == 0
    assert command(_request(name, "restart", ref.job_id), CLIContext("httk", tmp_path)) == 0
    capsys.readouterr()
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "succeeded" and _ran_by(workspace, ref.job_id) == new.id
    assert v3.state_of(done).job_digest == pinned and (done.path / "job.json").read_bytes() == stored
    assert JobDefinition.from_path(done.path / "job.json").workflow_id == old.id


def test_an_owned_job_switches_at_its_next_boundary(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    _old, new = _versions(tmp_path, workspace)
    ref = _new(workspace, "demo", "fail", "project/owned")
    with _kernel.register_owner(workspace, kind="manager", label="live", allocation=None, advertised={}) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        assert (
            command(_request(name, "set_workflow", "--workflow", "demo-next", ref.job_id), CLIContext("httk", tmp_path))
            == 0
        )
        assert "\tqueued\tanother owner holds the job" in capsys.readouterr().out
        owned.give_back()
    # A manager applies it when it claims the job, before it assesses and launches it.
    v3.run(workspace)
    assert v3.find(workspace, ref.job_id).state == "succeeded" and _ran_by(workspace, ref.job_id) == new.id


def test_refusals_post_nothing_and_name_the_rule(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    _versions(tmp_path, workspace)
    ref = _new(workspace, "demo", "fail", "project/refused")
    stranger = v3.install(workspace, tmp_path / "stranger", name="stranger")
    for workflow, rule in (("absent", "rule 1 (installed)"), (stranger.id, "rule 2 (declaration)")):
        assert (
            command(_request(name, "set_workflow", "--workflow", workflow, ref.job_id), CLIContext("httk", tmp_path))
            == 2
        )
        assert rule in capsys.readouterr().err
    assert not list(workspace.control.glob("requests/*.json"))
    assert command(_request(name, "set_workflow", ref.job_id), CLIContext("httk", tmp_path)) == 2
    assert "--workflow is required by, and only valid with, the set_workflow action" in capsys.readouterr().err
    assert command(_request(name, "pause", "--workflow", "demo-next", ref.job_id), CLIContext("httk", tmp_path)) == 2


def _candidate(
    tmp_path: Path, workspace: Workspace, label: str, manifest: str = "", **options: object
) -> _store.Installed:
    return v3.install(workspace, tmp_path / label, name=label, workflow=DECLARED, manifest=manifest, **options)  # type: ignore[arg-type]


def test_each_compatibility_rule(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    structure = '[workflow.inputs.structure]\ndestination = "POSCAR"\n'
    old = v3.install(
        workspace,
        tmp_path / "v1",
        workflow=DECLARED,
        manifest='[workflow.parameters.encut]\ntype = "number"\ndefault = 500\n' + structure,
    )
    ref = _new(workspace, old.id, "fail", "project/rules")
    job = JobDefinition.from_path(ref.path / "job.json")
    assert set(job.declared) == {"parameters", "inputs"} and "structure" in str(job.declared["inputs"])
    encut = '[workflow.parameters.encut]\ntype = "number"\n' + structure
    assert workflow_switch_problem(workspace, job, None, _candidate(tmp_path, workspace, "same", encut)) is None
    cases = {
        "rule 3 (inputs)": encut.replace("POSCAR", "CONTCAR"),
        "rule 4 (outputs)": encut + '[workflow.outputs.relaxed]\nentry_type = "structures"\n',
        "rule 5 (parameters): local:demo-param declares kpoints": encut
        + '[workflow.parameters.kpoints]\ntype = "integer"\n',
        "rule 5 (parameters): the job's parameters encut are unknown": structure,
        "rule 5 (parameters): workflow parameter 'encut' does not match": encut.replace("number", "string"),
        "rule 6 (environment)": encut + '[workflow.environment.VASP_COMMAND]\ntype = "string"\n',
    }
    for index, (rule, manifest) in enumerate(cases.items()):
        label = "demo-param" if "kpoints" in rule else f"case{index}"
        problem = workflow_switch_problem(workspace, job, None, _candidate(tmp_path, workspace, label, manifest))
        assert problem is not None and problem.startswith(rule), (rule, problem)
    # Rule 7: the job's current step, here a later activation's, must be one the candidate declares.
    package = tmp_path / "late"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "late"\n{DECLARED}\n[workflow.runner]\nsteps = ["start"]\n{encut}', encoding="utf-8"
    )
    (package / "run").write_text(MARKED, encoding="utf-8")
    with v3.cli_owner(workspace) as owner:
        late = _store.install(workspace, owner, package)
    assert workflow_switch_problem(workspace, job, None, late) is None
    at_finish = StateDoc.empty(job.id).next_activation("finish", "advance")
    assert str(workflow_switch_problem(workspace, job, at_finish, late)).startswith("rule 7 (step)")
    # Rule 2 without a declaration $id: the same short name.
    plain = v3.install(workspace, tmp_path / "plain", name="plain")
    unnamed = JobDefinition.from_path(_new(workspace, plain.id, "fail", "project/plain").path / "job.json")
    assert str(workflow_switch_problem(workspace, unnamed, None, old)).startswith("rule 2 (short name)")


def test_unverifiable_snapshots_and_language_realizations_are_refused(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    candidate = _candidate(tmp_path, workspace, "next")
    declaration = {"workflow": {"$id": URI}}
    # A job that stores no declaration (a spawned child) is compared with its current installation; it is gone.
    gone = v3.submit(workspace, ("local:gone", "next"), {"start": "fail"})
    job = JobDefinition.from_path(gone.path / "job.json")
    assert str(workflow_switch_problem(workspace, job, None, candidate)) == (
        "rule 3 (inputs): cannot be verified: job.json has no snapshot and local:gone is gone"
    )
    no_outputs = v3.submit(workspace, ("local:gone", "next"), {"start": "fail"}, declared={"inputs": {}})
    job = JobDefinition.from_path(no_outputs.path / "job.json")
    assert str(workflow_switch_problem(workspace, job, None, candidate)).startswith(
        "rule 4 (outputs): cannot be verified"
    )
    # A job made from a package stores its declaration and leaves empty snapshots out: they verify as empty,
    # so a job without inputs still switches after its old installation was uninstalled.
    scaffolded = v3.submit(
        workspace, ("local:gone", "gone"), {"start": "fail"}, declarations={"workflow": {"$id": URI, "outputs": []}}
    )
    job = JobDefinition.from_path(scaffolded.path / "job.json")
    assert workflow_switch_problem(workspace, job, None, candidate) is None
    language = v3.submit(
        workspace,
        candidate,
        {"start": "fail"},
        declarations=declaration,
        parameters={"workflow_realization": "language"},
    )
    job = JobDefinition.from_path(language.path / "job.json")
    assert "language realization" in str(workflow_switch_problem(workspace, job, None, candidate))


def test_a_call_follows_the_launched_installation(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    callee = v3.install(workspace, tmp_path / "callee", name="callee")
    old = v3.install(workspace, tmp_path / "v1", workflow=DECLARED)
    new = v3.install(
        workspace,
        tmp_path / "v2",
        name="demo-next",
        workflow=DECLARED,
        manifest=f'[workflow.calls]\nrelax = "{callee.id}"\n',
    )
    payload = _new(workspace, old.id, "fail", "project/caller").path

    def attempt(workflow: _store.Installed) -> Attempt:
        definition = json.loads((payload / "job.json").read_text(encoding="utf-8"))
        control = payload / "attempts" / str(uuid.uuid4())
        control.mkdir(parents=True)
        (payload / "run").mkdir(exist_ok=True)
        context = {
            "format": "httk-workflow-attempt-context",
            "format_version": 3,
            "workflow": {"id": workflow.id, "name": workflow.name},
            "workspace_id": workspace.workspace_id,
            "job_id": definition["id"],
            "job_key": payload.name.split("~")[0],
            "placement": definition["placement"],
            "payload": str(payload),
            "step": "start",
            "activation_id": str(uuid.uuid4()),
            "attempt_id": control.name,
            "settings": {},
            "durable": False,
            "deadline": None,
        }
        environment = {
            "HTTK_WORKFLOW_CONTEXT": json.dumps(context),
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(payload),
            "HTTK_WORKFLOW_WORKDIR": str(payload / "run"),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(workspace.root),
            "HTTK_WORKFLOW_DATA_DIR": str(payload / "data"),
            "HTTK_WORKFLOW_STEP": "start",
        }
        return Attempt.initialize(environment, runner=None)

    with pytest.raises(ValueError, match="not declared in \\[workflow.calls\\] of demo"):
        attempt(old).call("relax", label="relax")
    switched = attempt(new)
    switched.call("relax", label="relax")
    child = json.loads(next(switched.control.glob("outcome.tmp.*/children/jobs/*/job.json")).read_text())
    assert child["workflow"] == {"id": callee.id, "name": "callee"}


def test_mount_daemon_remotes_refuse_set_workflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    bundle = tmp_path / "remote"
    bundle.mkdir()
    binding = WorkspaceBinding("cluster:runs", "cluster", None)
    monkeypatch.setattr(job_cli, "_resolve_binding", lambda *_args, **_kwargs: (binding, None))
    monkeypatch.setattr(job_cli, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle=bundle))
    (bundle / "remote.json").write_text(json.dumps({"kind": "mount-daemon", "settings": {}}), encoding="utf-8")
    argv = _request("cluster:runs", "set_workflow", "--workflow", "demo-next", str(uuid.uuid4()))
    assert command(argv, CLIContext("httk", tmp_path)) == 2
    assert "not carried by the workspace daemon protocol" in capsys.readouterr().err
    argv = _request("cluster:runs", "pause", "--state", "ready")
    assert command(argv, CLIContext("httk", tmp_path)) == 2
    assert "--state, --step and --placement select jobs of a local workspace" in capsys.readouterr().err


def test_job_request_selects_by_state_for_every_action(tmp_path: Path, capsys) -> None:
    workspace, name = _workspace(tmp_path)
    v3.install(workspace, tmp_path / "v1", workflow=DECLARED)
    refs = [_new(workspace, "demo", "fail", f"project/{index}") for index in range(2)]
    # JOB selectors and --state union; a job named both ways is requested once.
    assert command(_request(name, "pause", refs[0].job_id, "--state", "ready"), CLIContext("httk", tmp_path)) == 0
    assert capsys.readouterr().out.count("\tapplied\t") == 2
    assert [v3.find(workspace, ref.job_id).state for ref in refs] == ["paused", "paused"]
    assert command(_request(name, "pause"), CLIContext("httk", tmp_path)) == 2
    assert "give JOB_ID selectors, --state STATE, --step STEP, or a combination" in capsys.readouterr().err
    assert command(_request(name, "pause", refs[0].job_id, "--placement", "project"), CLIContext("httk", tmp_path)) == 2
    assert "--placement narrows --state" in capsys.readouterr().err
    assert os.environ["XDG_CONFIG_HOME"].startswith(str(tmp_path))


def _switched(tmp_path: Path) -> tuple[Workspace, _store.Installed, _store.Installed, _kernel.JobRef]:
    """A workspace with ``demo`` and ``demo-next`` and one ready job of ``demo`` switched to ``demo-next``."""

    workspace = v3.workspace(tmp_path / "workspace")
    old, new = _versions(tmp_path, workspace)
    ref = _new(workspace, old.id, "fail", "project/switched")
    posted = _requests.post(
        workspace,
        action="set_workflow",
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="op",
        reason="r",
        workflow=new.id,
    )
    (outcome,) = removal.serve_requests(workspace, [(ref, posted)])
    assert outcome.status == "applied", outcome
    return workspace, old, new, v3.find(workspace, ref.job_id)


def test_inspection_and_uninstall_users_follow_the_effective_workflow(tmp_path: Path) -> None:
    workspace, old, new, ref = _switched(tmp_path)
    # workflow uninstall --check warns about the jobs that run an installation: the new one, no longer the old.
    assert _describe._users(workspace, new.id) == [ref.job_key] and _describe._users(workspace, old.id) == []
    report = describe_job(workspace, ref)
    assert report["workflow"] == {"id": old.id, "name": "demo"}
    assert report["workflow_override"] == {"id": new.id, "name": "demo-next"}
    assert f"demo ({old.id}) -> demo-next ({new.id}) by set_workflow" in render_job(report)
    (installed,) = (check for check in explain_job(workspace, ref).checks if check.name == "installed workflow")
    assert installed.satisfied and installed.detail.startswith(new.id)
    lines: list[str] = []
    debug_job(workspace, ref.job_id, emit=lines.append, timeout=60)
    assert f"(workflow {new.id})" in lines[0]


def test_eject_keeps_the_override_but_never_into_an_exchange(tmp_path: Path) -> None:
    workspace, _old, new, ref = _switched(tmp_path)
    exchange = tmp_path / "exchange"
    (exchange / "inbox").mkdir(parents=True)
    (exchange / "exchange.json").write_text("{}", encoding="utf-8")
    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        with pytest.raises(ValueError, match="set_workflow"):
            _moving.eject(workspace, owner, owned, destination=exchange / "inbox", tree=False)
    assert v3.find(workspace, ref.job_id).state == "ready" and not list((exchange / "inbox").iterdir())
    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, v3.find(workspace, ref.job_id))
        assert owned is not None
        report = _moving.eject(workspace, owner, owned, destination=tmp_path / "out", tree=False)
    # A trusted adoption keeps state.json, so the workflow the job is missing is the one it runs.
    target = v3.workspace(tmp_path / "target")
    v3.install(target, tmp_path / "target-v1", workflow=DECLARED)
    with v3.cli_owner(target) as owner:
        adopted = _moving.adopt(target, owner, report.destination, untrusted=False)
    assert adopted is not None and adopted.missing_workflows == (new.id,)
    assert dict(v3.state_of(v3.find(target, ref.job_id)).workflow_override or {}) == {"id": new.id, "name": "demo-next"}


def test_ssh_remote_set_workflow_resolves_on_the_far_side(tmp_path: Path, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="switching")
    fake_remote(project, template="local")
    create_workspace("station", tmp_path / "station", durable=False)
    remote = Workspace(tmp_path / "station", durable=False)
    old, new = _versions(tmp_path, remote)
    refs = [_new(remote, old.id, "fail", f"project/{index}") for index in range(2)]
    context = CLIContext("httk", project)
    # A name resolves on the far side, which checks every job there; the envelopes name the id it resolved to.
    argv = _request("cluster:station", "set_workflow", "--workflow", "demo-next", refs[0].job_id)
    assert command(argv, context) == 0
    captured = capsys.readouterr()
    assert "\tapplied\t" in captured.out and f"demo-next is installed here as {new.id}" in captured.err
    # An exact id must come back verbatim; an incompatible candidate is refused there, naming the rule.
    assert command(_request("cluster:station", "set_workflow", "--workflow", new.id, refs[1].job_id), context) == 0
    capsys.readouterr()
    for ref in refs:
        doc = v3.state_of(v3.find(remote, ref.job_id))
        assert dict(doc.workflow_override or {}) == {"id": new.id, "name": "demo-next"}
    stranger = v3.install(remote, tmp_path / "stranger", name="stranger")
    assert command(_request("cluster:station", "set_workflow", "--workflow", stranger.id, refs[0].job_id), context) == 2
    assert "rule 2 (declaration)" in capsys.readouterr().err


def test_remote_envelopes_must_name_the_typed_id_or_one_resolved_id() -> None:
    job_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    operator = "Test User <tester@example.test>"

    def envelope(job_id: str, workflow: str) -> dict[str, object]:
        return {
            "format": "httk-workflow-request",
            "format_version": 3,
            "request_id": str(uuid.uuid4()),
            "job_id": job_id,
            "placement": "project/x",
            "action": "set_workflow",
            "operator": operator,
            "reason": "r",
            "created_at": "2026-10-23T00:00:00Z",
            "workflow": workflow,
        }

    def check(typed: str, *workflows: str) -> list[dict[str, object]]:
        document = {
            "format": "httk-workflow-request-envelopes",
            "format_version": 2,
            "envelopes": [envelope(job_id, workflow) for job_id, workflow in zip(job_ids, workflows, strict=True)],
            "job_keys": job_ids,
        }
        arguments = SimpleNamespace(
            job_id=job_ids,
            action="set_workflow",
            reason="r",
            priority=None,
            override_step=None,
            force=False,
            workflow=typed,
        )
        return job_cli._validate_remote_envelopes(document, arguments, operator)  # type: ignore[arg-type]

    assert len(check("demo-next", "local:demo-next", "local:demo-next")) == 2
    assert len(check("local:demo-next", "local:demo-next", "local:demo-next")) == 2
    for typed, workflows in (
        ("local:demo-next", ("local:other", "local:other")),  # an exact id comes back verbatim
        ("demo-next", ("local:demo-next", "local:other")),  # a name resolves to one id for every job
    ):
        with pytest.raises(ValueError, match="'workflow' disagrees"):
            check(typed, *workflows)


def test_a_child_without_snapshots_is_compared_with_its_installation(tmp_path: Path) -> None:
    # A spawned child stores no declarations, declared parameters or declared environment.
    workspace = v3.workspace(tmp_path / "workspace")
    encut = '[workflow.parameters.encut]\ntype = "number"\n'
    environment = '[workflow.environment.VASP_COMMAND]\ntype = "string"\n'
    old = v3.install(workspace, tmp_path / "v1", workflow=DECLARED, manifest=encut + environment)
    child = JobDefinition.from_path(v3.submit(workspace, old, {"start": "fail"}).path / "job.json")
    assert not child.declarations and not child.declared and not child.environment["declared"]
    # Rule 2 compares the installation's $id, so the same declaration under another name switches.
    assert (
        workflow_switch_problem(workspace, child, None, _candidate(tmp_path, workspace, "same", encut + environment))
        is None
    )
    assert (
        workflow_switch_problem(workspace, child, None, _candidate(tmp_path, workspace, "next", encut + environment))
        is None
    )
    assert workflow_switch_problem(workspace, child, None, old) is None
    for label, manifest, rule in (
        ("dropped", environment, "rule 5 (parameters): the job's parameters encut are unknown"),
        ("other", encut, "rule 6 (environment)"),
    ):
        problem = workflow_switch_problem(workspace, child, None, _candidate(tmp_path, workspace, label, manifest))
        assert str(problem).startswith(rule), problem
    with v3.cli_owner(workspace) as owner:
        _store.uninstall(workspace, owner, old.id)
    assert str(workflow_switch_problem(workspace, child, None, old)).startswith("rule 3 (inputs): cannot be verified")


def test_a_waiting_parent_must_keep_the_steps_its_join_advances_to(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    old = v3.install(workspace, tmp_path / "v1", workflow=DECLARED)
    job = JobDefinition.from_path(_new(workspace, old.id, "fail", "project/parent").path / "job.json")
    package = tmp_path / "start-only"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "start-only"\n{DECLARED}\n[workflow.runner]\nsteps = ["start"]\n', encoding="utf-8"
    )
    (package / "run").write_text(MARKED, encoding="utf-8")
    with v3.cli_owner(workspace) as owner:
        start_only = _store.install(workspace, owner, package)
    waiting = StateDoc.empty(job.id).next_activation("start", "initial")
    assert workflow_switch_problem(workspace, job, waiting, start_only) is None
    join: dict[str, object] = {
        "children": [],
        "condition": "all_succeeded",
        "next_step": "gather",
        "on_impossible": None,
    }
    waiting = waiting.updated(join=join)
    assert str(workflow_switch_problem(workspace, job, waiting, start_only)) == (
        f"rule 7 (step): {start_only.id} does not declare the job's step 'gather'"
    )
    assert workflow_switch_problem(workspace, job, waiting, old) is None
    rescued = waiting.updated(join={**join, "on_impossible": {"action": "advance", "next_step": "salvage"}})
    assert "'salvage'" in str(workflow_switch_problem(workspace, job, rescued, old))


def test_the_owner_checks_again_and_records_what_was_asked(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    old, new = _versions(tmp_path, workspace)
    stranger = v3.install(workspace, tmp_path / "stranger", name="stranger")
    ref = _new(workspace, old.id, "fail", "project/owner")

    def serve(workflow: str) -> removal.RequestOutcome:
        current = v3.find(workspace, ref.job_id)
        posted = _requests.post(
            workspace,
            action="set_workflow",
            job_id=ref.job_id,
            placement=ref.placement or "",
            operator="op",
            reason="r",
            workflow=workflow,
        )
        (outcome,) = removal.serve_requests(workspace, [(current, posted)])
        return outcome

    for workflow, rule in ((stranger.id, "rule 2 (declaration)"), ("local:absent", "rule 1 (installed)")):
        outcome = serve(workflow)
        assert outcome.status == "refused" and rule in outcome.explanation, outcome
        dropped = v3.state_of(v3.find(workspace, ref.job_id)).history_tail[-2]
        assert dropped["event"] == "request_dropped" and dropped["workflow"] == {"from": old.id, "to": workflow}
    assert serve(new.id).status == "applied"
    assert v3.state_of(v3.find(workspace, ref.job_id)).workflow_override is not None
    # Switching back to job.json's workflow clears the override instead of recording a no-op one.
    assert serve(old.id).status == "applied"
    assert v3.state_of(v3.find(workspace, ref.job_id)).workflow_override is None


def test_an_empty_selection_is_an_error(tmp_path: Path, capsys) -> None:
    _, name = _workspace(tmp_path)
    assert command(_request(name, "pause", "--state", "ready"), CLIContext("httk", tmp_path)) == 1
    assert "no jobs selected" in capsys.readouterr().err


def test_outputs_produced_under_an_override_keep_their_workflow_after_a_switch_back(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    old = v3.install(workspace, tmp_path / "v1", workflow=DECLARED, executables={"run": MARKED})
    new = v3.install(workspace, tmp_path / "v2", name="demo-next", workflow=DECLARED, executables={"run": MARKED})
    ref = _new(workspace, old.id, "fail", "project/back")

    def switch(workflow: str) -> None:
        current = v3.find(workspace, ref.job_id)
        posted = _requests.post(
            workspace,
            action="set_workflow",
            job_id=ref.job_id,
            placement=ref.placement or "",
            operator="op",
            reason="r",
            workflow=workflow,
        )
        assert removal.serve_requests(workspace, [(current, posted)])[0].status == "applied"

    switch(new.id)
    v3.run(workspace)
    assert _ran_by(workspace, ref.job_id) == new.id
    switch(old.id)  # back to job.json's workflow: the override is gone, the pin still names demo-next
    (item,) = collect(workspace, states=["failed"], allow_job_collector=True)
    assert item.record.job.get("workflow_override") is None and item.workflow_id == new.id
    assert "modified" not in str(item.missing_collector)


def test_rule_7_also_checks_the_step_a_restart_reruns(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    old = v3.install(workspace, tmp_path / "v1", workflow=DECLARED)
    job = JobDefinition.from_path(_new(workspace, old.id, "fail", "project/parent").path / "job.json")
    package = tmp_path / "start-only"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "start-only"\n{DECLARED}\n[workflow.runner]\nsteps = ["start"]\n', encoding="utf-8"
    )
    (package / "run").write_text(MARKED, encoding="utf-8")
    with v3.cli_owner(workspace) as owner:
        start_only = _store.install(workspace, owner, package)
    join: dict[str, object] = {
        "children": [],
        "condition": "all_succeeded",
        "next_step": "start",
        "on_impossible": None,
    }
    failed_at_wait = StateDoc.empty(job.id).next_activation("finish", "advance").updated(join=join)
    assert "'finish'" in str(workflow_switch_problem(workspace, job, failed_at_wait, start_only))


def test_a_child_that_copied_its_parents_declarations_is_compared_with_its_installation(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    encut = '[workflow.parameters.encut]\ntype = "number"\n'
    old = v3.install(workspace, tmp_path / "v1", workflow=DECLARED, manifest=encut)
    parent = JobDefinition.from_path(_new(workspace, old.id, "fail", "project/parent").path / "job.json")
    mapping = v3.job_mapping(old, {"start": "fail"}, declarations=json.loads(json.dumps(_thaw(parent.declarations))))
    mapping["parent"] = {
        "workspace_id": workspace.workspace_id,
        "job_id": parent.id,
        "job_key": parent.job_key,
        "placement": "project/parent",
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }
    child = JobDefinition.from_mapping(mapping)
    assert "workflow" in child.declarations and "encut" not in child.parameters
    assert workflow_switch_problem(workspace, child, None, _candidate(tmp_path, workspace, "same", encut)) is None
