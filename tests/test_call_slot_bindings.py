"""Call slot bindings: chosen at job creation, carried in ``job.json``, and required before a job is claimed."""

import json
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _kernel, _store, scaffold
from httk.workflow._job import JobDefinition
from httk.workflow._manager_scheduling import assess
from httk.workflow.errors import FormatError
from httk.workflow.introspection import describe_job, explain_job, render_job
from httk.workflow.manager import TaskManager
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.precheck import _workflow_problems
from httk.workflow.removal import workflow_switch_problem
from httk.workflow.scaffold import new_job
from httk.workflow.sdk import ChildSpec
from httk.workflow.workflow_cli import command
from v3_helpers import cli_owner, job_mapping, submit_mapping

STRUCTURE = "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures"
FORCES = "https://schemas.httk.org/defs/v0.1/properties/core/cartesian_site_forces"
INTERFACE = {
    "inputs": [{"name": "structure", "entry_type": "structures", "ref": STRUCTURE}],
    "outputs": [{"name": "forces", "entry_type": "records", "ref": FORCES, "product_of": "structure"}],
}
_NAMES = ("tests.binding.pair", "tests.binding.other", "tests.binding.caller")


def _package(root: Path, name: str, body: str = "", *, uri: str | None = None) -> Path:
    root.mkdir(parents=True)
    declared = "" if uri is None else f'declaration_uri = "{uri}"\n'
    manifest = f'[workflow]\nname = "{name}"\n{declared}\n[workflow.runner]\nsteps = ["start"]\n{body}'
    (root / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (root / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (root / "run").chmod(0o755)
    return root


def _callee(root: Path, name: str = "tests.binding.pair", *, product_of: str = "structure") -> Path:
    """A forces callee: ``structure`` in, ``forces`` (a product of *product_of*) out, a typed ``cutoff``."""

    return _package(
        root,
        name,
        f'\n[workflow.inputs.structure]\ndestination = "POSCAR"\nentry_type = "structures"\nref = "{STRUCTURE}"\n'
        '\n[workflow.outputs.relaxed]\nentry_type = "structures"\nproduct_of = "structure"\n'
        f'\n[workflow.outputs.forces]\nentry_type = "records"\nref = "{FORCES}"\nproduct_of = "{product_of}"\n'
        '\n[workflow.parameters.cutoff]\ntype = "number"\n',
    )


def _caller(
    root: Path,
    *,
    default: str | None = None,
    alias: str = "forces.dft",
    extra: str = "",
    name: str = "tests.binding.caller",
    uri: str | None = None,
    interface: object = INTERFACE,
) -> Path:
    slot = f'\n[workflow.calls."{alias}"]\ndeclaration_file = "forces.json"\n'
    slot += "" if default is None else f'default = "{default}"\n'
    directory = _package(root, name, slot + extra, uri=uri)
    (directory / "forces.json").write_text(json.dumps(interface), encoding="utf-8")
    return directory


@pytest.fixture(autouse=True)
def _fresh() -> Iterator[None]:
    yield
    for name in _NAMES:
        scaffold._WORKFLOW_PROVIDERS.pop(name, None)


@pytest.fixture()
def workspace(tmp_path: Path) -> Workspace:
    return Workspace.initialize(tmp_path / "workspace", durable=False)


def _install(workspace: Workspace, source: Path, **options: object) -> _store.Installed:
    with cli_owner(workspace) as owner:
        return _store.install(workspace, owner, source, **options)  # type: ignore[arg-type]


def _definition(job: scaffold.ScaffoldedJob) -> JobDefinition:
    return JobDefinition.from_path(job.payload / "job.json")


# -- job.json -------------------------------------------------------------------------------------------------------

_BINDINGS = {"forces.dft": {"workflow": "local:tests.binding.pair", "parameters": {"cutoff": 400.0}}}


def _mapping(**members: object) -> dict[str, object]:
    return {**job_mapping(("local:demo", "demo"), {"start": "succeed"}), **members}


def test_job_json_carries_bindings_and_persisted_outputs_exactly() -> None:
    definition = JobDefinition.from_mapping(_mapping(calls=_BINDINGS, persist_outputs={"forces": "forces"}))
    document = definition.as_mapping()
    assert document["calls"] == _BINDINGS and document["persist_outputs"] == {"forces": "forces"}
    assert JobDefinition.from_bytes(definition.encode()) == definition
    plain = JobDefinition.from_mapping(_mapping())
    assert plain.calls is None and plain.persist_outputs is None
    assert not {"calls", "persist_outputs"} & set(plain.as_mapping())


def test_a_job_without_bindings_keeps_its_bytes() -> None:
    # The digest of this document under the job.json code before call slots existed.
    mapping = {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": "0b0c3c56-1d5a-4c69-9a43-1b2c3d4e5f60",
        "tag": "demo",
        "name": "demo job",
        "placement": "project/0",
        "workflow": {"id": "local:demo", "name": "demo"},
        "initial_step": "start",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"maximum_attempts_per_activation": 1},
        "resources": {},
        "step_resources": {},
        "parameters": {"x": 1},
        "declarations": {},
        "declared": {},
        "environment": {},
        "parent": None,
        "seal_succeeded": None,
    }
    assert JobDefinition.from_mapping(mapping).digest == (
        "6b1547a09e07b67c945da51c5a1b168b0fb57c41eb87d6b22099615a8e5a3808"
    )


@pytest.mark.parametrize(
    ("members", "message"),
    [
        ({"calls": {}}, "calls must bind at least one call slot"),
        ({"persist_outputs": {}}, "persist_outputs must name at least one output"),
        ({"calls": {"Forces": _BINDINGS["forces.dft"]}}, "calls alias has invalid component syntax"),
        ({"calls": {"f": {"workflow": "local:x", "parameters": {}, "extra": 1}}}, r"calls\.f members differ"),
        ({"calls": {"f": {"workflow": "local: x", "parameters": {}}}}, "without whitespace"),
        ({"calls": {"f": {"workflow": "local:x", "parameters": {"c": float("nan")}}}}, "only finite JSON values"),
        ({"calls": {"f": {"workflow": "local:x", "parameters": {"c": "x" * 262144}}}}, "exceeds the 262144-byte"),
        ({"persist_outputs": {"forces": ""}}, "must name an output role"),
        (
            # Each binding's parameters are within their own limit; together they exceed the member's.
            {"calls": {alias: {"workflow": "local:x", "parameters": {"c": "x" * 150000}} for alias in ("a", "b")}},
            r"^calls serializes to \d+ bytes, which exceeds the 262144-byte limit",
        ),
    ],
)
def test_malformed_bindings_are_refused(members: dict[str, object], message: str) -> None:
    with pytest.raises(FormatError, match=message):
        JobDefinition.from_mapping(_mapping(**members))


def test_a_child_spec_keeps_the_parent_bindings() -> None:
    parent = JobDefinition.from_mapping(_mapping(calls=_BINDINGS))
    spec = ChildSpec(step="start")._job_spec(parent, ("local:demo", "demo"), "one")
    assert spec.calls == _BINDINGS
    assert ChildSpec(step="start")._job_spec(JobDefinition.from_mapping(_mapping()), ("w", "w"), "one").calls is None


# -- binding at creation ----------------------------------------------------------------------------------------------


def test_a_slot_is_bound_to_its_default_or_to_the_creators_choice(tmp_path: Path, workspace: Workspace) -> None:
    pair = _install(workspace, _callee(tmp_path / "pair"))
    other = _install(workspace, _callee(tmp_path / "other", "tests.binding.other"))
    scaffold.register_workflow(parse_workflow_manifest(tmp_path / "pair"))
    caller = _install(workspace, _caller(tmp_path / "caller", default="tests.binding.pair"))
    assert caller.record["slots"] == {"forces.dft": pair.id}
    defaulted = _definition(new_job(workspace, caller.id))
    assert defaulted.as_mapping()["calls"] == {"forces.dft": {"workflow": pair.id, "parameters": {}}}
    chosen = _definition(
        new_job(
            workspace,
            caller.id,
            calls={"forces.dft": "tests.binding.other"},
            call_parameters={"forces.dft": {"cutoff": 300}},
        )
    )
    assert chosen.as_mapping()["calls"] == {"forces.dft": {"workflow": other.id, "parameters": {"cutoff": 300}}}


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({}, r"call slot 'forces\.dft' of local:tests\.binding\.caller has no default; bind it"),
        ({"calls": {"forces": "tests.binding.pair"}}, "'forces', which is not a call slot"),
        ({"calls": {"sub": "tests.binding.pair"}}, "fixed call of local:tests.binding.caller; fixed calls cannot"),
        ({"call_parameters": {"other": {}}}, "call_parameters names 'other'"),
        ({"calls": {"forces.dft": "tests.binding.nowhere"}}, r"call slot 'forces\.dft': workflow .* is not installed"),
        (
            {"calls": {"forces.dft": "tests.binding.pair"}, "call_parameters": {"forces.dft": {"cutoff": "x"}}},
            "workflow parameter 'cutoff' does not match type 'number'",
        ),
    ],
)
def test_a_binding_is_refused(tmp_path: Path, workspace: Workspace, options: dict[str, object], message: str) -> None:
    pair = _install(workspace, _callee(tmp_path / "pair"))
    caller = _install(workspace, _caller(tmp_path / "caller", extra=f'\n[workflow.calls]\nsub = "{pair.id}"\n'))
    with pytest.raises(ValueError, match=message):
        new_job(workspace, caller.id, **options)  # type: ignore[arg-type]


def test_a_binding_that_does_not_fit_is_refused_with_its_reasons(tmp_path: Path, workspace: Workspace) -> None:
    relaxed = _install(workspace, _callee(tmp_path / "relaxed", "tests.binding.other", product_of="relaxed"))
    caller = _install(workspace, _caller(tmp_path / "caller"))
    with pytest.raises(ValueError, match=rf"call slot 'forces\.dft' bound to {relaxed.id}: .*product_of"):
        new_job(workspace, caller.id, calls={"forces.dft": relaxed.id})


def test_bindings_for_a_workflow_without_slots_are_refused(tmp_path: Path, workspace: Workspace) -> None:
    pair = _install(workspace, _callee(tmp_path / "pair"))
    with pytest.raises(ValueError, match="not a call slot of local:tests.binding.pair"):
        new_job(workspace, pair.id, calls={"x": pair.id})


def test_install_installs_the_bound_workflow_but_never_a_path(tmp_path: Path, workspace: Workspace) -> None:
    caller = _install(workspace, _caller(tmp_path / "caller"))
    scaffold.register_workflow(parse_workflow_manifest(_callee(tmp_path / "pair")))
    with pytest.raises(ValueError, match="is not installed"):
        new_job(workspace, caller.id, calls={"forces.dft": "tests.binding.pair"})
    # A binding names a workflow like a [workflow.calls] reference, so a path is refused before anything installs.
    with pytest.raises(ValueError, match=r"call slot 'forces\.dft': .*not a path"):
        new_job(workspace, caller.id, calls={"forces.dft": str(tmp_path / "pair")}, install=True)
    assert [installed.id for installed in _store.list_installed(workspace)] == [caller.id]
    job = new_job(workspace, caller.id, calls={"forces.dft": "tests.binding.pair"}, install=True)
    assert _store.lookup(workspace, "local:tests.binding.pair") is not None
    assert _definition(job).as_mapping()["calls"]["forces.dft"]["workflow"] == "local:tests.binding.pair"  # type: ignore[index]


def test_job_new_binds_slots_from_the_command_line(
    tmp_path: Path, workspace: Workspace, caplog: pytest.LogCaptureFixture
) -> None:
    pair = _install(workspace, _callee(tmp_path / "pair"))
    caller = _install(workspace, _caller(tmp_path / "caller"))
    (tmp_path / "tags.json").write_text('{"ENCUT": 520}', encoding="utf-8")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "w")
    argv = ["job", "new", "--workspace", name, "--workflow", caller.id, "--call", f"forces.dft={pair.id}"]
    argv += ["--call-parameter", "forces.dft", "incar_tags=@" + str(tmp_path / "tags.json")]
    argv += ["--call-parameter", "forces.dft", "cutoff=400"]
    with caplog.at_level(logging.WARNING):
        assert command(argv, context) == 0
    assert "call slot 'forces.dft' parameter 'incar_tags' is not declared" in caplog.text
    [ref] = [path for path in (workspace.root / "jobs" / "ready").rglob("job.json")]
    calls = JobDefinition.from_path(ref).as_mapping()["calls"]
    assert calls == {"forces.dft": {"workflow": pair.id, "parameters": {"incar_tags": {"ENCUT": 520}, "cutoff": 400}}}


# -- readiness ------------------------------------------------------------------------------------------------------


def _bound_job(tmp_path: Path, workspace: Workspace) -> tuple[JobDefinition, _store.Installed, _store.Installed]:
    """A job of a caller whose slot defaults to ``pair`` but is bound to ``other``; return it, pair and other."""

    pair = _install(workspace, _callee(tmp_path / "pair"))
    other = _install(workspace, _callee(tmp_path / "other", "tests.binding.other"))
    scaffold.register_workflow(parse_workflow_manifest(tmp_path / "pair"))
    caller = _install(workspace, _caller(tmp_path / "caller", default="tests.binding.pair"))
    job = _definition(new_job(workspace, caller.id, calls={"forces.dft": other.id}))
    return job, pair, other


def _uninstall(workspace: Workspace, installed: _store.Installed) -> None:
    with cli_owner(workspace) as owner:
        _store.uninstall(workspace, owner, installed.id)


def test_a_job_waits_for_its_bound_workflow_not_for_the_default(tmp_path: Path, workspace: Workspace) -> None:
    job, pair, other = _bound_job(tmp_path, workspace)
    _uninstall(workspace, pair)  # only the overridden default is gone: no dependency of this job
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        assert assess(manager, job, None)[1] is None
    assert _workflow_problems(workspace, job.workflow_id, job)[1] == []
    _uninstall(workspace, other)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        blocker = assess(manager, job, None)[1]
    assert blocker is not None and blocker[0] == "calls" and f"{other.id} is not installed" in blocker[1]
    assert _workflow_problems(workspace, job.workflow_id, job)[1] == [
        f"called workflow {other.id} is not installed in this workspace; run 'httk workflow install'"
    ]


def test_bindings_must_be_exactly_the_installed_slots(tmp_path: Path, workspace: Workspace) -> None:
    pair = _install(workspace, _callee(tmp_path / "pair"))
    caller = _install(workspace, _caller(tmp_path / "caller"))
    unbound = JobDefinition.from_mapping(job_mapping(caller, {"start": "succeed"}))
    stray = JobDefinition.from_mapping({**job_mapping(pair, {"start": "succeed"}), "calls": _BINDINGS})
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        for job, message in (
            (unbound, "the job binds call slots none, but workflow local:tests.binding.caller has call slots"),
            (
                stray,
                "the job binds call slots ['forces.dft'], but workflow local:tests.binding.pair has call slots none",
            ),
        ):
            blocker = assess(manager, job, None)[1]
            assert blocker is not None and blocker[0] == "calls" and message in blocker[1]
            assert any(message in problem for problem in _workflow_problems(workspace, job.workflow_id, job)[1])
        # A workflow without slots and a job without bindings: nothing changes.
        assert assess(manager, JobDefinition.from_mapping(job_mapping(pair, {"start": "succeed"})), None)[1] is None


# -- workflow switch ------------------------------------------------------------------------------------------------


def test_a_switch_carries_the_bindings_only_where_they_fit(tmp_path: Path, workspace: Workspace) -> None:
    job, _pair, other = _bound_job(tmp_path, workspace)

    def candidate(label: str, **options: object) -> _store.Installed:
        return _install(workspace, _caller(tmp_path / label, **options))  # type: ignore[arg-type]

    assert workflow_switch_problem(workspace, job, None, candidate("same")) is None
    renamed = workflow_switch_problem(workspace, job, None, candidate("renamed", alias="forces"))
    assert str(renamed).startswith("rule 8 (call slots): local:tests.binding.caller has call slots ['forces']")
    narrowed = _caller(tmp_path / "narrowed")
    other_ref = json.dumps(INTERFACE).replace(STRUCTURE, "https://example.test/another-structure")
    (narrowed / "forces.json").write_text(other_ref, encoding="utf-8")
    problem = workflow_switch_problem(workspace, job, None, _install(workspace, narrowed))
    assert str(problem).startswith(f"rule 8 (call slots): {other.id} bound to call slot 'forces.dft' does not fit")
    _uninstall(workspace, other)
    gone = workflow_switch_problem(workspace, job, None, candidate("again"))
    assert str(gone) == f"rule 8 (call slots): the workflow {other.id} bound to call slot 'forces.dft' is not installed"


def test_a_switch_keeps_each_slot_interface(tmp_path: Path, workspace: Workspace) -> None:
    uri = "https://example.test/declarations/caller"
    pair = _install(workspace, _callee(tmp_path / "pair"))
    caller = _install(workspace, _caller(tmp_path / "caller", uri=uri))
    job = _definition(new_job(workspace, caller.id, calls={"forces.dft": pair.id}))
    # The bound workflow still fits an output renamed to its own role, but the step reads the interface's names.
    renamed = {**INTERFACE, "outputs": [{**INTERFACE["outputs"][0], "name": "site_forces"}]}
    candidate = _install(
        workspace, _caller(tmp_path / "renamed", name="tests.binding.renamed", uri=uri, interface=renamed)
    )
    assert workflow_switch_problem(workspace, job, None, candidate) == (
        "rule 8 (call slots): local:tests.binding.renamed declares another interface for call slot 'forces.dft'"
    )
    documented = {
        "title": "another title",
        "description": "documented",
        "outputs": INTERFACE["outputs"],
        "inputs": INTERFACE["inputs"],
    }
    same = _install(workspace, _caller(tmp_path / "same", name="tests.binding.same", uri=uri, interface=documented))
    assert workflow_switch_problem(workspace, job, None, same) is None


def test_job_show_and_why_report_bindings(tmp_path: Path, workspace: Workspace) -> None:
    job, _pair, other = _bound_job(tmp_path, workspace)
    [ref] = [ref for ref in _kernel.list_jobs(workspace, "ready") if ref.job_id == job.id]
    report = describe_job(workspace, ref)
    assert report["calls"] == {"forces.dft": {"workflow": other.id, "parameters": {}}}
    assert "persist_outputs" not in report
    assert f"forces.dft -> {other.id}" in render_job(report)
    caller = _store.lookup(workspace, "local:tests.binding.caller")
    assert caller is not None
    unbound = submit_mapping(
        workspace, {**job_mapping(caller, {"start": "succeed"}), "persist_outputs": {"f": "forces"}}
    )
    shown = describe_job(workspace, unbound)
    assert "calls" not in shown and shown["persist_outputs"] == {"f": "forces"}
    assert "f <- forces" in render_job(shown)
    checks = {check.name: check for check in explain_job(workspace, unbound).checks}
    assert checks["call slot bindings"].satisfied is False
    assert "the job binds call slots none" in checks["call slot bindings"].detail


def test_a_slot_is_fitted_once_per_installed_tree(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from httk.workflow import _slots
    from httk.workflow.sdk import Attempt

    job, _pair, other = _bound_job(tmp_path, workspace)
    fitted: list[str] = []
    original = _slots.fit

    def counting(interface: object, callee: object) -> object:
        fitted.append(str(getattr(callee, "workflow_id", "")))
        return original(interface, callee)  # type: ignore[arg-type]

    monkeypatch.setattr(_slots, "fit", counting)
    # The attempt of a step of this job, as far as calling through its slot reads it.
    attempt = object.__new__(Attempt)
    attempt.workspace = workspace.root
    attempt.context = SimpleNamespace(workflow_id=job.workflow_id, durable=False)  # type: ignore[assignment]
    attempt._own, attempt._fits, attempt._job = None, {}, job
    for _ in range(2):
        found = attempt._slot_callee("forces.dft")
        assert found is not None and found[0].id == other.id
    assert fitted == [other.id]
    # Reinstalled under the same id with another tree, the bound workflow is fitted again.
    (tmp_path / "other" / "README").write_text("changed", encoding="utf-8")
    assert _install(workspace, tmp_path / "other").id == other.id
    attempt._slot_callee("forces.dft")
    assert fitted == [other.id, other.id]
