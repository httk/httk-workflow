"""Calling through a call slot: translated inputs, the site guard, persisted outputs and ``child_outputs``.

A pure-Python callee computes deterministic per-site "forces" from the fractional
positions of the POSCAR it was staged, and a parent calls it through its ``forces``
slot. Everything runs with a real manager, as the fixed-call tests do.
"""

import json
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httk.core
import pytest

from attempt_fixtures import every_job, find, run_manager
from httk.workflow import Workspace, _kernel, _store, scaffold
from httk.workflow._job import JobDefinition
from httk.workflow._state import read_state_unowned
from httk.workflow.scaffold import new_job
from httk.workflow.sdk import Attempt, ChildResult

_SRC = str(Path(__file__).parents[1] / "src")
STRUCTURE = "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures"
FORCES = "https://schemas.httk.org/defs/v0.1/properties/core/cartesian_site_forces"
ENERGY = "https://schemas.httk.org/defs/v0.1/properties/core/total_energy"
INTERFACE = {
    "title": "structure to forces",
    "inputs": [{"name": "structure", "entry_type": "structures", "ref": STRUCTURE}],
    "outputs": [{"name": "forces", "entry_type": "records", "ref": FORCES, "product_of": "structure"}],
}

_CALLEE = f'''#!/usr/bin/env python3
import json
import sys

sys.path.insert(0, {_SRC!r})

import httk.core
from httk.atomistic import UnitcellStructureView
from httk.workflow import Runner

run = Runner("tests.rt.pair")


@run.step
def start(a):
    if a.parameter("stale", False):
        # An earlier step publishes data the final step replaces: data/ exists when the final step succeeds.
        (a.workdir / "forces.json").write_text(json.dumps({{"forces": [[9.0, 9.0, 9.0]] * 4, "energy": 0.0}}))
        a.put(a.workdir / "forces.json", "forces.json")
        a.advance("finish")
        return
    if a.parameter("staged", False):
        # A relaxation publishes its CONTCAR and cleans run/; the static step publishes its OUTCAR on success.
        (a.workdir / "CONTCAR").write_text("relaxed")
        a.put(a.workdir / "CONTCAR", "relax/CONTCAR")
        (a.workdir / "CONTCAR").unlink()
        a.advance("finish")
        return
    finish(a)


@run.step
def finish(a):
    view = UnitcellStructureView(httk.core.load(str(a.payload / "files" / "POSCAR"), precision=1e-6))
    scale = a.parameter("scale")
    forces = [[scale * x for x in row] for row in view.sites.reduced_coords.to_floats()]
    (a.workdir / "forces.json").write_text(json.dumps({{"forces": forces, "energy": -1.0 * len(forces)}}))
    if a.parameter("publish", False):
        a.put(a.workdir / "forces.json", "forces.json")
    if a.parameter("staged", False):
        a.put(a.workdir / "forces.json", "static/OUTCAR")
        (a.workdir / "forces.json").unlink()
    if a.parameter("no_store", False):
        # A runtime without httk-store: the outputs are persisted all the same.
        sys.modules["httk.store"] = None
        sys.modules["httk.store.validation"] = None
    a.succeed()


raise SystemExit(run.main())
'''

_COLLECT = """import json

from httk.core import TotalEnergyRecord
from httk.core.property_records import CartesianSiteForcesRecord


def collect(record):
    mode = record.parameter("collect_mode", "ok")
    if mode == "raise":
        raise RuntimeError("the collect hook exploded")
    if mode == "staged":
        assert record.result_file("CONTCAR", data_prefix="relax").read_text() == "relaxed"
        data = json.loads(record.result_file("OUTCAR", data_prefix="static").read_text())
    else:
        data = json.loads(record.result_file("forces.json").read_text())
    # The roles come out in another order than the interface lists them.
    outputs = {"energy": TotalEnergyRecord(data["energy"]), "site_forces": CartesianSiteForcesRecord.from_value(data["forces"])}
    if mode == "missing":
        del outputs["site_forces"]
    if mode == "wrong":
        outputs["site_forces"] = TotalEnergyRecord(1.0)
    return outputs
"""

_COLLECT_EXEC = f"""#!/usr/bin/env python3
import json
import sys
from pathlib import Path

sys.path.insert(0, {_SRC!r})

from httk.workflow.hookapi import collect_main


def collect(record):
    workdir = Path(record["workspace"], record["workdir_path"])
    data = json.loads((workdir / "forces.json").read_text())
    return {{"site_forces": {{"value": data["forces"]}}, "energy": {{"value": data["energy"]}}}}


collect_main(collect)
"""

_PARENT = f'''#!/usr/bin/env python3
import json
import sys

sys.path.insert(0, {_SRC!r})

import httk.core
from httk.workflow import Runner

run = Runner("tests.rt.parent")


@run.step
def start(a):
    for index, case in enumerate(a.parameter("cases")):
        value = case if a.parameter("by_path", False) else httk.core.load(case)
        inputs = dict() if a.parameter("no_input", False) else {{a.parameter("input_name", "structure"): value}}
        a.call("forces", label=f"c{{index}}", inputs=inputs, parameters=a.parameter("child_parameters", {{}}))
    a.gather("analyse", when="all_succeeded")


@run.step
def analyse(a):
    children = a.spawned.succeeded if a.parameter("spawned", False) else a.children.succeeded
    outputs = {{child.label: dict(a.child_outputs(child, call="forces")) for child in children}}
    (a.workdir / "outputs.json").write_text(json.dumps(outputs, sort_keys=True))
    a.succeed()


raise SystemExit(run.main())
'''

_FIXED_PARENT = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.rt.fixed")


@run.step
def start(a):
    a.call("middle", label="m")
    a.gather("analyse", when="all_terminal")


@run.step
def analyse(a):
    a.succeed()


raise SystemExit(run.main())
'''

_BASH_MIDDLE = """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.rt.middle start
step_start() { httk_workflow_succeed; }
httk_workflow_main
"""

_BASH_PARENT = """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.rt.parent start analyse
step_start() {
    httk_workflow_call c0 forces --input "structure=$(httk_workflow_parameter path)"
    httk_workflow_gather analyse
}
step_analyse() { httk_workflow_succeed; }
httk_workflow_main
"""


def _cif(*sites: str) -> str:
    head = (
        "data_x\n_cell_length_a 5.6\n_cell_length_b 5.6\n_cell_length_c 5.6\n_cell_angle_alpha 90\n"
        "_cell_angle_beta 90\n_cell_angle_gamma 90\n_symmetry_space_group_name_H-M 'P 1'\n"
        "loop_\n_symmetry_equiv_pos_as_xyz\nx,y,z\nloop_\n_atom_site_label\n_atom_site_type_symbol\n"
        "_atom_site_fract_x\n_atom_site_fract_y\n_atom_site_fract_z\n"
    )
    return head + "".join(f"{site}\n" for site in sites)


CONTIGUOUS = _cif("Na1 Na 0 0 0", "Na2 Na 0.5 0.5 0", "Cl1 Cl 0.5 0.5 0.5", "Cl2 Cl 0 0 0.5")
INTERLEAVED = _cif("Na1 Na 0 0 0", "Cl1 Cl 0.5 0.5 0.5", "Na2 Na 0.5 0.5 0", "Cl2 Cl 0 0 0.5")
THIRDS = _cif("Na1 Na 0 0 0", "Na2 Na 1/3 2/3 0", "Cl1 Cl 0.5 0.5 0.5", "Cl2 Cl 0 0 0.5")
#: The callee's forces of each case: its fractional positions in the given site order.
EXPECTED = {
    "contiguous": [[0.0, 0.0, 0.0], [0.5, 0.5, 0.0], [0.5, 0.5, 0.5], [0.0, 0.0, 0.5]],
    "thirds": [[0.0, 0.0, 0.0], [1 / 3, 2 / 3, 0.0], [0.5, 0.5, 0.5], [0.0, 0.0, 0.5]],
}


def _write(root: Path, manifest: str, files: dict[str, str]) -> Path:
    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    for name, text in files.items():
        (root / name).write_text(text, encoding="utf-8")
        (root / name).chmod(0o755)
    return root


def _callee(root: Path, *, product_of: str = "structure", executable: bool = False) -> Path:
    manifest = (
        '[workflow]\nname = "tests.rt.pair"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["start", "finish"]\n'
        f'\n[workflow.inputs.structure]\ndestination = "POSCAR"\nentry_type = "structures"\nref = "{STRUCTURE}"\n'
        f'\n[workflow.outputs.energy]\nentry_type = "records"\nref = "{ENERGY}"\nproduct_of = "structure"\n'
        f'\n[workflow.outputs.site_forces]\nentry_type = "records"\nref = "{FORCES}"\nproduct_of = "{product_of}"\n'
        '\n[workflow.parameters.scale]\ntype = "number"\ndefault = 1.0\n'
        f'\n[workflow.collect]\nfile = "{"collect" if executable else "collect.py"}"\n'
    )
    if product_of != "structure":
        manifest += '\n[workflow.outputs.relaxed]\nentry_type = "structures"\nproduct_of = "structure"\n'
    collect = {"collect": _COLLECT_EXEC} if executable else {"collect.py": _COLLECT}
    return _write(root, manifest, {"run.py": _CALLEE, **collect})


def _parent(root: Path, *, bash: bool = False) -> Path:
    runner = {"run": _BASH_PARENT} if bash else {"run.py": _PARENT}
    manifest = (
        '[workflow]\nname = "tests.rt.parent"\n\n[workflow.runner]\n'
        f'{"" if bash else "entry = \"run.py\"\n"}steps = ["start", "analyse"]\n'
        '\n[workflow.calls.forces]\ndeclaration_file = "forces.json"\n'
    )
    directory = _write(root, manifest, runner)
    (directory / "forces.json").write_text(json.dumps(INTERFACE), encoding="utf-8")
    return directory


def _install(workspace: Workspace, source: Path) -> _store.Installed:
    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        return _store.install(workspace, owner, source)
    finally:
        owner.close()


@pytest.fixture(autouse=True)
def _fresh() -> Iterator[None]:
    yield
    for name in ("tests.rt.parent", "tests.rt.pair", "tests.rt.middle", "tests.rt.fixed"):
        scaffold._WORKFLOW_PROVIDERS.pop(name, None)


@pytest.fixture()
def workspace(tmp_path: Path) -> Workspace:
    return Workspace.initialize(tmp_path / "workspace", durable=False)


def _cases(tmp_path: Path, **texts: str) -> list[str]:
    paths = []
    for name, text in texts.items():
        path = tmp_path / f"{name}.cif"
        path.write_text(text, encoding="utf-8")
        paths.append(str(path))
    return paths


def _run(
    tmp_path: Path,
    workspace: Workspace,
    cases: list[str],
    *,
    call_parameters: dict[str, object] | None = None,
    executable: bool = False,
    **parameters: object,
) -> _kernel.JobRef:
    callee = _install(workspace, _callee(tmp_path / "callee", executable=executable))
    parent = _install(workspace, _parent(tmp_path / "parent"))
    job = new_job(
        workspace,
        parent.id,
        parameters={"cases": cases, **parameters},
        calls={"forces": callee.id},
        call_parameters={"forces": call_parameters or {}},
    )
    run_manager(workspace)
    return find(workspace, job.job_id)


def _children(workspace: Workspace, parent: _kernel.JobRef) -> list[_kernel.JobRef]:
    return sorted((ref for ref in every_job(workspace) if ref.job_id != parent.job_id), key=lambda ref: ref.job_key)


def _error(ref: _kernel.JobRef) -> str:
    return "".join(path.read_text(encoding="utf-8") for path in ref.path.glob("attempts/*/error.json"))


def _failure(ref: _kernel.JobRef) -> dict[str, object]:
    doc, _damaged = read_state_unowned(ref.path / "state.json")
    assert doc is not None and doc.failure is not None
    return dict(doc.failure)


# -- calling --------------------------------------------------------------------------------------------------------


def test_a_slot_call_translates_inputs_persists_outputs_and_reads_them_back(
    tmp_path: Path, workspace: Workspace
) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS, thirds=THIRDS))
    assert parent.state == "succeeded", _error(parent)
    children = _children(workspace, parent)
    for child in children:
        job = JobDefinition.from_path(child.path / "job.json")
        assert job.as_mapping()["persist_outputs"] == {"forces": "site_forces"} and job.calls is None
        assert child.state == "succeeded" and (child.path / "files" / "POSCAR").is_file()
        document = json.loads((child.path / ".httk-job" / "outputs.json").read_text(encoding="utf-8"))
        assert document["format"] == "httk-workflow-outputs" and document["format_version"] == 1
        assert set(document["outputs"]) == {"forces"} and document["outputs"]["forces"]["role"] == "site_forces"
        assert document["outputs"]["forces"]["definition"] == FORCES
    outputs = json.loads((parent.path / "run" / "outputs.json").read_text(encoding="utf-8"))
    assert outputs["c0"]["forces"] == EXPECTED["contiguous"]
    flat = [value for row in outputs["c1"]["forces"] for value in row]
    assert flat == pytest.approx([value for row in EXPECTED["thirds"] for value in row], abs=1e-12)


def test_the_binding_parameters_reach_the_child_and_a_second_value_is_refused(
    tmp_path: Path, workspace: Workspace
) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS), call_parameters={"scale": 2.0})
    assert parent.state == "succeeded", _error(parent)
    outputs = json.loads((parent.path / "run" / "outputs.json").read_text(encoding="utf-8"))
    assert outputs["c0"]["forces"][1] == [1.0, 1.0, 0.0]
    clash = _run(
        tmp_path / "again",
        Workspace.initialize(tmp_path / "again" / "workspace", durable=False),
        _cases(tmp_path, contiguous=CONTIGUOUS),
        call_parameters={"scale": 2.0},
        child_parameters={"scale": 3.0},
    )
    assert clash.state == "failed" and "parameter 'scale' is already given by the job's binding" in _error(clash)


@pytest.mark.parametrize(
    ("parameters", "message"),
    [
        ({"input_name": "cell"}, "call slot 'forces' has no input 'cell'; its inputs: structure"),
        ({"no_input": True}, "call slot 'forces' needs every input; missing: structure"),
    ],
)
def test_slot_inputs_are_named_by_the_interface(
    tmp_path: Path, workspace: Workspace, parameters: dict[str, Any], message: str
) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS), **parameters)
    assert parent.state == "failed" and message in _error(parent)


@pytest.mark.parametrize("by_path", [False, True], ids=["object", "path"])
def test_the_site_guard_refuses_a_structure_the_staged_file_reorders(
    tmp_path: Path, workspace: Workspace, by_path: bool
) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, interleaved=INTERLEAVED), by_path=by_path)
    assert parent.state == "failed"
    assert "call slot 'forces' input for 'structure': species order differs at site 1" in _error(parent)
    assert "species-contiguously" in _error(parent)


def test_a_path_input_is_restaged_in_the_callees_format(tmp_path: Path, workspace: Workspace) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS), by_path=True)
    assert parent.state == "succeeded", _error(parent)
    [child] = _children(workspace, parent)
    staged = (child.path / "files" / "POSCAR").read_text(encoding="utf-8").splitlines()
    assert staged[5:7] == ["Na Cl", "2 2"] and "data_x" not in staged[0]


def test_a_bound_workflow_that_no_longer_fits_is_refused_at_the_call(tmp_path: Path, workspace: Workspace) -> None:
    callee = _install(workspace, _callee(tmp_path / "callee"))
    parent = _install(workspace, _parent(tmp_path / "parent"))
    job = new_job(
        workspace,
        parent.id,
        parameters={"cases": _cases(tmp_path, contiguous=CONTIGUOUS)},
        calls={"forces": callee.id},
    )
    # Reinstalled under the same id, its forces are now a product of the relaxed structure: no fit.
    assert _install(workspace, _callee(tmp_path / "relaxing", product_of="relaxed")).id == callee.id
    run_manager(workspace)
    failed = find(workspace, job.job_id)
    assert failed.state == "failed"
    assert f"call slot 'forces' bound to {callee.id}: interface output 'forces'" in _error(failed)


def test_a_bash_parent_calls_through_a_slot_with_a_path(tmp_path: Path, workspace: Workspace) -> None:
    callee = _install(workspace, _callee(tmp_path / "callee"))
    parent = _install(workspace, _parent(tmp_path / "parent", bash=True))
    [path] = _cases(tmp_path, contiguous=CONTIGUOUS)
    job = new_job(workspace, parent.id, parameters={"path": path}, calls={"forces": callee.id})
    run_manager(workspace)
    ref = find(workspace, job.job_id)
    assert ref.state == "succeeded", _error(ref)
    [child] = _children(workspace, ref)
    assert (child.path / "files" / "POSCAR").read_text(encoding="utf-8").splitlines()[5] == "Na Cl"
    document = json.loads((child.path / ".httk-job" / "outputs.json").read_text(encoding="utf-8"))
    assert document["outputs"]["forces"]["value"] == EXPECTED["contiguous"]


def test_a_fixed_callee_with_slots_is_called_with_its_defaults(tmp_path: Path, workspace: Workspace) -> None:
    callee = _install(workspace, _callee(tmp_path / "callee"))
    middle = _write(
        tmp_path / "middle",
        '[workflow]\nname = "tests.rt.middle"\n\n[workflow.runner]\nsteps = ["start"]\n'
        f'\n[workflow.calls.forces]\ndeclaration_file = "forces.json"\ndefault = "{callee.id}"\n',
        {"run": _BASH_MIDDLE},
    )
    (middle / "forces.json").write_text(json.dumps(INTERFACE), encoding="utf-8")
    installed_middle = _install(workspace, middle)
    fixed = _write(
        tmp_path / "fixed",
        '[workflow]\nname = "tests.rt.fixed"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["start", "analyse"]\n'
        f'\n[workflow.calls]\nmiddle = "{installed_middle.id}"\n',
        {"run.py": _FIXED_PARENT},
    )
    job = new_job(workspace, _install(workspace, fixed).id)
    run_manager(workspace)
    parent = find(workspace, job.job_id)
    assert parent.state == "succeeded", _error(parent)
    [child] = _children(workspace, parent)
    calls = JobDefinition.from_path(child.path / "job.json").as_mapping()["calls"]
    assert calls == {"forces": {"workflow": callee.id, "parameters": {}}}


def test_child_outputs_reads_a_spawned_child(tmp_path: Path, workspace: Workspace) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS), spawned=True)
    assert parent.state == "succeeded", _error(parent)
    outputs = json.loads((parent.path / "run" / "outputs.json").read_text(encoding="utf-8"))
    assert outputs["c0"]["forces"] == EXPECTED["contiguous"]


# -- persisting -----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("missing", "the collect hook returned no output role 'site_forces'"),
        ("wrong", f"output role 'site_forces' holds a {ENERGY} value, not the declared {FORCES}"),
        ("raise", "the collect hook exploded"),
    ],
)
def test_a_child_whose_outputs_are_unavailable_fails(
    tmp_path: Path, workspace: Workspace, mode: str, message: str
) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS), call_parameters={"collect_mode": mode})
    [child] = _children(workspace, parent)
    failure = _failure(child)
    assert failure["code"] == "workflow.outputs_unavailable" and message in str(failure["message"])
    assert not (child.path / ".httk-job" / "outputs.json").exists()


@pytest.mark.parametrize("role", ["nope", "relaxed"])
def test_a_role_that_cannot_be_recorded_fails_before_the_hook_runs(
    tmp_path: Path, workspace: Workspace, role: str
) -> None:
    callee = _install(workspace, _callee(tmp_path / "callee", product_of="relaxed"))
    [path] = _cases(tmp_path, contiguous=CONTIGUOUS)
    payload = tmp_path / "payload"
    payload.mkdir()
    # The hook would raise if it ran; the role is refused first.
    scaffold.scaffold_job(
        workspace, callee.id, payload, inputs={"structure": httk.core.load(path)}, parameters={"collect_mode": "raise"}
    )
    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        ref = scaffold.submit_payload(workspace, owner, payload, move=False, changes={"persist_outputs": {"f": role}})
    finally:
        owner.close()
    run_manager(workspace)
    failure = _failure(find(workspace, ref.job_id))
    assert failure["code"] == "workflow.outputs_unavailable"
    assert f"output role {role!r} is no records output with a ref" in str(failure["message"])
    assert "exploded" not in str(failure["message"])


def test_an_unlocked_job_persists_outputs_with_the_collect_hook_of_its_edited_copy(
    tmp_path: Path, workspace: Workspace
) -> None:
    from httk.workflow import removal

    callee = _install(workspace, _callee(tmp_path / "callee"))
    [path] = _cases(tmp_path, contiguous=CONTIGUOUS)
    payload = tmp_path / "payload"
    payload.mkdir()
    scaffold.scaffold_job(
        workspace, callee.id, payload, inputs={"structure": httk.core.load(path)}, parameters={"collect_mode": "raise"}
    )
    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        changes = {"persist_outputs": {"forces": "site_forces"}}
        ref = scaffold.submit_payload(workspace, owner, payload, move=False, changes=changes)
    finally:
        owner.close()
    run_manager(workspace)
    assert "exploded" in str(_failure(find(workspace, ref.job_id))["message"])

    with _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={}) as owner:
        assert removal.request_now(workspace, owner, find(workspace, ref.job_id), "unlock", "debug") is None
        hook = find(workspace, ref.job_id).path / _store.RUNNER_COPY / "collect.py"
        hook.write_text(hook.read_text(encoding="utf-8").replace('mode == "raise"', "False"), encoding="utf-8")
        assert removal.request_now(workspace, owner, find(workspace, ref.job_id), "restart", "debug") is None
    run_manager(workspace)
    done = find(workspace, ref.job_id)
    assert done.state == "succeeded", _error(done)
    document = json.loads((done.path / ".httk-job" / "outputs.json").read_text(encoding="utf-8"))
    assert document["outputs"]["forces"]["value"] == EXPECTED["contiguous"]


def test_an_unreadable_path_names_the_slot_input(tmp_path: Path, workspace: Workspace) -> None:
    parent = _run(tmp_path, workspace, [str(tmp_path / "absent.cif")], by_path=True)
    assert parent.state == "failed" and "call slot 'forces' input 'structure': " in _error(parent)


def test_outputs_published_through_a_transaction_in_the_same_step_are_read(
    tmp_path: Path, workspace: Workspace
) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS), call_parameters={"publish": True})
    assert parent.state == "succeeded", _error(parent)
    [child] = _children(workspace, parent)
    assert (child.path / "data" / "forces.json").is_file()
    # Data an earlier step published is not read in place of what the succeeding step publishes.
    again = Workspace.initialize(tmp_path / "again" / "workspace", durable=False)
    cases = _cases(tmp_path, contiguous=CONTIGUOUS)
    parent = _run(tmp_path / "again", again, cases, call_parameters={"publish": True, "stale": True})
    assert parent.state == "succeeded", _error(parent)
    outputs = json.loads((parent.path / "run" / "outputs.json").read_text(encoding="utf-8"))
    assert outputs["c0"]["forces"] == EXPECTED["contiguous"]


def test_outputs_staged_beside_data_of_an_earlier_step_are_read(tmp_path: Path, workspace: Workspace) -> None:
    cases = _cases(tmp_path, contiguous=CONTIGUOUS)
    parent = _run(tmp_path, workspace, cases, call_parameters={"staged": True, "collect_mode": "staged"})
    assert parent.state == "succeeded", _error(parent)
    outputs = json.loads((parent.path / "run" / "outputs.json").read_text(encoding="utf-8"))
    assert outputs["c0"]["forces"] == EXPECTED["contiguous"]
    [child] = _children(workspace, parent)
    assert {path.name for path in (child.path / "data").rglob("*") if path.is_file()} == {"CONTCAR", "OUTCAR"}


@pytest.mark.parametrize("no_store", [False, True], ids=["store", "no-store"])
def test_an_executable_collect_hook_persists_outputs(tmp_path: Path, workspace: Workspace, no_store: bool) -> None:
    cases = _cases(tmp_path, contiguous=CONTIGUOUS)
    parent = _run(tmp_path, workspace, cases, executable=True, call_parameters={"no_store": no_store})
    assert parent.state == "succeeded", _error(parent)
    outputs = json.loads((parent.path / "run" / "outputs.json").read_text(encoding="utf-8"))
    assert outputs["c0"]["forces"] == EXPECTED["contiguous"]


def test_a_job_without_persisted_outputs_runs_no_hook(tmp_path: Path, workspace: Workspace) -> None:
    callee = _install(workspace, _callee(tmp_path / "callee"))
    [path] = _cases(tmp_path, contiguous=CONTIGUOUS)
    # Called directly, the collect hook would raise; nothing runs it.
    structure = httk.core.load(path)
    job = new_job(workspace, callee.id, inputs={"structure": structure}, parameters={"collect_mode": "raise"})
    run_manager(workspace)
    ref = find(workspace, job.job_id)
    assert ref.state == "succeeded" and not (ref.path / ".httk-job" / "outputs.json").exists()


# -- reading --------------------------------------------------------------------------------------------------------


def test_tampered_outputs_are_refused_and_a_retried_analysis_reads_the_same_values(
    tmp_path: Path, workspace: Workspace
) -> None:
    parent = _run(tmp_path, workspace, _cases(tmp_path, contiguous=CONTIGUOUS))
    assert parent.state == "succeeded", _error(parent)
    first = (parent.path / "run" / "outputs.json").read_text(encoding="utf-8")
    [child] = _children(workspace, parent)

    outputs_path = child.path / ".httk-job" / "outputs.json"
    original = outputs_path.read_text(encoding="utf-8")
    result = ChildResult(
        "c0",
        child.job_id,
        child.job_key,
        "succeeded",
        None,
        JobDefinition.from_path(child.path / "job.json").placement,
        child.path,
        None,
        None,
        {},
    )
    # The parent's analyse step, read again outside its attempt: only its installation and workflow id are used.
    attempt = object.__new__(Attempt)
    attempt._own = _store.lookup(workspace, "local:tests.rt.parent")
    attempt.context = SimpleNamespace(workflow_id="local:tests.rt.parent")  # type: ignore[assignment]
    assert dict(attempt.child_outputs(result, call="forces")) == json.loads(first)["c0"]
    tampered = json.loads(original)
    tampered["outputs"]["forces"]["definition"] = ENERGY
    outputs_path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ValueError, match=f"output 'forces' is not a recorded {FORCES} value"):
        attempt.child_outputs(result, call="forces")
    with pytest.raises(ValueError, match="'other' is no call slot"):
        attempt.child_outputs(result, call="other")
    outputs_path.write_text(original, encoding="utf-8")
    # Reading again (as a retried analyse step would) gives identical values.
    assert dict(attempt.child_outputs(result, call="forces")) == json.loads(first)["c0"]
