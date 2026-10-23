"""Call slots: ``[workflow.calls.<ALIAS>]`` tables, their interface declarations, and whether a workflow fits one."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from httk.workflow._slots import SlotMismatch, fit, validate_interface
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.scaffold import CallSlot, WorkflowProvider
from httk.workflow.workflow_cli._describe import _render_text, _workflow_description
from test_workflow_packages import _package

STRUCTURE = "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures"
FORCES = "https://schemas.httk.org/defs/v0.1/properties/core/cartesian_site_forces"
INTERFACE: dict[str, object] = {
    "$id": "https://example.test/interfaces/structure-forces",
    "title": "structure to forces",
    "inputs": [{"name": "structure", "entry_type": "structures", "ref": STRUCTURE}],
    "outputs": [{"name": "forces", "entry_type": "records", "ref": FORCES, "product_of": "structure"}],
}
_HEAD = '[workflow]\nname = "{name}"\n\n[workflow.runner]\nsteps = ["start"]\n'
_STRUCTURE_INPUT = (
    '\n[workflow.inputs.{key}]\ndestination = "{key}.vasp"\nentry_type = "structures"\nref = "' + STRUCTURE + '"\n'
)


def _forces(product_of: str = "initial_structure", role: str = "forces", ref: str = FORCES) -> str:
    return f'\n[workflow.outputs.{role}]\nentry_type = "records"\nref = "{ref}"\nproduct_of = "{product_of}"\n'


def _slot_package(root: Path, table: str, interface: object = INTERFACE, calls: str = "") -> Path:
    """Write a package with *table* as its ``[workflow.calls.forces]`` body and the interface it names."""

    _package(
        root, _HEAD.format(name="tests.slots.caller") + f"\n[workflow.calls]\n{calls}\n[workflow.calls.forces]\n{table}"
    )
    (root / "calls").mkdir()
    (root / "calls" / "forces.json").write_text(json.dumps(interface), encoding="utf-8")
    return root


def _callee(root: Path, body: str = _STRUCTURE_INPUT.format(key="initial_structure") + _forces()) -> WorkflowProvider:
    return parse_workflow_manifest(_package(root, _HEAD.format(name="tests.slots.callee") + body))


def test_a_calls_table_is_a_slot_and_a_string_stays_a_fixed_call(tmp_path: Path) -> None:
    root = _slot_package(
        tmp_path / "caller",
        'declaration_file = "calls/forces.json"\ndescription = "forces on every site"\ndefault = "tests.ase"\n',
        calls='child = "tests.child"\n',
    )
    provider = parse_workflow_manifest(root)
    assert provider.calls == {"child": "tests.child"}
    assert provider.slots == {"forces": CallSlot(INTERFACE, "calls/forces.json", "forces on every site", "tests.ase")}
    assert parse_workflow_manifest(_package(tmp_path / "plain", _HEAD.format(name="tests.plain"))).slots is None


@pytest.mark.parametrize(
    ("table", "calls", "message"),
    [
        (
            'declaration_file = "calls/forces.json"\ncolour = "red"\n',
            "",
            r"unknown key \[workflow\.calls\.forces\.colour\]",
        ),
        ('description = "x"\n', "", r"\[workflow\.calls\.forces\]\.declaration_file must name a relative regular file"),
        ('declaration_file = "calls/none.json"\n', "", "member does not exist"),
        ('declaration_file = "../forces.json"\n', "", "without '..'"),
        ('declaration_file = "calls/forces.json"\ndefault = "../x"\n', "", "not a path"),
        ('declaration_file = "calls/forces.json"\ndefault = "child"\n', 'child = "tests.child"\n', "alias 'child'"),
        ('declaration_file = "calls/forces.json"\n', 'child = "forces"\n', "alias 'forces' is also another call's"),
        (
            'declaration_file = "calls/forces.json"\ndefault_parameters = {cutoff = 1}\n',
            "",
            r"\[workflow\.calls\.forces\]\.default_parameters needs a default",
        ),
        (
            'declaration_file = "calls/forces.json"\ndefault = "tests.ase"\ndefault_parameters = 3\n',
            "",
            r"default_parameters must be a table",
        ),
        (
            'declaration_file = "calls/forces.json"\ndefault = "tests.ase"\ndefault_parameters = {cutoff = nan}\n',
            "",
            r"default_parameters must hold finite values",
        ),
    ],
)
def test_a_malformed_slot_table_is_refused(tmp_path: Path, table: str, calls: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(_slot_package(tmp_path / "caller", table, calls=calls))


def _entry(kind: str, **changes: object) -> dict[str, object]:
    entries = INTERFACE[kind]
    assert isinstance(entries, list)
    return {**entries[0], **changes}


@pytest.mark.parametrize(
    ("interface", "message"),
    [
        ([], "must be a JSON object"),
        ({**INTERFACE, "version": "1"}, "unknown interface key 'version'"),
        ({**INTERFACE, "inputs": []}, "inputs must be a nonempty array"),
        ({**INTERFACE, "inputs": [{"name": "structure", "entry_type": "structures"}]}, "needs ref"),
        ({**INTERFACE, "outputs": [_entry("outputs", entry_type="structures")]}, "must have entry_type 'records'"),
        ({**INTERFACE, "outputs": [{"name": "forces", "entry_type": "records", "ref": FORCES}]}, "needs product_of"),
        ({**INTERFACE, "outputs": [_entry("outputs", product_of="cell")]}, "product_of names no interface input"),
        ({**INTERFACE, "inputs": [_entry("inputs"), _entry("inputs")]}, "name 'structure' is duplicated"),
        ({**INTERFACE, "inputs": [_entry("inputs", name="no spaces")]}, "invalid component syntax"),
        ({**INTERFACE, "outputs": [_entry("outputs", unit="eV")]}, "unknown key 'unit'"),
    ],
)
def test_a_malformed_interface_is_refused(tmp_path: Path, interface: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_interface(interface)
    with pytest.raises(ValueError, match=r"\[workflow\.calls\.forces\]\.declaration_file 'calls/forces\.json'"):
        parse_workflow_manifest(
            _slot_package(tmp_path / "caller", 'declaration_file = "calls/forces.json"\n', interface)
        )


def test_an_unreadable_interface_is_refused(tmp_path: Path) -> None:
    root = _slot_package(tmp_path / "caller", 'declaration_file = "calls/forces.json"\n')
    (root / "calls" / "forces.json").write_text("{", encoding="utf-8")
    with pytest.raises(ValueError, match="declaration_file 'calls/forces.json'"):
        parse_workflow_manifest(root)


def test_an_exact_fit_maps_interface_names_onto_input_keys_and_output_roles(tmp_path: Path) -> None:
    body = (
        '\n[workflow.inputs.poscar]\ndestination = "POSCAR"\nrole = "initial_structure"\nentry_type = "structures"\n'
        f'ref = "{STRUCTURE}"\n[workflow.inputs.notes]\ndestination = "notes.txt"\n' + _forces()
    )
    mapping = fit(INTERFACE, _callee(tmp_path / "callee", body))
    assert dict(mapping.inputs) == {"structure": "poscar"} and dict(mapping.outputs) == {"forces": "forces"}


def test_a_candidate_named_like_the_interface_entry_is_chosen(tmp_path: Path) -> None:
    body = (
        _STRUCTURE_INPUT.format(key="reference")
        + "required = false\n"
        + _STRUCTURE_INPUT.format(key="structure")
        + _forces("structure")
    )
    assert dict(fit(INTERFACE, _callee(tmp_path / "callee", body)).inputs) == {"structure": "structure"}


def test_several_unnamed_candidates_are_ambiguous(tmp_path: Path) -> None:
    body = _STRUCTURE_INPUT.format(key="a") + _STRUCTURE_INPUT.format(key="b") + _forces("a")
    with pytest.raises(SlotMismatch, match=r"interface input 'structure' matches several inputs .*\(a, b\)"):
        fit(INTERFACE, _callee(tmp_path / "callee", body))


def test_two_interface_inputs_cannot_share_one_workflow_input(tmp_path: Path) -> None:
    interface = {**INTERFACE, "inputs": [_entry("inputs"), _entry("inputs", name="other")]}
    body = _STRUCTURE_INPUT.format(key="structure") + _forces("structure")
    with pytest.raises(SlotMismatch, match="interface inputs other, structure all map to the workflow input"):
        fit(interface, _callee(tmp_path / "callee", body))


@pytest.mark.parametrize(
    "extra",
    [
        '\n[workflow.inputs.kpoints]\ndestination = "KPOINTS"\nentry_type = "files"\n',
        '\n[workflow.inputs.kpoints]\ndestination = "KPOINTS"\nrequired = true\n',
        '\n[workflow.inputs.kpoints]\nrequired = true\n\n[workflow.instantiate]\nfile = "instantiate.py"\n',
    ],
    ids=["typed", "untyped", "hook-consumed"],
)
def test_every_required_workflow_input_must_be_supplied(tmp_path: Path, extra: str) -> None:
    body = _STRUCTURE_INPUT.format(key="initial_structure") + _forces() + extra
    with pytest.raises(SlotMismatch, match="required input 'kpoints' is supplied by no interface input"):
        fit(INTERFACE, _callee(tmp_path / "callee", body))


def test_forces_on_a_relaxed_structure_do_not_fit_forces_on_the_input(tmp_path: Path) -> None:
    body = (
        _STRUCTURE_INPUT.format(key="initial_structure")
        + '\n[workflow.outputs.relaxed]\nentry_type = "structures"\nproduct_of = "initial_structure"\n'
        + _forces("relaxed")
    )
    with pytest.raises(SlotMismatch, match="product_of its input 'initial_structure'.*another: forces"):
        fit(INTERFACE, _callee(tmp_path / "callee", body))


def test_a_ref_of_another_version_does_not_fit(tmp_path: Path) -> None:
    body = _STRUCTURE_INPUT.format(key="initial_structure").replace("v1.3", "v1.2") + _forces()
    with pytest.raises(SlotMismatch, match="interface input 'structure' matches no input"):
        fit(INTERFACE, _callee(tmp_path / "callee", body))


def test_every_reason_is_reported_together_including_nested_slots(tmp_path: Path) -> None:
    caller = parse_workflow_manifest(_slot_package(tmp_path / "caller", 'declaration_file = "calls/forces.json"\n'))
    with pytest.raises(SlotMismatch) as raised:
        fit(INTERFACE, caller)
    assert len(raised.value.reasons) == 2 and str(raised.value) == "; ".join(raised.value.reasons)
    assert "nested call slots are not supported" in raised.value.reasons[0]
    assert "interface input 'structure' matches no input" in raised.value.reasons[1]


def test_an_external_declaration_supplies_the_semantics(tmp_path: Path) -> None:
    body = '\n[workflow.inputs.initial_structure]\ndestination = "POSCAR"\nentry_type = "structures"\n' + _forces()
    with pytest.raises(SlotMismatch):
        fit(INTERFACE, _callee(tmp_path / "plain", body))
    root = _package(tmp_path / "declared", _HEAD.format(name="tests.slots.callee") + body)
    declaration = {
        "inputs": [{"name": "initial_structure", "entry_type": "structures", "ref": STRUCTURE}],
        "outputs": [{"name": "forces", "entry_type": "records", "ref": FORCES}],
    }
    (root / "declaration.json").write_text(json.dumps(declaration), encoding="utf-8")
    text = (root / "httk_workflow.toml").read_text(encoding="utf-8")
    (root / "httk_workflow.toml").write_text(
        text.replace("[workflow.runner]", 'declaration_file = "declaration.json"\n\n[workflow.runner]'),
        encoding="utf-8",
    )
    assert dict(fit(INTERFACE, parse_workflow_manifest(root)).inputs) == {"structure": "initial_structure"}


@pytest.mark.parametrize("kind", ["inputs", "outputs"])
def test_an_external_declaration_ref_must_agree_with_the_manifest(tmp_path: Path, kind: str) -> None:
    root = _package(
        tmp_path / "callee",
        _HEAD.format(name="tests.slots.callee").replace(
            "[workflow.runner]", 'declaration_file = "d.json"\n\n[workflow.runner]'
        )
        + _STRUCTURE_INPUT.format(key="initial_structure")
        + _forces(),
    )
    declaration = {
        "inputs": [{"name": "initial_structure", "entry_type": "structures", "ref": STRUCTURE}],
        "outputs": [{"name": "forces", "entry_type": "records", "ref": FORCES}],
    }
    entry = declaration[kind][0]
    entry["ref"] = str(entry["ref"]) + "-other"
    (root / "d.json").write_text(json.dumps(declaration), encoding="utf-8")
    role = entry["name"]
    with pytest.raises(
        ValueError, match=f"external declaration {kind[:-1]} '{role}' has ref .*-other', but the manifest"
    ):
        parse_workflow_manifest(root)
    del entry["ref"]  # a ref only the manifest gives is fine
    (root / "d.json").write_text(json.dumps(declaration), encoding="utf-8")
    parse_workflow_manifest(root)


def test_describe_shows_fixed_calls_and_slots(tmp_path: Path) -> None:
    root = _slot_package(
        tmp_path / "caller",
        'declaration_file = "calls/forces.json"\ndescription = "forces on every site"\n',
        calls='child = "tests.child"\n',
    )
    calls = _workflow_description(str(root))["calls"]
    assert calls == {
        "fixed": {"child": "tests.child"},
        "slots": {
            "forces": {
                "description": "forces on every site",
                "default": None,
                "default_parameters": None,
                "interface": {
                    "$id": INTERFACE["$id"],
                    "title": "structure to forces",
                    "inputs": [{"name": "structure", "entry_type": "structures", "ref": STRUCTURE}],
                    "outputs": [{"name": "forces", "entry_type": "records", "ref": FORCES, "product_of": "structure"}],
                },
            }
        },
    }
    text = _render_text(_workflow_description(str(root)))
    assert "  child: tests.child\n  forces: slot 'structure to forces', default=- — forces on every site\n" in text
    assert f"    out forces: records {FORCES} (product_of structure)" in text


def test_an_implicitly_required_typed_input_must_be_supplied(tmp_path: Path) -> None:
    """Without a ``required`` flag, an input declaring an entry_type is required (as a registered provider has it)."""

    callee = _callee(tmp_path / "callee")
    metadata = {key: dict(value) for key, value in callee._input_metadata.items()}
    metadata["cell"] = {"role": "cell", "entry_type": "structures", "ref": "https://example.test/cell"}
    for value in metadata.values():
        value.pop("required", None)
    bare = replace(callee, inputs={**callee.inputs, "cell": "cell.vasp"}, _input_metadata=metadata)
    with pytest.raises(SlotMismatch, match="required input 'cell' is supplied by no interface input"):
        fit(INTERFACE, bare)


def test_describe_shows_default_parameters(tmp_path: Path) -> None:
    root = _slot_package(
        tmp_path / "caller",
        'declaration_file = "calls/forces.json"\ndefault = "tests.ase"\n'
        'default_parameters = {calculator = "emt", cutoff = 4.5}\n',
    )
    slot = parse_workflow_manifest(root).slots["forces"]  # type: ignore[index]
    assert slot.default_parameters == {"calculator": "emt", "cutoff": 4.5}
    description = _workflow_description(str(root))
    assert description["calls"]["slots"]["forces"]["default_parameters"] == {"calculator": "emt", "cutoff": 4.5}  # type: ignore[index]
    assert '    default parameters: {"calculator": "emt", "cutoff": 4.5}' in _render_text(description)
