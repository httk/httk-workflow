"""Call slots: interface declarations and whether a workflow fits one.

A ``[workflow.calls.<ALIAS>]`` table declares a call whose target the job
creator chooses. Its interface declaration is a workflow-declaration-shaped
document whose every entry names an ``entry_type`` and a definition ``ref``;
:func:`fit` decides, from metadata alone, whether a workflow can serve as the
target, and how the interface's names map onto the workflow's inputs and
outputs.
"""

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from .errors import FormatError
from .models import validate_label
from .scaffold import WorkflowProvider, _input_required

_INTERFACE_KEYS = {"$id", "title", "description", "inputs", "outputs"}
_ENTRY_KEYS = {"name", "entry_type", "ref", "description"}


@dataclass(frozen=True)
class SlotMapping:
    """How an interface's names map onto one workflow that fits it.

    :param inputs: Map each interface input name to the workflow's input key.
    :param outputs: Map each interface output name to the workflow's output role.
    """

    inputs: Mapping[str, str]
    outputs: Mapping[str, str]


class SlotMismatch(ValueError):
    """A workflow does not fit a call slot's interface.

    :param reasons: Every reason the workflow does not fit.
    """

    def __init__(self, reasons: tuple[str, ...]) -> None:
        self.reasons = reasons
        super().__init__("; ".join(reasons))


def validate_interface(document: object) -> dict[str, object]:
    """Validate one interface declaration document.

    :param document: The parsed JSON document.
    :return: The document.
    :raises ValueError: If the document is not a valid interface declaration.
    """

    if not isinstance(document, dict):
        raise ValueError("an interface declaration must be a JSON object")
    interface = cast(dict[str, object], document)
    unknown = sorted(set(interface) - _INTERFACE_KEYS)
    if unknown:
        raise ValueError(f"unknown interface key {unknown[0]!r}")
    for key in ("$id", "title", "description"):
        if key in interface and not isinstance(interface[key], str):
            raise ValueError(f"interface {key} must be a string")
    for kind in ("inputs", "outputs"):
        entries = interface.get(kind)
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"interface {kind} must be a nonempty array")
        names: set[str] = set()
        for index, entry in enumerate(cast(list[object], entries)):
            where = f"interface {kind}[{index}]"
            if not isinstance(entry, dict):
                raise ValueError(f"{where} must be an object")
            item = cast(dict[str, object], entry)
            allowed = _ENTRY_KEYS | {"product_of"} if kind == "outputs" else _ENTRY_KEYS
            extra = sorted(set(item) - allowed)
            if extra:
                raise ValueError(f"{where} has unknown key {extra[0]!r}")
            try:
                name = validate_label(item.get("name"), f"{where}.name")
            except FormatError as exc:
                raise ValueError(str(exc)) from exc
            if name in names:
                raise ValueError(f"interface {kind} name {name!r} is duplicated")
            names.add(name)
            for key in ("entry_type", "ref", "description") + (("product_of",) if kind == "outputs" else ()):
                if key == "description" and key not in item:
                    continue
                if not isinstance(item.get(key), str) or not item[key]:
                    raise ValueError(f"interface {kind} {name!r} needs {key} as a nonempty string")
            if kind == "outputs":
                if item["entry_type"] != "records":
                    raise ValueError(f"interface output {name!r} must have entry_type 'records'")
                if item["product_of"] not in _names(interface, "inputs"):
                    raise ValueError(f"interface output {name!r} product_of names no interface input")
    return interface


def _entries(document: Mapping[str, object], kind: str) -> list[Mapping[str, object]]:
    entries = document.get(kind)
    if not isinstance(entries, list):
        return []
    return [cast(Mapping[str, object], entry) for entry in cast(list[object], entries) if isinstance(entry, Mapping)]


def _names(document: Mapping[str, object], kind: str) -> set[object]:
    return {entry.get("name") for entry in _entries(document, kind)}


def interface_key(interface: Mapping[str, object]) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    """Return what an interface means, for comparing two: its entries, ignoring order and documentation.

    :param interface: A validated interface declaration.
    :return: The sorted ``(name, entry_type, ref)`` inputs and ``(name, entry_type, ref, product_of)`` outputs.
    """

    def entries(kind: str, keys: tuple[str, ...]) -> list[tuple[str, ...]]:
        return sorted(tuple(str(entry.get(key)) for key in keys) for entry in _entries(interface, kind))

    return entries("inputs", ("name", "entry_type", "ref")), entries(
        "outputs", ("name", "entry_type", "ref", "product_of")
    )


def _pick(name: str, candidates: list[str], kind: str, reasons: list[str], hint: str = "") -> str | None:
    """Choose the candidate named *name*, else the only one; record why there is none."""

    if name in candidates:
        return name
    if len(candidates) == 1:
        return candidates[0]
    if candidates:
        reasons.append(
            f"interface {kind} {name!r} matches several {kind}s of the workflow "
            f"({', '.join(sorted(candidates))}) and none is named {name!r}"
        )
    else:
        reasons.append(f"interface {kind} {name!r} matches no {kind} of the workflow with its entry_type and ref{hint}")
    return None


def _injective(mapping: Mapping[str, str], kind: str, reasons: list[str]) -> None:
    images: dict[str, list[str]] = {}
    for name, image in mapping.items():
        images.setdefault(image, []).append(name)
    for image, names in sorted(images.items()):
        if len(names) > 1:
            reasons.append(f"interface {kind}s {', '.join(sorted(names))} all map to the workflow {kind} {image!r}")


def fit(interface: Mapping[str, object], callee: WorkflowProvider) -> SlotMapping:
    """Check that a workflow fits a call slot's interface and map the interface's names onto it.

    The workflow's declaration (its external declaration when it has one) supplies the entry types
    and refs; its manifest supplies input keys, required inputs and the ``product_of`` of outputs.
    Each interface entry is matched among the workflow's entries with an equal ``entry_type`` and
    ``ref`` — the one named alike when there are several — and an output must in addition be a
    product of the input its interface ``product_of`` maps to. Both mappings must be injective, and
    every required input of the workflow must be supplied.

    :param interface: The validated interface declaration.
    :param callee: The candidate workflow.
    :return: The interface's input and output names mapped onto the workflow.
    :raises SlotMismatch: If the workflow does not fit, listing every reason.
    """

    from .packages import workflow_declaration_from_manifest

    declaration = callee.declarations.get("workflow") if callee.declaration_file is not None else None
    if declaration is None:
        declaration = workflow_declaration_from_manifest(callee)
    reasons: list[str] = []
    if callee.slots:
        reasons.append(f"workflow {callee.workflow_id} has call slots of its own; nested call slots are not supported")
    metadata = callee._input_metadata
    keys = {str(metadata.get(key, {}).get("role", key)): key for key in callee.inputs}

    roles: dict[str, str] = {}
    for entry in _entries(interface, "inputs"):
        name = str(entry["name"])
        candidates = [
            str(declared["name"])
            for declared in _entries(declaration, "inputs")
            if declared.get("entry_type") == entry["entry_type"] and declared.get("ref") == entry["ref"]
        ]
        role = _pick(name, candidates, "input", reasons)
        if role is not None:
            roles[name] = role
    _injective(roles, "input", reasons)
    inputs = {name: keys.get(role, role) for name, role in roles.items()}
    supplied = set(inputs.values())
    for key in callee.inputs:
        if _input_required(metadata.get(key, {})) and key not in supplied:
            reasons.append(f"the workflow's required input {key!r} is supplied by no interface input")

    products = {str(entry.get("role", name)): entry.get("product_of") for name, entry in callee.outputs.items()}
    outputs: dict[str, str] = {}
    for entry in _entries(interface, "outputs"):
        name = str(entry["name"])
        source = roles.get(str(entry["product_of"]))
        if source is None:
            continue  # its product_of input did not map, which is already a reason
        alike = [
            str(declared["name"])
            for declared in _entries(declaration, "outputs")
            if declared.get("entry_type") == entry["entry_type"] and declared.get("ref") == entry["ref"]
        ]
        candidates = [role for role in alike if products.get(role) == source]
        hint = ""
        if alike and not candidates:
            others = ", ".join(sorted(alike))
            hint = f" that is a product_of its input {source!r} (alike but a product of another: {others})"
        role = _pick(name, candidates, "output", reasons, hint)
        if role is not None:
            outputs[name] = role
    _injective(outputs, "output", reasons)
    if reasons:
        raise SlotMismatch(tuple(reasons))
    return SlotMapping(MappingProxyType(inputs), MappingProxyType(outputs))


#: The absolute tolerance on fractional coordinates, and the relative one on lattice vectors, of the site guard.
SITE_TOLERANCE = 1e-9


def check_staged_sites(supplied: object, staged: Path, where: str) -> None:
    """Refuse a staged structure whose sites are not the supplied structure's, site by site.

    A callee's per-site results come back in the order of the file it read, so the file must hold the
    supplied structure's sites in the same order and frame: per site the chemical symbols, fractional
    coordinates within :data:`SITE_TOLERANCE` modulo 1, and lattice vectors within :data:`SITE_TOLERANCE`
    relative to the longest one. No symmetry matching is attempted: it would accept exactly the
    permutations this guard exists to catch. A value that is not a structure is not checked.

    :param supplied: The value the caller supplied.
    :param staged: The file it was staged to.
    :param where: Name the slot input in errors.
    :raises ValueError: If the staged file holds other sites or another cell.
    """

    import httk.core

    try:
        from httk.atomistic import UnitcellStructureView
    except ImportError:  # pragma: no cover - only structures are guarded, and they need httk-atomistic
        return
    try:
        expected = UnitcellStructureView(cast(Any, supplied))
    except (TypeError, ValueError):
        return
    try:
        # Only what the file holds is compared; a stated precision keeps POSCAR from asking for one.
        loaded = httk.core.load(str(staged), precision=SITE_TOLERANCE)
    except TypeError:
        loaded = httk.core.load(str(staged))
    found = UnitcellStructureView(loaded)

    def symbols(view: Any) -> list[tuple[str, ...]]:
        by_name = {species.name: tuple(species.chemical_symbols) for species in view.species}
        return [by_name[name] for name in view.species_at_sites]

    hint = (
        "; order the sites species-contiguously (writers such as POSCAR group sites by species), "
        "so the staged file keeps the supplied order"
    )
    wanted, got = symbols(expected), symbols(found)
    if len(wanted) != len(got):
        raise ValueError(f"{where}: the staged {staged.name} has {len(got)} sites, the structure {len(wanted)}")
    for index, (left, right) in enumerate(zip(wanted, got, strict=True)):
        if left != right:
            raise ValueError(f"{where}: species order differs at site {index} ({left} staged as {right}){hint}")
    positions = zip(expected.sites.reduced_coords.to_floats(), found.sites.reduced_coords.to_floats(), strict=True)
    for index, (left, right) in enumerate(positions):
        for a, b in zip(left, right, strict=True):
            difference = a - b
            if abs(difference - math.floor(difference + 0.5)) > SITE_TOLERANCE:
                raise ValueError(f"{where}: position differs at site {index} ({left} staged as {right}){hint}")
    basis, staged_basis = expected.cell.basis.to_floats(), found.cell.basis.to_floats()
    scale = max(math.sqrt(sum(component * component for component in row)) for row in basis)
    for left, right in zip(basis, staged_basis, strict=True):
        if any(abs(a - b) > SITE_TOLERANCE * scale for a, b in zip(left, right, strict=True)):
            raise ValueError(f"{where}: lattice differs ({basis} staged as {staged_basis})")


def property_value(entry: object) -> tuple[str, object]:
    """Return the definition IRI and the plain JSON value of one property-value records entry.

    :param entry: A generated typed record, a total-energy record, or a ``DataRecord``.
    :return: The definition IRI and the value.
    :raises ValueError: If the entry is no property value (or a derived statistic of one), or not finite JSON.
    """

    from httk.core import AverageTotalEnergyRecord, DataRecord, TotalEnergyRecord
    from httk.core.data_records import AVERAGE_TOTAL_ENERGY_DEFINITION_ID, TOTAL_ENERGY_DEFINITION_ID
    from httk.core.typed_records import TypedRecord

    if isinstance(entry, TypedRecord) and entry.derivation is None:
        definition, value = entry.definition_id, entry.value
    elif isinstance(entry, TotalEnergyRecord):
        definition, value = TOTAL_ENERGY_DEFINITION_ID, entry.total_energy
    elif isinstance(entry, AverageTotalEnergyRecord):
        definition, value = AVERAGE_TOTAL_ENERGY_DEFINITION_ID, entry.average_total_energy
    elif isinstance(entry, DataRecord):
        definition, value = entry.definition_id, entry.value
    else:
        raise ValueError(f"a {type(entry).__name__} is not a property value")
    try:
        return definition, json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"the {definition} value is not finite JSON: {exc}") from exc
