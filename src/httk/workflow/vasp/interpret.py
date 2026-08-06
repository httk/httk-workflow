"""Interpret harvested VASP jobs as stored entries and run products.

Interpreter-owned artifact and output labels overlay the corresponding
``run_record`` edges: an owned label is replaced by the interpreted content id,
while unrelated declared edges survive.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httk.core

from httk.workflow.harvesting import HarvestRecord
from httk.workflow.interpretation import InterpretedRun, register_interpreter
from httk.workflow.provenance import run_record

__all__ = ["interpret_vasp_relax", "interpret_vasp_relax_static", "interpret_vasp_static"]

_TOTAL_ENERGY_DEFINITION = "https://schemas.httk.org/defs/v0.1/properties/core/total_energy"
_TOTAL_ENERGY_NAME = "_httk_total_energy"


def _identity(record: HarvestRecord) -> str:
    return f"{record.workspace_id}:{record.job_id}"


def _input(record: HarvestRecord, name: str, default: str) -> str:
    inputs = record.job.get("inputs")
    if isinstance(inputs, Mapping) and isinstance(inputs.get(name), str):
        return str(inputs[name])
    return default


def _data_file(record: HarvestRecord, relative: str) -> Path:
    identity = _identity(record)
    data = record.data
    if data is None or record.data_generation is None:
        raise ValueError(f"{identity}: expected published data file {relative!r}, but the job has no published data")
    path = data / relative
    if not path.is_file():
        raise ValueError(f"{identity}: expected published data file {path}")
    return path


def _load(path: Path, *, raw: bool = False) -> Any:
    """Load one VASP file through the optional httk-io and httk-atomistic providers."""

    try:
        import httk.atomistic
        import httk.core

        if not httk.core.has_reader_for(path.name):
            raise ImportError(f"no reader is registered for {path.name}")
        return httk.core.load(str(path), raw=raw)
    except ImportError as exc:
        raise ValueError(
            "VASP interpretation requires the file readers and structure adapters provided by "
            f"httk-io + httk-atomistic: {exc}"
        ) from exc


def _structure(record: HarvestRecord, path: Path) -> object:
    try:
        loaded = _load(path)
        import httk.atomistic

        return httk.atomistic.UnitcellStructureView(loaded)
    except (AttributeError, ImportError, TypeError, ValueError) as exc:
        raise ValueError(f"{_identity(record)}: cannot construct relaxed structure from {path}: {exc}") from exc


def _energy(record: HarvestRecord, path: Path) -> httk.core.DataRecord:
    lexeme: Any = None
    try:
        outcar = _load(path, raw=True)["outcar"]
        final_energies = getattr(outcar, "final_energies", None)
        lexeme = None if final_energies is None else getattr(final_energies, "energy_sigma0", None)
        value = float(lexeme)
        return httk.core.DataRecord.from_value(_TOTAL_ENERGY_DEFINITION, _TOTAL_ENERGY_NAME, value)
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{_identity(record)}: cannot construct total energy from {path}; energy_sigma0={lexeme!r}: {exc}"
        ) from exc


def _provenance_input_id(record: HarvestRecord) -> str | None:
    entry = record.declarations.get("provenance")
    if not isinstance(entry, Mapping):
        return None
    document = entry.get("observed") if entry.get("observed") is not None else entry.get("declared")
    if not isinstance(document, Mapping):
        return None
    inputs = document.get("inputs")
    initial = inputs.get("initial_structure") if isinstance(inputs, Mapping) else None
    value = initial.get("id") if isinstance(initial, Mapping) else None
    return value if isinstance(value, str) else None


def _entry_id(entry: object) -> str:
    value = getattr(entry, "id", None)
    if not isinstance(value, str):
        raise ValueError(f"interpreted VASP entry {type(entry).__name__} has no served string id")
    return value


def _overlay_edges(
    base: tuple[httk.core.RunEdge, ...], owned: tuple[httk.core.RunEdge, ...]
) -> tuple[httk.core.RunEdge, ...]:
    """Replace owned labels in *base* and retain every unrelated edge."""

    replacements = {edge.label: edge for edge in owned}
    result: list[httk.core.RunEdge] = []
    replaced: set[str] = set()
    for edge in base:
        replacement = replacements.get(edge.label)
        if replacement is None:
            result.append(edge)
        else:
            result.append(replacement)
            replaced.add(edge.label)
    result.extend(edge for edge in owned if edge.label not in replaced)
    return tuple(result)


def _interpreted(
    record: HarvestRecord,
    *,
    structure_path: str | None,
    energy_path: str,
    product: bool,
) -> InterpretedRun:
    base = run_record(record)
    structure_file = None if structure_path is None else _data_file(record, structure_path)
    structure = None if structure_file is None else _structure(record, structure_file)
    structure_id = None
    if structure is not None:
        try:
            structure_id = _entry_id(structure)
        except ValueError as exc:
            raise ValueError(
                f"{_identity(record)}: cannot construct relaxed structure from {structure_file}: {exc}"
            ) from exc
    energy = _energy(record, _data_file(record, energy_path))
    energy_edge = httk.core.RunEdge("total_energy", energy.type, energy.id)
    entries: tuple[object, ...]
    edges: tuple[httk.core.RunEdge, ...]
    if structure is None:
        entries = (energy,)
        edges = (energy_edge,)
    else:
        assert structure_id is not None
        entries = (structure, energy)
        edges = (httk.core.RunEdge("relaxed_structure", "structures", structure_id), energy_edge)
    run = httk.core.Run(
        workflow_declaration_uri=base.workflow_declaration_uri,
        inputs=base.inputs,
        artifacts=_overlay_edges(base.artifacts, edges),
        outputs=_overlay_edges(base.outputs, edges),
        immutable_id=base.immutable_id,
        last_modified=base.last_modified,
    )
    products: tuple[httk.core.ProductLink, ...] = ()
    if product and structure is not None:
        source_id = _provenance_input_id(record)
        if source_id is not None:
            assert structure_id is not None
            products = (
                httk.core.ProductLink(
                    source_type="structures",
                    source_id=source_id,
                    target_type="structures",
                    target_id=structure_id,
                    label="relaxed_structure",
                    workflow_declaration_uri=run.workflow_declaration_uri,
                ),
            )
    return InterpretedRun(run=run, entries=entries, products=products)


def interpret_vasp_relax(record: HarvestRecord) -> InterpretedRun:
    """Interpret a VASP relaxation's ``data/<prefix>/CONTCAR`` and ``OUTCAR``.

    The default prefix is ``vasp``. The relaxed structure and total energy are
    returned as entries, and both are run artifacts and outputs. An input
    ``initial_structure`` provenance edge also produces a ``relaxed_structure``
    product link. The generated artifact/output labels overlay those labels in
    the declared provenance while unrelated labels are preserved.
    """

    prefix = _input(record, "data_prefix", "vasp")
    return _interpreted(
        record,
        structure_path=f"{prefix}/CONTCAR" if prefix else "CONTCAR",
        energy_path=f"{prefix}/OUTCAR" if prefix else "OUTCAR",
        product=True,
    )


def interpret_vasp_static(record: HarvestRecord) -> InterpretedRun:
    """Interpret a VASP static job's ``data/<prefix>/OUTCAR`` total energy.

    The default prefix is ``vasp``. Static jobs do not require a CONTCAR and
    never emit a product link; the total energy is both an artifact and output.
    Its generated ``total_energy`` label overlays a declared edge with that
    label, while unrelated artifact/output labels are preserved.
    """

    prefix = _input(record, "data_prefix", "vasp")
    return _interpreted(
        record,
        structure_path=None,
        energy_path=f"{prefix}/OUTCAR" if prefix else "OUTCAR",
        product=False,
    )


def interpret_vasp_relax_static(record: HarvestRecord) -> InterpretedRun:
    """Interpret a relax-static job's archived relax and final static outputs.

    The default layout is ``data/relax/CONTCAR`` for the relaxed structure and
    ``data/static/OUTCAR`` for the final static energy. A non-empty
    ``data_prefix`` is prepended to both directories. Both entries are run
    artifacts and outputs, and an input ``initial_structure`` provenance edge
    produces a ``relaxed_structure`` product link.
    Generated labels overlay matching declared artifact/output labels; unrelated
    labels remain on the rebuilt run.
    """

    prefix = _input(record, "data_prefix", "")
    root = f"{prefix}/" if prefix else ""
    return _interpreted(
        record,
        structure_path=f"{root}relax/CONTCAR",
        energy_path=f"{root}static/OUTCAR",
        product=True,
    )


register_interpreter(workflow="httk.vasp.relax", interpreter=interpret_vasp_relax)
register_interpreter(workflow="httk.vasp.static", interpreter=interpret_vasp_static)
register_interpreter(workflow="httk.vasp.relax-static", interpreter=interpret_vasp_relax_static)
