"""Pure VASP result extraction adapters."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from httk.workflow.collecting import JobRecord

__all__ = ["postprocess_vasp_relax", "postprocess_vasp_relax_static", "postprocess_vasp_static"]

if TYPE_CHECKING:
    import httk.core

_TOTAL_ENERGY_DEFINITION = "https://schemas.httk.org/defs/v0.1/properties/core/total_energy"
_TOTAL_ENERGY_NAME = "_httk_total_energy"


def _identity(record: JobRecord) -> str:
    return f"{record.workspace_id}:{record.job_id}"


def _parameter(record: JobRecord, name: str, default: str) -> str:
    parameters = record.job.get("parameters")
    return (
        str(parameters[name]) if isinstance(parameters, Mapping) and isinstance(parameters.get(name), str) else default
    )


def _data_file(record: JobRecord, relative: str) -> Path:
    data = record.data
    if data is None or record.data_generation is None:
        raise ValueError(
            f"{_identity(record)}: expected published data file {relative!r}, but the job has no published data"
        )
    path = data / relative
    if not path.is_file():
        raise ValueError(f"{_identity(record)}: expected published data file {path}")
    return path


def _load(path: Path, *, raw: bool = False) -> Any:
    try:
        importlib.import_module("httk.atomistic")

        core = importlib.import_module("httk.core")
        if not core.has_reader_for(path.name):
            raise ImportError(f"no reader is registered for {path.name}")
        return core.load(str(path), raw=raw)
    except ImportError as exc:
        raise ValueError(
            "VASP postprocessing requires the file readers and structure adapters provided by "
            f"httk-io + httk-atomistic: {exc}"
        ) from exc


def _structure(record: JobRecord, path: Path) -> object:
    try:
        loaded = _load(path)
        import httk.atomistic

        return httk.atomistic.UnitcellStructureView(loaded)
    except (AttributeError, ImportError, TypeError, ValueError) as exc:
        raise ValueError(f"{_identity(record)}: cannot construct relaxed structure from {path}: {exc}") from exc


def _energy(record: JobRecord, path: Path) -> httk.core.DataRecord:
    lexeme: Any = None
    try:
        outcar = _load(path, raw=True)["outcar"]
        final_energies = getattr(outcar, "final_energies", None)
        lexeme = None if final_energies is None else getattr(final_energies, "energy_sigma0", None)
        core = __import__("httk.core", fromlist=["DataRecord"])
        return core.DataRecord.from_value(_TOTAL_ENERGY_DEFINITION, _TOTAL_ENERGY_NAME, float(lexeme))
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{_identity(record)}: cannot construct total energy from {path}; energy_sigma0={lexeme!r}: {exc}"
        ) from exc


def postprocess_vasp_relax(record: JobRecord) -> Mapping[str, object]:
    """Extract the relaxed structure and final energy from one VASP job.

    :param record: Read the collected VASP job record.
    :return: Extracted output roles for the relaxation workflow.
    """

    prefix = _parameter(record, "data_prefix", "vasp")
    return {
        "relaxed_structure": _structure(record, _data_file(record, f"{prefix}/CONTCAR" if prefix else "CONTCAR")),
        "total_energy": _energy(record, _data_file(record, f"{prefix}/OUTCAR" if prefix else "OUTCAR")),
    }


def postprocess_vasp_static(record: JobRecord) -> Mapping[str, object]:
    """Extract the final energy from one static VASP job.

    :param record: Read the collected VASP job record.
    :return: Extracted output roles for the static workflow.
    """

    prefix = _parameter(record, "data_prefix", "vasp")
    return {"total_energy": _energy(record, _data_file(record, f"{prefix}/OUTCAR" if prefix else "OUTCAR"))}


def postprocess_vasp_relax_static(record: JobRecord) -> Mapping[str, object]:
    """Extract the relaxed structure and final static energy.

    :param record: Read the collected VASP job record.
    :return: Extracted output roles from the relaxation and static stages.
    """

    prefix = _parameter(record, "data_prefix", "")
    root = f"{prefix}/" if prefix else ""
    return {
        "relaxed_structure": _structure(record, _data_file(record, f"{root}relax/CONTCAR")),
        "total_energy": _energy(record, _data_file(record, f"{root}static/OUTCAR")),
    }
