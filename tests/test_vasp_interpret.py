"""Built-in VASP interpretation over synthetic harvested data."""

from collections.abc import Mapping
from pathlib import Path, PurePosixPath

import pytest

pytest.importorskip("httk.atomistic")
pytest.importorskip("httk.io")

import httk.core
from httk.atomistic import UnitcellStructureView

import httk.workflow.vasp
from httk.workflow.harvesting import HarvestRecord
from httk.workflow.interpretation import interpret
from httk.workflow.vasp.interpret import interpret_vasp_relax, interpret_vasp_relax_static, interpret_vasp_static

_POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0 0.0 0.0
0.5 0.5 0.5
"""
_OUTCAR = """ vasp.5.2.12 synthetic
   FREE ENERGIE OF THE ION-ELECTRON SYSTEM (eV)
   free  energy   TOTEN  =       -27.09328752 eV
   energy  without entropy=      -27.09328752  energy(sigma->0) =      -27.09328752
  General timing and accounting informations for this job:
"""


def _record(
    root: Path,
    workflow: str,
    *,
    prefix: str | None = None,
    input_id: str | None = "structures/input",
    extra_outputs: dict[str, dict[str, str]] | None = None,
) -> HarvestRecord:
    inputs: dict[str, object] = {}
    if prefix is not None:
        inputs["data_prefix"] = prefix
    declarations: dict[str, dict[str, Mapping[str, object] | None]] = {
        "workflow": {
            "declared": {"$id": f"https://schemas.httk.org/defs/v0.1/workflows/{workflow.rsplit('.', 1)[-1]}"},
            "observed": None,
        }
    }
    provenance: dict[str, Mapping[str, object]] = {}
    if input_id is not None:
        provenance["inputs"] = {"initial_structure": {"type": "structures", "id": input_id}}
    if extra_outputs is not None:
        provenance["outputs"] = extra_outputs
    if provenance:
        declarations["provenance"] = {
            "declared": provenance,
            "observed": None,
        }
    return HarvestRecord(
        workspace_root=root,
        workspace_id="ws",
        job_id="12345678-1234-4234-8234-123456789abc",
        job_key="job--12345678-1234-4234-8234-123456789abc",
        job={"workflow": workflow, "inputs": inputs},
        runner_provenance=None,
        state="succeeded",
        failure=None,
        placement=PurePosixPath("jobs"),
        payload_path=PurePosixPath("jobs/job--12345678-1234-4234-8234-123456789abc"),
        workdir_path=None,
        data_path=PurePosixPath("data"),
        data_generation=1,
        provenance={},
        runner_steps=None,
        children={},
        declarations=declarations,
    )


def _write(root: Path, *parts: str) -> None:
    directory = root.joinpath(*parts[:-1])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / parts[-1]).write_text(_POSCAR if parts[-1] == "CONTCAR" else _OUTCAR, encoding="utf-8")


def test_relax_interpretation_builds_entries_edges_and_product(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "CONTCAR")
    _write(tmp_path / "data", "vasp", "OUTCAR")

    interpreted = interpret(_record(tmp_path, "httk.vasp.relax"))
    structure, energy = interpreted.entries
    assert isinstance(structure, UnitcellStructureView)
    assert isinstance(energy, httk.core.DataRecord)
    assert structure.type == "structures"
    assert energy.type == "_httk_records" and energy.value == -27.09328752
    assert [(edge.label, edge.entry_type, edge.entry_id) for edge in interpreted.run.inputs] == [
        ("initial_structure", "structures", "structures/input")
    ]
    assert [(edge.label, edge.entry_type, edge.entry_id) for edge in interpreted.run.outputs] == [
        ("relaxed_structure", "structures", structure.id),
        ("total_energy", "_httk_records", energy.id),
    ]
    assert interpreted.run.artifacts == interpreted.run.outputs
    assert len(interpreted.products) == 1
    product = interpreted.products[0]
    assert (product.source_id, product.target_id, product.label) == (
        "structures/input",
        structure.id,
        "relaxed_structure",
    )
    assert product.workflow_declaration_uri == interpreted.run.workflow_declaration_uri


def test_relax_without_input_id_omits_product(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "CONTCAR")
    _write(tmp_path / "data", "vasp", "OUTCAR")

    assert interpret_vasp_relax(_record(tmp_path, "httk.vasp.relax", input_id=None)).products == ()


def test_relax_overlays_owned_edges_and_preserves_unrelated_outputs(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "CONTCAR")
    _write(tmp_path / "data", "vasp", "OUTCAR")
    record = _record(
        tmp_path,
        "httk.vasp.relax",
        extra_outputs={
            "band_gap": {"type": "_httk_records", "id": "band-gap"},
            "relaxed_structure": {"type": "structures", "id": "stale-structure"},
        },
    )

    interpreted = interpret_vasp_relax(record)
    structure, energy = interpreted.entries
    assert isinstance(structure, UnitcellStructureView)
    assert isinstance(energy, httk.core.DataRecord)
    assert [(edge.label, edge.entry_id) for edge in interpreted.run.outputs] == [
        ("band_gap", "band-gap"),
        ("relaxed_structure", structure.id),
        ("total_energy", energy.id),
    ]
    assert len({edge.label for edge in interpreted.run.outputs}) == 3


def test_static_interpretation_needs_only_final_energy(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "OUTCAR")

    interpreted = interpret_vasp_static(_record(tmp_path, "httk.vasp.static"))
    (energy,) = interpreted.entries
    assert isinstance(energy, httk.core.DataRecord)
    assert energy.type == "_httk_records"
    assert [edge.label for edge in interpreted.run.outputs] == ["total_energy"]
    assert interpreted.products == ()


def test_relax_static_uses_archived_relax_and_final_static_layout(tmp_path: Path) -> None:
    _write(tmp_path / "data", "relax", "CONTCAR")
    _write(tmp_path / "data", "static", "OUTCAR")

    interpreted = interpret_vasp_relax_static(_record(tmp_path, "httk.vasp.relax-static"))
    structure, energy = interpreted.entries
    assert isinstance(structure, UnitcellStructureView)
    assert isinstance(energy, httk.core.DataRecord)
    assert [structure.type, energy.type] == ["structures", "_httk_records"]


def test_missing_vasp_file_names_path_and_job(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "CONTCAR")

    with pytest.raises(ValueError, match=r"ws:12345678-1234-4234-8234-123456789abc.*OUTCAR"):
        interpret_vasp_relax(_record(tmp_path, "httk.vasp.relax"))


def test_missing_total_energy_names_path_lexeme_and_job(tmp_path: Path) -> None:
    path = tmp_path / "data" / "vasp" / "OUTCAR"
    _write(tmp_path / "data", "vasp", "OUTCAR")
    path.write_text(
        " vasp.5.2.12 synthetic\n  General timing and accounting informations for this job:\n", encoding="utf-8"
    )

    with pytest.raises(
        ValueError,
        match=r"ws:12345678-1234-4234-8234-123456789abc.*OUTCAR.*energy_sigma0=None",
    ):
        interpret_vasp_static(_record(tmp_path, "httk.vasp.static"))


def test_missing_reader_names_optional_providers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write(tmp_path / "data", "vasp", "OUTCAR")
    monkeypatch.setattr(httk.core, "has_reader_for", lambda name: False)

    with pytest.raises(ValueError, match=r"httk-io \+ httk-atomistic"):
        interpret_vasp_static(_record(tmp_path, "httk.vasp.static"))
