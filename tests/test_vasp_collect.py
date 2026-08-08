"""VASP collectors and their packaged postprocess script use published data."""

import importlib.resources
import json
import re
import stat
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

pytest.importorskip("httk.atomistic")
pytest.importorskip("httk.io")

import httk.core

from httk.workflow.collecting import JobRecord
from httk.workflow.postprocessing import run_postprocess_script
from httk.workflow.scaffold import registered_workflow
from httk.workflow.vasp.collect import (
    collect_vasp_relax,
    collect_vasp_relax_static,
    collect_vasp_static,
)

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
   free  energy   TOTEN  =       -26.00000000 eV
   energy  without entropy=      -26.00000000  energy(sigma->0) =      -26.00000000
   free  energy   TOTEN  =       -27.00000000 eV
   energy  without entropy=      -27.00000000  energy(sigma->0) =      -27.00000000
   free  energy   TOTEN  =       -27.09328752 eV
   energy  without entropy=      -27.09328752  energy(sigma->0) =      -27.09328752
  General timing and accounting informations for this job:
"""


def _record(root: Path, workflow: str) -> JobRecord:
    return JobRecord(
        workspace_root=root,
        workspace_id="ws",
        job_id="12345678-1234-4234-8234-123456789abc",
        job_key="job--12345678-1234-4234-8234-123456789abc",
        job={"workflow": workflow},
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
        declarations={},
    )


def _write(root: Path, *parts: str) -> None:
    directory = root.joinpath(*parts[:-1])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / parts[-1]).write_text(_POSCAR if parts[-1] == "CONTCAR" else _OUTCAR, encoding="utf-8")


def test_relax_returns_declared_roles(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "CONTCAR")
    _write(tmp_path / "data", "vasp", "OUTCAR")
    outputs = collect_vasp_relax(_record(tmp_path, "httk.vasp.relax"))
    assert set(outputs) == {"relaxed_structure", "total_energy"}
    assert isinstance(outputs["total_energy"], httk.core.DataRecord)


def test_static_returns_only_energy(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "OUTCAR")
    outputs = collect_vasp_static(_record(tmp_path, "httk.vasp.static"))
    assert set(outputs) == {"total_energy"}


def test_relax_static_uses_two_output_layouts(tmp_path: Path) -> None:
    _write(tmp_path / "data", "relax", "CONTCAR")
    _write(tmp_path / "data", "static", "OUTCAR")
    outputs = collect_vasp_relax_static(_record(tmp_path, "httk.vasp.relax-static"))
    assert set(outputs) == {"relaxed_structure", "total_energy"}


def test_missing_file_names_job_identity(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "CONTCAR")
    with pytest.raises(ValueError, match=r"ws:12345678-1234-4234-8234-123456789abc.*OUTCAR"):
        collect_vasp_relax(_record(tmp_path, "httk.vasp.relax"))


def test_packaged_relaxation_report_runs_from_published_data(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "CONTCAR")
    _write(tmp_path / "data", "vasp", "OUTCAR")
    (tmp_path / "run").mkdir()
    record = replace(_record(tmp_path, "httk.vasp.relax"), workdir_path=PurePosixPath("run"))
    workflow = registered_workflow("vasp-relax")
    assert workflow is not None
    assert workflow.runner_package is not None
    script = Path(str(importlib.resources.files(workflow.runner_package).joinpath("scripts/relaxation_report")))
    assert script.stat().st_mode & stat.S_IXUSR

    result = run_postprocess_script(workflow, "relaxation-report", record)
    assert result.returncode == 0
    report = json.loads((result.output_dir / "relaxation_report.json").read_text(encoding="utf-8"))
    assert report["final_energy"] == pytest.approx(-27.09328752)
    assert report["structure_files"] == ["vasp/CONTCAR"]
    assert "vasp/CONTCAR" in (result.output_dir / "relaxation_report.txt").read_text(encoding="utf-8")


def test_packaged_relaxation_plot_runs_from_published_data(tmp_path: Path) -> None:
    _write(tmp_path / "data", "vasp", "OUTCAR")
    (tmp_path / "run").mkdir()
    record = replace(_record(tmp_path, "httk.vasp.relax"), workdir_path=PurePosixPath("run"))
    workflow = registered_workflow("vasp-relax")
    assert workflow is not None
    assert workflow.runner_package is not None
    script = Path(str(importlib.resources.files(workflow.runner_package).joinpath("scripts/relaxation_plot")))
    assert script.stat().st_mode & stat.S_IXUSR

    result = run_postprocess_script(workflow, "relaxation-plot", record)
    assert result.returncode == 0
    svg = (result.output_dir / "relaxation_energies.svg").read_text(encoding="utf-8")
    points = re.search(r'<polyline points="([^"]+)"', svg)
    assert points is not None
    assert len(points.group(1).split()) == 3
    assert "relaxation_energies.svg" in result.stdout


def test_packaged_relaxation_plot_tolerates_missing_outcar(tmp_path: Path) -> None:
    (tmp_path / "run").mkdir()
    record = replace(_record(tmp_path, "httk.vasp.relax"), workdir_path=PurePosixPath("run"))
    workflow = registered_workflow("vasp-relax")
    assert workflow is not None

    result = run_postprocess_script(workflow, "relaxation-plot", record)
    assert result.returncode == 0
    assert "no OUTCAR" in result.stdout
    assert not (result.output_dir / "relaxation_energies.svg").exists()
