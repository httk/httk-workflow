"""``scaffold_job`` builds a payload without submitting; ``new_job`` submits the same bytes.

:func:`~httk.workflow.scaffold.scaffold_job` is :func:`~httk.workflow.scaffold.new_job`
stopped one step short of submission — the shared body extracted so that
:meth:`httk.workflow.Attempt.call` can build a child of an installed workflow.
These tests hold the two to one implementation: the payload ``scaffold_job``
writes is byte-for-byte the payload ``new_job`` submits, save the random job id.
"""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from httk.workflow import Workspace, _kernel, new_job, scaffold_job
from httk.workflow._job import JobDefinition
from test_job_creation import install
from test_scaffold import _relax_package

_POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
"""


@pytest.fixture()
def workspace(tmp_path: Path) -> Iterator[Workspace]:
    """A workspace with ``tests.relax`` installed."""

    ws = Workspace.initialize(tmp_path / "workspace")
    install(ws, _relax_package(tmp_path / "relax"))
    yield ws


@pytest.fixture()
def structure(tmp_path: Path) -> Path:
    path = tmp_path / "POSCAR"
    path.write_text(_POSCAR, encoding="utf-8")
    return path


def test_scaffold_job_builds_a_payload_and_submits_nothing(
    workspace: Workspace, structure: Path, tmp_path: Path
) -> None:
    destination = tmp_path / "payload"
    destination.mkdir()

    job = scaffold_job(workspace, "tests.relax", destination, files={"POSCAR": structure}, tag="silicon")

    assert isinstance(job, JobDefinition)
    # The payload is complete: job.json plus the staged file where the runner reads it.
    assert (destination / "job.json").is_file()
    assert (destination / "files" / "POSCAR").read_text(encoding="utf-8") == _POSCAR
    definition = JobDefinition.from_path(destination / "job.json")
    assert definition.workflow_id == "local:tests.relax" and definition.initial_step == "prepare"
    # No job was submitted: no job directory exists anywhere in the workspace.
    assert _kernel.locate(workspace, job.id, placement_hint=None, exhaustive=True) is None


def test_scaffold_job_refuses_a_non_empty_destination(workspace: Workspace, structure: Path, tmp_path: Path) -> None:
    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "stray.txt").write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="empty directory"):
        scaffold_job(workspace, "tests.relax", occupied, files={"POSCAR": structure})


def test_scaffold_job_refuses_a_missing_destination(workspace: Workspace, structure: Path, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="existing directory"):
        scaffold_job(workspace, "tests.relax", tmp_path / "nope", files={"POSCAR": structure})


def test_scaffold_job_writes_what_new_job_submits(workspace: Workspace, structure: Path, tmp_path: Path) -> None:
    destination = tmp_path / "scaffolded"
    destination.mkdir()

    scaffolded = scaffold_job(
        workspace,
        "tests.relax",
        destination,
        files={"POSCAR": structure},
        tag="silicon",
        parameters={"kpoint_density": 30.0},
    )
    submitted = new_job(
        workspace,
        "tests.relax",
        files={"POSCAR": structure},
        tag="silicon",
        parameters={"kpoint_density": 30.0},
    )

    from_scaffold = json.loads((destination / "job.json").read_text(encoding="utf-8"))
    from_new_job = json.loads((submitted.payload / "job.json").read_text(encoding="utf-8"))
    # The only difference is the random per-job id (and the job_key it derives).
    assert from_scaffold["id"] == scaffolded.id
    assert from_new_job["id"] == submitted.job_id
    assert from_scaffold.pop("id") != from_new_job.pop("id")
    assert from_scaffold == from_new_job
