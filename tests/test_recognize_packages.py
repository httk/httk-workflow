"""Recognized-calculation collector packages: manifest, refusals, hook types."""

from pathlib import Path

import pytest

from attempt_fixtures import new_job
from httk.workflow import Workspace
from httk.workflow.hookapi import Claim, Unclaimed
from httk.workflow.packages import load_workflow_package, parse_workflow_manifest
from httk.workflow.scaffold import RecognizeSpec

_MANIFEST = """
[workflow]
name = "vasp.calculation.relax"
description = "a finished VASP relaxation found in a directory"

[workflow.recognize]
file = "recognize.py"
priority = 10
requires = ["OUTCAR", "POSCAR"]

[workflow.inputs.structure]
role = "initial_structure"
entry_type = "structures"

[workflow.outputs.relaxed_structure]
entry_type = "structures"
role = "relaxed_structure"
ref = "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures"
product_of = "initial_structure"

[workflow.collect]
file = "collect.py"
"""


def _package(root: Path, manifest: str = _MANIFEST) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (root / "recognize.py").write_text("def recognize(directory):\n    return None\n", encoding="utf-8")
    (root / "collect.py").write_text("def collect(record):\n    return {}\n", encoding="utf-8")
    return root


def test_recognize_package_parses(tmp_path: Path) -> None:
    provider = load_workflow_package(_package(tmp_path / "p"), register=False)
    assert provider.recognize == RecognizeSpec("recognize.py", 10, ("OUTCAR", "POSCAR"))
    assert provider.runnable is False
    assert provider.steps == ()
    assert provider.inputs == {"structure": None}


@pytest.mark.parametrize(
    "requires",
    ['["*.out"]', '["a/b"]', '["*"]', '[""]', "[]", '"OUTCAR"', '["*.a.b"]'],
)
def test_requires_validation(tmp_path: Path, requires: str) -> None:
    manifest = _MANIFEST.replace('["OUTCAR", "POSCAR"]', requires)
    if requires == '["*.out"]':
        assert parse_workflow_manifest(_package(tmp_path / "p", manifest)).recognize is not None
    else:
        with pytest.raises(ValueError, match="requires"):
            parse_workflow_manifest(_package(tmp_path / "p", manifest))


@pytest.mark.parametrize(
    "old, new, match",
    [
        ("priority = 10\n", "", "priority"),
        ("priority = 10", "priority = true", "priority"),
        ('file = "recognize.py"\n', "", "file"),
        ('[workflow.collect]\nfile = "collect.py"\n', "", "must have"),
    ],
)
def test_recognize_refusals(tmp_path: Path, old: str, new: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        parse_workflow_manifest(_package(tmp_path / "p", _MANIFEST.replace(old, new)))


def test_neither_runner_nor_recognize_refused(tmp_path: Path) -> None:
    manifest = '[workflow]\nname = "x"\n\n[workflow.collect]\nfile = "collect.py"\n'
    with pytest.raises(ValueError, match=r"\[workflow.runner\] must be a table"):
        parse_workflow_manifest(_package(tmp_path / "p", manifest))


def test_runner_and_recognize_refused(tmp_path: Path) -> None:
    manifest = _MANIFEST + '\n[workflow.runner]\nsteps = ["start"]\n'
    with pytest.raises(ValueError, match="either runs or recognizes"):
        parse_workflow_manifest(_package(tmp_path / "p", manifest))


def test_job_creation_refused(tmp_path: Path) -> None:
    package = _package(tmp_path / "p")
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match="vasp.calculation.relax recognizes calculations and cannot be run"):
        new_job(workspace, package)


def test_claim_and_unclaimed() -> None:
    assert Claim("ab12").identity == "ab12"
    assert Unclaimed("several outputs").reason == "several outputs"
    for bad in ("", "a:b", "a/b", "a b"):
        with pytest.raises(ValueError):
            Claim(bad)
