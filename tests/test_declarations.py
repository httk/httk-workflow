"""Workflow declarations: carried verbatim in ``job.json`` and collected side by side with observed ones.

A declaration says what a workflow *is* — its inputs, its method, its outputs —
without a graph. *httk-workflow* carries the document and never interprets it, so
what is tested here is carriage and honesty: that a document survives `job.json`
unchanged, and that a collect reports the declared and the observed document side
by side, including when one of them is damaged. Declaring at run time through
the SDK and the Bash bridge is tested in ``test_declarations_sdk.py``.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import FormatError, JobRecord, Workspace, job_records
from httk.workflow._job import JobDefinition
from httk.workflow.models import MAXIMUM_DECLARATIONS_BYTES, validate_declarations
from v3_helpers import find, install, job_mapping, run, submit, workspace

# One plausible declaration document. Nothing here reads any of it: the members
# that say which vocabulary and which version it follows live inside the document
# itself, which is exactly the point of carrying it verbatim.
_DECLARED: dict[str, Any] = {
    "$id": "https://example.org/workflows/relax/v1.0.0",
    "$schema": "https://schemas.optimade.org/defs/v1.2/workflow_declaration.json",
    "title": "Relaxation",
    "x-optimade-definition": {"kind": "workflow", "version": "1.0.0", "name": "relax"},
    "inputs": {"structure": {"$ref": "https://schemas.optimade.org/defs/v1.2/types/structure"}},
    "method": {"code": "example", "convergence": {"ediffg": -0.01}, "restarts": None},
    "outputs": {"structure": {"$ref": "https://schemas.optimade.org/defs/v1.2/types/structure"}},
    "unicode": "Ångström ≤ 1e-3",
}
_OBSERVED: dict[str, Any] = {**_DECLARED, "outputs": {"structures": 3}}
_OBSERVED_ONLY = {"$id": "https://example.org/hints/v1", "note": "runtime"}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_a_declaration_must_be_a_json_object() -> None:
    # Only the shape is checked, and only at the top level: whatever is inside a
    # document belongs to the vocabulary it names, not to this engine.
    assert validate_declarations({"workflow": _DECLARED}) == {"workflow": _DECLARED}
    assert validate_declarations({}) == {}
    for refused in (5, "a string", [1, 2], None, True):
        with pytest.raises(FormatError, match="declarations.workflow must be an object"):
            validate_declarations({"workflow": refused})
    with pytest.raises(FormatError, match="declarations must be an object"):
        validate_declarations([{"workflow": {}}])
    with pytest.raises(FormatError, match="must contain only JSON values"):
        validate_declarations({"workflow": {"structure": object()}})


def test_a_declaration_name_is_one_safe_path_component() -> None:
    for name in ("workflow", "_optimade_workflow", "relax.v1", "a-b_c.0", "Z" * 64):
        assert validate_declarations({name: {}}) == {name: {}}
    for refused in ("", ".", "..", "../escape", "a/b", ".hidden", "-leading", "x" * 65, "spaced name", "n\x00l"):
        with pytest.raises(FormatError):
            validate_declarations({refused: {}})
    with pytest.raises(FormatError, match="declarations key must be a nonempty string"):
        validate_declarations({7: {}})


def test_declarations_have_their_own_size_budget() -> None:
    filling = "x" * (MAXIMUM_DECLARATIONS_BYTES // 2)
    assert validate_declarations({"workflow": {"blob": filling}})["workflow"]["blob"] == filling
    with pytest.raises(FormatError, match=f"exceeds the {MAXIMUM_DECLARATIONS_BYTES}-byte limit"):
        validate_declarations({"workflow": {"blob": filling}, "second": {"blob": filling}})
    # The budget is the inputs budget again, not shared with it: a job may spend
    # both, because they bound two independent members of one job.json.
    mapping = job_mapping(("local:demo", "demo"), {"start": "succeed"}, declarations={"workflow": {"blob": filling}})
    definition = JobDefinition.from_mapping({**mapping, "parameters": {"blob": filling}})
    assert definition.parameters["blob"] == filling
    assert definition.declarations["workflow"] == {"blob": filling}


# ---------------------------------------------------------------------------
# job.json and spawned children
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# job.json and collecting
# ---------------------------------------------------------------------------


def test_a_job_carries_its_declarations_verbatim(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    ref = submit(ws, ("local:demo", "demo"), {"start": "succeed"}, declarations={"workflow": _DECLARED})

    stored = json.loads((ref.path / "job.json").read_text(encoding="utf-8"))
    assert stored["declarations"] == {"workflow": _DECLARED}
    assert JobDefinition.from_path(ref.path / "job.json").declarations == {"workflow": _DECLARED}
    # A job that declares nothing says so with an empty mapping.
    plain = submit(ws, ("local:demo", "demo"), {"start": "succeed"})
    assert JobDefinition.from_path(plain.path / "job.json").declarations == {}


def _campaign(root: Path) -> tuple[Workspace, str]:
    """Run one job that declares statically and refines its declaration at run time."""

    ws = workspace(root / "workspace")
    installed = install(ws, root / "package")
    ref = submit(
        ws,
        installed,
        {"start": "succeed"},
        tag="declaring",
        placement="project/declaring",
        declarations={"workflow": _DECLARED, "declared_only": {"$id": "https://example.org/static/v1"}},
        parameters={"declare": {"start": {"workflow": _OBSERVED, "observed_only": _OBSERVED_ONLY}}},
    )
    run(ws)
    return ws, ref.job_id


def test_a_collect_reports_declared_and_observed_side_by_side(tmp_path: Path) -> None:
    ws, _ = _campaign(tmp_path / "run")

    records = list(job_records(ws))
    assert len(records) == 1
    record = records[0]
    assert not record.gaps
    # Every name either source knows appears once, and the two sides are exactly
    # what each source said — never merged into one document.
    assert sorted(record.declarations) == ["declared_only", "observed_only", "workflow"]
    assert record.declarations["workflow"] == {"declared": _DECLARED, "observed": _OBSERVED}
    assert record.declarations["declared_only"] == {
        "declared": {"$id": "https://example.org/static/v1"},
        "observed": None,
    }
    assert record.declarations["observed_only"] == {"declared": None, "observed": _OBSERVED_ONLY}

    # A record survives being written, shipped, and read back unchanged.
    shipped = json.loads(json.dumps(record.as_mapping()))
    assert JobRecord.from_mapping(shipped).declarations == record.declarations
    assert JobRecord.from_mapping(shipped).as_mapping() == record.as_mapping()


def test_an_unreadable_observed_declaration_is_a_gap_and_not_a_silence(tmp_path: Path) -> None:
    ws, job_id = _campaign(tmp_path / "run")
    declarations = find(ws, job_id).path / ".httk-job" / "declarations"
    (declarations / "workflow.json").write_text("{ this is not JSON", encoding="utf-8")

    record = next(iter(job_records(ws)))
    # The result still exists, so it is still collected: the damaged side is
    # reported as absent, the intact side is untouched, and the record says its
    # history is not complete.
    assert record.gaps
    assert record.declarations["workflow"] == {"declared": _DECLARED, "observed": None}
    assert record.declarations["observed_only"]["observed"] == _OBSERVED_ONLY
    assert JobRecord.from_mapping(record.as_mapping()).declarations == record.declarations


def test_declaring_does_not_move_the_job_digest(tmp_path: Path) -> None:
    ws, job_id = _campaign(tmp_path / "run")
    ref = find(ws, job_id)
    digest = JobDefinition.from_path(ref.path / "job.json").digest
    # Observed declarations live below .httk-job/, and the job digest pins job.json alone.
    (ref.path / ".httk-job" / "declarations" / "late.json").write_text('{"late": true}', encoding="utf-8")
    assert JobDefinition.from_path(ref.path / "job.json").digest == digest
    record = next(iter(job_records(ws)))
    assert record.job["digest"] == digest
    assert record.declarations["late"] == {"declared": None, "observed": {"late": True}}
