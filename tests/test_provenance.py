"""The provenance declaration is interpreted without changing the collect record."""

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import cast

import pytest

import httk.workflow.vasp  # noqa: F401 - imports and registers packaged workflows
from httk.workflow import JobRecord, Workspace, job_records
from httk.workflow.provenance import run_record
from httk.workflow.scaffold import registered_workflow
from test_collect import campaign as _collect_campaign


@pytest.fixture(scope="module")
def real_record(tmp_path_factory: pytest.TempPathFactory) -> JobRecord:
    campaign = cast(
        Callable[[pytest.TempPathFactory], tuple[Workspace, dict[str, str]]], vars(_collect_campaign)["__wrapped__"]
    )
    workspace, _ = campaign(tmp_path_factory)
    return next(iter(job_records(workspace)))


def _record(declarations: Mapping[str, object], *, timeline: object = ()) -> JobRecord:
    return JobRecord(
        workspace_root=Path("."),
        workspace_id="ws",
        job_id="job",
        job_key="job--job",
        job={},
        runner_provenance=None,
        state="succeeded",
        failure=None,
        placement=PurePosixPath("project"),
        payload_path=PurePosixPath("project/job--job"),
        workdir_path=None,
        data_path=None,
        data_generation=None,
        provenance={"activations": timeline},
        runner_steps=None,
        children={},
        declarations=cast(Mapping[str, Mapping[str, Mapping[str, object] | None]], declarations),
    )


def test_declared_provenance_builds_edges_and_uri() -> None:
    record = _record(
        {
            "provenance": {
                "declared": {
                    "workflow_declaration_uri": "https://example.test/workflows/relax",
                    "inputs": {"initial": {"type": "structures", "id": "s1"}},
                    "artifacts": {"relaxed": {"type": "structures", "id": "s2"}},
                    "outputs": {"energy": {"type": "records", "id": "e1"}},
                },
                "observed": None,
            }
        }
    )

    run = run_record(record)
    assert run.workflow_declaration_uri == "https://example.test/workflows/relax"
    assert run.source_id == "ws:job"
    assert run.immutable_id is None
    assert [(edge.label, edge.entry_type, edge.entry_id) for edge in run.inputs] == [("initial", "structures", "s1")]
    assert [(edge.label, edge.entry_type, edge.entry_id) for edge in run.artifacts] == [("relaxed", "structures", "s2")]
    assert [(edge.label, edge.entry_type, edge.entry_id) for edge in run.outputs] == [("energy", "records", "e1")]


def test_observed_replaces_declared_wholesale_and_workflow_uri_falls_back() -> None:
    record = _record(
        {
            "provenance": {
                "declared": {
                    "workflow_declaration_uri": "https://example.test/provenance/declared",
                    "inputs": {"initial": {"type": "structures", "id": "s1"}},
                },
                "observed": {"outputs": {"energy": {"type": "records", "id": "e1"}}},
            },
            "workflow": {
                "declared": {"$id": "https://example.test/workflows/declared"},
                "observed": {},
            },
        }
    )

    run = run_record(record)
    assert run.inputs == () and run.artifacts == ()
    assert [(edge.label, edge.entry_id) for edge in run.outputs] == [("energy", "e1")]
    assert run.workflow_declaration_uri == "https://example.test/workflows/declared"


def test_workflow_id_supplies_uri_without_provenance() -> None:
    run = run_record(
        _record({"workflow": {"declared": {"$id": "https://example.test/workflows/v1"}, "observed": None}})
    )
    assert run.inputs == () and run.artifacts == () and run.outputs == ()
    assert run.workflow_declaration_uri == "https://example.test/workflows/v1"


def test_packaged_workflow_id_supplies_uri_without_provenance() -> None:
    workflow = registered_workflow("vasp-relax")
    assert workflow is not None
    run = run_record(_record({"workflow": {"declared": workflow.declarations["workflow"], "observed": None}}))
    assert run.workflow_declaration_uri == "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax"


def test_explicit_null_provenance_uri_does_not_fall_back() -> None:
    run = run_record(
        _record(
            {
                "provenance": {"declared": {"workflow_declaration_uri": None}, "observed": None},
                "workflow": {"declared": {"$id": "https://example.test/workflows/v1"}, "observed": None},
            }
        )
    )
    assert run.workflow_declaration_uri is None


def test_explicit_null_provenance_uri_skips_malformed_workflow_id() -> None:
    run = run_record(
        _record(
            {
                "provenance": {"declared": {"workflow_declaration_uri": None}, "observed": None},
                "workflow": {"declared": {"$id": 42}, "observed": None},
            }
        )
    )
    assert run.workflow_declaration_uri is None


def test_non_string_provenance_uri_names_identity_and_source() -> None:
    with pytest.raises(ValueError, match=r"ws:job.*provenance\.workflow_declaration_uri"):
        run_record(_record({"provenance": {"declared": {"workflow_declaration_uri": 42}, "observed": None}}))


def test_whitespace_padded_workflow_id_names_identity_and_source() -> None:
    with pytest.raises(ValueError, match=r"ws:job.*workflow \$id"):
        run_record(
            _record({"workflow": {"declared": {"$id": " https://example.test/workflows/v1 "}, "observed": None}})
        )


def test_null_workflow_id_names_identity_and_source() -> None:
    with pytest.raises(ValueError, match=r"ws:job.*workflow \$id"):
        run_record(_record({"workflow": {"declared": {"$id": None}, "observed": None}}))


@pytest.mark.parametrize(
    "document, member",
    [
        ({"inputs": {"initial": {"type": "structures", "id": "s1", "extra": True}}}, "inputs"),
        ({"outputs": []}, "outputs"),
    ],
)
def test_malformed_provenance_names_identity_member_and_label(document: dict[str, object], member: str) -> None:
    with pytest.raises(ValueError, match=rf"ws:job.*{member}"):
        run_record(_record({"provenance": {"declared": document, "observed": None}}))


def test_last_modified_is_the_latest_aware_finished_timestamp() -> None:
    older = "2026-08-06T10:00:00.000000Z"
    newer = "2026-08-06T11:00:00.000000Z"
    record = _record(
        {},
        timeline=[
            {"attempts": [{"finished_at": older}, {"finished_at": "not-a-timestamp"}]},
            {"attempts": [{"finished_at": newer}]},
        ],
    )
    run = run_record(record)
    assert run.last_modified == datetime(2026, 8, 6, 11, tzinfo=UTC)
    assert run.last_modified is not None and run.last_modified.tzinfo is not None


def test_last_modified_is_aware_on_a_really_run_job(real_record: JobRecord) -> None:
    run = run_record(real_record)
    assert run.last_modified is not None and run.last_modified.tzinfo is not None
