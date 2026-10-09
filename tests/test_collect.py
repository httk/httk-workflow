"""The results-collect contract, produced by one real campaign on the filesystem kernel.

Nothing here fabricates protocol state: a real campaign — a parent that spawns
two labeled children of which one fails, a second succeeding job and one job
whose runner exits without an outcome — is driven to completion by a real
:class:`httk.workflow.TaskManager`, and every assertion reads what
:func:`httk.workflow.job_records` reports about the workspace it left behind.
"""

import json
from collections.abc import Iterator
from dataclasses import replace
from typing import Any, cast

import httk.core
import pytest
from httk.core.cli import CLIContext
from httk.core.digests import sha256_file

from conftest import register_ws
from httk.workflow import FormatError, JobRecord, Workspace, job_records
from httk.workflow import collecting as collecting_module
from httk.workflow import scaffold as scaffold_module
from httk.workflow.collecting import COLLECT_FORMAT, COLLECT_FORMAT_VERSION
from httk.workflow.compat.pwd import PACKAGE
from httk.workflow.scaffold import WorkflowProvider
from httk.workflow.workflow_cli import command
from v3_helpers import find, install, run, submit, workspace

pytestmark = pytest.mark.xdist_group("collect-campaign")

_SPAWN = {
    "children": [
        {
            "label": "alpha",
            "script": {"start": "succeed"},
            "placement": "project/children",
            "parameters": {"files": {"start": {"energy.txt": "-10.5"}}},
        },
        {
            "label": "beta",
            "script": {"start": "fail:calculate.diverged"},
            "placement": "project/children",
            "parameters": {"files": {"start": {"energy.txt": "-10.5"}}},
        },
    ],
    "condition": "all_terminal",
    "next_step": "gather",
}


@pytest.fixture(scope="module")
def campaign(tmp_path_factory: pytest.TempPathFactory) -> tuple[Workspace, dict[str, Any]]:
    """Run one complete campaign and return its finished workspace."""

    root = tmp_path_factory.mktemp("collect")
    ws = workspace(root / "workspace")
    installed = install(ws, root / "package")
    parent = submit(
        ws,
        installed,
        {"start": "spawn", "gather": "succeed"},
        tag="campaign",
        placement="project/campaign",
        parameters={
            "structure": "Si",
            "spawn": _SPAWN,
            "files": {"gather": {"report.json": json.dumps({"succeeded": ["alpha"]})}},
            "runner_steps": ["start", "gather"],
        },
        environment={"declared": {"timeout": {"type": "integer"}}, "overrides": {"timeout": 60}},
    )
    single = submit(
        ws,
        installed,
        {"start": "succeed"},
        tag="single",
        placement="project/single",
        parameters={"files": {"start": {"result.txt": "done"}}},
    )
    crashed = submit(ws, installed, {"start": "exit"}, tag="crashed", placement="project/crashed")
    run(ws)
    identifiers: dict[str, Any] = {"parent": parent.job_id, "single": single.job_id, "crashed": crashed.job_id}
    identifiers["installed"] = installed
    for label in ("alpha", "beta"):
        ref = next(found for found in _all(ws) if found.job_key.startswith(f"{label}--"))
        identifiers[label] = ref.job_id
    return ws, identifiers


def _all(ws: Workspace) -> Iterator[Any]:
    from httk.workflow import _kernel

    for state in ("succeeded", "failed"):
        yield from _kernel.list_jobs(ws, state)


def _by_label(records: list[JobRecord]) -> dict[str, JobRecord]:
    """Key records by the tag their job key carries, which is unique here."""

    return {record.job_key.split("--")[0]: record for record in records}


# ---------------------------------------------------------------------------
# What a default collect reports
# ---------------------------------------------------------------------------


def test_a_default_collect_yields_only_the_succeeded_jobs(campaign: tuple[Workspace, dict[str, Any]]) -> None:
    ws, identifiers = campaign
    records = _by_label(list(job_records(ws)))

    # The failed child and the crashed job are not part of a default collect; everything that succeeded is.
    assert sorted(records) == ["alpha", "campaign", "single"]
    assert {record.state for record in records.values()} == {"succeeded"}

    parent = records["campaign"]
    ref = find(ws, identifiers["parent"])
    assert parent.job_id == identifiers["parent"] == ref.job_id
    assert parent.job_key == f"campaign--{identifiers['parent']}"
    assert parent.placement.as_posix() == "project/campaign"
    # The payload path is where the job was read: its current directory, relative to the workspace.
    assert parent.payload_path.as_posix() == f"jobs/succeeded/project/campaign/{ref.path.name}"
    assert parent.payload == ref.path
    assert parent.workdir_path is not None
    assert parent.workdir_path.as_posix() == f"{parent.payload_path.as_posix()}/run"
    assert parent.payload.is_dir() and parent.workdir is not None and parent.workdir.is_dir()
    from httk.workflow.collecting import _workspace_file_record

    url = _workspace_file_record(parent, "report.json").url  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]
    assert url == f"{parent.payload_path.as_posix()}/run/report.json"
    assert json.loads((parent.workdir / "report.json").read_text(encoding="utf-8")) == {"succeeded": ["alpha"]}
    # This job committed no data, so it has no data directory.
    assert parent.data_path is None and parent.data is None
    assert parent.failure is None and not parent.gaps
    assert parent.runner_steps == ("start", "gather")
    assert parent.runner_description is None


def test_a_record_pins_the_job_digest_and_the_installed_workflow(campaign: tuple[Workspace, dict[str, Any]]) -> None:
    ws, identifiers = campaign
    installed = identifiers["installed"]
    parent = _by_label(list(job_records(ws)))["campaign"]

    # The digest of a record is the digest of the stored job.json bytes, so a
    # consumer can verify the definition it was handed against the payload.
    assert parent.job["digest"] == sha256_file(parent.payload / "job.json")
    assert parent.job["id"] == identifiers["parent"]
    assert parent.job["workflow"] == installed.id and parent.job["workflow_name"] == "demo"
    assert parent.job["initial_step"] == "start"
    assert cast(dict[str, object], parent.job["parameters"])["structure"] == "Si"
    assert parent.job["environment"] == {"declared": {"timeout": {"type": "integer"}}, "overrides": {"timeout": 60}}
    assert "runner" not in parent.job and "workdir" not in parent.job
    assert parent.job["claim"] == {"pool": "default", "required_capabilities": []}
    policy = parent.job["retry_policy"]
    assert isinstance(policy, dict) and policy["maximum_attempts_per_activation"] == 1
    # The installed workflow the job ran is pinned by its tree digest.
    pin = {"id": installed.id, "tree_sha256": installed.record["tree_sha256"]}
    assert parent.runner_provenance == pin

    children = _by_label(list(job_records(ws, states=("succeeded", "failed"))))
    for label in ("alpha", "beta"):
        assert children[label].job["workflow"] == installed.id
        assert children[label].runner_provenance == pin


# ---------------------------------------------------------------------------
# Failures and state selection
# ---------------------------------------------------------------------------


def test_collecting_several_states_includes_the_failure_of_a_failed_job(
    campaign: tuple[Workspace, dict[str, Any]],
) -> None:
    ws, identifiers = campaign
    records = _by_label(list(job_records(ws, states=("succeeded", "failed"))))
    assert sorted(records) == ["alpha", "beta", "campaign", "crashed", "single"]

    failed = records["beta"]
    assert failed.state == "failed" and failed.job_id == identifiers["beta"]
    assert failed.failure is not None
    assert failed.failure.code == "calculate.diverged"
    assert failed.failure.message == "did not converge"
    assert failed.failure.details == {"cycles": 3}
    assert failed.failure.retryable is False
    assert failed.as_mapping()["failure"] == {
        "code": "calculate.diverged",
        "message": "did not converge",
        "details": {"cycles": 3},
    }
    # A failed job is still a result: its workdir holds what the attempt wrote.
    assert failed.workdir is not None
    assert (failed.workdir / "energy.txt").read_text(encoding="utf-8") == "-10.5"

    crashed = records["crashed"]
    assert crashed.failure is not None and crashed.failure.code == "process_failure"
    assert crashed.failure.details is not None and crashed.failure.details["exit_status"] == 3


def test_a_collect_refuses_a_state_no_finished_job_can_be_in(campaign: tuple[Workspace, dict[str, Any]]) -> None:
    ws, _ = campaign
    with pytest.raises(ValueError, match="cannot be collected"):
        list(job_records(ws, states=("ready",)))
    with pytest.raises(ValueError, match="at least one state kind"):
        list(job_records(ws, states=()))


def test_a_collect_is_a_lazy_iterator_over_one_listing(campaign: tuple[Workspace, dict[str, Any]]) -> None:
    ws, _ = campaign
    records = job_records(ws)
    assert isinstance(records, Iterator) and not isinstance(records, list)
    assert isinstance(next(records), JobRecord)
    assert sum(1 for _ in records) == 2


# ---------------------------------------------------------------------------
# The run-log timeline
# ---------------------------------------------------------------------------


def test_the_provenance_timeline_lists_activations_and_attempts_in_order(
    campaign: tuple[Workspace, dict[str, Any]],
) -> None:
    ws, _ = campaign
    parent = _by_label(list(job_records(ws)))["campaign"]
    provenance = parent.provenance
    assert provenance["gaps"] is False
    activations = cast(list[dict[str, Any]], provenance["activations"])

    # Two activations: the step that spawned and waited, then the step the satisfied join started.
    assert [entry["step"] for entry in activations] == ["start", "gather"]
    assert [entry["activation_ordinal"] for entry in activations] == [1, 2]
    # state.json names the reason of the current activation only.
    assert [entry["reason"] for entry in activations] == [None, "join"]
    assert len({str(entry["activation_id"]) for entry in activations}) == 2
    for entry in activations:
        attempts = entry["attempts"]
        assert isinstance(attempts, list) and len(attempts) == 1
        attempt = attempts[0]
        assert attempt["ordinal"] == 1 and attempt["owner_id"] and attempt["failure"] is None
        # Every timestamp is what the run log recorded, in the order it happened.
        assert str(attempt["claimed_at"]) <= str(attempt["started_at"]) <= str(attempt["finished_at"])
    assert activations[0]["attempts"][0]["outcome_action"] == "wait"
    assert activations[1]["attempts"][0]["outcome_action"] == "succeed"


def test_the_provenance_of_a_failed_attempt_records_the_outcome_and_the_failure(
    campaign: tuple[Workspace, dict[str, Any]],
) -> None:
    ws, _ = campaign
    failed = _by_label(list(job_records(ws, states=("failed",))))["beta"]
    activations = cast(list[dict[str, Any]], failed.provenance["activations"])
    assert len(activations) == 1 and activations[0]["step"] == "start"
    attempts = activations[0]["attempts"]
    assert len(attempts) == 1
    assert attempts[0]["outcome_action"] == "fail"
    assert attempts[0]["failure"]["code"] == "calculate.diverged"
    assert attempts[0]["failure"]["details"] == {"cycles": 3}


def test_a_torn_last_run_log_line_is_skipped_and_a_damaged_one_is_a_gap(
    campaign: tuple[Workspace, dict[str, Any]],
) -> None:
    ws, identifiers = campaign
    from httk.workflow.collecting import record_of

    ref = find(ws, identifiers["single"])
    log = ref.path / "logs" / "runlog.jsonl"
    original = log.read_bytes()
    try:
        log.write_bytes(original + b'{"at": "torn')
        record = record_of(ws, ref)
        assert record is not None and not record.gaps
        log.write_bytes(b"not json\n" + original)
        record = record_of(ws, ref)
        assert record is not None and record.gaps
        assert len(cast(list[object], record.provenance["activations"])) == 1
    finally:
        log.write_bytes(original)


# ---------------------------------------------------------------------------
# Campaign trees
# ---------------------------------------------------------------------------


def test_children_carry_their_spawn_labels_so_a_campaign_collects_as_a_tree(
    campaign: tuple[Workspace, dict[str, Any]],
) -> None:
    ws, identifiers = campaign
    records = _by_label(list(job_records(ws, states=("succeeded", "failed"))))
    parent = records["campaign"]

    assert sorted(parent.children) == ["alpha", "beta"]
    assert parent.children["alpha"] == {
        "job_id": identifiers["alpha"],
        "job_key": f"alpha--{identifiers['alpha']}",
        "kind": "succeeded",
    }
    assert parent.children["beta"]["job_id"] == identifiers["beta"]
    assert parent.children["beta"]["kind"] == "failed"
    # Following the tree is one collect per node: a child spawned nothing itself.
    assert records["alpha"].children == {} and records["beta"].children == {}
    assert records["single"].children == {}


def test_the_placement_filter_collects_one_subtree(campaign: tuple[Workspace, dict[str, Any]]) -> None:
    ws, _ = campaign
    children = list(job_records(ws, states=("succeeded", "failed"), placement="project/children"))
    assert sorted(_by_label(children)) == ["alpha", "beta"]
    assert {record.placement.as_posix() for record in children} == {"project/children"}
    single = list(job_records(ws, placement="project/single"))
    assert [record.job_key.split("--")[0] for record in single] == ["single"]
    # A placement no job sits below collects nothing rather than everything.
    assert list(job_records(ws, placement="project/absent")) == []


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_the_command_streams_one_record_per_line_and_round_trips(
    campaign: tuple[Workspace, dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    ws, _ = campaign
    context = CLIContext("httk", ws.root)
    name = register_ws(context, ws.root)

    assert command(["collect", "--workspace", name, "--raw"], context) == 0
    lines = capsys.readouterr().out.splitlines()
    summary = json.loads(lines[-1])
    assert summary["format"] == "httk-workflow-collect-summary"
    assert summary["collected"] == 3 and summary["skipped_unreadable"] == 0
    labels = []
    for line in lines[:-1]:
        mapping = json.loads(line)
        assert mapping["format"] == COLLECT_FORMAT and mapping["format_version"] == COLLECT_FORMAT_VERSION
        record = JobRecord.from_mapping(mapping)
        # A record survives the wire: what came back serializes to what went out.
        assert record.as_mapping() == mapping
        assert record.workspace_root == ws.root
        labels.append(record.job_key.split("--")[0])
    assert sorted(labels) == ["alpha", "campaign", "single"]


def test_the_command_selects_states_and_placements(
    campaign: tuple[Workspace, dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    ws, identifiers = campaign
    context = CLIContext("httk", ws.root)
    name = register_ws(context, ws.root)

    argv = ["collect", "--workspace", name, "--state", "failed", "--placement", "project/children", "--raw"]
    assert command(argv, context) == 0
    lines = capsys.readouterr().out.splitlines()
    assert json.loads(lines[-1])["format"] == "httk-workflow-collect-summary"
    assert len(lines[:-1]) == 1
    record = JobRecord.from_mapping(json.loads(lines[0]))
    assert record.job_id == identifiers["beta"] and record.state == "failed"

    # An unusable state is refused by the parser rather than silently ignored.
    assert command(["collect", "--workspace", name, "--state", "ready"], context) == 2
    assert "invalid choice" in capsys.readouterr().err


def test_collect_assembles_overlay_edges_and_products(
    campaign: tuple[Workspace, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, _ = campaign
    record = next(job_records(workspace))
    document = {
        "$id": "https://schemas.example.test/workflows/collect",
        "inputs": [{"name": "initial_structure", "entry_type": "structures"}],
        "outputs": [
            {"name": "relaxed_structure", "entry_type": "structures"},
            {"name": "total_energy", "entry_type": "records"},
        ],
    }
    record = replace(
        record,
        job={**record.job, "workflow": "tests.framework.collect"},
        declarations={
            "workflow": {"declared": document, "observed": None},
            "provenance": {
                "declared": {
                    "inputs": {"initial_structure": {"type": "structures", "id": "source-1"}},
                    "artifacts": {"unrelated": {"type": "records", "id": "artifact-1"}},
                    "outputs": {
                        "relaxed_structure": {"type": "structures", "id": "old-structure"},
                        "unrelated": {"type": "records", "id": "output-1"},
                    },
                },
                "observed": None,
            },
        },
    )

    class Structure:
        type = "structures"
        id = "new-structure"

    class Energy:
        type = "records"
        id = "new-energy"

    provider = WorkflowProvider(
        workflow_id="tests.framework.collect",
        alias="tests-framework-collect",
        runner_package=PACKAGE,
        runner_file="runner.py",
        initial_step="run",
        declarations={"workflow": document},
        outputs={
            "relaxed_structure": {
                "entry_type": "structures",
                "role": "relaxed_structure",
                "product_of": "initial_structure",
            },
            "total_energy": {
                "entry_type": "records",
                "role": "total_energy",
                "product_of": "relaxed_structure",
            },
        },
        collector=lambda _record: {"relaxed_structure": Structure(), "total_energy": Energy()},
    )
    monkeypatch.setitem(scaffold_module._WORKFLOW_PROVIDERS, provider.workflow_id, provider)
    monkeypatch.setattr(collecting_module, "job_records", lambda *_args, **_kwargs: iter((record,)))

    collected = next(collecting_module.collect(workspace))
    assert cast(Any, collected.outputs["relaxed_structure"]).id == "new-structure"
    assert cast(Any, collected.outputs["total_energy"]).id == "new-energy"
    assert [edge.label for edge in collected.run.inputs] == ["initial_structure"]
    assert [edge.label for edge in collected.run.artifacts] == ["unrelated", "relaxed_structure", "total_energy"]
    assert [edge.label for edge in collected.run.outputs] == ["relaxed_structure", "unrelated", "total_energy"]
    assert collected.products == (
        httk.core.ProductLink(
            source_type="structures",
            source_id="source-1",
            target_type="structures",
            target_id="new-structure",
            label="relaxed_structure",
            workflow_declaration_uri=str(document["$id"]),
        ),
        httk.core.ProductLink(
            source_type="structures",
            source_id="new-structure",
            target_type="records",
            target_id="new-energy",
            label="total_energy",
            workflow_declaration_uri=str(document["$id"]),
        ),
    )

    changed_provider = WorkflowProvider(
        workflow_id="tests.framework.collect",
        alias="tests-framework-collect",
        runner_package=PACKAGE,
        runner_file="runner.py",
        initial_step="run",
        declarations={"workflow": {"outputs": [{"name": "different", "entry_type": "records"}]}},
        collector=lambda _record: {"relaxed_structure": Structure(), "total_energy": Energy()},
    )
    monkeypatch.setitem(scaffold_module._WORKFLOW_PROVIDERS, changed_provider.workflow_id, changed_provider)
    recollected = next(collecting_module.collect(workspace))
    assert recollected.outputs.keys() == collected.outputs.keys()
    assert recollected.run == collected.run
    assert recollected.products == ()

    incomplete_provider = replace(
        provider,
        collector=lambda _record: {"total_energy": Energy()},
    )
    monkeypatch.setitem(scaffold_module._WORKFLOW_PROVIDERS, incomplete_provider.workflow_id, incomplete_provider)
    incomplete = next(collecting_module.collect(workspace))
    assert incomplete.unfulfilled == ("relaxed_structure",)
    assert incomplete.products == ()


def test_a_record_refuses_a_mapping_of_another_format() -> None:
    with pytest.raises(FormatError, match="httk-workflow-collect"):
        JobRecord.from_mapping({"format": "something-else", "format_version": COLLECT_FORMAT_VERSION})
