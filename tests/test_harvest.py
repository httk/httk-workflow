"""The results-harvest contract, produced by one real campaign.

Nothing here fabricates protocol state: a real campaign — a parent that spawns
two labeled children of which one fails, a second job sharing another published
runner, and one job running a packaged ``pkg:`` runner — is driven to completion
by a real :class:`httk.workflow.TaskManager`, and every assertion reads what
:func:`httk.workflow.harvest` reports about the workspace that campaign left
behind.
"""

import json
from collections.abc import Iterator
from importlib import metadata
from pathlib import Path

import pytest
from conftest import register_ws
from httk.core import CLIContext

from httk.workflow import (
    FormatError,
    HarvestRecord,
    TaskManager,
    Workspace,
    harvest,
)
from httk.workflow._util import sha256_file
from httk.workflow.harvesting import HARVEST_FORMAT, module_distribution
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.runners import runner_path, runner_reference
from httk.workflow.vasp.runners import PACKAGE
from httk.workflow.workflow_cli import command

_SRC = str(Path(__file__).parents[1] / "src")

# One published runner implements the whole campaign: the children are
# synthesized from a step name and their inputs, and they inherit the runner of
# the parent that spawned them.
_CAMPAIGN_RUNNER = f'''#!/usr/bin/env python3
import json
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.harvest")


@run.step
def branch(a):
    for label, failing in (("alpha", False), ("beta", True)):
        a.spawn(
            ChildSpec(
                step="calculate",
                inputs={{"failing": failing}},
                maximum_attempts_per_activation=1,
            ),
            label=label,
            placement="project/children",
        )
    a.gather("collect", when="all_terminal")


@run.step
def calculate(a):
    (a.workdir / "energy.txt").write_text("-10.5", encoding="utf-8")
    if a.input("failing"):
        a.fail("calculate.diverged", "the calculation did not converge", details={{"cycles": 3}})
    else:
        a.succeed()


@run.step
def collect(a):
    (a.workdir / "report.json").write_text(
        json.dumps({{"succeeded": [child.label for child in a.children.succeeded]}}),
        encoding="utf-8",
    )
    a.succeed()


raise SystemExit(run.main())
'''

_SINGLE_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.harvest.single")


@run.step
def only(a):
    (a.workdir / "result.txt").write_text("done", encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
'''

_CAMPAIGN_STEPS = ("branch", "calculate", "collect")


def _publish(workspace: Workspace, source: Path, text: str, name: str) -> dict[str, object]:
    """Publish one runner into the workspace store and return its reference."""

    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(text, encoding="utf-8")
    return workspace.publish_runner(source, name=name)


@pytest.fixture(scope="module")
def campaign(tmp_path_factory: pytest.TempPathFactory) -> tuple[Workspace, dict[str, str]]:
    """Run one complete campaign and return its finished workspace."""

    root = tmp_path_factory.mktemp("harvest")
    workspace = Workspace.initialize(root / "workspace")
    identifiers: dict[str, str] = {}

    campaign_runner = _publish(workspace, root / "runners" / "campaign.py", _CAMPAIGN_RUNNER, "campaign/run.py")
    parent = prepare_job_payload(
        root / "parent",
        JobSpec(
            name="Harvest campaign",
            workflow="tests.harvest",
            runner_path=str(campaign_runner["path"]),
            runner_source="workspace",
            runner_sha256=str(campaign_runner["sha256"]),
            tag="campaign",
            initial_step="branch",
            maximum_attempts_per_activation=1,
            inputs={"structure": "Si"},
        ),
    )
    workspace.submit(root / "parent", "project/campaign")
    identifiers["parent"] = parent.id

    single_runner = _publish(workspace, root / "runners" / "single.py", _SINGLE_RUNNER, "single/run.py")
    single = prepare_job_payload(
        root / "single",
        JobSpec(
            name="Harvest single",
            workflow="tests.harvest.single",
            runner_path=str(single_runner["path"]),
            runner_source="workspace",
            runner_sha256=str(single_runner["sha256"]),
            tag="single",
            initial_step="only",
            maximum_attempts_per_activation=1,
        ),
    )
    workspace.submit(root / "single", "project/single")
    identifiers["single"] = single.id

    # A packaged runner named by the reserved pkg: form, resolved by the manager
    # inside its own module allowlist. Its payload has no starting structure, so
    # the runner refuses the job by name after one real attempt.
    reference = runner_reference("vasp_static.py")
    packaged = prepare_job_payload(
        root / "packaged",
        JobSpec(
            name="Harvest packaged",
            workflow="httk.vasp.static",
            runner_path=str(reference["path"]),
            runner_source="installed",
            runner_sha256=str(reference["sha256"]),
            tag="packaged",
            initial_step="prepare",
            maximum_attempts_per_activation=1,
        ),
    )
    workspace.submit(root / "packaged", "project/packaged")
    identifiers["packaged"] = packaged.id

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=180.0)
    identifiers["runner_sha256"] = str(campaign_runner["sha256"])
    for label in ("alpha", "beta"):
        marker = next(found for found in workspace.scan_markers() if found.job_key.startswith(f"{label}--"))
        identifiers[label] = marker.job_id
    return workspace, identifiers


def _by_label(records: list[HarvestRecord]) -> dict[str, HarvestRecord]:
    """Key records by the tag their job key carries, which is unique here."""

    return {record.job_key.split("--")[0]: record for record in records}


# ---------------------------------------------------------------------------
# What a default harvest reports
# ---------------------------------------------------------------------------


def test_a_default_harvest_yields_only_the_succeeded_jobs(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, identifiers = campaign
    records = _by_label(list(harvest(workspace)))

    # The failed child and the refused packaged job are not part of a default
    # harvest; everything that succeeded is.
    assert sorted(records) == ["alpha", "campaign", "single"]
    assert {record.state for record in records.values()} == {"succeeded"}

    parent = records["campaign"]
    assert parent.job_id == identifiers["parent"]
    assert parent.job_key == f"campaign--{identifiers['parent']}"
    assert parent.placement.as_posix() == "project/campaign"
    assert parent.payload_path.as_posix() == f"project/campaign/{parent.job_key}"
    assert parent.workdir_path is not None
    assert parent.workdir_path.as_posix() == f"{parent.payload_path.as_posix()}/run"
    # The workspace-relative members resolve into the workspace they came from.
    assert parent.payload == workspace.root / "project" / "campaign" / parent.job_key
    assert parent.payload.is_dir() and parent.workdir is not None and parent.workdir.is_dir()
    assert json.loads((parent.workdir / "report.json").read_text(encoding="utf-8")) == {"succeeded": ["alpha"]}
    # This job publishes no transactional data, so it has no data directory.
    assert parent.data_path is None and parent.data is None and parent.data_generation is None
    assert parent.failure is None and not parent.gaps
    assert parent.runner_steps == _CAMPAIGN_STEPS
    # The two members reserved for a later phase are inert and say so.
    assert parent.declarations == {} and parent.runner_description is None


def test_a_record_pins_the_job_digest_and_the_runner_that_executed_it(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, identifiers = campaign
    parent = _by_label(list(harvest(workspace)))["campaign"]

    # The digest of a record is the digest of the stored job.json bytes, so a
    # consumer can verify the definition it was handed against the payload.
    assert parent.job["digest"] == sha256_file(parent.payload / "job.json")
    assert parent.job["id"] == identifiers["parent"]
    assert parent.job["workflow"] == "tests.harvest"
    assert parent.job["initial_step"] == "branch"
    assert parent.job["inputs"] == {"structure": "Si"}
    assert parent.job["runner"] == {
        "backend": "path",
        "source": "workspace",
        "path": "campaign/run.py",
        "sha256": identifiers["runner_sha256"],
        "arguments": [],
    }
    assert parent.job["claim"] == {"pool": "default", "required_capabilities": []}
    policy = parent.job["retry_policy"]
    assert isinstance(policy, dict) and policy["maximum_attempts_per_activation"] == 1
    # A shared runner is pinned by its digest, not by a distribution.
    assert parent.runner_provenance is None

    # Every child inherited exactly the runner of the parent that spawned it.
    children = _by_label(list(harvest(workspace, states=("succeeded", "failed"))))
    for label in ("alpha", "beta"):
        assert children[label].job["runner"] == parent.job["runner"]
        assert children[label].job["workflow"] == "tests.harvest"


# ---------------------------------------------------------------------------
# Failures and state selection
# ---------------------------------------------------------------------------


def test_harvesting_several_states_includes_the_failure_of_a_failed_job(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, identifiers = campaign
    records = _by_label(list(harvest(workspace, states=("succeeded", "failed"))))
    assert sorted(records) == ["alpha", "beta", "campaign", "packaged", "single"]

    failed = records["beta"]
    assert failed.state == "failed" and failed.job_id == identifiers["beta"]
    assert failed.failure is not None
    assert failed.failure.code == "calculate.diverged"
    assert failed.failure.message == "the calculation did not converge"
    assert failed.failure.details == {"cycles": 3}
    assert failed.failure.retryable is False
    # The unified failure shape is exactly what the record serializes.
    assert failed.as_mapping()["failure"] == {
        "code": "calculate.diverged",
        "message": "the calculation did not converge",
        "details": {"cycles": 3},
    }
    # A failed job is still a result: its workdir holds what the attempt wrote.
    assert failed.workdir is not None
    assert (failed.workdir / "energy.txt").read_text(encoding="utf-8") == "-10.5"


def test_a_harvest_refuses_a_state_no_finished_job_can_be_in(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, _ = campaign
    with pytest.raises(ValueError, match="cannot be harvested"):
        list(harvest(workspace, states=("ready",)))
    with pytest.raises(ValueError, match="at least one state kind"):
        list(harvest(workspace, states=()))


def test_a_harvest_is_a_lazy_iterator_over_one_scan(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, _ = campaign
    records = harvest(workspace)
    assert isinstance(records, Iterator) and not isinstance(records, list)
    first = next(records)
    assert isinstance(first, HarvestRecord)
    assert sum(1 for _ in records) == 2


# ---------------------------------------------------------------------------
# The journal-derived timeline
# ---------------------------------------------------------------------------


def test_the_provenance_timeline_lists_activations_and_attempts_in_order(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, _ = campaign
    parent = _by_label(list(harvest(workspace)))["campaign"]
    provenance = parent.provenance
    assert provenance["gaps"] is False
    activations = provenance["activations"]
    assert isinstance(activations, list)

    # Two activations: the step that spawned and waited, then the step the
    # satisfied join started.
    assert [entry["step"] for entry in activations] == ["branch", "collect"]
    assert [entry["activation_ordinal"] for entry in activations] == [1, 2]
    assert [entry["reason"] for entry in activations] == ["submitted", "join_satisfied"]
    assert len({str(entry["activation_id"]) for entry in activations}) == 2

    for entry in activations:
        attempts = entry["attempts"]
        assert isinstance(attempts, list) and len(attempts) == 1
        attempt = attempts[0]
        assert attempt["ordinal"] == 1
        assert attempt["manager_id"] and attempt["writer_id"] and attempt["record_ref"]
        assert attempt["failure"] is None
        # Every timestamp is what the frame recorded, in the order they happened.
        assert str(attempt["claimed_at"]) <= str(attempt["started_at"]) <= str(attempt["finished_at"])
    assert activations[0]["attempts"][0]["outcome_action"] == "wait"
    assert activations[1]["attempts"][0]["outcome_action"] == "succeed"


def test_the_provenance_of_a_failed_attempt_records_the_outcome_and_the_failure(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, _ = campaign
    failed = _by_label(list(harvest(workspace, states=("failed",))))["beta"]
    activations = failed.provenance["activations"]
    assert isinstance(activations, list) and len(activations) == 1
    attempts = activations[0]["attempts"]
    assert len(attempts) == 1
    assert attempts[0]["outcome_action"] == "fail"
    assert attempts[0]["failure"] == {
        "code": "calculate.diverged",
        "message": "the calculation did not converge",
        "details": {"cycles": 3},
    }
    assert activations[0]["step"] == "calculate"


# ---------------------------------------------------------------------------
# Campaign trees
# ---------------------------------------------------------------------------


def test_children_carry_their_spawn_labels_so_a_campaign_harvests_as_a_tree(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, identifiers = campaign
    records = _by_label(list(harvest(workspace, states=("succeeded", "failed"))))
    parent = records["campaign"]

    assert list(parent.children) == ["alpha", "beta"]
    assert parent.children["alpha"] == {
        "job_id": identifiers["alpha"],
        "job_key": f"alpha--{identifiers['alpha']}",
        "kind": "succeeded",
    }
    assert parent.children["beta"]["job_id"] == identifiers["beta"]
    assert parent.children["beta"]["kind"] == "failed"
    # Following the tree is one harvest per node: a child spawned nothing itself.
    assert records["alpha"].children == {} and records["beta"].children == {}
    assert records["single"].children == {}


def test_the_placement_filter_harvests_one_subtree(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, _ = campaign
    children = list(harvest(workspace, states=("succeeded", "failed"), placement="project/children"))
    assert sorted(_by_label(children)) == ["alpha", "beta"]
    assert {record.placement.as_posix() for record in children} == {"project/children"}
    single = list(harvest(workspace, placement="project/single"))
    assert [record.job_key.split("--")[0] for record in single] == ["single"]
    # A placement no job sits below harvests nothing rather than everything.
    assert list(harvest(workspace, placement="project/absent")) == []


# ---------------------------------------------------------------------------
# Runner provenance
# ---------------------------------------------------------------------------


def test_a_packaged_runner_record_names_the_distribution_that_installs_it(
    campaign: tuple[Workspace, dict[str, str]],
) -> None:
    workspace, _ = campaign
    packaged = _by_label(list(harvest(workspace, states=("failed",))))["packaged"]

    assert packaged.state == "failed"
    assert packaged.failure is not None and packaged.failure.code == "vasp.input_missing"
    assert packaged.job["runner"] == {
        "backend": "path",
        "source": "installed",
        "path": f"pkg:{PACKAGE}/vasp_static.py",
        "sha256": sha256_file(runner_path("vasp_static.py")),
        "arguments": [],
    }
    assert packaged.runner_provenance == {
        "module": PACKAGE,
        "resource": "vasp_static.py",
        "distribution": "httk-workflow",
        "version": metadata.version("httk-workflow"),
    }


def test_a_module_no_installed_distribution_owns_has_no_provenance() -> None:
    assert module_distribution(PACKAGE) == ("httk-workflow", metadata.version("httk-workflow"))
    assert module_distribution("json") is None
    assert module_distribution("tests.no_such_module") is None


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_the_command_streams_one_record_per_line_and_round_trips(
    campaign: tuple[Workspace, dict[str, str]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    workspace, _ = campaign
    context = CLIContext("httk", workspace.root)
    ws = register_ws(context, workspace.root)

    assert command(["harvest", ws], context) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 3
    labels = []
    for line in lines:
        mapping = json.loads(line)
        assert mapping["format"] == HARVEST_FORMAT and mapping["format_version"] == 1
        record = HarvestRecord.from_mapping(mapping)
        # A record survives the wire: what came back serializes to what went out.
        assert record.as_mapping() == mapping
        assert record.workspace_root == workspace.root
        labels.append(record.job_key.split("--")[0])
    assert sorted(labels) == ["alpha", "campaign", "single"]

    assert command(["harvest", ws, "--json"], context) == 0
    array = json.loads(capsys.readouterr().out)
    assert isinstance(array, list) and len(array) == 3
    assert {entry["state"] for entry in array} == {"succeeded"}


def test_the_command_selects_states_and_placements(
    campaign: tuple[Workspace, dict[str, str]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    workspace, identifiers = campaign
    context = CLIContext("httk", workspace.root)
    ws = register_ws(context, workspace.root)

    assert (
        command(
            [
                "harvest",
                ws,
                "--state",
                "failed",
                "--placement",
                "project/children",
                "--jsonl",
            ],
            context,
        )
        == 0
    )
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    record = HarvestRecord.from_mapping(json.loads(lines[0]))
    assert record.job_id == identifiers["beta"] and record.state == "failed"

    # An unusable state is refused by the parser rather than silently ignored.
    assert command(["harvest", ws, "--state", "ready"], context) == 2
    assert "invalid choice" in capsys.readouterr().err


def test_a_record_refuses_a_mapping_of_another_format() -> None:
    with pytest.raises(FormatError, match="httk-workflow-harvest"):
        HarvestRecord.from_mapping({"format": "something-else", "format_version": 1})
