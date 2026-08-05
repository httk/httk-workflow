"""The results-fetch loop, driven end to end over the local remote adapter.

Nothing here fabricates protocol state. One real campaign is run by a real
:class:`httk.workflow.TaskManager` in a *remote* workspace — the one a remote
adapter reaches — and every assertion reads what ``transfer offer``, ``transfer
fetch``, ``transfer retire``, and finally ``harvest`` report about the local
workspace that asked for the results back.
"""

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import (
    HarvestRecord,
    TaskManager,
    Workspace,
    harvest,
)
from httk.workflow.adapters import add_remote
from httk.workflow.projects import PROJECT_DIRECTORY, initialize_project
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.transfers import TRANSFER_DIRECTORY, _payload_digest
from httk.workflow.workflow_cli import command

pytestmark = pytest.mark.xdist_group("fetch-template")

_SRC = str(Path(__file__).parents[1] / "src")

_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.fetch")


@run.step
def compute(a):
    (a.workdir / "energy.txt").write_text("-4.25", encoding="utf-8")
    if a.input("failing"):
        a.fail("compute.diverged", "the calculation did not converge", details={{"cycles": 3}})
    else:
        a.succeed()


raise SystemExit(run.main())
'''


@dataclass(frozen=True)
class Pair:
    """One local workspace, one remote workspace, and the campaign between them."""

    local_root: Path
    remote_root: Path
    context: CLIContext
    ids: dict[str, str]

    @property
    def local(self) -> Workspace:
        return Workspace(self.local_root)

    @property
    def remote(self) -> Workspace:
        return Workspace(self.remote_root)


def _stage(payload: Path, *, failing: bool, runner: dict[str, object]) -> str:
    """Prepare one payload, including the two entries a transfer must preserve."""

    (payload / "files").mkdir(parents=True)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Fetch campaign",
            workflow="tests.fetch",
            runner_path=str(runner["path"]),
            runner_source="workspace",
            runner_sha256=str(runner["sha256"]),
            tag="failing" if failing else "succeeding",
            initial_step="compute",
            maximum_attempts_per_activation=1,
            inputs={"failing": failing},
        ),
    )
    # A helper the job carries rather than references: it is load bearing only
    # while it stays executable, which is exactly what the payload digest pins.
    helper = payload / "files" / "helper.sh"
    helper.write_text("#!/bin/sh\necho helper\n", encoding="utf-8")
    helper.chmod(0o755)
    return job.id


def _plant_legacy_link(workspace: Workspace, job_id: str) -> None:
    """Plant the shape the v1 compatibility executor leaves inside a payload.

    A submission deliberately dereferences symlinks, so the relative link a v1
    parent carries only ever appears in the live payload, exactly as
    ``httk.workflow.compat.v1`` writes it once the job is in the workspace.
    """

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    legacy = workspace.payload_path(marker.placement, marker.job_key) / "ht.task"
    legacy.mkdir()
    (legacy / "inputs").symlink_to("../files")


def _build(root: Path) -> dict[str, str]:
    """Run one campaign on the remote side and leave one job unfinished."""

    local_root = root / "local"
    remote_root = root / "remote"
    initialize_project(local_root, name="fetch-local")
    initialize_project(remote_root, name="fetch-remote")
    Workspace.initialize(local_root)
    Workspace.initialize(remote_root)
    add_remote("cluster", template="local", project=local_root)

    remote = Workspace(remote_root)
    source = root / "runners" / "fetch.py"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(_RUNNER, encoding="utf-8")
    runner = remote.publish_runner(source, name="fetch/run.py")

    identifiers = {
        "succeeded": _stage(root / "succeeding", failing=False, runner=runner),
        "failed": _stage(root / "failing", failing=True, runner=runner),
    }
    remote.submit(root / "succeeding", "project/campaign")
    remote.submit(root / "failing", "project/campaign")
    for job_id in identifiers.values():
        _plant_legacy_link(remote, job_id)
    with TaskManager(remote, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=180.0)

    # Submitted only after the manager stopped, so one job of the remote
    # workspace is still unfinished when the fetch runs.
    identifiers["pending"] = _stage(root / "pending", failing=False, runner=runner)
    remote.submit(root / "pending", "project/later")
    return identifiers


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Run the campaign once; every test works on its own copy of the result."""

    root = tmp_path_factory.mktemp("fetch-template")
    (root / "ids.json").write_text(json.dumps(_build(root)), encoding="utf-8")
    return root


@pytest.fixture
def pair(template: Path, tmp_path: Path) -> Pair:
    root = tmp_path / "pair"
    shutil.copytree(template, root, symlinks=True)
    local_root = root / "local"
    remote_root = root / "remote"
    # The adapter of the copy must name the remote workspace of the copy.
    metadata_path = local_root / PROJECT_DIRECTORY / "remotes" / "cluster" / "remote.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = str(remote_root)
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    identifiers = json.loads((root / "ids.json").read_text(encoding="utf-8"))
    context = CLIContext("httk", local_root)
    register_ws(context, local_root, "home")
    register_ws(context, remote_root, "station", remote="cluster")
    return Pair(local_root, remote_root, context, identifiers)


def _live_bundles(workspace: Workspace) -> list[Path]:
    """Every sealed bundle still sitting in the job tree of *workspace*."""

    return [
        path.parent
        for path in workspace.root.rglob(f"{TRANSFER_DIRECTORY}/manifest.json")
        if workspace.control not in path.parents
    ]


def _offer(pair: Pair, capsys: pytest.CaptureFixture[str], *arguments: str) -> dict[str, Any]:
    argv = [
        "transfer",
        "offer",
        str(pair.remote_root),
        "--destination-workspace-id",
        pair.local.workspace_id,
        "--json",
        *arguments,
    ]
    assert command(argv, pair.context) == 0
    return json.loads(capsys.readouterr().out)


def _fetch(pair: Pair, capsys: pytest.CaptureFixture[str], *arguments: str) -> dict[str, Any]:
    argv = ["transfer", "cluster:station", "home", "--json", *arguments]
    assert command(argv, pair.context) == 0
    return json.loads(capsys.readouterr().out)


# ---------------------------------------------------------------------------
# Offering, on the side that ran the work
# ---------------------------------------------------------------------------


def test_offer_seals_exactly_the_terminal_jobs_and_is_idempotent(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
) -> None:
    document = _offer(pair, capsys)
    assert document["format"] == "httk-workflow-transfer-offer" and document["format_version"] == 1
    assert document["destination_workspace_id"] == pair.local.workspace_id

    offers = document["offers"]
    assert isinstance(offers, list)
    by_id = {str(offer["job_id"]): offer for offer in offers}
    assert set(by_id) == {pair.ids["succeeded"], pair.ids["failed"]}
    assert by_id[pair.ids["succeeded"]]["state"] == "succeeded"
    assert by_id[pair.ids["failed"]]["state"] == "failed"
    for offer in offers:
        assert offer["placement"] == "project/campaign"
        bundle = Path(str(offer["bundle_path"]))
        assert (bundle / TRANSFER_DIRECTORY / "manifest.json").is_file()
        assert _payload_digest(bundle) == offer["payload_sha256"]

    remote = pair.remote
    # A sealed job has no schedulable marker left, and the job that never
    # finished was not touched at all.
    assert remote.find_marker_by_id(pair.ids["succeeded"]) is None
    assert remote.find_marker_by_id(pair.ids["failed"]) is None
    pending = remote.find_marker_by_id(pair.ids["pending"])
    assert pending is not None and pending.kind == "submitted"

    # Repeating the offer reports the same bundles rather than sealing again.
    assert _offer(pair, capsys) == document


def test_offer_selects_states_and_placements(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    document = _offer(pair, capsys, "--state", "succeeded")
    offers = document["offers"]
    assert isinstance(offers, list) and len(offers) == 1
    assert offers[0]["job_id"] == pair.ids["succeeded"]

    # The already sealed job stays offered under a wider selection, and the
    # failed one joins it.
    widened = _offer(pair, capsys, "--state", "succeeded", "--state", "failed")
    widened_offers = widened["offers"]
    assert isinstance(widened_offers, list)
    assert {str(offer["job_id"]) for offer in widened_offers} == {pair.ids["succeeded"], pair.ids["failed"]}

    # A placement no finished job sits below offers nothing.
    assert _offer(pair, capsys, "--placement", "project/later")["offers"] == []


def test_offer_refuses_a_state_no_finished_job_can_be_in(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
) -> None:
    argv = [
        "transfer",
        "offer",
        str(pair.remote_root),
        "--destination-workspace-id",
        pair.local.workspace_id,
        "--state",
        "running",
    ]
    assert command(argv, pair.context) == 2
    assert "invalid choice" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# The fetch round trip
# ---------------------------------------------------------------------------


def test_fetch_brings_the_finished_jobs_home_with_their_payloads_intact(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = _fetch(pair, capsys)
    fetched = {str(entry["job_id"]): entry for entry in list(report["moved"])}
    assert set(fetched) == {pair.ids["succeeded"], pair.ids["failed"]}
    assert fetched[pair.ids["succeeded"]]["state"] == "succeeded"
    assert fetched[pair.ids["failed"]]["state"] == "failed"

    local = pair.local
    for name in ("succeeded", "failed"):
        marker = local.find_marker_by_id(pair.ids[name])
        assert marker is not None and marker.kind == name
        # The placement the job had on the remote is the placement it lands at.
        assert marker.placement.as_posix() == "project/campaign"
        payload = local.payload_path(marker.placement, marker.job_key)

        # What the attempt wrote arrived with the job.
        assert (payload / "run" / "energy.txt").read_text(encoding="utf-8") == "-4.25"

        # The executable bit survived, and the digest that pins it verifies.
        helper = payload / "files" / "helper.sh"
        assert helper.is_file() and helper.stat().st_mode & 0o111
        assert _payload_digest(payload) == fetched[pair.ids[name]]["payload_sha256"]

        # The v1 style relative symlink is still a symlink, still relative, and
        # still resolves inside the payload it travelled with.
        link = payload / "ht.task" / "inputs"
        assert link.is_symlink() and os.readlink(link) == "../files"
        assert (link / "helper.sh").is_file()

    # Nothing is left staged once the payloads are published.
    staging = local.control / "transfers" / "incoming"
    assert not staging.is_dir() or not list(staging.iterdir())


def test_fetch_retires_the_remote_sources_and_repeating_it_does_nothing(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
) -> None:
    first = _fetch(pair, capsys)
    assert len(list(first["moved"])) == 2
    assert {str(entry["status"]) for entry in list(first["retired"])} == {"retired"}

    remote = pair.remote
    # Every source bundle moved under retired/ whole; none is left in the tree.
    retired = sorted((remote.control / "transfers" / "retired").iterdir())
    assert len(retired) == 2
    for entry in retired:
        assert (entry / "bundle" / TRANSFER_DIRECTORY / "manifest.json").is_file()
    assert _live_bundles(remote) == []
    for name in ("succeeded", "failed"):
        assert remote.find_marker_by_id(pair.ids[name]) is None
    pending = remote.find_marker_by_id(pair.ids["pending"])
    assert pending is not None and pending.kind == "submitted"

    # A retired source is not offered again, so a second fetch is a no-op and
    # the imported jobs are not duplicated.
    assert _fetch(pair, capsys) == {"moved": [], "retired": []}
    local = pair.local
    for name in ("succeeded", "failed"):
        assert local.find_marker_by_id(pair.ids[name]) is not None


def test_fetch_resumes_after_an_interruption_between_pull_and_import(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = Workspace.import_bundle
    interrupted = False

    def interrupt(self: Workspace, bundle: str | os.PathLike[str]) -> dict[str, object]:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise RuntimeError("simulated interruption after the bundle was pulled")
        return real_import(self, bundle)

    monkeypatch.setattr(Workspace, "import_bundle", interrupt)
    argv = ["transfer", "cluster:station", "home", "--json"]
    assert command(argv, pair.context) == 2
    assert interrupted

    local = pair.local
    # The pulled bundle is staged and no job was published from it.
    staged = sorted((local.control / "transfers" / "incoming").iterdir())
    assert len(staged) == 1
    assert local.find_marker_by_id(pair.ids["succeeded"]) is None
    assert local.find_marker_by_id(pair.ids["failed"]) is None

    monkeypatch.setattr(Workspace, "import_bundle", real_import)
    report = _fetch(pair, capsys)
    assert {str(entry["job_id"]) for entry in list(report["moved"])} == {
        pair.ids["succeeded"],
        pair.ids["failed"],
    }
    for name in ("succeeded", "failed"):
        assert pair.local.find_marker_by_id(pair.ids[name]) is not None
    assert not list((local.control / "transfers" / "incoming").iterdir())
    assert _live_bundles(pair.remote) == []


def test_fetch_selects_states_and_placements(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    report = _fetch(pair, capsys, "--state", "failed")
    fetched = list(report["moved"])
    assert len(fetched) == 1 and fetched[0]["job_id"] == pair.ids["failed"]
    # The succeeded job was never offered, so it is still schedulable remotely.
    assert pair.remote.find_marker_by_id(pair.ids["succeeded"]) is not None
    assert pair.local.find_marker_by_id(pair.ids["succeeded"]) is None

    assert _fetch(pair, capsys, "--placement", "project/later")["moved"] == []


# ---------------------------------------------------------------------------
# What the fetched jobs are worth locally
# ---------------------------------------------------------------------------


def test_a_fetched_job_harvests_locally_as_an_ordinary_result(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _fetch(pair, capsys)
    records = {record.job_id: record for record in harvest(pair.local, states=("succeeded", "failed"))}
    assert set(records) == {pair.ids["succeeded"], pair.ids["failed"]}

    succeeded = records[pair.ids["succeeded"]]
    assert succeeded.state == "succeeded" and succeeded.failure is None
    assert succeeded.workspace_id == pair.local.workspace_id
    assert succeeded.job["workflow"] == "tests.fetch"
    assert succeeded.workdir is not None
    assert (succeeded.workdir / "energy.txt").read_text(encoding="utf-8") == "-4.25"

    failed = records[pair.ids["failed"]]
    assert failed.failure is not None and failed.failure.code == "compute.diverged"
    assert failed.failure.details == {"cycles": 3}

    # The same thing through the command, which is the documented pipeline.
    assert command(["harvest", "home", "--state", "succeeded", "--state", "failed"], pair.context) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert {HarvestRecord.from_mapping(json.loads(line)).job_id for line in lines} == set(records)


def test_the_runner_the_remote_pinned_travels_with_the_fetched_jobs(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _fetch(pair, capsys)
    local = pair.local
    marker = local.find_marker_by_id(pair.ids["succeeded"])
    assert marker is not None
    job = local.load_job(marker)
    assert job.runner_source == "workspace" and job.runner_sha256 is not None
    stored = local.runner_store_path(job.runner_path)
    # The workspace runner was installed at the destination under the digest the
    # job pins, so the fetched job is runnable here rather than only readable.
    assert stored.is_file()
    assert local.find_marker_by_id(pair.ids["succeeded"]) is not None


# ---------------------------------------------------------------------------
# Retiring, standalone
# ---------------------------------------------------------------------------


def test_retire_is_idempotent_and_refuses_a_job_it_never_sealed(
    pair: Pair,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _offer(pair, capsys)
    local_id = pair.local.workspace_id
    argv = [
        "transfer",
        "retire",
        str(pair.remote_root),
        pair.ids["succeeded"],
        "--destination-workspace-id",
        local_id,
        "--json",
    ]
    assert command(argv, pair.context) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["format"] == "httk-workflow-transfer-retirement"
    retired = list(first["retired"])
    assert len(retired) == 1 and retired[0]["status"] == "retired"
    assert Path(str(retired[0]["retired_bundle"]), TRANSFER_DIRECTORY, "manifest.json").is_file()

    assert command(argv, pair.context) == 0
    assert json.loads(capsys.readouterr().out) == first

    # The pending job was never sealed, so retiring it is an error rather than a
    # silent success.
    assert command(["transfer", "retire", str(pair.remote_root), pair.ids["pending"]], pair.context) == 2
    assert "no detached transfer" in capsys.readouterr().err
