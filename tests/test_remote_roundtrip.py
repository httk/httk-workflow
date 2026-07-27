"""The whole remote round trip, driven through the ``ssh-slurm`` adapter.

One job is created in a local workspace, sent to a *remote* workspace that lives
in the stand-in cluster's filesystem root, run there by a real
:class:`httk.workflow.TaskManager`, fetched back over the same adapter, and
finally harvested at home. Only ``ssh`` and ``sbatch`` are stand-ins: the
transfers are real ``rsync`` runs, the offer, pull, import and retire steps are
the shipped ones, and every command really crosses the transport.
"""

import json
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest  # pyright: ignore[reportMissingImports]
from conftest import Remote, fake_remote, register_ws
from httk.core import CLIContext

from httk.workflow import (
    HarvestRecord,
    TaskManager,
    Workspace,
)
from httk.workflow.projects import initialize_project
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.transfers import TRANSFER_DIRECTORY
from httk.workflow.workflow_cli import command

_SRC = str(Path(__file__).parents[1] / "src")
_PLACEMENT = "project/screening"

_RUNNER = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.roundtrip")


@run.step
def compute(a):
    (a.workdir / "energy.txt").write_text("-7.5", encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
'''


@dataclass(frozen=True)
class Campaign:
    """One local workspace, one workspace on the stand-in cluster, one job."""

    cluster: Remote
    local: Workspace
    station: Workspace
    context: CLIContext
    job_id: str


def _campaign(tmp_path: Path, remote: Remote) -> Campaign:
    """Create both workspaces, the remote between them, and one runnable job."""

    local_root = tmp_path / "local"
    initialize_project(local_root, name="roundtrip")
    local = Workspace(local_root)
    station = Workspace.initialize(
        remote.root / "runs" / "workspace",
        extensions=["detached-transfer-v1"],
    )
    fake_remote(local_root, workspace=str(station.root), workers="2")

    source = tmp_path / "runners" / "roundtrip.py"
    source.parent.mkdir(parents=True)
    source.write_text(_RUNNER, encoding="utf-8")
    runner = local.publish_runner(source, name="roundtrip/run.py")

    payload = tmp_path / "payload"
    (payload / "files").mkdir(parents=True)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Round trip",
            workflow="tests.roundtrip",
            runner_path=str(runner["path"]),
            runner_source="workspace",
            runner_sha256=str(runner["sha256"]),
            tag="roundtrip",
            initial_step="compute",
            maximum_attempts_per_activation=1,
        ),
    )
    # A helper the job carries rather than references. It is load bearing only
    # while it stays executable, which is what the payload digest pins, so it
    # has to survive both legs of the trip with its mode intact.
    helper = payload / "files" / "helper.sh"
    helper.write_text("#!/bin/sh\necho helper\n", encoding="utf-8")
    helper.chmod(0o755)
    local.submit(payload, _PLACEMENT)
    context = CLIContext("httk", local_root)
    # Every command names a registered workspace: "home" is local, "station" is
    # the workspace on the "cluster" remote the transfers cross to.
    register_ws(context, local_root, "home")
    register_ws(context, station.root, "station", remote="cluster")
    return Campaign(cluster=remote, local=local, station=station, context=context, job_id=job.id)


def _payload_of(workspace: Workspace, job_id: str) -> Path:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    return workspace.payload_path(marker.placement, marker.job_key)


def _send(campaign: Campaign, capsys: pytest.CaptureFixture[str]) -> None:
    argv = ["transfer", "home", "station", "--job", campaign.job_id, "--json"]
    assert command(argv, campaign.context) == 0
    report = json.loads(capsys.readouterr().out)
    assert [str(entry["job_id"]) for entry in report["moved"]] == [campaign.job_id]


def _fetch(campaign: Campaign, capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    argv = ["transfer", "station", "home", "--json"]
    assert command(argv, campaign.context) == 0
    return json.loads(capsys.readouterr().out)


def _run_there(campaign: Campaign) -> None:
    with TaskManager(campaign.station, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=180.0)


def _retired(workspace: Workspace) -> list[Path]:
    retired = workspace.control / "transfers" / "retired"
    return sorted(retired.iterdir()) if retired.is_dir() else []


def _staged(workspace: Workspace) -> list[Path]:
    incoming = workspace.control / "transfers" / "incoming"
    return sorted(incoming.iterdir()) if incoming.is_dir() else []


def test_a_job_goes_out_over_ssh_runs_there_and_is_fetched_home(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
) -> None:
    campaign = _campaign(tmp_path, remote)

    # (b) Out over the transport.
    _send(campaign, capsys)
    assert campaign.local.find_marker_by_id(campaign.job_id) is None
    arrived = campaign.station.find_marker_by_id(campaign.job_id)
    assert arrived is not None and arrived.kind == "submitted"
    assert arrived.placement.as_posix() == _PLACEMENT
    there = _payload_of(campaign.station, campaign.job_id)
    assert stat.S_IMODE((there / "files" / "helper.sh").stat().st_mode) & 0o111

    # (c) The managers the operator would start really are submitted, with a
    # script that runs this workspace's manager, and the work itself is then
    # done by the manager that batch script would have exec'd.
    assert command(["manager", "run", "station", "--count", "2"], campaign.context) == 0
    submitted = json.loads(capsys.readouterr().out)
    assert submitted["count"] == 2 and len(submitted["job_ids"]) == 2
    spooled = sorted(remote.spool.glob("*.sbatch"))
    assert len(spooled) == 2
    script = spooled[0].read_text(encoding="utf-8")
    assert script.startswith("#!/bin/bash\n")
    assert f"#SBATCH --chdir={campaign.station.root}" in script
    assert f"exec httk workflow manager run {campaign.station.root} --by-path --workers 2" in script
    _run_there(campaign)
    finished = campaign.station.find_marker_by_id(campaign.job_id)
    assert finished is not None and finished.kind == "succeeded"

    # (d) Home again over the same adapter.
    report = _fetch(campaign, capsys)
    fetched = list(report["moved"])
    assert len(fetched) == 1 and fetched[0]["job_id"] == campaign.job_id
    assert [str(entry["status"]) for entry in list(report["retired"])] == ["retired"]

    marker = campaign.local.find_marker_by_id(campaign.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert marker.placement.as_posix() == _PLACEMENT
    home = _payload_of(campaign.local, campaign.job_id)
    assert (home / "run" / "energy.txt").read_text(encoding="utf-8") == "-7.5"
    # The executable bit survived both rsync legs and both digest checks.
    assert stat.S_IMODE((home / "files" / "helper.sh").stat().st_mode) & 0o111
    # The runner the job pins came back with it, so it is runnable here.
    job = campaign.local.load_job(marker)
    assert campaign.local.runner_store_path(job.runner_path).is_file()

    # (e) An ordinary harvest of the local workspace reports it.
    assert command(["harvest", "home", "--state", "succeeded"], campaign.context) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    record = HarvestRecord.from_mapping(json.loads(lines[0]))
    assert record.job_id == campaign.job_id
    assert record.workspace_id == campaign.local.workspace_id
    assert record.workdir is not None
    assert (record.workdir / "energy.txt").read_text(encoding="utf-8") == "-7.5"

    # The remote retired its source whole, keeps no live bundle, and a second
    # fetch therefore has nothing to do.
    retired = _retired(campaign.station)
    assert len(retired) == 1
    assert (retired[0] / "bundle" / TRANSFER_DIRECTORY / "manifest.json").is_file()
    assert campaign.station.find_marker_by_id(campaign.job_id) is None
    assert _staged(campaign.local) == []
    assert _fetch(campaign, capsys) == {"moved": [], "retired": []}
    assert campaign.local.find_marker_by_id(campaign.job_id) is not None

    # Every leg really used the stand-in transport rather than this filesystem.
    commands = remote.commands()
    assert any("workspace status" in item for item in commands)
    assert any("transfer receive" in item for item in commands)
    assert any("transfer offer" in item for item in commands)
    assert any("transfer retire" in item for item in commands)
    assert any("sbatch" in item for item in commands)
    assert any(item.startswith("rsync ") or " rsync " in item for item in commands)


def test_a_banner_on_the_remote_stdout_stops_the_fetch_before_anything_is_imported(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign = _campaign(tmp_path, remote)
    _send(campaign, capsys)
    _run_there(campaign)

    # A talkative login shell greets exactly the connection that runs the
    # remote offer, so its answer is no longer the pure JSON this protocol
    # requires. The workspace probe before it is untouched and still passes.
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER", "*** Welcome to the fake cluster ***")
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER_WHEN", "transfer offer")

    argv = ["transfer", "station", "home", "--json"]
    assert command(argv, campaign.context) == 2
    captured = capsys.readouterr()
    assert "remote offer did not return a transfer offer document" in captured.err
    assert captured.out == ""

    # Nothing was pulled, nothing was imported, and nothing was retired: the
    # remote still holds the bundle its offer sealed before it spoke.
    assert campaign.local.find_marker_by_id(campaign.job_id) is None
    assert _staged(campaign.local) == []
    assert _retired(campaign.station) == []

    # Silencing the far side is all the recovery there is; the sealed bundle is
    # offered again rather than sealed twice, and the job comes home.
    monkeypatch.delenv("HTTK_FAKE_SSH_BANNER")
    report = _fetch(campaign, capsys)
    assert [str(entry["job_id"]) for entry in list(report["moved"])] == [campaign.job_id]
    marker = campaign.local.find_marker_by_id(campaign.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert len(_retired(campaign.station)) == 1
