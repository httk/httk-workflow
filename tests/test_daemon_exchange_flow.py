"""Mock end-to-end job round trip through the confined daemon exchange, without bwrap or Slurm."""

import json
import os
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace, _exchange_staging
from httk.workflow._daemon_client import Endpoint, read_passive_status
from httk.workflow._daemon_exchange import ExchangeMover
from httk.workflow._exchange_staging import exchange_pass
from httk.workflow.transfers import exchange_staging
from test_eject_adopt import _payload, configure_identity

WORKSPACE_ID = "12345678-1234-4234-8234-123456789abc"
ENROLLMENT_ID = "e" * 32
PUBLIC_KEY = "ed25519:" + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
MANAGER = {"handle": "h" * 32, "profile": "cpu", "request_id": "r" * 32, "state": "RUNNING"}


def _site(tmp_path: Path) -> tuple[Path, Workspace, Endpoint]:
    site = tmp_path / "site"
    server = Workspace.initialize(site / "workspace")
    exchange = site / "exchange"
    for name in ("requests", "responses", "inbox", "outbox/rejected"):
        (exchange / name).mkdir(parents=True)
    staging = exchange_staging(server)
    for name in ("inbox", "outbox/rejected", "records"):
        (staging / name).mkdir(parents=True)
    (exchange / "endpoint.json").write_text(
        json.dumps(
            {
                "format": "httk-workspace-daemon-endpoint",
                "format_version": 2,
                "workspace_id": server.workspace_id,
                "enrollment_id": ENROLLMENT_ID,
                "daemon_public_key": PUBLIC_KEY,
                "configurations": {},
                "request_max_age": 3600,
            }
        ),
        encoding="utf-8",
    )
    return site, server, Endpoint(exchange, server.workspace_id, ENROLLMENT_ID, PUBLIC_KEY)


def test_a_job_round_trips_through_the_exchange(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    configure_identity()
    monkeypatch.setattr(_exchange_staging, "_EJECT_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(_exchange_staging, "_STEP_INTERVAL", 0.0)
    site, server, endpoint = _site(tmp_path)
    exchange = site / "exchange"
    client = Workspace.initialize(tmp_path / "client" / "workspace")
    marker = client.submit(_payload(tmp_path / "payloads", "trip"), "jobs")

    client.eject(marker.job_id, exchange / "inbox")
    assert (exchange / "inbox" / marker.job_key).is_dir()
    mover = ExchangeMover(site, "exchange", "workspace", ENROLLMENT_ID)
    mover.poll([])
    staging = exchange_staging(server)
    assert (staging / "inbox" / marker.job_key).is_dir() and not (exchange / "inbox" / marker.job_key).exists()

    with TaskManager(server, heartbeat_interval=0.01, exchange=True) as manager:
        manager.run_until_idle(timeout=120.0)
        manager.tick()  # the final pass ejects the finished job
    assert (staging / "outbox" / marker.job_key).is_dir()
    assert server.find_marker_by_id(marker.job_id) is None

    mover.poll([MANAGER])
    assert (exchange / "outbox" / marker.job_key).is_dir() and not (staging / "outbox" / marker.job_key).exists()
    passive = read_passive_status(endpoint)
    status, managers = passive["status"], passive["managers"]
    assert isinstance(status, dict) and isinstance(managers, dict)
    assert status["workspace_id"] == server.workspace_id and status["rejected"] == []
    # The status step runs after the eject step of the same pass, so it already shows the job gone.
    assert status["jobs"] == [] and status["eject_errors"] == []
    assert managers["managers"] == [MANAGER] and managers["enrollment_id"] == ENROLLMENT_ID

    adopted = client.adopt(exchange / "outbox" / marker.job_key)
    assert adopted.job_id == marker.job_id and adopted.kind == "succeeded"
    current = client.find_marker_by_id(marker.job_id)
    assert current is not None and current.kind == "succeeded"
    assert not (exchange / "outbox" / marker.job_key).exists()


def test_a_corrupt_bundle_is_rejected_back_to_the_client(tmp_path: Path) -> None:
    configure_identity()
    site, server, endpoint = _site(tmp_path)
    exchange = site / "exchange"
    (exchange / "inbox" / "broken").mkdir()
    (exchange / "inbox" / "broken" / "junk").write_text("not a job", encoding="utf-8")
    mover = ExchangeMover(site, "exchange", "workspace", ENROLLMENT_ID)
    mover.poll([])
    exchange_pass(server, now=1000.0)
    mover.poll([])
    assert (exchange / "outbox" / "rejected" / "broken" / "junk").is_file()
    assert not os.listdir(exchange / "inbox") and not os.listdir(exchange_staging(server) / "inbox")
    status = read_passive_status(endpoint)["status"]
    assert isinstance(status, dict)
    (entry,) = status["rejected"]
    assert entry["name"] == "broken" and entry["reason"]
