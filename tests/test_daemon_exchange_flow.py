"""Mock end-to-end job round trip through a daemon enrollment's exchange, without Bubblewrap or Slurm.

The enrollment is real (approved ``slurm`` launcher, layout, endpoint). The broker's movers run in
process, and the manager is the one the broker's submission would start: a confined exchange manager on
the real workspace path, whose attempt sandbox is replaced by a pass-through as in
``test_manager_confinement.py``.
"""

import base64
import json
import os
import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace, _confine, _daemon_setup, _exchange_staging
from httk.workflow._daemon_client import Endpoint, read_endpoint, read_passive_status
from httk.workflow._daemon_exchange import ExchangeMover
from httk.workflow._daemon_policy import Policy, load_policy
from httk.workflow._daemon_slurm import submission
from httk.workflow._exchange_staging import exchange_pass
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow.launchers import add_launcher
from httk.workflow.transfers import exchange_staging
from httk.workflow.workflow_cli import command
from test_eject_adopt import _payload, configure_identity

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")
HANDLE = "a" * 32


@dataclass
class _Site:
    root: Path
    server: Workspace
    policy: Policy
    endpoint: Endpoint

    @property
    def exchange(self) -> Path:
        return self.policy.exchange

    def mover(self) -> ExchangeMover:
        # The broker reaches the same parent through its sandbox alias; in process it is the real path.
        return ExchangeMover(self.root, self.exchange.name, self.server.root.name, self.policy.enrollment_id)


def _executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _enroll(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Site:
    tools = tmp_path / "tools"
    for name in ("bwrap", "sbatch", "squeue", "scancel"):
        _executable(tools / name)
    monkeypatch.setenv("PATH", f"{tools}:{os.environ['PATH']}")
    monkeypatch.delenv("SLURM_CONF", raising=False)
    add_launcher(
        "confined",
        template="slurm",
        global_=True,
        settings={"manager.confine": "bwrap", "confine.bwrap": "/usr/bin/bwrap", "manager.allocation": "none"},
    )
    root = tmp_path / "site"
    server = Workspace.initialize(root / "workspace")
    snapshot = _daemon_setup.initialize(
        server.root,
        exchange=root / "exchange",
        launchers=["confined"],
        authorized_keys=[AUTHORIZED_KEY],
        state=tmp_path / "state",
        broker=_daemon_setup.BrokerOptions(cluster="test-cluster", slurm_conf=None),
    )
    policy = load_policy(snapshot)
    public_key = str(read_endpoint(policy.exchange)["daemon_public_key"])
    return _Site(root, server, policy, Endpoint(policy.exchange, policy.workspace_id, policy.enrollment_id, public_key))


def _passive(endpoint: Endpoint) -> dict[str, Any]:
    return read_passive_status(endpoint)


def _submitted_manager(site: _Site) -> list[str]:
    """Return the manager command line of the batch script the broker would submit."""

    _argv, script = submission(site.policy, site.policy.launcher("confined"), HANDLE)
    words = shlex.split(script.decode("utf-8").splitlines()[-1])
    assert words[:5] == ["exec", str(site.policy.python), "-I", "-m", "httk.core.cli"]
    return words[5:]


def _pins(argv: list[str]) -> dict[str, str]:
    return dict(argv[index + 1].split("=", 1) for index, word in enumerate(argv) if word == "--setting")


@pytest.fixture
def sandboxes(monkeypatch: pytest.MonkeyPatch) -> list[Mapping[str, Any]]:
    """Replace the attempt sandbox with a pass-through and record every confined attempt."""

    prepared: list[Mapping[str, Any]] = []

    def prepare(settings: _confine.ConfineSettings, **arguments: Any) -> PreparedSandbox:
        prepared.append({"settings": settings, **arguments})
        return PreparedSandbox(["/usr/bin/env", "--"], ())

    monkeypatch.setattr(_confine, "probe_bwrap", lambda _settings: True)
    monkeypatch.setattr(_confine, "prepare_attempt_sandbox", prepare)
    return prepared


def test_a_job_round_trips_through_the_exchange(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sandboxes: list[Mapping[str, Any]]
) -> None:
    configure_identity()
    monkeypatch.setattr(_exchange_staging, "_EJECT_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(_exchange_staging, "_STEP_INTERVAL", 0.0)
    site = _enroll(tmp_path, monkeypatch)
    client = Workspace.initialize(tmp_path / "client" / "workspace")
    marker = client.submit(_payload(tmp_path / "payloads", "trip"), "jobs")

    # The client drops the sealed bundle into the mounted exchange.
    context = CLIContext("httk", client.root)
    assert command(["job", "eject", marker.job_id, str(site.exchange / "inbox")], context) == 0
    assert (site.exchange / "inbox" / marker.job_key).is_dir()
    mover = site.mover()
    mover.poll([])
    staging = exchange_staging(site.server)
    assert (staging / "inbox" / marker.job_key).is_dir()
    assert not (site.exchange / "inbox" / marker.job_key).exists()

    # The approved launcher's submission starts a confined exchange manager on the real workspace path.
    manager_argv = _submitted_manager(site)
    assert manager_argv[:8] == [
        "workflow",
        "manager",
        "run",
        "--by-path",
        "--workspace",
        str(site.server.root),
        "--exchange",
        "--idle",
    ]
    pins = _pins(manager_argv)
    assert pins["manager.confine"] == "bwrap"
    with TaskManager(site.server, heartbeat_interval=0.01, exchange=True, setting_overrides=pins) as manager:
        manager.run_until_idle(timeout=120.0)
        manager.tick()  # the final pass ejects the finished job
    assert [call["settings"].mode for call in sandboxes] == ["bwrap"]
    assert sandboxes[0]["workspace_root"] == site.server.root
    assert (staging / "outbox" / marker.job_key).is_dir()
    assert site.server.find_marker_by_id(marker.job_id) is None

    manager_row: dict[str, str | None] = {
        "handle": HANDLE,
        "profile": "confined",
        "request_id": "r" * 32,
        "state": "submitted",
        "job_id": "42",
        "scheduler_state": None,
        "exit_code": None,
        "started_at": None,
        "ended_at": None,
        "log": None,
    }
    mover.poll([manager_row])
    assert (site.exchange / "outbox" / marker.job_key).is_dir()
    assert not (staging / "outbox" / marker.job_key).exists()
    passive = _passive(site.endpoint)
    status, managers = passive["status"], passive["managers"]
    assert status["workspace_id"] == site.server.workspace_id and status["rejected"] == []
    # The status step runs after the eject step of the same pass, so it already shows the job gone.
    assert status["jobs"] == [] and status["eject_errors"] == []
    assert managers["managers"] == [manager_row] and managers["enrollment_id"] == site.policy.enrollment_id

    assert command(["job", "adopt", str(site.exchange / "outbox" / marker.job_key)], context) == 0
    current = client.find_marker_by_id(marker.job_id)
    assert current is not None and current.kind == "succeeded"
    assert not (site.exchange / "outbox" / marker.job_key).exists()


def test_a_corrupt_bundle_is_rejected_back_to_the_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    configure_identity()
    site = _enroll(tmp_path, monkeypatch)
    (site.exchange / "inbox" / "broken").mkdir()
    (site.exchange / "inbox" / "broken" / "junk").write_text("not a job", encoding="utf-8")
    mover = site.mover()
    mover.poll([])
    exchange_pass(site.server, now=1000.0)
    mover.poll([])
    assert (site.exchange / "outbox" / "rejected" / "broken" / "junk").is_file()
    assert not os.listdir(site.exchange / "inbox") and not os.listdir(exchange_staging(site.server) / "inbox")
    (entry,) = _passive(site.endpoint)["status"]["rejected"]
    assert entry["name"] == "broken" and entry["reason"]


def test_a_waiting_job_is_withdrawn_and_adopted_back_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    configure_identity()
    site = _enroll(tmp_path, monkeypatch)
    client = Workspace.initialize(tmp_path / "client" / "workspace")
    marker = client.submit(_payload(tmp_path / "payloads", "waiting"), "jobs")
    client.eject(marker.job_id, site.exchange / "inbox")
    mover = site.mover()
    mover.poll([])
    staged = exchange_staging(site.server) / "inbox" / marker.job_key
    assert staged.is_dir()
    assert json.loads((site.exchange / "outbox" / "managers.json").read_bytes())["staged"] == [marker.job_key]

    # No manager ever runs, so the client takes the bundle back.
    assert mover.withdraw(None) == [marker.job_key]
    assert not staged.exists() and site.server.find_marker_by_id(marker.job_id) is None
    mover.poll([])
    assert json.loads((site.exchange / "outbox" / "managers.json").read_bytes())["staged"] == []
    adopted = client.adopt(site.exchange / "outbox" / "withdrawn" / marker.job_key)
    assert adopted.job_id == marker.job_id and adopted.kind == marker.kind == "submitted"
    assert not (site.exchange / "outbox" / "withdrawn" / marker.job_key).exists()
