"""Mock end-to-end job round trip through an enrolled workspace's exchange extension, without Bubblewrap or Slurm.

The enrollment is real (approved ``slurm`` launcher, ``daemon.json``), and enrolling enables the
workspace's exchange extension. The client writes into ``WORKSPACE/exchange/inbox`` and fetches from
``WORKSPACE/exchange/outbox``; the broker never moves bundles. The manager is the one the broker's
submission would start: a confined manager on the real workspace path (every unrestricted manager serves
the exchange), whose attempt sandbox is replaced by a pass-through as in ``test_manager_confinement.py``.
"""

import base64
import json
import os
import shlex
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace, _confine, _daemon_setup, _exchange
from httk.workflow._daemon_client import Endpoint, read_daemon
from httk.workflow._daemon_policy import Policy, load_policy
from httk.workflow._daemon_slurm import submission
from httk.workflow._exchange import ExchangeService
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow._txn import owner_token
from httk.workflow.launchers import add_launcher
from httk.workflow.workflow_cli import command
from test_eject_adopt import _payload, configure_identity

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")
HANDLE = "a" * 32


@dataclass
class _Site:
    server: Workspace
    policy: Policy
    endpoint: Endpoint

    @property
    def exchange(self) -> Path:
        return self.policy.exchange


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
    server = Workspace.initialize(tmp_path / "site" / "workspace")
    reports: list[str] = []
    snapshot = _daemon_setup.initialize(
        server.root,
        changes=[
            ("add", "launchers=confined"),
            ("add", f"authorized_keys={AUTHORIZED_KEY}"),
            ("set", "cluster=test-cluster"),
            ("set", "slurm_conf="),
        ],
        state=tmp_path / "state",
        report=reports.append,
    )
    # Enrolling enables the exchange extension, and says so.
    assert reports == [f"enabled the exchange extension of {server.root}: {server.root / 'exchange'}"]
    server = Workspace(server.root)
    assert "exchange" in server.extensions and (server.root / "exchange" / "inbox").is_dir()
    policy = load_policy(snapshot)
    public_key = str(read_daemon(policy.exchange)["daemon_public_key"])
    return _Site(server, policy, Endpoint(policy.exchange, policy.workspace_id, policy.enrollment_id, public_key))


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
    monkeypatch.setattr(_exchange, "RETURN_GRACE_SECONDS", 0.5)
    monkeypatch.setattr(_exchange, "STEP_INTERVAL", 0.0)
    site = _enroll(tmp_path, monkeypatch)
    exchange = site.server.root / "exchange"
    client = Workspace.initialize(tmp_path / "client" / "workspace")
    marker = client.submit(_payload(tmp_path / "payloads", "trip"), "jobs")

    # The client drops the sealed bundle into the workspace's exchange inbox.
    context = CLIContext("httk", client.root)
    assert command(["job", "eject", marker.job_id, str(exchange / "inbox")], context) == 0
    assert (exchange / "inbox" / marker.job_key).is_dir()
    # The broker has no part in moving it: the bundle waits for a manager.
    assert (exchange / "inbox" / marker.job_key).is_dir()

    # The approved launcher's submission starts a confined manager on the real workspace path; it
    # serves the exchange because the workspace has the extension.
    manager_argv = _submitted_manager(site)
    assert manager_argv[:7] == [
        "workflow",
        "manager",
        "run",
        "--by-path",
        "--workspace",
        str(site.server.root),
        "--idle",
    ]
    assert "--exchange" not in manager_argv
    pins = _pins(manager_argv)
    assert pins["manager.confine"] == "bwrap"
    with TaskManager(site.server, heartbeat_interval=0.01, setting_overrides=pins) as manager:
        # Until-idle here, so the test ends; the daemon's managers keep serving with --idle.
        manager.run_until_idle(timeout=120.0, poll_interval=0.05)
        assert manager._exchange is not None
    assert [call["settings"].mode for call in sandboxes] == ["bwrap"]
    assert sandboxes[0]["workspace_root"] == site.server.root
    assert (exchange / "outbox" / marker.job_key).is_dir()
    assert site.server.find_marker_by_id(marker.job_id) is None
    assert os.listdir(exchange / "inbox") == [] and os.listdir(exchange / "outbox" / "rejected") == []
    status = json.loads((exchange / "status.json").read_bytes())
    assert status["format"] == "httk-workspace-exchange-status" and status["workspace_id"] == site.server.workspace_id

    assert command(["job", "adopt", str(exchange / "outbox" / marker.job_key)], context) == 0
    current = client.find_marker_by_id(marker.job_id)
    assert current is not None and current.kind == "succeeded"
    assert "origin" not in client.read_state(current)
    assert not (exchange / "outbox" / marker.job_key).exists()


def test_a_corrupt_bundle_is_rejected_back_to_the_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    configure_identity()
    site = _enroll(tmp_path, monkeypatch)
    exchange = site.server.root / "exchange"
    (exchange / "inbox" / "broken").mkdir()
    (exchange / "inbox" / "broken" / "junk").write_text("not a job", encoding="utf-8")
    ExchangeService(site.server, owner=owner_token(str(uuid.uuid4()))).run(now=1000.0)
    assert not os.listdir(exchange / "inbox")
    [unique] = list((exchange / "outbox" / "rejected").iterdir())
    assert (unique / "broken" / "junk").is_file()
    reason = json.loads((unique / "reason.json").read_bytes())
    assert reason["format"] == "httk-workspace-exchange-rejection" and reason["name"] == "broken" and reason["reason"]
