"""Mock end-to-end job round trip through an enrolled workspace's exchange extension, without Bubblewrap or Slurm.

The enrollment is real (approved ``slurm`` launcher, ``daemon.json``), and enrolling enables the workspace's
exchange extension. The client writes a fresh bundle into ``WORKSPACE/exchange/inbox`` and fetches the returned
tree from ``WORKSPACE/exchange/outbox/<its job UUID>/``; the broker never moves bundles. The manager is the one the
broker's submission would start: a confined manager on the real workspace path, whose attempt sandbox is replaced
by a pass-through.
"""

import base64
import json
import os
import shlex
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace, _confine, _daemon_setup
from httk.workflow._bundles import BundleManifest
from httk.workflow._daemon_policy import Policy, load_policy
from httk.workflow._daemon_slurm import submission
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow.launchers import add_launcher
from test_bundles import write_bundle
from v3_helpers import install, job_mapping

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")
HANDLE = "a" * 32


def _executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _enroll(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Workspace, Policy]:
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
    server = Workspace.initialize(tmp_path / "site" / "workspace", durable=False)
    server.set_policy({"visibility_deadline_seconds": 0.05})
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
    server = Workspace(server.root, durable=False)
    assert "exchange" in server.extensions and (server.root / "exchange" / "inbox").is_dir()
    return server, load_policy(snapshot)


def _submitted_manager(policy: Policy) -> list[str]:
    """Return the manager command line of the batch script the broker would submit."""

    _argv, script = submission(policy, policy.launcher("confined"), HANDLE)
    words = shlex.split(script.decode("utf-8").splitlines()[-1])
    assert words[:5] == ["exec", str(policy.python), "-I", "-m", "httk.core.cli"]
    return words[5:]


@pytest.fixture
def sandboxes(monkeypatch: pytest.MonkeyPatch) -> list[Mapping[str, Any]]:
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
    server, policy = _enroll(tmp_path, monkeypatch)
    exchange = server.root / "exchange"
    installed = install(server, tmp_path / "package")
    client_job = job_mapping(installed, {"start": "succeed"}, tag="trip", placement="client/trip")
    write_bundle(exchange / "inbox" / "trip", [client_job])

    # The approved launcher's submission starts a confined manager on the real workspace path; it serves the
    # exchange because the workspace has the extension.
    argv = _submitted_manager(policy)
    assert argv[:7] == ["workflow", "manager", "run", "--by-path", "--workspace", str(server.root), "--idle"]
    pins = dict(argv[index + 1].split("=", 1) for index, word in enumerate(argv) if word == "--setting")
    assert pins["manager.confine"] == "bwrap"
    with TaskManager(server, heartbeat_interval=0.01, setting_overrides=pins) as manager:
        # Until-idle here, so the test ends; the daemon's managers keep serving with --idle.
        manager.run_until_idle(timeout=120.0, poll_interval=0.05)
    assert [call["settings"].mode for call in sandboxes] == ["bwrap"]
    assert sandboxes[0]["workspace_root"] == server.root
    [bundle] = (exchange / "outbox" / str(client_job["id"])).iterdir()
    manifest = BundleManifest.from_json((bundle / "bundle.json").read_bytes())
    assert [member.state for member in manifest.members] == ["succeeded"]
    assert os.listdir(exchange / "inbox") == [] and os.listdir(exchange / "outbox" / "rejected") == []
    status = json.loads((exchange / "status.json").read_bytes())
    assert status["format"] == "httk-workspace-exchange-status" and status["workspace_id"] == server.workspace_id


def test_a_corrupt_bundle_is_rejected_back_to_the_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server, _policy = _enroll(tmp_path, monkeypatch)
    exchange = server.root / "exchange"
    (exchange / "inbox" / "broken").mkdir()
    (exchange / "inbox" / "broken" / "junk").write_text("not a job", encoding="utf-8")
    from httk.workflow._exchange import ExchangeService
    from v3_helpers import cli_owner

    with cli_owner(server) as owner:
        assert ExchangeService(server, owner).run()
    assert not os.listdir(exchange / "inbox")
    [unique] = list((exchange / "outbox" / "rejected").iterdir())
    assert (unique / "broken" / "junk").is_file()
    reason = json.loads((unique / "reason.json").read_bytes())
    assert reason["format"] == "httk-workspace-exchange-rejection" and reason["name"] == "broken" and reason["reason"]
