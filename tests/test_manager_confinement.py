"""The manager runs attempts inside the attempt sandbox when ``manager.confine=bwrap``.

Real Bubblewrap cannot mount ``/proc`` on every host, so most tests replace
:func:`~httk.workflow._confine.prepare_attempt_sandbox` (and the probe) with a
pass-through: the "sandbox" is a small wrapper that records what it was started
with and execs the rest of the command. That proves the manager's plumbing —
settings precedence, probing, the confinement-start checks, the environment,
stdin, descriptors and process-group supervision — while the sandbox itself is
covered by ``test_confine_sandbox.py``. One end-to-end test uses real
Bubblewrap and is required in the sandbox CI job.
"""

import json
import logging
import os
import shutil
import signal
import socket
import stat
import sys
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, _confine, _manager_launches
from httk.workflow import manager as manager_module
from httk.workflow._allocation import Allocation, Node
from httk.workflow._exchange import enable_exchange
from httk.workflow._logging import reset_logging
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow.errors import ConfinementUnavailableError, FormatError
from httk.workflow.models import Marker
from httk.workflow.workflow_cli import command

_BWRAP = "/usr/bin/bwrap"
_PINNED = {"manager.confine": "bwrap", "confine.bwrap": _BWRAP}

_RUNNER = """#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
workdir = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
(workdir / "seen.json").write_text(json.dumps({{"env": dict(os.environ), "settings": context["settings"]}}))
{body}
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({{
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}}))
os.rename(temporary, control / "outcome.ready")
"""

#: The stand-in sandbox: it records its argv, environment, stdin and open
#: descriptors, then execs the attempt command it was given.
_WRAPPER = """#!{python}
import json
import os
import stat
import sys

stdin = os.fstat(0)
devnull = os.stat("/dev/null")
entry = {{
    "argv": sys.argv[1:],
    "env": dict(os.environ),
    "stdin_devnull": stat.S_ISCHR(stdin.st_mode) and stdin.st_rdev == devnull.st_rdev,
    "fds": sorted(int(name) for name in os.listdir("/proc/self/fd")),
    "pid": os.getpid(),
    "pgid": os.getpgid(0),
}}
with open({record!r}, "a") as handle:
    handle.write(json.dumps(entry) + "\\n")
os.execvp(sys.argv[1], sys.argv[1:])
"""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    reset_logging()
    yield
    reset_logging()


@dataclass
class _Sandbox:
    """The pass-through sandbox and what the manager did with it."""

    directory: Path
    probe_result: bool | Exception = True
    probes: list[_confine.ConfineSettings] = field(default_factory=list)
    prepares: list[dict[str, Any]] = field(default_factory=list)
    closed: list[tuple[int, ...]] = field(default_factory=list)

    @property
    def wrapper(self) -> Path:
        return self.directory / "sandbox"

    @property
    def record(self) -> Path:
        return self.directory / "sandbox.jsonl"

    def records(self) -> list[dict[str, Any]]:
        if not self.record.exists():
            return []
        return [json.loads(line) for line in self.record.read_text(encoding="utf-8").splitlines() if line]


class _Recorded(PreparedSandbox):
    sandbox: _Sandbox

    def close(self) -> None:
        self.sandbox.closed.append(self.descriptors)
        super().close()


@pytest.fixture
def sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Sandbox:
    state = _Sandbox(tmp_path / "sandbox-bin")
    state.directory.mkdir()
    state.wrapper.write_text(_WRAPPER.format(python=sys.executable, record=str(state.record)), encoding="utf-8")
    state.wrapper.chmod(0o755)
    marker_file = state.directory / "descriptor"
    marker_file.write_text("sandbox descriptor\n", encoding="utf-8")

    def probe(settings: _confine.ConfineSettings) -> bool:
        state.probes.append(settings)
        if isinstance(state.probe_result, Exception):
            raise state.probe_result
        return state.probe_result

    def prepare(settings: _confine.ConfineSettings, **arguments: Any) -> PreparedSandbox:
        state.prepares.append({"settings": settings, **arguments, "environment": dict(arguments["environment"])})
        assert os.path.samestat(os.fstat(arguments["workspace_fd"]), os.stat(arguments["workspace_root"]))
        assert os.path.samestat(os.fstat(arguments["job_fd"]), os.stat(arguments["job_path"]))
        if arguments.get("launch_locks") is not None:
            locks_fd, locks_path = arguments["launch_locks"]
            assert os.path.samestat(os.fstat(locks_fd), os.stat(locks_path))
        descriptor = os.open(marker_file, os.O_RDONLY)
        os.set_inheritable(descriptor, True)
        prepared = _Recorded([str(state.wrapper)], (descriptor,))
        prepared.sandbox = state
        return prepared

    monkeypatch.setattr(_confine, "probe_bwrap", probe)
    monkeypatch.setattr(_confine, "prepare_attempt_sandbox", prepare)
    return state


def _payload(
    root: Path,
    tag: str,
    *,
    body: str = "",
    runner: Mapping[str, object] | None = None,
    resources: Mapping[str, int] | None = None,
    extra: Mapping[str, object] | None = None,
) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text(_RUNNER.format(body=body), encoding="utf-8")
    (files / "runner").chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": tag,
        "name": f"Confinement {tag}",
        "workflow": "tests.confinement",
        "runner": dict(runner) if runner is not None else {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "only",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"maximum_attempts_per_activation": 1, "maximum_total_attempts": 1, "retry_on": []},
        "resources": dict(resources or {}),
        "parent": None,
        **dict(extra or {}),
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _submit(workspace: Workspace, root: Path, tag: str, **options: Any) -> tuple[Marker, str]:
    payload, job_id = _payload(root, tag, **options)
    return workspace.submit(payload, f"project/{tag}"), job_id


def _outcome(workspace: Workspace, job_id: str) -> tuple[str, str | None, str]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    if marker.kind != "failed":
        return marker.kind, None, ""
    failure = workspace.read_state(marker)["failure"]
    return marker.kind, failure["code"], failure["message"]


def _seen(workspace: Workspace, marker: Marker) -> dict[str, Any]:
    path = workspace.payload_path(marker.placement, marker.job_key) / "run" / "seen.json"
    return json.loads(path.read_text(encoding="utf-8"))


# -- settings precedence and probing ---------------------------------------------


def test_pinned_settings_win_and_unpinned_workspace_settings_stay_live(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.confine", "none")
    workspace.set_setting("confine.isolate_network", "true")
    first, first_id = _submit(workspace, tmp_path / "source", "first")
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        assert len(sandbox.probes) == 1 and sandbox.probes[0].bwrap == Path(_BWRAP)
        manager.run_until_idle(timeout=60.0)
        # Workspace values of unpinned keys are read at every claim; a pinned
        # key keeps the manager's value whatever the workspace says now.
        workspace.set_setting("confine.isolate_network", "false")
        workspace.set_setting("manager.confine", "none")
        second, second_id = _submit(workspace, tmp_path / "source", "second")
        manager.run_until_idle(timeout=60.0)

    assert _outcome(workspace, first_id)[:2] == ("succeeded", None)
    assert _outcome(workspace, second_id)[:2] == ("succeeded", None)
    assert [call["settings"].isolate_network for call in sandbox.prepares] == [True, False]
    assert all(call["settings"].mode == "bwrap" for call in sandbox.prepares)
    assert len(sandbox.records()) == 2
    # The probe ran at start for the pinned Bubblewrap, and once more for the
    # sandbox shape the changed network isolation gives.
    assert [probe.isolate_network for probe in sandbox.probes] == [True, False]
    # The runner sees the effective settings the manager used.
    seen = _seen(workspace, second)
    assert seen["settings"]["manager.confine"] == "bwrap"
    assert seen["settings"]["confine.isolate_network"] == "false"
    assert seen["env"]["HTTK_MANAGER_CONFINE"] == "bwrap"
    assert _seen(workspace, first)["env"]["HTTK_CONFINE_ISOLATE_NETWORK"] == "true"


def test_a_pinned_launch_template_wins_over_the_workspace_one(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.launch_template", "workspace-launcher -n {procs}")
    marker, job_id = _submit(workspace, tmp_path / "source", "launch", resources={"procs": 1})
    allocation = Allocation("host", None, (Node(socket.gethostname(), 2, 1000),), {})
    with TaskManager(
        workspace,
        heartbeat_interval=0.01,
        resources=allocation.capacity(),
        allocation=allocation,
        setting_overrides={"manager.launch_template": "pinned-launcher -n {procs}"},
    ) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    assert _seen(workspace, marker)["env"]["HTTK_WORKFLOW_LAUNCH"] == "pinned-launcher -n 1"
    assert not sandbox.prepares


def test_a_live_block_mpi_spawn_change_reaches_the_next_trusted_launch(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    first_id = _submit(workspace, tmp_path / "source", "first")[1]
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager.run_until_idle(timeout=60.0)
        launch = manager._confinement(manager._effective_settings()).launch
        assert launch is not None and launch.block_mpi_spawn == "on"
        workspace.set_setting("manager.confine.block_mpi_spawn", "off")
        second_id = _submit(workspace, tmp_path / "source", "second")[1]
        manager.run_until_idle(timeout=60.0)
        # The trusted launch of a confined attempt carries this launch confinement.
        launch = manager._confinement(manager._effective_settings()).launch
        assert launch is not None and launch.block_mpi_spawn == "off"
        # An invalid live value holds back claims instead of reusing the last valid one.
        workspace.set_setting("manager.confine.block_mpi_spawn", "maybe")
        third_id = _submit(workspace, tmp_path / "source", "third")[1]
        manager.run_until_idle(timeout=60.0)
        assert _outcome(workspace, third_id)[0] == "ready"
    assert [_outcome(workspace, job_id)[:2] for job_id in (first_id, second_id)] == [("succeeded", None)] * 2
    assert [call["settings"].block_mpi_spawn for call in sandbox.prepares] == ["on", "off"]


def test_pinned_overrides_are_limited_to_confinement_keys(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match="pinned setting"):
        TaskManager(workspace, setting_overrides={"manager.workers": "2"})
    with pytest.raises(ValueError, match="manager.confine must be none or bwrap"):
        TaskManager(workspace, setting_overrides={"manager.confine": "chroot"})
    assert not (workspace.control / "managers").exists() or not list((workspace.control / "managers").iterdir())


def test_an_unusable_bubblewrap_refuses_the_manager_start_through_the_cli(
    tmp_path: Path, sandbox: _Sandbox, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    ws = register_ws(None, workspace.root)
    sandbox.probe_result = _confine.ConfinementUnavailableError("bwrap cannot create a PID namespace here")
    code = command(
        [
            "manager",
            "run",
            "--workspace",
            ws,
            "--allocation",
            "none",
            "--idle-timeout",
            "5",
            "--setting",
            "manager.confine=bwrap",
            "--setting",
            f"confine.bwrap={_BWRAP}",
        ],
        CLIContext("httk", tmp_path),
    )
    assert code != 0
    assert "bwrap cannot create a PID namespace here" in capsys.readouterr().err
    # Refused before it attached: no manager record is left behind.
    assert not list((workspace.control / "managers").iterdir())
    with pytest.raises(_confine.ConfinementUnavailableError):
        TaskManager(workspace, setting_overrides=_PINNED)


def test_a_workspace_switched_to_bwrap_is_probed_once_lazily(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        assert not sandbox.probes
        workspace.set_setting("manager.confine", "bwrap")
        workspace.set_setting("confine.bwrap", _BWRAP)
        identifiers = [_submit(workspace, tmp_path / "source", tag)[1] for tag in ("one", "two")]
        manager.run_until_idle(timeout=60.0)
    assert len(sandbox.probes) == 1
    assert all(_outcome(workspace, job_id)[:2] == ("succeeded", None) for job_id in identifiers)
    assert len(sandbox.records()) == 2


def _events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    return [record for record in caplog.records if getattr(record, "event", None) == name]


def test_a_lazy_probe_failure_holds_back_claims_until_bubblewrap_works(
    tmp_path: Path, sandbox: _Sandbox, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    sandbox.probe_result = _confine.ConfinementUnavailableError("no user namespaces")
    with (
        caplog.at_level("INFO", logger="httk.workflow.manager"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
        workspace.set_setting("manager.confine", "bwrap")
        workspace.set_setting("confine.bwrap", _BWRAP)
        identifiers = [_submit(workspace, tmp_path / "source", tag)[1] for tag in ("one", "two")]
        census = manager.run_until_idle(timeout=60.0)
        # A host condition is not the jobs' fault: they stay ready, and the
        # manager goes idle naming the condition instead of spinning.
        assert all(_outcome(workspace, job_id)[0] == "ready" for job_id in identifiers)
        assert census.ready_blocked == {"confinement": {"manager.confine": 2}}
        assert "confinement unavailable: 2" in census.summary_line()
        (reported,) = [record for record in _events(caplog, "confinement_unavailable") if record.levelname == "ERROR"]
        assert "no user namespaces" in reported.getMessage()
        # A failed probe stands until the reprobe interval passes.
        assert len(sandbox.probes) == 1
        manager.tick()
        assert len(sandbox.probes) == 1
        sandbox.probe_result = True
        monkeypatch.setattr(manager_module, "CONFINE_REPROBE_SECONDS", 0.0)
        manager.run_until_idle(timeout=60.0)
        assert _events(caplog, "confinement_available")
    assert len(sandbox.probes) == 2
    assert all(_outcome(workspace, job_id)[:2] == ("succeeded", None) for job_id in identifiers)
    assert len(sandbox.records()) == 2
    # No attempt was consumed by the held-back claims.
    for job_id in identifiers:
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and workspace.read_state(marker)["total_attempts"] == 1


def test_an_invalid_confinement_setting_holds_back_claims_naming_it(
    tmp_path: Path, sandbox: _Sandbox, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with (
        caplog.at_level("INFO", logger="httk.workflow.manager"),
        TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager,
    ):
        workspace.set_setting("confine.isolate_network", "maybe")
        _marker, job_id = _submit(workspace, tmp_path / "source", "invalid")
        manager.run_until_idle(timeout=60.0)
        assert _outcome(workspace, job_id)[0] == "ready"
        assert any(
            "confine.isolate_network" in record.getMessage() for record in _events(caplog, "confinement_unavailable")
        )
        # The settings are checked again once they change.
        workspace.set_setting("confine.isolate_network", "true")
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)


def test_unconfined_managers_do_not_validate_confinement_settings(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("confine.isolate_network", "maybe")
    workspace.set_setting("confine.unknown", "x")
    _marker, job_id = _submit(workspace, tmp_path / "source", "unconfined")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    assert not sandbox.probes and not sandbox.prepares


def test_a_claim_racing_a_confinement_condition_is_released_without_an_attempt(
    tmp_path: Path, sandbox: _Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker, job_id = _submit(workspace, tmp_path / "source", "raced")
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        # The claim pass saw confinement available; the condition appears before the launch.
        monkeypatch.setattr(manager, "_confinement_blocked", lambda: None)
        workspace.set_setting("confine.isolate_network", "maybe")
        for _ in range(5):
            manager.tick()
        current = workspace.find_marker_by_id(job_id)
        assert current is not None and current.kind == "ready"
        state = workspace.read_state(current)
        assert state["reason"] == "confinement_unavailable"
        assert state.get("total_attempts", 0) == 0
    assert not (workspace.payload_path(marker.placement, marker.job_key) / "attempts").exists()
    assert not sandbox.prepares


def test_probes_are_cached_per_sandbox_shape(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        assert len(sandbox.probes) == 1
        workspace.set_setting("confine.isolate_network", "false")
        _submit(workspace, tmp_path / "source", "shared-network")
        manager.run_until_idle(timeout=60.0)
        workspace.set_setting("confine.isolate_network", "true")
        _submit(workspace, tmp_path / "source", "isolated-again")
        manager.run_until_idle(timeout=60.0)
    assert [probe.isolate_network for probe in sandbox.probes] == [True, False]


# -- workspaces with the exchange extension require confinement ----------------------------


def _enrolled(tmp_path: Path) -> Workspace:
    workspace = Workspace.initialize(tmp_path / "workspace")
    assert enable_exchange(workspace)
    return workspace


def test_only_the_exchange_extension_requires_confinement(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    # Neither an exchange-shaped directory nor a daemon enrollment record enables the extension.
    for name in ("inbox", "outbox/rejected"):
        (workspace.root / "exchange" / name).mkdir(parents=True)
    (workspace.control / "exchange").mkdir()
    (workspace.control / "exchange" / "enrollment.json").write_text("{}", encoding="utf-8")
    TaskManager(workspace).close()
    shutil.rmtree(workspace.root / "exchange")
    assert enable_exchange(workspace)
    with pytest.raises(ConfinementUnavailableError, match="exchange extension"):
        TaskManager(workspace)


def test_an_unconfined_manager_on_a_plain_workspace_never_creates_the_exchange(
    tmp_path: Path, sandbox: _Sandbox
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _marker, first = _submit(workspace, tmp_path / "source", "first")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        for _ in range(5):
            manager.tick()
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, first)[:2] == ("succeeded", None)
    assert not os.path.lexists(workspace.root / "exchange") and "exchange" not in workspace.extensions
    assert not sandbox.prepares


def test_an_unconfined_manager_on_an_enrolled_workspace_is_refused(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = _enrolled(tmp_path)
    for overrides in ({}, {"manager.confine": "none"}):
        with pytest.raises(ConfinementUnavailableError, match="exchange extension"):
            TaskManager(workspace, setting_overrides=overrides)
    workspace.set_setting("manager.confine", "bwrap")
    workspace.set_setting("confine.bwrap", _BWRAP)
    with pytest.raises(ConfinementUnavailableError, match="manager.confine=bwrap"):
        TaskManager(workspace, setting_overrides={"manager.confine": "none"})
    assert not list((workspace.control / "managers").iterdir())


@pytest.mark.parametrize("addressing", ["registered", "by-path"])
def test_inline_cli_managers_on_an_enrolled_workspace_are_refused(
    tmp_path: Path, sandbox: _Sandbox, capsys: pytest.CaptureFixture[str], addressing: str
) -> None:
    workspace = _enrolled(tmp_path)
    target = ["--by-path", "--workspace", str(workspace.root)]
    if addressing == "registered":
        target = ["--workspace", register_ws(None, workspace.root)]
    code = command(
        ["manager", "run", *target, "--inline", "--allocation", "none", "--idle-timeout", "5"],
        CLIContext("httk", tmp_path),
    )
    assert code != 0
    assert "exchange extension" in capsys.readouterr().err
    assert not list((workspace.control / "managers").iterdir())
    assert not sandbox.probes


def test_a_confined_manager_on_an_enrolled_workspace_starts_and_serves(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = _enrolled(tmp_path)
    _marker, job_id = _submit(workspace, tmp_path / "source", "enrolled")
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    assert len(sandbox.records()) == 1
    # Through the workspace setting instead of a pin, and through the CLI.
    workspace.set_setting("manager.confine", "bwrap")
    workspace.set_setting("confine.bwrap", _BWRAP)
    with TaskManager(workspace, heartbeat_interval=0.01):
        pass
    code = command(
        [
            "manager",
            "run",
            "--by-path",
            "--workspace",
            str(workspace.root),
            "--inline",
            "--allocation",
            "none",
            "--idle-timeout",
            "30",
        ],
        CLIContext("httk", tmp_path),
    )
    assert code == 0


def test_an_enrolled_workspace_switched_to_unconfined_holds_back_claims(
    tmp_path: Path, sandbox: _Sandbox, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = _enrolled(tmp_path)
    workspace.set_setting("manager.confine", "bwrap")
    workspace.set_setting("confine.bwrap", _BWRAP)
    with (
        caplog.at_level("INFO", logger="httk.workflow.manager"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
        workspace.set_setting("manager.confine", "none")
        _marker, job_id = _submit(workspace, tmp_path / "source", "switched")
        manager.run_until_idle(timeout=60.0)
        assert _outcome(workspace, job_id)[0] == "ready"
        assert _events(caplog, "confinement_unavailable")
    assert not sandbox.records()


# -- confinement-start checks -------------------------------------------------------


def test_a_legacy_placement_naming_a_job_directory_is_not_confined(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker, _job_id = _submit(workspace, tmp_path / "source", "legacy")
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        legacy = replace(marker, placement=PurePosixPath(f"project/other--{uuid.uuid4()}"))
        with pytest.raises(FormatError, match="parses as a job key"):
            manager._check_confinement_start(legacy)
        manager._check_confinement_start(marker)


# -- the confined attempt -------------------------------------------------------------


def test_a_confined_attempt_gets_the_sandbox_environment_stdin_and_descriptors(
    tmp_path: Path, sandbox: _Sandbox, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("SLURM_JOB_ID", "4242")
    monkeypatch.setenv("PMIX_RANK", "0")
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.launch_template", "launcher -n {procs}")
    marker, job_id = _submit(workspace, tmp_path / "source", "confined", resources={"procs": 1})
    allocation = Allocation("host", None, (Node(socket.gethostname(), 2, 1000),), {})
    with (
        caplog.at_level("INFO", logger="httk.workflow.manager"),
        TaskManager(
            workspace,
            heartbeat_interval=0.01,
            resources=allocation.capacity(),
            allocation=allocation,
            setting_overrides=_PINNED,
        ) as manager,
    ):
        manager.run_until_idle(timeout=60.0)

    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    (call,) = sandbox.prepares
    job_path = workspace.payload_path(marker.placement, marker.job_key)
    assert call["workspace_root"] == workspace.root
    assert call["job_path"] == job_path
    assert call["workdir"] == job_path / "run"
    assert call["block_userns"] is True
    # Only an attempt with the launch client gets a lock directory, bound at its own host path.
    _locks_fd, locks_path = call["launch_locks"]
    attempt_id = json.loads(_seen(workspace, marker)["env"]["HTTK_WORKFLOW_CONTEXT"])["attempt_id"]
    assert locks_path == Path("/dev/shm") / f"httk-launch-{attempt_id}"
    assert not locks_path.exists(), "the manager removes the lock directory when it drops the attempt"
    (record,) = sandbox.records()
    # The sandbox's command is the attempt command; the gate stayed outside.
    assert record["argv"][0] == str(job_path / "files" / "runner")
    assert record["stdin_devnull"] is True
    # The sandbox descriptor was inherited, then closed in the manager.
    (descriptors,) = [closed for closed in sandbox.closed if closed]
    assert set(descriptors) <= set(record["fds"])
    for name, environment in (("sandbox", record["env"]), ("runner", _seen(workspace, marker)["env"])):
        assert environment["HTTK_WORKFLOW_CONFINED"] == "1", name
        assert environment["HOME"] == "/tmp/home" and environment["TMPDIR"] == "/tmp", name
        assert "SLURM_JOB_ID" not in environment and "PMIX_RANK" not in environment, name
        # The launch prefix is the launch client; the binding stays informational.
        assert environment["HTTK_WORKFLOW_LAUNCH"] == _manager_launches.client_prefix(), name
        assert environment["HTTK_WORKFLOW_LAUNCH_LOCKS"] == str(locks_path), name
        assert environment["HTTK_WORKFLOW_NODELIST"] == socket.gethostname(), name
        assert "HTTK_WORKFLOW_NODEFILE" in environment, name
    assert call["environment"] == {key: value for key, value in record["env"].items() if key in call["environment"]}
    launches = [record for record in caplog.records if getattr(record, "event", None) == "launch"]
    assert launches and getattr(launches[0], "confined", None) is True
    assert not any(getattr(item, "event", None) == "confined_launch_unavailable" for item in caplog.records)


def _launch_manager(workspace: Workspace, shm: Path) -> TaskManager:
    allocation = Allocation("host", None, (Node(socket.gethostname(), 2, 1000),), {})
    return TaskManager(
        workspace,
        heartbeat_interval=0.01,
        resources=allocation.capacity(),
        allocation=allocation,
        setting_overrides=_PINNED | {"confine.shm_root": str(shm)},
    )


def test_an_attempt_without_the_launch_client_gets_no_lock_directory(tmp_path: Path, sandbox: _Sandbox) -> None:
    shm = tmp_path / "shm"
    shm.mkdir(mode=0o700)
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker, job_id = _submit(workspace, tmp_path / "source", "nolaunch")
    # Without an allocation the attempt has no binding, hence no launch client.
    with TaskManager(
        workspace, heartbeat_interval=0.01, setting_overrides=_PINNED | {"confine.shm_root": str(shm)}
    ) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    (call,) = sandbox.prepares
    assert call["launch_locks"] is None
    assert "HTTK_WORKFLOW_LAUNCH_LOCKS" not in _seen(workspace, marker)["env"]
    assert list(shm.iterdir()) == []


def test_an_inherited_launch_locks_variable_never_reaches_an_attempt(
    tmp_path: Path, sandbox: _Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    shm = tmp_path / "shm"
    shm.mkdir(mode=0o700)
    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH_LOCKS", "/inherited")
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker, job_id = _submit(workspace, tmp_path / "source", "inherit")
    with TaskManager(
        workspace, heartbeat_interval=0.01, setting_overrides=_PINNED | {"confine.shm_root": str(shm)}
    ) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    assert "HTTK_WORKFLOW_LAUNCH_LOCKS" not in _seen(workspace, marker)["env"]


def test_a_pre_existing_lock_directory_refuses_the_attempt(
    tmp_path: Path, sandbox: _Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    shm = tmp_path / "shm"
    shm.mkdir(mode=0o700)
    planted: list[Path] = []
    original = _confine.create_launch_locks

    def plant(shm_root: Path, attempt_id: str) -> tuple[int, Path]:
        path = _confine.launch_locks_path(shm_root, attempt_id)
        path.mkdir(mode=0o700)
        planted.append(path)
        return original(shm_root, attempt_id)

    monkeypatch.setattr(_confine, "create_launch_locks", plant)
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.launch_template", "launcher -n {procs}")
    _marker, job_id = _submit(workspace, tmp_path / "source", "planted", resources={"procs": 1})
    with _launch_manager(workspace, shm) as manager:
        manager.run_until_idle(timeout=60.0)
    kind, code, message = _outcome(workspace, job_id)
    assert kind == "failed" and "exists already" in message, (kind, code, message)
    assert not sandbox.prepares and not sandbox.records()
    assert [path.exists() for path in planted] == [True], "the entry the manager did not make is left alone"


def test_a_failed_sandbox_removes_the_lock_directory(
    tmp_path: Path, sandbox: _Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    shm = tmp_path / "shm"
    shm.mkdir(mode=0o700)

    def refuse(settings: _confine.ConfineSettings, **arguments: Any) -> PreparedSandbox:
        assert len(list(shm.iterdir())) == 1
        raise ValueError("no sandbox today")

    monkeypatch.setattr(_confine, "prepare_attempt_sandbox", refuse)
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.launch_template", "launcher -n {procs}")
    _marker, job_id = _submit(workspace, tmp_path / "source", "refused", resources={"procs": 1})
    with _launch_manager(workspace, shm) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[0] == "failed"
    assert list(shm.iterdir()) == []


def test_a_clean_exit_with_a_surviving_attempt_removes_its_lock_directory(tmp_path: Path, sandbox: _Sandbox) -> None:
    shm = tmp_path / "shm"
    shm.mkdir(mode=0o700)
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.launch_template", "launcher -n {procs}")
    _marker, _job_id = _submit(
        workspace, tmp_path / "source", "survivor", body="time.sleep(120)\n", resources={"procs": 1}
    )
    manager = _launch_manager(workspace, shm)
    try:
        deadline = time.monotonic() + 30.0
        while not sandbox.records() and time.monotonic() < deadline:
            manager.tick()
            time.sleep(0.02)
        assert len(sandbox.records()) == 1
        assert manager.running_attempts == 1 and len(list(shm.iterdir())) == 1
    finally:
        manager.close()
        for line in sandbox.records():
            try:
                os.killpg(line["pgid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
    assert list(shm.iterdir()) == []


def test_an_unconfined_attempt_is_unchanged(
    tmp_path: Path, sandbox: _Sandbox, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker, job_id = _submit(workspace, tmp_path / "source", "plain")
    with (
        caplog.at_level("INFO", logger="httk.workflow.manager"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    assert not sandbox.probes and not sandbox.prepares and not sandbox.records()
    environment = _seen(workspace, marker)["env"]
    assert "HTTK_WORKFLOW_CONFINED" not in environment
    launches = [record for record in caplog.records if getattr(record, "event", None) == "launch"]
    assert launches and not hasattr(launches[0], "confined")


def test_a_shared_file_runner_runs_through_the_sandbox_by_descriptor(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source = tmp_path / "shared" / "succeed.py"
    source.parent.mkdir()
    source.write_text(_RUNNER.format(body=""), encoding="utf-8")
    source.chmod(0o755)
    reference = workspace.publish_runner(source)
    marker, job_id = _submit(workspace, tmp_path / "source", "shared", runner=dict(reference))
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    (record,) = sandbox.records()
    assert record["argv"][0].startswith("/dev/fd/")
    assert int(record["argv"][0].removeprefix("/dev/fd/")) in record["fds"]
    assert _seen(workspace, marker)["env"]["HTTK_WORKFLOW_CONFINED"] == "1"


def test_a_workflow_prelude_runs_inside_the_sandbox(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_workflow_prelude("tests.confinement", "export PRELUDE_RAN=yes")
    marker, job_id = _submit(workspace, tmp_path / "source", "prelude")
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    (record,) = sandbox.records()
    assert record["argv"][:2] == ["bash", "-l"]
    assert _seen(workspace, marker)["env"]["PRELUDE_RAN"] == "yes"


# -- supervision of a confined attempt --------------------------------------------------


_IGNORING = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(120)\n"


def _group_gone(pgid: int, *, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


@pytest.mark.timing
def test_a_confined_attempt_over_its_maxtime_is_stopped_through_its_process_group(
    tmp_path: Path, sandbox: _Sandbox
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _marker, job_id = _submit(workspace, tmp_path / "source", "slow", body=_IGNORING, resources={"maxtime": 1})
    with TaskManager(
        workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5, setting_overrides=_PINNED
    ) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("failed", "timeout")
    (record,) = sandbox.records()
    assert record["pgid"] == record["pid"]
    assert _group_gone(record["pgid"])


@pytest.mark.timing
def test_a_draining_manager_stops_a_confined_attempt(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _marker, job_id = _submit(workspace, tmp_path / "source", "drained", body="time.sleep(120)\n")

    def drain_once_confined() -> None:
        deadline = time.monotonic() + 30.0
        while not sandbox.records() and time.monotonic() < deadline:
            time.sleep(0.02)
        os.kill(os.getpid(), signal.SIGTERM)

    stopper = threading.Thread(target=drain_once_confined, daemon=True)
    with TaskManager(
        workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5, setting_overrides=_PINNED
    ) as manager:
        stopper.start()
        manager.run_until_idle(timeout=60.0, drain_timeout=10.0, drain_grace_seconds=0.5)
    stopper.join(timeout=5.0)
    (record,) = sandbox.records()
    assert _group_gone(record["pgid"])
    assert _outcome(workspace, job_id)[:2] == ("failed", "lease_lost")


@pytest.mark.timing
def test_cancelling_a_confined_attempt_kills_its_process_group(tmp_path: Path, sandbox: _Sandbox) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _marker, job_id = _submit(workspace, tmp_path / "source", "cancelled", body=_IGNORING)
    with TaskManager(
        workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5, setting_overrides=_PINNED
    ) as manager:
        deadline = time.monotonic() + 30.0
        running = None
        while time.monotonic() < deadline:
            manager.tick()
            running = workspace.find_marker_by_id(job_id)
            if running is not None and running.kind == "running" and sandbox.records():
                break
            time.sleep(0.02)
        assert running is not None and running.kind == "running"
        workspace.publish_request(
            {
                "format": "httk-workflow-request",
                "format_version": 2,
                "request_id": str(uuid.uuid4()),
                "job_id": running.job_id,
                "job_key": running.job_key,
                "placement": running.placement.as_posix(),
                "expected_generation": running.generation,
                "expected_record_ref": running.record_ref,
                "action": "cancel",
                "operator": "tester",
                "reason": "confinement test",
                "created_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            }
        )
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            manager.tick()
            marker = workspace.find_marker_by_id(job_id)
            if marker is not None and marker.kind == "cancelled":
                break
            time.sleep(0.02)
    assert _outcome(workspace, job_id)[0] == "cancelled"
    (record,) = sandbox.records()
    assert _group_gone(record["pgid"])


# -- requirement 3: job content cannot change confinement ---------------------------------

_SPAWNING_BODY = """
if context["step"] == "only":
    import uuid as _uuid
    child_id = str(_uuid.uuid5(_uuid.UUID(context["activation_id"]), "child"))
    child_key = "child--" + child_id
    draft = control / "outcome.tmp.test"
    child_dir = draft / "children" / "jobs" / child_key
    (child_dir / "files").mkdir(parents=True)
    (child_dir / "files" / "runner").write_text(Path(sys.argv[0]).read_text())
    (child_dir / "files" / "runner").chmod(0o755)
    document = json.loads((Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / "job.json").read_text())
    document.update(id=child_id, tag="child", initial_step="child", parent=None)
    (child_dir / "job.json").write_text(json.dumps(document))
    (draft / "children" / "spawn.json").write_text(json.dumps({"children": [{
        "workspace_id": context["workspace_id"], "job_id": child_id, "job_key": child_key,
        "placement": "project/children", "label": "child",
    }]}))
    (draft / "outcome.json").write_text(json.dumps({
        "format": "httk-workflow-outcome", "format_version": 2, "job_id": context["job_id"],
        "activation_id": context["activation_id"], "attempt_id": context["attempt_id"],
        "action": "wait", "next_step": "gather",
        "join": {"children": [{"workspace_id": context["workspace_id"], "job_id": child_id,
                               "job_key": child_key, "placement_hint": "project/children"}],
                 "condition": "all_terminal"},
    }))
    os.rename(draft, control / "outcome.ready")
    sys.exit(0)
"""

#: Job content that names the confinement settings everywhere a job can.
_HOSTILE_CONTENT = {
    "parameters": {"manager.confine": "none", "confine.bwrap": "/nonexistent", "manager_confine": "none"},
    "environment": {
        "declared": {
            "confine_mode": {"type": "string", "setting": "manager.confine", "default": "none"},
            "network": {"type": "string", "setting": "confine.isolate_network", "default": "false"},
        },
        "overrides": {"confine_mode": "none", "network": "false"},
    },
}


@pytest.mark.parametrize("workspace_mode", ["bwrap", "none"])
def test_job_content_cannot_change_the_confinement_the_manager_uses(
    tmp_path: Path, sandbox: _Sandbox, workspace_mode: str
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.confine", workspace_mode)
    workspace.set_setting("confine.bwrap", _BWRAP)
    marker, job_id = _submit(
        workspace,
        tmp_path / "source",
        "parent",
        body=_SPAWNING_BODY,
        extra={
            **_HOSTILE_CONTENT,
            "retry_policy": {"maximum_attempts_per_activation": 1, "maximum_total_attempts": 4, "retry_on": []},
        },
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=90.0)

    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    children = [item for item in workspace.scan_markers() if item.job_key.startswith("child--")]
    assert len(children) == 1 and children[0].kind == "succeeded"
    # Parent (two steps) and child: every attempt is confined exactly as the
    # workspace says, whatever the jobs declare.
    expected = 3 if workspace_mode == "bwrap" else 0
    assert len(sandbox.prepares) == expected and len(sandbox.records()) == expected
    for call in sandbox.prepares:
        assert call["settings"].mode == "bwrap"
        assert call["settings"].isolate_network is True
        assert call["settings"].bwrap == Path(_BWRAP)
    # The manager's own view of its settings ignores the job content.
    assert _seen(workspace, marker)["settings"]["manager.confine"] == workspace_mode


# -- real Bubblewrap ----------------------------------------------------------------------


def _unsupported_namespace_failure(detail: str) -> None:
    lowered = detail.lower()
    expected = any(
        phrase in lowered
        for phrase in ("operation not permitted", "permission denied", "no permissions to create", "user namespace")
    )
    if not expected:
        pytest.fail(f"Bubblewrap sandbox failed unexpectedly: {detail}")
    if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
        pytest.fail(f"required Bubblewrap sandbox is unavailable: {detail}")
    pytest.skip(f"Bubblewrap user namespaces are unavailable: {detail}")


_CONFINED_PROBE = """
job = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
workspace = Path(os.environ["HTTK_WORKFLOW_WORKSPACE_DIR"])
sibling = Path(os.environ["HTTK_SIBLING"])
results = {}
for name, target in (
    ("own", job / "own.txt"),
    ("sibling", sibling / "planted.txt"),
    ("control", workspace / ".httk-workspace" / "planted.txt"),
    ("root", workspace / "planted.txt"),
):
    try:
        target.write_text("planted")
        results[name] = "written"
    except OSError:
        results[name] = "refused"
(job / "results.json").write_text(json.dumps(results))
"""


def test_a_real_bubblewrap_attempt_writes_only_its_job_directory(tmp_path: Path) -> None:
    if shutil.which("bwrap") is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    pinned = {"manager.confine": "bwrap"}
    try:
        _confine.probe_bwrap(_confine.confine_settings(pinned))
    except _confine.ConfinementUnavailableError as exc:
        _unsupported_namespace_failure(str(exc))
        raise
    workspace = Workspace.initialize(tmp_path / "workspace")
    sibling, _sibling_id = _submit(workspace, tmp_path / "source", "sibling")
    sibling_path = workspace.payload_path(sibling.placement, sibling.job_key)
    workspace.set_setting("sibling", str(sibling_path))
    workspace.set_workflow_prelude("tests.confinement", "export PRELUDE_RAN=yes")
    confined, confined_id = _submit(workspace, tmp_path / "source", "confined", body=_CONFINED_PROBE)
    source = tmp_path / "shared" / "succeed.py"
    source.parent.mkdir()
    source.write_text(_RUNNER.format(body=""), encoding="utf-8")
    source.chmod(0o755)
    shared, shared_id = _submit(workspace, tmp_path / "source", "shared", runner=dict(workspace.publish_runner(source)))
    before = {path for path in workspace.root.iterdir()}

    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=pinned) as manager:
        manager.run_until_idle(timeout=120.0)

    for job_id in (confined_id, shared_id):
        kind, _code, message = _outcome(workspace, job_id)
        if kind != "succeeded":
            stdio = workspace.payload_path(confined.placement, confined.job_key) / "logs" / "stdio.out"
            _unsupported_namespace_failure(message + (stdio.read_text(errors="replace") if stdio.exists() else ""))
    confined_path = workspace.payload_path(confined.placement, confined.job_key)
    results = json.loads((confined_path / "results.json").read_text(encoding="utf-8"))
    assert results == {"own": "written", "sibling": "refused", "control": "refused", "root": "refused"}
    assert not (sibling_path / "planted.txt").exists()
    assert not (workspace.control / "planted.txt").exists()
    assert {path for path in workspace.root.iterdir()} == before
    for marker in (confined, shared):
        environment = _seen(workspace, marker)["env"]
        assert environment["HTTK_WORKFLOW_CONFINED"] == "1"
        assert environment["PRELUDE_RAN"] == "yes"
    assert stat.S_ISREG((confined_path / "own.txt").stat().st_mode)
