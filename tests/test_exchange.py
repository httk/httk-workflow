"""The exchange extension on a real filesystem: enabling it, inbox adoption, returns, job control and status."""

import base64
import errno
import json
import os
import threading
import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity, register_ws
from httk.workflow import TaskManager, Workspace, _confine, _exchange, _fs, _kernel, _requests, removal
from httk.workflow._bundles import MAX_MANIFEST_BYTES, BundleManifest
from httk.workflow._daemon_auth import sign_request
from httk.workflow._daemon_protocol import Request, decode_response
from httk.workflow._exchange import (
    EXCHANGE_SUBDIRECTORIES,
    ExchangeService,
    ExchangeUnavailableError,
    enable_exchange,
    exchange_directory,
)
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow._state import Release, StateDoc, read_state_unowned
from httk.workflow.errors import ConfinementUnavailableError, SealedError
from httk.workflow.seals import seal_workspace
from httk.workflow.workflow_cli import command
from test_bundles import write_bundle
from v3_helpers import cli_owner, install, job_mapping

ENROLLMENT_ID = "fedcba9876543210fedcba9876543210"


@pytest.fixture(autouse=True)
def _no_faults() -> Iterator[None]:
    yield
    _fs.set_fault_injector(None)


def _extensions(workspace: Workspace) -> list[str]:
    return list(json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))["extensions"])


def _server(root: Path) -> Workspace:
    return Workspace.initialize(
        root, durable=False, policy={"visibility_deadline_seconds": 0.05}, extensions=["exchange"]
    )


def _seed(tmp_path: Path, byte: int) -> Path:
    path = tmp_path / f"{byte}.seed"
    path.write_text(base64.b64encode(bytes([byte]) * 32).decode("ascii") + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _key(seed: Path) -> str:
    from httk.core.identity import identity_public_key

    key = identity_public_key(seed)
    assert key is not None
    return key


def _pass(ws: Workspace) -> bool:
    with cli_owner(ws) as owner:
        return ExchangeService(ws, owner).run()


def _send(ws: Workspace, name: str = "entry") -> list[dict[str, object]]:
    """Write a fresh client bundle (root, child, grandchild) into the inbox, as a client does."""

    return write_bundle(exchange_directory(ws) / "inbox" / name)[1]


def _rejections(ws: Workspace) -> list[tuple[list[str], dict[str, Any]]]:
    rejected = exchange_directory(ws) / "outbox" / "rejected"
    found = []
    for unique in sorted(rejected.iterdir()):
        entries = sorted(name for name in os.listdir(unique) if name != "reason.json")
        found.append((entries, json.loads((unique / "reason.json").read_bytes())))
    return found


def _indexed(ws: Workspace, client_id: object) -> _kernel.JobRef:
    indexed = _kernel.exchange_index(ws, str(client_id))
    assert indexed is not None
    ref = _kernel.locate(ws, indexed[0], placement_hint=indexed[1])
    assert ref is not None
    return ref


def _all_jobs(ws: Workspace) -> list[_kernel.JobRef]:
    return [
        ref for state in ("ready", "succeeded", "cancelled", "paused", "failed") for ref in _kernel.list_jobs(ws, state)
    ]


def _finish(ws: Workspace, state: str = "succeeded") -> None:
    """Move every job of the workspace to *state*, as managers running them would."""

    with cli_owner(ws) as owner:
        for ref in _all_jobs(ws):
            owned = _kernel.claim(ws, owner, ref)
            assert owned is not None
            doc = owned.read_state()
            assert doc is not None
            owned.release(doc.next_activation("start", "initial"), Release(state, ref.priority))


def _apply(ws: Workspace, ref: _kernel.JobRef) -> None:
    """Apply a job's pending requests as its owner, as a manager would at its next boundary."""

    current = _kernel.locate(ws, ref.job_id, placement_hint=ref.placement)
    assert current is not None
    with cli_owner(ws) as owner:
        assert removal.serve(ws, owner, current)


# -- enabling ----------------------------------------------------------------------------------------------------------


def test_enable_creates_the_layout_and_records_the_extension(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    assert "exchange" not in workspace.extensions and _extensions(workspace) == []
    assert enable_exchange(workspace) is True
    exchange = exchange_directory(workspace)
    for relative in EXCHANGE_SUBDIRECTORIES:
        assert (exchange / relative).is_dir() and not (exchange / relative).is_symlink()
    assert os.listdir(exchange / "outbox") == ["rejected"]
    assert json.loads((exchange / "exchange.json").read_bytes()) == {
        "format": "httk-workspace-exchange",
        "format_version": 1,
        "workspace_id": workspace.workspace_id,
    }
    assert sorted(os.listdir(exchange)) == ["exchange.json", "inbox", "managers", "outbox", "requests", "responses"]
    assert _extensions(workspace) == ["exchange"] and "exchange" in Workspace(workspace.root).extensions
    # Nothing is left behind: no rename probe.
    assert os.listdir(workspace.control / "tmp") == []


def test_enable_is_idempotent_and_completes_a_half_made_exchange(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    (workspace.root / "exchange" / "inbox").mkdir(parents=True)
    assert enable_exchange(workspace)
    (exchange_directory(workspace) / "inbox" / "a-bundle").mkdir()
    before = (workspace.control / "format.json").read_bytes()
    assert enable_exchange(Workspace(workspace.root)) is False
    assert (workspace.control / "format.json").read_bytes() == before
    assert os.listdir(exchange_directory(workspace) / "inbox") == ["a-bundle"]
    os.rmdir(workspace.root / "exchange" / "outbox" / "rejected")
    assert enable_exchange(workspace)
    assert (workspace.root / "exchange" / "outbox" / "rejected").is_dir()


@pytest.mark.parametrize("hostile", ["exchange", "inbox", "rejected", "file"])
def test_enable_refuses_a_symlinked_or_foreign_exchange(tmp_path: Path, hostile: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    exchange = workspace.root / "exchange"
    if hostile == "exchange":
        exchange.symlink_to(elsewhere)
    elif hostile == "file":
        exchange.write_text("not a directory", encoding="utf-8")
    else:
        relative = {"inbox": "inbox", "rejected": "outbox/rejected"}[hostile]
        (exchange / relative).parent.mkdir(parents=True, exist_ok=True)
        (exchange / relative).symlink_to(elsewhere)
    with pytest.raises(ExchangeUnavailableError, match="not a real directory"):
        enable_exchange(workspace)
    assert _extensions(workspace) == [] and os.listdir(elsewhere) == []


def test_enable_refuses_when_the_rename_probe_crosses_filesystems(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    rename = os.rename

    def cross_device(src: object, dst: object, **arguments: object) -> None:
        if ".probe." in str(src) and str(dst).startswith(str(workspace.root / "exchange")):
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        rename(src, dst, **arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", cross_device)
    with pytest.raises(ExchangeUnavailableError, match="different filesystems"):
        enable_exchange(workspace)
    monkeypatch.undo()
    assert _extensions(workspace) == [] and os.listdir(workspace.control / "tmp") == []
    assert enable_exchange(workspace) and _extensions(workspace) == ["exchange"]


def test_enable_refuses_a_sealed_workspace(tmp_path: Path) -> None:
    configure_identity()
    workspace = Workspace.initialize(tmp_path / "workspace")
    seal_workspace(workspace)
    with pytest.raises(SealedError):
        enable_exchange(workspace)
    assert not os.path.lexists(workspace.root / "exchange")


def test_the_enable_command_reports_and_is_idempotent(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root)
    assert command(["workspace", "exchange", "enable", name], context) == 0
    assert f"{workspace.root}\texchange enabled\t{workspace.root / 'exchange'}" in capsys.readouterr().out
    assert command(["workspace", "exchange", "enable", "--by-path", str(workspace.root)], context) == 0
    assert "exchange already enabled" in capsys.readouterr().out


def test_an_unconfined_manager_refuses_a_workspace_with_the_extension(tmp_path: Path) -> None:
    workspace = _server(tmp_path / "workspace")
    with pytest.raises(ConfinementUnavailableError, match="exchange extension"):
        TaskManager(Workspace(workspace.root))


# -- inbox adoption ----------------------------------------------------------------------------------------------------


def test_an_inbox_bundle_is_adopted_with_fresh_ids_and_its_exchange_name(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    jobs = _send(ws)
    assert _pass(ws) is True
    assert os.listdir(exchange_directory(ws) / "inbox") == []
    client_ids = {str(job["id"]) for job in jobs}
    adopted = _all_jobs(ws)
    assert len(adopted) == 3 and not {ref.job_id for ref in adopted} & client_ids
    names = set()
    for ref in adopted:
        doc, damaged = read_state_unowned(ref.path / "state.json")
        assert doc is not None and not damaged and doc.origin == "exchange"
        names.add(doc.exchange_name)
    assert names == client_ids
    root = _indexed(ws, jobs[0]["id"])
    assert root.state == "ready"
    # Nothing more to adopt: the next pass changes nothing.
    assert _pass(ws) is False


def _fifo(path: Path) -> None:
    os.mkfifo(path)


def _symlink(path: Path) -> None:
    path.symlink_to(path.parent.parent / "exchange.json")


def _oversized(path: Path) -> None:
    write_bundle(path)
    (path / "bundle.json").write_bytes(b" " * (MAX_MANIFEST_BYTES + 1))


@pytest.mark.parametrize("plant", [_fifo, _symlink, _oversized])
def test_a_fifo_symlink_or_oversized_entry_is_refused_to_the_rejected_directory(tmp_path: Path, plant: Any) -> None:
    ws = _server(tmp_path / "ws")
    before = (exchange_directory(ws) / "exchange.json").read_bytes()
    plant(exchange_directory(ws) / "inbox" / "bad")
    assert _pass(ws) is True
    assert os.listdir(exchange_directory(ws) / "inbox") == []
    [(entries, reason)] = _rejections(ws)
    assert entries == ["bad"]
    assert reason["format"] == "httk-workspace-exchange-rejection" and reason["name"] == "bad" and reason["reason"]
    assert _all_jobs(ws) == []
    # A symlink is moved, never followed.
    assert (exchange_directory(ws) / "exchange.json").read_bytes() == before
    assert not list((ws.control / "tmp").iterdir())


def test_dot_and_reserved_names_are_left_alone(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    inbox = exchange_directory(ws) / "inbox"
    for name in (".hidden", "status.json", "plan.json"):
        write_bundle(inbox / name)
    assert _pass(ws) is False
    assert sorted(os.listdir(inbox)) == [".hidden", "plan.json", "status.json"]


def test_two_managers_racing_for_one_inbox_entry_adopt_it_once(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    _send(ws)
    first, second = cli_owner(ws), cli_owner(ws)
    raced: list[bool] = []

    def other_manager_first(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "before" and src is not None and src.at is not None and not raced:
            _fs.set_fault_injector(None)
            raced.append(ExchangeService(ws, second).run())

    _fs.set_fault_injector(other_manager_first)
    assert ExchangeService(ws, first).run() is False  # it lost the take
    assert raced == [True]
    published = _all_jobs(ws)
    assert len(published) == len({ref.job_id for ref in published}) == 3
    first.close()
    second.close()
    assert not list((ws.control / "tmp").iterdir())


# -- returns -----------------------------------------------------------------------------------------------------------


def test_a_finished_exchange_tree_returns_to_the_outbox_and_leaves_the_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _server(tmp_path / "ws")
    jobs = _send(ws)
    _pass(ws)
    root = _indexed(ws, jobs[0]["id"])
    # Not finished yet: nothing returns.
    assert _pass(ws) is False
    _finish(ws)
    assert _pass(ws) is True
    _apply(ws, root)
    outbox = exchange_directory(ws) / "outbox" / str(jobs[0]["id"])
    manifest = BundleManifest.from_json((outbox / root.job_key / "bundle.json").read_bytes())
    assert len(manifest.members) == 3 and {member.state for member in manifest.members} == {"succeeded"}
    assert _all_jobs(ws) == []
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is None
    assert not list((ws.control / "requests").glob("*.json"))
    monkeypatch.setattr(_exchange, "STEP_INTERVAL", 0.0)
    assert _pass(ws) is False


def test_an_occupied_outbox_name_is_retried_later(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _server(tmp_path / "ws")
    jobs = _send(ws)
    _pass(ws)
    root = _indexed(ws, jobs[0]["id"])
    _finish(ws)
    occupant = exchange_directory(ws) / "outbox" / str(jobs[0]["id"]) / root.job_key
    occupant.mkdir(parents=True)
    (occupant / "earlier").write_text("not fetched yet")
    with cli_owner(ws) as owner:
        service = ExchangeService(ws, owner)
        assert service.run() is True
        _apply(ws, root)  # the eject rolls back: the jobs stay where they were
        assert len(_all_jobs(ws)) == 3 and _indexed(ws, jobs[0]["id"]).state == "succeeded"
        service.invalidate()
        assert service.run() is False  # throttled per job
        (occupant / "earlier").unlink()
        occupant.rmdir()
        monkeypatch.setattr(_exchange, "STEP_INTERVAL", 0.0)
        service.invalidate()
        assert service.run() is True  # a fresh request: the rolled-back eject changed the root's state
    _apply(ws, root)
    assert (occupant / "bundle.json").is_file() and _all_jobs(ws) == []


# -- job control -------------------------------------------------------------------------------------------------------


def _job_action(
    ws: Workspace,
    operation: str,
    client_id: object,
    seed: Path | None,
    *,
    now: int | None = None,
    request_id: str | None = None,
) -> Request:
    """Publish a client's job action into ``exchange/requests``; *seed* ``None`` leaves it unsigned."""

    request = Request(
        request_id or uuid.uuid4().hex,
        ws.workspace_id,
        operation,
        enrollment_id=ENROLLMENT_ID,
        job=str(client_id),
    )
    if seed is not None:
        request = sign_request(request, seed_path=seed, now=now)
    from httk.workflow._daemon_protocol import encode_request

    (exchange_directory(ws) / "requests" / f"{request.request_id}.json").write_bytes(encode_request(request))
    return request


def _response(ws: Workspace, request: Request) -> tuple[str, str | None]:
    response = decode_response((exchange_directory(ws) / "responses" / f"{request.request_id}.json").read_bytes())
    assert response.signature is None and response.operator_key is None
    return response.outcome, response.reason


@pytest.fixture
def control(tmp_path: Path) -> tuple[Workspace, list[dict[str, object]], Path]:
    ws = _server(tmp_path / "ws")
    seed = _seed(tmp_path, 1)
    ws.set_setting(_exchange.AUTHORIZED_KEYS_SETTING, f"{_key(_seed(seed.parent, 3))}, {_key(seed)}")
    jobs = _send(ws)
    _pass(ws)
    return ws, jobs, seed


def test_a_signed_cancel_job_cancels_the_job(control: tuple[Workspace, list[dict[str, object]], Path]) -> None:
    ws, jobs, seed = control
    root = _indexed(ws, jobs[0]["id"])
    request = _job_action(ws, "cancel_job", jobs[0]["id"], seed)
    assert _pass(ws) is True
    assert _response(ws, request) == ("accepted", None)
    assert not (exchange_directory(ws) / "requests" / f"{request.request_id}.json").exists()
    translated = ws.control / "requests" / f"{root.job_id}.{uuid.UUID(request.request_id)}.json"
    document = json.loads(translated.read_bytes())
    assert (document["action"], document["operator"]) == ("cancel", "exchange")
    _apply(ws, root)
    assert _indexed(ws, jobs[0]["id"]).state == "cancelled"
    assert _kernel.exchange_translation_applied(ws, str(jobs[0]["id"]), str(uuid.UUID(request.request_id)))


def test_unsigned_foreign_expired_and_unknown_job_actions_are_refused(
    tmp_path: Path, control: tuple[Workspace, list[dict[str, object]], Path]
) -> None:
    ws, jobs, seed = control
    unsigned = _job_action(ws, "cancel_job", jobs[0]["id"], None)
    foreign = _job_action(ws, "cancel_job", jobs[0]["id"], _seed(tmp_path, 2))
    expired = _job_action(ws, "cancel_job", jobs[0]["id"], seed, now=1_000)
    unknown = _job_action(ws, "cancel_job", uuid.uuid4(), seed)
    assert _pass(ws) is True
    assert _response(ws, unsigned) == ("refused", "request_unauthorized")
    assert _response(ws, foreign) == ("refused", "request_unauthorized")
    assert _response(ws, expired) == ("refused", "request_expired")
    assert _response(ws, unknown) == ("refused", "unknown_job")
    assert os.listdir(exchange_directory(ws) / "requests") == []
    assert not list((ws.control / "requests").glob("*.json"))
    assert _indexed(ws, jobs[0]["id"]).state == "ready"


def test_daemon_actions_are_left_to_the_daemon(control: tuple[Workspace, list[dict[str, object]], Path]) -> None:
    ws, _jobs, seed = control
    health = sign_request(
        Request(uuid.uuid4().hex, ws.workspace_id, "health", enrollment_id=ENROLLMENT_ID), seed_path=seed
    )
    from httk.workflow._daemon_protocol import encode_request

    path = exchange_directory(ws) / "requests" / f"{health.request_id}.json"
    path.write_bytes(encode_request(health))
    (exchange_directory(ws) / "requests" / ("f" * 32 + ".json")).write_bytes(b"not json")
    assert _pass(ws) is False
    assert path.exists() and os.listdir(exchange_directory(ws) / "responses") == []


def test_two_managers_translating_one_request_apply_it_once(
    control: tuple[Workspace, list[dict[str, object]], Path],
) -> None:
    ws, jobs, seed = control
    root = _indexed(ws, jobs[0]["id"])
    request = _job_action(ws, "stop_job", jobs[0]["id"], seed)
    published = (exchange_directory(ws) / "requests" / f"{request.request_id}.json").read_bytes()
    assert _pass(ws) is True
    # Another manager read the request before the first deleted it, and translates it again.
    (exchange_directory(ws) / "requests" / f"{request.request_id}.json").write_bytes(published)
    (exchange_directory(ws) / "responses" / f"{request.request_id}.json").unlink()
    assert _pass(ws) is True
    assert len(list((ws.control / "requests").glob("*.json"))) == 1
    _apply(ws, root)
    doc = read_state_unowned(_indexed(ws, jobs[0]["id"]).path / "state.json")[0]
    assert doc is not None
    applied = [entry for entry in doc.history_tail if entry.get("event") == "request_applied"]
    assert [entry.get("action") for entry in applied] == ["pause"]
    assert _indexed(ws, jobs[0]["id"]).state == "paused"


def test_a_lagging_translator_never_reapplies_an_applied_action(
    control: tuple[Workspace, list[dict[str, object]], Path],
) -> None:
    ws, jobs, seed = control
    root = _indexed(ws, jobs[0]["id"])
    request = _job_action(ws, "stop_job", jobs[0]["id"], seed)
    published = (exchange_directory(ws) / "requests" / f"{request.request_id}.json").read_bytes()
    assert _pass(ws) is True
    _apply(ws, root)
    assert _indexed(ws, jobs[0]["id"]).state == "paused"
    # The operator continues the job; that release no longer carries the stop among its applied requests.
    _requests.post(ws, action="continue", job_id=root.job_id, placement=root.placement or "", operator="t", reason="t")
    _apply(ws, root)
    _apply(ws, root)
    resumed = _indexed(ws, jobs[0]["id"])
    doc = read_state_unowned(resumed.path / "state.json")[0]
    assert resumed.state == "ready" and doc is not None
    assert str(uuid.UUID(request.request_id)) not in doc.applied_requests
    # A lagging translator re-posts the stop: the owner drops it (the index's translated/ set).
    (exchange_directory(ws) / "requests" / f"{request.request_id}.json").write_bytes(published)
    (exchange_directory(ws) / "responses" / f"{request.request_id}.json").unlink()
    assert _pass(ws) is True
    assert len(list((ws.control / "requests").glob("*.json"))) == 1
    _apply(ws, root)
    assert _indexed(ws, jobs[0]["id"]).state == "ready"
    assert not list((ws.control / "requests").glob("*.json"))


def test_a_client_eject_job_returns_the_tree(control: tuple[Workspace, list[dict[str, object]], Path]) -> None:
    ws, jobs, seed = control
    root = _indexed(ws, jobs[0]["id"])
    _finish(ws, "failed")
    _job_action(ws, "eject_job", jobs[0]["id"], seed)
    with cli_owner(ws) as owner:
        service = ExchangeService(ws, owner)
        service._last["returns"] = float("inf")  # only the client's request, no return of its own
        assert service.run() is True
    _apply(ws, root)
    bundle = exchange_directory(ws) / "outbox" / str(jobs[0]["id"]) / root.job_key
    assert len(BundleManifest.from_json((bundle / "bundle.json").read_bytes()).members) == 3
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is None


# -- status ------------------------------------------------------------------------------------------------------------


def test_the_status_lists_exchange_jobs_with_a_sanitized_progress_excerpt(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    jobs = _send(ws, "first")
    second = _send(ws, "second")
    _pass(ws)
    _pass(ws)
    root = _indexed(ws, jobs[0]["id"])
    progress = {"step": "relax", "fraction": 0.5, "nested": {"x": 1}, "text": "a\x00b" + "c" * 1000, "nan": None}
    (root.path / "progress.json").write_text(json.dumps(progress), encoding="utf-8")
    os.mkfifo(_indexed(ws, second[0]["id"]).path / "progress.json")  # never blocks the pass
    status_path = exchange_directory(ws) / "status.json"
    status_path.unlink(missing_ok=True)
    reader = threading.Thread(target=_pass, args=(ws,))
    reader.start()
    reader.join(timeout=30)
    assert not reader.is_alive()
    status = json.loads(status_path.read_bytes())
    assert status["format"] == "httk-workspace-exchange-status" and status["workspace_id"] == ws.workspace_id
    by_name = {entry["exchange_name"]: entry for entry in status["jobs"]}
    assert set(by_name) == {str(jobs[0]["id"]), str(second[0]["id"])}
    entry = by_name[str(jobs[0]["id"])]
    assert (entry["job_id"], entry["job_key"], entry["state"]) == (root.job_id, root.job_key, "ready")
    assert entry["progress"] == {"step": "relax", "fraction": 0.5, "text": "ab" + "c" * 254}
    assert "progress" not in by_name[str(second[0]["id"])]
    assert status["truncated"] is False


def test_a_symlink_at_status_json_is_replaced_not_followed(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    target = tmp_path / "target"
    target.write_text("keep")
    (exchange_directory(ws) / "status.json").symlink_to(target)
    _pass(ws)
    assert target.read_text() == "keep" and not (exchange_directory(ws) / "status.json").is_symlink()


def test_a_symlinked_inbox_stops_the_pass(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    ws = _server(tmp_path / "ws")
    inbox = exchange_directory(ws) / "inbox"
    os.rmdir(inbox)
    (tmp_path / "elsewhere").mkdir()
    write_bundle(tmp_path / "elsewhere" / "entry")
    inbox.symlink_to(tmp_path / "elsewhere")
    with caplog.at_level("WARNING"):
        assert _pass(ws) is False
    assert "not served" in caplog.text and _all_jobs(ws) == []
    assert os.listdir(tmp_path / "elsewhere") == ["entry"]


# -- a manager serving the exchange ------------------------------------------------------------------------------------


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


_PINNED = {"manager.confine": "bwrap", "confine.bwrap": "/usr/bin/bwrap"}


def test_an_until_idle_manager_adopts_runs_and_returns_a_client_job(
    tmp_path: Path, sandboxes: list[Mapping[str, Any]]
) -> None:
    ws = _server(tmp_path / "ws")
    installed = install(ws, tmp_path / "package")
    client_job = job_mapping(installed, {"start": "succeed"}, tag="trip", placement="client/trip")
    write_bundle(exchange_directory(ws) / "inbox" / "trip", [client_job])
    with TaskManager(ws, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager.run_until_idle(timeout=120.0, poll_interval=0.02)
    assert [call["settings"].mode for call in sandboxes] == ["bwrap"]
    [bundle] = (exchange_directory(ws) / "outbox" / str(client_job["id"])).iterdir()
    manifest = BundleManifest.from_json((bundle / "bundle.json").read_bytes())
    assert [member.state for member in manifest.members] == ["succeeded"]
    doc, _ = read_state_unowned(manifest.member_dir(bundle, manifest.members[0]) / "state.json")
    assert doc is not None and doc.origin == "exchange" and doc.exchange_name == str(client_job["id"])
    assert _all_jobs(ws) == [] and _kernel.exchange_index(ws, str(client_job["id"])) is None
    assert os.listdir(exchange_directory(ws) / "inbox") == []
    status = json.loads((exchange_directory(ws) / "status.json").read_bytes())
    assert status["format"] == "httk-workspace-exchange-status"


def test_only_unrestricted_managers_serve_the_exchange(tmp_path: Path, sandboxes: list[Mapping[str, Any]]) -> None:
    ws = _server(tmp_path / "ws")
    _send(ws)
    with TaskManager(ws, heartbeat_interval=0.01, setting_overrides=_PINNED, placement_prefixes=["x"]) as manager:
        manager.tick()
    with TaskManager(ws, heartbeat_interval=0.01, setting_overrides=_PINNED, pools=["gpu"]) as manager:
        manager.tick()
    assert os.listdir(exchange_directory(ws) / "inbox") == ["entry"]
    with TaskManager(ws, heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager.tick()
    assert os.listdir(exchange_directory(ws) / "inbox") == []
    assert len(_all_jobs(ws)) == 3


def test_the_translation_guard_covers_only_exchange_derived_requests(
    control: tuple[Workspace, list[dict[str, object]], Path],
) -> None:
    ws, jobs, seed = control
    root = _indexed(ws, jobs[0]["id"])
    request = _job_action(ws, "cancel_job", jobs[0]["id"], seed)
    _pass(ws)
    doc = StateDoc.empty(root.job_id).updated(origin="exchange", exchange_name=str(jobs[0]["id"]))
    parsed = _requests.parse(ws.control / "requests" / f"{root.job_id}.{uuid.UUID(request.request_id)}.json")
    assert not removal.translation_applied(ws, doc, parsed)
    _kernel.record_exchange_translation(ws, str(jobs[0]["id"]), str(uuid.UUID(request.request_id)))
    assert removal.translation_applied(ws, doc, parsed)
    # A request an operator posts is never guarded by the exchange index.
    local = _requests.parse(
        _requests.post(
            ws, action="cancel", job_id=root.job_id, placement=root.placement or "", operator="t", reason="t"
        )
    )
    assert not removal.translation_applied(ws, doc, local)
