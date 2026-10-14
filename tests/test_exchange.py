"""The exchange extension on a real filesystem: enabling it, inbox adoption, returns, job control and status."""

import base64
import errno
import json
import os
import shutil
import threading
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity, register_ws
from httk.workflow import TaskManager, Workspace, _confine, _exchange, _fs, _kernel, _moving, removal
from httk.workflow._bundles import MAX_MANIFEST_BYTES, BundleManifest
from httk.workflow._daemon_client import Endpoint, read_passive_status
from httk.workflow._exchange import (
    EXCHANGE_SUBDIRECTORIES,
    ExchangeService,
    ExchangeUnavailableError,
    enable_exchange,
    exchange_directory,
)
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow._state import Release, read_state_unowned
from httk.workflow.errors import ConfinementUnavailableError, SealedError
from httk.workflow.seals import seal_workspace
from httk.workflow.workflow_cli import command
from test_bundles import key_of, write_bundle
from test_moving import claim_root
from v3_helpers import cli_owner, install, job_mapping, submit_mapping
from v3_helpers import workspace as v3_workspace

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
    assert sorted(os.listdir(exchange)) == ["exchange.json", "inbox", "launchers", "outbox", "requests", "responses"]
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


def test_an_ejected_fresh_job_is_adopted_from_the_inbox(tmp_path: Path) -> None:
    client, server = v3_workspace(tmp_path / "client"), _server(tmp_path / "server")
    mapping = job_mapping(("demo--0123456789abcdef", "demo"), {"start": "succeed"}, tag="fresh")
    ref = submit_mapping(client, mapping)
    inbox = exchange_directory(server) / "inbox"
    with cli_owner(client) as owner:
        report = _moving.eject(client, owner, claim_root(client, owner, ref.job_id), destination=inbox, tree=False)

    assert report.destination.parent == inbox and report.destination.is_dir()
    member = next((report.destination / "jobs").rglob("job.json")).parent
    assert not (member / "logs").exists()
    assert _pass(server) is True
    assert os.listdir(inbox) == []
    assert _rejections(server) == []
    [adopted] = _all_jobs(server)
    doc, damaged = read_state_unowned(adopted.path / "state.json")
    assert adopted.state == "ready" and doc is not None and not damaged and doc.origin == "exchange"


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
    # Flat in the outbox, under the client's own job key.
    outbox = exchange_directory(ws) / "outbox"
    assert sorted(os.listdir(outbox)) == sorted([key_of(jobs[0]), "rejected"])
    manifest = BundleManifest.from_json((outbox / key_of(jobs[0]) / "bundle.json").read_bytes())
    assert len(manifest.members) == 3 and {member.state for member in manifest.members} == {"succeeded"}
    assert _all_jobs(ws) == []
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is None
    assert not list((ws.control / "requests").glob("*.json"))
    monkeypatch.setattr(_exchange, "STEP_INTERVAL", 0.0)
    assert _pass(ws) is False


def test_an_occupied_outbox_name_is_not_requested_until_fetched(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    jobs = _send(ws)
    _pass(ws)
    root = _indexed(ws, jobs[0]["id"])
    _finish(ws)
    occupant = exchange_directory(ws) / "outbox" / key_of(jobs[0])
    occupant.mkdir()
    (occupant / "earlier").write_text("not fetched yet")
    with cli_owner(ws) as owner:
        service = ExchangeService(ws, owner)
        # Not fetched yet: nothing is requested, so nothing is claimed and rolled back.
        assert service.run() is False
        assert not list((ws.control / "requests").glob("*.json"))
        (occupant / "earlier").unlink()
        occupant.rmdir()
        service.invalidate()
        assert service.run() is True
    _apply(ws, root)
    assert (occupant / "bundle.json").is_file() and _all_jobs(ws) == []


@pytest.mark.parametrize("planted", ["outbox", "outbox/name"])
def test_a_symlinked_outbox_never_redirects_a_return(tmp_path: Path, planted: str) -> None:
    ws = _server(tmp_path / "ws")
    jobs = _send(ws)
    _pass(ws)
    root = _indexed(ws, jobs[0]["id"])
    _finish(ws)
    assert _pass(ws) is True  # the return request is posted before the client plants its symlink
    outbox = exchange_directory(ws) / "outbox"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    if planted == "outbox":
        shutil.rmtree(outbox)
        os.symlink(elsewhere, outbox)
    else:
        os.symlink(elsewhere, outbox / key_of(jobs[0]))
    _apply(ws, root)
    # The eject opened the outbox without following the link, so it rolled back: nothing left the workspace.
    assert os.listdir(elsewhere) == [] and len(_all_jobs(ws)) == 3
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is not None
    # No new return is requested while the outbox is a symlink, and the pre-check looks at nothing through it.
    (elsewhere / key_of(jobs[0])).mkdir()
    with cli_owner(ws) as owner:
        service = ExchangeService(ws, owner)
        service._backoff.clear()
        assert service.run() is False
    assert not list((ws.control / "requests").glob("*.json"))


def test_a_return_that_rolled_back_waits_out_a_doubling_backoff(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    jobs = _send(ws)
    _pass(ws)
    root = _indexed(ws, jobs[0]["id"])
    _finish(ws)
    occupant = exchange_directory(ws) / "outbox" / key_of(jobs[0])
    requests = ws.control / "requests"

    def rolled_back() -> None:
        occupant.mkdir()  # the outbox fills between the request and its application
        (occupant / "earlier").write_text("not fetched yet")
        _apply(ws, root)
        (occupant / "earlier").unlink()
        occupant.rmdir()
        assert _indexed(ws, jobs[0]["id"]).state == "succeeded"

    with cli_owner(ws) as owner:
        service = ExchangeService(ws, owner)

        def run_after(seconds: float) -> bool:
            posted, wait = service._backoff[root.job_id]
            service._backoff[root.job_id] = (posted - seconds, wait)
            service.invalidate()
            return service.run()

        assert service.run() is True
        rolled_back()
        assert service._backoff[root.job_id][1] == _exchange.STEP_INTERVAL
        # Within the backoff, no request: no second claim and rollback.
        assert run_after(_exchange.STEP_INTERVAL / 2) is False and not list(requests.glob("*.json"))
        assert run_after(_exchange.STEP_INTERVAL / 2) is True
        assert service._backoff[root.job_id][1] == 2 * _exchange.STEP_INTERVAL
        rolled_back()
        assert run_after(1.5 * _exchange.STEP_INTERVAL) is False and not list(requests.glob("*.json"))
        assert run_after(_exchange.STEP_INTERVAL) is True
        _apply(ws, root)
        assert (occupant / "bundle.json").is_file() and _all_jobs(ws) == []
        # Returned: the job's backoff is over.
        service.invalidate()
        assert service.run() is False and service._backoff == {}


# -- job control -------------------------------------------------------------------------------------------------------


def test_managers_leave_command_mailboxes_and_retired_authorization_setting_alone(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    ws.set_setting("exchange.authorized_keys", "unused stored value")
    exchange = exchange_directory(ws)
    request = exchange / "requests" / ("a" * 32 + ".json")
    response = exchange / "responses" / ("b" * 32 + ".json")
    request.write_bytes(b"untrusted command")
    response.write_bytes(b"signed result")
    assert _pass(ws) is False
    assert request.read_bytes() == b"untrusted command"
    assert response.read_bytes() == b"signed result"
    assert ws.read_settings()["exchange.authorized_keys"] == "unused stored value"
    assert not list((ws.control / "requests").glob("*.json"))


def test_the_pass_sweeps_old_write_temporaries_and_trash(tmp_path: Path) -> None:
    ws = _server(tmp_path / "ws")
    exchange = exchange_directory(ws)
    old = time.time() - 2 * _exchange.SWEEP_INTERVAL
    stale = [
        exchange / ".status.json.abcdefghijklmnop.tmp",
        exchange / "responses" / f".{'a' * 32}.json.abcdefghijklmnop.tmp",
    ]
    kept = [exchange / ".status.json.qrstuvwxyz234567.tmp", exchange / "inbox" / ".entry.abcdefghijklmnop.tmp"]
    for path in [*stale, *kept]:
        path.write_text("x")
    trash = exchange / "trash.abcdefghijklmnop"
    (trash / "bundle").mkdir(parents=True)
    (trash / "bundle" / "file").write_text("x")
    for path in [*stale, kept[1], trash]:
        os.utime(path, (old, old))
    with cli_owner(ws) as owner:
        service = ExchangeService(ws, owner)
        service.run()
        assert not any(path.exists() for path in stale) and all(path.exists() for path in kept)
        assert not [name for name in os.listdir(exchange) if name.startswith("trash.")]
        # At most once per interval.
        stale[0].write_text("x")
        os.utime(stale[0], (old, old))
        service.run()
        assert stale[0].exists()


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
    # The client reader accepts exactly what the managers write.
    endpoint = Endpoint(exchange_directory(ws), ws.workspace_id)
    assert read_passive_status(endpoint)["status"] == status


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
    bundle = exchange_directory(ws) / "outbox" / key_of(client_job)
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
