"""The exchange pass: inbox adoption, return of finished trees, status, recovery and the hostile exchange."""

import errno
import json
import logging
import os
import shutil
import stat
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity
from httk.workflow import (
    TaskManager,
    Workspace,
    _bundle,
    _confine,
    _exchange,
    _sealing,
    _txn,
    transfers,
    workflow_cli,
)
from httk.workflow._bundle import TRANSFER_DIRECTORY, TRANSFER_MANIFEST
from httk.workflow._exchange import ExchangeService
from httk.workflow._job_tree import record_spawns
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow._util import timestamp_seconds
from httk.workflow.errors import WorkspaceCorruptionError
from httk.workflow.introspection import ScopedWorkspace
from httk.workflow.manifests import workspace_maintenance_guard
from httk.workflow.models import STATE_KINDS, Marker, make_job_key
from httk.workflow.seals import seal_workspace, unseal_workspace
from test_eject_adopt import _payload
from test_sealing import _tree

#: A pass time well beyond the return grace period of jobs finished in a test.
_LATER = time.time() + 3600.0
_PINNED = {"manager.confine": "bwrap", "confine.bwrap": "/usr/bin/bwrap"}


class Crash(Exception):
    """A simulated process death at one protocol step."""


@pytest.fixture(autouse=True)
def _no_hook() -> Iterator[None]:
    yield
    _txn._HOOK = None


def _hook_at(step: str, action: Callable[[], object], occurrence: int = 1) -> None:
    seen = 0

    def hook(name: str) -> None:
        nonlocal seen
        if name != step:
            return
        seen += 1
        if seen == occurrence:
            _txn._HOOK = None
            action()

    _txn._HOOK = hook


def _crash() -> None:
    raise Crash()


@dataclass
class _Site:
    """A client workspace and a server workspace whose exchange it writes."""

    client: Workspace
    server: Workspace
    service: ExchangeService
    tmp: Path

    @property
    def exchange(self) -> Path:
        return self.server.root / "exchange"

    @property
    def inbox(self) -> Path:
        return self.exchange / "inbox"

    @property
    def outbox(self) -> Path:
        return self.exchange / "outbox"

    @property
    def rejected(self) -> Path:
        return self.outbox / "rejected"


def _service(workspace: Workspace) -> ExchangeService:
    return ExchangeService(workspace, owner=_txn.owner_token(str(uuid.uuid4())))


@pytest.fixture
def site(tmp_path: Path) -> _Site:
    configure_identity()
    client = Workspace.initialize(tmp_path / "client")
    server = Workspace.initialize(tmp_path / "server", extensions=["exchange"])
    server.set_policy({"visibility_deadline_seconds": 0.5})
    server = Workspace(server.root)
    return _Site(client, server, _service(server), tmp_path)


def _finish(workspace: Workspace, marker: Marker, kind: str = "succeeded") -> Marker:
    with workspace.open_journal_writer() as writer:
        return workspace.transition(writer, marker, kind, {"reason": "test"})


def _send(site: _Site, tag: str, *, kind: str | None = None, name: str | None = None) -> Marker:
    """Submit a job on the client and eject it into the server's exchange inbox, as a client does."""

    marker = site.client.submit(_payload(site.tmp / "payloads", tag), "jobs")
    if kind is not None:
        marker = _finish(site.client, marker, kind)
    transfers.eject_job(site.client, marker.job_id, site.inbox / (name or marker.job_key))
    return marker


def _loose(site: _Site, tag: str, *, kind: str | None = None) -> tuple[Marker, Path]:
    """Eject a client job to a free-standing directory outside every workspace."""

    marker = site.client.submit(_payload(site.tmp / "payloads", tag), "jobs")
    if kind is not None:
        marker = _finish(site.client, marker, kind)
    return marker, transfers.eject_job(site.client, marker.job_id, site.tmp / f"loose-{tag}")


def _edit_manifest(bundle: Path, edit: Callable[[dict[str, Any]], None]) -> None:
    path = bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    manifest = json.loads(path.read_text(encoding="utf-8"))
    edit(manifest)
    path.write_text(json.dumps(manifest), encoding="utf-8")


def _rejections(site: _Site) -> list[tuple[list[str], dict[str, Any]]]:
    """Return each ``rejected/<unique>``'s entries (besides ``reason.json``) and its reason document."""

    found = []
    for unique in sorted(site.rejected.iterdir()):
        entries = sorted(name for name in os.listdir(unique) if name != "reason.json")
        found.append((entries, json.loads((unique / "reason.json").read_text(encoding="utf-8"))))
    return found


def _status(site: _Site) -> dict[str, Any]:
    return json.loads((site.exchange / "status.json").read_text(encoding="utf-8"))


def _lineages(workspace: Workspace) -> list[Path]:
    return sorted((workspace.control / "tmp").glob("import.*"))


def _claims(workspace: Workspace) -> list[str]:
    directory = workspace.control / "transfers" / "adopting"
    return sorted(os.listdir(directory)) if directory.is_dir() else []


def _events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    return [record for record in caplog.records if getattr(record, "event", None) == name]


def _owners_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(_sealing, "_owner_gone", lambda *_args, **_kwargs: True)


def _present_once(workspace: Workspace, job_id: str) -> Marker:
    found = [marker for marker in workspace.scan_markers(STATE_KINDS) if marker.job_id == job_id]
    assert len(found) == 1, found
    return found[0]


# ---------------------------------------------------------------------------
# Adoption from the inbox
# ---------------------------------------------------------------------------


def test_an_inbox_bundle_is_adopted_with_exchange_origin_and_reported(site: _Site) -> None:
    marker = _send(site, "a")
    assert site.service.run(now=1000.0) is True
    adopted = _present_once(site.server, marker.job_id)
    state = site.server.read_state(adopted)
    assert adopted.kind == "submitted" and state["origin"] == "exchange"
    assert state["transfer"]["source_workspace_id"] == site.client.workspace_id
    assert os.listdir(site.inbox) == [] and _lineages(site.server) == [] and _claims(site.server) == []
    # Not finished, so not returned; the status document lists it.
    assert sorted(os.listdir(site.outbox)) == ["rejected"]
    status = _status(site)
    assert set(status) == {"format", "format_version", "workspace_id", "updated_at", "jobs", "truncated"}
    assert (status["format"], status["format_version"]) == ("httk-workspace-exchange-status", 1)
    assert status["workspace_id"] == site.server.workspace_id
    assert status["updated_at"] == "1970-01-01T00:16:40.000000Z" and status["truncated"] is False
    assert status["jobs"] == [{"job_id": marker.job_id, "job_key": marker.job_key, "state": "submitted"}]


def test_at_most_one_inbox_entry_is_adopted_per_pass(site: _Site) -> None:
    first, second = _send(site, "a"), _send(site, "b")
    site.service.run(now=1000.0)
    assert len(os.listdir(site.inbox)) == 1
    site.service.run(now=1001.0)
    assert os.listdir(site.inbox) == []
    for marker in (first, second):
        _present_once(site.server, marker.job_id)


def test_dot_reserved_and_ill_formed_names_are_ignored(site: _Site) -> None:
    names = [".partial", ".tmp-upload", "status.json", "rejected", "outbox", "inbox", "bad name", "-dash", "x" * 129]
    for name in names:
        (site.inbox / name).mkdir()
    assert site.service.run(now=1000.0) is False
    assert sorted(os.listdir(site.inbox)) == sorted(names) and os.listdir(site.rejected) == []


def test_a_refused_bundle_goes_to_a_fresh_rejected_directory_and_never_back(site: _Site) -> None:
    for _ in range(2):
        (site.inbox / "broken").mkdir()
        (site.inbox / "broken" / "junk").write_text("not a job", encoding="utf-8")
        site.service.run(now=1000.0)
        assert not os.path.lexists(site.inbox / "broken")
    rejections = _rejections(site)
    assert [entries for entries, _reason in rejections] == [["broken"], ["broken"]]
    for _entries, reason in rejections:
        assert set(reason) == {"format", "format_version", "name", "reason", "rejected_at"}
        assert (reason["format"], reason["format_version"], reason["name"]) == (
            "httk-workspace-exchange-rejection",
            1,
            "broken",
        )
        assert reason["reason"]
    for unique in site.rejected.iterdir():
        assert (unique / "broken" / "junk").read_text(encoding="utf-8") == "not a job"
    # The client fixes it and sends a good job under the same name.
    marker = _send(site, "fixed", name="broken")
    site.service.run(now=1001.0)
    _present_once(site.server, marker.job_id)
    assert _lineages(site.server) == []


def test_non_directory_inbox_entries_are_rejected_without_being_followed(site: _Site) -> None:
    target = site.tmp / "target"
    target.mkdir()
    (target / "keep").write_text("keep", encoding="utf-8")
    (site.inbox / "link").symlink_to(target)
    (site.inbox / "file").write_text("not a job", encoding="utf-8")
    os.mkfifo(site.inbox / "fifo")
    for index in range(3):
        site.service.run(now=1000.0 + index)
    assert os.listdir(site.inbox) == []
    entries = sorted(name for names, _reason in _rejections(site) for name in names)
    assert entries == ["fifo", "file", "link"]
    for unique in site.rejected.iterdir():
        for name in os.listdir(unique):
            mode = os.lstat(unique / name).st_mode
            if name == "link":
                assert stat.S_ISLNK(mode)
            elif name == "fifo":
                assert stat.S_ISFIFO(mode)
    assert os.listdir(target) == ["keep"] and (target / "keep").read_text(encoding="utf-8") == "keep"


def _forge(site: _Site, case: str) -> tuple[str, Path]:
    """Build one hostile bundle; return the job id it would publish and the bundle."""

    if case == "parent":
        local = site.server.submit(_payload(site.tmp / "payloads", "local"), "jobs")
        payload = _payload(site.tmp / "payloads", "orphan")
        document = json.loads((payload / "job.json").read_text(encoding="utf-8"))
        document["parent"] = {"job_id": local.job_id, "job_key": local.job_key, "placement": "jobs"}
        (payload / "job.json").write_text(json.dumps(document), encoding="utf-8")
        marker = site.client.submit(payload, "jobs")
        return marker.job_id, transfers.eject_job(site.client, marker.job_id, site.tmp / "loose-parent")
    if case == "member_placement":
        root, _members = _tree(site.client, site.tmp / "tree")
        bundle = transfers.eject_job(site.client, root.job_id, site.tmp / "loose-tree")
        _edit_manifest(bundle, lambda manifest: manifest["members"][0].update(placement="elsewhere"))
        return root.job_id, bundle
    marker, bundle = _loose(site, case, kind="paused")
    envelope = bundle / TRANSFER_DIRECTORY
    if case == "job_key":
        _edit_manifest(bundle, lambda manifest: manifest.update(job_key=make_job_key(str(uuid.uuid4()), "forged")))
    elif case == "extra_marker":
        (envelope / "markers" / str(uuid.uuid4())).write_bytes(b"")
    elif case == "missing_marker":
        (envelope / "markers" / marker.job_id).unlink()
    elif case == "hard_link":
        os.link(bundle / "files" / "runner", site.tmp / "outside-link")
    elif case == "fifo":
        os.mkfifo(bundle / "files" / "pipe")
    elif case == "escaping_symlink":
        (bundle / "files" / "escape").symlink_to("/etc/passwd")
    elif case == "join":
        local = site.server.submit(_payload(site.tmp / "payloads", "joined"), "jobs")

        def join(manifest: dict[str, Any]) -> None:
            child = {"job_id": local.job_id, "job_key": local.job_key, "workspace_id": site.server.workspace_id}
            manifest["prior_kind"] = "waiting"
            manifest["prior_state"] = {**manifest["prior_state"], "join": {"children": [child]}}

        _edit_manifest(bundle, join)
    else:
        raise AssertionError(case)
    return marker.job_id, bundle


@pytest.mark.parametrize(
    "case",
    [
        "job_key",
        "member_placement",
        "extra_marker",
        "missing_marker",
        "hard_link",
        "fifo",
        "escaping_symlink",
        "join",
        "parent",
    ],
)
def test_a_forged_bundle_is_rejected_and_publishes_nothing(site: _Site, case: str) -> None:
    job_id, bundle = _forge(site, case)
    before = {marker.job_id for marker in site.server.scan_markers(STATE_KINDS)}
    os.rename(bundle, site.inbox / "forged")
    site.service.run(now=1000.0)
    assert os.listdir(site.inbox) == []
    [(entries, reason)] = _rejections(site)
    assert entries == ["forged"] and reason["name"] == "forged" and reason["reason"]
    assert {marker.job_id for marker in site.server.scan_markers(STATE_KINDS)} == before
    assert job_id not in before
    assert _lineages(site.server) == [] and _claims(site.server) == []
    assert not [path for path in site.server.jobs.rglob("*") if path.name.endswith(job_id)]


def test_client_prior_state_is_filtered_and_provenance_is_never_taken_from_it(site: _Site) -> None:
    marker = site.client.submit(_payload(site.tmp / "payloads", "paused"), "jobs")
    with site.client.open_journal_writer() as writer:
        marker = site.client.transition(writer, marker, "paused", {"reason": "operator_pause", "invented": "x"})
    bundle = transfers.eject_job(site.client, marker.job_id, site.tmp / "loose")
    transfer_id = str(transfers.validate_bundle(bundle)["transfer_id"])

    def forge(manifest: dict[str, Any]) -> None:
        manifest["prior_state"] = {
            **manifest["prior_state"],
            "transfer": {"transfer_id": str(uuid.uuid4())},
            "origin": "client",
            "outgoing": {"transfer_id": str(uuid.uuid4()), "role": "root"},
            "prior_kind": "running",
            "prior_state": {},
        }

    _edit_manifest(bundle, forge)
    os.rename(bundle, site.inbox / marker.job_key)
    site.service.run(now=1000.0)
    adopted = _present_once(site.server, marker.job_id)
    state = site.server.read_state(adopted)
    assert adopted.kind == "paused" and state["reason"] == "operator_pause"
    assert state["origin"] == "exchange" and state["transfer"]["transfer_id"] == transfer_id
    for member in ("invented", "outgoing", "prior_kind", "prior_state"):
        assert member not in state


# ---------------------------------------------------------------------------
# The hostile exchange directories
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("hostile", ["exchange", "inbox", "outbox", "rejected"])
def test_a_symlinked_exchange_directory_stops_the_pass(
    site: _Site, hostile: str, caplog: pytest.LogCaptureFixture
) -> None:
    finished = _send(site, "finished", kind="succeeded")
    site.service.run(now=1000.0)
    _present_once(site.server, finished.job_id)
    pending = _send(site, "pending")
    (site.inbox / "broken").mkdir()
    real = {
        "exchange": site.exchange,
        "inbox": site.inbox,
        "outbox": site.outbox,
        "rejected": site.rejected,
    }[hostile]
    aside = site.tmp / "aside"
    os.rename(real, aside)
    real.symlink_to(aside)
    before = sorted(str(path.relative_to(aside)) for path in aside.rglob("*"))
    service = _service(site.server)
    with caplog.at_level(logging.WARNING, logger="httk.workflow._exchange"):
        assert service.run(now=_LATER) is False
        assert service.run(now=_LATER + 20) is False
    assert [event.getMessage() for event in _events(caplog, "exchange_unavailable")]
    assert "not a real directory" in _events(caplog, "exchange_unavailable")[0].getMessage()
    assert len(_events(caplog, "exchange_unavailable")) == 1  # reported once while it persists
    # Nothing moved: no adoption, no rejection, no return, no status, no write through the link.
    assert sorted(str(path.relative_to(aside)) for path in aside.rglob("*")) == before
    assert site.server.find_marker_by_id(pending.job_id) is None
    _present_once(site.server, finished.job_id)
    assert _lineages(site.server) == []
    real.unlink()
    os.rename(aside, real)
    for index in range(3):
        site.service.run(now=_LATER + 40 + 10 * index)
    _present_once(site.server, pending.job_id)
    assert site.server.find_marker_by_id(finished.job_id) is None
    assert (site.outbox / finished.job_key).is_dir()


def test_a_removed_inbox_or_rejected_directory_is_recreated(site: _Site) -> None:
    shutil.rmtree(site.inbox)
    shutil.rmtree(site.rejected)
    site.service.run(now=1000.0)
    assert site.inbox.is_dir() and site.rejected.is_dir()


def test_a_rejected_directory_swapped_for_a_symlink_is_never_written_through(site: _Site) -> None:
    elsewhere = site.tmp / "elsewhere"
    elsewhere.mkdir()

    def swap() -> None:
        [fresh] = os.listdir(site.rejected)
        os.rename(site.rejected / fresh, site.tmp / "taken")
        (site.rejected / fresh).symlink_to(elsewhere)

    (site.inbox / "broken").mkdir()
    _hook_at("V10.rejected_dir", swap)
    site.service.run(now=1000.0)
    assert os.listdir(elsewhere) == [] and os.listdir(site.tmp / "taken") == []
    real = [entry for entry in site.rejected.iterdir() if not entry.is_symlink()]
    assert len(real) == 1 and sorted(os.listdir(real[0])) == ["broken", "reason.json"]
    assert _lineages(site.server) == []


def test_a_symlink_at_status_json_is_replaced_not_followed(site: _Site, caplog: pytest.LogCaptureFixture) -> None:
    outside = site.tmp / "outside.json"
    outside.write_text("untouched", encoding="utf-8")
    (site.exchange / "status.json").symlink_to(outside)
    site.service.run(now=1000.0)
    assert not (site.exchange / "status.json").is_symlink() and _status(site)["jobs"] == []
    assert outside.read_text(encoding="utf-8") == "untouched"
    # A directory there refuses the install: reported, nothing else breaks.
    (site.exchange / "status.json").unlink()
    (site.exchange / "status.json").mkdir()
    marker = _send(site, "a")
    with caplog.at_level(logging.ERROR, logger="httk.workflow._exchange"):
        site.service.run(now=1010.0)
    assert _events(caplog, "exchange_step_failed")
    _present_once(site.server, marker.job_id)
    assert sorted(name for name in os.listdir(site.exchange) if name.startswith(".")) == []


def test_the_status_document_is_rate_limited_and_bounded(site: _Site, monkeypatch: pytest.MonkeyPatch) -> None:
    markers = sorted((site.server.submit(_payload(site.tmp / "p", f"j{i}"), "jobs") for i in range(4)), key=str)
    site.service.run(now=1000.0)
    path = site.exchange / "status.json"
    assert len(_status(site)["jobs"]) == 4
    path.unlink()
    site.service.run(now=1009.0)
    assert not path.exists()
    site.service.run(now=1010.0)
    assert _status(site)["updated_at"] == "1970-01-01T00:16:50.000000Z"
    monkeypatch.setattr(_exchange, "STATUS_LIMIT", 600)
    site.service.run(now=1020.0)
    status = _status(site)
    keys = sorted(marker.job_key for marker in markers)
    assert status["truncated"] is True and path.stat().st_size <= 600
    assert 0 < len(status["jobs"]) < 4 and [job["job_key"] for job in status["jobs"]] == keys[: len(status["jobs"])]


# ---------------------------------------------------------------------------
# Returning finished exchange-origin trees
# ---------------------------------------------------------------------------


def test_only_finished_exchange_jobs_return_and_only_after_the_grace_period(site: _Site) -> None:
    local = _finish(site.server, site.server.submit(_payload(site.tmp / "p", "local"), "jobs"))
    running = _send(site, "todo")
    site.service.run(now=1000.0)
    sent = _send(site, "sent")
    site.service.run(now=1001.0)
    adopted = _present_once(site.server, sent.job_id)
    finished = _finish(site.server, adopted, "failed")
    at = timestamp_seconds(str(site.server.read_state(finished)["created_at"]))
    site.service.run(now=at + 30)
    assert site.server.find_marker_by_id(sent.job_id) is not None
    site.service.run(now=at + 61)
    assert site.server.find_marker_by_id(sent.job_id) is None
    manifest = transfers.validate_bundle(site.outbox / sent.job_key)
    assert (manifest["job_id"], manifest["prior_kind"]) == (sent.job_id, "failed")
    for _ in range(3):
        site.service.run(now=_LATER + _ * 10)
    # A local job and an unfinished exchange job stay.
    _present_once(site.server, local.job_id)
    assert _present_once(site.server, running.job_id).kind == "submitted"
    assert sorted(os.listdir(site.outbox)) == sorted(["rejected", sent.job_key])
    assert not list(site.server.scan_markers(("transferring",)))


def test_an_exchange_tree_returns_whole_and_the_client_adopts_it_back(site: _Site) -> None:
    root, members = _tree(site.client, site.tmp / "tree")
    transfers.eject_job(site.client, root.job_id, site.inbox / "tree")
    site.service.run(now=1000.0)
    for job in (root, *members):
        assert site.server.read_state(_present_once(site.server, job.job_id))["origin"] == "exchange"
    site.service.run(now=_LATER)
    for job in (root, *members):
        assert site.server.find_marker_by_id(job.job_id) is None
    returned = site.outbox / root.job_key
    assert {member["job_id"] for member in transfers.validate_bundle(returned)["members"]} == {
        member.job_id for member in members
    }
    adopted = transfers.adopt_job(site.client, returned)
    assert adopted.job_id == root.job_id and not returned.exists()
    for job in (root, *members):
        state = site.client.read_state(_present_once(site.client, job.job_id))
        assert "origin" not in state


@pytest.mark.timing
def test_a_recently_finished_member_holds_its_tree_back(site: _Site) -> None:
    root, members = _tree(site.client, site.tmp / "tree")
    transfers.eject_job(site.client, root.job_id, site.inbox / "tree")
    now = time.time()
    site.service.run(now=now)
    # The members finished on the client, but the grace counts from their last frame here.
    site.service.run(now=now + 30)
    assert site.server.find_marker_by_id(root.job_id) is not None
    site.service.run(now=now + 70)
    assert site.server.find_marker_by_id(root.job_id) is None
    for member in members:
        assert site.server.find_marker_by_id(member.job_id) is None


def test_a_finished_root_with_a_live_bound_child_is_pending(site: _Site) -> None:
    sent = _send(site, "root")
    site.service.run(now=1000.0)
    root = _present_once(site.server, sent.job_id)
    child_payload = _payload(site.tmp / "payloads", "kid")
    definition = json.loads((child_payload / "job.json").read_text(encoding="utf-8"))
    definition["parent"] = {"job_id": root.job_id, "job_key": root.job_key, "placement": "jobs", "spawn_id": None}
    (child_payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
    record_spawns(
        site.server.payload_path(root.placement, root.job_key),
        str(uuid.uuid4()),
        [{"job_key": make_job_key(definition["id"], "kid"), "label": "kid", "placement": "jobs/kids"}],
        durable=False,
    )
    child = site.server.submit(child_payload, "jobs/kids")
    # Registered here as the exchange root's child, its first frame inherits the root's origin.
    with site.server.open_journal_writer() as writer:
        child = site.server.transition(writer, child, "ready", {"reason": "submitted"})
    assert site.server.read_state(child)["origin"] == "exchange"
    _finish(site.server, root)
    site.service.run(now=_LATER)
    _present_once(site.server, root.job_id)
    _present_once(site.server, child.job_id)
    assert sorted(os.listdir(site.outbox)) == ["rejected"]
    # Once the child ends too, the whole tree leaves as one bundle, the locally spawned child inside it.
    _finish(site.server, site.server.find_marker_by_id(child.job_id) or child, "cancelled")
    site.service.run(now=_LATER + 10)
    assert site.server.find_marker_by_id(root.job_id) is None and site.server.find_marker_by_id(child.job_id) is None
    [member] = transfers.validate_bundle(site.outbox / root.job_key)["members"]
    assert member["job_id"] == child.job_id


@pytest.mark.parametrize("occupant", ["full", "empty_file", "symlink"])
def test_an_occupied_outbox_name_is_skipped_silently_and_retried(
    site: _Site, occupant: str, caplog: pytest.LogCaptureFixture
) -> None:
    sent = _send(site, "done", kind="succeeded")
    site.service.run(now=1000.0)
    target = site.outbox / sent.job_key
    if occupant == "full":
        (target / "previous").mkdir(parents=True)
    elif occupant == "empty_file":
        target.write_bytes(b"")
    else:
        target.symlink_to(site.tmp)
    with caplog.at_level(logging.INFO, logger="httk.workflow"):
        site.service.run(now=_LATER)
        site.service.run(now=_LATER + 10)
    _present_once(site.server, sent.job_id)
    assert not _events(caplog, "exchange_return_failed") and not _events(caplog, "exchange_step_failed")
    assert not list(site.server.scan_markers(("transferring",)))
    # The client fetches the earlier copy; the next scan returns the job.
    if occupant == "full":
        shutil.rmtree(target)
    else:
        target.unlink()
    site.service.run(now=_LATER + 20)
    assert site.server.find_marker_by_id(sent.job_id) is None
    assert transfers.validate_bundle(target)["job_id"] == sent.job_id


def test_a_return_error_goes_to_the_manager_log_and_is_retried(
    site: _Site, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    sent = _send(site, "done", kind="succeeded")
    site.service.run(now=1000.0)

    def broken(*_args: object, **_kwargs: object) -> Path:
        raise OSError("disk on fire")

    monkeypatch.setattr(_exchange, "eject_job", broken)
    with caplog.at_level(logging.WARNING, logger="httk.workflow._exchange"):
        site.service.run(now=_LATER)
        site.service.run(now=_LATER + 10)
    [event] = _events(caplog, "exchange_return_failed")
    assert "disk on fire" in event.getMessage()
    monkeypatch.undo()
    site.service.run(now=_LATER + 20)
    assert site.server.find_marker_by_id(sent.job_id) is None and (site.outbox / sent.job_key).is_dir()


# ---------------------------------------------------------------------------
# Recovery, takeover and concurrent managers
# ---------------------------------------------------------------------------


def test_recovery_runs_at_most_every_step_interval(site: _Site, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    recover = _exchange.recover_lineages

    def counted(*args: Any, **kwargs: Any) -> list[dict[str, object]]:
        calls.append(kwargs.get("exchange"))
        return recover(*args, **kwargs)

    monkeypatch.setattr(_exchange, "recover_lineages", counted)
    for now in (1000.0, 1001.0, 1009.9, 1010.0, 1015.0, 1020.0):
        site.service.run(now=now)
    assert len(calls) == 3 and all(isinstance(item, _exchange.ExchangeDescriptors) for item in calls)


def test_an_interrupted_return_is_settled_by_recovery_after_the_client_fetched_it(
    site: _Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent = _send(site, "done", kind="succeeded")
    site.service.run(now=1000.0)
    _hook_at("S7.cleanup", _crash)
    site.service.run(now=_LATER)
    returned = site.outbox / sent.job_key
    assert transfers.validate_bundle(returned)["job_id"] == sent.job_id
    assert [marker.job_id for marker in site.server.scan_markers(("transferring",))] == [sent.job_id]
    # The client fetches the bundle before any recovery runs; client territory is never inspected.
    fetched = site.tmp / "fetched"
    os.rename(returned, fetched)
    _owners_gone(monkeypatch)
    _service(site.server).run(now=_LATER + 10)
    assert not list(site.server.scan_markers(("transferring",))) and site.server.find_marker_by_id(sent.job_id) is None
    assert transfers.validate_bundle(fetched)["job_id"] == sent.job_id


@pytest.mark.parametrize("bundle", ["tampered", "good"])
def test_an_exchange_lineage_is_taken_over_only_with_the_exchange_descriptors(
    site: _Site, bundle: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker, loose = _loose(site, "taken")
    if bundle == "tampered":
        (loose / "files" / "runner").write_text("tampered", encoding="utf-8")
    os.rename(loose, site.inbox / "entry")
    _hook_at("V2.claimed", _crash)  # after the claim, before verification
    site.service.run(now=1000.0)  # the step's crash is logged, the lineage stays
    [lineage] = _lineages(site.server)
    assert os.listdir(site.inbox) == []
    _owners_gone(monkeypatch)
    # Recovery by an actor without the exchange's descriptors (a CLI) leaves it alone:
    # it never renames a bundle back into client territory by path.
    transfers.recover_transfers(site.server)
    assert _lineages(site.server) == [lineage] and os.listdir(site.inbox) == []
    # An exchange pass that cannot open the exchange leaves it too.
    os.rename(site.rejected, site.tmp / "rejected-aside")
    site.rejected.symlink_to(site.tmp / "rejected-aside")
    takeover = _service(site.server)
    takeover.run(now=2000.0)
    assert _lineages(site.server) == [lineage]
    site.rejected.unlink()
    os.rename(site.tmp / "rejected-aside", site.rejected)
    takeover.run(now=2010.0)
    assert _lineages(site.server) == [] and os.listdir(site.inbox) == []
    if bundle == "tampered":
        [(entries, reason)] = _rejections(site)
        assert entries == ["entry"] and "digest" in reason["reason"].lower()
        assert site.server.find_marker_by_id(marker.job_id) is None
    else:
        assert site.server.read_state(_present_once(site.server, marker.job_id))["origin"] == "exchange"
        assert os.listdir(site.rejected) == []


def test_an_own_waiting_lineage_is_resumed_by_a_later_pass(site: _Site, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: False)
    sent = _send(site, "waits")
    claims = site.server.control / "transfers" / "adopting"
    claims.mkdir(parents=True, exist_ok=True)
    (claims / sent.job_id).write_text("another lineage's claim", encoding="utf-8")
    site.service.run(now=1000.0)
    [lineage] = _lineages(site.server)
    assert site.service.owner in lineage.name and site.server.find_marker_by_id(sent.job_id) is None
    (claims / sent.job_id).unlink()
    site.service.run(now=1005.0)  # recovery is not due yet
    assert _lineages(site.server) == [lineage]
    site.service.run(now=1010.0)
    _present_once(site.server, sent.job_id)
    assert _lineages(site.server) == [] and _claims(site.server) == []


@pytest.mark.parametrize("step", ["V1.staged", "V2.claimed", "V9.payload"])
def test_two_managers_never_adopt_one_inbox_entry_twice(site: _Site, step: str) -> None:
    sent = _send(site, "contested")
    other = _service(site.server)
    _hook_at(step, lambda: other.run(now=1000.0))
    site.service.run(now=1000.0)
    _present_once(site.server, sent.job_id)
    assert os.listdir(site.inbox) == [] and os.listdir(site.rejected) == []
    assert _lineages(site.server) == [] and _claims(site.server) == []


def test_a_sealed_or_maintenance_locked_workspace_keeps_bundles_waiting(
    site: _Site, caplog: pytest.LogCaptureFixture
) -> None:
    sent = _send(site, "waits")
    seal_workspace(site.server)
    with caplog.at_level(logging.INFO, logger="httk.workflow._exchange"):
        assert site.service.run(now=1000.0) is False
        assert site.service.outstanding(now=1000.0) == 0
    assert _events(caplog, "exchange_paused") and os.listdir(site.inbox) == [sent.job_key]
    assert os.listdir(site.rejected) == [] and not (site.exchange / "status.json").exists()
    unseal_workspace(site.server)
    with workspace_maintenance_guard(site.server):
        assert site.service.run(now=1001.0) is False
    assert os.listdir(site.inbox) == [sent.job_key]
    site.service.run(now=1002.0)
    _present_once(site.server, sent.job_id)


# ---------------------------------------------------------------------------
# Crash injection through the pass
# ---------------------------------------------------------------------------

_PASS_STEPS = (
    "V1.staged",
    "V2.claimed",
    "V4.verified",
    "V6.claimed",
    "V8.envelope",
    "V9.marker",
    "V11.release",
    "S1.born",
    "S2.member",
    "S3.fenced",
    "S4.member",
    "S5.detach",
    "S6.commit",
    "S7.marker",
    "S7.trash",
)
_NORMAL = {"V2.claimed", "V8.envelope", "V11.release", "S3.fenced", "S5.detach", "S7.marker"}


@pytest.mark.parametrize(
    "step", [pytest.param(step, marks=() if step in _NORMAL else pytest.mark.extended) for step in _PASS_STEPS]
)
def test_a_pass_crashed_at_any_step_is_finished_by_the_next_manager(
    site: _Site, step: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, members = _tree(site.client, site.tmp / "tree")
    transfers.eject_job(site.client, root.job_id, site.inbox / "tree")
    jobs = [root, *members]
    if step.startswith("S"):
        site.service.run(now=1000.0)
    _hook_at(step, _crash)
    site.service.run(now=_LATER)  # the crash is logged by the pass, never raised
    _owners_gone(monkeypatch)
    successor = _service(site.server)
    for index in range(4):
        successor.run(now=_LATER + 10 * (index + 1))
    # The successor finishes the adoption or the return, and then the return: exactly once.
    returned = site.outbox / root.job_key
    manifest = transfers.validate_bundle(returned)
    assert {member["job_id"] for member in manifest["members"]} == {member.job_id for member in members}
    for job in jobs:
        assert site.server.find_marker_by_id(job.job_id) is None
    assert os.listdir(site.inbox) == [] and os.listdir(site.rejected) == []
    assert not list(site.server.scan_markers(("transferring",)))
    assert _lineages(site.server) == [] and _claims(site.server) == []
    leftovers = [name for name in os.listdir(site.server.control / "tmp") if not name.startswith("trash.")]
    # A transaction directory no marker names (born, or emptied, around a crash)
    # is left to the orphan sweep, which takes it after 24 hours.
    assert all(name.startswith("eject.") for name in leftovers), leftovers
    for _ in range(2):
        _sealing.recover(site.server, now=time.time_ns() + 25 * 3600 * 10**9)
    assert [name for name in os.listdir(site.server.control / "tmp") if not name.startswith("trash.")] == []
    assert sorted(os.listdir(site.outbox)) == sorted(["rejected", root.job_key])


# ---------------------------------------------------------------------------
# Managers: who serves, until-idle, the round trip
# ---------------------------------------------------------------------------


def test_outstanding_counts_what_keeps_an_until_idle_manager_running(site: _Site) -> None:
    service = site.service
    assert service.outstanding() == 0
    sent = _send(site, "done", kind="succeeded")
    assert service.outstanding() == 1  # an eligible inbox entry
    service.run()
    assert service.outstanding() == 1  # finished here inside its grace period
    assert service.outstanding(now=_LATER) == 1  # ready to return
    (site.outbox / sent.job_key / "previous").mkdir(parents=True)
    assert service.outstanding(now=_LATER) == 0  # waiting for the client to fetch an earlier copy
    shutil.rmtree(site.outbox / sent.job_key)
    lineage = site.server.control / "tmp" / f"import.{service.owner}.1.{'0' * 16}"
    lineage.mkdir()
    assert service.outstanding(now=_LATER) == 2
    lineage.rmdir()
    _hook_at("S3.fenced", _crash)
    service.run(now=_LATER)
    assert [marker.job_id for marker in site.server.scan_markers(("transferring",))] == [sent.job_id]
    assert service.outstanding(now=_LATER) == 1  # an ejection in flight
    # An addressed transfer waiting for its acknowledgement is not exchange work.
    local = site.server.submit(_payload(site.tmp / "payloads", "addressed"), "jobs")
    transfers.detach_job(site.server, local.job_id, destination_workspace_id=site.client.workspace_id)
    assert service.outstanding(now=_LATER) == 1


@pytest.fixture
def confined(monkeypatch: pytest.MonkeyPatch) -> list[Mapping[str, Any]]:
    """Replace the attempt sandbox with a pass-through and record every confined attempt."""

    prepared: list[Mapping[str, Any]] = []

    def prepare(settings: _confine.ConfineSettings, **arguments: Any) -> PreparedSandbox:
        prepared.append({"settings": settings, **arguments})
        return PreparedSandbox(["/usr/bin/env", "--"], ())

    monkeypatch.setattr(_confine, "probe_bwrap", lambda _settings: True)
    monkeypatch.setattr(_confine, "prepare_attempt_sandbox", prepare)
    return prepared


def test_only_unrestricted_managers_serve_the_exchange(site: _Site, confined: list[Mapping[str, Any]]) -> None:
    def served(**options: Any) -> bool:
        with TaskManager(site.server, setting_overrides=_PINNED, **options) as manager:
            return manager._exchange is not None and manager._serving_exchange()

    assert served()
    assert served(accept_any_pool=True)
    assert served(pools=["default", "gpu"])
    assert not served(pools=["gpu"])
    assert not served(placement_prefixes=["jobs"])
    assert served(placement_prefixes=[""])  # the empty placement is no restriction
    with TaskManager(ScopedWorkspace(site.server.root, set()), setting_overrides=_PINNED) as manager:
        assert manager._exchange is None


def test_a_manager_whose_confinement_is_switched_off_stops_serving(
    site: _Site, confined: list[Mapping[str, Any]]
) -> None:
    site.server.set_setting("manager.confine", "bwrap")
    site.server.set_setting("confine.bwrap", "/usr/bin/bwrap")
    with TaskManager(Workspace(site.server.root), heartbeat_interval=0.01) as manager:
        site.server.set_setting("manager.confine", "none")
        sent = _send(site, "held")
        for _ in range(3):
            manager.tick()
        assert os.listdir(site.inbox) == [sent.job_key]
        assert manager._work_census().exchange == 0


@pytest.mark.timing
def test_an_until_idle_manager_adopts_runs_and_returns_before_it_exits(
    site: _Site, confined: list[Mapping[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_exchange, "RETURN_GRACE_SECONDS", 1.0)
    monkeypatch.setattr(_exchange, "STEP_INTERVAL", 0.0)
    sent = _send(site, "trip")
    with TaskManager(Workspace(site.server.root), heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        census = manager.run_until_idle(timeout=120.0, poll_interval=0.05)
    assert census.exchange == 0
    assert [call["settings"].mode for call in confined] == ["bwrap"]
    returned = site.outbox / sent.job_key
    manifest = transfers.validate_bundle(returned)
    assert (manifest["job_id"], manifest["prior_kind"]) == (sent.job_id, "succeeded")
    assert site.server.find_marker_by_id(sent.job_id) is None
    adopted = transfers.adopt_job(site.client, returned)
    assert adopted.kind == "succeeded" and not returned.exists()


def test_the_exchange_flag_is_gone_from_the_manager_command(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    with pytest.raises(SystemExit):
        parser.parse_args(["manager", "run", "--exchange"])


# ---------------------------------------------------------------------------
# Review findings (October 6 2026): local adoption, damaged frames, the census
# cache, local orphans, inbox commits, rejection cleanup
# ---------------------------------------------------------------------------


def test_a_users_adoption_of_an_outbox_entry_is_claimed_by_descriptor_and_checked_as_client_content(
    site: _Site,
) -> None:
    marker = site.server.submit(_payload(site.tmp / "payloads", "mine"), "jobs")
    with site.server.open_journal_writer() as writer:
        marker = site.server.transition(writer, marker, "paused", {"reason": "operator_pause", "invented": "x"})
    entry = transfers.eject_job(site.server, marker.job_id, site.outbox)
    adopted = transfers.adopt_job(site.server, entry)
    state = site.server.read_state(adopted)
    assert adopted.job_id == marker.job_id and not entry.exists()
    # Client content: the exchange allowlist applies, but no exchange origin is recorded.
    assert "invented" not in state and "origin" not in state and state["reason"] == "operator_pause"


def test_a_refused_outbox_entry_is_quarantined_never_put_back_by_path(site: _Site) -> None:
    marker, loose = _loose(site, "tampered")
    (loose / "files" / "runner").write_text("tampered", encoding="utf-8")
    entry = site.outbox / marker.job_key
    os.rename(loose, entry)
    with pytest.raises(_exchange.FormatError):
        transfers.adopt_job(site.server, entry)
    assert not os.path.lexists(entry) and sorted(os.listdir(site.outbox)) == ["rejected"]
    [quarantined] = list((site.server.control / "quarantine").iterdir())
    assert (quarantined / "entry" / "bundle" / "job.json").is_file()
    assert _lineages(site.server) == []


def test_an_outbox_adoption_through_a_symlinked_outbox_is_refused(site: _Site) -> None:
    marker, loose = _loose(site, "elsewhere")
    aside = site.tmp / "aside"
    os.rename(site.outbox, aside)
    site.outbox.symlink_to(aside)
    os.rename(loose, aside / marker.job_key)
    with pytest.raises(ValueError, match="not a real directory"):
        transfers.adopt_job(site.server, site.outbox / marker.job_key)
    assert (aside / marker.job_key / "job.json").is_file() and site.server.find_marker_by_id(marker.job_id) is None


def test_a_recovered_outbox_lineage_is_never_renamed_back_into_client_territory(
    site: _Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker, loose = _loose(site, "crashed")
    (loose / "files" / "runner").write_text("tampered", encoding="utf-8")
    entry = site.outbox / marker.job_key
    os.rename(loose, entry)
    _hook_at("V2.claimed", _crash)
    with pytest.raises(Crash):
        transfers.adopt_job(site.server, entry)
    assert len(_lineages(site.server)) == 1
    _owners_gone(monkeypatch)
    _service(site.server).run(now=1000.0)  # a manager's recovery takes it over and refuses it
    assert not os.path.lexists(entry) and _lineages(site.server) == []
    assert any((site.server.control / "quarantine").iterdir())


def test_a_damaged_waiting_frame_neither_crashes_the_census_nor_stops_other_returns(
    site: _Site, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    sent = _send(site, "done", kind="succeeded")
    site.service.run(now=1000.0)
    local = site.server.submit(_payload(site.tmp / "payloads", "waits"), "jobs")
    with site.server.open_journal_writer() as writer:
        waiting = site.server.transition(writer, local, "waiting", {"reason": "test", "join": {"children": []}})
    read_state = site.server.read_state

    def damaged(marker: Marker) -> dict[str, Any]:
        if marker.job_id == waiting.job_id:
            raise WorkspaceCorruptionError(f"the frame of {marker.job_key} is damaged")
        return read_state(marker)

    monkeypatch.setattr(site.server, "read_state", damaged)
    with caplog.at_level(logging.WARNING, logger="httk.workflow._exchange"):
        assert site.service.outstanding(now=_LATER) == 1
        site.service.run(now=_LATER)
        site.service.run(now=_LATER + 10)
    assert site.server.find_marker_by_id(sent.job_id) is None and (site.outbox / sent.job_key).is_dir()
    assert len(_events(caplog, "exchange_waiting_unreadable")) == 1
    assert not _events(caplog, "exchange_step_failed")


def test_the_census_reuses_its_scan_until_something_changes(site: _Site, monkeypatch: pytest.MonkeyPatch) -> None:
    for tag in ("a", "b"):
        _finish(site.server, site.server.submit(_payload(site.tmp / "payloads", tag), "jobs"))
    scans: list[float] = []
    scan = ExchangeService._scan

    def counted(self: ExchangeService, now: float) -> None:
        scans.append(now)
        scan(self, now)

    monkeypatch.setattr(ExchangeService, "_scan", counted)
    service = site.service
    for offset in range(5):
        service.outstanding(now=1000.0 + offset)
    assert scans == [1000.0]
    service.outstanding(now=1010.0)
    assert scans == [1000.0, 1010.0]
    # A change (a tick that changed state) makes the next census scan afresh: no premature exit.
    sent = _send(site, "late", kind="succeeded")
    service.run(now=1011.0)
    assert service.outstanding(now=1012.0) == 1 and scans[-1] == 1012.0
    assert _present_once(site.server, sent.job_id)


@contextmanager
def caplog_events(site: _Site) -> Iterator[list[str]]:
    """Collect the messages the exchange module logs meanwhile."""

    messages: list[str] = []

    class Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    handler = Collect(level=logging.INFO)
    logger = logging.getLogger("httk.workflow._exchange")
    logger.addHandler(handler)
    try:
        yield messages
    finally:
        logger.removeHandler(handler)


def test_a_local_orphan_named_by_a_client_tree_never_leaves(site: _Site, monkeypatch: pytest.MonkeyPatch) -> None:
    parent_id = str(uuid.uuid4())
    parent_key = make_job_key(parent_id, "claimed-parent")
    # A local job L whose parent P is absent here.
    orphan_payload = _payload(site.tmp / "payloads", "orphan")
    definition = json.loads((orphan_payload / "job.json").read_text(encoding="utf-8"))
    definition["parent"] = {"job_id": parent_id, "job_key": parent_key, "placement": "jobs"}
    (orphan_payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
    orphan = _finish(site.server, site.server.submit(orphan_payload, "jobs/kids"))
    # The client sends a finished P whose spawn records list L.
    payload = _payload(site.tmp / "payloads", "claimed-parent")
    document = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    document["id"] = parent_id
    (payload / "job.json").write_text(json.dumps(document), encoding="utf-8")
    claimed = _finish(site.client, site.client.submit(payload, "jobs"))
    assert claimed.job_key == parent_key
    record_spawns(
        site.client.payload_path(claimed.placement, claimed.job_key),
        str(uuid.uuid4()),
        [{"job_key": orphan.job_key, "label": "orphan", "placement": "jobs/kids"}],
        durable=False,
    )
    transfers.eject_job(site.client, claimed.job_id, site.inbox / "forged")
    site.service.run(now=1000.0)
    # Refused at the inbox: the spawn records name a job of this workspace.
    [(entries, reason)] = _rejections(site)
    assert entries == ["forged"] and "spawned child" in reason["reason"]
    assert site.server.find_marker_by_id(claimed.job_id) is None
    # Even if adoption let it in, the return pass would never take L along.
    [unique] = list(site.rejected.iterdir())
    os.rename(unique / "forged", site.inbox / "forged")
    monkeypatch.setattr(_bundle, "_check_exchange_spawns", lambda *_args: None)
    site.service.run(now=1001.0)
    assert site.server.read_state(_present_once(site.server, claimed.job_id))["origin"] == "exchange"
    with caplog_events(site) as events:
        for index in range(3):
            site.service.run(now=_LATER + 10 * index)
    _present_once(site.server, orphan.job_id)
    _present_once(site.server, claimed.job_id)
    assert sorted(os.listdir(site.outbox)) == ["rejected"]
    assert any("did not come through the exchange" in message for message in events)


def test_a_child_registered_here_inherits_the_exchange_origin_of_its_parent(
    site: _Site, confined: list[Mapping[str, Any]]
) -> None:
    sent = _send(site, "root")
    site.service.run(now=1000.0)
    root = _present_once(site.server, sent.job_id)
    local = site.server.submit(_payload(site.tmp / "payloads", "local-parent"), "jobs")
    children = {}
    for parent, tag in ((root, "exchange-child"), (local, "local-child")):
        payload = _payload(site.tmp / "payloads", tag)
        definition = json.loads((payload / "job.json").read_text(encoding="utf-8"))
        definition["parent"] = {"job_id": parent.job_id, "job_key": parent.job_key, "placement": "jobs"}
        (payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
        children[tag] = site.server.submit(payload, "jobs/kids")
    with TaskManager(Workspace(site.server.root), heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager._register_submissions()
    states = {tag: site.server.read_state(_present_once(site.server, child.job_id)) for tag, child in children.items()}
    assert states["exchange-child"]["origin"] == "exchange" and "origin" not in states["local-child"]


@pytest.mark.timing
def test_an_until_idle_manager_survives_a_damaged_frame(
    site: _Site, confined: list[Mapping[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_exchange, "RETURN_GRACE_SECONDS", 0.5)
    monkeypatch.setattr(_exchange, "STEP_INTERVAL", 0.0)
    sent = _send(site, "done", kind="succeeded")
    local = site.server.submit(_payload(site.tmp / "payloads", "waits"), "jobs")
    with site.server.open_journal_writer() as writer:
        waiting = site.server.transition(writer, local, "waiting", {"reason": "test", "join": {"children": []}})
    server = Workspace(site.server.root)
    read_state = server.read_state

    def damaged(marker: Marker) -> dict[str, Any]:
        if marker.job_id == waiting.job_id:
            raise WorkspaceCorruptionError("damaged frame")
        return read_state(marker)

    monkeypatch.setattr(server, "read_state", damaged)
    with TaskManager(server, heartbeat_interval=0.01, setting_overrides=_PINNED, join_grace_seconds=3600.0) as manager:
        census = manager.run_until_idle(timeout=60.0, poll_interval=0.05)
    assert census.exchange == 0
    assert (site.outbox / sent.job_key).is_dir() and site.server.find_marker_by_id(sent.job_id) is None


@pytest.mark.parametrize("own", [True, False])
def test_an_ejection_into_an_exchange_inbox_commits_by_descriptor(site: _Site, own: bool) -> None:
    workspace = site.server if own else site.client
    marker = workspace.submit(_payload(site.tmp / "payloads", "sent"), "jobs")
    target = transfers._eject_commit(workspace, site.inbox, marker.job_key)
    assert (target.kind, target.path, target.name) == ("inbox", site.server.root, marker.job_key)
    assert transfers.eject_job(workspace, marker.job_id, site.inbox) == site.inbox / marker.job_key
    assert transfers.validate_bundle(site.inbox / marker.job_key)["job_id"] == marker.job_id


def test_an_inbox_swapped_for_a_symlink_before_the_commit_is_never_written_through(site: _Site) -> None:
    marker = site.client.submit(_payload(site.tmp / "payloads", "sent"), "jobs")
    elsewhere = site.tmp / "elsewhere"
    elsewhere.mkdir()

    def swap() -> None:
        os.rename(site.inbox, site.tmp / "inbox-aside")
        site.inbox.symlink_to(elsewhere)

    _hook_at("S6.commit", swap)
    with pytest.raises(ValueError, match="aborted|not a real directory"):
        transfers.eject_job(site.client, marker.job_id, site.inbox)
    assert os.listdir(elsewhere) == [] and os.listdir(site.tmp / "inbox-aside") == []
    home = _present_once(site.client, marker.job_id)
    assert home.kind == marker.kind and site.client.payload_path(home.placement, home.job_key).is_dir()


def test_a_rejection_whose_bundle_was_taken_over_leaves_no_rejected_directory(site: _Site) -> None:
    (site.inbox / "broken").mkdir()
    taken: list[Path] = []

    def take_over() -> None:
        [lineage] = _lineages(site.server)
        moved = lineage.with_name(f"{lineage.name}-taken")
        os.rename(lineage, moved)
        taken.append(moved)

    _hook_at("V10.rejected_dir", take_over)
    site.service.run(now=1000.0)
    assert os.listdir(site.rejected) == []
    assert (taken[0] / "bundle").is_dir()


def test_every_first_frame_of_a_child_inherits_exchange_origin_and_the_tree_returns(
    site: _Site, confined: list[Mapping[str, Any]]
) -> None:
    sent = _send(site, "root")
    site.service.run(now=1000.0)
    root = _present_once(site.server, sent.job_id)
    children: dict[str, Marker] = {}
    for tag in ("refused", "cancelled", "paused"):
        payload = _payload(site.tmp / "payloads", tag)
        definition = json.loads((payload / "job.json").read_text(encoding="utf-8"))
        definition["parent"] = {"job_id": root.job_id, "job_key": root.job_key, "placement": "jobs"}
        (payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
        children[tag] = site.server.submit(payload, "jobs/kids")
    record_spawns(
        site.server.payload_path(root.placement, root.job_key),
        str(uuid.uuid4()),
        [{"job_key": child.job_key, "label": tag, "placement": "jobs/kids"} for tag, child in children.items()],
        durable=False,
    )
    # A registration that fails: the runner the child names is gone.
    refused = children["refused"]
    (site.server.payload_path(refused.placement, refused.job_key) / "files" / "runner").unlink()
    with site.server.open_journal_writer() as writer:
        site.server.transition(writer, children["cancelled"], "cancelled", {"reason": "operator_cancel"})
        site.server.transition(writer, children["paused"], "paused", {"reason": "operator_pause"})
    with TaskManager(Workspace(site.server.root), heartbeat_interval=0.01, setting_overrides=_PINNED) as manager:
        manager._register_submissions()
    kinds = {}
    for tag, child in children.items():
        current = _present_once(site.server, child.job_id)
        kinds[tag] = current.kind
        assert site.server.read_state(current)["origin"] == "exchange", tag
    assert kinds == {"refused": "failed", "cancelled": "cancelled", "paused": "paused"}
    _finish(site.server, _present_once(site.server, root.job_id))
    site.service.run(now=_LATER)
    assert site.server.find_marker_by_id(root.job_id) is None
    members = transfers.validate_bundle(site.outbox / root.job_key)["members"]
    assert {member["job_id"] for member in members} == {child.job_id for child in children.values()}


@pytest.mark.parametrize("unreadable", ["directory", "file"])
def test_an_unreadable_entry_in_an_inbox_bundle_is_refused_not_retried(site: _Site, unreadable: str) -> None:
    if os.geteuid() == 0:
        pytest.skip("root reads everything")
    marker, loose = _loose(site, "locked")
    entry = loose / "files" / ("locked-dir" if unreadable == "directory" else "runner")
    if unreadable == "directory":
        entry.mkdir()
        (entry / "inside").write_text("x", encoding="utf-8")
    os.rename(loose, site.inbox / "locked")
    locked = site.inbox / "locked" / "files" / entry.name
    os.chmod(locked, 0)
    try:
        site.service.run(now=1000.0)
        [(entries, reason)] = _rejections(site)
        assert entries == ["locked"] and "cannot be read or searched" in reason["reason"]
        assert f"files/{entry.name}" in reason["reason"]
        assert site.server.find_marker_by_id(marker.job_id) is None
        assert _lineages(site.server) == [] and os.listdir(site.inbox) == []
        assert site.service.outstanding(now=1000.0) == 0
    finally:
        for unique in site.rejected.iterdir():
            target = unique / "locked" / "files" / entry.name
            if os.path.lexists(target):
                os.chmod(target, 0o700)


def test_a_symlinked_workspace_prefix_never_bypasses_the_inbox_refusal(site: _Site) -> None:
    marker, loose = _loose(site, "prefix")
    alias = site.tmp / "alias"
    alias.symlink_to(site.server.root)
    elsewhere = site.tmp / "client-dir"
    elsewhere.mkdir()
    os.rename(loose, elsewhere / "entry")
    # The client swaps the inbox for a symlink to a directory outside the workspace.
    os.rename(site.inbox, site.tmp / "inbox-aside")
    site.inbox.symlink_to(elsewhere)
    with pytest.raises(ValueError, match="exchange inbox"):
        transfers.adopt_job(site.server, alias / "exchange" / "inbox" / "entry")
    assert (elsewhere / "entry" / "job.json").is_file() and site.server.find_marker_by_id(marker.job_id) is None
    # The same through the outbox: claimed by descriptor, so a swapped outbox refuses.
    os.rename(site.outbox, site.tmp / "outbox-aside")
    site.outbox.symlink_to(elsewhere)
    with pytest.raises(ValueError, match="not a real directory"):
        transfers.adopt_job(site.server, alias / "exchange" / "outbox" / "entry")
    assert (elsewhere / "entry" / "job.json").is_file()


def test_a_denied_workspace_path_during_verification_is_retried_not_refused(
    site: _Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent = _send(site, "retried")
    state = site.server.control / "state" / "succeeded"

    def denied(*_args: object) -> None:
        raise PermissionError(errno.EACCES, "Permission denied", str(state))

    monkeypatch.setattr(_bundle, "_check_exchange_spawns", denied)
    monkeypatch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: False)
    site.service.run(now=1000.0)
    # This workspace's own state, not the bundle: the lineage waits for a retry.
    assert len(_lineages(site.server)) == 1 and os.listdir(site.rejected) == []
    assert site.server.find_marker_by_id(sent.job_id) is None
    monkeypatch.undo()
    monkeypatch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: False)
    site.service.run(now=1010.0)
    _present_once(site.server, sent.job_id)
    assert _lineages(site.server) == [] and os.listdir(site.rejected) == []
