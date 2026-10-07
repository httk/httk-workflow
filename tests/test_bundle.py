"""Bundle format v3 and the one verification function every import trusts it through (plan 4.2)."""

import errno
import json
import os
import shutil
import time
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from httk.core.digests import sha256_file

from conftest import configure_identity
from httk.workflow import Workspace, _bundle
from httk.workflow._bundle import (
    EXCHANGE_PRIOR_STATE_MEMBERS,
    TRANSFER_DIRECTORY,
    BundleManifest,
    VerifiedBundle,
    check_bundle,
    verify_bundle,
)
from httk.workflow.errors import FormatError
from httk.workflow.models import make_job_key
from httk.workflow.seals import job_seal_path

_RUNNER = "tools/run.py"


def _job(directory: Path, tag: str, *, parent: dict[str, object] | None = None) -> tuple[str, str]:
    """Write one minimal job payload at *directory*; return its job id and key."""

    job_id = str(uuid.uuid4())
    directory.mkdir(parents=True)
    (directory / "files").mkdir()
    (directory / "files" / "runner").write_text("#!/bin/sh\nexit 0\n")
    (directory / "files" / "runner").chmod(0o755)
    document: dict[str, object] = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": tag,
        "name": f"bundle {tag}",
        "workflow": "tests.bundle",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "start",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
    }
    if parent is not None:
        document["parent"] = dict(parent)
    (directory / "job.json").write_text(json.dumps(document))
    return job_id, make_job_key(job_id, tag)


class Bundle:
    """A hand-built version 3 bundle whose manifest is rewritten on demand."""

    def __init__(self, root: Path, manifest: dict[str, Any]) -> None:
        self.root = root
        self.manifest = manifest

    @property
    def envelope(self) -> Path:
        return self.root / TRANSFER_DIRECTORY

    def member(self, index: int) -> dict[str, Any]:
        return self.manifest["members"][index]

    def member_path(self, index: int) -> Path:
        member = self.member(index)
        return self.envelope.joinpath("tree", *PurePosixPath(member["placement"]).parts, member["job_key"])

    def write(self) -> None:
        (self.envelope / "manifest.json").write_text(json.dumps(self.manifest))

    def redigest(self) -> None:
        """Recompute every digest after a deliberate change of content."""

        self.manifest["payload_sha256"] = _bundle._payload_digest(self.root)
        for index in range(len(self.manifest["members"])):
            self.member(index)["payload_sha256"] = _bundle._payload_digest(self.member_path(index))
        self.write()


def _build(
    tmp_path: Path,
    *,
    members: int = 0,
    runner: bool = False,
    destination: str | None = None,
    prior_kind: str = "succeeded",
    prior_state: dict[str, Any] | None = None,
) -> Bundle:
    """Build a valid bundle: a root, *members* children (the last at the empty placement), a runner."""

    root = tmp_path / "bundle"
    root_id, root_key = _job(root, "root")
    envelope = root / TRANSFER_DIRECTORY
    for name in ("markers", "runners", "tree"):
        (envelope / name).mkdir(parents=True)
    runners: list[dict[str, str]] = []
    if runner:
        carried = envelope / "runners" / _RUNNER
        carried.parent.mkdir(parents=True)
        carried.write_text("print('run')\n")
        carried.chmod(0o555)
        runners.append({"path": _RUNNER, "sha256": sha256_file(carried)})
    entries: list[dict[str, Any]] = []
    parent_id, parent_key, parent_placement = root_id, root_key, "jobs/a"
    for index in range(members):
        placement = "" if index == members - 1 else "kids"
        member_dir = envelope.joinpath("tree", *PurePosixPath(placement).parts)
        job_id, job_key = _job(
            member_dir / "pending",
            f"m{index}",
            parent={"job_id": parent_id, "job_key": parent_key, "placement": parent_placement},
        )
        (member_dir / "pending").rename(member_dir / job_key)
        entries.append(
            {
                "job_id": job_id,
                "job_key": job_key,
                "placement": placement,
                "parent_job_id": parent_id,
                "payload_sha256": _bundle._payload_digest(member_dir / job_key),
                "seal_sha256": None,
                "prior_kind": "succeeded",
                "prior_state": {"kind": "succeeded", "step": "start", "reason": "succeeded"},
                "priority": 500,
                "source_generation": 7,
            }
        )
        parent_id, parent_key, parent_placement = job_id, job_key, placement
    for job_id in (root_id, *(entry["job_id"] for entry in entries)):
        (envelope / "markers" / job_id).touch()
    manifest: dict[str, Any] = {
        "format": "httk-workflow-detached-transfer",
        "format_version": 3,
        "core_profile": "core-v3",
        "transfer_id": str(uuid.uuid4()),
        "source_workspace_id": str(uuid.uuid4()),
        "destination_workspace_id": destination,
        "destination_remote": None,
        "destination_placement": "jobs/a",
        "sealed_at": time.time_ns(),
        "job_id": root_id,
        "job_key": root_key,
        "source_placement": "jobs/a",
        "payload_sha256": _bundle._payload_digest(root),
        "seal_sha256": None,
        "runners": runners,
        "prior_kind": prior_kind,
        "prior_state": {"kind": prior_kind, "step": "start"} if prior_state is None else prior_state,
        "priority": 500,
        "source_generation": 9,
        "members": entries,
    }
    bundle = Bundle(root, manifest)
    bundle.write()
    return bundle


@pytest.fixture
def workspace(tmp_path: Path) -> Workspace:
    configure_identity()
    return Workspace.initialize(tmp_path / "workspace")


def _refused(bundle: Bundle, workspace: Workspace, match: str, *, source: Any = "adopt") -> None:
    with pytest.raises(FormatError, match=match):
        verify_bundle(bundle.root, workspace=workspace, source=source)


# ---------------------------------------------------------------------------
# Valid bundles
# ---------------------------------------------------------------------------


def test_a_root_only_bundle_verifies(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path)
    verified = verify_bundle(bundle.root, workspace=workspace, source="adopt")
    assert isinstance(verified, VerifiedBundle)
    assert verified.transfer_id == bundle.manifest["transfer_id"]
    [job] = verified.jobs
    assert job.is_root and job.job_key == bundle.manifest["job_key"]
    assert job.placement == PurePosixPath("jobs/a") and job.payload == bundle.root
    assert job.envelope_relative is None and job.prior_kind == "succeeded"
    assert verified.manifest.as_mapping() == bundle.manifest
    assert BundleManifest.from_mapping(verified.manifest.as_mapping()) == verified.manifest


def _ids(bundle: Bundle) -> list[str]:
    return [bundle.manifest["job_id"], *(member["job_id"] for member in bundle.manifest["members"])]


def test_a_tree_bundle_with_a_runner_verifies(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=3, runner=True)
    verified = verify_bundle(bundle.root, workspace=workspace, source="export")
    assert [job.job_id for job in verified.jobs] == sorted(_ids(bundle))
    assert [runner.path for runner in verified.manifest.runners] == [PurePosixPath(_RUNNER)]


def test_tree_facts_are_ready_for_the_import(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=3, runner=True)
    verified = verify_bundle(bundle.root, workspace=workspace, source="adopt")
    assert [job.job_id for job in verified.top_down] == _ids(bundle)
    empty = verified.job(bundle.member(2)["job_id"])
    assert empty.placement == PurePosixPath()
    assert empty.envelope_relative == PurePosixPath("tree", bundle.member(2)["job_key"])
    assert empty.payload == bundle.member_path(2)
    assert empty.parent_job_id == bundle.member(1)["job_id"]
    assert verified.job(bundle.member(0)["job_id"]).parent_job_id == bundle.manifest["job_id"]
    assert verified.manifest.as_mapping() == bundle.manifest
    with pytest.raises(KeyError):
        verified.job(str(uuid.uuid4()))


def test_an_addressed_bundle_verifies_only_as_incoming_to_its_destination(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, destination=workspace.workspace_id)
    assert verify_bundle(bundle.root, workspace=workspace, source="incoming").manifest.destination_workspace_id
    _refused(bundle, workspace, "only an ejected bundle", source="adopt")
    _refused(bundle, workspace, "only an ejected bundle", source="exchange_inbox")
    bundle.manifest["destination_workspace_id"] = str(uuid.uuid4())
    bundle.write()
    _refused(bundle, workspace, "is addressed to workspace", source="incoming")
    bundle.manifest["destination_workspace_id"] = None
    bundle.write()
    _refused(bundle, workspace, "is addressed to workspace None", source="incoming")


def test_the_walk_paces_its_caller(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path)
    many = bundle.root / "files" / "many"
    many.mkdir()
    for index in range(2100):
        (many / str(index)).write_text("")
    bundle.redigest()
    calls: list[None] = []
    verify_bundle(bundle.root, workspace=workspace, source="adopt", pace=lambda: calls.append(None))
    assert len(calls) >= 4  # twice in the walk, twice in the digest


def test_an_unknown_source_is_a_programming_error(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path)
    with pytest.raises(ValueError, match="unknown bundle source"):
        verify_bundle(bundle.root, workspace=workspace, source="elsewhere")  # type: ignore[arg-type]


def test_a_read_error_is_left_for_a_retry_not_refused(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _build(tmp_path)

    def stale(*args: Any, **kwargs: Any) -> bytes:
        raise OSError(errno.ESTALE, "stale file handle")

    monkeypatch.setattr(_bundle, "_read_regular_file", stale)
    with pytest.raises(OSError) as raised:
        verify_bundle(bundle.root, workspace=workspace, source="adopt")
    assert not isinstance(raised.value, FormatError) and raised.value.errno == errno.ESTALE
    with pytest.raises(FileNotFoundError):
        verify_bundle(tmp_path / "absent", workspace=workspace, source="adopt")


# ---------------------------------------------------------------------------
# Refusals, step by step
# ---------------------------------------------------------------------------


def test_a_symlinked_root_is_refused(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path)
    (tmp_path / "alias").symlink_to(bundle.root)
    with pytest.raises(FormatError, match="not a directory"):
        verify_bundle(tmp_path / "alias", workspace=workspace, source="adopt")


def test_unsafe_entries_are_refused_anywhere(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=1)
    fifo = bundle.envelope / "runners" / "fifo"
    os.mkfifo(fifo)
    _refused(bundle, workspace, "special entry")
    fifo.unlink()
    target = bundle.member_path(0) / "files" / "runner"
    os.link(target, bundle.member_path(0) / "files" / "alias")
    _refused(bundle, workspace, "hard-linked")
    (bundle.member_path(0) / "files" / "alias").unlink()
    (bundle.root / "files" / "escape").symlink_to("../../..")
    _refused(bundle, workspace, "symlink")


def test_a_symlinked_envelope_or_manifest_is_refused(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path)
    shutil.move(bundle.envelope / "manifest.json", bundle.root / "files" / "manifest.json")
    (bundle.envelope / "manifest.json").symlink_to("../files/manifest.json")
    _refused(bundle, workspace, "not a regular file")
    (bundle.envelope / "manifest.json").unlink()
    _refused(bundle, workspace, "lacks .httk-transfer/manifest.json")
    shutil.move(bundle.envelope, bundle.root / "files" / "envelope")
    bundle.envelope.symlink_to("files/envelope")
    _refused(bundle, workspace, "not a real directory")


@pytest.mark.parametrize(
    ("change", "match"),
    [
        (lambda m: m.update(job_key="../../../../x"), "invalid job key"),
        (lambda m: m.update(job_key=make_job_key(str(uuid.uuid4()), "root")), "does not carry the job id"),
        (lambda m: m["members"][0].update(job_key=make_job_key(str(uuid.uuid4()), "m0")), "does not carry"),
        (lambda m: m["members"].append(dict(m["members"][0])), "more than once"),
        (lambda m: m["members"][1].update(parent_job_id=str(uuid.uuid4())), "neither the root nor an earlier"),
        (lambda m: m["members"].reverse(), "neither the root nor an earlier"),
        (lambda m: m.update(prior_kind="running"), "quiescent"),
        (lambda m: m["members"][0].update(prior_kind="transferring"), "quiescent"),
        (lambda m: m.update(format_version=2), "unsupported transfer bundle"),
        (lambda m: m.update(core_profile="core-v2"), "unsupported transfer bundle"),
        (lambda m: m.update(sealed_marker="x"), "unknown members sealed_marker"),
        (lambda m: m.pop("sealed_at"), "lacks sealed_at"),
        (lambda m: m.update(sealed_at="1"), "sealed_at must be an integer"),
        (lambda m: m.update(sealed_at=True), "sealed_at must be an integer"),
        (lambda m: m.update(transfer_id=str(uuid.uuid4()).upper()), "canonical"),
        (lambda m: m.update(destination_placement="../x"), "placement"),
        (lambda m: m["members"][0].update(placement=f"kids/{uuid.uuid4()}"), "parses as a job key"),
        (lambda m: m.update(priority=1000), "priority"),
        (lambda m: m["members"][0].update(prior_state=[]), "prior_state must be an object"),
        (lambda m: m["members"][0].update(extra=1), "unknown members extra"),
        (lambda m: m.update(destination_remote=""), "destination_remote"),
        (lambda m: m.update(runners=[{"path": "../x", "sha256": "0" * 64}]), "runner"),
    ],
)
def test_a_malformed_manifest_is_refused(
    tmp_path: Path, workspace: Workspace, change: Callable[[dict[str, Any]], None], match: str
) -> None:
    bundle = _build(tmp_path, members=2)
    change(bundle.manifest)
    bundle.write()
    _refused(bundle, workspace, match)


def test_a_manifest_that_is_not_json_is_refused(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path)
    (bundle.envelope / "manifest.json").write_text("{")
    _refused(bundle, workspace, "not JSON")
    (bundle.envelope / "manifest.json").write_text("[]")
    _refused(bundle, workspace, "must be an object")


def test_markers_must_be_exactly_one_empty_file_per_job(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=1)
    markers = bundle.envelope / "markers"
    (markers / bundle.member(0)["job_id"]).unlink()
    _refused(bundle, workspace, "lacks the marker of job")
    (markers / bundle.member(0)["job_id"]).touch()
    (markers / str(uuid.uuid4())).touch()
    _refused(bundle, workspace, "unexpected marker")
    for extra in markers.iterdir():
        if extra.name not in _ids(bundle):
            extra.unlink()
    (markers / bundle.manifest["job_id"]).write_text("not empty")
    _refused(bundle, workspace, "is not empty")
    (markers / bundle.manifest["job_id"]).unlink()
    (markers / bundle.manifest["job_id"]).mkdir()
    _refused(bundle, workspace, "not a regular file")


def test_the_tree_holds_exactly_the_members(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=2)
    tree = bundle.envelope / "tree"
    (tree / "kids" / "stray.txt").write_text("x")
    _refused(bundle, workspace, "unexpected entry .httk-transfer/tree/kids/stray.txt")
    (tree / "kids" / "stray.txt").unlink()
    (tree / "other").mkdir()
    _refused(bundle, workspace, "unexpected entry .httk-transfer/tree/other")
    (tree / "other").rmdir()
    member = bundle.member_path(1)
    shutil.move(member, tmp_path / "away")
    _refused(bundle, workspace, "lacks .httk-transfer/tree/")
    member.symlink_to(Path("..", "..", "..", "..", "away"))
    with pytest.raises(FormatError):
        verify_bundle(bundle.root, workspace=workspace, source="adopt")


def test_the_envelope_holds_nothing_else(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path)
    (bundle.envelope / ".manifest.json.link-0123").write_text("leftover")
    _refused(bundle, workspace, "unexpected entry")
    (bundle.envelope / ".manifest.json.link-0123").unlink()
    shutil.rmtree(bundle.envelope / "runners")
    _refused(bundle, workspace, "lacks .httk-transfer/runners")


def test_runners_must_be_exactly_the_declared_ones(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, runner=True)
    (bundle.envelope / "runners" / "tools" / "other.py").write_text("x")
    _refused(bundle, workspace, "unexpected entry .httk-transfer/runners/tools/other.py")
    (bundle.envelope / "runners" / "tools" / "other.py").unlink()
    carried = bundle.envelope / "runners" / _RUNNER
    carried.chmod(0o755)
    carried.write_text("print('changed')\n")
    _refused(bundle, workspace, "runner digest mismatch")
    carried.unlink()
    _refused(bundle, workspace, "lacks .httk-transfer/runners/tools/run.py")


def test_digest_and_seal_mismatches_are_refused(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=1)
    (bundle.root / "files" / "runner").write_text("#!/bin/sh\nexit 1\n")
    _refused(bundle, workspace, "payload digest mismatch for job root")
    bundle.redigest()
    (bundle.member_path(0) / "added").write_text("x")
    _refused(bundle, workspace, "payload digest mismatch for job m0")
    bundle.redigest()
    seal = job_seal_path(bundle.member_path(0))
    seal.parent.mkdir(parents=True)
    seal.write_text("{}")
    _refused(bundle, workspace, "seal does not match the manifest for job m0")
    bundle.member(0)["seal_sha256"] = sha256_file(seal)
    bundle.write()
    verify_bundle(bundle.root, workspace=workspace, source="adopt")


# ---------------------------------------------------------------------------
# Exchange-inbox bundles
# ---------------------------------------------------------------------------


def test_the_exchange_allowlist_filters_prior_state(tmp_path: Path, workspace: Workspace) -> None:
    prior = {
        "kind": "failed",
        "step": "start",
        "failure": {"code": "x"},
        "manager_id": str(uuid.uuid4()),
        "transfer": {"transfer_id": str(uuid.uuid4())},
        "origin": "exchange",
        "outgoing": {"transfer_id": str(uuid.uuid4())},
        "prior_state": {},
        "invented": True,
    }
    bundle = _build(tmp_path, members=1, prior_kind="failed", prior_state=prior)
    inbox = verify_bundle(bundle.root, workspace=workspace, source="exchange_inbox")
    assert dict(inbox.root.prior_state) == {
        "step": "start",
        "failure": {"code": "x"},
        "manager_id": prior["manager_id"],
    }
    assert set(inbox.job(bundle.member(0)["job_id"]).prior_state) == {"step", "reason"}
    # The manifest itself is kept verbatim, and other sources carry everything.
    assert dict(inbox.manifest.prior_state) == prior
    adopted = verify_bundle(bundle.root, workspace=workspace, source="adopt")
    assert dict(adopted.root.prior_state) == prior
    assert not {"transfer", "origin", "outgoing", "prior_kind", "prior_state"} & EXCHANGE_PRIOR_STATE_MEMBERS


def test_an_exchange_waiting_join_may_name_only_jobs_of_the_bundle(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=1, prior_kind="waiting", prior_state={"join": {"children": []}})
    member = bundle.member(0)
    children: list[dict[str, str]] = [{"job_id": member["job_id"], "job_key": member["job_key"]}]
    join: dict[str, object] = {"condition": "all_succeeded", "children": children}
    bundle.manifest["prior_state"] = {"join": join, "next_step": "after"}
    bundle.write()
    assert dict(verify_bundle(bundle.root, workspace=workspace, source="exchange_inbox").root.prior_state)["join"]
    local = str(uuid.uuid4())
    children.append({"job_id": local, "job_key": local})
    bundle.write()
    _refused(bundle, workspace, "not part of the bundle", source="exchange_inbox")
    verify_bundle(bundle.root, workspace=workspace, source="adopt")  # not an exchange concern elsewhere
    join["children"] = [{"job_id": member["job_id"], "job_key": make_job_key(local, "x")}]
    bundle.write()
    _refused(bundle, workspace, "does not carry", source="exchange_inbox")
    bundle.manifest["prior_state"] = {"next_step": "after"}
    bundle.write()
    _refused(bundle, workspace, "carries no join", source="exchange_inbox")


def _rewrite_parent(payload: Path, parent: Mapping[str, object] | None) -> None:
    document = json.loads((payload / "job.json").read_text())
    if parent is None:
        document.pop("parent", None)
    else:
        document["parent"] = dict(parent)
    (payload / "job.json").write_text(json.dumps(document))


def test_an_exchange_root_may_not_name_a_parent_present_here(tmp_path: Path, workspace: Workspace) -> None:
    _job(tmp_path / "local", "local")
    local = workspace.submit(tmp_path / "local", "jobs/local")
    bundle = _build(tmp_path)
    parent = {"job_id": local.job_id, "job_key": local.job_key, "placement": "jobs/local"}
    _rewrite_parent(bundle.root, parent)
    bundle.redigest()
    _refused(bundle, workspace, "a job of this workspace", source="exchange_inbox")
    verify_bundle(bundle.root, workspace=workspace, source="adopt")
    # A parent that is transferring away still counts as present.
    with workspace.open_journal_writer() as writer:
        workspace.transition(writer, local, "transferring", {"prior_kind": "submitted"})
    assert workspace.find_marker_by_id(local.job_id) is None
    _refused(bundle, workspace, "a job of this workspace", source="exchange_inbox")
    elsewhere = str(uuid.uuid4())
    _rewrite_parent(bundle.root, {"job_id": elsewhere, "job_key": elsewhere, "placement": "jobs"})
    bundle.redigest()
    verify_bundle(bundle.root, workspace=workspace, source="exchange_inbox")


def test_an_exchange_member_must_be_its_manifest_parents_child(tmp_path: Path, workspace: Workspace) -> None:
    bundle = _build(tmp_path, members=2)
    verify_bundle(bundle.root, workspace=workspace, source="exchange_inbox")
    other = str(uuid.uuid4())
    _rewrite_parent(bundle.member_path(1), {"job_id": other, "job_key": other, "placement": "x"})
    bundle.redigest()
    _refused(bundle, workspace, "is not a child of its manifest parent", source="exchange_inbox")
    _rewrite_parent(bundle.member_path(1), None)
    bundle.redigest()
    _refused(bundle, workspace, "is not a child of its manifest parent", source="exchange_inbox")


@pytest.mark.parametrize("source", ["adopt", "export", "incoming", "exchange_inbox"])
@pytest.mark.parametrize("which", ["root", "member"])
def test_every_source_checks_each_payloads_job_json_against_its_key(
    source: str, which: str, tmp_path: Path, workspace: Workspace
) -> None:
    bundle = _build(tmp_path, members=1, destination=workspace.workspace_id if source == "incoming" else None)
    verify_bundle(bundle.root, workspace=workspace, source=source)  # type: ignore[arg-type]
    payload = bundle.root if which == "root" else bundle.member_path(0)
    document = json.loads((payload / "job.json").read_text())
    document["id"] = str(uuid.uuid4())  # a valid job.json of another job
    (payload / "job.json").write_text(json.dumps(document))
    bundle.redigest()
    _refused(bundle, workspace, "holds the job.json of", source=source)
    with pytest.raises(FormatError, match="holds the job.json of"):
        check_bundle(bundle.root)
