"""Tests of :mod:`httk.workflow._bundles`: building, validating (the adoption trust boundary) and rekeying bundles."""

import dataclasses
import json
import os
import shutil
import signal
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import Workspace, _fs, _kernel
from httk.workflow._bundles import (
    _EXCHANGE_NAMESPACE,
    BUNDLE_FORMAT,
    BUNDLE_VERSION,
    MAX_MANIFEST_BYTES,
    BundleError,
    BundleManifest,
    BundleMember,
    build_bundle,
    exchange_names,
    read_rekeyed,
    rekey_untrusted,
    validate_bundle,
)
from httk.workflow._fs import Loc, WalkLimits
from httk.workflow._job import JobDefinition
from httk.workflow.errors import WorkflowError
from httk.workflow.models import make_job_key
from v3_helpers import cli_owner, job_mapping
from v3_helpers import workspace as v3_workspace

SOURCE = "2d7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e03"
ADOPTER = "7c1e9a52-3b0d-4f6e-8a21-5d4c3b2a1f09"
OTHER_ADOPTER = "0e8d7c6b-5a49-4382-b1a0-9f8e7d6c5b4a"
WORKFLOW = ("local:demo", "demo")
CREATED = "2026-10-10T08:00:00.000000Z"
OUTSIDER = "3f2e1d0c-9b8a-4766-a554-433221100fed"


@pytest.fixture(autouse=True)
def _clean_kernel(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_kernel, "_RECONCILERS", {})
    yield
    _fs.set_fault_injector(None)


@contextmanager
def no_hang(seconds: int = 5) -> Iterator[None]:
    """Fail instead of hanging when a call blocks (a FIFO opened without O_NONBLOCK)."""

    def expire(signum: int, frame: object) -> None:
        raise TimeoutError("the call blocked")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def parent_link(parent: dict[str, object], workspace_id: str = SOURCE) -> dict[str, object]:
    return {
        "workspace_id": workspace_id,
        "job_id": parent["id"],
        "job_key": make_job_key(str(parent["id"]), parent["tag"]),  # type: ignore[arg-type]
        "placement": parent["placement"],
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }


def family(workspace_id: str = SOURCE) -> list[dict[str, object]]:
    """``job.json`` mappings of a root, its child and its grandchild, top-down."""

    root = job_mapping(WORKFLOW, {"start": "succeed"}, tag="root", placement="project/0")
    child = job_mapping(WORKFLOW, {"start": "succeed"}, tag="child", placement="project/0/c0")
    grandchild = job_mapping(WORKFLOW, {"start": "succeed"}, tag=None, placement="elsewhere")
    child["parent"] = parent_link(root, workspace_id)
    grandchild["parent"] = parent_link(child, workspace_id)
    return [root, child, grandchild]


def key_of(mapping: dict[str, object]) -> str:
    return make_job_key(str(mapping["id"]), mapping["tag"])  # type: ignore[arg-type]


def member_of(mapping: dict[str, object], parent: dict[str, object] | None, state: str = "ready") -> BundleMember:
    return BundleMember(
        str(mapping["id"]),
        key_of(mapping),
        PurePosixPath(str(mapping["placement"])),
        state,
        500,
        None if parent is None else str(parent["id"]),
    )


def manifest_of(jobs: list[dict[str, object]], states: tuple[str, ...] | None = None) -> BundleManifest:
    states = states or ("ready",) * len(jobs)
    by_id = {mapping["id"]: mapping for mapping in jobs}
    members = []
    for mapping, state in zip(jobs, states, strict=True):
        link = mapping["parent"]
        parent = by_id.get(link["job_id"]) if isinstance(link, dict) else None
        members.append(member_of(mapping, parent, state))
    # Deterministic per family, so that copies of one client bundle are byte-identical.
    transfer_id = uuid.uuid5(uuid.NAMESPACE_OID, str(jobs[0]["id"])).hex
    return BundleManifest(transfer_id, SOURCE, CREATED, None, None, tuple(members))


def write_bundle(
    bundle: Path, jobs: list[dict[str, object]] | None = None, manifest: BundleManifest | None = None
) -> tuple[BundleManifest, list[dict[str, object]]]:
    """Write a bundle by hand, as an exchange client would."""

    jobs = jobs if jobs is not None else family()
    manifest = manifest or manifest_of(jobs)
    bundle.mkdir(parents=True)
    (bundle / "bundle.json").write_bytes(manifest.to_json())
    for mapping, member in zip(jobs, manifest.members, strict=True):
        directory = manifest.member_dir(bundle, member)
        directory.mkdir(parents=True)
        (directory / "job.json").write_bytes(JobDefinition.from_mapping(mapping).encode())
        (directory / "input.txt").write_text(f"input of {member.job_key}")
    return manifest, jobs


def put(ws: Workspace, mapping: dict[str, object], state: str) -> _kernel.JobRef:
    with cli_owner(ws) as owner:
        staging = owner.scratch("submit") / "job"
        staging.mkdir()
        (staging / "job.json").write_bytes(JobDefinition.from_mapping(mapping).encode())
        (staging / "run").mkdir()
        (staging / "run" / "output.txt").write_text("result")
        return _kernel.submit(ws, owner, staging, state=state)


def snapshot(root: Path) -> dict[str, bytes | str | None]:
    """Every entry below *root*: file bytes, symlink targets, ``None`` for directories, inodes for the rest."""

    found: dict[str, bytes | str | None] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            found[relative] = os.readlink(path)
        elif path.is_dir():
            found[relative] = None
        elif path.is_file():
            found[relative] = path.read_bytes()
        else:
            # A FIFO is never opened here.
            found[relative] = f"special {path.lstat().st_ino}"
    return found


# -- building and the round trip ------------------------------------------------------------------------------------


def test_build_validate_round_trip(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs = family(ws.workspace_id)
    states = ("paused", "succeeded", "failed")
    refs = [put(ws, mapping, state) for mapping, state in zip(jobs, states, strict=True)]
    owner = cli_owner(ws)
    held = [_kernel.claim(ws, owner, ref) for ref in refs]
    assert all(job is not None for job in held)
    members = [job for job in held if job is not None]
    bundle = build_bundle(
        owner,
        members,
        source_workspace_id=ws.workspace_id,
        destination_locator="/srv/dest",
        destination_workspace_id=ADOPTER,
    )
    assert bundle.name == "bundle" and bundle.parent.name.startswith(f"{owner.owner_id}.eject.")
    assert owner.owned() == [] and not any(owner.holds(job.ref) for job in members)
    data = (bundle / "bundle.json").read_bytes()
    manifest = validate_bundle(bundle, untrusted=False)
    expected = BundleManifest(
        manifest.transfer_id,
        ws.workspace_id,
        manifest.created_at,
        ADOPTER,
        "/srv/dest",
        (
            BundleMember(str(jobs[0]["id"]), key_of(jobs[0]), PurePosixPath("project/0"), "paused", 500, None),
            BundleMember(
                str(jobs[1]["id"]), key_of(jobs[1]), PurePosixPath("project/0/c0"), "succeeded", 500, str(jobs[0]["id"])
            ),
            BundleMember(
                str(jobs[2]["id"]), key_of(jobs[2]), PurePosixPath("elsewhere"), "failed", 500, str(jobs[1]["id"])
            ),
        ),
    )
    assert manifest == expected
    assert manifest.to_json() == data and BundleManifest.from_json(data) == manifest
    assert BundleManifest.from_json(manifest.to_json()).to_json() == data
    assert len(manifest.transfer_id) == 32
    for member in manifest.members:
        assert (manifest.member_dir(bundle, member) / "run" / "output.txt").read_text() == "result"
    # The payloads are complete jobs from a workspace (state.json, logs/): trusted only.
    with pytest.raises(BundleError):
        validate_bundle(bundle, untrusted=True)
    owner._discard(bundle.parent)
    owner.close()


def test_build_takes_the_name_priority_and_a_subtree_root(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs = family(ws.workspace_id)
    refs = [put(ws, mapping, "succeeded") for mapping in jobs]
    owner = cli_owner(ws)
    # A subtree: the child is the bundle's root although its job.json names a parent outside the bundle.
    held = [_kernel.claim(ws, owner, ref) for ref in refs[1:]]
    members = [job for job in held if job is not None]
    assert len(members) == 2
    bundle = build_bundle(owner, members, source_workspace_id=ws.workspace_id)
    manifest = validate_bundle(bundle, untrusted=False)
    assert manifest.members[0].parent_job_id is None and manifest.members[1].parent_job_id == jobs[1]["id"]
    assert (manifest.destination_workspace_id, manifest.destination_locator) == (None, None)
    owner._discard(bundle.parent)
    owner.close()


def test_build_refusals_move_nothing(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs = family(ws.workspace_id)
    refs = [put(ws, mapping, "paused") for mapping in jobs]
    owner, other = cli_owner(ws), cli_owner(ws)
    held = [_kernel.claim(ws, owner, ref) for ref in refs]
    members = [job for job in held if job is not None]
    with pytest.raises(ValueError):
        build_bundle(owner, [], source_workspace_id=ws.workspace_id)
    with pytest.raises(ValueError):
        build_bundle(owner, [members[1], members[0], members[2]], source_workspace_id=ws.workspace_id)
    with pytest.raises(ValueError):
        build_bundle(owner, [members[0], members[2], members[1]], source_workspace_id=ws.workspace_id)
    with pytest.raises(ValueError):
        build_bundle(other, members, source_workspace_id=ws.workspace_id)
    with pytest.raises(BundleError):
        build_bundle(owner, members, source_workspace_id="not-a-uuid")
    with pytest.raises(BundleError):
        build_bundle(owner, members, source_workspace_id=ws.workspace_id, destination_locator="")
    with pytest.raises(BundleError):
        build_bundle(owner, [members[0], members[1], members[1]], source_workspace_id=ws.workspace_id)
    attempt = str(uuid.uuid4())
    members[2].begin_attempt(attempt)
    with pytest.raises(WorkflowError):
        build_bundle(owner, members, source_workspace_id=ws.workspace_id)
    members[2].end_attempt(attempt)
    assert len(owner.owned()) == 3 and all(owner.holds(job.ref) for job in members)
    assert not list((ws.control / "tmp").glob(f"{owner.owner_id}.eject.*"))
    for job in members:
        job.give_back()
    owner.close()
    other.close()


def test_member_dir() -> None:
    manifest = manifest_of(family())
    bundle = Path("/b")
    assert (
        manifest.member_dir(bundle, manifest.members[1]) == Path("/b/jobs/project/0/c0") / manifest.members[1].job_key
    )


def test_empty_placement_is_accepted(tmp_path: Path) -> None:
    jobs = [job_mapping(WORKFLOW, {"start": "succeed"}, tag="flat", placement="")]
    manifest, _ = write_bundle(tmp_path / "b", jobs)
    assert validate_bundle(tmp_path / "b", untrusted=True) == manifest
    assert manifest.member_dir(tmp_path / "b", manifest.members[0]) == tmp_path / "b" / "jobs" / key_of(jobs[0])


# -- the strict schema ----------------------------------------------------------------------------------------------

type Mutation = Callable[[dict[str, object]], object]


def _members(document: dict[str, object]) -> list[dict[str, object]]:
    members = document["members"]
    assert isinstance(members, list)
    return members


def _set(*path: object, value: object) -> Mutation:
    def mutate(document: dict[str, object]) -> None:
        target: object = document
        for step in path[:-1]:
            target = target[step]  # type: ignore[index]
        target[path[-1]] = value  # type: ignore[index]

    return mutate


def _delete(*path: object) -> Mutation:
    def mutate(document: dict[str, object]) -> None:
        target: object = document
        for step in path[:-1]:
            target = target[step]  # type: ignore[index]
        del target[path[-1]]  # type: ignore[attr-defined]

    return mutate


def _swap_members(document: dict[str, object]) -> None:
    members = _members(document)
    members[1], members[2] = members[2], members[1]


def _duplicate_member(document: dict[str, object]) -> None:
    _members(document).append(dict(_members(document)[2]))


def _second_root(document: dict[str, object]) -> None:
    _members(document)[2]["parent_job_id"] = None


def _outside_parent(document: dict[str, object]) -> None:
    _members(document)[2]["parent_job_id"] = str(uuid.uuid4())


def _root_parent_inside(document: dict[str, object]) -> None:
    _members(document)[0]["parent_job_id"] = _members(document)[2]["job_id"]


def _self_parent(document: dict[str, object]) -> None:
    _members(document)[1]["parent_job_id"] = _members(document)[1]["job_id"]


def _key_of_other(document: dict[str, object]) -> None:
    _members(document)[1]["job_key"] = _members(document)[2]["job_key"]


SCHEMA_REFUSALS: dict[str, Mutation] = {
    "unknown top-level key": _set("extra", value=1),
    "missing top-level key": _delete("created_at"),
    "missing destination": _delete("destination"),
    "unknown destination key": _set("destination", "extra", value=None),
    "missing destination key": _delete("destination", "locator"),
    "unknown member key": _set("members", 0, "extra", value=1),
    "missing member key": _delete("members", 1, "state"),
    "format": _set("format", value="httk-workflow-job"),
    "format version 2": _set("format_version", value=2),
    "format version string": _set("format_version", value="1"),
    "format version boolean": _set("format_version", value=True),
    "format version float": _set("format_version", value=1.0),
    "transfer id integer": _set("transfer_id", value=7),
    "transfer id hyphenated": _set("transfer_id", value=str(uuid.uuid4())),
    "transfer id uppercase": _set("transfer_id", value=uuid.uuid4().hex.upper()),
    "source workspace not a uuid": _set("source_workspace_id", value="workspace"),
    "source workspace uppercase": _set("source_workspace_id", value=SOURCE.upper()),
    "source workspace null": _set("source_workspace_id", value=None),
    "created at integer": _set("created_at", value=1760083200),
    "created at not a timestamp": _set("created_at", value="yesterday"),
    "created at without a zone": _set("created_at", value="2026-10-10T08:00:00"),
    "created at too long": _set("created_at", value="2026-10-10T08:00:00" + "0" * 60 + "Z"),
    "destination not an object": _set("destination", value=[None, None]),
    "destination workspace not a uuid": _set("destination", "workspace_id", value="x"),
    "destination locator empty": _set("destination", "locator", value=""),
    "destination locator with NUL": _set("destination", "locator", value="/a\0b"),
    "destination locator too long": _set("destination", "locator", value="/" * 4097),
    "destination locator integer": _set("destination", "locator", value=3),
    "members not an array": _set("members", value={}),
    "members empty": _set("members", value=[]),
    "member not an object": _set("members", 1, value="member"),
    "job id not a uuid": _set("members", 0, "job_id", value="job"),
    "job id uppercase": _set("members", 0, "job_id", value=str(uuid.uuid4()).upper()),
    "job id integer": _set("members", 0, "job_id", value=5),
    "job key integer": _set("members", 0, "job_key", value=5),
    "job key names another job": _key_of_other,
    "job key bad tag": _set("members", 0, "job_key", value="Bad--" + str(uuid.uuid4())),
    "job key with tilde": _set("members", 0, "job_key", value="a~b--" + str(uuid.uuid4())),
    "placement parent": _set("members", 1, "placement", value=".."),
    "placement dotdot inside": _set("members", 1, "placement", value="project/../c0"),
    "placement absolute": _set("members", 1, "placement", value="/project/0"),
    "placement empty component": _set("members", 1, "placement", value="project//0"),
    "placement trailing slash": _set("members", 1, "placement", value="project/0/"),
    "placement dot": _set("members", 1, "placement", value="./project"),
    "placement tilde": _set("members", 1, "placement", value="project/a~b"),
    "placement control directory": _set("members", 1, "placement", value="project/.httk-workspace"),
    "placement names a job": _set("members", 1, "placement", value=f"project/{uuid.uuid4()}"),
    "placement integer": _set("members", 1, "placement", value=0),
    "placement null": _set("members", 1, "placement", value=None),
    "state unknown": _set("members", 1, "state", value="running"),
    "state owned": _set("members", 1, "state", value="owned"),
    "state empty": _set("members", 1, "state", value=""),
    "state integer": _set("members", 1, "state", value=1),
    "priority negative": _set("members", 1, "priority", value=-1),
    "priority too high": _set("members", 1, "priority", value=1000),
    "priority string": _set("members", 1, "priority", value="500"),
    "priority boolean": _set("members", 1, "priority", value=True),
    "priority float": _set("members", 1, "priority", value=500.0),
    "parent not a uuid": _set("members", 1, "parent_job_id", value="parent"),
    "parent integer": _set("members", 1, "parent_job_id", value=1),
    "duplicate id": _duplicate_member,
    "parent not earlier": _swap_members,
    "two roots": _second_root,
    "parent outside the bundle": _outside_parent,
    "root's parent inside": _root_parent_inside,
    "own parent": _self_parent,
}


@pytest.mark.parametrize("mutate", SCHEMA_REFUSALS.values(), ids=SCHEMA_REFUSALS.keys())
def test_schema_refusals(mutate: Mutation) -> None:
    document = manifest_of(family()).as_mapping()
    mutate(document)
    with pytest.raises(BundleError):
        BundleManifest.from_json(json.dumps(document).encode())


RAW_REFUSALS: dict[str, bytes] = {
    "array": b"[]",
    "number": b"1",
    "string": b'"bundle"',
    "null": b"null",
    "not JSON": b"{bundle",
    "empty": b"",
    "not UTF-8": b'{"format": "\xff"}',
    "deeply nested": b"[" * 100_000 + b"]" * 100_000,
    "huge integer": b'{"format_version": ' + b"1" * 5000 + b"}",
    "NaN": b'{"format_version": NaN}',
    "Infinity": b'{"format_version": Infinity}',
}


@pytest.mark.parametrize("data", RAW_REFUSALS.values(), ids=RAW_REFUSALS.keys())
def test_raw_refusals(data: bytes) -> None:
    with pytest.raises(BundleError):
        BundleManifest.from_json(data)


def test_duplicate_keys_are_refused() -> None:
    data = manifest_of(family()).to_json()
    assert data.startswith(b'{"created_at"')
    with pytest.raises(BundleError, match="duplicate"):
        BundleManifest.from_json(b'{"created_at":"x",' + data[1:])


def test_direct_construction_is_validated() -> None:
    member = member_of(family()[0], None)
    with pytest.raises(BundleError):
        BundleManifest(uuid.uuid4().hex, SOURCE, CREATED, None, None, ())
    with pytest.raises(BundleError):
        BundleManifest(uuid.uuid4().hex, SOURCE, CREATED, None, None, [member])  # type: ignore[arg-type]
    with pytest.raises(BundleError):
        BundleMember(member.job_id, member.job_key, "project/0", "ready", 500, None)  # type: ignore[arg-type]
    with pytest.raises(BundleError):
        BundleMember(member.job_id, member.job_key, PurePosixPath("/abs"), "ready", 500, None)
    # A manifest whose text is not canonical still parses, and re-encodes canonically.
    document = manifest_of(family()).as_mapping()
    text = json.dumps(document, indent=2).encode()
    assert (
        BundleManifest.from_json(text).to_json()
        == json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
    )
    assert document["format"] == BUNDLE_FORMAT and document["format_version"] == BUNDLE_VERSION


# -- bundle.json on disk --------------------------------------------------------------------------------------------


def test_bundle_json_must_be_a_bounded_regular_file(tmp_path: Path) -> None:
    bundle = tmp_path / "b"
    manifest, _ = write_bundle(bundle)
    assert validate_bundle(bundle, untrusted=True) == manifest
    target = bundle / "bundle.json"
    data = target.read_bytes()
    # Oversized: valid JSON padded beyond the bound.
    target.write_bytes(data + b" " * (MAX_MANIFEST_BYTES + 1 - len(data)))
    with pytest.raises(BundleError, match="larger"):
        validate_bundle(bundle, untrusted=False)
    target.write_bytes(data + b" " * (MAX_MANIFEST_BYTES - len(data)))
    assert validate_bundle(bundle, untrusted=False) == manifest
    # A symlink to a valid manifest.
    (tmp_path / "real.json").write_bytes(data)
    target.unlink()
    target.symlink_to(tmp_path / "real.json")
    for untrusted in (False, True):
        with pytest.raises(BundleError, match="symlink"):
            validate_bundle(bundle, untrusted=untrusted)
    # Missing, a directory, and not an object.
    target.unlink()
    with pytest.raises(BundleError, match="missing"):
        validate_bundle(bundle, untrusted=False)
    target.mkdir()
    with pytest.raises(BundleError):
        validate_bundle(bundle, untrusted=False)
    target.rmdir()
    target.write_bytes(b"[]")
    with pytest.raises(BundleError, match="object"):
        validate_bundle(bundle, untrusted=False)


def test_fifo_bundle_json_is_refused_without_blocking(tmp_path: Path) -> None:
    bundle = tmp_path / "b"
    write_bundle(bundle)
    (bundle / "bundle.json").unlink()
    os.mkfifo(bundle / "bundle.json")
    with no_hang():
        for untrusted in (False, True):
            with pytest.raises(BundleError, match="special"):
                validate_bundle(bundle, untrusted=untrusted)


def test_the_bundle_root_must_be_a_directory(tmp_path: Path) -> None:
    bundle = tmp_path / "b"
    write_bundle(bundle)
    (tmp_path / "link").symlink_to(bundle)
    with pytest.raises(BundleError, match="root"):
        validate_bundle(tmp_path / "link", untrusted=False)


# -- the structure --------------------------------------------------------------------------------------------------


def _replace_with_directory(path: Path) -> None:
    path.unlink()
    path.mkdir()


def _member(bundle: Path, index: int) -> Path:
    manifest = BundleManifest.from_json((bundle / "bundle.json").read_bytes())
    return manifest.member_dir(bundle, manifest.members[index])


def _rewrite_job(index: int, **changes: object) -> Callable[[Path, list[dict[str, object]]], object]:
    def mutate(bundle: Path, jobs: list[dict[str, object]]) -> None:
        mapping = dict(jobs[index], **changes)
        (_member(bundle, index) / "job.json").write_bytes(JobDefinition.from_mapping(mapping).encode())

    return mutate


def _rewrite_parent(index: int, **changes: object) -> Callable[[Path, list[dict[str, object]]], object]:
    # job_key=None: keep the parent's id but give its key another tag.
    def mutate(bundle: Path, jobs: list[dict[str, object]]) -> None:
        link = jobs[index]["parent"]
        assert isinstance(link, dict)
        update = dict(changes)
        if "job_key" in update and update["job_key"] is None:
            update["job_key"] = f"other--{link['job_id']}"
        _rewrite_job(index, parent=dict(link, **update))(bundle, jobs)

    return mutate


def _rewrite_manifest(mutate: Mutation) -> Callable[[Path, list[dict[str, object]]], object]:
    def rewrite(bundle: Path, jobs: list[dict[str, object]]) -> None:
        document = json.loads((bundle / "bundle.json").read_bytes())
        mutate(document)
        (bundle / "bundle.json").write_text(json.dumps(document))

    return rewrite


def _move_aside(relative: Callable[[Path], Path]) -> Callable[[Path, list[dict[str, object]]], object]:
    # Replace an entry with a symlink to its moved-away content.
    def mutate(bundle: Path, jobs: list[dict[str, object]]) -> None:
        target = relative(bundle)
        aside = bundle.parent / f"aside-{target.name}"
        target.rename(aside)
        target.symlink_to(aside)

    return mutate


def _extra_member(bundle: Path, jobs: list[dict[str, object]]) -> None:
    stray = job_mapping(WORKFLOW, {"start": "succeed"}, tag="stray", placement="project/0")
    directory = bundle / "jobs" / "project" / "0" / key_of(stray)
    directory.mkdir()
    (directory / "job.json").write_bytes(JobDefinition.from_mapping(stray).encode())


def _hard_link(bundle: Path, jobs: list[dict[str, object]]) -> None:
    os.link(_member(bundle, 1) / "input.txt", _member(bundle, 1) / "again.txt")


def _fifo_job(bundle: Path, jobs: list[dict[str, object]]) -> None:
    (_member(bundle, 2) / "job.json").unlink()
    os.mkfifo(_member(bundle, 2) / "job.json")


def _write(relative: str, data: bytes = b"") -> Callable[[Path, list[dict[str, object]]], object]:
    def mutate(bundle: Path, jobs: list[dict[str, object]]) -> None:
        (bundle / relative).parent.mkdir(parents=True, exist_ok=True)
        (bundle / relative).write_bytes(data)

    return mutate


def _job_bytes(index: int, data: bytes) -> Callable[[Path, list[dict[str, object]]], object]:
    def mutate(bundle: Path, jobs: list[dict[str, object]]) -> None:
        (_member(bundle, index) / "job.json").write_bytes(data)

    return mutate


STRUCTURE_REFUSALS: dict[str, Callable[[Path, list[dict[str, object]]], object]] = {
    "extra top-level file": _write("README"),
    "extra top-level directory": lambda bundle, jobs: (bundle / "more").mkdir(),
    "client rekey.json": _write("rekey.json", b"{}"),
    "extra file in jobs": _write("jobs/notes.txt"),
    "extra placement directory": lambda bundle, jobs: (bundle / "jobs" / "project" / "1").mkdir(),
    "extra file in a placement directory": _write("jobs/project/0/stray.txt"),
    "extra member directory": _extra_member,
    "missing member directory": lambda bundle, jobs: shutil.rmtree(_member(bundle, 2)),
    "missing placement directory": lambda bundle, jobs: shutil.rmtree(bundle / "jobs" / "elsewhere"),
    "missing jobs": lambda bundle, jobs: shutil.rmtree(bundle / "jobs"),
    "missing job.json": lambda bundle, jobs: (_member(bundle, 1) / "job.json").unlink(),
    "job.json a directory": lambda bundle, jobs: _replace_with_directory(_member(bundle, 1) / "job.json"),
    "member directory a symlink": _move_aside(lambda bundle: _member(bundle, 2)),
    "placement directory a symlink": _move_aside(lambda bundle: bundle / "jobs" / "elsewhere"),
    "jobs a symlink": _move_aside(lambda bundle: bundle / "jobs"),
    "job.json a symlink": _move_aside(lambda bundle: _member(bundle, 0) / "job.json"),
    "job.json a FIFO": _fifo_job,
    "job.json not JSON": _job_bytes(1, b"{job"),
    "job.json deeply nested": _job_bytes(1, b"[" * 100_000 + b"]" * 100_000),
    "job.json huge integer": _job_bytes(1, b'{"priority": ' + b"9" * 5000 + b"}"),
    "job.json too large": _job_bytes(1, b" " * ((1 << 20) + 1)),
    "job.json id": _rewrite_job(1, id=str(uuid.uuid4())),
    "job.json tag": _rewrite_job(1, tag="renamed"),
    "job.json placement": _rewrite_job(2, placement="project/9"),
    "job.json parent id": _rewrite_parent(2, job_id=OUTSIDER, job_key=OUTSIDER),
    "job.json parent key": _rewrite_parent(2, job_key=None),
    "job.json parent placement": _rewrite_parent(2, placement="project/9"),
    "job.json without a parent": _rewrite_job(1, parent=None),
    "duplicate id": _rewrite_manifest(_duplicate_member),
    "parent not earlier": _rewrite_manifest(_swap_members),
    "two roots": _rewrite_manifest(_second_root),
}


@pytest.mark.parametrize("untrusted", [False, True], ids=["trusted", "untrusted"])
@pytest.mark.parametrize("mutate", STRUCTURE_REFUSALS.values(), ids=STRUCTURE_REFUSALS.keys())
def test_structure_refusals(
    tmp_path: Path, mutate: Callable[[Path, list[dict[str, object]]], object], untrusted: bool
) -> None:
    bundle = tmp_path / "b"
    _, jobs = write_bundle(bundle)
    mutate(bundle, jobs)
    before = snapshot(tmp_path)
    with no_hang(), pytest.raises(BundleError):
        validate_bundle(bundle, untrusted=untrusted)
    # Validation is read-only.
    assert snapshot(tmp_path) == before


# -- untrusted rules ------------------------------------------------------------------------------------------------

NOT_FRESH: dict[str, str] = {
    "state.json": "state.json",
    "logs": "logs/runlog.jsonl",
    "attempts": "attempts/a/outcome.json",
    "seal": ".httk-job/seal.json",
    "empty logs": "logs/",
}


@pytest.mark.parametrize("relative", NOT_FRESH.values(), ids=NOT_FRESH.keys())
def test_untrusted_members_must_be_fresh(tmp_path: Path, relative: str) -> None:
    bundle = tmp_path / "b"
    manifest, _ = write_bundle(bundle)
    target = _member(bundle, 1) / relative
    if relative.endswith("/"):
        target.mkdir()
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}")
    with pytest.raises(BundleError, match="fresh"):
        validate_bundle(bundle, untrusted=True)
    assert validate_bundle(bundle, untrusted=False) == manifest


def test_a_not_fresh_name_deeper_in_the_payload_is_payload(tmp_path: Path) -> None:
    bundle = tmp_path / "b"
    manifest, _ = write_bundle(bundle)
    (_member(bundle, 1) / "inputs").mkdir()
    (_member(bundle, 1) / "inputs" / "state.json").write_text("{}")
    assert validate_bundle(bundle, untrusted=True) == manifest


@pytest.mark.parametrize("state", ["waiting", "paused", "failed", "succeeded", "cancelled"])
def test_untrusted_members_must_be_ready(tmp_path: Path, state: str) -> None:
    jobs = family()
    manifest = manifest_of(jobs, ("ready", state, "ready"))
    write_bundle(tmp_path / "b", jobs, manifest)
    with pytest.raises(BundleError, match="ready"):
        validate_bundle(tmp_path / "b", untrusted=True)
    assert validate_bundle(tmp_path / "b", untrusted=False) == manifest


def test_untrusted_root_may_not_have_a_parent(tmp_path: Path) -> None:
    outside = job_mapping(WORKFLOW, {"start": "succeed"}, tag="outside", placement="up")
    jobs = family()
    jobs[0]["parent"] = parent_link(outside)
    # The manifest root names the outside parent: trusted only.
    base = manifest_of(jobs)
    root = dataclasses.replace(base.members[0], parent_job_id=str(outside["id"]))
    manifest = BundleManifest(base.transfer_id, SOURCE, CREATED, None, None, (root, *base.members[1:]))
    write_bundle(tmp_path / "named", jobs, manifest)
    assert validate_bundle(tmp_path / "named", untrusted=False) == manifest
    with pytest.raises(BundleError, match="parent"):
        validate_bundle(tmp_path / "named", untrusted=True)
    # The manifest root has none but its job.json does: trusted only.
    write_bundle(tmp_path / "hidden", jobs, base)
    assert validate_bundle(tmp_path / "hidden", untrusted=False) == base
    with pytest.raises(BundleError, match="parent"):
        validate_bundle(tmp_path / "hidden", untrusted=True)
    # A manifest root naming another parent than its job.json is refused even when trusted.
    other = BundleMember(root.job_id, root.job_key, root.placement, "ready", 500, str(uuid.uuid4()))
    write_bundle(
        tmp_path / "wrong",
        jobs,
        BundleManifest(base.transfer_id, SOURCE, CREATED, None, None, (other, *base.members[1:])),
    )
    with pytest.raises(BundleError, match="parent"):
        validate_bundle(tmp_path / "wrong", untrusted=False)


@pytest.mark.parametrize("where", ["payload", "deep payload", "dangling"])
def test_payload_symlinks_are_trusted_only(tmp_path: Path, where: str) -> None:
    bundle = tmp_path / "b"
    manifest, _ = write_bundle(bundle)
    member = _member(bundle, 2)
    link = {
        "payload": member / "link",
        "deep payload": member / "run" / "a" / "b" / "link",
        "dangling": member / "dangling",
    }[where]
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to("/etc/passwd" if where != "dangling" else "missing")
    with pytest.raises(BundleError, match="symlink"):
        validate_bundle(bundle, untrusted=True)
    assert validate_bundle(bundle, untrusted=False) == manifest


def test_walk_limits_are_enforced(tmp_path: Path) -> None:
    bundle = tmp_path / "b"
    manifest, _ = write_bundle(bundle)
    entries = len(list(bundle.rglob("*")))
    assert validate_bundle(bundle, untrusted=True, limits=WalkLimits(entries=entries)) == manifest
    with pytest.raises(BundleError, match="entries"):
        validate_bundle(bundle, untrusted=True, limits=WalkLimits(entries=entries - 1))
    depth = max(len(path.relative_to(bundle).parts) for path in bundle.rglob("*"))
    assert validate_bundle(bundle, untrusted=True, limits=WalkLimits(depth=depth)) == manifest
    with pytest.raises(BundleError, match="deeper"):
        validate_bundle(bundle, untrusted=True, limits=WalkLimits(depth=depth - 1))


def test_trusted_payloads_are_job_content(tmp_path: Path) -> None:
    # A trusted bundle carries complete jobs: hard links, special files, many and deep entries in a payload are
    # the job's business; only the structural positions are checked.
    bundle = tmp_path / "b"
    manifest, jobs = write_bundle(bundle)
    _hard_link(bundle, jobs)
    with pytest.raises(BundleError, match="hard-linked"):
        validate_bundle(bundle, untrusted=True)
    os.mkfifo(_member(bundle, 0) / "pipe")
    deep = _member(bundle, 2).joinpath(*["d"] * 8)
    deep.mkdir(parents=True)
    tiny = WalkLimits(entries=1, depth=1)
    with no_hang():
        assert validate_bundle(bundle, untrusted=False, limits=tiny) == manifest
        with pytest.raises(BundleError, match="special"):
            validate_bundle(bundle, untrusted=True)
    (_member(bundle, 0) / "pipe").unlink()
    (_member(bundle, 1) / "again.txt").unlink()
    with pytest.raises(BundleError, match="entries"):
        validate_bundle(bundle, untrusted=True, limits=tiny)


@pytest.mark.parametrize("where", ["top", "jobs", "placement"])
def test_trusted_structural_positions_refuse_special_files(tmp_path: Path, where: str) -> None:
    bundle = tmp_path / "b"
    write_bundle(bundle)
    directory = {"top": bundle, "jobs": bundle / "jobs", "placement": bundle / "jobs" / "elsewhere"}[where]
    os.mkfifo(directory / "pipe")
    with no_hang(), pytest.raises(BundleError, match="not listed"):
        validate_bundle(bundle, untrusted=False)


# -- rekeying -------------------------------------------------------------------------------------------------------


def rekeyed_id(workspace_id: str, job_id: object) -> str:
    return str(uuid.uuid5(_EXCHANGE_NAMESPACE, f"{workspace_id}/{job_id}"))


def inbox(tmp_path: Path, name: str, jobs: list[dict[str, object]] | None = None) -> tuple[Path, Path, BundleManifest]:
    scratch = tmp_path / name
    bundle = scratch / "entry"
    write_bundle(bundle, jobs if jobs is not None else family(str(uuid.uuid4())))
    return scratch, bundle, validate_bundle(bundle, untrusted=True)


def test_rekey(tmp_path: Path) -> None:
    jobs = family()
    scratch, bundle, manifest = inbox(tmp_path, "s", jobs)
    assert read_rekeyed(scratch) is None and exchange_names(scratch) == {}
    rekeyed = rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    new_ids = [rekeyed_id(ADOPTER, mapping["id"]) for mapping in jobs]
    assert [member.job_id for member in rekeyed.members] == new_ids
    assert [member.job_key for member in rekeyed.members] == [
        f"root--{new_ids[0]}",
        f"child--{new_ids[1]}",
        new_ids[2],
    ]
    assert [member.parent_job_id for member in rekeyed.members] == [None, new_ids[0], new_ids[1]]
    assert [(m.placement, m.state, m.priority) for m in rekeyed.members] == [
        (m.placement, m.state, m.priority) for m in manifest.members
    ]
    assert (rekeyed.transfer_id, rekeyed.created_at, rekeyed.source_workspace_id) == (
        manifest.transfer_id,
        manifest.created_at,
        manifest.source_workspace_id,
    )
    assert exchange_names(scratch) == {new: str(mapping["id"]) for new, mapping in zip(new_ids, jobs, strict=True)}
    assert read_rekeyed(scratch) == rekeyed
    assert validate_bundle(bundle, untrusted=False) == rekeyed
    assert validate_bundle(bundle, untrusted=True) == rekeyed
    assert (bundle / "bundle.json").read_bytes() == rekeyed.to_json()
    for old, new in zip(manifest.members, rekeyed.members, strict=True):
        assert not manifest.member_dir(bundle, old).exists()
        directory = rekeyed.member_dir(bundle, new)
        assert (directory / "input.txt").read_text() == f"input of {old.job_key}"
        job = JobDefinition.from_path(directory / "job.json")
        assert (job.id, job.tag) == (new.job_id, JobDefinition.from_mapping(jobs[rekeyed.members.index(new)]).tag)
    child = JobDefinition.from_path(rekeyed.member_dir(bundle, rekeyed.members[1]) / "job.json")
    assert child.parent is not None
    original_link = jobs[1]["parent"]
    assert isinstance(original_link, dict)
    assert dict(child.parent) == {
        "workspace_id": ADOPTER,
        "job_id": new_ids[0],
        "job_key": f"root--{new_ids[0]}",
        "placement": "project/0",
        "activation_id": original_link["activation_id"],
        "spawn_id": original_link["spawn_id"],
    }
    # The record lives outside the bundle tree.
    assert sorted(path.name for path in scratch.iterdir()) == ["entry", "rekey.json"]


def test_rekey_is_deterministic_per_workspace(tmp_path: Path) -> None:
    jobs = family()
    results = []
    for name, workspace_id in (("a", ADOPTER), ("b", ADOPTER), ("c", OTHER_ADOPTER)):
        scratch, bundle, manifest = inbox(tmp_path, name, jobs)
        results.append((rekey_untrusted(scratch, bundle, manifest, workspace_id=workspace_id), snapshot(scratch)))
    assert results[0] == results[1]
    assert not {m.job_id for m in results[0][0].members} & {m.job_id for m in results[2][0].members}


class Crash(Exception):
    """A simulated crash."""


def crash_at(at_op: str, at_phase: str, name: str, nth: int = 1) -> _fs.Fault:
    seen = [0]

    def fault(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        if (op, phase) == (at_op, at_phase) and dst is not None and dst.name() == name:
            seen[0] += 1
            if seen[0] == nth:
                raise Crash(f"{op} {phase} {name}")

    return fault


CRASHES: dict[str, Callable[[BundleManifest], _fs.Fault]] = {
    "after rekey.json, before any job.json": lambda m: crash_at("write", "before_replace", "job.json"),
    "after the first job.json, before its move": lambda m: crash_at("rename", "before", m.members[0].job_key),
    "after the first move": lambda m: crash_at("rename", "after", m.members[0].job_key),
    "after the second move": lambda m: crash_at("rename", "after", m.members[1].job_key),
    "after every move, before bundle.json": lambda m: crash_at("write", "before_replace", "bundle.json"),
}


@pytest.mark.parametrize("crash", CRASHES.values(), ids=CRASHES.keys())
def test_rekey_resumes_after_a_crash(tmp_path: Path, crash: Callable[[BundleManifest], _fs.Fault]) -> None:
    jobs = family()
    reference_scratch, reference_bundle, reference_manifest = inbox(tmp_path, "reference", jobs)
    reference = rekey_untrusted(reference_scratch, reference_bundle, reference_manifest, workspace_id=ADOPTER)
    scratch, bundle, manifest = inbox(tmp_path, "crashed", jobs)
    # The rekeyed keys name the moves' destinations.
    _fs.set_fault_injector(crash(reference))
    with pytest.raises(Crash):
        rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    _fs.set_fault_injector(None)
    assert (scratch / "rekey.json").is_file()
    # The resume contract: rekey.json exists, so finish with the manifest bundle.json holds, then validate trusted.
    resumed = rekey_untrusted(
        scratch, bundle, BundleManifest.from_json((bundle / "bundle.json").read_bytes()), workspace_id=ADOPTER
    )
    assert resumed == reference == read_rekeyed(scratch)
    assert validate_bundle(bundle, untrusted=False) == reference
    leftovers = [path for path in scratch.rglob(".*.tmp")]
    for path in leftovers:
        path.unlink()
    assert snapshot(scratch) == snapshot(reference_scratch)


def test_rekey_is_idempotent_once_finished(tmp_path: Path) -> None:
    scratch, bundle, manifest = inbox(tmp_path, "s")
    rekeyed = rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    after = snapshot(scratch)
    assert rekey_untrusted(scratch, bundle, rekeyed, workspace_id=ADOPTER) == rekeyed
    assert rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER) == rekeyed
    assert snapshot(scratch) == after


def test_rekey_refusals(tmp_path: Path) -> None:
    scratch, bundle, manifest = inbox(tmp_path, "s")
    with pytest.raises(ValueError):
        rekey_untrusted(tmp_path, bundle, manifest, workspace_id=ADOPTER)
    with pytest.raises(BundleError):
        rekey_untrusted(scratch, bundle, manifest, workspace_id="not-a-uuid")
    rekeyed = rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    # The record was derived for another workspace.
    with pytest.raises(BundleError, match="workspace"):
        rekey_untrusted(scratch, bundle, rekeyed, workspace_id=OTHER_ADOPTER)
    # The record belongs to another bundle.
    other = manifest_of(family())
    with pytest.raises(BundleError, match="another bundle"):
        rekey_untrusted(scratch, bundle, other, workspace_id=ADOPTER)
    # A member missing under both keys.
    shutil.rmtree(rekeyed.member_dir(bundle, rekeyed.members[2]))
    with pytest.raises(BundleError, match="missing"):
        rekey_untrusted(scratch, bundle, rekeyed, workspace_id=ADOPTER)


@pytest.mark.parametrize("data", [b"{job", b"[" * 100_000 + b"]" * 100_000], ids=["not JSON", "deeply nested"])
def test_rekey_resumption_rereads_job_json_strictly(tmp_path: Path, data: bytes) -> None:
    jobs = family()
    scratch, bundle, manifest = inbox(tmp_path, "s", jobs)
    _fs.set_fault_injector(crash_at("write", "before_replace", "job.json"))
    with pytest.raises(Crash):
        rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    _fs.set_fault_injector(None)
    # An open client descriptor changed a payload after validation.
    (manifest.member_dir(bundle, manifest.members[0]) / "job.json").write_bytes(data)
    with pytest.raises(BundleError, match="invalid"):
        rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)


def test_rekey_refuses_a_client_id_that_collides_with_a_rekeyed_id(tmp_path: Path) -> None:
    jobs = family()
    # The grandchild takes the id its parent would be rekeyed to.
    jobs[2]["id"] = rekeyed_id(ADOPTER, jobs[1]["id"])
    scratch, bundle, manifest = inbox(tmp_path, "s", jobs)
    before = snapshot(scratch)
    with pytest.raises(BundleError, match="collides"):
        rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    assert snapshot(scratch) == before
    # In another workspace the same ids are harmless.
    rekey_untrusted(scratch, bundle, manifest, workspace_id=OTHER_ADOPTER)


def test_rekey_refuses_a_bundle_named_like_its_record(tmp_path: Path) -> None:
    scratch = tmp_path / "s"
    write_bundle(scratch / "rekey.json")
    manifest = validate_bundle(scratch / "rekey.json", untrusted=True)
    with pytest.raises(BundleError):
        rekey_untrusted(scratch, scratch / "rekey.json", manifest, workspace_id=ADOPTER)


def _bad_uuid_name(record: dict[str, object]) -> None:
    names = record["exchange_names"]
    assert isinstance(names, dict)
    names[next(iter(names))] = "x"


def _repeat_name(record: dict[str, object]) -> None:
    names = record["exchange_names"]
    assert isinstance(names, dict)
    first, second = list(names)[:2]
    names[second] = names[first]


REKEY_RECORD_REFUSALS: dict[str, Callable[[dict[str, object]], object]] = {
    "unknown key": _set("extra", value=1),
    "missing key": _delete("exchange_names"),
    "format": _set("format", value=BUNDLE_FORMAT),
    "version": _set("format_version", value=2),
    "version boolean": _set("format_version", value=True),
    "manifest": _set("manifest", "members", value=[]),
    "names not an object": _set("exchange_names", value=[]),
    "names incomplete": lambda record: record["exchange_names"].popitem(),  # type: ignore[attr-defined]
    "names bad uuid": _bad_uuid_name,
    "names repeat a client id": _repeat_name,
}


@pytest.mark.parametrize("mutate", REKEY_RECORD_REFUSALS.values(), ids=REKEY_RECORD_REFUSALS.keys())
def test_malformed_rekey_records_are_refused(tmp_path: Path, mutate: Callable[[dict[str, object]], object]) -> None:
    scratch, bundle, manifest = inbox(tmp_path, "s")
    rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    record = json.loads((scratch / "rekey.json").read_bytes())
    mutate(record)
    (scratch / "rekey.json").write_text(json.dumps(record))
    for read in (read_rekeyed, exchange_names):
        with pytest.raises(BundleError):
            read(scratch)


def test_a_symlinked_rekey_record_is_refused(tmp_path: Path) -> None:
    scratch, bundle, manifest = inbox(tmp_path, "s")
    rekey_untrusted(scratch, bundle, manifest, workspace_id=ADOPTER)
    (scratch / "rekey.json").rename(tmp_path / "record.json")
    (scratch / "rekey.json").symlink_to(tmp_path / "record.json")
    with pytest.raises(BundleError):
        read_rekeyed(scratch)
