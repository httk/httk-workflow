"""Adoption and transfer recovery never trust what a job left in its directory."""

import json
import os
import shutil
import threading
from pathlib import Path

import pytest

from conftest import configure_identity
from httk.workflow import Workspace, _bundle
from httk.workflow.errors import FormatError
from httk.workflow.models import JobDefinition
from httk.workflow.transfers import TRANSFER_DIRECTORY, TRANSFER_MANIFEST, validate_bundle
from test_eject_adopt import _payload


def _pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    configure_identity()
    return Workspace.initialize(tmp_path / "source"), Workspace.initialize(tmp_path / "destination")


def _loose(tmp_path: Path) -> tuple[Workspace, Path]:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    return destination, source.eject(marker.job_id, tmp_path / "loose")


# ---------------------------------------------------------------------------
# Hard links and walk bounds at adoption
# ---------------------------------------------------------------------------


def test_adopt_refuses_a_file_hard_linked_inside_the_directory(tmp_path: Path) -> None:
    destination, loose = _loose(tmp_path)
    (loose / "logs").mkdir(exist_ok=True)
    os.link(loose / "files" / "runner", loose / "logs" / "second-name")
    with pytest.raises(FormatError, match="hard links are not accepted in bundles"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


def test_adopt_refuses_a_hard_link_to_a_file_outside(tmp_path: Path) -> None:
    destination, loose = _loose(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("not the job's\n", encoding="utf-8")
    (loose / "run").mkdir(exist_ok=True)
    os.link(outside, loose / "run" / "borrowed.txt")
    with pytest.raises(FormatError, match=r"hard-linked file run/borrowed\.txt: hard links are not accepted"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())
    assert outside.read_text(encoding="utf-8") == "not the job's\n"


def test_adopt_bounds_the_walk_by_depth_and_entries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination, loose = _loose(tmp_path)
    deep = loose / "run"
    for _ in range(4):
        deep = deep / "d"
    deep.mkdir(parents=True)
    monkeypatch.setattr(_bundle, "_ADOPT_MAX_DEPTH", 3)
    with pytest.raises(FormatError, match="nests deeper than 3 levels"):
        destination.adopt(loose)
    monkeypatch.setattr(_bundle, "_ADOPT_MAX_DEPTH", 256)
    monkeypatch.setattr(_bundle, "_ADOPT_MAX_ENTRIES", 3)
    with pytest.raises(FormatError, match="holds more than 3 entries"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


def test_adopt_still_accepts_an_ordinary_directory(tmp_path: Path) -> None:
    destination, loose = _loose(tmp_path)
    adopted = destination.adopt(loose)
    assert not loose.exists() and destination.find_marker_by_id(adopted.job_id) is not None


# ---------------------------------------------------------------------------
# job.json reads
# ---------------------------------------------------------------------------


def _without_hanging(call: object) -> BaseException | None:
    """Run *call* in a thread; fail the test if it does not return within 10 s."""

    outcome: list[BaseException | None] = []

    def target() -> None:
        try:
            call()  # type: ignore[operator]
        except BaseException as exc:
            outcome.append(exc)
        else:
            outcome.append(None)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(10.0)
    assert not thread.is_alive(), "the read hung"
    return outcome[0]


def test_a_fifo_job_json_fails_load_job_without_hanging(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    document = workspace.payload_path(marker.placement, marker.job_key) / "job.json"
    document.unlink()
    os.mkfifo(document)
    failure = _without_hanging(lambda: workspace.load_job(marker))
    assert isinstance(failure, FormatError) and "not a regular file" in str(failure)


def test_a_symlinked_job_json_is_refused(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    document = workspace.payload_path(marker.placement, marker.job_key) / "job.json"
    elsewhere = tmp_path / "elsewhere.json"
    shutil.copyfile(document, elsewhere)
    document.unlink()
    document.symlink_to(elsewhere)
    with pytest.raises(FormatError, match="cannot read JSON object"):
        workspace.load_job(marker)


def test_an_oversized_job_json_is_refused(tmp_path: Path) -> None:
    payload = _payload(tmp_path / "payloads")
    document = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    (payload / "job.json").write_text(json.dumps(document) + " " * (1024 * 1024), encoding="utf-8")
    with pytest.raises(FormatError, match="larger than 1048576 bytes"):
        JobDefinition.from_path(payload / "job.json")


def test_submit_still_follows_a_symlinked_job_json_in_the_users_source(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _payload(tmp_path / "payloads")
    real = tmp_path / "job.json"
    shutil.move(payload / "job.json", real)
    (payload / "job.json").symlink_to(real)
    marker = workspace.submit(payload, "jobs")
    stored = workspace.payload_path(marker.placement, marker.job_key) / "job.json"
    # The copy dereferenced the link, so the workspace holds a regular file.
    assert not stored.is_symlink() and workspace.load_job(marker).id == marker.job_id


def test_a_moving_submit_refuses_a_symlinked_job_json(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _payload(tmp_path / "payloads")
    real = tmp_path / "job.json"
    shutil.move(payload / "job.json", real)
    (payload / "job.json").symlink_to(real)
    # Moved, the link would stay in the workspace, where the manager never follows one.
    with pytest.raises(FormatError, match="its job.json is a symlink"):
        workspace.submit(payload, "jobs", move=True)
    assert (payload / "job.json").is_symlink() and not list(workspace.scan_markers())


def test_a_moving_submit_refuses_a_fifo_job_json_without_hanging(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload = _payload(tmp_path / "payloads")
    (payload / "job.json").unlink()
    os.mkfifo(payload / "job.json")
    failure = _without_hanging(lambda: workspace.submit(payload, "jobs", move=True))
    assert isinstance(failure, FormatError) and "not a regular file" in str(failure)
    assert payload.is_dir() and not list(workspace.scan_markers())


# ---------------------------------------------------------------------------
# Transfer manifests
# ---------------------------------------------------------------------------


def test_an_oversized_bundle_manifest_is_refused(tmp_path: Path) -> None:
    destination, loose = _loose(tmp_path)
    manifest_path = loose / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    manifest_path.write_text(manifest_path.read_text(encoding="utf-8") + " " * (1024 * 1024), encoding="utf-8")
    with pytest.raises(FormatError, match="larger than 1048576 bytes"):
        validate_bundle(loose)
    with pytest.raises(FormatError, match="larger than 1048576 bytes"):
        destination.adopt(loose)
    assert loose.is_dir() and not list(destination.scan_markers())


@pytest.mark.parametrize("kind", ["fifo", "symlink"])
def test_a_special_bundle_manifest_is_refused_without_hanging(tmp_path: Path, kind: str) -> None:
    _destination, loose = _loose(tmp_path)
    manifest_path = loose / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    copy = tmp_path / "manifest-copy.json"
    shutil.move(manifest_path, copy)
    if kind == "fifo":
        os.mkfifo(manifest_path)
    else:
        manifest_path.symlink_to(copy)
    failure = _without_hanging(lambda: validate_bundle(loose))
    assert isinstance(failure, FormatError)


def test_seal_refuses_a_symlinked_transfer_directory(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    outside = tmp_path / "outside"
    outside.mkdir()
    payload = source.payload_path(marker.placement, marker.job_key)
    (payload / TRANSFER_DIRECTORY).symlink_to(outside, target_is_directory=True)
    with pytest.raises(FormatError, match="is a symlink or not a directory"):
        source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)
    assert not list(outside.iterdir())


def test_seal_refuses_a_transfer_directory_that_is_a_file(tmp_path: Path) -> None:
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    payload = source.payload_path(marker.placement, marker.job_key)
    (payload / TRANSFER_DIRECTORY).write_text("not a directory", encoding="utf-8")
    with pytest.raises(FormatError, match="is a symlink or not a directory"):
        source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)


# ---------------------------------------------------------------------------
# Transfer recovery
# ---------------------------------------------------------------------------


def _plant(workdir: Path, manifest: dict[str, object]) -> list[Path]:
    """Plant transfer manifests at several places inside one job's directory."""

    planted = []
    job_key = str(manifest["job_key"])
    for bundle in (workdir, workdir / "nested", workdir / "deeper" / job_key):
        (bundle / TRANSFER_DIRECTORY).mkdir(parents=True)
        path = bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
        path.write_text(json.dumps(manifest), encoding="utf-8")
        planted.append(bundle)
    return planted
