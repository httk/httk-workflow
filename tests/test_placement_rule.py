"""Job directories never nest: no placement component may parse as a job key."""

import json
from pathlib import Path, PurePosixPath

import pytest

from conftest import configure_identity
from httk.workflow import Workspace
from httk.workflow.errors import FormatError
from httk.workflow.models import check_job_placement
from httk.workflow.transfers import TRANSFER_DIRECTORY, TRANSFER_MANIFEST, adopt_job, validate_bundle
from test_eject_adopt import _payload

# Fixed, so parametrized test ids agree between xdist workers.
_JOB_ID = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"


@pytest.fixture(params=[_JOB_ID, f"parent--{_JOB_ID}"], ids=["bare", "tagged"])
def nesting(request: pytest.FixtureRequest) -> str:
    """A placement whose middle component names a job directory."""

    return f"project/{request.param}/children"


def test_check_job_placement_refuses_a_job_key_component_and_names_the_rule(nesting: str) -> None:
    component = PurePosixPath(nesting).parts[1]
    with pytest.raises(FormatError, match="must not name a job directory") as refused:
        check_job_placement(PurePosixPath(nesting))
    assert repr(component) in str(refused.value)


@pytest.mark.parametrize(
    "placement",
    [
        "project/children",
        "jobs",
        # Near misses: not a UUID, an uppercase UUID, a single-dash separator.
        "project/parent--not-a-uuid",
        f"project/{_JOB_ID.upper()}",
        f"project/tag-{_JOB_ID}",
    ],
)
def test_check_job_placement_accepts_ordinary_placements(placement: str) -> None:
    check_job_placement(PurePosixPath(placement))


def test_submit_refuses_a_nesting_placement_before_copying_anything(tmp_path: Path, nesting: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(FormatError, match="must not name a job directory"):
        workspace.submit(_payload(tmp_path / "payloads"), nesting)
    assert not list(workspace.scan_markers())
    assert not (workspace.root / "project").exists()
    assert not list((workspace.control / "tmp").glob("submit.*"))


def test_submit_and_job_new_still_accept_ordinary_placements(tmp_path: Path) -> None:
    from attempt_fixtures import new_job
    from test_runner_builds import _compiled_package

    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(_payload(tmp_path / "payloads"), "project/children")
    assert marker.placement == PurePosixPath("project/children")
    package = _compiled_package(tmp_path)
    job = new_job(workspace, package, placement="project/children")
    assert job.payload.parent == workspace.jobs / "ready" / "project" / "children"


def test_job_new_refuses_a_nesting_placement(tmp_path: Path, nesting: str) -> None:
    from attempt_fixtures import every_job, new_job
    from test_runner_builds import _compiled_package

    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(FormatError, match="must not name a job directory"):
        new_job(workspace, _compiled_package(tmp_path), placement=nesting)
    assert every_job(workspace) == []


def test_import_refuses_a_bundle_whose_destination_placement_nests(tmp_path: Path, nesting: str) -> None:
    configure_identity()
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)
    # The source refuses to seal such a placement itself, so it is written into the manifest.
    manifest_path = bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["destination_placement"] = nesting
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(FormatError, match="must not name a job directory"):
        destination.import_bundle(bundle)
    assert not list(destination.scan_markers())
    assert not (destination.root / "project").exists()
    # The bundle is untouched (the strict manifest check refuses its placement anywhere).
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest
    with pytest.raises(FormatError, match="must not name a job directory"):
        validate_bundle(bundle)


def _ejected(tmp_path: Path) -> tuple[Workspace, Path, str]:
    configure_identity()
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    return destination, source.eject(marker.job_id, tmp_path / "loose"), marker.job_id


def test_adopt_refuses_a_nesting_manifest_placement_and_leaves_the_directory(tmp_path: Path, nesting: str) -> None:
    destination, loose, _job_id = _ejected(tmp_path)
    manifest_path = loose / TRANSFER_DIRECTORY / TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["destination_placement"] = nesting
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(FormatError, match="must not name a job directory"):
        destination.adopt(loose)
    assert loose.is_dir() and json.loads(manifest_path.read_text(encoding="utf-8"))["destination_placement"] == nesting
    assert not list(destination.scan_markers())
    assert not list((destination.control / "transfers").rglob("*.json"))


def test_adopt_refuses_a_nesting_placement_override(tmp_path: Path, nesting: str) -> None:
    destination, loose, _job_id = _ejected(tmp_path)
    with pytest.raises(FormatError, match="must not name a job directory"):
        adopt_job(destination, loose, placement=nesting)
    assert loose.is_dir()
    assert not list(destination.scan_markers())


def test_adopt_accepts_an_ordinary_placement_override(tmp_path: Path) -> None:
    destination, loose, job_id = _ejected(tmp_path)
    adopted = adopt_job(destination, loose, placement="project/children")
    assert adopted.job_id == job_id and adopted.placement == PurePosixPath("project/children")


def test_detach_refuses_a_nesting_destination_placement_before_fencing(tmp_path: Path, nesting: str) -> None:
    configure_identity()
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    with pytest.raises(FormatError, match="must not name a job directory"):
        source.detach(marker.job_id, destination_workspace_id=destination.workspace_id, destination_placement=nesting)
    # Nothing was fenced or sealed: the job is still where and as it was.
    assert source.find_marker_by_id(marker.job_id) == marker
    assert not list(source.scan_markers(("transferring",)))
    assert not (source.payload_path(marker.placement, marker.job_key) / TRANSFER_DIRECTORY).exists()
