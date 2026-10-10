"""Job directories never nest: no placement component may parse as a job key."""

from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import Workspace
from httk.workflow.errors import FormatError
from httk.workflow.models import check_job_placement

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


def test_job_new_still_accepts_ordinary_placements(tmp_path: Path) -> None:
    from attempt_fixtures import new_job
    from test_runner_builds import _compiled_package

    workspace = Workspace.initialize(tmp_path / "workspace")
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
