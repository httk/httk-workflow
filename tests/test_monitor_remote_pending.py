"""The monitor's remote page relay through the far side's ``job list`` (split out of ``test_monitor.py``)."""

from pathlib import Path

from httk.core.cli import CLIContext

from conftest import fake_remote, register_ws
from httk.workflow import Workspace
from httk.workflow.monitor.data import WorkspaceView
from httk.workflow.projects import initialize_project
from test_job_paging import _job as _marker


def test_monitor_remote_page_uses_json_relay(tmp_path: Path, remote: object) -> None:
    """A remote view parses the existing JSON job-list protocol."""

    project = tmp_path / "project"
    initialize_project(project, name="monitor-remote")
    fake_remote(project)
    root = remote.root / "runs" / "workspace"  # type: ignore[attr-defined]
    workspace = Workspace.initialize(root)
    _marker(workspace, "ready", "jobs")
    context = CLIContext("httk", project)
    register_ws(context, root, "station")
    view = WorkspaceView("cluster:station", context)

    page = view.page(limit=5)
    assert len(page.jobs) == 1
    assert page.jobs[0]["state"] == "ready"
