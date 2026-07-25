"""Initialize an httk workflow workspace."""

from pathlib import Path

from httk.workflow import WorkflowWorkspace

workspace = WorkflowWorkspace.initialize(Path("example-workflow-workspace"))
print(f"initialized workflow workspace {workspace.workspace_id} at {workspace.root}")
