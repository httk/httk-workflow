"""Initialize an httk workflow store."""

from pathlib import Path

from httk.workflow import WorkflowStore

store = WorkflowStore.initialize(Path("example-workflow-store"))
print(f"initialized workflow store {store.store_id} at {store.root}")
