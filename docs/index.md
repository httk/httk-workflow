# *httk-workflow*

This site documents the *httk-workflow* module. For the full documentation of
*httk₂*, see [docs.httk.org](https://docs.httk.org).

The module implements a recoverable, language-neutral workflow protocol whose
source of truth is a single atomically renamed state marker per job. Python
applications use `httk.workflow`; installations also provide the
`httk-taskmanager` and `httk-v1-taskmanager` commands.

```{admonition} Quick links
:class: tip

- **API reference**: {doc}`reference/index`
- **Task-manager usage**: {doc}`taskmanager`
- **Native runner helpers**: {doc}`runtime_helpers`
- **Project and workflow CLI**: {doc}`workflow_cli`
- **httk v1 compatibility**: {doc}`v1_compatibility`
- **Workflow filesystem API**: {doc}`workflow_filesystem_api`
- **Examples notebook**: {doc}`notebooks/examples`
````

## Install

Preferably work in a Python virtual environment:

```bash
git clone https://github.com/httk/httk-workflow
cd httk-workflow
python -m pip install -e .
```

## Minimal setup

```python
from httk.workflow import WorkflowStore

store = WorkflowStore.initialize("workflow-store")
print(store.store_id)
```

Then submit a complete job payload and run the installed manager:

```console
httk-taskmanager submit workflow-store prepared-job --placement project/00
httk-taskmanager run workflow-store
```

```{toctree}
:maxdepth: 2
:caption: Documentation

reference/index
taskmanager
runtime_helpers
workflow_cli
v1_compatibility
workflow_filesystem_api
notebooks/examples
```
