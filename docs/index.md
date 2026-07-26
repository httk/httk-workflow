# *httk-workflow*

This site documents the *httk-workflow* module. For the full documentation of
*httk₂*, see [docs.httk.org](https://docs.httk.org).

The module implements a recoverable, language-neutral workflow protocol whose
source of truth is a single atomically renamed state marker per job. Python
applications use `httk.workflow`; installations also provide the
`httk-taskmanager` and `httk-v1-taskmanager` commands.

```{admonition} Quick links
:class: tip

- **Start here**: {doc}`quickstart` — five commands to a finished relaxation
- **API reference**: {doc}`reference/index`
- **Task-manager usage**: {doc}`taskmanager`
- **Native runner helpers**: {doc}`runtime_helpers`
- **Native Bash API**: {doc}`native_bash_api`
- **Packaged VASP runners**: {doc}`vasp_runners`
- **Harvesting results**: {doc}`harvest`
- **Project and workflow CLI**: {doc}`workflow_cli`
- **Workflow filesystem API**: {doc}`workflow_filesystem_api`
- **Examples notebook**: {doc}`notebooks/examples`
- [*httk* v1 migration guide](httk_v1_migration_guide.md)
- [*httk* v1 compatibility](v1_compatibility.md)
````

## Install

Preferably work in a Python virtual environment:

```bash
git clone https://github.com/httk/httk-workflow
cd httk-workflow
python -m pip install -e .
```

## Minimal setup

One workspace, one job of a packaged runner, and one manager that runs it:

```console
httk-taskmanager init workflow-workspace --extension transactional-data-v1
httk workflow job new workflow-workspace --template vasp-relax --from POSCAR --tag silicon
httk-taskmanager run workflow-workspace --until-idle
httk workflow harvest workflow-workspace
```

{doc}`quickstart` walks through exactly those commands, including how to run them
without VASP installed. A complete payload prepared some other way is still
submitted directly:

```console
httk-taskmanager submit workflow-workspace prepared-job --placement project/00
```

```{toctree}
:maxdepth: 2
:caption: Documentation

quickstart
reference/index
taskmanager
runtime_helpers
native_bash_api
vasp_runners
harvest
workflow_cli
workflow_filesystem_api
notebooks/examples
httk_v1_migration_guide
v1_compatibility
```
