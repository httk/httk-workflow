# *httk-workflow*

This site documents the *httk-workflow* module. For the full documentation of
*httk₂*, see [docs.httk.org](https://docs.httk.org).

*httk-workflow* runs computational workflows from the filesystem. A
**workspace** holds jobs as directories, each with one atomically renamed state
marker as its source of truth, so an interrupted manager, node or calculation
is resumed from what is on disk rather than cleaned up. A **runner** implements
the steps of a workflow in any language and decides at run time what to spawn
and what runs next; there is no graph language. Managers run the jobs, on the
local machine or through a scheduler, and `collect` hands the finished results
to a data layer such as *httk-store*.

```{admonition} Quick links
:class: tip

- **Quickstart**: {doc}`quickstart`, from an empty directory to a finished
  relaxation in eight commands
- **Setting up workspaces**: {doc}`workspaces`, projects, settings, launchers,
  and remotes
- **Running workflows**: {doc}`running`, creating jobs, managers, transfers,
  collecting, and sealing
- **Writing runners in Python**: {doc}`runtime_helpers`
- **Runner SDKs in other languages**: {doc}`sdks/index`
- **Workflow packages and URIs**: {doc}`workflow_packages`
- **Ready-made VASP workflows**: {doc}`vasp_runners`
- **Supporting a simulation code**: {doc}`code_support`
- **Collecting results**: {doc}`collecting`
- **Provenance, declarations, and stable ids**: {doc}`provenance`
- **CWL, PWD, jobflow, and httk v1 workflows**: {doc}`workflow_compat`
- **Campaigns across many workspaces**: {doc}`campaigns`
- **The command line**: {doc}`workflow_cli`
- **The filesystem protocol**: {doc}`workflow_filesystem_api`
- **Migrating from httk v1**: {doc}`httk_v1_migration_guide`
- **API reference**: {doc}`reference/index`
- **Examples notebook**: {doc}`notebooks/examples`

The topic pages above are short and practical; each links onward to its full
guide in the **Details** section of the sidebar.
```

## Install

Preferably work in a Python virtual environment, then do:

```bash
git clone https://github.com/httk/httk-workflow
cd httk-workflow
python -m pip install -e .
```

## Minimal setup

One workspace, one job of a ready-made VASP workflow referenced by its git URI,
and one manager that runs it:

```console
httk init --name "Your Name" --email you@example.org
httk project init --name quickstart .
httk workspace init --name default .
httk job new --workflow 'git+https://github.com/httk/workflows-vasp#vasp-relax' --input structure=POSCAR --tag silicon
httk workspace settings set --key vasp.command --value "$PWD/examples/mock_vasp.py" default
httk workflow run
httk collect
```

{doc}`quickstart` walks through exactly those commands, including how to run
them without VASP installed.

```{toctree}
:maxdepth: 2
:caption: Documentation

quickstart
workspaces
running
runtime_helpers
sdks/index
workflow_packages
vasp_runners
code_support
collecting
provenance
workflow_compat
campaigns
workflow_cli
workflow_filesystem_api
httk_v1_migration_guide
reference/index
notebooks/examples
```

```{toctree}
:maxdepth: 1
:caption: Details

details/taskmanager
details/workflow_cli
details/launchers
details/launcher_authoring
details/remotes
details/adapter_authoring
details/transfer_reclamation
details/workspace_daemon
details/sealing
details/monitor
details/runtime_helpers
details/composing_workflows
details/workflow_packages
details/workflow_uris
details/collecting
details/declarations
details/provenance
details/stable_ids
details/workflow_compat
details/v1_compatibility
details/httk_v1_migration_guide
details/benchmarks
details/workflow_filesystem_api
```
