# *httk-workflow*

This site documents the *httk-workflow* module. For the full documentation of
*httk₂*, see [docs.httk.org](https://docs.httk.org).

The module implements a recoverable, language-neutral workflow protocol whose
source of truth is a single atomically renamed state marker per job. It presents
three layers, each with its own import home: the **filesystem protocol**
(`httk.workflow.protocol`), the **execution and authoring** surface
(`httk.workflow` — `Runner`, `Attempt` — with lower-level helpers in
`httk.workflow.runtime`), and **orchestration and management** (`Workspace`,
`TaskManager`, `harvest`, and named submodules for transfers, remotes, and
compatibility). Installations register the canonical `httk workflow` command
tree, plus the `httk-taskmanager` and `httk-v1-taskmanager` executables, which
are aliases of it.

```{admonition} Quick links
:class: tip

**New here** — start with {doc}`quickstart`: five commands from an empty
directory to a finished relaxation, no runner written and no VASP required.

**The filesystem protocol** — the language-neutral on-disk contract

- {doc}`workflow_protocol_api` — the `httk.workflow.protocol` namespace
- {doc}`workflow_filesystem_api` — the normative on-disk specification

**The execution API** — writing and running workflow steps

- {doc}`runtime_helpers` — the Python authoring SDK: `Runner`, `Attempt`, steps
- {doc}`native_bash_api` — the same surface in Bash
- {doc}`sdk_parity` — the normative table both of the above must agree with
- {doc}`vasp_runners` — the packaged runners, for campaigns that write none
- {doc}`declarations` — saying what a workflow *is*, for a data layer
- {doc}`importing_workflows` — running PWD and CWL documents as ordinary jobs
- {doc}`notebooks/examples` — worked examples as a notebook

**Orchestration and management** — driving and inspecting a workspace

- {doc}`taskmanager` — workspaces, submission, managers, inspection, repair
- {doc}`workflow_cli` — the whole `httk workflow` tree: projects, config, remotes
- {doc}`campaigns` — partitioning a very large campaign across many workspaces
- {doc}`benchmarks` — measured local scale snapshot and benchmark methodology
- {doc}`harvest` — reading finished jobs back out as records
- {doc}`adapter_authoring` — reaching a machine the packaged adapters do not cover
- {doc}`reference/index` — the generated API reference

**Migration**

- [*httk* v1 migration guide](httk_v1_migration_guide.md)
- [*httk* v1 compatibility](v1_compatibility.md)
```

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
httk project init --name quickstart
httk workflow workspace init . --name default
httk workflow job new --template vasp-relax --parameter structure=POSCAR --tag silicon
httk workflow workspace settings set vasp.command "$PWD/examples/mock_vasp.py"
httk workflow run
httk workflow harvest
```

{doc}`quickstart` walks through exactly those commands, including how to run them
without VASP installed. A complete payload prepared some other way is still
submitted directly:

```console
httk workflow job submit workflow-workspace prepared-job --placement project/00
```

```{toctree}
:maxdepth: 2
:caption: Documentation

quickstart
workflow_protocol_api
runtime_helpers
native_bash_api
sdk_parity
vasp_runners
declarations
importing_workflows
taskmanager
workflow_cli
campaigns
benchmarks
harvest
adapter_authoring
reference/index
notebooks/examples
workflow_filesystem_api
httk_v1_migration_guide
v1_compatibility
```
