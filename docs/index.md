# *httk-workflow*

This site documents the *httk-workflow* module. For the full documentation of
*httk₂*, see [docs.httk.org](https://docs.httk.org).

The module implements a recoverable, language-neutral workflow protocol whose
source of truth is a single atomically renamed state marker per job. It presents
three layers, each with its own import home: the **filesystem protocol**
(`httk.workflow.protocol`), the **execution and authoring** surface
(`httk.workflow` — `Runner`, `Attempt` — with lower-level helpers in
`httk.workflow.runtime`), and **orchestration and management** (`Workspace`,
`TaskManager`, `collect`, and named submodules for transfers, remotes, and
compatibility). Installations register the `httk workflow` command tree.

Managers are started by the workspace's `manager.launch` setting, using the
built-in `process` launcher or a launcher bundle such as `slurm`. A remote is
only the transport for files and commands on another machine.

*httk₂* workflows are language-independent: runners, hooks, and postprocess
scripts can be written in any language; a workflow is a manifest plus the
members it references. Python hooks remain first-class, with an in-process fast
path. Successful hook outputs use the same assembly semantics as executable
hooks; collector failures differ deliberately: registered `.py` exceptions
abort iteration, while executable-hook errors degrade per job and continue the
sweep.

```{admonition} Quick links
:class: tip

**New here** — start with {doc}`quickstart`: five commands from an empty
directory to a finished relaxation, no runner written and no VASP required.

**The filesystem protocol** — the language-neutral on-disk contract

- {doc}`workflow_protocol_api` — the `httk.workflow.protocol` namespace
- {doc}`workflow_filesystem_api` — the normative on-disk specification

**The execution API** — writing and running workflow steps

- {doc}`runtime_helpers` — the Python authoring SDK: `Runner`, `Attempt`, steps
- {doc}`sdks/index` — the same authoring surface in eight more languages
- {doc}`vasp_runners` — the packaged runners and relaxation report, for campaigns that write none
- {doc}`workflow_packages` — authoring directory packages and their manifest
- {doc}`declarations` — saying what a workflow *is*, for a data layer
- {doc}`provenance` — turning one `JobRecord` into one `httk.core.Run`
- {doc}`collecting` — collecting provider-produced outputs and products
- {doc}`workflow_languages` — CWL, PWD, jobflow, and httk-v1 runner realizations
- {doc}`notebooks/examples` — worked examples as a notebook

**Orchestration and management** — driving and inspecting a workspace

- {doc}`taskmanager` — workspaces, submission, managers, inspection, repair
- {doc}`workflow_cli` — the whole `httk workflow` tree: projects, config, remotes, and launchers
- {doc}`campaigns` — partitioning a very large campaign across many workspaces
- {doc}`composing_workflows` — a workflow that calls other workflows as child jobs
- {doc}`benchmarks` — measured local scale snapshot and benchmark methodology
- {doc}`collecting` — reading finished jobs back out as records and collected outputs
- {doc}`remotes` — reaching a machine with a packaged or custom remote adapter
- {doc}`launchers` — starting managers with a packaged or custom launcher
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
httk project init --name quickstart .
httk workspace init --name default .
httk job new --workflow vasp-relax --input structure=POSCAR --tag silicon
httk workspace settings set --key vasp.command --value "$PWD/examples/mock_vasp.py" default
httk workflow run
httk workflow collect
```

{doc}`quickstart` walks through exactly those commands, including how to run them
without VASP installed. A complete payload prepared some other way is still
submitted directly:

```console
httk job submit --workspace workflow-workspace --placement project/00 prepared-job
```

```{toctree}
:maxdepth: 2
:caption: Documentation

quickstart
workflow_protocol_api
runtime_helpers
sdks/index
vasp_runners
workflow_packages
declarations
provenance
collecting
workflow_languages
taskmanager
workflow_cli
campaigns
composing_workflows
benchmarks
remotes
launchers
reference/index
notebooks/examples
workflow_filesystem_api
httk_v1_migration_guide
```

```{toctree}
:maxdepth: 1
:caption: Details

details/runtime_helpers
details/taskmanager
details/workflow_packages
details/workflow_cli
details/monitor
details/adapter_authoring
details/launcher_authoring
details/workflow_filesystem_api
details/httk_v1_migration_guide
v1_compatibility
```
