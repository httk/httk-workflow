# httk-workflow

*httk-workflow* is the filesystem-native workflow engine for
[*httk₂*](https://github.com/httk/httk2).

It provides the `httk.workflow` Python API in three layers — the language-neutral
filesystem protocol (`httk.workflow.protocol`), the execution and authoring
surface (`httk.workflow` — `Runner`, `Attempt`), and orchestration and management
(`Workspace`, `TaskManager`, `harvest`, and named submodules) — and registers
`httk workflow`, the canonical command tree for everything below. The
`httk-taskmanager` and
`httk-v1-taskmanager` executables remain installed as aliases of that tree; the
second one prepares and executes legacy `ht_steps`/`ht_run` task templates
through the same *httk₂* workspace. Jobs communicate through atomically
published filesystem state, so interrupted managers and calculations can be
recovered without cleanup hooks.

From nothing to a finished VASP relaxation, without writing a runner:

```console
httk workflow workspace init workflow-workspace --extension transactional-data-v1
httk workflow job new workflow-workspace --template vasp-relax --from POSCAR --tag silicon
export HTTK_VASP_COMMAND="$PWD/examples/mock_vasp.py"   # or: srun -n 32 vasp_std
httk workflow manager run workflow-workspace --until-idle
httk workflow harvest workflow-workspace
```

[`docs/quickstart.md`](docs/quickstart.md) explains each command, and
`examples/quickstart.sh` runs the whole sequence — with the mock VASP above
standing in for VASP on a machine that has none.

## Install

```console
python -m pip install httk-workflow
```

One optional extra exists. `httk-workflow[cwl]` adds the CWL *parser* needed to
run `httk workflow import cwl`; executing what was imported needs nothing extra,
so the extra belongs only on the machine that does the importing. Importing
Python Workflow Definition documents needs no extra at all.

## What it does

- **Runs workflows without a graph.** A step decides at run time which children
  to spawn and which step runs next, so a two-step relaxation and a
  thousand-child campaign are the same engine —
  [runners in Python](docs/runtime_helpers.md) or
  [in Bash](docs/native_bash_api.md), with a
  [normative parity table](docs/sdk_parity.md) between the two.
- **Recovers instead of cleaning up.** One atomically renamed state marker per
  job is the source of truth, so an interrupted manager, node, or calculation is
  resumed from what is on disk. The protocol is specified in
  [`docs/workflow_filesystem_api.md`](docs/workflow_filesystem_api.md).
- **Ships complete VASP runners**, so an ordinary relaxation or single point
  needs no runner written at all — see [`docs/vasp_runners.md`](docs/vasp_runners.md).
- **Runs workflows written elsewhere.** Python Workflow Definition and CWL
  documents become ordinary jobs; see
  [`docs/importing_workflows.md`](docs/importing_workflows.md).
- **Reaches other machines.** Versioned [remote adapters](docs/adapter_authoring.md)
  send work to a cluster, start managers there, and fetch results back through
  crash-recoverable detached transfer.
- **Manages projects and identity**: XDG configuration, signed project
  manifests, and workspace policy — see
  [`docs/workflow_cli.md`](docs/workflow_cli.md).
- **Hands results to a data layer.** [`harvest`](docs/harvest.md) yields one
  record per stopped job; *httk-workflow* itself has no database dependency.
- **Keeps *httk* v1 workflows running.** `ht_steps`/`ht_run` task directories
  execute unchanged on this engine — see
  [`docs/v1_compatibility.md`](docs/v1_compatibility.md) and the
  [migration guide](docs/httk_v1_migration_guide.md).

```console
httk workflow project init . --name example
httk workflow project manifest create
httk workflow workspace status .
```
