# Launchers

*For operators who need to choose where workflow managers start.* A launcher is
to starting managers what a remote is to reaching a machine. It is a named
bundle containing `launcher.json` and one executable named `launcher`; the
bundle's `check` and `start` operations let *httk-workflow* validate an
environment and start managers without embedding scheduler-specific commands
in the workflow engine. The built-in `process` launcher is the default for a
workspace, and named launchers such as `slurm` submit managers through an
external scheduler.

Named launchers are resolved project-first and then globally. A project-local
bundle lives at `httk_project/launchers/NAME`; a global bundle lives at
`~/.config/httk/launchers/NAME`.

## Setting one up

The packaged template is `slurm`. Create a global launcher profile on a
machine where `sbatch` is available:

```console
$ httk workflow launcher add --template slurm --global cluster
$ httk workflow launcher check cluster
```

You can provide a setting while creating it:

```console
$ httk workflow launcher add --template slurm --global --set slurm.partition=batch cluster
```

Launcher settings are non-secret configuration stored in `launcher.json` and
shared with the project; credentials do not belong there.

Then select it for the workspace. The workspace setting is deliberately
separate from the launcher bundle, so the same launcher can be used by several
workspaces with different workspace-level values:

```console
$ httk workspace settings set --key manager.launch --value cluster default
$ httk workspace settings set --key manager.workers --value 4 default
$ httk workspace settings set --key slurm.account --value project-123 default
$ httk workspace settings set --key slurm.time_limit --value 02:00:00 default
$ httk workspace settings set --key environment.prelude --value 'module load httk' default
$ httk workflow run --count 4 --workspace default
```

`httk workflow run --count 4` returns the submitted scheduler job IDs. The
available workspace settings are:

| Setting | Meaning |
| --- | --- |
| `manager.launch` | Launcher name; the built-in `process` launcher is the default. |
| `manager.count` | Default number of managers; `--count N` overrides it for one invocation. |
| `manager.workers` | Default number of workers per manager; `--workers N` overrides it. |
| `manager.command` | The manager interpreter/command used after `environment.prelude`; without a prelude, the launching Python interpreter is used. |
| `slurm.account` | Slurm account directive. |
| `slurm.partition` | Slurm partition directive. |
| `slurm.time_limit` | Slurm time limit directive. |
| `slurm.nodes` | Number of Slurm nodes. |
| `slurm.cpus_per_task` | CPUs per Slurm task. |
| `slurm.ntasks` | Number of Slurm tasks. |
| `slurm.ntasks_per_node` | Slurm tasks per node. |
| `slurm.mem` | Slurm memory allocation. |
| `slurm.gres` | Slurm generic resource request. |
| `slurm.reservation` | Slurm reservation. |
| `environment.prelude` | Shell setup run before the manager, such as a module load or environment activation. |

The `slurm.*` values become batch directives. `environment.prelude` runs
before the manager under `set -e`; with a prelude, `manager.command` is looked
up on the resulting `PATH`. Without a prelude, the launcher preserves the
Python interpreter that started the command.

For MPI programs, `slurm.ntasks` requests the total number of processes and
`slurm.ntasks_per_node` controls their placement per node; `slurm.cpus_per_task`
instead requests the CPU cores allocated to each process. Set the task count
for MPI ranks and use `cpus_per_task` when each rank needs multiple cores.

For a debugging pass, run exactly one manager in the current process:

```console
$ httk workflow run --count 1 --inline --workspace default
```

`--inline` is useful for seeing manager output directly and ignores the
workspace launcher. `--detach` starts `process` managers detached and returns
immediately. The same launch behavior applies on a login node, through a
workspace addressed by a configured `machine_names` name, or when a remote
invokes the manager on its owning machine; a remote supplies transport, while
the target workspace's launcher starts the managers. Use `--launcher NAME` to
override `manager.launch` for one invocation (including `--launcher process`):

```console
$ httk workflow run --workspace default --count 4 --launcher cluster
```

After submission, the Slurm template leaves its generated files below the
workspace:

```text
.httk-workspace/batch/manager-*.sbatch
.httk-workspace/batch/manager-%j.out
.httk-workspace/batch/manager-%j.err
.httk-workspace/managers.log
```

The batch script and scheduler output are launcher output. `managers.log` is
the workspace-level manager log shared by managers attached to that workspace.

## Several launchers

Create separate profiles when, for example, CPU and GPU managers need
different queues or reservations:

```console
$ httk workflow launcher add --template slurm --global --set slurm.partition=cpu cpu
$ httk workflow launcher add --template slurm --global --set slurm.partition=gpu gpu
$ httk workflow launcher configure --set slurm.reservation=gpu-a100 gpu
$ httk workflow run --workspace default --launcher cpu --count 4
$ httk workflow run --workspace default --launcher gpu --count 2
```

Settings in a launcher's `launcher.json` `settings` object take precedence over
workspace settings with the same key. This lets `cpu` and `gpu` retain
different scheduler values even when they start managers for the same
workspace. `launcher list`, `launcher show [--json]`, and `launcher remove`
inspect and manage the visible bundles.

## From Python

The launcher helpers are in `httk.workflow.launchers`. After a workspace has
been initialized, the equivalent setup and a scheduler submission are:

```python
import sys
from pathlib import Path

from httk.workflow import Workspace
from httk.workflow.launchers import (
    add_launcher,
    check_launcher,
    configure_launcher,
    list_launchers,
    resolve_launcher,
    start_managers,
)

project = Path(".").resolve()
workspace = Workspace(project / "default")

add_launcher(
    "cluster",
    template="slurm",
    settings={"slurm.partition": "batch"},
    global_=True,
)
target = resolve_launcher("cluster", project=project)
check_launcher(target)
print(list_launchers(project))
workspace.set_setting("manager.launch", "cluster")
workspace.set_setting("manager.workers", 4)

result = start_managers(
    target,
    workspace_root=workspace.root,
    argv=[
        sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run",
        "--by-path", "--workspace", str(workspace.root),
    ],
    count=4,
    settings=workspace.settings,
    timeout=None,
)
print(result)
```

`add_launcher` creates the maintained template and `check_launcher` runs its
environment check. Pass a `settings` mapping to `add_launcher`, or update an
existing bundle with `configure_launcher`; the CLI equivalent is `httk workflow
launcher configure --set KEY=VALUE NAME`. For local debugging, bypass launcher
submission and run one manager in-process:

```python
from httk.workflow import TaskManager, Workspace

workspace = Workspace("default")
with TaskManager(workspace, maximum_workers=4) as manager:
    census = manager.run_until_idle()
print(census)
```

## Writing a launcher

A custom launcher is a versioned bundle with `launcher.json` and an executable
`launcher`. The executable answers two operations, `check` and `start`, each
from one JSON request and with one JSON result. `check` verifies the launcher's
local requirements; `start` receives the workspace path, complete manager
argument vector, manager count, and separate `settings` (workspace) and
`launcher_settings` (bundle) mappings. The packaged Slurm kind merges these
with launcher precedence; custom launchers define their own merge. The engine refuses an
unknown operation, malformed metadata or result, a non-executable dispatcher,
missing required binary, non-zero dispatcher exit, or a result that does not
confirm success. It also refuses a launcher that tries to take over remote
transport: reaching a machine belongs to a remote adapter.

The complete bundle layout, request and result documents, settings precedence,
and refusal rules are in {doc}`details/launcher_authoring`.
