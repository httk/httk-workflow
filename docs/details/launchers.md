# Launchers

A launcher decides where workflow managers start. It is to starting managers
what a remote is to reaching a machine: a named bundle containing
`launcher.json` and one executable named `launcher`. The bundle's `check` and
`start` operations let *httk-workflow* validate an environment and start
managers without scheduler-specific commands in the workflow engine. The
built-in `process` launcher is a workspace's default; named launchers such as
`slurm` submit managers through an external scheduler.

Named launchers are resolved project-first, then globally. A project-local
bundle lives at `httk_project/launchers/NAME`, a global one at
`~/.config/httk/launchers/NAME`.

## Setting one up

The packaged template is `slurm`. Create a global launcher profile on a machine
where `sbatch` is available:

```console
$ httk workflow launcher add --template slurm --global cluster
$ httk workflow launcher check cluster
```

Settings can also be given at creation:

```console
$ httk workflow launcher add --template slurm --global --set slurm.partition=batch cluster
```

Launcher settings are non-secret configuration stored in `launcher.json` and
shared with the project; credentials do not belong there.

Then select the launcher in the workspace. The workspace setting is separate
from the bundle, so several workspaces can use the same launcher with different
workspace-level values:

```console
$ httk workspace settings set --key manager.launch --value cluster default
$ httk workspace settings set --key manager.workers --value 4 default
$ httk workspace settings set --key slurm.account --value project-123 default
$ httk workspace settings set --key slurm.time_limit --value 02:00:00 default
$ httk workspace settings set --key environment.prelude --value 'module load httk' default
$ httk workflow run --count 4 --workspace default
```

`httk workflow run --count 4` returns the submitted scheduler job IDs.

### Workspace settings

| Setting | Meaning |
| --- | --- |
| `manager.launch` | Launcher name; the built-in `process` launcher is the default. |
| `manager.count` | Default number of managers; `--count N` overrides it for one invocation. |
| `manager.workers` | Default number of workers per manager; `--workers N` overrides it. |
| `manager.command` | The manager interpreter/command used after `environment.prelude`; without a prelude, the launching Python interpreter is used. |
| `manager.allocation` | The `--allocation` probe a launcher passes its managers: `auto`, `none`, `slurm`, `host` or `exec:PATH`; the Slurm launcher's default is `slurm`. |
| `manager.launch_template` | Argv template for the attempt launch prefix; placeholders `{procs}` `{nodes}` `{hosts}` `{nodefile}` `{gpus}` `{mem}` `{cpus_per_proc}`. |
| `manager.bind_cpus` | `true`, `1` or `yes` pins locally executed attempts to the CPUs of their processor slots; off by default. |
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

The `slurm.*` values become batch directives. `environment.prelude` runs before
the manager under `set -e`, and `manager.command` is then looked up on the
resulting `PATH`. Without a prelude, the launcher preserves the Python
interpreter that started the command.

For MPI programs, `slurm.ntasks` requests the total number of processes and
`slurm.ntasks_per_node` their placement per node, while `slurm.cpus_per_task`
requests the CPU cores allocated to each process. Set the task count for MPI
ranks, and use `cpus_per_task` when each rank needs several cores.

### Overrides and debugging

`--launcher NAME` overrides `manager.launch` for one invocation, including
`--launcher process`:

```console
$ httk workflow run --workspace default --count 4 --launcher cluster
```

For a debugging pass, run exactly one manager in the current process:

```console
$ httk workflow run --count 1 --inline --workspace default
```

`--inline` shows manager output directly and ignores the workspace launcher.
`--detach` starts `process` managers detached and returns immediately.

Launch behaves the same on a login node, through a workspace addressed by a
configured `machine_names` name, and when a remote invokes the manager on its
owning machine: the remote supplies transport, and the target workspace's
launcher starts the managers.

### Generated files

After submission, the Slurm template leaves its generated files below the
workspace:

```text
.httk-workspace/batch/manager-*.sbatch
.httk-workspace/batch/manager-%j.out
.httk-workspace/batch/manager-%j.err
.httk-workspace/managers.log
```

The batch script and scheduler output are launcher output. `managers.log` is
the workspace-level log shared by all managers attached to the workspace.

## Several launchers

Create separate profiles when, for example, CPU and GPU managers need different
queues or reservations:

```console
$ httk workflow launcher add --template slurm --global --set slurm.partition=cpu cpu
$ httk workflow launcher add --template slurm --global --set slurm.partition=gpu gpu
$ httk workflow launcher configure --set slurm.reservation=gpu-a100 gpu
$ httk workflow run --workspace default --launcher cpu --count 4
$ httk workflow run --workspace default --launcher gpu --count 2
```

Settings in a launcher's `launcher.json` `settings` object take precedence over
workspace settings with the same key, so `cpu` and `gpu` keep different
scheduler values while starting managers for the same workspace. `launcher
list`, `launcher show [--json]`, and `launcher remove` inspect and manage the
visible bundles.

## Daemon launchers

The `daemon` template creates launchers that only `httk workspace daemon` reads.
They configure the approved manager resources of the confined daemon and refuse
to run directly:

```console
$ httk workflow launcher add --template daemon --global --set slurm.cpus_per_task=2 small
```

Names match `[a-z][a-z0-9_-]{0,63}`. See {doc}`workspace_daemon` for the keys.

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

`add_launcher` creates the bundle from the maintained template and
`check_launcher` runs its environment check. Pass a `settings` mapping to
`add_launcher`, or update an existing bundle with `configure_launcher` (the CLI
equivalent is `httk workflow launcher configure --set KEY=VALUE NAME`).

For local debugging, bypass launcher submission and run one manager
in-process:

```python
from httk.workflow import TaskManager, Workspace

workspace = Workspace("default")
with TaskManager(workspace, maximum_workers=4) as manager:
    census = manager.run_until_idle()
print(census)
```

## Writing a launcher

A custom launcher is a versioned bundle with `launcher.json` and an executable
`launcher` that answers the `check` and `start` operations, each with one JSON
request and one JSON result. {doc}`launcher_authoring` has the complete bundle
layout, request and result documents, settings precedence, and refusal rules.
