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
| `manager.command` | The manager interpreter/command used after `environment.prelude`; without a prelude, the launching Python interpreter is used (for a daemon submission, the daemon configuration's `python`). |
| `manager.allocation` | The `--allocation` probe a launcher passes its managers: `auto`, `none`, `slurm`, `host` or `exec:PATH`; the Slurm launcher's default is `slurm`. |
| `manager.launch_template` | Argv template for the attempt launch prefix; placeholders `{procs}` `{nodes}` `{hosts}` `{nodefile}` `{gpus}` `{mem}` `{cpus_per_proc}`. |
| `manager.launch_mpi` | Slurm MPI plugin appended as `--mpi=<name>` to the built-in Slurm launch step (for example `pmi2` for Intel MPI); ignored when `manager.launch_template` is set. |
| `manager.confine.block_mpi_spawn` | Under `manager.confine=bwrap`: `on` (default) relays confined ranks' Slurm PMI-1 and refuses `MPI_Comm_spawn`, `auto` does so only when Slurm sets `PMI_FD`, `off` passes Slurm's PMI through; see [PMI-2 launches](workspace_daemon.md#pmi-2-launches-intel-mpi). |
| `manager.bind_cpus` | `true`, `1` or `yes` pins locally executed attempts to the CPUs of their processor slots; off by default. |
| `manager.confine`, `confine.*` | Attempt confinement; see [Confinement](#confinement). |
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
| `slurm.export` | sbatch `--export` mode for a workspace-daemon manager submission: `NONE` (default) or `NIL`. |
| `environment.prelude` | Shell setup run before the manager, such as a module load or environment activation. |

The `slurm.*` values become batch directives, except `slurm.export`, which sets
the `sbatch --export` command-line mode (`NONE` or `NIL`, default `NONE`) of a
workspace-daemon manager submission; see the site's `sbatch` manual for what
each mode means. `environment.prelude` runs before
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
logs/batch/manager-*.sbatch
logs/batch/manager-%j.out
logs/batch/manager-%j.err
logs/managers/<manager-id>.log
```

The batch script and scheduler output are launcher output in `logs/batch/`,
which garbage collection never touches. Each manager writes its own log
`logs/managers/<manager-id>.log`; no file is shared between managers.

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

## Confinement

A manager with `manager.confine=bwrap` starts every attempt in a Bubblewrap
sandbox in which the workspace is read-only and only the attempt's own job
directory is writable; parallel launches go through the trusted manager. The
manager itself stays unconfined. {doc}`taskmanager` describes the sandbox, and
{doc}`workspace_daemon` the deployment that requires it. Set the keys on a
launcher or as workspace settings:

| Key | Default | Meaning |
| --- | --- | --- |
| `manager.confine` | `none` | `none` runs attempts unconfined; `bwrap` confines each attempt. |
| `confine.readonly_paths` | existing of `/usr`, `/bin`, `/lib`, `/lib64`, `/etc`, the manager's resolved `sys.prefix` and `sys.base_prefix`, and the directories the `httk` packages are imported from, nested entries dropped | Colon-separated absolute paths bound read-only at their own paths into attempts and ranks. Must not cover `/`, `/tmp`, `/tmp/home`, `/proc` or `/dev`, nor lie inside the workspace. |
| `confine.isolate_network` | `true` | `true` or `false` (also `1`, `0`): give attempts a private network namespace. Ranks of a confined launch always use the host network. |
| `confine.bwrap` | `bwrap` on `PATH` | Absolute path of the Bubblewrap executable. |
| `confine.devices` | none | Colon-separated device nodes below `/dev` bound into attempts and ranks, such as GPU or InfiniBand devices. |
| `confine.pmix_roots` | none | Colon-separated approved parents of the per-step PMIx directory exposed to ranks. |
| `confine.shm_root` | `/dev/shm` | Node-local parent of the per-launch shared-memory directories and of the launch client's liveness lock directory `httk-launch-<attempt_id>/`; it is assumed to be a tmpfs (not checked). |
| `confine.environment.<NAME>` | none | A variable set in rank sandboxes, such as site MPI tuning; `NAME` is a portable identifier outside `HTTK_*`, the value at most 4096 bytes. |

Values are strings; an unknown `confine.*` key or a malformed value refuses the
manager's start, and is refused already by `launcher add` and
`launcher configure` for a `slurm` launcher. With `bwrap`, a manager probes
Bubblewrap once when it starts and refuses to start if it cannot build the
sandbox. If confinement becomes unavailable later, for example after a
workspace setting changes, the manager stops claiming work and reports the
reason until it is available again.

### Pinned settings

A `slurm` launcher pins its own values of `manager.confine`,
`manager.launch_template`, `manager.launch_mpi`, `manager.confine.block_mpi_spawn`, `manager.bind_cpus` and every
`confine.*` key on each manager it starts: they are passed as `--setting KEY=VALUE`, appended after any
`--setting` given on the command line, and the last occurrence of a key wins.
Pinned values override the workspace setting of the same key and stay fixed
for the manager's lifetime. Every other workspace setting, and any of these
keys the launcher does not set, is read live at each claim. The process
launcher has no bundle and pins nothing; set the keys as workspace settings,
or pass `--setting` to `httk workflow run` or `manager run` yourself (the
option is not shown in `--help` and accepts only these keys). Job parameters
and declared job environment never reach these keys.

```console
$ httk workflow launcher add --template slurm --global --set manager.confine=bwrap confined
$ httk workflow run --workspace default --launcher confined --count 2
```

### Extending path lists

For the colon-separated path settings of a `slurm` launcher
(`confine.readonly_paths`, `confine.devices`, `confine.pmix_roots`),
`launcher configure --add-path KEY=PATH[:PATH...]` appends absolute paths in
order without duplicates, after any `--set`, and prints the resulting value:

```console
$ httk workflow launcher configure confined --add-path confine.readonly_paths=/software:/opt/modules
```

When `confine.readonly_paths` is not set yet, the list starts from the default
computed by the interpreter running this command, so adding one path does not
drop the system directories, Python prefixes and *httk* import roots. List
everything a prelude or code needs at run time there: module trees, conda or
virtual-environment prefixes, code binaries and installed runner search paths.

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
