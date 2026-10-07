# Writing a manager launcher in detail

A launcher starts one or more workflow managers on the machine that hosts a
workspace, just as a remote adapter moves data and runs commands on a machine.
This guide covers writing one for a scheduler or process system that the
maintained templates do not cover.

## The bundle

A launcher is a versioned directory with one dispatcher executable:

```text
my-cluster/
├── launcher.json
└── launcher
```

Project launchers live below `PROJECT/httk_project/launchers/NAME/` and global
launchers below `$XDG_CONFIG_HOME/httk/launchers/NAME/`. A project definition
shadows a global definition of the same name. The name `process` is reserved
for the built-in detached-process implementation and cannot be defined as a
bundle.

The maintained Slurm template is installed with:

```console
httk workflow launcher add --template slurm --global cluster
httk workflow launcher check cluster
```

The executable is normally a small wrapper:

```sh
#!/bin/sh
exec python3 -m httk.workflow.launch_runtime "$@"
```

It receives the name of one temporary JSON request file, prints exactly one JSON
result object, and writes diagnostics to stderr. The engine never invokes it
through a shell.

## `launcher.json`

The metadata document is validated when a bundle is added, resolved, or run.
The maintained format is:

```json
{
  "format": "httk-manager-launcher",
  "format_version": 2,
  "launcher_version": 2,
  "kind": "slurm",
  "settings": {},
  "required_binaries": ["sbatch"],
  "timeout_seconds": 60
}
```

| Member | Required | Meaning |
| --- | --- | --- |
| `format` | yes | `httk-manager-launcher` |
| `format_version` | yes | `2` |
| `launcher_version` | yes | `2`, the operation contract version |
| `kind` | yes for maintained dispatchers | Implementation selector, such as `slurm` |
| `settings` | no | Flat bundle settings, defaulting to `{}` |
| `required_binaries` | no | Programs checked with `shutil.which` on the launcher host |
| `timeout_seconds` | no | Positive operation timeout, default `60` |

`launcher` must exist and be executable. `required_binaries` is checked
locally: list `qsub` when the launcher itself calls a local `qsub`, but not a
binary that only exists after a remote hop. The metadata may name a custom
kind, but the packaged dispatcher refuses kinds it does not implement.

## The request and result envelopes

Every operation is sent as a request like this, with the operation-specific
members following the envelope:

```json
{
  "format": "httk-manager-launcher-request",
  "format_version": 2,
  "operation": "start",
  "launcher_dir": "/home/me/.config/httk/launchers/cluster",
  "workspace": "/scratch/me/runs/workspace",
  "argv": ["httk", "workflow", "manager", "run", "--workspace", "/scratch/me/runs/workspace"],
  "count": 2,
  "settings": {
    "slurm.partition": "batch",
    "manager.workers": "8",
    "environment.prelude": "module load python"
  }
}
```

The dispatcher prints one result object:

```json
{
  "format": "httk-manager-launcher-result",
  "format_version": 2,
  "operation": "start",
  "ok": true,
  "kind": "slurm",
  "count": 2,
  "job_ids": ["501", "502"],
  "script": "/scratch/me/runs/workspace/logs/batch/manager-....sbatch"
}
```

The engine checks the format, version, operation, and `ok`. A refusal has
`ok: false` and an `error`, and the dispatcher still exits zero so that the JSON
refusal crosses the boundary intact. A non-zero dispatcher exit, malformed JSON,
or a mismatched envelope is an engine error. Stderr is attached as
`diagnostics` on successful results. The caller can override the
`timeout_seconds` bound for one operation with another positive value; a
timeout raises `TimeoutError`.

The engine refuses an unknown operation, malformed metadata or result, a
non-executable dispatcher, a missing required binary, a non-zero dispatcher
exit, or a result that does not confirm success. It also refuses a launcher that
tries to take over remote transport, because reaching a machine belongs to a
remote adapter.

## The two operations

`check` verifies every `required_binaries` entry with `shutil.which` and returns
the kind. It submits nothing.

`start` receives an absolute `workspace`, the full manager `argv`, a positive
manager `count`, the workspace `settings` mapping, and the bundle's
`launcher_settings` mapping. The maintained Slurm kind merges them, with bundle
settings taking precedence over workspace settings for the keys the kind
consumes; custom launchers define their own merge. Workspace settings are not
copied into the bundle. The settings fall into four groups:

- scheduler settings: `slurm.account`, `slurm.partition`, `slurm.time_limit`,
  `slurm.nodes`, `slurm.cpus_per_task`, `slurm.ntasks`,
  `slurm.ntasks_per_node`, `slurm.mem`, `slurm.gres`, and `slurm.reservation`,
  which become batch directives; and `slurm.export` (`NONE`, the default, or
  `NIL`), which instead sets the `sbatch --export` mode of a workspace-daemon
  manager submission;
- `manager.workers` and `manager.allocation`, which belong to the manager
  command;
- the manager's pinned settings `manager.confine`, `manager.launch_template`,
  `manager.bind_cpus` and `confine.*`, which the manager itself reads (see
  [confinement](launchers.md#confinement));
- `environment.prelude`, shell setup such as module loads.

The manager reads its own settings from the workspace at each claim, so a
bundle value of a key the manager reads reaches it only through the manager
command. The maintained Slurm kind appends `--setting KEY=VALUE` for each
bundle value of a pinned setting, after any `--setting` already in the argv;
the last occurrence of a key wins, so bundle values win, and they stay fixed
for the manager's lifetime. Workspace values of those keys are never copied
into `--setting`: the manager reads them live. A custom launcher that should
pin confinement must forward its values the same way.

`environment.prelude` runs before the manager, outside any attempt sandbox,
and the environment it leaves is the manager's. A confined attempt inherits
that environment without the `SLURM_*`, `SRUN_*`, `SBATCH_*`, `SALLOC_*`,
`PMI_*` and `PMIX_*` variables, and can use only what `confine.readonly_paths`
exposes, so list the directories the prelude adds (module trees, software
prefixes) there.

A launcher may use other settings, but should keep its interpretation explicit.

The maintained Slurm dispatcher writes one mode-0700 script below
`logs/batch/`, adds `--chdir`, output, and error paths, and calls
`sbatch` once per requested manager. The script's final command is an
argument-quoted `exec` line. If `environment.prelude` is set, the prelude runs
first under `set -e`, and the manager command is resolved on the resulting
`PATH` as `manager.command` (default `httk`). Without a prelude, the supplied
Python interpreter argv is preserved. Unless the argv already has one, the
dispatcher appends `--allocation` with the `manager.allocation` setting
(default `slurm`), so each manager probes its job's nodes; see
[allocation probes](#allocation-probes). It then appends the bundle's pinned
`--setting` values. A successful result contains the parsed
Slurm job IDs and the script path. If submission fails after some jobs were
accepted, the refusal includes `submitted` and `job_ids` so the operator can
cancel those jobs.

## A PBS launcher

This compact custom dispatcher follows the same request and result rules,
composes PBS directives from workspace settings, and submits the same manager
command once per requested count. In a real bundle, save it as `launcher`, make
it executable, use `"kind": "pbs"`, and list `qsub` in `required_binaries`. It
points each manager at the bundle's `allocation` probe
([below](#allocation-probes)) unless `manager.allocation` names another.

```python
#!/usr/bin/env python3
import json
import os
import shlex
import subprocess
import sys
import uuid
from pathlib import Path

def result(operation, **values):
    print(json.dumps({"format": "httk-manager-launcher-result",
                      "format_version": 2, "operation": operation,
                      "ok": True, **values}))

def refusal(operation, message):
    print(json.dumps({"format": "httk-manager-launcher-result",
                      "format_version": 2, "operation": operation,
                      "ok": False, "error": message}))

def main():
    request = json.loads(Path(sys.argv[1]).read_text())
    operation = request["operation"]
    if operation == "check":
        if subprocess.run(["sh", "-c", "command -v qsub"], capture_output=True).returncode:
            refusal(operation, "qsub is unavailable")
        else:
            result(operation, kind="pbs")
        return
    if operation != "start":
        refusal(operation, "unsupported operation")
        return
    workspace = Path(request["workspace"])
    settings = {**request.get("settings", {}), **request.get("launcher_settings", {})}
    directory = workspace / "logs" / "batch"
    directory.mkdir(parents=True, exist_ok=True)
    script = directory / ("manager-" + uuid.uuid4().hex + ".pbs")
    directives = [("#PBS -N httk-manager",),
                  (f"#PBS -d {workspace}",),
                  (f"#PBS -o {directory}/manager-$PBS_JOBID.out",),
                  (f"#PBS -e {directory}/manager-$PBS_JOBID.err",)]
    names = {"slurm.account": "#PBS -A", "slurm.partition": "#PBS -q",
             "slurm.time_limit": "#PBS -l walltime", "slurm.nodes": "#PBS -l nodes"}
    lines = ["#!/bin/bash -l"]
    lines.extend(item[0] for item in directives)
    for key, prefix in names.items():
        if key in settings:
            lines.append(f"{prefix}={settings[key]}")
    lines.append("set -e")
    if settings.get("environment.prelude"):
        lines.append(str(settings["environment.prelude"]))
    argv = list(request["argv"])
    if "--allocation" not in argv:
        bundle = request["launcher_dir"]
        argv += ["--allocation", settings.get("manager.allocation", f"exec:{bundle}/allocation")]
    lines.append("exec " + shlex.join(argv))
    script.write_text("\n".join(lines) + "\n")
    os.chmod(script, 0o700)
    jobs = []
    for _ in range(request.get("count", 1)):
        completed = subprocess.run(["qsub", str(script)], cwd=workspace,
                                   text=True, capture_output=True, check=False)
        if completed.returncode:
            refusal(operation, completed.stderr.strip() or "qsub failed")
            return
        jobs.append(completed.stdout.strip())
    result(operation, kind="pbs", count=len(jobs), job_ids=jobs, script=str(script))

if __name__ == "__main__":
    main()
```

## Allocation probes

A manager learns its nodes, processors, memory and devices from the allocation
probe its `--allocation SPEC` selects: `auto`, `none`, `slurm`, `host`, or
`exec:PATH` (see {doc}`taskmanager` under "Allocations"). A launcher appends the
spec its managers need; the maintained Slurm dispatcher appends the
`manager.allocation` setting, default `slurm`. A scheduler without a built-in
probe uses `exec:PATH`: the manager runs PATH, without a shell or arguments,
inside the allocation when it starts, and the executable prints one envelope on
stdout:

```json
{
  "format": "httk-workflow-allocation",
  "format_version": 1,
  "kind": "pbs",
  "end_time": 1790000000,
  "cpus_per_proc": 8,
  "nodes": [
    {"host": "n001", "procs": 4, "mem": 128000, "gpus": 2,
     "cpus": ["0-7", "8-15", "16-23", "24-31"],
     "gpu_ids": ["0", "1"], "gpu_variable": "CUDA_VISIBLE_DEVICES",
     "local": true}
  ],
  "resources": {"license": 2}
}
```

| Member | Required | Meaning |
| --- | --- | --- |
| `format`, `format_version` | yes | `httk-workflow-allocation`, `1` |
| `kind` | yes | A label naming the probe, such as `pbs` |
| `end_time` | no | Epoch second the allocation ends, a positive number or `null` |
| `cpus_per_proc` | no | CPUs per processor slot, a positive integer, default `1` |
| `nodes` | yes | Non-empty list of nodes with unique `host` names |
| `nodes[].host` | yes | Non-empty host name |
| `nodes[].procs` | yes | Processor slots on the node, a non-negative integer |
| `nodes[].mem` | no | Memory in MB; the allocation has a `mem` capacity only when every node gives it |
| `nodes[].gpus` | no | GPUs on the node, default `0` |
| `nodes[].cpus` | no | One Linux cpulist (`0-7`, `0,2,4-6`) per processor slot, `procs` entries |
| `nodes[].gpu_ids` | no | One non-empty device id per GPU, `gpus` entries |
| `nodes[].gpu_variable` | with `gpu_ids` | The environment variable the ids belong in, such as `CUDA_VISIBLE_DEVICES` |
| `nodes[].local` | no | Whether this is the manager's host, default `false`; at most one node may set it to `true` |
| `resources` | no | Extra non-negative integer capacities; not `procs`, `mem`, `gpus`, `nodes`, `maxtime` or `mintime` |

CPU sets of different slots on one node must be disjoint, and GPU IDs on that
node must be unique. The same CPU numbers or GPU IDs may occur on different
nodes. Set `local: true` when the scheduler's name for the manager's host differs
from its operating-system hostname, so local CPU and GPU binding applies.

Unknown members are refused. The capacity the manager advertises is the node
sums of `procs`, `gpus` and `mem`, the node count as `nodes`, and the extra
`resources`; `--worker-resource` overrides any of them. A non-zero exit, a
timeout after 60 seconds, or an invalid envelope stops the manager with an error
naming the probe and the end of its stderr.

Maintained scheduler integrations use the private `_scheduler.Scheduler`
interface for environment detection, aggregate capacity, allocation end,
node probing and default application-step arguments. The Slurm implementation
lives in `_slurm`; the manager consumes normalized allocation metadata and
placement results. Site integrations use the launcher bundle, allocation
envelope and `manager.launch_template` described here without importing those
private Python modules.

For the PBS launcher above, save this as `allocation` next to `launcher` and
make it executable. `$PBS_NODEFILE` lists one line per processor slot, repeating
each host:

```python
#!/usr/bin/env python3
import collections
import json
import os

with open(os.environ["PBS_NODEFILE"]) as nodefile:
    slots = collections.Counter(line.strip() for line in nodefile if line.strip())
print(json.dumps({
    "format": "httk-workflow-allocation",
    "format_version": 1,
    "kind": "pbs",
    "nodes": [{"host": host, "procs": procs} for host, procs in slots.items()],
}))
```

The PBS dispatcher appends `--allocation exec:BUNDLE/allocation` by default;
setting it explicitly is equivalent:

```console
httk workflow launcher configure --set manager.allocation=exec:/home/me/.config/httk/launchers/pbs-cluster/allocation pbs-cluster
```
