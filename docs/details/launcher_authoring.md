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
  "script": "/scratch/me/runs/workspace/.httk-workspace/batch/manager-....sbatch"
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
copied into the bundle. The settings fall into three groups:

- scheduler settings: `slurm.account`, `slurm.partition`, `slurm.time_limit`,
  `slurm.nodes`, `slurm.cpus_per_task`, `slurm.ntasks`,
  `slurm.ntasks_per_node`, `slurm.mem`, `slurm.gres`, and `slurm.reservation`;
- `manager.workers`, which belongs to the manager command;
- `environment.prelude`, shell setup such as module loads.

A launcher may use other settings, but should keep its interpretation explicit.

The maintained Slurm dispatcher writes one mode-0700 script below
`.httk-workspace/batch/`, adds `--chdir`, output, and error paths, and calls
`sbatch` once per requested manager. The script's final command is an
argument-quoted `exec` line. If `environment.prelude` is set, the prelude runs
first under `set -e`, and the manager command is resolved on the resulting
`PATH` as `manager.command` (default `httk`). Without a prelude, the supplied
Python interpreter argv is preserved. A successful result contains the parsed
Slurm job IDs and the script path. If submission fails after some jobs were
accepted, the refusal includes `submitted` and `job_ids` so the operator can
cancel those jobs.

## A PBS launcher

This compact custom dispatcher follows the same request and result rules,
composes PBS directives from workspace settings, and submits the same manager
command once per requested count. In a real bundle, save it as `launcher`, make
it executable, use `"kind": "pbs"`, and list `qsub` in `required_binaries`.

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
    settings = request.get("settings", {})
    directory = workspace / ".httk-workspace" / "batch"
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
    lines.append("exec " + shlex.join(request["argv"]))
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
