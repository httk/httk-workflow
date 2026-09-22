# Task-manager usage

*For operators: workspaces, submission, managers, and inspecting what they
leave behind.* The everyday cycle is four commands:

```console
$ httk workspace init --name default .
$ httk job new --workflow vasp-relax --input structure=POSCAR --tag silicon
$ httk workflow run                 # serve jobs until idle (--idle keeps serving)
$ httk workspace status
```

Managers drive every claimable job through its steps and record everything
they do; `job list`, `job show`, `job log`, and `job why` (why is this job
*not* progressing?) read it back, `precheck` reports readiness before a
manager ever starts, and `job debug` drives one job in the foreground while
you author a runner.

Use repeatable `--worker-resource NAME COUNT` options to advertise capacities;
inside an active SLURM allocation, local managers derive `procs`, `mem`,
`gpus`, and `nodes` from the allocation variables. `--count N` starts N managers
at the selected launch site, with metadata directories removed after clean
process exit and one workspace-level append-only manager log.

For example, a manager can advertise CPUs, memory, and two license slots:

```console
httk workflow run --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000 \
  --worker-resource matlab_license_slots 2
```

Workflow and per-step declarations, including dense resource packing, are
covered in the {doc}`details/taskmanager` resource reference.

## Environment preludes

Shell setup an HPC job needs — `module load`, `source activate`, `export` — is
carried by two prelude layers, both run under `set -e` so a failing line aborts
the job instead of running the calculation in a half-set-up environment:

- **Layer 1, `environment.prelude`** — workspace-wide, applies to every job. Set
  it with `workspace settings set --key environment.prelude --value "…" NAME`. A
  launcher runs it before the manager: the Slurm batch script runs the prelude
  under `set -e`, and the process launcher wraps the child in the same shell.
  After a prelude, the manager is invoked as `manager.command` on `PATH`
  (default `httk`), so a module-loaded interpreter is selected; without a
  prelude the launcher preserves the Python interpreter command supplied by the
  caller. A manager started directly with `manager run` inherits your shell.
- **Layer 2, `workflow-prelude`** — per workflow, keyed by workflow id, applies
  only to that workflow's jobs and runs *after* Layer 1. Each launch sources it
  with `bash -l` (a login shell, so `module` is available):

  ```console
  $ httk workspace workflow-prelude set --workflow relax-vasp --value "module load VASP/6.2.1" default
  ```

Enabling either layer runs the manager and runners under a **login shell**,
which re-sources login profiles (`/etc/profile`, `~/.bash_profile`, …); that can
reset generic variables such as `PATH` and `LD_LIBRARY_PATH`, but the prelude
runs last and is therefore the intended override point.

Preludes are workspace-local state: they do **not** travel with `transfer`. A job
moved to another workspace runs under that workspace's own preludes, so set the
destination's preludes there. See {doc}`details/workflow_cli` for the full
command reference.

The full guide, {doc}`details/taskmanager`, covers workspace naming and
defaults, submission forms, manager scheduling and capabilities, placement,
requests, inspection and repair (`fsck`, `gc`, `unlock`).
