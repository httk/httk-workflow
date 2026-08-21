# Task-manager usage

*For operators: workspaces, submission, managers, and inspecting what they
leave behind.* The everyday cycle is four commands:

```console
$ httk workflow workspace init --name default .
$ httk workflow job new --workflow vasp-relax --input structure=POSCAR --tag silicon
$ httk workflow run                 # serve jobs until idle (--idle keeps serving)
$ httk workflow workspace status
```

Managers drive every claimable job through its steps and record everything
they do; `job list`, `job show`, `job log`, and `job why` (why is this job
*not* progressing?) read it back, `precheck` reports readiness before a
manager ever starts, and `job debug` drives one job in the foreground while
you author a runner.

## Environment preludes

Shell setup an HPC job needs — `module load`, `source activate`, `export` — is
carried by two prelude layers, both run under `set -e` so a failing line aborts
the job instead of running the calculation in a half-set-up environment:

- **Layer 1, `environment.prelude`** — workspace-wide, applies to every job. Set
  it with `workspace settings set --key environment.prelude --value "…" NAME`. For a **slurm**
  remote it is written into the generated login-shell submit script, so it runs
  once inside the allocation and the manager and every runner it spawns inherit
  it; for a **local** remote it wraps the manager start; a manager you start
  yourself with `manager run` inherits your own shell, so run it from a shell
  that already has your setup.
- **Layer 2, `workflow-prelude`** — per workflow, keyed by workflow id, applies
  only to that workflow's jobs and runs *after* Layer 1. Each launch sources it
  with `bash -l` (a login shell, so `module` is available):

  ```console
  $ httk workflow workspace workflow-prelude set --workflow relax-vasp --value "module load VASP/6.2.1" default
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
