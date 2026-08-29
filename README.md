# httk-workflow

![Status: Early beta](https://img.shields.io/badge/status-early--beta-orange)

> **⚠️ EARLY BETA**
>
> This is an early beta release of *httk₂*. The organization of the packages
> and their APIs should not yet be regarded as stable, and may change between
> releases.

*httk-workflow* is the filesystem-native workflow engine for
[*httk₂*](https://github.com/httk/httk2).

It provides the `httk.workflow` Python API in three layers — the language-neutral
filesystem protocol (`httk.workflow.protocol`), the execution and authoring
surface (`httk.workflow` — `Runner`, `Attempt`), and orchestration and management
(`Workspace`, `TaskManager`, `collect`, and named submodules) — and registers
`httk workflow`, the command tree for everything below. Legacy
`ht_steps`/`ht_run` workflows are converted to packages and execute through
the normal manager in the same *httk₂* workspace. Jobs communicate through atomically
published filesystem state, so interrupted managers and calculations can be
recovered without cleanup hooks.

*httk₂* workflows are language-independent: runners, hooks, and postprocess
scripts can be written in any language; a workflow is a manifest plus the
members it references. Python hooks remain first-class, with an in-process fast
path. Successful hook outputs use the same assembly semantics as executable
hooks; collector failures differ deliberately: registered `.py` exceptions
abort iteration, while executable-hook errors degrade per job and continue the
sweep.

From nothing to a finished VASP relaxation, without writing a runner:

```console
httk project init --name quickstart .
httk job new --workflow vasp-relax --input structure=POSCAR --tag silicon
httk workspace settings set --key vasp.command --value "$PWD/examples/mock_vasp.py" default
httk workflow run
httk workflow collect
```

[`docs/quickstart.md`](docs/quickstart.md) explains each command, and
`examples/quickstart.sh` runs the whole sequence — with the mock VASP above
standing in for VASP on a machine that has none.

## Install

```console
python -m pip install httk-workflow
```

One optional extra exists. `httk-workflow[cwl]` adds the CWL *parser* needed to
prepare the CWL language realization; executing the normalized plan needs
nothing extra, so the extra belongs only on the machine that creates the job.
Python Workflow Definition documents need no extra at all.

## Running tests

The everyday regression gate is the normal profile: `make test`. It runs a
parallel pass (`PYTHONPATH=src python -m pytest -q -m "not timing"`) followed by
a serial pass (`PYTHONPATH=src python -m pytest -q -m timing -n 0`). The default
marker selection omits only full-depth `extended` parameter cases.
Profiled tests keep one test body and reduce their input scale in normal mode;
they still exercise every property with representative inputs.

Run `make test-extended` at phase ends and in CI to select every parameter case
at its current full depth; it uses the same two passes with
`HTTK_TEST_PROFILE=extended`. The underlying knob is
`HTTK_TEST_PROFILE=normal|extended`; an explicit extended parallel invocation
is `HTTK_TEST_PROFILE=extended PYTHONPATH=src python -m pytest -q -m "not timing"`,
followed by the serial timing command above. `make ci` uses the same extended
profile with fast-fail enabled. Tests whose process timing must remain
comparable use the `timing` marker and run serially after the parallel pass.

## What it does

- **Runs workflows without a graph.** A step decides at run time which children
  to spawn and which step runs next, so a two-step relaxation and a
  partitioned child campaign are the same engine —
  [runners in Python](docs/runtime_helpers.md),
  [in Bash](docs/sdks/native_bash_api.md),
  [in C](docs/sdks/native_c_api.md),
  [in modern Fortran](docs/sdks/native_fortran_api.md), or
  [in safe Rust](docs/sdks/native_rust_api.md), with a
  [normative parity table](docs/sdks/sdk_parity.md) between the language SDKs.
- **Recovers instead of cleaning up.** One atomically renamed state marker per
  job is the source of truth, so an interrupted manager, node, or calculation is
  resumed from what is on disk. The protocol is specified in
  [`docs/workflow_filesystem_api.md`](docs/workflow_filesystem_api.md).
- **Ships complete VASP runners**, so an ordinary relaxation or single point
  needs no runner written at all — see [`docs/vasp_runners.md`](docs/vasp_runners.md).
- **Runs workflows written elsewhere.** Python Workflow Definition and CWL
  documents become ordinary jobs; see
  [`docs/workflow_languages.md`](docs/workflow_languages.md).
- **Reaches other machines.** Versioned [remote adapters](docs/remotes.md)
  transport files and run commands on a cluster; the workspace's [launcher](docs/launchers.md)
  starts its managers, and crash-recoverable detached transfer fetches results back.
- **Manages projects and identity**: XDG configuration, signed project
  manifests, and workspace policy — see
  [`docs/workflow_cli.md`](docs/workflow_cli.md).
- **Hands results to a data layer.** [`collect`](docs/collecting.md) yields one
  collected result per stopped job; *httk-workflow* itself has no database dependency.
- **Keeps *httk* v1 workflows running.** Converted `ht_steps`/`ht_run` packages
  execute unchanged on the normal engine — see
  [`docs/v1_compatibility.md`](docs/v1_compatibility.md) and the
  [migration guide](docs/httk_v1_migration_guide.md).

```console
httk workflow project init --name example .
httk workflow project manifest create .
httk workspace status
```
