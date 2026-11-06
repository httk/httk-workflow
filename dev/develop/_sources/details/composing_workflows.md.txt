# Composing workflows

A runner does not have to do everything itself. From inside a running step it
can **call** another workflow installed in the same workspace, such as
`vasp.relax` from [workflows-vasp](https://github.com/httk/workflows-vasp) or
a package of your own. The called workflow runs as a child job with its own runner, and the
calling step resumes when the child is done. This assembles one workflow out
of others without copying their steps.

## The model

A calling workflow is an ordinary runner. {py:meth}`~httk.workflow.Attempt.call`
scaffolds a complete child payload for the named workflow, stages the files
and inputs you give it, and registers it as a child of this attempt's outcome.
This is the same child {py:meth}`~httk.workflow.Attempt.spawn` registers,
except that it runs another workflow's runner instead of a step of yours. You
then {py:meth}`~httk.workflow.Attempt.gather` on it: the manager resumes the
calling job when the child is terminal, and the gathering step reads the child
through {py:attr}`~httk.workflow.Attempt.children`.

`call` differs from two nearby tools:

- **A multi-step runner sharing one workdir.** `vasp.relax-static` (see
  {doc}`/vasp_runners`) is one runner whose steps hand a directory from
  relaxation to a static run in place. Use that when the stages are phases of
  one program, and `call` when a stage is another workflow with its own
  runner, inputs, and failure handling.
- **A `ChildSpec` spawn.** {py:class}`~httk.workflow.ChildSpec` and
  {py:meth}`~httk.workflow.Attempt.spawn` fan a job out into children that run
  this same runner's steps, the partitioned-campaign pattern of
  {doc}`/campaigns`. `call` runs a different workflow and can carry input
  files, which a `ChildSpec` cannot.

## What can be called

A workflow package **declares** what it calls in `[workflow.calls]`, and a
step calls it by alias:

```toml
[workflow.calls]
relax = "vasp.relax"
```

```python
a.call("relax", label="relax", files={"POSCAR": a.payload / "files" / "POSCAR"})
```

Each value is a workflow name or a git URI pinned to a full commit (see
{doc}`/details/workflow_uris`); a path is refused. Installing the calling
package (`httk workflow install --workspace WS`, `job new --install`)
installs its calls too, recursively, and records each alias against the
installed id it resolved to. `call` then resolves only through the workspace
store:

- the first argument is a declared alias, or the installed id an alias names;
  anything else is refused with the declared calls listed;
- the called workflow must be installed in the workspace, or the call is
  refused with the `httk workflow install` command that fixes it;
- a manager does not claim a job until its workflow and every workflow it
  calls, transitively, are installed (and built, when compiled), so a
  missing callee shows up in `httk job why` and `httk workflow precheck`
  before any step runs.

An ad hoc workflow (a runner file given to `job new --from-runner`) has no
`[workflow.calls]`, so it cannot call anything; give it a package manifest to
compose it. See the `[workflow.calls]` section of
{doc}`/details/workflow_packages`.

The child is built from the callee's installed package, exactly as
{py:func}`~httk.workflow.scaffold.new_job` builds a job of it: its files and
inputs are staged and its instantiate hook runs, inside the calling attempt.
Its `job.json` names the callee's installed id, which the manager runs from
the store; nothing is copied into the workspace beyond the child payload.

## A worked example

`start` calls `vasp.relax` on a structure and waits for it, then calls a
second workflow of your own on the relaxed structure, and finishes. The
package declares both calls:

```toml
[workflow]
name = "examples.elastic-constant"

[workflow.runner]
entry = "run.py"
steps = ["start", "after_relax", "finish", "triage"]

[workflow.calls]
relax = "git+https://github.com/httk/workflows-vasp@458aacb2493586faa2c9ac033334457569aeaf75#vasp-relax"
strain = "examples.strain"
```

```python
from httk.workflow import Runner

run = Runner("examples.elastic-constant")


@run.step
def start(a):
    a.call("relax", label="relax", files={"POSCAR": a.payload / "files" / "POSCAR"})
    a.gather("after_relax", when="all_succeeded", on_impossible="triage")


@run.step
def after_relax(a):
    relaxed = a.children["relax"]
    # File plumbing is explicit: hand the relaxed structure from the child's
    # workdir to the next call as an input file.
    a.call("strain", label="strain", files={"POSCAR": relaxed.workdir / "CONTCAR"})
    a.gather("finish", when="all_succeeded", on_impossible="triage")


@run.step
def finish(a):
    strained = a.children["strain"]
    # Read the strain child's results out of its workdir here.
    a.state["strain_results"] = str(strained.workdir)
    a.succeed()


@run.step
def triage(a):
    a.fail("elastic.dependency_failed", "a called workflow did not succeed")


if __name__ == "__main__":
    raise SystemExit(run.main())
```

The same shape in a Bash runner, with the same manifest and `entry = "run"`,
uses `httk_workflow_call LABEL ALIAS` and `httk_workflow_gather`:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner examples.elastic-constant start after_relax finish triage

step_start() {
    httk_workflow_call relax relax --file "POSCAR=$HTTK_WORKFLOW_JOB_DIR/files/POSCAR"
    httk_workflow_gather after_relax --when all_succeeded --on-impossible triage
}

step_after_relax() {
    contcar=$(httk_workflow_child relax workdir)/CONTCAR
    httk_workflow_call strain strain --file "POSCAR=$contcar"
    httk_workflow_gather finish --when all_succeeded --on-impossible triage
}

step_finish() { httk_workflow_succeed; }
step_triage() { httk_workflow_fail elastic.dependency_failed "a called workflow did not succeed"; }

httk_workflow_main
```

## Passing results between calls

For a fixed call, wiring outputs into the next call is explicit. Read the child back
through {py:attr}`~httk.workflow.Attempt.children` and copy the file you want
from {py:attr}`~httk.workflow.ChildResult.workdir` (its persistent `run/`)
or {py:attr}`~httk.workflow.ChildResult.data` (what it committed with
transactions) into the `files=` of the next `call`. A call through a
[call slot](#call-slots) instead passes values by role: the child records the
property values the slot's interface names, and
{py:meth}`~httk.workflow.Attempt.child_outputs` reads them back.

## Call slots

A **call slot** is a call whose target the job's creator chooses, constrained by
an interface: "a structure in, the forces on that structure out", whichever code
computes them. A phonon workflow declares one slot and works with DFT or an
interatomic potential alike.

### Declaring a slot

A table in `[workflow.calls]` is a slot; a string stays a fixed call:

```toml
[workflow.calls]
relax = "vasp.relax"                       # a fixed call

[workflow.calls.forces]                    # a call slot
description = "computes the forces on every site of one structure"
declaration_file = "calls/structure-forces.json"
default = "ase.static"                     # optional
default_parameters = { calculator = "ase.calculators.emt:EMT" }   # optional, only with default
```

`declaration_file` names the slot's **interface declaration**, a package member
shaped like a workflow declaration (see
[interface declarations](declarations.md#interface-declarations)):

```json
{
  "title": "structure to forces",
  "inputs": [{"name": "structure", "entry_type": "structures",
              "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures"}],
  "outputs": [{"name": "forces", "entry_type": "records",
               "ref": "https://schemas.httk.org/defs/v0.1/properties/core/cartesian_site_forces",
               "product_of": "structure"}]
}
```

### What fits a slot

A workflow fits a slot when its declaration (its external declaration file, or
the one generated from its manifest) matches the interface:

- each interface input matches the workflow's input with an equal `entry_type`
  and `ref`: the one with the same name when several do, otherwise the only one;
- each interface output matches an output the same way, which must in addition
  be a `product_of` the input its interface `product_of` maps to, so forces on a
  relaxed structure never fit "forces on the input structure";
- no two interface entries map to one workflow input or output, and every
  required input of the workflow is supplied by the interface;
- the workflow has no call slots of its own.

IRIs compare exactly, versions included. A refusal lists every reason at once.

### Binding a slot

Every job binds each slot of its workflow when it is created: to the workflow
the creator names, or else to the slot's `default`. A slot without either
refuses the job. Installing a workflow never installs its defaults (it prints a
notice for each one that is not installed), so a default must be installed
before a job relies on it, or `job new --install` installs it. When the binding
resolves to the same installation the recorded default resolves to (a short
name resolves to its newest installation), a slot passes its
`default_parameters`, which `--call-parameter` overrides entry by entry; bound
to another installation, they are ignored.

```bash
httk job new --workflow phonopy.phonons --input structure=POSCAR \
    --call forces=ase.static --call-parameter forces calculator=ase.calculators.emt:EMT
```

`--call ALIAS=WORKFLOW` names the workflow (a workflow name or commit-pinned
git URI, like a `[workflow.calls]` reference (never a path), installed first
with `--install`); `--call-parameter ALIAS NAME=VALUE` gives it
a parameter, spelled as for `--parameter`. In Python these are the `calls=` and
`call_parameters=` arguments of {py:func}`~httk.workflow.scaffold.new_job`. The
binding is fitted and its parameters type-checked at creation, recorded in the
job's `job.json`, and the job waits until the bound workflow is installed;
`workflow build` also builds the installed defaults.

### Calling through a slot

```python
for index, supercell in enumerate(supercells):
    a.call("forces", label=f"d{index}", inputs={"structure": supercell})
a.gather("analyse")
```

```python
def analyse(a):
    forces = [a.child_outputs(child, call="forces")["forces"] for child in a.children.succeeded]
```

- `inputs` are named by the interface and every one must be given; they reach
  the bound workflow's inputs by the fit. A path is loaded and restaged in the
  bound workflow's format when its destination has a writer, so a Bash runner
  passing `--input structure=supercell.cif` reaches a POSCAR reader.
- The binding's parameters are passed; naming one of them again in
  `parameters=` is refused.
- A staged structure is read back and compared with the supplied one site by
  site: chemical symbols, fractional coordinates within 1e-9 (modulo 1) and the
  lattice within 1e-9 relative. Writers such as POSCAR group sites by species,
  so order the sites species-contiguously before calling.
- The child records the interface's outputs when it succeeds: its own collect
  hook runs on it and each value lands in `.httk-job/outputs.json`. A missing
  role or a value of another definition fails the child with
  `workflow.outputs_unavailable`. The hook sees the job as its data will be
  once the outcome commits: `result_file` and `read_file` find what the
  succeeding attempt published with `a.put` or a transaction before the
  committed `data/`. An executable hook reads paths itself, so it sees the
  committed `data/` and the workdir only.
- {py:meth}`~httk.workflow.Attempt.child_outputs` reads that file back as plain
  JSON, per-site values in the order of the structure you passed, with no
  installation of the called workflow needed.

### Limits

- No nested slots: a workflow with slots cannot be bound to a slot, and one
  called through a fixed call must give every slot a default.
- IRIs compare exactly; a definition of another version does not fit.
- `child_outputs` is Python-only; a Bash step reads `.httk-job/outputs.json`
  of the child itself.
- The bound workflow's collect hook runs inside its last attempt, so keep it
  deterministic and light.
- Values persisted for a slot call are checked for their definition and
  finiteness only; full property validation happens at collection.

## Sharing files with children

Results flow back up through {py:attr}`~httk.workflow.Attempt.children`. Files
flow down to a child in one of two ways, chosen mostly by size.

### Copy them in at spawn time

The `files=` of `call`, or a prepared payload directory passed to `spawn`,
puts the file in the child's own immutable payload, where the child stages it
with `stage_input`. This is the better default for small inputs such as a
POSCAR, an INCAR fragment, or a parameter file. The child is self-contained:
a later parent step cannot change its input underneath it, and a seal of it covers
exactly what it ran with.

### Read them in place

Copying stops making sense for a large file that many children share: a
CHGCAR that non-self-consistent band-structure children start from, or a
multi-gigabyte WAVECAR that same-mesh follow-ups such as optics or
hybrid-functional runs start from. The child then locates its parent with
{py:attr}`~httk.workflow.Attempt.parent` (`httk_workflow_parent` in Bash, and
the same `parent` read in every other SDK) and reads the file from the
parent's workdir, or links to it:

```python
@run.step
def bands(a):
    parent = a.parent
    if parent is None:
        a.fail("bands.no_parent", "no parent workdir to read CHGCAR from")
        return
    link = a.workdir / "CHGCAR"
    link.unlink(missing_ok=True)  # a replayed step finds the link already there
    link.symlink_to(parent.workdir / "CHGCAR")
    ...  # run VASP with ICHARG = 11 and LCHARG = .FALSE.
    link.unlink()  # a link out of the payload would dangle after a transfer
```

`a.parent` is a read-only lookup by the parent's recorded placement, as the
children's paths in `a.children` are located when it is first read; a
confined attempt sees both read-only. Reading in
place gives up the guarantees of a copy, so it comes with rules:

- **The parent writes the shared files before it publishes the spawning
  outcome, and leaves them alone until every child that reads them is
  terminal.** A child can start as soon as that outcome is published, and
  nothing freezes the parent's workdir. With an `all_*` join condition that
  point is the gather. With `any_*`, `at_least`, or an `on_impossible` route
  the parent resumes while siblings may still run, and a step that rewrites
  the file changes it for all of them. On a storage-durable workspace,
  synchronize the shared files yourself before publishing: the outcome is
  flushed, the parent's workdir is not.
- **A link is only safe for a file the child does not write.** VASP rewrites
  WAVECAR and CHGCAR at the end of a run unless `LWAVE` and `LCHARG` are
  `.FALSE.`; unconfined, writing through a symbolic link changes the parent's
  file for every sibling, and confined, the write fails. Copy the file when
  the child needs to write its own.
- **A link leaving the payload does not travel.** It dangles once the job is
  moved, and an untrusted bundle refuses symbolic links outright. Remove the
  link at the end of the step, or read the file directly.
- **The child stays with its parent.** A parent and its children move
  together (`--tree`; see {doc}`/details/remotes`). An operator can detach a
  child with `httk job detach`, and a detached child's `a.parent` is `None`.
  `a.parent` is also `None` whenever the parent is not found at its recorded
  placement in the child's workspace, for example after it was removed or
  moved away alone. Do not remove a parent while children that read it in
  place are still running.

## Failure semantics

A called child that fails is an ordinary join dependency. When the join
condition can no longer be met, the calling job advances to the
`on_impossible` step if one is given, and otherwise fails with
`dependency_failure` (see {py:meth}`~httk.workflow.Attempt.gather`). Retries
belong to the child's own runner and retry policy; the caller does not retry
the child. The calling job's private {py:attr}`~httk.workflow.Attempt.state`
survives the wait, so a `start` step can record what it needs and read it back
in the gathering step.

A parent that fails with `dependency_failure` keeps its join. An operator fixes
the child and `continue`s it, then `continue`s the parent, which reopens the
wait with the same join and no new attempt. `restart` on the parent instead
reruns the spawning step, which reuses the children that still exist (child
labels are unique per parent job). See
{doc}`/restarting_jobs`.

## Requirements and limits

- **Installed callees.** Every workflow a job calls, transitively, is
  installed in the job's workspace; jobs carry workflow ids, never the
  workflows themselves, so a job moved to another workspace needs them
  installed there too.
- **Workspace reachability.** `call` reads the callee's installed package
  from the workspace store and stages the child in the calling attempt's
  outcome, so the workspace root must be readable from where the step
  executes, as {py:attr}`~httk.workflow.Attempt.parent` also requires. This
  works in a confined attempt, which sees `jobs/` and `workflows/` read-only
  and `.httk-workspace/format.json` as a read-only content snapshot, while
  writing only its own job directory. Other workspace paths, including the
  rest of `.httk-workspace/` and `exchange/`, are absent.
- **Unrestricted nesting.** A called workflow is just another job, so it may
  call further workflows; the depth is not capped, but the default placement stops nesting once it
  would exceed 1024 bytes (the tree continues at that level's placement).
- **Workspace is inherited; placement is nested.** Like every spawned child, a
  called child is created in the calling job's workspace. Unless you pass
  `placement=`, its placement is `<parent placement>/<parent job key>.children`;
  the workspace setting `children.placement=flat` gives it the parent's
  placement instead. Either way the whole tree below a root stays under the
  root's placement, so a `--placement-prefix` manager serves it, which
  {doc}`/campaigns` relies on. A `placement=` must not
  contain a component that parses as a job key, since job directories never
  nest; see the
  [placement rules](workflow_filesystem_api.md#placement-rules).
- **Gather size is bounded only by `state.json`** (100 MiB). Up to about
  10 000 children per parent stays cheap; beyond that, fan out through
  intermediate parents.
- **Order is unspecified.** Listings and collect yield a tree's descendants
  before its root (`.children` sorts before the job directory); do not rely on
  order.

## Where to go next

- {doc}`/sdks/bash_api`: `httk_workflow_call` and the rest of the Bash
  authoring SDK.
- {doc}`/campaigns`: `ChildSpec` fan-out and partitioning, the other way one
  job becomes many.
- {doc}`/details/declarations`: what a workflow records about its inputs and outputs.
- {doc}`/vasp_runners`: the VASP workflows a runner most often calls.
