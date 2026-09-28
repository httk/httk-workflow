# Composing workflows

*A workflow that builds on other workflows — calling one as a child job and
resuming when it finishes, not re-implementing what it already does.*

A runner does not have to do everything itself. From inside a running step it can
**call** another workflow — an installed one like `vasp.relax` from
[workflows-vasp](https://github.com/httk/workflows-vasp), or a runner of your own — which runs as a child job with that workflow's own runner,
and the calling step resumes when the child is done. This is how one workflow is
assembled out of others without copying their steps into it.

## The model

A calling workflow is an ordinary runner. {py:meth}`~httk.workflow.Attempt.call`
scaffolds a complete child payload for the workflow you name, stages the files
and inputs you give it, and registers it as a child of this attempt's outcome —
exactly the child {py:meth}`~httk.workflow.Attempt.spawn` registers, only its
runner is *another workflow's* runner instead of a step of yours. You then
{py:meth}`~httk.workflow.Attempt.gather` on it: the manager resumes the calling
job when the child is terminal, and the gathering step reads the child back
through {py:attr}`~httk.workflow.Attempt.children`.

This is a distinct tool from the two nearby ones:

- It is **not** a single multi-step runner sharing one workdir. `vasp.relax-static`
  (see {doc}`vasp_runners`) is one runner whose steps hand a directory from
  relaxation to a static run in place. Reach for that when the stages are one
  program's phases; reach for `call` when a stage *is* another workflow with its
  own runner, inputs, and failure handling.
- It is **not** a `ChildSpec` spawn. {py:class}`~httk.workflow.ChildSpec` and
  {py:meth}`~httk.workflow.Attempt.spawn` fan a job out into children that run
  *this same runner's* steps — the partitioned-campaign pattern of
  {doc}`campaigns`. `call` runs a *different* workflow, and carries input files a
  `ChildSpec` deliberately cannot.

## What can be called

`call` resolves its first argument exactly as
{py:func}`~httk.workflow.scaffold.new_job` (and `httk job new`) does:

- a **registered id or alias** — a workflow a Python package registers in-process;
- a **git URI** such as `git+https://github.com/httk/workflows-vasp#vasp-relax`,
  fetched and installed on first reference (see {doc}`workflow_uris`), or the
  short name of a workflow installed that way, such as `vasp.relax`;
- a **runner file** of your own (`./elastic_constant.py`);
- a **workflow package directory** (one holding `httk_workflow.toml`);
- a **bare workflow document** of a compat format (a CWL file, a jobflow document, …).

A workflow package **declares** what it calls in `[workflow.calls]` and calls it
by alias:

```toml
[workflow.calls]
relax = "vasp.relax"
```

```python
a.call("relax", label="relax", files={"POSCAR": a.payload / "files" / "POSCAR"})
```

The declaration makes the dependencies known before anything runs: a job is
refused at creation if a declared workflow is unknown, a manager does not start
it until each is installed (and built, when compiled) on its machine, and a call
to an undeclared workflow is refused. See the `[workflow.calls]` section of
{doc}`details/workflow_packages`. A runner file of your own has no manifest and
may call anything.

Where the runner ends up depends on what it is. A registered packaged workflow is
referenced through the reserved `pkg:` form, so **nothing is copied** into the
workspace runner store. A runner file of your own is **published into the
workspace runner store**, which is content-addressed and idempotent: calling the
same runner twice publishes nothing the second time, and the child's `job.json`
pins it by digest so an upgrade underneath a queued job cannot change what runs.

## A worked example

`start` calls the installed `vasp.relax` on a structure, waits for it, then calls a
second runner of your own on the relaxed structure, and finishes.

```python
from httk.workflow import Runner

run = Runner("elastic_constant")


@run.step
def start(a):
    a.call("vasp.relax", label="relax", files={"POSCAR": a.payload / "files" / "POSCAR"})
    a.gather("after_relax", when="all_succeeded", on_impossible="triage")


@run.step
def after_relax(a):
    relaxed = a.children["relax"]
    # File plumbing is explicit: copy the relaxed structure out of the child's
    # committed data and hand it to the next call as an input file.
    contcar = relaxed.data / "CONTCAR"
    a.call("./strain.py", label="strain", files={"POSCAR": contcar})
    a.gather("finish", when="all_succeeded", on_impossible="triage")


@run.step
def finish(a):
    strained = a.children["strain"]
    # Read the strain child's results out of its committed data here.
    a.state["strain_data"] = str(strained.data)
    a.succeed()


@run.step
def triage(a):
    a.fail("elastic.dependency_failed", "a called workflow did not succeed")


if __name__ == "__main__":
    raise SystemExit(run.main())
```

The same shape in a Bash runner uses `httk_workflow_call` and
`httk_workflow_gather`:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner elastic_constant start after_relax finish triage

step_start() {
    httk_workflow_call relax vasp.relax --file "POSCAR=$HTTK_WORKFLOW_JOB_DIR/files/POSCAR"
    httk_workflow_gather after_relax --when all_succeeded --on-impossible triage
}

step_after_relax() {
    contcar=$(httk_workflow_child relax data)/CONTCAR
    httk_workflow_call strain ./strain.sh --file "POSCAR=$contcar"
    httk_workflow_gather finish --when all_succeeded --on-impossible triage
}

step_finish() { httk_workflow_succeed; }
step_triage() { httk_workflow_fail elastic.dependency_failed "a called workflow did not succeed"; }

httk_workflow_main
```

## Passing results between calls

Wiring outputs into the next call's inputs is **explicit today**: read the child
back through {py:attr}`~httk.workflow.Attempt.children`, and copy the file you
want out of {py:attr}`~httk.workflow.ChildResult.data` (its committed
transactional data) or {py:attr}`~httk.workflow.ChildResult.workdir` into the
`files=` of the next `call`. There is no automatic output-role → input-role
matching between jobs. A workflow's {doc}`declarations` describe what it consumes
and produces as *provenance* — they are recorded, not used to plumb one call into
the next.

## Sharing files with children

Results flow back up through {py:attr}`~httk.workflow.Attempt.children`. Files
flow down to a child in one of two ways, and the choice is mostly about size.

**Copy them in at spawn time.** The `files=` of `call`, or a prepared payload
directory passed to `spawn`, puts the file in the child's own immutable payload,
where the child stages it with `stage_input`. This is the better default for
small inputs such as a POSCAR, an INCAR fragment, or a parameter file: the child
is self-contained, the payload digest covers exactly what it ran with, a later
step of the parent cannot change it underneath the child, and the child can be
transferred to another workspace on its own.

**Read them in place.** Copying stops making sense when the file is large and
many children share it: a CHGCAR that non-self-consistent band-structure
children start from, or a multi-gigabyte WAVECAR that same-mesh follow-ups
such as optics or hybrid-functional runs start from. A child then locates its
parent with {py:attr}`~httk.workflow.Attempt.parent` (`httk_workflow_parent` in
Bash, and the same `parent` read in every other SDK) and reads the file from the
parent's workdir, or links to it:

```python
@run.step
def bands(a):
    parent = a.parent
    if parent is None or parent.workdir is None:
        a.fail("bands.no_parent_workdir", "no persistent parent workdir to read CHGCAR from")
        return
    link = a.workdir / "CHGCAR"
    link.unlink(missing_ok=True)  # a replayed step finds the link already there
    link.symlink_to(parent.workdir / "CHGCAR")
    ...  # run VASP with ICHARG = 11 and LCHARG = .FALSE.
    link.unlink()  # an absolute link would keep this job from being transferred
```

Reading in place gives up the guarantees a copy has, so it comes with rules:

- **The parent writes the shared files before it publishes the spawning
  outcome, and leaves them alone until every child that reads them is
  terminal.** A child can start as soon as that outcome is published, and
  nothing freezes the parent's workdir. With an `all_*` join condition that
  point is the gather; with `any_*`, `at_least`, or an `on_impossible` route the
  parent resumes while siblings may still run, and a step that rewrites the file
  then changes it for all of them. On a storage-durable workspace, synchronize
  the shared files yourself before publishing: the outcome is flushed, the
  parent's workdir is not.
- **A link is only safe for a file the child does not write.** VASP rewrites
  WAVECAR and CHGCAR at the end of a run unless `LWAVE` and `LCHARG` are
  `.FALSE.`, and writing through a symbolic link, or to a hard link, changes the
  parent's file for every sibling. Copy the file when the child needs to write
  its own.
- **A link leaving the payload blocks transfer.** A transfer refuses a payload
  containing an absolute symbolic link, or a relative one pointing outside it,
  even when parent and child move together. Remove the link at the end of the
  step, or read the file directly, if the child must stay transferable.
- **The parent needs a persistent workdir.** An isolated workdir is a fresh
  directory per attempt that the child cannot name, so `parent.workdir` is
  `None` for such a parent.
- **The child stays with its parent.** A transfer moves a parent together with
  its spawned children and refuses to move a child on its own (see
  {doc}`remotes`), so the pair normally stays together; an operator can detach a
  child with `httk job detach`, and a detached child's `a.parent` is `None`.
  `a.parent` is also `None` whenever the parent is not in the child's
  workspace: after it was removed, or in the rarer cases the rule cannot cover
  (a parent from before tree records existed, a tree split by an interrupted
  transfer, or a transfer through an older *httk-workflow*). Do not remove a
  parent while children that read it in place are still running.

## Failure semantics

A called child that fails is an ordinary join dependency. When the join condition
can no longer be met, the calling job advances to the step named by
`on_impossible` if one is given, and otherwise fails with `dependency_failure`
(see {py:meth}`~httk.workflow.Attempt.gather`). Retries belong to the *child's
own* runner and retry policy; the caller does not retry the child on its behalf.
The calling job's private {py:attr}`~httk.workflow.Attempt.state` survives across
the wait, so a `start` step can record what it needs and read it back in the
gathering step.

## Requirements and limits

- **Workspace reachability.** `call` scaffolds into, and publishes a runner file
  into, the workspace the calling step runs in, so the workspace root must be
  reachable from where the step executes — the same condition
  {py:attr}`~httk.workflow.Attempt.children` needs. For a packaged workflow only
  the `pkg:` reference is written, so nothing is copied, but the child payload is
  still built in the workspace.
- **Nesting is unrestricted.** A called workflow is just another job, so a
  workflow it calls may itself call further workflows; the depth is not capped.
- **A child inherits its parent's workspace and placement.** Like every spawned
  child, a called child is created in the calling job's workspace (and its
  placement, unless you pass `placement=`), so the whole tree below a root stays
  where the root was assigned — the convention {doc}`campaigns` relies on.

## Where to go next

- {doc}`sdks/bash_api` — `httk_workflow_call` and the rest of the Bash
  authoring SDK.
- {doc}`campaigns` — `ChildSpec` fan-out and partitioning, the other way one job
  becomes many.
- {doc}`declarations` — what a workflow records about its inputs and outputs.
- {doc}`vasp_runners` — the VASP workflows a runner most often calls.
