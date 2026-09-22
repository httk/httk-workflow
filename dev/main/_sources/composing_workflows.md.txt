# Composing workflows

*A workflow that builds on other workflows — calling one as a child job and
resuming when it finishes, not re-implementing what it already does.*

A runner does not have to do everything itself. From inside a running step it can
**call** another registered workflow — a packaged one like `vasp-relax`, or a
runner of your own — which runs as a child job with that workflow's own runner,
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

- It is **not** a single multi-step runner sharing one workdir. `vasp-relax-static`
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
{py:func}`~httk.workflow.scaffold.new_job` (and `httk workflow job new`) does:

- a **registered id or alias** — a packaged workflow such as `vasp-relax`;
- a **runner file** of your own (`./elastic_constant.py`);
- a **workflow package directory** (one holding `httk_workflow.toml`);
- a **bare language document** (a CWL file, a jobflow document, …).

Where the runner ends up depends on what it is. A registered packaged workflow is
referenced through the reserved `pkg:` form, so **nothing is copied** into the
workspace runner store. A runner file of your own is **published into the
workspace runner store**, which is content-addressed and idempotent: calling the
same runner twice publishes nothing the second time, and the child's `job.json`
pins it by digest so an upgrade underneath a queued job cannot change what runs.

## A worked example

`start` calls the packaged `vasp-relax` on a structure, waits for it, then calls a
second runner of your own on the relaxed structure, and finishes.

```python
from httk.workflow import Runner

run = Runner("elastic_constant")


@run.step
def start(a):
    a.call("vasp-relax", label="relax", files={"POSCAR": a.payload / "files" / "POSCAR"})
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
    httk_workflow_call relax vasp-relax --file "POSCAR=$HTTK_WORKFLOW_JOB_DIR/files/POSCAR"
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

- {doc}`sdks/native_bash_api` — `httk_workflow_call` and the rest of the Bash
  authoring SDK.
- {doc}`campaigns` — `ChildSpec` fan-out and partitioning, the other way one job
  becomes many.
- {doc}`declarations` — what a workflow records about its inputs and outputs.
- {doc}`vasp_runners` — the packaged VASP workflows a runner most often calls.
