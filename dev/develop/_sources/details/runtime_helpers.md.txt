# Native runner helpers in detail

This page teaches the Python runner API. The complete authoring surface,
Python beside its Bash equivalent, is the table in {doc}`/sdks/sdk_parity`,
which is normative and enforced by the test suite.

A native *httk₂* runner is one program that implements the steps of one
workflow. The manager launches it once per attempt, tells it which step to
run, and reads exactly one published outcome back. `Runner` registers the
steps, and `Runner.main` dispatches the requested step to its handler with an
`Attempt` object carrying everything the step may read, do, and publish.

Nothing declares the shape of a workflow. A step decides at run time which
children to spawn and which step runs next, so a job's graph is whatever its
steps published. One runner can drive a two-step relaxation or a large,
partitioned child campaign without a graph definition anywhere.

## A complete runner

This defect campaign characterizes a structure, spawns one child job per
candidate site, gathers them, and either reports or triages.

```python
#!/usr/bin/env python3
"""Defect campaign: characterize, relax every site, aggregate, triage."""

import json

from httk.workflow import ChildSpec, Runner

run = Runner("defects")


@run.step
def characterize(a):
    result = a.run(["characterize", "--structure", "POSCAR"], timeout=600)
    if result.returncode:
        a.fail("characterize_failed", "site characterization failed", retryable=True)
        return
    sites = json.loads((a.workdir / "sites.json").read_text(encoding="utf-8"))
    a.state["sites"] = len(sites)
    for index, site in enumerate(sites):
        a.spawn(
            ChildSpec(step="relax", parameters={"site": site}, maximum_attempts_per_activation=2),
            label=f"site-{index}",
        )
    a.gather("aggregate", when="all_terminal", on_impossible="triage")


@run.step
def relax(a):
    site = a.parameter("site")
    result = a.run(["relax", "--site", json.dumps(site)], timeout=3600)
    if result.timed_out:
        a.retry("the relaxation timed out")
    elif result.returncode:
        a.fail("relax_diverged", f"site {site} did not relax")
    else:
        a.succeed()


@run.step
def aggregate(a):
    energies = {
        child.label: json.loads((child.workdir / "energy.json").read_text(encoding="utf-8"))
        for child in a.children.succeeded
    }
    (a.workdir / "campaign.json").write_text(json.dumps(energies, sort_keys=True), encoding="utf-8")
    failed = [child.label for child in a.children.failed]
    if failed:
        a.advance("triage", state={"failed": failed})
    else:
        a.succeed()


@run.step
def triage(a):
    failed = a.state.get("failed") or [child.label for child in a.children.failed]
    a.log.append("triage", f"{len(failed)} of {a.state['sites']} sites did not relax")
    a.fail("campaign_incomplete", "not every site relaxed", details={"failed": failed})


if __name__ == "__main__":
    raise SystemExit(run.main())
```

The runner is a single file, and every child job runs the same file at a
different step. Creating a job from the file installs it in the workspace as
an `adhoc:defects@<sha12>` workflow, and every job, parent and child, runs that
installation:

```console
httk job new --workspace workflow-workspace --from-runner defects.py --step characterize \
    --parameter encut=520 --parameter 'supercell=[2, 2, 2]' --placement project/si-vacancies
```

```python
from httk.workflow import Workspace, new_job

workspace = Workspace("workflow-workspace")
job = new_job(
    workspace,
    "defects.py",
    step="characterize",
    parameters={"encut": 520, "supercell": [2, 2, 2]},
    placement="project/si-vacancies",
)
```

A directory package is installed the same way with `httk workflow install
--workspace WS DIR`, or by `job new --install` / `new_job(..., install=True)`;
see {doc}`/details/workflow_packages`.

## Steps and dispatch

### Registering steps

`Runner(workflow, inputs=...)` declares creation-time staged inputs as `name`
→ destination, or `null` for a hook-consumed input. The optional `inputs`
member appears in the runner description when non-empty.

`@run.step` registers the handler under the function's own name, and
`@run.step(name="collect-results")` names it explicitly. Registering a name
twice is an error at decoration time, so the step set, `run.steps`, is
complete and unambiguous before any work starts.

Every step name a handler publishes is therefore checked at the call that
names it: `a.advance("colect")` raises immediately, listing the registered
steps, instead of failing the job one activation later. The same check covers
`gather`, its `on_impossible` step, and the `step` of a `ChildSpec` that
inherits this runner.

### How `Runner.main` ends

`Runner.main` turns every ending into an outcome:

| Ending | Published outcome |
| --- | --- |
| the handler publishes one | that outcome |
| the handler returns without publishing | `fail("no_outcome", ...)` |
| the step is not registered | `fail("unknown_step", "... registered steps: ...")` |
| the handler raises | an `error.json` breadcrumb, then the exception reaches the manager, whose retry policy decides |

`HTTK_WORKFLOW_DESCRIBE=1` (or `--describe`) makes `main` print
`{"format": "httk-workflow-runner-description", "format_version": 2, "workflow":
..., "steps": [...]}` and exit without binding an attempt or touching
anything, so a tool can list the steps of a runner it is not running. The
step set is also recorded in the job's `state.json` as `runner_steps`, from the
first outcome the job publishes.

### Creation-time instantiation

There are two equivalent creation-time forms. In a Python runner,
`@run.instantiate` is the in-process hook that replaces v1's
`ht.instantiate.py`. `new_job(s)` runs it on the creating machine after
declared inputs are staged and before `job.json` is finalized. Its
`InstantiateContext` provides:

- the staging `payload`;
- read-only `inputs`;
- mutable caller-supplied `parameters`;
- read-only declared `defaults`, applied after the hook to every parameter
  still absent;
- the caller's `tag`, and `suggest_tag`, which supplies a tag only when the
  caller did not.

```python
@run.instantiate
def instantiate(ctx):
    (ctx.payload / "files" / "generated.txt").write_text(ctx.inputs["structure"])
    ctx.parameters["derived"] = "ready"
    ctx.suggest_tag("generated")
```

The hook may write anywhere below `payload`. It is trusted workflow code,
since the file is installed and executed anyway.

A directory package may instead name any executable member in
`[workflow.instantiate].file`. The framework starts it in the staging payload,
pre-serializes hook-consumed inputs, and sends the JSON
`httk-workflow-instantiate` envelope on stdin. The executable returns
`{"parameters": {...}}` and may return a string `tag`; a nonzero exit or
malformed output aborts submission. This language-neutral form has the same
parameter, tag, payload, and input semantics as the Python hook: `parameters`
holds only caller-supplied values, and the declared defaults arrive
separately (`defaults`, like `ctx.defaults`) and are applied after the hook.
{doc}`/details/workflow_packages` holds the normative envelope and serialization
rules.

## What an attempt reads

| Member | What it is |
| --- | --- |
| `a.context` | the immutable identity and restart evidence of this attempt |
| `a.step` | the step this attempt runs |
| `a.payload`, `a.workdir`, `a.workspace`, `a.data` | absolute paths; `a.workdir` is the persistent `run/` and `a.data` is `data/` of the payload, which exists once a transaction put something there |
| `a.job`, `a.parameters`, `a.parameter(name[, default])` | the job definition and its opaque implementation `parameters` object |
| `a.stage_input(name, destination[, default])` | copies the payload file parameter `name` names into the workdir at `destination`; `None` when the payload has no such file |
| `a.state` | dict-like JSON state that belongs to the **job** |
| `a.log` | the runner's append-only structured evidence log (`.httk-job/runlog.jsonl`); the owner-written timeline is `logs/runlog.jsonl` |
| `a.children` | the typed children of the join that started this activation |
| `a.parent` | the job that spawned this one, located for reading its files in place; `None` for a root job |
| `a.declaration(name)` | one workflow declaration: the observed document, else the declared one, else `None` |

### Job state

`a.state` is stored in the payload below `.httk-job/`, so it survives retries
and step advances, and travels with a transferred job. The
directory is excluded from every payload digest, so writing state never
disturbs an immutability check. Keys are strings, values are JSON, and every
mutation is one atomic replace:

```python
a.state["converged"] = True
a.state.merge({"attempts": 3, "energy": -12.5})
if a.state.get("converged"):
    ...
```

### Declarations

`a.declare(name, document)` records what this job observed about its own
workflow declaration. `a.declaration(name)` reads one back: the observed
document when the job wrote one, otherwise the document `job.json` declared.
The document is a JSON object carried verbatim; *httk-workflow* never looks
inside it. It is written atomically to `.httk-job/declarations/<name>.json`,
digest-excluded like the rest of `.httk-job/`. The last write of a name wins,
because a dynamic campaign refines its declaration as it learns:

```python
a.declare("workflow", {**a.declaration("workflow"), "outputs": {"structures": 3}})
```

See {doc}`/details/declarations` for the declared/observed contract and what a
collect reports.

### Children

`a.children` is empty unless this activation followed a `gather`. Each child
is a frozen `ChildResult` with its `label`, `job_id`, `job_key`, terminal
`kind`, its published `failure` as the canonical `Failure`, and absolute
`payload`, `workdir`, and `data` paths, so reading a child's results is a
plain file read. The manager locates each child when it launches the attempt;
under confinement the children are visible read-only:

```python
for child in a.children.succeeded:
    energy = json.loads((child.workdir / "energy.json").read_text(encoding="utf-8"))

reference = a.children["site-0"]           # by label
codes = [child.failure.code for child in a.children.failed]
```

An activation reached by `advance` observes no children, which is why
`aggregate` above hands what it learned to `triage` through `a.state`.

### Parent

`a.parent` is the other direction: a frozen `ParentJob` for the job that
spawned this one, with its `job_id`, `job_key`, `placement`, and absolute
`payload` and `workdir` paths, located from the `parent` member of this job's
`job.json` with a read-only lookup at its recorded placement; `workdir` is
the parent's `run/`. Under confinement the parent is visible read-only.
`a.parent` itself is `None` for a root job, for a detached child, and for a
child whose parent is not found in this workspace:

```python
parent = a.parent
if parent is not None:
    chgcar = parent.workdir / "CHGCAR"
```

It raises `FormatError` when this job's `parent` member or the parent's
`job.json` is malformed, which is corruption rather than absence. When to read
a parent's files in place instead of copying them into the child, and the
rules that keep it safe, are in the "Sharing files with children" section of
{doc}`/details/composing_workflows`.

## What an attempt publishes

Each attempt publishes exactly one outcome. `spawn`, `call`, and `put`
accumulate in one implicit draft below the attempt control directory. The
draft has no effect until a terminal call publishes it with a single atomic
rename. A second terminal call raises, and the draft of a handler that raises
is discarded, so no half-outcome reaches the manager.

| Call | Meaning |
| --- | --- |
| `a.advance(step, state=..., priority=...)` | run `step` next; `state` is written before publication |
| `a.gather(step, when=..., count=..., on_impossible=..., rejoin=...)` | wait for children spawned on this attempt and earlier-activation labels named by `rejoin`, then run `step` |
| `a.succeed()` | the job is done |
| `a.retry(reason)` | repeat this activation within the job's attempt budget |
| `a.pause(reason)` | stop until an operator resumes the job |
| `a.fail(code, message, details=..., retryable=...)` | the canonical failure record |

`fail` publishes the canonical failure object
`{"code", "message", "details", "retryable"}`, the same one the Bash bridge
and the manager publish. `code` is the string a job lists in `retry_on`.
`retryable` declares that repeating the attempt could help, which the manager
honours within the job's budgets. The manager records a malformed failure as
`protocol_error`.

### Spawning children

`a.spawn(child, label=..., placement=...)` registers one child job, created
when the outcome is published. The label is mandatory and unique within one
attempt; `gather` and `a.children` use it to name the child. It is also the
child's default job tag, so its payload directory is readable at a glance.
`placement` defaults to the placement of the spawning job.

A `ChildSpec` needs no prepared payload. It synthesizes a complete `job.json`
from the child's starting step and parameters, and everything else follows
the spawning job: its installed workflow, claim pool, priority, resources, and
sealing. A child never inherits `mintime`, and its `maxtime` is capped at the
spawning attempt's `maxtime`. To start a different workflow, use `a.call`.

A prepared payload directory is spawned as written, with its `job.json` from
`protocol.prepare_job_payload`; its workflow must be installed by the time the
child is claimed:

```python
from httk.workflow.protocol import JobSpec, prepare_job_payload

prepare_job_payload(
    a.workdir / "child",
    JobSpec(name="Child", workflow_id=a.job.workflow_id, workflow_name=a.job.workflow_name, initial_step="relax"),
)
a.spawn(a.workdir / "child", label="prepared", placement="project/children")
```

### Calling another workflow

`a.call(workflow, label=..., files=..., ...)` spawns a different workflow as a
child job, which runs that workflow's own runner rather than a step of this
one. `workflow` is an alias declared in `[workflow.calls]` of this job's
installed workflow, or the installed id an alias names; the called workflow
must be installed in the workspace too (`httk workflow install` installs the
declared calls with the caller). `call` builds a complete child payload from
the installed package, stages its `files` and `inputs`, runs its instantiate
hook, and registers it as a child:

```toml
[workflow.calls]
relax = "vasp.relax"
```

```python
a.call("relax", label="relax", files={"POSCAR": path})
a.gather("after_relax", on_impossible="triage")
```

```python
@run.step
def after_relax(a):
    relaxed = a.children["relax"].data      # the child's committed data
    ...
```

An undeclared or uninstalled workflow is refused at the call. Calling needs
the workspace root readable from the step, as `a.children` does.
{doc}`/details/composing_workflows` covers the full model, a worked example, failure
semantics, and how results move between calls.

### Gathering children

`a.gather(step)` joins the children spawned on this attempt, plus children
from earlier activations named by label in `rejoin=(...)`, and runs `step`
when the condition holds. `when` is `all_succeeded` (the default),
`all_terminal`, `any_succeeded`, `any_terminal`, or `at_least` with `count`.
When the condition can no longer be met, the job advances to `on_impossible`
if one is named and otherwise fails with `dependency_failure`. A named child
the manager cannot resolve fails the parent with `dependency_failure` once its
grace expires, rather than waiting forever.

### Data and workdir changes

`a.put(source, destination)` stages a copy of a file or directory for
`data/<destination>` in the attempt's implicit transaction, which commits
with the outcome; the manager applies it exactly once when it commits the
outcome, whatever the action. A file replaces what is there, and a directory
is merged into what is there:

```python
a.put(a.workdir / "energy.json", "results/energy.json")
a.put(a.workdir / "bands", "results/bands")
a.succeed()
```

`a.transaction()` is a commit point in the middle of a step: stage with
`put(source, destination)`, where destinations are relative to the job
directory (`data/result` lands in `data/`), then `commit()`. The manager
applies every committed transaction at the next attempt boundary, before any
later attempt starts, whatever the outcome; an uncommitted one is discarded.
Destinations below `job.json`, `state.json`, `seal.json`, `logs` and
`attempts` are refused, and there is no removal operation:

```python
checkpoint = a.transaction()
checkpoint.put(a.workdir / "WAVECAR", "data/checkpoint/WAVECAR")
checkpoint.commit()
```

`a.workdir_batch()` groups changes to the workdir into a sealed batch that is
replayed idempotently. `Attempt.initialize`, and so every dispatch through
`Runner.main`, completes any batch an interrupted attempt sealed but did not
apply.

### Running processes

`a.run(argv, timeout=...)` runs an argv array in the workdir and terminates
its whole process group on timeout. `ProcessSupervisor` adds streamed
stdout/stderr, followed-file monitoring, process-group timeout handling, and
versioned executable checkers. A checker receives
`httk-workflow-checker-event` version 2 JSON lines and emits
`httk-workflow-checker-result` version 2 JSON lines. Commands and checker
commands are always argument arrays; the API does not reproduce the *httk* v1
exit-code and `ht.nextstep` interface.

Native Bash runners use the same protocol through {doc}`/sdks/bash_api`.

## Simulation-code helpers

*httk-workflow* bundles no helpers for a particular simulation code. The VASP
helpers (input preparation, supervised execution, structured diagnosis, and
the explicit remedy ladder) live in the separate *httk-workflow-vasp*
distribution (`pip install httk-workflow-vasp`) as `httk.codes.vasp`,
documented there. See {doc}`/code_support` for how such a distribution plugs
in, and {doc}`/vasp_runners` for the ready-made workflows built on it. The
native runtime and supervision modules above are independent *httk₂*
interfaces that use only the Python standard library and other
`httk.workflow` modules.

For unchanged *httk* v1 `ht_steps`, keep sourcing the historic filenames
under `$HTTK_DIR/Execution/tasks/`. They are thin redirects to the attributed
compatibility implementation described in {doc}`/details/v1_compatibility`.
