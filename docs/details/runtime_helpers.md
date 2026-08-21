# Native runner helpers in detail

*For authors writing a workflow runner in Python.* The complete authoring
surface, Python beside its Bash equivalent, is the table in {doc}`/sdks/sdk_parity`,
which is normative and enforced by the test suite; this page teaches it.

A native *httk₂* runner is one program that implements the steps of one workflow.
The manager launches it once per attempt, tells it which step to run, and reads
exactly one published outcome back. `Runner` registers the steps, and
`Runner.main` dispatches the requested step to its handler with one `Attempt`
object that carries everything the step may read, do, and publish.

Nothing declares the shape of a workflow. A step decides *at run time* which
children to spawn and which step runs next, so the graph of a job is whatever its
steps published — one runner can drive a two-step relaxation and a large,
partitioned child campaign without a graph definition anywhere.

## A complete runner

This is a full defect campaign: it characterizes a structure, spawns one child
job per candidate site, gathers them, and either reports or triages.

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
different step. Publish it once in the workspace and every job — parent and
child — references those bytes by digest:

```console
httk workflow runner publish --workspace workflow-workspace --name defects/run.py defects.py
```

```python
from httk.workflow import JobSpec, Workspace, prepare_job_payload

workspace = Workspace("workflow-workspace")
reference = workspace.publish_runner("defects.py", name="defects/run.py")
job = prepare_job_payload(
    "prepared-job",
    JobSpec(
        name="Si vacancies",
        workflow="defects",
        runner_source="workspace",
        runner_path=str(reference["path"]),
        runner_sha256=str(reference["sha256"]),
        initial_step="characterize",
        parameters={"encut": 520, "supercell": [2, 2, 2]},
    ),
)
workspace.submit("prepared-job", "project/si-vacancies")
```

## Steps and dispatch

`Runner(workflow, inputs=...)` declares creation-time staged inputs as
`name` → destination (or `null` for a hook-consumed input). The optional
`inputs` member is included in the runner description when non-empty.

`@run.step` registers the handler under the function's own name;
`@run.step(name="collect-results")` names it explicitly. Registering the same
name twice is an error at decoration time, so the step set is complete and
unambiguous before any work starts. `run.steps` is that set.

Because registration precedes work, every step name a handler *publishes* is
checked against it at the call that names it: `a.advance("colect")` raises
immediately, listing the registered steps, instead of failing the job one
activation later. The same check covers `gather`, its `on_impossible` step, and
the `step` of a `ChildSpec` that inherits this runner.

`Runner.main` turns every ending into an outcome:

| Ending | Published outcome |
| --- | --- |
| the handler publishes one | that outcome |
| the handler returns without publishing | `fail("no_outcome", ...)` |
| the step is not registered | `fail("unknown_step", "... registered steps: ...")` |
| the handler raises | an `error.json` breadcrumb, then the exception reaches the manager, whose retry policy decides |

`HTTK_WORKFLOW_DESCRIBE=1` (or `--describe`) makes `main` print
`{"format": "httk-workflow-runner-description", "format_version": 2, "workflow":
..., "steps": [...]}` and exit without binding an attempt or touching anything,
so a tool can enumerate the steps of a runner it is not running. The step set is
also recorded in the job's state frame as `runner_steps`, from the first outcome
the job publishes.

### Creation-time instantiation

There are two equivalent creation-time forms. For a Python runner,
`@run.instantiate` is the in-process hook replacing v1's `ht.instantiate.py`.
`new_job(s)` resolves it on the creating machine, after declared inputs are
staged and before `job.json` is finalized. Its `InstantiateContext` provides the
staging `payload`, read-only `inputs`, mutable merged `parameters`, and caller
`tag`; `suggest_tag` supplies a tag only when the caller did not. For example:

```python
@run.instantiate
def instantiate(ctx):
    (ctx.payload / "files" / "generated.txt").write_text(ctx.inputs["structure"])
    ctx.parameters["derived"] = "ready"
    ctx.suggest_tag("generated")
```

The hook may write anywhere below `payload`. It is trusted workflow code: the
file is code being published and executed anyway.

A directory package may instead name any executable member in
`[workflow.instantiate].file`. The framework starts it in the staging payload,
pre-serializes hook-consumed inputs, and sends the JSON
`httk-workflow-instantiate` envelope on stdin. The executable returns
`{"parameters": {...}}` and may return a string `tag`; nonzero exit or malformed
output aborts submission. This form is language-neutral and has the same
parameter, tag, payload, and input semantics as the Python hook. The complete
envelope and serialization rules are normative in {doc}`workflow_packages`.

## What an attempt reads

| Member | What it is |
| --- | --- |
| `a.context` | the immutable identity and restart evidence of this attempt |
| `a.step` | the step this attempt runs |
| `a.payload`, `a.workdir`, `a.workspace`, `a.data` | absolute paths; `a.data` is set only for a transactional job |
| `a.job`, `a.parameters`, `a.parameter(name[, default])` | the job definition and its opaque implementation `parameters` object |
| `a.state` | dict-like JSON state that belongs to the **job** |
| `a.log` | the append-only structured run log of the workdir |
| `a.children` | the typed children of the join that started this activation |
| `a.declaration(name)` | one workflow declaration: the observed document, else the declared one, else `None` |

`a.state` is stored inside the payload, below `.httk-job/`, so it survives
retries, step advances, and isolated workdirs, and it travels with a transferred
job. The directory is excluded from every payload digest, so writing state never
disturbs an immutability check. Keys are strings, values are JSON, and every
mutation is one atomic replace:

```python
a.state["converged"] = True
a.state.merge({"attempts": 3, "energy": -12.5})
if a.state.get("converged"):
    ...
```

`a.declare(name, document)` records what this job *observed* about its own
workflow declaration, and `a.declaration(name)` reads one back — the observed
document when the job wrote one, the document `job.json` declared otherwise. The
document is a JSON object carried verbatim; nothing in *httk-workflow* looks
inside it. It is written to `.httk-job/declarations/<name>.json`, atomically and
digest-excluded like the rest of `.httk-job/`, and the last write of a name wins,
because a dynamic campaign refines what it declares as it learns it:

```python
a.declare("workflow", {**a.declaration("workflow"), "outputs": {"structures": 3}})
```

See {doc}`/declarations` for the declared/observed contract and what a collect
reports.

`a.children` is empty unless this activation followed a `gather`. Every child is
a frozen `ChildResult` with its `label`, `job_id`, `job_key`, terminal `kind`,
its published `failure` as the canonical `Failure`, and absolute `payload`,
`workdir`, and `data` paths, so reading a child's results is a plain file read:

```python
for child in a.children.succeeded:
    energy = json.loads((child.workdir / "energy.json").read_text(encoding="utf-8"))

reference = a.children["site-0"]           # by label
codes = [child.failure.code for child in a.children.failed]
```

A new activation reached by `advance` observes no children, which is why
`aggregate` above hands what it learned to `triage` through `a.state`.

## What an attempt publishes

Exactly one outcome per attempt. `spawn`, `put`, and `remove` accumulate in one
implicit draft below the attempt control directory, and the draft has no effect
until a terminal call publishes it with a single atomic rename. A second
terminal call raises, and a draft left by a handler that raises is discarded, so
no half-outcome ever reaches the manager.

| Call | Meaning |
| --- | --- |
| `a.advance(step, state=..., priority=...)` | run `step` next; `state` is written before publication |
| `a.gather(step, when=..., count=..., on_impossible=..., rejoin=...)` | wait for children spawned on this attempt and earlier-activation labels named by `rejoin`, then run `step` |
| `a.succeed()` | the job is done |
| `a.retry(reason)` | repeat this activation within the job's attempt budget |
| `a.pause(reason)` | stop until an operator resumes the job |
| `a.fail(code, message, details=..., retryable=...)` | the canonical failure record |

`fail` publishes the one canonical failure object,
`{"code", "message", "details", "retryable"}`, which is also what the Bash
bridge and the manager itself publish. `code` is the string a job lists in
`retry_on`; `retryable` declares that repeating the attempt could help, which the
manager honours within the job's budgets. A malformed failure is recorded by the
manager as `protocol_error`.

### Spawning children

`a.spawn(child, label=..., placement=...)` registers one child job, to be
created when the outcome is published. The label is mandatory and unique within
one attempt: it is how `gather` and `a.children` name that child later. It also
becomes the child's job tag by default, so its payload directory is readable at a
glance.

A `ChildSpec` needs no prepared payload at all. It synthesizes a complete
`job.json` from the step and parameters the child starts with, and everything else
follows the spawning job: workflow, claim pool, priority, resources, and runner.
`RunnerRef.inherit()` — the default — copies the parent's own `(source, path,
sha256)`, which is what a campaign whose steps all live in one published runner
wants; `RunnerRef.workspace(path, sha256)` and `RunnerRef.installed(path,
sha256)` name another shared runner. A payload runner cannot be inherited,
because a synthesized child has no payload to copy it into: publish it with
`Workspace.publish_runner`, or spawn a prepared payload directory
instead:

```python
prepare_job_payload(
    a.workdir / "child",
    JobSpec(name="Child", workflow="defects", runner_path="files/runner"),
)
a.spawn(a.workdir / "child", label="prepared", placement="project/children")
```

`placement` defaults to the placement of the spawning job.

### Gathering them

`a.gather(step)` joins the children spawned on this attempt and can add children
from earlier activations by label with `rejoin=(...)`. It runs `step` when the
condition holds. `when` is `all_succeeded` (the default), `all_terminal`,
`any_succeeded`, `any_terminal`, or `at_least` with `count`. When the condition
can no longer be met the job advances to `on_impossible` if one is named, and
fails with `dependency_failure` otherwise. A named child the manager cannot
resolve fails the parent with `dependency_failure` once its grace expires,
rather than waiting forever.

### Data and workdir changes

For a job with `data_mode="transactional"`, `a.put(source, destination)` stages a
file or a directory and `a.remove(destination, missing_ok=...)` stages a removal.
The manager applies them exactly once when it commits the outcome. A `put`
overwrites its destination whether the source is a file or a directory: when the
destination already exists in the committed data, a directory put replaces that
tree, so a step that advances back onto a step and re-puts the same tree
succeeds rather than failing. Operation identifiers are generated in call order
(`op-0001`, `op-0002`, …), so replaying the same step produces the same manifest:

```python
a.put(a.workdir / "energy.json", "results/energy.json")
a.put(a.workdir / "bands", "results/bands")
a.remove("scratch", missing_ok=True)
a.succeed()
```

`a.workdir_batch()` groups changes to the *workdir* into a sealed batch that is
replayed idempotently: `Attempt.initialize`, and therefore every dispatch through
`Runner.main`, completes any batch an interrupted attempt sealed but did not
apply.

`a.run(argv, timeout=...)` runs an argv array in the workdir and terminates its
whole process group on timeout. `ProcessSupervisor` adds streamed stdout/stderr
and followed-file monitoring, process-group timeout handling, and versioned
executable checkers; a checker receives `httk-workflow-checker-event` version 2
JSON lines and emits `httk-workflow-checker-result` version 2 JSON lines.
Commands and checker commands are always argument arrays: this API deliberately
does not reproduce the *httk* v1 exit-code and `ht.nextstep` interface.

Native Bash runners use the same protocol through {doc}`/sdks/native_bash_api`.

## VASP files

The dependency-free VASP helpers cover preparation, supervised execution,
structured diagnosis, and explicit remedies:

```python
from httk.workflow import (
    assemble_potcar,
    automatic_kpoint_grid,
    update_incar,
    write_automatic_kpoints,
)

grid = automatic_kpoint_grid(40, poscar="POSCAR")
write_automatic_kpoints(grid, "KPOINTS")
update_incar({"ENCUT": 520, "ISPIN": 2}, "INCAR")
assembled = assemble_potcar("/data/vasp/potpaw_PBE", poscar="POSCAR", output="POTCAR")
print([(item.species, item.variant, item.source) for item in assembled.choices])
```

The API also provides `read_poscar_header`, `suggested_magnetic_moments`,
`read_incar`, `calculate_nbands`, `prepare_vasp_inputs`, `run_vasp`,
`plan_vasp_remedy`, `apply_vasp_remedy`, output extraction/cleaning, and
`contcar_to_poscar`. `validate_vasp_workdir` checks VASP's conservative path
limit and `clean_vasp_outputs` performs explicit pre-run cleanup while
preserving requested files. Diagnosis never changes inputs. Whole workflows built
from these helpers ship with the module: see {doc}`/vasp_runners`.

A few behaviors are worth stating explicitly, because they are what makes a run
reproducible and a retry meaningful:

- **One k-point default.** `write_automatic_kpoints`, `VaspPreparationOptions`, and
  the Bash bridge all start at `DEFAULT_KPOINT_CENTERING`, which is
  `Monkhorst-Pack`. The reviewed remedy ladder promotes `Gamma` as the fix for two
  k-point failure classes, so a workflow that already started there would have no
  such remedy left.
- **Explicit tags win.** `VaspPreparationOptions.incar_tags` is written to the INCAR
  before anything is derived, and no derived value overwrites a tag the caller set.
- **Updates rewrite statements, not lines.** `update_incar` understands the
  `;`-separated assignments `read_incar` reads, so updating one tag of
  `ISPIN = 2 ; ISYM = 2` leaves no second, stale assignment behind.
- **A rattle needs entropy.** `rattle_poscar` requires an explicit `seed` or an
  `entropy` string that `derive_seed` turns into one — normally something
  attempt-derived — because two retries that rattle identically are two identical
  calculations.
- **Cleanup keeps its own evidence.** `clean_vasp_outputs` preserves
  `VASP_RESTART_ARTIFACTS`, that is `CONTCAR` and `vasp-run-report.json`, which are
  what the remedy machinery and restart promotion read. Name them in `also_remove`
  to delete them anyway.
- **Provenance is recorded.** `assemble_potcar` returns a `PotcarAssembly` naming the
  variant, source path, digest, and `TITEL` of every chosen potential, and writes the
  same record next to the POTCAR as `POTCAR.provenance.json`.

Remedies are bounded, planned, and applied explicitly. `plan_vasp_remedy` validates
every rung of the ladder against the calculation directory it is given and skips one
it cannot execute — a `bump_bands` for an INCAR without `NBANDS`, a KPOINTS change
with no KPOINTS staged — so `apply_vasp_remedy` never receives a remedy it would
have to refuse. Both resolve a relative history path against that same directory;
`job_remedy_history_path(payload)` is where a runner should keep it, beside the job
state, so an isolated workdir does not silently restart the escalation.

Policies are a registry rather than module source: `register_remedy_policy(name,
sequences, precedence)` adds one, `remedy_policy_names()` lists what is registered,
and an unknown name is refused with the alternatives named. `reviewed-v1` is
registered by the module itself and is the default.

These functions are independent *httk₂* interfaces. They do not import the Python *httk* v1
`httk.task.ht_tasks_api` or `httk.task.vasptools` implementations.
The native runtime, supervision, utility, and VASP modules were audited to use
only the Python standard library and other `httk.workflow` modules. No numeric
or atomistic capability package is required. Legacy Unix-compress
`POTCAR.Z` is the one external-format exception: it is read through a checked
argv invocation of `gzip` or `uncompress` when either executable is available.

For unchanged *httk* v1 `ht_steps`, continue sourcing the exact historic filenames
under `$HTTK_DIR/Execution/tasks/`. Those are thin redirects to the attributed
compatibility implementation described in [*httk* v1 task
compatibility](v1_compatibility.md).
