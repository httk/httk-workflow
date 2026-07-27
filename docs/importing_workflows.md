# Importing workflows written elsewhere

*For teams whose workflows are already written as PWD or CWL documents.*

A workflow that already exists in another language does not have to be rewritten
to run on *httk₂*. Two exchange formats are imported into ordinary jobs:

| Format | Command | Needs |
| --- | --- | --- |
| [Python Workflow Definition](https://github.com/pythonworkflow/python-workflow-definition) (PWD) | `httk workflow import pwd` | nothing beyond *httk-workflow* |
| [Common Workflow Language](https://www.commonwl.org/) (CWL) | `httk workflow import cwl` | `pip install httk-workflow[cwl]` |

Both importers share three properties.

**Importing is one way.** The imported job carries the document it came from, and
nothing translates a job back into the language it was written in. A document
that changes afterwards is picked up by importing it again, which submits a new
job; the old one keeps running exactly the bytes it was submitted with.

**No runner file is written per workflow.** Both formats are executed by a
*packaged* runner — `pkg:httk.workflow.compat.pwd/pwd_runner.py` and
`pkg:httk.workflow.compat.cwl/cwl_runner.py` — referenced through the reserved
installed form the manager resolves inside its own module allowlist. The document
is data in the payload; the program that executes it is the same installed bytes
for every imported job, so a partitioned campaign of imported workflows copies no
runner even once.

**The workflow runs on httk's own machinery.** No other engine is invoked, no
other engine is bundled, and nothing shells out to `cwltool`. An imported job is
claimed, leased, retried, checkpointed and journalled like every other job of a
workspace, and shows up in `job list`, `job show`, `job why` and `harvest`.

## Python Workflow Definition

A PWD document is a JSON graph: `nodes` are Python functions named
`module.function`, literal inputs, and named outputs; `edges` connect one node's
output port to another node's input port. The functions themselves live in a
Python module beside the document.

```console
httk workflow import pwd WORKSPACE workflow.json --module workflow.py --tag arithmetic
httk workflow manager run WORKSPACE --until-idle
```

`--module FILE` stages a Python file into the payload, and the runner puts the
payload's `files/` directory first on `sys.path`, so document and module travel
together as one job. `--module-path DIRECTORY` adds a further import root that
must exist on the machine that runs the job. `--input NAME=VALUE` overrides an
input node by name, so one document runs with values it does not itself carry.

From Python:

```python
from httk.workflow import Workspace
from httk.workflow.compat.pwd import import_pwd

workspace = Workspace("workflow-workspace")
job = import_pwd(workspace, "workflow.json", modules=["workflow.py"], workflow_inputs={"x": 3})
print(job.job_key, job.payload)
```

### What a PWD job does

The whole graph runs inside **one** job, sequentially, in topological order. A
PWD node is one Python call: it is not worth a claim, a lease and a process of
its own, and a multi-node document would otherwise become one job per node with
no parallelism to show for it.

The packaged runner registers exactly one step, `execute`, and keeps the position
in the graph in the job state instead of in a step name — a runner's step set is
registered when its module is imported and pinned per job, so it cannot depend on
which document a job happens to carry. `httk workflow runner describe` and
`--describe` therefore report one honest entry point rather than a step set that
changes per payload.

Restarts are handled by a checkpoint rather than by a step name. Every node's
result is written to the job state as it is produced, in one atomic replace per
node, and a repeated attempt skips every node already in the checkpoint. A result
that is not JSON cannot be checkpointed, so its node — and everything downstream
of it, which comes later in the topological order — is recomputed. Nothing is
ever silently reused.

A node that raises fails the job with `pwd.node_failed`, and that failure is
retryable by default, so the manager repeats the attempt within the job's attempt
budget (`--attempts`, three by default) and the repeat resumes from the
checkpoint.

Reading the document, in `job show`:

| Job state key | What it holds |
| --- | --- |
| `pwd_results` | the checkpoint: one JSON value per completed node, by node id |
| `pwd_completed` | the node ids completed so far, in completion order |

The named outputs are written to `pwd-outputs.json` in the job's workdir, and
published under `data/pwd/` when the job was imported with
`--data-mode transactional`.

### Documents too large to embed

A job's `inputs` object is bounded, so a document larger than half that budget is
staged in the payload as `files/pwd.json` and the job carries a
`pwd_document_path` pointer to it instead of the document itself. Both spellings
run identically; `import_pwd(..., maximum_embedded_bytes=N)` moves the boundary.

### Security: a PWD document is code

Running a PWD document **imports and calls the Python functions it names**, with
no sandbox — the format's entire content is `module.function` references. There
is no safe way to run an untrusted one, and *httk₂* does not pretend otherwise:
importing a document is exactly as dangerous as running the module it names.

The default is therefore to allow what the interpreter can import, because the
person importing the document is the person who would otherwise have run it by
hand. When a document comes from somewhere less trusted, `--allow-module PREFIX`
(or `allowed_modules=[…]`) records a prefix allowlist in the job, and the runner
refuses to import anything outside it, failing with `pwd.node_failed` and naming
the module it would have imported.

## Common Workflow Language

CWL is supported as a workflow **language**. A document is parsed and validated
with [cwl-utils](https://github.com/common-workflow-language/cwl-utils),
normalized into one self-contained JSON plan with every `run:` reference inlined,
and executed by the packaged `cwl_runner.py`.

```console
pip install httk-workflow[cwl]
httk workflow import cwl WORKSPACE flow.cwl job.yml --tag echo
httk workflow manager run WORKSPACE --until-idle
```

The extra is needed on the machine that *imports*. The machine that *runs* the
imported job needs neither *cwl-utils* nor anything else: the plan in the payload
is plain JSON, and the runner is one file of an installed *httk-workflow*.

### How a CWL workflow becomes httk jobs

| CWL | On httk |
| --- | --- |
| `Workflow` step running a `CommandLineTool` | one activation of the parent job, executed in `run/cwl/<step>/` |
| scattered step | one labeled child job per shard — `s0000`, `s0001`, … — joined with `gather` |
| subworkflow | one child job labeled `sub`, carrying its position in the same plan |
| workflow outputs | `cwl-outputs.json` in the workdir, and `data/cwl/` when transactional |

The runner's registered steps — `start`, `enter`, `advance`, `collect` — are a
fixed dispatch vocabulary, not the workflow's own step names: which CWL step runs
next is state, so one runner describes itself the same way whatever document it
is given. One CWL step per activation means every step boundary is a journalled
state frame a restart resumes from, and a tool interrupted mid-run is re-run from
a cleaned execution directory rather than from a half-written result.

File inputs are staged into a numbered slot beside the execution directory, never
into it, so a tool's output globs can never match the files it was given, and two
inputs with one basename — three scatter shards each producing `spoken.txt` is
the normal case — never become one file.

### The supported subset

| Feature | Supported |
| --- | --- |
| `class` | `Workflow`, `CommandLineTool` |
| `cwlVersion` | v1.0, v1.1, v1.2 (older versions are upgraded when *cwl-upgrader* is installed) |
| types | `File`, `Directory`, `string`, `int`, `long`, `float`, `double`, `boolean`, `Any`, arrays of those, and optional (`?`) forms |
| command | `baseCommand`, `arguments` |
| input bindings | `position`, `prefix`, `separate`, `itemSeparator`, plain-reference `valueFrom` |
| outputs | `outputBinding.glob` (with `loadContents`), the `stdout` and `stderr` type shortcuts |
| redirection | `stdout`, `stderr` |
| step inputs | `source`, `default`, `linkMerge` (`merge_nested`, `merge_flattened`), plain-reference `valueFrom` |
| scatter | one input, or several of equal length with `scatterMethod: dotproduct` |
| subworkflows | any depth |
| conditionals | `when` written as one plain parameter reference |
| requirements | `EnvVarRequirement` (honoured), `ToolTimeLimit` (honoured), `ResourceRequirement` and the feature requirements (recorded) |
| expressions | `$(inputs.x)`, `$(inputs.x.path)`, `$(runtime.outdir)` and other plain parameter references, alone or interpolated into a string |

Everything else is refused at import time, before anything is submitted, with a
message naming the feature and where in the document it was found:

| Refused | Why |
| --- | --- |
| `InlineJavascriptRequirement`, `${…}` bodies, anything computed inside `$(…)` | *httk₂* runs no JavaScript engine |
| `ExpressionTool`, `Operation` | there is nothing to execute but an expression |
| `ShellCommandRequirement`, `stdin` redirection | *httk₂* runs an argument vector, never a shell command line |
| `InitialWorkDirRequirement` | *httk₂* stages the files an input names and builds no working directory from a listing |
| `SchemaDefRequirement`, record and enum types | outside the supported type set |
| `nested_crossproduct`, `flat_crossproduct` | only `dotproduct` scatter is implemented |
| `streamable`, `secondaryFiles` | whole files only, and exactly the files an input names |
| `outputEval` | the files a glob matched are collected unchanged |
| `InplaceUpdateRequirement` | a tool never writes to its input files |
| CWL v1.2 loops | no loop support |
| complex `when` | one plain parameter reference, nothing that has to be computed |

`DockerRequirement` is neither refused nor honoured. It is recorded as the
required capability `docker` on the job — so only a manager started with
`--capability docker` will claim it — and reported as a warning, because *httk₂*
runs the command directly and never pulls, builds or enters an image. An
unsupported *hint* (as opposed to a requirement) is dropped with a warning, which
is what a hint means.

### Failures

| Code | What happened |
| --- | --- |
| `cwl.tool_failed` | the tool exited outside its `successCodes`; the message carries the last lines of its standard error |
| `cwl.input_missing` | a required input was not connected, or its file is gone |
| `cwl.output_missing` | a non-optional output's glob matched nothing |
| `cwl.scatter_invalid` | a scattered input is not an array, or a dotproduct's arrays differ in length |
| `cwl.unsatisfiable` | no step of the workflow can run and some are unfinished |
| `cwl.child_invalid` | a child job published no readable outputs |

None of them are declared retryable: a tool that exited nonzero says the same
thing on the next attempt. What repeated attempts are for — a lost node, a killed
process — is handled by the manager without any failure being published at all.

### From Python

```python
from httk.workflow import Workspace
from httk.workflow.compat.cwl import import_cwl

workspace = Workspace("workflow-workspace")
imported = import_cwl(workspace, "flow.cwl", "job.yml", tag="echo", data_mode="transactional")
for warning in imported.warnings:
    print(warning)
print(imported.job.job_key, imported.document["class"])
```

{py:func}`~httk.workflow.compat.cwl.load_cwl_plan` does the parsing and the
subset check alone, without submitting anything, which is the cheap way to find
out whether a document can be imported at all.
