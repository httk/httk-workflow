# Workflow-language runner realizations

*For workflows written as CWL, PWD, jobflow Maker documents, or httk v1 task
templates.*

The language integrations are runner realizations, not import commands. A
document or template is resolved into an ordinary job by `job new`; the job is
claimed, retried, checkpointed, journalled, and collected by the normal
workflow machinery.

| Language | Bare document or package form | Installed runner |
| --- | --- | --- |
| CWL | `job new WS --workflow flow.cwl` | `pkg:httk.workflow.languages.cwl/cwl_runner.py` |
| PWD | `job new WS --workflow graph.json` | `pkg:httk.workflow.languages.pwd/pwd_runner.py` |
| jobflow | `job new WS --workflow maker.json`, or a package with `language = "jobflow"` | `pkg:httk.workflow.languages.jobflow/jobflow_runner.py` |
| httk-v1 | a template directory, or a package with `language = "httk-v1"` | payload runner under executor `httk-v1` |

The CWL realization needs `httk-workflow[cwl]` when the document is prepared.
The normalized plan is carried by the job, so the machine executing the job
does not need the parser extra.

The jobflow realization needs no extra when a job is prepared or collected.
The machine that runs the job needs `httk-workflow[jobflow]`; use
`httk-workflow[atomate2]` as well when the Maker comes from atomate2. The
Maker module named by the manifest or document must be importable in that
runner environment. This is the reverse of CWL's parser placement.

## Language packages

A package selects a language in `[workflow.runner]`. The language supplies its
steps, instantiate behavior, runner, workdir contract, and default
collector. A package may override the default with `[workflow.collect]`.
Language inputs are consumed by that realization, so `destination` is forbidden
and `[workflow.instantiate]` is implied and forbidden. The optional `port` key
maps a package input or output name to a document or language port; omitted
ports use the package name. Statically known ports are checked against the
document and duplicates are errors.

Language registrations expose a `collector` field and a
`has_default_collector` flag. For languages with defaults, package resolution
uses `httk.workflow.languages.<mod>:collect`; CWL, PWD, and jobflow set the
flag true, while httk-v1 sets it false.

### CWL package

This is the same shape used by the package fixtures:

```toml
[workflow]
id = "example.cwl"

[workflow.runner]
language = "cwl"
document = "echo.cwl"

[workflow.inputs.message]
entry_type = "strings"

[workflow.outputs.spoken]
entry_type = "strings"
```

The CWL document is parsed and normalized during preparation. Its input values
are staged according to the CWL schema; output ports become declared output
roles. A single CWL `File` output is served and stored as a standard `files`
entry, with a workspace-relative POSIX `url`, file `name`, size, and flat
`sha256`; media types are left unset unless a collector knows them explicitly.
Lists of files deliberately remain descriptor values inside a
`DataRecord`, preserving list shape for callers; the asymmetry keeps one file
addressable as an entry while a list remains one role value.

### PWD package

PWD modules are package members when `modules` is used. `module_path` names
additional import roots, and `allowed_modules` is a module-prefix allowlist:

```toml
[workflow]
id = "example.pwd"

[workflow.runner]
language = "pwd"
document = "workflow.json"
modules = ["module.py"]
module_path = ["."]
allowed_modules = ["module"]

[workflow.inputs.message]
entry_type = "strings"

[workflow.outputs.result]
entry_type = "strings"
```

The whole graph executes as one job in topological order. The runner writes
`pwd-outputs.json`; each completed node is checkpointed in `pwd_results` and
`pwd_completed`. `modules` are staged into `files/`; a document too large for
the parameter budget is staged as `files/pwd.json`.

A PWD document is code: it imports and calls the `module.function` names it
contains. There is no sandbox. `allowed_modules` records and enforces a prefix
allowlist when a document is less trusted. An untrusted PWD document must not
be run merely because it passed format validation.

### jobflow package

The jobflow realization runs a jobflow `Maker` and is intended especially for
atomate2, whose workflows are jobflow Makers. It accepts exactly one of a
Maker import specification or a Monty-serialized Maker document. The import
form constructs the Maker with `Class(**parameters)`:

```toml
[workflow]
id = "example.jobflow"

[workflow.runner]
language = "jobflow"
maker = "atomate2.vasp.flows.core:DoubleRelaxMaker"

[workflow.parameters.name]
type = "string"
default = "double relax"
description = "The atomate2 Maker name."

[workflow.inputs.structure]
entry_type = "structures"
port = "structure"
description = "The starting structure."
role = "initial_structure"

[workflow.outputs.result]
entry_type = "structures"
port = "output"
role = "relaxed_structure"
description = "The resolved final jobflow output."
```

The `maker` value is a `module:Class` specification. The document form names
a relative JSON member instead:

```toml
[workflow.runner]
language = "jobflow"
document = "maker.json"
```

`maker.json` is a Monty-serialized Maker document with string `@module` and
`@class` members; `document_from_maker` serializes an MSONable Maker into this
form. Declared `[workflow.parameters.*]` are Maker constructor configuration,
not arbitrary httk runner settings. A per-job `job new --parameter` value
overrides its manifest default. In the document form, the runner applies
declared values with `dataclasses.replace`, so a Maker receiving document
parameters must support that operation.

Declared `[workflow.inputs.NAME]` values are passed to `make()` using their
`port` labels (or their manifest names when `port` is omitted). Literal JSON
values are decoded with Monty. A path input is staged under `files/inputs/`;
`.json` files are Monty-decoded and other path inputs are loaded with
`pymatgen.Structure.from_file`. The Maker's `@module`, and the modules needed
by its serialized job functions, must be importable wherever the jobs run.

The parent httk job is an event-driven scheduler. It creates one httk child
job for each jobflow job as soon as its dependencies settle; it wakes when any
live child reaches a terminal state, so independent branches can run in
parallel across manager workers. The scheduler state and a file-backed
jobflow `JobStore` are checkpointed in the parent's persistent workdir, so
re-activation and crash recovery resume from those files. No MongoDB service is
used. Jobflow response semantics are preserved for `replace`, `detour`,
`addition`, `stop_children`, and `stop_jobflow`; children of a replacing or
detouring job wait for the whole replacement or detour sub-flow.

The runner writes `jobflow-outputs.json` with one primary `output` port, set to
the flow's resolved final output. If jobs return `stored_data`, that mapping is
passed through as an additional output. The default collector maps these
values to declared roles and returns `DataRecord` values. FileRecord outputs
are not implemented yet, and there is no separate jobflow output-port model
beyond `output` and the optional `stored_data` passthrough.
To collect `stored_data` as a declared package output, give an output table
`port = "stored_data"`.

Failures use jobflow-specific codes such as `jobflow.missing_dependency`,
`jobflow.document_invalid`, `jobflow.maker_config_failed`,
`jobflow.input_invalid`, `jobflow.make_failed`, `jobflow.job_failed`, and
`jobflow.flow_failed`.

For atomate2 VASP workflows, execution settings belong to atomate2: use its
`~/.atomate2.yaml` and `ATOMATE2_VASP_CMD` configuration. They are not httk
workflow settings. Atomate2 workflows are jobflow Makers; this realization is
not a FireWorks runner.

### httk-v1 package

The v1 form has no document member:

```toml
[workflow]
id = "example.v1"

[workflow.runner]
language = "httk-v1"
taskset = "vasp"
attempts = 10

[workflow.inputs.structure]
entry_type = "structures"

[workflow.collect]
file = "collect.py"
```

The source directory must contain executable `ht_steps` or `ht_run`, including
`.template` forms. `taskset` selects the claim pool and `attempts` sets the v1
retry budget. `data_mode` and `workdir_mode` are forbidden: the realization
forces no transactional data and persistent `ht.run.current`.

## Bare documents and one-shot jobs

`job new` recognizes a bare CWL document, a PWD `.json` graph, a jobflow Maker
`.json` document, or a bare v1 template directory:

```console
httk workflow job new WS --workflow flow.cwl --input message=echo
httk workflow job new WS --workflow workflow.json \
  --parameter pwd_module_path='["."]'
httk workflow job new WS --workflow maker.json
httk workflow job new WS --workflow ./v1-template \
  --parameter structure=structures/si.cif
```

The resolver synthesizes an anonymous workflow with id `<language>.<stem>`.
Document input ports become hook-consumed inputs; document outputs become
`records`-typed outputs and the resolver generates the declaration. A bare
jobflow Maker document exposes only its `output` result, so inputs must be
embedded in a Maker document or declared in a package. Bare PWD module roots
come from `pwd_module_path`. Bare v1 template globals come from `--parameter`
values.

`--input-from` may batch a document or package input. Campaign preparation is
shared: the workflow is resolved and prepared once, then instantiated once per
job. For httk-v1, the source package is snapshotted at preparation, so edits
made during a campaign cannot leak into later jobs. Symlinks in a v1 package
are rejected. Language-produced parameters are reserved; a caller collision is
an error. `publish=` is ignored for language workflows because their runners
are installed or selected by the v1 executor, not copied to the workspace
runner store.

## Collection

The collector chooses a registered provider's collector first. A language
job without a provider falls back through its job `workflow_language` parameter
to the language default:

| Language | Default output document | Default behavior |
| --- | --- | --- |
| CWL | `cwl-outputs.json` | map ports to declared roles; single `File` values become `files` entries and lists remain `DataRecord` values |
| PWD | `pwd-outputs.json` | map ports to declared roles and create `DataRecord` values |
| jobflow | `jobflow-outputs.json` | map `output` and optional `stored_data` to declared roles and create `DataRecord` values |
| httk-v1 | none | degrade unless the package declares `[workflow.collect]` |

The registered provider's custom hook is authoritative. Such a package records
`workflow_collect = "package"` in the job; a provider-less collection
degrades with a registration hint rather than silently running a language
default. The `allow_job_collector` pinned-tree fallback is attempted only
after the language fallback and only when its digest and manifest match the
job. A failed language or hook collector degrades that job and does not
stop collection of its siblings.

The default CWL/PWD/jobflow collectors read the output JSON from the workdir or
transactional data tree, map document ports to manifest roles, and return
`DataRecord` values. CWL single `File` outputs additionally require a readable
path inside the workspace, workdir, or data tree and become standard `files`
entries; file lists retain their descriptor values and record sha256 evidence.

## CWL supported subset

CWL is parsed by cwl-utils and normalized into a self-contained JSON plan.
`run:` references are inlined. The runner executes these features:

| Feature | Supported |
| --- | --- |
| `class` | `Workflow`, `CommandLineTool` |
| `cwlVersion` | v1.0, v1.1, v1.2; older versions are upgraded when cwl-upgrader is installed |
| types | `File`, `Directory`, `string`, `int`, `long`, `float`, `double`, `boolean`, `Any`, arrays, optional forms |
| command | `baseCommand`, `arguments` |
| input bindings | `position`, `prefix`, `separate`, `itemSeparator`, plain-reference `valueFrom` |
| outputs | `outputBinding.glob` with `loadContents`, `stdout`, `stderr` shortcuts |
| redirection | `stdout`, `stderr` |
| step inputs | `source`, `default`, `linkMerge` (`merge_nested`, `merge_flattened`), plain-reference `valueFrom` |
| scatter | one input, or equal-length inputs with `scatterMethod: dotproduct` |
| subworkflows | any depth |
| conditionals | `when` as one plain parameter reference |
| requirements | `EnvVarRequirement` and `ToolTimeLimit` honoured; resource and feature requirements recorded |
| expressions | `$(inputs.x)`, `$(inputs.x.path)`, `$(runtime.outdir)`, and interpolated plain references |

The following are refused before submission:

| Refused | Why |
| --- | --- |
| `InlineJavascriptRequirement`, `${…}`, computed `$(…)` | no JavaScript engine |
| `ExpressionTool`, `Operation` | no executable process |
| `ShellCommandRequirement`, `stdin` | the runner uses an argument vector, not a shell command line |
| `InitialWorkDirRequirement` | the runner stages named inputs, not a listing-built workdir |
| `SchemaDefRequirement`, record and enum types | outside the supported type set |
| `nested_crossproduct`, `flat_crossproduct` | only `dotproduct` scatter is implemented |
| `streamable`, `secondaryFiles` | whole named files only |
| `outputEval` | glob matches are collected unchanged |
| `InplaceUpdateRequirement` | tools do not write input files |
| CWL v1.2 loops and complex `when` | no loop or computed conditional support |

`DockerRequirement` is recorded as the `docker` capability and warned about;
the runner executes directly and does not pull, build, or enter an image.
Unsupported hints are dropped with a warning. Failures include
`cwl.tool_failed`, `cwl.input_missing`, `cwl.output_missing`,
`cwl.scatter_invalid`, `cwl.unsatisfiable`, and `cwl.child_invalid`.

## Python API and registry

The language registry is available through
`httk.workflow.languages.available_languages()`, `language(name)`,
`match_document(path)`, `runner_path(package, name)`, and
`runner_reference(package, name)`. The language-specific loaders remain
available at `httk.workflow.languages.cwl.load_cwl_plan` and
`httk.workflow.languages.pwd.load_pwd_document`. Jobflow exposes
`httk.workflow.languages.jobflow.document_from_maker` for creating a document
from an MSONable Maker.

```python
from httk.workflow import Workspace, new_job

workspace = Workspace("workflow-workspace")
job = new_job(workspace, "flow.cwl", inputs={"message": "echo"})
print(job.job_key)
```
