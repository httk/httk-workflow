# Compatibility with other workflow systems

`httk.workflow.compat` runs workflows written as CWL, PWD, jobflow Maker
documents or *httk* v1 task templates. These integrations are runner
realizations, not import commands: `job new` turns the document or template
into an ordinary job that the normal machinery claims, retries, checkpoints,
journals and collects. A *format* name selects the realization: `cwl`, `pwd`,
`jobflow` or `httk-v1`.

| Format | Bare document or package form | Installed runner |
| --- | --- | --- |
| CWL | `job new --workspace WS --from-runner flow.cwl` | `pkg:httk.workflow.compat.cwl/cwl_runner.py` |
| PWD | `job new --workspace WS --from-runner graph.json` | `pkg:httk.workflow.compat.pwd/pwd_runner.py` |
| jobflow | `job new --workspace WS --from-runner maker.json`, or a package with `format = "jobflow"` | `pkg:httk.workflow.compat.jobflow/jobflow_runner.py` |
| httk-v1 | a package with `format = "httk-v1"` | `pkg:httk.workflow.compat.v1/v1_runner.py` through the ordinary `path` runner |

## Optional extras

CWL needs `httk-workflow[cwl]` where the document is prepared. The job carries
the normalized plan, so the executing machine needs no parser.

jobflow is the reverse: preparation and collection need no extra, but the
executing machine needs `httk-workflow[jobflow]`, plus
`httk-workflow[atomate2]` for atomate2 Makers. The Maker module named by the
manifest or document must be importable there.

## Format packages

A package selects a format with the `format` key of `[workflow.runner]`. The
realization supplies the steps, instantiate behavior, runner, workdir contract
and default collector; `[workflow.collect]` may override the collector. The
realization consumes the inputs, so `destination` is forbidden, and
`[workflow.instantiate]` is implied and also forbidden.

The optional `port` key maps a package input or output name to a document or
realization port; an omitted port uses the package name. Statically known
ports are checked against the document, and duplicates are errors.

Each registration exposes a `collect` function and a `has_default_collector`
flag. When the flag is set, package resolution uses the `collect` function of
the realization's `httk.workflow.compat` subpackage. CWL, PWD and jobflow set
it; httk-v1 does not.

### CWL package

The package fixtures use this shape:

```toml
[workflow]
name = "example.cwl"

[workflow.runner]
format = "cwl"
document = "echo.cwl"

[workflow.inputs.message]
entry_type = "strings"

[workflow.outputs.spoken]
entry_type = "strings"
```

Preparation parses and normalizes the document. Inputs are staged according to
the CWL schema, and output ports become declared output roles.

A single CWL `File` output is served and stored as a standard `files` entry
with a workspace-relative POSIX `url`, file `name`, size and flat `sha256`.
Media types stay unset unless a collector knows them. A list of files stays a
descriptor value inside a `DataRecord`, preserving its shape: one file is
addressable as an entry, a list is one role value.

### PWD package

With `modules`, PWD modules are package members. `module_path` adds import
roots, and `allowed_modules` is a module-prefix allowlist:

```toml
[workflow]
name = "example.pwd"

[workflow.runner]
format = "pwd"
document = "workflow.json"
modules = ["module.py"]
module_path = ["."]
allowed_modules = ["module"]

[workflow.inputs.message]
entry_type = "strings"

[workflow.outputs.result]
entry_type = "strings"
```

The whole graph runs as one job in topological order. The runner writes
`pwd-outputs.json` and checkpoints each completed node in `pwd_results` and
`pwd_completed`. `modules` are staged into `files/`; a document too large for
the parameter budget is staged as `files/pwd.json`.

A PWD document is code. It imports and calls the `module.function` names it
contains, with no sandbox. `allowed_modules` enforces a prefix allowlist for
less trusted documents. Passing format validation does not make an untrusted
document safe to run.

### jobflow package

The jobflow realization runs a jobflow `Maker`, notably from atomate2, whose
workflows are Makers. It accepts exactly one of a Maker import specification
or a Monty-serialized Maker document.

#### Maker sources

The import form gives a `module:Class` value in `maker` and constructs the
Maker with `Class(**parameters)`:

```toml
[workflow]
name = "example.jobflow"

[workflow.runner]
format = "jobflow"
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

The document form names a relative JSON member:

```toml
[workflow.runner]
format = "jobflow"
document = "maker.json"
```

`maker.json` is a Monty-serialized Maker with string `@module` and `@class`
members; `document_from_maker` produces it from an MSONable Maker.

#### Parameters and inputs

`[workflow.parameters.*]` are Maker constructor configuration, not general
*httk* runner settings. A `job new --parameter` value overrides the manifest
default. The document form applies declared values with
`dataclasses.replace`, which the Maker must support.

`[workflow.inputs.NAME]` values are passed to `make()` under their `port`
labels, or their manifest names without one. Literal JSON values are
Monty-decoded. A path input is staged under `files/inputs/`; `.json` files are
Monty-decoded and other paths are loaded with `pymatgen.Structure.from_file`.
The Maker's `@module`, and the modules its serialized job functions need, must
be importable wherever the jobs run.

#### Scheduling

The parent *httk* job is an event-driven scheduler. It creates one child job
per jobflow job once that job's dependencies settle, and wakes whenever a live
child becomes terminal, so independent branches run in parallel across
manager workers.

The scheduler state and a file-backed jobflow `JobStore` are checkpointed in
the parent's persistent workdir; re-activation and crash recovery resume from
them. No MongoDB service is used. The `replace`, `detour`, `addition`,
`stop_children` and `stop_jobflow` responses keep their jobflow semantics.
Children of a replacing or detouring job wait for the whole replacement or
detour sub-flow.

#### Outputs and failures

The runner writes `jobflow-outputs.json` with one primary `output` port, the
flow's resolved final output. A `stored_data` mapping returned by jobs is
passed through as an extra output; collect it as a declared output with
`port = "stored_data"`. The default collector maps these values to declared
roles as `DataRecord` values. FileRecord outputs are not implemented yet, and
there are no output ports beyond `output` and `stored_data`.

Failure codes are `jobflow.missing_dependency`, `jobflow.document_invalid`,
`jobflow.maker_config_failed`, `jobflow.input_invalid`, `jobflow.make_failed`,
`jobflow.job_failed` and `jobflow.flow_failed`.

#### atomate2

atomate2 VASP execution settings belong to atomate2 (`~/.atomate2.yaml`,
`ATOMATE2_VASP_CMD`), not to *httk* workflow settings. atomate2 workflows run
here as jobflow Makers; this is not a FireWorks runner.

### httk-v1 package

The v1 form has no document member and uses the ordinary manager:

```toml
[workflow]
name = "example.v1"

[workflow.runner]
format = "httk-v1"
taskset = "vasp"
attempts = 10

[workflow.inputs.structure]
entry_type = "structures"

[workflow.collect]
file = "collect.py"
```

The directory must contain an executable `ht_steps` or `ht_run`, possibly as
`.template`. `taskset` selects the claim pool and `attempts` sets the v1 retry
budget. `data_mode` and `workdir_mode` are forbidden: the realization forces
no transactional data and a persistent `ht.run.current`. The packaged
`v1_runner.py` runs through the normal `path` executor, with no v1-specific
manager or capability. {doc}`v1_compatibility` covers the environment entries
and legacy runtime behavior.

## Bare documents and one-shot jobs

`job new` recognizes a bare CWL document, a PWD `.json` graph or a jobflow
Maker `.json` document:

```console
httk job new --workspace WS --from-runner flow.cwl --input message=echo
httk job new --workspace WS --from-runner workflow.json \
  --parameter pwd_module_path='["."]'
httk job new --workspace WS --from-runner maker.json
```

When path matching is not appropriate, `--format FORMAT` selects the reader:
`cwl`, `pwd` or `jobflow`. A manifest package directory or registered workflow
id rejects `--format`, since its format is already declared.

### Synthesized workflows

The resolver synthesizes an anonymous workflow with id `<format>.<stem>` and
generates its declaration. Document input ports become hook-consumed inputs;
document outputs become `records`-typed outputs. A bare jobflow Maker document
exposes only its `output` result, so inputs must be embedded in the document
or declared in a package. Bare PWD module roots come from `pwd_module_path`,
and bare v1 template globals from `--parameter` values.

### Campaigns

`--input-from` can batch a document or package input. The workflow is
resolved and prepared once, then instantiated per job. httk-v1 snapshots the
source package at preparation, so edits during a campaign cannot leak into
later jobs; symlinks in a v1 package are rejected. Realization-produced
parameters are reserved, and a caller collision is an error. `publish=` is
ignored, because these realizations supply installed runners instead of
copying them to the workspace runner store.

## Collection

### Collector selection

A registered provider's collector comes first. Without a provider, a job of
one of these formats falls back through its `workflow_language` job parameter
to the format default:

| Format | Default output document | Default behavior |
| --- | --- | --- |
| CWL | `cwl-outputs.json` | map ports to declared roles; single `File` values become `files` entries and lists remain `DataRecord` values |
| PWD | `pwd-outputs.json` | map ports to declared roles and create `DataRecord` values |
| jobflow | `jobflow-outputs.json` | map `output` and optional `stored_data` to declared roles and create `DataRecord` values |
| httk-v1 | none | degrade unless the package declares `[workflow.collect]` |

A provider's custom hook is authoritative. Such a package records
`workflow_collect = "package"` in the job; collecting it without the provider
degrades with a registration hint instead of running a format default. The
`allow_job_collector` pinned-tree fallback is tried only after the format
fallback, and only when its digest and manifest match the job. A failed format
or hook collector degrades that job without stopping its siblings.

### Default collectors

The CWL, PWD and jobflow defaults read the output JSON from the workdir or
transactional data tree, map document ports to manifest roles and return
`DataRecord` values. A CWL single `File` output must also have a readable path
inside the workspace, workdir or data tree; it becomes a standard `files`
entry. File lists keep their descriptor values and record sha256 evidence.

## CWL supported subset

cwl-utils parses CWL into a self-contained JSON plan with `run:` references
inlined. The runner executes:

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

These are refused before submission:

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

`DockerRequirement` is recorded as the `docker` capability with a warning; the
runner executes directly and does not pull, build or enter an image.
Unsupported hints are dropped with a warning. Failure codes include
`cwl.tool_failed`, `cwl.input_missing`, `cwl.output_missing`,
`cwl.scatter_invalid`, `cwl.unsatisfiable` and `cwl.child_invalid`.

## Python API and registry

The format registry is `httk.workflow.compat.available_languages()`,
`language(name)` (by format name), `match_document(path)`,
`runner_path(package, name)` and `runner_reference(package, name)`. The
format loaders are `httk.workflow.compat.cwl.load_cwl_plan` and
`httk.workflow.compat.pwd.load_pwd_document`;
`httk.workflow.compat.jobflow.document_from_maker` creates a jobflow document
from an MSONable Maker.

```python
from httk.workflow import Workspace, new_job

workspace = Workspace("workflow-workspace")
job = new_job(workspace, "flow.cwl", inputs={"message": "echo"})
print(job.job_key)
```
