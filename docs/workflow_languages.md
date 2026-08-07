# Workflow-language runner realizations

*For workflows written as CWL, PWD, or httk v1 task templates.*

The language integrations are runner realizations, not import commands. A
document or template is resolved into an ordinary job by `job new`; the job is
claimed, retried, checkpointed, journalled, and collected by the normal
workflow machinery.

| Language | Bare document or package form | Installed runner |
| --- | --- | --- |
| CWL | `job new WS --workflow flow.cwl` | `pkg:httk.workflow.languages.cwl/cwl_runner.py` |
| PWD | `job new WS --workflow graph.json` | `pkg:httk.workflow.languages.pwd/pwd_runner.py` |
| httk-v1 | a template directory, or a package with `language = "httk-v1"` | payload runner under executor `httk-v1` |

The CWL realization needs `httk-workflow[cwl]` when the document is prepared.
The normalized plan is carried by the job, so the machine executing the job
does not need the parser extra.

## Language packages

A package selects a language in `[workflow.runner]`. The language supplies its
steps, instantiate behavior, runner, workdir contract, and default
postprocessor. A package may override the default with `[workflow.postprocess]`.
Language inputs are consumed by that realization, so `destination` is forbidden
and `[workflow.instantiate]` is implied and forbidden. The optional `port` key
maps a package input or output name to a document port; omitted ports use the
package name. Ports are checked against the document and duplicates are errors.

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
roles. CWL `File` outputs are converted during collection to file-descriptor
`DataRecord` values, with workspace-confined paths, size, and sha256 evidence.

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

[workflow.postprocess]
file = "postprocess.py"
```

The source directory must contain executable `ht_steps` or `ht_run`, including
`.template` forms. `taskset` selects the claim pool and `attempts` sets the v1
retry budget. `data_mode` and `workdir_mode` are forbidden: the realization
forces no transactional data and persistent `ht.run.current`.

## Bare documents and one-shot jobs

`job new` recognizes a bare CWL document, a PWD `.json` graph, or a bare v1
template directory:

```console
httk workflow job new WS --workflow flow.cwl --input message=echo
httk workflow job new WS --workflow workflow.json \
  --parameter pwd_module_path='["."]'
httk workflow job new WS --workflow ./v1-template \
  --parameter structure=structures/si.cif
```

The resolver synthesizes an anonymous workflow with id `<language>.<stem>`.
Document input ports become hook-consumed inputs; document outputs become
`records`-typed outputs and the resolver generates the declaration. Bare PWD
module roots come from `pwd_module_path`. Bare v1 template globals come from
`--parameter` values.

`--input-from` may batch a document or package input. Campaign preparation is
shared: the workflow is resolved and prepared once, then instantiated once per
job. For httk-v1, the source package is snapshotted at preparation, so edits
made during a campaign cannot leak into later jobs. Symlinks in a v1 package
are rejected. Language-produced parameters are reserved; a caller collision is
an error. `publish=` is ignored for language workflows because their runners
are installed or selected by the v1 executor, not copied to the workspace
runner store.

## Collection

The collector chooses a registered provider's postprocessor first. A language
job without a provider falls back through its job `workflow_language` parameter
to the language default:

| Language | Default output document | Default behavior |
| --- | --- | --- |
| CWL | `cwl-outputs.json` | map ports to declared roles and create `DataRecord` values |
| PWD | `pwd-outputs.json` | map ports to declared roles and create `DataRecord` values |
| httk-v1 | none | degrade unless the package declares `[workflow.postprocess]` |

The registered provider's custom hook is authoritative. Such a package records
`workflow_postprocess = "package"` in the job; a provider-less collection
degrades with a registration hint rather than silently running a language
default. The `allow_job_postprocessor` pinned-tree fallback is attempted only
after the language fallback and only when its digest and manifest match the
job. A failed language or hook postprocessor degrades that job and does not
stop collection of its siblings.

The default CWL/PWD postprocessors read the output JSON from the workdir or
transactional data tree, map document ports to manifest roles, and return
`DataRecord` values. CWL `File` outputs additionally require a readable path
inside the workspace, workdir, or data tree and record sha256 evidence.

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
`httk.workflow.languages.pwd.load_pwd_document`.

```python
from httk.workflow import Workspace, new_job

workspace = Workspace("workflow-workspace")
job = new_job(workspace, "flow.cwl", inputs={"message": "echo"})
print(job.job_key)
```
