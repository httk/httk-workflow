# Compatibility with other workflow systems

`httk.workflow.compat` runs workflows written as CWL, PWD, jobflow Maker
documents or *httk* v1 task templates. These integrations are runner
realizations, not import commands: a document or template is installed in the
workspace like any workflow package, and its jobs are ordinary jobs that the
normal machinery claims, retries, checkpoints, logs and collects. A *format*
name selects the realization: `cwl`, `pwd`, `jobflow` or `httk-v1`.

An installed package of a format records it as `runner.builtin` in its
`install.json`. The manager then runs the realization's own runner from its
*httk* installation; the package carries no runner.

| Format | Bare document or package form | Built-in runner |
| --- | --- | --- |
| CWL | `job new --workspace WS --from-runner flow.cwl` | `httk.workflow.compat.cwl/cwl_runner.py` |
| PWD | `job new --workspace WS --from-runner graph.json` | `httk.workflow.compat.pwd/pwd_runner.py` |
| jobflow | `job new --workspace WS --from-runner maker.json`, or a package with `format = "jobflow"` | `httk.workflow.compat.jobflow/jobflow_runner.py` |
| httk-v1 | a package with `format = "httk-v1"` | `httk.workflow.compat.v1/v1_runner.py` |

A package directory is installed first, with `httk workflow install
--workspace WS DIR`, `job new --workflow-dir DIR --install` or
`new_job(..., install=True)`; see {doc}`/details/workflow_packages`.

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
The media type stays `application/octet-stream` unless a collector knows it,
and `format` is the reader tag the file name selects, if any. A list of files stays a
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
Monty-decoded, so a pymatgen `Molecule` or other MSONable object is passed as
its `as_dict()`. A path input is staged under `files/inputs/`; `.json` files are
Monty-decoded and other paths are loaded with `pymatgen.Structure.from_file`.
A live *httk* structure passed from Python (a `StructureBackend` such as
`UnitcellStructure`, `UnitcellStructureView` or `SymopsStructure`; other views,
such as an asymmetric-unit view, are refused as not JSON-serializable) is written as a staged `POSCAR` and so reaches `make()` as a
pymatgen `Structure`; job creation needs *httk-atomistic*, not pymatgen. The
`POSCAR` carries no magnetic moments, so a structure with site moments is
refused: pass a pymatgen `Structure` with `magmom` site properties as Monty
JSON (`structure.as_dict()`) instead. The Maker's `@module`, and the modules its serialized job functions
need, must be importable wherever the jobs run.

An input declared `directory = true` (accepted only for `format = "jobflow"`)
takes a directory, for example a finished calculation for atomate2's
`prev_dir`:

```toml
[workflow.inputs.prev_dir]
directory = true
description = "A finished VASP calculation to continue from."
```

Job creation copies the tree to `files/inputs/<port>/<basename>`, following
symbolic links so the copy is self-contained, so it travels with the job and is
visible under confinement. A dangling link or a special file in the tree, a
file given for a directory input, or a directory for any other input, is
refused; a relative path resolves against the package root. `make()` receives
the placeholder `httk-jobflow-input://<port>/<basename>`. A child job whose
arguments, or the output documents its references resolve to, contain the
placeholder copies the staged tree from the parent's payload, through
descriptors so the parent may be moved meanwhile, to its own
`run/inputs/<port>/<basename>` and gets that absolute path; the placeholder
stays as it is elsewhere in the store. A path into that copy in a job's output
ships back as the placeholder.

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
detour sub-flow. Documents are stored as plain JSON, so a `datetime` in an
output becomes its text form.

A child receives only the store documents its input references resolve
through (every index of the referenced jobs, the jobs a replaced job's output
refers to, and their blobs), not the whole flow's store. A job whose
`JobConfig.expose_store` is true, and so may query the store itself, receives
the full store.

#### Resources per job

Every child inherits the package's `[workflow.resources]`. A table under the
runner options sets resources per jobflow job:

```toml
[workflow.resources]
procs = 1
maxtime = "2-00:00:00"

[workflow.runner.resources."relax*"]
procs = 32
maxtime = "12:00:00"

[workflow.runner.resources."*static*"]
procs = 16

[workflow.runner.resources."*"]
procs = 4
```

Keys are `fnmatch` globs over the jobflow job name (case-sensitive), tried in
manifest order; values are validated like `[workflow.resources]`, with
`maxtime`/`mintime` as Slurm durations. When no glob matches, the job's
`JobConfig.manager_config["httk"]["resources"]` table applies (settable per job
with `Flow.update_config` or a `config` override); a malformed table there fails
the parent with `jobflow.resources_invalid`. The table found overlays the
inherited resources: named labels are replaced, the others kept (`mintime` is
never inherited). Without either, the child keeps the inherited resources. A
trailing `"*"` row makes every job's resources explicit. jobflow's
`pass_manager_config` copies a generating job's `manager_config` to the
dynamic jobs it creates, so name globs remain the way to set those apart.

A child's `maxtime` is capped at the parent attempt's effective `maxtime`, so
a package whose jobs need long walltimes declares a parent `[workflow.resources]`
`maxtime` at least as long; the parent's own steps are light. A table whose
`maxtime` exceeds the parent attempt's appends a run-log warning naming both.
A table no manager can satisfy (more `procs` or `mem` than any manager offers)
leaves that child, and so the flow, waiting rather than failed; `httk job why`
and the census show what it waits for.

#### Overrides

The job parameter `jobflow_overrides` is a list of operations applied before
anything runs. It needs no `[workflow.parameters]` declaration, is checked when
the job is created, and is recorded in `job.json`; a failing operation fails
the job with `jobflow.override_failed`, naming its index. The root Maker is
built with the declared parameters as constructor arguments, then every `maker`
operation updates it, then `make()` runs, then the other kinds apply to the
flow it returned; each group keeps list order, wherever in the list its
operations stand. The kinds are:

- `{"kind": "maker", "update": {"path": value, ...}}` sets fields of the root
  Maker before `make()`. A path is dotted field names, through nested Makers and
  into mappings, whose last key may be new; values are JSON, decoded with
  `monty`, so an object with `@module`/`@class` becomes that object (one whose
  module or class cannot be imported fails the job with
  `jobflow.override_failed`), and `null` is a value (atomate2 skips a step
  whose Maker is `None`). The Makers and mappings along a path are rebuilt,
  never modified. This is the way to set the
  fields `make()` itself reads, which `maker_kwargs` cannot reach, for atomate2's
  `PhononMaker`:

  ```json
  [{"kind": "maker", "update": {
    "min_length": 10,
    "phonon_displacement_maker.input_set_generator.user_incar_settings.ENCUT": 600}}]
  ```

- `{"kind": "maker_kwargs", "update": {...}, "name_filter": ..., "class_filter":
  "module:Class", "nested": true, "dict_mod": false}` calls
  `update_maker_kwargs` on the flow: the update applies to every Maker whose
  `name` contains `name_filter` (a substring) and that is an instance of
  `class_filter`, including Makers nested in other Makers' fields.
- `{"kind": "config", "config": {...}, "name_filter": ..., "attributes": [...],
  "dynamic": true, "dict_mod": false}` calls `update_config`: jobs whose name
  contains `name_filter` get the `JobConfig` fields named in `attributes` (by
  default every key of `config`). This is the per-job route to
  `manager_config["httk"]["resources"]`.
- `{"kind": "function", "function": "module:callable", "kwargs": {...}}` calls
  `callable(flow, **kwargs)` and continues with its return value when that is a
  jobflow `Flow` or `Job` (atomate2's powerups return modified copies); a
  `class_filter` string in `kwargs` is imported first.

With `dict_mod`, the update is a MongoDB-style modification whose `->` paths
reach into nested fields, for example:

```json
[{"kind": "maker_kwargs", "dict_mod": true,
  "update": {"_set": {"input_set_generator->user_incar_settings->ENCUT": 600}}}]
```

The same change through an atomate2 powerup:

```json
[{"kind": "function", "function": "atomate2.vasp.powerups:update_user_incar_settings",
  "kwargs": {"incar_updates": {"ENCUT": 600}}}]
```

Name filters differ: the resource globs match the whole job name with
`fnmatch`, while `update_config` and `update_maker_kwargs` match a substring of
the job or Maker name.

Each jobflow job runs in its child's `run/exec` directory, and jobs that chain
by directory (atomate2's `dir_name`, received by the next job as `prev_dir`)
would record a path that is stale once that child's job directory moves to its
terminal state or is transferred. A child therefore ships every reference into
its `exec` directory, plain or `host:`-prefixed, as
`httk-jobflow-exec://<job_id>[/subpath]`, and so ships back any sibling path
it resolved. A consuming child resolves these tokens to the producing job's
current `run/exec` path, without a host prefix, for its own run only. A token
in the job's own inputs, or in the output document of a job it references,
whose job is not in the workspace fails the step with
`jobflow.reference_unresolved`; elsewhere in the store it stays in place with a
run-log warning. The parent's `jobflow-outputs.json` keeps the tokens: a stable
reference to the job that produced each directory. Tokens resolve through the
placements the driver recorded for each producer; a token minted by another
driver stays unresolved (`jobflow.reference_unresolved`), and
`children.placement=flat` is the remedy only when both drivers share a placement
and `flat` was set before the producer was spawned.

#### Outputs and failures

The runner writes `jobflow-outputs.json` with one primary `output` port, the
flow's resolved final output. A `stored_data` mapping returned by jobs is
passed through as an extra output; collect it as a declared output with
`port = "stored_data"`. The default collector maps these values to declared
roles as `DataRecord` values. FileRecord outputs are not implemented yet, and
there are no output ports beyond `output` and `stored_data`.

Failure codes are `jobflow.missing_dependency`, `jobflow.document_invalid`,
`jobflow.maker_config_failed`, `jobflow.input_invalid`, `jobflow.make_failed`,
`jobflow.job_failed`, `jobflow.flow_failed`, `jobflow.launch_conflict`,
`jobflow.reference_unresolved`, `jobflow.resources_invalid` and
`jobflow.override_failed`.

#### atomate2

atomate2 workflows run here as jobflow Makers; this is not a FireWorks runner.
Ready-made atomate2 VASP workflow packages are in
[workflows-vasp-atomate2](https://github.com/httk/workflows-vasp-atomate2).
Before any job module is imported, the `enter` step maps *httk* settings into
the environment variables atomate2 and pymatgen read (`start`, where no VASP
runs, maps all but the VASP commands). These variables override
`~/.atomate2.yaml` and pymatgen's `~/.config/.pmgrc.yaml`, which a confined
attempt cannot see (its `HOME` is private):

| Setting | Variable |
| --- | --- |
| `vasp.command` | `ATOMATE2_VASP_CMD` |
| `vasp.gamma_command` (default: `vasp.command`) | `ATOMATE2_VASP_GAMMA_CMD` |
| `vasp.ncl_command` (default: `vasp.command`) | `ATOMATE2_VASP_NCL_CMD` |
| `pymatgen.psp_dir` | `PMG_VASP_PSP_DIR` |
| any other `atomate2.<key>` | `ATOMATE2_<KEY>` |
| any other `pymatgen.<key>` | `PMG_<KEY>` |

The three VASP commands resolve like `a.setting` (job parameter, then
`HTTK_VASP_COMMAND` and so on, then the workspace setting). Job parameters with
these names or `vasp.parallelization`, or starting with `atomate2.` or
`pymatgen.`, are forwarded from the parent to every child job, so a job-level
value reaches each step that runs VASP. As for the VASP code package,
`vasp.command` names only the program, such as `vasp_std`. A command that
starts with `srun`, `mpirun` or another launcher while a launch prefix applies
fails a job whose function lives in an `atomate2.` module with
`jobflow.launch_conflict`. Any other jobflow job runs without the VASP
variables, with a run-log warning; a custom Maker defined elsewhere that still
calls atomate2's `run_vasp` therefore gets that warning and atomate2's own
configuration. Do not set `atomate2.vasp_cmd` through the generic passthrough:
it bypasses the launch prefix; use `vasp.command`.

When *httk-workflow-vasp* is installed where the jobs run (checked without
importing the VASP package), all three VASP variables name its launch wrapper,
in front of the launch prefix:

```console
<python> -P -m httk.codes.vasp.launch --standard 'vasp_std' --ncl 'vasp_ncl' --gamma 'vasp_gam'
```

`--ncl` is `vasp.command` when `vasp.ncl_command` is unset, and `--gamma` is
passed only when `vasp.gamma_command` is set. Each command is a program-only
shell-quoted string (a launcher in any of them is a `jobflow.launch_conflict`
as above); `-P` keeps the job's working directory off the wrapper's module
path. The wrapper runs once per VASP launch, in the job, because that is where
its inputs exist: a job's `LSORBIT`/`LNONCOLLINEAR` comes from its input-set
generator, `ATOMATE2_VASP_INCAR_UPDATES`, an inherited `prev_dir` INCAR and
pymatgen's handling of non-collinear magnetic moments, and the INCAR is only
written inside the job. It runs the non-collinear program for `LSORBIT` or
`LNONCOLLINEAR`, the Gamma-only one for a Gamma-only mesh (custodian's rule,
never under `LEPSILON` or `LOPTICS`), and the standard one otherwise. Before
that it writes the `KPAR` and `NCORE` it plans from the inputs and the
attempt's rank count into the INCAR, unless the INCAR sets `KPAR`, `NCORE` or
`NPAR` already (so custodian's fixes stick; with atomate2's `VASP_INHERIT_INCAR`
enabled, which is not the default, later steps inherit the planned tags even
on a different rank count, so leave it off or set the tags explicitly) or
`vasp.parallelization` is `off`;
then it applies the attempt's launch prefix (`HTTK_WORKFLOW_LAUNCH`) itself and
execs. Each decision is a `httk.codes.vasp.launch: parallel:` line in the
job's `std_err.txt` and `httk-vasp-launch.log`; *httk-workflow-vasp*'s
documentation states the planning rules. Whichever command custodian's
automatic Gamma-only switch picks, it runs the same wrapper, which decides
Gamma itself.

Without *httk-workflow-vasp* each command maps directly with the launch prefix
prepended, the gamma and noncollinear commands defaulting to the `vasp.command`
mapping so custodian's automatic Gamma-only switch never runs a bare `vasp_gam`
outside the prefix; set variant commands are then mapped after a run-log
warning, and no parallelization is planned. atomate2 itself never reads
`ATOMATE2_VASP_NCL_CMD`.

`pymatgen.psp_dir` is a POTCAR library in pymatgen's layout
(`POT_GGA_PAW_PBE/POTCAR.<El>` and so on), distinct from *httk-workflow-vasp*'s
`vasp.pseudo_library`. A confined attempt sees it only when it lies inside
`confine.readonly_paths`. Other `atomate2.*` and `pymatgen.*` settings pass
through with the key upper-cased and dots replaced by underscores, as text;
boolean and structured values are not passed. For example:

```console
httk workspace settings set --key vasp.command --value vasp_std default
httk workspace settings set --key pymatgen.psp_dir --value /opt/potcars default
httk workspace settings set --key atomate2.vasp_custodian_max_errors --value 3 default
```

Without `vasp.command` no VASP variable is set and atomate2's own configuration
(`~/.atomate2.yaml`, `ATOMATE2_VASP_CMD`) applies, which a confined attempt
does not see.

custodian starts VASP in a process group of its own, which the manager's stop
signal to the attempt does not reach. When `psutil` is installed (custodian
requires it), a `SIGTERM` or `SIGINT` to a running job is forwarded to every
such descendant group; every such group is killed after a
grace period, including members that outlive their leader, before the job
itself stops.

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
budget. The realization keeps a persistent `ht.run.current`. The built-in `v1_runner.py`
runs like any other runner, with no v1-specific manager or capability.
{doc}`/details/v1_compatibility` covers the environment entries and legacy
runtime behavior.

## Bare documents and one-shot jobs

`job new` recognizes a bare CWL document, a PWD `.json` graph or a jobflow
Maker `.json` document, and installs it ad hoc before creating the job:

```console
httk job new --workspace WS --from-runner flow.cwl --input message=echo
httk job new --workspace WS --from-runner workflow.json \
  --parameter pwd_module_path='["."]'
httk job new --workspace WS --from-runner maker.json
```

When path matching is not appropriate, `--format FORMAT` selects the reader:
`cwl`, `pwd` or `jobflow`. A package directory rejects `--format`, since its
format is already declared. `httk workflow install --format FORMAT` installs a
bare document, or with `httk-v1` a bare task directory
({doc}`/details/v1_compatibility`), the same way without creating a job.

### Synthesized workflows

The installer wraps the document in a generated package named
`<format>.<stem>`, installed as `adhoc:<format>.<stem>@<sha12>` (the first 12
hex digits of the document's SHA-256). Document input ports become
hook-consumed inputs;
document outputs become `records`-typed outputs. A bare jobflow Maker document
exposes only its `output` result, so inputs must be embedded in the document
or declared in a package. Bare PWD module roots come from `pwd_module_path`,
and bare v1 template globals from `--parameter` values.

### Campaigns

`--input-from` can batch a document or package input. The workflow is
resolved and prepared once, then instantiated per job. Every job is built from
the installed copy, so edits to the source during a campaign cannot leak into
later jobs; symlinks in a package are rejected at installation.
Realization-produced parameters are reserved, and a caller collision is an
error.

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
committed data tree, map document ports to manifest roles and return
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
`language(name)` (by format name), `match_document(path)` and
`runner_path(package, name)`. The
format loaders are `httk.workflow.compat.cwl.load_cwl_plan` and
`httk.workflow.compat.pwd.load_pwd_document`;
`httk.workflow.compat.jobflow.document_from_maker` creates a jobflow document
from an MSONable Maker.

```python
from httk.workflow import Workspace, new_job

workspace = Workspace("workflow-workspace")
job = new_job(workspace, "flow.cwl", inputs={"message": "echo"})  # installed ad hoc
print(job.job_key)
```
