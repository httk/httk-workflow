# Workflow packages in detail

A workflow package is one portable directory that describes, instantiates,
runs, and collects a workflow. Its `httk_workflow.toml` manifest is the
*httk*-owned glue around a runner, not an embedded OPTIMADE declaration
language: it generates the workflow declaration, or points to an externally
authored one that it validates and carries.

## Package layout

The smallest useful package has a manifest and an executable `run` entry (or a
`[workflow.runner] command` in its place):

```text
my-workflow/
├── httk_workflow.toml
├── run
├── instantiate.py       # optional
├── instantiate           # optional executable hook
├── collect.py            # optional
├── collect               # optional executable hook
└── support/              # any regular support files
```

`run` receives the normal runner environment and publishes the outcome
protocol the manager reads. The runner entry, hooks, and postprocess scripts
may be written in any language that runs as an executable on the host. A
workflow is a manifest plus the members it references.

A `.py` instantiate or collect hook runs in-process; any other hook runs as an
executable under the contracts in [Hooks and trust](#hooks-and-trust). Both
assemble successful outputs the same way but handle collector failures
differently.

Manifest members must be relative regular files inside the package, which may
also hold other support files. A non-`.py` instantiate or collect member, and
a selected postprocess member, must be executable (`chmod +x`). Symlinks,
special files, absolute names, and `..` members are refused.

Resolve and register a package in Python:

```python
from httk.workflow.packages import load_workflow_package

provider = load_workflow_package("my-workflow")  # registers id and alias
```

The command line can use the directory without registering it:

```console
httk workflow describe ./my-workflow
httk job new --workspace WS --workflow-dir ./my-workflow --input structure=POSCAR
```

## Runner realizations

`[workflow.runner]` selects one of four forms. The executable form is the
ordinary package runner. The `format` forms delegate instantiate, run, and
default collection to a registered realization in `httk.workflow.compat` (see
{doc}`/details/workflow_compat`).

| Form | Manifest selector | Required/allowed members | Runner contract |
| --- | --- | --- | --- |
| executable entry | no `format` | `entry` or `command`, `steps`, `initial_step`, `data_mode`, `workdir_mode` | the package entry (default `run`) or the declared `command`, plus the declared step set |
| document format | `format = "cwl"` or `"pwd"` and `document` | format keys only; `port` is allowed on document inputs/outputs | installed `cwl_runner.py` or `pwd_runner.py` |
| jobflow | `format = "jobflow"` and `maker` or `document` | `maker` or `document` (exactly one); `port` is allowed on inputs/outputs; no mode keys | installed `jobflow_runner.py` |
| httk-v1 | `format = "httk-v1"` and no `document` | `taskset`, `attempts`; no mode keys | package snapshot plus `pkg:httk.workflow.compat.v1/v1_runner.py` through the ordinary `path` runner |

In the `format` forms the realization supplies the steps and consumes the
inputs, so `entry`, `command`, `steps`, `initial_step`, `[workflow.instantiate]`
and input `destination` keys are forbidden. An omitted httk-v1 destination
becomes an `ht.instantiate.py` global. Jobflow and httk-v1 cannot set
`data_mode` or `workdir_mode`: jobflow pins a persistent workdir, and httk-v1
forces `none` and a persistent `ht.run.current`. `[workflow.collect]` may
override the default collector; CWL, PWD, and jobflow have one, while httk-v1
has none and normally declares a hook. Unknown format keys are errors.

## `httk_workflow.toml` reference

`parse_workflow_manifest` validates the vocabulary below, and unknown keys at
any level are errors. TOML syntax, member containment, nonempty names,
aliases, runner modes, hook members, parameter destinations, input defaults,
and output relationships are all validated before a provider is returned.

### `[workflow]`

| Key | Required | Meaning |
| --- | --- | --- |
| `name` | yes | Nonempty workflow name with no whitespace, not starting with `git+`. It is the registry key, the `job.json` workflow, and the collect dispatch key. For a package installed by git URI it is the short name, and the URI is the id. |
| `alias` | no | Alternate name matching `[a-z0-9._-]+`. |
| `description` | no | Human-readable summary and generated declaration description. |
| `declaration_uri` | no | String `$id` for the generated or external workflow declaration. |
| `declaration_file` | no | Relative regular-file member containing an externally authored OPTIMADE-format workflow declaration JSON. |
| `resources` | no | Default resource requirements: a table mapping resource labels to non-negative integers. |
| `steps` | no | Per-step resource overrides. Valid only with an executable runner, and only for names in its declared `steps` list. |
| `calls` | no | The sub-workflows this one calls: a `[workflow.calls]` table mapping an alias to a workflow name or git URI. |
| `requires` | no | Minimum distribution versions as a list of `NAME>=VERSION` strings (only `>=`, a plain `N(.N)*` release, each distribution once), for example `["httk-workflow>=2.2.0", "httk-atomistic>=2.1.2"]`. |

```toml
[workflow]
name = "example.relax"
alias = "relax"
description = "Relax one structure."
declaration_uri = "https://example.org/workflows/relax"
# declaration_file = "declaration.json"
requires = ["httk-workflow>=2.2.0"]

[workflow.resources]
procs = 4
mem = 4096

[workflow.steps.relax.resources]
procs = 8
```

### Where `requires` is checked

`requires` is checked twice, each time against the interpreter doing the work:

- **At job creation**, when `job new`, `campaign submit`, `workflow describe`,
  or `Attempt.call` resolves the workflow: a package directory, a registered
  or plugin-bundled name, or a git URI (after it is fetched). The refusal lists
  every unmet requirement and its installed version. A plugin's
  `[plugin] requires` applies to every workflow it bundles and is merged in,
  keeping the higher minimum per distribution.
- **At claim time.** The requirements are copied into `job.json` as the
  optional `requires` member. A manager whose environment does not meet them
  leaves the job unclaimed for another manager, as with a missing capability.
  Install the required versions in its environment and restart it.

The manager puts its own interpreter's directory first on `PATH` for every
runner, so a Python runner's `#!/usr/bin/env python3` is the interpreter the
requirements were checked in and needs no import guard. Describe, executable
instantiate hooks, and postprocess scripts get the same `PATH`.

### `[workflow.calls]`: declared sub-workflows

A workflow that calls others with `Attempt.call` (or `httk_workflow_call` and
the other SDKs' `call`) declares them, so its dependencies are known before
any job runs:

```toml
[workflow.calls]
child = "examples.subworkflow-child"
relax = "git+https://github.com/httk/workflows-vasp@458aacb2493586faa2c9ac033334457569aeaf75#vasp-relax"
```

Each key is an alias (label syntax) and may not equal another entry's
reference. Each value is either a name selecting a registered, plugin-bundled,
or installed workflow, or a git URI pinned to a full commit hash, so every job
of a campaign calls the same definition. A path is refused, because it would
resolve against whatever directory a job is created from.

The declaration is checked three times:

- **At job creation**, every declared call must resolve, transitively; a job
  with an unknown dependency is refused. The job records the calls as the
  `job.json` `calls` member: a name as written, resolved again wherever the
  job runs, and a git URI in canonical, commit-pinned form, so the job calls
  the definition that existed at creation.
- **At claim time**, a manager claims the job only when every recorded call
  resolves on its machine and, for a compiled package, is built in the
  workspace, again transitively. The check only looks things up: a git URI
  resolves through the installed cache and is never fetched. A running
  manager notices a newly installed or built workflow within about a minute.
  Until then the job stays unclaimed, and `httk job why`,
  `httk workflow precheck`, and the manager's idle summary name what to
  install or build. `httk workflow build NAME` builds a workflow and,
  transitively, every workflow it declares it calls.
- **At call time**, a job whose workflow declares `[workflow.calls]` may call
  only those workflows, by alias (`a.call("child", ...)`) or by the recorded
  reference. A job run from your own runner file has no manifest and may call
  anything.

### `[workflow.resources]` and `[workflow.steps.NAME]`

`[workflow.resources]` maps validated resource labels to non-negative,
non-boolean integers; units are opaque. An executable runner may add
`[workflow.steps.NAME]` tables, each holding only a `resources` table, where
`NAME` must occur in `[workflow.runner].steps`. `format` runners reject
`[workflow.steps]`, because the realization supplies the steps.

```toml
[workflow.resources]
procs = 4
mem = 16000            # MB

[workflow.steps.relax]
resources = { procs = 32, mem = 120000 }

[workflow.steps.analyse]
resources = { procs = 1, mem = 2000, matlab_license_slots = 1 }
```

Here `relax` and `analyse` override the defaults. The manager's advertised
capacities decide whether each activation fits.

### `[workflow.runner]`: executable form

Without `format`, the table describes an executable runner.

| Key | Required/default | Meaning |
| --- | --- | --- |
| `entry` | `"run"` | Relative executable package member the manager runs, for example `run.py` or `run.sh`; excludes `command`. |
| `command` | absent | Argument vector the manager runs instead of the entry; excludes `entry`. |
| `initial_step` | `"start"` when present; otherwise sole step | First scaffolded step. |
| `steps` | required | Nonempty runner step list. |
| `data_mode` | `"none"` | `"none"` or `"transactional"`. |
| `workdir_mode` | `"persistent"` | `"persistent"` or `"isolated"`. |

`initial_step` is required when `steps` has several names and no `start`.

A descriptive entry name such as `run.py` or `run.sh` is recommended, because
it keeps editors and linters working. An entry other than `run` is recorded in
each job as the command `["{package}/<entry>"]` and follows the `command`
rules below: it must be an executable regular member (`chmod +x`, with a `#!`
line), not a build artifact, and the package must have no `run` member, which
a manager that predates `runner.command` would run instead.

`command` is a nonempty array of strings naming the program a compiled, JVM,
or interpreted package runs, so no one-line `run` bridge script is needed. An
element may use only two placeholders: `{package}`, the verified published
package tree, and `{artifacts}`, the build registered for this machine (see
[Building and registering binaries](#building-and-registering-binaries)). The
manager appends the job's runner arguments and runs the expanded vector in the
job workdir with the attempt environment (including
`HTTK_WORKFLOW_RUNNER_ARTIFACTS`) and any workflow prelude.

```toml
[workflow.runner]
command = ["{artifacts}/relax"]                        # C, C++, Fortran, Ada, Rust
# command = ["java", "-cp", "{artifacts}/classes", "Relax"]
# command = ["perl", "{package}/relax.pl"]
steps = ["publish", "prepare", "run"]
initial_step = "prepare"
```

The command is validated when the package loads:

- A placeholder starts its element or directly follows `NAME=` (as in
  `-Dhome={package}`). The rest of the element is empty or `/PATH`, a relative
  POSIX path whose parts are nonempty and not `.` or `..`.
- The first element starts with a placeholder or is a bare program name found
  on the attempt `PATH` (`java`, `perl`, `python3`). Absolute paths and other
  paths containing `/` are refused.
- Each `{package}/MEMBER` names an existing regular source member (not a build
  artifact); as the program, it must be executable.
- `{artifacts}` requires `[workflow.build]`, and each `{artifacts}/PATH` must
  be a relative path covered by `[workflow.build].artifacts`.
- The package must not contain a `run` member. A job records the command in
  `job.json`, and a manager that predates `command` looks for `run` and fails
  with `runner_unavailable` instead of running a stray entry.

A command using `{artifacts}` fails with `runner_not_built` until its build is
registered, like a `run` bridge into the artifacts. `httk workflow describe`
runs a command without `{artifacts}` with `HTTK_WORKFLOW_DESCRIBE=1` to check
the manifest steps. Describe has no build registration, so for a compiled
package it reports the manifest alone.

### `[workflow.runner]`: format vocabulary

| Key | CWL/PWD | jobflow | httk-v1 | Meaning |
| --- | --- | --- | --- | --- |
| `format` | required: `"cwl"` or `"pwd"` | required: `"jobflow"` | required: `"httk-v1"` | Select the realization. |
| `document` | required, relative regular member | optional, relative regular member; mutually exclusive with `maker` | forbidden | Format document member. |
| `maker` | forbidden | required when `document` is omitted; `module:Class` Maker spec | forbidden | Import a Maker class for the jobflow root. |
| `port` | optional on an input/output table | optional on an input/output table | forbidden | Alias a manifest name to a document or jobflow port. |
| `modules` | PWD only, list of relative `.py` members | forbidden | forbidden | Package Python modules to stage. |
| `module_path` | PWD only, list of import roots | forbidden | forbidden | Additional PWD import roots. |
| `allowed_modules` | PWD only, list of module prefixes | forbidden | forbidden | PWD import allowlist. |
| `taskset` | forbidden | forbidden | label, default `"default"` | v1 claim pool. |
| `attempts` | forbidden | forbidden | integer, default `10` | v1 retry budget. |
| `entry`, `steps`, `initial_step` | forbidden | forbidden | forbidden | Built-in realization steps. |
| `data_mode`, `workdir_mode` | allowed only in executable form | forbidden; jobflow pins a persistent workdir | forbidden | v1 forces `none`/persistent. |

`port` defaults to the manifest name. For a document format, each effective
input and output port must exist in the document and occur only once. Jobflow
ports are open, because Maker `make()` signatures are not inspected. Preparing
a `maker`-form job imports the named module on the submitting machine to
verify the class, so submit where the Maker is installed; describing or
resolving the package never imports it.

### `[workflow.inputs.<NAME>]`

| Key | Meaning |
| --- | --- |
| `destination` | Optional payload-relative destination for executable runners. Omit it when `[workflow.instantiate]` consumes the value. `format` runners forbid it because the realization's hook consumes every input; for httk-v1 an omitted destination is an `ht.instantiate.py` global. |
| `description` | Optional input description. |
| `entry_type` | Optional declaration entry type. |
| `ref` | Optional declaration reference. |
| `role` | Optional declaration role; defaults to the input key. |
| `required` | Optional boolean; defaults to `true` when the input declares `entry_type`, otherwise `false`. |

A required input must be supplied at submission: staging its `destination`
(including a directly staged file) satisfies it, and a hook-consumed input
needs a value. The check runs before any instantiate hook, so a missing input
is refused without running package code, and hooks need not null-check
required inputs. It does not apply to `format` workflows, which satisfy their
own inputs.

```toml
[workflow.inputs.structure]
destination = "POSCAR"
entry_type = "structures"
ref = "https://example.org/types/structure"
description = "The starting structure."
role = "initial_structure"
# required defaults to true here because entry_type is declared.

[workflow.inputs.settings]
description = "Values consumed by instantiate.py."
role = "settings"
```

### `[workflow.parameters.<NAME>]`

Each parameter accepts `type`, `description`, and `default`. `type` is one of
`"string"`, `"number"`, `"integer"`, `"boolean"`, `"array"`, or `"object"`,
and a default must have that JSON/TOML type.

```toml
[workflow.parameters.kpoint_density]
type = "number"
default = 30.0
description = "Sampling density."
```

A workflow that declares no parameters leaves the channel fully open. Once it
declares any, three rules apply at submission:

- **Defaults** fill in each declared name nobody supplied and are recorded
  verbatim in `job.json`. They are applied *after* any instantiate hook. The
  hook's parameters hold only caller-supplied values plus any a format
  realization wires in; it sees the declared defaults separately, as
  `InstantiateContext.defaults` or the executable envelope's `defaults`. The
  hook's final parameters therefore win over defaults, and a caller-supplied
  value stays unless the hook changes it.
- **Types** are enforced: a supplied value, or one the hook sets, that
  mismatches its declared `type` is an error, as in the environment channel.
- **Undeclared names** are kept, because parameters are open, with one stderr
  warning naming it and the declared names.

The declared parameter and input metadata also travel in the optional
`job.json` `declared` member (sections `parameters` and `inputs`), shaped like
the environment member, so a later precheck can read them.

### `[workflow.environment.<NAME>]`

Environment entries are declared, typed settings a runner consumes. Each table
accepts `type`, `description`, `default`, and `setting`. `type` is `string`,
`number`, `integer`, `boolean`, `array`, or `object`; when present, defaults
and job overrides must have that JSON/TOML type. `setting` names the dotted
workspace setting, and so the environment variable used for lookup; it
defaults to the entry name. Format realizations may contribute entries (the
`httk-v1` realization contributes four `httk_v1.*` entries), and a manifest
entry overrides one with the same name.

```toml
[workflow.environment.command]
type = "string"
description = "Executable used by the runner."
setting = "tool.command"
default = "echo"
```

Resolution is most-specific first:

1. the job override;
2. the declared setting's `HTTK_` environment variable, upper-cased with dots
   replaced by underscores;
3. the workspace application setting;
4. the declaration's `default`.

`Attempt.environment(name, default=...)` reads the resolved value of a
declared name and raises `KeyError` for an unresolved one unless the call
supplies a default. `Attempt.setting` is the separate, untyped
application-setting lookup, ordered parameter, `HTTK_*`, workspace, call
default.

The runner resolves every declared entry at attempt start, before any step
code runs. A missing default-less entry or a type error publishes the
non-retryable `environment_unresolved` failure, naming the layers consulted
and the remedies. On success, the values and their source layers are recorded
as the observed `environment` declaration
(`httk-workflow-environment-resolution` version 2). A one-line run-log note
records the same resolution after the handler completes, so the gate never
creates or touches a workdir before the handler; if the handler aborts before
a workdir exists, the declaration is kept and the note omitted. The snapshot
holds for the whole attempt, and unchanged resolutions on later activations
add no observed data or log lines.

The CLI supplies per-job overrides with repeatable `--environment NAME=VALUE`,
decoding JSON values when possible. In Python, `new_job(environment={...})`
supplies shared overrides, and each `JobItem` in `new_jobs` may carry its own
`environment` mapping. `workflow describe` shows each entry's type, setting,
default, and description.

### `[workflow.outputs.<NAME>]`

`entry_type` is required; `ref`, `description`, `product_of`, and `role` are
optional, and `role` defaults to the output key. `product_of` is a scalar
reference to an input or output role naming "the single entity this output is
an attribute-like property of". Joint derivations stay unmarked; the `Run`
carries them. Self-references, output cycles, ambiguous parameter/output
names, and unknown roles are rejected.

```toml
[workflow.outputs.relaxed]
entry_type = "structures"
ref = "https://example.org/types/structure"
description = "The relaxed structure."
product_of = "initial_structure"
role = "relaxed_structure"
```

### `[workflow.instantiate]` and `[workflow.collect]`

The presence of either table declares that hook. Each has exactly one key,
`file`, naming a relative regular member: a `.py` file selects the in-process
Python path, and any other must be executable and selects the subprocess
contract. `workflow describe` reports `kind=python` or `kind=executable` for
each. The contracts are in [Hooks and trust](#hooks-and-trust).

```toml
[workflow.instantiate]
file = "instantiate.py"

[workflow.collect]
file = "collect.py"
```

### `[workflow.postprocess.<NAME>]`

Curated postprocess scripts are provider-owned executables, explicitly
selected and run after a job is collected. A package can declare several:

```toml
[workflow.postprocess.relaxation-report]
file = "scripts/relaxation_report"
description = "write a text and JSON relaxation summary"

[workflow.postprocess.archive]
file = "scripts/archive_results.sh"
description = "copy selected results to an archive"
```

| Key | Required | Meaning |
| --- | --- | --- |
| `<NAME>` | required | The name selected by `httk workflow postprocess --script`. |
| `file` | required | A relative executable regular file inside the registered package. |
| `description` | optional | Human-readable text shown by `workflow describe`. |

`describe` lists them under `postprocess scripts:`, separately from the
collect hook, which is the provider's output adapter returning role-keyed data
to `collect`. The old flat `[workflow.postprocess]` table with a `file` key is
rejected with an error pointing the collect hook to `[workflow.collect]`.

Scripts run only from the registered provider package or the package given
with `--workflow-dir`, never from job payloads or pinned workflow-store trees
(loading them from those trees is future work). A script receives `HTTK_WORKFLOW_WORKSPACE_DIR`,
`HTTK_WORKFLOW_JOB_DIR` (the immutable payload, read-only to the script), and
`HTTK_WORKFLOW_POSTPROCESS_DIR` (the output directory), plus
`HTTK_WORKFLOW_WORKDIR` and `HTTK_WORKFLOW_DATA_DIR` when they exist, so
scripts should fall back across them. It runs with the framework's
interpreter directory first on `PATH`, in the output directory
`<root>/<placement>/<job_key>/<NAME>/`, where reports belong. The root is
`<workspace>/postprocess` by default, the `postprocess.directory` workspace
setting when set, or the `postprocess --output-dir` override. Output never lands in the payload,
so a sealed job can still be postprocessed.

### `[workflow.build]`

A compiled package declares its foreground build and disposable outputs:

```toml
[workflow.build]
command = "make"
platform = "uname -sm"       # optional; omit for one `any` registration
artifacts = ["relax", "*.o"]
```

| Key | Required | Meaning |
| --- | --- | --- |
| `command` | yes | Shell-word build command string. |
| `platform` | no | Shell-word platform probe command; omit it for one `any` registration. |
| `artifacts` | yes | Nonempty list of relative POSIX `fnmatch` patterns for build outputs. |

Patterns match relative paths, and a directory match covers its subtree. They
may not strip `run` or `httk_workflow.toml`, so a committed `run` entry and
the manifest stay in the source package. See
[Building and registering binaries](#building-and-registering-binaries).

## Inputs, parameters, and files

A job is created from three kinds of things, and the distinction carries
meaning.

**Inputs** are what the workflow operates on, named in its declaration and
described by OPTIMADE property and entry-type definitions. They define what
the workflow is: two runs of `vasp.relax` on different structures are one
workflow applied to different inputs, and the inputs and declared outputs give
its `$id` meaning across databases. Inputs are staged into the payload at
creation (`new_job(..., inputs={"structure": ...})`), become the input roles
of the recorded provenance, and are what a served entry's `has_input` edges
point to.

**Parameters** are the knobs one implementation exposes: cutoffs, densities,
tolerances, switches. They are left out of the declaration by design. Almost
any of the hundreds of settings in a VASP calculation could be hard-coded in
an INCAR template or lifted out for the caller. If each changed which
workflow ran, nearly every calculation would be a distinct workflow, the
declaration registry would fragment, and every knob would need a curated
property definition. Instead, an implementation may expose any number of
parameters without changing its declared identity. They need no property
definitions and travel in `job.json` as opaque JSON, digest-pinned and
reproducible, but never appear among the declared inputs and outputs.

**Files** stage additional payload content by name, without either role.

The implementer draws the boundary. A knob that changes what is computed, not
just how carefully or by what route, is not a parameter: make it a declared
input, or declare a different workflow.

## Generated and external declarations

Without `declaration_file`, `workflow_declaration_from_manifest(provider)`
generates an OPTIMADE-format document with optional `$id` and `description`,
entry-typed input entries, and output entries. `product_of` is curation
metadata and is never emitted. The document becomes
`provider.declarations["workflow"]` and is embedded in `job.json`.

With `declaration_file`, the document is validated and embedded verbatim. Its
`$id` must equal `declaration_uri` when both are given, its input and output
role maps must exactly cover the manifest's entry-typed inputs and outputs,
and it must not contain `product_of`. The declaration is the authoritative
OPTIMADE document; the manifest is the authoritative *httk*-owned glue.

## Building and registering binaries

The `[workflow.build]` vocabulary, the build engine, `BuildSpec`, and its
execution helpers are shared `httk.core.building` machinery. *httk-workflow*
owns the workspace store layout and the platform-tagged build registrations.

### Build environment

The build command runs in a copy of the published source tree and can use only
files in that package and the installed language SDKs. Its environment keeps
no `HTTK_WORKFLOW_*` variable except `HTTK_WORKFLOW_LANGUAGES_DIR`, the
absolute path of the installed `httk/workflow/languages` directory, with one
subdirectory per SDK (`c`, `cpp`, `fortran`, `rust`, `ada`, `java`, `perl`).
The manager exports the same variable to every attempt. A C package, for
example, builds with `cc -I"$HTTK_WORKFLOW_LANGUAGES_DIR/c" relax.c
"$HTTK_WORKFLOW_LANGUAGES_DIR/c/httk_workflow.c" -o relax`.

The source digest does not cover the SDK, so re-register builds after
upgrading *httk-workflow*, or vendor the SDK into the package. The
per-language relax packages of
[workflows-vasp-other-languages](https://github.com/httk/workflows-vasp-other-languages)
are complete examples.

### Sources-only publication

Build-declaring packages publish sources only. Declared artifacts are
stripped, and the digest pins the remaining sources, not a machine's compiler
output. Binaries never ride a transfer bundle, so each destination builds its
own native runner. The manager passes the registered artifact directory to the
runner as `HTTK_WORKFLOW_RUNNER_ARTIFACTS` and never modifies or overlays the
published tree.

The build command is never inferred from the runner entry. A compiled package
declares `command = ["{artifacts}/relax"]`, or locates its binaries under
`$HTTK_WORKFLOW_RUNNER_ARTIFACTS` from a committed `run` entry. The runner
runs with the job workdir as its cwd.

### Registering a build

Register one foreground build per platform class:

```console
httk workflow build --workspace WORKSPACE ./my-workflow
httk job new --workspace WORKSPACE --workflow-dir ./my-workflow --step prepare
httk workflow run --workspace WORKSPACE
```

The target may also be a workflow reference, resolved as by
`httk job new --workflow`: a registered id or alias, a plugin-bundled
workflow, an installed git workflow's short name, or a git URI (fetched and
installed). The command builds the package directory the reference resolves
to, which is the registration a job selecting that name pins, so you need not
locate a plugin's installed directory:

```console
httk workflow build --workspace WORKSPACE examples.chain-rust
httk job new --workspace WORKSPACE --workflow examples.chain-rust
```

A target spelled as a path (absolute, `./`, `../`, or containing `/`) is
always a package directory; write `./NAME` for one in the current directory.
A bare name is a workspace runner-store name if that entry exists, otherwise a
workflow reference, otherwise a job reference. A workflow without
`[workflow.build]` succeeds with "nothing to build"; one that is not a
directory package (a packaged runner file) is an error.

Managers never build, which avoids a thundering herd compiling the same
package. On a shared filesystem, one registration per platform tag serves all
matching nodes. A heterogeneous cluster should declare `platform` and build
once per resulting tag; without it, one `any` registration gives every node
the same binary regardless of architecture.

### Registrations and failures

Registrations live under
`WORKSPACE/.httk-workspace/runner-builds/<store-name>/<tag>/current.json`,
which points to `<tag>/gen-*/artifacts/`. Each generation's `build.json`
records the source digest, command, platform probe, and time; the sibling
`<tag>.log` records the command metadata and exit status. Re-registration
keeps prior generations; remove the tag directory only while no managers are
running. `httk workflow build --workspace WORKSPACE --list` lists
registrations.

A missing or stale registration fails with a structured `runner_not_built`; a
failed platform probe, build, or artifact collection with
`runner_build_failed`. Both are terminal unless the job opts into `retry_on`,
for example `retry_on = ["runner_not_built"]`.

`workflow precheck` reports a missing unqualified registration (`platform`
omitted) as a problem. Platform-specific builds are indeterminate, because a
local precheck cannot stand in for the manager's machine, which probes its own
platform at attempt start.

## Installed plugin workflows

Installed *httk₂* plugins bundle workflow package directories by listing them
in `httk_plugin.toml`; the full plugin manifest and installation rules are
documented in *httk-core*:

```toml
[plugin]
workflows = ["workflows/relax"]
```

Each directory loads as a normal workflow package, with its canonical id and
optional alias. Resolution checks in-process registrations first and plugins
only on a miss, so an in-process registration wins. A name provided by two
plugins is poisoned: looking it up raises an error naming both. Other plugin
workflows stay resolvable.

Plugin discovery is lazy and cached per process, so start a new process after
installing a plugin. Installation is explicit consent, so plugin workflows
share the installed/registered trust tier. They are published and built
through the normal pinning and build-registration path; resolving a name
never executes a hook or build command. Listings and unknown-workflow hints
label plugin entries `[plugin PLUGIN_NAME]`, with any alias, and
`workflow describe` reports `source: installed-package`.

## Hooks and trust

### Python hooks

Python hooks keep their in-process signatures:

```python
def instantiate(context):
    # context.payload, context.inputs, context.parameters (caller-supplied only),
    # context.defaults (declared defaults, read-only), context.tag
    ...


def collect(record):
    # return {"output_role": entry_object, ...}
    ...
```

An instantiate hook runs during scaffolding and may write the payload or
update parameters. A collect hook runs during `collect` and returns role-keyed
outputs. The framework validates the roles, derives unfulfilled ones, writes
each manifest/provider `product_of` curation into its data-record output as a
`product_of` edge (record content; `--into` rewrites it to the store-minted
id), overlays output edges onto the `Run`, and emits `ProductLink` values.

A direct package path's instantiate hook and the job-pinned collect fallback
run from the published, digest-pinned tree. Registered-directory collectors
run current source bytes, by explicit registration consent.

### Executable instantiate hook

The hook is launched from the published, digest-pinned package tree with the
staging payload as working directory. Inherited `HTTK_WORKFLOW_*` variables
are removed; only `HTTK_WORKFLOW_WORKSPACE_DIR` (the workspace path) is
supplied, and the framework's interpreter directory is first on `PATH`. One
JSON request arrives on stdin:

```json
{
  "format": "httk-workflow-instantiate",
  "format_version": 3,
  "workflow": "example.relax",
  "tag": "silicon",
  "parameters": {"cutoff": 520},
  "defaults": {"cutoff": 450, "kpoint_density": 30.0},
  "inputs": {
    "structure": {"kind": "file", "path": "files/inputs/structure/POSCAR"},
    "settings": {"kind": "value", "value": {"kpoints": [4, 4, 4]}}
  }
}
```

`tag` is a string or `null`. `parameters`, `defaults`, and `inputs` are
objects: `parameters` holds only caller-supplied values (plus any a format
realization wires in), and `defaults` the declared parameter defaults, applied
after the hook to every parameter still absent. Version 3 added `defaults` and
stopped merging them into `parameters`; `httk.workflow.hookapi` accepts only
version 3. An input descriptor is `{"kind": "file", "path": "<payload-relative
POSIX path>"}` or `{"kind": "value", "value": <JSON value>}`. The hook may
read file inputs relative to its working directory and write files into the
payload. It returns one JSON object on stdout:

```json
{"parameters": {"cutoff": 520, "derived": "ready"}, "tag": "silicon-4x4x4"}
```

`parameters` is a required object and is merged into the job parameters. An
optional string `tag` is used only when the caller supplied none. A nonzero
exit, malformed stdout, or an invalid response aborts submission.

Before launch, the framework serializes the inputs the hook consumes (those
without a manifest `destination`):

| Supplied input | Descriptor and staged member |
| --- | --- |
| Existing regular file path or path-like value | `{"kind": "file", "path": "files/inputs/<name>/<basename>"}`; the file is copied there. |
| JSON-native value | `{"kind": "value", "value": <value>}` with the value unchanged. Accepted shapes are strings, booleans, `null`, finite numbers, lists, and string-keyed mappings, recursively. |
| Live object or other non-JSON-native value | Everything else goes through registered-writer serialization. `httk.core.save` probes registered dispatch keys in deterministic sorted order: extension keys stage `files/inputs/<name>/<name><extension>`, while exact-basename keys stage `files/inputs/<name>/<basename>`; the first successful writer wins. |

An object with no registered writer is a submission error naming its type and
the remedies: use a `.py` hook, or register a `httk.core` writer. The Python
path receives the original `InstantiateContext` and inputs; serialization is a
property of the executable boundary, not a semantic difference.

### Executable collect hook

The hook runs with its package tree as working directory. For a direct package
path and the opt-in job-pinned fallback, that is the published tree, whose
full digest is checked against `job.json`; a registered-directory provider is
the explicit-consent exception and runs its current source tree.

One process handles all of a collector's records in a sweep. It receives a
handshake line, then one record line per job in collection order:

```json
{"format": "httk-workflow-collect-stream", "format_version": 2}
{"record": {"workspace_id": "workspace", "job_id": "job-1", "state": "succeeded", "job": {}}}
```

`record` is the complete `JobRecord.as_mapping()` mapping, shortened above.
The hook writes one response line per record, in the same order:

```json
{"job_id": "job-1", "outputs": {"energy": {"value": 3.14}}}
```

or:

```json
{"job_id": "job-1", "error": "could not read the result"}
```

The response `job_id` must match the record. A malformed response, wrong job
id, explicit error, missing response, or unresolvable output degrades that job
only; the sweep continues with the other responses.

Each output value is exactly one of these wrappers:

| Wrapper | Result |
| --- | --- |
| `{"entry": { ... }}` | A registered entry type is reconstructed as its real entry. The mapping must contain a registered string `type`, which must equal the declared output's `entry_type` when one is declared; a declared `ref` naming a registered definition IRI narrows the choice, and an unrelated `ref` is ignored with a warning. A registered record that constructs itself from the mapping (`from_obj`, as `files`, `records`, and `runs` do) owns it; otherwise the mapping is the family's served OPTIMADE form (for `structures`: `lattice_vectors`, `species`, `species_at_sites`, `cartesian_site_positions`, and so on, plus `_httk_*` extensions) and is read through the family's OPTIMADE entry binding, the one a remote entry is read with, via `httk.core.optimade.served_entry`. An optional `id` must match the constructed record (for a served form, its content id). |
| `{"value": <JSON value>}` | A `DataRecord`. If the declared output has `ref`, the referenced property definition is loaded and the value is hard-validated with `httk-store` (which is required at collect time); without `ref`, a generated `_httk_custom_*` property definition is used. |
| `{"file": "<path>"}` | A workspace-confined `FileRecord`. The wrapper must contain exactly the `file` key, and the path must resolve to a regular file below the workspace or workdir. |

The discriminator is reserved, and extra keys are rejected. The `url` of a
workflow-produced `files` entry is a workspace-relative POSIX locator,
resolved against a root at read time and containment-checked, not a
dereferenceable URL; this keeps it relocation-stable and follows the
local-file locator convention.

The Python path hands ordinary objects to the same assembler, with the same
role validation and record assembly. Only failures differ: an exception from a
registered Python collector aborts collection iteration, while a bad
executable response degrades only its job.

### Run and product curation

The job-embedded declaration governs the `Run`, the immutable facts per job.
Product curation (`product_of` edges on data records and `ProductLink`s)
comes from the live registered provider's manifest, so it reflects today's
curation. Under the job-pinned fallback it comes from the job's verified
pinned manifest and keeps its historical curation. With neither reachable, no
products are emitted.

### Trust tiers

1. **Installed/registered.** A provider registered by a package or domain
   carries its runner and collect adapter. Installed runner references are
   trusted by the registration and distribution boundary.
2. **Explicit-path consent.** A package or runner given by an explicit
   filesystem path is used because the caller selected it. Registered
   directory providers run the current source-directory hook bytes at
   collection time, without a digest gate. A direct package path is
   published, and its scaffold instantiate hook runs from the pinned tree.
3. **Job collector, off by default and digest-verified.** `collect` loads a
   collect hook from a job-pinned workspace package tree only with
   `allow_job_collector=True` (CLI `--allow-job-collector`). The tree's
   manifest `name` must equal the job workflow (for a git URI job, the
   installed short name; not compared when the URI is not installed here), and
   its full tree digest must match `job.json`. Refusal or tampering degrades
   that job instead of stopping the sweep.

## Git URI references

A workflow reference starting with `git+` is a git URI
({py:mod}`httk.workflow.git_workflows`, built on the shared
`httk.core.git_sources`). One that fails to parse is an error and never falls
through to path or id resolution.

### Grammar

```text
git+SCHEME://AUTHORITY/PATH[@REF][#SUBDIR]      SCHEME = https | http | file
```

`#` is split first (the fragment is the package subdirectory), then `@REF` at
the last `@` after the authority, so refs may contain `/`. The authority may
be empty only for `file`. Refused: ssh and `git@host:path` forms, userinfo in
the authority (it would be copied into `job.json`), a query, an empty ref or
one starting with `-`, an empty fragment, and a subdirectory that is
absolute, has backslashes, or has empty, `.`, or `..` components.

### Canonical form

The canonical form is
`git+SCHEME://AUTHORITY(scheme and host lowercased)PATH(trailing / stripped)@COMMIT[#SUBDIR]`,
where `COMMIT` is the full lowercase commit hash (40 or 64 hex digits). The
path is otherwise verbatim, so `…/repo` and `…/repo.git` differ.

The canonical URI is the provider's `workflow_id` and `definition_uri`, the
job's `workflow`, and the collected `Run`'s `workflow_definition_uri`. It
identifies the workflow *definition* (its code); the declaration `$id` stays
the manifest's `declaration_uri`, if any. The manifest `[workflow] name`, which
may not start with `git+`, becomes the provider's short `name`.

### Cache layout

Under `httk.core.userdirs.data_home()`:

```text
git/<sha256(repository)[:16]>/<commit>/                 checkout tree, .git removed
git/<sha256(repository)[:16]>/<commit>.json             checkout record
workflows/installed/<sha256(canonical URI)[:32]>.json   installed workflow entry
```

A pinned URI whose `<commit>/` tree exists is a cache hit and runs no git.
Clones land in a temporary directory beside the final name and are renamed
into place; JSON files are replaced atomically. Every explicit reference
rewrites the installed entry, refreshing `referenced_at`. Git runs without
hooks, global or system configuration, credential helpers, `GIT_*`
environment, or terminal prompts, so private repositories are not supported.

A package whose tree cannot be published (for example one containing a
symlink) is refused at fetch time, before installation. Its instantiate hook
runs from the published, digest-pinned tree; its collect hook and postprocess
scripts run from the installed cache tree, like a plugin package.
`httk workflow install URI...` installs without creating a job, and
`httk workflow uninstall SELECTOR...` removes installed entries, never the
cached checkouts.

### Resolution precedence

`resolve_workflow`, and so `new_job`, `job new --workflow`,
`campaign submit --workflow`, `workflow describe`, and `Attempt.call`, tries a
git URI first, fetching and installing it with
{py:func}`~httk.workflow.git_workflows.fetch_workflow`; the result is
published and digest-pinned into the runner store like any directory package.
A short name resolves to in-process registrations, then installed plugins,
then installed git workflows by `name` or `alias`. Among installed entries
claiming the name, one lineage (`repository`, `subdir`) selects its most
recently referenced commit, and several lineages are an error naming the
competing URIs. `workflow list` and the unknown-workflow hint include
installed short names that are neither shadowed nor conflicted.

### Cache-only lookup

`workflow_provider(URI)`, like
{py:func}`~httk.workflow.git_workflows.fetched_workflows`, returns an installed
provider only for a pinned URI with an installed entry and otherwise `None`,
also for malformed URI text. It never runs git or writes files. Collection
dispatches through it, so a job payload can never cause code acquisition.
With `--allow-job-collector`, a job whose URI is not installed here uses its
digest-verified pinned tree without a name comparison; if the URI is
installed, the tree's manifest name must equal the installed short name.

## Publication and lifecycle

### Publication

`job new --workflow-dir` publishes the complete package tree into the
workspace runner store: one tree digest, a read-only tree, and the full digest
recorded in `job.json`, which the manager verifies before execution.
Republishing identical content is an idempotent no-op; changed content cannot
replace an existing name without an explicit replacement.

`publish=` is ignored for `format` workflows, which use their installed
realization runners. `new_jobs` and CLI `--input-from` campaigns prepare a
format package once and instantiate it per job. Realization-produced parameter
names are reserved, and collisions fail loudly. httk-v1 snapshots the complete
package at preparation, so later edits do not affect later jobs, and rejects
symlinks.

### Lifecycle commands

The usual lifecycle is instantiate, run, then collect:

```console
httk job new --workspace WS --workflow-dir ./my-workflow \\
    --input-from structure structures/ \\
    --parameter kpoint_density=30.0 \\
    --placement project/screening
httk workflow run --workspace WS
httk collect --workspace WS
httk collect --workspace WS --into results.sqlite --id-base httk.workflow
```

`httk workflow describe TARGET [--json]` reports a registered id or alias,
runner file, or package directory without publishing it. `collect` emits one
`CollectedJob` summary per line by default, `--raw` emits `JobRecord`
records, and `--allow-job-collector` enables the third trust tier.

### Storing results

In Python, call `store.save(...)` yourself. `--into` is the CLI shortcut: it
opens a file-backed SQLite `SqlStore`, saves output entries, runs, and
products, and reports stored ids. Entry families and record classes resolve
lazily from the core registry, so output types may require *httk-store* and
*httk-atomistic*. `--id-base BASE` is required with `--into`, and
`--id-series SERIES` defaults to `1`. Edges to outputs without store ids use
content ids until the outputs are saved; `--into` then rewrites them to the
minted ids.

Each job's entries, run, and products are stored as one job-level operation.
A storage failure appears on that job's summary as `storage_error` while other
jobs may still be stored, so a sweep can partially succeed. Check each JSONL
line's `storage_error` before retrying or reconciling the destination store.

See {doc}`/details/declarations` for declaration carriage,
{doc}`/details/provenance` for the tree-pinned provenance handoff,
{doc}`/details/collecting` for collect-hook and fallback behavior, and
{doc}`workflow_cli` for the complete command reference.
