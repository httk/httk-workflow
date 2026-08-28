# Workflow packages in detail

*For workflow authors who want one portable directory to describe, instantiate,
run, and collect a workflow.*

A workflow package is a directory whose `httk_workflow.toml` is the strictly
*httk*-owned glue around a runner. It is not an embedded OPTIMADE declaration
language: the manifest can generate the workflow declaration, or point to an
externally authored declaration that it validates and carries.

## Package layout

The smallest useful package has an executable `run` entry and a manifest:

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

`run` receives the normal runner environment and publishes the outcome protocol
used by the manager. A workflow package is language-independent: its runner
entry, instantiate hook, collect hook, and postprocess scripts may be written in
any language available as an executable on the host. A workflow is a manifest
plus the members it references. Python hooks remain first-class: a `.py`
instantiate or collect hook uses the existing in-process path, while an
executable hook uses the contracts below. Successful outputs share the same
assembly semantics; collector failure handling is intentionally different and
is documented below.

A package may contain other regular files needed by its entry or hooks; manifest
members must be relative regular files inside the package. `.py` is the Python
fast path for instantiate and collect; a non-`.py` instantiate or collect member
must have execute mode (`chmod +x`). Postprocess members must also be
executable when selected. Symlinks, special files, absolute names, and `..`
members are refused.

Resolve or register a package with the Python API:

```python
from httk.workflow.packages import load_workflow_package

provider = load_workflow_package("my-workflow")  # registers id and alias
```

The command line can use the directory without registering it:

```console
httk workflow describe ./my-workflow
httk workflow job new --workspace WS --workflow-dir ./my-workflow --input structure=POSCAR
```

## Runner realizations

`[workflow.runner]` selects one of four forms. The executable form is the
ordinary package runner. The language forms delegate instantiate, run, and
default collection to the registered language realization.

| Form | Manifest selector | Required/allowed members | Runner contract |
| --- | --- | --- | --- |
| executable entry | no `language` | `entry`, `steps`, `initial_step`, `data_mode`, `workdir_mode` | package `run` plus the declared step set |
| document language | `language = "cwl"` or `"pwd"` and `document` | language keys only; `port` is allowed on document inputs/outputs | installed `cwl_runner.py` or `pwd_runner.py` |
| jobflow | `language = "jobflow"` and `maker` or `document` | `maker` or `document` (exactly one); `port` is allowed on inputs/outputs; no mode keys | installed `jobflow_runner.py` |
| httk-v1 | `language = "httk-v1"` and no `document` | `taskset`, `attempts`; no mode keys | package snapshot plus `pkg:httk.workflow.languages.httk_v1/v1_runner.py` through the ordinary `path` runner |

For language forms, `entry`, `steps`, and `initial_step` are forbidden because
the language supplies built-in steps. `[workflow.instantiate]` is forbidden
because language inputs are hook-consumed. `destination` is forbidden on
CWL/PWD, jobflow, and httk-v1 inputs; an omitted v1 destination is an
`ht.instantiate.py` global. Language workflows may declare
`[workflow.collect]` to override the default. CWL and PWD have defaults;
jobflow has a default; httk-v1 has none and normally declares a hook.

Language manifests cannot set `data_mode` or `workdir_mode` for jobflow or
httk-v1. Jobflow pins the workdir persistent; httk-v1 forces `none` and
persistent `ht.run.current`. Unknown language keys are errors.

## `httk_workflow.toml` reference

This is the complete manifest vocabulary validated by
`parse_workflow_manifest`. Unknown keys at any level are errors. TOML syntax,
member containment, nonempty names, aliases, runner modes, hook members,
parameter destinations, input defaults, and output relationships are validated
before a provider is returned.

### `[workflow]`

| Key | Required | Meaning |
| --- | --- | --- |
| `id` | yes | Nonempty workflow id with no whitespace. This is the registry key, the `job.json` workflow, and the collect dispatch key. |
| `alias` | no | Alternate name matching `[a-z0-9._-]+`. |
| `description` | no | Human-readable summary and generated declaration description. |
| `declaration_uri` | no | String `$id` for the generated or external workflow declaration. |
| `declaration_file` | no | Relative regular-file member containing an externally authored OPTIMADE-format workflow declaration JSON. |
| `resources` | no | Default resource requirements, a table mapping resource labels to non-negative integer values. |
| `steps` | no | Per-step resource overrides; only valid with an executable runner and only for names in its declared `steps` list. |

```toml
[workflow]
id = "example.relax"
alias = "relax"
description = "Relax one structure."
declaration_uri = "https://example.org/workflows/relax"
# declaration_file = "declaration.json"

[workflow.resources]
procs = 4
mem = 4096

[workflow.steps.relax.resources]
procs = 8
```

### `[workflow.runner]`: executable form

With no `language`, the table is a normal executable runner. `steps` is a
nonempty list; if `initial_step` is omitted, `start` is selected when present,
or the sole step is selected. Otherwise `initial_step` is required.

| Key | Required/default | Meaning |
| --- | --- | --- |
| `entry` | `"run"` | Relative entry member; it must be named `run`. |
| `initial_step` | `"start"` when present; otherwise sole step | First scaffolded step. |
| `steps` | required | Nonempty runner step list. |
| `data_mode` | `"none"` | `"none"` or `"transactional"`. |
| `workdir_mode` | `"persistent"` | `"persistent"` or `"isolated"`. |

### `[workflow.resources]` and `[workflow.steps.NAME]`

`[workflow.resources]` maps resource labels to non-negative integers. Resource
names are validated labels, values are not booleans, and units are opaque. An
executable runner may add `[workflow.steps.NAME]` tables, each allowing only a
`resources` table. `NAME` must occur in `[workflow.runner].steps`; an unknown
name is an error. `[workflow.steps]` is rejected for language runners because
their step set is supplied by the language.

For example, a manifest can set workflow defaults and denser per-step
requirements together:

```toml
[workflow.resources]
procs = 4
mem = 16000            # MB

[workflow.steps.relax]
resources = { procs = 32, mem = 120000 }

[workflow.steps.analyse]
resources = { procs = 1, mem = 2000, matlab_license_slots = 1 }
```

The `relax` and `analyse` declarations override the defaults for those steps;
the manager's advertised capacities determine whether each activation fits.

### `[workflow.runner]`: language vocabulary

| Key | CWL/PWD | jobflow | httk-v1 | Meaning |
| --- | --- | --- | --- | --- |
| `language` | required: `"cwl"` or `"pwd"` | required: `"jobflow"` | required: `"httk-v1"` | Select the realization. |
| `document` | required, relative regular member | optional, relative regular member; mutually exclusive with `maker` | forbidden | Language document member. |
| `maker` | forbidden | required when `document` is omitted; `module:Class` Maker spec | forbidden | Import a Maker class for the jobflow root. |
| `port` | optional on an input/output table | optional on an input/output table | forbidden | Alias a manifest name to a document or jobflow port. |
| `modules` | PWD only, list of relative `.py` members | forbidden | forbidden | Package Python modules to stage. |
| `module_path` | PWD only, list of import roots | forbidden | forbidden | Additional PWD import roots. |
| `allowed_modules` | PWD only, list of module prefixes | forbidden | forbidden | PWD import allowlist. |
| `taskset` | forbidden | forbidden | label, default `"default"` | v1 claim pool. |
| `attempts` | forbidden | forbidden | integer, default `10` | v1 retry budget. |
| `entry`, `steps`, `initial_step` | forbidden | forbidden | forbidden | Built-in language steps. |
| `data_mode`, `workdir_mode` | allowed only in executable form | forbidden; jobflow pins a persistent workdir | forbidden | v1 forces `none`/persistent. |

For a document language, each effective input and output port must exist in the
document and may occur only once. `port` defaults to the manifest name. Jobflow
has open ports because Maker `make()` signatures are not inspected during
manifest preparation. Preparing a `maker`-form job imports the named module on
the submitting machine to verify the class exists, so submit where the Maker is
installed; merely describing or resolving the package never imports it.

## Building and registering binaries

A compiled package declares its foreground build and disposable outputs in
`[workflow.build]`:

```toml
[workflow.build]
command = "make"
platform = "uname -sm"       # optional; omit for one `any` registration
artifacts = ["relax", "*.o"]
```

`command` and the optional `platform` are shell-word command strings. Each
`artifacts` member is a relative POSIX `fnmatch` pattern. A directory match
covers that directory's subtree; patterns are evaluated against relative paths.
The patterns may not strip `run` or `httk_workflow.toml`, so the committed
`run` entry and manifest remain in the source package. The build command runs in
a copy of the published source tree and can use only files inside that package:
compiled packages must vendor their SDK and other build inputs. For example,
`examples/relax_cpp` vendors both halves of its native SDK.

The `[workflow.build]` vocabulary and build engine are shared
`httk.core.building` machinery; `BuildSpec` and its execution helpers live
there. *httk-workflow* owns the workspace store layout, platform-tagged build
registrations, and the manager's artifact overlay. The package build semantics
described here are unchanged.

Publication is sources-only for build-declaring packages. Declared artifacts are
stripped before publication, and the digest pins those remaining sources, not a
machine's compiler output. Binaries never ride a transfer bundle. This makes a
package self-contained and lets each destination build its own native runner.

The operational sequence is one foreground registration per platform class:

```console
httk workflow build --workspace WORKSPACE ./my-workflow
httk workflow job new --workspace WORKSPACE --workflow-dir ./my-workflow --step prepare
httk workflow run --workspace WORKSPACE
```

Managers never build. This rejects a thundering herd of managers compiling the
same package; on a shared filesystem, one registration for a platform tag serves
all matching nodes. A heterogeneous cluster should declare `platform` and run
the command once for each resulting tag. Omitting it on such a cluster creates
one `any` registration, so every node uses the same binary regardless of its
architecture.

Registrations live under
`WORKSPACE/.httk-workspace/runner-builds/<store-name>/<tag>/current.json`, which
points to `<tag>/gen-*/artifacts/`. Each generation's `build.json` stamp records
the source digest, command, platform probe, and time; the sibling `<tag>.log`
records the command metadata and exit status. Re-registration leaves prior
generations on disk; remove the tag directory only while no managers are
running. Use
`httk workflow build --workspace WORKSPACE --list` to inspect registrations. A missing or
stale registration produces structured `runner_not_built`; a failed platform
probe, build, or artifact collection produces `runner_build_failed`. These
manager-detected failures are terminal unless the job explicitly opts into
`retry_on`, for example `retry_on = ["runner_not_built"]`.

`workflow precheck` checks an unqualified (`platform` omitted) build
registration and reports a missing one as a problem. Platform-specific builds
are reported as indeterminate because the local precheck cannot stand in for the
manager's machine; the manager probes its own platform at attempt start. The
build command is never inferred from `run`: `run` must be committed, executable,
and usable as the package's source entry point before any build is registered.

### Hook tables

Each hook table has exactly one key, `file`, naming a relative regular file
member. A `.py` member selects the Python in-process fast path. Any other member
must be executable (`chmod +x`) and selects the language-neutral subprocess
contract. `workflow describe` reports `kind=python` or `kind=executable` for
each present hook. Presence of `[workflow.instantiate]` declares an instantiate
hook; presence of `[workflow.collect]` declares a collect hook.

```toml
[workflow.instantiate]
file = "instantiate.py"

[workflow.collect]
file = "collect.py"
```

### [workflow.postprocess.<NAME>]: curated scripts

Curated postprocess scripts are provider-owned executables that run after a job
has been collected. A package can declare more than one:

~~~toml
[workflow.postprocess.relaxation-report]
file = "scripts/relaxation_report"
description = "write a text and JSON relaxation summary"

[workflow.postprocess.archive]
file = "scripts/archive_results.sh"
description = "copy selected results to an archive"
~~~

| Key | Required | Meaning |
| --- | --- | --- |
| <NAME> | required | The name selected by httk workflow postprocess --script. |
| file | required | A relative executable regular file inside the registered package. |
| description | optional | Human-readable text shown by workflow describe. |

The old flat [workflow.postprocess] table with a file key is rejected with
a teaching error: the collect hook belongs in [workflow.collect], while
[workflow.postprocess.<NAME>] tables declare curated scripts.

Scripts run only from the registered provider package or from the package
explicitly supplied with --workflow-dir; they are never loaded from job
payloads or pinned workflow-store trees. (Loading curated scripts from those
trees is future work.) The process receives
HTTK_WORKFLOW_WORKSPACE_DIR, HTTK_WORKFLOW_JOB_DIR, and
HTTK_WORKFLOW_WORKDIR; when transactional data exists it also receives
HTTK_WORKFLOW_DATA_DIR, otherwise scripts should fall back to the workdir.
The current working directory is always workdir/postprocess/<NAME>/, where
reports should be written.

The collect hook is a provider output adapter that returns role-keyed data
for the collect verb; the postprocess scripts: section in describe lists
these separate, explicitly selected follow-up executables.

The `url` of a workflow-produced `files` entry is deliberately a
workspace-relative POSIX locator, resolved against a root at read time and
containment-checked, not a dereferenceable URL; it is relocation-stable by
design and follows the established local-file locator convention.

### `[workflow.inputs.<NAME>]`

Every input table accepts these keys:

| Key | Meaning |
| --- | --- |
| `destination` | Optional payload-relative destination for executable runners. Omit it when `[workflow.instantiate]` consumes the value. Language runners forbid it because the language hook consumes every input; for httk-v1 an omitted destination is an `ht.instantiate.py` global. |
| `description` | Optional input description. |
| `entry_type` | Optional declaration entry type. |
| `ref` | Optional declaration reference. |
| `role` | Optional declaration role; defaults to the input key. |
| `required` | Optional boolean. Defaults to `true` when the input declares `entry_type` and `false` otherwise. A required input must be supplied at submission — for an input with a `destination`, staging that destination (including a directly staged file) satisfies it; for a hook-consumed input, the value must be supplied. Language workflows satisfy their own inputs, so the check does not apply to them. |

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

Each parameter accepts `type`, `description`, and `default`. `type`, when present,
must be one of `"string"`, `"number"`, `"integer"`, `"boolean"`, `"array"`,
or `"object"`; a supplied default must have that matching JSON/TOML type.

```toml
[workflow.parameters.kpoint_density]
type = "number"
default = 30.0
description = "Sampling density."
```

When a workflow declares any parameters, three rules apply at submission — and
only then, because a workflow that declares no parameters leaves the channel
fully open. A `default` is applied for a declared name nobody supplied, so it
is recorded verbatim in `job.json`. A supplied value whose declared `type`
mismatches is an error, exactly like the environment channel. A supplied name
outside the declaration is *not* an error — parameters are deliberately open —
but it prints one warning on stderr naming it and the declared names, and the
value is kept. The declared parameter and input metadata are also carried in
`job.json`'s optional `declared` member (sections `parameters` and `inputs`),
mirroring the environment member's shape, so a later precheck can read them.

### `[workflow.environment.<NAME>]`

Environment entries are declared, typed workflow settings consumed by a
runner. Each table accepts `type`, `description`, `default`, and `setting`.
Language realizations may contribute entries automatically; manifest entries
override entries with the same name. The `httk-v1` realization contributes its
four `httk_v1.*` entries.
`type` may be `string`, `number`, `integer`, `boolean`, `array`, or `object`;
when present, defaults and job overrides must have that JSON/TOML type.
`setting` names the dotted workspace setting and therefore the environment
variable used for lookup; when omitted, the environment name is used.

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

`Attempt.environment(name, default=...)` reads this resolved value. The name
must be declared; an unresolved name raises `KeyError` unless the call supplies
its own default. `Attempt.setting` is the separate, untyped application-setting
lookup and follows its own parameter → `HTTK_*` → workspace → call-default
order.

At attempt start, before any step code runs, the runner resolves every declared
entry. A missing default-less entry or a type error publishes the non-retryable
`environment_unresolved` failure and names the consulted layers and remedies.
On success, the resolved values and their source layers are recorded as the
observed `environment` declaration in
`httk-workflow-environment-resolution` version 2, and a one-line run-log note
records the same resolution after the handler completes. Deferring that note
ensures the gate never creates or touches a workdir before the handler; if the
handler aborts before a workdir exists, the observed declaration is still kept
but the note is omitted. The snapshot is consistent for the whole attempt;
unchanged resolutions on later activations do not create new observed data or
log churn.

The CLI supplies per-job overrides with repeatable
`--environment NAME=VALUE`; JSON values are decoded when possible. Python
`new_job(environment={...})` supplies shared overrides, and each `JobItem` in
`new_jobs` may supply its own `environment` mapping. `workflow describe`
renders the declared environment entries, including type, setting, default, and
description.

### `[workflow.outputs.<NAME>]`

`entry_type` is required. The other accepted keys are `ref`, `description`,
`product_of`, and `role`. `role` defaults to the output key. `product_of` is a
scalar role reference: it may name an input role or another output role. It
means “the single entity this output is an attribute-like property of”; joint
derivations stay unmarked—the `Run` carries them. Self-references, output
cycles, ambiguous parameter/output names, and unknown roles are rejected.

```toml
[workflow.outputs.relaxed]
entry_type = "structures"
ref = "https://example.org/types/structure"
description = "The relaxed structure."
product_of = "initial_structure"
role = "relaxed_structure"
```

### Inputs, parameters, and files

A job is created from three kinds of things, and the distinction carries
meaning beyond convenience.

**Inputs** are the objects the workflow *operates on* — the things named in the
workflow's declaration, described by OPTIMADE property and entry-type
definitions. They define what the workflow *is*: two runs of `httk.vasp.relax`
on different structures are the same workflow applied to different inputs, and
it is the inputs (and the declared outputs) that give the workflow's `$id` its
meaning across databases. Inputs are staged into the job payload at creation
time (`new_job(..., inputs={"structure": ...})`), become the input roles of the
recorded provenance, and are what a served entry's `has_input` edges point back
to.

**Parameters** are the knobs a particular workflow *implementation* exposes —
cutoffs, densities, tolerances, switches. They are deliberately **not** part of
the declaration, and this is a design decision rather than an omission.
Practically everything in a VASP calculation could be regarded as an input:
hundreds of settings, each either hard-coded in an INCAR template or lifted out
as something the caller may adjust. If adjusting any of them changed *which*
workflow was being run, nearly every calculation would be a semantically
distinct workflow, the declaration registry would fragment into uselessness,
and every knob would demand a curated property definition. Parameters are the
escape from that: an implementation may lift as many knobs as it likes without
touching the workflow's declared identity. They require no property
definitions, travel in `job.json` as opaque JSON — so they remain
digest-pinned, recorded facts of the execution, fully reproducible — but they
never appear among the declared inputs and outputs.

**Files** stage additional payload content by name, without either role.

The boundary is a judgment the workflow implementer owns: if turning a knob
genuinely changes *what* is being computed — not just how carefully or by what
route — it does not belong among the parameters. It belongs as a declared
input, or in a differently declared workflow.

### Declarations

Without `declaration_file`, `workflow_declaration_from_manifest(provider)`
generates an OPTIMADE-format document with optional `$id` and `description`,
entry-typed input entries, and output entries. `product_of` is curation
metadata and is never emitted in this declaration. It becomes
`provider.declarations["workflow"]` and is embedded in `job.json`.

With `declaration_file`, the JSON document is loaded and embedded verbatim after
validation. Its `$id` must equal `declaration_uri` when both are supplied. Every
external input and output role maps must exactly cover the manifest's
entry-typed inputs and outputs. An external declaration must not contain
`product_of`; the declaration remains the authoritative OPTIMADE document, and
the manifest remains the authoritative strictly httk-owned package glue.

## Hooks and trust

### Installed plugin workflows

Installed *httk₂* plugins can bundle workflow package directories by declaring them
in `httk_plugin.toml`:

```toml
[plugin]
workflows = ["workflows/relax"]
```

Each listed directory is loaded as a normal workflow package, including its
canonical id and optional alias. The full plugin manifest and installation
rules are documented in *httk-core*; this is only the workflow-package entry
point.

Workflow resolution first checks in-process registrations. Only on a miss does
it consult installed plugins, so an in-process registration wins a plugin id or
alias. If two plugins provide the same id or alias, that name is poisoned:
looking it up raises an error naming both owning plugins. Other, non-conflicting
plugin workflows remain resolvable.

Plugin discovery is lazy and cached for the lifetime of the process. Install a
plugin, then start a new process before expecting its workflows to resolve.
Installing a plugin is explicit consent, so plugin-bundled workflows share the
installed/registered trust tier. Their packages are published and built through
the normal workspace pinning and build-registration path; resolving a name does
not execute a hook or build command.

Workflow-name listings and unknown-workflow hints label plugin entries with
`[plugin PLUGIN_NAME]`, including an alias when one exists. `workflow describe`
resolves these names and reports `source: installed-package`.

The hook contracts are deliberately small. Python hooks keep their existing
in-process signatures:

```python
def instantiate(context):
    # context.payload, context.inputs, context.parameters, context.tag
    ...


def collect(record):
    # return {"output_role": entry_object, ...}
    ...
```

An instantiate hook runs during scaffolding and may write the payload or update
parameters. A collect hook runs during `collect`; it returns role-keyed outputs.
The framework validates those roles, derives unfulfilled roles, overlays output
edges onto the `Run`, and emits `ProductLink` values from manifest/provider
`product_of`. A direct
package path's instantiate hook and the job-pinned collect fallback execute
from the published, digest-pinned tree; registered-directory collectors
instead execute current source bytes by explicit registration consent.

### Executable instantiate hook

An executable instantiate hook is launched from the published, digest-pinned
package tree. Its current working directory is the staging payload. The
framework removes inherited `HTTK_WORKFLOW_*` variables, then supplies only
`HTTK_WORKFLOW_WORKSPACE_DIR` with the workspace path. It sends one JSON request
on stdin:

```json
{
  "format": "httk-workflow-instantiate",
  "format_version": 2,
  "workflow": "example.relax",
  "tag": "silicon",
  "parameters": {"cutoff": 520},
  "inputs": {
    "structure": {"kind": "file", "path": "files/inputs/structure/POSCAR"},
    "settings": {"kind": "value", "value": {"kpoints": [4, 4, 4]}}
  }
}
```

`tag` is a string or `null`; `parameters` and `inputs` are JSON objects. An
input descriptor is either `{"kind": "file", "path": "<payload-relative POSIX
path>"}` or `{"kind": "value", "value": <JSON value>}`. The hook may read
file descriptors relative to its payload working directory, write files into
that payload, and return one JSON object on stdout:

```json
{"parameters": {"cutoff": 520, "derived": "ready"}, "tag": "silicon-4x4x4"}
```

`parameters` is required and must be an object. `tag` is optional and, when
returned, must be a string. Returned parameters are merged into the job
parameters; a returned tag is used only when the caller did not supply one.
Nonzero exit status, malformed stdout, or an invalid response aborts submission.

Before launching this form, the framework pre-serializes only inputs consumed
by the hook (`destination` omitted in the manifest):

| Supplied input | Descriptor and staged member |
| --- | --- |
| Existing regular file path or path-like value | `{"kind": "file", "path": "files/inputs/<name>/<basename>"}`; the file is copied there. |
| JSON-native value | `{"kind": "value", "value": <value>}` with the value unchanged. Accepted shapes are strings, booleans, `null`, finite numbers, lists, and string-keyed mappings, recursively. |
| Live object or other non-JSON-native value | Everything else goes through registered-writer serialization. `httk.core.save` probes registered dispatch keys in deterministic sorted order: extension keys stage `files/inputs/<name>/<name><extension>`, while exact-basename keys stage `files/inputs/<name>/<basename>`; the first successful writer wins. |

An object with no registered writer is a submission error naming the object type
and the remedies: use a `.py` hook, or register a `httk.core` writer. The Python
fast path receives the original `InstantiateContext` and inputs; this
pre-serialization is the executable boundary, not a semantic difference.

### Executable collect hook

An executable collect hook is launched with its package tree as the current
working directory. For a direct package path and the opt-in job-pinned fallback,
that is the published tree whose full digest is checked against `job.json`; a
registered-directory provider is the explicit-consent exception and runs its
current source tree. It receives one stream per executable-collector sweep:
first the handshake line, then one record line per job, in collection order:

```json
{"format": "httk-workflow-collect-stream", "format_version": 2}
{"record": {"workspace_id": "workspace", "job_id": "job-1", "state": "succeeded", "job": {}}}
```

The `record` value is the complete `JobRecord.as_mapping()` mapping; the
shortened object above only illustrates the envelope. The hook writes one
response line for each record, in the same order:

```json
{"job_id": "job-1", "outputs": {"energy": {"value": 3.14}}}
```

or:

```json
{"job_id": "job-1", "error": "could not read the result"}
```

The response `job_id` must match the input record. A malformed response, a
wrong job id, an explicit error, a missing response, or a response whose output
cannot be resolved degrades that job only; the sweep continues and other jobs'
responses remain usable. One executable collector process handles all records
for that collector in the sweep.

Each output value must be exactly one of these discriminator wrappers:

| Wrapper | Result |
| --- | --- |
| `{"entry": { ... }}` | A registered entry type is reconstructed as its real record. The mapping must contain a registered string `type`; an optional `id` must match the constructed record. |
| `{"value": <JSON value>}` | A `DataRecord`. If the declared output has `ref`, the referenced property definition is loaded and the value is hard-validated with `httk-store` (which is required at collect time); without `ref`, a generated `_httk_custom_*` property definition is used. |
| `{"file": "<path>"}` | A workspace-confined `FileRecord`. The wrapper must contain exactly the `file` key, and the path must resolve to a regular file below the workspace or workdir. |

The wrapper discriminator is reserved: extra keys are rejected. On the
successful path, the Python fast path returns ordinary Python objects to the
existing assembler and has the same role validation and record assembly
semantics as these executable wrappers. Failure behavior remains deliberately
different: an exception from a registered Python collector aborts collection
iteration, while an executable collector's malformed, errored, missing, or
unresolvable response degrades only its job and lets the sweep continue.

The job-embedded declaration governs the Run (immutable facts per job). ProductLinks
come from the live registered provider's manifest and therefore apply today's
curation; if collection uses the job-pinned fallback instead, they come from
that job's own verified pinned manifest and preserve its historical curation,
not today's. If no provider or pinned manifest is reachable, no products are
emitted.

There are THREE TRUST TIERS:

1. **installed/registered** — a provider registered by a package or domain
   carries its runner and collect adapter. Installed runner references are
   trusted by the registration and distribution boundary.
2. **explicit-path consent** — a package or runner supplied by an explicit
   filesystem path is parsed and used because the caller selected that path.
   Registered directory providers execute the current source-directory hook
   bytes at collection time: this is tier two and is not digest-gated. A direct
   package path is published and its scaffold instantiate hook executes from
   the resulting pinned tree.
3. **allow-job-collector off-by-default digest-verified** — `collect` only
   loads a collect hook from a job-pinned workspace package tree when
   `allow_job_collector=True` (or the CLI flag is supplied). The tree's own
   manifest must match the job workflow id, and its full tree digest must match
   `job.json`; refusal or tampering degrades that job instead of stopping the
   sweep.

## Publication and lifecycle

`job new --workflow-dir` publishes the complete package tree into the workspace
runner store. Publication computes one tree digest, installs a read-only tree,
and records the full digest in `job.json`. Republish of identical content is an
idempotent no-op; changed content cannot replace an existing name without an
explicit replacement. The manager verifies the same digest before execution.

`publish=` is ignored for language workflows: CWL/PWD/jobflow and httk-v1 use
their installed language runners. `new_jobs` and CLI
`--input-from` campaigns prepare a language package once and instantiate it per
job. Language-produced parameter names are reserved and collisions fail
loudly. httk-v1 snapshots the complete package at preparation, so edits made
after preparation do not change later jobs; symlinks are rejected.

The usual lifecycle is instantiate, run, then collect:

```console
httk workflow job new --workspace WS --workflow-dir ./my-workflow \\
    --input-from structure structures/ \\
    --parameter kpoint_density=30.0 \\
    --placement project/screening
httk workflow run --workspace WS
httk workflow collect --workspace WS
httk workflow collect --workspace WS --into results.sqlite --id-base httk.workflow
```

`httk workflow describe TARGET [--json]` reports a registered id or alias,
runner file, or package directory without publishing it. The default collect
verb emits one `CollectedJob` summary per line; `--raw` emits `JobRecord`
records. `--allow-job-collector` enables the third trust tier above.

Python users who need persistence call `store.save(...)` themselves. `--into`
is the CLI shortcut: it opens a file-backed SQLite `SqlStore`, saves output
entries, runs, and products, and reports stored ids. Entry families and record
classes are resolved lazily from the core registry; output types may require
`httk-store` and `httk-atomistic` to be installed.

`--id-base BASE` is required with `--into`; `--id-series SERIES` defaults to
`1`. Collected edges to outputs without store ids use content ids until the
outputs are saved, after which `--into` rewrites those edges to the minted ids.

With `--into`, each job's entries, run, and products are stored as one job-level
operation. A storage failure is reported on that job's summary as
`storage_error`; other jobs may still be stored, so a sweep can partially
succeed. Inspect each JSONL line's `storage_error` before retrying or
reconciling the destination store.

See {doc}`/declarations` for declaration carriage, {doc}`/provenance` for the
tree-pinned provenance handoff, {doc}`/collecting` for collect-hook and
fallback behavior, and {doc}`workflow_cli` for the complete command reference.
