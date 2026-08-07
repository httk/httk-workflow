# Workflow packages

*For workflow authors who want one portable directory to describe, instantiate,
run, and collect a workflow.*

A workflow package is a directory whose `workflow.toml` is the strictly
*httk*-owned glue around a runner. It is not an embedded OPTIMADE declaration
language: the manifest can generate the workflow declaration, or point to an
externally authored declaration that it validates and carries.

## Package layout

The smallest useful package has an executable `run` entry and a manifest:

```text
my-workflow/
├── workflow.toml
├── run
├── instantiate.py       # optional
├── postprocess.py        # optional
└── support/              # any regular support files
```

`run` receives the normal runner environment and publishes the outcome protocol
used by the manager. `instantiate.py` and `postprocess.py` are optional Python
hooks. A package may contain other regular files needed by its entry or hooks;
manifest members must be relative regular files inside the package, and hook
members must end in `.py`. Symlinks, special files, absolute names, and `..`
members are refused.

Resolve or register a package with the Python API:

```python
from httk.workflow.packages import load_workflow_package

provider = load_workflow_package("my-workflow")  # registers id and alias
```

The command line can use the directory without registering it:

```console
httk workflow describe ./my-workflow
httk workflow job new WS --workflow-dir ./my-workflow --input structure=POSCAR
```

## Runner realizations

`[workflow.runner]` selects one of three forms. The executable form is the
ordinary package runner. The language forms delegate instantiate, run, and
default collection to the registered language realization.

| Form | Manifest selector | Required/allowed members | Runner contract |
| --- | --- | --- | --- |
| executable entry | no `language` | `entry`, `steps`, `initial_step`, `data_mode`, `workdir_mode` | package `run` plus the declared step set |
| document language | `language = "cwl"` or `"pwd"` and `document` | language keys only; `port` is allowed on document inputs/outputs | installed `cwl_runner.py` or `pwd_runner.py` |
| httk-v1 | `language = "httk-v1"` and no `document` | `taskset`, `attempts`; no mode keys | package snapshot under executor `httk-v1`, with `ht_steps` or `ht_run` |

For language forms, `entry`, `steps`, and `initial_step` are forbidden because
the language supplies built-in steps. `[workflow.instantiate]` is forbidden
because language inputs are hook-consumed. `destination` is forbidden on
CWL/PWD and on httk-v1 inputs; an omitted v1 destination is an
`ht.instantiate.py` global. Language workflows may declare
`[workflow.postprocess]` to override the default. CWL and PWD have defaults;
httk-v1 has none and normally declares a hook.

Language manifests cannot set `data_mode` or `workdir_mode` for httk-v1: they
are forced to `none` and persistent `ht.run.current`. Unknown language keys are
errors.

## `workflow.toml` reference

This is the complete manifest vocabulary validated by
`parse_workflow_manifest`. Unknown keys at any level are errors. TOML syntax,
member containment, nonempty names, aliases, runner modes, hook members,
parameter destinations, input defaults, and output relationships are validated
before a provider is returned.

### `[workflow]`

| Key | Required | Meaning |
| --- | --- | --- |
| `id` | yes | Nonempty workflow id with no whitespace. This is the registry key, the `job.json` workflow, and the postprocess dispatch key. |
| `alias` | no | Alternate name matching `[a-z0-9._-]+`. |
| `description` | no | Human-readable summary and generated declaration description. |
| `declaration_uri` | no | String `$id` for the generated or external workflow declaration. |
| `declaration_file` | no | Relative regular-file member containing an externally authored OPTIMADE-format workflow declaration JSON. |

```toml
[workflow]
id = "example.relax"
alias = "relax"
description = "Relax one structure."
declaration_uri = "https://example.org/workflows/relax"
# declaration_file = "declaration.json"
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

### `[workflow.runner]`: language vocabulary

| Key | CWL/PWD | httk-v1 | Meaning |
| --- | --- | --- | --- |
| `language` | required: `"cwl"` or `"pwd"` | required: `"httk-v1"` | Select the realization. |
| `document` | required, relative regular member | forbidden | Language document member. |
| `port` | optional on an input/output table | forbidden | Alias a manifest name to a document port. |
| `modules` | PWD only, list of relative `.py` members | forbidden | Package Python modules to stage. |
| `module_path` | PWD only, list of import roots | forbidden | Additional PWD import roots. |
| `allowed_modules` | PWD only, list of module prefixes | forbidden | PWD import allowlist. |
| `taskset` | forbidden | label, default `"default"` | v1 claim pool. |
| `attempts` | forbidden | integer, default `10` | v1 retry budget. |
| `entry`, `steps`, `initial_step` | forbidden | forbidden | Built-in language steps. |
| `data_mode`, `workdir_mode` | allowed only in executable form | forbidden | v1 forces `none`/persistent. |

For a document language, each effective input and output port must exist in the
document and may occur only once. `port` defaults to the manifest name.

### Hook tables

Each hook table has exactly one key, `file`, naming a relative `.py` regular
file member. Presence of `[workflow.instantiate]` declares an instantiate hook;
presence of `[workflow.postprocess]` declares a postprocess hook.

```toml
[workflow.instantiate]
file = "instantiate.py"

[workflow.postprocess]
file = "postprocess.py"
```

### `[workflow.inputs.<NAME>]`

Every input table accepts these keys:

| Key | Meaning |
| --- | --- |
| `destination` | Optional payload-relative destination for executable runners. Omit it when `[workflow.instantiate]` consumes the value. Language runners forbid it because the language hook consumes every input; for httk-v1 an omitted destination is an `ht.instantiate.py` global. |
| `description` | Optional input description. |
| `entry_type` | Optional declaration entry type. |
| `ref` | Optional declaration reference. |
| `role` | Optional declaration role; defaults to the input key. |

```toml
[workflow.inputs.structure]
destination = "POSCAR"
entry_type = "structures"
ref = "https://example.org/types/structure"
description = "The starting structure."
role = "initial_structure"

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

The hook contracts are deliberately small:

```python
def instantiate(context):
    # context.payload, context.inputs, context.parameters, context.tag
    ...


def postprocess(record):
    # return {"output_role": entry_object, ...}
    ...
```

An instantiate hook runs during scaffolding and may write the payload or update
parameters. A postprocess hook runs during `collect`; it returns role-keyed outputs.
The framework validates those roles, derives unfulfilled roles, overlays output
edges onto the `Run`, and emits `ProductLink` values from manifest/provider
`product_of`. A direct
package path's instantiate hook and the job-pinned postprocess fallback execute
from the published, digest-pinned tree; registered-directory postprocessors
instead execute current source bytes by explicit registration consent.

The job-embedded declaration governs the Run (immutable facts per job). ProductLinks
come from the live registered provider's manifest and therefore apply today's
curation; if collection uses the job-pinned fallback instead, they come from
that job's own verified pinned manifest and preserve its historical curation,
not today's. If no provider or pinned manifest is reachable, no products are
emitted.

There are THREE TRUST TIERS:

1. **installed/registered** — a provider registered by a package or domain
   carries its runner and postprocess adapter. Installed runner references are
   trusted by the registration and distribution boundary.
2. **explicit-path consent** — a package or runner supplied by an explicit
   filesystem path is parsed and used because the caller selected that path.
   Registered directory providers execute the current source-directory hook
   bytes at collection time: this is tier two and is not digest-gated. A direct
   package path is published and its scaffold instantiate hook executes from
   the resulting pinned tree.
3. **allow-job-postprocessor off-by-default digest-verified** — `collect` only
   loads a postprocess hook from a job-pinned workspace package tree when
   `allow_job_postprocessor=True` (or the CLI flag is supplied). The tree's own
   manifest must match the job workflow id, and its full tree digest must match
   `job.json`; refusal or tampering degrades that job instead of stopping the
   sweep.

## Publication and lifecycle

`job new --workflow-dir` publishes the complete package tree into the workspace
runner store. Publication computes one tree digest, installs a read-only tree,
and records the full digest in `job.json`. Republish of identical content is an
idempotent no-op; changed content cannot replace an existing name without an
explicit replacement. The manager verifies the same digest before execution.

`publish=` is ignored for language workflows: CWL/PWD use their installed
runner and httk-v1 uses its payload runner and executor. `new_jobs` and CLI
`--input-from` campaigns prepare a language package once and instantiate it per
job. Language-produced parameter names are reserved and collisions fail
loudly. httk-v1 snapshots the complete package at preparation, so edits made
after preparation do not change later jobs; symlinks are rejected.

The usual lifecycle is instantiate, run, then collect:

```console
httk workflow job new WS --workflow-dir ./my-workflow \\
    --input-from structure structures/ \\
    --parameter kpoint_density=30.0 \\
    --placement project/screening
httk workflow run WS
httk workflow collect WS
httk workflow collect WS --into results.sqlite
```

`httk workflow describe TARGET [--json]` reports a registered id or alias,
runner file, or package directory without publishing it. The default collect
verb emits one `CollectedJob` summary per line; `--raw` emits `JobRecord`
records. `--allow-job-postprocessor` enables the third trust tier above.

Python users who need persistence call `store.save(...)` themselves. `--into`
is the CLI shortcut: it opens a file-backed SQLite `SqlStore`, saves output
entries, runs, and products, and reports stored ids. Entry families and record
classes are resolved lazily from the core registry; output types may require
`httk-data` and `httk-atomistic` to be installed.

With `--into`, each job's entries, run, and products are stored as one job-level
operation. A storage failure is reported on that job's summary as
`storage_error`; other jobs may still be stored, so a sweep can partially
succeed. Inspect each JSONL line's `storage_error` before retrying or
reconciling the destination store.

See {doc}`declarations` for declaration carriage, {doc}`provenance` for the
tree-pinned provenance handoff, {doc}`collecting` for postprocessing and
fallback behavior, and {doc}`workflow_cli` for the complete command reference.
