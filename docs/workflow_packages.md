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
httk workflow job new WS --workflow-dir ./my-workflow --parameter structure=POSCAR
```

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

### `[workflow.runner]`

This table is required. `steps` is a nonempty list of strings. If `initial_step`
is omitted, `start` is selected when present; a one-step runner uses that one
step; otherwise `initial_step` is required and must name a listed step.

| Key | Required/default | Meaning |
| --- | --- | --- |
| `entry` | `"run"` | Reserved relative entry member. It must be named `run`; custom entry names are not yet supported. |
| `initial_step` | `"start"` when present; otherwise the sole step | First step for a scaffolded job. |
| `steps` | required | Nonempty runner step list. |
| `data_mode` | `"none"` | `"none"` or `"transactional"`. |
| `workdir_mode` | `"persistent"` | `"persistent"` or `"isolated"`. |

```toml
[workflow.runner]
# Reserved: the manager's tree entry point must be named "run".
entry = "run"
initial_step = "start"
steps = ["start", "finish"]
data_mode = "transactional"
workdir_mode = "persistent"
```

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

### `[workflow.parameters.<NAME>]`

Every parameter table accepts these keys:

| Key | Meaning |
| --- | --- |
| `destination` | Optional payload-relative destination. Omit it when the instantiate hook consumes the value; omission requires `[workflow.instantiate]`. Existing parameter and payload-relative-name validation applies. |
| `description` | Optional parameter description. |
| `entry_type` | Optional declaration entry type. |
| `ref` | Optional declaration reference. |
| `role` | Optional declaration role; defaults to the parameter key. |

```toml
[workflow.parameters.structure]
destination = "POSCAR"
entry_type = "structures"
ref = "https://example.org/types/structure"
description = "The starting structure."
role = "initial_structure"

[workflow.parameters.settings]
description = "Values consumed by instantiate.py."
role = "settings"
```

### `[workflow.inputs.<NAME>]`

Each input accepts `type`, `description`, and `default`. `type`, when present,
must be one of `"string"`, `"number"`, `"integer"`, `"boolean"`, `"array"`,
or `"object"`; a supplied default must have that matching JSON/TOML type.

```toml
[workflow.inputs.kpoint_density]
type = "number"
default = 30.0
description = "Sampling density."
```

### `[workflow.outputs.<NAME>]`

`entry_type` is required. The other accepted keys are `ref`, `description`,
`product_of`, and `role`. `role` defaults to the output key. `product_of`, when
present, must name an entry-typed `[workflow.parameters]` key; generated declarations resolve
that parameter key to its declaration `role`.

```toml
[workflow.outputs.relaxed]
entry_type = "structures"
ref = "https://example.org/types/structure"
description = "The relaxed structure."
product_of = "structure"
role = "relaxed_structure"
```

### Declarations

Without `declaration_file`, `workflow_declaration_from_manifest(provider)`
generates an OPTIMADE-format document with optional `$id` and `description`,
entry-typed parameter entries, and output entries with `product_of` role names.
It becomes `provider.declarations["workflow"]` and is embedded in `job.json`.

With `declaration_file`, the JSON document is loaded and embedded verbatim after
validation. Its `$id` must equal `declaration_uri` when both are supplied. Every
external parameter and output role maps must exactly cover the manifest's
entry-typed parameters and outputs, and output `entry_type` and `product_of`
values must agree with the manifest. Every product source must be emitted by an
entry-typed parameter. The declaration remains the authoritative OPTIMADE
document; the manifest remains the authoritative strictly httk-owned package glue.

## Hooks and trust

The hook contracts are deliberately small:

```python
def instantiate(context):
    # context.payload, context.parameters, context.inputs, context.tag
    ...


def postprocess(record):
    # return {"output_role": entry_object, ...}
    ...
```

An instantiate hook runs during scaffolding and may write the payload or update
inputs. A postprocess hook runs during `collect`; it returns role-keyed outputs.
The framework validates those roles, derives unfulfilled roles, overlays output
edges onto the `Run`, and emits `ProductLink` values from `product_of`. A direct
package path's instantiate hook and the job-pinned postprocess fallback execute
from the published, digest-pinned tree; registered-directory postprocessors
instead execute current source bytes by explicit registration consent.

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

The usual lifecycle is instantiate, run, then collect:

```console
httk workflow job new WS --workflow-dir ./my-workflow \\
    --parameter-from structure structures/ \\
    --input kpoint_density=30.0 \\
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
