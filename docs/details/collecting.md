# Collecting results

Collecting is the read-only counterpart of running work. The low-level
`job_records()` iterator reads each stopped job into a `JobRecord`: where its
files are, what produced them and what happened on the way. The
framework-level `collect()` passes each record to its registered workflow's
collector and yields a `CollectedJob` with role-keyed outputs, provenance,
products and any unfulfilled roles.

## Collected jobs

### Degraded and run-only jobs

A job that cannot be collected at all yields a *degraded* `CollectedJob`.
`missing_collector` explains why, `outputs` is empty and `unfulfilled` names
every declared output role, so it cannot be mistaken for a complete
collection that declares no outputs.

A workflow with nothing to collect, whose registered provider has no
collector and declares no outputs, is not degraded. Its `CollectedJob` is
complete, with `run_only` set, no outputs and the job's run as its only
product. Orchestrating workflows that only spawn or call others, and the small
steps they call, are typically like this.

Every `CollectedJob` also carries `child_runs`: one
`(label, run source id)` pair per child the job spawned or called, read from
the published `children` of its `state.json`.

When an output role is a single file-valued result, its run edge points to a
standard `files` entry (`type = "files"`). File lists remain values within
their role record.

### Runs and product curation

The job-embedded declaration governs the Run (the job's immutable facts). The
Run carries the executing system's job identity in `source_id`; *httk-store*
mints its store-owned `immutable_id`.

Product curation (`product_of` on an output role) comes from the live
registered provider's manifest, so today's curation applies. Under the
job-pinned fallback it comes from the job's verified pinned manifest, keeping
its historical curation. With neither reachable, no products are emitted.

A data-record output whose role is `product_of` another role gets that source
as a `product_of` edge on the record itself. The edge is a `StrongLink` with
the same `(label, entry_type, entry_id)` scheme as run edges, so it is record
content and searchable as `record.links.product_of == structure`. Every
curation is also emitted as a `ProductLink`.

Introducing `product_of` as record content changed every `DataRecord`
content id and the record layout. A store created before that change and
holding data records must be rebuilt (`--into` a new file), not reopened.

## Records as the layering boundary

`JobRecord` is the layering boundary of *httk₂*. *httk-workflow* has no
database dependency: it produces records and *httk-store* consumes them. A
consumer reads results like this, and nothing in *httk-workflow* knows what
`store` or `load_vasp` are:

```python
from httk.workflow import Workspace, job_records

workspace = Workspace("workflow-workspace")
for record in job_records(workspace):
    store.save(load_vasp(record))
```

Every member of a record derives from the job directory alone: the immutable
`job.json`, the owner-written `state.json` and the owner run log
`logs/runlog.jsonl`. A record never says anything the workspace does not. It
identifies its job by `workspace_id` and `job_id`; a job directory moves with
every state change, so `payload_path` is only where the job was when it was
read.

## What a record guarantees

### The executed code is pinned

A record carries the immutable job digest and, in `runner_provenance`, the
installed workflow the job ran: its id and the SHA-256 of its installed package
tree, as the manager recorded them at launch (`workflow_pin` in `state.json`).
A stored result therefore names the software that produced it.

### Damage is reported

A job whose run log, `state.json` or observed declarations are damaged is
still collected with whatever remains readable, and `provenance.gaps` is set to
`true`, so a result does not vanish because part of its history was lost. Only
a job whose `job.json` cannot be read is skipped, with a module-logger message,
because a record stands for the *validated* job behind a result.

### Collection scales by iteration

`job_records` is a lazy iterator over one listing of the requested state
directories, and each record reads only its own job's `job.json`, `state.json`
and run log. A job that moves between listing and reading is skipped like an
unreadable one. {doc}`/details/benchmarks` gives a measured local reference
point. Partition larger campaigns across workspaces and collect them one
partition at a time, not as one in-memory result array.

## Members

| Member | Meaning |
| --- | --- |
| `workspace`, `workspace_id` | the absolute workspace root and its identity |
| `job_id`, `job_key` | the job UUID and the complete `tag--uuid` key |
| `job` | the validated job definition, including its `digest`, `workflow` (the installed workflow's id) and `workflow_name` |
| `runner_provenance` | `{"id", "tree_sha256"}` of the installed workflow the job last launched with, or `null` |
| `state` | the kind this job stopped in: `succeeded`, `failed`, `cancelled`, or `paused` |
| `failure` | the unified failure record — `code`, `message`, optional `details`, `retryable` — or `null` |
| `placement` | the placement subtree this job sits in |
| `payload_path`, `workdir_path`, `data_path` | workspace-relative paths to the job directory, its persistent workdir `run/`, and its `data/` directory when the job committed data |
| `provenance` | `{"activations": [...], "gaps": bool}` — the timeline derived from the owner run log, oldest first |
| `runner_steps` | the step set the runner declared, when one was ever recorded |
| `runner_description` | reserved: a runner's own `--describe` output attaches here in a later phase, `null` today |
| `children` | the labeled children this job spawned, keyed by spawn label |
| `declarations` | the workflow declarations of this job, keyed by name: `{"declared": ..., "observed": ...}` per name, both carried verbatim |

`payload_path`, `workdir_path` and `data_path` are workspace-relative, so a
stored record survives moving the workspace. The properties `record.payload`,
`record.workdir` and `record.data` resolve them to absolute `Path` objects
against the record's workspace.

### Parameters and result files

A collect hook reads parameters and finds result files through the record:

- `record.parameter(name, default)` returns one member of the job's
  `parameters`. Without a default, a missing name raises `KeyError`, like
  `Attempt.parameter`.
- `record.result_file(name, data_prefix=..., published=...)` returns the
  existing file: below the committed data for a job that has a `data/`
  directory (at `published`, default `name`, below `data_prefix`), otherwise
  in the persistent workdir. A compressed copy (`OUTCAR.gz`) is found too. A
  missing file raises `ValueError`; a job with committed data never falls back
  to the workdir.

```python
from httk.codes.vasp.collect import read_total_energy


def collect(record):
    prefix = record.parameter("data_prefix", "vasp") or ""
    return {"total_energy": read_total_energy(record.result_file("OUTCAR", data_prefix=prefix))}
```

The reading helper comes from the code package, here *httk-workflow-vasp*.

### Provenance, children and declarations

The timeline regroups the owner run log: each `attempt_started` event opens
an attempt, and a change of activation opens an activation. Each entry of
`provenance.activations` carries `activation_id`, `activation_ordinal`, the
`step` it ran, the `reason` it started (known for the current activation, from
`state.json`) and its `attempts`. Each attempt carries `attempt_id`,
`ordinal`, the `owner_id` that launched it, the `claimed_at`, `started_at` and
`finished_at` timestamps, the `outcome_action` it published and its `failure`
(the code from the run log; the full failure for the last attempt). The
runner's own evidence log `.httk-job/runlog.jsonl` is not part of the record;
`httk job log` shows both.

`children` maps each spawn label to `job_id`, `job_key` and `kind`, the state
the parent's join last observed the child in (`null` when no join observed
it). A campaign thus collects as a tree of named children, each collected in
its own right.

`declarations` reports every declaration name either source knows:

- `declared` is the document `job.json` carried, pinned by the immutable job
  digest.
- `observed` is the runtime-refined document the job wrote below
  `.httk-job/declarations/`.

Either is `null` when its source has nothing. They are not merged, because
reconciling them needs the document's own vocabulary, which is the consumer's
job. An unreadable observed document is reported as `null` and sets
`provenance.gaps`. `job` does not repeat the declared documents. See
{doc}`/details/declarations`.

The observed `environment` declaration is the exact resolved value and source
snapshot that drove the run (format `httk-workflow-environment-resolution`
version 2). The `provenance`
declaration becomes a stored `httk.core.Run`; see {doc}`/details/provenance`.

## Selecting records

```python
records = job_records(
    workspace,
    states=("succeeded", "failed"),
    placement="project/campaign",
)
```

`states` defaults to `("succeeded",)` and accepts only stopped-job kinds:
`succeeded`, `failed`, `cancelled` and `paused`; other values are refused by
name. `placement` restricts collection to jobs at or below one placement, as
`httk job list --placement` does.

## From the command line

```console
httk collect --workspace WORKSPACE
httk collect --workspace WORKSPACE --state succeeded --state failed
httk collect --workspace WORKSPACE --placement project/campaign --raw
```

`httk collect` also takes a PATH. A workspace root or a registered workspace
name collects that workspace; any other directory is walked as a tree of
finished calculations (see [Recognized calculations](#recognized-calculations)
below).

### Output forms

The workspace is attached read-only. By default `collect` prints one
`CollectedJob` summary per line (format `httk-workflow-collected`). `--raw`
prints one `JobRecord` per line instead:

```console
$ httk collect --workspace workflow-workspace --raw | head -1
{"children":{},"data_path":null,"declarations":{"workflow":{"declared":{"inputs":[],"outputs":[]},
 "observed":null}},"failure":null,"format":"httk-workflow-collect","format_version":3,
 "job":{"claim":{"pool":"default","required_capabilities":[]},"digest":"b5e9…",
 "environment":{"declared":{},"overrides":{}},"id":"6cde…","initial_step":"only",
 "job_key":"single--6cde…","name":"run: single","parameters":{},"parent":null,"priority":500,
 "resources":{},"retry_policy":{…},"tag":"single","workflow":"adhoc:run@e977…",
 "workflow_name":"run"},"job_id":"6cde…","job_key":"single--6cde…",
 "payload_path":"jobs/succeeded/project/single/single--6cde…~p500~uluonuoo5prwmbss",
 "placement":"project/single","provenance":{"activations":[{"activation_id":"f5a3…",
 "activation_ordinal":1,"attempts":[{"attempt_id":"3dfa…","claimed_at":"…","failure":null,
 "finished_at":"…","ordinal":1,"outcome_action":"succeed","owner_id":"ab79…",
 "started_at":"…"}],"reason":"initial","step":"only"}],"gaps":false},
 "runner_description":null,"runner_provenance":{"id":"adhoc:run@e977…","tree_sha256":"9ec2…"},
 "runner_steps":["only"],"state":"succeeded",
 "workdir_path":"jobs/succeeded/project/single/single--6cde…~p500~uluonuoo5prwmbss/run",
 "workspace":"/…/workflow-workspace","workspace_id":"656c…"}
```

Each `--raw` line is exactly `JobRecord.as_mapping()`, and
`JobRecord.from_mapping()` rebuilds the record, so the stream can be written
to a file, shipped and read back by the storing process.

Jobs that ran on a remote are collected the same way once home:
`httk job transfer --job JOB REMOTE:NAME default` moves them into the local
default workspace in their terminal state, and collect cannot tell them from
local jobs. See {doc}`/details/workflow_cli`.

### Summary line and exit codes

Every `collect` invocation ends with one JSONL summary line:

```text
{"format":"httk-workflow-collect-summary","format_version":2,
 "collected":N,"degraded":N,"unfulfilled_roles":N,
 "storage_errors":N,"skipped_unreadable":N}
```

A calculation-tree sweep adds `"unclaimed":N`, the directories a collector
declined. A sweep with `--into` adds `"revised":N`, the jobs whose stored
entries gained a revision.

- `collected`: jobs collected without degradation.
- `degraded`: degraded jobs.
- `unfulfilled_roles`: declared roles left unfulfilled, summed over all jobs.
- `storage_errors`: jobs an `--into` store could not persist.
- `skipped_unreadable`: jobs dropped because their `job.json` could not be
  read. They never appear as records; this count is the only place they
  surface.

The command exits `0` only when `degraded`, `storage_errors` and
`skipped_unreadable` are all zero. Unfulfilled roles alone do not fail the
sweep; a partially fulfilled job is a normal result. As a gate, a nonzero exit
means a job could not be collected, stored or read, and the counts say which.

### Per-job triage

Each summary line carries its own triage fields:

- `missing_collector` explains a degradation.
- `products_unlinked` lists `product_of` links skipped because the output was
  produced but its curated source edge is absent from the observed provenance,
  as `"<role> -> <source> (source edge absent in observed provenance)"`. A
  product whose own output role went unfulfilled is reported only through
  `unfulfilled`.
- `collector_exit_status` reports an executable collector that answered every
  record but still exited nonzero.

## Storing results with `--into`

With `--into PATH --id-base BASE`, each collected job's entries, run and
product links are saved into a file-backed SQLite store, and its report gains
`"stored": {...}`.

### Degraded and failed jobs

A degraded job stores nothing: its report carries
`"stored": null, "skipped": "degraded"` and no empty `Run` is written. A job
whose entries cannot be stored keeps a `"storage_error"` and fails the exit
code.

### Run-only jobs and child runs

A `run_only` job stores its run by default, so an orchestrating parent and
every job it spawned or called each leave one `runs` entry (served as
`_httk_runs`) naming their own workflow declaration and, for a commit-pinned
git workflow, its definition. `--no-bare-runs` opts out, and such jobs then
report `"stored": null, "skipped": "run-only"`. Their report lines carry
`"run_only": true`, and a job that spawned children lists them as
`"children": [{"label", "run_source_id"}]`.

A parent's stored run gains one `has_artifact` edge of type `runs` to the run
of each child it spawned or called, including detached children (detaching
changes where a child may move, not who created it). The edge is labelled by
the spawn label, or by the child's run source id if that label is taken on
the artifact side. Children are stored before parents so their runs exist. A
child collected but not stored in this sweep (degraded or skipped) is left
out, as is one never collected; one stored by an earlier sweep is linked.

### Revisions

Each job has one run entry. A changed run on re-collection, such as a parent
whose children were collected since, is stored as a revision of the same entry
(same entry id, one lineage); unchanged runs deduplicate. With an id ledger,
a job's record whose content changed since the last sweep likewise keeps its
ledger id and gains a revision. The report line then carries
`"revised": true`.

### Entry ids and the id ledger

`--id-base` is required with `--into` and names the dot-separated namespace
for minted entry ids. `--id-series` selects the campaign series and defaults
to `1`.

By default a sealed *id ledger* allocates these ids so they stay stable across
rebuilds; see {doc}`/details/stable_ids`. It lives beside the store at
`<into>.ids.sqlite` (relocate it with `--id-ledger PATH`), is created and
signed on first use, and is announced prominently because it should be kept
and committed with the store.

`--no-id-ledger` opts out, and a sweep without a resolvable workspace signing
key has no ledger. Either way the store mints ids that are not stable across
rebuilds, with one warning.

An output that already carries an assigned public id is not renumbered.
Content the store deduplicates onto one row is recorded as one id, with the
other keys aliased to it. A job whose identity is not stable (a v1 tree with
no manifest) is store-minted with a warning instead of pinned to a stale key.

### Resolving references

Before persistence, edges to output records that have no store id yet use
those records' content ids. `--into` then stores in two passes:

1. It commits all job outputs while building one sweep-wide map from content
   id to entry id.
2. It rewrites and commits the runs and product links.

A reference not produced in the sweep first keeps an already stored public id,
then resolves against the destination store by content id. An unknown 64-hex
content id makes that job a storage error: its outputs stay saved, but its run
and products are not written. Other loose external references are kept
unchanged.

### Re-collecting into a store

Re-collection is stateless and safe: `collect --into` reads the workspace
afresh and writes what it finds. Re-storing a stored job deduplicates on its
stable entry, run and product ids, so a repeated collect changes nothing.

A store built under a different entry-family layout is not migrated in place;
point `--into` at a new store file when the layout changes. Reusing a store
whose entry-type layout does not match the sweep fails fast. The error names
the store path, the entry types the sweep needs and the layout difference, and
ends with `Collect into a new store file.`

## Workflow collect hooks

`collect()` is the workflow-owned collection layer, and the collect hook a
workflow package provides is the workflow-owned substep of collecting. For each
record, `collect()`:

1. resolves the record's workflow id through `workflow_provider()`;
2. calls that provider's collector, a callable or a lazy `module:function`;
3. validates role names against the declared `outputs` in the job's embedded
   workflow declaration;
4. assembles the `Run`, the data records' `product_of` edges and the
   `ProductLink` values.

Product curation comes from the provider manifest, as described under
[Runs and product curation](#runs-and-product-curation); the embedded
declaration supplies only the immutable Run facts. Old jobs without that
declaration use the currently registered provider declaration, so their roles
are interpreted live, not historically.

### Missing collectors

- A workflow without a provider, or whose provider declares outputs but has no
  collector, yields a degraded `CollectedJob` with `missing_collector` set.
- A provider with neither a collector nor outputs yields a `run_only` job (see
  above).
- A job whose provider is not registered on the collecting machine stays
  degraded, not run-only, because a collector may exist for it elsewhere.
- With `--allow-job-collector`, a job is decided by its verified installed
  package instead, so an unregistered package with nothing to collect is
  `run_only` too.
- A job of a runner file of your own runs an ad hoc installed package whose
  generated manifest declares no collector and no outputs, so it is degraded
  without `--allow-job-collector` and `run_only` with it.

`--allow-job-collector` lets collection load the collect hook of the job's
installed workflow package in the workspace, after checking that its tree
digest still matches the job's `runner_provenance` (a workflow reinstalled or
modified since the job ran is refused); refusals degrade only that job. See
{doc}`/details/workflow_packages` for the trust tiers and package hook
contract.

### Executable hooks

An executable `[workflow.collect]` member runs once per matching collection
window from its package tree. Under the job-pinned fallback that tree is the
job's installed package, digest-checked against the pin. A registered-directory
provider is the explicit-consent exception that runs from current source.

The first stdin line is
`{"format":"httk-workflow-collect-stream","format_version":2}`, and each
following line is `{"record": <JobRecord mapping>}`. The hook returns one
JSONL response per record, in order:
`{"job_id": ..., "outputs": {role: value}}` or
`{"job_id": ..., "error": ...}`. Each output value uses exactly one wrapper:

- `{"entry": {...}}` for a registered entry record;
- `{"value": <json>}` for a `DataRecord`;
- exactly `{"file": "<path>"}` for a workspace-confined `FileRecord`.

A declared output `ref` makes *httk-store* validation hard-required at collect
time. Without a `ref`, the framework creates a `_httk_custom_*` property
definition.

Response lines are drained as binary newline-delimited data and decoded as
UTF-8 one line at a time. Limits are enforced while draining: 1 MiB per
response line, 64 KiB per stderr line and 1 MiB of total stderr.

- A malformed, non-UTF-8, errored, missing, wrong-id or unresolvable response
  degrades only its own job, and the sweep continues.
- A limit breach terminates and degrades the affected executable collector
  group.
- Surplus blank response lines are ignored; a nonblank surplus response line
  degrades the whole group.

Python `.py` hooks run in process with the same assembled-output semantics;
an ordinary per-job exception degrades that job.

### Format fallback and degradation

For a job of a compat format (`workflow_realization = "language"`), provider
dispatch is followed by the job's own `workflow_language` parameter. A
provider-less CWL, PWD or jobflow job then uses the format's default
collector. It reads the output document from the workdir or committed data
tree, maps ports to declared roles and creates `DataRecord` objects. CWL
`File` values must stay inside the workspace, workdir or data tree and are
recorded as a file descriptor and sha256. Jobflow reads
`jobflow-outputs.json`.

A package with a custom hook records `workflow_collect = "package"`;
provider-less collection of it degrades with a registration hint instead of
running the format default. httk-v1 has no default: such a job degrades with
a message to declare `[workflow.collect]` in a package manifest. The
`allow_job_collector` fallback is tried only after this format fallback, and
only with a matching digest.

Any per-job collector, load or assembly failure degrades that job without
stopping the sweep; `fail_fast=True` (`--fail-fast`) stops at the first such
job instead. Interrupts, missing Python dependencies and unavailable
definition-validation dependencies still propagate. An executable hook that
cannot be launched (for example, from a damaged pinned job tree) degrades its
batch, while an unreadable output file degrades only its job. Neither stops
other hooks from running.

### Batching and streaming

`collect(..., batch_size=64)` consumes records in bounded windows. A matching
executable hook runs once per window, and results stay in workspace scan
order. `fail_fast=True` (`--fail-fast`) forces the window size to one, which
disables executable batching and starts one collector process per job.
`batch_size` (`--batch-size`) must still be positive but is otherwise ignored
in that mode.

Normal CLI output is JSONL, streamed as each window completes. `--degraded`
filters the output lines to degraded jobs but keeps whole-sweep summary
counts. `--into` retains the complete sweep so its second storage pass can
resolve cross-job provenance; use ordinary streaming collection when that
memory cost is too high.

For the distinction between declared entry-typed inputs and opaque
implementation parameters, see {doc}`/details/workflow_packages` and
{doc}`/details/declarations`.

## Recognized calculations

A tree of finished calculations that did not run through a workspace, such as
a directory of VASP runs or Quantum ESPRESSO outputs, is collected by walking
it:

```console
httk collect calculations/ --dry-run
httk collect calculations/ --into results.sqlite --id-base mydb
```

### Collector packages

Each directory is offered to the *recognized-calculation collectors*: those
that code packages register (see {doc}`/code_support`) and any given with
`--collector DIR`. A collector is a workflow package with a
`[workflow.recognize]` table, a `recognize.py` hook, a `collect.py` hook and
no runner. It is collected, never run.

```toml
[workflow]
name = "mycode.calculation"
description = "a finished mycode run"

[workflow.recognize]
file = "recognize.py"
priority = 10
requires = ["INPUT", "*.out"]

[workflow.inputs.structure]
role = "initial_structure"
entry_type = "structures"

[workflow.outputs.total_energy]
role = "total_energy"
entry_type = "records"
ref = "https://schemas.httk.org/defs/v0.1/properties/core/total_energy"
product_of = "initial_structure"

[workflow.collect]
file = "collect.py"
```

```python
# recognize.py
from httk.workflow.calculations import content_digest
from httk.workflow.hookapi import Claim, Unclaimed


def recognize(directory):
    outputs = list(directory.glob("*.out"))
    if len(outputs) > 1:
        return Unclaimed("several outputs")
    return Claim(content_digest(directory, ["INPUT"]))
```

`collect.py` is an ordinary collect hook; `record.result_file(name)` also
finds compressed copies such as `OUTCAR.gz`. Besides declared output roles it
may return declared input roles (here the starting structure), which become
the run's input edges.

### Recognition

`requires` lists cheap markers: exact basenames or `*.ext` globs, matched
case-insensitively against the directory's files with any compression suffix
stripped. Content lookup is exact: `content_digest`, `record.result_file` and
`existing_file` find files by their exact names, and only the compression
suffix may vary.

Only a collector whose markers all match has its `recognize(directory)` hook
called. The hook returns:

- a `Claim`;
- `Unclaimed` when the directory is this code's but cannot be collected; it is
  reported with the reason and counted as unclaimed;
- `None` when the directory is not this code's.

A hook that raises declines the directory, except that an `ImportError` (a
missing dependency) stops the sweep. When several collectors claim a
directory, the highest `priority` wins and the others are reported as also
matching. A tie at the highest priority is an error unless `--prefer NAME`
(repeatable) names one of them.

### Identity

The collector chooses the claim's identity from the calculation's content,
typically `content_digest` over its input files, so the stored run is
`<collector name>:<identity>` wherever the directory is. `content_digest`
digests decompressed contents, and an absent file differs from an empty one.
An in-place rerun with unchanged inputs is the same calculation and stores a
revision; changed inputs (`cp CONTCAR POSCAR` and rerun) make a new
calculation.

### Duplicates and collisions

Within one sweep, two directories claiming the same identity with identical
marker files are copies, and the second is skipped. With different marker
files they collide, and the sweep stops and names both directories.
`--exclude PATTERN` (a root-relative glob, repeatable) skips one of them.

`--dry-run` prints how every directory would be claimed, as
`httk-collect-claim` lines, without collecting anything. When collecting, each
calculation's summary or report line carries `"directory"`, its path relative
to the swept root, because its `job_id` is only the identity digest. The walk
does not visit directories starting with `.` or symlinked directories, and it
collects a nested workspace as a workspace instead of walking it.

### Multi-directory calculations

A calculation spread over several directories, such as a phonon calculation
whose displacements are separate VASP runs, is claimed in its top directory
with `Claim(identity, consumes=("disp-001", ...))`. Their own collectors still
collect the consumed subdirectories. The claiming calculation is collected
after them, and its run links to their runs as child runs (`has_artifact`
edges of type `runs`, labelled by the relative path).

A calculation with a failed part is incomplete. A consumed directory that is
missing, excluded or not claimed, or whose own collection degraded, degrades
the claiming calculation. The degraded part is still reported on its own line.
`--dry-run` lists the consumed directories on the claiming line.

### Python API

```python
from httk.workflow import claims, collect_tree, store_collected

for claim in claims("calculations", collectors=["my-collector"]):
    print(claim.directory, claim.kind, claim.collector)

items = list(collect_tree("calculations", collectors=["my-collector"]))
reports = store_collected(items, "results.sqlite", id_base="mydb")
```

### Typed records and store upgrades

`store_collected` stores each calculation's inputs like its outputs. A
`DataRecord` whose property definition has a typed record kind is stored as
that kind, served and filterable under its property name: the core total
energy as a `TotalEnergyRecord` (`_httk_total_energy`), the average total
energy of a molecular-dynamics run as an `AverageTotalEnergyRecord`
(`_httk_average_total_energy`), the other core kinds such as `_httk_temperature`,
and the analysis kinds such as `_httk_bulk_modulus` when *httk-analyse* is
installed. Other definitions stay generic records: stored, but their values
are not served.

A store's layout declares every registered record kind, so it depends on the
installed packages. When a newer *httk* or a newly installed package brings
another record kind, an existing store is refused with a message naming
`--upgrade`. Rerun with `--upgrade` (`upgrade=True`) to append the new kinds.
The upgrade is additive: it adds tables and does not rewrite stored rows or
ids. Keep a backup of the store first. A store declared with the analysis
kinds needs *httk-analyse* installed to be collected into again.

A store holding generic records of a definition that is now typed (collected
before its typed kind existed or before it was used for collected values) is
refused, naming those definitions, with advice to rebuild it. Delete it and collect again, keeping the id ledger so record
and run ids are preserved.

### Ids and re-collection

Structures are deduplicated by content and keep the id the store minted for
them. Records, runs and files get ledger ids, which a tree sweep signs with
the project or operator identity key (`tree_ledger_keys`).

Re-collecting only adds. A calculation directory deleted since the last sweep
stays in the store, and a calculation reverted to the exact content of an
older revision writes nothing, so the newer revision stays latest. Rebuild
the store in either case.
