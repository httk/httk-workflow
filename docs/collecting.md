# Collecting results

*For data-layer authors and anyone reading finished jobs back out of a workspace.*

Collecting is the read-only counterpart of running work. The low-level
`job_records()` iterator reads each stopped job into a `JobRecord`, preserving
where the files are, what produced them, and what happened on the way. The
framework-level `collect()` iterator dispatches each record through its
registered workflow collector and yields a `CollectedJob` with role-keyed
outputs, provenance, products, and any unfulfilled roles. A job that could not
be collected at all is a *degraded* `CollectedJob`: `missing_collector` explains
why, `outputs` is empty, and `unfulfilled` names **every** declared output role,
so a degradation is never mistaken for a complete collection that happened to
declare no outputs.

When a collected output role is a file-valued single result, its run edge points
to a standard `files` entry (`type = "files"`); file lists remain values within
their role record.

The job-embedded declaration governs the Run (immutable facts per job). The
collected Run carries the executing system's job identity in `source_id`; its
store-owned `immutable_id` is left for `httk-store` to mint. ProductLinks
come from the live registered provider's manifest and therefore apply today's
curation; otherwise, for the job-pinned fallback, they come from that job's
own verified pinned manifest and preserve its historical curation,
not today's. The workflow collect hook is the workflow-owned substep of collecting. If no
provider or pinned manifest is reachable, no products are emitted.

`JobRecord` is the layering boundary of *httk₂*. *httk-workflow* has no
database dependency: it produces records, and something else — `httk-store` —
consumes them. A consumer therefore reads results like this, and nothing in
*httk-workflow* knows what `store` or `load_vasp` are:

```python
from httk.workflow import Workspace, job_records

workspace = Workspace("workflow-workspace", mutable=False)
for record in job_records(workspace):
    store.save(load_vasp(record))
```

Every member of a record is derived from exactly the authoritative state a
manager reads — the marker below `state/`, the journal frames its chain names,
and the immutable `job.json` — so a record never says anything the workspace does
not.

## What a record guarantees

**The executed code is pinned.** A record carries the immutable job digest and
the complete runner identity: executor, source, path, and the SHA-256 the job
pinned for every runner living outside its payload. A runner named by the
reserved `pkg:` form additionally reports the installed distribution and its
version, so a stored result names the software that produced it.

**Damage is reported, never guessed.** A job whose journal chain is broken is
still collected, with whatever remains readable and `provenance.gaps` set to
`true`. A result that exists must not become invisible because part of its
history did not survive. Only a job whose `job.json` cannot be read at all is
skipped, loudly, through the module logger: the contract of a record is the
*validated* job behind a result.

**Collecting scales by iteration.** `job_records` is a lazy iterator over one scan of
the requested state directories, and building one record reads only that job's
own payload and journal chain. The measured local snapshot in
{doc}`benchmarks` establishes a reference point; larger campaigns should be
partitioned across workspaces and collected one partition at a time, never
materialized as one in-memory result array.

## Members

| Member | Meaning |
| --- | --- |
| `workspace`, `workspace_id` | the absolute workspace root and its identity |
| `job_id`, `job_key` | the job UUID and the complete `tag--uuid` key |
| `job` | the validated job definition, including its `digest` and the `runner` identity that executed it |
| `runner_provenance` | `module`, `resource`, `distribution`, and `version` of a `pkg:` runner; `null` for every other runner |
| `state` | the kind this job stopped in: `succeeded`, `failed`, `cancelled`, or `paused` |
| `failure` | the unified failure record — `code`, `message`, optional `details`, `retryable` — or `null` |
| `placement` | the placement subtree this job sits in |
| `payload_path`, `workdir_path`, `data_path` | workspace-relative paths to the payload, the last attempt's workdir, and the transactional data of a job that has one |
| `data_generation` | the committed data generation, or `null` for `data.mode` `none` |
| `provenance` | `{"activations": [...], "gaps": bool}` — the journal-derived timeline, oldest first |
| `runner_steps` | the step set the runner declared, when one was ever recorded |
| `runner_description` | reserved: a runner's own `--describe` output attaches here in a later phase, `null` today |
| `children` | the labeled children this job spawned, keyed by spawn label |
| `declarations` | the workflow declarations of this job, keyed by name: `{"declared": ..., "observed": ...}` per name, both carried verbatim |

On the dataclass the paths appear twice on purpose: `payload_path`,
`workdir_path`, and `data_path` are workspace relative, which is what a *stored*
record must hold so it survives moving the workspace, while the properties
`record.payload`, `record.workdir`, and `record.data` resolve them into absolute
`Path` objects against the workspace the record came from.

One activation of `provenance.activations` carries `activation_id`,
`activation_ordinal`, the `step` it ran, the `reason` it started, and its
`attempts`. One attempt carries `attempt_id`, `ordinal`, the `manager_id` and
`writer_id` that owned it, the `record_ref` of the journal frame that opened it,
the recorded `claimed_at`, `started_at`, and `finished_at` timestamps, the
`outcome_action` it published, and its `failure`.

`children` maps each spawn label to `job_id`, `job_key`, and the child's `kind`
when the parent's own frames recorded it. A campaign therefore collects as a
tree: each named child is a job a consumer collects in its own right.

`declarations` reports every declaration name either source knows. `declared` is
the document `job.json` carried, pinned by the immutable job digest; `observed`
is the runtime-refined document the job wrote below
`.httk-job/declarations/`; either is `null` when that source has nothing. The two
are reported side by side and never merged, because reconciling them requires
understanding the vocabulary the document names itself — which is the consumer's
job, not this module's. An observed document that cannot be read is reported as
`null` and sets `provenance.gaps`. The declared documents are not repeated inside
`job`; `declarations` is where they are read. See {doc}`declarations`.
The observed `environment` declaration, when present, is the exact resolved
value/source snapshot that drove the run; its format is
`httk-workflow-environment-resolution` version 2.
The `provenance` declaration becomes a stored `httk.core.Run`; see
{doc}`provenance`.

## Selecting records

```python
records = job_records(
    workspace,
    states=("succeeded", "failed"),
    placement="project/campaign",
)
```

`states` defaults to `("succeeded",)` and accepts only the kinds a stopped job
can be in — `succeeded`, `failed`, `cancelled`, and `paused`; anything else is
refused by name. `placement` restricts the collect to the jobs at or below one
placement, exactly as `httk job list --placement` does.

## From the command line

```console
httk workflow collect --workspace WORKSPACE
httk workflow collect --workspace WORKSPACE --state succeeded --state failed
httk workflow collect --workspace WORKSPACE --placement project/campaign --raw
```

The workspace is attached read-only. The default `collect` command prints one
`CollectedJob` summary per line. `--raw` prints one `JobRecord` per line, while
the hidden compatibility `--json` form materializes those raw records as an
array.

### Summary line and exit codes

Every `collect` invocation except the pure-array `--json` form ends with one
trailing JSONL summary line:

```text
{"format":"httk-workflow-collect-summary","format_version":2,
 "collected":N,"degraded":N,"unfulfilled_roles":N,
 "storage_errors":N,"skipped_unreadable":N}
```

`collected` counts the jobs collected without degradation; `degraded` counts the
degraded ones; `unfulfilled_roles` sums the declared roles left unfulfilled
across all jobs; `storage_errors` counts jobs a `--into` store could not persist;
`skipped_unreadable` counts jobs dropped because their `job.json` could not be
read (these never appear as records — the count is the only place they surface).

The command exits `0` only when `degraded`, `storage_errors`, and
`skipped_unreadable` are all zero. Unfulfilled roles alone do **not** fail the
sweep — a partially fulfilled job is a normal, honestly reported result. This
makes `collect` usable as a gate: a nonzero exit means a job could not be
collected, stored, or even read, and the summary counts say which.

Per-job triage lives on each summary line: `missing_collector` explains a
degradation, `products_unlinked` lists `product_of` links skipped because the
output *was* produced but its curated source edge is absent from the observed
provenance (`"<role> -> <source> (source edge absent in observed provenance)"`) —
a product whose own output role went unfulfilled is reported through
`unfulfilled` only, never here. `collector_exit_status` reports an executable
collector that answered every record but still exited nonzero.

### `--into` partial state

With `--into PATH --id-base BASE`, each collected job's entries, run, and product links are
saved into a file-backed SQLite store, and its report gains
`"stored": {...}`. A degraded job stores nothing — its report carries
`"stored": null, "skipped": "degraded"` and **no** empty `Run` is written, so the
store never fills with contentless provenance. A job whose entries cannot be
stored keeps a `"storage_error"` and fails the exit code.

`--id-base` is required with `--into` and names the dot-separated namespace used
for minted entry ids; `--id-series` selects the campaign series and defaults to
`1`. By default a sealed **id ledger** allocates those ids so they stay stable
across rebuilds — see {doc}`stable_ids`. It lives beside the store at
`<into>.ids.sqlite` (relocate with `--id-ledger PATH`), is created and signed on
first use, and is announced loudly because it is a keep-worthy file to commit
alongside the store. `--no-id-ledger` opts out, and a sweep with no resolvable
workspace signing key falls back to no ledger; both mean store-minted ids that
are **not** stable across rebuilds, warned once. An output already carrying an
assigned public id is never re-numbered, content the store deduplicates onto one
row is recorded as one id with the other keys aliased to it, and a job whose
identity is not stable (a v1 tree with no manifest) is store-minted with a
warning rather than pinned to a stale key. Before persistence, edges to output
records that do not yet have store ids
use those records' content ids. The `--into` path uses two passes: it commits
all job outputs first while building one sweep-wide content-id-to-entry-id map,
then rewrites and commits runs and product links. References not produced in
the sweep first keep an already stored public id, then resolve against the
destination store by content id. Unknown 64-hex content ids make that job a
storage error while leaving its outputs saved and preventing its run and
products from being written; other loose external references are retained
unchanged.

Re-collection is safe because it is stateless: `collect --into` reads the
workspace afresh every time and writes whatever it finds. Re-storing an
already-stored job is de-duplicated on its stable entry, run, and product ids, so
running the same collect twice into the same store changes nothing. A store built
under a different entry-family layout is not migrated in place; point `--into` at
a new store file when the layout changes. Reusing a store whose entry-type layout
does not match the sweep fails fast with a teaching error that names the store
path, the entry types this sweep needs, and the layout difference, and ends
`Collect into a new store file.`

```console
$ httk workflow collect --workspace workflow-workspace | head -1
{"children":{},"data_generation":null,"data_path":null,"declarations":{},"failure":null,
 "format":"httk-workflow-collect","format_version":2,
 "job":{"claim":{"pool":"default","required_capabilities":[]},"data":{"mode":"none"},
"digest":"0e6f…","id":"5c0a…","initial_step":"only","parameters":{},"job_key":"single--5c0a…",
 "name":"collect single","parent":null,"priority":500,
 "runner":{"arguments":[],"executor":"path","path":"single/run.py","sha256":"41b1…",
 "source":"workspace"},"retry_policy":{…},"resources":{},"tag":"single",
 "workdir":{"mode":"persistent","path":"run"},"workflow":"tests.collect.single"},
 "job_id":"5c0a…","job_key":"single--5c0a…","payload_path":"project/single/single--5c0a…",
 "placement":"project/single","provenance":{"activations":[…],"gaps":false},
 "runner_description":null,"runner_provenance":null,"runner_steps":["only"],
 "state":"succeeded","workdir_path":"project/single/single--5c0a…/run",
 "workspace":"/…/workflow-workspace","workspace_id":"a2d1…"}
```

Jobs that ran on a remote are collected the same way once they are
home: `httk workflow transfer REMOTE:NAME default` imports them into the local
default workspace in the terminal state they stopped in, and the collect that
follows cannot tell them from jobs that ran locally. See
{doc}`workflow_cli`.

With `--raw`, each line is exactly `JobRecord.as_mapping()`, and
`JobRecord.from_mapping()` rebuilds the record from it, so the record stream
survives being written to a file, shipped, and read back by the process that
stores it.

## Workflow collect hooks

`collect()` is the workflow-owned collection layer. It resolves the record's
workflow id through `workflow_provider()`, calls that provider's callable or
lazy `module:function` collector, validates role names against declared
`outputs` in the job's embedded workflow declaration, and assembles the
`Run` and `ProductLink` values. Product curation is read from the live
registered provider's manifest, or from the job's own verified pinned manifest
when the fallback is enabled; the embedded declaration supplies only the
immutable Run facts. Old jobs without that declaration fall back to
the currently registered provider declaration, so their role interpretation is
necessarily live rather than historical. A workflow
without a provider or collector is represented as a degraded `CollectedJob`
with `missing_collector` set. With `--allow-job-collector`, collecting
can inspect the job-pinned package tree, validate its own manifest and digest,
and load that tree's collect hook; refusals degrade only that job. See
{doc}`workflow_packages` for the trust tiers and package hook contract.

An executable `[workflow.collect]` member is run once for the matching sweep
from its package tree. For direct package paths and the opt-in job-pinned
fallback, that tree is published and digest-checked; a registered-directory
provider is the explicit-consent current-source exception. The first stdin line is
`{"format":"httk-workflow-collect-stream","format_version":2}`; each
following line is `{"record": <JobRecord mapping>}`. The hook must return one
JSONL response per record, in order: `{"job_id": ..., "outputs": {role:
value}}` or `{"job_id": ..., "error": ...}`. Output values use exactly one
wrapper: `{"entry": {...}}` for a registered entry record,
`{"value": <json>}` for a `DataRecord`, or exactly `{"file": "<path>"}` for a
workspace-confined `FileRecord`. A malformed, errored, missing, wrong-id, or
unresolvable response degrades that job and does not stop the sweep. Response
lines are drained as binary newline-delimited data and decoded as UTF-8 one line
at a time. The limits are enforced during draining: a 1 MiB response line, a
64 KiB stderr line, and 1 MiB total stderr; any limit breach terminates and
degrades the affected executable collector group. Surplus blank response lines
are ignored; a nonblank surplus response line likewise degrades the whole group. Other
malformed or non-UTF-8 responses degrade only their individual job. A declared output
`ref` makes `httk-store` validation hard-required at collect time; without a
`ref`, the framework creates a `_httk_custom_*` property definition. Python
`.py` hooks keep the in-process path and the same successful assembled-output
semantics, but a registered Python collector exception aborts iteration rather
than degrading the job.

### Language fallback and degradation

For a language job, provider dispatch is followed by the job's own
`workflow_language` parameter. A provider-less CWL, PWD, or jobflow job then
uses the language default collector: its output document is read from the
workdir or transactional data tree, ports are mapped to declared roles, and
values become `DataRecord` objects. CWL `File` values are accepted only when
their paths remain inside the workspace, workdir, or data tree; the result
records a file descriptor and sha256. Jobflow reads `jobflow-outputs.json`.

A package with a custom hook records `workflow_collect = "package"`.
Provider-less collection of that job degrades with a registration hint; it
does not silently run the language default. httk-v1 has no default at all and
degrades with a message to declare `[workflow.collect]` when submitted as a
manifest package. A bare v1 directory is not submitted by the `job new` CLI;
use `--workflow-dir` with a manifest or `remote import-v1`. The
`allow_job_collector` pinned-tree fallback is attempted only after this
language fallback, and only with a matching digest and manifest. Any
collector failure degrades that job and does not stop the sweep.

For the distinction between declared entry-typed inputs and opaque implementation
parameters, see {doc}`workflow_packages` and {doc}`declarations`.
