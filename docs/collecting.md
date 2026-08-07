# Collecting results

*For data-layer authors and anyone reading finished jobs back out of a workspace.*

Collecting is the read-only counterpart of running work. The low-level
`job_records()` iterator reads each stopped job into a `JobRecord`, preserving
where the files are, what produced them, and what happened on the way. The
framework-level `collect()` iterator dispatches each record through its
registered workflow postprocessor and yields a `CollectedJob` with role-keyed
outputs, provenance, products, and any unfulfilled roles.

The job-embedded declaration governs the Run (immutable facts per job). ProductLinks
come from the live registered provider's manifest and therefore apply today's
curation; otherwise, for the job-pinned fallback, they come from that job's
own verified pinned manifest and preserve its historical curation,
not today's. Postprocessing is the workflow-owned substep of collecting. If no
provider or pinned manifest is reachable, no products are emitted.

`JobRecord` is the layering boundary of *httk₂*. *httk-workflow* has no
database dependency: it produces records, and something else — `httk-data` —
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
placement, exactly as `httk workflow job list --placement` does.

## From the command line

```console
httk workflow collect WORKSPACE
httk workflow collect WORKSPACE --state succeeded --state failed
httk workflow collect WORKSPACE --placement project/campaign --raw
```

The workspace is attached read-only. The default `collect` command prints one
`CollectedJob` summary per line. `--raw` prints one `JobRecord` per line, while
the hidden compatibility `--json` form materializes those raw records as an
array.

```console
$ httk workflow collect workflow-workspace | head -1
{"children":{},"data_generation":null,"data_path":null,"declarations":{},"failure":null,
 "format":"httk-workflow-collect","format_version":1,
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

## Workflow postprocessing

`collect()` is the workflow-owned postprocessing layer. It resolves the record's
workflow id through `workflow_provider()`, calls that provider's callable or
lazy `module:function` postprocessor, validates role names against declared
`outputs` in the job's embedded workflow declaration, and assembles the
`Run` and `ProductLink` values. Product curation is read from the live
registered provider's manifest, or from the job's own verified pinned manifest
when the fallback is enabled; the embedded declaration supplies only the
immutable Run facts. Old jobs without that declaration fall back to
the currently registered provider declaration, so their role interpretation is
necessarily live rather than historical. A workflow
without a provider or postprocessor is represented as a degraded `CollectedJob`
with `missing_postprocessor` set. With `--allow-job-postprocessor`, collecting
can inspect the job-pinned package tree, validate its own manifest and digest,
and load that tree's postprocess hook; refusals degrade only that job. See
{doc}`workflow_packages` for the trust tiers and package hook contract.

For the distinction between declared entry-typed inputs and opaque implementation
parameters, see {doc}`workflow_packages` and {doc}`declarations`.
