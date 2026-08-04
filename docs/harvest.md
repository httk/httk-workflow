# Harvesting results

*For data-layer authors and anyone reading finished jobs back out of a workspace.*

A harvest is the read-only counterpart of running work: it iterates the jobs of
one workspace that stopped and yields, per job, everything a data layer needs to
store the result — where the files are, what produced them, and what happened on
the way.

`HarvestRecord` is the layering boundary of *httk₂*. *httk-workflow* has no
database dependency: it produces records, and something else — `httk-data` —
consumes them. A consumer therefore reads results like this, and nothing in
*httk-workflow* knows what `store` or `load_vasp` are:

```python
from httk.workflow import Workspace, harvest

workspace = Workspace("workflow-workspace", mutable=False)
for record in harvest(workspace):
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
still harvested, with whatever remains readable and `provenance.gaps` set to
`true`. A result that exists must not become invisible because part of its
history did not survive. Only a job whose `job.json` cannot be read at all is
skipped, loudly, through the module logger: the contract of a record is the
*validated* job behind a result.

**Harvesting scales by iteration.** `harvest` is a lazy iterator over one scan of
the requested state directories, and building one record reads only that job's
own payload and journal chain. The measured local snapshot in
{doc}`benchmarks` establishes a reference point; larger campaigns should be
partitioned across workspaces and harvested one partition at a time, never
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
when the parent's own frames recorded it. A campaign therefore harvests as a
tree: each named child is a job a consumer harvests in its own right.

`declarations` reports every declaration name either source knows. `declared` is
the document `job.json` carried, pinned by the immutable job digest; `observed`
is the runtime-refined document the job wrote below
`.httk-job/declarations/`; either is `null` when that source has nothing. The two
are reported side by side and never merged, because reconciling them requires
understanding the vocabulary the document names itself — which is the consumer's
job, not this module's. An observed document that cannot be read is reported as
`null` and sets `provenance.gaps`. The declared documents are not repeated inside
`job`; `declarations` is where they are read. See {doc}`declarations`.

## Selecting what to harvest

```python
records = harvest(
    workspace,
    states=("succeeded", "failed"),
    placement="project/campaign",
)
```

`states` defaults to `("succeeded",)` and accepts only the kinds a stopped job
can be in — `succeeded`, `failed`, `cancelled`, and `paused`; anything else is
refused by name. `placement` restricts the harvest to the jobs at or below one
placement, exactly as `httk workflow job list --placement` does.

## From the command line

```console
httk workflow harvest WORKSPACE
httk workflow harvest WORKSPACE --state succeeded --state failed
httk workflow harvest WORKSPACE --placement project/campaign --json
```

The workspace is attached read-only. The default `--jsonl` prints one record
object per line as each is produced, which is what a pipeline consumes;
`--json` prints a single array instead and therefore materializes the whole
harvest in memory.

```console
$ httk workflow harvest workflow-workspace | head -1
{"children":{},"data_generation":null,"data_path":null,"declarations":{},"failure":null,
 "format":"httk-workflow-harvest","format_version":1,
 "job":{"claim":{"pool":"default","required_capabilities":[]},"data":{"mode":"none"},
 "digest":"0e6f…","id":"5c0a…","initial_step":"only","inputs":{},"job_key":"single--5c0a…",
 "name":"Harvest single","parent":null,"priority":500,
 "runner":{"arguments":[],"executor":"path","path":"single/run.py","sha256":"41b1…",
 "source":"workspace"},"retry_policy":{…},"resources":{},"tag":"single",
 "workdir":{"mode":"persistent","path":"run"},"workflow":"tests.harvest.single"},
 "job_id":"5c0a…","job_key":"single--5c0a…","payload_path":"project/single/single--5c0a…",
 "placement":"project/single","provenance":{"activations":[…],"gaps":false},
 "runner_description":null,"runner_provenance":null,"runner_steps":["only"],
 "state":"succeeded","workdir_path":"project/single/single--5c0a…/run",
 "workspace":"/…/workflow-workspace","workspace_id":"a2d1…"}
```

Jobs that ran on a remote are harvested the same way once they are
home: `httk workflow transfer REMOTE:NAME default` imports them into the local
default workspace in the terminal state they stopped in, and the harvest that
follows cannot tell them from jobs that ran locally. See
{doc}`workflow_cli`.

Each line is exactly `HarvestRecord.as_mapping()`, and
`HarvestRecord.from_mapping()` rebuilds the record from it, so a harvest survives
being written to a file, shipped, and read back by the process that stores it.
