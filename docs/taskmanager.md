# Task-manager usage

Installing *httk-workflow* provides the `httk-taskmanager` executable.

## Initialize a workspace

```console
httk-taskmanager init WORKSPACE
```

This creates a `core-v2` workspace. Optional supported extensions are enabled at
initialization:

```console
httk-taskmanager init WORKSPACE \
  --extension transactional-data-v1 \
  --extension priority-bands-v1
```

## Submit a job

A prepared payload is a directory containing an immutable `job.json` and its
runner. Submit it at any arbitrary placement:

```console
httk-taskmanager submit WORKSPACE PAYLOAD --placement project-a/00/17
```

Submission copies by default. `--move` performs a same-filesystem rename and
consumes the source directory.

## Share one runner between many jobs

A campaign of millions of jobs should not copy its runner into every payload.
Publish the runner once into the workspace runner store instead:

```console
httk workflow runner publish ./relax.py --workspace WORKSPACE --name relax.py
```

The command prints the reference to embed in every `job.json` that uses it:

```json
{"path": "relax.py", "sha256": "…", "source": "workspace"}
```

Publication is content addressed. Publishing identical bytes again changes
nothing, and replacing a stored name whose content differs requires `--replace`,
because live jobs already reference the stored digest. Before each attempt the
manager copies the runner below the attempt control directory, verifies the
pinned digest against that copy, and executes only the copy; a mismatch fails
the job with `runner_mismatch` and an unresolvable runner with
`runner_unavailable`. A detached transfer carries the runners its job
references, and importing installs the missing ones at the destination.

Runners deployed outside any workspace use `"source": "installed"` and resolve
against the ordered `--runner-search-path` roots of the manager.

## Run

```console
httk-taskmanager run WORKSPACE --workers 8
```

Without pool configuration, a manager advertises the reserved `default` pool.
Additional routing and capability labels are explicit:

```console
httk-taskmanager run WORKSPACE \
  --pool vasp \
  --capability gpu \
  --workers 4
```

`--until-idle` is useful for batch invocations and tests. Persistent-workdir
takeover requires proof that an old writer has stopped; the explicitly unsafe
`--unsafe-persistent-takeover` option relaxes that rule and is recorded in
attempt state.

Every claim, launch, transition, recovery decision, and refused request is
logged. The console reports warnings and errors, while the complete info-level
record is rotated into `.httk-workflow/managers/MANAGER_ID/log`. `--log-level`
raises or lowers both, `--log-file` moves the file, and `--json-logs` emits one
JSON object per line for ingestion.

A manager drains on `SIGTERM` or `SIGINT`, which is what a batch system sends
at walltime. The first signal stops claiming, terminates the running attempts,
and keeps committing their outcomes for `--drain-timeout` seconds before
exiting successfully; a second signal exits immediately. Anything left behind
is recovered from its expired lease by the next manager.

## Inspect and control

```console
httk-taskmanager status WORKSPACE
httk-taskmanager status WORKSPACE --json

httk-taskmanager request WORKSPACE JOB_UUID pause \
  --operator "$USER" --reason "inspection"

httk-taskmanager request WORKSPACE JOB_UUID continue \
  --operator "$USER" --reason "inputs repaired"
```

Requests capture the exact current marker generation and record reference.
A delayed request therefore cannot mutate a newer job state.

## Inspecting jobs

Five commands read one job the way a manager reads it — the authoritative marker,
the journal frame that marker names, and the immutable `job.json` — and none of
them writes protocol state:

```console
httk workflow job list WORKSPACE --kind ready --placement project-a
httk workflow job show WORKSPACE JOB
httk workflow job log WORKSPACE JOB --limit 20
httk workflow job why WORKSPACE JOB
```

`JOB` is a job UUID, a complete `tag--uuid` job key, or any unique prefix of
either; an ambiguous prefix is refused with the jobs it matched. Every command
also accepts `--json` and prints one object: a report, a frame array, a
diagnosis, or a job array.

`job show` reports the state kind, placement, priority, generation, job digest,
runner identity, the budgets of the retry policy against what has been consumed,
the current and initial step, any step set the runner declared, the last
failure, the join and per-child state of a waiting job, and the payload,
workdir, and data paths.

`job log` walks the journal backward from the marker through
`previous_record_ref` and prints one line per state frame, oldest first, with the
timestamp, the transition, the step, the attempt ordinal, the reason, and any
failure code. A frame that cannot be read is reported in place; whatever history
remains readable is still shown.

`job why` answers "why is this job not running?" for every state:

- `submitted`: whether any manager has registered it, and which live managers
  serve its runner backend;
- `ready`: every claim precondition, one line each — runner backend, claim pool,
  required capabilities, the maintenance lock, the workspace core profile, the
  attempt budgets, and which live manager would accept the job;
- `claimed` and `running`: the owning manager, its heartbeat age against the
  recorded lease, and whether an expired lease means recovery rather than a stuck
  job;
- `committing`: that a published outcome is being committed and any manager
  serving the backend resumes it;
- `waiting`: the join condition, every child with its label and state, which
  children block, and which cannot be resolved in this workspace;
- `failed`: the failure, whether an operator `continue` still fits inside the
  retry budget, and the `error.json` breadcrumb of the last attempt;
- `paused`, `succeeded`, and `cancelled`: the state and how to proceed.

The job side of every precondition comes from `job.json` and cannot drift. The
other side — pools, capabilities, and served backends — is deployment policy of
whichever manager is running and is read from the manifest each manager
publishes, so a manager that is not running is reported as absent rather than
assumed.

Reading *results* rather than status is a harvest: `httk workflow harvest
WORKSPACE`, or `httk.workflow.harvest`, streams one record per finished job for a
data layer to store; see {doc}`harvest`.

## The foreground debug runner

```console
httk workflow job debug WORKSPACE PAYLOAD --step relax
httk workflow job debug WORKSPACE JOB --follow-children
```

`job debug` drives exactly one job to a terminal state in the foreground and
streams the attempt's `stdout.log` and `stderr.log` to the console as they grow.
Every transition is performed by a private task manager whose scans are
restricted to that one job, so the debugged job runs through exactly the code
paths a production manager uses and no unrelated work is claimed. Lines are
prefixed with the step that produced them, and `[debug]` marks each transition
the polling loop observed; `job log` always holds the complete record afterwards.
`--log-level` raises the private manager's own console log, which is quiet by
default.

The first argument is either a payload directory, which is submitted fresh at
`--placement` (`debug` by default), or a selector of a job that already exists.
`--step` overrides the initial step of a fresh payload; overriding the step of a
job that already has a history is refused, because rewriting history is what the
recorded `override_step` request is for. `--follow-children` drives the children a
waiting job spawned, depth first, and then resumes the parent.

The exit status is `0` when the job succeeded, `3` when it failed, and `4` when
it stopped without finishing — paused, cancelled, or waiting for children
without `--follow-children`. A live maintenance lock is refused up front, since
it would stop every launch anyway.

## Runner contract

The runner executes in the selected persistent or isolated workdir. It reads
the context named by `HTTK_WORKFLOW_CONTEXT` and publishes
`outcome.tmp.<nonce>/` as `outcome.ready/` beneath
`HTTK_WORKFLOW_CONTROL_DIR`. See the
{doc}`workflow_filesystem_api` for the complete protocol.

The local executor starts runners behind a one-byte launch gate. It records
the process identity and commits the `running` marker before releasing that
gate. If the manager disappears during this narrow launch interval, the gated
process observes end-of-file and exits without executing the runner.

`httk-taskmanager` executes only the normal `path` runner backend. Legacy
`ht_steps` jobs use a distinct backend and are intentionally left untouched;
run those with [*httk* v1 task compatibility](v1_compatibility.md).
