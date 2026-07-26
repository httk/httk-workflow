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
