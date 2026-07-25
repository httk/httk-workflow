# Task-manager usage

Installing *httk-workflow* provides the `httk-taskmanager` executable.

## Initialize a workspace

```console
httk-taskmanager init WORKSPACE
```

This creates a `core-v1` workspace. Optional supported extensions are enabled at
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
