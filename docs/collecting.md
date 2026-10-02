# Collecting results

Collecting is the read-only counterpart of running: it reads stopped jobs back
out of a workspace as records and, optionally, stores them. *httk-workflow*
has no database dependency of its own. It produces records, and *httk-store*
consumes them.

```console
httk collect --workspace default
httk collect --workspace default --state succeeded --state failed --placement project/screening
httk collect --workspace default --into results.sqlite --id-base httk.campaign
```

Each collected job prints one JSON summary line, and the sweep ends with one
summary line of counts. The command exits non-zero only when a job could not be
collected, stored or read, so it doubles as a gate. `--into` saves each job's
output entries, its run and its product links in a file-backed SQLite store;
`--id-base` names the namespace of the minted entry ids, which an id ledger
beside the store keeps stable across rebuilds (see {doc}`provenance`).
Re-collecting is stateless and deduplicated, so running the same collect twice
changes nothing.

## What a record holds

The low-level `job_records()` iterator yields one `JobRecord` per stopped
job: the validated job definition with its digest and pinned runner identity,
the terminal state and failure, the paths of the payload, workdir and
committed data, the attempt timeline derived from the journal, the children
it spawned, and its declarations. A job whose journal is damaged is still
collected, with `provenance.gaps` set, so a result never becomes invisible
because part of its history did not survive.

`collect()` dispatches each record through the collector its workflow
provides and yields a `CollectedJob` with role-keyed outputs, the run, and
products. A workflow package supplies that collector as its `[workflow.collect]`
hook, which reads the job's parameters and result files through the record:

```python
from httk.codes.vasp.collect import read_total_energy


def collect(record):
    prefix = record.parameter("data_prefix", "vasp") or ""
    return {"total_energy": read_total_energy(record.result_file("OUTCAR", data_prefix=prefix))}
```

A job without a usable collector is reported as *degraded*, with every
declared output role unfulfilled; a workflow that only orchestrates others and
declares no outputs is *run-only* and stores just its run.

From Python, a consumer reads a workspace like this:

```python
from httk.workflow import Workspace, collect

workspace = Workspace("default", mutable=False)
for item in collect(workspace, states=("succeeded",)):
    print(item.record.job_key, item.outputs)
```

## Recognized calculations

A directory tree of finished calculations that were never run through a
workspace, such as a folder of VASP runs, is collected by walking it:

```console
httk collect calculations/ --dry-run
httk collect calculations/ --into results.sqlite --id-base mydb
```

Each directory is offered to the recognized-calculation collectors that
installed code packages register (see {doc}`code_support`). A collector is a
workflow package with a `[workflow.recognize]` table and a `recognize.py` hook
that claims a directory from its content, so the stored run keeps the same
identity wherever the directory lives. `--dry-run` prints how every directory
would be claimed without collecting anything.

The full guide, {doc}`details/collecting`, covers every record member, the
summary line and exit codes, the two-pass `--into` storage and its revisions,
executable collect hooks and their limits, format fallbacks for CWL, PWD and
jobflow jobs, and the recognition, identity and collision rules of calculation
trees.
