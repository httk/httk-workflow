# Transfer completion and bounded metadata

`httk job transfer` moves a job by sealing it in the source workspace,
importing the bundle at the destination, and retiring the source copy only
after the destination has acknowledged the import. `httk job eject` and
`httk job adopt` move jobs to and from free-standing directories. This page
describes what those protocols leave on disk, when `httk workspace gc` collects
it, and what an operator does with a transfer that never completed. Ordinary
use needs none of this; it matters when a transfer was interrupted, when a
filesystem quota counts files, or when `httk workflow transfer status [--workspace WS]` reports a
transfer waiting for an operator (`httk project repair --dry-run` reports the
same for workspaces registered in a project). The normative protocol is in the
{doc}`workflow_filesystem_api`.

The protocol takes no file locks and keeps no ledger, epoch or sequence
number. Every state below is a directory or a small file whose name carries the
transfer ID, and every one of them is removed by a rule stated here.

## What the workspace keeps

| Entry | Holds | Kept |
| --- | --- | --- |
| `tmp/eject.<T>`, `tmp/abort.<T>` | a sealing transaction in progress (or being undone) | while the transaction runs; removed by the protocol itself |
| `tmp/import.<owner>.<ns>.<L>` | an adoption lineage and its claims | while an import runs; removed by the import or its takeover |
| `tmp/export.<owner>.<ns>.<T>` | a copy-out of a held export | while the copy-out runs |
| `transfers/adopting/<job_id>` | the per-job claim of a running import | while the import runs |
| `transfers/outgoing/<T>/` | a sealed bundle waiting for its acknowledgement | until acknowledged, retired or reclaimed |
| `transfers/retired/<T>/` | the acknowledged bundle, a full second copy of the payload | `trash_days` after retirement |
| `transfers/acks/<T>.json` | the destination's signed acknowledgement | `trash_days` |
| `transfers/received/<T>` | the replay receipt of an addressed import (a few lines of JSON) | until `sealed_at + W + S` |
| `transfers/exports/<T>/<job_key>/` | an ejected bundle held for copy-out | until copied out or adopted |

`W` is the freshness window (7 days) and `S` the clock-skew bound (130
minutes); a bundle is accepted only within `W` of its sealing time, so after
`W + S` no importer can accept a copy and the receipt that would have
recognized it as a replay is no longer needed.

A transaction directory may be the only copy of a job. Recovery (run by manager
attach, by `job eject`, `job adopt` and the transfer verbs) finishes or undoes
a transaction whose owner is gone: it takes an import over by renaming its
lineage directory, runs the recorded abort or completion steps for a
transaction under `transferring` markers, and sweeps leftovers (an `eject.<T>`
nobody names and older than a day is aborted, an unused `abort.<T>` and a
`tmp/birth.*` older than a day are trashed). A completed transaction leaves
nothing behind in `tmp/`.

## What gc collects, and when

`httk workspace gc` handles transfer state in these categories, all gated the
same way as the rest of garbage collection:

- `retired_bundles`: `transfers/retired/<T>` older than `trash_days`. This is
  the largest item a busy transfer campaign accumulates, since it holds the
  whole payload again. With no `trash_days` configured it is skipped and
  reported as such.
- `transfer_records`: `transfers/acks/<T>.json` older than `trash_days`.
- `transfer_receipts`: `transfers/received/<T>` once `now > sealed_at + W +
  S`. A receipt that cannot be parsed is never deleted.
- `tmp_entries`: a `tmp/trash.*` directory a discarder left behind (it died
  between renaming a tree there and removing it) once it is older than a day,
  like every other abandoned `tmp/` entry. A trash directory that still holds a
  job payload is moved to `quarantine/` instead of being removed. (The
  `transaction_trash` category is unrelated: it collects the aged
  `outcome.ready/transaction/trash` of attempt outcomes.)

gc never removes `tmp/import.*`, `eject.*`, `abort.*`, `export.*` or `birth.*`,
nor `transfers/adopting`, `outgoing` or `exports`: only the protocol steps and
the recovery sweep do. Nothing about a job in flight is therefore reclaimed by
a collection that runs at the wrong moment.

## Held exports

Ejecting to a directory on another filesystem commits the bundle to
`transfers/exports/<T>/<job_key>` and finishes the ejection there (the job has
left the workspace); the copy to its target is a separate, resumable step. If
that step was interrupted or the target was not writable, the bundle stays
held:

```console
$ httk job eject --resume
```

finishes every pending copy-out (managers never do this). The held bundle can
also be taken back with `httk job adopt` of its path under `exports/`. While a
bundle is held the job is not in the workspace and not at the target:
`httk workflow transfer status` reports it as an export waiting for copy-out.

## Transfers in doubt

A bundle in `transfers/outgoing/<T>` is delivered by the transfer CLI, which
re-sends every pending bundle each time it runs. It stays there until the
destination's acknowledgement arrives, so a transfer whose acknowledgement is
lost simply repeats: the destination recognizes the replay and acknowledges
again without creating a second job.

A bundle still unacknowledged after `W` is *in doubt*: it may have been
delivered, or it may still be delivered until `W + S` has passed. `httk workflow transfer status`
reports it, and two operator verbs settle it:

- `httk workflow transfer retire [--workspace WS] JOB_ID` when the destination
  holds the job (verify first). The source behaves as if it had received the
  acknowledgement: the bundle moves to `retired/<T>` and the job's marker is
  removed.
- `httk workflow transfer reclaim [--workspace WS] JOB_ID` to take the job
  back. It is refused until `sealed_at + W + S`, because until then a
  destination could still import the bundle. The job returns to its
  placement and its previous state; a later transfer starts afresh with a new
  transfer ID.

Automated fetch and relay use acknowledgements from the destination and never
retire by job ID. The `retire` verb is an explicit statement by the operator
that delivery succeeded.

## Bounded size

There is no longer a fixed set of "protocol files" whose number does not grow.
What stays on disk is bounded by retention and by recent traffic, not by the
number of jobs ever transferred:

- bundles and acknowledgements for `trash_days` (finite retention bounds them;
  unlimited retention keeps them by design);
- one `received/<T>` receipt per addressed transfer received in the last `W +
  S` (about a week and two hours), so the receipt count is bounded by the
  transfers received in that window;
- claims and transaction directories only while the transaction runs;
- held exports and in-doubt outgoing bundles until an operator or a resume
  finishes them.

An emptied HPC workspace whose transfers are all acknowledged, retired and past
retention therefore keeps nothing transfer-related except, briefly, receipts
from the last week. A returning home workspace necessarily keeps the actual
jobs and their markers and journals.

## Cost of a sweep

The CLI seals and pulls a sweep before importing it in a batch, then retires
the acknowledgements in a batch, with one recovery and selection pass per
sweep rather than per job. Each import checks presence by name lookups
(`received/<T>`, `acks/<T>.json`, `transfers/adopting/<job_id>`) and by the
marker index, never by scanning a directory of receipts. Imports of different
bundles proceed independently: they serialize only on a job claimed by two
copies of the same bundle. Repeated single-job commands forfeit the batching;
use one multi-job transfer or sweep for many jobs.
