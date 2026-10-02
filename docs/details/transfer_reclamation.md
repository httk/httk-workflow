# Transfer completion and bounded metadata

`httk job transfer` moves a job by detaching it from the source workspace,
importing its sealed bundle at the destination, and retiring the source only
after the destination has acknowledged the import. This page describes what
that completion protocol leaves on disk, how it recovers when a transfer is
interrupted or replayed, and why its bookkeeping stays small: an HPC workspace
that has sent thousands of jobs home keeps a handful of protocol files whose
count does not grow with the number of jobs. Ordinary use needs none of this;
it matters when a transfer was interrupted, when both ends run different
*httk-workflow* versions, or when a filesystem quota counts files.

## Epochs and sequence numbers

### Allocation

New detached transfers carry a random UUID `transfer_epoch` and a positive
`transfer_sequence`. Each process allocates a fresh epoch per
workspace/destination stream, keeping its counter and pending reservations in
process memory. A restart never adopts an epoch from disk.

The session key includes the PID (so a fork cannot inherit an allocator), host,
canonical workspace path, and control directory device/inode. A login-node
switch or new process session therefore starts a fresh epoch. This is harmless:
it adds one coalesced receipt range per session, not one per job. Interleaved
processes keep their own volatile streams.

### Issued state on disk

`transfers/protocol/issued.json` and its write-ahead copy
`issued-checkpoint.json` remain durable diagnostic and retry state, but
**neither copy authorizes reuse of an epoch**. A reset, a missing copy, or a
mismatch with the current session's remembered stream rotates the epoch.

Restoring both files consistently in place cannot reuse a ticket: a restarted
process generates a new epoch, and a running process keeps its counter in
memory and detects rollback of its own stream. A disk entry of another epoch
cannot replace that volatile counter. The guarantee assumes fresh UUID
randomness and filesystem restore, not rollback of the whole running process
and its random-number source.

### Reservations and fencing

A reservation may be reused until fencing is attempted. Before invoking the
fencing transition, the allocator forgets the reusable in-memory reservation:
another process could finish that transfer, so the reservation must not be
reused when the job returns, even if its old pending disk entry is restored. An
interrupted attempt or process restart may reissue an unfenced reservation
under a fresh epoch, because it was never offered.

Once the fencing transition is durable, recovery uses the epoch and sequence in
that state. A SEALED transfer always resumes from its immutable manifest and
ledger, even when newer allocations have started in another session.
Reservations are removed by exact transfer UUID, so an old completion cannot
remove a newer reservation for the same job. An interrupted allocator
publication may waste a reservation or epoch but cannot reissue an accepted
ticket.

## Receipts at the destination

### Compact receipt ranges

The destination's `transfers/protocol/received.json` holds merged inclusive
sequence ranges keyed by `source_workspace_id/transfer_epoch`. Receiving 1
through N in one session leaves just `[[1, N]]`: no job UUIDs, transfer UUIDs,
digests or payload paths. Out-of-order imports keep separate ranges until the
intervening transfers arrive.

The range state is the durable replay fence, also after an imported job has
left this workspace. Never roll back or delete destination receipt state
independently of its actual jobs.

### Import and replay

Import validates the sealed bundle, publishes the durable payload and state,
and writes its ordinary acknowledgement. It then durably records the sequence
receipt before unlinking the individual `acks/` and `imported/` JSON files. This
can safely precede source retirement, because the compact receipt replaces
duplicate detection rather than abandoning it.

A replay validates the whole bundle again and looks up the presented job UUID:

- If a marker exists, its transfer UUID, digest and epoch must match the
  manifest, or import raises `WorkspaceCorruptionError` before signing.
- With a valid epoch and no live job, the old receipt range permits a replay
  acknowledgement without creating a job.
- A *new* job after an allocator reset has a new epoch and is imported normally;
  a matching source ledger alone would not prevent silent loss here.

### Receipts from before epochs

Pre-epoch compact receipts from the earlier implementation cannot safely
certify a replay after their job has left. Such a replay fails closed with
`WorkspaceCorruptionError`. The remedy: verify delivery, then explicitly run
`httk workflow transfer retire . <JOB_ID>` from the source workspace.

### Acknowledgements

Sequenced acknowledgements omit the historical `acknowledged_at` timestamp;
their signature attributes the returned receipt, not a permanently retained
original importer. Before retirement the source still checks the
acknowledgement signature, job/workspace identity, payload digest, epoch and
sequence against its sealed ledger.

## Retirement and reclamation at the source

Retirement durably renames the source bundle and publishes a retired ledger
before reclaiming anything, and prunes the ledger after reclamation. A missing
ledger is a terminal no-op, even when a newer transfer of that job exists.

Journal segments protected by other jobs or managers are recorded once per
segment in `transfers/protocol/journal.json`, instead of keeping one ledger per
retired job:

- An actual retirement or explicit cleanup retries this inventory;
  `recover_transfers` alone does not when there is no retired ledger.
- Missing-ledger acknowledgements and idle recovery never trigger journal GC.
- An empty inventory is not created, and a drained one is removed.
- No cleanup journal writer is opened.

Existing marker-chain, live-manager, retention and quarantine protections still
apply. A header-only writer from a failed detach is collected under the same
reference checks.

## Locking

Protocol mutations are serialized per workspace by the permanent
`transfers/protocol/lock` inode using POSIX `flock`. Both ends must support
shared-filesystem locking and the existing durable write/rename/fsync
semantics. The OS releases the lock on process death. No operation holds two
workspace locks.

This trades concurrent imports within one workspace for simple, atomic updates
of the compact receipt state. It has not been benchmarked at one million jobs
or verified against an actual HPC/NFS power-loss failure.

## Crash and duplicate cases

| Boundary | Durable state and retry |
| --- | --- |
| Import before acknowledgement | The destination marker's transfer provenance recognizes the same import. Retry finishes the seal/receipt without another job. Before this job can detach onward, its import receipt is completed from its authoritative state. |
| Acknowledgement before compact receipt | The individual acknowledgement still exists. Retry records the sequence and prunes the individual records. The source remains sealed until acknowledged. |
| Compact receipt before/partway through pruning | The range prevents a second import even if either individual record is missing. Pruning is idempotent. |
| Acknowledgement before source retirement | The intact source is re-offered; the destination reconstructs its receipt. The source checks it before retiring. |
| Retirement before/partway through reclamation | The retired ledger fences the source and retains the segment inventory. Recovery finishes payload/segment reclamation and ledger pruning. |
| Source ledger already pruned | Acknowledgement by transfer UUID is a no-op. Explicit offers/transfers for absent exact job UUIDs return no work. A missing UUID cannot be distinguished from an unknown UUID without retaining tombstones; reports include its UUID and reason in `skipped`, and text output prints the skip. |
| Old bundle replayed after the destination sent the job onward | Its sequence remains received. Validation and acknowledgement occur, but no marker or payload is created. |
| Old remote retirement replayed while a newer transfer exists | Automated fetch/relay retire with the actual acknowledgements, including transfer UUID, rather than only the job UUID. The newer ledger is untouched. |

## Retirement by job id and upgrades

The legacy explicit `retire JOB_ID` operation remains a caller assertion that
payload delivery succeeded. Automated fetch/relay instead use
`--acknowledgements-json` with the validated destination envelopes. This
protocol extension needs both ends upgraded; it is never silently downgraded to
retirement by job id. Receive accepts repeated `--bundle` arguments and
acknowledges them as one batch.

**Upgrade check:** before starting transfers, run this read-only check on both
endpoints, in the same Python environment as their `httk` executable:

```console
python -c 'from httk.workflow.transfers import import_bundles, acknowledge_transfers; from httk.workflow._transfer_receipts import epoch_of'
```

If it fails, stop and upgrade that endpoint before importing anything. This is
an explicit operator preflight, not automatic fallback or version negotiation.
Skipping it can still fail a command after imports; source bundles stay intact
until valid acknowledgements have been accepted.

## Residual size and compatibility limits

### What remains on disk

With all sequenced transfers completed and ordinary finite retention, an emptied
HPC workspace keeps four protocol files: `lock`, `issued.json`,
`issued-checkpoint.json` and `received.json`. A fifth, `journal.json`, exists
only while protected segments remain to be collected.

The file count is independent of job count, but byte size is not fixed. JSON
counters and range endpoints need logarithmically more digits as the sequence
grows. Summary entries scale with workspace peers and historical epochs,
unfinished reservations and sequence holes, and protected journal segments, not
with completed job identities. Once every transfer issued in a session has
arrived, that session's receipt ranges coalesce to one interval, so metadata
bytes grow with historical sessions. Batch jobs within a process/session to
amortize this state as well as scan cost.

### Outside the bound

- Unsequenced bundles already in flight keep their individual destination
  receipts: without an ordered sequence, removing them would let an old bundle
  recreate a job after it left the workspace. This is a compatibility
  exception; legacy transfers do not meet the bound.
- Existing unlimited trash/journal retention keeps payloads and history by
  design and is also outside the bound.
- Protected live state and quarantine are never removed to make a size test
  pass.
- A returning home workspace necessarily keeps the N actual jobs and their live
  state markers and journals. The bound covers transfer bookkeeping there, and
  the entire control tree of an emptied HPC workspace.

## Cost of a sweep

### Batching

The CLI seals and pulls a sweep before importing it in a batch, then retires the
acknowledgements in a batch, with one source recovery/selection pass per sweep,
not per job. Known source markers are passed to detach. Imports take one
identity snapshot under the destination protocol lock; standalone imports use
`find_marker_by_id` and check duplicate provenance. That existing index is not
O(1) for cold, terminal or absent lookups, so replacing a full-scan helper alone
would not have solved the problem.

### Retirement index

A retirement batch scans current markers and sealed ledgers once, recording
segment protection counts and the references each sealed transfer owns.
Retiring a transfer removes just its protection counts, and later collection
queries only the candidate segments.

As in ordinary GC, a terminal marker protects its head segment without reading
frames, and only non-terminal marker chains are walked. Terminal historical
references do not pin retired candidate segments. Shared segments are freed only
when no live head, non-terminal chain or sealed owner still protects them. The
duplicate GC call is removed. No-op acknowledgements neither build this index
nor touch an empty journal inventory.

### Complexity

For J transfers, U unrelated markers and H journal references examined, a sweep
costs O(U + J + H) metadata work plus its payload I/O, where H excludes
unrelated terminal history entirely. Initial marker enumeration is still O(U);
steady-state retirement is O(the retiring transfer's candidate references),
independent of U.

This is **amortized sweep complexity**, not a claim that one isolated, cold
single-job call is O(1). The timing regression includes the initial scan in its
batch mean and also reports the warm median. Repeated single-job commands
forfeit batching; use one multi-job transfer or sweep.
