# Transfer completion and bounded metadata

New detached transfers carry a random UUID `transfer_epoch` and a positive
`transfer_sequence`. Each process allocates a fresh epoch per workspace/destination
stream, with its counter and pending reservations held in process memory. A
restart never adopts an epoch from disk. The session key includes the PID (so a
fork cannot inherit an allocator), host, canonical workspace path, and control
directory device/inode. A login-node switch or new process session therefore
starts a fresh epoch; this is harmless and adds one coalesced receipt range per
session, not one per job. Interleaved processes keep their own volatile streams.

`transfers/protocol/issued.json` and its `issued-checkpoint.json` write-ahead copy
remain durable diagnostic/retry state, but **neither copy authorizes reuse of an
epoch**. A reset, missing copy, or mismatch with the current session's remembered
stream rotates the epoch. Restoring both files consistently in place cannot
reuse a ticket: a restarted process generates a new epoch, while a running
process retains its counter in memory and detects rollback of its own stream.
A disk entry belonging to another epoch cannot replace that volatile counter.
The guarantee assumes fresh UUID randomness and filesystem restore, not rollback
of the entire running process and its random-number source.

A reservation may be reused before fencing is attempted. Before invoking the
fencing transition, the allocator forgets the reusable in-memory reservation:
another process could finish that transfer, so even restoring its old pending
disk entry must not reuse it when the job returns. An interrupted attempt or
process restart may reissue an unfenced reservation under a fresh epoch because
it has never been offered.
Once the fencing transition is durable, recovery uses the epoch and sequence in
that state. A SEALED transfer always resumes from its immutable manifest/ledger,
even when newer allocations have started in another session. Reservations are
removed by exact transfer UUID, so an old completion cannot remove a newer
reservation for the same job. An interrupted allocator publication may waste a
reservation or epoch but cannot reissue an accepted ticket.

The destination's `transfers/protocol/received.json` holds merged inclusive
sequence ranges keyed by `source_workspace_id/transfer_epoch`. Receiving 1 through
N within one session leaves just `[[1, N]]`: no job UUIDs, transfer UUIDs, digests,
or payload paths. Out-of-order imports retain separate ranges until intervening
transfers arrive. The range state is the durable replay fence, including after
an imported job has left this workspace. Never roll back or delete destination
receipt state independently of its actual jobs.

Import first validates the sealed bundle, publishes the durable payload and state,
and writes its ordinary acknowledgement. It then durably records the sequence
receipt before unlinking the individual `acks/` and `imported/` JSON files. This
can safely happen before source retirement because the compact receipt replaces,
rather than abandons, duplicate detection. A replay validates the whole bundle
again and looks up the presented job UUID. If a marker exists, its transfer UUID,
digest, and epoch must match the manifest or import raises
`WorkspaceCorruptionError` before signing. With a valid epoch and no live job,
the old receipt range permits a replay acknowledgement without creating a job.
A *new* job after an allocator reset has a new epoch and is imported normally;
a matching source ledger alone would not prevent silent loss here.

Pre-epoch compact receipts from the earlier implementation cannot safely certify
a replay after their job has left. Such a replay fails closed with
`WorkspaceCorruptionError`, with a remedy to verify delivery and explicitly run
`httk workflow transfer retire . <JOB_ID>` from the source workspace. Sequenced
acknowledgements omit the historical `acknowledged_at` timestamp; their signature
attributes the returned receipt, not a permanently retained original importer.
The source still checks the acknowledgement signature, job/workspace identity,
payload digest, epoch, and sequence against its sealed ledger before retirement.

Retirement durably renames the source bundle and publishes a retired ledger before
reclaiming anything. After reclamation it prunes the ledger. A missing ledger is a
terminal no-op, including when a newer transfer of that job exists. Journal
segments protected by other jobs or managers are recorded once per segment in
`transfers/protocol/journal.json`, rather than retaining one ledger per retired job.
An actual retirement or explicit cleanup retries this inventory;
`recover_transfers` alone does not retry it when there is no retired ledger. Missing-ledger
acknowledgements and idle recovery never trigger journal GC; an empty inventory
is not created and is removed once drained. No cleanup journal writer
is opened. Existing marker-chain, live-manager, retention, and quarantine
protections continue to apply. A header-only writer from a failed detach is
collected using those same reference checks.

Protocol mutations are serialized per workspace by the permanent
`transfers/protocol/lock` inode using POSIX `flock`. Both ends must support shared
filesystem locking and the existing durable write/rename/fsync semantics. The lock
is released by the OS on process death. No operation holds two workspace locks.
This trades concurrent imports within one workspace for simple, atomic updates to
the compact receipt state. It has not been benchmarked at one million jobs or
verified against an actual HPC/NFS power-loss failure.

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

The legacy explicit `retire JOB_ID` operation remains a caller assertion that
payload delivery succeeded. Automated fetch/relay use `--acknowledgements-json`
with the validated destination envelopes. Upgrade both ends for that protocol
extension; it is not silently downgraded to retirement by job ID. Receive now
accepts repeated `--bundle` arguments and acknowledges them as one batch.

**Upgrade guard:** before starting transfers, run this read-only check on both
endpoints, using the same Python environment as their `httk` executable:

```console
python -c 'from httk.workflow.transfers import import_bundles, acknowledge_transfers; from httk.workflow._transfer_receipts import epoch_of'
```

If it fails, stop and upgrade that endpoint before importing anything. This is an
explicit operator preflight, not an automatic fallback/version negotiation.
Ignoring it can still fail a command after imports; source bundles remain intact
until valid acknowledgements have been accepted.

## Residual size and compatibility limits

With all sequenced transfers completed and ordinary finite retention, an emptied
HPC workspace retains four protocol files: `lock`, `issued.json`,
`issued-checkpoint.json`, and `received.json`. A fifth, `journal.json`, exists
only while protected segments remain to be collected.
The file count is independent of job count. JSON counters and range endpoints
need logarithmically more digits as the sequence increases. Summary entries scale
with workspace peers and historical epochs, unfinished reservations/sequence holes, and protected
journal segments, rather than completed job identities. Once every issued
transfer in a session has arrived, that session's receipt ranges coalesce to one
interval. Metadata bytes grow with historical sessions; fixed file count does
not mean fixed byte count. Batch jobs within a process/session to amortize this
state as well as scan cost.

Unsequenced bundles already in flight retain their individual destination
receipts: without an ordered sequence, removing those receipts would allow an old
bundle to recreate a job after it left the workspace. This is a compatibility
exception, not a claim that legacy transfers meet the bound. Existing unlimited
trash/journal retention deliberately retains payloads/history and is also outside
the bound. Protected live state and quarantine are never removed to make a size
test pass. A returning home workspace necessarily retains the N actual jobs and
their live state markers/journals; the boundedness guarantee concerns transfer
bookkeeping there, and the entire control tree of an emptied HPC workspace.

`tests/test_transfer_residuals.py` compares exact metadata file lists for 5 and
50 roundtrips, including shared terminal journal segments, and checks crash
boundaries, delayed replays, concurrent receivers, sequence reservation recovery,
remote incoming staging, and transfer-specific retirement. The existing retirement
suite checks shared/live journal protection, partial reclamation, retention,
sealed integrity, and quarantine preservation.

## Sweep complexity

The CLI seals/pulls a sweep before importing it in a batch, then retires the
acknowledgements in a batch. There is one source recovery/selection pass per
sweep, not per job. Known source markers are passed to detach. Imports take one
identity snapshot under the destination protocol lock; standalone imports use
`find_marker_by_id` and check duplicate provenance. This existing index is not
O(1) on cold/terminal/absent lookups, so replacing a full-scan helper alone would
not have solved the problem.

A retirement batch scans current markers and sealed ledgers once. It records
segment protection counts and the references owned by each sealed transfer.
Retiring that transfer removes just its protection counts; subsequent collection
queries only the candidate segments. As in ordinary GC, a terminal marker
protects its head segment without reading any frames; only non-terminal marker
chains are walked. Terminal historical references do not pin retired candidate
segments. Shared segments are freed only when all live heads, non-terminal
chains, and sealed owners no longer protect them. The duplicate GC call is
removed. No-op acknowledgements do not build this index or touch an empty
journal inventory.

For J transfers, U unrelated markers, and H journal references examined, a sweep
costs O(U + J + H) metadata work (plus its payload I/O), where H excludes
unrelated terminal history entirely. Initial marker enumeration is still O(U),
and steady-state retirement is O(the retiring transfer's candidate references),
independent of U. Thus this is **amortized sweep complexity**, not a claim that
one isolated, cold single-job call is O(1). The timing regression includes the
initial scan in its batch mean and also reports the warm median. Repeatedly
issuing single-job commands forfeits batching; use one multi-job transfer/sweep.

The epoch regressions cover delete/reset, allocator-only and consistent two-file
in-place rollback, clone, inherited fork memory, interleaved processes, and both
live-destination and job-already-left cases. Restart tests cover sealed resume
and unfenced pending reissue, including a second process completing a fenced
transfer and a subsequent consistent restore of its old pending reservation
before the job is sent again. Conflicting live-job transfer IDs and payload
digests are rejected. The 0/1,000/4,000-marker timing test asserts one protection
scan per 128-job batch. A separate terminal-history test counts protection-chain
frame reads for 4 and 4,000 terminal jobs, including six-frame histories: counts
must be identical. Another test verifies that old terminal references do not
prevent eager collection while terminal heads remain protected. All original
interruption and file-residual tests remain in place.
